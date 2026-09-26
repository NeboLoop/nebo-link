//! The supervisor against a real Hermes install, run by hand:
//!
//! ```text
//! NEBO_LINK_LIVE_HERMES_HOME=/path/to/a/throwaway/home \
//!   cargo test -p nebo-link --test supervise_live -- --ignored --nocapture
//! ```
//!
//! It starts that home's gateway and dashboard with Hermes' own commands
//! (the service path only where `hermes gateway install` accepts the home),
//! waits for both to answer, then stops what it started. Use a throwaway
//! copy of a home, never a linked one: it starts processes.

#![cfg(unix)]

use std::time::{Duration, Instant};

use nebo_link::endpoints::Endpoints;
use nebo_link::state::{Link, ModelsEndpoint, Root};
use nebo_link::supervise::{self, ProcessState, Supervisor};
use nebo_runtimes::{Environment, Runtime, detect};

#[tokio::test]
#[ignore = "starts a real Hermes; run by hand against a throwaway home"]
async fn hermes_gateway_and_dashboard_are_started_and_stopped() {
    let home = std::env::var("NEBO_LINK_LIVE_HERMES_HOME").expect("NEBO_LINK_LIVE_HERMES_HOME");
    let mut env = Environment::current();
    env.vars.insert("HERMES_HOME".into(), home.clone());
    let install = detect(&env)
        .into_iter()
        .find(|i| i.runtime == Runtime::Hermes)
        .expect("a Hermes install at that home");
    assert_eq!(install.home.display().to_string(), home);
    for process in &install.processes {
        println!(
            "{}: health {} run `{} {}` service {:?}",
            process.name,
            process.health.url,
            process.run.program,
            process.run.args.join(" "),
            process.service.as_ref().map(|s| s.definition.display().to_string())
        );
    }

    // The link's state beside the home, kept after the run for its logs.
    let state = std::path::Path::new(&home).with_extension("link");
    let _ = std::fs::remove_dir_all(&state);
    let dir = Root::at(&state).bot("live");
    dir.create().unwrap();
    dir.save(&Link {
        bot_id: "live".into(),
        name: "live".into(),
        runtime: Runtime::Hermes,
        owner_id: "owner".into(),
        home: install.home.clone(),
        env: install.restart.env.clone(),
        endpoints: Endpoints::from_env(),
        local_password: String::new(),
        models: ModelsEndpoint {
            port: 1,
            key: String::new(),
            enabled: false,
        },
        api_server_key: String::new(),
        services: vec![],
        acp: None,
    })
    .unwrap();

    let supervisor = Supervisor::new(&dir, Runtime::Hermes, install.processes.clone());
    let running = tokio::spawn(supervisor.clone().run());
    let started = Instant::now();
    let all_up = loop {
        let status = supervisor.status();
        for p in &status {
            println!("{:>5}s {}: {}", started.elapsed().as_secs(), p.name, supervise::describe(p));
        }
        if status.len() == install.processes.len()
            && status.iter().all(|p| matches!(p.state, ProcessState::Running { .. }))
        {
            break true;
        }
        if started.elapsed() > Duration::from_secs(120) {
            break false;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    running.abort();
    let link = dir.load().unwrap();
    println!("services the link installed: {:?}", link.services);
    println!("processes.json: {}", std::fs::read_to_string(dir.processes_file()).unwrap_or_default());
    for process in &install.processes {
        let log = dir.runtime_log(&format!("hermes-{}", process.name));
        let text = std::fs::read_to_string(&log).unwrap_or_default();
        let tail: Vec<&str> = text.lines().rev().take(15).collect();
        println!("-- {} (last lines) --", log.display());
        for line in tail.iter().rev() {
            println!("{line}");
        }
    }

    // Stopped whether or not both came up: nothing of the test's outlives it.
    let released = supervise::release(&dir, Runtime::Hermes, &link.services, &install.processes).await;
    println!("released: {released:?}");
    assert!(all_up, "not both up in time");
    assert!(!released.stopped.is_empty() || !released.uninstalled.is_empty());
    for process in &install.processes {
        let answering = reqwest::get(&process.health.url).await.is_ok();
        assert!(!answering, "{} still answers after release", process.name);
    }
}
