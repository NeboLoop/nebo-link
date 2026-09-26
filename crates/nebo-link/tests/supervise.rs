//! The supervisor against fake runtime processes: a process that is down
//! is started; one that ends is started again with backoff; a service the
//! owner installed is left alone; a service the link installs is recorded
//! and is the only one `unlink` removes; a process pairing started is
//! adopted, not started twice.
//!
//! The fake process is [`fake_http`]: it answers `/` with 200 and runs in
//! the foreground, like a runtime's own gateway command.

#![cfg(unix)]

#[path = "common/fake_http.rs"]
mod fake_http;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use nebo_link::endpoints::Endpoints;
use nebo_link::state::{BotDir, Link, ModelsEndpoint, Root};
use nebo_link::supervise::{self, ProcessState, StartedBy, Supervisor};
use nebo_runtimes::{HealthCheck, ManagedProcess, Runtime, RuntimeCommand, ServiceCommand};

const PATIENCE: Duration = Duration::from_secs(30);

struct Fixture {
    _tmp: tempfile::TempDir,
    dir: BotDir,
    port: u16,
    /// Every command the fake service scripts ran, one per line.
    commands: PathBuf,
    /// Where the fake install script writes the service definition.
    definition: PathBuf,
    /// The fake server program.
    server: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let root = Root::at(tmp.path().join("nebo-link"));
        let dir = root.bot("bot-1");
        dir.create().unwrap();
        dir.save(&Link {
            bot_id: "bot-1".into(),
            name: "test · Hermes".into(),
            runtime: Runtime::Hermes,
            owner_id: "owner-1".into(),
            home: tmp.path().join("hh"),
            env: vec![],
            endpoints: Endpoints::from_env(),
            local_password: "pw".into(),
            models: ModelsEndpoint {
                port: 1,
                key: "k".into(),
                enabled: false,
            },
            api_server_key: String::new(),
            services: vec![],
            acp: None,
        })
        .unwrap();
        Self {
            port: free_port(),
            commands: tmp.path().join("commands"),
            definition: tmp.path().join("ai.hermes.gateway.plist"),
            server: fake_http::build(tmp.path()),
            dir,
            _tmp: tmp,
        }
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}/", self.port)
    }

    /// The foreground command: an HTTP server on the fixture's port.
    fn run(&self) -> RuntimeCommand {
        RuntimeCommand {
            program: self.server.display().to_string(),
            args: vec![self.port.to_string()],
            env: vec![],
        }
    }

    /// A script that records its call and does `body`.
    fn script(&self, name: &str, body: &str) -> RuntimeCommand {
        let path = self.commands.parent().unwrap().join(format!("{name}.sh"));
        std::fs::write(
            &path,
            format!("#!/bin/sh\necho \"{name}\" >> '{}'\n{body}\n", self.commands.display()),
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        RuntimeCommand {
            program: path.display().to_string(),
            args: vec![],
            env: vec![],
        }
    }

    /// The runtime's service commands: `install` writes the definition and
    /// starts the server detached; `start` starts it; `uninstall` records.
    fn service(&self) -> ServiceCommand {
        // Detached the way a service manager would leave it (macOS has no
        // `setsid`): backgrounded, its pid kept so the test can end it.
        let serve = format!(
            "nohup '{}' {} >/dev/null 2>&1 &\necho $! >> '{}'",
            self.server.display(),
            self.port,
            self.service_pids().display()
        );
        ServiceCommand {
            definition: self.definition.clone(),
            install: self.script("install", &format!("touch '{}'\n{serve}", self.definition.display())),
            start: self.script("start", &serve),
            uninstall: self.script("uninstall", &format!("rm -f '{}'", self.definition.display())),
        }
    }

    fn service_pids(&self) -> PathBuf {
        self.commands.parent().unwrap().join("service.pids")
    }

    fn process(&self, service: Option<ServiceCommand>) -> ManagedProcess {
        ManagedProcess {
            name: "gateway".into(),
            health: HealthCheck {
                url: self.url(),
                pid_file: None,
            },
            service,
            run: self.run(),
        }
    }

    fn commands(&self) -> Vec<String> {
        std::fs::read_to_string(&self.commands)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn started_pid(&self) -> Option<u32> {
        let file: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(self.dir.processes_file()).ok()?).ok()?;
        file["gateway"]["pid"].as_u64().map(|pid| pid as u32)
    }

    /// Ends every server the fake service started.
    fn stop_service_servers(&self) {
        for pid in std::fs::read_to_string(self.service_pids())
            .unwrap_or_default()
            .lines()
            .filter_map(|l| l.trim().parse::<u32>().ok())
        {
            // SAFETY: signalling a process by id has no memory effects.
            unsafe { libc::kill(pid as libc::pid_t, libc::SIGKILL) };
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Nothing the tests start outlives them.
        if let Some(pid) = self.started_pid() {
            // SAFETY: signalling a process group by id has no memory effects.
            unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
        }
        self.stop_service_servers();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

async fn wait_for(what: &str, mut pred: impl FnMut() -> bool) -> Duration {
    let started = Instant::now();
    while !pred() {
        assert!(started.elapsed() < PATIENCE, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    started.elapsed()
}

fn state(supervisor: &Supervisor) -> Option<ProcessState> {
    supervisor.status().into_iter().find(|p| p.name == "gateway").map(|p| p.state)
}

#[tokio::test]
async fn a_process_that_is_down_is_started_and_started_again_when_it_ends() {
    let fx = Fixture::new();
    let supervisor = Supervisor::new(&fx.dir, Runtime::Hermes, vec![fx.process(None)]);
    let running = tokio::spawn(supervisor.clone().run());

    wait_for("the server to be started", || {
        state(&supervisor) == Some(ProcessState::Running { by: StartedBy::Link })
    })
    .await;
    let pid = fx.started_pid().expect("the started process is recorded");
    assert!(supervise::alive(pid));
    assert!(fx.dir.runtime_log("hermes-gateway").exists(), "its output goes to the bot directory");

    // It ends: started again, after the first backoff.
    let ended = Instant::now();
    supervise::stop(pid).await;
    wait_for("the end to be noticed", || {
        matches!(state(&supervisor), Some(ProcessState::Down { ref why }) if why.contains("fake-http") && why.contains("ended"))
    })
    .await;
    wait_for("the server to be started again", || {
        state(&supervisor) == Some(ProcessState::Running { by: StartedBy::Link })
    })
    .await;
    let again = fx.started_pid().expect("recorded again");
    assert_ne!(again, pid);
    assert!(ended.elapsed() >= supervise::FIRST_BACKOFF, "started again only after the backoff");

    running.abort();
    supervise::stop(again).await;
    assert!(!supervise::alive(again));
}

#[tokio::test]
async fn the_owners_own_service_is_left_alone() {
    let fx = Fixture::new();
    std::fs::write(&fx.definition, "the owner's plist").unwrap();
    let supervisor = Supervisor::new(&fx.dir, Runtime::Hermes, vec![fx.process(Some(fx.service()))]);
    let running = tokio::spawn(supervisor.clone().run());

    wait_for("the state", || state(&supervisor).is_some()).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        state(&supervisor),
        Some(ProcessState::Down {
            why: "Hermes's own gateway service is installed; the link leaves it to that service".into()
        })
    );
    assert!(fx.commands().is_empty(), "no runtime command was run");
    assert!(fx.started_pid().is_none(), "nothing was started in the foreground");
    assert!(std::fs::read_to_string(&fx.definition).unwrap() == "the owner's plist");
    running.abort();

    // Unlinking removes nothing of the owner's.
    let released = supervise::release(&fx.dir, Runtime::Hermes, &[], &[fx.process(Some(fx.service()))]).await;
    assert_eq!(released, supervise::Released::default());
    assert!(fx.commands().is_empty());
    assert!(fx.definition.exists());
}

#[tokio::test]
async fn the_runtimes_service_is_installed_once_recorded_and_removed_by_unlink() {
    let fx = Fixture::new();
    let process = fx.process(Some(fx.service()));
    let supervisor = Supervisor::new(&fx.dir, Runtime::Hermes, vec![process.clone()]);
    let running = tokio::spawn(supervisor.clone().run());

    wait_for("the service to be installed and answering", || {
        state(&supervisor) == Some(ProcessState::Running { by: StartedBy::LinkService })
    })
    .await;
    assert_eq!(fx.commands(), ["install"]);
    assert!(fx.definition.exists());
    assert_eq!(fx.dir.load().unwrap().services, ["gateway"], "recorded in link.json");
    assert!(fx.started_pid().is_none(), "a service, not a foreground process");
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(fx.commands(), ["install"], "installed once");
    running.abort();

    // The link installed it, so unlink removes it.
    let services = fx.dir.load().unwrap().services;
    let released = supervise::release(&fx.dir, Runtime::Hermes, &services, &[process]).await;
    assert_eq!(released.uninstalled, ["gateway"]);
    assert!(released.stopped.is_empty() && released.not_uninstalled.is_empty());
    assert_eq!(fx.commands(), ["install", "uninstall"]);
    assert!(!fx.definition.exists());
    fx.stop_service_servers();
}

#[tokio::test]
async fn a_service_the_link_installed_is_started_when_down() {
    let fx = Fixture::new();
    std::fs::write(&fx.definition, "installed by the link earlier").unwrap();
    let mut link = fx.dir.load().unwrap();
    link.services = vec!["gateway".into()];
    fx.dir.save(&link).unwrap();
    let supervisor = Supervisor::new(&fx.dir, Runtime::Hermes, vec![fx.process(Some(fx.service()))]);
    let running = tokio::spawn(supervisor.clone().run());

    wait_for("the service to be started", || {
        state(&supervisor) == Some(ProcessState::Running { by: StartedBy::LinkService })
    })
    .await;
    assert_eq!(fx.commands(), ["start"], "started, not installed again");
    running.abort();
    fx.stop_service_servers();
}

#[tokio::test]
async fn what_pairing_started_is_adopted_and_stopped_by_unlink() {
    let fx = Fixture::new();
    // Pairing: one pass starts what is down.
    let started = Supervisor::new(&fx.dir, Runtime::Hermes, vec![fx.process(None)])
        .ensure()
        .await;
    assert_eq!(started, ["gateway"]);
    let pid = fx.started_pid().expect("recorded for the service to adopt");

    // The service: adopts it rather than starting a second one.
    let supervisor = Supervisor::new(&fx.dir, Runtime::Hermes, vec![fx.process(None)]);
    let running = tokio::spawn(supervisor.clone().run());
    wait_for("the adopted server to answer", || {
        state(&supervisor) == Some(ProcessState::Running { by: StartedBy::Link })
    })
    .await;
    assert_eq!(fx.started_pid(), Some(pid), "the same process");
    running.abort();

    // Pairing again with it up starts nothing.
    let supervisor = Supervisor::new(&fx.dir, Runtime::Hermes, vec![fx.process(None)]);
    assert_eq!(supervisor.running().await, ["gateway"]);
    assert!(supervisor.ensure().await.is_empty());

    // Unlink stops it and forgets it.
    let released = supervise::release(&fx.dir, Runtime::Hermes, &[], &[fx.process(None)]).await;
    assert_eq!(released.stopped, ["gateway"]);
    assert!(!supervise::alive(pid));
    assert!(!fx.dir.processes_file().exists());
}

#[test]
fn a_recorded_process_that_is_gone_is_forgotten() {
    let fx = Fixture::new();
    std::fs::write(
        fx.dir.processes_file(),
        format!(r#"{{"gateway": {{"pid": {}, "started": 1}}}}"#, u32::MAX - 7),
    )
    .unwrap();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let released = rt.block_on(supervise::release(&fx.dir, Runtime::Hermes, &[], &[fx.process(None)]));
    assert!(released.stopped.is_empty());
    assert!(!fx.dir.processes_file().exists());
}
