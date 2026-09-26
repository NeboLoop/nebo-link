//! `oal-conformance`: test an OAL host or client against the recorded
//! examples in `spec/examples/`.

use std::net::SocketAddr;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use oal_conformance::{fake_agent, fake_host, spec, transcript};
use serde_json::Value;

#[derive(Parser)]
#[command(
    name = "oal-conformance",
    version,
    about = "Conformance suite for Open Agent Link (OAL) 0.1"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Test a host: drive every example against it as a client. The host
    /// must run `oal-conformance agent` as one of its agents and show a
    /// pairing code.
    Host {
        /// The host's OAL WebSocket URL (through its relay or on the LAN).
        url: String,
        /// A fresh pairing code from the host.
        #[arg(long)]
        code: String,
        /// The id the host gave the `oal-conformance agent` agent.
        #[arg(long)]
        agent: String,
        /// Where pairing goes, when not `url` (a relay's /oal/pair/<code>).
        #[arg(long)]
        pair_url: Option<String>,
        /// An upgrade header, `Name: value` (a relay's Authorization). Repeatable.
        #[arg(long = "header")]
        headers: Vec<String>,
        /// Run only these examples (`pair` and `agents` always run first). Repeatable.
        #[arg(long = "only")]
        only: Vec<String>,
    },
    /// Test a client: serve a fake host with one fake agent until stopped.
    /// Frames the client sends that break the spec are printed.
    Client {
        #[arg(long, default_value = "127.0.0.1:7878")]
        listen: SocketAddr,
        #[arg(long, default_value = "K7QM-3XRD")]
        code: String,
    },
    /// Speak ACP on stdin/stdout as the scripted fake agent, for a host
    /// under test to run.
    Agent,
    /// List the examples.
    Examples,
}

#[tokio::main]
async fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Host {
            url,
            code,
            agent,
            pair_url,
            headers,
            only,
        } => {
            let mut parsed = Vec::new();
            for header in headers {
                let Some((name, value)) = header.split_once(':') else {
                    eprintln!("--header takes `Name: value`, got {header}");
                    return ExitCode::from(2);
                };
                parsed.push((name.trim().to_owned(), value.trim().to_owned()));
            }
            // `pair` and `agents` capture what the others use.
            let mut examples = vec![
                spec::example("pair").expect("pair"),
                spec::example("agents").expect("agents"),
            ];
            for name in &only {
                match spec::example(name) {
                    Some(e) if !examples.iter().any(|x| x.name == e.name) => examples.push(e),
                    Some(_) => {}
                    None => {
                        eprintln!(
                            "There's no example called {name}. `oal-conformance examples` lists them."
                        );
                        return ExitCode::from(2);
                    }
                }
            }
            if only.is_empty() {
                examples = spec::EXAMPLES.iter().collect();
            }
            let target = transcript::Target {
                url,
                pair_url,
                headers: parsed,
                known: vec![
                    ("code".into(), Value::String(code)),
                    ("agent".into(), Value::String(agent)),
                ],
            };
            let outcomes = transcript::run(&target, &examples).await;
            let failed = outcomes.iter().filter(|o| o.result.is_err()).count();
            for outcome in &outcomes {
                match &outcome.result {
                    Ok(()) => println!("PASS {}", outcome.example),
                    Err(e) => println!("FAIL {}: {e}", outcome.example),
                }
            }
            let not_run = examples.len() - outcomes.len();
            if not_run > 0 {
                println!(
                    "{not_run} not run: they need what the failed example captures. A pairing code works once; get a new one."
                );
            }
            println!("{} passed, {failed} failed", outcomes.len() - failed);
            if failed == 0 && not_run == 0 {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            }
        }
        Command::Client { listen, code } => {
            let config = fake_host::Config {
                code: code.clone(),
                log: true,
                ..fake_host::Config::default()
            };
            match fake_host::serve(listen, config).await {
                Ok(addr) => {
                    println!(
                        "Fake OAL host at ws://{addr}/oal. Pairing code {code}; agent `{}` works in {}.",
                        fake_host::AGENT,
                        fake_host::FOLDER
                    );
                    println!(
                        "Prompts: `run: echo hi` asks permission, `wait` runs until cancelled, anything else is echoed."
                    );
                    let _ = tokio::signal::ctrl_c().await;
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("Could not listen on {listen}: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Agent => {
            fake_agent::stdio().await;
            ExitCode::SUCCESS
        }
        Command::Examples => {
            for example in spec::EXAMPLES {
                let doc: Value = serde_json::from_str(example.json).expect("example JSON");
                println!(
                    "{:<20} {}",
                    example.name,
                    doc["title"].as_str().unwrap_or("")
                );
            }
            ExitCode::SUCCESS
        }
    }
}
