//! The link's side of `nebo-runtimes`: choosing the installation to link,
//! finding it again, and running the runtime's own restart command.

use std::net::SocketAddr;

use nebo_runtimes::{Environment, Installation, Runtime, RuntimeCommand, Service, detect};

use crate::error::{Error, Result};
use crate::state::Link;

/// The key the hub stores in `bots.runtime`.
pub fn runtime_key(runtime: Runtime) -> &'static str {
    match runtime {
        Runtime::Openclaw => "openclaw",
        Runtime::Hermes => "hermes",
    }
}

/// The runtime's name as the owner knows it.
pub fn runtime_name(runtime: Runtime) -> &'static str {
    match runtime {
        Runtime::Openclaw => "OpenClaw",
        Runtime::Hermes => "Hermes",
    }
}

/// Picks the installation to link from those detected. `wanted` narrows it
/// to one runtime; `linked` are the homes already linked to a bot, which are
/// never linked twice.
pub fn choose(
    installs: Vec<Installation>,
    wanted: Option<Runtime>,
    linked: &[std::path::PathBuf],
) -> Result<Installation> {
    let found: Vec<Installation> = installs
        .into_iter()
        .filter(|i| wanted.is_none_or(|w| i.runtime == w))
        .collect();
    let mut free: Vec<Installation> = found
        .iter()
        .filter(|i| !linked.contains(&i.home))
        .cloned()
        .collect();
    let install = match free.len() {
        0 if !found.is_empty() => {
            return Err(Error::Message(format!(
                "{} at {} is already linked. Run `nebo-link status` to see it, or `nebo-link unlink --bot <id>` first.",
                runtime_name(found[0].runtime),
                found[0].home.display()
            )));
        }
        0 => {
            return Err(Error::Message(match wanted {
                Some(w) => format!("No {} install found for this user.", runtime_name(w)),
                None => "No OpenClaw or Hermes install found for this user.".into(),
            }));
        }
        1 => free.remove(0),
        _ => {
            return Err(Error::Message(format!(
                "Several agents are installed here. Link one per run with --runtime:\n{}",
                free.iter()
                    .map(|i| format!(
                        "  --runtime {}  ({} at {})",
                        runtime_key(i.runtime),
                        runtime_name(i.runtime),
                        i.home.display()
                    ))
                    .collect::<Vec<_>>()
                    .join("\n")
            )));
        }
    };
    if install.proxy_supported == Some(false) {
        return Err(Error::Message(format!(
            "{} {} can't be opened from NeboAI yet. Update it, then run this again.",
            runtime_name(install.runtime),
            install.version.as_deref().unwrap_or("")
        )));
    }
    if ui_addr(&install).is_none() {
        return Err(Error::Message(format!(
            "{} at {} has no local UI to open.",
            runtime_name(install.runtime),
            install.home.display()
        )));
    }
    Ok(install)
}

/// The loopback address of the installation's own UI: the OpenClaw gateway
/// or the Hermes dashboard.
pub fn ui_addr(install: &Installation) -> Option<SocketAddr> {
    install
        .endpoints
        .iter()
        .find(|e| matches!(e.service, Service::OpenclawGateway { .. } | Service::HermesDashboard))
        .map(|e| e.addr)
}

/// Finds the installation `link` names, with the environment overrides that
/// selected it at pairing (a service does not inherit the owner's shell).
pub fn find(link: &Link) -> Result<Installation> {
    let mut env = Environment::current();
    env.vars.extend(link.env.iter().cloned());
    detect(&env)
        .into_iter()
        .find(|i| i.runtime == link.runtime && i.home == link.home)
        .ok_or_else(|| {
            Error::Message(format!(
                "Could not find {} at {}. Is it still installed?",
                runtime_name(link.runtime),
                link.home.display()
            ))
        })
}

/// How long a runtime's restart command gets to return before the link
/// treats it as the runtime itself running in the foreground.
pub const RESTART_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Runs the runtime's own restart command and waits up to `wait` for it.
///
/// A restart of a service returns at once. Without a service, `hermes
/// gateway restart` *is* the gateway, in the foreground, and never returns;
/// the first live run hung the link on it. A command still running after
/// `wait` is therefore the runtime, up: it is left running in its own process
/// group so it outlives the link, and the restart counts as done.
pub async fn restart(command: &RuntimeCommand, wait: std::time::Duration) -> Result<()> {
    let shown = std::iter::once(command.program.as_str())
        .chain(command.args.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join(" ");
    let mut cmd = tokio::process::Command::new(&command.program);
    cmd.args(&command.args)
        .envs(command.env.iter().cloned())
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(false);
    #[cfg(unix)]
    cmd.process_group(0);
    let mut child = cmd
        .spawn()
        .map_err(|e| Error::Message(format!("Could not run `{shown}`: {e}")))?;
    match tokio::time::timeout(wait, child.wait()).await {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => Err(Error::Message(format!(
            "`{shown}` failed ({status}). Run it yourself to see why."
        ))),
        Ok(Err(e)) => Err(Error::Message(format!("Could not run `{shown}`: {e}"))),
        Err(_) => {
            tracing::info!(command = %shown, "still running; the runtime is up in the foreground and left running");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebo_runtimes::Endpoint;

    fn install(runtime: Runtime, home: &str, supported: Option<bool>) -> Installation {
        let service = match runtime {
            Runtime::Openclaw => Service::OpenclawGateway {
                bind: "loopback".into(),
                auth_mode: "token".into(),
            },
            Runtime::Hermes => Service::HermesDashboard,
        };
        Installation {
            runtime,
            home: home.into(),
            config_path: format!("{home}/config").into(),
            version: Some("1.0".into()),
            proxy_supported: supported,
            config_error: None,
            endpoints: vec![Endpoint {
                service,
                addr: "127.0.0.1:18789".parse().unwrap(),
                base_path: String::new(),
            }],
            profiles: vec![],
            restart: RuntimeCommand {
                program: "true".into(),
                args: vec![],
                env: vec![],
            },
        }
    }

    #[test]
    fn one_install_is_chosen() {
        let got = choose(vec![install(Runtime::Hermes, "/h", Some(true))], None, &[]).unwrap();
        assert_eq!(got.runtime, Runtime::Hermes);
    }

    #[test]
    fn several_need_a_runtime_and_list_the_choices() {
        let both = || {
            vec![
                install(Runtime::Openclaw, "/o", Some(true)),
                install(Runtime::Hermes, "/h", Some(true)),
            ]
        };
        let err = choose(both(), None, &[]).unwrap_err().to_string();
        assert!(err.contains("--runtime openclaw") && err.contains("--runtime hermes"), "{err}");
        assert_eq!(choose(both(), Some(Runtime::Openclaw), &[]).unwrap().home, std::path::PathBuf::from("/o"));
        // An install already linked is not offered again.
        assert_eq!(choose(both(), None, &["/o".into()]).unwrap().runtime, Runtime::Hermes);
    }

    #[test]
    fn refusals_say_what_to_do() {
        assert!(choose(vec![], None, &[]).unwrap_err().to_string().contains("No OpenClaw or Hermes"));
        let linked = choose(vec![install(Runtime::Hermes, "/h", Some(true))], None, &["/h".into()]);
        assert!(linked.unwrap_err().to_string().contains("already linked"));
        let old = choose(vec![install(Runtime::Hermes, "/h", Some(false))], None, &[]);
        assert!(old.unwrap_err().to_string().contains("Update it"));
        // Unknown version: allowed, the UI will tell.
        assert!(choose(vec![install(Runtime::Hermes, "/h", None)], None, &[]).is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn restart_reports_failure() {
        let command = |program: &str| RuntimeCommand {
            program: program.into(),
            args: vec![],
            env: vec![],
        };
        restart(&command("true"), RESTART_WAIT).await.unwrap();
        assert!(restart(&command("false"), RESTART_WAIT).await.is_err());
    }

    /// `hermes gateway restart` without a service is the gateway itself and
    /// never returns; the link must not hang on it.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_command_that_keeps_running_is_the_runtime_up() {
        let command = RuntimeCommand {
            program: "sleep".into(),
            args: vec!["30".into()],
            env: vec![],
        };
        let started = std::time::Instant::now();
        restart(&command, std::time::Duration::from_millis(300)).await.unwrap();
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }
}
