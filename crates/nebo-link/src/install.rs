//! The link's side of `nebo-runtimes`: choosing the installation to link,
//! finding it again, and running the runtime's own restart command.

use std::net::SocketAddr;
use std::path::Path;

use nebo_runtimes::{Environment, Installation, Runtime, RuntimeCommand, Service, detect};

use crate::error::{Error, Result};
use crate::state::InstallLink;

/// The key the hub stores in `bots.runtime`.
pub fn runtime_key(runtime: Runtime) -> &'static str {
    match runtime {
        Runtime::Openclaw => "openclaw",
        Runtime::Hermes => "hermes",
        Runtime::Acp(agent) => agent.key(),
    }
}

/// The runtime's name as the owner knows it.
pub fn runtime_name(runtime: Runtime) -> &'static str {
    match runtime {
        Runtime::Openclaw => "OpenClaw",
        Runtime::Hermes => "Hermes",
        Runtime::Acp(agent) => agent.name(),
    }
}

/// Picks the installation to link from those detected. `wanted` narrows it
/// to one runtime; `linked` are the homes already linked to a bot, which are
/// never linked twice. Without `wanted` only OpenClaw and Hermes are picked:
/// an ACP agent (Claude Code, Codex, ...) is linked when named.
pub fn choose(
    installs: Vec<Installation>,
    wanted: Option<Runtime>,
    linked: &[std::path::PathBuf],
) -> Result<Installation> {
    let acp: Vec<Runtime> = installs.iter().map(|i| i.runtime).filter(|r| r.acp().is_some()).collect();
    let found: Vec<Installation> = installs
        .into_iter()
        .filter(|i| match wanted {
            Some(w) => i.runtime == w,
            None => i.runtime.acp().is_none(),
        })
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
                None if !acp.is_empty() => format!(
                    "No OpenClaw or Hermes install found for this user. To link a coding agent, name it:\n{}",
                    acp.iter()
                        .map(|r| format!("  --runtime {}  ({})", runtime_key(*r), runtime_name(*r)))
                        .collect::<Vec<_>>()
                        .join("\n")
                ),
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
    if install.runtime.acp().is_some() {
        // No UI and no settings of its own to open: it only has to start.
        return match &install.config_error {
            Some(problem) => Err(Error::Message(problem.clone())),
            None => Ok(install),
        };
    }
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

/// Finds the `runtime` installation a hosted agent names, with the
/// environment overrides that selected it when it was linked (a service does
/// not inherit the owner's shell).
pub fn find(runtime: Runtime, install: &InstallLink) -> Result<Installation> {
    let mut env = Environment::current();
    env.vars.extend(install.env.iter().cloned());
    detect(&env)
        .into_iter()
        .find(|i| i.runtime == runtime && i.home == install.home)
        .ok_or_else(|| {
            Error::Message(format!(
                "Could not find {} at {}. Is it still installed?",
                runtime_name(runtime),
                install.home.display()
            ))
        })
}

/// How long a runtime's own command gets to return before the link treats
/// it as the runtime itself running in the foreground.
pub const COMMAND_WAIT: std::time::Duration = std::time::Duration::from_secs(30);

/// Runs one of the runtime's own commands (restart, service install, start,
/// uninstall) and waits up to `wait` for it. Its output goes to `log`.
///
/// A service command returns at once. Without a service, `hermes gateway
/// restart` *is* the gateway, in the foreground, and never returns; the first
/// live run hung the link on it. A command still running after `wait` is
/// therefore the runtime, up: it is left running in its own process group so
/// it outlives the link, and the command counts as done.
pub async fn run(command: &RuntimeCommand, wait: std::time::Duration, log: &Path) -> Result<()> {
    let shown = shown(command);
    let mut cmd = detached(command, log)?;
    let mut child = cmd
        .spawn()
        .map_err(|e| Error::Message(format!("Could not run `{shown}`: {e}")))?;
    match tokio::time::timeout(wait, child.wait()).await {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => Err(Error::Message(match last_line(log) {
            Some(line) => format!("`{shown}` failed ({status}): {line}"),
            None => format!("`{shown}` failed ({status}). Run it yourself to see why."),
        })),
        Ok(Err(e)) => Err(Error::Message(format!("Could not run `{shown}`: {e}"))),
        Err(_) => {
            tracing::info!(command = %shown, "still running; the runtime is up in the foreground and left running");
            Ok(())
        }
    }
}

/// The command as the owner would type it.
pub fn shown(command: &RuntimeCommand) -> String {
    std::iter::once(command.program.as_str())
        .chain(command.args.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join(" ")
}

/// A command of the runtime's, set up to outlive the link: its own process
/// group, never killed when dropped, its output appended to `log`.
pub fn detached(command: &RuntimeCommand, log: &Path) -> Result<tokio::process::Command> {
    let out = log_file(log)?;
    let err = out.try_clone().map_err(|e| Error::io(log, e))?;
    let mut cmd = tokio::process::Command::new(&command.program);
    cmd.args(&command.args)
        .envs(command.env.iter().cloned())
        .stdin(std::process::Stdio::null())
        .stdout(out)
        .stderr(err)
        .kill_on_drop(false);
    #[cfg(unix)]
    cmd.process_group(0);
    Ok(cmd)
}

/// Longest a runtime's log grows before it is started over.
const LOG_LIMIT: u64 = 5 * 1024 * 1024;

fn log_file(path: &Path) -> Result<std::fs::File> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| Error::io(dir, e))?;
    }
    let oversized = std::fs::metadata(path).is_ok_and(|m| m.len() > LOG_LIMIT);
    std::fs::OpenOptions::new()
        .create(true)
        .append(!oversized)
        .write(true)
        .truncate(oversized)
        .open(path)
        .map_err(|e| Error::io(path, e))
}

/// The last non-empty line of `log`, for saying why a command failed.
fn last_line(log: &Path) -> Option<String> {
    let text = std::fs::read_to_string(log).ok()?;
    text.lines().rev().map(str::trim).find(|l| !l.is_empty()).map(str::to_owned)
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
            Runtime::Hermes | Runtime::Acp(_) => Service::HermesDashboard,
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
            processes: vec![],
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

    #[test]
    fn coding_agents_are_linked_when_named() {
        use nebo_runtimes::acp::Agent;
        let mut claude = install(Runtime::Acp(Agent::ClaudeCode), "/bin/claude", None);
        claude.endpoints.clear();
        let with_openclaw = vec![install(Runtime::Openclaw, "/o", Some(true)), claude.clone()];
        // Unnamed: OpenClaw, as before a coding agent was installed.
        assert_eq!(choose(with_openclaw.clone(), None, &[]).unwrap().runtime, Runtime::Openclaw);
        // Named: no UI needed.
        let got = choose(with_openclaw, Some(Runtime::Acp(Agent::ClaudeCode)), &[]).unwrap();
        assert_eq!(got.home, std::path::PathBuf::from("/bin/claude"));
        // Alone and unnamed: the owner is told how to name it.
        let err = choose(vec![claude.clone()], None, &[]).unwrap_err().to_string();
        assert!(err.contains("--runtime claude-code  (Claude Code)"), "{err}");
        // Installed but unable to start: the reason.
        claude.config_error = Some("Claude Code needs Node.js".into());
        let err = choose(vec![claude], Some(Runtime::Acp(Agent::ClaudeCode)), &[]).unwrap_err().to_string();
        assert!(err.contains("needs Node.js"), "{err}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_command_says_why() {
        let tmp = tempfile::tempdir().unwrap();
        let log = tmp.path().join("logs").join("runtime.log");
        let command = |program: &str, args: &[&str]| RuntimeCommand {
            program: program.into(),
            args: args.iter().map(|a| (*a).to_owned()).collect(),
            env: vec![],
        };
        run(&command("true", &[]), COMMAND_WAIT, &log).await.unwrap();
        let err = run(&command("sh", &["-c", "echo nope >&2; exit 3"]), COMMAND_WAIT, &log)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("`sh -c echo nope >&2; exit 3` failed") && err.ends_with(": nope"), "{err}");
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
        let tmp = tempfile::tempdir().unwrap();
        let started = std::time::Instant::now();
        run(&command, std::time::Duration::from_millis(300), &tmp.path().join("runtime.log"))
            .await
            .unwrap();
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
    }
}
