//! Running `nebo-link run --bot <id>` as a background service that starts at
//! login or boot and restarts on failure:
//!
//! - macOS: a launchd user agent.
//! - Linux: a systemd user unit (with lingering, so it runs without a login
//!   session); as root, a system unit.
//! - Windows: a scheduled task that starts at logon as the owner and restarts
//!   on failure. It runs as the owner, like the launchd agent and the
//!   systemd user unit, so it sees the owner's runtimes and files.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// What the service runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    pub bot_id: String,
    /// The `nebo-link` binary.
    pub exe: PathBuf,
    /// `--home` to pass on, when the link's state is not in the default place.
    pub home: Option<PathBuf>,
    /// The owner's `PATH`, so the runtime's own commands (e.g. `openclaw`)
    /// are found from the service.
    pub path: Option<String>,
}

impl Spec {
    /// The service's command line.
    pub fn args(&self) -> Vec<String> {
        let mut args = vec![self.exe.display().to_string()];
        if let Some(home) = &self.home {
            args.extend(["--home".into(), home.display().to_string()]);
        }
        args.extend(["run".into(), "--bot".into(), self.bot_id.clone()]);
        args
    }
}

/// The service's name for `bot_id`.
pub fn name(bot_id: &str) -> String {
    if cfg!(target_os = "macos") {
        format!("com.neboai.link.{bot_id}")
    } else if cfg!(windows) {
        format!("NeboLink-{bot_id}")
    } else {
        format!("nebo-link-{bot_id}.service")
    }
}

/// Installs and starts the service; replaces one installed before.
pub fn install(spec: &Spec) -> Result<()> {
    platform::install(spec)
}

/// Removes and stops the service. Removing one that isn't installed is fine.
///
/// `itself`: this process is the service, removing itself because NeboAI
/// removed its bot. Stopping the service may end this process, so its
/// definition is removed before it is stopped, and on Windows it is not
/// stopped at all: this process exits on its own once the task is deleted.
pub fn uninstall(bot_id: &str, itself: bool) -> Result<()> {
    platform::uninstall(bot_id, itself)
}

/// Restarts the service, so it runs the binary now on disk.
pub fn restart(bot_id: &str) -> Result<()> {
    platform::restart(bot_id)
}

/// Whether the service is installed.
pub fn installed(bot_id: &str) -> bool {
    platform::definition(bot_id).is_some_and(|path| path.exists())
}

fn run(program: &str, args: &[&str]) -> Result<()> {
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .map_err(|e| Error::Service(format!("could not run {program}: {e}")))?;
    if output.status.success() {
        return Ok(());
    }
    Err(Error::Service(format!(
        "`{program} {}` failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    )))
}

fn write(path: &Path, text: &str) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| Error::io(dir, e))?;
    }
    std::fs::write(path, text).map_err(|e| Error::io(path, e))
}

fn remove(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(Error::io(path, e)),
        _ => Ok(()),
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// launchd property list for a user agent.
pub fn launchd_plist(spec: &Spec) -> String {
    let args: String = spec
        .args()
        .iter()
        .map(|a| format!("    <string>{}</string>\n", xml_escape(a)))
        .collect();
    let env = spec
        .path
        .as_ref()
        .map(|path| {
            format!(
                "  <key>EnvironmentVariables</key>\n  <dict>\n    <key>PATH</key>\n    <string>{}</string>\n  </dict>\n",
                xml_escape(path)
            )
        })
        .unwrap_or_default();
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{label}</string>
  <key>ProgramArguments</key>
  <array>
{args}  </array>
{env}  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>10</integer>
  <key>ProcessType</key>
  <string>Background</string>
</dict>
</plist>
"#,
        label = xml_escape(&format!("com.neboai.link.{}", spec.bot_id)),
    )
}

/// systemd unit; `system` for a system unit (root), otherwise a user unit.
pub fn systemd_unit(spec: &Spec, system: bool) -> String {
    let quote = |a: &str| format!("\"{}\"", a.replace('\\', "\\\\").replace('"', "\\\"").replace('%', "%%"));
    let exec = spec.args().iter().map(|a| quote(a)).collect::<Vec<_>>().join(" ");
    let env = spec
        .path
        .as_ref()
        .map(|path| format!("Environment={}\n", quote(&format!("PATH={path}"))))
        .unwrap_or_default();
    format!(
        "[Unit]\nDescription=Nebo Link ({bot})\nWants=network-online.target\nAfter=network-online.target\n\n\
         [Service]\nExecStart={exec}\n{env}Restart=always\nRestartSec=5\n\n\
         [Install]\nWantedBy={target}\n",
        bot = spec.bot_id,
        target = if system { "multi-user.target" } else { "default.target" },
    )
}

/// Task Scheduler definition: at the owner's logon, restart on failure,
/// no time limit, never stopped for battery.
pub fn windows_task(spec: &Spec, user: &str) -> String {
    let args = spec.args();
    let rest = args[1..]
        .iter()
        .map(|a| format!("\"{}\"", a.replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>Nebo Link ({bot})</Description></RegistrationInfo>
  <Triggers><LogonTrigger><Enabled>true</Enabled><UserId>{user}</UserId></LogonTrigger></Triggers>
  <Principals><Principal id="Author"><UserId>{user}</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <RestartOnFailure><Interval>PT1M</Interval><Count>999</Count></RestartOnFailure>
    <StartWhenAvailable>true</StartWhenAvailable>
    <Hidden>true</Hidden>
  </Settings>
  <Actions Context="Author"><Exec><Command>{exe}</Command><Arguments>{rest}</Arguments></Exec></Actions>
</Task>
"#,
        bot = xml_escape(&spec.bot_id),
        user = xml_escape(user),
        exe = xml_escape(&args[0]),
        rest = xml_escape(&rest),
    )
}

#[cfg(target_os = "macos")]
mod platform {
    use super::*;

    fn domain() -> String {
        // SAFETY: getuid has no preconditions and cannot fail.
        format!("gui/{}", unsafe { libc::getuid() })
    }

    pub fn definition(bot_id: &str) -> Option<PathBuf> {
        dirs::home_dir().map(|h| h.join("Library/LaunchAgents").join(format!("{}.plist", name(bot_id))))
    }

    pub fn install(spec: &Spec) -> Result<()> {
        let plist = definition(&spec.bot_id).ok_or_else(|| Error::Service("no home directory".into()))?;
        let _ = run("launchctl", &["bootout", &format!("{}/{}", domain(), name(&spec.bot_id))]);
        write(&plist, &launchd_plist(spec))?;
        run("launchctl", &["bootstrap", &domain(), &plist.display().to_string()])
    }

    pub fn restart(bot_id: &str) -> Result<()> {
        run("launchctl", &["kickstart", "-k", &format!("{}/{}", domain(), name(bot_id))])
    }

    pub fn uninstall(bot_id: &str, _itself: bool) -> Result<()> {
        if let Some(plist) = definition(bot_id) {
            remove(&plist)?;
        }
        let _ = run("launchctl", &["bootout", &format!("{}/{}", domain(), name(bot_id))]);
        Ok(())
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
mod platform {
    use super::*;

    fn is_root() -> bool {
        // SAFETY: geteuid has no preconditions and cannot fail.
        unsafe { libc::geteuid() == 0 }
    }

    fn system_unit(bot_id: &str) -> PathBuf {
        PathBuf::from("/etc/systemd/system").join(name(bot_id))
    }

    fn user_unit(bot_id: &str) -> Option<PathBuf> {
        dirs::config_dir().map(|c| c.join("systemd/user").join(name(bot_id)))
    }

    pub fn definition(bot_id: &str) -> Option<PathBuf> {
        if is_root() { Some(system_unit(bot_id)) } else { user_unit(bot_id) }
    }

    pub fn install(spec: &Spec) -> Result<()> {
        let unit = name(&spec.bot_id);
        if is_root() {
            write(&system_unit(&spec.bot_id), &systemd_unit(spec, true))?;
            run("systemctl", &["daemon-reload"])?;
            return run("systemctl", &["enable", "--now", &unit]);
        }
        if run("systemctl", &["--user", "show-environment"]).is_err() {
            return Err(Error::Service(
                "systemd user services are not available for this account. Run `nebo-link <code>` as root to install a system service instead.".into(),
            ));
        }
        let path = user_unit(&spec.bot_id).ok_or_else(|| Error::Service("no config directory".into()))?;
        write(&path, &systemd_unit(spec, false))?;
        run("systemctl", &["--user", "daemon-reload"])?;
        run("systemctl", &["--user", "enable", "--now", &unit])?;
        // Without lingering, user services stop at logout and don't start at
        // boot. Allowed for one's own account on most systems.
        if let Err(e) = run("loginctl", &["enable-linger"]) {
            tracing::warn!(error = %e, "could not enable lingering; the link runs only while you are logged in");
        }
        Ok(())
    }

    pub fn restart(bot_id: &str) -> Result<()> {
        let unit = name(bot_id);
        if is_root() {
            return run("systemctl", &["restart", &unit]);
        }
        run("systemctl", &["--user", "restart", &unit])
    }

    pub fn uninstall(bot_id: &str, _itself: bool) -> Result<()> {
        let unit = name(bot_id);
        if is_root() {
            let _ = run("systemctl", &["disable", &unit]);
            remove(&system_unit(bot_id))?;
            run("systemctl", &["daemon-reload"])?;
            let _ = run("systemctl", &["stop", &unit]);
            return Ok(());
        }
        let _ = run("systemctl", &["--user", "disable", &unit]);
        if let Some(path) = user_unit(bot_id) {
            remove(&path)?;
        }
        let _ = run("systemctl", &["--user", "daemon-reload"]);
        let _ = run("systemctl", &["--user", "stop", &unit]);
        Ok(())
    }
}

#[cfg(windows)]
mod platform {
    use super::*;

    pub fn definition(bot_id: &str) -> Option<PathBuf> {
        dirs::data_local_dir().map(|d| d.join("nebo-link").join(format!("{}.xml", name(bot_id))))
    }

    pub fn install(spec: &Spec) -> Result<()> {
        let task = name(&spec.bot_id);
        let user = match (std::env::var("USERDOMAIN"), std::env::var("USERNAME")) {
            (Ok(domain), Ok(user)) => format!("{domain}\\{user}"),
            (_, Ok(user)) => user,
            _ => return Err(Error::Service("could not tell which Windows user this is".into())),
        };
        let path = definition(&spec.bot_id).ok_or_else(|| Error::Service("no local data directory".into()))?;
        // Task Scheduler reads UTF-16 task files.
        let xml = windows_task(spec, &user);
        let mut bytes = vec![0xFF, 0xFE];
        bytes.extend(xml.encode_utf16().flat_map(u16::to_le_bytes));
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| Error::io(dir, e))?;
        }
        std::fs::write(&path, bytes).map_err(|e| Error::io(&path, e))?;
        run("schtasks", &["/Create", "/TN", &task, "/XML", &path.display().to_string(), "/F"])?;
        run("schtasks", &["/Run", "/TN", &task])
    }

    pub fn restart(bot_id: &str) -> Result<()> {
        let task = name(bot_id);
        // /Run is ignored while the task runs (MultipleInstancesPolicy
        // IgnoreNew), so end it first.
        let _ = run("schtasks", &["/End", "/TN", &task]);
        run("schtasks", &["/Run", "/TN", &task])
    }

    pub fn uninstall(bot_id: &str, itself: bool) -> Result<()> {
        let task = name(bot_id);
        if !itself {
            let _ = run("schtasks", &["/End", "/TN", &task]);
        }
        let _ = run("schtasks", &["/Delete", "/TN", &task, "/F"]);
        match definition(bot_id) {
            Some(path) => remove(&path),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> Spec {
        Spec {
            bot_id: "b1".into(),
            exe: "/opt/nebo link/nebo-link".into(),
            home: Some("/srv/link state".into()),
            path: Some("/usr/local/bin:/usr/bin".into()),
        }
    }

    #[test]
    fn command_line_passes_home_then_runs_the_bot() {
        assert_eq!(
            spec().args(),
            ["/opt/nebo link/nebo-link", "--home", "/srv/link state", "run", "--bot", "b1"]
        );
        let bare = Spec { home: None, ..spec() };
        assert_eq!(bare.args(), ["/opt/nebo link/nebo-link", "run", "--bot", "b1"]);
    }

    #[test]
    fn launchd_agent_restarts_and_starts_at_login() {
        let plist = launchd_plist(&spec());
        assert!(plist.contains("<string>com.neboai.link.b1</string>"));
        assert!(plist.contains("<string>/opt/nebo link/nebo-link</string>\n    <string>--home</string>"));
        assert!(plist.contains("<key>RunAtLoad</key>\n  <true/>"));
        assert!(plist.contains("<key>KeepAlive</key>\n  <true/>"));
        assert!(plist.contains("<string>/usr/local/bin:/usr/bin</string>"));
    }

    #[test]
    fn systemd_units_quote_and_restart() {
        let user = systemd_unit(&spec(), false);
        assert!(user.contains(
            r#"ExecStart="/opt/nebo link/nebo-link" "--home" "/srv/link state" "run" "--bot" "b1""#
        ));
        assert!(user.contains("Restart=always"));
        assert!(user.contains(r#"Environment="PATH=/usr/local/bin:/usr/bin""#));
        assert!(user.contains("WantedBy=default.target"));
        assert!(systemd_unit(&spec(), true).contains("WantedBy=multi-user.target"));
    }

    #[test]
    fn windows_task_runs_at_logon_and_restarts() {
        let task = windows_task(&spec(), r"HOST\owner");
        assert!(task.contains(r"<UserId>HOST\owner</UserId></LogonTrigger>"));
        assert!(task.contains("<RestartOnFailure>"));
        assert!(task.contains("<Command>/opt/nebo link/nebo-link</Command>"));
        assert!(task.contains(r#"<Arguments>&quot;--home&quot; &quot;/srv/link state&quot; &quot;run&quot; &quot;--bot&quot; &quot;b1&quot;</Arguments>"#));
    }
}
