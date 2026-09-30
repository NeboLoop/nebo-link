//! Running `nebo-link run --bot <id>` as a background service that starts at
//! login or boot and restarts on failure:
//!
//! - macOS: a launchd user agent.
//! - Linux: a systemd user unit (with lingering, so it runs without a login
//!   session); as root, a system unit.
//! - Windows: a scheduled task that starts at logon as the owner and restarts
//!   on failure. It runs as the owner, like the launchd agent and the
//!   systemd user unit, so it sees the owner's runtimes and files. It opens
//!   no window: see [`windows_task`].

use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::{Error, Result};

/// The file in the bot's logs folder the service's own output goes to:
/// what it writes before its log opens, and a crash's last words. Its log
/// proper is the daily file beside it.
pub const OUTPUT_LOG: &str = "service.log";

/// How long the service manager lets the service stop before it kills it:
/// time for a running prompt to finish ([`crate::run::AGENT_GRACE`]) and for
/// its agents to stop.
const STOP_TIMEOUT_SECS: u64 = crate::run::AGENT_GRACE.as_secs() + 30;

/// What stopping takes besides a prompt's grace: the agents' processes
/// asked to end, then made to, and the rest of the service.
const STOP_MARGIN: Duration = Duration::from_secs(10);

/// Makes the installed definition of this service the one this version
/// writes, when the service runs from it (an update changes what it
/// should say: its log, how long it may take to stop), and says how long a
/// running prompt may take to finish when this run stops. A definition the
/// service manager loaded holds until it next loads the service, so an
/// outdated one bounds this run by the manager's default stop timeout.
pub fn refresh(spec: &Spec) -> Duration {
    let full = crate::run::AGENT_GRACE;
    if !platform::running_as_service(&spec.bot_id) {
        return full;
    }
    let (Some(path), Some(current)) = (platform::definition(&spec.bot_id), platform::text(spec)) else {
        return full;
    };
    let installed = std::fs::read(&path).ok().and_then(|bytes| decode(&bytes));
    let grace = grace_under(installed.as_deref(), &current, platform::DEFAULT_STOP_TIMEOUT);
    if let Some(installed) = installed.as_deref().filter(|text| *text != current) {
        match write(&path, &current).and_then(|()| platform::reread(&spec.bot_id)) {
            Ok(Reread::NextLoad) => tracing::info!(path = %path.display(), "the service's definition was out of date; rewritten, it takes effect when the service is next loaded"),
            Ok(Reread::RunEnds) => {
                tracing::info!(path = %path.display(), "the service's definition was out of date; rewritten, the service starts again from it");
                std::process::exit(0);
            }
            Err(e) => {
                tracing::warn!(error = %e, "could not rewrite the service's out-of-date definition");
                // The service manager still has the old one: the file says
                // so again, and the next start tries again.
                let _ = write(&path, installed);
            }
        }
    }
    grace
}

/// When a rewritten definition takes effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reread {
    /// The next time the service manager loads the service (launchd,
    /// systemd).
    #[cfg_attr(windows, allow(dead_code))]
    NextLoad,
    /// Once this run ends: Task Scheduler has a run of the new definition
    /// waiting for it, so this run exits.
    #[cfg_attr(not(windows), allow(dead_code))]
    RunEnds,
}

/// A running prompt's grace for a service started from `installed` when
/// this version writes `current`: the full grace under the current
/// definition, and what fits the manager's `default_timeout` under an older
/// one.
fn grace_under(installed: Option<&str>, current: &str, default_timeout: Duration) -> Duration {
    let full = crate::run::AGENT_GRACE;
    match installed {
        Some(text) if text != current => default_timeout.saturating_sub(STOP_MARGIN).min(full),
        _ => full,
    }
}

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
    /// The bot's logs folder, where the service's output goes
    /// ([`OUTPUT_LOG`]).
    pub logs: PathBuf,
}

impl Spec {
    fn output_log(&self) -> String {
        self.logs.join(OUTPUT_LOG).display().to_string()
    }
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
    // The service manager opens the output log before the service runs.
    std::fs::create_dir_all(&spec.logs).map_err(|e| Error::io(&spec.logs, e))?;
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
    let output = command::new::<std::process::Command>(program, command::Console::Hidden)
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
    std::fs::write(path, encode(text)).map_err(|e| Error::io(path, e))
}

/// A definition as its service manager reads it: Task Scheduler reads
/// UTF-16 task files (with a byte-order mark), launchd and systemd UTF-8.
fn encode(text: &str) -> Vec<u8> {
    if cfg!(windows) {
        let mut bytes = vec![0xFF, 0xFE];
        bytes.extend(text.encode_utf16().flat_map(u16::to_le_bytes));
        bytes
    } else {
        text.as_bytes().to_vec()
    }
}

/// A definition file's text, UTF-16 (with its byte-order mark) or UTF-8.
fn decode(bytes: &[u8]) -> Option<String> {
    match bytes {
        [0xFF, 0xFE, rest @ ..] => {
            let units: Vec<u16> = rest.chunks_exact(2).map(|c| u16::from_le_bytes([c[0], c[1]])).collect();
            String::from_utf16(&units).ok()
        }
        _ => String::from_utf8(bytes.to_vec()).ok(),
    }
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
  <key>ExitTimeOut</key>
  <integer>{stop}</integer>
  <key>StandardOutPath</key>
  <string>{log}</string>
  <key>StandardErrorPath</key>
  <string>{log}</string>
  <key>ProcessType</key>
  <string>Background</string>
</dict>
</plist>
"#,
        label = xml_escape(&format!("com.neboai.link.{}", spec.bot_id)),
        stop = STOP_TIMEOUT_SECS,
        log = xml_escape(&spec.output_log()),
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
    let log = spec.output_log().replace('%', "%%");
    format!(
        "[Unit]\nDescription=Nebo Link ({bot})\nWants=network-online.target\nAfter=network-online.target\n\n\
         [Service]\nExecStart={exec}\n{env}Restart=always\nRestartSec=5\nTimeoutStopSec={stop}\n\
         StandardOutput=append:{log}\nStandardError=append:{log}\n\n\
         [Install]\nWantedBy={target}\n",
        bot = spec.bot_id,
        stop = STOP_TIMEOUT_SECS,
        target = if system { "multi-user.target" } else { "default.target" },
    )
}

/// Task Scheduler definition: at the owner's logon, restart on failure,
/// no time limit, never stopped for battery, and no window.
///
/// `nebo-link` is a console program (its commands print to the terminal
/// they are run from), and a console program Task Scheduler starts gets a
/// console window of its own; `<Hidden>` hides only the task in Task
/// Scheduler's list. So the task runs it under `conhost.exe --headless`:
/// the console host every console program gets, here without a window
/// (Windows 10 1809 and later; headless, it never hands the console to
/// Windows Terminal either). The service and everything it starts share
/// that windowless console, and the host runs as long as any of them does,
/// so a service that updates itself in place is still the task's run.
///
/// `Queue`: a run asked for while one runs starts when it ends. A service
/// whose definition this version rewrote asks for that run and exits
/// ([`refresh`]); a restart ends the run before asking.
pub fn windows_task(spec: &Spec, user: &str) -> String {
    let mut args = spec.args();
    args[0] = without_verbatim_prefix(&args[0]);
    let client = args.iter().map(|a| windows_quote(a)).collect::<Vec<_>>().join(" ");
    format!(
        r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>Nebo Link ({bot})</Description></RegistrationInfo>
  <Triggers><LogonTrigger><Enabled>true</Enabled><UserId>{user}</UserId></LogonTrigger></Triggers>
  <Principals><Principal id="Author"><UserId>{user}</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings>
    <MultipleInstancesPolicy>Queue</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <RestartOnFailure><Interval>PT1M</Interval><Count>999</Count></RestartOnFailure>
    <StartWhenAvailable>true</StartWhenAvailable>
    <Hidden>true</Hidden>
  </Settings>
  <Actions Context="Author"><Exec><Command>{host}</Command><Arguments>--headless {client}</Arguments></Exec></Actions>
</Task>
"#,
        bot = xml_escape(&spec.bot_id),
        user = xml_escape(user),
        host = xml_escape(WINDOWS_CONSOLE_HOST),
        client = xml_escape(&client),
    )
}

/// The windowless console host the Windows service runs under
/// ([`windows_task`]). Task Scheduler expands the variable.
pub const WINDOWS_CONSOLE_HOST: &str = r"%SystemRoot%\System32\conhost.exe";

/// One argument quoted for a Windows command line so that programs split
/// it back as given (`CommandLineToArgvW`, Rust's own parsing): backslashes
/// are literal except before a quote, where they and the quote are escaped.
fn windows_quote(arg: &str) -> String {
    let mut quoted = String::from('"');
    let mut backslashes = 0;
    for c in arg.chars() {
        match c {
            '\\' => backslashes += 1,
            '"' => {
                quoted.extend(std::iter::repeat_n('\\', backslashes * 2 + 1));
                quoted.push('"');
                backslashes = 0;
            }
            _ => {
                quoted.extend(std::iter::repeat_n('\\', backslashes));
                quoted.push(c);
                backslashes = 0;
            }
        }
    }
    quoted.extend(std::iter::repeat_n('\\', backslashes * 2));
    quoted.push('"');
    quoted
}

/// `\\?\C:\…` as `C:\…` and `\\?\UNC\host\…` as `\\host\…`: a canonical
/// Windows path is verbatim, which a command line does not need and not
/// every program reads.
fn without_verbatim_prefix(path: &str) -> String {
    if let Some(rest) = path.strip_prefix(r"\\?\UNC\") {
        return format!(r"\\{rest}");
    }
    match path.strip_prefix(r"\\?\") {
        Some(rest) if rest.as_bytes().get(1) == Some(&b':') => rest.to_owned(),
        _ => path.to_owned(),
    }
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

    /// launchd's `ExitTimeOut` when a definition doesn't say.
    pub const DEFAULT_STOP_TIMEOUT: Duration = Duration::from_secs(20);

    pub fn text(spec: &Spec) -> Option<String> {
        Some(launchd_plist(spec))
    }

    /// launchd names the job it runs in `XPC_SERVICE_NAME`.
    pub fn running_as_service(bot_id: &str) -> bool {
        std::env::var("XPC_SERVICE_NAME").is_ok_and(|n| n == name(bot_id))
    }

    /// launchd reads a definition when it loads the service.
    pub fn reread(_bot_id: &str) -> Result<Reread> {
        Ok(Reread::NextLoad)
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

    /// systemd's `DefaultTimeoutStopSec`.
    pub const DEFAULT_STOP_TIMEOUT: Duration = Duration::from_secs(90);

    pub fn text(spec: &Spec) -> Option<String> {
        Some(systemd_unit(spec, is_root()))
    }

    /// systemd marks every process it starts with `INVOCATION_ID`, and the
    /// unit it runs in `/proc/self/cgroup`.
    pub fn running_as_service(bot_id: &str) -> bool {
        std::env::var_os("INVOCATION_ID").is_some()
            && std::fs::read_to_string("/proc/self/cgroup").is_ok_and(|c| c.contains(&name(bot_id)))
    }

    /// systemd takes a changed unit once told to read it again.
    pub fn reread(_bot_id: &str) -> Result<Reread> {
        if is_root() { run("systemctl", &["daemon-reload"])? } else { run("systemctl", &["--user", "daemon-reload"])? }
        Ok(Reread::NextLoad)
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

    /// Task Scheduler ends a task at once.
    pub const DEFAULT_STOP_TIMEOUT: Duration = Duration::ZERO;

    fn user() -> Result<String> {
        match (std::env::var("USERDOMAIN"), std::env::var("USERNAME")) {
            (Ok(domain), Ok(user)) => Ok(format!("{domain}\\{user}")),
            (_, Ok(user)) => Ok(user),
            _ => Err(Error::Service("could not tell which Windows user this is".into())),
        }
    }

    /// The task file as this version writes it. Task Scheduler runs what it
    /// registered from that file, so the file says what the task runs.
    pub fn text(spec: &Spec) -> Option<String> {
        user().ok().map(|user| windows_task(spec, &user))
    }

    /// Task Scheduler tells what it starts nothing. A bot's task file is
    /// there only while its task is installed, and then the task is what
    /// runs the bot's connection.
    pub fn running_as_service(bot_id: &str) -> bool {
        definition(bot_id).is_some_and(|path| path.exists())
    }

    /// A run keeps the definition it started with, window and all: register
    /// the rewritten file and ask for a run, which Task Scheduler starts
    /// when this one ends (`Queue`). This run then exits.
    pub fn reread(bot_id: &str) -> Result<Reread> {
        register(bot_id)?;
        run("schtasks", &["/Run", "/TN", &name(bot_id)])?;
        Ok(Reread::RunEnds)
    }

    fn register(bot_id: &str) -> Result<()> {
        let path = definition(bot_id).ok_or_else(|| Error::Service("no local data directory".into()))?;
        run("schtasks", &["/Create", "/TN", &name(bot_id), "/XML", &path.display().to_string(), "/F"])
    }

    pub fn install(spec: &Spec) -> Result<()> {
        let path = definition(&spec.bot_id).ok_or_else(|| Error::Service("no local data directory".into()))?;
        write(&path, &windows_task(spec, &user()?))?;
        // One installed before runs until it is ended; a run asked for
        // meanwhile would wait for it (Queue).
        let _ = run("schtasks", &["/End", "/TN", &name(&spec.bot_id)]);
        register(&spec.bot_id)?;
        run("schtasks", &["/Run", "/TN", &name(&spec.bot_id)])
    }

    pub fn restart(bot_id: &str) -> Result<()> {
        let task = name(bot_id);
        // A run asked for while one runs waits for it (Queue), so end it
        // first.
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
            logs: "/srv/link state/b1/logs".into(),
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
        assert!(plist.contains("<key>StandardOutPath</key>\n  <string>/srv/link state/b1/logs/service.log</string>"));
        assert!(plist.contains("<key>StandardErrorPath</key>\n  <string>/srv/link state/b1/logs/service.log</string>"));
        assert!(plist.contains("<key>ExitTimeOut</key>\n  <integer>90</integer>"));
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
        assert!(user.contains("StandardOutput=append:/srv/link state/b1/logs/service.log\n"));
        assert!(user.contains("StandardError=append:/srv/link state/b1/logs/service.log\n"));
        assert!(user.contains("TimeoutStopSec=90\n"));
        assert!(systemd_unit(&spec(), true).contains("WantedBy=multi-user.target"));
    }

    /// A service started from an older definition stops within the service
    /// manager's default timeout; one started from this version's has the
    /// whole grace.
    #[test]
    fn a_prompts_grace_fits_the_definition_the_service_runs_under() {
        let full = crate::run::AGENT_GRACE;
        let launchd = Duration::from_secs(20);
        assert_eq!(grace_under(Some("old"), "new", launchd), Duration::from_secs(10));
        assert_eq!(grace_under(Some("new"), "new", launchd), full);
        assert_eq!(grace_under(None, "new", launchd), full, "run by hand, not as a service");
        assert_eq!(grace_under(Some("old"), "new", Duration::from_secs(90)), full);
        assert_eq!(grace_under(Some("old"), "new", Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn windows_task_runs_at_logon_and_restarts() {
        let task = windows_task(&spec(), r"HOST\owner");
        assert!(task.contains(r"<UserId>HOST\owner</UserId></LogonTrigger>"));
        assert!(task.contains("<RestartOnFailure>"));
        assert!(task.contains("<MultipleInstancesPolicy>Queue</MultipleInstancesPolicy>"));
    }

    /// The owner saw terminals open on Windows: the service runs under the
    /// windowless console host, never as the task's own program.
    #[test]
    fn windows_task_runs_the_link_under_a_windowless_console_host() {
        let task = windows_task(&spec(), r"HOST\owner");
        assert!(task.contains(r"<Command>%SystemRoot%\System32\conhost.exe</Command>"));
        assert!(task.contains(
            r#"<Arguments>--headless &quot;/opt/nebo link/nebo-link&quot; &quot;--home&quot; &quot;/srv/link state&quot; &quot;run&quot; &quot;--bot&quot; &quot;b1&quot;</Arguments>"#
        ));
        let canonical = Spec { exe: r"\\?\C:\Program Files\Nebo Link\nebo-link.exe".into(), home: None, ..spec() };
        assert!(windows_task(&canonical, "owner").contains(
            r#"<Arguments>--headless &quot;C:\Program Files\Nebo Link\nebo-link.exe&quot; &quot;run&quot;"#
        ));
    }

    #[test]
    fn windows_arguments_split_back_as_given() {
        assert_eq!(windows_quote("plain"), r#""plain""#);
        assert_eq!(windows_quote(r"C:\state dir\"), r#""C:\state dir\\""#);
        assert_eq!(windows_quote(r#"say "hi""#), r#""say \"hi\"""#);
        assert_eq!(windows_quote(r#"a\"b"#), r#""a\\\"b""#);
        assert_eq!(without_verbatim_prefix(r"\\?\C:\x\nebo-link.exe"), r"C:\x\nebo-link.exe");
        assert_eq!(without_verbatim_prefix(r"\\?\UNC\host\share\nebo-link.exe"), r"\\host\share\nebo-link.exe");
        assert_eq!(without_verbatim_prefix("/opt/nebo-link"), "/opt/nebo-link");
    }

    /// Task files are UTF-16: one written reads back as written, so a
    /// current definition is never taken for an outdated one.
    #[test]
    fn definitions_read_back_as_written() {
        let text = windows_task(&spec(), "owner");
        assert_eq!(decode(&encode(&text)).as_deref(), Some(text.as_str()));
        let mut utf16 = vec![0xFF, 0xFE];
        utf16.extend("ä<Task/>".encode_utf16().flat_map(u16::to_le_bytes));
        assert_eq!(decode(&utf16).as_deref(), Some("ä<Task/>"));
        assert_eq!(decode("plain".as_bytes()).as_deref(), Some("plain"));
    }
}
