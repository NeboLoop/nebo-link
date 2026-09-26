//! Keeping the runtime's processes up. A linked Hermes serves the link only
//! while its gateway (the API server) and its dashboard (the UI the proxy
//! opens) run, and neither is a service by default; OpenClaw's gateway is
//! the same in principle. The runtime says what must be running
//! ([`ManagedProcess`]); this module makes it so.
//!
//! For each process, in order of preference:
//!
//! 1. It answers its health URL: nothing to do.
//! 2. The runtime's own service definition exists. Installed by the link
//!    (recorded in `link.json`): started with the runtime's `start`. Installed
//!    by the owner: left alone, and said so. The owner's service manager, not
//!    the link, keeps that process up.
//! 3. No service: the runtime's own `install` command installs one and the
//!    link records that it did, so `unlink` removes exactly that.
//! 4. No service can be installed (the runtime has none for this process,
//!    or its install refused): the foreground command is started detached,
//!    in its own process group, its output in the bot's logs, and started
//!    again with backoff when it exits. What the link started is recorded in
//!    the agent's `processes.json`, so the next link process adopts it and
//!    `unlink` stops it.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use nebo_runtimes::{ManagedProcess, Runtime};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::error::Result;
use crate::install::{self, runtime_name};
use crate::state::{AgentDir, read_json, write_json};

/// How often a process that answers is checked again.
pub const POLL: Duration = Duration::from_secs(30);
/// How often a process that was just started is checked.
const STARTING_POLL: Duration = Duration::from_secs(2);
/// How long a started process gets to answer.
pub const STARTUP_GRACE: Duration = Duration::from_secs(60);
/// First wait before starting a process again after it failed.
pub const FIRST_BACKOFF: Duration = Duration::from_secs(5);
/// Longest wait before starting a process again.
const MAX_BACKOFF: Duration = Duration::from_secs(300);
/// How long a health request may take.
const HEALTH_TIMEOUT: Duration = Duration::from_secs(3);

/// One process, as `nebo-link status` shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessStatus {
    pub name: String,
    #[serde(flatten)]
    pub state: ProcessState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ProcessState {
    /// Answers its health URL.
    Running { by: StartedBy },
    /// Started; not answering yet.
    Starting,
    /// Not answering, and why nothing is (being) done about it.
    Down { why: String },
}

/// Who keeps a running process up.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StartedBy {
    /// The owner, or a service the owner installed.
    Owner,
    /// The link, in the foreground.
    Link,
    /// The runtime's own service, which the link installed.
    LinkService,
}

/// A foreground process the link started, as `processes.json` records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Started {
    pid: u32,
    /// Unix seconds.
    started: u64,
}

type StartedFile = BTreeMap<String, Started>;

/// The processes of one linked installation.
pub struct Supervisor {
    dir: AgentDir,
    runtime_name: &'static str,
    processes: Vec<ManagedProcess>,
    client: reqwest::Client,
    status: Mutex<BTreeMap<String, ProcessState>>,
    /// Bumped each time a process starts answering.
    came_up: watch::Sender<u64>,
}

impl Supervisor {
    /// The processes of the install the agent `dir` names.
    pub fn new(dir: &AgentDir, runtime: Runtime, processes: Vec<ManagedProcess>) -> Arc<Self> {
        Arc::new(Self {
            dir: dir.clone(),
            runtime_name: runtime_name(runtime),
            processes,
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(HEALTH_TIMEOUT)
                .build()
                .expect("http client"),
            status: Mutex::new(BTreeMap::new()),
            came_up: watch::channel(0).0,
        })
    }

    /// Every process, in the runtime's order, as last seen.
    pub fn status(&self) -> Vec<ProcessStatus> {
        let status = self.status.lock().expect("status lock");
        self.processes
            .iter()
            .filter_map(|p| {
                status.get(&p.name).map(|state| ProcessStatus {
                    name: p.name.clone(),
                    state: state.clone(),
                })
            })
            .collect()
    }

    /// Changes whenever a process starts answering: the moment to probe
    /// what it serves.
    pub fn came_up(&self) -> watch::Receiver<u64> {
        self.came_up.subscribe()
    }

    /// Keeps every process up until dropped.
    pub async fn run(self: Arc<Self>) {
        let workers: Vec<_> = self
            .processes
            .iter()
            .map(|process| {
                let mut worker = Worker::new(self.clone(), process.clone());
                tokio::spawn(async move {
                    loop {
                        let wait = worker.step().await;
                        worker.pause(wait).await;
                    }
                })
            })
            .collect();
        futures::future::join_all(workers).await;
    }

    /// One pass over every process, starting what is not answering; for
    /// pairing, so the first link works without the owner starting
    /// anything. Returns the names of the processes it started.
    pub async fn ensure(&self) -> Vec<String> {
        let mut started = Vec::new();
        for process in &self.processes {
            let mut worker = Worker::new(self, process.clone());
            worker.step().await;
            if worker.started {
                started.push(process.name.clone());
            }
        }
        started
    }

    /// The names of the processes running now: answering, or alive by the
    /// runtime's own pid file (a Hermes gateway without its API key yet).
    pub async fn running(&self) -> Vec<String> {
        let mut up = Vec::new();
        for process in &self.processes {
            if healthy(&self.client, &process.health.url).await || recorded_pid(process).is_some() {
                up.push(process.name.clone());
            }
        }
        up
    }

    fn set(&self, name: &str, state: ProcessState) {
        let previous = self.status.lock().expect("status lock").insert(name.to_owned(), state.clone());
        if matches!(state, ProcessState::Running { .. }) && !matches!(previous, Some(ProcessState::Running { .. })) {
            tracing::info!(process = name, "answering");
            self.came_up.send_modify(|n| *n += 1);
        } else if previous.as_ref() != Some(&state) {
            match &state {
                ProcessState::Starting => tracing::info!(process = name, "started"),
                ProcessState::Down { why } => tracing::info!(process = name, why, "not answering"),
                ProcessState::Running { .. } => {}
            }
        }
    }

    fn record_service(&self, name: &str) -> Result<()> {
        self.dir.record_service(name)
    }

    fn link_installed_service(&self, name: &str) -> bool {
        self.dir.services().iter().any(|s| s == name)
    }

    fn started_file(&self) -> StartedFile {
        read_json(&self.dir.processes_file()).unwrap_or_default()
    }

    fn record_started(&self, name: &str, pid: u32) {
        if let Err(e) = self.dir.create() {
            tracing::warn!(error = %e, "could not make the agent's directory");
        }
        let mut file = self.started_file();
        file.insert(
            name.to_owned(),
            Started {
                pid,
                started: unix_now(),
            },
        );
        if let Err(e) = write_json(&self.dir.processes_file(), &file) {
            tracing::warn!(error = %e, "could not record the started process");
        }
    }

    fn forget_started(&self, name: &str) {
        let mut file = self.started_file();
        if file.remove(name).is_some()
            && let Err(e) = write_json(&self.dir.processes_file(), &file)
        {
            tracing::warn!(error = %e, "could not update the started processes");
        }
    }
}

/// A foreground process the link started (or adopted from the link process
/// before it).
struct Own {
    pid: u32,
    /// Present when this link process spawned it.
    child: Option<tokio::process::Child>,
    started: Instant,
}

impl Own {
    fn alive(&mut self) -> bool {
        match &mut self.child {
            Some(child) => matches!(child.try_wait(), Ok(None)),
            None => alive(self.pid),
        }
    }

    fn exit_status(&mut self) -> Option<std::process::ExitStatus> {
        self.child.as_mut().and_then(|c| c.try_wait().ok().flatten())
    }
}

/// The supervision of one process.
struct Worker<S> {
    sup: S,
    process: ManagedProcess,
    own: Option<Own>,
    /// When the runtime's service was told to start it.
    service_starting: Option<Instant>,
    backoff: Duration,
    /// Not before this is anything started again.
    next_attempt: Option<Instant>,
    /// The runtime refused to install its service this time; the process
    /// runs in the foreground instead.
    install_failed: bool,
    /// Whether the last step started the process.
    started: bool,
}

impl<S: std::ops::Deref<Target = Supervisor>> Worker<S> {
    fn new(sup: S, process: ManagedProcess) -> Self {
        let mut worker = Worker {
            sup,
            process,
            own: None,
            service_starting: None,
            backoff: FIRST_BACKOFF,
            next_attempt: None,
            install_failed: false,
            started: false,
        };
        worker.adopt();
        worker
    }

    /// Takes over a process a previous link process started, if it is
    /// still alive.
    fn adopt(&mut self) {
        let Some(record) = self.sup.started_file().remove(&self.process.name) else {
            return;
        };
        if alive(record.pid) {
            let age = Duration::from_secs(unix_now().saturating_sub(record.started));
            self.own = Some(Own {
                pid: record.pid,
                child: None,
                started: Instant::now().checked_sub(age).unwrap_or_else(Instant::now),
            });
        } else {
            self.sup.forget_started(&self.process.name);
        }
    }

    /// Checks the process once, doing the next thing for it, and says how
    /// long to wait before the next check.
    async fn step(&mut self) -> Duration {
        self.started = false;
        let name = self.process.name.clone();
        if healthy(&self.sup.client, &self.process.health.url).await {
            let by = if self.own.is_some() {
                StartedBy::Link
            } else if self.sup.link_installed_service(&name) {
                StartedBy::LinkService
            } else {
                StartedBy::Owner
            };
            self.sup.set(&name, ProcessState::Running { by });
            self.backoff = FIRST_BACKOFF;
            self.next_attempt = None;
            self.service_starting = None;
            return POLL;
        }

        // Started by the link and not answering: still starting, stuck, or gone.
        if let Some(mut own) = self.own.take() {
            let why = if own.alive() {
                if own.started.elapsed() < STARTUP_GRACE {
                    self.own = Some(own);
                    self.sup.set(&name, ProcessState::Starting);
                    return STARTING_POLL;
                }
                stop(own.pid).await;
                format!("did not answer within {} s", STARTUP_GRACE.as_secs())
            } else {
                match own.exit_status() {
                    Some(status) => format!("`{}` ended ({status})", install::shown(&self.process.run)),
                    None => format!("`{}` ended", install::shown(&self.process.run)),
                }
            };
            self.sup.forget_started(&name);
            return self.failed(why);
        }

        // Up by the runtime's own account, but not serving: nothing to start.
        if let Some(pid) = recorded_pid(&self.process) {
            self.sup.set(
                &name,
                ProcessState::Down {
                    why: format!("pid {pid} is up and not serving {}", self.process.health.url),
                },
            );
            return POLL;
        }

        if let Some(since) = self.service_starting {
            if since.elapsed() < STARTUP_GRACE {
                self.sup.set(&name, ProcessState::Starting);
                return STARTING_POLL;
            }
            self.service_starting = None;
            return self.failed(format!("the service did not answer within {} s", STARTUP_GRACE.as_secs()));
        }

        if let Some(at) = self.next_attempt
            && at > Instant::now()
        {
            return at - Instant::now();
        }

        if let Some(service) = &self.process.service {
            let log = self.sup.dir.log(None);
            if service.definition.exists() {
                if self.sup.link_installed_service(&name) {
                    return match install::run(&service.start, install::COMMAND_WAIT, &log).await {
                        Ok(()) => self.service_started(),
                        Err(e) => self.failed(e.to_string()),
                    };
                }
                self.sup.set(
                    &name,
                    ProcessState::Down {
                        why: format!(
                            "{}'s own {name} service is installed; the link leaves it to that service",
                            self.sup.runtime_name
                        ),
                    },
                );
                return POLL;
            }
            if !self.install_failed {
                // Installed means the definition is there: `hermes gateway
                // install` can refuse a home and still exit 0.
                let installed = install::run(&service.install, install::COMMAND_WAIT, &log)
                    .await
                    .and_then(|()| {
                        service.definition.exists().then_some(()).ok_or_else(|| {
                            crate::error::Error::Message(format!(
                                "`{}` wrote no service at {}",
                                install::shown(&service.install),
                                service.definition.display()
                            ))
                        })
                    });
                match installed {
                    Ok(()) => {
                        if let Err(e) = self.sup.record_service(&name) {
                            tracing::warn!(error = %e, "could not record the installed service");
                        }
                        return self.service_started();
                    }
                    Err(e) => {
                        tracing::info!(process = %name, error = %e, "the runtime did not install its service; running it in the foreground");
                        self.install_failed = true;
                    }
                }
            }
        }

        let log = self.sup.dir.log(Some(&name));
        let spawned = install::detached(&self.process.run, &log).and_then(|mut cmd| {
            cmd.spawn()
                .map_err(|e| crate::error::Error::Message(format!("could not run `{}`: {e}", install::shown(&self.process.run))))
        });
        match spawned {
            Ok(child) => {
                let pid = child.id().unwrap_or_default();
                self.sup.record_started(&name, pid);
                self.own = Some(Own {
                    pid,
                    child: Some(child),
                    started: Instant::now(),
                });
                self.started = true;
                self.sup.set(&name, ProcessState::Starting);
                STARTING_POLL
            }
            Err(e) => self.failed(e.to_string()),
        }
    }

    /// Waits `wait`, or less if a process this link started ends first.
    async fn pause(&mut self, wait: Duration) {
        match self.own.as_mut().and_then(|own| own.child.as_mut()) {
            Some(child) => {
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {}
                    _ = child.wait() => {}
                }
            }
            None => tokio::time::sleep(wait).await,
        }
    }

    fn service_started(&mut self) -> Duration {
        self.service_starting = Some(Instant::now());
        self.started = true;
        self.sup.set(&self.process.name, ProcessState::Starting);
        STARTING_POLL
    }

    fn failed(&mut self, why: String) -> Duration {
        self.sup.set(&self.process.name, ProcessState::Down { why });
        let wait = self.backoff;
        self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
        self.next_attempt = Some(Instant::now() + wait);
        wait
    }
}

/// What `unlink` undid.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Released {
    /// Foreground processes the link had started, now stopped.
    pub stopped: Vec<String>,
    /// Services the link had installed, now removed.
    pub uninstalled: Vec<String>,
    /// Why a service the link installed could not be removed.
    pub not_uninstalled: Vec<String>,
}

/// Stops every foreground process the link started and removes every
/// service the link installed, never one the owner had. `services` are the
/// process names whose service the link installed (`link.json`).
pub async fn release(dir: &AgentDir, services: &[String], processes: &[ManagedProcess]) -> Released {
    let mut released = Released::default();
    let started: StartedFile = read_json(&dir.processes_file()).unwrap_or_default();
    for (name, record) in started {
        if alive(record.pid) {
            stop(record.pid).await;
            released.stopped.push(name);
        }
    }
    let _ = std::fs::remove_file(dir.processes_file());
    let log = dir.log(None);
    for process in processes.iter().filter(|p| services.contains(&p.name)) {
        let Some(service) = &process.service else { continue };
        match install::run(&service.uninstall, install::COMMAND_WAIT, &log).await {
            Ok(()) => released.uninstalled.push(process.name.clone()),
            Err(e) => released.not_uninstalled.push(e.to_string()),
        }
    }
    released
}

/// Says a process's state in the owner's words.
pub fn describe(status: &ProcessStatus) -> String {
    match &status.state {
        ProcessState::Running { by: StartedBy::Owner } => "running".to_owned(),
        ProcessState::Running { by: StartedBy::Link } => "running, started by the link".to_owned(),
        ProcessState::Running {
            by: StartedBy::LinkService,
        } => "running, as a service the link installed".to_owned(),
        ProcessState::Starting => "starting".to_owned(),
        ProcessState::Down { why } => format!("not answering: {why}"),
    }
}

async fn healthy(client: &reqwest::Client, url: &str) -> bool {
    match client.get(url).send().await {
        Ok(response) => response.status().is_success() || response.status().is_redirection(),
        Err(_) => false,
    }
}

/// The pid in the runtime's own pid file, when that process is alive and
/// is the runtime (a pid file left from before a reboot can name any
/// process by now).
fn recorded_pid(process: &ManagedProcess) -> Option<u32> {
    let path = process.health.pid_file.as_deref()?;
    let pid: u32 = std::fs::read_to_string(path).ok()?.trim().parse().ok()?;
    (pid != 0 && command_line(pid).is_some_and(|line| line.contains(&process.run.program))).then_some(pid)
}

/// The command line of a live process.
fn command_line(pid: u32) -> Option<String> {
    #[cfg(unix)]
    let output = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
        .ok()?;
    #[cfg(not(unix))]
    let output = std::process::Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
        .output()
        .ok()?;
    let line = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!line.is_empty()).then_some(line)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// Whether a process with this pid exists.
pub fn alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // SAFETY: kill with signal 0 only checks that the process exists.
        let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
        rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(not(unix))]
    {
        std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH"])
            .output()
            .is_ok_and(|o| String::from_utf8_lossy(&o.stdout).contains(&format!(" {pid} ")))
    }
}

/// Stops a process the link started: its whole process group is asked to
/// end, and made to after ten seconds.
pub async fn stop(pid: u32) {
    #[cfg(unix)]
    {
        // SAFETY: signalling a process group by id has no memory effects.
        unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(10);
        while alive(pid) && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if alive(pid) {
            // SAFETY: as above.
            unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .output()
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn states_in_the_owners_words() {
        let status = |state| ProcessStatus {
            name: "gateway".into(),
            state,
        };
        assert_eq!(describe(&status(ProcessState::Running { by: StartedBy::Owner })), "running");
        assert_eq!(
            describe(&status(ProcessState::Running { by: StartedBy::Link })),
            "running, started by the link"
        );
        assert_eq!(describe(&status(ProcessState::Starting)), "starting");
        assert_eq!(
            describe(&status(ProcessState::Down { why: "`x` ended".into() })),
            "not answering: `x` ended"
        );
    }

    #[test]
    fn status_serializes_flat() {
        let status = ProcessStatus {
            name: "gateway".into(),
            state: ProcessState::Running { by: StartedBy::LinkService },
        };
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json, serde_json::json!({"name": "gateway", "state": "running", "by": "link_service"}));
        assert_eq!(serde_json::from_value::<ProcessStatus>(json).unwrap(), status);
    }

    #[test]
    fn this_process_is_alive_and_a_dead_pid_is_not() {
        assert!(alive(std::process::id()));
        // The largest pid the kernel could hand out is far below this.
        assert!(!alive(u32::MAX - 7));
        assert!(command_line(std::process::id()).is_some());
        assert_eq!(command_line(u32::MAX - 7), None);
    }

    #[test]
    fn a_pid_file_counts_only_when_it_names_the_runtime() {
        let tmp = tempfile::tempdir().unwrap();
        let pid_file = tmp.path().join("gateway.pid");
        let process = |program: &str| ManagedProcess {
            name: "gateway".into(),
            health: nebo_runtimes::HealthCheck {
                url: "http://127.0.0.1:9/health".into(),
                pid_file: Some(pid_file.clone()),
            },
            service: None,
            run: nebo_runtimes::RuntimeCommand {
                program: program.into(),
                args: vec![],
                env: vec![],
            },
        };
        assert_eq!(recorded_pid(&process("hermes")), None, "no pid file yet");
        std::fs::write(&pid_file, std::process::id().to_string()).unwrap();
        assert_eq!(recorded_pid(&process("no-such-runtime-program")), None, "a live pid that is not the runtime");
        // This test binary's own command line names itself.
        let me = std::env::current_exe().unwrap();
        let me = me.file_name().unwrap().to_str().unwrap().to_owned();
        assert_eq!(recorded_pid(&process(&me)), Some(std::process::id()));
        std::fs::write(&pid_file, (u32::MAX - 7).to_string()).unwrap();
        assert_eq!(recorded_pid(&process(&me)), None, "a dead pid");
    }
}
