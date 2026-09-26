use std::net::SocketAddr;
use std::path::PathBuf;

use crate::{Environment, Runtime, acp, hermes, openclaw};

/// One installed runtime for the current user. A machine running both
/// OpenClaw and Hermes yields two installations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Installation {
    pub runtime: Runtime,
    /// OpenClaw: the state directory (`~/.openclaw`). Hermes: the Hermes
    /// root (`~/.hermes`), which holds the default profile and `profiles/`.
    pub home: PathBuf,
    /// The main config file: OpenClaw `openclaw.json`, Hermes the default
    /// profile's `config.yaml`. It may not exist yet.
    pub config_path: PathBuf,
    /// The runtime's version when it is recorded on disk.
    pub version: Option<String>,
    /// Whether `version` can serve its UI behind the link
    /// ([`Change::ProxyAccess`](crate::Change::ProxyAccess)); `None` when the
    /// version is unknown.
    pub proxy_supported: Option<bool>,
    /// Why the config file could not be read, when it exists but is not
    /// valid. Endpoints then fall back to the runtime's defaults.
    pub config_error: Option<String>,
    /// The local services the link proxies to.
    pub endpoints: Vec<Endpoint>,
    /// Hermes named profiles (the default profile is the installation
    /// itself). Always empty for OpenClaw.
    pub profiles: Vec<Profile>,
    /// The runtime's own command for restarting it, with the environment
    /// overrides that selected this installation. For an ACP agent, the
    /// command that starts it speaking ACP on stdio (it is restarted by
    /// starting it again); `home` is then the agent's program.
    pub restart: RuntimeCommand,
    /// What must be running for the link to serve this installation, the
    /// runtime itself (what `restart` restarts) first.
    pub processes: Vec<ManagedProcess>,
}

/// A process of an installation the link needs up: how to tell it is, the
/// runtime's own service for it when it has one, and the command that runs
/// it in the foreground otherwise. All data; the caller runs the commands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedProcess {
    /// `gateway` or `dashboard`; unique within the installation.
    pub name: String,
    pub health: HealthCheck,
    /// The runtime's own commands for running this process as a service that
    /// starts at login and after a crash; `None` where the runtime has none
    /// for it (the Hermes dashboard; both runtimes on Windows).
    pub service: Option<ServiceCommand>,
    /// Runs the process in the foreground (never returns while it lives).
    pub run: RuntimeCommand,
}

/// How to tell a process is up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthCheck {
    /// A URL the process answers without credentials once it is serving.
    pub url: String,
    /// The runtime's own record of the running process, when it keeps one
    /// (Hermes `<home>/gateway.pid`). A live pid here with no answer at `url`
    /// is a process that is up but not serving the link (for one, a Hermes
    /// gateway started before its API key was written).
    pub pid_file: Option<PathBuf>,
}

/// The runtime's own service for a process. Every command is bounded and
/// touches only the current user's service manager.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceCommand {
    /// The service definition the runtime writes (a launchd plist, a systemd
    /// user unit): present means a service is installed, by whoever did.
    pub definition: PathBuf,
    /// Installs the service and starts it.
    pub install: RuntimeCommand,
    /// Starts the installed service.
    pub start: RuntimeCommand,
    /// Stops and removes the service.
    pub uninstall: RuntimeCommand,
}

/// A Hermes named profile under `<root>/profiles/<name>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Profile {
    pub name: String,
    pub home: PathBuf,
    pub config_path: PathBuf,
}

/// A local HTTP/WebSocket service of an installation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub service: Service,
    /// The address to dial on this machine.
    pub addr: SocketAddr,
    /// The path prefix the service is served under: OpenClaw's current
    /// `gateway.controlUi.basePath`, a Hermes secondary profile's
    /// `/p/<name>` API mirror, or empty.
    pub base_path: String,
}

/// What an [`Endpoint`] serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Service {
    /// The OpenClaw gateway: Control UI, HTTP APIs and the WebSocket
    /// protocol on one port.
    OpenclawGateway {
        /// `gateway.bind` (`loopback` unless configured).
        bind: String,
        /// The effective `gateway.auth.mode`.
        auth_mode: String,
    },
    /// The Hermes web dashboard; one per Hermes root, serving every profile.
    HermesDashboard,
    /// The Hermes OpenAI-compatible API server of one profile.
    HermesApiServer { profile: String },
}

/// A command the caller runs; this crate never runs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCommand {
    pub program: String,
    pub args: Vec<String>,
    /// Variables to set on top of the caller's environment.
    pub env: Vec<(String, String)>,
}

/// Finds the OpenClaw and Hermes installations of the user `env` describes,
/// then the ACP agents installed for them ([`acp`]). Honours each runtime's
/// own home, profile and config-path overrides.
pub fn detect(env: &Environment) -> Vec<Installation> {
    [openclaw::detect(env), hermes::detect(env)]
        .into_iter()
        .flatten()
        .chain(acp::detect(env))
        .collect()
}

/// Whether a dotted version (`2026.9.6`, `0.19.0`) is at least `min`.
/// Pre-release and build suffixes are ignored.
pub(crate) fn version_at_least(version: &str, min: &[u64]) -> Option<bool> {
    let parts: Vec<u64> = version
        .trim()
        .trim_start_matches('v')
        .split(['.', '-', '+'])
        .map_while(|part| part.parse().ok())
        .collect();
    (!parts.is_empty()).then(|| parts.as_slice() >= min)
}
