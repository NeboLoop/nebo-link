use std::net::SocketAddr;
use std::path::PathBuf;

use crate::{Environment, Runtime, hermes, openclaw};

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
    /// overrides that selected this installation.
    pub restart: RuntimeCommand,
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

/// Finds the OpenClaw and Hermes installations of the user `env` describes.
/// Honours each runtime's own home, profile and config-path overrides.
pub fn detect(env: &Environment) -> Vec<Installation> {
    [openclaw::detect(env), hermes::detect(env)]
        .into_iter()
        .flatten()
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
