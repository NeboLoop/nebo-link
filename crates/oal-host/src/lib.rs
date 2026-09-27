//! Open Agent Link (OAL) host: a computer's agents, served to clients over
//! OAL (`spec/oal-0.1.md`). It carries a [`link_core::host::Host`] to
//! clients: the host channel (`host/*`), each agent's channel carrying ACP
//! unchanged, and the host's rules for many clients on one agent, all in the
//! types link-core already speaks.
//!
//! Every connection is end-to-end encrypted ([`oal_secure`]): a device pairs
//! once with a code (CPace, then Noise, then `host/pair` inside), and every
//! later connection is a Noise IK session keyed by the device's static key.
//! The host takes no plaintext connection: one whose first message is text
//! is closed with 4001 (spec 17.2). The one exception is a client in the
//! host's own process ([`OalHost::connect_local`]), whose frames nothing
//! carries.
//!
//! - [`OalHost`]: the host's identity, its paired devices and pairing codes,
//!   and [`OalHost::serve`] for one client connection, whatever carries it:
//!   a relay's stream, the LAN, or a tunnel that carries whole WebSocket
//!   connections ([`Via::Tunnel`], NeboAI's `/t/<botId>/oal`).
//! - [`relay`]: the host's tunnel to a relay (self-hosted `oal-relay`, or
//!   NeboAI's), kept up with backoff; client connections arrive through it.
//! - [`lan`]: the host's own `wss://…/oal` on the local network, with a
//!   self-signed certificate and DNS-SD. Off unless the owner turns it on.
//! - [`wire`]: how a connection reaches [`OalHost::serve`].
//!
//! On an encrypted connection the handshake replaces `host/hello` (spec
//! 17.2): message 2's payload is `{"protocol","device":{"id","name"}}`, or
//! `{"error":{…}}` (a version mismatch, then close 4002).

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use link_core::host::Host;
use link_core::model::{self, DeviceRef, ErrorObject, code};
use oal_relay::host::HostHandle;
use oal_secure::{KeyStore, PairingCode, PublicKey, Side};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::watch;

mod conn;
pub mod lan;
pub mod relay;
pub mod wire;

pub use wire::{Incoming, Outgoing, Wire};

/// The OAL version this host speaks.
pub const PROTOCOL: &str = "0.1";
/// The largest frame the host accepts (spec 4.1: at least 4 MiB).
pub const MAX_FRAME: usize = oal_secure::DEFAULT_MAX_FRAME;
/// How long a pairing code stays good (spec 6.2: at most 10 minutes).
pub const CODE_LIFETIME: Duration = Duration::from_secs(10 * 60);
/// Failed pairing attempts a code survives (spec 6.2).
const CODE_FAILURES: u32 = 5;
/// Failed pairing attempts per minute across all connections (spec 6.2).
const FAILURES_PER_MINUTE: usize = 10;

/// One runtime the host can run (`host/info` `runtimes`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Runtime {
    /// `claude-code`, `codex`, `openclaw`, ...
    pub id: String,
    pub name: String,
    /// `acp`, or the adapter's name (`openclaw`, `hermes`).
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// Who the host is and where it keeps its state.
pub struct Config {
    /// The host's stable id: its relay path and its encryption prologue.
    pub host_id: String,
    /// What the owner calls the computer.
    pub host_name: String,
    /// `host/info` `software`: `nebo-link` and its version.
    pub software: (String, String),
    /// The host's key and its paired devices. The embedder opens it: an
    /// app that is also a client (Nebo) keeps its one key and every pairing,
    /// host or device, in the one store.
    pub keys: KeyStore,
    /// When each device was last seen.
    pub seen_file: PathBuf,
    /// The runtimes the host runs, asked each time `host/info` is.
    pub runtimes: Arc<dyn Fn() -> Vec<Runtime> + Send + Sync>,
}

/// How a connection reached the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Via {
    /// The host's own listener on the LAN.
    Lan,
    /// A tunnel that carries whole WebSocket connections to the host
    /// (NeboAI's, at `/t/<botId>/oal`): pairings and sessions are told apart
    /// by their first message, as on the LAN.
    Tunnel,
    /// Through the relay: the client key it verified, and the nameplate of a
    /// pairing connection.
    Relay { client_key: String, nameplate: Option<String> },
}

/// A device paired with the host (`host/devices`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    pub id: String,
    pub name: String,
    pub paired_at: String,
    pub last_seen_at: String,
    /// Its X25519 static key (base64url): the key its sessions prove.
    #[serde(skip)]
    pub key: String,
}

/// The OAL host over one link-core host.
pub struct OalHost {
    config: Config,
    host: Arc<Host>,
    keys: KeyStore,
    state: Mutex<State>,
    /// Set when the host shuts down: every connection closes with 1001.
    closing: watch::Sender<bool>,
}

#[derive(Default)]
struct State {
    pairing: Option<Active>,
    /// When pairing attempts failed lately.
    failures: VecDeque<Instant>,
    /// When each device was last seen (RFC 3339), by device id.
    seen: HashMap<String, String>,
    relay: Option<HostHandle>,
    /// The LAN certificate's fingerprint, while LAN direct is on.
    fingerprint: Option<String>,
}

/// The pairing code the host is showing.
struct Active {
    code: PairingCode,
    expires: Instant,
    failures: u32,
}

impl OalHost {
    /// Opens the host's key store (made on first use) and its record of when
    /// devices were last seen.
    pub fn new(config: Config, host: Arc<Host>) -> Result<Arc<Self>, String> {
        let keys = config.keys.clone();
        let seen = std::fs::read_to_string(&config.seen_file)
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default();
        let (closing, _) = watch::channel(false);
        Ok(Arc::new(Self {
            config,
            host,
            keys,
            state: Mutex::new(State {
                seen,
                ..State::default()
            }),
            closing,
        }))
    }

    pub fn host(&self) -> &Arc<Host> {
        &self.host
    }

    pub fn host_id(&self) -> &str {
        &self.config.host_id
    }

    pub fn host_name(&self) -> &str {
        &self.config.host_name
    }

    /// The host's X25519 static public key (base64url).
    pub fn public_key(&self) -> String {
        self.keys.public_key().to_string()
    }

    /// The host's key store, whose key also proves the host to its relay.
    pub fn keys(&self) -> &KeyStore {
        &self.keys
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("oal host state")
    }

    /// `host/info` (spec 7.1).
    pub fn info(&self) -> Value {
        let mut host = json!({ "id": self.config.host_id, "name": self.config.host_name, "publicKey": self.public_key() });
        if let Some(fingerprint) = &self.lock().fingerprint {
            host["tlsFingerprint"] = json!(fingerprint);
        }
        let mut runtimes = (self.config.runtimes)();
        runtimes.dedup_by(|a, b| a.id == b.id);
        json!({
            "host": host,
            "software": { "name": self.config.software.0, "version": self.config.software.1 },
            "protocol": { "min": PROTOCOL, "max": PROTOCOL },
            "acp": { "protocolVersion": 1 },
            "runtimes": runtimes,
            "maxFrameBytes": MAX_FRAME,
            "attachments": { "schemes": [], "maxBytes": 0 },
        })
    }

    /// Makes a new pairing code, replacing any other: the nameplate from the
    /// relay when the host's tunnel is up (so the relay routes the pairing),
    /// else made here; the secret half always here. It works once, for
    /// [`CODE_LIFETIME`] or until the relay lets the nameplate go.
    pub async fn pairing_code(&self) -> Result<PairingCode, String> {
        let relay = self.lock().relay.clone();
        let (code, lifetime) = match relay {
            Some(relay) => {
                let nameplate = relay
                    .nameplate(None)
                    .await
                    .map_err(|e| format!("the relay did not give a code: {e}"))?;
                let code = PairingCode::generate(Some(&nameplate.nameplate)).map_err(|e| e.to_string())?;
                let left = model::unix_seconds(&nameplate.expires_at)
                    .map(|at| Duration::from_secs_f64((at - unix_now()).max(0.0)))
                    .unwrap_or(CODE_LIFETIME);
                (code, left.min(CODE_LIFETIME))
            }
            None => (PairingCode::generate(None).map_err(|e| e.to_string())?, CODE_LIFETIME),
        };
        self.lock().pairing = Some(Active {
            code: code.clone(),
            expires: Instant::now() + lifetime,
            failures: 0,
        });
        tracing::info!(nameplate = code.nameplate(), "oal: a pairing code is showing");
        Ok(code)
    }

    /// The code a pairing connection may use now, unless too many attempts
    /// failed.
    fn active_code(&self) -> Option<PairingCode> {
        let mut state = self.lock();
        let minute_ago = Instant::now() - Duration::from_secs(60);
        while state.failures.front().is_some_and(|at| *at < minute_ago) {
            state.failures.pop_front();
        }
        if state.failures.len() >= FAILURES_PER_MINUTE {
            return None;
        }
        match &state.pairing {
            Some(active) if active.expires > Instant::now() && active.failures < CODE_FAILURES => Some(active.code.clone()),
            _ => None,
        }
    }

    /// Counts a failed pairing attempt against the code and the minute.
    fn pairing_failed(&self) {
        let mut state = self.lock();
        state.failures.push_back(Instant::now());
        if let Some(active) = &mut state.pairing {
            active.failures += 1;
        }
    }

    /// Takes the code: a pairing succeeded with it. `false` if it was taken
    /// or replaced meanwhile.
    fn take_code(&self, code: &PairingCode) -> bool {
        let mut state = self.lock();
        match &state.pairing {
            Some(active) if &active.code == code => {
                state.pairing = None;
                true
            }
            _ => false,
        }
    }

    /// Every paired device.
    pub fn devices(&self) -> Vec<Device> {
        let seen = self.lock().seen.clone();
        self.keys
            .peers()
            .into_iter()
            .filter(|p| p.side == Side::Client)
            .map(|p| {
                let paired_at = model::rfc3339(p.paired_at as i64, 0);
                Device {
                    last_seen_at: seen.get(&p.id).cloned().unwrap_or_else(|| paired_at.clone()),
                    id: p.id,
                    name: p.name,
                    paired_at,
                    key: p.public_key.to_string(),
                }
            })
            .collect()
    }

    /// Notes that `device` was seen now.
    fn seen(&self, device: &str) {
        let seen = {
            let mut state = self.lock();
            state.seen.insert(device.to_owned(), model::now());
            state.seen.clone()
        };
        if let Err(e) = write_private(&self.config.seen_file, &serde_json::to_vec_pretty(&seen).unwrap_or_default()) {
            tracing::debug!(error = %e, "oal: could not note when a device was seen");
        }
    }

    /// Unpairs a device: its key is revoked (its open sessions end with
    /// 4003), and the relay stops letting it through.
    pub async fn unpair(&self, device_id: &str) -> Result<(), ErrorObject> {
        let device = self
            .devices()
            .into_iter()
            .find(|d| d.id == device_id)
            .ok_or_else(|| ErrorObject::new(code::INVALID_PARAMS, "There's no such device."))?;
        let key: PublicKey = device.key.parse().map_err(|_| ErrorObject::new(code::INTERNAL, "That device's key is damaged."))?;
        self.keys
            .revoke(&key)
            .map_err(|e| ErrorObject::new(code::INTERNAL, format!("Could not unpair {}: {e}", device.name)))?;
        self.lock().seen.remove(device_id);
        let relay = self.lock().relay.clone();
        if let Some(relay) = relay
            && let Err(e) = relay.unpair(&device.key).await
        {
            tracing::info!(error = %e, "oal: the relay did not take the unpairing; it will on the next tunnel");
        }
        tracing::info!(device = %device.id, "oal: a device was unpaired");
        Ok(())
    }

    /// The relay's handle while the tunnel is up.
    pub(crate) fn set_relay(&self, relay: Option<HostHandle>) {
        self.lock().relay = relay;
    }

    pub(crate) fn relay(&self) -> Option<HostHandle> {
        self.lock().relay.clone()
    }

    /// Whether the relay's tunnel is up.
    pub fn relay_connected(&self) -> bool {
        self.lock().relay.is_some()
    }

    pub(crate) fn set_fingerprint(&self, fingerprint: Option<String>) {
        self.lock().fingerprint = fingerprint;
    }

    /// Closes every connection with 1001 (the host is shutting down or
    /// restarting): clients reconnect.
    pub fn shutdown(&self) {
        let _ = self.closing.send(true);
    }

    fn closing(&self) -> watch::Receiver<bool> {
        self.closing.subscribe()
    }

    /// Starts every agent that can be started, so `host/agents` says what
    /// each can do (a coding agent's capabilities come from its own
    /// `initialize`), then tells clients what changed.
    pub async fn warm_up(&self) {
        let agents = self.host.agents().await;
        for agent in &agents {
            if let Err(why) = self.host.ready_agent(&agent.id).await {
                tracing::info!(agent = %agent.id, why, "oal: an agent could not be started");
            }
        }
        self.host.refresh().await;
    }

    /// Serves one client connection until it ends.
    pub async fn serve(self: &Arc<Self>, wire: Wire, via: Via) {
        conn::serve(self.clone(), wire, via).await;
    }

    /// A connection for a client in this process, authenticated as
    /// `device`: an app that hosts its own computer's agents speaks OAL to
    /// them this way, with nothing to carry the frames and so nothing to
    /// encrypt. Frames are whole OAL frames, as over any other connection.
    pub fn connect_local(self: &Arc<Self>, device: DeviceRef) -> LocalConnection {
        let (to_host, from_client) = tokio::sync::mpsc::unbounded_channel();
        let (to_client, from_host) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(conn::serve_local(self.clone(), device, from_client, to_client));
        LocalConnection { tx: to_host, rx: from_host }
    }
}

/// An in-process client's end of a connection ([`OalHost::connect_local`]):
/// frames to the host on `tx`, from it on `rx`. Dropping `tx` closes the
/// connection; `rx` ends when the host closes it.
pub struct LocalConnection {
    pub tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    pub rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
}

/// The version to speak with a client whose range is `range`
/// (`{"min","max"}`), or the error that says who should update: the app, or
/// `software` on the host.
fn select_version(range: &Value, host_name: &str, software: &str) -> Result<&'static str, ErrorObject> {
    let parse = |v: &Value| -> Option<(u64, u64)> {
        let (major, minor) = v.as_str()?.split_once('.')?;
        Some((major.parse().ok()?, minor.parse().ok()?))
    };
    let ours = parse(&json!(PROTOCOL)).expect("our version");
    match (parse(&range["min"]), parse(&range["max"])) {
        (Some(min), Some(max)) if min <= ours && ours <= max => Ok(PROTOCOL),
        (Some(min), Some(max)) => {
            let theirs = if min == max {
                format!("{}.{}", min.0, min.1)
            } else {
                format!("{}.{} to {}.{}", min.0, min.1, max.0, max.1)
            };
            let update = if max < ours { "Update the app.".to_owned() } else { format!("Update {software} on this computer.") };
            Err(ErrorObject::new(
                code::VERSION_MISMATCH,
                format!("This app speaks OAL {theirs} and {host_name} speaks {PROTOCOL}. {update}"),
            )
            .with_data(json!({ "client": range, "host": { "min": PROTOCOL, "max": PROTOCOL } })))
        }
        _ => Err(ErrorObject::new(code::INVALID_PARAMS, "protocol needs min and max.")),
    }
}

/// Resolves once the host shuts down.
async fn stopped(closing: &mut watch::Receiver<bool>) {
    let _ = closing.wait_for(|closing| *closing).await;
}

fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

/// A random secret, base64url without padding.
fn token() -> String {
    use base64::Engine;
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).expect("randomness");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Writes `bytes` through a temporary file, readable by the owner only.
fn write_private(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_are_chosen_or_refused_plainly() {
        let select = |range: Value| select_version(&range, "Studio", "nebo-link");
        assert_eq!(select(json!({ "min": "0.1", "max": "0.1" })).unwrap(), "0.1");
        assert_eq!(select(json!({ "min": "0.0", "max": "1.0" })).unwrap(), "0.1");
        let newer = select(json!({ "min": "9.0", "max": "9.0" })).unwrap_err();
        assert_eq!(newer.code, code::VERSION_MISMATCH);
        assert_eq!(newer.message, "This app speaks OAL 9.0 and Studio speaks 0.1. Update nebo-link on this computer.");
        let older = select(json!({ "min": "0.0", "max": "0.0" })).unwrap_err();
        assert!(older.message.ends_with("Update the app."), "{}", older.message);
        assert_eq!(select(json!({})).unwrap_err().code, code::INVALID_PARAMS);
    }
}
