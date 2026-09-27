//! Open Agent Link beside NeboAI: the service serves the bot's agents to OAL
//! clients (`oal_host`), end-to-end encrypted, through NeboAI's tunnel
//! (`/t/<botId>/oal`, [`upgrade`]), through a relay it dials out to (a
//! self-hosted `oal-relay`) and, when the owner turns it on, directly on the
//! LAN. The bot's id is the host's id and its name the host's name.
//!
//! An app of the owner's that reaches the bot through NeboAI (Nebo, for the
//! bot's agents it hired) pairs without the owner: it asks for a code on the
//! tunnel ([`bootstrap`], `POST /_link/oal/pair`), which only the owner's
//! own requests reach, and pairs with it at once over `/oal`. The code is
//! made here and used once; CPace and Noise turn it into keys that never
//! leave the two ends, so NeboAI, which carried the code, carries only
//! ciphertext after.
//!
//! The service owns the host's keys; the CLI asks it for what needs them
//! through the bot's `oal/` folder (owner-only): `nebo-link pair` writes a
//! request there and the service answers with a pairing code, then with the
//! device that paired; `nebo-link unpair` likewise.
//!
//! ```text
//! oal/keys/         the host's X25519 key and its paired devices
//! oal/seen.json     when each device was last seen
//! oal/lan-*.pem     LAN direct's self-signed certificate
//! oal/request.json  the CLI's request; oal/answer.json the service's answer
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Response, StatusCode};
use hyper_util::rt::TokioIo;
use link_core::host::Host;
use oal_host::{Config, OalHost, Runtime as OalRuntime, Via};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};

use crate::error::{Error, Result};
use crate::proxy::{Body, full, json, text};
use crate::install::{runtime_key, runtime_name};
use crate::state::{BotDir, Link, Oal, read_json, write_json};

/// LAN direct's address when the owner turns it on without one.
pub const DEFAULT_LAN: &str = "0.0.0.0:8481";
/// How often the service looks for a request from the CLI.
const REQUEST_EVERY: Duration = Duration::from_millis(300);
/// How long the CLI waits for the service to answer.
const ANSWER_WAIT: Duration = Duration::from_secs(20);

/// The bot's `oal/` folder.
pub fn dir(bot: &BotDir) -> PathBuf {
    bot.path().join("oal")
}

fn request_file(bot: &BotDir) -> PathBuf {
    dir(bot).join("request.json")
}

fn answer_file(bot: &BotDir) -> PathBuf {
    dir(bot).join("answer.json")
}

/// What the CLI asks the service.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum Request {
    /// A pairing code for a new device.
    Pair { id: String },
    /// Unpair a device.
    Unpair { id: String, device: String },
}

impl Request {
    fn id(&self) -> &str {
        match self {
            Request::Pair { id } | Request::Unpair { id, .. } => id,
        }
    }
}

/// The service's answer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Answer {
    pub id: String,
    /// The pairing code to show.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// When the code stops working (Unix seconds).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<u64>,
    /// The device that paired with the code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paired: Option<String>,
    /// The code expired unused.
    #[serde(default)]
    pub expired: bool,
    /// The request failed, and why.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// The request was done (an unpairing).
    #[serde(default)]
    pub done: bool,
}

/// OAL's part of the service's status, for `nebo-link status`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relay: Option<String>,
    #[serde(default)]
    pub relay_connected: bool,
    /// LAN direct's address and certificate fingerprint, while it is on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lan: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    /// The paired devices, by name.
    #[serde(default)]
    pub devices: Vec<String>,
}

/// The OAL host the service runs.
pub struct Service {
    pub host: Arc<OalHost>,
    settings: Oal,
    lan: Option<oal_host::lan::Lan>,
}

impl Service {
    pub fn status(&self) -> Status {
        Status {
            relay: self.settings.relay.clone(),
            relay_connected: self.host.relay_connected(),
            lan: self.lan.as_ref().map(|lan| lan.addr.to_string()),
            fingerprint: self.lan.as_ref().map(|lan| lan.fingerprint.clone()),
            devices: self.host.devices().into_iter().map(|d| d.name).collect(),
        }
    }
}

/// The runtimes a link runs, for `host/info`.
fn runtimes(link: &Link) -> Vec<OalRuntime> {
    let mut all: Vec<OalRuntime> = link
        .agents
        .iter()
        .map(|a| OalRuntime {
            id: runtime_key(a.runtime).to_owned(),
            name: runtime_name(a.runtime).to_owned(),
            kind: match a.runtime {
                nebo_runtimes::Runtime::Acp(_) => "acp".to_owned(),
                other => runtime_key(other).to_owned(),
            },
            version: None,
        })
        .collect();
    all.sort_by(|a, b| a.id.cmp(&b.id));
    all.dedup_by(|a, b| a.id == b.id);
    all
}

/// Starts the bot's OAL host over `host`, with `settings` (the link's, or
/// what the command line says instead): the relay's tunnel, LAN direct, and
/// the CLI's requests.
pub async fn start(bot: &BotDir, link: &Link, host: Arc<Host>, settings: Oal) -> Result<Service> {
    let folder = dir(bot);
    let links = bot.clone();
    let keys_dir = folder.join("keys");
    let keys = oal_secure::KeyStore::open(&keys_dir)
        .map_err(|e| Error::Message(format!("could not open the host's keys in {}: {e}", keys_dir.display())))?;
    let config = Config {
        host_id: link.bot_id.clone(),
        host_name: link.name.clone(),
        software: ("nebo-link".to_owned(), env!("CARGO_PKG_VERSION").to_owned()),
        keys,
        seen_file: folder.join("seen.json"),
        runtimes: Arc::new(move || links.load().map(|link| runtimes(&link)).unwrap_or_default()),
    };
    let oal = OalHost::new(config, host).map_err(Error::Message)?;
    let warm = oal.clone();
    tokio::spawn(async move { warm.warm_up().await });
    if let Some(relay) = &settings.relay {
        tokio::spawn(oal_host::relay::run(oal.clone(), relay.clone()));
    }
    // LAN direct that can't start (its port taken) leaves the relay serving.
    let lan = match &settings.lan {
        Some(listen) => match listen.parse() {
            Ok(addr) => match oal_host::lan::serve(oal.clone(), addr, &folder, true).await {
                Ok(lan) => Some(lan),
                Err(e) => {
                    tracing::error!(error = %e, "oal: LAN direct could not start");
                    None
                }
            },
            Err(_) => {
                tracing::error!(listen, "oal: LAN direct's address isn't an address and port, like {DEFAULT_LAN}");
                None
            }
        },
        None => None,
    };
    tokio::spawn(requests(bot.clone(), oal.clone()));
    tracing::info!(relay = ?settings.relay, lan = ?lan.as_ref().map(|l| l.addr), "open agent link is serving");
    Ok(Service {
        host: oal,
        settings,
        lan,
    })
}

/// `WS /oal` through NeboAI's tunnel: the upgrade answered (with the `oal`
/// subprotocol when offered, spec 4.1), then the connection served as any
/// other, end-to-end encrypted, a pairing or a session.
pub fn upgrade(oal: Arc<OalHost>, req: &mut hyper::Request<Incoming>) -> Response<Body> {
    let upgrading = req
        .headers()
        .get(header::UPGRADE)
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"));
    let Some(key) = req.headers().get(header::SEC_WEBSOCKET_KEY).filter(|_| upgrading) else {
        return text(StatusCode::BAD_REQUEST, "Open Agent Link is a WebSocket at /oal.");
    };
    let accept = derive_accept_key(key.as_bytes());
    let offered = req
        .headers()
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|p| p.trim() == "oal"));
    let upgrade = hyper::upgrade::on(req);
    tokio::spawn(async move {
        match upgrade.await {
            Ok(upgraded) => {
                let config = WebSocketConfig::default()
                    .max_message_size(Some(oal_host::MAX_FRAME + 1024))
                    .max_frame_size(Some(oal_host::MAX_FRAME + 1024));
                let socket = WebSocketStream::from_raw_socket(TokioIo::new(upgraded), Role::Server, Some(config)).await;
                oal.serve(oal_host::wire::websocket(socket), Via::Tunnel).await;
            }
            Err(e) => tracing::info!(error = %e, "oal: an upgrade through the tunnel failed"),
        }
    });
    let mut resp = Response::new(full(""));
    *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    let headers = resp.headers_mut();
    headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    if let Ok(accept) = HeaderValue::from_str(&accept) {
        headers.insert(header::SEC_WEBSOCKET_ACCEPT, accept);
    }
    if offered {
        headers.insert(header::SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static("oal"));
    }
    resp
}

/// `POST /_link/oal/pair`: a one-time pairing code for an app of the
/// owner's that reaches this bot through NeboAI, to pair with at once over
/// `/oal`. Only a request NeboAI authenticated as the owner's (the tunnel's
/// stamp) gets here. The code replaces any other showing, works once and
/// for at most [`oal_host::CODE_LIFETIME`].
pub async fn bootstrap(oal: &OalHost) -> Response<Body> {
    match oal.pairing_code().await {
        Ok(code) => {
            tracing::info!(nameplate = code.nameplate(), "oal: a pairing code was issued through NeboAI");
            json(StatusCode::OK, &serde_json::json!({ "code": code.to_string(), "hostId": oal.host_id() }))
        }
        Err(e) => json(StatusCode::BAD_GATEWAY, &serde_json::json!({ "error": e })),
    }
}

/// Answers the CLI's requests while the service runs.
async fn requests(bot: BotDir, oal: Arc<OalHost>) {
    let file = request_file(&bot);
    loop {
        tokio::time::sleep(REQUEST_EVERY).await;
        let Ok(request) = read_json::<Request>(&file) else {
            continue;
        };
        let _ = std::fs::remove_file(&file);
        let id = request.id().to_owned();
        let answer = match request {
            Request::Pair { .. } => match oal.pairing_code().await {
                Ok(code) => {
                    let expires = unix_now() + oal_host::CODE_LIFETIME.as_secs();
                    let answer = Answer {
                        id: id.clone(),
                        code: Some(code.to_string()),
                        expires: Some(expires),
                        ..Answer::default()
                    };
                    tokio::spawn(watch_pairing(bot.clone(), oal.clone(), answer.clone()));
                    answer
                }
                Err(e) => Answer {
                    id,
                    error: Some(e),
                    ..Answer::default()
                },
            },
            Request::Unpair { device, .. } => {
                let found = oal.devices().into_iter().find(|d| d.id == device || d.name == device);
                match found {
                    Some(found) => match oal.unpair(&found.id).await {
                        Ok(()) => Answer {
                            id,
                            done: true,
                            paired: Some(found.name),
                            ..Answer::default()
                        },
                        Err(e) => Answer {
                            id,
                            error: Some(e.message),
                            ..Answer::default()
                        },
                    },
                    None => Answer {
                        id,
                        error: Some(format!("No device called {device} is paired.")),
                        ..Answer::default()
                    },
                }
            }
        };
        if let Err(e) = write_json(&answer_file(&bot), &answer) {
            tracing::warn!(error = %e, "oal: could not answer the command line");
        }
    }
}

/// Tells the CLI which device paired with the code, or that it expired.
async fn watch_pairing(bot: BotDir, oal: Arc<OalHost>, mut answer: Answer) {
    let before: Vec<String> = oal.devices().into_iter().map(|d| d.id).collect();
    let expires = answer.expires.unwrap_or_default();
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        if let Some(device) = oal.devices().into_iter().find(|d| !before.contains(&d.id)) {
            answer.paired = Some(device.name);
            break;
        }
        if unix_now() >= expires {
            answer.expired = true;
            break;
        }
    }
    let _ = write_json(&answer_file(&bot), &answer);
}

/// Asks the running service `request` and returns its first answer.
pub async fn ask(bot: &BotDir, request: Request) -> Result<Answer> {
    let answers = answer_file(bot);
    let _ = std::fs::remove_file(&answers);
    write_json(&request_file(bot), &request)?;
    let deadline = tokio::time::Instant::now() + ANSWER_WAIT;
    loop {
        if let Ok(answer) = read_json::<Answer>(&answers)
            && answer.id == request.id()
        {
            return Ok(answer);
        }
        if tokio::time::Instant::now() >= deadline {
            let _ = std::fs::remove_file(request_file(bot));
            return Err(Error::Message(
                "nebo-link isn't running for this bot, so it can't answer. Start it again (it runs as a service; `nebo-link status` says how it is), then run this again."
                    .to_owned(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Waits for the device that pairs with the code `answer` carries: its name,
/// or `None` once the code expired (or the service stopped saying).
pub async fn paired(bot: &BotDir, answer: &Answer) -> Option<String> {
    let deadline = answer.expires.unwrap_or_default() + 5;
    loop {
        tokio::time::sleep(Duration::from_millis(300)).await;
        match read_json::<Answer>(&answer_file(bot)) {
            Ok(now) if now.id == answer.id && now.paired.is_some() => return now.paired,
            Ok(now) if now.id == answer.id && now.expired => return None,
            _ if unix_now() > deadline => return None,
            _ => {}
        }
    }
}

/// A request id.
pub fn request_id() -> String {
    crate::state::secret()[..16].to_owned()
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// Refuses, in plain words, while an app hosts this OS user's agents: one
/// program hosts a computer's agents (`link_core::machine`).
pub fn refuse_if_an_app_hosts(home: &std::path::Path) -> Result<()> {
    let Some(host) = link_core::machine::hosting_app(home) else {
        return Ok(());
    };
    let file = home.join(link_core::machine::APP_HOST_FILE);
    Err(Error::Message(format!(
        "{app} already hosts the coding agents on this computer ({agents}). One program hosts a computer's agents: stop hosting them in {app} first, then run this again. If {app} is no longer installed, delete {file}.",
        app = host.app,
        agents = host.agents.join(", "),
        file = file.display(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_and_answers_are_json() {
        let request = Request::Unpair {
            id: "r1".into(),
            device: "d-1".into(),
        };
        let text = serde_json::to_string(&request).unwrap();
        assert_eq!(text, r#"{"kind":"unpair","id":"r1","device":"d-1"}"#);
        assert_eq!(serde_json::from_str::<Request>(&text).unwrap(), request);
        let answer: Answer = serde_json::from_str(r#"{"id":"r1","code":"K7QM-3XRD","expires":5}"#).unwrap();
        assert_eq!(answer.code.as_deref(), Some("K7QM-3XRD"));
        assert!(!answer.expired && answer.paired.is_none());
    }

    #[test]
    fn an_app_hosting_agents_refuses_in_plain_words() {
        let home = tempfile::tempdir().unwrap();
        assert!(refuse_if_an_app_hosts(home.path()).is_ok());
        link_core::machine::record_app_host(home.path(), "Nebo", &["Claude Code".into()]).unwrap();
        let refused = refuse_if_an_app_hosts(home.path()).unwrap_err().to_string();
        assert!(refused.starts_with("Nebo already hosts the coding agents on this computer (Claude Code)."), "{refused}");
    }
}
