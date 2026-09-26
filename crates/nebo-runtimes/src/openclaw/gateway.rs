//! A client for the OpenClaw gateway WebSocket, as the link's own operator
//! connection over loopback.
//!
//! The gateway is in `gateway.auth.mode: "trusted-proxy"` after
//! [`Change::ProxyAccess`](crate::Change::ProxyAccess). To the gateway this
//! socket is that proxy: it carries the identity header and the forwarded
//! headers the link stamps on a browser's request, the device keypair proves
//! the connection (`connect.params.device`), and trusted-proxy device
//! auto-approval pairs it without a prompt. The socket then carries
//! `req`/`res` frames for methods and `event` frames for the run.
//!
//! ```no_run
//! use nebo_runtimes::ProxyAccess;
//! use nebo_runtimes::openclaw::gateway::{ChatSend, Connect, Event, FileDeviceStore, Gateway};
//!
//! # async fn run(access: &ProxyAccess) -> Result<(), nebo_runtimes::openclaw::gateway::Error> {
//! let store = FileDeviceStore::new("/path/to/link/openclaw-device.json");
//! let (gateway, mut events) = Gateway::connect(
//!     &Connect::new("ws://127.0.0.1:18789", access, "203.0.113.10"),
//!     &store,
//! )
//! .await?;
//! let ack = gateway
//!     .chat_send(&ChatSend::new("agent:main:nebo-link", "hello"))
//!     .await?;
//! while let Some(event) = events.next().await {
//!     if let Event::Chat(chat) = &event {
//!         if chat.run_id == ack.run_id && chat.state.is_terminal() {
//!             break;
//!         }
//!     }
//! }
//! # Ok(())
//! # }
//! ```

mod device;
mod protocol;

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{Message, Utf8Bytes};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub use device::{DeviceKey, DeviceStore, FileDeviceStore};
pub use protocol::{
    AgentEvent, AgentIdentity, AgentModel, AgentStream, AgentSummary, AgentsList, ApprovalKind,
    ApprovalOutcome, ApprovalRequest, ApprovalRequested, ApprovalResolved, Attachment, CAPS,
    CLIENT_ID, CLIENT_MODE, ChatEvent, ChatSend, ChatSendAck, ChatState, Decision, ErrorShape,
    Event, ExecApprovalRequest, HelloAuth, HelloFeatures, HelloOk, HelloPolicy, HelloServer,
    History, HistoryDelta, HistoryPage, HistoryQuery, InFlightRun, PROTOCOL_VERSION,
    PluginApprovalRequest, QueueMode, ROLE, SCOPES, SessionInfo, SessionMessage, SessionRow,
    SessionsChanged, SessionsList, SessionsQuery, SessionsSubscribed, new_idempotency_key,
};

use crate::ProxyAccess;
use device::{AuthPayload, auth_payload};
use protocol::{Challenge, CursorResult, Frame, RequestFrame};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The close code the gateway sends for a protocol mismatch
/// (`src/gateway/server/ws-connection/connect-admission.ts:275`).
const PROTOCOL_MISMATCH_CLOSE: u16 = 1002;
/// `ConnectErrorDetailCodes.PROTOCOL_MISMATCH`
/// (`packages/gateway-protocol/src/connect-error-details.ts:36`).
const PROTOCOL_MISMATCH_CODE: &str = "PROTOCOL_MISMATCH";
/// `ConnectErrorDetailCodes.PAIRING_REQUIRED` (`connect-error-details.ts:46`).
const PAIRING_REQUIRED_CODE: &str = "PAIRING_REQUIRED";
/// Events the reader buffers for a slow consumer before it applies
/// backpressure to the socket.
const EVENT_BUFFER: usize = 1024;
/// Terminal run ids remembered so a `chat.send` acknowledgment that lands
/// after its run's final event does not resurrect the run as active.
const ENDED_RUNS: usize = 64;

/// Everything that can go wrong on the gateway socket.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The WebSocket could not be opened, or the device store failed.
    #[error("could not connect to the OpenClaw gateway at {url}: {message}")]
    Connect { url: String, message: String },
    /// The gateway speaks a different protocol version than this client.
    #[error("{}", mismatch_message(*expected))]
    ProtocolMismatch { expected: Option<u32> },
    /// The gateway wants an operator to approve this device
    /// (`docs/gateway/clients.md:83-87`): trusted-proxy auto-approval did
    /// not apply.
    #[error("OpenClaw is waiting for someone to approve this device: {message}")]
    PairingRequired { message: String },
    /// The gateway refused the `connect` request for another reason.
    #[error("OpenClaw refused the connection ({code}): {message}")]
    Handshake { code: String, message: String },
    /// A method returned `ok: false`.
    #[error("OpenClaw refused {method} ({code}): {message}")]
    Rejected {
        method: String,
        code: String,
        message: String,
        retryable: bool,
    },
    /// The socket closed; `code` and `reason` are the close frame's.
    #[error("the OpenClaw gateway connection closed ({code}): {reason}")]
    Closed { code: u16, reason: String },
    /// No answer within the connection's timeout.
    #[error("the OpenClaw gateway did not answer {what} within {timeout:?}")]
    Timeout { what: String, timeout: Duration },
    /// A frame did not have the shape this client expects.
    #[error("unexpected frame from the OpenClaw gateway: {0}")]
    Decode(String),
}

fn mismatch_message(expected: Option<u32>) -> String {
    let theirs = match expected {
        Some(version) => format!("gateway protocol {version}"),
        None => "a different gateway protocol".to_owned(),
    };
    format!(
        "this OpenClaw speaks {theirs}; Nebo Link speaks protocol {PROTOCOL_VERSION}. Update Nebo Link, or OpenClaw, so the two match"
    )
}

/// How to reach a gateway. The request headers are the ones the link's proxy
/// stamps on a browser's request, because the gateway admits this socket by
/// the same rules:
/// - `user_header: identity` is the trusted-proxy identity
///   (`src/gateway/auth.ts:256-270`);
/// - `Origin: origin` passes the browser-origin allowlist a Control UI
///   client is held to (`src/gateway/server/ws-origin-policy.ts:25-46`,
///   `origin-check.ts:113-119`), which `ProxyAccess` filled with `origin`;
/// - `X-Forwarded-For: forwarded_for` (with `-Proto` and `-Host`) attributes
///   the client; a trusted-proxy socket without one is refused as
///   `proxy_attribution_required` (`src/gateway/ingress-attribution.ts:12-14`).
#[derive(Debug, Clone)]
pub struct Connect {
    /// `ws://127.0.0.1:<port>`, the gateway's own listener.
    pub url: String,
    /// `gateway.auth.trustedProxy.userHeader` (`ProxyAccess::user_header`).
    pub user_header: String,
    /// The identity sent in that header (`ProxyAccess::identity`).
    pub identity: String,
    /// The browser origin `ProxyAccess` allowlisted (`ProxyAccess::origin`).
    pub origin: String,
    /// The client address the link reports in `X-Forwarded-For`.
    pub forwarded_for: String,
    /// `connect.params.client.version`.
    pub client_version: String,
    /// `connect.params.client.displayName`.
    pub display_name: String,
    /// The most a handshake step or a method call may take.
    pub timeout: Duration,
}

impl Connect {
    /// A connection admitted by the `ProxyAccess` change the link applied to
    /// this gateway, attributed to `forwarded_for`.
    pub fn new(
        url: impl Into<String>,
        access: &ProxyAccess,
        forwarded_for: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            user_header: access.user_header.clone(),
            identity: access.identity.clone(),
            origin: access.origin.clone(),
            forwarded_for: forwarded_for.into(),
            client_version: env!("CARGO_PKG_VERSION").to_owned(),
            display_name: "Nebo Link".to_owned(),
            timeout: Duration::from_secs(30),
        }
    }
}

/// `connect.params.client.platform`, in Node's `process.platform` words so
/// the pinned device metadata reads naturally beside OpenClaw's own.
fn platform() -> &'static str {
    match std::env::consts::OS {
        "macos" => "darwin",
        "windows" => "win32",
        other => other,
    }
}

struct Shared {
    pending: Mutex<HashMap<String, oneshot::Sender<Result<Value, Error>>>>,
    /// The run in progress per session key, from `chat.send` acks, `chat`
    /// events and `chat.history`; decides `queueMode: "steer"`.
    active_runs: Mutex<HashMap<String, String>>,
    /// The last [`ENDED_RUNS`] run ids a terminal `chat` event closed.
    ended_runs: Mutex<VecDeque<String>>,
    closed: Mutex<Option<Error>>,
    next_id: AtomicU64,
}

impl Shared {
    fn fail_pending(&self, error: &Error) {
        let pending = std::mem::take(&mut *self.pending.lock().expect("pending lock"));
        for (_, sender) in pending {
            let _ = sender.send(Err(clone_error(error)));
        }
    }
}

fn clone_error(error: &Error) -> Error {
    match error {
        Error::Closed { code, reason } => Error::Closed {
            code: *code,
            reason: reason.clone(),
        },
        other => Error::Decode(other.to_string()),
    }
}

/// A connected gateway. Cloning shares the connection; the connection ends
/// when the last clone and the [`Events`] are dropped, or when the gateway
/// closes it.
#[derive(Clone)]
pub struct Gateway {
    shared: Arc<Shared>,
    outgoing: mpsc::Sender<Message>,
    hello: Arc<HelloOk>,
    timeout: Duration,
}

impl std::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Gateway")
            .field("conn_id", &self.hello.server.conn_id)
            .field("closed", &self.closed().map(|error| error.to_string()))
            .finish_non_exhaustive()
    }
}

/// The gateway's events, in arrival order. `next` returns `None` once the
/// connection is closed; [`Gateway::closed`] then says why.
#[derive(Debug)]
pub struct Events {
    receiver: mpsc::Receiver<Event>,
}

impl Events {
    pub async fn next(&mut self) -> Option<Event> {
        self.receiver.recv().await
    }
}

impl Gateway {
    /// Open the socket, answer the challenge with the device proof, and
    /// return once `hello-ok` is in.
    pub async fn connect(
        config: &Connect,
        device: &dyn DeviceStore,
    ) -> Result<(Self, Events), Error> {
        let connect_error = |message: String| Error::Connect {
            url: config.url.clone(),
            message,
        };
        let key = match device
            .load()
            .map_err(|error| connect_error(error.to_string()))?
        {
            Some(key) => key,
            None => {
                let key =
                    DeviceKey::generate().map_err(|error| connect_error(error.to_string()))?;
                device
                    .save(&key)
                    .map_err(|error| connect_error(error.to_string()))?;
                key
            }
        };

        let mut request = config
            .url
            .as_str()
            .into_client_request()
            .map_err(|error| connect_error(error.to_string()))?;
        let forwarded_host = config
            .origin
            .trim_start_matches("https://")
            .trim_start_matches("http://");
        for (name, value) in [
            (config.user_header.as_str(), config.identity.as_str()),
            ("origin", config.origin.as_str()),
            ("x-forwarded-for", config.forwarded_for.as_str()),
            ("x-forwarded-proto", "https"),
            ("x-forwarded-host", forwarded_host),
        ] {
            let name = name
                .parse::<tokio_tungstenite::tungstenite::http::HeaderName>()
                .map_err(|error| connect_error(format!("header {name}: {error}")))?;
            let value = value
                .parse::<tokio_tungstenite::tungstenite::http::HeaderValue>()
                .map_err(|error| connect_error(format!("header {name}: {error}")))?;
            request.headers_mut().insert(name, value);
        }

        let (socket, _) =
            tokio::time::timeout(config.timeout, tokio_tungstenite::connect_async(request))
                .await
                .map_err(|_| Error::Timeout {
                    what: "the WebSocket upgrade".to_owned(),
                    timeout: config.timeout,
                })?
                .map_err(|error| connect_error(error.to_string()))?;
        let (mut sink, mut stream) = socket.split();

        let handshake = async {
            let challenge = read_challenge(&mut stream).await?;
            let params = connect_params(config, &key, &challenge);
            let frame = RequestFrame {
                kind: "req",
                id: "connect",
                method: "connect",
                params: Some(&params),
            };
            let text =
                serde_json::to_string(&frame).map_err(|error| Error::Decode(error.to_string()))?;
            sink.send(Message::Text(text.into()))
                .await
                .map_err(|error| connect_error(error.to_string()))?;
            read_hello(&mut stream).await
        };
        let hello = tokio::time::timeout(config.timeout, handshake)
            .await
            .map_err(|_| Error::Timeout {
                what: "the connect handshake".to_owned(),
                timeout: config.timeout,
            })??;
        if hello.protocol != PROTOCOL_VERSION {
            return Err(Error::ProtocolMismatch {
                expected: Some(hello.protocol),
            });
        }

        let shared = Arc::new(Shared {
            pending: Mutex::new(HashMap::new()),
            active_runs: Mutex::new(HashMap::new()),
            ended_runs: Mutex::new(VecDeque::with_capacity(ENDED_RUNS)),
            closed: Mutex::new(None),
            next_id: AtomicU64::new(1),
        });
        let (outgoing, outgoing_rx) = mpsc::channel::<Message>(64);
        let (events_tx, events_rx) = mpsc::channel::<Event>(EVENT_BUFFER);
        tokio::spawn(write_loop(sink, outgoing_rx));
        tokio::spawn(read_loop(
            stream,
            Arc::clone(&shared),
            outgoing.clone(),
            events_tx,
        ));

        Ok((
            Self {
                shared,
                outgoing,
                hello: Arc::new(hello),
                timeout: config.timeout,
            },
            Events {
                receiver: events_rx,
            },
        ))
    }

    /// The `hello-ok` this connection was admitted with.
    pub fn hello(&self) -> &HelloOk {
        &self.hello
    }

    /// Why the connection ended, once it has.
    pub fn closed(&self) -> Option<Error> {
        self.shared
            .closed
            .lock()
            .expect("closed lock")
            .as_ref()
            .map(clone_error)
    }

    /// The run this client believes is active on `session_key`.
    pub fn active_run(&self, session_key: &str) -> Option<String> {
        self.shared
            .active_runs
            .lock()
            .expect("active runs lock")
            .get(session_key)
            .cloned()
    }

    /// Call any method. The typed methods below are this, plus a shape.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, Error> {
        let id = format!("nl-{}", self.shared.next_id.fetch_add(1, Ordering::Relaxed));
        let frame = RequestFrame {
            kind: "req",
            id: &id,
            method,
            params: Some(&params),
        };
        let text =
            serde_json::to_string(&frame).map_err(|error| Error::Decode(error.to_string()))?;
        let (sender, receiver) = oneshot::channel();
        if let Some(error) = self.closed() {
            return Err(error);
        }
        self.shared
            .pending
            .lock()
            .expect("pending lock")
            .insert(id.clone(), sender);
        if self
            .outgoing
            .send(Message::Text(text.into()))
            .await
            .is_err()
        {
            self.shared
                .pending
                .lock()
                .expect("pending lock")
                .remove(&id);
            return Err(self.closed().unwrap_or(Error::Closed {
                code: 1006,
                reason: "connection lost".to_owned(),
            }));
        }
        match tokio::time::timeout(self.timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(self.closed().unwrap_or(Error::Closed {
                code: 1006,
                reason: "connection lost".to_owned(),
            })),
            Err(_) => {
                self.shared
                    .pending
                    .lock()
                    .expect("pending lock")
                    .remove(&id);
                Err(Error::Timeout {
                    what: method.to_owned(),
                    timeout: self.timeout,
                })
            }
        }
    }

    async fn call<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T, Error> {
        let payload = self.request(method, params).await?;
        serde_json::from_value(payload)
            .map_err(|error| Error::Decode(format!("{method} result: {error}")))
    }

    /// `agents.list` (`schema/agents-models-skills.ts:85-100`).
    pub async fn agents_list(&self) -> Result<AgentsList, Error> {
        self.call("agents.list", json!({})).await
    }

    /// `sessions.list` (`schema/sessions-list.ts:5-88`).
    pub async fn sessions_list(&self, query: &SessionsQuery) -> Result<SessionsList, Error> {
        let params =
            serde_json::to_value(query).map_err(|error| Error::Decode(error.to_string()))?;
        self.call("sessions.list", params).await
    }

    /// `sessions.subscribe` (`server-methods/sessions-subscriptions.ts:27-49`):
    /// start receiving `sessions.changed`, and with a non-empty `query` load
    /// the roster in the same call. Reconnects need a new subscription.
    pub async fn sessions_subscribe(
        &self,
        query: &SessionsQuery,
    ) -> Result<SessionsSubscribed, Error> {
        let params =
            serde_json::to_value(query).map_err(|error| Error::Decode(error.to_string()))?;
        self.call("sessions.subscribe", params).await
    }

    /// `sessions.messages.subscribe`
    /// (`server-methods/sessions-subscriptions.ts:96-246`, `operator.read`):
    /// start receiving `session.message` for the transcript rows of one
    /// session key. The key need not exist yet. Reconnects need a new
    /// subscription.
    pub async fn sessions_messages_subscribe(
        &self,
        session_key: &str,
        agent_id: Option<&str>,
    ) -> Result<(), Error> {
        let mut params = json!({ "key": session_key });
        if let Some(agent_id) = agent_id {
            params["agentId"] = json!(agent_id);
        }
        self.request("sessions.messages.subscribe", params).await?;
        Ok(())
    }

    /// `chat.history` (`schema/logs-chat.ts:36-54`). With `cursor`, the
    /// gateway's catch-up delta; when it answers `reset` (the cursor is
    /// stale or crossed a compaction, `logs-chat.ts:168-171`) the tail
    /// page is fetched and returned instead, so a caller always gets
    /// messages and a fresh cursor. The in-flight run, if any, is adopted
    /// as the session's active run.
    pub async fn chat_history(&self, query: &HistoryQuery) -> Result<History, Error> {
        let history = if query.cursor.is_some() {
            let params =
                serde_json::to_value(query).map_err(|error| Error::Decode(error.to_string()))?;
            match self.call::<CursorResult>("chat.history", params).await? {
                CursorResult::Delta(mut delta) => {
                    // Catch-up entries are `session.message` payloads
                    // (`{sessionKey, agentId, message}`, `rpc-session-control.md:45`);
                    // hand the caller the transcript rows a tail page has.
                    for entry in &mut delta.messages {
                        if let Some(message) = entry.get_mut("message") {
                            *entry = message.take();
                        }
                    }
                    Some(History::Delta(*delta))
                }
                CursorResult::Reset {} => None,
            }
        } else {
            None
        };
        let history = match history {
            Some(history) => history,
            None => {
                let tail = HistoryQuery {
                    cursor: None,
                    ..query.clone()
                };
                let params = serde_json::to_value(&tail)
                    .map_err(|error| Error::Decode(error.to_string()))?;
                History::Page(self.call("chat.history", params).await?)
            }
        };
        let mut active = self.shared.active_runs.lock().expect("active runs lock");
        match history.in_flight_run() {
            Some(run) => {
                active.insert(query.session_key.clone(), run.run_id.clone());
            }
            None if history.session_info().has_active_run != Some(true) => {
                active.remove(&query.session_key);
            }
            None => {}
        }
        Ok(history)
    }

    /// `chat.send` (`schema/logs-chat.ts:296-332`): one message into
    /// `session_key`, creating the session on first use. When the client
    /// knows a run is active there and `queue_mode` is unset, the send
    /// goes as `queueMode: "steer"` so it reaches that run
    /// (`rpc-session-control.md:49`).
    pub async fn chat_send(&self, send: &ChatSend) -> Result<ChatSendAck, Error> {
        let queue_mode = send.queue_mode.or_else(|| {
            self.active_run(&send.session_key)
                .is_some()
                .then_some(QueueMode::Steer)
        });
        let mut params = json!({
            "sessionKey": send.session_key,
            "message": send.message,
            "idempotencyKey": send.idempotency_key,
        });
        if let Some(agent_id) = &send.agent_id {
            params["agentId"] = json!(agent_id);
        }
        if let Some(mode) = queue_mode {
            params["queueMode"] = json!(mode);
        }
        if !send.attachments.is_empty() {
            params["attachments"] = json!(send.attachments);
        }
        let ack: ChatSendAck = self.call("chat.send", params).await?;
        // The run's final event can be on the wire right behind this ack;
        // the reader has then already closed the run.
        let ended = self
            .shared
            .ended_runs
            .lock()
            .expect("ended runs lock")
            .contains(&ack.run_id);
        if !ended {
            self.shared
                .active_runs
                .lock()
                .expect("active runs lock")
                .insert(send.session_key.clone(), ack.run_id.clone());
        }
        Ok(ack)
    }

    /// `chat.abort` (`schema/logs-chat.ts:335-340`): cancel `run_id`, or
    /// the session's active run when `None`.
    pub async fn chat_abort(
        &self,
        session_key: &str,
        agent_id: Option<&str>,
        run_id: Option<&str>,
    ) -> Result<(), Error> {
        let mut params = json!({ "sessionKey": session_key });
        if let Some(agent_id) = agent_id {
            params["agentId"] = json!(agent_id);
        }
        if let Some(run_id) = run_id {
            params["runId"] = json!(run_id);
        }
        self.request("chat.abort", params).await?;
        Ok(())
    }

    /// `approval.resolve` (`schema/approvals.ts:316-325`): answer a
    /// pending approval by its full id. First answer wins; the result says
    /// whether this one was it.
    pub async fn approval_resolve(
        &self,
        id: &str,
        kind: ApprovalKind,
        decision: Decision,
    ) -> Result<ApprovalResolved, Error> {
        self.call(
            "approval.resolve",
            json!({ "id": id, "kind": kind, "decision": decision }),
        )
        .await
    }

    /// `exec.approval.list` (`server-methods/exec-approval.ts:142-154`):
    /// the exec approvals still waiting, as this connection may see them.
    pub async fn exec_approval_list(&self) -> Result<Vec<ApprovalRequested>, Error> {
        let rows: Vec<Value> = self.call("exec.approval.list", json!({})).await?;
        rows.into_iter()
            .map(|row| {
                ApprovalRequested::from_payload(ApprovalKind::Exec, row)
                    .map_err(|error| Error::Decode(format!("exec.approval.list row: {error}")))
            })
            .collect()
    }
}

/// `connect.params` (`schema/frames.ts:40-101`).
fn connect_params(config: &Connect, key: &DeviceKey, challenge: &Challenge) -> Value {
    let device_id = key.device_id();
    let payload = auth_payload(&AuthPayload {
        device_id: &device_id,
        client_id: CLIENT_ID,
        client_mode: CLIENT_MODE,
        role: ROLE,
        scopes: &SCOPES,
        signed_at_ms: challenge.ts,
        token: "",
        nonce: &challenge.nonce,
        platform: platform(),
        device_family: "",
    });
    json!({
        "minProtocol": PROTOCOL_VERSION,
        "maxProtocol": PROTOCOL_VERSION,
        "client": {
            "id": CLIENT_ID,
            "displayName": config.display_name,
            "version": config.client_version,
            "platform": platform(),
            "mode": CLIENT_MODE,
        },
        "role": ROLE,
        "scopes": SCOPES,
        "caps": CAPS,
        "device": {
            "id": device_id,
            "publicKey": key.public_key(),
            "signature": key.sign(&payload),
            "signedAt": challenge.ts,
            "nonce": challenge.nonce,
        },
        "userAgent": format!("nebo-link/{}", config.client_version),
    })
}

async fn read_challenge(stream: &mut SplitStream<Socket>) -> Result<Challenge, Error> {
    loop {
        match next_frame(stream).await? {
            Frame::Event { event, payload } if event == "connect.challenge" => {
                return serde_json::from_value(payload.unwrap_or(Value::Null))
                    .map_err(|error| Error::Decode(format!("connect.challenge: {error}")));
            }
            Frame::Event { .. } => continue,
            Frame::Response { .. } => {
                return Err(Error::Decode(
                    "a response arrived before the connect challenge".to_owned(),
                ));
            }
        }
    }
}

async fn read_hello(stream: &mut SplitStream<Socket>) -> Result<HelloOk, Error> {
    loop {
        match next_frame(stream).await? {
            Frame::Response {
                ok: true, payload, ..
            } => {
                return serde_json::from_value(payload.unwrap_or(Value::Null))
                    .map_err(|error| Error::Decode(format!("hello-ok: {error}")));
            }
            Frame::Response {
                ok: false, error, ..
            } => {
                let error = error
                    .ok_or_else(|| Error::Decode("connect refused without an error".to_owned()))?;
                return Err(match error.detail_code() {
                    Some(PROTOCOL_MISMATCH_CODE) => Error::ProtocolMismatch {
                        expected: error
                            .details
                            .as_ref()
                            .and_then(|details| details.get("expectedProtocol"))
                            .and_then(Value::as_u64)
                            .and_then(|version| u32::try_from(version).ok()),
                    },
                    Some(PAIRING_REQUIRED_CODE) => Error::PairingRequired {
                        message: error.message,
                    },
                    _ => Error::Handshake {
                        code: error.code,
                        message: error.message,
                    },
                });
            }
            Frame::Event { .. } => continue,
        }
    }
}

/// The next `res` or `event` frame; pings, pongs and binary frames are
/// skipped, and a close frame becomes the matching error.
async fn next_frame(stream: &mut SplitStream<Socket>) -> Result<Frame, Error> {
    loop {
        let message = match stream.next().await {
            Some(Ok(message)) => message,
            Some(Err(error)) => return Err(Error::Decode(error.to_string())),
            None => {
                return Err(Error::Closed {
                    code: 1006,
                    reason: "connection lost".to_owned(),
                });
            }
        };
        match message {
            Message::Text(text) => return parse_frame(&text),
            Message::Close(frame) => return Err(close_error(frame)),
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

fn parse_frame(text: &Utf8Bytes) -> Result<Frame, Error> {
    serde_json::from_str(text.as_str()).map_err(|error| Error::Decode(error.to_string()))
}

fn close_error(frame: Option<CloseFrame>) -> Error {
    let (code, reason) = match frame {
        Some(frame) => (u16::from(frame.code), frame.reason.to_string()),
        None => (1005, String::new()),
    };
    if code == PROTOCOL_MISMATCH_CLOSE {
        Error::ProtocolMismatch { expected: None }
    } else {
        Error::Closed { code, reason }
    }
}

async fn write_loop(mut sink: SplitSink<Socket, Message>, mut outgoing: mpsc::Receiver<Message>) {
    while let Some(message) = outgoing.recv().await {
        if sink.send(message).await.is_err() {
            break;
        }
    }
    let _ = sink.close().await;
}

async fn read_loop(
    mut stream: SplitStream<Socket>,
    shared: Arc<Shared>,
    outgoing: mpsc::Sender<Message>,
    events: mpsc::Sender<Event>,
) {
    let error = loop {
        let message = match stream.next().await {
            Some(Ok(message)) => message,
            Some(Err(error)) => break Error::Decode(error.to_string()),
            None => {
                break Error::Closed {
                    code: 1006,
                    reason: "connection lost".to_owned(),
                };
            }
        };
        match message {
            Message::Text(text) => match parse_frame(&text) {
                Ok(Frame::Response {
                    id,
                    ok,
                    payload,
                    error,
                }) => {
                    let sender = shared.pending.lock().expect("pending lock").remove(&id);
                    let Some(sender) = sender else { continue };
                    let result = if ok {
                        Ok(payload.unwrap_or(Value::Null))
                    } else {
                        let error = error.unwrap_or(ErrorShape {
                            code: "UNKNOWN".to_owned(),
                            message: "refused without an error".to_owned(),
                            details: None,
                            retryable: None,
                            retry_after_ms: None,
                        });
                        Err(Error::Rejected {
                            method: id,
                            code: error.code,
                            message: error.message,
                            retryable: error.retryable.unwrap_or(false),
                        })
                    };
                    let _ = sender.send(result);
                }
                Ok(Frame::Event { event, payload }) => {
                    let event = Event::parse(event, payload.unwrap_or(Value::Null));
                    track_active_run(&shared, &event);
                    if events.send(event).await.is_err() {
                        // The consumer is gone: nothing left to deliver to.
                        break Error::Closed {
                            code: 1000,
                            reason: "events dropped".to_owned(),
                        };
                    }
                }
                Err(error) => {
                    // One unreadable frame is not the end of the socket.
                    let _ = events
                        .send(Event::Other {
                            event: "unreadable".to_owned(),
                            payload: json!({ "error": error.to_string(), "text": text.as_str() }),
                        })
                        .await;
                }
            },
            Message::Ping(data) => {
                if outgoing.send(Message::Pong(data)).await.is_err() {
                    break Error::Closed {
                        code: 1006,
                        reason: "connection lost".to_owned(),
                    };
                }
            }
            Message::Close(frame) => break close_error(frame),
            Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    };
    shared.fail_pending(&error);
    *shared.closed.lock().expect("closed lock") = Some(error);
}

/// Keep `active_runs` current from the event stream.
fn track_active_run(shared: &Shared, event: &Event) {
    let Event::Chat(chat) = event else { return };
    let mut active = shared.active_runs.lock().expect("active runs lock");
    if chat.state.is_terminal() {
        if active.get(&chat.session_key) == Some(&chat.run_id) {
            active.remove(&chat.session_key);
        }
        let mut ended = shared.ended_runs.lock().expect("ended runs lock");
        if ended.len() == ENDED_RUNS {
            ended.pop_front();
        }
        ended.push_back(chat.run_id.clone());
    } else {
        active.insert(chat.session_key.clone(), chat.run_id.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_params_pin_protocol_and_sign_the_challenge() {
        let key = DeviceKey::from_secret([1u8; 32]);
        let access = ProxyAccess {
            base_path: "/t/bot".to_owned(),
            origin: "https://neboai.com".to_owned(),
            user_header: "x-nebo-user".to_owned(),
            identity: "owner".to_owned(),
            password: "secret".to_owned(),
        };
        let config = Connect::new("ws://127.0.0.1:1", &access, "203.0.113.10");
        let params = connect_params(
            &config,
            &key,
            &Challenge {
                nonce: "abc".to_owned(),
                ts: 1700000000000,
            },
        );
        assert_eq!(params["minProtocol"], 4);
        assert_eq!(params["maxProtocol"], 4);
        assert_eq!(params["client"]["id"], "openclaw-control-ui");
        assert_eq!(params["client"]["mode"], "ui");
        assert_eq!(params["role"], "operator");
        assert_eq!(params["caps"], json!(["tool-events", "approvals"]));
        assert_eq!(params["device"]["id"], key.device_id());
        assert_eq!(params["device"]["signedAt"], 1700000000000u64);
        assert_eq!(params["device"]["nonce"], "abc");
        let expected = auth_payload(&AuthPayload {
            device_id: &key.device_id(),
            client_id: CLIENT_ID,
            client_mode: CLIENT_MODE,
            role: ROLE,
            scopes: &SCOPES,
            signed_at_ms: 1700000000000,
            token: "",
            nonce: "abc",
            platform: platform(),
            device_family: "",
        });
        assert_eq!(params["device"]["signature"], key.sign(&expected));
        assert!(
            params.get("auth").is_none(),
            "trusted-proxy carries no token"
        );
    }

    #[test]
    fn close_1002_is_a_plain_mismatch() {
        let error = close_error(Some(CloseFrame {
            code: 1002.into(),
            reason: "protocol mismatch".into(),
        }));
        assert!(matches!(error, Error::ProtocolMismatch { expected: None }));
        assert_eq!(
            error.to_string(),
            "this OpenClaw speaks a different gateway protocol; Nebo Link speaks protocol 4. Update Nebo Link, or OpenClaw, so the two match"
        );
        let with_version = Error::ProtocolMismatch { expected: Some(5) };
        assert_eq!(
            with_version.to_string(),
            "this OpenClaw speaks gateway protocol 5; Nebo Link speaks protocol 4. Update Nebo Link, or OpenClaw, so the two match"
        );
    }
}
