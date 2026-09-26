//! The client of a Hermes profile's API server: runs with their event
//! stream, approvals, steering and stop, sessions and their messages, and
//! the capabilities probe. A plain async library over `reqwest` with no
//! state beyond the connection it is given.
//!
//! Every shape here is taken from `gateway/platforms/api_server.py` and
//! `api_server_runs.py` at hermes-agent `d0288be5b3` (2026-09-26); line
//! references in comments point there.
//!
//! ```no_run
//! use nebo_runtimes::hermes::runs::{Client, Event, NewRun};
//!
//! # async fn turn() -> Result<(), nebo_runtimes::hermes::runs::Error> {
//! let client = Client::new("http://127.0.0.1:8642", None, "0123456789abcdef0123");
//! let started = client
//!     .start_run(&NewRun {
//!         input: "hello".into(),
//!         session_id: Some("chat-1".into()),
//!         conversation_history: None,
//!         idempotency_key: "turn-1".into(),
//!     })
//!     .await?;
//! let mut events = client.events(&started.run_id, None).await?;
//! while let Some(envelope) = events.next().await {
//!     match envelope?.event {
//!         Event::MessageDelta { delta } => print!("{delta}"),
//!         Event::Completed(outcome) => println!("{:?}", outcome.usage),
//!         _ => {}
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::pin::Pin;
use std::time::Duration;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use reqwest::{Method, StatusCode, header};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::sse::{Frame, Parser};

/// Everything a request can fail with. Each variant names the request.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The API server could not be reached or the connection broke.
    #[error("{method} {path}: {source}")]
    Transport {
        method: String,
        path: String,
        #[source]
        source: reqwest::Error,
    },
    /// The API server answered with an error status. `code` is its
    /// machine-readable reason when it gave one (`run_not_found`,
    /// `approval_not_pending`, `gateway_auth_failed`, …).
    #[error("{method} {path}: HTTP {status}{}: {message}", code.as_deref().map(|c| format!(" ({c})")).unwrap_or_default())]
    Api {
        method: String,
        path: String,
        status: u16,
        code: Option<String>,
        message: String,
    },
    /// A response or event the client could not read.
    #[error("{path}: {message}")]
    Protocol { path: String, message: String },
    /// The event stream broke and could not be resumed.
    #[error("run {run_id}: the event stream was lost after {attempts} reconnects: {last}")]
    StreamLost {
        run_id: String,
        attempts: u32,
        last: String,
    },
}

impl Error {
    /// The HTTP status of an [`Error::Api`].
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Api { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// The API server's error code of an [`Error::Api`].
    pub fn code(&self) -> Option<&str> {
        match self {
            Error::Api { code, .. } => code.as_deref(),
            _ => None,
        }
    }
}

/// A connection to one profile's API server.
#[derive(Clone)]
pub struct Client {
    http: reqwest::Client,
    base: String,
    key: String,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client").field("base", &self.base).finish()
    }
}

/// How long a plain request may take. The event stream has no limit: the
/// server writes `: keepalive` while idle
/// (`CHAT_COMPLETIONS_SSE_KEEPALIVE_SECONDS`).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// How many times a broken event stream is reopened with `Last-Event-ID`.
const MAX_RECONNECTS: u32 = 3;

impl Client {
    /// A client of the API server at `base_url` (`http://127.0.0.1:8642`)
    /// with the profile's `API_SERVER_KEY`. A named `profile` is reached at
    /// `/p/<profile>/` on the default profile's listener
    /// (`api_server.py` `_make_profile_prefix_middleware`); `None` or
    /// `"default"` is the listener itself.
    pub fn new(base_url: &str, profile: Option<&str>, key: &str) -> Self {
        let mut base = base_url.trim_end_matches('/').to_owned();
        if let Some(name) = profile.filter(|name| *name != "default") {
            base.push_str("/p/");
            base.push_str(name);
        }
        Self {
            http: reqwest::Client::new(),
            base,
            key: key.to_owned(),
        }
    }

    /// The URL prefix every request goes to.
    pub fn base_url(&self) -> &str {
        &self.base
    }

    /// `GET /v1/capabilities` (`api_server.py` `_handle_capabilities`).
    pub async fn capabilities(&self) -> Result<Capabilities, Error> {
        self.json(Method::GET, "/v1/capabilities", None::<&()>).await
    }

    /// `GET /api/sessions` (`api_server.py` `_handle_list_sessions`), most
    /// recently active first.
    pub async fn sessions(&self, query: &SessionQuery) -> Result<SessionPage, Error> {
        let mut params = Vec::new();
        if let Some(limit) = query.limit {
            params.push(("limit", limit.to_string()));
        }
        if let Some(offset) = query.offset {
            params.push(("offset", offset.to_string()));
        }
        if let Some(source) = &query.source {
            params.push(("source", source.clone()));
        }
        self.json(Method::GET, &with_query("/api/sessions", &params), None::<&()>)
            .await
    }

    /// `POST /api/sessions` (`api_server.py` `_handle_create_session`): an
    /// empty session. Without `id` the server names it `api_<time>_<hex>`.
    pub async fn create_session(&self, new: &NewSession) -> Result<Session, Error> {
        #[derive(Deserialize)]
        struct Created {
            session: Session,
        }
        let created: Created = self.json(Method::POST, "/api/sessions", Some(new)).await?;
        Ok(created.session)
    }

    /// `GET /api/sessions/{id}/messages` (`api_server.py`
    /// `_handle_session_messages`). Without `limit` the server returns the
    /// latest 500 in order. [`MessagePage::session_id`] is the live id after
    /// any compression rotation (`db.resolve_resume_session_id`).
    pub async fn messages(
        &self,
        session_id: &str,
        query: &MessageQuery,
    ) -> Result<MessagePage, Error> {
        let mut params = Vec::new();
        if let Some(limit) = query.limit {
            params.push(("limit", limit.to_string()));
        }
        if let Some(offset) = query.offset {
            params.push(("offset", offset.to_string()));
        }
        if let Some(order) = query.order {
            params.push(("order", order.as_str().to_owned()));
        }
        let path = with_query(
            &format!("/api/sessions/{}/messages", encode(session_id)),
            &params,
        );
        self.json(Method::GET, &path, None::<&()>).await
    }

    /// `POST /v1/runs` (`api_server_runs.py` `_handle_runs`): one agent turn
    /// on `session_id`'s transcript, held by the server. Answered 202 as soon
    /// as the run is admitted (`_accepted_response`, `:461-467`); a repeated
    /// `idempotency_key` with the same body returns the original run with
    /// `replayed: true`, and with a different body fails 409
    /// `idempotency_key_conflict`.
    pub async fn start_run(&self, run: &NewRun) -> Result<RunStarted, Error> {
        let request = self
            .request(Method::POST, "/v1/runs")
            .header("Idempotency-Key", &run.idempotency_key)
            .json(&run);
        let response = self.send(request, Method::POST, "/v1/runs").await?;
        let replayed_header = response
            .headers()
            .get("Idempotency-Replayed")
            .is_some_and(|v| v == "true");
        let mut started: RunStarted = read_json(response, "/v1/runs").await?;
        started.replayed |= replayed_header;
        Ok(started)
    }

    /// `GET /v1/runs/{id}` (`api_server_runs.py` `_handle_get_run`): the
    /// pollable status. [`RunStatus::session_id`] is the session the run
    /// actually used: a client id from before a compression rotation is
    /// resolved to the live tip (`_resolve_live_session_id`, `:538-552`,
    /// applied at `:701-703`), so a caller following a session adopts this
    /// id when it differs from the one it sent.
    pub async fn run(&self, run_id: &str) -> Result<RunStatus, Error> {
        let path = format!("/v1/runs/{}", encode(run_id));
        self.json(Method::GET, &path, None::<&()>).await
    }

    /// `GET /v1/runs/{id}/events` (`api_server_runs.py` `_handle_run_events`)
    /// from the event after `after_seq` (`Last-Event-ID`), or from the start.
    /// The stream reopens itself with the last seen seq when the connection
    /// breaks before the run ends. The server keeps a run's events for five
    /// minutes after nobody is subscribed (`_RUN_STREAM_TTL`); after that the
    /// endpoint answers 404 and [`Client::run`] has the outcome.
    pub async fn events(&self, run_id: &str, after_seq: Option<u64>) -> Result<EventStream, Error> {
        let body = self.open_events(run_id, after_seq).await?;
        Ok(EventStream {
            client: self.clone(),
            run_id: run_id.to_owned(),
            body: Some(body),
            parser: Parser::default(),
            last_seq: after_seq,
            terminal: false,
            closed: false,
            reconnects: 0,
        })
    }

    /// `POST /v1/runs/{id}/approval` (`api_server_runs.py`
    /// `_handle_run_approval`, `:1168-1216`): answer the run's pending
    /// approval. `request_id` picks one request (the one from
    /// [`ApprovalRequest::request_id`]); without it the oldest pending one is
    /// answered. An empty id is refused 400 (`:1189`), so pass `None` for
    /// none. Fails 409 `approval_not_active` when the run has no approval
    /// session and `approval_not_pending` when nothing is waiting.
    pub async fn approve(
        &self,
        run_id: &str,
        choice: Choice,
        request_id: Option<&str>,
    ) -> Result<ApprovalOutcome, Error> {
        #[derive(Serialize)]
        struct Body<'a> {
            choice: Choice,
            #[serde(skip_serializing_if = "Option::is_none")]
            request_id: Option<&'a str>,
        }
        let path = format!("/v1/runs/{}/approval", encode(run_id));
        self.json(Method::POST, &path, Some(&Body { choice, request_id }))
            .await
    }

    /// `POST /v1/runs/{id}/steer` (`api_server_runs.py` `_handle_steer_run`,
    /// `:1219-1250`): hand a running agent more text mid-turn. Fails 409
    /// `run_not_accepting_steer` unless the run is `running`, and 404 on a
    /// server without [`STEER_FEATURE`].
    pub async fn steer(&self, run_id: &str, text: &str) -> Result<(), Error> {
        #[derive(Serialize)]
        struct Body<'a> {
            input: &'a str,
        }
        #[derive(Deserialize)]
        struct Steered {
            accepted: bool,
        }
        let path = format!("/v1/runs/{}/steer", encode(run_id));
        let steered: Steered = self
            .json(Method::POST, &path, Some(&Body { input: text }))
            .await?;
        if steered.accepted {
            Ok(())
        } else {
            Err(Error::Protocol {
                path,
                message: "the run did not accept the steer text".to_owned(),
            })
        }
    }

    /// `POST /v1/runs/{id}/stop` (`api_server_runs.py` `_handle_stop_run`,
    /// `:1253-1274`): interrupt the run. The run is `stopping` until the
    /// agent yields, then `cancelled` on the event stream. A run that already
    /// ended is returned as it is.
    pub async fn stop(&self, run_id: &str) -> Result<StopOutcome, Error> {
        let path = format!("/v1/runs/{}/stop", encode(run_id));
        let value: Value = self.json(Method::POST, &path, None::<&()>).await?;
        if value.get("status").and_then(Value::as_str) == Some("stopping") {
            return Ok(StopOutcome::Stopping);
        }
        serde_json::from_value(value)
            .map(|status| StopOutcome::Ended(Box::new(status)))
            .map_err(|error| Error::Protocol {
                path,
                message: error.to_string(),
            })
    }

    async fn open_events(
        &self,
        run_id: &str,
        after_seq: Option<u64>,
    ) -> Result<Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>, Error> {
        let path = format!("/v1/runs/{}/events", encode(run_id));
        let mut request = self
            .http
            .request(Method::GET, format!("{}{path}", self.base))
            .bearer_auth(&self.key)
            .header(header::ACCEPT, "text/event-stream");
        if let Some(seq) = after_seq {
            request = request.header("Last-Event-ID", seq.to_string());
        }
        let response = self.send(request, Method::GET, &path).await?;
        Ok(Box::pin(response.bytes_stream()))
    }

    fn request(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(&self.key)
            .timeout(REQUEST_TIMEOUT)
    }

    async fn json<T: for<'de> Deserialize<'de>>(
        &self,
        method: Method,
        path: &str,
        body: Option<&impl Serialize>,
    ) -> Result<T, Error> {
        let mut request = self.request(method.clone(), path);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = self.send(request, method, path).await?;
        read_json(response, path).await
    }

    /// Sends the request and turns a non-2xx answer into [`Error::Api`].
    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        method: Method,
        path: &str,
    ) -> Result<reqwest::Response, Error> {
        let response = request.send().await.map_err(|source| Error::Transport {
            method: method.to_string(),
            path: path.to_owned(),
            source,
        })?;
        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        let text = response.text().await.unwrap_or_default();
        let (code, message) = error_body(&text, status);
        Err(Error::Api {
            method: method.to_string(),
            path: path.to_owned(),
            status: status.as_u16(),
            code,
            message,
        })
    }
}

/// `{"error": {"message", "type", "param", "code"}}` (`api_server.py`
/// `_openai_error`) or `{"error": "<text>"}` (the profile-prefix middleware).
fn error_body(text: &str, status: StatusCode) -> (Option<String>, String) {
    let value: Value = serde_json::from_str(text).unwrap_or(Value::Null);
    match value.get("error") {
        Some(Value::Object(error)) => (
            error.get("code").and_then(Value::as_str).map(str::to_owned),
            error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or(text)
                .to_owned(),
        ),
        Some(Value::String(message)) => (None, message.clone()),
        _ => (
            None,
            if text.trim().is_empty() {
                status.to_string()
            } else {
                text.trim().to_owned()
            },
        ),
    }
}

async fn read_json<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
    path: &str,
) -> Result<T, Error> {
    let text = response.text().await.map_err(|source| Error::Transport {
        method: "read".to_owned(),
        path: path.to_owned(),
        source,
    })?;
    serde_json::from_str(&text).map_err(|error| Error::Protocol {
        path: path.to_owned(),
        message: format!("unexpected response: {error}"),
    })
}

fn with_query(path: &str, params: &[(&str, String)]) -> String {
    if params.is_empty() {
        return path.to_owned();
    }
    let query: Vec<String> = params
        .iter()
        .map(|(key, value)| format!("{key}={}", encode(value)))
        .collect();
    format!("{path}?{}", query.join("&"))
}

/// Percent-encodes a path segment or query value.
fn encode(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

// -- Capabilities -----------------------------------------------------------

/// `GET /v1/capabilities` (`api_server.py` `_handle_capabilities`,
/// `:2494-2532`).
#[derive(Debug, Clone, Deserialize)]
pub struct Capabilities {
    #[serde(default)]
    pub platform: String,
    /// The virtual model name the server answers as (the profile's name, or
    /// `hermes-agent` for the default profile).
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub auth: Auth,
    /// Feature flags (`_STATIC_FEATURE_FLAGS`, `:68-77`, plus the dynamic
    /// ones). Booleans, and strings for the two header names.
    #[serde(default)]
    pub features: Map<String, Value>,
    /// `name -> {method, path}` (`_CAPABILITY_ENDPOINTS`, `:79-102`).
    #[serde(default)]
    pub endpoints: Map<String, Value>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Auth {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub required: bool,
}

/// The feature flags a chat cannot work without. All are static `True` in
/// `_STATIC_FEATURE_FLAGS` (`:68-77`) and `_handle_capabilities` (`:2508`)
/// at the pinned commit, and all are advertised by the released v0.19.0.
pub const REQUIRED_FEATURES: [&str; 8] = [
    "run_submission",
    "run_status",
    "run_events_sse",
    "run_stop",
    "run_approval_response",
    "tool_progress_events",
    "approval_events",
    "session_resources",
];

/// The flag for [`Client::steer`]. Not required: the released v0.19.0 has
/// neither the flag nor the endpoint (they came later, `_STATIC_FEATURE_FLAGS`
/// at the pinned commit), and a caller without it waits for the run to end
/// before sending the next message.
pub const STEER_FEATURE: &str = "run_steer";

impl Capabilities {
    /// The [`REQUIRED_FEATURES`] this server does not advertise as `true`.
    pub fn missing(&self) -> Vec<&'static str> {
        REQUIRED_FEATURES
            .iter()
            .copied()
            .filter(|flag| !self.has(flag))
            .collect()
    }

    /// Whether `flag` is advertised as `true`.
    pub fn has(&self, flag: &str) -> bool {
        self.features.get(flag).and_then(Value::as_bool) == Some(true)
    }
}

// -- Sessions ---------------------------------------------------------------

/// Parameters of [`Client::sessions`].
#[derive(Debug, Clone, Default)]
pub struct SessionQuery {
    /// At most 200 (the server's cap); the server's default is 50.
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    /// Only sessions created by this source (`api_server`, `cli`, …).
    pub source: Option<String>,
}

/// One page of [`Client::sessions`].
#[derive(Debug, Clone, Deserialize)]
pub struct SessionPage {
    #[serde(default)]
    pub data: Vec<Session>,
    #[serde(default)]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
    #[serde(default)]
    pub has_more: bool,
}

/// A session as `api_server.py` `_session_response` (`:2968-2983`) returns
/// it.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Session {
    pub id: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    /// Unix seconds.
    #[serde(default)]
    pub started_at: Option<f64>,
    #[serde(default)]
    pub ended_at: Option<f64>,
    #[serde(default)]
    pub last_active: Option<f64>,
    #[serde(default)]
    pub message_count: Option<u64>,
    #[serde(default)]
    pub tool_call_count: Option<u64>,
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
    /// The first user message, shortened.
    #[serde(default)]
    pub preview: Option<String>,
    /// The session this one continued (compression rotation or fork).
    #[serde(default)]
    pub parent_session_id: Option<String>,
    #[serde(default)]
    pub pinned: bool,
    #[serde(default)]
    pub archived: bool,
    #[serde(default)]
    pub hidden: bool,
}

/// Parameters of [`Client::create_session`].
#[derive(Debug, Clone, Default, Serialize)]
pub struct NewSession {
    /// The session id to create; `None` lets the server name it. Fails 409
    /// `session_exists` when taken.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Recorded as the session's `source`; the server's default is
    /// `api_server`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// Parameters of [`Client::messages`].
#[derive(Debug, Clone, Default)]
pub struct MessageQuery {
    /// At most 500 (the server's cap).
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub order: Option<Order>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Oldest,
    Latest,
}

impl Order {
    pub fn as_str(self) -> &'static str {
        match self {
            Order::Oldest => "oldest",
            Order::Latest => "latest",
        }
    }
}

/// One page of [`Client::messages`].
#[derive(Debug, Clone, Deserialize)]
pub struct MessagePage {
    /// The live session id the messages were read from (see
    /// [`Client::messages`]).
    pub session_id: String,
    #[serde(default)]
    pub data: Vec<Message>,
    #[serde(default)]
    pub pagination: Pagination,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Pagination {
    #[serde(default)]
    pub limit: u32,
    #[serde(default)]
    pub offset: u32,
    #[serde(default)]
    pub order: String,
    #[serde(default)]
    pub returned: u32,
}

/// A stored message as `api_server.py` `_message_response` (`:2986-2992`)
/// returns it.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Message {
    #[serde(default)]
    pub id: Option<Value>,
    pub role: String,
    /// Text, or a list of content parts for a multimodal message; see
    /// [`Message::text`].
    #[serde(default)]
    pub content: Value,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub tool_calls: Option<Value>,
    #[serde(default)]
    pub tool_name: Option<String>,
    /// Unix seconds.
    #[serde(default)]
    pub timestamp: Option<f64>,
    #[serde(default)]
    pub finish_reason: Option<String>,
    #[serde(default)]
    pub reasoning: Option<String>,
    #[serde(default)]
    pub display_kind: Option<String>,
}

impl Message {
    /// The message's text: the string content, or the `text` parts of a
    /// content list joined.
    pub fn text(&self) -> String {
        match &self.content {
            Value::String(text) => text.clone(),
            Value::Array(parts) => parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join(""),
            Value::Null => String::new(),
            other => other.to_string(),
        }
    }
}

// -- Runs -------------------------------------------------------------------

/// Parameters of [`Client::start_run`].
#[derive(Debug, Clone, Serialize)]
pub struct NewRun {
    /// The user's message.
    pub input: String,
    /// The session to continue, or `None` for a fresh one named after the
    /// run (`:703`). The server at the pinned commit loads that session's
    /// transcript itself (`_handle_runs`, `:713-714`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// Earlier turns to run with, for a server that does not load them: the
    /// released v0.19.0 runs every `/v1/runs` turn with an empty history
    /// (its `_handle_runs` takes history only from this field,
    /// `previous_response_id` or a multi-message `input`; the agent log
    /// shows `history=0`) while still storing the turn on the session. When
    /// given, the server uses this instead of the stored transcript
    /// (`_resolve_conversation_history`, `:424-425`), so leave it `None` on
    /// a server that loads the session.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conversation_history: Option<Vec<HistoryMessage>>,
    /// 1–255 visible ASCII characters, unique per turn (`:638-642`).
    #[serde(skip)]
    pub idempotency_key: String,
}

/// One earlier turn in [`NewRun::conversation_history`] (`:434-439`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryMessage {
    /// `user` or `assistant`.
    pub role: String,
    pub content: String,
}

/// The 202 answer of [`Client::start_run`].
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct RunStarted {
    pub run_id: String,
    /// `started` for a new run; the run's current status on a replay.
    pub status: String,
    /// The idempotency key was seen before: this is the earlier run.
    #[serde(default)]
    pub replayed: bool,
}

/// A run's lifecycle state (`api_server_runs.py` `_set_run_status` callers;
/// terminal ones in `TERMINAL_STATUSES`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Queued,
    Running,
    WaitingForApproval,
    Stopping,
    Completed,
    Failed,
    Cancelled,
    /// The gateway shut down or restarted under the run (`:286-295`,
    /// `:409-413`).
    Interrupted,
    #[serde(untagged)]
    Other(String),
}

impl RunState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            RunState::Completed | RunState::Failed | RunState::Cancelled | RunState::Interrupted
        )
    }
}

/// `GET /v1/runs/{id}` (`api_server_runs.py` `_set_run_status`, `:258-283`,
/// with the terminal fields of `_finish`, `:937-948`).
#[derive(Debug, Clone, Deserialize)]
pub struct RunStatus {
    pub run_id: String,
    pub status: RunState,
    /// The session the run used, after rotation (see [`Client::run`]).
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub created_at: Option<f64>,
    #[serde(default)]
    pub updated_at: Option<f64>,
    /// The name of the last event put on the stream.
    #[serde(default)]
    pub last_event: Option<String>,
    /// The final answer of a completed run.
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub usage: Option<Usage>,
    #[serde(default)]
    pub runtime: Option<ServedRuntime>,
    /// The pending request while `waiting_for_approval`.
    #[serde(default)]
    pub approval: Option<ApprovalRequest>,
    /// Steer text the agent never consumed, for the caller to resend.
    #[serde(default)]
    pub pending_steer: Option<Value>,
    /// Set while the gateway drains: the run will end `interrupted`.
    #[serde(default)]
    pub shutdown_requested_at: Option<f64>,
}

/// Token counts of one run (`api_server_runs.py` `_USAGE_FIELDS`, `:88-91`).
/// The agent behind a run is created for that run (`api_server.py`
/// `_create_agent`, `:2396`) with its counters at zero (`agent/agent_init.py`
/// `_USAGE_STATE`) and nothing reloads them from the session, so these are
/// the run's own totals over every model call of the turn, not a running
/// total of the session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    #[serde(default)]
    pub cache_read_tokens: u64,
    #[serde(default)]
    pub cache_write_tokens: u64,
}

/// The provider and model that served a run (`api_server_runs.py`
/// `_served_runtime`, `:769-778`, shaped by `_sanitize_runtime_metadata`).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct ServedRuntime {
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub model: String,
    /// `global`, `raw_request` or `model_routes`.
    #[serde(default)]
    pub route_source: Option<String>,
}

/// The answer to an approval (`tools/approval.py` `resolve_gateway_approval`;
/// aliases `approve`/`allow` are folded into `once` server-side,
/// `api_server_runs.py` `:1165`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Choice {
    /// Run this one command.
    Once,
    /// Allow this command for the rest of the session.
    Session,
    /// Allow it permanently.
    Always,
    Deny,
}

impl Choice {
    pub fn as_str(self) -> &'static str {
        match self {
            Choice::Once => "once",
            Choice::Session => "session",
            Choice::Always => "always",
            Choice::Deny => "deny",
        }
    }
}

/// An `approval.request` event (`api_server.py` `_approval_request_event`,
/// `:113-126`, over the data `tools/approval.py` builds at `:862-869`) and
/// the `approval` field of a waiting run's status.
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct ApprovalRequest {
    /// The id to answer with (`tools/approval_gateway_wait.py`
    /// `_ApprovalEntry`). `None` on a v0.19.0 server, which sends none;
    /// the answer then resolves the oldest pending request.
    #[serde(default, deserialize_with = "non_empty")]
    pub request_id: Option<String>,
    /// The command, secrets redacted (`gateway/run.py`
    /// `_redact_approval_command`).
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub pattern_key: Option<String>,
    /// The choices the server accepts for this request
    /// (`_approval_event_choices`, `:107-110`).
    #[serde(default)]
    pub choices: Vec<Choice>,
    /// The guardian model advised denying; only `once` and `deny` are
    /// offered.
    #[serde(default)]
    pub smart_denied: bool,
}

/// An absent or empty string as `None`.
fn non_empty<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    let value: Option<String> = Option::deserialize(deserializer)?;
    Ok(value.filter(|text| !text.is_empty()))
}

/// The answer of [`Client::approve`] (`api_server_runs.py` `:1214-1216`).
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ApprovalOutcome {
    pub run_id: String,
    pub choice: Choice,
    #[serde(default)]
    pub request_id: Option<String>,
    /// How many pending requests the answer resolved.
    #[serde(default)]
    pub resolved: u64,
}

/// The answer of [`Client::stop`].
#[derive(Debug, Clone)]
pub enum StopOutcome {
    /// The agent was asked to stop; `run.cancelled` follows on the stream.
    Stopping,
    /// The run had already ended; nothing to stop.
    Ended(Box<RunStatus>),
}

// -- Events -----------------------------------------------------------------

/// One event of a run's stream with its envelope (`api_server_runs.py`
/// `_run_event`, `:176-178`; `seq` from the frame id, `:1101-1104`).
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope {
    /// The stream position, to resume from (`Last-Event-ID`). `None` on the
    /// out-of-band `replay.truncated` notice.
    pub seq: Option<u64>,
    pub run_id: String,
    /// Unix seconds.
    pub timestamp: f64,
    pub event: Event,
}

/// The events a run emits, in the order `_execute_run` (`:915-1003`) and
/// its callbacks produce them.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// A piece of the answer (`_text_cb`, `:921-925`).
    MessageDelta { delta: String },
    /// Mid-turn commentary beside tool calls (`_interim_cb`, `:927-935`).
    /// `already_streamed` means it also went out as deltas.
    MessageInterim { text: String, already_streamed: bool },
    /// Reasoning text (`_FIXED_EVENT_FIELDS["reasoning.available"]`, `:97`).
    Reasoning { text: String },
    /// A tool call began; `preview` shows its arguments (`:94`).
    ToolStarted { tool: String, preview: String },
    /// A tool call ended (`:95-96`, preview at `:333-335`: redacted, at most
    /// 500 characters).
    ToolCompleted {
        tool: String,
        /// Seconds.
        duration: f64,
        error: bool,
        preview: String,
    },
    /// The agent needs the owner's decision; the run is
    /// `waiting_for_approval` until [`Client::approve`] answers
    /// (`_make_approval_notify`, `:850-861`).
    ApprovalRequest(ApprovalRequest),
    /// An approval was answered, from anywhere (`:1213`).
    ApprovalResponded {
        choice: Choice,
        request_id: Option<String>,
        resolved: u64,
    },
    /// Steer text was accepted (`:1249`).
    Steered { accepted: bool },
    /// A subagent delegation began or ended (`:337-347`); the fields are
    /// the redacted lifecycle set of `_SUBAGENT_EVENT_KEYS`.
    Subagent {
        started: bool,
        fields: Map<String, Value>,
    },
    /// The run ended with an answer (`_finish`, `:986`).
    Completed(RunOutcome),
    /// The run ended in error (`:977`, `:993`, `:996`).
    Failed(RunOutcome),
    /// The run was stopped (`:974`, `:988`).
    Cancelled(RunOutcome),
    /// The gateway went down under the run (`:940-943`, `:954`).
    Interrupted(RunOutcome),
    /// A resume asked for events the server no longer holds
    /// (`_handle_run_events`, `:1115-1123`); the stream continues from
    /// `oldest_retained_seq`.
    ReplayTruncated {
        oldest_retained_seq: u64,
        requested_seq: Option<u64>,
    },
    /// An event this client does not know; `fields` is the payload without
    /// the envelope.
    Other {
        name: String,
        fields: Map<String, Value>,
    },
}

impl Event {
    /// Whether this event ends the run (`run.completed`, `run.failed`,
    /// `run.cancelled`, `run.interrupted`).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Event::Completed(_) | Event::Failed(_) | Event::Cancelled(_) | Event::Interrupted(_)
        )
    }
}

/// The fields of a terminal event (`terminal_run_status`, `:181-200`, plus
/// `output`/`usage`/`runtime` on completion and `error` on failure).
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
pub struct RunOutcome {
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub usage: Option<Usage>,
    #[serde(default)]
    pub runtime: Option<ServedRuntime>,
    #[serde(default)]
    pub completed: bool,
    #[serde(default)]
    pub partial: bool,
    #[serde(default)]
    pub interrupted: bool,
    /// Why a turn ended short (`failed`/`cancelled` only).
    #[serde(default)]
    pub turn_exit_reason: Option<String>,
    /// Steer text the agent never consumed.
    #[serde(default)]
    pub pending_steer: Option<Value>,
}

/// The payload of `name` as `T`.
fn typed<T: for<'de> Deserialize<'de>>(name: &str, fields: Map<String, Value>) -> Result<T, String> {
    serde_json::from_value(Value::Object(fields)).map_err(|e| format!("{name}: {e}"))
}

/// Parses one `data:` payload.
fn parse_event(data: &str) -> Result<Envelope, String> {
    let Value::Object(mut fields) = serde_json::from_str(data).map_err(|e| e.to_string())? else {
        return Err("event is not a JSON object".to_owned());
    };
    let name = match fields.remove("event") {
        Some(Value::String(name)) => name,
        _ => return Err("event has no name".to_owned()),
    };
    let run_id = match fields.remove("run_id") {
        Some(Value::String(id)) => id,
        _ => String::new(),
    };
    let timestamp = fields
        .remove("timestamp")
        .and_then(|v| v.as_f64())
        .unwrap_or_default();
    let seq = fields.remove("seq").and_then(|v| v.as_u64());
    let event = match name.as_str() {
        "message.delta" => {
            #[derive(Deserialize)]
            struct Delta {
                #[serde(default)]
                delta: String,
            }
            let Delta { delta } = typed(&name, fields)?;
            Event::MessageDelta { delta }
        }
        "message.interim" => {
            #[derive(Deserialize)]
            struct Interim {
                #[serde(default)]
                text: String,
                #[serde(default)]
                already_streamed: bool,
            }
            let Interim {
                text,
                already_streamed,
            } = typed(&name, fields)?;
            Event::MessageInterim {
                text,
                already_streamed,
            }
        }
        "reasoning.available" => {
            #[derive(Deserialize)]
            struct Reasoning {
                #[serde(default)]
                text: String,
            }
            let Reasoning { text } = typed(&name, fields)?;
            Event::Reasoning { text }
        }
        "tool.started" => {
            #[derive(Deserialize)]
            struct Started {
                #[serde(default)]
                tool: String,
                #[serde(default)]
                preview: String,
            }
            let Started { tool, preview } = typed(&name, fields)?;
            Event::ToolStarted { tool, preview }
        }
        "tool.completed" => {
            #[derive(Deserialize)]
            struct Done {
                #[serde(default)]
                tool: String,
                #[serde(default)]
                duration: f64,
                #[serde(default)]
                error: bool,
                #[serde(default)]
                preview: String,
            }
            let Done {
                tool,
                duration,
                error,
                preview,
            } = typed(&name, fields)?;
            Event::ToolCompleted {
                tool,
                duration,
                error,
                preview,
            }
        }
        "approval.request" => Event::ApprovalRequest(typed(&name, fields)?),
        "approval.responded" => {
            #[derive(Deserialize)]
            struct Responded {
                choice: Choice,
                #[serde(default)]
                request_id: Option<String>,
                #[serde(default)]
                resolved: u64,
            }
            let Responded {
                choice,
                request_id,
                resolved,
            } = typed(&name, fields)?;
            Event::ApprovalResponded {
                choice,
                request_id,
                resolved,
            }
        }
        "run.steered" => {
            #[derive(Deserialize)]
            struct Steered {
                #[serde(default)]
                accepted: bool,
            }
            let Steered { accepted } = typed(&name, fields)?;
            Event::Steered { accepted }
        }
        "subagent.start" => Event::Subagent {
            started: true,
            fields,
        },
        "subagent.complete" => Event::Subagent {
            started: false,
            fields,
        },
        "run.completed" => Event::Completed(typed(&name, fields)?),
        "run.failed" => Event::Failed(typed(&name, fields)?),
        "run.cancelled" => Event::Cancelled(typed(&name, fields)?),
        "run.interrupted" => Event::Interrupted(typed(&name, fields)?),
        "replay.truncated" => {
            #[derive(Deserialize)]
            struct Truncated {
                #[serde(default)]
                oldest_retained_seq: u64,
                #[serde(default)]
                requested_seq: Option<u64>,
            }
            let Truncated {
                oldest_retained_seq,
                requested_seq,
            } = typed(&name, fields)?;
            Event::ReplayTruncated {
                oldest_retained_seq,
                requested_seq,
            }
        }
        _ => Event::Other { name, fields },
    };
    Ok(Envelope {
        seq,
        run_id,
        timestamp,
        event,
    })
}

/// A run's event stream from [`Client::events`].
pub struct EventStream {
    client: Client,
    run_id: String,
    body: Option<Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>>,
    parser: Parser,
    last_seq: Option<u64>,
    /// A terminal event was delivered.
    terminal: bool,
    /// The server said `: stream closed`, or the stream failed for good.
    closed: bool,
    reconnects: u32,
}

impl fmt::Debug for EventStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStream")
            .field("run_id", &self.run_id)
            .field("last_seq", &self.last_seq)
            .field("terminal", &self.terminal)
            .finish()
    }
}

impl EventStream {
    /// The seq of the last event delivered: what a new [`Client::events`]
    /// call resumes from.
    pub fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }

    /// The next event, or `None` once the run has ended and the server
    /// closed the stream. An `Err` ends the stream: `next` then returns
    /// `None`.
    pub async fn next(&mut self) -> Option<Result<Envelope, Error>> {
        loop {
            while let Some(frame) = self.parser.next_frame() {
                match frame {
                    Frame::Comment(comment) if comment == "stream closed" => {
                        self.closed = true;
                        return None;
                    }
                    Frame::Comment(_) => {}
                    Frame::Data { id, data } => {
                        let envelope = match parse_event(&data) {
                            Ok(envelope) => envelope,
                            Err(message) => {
                                self.closed = true;
                                return Some(Err(Error::Protocol {
                                    path: format!("/v1/runs/{}/events", self.run_id),
                                    message,
                                }));
                            }
                        };
                        let seq = id.or(envelope.seq);
                        if let Some(seq) = seq {
                            // A replay never repeats a delivered event
                            // (`stream.attach(last_seq)`, `:150`), but a
                            // reopened stream is held to that here too.
                            if self.last_seq.is_some_and(|last| seq <= last) {
                                continue;
                            }
                            self.last_seq = Some(seq);
                        }
                        self.terminal |= envelope.event.is_terminal();
                        return Some(Ok(Envelope { seq, ..envelope }));
                    }
                }
            }
            if self.closed {
                return None;
            }
            let Some(body) = self.body.as_mut() else {
                match self.reopen().await {
                    Ok(()) => continue,
                    Err(error) => {
                        self.closed = true;
                        return Some(Err(error));
                    }
                }
            };
            match body.next().await {
                Some(Ok(chunk)) => self.parser.push(&chunk),
                Some(Err(error)) => {
                    self.body = None;
                    if self.terminal {
                        self.closed = true;
                        return None;
                    }
                    if self.reconnects >= MAX_RECONNECTS {
                        self.closed = true;
                        return Some(Err(Error::StreamLost {
                            run_id: self.run_id.clone(),
                            attempts: self.reconnects,
                            last: error.to_string(),
                        }));
                    }
                }
                None => {
                    self.body = None;
                    if self.terminal {
                        self.closed = true;
                        return None;
                    }
                    if self.reconnects >= MAX_RECONNECTS {
                        self.closed = true;
                        return Some(Err(Error::StreamLost {
                            run_id: self.run_id.clone(),
                            attempts: self.reconnects,
                            last: "the connection closed before the run ended".to_owned(),
                        }));
                    }
                }
            }
        }
    }

    /// Reopens the stream after the last delivered event.
    async fn reopen(&mut self) -> Result<(), Error> {
        self.reconnects += 1;
        self.parser = Parser::default();
        self.body = Some(self.client.open_events(&self.run_id, self.last_seq).await?);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_prefix_and_encoding() {
        assert_eq!(
            Client::new("http://127.0.0.1:8642/", None, "k").base_url(),
            "http://127.0.0.1:8642"
        );
        assert_eq!(
            Client::new("http://127.0.0.1:8642", Some("default"), "k").base_url(),
            "http://127.0.0.1:8642"
        );
        assert_eq!(
            Client::new("http://127.0.0.1:8642", Some("coder"), "k").base_url(),
            "http://127.0.0.1:8642/p/coder"
        );
        assert_eq!(encode("api_1 x/y"), "api_1%20x%2Fy");
        assert_eq!(
            with_query("/api/sessions", &[("limit", "5".into()), ("source", "a b".into())]),
            "/api/sessions?limit=5&source=a%20b"
        );
    }

    #[test]
    fn error_bodies() {
        let (code, message) = error_body(
            r#"{"error":{"message":"Run not found: r","type":"invalid_request_error","param":null,"code":"run_not_found"}}"#,
            StatusCode::NOT_FOUND,
        );
        assert_eq!(code.as_deref(), Some("run_not_found"));
        assert_eq!(message, "Run not found: r");
        let (code, message) = error_body(
            r#"{"error":"Unknown or unconfigured profile"}"#,
            StatusCode::NOT_FOUND,
        );
        assert_eq!(code, None);
        assert_eq!(message, "Unknown or unconfigured profile");
        let (_, message) = error_body("", StatusCode::BAD_GATEWAY);
        assert_eq!(message, "502 Bad Gateway");
    }

    #[test]
    fn run_states() {
        let state: RunState = serde_json::from_str("\"waiting_for_approval\"").unwrap();
        assert_eq!(state, RunState::WaitingForApproval);
        let state: RunState = serde_json::from_str("\"draining\"").unwrap();
        assert_eq!(state, RunState::Other("draining".into()));
        assert!(RunState::Interrupted.is_terminal());
        assert!(!RunState::Stopping.is_terminal());
    }

    #[test]
    fn message_text() {
        let message: Message = serde_json::from_value(serde_json::json!({
            "role": "user",
            "content": [{"type": "text", "text": "a"}, {"type": "image_url"}, {"type": "text", "text": "b"}]
        }))
        .unwrap();
        assert_eq!(message.text(), "ab");
    }

    #[test]
    fn unknown_events_keep_their_fields() {
        let envelope = parse_event(
            r#"{"event":"session.compacted","run_id":"run_1","timestamp":1.5,"seq":4,"from":"a"}"#,
        )
        .unwrap();
        assert_eq!(envelope.seq, Some(4));
        assert_eq!(envelope.timestamp, 1.5);
        assert_eq!(
            envelope.event,
            Event::Other {
                name: "session.compacted".into(),
                fields: serde_json::from_str(r#"{"from":"a"}"#).unwrap()
            }
        );
        assert!(parse_event(r#"{"run_id":"r"}"#).is_err());
        assert!(parse_event("[]").is_err());
    }
}
