//! The gateway wire contract this client speaks: protocol 4 as published by
//! `packages/gateway-protocol` (`src/version.ts:2`). Frame envelopes are in
//! `src/schema/frames.ts`; method and event payloads in the schema module
//! named beside each type. Every shape here was read at the OpenClaw tip on
//! 2026-09-25; the line numbers cite that tip.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// `PROTOCOL_VERSION` (`packages/gateway-protocol/src/version.ts:2`). The
/// gateway admits a client whose `[minProtocol, maxProtocol]` contains its
/// own version (`connect-admission.ts:243-244`); operator clients get no
/// N-1 allowance (`:249-254`), so the range is pinned to exactly this.
pub const PROTOCOL_VERSION: u32 = 4;

/// `GATEWAY_CLIENT_IDS.CONTROL_UI` (`packages/gateway-protocol/src/client-info.ts:12`).
/// The client id is a closed enum on the wire (`schema/primitives.ts:63`),
/// and only browser-operator-UI, webchat and native-app ids qualify for
/// trusted-proxy device auto-approval
/// (`ws-connection/connect-pairing-approval-plan.ts:160-166`, via
/// `isBrowserOperatorUiClient`, `src/utils/message-channel.ts:72-78`); a
/// `cli` or `gateway-client` id would sit in a pairing prompt instead.
pub const CLIENT_ID: &str = "openclaw-control-ui";
/// `GATEWAY_CLIENT_MODES.UI` (`client-info.ts:43`).
pub const CLIENT_MODE: &str = "ui";
/// `connect.params.role` (`docs/gateway/protocol/handshake.md` "Roles").
pub const ROLE: &str = "operator";
/// The scopes a chat-and-approvals client requests
/// (`docs/gateway/clients.md:57-64`). Trusted-proxy device auto-approval
/// grants the requested scopes that fall inside its defaults
/// (`connect-pairing-approval-plan.ts:28-56`: read, write, approvals,
/// questions), and `gateway.auth.identityScopes[<identity>]` adds
/// `operator.admin` on top (`connect-admission.ts:138-164`).
pub const SCOPES: [&str; 3] = ["operator.read", "operator.write", "operator.approvals"];
/// `connect.params.caps` (`client-info.ts:79-97`). `tool-events` registers
/// the connection for a run's tool events (`chat-send-agent-dispatch.ts:409-414`);
/// `approvals` marks it an approval client. `session-scoped-events` is
/// deliberately absent: it withholds every `chat` and `agent` event until
/// the connection subscribes to that session (`server-broadcast.ts:42-51,386-417`).
pub const CAPS: [&str; 2] = ["tool-events", "approvals"];

/// `ErrorShapeSchema` (`frames.ts:180-186`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorShape {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub details: Option<Value>,
    #[serde(default)]
    pub retryable: Option<bool>,
    #[serde(default)]
    pub retry_after_ms: Option<u64>,
}

impl ErrorShape {
    /// `details.code`, the stable machine-readable reason
    /// (`docs/gateway/protocol/transport.md:80-89`).
    pub fn detail_code(&self) -> Option<&str> {
        self.details.as_ref()?.get("code")?.as_str()
    }
}

/// `RequestFrameSchema` (`frames.ts:189-196`).
#[derive(Serialize)]
pub(crate) struct RequestFrame<'a> {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub id: &'a str,
    pub method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<&'a Value>,
}

/// A frame from the gateway: `ResponseFrameSchema` (`frames.ts:199-205`) or
/// `EventFrameSchema` (`frames.ts:208-215`).
#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum Frame {
    #[serde(rename = "res")]
    Response {
        id: String,
        ok: bool,
        #[serde(default)]
        payload: Option<Value>,
        #[serde(default)]
        error: Option<ErrorShape>,
    },
    #[serde(rename = "event")]
    Event {
        event: String,
        #[serde(default)]
        payload: Option<Value>,
    },
}

/// `connect.challenge` (`src/gateway/server/connection.ts:332-340`), the
/// first frame on every gateway socket; `ts` is the device proof's
/// `signedAt` (`docs/gateway/protocol/handshake.md:26-30`).
#[derive(Debug, Deserialize)]
pub(crate) struct Challenge {
    pub nonce: String,
    pub ts: u64,
}

/// `HelloOkSchema` (`frames.ts:104-177`), the fields a chat client reads.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelloOk {
    pub protocol: u32,
    pub server: HelloServer,
    pub features: HelloFeatures,
    pub auth: HelloAuth,
    pub policy: HelloPolicy,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelloServer {
    pub version: String,
    pub conn_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelloFeatures {
    pub methods: Vec<String>,
    pub events: Vec<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

/// `hello-ok.auth` (`frames.ts:130-158`): the negotiated role and the
/// socket's effective scopes.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelloAuth {
    #[serde(default)]
    pub method: Option<String>,
    pub role: String,
    pub scopes: Vec<String>,
    #[serde(default)]
    pub device_token: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelloPolicy {
    pub max_payload: u64,
    pub max_buffered_bytes: u64,
    pub tick_interval_ms: u64,
}

// ---------------------------------------------------------------- agents.list

/// `AgentsListResultSchema` (`schema/agents-models-skills.ts:93-100`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentsList {
    pub default_id: String,
    pub main_key: String,
    pub scope: String,
    pub agents: Vec<AgentSummary>,
}

/// `AgentSummarySchema` (`agents-models-skills.ts:48-82`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentSummary {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub identity: Option<AgentIdentity>,
    #[serde(default)]
    pub workspace: Option<String>,
    #[serde(default)]
    pub model: Option<AgentModel>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentIdentity {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub theme: Option<String>,
    #[serde(default)]
    pub emoji: Option<String>,
    #[serde(default)]
    pub avatar: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentModel {
    #[serde(default)]
    pub primary: Option<String>,
    #[serde(default)]
    pub fallbacks: Vec<String>,
}

// -------------------------------------------------- sessions.list / subscribe

/// `SessionsListParamsSchema` (`schema/sessions-list.ts:5-88`), the
/// selectors a chat client uses. Every field is optional on the wire; the
/// gateway applies a bounded default `limit`.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionsQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub search: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub active_only: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub exclude_subagents: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub exclude_cron: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub exclude_system: bool,
    /// Derive a title from the first user message (`sessions-list.ts:32-36`).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub include_derived_titles: bool,
    /// Add `lastMessagePreview` (`sessions-list.ts:37-41`).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub include_last_message: bool,
}

/// `sessions.list` result (`docs/gateway/protocol/rpc-bootstrap-and-events.md:57-61`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionsList {
    pub sessions: Vec<SessionRow>,
    #[serde(default)]
    pub has_more: Option<bool>,
    #[serde(default)]
    pub next_offset: Option<u32>,
    #[serde(default)]
    pub total_count: Option<u32>,
}

/// `SessionRowSchema` (`schema/sessions-row.ts:102-260`), the fields a
/// chat list reads. Unknown fields are ignored; `rest` keeps the others.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionRow {
    pub key: String,
    #[serde(default)]
    pub session_id: Option<String>,
    pub kind: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub derived_title: Option<String>,
    #[serde(default)]
    pub last_message_preview: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub is_main: Option<bool>,
    #[serde(default)]
    pub updated_at: Option<f64>,
    #[serde(default)]
    pub created_at: Option<f64>,
    #[serde(default)]
    pub last_interaction_at: Option<f64>,
    #[serde(default)]
    pub total_tokens: Option<f64>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub archived: Option<bool>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

impl SessionRow {
    /// The best display title: `label`, else `displayName`, else
    /// `derivedTitle`.
    pub fn title(&self) -> Option<&str> {
        self.label
            .as_deref()
            .or(self.display_name.as_deref())
            .or(self.derived_title.as_deref())
    }
}

/// `sessions.subscribe` result (`server-methods/sessions-subscriptions.ts:27-49`):
/// `{subscribed}` for `{}` params, `{subscribed, list}` for list params.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionsSubscribed {
    pub subscribed: bool,
    #[serde(default)]
    pub list: Option<SessionsList>,
}

// ------------------------------------------------------------- chat.history

/// `ChatHistoryParamsSchema` (`schema/logs-chat.ts:36-54`), the fields a
/// chat client sends.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryQuery {
    pub session_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// A `deltaCursor` from an earlier page: catch up from there
    /// (`logs-chat.ts:149-166`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Newest messages per page, 1..=`CHAT_HISTORY_MAX_ENTRIES`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_chars: Option<u32>,
}

/// What `chat.history` returns: a tail page, or a catch-up delta for a
/// cursor. A `{kind:"reset"}` answer (`logs-chat.ts:168-171`) never
/// reaches the caller; the client fetches the tail page instead.
#[derive(Debug, Clone)]
pub enum History {
    Page(HistoryPage),
    Delta(HistoryDelta),
}

impl History {
    pub fn messages(&self) -> &[Value] {
        match self {
            History::Page(page) => &page.messages,
            History::Delta(delta) => &delta.messages,
        }
    }

    pub fn delta_cursor(&self) -> Option<&str> {
        match self {
            History::Page(page) => page.delta_cursor.as_deref(),
            History::Delta(delta) => Some(&delta.delta_cursor),
        }
    }

    pub fn session_info(&self) -> &SessionInfo {
        match self {
            History::Page(page) => &page.session_info,
            History::Delta(delta) => &delta.session_info,
        }
    }

    pub fn in_flight_run(&self) -> Option<&InFlightRun> {
        match self {
            History::Page(page) => page.in_flight_run.as_ref(),
            History::Delta(delta) => delta.in_flight_run.as_ref(),
        }
    }
}

/// The tail page (`server-methods/chat-history-handler.ts:613-638`).
/// `messages` are display-normalized transcript rows (`role`, `content`,
/// `timestamp`, …) kept as JSON; the contract layer projects them.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryPage {
    pub session_key: String,
    #[serde(default)]
    pub session_id: Option<String>,
    pub messages: Vec<Value>,
    #[serde(default)]
    pub delta_cursor: Option<String>,
    #[serde(default)]
    pub offset: Option<u32>,
    #[serde(default)]
    pub next_offset: Option<u32>,
    #[serde(default)]
    pub has_more: Option<bool>,
    #[serde(default)]
    pub total_messages: Option<u32>,
    pub session_info: SessionInfo,
    #[serde(default)]
    pub in_flight_run: Option<InFlightRun>,
}

/// `ChatHistoryDeltaResultSchema` (`logs-chat.ts:154-166`): replay
/// `messages` in order after the page the cursor came from.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryDelta {
    pub messages: Vec<Value>,
    pub delta_cursor: String,
    pub session_info: SessionInfo,
    #[serde(default)]
    pub in_flight_run: Option<InFlightRun>,
}

/// The cursor outcome union (`logs-chat.ts:174-177`), before the reset is
/// resolved into a tail fetch.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub(crate) enum CursorResult {
    Delta(Box<HistoryDelta>),
    Reset {},
}

/// `chat.history.sessionInfo`: aggregate activity (`hasActiveRun`) and,
/// when known, the exact active set (`docs/gateway/clients.md:166-175`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub has_active_run: Option<bool>,
    #[serde(default)]
    pub active_run_ids: Option<Vec<String>>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

/// `chat.history.inFlightRun`: the run in progress and its buffered text
/// (`docs/gateway/clients.md:163-165`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InFlightRun {
    pub run_id: String,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

// ---------------------------------------------------------------- chat.send

/// `queueMode` (`logs-chat.ts:274`): how a send meets an active run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum QueueMode {
    /// Inject into the session's direct active run, or start a turn when
    /// idle (`docs/gateway/protocol/rpc-session-control.md:49`).
    Steer,
    Followup,
    Collect,
    Interrupt,
}

/// `ChatAttachmentSchema` (`logs-chat.ts:248-262`); `content` is the file
/// as the gateway accepts it (base64 text from the Control UI).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Attachment {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    pub content: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

/// `ChatSendParamsSchema` (`logs-chat.ts:296-332`): one message and a
/// caller-held `idempotencyKey` so a retry after a transport failure is
/// the same send. `queue_mode` `None` lets the client choose `Steer` when
/// it knows a run is active on `session_key`.
#[derive(Debug, Clone)]
pub struct ChatSend {
    pub session_key: String,
    pub agent_id: Option<String>,
    pub message: String,
    pub idempotency_key: String,
    pub queue_mode: Option<QueueMode>,
    pub attachments: Vec<Attachment>,
}

impl ChatSend {
    pub fn new(session_key: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            session_key: session_key.into(),
            agent_id: None,
            message: message.into(),
            idempotency_key: new_idempotency_key(),
            queue_mode: None,
            attachments: Vec::new(),
        }
    }
}

/// A fresh idempotency key: 16 random bytes, hex.
pub fn new_idempotency_key() -> String {
    let mut bytes = [0u8; 16];
    // A failed OS RNG read leaves zeros; the key only needs to be unique
    // per send, so fall back to the clock rather than fail the send.
    if getrandom::getrandom(&mut bytes).is_err() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        bytes = nanos.to_le_bytes();
    }
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The `chat.send` acknowledgment (`server-methods/chat-send-handler.ts:580-587`):
/// admission, not transcript persistence (`rpc-session-control.md:51`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatSendAck {
    pub run_id: String,
    pub status: String,
    #[serde(default)]
    pub message_seq: Option<u64>,
    #[serde(default)]
    pub interrupted_active_run: Option<bool>,
}

// ---------------------------------------------------------------- approvals

/// `ApprovalKindSchema` (`schema/approvals.ts:18-22`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ApprovalKind {
    Exec,
    Plugin,
    SystemAgent,
}

/// `ApprovalDecisionSchema` (`approvals.ts:25-29`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Decision {
    AllowOnce,
    AllowAlways,
    Deny,
}

/// `ApprovalResolveResultSchema` (`approvals.ts:328-331`): whether this
/// answer was the first, and the recorded terminal state either way.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalResolved {
    pub applied: bool,
    pub approval: ApprovalOutcome,
}

/// The terminal snapshot fields a client reads (`approvals.ts:240-271`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApprovalOutcome {
    pub id: String,
    /// `allowed`, `denied`, `expired` or `cancelled`.
    pub status: String,
    #[serde(default)]
    pub decision: Option<Decision>,
    #[serde(default)]
    pub reason: Option<String>,
}

/// One pending approval: the `exec.approval.requested` /
/// `plugin.approval.requested` event payload
/// (`server-methods/approval-shared.ts:133-147` `buildRequestedApprovalEvent`)
/// and each row of `exec.approval.list`
/// (`server-methods/approval-record-lookup.ts:121-151`).
#[derive(Debug, Clone)]
pub struct ApprovalRequested {
    pub id: String,
    pub created_at_ms: u64,
    pub expires_at_ms: u64,
    pub request: ApprovalRequest,
}

impl ApprovalRequested {
    pub fn kind(&self) -> ApprovalKind {
        match self.request {
            ApprovalRequest::Exec(_) => ApprovalKind::Exec,
            ApprovalRequest::Plugin(_) => ApprovalKind::Plugin,
        }
    }

    /// The session the request came from, when the runtime recorded one.
    pub fn session_key(&self) -> Option<&str> {
        match &self.request {
            ApprovalRequest::Exec(exec) => exec.session_key.as_deref(),
            ApprovalRequest::Plugin(plugin) => plugin.session_key.as_deref(),
        }
    }

    pub fn agent_id(&self) -> Option<&str> {
        match &self.request {
            ApprovalRequest::Exec(exec) => exec.agent_id.as_deref(),
            ApprovalRequest::Plugin(plugin) => plugin.agent_id.as_deref(),
        }
    }

    /// The decisions a reviewer may give. `deny` is always one
    /// (`approvals.ts:134-141`).
    pub fn allowed_decisions(&self) -> Vec<Decision> {
        match &self.request {
            ApprovalRequest::Exec(ExecApprovalRequest {
                allowed_decisions: Some(decisions),
                ..
            }) if !decisions.is_empty() => decisions.clone(),
            ApprovalRequest::Exec(exec) => {
                let mut decisions = vec![Decision::AllowOnce];
                if !exec
                    .unavailable_decisions
                    .iter()
                    .any(|decision| decision == "allow-always")
                {
                    decisions.push(Decision::AllowAlways);
                }
                decisions.push(Decision::Deny);
                decisions
            }
            ApprovalRequest::Plugin(plugin) => match &plugin.allowed_decisions {
                Some(decisions) if !decisions.is_empty() => decisions.clone(),
                _ => vec![Decision::AllowOnce, Decision::AllowAlways, Decision::Deny],
            },
        }
    }

    pub(crate) fn from_payload(kind: ApprovalKind, payload: Value) -> Result<Self, String> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Raw {
            id: String,
            #[serde(default)]
            created_at_ms: u64,
            #[serde(default)]
            expires_at_ms: u64,
            request: Value,
        }
        let raw: Raw = serde_json::from_value(payload).map_err(|error| error.to_string())?;
        let request = match kind {
            ApprovalKind::Exec => ApprovalRequest::Exec(
                serde_json::from_value(raw.request).map_err(|error| error.to_string())?,
            ),
            ApprovalKind::Plugin => ApprovalRequest::Plugin(
                serde_json::from_value(raw.request).map_err(|error| error.to_string())?,
            ),
            ApprovalKind::SystemAgent => {
                return Err("system-agent approvals are not chat approvals".to_owned());
            }
        };
        Ok(Self {
            id: raw.id,
            created_at_ms: raw.created_at_ms,
            expires_at_ms: raw.expires_at_ms,
            request,
        })
    }
}

#[derive(Debug, Clone)]
pub enum ApprovalRequest {
    Exec(ExecApprovalRequest),
    Plugin(PluginApprovalRequest),
}

/// `ExecApprovalRequestParamsSchema` (`schema/exec-approvals.ts:248-323`),
/// the reviewer-facing fields.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecApprovalRequest {
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub warning_text: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub session_key: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    /// `["allow-always"]` when a standing grant is not on offer.
    #[serde(default)]
    pub unavailable_decisions: Vec<String>,
    /// The decisions the gateway offers, when it lists them (it does on the
    /// requested event and the pending list).
    #[serde(default)]
    pub allowed_decisions: Option<Vec<Decision>>,
}

/// `PluginApprovalRequestParamsSchema` (`schema/plugin-approvals.ts:32-86`),
/// the reviewer-facing fields.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginApprovalRequest {
    #[serde(default)]
    pub plugin_id: Option<String>,
    pub title: String,
    pub description: String,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub severity: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_call_id: Option<String>,
    #[serde(default)]
    pub allowed_decisions: Option<Vec<Decision>>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub session_key: Option<String>,
}

// ------------------------------------------------------------------- events

/// An event frame, typed where the chat contract reads it.
#[derive(Debug, Clone)]
pub enum Event {
    /// `chat` (`logs-chat.ts:485-491`).
    Chat(ChatEvent),
    /// `agent` (`src/infra/agent-events.ts:56-72`).
    Agent(AgentEvent),
    /// `exec.approval.requested` / `plugin.approval.requested`.
    ApprovalRequested(ApprovalRequested),
    /// `exec.approval.resolved` / `plugin.approval.resolved`: the request
    /// was answered, here or anywhere else.
    ApprovalResolved {
        kind: ApprovalKind,
        id: String,
        payload: Value,
    },
    /// `session.message` (`rpc-bootstrap-and-events.md:80-81`): a transcript
    /// row as persisted. The assistant row carries the turn's `usage`,
    /// `stopReason`, `provider` and `model`.
    SessionMessage(SessionMessage),
    /// `sessions.changed` (`rpc-bootstrap-and-events.md:94-151`).
    SessionsChanged(Box<SessionsChanged>),
    /// `tick` (`frames.ts:29-31`).
    Tick,
    /// `shutdown` (`frames.ts:34-37`).
    Shutdown {
        reason: String,
        restart_expected_ms: Option<u64>,
    },
    /// Any other event, as received.
    Other { event: String, payload: Value },
}

impl Event {
    pub(crate) fn parse(event: String, payload: Value) -> Self {
        let decoded = match event.as_str() {
            "chat" => serde_json::from_value(payload.clone())
                .map(Event::Chat)
                .map_err(|error| error.to_string()),
            "agent" => AgentEvent::from_payload(payload.clone()).map(Event::Agent),
            "exec.approval.requested" => {
                ApprovalRequested::from_payload(ApprovalKind::Exec, payload.clone())
                    .map(Event::ApprovalRequested)
            }
            "plugin.approval.requested" => {
                ApprovalRequested::from_payload(ApprovalKind::Plugin, payload.clone())
                    .map(Event::ApprovalRequested)
            }
            "exec.approval.resolved" | "plugin.approval.resolved" => {
                let kind = if event.starts_with("exec") {
                    ApprovalKind::Exec
                } else {
                    ApprovalKind::Plugin
                };
                payload
                    .get("id")
                    .and_then(Value::as_str)
                    .map(|id| Event::ApprovalResolved {
                        kind,
                        id: id.to_owned(),
                        payload: payload.clone(),
                    })
                    .ok_or_else(|| "missing id".to_owned())
            }
            "session.message" => serde_json::from_value(payload.clone())
                .map(Event::SessionMessage)
                .map_err(|error| error.to_string()),
            "sessions.changed" => serde_json::from_value(payload.clone())
                .map(|changed| Event::SessionsChanged(Box::new(changed)))
                .map_err(|error| error.to_string()),
            "tick" => Ok(Event::Tick),
            "shutdown" => Ok(Event::Shutdown {
                reason: payload
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("shutdown")
                    .to_owned(),
                restart_expected_ms: payload.get("restartExpectedMs").and_then(Value::as_u64),
            }),
            _ => Err(String::new()),
        };
        // A payload this client can't read is still delivered, raw, so a
        // caller can log it; nothing is dropped on the floor.
        decoded.unwrap_or(Event::Other { event, payload })
    }
}

/// `ChatEventSchema` (`logs-chat.ts:350-357,485-491`): the shared fields
/// plus the `state`-tagged body.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChatEvent {
    pub run_id: String,
    pub session_key: String,
    #[serde(default)]
    pub agent_id: Option<String>,
    pub seq: u64,
    #[serde(flatten)]
    pub state: ChatState,
}

/// `chat` event bodies by `state`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "state", rename_all = "lowercase")]
pub enum ChatState {
    /// Startup progress before visible output (`logs-chat.ts:383-394`).
    Status { phase: String },
    /// Incremental output (`logs-chat.ts:397-404`): `delta_text` appends,
    /// or replaces the whole text when `replace` is set.
    Delta {
        #[serde(rename = "deltaText")]
        delta_text: String,
        #[serde(default)]
        replace: Option<bool>,
        #[serde(default)]
        usage: Option<Value>,
    },
    /// The run completed (`logs-chat.ts:407-414`).
    Final {
        #[serde(default)]
        message: Option<Value>,
        #[serde(default)]
        usage: Option<Value>,
        #[serde(default, rename = "stopReason")]
        stop_reason: Option<String>,
    },
    /// The run was cancelled (`logs-chat.ts:417-423`).
    Aborted {
        #[serde(default, rename = "errorMessage")]
        error_message: Option<String>,
        #[serde(default, rename = "stopReason")]
        stop_reason: Option<String>,
    },
    /// The run failed (`logs-chat.ts:473-482`).
    Error {
        #[serde(default, rename = "errorMessage")]
        error_message: Option<String>,
        #[serde(default, rename = "errorKind")]
        error_kind: Option<String>,
        #[serde(default)]
        usage: Option<Value>,
        #[serde(default, rename = "stopReason")]
        stop_reason: Option<String>,
    },
}

impl ChatState {
    /// Whether this event ends the run.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ChatState::Final { .. } | ChatState::Aborted { .. } | ChatState::Error { .. }
        )
    }
}

/// `AgentEventPayload` (`agent-events.ts:56-72`) as broadcast to operator
/// clients (`server-chat.ts:1470-1481`).
#[derive(Debug, Clone)]
pub struct AgentEvent {
    pub run_id: String,
    pub seq: u64,
    pub ts: u64,
    pub session_key: Option<String>,
    pub agent_id: Option<String>,
    pub stream: AgentStream,
}

/// The `agent` event streams a chat contract reads, by `stream` name
/// (`agent-events.ts:40-53`).
#[derive(Debug, Clone)]
pub enum AgentStream {
    /// `thinking` (`embedded-agent-subscribe.stream-rendering.ts:698-705`):
    /// `text` is the whole reasoning so far, `delta` the new part.
    Thinking { text: String, delta: String },
    /// `tool` / `phase:"start"` (`…handlers.tools.start.ts:554-565`).
    ToolStart {
        tool_call_id: String,
        name: String,
        args: Value,
        parent_tool_call_id: Option<String>,
    },
    /// `tool` / `phase:"update"` (`…handlers.tools.progress.ts`): output so
    /// far from a long-running tool such as `exec`.
    ToolUpdate {
        tool_call_id: String,
        name: String,
        partial_result: Value,
    },
    /// `tool` / `phase:"result"` (`…handlers.tools.completion.ts:456-471`).
    ToolResult {
        tool_call_id: String,
        name: String,
        is_error: bool,
        result: Value,
        meta: Option<String>,
        tool_error_summary: Option<String>,
    },
    /// `usage` (`embedded-agent-subscribe.model-state.ts:132-143`,
    /// `src/infra/agent-run-usage.ts:16-34`): cumulative output tokens for
    /// the run so far.
    Usage { output_tokens: u64 },
    /// `lifecycle`: `phase` is `start`, `model`, `end` or `error`.
    Lifecycle { phase: String, data: Value },
    /// Every other stream (`assistant`, `item`, `plan`, `approval`,
    /// `compaction`, …), as received.
    Other { stream: String, data: Value },
}

impl AgentEvent {
    pub(crate) fn from_payload(payload: Value) -> Result<Self, String> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Raw {
            run_id: String,
            seq: u64,
            #[serde(default)]
            ts: u64,
            stream: String,
            #[serde(default)]
            data: Value,
            #[serde(default)]
            session_key: Option<String>,
            #[serde(default)]
            agent_id: Option<String>,
        }
        let raw: Raw = serde_json::from_value(payload).map_err(|error| error.to_string())?;
        let stream = AgentStream::decode(raw.stream, raw.data);
        Ok(Self {
            run_id: raw.run_id,
            seq: raw.seq,
            ts: raw.ts,
            session_key: raw.session_key,
            agent_id: raw.agent_id,
            stream,
        })
    }
}

impl AgentStream {
    fn decode(stream: String, data: Value) -> Self {
        let text = |key: &str| data.get(key).and_then(Value::as_str).map(str::to_owned);
        match stream.as_str() {
            "thinking" => AgentStream::Thinking {
                text: text("text").unwrap_or_default(),
                delta: text("delta").unwrap_or_default(),
            },
            "tool" => {
                let phase = text("phase").unwrap_or_default();
                let (Some(tool_call_id), Some(name)) = (text("toolCallId"), text("name")) else {
                    return AgentStream::Other { stream, data };
                };
                match phase.as_str() {
                    "start" => AgentStream::ToolStart {
                        tool_call_id,
                        name,
                        args: data.get("args").cloned().unwrap_or(Value::Null),
                        parent_tool_call_id: text("parentToolCallId"),
                    },
                    "update" => AgentStream::ToolUpdate {
                        tool_call_id,
                        name,
                        partial_result: data.get("partialResult").cloned().unwrap_or(Value::Null),
                    },
                    "result" => AgentStream::ToolResult {
                        tool_call_id,
                        name,
                        is_error: data
                            .get("isError")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                        result: data.get("result").cloned().unwrap_or(Value::Null),
                        meta: text("meta"),
                        tool_error_summary: text("toolErrorSummary"),
                    },
                    _ => AgentStream::Other { stream, data },
                }
            }
            "usage" => match data.get("outputTokens").and_then(Value::as_u64) {
                Some(output_tokens) => AgentStream::Usage { output_tokens },
                None => AgentStream::Other { stream, data },
            },
            "lifecycle" => AgentStream::Lifecycle {
                phase: text("phase").unwrap_or_default(),
                data,
            },
            _ => AgentStream::Other { stream, data },
        }
    }
}

/// A `session.message` payload: one transcript row for `session_key`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionMessage {
    pub session_key: String,
    #[serde(default)]
    pub agent_id: Option<String>,
    /// The row: `role`, `content` (a string or content blocks),
    /// `timestamp`, and on assistant rows `usage`, `stopReason`, `provider`,
    /// `model`.
    pub message: Value,
}

impl SessionMessage {
    pub fn role(&self) -> Option<&str> {
        self.message.get("role").and_then(Value::as_str)
    }

    /// The assistant row's `usage` (`input`, `output`, `totalTokens`,
    /// `cacheRead`, `cacheWrite`, `cost`), when the gateway recorded one.
    pub fn usage(&self) -> Option<&Value> {
        self.message.get("usage")
    }
}

/// `sessions.changed`: a keyed change carries the session (and its `agentId`)
/// and a `reason`; a keyless one invalidates the whole list
/// (`rpc-bootstrap-and-events.md:94-151`). `session`, when present, is
/// the refreshed row.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionsChanged {
    #[serde(default)]
    pub session_key: Option<String>,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub session: Option<SessionRow>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}
