//! Open Agent Link 0.1's host-layer types (`spec/oal-0.1.md` §7–§10,
//! `spec/schemas/defs.schema.json`), as the host core produces them. Each
//! serializes to the OAL JSON it is named for, so an OAL host sends it as it
//! is: [`Agent`] is `host/agents`' entry, [`AgentUpdate`] is
//! `host/agent_update`, [`PendingRequest`] and [`PendingUpdate`] are
//! `host/pending` and `host/pending_update`, [`TurnUpdate`] is `host/turn`.

use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// OAL's error codes (§15), and the JSON-RPC and ACP codes a host answers
/// with.
pub mod code {
    /// No common protocol version (then close 4002).
    pub const VERSION_MISMATCH: i64 = -33001;
    /// The first request wasn't `host/hello` or `host/pair`, or its
    /// credentials failed (then close 4001).
    pub const UNAUTHENTICATED: i64 = -33002;
    /// A wrong, expired or used pairing code.
    pub const PAIRING_REFUSED: i64 = -33003;
    /// The agent named isn't on this host.
    pub const UNKNOWN_AGENT: i64 = -33004;
    /// The agent isn't running and can't be started.
    pub const AGENT_UNAVAILABLE: i64 = -33005;
    /// A prompt while a turn is running in the session.
    pub const TURN_IN_PROGRESS: i64 = -33006;
    /// An answer to a request already resolved.
    pub const ALREADY_ANSWERED: i64 = -33007;
    /// An answer to a request the host doesn't know.
    pub const UNKNOWN_REQUEST: i64 = -33008;
    /// A file couldn't be fetched or is too large.
    pub const ATTACHMENT_FAILED: i64 = -33009;
    /// Host policy refuses the request.
    pub const NOT_PERMITTED: i64 = -33010;
    /// The runtime failed the turn.
    pub const TURN_FAILED: i64 = -33011;
    /// ACP: authentication required.
    pub const AUTH_REQUIRED: i64 = -32000;
    /// ACP: resource not found (a session the agent doesn't know).
    pub const NOT_FOUND: i64 = -32002;
    /// JSON-RPC: invalid request.
    pub const INVALID_REQUEST: i64 = -32600;
    /// JSON-RPC: method not found.
    pub const METHOD_NOT_FOUND: i64 = -32601;
    /// JSON-RPC invalid params: an option that isn't one of the request's.
    pub const INVALID_PARAMS: i64 = -32602;
    /// JSON-RPC: internal error.
    pub const INTERNAL: i64 = -32603;
    /// ACP: the request was cancelled (`$/cancel_request`).
    pub const REQUEST_CANCELLED: i64 = -32800;
}

/// One agent the host runs (`host/agents`, §7.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Agent {
    /// Stable, unique on the host: lowercase letters, digits and hyphens.
    pub id: String,
    /// What the owner calls it ("Claude Code · api").
    pub label: String,
    /// The runtime's id: `claude-code`, `codex`, `openclaw`, `hermes`, ...
    pub runtime: String,
    /// Where its sessions work, for runtimes that have a folder.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folder: Option<String>,
    /// It can take a prompt now, or will be started on first use.
    pub online: bool,
    /// Why not, in one plain sentence, when `online` is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offline_reason: Option<String>,
    /// ACP `AgentCapabilities`, as its last `initialize` answered (or as the
    /// adapter provides them).
    pub capabilities: Value,
    /// ACP `SessionModeState` a new session starts in, when it has modes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modes: Option<SessionModeState>,
}

/// ACP `SessionModeState`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionModeState {
    pub current_mode_id: String,
    pub available_modes: Vec<SessionMode>,
}

/// ACP `SessionMode`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMode {
    pub id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentChange {
    Added,
    Updated,
    Removed,
}

/// `host/agent_update` (§7.3): for `Removed`, the agent's last known state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentUpdate {
    pub change: AgentChange,
    pub agent: Agent,
}

/// ACP `ToolCallUpdate`: the call a permission request is about.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallUpdate {
    pub tool_call_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// ACP `ToolKind`: `read`, `edit`, `execute`, `fetch`, ...
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<ToolCallStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_input: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_output: Option<Value>,
    /// ACP `ToolCallContent` blocks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<Vec<Value>>,
}

impl ToolCallUpdate {
    /// Text content, as ACP carries it: `[{type: "content", content: {type: "text", text}}]`.
    pub fn text(text: &str) -> Vec<Value> {
        vec![serde_json::json!({ "type": "content", "content": { "type": "text", "text": text } })]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

/// ACP `PermissionOption`: one answer a permission request offers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionOption {
    /// What the agent is answered with.
    pub option_id: String,
    pub name: String,
    /// `allow_once`, `allow_always`, `reject_once` or `reject_always`.
    pub kind: String,
}

/// A permission request waiting for the owner (§10).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingRequest {
    /// Host-assigned: the runtime's own id for the request when it gives
    /// one and no other pending request has it.
    pub id: String,
    pub agent: String,
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub tool_call: ToolCallUpdate,
    pub options: Vec<PermissionOption>,
    /// RFC 3339, UTC.
    pub created_at: String,
    /// The request in the owner's words, for clients that show a sentence
    /// rather than the tool call, where the runtime gave its own (empty
    /// otherwise). Not part of OAL's `PendingRequest`.
    #[serde(skip)]
    pub words: Words,
    /// The agent's `session/request_permission` params, as it sent them:
    /// what every attached client is sent, unchanged. Not part of OAL's
    /// `PendingRequest`.
    #[serde(skip)]
    pub params: Value,
}

/// A permission request as the owner reads it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Words {
    /// The question on the card ("Hermes asks to run:\nrm -rf ./scratch").
    pub question: String,
    /// What the agent wants to do, completing "<agent> asks to …"
    /// ("run `ls`").
    pub summary: String,
    /// Each option's label, in `options` order ("Allow once").
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PendingChange {
    Added,
    Resolved,
}

/// ACP `RequestPermissionOutcome`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Outcome {
    Selected {
        #[serde(rename = "optionId")]
        option_id: String,
    },
    Cancelled,
}

/// The device that did something (`DeviceRef`); `None` where it is the
/// computer itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeviceRef {
    pub device_id: String,
    pub name: String,
}

/// `host/pending_update` (§10).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingUpdate {
    pub change: PendingChange,
    pub request: PendingRequest,
    /// On `Resolved`: how it was answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<Outcome>,
    /// On `Resolved`: who answered; null when the computer itself did.
    #[serde(default)]
    pub answered_by: Option<DeviceRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TurnState {
    Running,
    Ended,
}

/// ACP `StopReason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    EndTurn,
    MaxTokens,
    MaxTurnRequests,
    Refusal,
    Cancelled,
}

impl StopReason {
    /// ACP's name for it; anything unknown ends the turn normally.
    pub fn parse(name: &str) -> Self {
        match name {
            "max_tokens" => StopReason::MaxTokens,
            "max_turn_requests" => StopReason::MaxTurnRequests,
            "refusal" => StopReason::Refusal,
            "cancelled" => StopReason::Cancelled,
            _ => StopReason::EndTurn,
        }
    }
}

/// JSON-RPC error object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorObject {
    pub code: i64,
    /// One plain sentence for the owner.
    pub message: String,
    /// Details for software: the agent's own text of an error whose message
    /// the host made plain (`detail`), the version ranges of a mismatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl ErrorObject {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// With `data`.
    pub fn with_data(self, data: Value) -> Self {
        Self {
            data: Some(data),
            ..self
        }
    }
}

/// `host/turn` (§9): a turn started, is running, or ended.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnUpdate {
    pub agent: String,
    pub session_id: String,
    pub turn_id: String,
    pub state: TurnState,
    /// RFC 3339, UTC: when the prompt was sent.
    pub started_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<DeviceRef>,
    /// On `Ended`, when the turn ended with an answer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_reason: Option<StopReason>,
    /// On `Ended`, when the turn ended with an error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorObject>,
    /// On `Ended`, this turn's tokens, when the runtime reports them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

/// One turn's tokens and cost, in ACP `Usage`'s field names.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thought_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_read_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_write_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<Cost>,
}

impl Usage {
    /// Every input token the turn read: fresh, cache reads and cache writes.
    pub fn all_input(&self) -> u64 {
        self.input_tokens
            + self.cached_read_tokens.unwrap_or(0)
            + self.cached_write_tokens.unwrap_or(0)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cost {
    pub amount: f64,
    /// ISO 4217.
    pub currency: String,
}

/// Now as RFC 3339, UTC, to the millisecond.
pub fn now() -> String {
    let since = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    rfc3339(since.as_secs() as i64, since.subsec_millis())
}

/// Unix seconds (and milliseconds) as RFC 3339, UTC.
pub fn rfc3339(secs: i64, millis: u32) -> String {
    // Howard Hinnant's civil_from_days.
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// Unix seconds of an RFC 3339 time (`2026-09-26T14:23:13.025Z`,
/// `…+02:00`).
pub fn unix_seconds(text: &str) -> Option<f64> {
    let b = text.as_bytes();
    let num = |from: usize, len: usize| -> Option<i64> { text.get(from..from + len)?.parse().ok() };
    if b.len() < 19 || b[4] != b'-' || b[7] != b'-' || !matches!(b[10], b'T' | b't' | b' ') {
        return None;
    }
    let (year, month, day) = (num(0, 4)?, num(5, 2)?, num(8, 2)?);
    let (hour, minute, second) = (num(11, 2)?, num(14, 2)?, num(17, 2)?);
    let mut rest = &text[19..];
    let mut fraction = 0.0;
    if let Some(after) = rest.strip_prefix('.') {
        let digits = after.bytes().take_while(u8::is_ascii_digit).count();
        fraction = format!("0.{}", &after[..digits]).parse().unwrap_or(0.0);
        rest = &after[digits..];
    }
    let offset = match rest {
        "Z" | "z" | "" => 0,
        _ => {
            let sign = match rest.as_bytes().first()? {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            sign * (rest.get(1..3)?.parse::<i64>().ok()? * 3600
                + rest.get(4..6)?.parse::<i64>().ok()? * 60)
        }
    };
    // Days from 1970-01-01 (Howard Hinnant's days_from_civil).
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    Some((days * 86_400 + hour * 3600 + minute * 60 + second - offset) as f64 + fraction)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rfc3339_times_read_back() {
        assert_eq!(unix_seconds("1970-01-01T00:00:00Z"), Some(0.0));
        assert_eq!(unix_seconds("2026-09-26T14:23:13.025Z"), Some(1_790_432_593.025));
        assert_eq!(unix_seconds("2026-09-26T16:23:13+02:00"), Some(1_790_432_593.0));
        assert_eq!(unix_seconds("yesterday"), None);
    }

    #[test]
    fn times_are_rfc3339_utc() {
        assert_eq!(rfc3339(0, 0), "1970-01-01T00:00:00.000Z");
        assert_eq!(rfc3339(1_790_432_593, 25), "2026-09-26T14:23:13.025Z");
        assert_eq!(rfc3339(951_782_400, 0), "2000-02-29T00:00:00.000Z");
    }

    /// The types serialize to the JSON the OAL schemas name.
    #[test]
    fn the_types_are_oal_on_the_wire() {
        let request = PendingRequest {
            id: "call_1".into(),
            agent: "app".into(),
            session_id: "s1".into(),
            turn_id: Some("t1".into()),
            tool_call: ToolCallUpdate {
                tool_call_id: "call_1".into(),
                title: Some("ls".into()),
                kind: Some("execute".into()),
                ..ToolCallUpdate::default()
            },
            options: vec![PermissionOption {
                option_id: "allow".into(),
                name: "Allow".into(),
                kind: "allow_once".into(),
            }],
            created_at: "2026-09-26T14:23:13.025Z".into(),
            words: Words {
                question: "ls".into(),
                summary: "run `ls`".into(),
                labels: vec!["Allow once".into()],
            },
            params: json!({ "sessionId": "s1" }),
        };
        let update = PendingUpdate {
            change: PendingChange::Resolved,
            request,
            outcome: Some(Outcome::Selected {
                option_id: "allow".into(),
            }),
            answered_by: None,
        };
        assert_eq!(
            serde_json::to_value(&update).unwrap(),
            json!({
                "change": "resolved",
                "request": {
                    "id": "call_1",
                    "agent": "app",
                    "sessionId": "s1",
                    "turnId": "t1",
                    "toolCall": { "toolCallId": "call_1", "title": "ls", "kind": "execute" },
                    "options": [{ "optionId": "allow", "name": "Allow", "kind": "allow_once" }],
                    "createdAt": "2026-09-26T14:23:13.025Z",
                },
                "outcome": { "outcome": "selected", "optionId": "allow" },
                "answeredBy": null,
            })
        );
        let turn = TurnUpdate {
            agent: "app".into(),
            session_id: "s1".into(),
            turn_id: "t1".into(),
            state: TurnState::Ended,
            started_at: "2026-09-26T14:23:13.025Z".into(),
            by: None,
            stop_reason: Some(StopReason::EndTurn),
            error: None,
            usage: Some(Usage {
                input_tokens: 10,
                output_tokens: 2,
                cached_read_tokens: Some(5),
                ..Usage::default()
            }),
        };
        assert_eq!(
            serde_json::to_value(&turn).unwrap(),
            json!({
                "agent": "app",
                "sessionId": "s1",
                "turnId": "t1",
                "state": "ended",
                "startedAt": "2026-09-26T14:23:13.025Z",
                "stopReason": "end_turn",
                "usage": { "inputTokens": 10, "outputTokens": 2, "cachedReadTokens": 5 },
            })
        );
        let agent = AgentUpdate {
            change: AgentChange::Added,
            agent: Agent {
                id: "app".into(),
                label: "app".into(),
                runtime: "claude-code".into(),
                folder: Some("/Users/me/code/app".into()),
                online: false,
                offline_reason: Some("Claude Code isn't signed in on this computer.".into()),
                capabilities: json!({ "loadSession": true }),
                modes: Some(SessionModeState {
                    current_mode_id: "default".into(),
                    available_modes: vec![SessionMode {
                        id: "default".into(),
                        name: "Default".into(),
                        description: None,
                    }],
                }),
            },
        };
        assert_eq!(
            serde_json::to_value(&agent).unwrap(),
            json!({
                "change": "added",
                "agent": {
                    "id": "app",
                    "label": "app",
                    "runtime": "claude-code",
                    "folder": "/Users/me/code/app",
                    "online": false,
                    "offlineReason": "Claude Code isn't signed in on this computer.",
                    "capabilities": { "loadSession": true },
                    "modes": { "currentModeId": "default", "availableModes": [{ "id": "default", "name": "Default" }] },
                },
            })
        );
    }
}
