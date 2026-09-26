//! The slice of ACP v1 the link uses, read from JSON-RPC payloads
//! (`schema/v1/schema.json` of agentclientprotocol/agent-client-protocol).
//! Every field is read leniently: agents add `_meta` and fields from the
//! unstable schema, and a field the link does not know is ignored.

use serde_json::{Value, json};

/// The protocol version the link speaks.
pub const PROTOCOL_VERSION: u64 = 1;

/// `initialize` params: no file system and no terminal offered, so the agent
/// uses its own tools on the machine it runs on.
pub fn initialize_params(client: &str, version: &str) -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "clientCapabilities": { "fs": { "readTextFile": false, "writeTextFile": false }, "terminal": false },
        "clientInfo": { "name": client, "version": version },
    })
}

/// What `initialize` answered.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Initialized {
    pub protocol_version: u64,
    /// `agentInfo.title` (or `.name`).
    pub title: Option<String>,
    /// `agentCapabilities.loadSession`: `session/load` replays a session.
    pub load_session: bool,
    /// `sessionCapabilities.list`: `session/list` is served.
    pub list_sessions: bool,
    /// `sessionCapabilities.resume`: `session/resume` reopens a session
    /// without replaying it.
    pub resume_session: bool,
}

impl Initialized {
    pub fn parse(result: &Value) -> Self {
        let caps = &result["agentCapabilities"];
        let info = &result["agentInfo"];
        Self {
            protocol_version: result["protocolVersion"].as_u64().unwrap_or(0),
            title: text(&info["title"]).or_else(|| text(&info["name"])),
            load_session: caps["loadSession"].as_bool().unwrap_or(false),
            list_sessions: caps["sessionCapabilities"]["list"].is_object(),
            resume_session: caps["sessionCapabilities"]["resume"].is_object(),
        }
    }
}

/// The model a `session/new` / `session/load` answer (or a
/// `config_option_update`) names: the `model` config option's current value,
/// by its display name, else the unstable `models.currentModelId`.
pub fn model(result: &Value) -> Option<String> {
    let from_options = result["configOptions"].as_array().and_then(|options| {
        let option = options
            .iter()
            .find(|o| o["category"] == "model" || o["id"] == "model")?;
        let current = option["currentValue"].as_str()?;
        let name = option["options"]
            .as_array()
            .and_then(|values| values.iter().find(|v| v["value"] == current))
            .and_then(|v| text(&v["name"]));
        Some(name.unwrap_or_else(|| current.to_owned()))
    });
    from_options.or_else(|| {
        let models = &result["models"];
        let current = models["currentModelId"].as_str()?;
        let name = models["availableModels"]
            .as_array()
            .and_then(|all| all.iter().find(|m| m["modelId"] == current))
            .and_then(|m| text(&m["name"]));
        Some(name.unwrap_or_else(|| current.to_owned()))
    })
}

/// A permission mode the agent offers a session (`modes.availableModes`):
/// Claude Code's `default`, `acceptEdits`, `plan`, `auto`,
/// `bypassPermissions`; Codex's `read-only`, `agent`, `agent-full-access`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMode {
    pub id: String,
    /// `_meta.kind`, where the agent says what the mode is: `standard`,
    /// `plan`, `auto_review` or `full_access`.
    pub kind: Option<String>,
}

/// A session's modes, from a `session/new` / `session/load` /
/// `session/resume` answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Modes {
    pub current: String,
    pub available: Vec<SessionMode>,
}

pub fn modes(result: &Value) -> Option<Modes> {
    let modes = &result["modes"];
    Some(Modes {
        current: text(&modes["currentModeId"])?,
        available: modes["availableModes"]
            .as_array()?
            .iter()
            .filter_map(|m| {
                Some(SessionMode {
                    id: text(&m["id"])?,
                    kind: text(&m["_meta"]["kind"]),
                })
            })
            .collect(),
    })
}

/// One `session/list` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInfo {
    pub id: String,
    pub title: Option<String>,
    /// RFC 3339.
    pub updated_at: Option<String>,
}

pub fn sessions(result: &Value) -> Vec<SessionInfo> {
    result["sessions"]
        .as_array()
        .map(|all| {
            all.iter()
                .filter_map(|s| {
                    Some(SessionInfo {
                        id: s["sessionId"].as_str()?.to_owned(),
                        title: text(&s["title"]),
                        updated_at: text(&s["updatedAt"]),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

impl ToolStatus {
    pub fn is_done(self) -> bool {
        matches!(self, ToolStatus::Completed | ToolStatus::Failed)
    }
}

/// A `tool_call` or `tool_call_update`: every field but the id may be
/// absent from an update, which then leaves it as it was.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ToolCall {
    pub id: String,
    /// What the agent shows for the call ("Terminal", then the command).
    pub title: Option<String>,
    /// The tool's own name ("Bash"), when the agent gives one.
    pub name: Option<String>,
    /// `read`, `edit`, `execute`, `fetch`, ...
    pub kind: Option<String>,
    pub status: Option<ToolStatus>,
    pub raw_input: Option<Value>,
    pub raw_output: Option<Value>,
    /// The call's text content, joined; a diff reads as the file it edits.
    pub content: Option<String>,
}

impl ToolCall {
    pub fn parse(value: &Value) -> Option<Self> {
        let status = match value["status"].as_str() {
            Some("pending") => Some(ToolStatus::Pending),
            Some("in_progress") => Some(ToolStatus::InProgress),
            Some("completed") => Some(ToolStatus::Completed),
            Some("failed") => Some(ToolStatus::Failed),
            _ => None,
        };
        let content = value["content"].as_array().and_then(|blocks| {
            let parts: Vec<String> = blocks
                .iter()
                .filter_map(|block| match block["type"].as_str() {
                    Some("content") => text(&block["content"]["text"]),
                    Some("diff") => block["path"].as_str().map(|p| format!("Edited {p}")),
                    _ => None,
                })
                .collect();
            (!parts.is_empty()).then(|| parts.join("\n"))
        });
        let object = |v: &Value| (!v.is_null() && v != &json!({})).then(|| v.clone());
        Some(Self {
            id: value["toolCallId"].as_str()?.to_owned(),
            title: text(&value["title"]),
            name: text(&value["name"]),
            kind: text(&value["kind"]),
            status,
            raw_input: object(&value["rawInput"]),
            raw_output: object(&value["rawOutput"]),
            content,
        })
    }

    /// Takes whatever `update` sets.
    pub fn merge(&mut self, update: ToolCall) {
        let ToolCall {
            id: _,
            title,
            name,
            kind,
            status,
            raw_input,
            raw_output,
            content,
        } = update;
        self.title = title.or(self.title.take());
        self.name = name.or(self.name.take());
        self.kind = kind.or(self.kind.take());
        self.status = status.or(self.status);
        self.raw_input = raw_input.or(self.raw_input.take());
        self.raw_output = raw_output.or(self.raw_output.take());
        self.content = content.or(self.content.take());
    }

    /// What the owner reads for the call: its title, else its name.
    pub fn label(&self) -> String {
        self.title
            .clone()
            .or_else(|| self.name.clone())
            .unwrap_or_else(|| "Tool".to_owned())
    }

    /// The call's output as text: its content, else its raw output (the
    /// Codex adapter sends a command's as `{"formatted_output", "exit_code"}`).
    pub fn output(&self) -> String {
        match (&self.content, &self.raw_output) {
            (Some(content), _) => content.clone(),
            (None, Some(Value::String(s))) => s.clone(),
            (None, Some(other)) => ["formatted_output", "output", "stdout"]
                .iter()
                .find_map(|key| other[*key].as_str())
                .map_or_else(|| other.to_string(), str::to_owned),
            (None, None) => String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanEntry {
    pub content: String,
    /// `pending`, `in_progress` or `completed`.
    pub status: String,
}

/// One `session/update`.
#[derive(Debug, Clone, PartialEq)]
pub enum Update {
    /// A replayed message of the owner's (`session/load`).
    UserText {
        text: String,
        message_id: Option<String>,
    },
    AgentText {
        text: String,
        message_id: Option<String>,
    },
    Thought(String),
    ToolCall(ToolCall),
    ToolCallUpdate(ToolCall),
    Plan(Vec<PlanEntry>),
    Title(String),
    Model(String),
    /// The session's permission mode changed (`current_mode_update`).
    Mode(String),
    /// Anything the link does not use (commands, context usage).
    Other,
}

/// A `session/update` notification's session and update.
pub fn update(params: &Value) -> Option<(String, Update)> {
    let session = params["sessionId"].as_str()?.to_owned();
    let update = &params["update"];
    let chunk = |update: &Value| {
        // Text only: images and other blocks have no text to show.
        let content = &update["content"];
        (content["type"] == "text").then(|| content["text"].as_str().unwrap_or("").to_owned())
    };
    let message_id = text(&update["messageId"]);
    let parsed = match update["sessionUpdate"].as_str()? {
        "user_message_chunk" => chunk(update).map(|text| Update::UserText { text, message_id }),
        "agent_message_chunk" => chunk(update).map(|text| Update::AgentText { text, message_id }),
        "agent_thought_chunk" => chunk(update).map(Update::Thought),
        "tool_call" => ToolCall::parse(update).map(Update::ToolCall),
        "tool_call_update" => ToolCall::parse(update).map(Update::ToolCallUpdate),
        "plan" => Some(Update::Plan(
            update["entries"]
                .as_array()
                .map(|entries| {
                    entries
                        .iter()
                        .filter_map(|e| {
                            Some(PlanEntry {
                                content: e["content"].as_str()?.to_owned(),
                                status: e["status"].as_str().unwrap_or("pending").to_owned(),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default(),
        )),
        "session_info_update" => text(&update["title"]).map(Update::Title),
        "config_option_update" => model(update).map(Update::Model),
        "current_mode_update" => text(&update["currentModeId"]).map(Update::Mode),
        _ => None,
    };
    Some((session, parsed.unwrap_or(Update::Other)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionOption {
    pub id: String,
    pub name: String,
    /// `allow_once`, `allow_always`, `reject_once` or `reject_always`.
    pub kind: String,
}

/// A `session/request_permission`.
#[derive(Debug, Clone, PartialEq)]
pub struct PermissionRequest {
    pub session_id: String,
    pub tool_call: ToolCall,
    pub options: Vec<PermissionOption>,
}

impl PermissionRequest {
    pub fn parse(params: &Value) -> Option<Self> {
        Some(Self {
            session_id: params["sessionId"].as_str()?.to_owned(),
            tool_call: ToolCall::parse(&params["toolCall"])?,
            options: params["options"]
                .as_array()?
                .iter()
                .filter_map(|o| {
                    Some(PermissionOption {
                        id: o["optionId"].as_str()?.to_owned(),
                        name: o["name"].as_str().unwrap_or("").to_owned(),
                        kind: o["kind"].as_str().unwrap_or("").to_owned(),
                    })
                })
                .collect(),
        })
    }
}

/// The answer to a permission request: the option chosen.
pub fn selected(option_id: &str) -> Value {
    json!({ "outcome": { "outcome": "selected", "optionId": option_id } })
}

/// The answer to a permission request the turn was cancelled under.
pub fn cancelled() -> Value {
    json!({ "outcome": { "outcome": "cancelled" } })
}

/// A turn's tokens, from `session/prompt`'s `usage` (unstable in the schema;
/// sent by the Claude Code and Codex adapters).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TokenUsage {
    /// Every input token the turn read: fresh, cache reads and cache writes.
    pub input: u64,
    pub output: u64,
}

/// What `session/prompt` answered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptResult {
    /// `end_turn`, `max_tokens`, `max_turn_requests`, `refusal`, `cancelled`.
    pub stop_reason: String,
    pub usage: Option<TokenUsage>,
}

impl PromptResult {
    pub fn parse(result: &Value) -> Self {
        let usage = &result["usage"];
        let count = |key: &str| usage[key].as_u64().unwrap_or(0);
        Self {
            stop_reason: result["stopReason"]
                .as_str()
                .unwrap_or("end_turn")
                .to_owned(),
            usage: usage.is_object().then(|| TokenUsage {
                input: count("inputTokens")
                    + count("cachedReadTokens")
                    + count("cachedWriteTokens"),
                output: count("outputTokens"),
            }),
        }
    }
}

fn text(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Frames recorded from `@agentclientprotocol/claude-agent-acp` 0.81.2
    /// and `@agentclientprotocol/codex-acp` 1.13.1 (2026-09-26).
    #[test]
    fn recorded_frames_read_as_the_link_needs() {
        let init = Initialized::parse(&json!({
            "protocolVersion": 1,
            "agentCapabilities": { "loadSession": true, "sessionCapabilities": { "list": {}, "resume": {} } },
            "agentInfo": { "name": "@agentclientprotocol/claude-agent-acp", "title": "Claude Agent", "version": "0.81.2" },
            "authMethods": []
        }));
        assert_eq!(init.protocol_version, 1);
        assert!(init.load_session && init.list_sessions && init.resume_session);
        assert_eq!(init.title.as_deref(), Some("Claude Agent"));

        let claude_new = json!({ "sessionId": "s", "configOptions": [
            { "id": "mode", "category": "mode", "currentValue": "default", "options": [] },
            { "id": "model", "category": "model", "currentValue": "default",
              "options": [{ "value": "default", "name": "Default (recommended)" }, { "value": "sonnet", "name": "Sonnet 5" }] }
        ]});
        assert_eq!(model(&claude_new).as_deref(), Some("Default (recommended)"));
        let codex_new = json!({ "sessionId": "s", "models": { "currentModelId": "gpt-x[low]",
            "availableModels": [{ "modelId": "gpt-x[low]", "name": "X (low)" }] } });
        assert_eq!(model(&codex_new).as_deref(), Some("X (low)"));
        assert_eq!(modes(&codex_new), None, "no modes offered");

        let claude_modes = json!({ "sessionId": "s", "modes": { "currentModeId": "default", "availableModes": [
            { "id": "default", "name": "Manual", "_meta": { "kind": "standard" } },
            { "id": "acceptEdits", "name": "Accept edits", "_meta": { "kind": "standard" } },
            { "id": "bypassPermissions", "name": "Bypass permissions", "_meta": { "kind": "full_access" } },
            { "id": "odd" } ] } });
        let parsed = modes(&claude_modes).unwrap();
        assert_eq!(parsed.current, "default");
        assert_eq!(parsed.available.len(), 4);
        assert_eq!(parsed.available[2].kind.as_deref(), Some("full_access"));
        assert_eq!(parsed.available[3].kind, None);
        assert_eq!(
            update_of(json!({ "sessionUpdate": "current_mode_update", "currentModeId": "plan" })),
            Update::Mode("plan".into())
        );

        let (session, update) = update(&json!({ "sessionId": "s", "update": {
            "sessionUpdate": "tool_call", "toolCallId": "toolu_1", "name": "Bash", "rawInput": {},
            "status": "pending", "title": "Terminal", "kind": "execute", "content": [] } }))
        .unwrap();
        assert_eq!(session, "s");
        let Update::ToolCall(mut call) = update else {
            panic!("{update:?}")
        };
        assert_eq!(call.raw_input, None, "an empty input is no input yet");
        let (_, later) = super::update(&json!({ "sessionId": "s", "update": {
            "sessionUpdate": "tool_call_update", "toolCallId": "toolu_1", "status": "completed", "rawOutput": "hi",
            "rawInput": { "command": "echo hi" }, "title": "echo hi",
            "content": [{ "type": "content", "content": { "type": "text", "text": "```console\nhi\n```" } }] } })).unwrap();
        let Update::ToolCallUpdate(later) = later else {
            panic!()
        };
        call.merge(later);
        assert_eq!(call.label(), "echo hi");
        assert_eq!(call.status, Some(ToolStatus::Completed));
        assert_eq!(call.output(), "```console\nhi\n```");
        assert_eq!(call.name.as_deref(), Some("Bash"));

        let codex = ToolCall::parse(&json!({ "toolCallId": "exec-1", "status": "completed",
            "rawOutput": { "formatted_output": "acp-live-ok\n", "exit_code": 0 } }))
        .unwrap();
        assert_eq!(codex.output(), "acp-live-ok\n");

        let permission = PermissionRequest::parse(&json!({ "sessionId": "s",
            "toolCall": { "toolCallId": "toolu_1", "title": "echo hi" },
            "options": [{ "optionId": "allow-once", "name": "Yes", "kind": "allow_once" },
                        { "optionId": "reject", "name": "No", "kind": "reject_once" }] }))
        .unwrap();
        assert_eq!(permission.options[1].kind, "reject_once");

        let prompt = PromptResult::parse(&json!({ "stopReason": "end_turn",
            "usage": { "inputTokens": 2, "outputTokens": 4, "cachedReadTokens": 10234, "cachedWriteTokens": 18042, "totalTokens": 28282 } }));
        assert_eq!(
            prompt.usage,
            Some(TokenUsage {
                input: 28278,
                output: 4
            })
        );
        assert_eq!(
            PromptResult::parse(&json!({ "stopReason": "cancelled" })).usage,
            None
        );

        assert_eq!(
            update_of(json!({ "sessionUpdate": "session_info_update", "title": "Reply with ok" })),
            Update::Title("Reply with ok".into())
        );
        assert_eq!(
            update_of(json!({ "sessionUpdate": "usage_update", "used": 1, "size": 2 })),
            Update::Other
        );
        assert_eq!(
            update_of(
                json!({ "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": "ok" }, "messageId": "m" })
            ),
            Update::AgentText {
                text: "ok".into(),
                message_id: Some("m".into())
            }
        );
        let listed = sessions(
            &json!({ "sessions": [{ "sessionId": "a", "cwd": "/w", "title": "T", "updatedAt": "2026-09-26T14:23:13.025Z" }, { "cwd": "/w" }] }),
        );
        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed[0].updated_at.as_deref(),
            Some("2026-09-26T14:23:13.025Z")
        );
    }

    fn update_of(update: Value) -> Update {
        super::update(&json!({ "sessionId": "s", "update": update }))
            .unwrap()
            .1
    }
}
