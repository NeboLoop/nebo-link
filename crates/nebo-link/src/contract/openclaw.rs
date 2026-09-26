//! The OpenClaw backend: an agent is an agent, a chat is a session key
//! under it (`agent:<agentId>:<key>`), and a turn is one `chat.send`
//! followed on the gateway's `chat` and `agent` events
//! ([`nebo_runtimes::openclaw::gateway`]).
//!
//! One operator socket serves every turn: the gateway multiplexes every
//! session's events on it, so a router hands each event to the turn on its
//! session (by run id once the `chat.send` ack names it). The socket is
//! opened on first use and again after it closes; a turn under a closed
//! socket fails with the plain copy.
//!
//! Approvals (`exec.approval.requested`, `plugin.approval.requested`) carry
//! the session key and an id; the card offers the decisions the gateway
//! allows, and the answer is `approval.resolve`. A `*.approval.resolved`
//! from anywhere (the Control UI, another operator) ends the ask here too.
//!
//! Usage: the final `chat` event carries none (live spike, PRD Appendix
//! C.4); the turn's tokens ride the assistant `session.message` row, kept
//! per session until the run ends.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use nebo_runtimes::openclaw::gateway::{
    AgentStream, ApprovalKind, ApprovalRequest, ApprovalRequested, ChatEvent, ChatSend, ChatState,
    Connect, Decision, Event, Events, FileDeviceStore, Gateway, HistoryQuery, SessionsQuery,
    new_idempotency_key,
};
use serde_json::Value;
use tokio::sync::mpsc;

use super::backend::{
    Agent, Ask, Backend, BoxFuture, Chat, Choice, Control, Error, Message, Role, ToolCall,
    ToolResult, Turn, TurnEvent, Usage,
};

/// The scopes a turn needs: send, and answer approvals
/// (`connect-admission.ts:138-163`).
const REQUIRED_SCOPES: [&str; 2] = ["operator.write", "operator.approvals"];

/// A linked OpenClaw install's gateway.
pub struct Openclaw {
    connect: Connect,
    device: FileDeviceStore,
    live: tokio::sync::Mutex<Option<Gateway>>,
    router: Arc<Router>,
}

/// Where the socket's events go.
#[derive(Default)]
struct Router {
    /// The turn on each session key.
    turns: Mutex<HashMap<String, Sink>>,
    /// Pending approvals: id to (session key, kind).
    approvals: Mutex<HashMap<String, (String, ApprovalKind)>>,
    /// The last assistant row's usage per session key.
    usage: Mutex<HashMap<String, Usage>>,
}

struct Sink {
    /// Known once the `chat.send` ack lands; events before it are matched
    /// by session alone.
    run_id: Option<String>,
    events: mpsc::Sender<TurnEvent>,
}

impl Router {
    /// The turn an event on `session_key` from `run_id` belongs to.
    fn sink(&self, session_key: &str, run_id: Option<&str>) -> Option<mpsc::Sender<TurnEvent>> {
        let turns = self.turns.lock().expect("turns lock");
        let sink = turns.get(session_key)?;
        match (&sink.run_id, run_id) {
            (Some(ours), Some(theirs)) if ours != theirs => None,
            _ => Some(sink.events.clone()),
        }
    }

    fn end(&self, session_key: &str) {
        self.turns.lock().expect("turns lock").remove(session_key);
        self.usage.lock().expect("usage lock").remove(session_key);
    }

    async fn chat(&self, chat: ChatEvent) {
        let Some(sink) = self.sink(&chat.session_key, Some(&chat.run_id)) else {
            return;
        };
        let (event, terminal) = match chat.state {
            ChatState::Status { .. } => return,
            ChatState::Delta { delta_text, .. } => (TurnEvent::Text(delta_text), false),
            ChatState::Final { usage, .. } => {
                let usage = usage.as_ref().and_then(usage_of).or_else(|| {
                    self.usage
                        .lock()
                        .expect("usage lock")
                        .get(&chat.session_key)
                        .copied()
                });
                (TurnEvent::Completed { usage }, true)
            }
            ChatState::Aborted { .. } => (TurnEvent::Cancelled, true),
            ChatState::Error { error_message, .. } => (
                TurnEvent::Failed(
                    error_message
                        .unwrap_or_else(|| "OpenClaw could not finish the turn.".to_owned()),
                ),
                true,
            ),
        };
        if terminal {
            self.end(&chat.session_key);
        }
        let _ = sink.send(event).await;
    }

    async fn agent(&self, event: nebo_runtimes::openclaw::gateway::AgentEvent) {
        let Some(session_key) = event.session_key.as_deref() else {
            return;
        };
        let Some(sink) = self.sink(session_key, Some(&event.run_id)) else {
            return;
        };
        let turn_event = match event.stream {
            AgentStream::Thinking { text, .. } => TurnEvent::Thinking(text),
            AgentStream::ToolStart {
                tool_call_id,
                name,
                args,
                ..
            } => TurnEvent::ToolStart {
                id: tool_call_id,
                name,
                input: args,
            },
            AgentStream::ToolResult {
                tool_call_id,
                name,
                is_error,
                result,
                ..
            } => TurnEvent::ToolResult {
                id: tool_call_id,
                name,
                result: text_of(&result.get("content").cloned().unwrap_or(result.clone())),
                is_error,
                duration_ms: result
                    .get("details")
                    .and_then(|d| d.get("durationMs"))
                    .and_then(Value::as_u64),
            },
            AgentStream::ToolUpdate { .. }
            | AgentStream::Usage { .. }
            | AgentStream::Lifecycle { .. }
            | AgentStream::Other { .. } => return,
        };
        let _ = sink.send(turn_event).await;
    }

    async fn ask(&self, requested: ApprovalRequested) {
        let Some(session_key) = requested.session_key() else {
            return;
        };
        let Some(sink) = self.sink(session_key, None) else {
            return;
        };
        let (prompt, summary) = match &requested.request {
            ApprovalRequest::Exec(exec) => {
                let command = exec.command.clone().unwrap_or_default();
                let prompt = match exec.warning_text.as_deref().map(str::trim) {
                    Some(warning) if !warning.is_empty() => {
                        format!("OpenClaw asks to run:\n{command}\n\n{warning}")
                    }
                    _ => format!("OpenClaw asks to run:\n{command}"),
                };
                (prompt, format!("run {command}"))
            }
            ApprovalRequest::Plugin(plugin) => (
                format!("OpenClaw asks: {}\n\n{}", plugin.title, plugin.description),
                plugin.title.clone(),
            ),
        };
        self.approvals.lock().expect("approvals lock").insert(
            requested.id.clone(),
            (session_key.to_owned(), requested.kind()),
        );
        let choices = requested
            .allowed_decisions()
            .into_iter()
            .map(|decision| Choice {
                value: decision_value(decision).to_owned(),
                label: decision_label(decision).to_owned(),
            })
            .collect();
        let _ = sink
            .send(TurnEvent::Ask(Ask {
                request_id: Some(requested.id),
                prompt,
                summary,
                choices,
            }))
            .await;
    }

    async fn resolved(&self, id: &str) {
        let taken = self.approvals.lock().expect("approvals lock").remove(id);
        let Some((session_key, _)) = taken else {
            return;
        };
        if let Some(sink) = self.sink(&session_key, None) {
            let _ = sink
                .send(TurnEvent::AskAnswered {
                    request_id: Some(id.to_owned()),
                })
                .await;
        }
    }

    fn record_usage(&self, row: &nebo_runtimes::openclaw::gateway::SessionMessage) {
        if row.role() != Some("assistant") {
            return;
        }
        if let Some(usage) = row.usage().and_then(usage_of) {
            self.usage
                .lock()
                .expect("usage lock")
                .insert(row.session_key.clone(), usage);
        }
    }

    /// The socket closed: every turn on it is over.
    async fn disconnected(&self) {
        let sinks: Vec<mpsc::Sender<TurnEvent>> = self
            .turns
            .lock()
            .expect("turns lock")
            .drain()
            .map(|(_, sink)| sink.events)
            .collect();
        self.approvals.lock().expect("approvals lock").clear();
        for sink in sinks {
            let _ = sink
                .send(TurnEvent::Failed(
                    "Could not connect to OpenClaw. Try again.".to_owned(),
                ))
                .await;
        }
    }
}

/// Hands the socket's events to the turns until it closes.
async fn route(mut events: Events, router: Arc<Router>) {
    while let Some(event) = events.next().await {
        match event {
            Event::Chat(chat) => router.chat(chat).await,
            Event::Agent(agent) => router.agent(agent).await,
            Event::ApprovalRequested(requested) => router.ask(requested).await,
            Event::ApprovalResolved { id, .. } => router.resolved(&id).await,
            Event::SessionMessage(row) => router.record_usage(&row),
            Event::SessionsChanged(_)
            | Event::Tick
            | Event::Shutdown { .. }
            | Event::Other { .. } => {}
        }
    }
    router.disconnected().await;
}

impl Openclaw {
    /// `connect` reaches the gateway as the link's proxy does; `device` keeps
    /// the keypair the socket proves itself with.
    pub fn new(connect: Connect, device: FileDeviceStore) -> Self {
        Self {
            connect,
            device,
            live: tokio::sync::Mutex::new(None),
            router: Arc::new(Router::default()),
        }
    }

    /// The open socket, opened now when there is none.
    async fn gateway(&self) -> Result<Gateway, Error> {
        let mut live = self.live.lock().await;
        if let Some(gateway) = live.as_ref()
            && gateway.closed().is_none()
        {
            return Ok(gateway.clone());
        }
        let (gateway, events) = Gateway::connect(&self.connect, &self.device)
            .await
            .map_err(map)?;
        tokio::spawn(route(events, self.router.clone()));
        *live = Some(gateway.clone());
        Ok(gateway)
    }
}

impl Backend for Openclaw {
    fn ready(&self) -> BoxFuture<'_, Result<(), String>> {
        async move {
            let gateway = self.gateway().await.map_err(|e| e.message("OpenClaw"))?;
            let scopes = &gateway.hello().auth.scopes;
            let missing: Vec<&str> = REQUIRED_SCOPES
                .iter()
                .copied()
                .filter(|scope| !scopes.iter().any(|s| s == scope || s == "operator.admin"))
                .collect();
            if !missing.is_empty() {
                return Err(format!(
                    "OpenClaw did not grant the link {}",
                    missing.join(", ")
                ));
            }
            Ok(())
        }
        .boxed()
    }

    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>> {
        async move {
            let list = self.gateway().await?.agents_list().await.map_err(map)?;
            Ok(list
                .agents
                .into_iter()
                .map(|agent| {
                    let name = agent
                        .identity
                        .as_ref()
                        .and_then(|identity| identity.name.clone())
                        .or(agent.name.clone())
                        .unwrap_or_else(|| agent.id.clone());
                    Agent {
                        is_default: agent.id == list.default_id,
                        description: match agent.model.as_ref().and_then(|m| m.primary.as_deref()) {
                            Some(model) => format!("OpenClaw agent on {model}"),
                            None => "OpenClaw agent".to_owned(),
                        },
                        id: agent.id,
                        name,
                    }
                })
                .collect())
        }
        .boxed()
    }

    fn chats<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Vec<Chat>, Error>> {
        async move {
            let list = self
                .gateway()
                .await?
                .sessions_list(&SessionsQuery {
                    agent_id: Some(agent.to_owned()),
                    limit: Some(100),
                    exclude_subagents: true,
                    exclude_cron: true,
                    exclude_system: true,
                    include_derived_titles: true,
                    include_last_message: true,
                    ..SessionsQuery::default()
                })
                .await
                .map_err(map)?;
            Ok(list
                .sessions
                .into_iter()
                .filter(|row| row.archived != Some(true))
                .map(|row| Chat {
                    title: row.title().unwrap_or_default().to_owned(),
                    preview: row.last_message_preview.clone().unwrap_or_default(),
                    last_active: row
                        .last_interaction_at
                        .or(row.updated_at)
                        .or(row.created_at)
                        .map(|ms| ms / 1000.0),
                    message_count: 0,
                    id: row.key,
                })
                .collect())
        }
        .boxed()
    }

    fn create_chat<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Chat, Error>> {
        async move {
            // A session key is created by its first `chat.send`
            // (`session-key.ts:38-43`); nothing to ask the gateway yet.
            let key = format!("agent:{agent}:nebo-{}", new_idempotency_key());
            Ok(Chat {
                id: key,
                title: String::new(),
                preview: String::new(),
                last_active: None,
                message_count: 0,
            })
        }
        .boxed()
    }

    fn messages<'a>(
        &'a self,
        agent: &'a str,
        chat: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Message>, Error>> {
        async move {
            let history = self
                .gateway()
                .await?
                .chat_history(&HistoryQuery {
                    session_key: chat.to_owned(),
                    agent_id: Some(agent.to_owned()),
                    limit: Some(200),
                    ..HistoryQuery::default()
                })
                .await
                .map_err(map)?;
            Ok(history
                .messages()
                .iter()
                .enumerate()
                .filter_map(|(i, row)| message(row, i))
                .collect())
        }
        .boxed()
    }

    fn model<'a>(
        &'a self,
        agent: &'a str,
        _chat: Option<&'a str>,
    ) -> BoxFuture<'a, Result<String, Error>> {
        async move {
            let list = self.gateway().await?.agents_list().await.map_err(map)?;
            list.agents
                .iter()
                .find(|a| a.id == agent)
                .ok_or_else(|| Error::NotFound(format!("The agent {agent}")))
                .map(|a| {
                    a.model
                        .as_ref()
                        .and_then(|m| m.primary.clone())
                        .unwrap_or_default()
                })
        }
        .boxed()
    }

    fn turn<'a>(
        &'a self,
        agent: &'a str,
        chat: &'a str,
        prompt: String,
    ) -> BoxFuture<'a, Result<Turn, Error>> {
        async move {
            let gateway = self.gateway().await?;
            let (events, events_rx) = mpsc::channel(64);
            let (control, control_rx) = mpsc::channel(8);
            self.router.turns.lock().expect("turns lock").insert(
                chat.to_owned(),
                Sink {
                    run_id: None,
                    events,
                },
            );
            let ack = gateway
                .chat_send(&ChatSend {
                    session_key: chat.to_owned(),
                    agent_id: Some(agent.to_owned()),
                    message: prompt,
                    idempotency_key: new_idempotency_key(),
                    queue_mode: None,
                    attachments: Vec::new(),
                })
                .await;
            let ack = match ack {
                Ok(ack) => ack,
                Err(e) => {
                    self.router.end(chat);
                    return Err(map(e));
                }
            };
            if let Some(sink) = self.router.turns.lock().expect("turns lock").get_mut(chat) {
                sink.run_id = Some(ack.run_id.clone());
            }
            tokio::spawn(steer(
                gateway,
                self.router.clone(),
                chat.to_owned(),
                agent.to_owned(),
                ack.run_id,
                control_rx,
            ));
            Ok(Turn {
                events: events_rx,
                control,
            })
        }
        .boxed()
    }
}

/// Forwards the contract's controls to the gateway.
async fn steer(
    gateway: Gateway,
    router: Arc<Router>,
    session_key: String,
    agent: String,
    run_id: String,
    mut control: mpsc::Receiver<Control>,
) {
    while let Some(control) = control.recv().await {
        let result = match control {
            Control::Cancel => {
                gateway
                    .chat_abort(&session_key, Some(&agent), Some(&run_id))
                    .await
            }
            Control::Answer { request_id, choice } => {
                let Some(id) = request_id else {
                    tracing::info!("openclaw: an approval answer without its id");
                    continue;
                };
                let kind = router
                    .approvals
                    .lock()
                    .expect("approvals lock")
                    .get(&id)
                    .map(|(_, kind)| *kind);
                let (Some(kind), Some(decision)) = (kind, decision_of(&choice)) else {
                    tracing::info!(choice, "openclaw: not a pending approval or not a decision");
                    continue;
                };
                gateway
                    .approval_resolve(&id, kind, decision)
                    .await
                    .map(|_| ())
            }
        };
        if let Err(e) = result {
            tracing::info!(error = %e, "openclaw: the gateway did not take the control");
        }
    }
}

fn decision_value(decision: Decision) -> &'static str {
    match decision {
        Decision::AllowOnce => "allow-once",
        Decision::AllowAlways => "allow-always",
        Decision::Deny => "deny",
    }
}

fn decision_label(decision: Decision) -> &'static str {
    match decision {
        Decision::AllowOnce => "Allow once",
        Decision::AllowAlways => "Always allow",
        Decision::Deny => "Deny",
    }
}

fn decision_of(value: &str) -> Option<Decision> {
    match value {
        "allow-once" => Some(Decision::AllowOnce),
        "allow-always" => Some(Decision::AllowAlways),
        "deny" => Some(Decision::Deny),
        _ => None,
    }
}

/// An OpenClaw `usage` object (`{input, output, totalTokens, …}`).
fn usage_of(usage: &Value) -> Option<Usage> {
    let input = usage.get("input").and_then(Value::as_u64);
    let output = usage.get("output").and_then(Value::as_u64);
    (input.is_some() || output.is_some()).then(|| Usage {
        input_tokens: input.unwrap_or_default(),
        output_tokens: output.unwrap_or_default(),
    })
}

/// The text of a content value: a string, or the `text` blocks of a list.
fn text_of(content: &Value) -> String {
    match content {
        Value::String(text) => text.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
            .filter_map(|b| b.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(""),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// A transcript row (`chat.history` `messages[]`, the agent's own message
/// shape): `user` and `assistant` rows with text and `toolCall` blocks
/// (`{type, id, name, arguments}`), and `toolResult` rows
/// (`{toolCallId, toolName, content, isError}`).
fn message(row: &Value, index: usize) -> Option<Message> {
    let role = match row.get("role").and_then(Value::as_str)? {
        "user" => Role::User,
        "assistant" => Role::Assistant,
        "toolResult" => Role::Tool,
        _ => return None,
    };
    let id = row
        .get("__openclaw")
        .and_then(|meta| meta.get("id"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("m{index}"));
    let content = row.get("content").cloned().unwrap_or(Value::Null);
    let tool_calls = content
        .as_array()
        .map(|blocks| {
            blocks
                .iter()
                .filter(|b| b.get("type").and_then(Value::as_str) == Some("toolCall"))
                .enumerate()
                .map(|(i, call)| ToolCall {
                    id: call
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("{id}-call-{i}")),
                    name: call
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or("tool")
                        .to_owned(),
                    input: call
                        .get("arguments")
                        .cloned()
                        .unwrap_or(Value::Object(Default::default())),
                })
                .collect()
        })
        .unwrap_or_default();
    let tool_result = (role == Role::Tool).then(|| ToolResult {
        tool_call_id: row
            .get("toolCallId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        content: text_of(&content),
        is_error: row.get("isError").and_then(Value::as_bool).unwrap_or(false),
    });
    Some(Message {
        id,
        role,
        text: if role == Role::Tool {
            String::new()
        } else {
            text_of(&content)
        },
        created_at: row
            .get("timestamp")
            .and_then(Value::as_f64)
            .map(|ms| ms / 1000.0),
        tool_calls,
        tool_result,
    })
}

fn map(e: nebo_runtimes::openclaw::gateway::Error) -> Error {
    use nebo_runtimes::openclaw::gateway::Error as E;
    match e {
        E::Connect { .. } | E::Closed { .. } | E::Timeout { .. } => {
            Error::Unavailable(e.to_string())
        }
        E::Rejected { ref code, .. } if code.contains("NOT_FOUND") => {
            Error::NotFound(e.to_string())
        }
        E::ProtocolMismatch { .. }
        | E::PairingRequired { .. }
        | E::Handshake { .. }
        | E::Rejected { .. }
        | E::Decode(_) => Error::Failed(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn transcript_rows_are_mapped() {
        let assistant = json!({
            "role": "assistant", "timestamp": 1790397297952.0,
            "content": [{"type": "text", "text": "Running."}, {"type": "toolCall", "id": "call_1", "name": "exec", "arguments": {"command": "uname -a"}}],
            "__openclaw": {"id": "aac0785c"}
        });
        let m = message(&assistant, 0).unwrap();
        assert_eq!(m.id, "aac0785c");
        assert_eq!(m.text, "Running.");
        assert_eq!(m.created_at, Some(1790397297.952));
        assert_eq!(m.tool_calls[0].id, "call_1");
        assert_eq!(m.tool_calls[0].input["command"], "uname -a");

        let result = json!({ "role": "toolResult", "toolCallId": "call_1", "toolName": "exec", "isError": false, "content": [{"type": "text", "text": "Darwin"}] });
        let m = message(&result, 1).unwrap();
        assert_eq!(m.role, Role::Tool);
        assert_eq!(m.id, "m1");
        assert_eq!(m.tool_result.unwrap().content, "Darwin");

        assert!(message(&json!({ "role": "system", "content": "x" }), 2).is_none());
    }

    #[test]
    fn usage_and_decisions() {
        assert_eq!(
            usage_of(&json!({"input": 388, "output": 1, "totalTokens": 17321})),
            Some(Usage {
                input_tokens: 388,
                output_tokens: 1
            })
        );
        assert_eq!(usage_of(&json!({})), None);
        assert_eq!(decision_of("allow-once"), Some(Decision::AllowOnce));
        assert_eq!(decision_of("once"), None);
    }
}
