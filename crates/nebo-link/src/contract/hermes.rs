//! The Hermes backend: a profile is an agent, a session is a chat, and a
//! turn is one `/v1/runs` run followed on its SSE stream
//! ([`nebo_runtimes::hermes::runs`]).
//!
//! # Which Hermes API a turn uses
//!
//! Runs + SSE on every version, never the session-native
//! `POST /api/sessions/{id}/chat/stream`. On the released v0.19.0 the
//! session-native stream (`api_server.py` `_handle_session_chat_stream`) has
//! no run id in the run registry, so `/v1/runs/{id}/stop` and
//! `/v1/runs/{id}/approval` can't reach it: no cancel and no approval card.
//! Runs give all three on 0.19.0 (the 2026-09-26 spike, nebo-link #11).
//!
//! What differs by version is the transcript. A run with `session_id` loads
//! that session's history itself only since hermes-agent `3db45bab6e`
//! (2026-09-07, "load declared-key history"), first released in v2026.9.11
//! as `0.21.2` (the v2026.9.7 tag, `0.21.1`, does not contain it); 0.19.0
//! runs every turn with an empty history while still storing the turn on
//! the session. So on a server older than [`LOADS_SESSION_SINCE`] the link
//! reads the session's own messages and sends them as
//! `conversation_history`, the runtime's store being the only copy; on a
//! newer one it sends the message alone. The version comes from the API
//! server's own `GET /health` (`{"version"}` on both, `_hermes_version`),
//! read per turn so a Hermes upgrade under a running link is honoured.
//!
//! Approvals: Hermes asks only when its own gate does (`approvals.mode`);
//! the card offers whatever `choices` the event carries. 0.19.0 sends no
//! `request_id`, so the answer resolves the oldest pending request, which is
//! the only one a run can have.

use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use nebo_runtimes::hermes::runs::{
    Choice as HermesChoice, Client, Event, HistoryMessage, MessageQuery, NewRun, NewSession,
    RunState, SessionQuery,
};
use serde_json::Value;
use tokio::sync::mpsc;

use super::backend::{
    Agent, Ask, Backend, BoxFuture, Chat, Choice, Control, Error, Message, Permission, Role,
    ToolCall, ToolResult, Turn, TurnEvent, Usage,
};

/// The default profile's id on the contract's backend side.
pub const DEFAULT_PROFILE: &str = "default";

/// The first Hermes version whose `/v1/runs` loads the session's transcript
/// (see the module docs).
pub const LOADS_SESSION_SINCE: [u64; 3] = [0, 21, 2];

/// How long a broken event stream is polled for the run's outcome.
const OUTCOME_PATIENCE: Duration = Duration::from_secs(600);

/// A linked Hermes install's API server.
pub struct Hermes {
    base_url: String,
    key: String,
    /// The named profiles beside the default one.
    profiles: Vec<String>,
}

impl Hermes {
    /// `base_url` is the default profile's listener (`http://127.0.0.1:8642`);
    /// `key` is the `API_SERVER_KEY` the link wrote for every profile.
    pub fn new(base_url: &str, key: &str, profiles: Vec<String>) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_owned(),
            key: key.to_owned(),
            profiles,
        }
    }

    fn client(&self, profile: &str) -> Result<Client, Error> {
        if profile != DEFAULT_PROFILE && !self.profiles.iter().any(|p| p == profile) {
            return Err(Error::NotFound(format!("The profile {profile}")));
        }
        Ok(Client::new(&self.base_url, Some(profile), &self.key))
    }

    /// Whether this server's `/v1/runs` loads the session itself.
    async fn loads_session(&self, client: &Client) -> Result<bool, Error> {
        let health = client.health().await.map_err(map)?;
        Ok(version_at_least(&health.version, &LOADS_SESSION_SINCE))
    }

    /// The session's transcript as `/v1/runs` takes it: user and assistant
    /// text only, the way Hermes' own `get_messages_as_conversation` feeds a
    /// turn.
    async fn history(&self, client: &Client, chat: &str) -> Result<Vec<HistoryMessage>, Error> {
        let page = client
            .messages(chat, &MessageQuery::default())
            .await
            .map_err(map)?;
        Ok(page
            .data
            .iter()
            .filter(|m| m.role == "user" || m.role == "assistant")
            .map(|m| HistoryMessage {
                role: m.role.clone(),
                content: m.text(),
            })
            .filter(|m| !m.content.is_empty())
            .collect())
    }
}

impl Backend for Hermes {
    fn ready(&self) -> BoxFuture<'_, Result<(), String>> {
        async move {
            let client = Client::new(&self.base_url, None, &self.key);
            let caps = client.capabilities().await.map_err(|e| match e {
                nebo_runtimes::hermes::runs::Error::Transport { .. } => {
                    format!(
                        "the Hermes API server at {} is not answering",
                        self.base_url
                    )
                }
                e => format!("the Hermes API server refused the link: {e}"),
            })?;
            let missing = caps.missing();
            if !missing.is_empty() {
                return Err(format!("this Hermes lacks {}", missing.join(", ")));
            }
            Ok(())
        }
        .boxed()
    }

    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>> {
        async move {
            let mut agents = vec![Agent {
                id: DEFAULT_PROFILE.to_owned(),
                name: "Hermes".to_owned(),
                description: "The default Hermes profile".to_owned(),
                is_default: true,
            }];
            agents.extend(self.profiles.iter().map(|name| Agent {
                id: name.clone(),
                name: name.clone(),
                description: format!("The Hermes profile {name}"),
                is_default: false,
            }));
            Ok(agents)
        }
        .boxed()
    }

    fn chats<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Vec<Chat>, Error>> {
        async move {
            let client = self.client(agent)?;
            let page = client
                .sessions(&SessionQuery {
                    limit: Some(100),
                    ..SessionQuery::default()
                })
                .await
                .map_err(map)?;
            Ok(page
                .data
                .into_iter()
                .filter(|s| !s.hidden && !s.archived)
                .map(|s| Chat {
                    id: s.id,
                    title: s.title.unwrap_or_default(),
                    preview: s.preview.unwrap_or_default(),
                    last_active: s.last_active.or(s.started_at),
                    message_count: s.message_count.unwrap_or_default(),
                })
                .collect())
        }
        .boxed()
    }

    fn create_chat<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Chat, Error>> {
        async move {
            let client = self.client(agent)?;
            let session = client
                .create_session(&NewSession::default())
                .await
                .map_err(map)?;
            Ok(Chat {
                id: session.id,
                title: session.title.unwrap_or_default(),
                preview: String::new(),
                last_active: session.started_at,
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
            let client = self.client(agent)?;
            let page = client
                .messages(chat, &MessageQuery::default())
                .await
                .map_err(map)?;
            Ok(page
                .data
                .iter()
                .enumerate()
                .filter_map(|(i, m)| message(m, i))
                .collect())
        }
        .boxed()
    }

    fn model<'a>(
        &'a self,
        agent: &'a str,
        chat: Option<&'a str>,
    ) -> BoxFuture<'a, Result<String, Error>> {
        async move {
            let client = self.client(agent)?;
            if let Some(chat) = chat {
                let session = client.session(chat).await.map_err(map)?;
                if let Some(model) = session.model.filter(|m| !m.is_empty()) {
                    return Ok(model);
                }
            }
            Ok(client.capabilities().await.map_err(map)?.model)
        }
        .boxed()
    }

    /// Hermes and OpenClaw keep their own approval settings: the
    /// permission is not theirs to take.
    fn turn<'a>(
        &'a self,
        agent: &'a str,
        chat: &'a str,
        prompt: String,
        _permission: Option<Permission>,
    ) -> BoxFuture<'a, Result<Turn, Error>> {
        async move {
            let client = self.client(agent)?;
            let conversation_history = if self.loads_session(&client).await? {
                None
            } else {
                Some(self.history(&client, chat).await?)
            };
            let started = client
                .start_run(&NewRun {
                    input: prompt,
                    session_id: Some(chat.to_owned()),
                    conversation_history,
                    idempotency_key: uuid::Uuid::new_v4().to_string(),
                })
                .await
                .map_err(map)?;
            let stream = client.events(&started.run_id, None).await.map_err(map)?;
            let (events, events_rx) = mpsc::channel(64);
            let (control, control_rx) = mpsc::channel(8);
            let run_id: Arc<str> = started.run_id.into();
            tokio::spawn(steer(client.clone(), run_id.clone(), control_rx));
            tokio::spawn(relay(client, run_id, stream, events));
            Ok(Turn {
                events: events_rx,
                control,
            })
        }
        .boxed()
    }
}

/// Forwards the contract's controls to the run.
async fn steer(client: Client, run_id: Arc<str>, mut control: mpsc::Receiver<Control>) {
    while let Some(control) = control.recv().await {
        let result = match control {
            Control::Cancel => client.stop(&run_id).await.map(|_| ()),
            Control::Answer { request_id, choice } => match choice.parse::<HermesChoice>() {
                Ok(choice) => client
                    .approve(&run_id, choice, request_id.as_deref())
                    .await
                    .map(|_| ()),
                Err(()) => {
                    tracing::info!(choice, "hermes: not an approval choice");
                    continue;
                }
            },
        };
        if let Err(e) = result {
            tracing::info!(error = %e, "hermes: the run did not take the control");
        }
    }
}

/// Turns the run's events into the contract's, ending with one terminal
/// event. A stream that breaks before the run ends is replaced by polling
/// the run's status for its outcome (0.19.0 keeps no replay).
async fn relay(
    client: Client,
    run_id: Arc<str>,
    mut stream: nebo_runtimes::hermes::runs::EventStream,
    events: mpsc::Sender<TurnEvent>,
) {
    let mut open_tools: Vec<(String, String)> = Vec::new();
    let mut tool_seq = 0u32;
    loop {
        let envelope = match stream.next().await {
            Some(Ok(envelope)) => envelope,
            Some(Err(e)) => {
                tracing::info!(error = %e, "hermes: the run's event stream broke; reading its outcome");
                let _ = events.send(outcome(&client, &run_id).await).await;
                return;
            }
            None => {
                let _ = events.send(outcome(&client, &run_id).await).await;
                return;
            }
        };
        let event = match envelope.event {
            Event::MessageDelta { delta } => TurnEvent::Text(delta),
            Event::MessageInterim {
                text,
                already_streamed,
            } => {
                if already_streamed {
                    continue;
                }
                TurnEvent::Text(text)
            }
            Event::Reasoning { text } => TurnEvent::Thinking(text),
            Event::ToolStarted { tool, preview } => {
                tool_seq += 1;
                let id = format!("{run_id}-tool-{tool_seq}");
                open_tools.push((tool.clone(), id.clone()));
                TurnEvent::ToolStart {
                    id,
                    name: tool,
                    input: tool_input(&preview),
                }
            }
            Event::ToolCompleted {
                tool,
                duration,
                error,
                preview,
            } => {
                let id = match open_tools.iter().position(|(name, _)| *name == tool) {
                    Some(i) => open_tools.remove(i).1,
                    None => {
                        tool_seq += 1;
                        format!("{run_id}-tool-{tool_seq}")
                    }
                };
                TurnEvent::ToolResult {
                    id,
                    name: tool,
                    result: preview,
                    is_error: error,
                    duration_ms: Some((duration * 1000.0).round().max(0.0) as u64),
                }
            }
            Event::ApprovalRequest(request) => {
                let command = request.command.clone().unwrap_or_default();
                let description = request.description.clone().unwrap_or_default();
                let choices = if request.choices.is_empty() {
                    vec![HermesChoice::Once, HermesChoice::Deny]
                } else {
                    request.choices.clone()
                };
                TurnEvent::Ask(Ask {
                    request_id: request.request_id.clone(),
                    prompt: match description.trim() {
                        "" => format!("Hermes asks to run:\n{command}"),
                        description => format!("Hermes asks to run:\n{command}\n\n{description}"),
                    },
                    summary: format!("run {command}"),
                    choices: choices
                        .into_iter()
                        .map(|c| Choice {
                            value: c.as_str().to_owned(),
                            label: choice_label(c).to_owned(),
                        })
                        .collect(),
                })
            }
            Event::ApprovalResponded { request_id, .. } => TurnEvent::AskAnswered { request_id },
            Event::Completed(done) => TurnEvent::Completed {
                usage: done.usage.map(usage),
            },
            Event::Failed(failed) => TurnEvent::Failed(
                failed
                    .error
                    .unwrap_or_else(|| "Hermes could not finish the turn.".to_owned()),
            ),
            Event::Cancelled(_) => TurnEvent::Cancelled,
            Event::Interrupted(gone) => TurnEvent::Failed(
                gone.error
                    .unwrap_or_else(|| "Hermes restarted during the turn.".to_owned()),
            ),
            Event::Steered { .. }
            | Event::Subagent { .. }
            | Event::ReplayTruncated { .. }
            | Event::Other { .. } => {
                continue;
            }
        };
        let terminal = matches!(
            event,
            TurnEvent::Completed { .. } | TurnEvent::Failed(_) | TurnEvent::Cancelled
        );
        if events.send(event).await.is_err() || terminal {
            return;
        }
    }
}

/// The run's terminal event, read from its status.
async fn outcome(client: &Client, run_id: &str) -> TurnEvent {
    let deadline = tokio::time::Instant::now() + OUTCOME_PATIENCE;
    loop {
        match client.run(run_id).await {
            Ok(status) if status.status.is_terminal() => {
                return match status.status {
                    RunState::Completed => TurnEvent::Completed {
                        usage: status.usage.map(usage),
                    },
                    RunState::Cancelled => TurnEvent::Cancelled,
                    _ => TurnEvent::Failed(
                        status
                            .error
                            .unwrap_or_else(|| "Hermes could not finish the turn.".to_owned()),
                    ),
                };
            }
            Ok(_) => {}
            Err(e) => return TurnEvent::Failed(Error::from(e).message("Hermes")),
        }
        if tokio::time::Instant::now() >= deadline {
            return TurnEvent::Failed("Hermes did not finish the turn.".to_owned());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn usage(u: nebo_runtimes::hermes::runs::Usage) -> Usage {
    Usage {
        input_tokens: u.input_tokens,
        output_tokens: u.output_tokens,
    }
}

fn choice_label(choice: HermesChoice) -> &'static str {
    match choice {
        HermesChoice::Once => "Allow once",
        HermesChoice::Session => "Allow for this session",
        HermesChoice::Always => "Always allow",
        HermesChoice::Deny => "Deny",
    }
}

/// A tool's arguments as the card shows them: the preview parsed when it is
/// JSON (`{"command": "ls"}`), otherwise the text itself.
fn tool_input(preview: &str) -> Value {
    match serde_json::from_str::<Value>(preview) {
        Ok(value @ Value::Object(_)) => value,
        _ => serde_json::json!({ "input": preview }),
    }
}

/// A stored Hermes message as the contract's. Tool calls are OpenAI-shaped
/// (`{id, function: {name, arguments}}`, `hermes_state.py` `add_message`).
fn message(m: &nebo_runtimes::hermes::runs::Message, index: usize) -> Option<Message> {
    let role = match m.role.as_str() {
        "user" => Role::User,
        "assistant" => Role::Assistant,
        "tool" => Role::Tool,
        _ => return None,
    };
    let id = match &m.id {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) => s.clone(),
        _ => format!("m{index}"),
    };
    let tool_calls = m
        .tool_calls
        .as_ref()
        .and_then(Value::as_array)
        .map(|calls| {
            calls
                .iter()
                .enumerate()
                .map(|(i, call)| {
                    let function = call.get("function").unwrap_or(call);
                    let arguments = function.get("arguments").cloned().unwrap_or(Value::Null);
                    ToolCall {
                        id: call
                            .get("id")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .unwrap_or_else(|| format!("{id}-call-{i}")),
                        name: function
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("tool")
                            .to_owned(),
                        input: match arguments {
                            Value::String(text) => tool_input(&text),
                            Value::Null => Value::Object(Default::default()),
                            other => other,
                        },
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let tool_result = (role == Role::Tool).then(|| ToolResult {
        tool_call_id: m.tool_call_id.clone().unwrap_or_default(),
        content: m.text(),
        is_error: false,
    });
    Some(Message {
        id,
        role,
        text: if role == Role::Tool {
            String::new()
        } else {
            m.text()
        },
        created_at: m.timestamp,
        tool_calls,
        tool_result,
    })
}

fn map(e: nebo_runtimes::hermes::runs::Error) -> Error {
    Error::from(e)
}

impl From<nebo_runtimes::hermes::runs::Error> for Error {
    fn from(e: nebo_runtimes::hermes::runs::Error) -> Self {
        use nebo_runtimes::hermes::runs::Error as E;
        match e {
            E::Transport { .. } | E::StreamLost { .. } => Error::Unavailable(e.to_string()),
            E::Api {
                status: 404,
                message,
                ..
            } => Error::NotFound(message),
            E::Api { message, .. } => Error::Failed(message),
            E::Protocol { .. } => Error::Failed(e.to_string()),
        }
    }
}

/// Whether a dotted version (`0.19.0`, `0.21.5`) is at least `min`;
/// suffixes after a `-` or `+` are ignored, and an unreadable version is
/// treated as older.
fn version_at_least(version: &str, min: &[u64]) -> bool {
    let parts: Vec<u64> = version
        .trim()
        .trim_start_matches('v')
        .split(['.', '-', '+'])
        .map_while(|part| part.parse().ok())
        .collect();
    !parts.is_empty() && parts.as_slice() >= min
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_before_0_21_2_send_history() {
        assert!(!version_at_least("0.19.0", &LOADS_SESSION_SINCE));
        assert!(!version_at_least("0.21.1", &LOADS_SESSION_SINCE));
        assert!(version_at_least("0.21.2", &LOADS_SESSION_SINCE));
        assert!(version_at_least("0.21.5", &LOADS_SESSION_SINCE));
        assert!(version_at_least("v1.0.0-rc.1", &LOADS_SESSION_SINCE));
        assert!(!version_at_least("dev", &LOADS_SESSION_SINCE));
    }

    #[test]
    fn stored_tool_calls_and_results_are_mapped() {
        let assistant: nebo_runtimes::hermes::runs::Message = serde_json::from_value(serde_json::json!({
            "id": 7, "role": "assistant", "content": "Listing.", "timestamp": 1.5,
            "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "terminal", "arguments": "{\"command\": \"ls\"}"}}]
        }))
        .unwrap();
        let m = message(&assistant, 0).unwrap();
        assert_eq!(m.id, "7");
        assert_eq!(m.role, Role::Assistant);
        assert_eq!(m.tool_calls.len(), 1);
        assert_eq!(m.tool_calls[0].id, "call_1");
        assert_eq!(m.tool_calls[0].name, "terminal");
        assert_eq!(m.tool_calls[0].input["command"], "ls");

        let tool: nebo_runtimes::hermes::runs::Message = serde_json::from_value(serde_json::json!({
            "id": 8, "role": "tool", "content": "Cargo.toml", "tool_call_id": "call_1", "tool_name": "terminal"
        }))
        .unwrap();
        let m = message(&tool, 1).unwrap();
        assert_eq!(m.role, Role::Tool);
        assert_eq!(m.tool_result.unwrap().tool_call_id, "call_1");

        let system: nebo_runtimes::hermes::runs::Message =
            serde_json::from_value(serde_json::json!({"role": "system", "content": "x"})).unwrap();
        assert!(message(&system, 2).is_none());
    }

    #[test]
    fn tool_previews_become_inputs() {
        assert_eq!(tool_input(r#"{"command": "ls"}"#)["command"], "ls");
        assert_eq!(tool_input("rm -rf ./x")["input"], "rm -rf ./x");
    }
}
