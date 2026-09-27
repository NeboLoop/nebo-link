//! Runtimes that don't speak ACP (OpenClaw, Hermes), adapted into it as
//! Open Agent Link's Appendix A maps them. A runtime implements
//! [`Runtime`] once, in its own terms (its agents, chats, transcripts, one
//! streamed turn at a time); [`Adapted`] serves that as ACP to the host, so
//! the host and every client see one protocol whatever runs the agent.
//!
//! | ACP | [`Runtime`] |
//! |---|---|
//! | `session/new` | [`Runtime::create_chat`] |
//! | `session/list` | [`Runtime::chats`] (preview and message count in `_meta`) |
//! | `session/load` | [`Runtime::messages`], replayed as updates |
//! | `session/resume` | nothing to do: the runtime keeps the session |
//! | `session/prompt` | [`Runtime::turn`]: text as `agent_message_chunk`, thinking as `agent_thought_chunk`, tools as `tool_call` / `tool_call_update`, asks as `session/request_permission`, the end as the `PromptResponse` (`usage` included) or `turn_failed` |
//! | `session/cancel` | [`Control::Cancel`] |
//! | permission answer | [`Control::Answer`]; an ask answered in the runtime's own interface is withdrawn |

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

use crate::backend::{
    Agent, AgentMessage, Backend, BoxFuture, Error, ErrorObject, FromAgent, Inbox, Reply,
};
use crate::model::{self, PermissionOption, StopReason, ToolCallUpdate, Usage, Words, code};

/// One conversation: a runtime session.
#[derive(Debug, Clone, PartialEq)]
pub struct Chat {
    /// The runtime's session id.
    pub id: String,
    pub title: String,
    pub preview: String,
    /// Unix seconds of the last activity.
    pub last_active: Option<f64>,
    pub message_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    User,
    Assistant,
    /// A tool's output, answering one of an assistant message's calls.
    Tool,
}

/// A stored message of a chat.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub id: String,
    pub role: Role,
    pub text: String,
    /// Unix seconds.
    pub created_at: Option<f64>,
    /// The calls an assistant message made, in order.
    pub tool_calls: Vec<ToolCall>,
    /// The result a tool message carries.
    pub tool_result: Option<ToolResult>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolResult {
    pub tool_call_id: String,
    pub content: String,
    pub is_error: bool,
}

/// The runtime stopped for the owner's decision: ACP's
/// `session/request_permission`, and the same in the owner's words.
#[derive(Debug, Clone, PartialEq)]
pub struct Ask {
    /// The runtime's id for the request, when it gives one.
    pub request_id: Option<String>,
    /// The call it asks about.
    pub tool_call: ToolCallUpdate,
    /// The answers it offers; an option's id is what it is answered with.
    pub options: Vec<PermissionOption>,
    pub words: Words,
}

/// What a turn emits, in order, ending with exactly one of `Completed`,
/// `Failed` or `Cancelled`.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnEvent {
    Text(String),
    Thinking(String),
    ToolStart {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        id: String,
        name: String,
        result: String,
        is_error: bool,
        duration_ms: Option<u64>,
    },
    Ask(Box<Ask>),
    /// An ask was answered, from anywhere (the runtime's own UI included).
    AskAnswered {
        request_id: Option<String>,
    },
    Completed {
        stop_reason: StopReason,
        usage: Option<Usage>,
    },
    Failed(String),
    Cancelled,
}

/// What the host sends a running turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    Cancel,
    Answer {
        request_id: Option<String>,
        /// A [`PermissionOption::option_id`] of the ask.
        choice: String,
    },
}

/// A turn in progress: its events, and the channel to steer it.
pub struct Turn {
    pub events: mpsc::Receiver<TurnEvent>,
    pub control: mpsc::Sender<Control>,
}

/// A runtime that doesn't speak ACP, in its own terms.
pub trait Runtime: Send + Sync + 'static {
    /// Whether the runtime can serve chats now: reachable, and every feature
    /// the host needs present. The error says what is missing.
    fn ready(&self) -> BoxFuture<'_, Result<(), String>>;
    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>>;
    /// The agent's chats, most recent first.
    fn chats<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Vec<Chat>, Error>>;
    fn create_chat<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Chat, Error>>;
    /// The chat's transcript, oldest first.
    fn messages<'a>(
        &'a self,
        agent: &'a str,
        chat: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Message>, Error>>;
    /// The model `chat` runs on, or the agent's current model without one.
    fn model<'a>(
        &'a self,
        agent: &'a str,
        chat: Option<&'a str>,
    ) -> BoxFuture<'a, Result<String, Error>>;
    /// Sends `prompt` on `chat` and starts streaming the turn. The runtime
    /// holds the transcript; only the new message is sent.
    fn turn<'a>(
        &'a self,
        agent: &'a str,
        chat: &'a str,
        prompt: String,
    ) -> BoxFuture<'a, Result<Turn, Error>>;
}

/// A shared runtime is the runtime.
impl<R: Runtime> Runtime for Arc<R> {
    fn ready(&self) -> BoxFuture<'_, Result<(), String>> {
        (**self).ready()
    }
    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>> {
        (**self).agents()
    }
    fn chats<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Vec<Chat>, Error>> {
        (**self).chats(agent)
    }
    fn create_chat<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Chat, Error>> {
        (**self).create_chat(agent)
    }
    fn messages<'a>(&'a self, agent: &'a str, chat: &'a str) -> BoxFuture<'a, Result<Vec<Message>, Error>> {
        (**self).messages(agent, chat)
    }
    fn model<'a>(&'a self, agent: &'a str, chat: Option<&'a str>) -> BoxFuture<'a, Result<String, Error>> {
        (**self).model(agent, chat)
    }
    fn turn<'a>(&'a self, agent: &'a str, chat: &'a str, prompt: String) -> BoxFuture<'a, Result<Turn, Error>> {
        (**self).turn(agent, chat, prompt)
    }
}

/// A [`Runtime`] served as ACP.
pub struct Adapted<R> {
    /// The runtime's name, for the owner's messages ("Could not connect to
    /// Hermes. Try again.").
    name: String,
    runtime: Arc<R>,
    shared: Arc<Shared>,
}

#[derive(Default)]
struct Shared {
    inbox: Mutex<Option<Inbox>>,
    /// The running turn of each session, by (agent, session).
    turns: Mutex<HashMap<(String, String), mpsc::Sender<Control>>>,
    next_reply: Mutex<u64>,
}

impl Shared {
    fn send(&self, agent: &str, message: AgentMessage) {
        let inbox = self.inbox.lock().expect("inbox").clone();
        if let Some(inbox) = inbox {
            inbox(FromAgent {
                agent: agent.to_owned(),
                message,
            });
        }
    }

    fn update(&self, agent: &str, session: &str, update: Value) {
        self.send(
            agent,
            AgentMessage::Update {
                session_id: session.to_owned(),
                update,
            },
        );
    }

    fn reply_id(&self) -> u64 {
        let mut next = self.next_reply.lock().expect("reply ids");
        *next += 1;
        *next
    }
}

impl<R: Runtime> Adapted<R> {
    /// `name` is the runtime's name as the owner reads it.
    pub fn new(name: impl Into<String>, runtime: R) -> Self {
        Self {
            name: name.into(),
            runtime: Arc::new(runtime),
            shared: Arc::default(),
        }
    }

    /// The runtime, for what only it answers.
    pub fn runtime(&self) -> &R {
        &self.runtime
    }

    fn refusal(&self, error: Error, failed: i64) -> ErrorObject {
        let code = match &error {
            Error::Unavailable(_) => code::AGENT_UNAVAILABLE,
            Error::NotFound(_) => code::NOT_FOUND,
            Error::Failed(_) => failed,
        };
        ErrorObject::new(code, error.message(&self.name))
    }

    async fn model_state(&self, agent: &str, session: &str) -> Option<Value> {
        let model = self.runtime.model(agent, Some(session)).await.ok()?;
        (!model.is_empty()).then(|| {
            json!({ "currentModelId": model, "availableModels": [{ "modelId": model, "name": model }] })
        })
    }

    async fn new_session(&self, agent: &str) -> Result<Value, ErrorObject> {
        let chat = self
            .runtime
            .create_chat(agent)
            .await
            .map_err(|e| self.refusal(e, code::INTERNAL))?;
        let mut result = json!({ "sessionId": chat.id });
        if let Some(models) = self.model_state(agent, &chat.id).await {
            result["models"] = models;
        }
        Ok(result)
    }

    async fn list(&self, agent: &str) -> Result<Value, ErrorObject> {
        let chats = self
            .runtime
            .chats(agent)
            .await
            .map_err(|e| self.refusal(e, code::INTERNAL))?;
        let sessions: Vec<Value> = chats
            .into_iter()
            .map(|c| {
                let mut session = json!({
                    "sessionId": c.id,
                    "cwd": "/",
                    "_meta": { "preview": c.preview, "messageCount": c.message_count },
                });
                if !c.title.is_empty() {
                    session["title"] = json!(c.title);
                }
                if let Some(at) = c.last_active {
                    session["updatedAt"] = json!(model::rfc3339(at as i64, ((at.fract()) * 1000.0) as u32));
                }
                session
            })
            .collect();
        Ok(json!({ "sessions": sessions }))
    }

    /// Replays the session's messages as updates, then answers.
    async fn load(&self, agent: &str, session: &str, replay: bool) -> Result<Value, ErrorObject> {
        if replay {
            let messages = self
                .runtime
                .messages(agent, session)
                .await
                .map_err(|e| self.refusal(e, code::INTERNAL))?;
            for update in replayed(&messages) {
                self.shared.update(agent, session, update);
            }
        }
        let mut result = json!({});
        if let Some(models) = self.model_state(agent, session).await {
            result["models"] = models;
        }
        Ok(result)
    }

    async fn prompt(&self, agent: &str, session: &str, prompt: &Value) -> Result<Value, ErrorObject> {
        let text = prompt_text(prompt);
        let turn = self
            .runtime
            .turn(agent, session, text)
            .await
            .map_err(|e| self.refusal(e, code::TURN_FAILED))?;
        let key = (agent.to_owned(), session.to_owned());
        self.shared
            .turns
            .lock()
            .expect("turns")
            .insert(key.clone(), turn.control.clone());
        let result = self.run(agent, session, turn).await;
        self.shared.turns.lock().expect("turns").remove(&key);
        result
    }

    /// Relays one turn's events as ACP until it ends.
    async fn run(&self, agent: &str, session: &str, turn: Turn) -> Result<Value, ErrorObject> {
        let Turn {
            mut events,
            control,
        } = turn;
        // The asks waiting for an answer: the host's reply id, the runtime's
        // request id.
        let mut asks: Vec<(u64, Option<String>)> = Vec::new();
        // The tool calls the runtime started, by id.
        let mut started: Vec<String> = Vec::new();
        let mut answers: FuturesUnordered<BoxFuture<'static, Answer>> = FuturesUnordered::new();
        loop {
            tokio::select! {
                event = events.recv() => {
                    let Some(event) = event else {
                        return Err(ErrorObject::new(
                            code::AGENT_UNAVAILABLE,
                            format!("Could not connect to {}. Try again.", self.name),
                        ));
                    };
                    match event {
                        TurnEvent::Text(text) => self.shared.update(agent, session, chunk("agent_message_chunk", &text)),
                        TurnEvent::Thinking(text) => self.shared.update(agent, session, chunk("agent_thought_chunk", &text)),
                        TurnEvent::ToolStart { id, name, input } => {
                            started.push(id.clone());
                            self.shared.update(
                                agent,
                                session,
                                json!({ "sessionUpdate": "tool_call", "toolCallId": id, "title": name, "kind": "other",
                                    "status": "in_progress", "rawInput": input }),
                            )
                        }
                        TurnEvent::ToolResult { id, name, result, is_error, duration_ms } => {
                            // ACP announces a call before it updates one: a
                            // result for a call the runtime never started is
                            // the call, finished.
                            let kind = if started.contains(&id) { "tool_call_update" } else { "tool_call" };
                            let mut update = json!({ "sessionUpdate": kind, "toolCallId": id, "title": name,
                                "status": if is_error { "failed" } else { "completed" },
                                "content": ToolCallUpdate::text(&result) });
                            if kind == "tool_call" {
                                update["kind"] = json!("other");
                            }
                            if let Some(ms) = duration_ms {
                                update["_meta"] = json!({ "durationMs": ms });
                            }
                            self.shared.update(agent, session, update);
                        }
                        TurnEvent::Ask(ask) => {
                            let id = self.shared.reply_id();
                            let (reply, rx) = Reply::new(id);
                            let runtime_id = ask.request_id.clone();
                            asks.push((id, runtime_id.clone()));
                            answers.push(Box::pin(async move { (id, runtime_id, rx.await) }));
                            // The runtime's own words ride along, so a client
                            // across Open Agent Link shows the card it wrote.
                            let params = crate::turn::with_words(
                                json!({ "sessionId": session, "toolCall": ask.tool_call, "options": ask.options }),
                                &ask.words,
                            );
                            self.shared.send(agent, AgentMessage::Permission {
                                session_id: session.to_owned(),
                                params,
                                words: Some(ask.words),
                                reply,
                            });
                        }
                        TurnEvent::AskAnswered { request_id } => {
                            let found = match &request_id {
                                Some(runtime_id) => asks.iter().position(|(_, r)| r.as_ref() == Some(runtime_id)),
                                None => (!asks.is_empty()).then_some(0),
                            };
                            if let Some(i) = found {
                                let (reply, _) = asks.remove(i);
                                self.shared.send(agent, AgentMessage::Withdrawn { reply });
                            }
                        }
                        TurnEvent::Completed { stop_reason, usage } => {
                            let mut result = json!({ "stopReason": stop_reason });
                            if let Some(usage) = usage {
                                result["usage"] = json!(usage);
                            }
                            return Ok(result);
                        }
                        TurnEvent::Failed(message) => return Err(ErrorObject::new(code::TURN_FAILED, message)),
                        TurnEvent::Cancelled => return Ok(json!({ "stopReason": StopReason::Cancelled })),
                    }
                }
                Some((id, runtime_id, answer)) = answers.next(), if !answers.is_empty() => {
                    let Some(i) = asks.iter().position(|(a, _)| *a == id) else { continue };
                    asks.remove(i);
                    // A selected option goes to the runtime; a cancelled
                    // answer comes with the turn's cancel.
                    if let Ok(response) = answer
                        && let Some(choice) = response["outcome"]["optionId"].as_str()
                        && response["outcome"]["outcome"] == "selected"
                    {
                        let _ = control.send(Control::Answer { request_id: runtime_id, choice: choice.to_owned() }).await;
                    }
                }
            }
        }
    }
}

impl<R: Runtime> Backend for Adapted<R> {
    fn ready(&self) -> BoxFuture<'_, Result<(), String>> {
        self.runtime.ready()
    }

    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>> {
        self.runtime.agents()
    }

    fn connect(&self, inbox: Inbox) {
        *self.shared.inbox.lock().expect("inbox") = Some(inbox);
    }

    fn request<'a>(
        &'a self,
        agent: &'a str,
        method: &'a str,
        params: Value,
    ) -> BoxFuture<'a, Result<Value, ErrorObject>> {
        Box::pin(async move {
            let session = params["sessionId"].as_str().unwrap_or("").to_owned();
            match method {
                "session/new" => self.new_session(agent).await,
                "session/list" => self.list(agent).await,
                "session/load" => self.load(agent, &session, true).await,
                "session/resume" => self.load(agent, &session, false).await,
                "session/prompt" => self.prompt(agent, &session, &params["prompt"]).await,
                "session/close" => Ok(json!({})),
                _ => Err(ErrorObject::new(code::METHOD_NOT_FOUND, format!("{} can't do {method}.", self.name))),
            }
        })
    }

    fn notify(&self, agent: &str, method: &str, params: Value) {
        if method != "session/cancel" {
            return;
        }
        let key = (agent.to_owned(), params["sessionId"].as_str().unwrap_or("").to_owned());
        let control = self.shared.turns.lock().expect("turns").get(&key).cloned();
        if let Some(control) = control {
            tokio::spawn(async move {
                let _ = control.send(Control::Cancel).await;
            });
        }
    }

    fn model<'a>(
        &'a self,
        agent: &'a str,
        session: Option<&'a str>,
    ) -> BoxFuture<'a, Result<String, Error>> {
        self.runtime.model(agent, session)
    }

    /// OpenClaw and Hermes keep their own transcripts, which their own
    /// interfaces and channels write to as well.
    fn shared_sessions(&self) -> bool {
        true
    }
}

/// An ask's answer as it arrives: the host's reply id, the runtime's
/// request id, and the answer (none if the host dropped it).
type Answer = (u64, Option<String>, Result<Value, oneshot::error::RecvError>);

/// A text chunk update.
fn chunk(kind: &str, text: &str) -> Value {
    json!({ "sessionUpdate": kind, "content": { "type": "text", "text": text } })
}

/// A prompt's text: its text blocks, and each linked file by its name and
/// address, for a runtime that reads text only.
fn prompt_text(prompt: &Value) -> String {
    let parts: Vec<String> = prompt
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|block| match block["type"].as_str()? {
            "text" => block["text"].as_str().map(str::to_owned),
            "resource_link" => {
                let uri = block["uri"].as_str()?;
                Some(match block["name"].as_str() {
                    Some(name) => format!("{name}: {uri}"),
                    None => uri.to_owned(),
                })
            }
            _ => None,
        })
        .collect();
    parts.join("\n")
}

/// A stored transcript as the updates `session/load` replays: each message's
/// text as a chunk (its id as `messageId`, its time as `_meta.createdAt`),
/// each tool call as `tool_call` and its result as `tool_call_update`.
pub fn replayed(messages: &[Message]) -> Vec<Value> {
    let mut updates = Vec::new();
    for m in messages {
        let stamp = |mut update: Value| {
            update["messageId"] = json!(m.id);
            if let Some(at) = m.created_at {
                update["_meta"] = json!({ "createdAt": at });
            }
            update
        };
        match m.role {
            Role::User => updates.push(stamp(chunk("user_message_chunk", &m.text))),
            Role::Assistant => {
                if !m.text.is_empty() {
                    updates.push(stamp(chunk("agent_message_chunk", &m.text)));
                }
                for call in &m.tool_calls {
                    let mut update = json!({ "sessionUpdate": "tool_call", "toolCallId": call.id, "title": call.name,
                        "kind": "other", "status": "pending", "rawInput": call.input });
                    if let Some(at) = m.created_at {
                        update["_meta"] = json!({ "createdAt": at });
                    }
                    updates.push(update);
                }
            }
            Role::Tool => {
                if let Some(result) = &m.tool_result {
                    let mut update = json!({ "sessionUpdate": "tool_call_update", "toolCallId": result.tool_call_id,
                        "status": if result.is_error { "failed" } else { "completed" },
                        "content": ToolCallUpdate::text(&result.content) });
                    if let Some(at) = m.created_at {
                        update["_meta"] = json!({ "createdAt": at });
                    }
                    updates.push(update);
                }
            }
        }
    }
    updates
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prompt_reads_as_text() {
        let prompt = json!([
            { "type": "text", "text": "Read this" },
            { "type": "resource_link", "uri": "file:///tmp/a.pdf", "name": "a.pdf" },
            { "type": "image", "data": "..." }
        ]);
        assert_eq!(prompt_text(&prompt), "Read this\na.pdf: file:///tmp/a.pdf");
    }

    #[test]
    fn a_transcript_replays_as_updates() {
        let messages = vec![
            Message {
                id: "1".into(),
                role: Role::User,
                text: "list".into(),
                created_at: Some(10.0),
                tool_calls: vec![],
                tool_result: None,
            },
            Message {
                id: "2".into(),
                role: Role::Assistant,
                text: String::new(),
                created_at: Some(11.0),
                tool_calls: vec![ToolCall {
                    id: "c1".into(),
                    name: "terminal".into(),
                    input: json!({ "command": "ls" }),
                }],
                tool_result: None,
            },
            Message {
                id: "3".into(),
                role: Role::Tool,
                text: String::new(),
                created_at: Some(12.0),
                tool_calls: vec![],
                tool_result: Some(ToolResult {
                    tool_call_id: "c1".into(),
                    content: "a b".into(),
                    is_error: true,
                }),
            },
        ];
        let updates = replayed(&messages);
        let kinds: Vec<&str> = updates.iter().map(|u| u["sessionUpdate"].as_str().unwrap()).collect();
        assert_eq!(kinds, ["user_message_chunk", "tool_call", "tool_call_update"]);
        assert_eq!(updates[0]["messageId"], "1");
        assert_eq!(updates[0]["_meta"]["createdAt"], 10.0);
        assert_eq!(updates[1]["title"], "terminal");
        assert_eq!(updates[2]["status"], "failed");
        assert_eq!(updates[2]["content"][0]["content"]["text"], "a b");
    }
}
