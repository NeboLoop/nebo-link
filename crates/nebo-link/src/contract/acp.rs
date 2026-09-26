//! The ACP backend: one agent process (Claude Code, Codex, Gemini CLI,
//! OpenCode, or any command that speaks ACP) driven over stdio
//! ([`nebo_runtimes::acp`]). The agent is the bot's one agent, a session is a
//! chat, and a turn is one `session/prompt`.
//!
//! - **The process.** Started on first use (the readiness probe is the first
//!   use, so it runs while the link does) in the bot's working folder, kept
//!   running, and started again on the next use after it exits: the probe
//!   every 30 s is what brings a crashed agent back. Its stderr goes to the
//!   bot's log folder. Starting runs detached from the caller, so a probe
//!   that times out while `npx` fetches the adapter doesn't kill it.
//! - **Sessions.** `session/new` in the working folder, with no MCP servers.
//!   One Nebo chat is one session; the agent keeps the transcript and only
//!   the owner's newest message is sent. A session this process has not seen
//!   is reopened with `session/load` (which replays it, giving the
//!   transcript) or else `session/resume`.
//! - **Chats.** `session/list` for the working folder where the agent serves
//!   it (so a conversation begun in the terminal there shows too), else the
//!   link's own record of the sessions it created.
//! - **Turns.** Message chunks are text; thought chunks and plans are
//!   thinking; tool calls are tool cards, announced once the agent says what
//!   the call is (it names a Bash call "Terminal" before its command
//!   arrives); `session/request_permission` is an ask whose choices are the
//!   agent's options; `session/prompt`'s `usage` is the turn's tokens.
//!   Cancel is `session/cancel`, and any question still open is answered
//!   `cancelled`, as ACP requires.
//! - **Sign-in.** The agent runs under its owner's own login. `-32000`
//!   "Authentication required" reads "Claude Code isn't signed in on this
//!   computer. Run `claude` once to sign in."

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use nebo_runtimes::RuntimeCommand;
use nebo_runtimes::acp::Agent as AcpAgent;
use nebo_runtimes::acp::client::{
    self, AUTH_REQUIRED, CLOSED, Connection, Incoming, METHOD_NOT_FOUND, NOT_FOUND, Responder,
    RpcError,
};
use nebo_runtimes::acp::protocol::{
    self, Initialized, PermissionRequest, PlanEntry, PromptResult, ToolCall as AcpToolCall,
    ToolStatus, Update,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use super::backend::{
    Agent, Ask, Backend, BoxFuture, Chat, Choice, Control, Error, Message, Role, ToolCall,
    ToolResult, Turn, TurnEvent, Usage,
};

/// How long the agent gets to answer `initialize`: `npx` may be fetching the
/// adapter on a first start.
const START_TIMEOUT: Duration = Duration::from_secs(180);

/// How many chats the link's own record keeps (agents without
/// `session/list`).
const RECORDED_CHATS: usize = 200;

/// What the backend is given.
#[derive(Debug, Clone)]
pub struct Settings {
    pub agent: AcpAgent,
    /// The agent's name on the roster ("Claude Code", or what an `Other`
    /// agent calls itself).
    pub name: String,
    /// Starts the agent speaking ACP on stdio.
    pub command: RuntimeCommand,
    /// Where its sessions work.
    pub workdir: PathBuf,
    /// Where its stderr goes.
    pub log: PathBuf,
    /// The link's record of the chats it created.
    pub chats_file: PathBuf,
}

/// A linked ACP agent.
pub struct Acp {
    shared: Arc<Shared>,
}

impl Acp {
    pub fn new(settings: Settings) -> Self {
        Self {
            shared: Arc::new(Shared {
                settings,
                live: tokio::sync::Mutex::new(None),
                opening: tokio::sync::Mutex::new(()),
            }),
        }
    }

    /// Runs `work` on its own task, so a caller that gives up (a probe's
    /// timeout, a phone that hung up) never leaves a start or a session
    /// load half done.
    async fn detached<T, F, Fut>(&self, work: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Shared>) -> Fut,
        Fut: Future<Output = Result<T, Error>> + Send + 'static,
    {
        tokio::spawn(work(self.shared.clone()))
            .await
            .unwrap_or_else(|e| Err(Error::Unavailable(e.to_string())))
    }

    fn check_agent(&self, agent: &str) -> Result<(), Error> {
        if agent == self.shared.settings.agent.key() {
            Ok(())
        } else {
            Err(Error::NotFound(format!("The agent {agent}")))
        }
    }
}

struct Shared {
    settings: Settings,
    live: tokio::sync::Mutex<Option<Arc<Live>>>,
    /// One session load at a time, so two callers never load one twice.
    opening: tokio::sync::Mutex<()>,
}

/// The running agent.
struct Live {
    conn: Arc<Connection>,
    /// Killed when the last reference goes.
    _child: tokio::process::Child,
    init: Initialized,
    state: Arc<Mutex<Sessions>>,
}

#[derive(Default)]
struct Sessions {
    map: HashMap<String, Session>,
    /// The model the agent last said it runs.
    model: Option<String>,
}

#[derive(Default)]
struct Session {
    transcript: Transcript,
    turn: Option<Sink>,
    model: Option<String>,
    title: Option<String>,
}

impl Shared {
    fn name(&self) -> &str {
        &self.settings.name
    }

    /// The running agent, started if it is not.
    async fn live(self: &Arc<Self>) -> Result<Arc<Live>, Error> {
        let mut slot = self.live.lock().await;
        if let Some(live) = slot.as_ref()
            && !live.conn.is_closed()
        {
            return Ok(live.clone());
        }
        *slot = None;
        let live = Arc::new(self.start().await?);
        *slot = Some(live.clone());
        Ok(live)
    }

    async fn start(&self) -> Result<Live, Error> {
        let settings = &self.settings;
        std::fs::create_dir_all(&settings.workdir).map_err(|e| {
            Error::Unavailable(format!(
                "could not use the folder {}: {e}",
                settings.workdir.display()
            ))
        })?;
        let stderr = log_file(&settings.log)
            .map(Stdio::from)
            .unwrap_or_else(|_| Stdio::null());
        let state: Arc<Mutex<Sessions>> = Arc::default();
        let handler_state = state.clone();
        let (child, conn) = client::spawn(
            &settings.command,
            &settings.workdir,
            stderr,
            Box::new(move |incoming, responder| handle(&handler_state, incoming, responder)),
        )
        .map_err(|e| Error::Unavailable(format!("could not start {}: {e}", self.name())))?;
        let answered = tokio::time::timeout(
            START_TIMEOUT,
            conn.request(
                "initialize",
                protocol::initialize_params("nebo-link", crate::update::VERSION),
            ),
        )
        .await;
        let init = match answered {
            Ok(Ok(result)) => Initialized::parse(&result),
            Ok(Err(e)) if e.code == CLOSED => {
                return Err(Error::Unavailable(match last_line(&settings.log) {
                    Some(line) => format!("{} stopped as it started: {line}", self.name()),
                    None => format!("{} stopped as it started", self.name()),
                }));
            }
            Ok(Err(e)) => {
                return Err(Error::Unavailable(format!(
                    "{} refused to start: {}",
                    self.name(),
                    e.message
                )));
            }
            Err(_) => return Err(Error::Unavailable(format!("{} did not start", self.name()))),
        };
        if init.protocol_version != protocol::PROTOCOL_VERSION {
            return Err(Error::Unavailable(format!(
                "{} speaks ACP version {}, and nebo-link speaks version {}. Update both.",
                self.name(),
                init.protocol_version,
                protocol::PROTOCOL_VERSION
            )));
        }
        tracing::info!(agent = settings.agent.key(), title = ?init.title, "the ACP agent started");
        Ok(Live {
            conn,
            _child: child,
            init,
            state,
        })
    }

    /// What an error answer means to the owner.
    fn refusal(&self, e: RpcError) -> Error {
        match e.code {
            AUTH_REQUIRED => Error::Failed(self.settings.agent.sign_in(self.name())),
            NOT_FOUND => Error::NotFound("That conversation".to_owned()),
            CLOSED => Error::Unavailable(format!("{} stopped", self.name())),
            _ => Error::Failed(format!("{}: {}", self.name(), e.message)),
        }
    }

    /// Makes `chat` a session of the running agent: loaded (replayed) or
    /// resumed if this process has not seen it.
    async fn open(&self, live: &Live, chat: &str) -> Result<(), Error> {
        if live.state.lock().expect("sessions").map.contains_key(chat) {
            return Ok(());
        }
        let _one = self.opening.lock().await;
        if live.state.lock().expect("sessions").map.contains_key(chat) {
            return Ok(());
        }
        let method = if live.init.load_session {
            "session/load"
        } else if live.init.resume_session {
            "session/resume"
        } else {
            return Err(Error::Failed(format!(
                "{} can't reopen a conversation after it restarts. Start a new chat.",
                self.name()
            )));
        };
        // In place first, so the replay lands in its transcript.
        live.state
            .lock()
            .expect("sessions")
            .map
            .insert(chat.to_owned(), Session::default());
        let answered = live
            .conn
            .request(
                method,
                json!({ "sessionId": chat, "cwd": self.settings.workdir, "mcpServers": [] }),
            )
            .await;
        let mut state = live.state.lock().expect("sessions");
        match answered {
            Ok(result) => {
                let model = protocol::model(&result);
                if let Some(session) = state.map.get_mut(chat) {
                    session.model = model.clone();
                }
                state.model = model.or(state.model.take());
                Ok(())
            }
            Err(e) => {
                state.map.remove(chat);
                Err(self.refusal(e))
            }
        }
    }

    async fn chats(self: Arc<Self>) -> Result<Vec<Chat>, Error> {
        let live = self.live().await?;
        if !live.init.list_sessions {
            return Ok(recorded(&self.settings.chats_file)
                .into_iter()
                .map(|r| Chat {
                    id: r.id,
                    title: r.title,
                    preview: String::new(),
                    last_active: Some(r.updated),
                    message_count: 0,
                })
                .collect());
        }
        let result = live
            .conn
            .request("session/list", json!({ "cwd": self.settings.workdir }))
            .await
            .map_err(|e| self.refusal(e))?;
        Ok(protocol::sessions(&result)
            .into_iter()
            .map(|s| Chat {
                id: s.id,
                title: s.title.unwrap_or_else(|| "New chat".to_owned()),
                preview: String::new(),
                last_active: s.updated_at.as_deref().and_then(unix_seconds),
                message_count: 0,
            })
            .collect())
    }

    async fn create_chat(self: Arc<Self>) -> Result<Chat, Error> {
        let live = self.live().await?;
        let result = live
            .conn
            .request(
                "session/new",
                json!({ "cwd": self.settings.workdir, "mcpServers": [] }),
            )
            .await
            .map_err(|e| self.refusal(e))?;
        let id = result["sessionId"]
            .as_str()
            .ok_or_else(|| {
                Error::Failed(format!(
                    "{} started a conversation without an id",
                    self.name()
                ))
            })?
            .to_owned();
        let model = protocol::model(&result);
        {
            let mut state = live.state.lock().expect("sessions");
            state.model = model.clone().or(state.model.take());
            state.map.insert(
                id.clone(),
                Session {
                    model,
                    ..Session::default()
                },
            );
        }
        record(&self.settings.chats_file, &id, None);
        Ok(Chat {
            id,
            title: "New chat".to_owned(),
            preview: String::new(),
            last_active: Some(now()),
            message_count: 0,
        })
    }

    async fn messages(self: Arc<Self>, chat: String) -> Result<Vec<Message>, Error> {
        let live = self.live().await?;
        self.open(&live, &chat).await?;
        let state = live.state.lock().expect("sessions");
        Ok(state
            .map
            .get(&chat)
            .map(|s| s.transcript.messages.clone())
            .unwrap_or_default())
    }

    async fn model(self: Arc<Self>, chat: Option<String>) -> Result<String, Error> {
        let live = self.live().await?;
        let state = live.state.lock().expect("sessions");
        let model = chat
            .and_then(|c| state.map.get(&c).and_then(|s| s.model.clone()))
            .or_else(|| state.model.clone())
            .or_else(|| live.init.title.clone())
            .unwrap_or_else(|| self.name().to_owned());
        Ok(model)
    }

    async fn turn(self: Arc<Self>, chat: String, prompt: String) -> Result<Turn, Error> {
        let live = self.live().await?;
        self.open(&live, &chat).await?;
        let (sink_tx, mut sink_rx) = mpsc::unbounded_channel::<TurnEvent>();
        {
            let mut state = live.state.lock().expect("sessions");
            let session = state
                .map
                .get_mut(&chat)
                .ok_or_else(|| Error::NotFound("That conversation".to_owned()))?;
            if session.turn.is_some() {
                return Err(Error::Failed(format!(
                    "{} is still working on this conversation.",
                    self.name()
                )));
            }
            session.transcript.user(&prompt, None, Some(now()));
            session.turn = Some(Sink::new(sink_tx.clone()));
        }
        let (events_tx, events_rx) = mpsc::channel(64);
        tokio::spawn(async move {
            while let Some(event) = sink_rx.recv().await {
                let last = matches!(
                    event,
                    TurnEvent::Completed { .. } | TurnEvent::Failed(_) | TurnEvent::Cancelled
                );
                if events_tx.send(event).await.is_err() || last {
                    return;
                }
            }
        });
        let (control_tx, mut control_rx) = mpsc::channel(8);
        tracing::info!(agent = self.settings.agent.key(), session = %chat, prompt_len = prompt.len(), "acp: prompt sent");
        tokio::spawn(async move {
            let request = live.conn.request(
                "session/prompt",
                json!({ "sessionId": chat, "prompt": [{ "type": "text", "text": prompt }] }),
            );
            tokio::pin!(request);
            let mut controls_open = true;
            let answered = loop {
                tokio::select! {
                    answered = &mut request => break answered,
                    control = control_rx.recv(), if controls_open => match control {
                        Some(control) => self.control(&live, &chat, control),
                        None => controls_open = false,
                    },
                }
            };
            let (sink, title) = {
                let mut state = live.state.lock().expect("sessions");
                match state.map.get_mut(&chat) {
                    Some(session) => (session.turn.take(), session.title.clone()),
                    None => (None, None),
                }
            };
            let responder = live.conn.responder();
            if let Some(mut sink) = sink {
                sink.flush();
                // A question nobody answered dies with its turn.
                for (_, id) in sink.asks.drain() {
                    responder.respond(&id, Ok(protocol::cancelled()));
                }
            }
            record(&self.settings.chats_file, &chat, title.as_deref());
            match &answered {
                Ok(result) => tracing::info!(session = %chat, stop_reason = %PromptResult::parse(result).stop_reason, "acp: turn ended"),
                Err(e) => tracing::info!(session = %chat, code = e.code, "acp: turn failed"),
            }
            let end = match answered {
                Ok(result) => {
                    let result = PromptResult::parse(&result);
                    match result.stop_reason.as_str() {
                        "cancelled" => TurnEvent::Cancelled,
                        "refusal" => {
                            TurnEvent::Failed(format!("{} declined to do that.", self.name()))
                        }
                        _ => TurnEvent::Completed {
                            usage: result.usage.map(|u| Usage {
                                input_tokens: u.input,
                                output_tokens: u.output,
                            }),
                        },
                    }
                }
                Err(e) if e.code == CLOSED => {
                    TurnEvent::Failed(format!("Could not connect to {}. Try again.", self.name()))
                }
                Err(e) => TurnEvent::Failed(self.refusal(e).message(self.name())),
            };
            let _ = sink_tx.send(end);
        });
        Ok(Turn {
            events: events_rx,
            control: control_tx,
        })
    }

    fn control(&self, live: &Live, chat: &str, control: Control) {
        let responder = live.conn.responder();
        let mut state = live.state.lock().expect("sessions");
        let Some(sink) = state.map.get_mut(chat).and_then(|s| s.turn.as_mut()) else {
            return;
        };
        match control {
            Control::Cancel => {
                tracing::info!(session = %chat, open_asks = sink.asks.len(), "acp: turn cancelled by the owner");
                live.conn
                    .notify("session/cancel", json!({ "sessionId": chat }));
                for (_, id) in sink.asks.drain() {
                    responder.respond(&id, Ok(protocol::cancelled()));
                }
            }
            Control::Answer { request_id, choice } => {
                let id = match request_id {
                    Some(request_id) => sink.asks.remove(&request_id),
                    None => sink
                        .asks
                        .keys()
                        .next()
                        .cloned()
                        .and_then(|k| sink.asks.remove(&k)),
                };
                match id {
                    Some(id) => {
                        tracing::info!(session = %chat, choice = %choice, "acp: permission answered");
                        responder.respond(&id, Ok(protocol::selected(&choice)))
                    }
                    None => tracing::info!("an answer for a question the agent no longer asks"),
                }
            }
        }
    }
}

/// What the agent sends: session updates into their session (and its turn),
/// permission requests into the turn as asks. The agent was offered no file
/// system and no terminal, so every other request is refused.
fn handle(state: &Mutex<Sessions>, incoming: Incoming, responder: &Responder) {
    match incoming {
        Incoming::Notification { method, params } if method == "session/update" => {
            let Some((session, update)) = protocol::update(&params) else {
                return;
            };
            let mut state = state.lock().expect("sessions");
            if let Update::Model(model) = &update {
                state.model = Some(model.clone());
            }
            if let Some(session) = state.map.get_mut(&session) {
                session.apply(update);
            }
        }
        Incoming::Request { id, method, params } if method == "session/request_permission" => {
            let Some(request) = PermissionRequest::parse(&params) else {
                tracing::warn!("acp: a permission request that does not parse; refused");
                responder.respond(
                    &id,
                    Err(RpcError::new(-32602, "invalid permission request")),
                );
                return;
            };
            let mut state = state.lock().expect("sessions");
            match state
                .map
                .get_mut(&request.session_id)
                .and_then(|s| s.turn.as_mut())
            {
                Some(sink) => {
                    tracing::info!(
                        session = %request.session_id,
                        tool = %request.tool_call.id,
                        options = request.options.len(),
                        "acp: permission requested; asking the owner"
                    );
                    sink.ask(id, request)
                }
                // Nobody is there to answer: no turn of ours is running.
                None => {
                    tracing::info!(session = %request.session_id, "acp: permission requested with no turn running; cancelled");
                    responder.respond(&id, Ok(protocol::cancelled()))
                }
            }
        }
        Incoming::Request { id, method, .. } => {
            tracing::info!(method = %method, "acp: a request the link does not offer; refused");
            responder.respond(
                &id,
                Err(RpcError::new(
                    METHOD_NOT_FOUND,
                    format!("{method} is not offered"),
                )),
            );
        }
        Incoming::Notification { .. } => {}
    }
}

impl Session {
    fn apply(&mut self, update: Update) {
        let live = self.turn.is_some().then(now);
        match update {
            Update::UserText { text, message_id } => self.transcript.user(&text, message_id, live),
            Update::AgentText { text, message_id } => {
                self.transcript.agent(&text, message_id, live);
                if let Some(sink) = &mut self.turn {
                    sink.flush();
                    sink.send(TurnEvent::Text(text));
                }
            }
            Update::Thought(text) => {
                if let Some(sink) = &self.turn {
                    sink.send(TurnEvent::Thinking(text));
                }
            }
            Update::Plan(entries) => {
                if let Some(sink) = &self.turn {
                    sink.send(TurnEvent::Thinking(plan(&entries)));
                }
            }
            Update::ToolCall(call) | Update::ToolCallUpdate(call) => {
                self.transcript.tool(&call, live);
                if let Some(sink) = &mut self.turn {
                    sink.tool(call);
                }
            }
            Update::Title(title) => self.title = Some(title),
            Update::Model(model) => self.model = Some(model),
            Update::Other => {}
        }
    }
}

/// A plan as thinking text, one line per step.
fn plan(entries: &[PlanEntry]) -> String {
    let lines: Vec<String> = entries
        .iter()
        .map(|e| {
            let mark = match e.status.as_str() {
                "completed" => "[x]",
                "in_progress" => "[~]",
                _ => "[ ]",
            };
            format!("{mark} {}", e.content)
        })
        .collect();
    format!("Plan:\n{}", lines.join("\n"))
}

/// A running turn's side of a session.
struct Sink {
    tx: mpsc::UnboundedSender<TurnEvent>,
    tools: HashMap<String, Tool>,
    /// Tool ids in the order the agent opened them.
    order: Vec<String>,
    /// Open questions: request id to the JSON-RPC id to answer.
    asks: HashMap<String, Value>,
}

struct Tool {
    call: AcpToolCall,
    started: Instant,
    announced: bool,
    finished: bool,
}

impl Sink {
    fn new(tx: mpsc::UnboundedSender<TurnEvent>) -> Self {
        Self {
            tx,
            tools: HashMap::new(),
            order: Vec::new(),
            asks: HashMap::new(),
        }
    }

    fn send(&self, event: TurnEvent) {
        let _ = self.tx.send(event);
    }

    /// A tool call or its update: its card once the agent has said what it
    /// is (running, finished, or asking), its result once it finished.
    fn tool(&mut self, call: AcpToolCall) {
        let id = call.id.clone();
        if !self.tools.contains_key(&id) {
            self.order.push(id.clone());
        }
        let tool = self.tools.entry(id.clone()).or_insert_with(|| Tool {
            call: AcpToolCall {
                id: id.clone(),
                ..AcpToolCall::default()
            },
            started: Instant::now(),
            announced: false,
            finished: false,
        });
        tool.call.merge(call);
        let status = tool.call.status;
        if matches!(status, Some(ToolStatus::InProgress)) || status.is_some_and(ToolStatus::is_done)
        {
            self.announce(&id);
        }
        let tool = self.tools.get_mut(&id).expect("tool");
        if let Some(status) = status.filter(|s| s.is_done())
            && !tool.finished
        {
            tool.finished = true;
            tracing::info!(tool = %id, ?status, "acp: tool call finished");
            let event = TurnEvent::ToolResult {
                id,
                name: tool.call.label(),
                result: tool.call.output(),
                is_error: status == ToolStatus::Failed,
                duration_ms: Some(tool.started.elapsed().as_millis() as u64),
            };
            self.send(event);
        }
    }

    fn announce(&mut self, id: &str) {
        let Some(tool) = self.tools.get_mut(id) else {
            return;
        };
        if tool.announced {
            return;
        }
        tool.announced = true;
        tracing::info!(tool = id, kind = ?tool.call.kind, "acp: tool call");
        let event = TurnEvent::ToolStart {
            id: id.to_owned(),
            name: tool.call.label(),
            input: tool.call.raw_input.clone().unwrap_or_else(|| json!({})),
        };
        self.send(event);
    }

    /// Announces every call not yet shown, before text that follows them.
    fn flush(&mut self) {
        for id in self.order.clone() {
            self.announce(&id);
        }
    }

    /// The agent stops for the owner: the call's card, then the question.
    fn ask(&mut self, rpc_id: Value, request: PermissionRequest) {
        let call_id = request.tool_call.id.clone();
        self.tool(request.tool_call);
        self.announce(&call_id);
        let call = &self.tools[&call_id].call;
        let label = call.label();
        let summary = match call.kind.as_deref() {
            Some("execute") => format!("run `{label}`"),
            Some("edit" | "delete" | "move") => format!("change {label}"),
            Some("fetch") => format!("fetch {label}"),
            _ => format!("use {label}"),
        };
        let prompt = match call.content.as_deref().filter(|c| *c != label) {
            Some(detail) => format!("{label}\n{detail}"),
            None => label.clone(),
        };
        let mut request_id = call_id.clone();
        while self.asks.contains_key(&request_id) {
            request_id.push('+');
        }
        let choices = request
            .options
            .iter()
            .map(|o| Choice {
                value: o.id.clone(),
                label: match o.kind.as_str() {
                    "allow_once" => "Allow once".to_owned(),
                    "allow_always" => "Always allow".to_owned(),
                    "reject_once" => "Deny".to_owned(),
                    "reject_always" => "Never allow".to_owned(),
                    _ => o.name.clone(),
                },
            })
            .collect();
        self.asks.insert(request_id.clone(), rpc_id);
        self.send(TurnEvent::Ask(Ask {
            request_id: Some(request_id),
            prompt,
            summary,
            choices,
        }));
    }
}

/// A session's transcript as the agent streamed or replayed it.
#[derive(Default)]
struct Transcript {
    messages: Vec<Message>,
    /// The agent's id of the message being streamed.
    current: Option<String>,
    /// Each tool call: where it is (message, call index), its merged state,
    /// and whether its result is in.
    calls: HashMap<String, (usize, usize, AcpToolCall, bool)>,
}

impl Transcript {
    fn push(&mut self, role: Role, text: &str, created_at: Option<f64>) {
        self.messages.push(Message {
            id: format!("m{}", self.messages.len()),
            role,
            text: text.to_owned(),
            created_at,
            tool_calls: Vec::new(),
            tool_result: None,
        });
    }

    /// Whether a chunk of `role` with `id` continues the last message.
    fn continues(&self, role: Role, id: &Option<String>) -> bool {
        let Some(last) = self.messages.last() else {
            return false;
        };
        last.role == role
            && last.tool_calls.is_empty()
            && (id.is_none() || self.current.is_none() || *id == self.current)
    }

    fn user(&mut self, text: &str, id: Option<String>, created_at: Option<f64>) {
        if self.continues(Role::User, &id) && id.is_some() {
            self.messages.last_mut().expect("last").text.push_str(text);
        } else {
            self.push(Role::User, text, created_at);
        }
        self.current = id;
    }

    fn agent(&mut self, text: &str, id: Option<String>, created_at: Option<f64>) {
        if self.continues(Role::Assistant, &id) {
            self.messages.last_mut().expect("last").text.push_str(text);
        } else {
            self.push(Role::Assistant, text, created_at);
        }
        self.current = id;
    }

    fn tool(&mut self, update: &AcpToolCall, created_at: Option<f64>) {
        if !self.calls.contains_key(&update.id) {
            if self
                .messages
                .last()
                .is_none_or(|m| m.role != Role::Assistant)
            {
                self.push(Role::Assistant, "", created_at);
            }
            let at = self.messages.len() - 1;
            let index = self.messages[at].tool_calls.len();
            self.messages[at].tool_calls.push(ToolCall {
                id: update.id.clone(),
                name: String::new(),
                input: json!({}),
            });
            let fresh = AcpToolCall {
                id: update.id.clone(),
                ..AcpToolCall::default()
            };
            self.calls
                .insert(update.id.clone(), (at, index, fresh, false));
        }
        let (at, index, call, resulted) = self.calls.get_mut(&update.id).expect("call");
        call.merge(update.clone());
        let entry = &mut self.messages[*at].tool_calls[*index];
        entry.name = call.label();
        entry.input = call.raw_input.clone().unwrap_or_else(|| json!({}));
        if let Some(status) = call.status.filter(|s| s.is_done())
            && !*resulted
        {
            *resulted = true;
            let result = ToolResult {
                tool_call_id: call.id.clone(),
                content: call.output(),
                is_error: status == ToolStatus::Failed,
            };
            self.push(Role::Tool, "", created_at);
            self.messages.last_mut().expect("tool row").tool_result = Some(result);
        }
    }
}

impl Backend for Acp {
    fn ready(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            self.detached(|shared| async move { shared.live().await.map(|_| ()) })
                .await
                .map_err(|e| match e {
                    Error::Unavailable(why) | Error::Failed(why) | Error::NotFound(why) => why,
                })
        })
    }

    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>> {
        let settings = &self.shared.settings;
        let agent = Agent {
            id: settings.agent.key().to_owned(),
            name: settings.name.clone(),
            description: format!("Works in {}", settings.workdir.display()),
            is_default: true,
        };
        Box::pin(async move { Ok(vec![agent]) })
    }

    fn chats<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Vec<Chat>, Error>> {
        Box::pin(async move {
            self.check_agent(agent)?;
            self.detached(Shared::chats).await
        })
    }

    fn create_chat<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Chat, Error>> {
        Box::pin(async move {
            self.check_agent(agent)?;
            self.detached(Shared::create_chat).await
        })
    }

    fn messages<'a>(
        &'a self,
        agent: &'a str,
        chat: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Message>, Error>> {
        Box::pin(async move {
            self.check_agent(agent)?;
            let chat = chat.to_owned();
            self.detached(|shared| shared.messages(chat)).await
        })
    }

    fn model<'a>(
        &'a self,
        agent: &'a str,
        chat: Option<&'a str>,
    ) -> BoxFuture<'a, Result<String, Error>> {
        Box::pin(async move {
            self.check_agent(agent)?;
            let chat = chat.map(str::to_owned);
            self.detached(|shared| shared.model(chat)).await
        })
    }

    fn turn<'a>(
        &'a self,
        agent: &'a str,
        chat: &'a str,
        prompt: String,
    ) -> BoxFuture<'a, Result<Turn, Error>> {
        Box::pin(async move {
            self.check_agent(agent)?;
            let chat = chat.to_owned();
            self.detached(|shared| shared.turn(chat, prompt)).await
        })
    }
}

/// One chat the link created, for agents that can't list their sessions.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Recorded {
    id: String,
    title: String,
    /// Unix seconds.
    updated: f64,
}

fn recorded(file: &std::path::Path) -> Vec<Recorded> {
    std::fs::read_to_string(file)
        .ok()
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default()
}

/// Notes activity on `id` (and its title, once the agent gave one), most
/// recent first.
fn record(file: &std::path::Path, id: &str, title: Option<&str>) {
    let mut all = recorded(file);
    let previous = all.iter().position(|r| r.id == id).map(|i| all.remove(i));
    let title = title
        .map(str::to_owned)
        .or(previous.map(|r| r.title))
        .unwrap_or_else(|| "New chat".to_owned());
    all.insert(
        0,
        Recorded {
            id: id.to_owned(),
            title,
            updated: now(),
        },
    );
    all.truncate(RECORDED_CHATS);
    if let Err(e) = crate::state::write_json(file, &all) {
        tracing::info!(error = %e, "could not record the chat");
    }
}

fn log_file(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

fn last_line(path: &std::path::Path) -> Option<String> {
    let text = std::fs::read_to_string(path).ok()?;
    text.lines()
        .rev()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .map(str::to_owned)
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

/// Unix seconds of an RFC 3339 time (`2026-09-26T14:23:13.025Z`,
/// `…+02:00`).
fn unix_seconds(text: &str) -> Option<f64> {
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

    #[test]
    fn rfc3339_times() {
        assert_eq!(unix_seconds("1970-01-01T00:00:00Z"), Some(0.0));
        assert_eq!(
            unix_seconds("2026-09-26T14:23:13.025Z"),
            Some(1_790_432_593.025)
        );
        assert_eq!(
            unix_seconds("2026-09-26T16:23:13+02:00"),
            Some(1_790_432_593.0)
        );
        assert_eq!(unix_seconds("yesterday"), None);
    }

    #[test]
    fn a_replayed_session_reads_as_the_phone_shows_it() {
        let mut t = Transcript::default();
        t.user("Run it", Some("u1".into()), None);
        let call = |status: Option<ToolStatus>, title: &str| AcpToolCall {
            id: "t1".into(),
            title: Some(title.into()),
            status,
            raw_input: Some(json!({ "command": "echo hi" })),
            content: status.map(|_| "hi".to_owned()),
            ..AcpToolCall::default()
        };
        t.tool(&call(None, "Terminal"), None);
        t.tool(&call(Some(ToolStatus::Completed), "echo hi"), None);
        t.agent("The command ", Some("a1".into()), None);
        t.agent("printed hi.", Some("a1".into()), None);
        t.user("Thanks", Some("u2".into()), None);
        let roles: Vec<Role> = t.messages.iter().map(|m| m.role).collect();
        assert_eq!(
            roles,
            [
                Role::User,
                Role::Assistant,
                Role::Tool,
                Role::Assistant,
                Role::User
            ]
        );
        assert_eq!(t.messages[1].tool_calls[0].name, "echo hi");
        assert_eq!(t.messages[2].tool_result.as_ref().unwrap().content, "hi");
        assert_eq!(t.messages[3].text, "The command printed hi.");
    }

    #[test]
    fn plans_read_as_steps() {
        let entries = vec![
            PlanEntry {
                content: "Read".into(),
                status: "completed".into(),
            },
            PlanEntry {
                content: "Fix".into(),
                status: "in_progress".into(),
            },
        ];
        assert_eq!(plan(&entries), "Plan:\n[x] Read\n[~] Fix");
    }
}
