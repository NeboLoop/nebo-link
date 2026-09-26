//! The chat contract: the slice of Nebo's own bot API the phone speaks
//! (roster, chats, messages, the chat WebSocket, asks, notifications),
//! served for the linked runtime on the link's loopback listener beside
//! `/_link/`. The proxy hands it `/health`, `/api/v1/*` and `/ws`; a
//! [`Backend`] translates each runtime once, and everything the phone reads
//! is rendered here in the shapes `mobile lib/api/bot_api.dart`,
//! `bot_ws.dart` and `bot_chat_controller.dart` parse.
//!
//! One Nebo chat is one runtime session; the contract's session key is
//! Nebo's own `agent:<agentId>:thread:<chatId>` with the runtime's session
//! id as the chat id. The runtime's default agent is `assistant`, the id the
//! phone finds the primary employee by.
//!
//! An approval the runtime stops for is an ask card in the chat
//! (`ask_request` / `ask_response`), a row in the link's notifications, and
//! an item in the owner's hub inbox (`POST /api/v1/bots/self/inbox`),
//! resolved in all three places whichever one answers it.

pub mod backend;
pub mod hermes;
pub mod openclaw;
mod ws;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use nebo_comm::api::NeboAIApi;
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc, watch};

use crate::proxy::{Body, json};
use backend::{Agent, Ask, Backend, Choice, Control, Error, Message, Role, TurnEvent};

/// The contract's id for the runtime's default agent.
pub const PRIMARY: &str = "assistant";

/// How long the readiness probe behind `/health` and CONNECT may take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// How many `chat` frames' `message_id`s are remembered, so a frame the
/// phone re-sends after a reconnect runs once.
const SEEN_MESSAGES: usize = 1000;

/// The paths the contract serves; everything else on the listener is the
/// runtime's UI.
pub fn routes(path: &str) -> bool {
    path == "/health" || path == "/ws" || path == "/api/v1" || path.starts_with("/api/v1/")
}

/// The owner's hub inbox, reached with the bot's token.
pub struct Inbox {
    api: Arc<NeboAIApi>,
    /// The current bot token; the hub rotates it on every connect.
    token: watch::Receiver<String>,
}

impl Inbox {
    pub fn new(api_url: &str, bot_id: &str, token: watch::Receiver<String>) -> Self {
        let api = NeboAIApi::new(
            api_url.to_owned(),
            bot_id.to_owned(),
            token.borrow().clone(),
        );
        Self {
            api: Arc::new(api),
            token,
        }
    }
}

/// One event for every connected socket: `{type, data}` on the wire.
#[derive(Debug, Clone)]
pub struct Outbound {
    pub kind: String,
    pub data: Value,
}

/// The contract server for one linked runtime.
pub struct Contract {
    runtime_key: &'static str,
    runtime_name: &'static str,
    bot_id: String,
    backend: Arc<dyn Backend>,
    inbox: Option<Inbox>,
    hub: broadcast::Sender<Outbound>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// The turn running on each chat, by the runtime's session id.
    runs: HashMap<String, ActiveRun>,
    /// Messages sent to a chat while a turn was running, oldest first.
    queued: HashMap<String, VecDeque<String>>,
    notices: Vec<Notice>,
    seen: HashSet<String>,
    seen_order: VecDeque<String>,
}

struct ActiveRun {
    /// The contract's agent id.
    agent_id: String,
    agent_name: String,
    turn_id: String,
    /// `None` while the backend is still starting the turn.
    control: Option<mpsc::Sender<Control>>,
    asks: Vec<PendingAsk>,
}

#[derive(Debug, Clone)]
struct PendingAsk {
    /// The contract's request id (the runtime's when it gave one).
    id: String,
    /// The runtime's request id, to answer with.
    backend_id: Option<String>,
    prompt: String,
    choices: Vec<Choice>,
}

impl PendingAsk {
    /// The `ask_request` payload's question and card, as
    /// `bot_chat_controller.dart` `showAsk` reads them.
    fn card(&self) -> Value {
        json!({
            "request_id": self.id,
            "prompt": self.prompt,
            "widgets": [{
                "type": "options",
                "multiSelect": false,
                "options": self.choices.iter().map(|c| c.label.clone()).collect::<Vec<_>>(),
            }],
        })
    }
}

/// One row of the link's notifications: a pending approval.
#[derive(Debug, Clone)]
struct Notice {
    id: String,
    title: String,
    body: String,
    created_at: u64,
    read: bool,
    agent_id: String,
    chat_id: String,
}

impl Notice {
    fn row(&self) -> Value {
        json!({
            "id": self.id,
            "type": "approval",
            "title": self.title,
            "body": self.body,
            "createdAt": self.created_at,
            "readAt": self.read.then_some(self.created_at),
            "agentId": self.agent_id,
            "actionUrl": format!("/{}/threads/{}", self.agent_id, self.chat_id),
        })
    }
}

/// Nebo's session key for a chat: `agent:<agentId>:thread:<chatId>`.
pub fn session_key(agent_id: &str, chat_id: &str) -> String {
    format!("agent:{agent_id}:thread:{chat_id}")
}

/// The agent and chat a session key names.
pub fn parse_session_key(key: &str) -> Option<(&str, &str)> {
    let rest = key.strip_prefix("agent:")?;
    let (agent, chat) = rest.split_once(":thread:")?;
    (!agent.is_empty() && !chat.is_empty()).then_some((agent, chat))
}

impl Contract {
    /// `inbox` is `None` only where there is no hub to tell (tests).
    pub fn new(
        runtime_key: &'static str,
        runtime_name: &'static str,
        bot_id: &str,
        backend: Arc<dyn Backend>,
        inbox: Option<Inbox>,
    ) -> Arc<Self> {
        let (hub, _) = broadcast::channel(256);
        Arc::new(Self {
            runtime_key,
            runtime_name,
            bot_id: bot_id.to_owned(),
            backend,
            inbox,
            hub,
            state: Mutex::new(State::default()),
        })
    }

    /// Whether the runtime can serve chats now; the error says why not.
    pub async fn ready(&self) -> Result<(), String> {
        match tokio::time::timeout(PROBE_TIMEOUT, self.backend.ready()).await {
            Ok(result) => result,
            Err(_) => Err(format!("{} did not answer the link", self.runtime_name)),
        }
    }

    /// Serves one request on a contract path ([`routes`]).
    pub async fn handle(self: &Arc<Self>, mut req: Request<Incoming>) -> Response<Body> {
        let path = req.uri().path().to_owned();
        if path == "/ws" {
            return ws::upgrade(self.clone(), &mut req);
        }
        let segments: Vec<String> = path
            .trim_start_matches('/')
            .split('/')
            .map(percent_decode)
            .collect();
        let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
        let method = req.method().clone();
        let result = match (&method, segments.as_slice()) {
            (&Method::GET, ["health"]) => Ok(self.health().await),
            (&Method::GET, ["api", "v1", "agents"]) => self.agents().await,
            (&Method::GET, ["api", "v1", "agents", id]) => self.agent(id).await,
            (&Method::GET, ["api", "v1", "agents", id, "chats"]) => self.chats(id).await,
            (&Method::POST, ["api", "v1", "agents", id, "chats"]) => self.create_chat(id).await,
            (&Method::GET, ["api", "v1", "chats", id]) => self.chat_model(id).await,
            (&Method::PUT, ["api", "v1", "chats", _]) => Err(Refusal::plain(
                StatusCode::BAD_REQUEST,
                format!("The model is chosen in {}.", self.runtime_name),
            )),
            (&Method::GET, ["api", "v1", "chats", id, "messages"]) => self.messages(id).await,
            (&Method::GET, ["api", "v1", "models"]) => self.models().await,
            (&Method::GET, ["api", "v1", "notifications"]) => Ok(self.notifications()),
            (&Method::GET, ["api", "v1", "notifications", "unread-count"]) => {
                Ok(self.unread_count())
            }
            (&Method::PUT, ["api", "v1", "notifications", "read-all"]) => Ok(self.mark_read(None)),
            (&Method::PUT, ["api", "v1", "notifications", id, "read"]) => {
                Ok(self.mark_read(Some(id)))
            }
            (&Method::DELETE, ["api", "v1", "notifications", id]) => {
                Ok(self.delete_notification(id))
            }
            _ => Err(Refusal::plain(
                StatusCode::NOT_FOUND,
                "not on a linked bot".to_owned(),
            )),
        };
        // Bodies are read for nothing: every write the contract takes is a
        // frame on the socket or an id in the path.
        let _ = Limited::new(req.into_body(), 64 << 10).collect().await;
        match result {
            Ok(value) => json(StatusCode::OK, &value),
            Err(refusal) => json(refusal.status, &json!({ "error": refusal.message })),
        }
    }

    async fn health(&self) -> Value {
        json!({
            "version": crate::update::VERSION,
            "runtime": self.runtime_key,
            "chat": self.ready().await.is_ok(),
        })
    }

    /// The runtime's agents as employee rows, the default one first as
    /// `assistant`.
    async fn agents(&self) -> Result<Value, Refusal> {
        let agents = self.roster().await?;
        let rows: Vec<Value> = agents
            .iter()
            .map(|a| employee(a, &contract_id(a)))
            .collect();
        Ok(json!({ "agents": rows, "primaryChristened": true }))
    }

    async fn agent(&self, id: &str) -> Result<Value, Refusal> {
        let agent = self.resolve(id).await?;
        let mut row = employee(&agent, id);
        row["nameLocked"] = json!(true);
        Ok(json!({ "agent": row }))
    }

    async fn chats(&self, id: &str) -> Result<Value, Refusal> {
        let agent = self.resolve(id).await?;
        let chats = self
            .backend
            .chats(&agent.id)
            .await
            .map_err(|e| self.refuse(e))?;
        let now = unix_now();
        let rows: Vec<Value> = chats
            .iter()
            .map(|c| {
                json!({
                    "id": c.id,
                    "title": if c.title.is_empty() { "New chat" } else { &c.title },
                    "preview": c.preview,
                    "relativeTime": relative_time(c.last_active, now),
                    "messageCount": c.message_count,
                })
            })
            .collect();
        Ok(json!({ "chats": rows }))
    }

    async fn create_chat(&self, id: &str) -> Result<Value, Refusal> {
        let agent = self.resolve(id).await?;
        let chat = self
            .backend
            .create_chat(&agent.id)
            .await
            .map_err(|e| self.refuse(e))?;
        Ok(json!({
            "chat": {
                "id": chat.id,
                "title": if chat.title.is_empty() { "New chat" } else { &chat.title },
                "preview": "",
                "relativeTime": "just now",
                "messageCount": 0,
            }
        }))
    }

    async fn chat_model(&self, chat_id: &str) -> Result<Value, Refusal> {
        for agent in self.owners_of(chat_id).await? {
            match self.backend.model(&agent.id, Some(chat_id)).await {
                Ok(model) => return Ok(json!({ "id": chat_id, "model": model })),
                Err(Error::NotFound(_)) => continue,
                Err(e) => return Err(self.refuse(e)),
            }
        }
        Err(Refusal::plain(
            StatusCode::NOT_FOUND,
            format!("No chat {chat_id} on this bot."),
        ))
    }

    async fn models(&self) -> Result<Value, Refusal> {
        let agents = self.roster().await?;
        let primary = agents.iter().find(|a| a.is_default).or(agents.first());
        let model = match primary {
            Some(agent) => self
                .backend
                .model(&agent.id, None)
                .await
                .map_err(|e| self.refuse(e))?,
            None => String::new(),
        };
        Ok(json!({
            "models": {
                self.runtime_key: [{ "id": model, "displayName": model, "isActive": true }],
            }
        }))
    }

    /// The chat's transcript in the phone's row shape, with the turn still
    /// running on it and the question it is parked on.
    async fn messages(&self, chat_id: &str) -> Result<Value, Refusal> {
        let mut messages = None;
        for agent in self.owners_of(chat_id).await? {
            match self.backend.messages(&agent.id, chat_id).await {
                Ok(found) => {
                    messages = Some(found);
                    break;
                }
                Err(Error::NotFound(_)) => continue,
                Err(e) => return Err(self.refuse(e)),
            }
        }
        let Some(messages) = messages else {
            return Err(Refusal::plain(
                StatusCode::NOT_FOUND,
                format!("No chat {chat_id} on this bot."),
            ));
        };
        let (active_run, pending_ask) = {
            let state = self.state.lock().expect("contract state");
            match state.runs.get(chat_id) {
                Some(run) => (
                    json!({ "turnId": run.turn_id, "agentId": run.agent_id }),
                    run.asks
                        .first()
                        .map(PendingAsk::card)
                        .unwrap_or(Value::Null),
                ),
                None => (Value::Null, Value::Null),
            }
        };
        Ok(json!({
            "messages": rows(&messages),
            "hasMore": false,
            "activeRun": active_run,
            "pendingAsk": pending_ask,
        }))
    }

    fn notifications(&self) -> Value {
        let state = self.state.lock().expect("contract state");
        let mut rows: Vec<Value> = state.notices.iter().map(Notice::row).collect();
        rows.reverse();
        json!({ "notifications": rows })
    }

    fn unread_count(&self) -> Value {
        let state = self.state.lock().expect("contract state");
        json!({ "count": state.notices.iter().filter(|n| !n.read).count() })
    }

    /// Marks one notice read, or every notice without an id.
    fn mark_read(&self, id: Option<&str>) -> Value {
        let mut state = self.state.lock().expect("contract state");
        for notice in state
            .notices
            .iter_mut()
            .filter(|n| id.is_none_or(|id| n.id == id))
        {
            notice.read = true;
        }
        json!({ "status": "ok" })
    }

    fn delete_notification(&self, id: &str) -> Value {
        let mut state = self.state.lock().expect("contract state");
        state.notices.retain(|n| n.id != id);
        json!({ "status": "ok" })
    }

    async fn roster(&self) -> Result<Vec<Agent>, Refusal> {
        self.backend.agents().await.map_err(|e| self.refuse(e))
    }

    /// The runtime agent behind a contract id.
    async fn resolve(&self, id: &str) -> Result<Agent, Refusal> {
        self.roster()
            .await?
            .into_iter()
            .find(|a| contract_id(a) == id)
            .ok_or_else(|| {
                Refusal::plain(StatusCode::NOT_FOUND, format!("No agent {id} on this bot."))
            })
    }

    /// The agents a chat may belong to, the one running it first: the REST
    /// paths name a chat without its agent, and each agent may keep its own
    /// session store.
    async fn owners_of(&self, chat_id: &str) -> Result<Vec<Agent>, Refusal> {
        let running = {
            let state = self.state.lock().expect("contract state");
            state.runs.get(chat_id).map(|run| run.agent_id.clone())
        };
        let mut agents = self.roster().await?;
        agents.sort_by_key(|a| {
            let id = contract_id(a);
            (running.as_deref() != Some(id.as_str()), id != PRIMARY)
        });
        Ok(agents)
    }

    fn refuse(&self, error: Error) -> Refusal {
        let status = match error {
            Error::Unavailable(ref why) => {
                tracing::info!(
                    runtime = self.runtime_name,
                    why,
                    "contract: the runtime is not answering"
                );
                StatusCode::BAD_GATEWAY
            }
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Failed(_) => StatusCode::BAD_GATEWAY,
        };
        Refusal::plain(status, error.message(self.runtime_name))
    }

    // -- The socket -----------------------------------------------------------

    /// Every socket's events.
    pub fn subscribe(&self) -> broadcast::Receiver<Outbound> {
        self.hub.subscribe()
    }

    fn broadcast(&self, kind: &str, data: Value) {
        let _ = self.hub.send(Outbound {
            kind: kind.to_owned(),
            data,
        });
    }

    /// One frame from the phone. Returns the direct answer, when the frame
    /// has one; everything else is broadcast.
    pub fn inbound(self: &Arc<Self>, frame: &Value) -> Option<Value> {
        let data = frame.get("data").cloned().unwrap_or(Value::Null);
        match frame.get("type").and_then(Value::as_str).unwrap_or("") {
            // The tunnel already authenticated the request (the proxy admits
            // only stamped ones), so the token is not checked again.
            "auth" | "connect" => Some(json!({ "type": "auth_ok" })),
            "ping" => Some(json!({ "type": "pong" })),
            "chat" => {
                if let Some(message_id) = frame.get("message_id").and_then(Value::as_str)
                    && !self.first_time(message_id)
                {
                    return None;
                }
                let contract = self.clone();
                tokio::spawn(async move { contract.chat(data).await });
                None
            }
            "cancel" => {
                self.cancel(&data);
                None
            }
            "ask_response" => {
                let request_id = data["request_id"].as_str().unwrap_or("");
                let value = data["value"].as_str().unwrap_or("");
                self.answer(request_id, value);
                None
            }
            _ => None,
        }
    }

    fn first_time(&self, message_id: &str) -> bool {
        let mut state = self.state.lock().expect("contract state");
        if !state.seen.insert(message_id.to_owned()) {
            return false;
        }
        state.seen_order.push_back(message_id.to_owned());
        if state.seen_order.len() > SEEN_MESSAGES
            && let Some(old) = state.seen_order.pop_front()
        {
            state.seen.remove(&old);
        }
        true
    }

    /// A `chat` frame: `{prompt, agent_id, session_id?}`. Without a session
    /// the chat is created first and announced with `chat_created`.
    async fn chat(self: Arc<Self>, data: Value) {
        let agent_id = data["agent_id"].as_str().unwrap_or(PRIMARY).to_owned();
        let prompt = data["prompt"].as_str().unwrap_or("").trim().to_owned();
        let agent = match self.resolve(&agent_id).await {
            Ok(agent) => agent,
            Err(refusal) => {
                self.broadcast(
                    "chat_error",
                    json!({ "agent_id": agent_id, "session_id": data["session_id"], "error": refusal.message }),
                );
                return;
            }
        };
        let chat_id = match data["session_id"].as_str() {
            Some(key) => match parse_session_key(key) {
                Some((_, chat_id)) => chat_id.to_owned(),
                None => {
                    self.broadcast(
                        "chat_error",
                        json!({ "agent_id": agent_id, "session_id": key, "error": "That conversation is not on this bot." }),
                    );
                    return;
                }
            },
            None => match self.backend.create_chat(&agent.id).await {
                Ok(chat) => {
                    self.broadcast(
                        "chat_created",
                        json!({ "agent_id": agent_id, "session_id": session_key(&agent_id, &chat.id) }),
                    );
                    chat.id
                }
                Err(e) => {
                    self.broadcast(
                        "chat_error",
                        json!({ "agent_id": agent_id, "error": e.message(self.runtime_name) }),
                    );
                    return;
                }
            },
        };
        let session_id = session_key(&agent_id, &chat_id);
        if prompt.is_empty() {
            self.broadcast(
                "chat_error",
                json!({ "agent_id": agent_id, "session_id": session_id, "error": "Nothing to send." }),
            );
            return;
        }
        let queued = {
            let mut state = self.state.lock().expect("contract state");
            if state.runs.contains_key(&chat_id) {
                state
                    .queued
                    .entry(chat_id.clone())
                    .or_default()
                    .push_back(prompt.clone());
                true
            } else {
                state.runs.insert(
                    chat_id.clone(),
                    ActiveRun {
                        agent_id: agent_id.clone(),
                        agent_name: agent.name.clone(),
                        turn_id: String::new(),
                        control: None,
                        asks: Vec::new(),
                    },
                );
                false
            }
        };
        if queued {
            // The phone marks the message pending until the running turn ends.
            self.broadcast(
                "chat_error",
                json!({ "agent_id": agent_id, "session_id": session_id, "stop_reason": "queued_into_running_turn" }),
            );
            return;
        }
        self.turn(agent, agent_id, chat_id, prompt).await;
    }

    /// Runs one turn on a chat whose slot in `runs` is taken, then the next
    /// queued message, until the chat is idle.
    async fn turn(
        self: Arc<Self>,
        agent: Agent,
        agent_id: String,
        chat_id: String,
        mut prompt: String,
    ) {
        loop {
            let turn_id = uuid::Uuid::new_v4().to_string();
            let session_id = session_key(&agent_id, &chat_id);
            let payload = |fields: Value| {
                let mut data =
                    json!({ "agent_id": agent_id, "session_id": session_id, "turn_id": turn_id });
                if let (Value::Object(data), Value::Object(fields)) = (&mut data, fields) {
                    data.extend(fields);
                }
                data
            };
            match self.backend.turn(&agent.id, &chat_id, prompt).await {
                Ok(turn) => {
                    {
                        let mut state = self.state.lock().expect("contract state");
                        if let Some(run) = state.runs.get_mut(&chat_id) {
                            run.turn_id = turn_id.clone();
                            run.control = Some(turn.control);
                        }
                    }
                    self.relay(turn.events, &chat_id, &turn_id, &payload).await;
                }
                Err(e) => self.broadcast(
                    "chat_error",
                    payload(json!({ "error": e.message(self.runtime_name) })),
                ),
            }
            let next = {
                let mut state = self.state.lock().expect("contract state");
                let next = state.queued.get_mut(&chat_id).and_then(VecDeque::pop_front);
                if next.is_none() {
                    state.queued.remove(&chat_id);
                    if let Some(run) = state.runs.remove(&chat_id) {
                        drop(state);
                        self.resolve_asks(run.asks.iter().map(|a| a.id.clone()).collect());
                    }
                }
                next
            };
            match next {
                Some(text) => prompt = text,
                None => return,
            }
        }
    }

    /// Broadcasts a turn's events until it ends.
    async fn relay(
        &self,
        mut events: mpsc::Receiver<TurnEvent>,
        chat_id: &str,
        turn_id: &str,
        payload: &(dyn Fn(Value) -> Value + Sync),
    ) {
        while let Some(event) = events.recv().await {
            match event {
                TurnEvent::Text(content) => {
                    self.broadcast(
                        "chat_stream",
                        payload(json!({ "content": content, "done": false })),
                    );
                }
                TurnEvent::Thinking(text) => {
                    self.broadcast(
                        "thinking",
                        payload(json!({ "text": text, "content": text })),
                    );
                }
                TurnEvent::ToolStart { id, name, input } => {
                    self.broadcast(
                        "tool_start",
                        payload(
                            json!({ "tool_id": id, "tool": name, "label": name, "input": input }),
                        ),
                    );
                }
                TurnEvent::ToolResult {
                    id,
                    name,
                    result,
                    is_error,
                    duration_ms,
                } => {
                    self.broadcast(
                        "tool_result",
                        payload(json!({
                            "tool_id": id,
                            "tool_name": name,
                            "result": result,
                            "is_error": is_error,
                            "outcome": name,
                            "duration_ms": duration_ms,
                        })),
                    );
                }
                TurnEvent::Ask(ask) => {
                    if let Some(card) = self.ask(chat_id, ask) {
                        self.broadcast("ask_request", payload(card));
                    }
                }
                TurnEvent::AskAnswered { request_id } => {
                    let ids = {
                        let mut state = self.state.lock().expect("contract state");
                        match state.runs.get_mut(chat_id) {
                            Some(run) => take_asks(&mut run.asks, request_id.as_deref()),
                            None => Vec::new(),
                        }
                    };
                    self.resolve_asks(ids);
                }
                TurnEvent::Completed { usage } => {
                    if let Some(usage) = usage {
                        self.broadcast(
                            "usage",
                            payload(json!({ "input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens })),
                        );
                    }
                    self.broadcast(
                        "chat_complete",
                        payload(json!({ "stop_reason": "end_turn", "message_id": turn_id })),
                    );
                    return;
                }
                TurnEvent::Failed(error) => {
                    self.broadcast("chat_error", payload(json!({ "error": error })));
                    return;
                }
                TurnEvent::Cancelled => {
                    self.broadcast("chat_cancelled", payload(Value::Null));
                    return;
                }
            }
        }
        // The backend stopped without saying how.
        self.broadcast(
            "chat_error",
            payload(json!({ "error": format!("Could not connect to {}. Try again.", self.runtime_name) })),
        );
    }

    /// Records an ask on its run and in the notifications, tells the hub,
    /// and returns the card to broadcast.
    fn ask(&self, chat_id: &str, ask: Ask) -> Option<Value> {
        let (card, notice) = {
            let mut state = self.state.lock().expect("contract state");
            let run = state.runs.get_mut(chat_id)?;
            let id = ask
                .request_id
                .clone()
                .unwrap_or_else(|| format!("{}-ask-{}", run.turn_id, run.asks.len() + 1));
            let pending = PendingAsk {
                id: id.clone(),
                backend_id: ask.request_id,
                prompt: ask.prompt,
                choices: ask.choices,
            };
            let card = pending.card();
            let notice = Notice {
                id: format!("approval:{id}"),
                title: format!("{} asks to {}", run.agent_name, ask.summary),
                body: pending.prompt.clone(),
                created_at: unix_now(),
                read: false,
                agent_id: run.agent_id.clone(),
                chat_id: chat_id.to_owned(),
            };
            run.asks.push(pending);
            state.notices.push(notice.clone());
            (card, notice)
        };
        self.post_inbox(json!({
            "id": notice.id,
            "type": "approval",
            "title": notice.title,
            "body": notice.body,
            "link": format!("/t/{}/{}/threads/{}", self.bot_id, notice.agent_id, notice.chat_id),
            "agentId": notice.agent_id,
            "chatId": notice.chat_id,
        }));
        Some(card)
    }

    /// An `ask_response {request_id, value}`: the answer goes to the run
    /// holding the request, and the ask is resolved everywhere.
    fn answer(&self, request_id: &str, value: &str) {
        let answered = {
            let mut state = self.state.lock().expect("contract state");
            state.runs.values_mut().find_map(|run| {
                let i = run.asks.iter().position(|a| a.id == request_id)?;
                let ask = &run.asks[i];
                let choice = ask
                    .choices
                    .iter()
                    .find(|c| c.label == value || c.value == value)
                    .map(|c| c.value.clone())?;
                let control = run.control.clone()?;
                let ask = run.asks.remove(i);
                Some((control, ask, choice))
            })
        };
        let Some((control, ask, choice)) = answered else {
            tracing::info!(
                request_id,
                "ask_response for a question nobody is waiting on"
            );
            return;
        };
        let request_id = ask.backend_id.clone();
        tokio::spawn(async move {
            let _ = control.send(Control::Answer { request_id, choice }).await;
        });
        self.resolve_asks(vec![ask.id]);
    }

    /// Clears the notices of answered asks and tells the hub.
    fn resolve_asks(&self, ids: Vec<String>) {
        if ids.is_empty() {
            return;
        }
        {
            let mut state = self.state.lock().expect("contract state");
            state
                .notices
                .retain(|n| !ids.iter().any(|id| n.id == format!("approval:{id}")));
        }
        for id in ids {
            self.post_inbox(json!({ "id": format!("approval:{id}"), "resolved": true }));
        }
    }

    /// A `cancel {session_id | agent_id}`: stops the chat's turn, or every
    /// turn of the agent. With nothing running the phone is told so.
    fn cancel(&self, data: &Value) {
        let session_id = data["session_id"].as_str();
        let chat_id = session_id.and_then(parse_session_key).map(|(_, chat)| chat);
        let agent_id = data["agent_id"].as_str();
        let controls: Vec<mpsc::Sender<Control>> = {
            let state = self.state.lock().expect("contract state");
            state
                .runs
                .iter()
                .filter(|(id, run)| match (chat_id, agent_id) {
                    (Some(chat), _) => id.as_str() == chat,
                    (None, Some(agent)) => run.agent_id == agent,
                    (None, None) => true,
                })
                .filter_map(|(_, run)| run.control.clone())
                .collect()
        };
        if controls.is_empty() {
            self.broadcast(
                "chat_cancelled",
                json!({ "agent_id": agent_id, "session_id": session_id.unwrap_or("default") }),
            );
        }
        for control in controls {
            tokio::spawn(async move {
                let _ = control.send(Control::Cancel).await;
            });
        }
    }

    /// Tells the hub, off the socket's path: a hub that is slow or down
    /// never holds a turn.
    fn post_inbox(&self, item: Value) {
        let Some(inbox) = &self.inbox else {
            return;
        };
        let api = inbox.api.clone();
        let token = inbox.token.borrow().clone();
        tokio::spawn(async move {
            api.set_token(token);
            if let Err(e) = api.push_inbox_item(&item).await {
                tracing::info!(error = %e, "the hub did not take the inbox item");
            }
        });
    }
}

/// The asks `request_id` names: that one, or the oldest when the runtime
/// gave no id.
fn take_asks(asks: &mut Vec<PendingAsk>, request_id: Option<&str>) -> Vec<String> {
    let i = match request_id {
        Some(id) => asks
            .iter()
            .position(|a| a.backend_id.as_deref() == Some(id)),
        None => (!asks.is_empty()).then_some(0),
    };
    i.map(|i| vec![asks.remove(i).id]).unwrap_or_default()
}

/// An error answer.
struct Refusal {
    status: StatusCode,
    message: String,
}

impl Refusal {
    fn plain(status: StatusCode, message: String) -> Self {
        Self { status, message }
    }
}

/// The contract's id for a runtime agent.
fn contract_id(agent: &Agent) -> String {
    if agent.is_default {
        PRIMARY.to_owned()
    } else {
        agent.id.clone()
    }
}

/// An employee row as `Employee.fromJson` reads it.
fn employee(agent: &Agent, id: &str) -> Value {
    json!({
        "id": id,
        "name": agent.name,
        "displayName": agent.name,
        "description": agent.description,
        "color": Value::Null,
        "handle": Value::Null,
        "isEnabled": true,
        "editable": false,
        "isApp": false,
        "isolated": true,
        "soul": "",
        "rules": "",
    })
}

/// A transcript in the rows `BotApi.parseChatHistory` reads: tool calls on
/// the assistant row (`toolCalls` as a JSON string with the ids,
/// `metadata.toolCalls` and `metadata.contentBlocks` for the order), results
/// on tool rows (`toolResults` as a JSON string keyed by call id).
fn rows(messages: &[Message]) -> Vec<Value> {
    let failed: HashSet<&str> = messages
        .iter()
        .filter_map(|m| m.tool_result.as_ref())
        .filter(|r| r.is_error)
        .map(|r| r.tool_call_id.as_str())
        .collect();
    messages
        .iter()
        .map(|m| {
            let created_at = m.created_at.map(|t| t as i64);
            match m.role {
                Role::User => json!({ "id": m.id, "role": "user", "content": m.text, "createdAt": created_at }),
                Role::Assistant => {
                    let mut row = json!({ "id": m.id, "role": "assistant", "content": m.text, "createdAt": created_at });
                    if !m.tool_calls.is_empty() {
                        let calls: Vec<Value> = m
                            .tool_calls
                            .iter()
                            .map(|c| json!({ "id": c.id, "name": c.name, "input": c.input }))
                            .collect();
                        let meta_calls: Vec<Value> = m
                            .tool_calls
                            .iter()
                            .map(|c| {
                                json!({
                                    "name": c.name,
                                    "input": c.input,
                                    "status": if failed.contains(c.id.as_str()) { "error" } else { "success" },
                                })
                            })
                            .collect();
                        let mut blocks = Vec::new();
                        if !m.text.is_empty() {
                            blocks.push(json!({ "type": "text", "text": m.text }));
                        }
                        blocks.extend((0..m.tool_calls.len()).map(|i| json!({ "type": "tool", "toolCallIndex": i })));
                        row["toolCalls"] = json!(Value::Array(calls).to_string());
                        row["metadata"] = json!({ "toolCalls": meta_calls, "contentBlocks": blocks });
                    }
                    row
                }
                Role::Tool => {
                    let results: Vec<Value> = m
                        .tool_result
                        .iter()
                        .map(|r| json!({ "tool_call_id": r.tool_call_id, "content": r.content }))
                        .collect();
                    json!({
                        "id": m.id,
                        "role": "tool",
                        "content": "",
                        "createdAt": created_at,
                        "toolResults": Value::Array(results).to_string(),
                    })
                }
            }
        })
        .collect()
}

/// "just now", "5m ago", "3h ago", "2d ago".
fn relative_time(last_active: Option<f64>, now: u64) -> String {
    let Some(then) = last_active else {
        return String::new();
    };
    let ago = now.saturating_sub(then as u64);
    match ago {
        0..60 => "just now".to_owned(),
        60..3600 => format!("{}m ago", ago / 60),
        3600..86400 => format!("{}h ago", ago / 3600),
        _ => format!("{}d ago", ago / 86400),
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// One path segment with its `%XX` escapes decoded.
fn percent_decode(segment: &str) -> String {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3])
            && let Ok(byte) = u8::from_str_radix(hex, 16)
        {
            out.push(byte);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_keys() {
        assert_eq!(
            session_key("assistant", "api_1"),
            "agent:assistant:thread:api_1"
        );
        assert_eq!(
            parse_session_key("agent:coder:thread:api_1_x"),
            Some(("coder", "api_1_x"))
        );
        assert_eq!(parse_session_key("agent:coder"), None);
        assert_eq!(parse_session_key("agent::thread:x"), None);
    }

    #[test]
    fn contract_paths() {
        assert!(routes("/health"));
        assert!(routes("/ws"));
        assert!(routes("/api/v1/agents"));
        assert!(!routes("/api/ws"));
        assert!(!routes("/"));
        assert!(!routes("/healthz"));
    }

    #[test]
    fn segments_are_decoded() {
        assert_eq!(percent_decode("api%5F1%20x"), "api_1 x");
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("%2"), "%2");
    }

    #[test]
    fn relative_times() {
        assert_eq!(relative_time(None, 1000), "");
        assert_eq!(relative_time(Some(990.0), 1000), "just now");
        assert_eq!(relative_time(Some(1000.0 - 300.0), 1000), "5m ago");
        assert_eq!(relative_time(Some(100_000.0 - 7200.0), 100_000), "2h ago");
        assert_eq!(
            relative_time(Some(1_000_000.0 - 3.0 * 86400.0), 1_000_000),
            "3d ago"
        );
    }

    #[test]
    fn transcript_rows_carry_tool_work_the_way_the_phone_reads_it() {
        use backend::{ToolCall, ToolResult};
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
                text: "Listing.".into(),
                created_at: Some(11.0),
                tool_calls: vec![ToolCall {
                    id: "call_1".into(),
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
                    tool_call_id: "call_1".into(),
                    content: "boom".into(),
                    is_error: true,
                }),
            },
        ];
        let rows = rows(&messages);
        assert_eq!(rows[0]["role"], "user");
        assert_eq!(rows[0]["createdAt"], 10);
        let calls: Value = serde_json::from_str(rows[1]["toolCalls"].as_str().unwrap()).unwrap();
        assert_eq!(calls[0]["id"], "call_1");
        assert_eq!(rows[1]["metadata"]["toolCalls"][0]["status"], "error");
        assert_eq!(rows[1]["metadata"]["contentBlocks"][0]["type"], "text");
        assert_eq!(rows[1]["metadata"]["contentBlocks"][1]["toolCallIndex"], 0);
        let results: Value =
            serde_json::from_str(rows[2]["toolResults"].as_str().unwrap()).unwrap();
        assert_eq!(results[0]["tool_call_id"], "call_1");
        assert_eq!(results[0]["content"], "boom");
    }

    #[test]
    fn asks_are_taken_by_id_or_oldest_first() {
        let ask = |id: &str, backend: Option<&str>| PendingAsk {
            id: id.into(),
            backend_id: backend.map(str::to_owned),
            prompt: String::new(),
            choices: vec![],
        };
        let mut asks = vec![ask("a", None), ask("b", Some("b"))];
        assert_eq!(take_asks(&mut asks, Some("b")), vec!["b".to_owned()]);
        assert_eq!(take_asks(&mut asks, Some("zz")), Vec::<String>::new());
        assert_eq!(take_asks(&mut asks, None), vec!["a".to_owned()]);
        assert_eq!(take_asks(&mut asks, None), Vec::<String>::new());
    }
}
