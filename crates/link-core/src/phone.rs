//! The Nebo phone contract, served over the [`Host`]: the slice of Nebo's
//! own bot API the phone speaks (roster, chats, messages, the chat socket's
//! frames, asks, notifications), in the shapes `mobile lib/api/bot_api.dart`,
//! `bot_ws.dart` and `bot_chat_controller.dart` parse. Open Agent Link
//! replaces it (`spec/oal-0.1.md` Appendix B); it stays while Nebo's clients
//! move over. No transport here: [`Contract::rest`] answers a REST path,
//! [`Contract::inbound`] takes a socket frame, and [`Contract::subscribe`]
//! gives the frames every socket gets.
//!
//! One Nebo chat is one session; the contract's session key is Nebo's own
//! `agent:<agentId>:thread:<chatId>`. The first hosted agent is `assistant`,
//! the id the phone finds the primary employee by.
//!
//! A permission request is an ask card in the chat (`ask_request` /
//! `ask_response`), a row in the contract's notifications, and an item in
//! the owner's [`Inbox`], resolved in all three places whichever one
//! answers it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::PRIMARY;
use crate::backend::{Agent, Backend, Error, Message, Permission, Role};
use crate::host::{Event, Host, Update};
use crate::model::{PendingChange, PendingRequest, StopReason, TurnState, TurnUpdate};
use crate::roster::Roster;

/// How long the readiness probe may take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// How many `chat` frames' `message_id`s are remembered, so a frame the
/// phone re-sends after a reconnect runs once.
const SEEN_MESSAGES: usize = 1000;

/// The owner's inbox, outside the computer: where a question waiting for the
/// owner is also told, and taken back once answered.
pub trait Inbox: Send + Sync {
    /// Tells the inbox; must return at once (a slow inbox never holds a
    /// turn).
    fn post(&self, item: InboxItem);
}

/// What the inbox is told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxItem {
    /// A question is waiting for the owner.
    Approval {
        /// `approval:<request id>`.
        id: String,
        title: String,
        body: String,
        agent_id: String,
        chat_id: String,
    },
    /// It was answered, or its turn ended.
    Resolved { id: String },
}

/// One event for every connected socket: `{type, data}` on the wire.
#[derive(Debug, Clone)]
pub struct Outbound {
    pub kind: String,
    pub data: Value,
}

/// A refused REST request: the status and the message the owner reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub status: u16,
    pub message: String,
}

impl Refusal {
    fn new(status: u16, message: String) -> Self {
        Self { status, message }
    }
}

/// The phone contract over one host.
pub struct Contract {
    runtime_key: &'static str,
    runtime_name: &'static str,
    host: Arc<Host>,
    inbox: Option<Arc<dyn Inbox>>,
    hub: broadcast::Sender<Outbound>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// The chats a phone message is running on, by chat id, with the name
    /// of the agent it was sent to.
    running: HashMap<String, String>,
    /// Messages sent to a chat while a turn was running, oldest first, each
    /// with the permission it was sent under.
    queued: HashMap<String, VecDeque<(String, Option<Permission>)>>,
    notices: Vec<Notice>,
    seen: HashSet<String>,
    seen_order: VecDeque<String>,
    /// The id a caller named an agent by on a chat, by (agent, chat), where
    /// it named it by an id saved before the ids took their one form: that
    /// chat's frames carry the id it knows.
    named: HashMap<(String, String), String>,
}

/// One row of the contract's notifications: a pending approval.
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

/// The `ask_request` payload's question and card, as
/// `bot_chat_controller.dart` `showAsk` reads them.
fn card(request: &PendingRequest) -> Value {
    json!({
        "request_id": request.id,
        "prompt": request.words.question,
        "widgets": [{
            "type": "options",
            "multiSelect": false,
            "options": request.words.labels,
        }],
    })
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
    /// `runtime_key` and `runtime_name` are the host's runtime as the phone
    /// shows it; `inbox` is `None` where there is no inbox to tell.
    pub fn new(
        runtime_key: &'static str,
        runtime_name: &'static str,
        host: Arc<Host>,
        inbox: Option<Arc<dyn Inbox>>,
    ) -> Arc<Self> {
        let (hub, _) = broadcast::channel(256);
        let mut events = host.subscribe();
        let contract = Arc::new(Self {
            runtime_key,
            runtime_name,
            host,
            inbox,
            hub,
            state: Mutex::new(State::default()),
        });
        let weak = Arc::downgrade(&contract);
        tokio::spawn(async move {
            loop {
                let event = match events.recv().await {
                    Ok(event) => event,
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        tracing::warn!(missed, "contract: fell behind the host's events");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                };
                let Some(contract) = weak.upgrade() else {
                    return;
                };
                contract.on_event(event);
            }
        });
        contract
    }

    pub fn host(&self) -> &Arc<Host> {
        &self.host
    }

    pub fn runtime_key(&self) -> &'static str {
        self.runtime_key
    }

    fn roster(&self) -> &Roster {
        self.host.roster()
    }

    /// Whether the runtime can serve chats now; the error says why not.
    pub async fn ready(&self) -> Result<(), String> {
        match tokio::time::timeout(PROBE_TIMEOUT, self.host.ready()).await {
            Ok(result) => result,
            Err(_) => Err(format!("{} did not answer the link", self.runtime_name)),
        }
    }

    /// Answers one REST request on `/api/v1/…`.
    pub async fn rest(&self, method: &str, path: &str) -> Result<Value, Refusal> {
        let segments: Vec<String> = path.trim_start_matches('/').split('/').map(percent_decode).collect();
        let segments: Vec<&str> = segments.iter().map(String::as_str).collect();
        match (method, segments.as_slice()) {
            ("GET", ["api", "v1", "agents"]) => self.agents().await,
            ("GET", ["api", "v1", "agents", id]) => self.agent(id).await,
            ("GET", ["api", "v1", "agents", id, "chats"]) => self.chats(id).await,
            ("POST", ["api", "v1", "agents", id, "chats"]) => self.create_chat(id).await,
            ("GET", ["api", "v1", "chats", id]) => self.chat_model(id).await,
            ("PUT", ["api", "v1", "chats", _]) => Err(Refusal::new(
                400,
                format!("The model is chosen in {}.", self.runtime_name),
            )),
            ("GET", ["api", "v1", "chats", id, "messages"]) => self.messages(id).await,
            ("GET", ["api", "v1", "models"]) => self.models().await,
            ("GET", ["api", "v1", "notifications"]) => Ok(self.notifications()),
            ("GET", ["api", "v1", "notifications", "unread-count"]) => Ok(self.unread_count()),
            ("PUT", ["api", "v1", "notifications", "read-all"]) => Ok(self.mark_read(None)),
            ("PUT", ["api", "v1", "notifications", id, "read"]) => Ok(self.mark_read(Some(id))),
            ("DELETE", ["api", "v1", "notifications", id]) => Ok(self.delete_notification(id)),
            _ => Err(Refusal::new(404, "not on a linked bot".to_owned())),
        }
    }

    /// The hosted agents as employee rows, the first as `assistant`.
    async fn agents(&self) -> Result<Value, Refusal> {
        let agents = self.roster_agents().await?;
        let rows: Vec<Value> = agents.iter().map(|a| employee(a, &a.id)).collect();
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
        let chats = self.host.sessions(&agent.id).await.map_err(|e| self.refuse(e))?;
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
        let chat = self.host.new_session(&agent.id).await.map_err(|e| self.refuse(e))?;
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
            match self.host.model(&agent.id, Some(chat_id)).await {
                Ok(model) => return Ok(json!({ "id": chat_id, "model": model })),
                Err(Error::NotFound(_)) => continue,
                Err(e) => return Err(self.refuse(e)),
            }
        }
        Err(Refusal::new(404, format!("No chat {chat_id} on this bot.")))
    }

    async fn models(&self) -> Result<Value, Refusal> {
        let agents = self.roster_agents().await?;
        let primary = agents.iter().find(|a| a.is_default).or(agents.first());
        let model = match primary {
            Some(agent) => self.host.model(&agent.id, None).await.map_err(|e| self.refuse(e))?,
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
            match self.host.transcript(&agent.id, chat_id).await {
                Ok(found) => {
                    messages = Some(found);
                    break;
                }
                Err(Error::NotFound(_)) => continue,
                Err(e) => return Err(self.refuse(e)),
            }
        }
        let Some(messages) = messages else {
            return Err(Refusal::new(404, format!("No chat {chat_id} on this bot.")));
        };
        let (active_run, pending_ask) = match self.host.turn(chat_id) {
            Some(turn) => (
                json!({ "turnId": turn.turn_id, "agentId": turn.agent }),
                self.host
                    .pending()
                    .iter()
                    .find(|p| p.session_id == chat_id)
                    .map(card)
                    .unwrap_or(Value::Null),
            ),
            None => (Value::Null, Value::Null),
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
        for notice in state.notices.iter_mut().filter(|n| id.is_none_or(|id| n.id == id)) {
            notice.read = true;
        }
        json!({ "status": "ok" })
    }

    fn delete_notification(&self, id: &str) -> Value {
        let mut state = self.state.lock().expect("contract state");
        state.notices.retain(|n| n.id != id);
        json!({ "status": "ok" })
    }

    async fn roster_agents(&self) -> Result<Vec<Agent>, Refusal> {
        self.roster().agents().await.map_err(|e| self.refuse(e))
    }

    /// The hosted agent a contract id names; an id saved before the ids
    /// took their one form still names its agent.
    async fn resolve(&self, id: &str) -> Result<Agent, Refusal> {
        let canonical = match self.roster().canonical(id).await {
            Ok(canonical) => canonical,
            Err(Error::NotFound(_)) => return Err(Refusal::new(404, format!("No agent {id} on this bot."))),
            Err(e) => return Err(self.refuse(e)),
        };
        self.roster_agents()
            .await?
            .into_iter()
            .find(|a| a.id == canonical)
            .ok_or_else(|| Refusal::new(404, format!("No agent {id} on this bot.")))
    }

    /// The id the phone knows `agent` by on `chat`.
    fn named(&self, agent: &str, chat: &str) -> String {
        let state = self.state.lock().expect("contract state");
        state
            .named
            .get(&(agent.to_owned(), chat.to_owned()))
            .cloned()
            .unwrap_or_else(|| agent.to_owned())
    }

    /// A turn's frame fields, with the agent as the phone knows it.
    fn payload(&self, agent: &str, chat: &str, turn_id: &str) -> Value {
        payload(&self.named(agent, chat), chat, turn_id)
    }

    /// The agents a chat may belong to, the one running it first: the REST
    /// paths name a chat without its agent, and each agent may keep its own
    /// session store.
    async fn owners_of(&self, chat_id: &str) -> Result<Vec<Agent>, Refusal> {
        let running = self.host.turn(chat_id).map(|t| t.agent);
        let mut agents = self.roster_agents().await?;
        agents.sort_by_key(|a| (running.as_deref() != Some(a.id.as_str()), a.id != PRIMARY));
        Ok(agents)
    }

    fn refuse(&self, error: Error) -> Refusal {
        let status = match error {
            Error::Unavailable(ref why) => {
                tracing::info!(runtime = self.runtime_name, why, "contract: the runtime is not answering");
                502
            }
            Error::NotFound(_) => 404,
            Error::Failed(_) => 502,
        };
        Refusal::new(status, error.message(self.runtime_name))
    }

    // -- The socket -----------------------------------------------------------

    /// Every socket's frames.
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
            // The transport already authenticated the peer (the link's proxy
            // admits only tunnel-stamped requests; Nebo is its own caller),
            // so the token is not checked again.
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
                self.cancel(data);
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

    /// A `chat` frame: `{prompt, agent_id, session_id?, permission_mode?}`.
    /// Without a session the chat is created first and announced with
    /// `chat_created`. `permission_mode` is Nebo's permission mode for the
    /// employee, which a runtime with modes of its own runs the turn in.
    async fn chat(self: Arc<Self>, data: Value) {
        let agent_id = data["agent_id"].as_str().unwrap_or(PRIMARY).to_owned();
        let prompt = data["prompt"].as_str().unwrap_or("").trim().to_owned();
        let permission = data["permission_mode"].as_str().and_then(Permission::parse);
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
            None => match self.host.new_session(&agent.id).await {
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
        if agent.id != agent_id {
            self.state
                .lock()
                .expect("contract state")
                .named
                .insert((agent.id.clone(), chat_id.clone()), agent_id.clone());
        }
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
            if state.running.contains_key(&chat_id) {
                state
                    .queued
                    .entry(chat_id.clone())
                    .or_default()
                    .push_back((prompt.clone(), permission));
                true
            } else {
                state.running.insert(chat_id.clone(), agent.name.clone());
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
        self.start(&agent.id, &chat_id, prompt, permission);
    }

    /// Starts a turn on a chat whose slot in `running` is taken.
    fn start(self: &Arc<Self>, agent_id: &str, chat_id: &str, prompt: String, permission: Option<Permission>) {
        if let Err(refused) = self.host.prompt(agent_id, chat_id, prompt, permission, None) {
            {
                let mut state = self.state.lock().expect("contract state");
                state.running.remove(chat_id);
                state.queued.remove(chat_id);
            }
            let named = self.named(agent_id, chat_id);
            self.broadcast(
                "chat_error",
                json!({ "agent_id": named, "session_id": session_key(&named, chat_id), "error": refused.message }),
            );
        }
    }

    /// One event from the host, as the phone's frames.
    fn on_event(self: &Arc<Self>, event: Event) {
        match event {
            Event::Update(update) => {
                let data = self.payload(&update.agent, &update.session_id, &update.turn_id);
                match update.update {
                    Update::Text(content) => {
                        self.broadcast("chat_stream", with(data, json!({ "content": content, "done": false })));
                    }
                    Update::Thinking(text) => {
                        self.broadcast("thinking", with(data, json!({ "text": text, "content": text })));
                    }
                    Update::ToolStart { id, name, input } => {
                        self.broadcast(
                            "tool_start",
                            with(data, json!({ "tool_id": id, "tool": name, "label": name, "input": input })),
                        );
                    }
                    Update::ToolResult {
                        id,
                        name,
                        result,
                        is_error,
                        duration_ms,
                    } => {
                        self.broadcast(
                            "tool_result",
                            with(
                                data,
                                json!({
                                    "tool_id": id,
                                    "tool_name": name,
                                    "result": result,
                                    "is_error": is_error,
                                    "outcome": name,
                                    "duration_ms": duration_ms,
                                }),
                            ),
                        );
                    }
                }
            }
            Event::Pending(update) => match update.change {
                PendingChange::Added => self.asked(&update.request),
                PendingChange::Resolved => self.resolved(&update.request.id),
            },
            Event::Turn(turn) if turn.state == TurnState::Ended => {
                self.ended(&turn);
                self.next(&turn);
            }
            Event::Turn(_) | Event::Agent(_) => {}
        }
    }

    /// A turn's end as the phone reads it.
    fn ended(&self, turn: &TurnUpdate) {
        let data = self.payload(&turn.agent, &turn.session_id, &turn.turn_id);
        if let Some(error) = &turn.error {
            self.broadcast("chat_error", with(data, json!({ "error": error.message })));
        } else if turn.stop_reason == Some(StopReason::Cancelled) {
            self.broadcast("chat_cancelled", data);
        } else {
            if let Some(usage) = &turn.usage {
                self.broadcast(
                    "usage",
                    with(
                        data.clone(),
                        json!({ "input_tokens": usage.all_input(), "output_tokens": usage.output_tokens }),
                    ),
                );
            }
            self.broadcast(
                "chat_complete",
                with(data, json!({ "stop_reason": "end_turn", "message_id": turn.turn_id })),
            );
        }
    }

    /// Runs the next message queued on the ended turn's chat, or frees it.
    fn next(self: &Arc<Self>, turn: &TurnUpdate) {
        let chat_id = &turn.session_id;
        let next = {
            let mut state = self.state.lock().expect("contract state");
            let next = state.queued.get_mut(chat_id).and_then(VecDeque::pop_front);
            if next.is_none() {
                state.queued.remove(chat_id);
                state.running.remove(chat_id);
            }
            next
        };
        if let Some((prompt, permission)) = next {
            self.start(&turn.agent, chat_id, prompt, permission);
        }
    }

    /// A question waiting for the owner: its card on the chat, a notice, and
    /// the inbox told.
    fn asked(&self, request: &PendingRequest) {
        let agent_name = {
            let state = self.state.lock().expect("contract state");
            state
                .running
                .get(&request.session_id)
                .cloned()
                .unwrap_or_else(|| request.agent.clone())
        };
        let notice = Notice {
            id: format!("approval:{}", request.id),
            title: format!("{agent_name} asks to {}", request.words.summary),
            body: request.words.question.clone(),
            created_at: unix_now(),
            read: false,
            agent_id: request.agent.clone(),
            chat_id: request.session_id.clone(),
        };
        self.state.lock().expect("contract state").notices.push(notice.clone());
        if let Some(inbox) = &self.inbox {
            inbox.post(InboxItem::Approval {
                id: notice.id,
                title: notice.title,
                body: notice.body,
                agent_id: notice.agent_id,
                chat_id: notice.chat_id,
            });
        }
        let turn_id = request.turn_id.as_deref().unwrap_or("");
        self.broadcast(
            "ask_request",
            with(self.payload(&request.agent, &request.session_id, turn_id), card(request)),
        );
    }

    /// A question answered or dropped: its notice and inbox item go.
    fn resolved(&self, request_id: &str) {
        let id = format!("approval:{request_id}");
        self.state.lock().expect("contract state").notices.retain(|n| n.id != id);
        if let Some(inbox) = &self.inbox {
            inbox.post(InboxItem::Resolved { id });
        }
    }

    /// An `ask_response {request_id, value}`: the value is the label the
    /// phone showed (or the option's id), and the host takes it from there.
    fn answer(&self, request_id: &str, value: &str) {
        let option = self.host.pending().into_iter().find(|p| p.id == request_id).and_then(|p| {
            let i = p
                .options
                .iter()
                .zip(&p.words.labels)
                .position(|(o, label)| label == value || o.option_id == value)?;
            Some(p.options[i].option_id.clone())
        });
        let answered = option.and_then(|option| self.host.answer(request_id, &option, None).ok());
        if answered.is_none() {
            tracing::info!(request_id, "ask_response for a question nobody is waiting on");
        }
    }

    /// A `cancel {session_id | agent_id}`: stops the chat's turn, or every
    /// turn of the agent. With nothing running the phone is told so.
    fn cancel(self: &Arc<Self>, data: Value) {
        let session_id = data["session_id"].as_str().map(str::to_owned);
        let chat_id = session_id.as_deref().and_then(parse_session_key).map(|(_, chat)| chat.to_owned());
        let agent_id = data["agent_id"].as_str().map(str::to_owned);
        match (&chat_id, &agent_id) {
            // An agent by an id saved before the ids took their one form is
            // stopped by its id now.
            (None, Some(agent)) => {
                let contract = self.clone();
                let agent = agent.clone();
                tokio::spawn(async move {
                    let canonical = contract.roster().canonical(&agent).await.unwrap_or_else(|_| agent.clone());
                    contract.stop(Some(&canonical), None, Some(&agent), None);
                });
            }
            _ => self.stop(agent_id.as_deref(), chat_id.as_deref(), agent_id.as_deref(), session_id.as_deref()),
        }
    }

    /// Stops the turns [`Host::cancel`] names; with none, tells the phone.
    fn stop(&self, agent: Option<&str>, chat: Option<&str>, named: Option<&str>, session_id: Option<&str>) {
        if self.host.cancel(agent, chat) == 0 {
            self.broadcast(
                "chat_cancelled",
                json!({ "agent_id": named, "session_id": session_id.unwrap_or("default") }),
            );
        }
    }
}

/// A turn's frame fields: `{agent_id, session_id, turn_id}`.
fn payload(agent_id: &str, chat_id: &str, turn_id: &str) -> Value {
    json!({ "agent_id": agent_id, "session_id": session_key(agent_id, chat_id), "turn_id": turn_id })
}

/// `data` with `fields` added.
fn with(mut data: Value, fields: Value) -> Value {
    if let (Value::Object(data), Value::Object(fields)) = (&mut data, fields) {
        data.extend(fields);
    }
    data
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
        assert_eq!(session_key("assistant", "api_1"), "agent:assistant:thread:api_1");
        assert_eq!(parse_session_key("agent:coder:thread:api_1_x"), Some(("coder", "api_1_x")));
        assert_eq!(parse_session_key("agent:coder"), None);
        assert_eq!(parse_session_key("agent::thread:x"), None);
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
        assert_eq!(relative_time(Some(1_000_000.0 - 3.0 * 86400.0), 1_000_000), "3d ago");
    }

    #[test]
    fn transcript_rows_carry_tool_work_the_way_the_phone_reads_it() {
        use crate::backend::{ToolCall, ToolResult};
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
        let results: Value = serde_json::from_str(rows[2]["toolResults"].as_str().unwrap()).unwrap();
        assert_eq!(results[0]["tool_call_id"], "call_1");
        assert_eq!(results[0]["content"], "boom");
    }
}
