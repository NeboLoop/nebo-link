//! The Nebo phone contract, served over the [`Host`]: the slice of Nebo's
//! own bot API the phone speaks (roster, chats, messages, the chat socket's
//! frames, asks, notifications), in the shapes `mobile lib/api/bot_api.dart`,
//! `bot_ws.dart` and `bot_chat_controller.dart` parse. Open Agent Link
//! replaces it (`spec/oal-0.1.md` Appendix B); it stays while Nebo's clients
//! move over. It is one client of the host among any others: a turn another
//! client started shows on the phone too. No transport here:
//! [`Contract::rest`] answers a REST path, [`Contract::inbound`] takes a
//! socket frame, and [`Contract::subscribe`] gives the frames every socket
//! gets.
//!
//! One Nebo chat is one session; the contract's session key is Nebo's own
//! `agent:<agentId>:thread:<chatId>`. A chat's id is its session's, prefixed
//! `<member>~` on every member but the first ([`chat_id`]), because the REST
//! paths name a chat without its agent. The first hosted agent is
//! `assistant`, the id the phone finds the primary employee by. The
//! employee's permission mode on a `chat` frame becomes the agent's own
//! mode ([`crate::turn::mode_for`]).
//!
//! A permission request is an ask card in the chat (`ask_request` /
//! `ask_response`), a row in the contract's notifications, and an item in
//! the owner's [`Inbox`], resolved in all three places whichever one
//! answers it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use nebo_runtimes::acp::protocol::{self, ToolCall as AcpToolCall, ToolStatus};
use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::PRIMARY;
use crate::adapter::{Message, Role, ToolCall, ToolResult};
use crate::backend::{Agent, Error};
use crate::host::{ClientId, Event, Host, Open, Recorded, Stamped};
use crate::model::{ErrorObject, Outcome, PendingChange, PendingRequest, StopReason, TurnState, TurnUpdate, Words, code};
use crate::roster::Roster;
use crate::turn::{self, Permission, ToolEvent, Tools, mode_for};

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
    /// The contract as one of the host's clients.
    me: ClientId,
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
    /// The id a caller named an agent by on a chat, by (agent, session),
    /// where it named it by an id saved before the ids took their one form:
    /// that chat's frames carry the id it knows.
    named: HashMap<(String, String), String>,
    /// Each agent's member, as the roster last located it.
    members: HashMap<String, String>,
    /// Each agent's name, as the roster last listed it.
    names: HashMap<String, String>,
    /// Each session's tool cards, by (agent, session).
    tools: HashMap<(String, String), Tools>,
    /// Each pending request's words, as the phone shows them.
    words: HashMap<String, Words>,
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
fn card(request: &PendingRequest, words: &Words) -> Value {
    json!({
        "request_id": request.id,
        "prompt": words.question,
        "widgets": [{
            "type": "options",
            "multiSelect": false,
            "options": words.labels,
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

/// What separates a member's id from its session's id in a chat id.
const SEP: char = '~';

/// The chat id the phone knows a session of `member`'s by: the session's
/// own id on the first member, `<member>~<session>` on any other.
pub fn chat_id(member: &str, session: &str) -> String {
    if member == PRIMARY {
        session.to_owned()
    } else {
        format!("{member}{SEP}{session}")
    }
}

/// The session of `member`'s a chat id names, when the chat is that
/// member's; `members` is every member's id.
pub fn session_of<'a>(member: &str, chat: &'a str, members: &[&str]) -> Option<&'a str> {
    if member != PRIMARY {
        return chat.strip_prefix(member)?.strip_prefix(SEP);
    }
    let another = chat
        .split_once(SEP)
        .is_some_and(|(prefix, _)| members.iter().any(|m| *m != PRIMARY && *m == prefix));
    (!another).then_some(chat)
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
            me: host.client(),
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
        let (agent, _) = self.resolve(id).await?;
        let mut row = employee(&agent, id);
        row["nameLocked"] = json!(true);
        Ok(json!({ "agent": row }))
    }

    async fn chats(&self, id: &str) -> Result<Value, Refusal> {
        let (agent, member) = self.resolve(id).await?;
        let listed = self
            .host
            .list_sessions(&agent.id, json!({}))
            .await
            .map_err(|e| self.refuse_acp(&agent.id, e))?;
        let now = unix_now();
        let rows: Vec<Value> = listed["sessions"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| {
                let session = s["sessionId"].as_str()?;
                let title = s["title"].as_str().filter(|t| !t.is_empty()).unwrap_or("New chat");
                let last_active = s["updatedAt"].as_str().and_then(crate::model::unix_seconds);
                Some(json!({
                    "id": chat_id(&member, session),
                    "title": title,
                    "preview": s["_meta"]["preview"].as_str().unwrap_or(""),
                    "relativeTime": relative_time(last_active, now),
                    "messageCount": s["_meta"]["messageCount"].as_u64().unwrap_or(0),
                }))
            })
            .collect();
        Ok(json!({ "chats": rows }))
    }

    async fn create_chat(&self, id: &str) -> Result<Value, Refusal> {
        let (agent, member) = self.resolve(id).await?;
        let created = self
            .host
            .new_session(&agent.id, new_session_params(&agent))
            .await
            .map_err(|e| self.refuse_acp(&agent.id, e))?;
        let session = created["sessionId"].as_str().unwrap_or("");
        Ok(json!({
            "chat": {
                "id": chat_id(&member, session),
                "title": "New chat",
                "preview": "",
                "relativeTime": "just now",
                "messageCount": 0,
            }
        }))
    }

    async fn chat_model(&self, chat: &str) -> Result<Value, Refusal> {
        for (agent, session) in self.owners_of(chat).await? {
            match self.host.model(&agent.id, Some(&session)).await {
                Ok(model) => return Ok(json!({ "id": chat, "model": model })),
                Err(Error::NotFound(_)) => continue,
                Err(e) => return Err(self.refuse(e)),
            }
        }
        Err(Refusal::new(404, format!("No chat {chat} on this bot.")))
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
    async fn messages(&self, chat: &str) -> Result<Value, Refusal> {
        for (agent, session) in self.owners_of(chat).await? {
            let opened = match self.open(&agent, &session, Open::Load).await {
                Ok(opened) => opened,
                Err(e) if e.code == code::NOT_FOUND || e.code == code::METHOD_NOT_FOUND => continue,
                Err(e) => return Err(self.refuse_acp(&agent.id, e)),
            };
            let (active_run, pending_ask) = match self.host.turn(&agent.id, &session) {
                Some(turn) => (
                    json!({ "turnId": turn.turn_id, "agentId": turn.agent }),
                    opened
                        .pending
                        .first()
                        .map(|p| card(p, &self.words_of(p)))
                        .unwrap_or(Value::Null),
                ),
                None => (Value::Null, Value::Null),
            };
            return Ok(json!({
                "messages": rows(&transcript(&opened.record)),
                "hasMore": false,
                "activeRun": active_run,
                "pendingAsk": pending_ask,
            }));
        }
        Err(Refusal::new(404, format!("No chat {chat} on this bot.")))
    }

    /// The session opened in the host, with `session/load` where the agent
    /// replays sessions, else `session/resume`.
    async fn open(&self, agent: &Agent, session: &str, how: Open) -> Result<crate::host::Opened, ErrorObject> {
        let caps = &agent.capabilities;
        let how = match how {
            Open::Load if !caps["loadSession"].as_bool().unwrap_or(false) && caps["sessionCapabilities"]["resume"].is_object() => Open::Resume,
            how => how,
        };
        if !self.host.is_open(&agent.id, session)
            && !caps["loadSession"].as_bool().unwrap_or(false)
            && !caps["sessionCapabilities"]["resume"].is_object()
        {
            return Err(ErrorObject::new(
                code::INTERNAL,
                format!("{} can't reopen a conversation after it restarts. Start a new chat.", agent.name),
            ));
        }
        let mut params = new_session_params(agent);
        params["sessionId"] = json!(session);
        self.host.open_session(&agent.id, how, params).await
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

    /// The hosted agent a contract id names (an id saved before the ids took
    /// their one form still names its agent), and its member's id.
    async fn resolve(&self, id: &str) -> Result<(Agent, String), Refusal> {
        let canonical = match self.roster().canonical(id).await {
            Ok(canonical) => canonical,
            Err(Error::NotFound(_)) => return Err(Refusal::new(404, format!("No agent {id} on this bot."))),
            Err(e) => return Err(self.refuse(e)),
        };
        let member = match self.roster().locate(&canonical).await {
            Ok((member, _)) => member.id,
            Err(e) => return Err(self.refuse(e)),
        };
        let agent = self
            .roster_agents()
            .await?
            .into_iter()
            .find(|a| a.id == canonical)
            .ok_or_else(|| Refusal::new(404, format!("No agent {id} on this bot.")))?;
        let mut state = self.state.lock().expect("contract state");
        state.members.insert(canonical.clone(), member.clone());
        state.names.insert(canonical, agent.name.clone());
        drop(state);
        Ok((agent, member))
    }

    /// What the owner calls `agent`: its name as last resolved, else its
    /// member's.
    fn name_of(&self, agent: &str) -> String {
        let state = self.state.lock().expect("contract state");
        state.names.get(agent).cloned().unwrap_or_else(|| self.host.label(agent))
    }

    /// The member hosting `agent`: as the roster last located it, else by
    /// the id's form (a member's own id, `<member>-<agent>`, else one of the
    /// first member's agents).
    fn member_of(&self, agent: &str) -> String {
        if let Some(member) = self.state.lock().expect("contract state").members.get(agent) {
            return member.clone();
        }
        let members = self.roster().members();
        members
            .iter()
            .find(|m| m.id == agent)
            .or_else(|| {
                members
                    .iter()
                    .filter(|m| m.id != PRIMARY && agent.strip_prefix(m.id.as_str()).is_some_and(|r| r.starts_with('-')))
                    .max_by_key(|m| m.id.len())
            })
            .map(|m| m.id.clone())
            .unwrap_or_else(|| PRIMARY.to_owned())
    }

    /// The agents a chat may belong to, with its session on each, the one
    /// running it first: the REST paths name a chat without its agent, and
    /// each agent may keep its own session store.
    async fn owners_of(&self, chat: &str) -> Result<Vec<(Agent, String)>, Refusal> {
        let members = self.roster().members();
        let ids: Vec<&str> = members.iter().map(|m| m.id.as_str()).collect();
        let running: Vec<String> = self.host.turns().into_iter().map(|t| t.agent).collect();
        let mut owners = Vec::new();
        for agent in self.roster_agents().await? {
            let member = self.member_of(&agent.id);
            if let Some(session) = session_of(&member, chat, &ids) {
                let session = session.to_owned();
                owners.push((agent, session));
            }
        }
        owners.sort_by_key(|(a, s)| {
            let runs = running.contains(&a.id) && self.host.turn(&a.id, s).is_some();
            (!runs, a.id != PRIMARY)
        });
        Ok(owners)
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

    fn refuse_acp(&self, agent: &str, error: ErrorObject) -> Refusal {
        let status = match error.code {
            code::NOT_FOUND | code::UNKNOWN_AGENT => 404,
            _ => 502,
        };
        Refusal::new(status, self.say(agent, &error))
    }

    /// An error as the owner reads it: the host's plain words, or the
    /// agent's own with its name.
    fn say(&self, agent: &str, error: &ErrorObject) -> String {
        turn::plain(&self.name_of(agent), error)
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
                let contract = self.clone();
                tokio::spawn(async move { contract.cancel(data).await });
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

    /// The id the phone knows `agent` by on `session`.
    fn named(&self, agent: &str, session: &str) -> String {
        let state = self.state.lock().expect("contract state");
        state
            .named
            .get(&(agent.to_owned(), session.to_owned()))
            .cloned()
            .unwrap_or_else(|| agent.to_owned())
    }

    /// A session's key as the phone knows it.
    fn key(&self, agent: &str, session: &str) -> String {
        session_key(&self.named(agent, session), &chat_id(&self.member_of(agent), session))
    }

    /// A turn's frame fields: `{agent_id, session_id, turn_id}`, with the
    /// agent and chat as the phone knows them.
    fn payload(&self, agent: &str, session: &str, turn_id: &str) -> Value {
        json!({ "agent_id": self.named(agent, session), "session_id": self.key(agent, session), "turn_id": turn_id })
    }

    /// A `chat` frame: `{prompt, agent_id, session_id?, permission_mode?}`.
    /// Without a session the chat is created first and announced with
    /// `chat_created`. `permission_mode` is Nebo's permission mode for the
    /// employee, which a runtime with modes of its own runs the turn in.
    async fn chat(self: Arc<Self>, data: Value) {
        let agent_id = data["agent_id"].as_str().unwrap_or(PRIMARY).to_owned();
        let prompt = data["prompt"].as_str().unwrap_or("").trim().to_owned();
        let permission = data["permission_mode"].as_str().and_then(Permission::parse);
        let (agent, member) = match self.resolve(&agent_id).await {
            Ok(found) => found,
            Err(refusal) => {
                self.broadcast(
                    "chat_error",
                    json!({ "agent_id": agent_id, "session_id": data["session_id"], "error": refusal.message }),
                );
                return;
            }
        };
        let members: Vec<String> = self.roster().members().into_iter().map(|m| m.id).collect();
        let members: Vec<&str> = members.iter().map(String::as_str).collect();
        let session = match data["session_id"].as_str() {
            Some(key) => match parse_session_key(key).and_then(|(_, chat)| session_of(&member, chat, &members)) {
                Some(session) => session.to_owned(),
                None => {
                    self.broadcast(
                        "chat_error",
                        json!({ "agent_id": agent_id, "session_id": key, "error": "That conversation is not on this bot." }),
                    );
                    return;
                }
            },
            None => match self.host.new_session(&agent.id, new_session_params(&agent)).await {
                Ok(created) => {
                    let session = created["sessionId"].as_str().unwrap_or("").to_owned();
                    self.broadcast(
                        "chat_created",
                        json!({ "agent_id": agent_id, "session_id": session_key(&agent_id, &chat_id(&member, &session)) }),
                    );
                    session
                }
                Err(e) => {
                    self.broadcast(
                        "chat_error",
                        json!({ "agent_id": agent_id, "error": self.say(&agent.id, &e) }),
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
                .insert((agent.id.clone(), session.clone()), agent_id.clone());
        }
        let chat = chat_id(&member, &session);
        let session_id = session_key(&agent_id, &chat);
        if prompt.is_empty() {
            self.broadcast(
                "chat_error",
                json!({ "agent_id": agent_id, "session_id": session_id, "error": "Nothing to send." }),
            );
            return;
        }
        let queued = {
            let mut state = self.state.lock().expect("contract state");
            if state.running.contains_key(&chat) {
                state
                    .queued
                    .entry(chat.clone())
                    .or_default()
                    .push_back((prompt.clone(), permission));
                true
            } else {
                state.running.insert(chat.clone(), agent.name.clone());
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
        self.start(agent, session, prompt, permission).await;
    }

    /// Starts a turn on a chat whose slot in `running` is taken: the session
    /// opened in the host if it is not, the agent put in the mode the
    /// employee's permission maps to, and the prompt sent.
    async fn start(self: &Arc<Self>, agent: Agent, session: String, prompt: String, permission: Option<Permission>) {
        let refused = match self.begin(&agent, &session, prompt, permission).await {
            Ok(()) => return,
            Err(message) => message,
        };
        let chat = chat_id(&self.member_of(&agent.id), &session);
        {
            let mut state = self.state.lock().expect("contract state");
            state.running.remove(&chat);
            state.queued.remove(&chat);
        }
        self.broadcast(
            "chat_error",
            json!({ "agent_id": self.named(&agent.id, &session), "session_id": self.key(&agent.id, &session), "error": refused }),
        );
    }

    async fn begin(&self, agent: &Agent, session: &str, prompt: String, permission: Option<Permission>) -> Result<(), String> {
        if !self.host.is_open(&agent.id, session) {
            // A runtime that keeps its own transcript is read when a client
            // loads the chat; the turn needs none of it.
            let how = if self.host.shared_sessions(&agent.id).await { Open::Resume } else { Open::Load };
            self.open(agent, session, how).await.map_err(|e| self.say(&agent.id, &e))?;
        }
        if let Some(permission) = permission
            && let Some(modes) = self.host.modes(&agent.id, session).and_then(|m| protocol::modes(&json!({ "modes": m })))
            && let Some(wanted) = mode_for(permission, &modes.available).filter(|id| *id != modes.current)
        {
            let wanted = wanted.to_owned();
            self.host
                .change_session(
                    &agent.id,
                    "session/set_mode",
                    json!({ "sessionId": session, "modeId": wanted }),
                    Some(self.me),
                )
                .await
                .map_err(|e| format!("{} could not switch to its {wanted} mode: {}", agent.name, e.message))?;
            tracing::info!(session, mode = %wanted, ?permission, "contract: session mode set");
        }
        self.host
            .prompt(&agent.id, session, vec![json!({ "type": "text", "text": prompt })], None, Some(self.me))
            .map(|_| ())
            .map_err(|e| self.say(&agent.id, &e))
    }

    /// One event from the host, as the phone's frames.
    fn on_event(self: &Arc<Self>, stamped: Stamped) {
        match stamped.event {
            Event::Update(update) => {
                let (agent, session) = (update.agent, update.session_id);
                let Some((_, parsed)) = protocol::update(&json!({ "sessionId": session, "update": update.update })) else {
                    return;
                };
                let turn_id = self.host.turn(&agent, &session).map(|t| t.turn_id).unwrap_or_default();
                let data = self.payload(&agent, &session, &turn_id);
                match parsed {
                    protocol::Update::AgentText { text, .. } => {
                        self.tools(&agent, &session, |tools| tools.flush());
                        self.broadcast("chat_stream", with(data, json!({ "content": text, "done": false })));
                    }
                    protocol::Update::Thought(text) => {
                        self.broadcast("thinking", with(data, json!({ "text": text, "content": text })));
                    }
                    protocol::Update::Plan(entries) => {
                        let text = turn::plan(&entries);
                        self.broadcast("thinking", with(data, json!({ "text": text, "content": text })));
                    }
                    protocol::Update::ToolCall(call) | protocol::Update::ToolCallUpdate(call) => {
                        let duration = update_duration(&update.update);
                        self.tools(&agent, &session, |tools| tools.tool(call, duration));
                    }
                    _ => {}
                }
            }
            Event::Pending(update) => match update.change {
                PendingChange::Added => self.asked(&update.request),
                PendingChange::Resolved => self.resolved(&update.request.id),
            },
            Event::Turn(turn) if turn.state == TurnState::Ended => {
                self.tools(&turn.agent, &turn.session_id, |tools| tools.flush());
                self.state
                    .lock()
                    .expect("contract state")
                    .tools
                    .remove(&(turn.agent.clone(), turn.session_id.clone()));
                self.ended(&turn);
                self.next(&turn);
            }
            Event::Closed { agent, session_id } => {
                self.state.lock().expect("contract state").tools.remove(&(agent, session_id));
            }
            Event::Turn(_) | Event::Agent(_) | Event::Answered(_) => {}
        }
    }

    /// Runs `work` on the session's tool cards and sends the frames it made.
    fn tools<T>(&self, agent: &str, session: &str, work: impl FnOnce(&mut Tools) -> T) -> T {
        let (result, frames) = {
            let mut state = self.state.lock().expect("contract state");
            let tools = state.tools.entry((agent.to_owned(), session.to_owned())).or_default();
            let result = work(tools);
            (result, tools.take())
        };
        if frames.is_empty() {
            return result;
        }
        let turn_id = self.host.turn(agent, session).map(|t| t.turn_id).unwrap_or_default();
        let data = self.payload(agent, session, &turn_id);
        for card in frames {
            let (kind, fields) = frame(card);
            self.broadcast(kind, with(data.clone(), fields));
        }
        result
    }

    /// A turn's end as the phone reads it.
    fn ended(&self, turn: &TurnUpdate) {
        let data = self.payload(&turn.agent, &turn.session_id, &turn.turn_id);
        if let Some(error) = &turn.error {
            self.broadcast("chat_error", with(data, json!({ "error": self.say(&turn.agent, error) })));
        } else if turn.stop_reason == Some(StopReason::Cancelled) {
            self.broadcast("chat_cancelled", data);
        } else if turn.stop_reason == Some(StopReason::Refusal) {
            let error = format!("{} declined to do that.", self.name_of(&turn.agent));
            self.broadcast("chat_error", with(data, json!({ "error": error })));
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
        let chat = chat_id(&self.member_of(&turn.agent), &turn.session_id);
        let next = {
            let mut state = self.state.lock().expect("contract state");
            let next = state.queued.get_mut(&chat).and_then(VecDeque::pop_front);
            if next.is_none() {
                state.queued.remove(&chat);
                state.running.remove(&chat);
            }
            next
        };
        if let Some((prompt, permission)) = next {
            let contract = self.clone();
            let agent_id = turn.agent.clone();
            let session = turn.session_id.clone();
            tokio::spawn(async move {
                match contract.roster().agents().await.ok().and_then(|all| all.into_iter().find(|a| a.id == agent_id)) {
                    Some(agent) => contract.start(agent, session, prompt, permission).await,
                    None => {
                        let mut state = contract.state.lock().expect("contract state");
                        state.running.remove(&chat);
                        state.queued.remove(&chat);
                    }
                }
            });
        }
    }

    /// The words a request is shown in: the runtime's own, else made from
    /// the tool call as the session's cards know it.
    fn words_of(&self, request: &PendingRequest) -> Words {
        if let Some(words) = self.state.lock().expect("contract state").words.get(&request.id) {
            return words.clone();
        }
        if !request.words.question.is_empty() {
            return request.words.clone();
        }
        let call = AcpToolCall::parse(&request.params["toolCall"]).unwrap_or_else(|| AcpToolCall {
            id: request.tool_call.tool_call_id.clone(),
            ..AcpToolCall::default()
        });
        turn::words(&call, &request.options)
    }

    /// A question waiting for the owner: its tool's card, its card on the
    /// chat, a notice, and the inbox told.
    fn asked(&self, request: &PendingRequest) {
        let words = match request.words.question.is_empty() {
            false => request.words.clone(),
            true => {
                let call = AcpToolCall::parse(&request.params["toolCall"]).unwrap_or_else(|| AcpToolCall {
                    id: request.tool_call.tool_call_id.clone(),
                    ..AcpToolCall::default()
                });
                self.tools(&request.agent, &request.session_id, |tools| tools.ask(call, &request.options))
            }
        };
        let chat = chat_id(&self.member_of(&request.agent), &request.session_id);
        let agent_name = {
            let mut state = self.state.lock().expect("contract state");
            state.words.insert(request.id.clone(), words.clone());
            state.running.get(&chat).cloned().unwrap_or_else(|| self.host.label(&request.agent))
        };
        let notice = Notice {
            id: format!("approval:{}", request.id),
            title: format!("{agent_name} asks to {}", words.summary),
            body: words.question.clone(),
            created_at: unix_now(),
            read: false,
            agent_id: self.named(&request.agent, &request.session_id),
            chat_id: chat,
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
            with(self.payload(&request.agent, &request.session_id, turn_id), card(request, &words)),
        );
    }

    /// A question answered or dropped: its notice and inbox item go.
    fn resolved(&self, request_id: &str) {
        let id = format!("approval:{request_id}");
        {
            let mut state = self.state.lock().expect("contract state");
            state.notices.retain(|n| n.id != id);
            state.words.remove(request_id);
        }
        if let Some(inbox) = &self.inbox {
            inbox.post(InboxItem::Resolved { id });
        }
    }

    /// An `ask_response {request_id, value}`: the value is the label the
    /// phone showed (or the option's id), and the host takes it from there.
    fn answer(&self, request_id: &str, value: &str) {
        let option = self.host.pending().into_iter().find(|p| p.id == request_id).and_then(|p| {
            let labels = self.words_of(&p).labels;
            let i = p
                .options
                .iter()
                .zip(labels.iter().map(Some).chain(std::iter::repeat(None)))
                .position(|(o, label)| label.is_some_and(|l| l == value) || o.option_id == value)?;
            Some(p.options[i].option_id.clone())
        });
        let answered = option.and_then(|option_id| {
            self.host
                .answer(request_id, Outcome::Selected { option_id }, None)
                .ok()
        });
        if answered.is_none() {
            tracing::info!(request_id, "ask_response for a question nobody is waiting on");
        }
    }

    /// A `cancel {session_id | agent_id}`: stops the chat's turn, or every
    /// turn of the agent. With nothing running the phone is told so.
    async fn cancel(&self, data: Value) {
        let session_id = data["session_id"].as_str().map(str::to_owned);
        let agent_id = data["agent_id"].as_str().map(str::to_owned);
        let key = session_id.as_deref().and_then(parse_session_key);
        let named = key.map(|(agent, _)| agent.to_owned()).or_else(|| agent_id.clone());
        let stopped = match (&named, key) {
            (Some(named), Some((_, chat))) => match self.resolve(named).await {
                Ok((agent, member)) => {
                    let members: Vec<String> = self.roster().members().into_iter().map(|m| m.id).collect();
                    let members: Vec<&str> = members.iter().map(String::as_str).collect();
                    match session_of(&member, chat, &members) {
                        Some(session) => self.host.cancel(Some(&agent.id), Some(session)),
                        None => 0,
                    }
                }
                Err(_) => 0,
            },
            (Some(named), None) => match self.resolve(named).await {
                Ok((agent, _)) => self.host.cancel(Some(&agent.id), None),
                Err(_) => 0,
            },
            (None, _) => 0,
        };
        if stopped == 0 {
            self.broadcast(
                "chat_cancelled",
                json!({ "agent_id": agent_id, "session_id": session_id.as_deref().unwrap_or("default") }),
            );
        }
    }
}

/// `session/new`'s params for `agent`: its folder, or `/` for one without.
fn new_session_params(agent: &Agent) -> Value {
    json!({ "cwd": agent.folder.as_deref().unwrap_or("/"), "mcpServers": [] })
}

/// How long a tool call took, where the runtime says (`_meta.durationMs`).
fn update_duration(update: &Value) -> Option<u64> {
    update["_meta"]["durationMs"].as_u64()
}

/// A session's record as the transcript the phone shows.
fn transcript(record: &[Recorded]) -> Vec<Message> {
    let mut t = Transcript::default();
    for recorded in record {
        let at = recorded.at.or_else(|| recorded.update["_meta"]["createdAt"].as_f64());
        let Some((_, update)) = protocol::update(&json!({ "sessionId": "", "update": recorded.update })) else {
            continue;
        };
        match update {
            protocol::Update::UserText { text, message_id } => t.user(&text, message_id, at),
            protocol::Update::AgentText { text, message_id } => t.agent(&text, message_id, at),
            protocol::Update::ToolCall(call) | protocol::Update::ToolCallUpdate(call) => t.tool(&call, at),
            _ => {}
        }
    }
    t.messages
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
            if self.messages.last().is_none_or(|m| m.role != Role::Assistant) {
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
            self.calls.insert(update.id.clone(), (at, index, fresh, false));
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

/// A tool card as the phone's frame: `tool_start` or `tool_result`.
fn frame(card: ToolEvent) -> (&'static str, Value) {
    match card {
        ToolEvent::Started { id, name, input } => (
            "tool_start",
            json!({ "tool_id": id, "tool": name, "label": name, "input": input }),
        ),
        ToolEvent::Finished {
            id,
            name,
            output,
            failed,
            duration_ms,
        } => (
            "tool_result",
            json!({ "tool_id": id, "tool_name": name, "result": output, "is_error": failed, "outcome": name, "duration_ms": duration_ms }),
        ),
    }
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

    #[test]
    fn a_replayed_session_reads_as_the_phone_shows_it() {
        let recorded = |update: Value| Recorded { update, at: None };
        let record = vec![
            recorded(json!({ "sessionUpdate": "user_message_chunk", "messageId": "u1", "content": { "type": "text", "text": "Run it" } })),
            recorded(json!({ "sessionUpdate": "tool_call", "toolCallId": "t1", "title": "Terminal", "rawInput": { "command": "echo hi" } })),
            recorded(json!({ "sessionUpdate": "tool_call_update", "toolCallId": "t1", "title": "echo hi", "status": "completed",
                "content": [{ "type": "content", "content": { "type": "text", "text": "hi" } }] })),
            recorded(json!({ "sessionUpdate": "agent_message_chunk", "messageId": "a1", "content": { "type": "text", "text": "The command " } })),
            recorded(json!({ "sessionUpdate": "agent_message_chunk", "messageId": "a1", "content": { "type": "text", "text": "printed hi." } })),
            recorded(json!({ "sessionUpdate": "user_message_chunk", "messageId": "u2", "_meta": { "createdAt": 7.0 },
                "content": { "type": "text", "text": "Thanks" } })),
        ];
        let messages = transcript(&record);
        let roles: Vec<Role> = messages.iter().map(|m| m.role).collect();
        assert_eq!(roles, [Role::User, Role::Assistant, Role::Tool, Role::Assistant, Role::User]);
        assert_eq!(messages[1].tool_calls[0].name, "echo hi");
        assert_eq!(messages[2].tool_result.as_ref().unwrap().content, "hi");
        assert_eq!(messages[3].text, "The command printed hi.");
        assert_eq!(messages[4].created_at, Some(7.0), "a runtime's own time of a replayed message");
    }

    #[test]
    fn chat_ids_name_their_member() {
        let members = ["assistant", "codex"];
        assert_eq!(chat_id("assistant", "s1"), "s1");
        assert_eq!(chat_id("codex", "s1"), "codex~s1");
        assert_eq!(session_of("codex", "codex~s1", &members), Some("s1"));
        assert_eq!(session_of("assistant", "codex~s1", &members), None, "another member's chat");
        assert_eq!(session_of("assistant", "s1", &members), Some("s1"));
        assert_eq!(session_of("codex", "s1", &members), None);
    }
}
