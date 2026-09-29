//! The host core: every agent a computer runs (its [`Roster`]), each spoken
//! to in ACP, and Open Agent Link's rules for many clients sharing them
//! (`spec/oal-0.1.md` §7–§10, §12). No transport: whoever embeds the host
//! (the nebo-link daemon, Nebo itself) carries it to clients, and each client
//! (an OAL connection, the phone contract) is a [`ClientId`] reading the
//! host's [`Event`]s.
//!
//! - **Sessions.** The host is the only ACP client of every agent. For each
//!   session open in an agent it keeps a record: every `session/update` sent
//!   about it (the agent's replay when it was opened, the prompts, every
//!   turn's updates), its modes and config options, its running turn, the
//!   last turn's end, and its pending permission requests.
//!   [`Host::open_session`] answers a session already open from that record,
//!   and opens any other in its agent.
//! - **Turns.** [`Host::prompt`] starts one on a session and says so with
//!   [`TurnUpdate`] `running`; every turn it accepts ends with exactly one
//!   `ended`, after its last update and after [`Event::Answered`] carries the
//!   agent's answer. A second prompt while one runs is refused with
//!   `turn_in_progress`.
//! - **Permission requests.** One an agent sends becomes a
//!   [`PendingRequest`] on the host-wide list and a [`PendingUpdate`]
//!   `added`. The first answer wins ([`Host::answer`], a cancel, the agent
//!   taking it back); a turn that ends resolves what it left open as
//!   `cancelled`.
//! - **Agents.** [`Host::agents`] lists every hosted agent, one whose
//!   runtime doesn't answer included, offline with the reason;
//!   [`Host::set_members`] and [`Host::refresh`] announce what changed with
//!   [`AgentUpdate`].
//! - **Adding and removing agents.** [`Host::add_agent`] starts a new
//!   coding agent in a folder of its own and hosts it; [`Host::remove_agent`]
//!   stops hosting one and leaves its folder. What the host keeps of them is
//!   its embedder's ([`crate::keep`]).
//! - **Moving.** Every coding agent's session gets the host's own tools
//!   ([`crate::tools`]); with `move_to_folder` the agent moves the
//!   conversation to another folder when the owner asks
//!   ([`Host::move_to_folder`]).
//! - **Order.** Every event carries the host's sequence number, and a
//!   snapshot ([`Opened::seq`]) the number it was taken at, so a client that
//!   attaches to a session skips the events its snapshot already holds.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime};

use nebo_runtimes::acp::protocol;
use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::backend::{AgentMessage, Error, FromAgent, Inbox, Reply};
use crate::model::{
    self, Agent, AgentChange, AgentStatus, AgentUpdate, DeviceRef, ErrorObject, Life, Outcome, PendingChange,
    PendingRequest, PendingUpdate, SessionStatus, StopReason, ToolCallUpdate, TurnState, TurnUpdate, Usage, Working, code,
};
use crate::keep::{Add, Addable, CodingAgent, Keeper, Kept};
use crate::roster::{Member, Roster, agent_ids, member_agents};
use crate::tools::Tools;

/// Where a session's `_meta` says the folder it works in, once it moved
/// (`session_info_update`, and the answer to `session/load` and
/// `session/resume`).
pub const META_CWD: &str = "oal/cwd";

/// How many resolved request ids are remembered, so a late answer reads
/// `already_answered` rather than `unknown_request`.
const RESOLVED_REMEMBERED: usize = 256;
/// How many sessions keep their record; past it, the least recently used
/// with nothing running or waiting is let go (a later load opens it again).
const OPEN_SESSIONS: usize = 256;
/// How many updates for a session the host doesn't know yet are kept for
/// when it learns of it (an agent's first updates can come before its
/// answer to `session/new`).
const STRAYS: usize = 64;

/// A client of the host: an Open Agent Link connection, the phone contract.
/// Events that a client caused and already knows name it
/// ([`SessionUpdate::skip`], [`Answered::client`]).
pub type ClientId = u64;

/// What the host tells its clients, in order.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// `host/turn`: a turn started or ended.
    Turn(TurnUpdate),
    /// ACP `session/update`, recorded.
    Update(SessionUpdate),
    /// The agent's answer to a turn's `session/prompt`: just before the
    /// turn's `ended`, for the client that sent the prompt.
    Answered(Answered),
    /// `host/pending_update`: a permission request was added or resolved.
    Pending(Box<PendingUpdate>),
    /// `host/agent_update`: an agent was added, changed or removed.
    Agent(AgentUpdate),
    /// The session was closed or deleted: every client is detached from it.
    Closed { agent: String, session_id: String },
}

/// An event and its place in the host's order.
#[derive(Debug, Clone, PartialEq)]
pub struct Stamped {
    pub seq: u64,
    pub event: Event,
}

/// One `session/update` of a session.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionUpdate {
    pub agent: String,
    pub session_id: String,
    /// The ACP `SessionUpdate`, as the agent sent it (or as the host made it:
    /// a prompt's echo, a mode change).
    pub update: Value,
    /// The client that caused it and so already knows it (its own prompt,
    /// its own mode change), which is not sent it again.
    pub skip: Option<ClientId>,
}

/// A turn's `session/prompt`, answered.
#[derive(Debug, Clone, PartialEq)]
pub struct Answered {
    pub agent: String,
    pub session_id: String,
    pub turn_id: String,
    /// The client that sent the prompt.
    pub client: Option<ClientId>,
    /// The agent's `PromptResponse`, or its error.
    pub response: Result<Value, ErrorObject>,
}

/// One recorded update and when the host received it live (`None` for one
/// replayed when the session was opened).
#[derive(Debug, Clone, PartialEq)]
pub struct Recorded {
    pub update: Value,
    /// Unix seconds.
    pub at: Option<f64>,
}

/// A session as a client attaching to it gets it (§8, §12).
#[derive(Debug, Clone, PartialEq)]
pub struct Opened {
    /// The host's sequence number when this was taken: events up to it are
    /// in it.
    pub seq: u64,
    /// Every update of the session so far (`session/load`; empty for
    /// `session/resume`).
    pub record: Vec<Recorded>,
    /// The session's most recent `host/turn`: its running turn, else the last
    /// turn's end.
    pub turn: Option<TurnUpdate>,
    /// The answer to `session/load` or `session/resume`: the agent's own when
    /// the host opened the session now, else the session's current modes and
    /// config options.
    pub response: Value,
    /// The session's pending permission requests, oldest first.
    pub pending: Vec<PendingRequest>,
}

/// How `session/load` and `session/resume` differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Open {
    /// Replays the session.
    Load,
    /// Reopens it without the replay.
    Resume,
}

impl Open {
    fn method(self) -> &'static str {
        match self {
            Open::Load => "session/load",
            Open::Resume => "session/resume",
        }
    }
}

/// Every agent on the computer, and their sessions.
pub struct Host {
    roster: Arc<Roster>,
    events: broadcast::Sender<Stamped>,
    state: Mutex<State>,
    /// One session opened in an agent at a time, so none is opened twice.
    opening: tokio::sync::Mutex<()>,
    clients: AtomicU64,
    /// The backends handed the host's inbox, by address.
    connected: Mutex<std::collections::HashSet<usize>>,
    /// What keeps the agents the host adds; `None` = agents are added only
    /// where the host is made.
    keeper: Mutex<Option<Arc<dyn Keeper>>>,
    /// One agent added or removed at a time, so two never take one id.
    adding: tokio::sync::Mutex<()>,
    /// The host's own MCP server, started when a session first needs it.
    tools: tokio::sync::OnceCell<Option<Arc<Tools>>>,
    /// The OS user's home: where new agents' folders go, and where a session
    /// may move outside Full access.
    home: Mutex<Option<PathBuf>>,
    me: Weak<Host>,
}

#[derive(Default)]
struct State {
    seq: u64,
    sessions: HashMap<Key, Session>,
    /// Which session an agent's message is about: (member, runtime agent,
    /// session) to the host's key.
    index: HashMap<(String, String, String), Key>,
    /// Updates for sessions the host doesn't know yet.
    strays: HashMap<(String, String, String), Vec<Value>>,
    /// Every pending permission request, oldest first.
    pending: Vec<Pending>,
    /// Requests resolved lately, newest last.
    resolved: VecDeque<String>,
    /// The agents as last announced; `None` until first listed.
    known: Option<Vec<Agent>>,
    /// Bumped on every use of a session, for letting the least used go.
    tick: u64,
}

/// (agent, session).
type Key = (String, String);

struct Session {
    member: String,
    runtime_agent: String,
    record: Vec<Recorded>,
    modes: Option<Value>,
    config_options: Option<Value>,
    model: Option<String>,
    /// The folder it works in, once it moved to another.
    folder: Option<String>,
    turn: Option<Running>,
    last_ended: Option<TurnUpdate>,
    /// Being opened in its agent: its replay is recorded but not announced.
    opening: bool,
    used: u64,
}

impl Session {
    fn new(member: &str, runtime_agent: &str) -> Self {
        Self {
            member: member.to_owned(),
            runtime_agent: runtime_agent.to_owned(),
            record: Vec::new(),
            modes: None,
            config_options: None,
            model: None,
            folder: None,
            turn: None,
            last_ended: None,
            opening: false,
            used: 0,
        }
    }

    /// Takes in what an agent's answer says about the session.
    fn answered(&mut self, result: &Value) {
        if result["modes"].is_object() {
            self.modes = Some(result["modes"].clone());
        }
        if result["configOptions"].is_array() {
            self.config_options = Some(result["configOptions"].clone());
        }
        if let Some(model) = protocol::model(result) {
            self.model = Some(model);
        }
        if let Some(folder) = result["_meta"][META_CWD].as_str() {
            self.folder = Some(folder.to_owned());
        }
    }

    /// Takes in what an update says about the session.
    fn apply(&mut self, update: &Value) {
        match update["sessionUpdate"].as_str() {
            Some("current_mode_update") => {
                if let (Some(modes), Some(mode)) = (&mut self.modes, update.get("currentModeId")) {
                    modes["currentModeId"] = mode.clone();
                }
            }
            Some("config_option_update") => {
                if update["configOptions"].is_array() {
                    self.config_options = Some(update["configOptions"].clone());
                }
                if let Some(model) = protocol::model(update) {
                    self.model = Some(model);
                }
            }
            _ => {}
        }
    }

    /// The session's current modes and config options, as `session/load`
    /// answers for a session already open.
    fn current(&self) -> Value {
        let mut answer = json!({});
        if let Some(modes) = &self.modes {
            answer["modes"] = modes.clone();
        }
        if let Some(options) = &self.config_options {
            answer["configOptions"] = options.clone();
        }
        if let Some(folder) = &self.folder {
            answer["_meta"] = json!({ META_CWD: folder });
        }
        answer
    }

    fn latest_turn(&self) -> Option<TurnUpdate> {
        self.turn.as_ref().map(|r| r.turn.clone()).or_else(|| self.last_ended.clone())
    }
}

struct Running {
    turn: TurnUpdate,
    client: Option<ClientId>,
}

struct Pending {
    request: PendingRequest,
    member: String,
    /// The agent's reply, and its id within its backend.
    reply: Option<Reply>,
    reply_id: u64,
}

impl Host {
    pub fn new(roster: Arc<Roster>) -> Arc<Self> {
        let (events, _) = broadcast::channel(4096);
        let host = Arc::new_cyclic(|me| Self {
            roster,
            events,
            state: Mutex::new(State::default()),
            opening: tokio::sync::Mutex::new(()),
            clients: AtomicU64::new(0),
            connected: Mutex::new(std::collections::HashSet::new()),
            keeper: Mutex::new(None),
            adding: tokio::sync::Mutex::new(()),
            tools: tokio::sync::OnceCell::new(),
            home: Mutex::new(dirs::home_dir()),
            me: me.clone(),
        });
        for member in host.roster.members() {
            host.ensure(&member);
        }
        host
    }

    pub fn roster(&self) -> &Arc<Roster> {
        &self.roster
    }

    /// Where the agents the host adds are kept: from now on the host adds
    /// and removes agents ([`Host::add_agent`], [`Host::remove_agent`]).
    pub fn set_keeper(&self, keeper: Arc<dyn Keeper>) {
        *self.keeper.lock().expect("keeper") = Some(keeper);
    }

    /// The OS user's home, when it isn't the one the OS reports.
    pub fn set_home(&self, home: PathBuf) {
        *self.home.lock().expect("home") = Some(home);
    }

    fn home(&self) -> Option<PathBuf> {
        self.home.lock().expect("home").clone()
    }

    /// Every event from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<Stamped> {
        self.events.subscribe()
    }

    /// A new client's id.
    pub fn client(&self) -> ClientId {
        self.clients.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Hands a member's backend the host's inbox once: a member the roster
    /// took in directly ([`Roster::set`]) is connected when first used.
    fn ensure(&self, member: &Member) {
        let address = Arc::as_ptr(&member.backend) as *const () as usize;
        if self.connected.lock().expect("connected").insert(address) {
            self.connect(member);
        }
    }

    /// The backend of the member `id`, connected.
    fn backend(&self, id: &str) -> Option<Arc<dyn crate::backend::Backend>> {
        let member = self.roster.members().into_iter().find(|m| m.id == id)?;
        self.ensure(&member);
        Some(member.backend)
    }

    /// Hands a member's backend the host's inbox.
    fn connect(&self, member: &Member) {
        let me = self.me.clone();
        let id = member.id.clone();
        let inbox: Inbox = Arc::new(move |message: FromAgent| {
            if let Some(host) = me.upgrade() {
                host.receive(&id, message);
            }
        });
        member.backend.connect(inbox);
    }

    /// Emits `event` in order; the state's lock is held, so the order is the
    /// order the state changed in.
    fn emit(state: &mut State, events: &broadcast::Sender<Stamped>, event: Event) {
        state.seq += 1;
        let _ = events.send(Stamped {
            seq: state.seq,
            event,
        });
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("host state")
    }

    /// Whether any agent can take a prompt now; the error says why not.
    pub async fn ready(&self) -> Result<(), String> {
        self.roster.ready().await
    }

    // -- Agents ---------------------------------------------------------------

    /// Every hosted agent (`host/agents`): one whose runtime doesn't answer
    /// is listed offline, with the reason.
    pub async fn agents(&self) -> Vec<Agent> {
        let now = self.current().await;
        let mut state = self.lock();
        state.known.get_or_insert_with(|| now.clone());
        now
    }

    async fn current(&self) -> Vec<Agent> {
        let mut all = Vec::new();
        let members = self.roster.members();
        let ids: Vec<&str> = members.iter().map(|m| m.id.as_str()).collect();
        for member in &members {
            match member.backend.agents().await {
                Ok(agents) => all.extend(member_agents(member, agents, &ids).into_iter().map(|a| Agent {
                    online: a.offline_reason.is_none(),
                    id: a.id,
                    label: a.name,
                    runtime: member.runtime.clone(),
                    folder: a.folder,
                    offline_reason: a.offline_reason,
                    capabilities: a.capabilities,
                    modes: a.modes,
                })),
                Err(e) => all.push(Agent {
                    id: member.id.clone(),
                    label: member.label.clone(),
                    runtime: member.runtime.clone(),
                    folder: None,
                    online: false,
                    offline_reason: Some(match e {
                        Error::Unavailable(_) => {
                            format!("Could not connect to {}. Try again.", member.label)
                        }
                        other => other.message(&member.label),
                    }),
                    capabilities: json!({}),
                    modes: None,
                }),
            }
        }
        all
    }

    /// Replaces the hosted agents: one that stays keeps its backend (its
    /// running process and sessions). What changed is announced.
    pub fn set_members(self: &Arc<Self>, members: Vec<Member>) {
        for member in &members {
            self.ensure(member);
        }
        self.roster.set(members);
        let host = self.clone();
        tokio::spawn(async move { host.refresh().await });
    }

    /// Lists the agents again and announces each that was added, changed
    /// (came online, went offline, was renamed) or removed since last time.
    pub async fn refresh(&self) {
        // Nothing was announced yet, so nothing can have changed for anyone.
        if self.lock().known.is_none() {
            return;
        }
        let now = self.current().await;
        let mut state = self.lock();
        let Some(before) = state.known.replace(now.clone()) else {
            return;
        };
        let mut updates = Vec::new();
        for agent in &now {
            match before.iter().find(|b| b.id == agent.id) {
                None => updates.push((AgentChange::Added, agent.clone())),
                Some(b) if b != agent => updates.push((AgentChange::Updated, agent.clone())),
                Some(_) => {}
            }
        }
        for gone in before.into_iter().filter(|b| !now.iter().any(|a| a.id == b.id)) {
            updates.push((AgentChange::Removed, gone));
        }
        for (change, agent) in updates {
            Self::emit(&mut state, &self.events, Event::Agent(AgentUpdate { change, agent }));
        }
    }

    /// The member hosting `agent` and the agent's id in its runtime.
    async fn locate(&self, agent: &str) -> Result<(Member, String), ErrorObject> {
        let found = self.roster.locate(agent).await.map_err(|e| match e {
            Error::NotFound(_) => ErrorObject::new(code::UNKNOWN_AGENT, format!("There's no agent called {agent} on this computer.")),
            Error::Unavailable(why) | Error::Failed(why) => ErrorObject::new(code::AGENT_UNAVAILABLE, why),
        })?;
        self.ensure(&found.0);
        Ok(found)
    }

    // -- Adding and removing agents -------------------------------------------

    fn keeper(&self) -> Result<Arc<dyn Keeper>, ErrorObject> {
        self.keeper
            .lock()
            .expect("keeper")
            .clone()
            .ok_or_else(|| ErrorObject::new(code::NOT_PERMITTED, "Agents are added on this computer itself."))
    }

    /// The coding agents that can be added on this computer
    /// ([`Host::add_agent`]); none where the host adds none.
    pub fn addable(&self) -> Vec<Addable> {
        let keeper = self.keeper.lock().expect("keeper").clone();
        keeper
            .map(|k| k.addable())
            .unwrap_or_default()
            .into_iter()
            .map(|a| Addable { id: a.id, name: a.name })
            .collect()
    }

    /// Adds a coding agent ([`crate::keep`]): it is started once in its
    /// folder (made now, one of its own unless the owner on the computer
    /// named one), kept, hosted, and announced `added`.
    pub async fn add_agent(self: &Arc<Self>, add: Add) -> Result<Agent, ErrorObject> {
        let keeper = self.keeper()?;
        let refused = |why: String| ErrorObject::new(code::NOT_PERMITTED, why);
        let (kind, name, command) = match add.command {
            Some(command) => (nebo_runtimes::acp::Agent::Other, nebo_runtimes::acp::Agent::Other.name().to_owned(), command),
            None => {
                let found = crate::keep::find(keeper.addable(), &add.runtime).map_err(refused)?;
                (found.agent, found.name, found.command)
            }
        };
        let name = name.as_str();
        let _one = self.adding.lock().await;
        let kept = keeper.agents();
        let label = add
            .label
            .as_deref()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| crate::keep::label(&kept, kind.key(), name, add.folder.as_deref()));
        let id = crate::keep::id(&kept, &label);
        let folder = match add.folder {
            Some(folder) => folder,
            None => {
                let home = self.home().ok_or_else(|| refused("This user has no home folder to work in.".to_owned()))?;
                crate::keep::default_folder(&home, &id)
            }
        };
        let folder = crate::keep::make(&folder, name).map_err(refused)?;
        if let Some(same) = kept.iter().find(|k| k.runtime == kind.key() && k.folder.as_deref() == Some(folder.as_path())) {
            return Err(refused(format!(
                "{name} already works in {} as \"{}\". Choose another folder.",
                folder.display(),
                same.label
            )));
        }
        let title = crate::acp::probe(name, &command, &folder, keeper.client()).await.map_err(|why| {
            ErrorObject::new(code::AGENT_UNAVAILABLE, why)
        })?;
        let label = match (kind, title, add.label.is_some()) {
            (nebo_runtimes::acp::Agent::Other, Some(title), false) => title,
            _ => label,
        };
        let coding = CodingAgent {
            id: id.clone(),
            label,
            agent: kind,
            acp: crate::acp::AcpLink {
                program: command.program,
                args: command.args,
                env: command.env,
                workdir: folder,
            },
        };
        let member = keeper.keep(&coding).map_err(|why| ErrorObject::new(code::INTERNAL, why))?;
        self.ensure(&member);
        let mut members = self.roster.members();
        members.retain(|m| m.id != member.id);
        members.push(member);
        self.roster.set(members);
        self.refresh().await;
        tracing::info!(agent = %coding.id, folder = %coding.acp.workdir.display(), "host: an agent was added");
        Ok(self
            .current()
            .await
            .into_iter()
            .find(|a| a.id == id)
            .unwrap_or_else(|| Agent {
                id,
                label: coding.label.clone(),
                runtime: kind.key().to_owned(),
                folder: Some(coding.acp.workdir.display().to_string()),
                online: true,
                offline_reason: None,
                capabilities: json!({}),
                modes: None,
            }))
    }

    /// Stops hosting the agent `id`: its turns are stopped, its
    /// conversations closed, its process ended, and it is announced
    /// `removed`. Its folder and everything in it stay.
    pub async fn remove_agent(self: &Arc<Self>, id: &str) -> Result<Agent, ErrorObject> {
        let keeper = self.keeper()?;
        let _one = self.adding.lock().await;
        let listed = self.current().await.into_iter().find(|a| a.id == id);
        let kept: Option<Kept> = keeper.agents().into_iter().find(|k| k.id == id);
        let Some(agent) = listed.or_else(|| {
            kept.map(|k| Agent {
                id: k.id,
                label: k.label,
                runtime: k.runtime,
                folder: k.folder.map(|f| f.display().to_string()),
                online: false,
                offline_reason: None,
                capabilities: json!({}),
                modes: None,
            })
        }) else {
            return Err(ErrorObject::new(code::UNKNOWN_AGENT, format!("There's no agent called {id} on this computer.")));
        };
        keeper.forget(id).map_err(|why| ErrorObject::new(code::NOT_PERMITTED, why))?;
        self.cancel(Some(id), None);
        {
            let mut state = self.lock();
            let keys: Vec<Key> = state.sessions.keys().filter(|k| k.0 == id).cloned().collect();
            for key in keys {
                self.resolve_session(&mut state, &key, None);
                Self::forget(&mut state, &key);
                Self::emit(&mut state, &self.events, Event::Closed { agent: key.0.clone(), session_id: key.1.clone() });
            }
        }
        if let Some(Some(tools)) = self.tools.get() {
            tools.forget_session(id, None);
        }
        let mut members = self.roster.members();
        members.retain(|m| m.id != id);
        self.roster.set(members);
        self.refresh().await;
        tracing::info!(agent = %id, "host: an agent was removed; its folder stays");
        Ok(agent)
    }

    // -- The host's tools -----------------------------------------------------

    /// The host's MCP server, started on first use; `None` when it can't
    /// listen.
    async fn tools(&self) -> Option<Arc<Tools>> {
        self.tools
            .get_or_init(|| async {
                match Tools::start(self.me.clone()).await {
                    Ok(tools) => Some(tools),
                    Err(e) => {
                        tracing::warn!(error = %e, "host: the host's tools could not be served");
                        None
                    }
                }
            })
            .await
            .clone()
    }

    /// Whether `member`'s agent gets the host's tools: it works in a folder
    /// and takes an HTTP MCP server (started first, so its capabilities are
    /// its own).
    async fn takes_tools(&self, member: &Member, runtime_agent: &str) -> bool {
        let capabilities = |agents: Vec<crate::backend::Agent>| {
            agents
                .into_iter()
                .find(|a| a.id == runtime_agent && a.folder.is_some())
                .map(|a| a.capabilities)
        };
        let Some(mut known) = member.backend.agents().await.ok().and_then(capabilities) else {
            return false;
        };
        if known.as_object().is_none_or(|o| o.is_empty()) && member.backend.ready().await.is_ok() {
            known = member.backend.agents().await.ok().and_then(capabilities).unwrap_or_default();
        }
        known["mcpCapabilities"]["http"].as_bool() == Some(true)
    }

    /// `params` with the host's tools added to its `mcpServers`, for the
    /// conversation `session` of `agent` (`None`: about to be made), and the
    /// token they carry.
    async fn with_tools(&self, member: &Member, runtime_agent: &str, agent: &str, session: Option<&str>, mut params: Value) -> (Value, Option<String>) {
        if !self.takes_tools(member, runtime_agent).await {
            return (params, None);
        }
        let Some(tools) = self.tools().await else {
            return (params, None);
        };
        let token = tools.token(agent, session);
        let mut servers = params["mcpServers"].as_array().cloned().unwrap_or_default();
        servers.retain(|s| s["name"] != crate::tools::SERVER);
        servers.push(tools.entry(&token));
        params["mcpServers"] = Value::Array(servers);
        (params, Some(token))
    }

    /// Moves the conversation `session` of `agent` to `path` on the owner's
    /// request (the agent's `move_to_folder`): the folder must exist (made
    /// when `create`), and be inside the owner's home unless the session
    /// runs with full access. The agent starts a new session there, in the
    /// conversation's mode, and the conversation continues in it from its
    /// next prompt, whose first context is `handoff`. Clients are told the
    /// folder (`session_info_update` `_meta."oal/cwd"`) and the owner reads
    /// "Now working in <folder>." The error is the agent's to relay.
    pub async fn move_to_folder(&self, agent: &str, session: &str, path: &str, handoff: &str, create: bool) -> Result<PathBuf, String> {
        let key = (agent.to_owned(), session.to_owned());
        let (member, runtime_agent, modes, current) = {
            let state = self.lock();
            let s = state.sessions.get(&key).ok_or("This conversation isn't open on this computer.")?;
            (s.member.clone(), s.runtime_agent.clone(), s.modes.clone(), s.folder.clone())
        };
        let backend = self.backend(&member).ok_or("This agent is no longer on this computer.")?;
        let home = self.home();
        let here = match current {
            Some(folder) => PathBuf::from(folder),
            None => self
                .current()
                .await
                .into_iter()
                .find(|a| a.id == agent)
                .and_then(|a| a.folder)
                .map(PathBuf::from)
                .ok_or("This agent works without a folder, so it can't move to one.")?,
        };
        let full_access = modes.as_ref().and_then(|m| protocol::modes(&json!({ "modes": m }))).is_some_and(|m| {
            crate::turn::mode_for(crate::turn::Permission::FullAccess, &m.available) == Some(m.current.as_str())
        });
        let folder = resolve_folder(path, home.as_deref(), &here, create, full_access)?;
        // The call came through the host's tools, so they are serving.
        let tools = self.tools.get().cloned().flatten().ok_or("This computer can't move conversations right now. Try again.")?;
        let token = tools.token(agent, Some(session));
        backend
            .move_session(&runtime_agent, session, &folder, handoff, vec![tools.entry(&token)])
            .await
            .map_err(|e| e.message)?;
        // The new session runs in the conversation's mode, as the old one did.
        if let Some(mode) = modes.as_ref().and_then(|m| m["currentModeId"].as_str()) {
            let params = json!({ "sessionId": session, "modeId": mode });
            if let Err(e) = backend.request(&runtime_agent, "session/set_mode", params).await {
                tracing::info!(error = %e.message, mode, "host: the moved conversation's mode was not set again");
            }
        }
        let shown = shown_folder(&folder, home.as_deref());
        let mut state = self.lock();
        if let Some(s) = state.sessions.get_mut(&key) {
            s.folder = Some(folder.display().to_string());
        }
        let info = json!({ "sessionUpdate": "session_info_update", "_meta": { META_CWD: folder.display().to_string() } });
        self.record(&mut state, &key, info, None);
        let note = json!({ "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": format!("\n\nNow working in {shown}.\n\n") } });
        self.record(&mut state, &key, note, None);
        tracing::info!(agent, session, folder = %folder.display(), "host: a conversation moved to another folder");
        Ok(folder)
    }

    // -- Sessions -------------------------------------------------------------

    /// ACP `session/new` on `agent`; the session is open from its answer on.
    /// A coding agent's session gets the host's own tools.
    pub async fn new_session(&self, agent: &str, params: Value) -> Result<Value, ErrorObject> {
        let (member, runtime_agent) = self.locate(agent).await?;
        let (params, token) = self.with_tools(&member, &runtime_agent, agent, None, params).await;
        let answered = member.backend.request(&runtime_agent, "session/new", params).await;
        let tools = self.tools.get().cloned().flatten();
        let result = match answered {
            Ok(result) => result,
            Err(e) => {
                if let (Some(tools), Some(token)) = (&tools, &token) {
                    tools.forget(token);
                }
                return Err(e);
            }
        };
        let Some(session_id) = result["sessionId"].as_str() else {
            return Err(ErrorObject::new(
                code::INTERNAL,
                format!("{} started a conversation without an id.", member.label),
            ));
        };
        if let (Some(tools), Some(token)) = (&tools, &token) {
            tools.bind(token, session_id);
        }
        let mut session = Session::new(&member.id, &runtime_agent);
        session.answered(&result);
        let key = (agent.to_owned(), session_id.to_owned());
        let dropped = Self::insert(&mut self.lock(), key, session);
        self.close_in_agents(dropped);
        Ok(result)
    }

    /// Records a session the host now holds open, with any updates that came
    /// before it, and lets the least used go past [`OPEN_SESSIONS`]: those
    /// are returned, for the caller to close in their agents
    /// ([`Host::close_in_agents`]) once it lets go of the state.
    fn insert(state: &mut State, key: Key, mut session: Session) -> Vec<(Key, Session)> {
        let index = (session.member.clone(), session.runtime_agent.clone(), key.1.clone());
        if let Some(strays) = state.strays.remove(&index) {
            session.record.extend(strays.into_iter().map(|update| Recorded { update, at: None }));
        }
        state.tick += 1;
        session.used = state.tick;
        state.index.insert(index, key.clone());
        state.sessions.insert(key, session);
        let mut dropped = Vec::new();
        while state.sessions.len() > OPEN_SESSIONS {
            let idle = state
                .sessions
                .iter()
                .filter(|(k, s)| {
                    s.turn.is_none() && !s.opening && !state.pending.iter().any(|p| p.request.agent == k.0 && p.request.session_id == k.1)
                })
                .min_by_key(|(_, s)| s.used)
                .map(|(k, _)| k.clone());
            let Some(idle) = idle else { break };
            dropped.extend(Self::forget(state, &idle).map(|session| (idle, session)));
        }
        dropped
    }

    /// Closes sessions the host let go in their agents (`session/close`), so
    /// what an agent runs for one (its process, its tool servers) ends with
    /// it rather than when the agent pauses. Each backend decides what
    /// closing means to it; a runtime whose sessions are shared keeps them.
    fn close_in_agents(&self, dropped: Vec<(Key, Session)>) {
        for ((agent, session_id), session) in dropped {
            let Some(backend) = self.backend(&session.member) else { continue };
            let runtime_agent = session.runtime_agent;
            tokio::spawn(async move {
                let params = json!({ "sessionId": session_id });
                if let Err(e) = backend.request(&runtime_agent, "session/close", params).await {
                    tracing::info!(agent = %agent, session = %session_id, error = %e.message, "host: could not close a session it let go");
                }
            });
        }
    }

    fn forget(state: &mut State, key: &Key) -> Option<Session> {
        let session = state.sessions.remove(key)?;
        state
            .index
            .remove(&(session.member.clone(), session.runtime_agent.clone(), key.1.clone()));
        Some(session)
    }

    /// ACP `session/load` or `session/resume` (`params` as the client sent
    /// them): a session already open is answered from its record; any other
    /// is opened in its agent, which replays it into the record.
    /// A runtime whose sessions change outside the host
    /// ([`crate::backend::Backend::shared_sessions`]) is asked afresh on
    /// every `session/load` of a session with nothing running or waiting.
    pub async fn open_session(&self, agent: &str, how: Open, params: Value) -> Result<Opened, ErrorObject> {
        let session_id = params["sessionId"].as_str().unwrap_or("").to_owned();
        let key = (agent.to_owned(), session_id.clone());
        let (member, runtime_agent) = self.locate(agent).await?;
        let refresh = how == Open::Load && member.backend.shared_sessions();
        if !refresh && let Some(opened) = self.snapshot(&key, how, None) {
            return Ok(opened);
        }
        let _one = self.opening.lock().await;
        let mut dropped = Vec::new();
        let was_open = {
            let mut state = self.lock();
            let busy = state.sessions.get(&key).is_some_and(|s| s.turn.is_some())
                || state.pending.iter().any(|p| p.request.agent == key.0 && p.request.session_id == key.1);
            let open = state.sessions.get(&key).is_some_and(|s| !s.opening);
            if open && (!refresh || busy) {
                None
            } else {
                Some(match state.sessions.get_mut(&key) {
                    Some(session) => {
                        // Read afresh: the runtime's replay is the record.
                        session.record.clear();
                        session.opening = true;
                        true
                    }
                    None => {
                        let mut session = Session::new(&member.id, &runtime_agent);
                        session.opening = true;
                        dropped = Self::insert(&mut state, key.clone(), session);
                        false
                    }
                })
            }
        };
        self.close_in_agents(dropped);
        let Some(was_open) = was_open else {
            return self
                .snapshot(&key, how, None)
                .ok_or_else(|| ErrorObject::new(code::NOT_FOUND, "That conversation was closed."));
        };
        let (params, _) = self.with_tools(&member, &runtime_agent, agent, Some(&session_id), params).await;
        match member.backend.request(&runtime_agent, how.method(), params).await {
            Ok(mut result) => {
                if let Some(folder) = member.backend.session_folder(&runtime_agent, &session_id) {
                    result["_meta"][META_CWD] = json!(folder.display().to_string());
                }
                {
                    let mut state = self.lock();
                    if let Some(session) = state.sessions.get_mut(&key) {
                        session.opening = false;
                        session.answered(&result);
                    }
                }
                self.snapshot(&key, how, Some(result)).ok_or_else(|| {
                    ErrorObject::new(code::NOT_FOUND, "That conversation was closed.")
                })
            }
            Err(e) => {
                let mut state = self.lock();
                match state.sessions.get_mut(&key) {
                    Some(session) if was_open => session.opening = false,
                    _ => {
                        Self::forget(&mut state, &key);
                    }
                }
                Err(e)
            }
        }
    }

    /// The session as a client attaching to it gets it, if it is open.
    fn snapshot(&self, key: &Key, how: Open, response: Option<Value>) -> Option<Opened> {
        let mut state = self.lock();
        state.tick += 1;
        let tick = state.tick;
        let session = state.sessions.get_mut(key).filter(|s| !s.opening)?;
        session.used = tick;
        let record = match how {
            Open::Load => session.record.clone(),
            Open::Resume => Vec::new(),
        };
        let turn = session.latest_turn();
        let response = response.unwrap_or_else(|| session.current());
        let pending = state
            .pending
            .iter()
            .filter(|p| p.request.agent == key.0 && p.request.session_id == key.1)
            .map(|p| p.request.clone())
            .collect();
        Some(Opened {
            seq: state.seq,
            record,
            turn,
            response,
            pending,
        })
    }

    /// Starts `agent`'s runtime if it isn't running (a coding agent's
    /// process), so what the host says about it (its capabilities, whether
    /// it answers) is current. The error says why it can't.
    pub async fn ready_agent(&self, agent: &str) -> Result<(), String> {
        let (member, _) = self.locate(agent).await.map_err(|e| e.message)?;
        member.backend.ready().await
    }

    /// Whether `agent`'s runtime keeps sessions that change outside the host
    /// ([`crate::backend::Backend::shared_sessions`]).
    pub async fn shared_sessions(&self, agent: &str) -> bool {
        self.roster.locate(agent).await.is_ok_and(|(member, _)| member.backend.shared_sessions())
    }

    /// Whether the session is open in the host.
    pub fn is_open(&self, agent: &str, session: &str) -> bool {
        self.lock()
            .sessions
            .get(&(agent.to_owned(), session.to_owned()))
            .is_some_and(|s| !s.opening)
    }

    /// The session's current modes (ACP `SessionModeState`), when it has
    /// modes and is open.
    pub fn modes(&self, agent: &str, session: &str) -> Option<Value> {
        self.lock()
            .sessions
            .get(&(agent.to_owned(), session.to_owned()))
            .and_then(|s| s.modes.clone())
    }

    /// The folder the conversation `session` of `agent` works in: where it
    /// moved, else its agent's.
    pub async fn folder(&self, agent: &str, session: &str) -> Option<String> {
        let (member, moved) = {
            let state = self.lock();
            let s = state.sessions.get(&(agent.to_owned(), session.to_owned()));
            (s.map(|s| (s.member.clone(), s.runtime_agent.clone())), s.and_then(|s| s.folder.clone()))
        };
        if moved.is_some() {
            return moved;
        }
        let (member, runtime_agent) = match member {
            Some(found) => found,
            None => self.locate(agent).await.ok().map(|(m, r)| (m.id, r))?,
        };
        if let Some(folder) = self.backend(&member).and_then(|b| b.session_folder(&runtime_agent, session)) {
            return Some(folder.display().to_string());
        }
        self.current().await.into_iter().find(|a| a.id == agent).and_then(|a| a.folder)
    }

    /// ACP `session/list` on `agent`.
    pub async fn list_sessions(&self, agent: &str, params: Value) -> Result<Value, ErrorObject> {
        let (member, runtime_agent) = self.locate(agent).await?;
        member.backend.request(&runtime_agent, "session/list", params).await
    }

    /// ACP `session/set_mode`, `session/set_config_option`, `session/close`
    /// or `session/delete` on an open session. A mode or option changed is
    /// recorded and told to every other client (`client` caused it); a
    /// session closed or deleted is let go.
    pub async fn change_session(
        &self,
        agent: &str,
        method: &str,
        params: Value,
        client: Option<ClientId>,
    ) -> Result<Value, ErrorObject> {
        let session_id = params["sessionId"].as_str().unwrap_or("").to_owned();
        let key = (agent.to_owned(), session_id.clone());
        let (member, runtime_agent) = {
            let state = self.lock();
            let session = state.sessions.get(&key).filter(|s| !s.opening).ok_or_else(not_open)?;
            (session.member.clone(), session.runtime_agent.clone())
        };
        let backend = self
            .backend(&member)
            .ok_or_else(|| ErrorObject::new(code::UNKNOWN_AGENT, format!("There's no agent called {agent} on this computer.")))?;
        let result = backend.request(&runtime_agent, method, params.clone()).await?;
        let mut state = self.lock();
        let update = match method {
            "session/set_mode" => Some(json!({ "sessionUpdate": "current_mode_update", "currentModeId": params["modeId"] })),
            "session/set_config_option" if result["configOptions"].is_array() => {
                Some(json!({ "sessionUpdate": "config_option_update", "configOptions": result["configOptions"] }))
            }
            "session/close" | "session/delete" => {
                if let Some(Some(tools)) = self.tools.get() {
                    tools.forget_session(&key.0, Some(&key.1));
                }
                self.resolve_session(&mut state, &key, None);
                Self::forget(&mut state, &key);
                Self::emit(&mut state, &self.events, Event::Closed { agent: key.0.clone(), session_id });
                None
            }
            _ => None,
        };
        if let Some(update) = update {
            self.record(&mut state, &key, update, client);
        }
        Ok(result)
    }

    /// Records an update on an open session and announces it.
    fn record(&self, state: &mut State, key: &Key, update: Value, skip: Option<ClientId>) {
        let Some(session) = state.sessions.get_mut(key) else { return };
        session.apply(&update);
        let at = session.turn.is_some().then(unix_now);
        session.record.push(Recorded { update: update.clone(), at });
        if session.opening {
            return;
        }
        Self::emit(
            state,
            &self.events,
            Event::Update(SessionUpdate {
                agent: key.0.clone(),
                session_id: key.1.clone(),
                update,
                skip,
            }),
        );
    }

    // -- Turns ----------------------------------------------------------------

    /// The turn running on the session, if any.
    pub fn turn(&self, agent: &str, session: &str) -> Option<TurnUpdate> {
        self.lock()
            .sessions
            .get(&(agent.to_owned(), session.to_owned()))
            .and_then(|s| s.turn.as_ref().map(|r| r.turn.clone()))
    }

    // -- Life -----------------------------------------------------------------

    /// Where each agent is in its life and whether it works now
    /// (`host/status`), by the host's agent ids. A runtime that doesn't say
    /// ([`Backend::status`](crate::backend::Backend::status)) runs on its
    /// own: its agents run, and work while a turn of theirs runs.
    pub async fn status(&self) -> Vec<AgentStatus> {
        let members = self.roster.members();
        let ids: Vec<&str> = members.iter().map(|m| m.id.as_str()).collect();
        let mut all = Vec::new();
        for member in &members {
            let Ok(agents) = member.backend.agents().await else {
                continue;
            };
            let named = agent_ids(member, &agents, &ids);
            let reported = member.backend.status().await;
            for (agent, id) in agents.iter().zip(named) {
                let status = match reported.as_ref().and_then(|r| r.iter().find(|s| s.agent == agent.id)) {
                    Some(status) => AgentStatus { agent: id, ..status.clone() },
                    None => self.status_from_turns(id),
                };
                all.push(status);
            }
        }
        all
    }

    /// An agent the host doesn't run itself: running, and working while a
    /// turn of its runs.
    fn status_from_turns(&self, agent: String) -> AgentStatus {
        let sessions: Vec<SessionStatus> = self
            .lock()
            .sessions
            .iter()
            .filter(|((a, _), s)| *a == agent && s.turn.is_some())
            .map(|((_, session), _)| SessionStatus {
                session_id: session.clone(),
                state: Life::Running,
                busy: true,
                why: vec![Working::Prompt],
                last_update: None,
            })
            .collect();
        let busy = !sessions.is_empty();
        AgentStatus {
            agent,
            state: Life::Running,
            busy,
            why: if busy { vec![Working::Prompt] } else { Vec::new() },
            idle_since: None,
            sessions,
        }
    }

    /// Stops what the host runs as it shuts down: a prompt still running
    /// gets up to `grace` to finish, then every agent it runs is stopped, its
    /// sessions kept for the next start. Nothing it started outlives it.
    pub async fn shutdown(&self, grace: Duration) {
        let members = self.roster.members();
        futures::future::join_all(members.iter().map(|m| m.backend.shutdown(grace))).await;
    }

    /// Every running turn.
    pub fn turns(&self) -> Vec<TurnUpdate> {
        self.lock()
            .sessions
            .values()
            .filter_map(|s| s.turn.as_ref().map(|r| r.turn.clone()))
            .collect()
    }

    /// ACP `session/prompt` on an open session: `prompt` is its content
    /// blocks, `by` the device and `client` the client that sent it. The
    /// turn is announced `running` now, each block recorded as the owner's
    /// message and told to every other client, and `ended` when it ends,
    /// however it ends.
    pub fn prompt(
        self: &Arc<Self>,
        agent: &str,
        session: &str,
        prompt: Vec<Value>,
        by: Option<DeviceRef>,
        client: Option<ClientId>,
    ) -> Result<TurnUpdate, ErrorObject> {
        let key = (agent.to_owned(), session.to_owned());
        let turn = TurnUpdate {
            agent: agent.to_owned(),
            session_id: session.to_owned(),
            turn_id: uuid::Uuid::new_v4().to_string(),
            state: TurnState::Running,
            started_at: model::now(),
            by,
            stop_reason: None,
            error: None,
            usage: None,
        };
        let (member, runtime_agent) = {
            let mut state = self.lock();
            let session_state = state.sessions.get_mut(&key).filter(|s| !s.opening).ok_or_else(not_open)?;
            if session_state.turn.is_some() {
                return Err(ErrorObject::new(
                    code::TURN_IN_PROGRESS,
                    format!("{} is still working on the last message. Wait for it or stop it.", self.label(agent)),
                ));
            }
            session_state.turn = Some(Running {
                turn: turn.clone(),
                client,
            });
            let found = (session_state.member.clone(), session_state.runtime_agent.clone());
            Self::emit(&mut state, &self.events, Event::Turn(turn.clone()));
            for block in &prompt {
                self.record(&mut state, &key, json!({ "sessionUpdate": "user_message_chunk", "content": block }), client);
            }
            found
        };
        let host = self.clone();
        let started = turn.clone();
        tokio::spawn(async move {
            let backend = host.backend(&member);
            let answer = match backend {
                Some(backend) => {
                    backend
                        .request(&runtime_agent, "session/prompt", json!({ "sessionId": started.session_id, "prompt": prompt }))
                        .await
                }
                None => Err(ErrorObject::new(
                    code::AGENT_UNAVAILABLE,
                    format!("{} is no longer on this computer.", host.label(&started.agent)),
                )),
            };
            host.end(started, answer);
        });
        Ok(turn)
    }

    /// Frees the turn's session, resolves what it left pending, and
    /// announces the answer and then the turn's end.
    fn end(&self, turn: TurnUpdate, answer: Result<Value, ErrorObject>) {
        let mut state = self.lock();
        let key = (turn.agent.clone(), turn.session_id.clone());
        let client = state
            .sessions
            .get_mut(&key)
            .and_then(|s| s.turn.take())
            .and_then(|r| r.client);
        let left: Vec<String> = state
            .pending
            .iter()
            .filter(|p| p.request.turn_id.as_deref() == Some(turn.turn_id.as_str()))
            .map(|p| p.request.id.clone())
            .collect();
        for id in left {
            self.resolve(&mut state, &id, Some(Outcome::Cancelled), None);
        }
        let (stop_reason, usage, error) = match &answer {
            Ok(result) => (
                Some(StopReason::parse(result["stopReason"].as_str().unwrap_or("end_turn"))),
                usage(result),
                None,
            ),
            Err(error) => (None, None, Some(error.clone())),
        };
        Self::emit(
            &mut state,
            &self.events,
            Event::Answered(Answered {
                agent: turn.agent.clone(),
                session_id: turn.session_id.clone(),
                turn_id: turn.turn_id.clone(),
                client,
                response: answer,
            }),
        );
        let ended = TurnUpdate {
            state: TurnState::Ended,
            stop_reason,
            usage,
            error,
            ..turn
        };
        if let Some(session) = state.sessions.get_mut(&key) {
            session.last_ended = Some(ended.clone());
        }
        Self::emit(&mut state, &self.events, Event::Turn(ended));
    }

    /// ACP `session/cancel` for the session: the agent is told, and its
    /// pending permission requests are answered `cancelled`, as ACP requires
    /// of a client (`by` is the device that cancelled).
    pub fn cancel_session(&self, agent: &str, session: &str, by: Option<DeviceRef>) {
        let key = (agent.to_owned(), session.to_owned());
        let target = {
            let mut state = self.lock();
            let Some(found) = state.sessions.get(&key).map(|s| (s.member.clone(), s.runtime_agent.clone())) else {
                return;
            };
            self.resolve_session(&mut state, &key, by);
            found
        };
        let (member, runtime_agent) = target;
        if let Some(backend) = self.backend(&member) {
            backend.notify(&runtime_agent, "session/cancel", json!({ "sessionId": session }));
        }
    }

    /// Stops the running turns on `session`, or every turn of `agent`, or
    /// every turn. Returns how many were told to stop.
    pub fn cancel(&self, agent: Option<&str>, session: Option<&str>) -> usize {
        let running: Vec<(String, String)> = self
            .turns()
            .into_iter()
            .filter(|t| match (session, agent) {
                (Some(session), Some(agent)) => t.session_id == session && t.agent == agent,
                (Some(session), None) => t.session_id == session,
                (None, Some(agent)) => t.agent == agent,
                (None, None) => true,
            })
            .map(|t| (t.agent, t.session_id))
            .collect();
        for (agent, session) in &running {
            self.cancel_session(agent, session, None);
        }
        running.len()
    }

    // -- Permission requests --------------------------------------------------

    /// Every pending permission request, oldest first (`host/pending`).
    pub fn pending(&self) -> Vec<PendingRequest> {
        self.lock().pending.iter().map(|p| p.request.clone()).collect()
    }

    /// Answers the pending request `id` with `outcome` (`host/answer`, or a
    /// client's answer to its copy); `by` is the device that answered.
    pub fn answer(&self, id: &str, outcome: Outcome, by: Option<DeviceRef>) -> Result<(), ErrorObject> {
        let mut state = self.lock();
        let Some(pending) = state.pending.iter().find(|p| p.request.id == id) else {
            return Err(if state.resolved.iter().any(|r| r == id) {
                ErrorObject::new(code::ALREADY_ANSWERED, "This was already answered on another device.")
            } else {
                ErrorObject::new(code::UNKNOWN_REQUEST, "That request is no longer waiting.")
            });
        };
        if let Outcome::Selected { option_id } = &outcome
            && !pending.request.options.iter().any(|o| &o.option_id == option_id)
        {
            return Err(ErrorObject::new(code::INVALID_PARAMS, "That isn't one of the answers this request offers."));
        }
        self.resolve(&mut state, id, Some(outcome), by);
        Ok(())
    }

    /// Answers every pending request of the session `cancelled`.
    fn resolve_session(&self, state: &mut State, key: &Key, by: Option<DeviceRef>) {
        let ids: Vec<String> = state
            .pending
            .iter()
            .filter(|p| p.request.agent == key.0 && p.request.session_id == key.1)
            .map(|p| p.request.id.clone())
            .collect();
        for id in ids {
            self.resolve(state, &id, Some(Outcome::Cancelled), by.clone());
        }
    }

    /// Takes `id` off the pending list, answers the agent with `outcome`
    /// (none: it took the request back), and announces how it was resolved.
    fn resolve(&self, state: &mut State, id: &str, outcome: Option<Outcome>, by: Option<DeviceRef>) {
        let Some(i) = state.pending.iter().position(|p| p.request.id == id) else {
            return;
        };
        let mut pending = state.pending.remove(i);
        if let (Some(reply), Some(outcome)) = (pending.reply.take(), &outcome) {
            reply.send(json!({ "outcome": outcome }));
        }
        state.resolved.push_back(pending.request.id.clone());
        if state.resolved.len() > RESOLVED_REMEMBERED {
            state.resolved.pop_front();
        }
        Self::emit(
            state,
            &self.events,
            Event::Pending(Box::new(PendingUpdate {
                change: PendingChange::Resolved,
                request: pending.request,
                outcome,
                answered_by: by,
            })),
        );
    }

    // -- From the agents ------------------------------------------------------

    /// What a member's agent sent.
    fn receive(&self, member: &str, message: FromAgent) {
        let FromAgent { agent: runtime_agent, message } = message;
        let mut state = self.lock();
        match message {
            AgentMessage::Update { session_id, update } => {
                let index = (member.to_owned(), runtime_agent, session_id);
                match state.index.get(&index).cloned() {
                    Some(key) => self.record(&mut state, &key, update, None),
                    None => {
                        if state.strays.len() < OPEN_SESSIONS || state.strays.contains_key(&index) {
                            let strays = state.strays.entry(index).or_default();
                            if strays.len() < STRAYS {
                                strays.push(update);
                            }
                        }
                    }
                }
            }
            AgentMessage::Permission {
                session_id,
                params,
                words,
                reply,
            } => {
                let index = (member.to_owned(), runtime_agent, session_id.clone());
                // Nobody can answer for a session the host doesn't hold: the
                // reply drops, and the agent reads `cancelled`.
                let Some(key) = state.index.get(&index).cloned() else { return };
                let turn_id = state.sessions.get(&key).and_then(|s| s.turn.as_ref()).map(|r| r.turn.turn_id.clone());
                let tool_call: ToolCallUpdate = serde_json::from_value(params["toolCall"].clone()).unwrap_or_else(|_| ToolCallUpdate {
                    tool_call_id: params["toolCall"]["toolCallId"].as_str().unwrap_or("").to_owned(),
                    ..ToolCallUpdate::default()
                });
                // One pending request per tool call: an older one for the
                // same call is over.
                let same: Vec<String> = state
                    .pending
                    .iter()
                    .filter(|p| p.request.agent == key.0 && p.request.session_id == key.1 && p.request.tool_call.tool_call_id == tool_call.tool_call_id)
                    .map(|p| p.request.id.clone())
                    .collect();
                for id in same {
                    self.resolve(&mut state, &id, Some(Outcome::Cancelled), None);
                }
                let mut id = if tool_call.tool_call_id.is_empty() { format!("ask-{}", reply.id) } else { tool_call.tool_call_id.clone() };
                while state.pending.iter().any(|p| p.request.id == id) {
                    id.push('+');
                }
                let request = PendingRequest {
                    id,
                    agent: key.0.clone(),
                    session_id,
                    turn_id,
                    tool_call,
                    options: serde_json::from_value(params["options"].clone()).unwrap_or_default(),
                    created_at: model::now(),
                    words: words.unwrap_or_default(),
                    params,
                };
                let reply_id = reply.id;
                state.pending.push(Pending {
                    request: request.clone(),
                    member: member.to_owned(),
                    reply: Some(reply),
                    reply_id,
                });
                Self::emit(
                    &mut state,
                    &self.events,
                    Event::Pending(Box::new(PendingUpdate {
                        change: PendingChange::Added,
                        request,
                        outcome: None,
                        answered_by: None,
                    })),
                );
            }
            AgentMessage::Withdrawn { reply } => {
                let found = state
                    .pending
                    .iter()
                    .find(|p| p.member == member && p.reply_id == reply)
                    .map(|p| p.request.id.clone());
                if let Some(id) = found {
                    self.resolve(&mut state, &id, None, None);
                }
            }
        }
    }

    /// The model the session runs on, or the agent's current one.
    pub async fn model(&self, agent: &str, session: Option<&str>) -> Result<String, Error> {
        if let Some(session) = session
            && let Some(model) = self
                .lock()
                .sessions
                .get(&(agent.to_owned(), session.to_owned()))
                .and_then(|s| s.model.clone())
        {
            return Ok(model);
        }
        let (member, runtime_agent) = self.roster.locate(agent).await?;
        self.ensure(&member);
        member.backend.model(&runtime_agent, session).await
    }

    /// The name of the member hosting `agent`, for the owner's messages.
    pub fn label(&self, agent: &str) -> String {
        let members = self.roster.members();
        members
            .iter()
            .find(|m| m.id == agent)
            .or_else(|| {
                members
                    .iter()
                    .filter(|m| agent.strip_prefix(m.id.as_str()).is_some_and(|rest| rest.starts_with('-')))
                    .max_by_key(|m| m.id.len())
            })
            .or_else(|| members.iter().find(|m| m.id == crate::PRIMARY))
            .map(|m| m.label.clone())
            .unwrap_or_else(|| agent.to_owned())
    }
}

/// The folder `path` names, for a conversation working in `here`: `~` is
/// `home`, a relative path is inside `here`. It must exist unless `create`
/// (then it is made), be a folder, and be inside `home` unless
/// `full_access`. The error is plain, for the agent to relay.
pub fn resolve_folder(path: &str, home: Option<&Path>, here: &Path, create: bool, full_access: bool) -> Result<PathBuf, String> {
    let path = path.trim();
    let named = match (path, path.strip_prefix("~/"), home) {
        ("~", _, Some(home)) => home.to_path_buf(),
        (_, Some(rest), Some(home)) => home.join(rest),
        _ if path.starts_with('~') => return Err(format!("{path} isn't a folder on this computer. Name it with its full path.")),
        _ => here.join(path),
    };
    if !named.exists() {
        if !create {
            return Err(format!(
                "There's no folder {}. If the owner asked for a new folder, call move_to_folder again with create: true.",
                named.display()
            ));
        }
        std::fs::create_dir_all(&named).map_err(|e| format!("Could not make the folder {}: {e}", named.display()))?;
    }
    let folder = named.canonicalize().map_err(|e| format!("Could not use the folder {}: {e}", named.display()))?;
    if !folder.is_dir() {
        return Err(format!("{} is a file, not a folder.", folder.display()));
    }
    if !full_access {
        let home = home
            .and_then(|h| h.canonicalize().ok())
            .ok_or("This user has no home folder, so only an employee with Full access can move.")?;
        if !folder.starts_with(&home) {
            return Err(format!(
                "{} is outside the home folder ({}). In this permission mode the work stays inside the home folder; with Full access it can go anywhere.",
                folder.display(),
                home.display()
            ));
        }
    }
    Ok(folder)
}

/// A folder as the owner reads it: under the home folder, from `~`.
fn shown_folder(folder: &Path, home: Option<&Path>) -> String {
    let home = home.and_then(|h| h.canonicalize().ok());
    match home.as_deref().and_then(|h| folder.strip_prefix(h).ok()) {
        Some(rest) if rest.as_os_str().is_empty() => "~".to_owned(),
        Some(rest) => format!("~/{}", rest.display()),
        None => folder.display().to_string(),
    }
}

fn not_open() -> ErrorObject {
    ErrorObject::new(code::NOT_FOUND, "Load the session on this connection first.")
}

/// A turn's tokens as its `PromptResponse`'s `usage` reports them (ACP's
/// unstable `usage`, which the Claude Code and Codex adapters send).
fn usage(result: &Value) -> Option<Usage> {
    let usage = result.get("usage").filter(|u| u.is_object())?;
    let count = |key: &str| usage[key].as_u64();
    Some(Usage {
        input_tokens: count("inputTokens").unwrap_or(0),
        output_tokens: count("outputTokens").unwrap_or(0),
        thought_tokens: count("thoughtTokens"),
        cached_read_tokens: count("cachedReadTokens"),
        cached_write_tokens: count("cachedWriteTokens"),
        total_tokens: count("totalTokens"),
        cost: serde_json::from_value(usage["cost"].clone()).ok(),
    })
}

fn unix_now() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_folder_to_move_to_is_the_owners_and_exists() {
        let root = tempfile::tempdir().unwrap();
        let home = root.path().join("home");
        let foo = home.join("workspaces").join("foo");
        std::fs::create_dir_all(&foo).unwrap();
        let home = home.canonicalize().unwrap();
        let here = home.join("NeboAI").join("claude-code");
        let foo = foo.canonicalize().unwrap();
        assert_eq!(resolve_folder("~/workspaces/foo", Some(&home), &here, false, false).unwrap(), foo);
        assert_eq!(resolve_folder(&foo.display().to_string(), Some(&home), &here, false, false).unwrap(), foo);
        let missing = resolve_folder("~/workspaces/bar", Some(&home), &here, false, false).unwrap_err();
        assert!(missing.starts_with("There's no folder") && missing.contains("create: true"), "{missing}");
        assert!(!home.join("workspaces/bar").exists(), "nothing is made unless asked");
        let made = resolve_folder("~/workspaces/bar", Some(&home), &here, true, false).unwrap();
        assert!(made.is_dir() && made.ends_with("workspaces/bar"));
        let outside = root.path().join("elsewhere");
        std::fs::create_dir_all(&outside).unwrap();
        let refused = resolve_folder(&outside.display().to_string(), Some(&home), &here, false, false).unwrap_err();
        assert!(refused.contains("outside the home folder") && refused.contains("Full access"), "{refused}");
        assert_eq!(
            resolve_folder(&outside.display().to_string(), Some(&home), &here, false, true).unwrap(),
            outside.canonicalize().unwrap()
        );
        std::fs::write(home.join("notes.txt"), "x").unwrap();
        assert!(resolve_folder("~/notes.txt", Some(&home), &here, false, false).unwrap_err().ends_with("is a file, not a folder."));
        assert_eq!(shown_folder(&foo, Some(&home)), "~/workspaces/foo");
        assert_eq!(shown_folder(&outside, Some(&home)), outside.display().to_string());
    }

    #[test]
    fn usage_reads_the_prompt_response() {
        let result = json!({ "stopReason": "end_turn", "usage": { "inputTokens": 12, "outputTokens": 5, "totalTokens": 17,
            "cost": { "amount": 0.01, "currency": "USD" } } });
        let usage = usage(&result).unwrap();
        assert_eq!((usage.input_tokens, usage.output_tokens, usage.total_tokens), (12, 5, Some(17)));
        assert_eq!(usage.cost.unwrap().currency, "USD");
        assert_eq!(super::usage(&json!({ "stopReason": "end_turn" })), None);
    }
}
