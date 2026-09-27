//! The ACP backend: one agent process (Claude Code, Codex, Gemini CLI,
//! OpenCode, or any command that speaks ACP) driven over stdio
//! ([`nebo_runtimes::acp`]). The host is its only ACP client: requests go
//! to it as the host sends them, and what it sends back (updates, permission
//! requests, requests it takes back) goes to the host's inbox unchanged.
//!
//! - **The process.** Started on first use (the readiness probe is the first
//!   use, so it runs while the host does) in the agent's folder, kept
//!   running, and started again on the next use after it exits: the probe
//!   every 30 s is what brings a crashed agent back. Its stderr goes to its
//!   log file. Starting runs detached from the caller, so a probe
//!   that times out while `npx` fetches the adapter doesn't kill it. The
//!   host initializes it with no file system, no terminal and no
//!   elicitation, so it uses its own tools on its own computer.
//! - **Sessions** outlive the process: a request for a session the running
//!   process hasn't opened (it restarted) first reopens it, with
//!   `session/load` (whose replay the host already has, so it is not handed
//!   on) or else `session/resume`.
//! - **Chats.** `session/list` goes to the agent where it serves it (so a
//!   conversation begun in the terminal there shows too); for one that
//!   doesn't, the backend answers from its own record of the sessions it
//!   created.
//! - **Moving.** A conversation moves to another folder when the owner asks
//!   ([`Backend::move_session`]): the agent starts a new session there, and
//!   the conversation continues in it under its own id. The backend keeps
//!   which session each moved conversation continues in, its folder and the
//!   handoff its next prompt starts with (`acp-moves.json` beside the chats
//!   record), so a restart finds it where it went.
//! - **Sign-in.** The agent runs under its owner's own login. `-32000`
//!   "Authentication required" reads "Claude Code isn't signed in on this
//!   computer. Run `claude` once to sign in.", with the agent's text in
//!   `data.detail`.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime};

use nebo_runtimes::RuntimeCommand;
use nebo_runtimes::acp::Agent as AcpAgent;
use nebo_runtimes::acp::client::{self, AUTH_REQUIRED, CLOSED, Connection, Incoming, METHOD_NOT_FOUND, Responder, RpcError};
use nebo_runtimes::acp::protocol::{self, Initialized};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::backend::{Agent, AgentMessage, Backend, BoxFuture, Error, ErrorObject, FromAgent, Inbox, Reply};
use crate::model::{SessionMode as ModeInfo, SessionModeState, code};

/// How long the agent gets to answer `initialize`: `npx` may be fetching the
/// adapter on a first start.
const START_TIMEOUT: Duration = Duration::from_secs(180);

/// How many chats the host's own record keeps (agents without
/// `session/list`).
const RECORDED_CHATS: usize = 200;

/// Who drives the agent, as `initialize` introduces it: the host software's
/// name and version (`nebo-link`, `nebo`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Client {
    pub name: &'static str,
    pub version: &'static str,
}

/// An ACP agent a host runs, as it keeps it: the command that starts it
/// speaking ACP (with absolute paths, since a background service's `PATH`
/// is not the owner's shell's) and the folder its conversations work in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcpLink {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    /// The folder its conversations work in.
    pub workdir: PathBuf,
}

impl AcpLink {
    pub fn command(&self) -> RuntimeCommand {
        RuntimeCommand {
            program: self.program.clone(),
            args: self.args.clone(),
            env: self.env.clone(),
        }
    }
}

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
    /// The host's record of the chats it created.
    pub chats_file: PathBuf,
    pub client: Client,
}

/// A linked ACP agent.
pub struct Acp {
    shared: Arc<Shared>,
}

impl Acp {
    pub fn new(settings: Settings) -> Self {
        Self {
            shared: Arc::new(Shared {
                live: tokio::sync::Mutex::new(None),
                opening: tokio::sync::Mutex::new(()),
                known: Mutex::new(Known::default()),
                inbox: Mutex::new(None),
                next_reply: AtomicU64::new(0),
                moves: Mutex::new(read_json(&moves_file(&settings.chats_file)).unwrap_or_default()),
                mcp: Mutex::new(HashMap::new()),
                settings,
            }),
        }
    }

    /// Runs `work` on its own task, so a caller that gives up (a probe's
    /// timeout, a phone that hung up) never leaves a start or a session
    /// load half done.
    async fn detached<T, F, Fut>(&self, work: F) -> Result<T, ErrorObject>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Shared>) -> Fut,
        Fut: Future<Output = Result<T, ErrorObject>> + Send + 'static,
    {
        tokio::spawn(work(self.shared.clone()))
            .await
            .unwrap_or_else(|e| Err(ErrorObject::new(code::AGENT_UNAVAILABLE, e.to_string())))
    }
}

struct Shared {
    settings: Settings,
    live: tokio::sync::Mutex<Option<Arc<Live>>>,
    /// One session reopened at a time, so none is reopened twice.
    opening: tokio::sync::Mutex<()>,
    known: Mutex<Known>,
    inbox: Mutex<Option<Inbox>>,
    next_reply: AtomicU64,
    /// The conversations that moved to another folder, by their id.
    moves: Mutex<HashMap<String, Moved>>,
    /// The MCP servers each of the agent's sessions was opened with, so a
    /// session reopened after a restart has them again.
    mcp: Mutex<HashMap<String, Value>>,
}

/// Where a conversation that moved works now.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Moved {
    /// The agent's session it continues in.
    session: String,
    /// The folder that session works in.
    folder: PathBuf,
    /// The note its next prompt starts with, until that prompt is sent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    handoff: Option<String>,
    /// The sessions it continued in before, oldest first.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    earlier: Vec<String>,
}

/// What the agent last said about itself, for its roster entry.
#[derive(Default)]
struct Known {
    /// `agentCapabilities` from its last `initialize`.
    capabilities: Option<Value>,
    /// The modes its last new session started in.
    modes: Option<SessionModeState>,
    /// Why its last start failed; cleared when it starts.
    failed: Option<String>,
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
    /// The sessions this process has open.
    open: HashSet<String>,
    /// Sessions being reopened after a restart: their replay is the host's
    /// already.
    reopening: HashSet<String>,
    /// The model the agent last said it runs, and each session's.
    model: Option<String>,
    models: HashMap<String, String>,
    titles: HashMap<String, String>,
    /// Each permission request of the agent's still open: its JSON-RPC id
    /// (as text) to the reply the host was given.
    asks: HashMap<String, u64>,
}

impl Shared {
    fn name(&self) -> &str {
        &self.settings.name
    }

    fn key(&self) -> &'static str {
        self.settings.agent.key()
    }

    /// The agent's session the conversation `id` continues in: its own, or
    /// the one it moved to.
    fn target(&self, id: &str) -> String {
        self.moves
            .lock()
            .expect("moves")
            .get(id)
            .map(|m| m.session.clone())
            .unwrap_or_else(|| id.to_owned())
    }

    /// The conversation the agent's session `session` belongs to.
    fn conversation(&self, session: &str) -> String {
        self.moves
            .lock()
            .expect("moves")
            .iter()
            .find(|(_, m)| m.session == session || m.earlier.iter().any(|e| e == session))
            .map(|(id, _)| id.clone())
            .unwrap_or_else(|| session.to_owned())
    }

    /// Every session of the agent's the conversation `id` has run in.
    fn sessions_of(&self, id: &str) -> Vec<String> {
        let mut all = vec![id.to_owned()];
        if let Some(moved) = self.moves.lock().expect("moves").get(id) {
            all.extend(moved.earlier.iter().cloned());
            all.push(moved.session.clone());
        }
        all
    }

    /// The folder the agent's session `session` works in.
    fn folder_of(&self, session: &str) -> PathBuf {
        self.moves
            .lock()
            .expect("moves")
            .values()
            .find(|m| m.session == session)
            .map(|m| m.folder.clone())
            .unwrap_or_else(|| self.settings.workdir.clone())
    }

    fn save_moves(&self) {
        let moves = self.moves.lock().expect("moves").clone();
        if let Err(e) = write_private_json(&moves_file(&self.settings.chats_file), &moves) {
            tracing::info!(error = %e, "could not record where a conversation moved");
        }
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
        let started = self.start().await;
        let mut known = self.known.lock().expect("known");
        let live = match started {
            Ok((live, capabilities)) => {
                known.capabilities = Some(capabilities);
                known.failed = None;
                Arc::new(live)
            }
            Err(e) => {
                known.failed = Some(sentence(&e, self.name()));
                return Err(e);
            }
        };
        drop(known);
        *slot = Some(live.clone());
        Ok(live)
    }

    /// The agent started, and the capabilities it answered `initialize` with.
    async fn start(self: &Arc<Self>) -> Result<(Live, Value), Error> {
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
        let me = Arc::downgrade(self);
        let (child, conn) = client::spawn(
            &settings.command,
            &settings.workdir,
            stderr,
            Box::new(move |incoming, responder| handle(&me, &handler_state, incoming, responder)),
        )
        .map_err(|e| Error::Unavailable(format!("could not start {}: {e}", self.name())))?;
        let answered = tokio::time::timeout(
            START_TIMEOUT,
            conn.request(
                "initialize",
                protocol::initialize_params(settings.client.name, settings.client.version),
            ),
        )
        .await;
        let (init, capabilities) = match answered {
            Ok(Ok(result)) => (Initialized::parse(&result), result["agentCapabilities"].clone()),
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
                "{} speaks ACP version {}, and {} speaks version {}. Update both.",
                self.name(),
                init.protocol_version,
                settings.client.name,
                protocol::PROTOCOL_VERSION
            )));
        }
        tracing::info!(agent = settings.agent.key(), title = ?init.title, "the ACP agent started");
        let capabilities = if capabilities.is_object() { capabilities } else { json!({}) };
        Ok((
            Live {
                conn,
                _child: child,
                init,
                state,
            },
            capabilities,
        ))
    }

    /// Hands a message to the host; `false` when no host is connected.
    fn tell(&self, message: AgentMessage) -> bool {
        let inbox = self.inbox.lock().expect("inbox").clone();
        match inbox {
            Some(inbox) => {
                inbox(FromAgent {
                    agent: self.key().to_owned(),
                    message,
                });
                true
            }
            None => false,
        }
    }

    /// What an error answer means, as the host passes it on: unchanged,
    /// except a sign-in the owner must do and an agent that stopped.
    fn refusal(&self, e: RpcError) -> ErrorObject {
        match e.code {
            AUTH_REQUIRED => ErrorObject::new(AUTH_REQUIRED, self.settings.agent.sign_in(self.name()))
                .with_data(json!({ "detail": e.message })),
            CLOSED => ErrorObject::new(
                code::AGENT_UNAVAILABLE,
                format!("Could not connect to {}. Try again.", self.name()),
            ),
            code => ErrorObject::new(code, e.message),
        }
    }

    /// Makes `session` a session of the running agent: reopened with
    /// `session/load` (its replay not handed on) or `session/resume` if
    /// this process has not seen it.
    async fn ensure_open(&self, live: &Live, session: &str) -> Result<(), ErrorObject> {
        if live.state.lock().expect("sessions").open.contains(session) {
            return Ok(());
        }
        let _one = self.opening.lock().await;
        if live.state.lock().expect("sessions").open.contains(session) {
            return Ok(());
        }
        let method = if live.init.load_session {
            "session/load"
        } else if live.init.resume_session {
            "session/resume"
        } else {
            return Err(ErrorObject::new(
                code::NOT_FOUND,
                format!("{} can't reopen a conversation after it restarts. Start a new chat.", self.name()),
            ));
        };
        live.state.lock().expect("sessions").reopening.insert(session.to_owned());
        let answered = live
            .conn
            .request(
                method,
                json!({
                    "sessionId": session,
                    "cwd": self.folder_of(session),
                    "mcpServers": self.mcp.lock().expect("mcp").get(session).cloned().unwrap_or_else(|| json!([])),
                }),
            )
            .await;
        let mut state = live.state.lock().expect("sessions");
        state.reopening.remove(session);
        let result = answered.map_err(|e| self.refusal(e))?;
        state.open.insert(session.to_owned());
        if let Some(model) = protocol::model(&result) {
            state.models.insert(session.to_owned(), model.clone());
            state.model = Some(model);
        }
        Ok(())
    }

    async fn request(self: Arc<Self>, method: String, mut params: Value) -> Result<Value, ErrorObject> {
        let live = self.live().await.map_err(|e| {
            ErrorObject::new(code::AGENT_UNAVAILABLE, sentence(&e, self.name()))
        })?;
        // The conversation the host names, and the agent's session it
        // continues in (another, once it moved).
        let session = params["sessionId"].as_str().map(str::to_owned);
        let target = session.as_deref().map(|s| self.target(s));
        if let Some(target) = &target {
            params["sessionId"] = json!(target);
        }
        let mut handoff = false;
        match method.as_str() {
            "session/list" if !live.init.list_sessions => return Ok(self.recorded_sessions()),
            "session/list" => {
                if params.get("cwd").is_none_or(Value::is_null) {
                    params["cwd"] = json!(self.settings.workdir);
                }
            }
            "session/new" => {}
            "session/load" | "session/resume" => {
                if let (Some(session), Some(target)) = (&session, &target)
                    && session != target
                {
                    params["cwd"] = json!(self.folder_of(target));
                }
            }
            _ => {
                if let Some(target) = &target {
                    self.ensure_open(&live, target).await?;
                }
                // A moved conversation's first prompt starts with the
                // handoff its agent wrote before the move.
                if method == "session/prompt"
                    && let Some(note) = session.as_deref().and_then(|s| self.moves.lock().expect("moves").get(s).and_then(|m| m.handoff.clone()))
                    && let Some(prompt) = params["prompt"].as_array_mut()
                {
                    prompt.insert(0, json!({ "type": "text", "text": handoff_text(&note) }));
                    handoff = true;
                }
            }
        }
        let servers = params.get("mcpServers").cloned();
        let result = live.conn.request(&method, params).await.map_err(|e| self.refusal(e))?;
        let mut state = live.state.lock().expect("sessions");
        let session = session.or_else(|| result["sessionId"].as_str().map(str::to_owned));
        let target = target.or_else(|| session.clone());
        match (method.as_str(), &session, &target) {
            ("session/new" | "session/load" | "session/resume", Some(session), Some(target)) => {
                state.open.insert(target.clone());
                if let Some(servers) = servers {
                    self.mcp.lock().expect("mcp").insert(target.clone(), servers);
                }
                if let Some(model) = protocol::model(&result) {
                    state.models.insert(session.clone(), model.clone());
                    state.model = Some(model);
                }
                if method == "session/new" {
                    if let Some(modes) = mode_state(&result) {
                        self.known.lock().expect("known").modes = Some(modes);
                    }
                    record(&self.settings.chats_file, session, None);
                }
            }
            ("session/prompt", Some(session), _) => {
                record(&self.settings.chats_file, session, state.titles.get(session).map(String::as_str));
                if handoff {
                    if let Some(moved) = self.moves.lock().expect("moves").get_mut(session) {
                        moved.handoff = None;
                    }
                    self.save_moves();
                }
            }
            ("session/close" | "session/delete", Some(session), Some(target)) => {
                state.open.remove(target);
                if self.moves.lock().expect("moves").remove(session).is_some() {
                    self.save_moves();
                }
            }
            ("session/list", _, _) => {
                drop(state);
                return Ok(self.moved_in_list(result));
            }
            _ => {}
        }
        Ok(result)
    }

    /// `session/list`'s answer as the host tells it: a moved conversation
    /// listed with the folder it works in now, and the sessions it moved
    /// into not listed as conversations of their own.
    fn moved_in_list(&self, mut result: Value) -> Value {
        let moves = self.moves.lock().expect("moves").clone();
        if moves.is_empty() {
            return result;
        }
        if let Some(sessions) = result["sessions"].as_array_mut() {
            sessions.retain(|s| {
                let id = s["sessionId"].as_str().unwrap_or("");
                !moves.values().any(|m| m.session == id || m.earlier.iter().any(|e| e == id))
            });
            for session in sessions.iter_mut() {
                if let Some(moved) = session["sessionId"].as_str().and_then(|id| moves.get(id)) {
                    session["cwd"] = json!(moved.folder);
                }
            }
        }
        result
    }

    /// Starts the agent's new session in `folder` for the conversation `id`,
    /// which continues in it from its next prompt.
    async fn move_to(self: Arc<Self>, id: String, folder: PathBuf, handoff: String, mcp: Vec<Value>) -> Result<(), ErrorObject> {
        let live = self.live().await.map_err(|e| {
            ErrorObject::new(code::AGENT_UNAVAILABLE, sentence(&e, self.name()))
        })?;
        let servers = json!(mcp);
        let created = live
            .conn
            .request("session/new", json!({ "cwd": folder, "mcpServers": servers }))
            .await
            .map_err(|e| self.refusal(e))?;
        let session = created["sessionId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ErrorObject::new(code::INTERNAL, format!("{} started a conversation without an id.", self.name())))?
            .to_owned();
        live.state.lock().expect("sessions").open.insert(session.clone());
        self.mcp.lock().expect("mcp").insert(session.clone(), servers);
        {
            let mut moves = self.moves.lock().expect("moves");
            let moved = moves.entry(id.clone()).or_default();
            if !moved.session.is_empty() {
                let before = std::mem::take(&mut moved.session);
                moved.earlier.push(before);
            }
            moved.session = session.clone();
            moved.folder = folder.clone();
            moved.handoff = Some(handoff).filter(|h| !h.trim().is_empty());
        }
        self.save_moves();
        tracing::info!(conversation = %id, session = %session, folder = %folder.display(), "acp: a conversation moved to another folder");
        Ok(())
    }

    /// `session/list`'s answer from the backend's own record, for an agent
    /// that doesn't list its sessions.
    fn recorded_sessions(&self) -> Value {
        let sessions: Vec<Value> = recorded(&self.settings.chats_file)
            .into_iter()
            .map(|r| {
                json!({
                    "cwd": self.moves.lock().expect("moves").get(&r.id).map(|m| m.folder.clone()).unwrap_or_else(|| self.settings.workdir.clone()),
                    "sessionId": r.id,
                    "title": r.title,
                    "updatedAt": crate::model::rfc3339(r.updated as i64, (r.updated.fract() * 1000.0) as u32),
                })
            })
            .collect();
        json!({ "sessions": sessions })
    }

    async fn model(self: Arc<Self>, session: Option<String>) -> Result<String, Error> {
        let live = self.live().await?;
        let state = live.state.lock().expect("sessions");
        Ok(session
            .and_then(|s| state.models.get(&s).cloned())
            .or_else(|| state.model.clone())
            .or_else(|| live.init.title.clone())
            .unwrap_or_else(|| self.name().to_owned()))
    }
}

/// An error as the one sentence the owner reads.
fn sentence(e: &Error, name: &str) -> String {
    match e {
        Error::Unavailable(why) => {
            let mut chars = why.chars();
            chars.next().map(|c| c.to_uppercase().chain(chars).collect()).unwrap_or_default()
        }
        other => other.message(name),
    }
}

/// What the agent sends: updates and permission requests to the host, and
/// the requests it takes back. The agent was offered no file system and no
/// terminal, so every other request is refused.
fn handle(shared: &Weak<Shared>, state: &Arc<Mutex<Sessions>>, incoming: Incoming, responder: &Responder) {
    let Some(shared) = shared.upgrade() else {
        if let Incoming::Request { id, .. } = incoming {
            responder.respond(&id, Ok(protocol::cancelled()));
        }
        return;
    };
    match incoming {
        Incoming::Notification { method, params } if method == "session/update" => {
            let Some(session) = params["sessionId"].as_str().map(str::to_owned) else {
                return;
            };
            if state.lock().expect("sessions").reopening.contains(&session) {
                return;
            }
            let session = shared.conversation(&session);
            {
                let mut state = state.lock().expect("sessions");
                match protocol::update(&params).map(|(_, update)| update) {
                    Some(protocol::Update::Model(model)) => {
                        state.models.insert(session.clone(), model.clone());
                        state.model = Some(model);
                    }
                    Some(protocol::Update::Title(title)) => {
                        state.titles.insert(session.clone(), title);
                    }
                    _ => {}
                }
            }
            shared.tell(AgentMessage::Update {
                session_id: session,
                update: params["update"].clone(),
            });
        }
        Incoming::Notification { method, params } if method == "$/cancel_request" => {
            let reply = state.lock().expect("sessions").asks.remove(&params["requestId"].to_string());
            if let Some(reply) = reply {
                shared.tell(AgentMessage::Withdrawn { reply });
            }
        }
        Incoming::Request { id, method, mut params } if method == "session/request_permission" => {
            let Some(session) = params["sessionId"].as_str().map(|s| shared.conversation(s)) else {
                responder.respond(&id, Err(RpcError::new(-32602, "invalid permission request")));
                return;
            };
            // Asked in the session a conversation moved into: the host and
            // its clients know it by the conversation's id.
            params["sessionId"] = json!(session);
            let reply_id = shared.next_reply.fetch_add(1, Ordering::Relaxed) + 1;
            let (reply, answer) = Reply::new(reply_id);
            state.lock().expect("sessions").asks.insert(id.to_string(), reply_id);
            tracing::info!(session = %session, tool = %params["toolCall"]["toolCallId"], "acp: permission requested; asking the owner");
            let id = id.clone();
            let responder = responder.clone();
            let asks = state.clone();
            tokio::spawn(async move {
                // A reply dropped unanswered (nobody holds the session) is
                // `cancelled`, as ACP requires of a client.
                let response = answer.await.unwrap_or_else(|_| protocol::cancelled());
                asks.lock().expect("sessions").asks.remove(&id.to_string());
                responder.respond(&id, Ok(response));
            });
            if !shared.tell(AgentMessage::Permission {
                session_id: session,
                params,
                words: None,
                reply,
            }) {
                tracing::info!("acp: permission requested with no host to ask; cancelled");
            }
        }
        Incoming::Request { id, method, .. } => {
            tracing::info!(method = %method, "acp: a request the host does not offer; refused");
            responder.respond(
                &id,
                Err(RpcError::new(METHOD_NOT_FOUND, format!("{method} is not offered"))),
            );
        }
        Incoming::Notification { .. } => {}
    }
}

impl Backend for Acp {
    fn ready(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let shared = self.shared.clone();
            tokio::spawn(async move { shared.live().await.map(|_| ()) })
                .await
                .unwrap_or_else(|e| Err(Error::Unavailable(e.to_string())))
                .map_err(|e| match e {
                    Error::Unavailable(why) | Error::Failed(why) | Error::NotFound(why) => why,
                })
        })
    }

    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>> {
        let settings = &self.shared.settings;
        let known = self.shared.known.lock().expect("known");
        let agent = Agent {
            id: settings.agent.key().to_owned(),
            name: settings.name.clone(),
            description: format!("Works in {}", settings.workdir.display()),
            is_default: true,
            folder: Some(settings.workdir.display().to_string()),
            capabilities: known.capabilities.clone().unwrap_or_else(|| json!({})),
            modes: known.modes.clone(),
            offline_reason: known.failed.clone(),
        };
        drop(known);
        Box::pin(async move { Ok(vec![agent]) })
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
            if agent != self.shared.key() {
                return Err(ErrorObject::new(code::UNKNOWN_AGENT, format!("There's no agent called {agent} here.")));
            }
            let method = method.to_owned();
            self.detached(|shared| shared.request(method, params)).await
        })
    }

    fn notify(&self, _agent: &str, method: &str, params: Value) {
        let shared = self.shared.clone();
        let method = method.to_owned();
        tokio::spawn(async move {
            // A notification never starts the agent: with none running there
            // is nothing to tell.
            let live = shared.live.lock().await.clone();
            let Some(live) = live.filter(|l| !l.conn.is_closed()) else {
                return;
            };
            // A conversation that moved may still be finishing its turn in
            // the session it moved from: a cancel reaches each of them.
            let sessions = params["sessionId"].as_str().map(|s| shared.sessions_of(s)).unwrap_or_default();
            if sessions.len() < 2 {
                live.conn.notify(&method, params);
                return;
            }
            for session in sessions {
                let mut params = params.clone();
                params["sessionId"] = json!(session);
                live.conn.notify(&method, params);
            }
        });
    }

    fn model<'a>(
        &'a self,
        agent: &'a str,
        session: Option<&'a str>,
    ) -> BoxFuture<'a, Result<String, Error>> {
        Box::pin(async move {
            if agent != self.shared.key() {
                return Err(Error::NotFound(format!("The agent {agent}")));
            }
            let shared = self.shared.clone();
            let session = session.map(str::to_owned);
            tokio::spawn(shared.model(session))
                .await
                .unwrap_or_else(|e| Err(Error::Unavailable(e.to_string())))
        })
    }

    fn move_session<'a>(
        &'a self,
        agent: &'a str,
        session: &'a str,
        folder: &'a Path,
        handoff: &'a str,
        mcp: Vec<Value>,
    ) -> BoxFuture<'a, Result<(), ErrorObject>> {
        Box::pin(async move {
            if agent != self.shared.key() {
                return Err(ErrorObject::new(code::UNKNOWN_AGENT, format!("There's no agent called {agent} here.")));
            }
            let (session, folder, handoff) = (session.to_owned(), folder.to_path_buf(), handoff.to_owned());
            self.detached(|shared| shared.move_to(session, folder, handoff, mcp)).await
        })
    }

    fn session_folder(&self, agent: &str, session: &str) -> Option<PathBuf> {
        if agent != self.shared.key() {
            return None;
        }
        self.shared.moves.lock().expect("moves").get(session).map(|m| m.folder.clone())
    }
}

/// Where a backend keeps its moved conversations: beside its chats record.
fn moves_file(chats_file: &Path) -> PathBuf {
    chats_file.with_file_name("acp-moves.json")
}

/// The first context of a moved conversation's next prompt.
fn handoff_text(note: &str) -> String {
    format!("[You moved to a new folder at the owner's request. Your note from before the move: {note}]")
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Option<T> {
    serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
}

/// The modes a `session/new` answer says the session starts in.
fn mode_state(result: &Value) -> Option<SessionModeState> {
    let modes = &result["modes"];
    Some(SessionModeState {
        current_mode_id: modes["currentModeId"].as_str()?.to_owned(),
        available_modes: modes["availableModes"]
            .as_array()?
            .iter()
            .filter_map(|m| {
                let id = m["id"].as_str()?.to_owned();
                Some(ModeInfo {
                    name: m["name"].as_str().map(str::to_owned).unwrap_or_else(|| id.clone()),
                    description: m["description"].as_str().map(str::to_owned),
                    id,
                })
            })
            .collect(),
    })
}

/// One chat the host created, for agents that can't list their sessions.
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
    if let Err(e) = write_private_json(file, &all) {
        tracing::info!(error = %e, "could not record the chat");
    }
}

/// Writes `value` as JSON through a temporary file, so a crash never leaves
/// half a file. Readable by the owner only.
fn write_private_json<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp)?;
    file.write_all(serde_json::to_string_pretty(value).expect("chats serialize").as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&tmp, path)
}

/// How long a coding agent's first start may take (`npx` may be fetching
/// its adapter).
const FIRST_START: Duration = START_TIMEOUT;

/// Starts the agent once in `workdir`, proving it speaks ACP, and returns
/// what it calls itself. `name` is its name for the messages.
pub async fn probe(name: &str, command: &RuntimeCommand, workdir: &Path, client: Client) -> Result<Option<String>, String> {
    let mut probe = tokio::process::Command::new(&command.program);
    probe
        .args(&command.args)
        .envs(command.env.iter().cloned())
        .current_dir(workdir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = probe.spawn().map_err(|e| format!("Could not start {name}: {e}"))?;
    let conn = Connection::start(
        child.stdout.take().expect("piped"),
        child.stdin.take().expect("piped"),
        Box::new(|_, _| {}),
    );
    let answered = tokio::time::timeout(
        FIRST_START,
        conn.request("initialize", protocol::initialize_params(client.name, client.version)),
    )
    .await;
    let _ = child.kill().await;
    match answered {
        Ok(Ok(result)) => Ok(Initialized::parse(&result).title),
        Ok(Err(e)) => Err(format!(
            "{name} did not start in ACP mode ({e}). Run `{}` yourself to see why.",
            shown(command)
        )),
        Err(_) => Err(format!(
            "{name} did not answer in ACP mode. Run `{}` yourself to see why.",
            shown(command)
        )),
    }
}

/// A command as the owner would type it.
pub fn shown(command: &RuntimeCommand) -> String {
    std::iter::once(command.program.as_str())
        .chain(command.args.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join(" ")
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
