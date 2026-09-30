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
//! - **Life.** The agent runs in a process group of its own, recorded
//!   beside its chats (`acp-process.json`), so a stop reaches everything it
//!   started and the next start stops what a crashed run left behind. It
//!   pauses once it has been idle for the idle window ([`IDLE_WINDOW`]):
//!   its processes stop and free their memory, its sessions are kept
//!   (`acp-sessions.json`), and the next request for one resumes it with
//!   `session/load` in a new process, never a blank session. It is never
//!   paused while it works ([`Acp::busy`]), as the protocol tells: a prompt
//!   outstanding however long and silent, a tool call running, a permission
//!   request waiting, a plan in progress; the idle window counts from the
//!   last thing it sent. Beyond the protocol, only a process of its using the
//!   CPU works (a build, a test run a turn left running). A process that
//!   merely runs is not work: the agent starts its own for every session it
//!   opens (a process per session, the session's tool servers), at any time
//!   and while a prompt already runs, and they run until the session ends;
//!   counting them would keep it from ever pausing. A prompt that was
//!   cancelled leaves it suspect of being wedged: it pauses as soon as
//!   nothing else works, and resumes fresh. Shutting down, a prompt still
//!   running gets a grace period to finish; then everything pauses.
//! - **Closing.** `session/close` ends a session in the running agent, and
//!   with it what the agent runs for it, where the agent closes sessions
//!   (`sessionCapabilities.close`); elsewhere what it holds goes when it
//!   pauses. A paused agent is never started to close a session, nor a
//!   session reopened to be closed: what it held is gone already.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime};

use nebo_runtimes::RuntimeCommand;
use nebo_runtimes::acp::Agent as AcpAgent;
use nebo_runtimes::acp::client::{self, AUTH_REQUIRED, CLOSED, Connection, Incoming, METHOD_NOT_FOUND, Responder, RpcError};
use nebo_runtimes::acp::protocol::{self, Initialized};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::backend::{Agent, AgentMessage, Backend, BoxFuture, Error, ErrorObject, FromAgent, Inbox, Reply};
use crate::model::{AgentStatus, Life, SessionMode as ModeInfo, SessionModeState, SessionStatus, Working, code};
use crate::process;

/// How long the agent gets to answer `initialize`: `npx` may be fetching the
/// adapter on a first start.
const START_TIMEOUT: Duration = Duration::from_secs(180);

/// How many chats the host's own record keeps (agents without
/// `session/list`).
const RECORDED_CHATS: usize = 200;

/// How long an agent that works on nothing stays running before it pauses
/// (its processes stop and free their memory; its sessions are kept).
pub const IDLE_WINDOW: Duration = Duration::from_secs(10 * 60);
/// How often a running agent is looked at, at most.
const LOOK_EVERY: Duration = Duration::from_secs(30);
/// The share of one CPU core a process of the agent's uses between two looks
/// ([`LOOK_EVERY`] apart, far more than the second `ps` reports CPU time to
/// on Linux) that counts as work: a compiler or a test runner uses all of
/// one, an idle agent's processes a hundredth.
const CPU_WORK: f64 = 0.25;
/// How long the agent's processes get to end when asked, before made to.
const STOP_GRACE: Duration = Duration::from_secs(5);
/// How long a prompt that was cancelled and never ended still counts as
/// work: after it, the agent is wedged, and paused.
const CANCEL_GRACE: Duration = Duration::from_secs(60);

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
    /// The host's record of the chats it created. Its folder keeps the
    /// agent's other records too (its process, its paused sessions).
    pub chats_file: PathBuf,
    pub client: Client,
    /// How long the agent works on nothing before it pauses
    /// ([`IDLE_WINDOW`]).
    pub idle: Duration,
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
                in_use: AtomicUsize::new(0),
                starting: AtomicBool::new(false),
                closing: AtomicBool::new(false),
                settings,
            }),
        }
    }

    /// What the agent works on now, and why: empty when it is idle. The one
    /// check the agent is paused by, and never while it says otherwise. Its
    /// processes are as its last look found them.
    pub async fn busy(&self) -> Busy {
        let live = self.shared.current().await;
        match live {
            Some(live) => self.shared.busy_of(&live, 0),
            None => self.shared.requests(0),
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
    /// Requests to the agent in flight. Taken before the running agent is,
    /// so a pause (which looks at it holding the agent) never stops one.
    in_use: AtomicUsize,
    /// Its process is starting.
    starting: AtomicBool,
    /// The host is shutting down: no prompt is taken, nothing started.
    closing: AtomicBool,
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
    /// The model it last said it runs, for when it is paused.
    model: Option<String>,
}

/// The running agent.
struct Live {
    conn: Arc<Connection>,
    /// Its process, which leads a process group of its own: stopped with
    /// everything under it when the agent pauses.
    pid: u32,
    _group: Group,
    _child: tokio::process::Child,
    init: Initialized,
    state: Arc<Mutex<Sessions>>,
}

/// The agent's process group, killed with everything under it when dropped:
/// a start that fails, or an agent the host lets go without pausing it,
/// leaves nothing running.
struct Group(u32);

impl Drop for Group {
    fn drop(&mut self) {
        if self.0 != 0 {
            process::kill_now(self.0);
        }
    }
}

/// What an agent works on now ([`Acp::busy`]): empty when it is idle.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Busy {
    /// Why the agent works, over all.
    pub why: BTreeSet<Working>,
    /// Why each of its conversations works, by the conversation's id.
    pub sessions: BTreeMap<String, BTreeSet<Working>>,
}

impl Busy {
    pub fn is_idle(&self) -> bool {
        self.why.is_empty()
    }

    fn add(&mut self, session: Option<&str>, why: Working) {
        self.why.insert(why);
        if let Some(session) = session {
            self.sessions.entry(session.to_owned()).or_default().insert(why);
        }
    }
}

/// Since when an agent has been idle, looked at every so often: it is due to
/// pause once idle for its window, counted from the later of the first look
/// that found it idle and the last thing it did (a prompt that ran and ended
/// between two looks counts), never while it works.
#[derive(Debug, Default)]
struct Idle {
    since: Option<Instant>,
}

impl Idle {
    fn due(&mut self, now: Instant, idle: bool, active: Option<Instant>, window: Duration) -> bool {
        if !idle {
            self.since = None;
            return false;
        }
        let first = *self.since.get_or_insert(now);
        let from = active.map_or(first, |active| active.max(first));
        now.saturating_duration_since(from) >= window
    }
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
    /// (as text) to the reply the host was given, and its conversation.
    asks: HashMap<String, (u64, String)>,
    /// Prompts outstanding, by conversation, and when each conversation was
    /// last cancelled.
    prompts: HashMap<String, usize>,
    cancelled: HashMap<String, Instant>,
    /// Tool calls running (`in_progress`) and not finished, by
    /// conversation (a turn's end finishes them: what they left running
    /// shows in the processes).
    tools: HashMap<String, HashSet<String>>,
    /// Conversations whose plan has an entry pending or in progress.
    plans: HashSet<String>,
    /// When it last did anything: a request to it, a prompt started or
    /// ended, anything it sent.
    active: Option<Instant>,
    /// What each of its processes had used at the last look at the CPU, and
    /// when that was (since the Unix epoch).
    usage: BTreeMap<u32, process::Usage>,
    sampled: Option<Duration>,
    /// A process of its used the CPU between the last two looks.
    processes_work: bool,
    /// Since when it has been idle, as its looks found (for `host/status`).
    idle_since: Option<SystemTime>,
    /// When the agent last sent anything in each conversation.
    heard: HashMap<String, SystemTime>,
    /// A prompt was cancelled: the agent may be wedged, so it pauses as soon
    /// as nothing else works, and the next message resumes it fresh.
    suspect: bool,
}

impl Sessions {
    /// Why the agent works now, besides requests in flight: its
    /// conversations' prompts, tools, permission requests and plans, and its
    /// processes as last looked at. A prompt cancelled longer than
    /// [`CANCEL_GRACE`] ago that never ended is not work: the agent is
    /// wedged.
    fn busy(&mut self, now: Instant) -> Busy {
        let mut busy = Busy::default();
        for (session, n) in &self.prompts {
            let abandoned = self.cancelled.get(session).is_some_and(|at| now.duration_since(*at) > CANCEL_GRACE);
            if *n > 0 && abandoned {
                self.suspect = true;
            } else if *n > 0 {
                busy.add(Some(session), Working::Prompt);
            }
        }
        for (session, calls) in &self.tools {
            if !calls.is_empty() {
                busy.add(Some(session), Working::Tool);
            }
        }
        for (_, session) in self.asks.values() {
            busy.add(Some(session), Working::Permission);
        }
        for session in &self.plans {
            busy.add(Some(session), Working::Plan);
        }
        if self.processes_work {
            busy.add(None, Working::Processes);
        }
        busy
    }

    /// Takes in a look at the agent's processes at `now` (since the Unix
    /// epoch): one that used [`CPU_WORK`] of a core since the last look (or
    /// since it started, when it is new) works. One that merely runs doesn't.
    /// The first look only notes what they used. `None`: nothing to look at
    /// here, which is work, since what runs can't be told.
    fn looked(&mut self, tree: Option<&process::Tree>, now: Duration) {
        let Some(tree) = tree else {
            self.processes_work = cfg!(not(unix));
            return;
        };
        if let Some(sampled) = self.sampled {
            self.processes_work = tree.usage.iter().any(|(pid, usage)| {
                let started = Duration::from_secs(usage.started);
                // The same process, or a new one given its pid (`ps` tells
                // when one started to a second or two).
                let (before, from) = match self.usage.get(pid) {
                    Some(before) if before.started.abs_diff(usage.started) <= 2 => (before.cpu, sampled),
                    _ => (Duration::ZERO, started),
                };
                let elapsed = now.saturating_sub(from).max(Duration::from_secs(1));
                usage.cpu.saturating_sub(before).as_secs_f64() >= CPU_WORK * elapsed.as_secs_f64()
            });
        }
        self.usage = tree.usage.clone();
        self.sampled = Some(now);
    }

    /// Takes in a `session/update`: tool calls running and finished, and the
    /// plan.
    fn track(&mut self, session: &str, update: &Value) {
        let status = update["status"].as_str();
        match update["sessionUpdate"].as_str() {
            Some("tool_call" | "tool_call_update") => {
                let Some(call) = update["toolCallId"].as_str() else { return };
                let calls = self.tools.entry(session.to_owned()).or_default();
                // A call runs once it is `in_progress`. One still `pending`
                // (ACP's default: its input still streaming from the model,
                // or waiting on the owner's approval, which is work of its
                // own) has not started, and one completed or failed is done.
                match status {
                    Some("in_progress") => {
                        calls.insert(call.to_owned());
                    }
                    Some(_) => {
                        calls.remove(call);
                    }
                    None => {}
                }
            }
            Some("plan") => {
                let working = update["entries"]
                    .as_array()
                    .is_some_and(|entries| entries.iter().any(|e| matches!(e["status"].as_str(), Some("pending" | "in_progress"))));
                if working {
                    self.plans.insert(session.to_owned());
                } else {
                    self.plans.remove(session);
                }
            }
            _ => {}
        }
    }

    /// A prompt in `session` starts: a cancel from before it is not its.
    fn prompt_started(&mut self, session: &str, now: Instant) {
        *self.prompts.entry(session.to_owned()).or_default() += 1;
        self.cancelled.remove(session);
        self.tools.remove(session);
        self.plans.remove(session);
        self.active = Some(now);
    }

    /// A cancel for `session`: noted only while a prompt of its runs, so a
    /// cancel that came after its turn ended never touches the next one.
    fn cancel(&mut self, session: &str, now: Instant) {
        if self.prompts.get(session).is_some_and(|n| *n > 0) {
            self.cancelled.insert(session.to_owned(), now);
        }
    }

    /// A turn in `session` ended: its tool calls and plan with it.
    fn turn_ended(&mut self, session: &str) {
        self.active = Some(Instant::now());
        if let Some(n) = self.prompts.get_mut(session) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                self.prompts.remove(session);
                self.cancelled.remove(session);
            }
        }
        self.tools.remove(session);
        self.plans.remove(session);
    }
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
        if self.closing.load(Ordering::SeqCst) {
            return Err(Error::Unavailable(format!("could not connect to {}. Try again.", self.name())));
        }
        // A process that ended by itself: its sessions are paused, and
        // reopened in the next one when asked for.
        if let Some(ended) = slot.take() {
            let open: Vec<String> = ended.state.lock().expect("sessions").open.iter().map(|s| self.conversation(s)).collect();
            self.record_sessions(&open, Life::Paused);
        }
        self.starting.store(true, Ordering::SeqCst);
        let started = self.start().await;
        self.starting.store(false, Ordering::SeqCst);
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
        tokio::spawn(watch(Arc::downgrade(self), Arc::downgrade(&live)));
        let (conn, agent, pid) = (live.conn.clone(), self.key(), live.pid);
        tokio::spawn(async move {
            conn.closed().await;
            tracing::info!(agent, pid, "acp: the agent's process ended");
        });
        Ok(live)
    }

    /// The running agent, if it runs; never starts it.
    async fn current(&self) -> Option<Arc<Live>> {
        self.live.lock().await.clone().filter(|l| !l.conn.is_closed())
    }

    /// Why the agent works when nothing runs but `requests` beyond
    /// outstanding prompts.
    fn requests(&self, prompts: usize) -> Busy {
        let mut busy = Busy::default();
        if self.in_use.load(Ordering::SeqCst) > prompts {
            busy.add(None, Working::Request);
        }
        busy
    }

    /// Why the running agent works now ([`Sessions::busy`]), requests in
    /// flight with it but the caller's `own`.
    fn busy_of(&self, live: &Live, own: usize) -> Busy {
        let mut state = live.state.lock().expect("sessions");
        let prompts: usize = state.prompts.values().sum();
        let mut busy = state.busy(Instant::now());
        drop(state);
        let requests = self.requests(prompts + own);
        busy.why.extend(requests.why);
        busy
    }

    /// Looks at the running agent's processes ([`Sessions::looked`]).
    async fn look(&self, live: &Live) {
        let tree = Self::tree(live).await;
        live.state.lock().expect("sessions").looked(tree.as_ref(), Duration::from_secs_f64(now()));
    }

    async fn tree(live: &Live) -> Option<process::Tree> {
        let pid = live.pid;
        tokio::task::spawn_blocking(move || process::tree(pid)).await.ok().flatten()
    }

    /// Pauses the running agent `live`: its processes stop (asked, then made
    /// to) and free their memory; its sessions are kept, and the next
    /// request for one resumes it in a new process. Never while it works
    /// ([`Pause`]). Holding the agent while it stops, so a request that
    /// comes meanwhile starts it afresh after. Returns whether it paused.
    async fn pause(self: &Arc<Self>, live: &Arc<Live>, why: &str, when: Pause) -> bool {
        let mut slot = self.live.lock().await;
        if !slot.as_ref().is_some_and(|l| Arc::ptr_eq(l, live)) {
            return false;
        }
        let allowed = match when {
            Pause::WhenIdle => self.busy_of(live, 0).is_idle(),
            Pause::ForThisRequest => self.busy_of(live, 1).is_idle(),
            Pause::Now => true,
        };
        if !allowed {
            return false;
        }
        slot.take();
        let open: Vec<String> = live.state.lock().expect("sessions").open.iter().map(|s| self.conversation(s)).collect();
        process::stop(live.pid, STOP_GRACE).await;
        self.record_sessions(&open, Life::Paused);
        let _ = std::fs::remove_file(process_file(&self.settings.chats_file));
        tracing::info!(agent = self.key(), pid = live.pid, sessions = open.len(), why, "acp: the agent paused; its sessions resume when asked for");
        true
    }

    /// Records the conversations `sessions` as `state` (paused, or running
    /// again once reopened), so their state outlives this process.
    fn record_sessions(&self, sessions: &[String], state: Life) {
        if sessions.is_empty() {
            return;
        }
        let file = sessions_file(&self.settings.chats_file);
        let mut all: BTreeMap<String, Life> = read_json(&file).unwrap_or_default();
        for session in sessions {
            match state {
                Life::Paused => all.insert(session.clone(), Life::Paused),
                _ => all.remove(session),
            };
        }
        if let Err(e) = write_private_json(&file, &all) {
            tracing::info!(error = %e, "acp: could not record the agent's sessions");
        }
    }

    /// Where the agent is in its life, whether it works, and each session it
    /// holds (`host/status`).
    fn status(&self) -> AgentStatus {
        // Never waits on a start or a pause under way: it says so instead.
        let live = match self.live.try_lock() {
            Ok(slot) => slot.clone().filter(|l| !l.conn.is_closed()),
            Err(_) => None,
        };
        let paused: BTreeMap<String, Life> = read_json(&sessions_file(&self.settings.chats_file)).unwrap_or_default();
        let mut sessions: BTreeMap<String, Life> = paused;
        let (busy, idle_since, heard) = match &live {
            Some(live) => {
                let busy = self.busy_of(live, 0);
                let state = live.state.lock().expect("sessions");
                for session in &state.open {
                    sessions.insert(self.conversation(session), Life::Running);
                }
                for session in &state.reopening {
                    sessions.insert(self.conversation(session), Life::Resuming);
                }
                (busy, state.idle_since, state.heard.clone())
            }
            None => (self.requests(0), None, HashMap::new()),
        };
        let rfc3339 = |at: SystemTime| {
            let at = at.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
            crate::model::rfc3339(at.as_secs() as i64, at.subsec_millis())
        };
        let resuming = sessions.values().any(|s| *s == Life::Resuming);
        let state = match (&live, self.starting.load(Ordering::SeqCst)) {
            _ if resuming => Life::Resuming,
            (None, true) => Life::Starting,
            (Some(_), _) => Life::Running,
            (None, false) => Life::Paused,
        };
        AgentStatus {
            agent: self.key().to_owned(),
            state,
            busy: !busy.is_idle(),
            why: busy.why.iter().copied().collect(),
            idle_since: idle_since.filter(|_| busy.is_idle()).map(rfc3339),
            sessions: sessions
                .into_iter()
                .map(|(session_id, state)| {
                    let why: Vec<Working> = busy.sessions.get(&session_id).map(|w| w.iter().copied().collect()).unwrap_or_default();
                    SessionStatus {
                        busy: !why.is_empty(),
                        why,
                        last_update: heard.get(&session_id).copied().map(rfc3339),
                        session_id,
                        state,
                    }
                })
                .collect(),
        }
    }

    /// Shutting down: no prompt is taken from now on; one still running
    /// gets up to `grace` to finish; then the agent pauses, whatever it does.
    async fn shutdown(self: &Arc<Self>, grace: Duration) {
        self.closing.store(true, Ordering::SeqCst);
        let Some(live) = self.current().await else {
            return;
        };
        let deadline = Instant::now() + grace;
        while Instant::now() < deadline && live.state.lock().expect("sessions").prompts.values().any(|n| *n > 0) {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let unfinished = live.state.lock().expect("sessions").prompts.values().sum::<usize>();
        if unfinished > 0 {
            tracing::info!(agent = self.key(), prompts = unfinished, "acp: shutting down with prompts still running; their sessions resume when asked for");
        }
        self.pause(&live, "shutting down", Pause::Now).await;
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
        // What an earlier run of the agent left running (the host crashed, or
        // was killed before it could stop it) goes before another starts.
        let record = process_file(&settings.chats_file);
        if let Some(left) = read_json::<process::Started>(&record)
            && process::stop_recorded(&left, STOP_GRACE).await
        {
            tracing::info!(agent = settings.agent.key(), pid = left.pid, "acp: stopped what an earlier run of the agent left running");
        }
        let _ = std::fs::remove_file(&record);
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
        let pid = child.id().unwrap_or_default();
        let group = Group(pid);
        if let Err(e) = write_private_json(&record, &process::Started::now(pid)) {
            tracing::info!(error = %e, "acp: could not record the agent's process");
        }
        tracing::info!(agent = settings.agent.key(), pid, command = %shown(&settings.command), "acp: the agent's process started");
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
        let live = Live {
            conn,
            pid,
            _group: group,
            _child: child,
            init,
            state,
        };
        // What its processes used so far, for the next look to tell work by.
        let tree = tokio::task::spawn_blocking(move || process::tree(pid)).await.ok().flatten();
        live.state.lock().expect("sessions").looked(tree.as_ref(), Duration::from_secs_f64(now()));
        Ok((live, capabilities))
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
        let answered = self
            .call(
                live,
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
        // Never a blank session instead: the owner reads why it didn't reopen.
        let result = answered.map_err(|e| match e.code {
            AUTH_REQUIRED | CLOSED => self.refusal(e),
            code => ErrorObject::new(code, format!("{} could not reopen this conversation: {}", self.name(), e.message)),
        })?;
        state.open.insert(session.to_owned());
        if let Some(model) = protocol::model(&result) {
            state.models.insert(session.to_owned(), model.clone());
            state.model = Some(model);
        }
        drop(state);
        self.record_sessions(&[self.conversation(session)], Life::Running);
        Ok(())
    }

    /// One request to the agent, logged with its method, session and how it
    /// ended (never its content).
    async fn call(&self, live: &Live, method: &str, params: Value) -> Result<Value, RpcError> {
        let agent = self.key();
        let session = params["sessionId"].as_str().unwrap_or_default().to_owned();
        let call = self.next_reply.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::info!(agent, method, session, call, "acp: request");
        let began = Instant::now();
        live.state.lock().expect("sessions").active = Some(began);
        let answered = live.conn.request(method, params).await;
        live.state.lock().expect("sessions").active = Some(Instant::now());
        let ms = began.elapsed().as_millis() as u64;
        match &answered {
            Ok(result) => {
                let session = result["sessionId"].as_str().map(str::to_owned).unwrap_or(session);
                tracing::info!(agent, method, session, call, ms, stop_reason = result["stopReason"].as_str(), "acp: answered");
            }
            Err(e) => tracing::info!(agent, method, session, call, ms, code = e.code, error = %e.message, "acp: refused"),
        }
        answered
    }

    async fn request(self: Arc<Self>, method: String, mut params: Value) -> Result<Value, ErrorObject> {
        // Taken before the agent is: a pause looks at this holding the agent,
        // so it never stops a request under way.
        let _using = Using::new(&self.in_use);
        if method == "session/close" {
            return self.close(params).await;
        }
        if method == "session/prompt" && self.closing.load(Ordering::SeqCst) {
            return Err(ErrorObject::new(code::AGENT_UNAVAILABLE, format!("Could not connect to {}. Try again.", self.name())));
        }
        let unavailable = |e: Error| ErrorObject::new(code::AGENT_UNAVAILABLE, sentence(&e, self.name()));
        let mut live = self.live().await.map_err(unavailable)?;
        // A cancelled prompt may have left the agent wedged: a prompt goes to
        // a fresh process, its session reopened there, unless the agent
        // works on something else.
        let suspect = live.state.lock().expect("sessions").suspect;
        if method == "session/prompt"
            && suspect
            && self.pause(&live, "a cancelled prompt may have left it wedged; a new prompt starts it afresh", Pause::ForThisRequest).await
        {
            live = self.live().await.map_err(unavailable)?;
        }
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
        let prompted = session.clone().filter(|_| method == "session/prompt");
        if let Some(session) = &prompted {
            live.state.lock().expect("sessions").prompt_started(session, Instant::now());
        }
        let answered = self.call(&live, &method, params).await;
        if let Some(session) = &prompted {
            let mut state = live.state.lock().expect("sessions");
            state.turn_ended(session);
            if answered.as_ref().is_ok_and(|r| r["stopReason"] == "cancelled") {
                state.suspect = true;
            }
        }
        let result = answered.map_err(|e| self.refusal(e))?;
        let mut state = live.state.lock().expect("sessions");
        let session = session.or_else(|| result["sessionId"].as_str().map(str::to_owned));
        let target = target.or_else(|| session.clone());
        match (method.as_str(), &session, &target) {
            ("session/new" | "session/load" | "session/resume", Some(session), Some(target)) => {
                state.open.insert(target.clone());
                self.record_sessions(std::slice::from_ref(session), Life::Running);
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
            ("session/delete", Some(session), Some(target)) => {
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

    /// `session/close` of the conversation `params` names: every session of
    /// the agent's it ran in that the running agent holds ends there
    /// (`session/close`, where the agent closes sessions; elsewhere what it
    /// holds goes when it pauses), and the conversation is no longer held,
    /// running or paused. A paused agent is never started for it, nor a
    /// session reopened to be closed.
    async fn close(&self, params: Value) -> Result<Value, ErrorObject> {
        let Some(id) = params["sessionId"].as_str().map(str::to_owned) else {
            return Err(ErrorObject::new(code::INVALID_PARAMS, "session/close needs a sessionId."));
        };
        // Never while one of its sessions is being reopened.
        let _one = self.opening.lock().await;
        if let Some(live) = self.current().await {
            for session in self.sessions_of(&id) {
                if !live.state.lock().expect("sessions").open.contains(&session) {
                    continue;
                }
                if live.init.close_session {
                    let mut params = params.clone();
                    params["sessionId"] = json!(session);
                    self.call(&live, "session/close", params).await.map_err(|e| self.refusal(e))?;
                }
                live.state.lock().expect("sessions").open.remove(&session);
            }
        }
        // Not paused either: no longer recorded as held.
        self.record_sessions(std::slice::from_ref(&id), Life::Running);
        if self.moves.lock().expect("moves").remove(&id).is_some() {
            self.save_moves();
        }
        Ok(json!({}))
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
        let _using = Using::new(&self.in_use);
        let live = self.live().await.map_err(|e| {
            ErrorObject::new(code::AGENT_UNAVAILABLE, sentence(&e, self.name()))
        })?;
        let servers = json!(mcp);
        let created = self
            .call(&live, "session/new", json!({ "cwd": folder, "mcpServers": servers }))
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

    /// The model, from the running agent; a paused one isn't started for
    /// it, and answers with the model it last said it runs.
    async fn model(self: Arc<Self>, session: Option<String>) -> Result<String, Error> {
        let Some(live) = self.current().await else {
            let known = self.known.lock().expect("known").model.clone();
            return Ok(known.unwrap_or_else(|| self.name().to_owned()));
        };
        let state = live.state.lock().expect("sessions");
        let model = session
            .and_then(|s| state.models.get(&s).cloned())
            .or_else(|| state.model.clone())
            .or_else(|| live.init.title.clone())
            .unwrap_or_else(|| self.name().to_owned());
        drop(state);
        self.known.lock().expect("known").model = Some(model.clone());
        Ok(model)
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
            let update = &params["update"];
            if let (Some("tool_call" | "tool_call_update"), Some(status)) = (update["sessionUpdate"].as_str(), update["status"].as_str()) {
                tracing::info!(agent = shared.key(), session = %session, tool = %update["toolCallId"], status, "acp: tool");
            }
            {
                let mut state = state.lock().expect("sessions");
                state.track(&session, update);
                state.heard.insert(session.clone(), SystemTime::now());
                state.active = Some(Instant::now());
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
            if let Some((reply, _)) = reply {
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
            {
                let mut state = state.lock().expect("sessions");
                state.asks.insert(id.to_string(), (reply_id, session.clone()));
                state.heard.insert(session.clone(), SystemTime::now());
                state.active = Some(Instant::now());
            }
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
            // An agent that started once and paused (or ended by itself) is
            // ready: the next request resumes it. Asking must not undo a
            // pause.
            let started_once = {
                let known = shared.known.lock().expect("known");
                known.capabilities.is_some() && known.failed.is_none()
            };
            if started_once && shared.current().await.is_none() {
                return Ok(());
            }
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
            if method == "session/cancel"
                && let Some(session) = params["sessionId"].as_str()
            {
                tracing::info!(agent = shared.key(), session, "acp: cancel");
                live.state.lock().expect("sessions").cancel(session, Instant::now());
            }
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

    fn status(&self) -> BoxFuture<'_, Option<Vec<AgentStatus>>> {
        Box::pin(async move { Some(vec![self.shared.status()]) })
    }

    fn shutdown(&self, grace: Duration) -> BoxFuture<'_, ()> {
        let shared = self.shared.clone();
        Box::pin(async move {
            let _ = tokio::spawn(async move { shared.shutdown(grace).await }).await;
        })
    }
}

/// When an agent may pause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pause {
    /// Only while it works on nothing.
    WhenIdle,
    /// While it works on nothing but the request asking.
    ForThisRequest,
    /// Whatever it does (the host shuts down).
    Now,
}

/// A request in flight ([`Shared::in_use`]), until dropped.
struct Using<'a>(&'a AtomicUsize);

impl<'a> Using<'a> {
    fn new(count: &'a AtomicUsize) -> Self {
        count.fetch_add(1, Ordering::SeqCst);
        Self(count)
    }
}

impl Drop for Using<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Looks at the running agent `live` every so often: it pauses once it has
/// worked on nothing for its idle window, or at once when a cancelled prompt
/// may have left it wedged and nothing else works. Ends when the agent stops
/// running.
async fn watch(shared: Weak<Shared>, live: Weak<Live>) {
    let mut idle = Idle::default();
    loop {
        let every = match shared.upgrade() {
            Some(shared) => (shared.settings.idle / 4).clamp(Duration::from_millis(50), LOOK_EVERY),
            None => return,
        };
        tokio::time::sleep(every).await;
        let (Some(shared), Some(live)) = (shared.upgrade(), live.upgrade()) else {
            return;
        };
        let current = shared.live.lock().await.as_ref().is_some_and(|l| Arc::ptr_eq(l, &live));
        if !current || live.conn.is_closed() {
            return;
        }
        shared.look(&live).await;
        let busy = shared.busy_of(&live, 0);
        let (suspect, active) = {
            let mut state = live.state.lock().expect("sessions");
            state.idle_since = match busy.is_idle() {
                true => state.idle_since.or(Some(SystemTime::now())),
                false => None,
            };
            (state.suspect, state.active)
        };
        let due = idle.due(Instant::now(), busy.is_idle(), active, shared.settings.idle);
        let why = match (due, suspect && busy.is_idle()) {
            (true, _) => "idle",
            (false, true) => "a cancelled prompt may have left it wedged",
            (false, false) => continue,
        };
        if shared.pause(&live, why, Pause::WhenIdle).await {
            return;
        }
    }
}

/// Where a backend keeps its moved conversations: beside its chats record.
fn moves_file(chats_file: &Path) -> PathBuf {
    chats_file.with_file_name("acp-moves.json")
}

/// Where a backend records its agent's process while it runs, so the next
/// run stops what a crashed one left.
fn process_file(chats_file: &Path) -> PathBuf {
    chats_file.with_file_name("acp-process.json")
}

/// Where a backend keeps the sessions its agent paused with.
fn sessions_file(chats_file: &Path) -> PathBuf {
    chats_file.with_file_name("acp-sessions.json")
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
    let mut probe = command::new::<tokio::process::Command>(&command.program, command::Console::Hidden);
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

#[cfg(test)]
mod tests {
    use super::*;

    fn why(busy: &Busy) -> Vec<Working> {
        busy.why.iter().copied().collect()
    }

    /// A prompt outstanding is work however long it is silent: an hour of
    /// looks every 30 s never finds the agent idle, never pauses it.
    #[test]
    fn an_hour_long_silent_prompt_is_work_the_whole_hour() {
        let mut sessions = Sessions::default();
        let start = Instant::now();
        sessions.prompt_started("s1", start);
        let mut idle = Idle::default();
        for look in 0..=120u64 {
            let now = start + Duration::from_secs(30 * look);
            let busy = sessions.busy(now);
            assert_eq!(why(&busy), [Working::Prompt], "at {} min", look / 2);
            assert!(!idle.due(now, busy.is_idle(), sessions.active, IDLE_WINDOW), "paused at {} min", look / 2);
        }
        // It ends: the window starts only now, and runs its whole length.
        let ended = start + Duration::from_secs(3600);
        sessions.turn_ended("s1");
        sessions.active = Some(ended);
        assert!(sessions.busy(ended).is_idle());
        assert!(!idle.due(ended, true, sessions.active, IDLE_WINDOW));
        assert!(!idle.due(ended + IDLE_WINDOW - Duration::from_secs(1), true, sessions.active, IDLE_WINDOW));
        assert!(idle.due(ended + IDLE_WINDOW, true, sessions.active, IDLE_WINDOW));
        // Work again starts it over.
        assert!(!idle.due(ended + IDLE_WINDOW, false, sessions.active, IDLE_WINDOW));
        assert!(!idle.due(ended + IDLE_WINDOW * 2 - Duration::from_secs(1), true, sessions.active, IDLE_WINDOW));
    }

    /// A short prompt that runs and ends between two looks still starts the
    /// window over: the window counts from the last thing the agent did.
    #[test]
    fn a_prompt_between_two_looks_starts_the_window_over() {
        let start = Instant::now();
        let mut idle = Idle::default();
        assert!(!idle.due(start, true, None, IDLE_WINDOW));
        let prompted = start + IDLE_WINDOW - Duration::from_secs(60);
        let later = start + IDLE_WINDOW;
        assert!(!idle.due(later, true, Some(prompted), IDLE_WINDOW), "not due right after a prompt");
        assert!(idle.due(prompted + IDLE_WINDOW, true, Some(prompted), IDLE_WINDOW));
    }

    /// A cancel that arrived after its turn ended is nobody's: a later
    /// prompt, silent however long, is still work and leaves the agent
    /// trusted.
    #[test]
    fn a_stale_cancel_never_touches_a_later_prompt() {
        let mut sessions = Sessions::default();
        let now = Instant::now();
        sessions.prompt_started("s1", now);
        sessions.turn_ended("s1");
        // Stop, just after the turn ended.
        sessions.cancel("s1", now);
        assert!(sessions.cancelled.is_empty());
        let later = now + Duration::from_secs(3 * 3600);
        sessions.prompt_started("s1", later);
        assert_eq!(why(&sessions.busy(later + CANCEL_GRACE * 10)), [Working::Prompt]);
        assert!(!sessions.suspect);
        // A cancel of a previous prompt, still noted, is cleared by the next.
        sessions.cancel("s1", later);
        sessions.turn_ended("s1");
        sessions.prompt_started("s1", later);
        sessions.cancelled.insert("s1".into(), later);
        sessions.prompt_started("s1", later + Duration::from_secs(1));
        assert!(sessions.cancelled.is_empty());
        assert_eq!(why(&sessions.busy(later + CANCEL_GRACE * 10)), [Working::Prompt]);
        assert!(!sessions.suspect);
    }

    #[test]
    fn tools_permissions_and_plans_are_work_until_they_finish() {
        let mut sessions = Sessions::default();
        let now = Instant::now();
        sessions.track("s1", &json!({ "sessionUpdate": "tool_call", "toolCallId": "c1", "status": "in_progress" }));
        sessions.track("s2", &json!({ "sessionUpdate": "tool_call", "toolCallId": "c2" }));
        sessions.asks.insert("7".into(), (1, "s3".into()));
        sessions.track("s4", &json!({ "sessionUpdate": "plan", "entries": [{ "content": "a", "status": "completed" }, { "content": "b", "status": "in_progress" }] }));
        let busy = sessions.busy(now);
        assert_eq!(why(&busy), [Working::Tool, Working::Permission, Working::Plan]);
        assert_eq!(busy.sessions["s1"], BTreeSet::from([Working::Tool]));
        assert_eq!(busy.sessions["s3"], BTreeSet::from([Working::Permission]));
        assert!(!busy.sessions.contains_key("s2"), "a call with no status is pending: not running");

        sessions.track("s1", &json!({ "sessionUpdate": "tool_call_update", "toolCallId": "c1", "status": "completed" }));
        sessions.track("s2", &json!({ "sessionUpdate": "tool_call_update", "toolCallId": "c2", "status": "failed" }));
        sessions.asks.clear();
        sessions.track("s4", &json!({ "sessionUpdate": "plan", "entries": [{ "content": "b", "status": "completed" }] }));
        assert!(sessions.busy(now).is_idle());

        // A turn's end finishes what it left unfinished.
        sessions.track("s1", &json!({ "sessionUpdate": "tool_call", "toolCallId": "c3", "status": "in_progress" }));
        *sessions.prompts.entry("s1".into()).or_default() += 1;
        assert_eq!(why(&sessions.busy(now)), [Working::Prompt, Working::Tool]);
        sessions.turn_ended("s1");
        assert!(sessions.busy(now).is_idle());
    }

    /// A call still pending (its input streaming from the model, as a stream
    /// that stalled leaves it) has not started: the prompt it sits in reads
    /// as only a prompt. It runs once in progress, and is done once it
    /// completes.
    #[test]
    fn a_pending_tool_call_is_not_running() {
        let mut sessions = Sessions::default();
        let now = Instant::now();
        sessions.prompt_started("s1", now);
        sessions.track("s1", &json!({ "sessionUpdate": "tool_call", "toolCallId": "c1", "status": "pending" }));
        sessions.track("s1", &json!({ "sessionUpdate": "tool_call_update", "toolCallId": "c1", "rawInput": { "command": "ls" } }));
        assert_eq!(why(&sessions.busy(now)), [Working::Prompt]);
        sessions.track("s1", &json!({ "sessionUpdate": "tool_call_update", "toolCallId": "c1", "status": "in_progress" }));
        assert_eq!(why(&sessions.busy(now)), [Working::Prompt, Working::Tool]);
        sessions.track("s1", &json!({ "sessionUpdate": "tool_call_update", "toolCallId": "c1", "status": "completed" }));
        assert_eq!(why(&sessions.busy(now)), [Working::Prompt]);
    }

    /// A tree of processes, each `(pid, CPU ms so far, started at)`.
    fn tree(processes: &[(u32, u64, u64)]) -> process::Tree {
        process::Tree {
            descendants: processes.iter().map(|p| p.0).collect(),
            usage: processes
                .iter()
                .map(|&(pid, cpu, started)| (pid, process::Usage { cpu: Duration::from_millis(cpu), started }))
                .collect(),
        }
    }

    /// The owner's case: the first prompt goes the moment `session/new`
    /// answers, and a few seconds into it the agent starts the session's
    /// process and its tool servers, which run until the session ends. Idle,
    /// they are never work: an hour of looks after the turn finds the agent
    /// idle, and it is due to pause once its window has passed.
    #[test]
    fn processes_a_session_starts_are_not_work_while_they_idle() {
        let mut sessions = Sessions::default();
        let t0 = 1_000_000u64;
        let at = |s: u64| Duration::from_secs(t0 + s);
        let start = Instant::now();
        // The adapter, just started.
        sessions.looked(Some(&tree(&[(1, 300, t0)])), at(0));
        sessions.prompt_started("s1", start);
        // 5 s in: the session's process and three tool servers, each just
        // started with a little CPU of starting up.
        let opened = [(1, 350, t0), (10, 400, t0 + 1), (11, 200, t0 + 5), (12, 150, t0 + 5), (13, 180, t0 + 5)];
        sessions.looked(Some(&tree(&opened)), at(30));
        assert_eq!(why(&sessions.busy(start)), [Working::Prompt]);
        sessions.turn_ended("s1");
        let ended = start + Duration::from_secs(40);
        sessions.active = Some(ended);
        let mut idle = Idle::default();
        let mut paused_at = None;
        for look in 2..=120u64 {
            // Each idles at a hundredth of a core, as idle agents do.
            let trickle = 300 * look;
            let procs: Vec<(u32, u64, u64)> = opened.iter().map(|&(pid, cpu, started)| (pid, cpu + trickle, started)).collect();
            sessions.looked(Some(&tree(&procs)), at(30 * look));
            let now = start + Duration::from_secs(30 * look);
            let busy = sessions.busy(now);
            assert!(busy.is_idle(), "at {look}: {:?}", busy.why);
            if paused_at.is_none() && idle.due(now, busy.is_idle(), sessions.active, IDLE_WINDOW) {
                paused_at = Some(now);
            }
        }
        let paused_at = paused_at.expect("it pauses");
        assert!(paused_at >= ended + IDLE_WINDOW && paused_at <= ended + IDLE_WINDOW + LOOK_EVERY * 2);
    }

    /// A process that uses the CPU works (a build a turn left running in the
    /// background), however many idle ones there are; it stops working
    /// when it idles or exits. A pid given to another process is that
    /// process's.
    #[test]
    fn a_process_using_the_cpu_is_work() {
        let mut sessions = Sessions::default();
        let now = Instant::now();
        let t0 = 1_000_000u64;
        let at = |s: u64| Duration::from_secs(t0 + s);
        // Twenty idle sessions' processes.
        let idle: Vec<(u32, u64, u64)> = (10..30).map(|pid| (pid, 5_000, t0)).collect();
        sessions.looked(Some(&tree(&idle)), at(0));
        let trickled: Vec<(u32, u64, u64)> = (10..30).map(|pid| (pid, 5_300, t0)).collect();
        sessions.looked(Some(&tree(&trickled)), at(30));
        assert!(sessions.busy(now).is_idle(), "many idle processes are not work");
        // A compiler, started 10 s ago, busy the whole time.
        let build = [trickled.clone(), vec![(40, 10_000, t0 + 50)]].concat();
        sessions.looked(Some(&tree(&build)), at(60));
        assert_eq!(why(&sessions.busy(now)), [Working::Processes]);
        // Still compiling.
        let build = [trickled.clone(), vec![(40, 38_000, t0 + 50)]].concat();
        sessions.looked(Some(&tree(&build)), at(90));
        assert_eq!(why(&sessions.busy(now)), [Working::Processes]);
        // Done: it idles, then exits.
        let build = [trickled.clone(), vec![(40, 38_100, t0 + 50)]].concat();
        sessions.looked(Some(&tree(&build)), at(120));
        assert!(sessions.busy(now).is_idle());
        sessions.looked(Some(&tree(&trickled)), at(150));
        assert!(sessions.busy(now).is_idle());
        // Its pid, given to a new process that starts busy: work.
        let reused = [trickled.clone(), vec![(40, 4_000, t0 + 175)]].concat();
        sessions.looked(Some(&tree(&reused)), at(180));
        assert_eq!(why(&sessions.busy(now)), [Working::Processes]);
    }

    /// A cancelled prompt the agent never ends is work for a grace period;
    /// after it the agent is wedged, and suspect: it pauses and resumes
    /// fresh.
    #[test]
    fn a_cancelled_prompt_that_never_ends_leaves_the_agent_suspect() {
        let mut sessions = Sessions::default();
        let now = Instant::now();
        sessions.prompt_started("s1", now);
        sessions.cancel("s1", now);
        assert_eq!(why(&sessions.busy(now + CANCEL_GRACE)), [Working::Prompt]);
        assert!(!sessions.suspect);
        assert!(sessions.busy(now + CANCEL_GRACE + Duration::from_secs(1)).is_idle());
        assert!(sessions.suspect);
    }
}
