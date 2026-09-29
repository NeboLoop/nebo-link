//! A coding agent's life on the host, with a real agent process (the
//! conformance suite's scripted agent, this test binary again): it works
//! while a prompt, a tool call, a permission request runs, or a process of
//! its uses the CPU; the processes a session starts (its tool servers) are
//! not work while they idle; it pauses once idle for its window and never
//! before, and nothing it ran is left; a paused conversation resumes as the
//! same session, or says plainly why it can't; a session closed (by a client
//! or the host letting it go) ends what the agent runs for it, and never
//! starts a paused agent; shutting down lets a prompt finish, then stops
//! everything; and what a crashed run left running is stopped by the next.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use link_core::acp::{Acp, Client, IDLE_WINDOW, Settings};
use link_core::backend::{AgentMessage, Backend, FromAgent, Reply};
use link_core::host::Host;
use link_core::model::{AgentStatus, Life, Working};
use link_core::process;
use link_core::roster::{Member, Roster};
use nebo_runtimes::RuntimeCommand;
use nebo_runtimes::acp::Agent as AcpAgent;
use serde_json::{Value, json};
use tokio::sync::mpsc;

const SCRIPTED: &str = "LINK_CORE_LIFE_AGENT";
const CLIENT: Client = Client { name: "link-core tests", version: "0" };
const AGENT: &str = "acp";

/// Not a test when a host starts it: the scripted ACP agent.
#[test]
fn scripted_agent() {
    if std::env::var_os(SCRIPTED).is_none() {
        return;
    }
    println!();
    tokio::runtime::Runtime::new().unwrap().block_on(oal_conformance::fake_agent::stdio());
}

/// How the agent starts: its sessions kept in `store` when given; with
/// `starts_work`, a process it runs starts another a second after it does
/// (as a tool left running in the background would).
fn command(store: Option<&Path>, starts_work: bool) -> RuntimeCommand {
    let exe = std::env::current_exe().unwrap().to_string_lossy().into_owned();
    let args: Vec<String> = ["scripted_agent", "--exact", "--nocapture", "--test-threads=1"].map(String::from).to_vec();
    let mut env = vec![(SCRIPTED.to_owned(), "1".to_owned())];
    if let Some(store) = store {
        env.push(("OAL_FAKE_AGENT_SESSIONS".to_owned(), store.to_string_lossy().into_owned()));
    }
    match starts_work {
        false => RuntimeCommand { program: exe, args, env },
        true => RuntimeCommand {
            program: "sh".into(),
            // A new process a second in: `sleep 30` in the background of the
            // subshell, never the subshell itself (dash runs a subshell's last
            // command in its own pid).
            args: [vec!["-c".to_owned(), r#"(sleep 1; sleep 30 & wait) & exec "$0" "$@""#.to_owned(), exe], args].concat(),
            env,
        },
    }
}

/// `command` with every session the agent opens running `server` a second
/// later, until the session closes: its tool servers.
fn with_tool_server(mut command: RuntimeCommand, server: &str) -> RuntimeCommand {
    command.env.push(("OAL_FAKE_AGENT_TOOL_SERVER".to_owned(), server.to_owned()));
    command
}

/// A tool server that idles, as one waiting for its next call does.
const IDLE_SERVER: &str = "exec sleep 300";

struct Agent {
    acp: Arc<Acp>,
    from: mpsc::UnboundedReceiver<FromAgent>,
    dir: PathBuf,
}

fn agent(dir: &Path, command: RuntimeCommand, idle: Duration) -> Agent {
    let acp = Arc::new(Acp::new(Settings {
        agent: AcpAgent::Other,
        name: "Fake Agent".into(),
        command,
        workdir: dir.join("work"),
        log: dir.join("agent.log"),
        chats_file: dir.join("agent").join("acp-chats.json"),
        client: CLIENT,
        idle,
    }));
    let (tx, from) = mpsc::unbounded_channel();
    acp.connect(Arc::new(move |message| {
        let _ = tx.send(message);
    }));
    Agent { acp, from, dir: dir.to_path_buf() }
}

impl Agent {
    async fn request(&self, method: &str, params: Value) -> Result<Value, link_core::model::ErrorObject> {
        self.acp.request(AGENT, method, params).await
    }

    async fn new_session(&self) -> String {
        let created = self.request("session/new", json!({ "cwd": self.dir.join("work"), "mcpServers": [] })).await.unwrap();
        created["sessionId"].as_str().unwrap().to_owned()
    }

    /// Sends a prompt, answered on its own task.
    fn prompt(&self, session: &str, text: &str) -> tokio::task::JoinHandle<Result<Value, link_core::model::ErrorObject>> {
        let acp = self.acp.clone();
        let params = json!({ "sessionId": session, "prompt": [{ "type": "text", "text": text }] });
        tokio::spawn(async move { acp.request(AGENT, "session/prompt", params).await })
    }

    async fn status(&self) -> AgentStatus {
        self.acp.status().await.unwrap().remove(0)
    }

    /// The processes under the agent's process now.
    fn under(&self) -> Vec<u32> {
        process::tree(self.pid()).map(|t| t.descendants.into_iter().collect()).unwrap_or_default()
    }

    /// The agent's process now, as the host recorded it.
    fn pid(&self) -> u32 {
        let text = std::fs::read_to_string(self.dir.join("agent").join("acp-process.json")).unwrap();
        serde_json::from_str::<process::Started>(&text).unwrap().pid
    }

    /// The next permission request the agent sends.
    async fn permission(&mut self) -> Reply {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(20), self.from.recv()).await.unwrap().unwrap();
            if let AgentMessage::Permission { reply, .. } = message.message {
                return reply;
            }
        }
    }
}

/// Waits until `check` holds.
async fn until<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(20);
    while !check().await {
        assert!(Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn allow() -> Value {
    json!({ "outcome": { "outcome": "selected", "optionId": "allow-once" } })
}

#[tokio::test]
async fn it_works_while_a_prompt_a_tool_or_a_permission_request_is_outstanding() {
    let root = tempfile::tempdir().unwrap();
    let mut a = agent(root.path(), command(None, false), IDLE_WINDOW);
    let session = a.new_session().await;
    assert!(a.acp.busy().await.is_idle());

    // The call waits on the owner's approval: still pending, it has not
    // started; the permission request is the work.
    let turn = a.prompt(&session, "run: echo hi");
    let reply = a.permission().await;
    let busy = a.acp.busy().await;
    assert_eq!(busy.why.iter().copied().collect::<Vec<_>>(), [Working::Prompt, Working::Permission]);
    assert_eq!(busy.sessions[&session].len(), 2);
    let status = a.status().await;
    assert!(status.busy && status.state == Life::Running);
    assert_eq!(status.sessions[0].session_id, session);
    assert_eq!(status.sessions[0].state, Life::Running);
    assert!(status.idle_since.is_none());

    reply.send(allow());
    assert_eq!(turn.await.unwrap().unwrap()["stopReason"], "end_turn");
    assert!(a.acp.busy().await.is_idle());
}

/// The owner's case: the first prompt goes the moment `session/new`
/// answers, and the session's tool servers start after it, then idle. They
/// never keep the agent running: it pauses once idle for its window, and
/// nothing it ran is left. The conversation resumes, its tool server with
/// it.
#[tokio::test]
async fn tool_servers_a_session_starts_never_keep_it_running() {
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("sessions");
    let window = Duration::from_millis(1500);
    let a = agent(root.path(), with_tool_server(command(Some(&store), false), IDLE_SERVER), window);
    let session = a.new_session().await;
    let first = a.pid();
    assert_eq!(a.prompt(&session, "hello").await.unwrap().unwrap()["stopReason"], "end_turn");
    let idle_from = Instant::now();
    until("its tool server runs", || async { !a.under().is_empty() }).await;
    let servers = a.under();
    until("it paused", || async { a.status().await.state == Life::Paused }).await;
    assert!(idle_from.elapsed() >= window, "paused {:?} after it went idle", idle_from.elapsed());
    for p in servers.iter().chain([&first]) {
        assert!(!process::alive(*p), "{p} still runs");
    }

    assert_eq!(a.prompt(&session, "where").await.unwrap().unwrap()["stopReason"], "end_turn");
    assert_ne!(a.pid(), first);
    until("the resumed session's tool server runs", || async { !a.under().is_empty() }).await;
    until("it paused again", || async { a.status().await.state == Life::Paused }).await;
}

/// With its tool servers idling beside it, a turn in flight is never paused:
/// a tool call running quietly for many windows, and the prompt it is in.
#[tokio::test]
async fn a_turn_in_flight_is_never_paused_beside_idle_tool_servers() {
    let root = tempfile::tempdir().unwrap();
    let a = agent(root.path(), with_tool_server(command(None, false), IDLE_SERVER), Duration::from_millis(300));
    let session = a.new_session().await;
    let turn = a.prompt(&session, "busy");
    until("the call runs", || async { a.acp.busy().await.why.contains(&Working::Tool) }).await;
    until("its tool server runs", || async { !a.under().is_empty() }).await;
    let pid = a.pid();
    let servers = a.under();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let status = a.status().await;
    assert_eq!(status.state, Life::Running, "never paused while a call runs");
    assert_eq!(status.why, [Working::Prompt, Working::Tool]);
    for p in servers.iter().chain([&pid]) {
        assert!(process::alive(*p), "{p} stopped mid-turn");
    }
    a.acp.notify(AGENT, "session/cancel", json!({ "sessionId": session }));
    assert_eq!(turn.await.unwrap().unwrap()["stopReason"], "cancelled");
    until("it paused", || async { a.status().await.state == Life::Paused }).await;
    for p in servers.iter().chain([&pid]) {
        assert!(!process::alive(*p), "{p} still runs");
    }
}

/// A process of the agent's that uses the CPU (a build a turn left running)
/// is work, and keeps it running past its window; closed with its session,
/// it no longer does, and the agent pauses.
#[tokio::test]
async fn a_process_using_the_cpu_is_work_until_it_ends() {
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("sessions");
    let window = Duration::from_secs(3);
    let a = agent(root.path(), with_tool_server(command(Some(&store), false), "while :; do :; done"), window);
    let session = a.new_session().await;
    assert_eq!(a.prompt(&session, "hello").await.unwrap().unwrap()["stopReason"], "end_turn");
    until("the busy process is work", || async { a.acp.busy().await.why.contains(&Working::Processes) }).await;
    tokio::time::sleep(window * 2).await;
    assert_eq!(a.status().await.state, Life::Running, "never paused while a process works");
    let busy = a.under();
    a.request("session/close", json!({ "sessionId": session })).await.unwrap();
    until("what the session ran ended with it", || async { busy.iter().all(|p| !process::alive(*p)) }).await;
    until("it paused", || async { a.status().await.state == Life::Paused }).await;
}

/// `session/close` ends the session in the agent, and the tool server it ran
/// with it; the agent's other sessions run on. Closing a session of a paused
/// agent never starts it.
#[tokio::test]
async fn a_closed_session_ends_what_it_ran_and_never_starts_a_paused_agent() {
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("sessions");
    let a = agent(root.path(), with_tool_server(command(Some(&store), false), IDLE_SERVER), IDLE_WINDOW);
    let first = a.new_session().await;
    until("its tool server runs", || async { a.under().len() == 1 }).await;
    let first_server = a.under();
    let second = a.new_session().await;
    until("both tool servers run", || async { a.under().len() == 2 }).await;
    a.request("session/close", json!({ "sessionId": first })).await.unwrap();
    until("the closed session's tool server stopped", || async { !process::alive(first_server[0]) }).await;
    assert_eq!(a.under().len(), 1, "the other session's runs on");
    let status = a.status().await;
    assert_eq!(status.sessions.iter().map(|s| s.session_id.as_str()).collect::<Vec<_>>(), [second.as_str()]);

    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("sessions");
    let a = agent(root.path(), command(Some(&store), false), Duration::from_millis(300));
    let session = a.new_session().await;
    let pid = a.pid();
    until("it paused", || async { a.status().await.state == Life::Paused }).await;
    assert_eq!(a.request("session/close", json!({ "sessionId": session })).await.unwrap(), json!({}));
    let status = a.status().await;
    assert_eq!(status.state, Life::Paused, "not started to close a session");
    assert!(status.sessions.is_empty(), "the closed session is no longer held");
    assert!(!process::alive(pid));
    assert!(!a.dir.join("agent").join("acp-process.json").exists(), "no process started");
}

/// A session the host lets go (past its open sessions) is closed in its
/// agent, not only dropped from the host's record.
#[tokio::test]
async fn a_session_the_host_lets_go_is_closed_in_its_agent() {
    let root = tempfile::tempdir().unwrap();
    let a = agent(root.path(), command(None, false), IDLE_WINDOW);
    let host = Host::new(Arc::new(Roster::new(vec![Member {
        id: AGENT.into(),
        label: "Fake Agent".into(),
        runtime: "acp".into(),
        backend: a.acp.clone(),
    }])));
    let params = json!({ "cwd": root.path().join("work"), "mcpServers": [] });
    let first = host.new_session(AGENT, params.clone()).await.unwrap()["sessionId"].as_str().unwrap().to_owned();
    for _ in 0..256 {
        host.new_session(AGENT, params.clone()).await.unwrap();
    }
    assert!(!host.is_open(AGENT, &first), "the least used was let go");
    until("the agent closed it", || async { a.status().await.sessions.iter().all(|s| s.session_id != first) }).await;
    assert_eq!(a.status().await.sessions.len(), 256);
}

/// Idle for its window, it pauses (its process stops); the conversation
/// then resumes as the same session in a new process.
#[tokio::test]
async fn an_idle_agent_pauses_after_its_window_and_resumes_the_same_session() {
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("sessions");
    let window = Duration::from_millis(1500);
    let a = agent(root.path(), command(Some(&store), false), window);
    let session = a.new_session().await;
    let first = a.pid();
    assert_eq!(a.prompt(&session, "hello").await.unwrap().unwrap()["stopReason"], "end_turn");
    let idle_from = Instant::now();
    until("it paused", || async { a.status().await.state == Life::Paused }).await;
    assert!(idle_from.elapsed() >= window, "paused {:?} after it went idle", idle_from.elapsed());
    assert!(!process::alive(first), "its process stopped");
    let status = a.status().await;
    assert_eq!((status.sessions[0].session_id.as_str(), status.sessions[0].state), (session.as_str(), Life::Paused));

    // The agent only knows the session by loading it: the prompt answered
    // in it is the same conversation, in a new process.
    let answered = a.prompt(&session, "where").await.unwrap().unwrap();
    assert_eq!(answered["stopReason"], "end_turn");
    assert_ne!(a.pid(), first);
    let status = a.status().await;
    assert_eq!(status.state, Life::Running);
    assert_eq!((status.sessions[0].session_id.as_str(), status.sessions[0].state), (session.as_str(), Life::Running));
}

/// A prompt outstanding for many windows is never paused; cancelled, the
/// agent may be wedged, so it pauses at once and resumes fresh.
#[tokio::test]
async fn a_long_silent_prompt_is_never_paused() {
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("sessions");
    let a = agent(root.path(), command(Some(&store), false), Duration::from_millis(300));
    let session = a.new_session().await;
    let pid = a.pid();
    let turn = a.prompt(&session, "wait");
    tokio::time::sleep(Duration::from_secs(3)).await;
    let status = a.status().await;
    assert_eq!(status.state, Life::Running, "never paused mid-prompt");
    assert!(status.busy && status.why.contains(&Working::Prompt));
    assert!(process::alive(pid));

    a.acp.notify(AGENT, "session/cancel", json!({ "sessionId": session }));
    assert_eq!(turn.await.unwrap().unwrap()["stopReason"], "cancelled");
    until("the cancelled agent paused", || async { a.status().await.state == Life::Paused }).await;
    assert!(!process::alive(pid));
    assert_eq!(a.prompt(&session, "hello").await.unwrap().unwrap()["stopReason"], "end_turn");
}

/// A prompt after a cancel goes to a fresh process (the cancel may have left
/// the agent wedged), in the same session, before any pause would.
#[tokio::test]
async fn a_prompt_after_a_cancel_starts_the_agent_afresh() {
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("sessions");
    let a = agent(root.path(), command(Some(&store), false), IDLE_WINDOW);
    let session = a.new_session().await;
    let wedged = a.pid();
    let turn = a.prompt(&session, "wait");
    until("the prompt runs", || async { a.acp.busy().await.why.contains(&Working::Prompt) }).await;
    a.acp.notify(AGENT, "session/cancel", json!({ "sessionId": session }));
    assert_eq!(turn.await.unwrap().unwrap()["stopReason"], "cancelled");
    assert_eq!(a.prompt(&session, "hello").await.unwrap().unwrap()["stopReason"], "end_turn");
    assert_ne!(a.pid(), wedged, "a fresh process");
    assert!(!process::alive(wedged));
    assert_eq!(a.status().await.sessions[0].session_id, session);
}

/// A prompt whose stream stopped in the middle of a call (the call still
/// pending, then nothing) reads as only a prompt, its processes idle: what
/// a client takes for a turn that stopped answering. Cancelled, the agent
/// pauses at once, and a new session answers in a fresh process.
#[tokio::test]
async fn a_stalled_prompt_reads_as_only_a_prompt_and_a_new_session_answers_after_the_cancel() {
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("sessions");
    let a = agent(root.path(), command(Some(&store), false), Duration::from_millis(300));
    let session = a.new_session().await;
    let turn = a.prompt(&session, "stall");
    until("the stalled prompt reads as only a prompt", || async {
        let status = a.status().await;
        !status.why.contains(&Working::Processes)
            && status.sessions.iter().any(|s| s.session_id == session && s.why == [Working::Prompt] && s.last_update.is_some())
    })
    .await;
    let wedged = a.pid();
    a.acp.notify(AGENT, "session/cancel", json!({ "sessionId": session }));
    assert_eq!(turn.await.unwrap().unwrap()["stopReason"], "cancelled");
    until("the cancelled agent paused", || async { a.status().await.state == Life::Paused }).await;
    assert!(!process::alive(wedged));
    let fresh = a.new_session().await;
    assert_ne!(fresh, session);
    assert_eq!(a.prompt(&fresh, "hello").await.unwrap().unwrap()["stopReason"], "end_turn");
    assert_ne!(a.pid(), wedged, "a fresh process");
}

/// A call running is work however quiet the turn is: never taken for a
/// turn that stopped answering, never paused.
#[tokio::test]
async fn a_running_tool_call_is_work_however_quiet() {
    let root = tempfile::tempdir().unwrap();
    let a = agent(root.path(), command(None, false), Duration::from_millis(300));
    let session = a.new_session().await;
    let turn = a.prompt(&session, "busy");
    until("the call runs", || async { a.acp.busy().await.why.contains(&Working::Tool) }).await;
    let pid = a.pid();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let status = a.status().await;
    assert_eq!(status.state, Life::Running, "never paused while a call runs");
    let working = status.sessions.iter().find(|s| s.session_id == session).expect("the session");
    assert_eq!(working.why, [Working::Prompt, Working::Tool]);
    assert!(process::alive(pid));
    a.acp.notify(AGENT, "session/cancel", json!({ "sessionId": session }));
    assert_eq!(turn.await.unwrap().unwrap()["stopReason"], "cancelled");
}

/// An agent that can't load a paused session says so, in plain words; the
/// host never starts a blank session in its place.
#[tokio::test]
async fn a_session_that_cannot_resume_is_said_plainly() {
    let root = tempfile::tempdir().unwrap();
    let a = agent(root.path(), command(None, false), Duration::from_millis(300));
    let session = a.new_session().await;
    assert_eq!(a.prompt(&session, "hello").await.unwrap().unwrap()["stopReason"], "end_turn");
    until("it paused", || async { a.status().await.state == Life::Paused }).await;
    let refused = a.prompt(&session, "hello").await.unwrap().unwrap_err();
    assert_eq!(refused.code, -32002);
    assert!(refused.message.starts_with("Fake Agent could not reopen this conversation:"), "{}", refused.message);
}

/// Shutting down, a running prompt gets its grace period and finishes;
/// one that outlasts it is stopped with everything else. Nothing the agent
/// started runs after.
#[tokio::test]
async fn shutdown_lets_a_prompt_finish_then_leaves_nothing_running() {
    let root = tempfile::tempdir().unwrap();
    let store = root.path().join("sessions");
    let mut a = agent(root.path(), command(Some(&store), true), IDLE_WINDOW);
    let session = a.new_session().await;
    let pid = a.pid();
    let turn = a.prompt(&session, "run: echo hi");
    let reply = a.permission().await;
    let started = process::tree(pid).unwrap().descendants;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(800)).await;
        reply.send(allow());
    });
    let began = Instant::now();
    let stopping = {
        let acp = a.acp.clone();
        tokio::spawn(async move { acp.shutdown(Duration::from_secs(20)).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let refused = a.prompt(&session, "hello").await.unwrap().unwrap_err();
    assert_eq!(refused.message, "Could not connect to Fake Agent. Try again.", "no new prompt while it shuts down");
    stopping.await.unwrap();
    assert!(began.elapsed() >= Duration::from_millis(700), "it waited for the prompt");
    assert_eq!(turn.await.unwrap().unwrap()["stopReason"], "end_turn", "the prompt finished");
    assert_eq!(a.status().await.state, Life::Paused);
    for p in started.iter().chain([&pid]) {
        assert!(!process::alive(*p), "{p} still runs");
    }

    // A prompt that outlasts the grace period is stopped with the rest.
    let a = agent(root.path(), command(Some(&store), true), IDLE_WINDOW);
    let turn = a.prompt(&session, "wait");
    until("the prompt runs", || async { a.acp.busy().await.why.contains(&Working::Prompt) }).await;
    let pid = a.pid();
    let began = Instant::now();
    a.acp.shutdown(Duration::from_millis(500)).await;
    assert!(began.elapsed() < Duration::from_secs(10));
    assert!(turn.await.unwrap().is_err(), "stopped mid-prompt, it says so");
    assert!(!process::alive(pid));
}

/// A run of the host that crashed left its agent running: the next run
/// stops it, and everything under it, before it starts its own.
#[tokio::test]
async fn what_a_crashed_run_left_running_is_stopped_by_the_next() {
    let root = tempfile::tempdir().unwrap();
    let crashed = agent(root.path(), command(None, true), IDLE_WINDOW);
    crashed.new_session().await;
    let pid = crashed.pid();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let left = process::tree(pid).unwrap().descendants;
    assert!(!left.is_empty());
    // Crashed: nothing of it stops its processes.
    std::mem::forget(crashed);

    let next = agent(root.path(), command(None, false), IDLE_WINDOW);
    next.new_session().await;
    assert_ne!(next.pid(), pid);
    for p in left.iter().chain([&pid]) {
        assert!(!process::alive(*p), "{p} still runs");
    }
}
