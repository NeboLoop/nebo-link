//! Adding and removing coding agents, and moving a conversation to another
//! folder, on the host itself: the agent is the conformance suite's scripted
//! agent, as a process (this test binary again).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use link_core::acp::{Acp, Client, Settings};
use link_core::backend::Backend;
use link_core::host::{Event, Host, Stamped};
use link_core::keep::{Add, CodingAgent, Installable, Keeper, Kept};
use link_core::model::{AgentChange, TurnState, code};
use link_core::roster::{Member, Roster};
use nebo_runtimes::RuntimeCommand;
use nebo_runtimes::acp::Agent as AcpAgent;
use serde_json::{Value, json};
use tokio::sync::broadcast;

const SCRIPTED: &str = "LINK_CORE_SCRIPTED_AGENT";
const CLIENT: Client = Client { name: "link-core tests", version: "0" };

/// Not a test when a host starts it: the scripted ACP agent.
#[test]
fn scripted_agent() {
    if std::env::var_os(SCRIPTED).is_none() {
        return;
    }
    println!();
    tokio::runtime::Runtime::new().unwrap().block_on(oal_conformance::fake_agent::stdio());
}

fn scripted() -> RuntimeCommand {
    RuntimeCommand {
        program: std::env::current_exe().unwrap().to_string_lossy().into_owned(),
        args: ["scripted_agent", "--exact", "--nocapture", "--test-threads=1"].map(String::from).to_vec(),
        env: vec![(SCRIPTED.into(), "1".into())],
    }
}

/// The agents a test added, in memory; the scripted agent is addable as
/// `claude-code` (the runtime is named, how it starts is the keeper's).
struct Record {
    dir: PathBuf,
    agents: Mutex<Vec<CodingAgent>>,
    refuse: Option<&'static str>,
    /// The coding agents really installed here are addable, not the
    /// scripted one.
    live: bool,
}

fn settings(dir: &Path, agent: &CodingAgent) -> Settings {
    Settings {
        agent: agent.agent,
        name: agent.label.clone(),
        command: agent.acp.command(),
        workdir: agent.acp.workdir.clone(),
        log: dir.join(format!("{}.log", agent.id)),
        chats_file: dir.join(&agent.id).join("acp-chats.json"),
        client: CLIENT,
    }
}

impl Keeper for Record {
    fn client(&self) -> Client {
        CLIENT
    }

    fn addable(&self) -> Vec<Installable> {
        if self.live {
            return link_core::keep::installed();
        }
        vec![Installable { id: "claude-code".into(), name: "Claude Code".into(), agent: AcpAgent::ClaudeCode, command: scripted() }]
    }

    fn agents(&self) -> Vec<Kept> {
        let agents = self.agents.lock().unwrap();
        agents
            .iter()
            .map(|a| Kept { id: a.id.clone(), label: a.label.clone(), runtime: a.agent.key().into(), folder: Some(a.acp.workdir.clone()) })
            .collect()
    }

    fn keep(&self, agent: &CodingAgent) -> Result<Member, String> {
        self.agents.lock().unwrap().push(agent.clone());
        Ok(Member {
            id: agent.id.clone(),
            label: agent.label.clone(),
            runtime: agent.agent.key().into(),
            backend: Arc::new(Acp::new(settings(&self.dir, agent))),
        })
    }

    fn forget(&self, id: &str) -> Result<(), String> {
        if let Some(why) = self.refuse {
            return Err(why.into());
        }
        self.agents.lock().unwrap().retain(|a| a.id != id);
        Ok(())
    }
}

struct Setup {
    _root: tempfile::TempDir,
    home: PathBuf,
    record: Arc<Record>,
    host: Arc<Host>,
}

fn setup(refuse: Option<&'static str>) -> Setup {
    setup_as(refuse, false)
}

fn setup_as(refuse: Option<&'static str>, live: bool) -> Setup {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let home = home.canonicalize().unwrap();
    let record = Arc::new(Record { dir: root.path().join("record"), agents: Mutex::new(Vec::new()), refuse, live });
    let host = Host::new(Arc::new(Roster::default()));
    host.set_home(home.clone());
    host.set_keeper(record.clone());
    Setup { _root: root, home, record, host }
}

async fn next(events: &mut broadcast::Receiver<Stamped>) -> Event {
    tokio::time::timeout(Duration::from_secs(30), events.recv()).await.expect("an event in time").unwrap().event
}

/// Sends `text` and returns what the agent said and every update of the turn.
async fn turn(host: &Arc<Host>, agent: &str, session: &str, text: &str) -> (String, Vec<Value>) {
    let mut events = host.subscribe();
    host.prompt(agent, session, vec![json!({ "type": "text", "text": text })], None, None).unwrap();
    let mut updates = Vec::new();
    loop {
        match next(&mut events).await {
            // A real agent may ask first: allowed once.
            Event::Pending(p) if p.change == link_core::model::PendingChange::Added => {
                let allow = p.request.options.iter().find(|o| o.kind == "allow_once").or(p.request.options.first()).unwrap();
                let _ = host.answer(&p.request.id, link_core::model::Outcome::Selected { option_id: allow.option_id.clone() }, None);
            }
            Event::Update(u) if u.session_id == session && u.update["sessionUpdate"] != "user_message_chunk" => updates.push(u.update),
            Event::Turn(t) if t.session_id == session && t.state == TurnState::Ended => break,
            _ => {}
        }
    }
    let said = updates
        .iter()
        .filter(|u| u["sessionUpdate"] == "agent_message_chunk")
        .filter_map(|u| u["content"]["text"].as_str())
        .collect();
    (said, updates)
}

#[tokio::test]
async fn an_agent_is_added_in_a_folder_of_its_own_and_removed_leaving_it() {
    let s = setup(None);
    assert_eq!(s.host.addable().iter().map(|a| a.id.as_str()).collect::<Vec<_>>(), ["claude-code"]);
    s.host.agents().await;
    let mut events = s.host.subscribe();

    let first = s.host.add_agent(Add { runtime: "claude-code".into(), ..Add::default() }).await.unwrap();
    assert_eq!((first.id.as_str(), first.label.as_str()), ("claude-code", "Claude Code"));
    let folder = s.home.join("NeboAI").join("claude-code");
    assert_eq!(first.folder.as_deref(), Some(folder.to_str().unwrap()));
    assert!(folder.is_dir());
    match next(&mut events).await {
        Event::Agent(update) => assert_eq!((update.change, update.agent.id.as_str()), (AgentChange::Added, "claude-code")),
        other => panic!("{other:?}"),
    }
    let second = s.host.add_agent(Add { runtime: "claude-code".into(), ..Add::default() }).await.unwrap();
    assert_eq!((second.id.as_str(), second.label.as_str()), ("claude-code-2", "Claude Code 2"));
    assert_eq!(second.folder.as_deref(), Some(s.home.join("NeboAI/claude-code-2").to_str().unwrap()), "each its own folder");
    assert_eq!(s.host.agents().await.len(), 2);

    let refused = s.host.add_agent(Add { runtime: "codex".into(), ..Add::default() }).await.unwrap_err();
    assert_eq!((refused.code, refused.message.as_str()), (code::NOT_PERMITTED, "Codex isn't installed on this computer."));
    let refused = s.host.add_agent(Add { runtime: "rm -rf".into(), ..Add::default() }).await.unwrap_err();
    assert_eq!(refused.message, "rm -rf isn't a coding agent this computer can add.");

    // Removed: no longer hosted or kept, announced, and its folder stays.
    std::fs::write(folder.join("work.txt"), "kept").unwrap();
    let mut events = s.host.subscribe();
    let removed = s.host.remove_agent("claude-code").await.unwrap();
    assert_eq!(removed.id, "claude-code");
    match next(&mut events).await {
        Event::Agent(update) => assert_eq!((update.change, update.agent.id.as_str()), (AgentChange::Removed, "claude-code")),
        other => panic!("{other:?}"),
    }
    assert_eq!(std::fs::read_to_string(folder.join("work.txt")).unwrap(), "kept");
    assert_eq!(s.record.agents().len(), 1);
    assert_eq!(s.host.agents().await.iter().map(|a| a.id.as_str()).collect::<Vec<_>>(), ["claude-code-2"]);
    assert_eq!(s.host.remove_agent("claude-code").await.unwrap_err().code, code::UNKNOWN_AGENT);
}

#[tokio::test]
async fn what_the_keeper_refuses_is_not_removed() {
    let s = setup(Some("Assistant is the only agent of Mac. To unlink the bot, run `nebo-link unlink`."));
    let added = s.host.add_agent(Add { runtime: "claude-code".into(), ..Add::default() }).await.unwrap();
    let refused = s.host.remove_agent(&added.id).await.unwrap_err();
    assert_eq!(refused.code, code::NOT_PERMITTED);
    assert!(refused.message.starts_with("Assistant is the only agent"));
    assert_eq!(s.host.agents().await.len(), 1, "still hosted");
}

#[tokio::test]
async fn a_host_without_a_keeper_adds_nothing() {
    let host = Host::new(Arc::new(Roster::default()));
    assert!(host.addable().is_empty());
    let refused = host.add_agent(Add { runtime: "claude-code".into(), ..Add::default() }).await.unwrap_err();
    assert_eq!(refused.code, code::NOT_PERMITTED);
}

#[tokio::test]
async fn a_conversation_moves_to_the_folder_the_owner_names() {
    let s = setup(None);
    let agent = s.host.add_agent(Add { runtime: "claude-code".into(), ..Add::default() }).await.unwrap();
    let folder = PathBuf::from(agent.folder.clone().unwrap());
    let created = s.host.new_session(&agent.id, json!({ "cwd": folder, "mcpServers": [] })).await.unwrap();
    let session = created["sessionId"].as_str().unwrap().to_owned();
    let (said, _) = turn(&s.host, &agent.id, &session, "where").await;
    assert_eq!(said, format!("Working in {}.", folder.display()));
    assert_eq!(s.host.folder(&agent.id, &session).await.as_deref(), folder.to_str());

    let proj = s.home.join("workspaces").join("proj");
    std::fs::create_dir_all(&proj).unwrap();
    let (said, updates) = turn(&s.host, &agent.id, &session, "work in ~/workspaces/proj").await;
    assert_eq!(said, "\n\nNow working in ~/workspaces/proj.\n\nMoved.");
    assert!(updates.iter().any(|u| u["sessionUpdate"] == "session_info_update" && u["_meta"]["oal/cwd"] == proj.to_str().unwrap()));
    assert_eq!(s.host.folder(&agent.id, &session).await.as_deref(), proj.to_str());

    let (said, _) = turn(&s.host, &agent.id, &session, "where").await;
    assert!(said.starts_with(&format!("Working in {}.", proj.display())), "{said}");
    assert!(said.contains(&format!("Your note from before the move: Was working in {}.", folder.display())), "{said}");

    // Outside the home folder only with Full access.
    let outside = s.home.parent().unwrap().join("elsewhere");
    std::fs::create_dir_all(&outside).unwrap();
    let outside = outside.canonicalize().unwrap();
    let (said, _) = turn(&s.host, &agent.id, &session, &format!("work in {}", outside.display())).await;
    assert!(said.starts_with("Couldn't move:") && said.contains("outside the home folder"), "{said}");
    s.host.change_session(&agent.id, "session/set_mode", json!({ "sessionId": session, "modeId": "full" }), None).await.unwrap();
    let (said, _) = turn(&s.host, &agent.id, &session, &format!("work in {}", outside.display())).await;
    assert!(said.ends_with("Moved."), "{said}");
    let (said, _) = turn(&s.host, &agent.id, &session, "where").await;
    assert!(said.starts_with(&format!("Working in {}.", outside.display())), "{said}");

    // The agent's session list names the conversation once, where it works.
    let listed = s.host.list_sessions(&agent.id, json!({ "cwd": folder })).await.unwrap();
    let sessions = listed["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 1, "{listed}");
    assert_eq!(sessions[0]["sessionId"], session.as_str());
    assert_eq!(sessions[0]["cwd"], outside.to_str().unwrap());

    // Where it went outlives the host: a new backend reads it back.
    let kept = s.record.agents.lock().unwrap()[0].clone();
    let again = Acp::new(settings(&s.record.dir, &kept));
    assert_eq!(again.session_folder("claude-code", &session), Some(outside.clone()));
}

/// The real Claude Code (its ACP adapter, run with the owner's own sign-in)
/// sees the host's tool and moves where it is asked: the next prompt runs
/// in the new folder. `LINK_CORE_LIVE_ACP=1 cargo test -p link-core --test
/// keep live -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "needs LINK_CORE_LIVE_ACP and a signed-in Claude Code"]
async fn live_claude_code_moves_through_the_host_tool() {
    if std::env::var_os("LINK_CORE_LIVE_ACP").is_none() {
        return;
    }
    let s = setup_as(None, true);
    let agent = s.host.add_agent(Add { runtime: "claude-code".into(), ..Add::default() }).await.unwrap();
    let folder = PathBuf::from(agent.folder.clone().unwrap());
    let created = s.host.new_session(&agent.id, json!({ "cwd": folder, "mcpServers": [] })).await.unwrap();
    let session = created["sessionId"].as_str().unwrap().to_owned();
    let proj = s.home.join("workspaces").join("proj");
    std::fs::create_dir_all(&proj).unwrap();
    let (said, updates) = turn(&s.host, &agent.id, &session, "List the names of the tools you have from the MCP server named host, then use it to work in ~/workspaces/proj from now on.").await;
    eprintln!("-- said: {said}");
    for u in &updates {
        if u["sessionUpdate"] == "tool_call" {
            eprintln!("-- tool call: {} {}", u["title"], u["rawInput"]);
        }
    }
    assert!(said.contains("Now working in ~/workspaces/proj."), "{said}");
    assert_eq!(s.host.folder(&agent.id, &session).await.as_deref(), proj.to_str());
    let (said, _) = turn(&s.host, &agent.id, &session, "Run `pwd` with your shell tool and reply with only its output.").await;
    eprintln!("-- said: {said}");
    assert!(said.contains(proj.to_str().unwrap()), "{said}");
}

/// The phone contract, the hub's path to a computer: which coding agents it
/// can add, adding one, its folder on a chat, and removing it.
#[tokio::test]
async fn the_phone_contract_adds_and_removes_agents() {
    let s = setup(None);
    let contract = link_core::phone::Contract::new("acp", "ACP agent", s.host.clone(), None);
    let listed = contract.rest("GET", "/api/v1/agents").await.unwrap();
    assert_eq!(listed["runtimes"], json!([{ "id": "claude-code", "name": "Claude Code" }]));
    let added = contract.rest("POST", "/api/v1/runtimes/claude-code/agents").await.unwrap();
    let folder = s.home.join("NeboAI").join("claude-code");
    assert_eq!(added["agent"]["id"], "claude-code");
    assert_eq!(added["agent"]["name"], "Claude Code");
    assert_eq!(added["agent"]["description"], format!("Works in {}", folder.display()));
    let refused = contract.rest("POST", "/api/v1/runtimes/codex/agents").await.unwrap_err();
    assert_eq!((refused.status, refused.message.as_str()), (400, "Codex isn't installed on this computer."));

    let created = contract.rest("POST", "/api/v1/agents/claude-code/chats").await.unwrap();
    let chat = created["chat"]["id"].as_str().unwrap().to_owned();
    let settings = contract.rest("GET", &format!("/api/v1/chats/{chat}")).await.unwrap();
    assert_eq!(settings["folder"], folder.to_str().unwrap());

    let removed = contract.rest("DELETE", "/api/v1/agents/claude-code").await.unwrap();
    assert_eq!(removed, json!({ "removed": "claude-code" }));
    assert!(folder.is_dir(), "its folder stays");
    assert_eq!(contract.rest("DELETE", "/api/v1/agents/claude-code").await.unwrap_err().status, 404);
}

impl Record {
    fn agents(&self) -> Vec<Kept> {
        Keeper::agents(self)
    }
}
