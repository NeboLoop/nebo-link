//! A host for the tests: the conformance suite's scripted ACP agent, run in
//! process behind link-core's host, served by an OAL host.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use link_core::backend::{Agent, AgentMessage, Backend, BoxFuture, Error, ErrorObject, FromAgent, Inbox, Reply};
use link_core::acp::{Acp, Client, Settings};
use link_core::host::Host;
use link_core::keep::{CodingAgent, Installable, Keeper, Kept};
use link_core::roster::{Member, Roster};
use nebo_runtimes::RuntimeCommand;
use nebo_runtimes::acp::Agent as AcpAgent;
use oal_conformance::fake_agent;
use oal_host::{Config, OalHost, Runtime};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

/// The agent's id on the host.
pub const AGENT: &str = "fake";
/// The runtime it is installed as, which a device can add more agents of.
pub const RUNTIME: &str = oal_conformance::fake_host::RUNTIME;

/// The scripted agent over channels, speaking ACP as a process would.
pub struct FakeAcp {
    to_agent: mpsc::UnboundedSender<Value>,
    waiting: Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>,
    next: AtomicU64,
    inbox: Arc<Mutex<Option<Inbox>>>,
    capabilities: Mutex<Value>,
}

impl FakeAcp {
    pub fn start() -> Arc<Self> {
        let (to_agent, agent_in) = mpsc::unbounded_channel();
        let (agent_out, mut from_agent) = mpsc::unbounded_channel::<Value>();
        tokio::spawn(fake_agent::run(agent_in, agent_out));
        let fake = Arc::new(Self {
            to_agent,
            waiting: Arc::default(),
            next: AtomicU64::new(1000),
            inbox: Arc::default(),
            capabilities: Mutex::new(json!({})),
        });
        let reader = fake.clone();
        tokio::spawn(async move {
            let mut replies = 0u64;
            while let Some(msg) = from_agent.recv().await {
                let method = msg["method"].as_str().map(str::to_owned);
                match (method.as_deref(), msg.get("id").cloned()) {
                    (None, Some(id)) => {
                        let waiter = id.as_u64().and_then(|id| reader.waiting.lock().unwrap().remove(&id));
                        if let Some(waiter) = waiter {
                            let _ = waiter.send(msg);
                        }
                    }
                    (Some("session/update"), None) => reader.tell(AgentMessage::Update {
                        session_id: msg["params"]["sessionId"].as_str().unwrap_or("").to_owned(),
                        update: msg["params"]["update"].clone(),
                    }),
                    (Some("session/request_permission"), Some(id)) => {
                        replies += 1;
                        let (reply, answer) = Reply::new(replies);
                        let to_agent = reader.to_agent.clone();
                        tokio::spawn(async move {
                            let response = answer.await.unwrap_or_else(|_| json!({ "outcome": { "outcome": "cancelled" } }));
                            let _ = to_agent.send(json!({ "jsonrpc": "2.0", "id": id, "result": response }));
                        });
                        reader.tell(AgentMessage::Permission {
                            session_id: msg["params"]["sessionId"].as_str().unwrap_or("").to_owned(),
                            params: msg["params"].clone(),
                            words: None,
                            reply,
                        });
                    }
                    _ => {}
                }
            }
        });
        fake
    }

    fn tell(&self, message: AgentMessage) {
        let inbox = self.inbox.lock().unwrap().clone();
        if let Some(inbox) = inbox {
            inbox(FromAgent {
                agent: AGENT.into(),
                message,
            });
        }
    }

    async fn call(&self, method: &str, params: Value) -> Value {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.waiting.lock().unwrap().insert(id, tx);
        let _ = self.to_agent.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }));
        rx.await.unwrap_or_else(|_| json!({ "error": { "code": -1, "message": "the agent stopped" } }))
    }
}

impl Backend for FakeAcp {
    fn ready(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let init = self
                .call("initialize", json!({ "protocolVersion": 1, "clientCapabilities": { "fs": { "readTextFile": false, "writeTextFile": false }, "terminal": false } }))
                .await;
            *self.capabilities.lock().unwrap() = init["result"]["agentCapabilities"].clone();
            Ok(())
        })
    }

    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>> {
        let capabilities = self.capabilities.lock().unwrap().clone();
        Box::pin(async move {
            Ok(vec![Agent {
                id: AGENT.into(),
                name: "Fake Agent".into(),
                description: String::new(),
                is_default: true,
                folder: Some(oal_conformance::fake_host::FOLDER.into()),
                capabilities,
                modes: None,
                offline_reason: None,
            }])
        })
    }

    fn connect(&self, inbox: Inbox) {
        *self.inbox.lock().unwrap() = Some(inbox);
    }

    fn request<'a>(&'a self, _agent: &'a str, method: &'a str, params: Value) -> BoxFuture<'a, Result<Value, ErrorObject>> {
        Box::pin(async move {
            let answer = self.call(method, params).await;
            match answer.get("error") {
                Some(e) => Err(ErrorObject::new(e["code"].as_i64().unwrap_or(-32603), e["message"].as_str().unwrap_or(""))),
                None => Ok(answer["result"].clone()),
            }
        })
    }

    fn notify(&self, _agent: &str, method: &str, params: Value) {
        let _ = self.to_agent.send(json!({ "jsonrpc": "2.0", "method": method, "params": params }));
    }

    fn model<'a>(&'a self, _agent: &'a str, _session: Option<&'a str>) -> BoxFuture<'a, Result<String, Error>> {
        Box::pin(async { Ok("fake".to_owned()) })
    }
}

/// Set when this test binary runs as the scripted agent.
const SCRIPTED: &str = "OAL_HOST_SCRIPTED_AGENT";

/// Not a test when a host starts it: the conformance suite's scripted ACP
/// agent as a process (this test binary again, with `SCRIPTED` set), the
/// agent an added `oal-fake-agent` runs.
#[test]
fn scripted_agent() {
    if std::env::var_os(SCRIPTED).is_none() {
        return;
    }
    // The harness printed "test … " without a newline: end that line, so
    // every message is a line of its own.
    println!();
    tokio::runtime::Runtime::new().unwrap().block_on(fake_agent::stdio());
}

/// How an added agent starts the scripted agent.
pub fn scripted_command() -> RuntimeCommand {
    RuntimeCommand {
        program: std::env::current_exe().unwrap().to_string_lossy().into_owned(),
        args: ["common::scripted_agent", "--exact", "--nocapture", "--test-threads=1"].map(String::from).to_vec(),
        env: vec![(SCRIPTED.into(), "1".into())],
    }
}

const CLIENT: Client = Client { name: "oal-host tests", version: "0" };

/// What the host keeps the agents a device adds in: `oal-fake-agent` (the
/// scripted agent, as a process) is addable; `fake` is not the keeper's.
pub struct TestKeeper {
    dir: PathBuf,
    pub agents: Mutex<Vec<CodingAgent>>,
}

impl TestKeeper {
    pub fn new(dir: PathBuf) -> Arc<Self> {
        Arc::new(Self { dir, agents: Mutex::new(Vec::new()) })
    }
}

impl Keeper for TestKeeper {
    fn client(&self) -> Client {
        CLIENT
    }

    fn addable(&self) -> Vec<Installable> {
        vec![Installable {
            id: RUNTIME.into(),
            name: "Fake Agent".into(),
            agent: AcpAgent::Other,
            command: scripted_command(),
        }]
    }

    fn agents(&self) -> Vec<Kept> {
        self.agents
            .lock()
            .unwrap()
            .iter()
            .map(|a| Kept { id: a.id.clone(), label: a.label.clone(), runtime: RUNTIME.into(), folder: Some(a.acp.workdir.clone()) })
            .collect()
    }

    fn keep(&self, agent: &CodingAgent) -> Result<Member, String> {
        self.agents.lock().unwrap().push(agent.clone());
        let backend = Acp::new(Settings {
            agent: agent.agent,
            name: agent.label.clone(),
            command: agent.acp.command(),
            workdir: agent.acp.workdir.clone(),
            log: self.dir.join("logs").join(format!("{}.log", agent.id)),
            chats_file: self.dir.join("agents").join(&agent.id).join("acp-chats.json"),
            client: CLIENT,
        });
        Ok(Member { id: agent.id.clone(), label: agent.label.clone(), runtime: RUNTIME.into(), backend: Arc::new(backend) })
    }

    fn forget(&self, id: &str) -> Result<(), String> {
        let mut agents = self.agents.lock().unwrap();
        let before = agents.len();
        agents.retain(|a| a.id != id);
        match agents.len() < before {
            true => Ok(()),
            false => Err(format!("{id} is removed on the computer itself.")),
        }
    }
}

/// An OAL host hosting the scripted agent, its state in `dir`; a device can
/// add more of it, each in a folder of its own under `dir/home/NeboAI`.
pub async fn host(dir: &Path) -> Arc<OalHost> {
    let member = Member {
        id: AGENT.into(),
        label: "Fake Agent".into(),
        runtime: RUNTIME.into(),
        backend: FakeAcp::start(),
    };
    let host = Host::new(Arc::new(Roster::new(vec![member])));
    std::fs::create_dir_all(dir.join("home")).unwrap();
    host.set_home(dir.join("home").canonicalize().unwrap());
    host.set_keeper(TestKeeper::new(dir.join("added")));
    let oal = OalHost::new(
        Config {
            host_id: "h-test".into(),
            host_name: "Test Host".into(),
            software: ("oal-host tests".into(), "0".into()),
            keys: oal_secure::KeyStore::open(dir.join("keys")).unwrap(),
            seen_file: dir.join("seen.json"),
            runtimes: Arc::new(|| {
                vec![Runtime {
                    id: RUNTIME.into(),
                    name: "Fake Agent".into(),
                    kind: "acp".into(),
                    version: None,
                    addable: false,
                }]
            }),
        },
        host,
    )
    .unwrap();
    oal.warm_up().await;
    oal
}
