//! A host for the tests: the conformance suite's scripted ACP agent, run in
//! process behind link-core's host, served by an OAL host.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use link_core::backend::{Agent, AgentMessage, Backend, BoxFuture, Error, ErrorObject, FromAgent, Inbox, Reply};
use link_core::host::Host;
use link_core::roster::{Member, Roster};
use oal_conformance::fake_agent;
use oal_host::{Config, OalHost, Runtime};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};

/// The agent's id on the host.
pub const AGENT: &str = "fake";

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

/// An OAL host hosting the scripted agent, its state in `dir`.
pub async fn host(dir: &Path) -> Arc<OalHost> {
    let member = Member {
        id: AGENT.into(),
        label: "Fake Agent".into(),
        runtime: "oal-fake-agent".into(),
        backend: FakeAcp::start(),
    };
    let host = Host::new(Arc::new(Roster::new(vec![member])));
    let oal = OalHost::new(
        Config {
            host_id: "h-test".into(),
            host_name: "Test Host".into(),
            software: ("oal-host tests".into(), "0".into()),
            keys: dir.join("keys"),
            seen_file: dir.join("seen.json"),
            runtimes: Arc::new(|| {
                vec![Runtime {
                    id: "oal-fake-agent".into(),
                    name: "Fake Agent".into(),
                    kind: "acp".into(),
                    version: None,
                }]
            }),
        },
        host,
    )
    .unwrap();
    oal.warm_up().await;
    oal
}
