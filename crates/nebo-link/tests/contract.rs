//! The chat contract on the link's listener, in front of a fake runtime and
//! a fake hub: the phone's whole flow (roster, chat, a streamed turn with
//! tool events, the transcript, an approval as an ask card with its hub
//! inbox item, cancel) against a fake Hermes API server and a fake OpenClaw
//! gateway, plus the Hermes version gate on the transcript
//! (`conversation_history` sent to 0.19.0, not to 0.21.2+).
//!
//! The Hermes frames are the shapes `gateway/platforms/api_server.py` wrote
//! on the live v0.19.0 server on 2026-09-26 (no `id:` lines, no
//! `request_id` on `approval.request`) and at hermes-agent `d0288be5b3`.
//! The OpenClaw frames are the ones the 2026.9.6 gateway sent the
//! 2026-09-26 spike (`nebo-runtimes/tests/fixtures/openclaw-gateway-frames.json`).

use std::collections::HashSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use nebo_link::contract::backend::Backend;
use nebo_link::contract::hermes::Hermes;
use nebo_link::contract::openclaw::Openclaw;
use nebo_link::contract::{Contract, Inbox};
use nebo_link::proxy::{self, Body, BoxError, Control, FORWARDED_FOR, Target};
use nebo_runtimes::openclaw::gateway::{Connect, FileDeviceStore};
use nebo_runtimes::{PathMode, ProxyAccess, ProxyRoute};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

const KEY: &str = "0123456789abcdef0123456789abcdef";
const STAMP: &str = "s3cret";
const BOT: &str = "bot-1";

// -- A fake Hermes API server ------------------------------------------------

/// One request the fake saw.
#[derive(Debug, Clone)]
struct Seen {
    method: String,
    path: String,
    body: Value,
}

struct FakeHermes {
    version: &'static str,
    seen: Mutex<Vec<Seen>>,
    runs: Mutex<u32>,
    sessions: Mutex<u32>,
    /// The messages stored on each session, as the server returns them.
    transcripts: Mutex<Vec<(String, Vec<Value>)>>,
    approved: Notify,
    stopped: Notify,
}

impl FakeHermes {
    fn new(version: &'static str) -> Arc<Self> {
        Arc::new(Self {
            version,
            seen: Mutex::new(Vec::new()),
            runs: Mutex::new(0),
            sessions: Mutex::new(0),
            transcripts: Mutex::new(vec![("api_old".into(), Vec::new())]),
            approved: Notify::new(),
            stopped: Notify::new(),
        })
    }

    /// The requests to `path` (query string aside).
    fn seen(&self, method: &str, path: &str) -> Vec<Seen> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.method == method && s.path.split('?').next() == Some(path))
            .cloned()
            .collect()
    }

    fn transcript(&self, session: &str) -> Vec<Value> {
        self.transcripts
            .lock()
            .unwrap()
            .iter()
            .find(|(id, _)| id == session)
            .map(|(_, rows)| rows.clone())
            .unwrap_or_default()
    }

    fn store(&self, session: &str, rows: Vec<Value>) {
        let mut transcripts = self.transcripts.lock().unwrap();
        match transcripts.iter_mut().find(|(id, _)| id == session) {
            Some((_, stored)) => stored.extend(rows),
            None => transcripts.push((session.to_owned(), rows)),
        }
    }

    async fn respond(self: Arc<Self>, req: Request<Incoming>) -> Response<Body> {
        let method = req.method().to_string();
        let path_and_query = req
            .uri()
            .path_and_query()
            .map(|p| p.to_string())
            .unwrap_or_default();
        let path = req.uri().path().to_owned();
        let auth = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let bytes = req.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        self.seen.lock().unwrap().push(Seen {
            method: method.clone(),
            path: path_and_query.clone(),
            body: body.clone(),
        });
        assert_eq!(
            auth.as_deref(),
            Some(&*format!("Bearer {KEY}")),
            "{method} {path}"
        );
        let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
        match (method.as_str(), segments.as_slice()) {
            ("GET", ["health"]) => json_response(
                200,
                json!({ "status": "ok", "platform": "hermes-agent", "version": self.version }),
            ),
            ("GET", ["v1", "capabilities"]) => json_response(200, capabilities(self.version)),
            ("GET", ["api", "sessions"]) => json_response(
                200,
                json!({ "object": "list", "data": [session("api_old", 2)], "limit": 100, "offset": 0, "has_more": false }),
            ),
            ("POST", ["api", "sessions"]) => {
                let mut sessions = self.sessions.lock().unwrap();
                *sessions += 1;
                let id = format!("api_new_{sessions}");
                self.store(&id, Vec::new());
                json_response(
                    201,
                    json!({ "object": "hermes.session", "session": session(&id, 0) }),
                )
            }
            ("GET", ["api", "sessions", id]) => json_response(
                200,
                json!({ "object": "hermes.session", "session": session(id, 0) }),
            ),
            ("GET", ["api", "sessions", id, "messages"]) => {
                let known = self
                    .transcripts
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(s, _)| s == id);
                if !known {
                    return json_response(
                        404,
                        json!({ "error": { "message": format!("Session not found: {id}"), "code": "session_not_found" } }),
                    );
                }
                let rows = self.transcript(id);
                json_response(
                    200,
                    json!({ "object": "list", "session_id": id, "data": rows, "pagination": { "limit": 500, "offset": 0, "order": "latest", "returned": rows.len() } }),
                )
            }
            ("POST", ["v1", "runs"]) => {
                let mut runs = self.runs.lock().unwrap();
                *runs += 1;
                let run_id = format!("run_{runs}");
                json_response(
                    202,
                    json!({ "run_id": run_id, "status": "started", "replayed": false }),
                )
            }
            ("GET", ["v1", "runs", run_id, "events"]) => self.events(run_id),
            ("GET", ["v1", "runs", run_id]) => json_response(
                200,
                json!({ "object": "hermes.run", "run_id": run_id, "status": "completed", "output": "pong" }),
            ),
            ("POST", ["v1", "runs", run_id, "approval"]) => {
                self.approved.notify_one();
                json_response(
                    200,
                    json!({ "object": "hermes.run.approval_response", "run_id": run_id, "choice": body["choice"], "resolved": 1 }),
                )
            }
            ("POST", ["v1", "runs", run_id, "stop"]) => {
                self.stopped.notify_one();
                json_response(200, json!({ "run_id": run_id, "status": "stopping" }))
            }
            _ => json_response(
                404,
                json!({ "error": { "message": format!("no route {method} {path}") } }),
            ),
        }
    }

    /// The run's SSE stream: run 1 answers with a tool call, run 2 asks for
    /// approval and waits for the answer, run 3 streams until stopped. The
    /// frames are as v0.19.0 writes them: no `id:` lines.
    fn events(self: Arc<Self>, run_id: &str) -> Response<Body> {
        let (tx, rx) = mpsc::channel::<String>(16);
        let run_id = run_id.to_owned();
        let server = self.clone();
        tokio::spawn(async move {
            let send = |text: String| {
                let tx = tx.clone();
                async move {
                    let _ = tx.send(text).await;
                }
            };
            let frame = |event: &str, fields: Value| {
                let mut data =
                    json!({ "event": event, "run_id": run_id, "timestamp": 1790400011.5 });
                if let (Value::Object(data), Value::Object(fields)) = (&mut data, fields) {
                    data.extend(fields);
                }
                format!("data: {data}\n\n")
            };
            send(": open\n\n".into()).await;
            match run_id.as_str() {
                "run_1" => {
                    send(frame(
                        "tool.started",
                        json!({ "tool": "terminal", "preview": "{\"command\": \"ls\"}" }),
                    ))
                    .await;
                    send(frame("tool.completed", json!({ "tool": "terminal", "duration": 0.812, "error": false, "preview": "Cargo.toml\nsrc" }))).await;
                    send(frame(
                        "reasoning.available",
                        json!({ "text": "The user wants pong." }),
                    ))
                    .await;
                    send(frame("message.delta", json!({ "delta": "po" }))).await;
                    send(frame("message.delta", json!({ "delta": "ng" }))).await;
                    server.store(
                        "api_new_1",
                        vec![
                            json!({ "id": 11, "session_id": "api_new_1", "role": "user", "content": "Reply with exactly: pong", "timestamp": 1790400001.0 }),
                            json!({ "id": 12, "session_id": "api_new_1", "role": "assistant", "content": "", "timestamp": 1790400002.0, "tool_calls": [{ "id": "call_1", "type": "function", "function": { "name": "terminal", "arguments": "{\"command\": \"ls\"}" } }] }),
                            json!({ "id": 13, "session_id": "api_new_1", "role": "tool", "content": "Cargo.toml\nsrc", "tool_call_id": "call_1", "tool_name": "terminal", "timestamp": 1790400002.5 }),
                            json!({ "id": 14, "session_id": "api_new_1", "role": "assistant", "content": [{ "type": "text", "text": "pong" }], "timestamp": 1790400003.0, "finish_reason": "stop" }),
                        ],
                    );
                    send(frame("run.completed", json!({ "completed": true, "partial": false, "interrupted": false, "output": "pong", "usage": { "input_tokens": 1520, "output_tokens": 4, "total_tokens": 1524 } }))).await;
                }
                "run_2" => {
                    let mut ask = json!({ "command": "rm -rf ./scratch", "pattern_key": "rm -rf", "description": "Recursive delete", "allow_permanent": true, "choices": ["once", "session", "always", "deny"] });
                    if server.version != "0.19.0" {
                        ask["request_id"] = json!("8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f");
                    }
                    send(frame("approval.request", ask)).await;
                    server.approved.notified().await;
                    let mut responded = json!({ "choice": "deny", "resolved": 1 });
                    if server.version != "0.19.0" {
                        responded["request_id"] = json!("8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f");
                    }
                    send(frame("approval.responded", responded)).await;
                    send(frame("tool.completed", json!({ "tool": "terminal", "duration": 0.01, "error": true, "preview": "BLOCKED: Command denied by user." }))).await;
                    send(frame(
                        "message.delta",
                        json!({ "delta": "I won't run that." }),
                    ))
                    .await;
                    send(frame("run.completed", json!({ "completed": true, "output": "I won't run that.", "usage": { "input_tokens": 200, "output_tokens": 6, "total_tokens": 206 } }))).await;
                }
                _ => {
                    send(frame("message.delta", json!({ "delta": "1\n2\n" }))).await;
                    server.stopped.notified().await;
                    send(frame(
                        "run.cancelled",
                        json!({ "completed": false, "partial": true, "interrupted": true }),
                    ))
                    .await;
                }
            }
            send(": stream closed\n\n".into()).await;
        });
        let stream = tokio_stream_from(rx);
        Response::builder()
            .status(200)
            .header("content-type", "text/event-stream")
            .body(BodyExt::boxed(StreamBody::new(stream)))
            .unwrap()
    }
}

fn tokio_stream_from(
    rx: mpsc::Receiver<String>,
) -> impl futures::Stream<Item = Result<Frame<Bytes>, BoxError>> {
    futures::stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|text| (Ok(Frame::data(Bytes::from(text))), rx))
    })
}

fn capabilities(version: &str) -> Value {
    let mut features = json!({
        "chat_completions": true, "run_submission": true, "run_status": true, "run_events_sse": true,
        "run_stop": true, "run_approval_response": true, "tool_progress_events": true,
        "approval_events": true, "session_resources": true, "session_chat": true,
        "session_chat_streaming": true, "session_fork": true,
    });
    if version != "0.19.0" {
        features["run_steer"] = json!(true);
    }
    json!({
        "object": "hermes.api_server.capabilities", "platform": "hermes-agent", "model": "hermes-agent",
        "auth": { "type": "bearer", "required": true }, "features": features,
        "endpoints": { "runs": { "method": "POST", "path": "/v1/runs" } },
    })
}

fn session(id: &str, messages: u64) -> Value {
    json!({
        "id": id, "source": "api_server", "model": "nebo-1", "title": if messages > 0 { "First chat" } else { "" },
        "started_at": 1790000000.5, "message_count": messages, "last_active": 1790000010.0,
        "preview": if messages > 0 { "Reply with exactly: pong" } else { "" }, "pinned": false, "archived": false, "hidden": false,
    })
}

fn json_response(status: u16, value: Value) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(
            Full::new(Bytes::from(value.to_string()))
                .map_err(|never| match never {})
                .boxed(),
        )
        .unwrap()
}

async fn serve_hermes(server: Arc<FakeHermes>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let server = server.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |req| {
                    let server = server.clone();
                    async move { Ok::<_, Infallible>(server.respond(req).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    addr
}

// -- A fake hub: the owner's inbox ------------------------------------------

async fn serve_hub() -> (String, Arc<Mutex<Vec<Value>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let items: Arc<Mutex<Vec<Value>>> = Arc::default();
    let seen = items.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = seen.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |req: Request<Incoming>| {
                    let seen = seen.clone();
                    async move {
                        assert_eq!(req.uri().path(), "/api/v1/bots/self/inbox");
                        assert_eq!(
                            req.headers().get("authorization").unwrap(),
                            "Bearer bot-token"
                        );
                        let bytes = req.into_body().collect().await.unwrap().to_bytes();
                        seen.lock()
                            .unwrap()
                            .push(serde_json::from_slice(&bytes).unwrap());
                        Ok::<_, Infallible>(json_response(200, json!({ "status": "ok" })))
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    (url, items)
}

// -- The link -----------------------------------------------------------------

struct NoControl;

impl Control for NoControl {
    fn status(&self) -> Value {
        json!({})
    }
    async fn set_models(&self, _enabled: bool) -> Result<Value, String> {
        Ok(json!({}))
    }
}

/// The link's listener with the contract in front of `backend`, the way
/// `run.rs` serves it; the runtime UI target is an unused port.
async fn serve_contract(
    runtime: (&'static str, &'static str),
    backend: Arc<dyn Backend>,
    hub: &str,
) -> SocketAddr {
    let unused = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let target = Target {
        upstream: Some(unused),
        base_path: format!("/t/{BOT}"),
        route: ProxyRoute {
            path_mode: PathMode::StripWithForwardedPrefix,
            origin: None,
            identity_header: None,
        },
        identity: "owner-1".into(),
        runtime_name: runtime.1,
    };
    let (_token_tx, token_rx) = watch::channel("bot-token".to_string());
    let contract = Contract::new(
        runtime.0,
        runtime.1,
        BOT,
        backend,
        Some(Inbox::new(hub, BOT, token_rx)),
    );
    let listener = proxy::bind_loopback("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(proxy::serve(
        listener,
        target,
        STAMP.into(),
        Arc::new(NoControl),
        Some(contract),
    ));
    addr
}

/// The contract in front of a Hermes API server at `hermes`.
async fn start_link(hermes: SocketAddr, hub: &str) -> SocketAddr {
    let backend = Hermes::new(&format!("http://{hermes}"), KEY, vec!["coder".into()]);
    serve_contract(("hermes", "Hermes"), Arc::new(backend), hub).await
}

/// One stamped request, as the tunnel delivers it.
async fn call(addr: SocketAddr, method: &str, path: &str, body: &str) -> (StatusCode, Value) {
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(
        TcpStream::connect(addr).await.unwrap(),
    ))
    .await
    .unwrap();
    tokio::spawn(conn);
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "neboai.com")
        .header("x-nebo-tunnel-auth", STAMP)
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn get(addr: SocketAddr, path: &str) -> Value {
    let (status, value) = call(addr, "GET", path, "").await;
    assert_eq!(status, StatusCode::OK, "{path}: {value}");
    value
}

/// The phone's socket: connected, authenticated, and read one frame at a time.
struct Phone {
    ws: tokio_tungstenite::WebSocketStream<TcpStream>,
}

impl Phone {
    async fn connect(addr: SocketAddr) -> Self {
        let mut request =
            tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(
                "ws://neboai.com/ws",
            )
            .unwrap();
        request
            .headers_mut()
            .insert("x-nebo-tunnel-auth", STAMP.parse().unwrap());
        let (ws, resp) =
            tokio_tungstenite::client_async(request, TcpStream::connect(addr).await.unwrap())
                .await
                .unwrap();
        assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
        let mut phone = Self { ws };
        phone.send("auth", json!({ "token": "owner-jwt" })).await;
        let auth = phone.next().await;
        assert_eq!(auth["type"], "auth_ok");
        phone
    }

    async fn send(&mut self, kind: &str, data: Value) {
        let frame = json!({ "type": kind, "data": data, "message_id": uuid::Uuid::new_v4().to_string(), "timestamp": "2026-09-26T00:00:00Z" });
        self.ws
            .send(Message::text(frame.to_string()))
            .await
            .unwrap();
    }

    async fn next(&mut self) -> Value {
        let message = tokio::time::timeout(Duration::from_secs(10), self.ws.next())
            .await
            .expect("an event within 10 s")
            .unwrap()
            .unwrap();
        serde_json::from_str(message.to_text().unwrap()).unwrap()
    }

    /// Events until one of `kind`, returned with it last.
    async fn until(&mut self, kind: &str) -> Vec<Value> {
        let mut events = Vec::new();
        loop {
            let event = self.next().await;
            let done = event["type"] == kind;
            events.push(event);
            if done {
                return events;
            }
        }
    }
}

fn kinds(events: &[Value]) -> Vec<&str> {
    events.iter().map(|e| e["type"].as_str().unwrap()).collect()
}

async fn eventually(items: &Mutex<Vec<Value>>, count: usize) -> Vec<Value> {
    for _ in 0..100 {
        let got = items.lock().unwrap().clone();
        if got.len() >= count {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the hub got {:?}, not {count} items", items.lock().unwrap());
}

// -- Tests --------------------------------------------------------------------

#[tokio::test]
async fn the_phone_flow_against_hermes_0_19_0() {
    let hermes = FakeHermes::new("0.19.0");
    let hermes_addr = serve_hermes(hermes.clone()).await;
    let (hub_url, inbox) = serve_hub().await;
    let link = start_link(hermes_addr, &hub_url).await;

    // The probe: reachable, every flag present.
    let health = get(link, "/health").await;
    assert_eq!(health["runtime"], "hermes");
    assert_eq!(health["chat"], true);
    assert!(health["version"].is_string());

    // The roster: the default profile as `assistant`, the named one beside.
    let roster = get(link, "/api/v1/agents").await;
    assert_eq!(roster["primaryChristened"], true);
    let agents = roster["agents"].as_array().unwrap();
    assert_eq!(agents.len(), 2);
    assert_eq!(agents[0]["id"], "assistant");
    assert_eq!(agents[0]["name"], "Hermes");
    assert_eq!(agents[0]["editable"], false);
    assert_eq!(agents[0]["isApp"], false);
    assert_eq!(agents[1]["id"], "coder");
    let detail = get(link, "/api/v1/agents/assistant").await;
    assert_eq!(detail["agent"]["nameLocked"], true);
    let (status, refused) = call(link, "GET", "/api/v1/agents/nobody", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(refused["error"], "No agent nobody on this bot.");

    // Chats: the runtime's sessions, and a new one on demand.
    let chats = get(link, "/api/v1/agents/assistant/chats").await;
    assert_eq!(chats["chats"][0]["id"], "api_old");
    assert_eq!(chats["chats"][0]["title"], "First chat");
    assert_eq!(chats["chats"][0]["messageCount"], 2);
    assert!(
        chats["chats"][0]["relativeTime"]
            .as_str()
            .unwrap()
            .ends_with("d ago")
    );
    let (status, created) = call(link, "POST", "/api/v1/agents/assistant/chats", "{}").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(created["chat"]["title"], "New chat");
    assert_eq!(created["chat"]["id"], "api_new_1");
    assert_eq!(hermes.seen("POST", "/api/sessions").len(), 1);

    // Nothing else of Nebo's is here, and the model is the runtime's.
    let (status, refused) = call(link, "GET", "/api/v1/agents/assistant/runs", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(refused["error"], "not on a linked bot");
    let (status, refused) = call(link, "PUT", "/api/v1/chats/api_new_1", r#"{"model":"x"}"#).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(refused["error"], "The model is chosen in Hermes.");
    assert_eq!(
        get(link, "/api/v1/chats/api_new_1").await["model"],
        "nebo-1"
    );
    assert_eq!(
        get(link, "/api/v1/models").await["models"]["hermes"][0]["id"],
        "hermes-agent"
    );

    // A turn on the chat the phone created. 0.19.0 runs with an empty
    // history, so the link sends the session's own (empty, so far).
    let mut phone = Phone::connect(link).await;
    let session_id = "agent:assistant:thread:api_new_1";
    phone
        .send("chat", json!({ "prompt": "Reply with exactly: pong", "agent_id": "assistant", "session_id": session_id }))
        .await;
    let events = phone.until("chat_complete").await;
    assert_eq!(
        kinds(&events),
        [
            "tool_start",
            "tool_result",
            "thinking",
            "chat_stream",
            "chat_stream",
            "usage",
            "chat_complete"
        ]
    );
    for event in &events {
        assert_eq!(event["data"]["agent_id"], "assistant", "{event}");
        assert_eq!(event["data"]["session_id"], session_id, "{event}");
        assert!(event["data"]["turn_id"].is_string(), "{event}");
    }
    assert_eq!(events[0]["data"]["tool"], "terminal");
    assert_eq!(events[0]["data"]["input"]["command"], "ls");
    assert_eq!(events[1]["data"]["tool_id"], events[0]["data"]["tool_id"]);
    assert_eq!(events[1]["data"]["result"], "Cargo.toml\nsrc");
    assert_eq!(events[1]["data"]["is_error"], false);
    assert_eq!(events[1]["data"]["duration_ms"], 812);
    assert_eq!(events[2]["data"]["text"], "The user wants pong.");
    assert_eq!(events[3]["data"]["content"], "po");
    assert_eq!(events[4]["data"]["content"], "ng");
    assert_eq!(events[5]["data"]["input_tokens"], 1520);
    assert_eq!(events[6]["data"]["stop_reason"], "end_turn");
    let run = &hermes.seen("POST", "/v1/runs")[0].body;
    assert_eq!(run["input"], "Reply with exactly: pong");
    assert_eq!(run["session_id"], "api_new_1");
    assert_eq!(
        run["conversation_history"],
        json!([]),
        "0.19.0 gets the session's history, empty so far"
    );

    // The transcript, from the runtime's store, in the phone's row shape.
    let page = get(link, "/api/v1/chats/api_new_1/messages").await;
    assert_eq!(page["hasMore"], false);
    assert!(page["activeRun"].is_null() && page["pendingAsk"].is_null());
    let rows = page["messages"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0]["role"], "user");
    assert_eq!(rows[0]["createdAt"], 1790400001);
    let calls: Value = serde_json::from_str(rows[1]["toolCalls"].as_str().unwrap()).unwrap();
    assert_eq!(calls[0]["id"], "call_1");
    assert_eq!(rows[1]["metadata"]["toolCalls"][0]["name"], "terminal");
    assert_eq!(rows[1]["metadata"]["contentBlocks"][0]["toolCallIndex"], 0);
    let results: Value = serde_json::from_str(rows[2]["toolResults"].as_str().unwrap()).unwrap();
    assert_eq!(results[0]["tool_call_id"], "call_1");
    assert_eq!(rows[3]["content"], "pong");
    let (status, _) = call(link, "GET", "/api/v1/chats/api_none/messages", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // A chat without a session: the link creates one and says so first.
    phone
        .send("chat", json!({ "prompt": "Using the terminal, run: rm -rf ./scratch", "agent_id": "assistant" }))
        .await;
    let created = phone.next().await;
    assert_eq!(created["type"], "chat_created");
    assert_eq!(created["data"]["agent_id"], "assistant");
    assert_eq!(
        created["data"]["session_id"],
        "agent:assistant:thread:api_new_2"
    );

    // The approval: an ask card with the runtime's choices, a notification,
    // an inbox item on the hub, and the transcript shows the card too.
    let ask = phone.until("ask_request").await.pop().unwrap();
    let request_id = ask["data"]["request_id"].as_str().unwrap().to_owned();
    assert!(
        request_id.ends_with("-ask-1"),
        "0.19.0 sends no request id; the link mints one: {request_id}"
    );
    assert!(
        ask["data"]["prompt"]
            .as_str()
            .unwrap()
            .contains("rm -rf ./scratch")
    );
    assert_eq!(
        ask["data"]["widgets"][0]["options"],
        json!([
            "Allow once",
            "Allow for this session",
            "Always allow",
            "Deny"
        ])
    );
    let notices = get(link, "/api/v1/notifications").await;
    assert_eq!(
        notices["notifications"][0]["id"],
        format!("approval:{request_id}")
    );
    assert_eq!(notices["notifications"][0]["type"], "approval");
    assert_eq!(
        notices["notifications"][0]["title"],
        "Hermes asks to run rm -rf ./scratch"
    );
    assert_eq!(notices["notifications"][0]["agentId"], "assistant");
    assert_eq!(
        notices["notifications"][0]["actionUrl"],
        "/assistant/threads/api_new_2"
    );
    assert!(notices["notifications"][0]["readAt"].is_null());
    assert_eq!(
        get(link, "/api/v1/notifications/unread-count").await["count"],
        1
    );
    let items = eventually(&inbox, 1).await;
    assert_eq!(items[0]["id"], format!("approval:{request_id}"));
    assert_eq!(items[0]["type"], "approval");
    assert_eq!(items[0]["title"], "Hermes asks to run rm -rf ./scratch");
    assert_eq!(items[0]["link"], "/t/bot-1/assistant/threads/api_new_2");
    assert_eq!(items[0]["agentId"], "assistant");
    assert_eq!(items[0]["chatId"], "api_new_2");
    assert!(items[0].get("resolved").is_none());
    let page = get(link, "/api/v1/chats/api_new_2/messages").await;
    assert!(!page["activeRun"].is_null());
    assert_eq!(page["pendingAsk"]["request_id"], request_id);
    let (status, _) = call(
        link,
        "PUT",
        &format!("/api/v1/notifications/approval:{request_id}/read"),
        "{}",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        get(link, "/api/v1/notifications/unread-count").await["count"],
        0
    );
    // The second run carried the first chat's history? No: it is a new
    // session, whose history is empty; the first session's turn is stored.
    let run = &hermes.seen("POST", "/v1/runs")[1].body;
    assert_eq!(run["session_id"], "api_new_2");
    assert_eq!(run["conversation_history"], json!([]));

    // The answer from the card resolves it in the runtime and everywhere.
    phone
        .send(
            "ask_response",
            json!({ "request_id": request_id, "value": "Deny" }),
        )
        .await;
    let events = phone.until("chat_complete").await;
    assert_eq!(
        kinds(&events),
        ["tool_result", "chat_stream", "usage", "chat_complete"]
    );
    assert_eq!(events[0]["data"]["is_error"], true);
    let answer = &hermes.seen("POST", "/v1/runs/run_2/approval")[0].body;
    assert_eq!(answer["choice"], "deny");
    assert!(
        answer.get("request_id").is_none(),
        "no id from 0.19.0, none sent back: {answer}"
    );
    assert!(
        get(link, "/api/v1/notifications").await["notifications"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let items = eventually(&inbox, 2).await;
    assert_eq!(
        items[1],
        json!({ "id": format!("approval:{request_id}"), "resolved": true })
    );

    // A turn on the same session now carries what the runtime stored on it.
    hermes.store(
        "api_new_2",
        vec![
            json!({ "id": 21, "role": "user", "content": "Using the terminal, run: rm -rf ./scratch" }),
            json!({ "id": 22, "role": "tool", "content": "BLOCKED", "tool_call_id": "call_9" }),
            json!({ "id": 23, "role": "assistant", "content": "I won't run that." }),
        ],
    );
    phone
        .send("chat", json!({ "prompt": "Count to 500.", "agent_id": "assistant", "session_id": "agent:assistant:thread:api_new_2" }))
        .await;
    let stream = phone.until("chat_stream").await.pop().unwrap();
    assert_eq!(stream["data"]["content"], "1\n2\n");
    let run = &hermes.seen("POST", "/v1/runs")[2].body;
    assert_eq!(
        run["conversation_history"],
        json!([
            { "role": "user", "content": "Using the terminal, run: rm -rf ./scratch" },
            { "role": "assistant", "content": "I won't run that." }
        ]),
        "user and assistant text only, tool rows left out"
    );

    // Cancel mid-turn: the runtime is stopped and the phone told.
    phone
        .send(
            "cancel",
            json!({ "session_id": "agent:assistant:thread:api_new_2" }),
        )
        .await;
    let cancelled = phone.until("chat_cancelled").await.pop().unwrap();
    assert_eq!(
        cancelled["data"]["session_id"],
        "agent:assistant:thread:api_new_2"
    );
    assert_eq!(hermes.seen("POST", "/v1/runs/run_3/stop").len(), 1);
    assert!(get(link, "/api/v1/chats/api_new_2/messages").await["activeRun"].is_null());

    // Nothing running: cancel still answers, so the phone stops waiting.
    phone
        .send("cancel", json!({ "agent_id": "assistant" }))
        .await;
    assert_eq!(phone.next().await["type"], "chat_cancelled");
    phone.send("ping", json!({})).await;
    assert_eq!(phone.next().await["type"], "pong");
}

#[tokio::test]
async fn hermes_0_21_2_and_later_load_the_session_themselves() {
    let hermes = FakeHermes::new("0.21.5");
    let hermes_addr = serve_hermes(hermes.clone()).await;
    let (hub_url, _inbox) = serve_hub().await;
    let link = start_link(hermes_addr, &hub_url).await;
    assert_eq!(get(link, "/health").await["chat"], true);

    let mut phone = Phone::connect(link).await;
    phone
        .send("chat", json!({ "prompt": "Reply with exactly: pong", "agent_id": "assistant", "session_id": "agent:assistant:thread:api_old" }))
        .await;
    phone.until("chat_complete").await;
    let run = &hermes.seen("POST", "/v1/runs")[0].body;
    assert_eq!(run["session_id"], "api_old");
    assert!(
        run.get("conversation_history").is_none(),
        "the runtime loads its own session: {run}"
    );
    assert!(
        hermes
            .seen("GET", "/api/sessions/api_old/messages")
            .is_empty(),
        "nothing re-read for the turn"
    );

    // With a request id from the runtime, the answer names it.
    phone
        .send("chat", json!({ "prompt": "run rm -rf", "agent_id": "assistant", "session_id": "agent:assistant:thread:api_old" }))
        .await;
    let ask = phone.until("ask_request").await.pop().unwrap();
    assert_eq!(
        ask["data"]["request_id"],
        "8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f"
    );
    phone
        .send(
            "ask_response",
            json!({ "request_id": "8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f", "value": "Allow once" }),
        )
        .await;
    phone.until("chat_complete").await;
    let answer = &hermes.seen("POST", "/v1/runs/run_2/approval")[0].body;
    assert_eq!(
        answer,
        &json!({ "choice": "once", "request_id": "8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f" })
    );
}

#[tokio::test]
async fn a_runtime_that_is_down_reads_as_could_not_connect() {
    let unused = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let (hub_url, _inbox) = serve_hub().await;
    let link = start_link(unused, &hub_url).await;
    let health = get(link, "/health").await;
    assert_eq!(health["chat"], false);
    let (status, refused) = call(link, "GET", "/api/v1/agents/assistant/chats", "").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(refused["error"], "Could not connect to Hermes. Try again.");
    let mut phone = Phone::connect(link).await;
    phone
        .send("chat", json!({ "prompt": "hi", "agent_id": "assistant", "session_id": "agent:assistant:thread:x" }))
        .await;
    let error = phone.next().await;
    assert_eq!(error["type"], "chat_error");
    assert_eq!(
        error["data"]["error"],
        "Could not connect to Hermes. Try again."
    );
}

/// Against a live Hermes API server (`NEBO_LINK_LIVE_HERMES_URL`, e.g.
/// `http://127.0.0.1:8642`, and `NEBO_LINK_LIVE_HERMES_KEY`, its
/// `API_SERVER_KEY`), with the fake hub catching the inbox items: three
/// short turns on one new chat, as the phone drives them. Costs real model
/// calls. The evidence for PRD §9.2 and §9.3 (minus the phone and push).
#[tokio::test]
#[ignore = "needs NEBO_LINK_LIVE_HERMES_URL and NEBO_LINK_LIVE_HERMES_KEY"]
async fn live_hermes_phone_flow() {
    let (Ok(url), Ok(key)) = (
        std::env::var("NEBO_LINK_LIVE_HERMES_URL"),
        std::env::var("NEBO_LINK_LIVE_HERMES_KEY"),
    ) else {
        eprintln!("NEBO_LINK_LIVE_HERMES_URL / NEBO_LINK_LIVE_HERMES_KEY not set; nothing to do");
        return;
    };
    let (hub_url, inbox) = serve_hub().await;
    let backend = Hermes::new(&url, &key, Vec::new());
    let link = serve_contract(("hermes", "Hermes"), Arc::new(backend), &hub_url).await;

    let health = get(link, "/health").await;
    eprintln!("health: {health}");
    assert_eq!(health["chat"], true);
    let roster = get(link, "/api/v1/agents").await;
    eprintln!("agents: {roster}");
    assert_eq!(roster["agents"][0]["id"], "assistant");
    let before = get(link, "/api/v1/agents/assistant/chats").await["chats"]
        .as_array()
        .unwrap()
        .len();

    // 1. A turn on a chat the link creates: deltas, then the transcript.
    let mut phone = Phone::connect(link).await;
    phone
        .send(
            "chat",
            json!({ "prompt": "Reply with exactly the word: pong", "agent_id": "assistant" }),
        )
        .await;
    let created = phone.next().await;
    eprintln!("turn 1: {created}");
    assert_eq!(created["type"], "chat_created");
    let session_id = created["data"]["session_id"].as_str().unwrap().to_owned();
    let chat_id = session_id.rsplit(":thread:").next().unwrap().to_owned();
    let events = phone.until("chat_complete").await;
    let mut text = String::new();
    for event in &events {
        eprintln!("turn 1: {event}");
        assert_eq!(event["data"]["session_id"], session_id);
        if event["type"] == "chat_stream" {
            text.push_str(event["data"]["content"].as_str().unwrap());
        }
    }
    assert!(text.to_lowercase().contains("pong"), "streamed {text:?}");
    assert!(events.iter().any(|e| e["type"] == "usage"));
    let chats = get(link, "/api/v1/agents/assistant/chats").await;
    assert_eq!(chats["chats"].as_array().unwrap().len(), before + 1);
    assert!(
        chats["chats"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["id"] == chat_id)
    );
    let page = get(link, &format!("/api/v1/chats/{chat_id}/messages")).await;
    eprintln!("transcript after turn 1: {}", page["messages"]);
    let rows = page["messages"].as_array().unwrap();
    assert_eq!(rows[0]["role"], "user");
    assert_eq!(rows[0]["content"], "Reply with exactly the word: pong");
    assert!(rows.iter().any(|r| {
        r["role"] == "assistant"
            && r["content"]
                .as_str()
                .unwrap_or("")
                .to_lowercase()
                .contains("pong")
    }));
    assert!(page["activeRun"].is_null());
    eprintln!(
        "chat model: {}",
        get(link, &format!("/api/v1/chats/{chat_id}")).await
    );

    // 2. An approval: Hermes' guardian refuses this, so it asks. Denied.
    phone
        .send("chat", json!({ "prompt": "Using the terminal tool, run exactly this command and report its output: chmod -R 777 /etc", "agent_id": "assistant", "session_id": session_id }))
        .await;
    let events = phone.until("ask_request").await;
    for event in &events {
        eprintln!("turn 2: {event}");
    }
    let ask = events.last().unwrap();
    let request_id = ask["data"]["request_id"].as_str().unwrap().to_owned();
    assert!(
        ask["data"]["widgets"][0]["options"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o == "Deny")
    );
    let notices = get(link, "/api/v1/notifications").await;
    eprintln!("notifications: {notices}");
    assert_eq!(
        notices["notifications"][0]["id"],
        format!("approval:{request_id}")
    );
    let items = eventually(&inbox, 1).await;
    eprintln!("hub inbox: {}", items[0]);
    assert_eq!(items[0]["type"], "approval");
    let page = get(link, &format!("/api/v1/chats/{chat_id}/messages")).await;
    assert_eq!(page["pendingAsk"]["request_id"], request_id);
    phone
        .send(
            "ask_response",
            json!({ "request_id": request_id, "value": "Deny" }),
        )
        .await;
    let events = phone.until("chat_complete").await;
    for event in &events {
        eprintln!("turn 2: {event}");
    }
    assert!(
        get(link, "/api/v1/notifications").await["notifications"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let items = eventually(&inbox, 2).await;
    eprintln!("hub inbox: {}", items[1]);
    assert_eq!(items[1]["resolved"], true);

    // 3. Cancel mid-turn.
    phone
        .send("chat", json!({ "prompt": "Count from 1 to 500, one number per line, no other text.", "agent_id": "assistant", "session_id": session_id }))
        .await;
    loop {
        let event = phone.next().await;
        eprintln!("turn 3: {event}");
        if event["type"] == "chat_stream" || event["type"] == "tool_start" {
            break;
        }
        assert_ne!(
            event["type"], "chat_complete",
            "nothing arrived to cancel on"
        );
    }
    phone
        .send("cancel", json!({ "session_id": session_id }))
        .await;
    let events = phone.until("chat_cancelled").await;
    for event in &events {
        eprintln!("turn 3: {event}");
    }
    let page = get(link, &format!("/api/v1/chats/{chat_id}/messages")).await;
    assert!(page["activeRun"].is_null());
    let users = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["role"] == "user")
        .count();
    eprintln!("transcript holds {users} user messages after 3 turns");
    assert_eq!(
        users, 3,
        "one user message per turn: nothing re-sent into the store"
    );
}

// -- A fake OpenClaw gateway --------------------------------------------------

/// One method call the fake gateway saw.
#[derive(Debug, Clone)]
struct Called {
    method: String,
    params: Value,
}

struct FakeGateway {
    url: String,
    seen: Arc<Mutex<Vec<Called>>>,
    runs: Arc<Mutex<u32>>,
}

impl FakeGateway {
    fn seen(&self, method: &str) -> Vec<Called> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.method == method)
            .cloned()
            .collect()
    }
}

const SESSION_KEY: &str = "agent:main:nebo-link-spike";

fn oc_event(event: &str, payload: Value) -> Value {
    json!({ "type": "event", "event": event, "payload": payload })
}

/// A `chat` event for `run` on `session`.
fn chat_event(run: &str, session: &str, state: Value) -> Value {
    let mut payload = json!({ "runId": run, "sessionKey": session, "agentId": "main", "seq": 1 });
    if let (Value::Object(payload), Value::Object(state)) = (&mut payload, state) {
        payload.extend(state);
    }
    oc_event("chat", payload)
}

fn agent_event(run: &str, session: &str, stream: &str, data: Value) -> Value {
    oc_event(
        "agent",
        json!({ "runId": run, "sessionKey": session, "agentId": "main", "stream": stream, "data": data, "seq": 1, "ts": 1790397333173u64 }),
    )
}

/// The gateway as the 2026.9.6 spike saw it: challenge, `hello-ok` for any
/// `connect`, then `agents.list`, `sessions.list`, `chat.history`,
/// `sessions.messages.subscribe`, `chat.send` (run 1 answers with a tool
/// call, its closing assistant row trailing the final event; run 2 asks for
/// approval; run 3 streams until aborted), `chat.abort` and
/// `approval.resolve`. `session.message` rows reach a connection only for
/// the sessions it subscribed to (`server-session-events.ts`).
async fn serve_gateway() -> FakeGateway {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let seen: Arc<Mutex<Vec<Called>>> = Arc::default();
    let runs = Arc::new(Mutex::new(0u32));
    let (log, counter) = (seen.clone(), runs.clone());
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (mut tx, mut rx) = ws.split();
            tx.send(Message::text(
                oc_event(
                    "connect.challenge",
                    json!({ "nonce": "n1", "ts": 1790397272202u64 }),
                )
                .to_string(),
            ))
            .await
            .unwrap();
            let mut subscribed = HashSet::new();
            let (out, mut out_rx) = mpsc::unbounded_channel::<Value>();
            let writer = tokio::spawn(async move {
                while let Some(frame) = out_rx.recv().await {
                    if tx.send(Message::text(frame.to_string())).await.is_err() {
                        break;
                    }
                }
            });
            while let Some(Ok(message)) = rx.next().await {
                let Message::Text(text) = message else {
                    continue;
                };
                let frame: Value = serde_json::from_str(text.as_str()).unwrap();
                let id = frame["id"].as_str().unwrap().to_owned();
                let method = frame["method"].as_str().unwrap().to_owned();
                let params = frame["params"].clone();
                log.lock().unwrap().push(Called {
                    method: method.clone(),
                    params: params.clone(),
                });
                let reply = |payload: Value| json!({ "type": "res", "id": id, "ok": true, "payload": payload });
                let mut events = Vec::new();
                let response = match method.as_str() {
                    "connect" => reply(json!({
                        "type": "hello-ok", "protocol": 4,
                        "server": { "version": "2026.9.6", "connId": "c1" },
                        "features": { "methods": ["chat.send"], "events": ["chat", "agent"] },
                        "auth": { "method": "trusted-proxy", "role": "operator", "scopes": ["operator.admin", "operator.read", "operator.write", "operator.approvals"] },
                        "policy": { "maxPayload": 26214400, "maxBufferedBytes": 52428800, "tickIntervalMs": 30000 },
                    })),
                    "agents.list" => reply(json!({
                        "defaultId": "main", "mainKey": "main", "scope": "per-sender",
                        "agents": [
                            { "id": "main", "identity": { "name": "Claw" }, "model": { "primary": "neboai/nebo-1" } },
                            { "id": "writer", "name": "Writer", "model": { "primary": "neboai/nebo-1" } }
                        ]
                    })),
                    "sessions.list" => reply(json!({
                        "sessions": [{ "key": SESSION_KEY, "kind": "direct", "derivedTitle": "Reply with exactly the word: pong", "lastMessagePreview": "pong", "agentId": "main", "updatedAt": 1790000010000.0, "model": "nebo-1" }],
                        "hasMore": false
                    })),
                    "chat.history" => reply(json!({
                        "sessionKey": params["sessionKey"], "sessionId": "s1",
                        "messages": [
                            { "role": "user", "content": "Use your exec tool to run `uname -a`.", "timestamp": 1790397284698.0, "__openclaw": { "id": "u1" } },
                            { "role": "assistant", "content": [{ "type": "toolCall", "id": "call_9ec1", "name": "exec", "arguments": { "command": "uname -a" } }], "timestamp": 1790397333000.0, "__openclaw": { "id": "a1" } },
                            { "role": "toolResult", "toolCallId": "call_9ec1", "toolName": "exec", "isError": false, "content": [{ "type": "text", "text": "Darwin" }], "timestamp": 1790397334000.0 },
                            { "role": "assistant", "content": [{ "type": "text", "text": "The output is Darwin." }], "usage": { "input": 654, "output": 81 }, "timestamp": 1790397337078.0, "__openclaw": { "id": "a2" } }
                        ],
                        "deltaCursor": "c", "hasMore": false, "sessionInfo": { "hasActiveRun": false }
                    })),
                    "sessions.messages.subscribe" => {
                        subscribed.insert(params["key"].as_str().unwrap().to_owned());
                        reply(json!({ "subscribed": true, "key": params["key"] }))
                    }
                    "chat.send" => {
                        let mut runs = counter.lock().unwrap();
                        *runs += 1;
                        let run = format!("run_{runs}");
                        let session = params["sessionKey"].as_str().unwrap().to_owned();
                        events.push(chat_event(
                            &run,
                            &session,
                            json!({ "state": "status", "phase": "preparing_workspace" }),
                        ));
                        match *runs {
                            1 => {
                                events.push(agent_event(&run, &session, "thinking", json!({ "text": "Need the kernel.", "delta": "Need the kernel." })));
                                events.push(agent_event(&run, &session, "tool", json!({ "phase": "start", "name": "exec", "toolCallId": "call_9ec1", "args": { "command": "uname -a" } })));
                                events.push(oc_event("session.message", json!({ "sessionKey": session, "agentId": "main", "runId": run, "message": { "role": "assistant", "content": [{ "type": "toolCall", "id": "call_9ec1", "name": "exec", "arguments": { "command": "uname -a" } }], "usage": { "input": 600, "output": 20, "totalTokens": 16952 }, "stopReason": "toolUse", "__openclaw": { "runId": run } } })));
                                events.push(agent_event(&run, &session, "tool", json!({ "phase": "result", "name": "exec", "toolCallId": "call_9ec1", "isError": false, "result": { "content": [{ "type": "text", "text": "Darwin" }], "details": { "durationMs": 1204 } } })));
                                events.push(chat_event(
                                    &run,
                                    &session,
                                    json!({ "state": "delta", "deltaText": "The output is " }),
                                ));
                                events.push(chat_event(
                                    &run,
                                    &session,
                                    json!({ "state": "delta", "deltaText": "Darwin." }),
                                ));
                                events.push(chat_event(
                                    &run,
                                    &session,
                                    json!({ "state": "final", "stopReason": "stop" }),
                                ));
                                events.push(oc_event("session.message", json!({ "sessionKey": session, "agentId": "main", "runId": run, "message": { "role": "assistant", "content": [{ "type": "text", "text": "The output is Darwin." }], "usage": { "input": 654, "output": 81, "totalTokens": 17667 }, "stopReason": "stop", "__openclaw": { "runId": run } } })));
                            }
                            2 => {
                                events.push(oc_event("exec.approval.requested", json!({
                                    "approvalKind": "exec", "id": "1759b5f3-9141-4537-8c78-9afc7fa627c2",
                                    "request": { "command": "uname -a", "allowedDecisions": ["allow-once", "allow-always", "deny"], "agentId": "main", "sessionKey": session },
                                    "createdAtMs": 1790397523868u64, "expiresAtMs": 1790397568868u64
                                })));
                            }
                            _ => events.push(chat_event(
                                &run,
                                &session,
                                json!({ "state": "delta", "deltaText": "1\n2\n" }),
                            )),
                        }
                        reply(json!({ "runId": run, "status": "started", "messageSeq": 3 }))
                    }
                    "approval.resolve" => {
                        events.push(oc_event("exec.approval.resolved", json!({ "id": params["id"], "decision": params["decision"], "resolvedBy": "Nebo Link", "request": { "command": "uname -a", "sessionKey": "agent:main:nebo-approval" } })));
                        events.push(chat_event(
                            "run_2",
                            "agent:main:nebo-approval",
                            json!({ "state": "delta", "deltaText": "Done." }),
                        ));
                        events.push(chat_event(
                            "run_2",
                            "agent:main:nebo-approval",
                            json!({ "state": "final", "stopReason": "stop" }),
                        ));
                        reply(
                            json!({ "applied": true, "approval": { "id": params["id"], "status": "allowed", "decision": params["decision"], "reason": "user" } }),
                        )
                    }
                    "chat.abort" => {
                        events.push(chat_event(
                            "run_3",
                            params["sessionKey"].as_str().unwrap(),
                            json!({ "state": "aborted", "stopReason": "aborted" }),
                        ));
                        reply(json!({ "ok": true }))
                    }
                    other => {
                        json!({ "type": "res", "id": id, "ok": false, "error": { "code": "NOT_FOUND", "message": format!("no method {other}") } })
                    }
                };
                out.send(response).unwrap();
                for event in events {
                    if event["event"] == "session.message"
                        && !subscribed.contains(event["payload"]["sessionKey"].as_str().unwrap())
                    {
                        continue;
                    }
                    out.send(event).unwrap();
                }
            }
            drop(out);
            let _ = writer.await;
        }
    });
    FakeGateway { url, seen, runs }
}

async fn start_openclaw_link(
    gateway: &FakeGateway,
    hub: &str,
    dir: &std::path::Path,
) -> SocketAddr {
    let access = ProxyAccess {
        base_path: format!("/t/{BOT}"),
        origin: "https://neboai.com".into(),
        user_header: "x-nebo-user".into(),
        identity: "owner-1".into(),
        password: "pw".into(),
    };
    let backend = Openclaw::new(
        Connect::new(&gateway.url, &access, FORWARDED_FOR),
        FileDeviceStore::new(dir.join("openclaw-device.json")),
    );
    serve_contract(("openclaw", "OpenClaw"), Arc::new(backend), hub).await
}

#[tokio::test]
async fn the_phone_flow_against_an_openclaw_gateway() {
    let gateway = serve_gateway().await;
    let (hub_url, inbox) = serve_hub().await;
    let dir = tempfile::tempdir().unwrap();
    let link = start_openclaw_link(&gateway, &hub_url, dir.path()).await;

    let health = get(link, "/health").await;
    assert_eq!(health["runtime"], "openclaw");
    assert_eq!(health["chat"], true);
    assert!(
        dir.path().join("openclaw-device.json").exists(),
        "the device key is kept"
    );
    assert_eq!(
        gateway.seen("connect").len(),
        1,
        "one socket serves everything"
    );

    let roster = get(link, "/api/v1/agents").await;
    let agents = roster["agents"].as_array().unwrap();
    assert_eq!(agents.len(), 2);
    assert_eq!(agents[0]["id"], "assistant");
    assert_eq!(agents[0]["name"], "Claw");
    assert_eq!(agents[0]["description"], "");
    assert_eq!(agents[1]["id"], "writer");
    assert_eq!(agents[1]["name"], "Writer");

    let chats = get(link, "/api/v1/agents/assistant/chats").await;
    assert_eq!(chats["chats"][0]["id"], SESSION_KEY);
    assert_eq!(
        chats["chats"][0]["title"],
        "Reply with exactly the word: pong"
    );
    assert_eq!(chats["chats"][0]["preview"], "pong");
    assert_eq!(gateway.seen("sessions.list")[0].params["agentId"], "main");
    let (_, created) = call(link, "POST", "/api/v1/agents/assistant/chats", "{}").await;
    let new_key = created["chat"]["id"].as_str().unwrap().to_owned();
    assert!(new_key.starts_with("agent:main:nebo-"), "{new_key}");
    assert_eq!(get(link, "/api/v1/chats/x").await["model"], "neboai/nebo-1");

    // A turn on the spike's session: tool events, deltas, usage summed from
    // the run's assistant rows (the closing one after the final event),
    // completion.
    let mut phone = Phone::connect(link).await;
    let session_id = format!("agent:assistant:thread:{SESSION_KEY}");
    phone
        .send("chat", json!({ "prompt": "Use your exec tool to run `uname -a`.", "agent_id": "assistant", "session_id": session_id }))
        .await;
    let events = phone.until("chat_complete").await;
    assert_eq!(
        kinds(&events),
        [
            "thinking",
            "tool_start",
            "tool_result",
            "chat_stream",
            "chat_stream",
            "usage",
            "chat_complete"
        ]
    );
    for event in &events {
        assert_eq!(event["data"]["session_id"], session_id, "{event}");
    }
    assert_eq!(events[0]["data"]["text"], "Need the kernel.");
    assert_eq!(events[1]["data"]["tool_id"], "call_9ec1");
    assert_eq!(events[1]["data"]["input"]["command"], "uname -a");
    assert_eq!(events[2]["data"]["result"], "Darwin");
    assert_eq!(events[2]["data"]["duration_ms"], 1204);
    assert_eq!(events[5]["data"]["input_tokens"], 1254);
    assert_eq!(events[5]["data"]["output_tokens"], 101);
    assert_eq!(
        gateway.seen("sessions.messages.subscribe")[0].params,
        json!({ "key": SESSION_KEY, "agentId": "main" })
    );
    let send = &gateway.seen("chat.send")[0].params;
    assert_eq!(send["sessionKey"], SESSION_KEY);
    assert_eq!(send["agentId"], "main");
    assert_eq!(send["message"], "Use your exec tool to run `uname -a`.");
    assert!(send["idempotencyKey"].as_str().unwrap().len() >= 16);
    assert!(send.get("queueMode").is_none());

    // The transcript from `chat.history`, in the phone's row shape.
    let page = get(
        link,
        &format!("/api/v1/chats/{}/messages", SESSION_KEY.replace(':', "%3A")),
    )
    .await;
    let rows = page["messages"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0]["role"], "user");
    assert_eq!(rows[0]["createdAt"], 1790397284);
    let calls: Value = serde_json::from_str(rows[1]["toolCalls"].as_str().unwrap()).unwrap();
    assert_eq!(calls[0]["id"], "call_9ec1");
    let results: Value = serde_json::from_str(rows[2]["toolResults"].as_str().unwrap()).unwrap();
    assert_eq!(results[0]["tool_call_id"], "call_9ec1");
    assert_eq!(results[0]["content"], "Darwin");
    assert_eq!(rows[3]["content"], "The output is Darwin.");
    assert_eq!(gateway.seen("chat.history")[0].params["agentId"], "main");

    // An approval on a fresh chat: the gateway's decisions on the card, the
    // answer as `approval.resolve`, resolved everywhere.
    let approval_session = "agent:assistant:thread:agent:main:nebo-approval";
    phone
        .send("chat", json!({ "prompt": "run uname", "agent_id": "assistant", "session_id": approval_session }))
        .await;
    let ask = phone.until("ask_request").await.pop().unwrap();
    assert_eq!(
        ask["data"]["request_id"],
        "1759b5f3-9141-4537-8c78-9afc7fa627c2"
    );
    assert_eq!(
        ask["data"]["widgets"][0]["options"],
        json!(["Allow once", "Always allow", "Deny"])
    );
    assert!(ask["data"]["prompt"].as_str().unwrap().contains("uname -a"));
    let items = eventually(&inbox, 1).await;
    assert_eq!(
        items[0]["id"],
        "approval:1759b5f3-9141-4537-8c78-9afc7fa627c2"
    );
    assert_eq!(items[0]["title"], "Claw asks to run uname -a");
    assert_eq!(items[0]["chatId"], "agent:main:nebo-approval");
    phone
        .send(
            "ask_response",
            json!({ "request_id": "1759b5f3-9141-4537-8c78-9afc7fa627c2", "value": "Allow once" }),
        )
        .await;
    let events = phone.until("chat_complete").await;
    assert_eq!(kinds(&events), ["chat_stream", "chat_complete"]);
    let resolve = &gateway.seen("approval.resolve")[0].params;
    assert_eq!(
        resolve,
        &json!({ "id": "1759b5f3-9141-4537-8c78-9afc7fa627c2", "kind": "exec", "decision": "allow-once" })
    );
    assert!(
        get(link, "/api/v1/notifications").await["notifications"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(eventually(&inbox, 2).await[1]["resolved"], true);

    // Cancel: `chat.abort` names the session, agent and run.
    phone
        .send(
            "chat",
            json!({ "prompt": "Count to 500.", "agent_id": "assistant", "session_id": session_id }),
        )
        .await;
    phone.until("chat_stream").await;
    phone
        .send("cancel", json!({ "session_id": session_id }))
        .await;
    phone.until("chat_cancelled").await;
    assert_eq!(
        gateway.seen("chat.abort")[0].params,
        json!({ "sessionKey": SESSION_KEY, "agentId": "main", "runId": "run_3" })
    );
    assert!(
        get(
            link,
            &format!("/api/v1/chats/{}/messages", SESSION_KEY.replace(':', "%3A"))
        )
        .await["activeRun"]
            .is_null()
    );
    assert_eq!(*gateway.runs.lock().unwrap(), 3);
}

/// Against a live OpenClaw gateway (`NEBO_LINK_LIVE_OPENCLAW_URL`, e.g.
/// `ws://127.0.0.1:28790`, and `NEBO_LINK_LIVE_OPENCLAW_IDENTITY`, the
/// trusted-proxy identity `ProxyAccess` granted admin), with the fake hub
/// catching the inbox items: three short turns on one new chat. Costs real
/// model calls. The evidence for PRD §9.2 and §9.3 on OpenClaw.
#[tokio::test]
#[ignore = "needs NEBO_LINK_LIVE_OPENCLAW_URL and NEBO_LINK_LIVE_OPENCLAW_IDENTITY"]
async fn live_openclaw_phone_flow() {
    let (Ok(url), Ok(identity)) = (
        std::env::var("NEBO_LINK_LIVE_OPENCLAW_URL"),
        std::env::var("NEBO_LINK_LIVE_OPENCLAW_IDENTITY"),
    ) else {
        eprintln!(
            "NEBO_LINK_LIVE_OPENCLAW_URL / NEBO_LINK_LIVE_OPENCLAW_IDENTITY not set; nothing to do"
        );
        return;
    };
    let (hub_url, inbox) = serve_hub().await;
    let dir = tempfile::tempdir().unwrap();
    let access = ProxyAccess {
        base_path: format!("/t/{identity}"),
        origin: "https://neboai.com".into(),
        user_header: "x-nebo-user".into(),
        identity,
        password: String::new(),
    };
    let backend = Openclaw::new(
        Connect::new(&url, &access, FORWARDED_FOR),
        FileDeviceStore::new(dir.path().join("openclaw-device.json")),
    );
    let link = serve_contract(("openclaw", "OpenClaw"), Arc::new(backend), &hub_url).await;

    let health = get(link, "/health").await;
    eprintln!("health: {health}");
    assert_eq!(health["chat"], true);
    let roster = get(link, "/api/v1/agents").await;
    eprintln!("agents: {roster}");
    assert_eq!(roster["agents"][0]["id"], "assistant");

    // 1. A turn on a chat the link creates.
    let mut phone = Phone::connect(link).await;
    phone
        .send(
            "chat",
            json!({ "prompt": "Reply with exactly the word: pong", "agent_id": "assistant" }),
        )
        .await;
    let created = phone.next().await;
    eprintln!("turn 1: {created}");
    assert_eq!(created["type"], "chat_created");
    let session_id = created["data"]["session_id"].as_str().unwrap().to_owned();
    let chat_id = session_id
        .strip_prefix("agent:assistant:thread:")
        .unwrap()
        .to_owned();
    let events = phone.until("chat_complete").await;
    let mut text = String::new();
    for event in &events {
        eprintln!("turn 1: {event}");
        if event["type"] == "chat_stream" {
            text.push_str(event["data"]["content"].as_str().unwrap());
        }
    }
    assert!(text.to_lowercase().contains("pong"), "streamed {text:?}");
    let encoded = chat_id.replace(':', "%3A");
    let page = get(link, &format!("/api/v1/chats/{encoded}/messages")).await;
    eprintln!("transcript after turn 1: {}", page["messages"]);
    let rows = page["messages"].as_array().unwrap();
    assert_eq!(rows[0]["role"], "user");
    assert!(rows.iter().any(|r| {
        r["role"] == "assistant"
            && r["content"]
                .as_str()
                .unwrap_or("")
                .to_lowercase()
                .contains("pong")
    }));
    let chats = get(link, "/api/v1/agents/assistant/chats").await;
    assert!(
        chats["chats"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["id"] == chat_id),
        "{chats}"
    );

    // 2. A tool turn: the exec runs (this gateway has no exec-approval
    // policy, so the agent's own exec never asks) and its events stream.
    phone
        .send("chat", json!({ "prompt": "Use your exec tool to run the shell command `uname -a` and tell me the output in one line.", "agent_id": "assistant", "session_id": session_id }))
        .await;
    let events = phone.until("chat_complete").await;
    for event in &events {
        eprintln!("turn 2: {event}");
    }
    let kinds = kinds(&events);
    assert!(
        kinds.contains(&"tool_start") && kinds.contains(&"tool_result"),
        "{kinds:?}"
    );
    let start = events.iter().find(|e| e["type"] == "tool_start").unwrap();
    let result = events.iter().find(|e| e["type"] == "tool_result").unwrap();
    assert_eq!(start["data"]["tool"], "exec");
    assert_eq!(result["data"]["tool_id"], start["data"]["tool_id"]);
    assert!(
        result["data"]["result"]
            .as_str()
            .unwrap()
            .contains("Darwin")
    );

    // 3. An approval on the running turn, raised the way the spike did
    // (`exec.approval.request` from another operator socket, which the
    // gateway broadcasts as `exec.approval.requested` for the session):
    // the card, the notification, the inbox item, the answer, then cancel.
    phone
        .send("chat", json!({ "prompt": "Count from 1 to 500, one number per line, no other text.", "agent_id": "assistant", "session_id": session_id }))
        .await;
    loop {
        let event = phone.next().await;
        eprintln!("turn 3: {event}");
        if event["type"] == "chat_stream" || event["type"] == "tool_start" {
            break;
        }
        assert_ne!(event["type"], "chat_complete", "nothing arrived to ask on");
    }
    let (requester, _events) = nebo_runtimes::openclaw::gateway::Gateway::connect(
        &Connect::new(&url, &access, FORWARDED_FOR),
        &FileDeviceStore::new(dir.path().join("requester-device.json")),
    )
    .await
    .expect("a second operator socket");
    let raised = {
        let chat_id = chat_id.clone();
        tokio::spawn(async move {
            requester
                .request(
                    "exec.approval.request",
                    json!({ "command": "uname -a", "agentId": "main", "sessionKey": chat_id, "timeoutMs": 45000 }),
                )
                .await
        })
    };
    let mut ask = None;
    while ask.is_none() {
        let event = phone.next().await;
        eprintln!(
            "turn 3: {}",
            event.to_string().chars().take(300).collect::<String>()
        );
        assert_ne!(
            event["type"], "chat_complete",
            "the turn ended before the ask"
        );
        if event["type"] == "ask_request" {
            ask = Some(event);
        }
    }
    let ask = ask.unwrap();
    let request_id = ask["data"]["request_id"].as_str().unwrap().to_owned();
    assert_eq!(
        ask["data"]["widgets"][0]["options"],
        json!(["Allow once", "Always allow", "Deny"])
    );
    let notices = get(link, "/api/v1/notifications").await;
    eprintln!("notifications: {notices}");
    assert_eq!(
        notices["notifications"][0]["id"],
        format!("approval:{request_id}")
    );
    let items = eventually(&inbox, 1).await;
    eprintln!("hub inbox: {}", items[0]);
    assert_eq!(items[0]["chatId"], chat_id);
    phone
        .send(
            "ask_response",
            json!({ "request_id": request_id, "value": "Deny" }),
        )
        .await;
    let decided = tokio::time::timeout(Duration::from_secs(20), raised)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    eprintln!("exec.approval.request answered: {decided}");
    assert_eq!(decided["decision"], "deny");
    assert!(
        get(link, "/api/v1/notifications").await["notifications"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let items = eventually(&inbox, 2).await;
    eprintln!("hub inbox: {}", items[1]);
    assert_eq!(items[1]["resolved"], true);

    phone
        .send("cancel", json!({ "session_id": session_id }))
        .await;
    let events = phone.until("chat_cancelled").await;
    eprintln!("turn 3: cancelled after {} more events", events.len());
    let page = get(link, &format!("/api/v1/chats/{encoded}/messages")).await;
    assert!(page["activeRun"].is_null());
    let users = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["role"] == "user")
        .count();
    eprintln!("transcript holds {users} user messages after 3 turns");
    assert_eq!(users, 3, "one user message per turn: nothing re-sent");
}

// -- A fake ACP agent -----------------------------------------------------------
//
// This test binary, started again as `fake_acp_agent_process` with
// `NEBO_LINK_FAKE_ACP` set, is the agent: a scripted JSON-RPC peer on
// stdin/stdout in the shapes the Claude Code and Codex adapters sent on
// 2026-09-26 (`@agentclientprotocol/claude-agent-acp` 0.81.2,
// `@agentclientprotocol/codex-acp` 1.13.1). What a prompt does depends on
// its text: `hello` streams two chunks; `tool` runs a command after asking
// permission; `edit` changes a file after asking; `wait` runs until
// cancelled; `exit` ends the process mid-turn. It offers Claude Code's
// permission modes: in `bypassPermissions` nothing asks, in `acceptEdits` an
// edit doesn't. Sessions and their messages are kept in
// `NEBO_LINK_FAKE_ACP_STATE` (their modes beside it, `.modes`), so a
// restarted agent replays them on `session/load`.

/// Not a test when run by the harness: the fake agent's entry point.
#[test]
fn fake_acp_agent_process() {
    use std::io::{BufRead, Write};
    let Ok(mode) = std::env::var("NEBO_LINK_FAKE_ACP") else {
        return;
    };
    let state_file = std::env::var("NEBO_LINK_FAKE_ACP_STATE").unwrap();
    let load = || -> Value {
        std::fs::read_to_string(&state_file)
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_else(|| json!({}))
    };
    let save = |state: &Value| std::fs::write(&state_file, state.to_string()).unwrap();
    let modes_file = format!("{state_file}.modes");
    let mode_of = |session: &str| -> String {
        std::fs::read_to_string(&modes_file)
            .ok()
            .and_then(|t| serde_json::from_str::<Value>(&t).ok())
            .and_then(|m| m[session].as_str().map(str::to_owned))
            .unwrap_or_else(|| "default".to_owned())
    };
    let set_mode = |session: &str, mode: &str| {
        let mut modes: Value = std::fs::read_to_string(&modes_file)
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_else(|| json!({}));
        modes[session] = json!(mode);
        std::fs::write(&modes_file, modes.to_string()).unwrap();
    };
    let offered = |session: &str| json!({ "currentModeId": mode_of(session), "availableModes": [
        { "id": "default", "name": "Manual", "_meta": { "kind": "standard" } },
        { "id": "acceptEdits", "name": "Accept edits", "_meta": { "kind": "standard" } },
        { "id": "plan", "name": "Plan", "_meta": { "kind": "plan" } },
        { "id": "auto", "name": "Auto", "_meta": { "kind": "auto_review" } },
        { "id": "bypassPermissions", "name": "Bypass permissions", "_meta": { "kind": "full_access" } } ] });
    let stdout = std::io::stdout();
    let send = |frame: Value| {
        let mut out = stdout.lock();
        writeln!(out, "{frame}").unwrap();
        out.flush().unwrap();
    };
    let update = |session: &str, update: Value| {
        send(json!({ "jsonrpc": "2.0", "method": "session/update", "params": { "sessionId": session, "update": update } }))
    };
    let text = |session: &str, kind: &str, text: &str| {
        update(session, json!({ "sessionUpdate": kind, "content": { "type": "text", "text": text }, "messageId": "m" }))
    };
    // The harness has printed "test fake_acp_agent_process ... " with no
    // newline: end that line, so every frame is a line of its own.
    send(json!(null));
    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines();
    while let Some(Ok(line)) = lines.next() {
        let Ok(message) = serde_json::from_str::<Value>(&line) else { continue };
        let id = message["id"].clone();
        let params = &message["params"];
        let reply = |result: Value| send(json!({ "jsonrpc": "2.0", "id": id, "result": result }));
        match message["method"].as_str().unwrap_or("") {
            "initialize" => reply(json!({
                "protocolVersion": 1,
                "agentCapabilities": { "loadSession": true, "sessionCapabilities": { "list": {}, "resume": {} } },
                "agentInfo": { "name": "fake-acp", "title": "Fake Agent", "version": "0" },
                "authMethods": []
            })),
            "session/new" if mode == "signed-out" => send(json!({
                "jsonrpc": "2.0", "id": id, "error": { "code": -32000, "message": "Authentication required" }
            })),
            "session/new" => {
                let mut state = load();
                let session = format!("s{}", state.as_object().unwrap().len() + 1);
                state[&session] = json!([]);
                save(&state);
                reply(json!({ "sessionId": session, "modes": offered(&session), "configOptions": [{ "id": "model", "category": "model",
                    "currentValue": "m1", "options": [{ "value": "m1", "name": "Model One" }] }] }));
            }
            "session/list" => {
                let state = load();
                let sessions: Vec<Value> = state
                    .as_object()
                    .unwrap()
                    .keys()
                    .map(|s| json!({ "sessionId": s, "cwd": params["cwd"], "title": format!("Chat {s}"), "updatedAt": "2026-09-26T14:23:13.025Z" }))
                    .collect();
                reply(json!({ "sessions": sessions }));
            }
            "session/load" => {
                let session = params["sessionId"].as_str().unwrap().to_owned();
                let state = load();
                let Some(turns) = state[&session].as_array() else {
                    send(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32002, "message": "Session not found" } }));
                    continue;
                };
                for turn in turns {
                    text(&session, "user_message_chunk", turn["user"].as_str().unwrap());
                    text(&session, "agent_message_chunk", turn["agent"].as_str().unwrap());
                }
                reply(json!({ "sessionId": session, "modes": offered(&session) }));
            }
            "session/set_mode" if mode == "refuses-modes" => send(json!({
                "jsonrpc": "2.0", "id": id, "error": { "code": -32603, "message": "Invalid Mode" }
            })),
            "session/set_mode" => {
                set_mode(params["sessionId"].as_str().unwrap(), params["modeId"].as_str().unwrap());
                reply(json!({}));
            }
            "session/prompt" => {
                let session = params["sessionId"].as_str().unwrap().to_owned();
                let prompt = params["prompt"][0]["text"].as_str().unwrap_or("").to_owned();
                let mut said = String::new();
                let mut stop = "end_turn";
                match prompt.as_str() {
                    "exit" => std::process::exit(3),
                    "wait" => {
                        text(&session, "agent_thought_chunk", "Waiting");
                        // Until the client cancels.
                        while let Some(Ok(line)) = lines.next() {
                            if line.contains("session/cancel") {
                                break;
                            }
                        }
                        stop = "cancelled";
                    }
                    "tool" | "edit" if mode_of(&session) == "bypassPermissions"
                        || (prompt == "edit" && mode_of(&session) == "acceptEdits") =>
                    {
                        // Allowed by the session's mode: it runs without asking.
                        update(&session, json!({ "sessionUpdate": "tool_call", "toolCallId": "call_1", "name": "Bash",
                            "rawInput": { "command": "ls" }, "status": "in_progress", "title": "ls", "kind": "execute", "content": [] }));
                        update(&session, json!({ "sessionUpdate": "tool_call_update", "toolCallId": "call_1", "status": "completed",
                            "rawOutput": "file.txt", "content": [{ "type": "content", "content": { "type": "text", "text": "file.txt" } }] }));
                        said = "Done.".into();
                    }
                    "tool" | "edit" => {
                        let (kind, title) = if prompt == "edit" { ("edit", "Edit notes.md") } else { ("execute", "ls") };
                        update(&session, json!({ "sessionUpdate": "tool_call", "toolCallId": "call_1", "name": "Bash",
                            "rawInput": {}, "status": "pending", "title": "Terminal", "kind": kind, "content": [] }));
                        update(&session, json!({ "sessionUpdate": "tool_call_update", "toolCallId": "call_1",
                            "rawInput": { "command": "ls" }, "title": title, "kind": kind }));
                        send(json!({ "jsonrpc": "2.0", "id": 0, "method": "session/request_permission", "params": {
                            "sessionId": session,
                            "toolCall": { "toolCallId": "call_1", "status": "pending", "rawInput": { "command": "ls" }, "title": title, "kind": kind,
                                "content": [{ "type": "content", "content": { "type": "text", "text": "List the files" } }] },
                            "options": [{ "optionId": "allow-once", "name": "Yes", "kind": "allow_once" },
                                        { "optionId": "allow-always", "name": "Yes, and don't ask again", "kind": "allow_always" },
                                        { "optionId": "reject", "name": "No", "kind": "reject_once" }] } }));
                        let mut outcome = Value::Null;
                        while let Some(Ok(line)) = lines.next() {
                            let answer: Value = serde_json::from_str(&line).unwrap();
                            if answer["id"] == 0 && answer.get("method").is_none() {
                                outcome = answer["result"]["outcome"].clone();
                                break;
                            }
                        }
                        if outcome["outcome"] == "cancelled" {
                            stop = "cancelled";
                        } else if outcome["optionId"] == "allow-once" {
                            update(&session, json!({ "sessionUpdate": "tool_call_update", "toolCallId": "call_1", "status": "completed",
                                "rawOutput": "file.txt", "content": [{ "type": "content", "content": { "type": "text", "text": "file.txt" } }] }));
                            said = "Done.".into();
                        } else {
                            update(&session, json!({ "sessionUpdate": "tool_call_update", "toolCallId": "call_1", "status": "failed" }));
                            said = "Not run.".into();
                        }
                    }
                    _ => {
                        update(&session, json!({ "sessionUpdate": "plan", "entries": [
                            { "content": "Say hello", "priority": "high", "status": "in_progress" }] }));
                        said = "Hello".into();
                        text(&session, "agent_message_chunk", "Hel");
                        text(&session, "agent_message_chunk", "lo");
                    }
                }
                if !said.is_empty() && (prompt == "tool" || prompt == "edit") {
                    text(&session, "agent_message_chunk", &said);
                }
                update(&session, json!({ "sessionUpdate": "session_info_update", "title": "A fake chat" }));
                update(&session, json!({ "sessionUpdate": "usage_update", "used": 10, "size": 100 }));
                if stop == "end_turn" {
                    let mut state = load();
                    state[&session].as_array_mut().unwrap().push(json!({ "user": prompt, "agent": said }));
                    save(&state);
                }
                reply(json!({ "stopReason": stop,
                    "usage": { "inputTokens": 3, "outputTokens": 2, "cachedReadTokens": 5, "totalTokens": 10 } }));
            }
            _ if message.get("method").is_some() && !id.is_null() => send(json!({
                "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": "Method not found" }
            })),
            _ => {}
        }
    }
}

/// The contract in front of the fake ACP agent, as `agent` named `name`.
async fn start_acp_link(
    mode: &str,
    agent: nebo_runtimes::acp::Agent,
    name: &str,
    dir: &std::path::Path,
    hub: &str,
) -> SocketAddr {
    use nebo_link::contract::acp::{Acp, Settings};
    let command = nebo_runtimes::RuntimeCommand {
        program: std::env::current_exe().unwrap().to_string_lossy().into_owned(),
        args: ["fake_acp_agent_process", "--exact", "--nocapture", "--test-threads=1"]
            .map(String::from)
            .to_vec(),
        env: vec![
            ("NEBO_LINK_FAKE_ACP".into(), mode.into()),
            ("NEBO_LINK_FAKE_ACP_STATE".into(), dir.join("agent-state.json").to_string_lossy().into_owned()),
        ],
    };
    let backend = Acp::new(Settings {
        agent,
        name: name.into(),
        command,
        workdir: dir.join("work"),
        log: dir.join("logs").join("agent.log"),
        chats_file: dir.join("acp-chats.json"),
    });
    let runtime = nebo_link::install::runtime_name(nebo_runtimes::Runtime::Acp(agent));
    let key = nebo_link::install::runtime_key(nebo_runtimes::Runtime::Acp(agent));
    serve_contract((key, runtime), Arc::new(backend), hub).await
}

fn streamed(events: &[Value]) -> String {
    events
        .iter()
        .filter(|e| e["type"] == "chat_stream")
        .map(|e| e["data"]["content"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn the_phone_flow_against_an_acp_agent() {
    let tmp = tempfile::tempdir().unwrap();
    let (hub_url, inbox) = serve_hub().await;
    let link = start_acp_link("normal", nebo_runtimes::acp::Agent::Other, "Fake Agent", tmp.path(), &hub_url).await;

    // The probe starts the agent; its one agent is the primary employee.
    let health = get(link, "/health").await;
    assert_eq!(health["runtime"], "acp");
    assert_eq!(health["chat"], true, "{health}");
    let roster = get(link, "/api/v1/agents").await;
    let agents = roster["agents"].as_array().unwrap();
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0]["id"], "assistant");
    assert_eq!(agents[0]["name"], "Fake Agent");
    assert!(agents[0]["description"].as_str().unwrap().starts_with("Works in "));

    // 1. Prompt -> streamed reply, thinking from the plan, usage.
    let mut phone = Phone::connect(link).await;
    phone.send("chat", json!({ "prompt": "hello", "agent_id": "assistant" })).await;
    let created = phone.next().await;
    assert_eq!(created["type"], "chat_created", "{created}");
    let session_id = created["data"]["session_id"].as_str().unwrap().to_owned();
    let chat_id = session_id.rsplit(":thread:").next().unwrap().to_owned();
    let events = phone.until("chat_complete").await;
    assert_eq!(kinds(&events), ["thinking", "chat_stream", "chat_stream", "usage", "chat_complete"]);
    assert_eq!(events[0]["data"]["text"], "Plan:\n[~] Say hello");
    assert_eq!(streamed(&events), "Hello");
    assert_eq!(events[3]["data"]["input_tokens"], 8, "fresh + cached input");
    assert_eq!(events[3]["data"]["output_tokens"], 2);
    let model = get(link, &format!("/api/v1/chats/{chat_id}")).await;
    assert_eq!(model["model"], "Model One", "{model}");
    let chats = get(link, "/api/v1/agents/assistant/chats").await;
    assert_eq!(chats["chats"][0]["id"], chat_id.as_str());
    assert_eq!(chats["chats"][0]["title"], format!("Chat {chat_id}"));

    // 2. A tool call that needs permission: the card, the ask, the inbox
    //    item; the owner's answer goes back and the tool runs.
    phone
        .send("chat", json!({ "prompt": "tool", "agent_id": "assistant", "session_id": session_id }))
        .await;
    let events = phone.until("ask_request").await;
    assert_eq!(kinds(&events), ["tool_start", "ask_request"]);
    assert_eq!(events[0]["data"]["tool"], "ls");
    assert_eq!(events[0]["data"]["input"], json!({ "command": "ls" }));
    let ask = &events[1]["data"];
    assert_eq!(ask["request_id"], "call_1");
    assert_eq!(ask["prompt"], "ls\nList the files");
    assert_eq!(ask["widgets"][0]["options"], json!(["Allow once", "Always allow", "Deny"]));
    let items = eventually(&inbox, 1).await;
    assert_eq!(items[0]["id"], "approval:call_1");
    assert_eq!(items[0]["title"], "Fake Agent asks to run `ls`");
    phone.send("ask_response", json!({ "request_id": "call_1", "value": "Allow once" })).await;
    let events = phone.until("chat_complete").await;
    assert_eq!(kinds(&events), ["tool_result", "chat_stream", "usage", "chat_complete"]);
    assert_eq!(events[0]["data"]["tool_id"], "call_1");
    assert_eq!(events[0]["data"]["result"], "file.txt");
    assert_eq!(events[0]["data"]["is_error"], false);
    assert_eq!(streamed(&events), "Done.");
    let items = eventually(&inbox, 2).await;
    assert_eq!(items[1], json!({ "id": "approval:call_1", "resolved": true }));

    // The transcript as the phone reads it: the tool on its turn.
    let page = get(link, &format!("/api/v1/chats/{chat_id}/messages")).await;
    let rows = page["messages"].as_array().unwrap();
    let roles: Vec<&str> = rows.iter().map(|r| r["role"].as_str().unwrap()).collect();
    assert_eq!(roles, ["user", "assistant", "user", "assistant", "tool", "assistant"], "{rows:?}");
    assert_eq!(rows[0]["content"], "hello");
    assert_eq!(rows[1]["content"], "Hello");
    assert_eq!(rows[5]["content"], "Done.");

    // 3. Cancel: `session/cancel`, and the turn ends cancelled.
    phone
        .send("chat", json!({ "prompt": "wait", "agent_id": "assistant", "session_id": session_id }))
        .await;
    let events = phone.until("thinking").await;
    assert_eq!(kinds(&events), ["thinking"]);
    phone.send("cancel", json!({ "session_id": session_id })).await;
    let events = phone.until("chat_cancelled").await;
    assert_eq!(kinds(&events), ["chat_cancelled"]);

    // 4. A denied tool: the result is an error, the agent carries on.
    phone
        .send("chat", json!({ "prompt": "tool", "agent_id": "assistant", "session_id": session_id }))
        .await;
    phone.until("ask_request").await;
    phone.send("ask_response", json!({ "request_id": "call_1", "value": "Deny" })).await;
    let events = phone.until("chat_complete").await;
    assert_eq!(events[0]["type"], "tool_result");
    assert_eq!(events[0]["data"]["is_error"], true);
    assert_eq!(streamed(&events), "Not run.");
}

/// The mode the fake agent's session is in now.
fn fake_mode(dir: &std::path::Path, session: &str) -> String {
    std::fs::read_to_string(dir.join("agent-state.json.modes"))
        .ok()
        .and_then(|t| serde_json::from_str::<Value>(&t).ok())
        .and_then(|m| m[session].as_str().map(str::to_owned))
        .unwrap_or_else(|| "default".to_owned())
}

/// Nebo's permission mode for the employee rides on each `chat` frame and
/// the session runs in the agent's matching mode: Full access runs a
/// command with no card, Automatic accepts an edit but asks for a command,
/// Ask asks for the edit, Plan is the agent's plan mode, and a frame
/// without one leaves the session as it is.
#[tokio::test]
async fn an_acp_session_runs_in_the_employees_permission_mode() {
    let tmp = tempfile::tempdir().unwrap();
    let (hub_url, _inbox) = serve_hub().await;
    let link = start_acp_link("normal", nebo_runtimes::acp::Agent::ClaudeCode, "Claude Code", tmp.path(), &hub_url).await;
    let mut phone = Phone::connect(link).await;
    phone.send("chat", json!({ "prompt": "hello", "agent_id": "assistant" })).await;
    let session_id = phone.next().await["data"]["session_id"].as_str().unwrap().to_owned();
    let chat_id = session_id.rsplit(":thread:").next().unwrap().to_owned();
    phone.until("chat_complete").await;
    assert_eq!(fake_mode(tmp.path(), &chat_id), "default", "no permission sent: the agent's own");

    let frame = |prompt: &'static str, permission: &'static str| {
        json!({ "prompt": prompt, "agent_id": "assistant", "session_id": session_id, "permission_mode": permission })
    };

    // Full access: bypassPermissions, and the command runs with no card.
    phone.send("chat", frame("tool", "full_access")).await;
    let events = phone.until("chat_complete").await;
    assert_eq!(kinds(&events), ["tool_start", "tool_result", "chat_stream", "usage", "chat_complete"]);
    assert_eq!(streamed(&events), "Done.");
    assert_eq!(fake_mode(tmp.path(), &chat_id), "bypassPermissions");

    // Automatic: acceptEdits. An edit runs; a command asks.
    phone.send("chat", frame("edit", "automatic")).await;
    let events = phone.until("chat_complete").await;
    assert!(!kinds(&events).contains(&"ask_request"), "{events:?}");
    assert_eq!(fake_mode(tmp.path(), &chat_id), "acceptEdits");
    phone.send("chat", frame("tool", "automatic")).await;
    let events = phone.until("ask_request").await;
    assert_eq!(kinds(&events), ["tool_start", "ask_request"]);
    phone.send("ask_response", json!({ "request_id": "call_1", "value": "Deny" })).await;
    phone.until("chat_complete").await;

    // Ask: default. The edit asks now.
    phone.send("chat", frame("edit", "ask")).await;
    let events = phone.until("ask_request").await;
    assert_eq!(events.last().unwrap()["data"]["prompt"], "Edit notes.md\nList the files");
    assert_eq!(fake_mode(tmp.path(), &chat_id), "default");
    phone.send("ask_response", json!({ "request_id": "call_1", "value": "Allow once" })).await;
    phone.until("chat_complete").await;

    // Plan: the agent's plan mode.
    phone.send("chat", frame("hello", "plan")).await;
    phone.until("chat_complete").await;
    assert_eq!(fake_mode(tmp.path(), &chat_id), "plan");

    // No permission on the frame (the phone speaking to the bot directly):
    // the session stays as it is.
    phone
        .send("chat", json!({ "prompt": "hello", "agent_id": "assistant", "session_id": session_id }))
        .await;
    phone.until("chat_complete").await;
    assert_eq!(fake_mode(tmp.path(), &chat_id), "plan");
}

/// An agent that refuses the mode switch doesn't run the turn in a mode
/// the owner didn't choose: the turn fails and says so.
#[tokio::test]
async fn an_acp_mode_the_agent_refuses_fails_the_turn() {
    let tmp = tempfile::tempdir().unwrap();
    let (hub_url, _inbox) = serve_hub().await;
    let link = start_acp_link("refuses-modes", nebo_runtimes::acp::Agent::ClaudeCode, "Claude Code", tmp.path(), &hub_url).await;
    let mut phone = Phone::connect(link).await;
    phone
        .send("chat", json!({ "prompt": "tool", "agent_id": "assistant", "permission_mode": "full_access" }))
        .await;
    let created = phone.next().await;
    assert_eq!(created["type"], "chat_created");
    let events = phone.until("chat_error").await;
    assert_eq!(
        events.last().unwrap()["data"]["error"],
        "Claude Code could not switch to its bypassPermissions mode: Invalid Mode"
    );
}

#[tokio::test]
async fn an_acp_agent_that_exits_is_started_again_and_reopens_the_chat() {
    let tmp = tempfile::tempdir().unwrap();
    let (hub_url, _inbox) = serve_hub().await;
    let link = start_acp_link("normal", nebo_runtimes::acp::Agent::Other, "Fake Agent", tmp.path(), &hub_url).await;
    let mut phone = Phone::connect(link).await;
    phone.send("chat", json!({ "prompt": "hello", "agent_id": "assistant" })).await;
    let session_id = phone.next().await["data"]["session_id"].as_str().unwrap().to_owned();
    let chat_id = session_id.rsplit(":thread:").next().unwrap().to_owned();
    phone.until("chat_complete").await;

    // The process ends mid-turn: the turn fails in plain words.
    phone
        .send("chat", json!({ "prompt": "exit", "agent_id": "assistant", "session_id": session_id }))
        .await;
    let events = phone.until("chat_error").await;
    assert_eq!(
        events.last().unwrap()["data"]["error"],
        "Could not connect to Fake Agent. Try again."
    );

    // The next use starts it again; the chat is reopened with
    // `session/load`, whose replay is the transcript, and carries on.
    phone
        .send("chat", json!({ "prompt": "hello", "agent_id": "assistant", "session_id": session_id }))
        .await;
    let events = phone.until("chat_complete").await;
    assert_eq!(streamed(&events), "Hello");
    let page = get(link, &format!("/api/v1/chats/{chat_id}/messages")).await;
    let contents: Vec<&str> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["content"].as_str().unwrap())
        .collect();
    assert_eq!(contents, ["hello", "Hello", "hello", "Hello"]);
    assert_eq!(get(link, "/health").await["chat"], true);
}

#[tokio::test]
async fn an_acp_agent_that_is_not_signed_in_says_so() {
    let tmp = tempfile::tempdir().unwrap();
    let (hub_url, _inbox) = serve_hub().await;
    let link = start_acp_link(
        "signed-out",
        nebo_runtimes::acp::Agent::ClaudeCode,
        "Claude Code",
        tmp.path(),
        &hub_url,
    )
    .await;
    // Started and answering: chat is announced, so the owner hears why.
    assert_eq!(get(link, "/health").await["runtime"], "claude-code");
    let mut phone = Phone::connect(link).await;
    phone.send("chat", json!({ "prompt": "hello", "agent_id": "assistant" })).await;
    let error = phone.next().await;
    assert_eq!(error["type"], "chat_error");
    assert_eq!(
        error["data"]["error"],
        "Claude Code isn't signed in on this computer. Run `claude` once to sign in."
    );
}

#[tokio::test]
async fn an_acp_agent_that_will_not_start_is_not_announced() {
    use nebo_link::contract::acp::{Acp, Settings};
    let tmp = tempfile::tempdir().unwrap();
    let (hub_url, _inbox) = serve_hub().await;
    let backend = Acp::new(Settings {
        agent: nebo_runtimes::acp::Agent::Codex,
        name: "Codex".into(),
        command: nebo_runtimes::RuntimeCommand {
            program: tmp.path().join("no-such-agent").to_string_lossy().into_owned(),
            args: vec![],
            env: vec![],
        },
        workdir: tmp.path().join("work"),
        log: tmp.path().join("agent.log"),
        chats_file: tmp.path().join("acp-chats.json"),
    });
    let link = serve_contract(("codex", "Codex"), Arc::new(backend), &hub_url).await;
    assert_eq!(get(link, "/health").await["chat"], false);
    let (status, refused) = call(link, "GET", "/api/v1/agents/assistant/chats", "").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(refused["error"], "Could not connect to Codex. Try again.");
}

// -- Several agents on one bot ------------------------------------------------

/// A fake ACP agent hosted as `id`, with its own state (sessions) and folder
/// under `dir`.
fn fake_hosted(id: &str, label: &str, agent: nebo_runtimes::acp::Agent, dir: &std::path::Path) -> nebo_link::state::Hosted {
    use nebo_link::state::{AcpLink, Hosted, Via};
    Hosted {
        id: id.into(),
        label: label.into(),
        runtime: nebo_runtimes::Runtime::Acp(agent),
        via: Via::Acp(AcpLink {
            program: std::env::current_exe().unwrap().to_string_lossy().into_owned(),
            args: ["fake_acp_agent_process", "--exact", "--nocapture", "--test-threads=1"]
                .map(String::from)
                .to_vec(),
            env: vec![
                ("NEBO_LINK_FAKE_ACP".into(), "normal".into()),
                ("NEBO_LINK_FAKE_ACP_STATE".into(), dir.join(format!("{id}-state.json")).to_string_lossy().into_owned()),
            ],
            workdir: dir.join(format!("work-{id}")),
        }),
    }
}

/// The session id inside a chat frame's `session_id`.
fn chat_of(session_id: &str) -> String {
    session_id.rsplit(":thread:").next().unwrap().to_owned()
}

/// Events for `session` until one of `kind`, others' left out.
async fn until_on(phone: &mut Phone, session: &str, kind: &str) -> Vec<Value> {
    let mut events = Vec::new();
    loop {
        let event = phone.next().await;
        if event["data"]["session_id"] != session {
            continue;
        }
        let done = event["type"] == kind;
        events.push(event);
        if done {
            return events;
        }
    }
}

/// One bot hosting three agents, two of one runtime in different folders:
/// the roster lists each as its own employee; a chat reaches the agent it
/// names and only that one; a question goes back to the agent that asked;
/// an agent that dies takes no other with it; and agents join and leave
/// while the bot runs.
#[tokio::test]
async fn one_bot_hosts_several_agents_and_keeps_them_apart() {
    use nebo_link::contract::roster::Roster;
    use nebo_link::state::{Link, PRIMARY, Root};
    use nebo_runtimes::acp::Agent;
    let tmp = tempfile::tempdir().unwrap();
    let (hub_url, inbox) = serve_hub().await;
    let dir = Root::at(tmp.path().join("state")).bot(BOT);
    let site = fake_hosted(PRIMARY, "Claude Code", Agent::ClaudeCode, tmp.path());
    let api = fake_hosted("claude-code-api", "Claude Code · api", Agent::ClaudeCode, tmp.path());
    let codex = fake_hosted("codex", "Codex", Agent::Codex, tmp.path());
    let link = Link {
        bot_id: BOT.into(),
        name: "Mac".into(),
        owner_id: "owner-1".into(),
        endpoints: nebo_link::endpoints::Endpoints::from_env(),
        agents: vec![site.clone(), api.clone(), codex.clone()],
    };
    let members = link.agents.iter().filter_map(|a| nebo_link::link::acp_member(&dir, a)).collect();
    let roster = Arc::new(Roster::new(members));
    let bot = serve_contract(("claude-code", "Claude Code"), roster.clone(), &hub_url).await;

    // The roster: three employees, the first the primary.
    let agents = get(bot, "/api/v1/agents").await["agents"].as_array().unwrap().clone();
    let rows: Vec<(&str, &str)> = agents.iter().map(|a| (a["id"].as_str().unwrap(), a["name"].as_str().unwrap())).collect();
    assert_eq!(rows, [(PRIMARY, "Claude Code"), ("claude-code-api", "Claude Code · api"), ("codex", "Codex")]);
    assert!(agents[1]["description"].as_str().unwrap().ends_with("work-claude-code-api"), "{}", agents[1]);

    // A chat to each reaches that agent, in its own folder, and no other.
    let mut phone = Phone::connect(bot).await;
    let mut sessions = Vec::new();
    for id in [PRIMARY, "claude-code-api", "codex"] {
        phone.send("chat", json!({ "prompt": "hello", "agent_id": id })).await;
        let created = loop {
            let event = phone.next().await;
            if event["type"] == "chat_created" && event["data"]["agent_id"] == id {
                break event;
            }
        };
        let session = created["data"]["session_id"].as_str().unwrap().to_owned();
        let events = until_on(&mut phone, &session, "chat_complete").await;
        assert_eq!(streamed(&events), "Hello", "{id}");
        sessions.push(session);
    }
    let chats: Vec<String> = sessions.iter().map(|s| chat_of(s)).collect();
    // Every fake numbers its sessions from s1: the chat ids still differ.
    assert_eq!(chats, ["s1", "claude-code-api~s1", "codex~s1"]);
    for (id, file) in [(PRIMARY, "assistant-state.json"), ("claude-code-api", "claude-code-api-state.json"), ("codex", "codex-state.json")] {
        let state: Value = serde_json::from_str(&std::fs::read_to_string(tmp.path().join(file)).unwrap()).unwrap();
        assert_eq!(state["s1"][0]["user"], "hello", "{id} holds its own chat");
        assert_eq!(state.as_object().unwrap().len(), 1, "{id} holds only its own chat");
    }
    let listed = get(bot, "/api/v1/agents/claude-code-api/chats").await;
    assert_eq!(listed["chats"].as_array().unwrap().len(), 1);
    assert_eq!(listed["chats"][0]["id"], "claude-code-api~s1");
    let page = get(bot, "/api/v1/chats/codex~s1/messages").await;
    assert_eq!(page["messages"][0]["content"], "hello");

    // Two agents stop for permission at once, with the same tool call id:
    // each answer reaches the agent that asked.
    phone.send("chat", json!({ "prompt": "tool", "agent_id": "claude-code-api", "session_id": sessions[1] })).await;
    let api_ask = until_on(&mut phone, &sessions[1], "ask_request").await.pop().unwrap();
    phone.send("chat", json!({ "prompt": "tool", "agent_id": "codex", "session_id": sessions[2] })).await;
    let codex_ask = until_on(&mut phone, &sessions[2], "ask_request").await.pop().unwrap();
    let api_request = api_ask["data"]["request_id"].as_str().unwrap().to_owned();
    let codex_request = codex_ask["data"]["request_id"].as_str().unwrap().to_owned();
    assert_ne!(api_request, codex_request, "one id per question");
    phone.send("ask_response", json!({ "request_id": codex_request, "value": "Deny" })).await;
    let events = until_on(&mut phone, &sessions[2], "chat_complete").await;
    assert_eq!(streamed(&events), "Not run.", "Codex got its own answer");
    phone.send("ask_response", json!({ "request_id": api_request, "value": "Allow once" })).await;
    let events = until_on(&mut phone, &sessions[1], "chat_complete").await;
    assert_eq!(streamed(&events), "Done.", "the api folder's Claude Code got its own answer");
    let items = eventually(&inbox, 4).await;
    let titles: Vec<&str> = items.iter().filter_map(|i| i["title"].as_str()).collect();
    assert!(titles.contains(&"Claude Code · api asks to run `ls`") && titles.contains(&"Codex asks to run `ls`"), "{titles:?}");

    // One agent's process dies mid-turn: the others carry on.
    phone.send("chat", json!({ "prompt": "exit", "agent_id": PRIMARY, "session_id": sessions[0] })).await;
    let events = until_on(&mut phone, &sessions[0], "chat_error").await;
    assert_eq!(events.last().unwrap()["data"]["error"], "Could not connect to Claude Code. Try again.");
    for (id, session) in [("claude-code-api", &sessions[1]), ("codex", &sessions[2])] {
        phone.send("chat", json!({ "prompt": "hello", "agent_id": id, "session_id": session })).await;
        let events = until_on(&mut phone, session, "chat_complete").await;
        assert_eq!(streamed(&events), "Hello", "{id} is unaffected");
    }

    // Added and removed while the bot runs: Codex leaves (its process with
    // it), a Gemini CLI joins; the others keep their sessions.
    let gemini = fake_hosted("gemini", "Gemini CLI", Agent::Gemini, tmp.path());
    let after = Link {
        agents: vec![site.clone(), api.clone(), gemini],
        ..link.clone()
    };
    roster.set(nebo_link::link::reconcile(&dir, &link, &after, &roster.members()));
    let ids: Vec<String> = get(bot, "/api/v1/agents").await["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(ids, [PRIMARY, "claude-code-api", "gemini"]);
    let (status, _) = call(bot, "GET", "/api/v1/agents/codex/chats", "").await;
    assert_eq!(status, StatusCode::NOT_FOUND, "Codex is gone");
    phone.send("chat", json!({ "prompt": "hello", "agent_id": "claude-code-api", "session_id": sessions[1] })).await;
    let events = until_on(&mut phone, &sessions[1], "chat_complete").await;
    assert_eq!(streamed(&events), "Hello");
    let page = get(bot, "/api/v1/chats/claude-code-api~s1/messages").await;
    assert_eq!(page["messages"].as_array().unwrap().len(), 10, "the kept agent kept its session, every turn in it: {page}");
    phone.send("chat", json!({ "prompt": "hello", "agent_id": "gemini" })).await;
    let created = loop {
        let event = phone.next().await;
        if event["type"] == "chat_created" && event["data"]["agent_id"] == "gemini" {
            break event;
        }
    };
    let events = until_on(&mut phone, created["data"]["session_id"].as_str().unwrap(), "chat_complete").await;
    assert_eq!(streamed(&events), "Hello", "the agent added joined");
}

/// Against a real ACP agent on this machine (`NEBO_LINK_LIVE_ACP` =
/// `claude-code`, `codex`, `gemini` or `opencode`), found and started the
/// way pairing does, in a fresh folder, with the fake hub catching inbox
/// items. Runs on the agent owner's own sign-in and costs real model calls.
/// Turn 1: "reply with the word ok". Turn 2: a shell command that writes a
/// file, approved from the ask card when the agent asks (Claude Code does in
/// its default mode; Codex's sandbox may allow it without asking).
#[tokio::test]
#[ignore = "needs NEBO_LINK_LIVE_ACP and a signed-in agent"]
async fn live_acp_phone_flow() {
    use nebo_link::contract::acp::{Acp, Settings};
    use nebo_runtimes::{Environment, Runtime, detect};
    let Ok(key) = std::env::var("NEBO_LINK_LIVE_ACP") else {
        eprintln!("NEBO_LINK_LIVE_ACP not set; nothing to do");
        return;
    };
    let install = detect(&Environment::current())
        .into_iter()
        .find(|i| i.runtime.acp().is_some_and(|a| a.key() == key))
        .unwrap_or_else(|| panic!("{key} is not installed here"));
    assert!(install.config_error.is_none(), "{:?}", install.config_error);
    let Runtime::Acp(agent) = install.runtime else { unreachable!() };
    eprintln!("{} via `{} {}`", agent.name(), install.restart.program, install.restart.args.join(" "));
    let tmp = tempfile::tempdir().unwrap();
    let (hub_url, inbox) = serve_hub().await;
    let backend = Acp::new(Settings {
        agent,
        name: agent.name().into(),
        command: install.restart.clone(),
        workdir: tmp.path().join("work"),
        log: tmp.path().join("agent.log"),
        chats_file: tmp.path().join("acp-chats.json"),
    });
    let link = serve_contract((agent.key(), agent.name()), Arc::new(backend), &hub_url).await;
    // A first start may fetch the adapter: probe until it answers.
    let mut ready = false;
    for _ in 0..40 {
        if get(link, "/health").await["chat"] == true {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
    assert!(ready, "{} did not start: {}", agent.name(), std::fs::read_to_string(tmp.path().join("agent.log")).unwrap_or_default());

    let mut phone = Phone::connect(link).await;
    phone.send("chat", json!({ "prompt": "reply with the word ok", "agent_id": "assistant" })).await;
    let created = phone.next().await;
    eprintln!("turn 1: {created}");
    assert_eq!(created["type"], "chat_created", "{created}");
    let session_id = created["data"]["session_id"].as_str().unwrap().to_owned();
    let chat_id = session_id.rsplit(":thread:").next().unwrap().to_owned();
    let events = until_long(&mut phone, "chat_complete").await;
    for event in &events {
        eprintln!("turn 1: {event}");
    }
    let text = streamed(&events);
    assert!(text.to_lowercase().contains("ok"), "streamed {text:?}");
    assert!(events.iter().any(|e| e["type"] == "usage"), "usage reported");

    phone
        .send(
            "chat",
            json!({ "prompt": "Run the shell command `echo acp-live-ok > acp-live.txt && cat acp-live.txt` and tell me exactly what it printed.",
                    "agent_id": "assistant", "session_id": session_id }),
        )
        .await;
    let mut events = Vec::new();
    loop {
        let event = next_long(&mut phone).await;
        eprintln!("turn 2: {event}");
        let kind = event["type"].as_str().unwrap().to_owned();
        if kind == "ask_request" {
            let request_id = event["data"]["request_id"].clone();
            phone.send("ask_response", json!({ "request_id": request_id, "value": "Allow once" })).await;
        }
        events.push(event);
        if kind == "chat_complete" || kind == "chat_error" {
            break;
        }
    }
    assert_eq!(events.last().unwrap()["type"], "chat_complete");
    assert!(streamed(&events).contains("acp-live-ok"), "streamed {:?}", streamed(&events));
    assert!(events.iter().any(|e| e["type"] == "tool_start"), "the command shows as a tool card");
    let asked = events.iter().any(|e| e["type"] == "ask_request");
    eprintln!("asked: {asked}; inbox items: {:?}", inbox.lock().unwrap());
    let page = get(link, &format!("/api/v1/chats/{chat_id}/messages")).await;
    eprintln!("transcript: {}", page["messages"]);
}

async fn next_long(phone: &mut Phone) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(240), phone.ws.next())
        .await
        .expect("an event within 240 s")
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

async fn until_long(phone: &mut Phone, kind: &str) -> Vec<Value> {
    let mut events = Vec::new();
    loop {
        let event = next_long(phone).await;
        let done = event["type"] == kind || event["type"] == "chat_error";
        events.push(event);
        if done {
            return events;
        }
    }
}
