//! The chat contract on the link's listener, in front of a fake Hermes API
//! server and a fake hub: the phone's whole flow (roster, chat, a streamed
//! turn with tool events, the transcript, an approval as an ask card with
//! its hub inbox item, cancel), and the version gate on the transcript
//! (`conversation_history` sent to 0.19.0, not to 0.21.2+).
//!
//! The Hermes frames are the shapes `gateway/platforms/api_server.py` wrote
//! on the live v0.19.0 server on 2026-09-26 (no `id:` lines, no
//! `request_id` on `approval.request`) and at hermes-agent `d0288be5b3`.

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
use nebo_link::contract::hermes::Hermes;
use nebo_link::contract::{Contract, Inbox};
use nebo_link::proxy::{self, Body, BoxError, Control, Target};
use nebo_runtimes::{PathMode, ProxyRoute};
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

/// The link's listener with the contract in front of `hermes`, the way
/// `run.rs` serves it; the runtime UI target is an unused port.
async fn start_link(hermes: SocketAddr, hub: &str) -> SocketAddr {
    let unused = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let target = Target {
        upstream: unused,
        base_path: format!("/t/{BOT}"),
        route: ProxyRoute {
            path_mode: PathMode::StripWithForwardedPrefix,
            origin: None,
            identity_header: None,
        },
        identity: "owner-1".into(),
        runtime_name: "Hermes",
    };
    let backend = Hermes::new(&format!("http://{hermes}"), KEY, vec!["coder".into()]);
    let (_token_tx, token_rx) = watch::channel("bot-token".to_string());
    let contract = Contract::new(
        "hermes",
        "Hermes",
        BOT,
        Arc::new(backend),
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
    let (_token_tx, token_rx) = watch::channel("bot-token".to_string());
    let contract = Contract::new(
        "hermes",
        "Hermes",
        BOT,
        Arc::new(backend),
        Some(Inbox::new(&hub_url, BOT, token_rx)),
    );
    let unused = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let target = Target {
        upstream: unused,
        base_path: format!("/t/{BOT}"),
        route: ProxyRoute {
            path_mode: PathMode::StripWithForwardedPrefix,
            origin: None,
            identity_header: None,
        },
        identity: "owner-1".into(),
        runtime_name: "Hermes",
    };
    let listener = proxy::bind_loopback("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let link = listener.local_addr().unwrap();
    tokio::spawn(proxy::serve(
        listener,
        target,
        STAMP.into(),
        Arc::new(NoControl),
        Some(contract),
    ));

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
