//! The OpenClaw gateway client against recorded frames, and (ignored) against
//! a live gateway.
//!
//! `fixtures/openclaw-gateway-frames.json` holds frames recorded from an
//! OpenClaw 2026.9.6 gateway (protocol 4) on 2026-09-25, trimmed of the
//! session snapshots the gateway spreads into events. The fake gateway here
//! replays them and admits the client the way the real one does: it checks
//! the proxy headers and verifies the Ed25519 device proof over the v3
//! payload (`packages/gateway-client/src/device-auth.ts:44-64`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use futures::{SinkExt, StreamExt};
use nebo_runtimes::ProxyAccess;
use nebo_runtimes::openclaw::gateway::{
    AgentStream, ApprovalKind, ChatSend, ChatState, Connect, Decision, Error, Event,
    FileDeviceStore, Gateway, History, HistoryQuery, QueueMode, SessionsQuery,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

const FRAMES: &str = include_str!("fixtures/openclaw-gateway-frames.json");
const IDENTITY: &str = "5a137883-49df-4063-b52e-00c1eea1f535";
const ORIGIN: &str = "https://neboai.com";
const FORWARDED_FOR: &str = "203.0.113.10";
const SPIKE_SESSION: &str = "agent:main:nebo-link-spike";

fn frames() -> HashMap<String, Value> {
    serde_json::from_str(FRAMES).expect("fixture parses")
}

/// A recorded frame; responses get the live request id, events go as is.
fn frame(name: &str) -> Value {
    frames()
        .remove(name)
        .unwrap_or_else(|| panic!("fixture {name}"))
}

fn payload(name: &str) -> Value {
    frame(name)["payload"].clone()
}

fn access() -> ProxyAccess {
    ProxyAccess {
        base_path: format!("/t/{IDENTITY}"),
        origin: ORIGIN.to_owned(),
        user_header: "x-nebo-user".to_owned(),
        identity: IDENTITY.to_owned(),
        password: "local-secret".to_owned(),
    }
}

/// What the fake gateway answers a request with: the response payload (or
/// an error shape) and the events to push right after it.
type Reply = (Result<Value, Value>, Vec<Value>);
type Handler = Arc<Mutex<dyn FnMut(&str, &Value) -> Reply + Send>>;

/// A recorded request the fake gateway saw.
#[derive(Debug, Clone)]
struct Seen {
    method: String,
    params: Value,
}

struct FakeGateway {
    url: String,
    seen: mpsc::UnboundedReceiver<Seen>,
    /// Push an event to the connected client at any time.
    push: mpsc::UnboundedSender<Value>,
}

/// How the fake admits `connect`.
#[derive(Clone, Copy)]
enum Admission {
    HelloOk,
    /// `res ok:false` with `PROTOCOL_MISMATCH` details, then close 1002.
    ProtocolMismatch,
    /// No response at all, just close 1002.
    CloseOnly,
}

async fn fake_gateway(admission: Admission, handler: Handler) -> FakeGateway {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (seen_tx, seen_rx) = mpsc::unbounded_channel();
    let (push_tx, mut push_rx) = mpsc::unbounded_channel::<Value>();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let headers = Arc::new(Mutex::new(HashMap::<String, String>::new()));
        let seen_headers = Arc::clone(&headers);
        // The tungstenite callback's error type is what it is.
        #[allow(clippy::result_large_err)]
        let capture = move |request: &Request, response: Response| {
            let mut headers = seen_headers.lock().unwrap();
            for (name, value) in request.headers() {
                headers.insert(name.to_string(), value.to_str().unwrap_or("").to_owned());
            }
            Ok(response)
        };
        let socket = tokio_tungstenite::accept_hdr_async(stream, capture)
            .await
            .unwrap();
        let (mut sink, mut stream) = socket.split();
        // The trusted proxy is the link itself: the identity header, the
        // allowlisted browser origin, and the forwarded client address.
        let headers = headers.lock().unwrap().clone();
        assert_eq!(
            headers.get("x-nebo-user").map(String::as_str),
            Some(IDENTITY)
        );
        assert_eq!(headers.get("origin").map(String::as_str), Some(ORIGIN));
        assert_eq!(
            headers.get("x-forwarded-for").map(String::as_str),
            Some(FORWARDED_FOR)
        );
        assert_eq!(
            headers.get("x-forwarded-proto").map(String::as_str),
            Some("https")
        );
        assert_eq!(
            headers.get("x-forwarded-host").map(String::as_str),
            Some("neboai.com")
        );

        let mut challenge = frame("challenge");
        let nonce = "0b6f6b8d-test-nonce";
        let ts: u64 = 1790397000000;
        challenge["payload"]["nonce"] = json!(nonce);
        challenge["payload"]["ts"] = json!(ts);
        sink.send(Message::Text(challenge.to_string().into()))
            .await
            .unwrap();

        let Some(Ok(Message::Text(text))) = stream.next().await else {
            panic!("expected the connect request");
        };
        let connect: Value = serde_json::from_str(text.as_str()).unwrap();
        assert_eq!(connect["type"], "req");
        assert_eq!(connect["method"], "connect");
        let params = &connect["params"];
        // `connect-admission.ts:243-244`: the range must contain 4.
        assert_eq!(params["minProtocol"], 4);
        assert_eq!(params["maxProtocol"], 4);
        assert_eq!(params["client"]["id"], "openclaw-control-ui");
        assert_eq!(params["client"]["mode"], "ui");
        assert_eq!(params["role"], "operator");
        assert_eq!(params["caps"], json!(["tool-events", "approvals"]));
        assert!(params.get("auth").is_none());
        verify_device_proof(params, nonce, ts);
        seen_tx
            .send(Seen {
                method: "connect".to_owned(),
                params: params.clone(),
            })
            .unwrap();

        let id = connect["id"].as_str().unwrap();
        match admission {
            Admission::HelloOk => {
                let mut hello = frame("hello_ok");
                hello["id"] = json!(id);
                sink.send(Message::Text(hello.to_string().into()))
                    .await
                    .unwrap();
            }
            Admission::ProtocolMismatch => {
                // `connect-admission.ts:266-275`.
                let refusal = json!({
                    "type": "res", "id": id, "ok": false,
                    "error": {
                        "code": "INVALID_REQUEST", "message": "protocol mismatch",
                        "details": {
                            "code": "PROTOCOL_MISMATCH", "clientMinProtocol": 4,
                            "clientMaxProtocol": 4, "expectedProtocol": 5, "minimumProbeProtocol": 3
                        }
                    }
                });
                sink.send(Message::Text(refusal.to_string().into()))
                    .await
                    .unwrap();
                let _ = sink
                    .send(Message::Close(Some(
                        tokio_tungstenite::tungstenite::protocol::CloseFrame {
                            code: 1002.into(),
                            reason: "protocol mismatch".into(),
                        },
                    )))
                    .await;
                return;
            }
            Admission::CloseOnly => {
                let _ = sink
                    .send(Message::Close(Some(
                        tokio_tungstenite::tungstenite::protocol::CloseFrame {
                            code: 1002.into(),
                            reason: "protocol mismatch".into(),
                        },
                    )))
                    .await;
                return;
            }
        }

        loop {
            tokio::select! {
                incoming = stream.next() => {
                    let Some(Ok(message)) = incoming else { break };
                    let Message::Text(text) = message else { continue };
                    let request: Value = serde_json::from_str(text.as_str()).unwrap();
                    let id = request["id"].as_str().unwrap().to_owned();
                    let method = request["method"].as_str().unwrap().to_owned();
                    let params = request["params"].clone();
                    seen_tx.send(Seen { method: method.clone(), params: params.clone() }).unwrap();
                    let (result, events) = handler.lock().unwrap()(&method, &params);
                    let response = match result {
                        Ok(payload) => json!({"type": "res", "id": id, "ok": true, "payload": payload}),
                        Err(error) => json!({"type": "res", "id": id, "ok": false, "error": error}),
                    };
                    sink.send(Message::Text(response.to_string().into())).await.unwrap();
                    for event in events {
                        sink.send(Message::Text(event.to_string().into())).await.unwrap();
                    }
                }
                pushed = push_rx.recv() => {
                    let Some(event) = pushed else { break };
                    if event.is_null() {
                        let _ = sink.send(Message::Close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                            code: 1012.into(),
                            reason: "restarting".into(),
                        }))).await;
                        break;
                    }
                    sink.send(Message::Text(event.to_string().into())).await.unwrap();
                }
            }
        }
    });
    FakeGateway {
        url,
        seen: seen_rx,
        push: push_tx,
    }
}

/// `connect-device-proof.ts:64-100` and `handshake-auth-helpers.ts:288-318`:
/// the device id is the SHA-256 of the raw key, the signature covers the v3
/// payload built from the connect params themselves.
fn verify_device_proof(params: &Value, nonce: &str, ts: u64) {
    let device = &params["device"];
    let public_key = URL_SAFE_NO_PAD
        .decode(device["publicKey"].as_str().unwrap())
        .unwrap();
    let expected_id: String = Sha256::digest(&public_key)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert_eq!(device["id"], expected_id, "device-id-mismatch");
    assert_eq!(device["nonce"], nonce, "device-nonce-mismatch");
    assert_eq!(device["signedAt"], ts, "device-signature-stale");
    let scopes: Vec<&str> = params["scopes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|scope| scope.as_str().unwrap())
        .collect();
    let payload = [
        "v3",
        device["id"].as_str().unwrap(),
        params["client"]["id"].as_str().unwrap(),
        params["client"]["mode"].as_str().unwrap(),
        params["role"].as_str().unwrap(),
        &scopes.join(","),
        &ts.to_string(),
        "",
        nonce,
        &params["client"]["platform"]
            .as_str()
            .unwrap()
            .to_ascii_lowercase(),
        "",
    ]
    .join("|");
    let key: [u8; 32] = public_key.try_into().unwrap();
    let signature: [u8; 64] = URL_SAFE_NO_PAD
        .decode(device["signature"].as_str().unwrap())
        .unwrap()
        .try_into()
        .unwrap();
    VerifyingKey::from_bytes(&key)
        .unwrap()
        .verify(payload.as_bytes(), &Signature::from_bytes(&signature))
        .expect("device signature invalid");
}

fn handler(f: impl FnMut(&str, &Value) -> Reply + Send + 'static) -> Handler {
    Arc::new(Mutex::new(f))
}

fn device_store(dir: &tempfile::TempDir) -> FileDeviceStore {
    FileDeviceStore::new(dir.path().join("device.json"))
}

async fn connect(
    fake: &FakeGateway,
    store: &FileDeviceStore,
) -> (Gateway, nebo_runtimes::openclaw::gateway::Events) {
    Gateway::connect(&Connect::new(&fake.url, &access(), FORWARDED_FOR), store)
        .await
        .expect("connect")
}

async fn next_event(events: &mut nebo_runtimes::openclaw::gateway::Events) -> Event {
    tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .expect("an event within 5s")
        .expect("the connection is open")
}

/// Skip the events the gateway sends on its own (`presence`, `health`,
/// `tick`) and return the next one the test is about.
async fn next_typed(events: &mut nebo_runtimes::openclaw::gateway::Events) -> Event {
    loop {
        match next_event(events).await {
            Event::Tick | Event::Other { .. } => continue,
            event => return event,
        }
    }
}

#[tokio::test]
async fn connect_is_admitted_as_the_trusted_proxy_and_keeps_its_device() {
    let dir = tempfile::tempdir().unwrap();
    let store = device_store(&dir);
    let fake = fake_gateway(
        Admission::HelloOk,
        handler(|_, _| (Ok(Value::Null), vec![])),
    )
    .await;
    let (gateway, _events) = connect(&fake, &store).await;
    let hello = gateway.hello();
    assert_eq!(hello.protocol, 4);
    assert_eq!(hello.server.version, "2026.9.6");
    assert_eq!(hello.auth.method.as_deref(), Some("trusted-proxy"));
    assert_eq!(hello.auth.role, "operator");
    // §9.1: the identity grant adds operator.admin with no pairing prompt.
    assert!(
        hello
            .auth
            .scopes
            .iter()
            .any(|scope| scope == "operator.admin")
    );
    assert!(
        hello
            .auth
            .scopes
            .iter()
            .any(|scope| scope == "operator.approvals")
    );
    assert_eq!(hello.policy.tick_interval_ms, 30000);
    assert!(gateway.closed().is_none());

    // The keypair is kept: a second connection proves the same device.
    let first = nebo_runtimes::openclaw::gateway::DeviceStore::load(&store)
        .unwrap()
        .unwrap()
        .device_id();
    let fake2 = fake_gateway(
        Admission::HelloOk,
        handler(|_, _| (Ok(Value::Null), vec![])),
    )
    .await;
    let mut seen = fake2.seen;
    let _second = Gateway::connect(&Connect::new(&fake2.url, &access(), FORWARDED_FOR), &store)
        .await
        .unwrap();
    let seen_connect = seen.recv().await.unwrap();
    assert_eq!(seen_connect.params["device"]["id"], first);
}

#[tokio::test]
async fn agents_and_sessions_are_typed_from_recorded_frames() {
    let dir = tempfile::tempdir().unwrap();
    let store = device_store(&dir);
    let fake = fake_gateway(
        Admission::HelloOk,
        handler(|method, _| match method {
            "agents.list" => (Ok(payload("agents_list")), vec![]),
            "sessions.subscribe" => (Ok(payload("sessions_subscribe")), vec![]),
            "sessions.list" => (Ok(payload("sessions_subscribe")["list"].clone()), vec![]),
            "sessions.messages.subscribe" => (
                Ok(json!({"subscribed": true, "key": "agent:main:nebo-link-spike"})),
                vec![],
            ),
            other => panic!("unexpected {other}"),
        }),
    )
    .await;
    let (gateway, _events) = connect(&fake, &store).await;
    let mut seen = fake.seen;
    seen.recv().await.unwrap();

    let agents = gateway.agents_list().await.unwrap();
    assert_eq!(agents.default_id, "main");
    assert_eq!(agents.main_key, "main");
    assert_eq!(agents.scope, "per-sender");
    assert_eq!(agents.agents.len(), 1);
    assert_eq!(agents.agents[0].id, "main");
    assert_eq!(
        agents.agents[0]
            .model
            .as_ref()
            .and_then(|m| m.primary.as_deref()),
        Some("neboai/nebo-1")
    );
    assert_eq!(seen.recv().await.unwrap().params, json!({}));

    let query = SessionsQuery {
        limit: Some(5),
        include_derived_titles: true,
        include_last_message: true,
        ..SessionsQuery::default()
    };
    let subscribed = gateway.sessions_subscribe(&query).await.unwrap();
    assert!(subscribed.subscribed);
    let list = subscribed.list.expect("list with non-empty params");
    assert_eq!(list.has_more, Some(false));
    assert_eq!(list.next_offset, None, "null on the wire");
    assert!(list.sessions.is_empty());
    let sent = seen.recv().await.unwrap();
    assert_eq!(
        sent.params,
        json!({"limit": 5, "includeDerivedTitles": true, "includeLastMessage": true}),
        "false flags are not sent"
    );

    let listed = gateway
        .sessions_list(&SessionsQuery::default())
        .await
        .unwrap();
    assert_eq!(listed.total_count, Some(0));
    assert_eq!(seen.recv().await.unwrap().params, json!({}));

    gateway
        .sessions_messages_subscribe("agent:main:nebo-link-spike", Some("main"))
        .await
        .unwrap();
    let sent = seen.recv().await.unwrap();
    assert_eq!(sent.method, "sessions.messages.subscribe");
    assert_eq!(
        sent.params,
        json!({"key": "agent:main:nebo-link-spike", "agentId": "main"})
    );
}

#[tokio::test]
async fn chat_send_streams_the_turn_and_steers_while_a_run_is_active() {
    let dir = tempfile::tempdir().unwrap();
    let store = device_store(&dir);
    let sends = Arc::new(Mutex::new(0u32));
    let counter = Arc::clone(&sends);
    let fake = fake_gateway(
        Admission::HelloOk,
        handler(move |method, _| match method {
            "chat.send" => {
                let mut sends = counter.lock().unwrap();
                *sends += 1;
                let events = if *sends == 1 {
                    vec![
                        frame("chat_status"),
                        frame("agent_lifecycle_start"),
                        frame("agent_tool_start"),
                        frame("agent_tool_update"),
                        frame("agent_tool_result"),
                        frame("agent_usage"),
                        frame("agent_assistant"),
                        frame("chat_delta"),
                        frame("session_message_assistant"),
                    ]
                } else {
                    vec![frame("chat_final")]
                };
                (Ok(payload("chat_send_ack")), events)
            }
            "chat.abort" => (Ok(json!({"ok": true})), vec![]),
            other => panic!("unexpected {other}"),
        }),
    )
    .await;
    let (gateway, mut events) = connect(&fake, &store).await;
    let mut seen = fake.seen;
    seen.recv().await.unwrap();

    let send = ChatSend {
        agent_id: Some("main".to_owned()),
        ..ChatSend::new(SPIKE_SESSION, "Use your exec tool to run `uname -a`.")
    };
    let ack = gateway.chat_send(&send).await.unwrap();
    assert_eq!(ack.status, "started");
    assert_eq!(ack.run_id, "probe-approval-1790397330775");
    assert_eq!(ack.message_seq, Some(3));
    let sent = seen.recv().await.unwrap();
    assert_eq!(sent.method, "chat.send");
    assert_eq!(sent.params["sessionKey"], SPIKE_SESSION);
    assert_eq!(sent.params["agentId"], "main");
    assert_eq!(sent.params["idempotencyKey"], send.idempotency_key);
    assert!(
        sent.params.get("queueMode").is_none(),
        "idle session: no queue mode"
    );
    assert_eq!(
        gateway.active_run(SPIKE_SESSION).as_deref(),
        Some(ack.run_id.as_str())
    );

    let run_id = ack.run_id.clone();
    let Event::Chat(status) = next_typed(&mut events).await else {
        panic!("status first")
    };
    assert_eq!(status.run_id, run_id);
    assert!(matches!(&status.state, ChatState::Status { phase } if phase == "preparing_workspace"));

    let Event::Agent(start) = next_typed(&mut events).await else {
        panic!("lifecycle")
    };
    assert!(matches!(&start.stream, AgentStream::Lifecycle { phase, .. } if phase == "start"));
    assert_eq!(start.session_key.as_deref(), Some(SPIKE_SESSION));

    let Event::Agent(tool_start) = next_typed(&mut events).await else {
        panic!("tool start")
    };
    let AgentStream::ToolStart {
        tool_call_id,
        name,
        args,
        parent_tool_call_id,
    } = &tool_start.stream
    else {
        panic!("tool start, got {:?}", tool_start.stream)
    };
    assert_eq!(tool_call_id, "call_9ec1cef66f2a4b8e85c5e9c1");
    assert_eq!(name, "exec");
    assert_eq!(args["command"], "uname -a");
    assert!(parent_tool_call_id.is_none());

    let Event::Agent(update) = next_typed(&mut events).await else {
        panic!("tool update")
    };
    let AgentStream::ToolUpdate { partial_result, .. } = &update.stream else {
        panic!("tool update, got {:?}", update.stream)
    };
    assert!(
        partial_result["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("Darwin")
    );

    let Event::Agent(result) = next_typed(&mut events).await else {
        panic!("tool result")
    };
    let AgentStream::ToolResult {
        tool_call_id,
        is_error,
        result,
        meta,
        ..
    } = &result.stream
    else {
        panic!("tool result, got {:?}", result.stream)
    };
    assert_eq!(tool_call_id, "call_9ec1cef66f2a4b8e85c5e9c1");
    assert!(!is_error);
    assert_eq!(meta.as_deref(), Some("uname -a"));
    assert_eq!(result["details"]["exitCode"], 0);

    let Event::Agent(usage) = next_typed(&mut events).await else {
        panic!("usage")
    };
    assert!(matches!(
        usage.stream,
        AgentStream::Usage { output_tokens: 27 }
    ));

    let Event::Agent(assistant) = next_typed(&mut events).await else {
        panic!("assistant")
    };
    assert!(
        matches!(&assistant.stream, AgentStream::Other { stream, .. } if stream == "assistant")
    );

    let Event::Chat(delta) = next_typed(&mut events).await else {
        panic!("delta")
    };
    assert!(matches!(&delta.state, ChatState::Delta { delta_text, .. } if delta_text == "The"));
    assert!(!delta.state.is_terminal());

    let Event::SessionMessage(row) = next_typed(&mut events).await else {
        panic!("session.message")
    };
    assert_eq!(row.session_key, SPIKE_SESSION);
    assert_eq!(row.role(), Some("assistant"));
    assert_eq!(row.usage().unwrap()["output"], 81);
    assert_eq!(row.stop_reason(), Some("stop"));
    assert_eq!(row.run_id.as_deref(), Some("probe-approval-1790397330775"));

    // A second message while the run is active is a steer.
    let ack2 = gateway
        .chat_send(&ChatSend::new(SPIKE_SESSION, "also say hi"))
        .await
        .unwrap();
    let sent = seen.recv().await.unwrap();
    assert_eq!(sent.params["queueMode"], "steer");
    assert_eq!(ack2.status, "started");

    let Event::Chat(done) = next_typed(&mut events).await else {
        panic!("final")
    };
    assert!(
        matches!(&done.state, ChatState::Final { stop_reason: Some(reason), .. } if reason == "stop")
    );
    assert!(done.state.is_terminal());
    assert_eq!(
        gateway.active_run(SPIKE_SESSION),
        None,
        "the final event ends the run"
    );

    // An explicit queue mode is sent as given, and abort names the run.
    let explicit = ChatSend {
        queue_mode: Some(QueueMode::Followup),
        ..ChatSend::new(SPIKE_SESSION, "later")
    };
    gateway.chat_send(&explicit).await.unwrap();
    assert_eq!(seen.recv().await.unwrap().params["queueMode"], "followup");
    gateway
        .chat_abort(SPIKE_SESSION, Some("main"), Some(&run_id))
        .await
        .unwrap();
    let abort = seen.recv().await.unwrap();
    assert_eq!(abort.method, "chat.abort");
    assert_eq!(
        abort.params,
        json!({"sessionKey": SPIKE_SESSION, "agentId": "main", "runId": run_id})
    );
}

#[tokio::test]
async fn history_pages_and_recovers_a_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let store = device_store(&dir);
    let fake = fake_gateway(
        Admission::HelloOk,
        handler(|method, params| {
            assert_eq!(method, "chat.history");
            match params.get("cursor").and_then(Value::as_str) {
                None => (Ok(payload("chat_history_tail")), vec![]),
                Some("stale") => (Ok(payload("chat_history_reset")), vec![]),
                Some(_) => (Ok(payload("chat_history_delta")), vec![]),
            }
        }),
    )
    .await;
    let (gateway, _events) = connect(&fake, &store).await;
    let mut seen = fake.seen;
    seen.recv().await.unwrap();

    let query = HistoryQuery {
        session_key: SPIKE_SESSION.to_owned(),
        limit: Some(5),
        ..HistoryQuery::default()
    };
    let History::Page(page) = gateway.chat_history(&query).await.unwrap() else {
        panic!("a tail page")
    };
    assert_eq!(page.session_key, SPIKE_SESSION);
    assert_eq!(page.messages.len(), 2);
    assert_eq!(page.messages[0]["role"], "user");
    assert_eq!(page.messages[1]["role"], "assistant");
    assert_eq!(page.has_more, Some(false));
    let cursor = page
        .delta_cursor
        .clone()
        .expect("a tail page carries a cursor");
    assert_eq!(page.session_info.has_active_run, Some(false));
    assert!(page.in_flight_run.is_none());
    assert_eq!(gateway.active_run(SPIKE_SESSION), None);
    assert_eq!(
        seen.recv().await.unwrap().params,
        json!({"sessionKey": SPIKE_SESSION, "limit": 5})
    );

    // Catch up from the cursor: the gateway's delta entries are
    // `session.message` payloads; the client hands back transcript rows.
    let catch_up = HistoryQuery {
        cursor: Some(cursor.clone()),
        ..query.clone()
    };
    let History::Delta(delta) = gateway.chat_history(&catch_up).await.unwrap() else {
        panic!("a delta")
    };
    assert_eq!(delta.messages.len(), 4);
    assert_eq!(delta.messages[0]["role"], "user");
    assert_eq!(
        delta.messages[0]["content"],
        "Use your exec tool to run the shell command `uname -a` and tell me the output in one line."
    );
    assert_eq!(delta.messages[3]["role"], "assistant");
    assert_ne!(delta.delta_cursor, cursor);
    assert_eq!(seen.recv().await.unwrap().params["cursor"], cursor);

    // A stale cursor resets; the client fetches the tail instead.
    let stale = HistoryQuery {
        cursor: Some("stale".to_owned()),
        ..query.clone()
    };
    let History::Page(again) = gateway.chat_history(&stale).await.unwrap() else {
        panic!("the tail page after a reset")
    };
    assert_eq!(again.messages.len(), 2);
    assert_eq!(seen.recv().await.unwrap().params["cursor"], "stale");
    let tail = seen.recv().await.unwrap();
    assert!(tail.params.get("cursor").is_none());
}

#[tokio::test]
async fn approvals_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = device_store(&dir);
    let fake = fake_gateway(
        Admission::HelloOk,
        handler(|method, params| match method {
            "exec.approval.list" => (Ok(payload("exec_approval_list_pending")), vec![]),
            "approval.resolve" => {
                assert_eq!(params["id"], "1759b5f3-9141-4537-8c78-9afc7fa627c2");
                assert_eq!(params["kind"], "exec");
                assert_eq!(params["decision"], "allow-once");
                (
                    Ok(payload("approval_resolve")),
                    vec![frame("exec_approval_resolved")],
                )
            }
            other => panic!("unexpected {other}"),
        }),
    )
    .await;
    let (gateway, mut events) = connect(&fake, &store).await;
    let mut seen = fake.seen;
    seen.recv().await.unwrap();

    fake.push.send(frame("exec_approval_requested")).unwrap();
    let Event::ApprovalRequested(requested) = next_typed(&mut events).await else {
        panic!("requested")
    };
    assert_eq!(requested.kind(), ApprovalKind::Exec);
    assert_eq!(requested.id, "1759b5f3-9141-4537-8c78-9afc7fa627c2");
    assert_eq!(requested.session_key(), Some(SPIKE_SESSION));
    assert_eq!(requested.agent_id(), Some("main"));
    assert_eq!(requested.expires_at_ms - requested.created_at_ms, 45_000);
    let nebo_runtimes::openclaw::gateway::ApprovalRequest::Exec(exec) = &requested.request else {
        panic!("exec request")
    };
    assert_eq!(exec.command.as_deref(), Some("uname -a"));
    assert_eq!(
        requested.allowed_decisions(),
        vec![Decision::AllowOnce, Decision::AllowAlways, Decision::Deny]
    );

    let pending = gateway.exec_approval_list().await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].id, requested.id);
    assert_eq!(pending[0].kind(), ApprovalKind::Exec);

    let resolved = gateway
        .approval_resolve(&requested.id, ApprovalKind::Exec, Decision::AllowOnce)
        .await
        .unwrap();
    assert!(resolved.applied);
    assert_eq!(resolved.approval.status, "allowed");
    assert_eq!(resolved.approval.decision, Some(Decision::AllowOnce));
    assert_eq!(resolved.approval.reason.as_deref(), Some("user"));

    let Event::ApprovalResolved { kind, id, payload } = next_typed(&mut events).await else {
        panic!("resolved")
    };
    assert_eq!(kind, ApprovalKind::Exec);
    assert_eq!(id, requested.id);
    assert_eq!(payload["decision"], "allow-once");
    assert_eq!(payload["resolvedBy"], "Nebo Link probe");
}

#[tokio::test]
async fn sessions_changed_and_unknown_events_are_delivered() {
    let dir = tempfile::tempdir().unwrap();
    let store = device_store(&dir);
    let fake = fake_gateway(
        Admission::HelloOk,
        handler(|_, _| (Ok(Value::Null), vec![])),
    )
    .await;
    let (_gateway, mut events) = connect(&fake, &store).await;

    fake.push.send(frame("sessions_changed")).unwrap();
    let Event::SessionsChanged(changed) = next_typed(&mut events).await else {
        panic!("sessions.changed")
    };
    assert_eq!(changed.session_key.as_deref(), Some(SPIKE_SESSION));
    assert_eq!(changed.agent_id.as_deref(), Some("main"));
    assert_eq!(changed.reason.as_deref(), Some("agent.run.started"));
    let row = changed.session.expect("a keyed change carries the row");
    assert_eq!(row.key, SPIKE_SESSION);
    assert_eq!(row.kind, "direct");
    assert_eq!(row.model.as_deref(), Some("nebo-1"));
    assert_eq!(row.rest["hasActiveRun"], true);

    fake.push
        .send(json!({"type": "event", "event": "tick", "payload": {"ts": 1}}))
        .unwrap();
    assert!(matches!(next_event(&mut events).await, Event::Tick));
    fake.push
        .send(json!({"type": "event", "event": "presence", "payload": {"presence": []}}))
        .unwrap();
    assert!(
        matches!(next_event(&mut events).await, Event::Other { event, .. } if event == "presence")
    );
    fake.push
        .send(json!({"type": "event", "event": "shutdown", "payload": {"reason": "restart", "restartExpectedMs": 500}}))
        .unwrap();
    assert!(matches!(
        next_event(&mut events).await,
        Event::Shutdown { reason, restart_expected_ms: Some(500) } if reason == "restart"
    ));
}

#[tokio::test]
async fn a_refused_method_and_a_closed_socket_are_errors() {
    let dir = tempfile::tempdir().unwrap();
    let store = device_store(&dir);
    let fake = fake_gateway(
        Admission::HelloOk,
        handler(|_, _| {
            (
                Err(json!({"code": "FORBIDDEN", "message": "missing scope: operator.admin", "details": {"code": "MISSING_SCOPE"}})),
                vec![],
            )
        }),
    )
    .await;
    let (gateway, mut events) = connect(&fake, &store).await;
    let error = gateway.agents_list().await.unwrap_err();
    let Error::Rejected {
        method,
        code,
        message,
        retryable,
    } = error
    else {
        panic!("rejected, got {error}")
    };
    assert_eq!(code, "FORBIDDEN");
    assert!(message.contains("missing scope"));
    assert!(!retryable);
    assert!(method.starts_with("nl-"), "the request id names the call");

    fake.push.send(Value::Null).unwrap();
    let ended = tokio::time::timeout(Duration::from_secs(5), events.next())
        .await
        .expect("the stream ends within 5s");
    assert!(ended.is_none(), "the event stream ends with the socket");
    let closed = gateway.closed().expect("the reason is kept");
    assert!(matches!(closed, Error::Closed { code: 1012, ref reason } if reason == "restarting"));
    assert!(matches!(
        gateway.agents_list().await,
        Err(Error::Closed { code: 1012, .. })
    ));
}

#[tokio::test]
async fn a_protocol_mismatch_is_plain_words() {
    let dir = tempfile::tempdir().unwrap();
    let store = device_store(&dir);
    let fake = fake_gateway(
        Admission::ProtocolMismatch,
        handler(|_, _| (Ok(Value::Null), vec![])),
    )
    .await;
    let error = Gateway::connect(&Connect::new(&fake.url, &access(), FORWARDED_FOR), &store)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::ProtocolMismatch { expected: Some(5) }
    ));
    assert_eq!(
        error.to_string(),
        "this OpenClaw speaks gateway protocol 5; Nebo Link speaks protocol 4. Update Nebo Link, or OpenClaw, so the two match"
    );

    // A gateway that only closes with 1002 still reads as a mismatch.
    let fake = fake_gateway(
        Admission::CloseOnly,
        handler(|_, _| (Ok(Value::Null), vec![])),
    )
    .await;
    let error = Gateway::connect(&Connect::new(&fake.url, &access(), FORWARDED_FOR), &store)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::ProtocolMismatch { expected: None }));
    assert_eq!(
        error.to_string(),
        "this OpenClaw speaks a different gateway protocol; Nebo Link speaks protocol 4. Update Nebo Link, or OpenClaw, so the two match"
    );

    // Nothing listening: a plain connect error naming the address.
    let error = Gateway::connect(
        &Connect::new("ws://127.0.0.1:9", &access(), FORWARDED_FOR),
        &store,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, Error::Connect { .. }));
}

/// The live spike (§9.1): needs a gateway in trusted-proxy mode configured
/// by `Change::ProxyAccess`. Set:
/// - `OPENCLAW_GATEWAY_URL` (`ws://127.0.0.1:<port>`),
/// - `OPENCLAW_IDENTITY` (the identity in `identityScopes`),
/// - `OPENCLAW_ORIGIN` (default `https://neboai.com`),
/// - `OPENCLAW_DEVICE_FILE` (default: a temp file, so every run is a new
///   device that auto-approval must pair),
/// - `OPENCLAW_LIVE_TURN=1` to also send one short chat turn (costs model
///   time).
#[tokio::test]
#[ignore = "needs a live OpenClaw gateway; see the doc comment"]
async fn live_gateway() {
    let Ok(url) = std::env::var("OPENCLAW_GATEWAY_URL") else {
        eprintln!("OPENCLAW_GATEWAY_URL unset; nothing to do");
        return;
    };
    let identity = std::env::var("OPENCLAW_IDENTITY").expect("OPENCLAW_IDENTITY");
    let origin = std::env::var("OPENCLAW_ORIGIN").unwrap_or_else(|_| ORIGIN.to_owned());
    let dir = tempfile::tempdir().unwrap();
    let store = FileDeviceStore::new(
        std::env::var("OPENCLAW_DEVICE_FILE")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| dir.path().join("device.json")),
    );
    let access = ProxyAccess {
        base_path: format!("/t/{identity}"),
        origin,
        user_header: "x-nebo-user".to_owned(),
        identity,
        password: String::new(),
    };
    let (gateway, mut events) =
        Gateway::connect(&Connect::new(&url, &access, FORWARDED_FOR), &store)
            .await
            .expect("connect");
    let hello = gateway.hello();
    eprintln!(
        "hello-ok: protocol {} server {} auth {:?}",
        hello.protocol, hello.server.version, hello.auth
    );
    assert_eq!(hello.auth.method.as_deref(), Some("trusted-proxy"));
    assert!(
        hello
            .auth
            .scopes
            .iter()
            .any(|scope| scope == "operator.admin"),
        "identityScopes grant admin"
    );

    let agents = gateway.agents_list().await.expect("agents.list");
    eprintln!(
        "agents: default {} of {:?}",
        agents.default_id,
        agents.agents.iter().map(|a| &a.id).collect::<Vec<_>>()
    );
    let roster = gateway
        .sessions_subscribe(&SessionsQuery {
            limit: Some(10),
            include_derived_titles: true,
            include_last_message: true,
            ..SessionsQuery::default()
        })
        .await
        .expect("sessions.subscribe");
    eprintln!(
        "sessions: {} rows",
        roster.list.map_or(0, |l| l.sessions.len())
    );
    let pending = gateway
        .exec_approval_list()
        .await
        .expect("exec.approval.list");
    eprintln!("pending exec approvals: {}", pending.len());

    let session_key = format!("agent:{}:nebo-link-live-test", agents.default_id);
    let history = gateway
        .chat_history(&HistoryQuery {
            session_key: session_key.clone(),
            limit: Some(5),
            ..HistoryQuery::default()
        })
        .await
        .expect("chat.history");
    let cursor = history.delta_cursor().map(str::to_owned);
    eprintln!(
        "history: {} rows, cursor {:?}",
        history.messages().len(),
        cursor
    );

    if std::env::var("OPENCLAW_LIVE_TURN").as_deref() != Ok("1") {
        return;
    }
    let ack = gateway
        .chat_send(&ChatSend::new(
            &session_key,
            "Reply with exactly the word: pong",
        ))
        .await
        .expect("chat.send");
    eprintln!("run {} {}", ack.run_id, ack.status);
    let mut deltas = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    loop {
        let event = tokio::time::timeout_at(deadline, events.next())
            .await
            .expect("a terminal chat event within 120s")
            .expect("connection open");
        match event {
            Event::Chat(chat) if chat.run_id == ack.run_id => match &chat.state {
                ChatState::Delta { delta_text, .. } => {
                    deltas += 1;
                    eprintln!("delta: {delta_text:?}");
                }
                state if state.is_terminal() => {
                    eprintln!("terminal: {state:?}");
                    assert!(matches!(state, ChatState::Final { .. }));
                    break;
                }
                _ => {}
            },
            Event::Agent(agent) if agent.run_id == ack.run_id => {
                eprintln!("agent: {:?}", agent.stream)
            }
            _ => {}
        }
    }
    assert!(deltas > 0, "the turn streamed deltas");
    assert_eq!(gateway.active_run(&session_key), None);
    let caught_up = gateway
        .chat_history(&HistoryQuery {
            session_key: session_key.clone(),
            cursor,
            limit: Some(5),
            ..HistoryQuery::default()
        })
        .await
        .expect("chat.history with cursor");
    eprintln!(
        "catch-up: {} rows ({})",
        caught_up.messages().len(),
        match caught_up {
            History::Delta(_) => "delta",
            History::Page(_) => "page after reset",
        }
    );
}
