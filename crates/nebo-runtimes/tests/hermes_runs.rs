//! The Hermes runs client against recorded API-server exchanges, and one
//! ignored test against a live API server.
//!
//! The recorded frames are the shapes `gateway/platforms/api_server.py` and
//! `api_server_runs.py` write at hermes-agent `d0288be5b3` (Python
//! `json.dumps` spacing, `id:` then `data:`), as observed on a live v0.19.0
//! server on 2026-09-26.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use nebo_runtimes::hermes::runs::{
    Choice, Client, Error, Event, HistoryMessage, MessageQuery, NewRun, NewSession, Order,
    RunState, STEER_FEATURE, SessionQuery, StopOutcome,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const KEY: &str = "0123456789abcdef0123";

/// One request the fake server saw.
#[derive(Debug, Clone)]
struct Request {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: String,
}

impl Request {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).unwrap_or(serde_json::Value::Null)
    }
}

/// What the fake server answers.
struct Response {
    status: u16,
    headers: Vec<(&'static str, String)>,
    /// Written as separate chunks; SSE bodies are several.
    chunks: Vec<String>,
    /// Close the socket after the chunks without ending the body, as a
    /// dropped connection does.
    cut: bool,
}

impl Response {
    fn json(status: u16, body: &str) -> Self {
        Self {
            status,
            headers: vec![("Content-Type", "application/json".into())],
            chunks: vec![body.to_owned()],
            cut: false,
        }
    }

    fn sse(chunks: &[&str]) -> Self {
        Self {
            status: 200,
            headers: vec![("Content-Type", "text/event-stream".into())],
            chunks: chunks.iter().map(|c| (*c).to_owned()).collect(),
            cut: false,
        }
    }
}

type Handler = Arc<dyn Fn(&Request) -> Response + Send + Sync>;

/// Serves `handler` on a loopback port; every request is recorded.
async fn serve(handler: Handler) -> (SocketAddr, Arc<Mutex<Vec<Request>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let handler = handler.clone();
            let log = log.clone();
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut buf = [0u8; 4096];
                let head_end = loop {
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    raw.extend_from_slice(&buf[..n]);
                    if let Some(i) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                };
                let head = String::from_utf8_lossy(&raw[..head_end]).into_owned();
                let mut lines = head.lines();
                let mut first = lines.next().unwrap().split_whitespace();
                let method = first.next().unwrap().to_owned();
                let path = first.next().unwrap().to_owned();
                let headers: Vec<(String, String)> = lines
                    .filter_map(|l| l.split_once(':'))
                    .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
                    .collect();
                let length: usize = headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, v)| v.parse().ok())
                    .unwrap_or(0);
                while raw.len() < head_end + length {
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    raw.extend_from_slice(&buf[..n]);
                }
                let body = String::from_utf8_lossy(&raw[head_end..head_end + length]).into_owned();
                let request = Request {
                    method,
                    path,
                    headers,
                    body,
                };
                let response = handler(&request);
                log.lock().unwrap().push(request);
                let mut head = format!(
                    "HTTP/1.1 {} X\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n",
                    response.status
                );
                for (k, v) in &response.headers {
                    head.push_str(&format!("{k}: {v}\r\n"));
                }
                head.push_str("\r\n");
                let _ = socket.write_all(head.as_bytes()).await;
                for chunk in &response.chunks {
                    let framed = format!("{:x}\r\n{chunk}\r\n", chunk.len());
                    let _ = socket.write_all(framed.as_bytes()).await;
                    let _ = socket.flush().await;
                }
                if !response.cut {
                    let _ = socket.write_all(b"0\r\n\r\n").await;
                }
                let _ = socket.shutdown().await;
            });
        }
    });
    (addr, seen)
}

fn client(addr: SocketAddr, profile: Option<&str>) -> Client {
    Client::new(&format!("http://{addr}"), profile, KEY)
}

fn handler(f: impl Fn(&Request) -> Response + Send + Sync + 'static) -> Handler {
    Arc::new(f)
}

fn frame(seq: u64, json: &str) -> String {
    format!("id: {seq}\ndata: {json}\n\n")
}

// -- Recorded responses -----------------------------------------------------

const CAPABILITIES: &str = r#"{"object": "hermes.api_server.capabilities", "platform": "hermes-agent", "model": "hermes-agent", "auth": {"type": "bearer", "required": true}, "runtime": {"mode": "server_agent", "tool_execution": "server", "split_runtime": false, "description": "..."}, "features": {"chat_completions": true, "chat_completions_streaming": true, "responses_api": true, "responses_streaming": true, "run_submission": true, "runs_idempotency": {"supported": true, "durable": true, "retention_seconds": 86400}, "run_status": true, "run_events_sse": true, "run_stop": true, "run_steer": true, "run_approval_response": true, "tool_progress_events": true, "approval_events": true, "session_resources": true, "model_options": true, "session_chat": true, "session_chat_streaming": true, "session_fork": true, "session_model_lock": true, "reasoning_streaming": true, "admin_config_rw": false, "jobs_admin": false, "memory_write_api": false, "skills_api": true, "audio_api": false, "realtime_voice": false, "session_continuity_header": "X-Hermes-Session-Id", "session_key_header": "X-Hermes-Session-Key", "cors": false}, "endpoints": {"runs": {"method": "POST", "path": "/v1/runs"}, "run_events": {"method": "GET", "path": "/v1/runs/{run_id}/events"}}}"#;

const SESSION: &str = r#"{"id": "api_1790400000_ab12cd34", "source": "api_server", "user_id": null, "model": "nebo-1", "title": "First chat", "started_at": 1790400000.5, "ended_at": null, "end_reason": null, "message_count": 2, "tool_call_count": 0, "input_tokens": 120, "output_tokens": 8, "cache_read_tokens": 0, "cache_write_tokens": 0, "reasoning_tokens": 0, "estimated_cost_usd": 0.0, "actual_cost_usd": null, "api_call_count": 1, "parent_session_id": null, "last_active": 1790400010.0, "preview": "Reply with exactly: pong", "pinned": false, "archived": false, "hidden": false, "has_system_prompt": false, "has_model_config": false}"#;

const MESSAGES: &str = r#"{"object": "list", "session_id": "api_1790400000_ab12cd34_c2", "data": [{"id": 11, "session_id": "api_1790400000_ab12cd34_c2", "role": "user", "content": "Reply with exactly: pong", "tool_call_id": null, "tool_calls": null, "tool_name": null, "timestamp": 1790400001.0, "token_count": null, "finish_reason": null, "reasoning": null, "reasoning_content": null, "display_kind": null}, {"id": 12, "session_id": "api_1790400000_ab12cd34_c2", "role": "assistant", "content": [{"type": "text", "text": "pong"}], "tool_call_id": null, "tool_calls": null, "tool_name": null, "timestamp": 1790400003.0, "token_count": 1, "finish_reason": "stop", "reasoning": null, "reasoning_content": null, "display_kind": null}], "pagination": {"limit": 500, "offset": 0, "order": "latest", "returned": 2}}"#;

const RUN_STATUS_WAITING: &str = r#"{"object": "hermes.run", "run_id": "run_9f1", "status": "waiting_for_approval", "updated_at": 1790400012.0, "created_at": 1790400010.0, "session_id": "api_1790400000_ab12cd34_c2", "model": "hermes-agent", "last_event": "approval.request", "approval": {"command": "rm -rf ./scratch", "pattern_key": "rm -rf", "pattern_keys": ["rm -rf"], "description": "Recursive delete", "allow_permanent": true, "allow_session": true, "request_id": "8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f", "event": "approval.request", "run_id": "run_9f1", "timestamp": 1790400012.0, "choices": ["once", "session", "always", "deny"]}}"#;

const RUN_STATUS_DONE: &str = r#"{"object": "hermes.run", "run_id": "run_9f1", "status": "completed", "updated_at": 1790400020.0, "created_at": 1790400010.0, "session_id": "api_1790400000_ab12cd34_c2", "model": "hermes-agent", "last_event": "run.completed", "completed": true, "partial": false, "interrupted": false, "output": "pong", "usage": {"input_tokens": 1520, "output_tokens": 4, "total_tokens": 1524, "cache_read_tokens": 0, "cache_write_tokens": 0}, "runtime": {"provider": "neboai", "model": "nebo-1", "route_source": "global"}}"#;

// -- Tests ------------------------------------------------------------------

#[tokio::test]
async fn capabilities_and_the_required_flags() {
    let (addr, seen) = serve(handler(|req| {
        assert_eq!(req.header("authorization"), Some(&*format!("Bearer {KEY}")));
        Response::json(200, CAPABILITIES)
    }))
    .await;
    let caps = client(addr, None).capabilities().await.unwrap();
    assert_eq!(caps.model, "hermes-agent");
    assert!(caps.auth.required);
    assert!(caps.missing().is_empty(), "{:?}", caps.missing());
    assert!(caps.has(STEER_FEATURE));
    assert_eq!(caps.endpoints["runs"]["path"], "/v1/runs");
    assert_eq!(seen.lock().unwrap()[0].path, "/v1/capabilities");

    let (addr, _) = serve(handler(|_| {
        Response::json(
            200,
            &CAPABILITIES
                .replace(r#""run_steer": true"#, r#""run_steer": false"#)
                .replace(r#""approval_events": true, "#, ""),
        )
    }))
    .await;
    let caps = client(addr, None).capabilities().await.unwrap();
    assert_eq!(caps.missing(), vec!["approval_events"]);
    assert!(!caps.has(STEER_FEATURE));
}

#[tokio::test]
async fn sessions_are_created_listed_and_read_under_a_profile() {
    let (addr, seen) = serve(handler(|req| match (req.method.as_str(), req.path.as_str()) {
        ("POST", "/p/coder/api/sessions") => {
            assert_eq!(req.json()["title"], "First chat");
            Response::json(
                201,
                &format!(r#"{{"object": "hermes.session", "session": {SESSION}}}"#),
            )
        }
        ("GET", "/p/coder/api/sessions?limit=5&source=api_server") => Response::json(
            200,
            &format!(
                r#"{{"object": "list", "data": [{SESSION}], "limit": 5, "offset": 0, "has_more": true}}"#
            ),
        ),
        ("GET", "/p/coder/api/sessions/api_1790400000_ab12cd34/messages?limit=50&order=oldest") => {
            Response::json(200, MESSAGES)
        }
        _ => panic!("unexpected {} {}", req.method, req.path),
    }))
    .await;
    let client = client(addr, Some("coder"));

    let session = client
        .create_session(&NewSession {
            title: Some("First chat".into()),
            ..NewSession::default()
        })
        .await
        .unwrap();
    assert_eq!(session.id, "api_1790400000_ab12cd34");
    assert_eq!(session.title.as_deref(), Some("First chat"));
    assert_eq!(session.message_count, Some(2));
    assert_eq!(session.preview.as_deref(), Some("Reply with exactly: pong"));
    assert!(!session.archived);

    let page = client
        .sessions(&SessionQuery {
            limit: Some(5),
            source: Some("api_server".into()),
            ..SessionQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(page.data, vec![session.clone()]);
    assert!(page.has_more);

    let messages = client
        .messages(
            &session.id,
            &MessageQuery {
                limit: Some(50),
                order: Some(Order::Oldest),
                ..MessageQuery::default()
            },
        )
        .await
        .unwrap();
    // The session rotated under compression: the page names the live id.
    assert_eq!(messages.session_id, "api_1790400000_ab12cd34_c2");
    assert_eq!(messages.pagination.returned, 2);
    assert_eq!(messages.data[0].role, "user");
    assert_eq!(messages.data[0].text(), "Reply with exactly: pong");
    assert_eq!(messages.data[1].text(), "pong");
    assert_eq!(messages.data[1].finish_reason.as_deref(), Some("stop"));
    assert!(seen.lock().unwrap().iter().all(|r| r.path.starts_with("/p/coder/")));
}

#[tokio::test]
async fn a_run_is_started_with_an_idempotency_key_and_polled() {
    let (addr, seen) = serve(handler(|req| match (req.method.as_str(), req.path.as_str()) {
        ("POST", "/v1/runs") => {
            let body = req.json();
            assert_eq!(body["input"], "Reply with exactly: pong");
            assert_eq!(body["session_id"], "api_1790400000_ab12cd34");
            assert!(body.get("idempotency_key").is_none());
            if req.header("idempotency-key") == Some("turn-1") {
                assert!(body.get("conversation_history").is_none());
                Response::json(
                    202,
                    r#"{"run_id": "run_9f1", "status": "started", "replayed": false}"#,
                )
            } else {
                assert_eq!(
                    body["conversation_history"],
                    serde_json::json!([{"role": "user", "content": "hi"}, {"role": "assistant", "content": "hello"}])
                );
                Response {
                    headers: vec![
                        ("Content-Type", "application/json".into()),
                        ("Idempotency-Replayed", "true".into()),
                    ],
                    ..Response::json(
                        202,
                        r#"{"run_id": "run_9f1", "status": "completed", "replayed": true}"#,
                    )
                }
            }
        }
        ("GET", "/v1/runs/run_9f1") => Response::json(200, RUN_STATUS_WAITING),
        _ => panic!("unexpected {} {}", req.method, req.path),
    }))
    .await;
    let client = client(addr, None);
    let run = NewRun {
        input: "Reply with exactly: pong".into(),
        session_id: Some("api_1790400000_ab12cd34".into()),
        conversation_history: None,
        idempotency_key: "turn-1".into(),
    };
    let started = client.start_run(&run).await.unwrap();
    assert_eq!(started.run_id, "run_9f1");
    assert_eq!(started.status, "started");
    assert!(!started.replayed);
    let replayed = client
        .start_run(&NewRun {
            conversation_history: Some(vec![
                HistoryMessage {
                    role: "user".into(),
                    content: "hi".into(),
                },
                HistoryMessage {
                    role: "assistant".into(),
                    content: "hello".into(),
                },
            ]),
            idempotency_key: "turn-1-retry".into(),
            ..run
        })
        .await
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.status, "completed");
    assert_eq!(
        seen.lock().unwrap()[0].header("idempotency-key"),
        Some("turn-1")
    );

    let status = client.run("run_9f1").await.unwrap();
    assert_eq!(status.status, RunState::WaitingForApproval);
    assert!(!status.status.is_terminal());
    // The run adopted the live session id (`_resolve_live_session_id`).
    assert_eq!(
        status.session_id.as_deref(),
        Some("api_1790400000_ab12cd34_c2")
    );
    let approval = status.approval.unwrap();
    assert_eq!(
        approval.request_id.as_deref(),
        Some("8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f")
    );
    assert_eq!(approval.command.as_deref(), Some("rm -rf ./scratch"));
    assert_eq!(
        approval.choices,
        vec![Choice::Once, Choice::Session, Choice::Always, Choice::Deny]
    );
}

#[tokio::test]
async fn every_event_kind_is_typed() {
    let frames = [
        ": open\n\n".to_owned(),
        frame(0, r#"{"event": "tool.started", "run_id": "run_9f1", "timestamp": 1790400011.1, "tool": "terminal", "preview": "{\"command\": \"ls\"}", "seq": 0}"#),
        ": keepalive\n\n".to_owned(),
        frame(1, r#"{"event": "tool.completed", "run_id": "run_9f1", "timestamp": 1790400011.9, "tool": "terminal", "duration": 0.812, "error": false, "preview": "Cargo.toml\nsrc", "seq": 1}"#),
        frame(2, r#"{"event": "reasoning.available", "run_id": "run_9f1", "timestamp": 1790400012.0, "text": "The user wants pong.", "seq": 2}"#),
        frame(3, r#"{"event": "approval.request", "run_id": "run_9f1", "timestamp": 1790400012.0, "command": "rm -rf ./scratch", "pattern_key": "rm -rf", "pattern_keys": ["rm -rf"], "description": "Recursive delete", "allow_permanent": true, "allow_session": true, "request_id": "8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f", "choices": ["once", "session", "always", "deny"], "seq": 3}"#),
        frame(4, r#"{"event": "approval.responded", "run_id": "run_9f1", "timestamp": 1790400013.0, "choice": "deny", "request_id": "8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f", "resolved": 1, "seq": 4}"#),
        frame(5, r#"{"event": "run.steered", "run_id": "run_9f1", "timestamp": 1790400013.5, "accepted": true, "seq": 5}"#),
        frame(6, r#"{"event": "message.interim", "run_id": "run_9f1", "timestamp": 1790400014.0, "text": "Checking.", "already_streamed": false, "seq": 6}"#),
        frame(7, r#"{"event": "subagent.start", "run_id": "run_9f1", "timestamp": 1790400014.5, "goal": "lookup", "task_count": 1, "seq": 7}"#),
        frame(8, r#"{"event": "subagent.complete", "run_id": "run_9f1", "timestamp": 1790400015.0, "status": "completed", "summary": "done", "duration_seconds": 0.5, "seq": 8}"#),
        frame(9, r#"{"event": "message.delta", "run_id": "run_9f1", "timestamp": 1790400015.5, "delta": "po", "seq": 9}"#),
        frame(10, r#"{"event": "message.delta", "run_id": "run_9f1", "timestamp": 1790400015.6, "delta": "ng", "seq": 10}"#),
        frame(11, r#"{"event": "run.completed", "run_id": "run_9f1", "timestamp": 1790400016.0, "completed": true, "partial": false, "interrupted": false, "output": "pong", "usage": {"input_tokens": 1520, "output_tokens": 4, "total_tokens": 1524, "cache_read_tokens": 0, "cache_write_tokens": 0}, "runtime": {"provider": "neboai", "model": "nebo-1", "route_source": "global"}, "seq": 11}"#),
        ": stream closed\n\n".to_owned(),
    ];
    let (addr, seen) = serve(handler(move |req| {
        assert_eq!(req.path, "/v1/runs/run_9f1/events");
        assert_eq!(req.header("accept"), Some("text/event-stream"));
        assert_eq!(req.header("last-event-id"), None);
        Response::sse(&frames.iter().map(String::as_str).collect::<Vec<_>>())
    }))
    .await;
    let mut stream = client(addr, None).events("run_9f1", None).await.unwrap();
    let mut events = Vec::new();
    while let Some(envelope) = stream.next().await {
        let envelope = envelope.unwrap();
        assert_eq!(envelope.run_id, "run_9f1");
        assert_eq!(envelope.seq, Some(events.len() as u64));
        events.push(envelope.event);
    }
    assert_eq!(stream.last_seq(), Some(11));
    assert_eq!(seen.lock().unwrap().len(), 1, "one connection");
    assert_eq!(events.len(), 12);
    assert_eq!(
        events[0],
        Event::ToolStarted {
            tool: "terminal".into(),
            preview: "{\"command\": \"ls\"}".into()
        }
    );
    assert_eq!(
        events[1],
        Event::ToolCompleted {
            tool: "terminal".into(),
            duration: 0.812,
            error: false,
            preview: "Cargo.toml\nsrc".into()
        }
    );
    assert_eq!(
        events[2],
        Event::Reasoning {
            text: "The user wants pong.".into()
        }
    );
    let Event::ApprovalRequest(request) = &events[3] else {
        panic!("{:?}", events[3]);
    };
    assert_eq!(
        request.request_id.as_deref(),
        Some("8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f")
    );
    assert_eq!(request.description.as_deref(), Some("Recursive delete"));
    assert_eq!(request.pattern_key.as_deref(), Some("rm -rf"));
    assert!(!request.smart_denied);
    assert_eq!(request.choices.len(), 4);
    assert_eq!(
        events[4],
        Event::ApprovalResponded {
            choice: Choice::Deny,
            request_id: Some("8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f".into()),
            resolved: 1
        }
    );
    assert_eq!(events[5], Event::Steered { accepted: true });
    assert_eq!(
        events[6],
        Event::MessageInterim {
            text: "Checking.".into(),
            already_streamed: false
        }
    );
    let Event::Subagent { started: true, fields } = &events[7] else {
        panic!("{:?}", events[7]);
    };
    assert_eq!(fields["goal"], "lookup");
    let Event::Subagent { started: false, fields } = &events[8] else {
        panic!("{:?}", events[8]);
    };
    assert_eq!(fields["status"], "completed");
    assert_eq!(events[9], Event::MessageDelta { delta: "po".into() });
    assert_eq!(events[10], Event::MessageDelta { delta: "ng".into() });
    let Event::Completed(outcome) = &events[11] else {
        panic!("{:?}", events[11]);
    };
    assert!(events[11].is_terminal());
    assert_eq!(outcome.output.as_deref(), Some("pong"));
    assert!(outcome.completed && !outcome.partial && !outcome.interrupted);
    let usage = outcome.usage.unwrap();
    assert_eq!((usage.input_tokens, usage.output_tokens, usage.total_tokens), (1520, 4, 1524));
    assert_eq!(outcome.runtime.as_ref().unwrap().model, "nebo-1");
}

#[tokio::test]
async fn failed_cancelled_and_interrupted_runs() {
    let failed = frame(0, r#"{"event": "run.failed", "run_id": "run_a", "timestamp": 1.0, "completed": false, "partial": false, "interrupted": false, "turn_exit_reason": "max_iterations", "error": "agent run failed", "seq": 0}"#);
    let cancelled = frame(3, r#"{"event": "run.cancelled", "run_id": "run_b", "timestamp": 2.0, "completed": false, "partial": true, "interrupted": true, "pending_steer": "and then", "seq": 3}"#);
    let interrupted = frame(0, r#"{"event": "run.interrupted", "run_id": "run_c", "timestamp": 3.0, "error": "Gateway shutdown interrupted the run.", "seq": 0}"#);
    let (addr, _) = serve(handler(move |req| {
        let body = match req.path.as_str() {
            "/v1/runs/run_a/events" => &failed,
            "/v1/runs/run_b/events" => &cancelled,
            "/v1/runs/run_c/events" => &interrupted,
            other => panic!("{other}"),
        };
        Response::sse(&[": open\n\n", body, ": stream closed\n\n"])
    }))
    .await;
    let client = client(addr, None);
    let mut events = Vec::new();
    for run in ["run_a", "run_b", "run_c"] {
        let mut stream = client.events(run, None).await.unwrap();
        while let Some(envelope) = stream.next().await {
            events.push(envelope.unwrap().event);
        }
    }
    let [Event::Failed(failed), Event::Cancelled(cancelled), Event::Interrupted(interrupted)] =
        events.as_slice()
    else {
        panic!("{events:?}");
    };
    assert_eq!(failed.error.as_deref(), Some("agent run failed"));
    assert_eq!(failed.turn_exit_reason.as_deref(), Some("max_iterations"));
    assert!(cancelled.interrupted && cancelled.partial);
    assert_eq!(cancelled.pending_steer, Some("and then".into()));
    assert_eq!(
        interrupted.error.as_deref(),
        Some("Gateway shutdown interrupted the run.")
    );
}

#[tokio::test]
async fn a_cut_stream_resumes_from_the_last_event_id() {
    let first = [
        ": open\n\n".to_owned(),
        frame(0, r#"{"event": "message.delta", "run_id": "run_9f1", "timestamp": 1.0, "delta": "po", "seq": 0}"#),
        frame(1, r#"{"event": "message.delta", "run_id": "run_9f1", "timestamp": 1.1, "delta": "n", "seq": 1}"#),
    ];
    let second = [
        ": open\n\n".to_owned(),
        // The server keeps 1000 events; here it pretends 2 fell off.
        "data: {\"event\": \"replay.truncated\", \"run_id\": \"run_9f1\", \"timestamp\": 1.5, \"oldest_retained_seq\": 3, \"requested_seq\": 1}\n\n".to_owned(),
        frame(3, r#"{"event": "message.delta", "run_id": "run_9f1", "timestamp": 1.2, "delta": "g", "seq": 3}"#),
        frame(4, r#"{"event": "run.completed", "run_id": "run_9f1", "timestamp": 2.0, "completed": true, "partial": false, "interrupted": false, "output": "pong", "usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12, "cache_read_tokens": 0, "cache_write_tokens": 0}, "runtime": {"provider": "neboai", "model": "nebo-1", "route_source": "global"}, "seq": 4}"#),
        ": stream closed\n\n".to_owned(),
    ];
    let (addr, seen) = serve(handler(move |req| match req.header("last-event-id") {
        None => Response {
            cut: true,
            ..Response::sse(&first.iter().map(String::as_str).collect::<Vec<_>>())
        },
        Some("1") => Response::sse(&second.iter().map(String::as_str).collect::<Vec<_>>()),
        other => panic!("Last-Event-ID {other:?}"),
    }))
    .await;
    let mut stream = client(addr, None).events("run_9f1", None).await.unwrap();
    let mut text = String::new();
    let mut seqs = Vec::new();
    let mut truncated = None;
    let mut ended = false;
    while let Some(envelope) = stream.next().await {
        let envelope = envelope.unwrap();
        seqs.push(envelope.seq);
        match envelope.event {
            Event::MessageDelta { delta } => text.push_str(&delta),
            Event::ReplayTruncated {
                oldest_retained_seq,
                requested_seq,
            } => truncated = Some((oldest_retained_seq, requested_seq)),
            Event::Completed(_) => ended = true,
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(text, "pong");
    assert_eq!(seqs, vec![Some(0), Some(1), None, Some(3), Some(4)]);
    assert_eq!(truncated, Some((3, Some(1))));
    assert!(ended);
    assert_eq!(stream.last_seq(), Some(4));
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[1].header("last-event-id"), Some("1"));
}

#[tokio::test]
async fn a_stream_that_keeps_dropping_is_reported() {
    let (addr, seen) = serve(handler(|_| Response {
        cut: true,
        ..Response::sse(&[": open\n\n"])
    }))
    .await;
    let mut stream = client(addr, None).events("run_9f1", None).await.unwrap();
    let error = stream.next().await.unwrap().unwrap_err();
    assert!(
        matches!(error, Error::StreamLost { attempts: 3, .. }),
        "{error}"
    );
    assert!(stream.next().await.is_none());
    assert_eq!(seen.lock().unwrap().len(), 4, "one open and three reopens");
}

#[tokio::test]
async fn approval_steer_and_stop() {
    let (addr, seen) = serve(handler(|req| match (req.method.as_str(), req.path.as_str()) {
        ("POST", "/v1/runs/run_9f1/approval") => {
            assert_eq!(
                req.json(),
                serde_json::json!({"choice": "once", "request_id": "8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f"})
            );
            Response::json(200, r#"{"object": "hermes.run.approval_response", "run_id": "run_9f1", "choice": "once", "request_id": "8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f", "resolved": 1}"#)
        }
        ("POST", "/v1/runs/run_9f1/steer") => {
            assert_eq!(req.json(), serde_json::json!({"input": "shorter please"}));
            Response::json(200, r#"{"object": "hermes.run.steer", "run_id": "run_9f1", "accepted": true}"#)
        }
        ("POST", "/v1/runs/run_9f1/stop") => {
            Response::json(200, r#"{"run_id": "run_9f1", "status": "stopping"}"#)
        }
        ("POST", "/v1/runs/run_done/stop") => Response::json(200, RUN_STATUS_DONE),
        _ => panic!("unexpected {} {}", req.method, req.path),
    }))
    .await;
    let client = client(addr, None);
    let outcome = client
        .approve("run_9f1", Choice::Once, Some("8c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f"))
        .await
        .unwrap();
    assert_eq!(outcome.choice, Choice::Once);
    assert_eq!(outcome.resolved, 1);
    client.steer("run_9f1", "shorter please").await.unwrap();
    assert!(matches!(
        client.stop("run_9f1").await.unwrap(),
        StopOutcome::Stopping
    ));
    let StopOutcome::Ended(status) = client.stop("run_done").await.unwrap() else {
        panic!("expected the terminal status");
    };
    assert_eq!(status.status, RunState::Completed);
    assert_eq!(status.output.as_deref(), Some("pong"));
    assert_eq!(status.usage.unwrap().total_tokens, 1524);
    assert_eq!(status.runtime.unwrap().provider, "neboai");
    assert_eq!(seen.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn errors_carry_the_status_and_code() {
    let (addr, _) = serve(handler(|req| match req.path.as_str() {
        "/v1/runs/gone" => Response::json(404, r#"{"error": {"message": "Run not found: gone", "type": "invalid_request_error", "param": null, "code": "run_not_found"}}"#),
        "/v1/runs/run_9f1/approval" => Response::json(409, r#"{"error": {"message": "Run has no pending approval: run_9f1", "type": "invalid_request_error", "param": null, "code": "approval_not_pending"}}"#),
        "/v1/capabilities" => Response::json(401, r#"{"error": {"message": "Invalid gateway API key (API_SERVER_KEY)", "type": "gateway_auth_error", "code": "gateway_auth_failed"}}"#),
        "/p/ghost/v1/capabilities" => Response::json(404, r#"{"error": "Unknown or unconfigured profile"}"#),
        "/v1/runs/run_9f1/events" => Response::json(404, r#"{"error": {"message": "Run not found: run_9f1", "type": "invalid_request_error", "param": null, "code": "run_not_found"}}"#),
        other => panic!("{other}"),
    }))
    .await;
    let client = client(addr, None);
    let error = client.run("gone").await.unwrap_err();
    assert_eq!(error.status(), Some(404));
    assert_eq!(error.code(), Some("run_not_found"));
    assert_eq!(
        error.to_string(),
        "GET /v1/runs/gone: HTTP 404 (run_not_found): Run not found: gone"
    );
    let error = client.approve("run_9f1", Choice::Deny, None).await.unwrap_err();
    assert_eq!(error.code(), Some("approval_not_pending"));
    let error = client.capabilities().await.unwrap_err();
    assert_eq!(error.status(), Some(401));
    assert_eq!(error.code(), Some("gateway_auth_failed"));
    let error = Client::new(&format!("http://{addr}"), Some("ghost"), KEY)
        .capabilities()
        .await
        .unwrap_err();
    assert_eq!(error.status(), Some(404));
    assert_eq!(error.code(), None);
    assert!(error.to_string().ends_with("Unknown or unconfigured profile"));
    let error = client.events("run_9f1", Some(7)).await.unwrap_err();
    assert_eq!(error.code(), Some("run_not_found"));

    let unreachable = Client::new("http://127.0.0.1:1", None, KEY);
    assert!(matches!(
        unreachable.capabilities().await.unwrap_err(),
        Error::Transport { .. }
    ));
}

/// Against a live API server: `HERMES_API_URL` (`http://127.0.0.1:8642`) and
/// `HERMES_API_KEY` (the profile's `API_SERVER_KEY`) select it;
/// `HERMES_API_PROFILE` names a profile under `/p/`. Three short turns on
/// one new session: an answer over runs + SSE, an approval round trip
/// (denied, so nothing runs), and a `/stop` mid-turn. Costs real model
/// calls.
#[tokio::test]
#[ignore = "needs HERMES_API_URL and HERMES_API_KEY"]
async fn live_turn_approval_and_stop() {
    let (Ok(url), Ok(key)) = (std::env::var("HERMES_API_URL"), std::env::var("HERMES_API_KEY"))
    else {
        eprintln!("HERMES_API_URL / HERMES_API_KEY not set; nothing to do");
        return;
    };
    let profile = std::env::var("HERMES_API_PROFILE").ok();
    let client = Client::new(&url, profile.as_deref(), &key);

    let caps = client.capabilities().await.unwrap();
    eprintln!(
        "capabilities: model={} missing={:?} steer={}",
        caps.model,
        caps.missing(),
        caps.has(STEER_FEATURE)
    );
    assert!(caps.missing().is_empty(), "missing flags {:?}", caps.missing());

    let session = client
        .create_session(&NewSession {
            title: Some(format!("nebo-runtimes live {}", std::process::id())),
            ..NewSession::default()
        })
        .await
        .unwrap();
    eprintln!("session {}", session.id);
    let turn = |n: u32| format!("nebo-runtimes-live-{}-{n}", std::process::id());

    // 1. A turn completes over runs + SSE.
    let started = client
        .start_run(&NewRun {
            input: "Reply with exactly the word: pong".into(),
            session_id: Some(session.id.clone()),
            conversation_history: None,
            idempotency_key: turn(1),
        })
        .await
        .unwrap();
    assert!(!started.replayed);
    let mut stream = client.events(&started.run_id, None).await.unwrap();
    let mut text = String::new();
    let mut outcome = None;
    while let Some(envelope) = stream.next().await {
        let envelope = envelope.unwrap();
        eprintln!("run 1 seq={:?} {:?}", envelope.seq, envelope.event);
        match envelope.event {
            Event::MessageDelta { delta } => text.push_str(&delta),
            Event::Completed(done) => outcome = Some(done),
            _ => {}
        }
    }
    let outcome = outcome.expect("run.completed");
    assert!(text.to_lowercase().contains("pong"), "streamed {text:?}");
    let usage1 = outcome.usage.expect("usage on run.completed");
    let status = client.run(&started.run_id).await.unwrap();
    assert_eq!(status.status, RunState::Completed);
    assert_eq!(status.session_id.as_deref(), Some(session.id.as_str()));
    eprintln!("run 1 usage {usage1:?}");

    // 2. An approval round trip. Hermes' default `approvals.mode: smart`
    // asks the owner only when its guardian model refuses a flagged command,
    // so the command must look dangerous; the answer here is `deny`, and as
    // a non-root user it would fail anyway.
    let started = client
        .start_run(&NewRun {
            input: "Using the terminal tool, run exactly this command and report its output: chmod -R 777 /etc".into(),
            session_id: Some(session.id.clone()),
            conversation_history: None,
            idempotency_key: turn(2),
        })
        .await
        .unwrap();
    let mut stream = client.events(&started.run_id, None).await.unwrap();
    let mut asked = None;
    let mut responded = false;
    let mut outcome = None;
    while let Some(envelope) = stream.next().await {
        let envelope = envelope.unwrap();
        eprintln!("run 2 seq={:?} {:?}", envelope.seq, envelope.event);
        match envelope.event {
            Event::ApprovalRequest(request) => {
                let status = client.run(&started.run_id).await.unwrap();
                assert_eq!(status.status, RunState::WaitingForApproval);
                let answer = client
                    .approve(&started.run_id, Choice::Deny, request.request_id.as_deref())
                    .await
                    .unwrap();
                assert_eq!(answer.resolved, 1);
                asked = Some(request);
            }
            Event::ApprovalResponded { choice, .. } => responded = choice == Choice::Deny,
            Event::Completed(done) | Event::Failed(done) => outcome = Some(done),
            _ => {}
        }
    }
    let asked = asked.expect("approval.request");
    assert!(asked.choices.contains(&Choice::Deny));
    assert!(responded, "approval.responded");
    let usage2 = outcome.expect("terminal event").usage.expect("usage");
    eprintln!("run 2 usage {usage2:?} (run 1 was {usage1:?})");

    // Usage is the run's own, not the session's running total: a short
    // third turn reports far less than the two before it together.
    let started = client
        .start_run(&NewRun {
            input: "Reply with exactly the word: ok".into(),
            session_id: Some(session.id.clone()),
            conversation_history: None,
            idempotency_key: turn(4),
        })
        .await
        .unwrap();
    let mut stream = client.events(&started.run_id, None).await.unwrap();
    let mut usage3 = None;
    while let Some(envelope) = stream.next().await {
        if let Event::Completed(done) = envelope.unwrap().event {
            usage3 = done.usage;
        }
    }
    let usage3 = usage3.expect("usage on run 3");
    eprintln!("run 3 usage {usage3:?}");
    assert!(
        usage3.input_tokens < usage1.input_tokens + usage2.input_tokens,
        "usage is per run: {usage3:?} after {usage1:?} + {usage2:?}"
    );

    // 4. `/stop` ends a run.
    let started = client
        .start_run(&NewRun {
            input: "Count from 1 to 500, one number per line, no other text.".into(),
            session_id: Some(session.id.clone()),
            conversation_history: None,
            idempotency_key: turn(3),
        })
        .await
        .unwrap();
    let mut stream = client.events(&started.run_id, None).await.unwrap();
    let mut stopped = false;
    let mut ended = None;
    while let Some(envelope) = stream.next().await {
        let envelope = envelope.unwrap();
        eprintln!("run 3 seq={:?} {:?}", envelope.seq, envelope.event);
        match envelope.event {
            Event::MessageDelta { .. } | Event::ToolStarted { .. } if !stopped => {
                assert!(matches!(
                    client.stop(&started.run_id).await.unwrap(),
                    StopOutcome::Stopping
                ));
                stopped = true;
            }
            event if event.is_terminal() => ended = Some(event),
            _ => {}
        }
    }
    assert!(stopped, "no event arrived to stop on");
    assert!(matches!(ended, Some(Event::Cancelled(_))), "{ended:?}");
    let status = client.run(&started.run_id).await.unwrap();
    assert_eq!(status.status, RunState::Cancelled);

    let page = client
        .messages(&session.id, &MessageQuery::default())
        .await
        .unwrap();
    eprintln!(
        "session {} holds {} messages",
        page.session_id,
        page.data.len()
    );
    assert!(page.data.iter().any(|m| m.text().contains("pong")));
}
