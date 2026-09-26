//! A JSON-RPC 2.0 connection over newline-delimited JSON, the ACP stdio
//! transport: requests with ids both ways, and notifications.
//!
//! What the agent sends (its notifications and its requests) is handed to one
//! handler, inline and in the order it was written. A response resolves its
//! request only after everything the agent wrote before it has been handled,
//! so a caller that sees `session/prompt` return has already seen every
//! `session/update` of that turn.

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::Child;
use tokio::sync::{Notify, mpsc, oneshot};

use crate::RuntimeCommand;

/// JSON-RPC error: "Authentication required" (ACP `ErrorCode`).
pub const AUTH_REQUIRED: i64 = -32000;
/// JSON-RPC error: "Resource not found" (ACP `ErrorCode`), e.g. an unknown
/// session.
pub const NOT_FOUND: i64 = -32002;
/// JSON-RPC error: "Method not found".
pub const METHOD_NOT_FOUND: i64 = -32601;
/// Not a JSON-RPC code: the agent's process ended before it answered.
pub const CLOSED: i64 = -1;

/// An error answer.
#[derive(Debug, Clone, PartialEq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn closed() -> Self {
        Self::new(CLOSED, "the agent stopped")
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

/// Something the agent sent that is not a response.
#[derive(Debug, Clone, PartialEq)]
pub enum Incoming {
    Notification {
        method: String,
        params: Value,
    },
    /// Answer it with [`Responder::respond`] and this `id`.
    Request {
        id: Value,
        method: String,
        params: Value,
    },
}

/// Answers the agent's requests; cheap to clone.
#[derive(Clone)]
pub struct Responder(mpsc::UnboundedSender<String>);

impl Responder {
    pub fn respond(&self, id: &Value, result: Result<Value, RpcError>) {
        let frame = match result {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err(e) => {
                json!({ "jsonrpc": "2.0", "id": id, "error": { "code": e.code, "message": e.message } })
            }
        };
        let _ = self.0.send(frame.to_string());
    }
}

/// Handles what the agent sends. Runs on the connection's reader: it must not
/// block, and whatever it needs to do later it hands off.
pub type Handler = Box<dyn Fn(Incoming, &Responder) + Send + Sync>;

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>>>;

/// One JSON-RPC peer.
pub struct Connection {
    out: Responder,
    pending: Pending,
    next_id: AtomicU64,
    closed: Arc<AtomicBool>,
    closed_notify: Arc<Notify>,
}

impl Connection {
    /// Speaks JSON-RPC on `reader`/`writer` until either ends.
    pub fn start<R, W>(reader: R, writer: W, handler: Handler) -> Arc<Self>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let out = Responder(tx);
        let pending: Pending = Arc::default();
        let closed = Arc::new(AtomicBool::new(false));
        let closed_notify = Arc::new(Notify::new());

        let mut writer = writer;
        tokio::spawn(async move {
            while let Some(line) = rx.recv().await {
                let written = async {
                    writer.write_all(line.as_bytes()).await?;
                    writer.write_all(b"\n").await?;
                    writer.flush().await
                };
                if written.await.is_err() {
                    break;
                }
            }
        });

        let (reader_pending, reader_closed, reader_notify, responder) = (
            pending.clone(),
            closed.clone(),
            closed_notify.clone(),
            out.clone(),
        );
        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                let Ok(message) = serde_json::from_str::<Value>(&line) else {
                    // An agent that logs to stdout; not ours to read.
                    continue;
                };
                match (
                    message.get("method").and_then(Value::as_str),
                    message.get("id"),
                ) {
                    (Some(method), Some(id)) => handler(
                        Incoming::Request {
                            id: id.clone(),
                            method: method.to_owned(),
                            params: message.get("params").cloned().unwrap_or(Value::Null),
                        },
                        &responder,
                    ),
                    (Some(method), None) => handler(
                        Incoming::Notification {
                            method: method.to_owned(),
                            params: message.get("params").cloned().unwrap_or(Value::Null),
                        },
                        &responder,
                    ),
                    (None, Some(id)) => {
                        let Some(id) = id.as_u64() else { continue };
                        let result = match message.get("error") {
                            Some(error) => Err(RpcError::new(
                                error.get("code").and_then(Value::as_i64).unwrap_or(0),
                                error
                                    .get("message")
                                    .and_then(Value::as_str)
                                    .unwrap_or("")
                                    .to_owned(),
                            )),
                            None => Ok(message.get("result").cloned().unwrap_or(Value::Null)),
                        };
                        if let Some(waiter) = reader_pending.lock().expect("pending").remove(&id) {
                            let _ = waiter.send(result);
                        }
                    }
                    (None, None) => {}
                }
            }
            reader_closed.store(true, Ordering::SeqCst);
            for (_, waiter) in reader_pending.lock().expect("pending").drain() {
                let _ = waiter.send(Err(RpcError::closed()));
            }
            reader_notify.notify_waiters();
        });

        Arc::new(Self {
            out,
            pending,
            next_id: AtomicU64::new(1),
            closed,
            closed_notify,
        })
    }

    /// Sends a request and waits for its answer; [`CLOSED`] when the agent
    /// ends first.
    pub async fn request(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().expect("pending").insert(id, tx);
        if self.is_closed() {
            self.pending.lock().expect("pending").remove(&id);
            return Err(RpcError::closed());
        }
        let _ = self.out.0.send(
            json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string(),
        );
        rx.await.unwrap_or_else(|_| Err(RpcError::closed()))
    }

    pub fn notify(&self, method: &str, params: Value) {
        let _ = self
            .out
            .0
            .send(json!({ "jsonrpc": "2.0", "method": method, "params": params }).to_string());
    }

    pub fn responder(&self) -> Responder {
        self.out.clone()
    }

    /// Whether the agent's side has ended.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Resolves once the agent's side has ended.
    pub async fn closed(&self) {
        let notified = self.closed_notify.notified();
        if self.is_closed() {
            return;
        }
        notified.await;
    }
}

/// Starts `command` in `cwd` as an ACP agent on its stdin/stdout. Its stderr
/// goes to `stderr` (a log file). The child is killed when dropped.
pub fn spawn(
    command: &RuntimeCommand,
    cwd: &Path,
    stderr: Stdio,
    handler: Handler,
) -> std::io::Result<(Child, Arc<Connection>)> {
    let mut cmd = tokio::process::Command::new(&command.program);
    cmd.args(&command.args)
        .envs(command.env.iter().cloned())
        .current_dir(cwd)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(stderr)
        .kill_on_drop(true);
    let mut child = cmd.spawn()?;
    let stdin = child.stdin.take().expect("piped stdin");
    let stdout = child.stdout.take().expect("piped stdout");
    Ok((child, Connection::start(stdout, stdin, handler)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn requests_both_ways_and_notifications_before_the_answer() {
        let (client_io, agent_io) = tokio::io::duplex(1 << 16);
        let (client_read, client_write) = tokio::io::split(client_io);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        let conn = Connection::start(
            client_read,
            client_write,
            Box::new(move |incoming, responder| {
                if let Incoming::Request { id, .. } = &incoming {
                    responder.respond(id, Ok(json!({ "answered": true })));
                }
                log.lock().unwrap().push(incoming);
            }),
        );

        let agent = tokio::spawn(async move {
            let (read, mut write) = tokio::io::split(agent_io);
            let mut lines = BufReader::new(read).lines();
            let request: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(request["method"], "session/prompt");
            let frames = [
                json!({ "jsonrpc": "2.0", "method": "session/update", "params": { "n": 1 } }),
                json!({ "jsonrpc": "2.0", "id": 0, "method": "session/request_permission", "params": {} }),
            ];
            for frame in frames {
                write
                    .write_all(format!("{frame}\nnot json\n").as_bytes())
                    .await
                    .unwrap();
            }
            let answer: Value =
                serde_json::from_str(&lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(answer["id"], 0);
            assert_eq!(answer["result"]["answered"], true);
            let reply = json!({ "jsonrpc": "2.0", "id": request["id"], "result": { "stopReason": "end_turn" } });
            write
                .write_all(format!("{reply}\n").as_bytes())
                .await
                .unwrap();
            write
        });

        let result = conn.request("session/prompt", json!({})).await.unwrap();
        assert_eq!(result["stopReason"], "end_turn");
        assert_eq!(
            seen.lock().unwrap().len(),
            2,
            "both frames handled before the answer"
        );

        let err = conn.request("session/load", json!({}));
        drop(agent.await.unwrap());
        assert_eq!(err.await.unwrap_err().code, CLOSED);
        conn.closed().await;
        assert!(conn.is_closed());
    }
}
