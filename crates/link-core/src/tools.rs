//! The host's own tools, which it gives every coding agent's session: an MCP
//! server of the host's (never a client's), served on loopback over MCP's
//! Streamable HTTP, one secret URL per conversation.
//!
//! - **`move_to_folder`** moves the conversation to another folder when the
//!   owner asks the agent to work there ([`crate::host::Host::move_to_folder`]).
//!
//! An agent is given the server in its `session/new`, `session/load` and
//! `session/resume` as `{"type": "http", "name": "host", "url": …}` when it
//! can take an HTTP MCP server (`mcpCapabilities.http`). The URL's token
//! names the conversation, so a call can only act on the conversation it
//! was given to; nothing but this computer reaches the port.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, Weak};

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use serde_json::{Value, json};

use crate::host::Host;

/// The MCP server's name, as an agent lists its tools
/// (`mcp__host__move_to_folder`).
pub const SERVER: &str = "host";
/// The tool that moves a conversation to another folder.
pub const MOVE_TO_FOLDER: &str = "move_to_folder";
/// The MCP versions the server speaks, newest first.
const VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];
/// The largest request body taken.
const MAX_BODY: usize = 1 << 20;

/// What a token names: an agent's conversation, or an agent whose new
/// conversation's id isn't known yet.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Named {
    agent: String,
    session: Option<String>,
}

/// The host's MCP server.
pub(crate) struct Tools {
    addr: SocketAddr,
    tokens: Mutex<HashMap<String, Named>>,
}

impl Tools {
    /// Serves the host's tools on a loopback port.
    pub(crate) async fn start(host: Weak<Host>) -> std::io::Result<Arc<Self>> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let tools = Arc::new(Self {
            addr: listener.local_addr()?,
            tokens: Mutex::new(HashMap::new()),
        });
        let serving = Arc::downgrade(&tools);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { continue };
                let (host, tools) = (host.clone(), serving.clone());
                if tools.strong_count() == 0 {
                    return;
                }
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(move |req| serve(host.clone(), tools.clone(), req));
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        tracing::info!(addr = %tools.addr, "host tools are served");
        Ok(tools)
    }

    /// The server as ACP's `McpServer`, for the conversation `token` names.
    pub(crate) fn entry(&self, token: &str) -> Value {
        json!({ "type": "http", "name": SERVER, "url": format!("http://{}/mcp/{token}", self.addr), "headers": [] })
    }

    /// The token for `agent`'s conversation `session` (the one it already
    /// has), or for a conversation about to be made (`None`).
    pub(crate) fn token(&self, agent: &str, session: Option<&str>) -> String {
        let named = Named {
            agent: agent.to_owned(),
            session: session.map(str::to_owned),
        };
        let mut tokens = self.tokens.lock().expect("tokens");
        if named.session.is_some()
            && let Some((token, _)) = tokens.iter().find(|(_, n)| **n == named)
        {
            return token.clone();
        }
        let token = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
        tokens.insert(token.clone(), named);
        token
    }

    /// The conversation made with `token` is `session`.
    pub(crate) fn bind(&self, token: &str, session: &str) {
        if let Some(named) = self.tokens.lock().expect("tokens").get_mut(token) {
            named.session = Some(session.to_owned());
        }
    }

    /// Forgets `token`: its conversation wasn't made, or is gone.
    pub(crate) fn forget(&self, token: &str) {
        self.tokens.lock().expect("tokens").remove(token);
    }

    /// Forgets every token of `agent`'s conversation `session`, or of every
    /// conversation of `agent`.
    pub(crate) fn forget_session(&self, agent: &str, session: Option<&str>) {
        self.tokens
            .lock()
            .expect("tokens")
            .retain(|_, n| !(n.agent == agent && (session.is_none() || n.session.as_deref() == session)));
    }

    fn named(&self, token: &str) -> Option<Named> {
        self.tokens.lock().expect("tokens").get(token).cloned()
    }
}

type Body = Full<Bytes>;

fn reply(status: StatusCode, body: Option<Value>) -> Response<Body> {
    let mut response = Response::builder().status(status);
    if body.is_some() {
        response = response.header(hyper::header::CONTENT_TYPE, "application/json");
    }
    if status == StatusCode::METHOD_NOT_ALLOWED {
        response = response.header(hyper::header::ALLOW, "POST");
    }
    let bytes = body.map(|b| Bytes::from(b.to_string())).unwrap_or_default();
    response.body(Full::new(bytes)).expect("a response")
}

async fn serve(host: Weak<Host>, tools: Weak<Tools>, req: Request<Incoming>) -> Result<Response<Body>, Infallible> {
    let token = req.uri().path().strip_prefix("/mcp/").unwrap_or("").to_owned();
    let (Some(host), Some(tools)) = (host.upgrade(), tools.upgrade()) else {
        return Ok(reply(StatusCode::SERVICE_UNAVAILABLE, None));
    };
    let Some(named) = tools.named(&token) else {
        return Ok(reply(StatusCode::NOT_FOUND, None));
    };
    // No stream from the server: every answer comes back on its request.
    if req.method() != Method::POST {
        return Ok(reply(StatusCode::METHOD_NOT_ALLOWED, None));
    }
    let Ok(body) = Limited::new(req.into_body(), MAX_BODY).collect().await else {
        return Ok(reply(StatusCode::PAYLOAD_TOO_LARGE, None));
    };
    let Ok(message) = serde_json::from_slice::<Value>(&body.to_bytes()) else {
        let e = json!({ "jsonrpc": "2.0", "id": null, "error": { "code": -32700, "message": "That isn't JSON." } });
        return Ok(reply(StatusCode::BAD_REQUEST, Some(e)));
    };
    let answers: Vec<Value> = match &message {
        Value::Array(batch) => {
            let mut answers = Vec::new();
            for one in batch {
                answers.extend(answer(&host, &named, one).await);
            }
            answers
        }
        one => answer(&host, &named, one).await.into_iter().collect(),
    };
    Ok(match (message.is_array(), answers.len()) {
        (_, 0) => reply(StatusCode::ACCEPTED, None),
        (false, _) => reply(StatusCode::OK, answers.into_iter().next()),
        (true, _) => reply(StatusCode::OK, Some(Value::Array(answers))),
    })
}

/// The answer to one JSON-RPC message; none for a notification.
async fn answer(host: &Arc<Host>, named: &Named, message: &Value) -> Option<Value> {
    let id = message.get("id")?.clone();
    let method = message["method"].as_str().unwrap_or("");
    let params = &message["params"];
    let result = match method {
        "initialize" => {
            let asked = params["protocolVersion"].as_str().unwrap_or("");
            let version = VERSIONS.iter().find(|v| **v == asked).unwrap_or(&VERSIONS[0]);
            Ok(json!({
                "protocolVersion": version,
                "capabilities": { "tools": { "listChanged": false } },
                "serverInfo": { "name": SERVER, "version": env!("CARGO_PKG_VERSION") },
            }))
        }
        "ping" => Ok(json!({})),
        "tools/list" => Ok(json!({ "tools": [move_to_folder()] })),
        "tools/call" if params["name"] == MOVE_TO_FOLDER => Ok(call_move(host, named, &params["arguments"]).await),
        "tools/call" => Err((-32602, format!("There's no tool {}.", params["name"]))),
        other => Err((-32601, format!("{other} is not offered."))),
    };
    Some(match result {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err((code, message)) => json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } }),
    })
}

/// `move_to_folder`, as `tools/list` describes it.
pub fn move_to_folder() -> Value {
    json!({
        "name": MOVE_TO_FOLDER,
        "title": "Move to another folder",
        "description": "Move this conversation to work in another folder on this computer. Call it only when the owner asks you to work somewhere else (for example \"work in ~/workspaces/foo\"). The folder must already exist; set create to true only when the owner asked for a new folder. The owner's next message reaches you in a new session working in that folder, starting with your handoff note. When it succeeds, finish this turn with one short line and do no more work here.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": { "type": "string", "description": "The folder, as the owner named it: an absolute path, or one starting with ~ for the home folder." },
                "handoff": { "type": "string", "description": "What you were doing and what's next, for yourself in the new folder." },
                "create": { "type": "boolean", "description": "Make the folder if it doesn't exist. Only when the owner asked for a new folder." }
            },
            "required": ["path", "handoff"],
            "additionalProperties": false
        }
    })
}

async fn call_move(host: &Arc<Host>, named: &Named, arguments: &Value) -> Value {
    let Some(session) = &named.session else {
        return text("This conversation isn't ready yet. Try again in a moment.", true);
    };
    let path = arguments["path"].as_str().unwrap_or("").trim();
    if path.is_empty() {
        return text("Say which folder: path is required.", true);
    }
    let handoff = arguments["handoff"].as_str().unwrap_or("");
    let create = arguments["create"].as_bool().unwrap_or(false);
    match host.move_to_folder(&named.agent, session, path, handoff, create).await {
        Ok(folder) => text(
            &format!(
                "Moved. The owner's next message reaches you working in {}, starting with your handoff note. Finish this turn with one short line.",
                folder.display()
            ),
            false,
        ),
        Err(why) => text(&why, true),
    }
}

fn text(text: &str, is_error: bool) -> Value {
    json!({ "content": [{ "type": "text", "text": text }], "isError": is_error })
}
