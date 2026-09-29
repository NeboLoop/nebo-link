//! A scripted ACP agent. What it does depends on the prompt:
//!
//! | Prompt | Turn |
//! |---|---|
//! | `run: <command>` | A `tool_call` for the command, then `session/request_permission` (options `allow-once`, `reject-once`) unless the session is in mode `full`; then the result (`echo X` prints `X`), `Done.` and `end_turn`. |
//! | `wait` | `Working on it.`, then nothing until `session/cancel`, then `cancelled`. |
//! | `stall` | A `tool_call` still `pending` (as a model's stream leaves one it stopped in the middle of), then nothing until `session/cancel`, then `cancelled`. |
//! | `busy` | A `tool_call` `in_progress` (a long step running quietly), then nothing until `session/cancel`, then `cancelled`. |
//! | `work in <folder>` | Calls the host's `move_to_folder` tool (its MCP server named `host`, over HTTP) with that folder and the handoff `Was working in <cwd>.`, as a `move_to_folder` tool call; then `Moved.` or `Couldn't move: <why>`, and `end_turn`. `work in new <folder>` asks the host to make it. |
//! | `where` | `Working in <cwd>.`, then `Handoff: <text>` when the prompt started with another text block (a moved conversation's handoff), and `end_turn`. |
//! | anything else | `You said: <prompt>` and `end_turn`. |
//!
//! The command is the prompt's last text block. Every finished turn reports
//! usage `{inputTokens: 12, outputTokens: 5, totalTokens: 17}`. Sessions are
//! `sess-1`, `sess-2`, … and start in mode `ask`; the other modes are
//! `folder` and `full` (kind `full_access`). It takes HTTP MCP servers.
//!
//! With `OAL_FAKE_AGENT_SESSIONS` set to a folder, its sessions outlive it
//! there, as a real agent's do: another process of it loads one (its folder
//! and mode; the record starts empty).

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;

/// Speaks ACP on stdin and stdout, one JSON message per line.
pub async fn stdio() {
    let (to_agent, from_stdin) = mpsc::unbounded_channel();
    let (to_stdout, mut from_agent) = mpsc::unbounded_channel::<Value>();
    tokio::spawn(run(from_stdin, to_stdout));
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();
    loop {
        tokio::select! {
            line = lines.next_line() => match line {
                Ok(Some(line)) => {
                    if let Ok(msg) = serde_json::from_str::<Value>(&line) {
                        let _ = to_agent.send(msg);
                    }
                }
                _ => return,
            },
            Some(msg) = from_agent.recv() => {
                let mut line = msg.to_string();
                line.push('\n');
                if stdout.write_all(line.as_bytes()).await.is_err() || stdout.flush().await.is_err() {
                    return;
                }
            }
        }
    }
}

/// Runs the agent on channels of ACP messages until `input` closes.
pub async fn run(mut input: mpsc::UnboundedReceiver<Value>, out: mpsc::UnboundedSender<Value>) {
    let (done, mut calls) = mpsc::unbounded_channel();
    let mut agent = Agent {
        out,
        sessions: BTreeMap::new(),
        next_request: 0,
        done,
        store: std::env::var_os("OAL_FAKE_AGENT_SESSIONS").map(PathBuf::from),
    };
    loop {
        tokio::select! {
            msg = input.recv() => match msg {
                Some(msg) => agent.handle(msg),
                None => return,
            },
            Some(called) = calls.recv() => agent.called(called),
        }
    }
}

/// A tool call to the host that came back.
struct Called {
    session: String,
    prompt_id: Value,
    call: String,
    /// The tool's text, and whether it is an error.
    result: Result<String, String>,
}

struct Session {
    cwd: String,
    /// The URL of the host's MCP server, when the host gave one.
    host_tools: Option<String>,
    mode: String,
    /// Every update of the session, for `session/load` to replay.
    log: Vec<Value>,
    calls: u32,
    turn: Option<Turn>,
}

struct Turn {
    prompt_id: Value,
    waiting: Waiting,
}

enum Waiting {
    /// For the answer to our permission request.
    Permission {
        request_id: u64,
        call: String,
        command: String,
    },
    /// For `session/cancel`.
    Cancel,
}

struct Agent {
    out: mpsc::UnboundedSender<Value>,
    sessions: BTreeMap<String, Session>,
    next_request: u64,
    done: mpsc::UnboundedSender<Called>,
    /// Where its sessions outlive it (`OAL_FAKE_AGENT_SESSIONS`).
    store: Option<PathBuf>,
}

/// The host's MCP server among `params`' `mcpServers`: the HTTP one named
/// `host`.
fn host_tools(params: &Value) -> Option<String> {
    params["mcpServers"]
        .as_array()?
        .iter()
        .find(|s| s["type"] == "http" && s["name"] == "host")
        .and_then(|s| s["url"].as_str())
        .map(str::to_owned)
}

/// The fake agent's modes, `current` first chosen.
pub fn modes(current: &str) -> Value {
    json!({ "currentModeId": current, "availableModes": [
        { "id": "ask", "name": "Ask me", "description": "Asks before it runs a command." },
        { "id": "folder", "name": "Allow in its folder", "description": "Works in its folder; asks before it runs a command." },
        { "id": "full", "name": "Full access", "description": "Runs anything on this computer without asking.", "_meta": { "kind": "full_access" } }
    ]})
}

fn options() -> Value {
    json!([
        { "optionId": "allow-once", "name": "Allow once", "kind": "allow_once" },
        { "optionId": "reject-once", "name": "Deny", "kind": "reject_once" }
    ])
}

fn usage() -> Value {
    json!({ "inputTokens": 12, "outputTokens": 5, "totalTokens": 17 })
}

impl Agent {
    fn send(&self, msg: Value) {
        let _ = self.out.send(msg);
    }

    /// Keeps the session `id` where it outlives this process, when it has
    /// such a place.
    fn keep(&self, id: &str) {
        let (Some(store), Some(session)) = (&self.store, self.sessions.get(id)) else {
            return;
        };
        let _ = std::fs::create_dir_all(store);
        let kept = json!({ "cwd": session.cwd, "mode": session.mode });
        let _ = std::fs::write(store.join(format!("{id}.json")), kept.to_string());
    }

    /// The session `id` an earlier process of this agent kept.
    fn kept(&self, id: &str) -> Option<Session> {
        let text = std::fs::read_to_string(self.store.as_ref()?.join(format!("{id}.json"))).ok()?;
        let kept: Value = serde_json::from_str(&text).ok()?;
        Some(Session {
            cwd: kept["cwd"].as_str().unwrap_or("").to_owned(),
            host_tools: None,
            mode: kept["mode"].as_str().unwrap_or("ask").to_owned(),
            log: Vec::new(),
            calls: 0,
            turn: None,
        })
    }

    fn respond(&self, id: &Value, result: Result<Value, (i64, &str)>) {
        self.send(match result {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err((code, message)) => {
                json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
            }
        });
    }

    /// Sends an update and keeps it for replay.
    fn update(&mut self, session: &str, update: Value) {
        if let Some(s) = self.sessions.get_mut(session) {
            s.log.push(update.clone());
        }
        self.send(json!({ "jsonrpc": "2.0", "method": "session/update", "params": { "sessionId": session, "update": update } }));
    }

    fn say(&mut self, session: &str, text: &str) {
        self.update(session, json!({ "sessionUpdate": "agent_message_chunk", "content": { "type": "text", "text": text } }));
    }

    fn end(&mut self, session: &str, prompt_id: &Value, stop_reason: &str) {
        let result = if stop_reason == "cancelled" {
            json!({ "stopReason": "cancelled" })
        } else {
            json!({ "stopReason": stop_reason, "usage": usage() })
        };
        self.respond(prompt_id, Ok(result));
        if let Some(s) = self.sessions.get_mut(session) {
            s.turn = None;
        }
    }

    fn handle(&mut self, msg: Value) {
        let params = &msg["params"];
        match (msg["method"].as_str(), msg.get("id")) {
            (Some(method), Some(id)) => self.request(method, id.clone(), params.clone()),
            (Some("session/cancel"), None) => {
                self.cancel(params["sessionId"].as_str().unwrap_or(""))
            }
            (Some(_), None) => {}
            (None, Some(id)) => self.answered(id, &msg),
            (None, None) => {}
        }
    }

    fn request(&mut self, method: &str, id: Value, params: Value) {
        let session_id = params["sessionId"].as_str().unwrap_or("").to_owned();
        match method {
            "initialize" => self.respond(&id, Ok(json!({
                "protocolVersion": 1,
                "agentCapabilities": {
                    "loadSession": true,
                    "promptCapabilities": { "image": false, "audio": false, "embeddedContext": false },
                    "sessionCapabilities": { "list": {}, "resume": {} },
                    "mcpCapabilities": { "http": true, "sse": false }
                },
                "agentInfo": { "name": "oal-fake-agent", "title": "Fake Agent", "version": env!("CARGO_PKG_VERSION") },
                "authMethods": []
            }))),
            "session/new" => {
                let kept = self.store.as_ref().and_then(|s| std::fs::read_dir(s).ok()).map_or(0, |d| d.count());
                let id_text = format!("sess-{}", self.sessions.len().max(kept) + 1);
                self.sessions.insert(id_text.clone(), Session {
                    cwd: params["cwd"].as_str().unwrap_or("").to_owned(),
                    host_tools: host_tools(&params),
                    mode: "ask".into(),
                    log: Vec::new(),
                    calls: 0,
                    turn: None,
                });
                self.keep(&id_text);
                self.respond(&id, Ok(json!({ "sessionId": id_text, "modes": modes("ask") })));
            }
            "session/load" | "session/resume" => {
                if !self.sessions.contains_key(&session_id)
                    && let Some(kept) = self.kept(&session_id)
                {
                    self.sessions.insert(session_id.clone(), kept);
                }
                let Some(session) = self.sessions.get_mut(&session_id) else {
                    return self.respond(&id, Err((-32002, "Resource not found")));
                };
                if let Some(url) = host_tools(&params) {
                    session.host_tools = Some(url);
                }
                let session = &*session;
                let mode = session.mode.clone();
                if method == "session/load" {
                    for update in session.log.clone() {
                        self.send(json!({ "jsonrpc": "2.0", "method": "session/update", "params": { "sessionId": session_id, "update": update } }));
                    }
                }
                self.respond(&id, Ok(json!({ "modes": modes(&mode) })));
            }
            "session/list" => {
                let sessions: Vec<Value> = self
                    .sessions
                    .iter()
                    .map(|(id, s)| json!({ "sessionId": id, "cwd": s.cwd }))
                    .collect();
                self.respond(&id, Ok(json!({ "sessions": sessions })));
            }
            "session/set_mode" => {
                let mode = params["modeId"].as_str().unwrap_or("");
                match self.sessions.get_mut(&session_id) {
                    Some(s) if ["ask", "folder", "full"].contains(&mode) => {
                        s.mode = mode.to_owned();
                        self.keep(&session_id);
                        self.respond(&id, Ok(json!({})));
                    }
                    Some(_) => self.respond(&id, Err((-32602, "Invalid params"))),
                    None => self.respond(&id, Err((-32002, "Resource not found"))),
                }
            }
            "session/prompt" => self.prompt(id, &session_id, &params["prompt"]),
            _ => self.respond(&id, Err((-32601, "Method not found"))),
        }
    }

    fn prompt(&mut self, id: Value, session_id: &str, prompt: &Value) {
        let Some(session) = self.sessions.get_mut(session_id) else {
            return self.respond(&id, Err((-32002, "Resource not found")));
        };
        if session.turn.is_some() {
            return self.respond(&id, Err((-32600, "A turn is already running")));
        }
        let texts: Vec<String> = prompt
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| b["text"].as_str().map(str::to_owned))
            .collect();
        let text = texts.last().cloned().unwrap_or_default();
        // The owner's message, for replay; ACP agents don't echo it live.
        for block in prompt.as_array().into_iter().flatten() {
            session
                .log
                .push(json!({ "sessionUpdate": "user_message_chunk", "content": block }));
        }
        if let Some(command) = text.strip_prefix("run: ") {
            session.calls += 1;
            let call = format!("call-{}", session.calls);
            let full = session.mode == "full";
            let tool_call = json!({ "toolCallId": call, "title": command, "kind": "execute", "status": "pending", "rawInput": { "command": command } });
            let mut update = tool_call.clone();
            update["sessionUpdate"] = json!("tool_call");
            self.update(session_id, update);
            if full {
                return self.run_command(session_id, &id, &call, command);
            }
            self.next_request += 1;
            let request_id = self.next_request;
            self.sessions.get_mut(session_id).expect("session").turn = Some(Turn {
                prompt_id: id,
                waiting: Waiting::Permission {
                    request_id,
                    call,
                    command: command.to_owned(),
                },
            });
            self.send(json!({ "jsonrpc": "2.0", "id": request_id, "method": "session/request_permission",
                "params": { "sessionId": session_id, "toolCall": tool_call, "options": options() } }));
        } else if let Some(folder) = text.strip_prefix("work in ") {
            let (folder, create) = match folder.strip_prefix("new ") {
                Some(folder) => (folder.trim().to_owned(), true),
                None => (folder.trim().to_owned(), false),
            };
            session.calls += 1;
            let call = format!("call-{}", session.calls);
            let (url, cwd) = (session.host_tools.clone(), session.cwd.clone());
            self.update(session_id, json!({ "sessionUpdate": "tool_call", "toolCallId": call, "title": "move_to_folder", "kind": "other",
                "status": "in_progress", "rawInput": { "path": folder } }));
            let done = self.done.clone();
            let session = session_id.to_owned();
            tokio::spawn(async move {
                let arguments = json!({ "path": folder, "handoff": format!("Was working in {cwd}."), "create": create });
                let result = match url {
                    Some(url) => call_tool(&url, "move_to_folder", arguments).await,
                    None => Err("the host gave me no tools".to_owned()),
                };
                let _ = done.send(Called { session, prompt_id: id, call, result });
            });
        } else if text == "where" {
            let cwd = session.cwd.clone();
            self.say(session_id, &format!("Working in {cwd}."));
            if texts.len() > 1 {
                self.say(session_id, &format!(" Handoff: {}", texts[0]));
            }
            self.end(session_id, &id, "end_turn");
        } else if text == "wait" {
            session.turn = Some(Turn {
                prompt_id: id,
                waiting: Waiting::Cancel,
            });
            self.say(session_id, "Working on it.");
        } else if text == "stall" || text == "busy" {
            session.calls += 1;
            let call = format!("call-{}", session.calls);
            session.turn = Some(Turn {
                prompt_id: id,
                waiting: Waiting::Cancel,
            });
            let status = if text == "stall" { "pending" } else { "in_progress" };
            self.update(session_id, json!({ "sessionUpdate": "tool_call", "toolCallId": call, "title": "a long step", "kind": "other", "status": status }));
        } else {
            self.say(session_id, &format!("You said: {text}"));
            self.end(session_id, &id, "end_turn");
        }
    }

    /// A call to the host's tool came back: the turn ends with what it said.
    fn called(&mut self, called: Called) {
        let Called { session, prompt_id, call, result } = called;
        let (status, text) = match &result {
            Ok(text) => ("completed", text.clone()),
            Err(text) => ("failed", text.clone()),
        };
        self.update(&session, json!({ "sessionUpdate": "tool_call_update", "toolCallId": call, "status": status,
            "content": [{ "type": "content", "content": { "type": "text", "text": text } }] }));
        match result {
            Ok(_) => self.say(&session, "Moved."),
            Err(why) => self.say(&session, &format!("Couldn't move: {why}")),
        }
        self.end(&session, &prompt_id, "end_turn");
    }

    fn run_command(&mut self, session_id: &str, prompt_id: &Value, call: &str, command: &str) {
        let output = command.strip_prefix("echo ").unwrap_or("ran");
        self.update(
            session_id,
            json!({ "sessionUpdate": "tool_call_update", "toolCallId": call, "status": "completed",
            "content": [{ "type": "content", "content": { "type": "text", "text": output } }] }),
        );
        self.say(session_id, "Done.");
        self.end(session_id, prompt_id, "end_turn");
    }

    /// The answer to one of our permission requests.
    fn answered(&mut self, id: &Value, msg: &Value) {
        let Some((session_id, prompt_id, call, command)) =
            self.sessions.iter().find_map(|(sid, s)| match &s.turn {
                Some(Turn {
                    prompt_id,
                    waiting:
                        Waiting::Permission {
                            request_id,
                            call,
                            command,
                        },
                }) if id.as_u64() == Some(*request_id) => Some((
                    sid.clone(),
                    prompt_id.clone(),
                    call.clone(),
                    command.clone(),
                )),
                _ => None,
            })
        else {
            return;
        };
        let outcome = &msg["result"]["outcome"];
        match (outcome["outcome"].as_str(), outcome["optionId"].as_str()) {
            (Some("selected"), Some("allow-once")) => {
                self.run_command(&session_id, &prompt_id, &call, &command)
            }
            (Some("selected"), _) => {
                self.update(&session_id, json!({ "sessionUpdate": "tool_call_update", "toolCallId": call, "status": "failed" }));
                self.say(&session_id, "Okay, I won't run it.");
                self.end(&session_id, &prompt_id, "end_turn");
            }
            // Cancelled, or an error answer: the turn is over.
            _ => self.end(&session_id, &prompt_id, "cancelled"),
        }
    }

    fn cancel(&mut self, session_id: &str) {
        let waiting_cancel = matches!(
            self.sessions.get(session_id).and_then(|s| s.turn.as_ref()),
            Some(Turn {
                waiting: Waiting::Cancel,
                ..
            })
        );
        if waiting_cancel {
            let prompt_id = self.sessions[session_id]
                .turn
                .as_ref()
                .expect("turn")
                .prompt_id
                .clone();
            self.end(session_id, &prompt_id, "cancelled");
        }
        // A turn waiting for permission ends when the client answers the
        // request `cancelled`, as ACP requires of it.
    }
}

/// Calls `tool` on an MCP server over Streamable HTTP (`initialize`, then
/// `tools/call`): its text, or its error.
pub async fn call_tool(url: &str, tool: &str, arguments: Value) -> Result<String, String> {
    let init = json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": { "name": "oal-fake-agent", "version": env!("CARGO_PKG_VERSION") } } });
    post(url, &init).await?;
    post(url, &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" })).await?;
    let answer = post(url, &json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": { "name": tool, "arguments": arguments } })).await?;
    if let Some(e) = answer.get("error") {
        return Err(e["message"].as_str().unwrap_or("the tool failed").to_owned());
    }
    let text: String = answer["result"]["content"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| c["text"].as_str())
        .collect();
    match answer["result"]["isError"].as_bool() {
        Some(true) => Err(text),
        _ => Ok(text),
    }
}

/// One JSON-RPC message POSTed to `url` (`http://host:port/path`): the JSON
/// answer, or `null` for none (202).
async fn post(url: &str, message: &Value) -> Result<Value, String> {
    let rest = url.strip_prefix("http://").ok_or("only http:// tools")?;
    let (authority, path) = rest.split_once('/').map(|(a, p)| (a, format!("/{p}"))).unwrap_or((rest, "/".to_owned()));
    let mut stream = tokio::net::TcpStream::connect(authority).await.map_err(|e| e.to_string())?;
    let body = message.to_string();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {authority}\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.map_err(|e| e.to_string())?;
    let mut response = Vec::new();
    tokio::io::AsyncReadExt::read_to_end(&mut stream, &mut response).await.map_err(|e| e.to_string())?;
    let response = String::from_utf8_lossy(&response);
    let (head, body) = response.split_once("\r\n\r\n").ok_or("no HTTP answer")?;
    let status = head.split_whitespace().nth(1).unwrap_or("");
    match status {
        "202" => Ok(Value::Null),
        "200" => serde_json::from_str(body.trim()).map_err(|e| format!("not JSON: {e}")),
        other => Err(format!("the host's tools answered {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `work in <folder>` calls the host's `move_to_folder` over MCP's
    /// Streamable HTTP, with the handoff, and ends the turn with the answer.
    #[tokio::test]
    async fn work_in_calls_the_hosts_tool() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp/t0k", listener.local_addr().unwrap());
        let (calls_tx, mut calls) = mpsc::unbounded_channel::<Value>();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut buf = [0u8; 4096];
                // One small request per connection: head, then its body.
                loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    request.extend_from_slice(&buf[..n]);
                    let text = String::from_utf8_lossy(&request).to_string();
                    if let Some((head, body)) = text.split_once("\r\n\r\n") {
                        let length: usize = head
                            .lines()
                            .find_map(|l| l.strip_prefix("Content-Length: "))
                            .and_then(|v| v.trim().parse().ok())
                            .unwrap_or(0);
                        if body.len() >= length {
                            break;
                        }
                    }
                }
                let text = String::from_utf8_lossy(&request).to_string();
                assert!(text.starts_with("POST /mcp/t0k HTTP/1.1"), "{text}");
                let message: Value = serde_json::from_str(text.split_once("\r\n\r\n").unwrap().1).unwrap();
                let answer = match message["method"].as_str() {
                    Some("initialize") => Some(json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "protocolVersion": "2025-06-18", "capabilities": { "tools": {} }, "serverInfo": { "name": "host" } } })),
                    Some("tools/call") => {
                        calls_tx.send(message["params"].clone()).unwrap();
                        Some(json!({ "jsonrpc": "2.0", "id": message["id"], "result": { "content": [{ "type": "text", "text": "Moved there." }], "isError": false } }))
                    }
                    _ => None,
                };
                let response = match answer {
                    Some(answer) => {
                        let body = answer.to_string();
                        format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())
                    }
                    None => "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned(),
                };
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let (tx, rx) = mpsc::unbounded_channel();
        let (out, mut from) = mpsc::unbounded_channel();
        tokio::spawn(run(rx, out));
        let servers = json!([{ "type": "http", "name": "host", "url": url, "headers": [] }]);
        tx.send(json!({ "jsonrpc": "2.0", "id": 1, "method": "session/new", "params": { "cwd": "/w", "mcpServers": servers } })).unwrap();
        assert_eq!(from.recv().await.unwrap()["result"]["sessionId"], "sess-1");
        tx.send(json!({ "jsonrpc": "2.0", "id": 2, "method": "session/prompt", "params": { "sessionId": "sess-1", "prompt": [{ "type": "text", "text": "work in new ~/proj" }] } })).unwrap();
        assert_eq!(from.recv().await.unwrap()["params"]["update"]["title"], "move_to_folder");
        let call = calls.recv().await.unwrap();
        assert_eq!(call, json!({ "name": "move_to_folder", "arguments": { "path": "~/proj", "handoff": "Was working in /w.", "create": true } }));
        let done = from.recv().await.unwrap();
        assert_eq!(done["params"]["update"]["status"], "completed");
        assert_eq!(from.recv().await.unwrap()["params"]["update"]["content"]["text"], "Moved.");
        assert_eq!(from.recv().await.unwrap()["result"]["stopReason"], "end_turn");
    }

    #[tokio::test]
    async fn a_command_asks_then_runs() {
        let (tx, rx) = mpsc::unbounded_channel();
        let (out, mut from) = mpsc::unbounded_channel();
        tokio::spawn(run(rx, out));
        tx.send(json!({ "jsonrpc": "2.0", "id": 1, "method": "session/new", "params": { "cwd": "/w", "mcpServers": [] } })).unwrap();
        assert_eq!(from.recv().await.unwrap()["result"]["sessionId"], "sess-1");
        tx.send(json!({ "jsonrpc": "2.0", "id": 2, "method": "session/prompt", "params": { "sessionId": "sess-1", "prompt": [{ "type": "text", "text": "run: echo hi" }] } })).unwrap();
        assert_eq!(
            from.recv().await.unwrap()["params"]["update"]["sessionUpdate"],
            "tool_call"
        );
        let ask = from.recv().await.unwrap();
        assert_eq!(ask["method"], "session/request_permission");
        tx.send(json!({ "jsonrpc": "2.0", "id": ask["id"], "result": { "outcome": { "outcome": "selected", "optionId": "allow-once" } } })).unwrap();
        let done = from.recv().await.unwrap();
        assert_eq!(
            done["params"]["update"]["content"][0]["content"]["text"],
            "hi"
        );
        assert_eq!(
            from.recv().await.unwrap()["params"]["update"]["content"]["text"],
            "Done."
        );
        assert_eq!(
            from.recv().await.unwrap()["result"]["stopReason"],
            "end_turn"
        );
    }
}
