//! A scripted ACP agent. What it does depends on the prompt:
//!
//! | Prompt | Turn |
//! |---|---|
//! | `run: <command>` | A `tool_call` for the command, then `session/request_permission` (options `allow-once`, `reject-once`) unless the session is in mode `full`; then the result (`echo X` prints `X`), `Done.` and `end_turn`. |
//! | `wait` | `Working on it.`, then nothing until `session/cancel`, then `cancelled`. |
//! | anything else | `You said: <prompt>` and `end_turn`. |
//!
//! Every finished turn reports usage `{inputTokens: 12, outputTokens: 5,
//! totalTokens: 17}`. Sessions are `sess-1`, `sess-2`, … and start in mode
//! `ask`; the other modes are `folder` and `full`.

use std::collections::BTreeMap;

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
    let mut agent = Agent {
        out,
        sessions: BTreeMap::new(),
        next_request: 0,
    };
    while let Some(msg) = input.recv().await {
        agent.handle(msg);
    }
}

struct Session {
    cwd: String,
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
}

/// The fake agent's modes, `current` first chosen.
pub fn modes(current: &str) -> Value {
    json!({ "currentModeId": current, "availableModes": [
        { "id": "ask", "name": "Ask me", "description": "Asks before it runs a command." },
        { "id": "folder", "name": "Allow in its folder", "description": "Works in its folder; asks before it runs a command." },
        { "id": "full", "name": "Full access", "description": "Runs anything on this computer without asking." }
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
                    "sessionCapabilities": { "list": {}, "resume": {} }
                },
                "agentInfo": { "name": "oal-fake-agent", "title": "Fake Agent", "version": env!("CARGO_PKG_VERSION") },
                "authMethods": []
            }))),
            "session/new" => {
                let id_text = format!("sess-{}", self.sessions.len() + 1);
                self.sessions.insert(id_text.clone(), Session {
                    cwd: params["cwd"].as_str().unwrap_or("").to_owned(),
                    mode: "ask".into(),
                    log: Vec::new(),
                    calls: 0,
                    turn: None,
                });
                self.respond(&id, Ok(json!({ "sessionId": id_text, "modes": modes("ask") })));
            }
            "session/load" | "session/resume" => {
                let Some(session) = self.sessions.get(&session_id) else {
                    return self.respond(&id, Err((-32002, "Resource not found")));
                };
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
        let text: String = prompt
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|b| b["text"].as_str())
            .collect();
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
        } else if text == "wait" {
            session.turn = Some(Turn {
                prompt_id: id,
                waiting: Waiting::Cancel,
            });
            self.say(session_id, "Working on it.");
        } else {
            self.say(session_id, &format!("You said: {text}"));
            self.end(session_id, &id, "end_turn");
        }
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

#[cfg(test)]
mod tests {
    use super::*;

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
