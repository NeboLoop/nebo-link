//! A fake OAL host: one agent (`fake`, the [`crate::fake_agent`]) behind the
//! full host layer, for testing clients. It follows `spec/oal-0.1.md`
//! sections 4–12: pairing and hello, the host methods, the ACP rules for
//! many clients on one agent, turns, pending permission requests (first
//! answer wins), heartbeat and resuming.
//!
//! All state lives in one task ([`Host`]); connections and the agent send it
//! [`Event`]s, so the order frames are sent in is the order they happened.
//! Frames a client sends that break the spec are answered with an error and
//! logged (see [`Config::log`]).

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::SocketAddr;
use std::time::Duration;

use base64::Engine;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

use crate::schema::Schemas;
use crate::{PROTOCOL, now};

/// The agent's id, label and folder.
pub const AGENT: &str = "fake";
pub const FOLDER: &str = "/tmp/oal-fake-agent";
const MAX_FRAME: usize = 4 << 20;
/// A client that sends nothing for this long is closed with 4008.
const SILENCE: Duration = Duration::from_secs(60);

pub mod code {
    pub const VERSION_MISMATCH: i64 = -33001;
    pub const UNAUTHENTICATED: i64 = -33002;
    pub const PAIRING_REFUSED: i64 = -33003;
    pub const UNKNOWN_AGENT: i64 = -33004;
    pub const TURN_IN_PROGRESS: i64 = -33006;
    pub const ALREADY_ANSWERED: i64 = -33007;
    pub const UNKNOWN_REQUEST: i64 = -33008;
    pub const NOT_PERMITTED: i64 = -33010;
}

#[derive(Debug, Clone)]
pub struct Config {
    /// The one-time pairing code.
    pub code: String,
    pub host_name: String,
    /// Print every frame and every spec violation to stderr.
    pub log: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            code: "K7QM-3XRD".into(),
            host_name: "Fake Host".into(),
            log: false,
        }
    }
}

/// Binds `addr` and serves until the process ends.
pub async fn serve(addr: SocketAddr, config: Config) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let local = listener.local_addr()?;
    let (events, rx) = mpsc::unbounded_channel();
    let (to_agent, agent_in) = mpsc::unbounded_channel();
    let (agent_out, mut from_agent) = mpsc::unbounded_channel::<Value>();
    tokio::spawn(crate::fake_agent::run(agent_in, agent_out));
    let agent_events = events.clone();
    tokio::spawn(async move {
        while let Some(msg) = from_agent.recv().await {
            let _ = agent_events.send(Event::Agent(msg));
        }
    });
    tokio::spawn(Host::new(config, to_agent).run(rx));
    tokio::spawn(async move {
        let mut next = 0;
        while let Ok((stream, _)) = listener.accept().await {
            next += 1;
            tokio::spawn(connection(next, stream, events.clone()));
        }
    });
    Ok(local)
}

enum Event {
    Open(u64, mpsc::UnboundedSender<Out>),
    Frame(u64, Value),
    Closed(u64),
    Agent(Value),
}

enum Out {
    Frame(Value),
    Close(u16, String),
}

async fn connection(id: u64, stream: tokio::net::TcpStream, events: mpsc::UnboundedSender<Event>) {
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME))
        .max_frame_size(Some(MAX_FRAME));
    let Ok(ws) =
        tokio_tungstenite::accept_hdr_async_with_config(stream, Subprotocol, Some(config)).await
    else {
        return;
    };
    let (mut sink, mut source) = ws.split();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _ = events.send(Event::Open(id, tx));
    loop {
        tokio::select! {
            incoming = tokio::time::timeout(SILENCE, source.next()) => match incoming {
                Err(_) => {
                    let _ = sink.send(close(4008, "No heartbeat.")).await;
                    break;
                }
                Ok(Some(Ok(Message::Text(text)))) => match serde_json::from_str::<Value>(&text) {
                    Ok(frame) if frame.is_object() => { let _ = events.send(Event::Frame(id, frame)); }
                    _ => {}
                },
                Ok(Some(Ok(Message::Close(_)))) | Ok(None) | Ok(Some(Err(_))) => break,
                Ok(Some(Ok(_))) => {}
            },
            out = rx.recv() => match out {
                Some(Out::Frame(frame)) => { if sink.send(Message::text(frame.to_string())).await.is_err() { break; } }
                Some(Out::Close(code, reason)) => { let _ = sink.send(close(code, &reason)).await; break; }
                None => break,
            }
        }
    }
    let _ = events.send(Event::Closed(id));
}

/// Selects the `oal` subprotocol when the client offers it (section 4.1).
struct Subprotocol;

impl Callback for Subprotocol {
    fn on_request(self, req: &Request, mut resp: Response) -> Result<Response, ErrorResponse> {
        let offered = req
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.split(',').any(|p| p.trim() == "oal"));
        if offered {
            resp.headers_mut()
                .insert("sec-websocket-protocol", "oal".parse().expect("header"));
        }
        Ok(resp)
    }
}

fn close(code: u16, reason: &str) -> Message {
    Message::Close(Some(CloseFrame {
        code: CloseCode::from(code),
        reason: reason.to_owned().into(),
    }))
}

struct Conn {
    tx: mpsc::UnboundedSender<Out>,
    device: Option<String>,
    /// Ids of the requests we sent this connection on the agent channel.
    next_id: u64,
}

struct Device {
    name: String,
    token: String,
    paired_at: String,
    last_seen: String,
}

/// Where an answer from the agent goes.
enum Origin {
    Init,
    Client {
        conn: u64,
        id: Value,
        method: String,
        session: Option<String>,
        params: Value,
    },
}

#[derive(Default)]
struct Session {
    attached: BTreeSet<u64>,
    /// Every update sent to clients about the session (section 8).
    record: Vec<Value>,
    modes: Value,
    turn: Option<Turn>,
    /// A connection whose `session/load` was forwarded to the agent.
    loading: Option<u64>,
}

struct Turn {
    id: String,
    started_at: String,
    by: Value,
}

struct Pending {
    id: String,
    session: String,
    turn: Option<String>,
    params: Value,
    created_at: String,
    agent_request: Value,
    /// The id of the `session/request_permission` open on each connection.
    open_on: BTreeMap<u64, u64>,
}

impl Pending {
    fn describe(&self) -> Value {
        let mut request = json!({ "id": self.id, "agent": AGENT, "sessionId": self.session,
            "toolCall": self.params["toolCall"], "options": self.params["options"], "createdAt": self.created_at });
        if let Some(turn) = &self.turn {
            request["turnId"] = json!(turn);
        }
        request
    }
}

struct Host {
    config: Config,
    schemas: Schemas,
    conns: HashMap<u64, Conn>,
    devices: BTreeMap<String, Device>,
    code_used: bool,
    to_agent: mpsc::UnboundedSender<Value>,
    agent_init: Value,
    next_agent_id: u64,
    outstanding: HashMap<u64, Origin>,
    sessions: HashMap<String, Session>,
    pending: Vec<Pending>,
    resolved: HashSet<String>,
    counter: u64,
}

impl Host {
    fn new(config: Config, to_agent: mpsc::UnboundedSender<Value>) -> Self {
        let mut host = Self {
            config,
            schemas: Schemas::load(),
            conns: HashMap::new(),
            devices: BTreeMap::new(),
            code_used: false,
            to_agent,
            agent_init: Value::Null,
            next_agent_id: 0,
            outstanding: HashMap::new(),
            sessions: HashMap::new(),
            pending: Vec::new(),
            resolved: HashSet::new(),
            counter: 0,
        };
        // As section 8 says: no fs, no terminal, no elicitation.
        host.send_agent(Origin::Init, "initialize", json!({
            "protocolVersion": 1,
            "clientCapabilities": { "fs": { "readTextFile": false, "writeTextFile": false }, "terminal": false },
            "clientInfo": { "name": "oal-conformance fake host", "version": env!("CARGO_PKG_VERSION") }
        }));
        host
    }

    async fn run(mut self, mut events: mpsc::UnboundedReceiver<Event>) {
        while let Some(event) = events.recv().await {
            match event {
                Event::Open(id, tx) => {
                    self.conns.insert(
                        id,
                        Conn {
                            tx,
                            device: None,
                            next_id: 0,
                        },
                    );
                }
                Event::Frame(id, frame) => {
                    if self.config.log {
                        eprintln!("client {id} -> {frame}");
                    }
                    self.frame(id, frame);
                }
                Event::Closed(id) => self.closed(id),
                Event::Agent(msg) => self.on_agent(msg),
            }
        }
    }

    fn next(&mut self, prefix: &str) -> String {
        self.counter += 1;
        format!("{prefix}{}", self.counter)
    }

    fn info(&self) -> Value {
        json!({
            "host": { "id": "h-fake", "name": self.config.host_name, "publicKey": "3p7bfXt9wbTTW2HC7OQ1Nz-DQ8hbeGdNrfx-FG-IK08" },
            "software": { "name": "oal-conformance", "version": env!("CARGO_PKG_VERSION") },
            "protocol": { "min": PROTOCOL, "max": PROTOCOL },
            "acp": { "protocolVersion": 1 },
            "runtimes": [{ "id": "oal-fake-agent", "name": "Fake Agent", "kind": "acp", "version": env!("CARGO_PKG_VERSION") }],
            "maxFrameBytes": MAX_FRAME,
            "attachments": { "schemes": [], "maxBytes": 0 }
        })
    }

    fn agent(&self) -> Value {
        json!({ "id": AGENT, "label": "Fake Agent", "runtime": "oal-fake-agent", "folder": FOLDER, "online": true,
            "capabilities": self.agent_init["agentCapabilities"], "modes": crate::fake_agent::modes("ask") })
    }

    // ---- sending ----

    fn send(&self, conn: u64, frame: Value) {
        if let Some(c) = self.conns.get(&conn) {
            if self.config.log {
                eprintln!("client {conn} <- {frame}");
            }
            let _ = c.tx.send(Out::Frame(frame));
        }
    }

    fn close(&self, conn: u64, code: u16, reason: &str) {
        if let Some(c) = self.conns.get(&conn) {
            let _ = c.tx.send(Out::Close(code, reason.to_owned()));
        }
    }

    fn reply(
        &self,
        conn: u64,
        agent: Option<&str>,
        id: &Value,
        result: Result<Value, (i64, String)>,
    ) {
        let msg = match result {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err((code, message)) => {
                json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
            }
        };
        self.send(
            conn,
            match agent {
                Some(agent) => json!({ "agent": agent, "acp": msg }),
                None => msg,
            },
        );
    }

    fn notify(&self, conn: u64, method: &str, params: Value) {
        self.send(
            conn,
            json!({ "jsonrpc": "2.0", "method": method, "params": params }),
        );
    }

    fn notify_all(&self, method: &str, params: Value) {
        for (id, c) in &self.conns {
            if c.device.is_some() {
                self.notify(*id, method, params.clone());
            }
        }
    }

    fn acp_notify(&self, conn: u64, method: &str, params: Value) {
        self.send(conn, json!({ "agent": AGENT, "acp": { "jsonrpc": "2.0", "method": method, "params": params } }));
    }

    /// Sends a request on the agent channel; returns its id on that connection.
    fn ask_client(&mut self, conn: u64, method: &str, params: Value) -> u64 {
        let Some(c) = self.conns.get_mut(&conn) else {
            return 0;
        };
        c.next_id += 1;
        let id = c.next_id;
        self.send(conn, json!({ "agent": AGENT, "acp": { "jsonrpc": "2.0", "id": id, "method": method, "params": params } }));
        id
    }

    fn send_agent(&mut self, origin: Origin, method: &str, params: Value) {
        self.next_agent_id += 1;
        self.outstanding.insert(self.next_agent_id, origin);
        let _ = self.to_agent.send(json!({ "jsonrpc": "2.0", "id": self.next_agent_id, "method": method, "params": params }));
    }

    /// Records an update and sends it to the attached connections but `except`.
    fn broadcast_update(&mut self, session: &str, update: Value, except: Option<u64>) {
        let Some(s) = self.sessions.get_mut(session) else {
            return;
        };
        s.record.push(update.clone());
        let targets: Vec<u64> = s
            .attached
            .iter()
            .copied()
            .chain(s.loading)
            .filter(|c| Some(*c) != except)
            .collect();
        for conn in targets {
            self.acp_notify(
                conn,
                "session/update",
                json!({ "sessionId": session, "update": update }),
            );
        }
    }

    fn turn_notice(&self, session: &str, turn: &Turn, state: &str) -> Value {
        json!({ "agent": AGENT, "sessionId": session, "turnId": turn.id, "state": state, "startedAt": turn.started_at, "by": turn.by })
    }

    fn device_ref(&self, conn: u64) -> Value {
        let id = self
            .conns
            .get(&conn)
            .and_then(|c| c.device.clone())
            .unwrap_or_default();
        let name = self
            .devices
            .get(&id)
            .map(|d| d.name.clone())
            .unwrap_or_default();
        json!({ "deviceId": id, "name": name })
    }

    fn violation(&self, conn: u64, what: &str) {
        if self.config.log {
            eprintln!("client {conn}: SPEC VIOLATION: {what}");
        }
    }

    // ---- from clients ----

    fn frame(&mut self, conn: u64, frame: Value) {
        if let Err(e) = self.schemas.check(&frame, None) {
            self.violation(conn, &format!("{frame} {e}"));
            if let Some(id) = frame
                .get("id")
                .or_else(|| frame["acp"].get("id"))
                .filter(|_| frame.get("method").is_some() || frame["acp"].get("method").is_some())
            {
                let agent = frame["agent"].as_str();
                self.reply(
                    conn,
                    agent,
                    id,
                    Err((
                        -32602,
                        format!("That message doesn't match the OAL schema: {e}"),
                    )),
                );
            }
            return;
        }
        let authenticated = self.conns.get(&conn).is_some_and(|c| c.device.is_some());
        let method = frame["method"].as_str().unwrap_or("");
        if !authenticated && !matches!(method, "host/hello" | "host/pair") {
            if let Some(id) = frame.get("id").or_else(|| frame["acp"].get("id")) {
                let agent = frame["agent"].as_str();
                self.reply(
                    conn,
                    agent,
                    id,
                    Err((
                        code::UNAUTHENTICATED,
                        "Start with host/hello or host/pair.".into(),
                    )),
                );
            }
            return self.close(conn, 4001, "Not authenticated.");
        }
        match frame.get("agent").and_then(Value::as_str) {
            Some(agent) => self.agent_frame(conn, agent, &frame["acp"]),
            None => self.host_frame(conn, &frame),
        }
    }

    fn host_frame(&mut self, conn: u64, frame: &Value) {
        let (Some(method), Some(id)) = (frame["method"].as_str(), frame.get("id")) else {
            return; // Responses and notifications: the host channel expects none.
        };
        let params = &frame["params"];
        let result = match method {
            "host/hello" => return self.hello(conn, id, params),
            "host/pair" => return self.pair(conn, id, params),
            "host/info" => Ok(self.info()),
            "host/agents" => Ok(json!({ "agents": [self.agent()] })),
            "host/ping" => Ok(json!({})),
            "host/pending" => Ok(
                json!({ "requests": self.pending.iter().map(Pending::describe).collect::<Vec<_>>() }),
            ),
            "host/answer" => self.answer(conn, params),
            "host/devices" => {
                let current = self.conns.get(&conn).and_then(|c| c.device.clone());
                Ok(
                    json!({ "devices": self.devices.iter().map(|(id, d)| json!({ "id": id, "name": d.name,
                    "pairedAt": d.paired_at, "lastSeenAt": d.last_seen, "current": current.as_deref() == Some(id.as_str()) })).collect::<Vec<_>>() }),
                )
            }
            "host/unpair" => return self.unpair(conn, id, params),
            _ => Err((-32601, format!("{method} is not an OAL method."))),
        };
        self.reply(conn, None, id, result);
    }

    fn select_version(&self, params: &Value) -> Result<(), (i64, String)> {
        let parse = |v: &Value| -> Option<(u64, u64)> {
            let (major, minor) = v.as_str()?.split_once('.')?;
            Some((major.parse().ok()?, minor.parse().ok()?))
        };
        let ours = parse(&json!(PROTOCOL)).expect("version");
        let (min, max) = (
            parse(&params["protocol"]["min"]),
            parse(&params["protocol"]["max"]),
        );
        match (min, max) {
            (Some(min), Some(max)) if min <= ours && ours <= max => Ok(()),
            (Some(min), Some(max)) => {
                let (client, update) = (
                    format!("{}.{}", min.0, min.1)
                        + &if min == max {
                            String::new()
                        } else {
                            format!(" to {}.{}", max.0, max.1)
                        },
                    if max < ours {
                        "Update the app."
                    } else {
                        "Update the host software on this computer."
                    },
                );
                Err((
                    code::VERSION_MISMATCH,
                    format!(
                        "This app speaks OAL {client} and {} speaks {PROTOCOL}. {update}",
                        self.config.host_name
                    ),
                ))
            }
            _ => Err((-32602, "protocol needs min and max.".into())),
        }
    }

    fn hello(&mut self, conn: u64, id: &Value, params: &Value) {
        if let Err(e) = self.select_version(params) {
            self.reply(conn, None, id, Err((e.0, e.1)));
            return self.close(conn, 4002, "No common protocol version.");
        }
        let auth = &params["auth"];
        let device = auth["deviceId"].as_str().unwrap_or("");
        let ok = auth["type"] == "device"
            && self.devices.get(device).is_some_and(|d| {
                equal(
                    d.token.as_bytes(),
                    auth["token"].as_str().unwrap_or("").as_bytes(),
                )
            });
        if !ok {
            let message = format!(
                "This device isn't paired with {}. Pair it again.",
                self.config.host_name
            );
            self.reply(conn, None, id, Err((code::UNAUTHENTICATED, message)));
            return self.close(conn, 4001, "Not authenticated.");
        }
        self.authenticate(conn, device);
        let name = self.devices[device].name.clone();
        self.reply(conn, None, id, Ok(json!({ "protocol": PROTOCOL, "device": { "id": device, "name": name }, "info": self.info() })));
    }

    fn pair(&mut self, conn: u64, id: &Value, params: &Value) {
        if let Err(e) = self.select_version(params) {
            self.reply(conn, None, id, Err(e));
            return self.close(conn, 4002, "No common protocol version.");
        }
        let normal = |c: &str| {
            c.chars()
                .filter(|c| c.is_ascii_alphanumeric())
                .collect::<String>()
                .to_ascii_uppercase()
        };
        let given = normal(params["code"].as_str().unwrap_or(""));
        if self.code_used || !equal(given.as_bytes(), normal(&self.config.code).as_bytes()) {
            let message = "That code didn't work. Get a new one on the computer.".to_owned();
            self.reply(conn, None, id, Err((code::PAIRING_REFUSED, message)));
            return self.close(conn, 4001, "Pairing refused.");
        }
        self.code_used = true;
        let device = self.next("d-");
        let name = params["device"]["name"]
            .as_str()
            .unwrap_or("Device")
            .to_owned();
        let mut bytes = [0u8; 32];
        getrandom::getrandom(&mut bytes).expect("randomness");
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        self.devices.insert(
            device.clone(),
            Device {
                name: name.clone(),
                token: token.clone(),
                paired_at: now(),
                last_seen: now(),
            },
        );
        self.authenticate(conn, &device);
        self.reply(conn, None, id, Ok(json!({ "protocol": PROTOCOL, "device": { "id": device, "name": name, "token": token }, "info": self.info() })));
    }

    fn authenticate(&mut self, conn: u64, device: &str) {
        if let Some(c) = self.conns.get_mut(&conn) {
            c.device = Some(device.to_owned());
        }
        if let Some(d) = self.devices.get_mut(device) {
            d.last_seen = now();
        }
    }

    fn unpair(&mut self, conn: u64, id: &Value, params: &Value) {
        let target = params["deviceId"].as_str().unwrap_or("").to_owned();
        if self.devices.remove(&target).is_none() {
            return self.reply(
                conn,
                None,
                id,
                Err((-32602, "There's no such device.".into())),
            );
        }
        self.reply(conn, None, id, Ok(json!({})));
        let revoked: Vec<u64> = self
            .conns
            .iter()
            .filter(|(_, c)| c.device.as_deref() == Some(target.as_str()))
            .map(|(id, _)| *id)
            .collect();
        for c in revoked {
            self.close(c, 4003, "This device was unpaired.");
        }
    }

    fn answer(&mut self, conn: u64, params: &Value) -> Result<Value, (i64, String)> {
        let id = params["id"].as_str().unwrap_or("");
        let option = params["optionId"].as_str().unwrap_or("");
        let Some(index) = self.pending.iter().position(|p| p.id == id) else {
            return Err(if self.resolved.contains(id) {
                (
                    code::ALREADY_ANSWERED,
                    "This was already answered on another device.".into(),
                )
            } else {
                (
                    code::UNKNOWN_REQUEST,
                    "That request is no longer waiting.".into(),
                )
            });
        };
        let offered = self.pending[index].params["options"]
            .as_array()
            .is_some_and(|o| o.iter().any(|o| o["optionId"] == option));
        if !offered {
            return Err((-32602, "That isn't one of the choices.".into()));
        }
        let by = self.device_ref(conn);
        self.resolve(
            index,
            json!({ "outcome": "selected", "optionId": option }),
            by,
            None,
        );
        Ok(json!({}))
    }

    /// Answers the agent, withdraws the request from every other connection
    /// and tells everyone (section 10).
    fn resolve(&mut self, index: usize, outcome: Value, by: Value, answered_on: Option<u64>) {
        let pending = self.pending.remove(index);
        self.resolved.insert(pending.id.clone());
        let _ = self.to_agent.send(json!({ "jsonrpc": "2.0", "id": pending.agent_request, "result": { "outcome": outcome } }));
        for (conn, request) in &pending.open_on {
            if Some(*conn) != answered_on {
                self.acp_notify(*conn, "$/cancel_request", json!({ "requestId": request }));
            }
        }
        self.notify_all("host/pending_update", json!({ "change": "resolved", "request": pending.describe(), "outcome": outcome, "answeredBy": by }));
    }

    fn agent_frame(&mut self, conn: u64, agent: &str, msg: &Value) {
        let id = msg.get("id").cloned();
        if agent != AGENT {
            if let (Some(id), Some(_)) = (&id, msg.get("method")) {
                let message = format!(
                    "There's no agent called {agent} on {}.",
                    self.config.host_name
                );
                self.reply(conn, Some(agent), id, Err((code::UNKNOWN_AGENT, message)));
            }
            return;
        }
        let params = msg["params"].clone();
        let session = params["sessionId"].as_str().map(str::to_owned);
        match (msg["method"].as_str(), id) {
            (Some(method), Some(id)) => self.client_request(conn, method, id, session, params),
            (Some("session/cancel"), None) => self.cancel(conn, session.unwrap_or_default()),
            (Some(_), None) => {}
            (None, Some(id)) => self.acp_response(conn, &id, msg),
            (None, None) => {}
        }
    }

    fn client_request(
        &mut self,
        conn: u64,
        method: &str,
        id: Value,
        session: Option<String>,
        mut params: Value,
    ) {
        let err = |code: i64, message: &str| Err((code, message.to_owned()));
        let attached = session
            .as_ref()
            .and_then(|s| self.sessions.get(s))
            .is_some_and(|s| s.attached.contains(&conn));
        let forward = |host: &mut Self, params: Value| {
            let origin = Origin::Client {
                conn,
                id: id.clone(),
                method: method.to_owned(),
                session: session.clone(),
                params: params.clone(),
            };
            host.send_agent(origin, method, params);
        };
        let result = match method {
            "initialize" => {
                let mut init = self.agent_init.clone();
                init["authMethods"] = json!([]);
                Ok(init)
            }
            "authenticate" | "logout" => err(
                code::NOT_PERMITTED,
                "Sign in to Fake Agent on the computer itself.",
            ),
            "session/new" | "session/load" | "session/resume" | "session/list" => {
                let cwd = params["cwd"].as_str().unwrap_or(FOLDER).to_owned();
                if cwd != FOLDER && !cwd.starts_with(&format!("{FOLDER}/")) {
                    err(
                        code::NOT_PERMITTED,
                        &format!("Fake Agent works in {FOLDER} on this computer."),
                    )
                } else if method != "session/list"
                    && params["mcpServers"]
                        .as_array()
                        .is_some_and(|s| s.iter().any(|s| s.get("command").is_some()))
                {
                    err(
                        code::NOT_PERMITTED,
                        "MCP servers that run commands can't be added from another device.",
                    )
                } else if method == "session/list" {
                    params["cwd"] = json!(cwd);
                    return forward(self, params);
                } else if method == "session/new" {
                    params["mcpServers"] = json!([]);
                    return forward(self, params);
                } else {
                    let sid = session.clone().unwrap_or_default();
                    if self.sessions.contains_key(&sid) {
                        return self.attach(conn, &id, &sid, method == "session/load");
                    }
                    params["mcpServers"] = json!([]);
                    if method == "session/load" {
                        self.sessions.insert(
                            sid,
                            Session {
                                loading: Some(conn),
                                ..Session::default()
                            },
                        );
                    }
                    return forward(self, params);
                }
            }
            "session/prompt"
            | "session/set_mode"
            | "session/set_config_option"
            | "session/close"
            | "session/delete"
                if !attached =>
            {
                err(-32002, "Load the session on this connection first.")
            }
            "session/prompt" => {
                let sid = session.clone().unwrap_or_default();
                if self.sessions[&sid].turn.is_some() {
                    return self.reply(conn, Some(AGENT), &id, err(code::TURN_IN_PROGRESS, "Fake Agent is still working on the last message. Wait for it or stop it."));
                }
                let turn = Turn {
                    id: self.next("t-"),
                    started_at: now(),
                    by: self.device_ref(conn),
                };
                let notice = self.turn_notice(&sid, &turn, "running");
                self.sessions.get_mut(&sid).expect("session").turn = Some(turn);
                for c in self.sessions[&sid].attached.clone() {
                    self.notify(c, "host/turn", notice.clone());
                }
                for block in params["prompt"].as_array().cloned().unwrap_or_default() {
                    self.broadcast_update(
                        &sid,
                        json!({ "sessionUpdate": "user_message_chunk", "content": block }),
                        Some(conn),
                    );
                }
                return forward(self, params);
            }
            "session/set_mode"
            | "session/set_config_option"
            | "session/close"
            | "session/delete"
            | "session/cancel" => {
                return forward(self, params);
            }
            _ => err(-32601, "Method not found"),
        };
        self.reply(conn, Some(AGENT), &id, result);
    }

    /// `session/load` or `session/resume` of a session open in the agent
    /// (section 8): the record, the running turn, the answer, then the
    /// pending permission requests.
    fn attach(&mut self, conn: u64, id: &Value, sid: &str, replay: bool) {
        let session = self.sessions.get_mut(sid).expect("session");
        session.attached.insert(conn);
        if replay {
            for update in session.record.clone() {
                self.acp_notify(
                    conn,
                    "session/update",
                    json!({ "sessionId": sid, "update": update }),
                );
            }
        }
        let session = &self.sessions[sid];
        if let Some(turn) = &session.turn {
            self.notify(conn, "host/turn", self.turn_notice(sid, turn, "running"));
        }
        self.reply(conn, Some(AGENT), id, Ok(json!({ "modes": session.modes })));
        for index in 0..self.pending.len() {
            if self.pending[index].session == sid {
                let params = self.pending[index].params.clone();
                let request = self.ask_client(conn, "session/request_permission", params);
                self.pending[index].open_on.insert(conn, request);
            }
        }
    }

    fn acp_response(&mut self, conn: u64, id: &Value, msg: &Value) {
        let Some(index) = self.pending.iter().position(|p| {
            p.open_on
                .get(&conn)
                .is_some_and(|r| id.as_u64() == Some(*r))
        }) else {
            return; // Late answers and answers to $/cancel_request are ignored.
        };
        let outcome = match msg["result"]["outcome"].clone() {
            Value::Null => return, // An error answer only withdraws this connection.
            outcome => outcome,
        };
        let by = self.device_ref(conn);
        self.resolve(index, outcome, by, Some(conn));
    }

    fn cancel(&mut self, conn: u64, sid: String) {
        let attached = self
            .sessions
            .get(&sid)
            .is_some_and(|s| s.attached.contains(&conn));
        if !attached {
            return;
        }
        let _ = self.to_agent.send(
            json!({ "jsonrpc": "2.0", "method": "session/cancel", "params": { "sessionId": sid } }),
        );
        while let Some(index) = self.pending.iter().position(|p| p.session == sid) {
            let by = self.device_ref(conn);
            // The canceller answers its own copies `cancelled`, as ACP says.
            self.resolve(index, json!({ "outcome": "cancelled" }), by, Some(conn));
        }
    }

    fn closed(&mut self, conn: u64) {
        self.conns.remove(&conn);
        for session in self.sessions.values_mut() {
            session.attached.remove(&conn);
            if session.loading == Some(conn) {
                session.loading = None;
            }
        }
        for pending in &mut self.pending {
            pending.open_on.remove(&conn);
        }
    }

    // ---- from the agent ----

    fn on_agent(&mut self, msg: Value) {
        match (msg["method"].as_str(), msg.get("id").cloned()) {
            (Some("session/update"), None) => {
                let sid = msg["params"]["sessionId"].as_str().unwrap_or("").to_owned();
                self.broadcast_update(&sid, msg["params"]["update"].clone(), None);
            }
            (Some("session/request_permission"), Some(request)) => {
                self.permission(request, msg["params"].clone())
            }
            (Some(_), Some(request)) => {
                let _ = self.to_agent.send(json!({ "jsonrpc": "2.0", "id": request, "error": { "code": -32601, "message": "Method not found" } }));
            }
            (None, Some(id)) => {
                if let Some(origin) = id.as_u64().and_then(|id| self.outstanding.remove(&id)) {
                    self.agent_answered(origin, &msg);
                }
            }
            _ => {}
        }
    }

    fn permission(&mut self, agent_request: Value, params: Value) {
        let sid = params["sessionId"].as_str().unwrap_or("").to_owned();
        let turn = self
            .sessions
            .get(&sid)
            .and_then(|s| s.turn.as_ref())
            .map(|t| t.id.clone());
        let pending = Pending {
            id: self.next("p-"),
            session: sid.clone(),
            turn,
            params: params.clone(),
            created_at: now(),
            agent_request,
            open_on: BTreeMap::new(),
        };
        self.notify_all(
            "host/pending_update",
            json!({ "change": "added", "request": pending.describe() }),
        );
        self.pending.push(pending);
        let index = self.pending.len() - 1;
        for conn in self
            .sessions
            .get(&sid)
            .map(|s| s.attached.clone())
            .unwrap_or_default()
        {
            let request = self.ask_client(conn, "session/request_permission", params.clone());
            self.pending[index].open_on.insert(conn, request);
        }
    }

    fn agent_answered(&mut self, origin: Origin, msg: &Value) {
        let Origin::Client {
            conn,
            id,
            method,
            session,
            params,
        } = origin
        else {
            self.agent_init = msg["result"].clone();
            return;
        };
        let result = &msg["result"];
        let ok = msg.get("error").is_none();
        let sid = session.unwrap_or_else(|| result["sessionId"].as_str().unwrap_or("").to_owned());
        match method.as_str() {
            "session/new" if ok => {
                let session = Session {
                    attached: BTreeSet::from([conn]),
                    modes: result["modes"].clone(),
                    ..Session::default()
                };
                self.sessions.insert(sid, session);
            }
            "session/load" | "session/resume" => match self.sessions.get_mut(&sid) {
                Some(s) if ok => {
                    s.loading = None;
                    s.attached.insert(conn);
                    s.modes = result["modes"].clone();
                }
                _ if ok => {
                    let session = Session {
                        attached: BTreeSet::from([conn]),
                        modes: result["modes"].clone(),
                        ..Session::default()
                    };
                    self.sessions.insert(sid, session);
                }
                _ => {
                    self.sessions.remove(&sid);
                }
            },
            "session/prompt" => {
                if let Some(turn) = self.sessions.get_mut(&sid).and_then(|s| s.turn.take()) {
                    let mut notice = self.turn_notice(&sid, &turn, "ended");
                    if ok {
                        notice["stopReason"] = result["stopReason"].clone();
                        if result["usage"].is_object() {
                            notice["usage"] = result["usage"].clone();
                        }
                    } else {
                        notice["error"] = msg["error"].clone();
                    }
                    for c in self.sessions[&sid].attached.clone() {
                        self.notify(c, "host/turn", notice.clone());
                    }
                }
            }
            "session/set_mode" if ok => {
                let mode = params["modeId"].clone();
                if let Some(s) = self.sessions.get_mut(&sid) {
                    s.modes["currentModeId"] = mode.clone();
                }
                self.broadcast_update(
                    &sid,
                    json!({ "sessionUpdate": "current_mode_update", "currentModeId": mode }),
                    Some(conn),
                );
            }
            "session/close" | "session/delete" if ok => {
                self.sessions.remove(&sid);
            }
            _ => {}
        }
        let mut answer = msg.clone();
        answer["id"] = id;
        self.send(conn, json!({ "agent": AGENT, "acp": answer }));
    }
}

/// Compares secrets in time that doesn't depend on where they differ.
fn equal(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}
