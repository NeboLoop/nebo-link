//! One client connection, from its first message to its close (spec 4–12):
//! the encrypted handshake or pairing, then the host channel and each agent's
//! channel, with the host's events delivered in its order.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use link_core::host::{ClientId, Event, Open, Stamped};
use link_core::model::{Agent, DeviceRef, ErrorObject, Outcome, PendingChange, TurnState, code};
use oal_secure::{PairingCode, PublicKey, Session, SessionReader, SessionWriter, Side};
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc};

use crate::wire::{Incoming, Outgoing, Transport};
use crate::{OalHost, PROTOCOL, Via, Wire, select_version, token};

/// How long a client has for its first message and each handshake step
/// (spec 4.3).
pub(crate) const FIRST: Duration = Duration::from_secs(10);
/// A client that sends nothing for this long is closed with 4008 (spec 11).
const SILENCE: Duration = Duration::from_secs(60);

/// How a connection ends: its close code and reason.
type End = (u16, String);

pub(crate) async fn serve(oal: Arc<OalHost>, wire: Wire, via: Via) {
    let Wire { mut rx, tx } = wire;
    let first = match tokio::time::timeout(FIRST, rx.recv()).await {
        Ok(Some(first)) => first,
        Ok(None) => return,
        Err(_) => return close(&tx, (4001, "No first message.".into())),
    };
    let first = match first {
        Incoming::Binary(bytes) => bytes,
        Incoming::Text(text) => return refuse_plaintext(&tx, &text, oal.host_name()),
    };
    let pairing = match &via {
        Via::Relay { nameplate, .. } => nameplate.is_some(),
        // On the LAN and through a tunnel a pairing starts with CPace's
        // 34-byte MSGa; a session with Noise IK's first message, longer than
        // that.
        Via::Lan | Via::Tunnel => first.len() == 34 && first[0] == 0x20 && first[33] == 0x00,
    };
    let transport = Transport::new(first, rx, tx.clone());
    let opened = if pairing { pair(&oal, transport, &via).await } else { handshake(&oal, transport).await };
    let (session, device) = match opened {
        Ok(opened) => opened,
        Err(end) => return close(&tx, end),
    };
    let (reader, writer) = session.split();
    let conn = Connection::new(oal, device, Writer::Session(writer), false);
    let end = conn.run(Reader::Session(reader)).await;
    close(&tx, end);
}

/// Serves a client in this process (spec 4.3 after authentication): its
/// frames arrive on `rx` and go out on `tx` as they are, with no encryption
/// to add and no handshake, since nothing carries them. It is authenticated
/// as `device`; the connection is over when either channel closes.
pub(crate) async fn serve_local(oal: Arc<OalHost>, device: DeviceRef, rx: mpsc::UnboundedReceiver<Vec<u8>>, tx: mpsc::UnboundedSender<Vec<u8>>) {
    let conn = Connection::new(oal, device, Writer::Local(tx), true);
    let (code, reason) = conn.run(Reader::Local(rx)).await;
    tracing::debug!(code, reason, "oal: an in-process connection closed");
}

/// Where a connection's frames come from: a device's encrypted session, or a
/// client in this process.
enum Reader {
    Session(SessionReader<Transport>),
    Local(mpsc::UnboundedReceiver<Vec<u8>>),
}

impl Reader {
    fn set_max_frame(&mut self, bytes: usize) {
        if let Reader::Session(reader) = self {
            reader.set_max_frame(bytes);
        }
    }

    async fn recv(&mut self) -> Result<Option<Vec<u8>>, End> {
        match self {
            Reader::Session(reader) => reader.recv().await.map_err(ended),
            Reader::Local(rx) => Ok(rx.recv().await),
        }
    }
}

/// Where a connection's frames go.
enum Writer {
    Session(SessionWriter<Transport>),
    Local(mpsc::UnboundedSender<Vec<u8>>),
}

impl Writer {
    async fn send(&mut self, frame: &[u8]) -> Result<(), End> {
        match self {
            Writer::Session(writer) => writer.send(frame).await.map_err(ended),
            Writer::Local(tx) => tx.send(frame.to_vec()).map_err(|_| (1000, String::new())),
        }
    }
}

fn close(tx: &mpsc::UnboundedSender<Outgoing>, (code, reason): End) {
    tracing::debug!(code, reason, "oal: connection closed");
    let _ = tx.send(Outgoing::Close(code, reason));
}

/// A client that speaks OAL 0.1 without encryption: told why in its own
/// terms, then closed with 4001 (spec 17.2).
fn refuse_plaintext(tx: &mpsc::UnboundedSender<Outgoing>, text: &str, host_name: &str) {
    if let Ok(frame) = serde_json::from_str::<Value>(text)
        && let Some(id) = frame.get("id").filter(|_| frame.get("method").is_some())
    {
        let error = json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code::UNAUTHENTICATED,
            "message": format!("{host_name} takes encrypted connections only. Update the app.") } });
        let _ = tx.send(Outgoing::Text(error.to_string()));
    }
    close(tx, (4001, "Encrypted connections only.".into()));
}

fn ended(e: oal_secure::Error) -> End {
    (e.close_code(), e.to_string())
}

/// A JSON-RPC error answer.
fn error(id: &Value, error: &ErrorObject) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": error })
}

/// Pairs a new device (spec 6, 17.5): CPace and Noise with the code the host
/// is showing, then `host/pair` inside, answered once the relay (if it
/// carried the connection) lets the device through.
async fn pair(oal: &Arc<OalHost>, transport: Transport, via: &Via) -> Result<(Session<Transport>, DeviceRef), End> {
    let Some(code) = oal.active_code() else {
        return Err((4001, "No pairing code is showing on this computer.".into()));
    };
    let mut pairing = match tokio::time::timeout(FIRST, oal_secure::pair(transport, &code, oal.keys(), Side::Host)).await {
        Err(_) => return Err((4001, "The pairing took too long.".into())),
        Ok(Err(e)) => {
            if matches!(e, oal_secure::Error::PairingFailed) {
                oal.pairing_failed();
                tracing::info!("oal: a pairing attempt failed (a wrong code, or something in between)");
            }
            return Err(ended(e));
        }
        Ok(Ok(pairing)) => pairing,
    };
    let request: Value = match tokio::time::timeout(FIRST, pairing.recv()).await {
        Ok(Ok(Some(frame))) => serde_json::from_slice(&frame).map_err(|_| (1002, "host/pair is not JSON.".to_owned()))?,
        Ok(Ok(None)) => return Err((1000, String::new())),
        Ok(Err(e)) => return Err(ended(e)),
        Err(_) => return Err((4001, "No host/pair came.".into())),
    };
    let id = request.get("id").cloned().unwrap_or(Value::Null);
    let params = &request["params"];
    let refuse = |e: ErrorObject| error(&id, &e).to_string().into_bytes();
    let refused = || ErrorObject::new(code::PAIRING_REFUSED, "That code didn't work. Get a new one on the computer.");
    if request["method"] != "host/pair" {
        let e = ErrorObject::new(code::UNAUTHENTICATED, "Start with host/pair on a pairing connection.");
        let _ = pairing.send(&refuse(e)).await;
        return Err((4001, "Not authenticated.".into()));
    }
    let protocol = match select_version(&params["protocol"], oal.host_name(), &oal.config.software.0) {
        Ok(protocol) => protocol,
        Err(e) => {
            let _ = pairing.send(&refuse(e)).await;
            return Err((4002, "No common protocol version.".into()));
        }
    };
    // The code inside must be the code the handshake proved: every
    // character, compared in time that doesn't depend on where they differ.
    let given = params["code"].as_str().and_then(|c| PairingCode::parse(c).ok());
    let same = given.is_some_and(|given| equal(given.to_string().as_bytes(), code.to_string().as_bytes()));
    let claimed: Option<PublicKey> = params["device"]["publicKey"].as_str().and_then(|k| k.parse().ok());
    if !same || claimed != Some(pairing.peer_key()) || !oal.take_code(&code) {
        oal.pairing_failed();
        let _ = pairing.send(&refuse(refused())).await;
        return Err((4001, "Pairing refused.".into()));
    }
    let claimed = claimed.expect("checked");
    let name = params["device"]["name"].as_str().filter(|n| !n.trim().is_empty()).unwrap_or("Device").to_owned();
    let device_id = format!("d-{}", token()[..12].to_ascii_lowercase().replace(['-', '_'], "0"));
    // The relay lets the device through from now on, before it hears it's
    // paired (by the key it proved to the relay, and by its static key).
    if let Via::Relay { client_key, .. } = via
        && let Some(relay) = oal.relay()
    {
        for key in [client_key.clone(), claimed.to_string()] {
            if let Err(e) = relay.paired(&key).await {
                let failed = ErrorObject::new(code::INTERNAL, format!("The relay did not take the pairing: {e}"));
                let _ = pairing.send(&refuse(failed)).await;
                return Err((1011, "The relay did not take the pairing.".into()));
            }
        }
    }
    let result = json!({ "jsonrpc": "2.0", "id": id, "result": {
        "protocol": protocol,
        "device": { "id": device_id, "name": name, "token": token() },
        "info": oal.info(),
    } });
    pairing.send(result.to_string().as_bytes()).await.map_err(ended)?;
    let session = pairing.finish(&claimed, &device_id, &name, oal.host_id()).map_err(ended)?;
    oal.seen(&device_id);
    tracing::info!(device = %device_id, name = %name, "oal: a device paired");
    Ok((session, DeviceRef { device_id, name }))
}

/// A paired device's connection (spec 17.2): Noise IK, the hello's version
/// checked, and the answer in message 2.
async fn handshake(oal: &Arc<OalHost>, transport: Transport) -> Result<(Session<Transport>, DeviceRef), End> {
    let incoming = match tokio::time::timeout(FIRST, oal_secure::accept(transport, oal.keys(), oal.host_id())).await {
        Err(_) => return Err((4001, "The handshake took too long.".into())),
        Ok(Err(e)) => return Err(ended(e)),
        Ok(Ok(incoming)) => incoming,
    };
    let hello: Value = serde_json::from_slice(incoming.hello()).unwrap_or_default();
    let peer = incoming.peer().clone();
    match select_version(&hello["protocol"], oal.host_name(), &oal.config.software.0) {
        Err(e) => {
            let _ = incoming.finish(json!({ "error": e }).to_string().as_bytes()).await;
            Err((4002, "No common protocol version.".into()))
        }
        Ok(protocol) => {
            let reply = json!({ "protocol": protocol, "device": { "id": peer.id, "name": peer.name } });
            let session = incoming.finish(reply.to_string().as_bytes()).await.map_err(ended)?;
            Ok((session, DeviceRef { device_id: peer.id, name: peer.name }))
        }
    }
}

/// Compares secrets in time that doesn't depend on where they differ.
fn equal(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// An authenticated connection.
struct Connection {
    oal: Arc<OalHost>,
    device: DeviceRef,
    me: ClientId,
    writer: Writer,
    /// The sessions this connection is attached to, with the host's
    /// sequence number its snapshot was taken at.
    attached: HashMap<(String, String), u64>,
    /// This connection's copy of each pending permission request: its agent
    /// and the id of the `session/request_permission` sent on its channel.
    copies: HashMap<String, (String, u64)>,
    /// The prompts this connection sent that are running: the turn's id to
    /// its agent and the request's id.
    prompts: HashMap<String, (String, Value)>,
    /// Ids of the requests the host sends on agent channels.
    next_id: u64,
    /// Whether a frame from the device has decrypted yet (spec 17.2: only
    /// then is it present).
    seen: bool,
    /// The agents as last listed.
    agents: Vec<Agent>,
}

impl Connection {
    /// A connection authenticated as `device`; `seen` when its device is
    /// not one whose presence is recorded (a client in this process).
    fn new(oal: Arc<OalHost>, device: DeviceRef, writer: Writer, seen: bool) -> Self {
        Connection {
            me: oal.host().client(),
            oal,
            device,
            writer,
            attached: HashMap::new(),
            copies: HashMap::new(),
            prompts: HashMap::new(),
            next_id: 0,
            seen,
            agents: Vec::new(),
        }
    }

    async fn run(mut self, mut reader: Reader) -> End {
        let mut events = self.oal.host().subscribe();
        let mut closing = self.oal.closing();
        reader.set_max_frame(crate::MAX_FRAME);
        loop {
            let silence = tokio::time::sleep(SILENCE);
            tokio::pin!(silence);
            tokio::select! {
                frame = reader.recv() => match frame {
                    Ok(Some(frame)) => {
                        if let Err(end) = self.frame(&frame).await {
                            return end;
                        }
                    }
                    Ok(None) => return (1000, String::new()),
                    Err(end) => return end,
                },
                event = events.recv() => match event {
                    Ok(stamped) => {
                        if let Err(end) = self.event(stamped).await {
                            return end;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        return (1011, "This connection fell behind. Reconnect.".into());
                    }
                    Err(broadcast::error::RecvError::Closed) => return (1001, "The host stopped.".into()),
                },
                _ = crate::stopped(&mut closing) => return (1001, "The computer is restarting its link.".into()),
                _ = &mut silence => return (4008, "No heartbeat.".into()),
            }
        }
    }

    async fn send(&mut self, frame: Value) -> Result<(), End> {
        self.writer.send(frame.to_string().as_bytes()).await
    }

    async fn reply(&mut self, agent: Option<&str>, id: &Value, result: Result<Value, ErrorObject>) -> Result<(), End> {
        let msg = match result {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err(e) => error(id, &e),
        };
        self.send(match agent {
            Some(agent) => json!({ "agent": agent, "acp": msg }),
            None => msg,
        })
        .await
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<(), End> {
        self.send(json!({ "jsonrpc": "2.0", "method": method, "params": params })).await
    }

    async fn acp_notify(&mut self, agent: &str, method: &str, params: Value) -> Result<(), End> {
        self.send(json!({ "agent": agent, "acp": { "jsonrpc": "2.0", "method": method, "params": params } }))
            .await
    }

    /// Sends this connection its copy of a permission request.
    async fn ask(&mut self, request_id: &str, agent: &str, params: Value) -> Result<(), End> {
        self.next_id += 1;
        let id = self.next_id;
        self.copies.insert(request_id.to_owned(), (agent.to_owned(), id));
        self.send(json!({ "agent": agent, "acp": { "jsonrpc": "2.0", "id": id, "method": "session/request_permission", "params": params } }))
            .await
    }

    // -- From the client ------------------------------------------------------

    async fn frame(&mut self, bytes: &[u8]) -> Result<(), End> {
        if !self.seen {
            self.seen = true;
            self.oal.seen(&self.device.device_id);
        }
        // Anything not JSON is dropped (spec 4.2).
        let Ok(frame) = serde_json::from_slice::<Value>(bytes) else {
            return Ok(());
        };
        let Some(members) = frame.as_object() else {
            return Ok(());
        };
        match frame.get("agent") {
            Some(agent) => {
                let acp = &frame["acp"];
                match agent.as_str() {
                    Some(agent) if members.len() == 2 && acp.is_object() => self.agent_frame(agent, acp).await,
                    _ => {
                        if let Some(id) = acp.get("id").or_else(|| frame.get("id")) {
                            let invalid = ErrorObject::new(code::INVALID_REQUEST, "An agent frame is {\"agent\", \"acp\"}.");
                            return self.reply(agent.as_str(), id, Err(invalid)).await;
                        }
                        Ok(())
                    }
                }
            }
            None => self.host_frame(&frame).await,
        }
    }

    async fn host_frame(&mut self, frame: &Value) -> Result<(), End> {
        let (Some(method), Some(id)) = (frame["method"].as_str(), frame.get("id")) else {
            // Notifications and responses: the host channel expects none.
            return Ok(());
        };
        if frame["jsonrpc"] != "2.0" {
            return self
                .reply(None, id, Err(ErrorObject::new(code::INVALID_REQUEST, "That isn't JSON-RPC 2.0.")))
                .await;
        }
        let params = &frame["params"];
        let result = match method {
            // The handshake already said this; asked again, it says it again.
            "host/hello" => Ok(json!({ "protocol": PROTOCOL, "device": { "id": self.device.device_id, "name": self.device.name },
                "info": self.oal.info() })),
            "host/pair" => Err(ErrorObject::new(code::NOT_PERMITTED, "This device is already paired.")),
            "host/info" => Ok(self.oal.info()),
            "host/agents" => {
                let agents = self.list().await;
                Ok(json!({ "agents": agents }))
            }
            // Only a paired device (or the app hosting this computer, in its
            // own process) gets this far: adding and removing agents is the
            // owner's, from any of his devices.
            "host/agents/add" => match params["runtime"].as_str().filter(|r| !r.is_empty()) {
                Some(runtime) => {
                    let add = link_core::keep::Add {
                        runtime: runtime.to_owned(),
                        label: params["label"].as_str().map(str::to_owned),
                        ..Default::default()
                    };
                    let added = self.oal.host().add_agent(add).await;
                    if let Ok(agent) = &added {
                        tracing::info!(device = %self.device.device_id, agent = %agent.id, "oal: a device added an agent");
                    }
                    added.map(|agent| json!({ "agent": agent }))
                }
                None => Err(ErrorObject::new(code::INVALID_PARAMS, "host/agents/add needs a runtime.")),
            },
            "host/agents/remove" => match params["agentId"].as_str().filter(|a| !a.is_empty()) {
                Some(agent) => {
                    let removed = self.oal.host().remove_agent(agent).await;
                    if removed.is_ok() {
                        tracing::info!(device = %self.device.device_id, agent, "oal: a device removed an agent");
                    }
                    removed.map(|_| json!({}))
                }
                None => Err(ErrorObject::new(code::INVALID_PARAMS, "host/agents/remove needs an agentId.")),
            },
            "host/pending" => Ok(json!({ "requests": self.oal.host().pending() })),
            "host/status" => Ok(json!({ "agents": self.oal.host().status().await })),
            "host/answer" => {
                let id = params["id"].as_str().unwrap_or("");
                match params["optionId"].as_str() {
                    Some(option_id) => self
                        .oal
                        .host()
                        .answer(id, Outcome::Selected { option_id: option_id.to_owned() }, Some(self.device.clone()))
                        .map(|()| json!({})),
                    None => Err(ErrorObject::new(code::INVALID_PARAMS, "host/answer needs an optionId.")),
                }
            }
            "host/ping" => Ok(json!({})),
            "host/devices" => {
                let devices: Vec<Value> = self
                    .oal
                    .devices()
                    .into_iter()
                    .map(|d| {
                        let current = d.id == self.device.device_id;
                        let mut row = serde_json::to_value(d).expect("devices serialize");
                        row["current"] = json!(current);
                        row
                    })
                    .collect();
                Ok(json!({ "devices": devices }))
            }
            "host/unpair" => {
                let target = params["deviceId"].as_str().unwrap_or("").to_owned();
                // Any of the owner's devices may unpair any other: they are
                // all the owner's. The answer goes out before the revoked
                // device's connections close.
                let known = self.oal.devices().iter().any(|d| d.id == target);
                if known {
                    self.reply(None, id, Ok(json!({}))).await?;
                }
                return match self.oal.unpair(&target).await {
                    Ok(()) => Ok(()),
                    Err(e) if !known => self.reply(None, id, Err(e)).await,
                    Err(e) => {
                        tracing::info!(error = %e.message, "oal: unpairing failed after it was answered");
                        Ok(())
                    }
                };
            }
            other => Err(ErrorObject::new(code::METHOD_NOT_FOUND, format!("{other} is not an OAL method."))),
        };
        self.reply(None, id, result).await
    }

    /// Every agent (`host/agents`), kept for the agent channels.
    async fn list(&mut self) -> Vec<Agent> {
        self.agents = self.oal.host().agents().await;
        self.agents.clone()
    }

    /// The agent `id` names, listing the agents again if it isn't known.
    async fn agent(&mut self, id: &str) -> Option<Agent> {
        if let Some(agent) = self.agents.iter().find(|a| a.id == id) {
            return Some(agent.clone());
        }
        self.list().await.into_iter().find(|a| a.id == id)
    }

    async fn agent_frame(&mut self, agent: &str, msg: &Value) -> Result<(), End> {
        let id = msg.get("id").cloned();
        match (msg["method"].as_str(), id) {
            (Some(method), Some(id)) => {
                let Some(found) = self.agent(agent).await else {
                    let unknown = ErrorObject::new(
                        code::UNKNOWN_AGENT,
                        format!("There's no agent called {agent} on {}.", self.oal.host_name()),
                    );
                    return self.reply(Some(agent), &id, Err(unknown)).await;
                };
                let result = self.request(&found, method, &msg["params"], &id).await?;
                match result {
                    Some(result) => self.reply(Some(agent), &id, result).await,
                    None => Ok(()),
                }
            }
            (Some("session/cancel"), None) => {
                let session = msg["params"]["sessionId"].as_str().unwrap_or("").to_owned();
                if self.attached.contains_key(&(agent.to_owned(), session.clone())) {
                    self.oal.host().cancel_session(agent, &session, Some(self.device.clone()));
                }
                Ok(())
            }
            // `$/cancel_request` for its own requests, and anything else, is
            // not acted on.
            (Some(_), None) => Ok(()),
            (None, Some(id)) => {
                self.answered(agent, &id, msg);
                Ok(())
            }
            (None, None) => Ok(()),
        }
    }

    /// An ACP request on an agent channel (spec 8). `None` when it was
    /// answered already.
    async fn request(
        &mut self,
        agent: &Agent,
        method: &str,
        params: &Value,
        id: &Value,
    ) -> Result<Option<Result<Value, ErrorObject>>, End> {
        let host = self.oal.host().clone();
        let session = params["sessionId"].as_str().unwrap_or("").to_owned();
        let key = (agent.id.clone(), session.clone());
        let attached = self.attached.contains_key(&key);
        let result = match method {
            "initialize" => {
                // Started, so its capabilities are the ones it answers with.
                let _ = host.ready_agent(&agent.id).await;
                let caps = self
                    .list()
                    .await
                    .into_iter()
                    .find(|a| a.id == agent.id)
                    .map(|a| a.capabilities)
                    .unwrap_or_else(|| agent.capabilities.clone());
                Ok(json!({
                    "protocolVersion": 1,
                    "agentCapabilities": caps,
                    "agentInfo": { "name": agent.runtime, "title": agent.label },
                    "authMethods": [],
                }))
            }
            "authenticate" | "logout" => Err(ErrorObject::new(
                code::NOT_PERMITTED,
                format!("Sign in to {} on the computer itself.", agent.label),
            )),
            "session/new" | "session/load" | "session/resume" => {
                let params = match place(agent, params) {
                    Ok(params) => params,
                    Err(e) => return Ok(Some(Err(e))),
                };
                if method == "session/new" {
                    let created = host.new_session(&agent.id, params).await;
                    if let Ok(created) = &created
                        && let Some(session) = created["sessionId"].as_str()
                    {
                        self.attached.insert((agent.id.clone(), session.to_owned()), 0);
                    }
                    created
                } else {
                    let how = if method == "session/load" { Open::Load } else { Open::Resume };
                    return match host.open_session(&agent.id, how, params).await {
                        Ok(opened) => {
                            self.attach(agent, &session, opened, id).await?;
                            Ok(None)
                        }
                        Err(e) => Ok(Some(Err(e))),
                    };
                }
            }
            "session/list" => {
                let mut params = params.clone();
                match &agent.folder {
                    Some(folder) if params.get("cwd").is_none_or(Value::is_null) => params["cwd"] = json!(folder),
                    None => params["cwd"] = json!("/"),
                    Some(_) => {}
                }
                host.list_sessions(&agent.id, params).await
            }
            "session/prompt" | "session/set_mode" | "session/set_config_option" | "session/close" | "session/delete"
                if !attached =>
            {
                Err(ErrorObject::new(code::NOT_FOUND, "Load the session on this connection first."))
            }
            "session/prompt" => {
                let prompt = params["prompt"].as_array().cloned().unwrap_or_default();
                if prompt.iter().any(|block| block["type"] == "resource_link" && block["uri"].as_str().is_some_and(|u| u.starts_with("file:"))) {
                    return Ok(Some(Err(ErrorObject::new(
                        code::NOT_PERMITTED,
                        "Files on this computer can't be sent from another device. Send the file itself.",
                    ))));
                }
                return match host.prompt(&agent.id, &session, prompt, Some(self.device.clone()), Some(self.me)) {
                    Ok(turn) => {
                        self.prompts.insert(turn.turn_id, (agent.id.clone(), id.clone()));
                        Ok(None)
                    }
                    Err(e) => Ok(Some(Err(e))),
                };
            }
            "session/set_mode" | "session/set_config_option" | "session/close" | "session/delete" => {
                let changed = host.change_session(&agent.id, method, params.clone(), Some(self.me)).await;
                if changed.is_ok() && matches!(method, "session/close" | "session/delete") {
                    self.attached.remove(&key);
                }
                changed
            }
            _ => Err(ErrorObject::new(code::METHOD_NOT_FOUND, format!("{method} is not offered on this channel."))),
        };
        Ok(Some(result))
    }

    /// Attaches this connection to a session it loaded or resumed (spec 8,
    /// 12): the record, the session's most recent `host/turn`, the answer,
    /// then a copy of each of its pending permission requests.
    async fn attach(&mut self, agent: &Agent, session: &str, opened: link_core::host::Opened, id: &Value) -> Result<(), End> {
        for recorded in opened.record {
            self.acp_notify(&agent.id, "session/update", json!({ "sessionId": session, "update": recorded.update }))
                .await?;
        }
        if let Some(turn) = opened.turn {
            self.notify("host/turn", json!(turn)).await?;
        }
        self.reply(Some(&agent.id), id, Ok(opened.response)).await?;
        for request in opened.pending {
            self.ask(&request.id, &agent.id, request.params.clone()).await?;
        }
        self.attached.insert((agent.id.clone(), session.to_owned()), opened.seq);
        Ok(())
    }

    /// A response on an agent channel: the answer to this connection's copy
    /// of a permission request, or its reply to `$/cancel_request`.
    fn answered(&mut self, agent: &str, id: &Value, msg: &Value) {
        let found = self
            .copies
            .iter()
            .find(|(_, (a, copy))| a == agent && id.as_u64() == Some(*copy))
            .map(|(request, _)| request.clone());
        let Some(request) = found else {
            // A late answer, or the reply to $/cancel_request: ignored.
            return;
        };
        // An error answer withdraws only this connection's copy.
        let Ok(outcome) = serde_json::from_value::<Outcome>(msg["result"]["outcome"].clone()) else {
            self.copies.remove(&request);
            return;
        };
        if let Err(e) = self.oal.host().answer(&request, outcome, Some(self.device.clone())) {
            tracing::debug!(request, code = e.code, "oal: an answer that came too late");
        }
    }

    // -- From the host --------------------------------------------------------

    /// Whether this connection is attached to the session and its snapshot
    /// doesn't already hold what happened at `seq`.
    fn watching(&self, agent: &str, session: &str, seq: u64) -> bool {
        self.attached.get(&(agent.to_owned(), session.to_owned())).is_some_and(|from| seq > *from)
    }

    async fn event(&mut self, Stamped { seq, event }: Stamped) -> Result<(), End> {
        match event {
            Event::Turn(turn) => {
                if turn.state == TurnState::Ended {
                    self.prompts.remove(&turn.turn_id);
                }
                if self.watching(&turn.agent, &turn.session_id, seq) {
                    self.notify("host/turn", json!(turn)).await?;
                }
            }
            Event::Update(update) => {
                if update.skip != Some(self.me) && self.watching(&update.agent, &update.session_id, seq) {
                    self.acp_notify(&update.agent, "session/update", json!({ "sessionId": update.session_id, "update": update.update }))
                        .await?;
                }
            }
            Event::Answered(answered) => {
                // The prompt's answer, to the connection that sent it, before
                // the turn's end (spec 9).
                if answered.client == Some(self.me)
                    && let Some((agent, id)) = self.prompts.remove(&answered.turn_id)
                {
                    self.reply(Some(&agent), &id, answered.response).await?;
                }
            }
            Event::Pending(update) => {
                let request = &update.request;
                match update.change {
                    PendingChange::Added => {
                        if self.watching(&request.agent, &request.session_id, seq) {
                            self.ask(&request.id, &request.agent, request.params.clone()).await?;
                        }
                    }
                    PendingChange::Resolved => {
                        if let Some((agent, copy)) = self.copies.remove(&request.id) {
                            self.acp_notify(&agent, "$/cancel_request", json!({ "requestId": copy })).await?;
                        }
                    }
                }
                self.notify("host/pending_update", json!(update)).await?;
            }
            Event::Agent(update) => {
                match self.agents.iter_mut().find(|a| a.id == update.agent.id) {
                    Some(known) => *known = update.agent.clone(),
                    None => self.agents.push(update.agent.clone()),
                }
                if update.change == link_core::model::AgentChange::Removed {
                    self.agents.retain(|a| a.id != update.agent.id);
                }
                self.notify("host/agent_update", json!(update)).await?;
            }
            Event::Closed { agent, session_id } => {
                self.attached.remove(&(agent, session_id));
            }
        }
        Ok(())
    }
}

/// `session/new`, `session/load` or `session/resume` params with the host's
/// rules applied (spec 8): the session works in the agent's folder or inside
/// it, and no MCP server comes from a client (a `stdio` one is a command
/// that would run here). An agent without a folder ignores `cwd`.
fn place(agent: &Agent, params: &Value) -> Result<Value, ErrorObject> {
    let mut params = params.clone();
    if params["mcpServers"]
        .as_array()
        .is_some_and(|servers| servers.iter().any(|s| s.get("command").is_some()))
    {
        return Err(ErrorObject::new(
            code::NOT_PERMITTED,
            "MCP servers that run commands can't be added from another device.",
        ));
    }
    params["mcpServers"] = json!([]);
    match &agent.folder {
        Some(folder) => {
            let cwd = params["cwd"].as_str().unwrap_or(folder);
            let inside = cwd == folder || cwd.starts_with(&format!("{}/", folder.trim_end_matches('/')));
            if !inside {
                return Err(ErrorObject::new(
                    code::NOT_PERMITTED,
                    format!("{} works in {folder} on this computer.", agent.label),
                ));
            }
            params["cwd"] = json!(cwd);
        }
        None => params["cwd"] = json!("/"),
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(folder: Option<&str>) -> Agent {
        Agent {
            id: "app".into(),
            label: "Claude Code".into(),
            runtime: "claude-code".into(),
            folder: folder.map(str::to_owned),
            online: true,
            offline_reason: None,
            capabilities: json!({}),
            modes: None,
        }
    }

    #[test]
    fn a_session_works_in_its_agents_folder_with_no_commands_from_clients() {
        let app = agent(Some("/w/app"));
        assert_eq!(place(&app, &json!({ "cwd": "/w/app", "mcpServers": [] })).unwrap()["cwd"], "/w/app");
        assert_eq!(place(&app, &json!({ "cwd": "/w/app/src" })).unwrap()["mcpServers"], json!([]));
        assert_eq!(place(&app, &json!({ "cwd": "/w/apple" })).unwrap_err().code, code::NOT_PERMITTED);
        assert_eq!(place(&app, &json!({ "cwd": "/etc" })).unwrap_err().code, code::NOT_PERMITTED);
        let stdio = json!({ "cwd": "/w/app", "mcpServers": [{ "name": "x", "command": "rm", "args": [] }] });
        assert_eq!(place(&app, &stdio).unwrap_err().code, code::NOT_PERMITTED);
        let http = json!({ "cwd": "/w/app", "mcpServers": [{ "type": "http", "name": "x", "url": "https://example.com" }] });
        assert_eq!(place(&app, &http).unwrap()["mcpServers"], json!([]), "0.1 hosts pass none");
        assert_eq!(place(&agent(None), &json!({ "cwd": "/anything" })).unwrap()["cwd"], "/");
    }

    #[test]
    fn secrets_compare_whole() {
        assert!(equal(b"K7QM-3XRD", b"K7QM-3XRD"));
        assert!(!equal(b"K7QM-3XRD", b"K7QM-3XRE"));
        assert!(!equal(b"K7QM", b"K7QM-3XRD"));
    }
}
