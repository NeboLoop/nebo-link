//! The fake client: drives the recorded examples against a host.
//!
//! An example is a list of steps, each on a named connection (default
//! `client`):
//!
//! - `{"connect": "host" | "pair"}` opens the connection (for `pair`, to
//!   the relay's pairing endpoint with the code's nameplate, when given).
//! - `{"send": frame}` sends a frame.
//! - `{"expect": frame}` waits up to 5 s for a matching frame. With
//!   `"ordered": true` the frame must also have arrived after the frame the
//!   previous `expect` on this connection matched (the one guarantee across
//!   channels: `host/turn` `ended` after the turn's updates and answer).
//! - `{"close": true}` closes the connection.
//! - `{"expectClose": code}` waits for the host to close with `code`.
//!
//! Matching: an expected object matches when every member it names
//! matches (the host may send more); an expected array matches when its
//! elements match actual elements in order (the host may send more); other
//! values must be equal. The string `"{{name}}"` captures the actual value
//! the first time and must equal it afterwards; in a sent frame it is
//! replaced by the captured value. `"{{*}}"` matches anything. An example
//! passes on only the captures it lists in `keeps`.
//!
//! Order: frames on one agent channel must arrive in the order expected
//! (notifications nobody expects are skipped); host-channel notifications
//! may arrive in any order relative to each other and to agent channels,
//! except where a step says `"ordered": true`.
//! Every host-channel frame received is checked against `spec/schemas/`.
//!
//! With end-to-end encryption ([`Target::e2e`], spec 17), a `pair`
//! connection runs the pairing handshake (CPace, then Noise) with the code
//! before its `host/pair` goes inside it, with the device's own static key
//! as `device.publicKey`; a `host` connection runs the Noise IK handshake
//! where the example sends `host/hello` (the handshake replaces it), and the
//! suite checks the handshake's answer, with `host/info`, against the
//! example's expected `host/hello` result. Through a relay
//! ([`Target::relay`]) every connection proves the device's key to it.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::{Map, Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use futures::{Sink, Stream};
use oal_secure::{KeyStore, PairingCode, PublicKey, Side};

use crate::schema::Schemas;
use crate::spec::Example;

const WAIT: Duration = Duration::from_secs(5);
/// The id of the `host/info` the suite asks after an encrypted handshake.
const INFO_ID: &str = "oal-conformance-info";

/// Where and how to reach the host under test.
#[derive(Debug, Clone)]
pub struct Target {
    pub url: String,
    /// The relay's pairing endpoint (`wss://<relay>/oal/pair`): `host/pair`
    /// goes to `<pair_url>/<nameplate>`. When not set, pairing goes to `url`.
    pub pair_url: Option<String>,
    /// Extra upgrade headers (a relay's `Authorization`).
    pub headers: Vec<(String, String)>,
    /// Values known before the run: `code`, `agent`.
    pub known: Vec<(String, Value)>,
    /// Every connection end-to-end encrypted (spec 17).
    pub e2e: bool,
    /// `url` is a relay's base URL (`https://relay.example.com`): connections
    /// go to `/oal/pair/<nameplate>` and `/oal/hosts/<hostId>`, each proving
    /// the device's key to the relay.
    pub relay: bool,
    /// The host's LAN certificate fingerprint (`tlsFingerprint`): a `wss://`
    /// connection to the host itself accepts only that certificate.
    pub tls_fingerprint: Option<String>,
}

pub struct Outcome {
    pub example: &'static str,
    pub result: Result<(), String>,
}

/// Runs `examples` in order. What an example captures is its own, except
/// the names it lists in `keeps` (`pair` keeps the device credential,
/// `agents` the agent's folder), which later examples use.
pub async fn run(target: &Target, examples: &[&'static Example]) -> Vec<Outcome> {
    let schemas = Schemas::load();
    let mut kept: HashMap<String, Value> = target.known.iter().cloned().collect();
    let mut outcomes = Vec::new();
    // This run's device key and the host it pairs with.
    let dir = std::env::temp_dir().join(format!("oal-conformance-{}", crate::now().replace(':', "")  + &random_suffix()));
    let store = match KeyStore::open(&dir) {
        Ok(store) => store,
        Err(e) => {
            return vec![Outcome {
                example: "pair",
                result: Err(format!("could not make this run's device key: {e}")),
            }];
        }
    };
    for example in examples {
        let mut captures = kept.clone();
        let result = Run {
            target,
            schemas: &schemas,
            captures: &mut captures,
            conns: HashMap::new(),
            store: &store,
        }
        .example(example)
        .await;
        let doc: Value = serde_json::from_str(example.json).unwrap_or_default();
        let keeps: Vec<&str> = doc["keeps"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        for name in &keeps {
            if let Some(value) = captures.get(*name) {
                kept.insert((*name).to_owned(), value.clone());
            }
        }
        let stop = result.is_err() && !keeps.is_empty();
        outcomes.push(Outcome {
            example: example.name,
            result,
        });
        // The examples after it need what it failed to capture.
        if stop {
            break;
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
    outcomes
}

fn random_suffix() -> String {
    let mut bytes = [0u8; 6];
    let _ = getrandom::getrandom(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A connection's transport.
enum Wire {
    /// OAL 0.1: one JSON text message per frame.
    Plain(Socket),
    /// Encrypted, before its handshake: the next `host/pair` (`pairing`) or
    /// `host/hello` runs it.
    Opening { socket: Socket, pairing: bool },
    /// Encrypted: frames are Noise messages.
    Secure {
        session: oal_secure::Session<WsTransport>,
        closed: Arc<Mutex<Option<u16>>>,
    },
    /// Gone, while the step that took it runs.
    Taken,
}

/// What a connection gave next.
enum Received {
    Frame(Value),
    Closed(Option<u16>),
    Failed(String),
}

struct Conn {
    wire: Wire,
    /// Frames received and not yet matched, with their arrival number.
    buffer: VecDeque<(u64, Value)>,
    received: u64,
    /// The arrival number of the frame the last `expect` matched.
    last_matched: u64,
    /// Host-channel requests sent, by id, for checking their results.
    sent: HashMap<String, String>,
}

struct Run<'a> {
    target: &'a Target,
    schemas: &'a Schemas,
    captures: &'a mut HashMap<String, Value>,
    conns: HashMap<String, Conn>,
    store: &'a KeyStore,
}

impl Run<'_> {
    async fn example(&mut self, example: &Example) -> Result<(), String> {
        let doc: Value = serde_json::from_str(example.json)
            .map_err(|e| format!("the example is not JSON: {e}"))?;
        let steps = doc["steps"].as_array().ok_or("the example has no steps")?;
        for (n, step) in steps.iter().enumerate() {
            let conn = step["conn"].as_str().unwrap_or("client").to_owned();
            let note = step["note"].as_str().unwrap_or("");
            self.step(&conn, step)
                .await
                .map_err(|e| format!("step {} ({conn}: {note}): {e}", n + 1))?;
        }
        for (_, mut conn) in self.conns.drain() {
            conn.close().await;
        }
        Ok(())
    }

    async fn step(&mut self, name: &str, step: &Value) -> Result<(), String> {
        if let Some(kind) = step["connect"].as_str() {
            let socket = self.open(kind).await?;
            let wire = match self.target.e2e {
                true => Wire::Opening { socket, pairing: kind == "pair" },
                false => Wire::Plain(socket),
            };
            self.conns.insert(
                name.to_owned(),
                Conn {
                    wire,
                    buffer: VecDeque::new(),
                    received: 0,
                    last_matched: 0,
                    sent: HashMap::new(),
                },
            );
            return Ok(());
        }
        let conn = self
            .conns
            .get_mut(name)
            .ok_or("the connection is not open")?;
        if let Some(frame) = step.get("send") {
            let frame = substitute(frame, self.captures)?;
            if let (None, Some(id), Some(method)) = (
                frame.get("agent"),
                frame.get("id"),
                frame["method"].as_str(),
            ) {
                conn.sent.insert(id.to_string(), method.to_owned());
            }
            return match std::mem::replace(&mut conn.wire, Wire::Taken) {
                Wire::Opening { socket, pairing } => {
                    let store = self.store;
                    let schemas = self.schemas;
                    let code = self.captures.get("code").and_then(Value::as_str).unwrap_or("").to_owned();
                    let host_id = self.captures.get("hostId").and_then(Value::as_str).unwrap_or("").to_owned();
                    match (pairing, frame["method"].as_str()) {
                        (true, Some("host/pair")) => pair_encrypted(conn, schemas, store, socket, &code, frame).await,
                        (false, Some("host/hello")) => hello_encrypted(conn, schemas, store, socket, &host_id, frame).await,
                        _ => Err("an encrypted connection starts with host/pair (pairing) or host/hello".into()),
                    }
                }
                Wire::Plain(mut socket) => {
                    let sent = socket
                        .send(Message::text(frame.to_string()))
                        .await
                        .map_err(|e| format!("send failed: {e}"));
                    conn.wire = Wire::Plain(socket);
                    sent
                }
                Wire::Secure { mut session, closed } => {
                    let sent = session
                        .send(frame.to_string().as_bytes())
                        .await
                        .map_err(|e| format!("send failed: {e}"));
                    conn.wire = Wire::Secure { session, closed };
                    sent
                }
                Wire::Taken => Err("the connection is gone".into()),
            };
        }
        if let Some(expected) = step.get("expect") {
            let ordered = step["ordered"] == true;
            return expect(conn, self.schemas, expected, ordered, self.captures).await;
        }
        if step["close"] == true {
            conn.close().await;
            self.conns.remove(name);
            return Ok(());
        }
        if let Some(code) = step["expectClose"].as_u64() {
            return expect_close(conn, code).await;
        }
        Err(format!("unknown step {step}"))
    }
}

impl Run<'_> {
    /// Opens a connection of `kind` (`pair` or `host`).
    async fn open(&self, kind: &str) -> Result<Socket, String> {
        let code = self.captures.get("code").and_then(Value::as_str).unwrap_or("");
        if self.target.relay {
            let key = oal_relay::Keypair::from_secret(*self.store.secret());
            let relay = oal_relay::RelayClient::new(&self.target.url, key).map_err(|e| e.to_string())?;
            return match kind {
                "pair" => relay.pair(&crate::nameplate(code)).await.map(|(ws, _)| ws),
                _ => {
                    let host_id = self.captures.get("hostId").and_then(Value::as_str).unwrap_or("");
                    relay.connect(host_id).await
                }
            }
            .map_err(|e| format!("the relay refused the connection: {e}"));
        }
        let url = match (kind, &self.target.pair_url) {
            ("pair", Some(base)) => format!("{}/{}", base.trim_end_matches('/'), crate::nameplate(code)),
            _ => self.target.url.clone(),
        };
        connect(&url, &self.target.headers, self.target.tls_fingerprint.as_deref()).await
    }
}

impl Conn {
    async fn close(&mut self) {
        match std::mem::replace(&mut self.wire, Wire::Taken) {
            Wire::Plain(mut socket) | Wire::Opening { mut socket, .. } => {
                let _ = socket.close(None).await;
            }
            Wire::Secure { mut session, .. } => {
                let _ = session.close().await;
            }
            Wire::Taken => {}
        }
    }

    /// The next frame, close or failure.
    async fn next(&mut self) -> Received {
        match &mut self.wire {
            Wire::Plain(socket) | Wire::Opening { socket, .. } => loop {
                match socket.next().await {
                    Some(Ok(Message::Text(text))) => {
                        return match serde_json::from_str(&text) {
                            Ok(frame) => Received::Frame(frame),
                            Err(e) => Received::Failed(format!("the host sent something that isn't JSON ({e}): {text}")),
                        };
                    }
                    Some(Ok(Message::Close(close))) => {
                        return Received::Closed(close.map(|f| u16::from(f.code)));
                    }
                    Some(Ok(_)) => continue,
                    Some(Err(e)) => return Received::Failed(format!("the connection failed: {e}")),
                    None => return Received::Closed(None),
                }
            },
            Wire::Secure { session, closed } => match session.recv().await {
                Ok(Some(bytes)) => match serde_json::from_slice(&bytes) {
                    Ok(frame) => Received::Frame(frame),
                    Err(e) => Received::Failed(format!("the host sent a frame that isn't JSON ({e})")),
                },
                Ok(None) => Received::Closed(*closed.lock().expect("close code")),
                Err(e) => match *closed.lock().expect("close code") {
                    Some(code) => Received::Closed(Some(code)),
                    None => Received::Failed(format!("the encrypted session failed: {e}")),
                },
            },
            Wire::Taken => Received::Failed("the connection is gone".into()),
        }
    }

    /// Takes a frame the host sent: checked against the schemas and
    /// buffered for the steps that expect it.
    fn take(&mut self, schemas: &Schemas, frame: Value) -> Result<(), String> {
        let answered = frame
            .get("id")
            .and_then(|id| self.sent.get(&id.to_string()))
            .map(String::as_str);
        schemas
            .check(&frame, answered)
            .map_err(|e| format!("the host sent {frame}, which {e}"))?;
        self.received += 1;
        self.buffer.push_back((self.received, frame));
        Ok(())
    }

    /// Frames until the answer to request `id`, the others buffered.
    async fn answer_to(&mut self, schemas: &Schemas, id: &Value) -> Result<Value, String> {
        let deadline = tokio::time::Instant::now() + WAIT;
        loop {
            let received = tokio::time::timeout_at(deadline, self.next())
                .await
                .map_err(|_| format!("timed out waiting for the answer to {id}"))?;
            match received {
                Received::Frame(frame) if frame.get("agent").is_none() && frame.get("id") == Some(id) && frame.get("method").is_none() => {
                    return Ok(frame);
                }
                Received::Frame(frame) => self.take(schemas, frame)?,
                Received::Closed(code) => {
                    return Err(format!("the host closed the connection ({code:?}) before answering {id}"));
                }
                Received::Failed(e) => return Err(e),
            }
        }
    }
}

/// Pairs over an encrypted pairing connection (spec 17.5), then sends
/// `host/pair` inside it with the device's own key.
async fn pair_encrypted(conn: &mut Conn, schemas: &Schemas, store: &KeyStore, socket: Socket, code: &str, mut frame: Value) -> Result<(), String> {
    let code = PairingCode::parse(code).map_err(|e| format!("the code isn't valid: {e}"))?;
    let closed = Arc::new(Mutex::new(None));
    let transport = WsTransport { socket, closed: closed.clone() };
    let mut pairing = oal_secure::pair(transport, &code, store, Side::Client)
        .await
        .map_err(|e| format!("the pairing handshake failed ({e}), close {:?}", closed.lock().expect("close code")))?;
    frame["params"]["device"]["publicKey"] = json_key(&store.public_key());
    pairing
        .send(frame.to_string().as_bytes())
        .await
        .map_err(|e| format!("send failed: {e}"))?;
    let id = frame["id"].clone();
    let answer = loop {
        let received = tokio::time::timeout(WAIT, pairing.recv())
            .await
            .map_err(|_| "timed out waiting for the host/pair answer".to_owned())?;
        match received {
            Ok(Some(bytes)) => {
                let answer: Value = serde_json::from_slice(&bytes).map_err(|e| format!("the host/pair answer isn't JSON: {e}"))?;
                if answer.get("id") == Some(&id) {
                    break answer;
                }
                conn.take(schemas, answer)?;
            }
            Ok(None) | Err(_) => {
                return Err(format!("the host closed the pairing ({:?}) before answering host/pair", closed.lock().expect("close code")));
            }
        }
    };
    let result = &answer["result"];
    let host = &result["info"]["host"];
    let session = match (host["publicKey"].as_str().and_then(|k| k.parse::<PublicKey>().ok()), host["id"].as_str(), result["device"]["id"].as_str()) {
        (Some(key), Some(host_id), Some(device_id)) => pairing
            .finish(&key, host_id, host["name"].as_str().unwrap_or(""), device_id)
            .map_err(|e| format!("the host named a key the handshake didn't prove: {e}"))?,
        // A refusal: taken as it is, and the host closes.
        _ => {
            conn.take(schemas, answer)?;
            return Ok(());
        }
    };
    conn.wire = Wire::Secure { session, closed };
    conn.take(schemas, answer)
}

/// Opens an encrypted session where the example sends `host/hello` (spec
/// 17.2): the handshake carries the hello's versions and client, and its
/// answer (with `host/info`) stands for `host/hello`'s result.
async fn hello_encrypted(conn: &mut Conn, schemas: &Schemas, store: &KeyStore, socket: Socket, host_id: &str, frame: Value) -> Result<(), String> {
    let host = store
        .peers()
        .into_iter()
        .find(|p| p.side == Side::Host && p.id == host_id)
        .ok_or_else(|| format!("this run hasn't paired with {host_id}"))?;
    let closed = Arc::new(Mutex::new(None));
    let transport = WsTransport { socket, closed: closed.clone() };
    let hello = json!({ "protocol": frame["params"]["protocol"], "client": frame["params"]["client"] });
    let (session, reply) = oal_secure::connect(transport, store, &host, hello.to_string().as_bytes())
        .await
        .map_err(|e| format!("the encrypted handshake failed ({e}), close {:?}", closed.lock().expect("close code")))?;
    let reply: Value = serde_json::from_slice(&reply).map_err(|e| format!("the handshake's answer isn't JSON: {e}"))?;
    let id = frame["id"].clone();
    conn.wire = Wire::Secure { session, closed };
    if let Some(error) = reply.get("error") {
        return conn.take(schemas, json!({ "jsonrpc": "2.0", "id": id, "error": error }));
    }
    let info_id = json!(INFO_ID);
    conn.sent.insert(info_id.to_string(), "host/info".into());
    if let Wire::Secure { session, .. } = &mut conn.wire {
        session
            .send(json!({ "jsonrpc": "2.0", "id": INFO_ID, "method": "host/info", "params": {} }).to_string().as_bytes())
            .await
            .map_err(|e| format!("send failed: {e}"))?;
    }
    let info = conn.answer_to(schemas, &info_id).await?;
    conn.take(schemas, json!({ "jsonrpc": "2.0", "id": id, "result": {
        "protocol": reply["protocol"], "device": reply["device"], "info": info["result"],
    } }))
}

fn json_key(key: &PublicKey) -> Value {
    Value::String(key.to_string())
}

/// A WebSocket as `oal_secure`'s transport, noting the close code the host
/// ended it with.
struct WsTransport {
    socket: Socket,
    closed: Arc<Mutex<Option<u16>>>,
}

impl Stream for WsTransport {
    type Item = io::Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return Poll::Ready(match ready!(Pin::new(&mut self.socket).poll_next(cx)) {
                Some(Ok(Message::Binary(b))) => Some(Ok(b.to_vec())),
                Some(Ok(Message::Text(_))) => Some(Err(io::Error::new(io::ErrorKind::InvalidData, "a text message on an encrypted connection"))),
                Some(Ok(Message::Close(frame))) => {
                    *self.closed.lock().expect("close code") = frame.map(|f| u16::from(f.code));
                    None
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => Some(Err(io::Error::other(e))),
                None => None,
            });
        }
    }
}

impl Sink<Vec<u8>> for WsTransport {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_ready(cx).map_err(io::Error::other)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> io::Result<()> {
        Pin::new(&mut self.socket).start_send(Message::Binary(item.into())).map_err(io::Error::other)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_flush(cx).map_err(io::Error::other)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_close(cx).map_err(io::Error::other)
    }
}

async fn connect(url: &str, headers: &[(String, String)], fingerprint: Option<&str>) -> Result<Socket, String> {
    let mut request = url
        .into_client_request()
        .map_err(|e| format!("bad URL {url}: {e}"))?;
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "oal".parse().expect("header"));
    for (name, value) in headers {
        let name: tokio_tungstenite::tungstenite::http::HeaderName = name
            .parse()
            .map_err(|_| format!("bad header name {name}"))?;
        request.headers_mut().insert(
            name,
            value
                .parse()
                .map_err(|_| format!("bad header value for {value}"))?,
        );
    }
    let connector = fingerprint.map(|fingerprint| {
        tokio_tungstenite::Connector::Rustls(Arc::new(crate::pinned::config(fingerprint)))
    });
    let (socket, _) = tokio_tungstenite::connect_async_tls_with_config(request, None, false, connector)
        .await
        .map_err(|e| format!("could not connect to {url}: {e}"))?;
    Ok(socket)
}

/// The channel a frame is on: `Some(agent)` or `None` for the host channel.
fn channel(frame: &Value) -> Option<&str> {
    frame.get("agent").and_then(Value::as_str)
}

fn is_notification(frame: &Value) -> bool {
    let msg = frame.get("acp").unwrap_or(frame);
    msg.get("method").is_some() && msg.get("id").is_none()
}

async fn expect(
    conn: &mut Conn,
    schemas: &Schemas,
    expected: &Value,
    ordered: bool,
    captures: &mut HashMap<String, Value>,
) -> Result<(), String> {
    // The expected frame's channel, with a captured agent id put in.
    let want_channel = match expected.get("agent") {
        Some(agent) => Some(
            substitute(agent, captures)?
                .as_str()
                .unwrap_or("")
                .to_owned(),
        ),
        None => None,
    };
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        let mut index = 0;
        while index < conn.buffer.len() {
            let (seq, frame) = &conn.buffer[index];
            let seq = *seq;
            if channel(frame) != want_channel.as_deref() {
                index += 1;
                continue;
            }
            if let Some(found) = matches(expected, frame, captures) {
                if ordered && seq < conn.last_matched {
                    return Err(format!(
                        "{frame} arrived before the frame the previous step matched; it must come after it"
                    ));
                }
                *captures = found;
                conn.last_matched = seq;
                conn.buffer.remove(index);
                return Ok(());
            }
            match (&want_channel, is_notification(frame)) {
                // Host-channel frames may come in any order: leave them.
                (None, _) => index += 1,
                // An agent channel is ordered: skip a notification nobody
                // expects, fail on anything else.
                (Some(_), true) => {
                    conn.buffer.remove(index);
                }
                (Some(_), false) => return Err(format!("expected {expected}, got {frame}")),
            }
        }
        let received = tokio::time::timeout_at(deadline, conn.next()).await;
        match received {
            Err(_) => {
                let seen: Vec<String> = conn.buffer.iter().map(|(_, f)| f.to_string()).collect();
                return Err(format!(
                    "timed out waiting for {expected}; unmatched frames: [{}]",
                    seen.join(", ")
                ));
            }
            Ok(Received::Frame(frame)) => conn.take(schemas, frame)?,
            Ok(Received::Closed(close)) => {
                return Err(format!(
                    "the host closed the connection ({close:?}) while we waited for {expected}"
                ));
            }
            Ok(Received::Failed(e)) => return Err(e),
        }
    }
}

async fn expect_close(conn: &mut Conn, code: u64) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match tokio::time::timeout_at(deadline, conn.next()).await {
            Err(_) => return Err(format!("the host did not close the connection with {code}")),
            Ok(Received::Closed(Some(closed))) if u64::from(closed) == code => return Ok(()),
            Ok(Received::Closed(other)) => {
                return Err(format!("the host closed with {other:?}, expected {code}"));
            }
            Ok(Received::Frame(_)) => {}
            Ok(Received::Failed(e)) => {
                return Err(format!("the connection failed before closing with {code}: {e}"));
            }
        }
    }
}

fn placeholder(value: &Value) -> Option<&str> {
    value.as_str()?.strip_prefix("{{")?.strip_suffix("}}")
}

/// Replaces every `"{{name}}"` in `frame` with its captured value.
pub fn substitute(frame: &Value, captures: &HashMap<String, Value>) -> Result<Value, String> {
    if let Some(name) = placeholder(frame) {
        return captures
            .get(name)
            .cloned()
            .ok_or_else(|| format!("{{{{{name}}}}} has not been captured yet"));
    }
    Ok(match frame {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| Ok((k.clone(), substitute(v, captures)?)))
                .collect::<Result<Map<_, _>, String>>()?,
        ),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|v| substitute(v, captures))
                .collect::<Result<_, _>>()?,
        ),
        other => other.clone(),
    })
}

/// Whether `actual` matches `expected`; the captures after matching.
pub fn matches(
    expected: &Value,
    actual: &Value,
    captures: &HashMap<String, Value>,
) -> Option<HashMap<String, Value>> {
    if let Some(name) = placeholder(expected) {
        if name == "*" {
            return Some(captures.clone());
        }
        return match captures.get(name) {
            Some(known) => (known == actual).then(|| captures.clone()),
            None => {
                let mut more = captures.clone();
                more.insert(name.to_owned(), actual.clone());
                Some(more)
            }
        };
    }
    match (expected, actual) {
        (Value::Object(want), Value::Object(have)) => {
            let mut captures = captures.clone();
            for (key, value) in want {
                captures = matches(value, have.get(key)?, &captures)?;
            }
            Some(captures)
        }
        (Value::Array(want), Value::Array(have)) => {
            let mut captures = captures.clone();
            let mut rest = have.iter();
            for value in want {
                captures = rest.find_map(|item| matches(value, item, &captures))?;
            }
            Some(captures)
        }
        _ => (expected == actual).then(|| captures.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn matching_captures_and_ignores_extra_members() {
        let none = HashMap::new();
        let got = matches(
            &json!({ "id": "{{a}}", "list": [{ "x": 2 }] }),
            &json!({ "id": 7, "more": true, "list": [{ "x": 1 }, { "x": 2, "y": 0 }] }),
            &none,
        )
        .unwrap();
        assert_eq!(got["a"], json!(7));
        assert!(
            matches(&json!({ "id": "{{a}}" }), &json!({ "id": 8 }), &got).is_none(),
            "a capture must repeat"
        );
        assert!(
            matches(
                &json!([{ "x": 2 }, { "x": 1 }]),
                &json!([{ "x": 1 }, { "x": 2 }]),
                &none
            )
            .is_none(),
            "arrays keep order"
        );
        assert!(
            matches(&json!({ "gone": "{{*}}" }), &json!({}), &none).is_none(),
            "a named member must be there"
        );
        assert_eq!(
            substitute(&json!({ "id": "{{a}}" }), &got).unwrap(),
            json!({ "id": 7 })
        );
        assert!(substitute(&json!("{{b}}"), &got).is_err());
    }
}
