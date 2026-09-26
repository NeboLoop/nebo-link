//! The fake client: drives the recorded examples against a host.
//!
//! An example is a list of steps, each on a named connection (default
//! `client`):
//!
//! - `{"connect": "host" | "pair"}` opens the connection (to the pair URL
//!   for `pair`).
//! - `{"send": frame}` sends a frame.
//! - `{"expect": frame}` waits up to 5 s for a matching frame.
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
//! may arrive in any order relative to each other and to agent channels.
//! Every host-channel frame received is checked against `spec/schemas/`.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use serde_json::{Map, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::schema::Schemas;
use crate::spec::Example;

const WAIT: Duration = Duration::from_secs(5);

/// Where and how to reach the host under test.
#[derive(Debug, Clone)]
pub struct Target {
    pub url: String,
    /// Where `host/pair` goes; `url` when not set.
    pub pair_url: Option<String>,
    /// Extra upgrade headers (a relay's `Authorization`).
    pub headers: Vec<(String, String)>,
    /// Values known before the run: `code`, `agent`.
    pub known: Vec<(String, Value)>,
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
    for example in examples {
        let mut captures = kept.clone();
        let result = Run {
            target,
            schemas: &schemas,
            captures: &mut captures,
            conns: HashMap::new(),
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
    outcomes
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct Conn {
    socket: Socket,
    buffer: VecDeque<Value>,
    /// Host-channel requests sent, by id, for checking their results.
    sent: HashMap<String, String>,
}

struct Run<'a> {
    target: &'a Target,
    schemas: &'a Schemas,
    captures: &'a mut HashMap<String, Value>,
    conns: HashMap<String, Conn>,
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
            let _ = conn.socket.close(None).await;
        }
        Ok(())
    }

    async fn step(&mut self, name: &str, step: &Value) -> Result<(), String> {
        if let Some(kind) = step["connect"].as_str() {
            let url = match kind {
                "pair" => self.target.pair_url.as_ref().unwrap_or(&self.target.url),
                _ => &self.target.url,
            };
            let socket = connect(url, &self.target.headers).await?;
            self.conns.insert(
                name.to_owned(),
                Conn {
                    socket,
                    buffer: VecDeque::new(),
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
            return conn
                .socket
                .send(Message::text(frame.to_string()))
                .await
                .map_err(|e| format!("send failed: {e}"));
        }
        if let Some(expected) = step.get("expect") {
            return expect(conn, self.schemas, expected, self.captures).await;
        }
        if step["close"] == true {
            let _ = conn.socket.close(None).await;
            self.conns.remove(name);
            return Ok(());
        }
        if let Some(code) = step["expectClose"].as_u64() {
            return expect_close(conn, code).await;
        }
        Err(format!("unknown step {step}"))
    }
}

async fn connect(url: &str, headers: &[(String, String)]) -> Result<Socket, String> {
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
    let (socket, _) = tokio_tungstenite::connect_async(request)
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
            let frame = &conn.buffer[index];
            if channel(frame) != want_channel.as_deref() {
                index += 1;
                continue;
            }
            if let Some(found) = matches(expected, frame, captures) {
                *captures = found;
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
        let received = tokio::time::timeout_at(deadline, conn.socket.next()).await;
        match received {
            Err(_) => {
                let seen: Vec<String> = conn.buffer.iter().map(Value::to_string).collect();
                return Err(format!(
                    "timed out waiting for {expected}; unmatched frames: [{}]",
                    seen.join(", ")
                ));
            }
            Ok(Some(Ok(Message::Text(text)))) => {
                let frame: Value = serde_json::from_str(&text).map_err(|e| {
                    format!("the host sent something that isn't JSON ({e}): {text}")
                })?;
                let answered = frame
                    .get("id")
                    .and_then(|id| conn.sent.get(&id.to_string()))
                    .map(String::as_str);
                schemas
                    .check(&frame, answered)
                    .map_err(|e| format!("the host sent {frame}, which {e}"))?;
                conn.buffer.push_back(frame);
            }
            Ok(Some(Ok(Message::Close(close)))) => {
                return Err(format!(
                    "the host closed the connection ({close:?}) while we waited for {expected}"
                ));
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => return Err(format!("the connection failed: {e}")),
            Ok(None) => {
                return Err(format!(
                    "the connection ended while we waited for {expected}"
                ));
            }
        }
    }
}

async fn expect_close(conn: &mut Conn, code: u64) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match tokio::time::timeout_at(deadline, conn.socket.next()).await {
            Err(_) => return Err(format!("the host did not close the connection with {code}")),
            Ok(Some(Ok(Message::Close(Some(frame)))))
                if u64::from(u16::from(frame.code)) == code =>
            {
                return Ok(());
            }
            Ok(Some(Ok(Message::Close(other)))) => {
                return Err(format!("the host closed with {other:?}, expected {code}"));
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => {
                return Err(format!(
                    "the connection failed before closing with {code}: {e}"
                ));
            }
            Ok(None) => {
                return Err(format!(
                    "the connection ended without a close frame; expected {code}"
                ));
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
