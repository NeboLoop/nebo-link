//! Pairing and devices beyond the recorded examples: plaintext is refused,
//! a wrong code fails and counts against the code, a code works once, and an
//! unpaired device's connections close with 4003 and it can't come back.

mod common;

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, ready};

use futures::{Sink, SinkExt, Stream, StreamExt};
use oal_secure::{KeyStore, PairingCode, PublicKey, Side};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn dial(port: u16, fingerprint: &str) -> Socket {
    let connector = tokio_tungstenite::Connector::Rustls(Arc::new(oal_conformance::pinned::config(fingerprint)));
    let (ws, _) = tokio_tungstenite::connect_async_tls_with_config(format!("wss://127.0.0.1:{port}/oal"), None, false, Some(connector))
        .await
        .unwrap();
    ws
}

/// A WebSocket as `oal_secure`'s transport, keeping the close code.
struct Ws {
    socket: Socket,
    closed: Arc<Mutex<Option<u16>>>,
}

impl Stream for Ws {
    type Item = io::Result<Vec<u8>>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return Poll::Ready(match ready!(Pin::new(&mut self.socket).poll_next(cx)) {
                Some(Ok(Message::Binary(b))) => Some(Ok(b.to_vec())),
                Some(Ok(Message::Close(frame))) => {
                    *self.closed.lock().unwrap() = frame.map(|f| u16::from(f.code));
                    None
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => Some(Err(io::Error::other(e))),
                None => None,
            });
        }
    }
}

impl Sink<Vec<u8>> for Ws {
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

fn transport(socket: Socket) -> (Ws, Arc<Mutex<Option<u16>>>) {
    let closed = Arc::new(Mutex::new(None));
    (Ws { socket, closed: closed.clone() }, closed)
}

/// Pairs `store` with the host at `port` using `code`; the host's answer.
async fn pair(port: u16, fingerprint: &str, store: &KeyStore, code: &str) -> Result<Value, Option<u16>> {
    let (ws, closed) = transport(dial(port, fingerprint).await);
    let code = PairingCode::parse(code).unwrap();
    let mut pairing = match oal_secure::pair(ws, &code, store, Side::Client).await {
        Ok(pairing) => pairing,
        Err(_) => return Err(*closed.lock().unwrap()),
    };
    let request = json!({ "jsonrpc": "2.0", "id": 1, "method": "host/pair", "params": {
        "protocol": { "min": "0.1", "max": "0.1" }, "client": { "name": "test", "version": "0" },
        "code": code.to_string(), "device": { "name": "Test phone", "publicKey": store.public_key().to_string() } } });
    pairing.send(request.to_string().as_bytes()).await.unwrap();
    let answer: Value = match pairing.recv().await {
        Ok(Some(bytes)) => serde_json::from_slice(&bytes).unwrap(),
        _ => return Err(*closed.lock().unwrap()),
    };
    let host = &answer["result"]["info"]["host"];
    let key: PublicKey = host["publicKey"].as_str().unwrap().parse().unwrap();
    pairing
        .finish(&key, host["id"].as_str().unwrap(), host["name"].as_str().unwrap(), answer["result"]["device"]["id"].as_str().unwrap())
        .unwrap();
    Ok(answer)
}

#[tokio::test]
async fn plaintext_is_refused_in_words_then_closed() {
    let dir = tempfile::tempdir().unwrap();
    let oal = common::host(dir.path()).await;
    let lan = oal_host::lan::serve(oal.clone(), "127.0.0.1:0".parse().unwrap(), dir.path(), false).await.unwrap();
    let mut ws = dial(lan.addr.port(), &lan.fingerprint).await;
    let hello = json!({ "jsonrpc": "2.0", "id": 1, "method": "host/hello", "params": { "protocol": { "min": "0.1", "max": "0.1" } } });
    ws.send(Message::text(hello.to_string())).await.unwrap();
    let refused: Value = match ws.next().await {
        Some(Ok(Message::Text(text))) => serde_json::from_str(&text).unwrap(),
        other => panic!("expected the refusal, got {other:?}"),
    };
    assert_eq!(refused["error"]["code"], -33002);
    assert_eq!(refused["error"]["message"], "Test Host takes encrypted connections only. Update the app.");
    match ws.next().await {
        Some(Ok(Message::Close(Some(frame)))) => assert_eq!(u16::from(frame.code), 4001),
        other => panic!("expected close 4001, got {other:?}"),
    }
}

#[tokio::test]
async fn a_code_pairs_once_and_an_unpaired_device_is_gone() {
    let dir = tempfile::tempdir().unwrap();
    let oal = common::host(dir.path()).await;
    let lan = oal_host::lan::serve(oal.clone(), "127.0.0.1:0".parse().unwrap(), dir.path(), false).await.unwrap();
    let port = lan.addr.port();
    let code = oal.pairing_code().await.unwrap().to_string();

    // A wrong code fails at the host (4001) and nothing is paired.
    let wrong = format!("{}-ZZZZ", &code[..4]);
    let wrong = if wrong == code { format!("{}-YYYY", &code[..4]) } else { wrong };
    let stranger = KeyStore::open(dir.path().join("stranger")).unwrap();
    assert_eq!(pair(port, &lan.fingerprint, &stranger, &wrong).await.unwrap_err(), Some(4001));
    assert!(oal.devices().is_empty());

    // The right code pairs, once.
    let phone = KeyStore::open(dir.path().join("phone")).unwrap();
    let answer = pair(port, &lan.fingerprint, &phone, &code).await.unwrap();
    let device = answer["result"]["device"]["id"].as_str().unwrap().to_owned();
    assert_eq!(oal.devices().len(), 1);
    assert_eq!(oal.devices()[0].key, phone.public_key().to_string());
    let late = KeyStore::open(dir.path().join("late")).unwrap();
    assert_eq!(pair(port, &lan.fingerprint, &late, &code).await.unwrap_err(), Some(4001), "a code works once");

    // The device connects, unpairs itself: its connection closes with 4003,
    // and it can't open another.
    let host = phone.peers().into_iter().find(|p| p.side == Side::Host).unwrap();
    let (ws, closed) = transport(dial(port, &lan.fingerprint).await);
    let hello = json!({ "protocol": { "min": "0.1", "max": "0.1" }, "client": { "name": "test", "version": "0" } });
    let (mut session, reply) = oal_secure::connect(ws, &phone, &host, hello.to_string().as_bytes()).await.unwrap();
    let reply: Value = serde_json::from_slice(&reply).unwrap();
    assert_eq!(reply["device"]["id"], device.as_str());
    let unpair = json!({ "jsonrpc": "2.0", "id": 7, "method": "host/unpair", "params": { "deviceId": device } });
    session.send(unpair.to_string().as_bytes()).await.unwrap();
    let answer: Value = serde_json::from_slice(&session.recv().await.unwrap().unwrap()).unwrap();
    assert_eq!(answer, json!({ "jsonrpc": "2.0", "id": 7, "result": {} }));
    assert!(session.recv().await.map(|f| f.is_none()).unwrap_or(true));
    assert_eq!(*closed.lock().unwrap(), Some(4003));
    assert!(oal.devices().is_empty());
    let (ws, closed) = transport(dial(port, &lan.fingerprint).await);
    let again = oal_secure::connect(ws, &phone, &host, hello.to_string().as_bytes()).await;
    assert!(again.is_err(), "an unpaired device's handshake fails");
    let _ = closed;
}
