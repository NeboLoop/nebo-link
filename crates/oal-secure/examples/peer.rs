//! A host-side peer for testing another implementation of OAL's end-to-end
//! encryption (a client SDK in another language) against this one.
//!
//! ```sh
//! cargo run -p oal-secure --example peer -- <key dir> <host id> <code>
//! ```
//!
//! It prints `ws://127.0.0.1:<port>` and the host's public key, then serves
//! WebSockets:
//!
//! - `/oal/pair/<nameplate>`: pairing (spec section 17.5) with `<code>`, then
//!   `host/pair` inside it, answered as a host answers it. The connection then
//!   stays open as the new device's session.
//! - any other path: a session (section 17.2). Message 2's payload is
//!   `{"protocol":"0.1","device":{"id","name"}}`, or, when the client's range
//!   leaves out 0.1, `{"error":{"code":-33001,"message"}}` and close 4002.
//!
//! On a session every frame is answered: a request `{"id","method":"echo",
//! "params"}` gets `params` as its result; `{"method":"big","params":{"bytes"}}`
//! gets a result `{"data":"xxx…"}` of that many characters (a frame split
//! across Noise messages); `{"method":"unpair"}` revokes the device, which ends
//! its sessions with 4003. Anything else is sent back as `{"echo": frame}`.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, ready};

use futures::{Sink, SinkExt, Stream};
use oal_secure::{Error, KeyStore, PairingCode, PublicKey, Side, accept, pair};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Callback, ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [dir, host_id, code] = args.as_slice() else {
        eprintln!("usage: peer <key dir> <host id> <code>");
        std::process::exit(2);
    };
    let store = KeyStore::open(dir).expect("key store");
    let code = PairingCode::parse(code).expect("a pairing code");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    println!("ws://{}", listener.local_addr().expect("address"));
    println!("{}", store.public_key());
    let host = Arc::new(Host {
        store,
        id: host_id.clone(),
        code,
        next_device: AtomicU64::new(0),
        pairing: Mutex::new(()),
    });
    while let Ok((tcp, _)) = listener.accept().await {
        let host = host.clone();
        tokio::spawn(async move {
            let (path_tx, path_rx) = std::sync::mpsc::channel();
            let Ok(ws) = tokio_tungstenite::accept_hdr_async(tcp, Path(path_tx)).await else { return };
            let path = path_rx.recv().unwrap_or_default();
            let (ws, closer) = Ws::new(ws);
            let result = if path.starts_with("/oal/pair/") { host.pairing(ws).await } else { host.session(ws).await };
            if let Err((code, reason)) = result {
                closer.close(code, &reason).await;
            }
        });
    }
}

struct Host {
    store: KeyStore,
    id: String,
    code: PairingCode,
    next_device: AtomicU64,
    /// One pairing at a time.
    pairing: Mutex<()>,
}

type Session = oal_secure::Session<Ws>;

impl Host {
    fn info(&self) -> Value {
        json!({
            "host": { "id": self.id, "name": "OAL peer", "publicKey": self.store.public_key().to_string() },
            "software": { "name": "oal-secure peer", "version": env!("CARGO_PKG_VERSION") },
            "protocol": { "min": "0.1", "max": "0.1" },
            "acp": { "protocolVersion": 1 },
            "runtimes": [],
            "maxFrameBytes": oal_secure::DEFAULT_MAX_FRAME,
            "attachments": { "schemes": [], "maxBytes": 0 }
        })
    }

    async fn pairing(&self, ws: Ws) -> Result<(), (u16, String)> {
        let _one = self.pairing.lock().await;
        let mut p = pair(ws, &self.code, &self.store, Side::Host).await.map_err(refused)?;
        let request: Value = match p.recv().await.map_err(refused)? {
            Some(frame) => serde_json::from_slice(&frame).map_err(|_| (1002, "not JSON".to_owned()))?,
            None => return Ok(()),
        };
        let name = request["params"]["device"]["name"].as_str().unwrap_or("Device").to_owned();
        let claimed: PublicKey = request["params"]["device"]["publicKey"]
            .as_str()
            .and_then(|k| k.parse().ok())
            .ok_or((4001, "no device key".to_owned()))?;
        if claimed != p.peer_key() {
            let refusal = json!({ "jsonrpc": "2.0", "id": request["id"], "error": {
                "code": -33003, "message": "That code didn't work. Get a new one on the computer." } });
            let _ = p.send(refusal.to_string().as_bytes()).await;
            return Err((4001, "Pairing refused.".to_owned()));
        }
        let device = format!("d-{}", self.next_device.fetch_add(1, Ordering::Relaxed) + 1);
        let result = json!({ "jsonrpc": "2.0", "id": request["id"], "result": {
            "protocol": "0.1",
            "device": { "id": device, "name": name, "token": "unused-on-encrypted-connections" },
            "info": self.info(),
        } });
        p.send(result.to_string().as_bytes()).await.map_err(refused)?;
        let session = p.finish(&claimed, &device, &name, &self.id).map_err(refused)?;
        self.serve(session).await
    }

    async fn session(&self, ws: Ws) -> Result<(), (u16, String)> {
        let incoming = accept(ws, &self.store, &self.id).await.map_err(refused)?;
        let hello: Value = serde_json::from_slice(incoming.hello()).unwrap_or_default();
        let speaks = |v: &Value| v.as_str() == Some("0.1");
        let min = &hello["protocol"]["min"];
        let max = &hello["protocol"]["max"];
        let fits = speaks(min) || speaks(max) || (min.as_str() < Some("0.1") && max.as_str() > Some("0.1"));
        if !fits {
            let error = json!({ "error": { "code": -33001,
                "message": "This app speaks another version of OAL and this computer speaks 0.1. Update the app." } });
            let _ = incoming.finish(error.to_string().as_bytes()).await.map_err(refused)?;
            return Err((4002, "No common protocol version.".to_owned()));
        }
        let peer = incoming.peer().clone();
        let reply = json!({ "protocol": "0.1", "device": { "id": peer.id, "name": peer.name } });
        let session = incoming.finish(reply.to_string().as_bytes()).await.map_err(refused)?;
        self.serve(session).await
    }

    async fn serve(&self, mut session: Session) -> Result<(), (u16, String)> {
        loop {
            let Some(frame) = session.recv().await.map_err(refused)? else { return Ok(()) };
            let frame: Value = serde_json::from_slice(&frame).map_err(|_| (1002, "not JSON".to_owned()))?;
            let answer = match frame["method"].as_str() {
                Some("echo") => json!({ "jsonrpc": "2.0", "id": frame["id"], "result": frame["params"] }),
                Some("big") => {
                    let bytes = frame["params"]["bytes"].as_u64().unwrap_or(0) as usize;
                    json!({ "jsonrpc": "2.0", "id": frame["id"], "result": { "data": "x".repeat(bytes) } })
                }
                Some("unpair") => {
                    let key = session.peer().public_key;
                    self.store.revoke(&key).map_err(refused)?;
                    continue;
                }
                _ => json!({ "echo": frame }),
            };
            session.send(answer.to_string().as_bytes()).await.map_err(refused)?;
        }
    }
}

fn refused(e: Error) -> (u16, String) {
    (e.close_code(), e.to_string())
}

struct Path(std::sync::mpsc::Sender<String>);

/// Reports the upgrade's path, and selects the `oal` subprotocol when the
/// client offers it, as a host must (spec section 4.1).
impl Callback for Path {
    fn on_request(self, req: &Request, mut resp: Response) -> Result<Response, ErrorResponse> {
        let _ = self.0.send(req.uri().path().to_owned());
        let offered = req
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.split(',').any(|p| p.trim() == "oal"));
        if offered {
            resp.headers_mut().insert("sec-websocket-protocol", "oal".parse().expect("header"));
        }
        Ok(resp)
    }
}

type Socket = WebSocketStream<tokio::net::TcpStream>;

/// A WebSocket as an `oal_secure::Transport`, plus a way to close it with a
/// code once a session has taken it.
struct Ws {
    socket: Arc<Mutex<Option<Socket>>>,
    inner: Option<Socket>,
}

struct Closer(Arc<Mutex<Option<Socket>>>);

impl Closer {
    async fn close(&self, code: u16, reason: &str) {
        if let Some(mut socket) = self.0.lock().await.take() {
            let frame = CloseFrame { code: code.into(), reason: reason.to_owned().into() };
            let _ = socket.send(Message::Close(Some(frame))).await;
        }
    }
}

impl Ws {
    fn new(socket: Socket) -> (Self, Closer) {
        let shared = Arc::new(Mutex::new(None));
        (Ws { socket: shared.clone(), inner: Some(socket) }, Closer(shared))
    }

    fn inner(&mut self) -> &mut Socket {
        self.inner.as_mut().expect("the socket")
    }
}

/// When the session drops the transport, the closer gets the socket back.
impl Drop for Ws {
    fn drop(&mut self) {
        if let (Some(socket), Ok(mut slot)) = (self.inner.take(), self.socket.try_lock()) {
            *slot = Some(socket);
        }
    }
}

impl Stream for Ws {
    type Item = io::Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return Poll::Ready(match ready!(Pin::new(self.inner()).poll_next(cx)) {
                Some(Ok(Message::Binary(b))) => Some(Ok(b.to_vec())),
                Some(Ok(Message::Text(_))) => Some(Err(io::Error::new(io::ErrorKind::InvalidData, "a text message"))),
                Some(Ok(Message::Close(_))) | None => None,
                Some(Ok(_)) => continue,
                Some(Err(e)) => Some(Err(io::Error::other(e))),
            });
        }
    }
}

impl Sink<Vec<u8>> for Ws {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner()).poll_ready(cx).map_err(io::Error::other)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> io::Result<()> {
        Pin::new(self.inner()).start_send(Message::Binary(item.into())).map_err(io::Error::other)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner()).poll_flush(cx).map_err(io::Error::other)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner()).poll_close(cx).map_err(io::Error::other)
    }
}
