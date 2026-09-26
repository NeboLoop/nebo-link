//! A host's side of the relay: its tunnel ([`crate::RelayClient::host`]).
//!
//! The tunnel yields [`HostEvent`]s: the relay's list of paired clients when
//! it comes up, pairings the operator removes while it runs, and every client
//! connection. A [`ClientConn`] carries one client's WebSocket messages,
//! whole and unchanged, plus the client key the relay verified that
//! connection holds. That is the seam for end-to-end encryption: `oal-secure`
//! runs its pairing handshake and Noise sessions over the conn's binary
//! messages, and a host can check the static key it authenticates against
//! [`ClientConn::client_key`].
//!
//! Pairing: the host makes the code. It gets a nameplate from the relay
//! ([`HostHandle::nameplate`]), or registers the one it chose, adds the
//! secret half itself (`oal_secure::PairingCode::generate(Some(nameplate))`)
//! and shows the code. The secret never goes to the relay. The client's
//! pairing connection arrives as a [`ClientConn`] with
//! [`ClientConn::nameplate`] set; when `host/pair` succeeds, the host calls
//! [`HostHandle::paired`] before answering, so the relay lets that client
//! through from then on. The host checks the code and counts failures
//! against it; the relay only expires nameplates.
//!
//! A host whose OAL server is already a WebSocket endpoint can hand each
//! conn to [`forward`] instead of reading it itself.
//!
//! One tunnel is one connection: when [`HostTunnel::next`] returns `None` the
//! tunnel is gone, and the host reconnects (with backoff) by calling
//! [`crate::RelayClient::host`] again. Pairings live on the relay and survive.

use std::collections::HashMap;
use std::future::poll_fn;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_util::sync::CancellationToken;

use crate::client::{Error, WsStream};
use crate::pump::{self, StreamRead, StreamWrite, Traffic};
use crate::wire::{
    AgentPresence, Frame, HostToRelay, Message, Open, PairedClient, RelayToHost,
    normalize_nameplate,
};
use crate::wsio::WsIo;

/// How long the relay has to name a stream it opened.
const OPEN_WAIT: Duration = Duration::from_secs(10);
/// How long the relay has to answer a control request.
const REQUEST_WAIT: Duration = Duration::from_secs(10);

/// What happens on a host's tunnel.
#[derive(Debug)]
pub enum HostEvent {
    /// The tunnel is up. `pairings` is every client the relay lets through
    /// to this host; reconcile it with the host's own devices
    /// ([`HostHandle::paired`], [`HostHandle::unpair`]).
    Registered {
        host_id: String,
        pairings: Vec<PairedClient>,
    },
    /// The relay's operator removed a client's pairing; its connections are
    /// being closed with 4003.
    Unpaired { client_key: String },
    /// A client connected: a paired one, or one pairing
    /// ([`ClientConn::nameplate`] set).
    Client(ClientConn),
}

/// A nameplate the relay holds for this host: the first half of a pairing
/// code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nameplate {
    /// Four characters, normalized.
    pub nameplate: String,
    /// RFC 3339. Pairing connections through it are refused after this.
    pub expires_at: String,
}

enum Reply {
    Nameplate(Nameplate),
    Done,
}

/// A host's open tunnel.
pub struct HostTunnel {
    events: mpsc::Receiver<HostEvent>,
    handle: HostHandle,
}

impl HostTunnel {
    /// The next event, or `None` once the tunnel is gone. Keep calling it:
    /// answers to [`HostHandle`] requests arrive on the same connection and
    /// wait behind unread events.
    pub async fn next(&mut self) -> Option<HostEvent> {
        self.events.recv().await
    }

    /// A handle for requests over this tunnel. Cheap to clone.
    pub fn handle(&self) -> HostHandle {
        self.handle.clone()
    }
}

/// Requests a host makes of the relay over its tunnel.
#[derive(Clone)]
pub struct HostHandle {
    inner: Arc<Inner>,
}

struct Inner {
    ctl: mpsc::Sender<HostToRelay>,
    ctl_rx: Mutex<Option<mpsc::Receiver<HostToRelay>>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Reply, Error>>>>,
    next_request: AtomicU64,
    closed: CancellationToken,
}

impl HostHandle {
    /// Holds a nameplate for this host: `wanted` (the first half of a code
    /// this host, or a client, made) or a fresh one from the relay. Add the
    /// secret half here, never at the relay. Refused with `nameplate_taken`
    /// when another host holds `wanted`.
    pub async fn nameplate(&self, wanted: Option<&str>) -> Result<Nameplate, Error> {
        let wanted = match wanted {
            Some(w) => Some(normalize_nameplate(w).ok_or_else(|| Error::Refused {
                code: "bad_nameplate".into(),
                message: "A nameplate is the first 4 characters of a code.".into(),
            })?),
            None => None,
        };
        match self
            .request(|request| HostToRelay::NameplateRequest {
                request,
                nameplate: wanted,
            })
            .await?
        {
            Reply::Nameplate(n) => Ok(n),
            Reply::Done => Err(Error::Protocol(
                "the relay answered without a nameplate".into(),
            )),
        }
    }

    /// Tells the relay this host paired `client_key`, so the relay lets it
    /// through from now on. Call it when `host/pair` succeeds, and answer
    /// `host/pair` once it returns.
    pub async fn paired(&self, client_key: &str) -> Result<(), Error> {
        let client_key = client_key.to_owned();
        self.request(|request| HostToRelay::Paired {
            request,
            client_key,
        })
        .await
        .map(|_| ())
    }

    /// Publishes the host's agents    /// Publishes the host's agents and whether each is online, replacing the
    /// last list. Paired clients read it with [`crate::RelayClient::presence`];
    /// the relay's operator can see it too, so publish ids, not secrets.
    pub async fn publish_presence(&self, agents: Vec<AgentPresence>) -> Result<(), Error> {
        self.send(HostToRelay::Presence { agents }).await
    }

    /// Removes a client's pairing with this host (OAL `host/unpair`); the
    /// relay closes its connections with 4003 and refuses new ones.
    pub async fn unpair(&self, client_key: &str) -> Result<(), Error> {
        let client_key = client_key.to_owned();
        self.request(|request| HostToRelay::Unpair {
            request,
            client_key,
        })
        .await
        .map(|_| ())
    }

    /// Closes the tunnel.
    pub fn close(&self) {
        self.inner.closed.cancel();
    }

    async fn request(&self, msg: impl FnOnce(u64) -> HostToRelay) -> Result<Reply, Error> {
        let request = self.inner.next_request.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.inner
            .pending
            .lock()
            .expect("pending lock")
            .insert(request, tx);
        let answer = async {
            self.send(msg(request)).await?;
            rx.await.map_err(|_| Error::Closed)?
        };
        let result = tokio::time::timeout(REQUEST_WAIT, answer)
            .await
            .unwrap_or(Err(Error::Closed));
        self.inner
            .pending
            .lock()
            .expect("pending lock")
            .remove(&request);
        result
    }

    async fn send(&self, msg: HostToRelay) -> Result<(), Error> {
        if self.inner.closed.is_cancelled() {
            return Err(Error::Closed);
        }
        self.inner.ctl.send(msg).await.map_err(|_| Error::Closed)
    }
}

/// One client's connection, carried through the relay.
pub struct ClientConn {
    /// The client's X25519 static key, as the relay verified this
    /// connection holds it.
    pub client_key: String,
    /// Set on a pairing connection (`/oal/pair/<nameplate>`): the nameplate
    /// it came through. The client is not paired yet; its first messages are
    /// the pairing handshake (spec section 17.5), or `host/pair` in 0.1.
    pub nameplate: Option<String>,
    pub tx: ClientSender,
    pub rx: ClientReceiver,
}

impl std::fmt::Debug for ClientConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientConn")
            .field("client_key", &self.client_key)
            .field("nameplate", &self.nameplate)
            .finish_non_exhaustive()
    }
}

/// Sends WebSocket messages to a client.
pub struct ClientSender(StreamWrite);

impl ClientSender {
    /// Sends one message. A [`Message::Close`] closes the client's WebSocket
    /// with its code, and ends this conn.
    pub async fn send(&mut self, msg: Message) -> std::io::Result<()> {
        let closing = matches!(msg, Message::Close { .. });
        self.0.send(Frame::Message(msg)).await?;
        if closing {
            self.0.close().await?;
        }
        Ok(())
    }
}

/// Receives a client's WebSocket messages.
pub struct ClientReceiver(StreamRead);

impl ClientReceiver {
    /// The next message; `None` once the stream has ended. A client that
    /// closes sends [`Message::Close`] with its code (1006: it went away
    /// without one).
    pub async fn recv(&mut self) -> Option<std::io::Result<Message>> {
        loop {
            match self.0.next().await? {
                Ok(Frame::Message(m)) => return Some(Ok(m)),
                Ok(Frame::Open(_)) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Carries a client conn to a local OAL WebSocket (`ws://127.0.0.1:7878/oal`)
/// until either side closes, passing messages and close codes through
/// unchanged. For hosts whose OAL server is a WebSocket endpoint.
pub async fn forward(conn: ClientConn, url: &str) -> Result<(), Error> {
    let mut request = url.into_client_request()?;
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "oal".parse().expect("header"));
    let (ws, _) = tokio_tungstenite::connect_async(request).await?;
    let (_kill, kill) = oneshot::channel();
    let summary = pump::pump(ws, conn.rx.0, conn.tx.0, kill, Arc::new(Traffic::default())).await;
    tracing::debug!(
        code = ?summary.end.code(),
        end = ?summary.end,
        from_host_messages = summary.from_ws_messages,
        from_host_bytes = summary.from_ws_bytes,
        to_host_messages = summary.to_ws_messages,
        to_host_bytes = summary.to_ws_bytes,
        "forwarded client connection ended"
    );
    Ok(())
}

pub(crate) fn yamux_config() -> yamux::Config {
    yamux::Config::default()
}

/// Runs the host's end of a tunnel the relay accepted.
pub(crate) fn start(ws: WsStream, max_message: usize) -> HostTunnel {
    let (events_tx, events) = mpsc::channel(64);
    let (ctl, ctl_rx) = mpsc::channel(16);
    let inner = Arc::new(Inner {
        ctl,
        ctl_rx: Mutex::new(Some(ctl_rx)),
        pending: Mutex::new(HashMap::new()),
        next_request: AtomicU64::new(1),
        closed: CancellationToken::new(),
    });
    let conn = yamux::Connection::new(WsIo::new(ws), yamux_config(), yamux::Mode::Server);
    tokio::spawn(drive(conn, events_tx, inner.clone(), max_message));
    HostTunnel {
        events,
        handle: HostHandle { inner },
    }
}

async fn drive(
    mut conn: yamux::Connection<WsIo<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>,
    events: mpsc::Sender<HostEvent>,
    inner: Arc<Inner>,
    max_message: usize,
) {
    loop {
        let next = tokio::select! {
            next = poll_fn(|cx| conn.poll_next_inbound(cx)) => next,
            _ = inner.closed.cancelled() => {
                let _ = tokio::time::timeout(
                    Duration::from_secs(2),
                    poll_fn(|cx| conn.poll_close(cx)),
                ).await;
                break;
            }
        };
        match next {
            Some(Ok(stream)) => {
                tokio::spawn(accept(stream, events.clone(), inner.clone(), max_message));
            }
            Some(Err(e)) => {
                tracing::debug!(error = %e, "relay tunnel ended");
                break;
            }
            None => break,
        }
    }
    inner.closed.cancel();
    // Whatever was waiting on this tunnel will not be answered.
    inner.pending.lock().expect("pending lock").clear();
}

async fn accept(
    stream: yamux::Stream,
    events: mpsc::Sender<HostEvent>,
    inner: Arc<Inner>,
    max_message: usize,
) {
    let (mut rx, tx) = pump::framed(stream, max_message);
    let open = match tokio::time::timeout(OPEN_WAIT, rx.next()).await {
        Ok(Some(Ok(Frame::Open(open)))) => open,
        _ => return,
    };
    match open {
        Open::Control => control(rx, tx, events, inner).await,
        Open::Client {
            client_key,
            nameplate,
        } => {
            let conn = ClientConn {
                client_key,
                nameplate,
                tx: ClientSender(tx),
                rx: ClientReceiver(rx),
            };
            let _ = events.send(HostEvent::Client(conn)).await;
        }
    }
}

async fn control(
    mut rx: StreamRead,
    mut tx: StreamWrite,
    events: mpsc::Sender<HostEvent>,
    inner: Arc<Inner>,
) {
    let Some(mut outgoing) = inner.ctl_rx.lock().expect("ctl lock").take() else {
        return; // a second control stream; the first one owns the channel
    };
    loop {
        tokio::select! {
            frame = rx.next() => {
                let Some(Ok(Frame::Message(Message::Text(text)))) = frame else { break };
                let Ok(msg) = serde_json::from_str::<RelayToHost>(&text) else {
                    tracing::debug!("relay sent a control message this version does not know");
                    continue;
                };
                let event = match msg {
                    RelayToHost::Registered { host_id, pairings } => HostEvent::Registered { host_id, pairings },
                    RelayToHost::Unpaired { client_key } => HostEvent::Unpaired { client_key },
                    RelayToHost::Nameplate { request, nameplate, expires_at } => {
                        answer(&inner, request, Ok(Reply::Nameplate(Nameplate { nameplate, expires_at })));
                        continue;
                    }
                    RelayToHost::Done { request } => {
                        answer(&inner, request, Ok(Reply::Done));
                        continue;
                    }
                    RelayToHost::Error { request, code, message } => {
                        answer(&inner, request, Err(Error::Refused { code, message }));
                        continue;
                    }
                };
                if events.send(event).await.is_err() {
                    break;
                }
            }
            out = outgoing.recv() => {
                let Some(msg) = out else { break };
                let text = serde_json::to_string(&msg).expect("control messages serialize");
                if tx.send(Frame::Message(Message::Text(text))).await.is_err() {
                    break;
                }
            }
            _ = inner.closed.cancelled() => break,
        }
    }
    inner.closed.cancel();
}

fn answer(inner: &Inner, request: u64, result: Result<Reply, Error>) {
    if let Some(tx) = inner.pending.lock().expect("pending lock").remove(&request) {
        let _ = tx.send(result);
    }
}
