//! The relay's OAL endpoints: host tunnels, client connections, pairing and
//! presence.

use std::future::poll_fn;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use hyper::body::Incoming;
use hyper::header::{
    CONNECTION, HeaderValue, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_PROTOCOL, UPGRADE,
};
use hyper::upgrade::OnUpgrade;
use hyper::{Request, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::{Role as WsRole, WebSocketConfig};
use tokio_util::sync::CancellationToken;

use super::metrics::{dec, inc};
use super::store::RegisterError;
use super::{Resp, State, json, query, refuse};
use crate::auth::{self, Role, Target, decode_key};
use crate::client::{HOST_ID_HEADER, HOST_KEY_HEADER};
use crate::host::yamux_config;
use crate::pump::{self, CLOSE_HOST_GONE, StreamRead, StreamWrite};
use crate::time::{now, rfc3339};
use crate::wire::{
    AgentPresence, CODE_ALPHABET, Frame, HostToRelay, Message, NAMEPLATE_LEN, Open, RelayToHost,
    normalize_nameplate,
};
use crate::wsio::WsIo;

/// OAL's close code for a device that was unpaired (spec section 4.3).
pub(crate) const CLOSE_UNPAIRED: u16 = 4003;
/// How long a host has to accept a stream the relay opens.
const OPEN_WAIT: Duration = Duration::from_secs(10);
/// Failed pairing attempts allowed per source address per minute (OAL spec
/// section 6.2). Behind a proxy every client shares the proxy's address, so
/// the limit becomes relay-wide, which is what the spec asks of a host.
const PAIR_FAILURES_PER_MINUTE: u32 = 10;
/// The most agents a host may publish, and the longest id.
const MAX_AGENTS: usize = 256;
const MAX_AGENT_ID: usize = 128;

type OpenRequest = oneshot::Sender<Result<yamux::Stream, yamux::ConnectionError>>;

/// A host with its tunnel open.
pub(crate) struct HostSession {
    id: String,
    /// Tells this session apart from the one that replaces it on reconnect.
    conn_id: u64,
    open: mpsc::Sender<OpenRequest>,
    control: mpsc::Sender<RelayToHost>,
    agents: std::sync::Mutex<Vec<AgentPresence>>,
    close: CancellationToken,
}

impl HostSession {
    async fn open_stream(&self) -> Option<yamux::Stream> {
        let (tx, rx) = oneshot::channel();
        self.open.send(tx).await.ok()?;
        tokio::time::timeout(OPEN_WAIT, rx).await.ok()?.ok()?.ok()
    }

    pub(crate) fn agents(&self) -> Vec<AgentPresence> {
        self.agents.lock().map(|a| a.clone()).unwrap_or_default()
    }

    pub(crate) fn close_tunnel(&self) {
        self.close.cancel();
    }
}

/// A client connection being relayed.
pub(crate) struct ClientEntry {
    host_id: String,
    client_key: String,
    kill: Option<oneshot::Sender<u16>>,
}

impl State {
    pub(crate) fn host_session(&self, id: &str) -> Option<Arc<HostSession>> {
        self.hosts.lock().ok()?.get(id).cloned()
    }

    /// Closes the matching client connections with `code`.
    pub(crate) fn kill_clients(&self, host_id: Option<&str>, client_key: Option<&str>, code: u16) {
        let Ok(mut clients) = self.clients.lock() else {
            return;
        };
        for entry in clients.values_mut() {
            let host_matches = host_id.is_none_or(|h| h == entry.host_id);
            let key_matches = client_key.is_none_or(|k| k == entry.client_key);
            if host_matches
                && key_matches
                && let Some(kill) = entry.kill.take()
            {
                let _ = kill.send(code);
            }
        }
    }

    /// Tells a host (if its tunnel is open) that a pairing is gone.
    pub(crate) async fn notify_unpaired(&self, host_id: &str, client_key: &str) {
        if let Some(session) = self.host_session(host_id) {
            let _ = session
                .control
                .send(RelayToHost::Unpaired {
                    client_key: client_key.to_owned(),
                })
                .await;
        }
    }

    /// Holds a nameplate for `host_id`: `wanted` if it is free (or already
    /// this host's), or a fresh one. `(nameplate, expires_at)`, or `None` when
    /// `wanted` belongs to another host. The relay makes only the nameplate;
    /// the secret half of the code is the host's and never comes here.
    pub(crate) fn hold_nameplate(
        &self,
        host_id: &str,
        wanted: Option<&str>,
    ) -> Result<Option<(String, String)>, rusqlite::Error> {
        let now = now();
        let expires = now + self.nameplate_ttl;
        let held = match wanted {
            Some(nameplate) => self
                .store
                .hold_nameplate(nameplate, host_id, expires, now)?
                .then(|| nameplate.to_owned()),
            None => {
                // 20 bits: a free one is found at once unless the relay holds
                // hundreds of thousands.
                let mut found = None;
                for _ in 0..32 {
                    let nameplate = random_nameplate();
                    if self
                        .store
                        .hold_nameplate(&nameplate, host_id, expires, now)?
                    {
                        found = Some(nameplate);
                        break;
                    }
                }
                found
            }
        };
        if held.is_some() {
            inc(&self.metrics.nameplates_issued);
            tracing::info!(host = %host_id, expires_at = %rfc3339(expires), "nameplate held");
        }
        Ok(held.map(|n| (n, rfc3339(expires))))
    }

    fn pair_limited(&self, peer: &SocketAddr) -> bool {
        let minute = now() / 60;
        self.pair_failures
            .lock()
            .ok()
            .and_then(|m| m.get(&peer.ip()).copied())
            .is_some_and(|(m, n)| m == minute && n >= PAIR_FAILURES_PER_MINUTE)
    }

    fn pair_failed(&self, peer: &SocketAddr) {
        inc(&self.metrics.pairing_failures);
        let minute = now() / 60;
        if let Ok(mut m) = self.pair_failures.lock() {
            if m.len() > 10_000 {
                m.retain(|_, (at, _)| *at == minute);
            }
            let entry = m.entry(peer.ip()).or_insert((minute, 0));
            if entry.0 != minute {
                *entry = (minute, 0);
            }
            entry.1 += 1;
        }
    }

    /// Checks the request's proof of key possession; the verified key, or
    /// the refusal to send.
    fn authenticate(
        &self,
        req: &Request<Incoming>,
        role: Role,
        target: Target<'_>,
    ) -> Result<(String, std::collections::HashMap<String, String>), Box<Resp>> {
        let q = query(req);
        let (Some(key), Some(nonce), Some(proof)) = (q.get("key"), q.get("nonce"), q.get("proof"))
        else {
            inc(&self.metrics.auth_failures);
            return Err(Box::new(unauthenticated(role)));
        };
        if decode_key(key).is_none()
            || !auth::verify(&self.key, role, key, nonce, target, proof)
            || !self.nonces.redeem(nonce)
        {
            inc(&self.metrics.auth_failures);
            return Err(Box::new(unauthenticated(role)));
        }
        let key = key.clone();
        Ok((key, q))
    }

    fn revoked(&self, key: &str) -> Result<bool, Box<Resp>> {
        self.store
            .is_revoked(key)
            .map_err(|e| Box::new(store_error(e)))
    }
}

fn unauthenticated(role: Role) -> Resp {
    let message = match role {
        Role::Host => {
            "The relay could not verify this computer's key. Get a new challenge and try again."
        }
        Role::Client => {
            "The relay could not verify this device's key. Get a new challenge and try again."
        }
    };
    refuse(StatusCode::UNAUTHORIZED, "unauthenticated", message)
}

fn store_error(e: rusqlite::Error) -> Resp {
    tracing::error!(error = %e, "store error");
    refuse(
        StatusCode::SERVICE_UNAVAILABLE,
        "relay_unavailable",
        "The relay can't reach its store right now. Try again.",
    )
}

fn host_offline(id: &str) -> Resp {
    refuse(
        StatusCode::SERVICE_UNAVAILABLE,
        "host_offline",
        format!("{id} is offline."),
    )
}

fn not_paired(id: &str) -> Resp {
    refuse(
        StatusCode::FORBIDDEN,
        "not_paired",
        format!("This device isn't paired with {id}. Pair it again."),
    )
}

/// A nameplate no host holds (spec section 4.4).
fn unknown_nameplate() -> Resp {
    refuse(
        StatusCode::NOT_FOUND,
        "unknown_nameplate",
        "That code didn't work. Get a new one on the computer.",
    )
}

/// A pairing path that is not a nameplate: more than the code's first four
/// characters, or not code characters at all (spec section 4.4). Refused
/// before anything else, and never logged.
fn bad_nameplate() -> Resp {
    refuse(
        StatusCode::BAD_REQUEST,
        "bad_nameplate",
        "Only the first four characters of the code go to the relay.",
    )
}

fn random_nameplate() -> String {
    let mut raw = [0u8; NAMEPLATE_LEN];
    getrandom::getrandom(&mut raw).expect("the OS random source failed");
    raw.iter()
        .map(|b| CODE_ALPHABET[usize::from(b & 31)] as char)
        .collect()
}

fn valid_host_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// Short, stable handle for a key in logs.
pub(crate) fn fp(key: &str) -> String {
    key.chars().take(8).collect()
}

/// Checks the request is a WebSocket upgrade and builds the 101 for it,
/// selecting the `oal` subprotocol when the client offered it (OAL spec
/// section 4.1).
fn upgrade(req: &mut Request<Incoming>) -> Result<(Resp, OnUpgrade), Box<Resp>> {
    let headers = req.headers();
    let has = |name: hyper::header::HeaderName, want: &str| {
        headers
            .get_all(name)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|v| v.trim().eq_ignore_ascii_case(want))
    };
    let key = headers.get("sec-websocket-key").cloned();
    let version_ok = headers
        .get("sec-websocket-version")
        .is_some_and(|v| v.as_bytes() == b"13");
    let (Some(key), true, true, true) = (
        key,
        has(UPGRADE, "websocket"),
        has(CONNECTION, "upgrade"),
        version_ok,
    ) else {
        return Err(Box::new(refuse(
            StatusCode::BAD_REQUEST,
            "websocket_required",
            "This endpoint takes a WebSocket.",
        )));
    };
    let oal = has(SEC_WEBSOCKET_PROTOCOL, "oal");
    let mut resp = Resp::new(Default::default());
    *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    let h = resp.headers_mut();
    h.insert(UPGRADE, HeaderValue::from_static("websocket"));
    h.insert(CONNECTION, HeaderValue::from_static("Upgrade"));
    h.insert(
        SEC_WEBSOCKET_ACCEPT,
        HeaderValue::from_str(&derive_accept_key(key.as_bytes()))
            .expect("base64 is a header value"),
    );
    if oal {
        h.insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static("oal"));
    }
    Ok((resp, hyper::upgrade::on(req)))
}

fn ws_config(max: usize) -> WebSocketConfig {
    WebSocketConfig::default()
        .max_message_size(Some(max))
        .max_frame_size(Some(max))
}

async fn accept_ws(
    on_upgrade: OnUpgrade,
    config: Option<WebSocketConfig>,
) -> Option<WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>> {
    let upgraded = on_upgrade.await.ok()?;
    Some(WebSocketStream::from_raw_socket(TokioIo::new(upgraded), WsRole::Server, config).await)
}

// ---------------------------------------------------------------- tunnels

/// `GET /oal/tunnel/<hostId>`: a host opens its tunnel.
pub(crate) async fn tunnel(state: Arc<State>, mut req: Request<Incoming>, host_id: String) -> Resp {
    if !valid_host_id(&host_id) {
        return refuse(
            StatusCode::BAD_REQUEST,
            "bad_host_id",
            "A host id is 1 to 64 letters, digits, '-', '_' or '.'.",
        );
    }
    let key = match state.authenticate(&req, Role::Host, Target::Host(&host_id)) {
        Ok((key, _)) => key,
        Err(resp) => return *resp,
    };
    match state.revoked(&key) {
        Ok(false) => {}
        Ok(true) => {
            return refuse(
                StatusCode::FORBIDDEN,
                "revoked",
                "This computer was removed from the relay.",
            );
        }
        Err(resp) => return *resp,
    }
    if !state.allow_hosts.is_empty() && !state.allow_hosts.contains(&key) {
        return refuse(
            StatusCode::FORBIDDEN,
            "not_allowed",
            "This relay doesn't accept this computer. Ask its operator to allow its key.",
        );
    }
    let (resp, on_upgrade) = match upgrade(&mut req) {
        Ok(ok) => ok,
        Err(resp) => return *resp,
    };
    match state.store.register_host(&host_id, &key, now()) {
        Ok(Ok(())) => {}
        Ok(Err(RegisterError::IdTaken)) => {
            return refuse(
                StatusCode::CONFLICT,
                "host_id_taken",
                format!("Another computer already uses the name {host_id} on this relay."),
            );
        }
        Ok(Err(RegisterError::KeyTaken(other))) => {
            return refuse(
                StatusCode::CONFLICT,
                "host_key_taken",
                format!("This computer is already registered on this relay as {other}."),
            );
        }
        Err(e) => return store_error(e),
    }
    tokio::spawn(run_tunnel(state, on_upgrade, host_id, key));
    resp
}

async fn run_tunnel(state: Arc<State>, on_upgrade: OnUpgrade, host_id: String, key: String) {
    let Some(ws) = accept_ws(on_upgrade, None).await else {
        return;
    };
    let conn = yamux::Connection::new(WsIo::new(ws), yamux_config(), yamux::Mode::Client);
    let (open, open_rx) = mpsc::channel(64);
    let (control, mut control_rx) = mpsc::channel(64);
    let close = state.shutdown.child_token();
    tokio::spawn(drive_tunnel(conn, open_rx, close.clone()));
    let session = Arc::new(HostSession {
        id: host_id.clone(),
        conn_id: state.next_id.fetch_add(1, Ordering::Relaxed),
        open,
        control,
        agents: std::sync::Mutex::new(Vec::new()),
        close: close.clone(),
    });
    let Some(stream) = session.open_stream().await else {
        close.cancel();
        return;
    };
    let (mut rx, mut tx) = pump::framed(stream, state.max_message);
    if tx.send(Frame::Open(Open::Control)).await.is_err() {
        close.cancel();
        return;
    }
    let pairings = match state.store.pairings(&host_id) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "store error");
            close.cancel();
            return;
        }
    };
    let pairing_count = pairings.len();
    if send_control(
        &mut tx,
        &RelayToHost::Registered {
            host_id: host_id.clone(),
            pairings,
        },
    )
    .await
    .is_err()
    {
        close.cancel();
        return;
    }
    let replaced = state
        .hosts
        .lock()
        .ok()
        .and_then(|mut h| h.insert(host_id.clone(), session.clone()));
    if let Some(old) = replaced {
        old.close.cancel();
    } else {
        inc(&state.metrics.hosts_online);
    }
    inc(&state.metrics.host_connections);
    tracing::info!(host = %host_id, key = %fp(&key), pairings = pairing_count, "host online");

    loop {
        tokio::select! {
            frame = rx.next() => {
                let Some(Ok(Frame::Message(Message::Text(text)))) = frame else { break };
                let Ok(msg) = serde_json::from_str::<HostToRelay>(&text) else {
                    tracing::debug!(host = %host_id, "host sent a control message this relay does not know");
                    continue;
                };
                if let Some(reply) = handle_control(&state, &session, msg).await
                    && send_control(&mut tx, &reply).await.is_err()
                {
                    break;
                }
            }
            out = control_rx.recv() => {
                let Some(msg) = out else { break };
                if send_control(&mut tx, &msg).await.is_err() {
                    break;
                }
            }
            _ = close.cancelled() => break,
        }
    }

    close.cancel();
    let removed = state.hosts.lock().ok().is_some_and(|mut h| {
        if h.get(&host_id)
            .is_some_and(|s| s.conn_id == session.conn_id)
        {
            h.remove(&host_id);
            true
        } else {
            false
        }
    });
    if removed {
        dec(&state.metrics.hosts_online);
        let _ = state.store.touch_host(&host_id, now());
        tracing::info!(host = %host_id, "host offline");
    }
}

async fn send_control(tx: &mut StreamWrite, msg: &RelayToHost) -> std::io::Result<()> {
    let text = serde_json::to_string(msg).expect("control messages serialize");
    tx.send(Frame::Message(Message::Text(text))).await
}

async fn handle_control(
    state: &State,
    session: &HostSession,
    msg: HostToRelay,
) -> Option<RelayToHost> {
    let refused = |request: u64, code: &str, message: &str| RelayToHost::Error {
        request,
        code: code.to_owned(),
        message: message.to_owned(),
    };
    let unavailable = |request: u64, e: rusqlite::Error| {
        tracing::error!(error = %e, "store error");
        refused(
            request,
            "relay_unavailable",
            "The relay can't do that right now. Try again.",
        )
    };
    match msg {
        HostToRelay::NameplateRequest { request, nameplate } => {
            let wanted = match nameplate.as_deref().map(normalize_nameplate) {
                None => None,
                Some(Some(n)) => Some(n),
                Some(None) => {
                    return Some(refused(
                        request,
                        "bad_nameplate",
                        "A nameplate is the first 4 characters of a code.",
                    ));
                }
            };
            Some(match state.hold_nameplate(&session.id, wanted.as_deref()) {
                Ok(Some((nameplate, expires_at))) => RelayToHost::Nameplate {
                    request,
                    nameplate,
                    expires_at,
                },
                Ok(None) => refused(
                    request,
                    "nameplate_taken",
                    "That code is already in use on this relay. Make a new one.",
                ),
                Err(e) => unavailable(request, e),
            })
        }
        HostToRelay::Paired {
            request,
            client_key,
        } => {
            if decode_key(&client_key).is_none() {
                return Some(refused(request, "bad_key", "That isn't a device key."));
            }
            Some(
                match state.store.add_pairing(&session.id, &client_key, now()) {
                    Ok(()) => {
                        inc(&state.metrics.pairings);
                        tracing::info!(host = %session.id, client = %fp(&client_key), "paired");
                        RelayToHost::Done { request }
                    }
                    Err(e) => unavailable(request, e),
                },
            )
        }
        HostToRelay::Unpair {
            request,
            client_key,
        } => Some(match state.store.unpair(&session.id, &client_key) {
            Ok(removed) => {
                if removed {
                    inc(&state.metrics.revocations);
                    tracing::info!(host = %session.id, client = %fp(&client_key), "host unpaired a client");
                }
                state.kill_clients(Some(&session.id), Some(&client_key), CLOSE_UNPAIRED);
                RelayToHost::Done { request }
            }
            Err(e) => unavailable(request, e),
        }),
        HostToRelay::Presence { mut agents } => {
            agents.truncate(MAX_AGENTS);
            agents.retain(|a| !a.id.is_empty() && a.id.len() <= MAX_AGENT_ID);
            if let Ok(mut current) = session.agents.lock() {
                *current = agents;
            }
            None
        }
    }
}

/// Owns a tunnel's yamux connection: opens streams on request and keeps the
/// session moving until it ends or `close` fires.
async fn drive_tunnel<T>(
    mut conn: yamux::Connection<T>,
    mut open_rx: mpsc::Receiver<OpenRequest>,
    close: CancellationToken,
) where
    T: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    let mut waiting: std::collections::VecDeque<OpenRequest> = Default::default();
    let ended = poll_fn(|cx| {
        loop {
            while let std::task::Poll::Ready(Some(req)) = open_rx.poll_recv(cx) {
                waiting.push_back(req);
            }
            if !waiting.is_empty()
                && let std::task::Poll::Ready(result) = conn.poll_new_outbound(cx)
            {
                if let Some(req) = waiting.pop_front() {
                    let _ = req.send(result);
                }
                continue;
            }
            return match conn.poll_next_inbound(cx) {
                // Hosts never open streams; one that does is ignored.
                std::task::Poll::Ready(Some(Ok(_))) => continue,
                std::task::Poll::Ready(Some(Err(e))) => std::task::Poll::Ready(Some(e)),
                std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                std::task::Poll::Pending => std::task::Poll::Pending,
            };
        }
    });
    tokio::select! {
        e = ended => {
            if let Some(e) = e {
                tracing::debug!(error = %e, "tunnel ended");
            }
        }
        _ = close.cancelled() => {
            let _ = tokio::time::timeout(Duration::from_secs(2), poll_fn(|cx| conn.poll_close(cx))).await;
        }
    }
    close.cancel();
}

// ---------------------------------------------------------------- clients

/// `GET /oal/hosts/<hostId>`: a paired client connects to a host.
pub(crate) async fn connect(
    state: Arc<State>,
    mut req: Request<Incoming>,
    host_id: String,
) -> Resp {
    let key = match state.authenticate(&req, Role::Client, Target::Connect(&host_id)) {
        Ok((key, _)) => key,
        Err(resp) => return *resp,
    };
    match state.revoked(&key) {
        Ok(false) => {}
        Ok(true) => {
            return refuse(
                StatusCode::FORBIDDEN,
                "revoked",
                "This device was removed from the relay.",
            );
        }
        Err(resp) => return *resp,
    }
    match state.store.pairing(&host_id, &key) {
        Ok(Some(_)) => {}
        Ok(None) => {
            inc(&state.metrics.refused_unpaired);
            return not_paired(&host_id);
        }
        Err(e) => return store_error(e),
    }
    let (resp, on_upgrade) = match upgrade(&mut req) {
        Ok(ok) => ok,
        Err(resp) => return *resp,
    };
    let Some(session) = state.host_session(&host_id) else {
        inc(&state.metrics.refused_offline);
        return host_offline(&host_id);
    };
    let Some(stream) = session.open_stream().await else {
        inc(&state.metrics.refused_offline);
        return host_offline(&host_id);
    };
    let (rx, mut tx) = pump::framed(stream, state.max_message);
    let open = Open::Client {
        client_key: key.clone(),
        nameplate: None,
    };
    if tx.send(Frame::Open(open)).await.is_err() {
        inc(&state.metrics.refused_offline);
        return host_offline(&host_id);
    }
    tokio::spawn(relay_client(state, on_upgrade, rx, tx, host_id, key, false));
    resp
}

/// `GET /oal/pair/<nameplate>`: a client opens a pairing connection to the
/// host that holds the nameplate. The relay sees only the nameplate; the rest
/// of the code travels inside the connection, which the relay never reads
/// (and which OAL 0.2 encrypts and binds to the whole code with CPace). The
/// relay decides nothing about the pairing: the host checks the code, counts
/// failures against it, and tells the relay whom it paired
/// ([`HostToRelay::Paired`]). A nameplate routes every attempt until it
/// expires.
pub(crate) async fn pair(
    state: Arc<State>,
    peer: SocketAddr,
    mut req: Request<Incoming>,
    raw: String,
) -> Resp {
    if state.pair_limited(&peer) {
        return refuse(
            StatusCode::TOO_MANY_REQUESTS,
            "too_many_attempts",
            "Too many pairing attempts. Wait a minute and try again.",
        );
    }
    let Some(nameplate) = normalize_nameplate(&raw) else {
        state.pair_failed(&peer);
        return bad_nameplate();
    };
    let key = match state.authenticate(&req, Role::Client, Target::Pair(&nameplate)) {
        Ok((key, _)) => key,
        Err(resp) => return *resp,
    };
    match state.revoked(&key) {
        Ok(false) => {}
        Ok(true) => {
            return refuse(
                StatusCode::FORBIDDEN,
                "revoked",
                "This device was removed from the relay.",
            );
        }
        Err(resp) => return *resp,
    }
    let (mut resp, on_upgrade) = match upgrade(&mut req) {
        Ok(ok) => ok,
        Err(resp) => return *resp,
    };
    let host_id = match state.store.nameplate_host(&nameplate, now()) {
        Ok(Some(id)) => id,
        Ok(None) => {
            state.pair_failed(&peer);
            tracing::info!(client = %fp(&key), "pairing refused: unknown or expired nameplate");
            return unknown_nameplate();
        }
        Err(e) => return store_error(e),
    };
    let host = match state.store.host(&host_id) {
        Ok(Some(host)) => host,
        Ok(None) => return unknown_nameplate(),
        Err(e) => return store_error(e),
    };
    let Some(session) = state.host_session(&host_id) else {
        inc(&state.metrics.refused_offline);
        return host_offline(&host_id);
    };
    let Some(stream) = session.open_stream().await else {
        inc(&state.metrics.refused_offline);
        return host_offline(&host_id);
    };
    let (rx, mut tx) = pump::framed(stream, state.max_message);
    let open = Open::Client {
        client_key: key.clone(),
        nameplate: Some(nameplate),
    };
    if tx.send(Frame::Open(open)).await.is_err() {
        return host_offline(&host_id);
    }
    // Where the connection went, for clients that want the relay's view.
    // In OAL 0.2 the client learns and authenticates the host's key in the
    // pairing handshake itself.
    if let (Ok(id), Ok(hk)) = (
        HeaderValue::from_str(&host.id),
        HeaderValue::from_str(&host.public_key),
    ) {
        resp.headers_mut().insert(HOST_ID_HEADER, id);
        resp.headers_mut().insert(HOST_KEY_HEADER, hk);
    }
    inc(&state.metrics.pairing_connections);
    tokio::spawn(relay_client(state, on_upgrade, rx, tx, host_id, key, true));
    resp
}

async fn relay_client(
    state: Arc<State>,
    on_upgrade: OnUpgrade,
    rx: StreamRead,
    tx: StreamWrite,
    host_id: String,
    client_key: String,
    pairing: bool,
) {
    let Some(ws) = accept_ws(on_upgrade, Some(ws_config(state.max_message))).await else {
        return;
    };
    let id = state.next_id.fetch_add(1, Ordering::Relaxed);
    let (kill, kill_rx) = oneshot::channel();
    if let Ok(mut clients) = state.clients.lock() {
        clients.insert(
            id,
            ClientEntry {
                host_id: host_id.clone(),
                client_key: client_key.clone(),
                kill: Some(kill),
            },
        );
    }
    // A revocation that landed between the pairing check and the line above
    // would have missed this connection. (A pairing connection has no
    // pairing yet to lose.)
    if !pairing && !matches!(state.store.pairing(&host_id, &client_key), Ok(Some(_))) {
        state.kill_clients(Some(&host_id), Some(&client_key), CLOSE_UNPAIRED);
    }
    if state.shutdown.is_cancelled() {
        state.kill_clients(Some(&host_id), Some(&client_key), CLOSE_HOST_GONE);
    }
    inc(&state.metrics.clients_connected);
    inc(&state.metrics.client_connections);
    tracing::info!(conn = id, host = %host_id, client = %fp(&client_key), pairing, "client connected");
    let summary = pump::pump(ws, rx, tx, kill_rx, state.metrics.traffic.clone()).await;
    if let Ok(mut clients) = state.clients.lock() {
        clients.remove(&id);
    }
    dec(&state.metrics.clients_connected);
    tracing::info!(
        conn = id,
        host = %host_id,
        client = %fp(&client_key),
        code = ?summary.end.code(),
        end = ?summary.end,
        to_host_messages = summary.from_ws_messages,
        to_host_bytes = summary.from_ws_bytes,
        to_client_messages = summary.to_ws_messages,
        to_client_bytes = summary.to_ws_bytes,
        "client disconnected"
    );
}

/// `GET /oal/presence`: the presence of every host the client paired with.
pub(crate) fn presence(state: &State, req: &Request<Incoming>) -> Resp {
    let key = match state.authenticate(req, Role::Client, Target::Presence) {
        Ok((key, _)) => key,
        Err(resp) => return *resp,
    };
    let hosts = match state.store.paired_hosts(&key) {
        Ok(hosts) => hosts,
        Err(e) => return store_error(e),
    };
    let hosts: Vec<_> = hosts
        .into_iter()
        .map(|h| {
            let session = state.host_session(&h.id);
            serde_json::json!({
                "hostId": h.id,
                "online": session.is_some(),
                "lastSeenAt": rfc3339(if session.is_some() { now() } else { h.last_seen_at }),
                "agents": session.map(|s| s.agents()).unwrap_or_default(),
            })
        })
        .collect();
    json(StatusCode::OK, &serde_json::json!({ "hosts": hosts }))
}

/// Shutdown: every client connection closes with 1001 (the client
/// reconnects), every tunnel closes, and the relay waits briefly for the
/// close frames to go out.
pub(crate) async fn close_all(state: &State) {
    state.kill_clients(None, None, CLOSE_HOST_GONE);
    let sessions: Vec<_> = state
        .hosts
        .lock()
        .map(|h| h.values().cloned().collect())
        .unwrap_or_default();
    for s in &sessions {
        s.close.cancel();
    }
    for _ in 0..50 {
        let empty = state.clients.lock().map(|c| c.is_empty()).unwrap_or(true)
            && state.hosts.lock().map(|h| h.is_empty()).unwrap_or(true);
        if empty {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
