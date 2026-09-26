//! The relay itself: an HTTP server (optionally TLS) with the OAL relay
//! endpoints, the operator's admin API, `/health` and `/metrics`.
//!
//! | Endpoint | Who | What |
//! |---|---|---|
//! | `GET /oal/challenge` | anyone | a single-use nonce and the relay's key |
//! | `GET /oal/tunnel/<hostId>` (WebSocket) | a host | its tunnel; registers `hostId` to its key the first time |
//! | `GET /oal/hosts/<hostId>` (WebSocket) | a paired client | a connection to that host |
//! | `GET /oal/pair/<nameplate>` (WebSocket) | a client | a pairing connection to the host holding the nameplate |
//! | `GET /oal/presence` | a client | the presence of the hosts it paired with |
//! | `GET /admin/hosts`, `POST /admin/revoke` | the operator (admin token) | hosts and pairings, revocation |
//! | `GET /health`, `GET /metrics` | anyone | liveness; counters without identities |
//!
//! Every `/oal/*` request except the challenge carries `key`, `nonce` and
//! `proof` query parameters ([`crate::auth`]). The challenge and presence
//! answers are readable from a web page on any origin, so a browser client
//! can reach the relay.

mod admin;
mod metrics;
mod nonces;
mod relay;
mod store;

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::header::{ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::auth::Keypair;
use crate::wire::{DEFAULT_MAX_MESSAGE_BYTES, ErrorBody};

/// The file in the data directory that holds the admin token when none is
/// configured. `oal-relay hosts` and `oal-relay revoke` read it from there.
pub const ADMIN_TOKEN_FILE: &str = "admin.token";
const DB_FILE: &str = "relay.db";

/// How long a nameplate routes pairing connections unless configured
/// otherwise. OAL codes expire within 10 minutes (spec section 6.2).
pub const DEFAULT_NAMEPLATE_TTL: Duration = Duration::from_secs(300);

/// Certificate chain and private key, both PEM, for built-in TLS.
#[derive(Debug, Clone)]
pub struct TlsFiles {
    pub cert: PathBuf,
    pub key: PathBuf,
}

/// How the relay runs.
#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    /// Where the store (`relay.db`) and the generated admin token live.
    pub data_dir: PathBuf,
    /// Serve HTTPS itself. `None`: plain HTTP, for running behind a proxy
    /// that terminates TLS (or on this machine only).
    pub tls: Option<TlsFiles>,
    /// The operator's token for `/admin/*`. `None`: read or create
    /// `<data_dir>/admin.token`.
    pub admin_token: Option<String>,
    /// Host keys allowed to register. Empty: any key may register a free id.
    pub allow_hosts: Vec<String>,
    /// The largest WebSocket message relayed; larger closes with 1009.
    pub max_message_bytes: usize,
    /// How long a nameplate routes pairing connections to its host.
    pub nameplate_ttl: Duration,
}

impl Config {
    pub fn new(listen: SocketAddr, data_dir: PathBuf) -> Self {
        Self {
            listen,
            data_dir,
            tls: None,
            admin_token: None,
            allow_hosts: Vec::new(),
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            nameplate_ttl: DEFAULT_NAMEPLATE_TTL,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("the relay's store: {0}")]
    Store(#[from] rusqlite::Error),
    #[error("TLS: {0}")]
    Tls(String),
}

/// A running relay.
pub struct RelayHandle {
    addr: SocketAddr,
    relay_key: String,
    shutdown: CancellationToken,
    task: JoinHandle<()>,
}

impl RelayHandle {
    /// The address it listens on (useful with port 0).
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// The relay's public key (base64url), the same across restarts.
    pub fn relay_key(&self) -> &str {
        &self.relay_key
    }

    /// Stops accepting, closes every client connection with 1001 and every
    /// tunnel, and waits for them to go.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        let _ = self.task.await;
    }
}

/// The shared state behind every request.
struct State {
    store: store::Store,
    key: Keypair,
    relay_key: String,
    nonces: nonces::Nonces,
    admin_digest: [u8; 32],
    allow_hosts: HashSet<String>,
    max_message: usize,
    nameplate_ttl: i64,
    hosts: Mutex<HashMap<String, Arc<relay::HostSession>>>,
    clients: Mutex<HashMap<u64, relay::ClientEntry>>,
    next_id: AtomicU64,
    metrics: metrics::Metrics,
    pair_failures: Mutex<HashMap<IpAddr, (i64, u32)>>,
    shutdown: CancellationToken,
}

/// Starts the relay: opens the store, binds `config.listen`, and serves
/// until [`RelayHandle::shutdown`].
pub async fn start(config: Config) -> Result<RelayHandle, StartError> {
    std::fs::create_dir_all(&config.data_dir)?;
    let store = store::Store::open(&config.data_dir.join(DB_FILE))?;
    let key = Keypair::from_secret(store.relay_secret()?);
    let admin = match config.admin_token.clone() {
        Some(token) => token,
        None => admin_token(&config.data_dir)?,
    };
    let tls = config.tls.as_ref().map(tls_acceptor).transpose()?;
    let listener = TcpListener::bind(config.listen).await?;
    let addr = listener.local_addr()?;
    let shutdown = CancellationToken::new();
    let relay_key = key.public_b64();
    let state = Arc::new(State {
        store,
        relay_key: relay_key.clone(),
        key,
        nonces: nonces::Nonces::new(),
        admin_digest: admin::digest(&admin),
        allow_hosts: config.allow_hosts.iter().cloned().collect(),
        max_message: config.max_message_bytes,
        nameplate_ttl: config.nameplate_ttl.as_secs().max(1) as i64,
        hosts: Mutex::new(HashMap::new()),
        clients: Mutex::new(HashMap::new()),
        next_id: AtomicU64::new(1),
        metrics: metrics::Metrics::default(),
        pair_failures: Mutex::new(HashMap::new()),
        shutdown: shutdown.clone(),
    });
    tracing::info!(
        listen = %addr,
        tls = tls.is_some(),
        relay_key = %relay_key,
        open_registration = state.allow_hosts.is_empty(),
        "relay listening"
    );
    let task = tokio::spawn(accept_loop(state, listener, tls));
    Ok(RelayHandle {
        addr,
        relay_key,
        shutdown,
        task,
    })
}

/// The admin token stored in `data_dir`, created (random, owner-only) the
/// first time.
fn admin_token(data_dir: &Path) -> std::io::Result<String> {
    let path = data_dir.join(ADMIN_TOKEN_FILE);
    if let Ok(token) = std::fs::read_to_string(&path)
        && !token.trim().is_empty()
    {
        return Ok(token.trim().to_owned());
    }
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).expect("the OS random source failed");
    let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    std::io::Write::write_all(&mut options.open(&path)?, token.as_bytes())?;
    tracing::info!(path = %path.display(), "created the admin token");
    Ok(token)
}

fn tls_acceptor(files: &TlsFiles) -> Result<tokio_rustls::TlsAcceptor, StartError> {
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer};
    use tokio_rustls::rustls;
    let certs = CertificateDer::pem_file_iter(&files.cert)
        .and_then(|it| it.collect::<Result<Vec<_>, _>>())
        .map_err(|e| StartError::Tls(format!("{}: {e}", files.cert.display())))?;
    let key = PrivateKeyDer::from_pem_file(&files.key)
        .map_err(|e| StartError::Tls(format!("{}: {e}", files.key.display())))?;
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| StartError::Tls(e.to_string()))?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|e| StartError::Tls(e.to_string()))?;
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

async fn accept_loop(
    state: Arc<State>,
    listener: TcpListener,
    tls: Option<tokio_rustls::TlsAcceptor>,
) {
    loop {
        let (tcp, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(pair) => pair,
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = state.shutdown.cancelled() => break,
        };
        let state = state.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            let _ = tcp.set_nodelay(true);
            match tls {
                Some(acceptor) => {
                    match tokio::time::timeout(Duration::from_secs(10), acceptor.accept(tcp)).await
                    {
                        Ok(Ok(stream)) => serve_http(state, peer, stream).await,
                        _ => tracing::debug!(peer = %peer, "TLS handshake failed"),
                    }
                }
                None => serve_http(state, peer, tcp).await,
            }
        });
    }
    drop(listener);
    relay::close_all(&state).await;
    tracing::info!("relay stopped");
}

async fn serve_http<I>(state: Arc<State>, peer: SocketAddr, io: I)
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let shutdown = state.shutdown.clone();
    let service = hyper::service::service_fn(move |req| {
        let state = state.clone();
        async move { Ok::<_, Infallible>(route(state, peer, req).await) }
    });
    let conn = hyper::server::conn::http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(30))
        .serve_connection(TokioIo::new(io), service)
        .with_upgrades();
    // An upgraded connection has left `conn` by now; this only ends idle
    // HTTP keep-alive connections at shutdown.
    tokio::select! {
        _ = conn => {}
        _ = shutdown.cancelled() => {}
    }
}

pub(crate) type Resp = Response<Full<Bytes>>;

async fn route(state: Arc<State>, peer: SocketAddr, req: Request<Incoming>) -> Resp {
    let path = req.uri().path().to_owned();
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    let method = req.method().clone();
    match (&method, segments.as_slice()) {
        (&Method::GET, ["health"]) => match state.store.ping() {
            Ok(()) => json(
                StatusCode::OK,
                &serde_json::json!({"status": "ok", "version": env!("CARGO_PKG_VERSION")}),
            ),
            Err(_) => refuse(
                StatusCode::SERVICE_UNAVAILABLE,
                "unhealthy",
                "The relay cannot read its store.",
            ),
        },
        (&Method::GET, ["metrics"]) => {
            let mut resp = Response::new(Full::new(Bytes::from(state.metrics.render())));
            resp.headers_mut().insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; version=0.0.4"),
            );
            resp
        }
        (&Method::GET, ["oal", "challenge"]) => any_origin(json(
            StatusCode::OK,
            &serde_json::json!({
                "nonce": state.nonces.issue(),
                "relayKey": state.relay_key,
                "expiresIn": nonces::NONCE_TTL,
            }),
        )),
        (&Method::GET, ["oal", "presence"]) => any_origin(relay::presence(&state, &req)),
        (&Method::GET, ["oal", "tunnel", id]) => {
            let id = (*id).to_owned();
            relay::tunnel(state, req, id).await
        }
        (&Method::GET, ["oal", "hosts", id]) => {
            let id = (*id).to_owned();
            relay::connect(state, req, id).await
        }
        (&Method::GET, ["oal", "pair", nameplate]) => {
            let nameplate = (*nameplate).to_owned();
            relay::pair(state, peer, req, nameplate).await
        }
        (_, ["admin", ..]) => admin::route(state, req, &segments[1..].join("/")).await,
        _ => refuse(StatusCode::NOT_FOUND, "not_found", "There's nothing here."),
    }
}

/// Lets a web page on any origin read `resp`. For the answers a browser
/// client fetches (the challenge, presence): they carry no cookies, and what
/// authenticates the caller rides in the query string.
fn any_origin(mut resp: Resp) -> Resp {
    resp.headers_mut()
        .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    resp
}

pub(crate) fn json(status: StatusCode, body: &impl Serialize) -> Resp {
    let bytes = serde_json::to_vec(body).expect("responses serialize");
    let mut resp = Response::new(Full::new(Bytes::from(bytes)));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

pub(crate) fn refuse(status: StatusCode, code: &str, message: impl Into<String>) -> Resp {
    json(
        status,
        &ErrorBody {
            code: code.to_owned(),
            message: message.into(),
        },
    )
}

pub(crate) fn query(req: &Request<Incoming>) -> HashMap<String, String> {
    form_urlencoded::parse(req.uri().query().unwrap_or("").as_bytes())
        .into_owned()
        .collect()
}
