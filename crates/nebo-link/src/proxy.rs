//! The local proxy the tunnel delivers to. The hub strips `/t/<botId>` and
//! sends each owner request down the bot's tunnel; `nebo_comm::tunnel` stamps
//! it with `X-Nebo-Tunnel-Auth` and writes it here. This proxy:
//!
//! - refuses anything without that stamp, so only hub-authenticated requests
//!   reach the runtime (a local program can reach the proxy's port, never
//!   the stamp);
//! - serves the link's own endpoints under `/_link/`;
//! - forwards everything else to the runtime's UI with the path and headers
//!   the runtime expects ([`nebo_runtimes::ProxyRoute`]), streaming bodies
//!   unbuffered;
//! - rewrites URLs both ways ([`crate::rewrite`]) in headers, text bodies and
//!   WebSocket text frames, so the runtime's UI works under `/t/<botId>`
//!   whether or not the runtime knows the prefix.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{Sink, SinkExt, Stream, StreamExt};
use http_body_util::{BodyExt, Full, Limited, combinators::BoxBody};
use hyper::body::Incoming;
use hyper::header::{self, HeaderMap, HeaderName, HeaderValue};
use hyper::upgrade::Upgraded;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::rt::TokioIo;
use nebo_runtimes::{PathMode, ProxyRoute};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use crate::endpoints::WEB_ORIGIN;
use crate::rewrite::{Coding, Direction, RewrittenBody, Rewriter};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type Body = BoxBody<Bytes, BoxError>;

/// The client address the link reports in `X-Forwarded-For`. The hub does
/// not pass the browser's address down the tunnel, and OpenClaw refuses a
/// trusted-proxy request whose client is loopback (`proxy_attribution_required`),
/// so the link names a fixed non-loopback address (TEST-NET-3, never routed).
pub const FORWARDED_FOR: &str = "203.0.113.10";

/// The header that proves a request came through the tunnel.
const TUNNEL_AUTH: &str = "x-nebo-tunnel-auth";

/// The largest WebSocket message relayed (a frame may be as large). OpenClaw
/// accepts 25 MB payloads (canvas snapshots).
const MAX_WS_MESSAGE: usize = 64 << 20;

/// How long one side of a relayed WebSocket gets to finish closing after the
/// other has.
const WS_CLOSE_GRACE: Duration = Duration::from_secs(5);

/// Headers that describe one connection, never forwarded as they are.
const HOP_BY_HOP: [&str; 9] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "transfer-encoding",
    "te",
    "trailer",
    "upgrade",
    "proxy-authorization",
    "proxy-authenticate",
];

/// Where and how to forward.
#[derive(Debug, Clone)]
pub struct Target {
    /// The runtime UI's loopback address.
    pub upstream: SocketAddr,
    /// The prefix the browser sees: `/t/<botId>`.
    pub base_path: String,
    pub route: ProxyRoute,
    /// The value of the route's identity header: the owner's NeboAI id.
    pub identity: String,
    /// The runtime's name, for error pages.
    pub runtime_name: &'static str,
}

/// The link's own endpoints, served under `/_link/`.
pub trait Control: Send + Sync + 'static {
    /// `GET /_link/status`.
    fn status(&self) -> serde_json::Value;
    /// `POST /_link/models {"enabled": bool}`: turn NeboAI models on or off.
    fn set_models(&self, enabled: bool) -> impl Future<Output = Result<serde_json::Value, String>> + Send;
}

/// Binds `addr`, refusing anything but a loopback address.
pub async fn bind_loopback(addr: SocketAddr) -> std::io::Result<TcpListener> {
    if !addr.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("refusing to listen on {addr}: nebo-link only listens on loopback"),
        ));
    }
    TcpListener::bind(addr).await
}

/// Serves the proxy on `listener` until the task is dropped. `secret` is the
/// tunnel's stamp (`nebo_comm::tunnel::tunnel_auth_secret()`).
pub async fn serve<C: Control>(listener: TcpListener, target: Target, secret: String, control: Arc<C>) {
    let rewriter = Arc::new(rewriter(&target));
    let target = Arc::new(target);
    let secret: Arc<str> = secret.into();
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            // Out of file descriptors and the like: let it clear.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        };
        let (target, rewriter, secret, control) = (target.clone(), rewriter.clone(), secret.clone(), control.clone());
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| {
                handle(req, target.clone(), rewriter.clone(), secret.clone(), control.clone())
            });
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await;
        });
    }
}

/// The URL rewrites for `target`.
fn rewriter(target: &Target) -> Rewriter {
    Rewriter::new(WEB_ORIGIN, &target.base_path, target.upstream.port(), target.route.path_mode)
}

async fn handle<C: Control>(
    req: Request<Incoming>,
    target: Arc<Target>,
    rewriter: Arc<Rewriter>,
    secret: Arc<str>,
    control: Arc<C>,
) -> Result<Response<Body>, Infallible> {
    let stamped = req
        .headers()
        .get(TUNNEL_AUTH)
        .is_some_and(|v| constant_time_eq(v.as_bytes(), secret.as_bytes()));
    if !stamped {
        return Ok(text(StatusCode::FORBIDDEN, "Only NeboAI can reach this link."));
    }
    let path = req.uri().path();
    if path == "/_link" || path.starts_with("/_link/") {
        return Ok(link_endpoint(req, control.as_ref()).await);
    }
    Ok(forward(req, &target, rewriter).await)
}

async fn link_endpoint<C: Control>(req: Request<Incoming>, control: &C) -> Response<Body> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/_link/status") => json(StatusCode::OK, &control.status()),
        (&Method::POST, "/_link/models") => {
            #[derive(serde::Deserialize)]
            struct Toggle {
                enabled: bool,
            }
            let body = match Limited::new(req.into_body(), 4096).collect().await {
                Ok(body) => body.to_bytes(),
                Err(_) => return text(StatusCode::BAD_REQUEST, "request body too large"),
            };
            let Ok(toggle) = serde_json::from_slice::<Toggle>(&body) else {
                return text(StatusCode::BAD_REQUEST, r#"expected {"enabled": true|false}"#);
            };
            match control.set_models(toggle.enabled).await {
                Ok(value) => json(StatusCode::OK, &value),
                Err(error) => json(StatusCode::BAD_GATEWAY, &serde_json::json!({ "error": error })),
            }
        }
        _ => text(StatusCode::NOT_FOUND, "not found"),
    }
}

/// Forwards one request to the runtime, rewriting URLs in the response; a
/// WebSocket upgrade is then relayed message by message in both directions
/// (any other upgrade is spliced byte for byte).
async fn forward(mut req: Request<Incoming>, target: &Target, rewriter: Arc<Rewriter>) -> Response<Body> {
    let upgrade = is_upgrade(req.headers());
    let client_upgrade = upgrade.then(|| hyper::upgrade::on(&mut req));
    let (mut parts, body) = req.into_parts();
    rewrite(&mut parts.uri, &mut parts.headers, target, &rewriter, upgrade);
    let offline = || {
        text(
            StatusCode::BAD_GATEWAY,
            &format!("Could not connect to {}. Try again.", target.runtime_name),
        )
    };

    let Ok(stream) = TcpStream::connect(target.upstream).await else {
        tracing::info!(runtime = target.runtime_name, "proxy: the runtime's UI is not accepting connections");
        return offline();
    };
    let Ok((mut sender, conn)) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await else {
        return offline();
    };
    tokio::spawn(conn.with_upgrades());
    let Ok(mut resp) = sender.send_request(Request::from_parts(parts, body)).await else {
        return offline();
    };

    if resp.status() == StatusCode::SWITCHING_PROTOCOLS {
        if let Some(client_upgrade) = client_upgrade {
            let websocket = resp
                .headers()
                .get(header::UPGRADE)
                .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"));
            let upstream_upgrade = hyper::upgrade::on(&mut resp);
            tokio::spawn(async move {
                if let (Ok(client), Ok(upstream)) = tokio::join!(client_upgrade, upstream_upgrade) {
                    if websocket {
                        relay(client, upstream, rewriter).await;
                    } else {
                        let _ = tokio::io::copy_bidirectional(&mut TokioIo::new(client), &mut TokioIo::new(upstream))
                            .await;
                    }
                }
            });
        }
        return resp.map(|body| body.map_err(BoxError::from).boxed());
    }

    strip_hop_by_hop(resp.headers_mut());
    rewriter.response_headers(resp.headers_mut());
    // A partial body (206) is left alone: its ranges count the runtime's bytes.
    let coding = Coding::of(resp.headers());
    let rewrite_body = resp.status() != StatusCode::PARTIAL_CONTENT && crate::rewrite::is_text(resp.headers());
    match coding.filter(|_| rewrite_body) {
        Some(coding) => {
            resp.headers_mut().remove(header::CONTENT_LENGTH);
            resp.map(|body| RewrittenBody::new(body.map_err(BoxError::from).boxed(), rewriter, coding).boxed())
        }
        None => resp.map(|body| body.map_err(BoxError::from).boxed()),
    }
}

/// Relays an upgraded WebSocket between the browser and the runtime,
/// rewriting URLs in text messages; every other message passes unchanged.
async fn relay(client: Upgraded, upstream: Upgraded, rewriter: Arc<Rewriter>) {
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_WS_MESSAGE))
        .max_frame_size(Some(MAX_WS_MESSAGE));
    let client = WebSocketStream::from_raw_socket(TokioIo::new(client), Role::Server, Some(config)).await;
    let upstream = WebSocketStream::from_raw_socket(TokioIo::new(upstream), Role::Client, Some(config)).await;
    let (client_tx, client_rx) = client.split();
    let (upstream_tx, upstream_rx) = upstream.split();
    let mut down = std::pin::pin!(pipe(upstream_rx, client_tx, rewriter.clone(), Direction::Outbound));
    let mut up = std::pin::pin!(pipe(client_rx, upstream_tx, rewriter, Direction::Inbound));
    tokio::select! {
        () = &mut down => { let _ = tokio::time::timeout(WS_CLOSE_GRACE, up).await; }
        () = &mut up => { let _ = tokio::time::timeout(WS_CLOSE_GRACE, down).await; }
    }
}

/// Moves messages from one side to the other until either ends, then closes
/// the other side.
async fn pipe<R, W>(mut from: R, mut to: W, rewriter: Arc<Rewriter>, direction: Direction)
where
    R: Stream<Item = Result<Message, WsError>> + Unpin,
    W: Sink<Message> + Unpin,
{
    while let Some(Ok(message)) = from.next().await {
        let message = match message {
            Message::Text(text) => match rewriter.rewrite(direction, text.as_str()) {
                std::borrow::Cow::Borrowed(_) => Message::Text(text),
                std::borrow::Cow::Owned(rewritten) => Message::text(rewritten),
            },
            other => other,
        };
        if to.send(message).await.is_err() {
            return;
        }
    }
    let _ = to.close().await;
}

/// Rewrites a request's path and headers for the runtime. Every header the
/// runtime trusts is set by the link and never taken from the client.
pub fn rewrite(uri: &mut Uri, headers: &mut HeaderMap, target: &Target, rewriter: &Rewriter, upgrade: bool) {
    let path = uri.path();
    let path = match target.route.path_mode {
        PathMode::ReaddPrefix => format!("{}{}", target.base_path, path),
        PathMode::StripWithForwardedPrefix => path.to_string(),
    };
    let path_and_query = match uri.query() {
        Some(query) => format!("{path}?{query}"),
        None => path,
    };
    *uri = path_and_query.parse().unwrap_or_else(|_| Uri::from_static("/"));

    let upgrade_to = headers.get(header::UPGRADE).cloned();
    strip_hop_by_hop(headers);
    if upgrade && let Some(protocol) = upgrade_to {
        headers.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
        headers.insert(header::UPGRADE, protocol);
    }
    // The link reads every message and relays it uncompressed on both legs,
    // so no WebSocket extension (permessage-deflate) is negotiated.
    headers.remove(header::SEC_WEBSOCKET_EXTENSIONS);
    Coding::restrict_accept(headers);
    rewriter.request_headers(headers);

    let spoofable: Vec<HeaderName> = headers
        .keys()
        .filter(|name| {
            let name = name.as_str();
            name.starts_with("x-forwarded-") || name == "forwarded" || name == "x-real-ip" || name == TUNNEL_AUTH
        })
        .cloned()
        .collect();
    for name in spoofable {
        headers.remove(name);
    }

    let set = |headers: &mut HeaderMap, name: &str, value: &str| {
        if let (Ok(name), Ok(value)) = (HeaderName::try_from(name), HeaderValue::try_from(value)) {
            headers.insert(name, value);
        }
    };
    set(headers, "host", &target.upstream.to_string());
    match &target.route.origin {
        Some(origin) => set(headers, "origin", origin),
        None => {
            headers.remove(header::ORIGIN);
        }
    }
    if let Some(name) = &target.route.identity_header {
        headers.remove(name.as_str());
        set(headers, name, &target.identity);
    }
    if target.route.path_mode == PathMode::StripWithForwardedPrefix {
        set(headers, "x-forwarded-prefix", &target.base_path);
    }
    set(headers, "x-forwarded-for", FORWARDED_FOR);
    set(headers, "x-forwarded-proto", "https");
    set(headers, "x-forwarded-host", WEB_ORIGIN.trim_start_matches("https://"));
}

fn is_upgrade(headers: &HeaderMap) -> bool {
    headers.contains_key(header::UPGRADE)
        && headers
            .get_all(header::CONNECTION)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
}

/// Removes hop-by-hop headers, including any the `Connection` header names.
fn strip_hop_by_hop(headers: &mut HeaderMap) {
    let named: Vec<String> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| !token.is_empty())
        .collect();
    for name in HOP_BY_HOP.iter().copied().chain(named.iter().map(String::as_str)) {
        headers.remove(name);
    }
}

pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

pub fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).map_err(|never| match never {}).boxed()
}

pub fn text(status: StatusCode, message: &str) -> Response<Body> {
    let mut resp = Response::new(full(message.to_string()));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
    resp
}

pub fn json(status: StatusCode, value: &serde_json::Value) -> Response<Body> {
    let mut resp = Response::new(full(value.to_string()));
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOT: &str = "/t/test-bot";

    fn openclaw() -> Target {
        Target {
            upstream: "127.0.0.1:28789".parse().unwrap(),
            base_path: BOT.into(),
            route: ProxyRoute {
                path_mode: PathMode::ReaddPrefix,
                origin: Some("https://neboai.com".into()),
                identity_header: Some("x-nebo-user".into()),
            },
            identity: "owner-1".into(),
            runtime_name: "OpenClaw",
        }
    }

    fn hermes() -> Target {
        Target {
            upstream: "127.0.0.1:29119".parse().unwrap(),
            base_path: BOT.into(),
            route: ProxyRoute {
                path_mode: PathMode::StripWithForwardedPrefix,
                origin: None,
                identity_header: None,
            },
            identity: "owner-1".into(),
            runtime_name: "Hermes",
        }
    }

    /// A request as the tunnel delivers it: prefix already stripped by the
    /// hub, the browser's host, and whatever a client tried to smuggle.
    fn hostile_headers() -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in [
            ("host", "neboai.com"),
            ("origin", "https://evil.example"),
            ("x-nebo-user", "attacker"),
            ("x-forwarded-for", "127.0.0.1"),
            ("x-forwarded-host", "evil.example"),
            ("x-forwarded-proto", "http"),
            ("x-forwarded-prefix", "/elsewhere"),
            ("forwarded", "for=127.0.0.1"),
            ("x-real-ip", "127.0.0.1"),
            ("x-nebo-tunnel-auth", "secret"),
            ("connection", "keep-alive, x-drop-me"),
            ("x-drop-me", "1"),
            ("keep-alive", "timeout=5"),
            ("accept", "text/html"),
        ] {
            h.append(k, HeaderValue::from_static(v));
        }
        h
    }

    fn get<'a>(h: &'a HeaderMap, name: &str) -> Option<&'a str> {
        h.get(name).map(|v| v.to_str().unwrap())
    }

    /// OpenClaw (spike, 2026-09-25): the gateway serves under
    /// `controlUi.basePath = /t/<botId>`, so the prefix is re-added; Host is
    /// loopback, Origin is the allowed web origin (the hub strips it),
    /// X-Forwarded-For is non-loopback, and the identity header is the owner.
    #[test]
    fn openclaw_re_adds_the_prefix_and_sets_trusted_proxy_headers() {
        for (inbound, outbound) in [
            ("/", "/t/test-bot/"),
            ("/assets/index.js", "/t/test-bot/assets/index.js"),
            ("/?session=main", "/t/test-bot/?session=main"),
        ] {
            let mut uri: Uri = inbound.parse().unwrap();
            let mut h = hostile_headers();
            rewrite(&mut uri, &mut h, &openclaw(), &rewriter(&openclaw()), false);
            assert_eq!(uri.to_string(), outbound);
            assert_eq!(get(&h, "host"), Some("127.0.0.1:28789"));
            assert_eq!(get(&h, "origin"), Some("https://neboai.com"));
            assert_eq!(get(&h, "x-nebo-user"), Some("owner-1"));
            assert_eq!(h.get_all("x-nebo-user").iter().count(), 1);
            assert_eq!(get(&h, "x-forwarded-for"), Some(FORWARDED_FOR));
            assert_eq!(get(&h, "x-forwarded-proto"), Some("https"));
            assert_eq!(get(&h, "x-forwarded-host"), Some("neboai.com"));
            assert_eq!(get(&h, "x-forwarded-prefix"), None);
            assert_eq!(get(&h, "forwarded"), None);
            assert_eq!(get(&h, "x-real-ip"), None);
            assert_eq!(get(&h, "x-nebo-tunnel-auth"), None);
            assert_eq!(get(&h, "connection"), None);
            assert_eq!(get(&h, "keep-alive"), None);
            assert_eq!(get(&h, "x-drop-me"), None);
            assert_eq!(get(&h, "accept"), Some("text/html"));
        }
    }

    /// Hermes (spike, 2026-09-25): the dashboard serves at the root and takes
    /// its prefix from X-Forwarded-Prefix; Host is loopback and Origin must be
    /// absent (any non-loopback Origin fails its WS handshake with 403).
    #[test]
    fn hermes_strips_the_prefix_and_never_sends_origin() {
        for (inbound, outbound) in [
            ("/", "/"),
            ("/assets/index.js", "/assets/index.js"),
            ("/api/ws?token=x", "/api/ws?token=x"),
        ] {
            let mut uri: Uri = inbound.parse().unwrap();
            let mut h = hostile_headers();
            rewrite(&mut uri, &mut h, &hermes(), &rewriter(&hermes()), false);
            assert_eq!(uri.to_string(), outbound);
            assert_eq!(get(&h, "host"), Some("127.0.0.1:29119"));
            assert_eq!(get(&h, "origin"), None);
            assert_eq!(get(&h, "x-forwarded-prefix"), Some(BOT));
            assert_eq!(h.get_all("x-forwarded-prefix").iter().count(), 1);
            assert_eq!(get(&h, "x-forwarded-for"), Some(FORWARDED_FOR));
            assert_eq!(get(&h, "x-nebo-tunnel-auth"), None);
        }
    }

    #[test]
    fn websocket_upgrades_keep_their_upgrade_headers() {
        let mut uri: Uri = "/api/ws".parse().unwrap();
        let mut h = HeaderMap::new();
        h.insert("connection", HeaderValue::from_static("keep-alive, Upgrade"));
        h.insert("upgrade", HeaderValue::from_static("websocket"));
        h.insert("sec-websocket-key", HeaderValue::from_static("abc"));
        assert!(is_upgrade(&h));
        rewrite(&mut uri, &mut h, &hermes(), &rewriter(&hermes()), true);
        assert_eq!(get(&h, "connection"), Some("upgrade"));
        assert_eq!(get(&h, "upgrade"), Some("websocket"));
        assert_eq!(get(&h, "sec-websocket-key"), Some("abc"));
    }

    #[test]
    fn only_loopback_is_bound() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            assert!(bind_loopback("0.0.0.0:0".parse().unwrap()).await.is_err());
            assert!(bind_loopback("127.0.0.1:0".parse().unwrap()).await.is_ok());
        });
    }

    #[test]
    fn stamp_comparison() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
    }
}
