//! The link's proxy in front of a fake runtime UI: per-runtime paths and
//! headers (the rules the 2026-09-25 spike proved against OpenClaw 2026.9.6
//! and the Hermes 0.19.0 dashboard), the tunnel stamp gate, the `/_link/`
//! endpoints, unbuffered streaming, WebSocket upgrades, URL rewriting both
//! ways, and the whole path from a fake hub through `nebo_comm::tunnel` to
//! the runtime.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Empty, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::HeaderMap;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use nebo_link::proxy::{self, Body, BoxError, Control, FORWARDED_FOR, Target};
use nebo_runtimes::{PathMode, ProxyRoute};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;

const BOT: &str = "/t/test-bot";
const OWNER: &str = "owner-1";

/// What the fake runtime saw of one request, and the WebSocket text
/// messages it received on it.
#[derive(Debug, Clone)]
struct Seen {
    uri: String,
    headers: HeaderMap,
    messages: Arc<Mutex<Vec<String>>>,
}

type Log = Arc<Mutex<Vec<Seen>>>;

/// A runtime UI: answers `…/stream` with two chunks 1.5 s apart, echoes
/// WebSocket messages (answering `hello` with an OpenClaw-style hello that
/// names its canvas by origin only), serves the pages in [`page`], and
/// answers anything else with "ok".
async fn fake_runtime() -> (SocketAddr, Log) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let log: Log = Arc::default();
    let seen = log.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = seen.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |req: Request<Incoming>| {
                    let seen = seen.clone();
                    async move {
                        let messages = Arc::default();
                        seen.lock().unwrap().push(Seen {
                            uri: req.uri().to_string(),
                            headers: req.headers().clone(),
                            messages: Arc::clone(&messages),
                        });
                        Ok::<_, std::convert::Infallible>(respond(req, messages))
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .with_upgrades()
                    .await;
            });
        }
    });
    (addr, log)
}

/// The canvas URL OpenClaw's hello advertises: origin only, no prefix
/// (`resolveHostedPluginSurfaceUrl`).
const CANVAS: &str = "https://neboai.com:443/__openclaw__/cap/tok";

fn respond(mut req: Request<Incoming>, messages: Arc<Mutex<Vec<String>>>) -> Response<Body> {
    if let Some(key) = req.headers().get("sec-websocket-key") {
        let accept = derive_accept_key(key.as_bytes());
        let upgrade = hyper::upgrade::on(&mut req);
        tokio::spawn(async move {
            let io = TokioIo::new(upgrade.await.unwrap());
            let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(io, Role::Server, None).await;
            while let Some(Ok(msg)) = ws.next().await {
                match msg {
                    Message::Text(text) if text.as_str() == "hello" => {
                        let hello = serde_json::json!({ "type": "hello", "pluginSurfaceUrls": { "canvas": CANVAS } });
                        ws.send(Message::text(hello.to_string())).await.unwrap();
                    }
                    Message::Text(text) => {
                        messages.lock().unwrap().push(text.to_string());
                        ws.send(Message::text(format!("echo {}", text.as_str()))).await.unwrap();
                    }
                    Message::Binary(data) => ws.send(Message::Binary(data)).await.unwrap(),
                    _ => {}
                }
            }
        });
        return Response::builder()
            .status(StatusCode::SWITCHING_PROTOCOLS)
            .header("connection", "upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-accept", accept)
            .body(empty())
            .unwrap();
    }
    if req.uri().path().ends_with("/stream") {
        let chunks = futures::stream::unfold(0, |n| async move {
            match n {
                0 => Some((Ok::<_, BoxError>(Frame::data(Bytes::from("first"))), 1)),
                1 => {
                    tokio::time::sleep(Duration::from_millis(1500)).await;
                    Some((Ok(Frame::data(Bytes::from("second"))), 2))
                }
                _ => None,
            }
        });
        return Response::new(BodyExt::boxed(StreamBody::new(chunks)));
    }
    let host = req.headers().get("host").unwrap().to_str().unwrap().to_string();
    page(req.uri().path(), &host).unwrap_or_else(|| Response::new(proxy::full("ok")))
}

/// Responses that carry URLs, as a runtime unaware of the prefix writes
/// them. `host` is the runtime's own loopback address.
fn page(path: &str, host: &str) -> Option<Response<Body>> {
    let resp = Response::builder();
    let resp = match path.rsplit('/').next().unwrap() {
        "page" => resp.header("content-type", "text/html; charset=utf-8").body(proxy::full(format!(
            r#"<a href="https://neboai.com/chat">chat</a><script src="http://{host}/app.js"></script><a href="https://example.com/x">x</a>"#
        ))),
        "loopback.json" => resp.header("content-type", "application/json").body(proxy::full(format!(
            r#"{{"url":"http://{host}/x","escaped":"https:\/\/neboai.com\/y","origin":"https://neboai.com"}}"#
        ))),
        "gzip" => {
            use std::io::Write;
            let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            gz.write_all(br#"<a href="https://neboai.com/chat">chat</a>"#).unwrap();
            resp.header("content-type", "text/html")
                .header("content-encoding", "gzip")
                .body(proxy::full(gz.finish().unwrap()))
        }
        "redirect" => resp
            .status(StatusCode::FOUND)
            .header("location", "https://neboai.com/login?next=/")
            .header("content-location", "/login")
            .header("set-cookie", "sid=1; Path=/; HttpOnly")
            .body(proxy::full("")),
        "events" => {
            let chunks = futures::stream::unfold(0, |n| async move {
                match n {
                    0 => Some((Ok::<_, BoxError>(Frame::data(Bytes::from("data: https://neboai"))), 1)),
                    1 => {
                        tokio::time::sleep(Duration::from_millis(1500)).await;
                        Some((Ok(Frame::data(Bytes::from(".com/x\n\n"))), 2))
                    }
                    _ => None,
                }
            });
            resp.header("content-type", "text/event-stream")
                .body(BodyExt::boxed(StreamBody::new(chunks)))
        }
        _ => return None,
    };
    Some(resp.unwrap())
}

fn empty() -> Body {
    Empty::<Bytes>::new().map_err(|never| match never {}).boxed()
}

/// The link's endpoints, recording model toggles.
#[derive(Default)]
struct FakeControl {
    toggles: Mutex<Vec<bool>>,
}

impl Control for FakeControl {
    fn status(&self) -> serde_json::Value {
        serde_json::json!({ "online": true })
    }
    async fn set_models(&self, enabled: bool) -> Result<serde_json::Value, String> {
        self.toggles.lock().unwrap().push(enabled);
        Ok(serde_json::json!({ "enabled": enabled }))
    }
}

fn target(runtime: &str, upstream: SocketAddr) -> Target {
    let route = match runtime {
        "openclaw" => ProxyRoute {
            path_mode: PathMode::ReaddPrefix,
            origin: Some("https://neboai.com".into()),
            identity_header: Some("x-nebo-user".into()),
        },
        _ => ProxyRoute {
            path_mode: PathMode::StripWithForwardedPrefix,
            origin: None,
            identity_header: None,
        },
    };
    Target {
        upstream: Some(upstream),
        base_path: BOT.into(),
        route,
        identity: OWNER.into(),
        runtime_name: "Test",
    }
}

async fn start_proxy(target: Target, secret: &str) -> (SocketAddr, Arc<FakeControl>) {
    let listener = proxy::bind_loopback("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let control = Arc::new(FakeControl::default());
    tokio::spawn(proxy::serve(listener, target, secret.into(), control.clone(), None));
    (addr, control)
}

/// One HTTP/1.1 request over `io`, as the hub's tunnel would send it: the
/// prefix already stripped, the browser's Host, and spoofed headers.
async fn send<T>(io: T, method: &str, path: &str, stamp: Option<&str>, body: &str) -> Response<Incoming>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await.unwrap();
    tokio::spawn(conn);
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "neboai.com")
        .header("origin", "https://evil.example")
        .header("x-nebo-user", "attacker")
        .header("x-forwarded-for", "127.0.0.1")
        .header("x-forwarded-prefix", "/elsewhere");
    if let Some(stamp) = stamp {
        req = req.header("x-nebo-tunnel-auth", stamp);
    }
    sender
        .send_request(req.body(http_body_util::Full::new(Bytes::from(body.to_string()))).unwrap())
        .await
        .unwrap()
}

/// A stamped GET with `headers`, as the tunnel delivers it.
async fn fetch(addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Response<Incoming> {
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(TcpStream::connect(addr).await.unwrap()))
        .await
        .unwrap();
    tokio::spawn(conn);
    let mut req = Request::builder().uri(path).header("host", "neboai.com").header("x-nebo-tunnel-auth", "s3cret");
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
    sender.send_request(req.body(empty()).unwrap()).await.unwrap()
}

async fn text(resp: Response<Incoming>) -> String {
    String::from_utf8(resp.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap()
}

fn header<'a>(seen: &'a Seen, name: &str) -> Option<&'a str> {
    seen.headers.get(name).map(|v| v.to_str().unwrap())
}

#[tokio::test]
async fn requests_without_the_tunnel_stamp_are_refused() {
    let (upstream, log) = fake_runtime().await;
    let (addr, _) = start_proxy(target("openclaw", upstream), "s3cret").await;
    for stamp in [None, Some("wrong")] {
        let resp = send(TcpStream::connect(addr).await.unwrap(), "GET", "/", stamp, "").await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        let resp = send(TcpStream::connect(addr).await.unwrap(), "GET", "/_link/status", stamp, "").await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }
    assert!(log.lock().unwrap().is_empty(), "nothing unstamped reached the runtime");
}

#[tokio::test]
async fn openclaw_gets_the_prefix_back_and_trusted_proxy_headers() {
    let (upstream, log) = fake_runtime().await;
    let (addr, _) = start_proxy(target("openclaw", upstream), "s3cret").await;
    let resp = send(TcpStream::connect(addr).await.unwrap(), "GET", "/assets/app.js?v=1", Some("s3cret"), "").await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(text(resp).await, "ok");

    let seen = log.lock().unwrap()[0].clone();
    assert_eq!(seen.uri, "/t/test-bot/assets/app.js?v=1");
    assert_eq!(header(&seen, "host"), Some(upstream.to_string().as_str()));
    assert_eq!(header(&seen, "origin"), Some("https://neboai.com"));
    assert_eq!(header(&seen, "x-nebo-user"), Some(OWNER));
    assert_eq!(header(&seen, "x-forwarded-for"), Some(FORWARDED_FOR));
    assert_eq!(header(&seen, "x-forwarded-proto"), Some("https"));
    assert_eq!(header(&seen, "x-forwarded-host"), Some("neboai.com"));
    assert_eq!(header(&seen, "x-forwarded-prefix"), None);
    assert_eq!(header(&seen, "x-nebo-tunnel-auth"), None, "the stamp never reaches the runtime");
}

#[tokio::test]
async fn hermes_gets_the_path_stripped_a_forwarded_prefix_and_no_origin() {
    let (upstream, log) = fake_runtime().await;
    let (addr, _) = start_proxy(target("hermes", upstream), "s3cret").await;
    let resp = send(TcpStream::connect(addr).await.unwrap(), "GET", "/", Some("s3cret"), "").await;
    assert_eq!(resp.status(), StatusCode::OK);

    let seen = log.lock().unwrap()[0].clone();
    assert_eq!(seen.uri, "/");
    assert_eq!(header(&seen, "host"), Some(upstream.to_string().as_str()));
    assert_eq!(header(&seen, "origin"), None);
    assert_eq!(header(&seen, "x-forwarded-prefix"), Some(BOT));
    assert_eq!(header(&seen, "x-nebo-tunnel-auth"), None);
}

#[tokio::test]
async fn link_endpoints_are_served_by_the_link() {
    let (upstream, log) = fake_runtime().await;
    let (addr, control) = start_proxy(target("openclaw", upstream), "s3cret").await;

    let resp = send(TcpStream::connect(addr).await.unwrap(), "GET", "/_link/status", Some("s3cret"), "").await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(text(resp).await, r#"{"online":true}"#);

    let resp = send(
        TcpStream::connect(addr).await.unwrap(),
        "POST",
        "/_link/models",
        Some("s3cret"),
        r#"{"enabled":true}"#,
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(text(resp).await, r#"{"enabled":true}"#);
    assert_eq!(*control.toggles.lock().unwrap(), vec![true]);

    let resp = send(TcpStream::connect(addr).await.unwrap(), "POST", "/_link/models", Some("s3cret"), "nope").await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(log.lock().unwrap().is_empty(), "/_link/ never reaches the runtime");
}

#[tokio::test]
async fn bodies_stream_unbuffered() {
    let (upstream, _) = fake_runtime().await;
    let (addr, _) = start_proxy(target("hermes", upstream), "s3cret").await;
    let resp = send(TcpStream::connect(addr).await.unwrap(), "GET", "/api/events/stream", Some("s3cret"), "").await;
    let mut body = resp.into_body();
    let first = tokio::time::timeout(Duration::from_millis(1000), body.frame())
        .await
        .expect("the first chunk arrives before the upstream sends the second")
        .unwrap()
        .unwrap();
    assert_eq!(first.into_data().unwrap(), "first");
    let second = body.frame().await.unwrap().unwrap();
    assert_eq!(second.into_data().unwrap(), "second");
}

#[tokio::test]
async fn upstream_down_reads_as_could_not_connect() {
    let unused = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap();
    let (addr, _) = start_proxy(target("openclaw", unused), "s3cret").await;
    let resp = send(TcpStream::connect(addr).await.unwrap(), "GET", "/", Some("s3cret"), "").await;
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(text(resp).await, "Could not connect to Test. Try again.");
}

async fn websocket_echo<T>(io: T, path: &str, stamp: Option<&str>)
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut request = tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(format!(
        "ws://neboai.com{path}"
    ))
    .unwrap();
    request.headers_mut().insert("origin", "https://evil.example".parse().unwrap());
    if let Some(stamp) = stamp {
        request.headers_mut().insert("x-nebo-tunnel-auth", stamp.parse().unwrap());
    }
    let (mut ws, resp) = tokio_tungstenite::client_async(request, io).await.unwrap();
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    for n in 0..3 {
        ws.send(Message::text(format!("hi {n}"))).await.unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(reply.to_text().unwrap(), format!("echo hi {n}"));
    }
}

#[tokio::test]
async fn websockets_upgrade_with_the_runtime_headers() {
    for (runtime, path, upstream_path, origin) in [
        ("openclaw", "/", "/t/test-bot/", Some("https://neboai.com")),
        ("hermes", "/api/ws", "/api/ws", None),
    ] {
        let (upstream, log) = fake_runtime().await;
        let (addr, _) = start_proxy(target(runtime, upstream), "s3cret").await;
        websocket_echo(TcpStream::connect(addr).await.unwrap(), path, Some("s3cret")).await;
        let seen = log.lock().unwrap()[0].clone();
        assert_eq!(seen.uri, upstream_path, "{runtime}");
        assert_eq!(header(&seen, "origin"), origin, "{runtime}");
        assert_eq!(header(&seen, "upgrade"), Some("websocket"), "{runtime}");
        assert_eq!(header(&seen, "host"), Some(upstream.to_string().as_str()), "{runtime}");
    }
}

// ── URL rewriting ───────────────────────────────────────────────────────

/// OpenClaw names its canvas surface by origin only; the browser gets it
/// under the tunnel prefix, and the rest of the conversation is unchanged.
#[tokio::test]
async fn the_canvas_url_in_the_websocket_hello_gets_the_prefix() {
    let (upstream, _) = fake_runtime().await;
    let (addr, _) = start_proxy(target("openclaw", upstream), "s3cret").await;
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request("ws://neboai.com/").unwrap();
    request.headers_mut().insert("x-nebo-tunnel-auth", "s3cret".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::client_async(request, TcpStream::connect(addr).await.unwrap())
        .await
        .unwrap();
    ws.send(Message::text("hello")).await.unwrap();
    let hello = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap().unwrap().unwrap();
    let hello: serde_json::Value = serde_json::from_str(hello.to_text().unwrap()).unwrap();
    assert_eq!(
        hello["pluginSurfaceUrls"]["canvas"],
        "https://neboai.com:443/t/test-bot/__openclaw__/cap/tok"
    );
    ws.send(Message::binary(Bytes::from_static(b"https://neboai.com/raw"))).await.unwrap();
    let binary = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(binary.into_data(), "https://neboai.com/raw", "binary frames pass untouched");
}

/// Hermes serves at its root, so a public URL the browser sends it loses
/// the prefix, and gets it back on the way out.
#[tokio::test]
async fn inbound_frames_and_referer_follow_the_path_mode() {
    let (upstream, log) = fake_runtime().await;
    let (addr, _) = start_proxy(target("hermes", upstream), "s3cret").await;
    let mut request =
        tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request("ws://neboai.com/api/ws")
            .unwrap();
    request.headers_mut().insert("x-nebo-tunnel-auth", "s3cret".parse().unwrap());
    request
        .headers_mut()
        .insert("sec-websocket-extensions", "permessage-deflate; client_max_window_bits".parse().unwrap());
    let (mut ws, resp) = tokio_tungstenite::client_async(request, TcpStream::connect(addr).await.unwrap())
        .await
        .unwrap();
    assert!(resp.headers().get("sec-websocket-extensions").is_none());
    ws.send(Message::text("open https://neboai.com/t/test-bot/x")).await.unwrap();
    let reply = tokio::time::timeout(Duration::from_secs(5), ws.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(reply.to_text().unwrap(), "echo open https://neboai.com/t/test-bot/x");
    let seen = log.lock().unwrap()[0].clone();
    assert_eq!(*seen.messages.lock().unwrap(), vec!["open https://neboai.com/x".to_string()]);
    assert_eq!(header(&seen, "sec-websocket-extensions"), None);

    fetch(addr, "/", &[("referer", "https://neboai.com/t/test-bot/sessions")]).await;
    let seen = log.lock().unwrap()[1].clone();
    assert_eq!(header(&seen, "referer"), Some("https://neboai.com/sessions"));
}

#[tokio::test]
async fn html_json_and_redirects_point_through_the_tunnel() {
    let (upstream, _) = fake_runtime().await;
    let (addr, _) = start_proxy(target("hermes", upstream), "s3cret").await;

    let resp = fetch(addr, "/page", &[]).await;
    assert!(resp.headers().get("content-length").is_none());
    assert_eq!(
        text(resp).await,
        r#"<a href="https://neboai.com/t/test-bot/chat">chat</a><script src="https://neboai.com/t/test-bot/app.js"></script><a href="https://example.com/x">x</a>"#
    );

    let resp = fetch(addr, "/loopback.json", &[]).await;
    assert_eq!(
        text(resp).await,
        r#"{"url":"https://neboai.com/t/test-bot/x","escaped":"https:\/\/neboai.com\/t\/test-bot\/y","origin":"https://neboai.com"}"#
    );

    let resp = fetch(addr, "/redirect", &[]).await;
    assert_eq!(resp.status(), StatusCode::FOUND);
    let get = |name: &str| resp.headers().get(name).unwrap().to_str().unwrap().to_string();
    assert_eq!(get("location"), "https://neboai.com/t/test-bot/login?next=/");
    assert_eq!(get("content-location"), "/t/test-bot/login");
    assert_eq!(get("set-cookie"), "sid=1; Path=/t/test-bot; HttpOnly");

    // Unrewritten types stream as they are.
    assert_eq!(text(fetch(addr, "/other", &[]).await).await, "ok");
}

#[tokio::test]
async fn compressed_text_is_rewritten_and_stays_compressed() {
    let (upstream, log) = fake_runtime().await;
    let (addr, _) = start_proxy(target("openclaw", upstream), "s3cret").await;
    let resp = fetch(addr, "/gzip", &[("accept-encoding", "gzip, zstd;q=0.5")]).await;
    assert_eq!(resp.headers().get("content-encoding").unwrap(), "gzip");
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let mut html = String::new();
    std::io::Read::read_to_string(&mut flate2::read::GzDecoder::new(&body[..]), &mut html).unwrap();
    assert_eq!(html, r#"<a href="https://neboai.com/t/test-bot/chat">chat</a>"#);
    let seen = log.lock().unwrap()[0].clone();
    assert_eq!(header(&seen, "accept-encoding"), Some("gzip"), "only codings the link can read");
}

/// An event stream is rewritten without waiting for the response to end: a
/// URL split across chunks is completed when its end arrives.
#[tokio::test]
async fn event_streams_are_rewritten_as_they_stream() {
    let (upstream, _) = fake_runtime().await;
    let (addr, _) = start_proxy(target("hermes", upstream), "s3cret").await;
    let mut body = fetch(addr, "/events", &[]).await.into_body();
    let first = tokio::time::timeout(Duration::from_millis(1000), body.frame())
        .await
        .expect("the first chunk arrives before the upstream sends the second")
        .unwrap()
        .unwrap();
    assert_eq!(first.into_data().unwrap(), "data: ");
    let rest = body.collect().await.unwrap().to_bytes();
    assert_eq!(rest, "https://neboai.com/t/test-bot/x\n\n");
}

// ── Through the real tunnel ─────────────────────────────────────────────

/// A hub's side of the tunnel: accepts the bot's WebSocket and runs yamux
/// as the client, opening one stream per request the way the hub does.
async fn fake_hub() -> (String, mpsc::Receiver<mpsc::Sender<oneshot::Sender<yamux::Stream>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://127.0.0.1:{}/tunnel/connect", listener.local_addr().unwrap().port());
    let (ready_tx, ready_rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let (sock, _) = listener.accept().await.unwrap();
        let ws = tokio_tungstenite::accept_async(sock).await.unwrap();
        let mut conn = yamux::Connection::new(WsBytes::new(ws), yamux::Config::default(), yamux::Mode::Client);
        let (open_tx, mut open_rx) = mpsc::channel::<oneshot::Sender<yamux::Stream>>(8);
        ready_tx.send(open_tx).await.unwrap();
        loop {
            tokio::select! {
                Some(reply) = open_rx.recv() => {
                    let stream = futures::future::poll_fn(|cx| conn.poll_new_outbound(cx)).await.unwrap();
                    let _ = reply.send(stream);
                }
                inbound = futures::future::poll_fn(|cx| conn.poll_next_inbound(cx)) => {
                    if inbound.is_none() { return; }
                }
            }
        }
    });
    (url, ready_rx)
}

async fn open(hub: &mpsc::Sender<oneshot::Sender<yamux::Stream>>) -> tokio_util::compat::Compat<yamux::Stream> {
    use tokio_util::compat::FuturesAsyncReadCompatExt;
    let (tx, rx) = oneshot::channel();
    hub.send(tx).await.unwrap();
    rx.await.unwrap().compat()
}

#[tokio::test]
async fn owner_requests_flow_from_the_hub_through_the_tunnel() {
    let (upstream, log) = fake_runtime().await;
    let secret = nebo_comm::tunnel::tunnel_auth_secret();
    let (proxy_addr, _) = start_proxy(target("openclaw", upstream), secret).await;
    let (hub_url, mut ready) = fake_hub().await;
    let online = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let tunnel_online = online.clone();
    tokio::spawn(async move {
        let _ = nebo_comm::tunnel::run(&hub_url, "test-token", &proxy_addr.to_string(), &tunnel_online).await;
    });
    let hub = tokio::time::timeout(Duration::from_secs(5), ready.recv()).await.unwrap().unwrap();

    // A plain request: the tunnel stamps it, the proxy admits it, the runtime
    // gets the OpenClaw rewrite and never the stamp.
    let resp = send(open(&hub).await, "GET", "/chat?session=main", None, "").await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(text(resp).await, "ok");
    {
        let seen = log.lock().unwrap()[0].clone();
        assert_eq!(seen.uri, "/t/test-bot/chat?session=main");
        assert_eq!(header(&seen, "x-nebo-user"), Some(OWNER));
        assert_eq!(header(&seen, "x-nebo-tunnel-auth"), None);
    }
    assert!(online.load(std::sync::atomic::Ordering::Relaxed));

    // A forged stamp from the browser side is replaced by the tunnel's own.
    let resp = send(open(&hub).await, "GET", "/_link/status", Some("forged"), "").await;
    assert_eq!(resp.status(), StatusCode::OK);

    // The Control UI's WebSocket, end to end.
    websocket_echo(open(&hub).await, "/", None).await;
}

/// Bytes over WebSocket binary frames (the hub side of `nebo_comm`'s WsIo).
struct WsBytes {
    ws: tokio_tungstenite::WebSocketStream<TcpStream>,
    buf: Bytes,
}

impl WsBytes {
    fn new(ws: tokio_tungstenite::WebSocketStream<TcpStream>) -> Self {
        Self { ws, buf: Bytes::new() }
    }
}

impl futures::AsyncRead for WsBytes {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, out: &mut [u8]) -> Poll<std::io::Result<usize>> {
        loop {
            if !self.buf.is_empty() {
                let n = out.len().min(self.buf.len());
                let chunk = self.buf.split_to(n);
                out[..n].copy_from_slice(&chunk);
                return Poll::Ready(Ok(n));
            }
            match futures::ready!(self.ws.poll_next_unpin(cx)) {
                Some(Ok(Message::Binary(data))) => self.buf = data,
                Some(Ok(Message::Close(_))) | None => return Poll::Ready(Ok(0)),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Poll::Ready(Err(std::io::Error::other(e))),
            }
        }
    }
}

impl futures::AsyncWrite for WsBytes {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, data: &[u8]) -> Poll<std::io::Result<usize>> {
        futures::ready!(self.ws.poll_ready_unpin(cx)).map_err(std::io::Error::other)?;
        self.ws
            .start_send_unpin(Message::Binary(Bytes::copy_from_slice(data)))
            .map_err(std::io::Error::other)?;
        Poll::Ready(Ok(data.len()))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.ws.poll_flush_unpin(cx).map_err(std::io::Error::other)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.ws.poll_close_unpin(cx).map_err(std::io::Error::other)
    }
}
