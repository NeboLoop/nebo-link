//! The NeboAI models endpoint in front of a fake Janus: the runtime's local
//! key is required, the bot token (current after a rotation) and purpose go
//! upstream, and completions stream through unbuffered.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use nebo_link::janus::{self, Janus};
use nebo_link::proxy::{self, BoxError};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[derive(Debug, Clone)]
struct Seen {
    method: String,
    uri: String,
    authorization: Option<String>,
    bot: Option<String>,
    purpose: Option<String>,
    body: String,
}

async fn fake_janus(models: &'static str) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let log: Arc<Mutex<Vec<Seen>>> = Arc::default();
    let seen = log.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = seen.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |req: Request<Incoming>| {
                    let seen = seen.clone();
                    async move {
                        let header = |name: &str| req.headers().get(name).map(|v| v.to_str().unwrap().to_string());
                        let mut entry = Seen {
                            method: req.method().to_string(),
                            uri: req.uri().to_string(),
                            authorization: header("authorization"),
                            bot: header("x-bot-id"),
                            purpose: header("x-purpose"),
                            body: String::new(),
                        };
                        let path = req.uri().path().to_string();
                        entry.body = String::from_utf8(req.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap();
                        seen.lock().unwrap().push(entry);
                        let resp: Response<proxy::Body> = if path == "/v1/models" {
                            Response::new(proxy::full(models))
                        } else {
                            let chunks = futures::stream::unfold(0, |n| async move {
                                match n {
                                    0 => Some((Ok::<_, BoxError>(Frame::data(Bytes::from("data: 1\n\n"))), 1)),
                                    1 => {
                                        tokio::time::sleep(Duration::from_millis(1500)).await;
                                        Some((Ok(Frame::data(Bytes::from("data: [DONE]\n\n"))), 2))
                                    }
                                    _ => None,
                                }
                            });
                            Response::builder()
                                .header("content-type", "text/event-stream")
                                .body(BodyExt::boxed(StreamBody::new(chunks)))
                                .unwrap()
                        };
                        Ok::<_, std::convert::Infallible>(resp)
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    (url, log)
}

fn endpoint(url: String, token: watch::Receiver<String>) -> Janus {
    Janus {
        url,
        bot_id: "bot-1".into(),
        token,
        key: "local-key".into(),
        client: janus::client(),
    }
}

async fn start(janus: Janus) -> SocketAddr {
    let listener = proxy::bind_loopback("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(janus::serve(listener, janus));
    addr
}

async fn call(addr: SocketAddr, method: &str, path: &str, key: Option<&str>, body: &str) -> Response<Incoming> {
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(TcpStream::connect(addr).await.unwrap()))
        .await
        .unwrap();
    tokio::spawn(conn);
    let mut req = Request::builder()
        .method(method)
        .uri(path)
        .header("host", "127.0.0.1")
        .header("content-type", "application/json");
    if let Some(key) = key {
        req = req.header("authorization", format!("Bearer {key}"));
    }
    sender
        .send_request(req.body(Full::new(Bytes::from(body.to_string()))).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn the_local_key_is_required_and_the_bot_token_goes_upstream() {
    let (url, log) = fake_janus(r#"{"data":[]}"#).await;
    let (token_tx, token_rx) = watch::channel("jwt-1".to_string());
    let addr = start(endpoint(url, token_rx)).await;

    for key in [None, Some("wrong")] {
        let resp = call(addr, "GET", "/v1/models", key, "").await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
    assert_eq!(call(addr, "POST", "/v1/embeddings", Some("local-key"), "").await.status(), StatusCode::NOT_FOUND);
    assert!(log.lock().unwrap().is_empty());

    let resp = call(addr, "GET", "/v1/models", Some("local-key"), "").await;
    assert_eq!(resp.status(), StatusCode::OK);
    // The hub rotates the token on every connect; the next call uses the new one.
    token_tx.send_replace("jwt-2".into());
    let resp = call(addr, "POST", "/v1/chat/completions", Some("local-key"), r#"{"stream":true}"#).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()["content-type"], "text/event-stream");
    let mut body = resp.into_body();
    let first = tokio::time::timeout(Duration::from_millis(1000), body.frame())
        .await
        .expect("the first event arrives before Janus sends the next")
        .unwrap()
        .unwrap();
    assert_eq!(first.into_data().unwrap(), "data: 1\n\n");

    let seen = log.lock().unwrap().clone();
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].authorization.as_deref(), Some("Bearer jwt-1"));
    assert_eq!(seen[1].method, "POST");
    assert_eq!(seen[1].uri, "/v1/chat/completions");
    assert_eq!(seen[1].authorization.as_deref(), Some("Bearer jwt-2"));
    assert_eq!(seen[1].bot.as_deref(), Some("bot-1"));
    assert_eq!(seen[1].purpose.as_deref(), Some("linked_runtime"));
    assert_eq!(seen[1].body, r#"{"stream":true}"#);
}

#[tokio::test]
async fn model_list_keeps_neboai_models_and_prefers_nebo_1() {
    let (url, _) = fake_janus(
        r#"{"data":[
            {"id":"other/x","owned_by":"someone"},
            {"id":"nebo-1-pro","name":"Nebo 1 Pro","owned_by":"neboai"},
            {"id":"nebo-1","owned_by":"neboai"}
        ]}"#,
    )
    .await;
    let (_tx, rx) = watch::channel("jwt".to_string());
    let (models, default) = janus::models(&endpoint(url, rx)).await.unwrap();
    assert_eq!(models.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(), ["nebo-1-pro", "nebo-1"]);
    assert_eq!(models[0].name, "Nebo 1 Pro");
    assert_eq!(models[1].name, "nebo-1");
    assert_eq!(default, "nebo-1");

    let (url, _) = fake_janus(r#"{"data":[]}"#).await;
    let (_tx, rx) = watch::channel("jwt".to_string());
    assert!(janus::models(&endpoint(url, rx)).await.is_err());
}
