//! `chat` reaches NeboAI as it is now, without an unlink: a linked Hermes
//! whose gateway is not running connects announcing nothing; when the
//! gateway comes up (here: a fake API server appears on its port) the
//! service reconnects and CONNECT carries `chat: true`.
//!
//! The runtime's own commands are a recording `hermes` script that exits at
//! once, so the link's foreground starts of the gateway and dashboard end
//! immediately and it keeps trying; the test stands the gateway up itself.

#![cfg(unix)]

#[path = "common/hub.rs"]
mod hub;

use std::convert::Infallible;
use std::net::SocketAddr;

use bytes::Bytes;
use hub::{FakeHub, ROTATED_TOKEN, answer};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use nebo_link::credentials::Credentials;
use nebo_link::endpoints::Endpoints;
use nebo_link::state::{Link, ModelsEndpoint, Root};
use nebo_runtimes::Runtime;
use serde_json::json;
use tokio::net::TcpListener;

const KEY: &str = "0123456789abcdef0123456789abcdef";

/// A Hermes install linked to `hub`, with its API server's port chosen but
/// nothing listening on it.
fn linked_hermes(hub: &FakeHub, api_port: u16, dashboard_port: u16) -> (tempfile::TempDir, Root, String) {
    let tmp = tempfile::tempdir().unwrap();
    let home = tmp.path().join(".hermes");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("config.yaml"),
        format!("platforms:\n  api_server:\n    extra:\n      port: {api_port}\n"),
    )
    .unwrap();
    std::fs::write(home.join(".env"), format!("API_SERVER_KEY={KEY}\n")).unwrap();
    std::fs::write(
        home.join("spawn-ledger.json"),
        format!(r#"[{{"pid": 1, "purpose": "dashboard", "host": "127.0.0.1", "port": {dashboard_port}}}]"#),
    )
    .unwrap();

    let bin = tmp.path().join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let script = bin.join("hermes");
    std::fs::write(
        &script,
        format!("#!/bin/sh\necho \"hermes $*\" >> '{}'\n", tmp.path().join("commands").display()),
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap_or_default());
    // SAFETY: called before the test starts any other thread.
    unsafe { std::env::set_var("PATH", path) };

    let root = Root::at(tmp.path().join("nebo-link"));
    let bot_id = uuid::Uuid::new_v4().to_string();
    let dir = root.bot(&bot_id);
    dir.create().unwrap();
    dir.save(&Link {
        bot_id: bot_id.clone(),
        name: "test-mac · Hermes".into(),
        runtime: Runtime::Hermes,
        owner_id: "owner-1".into(),
        home: home.clone(),
        env: vec![("HERMES_HOME".into(), home.display().to_string())],
        endpoints: Endpoints {
            api: "http://127.0.0.1:9".into(),
            comms: hub.comms.clone(),
            tunnel: hub.tunnel.clone(),
            janus: "http://127.0.0.1:9".into(),
        },
        local_password: "pw".into(),
        models: ModelsEndpoint {
            port: free_port(),
            key: "k".into(),
            enabled: false,
        },
        api_server_key: KEY.into(),
        services: vec![],
        acp: None,
    })
    .unwrap();
    Credentials::open(&dir).save("bot-token").unwrap();
    (tmp, root, bot_id)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// A Hermes API server that answers `/health` and, with the key, every
/// capability the contract needs.
async fn fake_api_server(addr: SocketAddr) {
    let listener = TcpListener::bind(addr).await.unwrap();
    tokio::spawn(async move {
        loop {
            let (sock, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(|req: Request<Incoming>| async move {
                    let authorized = req
                        .headers()
                        .get("authorization")
                        .is_some_and(|v| v == format!("Bearer {KEY}").as_str());
                    let (status, body) = match (req.uri().path(), authorized) {
                        ("/health", _) => (StatusCode::OK, json!({ "status": "ok" })),
                        ("/v1/capabilities", true) => (
                            StatusCode::OK,
                            json!({
                                "object": "hermes.api_server.capabilities",
                                "auth": { "type": "bearer", "required": true },
                                "features": {
                                    "run_submission": true, "run_status": true, "run_events_sse": true,
                                    "run_stop": true, "run_approval_response": true,
                                    "tool_progress_events": true, "approval_events": true,
                                    "session_resources": true, "session_chat": true, "run_steer": true,
                                },
                            }),
                        ),
                        (_, false) => (StatusCode::UNAUTHORIZED, json!({ "error": "unauthorized" })),
                        _ => (StatusCode::NOT_FOUND, json!({ "error": "not found" })),
                    };
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(status)
                            .header("content-type", "application/json")
                            .body(Full::new(Bytes::from(body.to_string())))
                            .unwrap(),
                    )
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(sock), service)
                    .await;
            });
        }
    });
}

#[tokio::test]
async fn chat_is_announced_when_the_gateway_comes_up() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
        .with_test_writer()
        .try_init();
    let mut hub = FakeHub::start().await;
    let api_port = free_port();
    let (tmp, root, bot_id) = linked_hermes(&hub, api_port, free_port());
    let service = tokio::spawn(async move { nebo_link::run::run(&root, &bot_id).await });

    // Nothing answers on the gateway's port: CONNECT announces no chat.
    let (mut ws, payload) = hub.next_connect().await;
    assert_eq!(payload["runtime"], "hermes");
    assert!(payload.get("chat").is_none_or(|c| c == false), "{payload}");
    answer(&mut ws, None).await;
    let _tunnel = hub.next_tunnel().await;
    assert!(!hub.redialed(), "connected, nothing to announce differently");

    // The gateway appears: the service reconnects, announcing chat, with
    // the token the hub rotated to.
    fake_api_server(([127, 0, 0, 1], api_port).into()).await;
    let (mut ws, payload) = hub.next_connect().await;
    assert_eq!(payload["chat"], true, "{payload}");
    assert_eq!(payload["token"], ROTATED_TOKEN);
    answer(&mut ws, None).await;

    let commands = std::fs::read_to_string(tmp.path().join("commands")).unwrap_or_default();
    assert!(commands.contains("hermes gateway run"), "the link tried the gateway itself: {commands}");
    service.abort();
}
