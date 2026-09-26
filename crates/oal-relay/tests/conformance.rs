//! The OAL conformance suite, through the relay: its fake host (what
//! `oal-conformance client` serves) sits behind a host tunnel via
//! `host::forward`, and its fake client (what `oal-conformance host` runs)
//! drives every recorded example through the relay, pairing included. Every
//! example must pass exactly as it does on a direct connection.
//!
//! The fake client authenticates with static upgrade headers; the relay wants
//! a fresh proof of key possession on every connection. A local shim stands
//! in for the client's relay SDK: each connection the fake client opens to it
//! is carried to the relay by `RelayClient`, with its own proof.

mod common;

use std::sync::Arc;

use common::*;
use futures::{SinkExt, StreamExt};
use oal_conformance::fake_host;
use oal_conformance::spec;
use oal_conformance::transcript::{self, Target};
use oal_relay::host::{self, HostEvent};
use oal_relay::{Keypair, RelayClient};
use oal_secure::PairingCode;
use serde_json::json;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
use tokio_tungstenite::tungstenite::http::HeaderValue;

#[tokio::test]
async fn every_example_passes_through_the_relay() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;

    // The host: a tunnel that forwards every client to the fake host. The
    // code is made the way a host makes it: the relay's nameplate, the
    // host's own secret half.
    let (mut tunnel, _) = host_up(&relay, &Keypair::generate(), "fake-host").await;
    let handle = tunnel.handle();
    let nameplate = handle.nameplate(None).await.unwrap();
    let code = PairingCode::generate(Some(&nameplate.nameplate))
        .unwrap()
        .to_string();
    let fake = fake_host::serve(
        "127.0.0.1:0".parse().unwrap(),
        fake_host::Config {
            code: code.clone(),
            ..fake_host::Config::default()
        },
    )
    .await
    .unwrap();
    let local = format!("ws://{fake}/oal");
    tokio::spawn(async move {
        while let Some(event) = tunnel.next().await {
            if let HostEvent::Client(conn) = event {
                let local = local.clone();
                let handle = handle.clone();
                tokio::spawn(async move {
                    // The fake host pairs inside the connection, out of the
                    // relay's sight; like the `oal-relay host` bridge, record
                    // the device at the relay when its pairing connection
                    // arrives.
                    if conn.nameplate.is_some() {
                        handle.paired(&conn.client_key).await.unwrap();
                    }
                    host::forward(conn, &local).await
                });
            }
        }
    });

    // The client's side of the relay, for the fake client.
    let phone = Arc::new(client(&relay, Keypair::generate()));
    let shim = shim(phone, "fake-host").await;

    let target = Target {
        url: format!("ws://{shim}/host"),
        pair_url: Some(format!("ws://{shim}/pair")),
        headers: Vec::new(),
        known: vec![
            ("code".into(), json!(code)),
            ("agent".into(), json!(fake_host::AGENT)),
        ],
    };
    let examples: Vec<_> = spec::EXAMPLES.iter().collect();
    let outcomes = transcript::run(&target, &examples).await;
    assert_eq!(outcomes.len(), examples.len(), "every example ran");
    for outcome in &outcomes {
        if let Err(e) = &outcome.result {
            panic!("{} failed through the relay: {e}", outcome.example);
        }
    }
    relay.shutdown().await;
}

/// Accepts plain local WebSockets at `/host` and `/pair/<nameplate>` and carries
/// each to the relay as `phone`, message for message, close codes included.
async fn shim(phone: Arc<RelayClient>, host_id: &'static str) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((tcp, _)) = listener.accept().await {
            let phone = phone.clone();
            tokio::spawn(async move {
                let (path_tx, path_rx) = std::sync::mpsc::channel();
                let Ok(local) = tokio_tungstenite::accept_hdr_async(tcp, Accept(path_tx)).await
                else {
                    return;
                };
                let path = path_rx.recv().unwrap_or_default();
                // The suite appends only the nameplate to the pairing
                // endpoint; the whole code goes inside, in `host/pair`.
                let remote = match path.strip_prefix("/pair/") {
                    Some(nameplate) => phone.pair(nameplate).await.map(|(ws, _)| ws),
                    None => phone.connect(host_id).await,
                };
                let Ok(remote) = remote else { return };
                splice(local, remote).await;
            });
        }
    });
    addr
}

/// Selects `oal` (the fake client offers it) and reports the path.
struct Accept(std::sync::mpsc::Sender<String>);

impl Callback for Accept {
    fn on_request(self, req: &Request, mut resp: Response) -> Result<Response, ErrorResponse> {
        let _ = self.0.send(req.uri().path().to_owned());
        resp.headers_mut()
            .insert("sec-websocket-protocol", HeaderValue::from_static("oal"));
        Ok(resp)
    }
}

async fn splice<A, B>(
    a: tokio_tungstenite::WebSocketStream<A>,
    b: tokio_tungstenite::WebSocketStream<B>,
) where
    A: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    B: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio_tungstenite::tungstenite::Message;
    let (mut a_tx, mut a_rx) = a.split();
    let (mut b_tx, mut b_rx) = b.split();
    loop {
        tokio::select! {
            m = a_rx.next() => match m {
                Some(Ok(m @ (Message::Text(_) | Message::Binary(_)))) => { if b_tx.send(m).await.is_err() { break } }
                Some(Ok(Message::Close(f))) => { let _ = b_tx.send(Message::Close(f)).await; break }
                Some(Ok(_)) => {}
                _ => break,
            },
            m = b_rx.next() => match m {
                Some(Ok(m @ (Message::Text(_) | Message::Binary(_)))) => { if a_tx.send(m).await.is_err() { break } }
                Some(Ok(Message::Close(f))) => { let _ = a_tx.send(Message::Close(f)).await; break }
                Some(Ok(_)) => {}
                _ => break,
            },
        }
    }
    let _ = a_tx.close().await;
    let _ = b_tx.close().await;
}
