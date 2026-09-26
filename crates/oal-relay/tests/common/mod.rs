//! Shared by the relay's integration tests: a relay on a free port, a host
//! with its tunnel up, and waiting for what arrives.

#![allow(dead_code)]

use std::path::Path;
use std::time::Duration;

use futures::StreamExt;
use oal_relay::host::{ClientConn, HostEvent, HostTunnel};
use oal_relay::server::{self, Config, RelayHandle};
use oal_relay::wire::PairedClient;
use oal_relay::{Keypair, RelayClient, WsStream};
use tokio_tungstenite::tungstenite::Message as WsMessage;

pub const ADMIN: &str = "test-admin-token";
pub const WAIT: Duration = Duration::from_secs(10);

pub fn config(dir: &Path) -> Config {
    let mut config = Config::new("127.0.0.1:0".parse().unwrap(), dir.to_path_buf());
    config.admin_token = Some(ADMIN.into());
    config
}

pub async fn relay_with(config: Config) -> RelayHandle {
    server::start(config).await.expect("relay starts")
}

pub async fn relay(dir: &Path) -> RelayHandle {
    relay_with(config(dir)).await
}

pub fn url(relay: &RelayHandle) -> String {
    format!("http://{}", relay.local_addr())
}

pub fn client(relay: &RelayHandle, key: Keypair) -> RelayClient {
    RelayClient::new(&url(relay), key).unwrap()
}

pub async fn event(tunnel: &mut HostTunnel) -> HostEvent {
    tokio::time::timeout(WAIT, tunnel.next())
        .await
        .expect("an event in time")
        .expect("the tunnel is open")
}

/// Opens `id`'s tunnel and waits for the relay's list of pairings.
pub async fn host_up(
    relay: &RelayHandle,
    key: &Keypair,
    id: &str,
) -> (HostTunnel, Vec<PairedClient>) {
    let mut tunnel = client(relay, key.clone())
        .host(id)
        .await
        .expect("tunnel opens");
    match event(&mut tunnel).await {
        HostEvent::Registered { host_id, pairings } => {
            assert_eq!(host_id, id);
            (tunnel, pairings)
        }
        other => panic!("expected Registered, got {other:?}"),
    }
}

/// The next client connection, skipping pairing notices.
pub async fn next_client(tunnel: &mut HostTunnel) -> ClientConn {
    loop {
        match event(tunnel).await {
            HostEvent::Client(conn) => return conn,
            HostEvent::Unpaired { .. } => continue,
            other => panic!("expected a client, got {other:?}"),
        }
    }
}

/// Pairs `phone` with the host behind `tunnel` the way a host does it: a
/// nameplate from the relay, the pairing connection through it, and the host
/// confirming the pairing. Returns the pairing connection from both ends.
pub async fn pair(tunnel: &mut HostTunnel, phone: &RelayClient) -> (WsStream, ClientConn) {
    let handle = tunnel.handle();
    let nameplate = handle.nameplate(None).await.expect("a nameplate");
    let (ws, _) = phone.pair(&nameplate.nameplate).await.expect("pairs");
    let conn = next_client(tunnel).await;
    assert_eq!(
        conn.nameplate.as_deref(),
        Some(nameplate.nameplate.as_str())
    );
    handle
        .paired(&conn.client_key)
        .await
        .expect("the relay records it");
    (ws, conn)
}

/// The next data or close message on a client's socket (pings skipped).
pub async fn ws_next(ws: &mut WsStream) -> WsMessage {
    loop {
        let msg = tokio::time::timeout(WAIT, ws.next())
            .await
            .expect("a message in time")
            .expect("the socket is open")
            .expect("a message");
        match msg {
            WsMessage::Ping(_) | WsMessage::Pong(_) => continue,
            other => return other,
        }
    }
}

/// The close code a client's socket ends with.
pub async fn ws_close_code(ws: &mut WsStream) -> Option<u16> {
    loop {
        match tokio::time::timeout(WAIT, ws.next())
            .await
            .expect("closed in time")
        {
            Some(Ok(WsMessage::Close(frame))) => return frame.map(|f| u16::from(f.code)),
            Some(Ok(_)) => continue,
            Some(Err(_)) | None => return None,
        }
    }
}

/// The code of a refusal, or a panic if it was not one.
pub fn refusal<T>(result: Result<T, oal_relay::Error>) -> String {
    match result {
        Err(oal_relay::Error::Refused { code, .. }) => code,
        Err(e) => panic!("expected a refusal, got {e}"),
        Ok(_) => panic!("expected a refusal, got a connection"),
    }
}
