//! `WS /ws`: the phone's chat socket. Frames are `{type, data}` (the
//! phone adds `message_id` and `timestamp` to its own,
//! `mobile lib/api/bot_ws.dart`); inbound frames go to
//! [`Contract::inbound`], and every connected socket gets every broadcast
//! event, filtered by the phone on `agent_id` and `session_id`.

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use tokio::sync::broadcast;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::Role;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use super::{Contract, Outbound};
use crate::proxy::{Body, full, text};

/// The largest frame a phone sends (a prompt with its attachments).
const MAX_FRAME: usize = 4 << 20;

/// Answers the upgrade and serves the socket once it is up.
pub fn upgrade(contract: Arc<Contract>, req: &mut Request<Incoming>) -> Response<Body> {
    let upgrading = req
        .headers()
        .get(header::UPGRADE)
        .is_some_and(|v| v.as_bytes().eq_ignore_ascii_case(b"websocket"));
    let Some(key) = req
        .headers()
        .get(header::SEC_WEBSOCKET_KEY)
        .filter(|_| upgrading)
    else {
        return text(StatusCode::BAD_REQUEST, "expected a WebSocket upgrade");
    };
    let accept = derive_accept_key(key.as_bytes());
    let upgrade = hyper::upgrade::on(req);
    tokio::spawn(async move {
        match upgrade.await {
            Ok(upgraded) => {
                let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
                    .max_message_size(Some(MAX_FRAME))
                    .max_frame_size(Some(MAX_FRAME));
                let socket = WebSocketStream::from_raw_socket(
                    TokioIo::new(upgraded),
                    Role::Server,
                    Some(config),
                )
                .await;
                serve(contract, socket).await;
            }
            Err(e) => tracing::info!(error = %e, "contract: the socket upgrade failed"),
        }
    });
    let mut resp = Response::new(full(""));
    *resp.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    resp.headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    resp.headers_mut()
        .insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    if let Ok(accept) = HeaderValue::from_str(&accept) {
        resp.headers_mut()
            .insert(header::SEC_WEBSOCKET_ACCEPT, accept);
    }
    resp
}

async fn serve<S>(contract: Arc<Contract>, socket: WebSocketStream<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut tx, mut rx) = socket.split();
    let mut events = contract.subscribe();
    loop {
        tokio::select! {
            inbound = rx.next() => {
                let frame = match inbound {
                    Some(Ok(Message::Text(text))) => text,
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(_)) => continue,
                    Some(Err(WsError::ConnectionClosed | WsError::AlreadyClosed)) => break,
                    Some(Err(e)) => {
                        tracing::info!(error = %e, "contract: socket read failed");
                        break;
                    }
                };
                let Ok(frame) = serde_json::from_str::<Value>(frame.as_str()) else {
                    continue;
                };
                if let Some(reply) = contract.inbound(&frame)
                    && tx.send(Message::text(reply.to_string())).await.is_err()
                {
                    break;
                }
            }
            event = events.recv() => {
                let Outbound { kind, data } = match event {
                    Ok(event) => event,
                    // A phone that fell behind gets what comes next; the
                    // thread's transcript is one GET away.
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                let frame = json!({ "type": kind, "data": data });
                if tx.send(Message::text(frame.to_string())).await.is_err() {
                    break;
                }
            }
        }
    }
    let _ = tx.close().await;
}
