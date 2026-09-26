//! A fake NeboAI hub: the comms gateway and tunnel endpoints the service
//! dials, with every connection handed to the test.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nebo_comm::frame;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

pub type Ws = WebSocketStream<TcpStream>;

/// How long the hub waits for the service to do the next thing.
pub const PATIENCE: Duration = Duration::from_secs(20);

/// The hub's comms gateway and tunnel endpoints. Every connection the
/// service makes is handed to the test.
pub struct FakeHub {
    pub comms: String,
    pub tunnel: String,
    comms_rx: mpsc::UnboundedReceiver<Ws>,
    tunnel_rx: mpsc::UnboundedReceiver<Ws>,
}

impl FakeHub {
    pub async fn start() -> Self {
        let (comms, comms_rx) = accept_all("/ws").await;
        let (tunnel, tunnel_rx) = accept_all("/tunnel/connect").await;
        Self {
            comms,
            tunnel,
            comms_rx,
            tunnel_rx,
        }
    }

    /// The next comms connection and what its CONNECT announced.
    pub async fn next_connect(&mut self) -> (Ws, serde_json::Value) {
        let mut ws = tokio::time::timeout(PATIENCE, self.comms_rx.recv())
            .await
            .expect("the service dialed the comms gateway")
            .unwrap();
        let message = tokio::time::timeout(PATIENCE, ws.next())
            .await
            .expect("the service sent CONNECT")
            .unwrap()
            .unwrap();
        let data = message.into_data();
        let (header, payload) = frame::decode(&data).unwrap();
        assert_eq!(header.frame_type, frame::TYPE_CONNECT);
        (ws, serde_json::from_slice(payload).unwrap())
    }

    /// Whether the service dialed the comms gateway again.
    pub fn redialed(&mut self) -> bool {
        self.comms_rx.try_recv().is_ok()
    }

    pub async fn next_tunnel(&mut self) -> Ws {
        tokio::time::timeout(PATIENCE, self.tunnel_rx.recv())
            .await
            .expect("the service dialed the tunnel")
            .unwrap()
    }
}

async fn accept_all(path: &str) -> (String, mpsc::UnboundedReceiver<Ws>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}{path}", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            if let Ok(ws) = tokio_tungstenite::accept_async(sock).await {
                let _ = tx.send(ws);
            }
        }
    });
    (url, rx)
}

/// The token the fake hub rotates to on every AUTH_OK, as the real hub does.
pub const ROTATED_TOKEN: &str = "rotated-bot-token";

/// Answers CONNECT: AUTH_OK carrying a rotated token, or AUTH_FAIL with
/// `reason`.
pub async fn answer(ws: &mut Ws, refused: Option<&str>) {
    let (frame_type, payload) = match refused {
        None => (frame::TYPE_AUTH_OK, serde_json::json!({ "ok": true, "token": ROTATED_TOKEN })),
        Some(reason) => (frame::TYPE_AUTH_FAIL, serde_json::json!({ "ok": false, "reason": reason })),
    };
    let data = frame::encode(
        frame::Header {
            frame_type,
            ..Default::default()
        },
        &serde_json::to_vec(&payload).unwrap(),
    )
    .unwrap();
    ws.send(Message::Binary(data.into())).await.unwrap();
}

