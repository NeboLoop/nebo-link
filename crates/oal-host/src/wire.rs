//! One client connection as the host reads it, whatever carries it: a
//! WebSocket on the LAN, or a stream of the relay's tunnel. Each is pumped
//! into a [`Wire`] (whole messages in, whole messages and a close out), and
//! an encrypted session runs over the wire's binary messages
//! ([`Transport`]).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Sink, SinkExt, Stream, StreamExt};
use oal_relay::host::ClientConn;
use oal_relay::wire::Message as RelayMessage;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;

/// A message from the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Incoming {
    Text(String),
    Binary(Vec<u8>),
}

/// A message to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outgoing {
    /// A text frame: only to refuse a plaintext client in words it reads.
    Text(String),
    Binary(Vec<u8>),
    /// Closes the connection with this code and reason.
    Close(u16, String),
}

/// A client connection: its messages, and a way to answer and close it. The
/// connection is gone when `rx` ends.
pub struct Wire {
    pub rx: mpsc::Receiver<Incoming>,
    pub tx: mpsc::UnboundedSender<Outgoing>,
}

impl Wire {
    /// A wire and the far end of it, for a transport this crate doesn't
    /// pump itself.
    pub fn pair() -> (Wire, mpsc::Sender<Incoming>, mpsc::UnboundedReceiver<Outgoing>) {
        let (in_tx, rx) = mpsc::channel(64);
        let (tx, out_rx) = mpsc::unbounded_channel();
        (Wire { rx, tx }, in_tx, out_rx)
    }
}

/// Pumps a WebSocket (the LAN's) into a wire.
pub fn websocket<S>(ws: WebSocketStream<S>) -> Wire
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (wire, in_tx, mut out_rx) = Wire::pair();
    tokio::spawn(async move {
        let (mut sink, mut stream) = ws.split();
        loop {
            tokio::select! {
                message = stream.next() => {
                    let incoming = match message {
                        Some(Ok(Message::Text(text))) => Incoming::Text(text.to_string()),
                        Some(Ok(Message::Binary(bytes))) => Incoming::Binary(bytes.to_vec()),
                        Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                        Some(Ok(Message::Close(_)) | Err(_)) | None => break,
                    };
                    if in_tx.send(incoming).await.is_err() {
                        break;
                    }
                }
                out = out_rx.recv() => match out {
                    Some(Outgoing::Text(text)) => {
                        if sink.send(Message::text(text)).await.is_err() {
                            break;
                        }
                    }
                    Some(Outgoing::Binary(bytes)) => {
                        if sink.send(Message::Binary(bytes.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(Outgoing::Close(code, reason)) => {
                        let frame = CloseFrame { code: code.into(), reason: reason.into() };
                        let _ = sink.send(Message::Close(Some(frame))).await;
                        break;
                    }
                    None => {
                        let _ = sink.close().await;
                        break;
                    }
                },
            }
        }
    });
    wire
}

/// Pumps a client connection the relay carried into a wire.
pub fn relay(conn: ClientConn) -> Wire {
    let (wire, in_tx, mut out_rx) = Wire::pair();
    let ClientConn { mut tx, mut rx, .. } = conn;
    tokio::spawn(async move {
        loop {
            tokio::select! {
                message = rx.recv() => {
                    let incoming = match message {
                        Some(Ok(RelayMessage::Text(text))) => Incoming::Text(text),
                        Some(Ok(RelayMessage::Binary(bytes))) => Incoming::Binary(bytes.to_vec()),
                        Some(Ok(RelayMessage::Close { .. }) | Err(_)) | None => break,
                    };
                    if in_tx.send(incoming).await.is_err() {
                        break;
                    }
                }
                out = out_rx.recv() => match out {
                    Some(Outgoing::Text(text)) => {
                        if tx.send(RelayMessage::Text(text)).await.is_err() {
                            break;
                        }
                    }
                    Some(Outgoing::Binary(bytes)) => {
                        if tx.send(RelayMessage::Binary(bytes.into())).await.is_err() {
                            break;
                        }
                    }
                    Some(Outgoing::Close(code, reason)) => {
                        let _ = tx.send(RelayMessage::Close { code: Some(code), reason }).await;
                        break;
                    }
                    None => {
                        let _ = tx.send(RelayMessage::Close { code: Some(1000), reason: String::new() }).await;
                        break;
                    }
                },
            }
        }
    });
    wire
}

/// A wire's binary messages as `oal_secure`'s transport, with the message
/// already read to tell a pairing from a session put back first. A text
/// message on it is an error: the connection is encrypted.
pub struct Transport {
    first: Option<Vec<u8>>,
    rx: mpsc::Receiver<Incoming>,
    tx: mpsc::UnboundedSender<Outgoing>,
}

impl Transport {
    pub fn new(first: Vec<u8>, rx: mpsc::Receiver<Incoming>, tx: mpsc::UnboundedSender<Outgoing>) -> Self {
        Self { first: Some(first), rx, tx }
    }
}

impl Stream for Transport {
    type Item = io::Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(first) = self.first.take() {
            return Poll::Ready(Some(Ok(first)));
        }
        self.rx.poll_recv(cx).map(|message| match message? {
            Incoming::Binary(bytes) => Some(Ok(bytes)),
            Incoming::Text(_) => Some(Err(io::Error::new(io::ErrorKind::InvalidData, "a text message on an encrypted connection"))),
        })
    }
}

impl Sink<Vec<u8>> for Transport {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Vec<u8>) -> io::Result<()> {
        self.tx
            .send(Outgoing::Binary(item))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "the connection closed"))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
