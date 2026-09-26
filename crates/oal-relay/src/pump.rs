//! Carries one WebSocket over one tunnel stream, both ways, until either side
//! closes. The relay runs it between a client's WebSocket and the host's
//! stream; a host runs it ([`crate::host::forward`]) between the stream and
//! its own OAL WebSocket.
//!
//! Messages are forwarded one to one, in order and unchanged; nothing here
//! looks inside them. Each direction is its own task with a bounded queue, so
//! a slow reader on one side slows the sender on the other (yamux's per-stream
//! window and TCP do the rest) and a stalled direction never blocks the other.
//! The WebSocket side is pinged every 20 s and closed with 4008 after 60 s of
//! silence (OAL spec section 11).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};

use crate::wire::{Frame, FrameCodec, Message};

pub(crate) type StreamRead = FramedRead<ReadHalf<Compat<yamux::Stream>>, FrameCodec>;
pub(crate) type StreamWrite = FramedWrite<WriteHalf<Compat<yamux::Stream>>, FrameCodec>;

/// A tunnel stream as frames, split for reading and writing.
pub(crate) fn framed(stream: yamux::Stream, max_message: usize) -> (StreamRead, StreamWrite) {
    let (read, write) = tokio::io::split(stream.compat());
    (
        FramedRead::new(read, FrameCodec::new(max_message)),
        FramedWrite::new(write, FrameCodec::new(max_message)),
    )
}

const PING_EVERY: Duration = Duration::from_secs(20);
const SILENCE: Duration = Duration::from_secs(60);
/// How long a closing side gets to put its last frames on the wire.
const DRAIN: Duration = Duration::from_secs(5);
const QUEUE: usize = 8;

/// OAL close codes the relay itself sends (spec section 4.3).
pub(crate) const CLOSE_HOST_GONE: u16 = 1001;
pub(crate) const CLOSE_TOO_BIG: u16 = 1009;
pub(crate) const CLOSE_SILENT: u16 = 4008;
/// On a stream only (never sent on a WebSocket): the WebSocket side went away
/// without a close frame.
const CLOSE_ABNORMAL: u16 = 1006;

/// Message and byte counts, per connection and summed for the relay.
#[derive(Default)]
pub(crate) struct Traffic {
    pub(crate) from_ws_messages: AtomicU64,
    pub(crate) from_ws_bytes: AtomicU64,
    pub(crate) to_ws_messages: AtomicU64,
    pub(crate) to_ws_bytes: AtomicU64,
}

struct Counter {
    local: Traffic,
    total: Arc<Traffic>,
}

impl Counter {
    fn ws_in(&self, bytes: usize) {
        for t in [&self.local, &*self.total] {
            t.from_ws_messages.fetch_add(1, Ordering::Relaxed);
            t.from_ws_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    fn ws_out(&self, bytes: usize) {
        for t in [&self.local, &*self.total] {
            t.to_ws_messages.fetch_add(1, Ordering::Relaxed);
            t.to_ws_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }
}

/// How a pumped connection ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum End {
    /// The WebSocket side closed with this code.
    WsClosed(Option<u16>),
    /// The WebSocket side went away without a close frame.
    WsGone,
    /// The WebSocket side sent a message over the limit.
    WsTooBig,
    /// The stream side closed with this code.
    StreamClosed(Option<u16>),
    /// The stream (the tunnel) went away without a close frame.
    StreamGone,
    /// Closed on purpose with this code (unpaired, shutting down).
    Killed(u16),
    /// The WebSocket side sent nothing for [`SILENCE`].
    Silent,
}

impl End {
    /// The close code the connection ended with, for logs.
    pub(crate) fn code(&self) -> Option<u16> {
        match self {
            End::WsClosed(c) | End::StreamClosed(c) => *c,
            End::WsGone => Some(CLOSE_ABNORMAL),
            End::WsTooBig => Some(CLOSE_TOO_BIG),
            End::StreamGone => Some(CLOSE_HOST_GONE),
            End::Killed(c) => Some(*c),
            End::Silent => Some(CLOSE_SILENT),
        }
    }
}

/// What one connection carried, for its closing log line.
pub(crate) struct Summary {
    pub(crate) end: End,
    pub(crate) from_ws_messages: u64,
    pub(crate) from_ws_bytes: u64,
    pub(crate) to_ws_messages: u64,
    pub(crate) to_ws_bytes: u64,
}

/// Pumps until either side closes, `kill` fires, or the WebSocket side falls
/// silent. The other side is always told how it ended, with the same close
/// code where there is one.
pub(crate) async fn pump<S>(
    ws: WebSocketStream<S>,
    rx: StreamRead,
    tx: StreamWrite,
    mut kill: oneshot::Receiver<u16>,
    total: Arc<Traffic>,
) -> Summary
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let counter = Arc::new(Counter {
        local: Traffic::default(),
        total,
    });
    let (ws_sink, ws_source) = ws.split();
    let (to_ws, to_ws_rx) = mpsc::channel::<WsMessage>(QUEUE);
    let (to_stream, to_stream_rx) = mpsc::channel::<Message>(QUEUE);
    let ws_writer = tokio::spawn(write_ws(ws_sink, to_ws_rx));
    let stream_writer = tokio::spawn(write_stream(tx, to_stream_rx));
    let heard = Arc::new(std::sync::Mutex::new(Instant::now()));
    let mut up = tokio::spawn(ws_to_stream(
        ws_source,
        to_stream.clone(),
        heard.clone(),
        counter.clone(),
    ));
    let mut down = tokio::spawn(stream_to_ws(rx, to_ws.clone(), counter.clone()));

    let mut ping = tokio::time::interval_at(Instant::now() + PING_EVERY, PING_EVERY);
    let end = loop {
        tokio::select! {
            r = &mut up => break r.unwrap_or(End::WsGone),
            r = &mut down => break r.unwrap_or(End::StreamGone),
            code = &mut kill => break code.map_or(End::Killed(CLOSE_HOST_GONE), End::Killed),
            _ = ping.tick() => {
                let silent = heard.lock().map(|t| t.elapsed() >= SILENCE).unwrap_or(false);
                if silent {
                    break End::Silent;
                }
                let _ = to_ws.try_send(WsMessage::Ping(Bytes::new()));
            }
        }
    };

    // Tell each side what the other did. Everything a reader forwarded
    // before it returned is already queued ahead of these.
    let (for_ws, for_stream) = match &end {
        End::WsClosed(code) => (None, Some((*code, "closed"))),
        End::WsGone => (None, Some((Some(CLOSE_ABNORMAL), "gone"))),
        End::WsTooBig => (Some(CLOSE_TOO_BIG), Some((Some(CLOSE_TOO_BIG), "too big"))),
        End::StreamClosed(code) => (*code, None),
        End::StreamGone => (Some(CLOSE_HOST_GONE), None),
        End::Killed(code) => (Some(*code), Some((Some(*code), "closed by the relay"))),
        End::Silent => (
            Some(CLOSE_SILENT),
            Some((Some(CLOSE_SILENT), "no heartbeat")),
        ),
    };
    if !matches!(end, End::WsClosed(_) | End::WsGone) {
        let _ = to_ws.send(ws_close(for_ws)).await;
    }
    if let Some((code, reason)) = for_stream {
        let _ = to_stream
            .send(Message::Close {
                code,
                reason: reason.into(),
            })
            .await;
    }
    drop((to_ws, to_stream));
    up.abort();
    down.abort();
    let _ = tokio::time::timeout(DRAIN, ws_writer).await;
    let _ = tokio::time::timeout(DRAIN, stream_writer).await;

    let l = &counter.local;
    Summary {
        end,
        from_ws_messages: l.from_ws_messages.load(Ordering::Relaxed),
        from_ws_bytes: l.from_ws_bytes.load(Ordering::Relaxed),
        to_ws_messages: l.to_ws_messages.load(Ordering::Relaxed),
        to_ws_bytes: l.to_ws_bytes.load(Ordering::Relaxed),
    }
}

async fn ws_to_stream<S>(
    mut source: futures::stream::SplitStream<WebSocketStream<S>>,
    to_stream: mpsc::Sender<Message>,
    heard: Arc<std::sync::Mutex<Instant>>,
    counter: Arc<Counter>,
) -> End
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let msg = match source.next().await {
            Some(Ok(msg)) => msg,
            Some(Err(tokio_tungstenite::tungstenite::Error::Capacity(_))) => return End::WsTooBig,
            Some(Err(_)) | None => return End::WsGone,
        };
        if let Ok(mut t) = heard.lock() {
            *t = Instant::now();
        }
        let forward = match msg {
            WsMessage::Text(text) => {
                counter.ws_in(text.len());
                Message::Text(text.as_str().to_owned())
            }
            WsMessage::Binary(data) => {
                counter.ws_in(data.len());
                Message::Binary(data)
            }
            WsMessage::Close(frame) => return End::WsClosed(frame.map(|f| u16::from(f.code))),
            WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Frame(_) => continue,
        };
        if to_stream.send(forward).await.is_err() {
            return End::StreamGone;
        }
    }
}

async fn stream_to_ws(
    mut rx: StreamRead,
    to_ws: mpsc::Sender<WsMessage>,
    counter: Arc<Counter>,
) -> End {
    loop {
        let msg = match rx.next().await {
            Some(Ok(Frame::Message(Message::Text(text)))) => {
                counter.ws_out(text.len());
                WsMessage::Text(text.into())
            }
            Some(Ok(Frame::Message(Message::Binary(data)))) => {
                counter.ws_out(data.len());
                WsMessage::Binary(data)
            }
            Some(Ok(Frame::Message(Message::Close { code, .. }))) => {
                return End::StreamClosed(code);
            }
            Some(Ok(Frame::Open(_))) | Some(Err(_)) | None => return End::StreamGone,
        };
        if to_ws.send(msg).await.is_err() {
            return End::WsGone;
        }
    }
}

async fn write_ws<S>(
    mut sink: futures::stream::SplitSink<WebSocketStream<S>, WsMessage>,
    mut rx: mpsc::Receiver<WsMessage>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    while let Some(msg) = rx.recv().await {
        let closing = matches!(msg, WsMessage::Close(_));
        if sink.send(msg).await.is_err() {
            return;
        }
        if closing {
            break;
        }
    }
    let _ = sink.close().await;
}

async fn write_stream(mut tx: StreamWrite, mut rx: mpsc::Receiver<Message>) {
    while let Some(msg) = rx.recv().await {
        let closing = matches!(msg, Message::Close { .. });
        if tx.send(Frame::Message(msg)).await.is_err() {
            return;
        }
        if closing {
            break;
        }
    }
    // Ends the stream (yamux FIN) so the other end's reader finishes too.
    let _ = tx.close().await;
}

/// A close frame carrying `code`, or none when the code may not be sent on
/// the wire (RFC 6455 section 7.4: 1005, 1006 and 1015 are never sent).
pub(crate) fn ws_close(code: Option<u16>) -> WsMessage {
    match code {
        Some(c) if matches!(c, 1000..=1003 | 1007..=1014 | 3000..=4999) => {
            WsMessage::Close(Some(CloseFrame {
                code: CloseCode::from(c),
                reason: "".into(),
            }))
        }
        _ => WsMessage::Close(None),
    }
}
