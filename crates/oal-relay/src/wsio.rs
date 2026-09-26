//! The tunnel WebSocket as the byte stream yamux runs over, with the
//! heartbeat both ends of a tunnel share.
//!
//! Each write becomes one binary message; reads drain binary messages. yamux
//! 0.13 has no keepalive of its own, so this adapter keeps the tunnel honest:
//! it sends a WebSocket ping every [`PING_EVERY`] and fails the read once the
//! peer has sent nothing at all for [`SILENCE`], which unwinds the yamux
//! session. A peer that vanished without a TCP close (a laptop asleep, a NAT
//! mapping dropped) is noticed within a minute instead of never.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::{AsyncRead, AsyncWrite, Sink, Stream};
use tokio::time::{Instant, Interval, MissedTickBehavior, Sleep};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;

/// How often each end of a tunnel pings the other.
pub(crate) const PING_EVERY: Duration = Duration::from_secs(20);
/// How long a peer may send nothing before its tunnel is declared dead:
/// three missed pings.
pub(crate) const SILENCE: Duration = Duration::from_secs(60);

pub(crate) struct WsIo<S> {
    ws: WebSocketStream<S>,
    buf: Bytes,
    deadline: Pin<Box<Sleep>>,
    ping: Interval,
    ping_unflushed: bool,
}

impl<S> WsIo<S> {
    pub(crate) fn new(ws: WebSocketStream<S>) -> Self {
        let mut ping = tokio::time::interval_at(Instant::now() + PING_EVERY, PING_EVERY);
        ping.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Self {
            ws,
            buf: Bytes::new(),
            deadline: Box::pin(tokio::time::sleep(SILENCE)),
            ping,
            ping_unflushed: false,
        }
    }
}

impl<S> WsIo<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    fn heard_from_peer(&mut self) {
        self.deadline.as_mut().reset(Instant::now() + SILENCE);
    }

    /// Sends a ping when one is due. A sink that is busy is carrying traffic
    /// already, so a ping it cannot take right now is skipped.
    fn keepalive(&mut self, cx: &mut Context<'_>) {
        if self.ping.poll_tick(cx).is_ready()
            && let Poll::Ready(Ok(())) = Pin::new(&mut self.ws).poll_ready(cx)
            && Pin::new(&mut self.ws)
                .start_send(WsMessage::Ping(Bytes::new()))
                .is_ok()
        {
            self.ping_unflushed = true;
        }
        if self.ping_unflushed && Pin::new(&mut self.ws).poll_flush(cx).is_ready() {
            self.ping_unflushed = false;
        }
    }
}

impl<S> AsyncRead for WsIo<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            if !self.buf.is_empty() {
                let n = out.len().min(self.buf.len());
                let chunk = self.buf.split_to(n);
                out[..n].copy_from_slice(&chunk);
                return Poll::Ready(Ok(n));
            }
            self.keepalive(cx);
            match Pin::new(&mut self.ws).poll_next(cx) {
                Poll::Ready(Some(Ok(WsMessage::Binary(data)))) => {
                    self.heard_from_peer();
                    self.buf = data;
                }
                Poll::Ready(Some(Ok(WsMessage::Close(_)))) | Poll::Ready(None) => {
                    return Poll::Ready(Ok(0));
                }
                // Pings, pongs, text: proof of life, not tunnel bytes.
                Poll::Ready(Some(Ok(_))) => self.heard_from_peer(),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(io::Error::other(e))),
                Poll::Pending => {
                    return match self.deadline.as_mut().poll(cx) {
                        Poll::Ready(()) => Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "the other end of the tunnel sent nothing for a minute",
                        ))),
                        Poll::Pending => Poll::Pending,
                    };
                }
            }
        }
    }
}

impl<S> AsyncWrite for WsIo<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.ws).poll_ready(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(io::Error::other(e))),
            Poll::Pending => return Poll::Pending,
        }
        Pin::new(&mut self.ws)
            .start_send(WsMessage::Binary(Bytes::copy_from_slice(data)))
            .map_err(io::Error::other)?;
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.ws)
            .poll_flush(cx)
            .map_err(io::Error::other)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.ws)
            .poll_close(cx)
            .map_err(io::Error::other)
    }
}
