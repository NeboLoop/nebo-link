//! What the crate runs over: a stream of whole messages.
//!
//! A WebSocket is already one: each binary WebSocket message carries one
//! Noise message (spec section 17.3), and the caller adapts its WebSocket
//! type to [`Transport`] (the README shows the adapter for axum and for
//! tokio-tungstenite). A plain byte stream (a TCP connection, a yamux stream,
//! an in-memory pipe) becomes one with [`framed`].

use std::io;

use bytes::{Buf, BufMut, BytesMut};
use futures::{Sink, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::error::{Error, Result};

/// The largest Noise message, and so the largest transport message.
pub const MAX_MESSAGE: usize = 65535;

/// A two-way stream of whole messages, each at most [`MAX_MESSAGE`] bytes.
pub trait Transport: Stream<Item = io::Result<Vec<u8>>> + Sink<Vec<u8>, Error = io::Error> + Unpin + Send {}

impl<T> Transport for T where T: Stream<Item = io::Result<Vec<u8>>> + Sink<Vec<u8>, Error = io::Error> + Unpin + Send {}

/// Frames a byte stream into messages with [`MessageCodec`].
pub fn framed<IO: AsyncRead + AsyncWrite>(io: IO) -> Framed<IO, MessageCodec> {
    Framed::new(io, MessageCodec)
}

/// Each message is a two-byte big-endian length (1 to 65535) and then that
/// many bytes. A zero length is refused: no Noise message is empty.
#[derive(Debug, Clone, Copy, Default)]
pub struct MessageCodec;

impl Decoder for MessageCodec {
    type Item = Vec<u8>;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Vec<u8>>> {
        if src.len() < 2 {
            return Ok(None);
        }
        let len = u16::from_be_bytes([src[0], src[1]]) as usize;
        if len == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "a message of zero bytes"));
        }
        if src.len() < 2 + len {
            src.reserve(2 + len - src.len());
            return Ok(None);
        }
        src.advance(2);
        Ok(Some(src.split_to(len).to_vec()))
    }
}

impl Encoder<Vec<u8>> for MessageCodec {
    type Error = io::Error;

    fn encode(&mut self, item: Vec<u8>, dst: &mut BytesMut) -> io::Result<()> {
        if item.is_empty() || item.len() > MAX_MESSAGE {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "a message must be 1 to 65535 bytes"));
        }
        dst.reserve(2 + item.len());
        dst.put_u16(item.len() as u16);
        dst.put_slice(&item);
        Ok(())
    }
}

/// The next message of a handshake. The end of the stream is [`Error::Closed`].
pub(crate) async fn next_message<S>(stream: &mut S) -> Result<Vec<u8>>
where
    S: Stream<Item = io::Result<Vec<u8>>> + Unpin,
{
    match stream.next().await {
        Some(Ok(m)) if m.len() > MAX_MESSAGE => Err(Error::Protocol("a message longer than 65535 bytes")),
        Some(Ok(m)) => Ok(m),
        Some(Err(e)) => Err(Error::Io(e)),
        None => Err(Error::Closed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small deterministic generator, so the property tests need no
    /// dependency and every failure reproduces.
    struct XorShift(u64);

    impl XorShift {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    #[test]
    fn any_messages_split_anywhere_come_back_whole() {
        let mut rng = XorShift(0x9e37_79b9_7f4a_7c15);
        for _ in 0..300 {
            let messages: Vec<Vec<u8>> = (0..1 + rng.below(8))
                .map(|_| {
                    let len = match rng.below(4) {
                        0 => 1,
                        1 => MAX_MESSAGE,
                        _ => 1 + rng.below(2000),
                    };
                    (0..len).map(|_| rng.next() as u8).collect()
                })
                .collect();
            let mut wire = BytesMut::new();
            for m in &messages {
                MessageCodec.encode(m.clone(), &mut wire).unwrap();
            }
            let wire = wire.freeze();
            let mut buf = BytesMut::new();
            let mut out = Vec::new();
            let mut at = 0;
            while at < wire.len() {
                let step = 1 + rng.below(3000).min(wire.len() - at - 1);
                buf.extend_from_slice(&wire[at..at + step]);
                at += step;
                while let Some(m) = MessageCodec.decode(&mut buf).unwrap() {
                    out.push(m);
                }
            }
            assert_eq!(out, messages);
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn arbitrary_bytes_never_panic_and_never_yield_an_empty_message() {
        let mut rng = XorShift(0xdead_beef_cafe_f00d);
        for _ in 0..2000 {
            let len = rng.below(600);
            let mut buf: BytesMut = (0..len).map(|_| rng.next() as u8).collect::<Vec<u8>>().as_slice().into();
            while let Ok(Some(m)) = MessageCodec.decode(&mut buf) {
                assert!(!m.is_empty() && m.len() <= MAX_MESSAGE);
            }
        }
    }

    #[test]
    fn encode_refuses_empty_and_oversized_messages() {
        let mut dst = BytesMut::new();
        assert!(MessageCodec.encode(Vec::new(), &mut dst).is_err());
        assert!(MessageCodec.encode(vec![0; MAX_MESSAGE + 1], &mut dst).is_err());
        assert!(dst.is_empty());
    }
}
