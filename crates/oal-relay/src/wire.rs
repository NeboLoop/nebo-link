//! The relay's wire formats.
//!
//! **The tunnel.** A host holds one WebSocket to the relay. Its binary
//! messages carry a yamux session (the relay is the yamux client, the host
//! the server), the same multiplexer NeboAI's hub tunnel uses. The relay
//! opens one yamux stream for the host's control channel as soon as the
//! tunnel is up, then one stream per client connection.
//!
//! **A stream** is a sequence of frames:
//!
//! ```text
//! kind: u8 | length: u32 (big-endian) | payload: length bytes
//! ```
//!
//! | kind | frame | payload |
//! |---|---|---|
//! | 0 | open | JSON [`Open`]; always the first frame, sent by the relay |
//! | 1 | text | one WebSocket text message, UTF-8, unchanged |
//! | 2 | binary | one WebSocket binary message, unchanged |
//! | 3 | close | close code as u16 big-endian (0: none), then the UTF-8 reason |
//!
//! On a client stream, text and binary frames are the client's WebSocket
//! messages, one to one, in order, never inspected. A close frame closes the
//! other side with the same code. On the control stream, text frames carry
//! the JSON messages [`RelayToHost`] and [`HostToRelay`].

use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{Decoder, Encoder};

/// The largest WebSocket message the relay forwards by default. OAL hosts
/// must accept frames of at least 4 MiB (spec section 4.1).
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 16 << 20;

const KIND_OPEN: u8 = 0;
const KIND_TEXT: u8 = 1;
const KIND_BINARY: u8 = 2;
const KIND_CLOSE: u8 = 3;
const HEADER: usize = 5;

/// One WebSocket message, as it crosses the relay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    Text(String),
    Binary(Bytes),
    /// A close, with the WebSocket close code (`None`: no code) and reason.
    Close {
        code: Option<u16>,
        reason: String,
    },
}

/// A frame on a tunnel stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Open(Open),
    Message(Message),
}

/// What a stream is for: the first frame on every stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum Open {
    /// The host's control channel.
    Control,
    /// A client's connection. `clientKey` is the key the relay verified the
    /// connection holds: the static key end-to-end encryption expects.
    /// `nameplate` is set on a pairing connection (`/oal/pair/<nameplate>`):
    /// the client is not paired yet, and its first messages are the pairing
    /// handshake (spec section 17.5) or, in 0.1, `host/pair`.
    Client {
        client_key: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        nameplate: Option<String>,
    },
}

/// A client paired with a host, as the relay records it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairedClient {
    /// The client's X25519 static public key, base64url without padding.
    pub client_key: String,
    /// RFC 3339.
    pub paired_at: String,
}

/// An agent's presence, as a host chooses to publish it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentPresence {
    pub id: String,
    pub online: bool,
}

/// Control messages from the relay to a host.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum RelayToHost {
    /// The tunnel is up. `pairings` is every client the relay lets through
    /// to this host. A host reconciles it with its own list: [`HostToRelay::Paired`]
    /// for a device the relay is missing, [`HostToRelay::Unpair`] for one it
    /// no longer accepts.
    Registered {
        host_id: String,
        pairings: Vec<PairedClient>,
    },
    /// The operator removed a pairing; the client's connections are closing
    /// with 4003.
    Unpaired { client_key: String },
    /// The answer to [`HostToRelay::NameplateRequest`].
    Nameplate {
        request: u64,
        nameplate: String,
        expires_at: String,
    },
    /// The answer to [`HostToRelay::Paired`] and [`HostToRelay::Unpair`].
    Done { request: u64 },
    /// A request the relay refused.
    Error {
        request: u64,
        code: String,
        message: String,
    },
}

/// Control messages from a host to the relay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum HostToRelay {
    /// Register a nameplate for this host: `nameplate` when the host (or a
    /// client that showed the code) chose it, or a fresh one from the relay.
    /// The host makes the secret half of the code itself; it never comes
    /// here. The nameplate routes `/oal/pair/<nameplate>` to this host until
    /// it expires.
    NameplateRequest {
        request: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        nameplate: Option<String>,
    },
    /// The host paired this client: let it through to the host from now on.
    /// A host sends it once `host/pair` succeeds, and waits for
    /// [`RelayToHost::Done`] before answering `host/pair`.
    Paired { request: u64, client_key: String },
    /// Remove a client's pairing with this host and close its connections
    /// with 4003 (OAL `host/unpair`).
    Unpair { request: u64, client_key: String },
    /// The host's agents and whether each is online. Replaces the last list.
    /// Paired clients can read it (`GET /oal/presence`); so can the operator.
    Presence { agents: Vec<AgentPresence> },
}

/// The JSON body of every refusal the relay answers over HTTP, and of the
/// 503 for an offline host (spec section 4.4).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

/// The Crockford base32 alphabet OAL codes are drawn from (spec section 6.2).
pub const CODE_ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Characters in a nameplate: the first half of a pairing code.
pub const NAMEPLATE_LEN: usize = 4;

/// A nameplate as typed, reduced to its 4 canonical characters, or `None` if
/// it cannot be one. Case-insensitive; hyphens and spaces are ignored;
/// Crockford's look-alikes are read as what they look like (O as 0, I and L
/// as 1). A whole code (8 characters) is refused: its second half is the
/// secret, which never goes to the relay.
pub fn normalize_nameplate(nameplate: &str) -> Option<String> {
    let mut out = String::with_capacity(NAMEPLATE_LEN);
    for c in nameplate.chars() {
        let c = match c.to_ascii_uppercase() {
            '-' | ' ' => continue,
            'O' => '0',
            'I' | 'L' => '1',
            c => c,
        };
        if !c.is_ascii() || !CODE_ALPHABET.contains(&(c as u8)) {
            return None;
        }
        out.push(c);
    }
    (out.len() == NAMEPLATE_LEN).then_some(out)
}

/// Encodes and decodes stream frames, refusing any over `max` bytes.
#[derive(Clone, Debug)]
pub struct FrameCodec {
    max: usize,
}

impl FrameCodec {
    pub fn new(max: usize) -> Self {
        Self { max }
    }
}

fn invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Frame>> {
        if src.len() < HEADER {
            return Ok(None);
        }
        let kind = src[0];
        let len = u32::from_be_bytes([src[1], src[2], src[3], src[4]]) as usize;
        if len > self.max {
            return Err(invalid("frame larger than the relay's limit"));
        }
        if src.len() < HEADER + len {
            src.reserve(HEADER + len - src.len());
            return Ok(None);
        }
        src.advance(HEADER);
        let payload = src.split_to(len).freeze();
        let frame = match kind {
            KIND_OPEN => Frame::Open(
                serde_json::from_slice(&payload).map_err(|_| invalid("bad open frame"))?,
            ),
            KIND_TEXT => Frame::Message(Message::Text(
                String::from_utf8(payload.to_vec())
                    .map_err(|_| invalid("text frame is not UTF-8"))?,
            )),
            KIND_BINARY => Frame::Message(Message::Binary(payload)),
            KIND_CLOSE => {
                if payload.len() < 2 {
                    return Err(invalid("short close frame"));
                }
                let code = u16::from_be_bytes([payload[0], payload[1]]);
                let reason = String::from_utf8_lossy(&payload[2..]).into_owned();
                Frame::Message(Message::Close {
                    code: (code != 0).then_some(code),
                    reason,
                })
            }
            _ => return Err(invalid("unknown frame kind")),
        };
        Ok(Some(frame))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = io::Error;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> io::Result<()> {
        let (kind, payload): (u8, Bytes) = match frame {
            Frame::Open(open) => (
                KIND_OPEN,
                serde_json::to_vec(&open).map_err(io::Error::other)?.into(),
            ),
            Frame::Message(Message::Text(text)) => (KIND_TEXT, text.into()),
            Frame::Message(Message::Binary(data)) => (KIND_BINARY, data),
            Frame::Message(Message::Close { code, reason }) => {
                let mut p = BytesMut::with_capacity(2 + reason.len());
                p.put_u16(code.unwrap_or(0));
                p.put_slice(reason.as_bytes());
                (KIND_CLOSE, p.freeze())
            }
        };
        if payload.len() > self.max {
            return Err(invalid("frame larger than the relay's limit"));
        }
        dst.reserve(HEADER + payload.len());
        dst.put_u8(kind);
        dst.put_u32(payload.len() as u32);
        dst.put_slice(&payload);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(frame: Frame) {
        let mut codec = FrameCodec::new(1024);
        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();
        // Byte at a time: a partial frame is never yielded.
        let mut partial = BytesMut::new();
        let bytes = buf.to_vec();
        for (i, b) in bytes.iter().enumerate() {
            partial.put_u8(*b);
            let got = codec.decode(&mut partial).unwrap();
            if i + 1 < bytes.len() {
                assert!(got.is_none());
            } else {
                assert_eq!(got, Some(frame.clone()));
            }
        }
    }

    #[test]
    fn frames_round_trip() {
        round_trip(Frame::Open(Open::Control));
        round_trip(Frame::Open(Open::Client {
            client_key: "k".into(),
            nameplate: Some("K7QM".into()),
        }));
        round_trip(Frame::Open(Open::Client {
            client_key: "k".into(),
            nameplate: None,
        }));
        round_trip(Frame::Message(Message::Text(
            "{\"jsonrpc\":\"2.0\"}".into(),
        )));
        round_trip(Frame::Message(Message::Binary(Bytes::from_static(&[
            0, 1, 255,
        ]))));
        round_trip(Frame::Message(Message::Close {
            code: Some(4003),
            reason: "unpaired".into(),
        }));
        round_trip(Frame::Message(Message::Close {
            code: None,
            reason: String::new(),
        }));
    }

    #[test]
    fn an_oversized_frame_is_refused() {
        let mut codec = FrameCodec::new(4);
        let mut buf = BytesMut::new();
        assert!(
            codec
                .encode(Frame::Message(Message::Text("12345".into())), &mut buf)
                .is_err()
        );
        let mut raw = BytesMut::from(&[KIND_BINARY, 0, 0, 0, 5][..]);
        assert!(codec.decode(&mut raw).is_err());
    }

    #[test]
    fn control_messages_are_camel_case_json() {
        let m = RelayToHost::Nameplate {
            request: 1,
            nameplate: "K7QM".into(),
            expires_at: "t".into(),
        };
        assert_eq!(
            serde_json::to_string(&m).unwrap(),
            r#"{"type":"nameplate","request":1,"nameplate":"K7QM","expiresAt":"t"}"#
        );
        let u = HostToRelay::Unpair {
            request: 2,
            client_key: "k".into(),
        };
        assert_eq!(
            serde_json::to_string(&u).unwrap(),
            r#"{"type":"unpair","request":2,"clientKey":"k"}"#
        );
        let n = HostToRelay::NameplateRequest {
            request: 3,
            nameplate: None,
        };
        assert_eq!(
            serde_json::to_string(&n).unwrap(),
            r#"{"type":"nameplate_request","request":3}"#
        );
        let o = Open::Client {
            client_key: "k".into(),
            nameplate: Some("K7QM".into()),
        };
        assert_eq!(
            serde_json::to_string(&o).unwrap(),
            r#"{"type":"client","clientKey":"k","nameplate":"K7QM"}"#
        );
    }

    #[test]
    fn nameplates_normalize() {
        assert_eq!(normalize_nameplate("k7qm").as_deref(), Some("K7QM"));
        assert_eq!(normalize_nameplate(" 0o-1l ").as_deref(), Some("0011"));
        assert_eq!(normalize_nameplate("K7Q"), None);
        assert_eq!(normalize_nameplate("K7QU"), None); // U is not in the alphabet
        // A whole code is refused: its second half is the secret.
        assert_eq!(normalize_nameplate("K7QM-3XRD"), None);
    }
}
