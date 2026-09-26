//! What can go wrong, in terms a caller can act on.

use std::io;

/// Every failure in pairing, handshakes and sessions.
///
/// A session that returns any error other than [`Error::FrameTooLarge`] on a
/// send is over: every later call returns [`Error::SessionFailed`]. The caller
/// closes the connection, using [`Error::close_code`] on a WebSocket.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The transport under the session failed.
    #[error("the connection failed: {0}")]
    Io(#[from] io::Error),
    /// The transport ended in the middle of a handshake or a frame.
    #[error("the connection closed before the exchange finished")]
    Closed,
    /// The text entered is not a pairing code.
    #[error("that code isn't valid: {0}")]
    InvalidCode(&'static str),
    /// The two sides did not agree on the code. Either the code was wrong, or
    /// something between them (the relay) tried to take part. The two cases
    /// cannot be told apart, by design.
    #[error("pairing failed: the code was wrong, or something between the two sides tried to take part")]
    PairingFailed,
    /// A handshake came from, or was aimed at, a key that is not paired with
    /// this device (never paired, or revoked).
    #[error("this device isn't paired with the other side; pair again")]
    UnknownPeer,
    /// The peer was unpaired while the session was open.
    #[error("the other side was unpaired")]
    Revoked,
    /// The only key of ours the peer knows has been retired, so this device
    /// can no longer prove who it is to that peer.
    #[error("the key the other side knows for this device was retired; pair again")]
    KeyRetired,
    /// A message failed authentication: it was altered, replayed, reordered or
    /// dropped on the way, or the other side is not who we paired with.
    #[error("a message failed authentication; the connection is not safe to use")]
    Authentication,
    /// A frame was bigger than this side accepts.
    #[error("a frame was larger than {limit} bytes")]
    FrameTooLarge {
        /// The limit that was exceeded.
        limit: usize,
    },
    /// A message had the wrong shape for where it arrived.
    #[error("unexpected message: {0}")]
    Protocol(&'static str),
    /// The key store could not be read or written.
    #[error("the key store failed: {0}")]
    Store(String),
    /// An earlier error already ended this session.
    #[error("the session already failed")]
    SessionFailed,
}

impl Error {
    /// The WebSocket close code OAL uses for this error (spec sections 4.3
    /// and 17): 4001 not authenticated, 4003 unpaired, 1009 frame too large,
    /// 1002 protocol error, 1011 anything else.
    pub fn close_code(&self) -> u16 {
        match self {
            Error::UnknownPeer | Error::PairingFailed | Error::Authentication | Error::KeyRetired => 4001,
            Error::Revoked => 4003,
            Error::FrameTooLarge { .. } => 1009,
            Error::Protocol(_) | Error::InvalidCode(_) => 1002,
            Error::Io(_) | Error::Closed | Error::Store(_) | Error::SessionFailed => 1011,
        }
    }
}

/// Results in this crate.
pub type Result<T> = std::result::Result<T, Error>;
