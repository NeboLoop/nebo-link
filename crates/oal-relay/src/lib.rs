//! The self-hostable relay for Open Agent Link (OAL), <https://openagent.link>.
//!
//! A host (the process that runs a computer's agents) dials out to the relay
//! and keeps one tunnel open. A client that paired with the host connects to
//! the relay and is carried to the host through that tunnel. The relay needs
//! no accounts: every host and client is an X25519 key, the same static key
//! OAL's end-to-end encryption uses, and every connection proves it holds
//! that key before the relay routes it.
//!
//! The relay forwards WebSocket messages, text and binary, in order and
//! unchanged. It never needs to read them, so an end-to-end encrypted session
//! (`oal-secure`) passes through as ciphertext.
//!
//! - [`wire`]: the relay's wire formats (stream frames, control messages).
//! - [`auth`]: device keys and the proof of possession.
//! - [`RelayClient`]: what hosts and clients use to reach a relay: pair,
//!   connect, presence, and the host's tunnel ([`host`]).
//! - `server` (feature `server`, on by default): the relay itself.
//!
//! The protocol the relay speaks is written down in the crate's README.

pub mod auth;
mod client;
pub mod host;
mod pump;
#[cfg(feature = "server")]
mod time;
pub mod wire;
mod wsio;

#[cfg(feature = "server")]
pub mod server;

pub use auth::Keypair;
pub use client::{Error, HostPresence, Pairing, RelayClient, WsStream};
