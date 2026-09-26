//! End-to-end encryption for Open Agent Link (OAL).
//!
//! A client and a host pair once with a short code, and from then on every
//! connection between them is a Noise session keyed by the static keys they
//! pinned at pairing. A relay in the middle forwards ciphertext: it can
//! neither read the traffic nor insert itself, at pairing or later.
//!
//! - [`PairingCode`]: the code the owner types, `XXXX-XXXX`. The first half
//!   is a nameplate the relay routes by; the second half never leaves the two
//!   devices.
//! - [`pair`]: turns the code into an encrypted connection with both static
//!   keys authenticated; [`Pairing::finish`] records the peer in each side's
//!   [`KeyStore`] after `host/pair` runs inside it.
//! - [`connect`] (client) and [`accept`] (host): open a [`Session`], which
//!   carries whole OAL frames.
//! - [`KeyStore`]: this device's X25519 key pair and its paired peers, with
//!   [`KeyStore::revoke`], [`KeyStore::rotate`] and the rest of rotation.
//! - [`Transport`]: what it all runs over, a stream of whole messages (a
//!   WebSocket, or any byte stream through [`framed`]).
//!
//! # Why these primitives
//!
//! **Sessions: `Noise_IK_25519_ChaChaPoly_BLAKE2s` (the `snow` crate).** OAL
//! section 17 fixes this pattern. After pairing the client already knows the
//! host's static key, which is exactly what IK assumes: one round trip, the
//! client's identity encrypted to the host (the relay never sees which device
//! is connecting from the handshake), mutual authentication by static keys,
//! and forward secrecy for everything after the first message. XX would add a
//! round trip to exchange keys both sides already have; KK would need the
//! client to name itself in the clear so the host could pick the key.
//!
//! **Pairing: CPace (draft-irtf-cfrg-cpace-21, CPACE-RISTR255-SHA512)
//! feeding the PSK of `Noise_XXpsk0_25519_ChaChaPoly_BLAKE2s`.** The code
//! has 20 secret bits, so it must never be the only thing protecting a key
//! exchange that an attacker can test offline. The candidates:
//!
//! - *Noise with the code as the PSK.* Rejected. In `XXpsk0` the first
//!   message is keyed by the PSK and public values alone, so anyone who sees
//!   it can test every code offline against its AEAD tag. In the later-PSK
//!   variants a relay that plays the other side knows every Diffie-Hellman
//!   result except the PSK, and does the same. Either way the code falls in a
//!   fraction of a second. Noise PSKs must be high-entropy.
//! - *A short fingerprint the owner compares.* Rejected: hosts are often
//!   headless servers with no screen to compare on.
//! - *A PAKE.* A PAKE gives an attacker exactly one online guess per run, and
//!   a wrong guess makes the pairing fail visibly. CPace is the PAKE the CFRG
//!   selected as its recommended balanced PAKE; SPAKE2 (RFC 9382) needs fixed
//!   group elements with no known discrete logarithm and its common
//!   implementations predate the RFC's encoding. CPace it is.
//!
//! CPace is implemented in `cpace.rs` (about 100 lines) from the draft's
//! pseudocode, on curve25519-dalek's ristretto255 and the `sha2` crate's
//! SHA-512, and checked against every ristretto255 test vector in the
//! draft's appendix B.3. The existing Rust CPace crates were not used: one
//! (`pake-cpace`) implements an early draft with a different, non-standard
//! encoding, and the others are young single-author crates. A standard
//! encoding matters because clients in other languages must interoperate.
//! This module is the part of the crate a security review should read first.
//!
//! The CPace output ISK is then, through SHA-512 with a label, the PSK of an
//! `XXpsk0` handshake in which each side proves its static key. That
//! composes CPace with standard Noise, as the draft recommends (section 10.5:
//! use ISK in the higher-level protocol and confirm it there) instead of
//! inventing a key-confirmation or key-transport step: the static keys are
//! carried, authenticated and confirmed by Noise, and the keys named in
//! `host/pair` must equal them.
//!
//! **Keys: X25519 (`x25519-dalek`),** which OAL section 17.1 fixes and Noise
//! uses. Private keys are held in types that zeroize on drop.
//!
//! No cryptographic primitive is implemented here. The crate composes snow,
//! curve25519-dalek, x25519-dalek and sha2; `rand_core`'s `OsRng` is the only
//! randomness.
//!
//! # What the relay still sees
//!
//! Which host each connection goes to, the client's IP address, when
//! connections open and close, and the size and timing of every message
//! (each Noise message is its frame part plus 17 bytes). At pairing it sees
//! the nameplate and that a pairing happened. It never sees the secret half
//! of a code, a static key in the clear, agent ids, session ids, prompts,
//! replies, tool calls, permission requests or file names.
//!
//! The README has the threat model and how nebo-link, client SDKs and the
//! relay use this crate.

mod code;
mod cpace;
mod error;
mod keys;
mod pair;
mod session;
mod transport;

pub use code::PairingCode;
pub use error::{Error, Result};
pub use keys::{KeyStore, Peer, PublicKey, Side};
pub use pair::{Pairing, pair};
pub use session::{DEFAULT_MAX_FRAME, Incoming, Session, SessionReader, SessionWriter, accept, connect};
pub use transport::{MAX_MESSAGE, MessageCodec, Transport, framed};
