//! Pairing through an untrusted relay (OAL 0.2 pairing, spec section 17.5).
//!
//! Five binary messages on the pairing connection
//! (`wss://<relay>/oal/pair/<nameplate>`), the client sending first:
//!
//! 1. client → host: CPace MSGa = lv_cat(Ya, b"") (34 bytes).
//! 2. host → client: CPace MSGb = lv_cat(Yb, b"") (34 bytes).
//!    CPACE-RISTR255-SHA512, initiator-responder, PRS = the eight code
//!    characters, CI = `OAL-PAIR/1`, sid empty. Both sides now hold ISK,
//!    which only a party that knew the whole code can have.
//! 3. client → host: `Noise_XXpsk0_25519_ChaChaPoly_BLAKE2s` message 1
//!    (`psk, e`), PSK = the first 32 bytes of SHA-512(lv_cat(`OAL-PAIR/1 psk`,
//!    ISK)), prologue `OAL-PAIR/1`, empty payload.
//! 4. host → client: message 2 (`e, ee, s, es`), empty payload.
//! 5. client → host: message 3 (`s, se`), empty payload.
//!
//! The connection is then an encrypted session (section 17.3 framing), and
//! the ordinary `host/pair` request and result travel inside it. Each side
//! checks that the key the other names in `host/pair` (`device.publicKey`,
//! `info.host.publicKey`) is the static key the handshake authenticated;
//! [`Pairing::finish`] makes that check before it records the peer.
//!
//! A wrong code, or a relay that changes or substitutes any message, gives
//! the two sides different PSKs, and the first Noise message fails to
//! decrypt: [`Error::PairingFailed`], and nothing is recorded.

use futures::SinkExt;
use sha2::{Digest, Sha512};
use snow::{Builder, HandshakeState};
use zeroize::{Zeroize, Zeroizing};

use crate::code::PairingCode;
use crate::cpace::{Cpace, lv_cat};
use crate::error::{Error, Result};
use crate::keys::{KeyStore, Peer, PublicKey, Side, now};
use crate::session::Session;
use crate::transport::{MAX_MESSAGE, Transport, next_message};

const NOISE_PAIR: &str = "Noise_XXpsk0_25519_ChaChaPoly_BLAKE2s";
/// CPace CI, and the Noise prologue.
const CONTEXT: &[u8] = b"OAL-PAIR/1";
const PSK_LABEL: &[u8] = b"OAL-PAIR/1 psk";

/// Runs the pairing handshake as `side` over `transport`, using the code both
/// owners share. On success the connection is encrypted and both static keys
/// are authenticated; the caller exchanges `host/pair` over the returned
/// [`Pairing`] and then calls [`Pairing::finish`].
///
/// Both sides call it with the same code: the device that showed it and the
/// device it was typed on. The caller bounds it with a timeout, and a host
/// counts [`Error::PairingFailed`] against the code (spec section 6.2).
pub async fn pair<T: Transport>(mut transport: T, code: &PairingCode, store: &KeyStore, side: Side) -> Result<Pairing<T>> {
    let prs = code.password();
    let local = store.public_key();
    let private = store.private_for(&local).ok_or(Error::Store("the current key is missing".into()))?;
    let mut buf = Zeroizing::new(vec![0u8; MAX_MESSAGE]);

    let hs = match side {
        Side::Client => {
            let cpace = Cpace::start(&prs, CONTEXT, b"");
            transport.send(cpace_message(&cpace.share())).await?;
            let yb = cpace_share(&next_message(&mut transport).await?)?;
            let isk = cpace.finish(b"", b"", &yb, b"", true).ok_or(Error::PairingFailed)?;
            let mut hs = handshake(&isk, &private, true)?;

            let n = hs.write_message(&[], &mut buf).map_err(|_| Error::PairingFailed)?;
            transport.send(buf[..n].to_vec()).await?;
            let msg = next_message(&mut transport).await?;
            hs.read_message(&msg, &mut buf).map_err(|_| Error::PairingFailed)?;
            let n = hs.write_message(&[], &mut buf).map_err(|_| Error::PairingFailed)?;
            transport.send(buf[..n].to_vec()).await?;
            hs
        }
        Side::Host => {
            let ya = cpace_share(&next_message(&mut transport).await?)?;
            let cpace = Cpace::start(&prs, CONTEXT, b"");
            let yb = cpace.share();
            let isk = cpace.finish(b"", b"", &ya, b"", false).ok_or(Error::PairingFailed)?;
            transport.send(cpace_message(&yb)).await?;
            let mut hs = handshake(&isk, &private, false)?;

            let msg = next_message(&mut transport).await?;
            hs.read_message(&msg, &mut buf).map_err(|_| Error::PairingFailed)?;
            let n = hs.write_message(&[], &mut buf).map_err(|_| Error::PairingFailed)?;
            transport.send(buf[..n].to_vec()).await?;
            let msg = next_message(&mut transport).await?;
            hs.read_message(&msg, &mut buf).map_err(|_| Error::PairingFailed)?;
            hs
        }
    };
    let peer_key = PublicKey::from_slice(hs.get_remote_static().ok_or(Error::PairingFailed)?)?;
    let session = Session::new(transport, hs, store, None, local)?;
    let peer_side = match side {
        Side::Client => Side::Host,
        Side::Host => Side::Client,
    };
    Ok(Pairing { session, peer_key, peer_side, store: store.clone() })
}

/// A finished pairing handshake: an encrypted connection to a peer whose
/// static key is authenticated but not yet recorded.
pub struct Pairing<T> {
    session: Session<T>,
    peer_key: PublicKey,
    peer_side: Side,
    store: KeyStore,
}

impl<T: Transport> Pairing<T> {
    /// The peer's static key, as the handshake authenticated it.
    pub fn peer_key(&self) -> PublicKey {
        self.peer_key
    }

    /// Sends one frame (the `host/pair` request or result).
    pub async fn send(&mut self, frame: &[u8]) -> Result<()> {
        self.session.send(frame).await
    }

    /// Receives one frame.
    pub async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        self.session.recv().await
    }

    /// Records the peer and returns the connection as an ordinary session.
    ///
    /// `claimed` is the key the peer named in `host/pair`: `device.publicKey`
    /// on a host, `info.host.publicKey` on a client. It must be the key the
    /// handshake authenticated, or the pairing fails and nothing is recorded.
    /// `id` and `name` are the peer's (from `host/pair`: the device id this
    /// host gave it and `device.name`; or `info.host.id` and
    /// `info.host.name`); `local_id` is this device's id in the pairing (the
    /// host id on a host; `device.id` on a client).
    pub fn finish(mut self, claimed: &PublicKey, id: &str, name: &str, local_id: &str) -> Result<Session<T>> {
        if claimed != &self.peer_key {
            return Err(Error::PairingFailed);
        }
        let peer = Peer {
            id: id.to_string(),
            name: name.to_string(),
            side: self.peer_side,
            public_key: self.peer_key,
            local_id: local_id.to_string(),
            pinned: self.session.local_key(),
            paired_at: now(),
        };
        self.store.add_peer(peer.clone())?;
        self.session.record(peer);
        Ok(self.session)
    }
}

/// MSGa or MSGb: lv_cat(Y, AD) with AD empty.
fn cpace_message(share: &[u8; 32]) -> Vec<u8> {
    lv_cat(&[share, b""])
}

/// Y from MSGa or MSGb; any other shape fails the pairing.
fn cpace_share(msg: &[u8]) -> Result<[u8; 32]> {
    match msg {
        [0x20, y @ .., 0x00] if y.len() == 32 => Ok(y.try_into().expect("32 bytes")),
        _ => Err(Error::PairingFailed),
    }
}

/// The Noise handshake keyed by ISK.
fn handshake(isk: &[u8; 64], private: &[u8; 32], initiator: bool) -> Result<HandshakeState> {
    let input = Zeroizing::new(lv_cat(&[PSK_LABEL, isk]));
    let mut digest = Sha512::digest(input.as_slice());
    let mut psk = Zeroizing::new([0u8; 32]);
    psk.copy_from_slice(&digest[..32]);
    digest.as_mut_slice().zeroize();
    let setup = |_| Error::Protocol("Noise setup");
    let builder = Builder::new(NOISE_PAIR.parse().expect("a valid Noise pattern"))
        .local_private_key(private)
        .and_then(|b| b.prologue(CONTEXT))
        .and_then(|b| b.psk(0, &psk))
        .map_err(setup)?;
    if initiator { builder.build_initiator() } else { builder.build_responder() }.map_err(setup)
}
