//! Device keys and the relay's proof of possession.
//!
//! Every host and client is identified by an X25519 static public key: the
//! key OAL's end-to-end encryption uses (Noise `IK`, spec section 17), so a
//! device has one key, not one for the relay and another for its peers.
//!
//! X25519 keys cannot sign, so possession is proven the way Noise itself
//! authenticates a static key: with a Diffie-Hellman against the relay's own
//! static key. The relay hands out a fresh single-use nonce
//! (`GET /oal/challenge`); the device answers with
//!
//! ```text
//! shared = X25519(device_secret, relay_public)        // = X25519(relay_secret, device_public)
//! prk    = HMAC-SHA256(key = "oal-relay-auth/1", shared)
//! proof  = HMAC-SHA256(prk, "oal-relay-auth/1\n" role "\n" device_key "\n"
//!                           relay_key "\n" nonce "\n" target "\n")
//! ```
//!
//! `role` is `host` or `client`; keys are base64url without padding; `target`
//! names what the proof is for ([`Target`]), so a proof made to pair cannot be
//! replayed to connect. Only the holder of the device's secret (or the relay)
//! can compute `shared`, and the nonce is accepted once.

use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use x25519_dalek::{PublicKey, StaticSecret};

type HmacSha256 = Hmac<Sha256>;

/// The proof's domain separator, and the prefix of every transcript.
pub const PROOF_DOMAIN: &str = "oal-relay-auth/1";

/// Which side of the relay a key speaks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Host,
    Client,
}

impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Role::Host => "host",
            Role::Client => "client",
        }
    }
}

/// What a proof is for. It is part of the transcript, so a proof made for one
/// request is useless for any other.
#[derive(Clone, Copy, Debug)]
pub enum Target<'a> {
    /// A host opening its tunnel under this host id.
    Host(&'a str),
    /// A client connecting to this host id.
    Connect(&'a str),
    /// A client opening a pairing connection through this nameplate
    /// (normalized, see [`crate::wire::normalize_nameplate`]). Only the
    /// nameplate: the secret half of a code never reaches the relay.
    Pair(&'a str),
    /// A client asking for the presence of the hosts it paired with.
    Presence,
}

impl fmt::Display for Target<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Target::Host(id) => write!(f, "host:{id}"),
            Target::Connect(id) => write!(f, "connect:{id}"),
            Target::Pair(code) => write!(f, "pair:{code}"),
            Target::Presence => f.write_str("presence"),
        }
    }
}

/// An X25519 static key pair: a host's, a client's, or the relay's own.
#[derive(Clone)]
pub struct Keypair {
    secret: StaticSecret,
    public: PublicKey,
}

impl Keypair {
    /// A new key pair from the operating system's random source.
    pub fn generate() -> Self {
        let mut bytes = [0u8; 32];
        getrandom::getrandom(&mut bytes).expect("the OS random source failed");
        Self::from_secret(bytes)
    }

    /// The key pair for a stored secret (from [`Keypair::secret_bytes`]).
    pub fn from_secret(bytes: [u8; 32]) -> Self {
        let secret = StaticSecret::from(bytes);
        let public = PublicKey::from(&secret);
        Self { secret, public }
    }

    /// The secret, for storing. Keep it private to the device.
    pub fn secret_bytes(&self) -> [u8; 32] {
        self.secret.to_bytes()
    }

    /// The public key.
    pub fn public(&self) -> [u8; 32] {
        self.public.to_bytes()
    }

    /// The public key as the relay writes it: base64url without padding.
    pub fn public_b64(&self) -> String {
        encode_key(&self.public())
    }

    /// X25519 with `peer`, or `None` for a low-order peer key (a shared
    /// secret an attacker could predict).
    fn shared(&self, peer: &[u8; 32]) -> Option<[u8; 32]> {
        let shared = self.secret.diffie_hellman(&PublicKey::from(*peer));
        shared.was_contributory().then(|| shared.to_bytes())
    }
}

impl fmt::Debug for Keypair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Keypair")
            .field("public", &self.public_b64())
            .finish_non_exhaustive()
    }
}

/// A 32-byte key as base64url without padding.
pub fn encode_key(key: &[u8; 32]) -> String {
    B64.encode(key)
}

/// A key written by [`encode_key`], or `None` if it is not one.
pub fn decode_key(s: &str) -> Option<[u8; 32]> {
    B64.decode(s).ok()?.try_into().ok()
}

/// A device's proof for `target`, answering the relay's challenge (`nonce`,
/// `relay_key`). `None` if `relay_key` is not a usable key.
pub fn prove(
    device: &Keypair,
    role: Role,
    relay_key: &str,
    nonce: &str,
    target: Target<'_>,
) -> Option<String> {
    let relay = decode_key(relay_key)?;
    let shared = device.shared(&relay)?;
    let mac = transcript(
        &shared,
        role,
        &device.public_b64(),
        relay_key,
        nonce,
        target,
    );
    Some(B64.encode(mac.finalize().into_bytes()))
}

/// The relay's check of a device's proof. `device_key` is the key the device
/// claims; the proof only verifies if it was made with that key's secret.
pub fn verify(
    relay: &Keypair,
    role: Role,
    device_key: &str,
    nonce: &str,
    target: Target<'_>,
    proof: &str,
) -> bool {
    let (Some(device), Ok(proof)) = (decode_key(device_key), B64.decode(proof)) else {
        return false;
    };
    let Some(shared) = relay.shared(&device) else {
        return false;
    };
    transcript(
        &shared,
        role,
        device_key,
        &relay.public_b64(),
        nonce,
        target,
    )
    .verify_slice(&proof)
    .is_ok()
}

fn transcript(
    shared: &[u8; 32],
    role: Role,
    device_key: &str,
    relay_key: &str,
    nonce: &str,
    target: Target<'_>,
) -> HmacSha256 {
    let mut extract =
        HmacSha256::new_from_slice(PROOF_DOMAIN.as_bytes()).expect("HMAC takes any key length");
    extract.update(shared);
    let prk = extract.finalize().into_bytes();
    let mut mac = HmacSha256::new_from_slice(&prk).expect("HMAC takes any key length");
    let target = target.to_string();
    for part in [
        PROOF_DOMAIN,
        role.as_str(),
        device_key,
        relay_key,
        nonce,
        &target,
    ] {
        mac.update(part.as_bytes());
        mac.update(b"\n");
    }
    mac
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_proof_verifies_only_for_its_key_role_nonce_and_target() {
        let relay = Keypair::generate();
        let device = Keypair::generate();
        let other = Keypair::generate();
        let rk = relay.public_b64();
        let proof = prove(&device, Role::Client, &rk, "n1", Target::Connect("studio")).unwrap();

        let dk = device.public_b64();
        assert!(verify(
            &relay,
            Role::Client,
            &dk,
            "n1",
            Target::Connect("studio"),
            &proof
        ));
        // Someone else's key, with this device's proof.
        let ok = other.public_b64();
        assert!(!verify(
            &relay,
            Role::Client,
            &ok,
            "n1",
            Target::Connect("studio"),
            &proof
        ));
        assert!(!verify(
            &relay,
            Role::Host,
            &dk,
            "n1",
            Target::Connect("studio"),
            &proof
        ));
        assert!(!verify(
            &relay,
            Role::Client,
            &dk,
            "n2",
            Target::Connect("studio"),
            &proof
        ));
        assert!(!verify(
            &relay,
            Role::Client,
            &dk,
            "n1",
            Target::Connect("other"),
            &proof
        ));
        assert!(!verify(
            &relay,
            Role::Client,
            &dk,
            "n1",
            Target::Presence,
            &proof
        ));
        // A different relay cannot verify it either.
        let relay2 = Keypair::generate();
        assert!(!verify(
            &relay2,
            Role::Client,
            &dk,
            "n1",
            Target::Connect("studio"),
            &proof
        ));
    }

    #[test]
    fn a_low_order_key_is_refused() {
        let relay = Keypair::generate();
        let zero = encode_key(&[0u8; 32]);
        assert!(!verify(
            &relay,
            Role::Client,
            &zero,
            "n",
            Target::Presence,
            "AAAA"
        ));
        let device = Keypair::generate();
        assert!(prove(&device, Role::Client, &zero, "n", Target::Presence).is_none());
    }

    #[test]
    fn keys_round_trip() {
        let k = Keypair::generate();
        assert_eq!(Keypair::from_secret(k.secret_bytes()).public(), k.public());
        assert_eq!(decode_key(&k.public_b64()), Some(k.public()));
        assert_eq!(decode_key("not a key"), None);
        assert_eq!(k.public_b64().len(), 43);
    }
}
