//! Challenge nonces: stateless to issue, single-use to redeem.
//!
//! A nonce is `issued_at (8 bytes) | random (16) | tag (16)`, base64url, where
//! the tag is an HMAC under a key that lives only in this process. Handing
//! one out stores nothing, so `GET /oal/challenge` cannot be used to fill the
//! relay's memory. A nonce is good for [`NONCE_TTL`] and only once: the relay
//! remembers redeemed nonces until they would have expired anyway. A restart
//! forgets the key, so outstanding nonces die with it; clients simply ask
//! again.

use std::collections::HashMap;
use std::sync::Mutex;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::time::now;

pub(crate) const NONCE_TTL: i64 = 60;

pub(crate) struct Nonces {
    key: [u8; 32],
    used: Mutex<HashMap<[u8; 16], i64>>,
}

impl Nonces {
    pub(crate) fn new() -> Self {
        let mut key = [0u8; 32];
        getrandom::getrandom(&mut key).expect("the OS random source failed");
        Self {
            key,
            used: Mutex::new(HashMap::new()),
        }
    }

    fn tag(&self, body: &[u8]) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC takes any key length");
        mac.update(body);
        mac
    }

    pub(crate) fn issue(&self) -> String {
        let mut raw = [0u8; 40];
        raw[..8].copy_from_slice(&now().to_be_bytes());
        getrandom::getrandom(&mut raw[8..24]).expect("the OS random source failed");
        let tag = self.tag(&raw[..24]).finalize().into_bytes();
        raw[24..].copy_from_slice(&tag[..16]);
        B64.encode(raw)
    }

    /// True the first time a genuine, unexpired nonce is redeemed.
    pub(crate) fn redeem(&self, nonce: &str) -> bool {
        let Ok(raw) = B64.decode(nonce) else {
            return false;
        };
        let Ok(raw) = <[u8; 40]>::try_from(raw) else {
            return false;
        };
        if self
            .tag(&raw[..24])
            .verify_truncated_left(&raw[24..])
            .is_err()
        {
            return false;
        }
        let issued = i64::from_be_bytes(raw[..8].try_into().expect("8 bytes"));
        let now = now();
        if now < issued || now - issued > NONCE_TTL {
            return false;
        }
        let id: [u8; 16] = raw[8..24].try_into().expect("16 bytes");
        let mut used = self.used.lock().unwrap_or_else(|e| e.into_inner());
        if used.len() > 1024 {
            used.retain(|_, at| now - *at <= NONCE_TTL);
        }
        used.insert(id, issued).is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_nonce_is_good_once_and_only_from_this_relay() {
        let n = Nonces::new();
        let nonce = n.issue();
        assert!(n.redeem(&nonce));
        assert!(!n.redeem(&nonce));
        assert!(!Nonces::new().redeem(&n.issue()));
        assert!(!n.redeem("garbage"));
    }
}
