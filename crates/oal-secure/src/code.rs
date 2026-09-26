//! Pairing codes: a nameplate the relay routes by, and a secret it never sees.

use std::fmt;

use rand_core::{OsRng, RngCore};
use zeroize::Zeroizing;

use crate::error::{Error, Result};

/// Crockford base32, the alphabet OAL codes use (spec section 6.2).
const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Characters in each half of a code.
const HALF: usize = 4;

/// A pairing code, shown as `XXXX-XXXX` like every OAL code.
///
/// The first half is the **nameplate**: the relay routes the pairing by it
/// (`wss://<relay>/oal/pair/<nameplate>`), so the relay may issue it and
/// always sees it. The second half is the **secret**: made on the device that
/// shows the code, typed on the other device, and never sent to the relay.
/// The whole code is the CPace password, so a relay that knows only the
/// nameplate has one guess in 2^20 per attempt at inserting itself, and a
/// wrong guess makes the pairing fail for the owner to see.
#[derive(Clone, PartialEq, Eq)]
pub struct PairingCode {
    nameplate: String,
    secret: Zeroizing<String>,
}

impl PairingCode {
    /// Makes a new code. `nameplate` is the half a relay issued, if it issued
    /// one; otherwise this device makes both halves (and registers the
    /// nameplate with the relay). The secret half is always made here.
    pub fn generate(nameplate: Option<&str>) -> Result<Self> {
        let nameplate = match nameplate {
            Some(n) => {
                let n = normalize(n)?;
                if n.len() != HALF {
                    return Err(Error::InvalidCode("a nameplate is 4 characters"));
                }
                n
            }
            None => random_half(),
        };
        Ok(PairingCode { nameplate, secret: Zeroizing::new(random_half()) })
    }

    /// Reads a code as the owner typed it. Case is ignored, and so are hyphens
    /// and spaces; `I` and `L` read as `1`, `O` as `0`.
    pub fn parse(text: &str) -> Result<Self> {
        let all = Zeroizing::new(normalize(text)?);
        if all.len() != 2 * HALF {
            return Err(Error::InvalidCode("a code is 8 characters"));
        }
        Ok(PairingCode {
            nameplate: all[..HALF].to_string(),
            secret: Zeroizing::new(all[HALF..].to_string()),
        })
    }

    /// The half the relay routes by. This, and only this, goes to the relay.
    pub fn nameplate(&self) -> &str {
        &self.nameplate
    }

    /// The CPace password (PRS): the eight characters, normalized, without
    /// the hyphen.
    pub(crate) fn password(&self) -> Zeroizing<Vec<u8>> {
        Zeroizing::new(format!("{}{}", self.nameplate, self.secret.as_str()).into_bytes())
    }
}

/// The code as the owner reads it: `XXXX-XXXX`.
impl fmt::Display for PairingCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.nameplate, self.secret.as_str())
    }
}

/// Never prints the secret half.
impl fmt::Debug for PairingCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PairingCode({}-****)", self.nameplate)
    }
}

fn normalize(text: &str) -> Result<String> {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        let c = match c.to_ascii_uppercase() {
            '-' | ' ' => continue,
            'I' | 'L' => '1',
            'O' => '0',
            c => c,
        };
        if !c.is_ascii() || !ALPHABET.contains(&(c as u8)) {
            return Err(Error::InvalidCode("use only the letters and digits shown"));
        }
        out.push(c);
    }
    Ok(out)
}

fn random_half() -> String {
    let mut bytes = Zeroizing::new([0u8; HALF]);
    OsRng.fill_bytes(bytes.as_mut());
    // 32 symbols: the low five bits of a uniform byte are uniform.
    bytes.iter().map(|b| ALPHABET[(b & 0x1f) as usize] as char).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_is_forgiving_about_how_it_was_typed() {
        let a = PairingCode::parse("abcd-efgh").unwrap();
        let b = PairingCode::parse(" ABCD EFGH ").unwrap();
        assert_eq!(a, b);
        assert_eq!(a.to_string(), "ABCD-EFGH");
        assert_eq!(PairingCode::parse("OIL0-0000").unwrap().to_string(), "0110-0000");
    }

    #[test]
    fn parse_refuses_the_wrong_length_or_letters() {
        assert!(PairingCode::parse("ABCD-EFG").is_err());
        assert!(PairingCode::parse("ABCD-EFGHJ").is_err());
        assert!(PairingCode::parse("ABCD-EFGU").is_err());
        assert!(PairingCode::parse("ABCD-EFGÉ").is_err());
    }

    #[test]
    fn generate_keeps_a_relay_nameplate_and_makes_a_fresh_secret() {
        let a = PairingCode::generate(Some("k7q2")).unwrap();
        let b = PairingCode::generate(Some("K7Q2")).unwrap();
        assert_eq!(a.nameplate(), "K7Q2");
        assert_eq!(b.nameplate(), "K7Q2");
        let again = PairingCode::parse(&a.to_string()).unwrap();
        assert_eq!(again, a);
        assert!(PairingCode::generate(Some("K7Q")).is_err());
        assert_eq!(PairingCode::generate(None).unwrap().nameplate().len(), 4);
    }

    #[test]
    fn debug_hides_the_secret() {
        let c = PairingCode::parse("ABCD-EFGH").unwrap();
        assert_eq!(format!("{c:?}"), "PairingCode(ABCD-****)");
    }
}
