//! CPace, the CFRG's balanced PAKE, exactly as draft-irtf-cfrg-cpace-21
//! specifies it: cipher suite CPACE-RISTR255-SHA512, initiator-responder
//! setting (sections 7, 8.1, 8.3 and A.1 of the draft).
//!
//! This is the protocol layer only. The group (ristretto255, RFC 9496) is
//! curve25519-dalek's and the hash is the `sha2` crate's SHA-512; nothing
//! here is a primitive. The draft's test vectors (appendix B.3) are the
//! tests below.

use curve25519_dalek::ristretto::{CompressedRistretto, RistrettoPoint};
use curve25519_dalek::scalar::Scalar;
use rand_core::{OsRng, RngCore};
use sha2::{Digest, Sha512};
use zeroize::{Zeroize, Zeroizing};

/// G_Ristretto255.DSI.
const DSI: &[u8] = b"CPaceRistretto255";
/// G.DSI || b"_ISK".
const DSI_ISK: &[u8] = b"CPaceRistretto255_ISK";
/// SHA-512's input block size, H.s_in_bytes.
const S_IN_BYTES: usize = 128;

/// One party's CPace run, between sending its share and receiving the other.
pub(crate) struct Cpace {
    y: Scalar,
    /// This party's encoded share, Ya or Yb.
    share: [u8; 32],
}

impl Drop for Cpace {
    fn drop(&mut self) {
        self.y.zeroize();
    }
}

impl Cpace {
    /// Starts a run: samples y, computes Y = y * g. Returns the run and Y.
    pub(crate) fn start(prs: &[u8], ci: &[u8], sid: &[u8]) -> Self {
        let mut bytes = Zeroizing::new([0u8; 32]);
        OsRng.fill_bytes(bytes.as_mut());
        Self::with_scalar(prs, ci, sid, &bytes)
    }

    /// G.sample_scalar() as section 8.3 recommends: 32 random bytes with the
    /// bits above 252 cleared, read little-endian (always below the order).
    fn with_scalar(prs: &[u8], ci: &[u8], sid: &[u8], random: &[u8; 32]) -> Self {
        let mut bytes = Zeroizing::new(*random);
        bytes[31] &= 0x0f;
        let y = Scalar::from_bytes_mod_order(*bytes);
        let share = (calculate_generator(prs, ci, sid) * y).compress().to_bytes();
        Cpace { y, share }
    }

    /// This party's encoded share.
    pub(crate) fn share(&self) -> [u8; 32] {
        self.share
    }

    /// Finishes the run with the other party's share and returns ISK, or
    /// `None` if the share does not decode or the result is the identity
    /// (the draft's abort conditions). `initiator` fixes the transcript
    /// order: transcript_ir(Ya, ADa, Yb, ADb).
    pub(crate) fn finish(self, sid: &[u8], ad_ours: &[u8], theirs: &[u8; 32], ad_theirs: &[u8], initiator: bool) -> Option<Zeroizing<[u8; 64]>> {
        let k = scalar_mult_vfy(&self.y, theirs)?;
        let (ya, ada, yb, adb) = if initiator {
            (&self.share[..], ad_ours, &theirs[..], ad_theirs)
        } else {
            (&theirs[..], ad_theirs, &self.share[..], ad_ours)
        };
        let mut input = Zeroizing::new(lv_cat(&[DSI_ISK, sid, &k[..]]));
        input.extend_from_slice(&lv_cat(&[ya, ada]));
        input.extend_from_slice(&lv_cat(&[yb, adb]));
        let mut isk = Zeroizing::new([0u8; 64]);
        isk.copy_from_slice(&Sha512::digest(input.as_slice()));
        Some(isk)
    }
}

/// G.calculate_generator(H, PRS, CI, sid).
fn calculate_generator(prs: &[u8], ci: &[u8], sid: &[u8]) -> RistrettoPoint {
    let len_zpad = S_IN_BYTES.saturating_sub(1 + prepend_len_size(prs.len()) + prs.len() + prepend_len_size(DSI.len()) + DSI.len());
    let zpad = vec![0u8; len_zpad];
    let gen_str = Zeroizing::new(lv_cat(&[DSI, prs, &zpad, ci, sid]));
    let mut hash = Zeroizing::new([0u8; 64]);
    hash.copy_from_slice(&Sha512::digest(gen_str.as_slice()));
    RistrettoPoint::from_uniform_bytes(&hash)
}

/// G.scalar_mult_vfy(y, X), returning `None` where the draft returns G.I
/// (X does not decode, or y * X is the identity), since both abort.
fn scalar_mult_vfy(y: &Scalar, x: &[u8; 32]) -> Option<Zeroizing<[u8; 32]>> {
    let point = CompressedRistretto(*x).decompress()?;
    let k = Zeroizing::new((point * y).compress().to_bytes());
    if *k == [0u8; 32] { None } else { Some(k) }
}

/// The bytes LEB128 takes to encode `len`.
fn prepend_len_size(mut len: usize) -> usize {
    let mut n = 1;
    while len >= 128 {
        len >>= 7;
        n += 1;
    }
    n
}

/// prepend_len: the LEB128 length, then the data.
fn prepend_len(out: &mut Vec<u8>, data: &[u8]) {
    let mut len = data.len();
    loop {
        if len < 128 {
            out.push(len as u8);
            break;
        }
        out.push((len as u8 & 0x7f) | 0x80);
        len >>= 7;
    }
    out.extend_from_slice(data);
}

/// lv_cat(a0, a1, …).
pub(crate) fn lv_cat(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in parts {
        prepend_len(&mut out, p);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        let s: String = s.split_whitespace().collect();
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    fn arr(s: &str) -> [u8; 32] {
        hex(s).try_into().unwrap()
    }

    #[test]
    fn prepend_len_and_lv_cat_match_appendix_a1() {
        assert_eq!(lv_cat(&[b""]), hex("00"));
        assert_eq!(lv_cat(&[b"1234"]), hex("0431323334"));
        let r127: Vec<u8> = (0..127).collect();
        assert_eq!(lv_cat(&[&r127])[..2], [0x7f, 0x00]);
        let r128: Vec<u8> = (0..128).collect();
        let enc = lv_cat(&[&r128]);
        assert_eq!(enc.len(), 130);
        assert_eq!(enc[..3], [0x80, 0x01, 0x00]);
        assert_eq!(lv_cat(&[b"1234", b"5", b"", b"678"]), hex("043132333401350003363738"));
        assert_eq!(prepend_len_size(127), 1);
        assert_eq!(prepend_len_size(128), 2);
    }

    // Appendix B.3: CPace with ristretto255 and SHA-512.
    const PRS: &[u8] = b"Password";
    const SID: &str = "7e4b4791d6a8ef019b936c79fb7f2c57";
    fn ci() -> Vec<u8> {
        hex("0b415f696e69746961746f720b425f726573706f6e646572")
    }

    #[test]
    fn generator_matches_b31() {
        let g = calculate_generator(PRS, &ci(), &hex(SID));
        assert_eq!(g.compress().to_bytes(), arr("222b6b195fe84b1652badb6f6a3ae3d24341e7306967f0b8115b40d5698c7e56"));
    }

    #[test]
    fn shares_secret_point_and_isk_match_b32_to_b35() {
        let ya = arr("da3d23700a9e5699258aef94dc060dfda5ebb61f02a5ea77fad53f4ff0976d08");
        let yb = arr("d2316b454718c35362d83d69df6320f38578ed5984651435e2949762d900b80d");
        let sid = hex(SID);
        let a = Cpace::with_scalar(PRS, &ci(), &sid, &ya);
        let b = Cpace::with_scalar(PRS, &ci(), &sid, &yb);
        assert_eq!(a.share(), arr("d6bac480f2c386c394efc7c47adb9925dcd2630b64f240c50f8d0eec482b9157"));
        assert_eq!(b.share(), arr("3ea7e0b19560d7c0b0f5734f63b955286dfa8232b5ebe63324e2d9e7433f7258"));
        let k = arr("80b69a8a76457ab6a4d7f887a4bf6b55a2f80ac19c333f917a05fc9887c8b40f");
        assert_eq!(*scalar_mult_vfy(&a.y, &b.share()).unwrap(), k);
        assert_eq!(*scalar_mult_vfy(&b.y, &a.share()).unwrap(), k);
        let isk = hex(
            "b69effbf61b51d56401c0f65601abe428de8206feaaf0e32198896dcae7b35cd\
             2b38950a39dfd5d4a79164614c2984f7daa460b588c1e80c3fa2068af7900447",
        );
        let (a_share, b_share) = (a.share(), b.share());
        assert_eq!(a.finish(&sid, b"ADa", &b_share, b"ADb", true).unwrap().to_vec(), isk);
        assert_eq!(b.finish(&sid, b"ADb", &a_share, b"ADa", false).unwrap().to_vec(), isk);
    }

    #[test]
    fn scalar_mult_vfy_matches_b310_and_refuses_b311() {
        let s = Scalar::from_bytes_mod_order(arr("7cd0e075fa7955ba52c02759a6c90dbbfc10e6d40aea8d283e407d88cf538a05"));
        let x = arr("2c3c6b8c4f3800e7aef6864025b4ed79bd599117e427c41bd47d93d654b4a51c");
        assert_eq!(*scalar_mult_vfy(&s, &x).unwrap(), arr("7c13645fe790a468f62c39beb7388e541d8405d1ade69d1778c5fe3e7f6b600e"));
        assert!(scalar_mult_vfy(&s, &arr("2b3c6b8c4f3800e7aef6864025b4ed79bd599117e427c41bd47d93d654b4a51c")).is_none());
        assert!(scalar_mult_vfy(&s, &[0u8; 32]).is_none());
    }

    #[test]
    fn a_different_password_gives_a_different_key() {
        let a = Cpace::start(b"ABCD1234", b"ci", b"");
        let b = Cpace::start(b"ABCD1235", b"ci", b"");
        let (sa, sb) = (a.share(), b.share());
        let ka = a.finish(b"", b"", &sb, b"", true).unwrap();
        let kb = b.finish(b"", b"", &sa, b"", false).unwrap();
        assert_ne!(*ka, *kb);
    }
}
