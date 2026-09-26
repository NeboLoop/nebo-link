"""CPace, the CFRG's balanced PAKE, as draft-irtf-cfrg-cpace-21 specifies it:
cipher suite CPACE-RISTR255-SHA512, initiator-responder setting (sections 7,
8.1, 8.3 and A.1 of the draft).

The protocol layer only. The group (ristretto255, RFC 9496) is libsodium's,
bundled by ``rbcl``: PyNaCl does not bind libsodium's ristretto255 functions.
The hash is the standard library's SHA-512. The draft's test vectors (appendix
B.3) are in the tests. This is a port of ``crates/oal-secure/src/cpace.rs``.
"""

from __future__ import annotations

import hashlib
import os

import rbcl

#: G_Ristretto255.DSI.
_DSI = b"CPaceRistretto255"
#: G.DSI || b"_ISK".
_DSI_ISK = b"CPaceRistretto255_ISK"
#: SHA-512's input block size, H.s_in_bytes.
_S_IN_BYTES = 128


def _leb128(n: int) -> bytes:
    out = bytearray()
    while n >= 128:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def lv_cat(*parts: bytes) -> bytes:
    """lv_cat(a0, a1, …): each part after its LEB128 length."""
    return b"".join(_leb128(len(p)) + p for p in parts)


def calculate_generator(prs: bytes, ci: bytes, sid: bytes) -> bytes:
    """G.calculate_generator(H, PRS, CI, sid), encoded."""
    len_zpad = max(0, _S_IN_BYTES - 1 - len(_leb128(len(prs))) - len(prs) - len(_leb128(len(_DSI))) - len(_DSI))
    gen_str = lv_cat(_DSI, prs, b"\x00" * len_zpad, ci, sid)
    return bytes(rbcl.crypto_core_ristretto255_from_hash(hashlib.sha512(gen_str).digest()))


def scalar_mult_vfy(y: bytes, x: bytes) -> bytes | None:
    """G.scalar_mult_vfy(y, X), or ``None`` where the draft returns G.I (X
    does not decode, or y * X is the identity), since both abort."""
    if len(x) != 32:
        return None
    try:
        return bytes(rbcl.crypto_scalarmult_ristretto255(y, x))
    except RuntimeError:
        return None


class Cpace:
    """One party's run, between sending its share and receiving the other's."""

    def __init__(self, prs: bytes, ci: bytes, sid: bytes, random: bytes | None = None) -> None:
        # G.sample_scalar() as section 8.3 recommends: 32 random bytes with the
        # bits above 252 cleared, read little-endian (always below the order).
        # ``random`` is for the test vectors only.
        scalar = bytearray(random if random is not None else os.urandom(32))
        scalar[31] &= 0x0F
        self.y = bytes(scalar)
        #: This party's encoded share, Ya or Yb.
        self.share = bytes(rbcl.crypto_scalarmult_ristretto255(self.y, calculate_generator(prs, ci, sid)))

    def finish(self, sid: bytes, ad_ours: bytes, theirs: bytes, ad_theirs: bytes, *, initiator: bool) -> bytes | None:
        """ISK from the other party's share, or ``None`` on the draft's abort
        conditions. ``initiator`` fixes the order: transcript_ir(Ya, ADa, Yb, ADb)."""
        k = scalar_mult_vfy(self.y, theirs)
        if k is None:
            return None
        if initiator:
            ya, ada, yb, adb = self.share, ad_ours, theirs, ad_theirs
        else:
            ya, ada, yb, adb = theirs, ad_theirs, self.share, ad_ours
        return hashlib.sha512(lv_cat(_DSI_ISK, sid, k) + lv_cat(ya, ada) + lv_cat(yb, adb)).digest()
