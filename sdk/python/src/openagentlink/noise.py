"""The Noise Protocol Framework (revision 34), for the handshakes OAL uses:
``Noise_IK_25519_ChaChaPoly_BLAKE2s`` (sessions, spec section 17.2) and
``Noise_XXpsk0_25519_ChaChaPoly_BLAKE2s`` (pairing, section 17.5).

This is the protocol layer only: X25519 and ChaCha20-Poly1305 are the
``cryptography`` package's, BLAKE2s and HMAC the standard library's. It behaves
as the Rust ``snow`` crate the host uses, and is checked against the cacophony
and snow test vectors.
"""

from __future__ import annotations

import hashlib
import hmac
import re

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey, X25519PublicKey
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

#: The largest Noise message.
MAX_MESSAGE = 65535
#: The AEAD tag on every encrypted payload.
TAG = 16
_DHLEN = 32
_HASHLEN = 32
_MAX_NONCE = 2**64 - 1

# Pre-messages (initiator's, responder's) and messages, by base pattern name.
_PATTERNS: dict[str, tuple[tuple[tuple[str, ...], tuple[str, ...]], list[tuple[str, ...]]]] = {
    "IK": (((), ("s",)), [("e", "es", "s", "ss"), ("e", "ee", "se")]),
    "XX": (((), ()), [("e",), ("e", "ee", "s", "es"), ("s", "se")]),
}


class NoiseError(Exception):
    """A handshake or transport message that fails: altered, out of order, or keyed differently."""


def public_bytes(key: X25519PrivateKey) -> bytes:
    return key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)


def _hmac(key: bytes, data: bytes) -> bytes:
    return hmac.digest(key, data, hashlib.blake2s)


def _hkdf(chaining_key: bytes, ikm: bytes, outputs: int) -> list[bytes]:
    temp = _hmac(chaining_key, ikm)
    out = [_hmac(temp, b"\x01")]
    while len(out) < outputs:
        out.append(_hmac(temp, out[-1] + bytes([len(out) + 1])))
    return out


def _nonce(n: int) -> bytes:
    return b"\x00" * 4 + n.to_bytes(8, "little")


class CipherState:
    """A key and its counter nonce."""

    def __init__(self, key: bytes | None = None) -> None:
        self._aead = ChaCha20Poly1305(key) if key is not None else None
        self.n = 0

    @property
    def has_key(self) -> bool:
        return self._aead is not None

    def encrypt(self, ad: bytes, plaintext: bytes) -> bytes:
        if self._aead is None:
            return plaintext
        if self.n == _MAX_NONCE:
            raise NoiseError("The nonce is exhausted.")
        ciphertext = self._aead.encrypt(_nonce(self.n), plaintext, ad)
        self.n += 1
        return ciphertext

    def decrypt(self, ad: bytes, ciphertext: bytes) -> bytes:
        if self._aead is None:
            return ciphertext
        if self.n == _MAX_NONCE:
            raise NoiseError("The nonce is exhausted.")
        try:
            plaintext = self._aead.decrypt(_nonce(self.n), ciphertext, ad)
        except InvalidTag:
            raise NoiseError("A message failed to decrypt.") from None
        self.n += 1
        return plaintext

    def rekey(self) -> None:
        """Noise ``REKEY``: the new key is the old key's encryption of 32 zero
        bytes at the maximum nonce. The counter carries on."""
        assert self._aead is not None
        key = self._aead.encrypt(_nonce(_MAX_NONCE), b"\x00" * 32, b"")[:32]
        self._aead = ChaCha20Poly1305(key)


class Handshake:
    """A HandshakeState for ``Noise_<IK|XX>[pskN…]_25519_ChaChaPoly_BLAKE2s``.

    ``static`` is this side's key; ``remote_static`` the peer's, when the
    pattern has this side know it in advance (IK's initiator); ``psks`` the
    pre-shared keys in the order their tokens appear. ``ephemeral`` is for
    test vectors only. Once the last message is written or read,
    ``transport`` holds the (sending, receiving) cipher states.
    """

    def __init__(
        self,
        protocol: str,
        *,
        initiator: bool,
        static: X25519PrivateKey,
        prologue: bytes,
        remote_static: bytes | None = None,
        psks: list[bytes] | None = None,
        ephemeral: X25519PrivateKey | None = None,
    ) -> None:
        match = re.fullmatch(r"Noise_([A-Z]+)((?:psk\d+)(?:\+psk\d+)*)?_25519_ChaChaPoly_BLAKE2s", protocol)
        if match is None or match.group(1) not in _PATTERNS:
            raise ValueError(f"Unsupported Noise protocol: {protocol}")
        (pre_initiator, pre_responder), base = _PATTERNS[match.group(1)]
        messages = [list(m) for m in base]
        modifiers = match.group(2).split("+") if match.group(2) else []
        self._psk_mode = bool(modifiers)
        for modifier in modifiers:
            position = int(modifier[3:])
            if position == 0:
                messages[0].insert(0, "psk")
            else:
                messages[position - 1].append("psk")
        self._messages = messages
        self._initiator = initiator
        self._s = static
        self._e = ephemeral
        self._rs = remote_static
        self._re: bytes | None = None
        self._psks = list(psks or [])
        self._index = 0
        self.transport: tuple[CipherState, CipherState] | None = None

        name = protocol.encode()
        self.h = name.ljust(_HASHLEN, b"\x00") if len(name) <= _HASHLEN else hashlib.blake2s(name).digest()
        self._ck = self.h
        self._cipher = CipherState()
        self._mix_hash(prologue)
        for tokens, ours in ((pre_initiator, initiator), (pre_responder, not initiator)):
            for token in tokens:
                assert token == "s"
                key = public_bytes(static) if ours else remote_static
                if key is None:
                    raise ValueError("This pattern needs the peer's static key.")
                self._mix_hash(key)

    @property
    def remote_static(self) -> bytes | None:
        """The peer's static public key, once known."""
        return self._rs

    def write_message(self, payload: bytes) -> bytes:
        tokens = self._next(writing=True)
        out = bytearray()
        for token in tokens:
            if token == "e":
                if self._e is None:
                    self._e = X25519PrivateKey.generate()
                e = public_bytes(self._e)
                out += e
                self._mix_hash(e)
                if self._psk_mode:
                    self._mix_key(e)
            elif token == "s":
                out += self._encrypt_and_hash(public_bytes(self._s))
            elif token == "psk":
                self._mix_key_and_hash(self._psks.pop(0))
            else:
                self._mix_key(self._dh(token))
        out += self._encrypt_and_hash(payload)
        if len(out) > MAX_MESSAGE:
            raise NoiseError("The handshake message is too long.")
        self._advance()
        return bytes(out)

    def read_message(self, message: bytes) -> bytes:
        if len(message) > MAX_MESSAGE:
            raise NoiseError("The handshake message is too long.")
        tokens = self._next(writing=False)
        rest = message
        for token in tokens:
            if token == "e":
                if len(rest) < _DHLEN:
                    raise NoiseError("The handshake message is too short.")
                self._re, rest = rest[:_DHLEN], rest[_DHLEN:]
                self._mix_hash(self._re)
                if self._psk_mode:
                    self._mix_key(self._re)
            elif token == "s":
                size = _DHLEN + (TAG if self._cipher.has_key else 0)
                if len(rest) < size:
                    raise NoiseError("The handshake message is too short.")
                self._rs = self._decrypt_and_hash(rest[:size])
                rest = rest[size:]
            elif token == "psk":
                self._mix_key_and_hash(self._psks.pop(0))
            else:
                self._mix_key(self._dh(token))
        payload = self._decrypt_and_hash(rest)
        self._advance()
        return payload

    # ---- internal ----

    def _next(self, *, writing: bool) -> list[str]:
        if self.transport is not None:
            raise NoiseError("The handshake is over.")
        if (self._index % 2 == 0) != (self._initiator == writing):
            raise NoiseError("It is the other side's turn.")
        return self._messages[self._index]

    def _advance(self) -> None:
        self._index += 1
        if self._index == len(self._messages):
            k1, k2 = _hkdf(self._ck, b"", 2)
            c1, c2 = CipherState(k1), CipherState(k2)
            self.transport = (c1, c2) if self._initiator else (c2, c1)

    def _dh(self, token: str) -> bytes:
        # The first letter is the initiator's key, the second the responder's.
        mine, theirs = (token[0], token[1]) if self._initiator else (token[1], token[0])
        private = self._e if mine == "e" else self._s
        public = self._re if theirs == "e" else self._rs
        if private is None or public is None:
            raise NoiseError("A key the handshake needs is missing.")
        try:
            return private.exchange(X25519PublicKey.from_public_bytes(public))
        except ValueError:
            raise NoiseError("The peer sent a key that can't be used.") from None

    def _mix_hash(self, data: bytes) -> None:
        self.h = hashlib.blake2s(self.h + data).digest()

    def _mix_key(self, ikm: bytes) -> None:
        self._ck, key = _hkdf(self._ck, ikm, 2)
        self._cipher = CipherState(key)

    def _mix_key_and_hash(self, ikm: bytes) -> None:
        self._ck, temp_h, key = _hkdf(self._ck, ikm, 3)
        self._mix_hash(temp_h)
        self._cipher = CipherState(key)

    def _encrypt_and_hash(self, plaintext: bytes) -> bytes:
        ciphertext = self._cipher.encrypt(self.h, plaintext)
        self._mix_hash(ciphertext)
        return ciphertext

    def _decrypt_and_hash(self, ciphertext: bytes) -> bytes:
        plaintext = self._cipher.decrypt(self.h, ciphertext)
        self._mix_hash(ciphertext)
        return plaintext
