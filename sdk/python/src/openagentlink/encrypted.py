"""End-to-end encryption (spec section 17): the relay forwards ciphertext.

- **Pairing** (section 17.5): CPace on the code, then
  ``Noise_XXpsk0_25519_ChaChaPoly_BLAKE2s`` keyed by the CPace result, which
  authenticates both static keys. ``host/pair`` then travels inside.
- **Sessions** (section 17.2): ``Noise_IK_25519_ChaChaPoly_BLAKE2s`` with the
  host's static key from pairing. The device is authenticated by its static
  key, so the client sends no ``host/hello``.
- **Framing** (section 17.3): each frame in parts of at most 65518 bytes, each
  part one Noise transport message in its own binary WebSocket message, with
  a rekey every 2^20 messages in each direction.

The Rust reference is ``crates/oal-secure`` in nebo-link.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
from typing import Any, NoReturn

from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

from .channel import Authenticated, ChannelClosed, ChannelContext, Frame, FrameChannel, SecureChannel, Socket
from .cpace import Cpace, lv_cat
from .errors import InvalidParams, OALError
from .noise import MAX_MESSAGE, TAG, CipherState, Handshake, NoiseError
from .relay import _unb64url, b64url

_SESSION = "Noise_IK_25519_ChaChaPoly_BLAKE2s"
_SESSION_PROLOGUE = b"OAL-E2E/1 "
_PAIR = "Noise_XXpsk0_25519_ChaChaPoly_BLAKE2s"
#: CPace CI, and the pairing handshake's prologue.
_PAIR_CONTEXT = b"OAL-PAIR/1"
_PSK_LABEL = b"OAL-PAIR/1 psk"
#: Crockford base32, the alphabet OAL codes use (section 6.2).
_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

_MORE = 0x01
_LAST = 0x00
#: Frame bytes in one Noise message: the message, less the tag and the part byte.
PART = MAX_MESSAGE - TAG - 1
#: Messages per direction between rekeys.
REKEY_EVERY = 1 << 20
#: The largest frame this client accepts: the minimum every host accepts (section 4.1).
DEFAULT_MAX_FRAME = 4 * 1024 * 1024


class _Encrypted:
    """OAL end-to-end encryption: pairs with CPace and Noise XXpsk0 when the
    context has a code, otherwise opens a Noise IK session."""

    async def open(self, socket: Socket, context: ChannelContext) -> FrameChannel:
        try:
            if context.code is not None:
                return await _pair(socket, context)
            return await _session(socket, context)
        except BaseException:
            with contextlib.suppress(Exception):
                await socket.close(1000, "")
            raise


#: End-to-end encryption (spec section 17), the default for ``pair`` and ``connect``.
encrypted: SecureChannel = _Encrypted()


class _EncryptedFrames:
    """Whole OAL frames over Noise transport messages (section 17.3)."""

    def __init__(
        self,
        socket: Socket,
        send: CipherState,
        receive: CipherState,
        *,
        authenticated: Authenticated | None,
        host_key: str,
    ) -> None:
        self.authenticated = authenticated
        self.host_key: str | None = host_key
        #: Messages per direction between rekeys.
        self.rekey_every = REKEY_EVERY
        #: The largest joined frame this side accepts.
        self.max_frame = DEFAULT_MAX_FRAME
        self._socket = socket
        self._send = send
        self._receive = receive
        self._sent = 0
        self._received = 0
        self._partial = bytearray()
        self._writing = asyncio.Lock()
        self._failed: ChannelClosed | None = None

    async def send(self, frame: Frame) -> None:
        data = json.dumps(frame, separators=(",", ":")).encode()
        parts = max(1, -(-len(data) // PART))
        async with self._writing:
            for i in range(parts):
                if self._failed is not None:
                    raise self._failed
                flag = _LAST if i + 1 == parts else _MORE
                message = self._send.encrypt(b"", bytes([flag]) + data[i * PART : (i + 1) * PART])
                self._sent += 1
                if self._sent % self.rekey_every == 0:
                    self._send.rekey()
                await self._socket.send(message)

    async def recv(self) -> Frame:
        while True:
            if self._failed is not None:
                raise self._failed
            data = await self._socket.recv()
            if isinstance(data, str):
                await self._fail(1002, "A text message on an encrypted connection.")
            try:
                plain = self._receive.decrypt(b"", data)
            except NoiseError:
                await self._fail(4001, "A message failed authentication.")
            self._received += 1
            if self._received % self.rekey_every == 0:
                self._receive.rekey()
            if not plain or plain[0] not in (_MORE, _LAST):
                await self._fail(1002, "A message part without a valid part byte.")
            if len(self._partial) + len(plain) - 1 > self.max_frame:
                await self._fail(1009, "A frame was too large.")
            self._partial += plain[1:]
            if plain[0] == _MORE:
                continue
            joined = bytes(self._partial)
            self._partial.clear()
            try:
                frame = json.loads(joined)
            except ValueError:
                continue  # Not JSON: the spec says to drop it.
            if isinstance(frame, dict):
                return frame

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._socket.close(code, reason)

    async def _fail(self, code: int, reason: str) -> NoReturn:
        """Ends the connection: nothing after a failed message is read."""
        self._failed = ChannelClosed(code, reason)
        with contextlib.suppress(Exception):
            await self._socket.close(code, reason)
        raise self._failed


async def _pair(socket: Socket, context: ChannelContext) -> _EncryptedFrames:
    """Section 17.5: CPace MSGa/MSGb, then Noise XXpsk0 keyed by ISK."""
    assert context.code is not None
    if context.device is None:
        raise InvalidParams("Pairing needs this device's key pair.")
    static = _private_key(context.device)
    cpace = Cpace(_password(context.code), _PAIR_CONTEXT, b"")
    await socket.send(lv_cat(cpace.share, b""))
    msg_b = await _handshake_message(socket)
    if len(msg_b) != 34 or msg_b[0] != 0x20 or msg_b[33] != 0x00:
        await _abort(socket, 4001, "Pairing failed.")
    isk = cpace.finish(b"", b"", msg_b[1:33], b"", initiator=True)
    if isk is None:
        await _abort(socket, 4001, "Pairing failed.")
    psk = hashlib.sha512(lv_cat(_PSK_LABEL, isk)).digest()[:32]
    handshake = Handshake(_PAIR, initiator=True, static=static, prologue=_PAIR_CONTEXT, psks=[psk])
    await socket.send(handshake.write_message(b""))
    try:
        handshake.read_message(await _handshake_message(socket))
    except NoiseError:
        await _abort(socket, 4001, "Pairing failed.")
    await socket.send(handshake.write_message(b""))
    assert handshake.transport is not None and handshake.remote_static is not None
    send, receive = handshake.transport
    return _EncryptedFrames(socket, send, receive, authenticated=None, host_key=b64url(handshake.remote_static))


async def _session(socket: Socket, context: ChannelContext) -> _EncryptedFrames:
    """Section 17.2: Noise IK to the host's pinned key; message 2 names the device."""
    if context.host is None or context.device is None:
        raise InvalidParams("Pair first: an encrypted connection needs the host's key and this device's.")
    static = _private_key(context.device)
    try:
        host_key = _unb64url(context.host["publicKey"])
    except (KeyError, ValueError):
        raise InvalidParams("The host's key in these credentials isn't valid. Pair again.") from None
    if len(host_key) != 32:
        raise InvalidParams("The host's key in these credentials isn't valid. Pair again.")
    prologue = _SESSION_PROLOGUE + context.host["id"].encode()
    handshake = Handshake(_SESSION, initiator=True, static=static, prologue=prologue, remote_static=host_key)
    hello = json.dumps({"protocol": context.protocol, "client": context.client}, separators=(",", ":"))
    await socket.send(handshake.write_message(hello.encode()))
    try:
        reply = handshake.read_message(await _handshake_message(socket))
    except NoiseError:
        await _abort(socket, 4001, "The host couldn't be authenticated.")
    try:
        payload: Any = json.loads(reply)
    except ValueError:
        payload = None
    if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
        # The host closes after this (4002 for version_mismatch).
        await socket.close(1000, "")
        raise OALError.from_rpc(payload["error"])
    device = payload.get("device") if isinstance(payload, dict) else None
    valid = isinstance(payload, dict) and isinstance(payload.get("protocol"), str)
    if not valid or not isinstance(device, dict) or not isinstance(device.get("id"), str):
        await _abort(socket, 1002, "The host's handshake reply isn't valid.")
    assert handshake.transport is not None
    send, receive = handshake.transport
    authenticated = Authenticated(protocol=payload["protocol"], device=device)
    return _EncryptedFrames(socket, send, receive, authenticated=authenticated, host_key=context.host["publicKey"])


async def _handshake_message(socket: Socket) -> bytes:
    data = await socket.recv()
    if isinstance(data, str):
        await _abort(socket, 1002, "A text message on an encrypted connection.")
    return data


async def _abort(socket: Socket, code: int, reason: str) -> NoReturn:
    with contextlib.suppress(Exception):
        await socket.close(code, reason)
    raise ChannelClosed(code, reason)


def _private_key(device: dict[str, str]) -> X25519PrivateKey:
    try:
        return X25519PrivateKey.from_private_bytes(_unb64url(device["privateKey"]))
    except (KeyError, ValueError):
        raise InvalidParams("This device's key isn't valid. Pair again.") from None


def _password(code: str) -> bytes:
    """The CPace PRS: the code's eight characters, normalized (section 6.1)."""
    out = []
    for c in code:
        c = c.upper() if c.isascii() else c
        if c in "- ":
            continue
        c = {"I": "1", "L": "1", "O": "0"}.get(c, c)
        if c not in _ALPHABET:
            raise InvalidParams("That isn't a pairing code. Use only the letters and digits shown.")
        out.append(c)
    if len(out) != 8:
        raise InvalidParams("A pairing code is 8 letters and digits.")
    return "".join(out).encode()
