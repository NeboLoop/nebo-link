"""The transport, in two layers, so that end-to-end encryption (spec section 17)
drops in without changing the client API:

- a ``Dialer`` opens a ``Socket``: WebSocket messages, text or binary;
- a ``SecureChannel`` turns a ``Socket`` into a ``FrameChannel``: whole OAL frames.

``plaintext`` sends each frame as one JSON text message (OAL 0.1) and leaves
authentication to ``host/hello``. An end-to-end channel (Noise IK, section
17.2) does its handshake in ``open``, authenticates the device by its static
key, and reports that in ``FrameChannel.authenticated``; the client then skips
``host/hello``.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import urlparse

import websockets
from websockets.asyncio.client import ClientConnection
from websockets.typing import Subprotocol

from .errors import HostOffline, InvalidParams
from .types import ClientInfo, VersionRange

Frame = dict[str, Any]


class ChannelClosed(Exception):
    """Raised by ``recv()`` once the socket or channel has closed."""

    def __init__(self, code: int, reason: str = "") -> None:
        super().__init__(f"Closed with {code}{': ' + reason if reason else ''}")
        self.code = code
        self.reason = reason


class Socket(Protocol):
    """An open WebSocket, message by message."""

    async def send(self, data: str | bytes) -> None: ...

    async def recv(self) -> str | bytes:
        """The next message. Raises ``ChannelClosed`` once the socket has closed."""
        ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


#: Opens a socket to a URL, offering the given subprotocols (OAL offers ``oal``).
Dialer = Callable[[str, list[str]], Awaitable[Socket]]


@dataclass
class ChannelContext:
    """What a secure channel knows when it opens."""

    protocol: VersionRange
    client: ClientInfo
    #: The host's ``{"id", "publicKey"}``, known once paired.
    host: dict[str, str] | None = None
    #: This device's X25519 ``{"publicKey", "privateKey"}`` (base64url), and ``"id"`` once paired.
    device: dict[str, str] | None = None
    #: The pairing code, when this connection pairs.
    code: str | None = None


@dataclass(frozen=True)
class Authenticated:
    """Set by a channel that authenticated the device itself (an end-to-end handshake)."""

    protocol: str
    device: dict[str, str] = field(default_factory=dict)


class FrameChannel(Protocol):
    """A connection that carries whole OAL frames."""

    #: When set, the client sends no ``host/hello``.
    authenticated: Authenticated | None

    async def send(self, frame: Frame) -> None: ...

    async def recv(self) -> Frame:
        """The next frame. Raises ``ChannelClosed`` once the channel has closed."""
        ...

    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class SecureChannel(Protocol):
    """Turns a socket into a frame channel. See ``plaintext``."""

    async def open(self, socket: Socket, context: ChannelContext) -> FrameChannel: ...


class _PlainFrames:
    authenticated: Authenticated | None = None

    def __init__(self, socket: Socket) -> None:
        self._socket = socket

    async def send(self, frame: Frame) -> None:
        await self._socket.send(json.dumps(frame, separators=(",", ":")))

    async def recv(self) -> Frame:
        while True:
            data = await self._socket.recv()
            if not isinstance(data, str):
                continue  # Binary is reserved for encryption.
            try:
                frame = json.loads(data)
            except ValueError:
                continue  # Not JSON: the spec says to drop it.
            if isinstance(frame, dict):
                return frame

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._socket.close(code, reason)


class _Plaintext:
    """OAL 0.1: one frame per JSON text message, authenticated by ``host/hello``."""

    async def open(self, socket: Socket, context: ChannelContext) -> FrameChannel:
        return _PlainFrames(socket)


plaintext: SecureChannel = _Plaintext()


class _WebSocket:
    def __init__(self, connection: ClientConnection) -> None:
        self._ws = connection

    async def send(self, data: str | bytes) -> None:
        try:
            await self._ws.send(data)
        except websockets.ConnectionClosed as error:
            raise _closed(error) from None

    async def recv(self) -> str | bytes:
        try:
            return await self._ws.recv()
        except websockets.ConnectionClosed as error:
            raise _closed(error) from None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._ws.close(code, reason)


def _closed(error: websockets.ConnectionClosed) -> ChannelClosed:
    frame = error.rcvd or error.sent
    return ChannelClosed(frame.code, frame.reason) if frame else ChannelClosed(1006)


async def websocket_dialer(url: str, protocols: list[str]) -> Socket:
    """Dials with the ``websockets`` library. OAL's own heartbeat replaces WebSocket pings."""
    try:
        connection = await websockets.connect(
            url, subprotocols=[Subprotocol(p) for p in protocols], ping_interval=None, max_size=None
        )
    except websockets.InvalidURI:
        raise InvalidParams(f"That isn't a WebSocket URL: {url}") from None
    except (OSError, websockets.InvalidHandshake, TimeoutError):
        raise HostOffline(f"Couldn't reach {urlparse(url).netloc}.") from None
    return _WebSocket(connection)
