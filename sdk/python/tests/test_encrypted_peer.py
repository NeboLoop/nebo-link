"""Interop with the Rust reference (crates/oal-secure) through its test peer,
a host that pairs and serves encrypted sessions. Set ``OAL_PEER`` to the
built ``oal-secure-peer`` (``cargo build -p oal-secure --example peer``);
skipped otherwise."""

from __future__ import annotations

import asyncio
import os
import tempfile
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import pytest

from openagentlink import (
    PROTOCOL,
    ChannelClosed,
    ChannelContext,
    FrameChannel,
    Identity,
    PairingRefused,
    Unauthenticated,
    VersionMismatch,
    connect,
    encrypted,
    pair,
    websocket_dialer,
)
from openagentlink.identity import _generate_key_pair
from openagentlink.types import ClientInfo, VersionRange

pytestmark = pytest.mark.skipif(not os.environ.get("OAL_PEER"), reason="set OAL_PEER to the oal-secure peer binary")

CODE = "K7QM-3XRD"
HOST_ID = "h-peer"
CLIENT: ClientInfo = {"name": "openagentlink-python tests", "version": "0"}


@dataclass
class Peer:
    url: str
    public_key: str


@pytest.fixture
async def peer() -> AsyncIterator[Peer]:
    with tempfile.TemporaryDirectory(prefix="oal-peer-") as keys:
        process = await asyncio.create_subprocess_exec(
            os.environ["OAL_PEER"], keys, HOST_ID, CODE, stdout=asyncio.subprocess.PIPE
        )
        assert process.stdout
        url = (await asyncio.wait_for(process.stdout.readline(), 10)).decode().strip()
        public_key = (await asyncio.wait_for(process.stdout.readline(), 10)).decode().strip()
        assert url.startswith("ws://"), url
        try:
            yield Peer(url, public_key)
        finally:
            process.kill()
            await process.wait()


@pytest.fixture
async def identity(peer: Peer) -> Identity:
    return await pair(url=f"{peer.url}/oal/pair/{CODE[:4]}", code=CODE.lower(), device_name="Python laptop")


def context(identity: Identity, protocol: VersionRange | None = None) -> ChannelContext:
    """What ``connect`` gives the channel for ``identity``."""
    return ChannelContext(
        protocol=protocol or {"min": PROTOCOL["min"], "max": PROTOCOL["max"]},
        client=CLIENT,
        host={"id": identity.host.id, "publicKey": identity.host.public_key},
        device={"id": identity.device.id, "publicKey": identity.device.public_key, "privateKey": identity.device.private_key},
    )


async def pairing(peer: Peer, code: str) -> tuple[FrameChannel, str]:
    """A pairing connection opened as ``pair`` opens it, and the device's public key."""
    public_key, private_key = _generate_key_pair()
    ctx = ChannelContext(
        protocol={"min": PROTOCOL["min"], "max": PROTOCOL["max"]},
        client=CLIENT,
        device={"publicKey": public_key, "privateKey": private_key},
        code=code,
    )
    socket = await websocket_dialer(f"{peer.url}/oal/pair/{code[:4]}", ["oal"])
    return await encrypted.open(socket, ctx), public_key


async def session(peer: Peer, identity: Identity, protocol: VersionRange | None = None) -> FrameChannel:
    socket = await websocket_dialer(f"{peer.url}/oal/hosts/{HOST_ID}", ["oal"])
    return await encrypted.open(socket, context(identity, protocol))


async def call(channel: FrameChannel, method: str, params: Any, id: int = 1) -> Any:
    await channel.send({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
    frame = await channel.recv()
    assert frame["id"] == id
    return frame["result"]


async def test_pairs_then_connects_with_the_stored_identity(peer: Peer, identity: Identity) -> None:
    assert (identity.host.id, identity.host.name) == (HOST_ID, "OAL peer")
    assert identity.host.public_key == peer.public_key
    assert identity.device.id == "d-1"

    channel = await session(peer, identity)
    try:
        assert channel.authenticated is not None
        assert channel.authenticated.protocol == "0.1"
        assert channel.authenticated.device == {"id": "d-1", "name": "Python laptop"}
        assert await call(channel, "echo", {"hello": "wörld", "n": [1, 2]}) == {"hello": "wörld", "n": [1, 2]}
        assert await call(channel, "echo", "again", id=2) == "again"
    finally:
        await channel.close()


async def test_the_pairing_connection_stays_open_as_a_session(peer: Peer) -> None:
    channel, public_key = await pairing(peer, CODE)
    try:
        assert channel.authenticated is None
        assert channel.host_key == peer.public_key
        params = {"protocol": PROTOCOL, "client": CLIENT, "code": CODE, "device": {"name": "Tab", "publicKey": public_key}}
        result = await call(channel, "host/pair", params, id=0)
        assert result["info"]["host"]["publicKey"] == channel.host_key
        assert await call(channel, "echo", [1, 2, 3], id=1) == [1, 2, 3]
    finally:
        await channel.close()


async def test_big_frames_both_ways(peer: Peer, identity: Identity) -> None:
    channel = await session(peer, identity)
    try:
        payload = {"data": "é" * 150_000 + "y" * 200_001}  # over 1 MB as JSON: many parts
        assert await call(channel, "echo", payload) == payload
        result = await call(channel, "big", {"bytes": 3_000_000}, id=2)
        assert result == {"data": "x" * 3_000_000}
        assert await call(channel, "echo", "small after big", id=3) == "small after big"
    finally:
        await channel.close()


async def test_a_frame_over_the_limit_ends_it_with_1009(peer: Peer, identity: Identity) -> None:
    channel = await session(peer, identity)
    await channel.send({"jsonrpc": "2.0", "id": 1, "method": "big", "params": {"bytes": 5 * 1024 * 1024}})
    with pytest.raises(ChannelClosed) as caught:
        await channel.recv()
    assert caught.value.code == 1009


async def test_a_wrong_code_is_refused(peer: Peer) -> None:
    with pytest.raises(PairingRefused) as caught:
        await pair(url=f"{peer.url}/oal/pair/K7QM", code="K7QM-3XRE", device_name="x")
    assert caught.value.code == "pairing_refused"

    # On the wire: the host closes with 4001 once the first Noise message fails.
    with pytest.raises(ChannelClosed) as closed:
        await pairing(peer, "K7QM-3XRE")
    assert closed.value.code == 4001


async def test_version_mismatch(peer: Peer, identity: Identity) -> None:
    with pytest.raises(VersionMismatch) as caught:
        await session(peer, identity, {"min": "9.0", "max": "9.0"})
    assert caught.value.rpc_code == -33001
    assert caught.value.message == "This app speaks another version of OAL and this computer speaks 0.1. Update the app."


async def test_unpair_ends_the_session_with_4003(peer: Peer, identity: Identity) -> None:
    channel = await session(peer, identity)
    other = await session(peer, identity)
    await channel.send({"jsonrpc": "2.0", "id": 1, "method": "unpair"})
    for c in (channel, other):
        with pytest.raises(ChannelClosed) as caught:
            await c.recv()
        assert caught.value.code == 4003

    # The key is no longer paired: a new session is refused with 4001.
    with pytest.raises(ChannelClosed) as refused:
        await session(peer, identity)
    assert refused.value.code == 4001
    with pytest.raises(Unauthenticated):
        await connect(url=f"{peer.url}/oal/hosts/{HOST_ID}", credentials=identity)
