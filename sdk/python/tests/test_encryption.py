"""End-to-end encryption without a host: CPace against the draft's vectors,
Noise against the cacophony and snow vectors, and the framing, rekeying and
pairing checks against an in-process peer built from the same pieces."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey

from openagentlink import ChannelClosed, InvalidParams, PairingRefused, Socket, pair
from openagentlink.cpace import Cpace, calculate_generator, lv_cat, scalar_mult_vfy
from openagentlink.encrypted import PART, _EncryptedFrames, _password
from openagentlink.noise import CipherState, Handshake, NoiseError, public_bytes
from openagentlink.relay import b64url


def unhex(text: str) -> bytes:
    return bytes.fromhex("".join(text.split()))


# ---- CPace: draft-irtf-cfrg-cpace-21 ----


def test_lv_cat_matches_appendix_a1() -> None:
    assert lv_cat(b"") == unhex("00")
    assert lv_cat(b"1234") == unhex("0431323334")
    assert lv_cat(bytes(range(127)))[:2] == bytes([0x7F, 0x00])
    encoded = lv_cat(bytes(range(128)))
    assert len(encoded) == 130 and encoded[:3] == bytes([0x80, 0x01, 0x00])
    assert lv_cat(b"1234", b"5", b"", b"678") == unhex("043132333401350003363738")


# Appendix B.3: CPace with ristretto255 and SHA-512.
PRS = b"Password"
CI = unhex("0b415f696e69746961746f720b425f726573706f6e646572")
SID = unhex("7e4b4791d6a8ef019b936c79fb7f2c57")


def test_generator_matches_b31() -> None:
    assert calculate_generator(PRS, CI, SID) == unhex("222b6b195fe84b1652badb6f6a3ae3d24341e7306967f0b8115b40d5698c7e56")


def test_shares_secret_point_and_isk_match_b32_to_b35() -> None:
    a = Cpace(PRS, CI, SID, random=unhex("da3d23700a9e5699258aef94dc060dfda5ebb61f02a5ea77fad53f4ff0976d08"))
    b = Cpace(PRS, CI, SID, random=unhex("d2316b454718c35362d83d69df6320f38578ed5984651435e2949762d900b80d"))
    assert a.share == unhex("d6bac480f2c386c394efc7c47adb9925dcd2630b64f240c50f8d0eec482b9157")
    assert b.share == unhex("3ea7e0b19560d7c0b0f5734f63b955286dfa8232b5ebe63324e2d9e7433f7258")
    k = unhex("80b69a8a76457ab6a4d7f887a4bf6b55a2f80ac19c333f917a05fc9887c8b40f")
    assert scalar_mult_vfy(a.y, b.share) == k
    assert scalar_mult_vfy(b.y, a.share) == k
    isk = unhex(
        "b69effbf61b51d56401c0f65601abe428de8206feaaf0e32198896dcae7b35cd"
        "2b38950a39dfd5d4a79164614c2984f7daa460b588c1e80c3fa2068af7900447"
    )
    assert a.finish(SID, b"ADa", b.share, b"ADb", initiator=True) == isk
    assert b.finish(SID, b"ADb", a.share, b"ADa", initiator=False) == isk


def test_scalar_mult_vfy_matches_b310_and_refuses_b311() -> None:
    s = unhex("7cd0e075fa7955ba52c02759a6c90dbbfc10e6d40aea8d283e407d88cf538a05")
    x = unhex("2c3c6b8c4f3800e7aef6864025b4ed79bd599117e427c41bd47d93d654b4a51c")
    assert scalar_mult_vfy(s, x) == unhex("7c13645fe790a468f62c39beb7388e541d8405d1ade69d1778c5fe3e7f6b600e")
    assert scalar_mult_vfy(s, unhex("2b3c6b8c4f3800e7aef6864025b4ed79bd599117e427c41bd47d93d654b4a51c")) is None
    assert scalar_mult_vfy(s, bytes(32)) is None


def test_a_different_password_gives_a_different_key() -> None:
    a = Cpace(b"ABCD1234", b"ci", b"")
    b = Cpace(b"ABCD1235", b"ci", b"")
    assert a.finish(b"", b"", b.share, b"", initiator=True) != b.finish(b"", b"", a.share, b"", initiator=False)


def test_code_normalization_matches_the_host() -> None:
    assert _password("k7qm-3xrd") == b"K7QM3XRD"
    assert _password(" K7QM 3XRD ") == b"K7QM3XRD"
    assert _password("OIL0-0000") == b"01100000"
    for bad in ("ABCD-EFG", "ABCD-EFGHJ", "ABCD-EFGU", "ABCD-EFGÉ", "ABCD-EFGß"):
        with pytest.raises(InvalidParams):
            _password(bad)


# ---- Noise ----

VECTORS = json.loads((Path(__file__).parent / "noise_vectors.json").read_text())["vectors"]


@pytest.mark.parametrize("vector", VECTORS, ids=lambda v: f"{v['source']}:{v['protocol_name']}")
def test_noise_matches_the_vectors(vector: dict[str, Any]) -> None:
    def key(name: str) -> X25519PrivateKey:
        return X25519PrivateKey.from_private_bytes(unhex(vector[name]))

    initiator = Handshake(
        vector["protocol_name"],
        initiator=True,
        static=key("init_static"),
        prologue=unhex(vector["init_prologue"]),
        remote_static=unhex(vector["init_remote_static"]) if "init_remote_static" in vector else None,
        psks=[unhex(p) for p in vector.get("init_psks", [])],
        ephemeral=key("init_ephemeral"),
    )
    responder = Handshake(
        vector["protocol_name"],
        initiator=False,
        static=key("resp_static"),
        prologue=unhex(vector["resp_prologue"]),
        psks=[unhex(p) for p in vector.get("resp_psks", [])],
        ephemeral=key("resp_ephemeral"),
    )
    for i, message in enumerate(vector["messages"]):
        payload, ciphertext = unhex(message["payload"]), unhex(message["ciphertext"])
        sender, receiver = (initiator, responder) if i % 2 == 0 else (responder, initiator)
        if sender.transport is None:
            assert sender.write_message(payload) == ciphertext
            assert receiver.read_message(ciphertext) == payload
        else:
            assert receiver.transport is not None
            assert sender.transport[0].encrypt(b"", payload) == ciphertext
            assert receiver.transport[1].decrypt(b"", ciphertext) == payload
    if "handshake_hash" in vector:
        assert initiator.h == responder.h == unhex(vector["handshake_hash"])
    assert initiator.remote_static == public_bytes(key("resp_static"))
    assert responder.remote_static == public_bytes(key("init_static"))


def test_rekey_changes_the_key_and_keeps_the_counter() -> None:
    a, b = CipherState(bytes(32)), CipherState(bytes(32))
    first = a.encrypt(b"", b"hi")
    a.rekey()
    assert a.n == 1
    assert b.decrypt(b"", first) == b"hi"
    with pytest.raises(NoiseError):
        b.decrypt(b"", a.encrypt(b"", b"after"))


def test_ik_to_the_wrong_host_key_fails() -> None:
    host, stranger = X25519PrivateKey.generate(), X25519PrivateKey.generate()
    client = Handshake(
        "Noise_IK_25519_ChaChaPoly_BLAKE2s",
        initiator=True,
        static=X25519PrivateKey.generate(),
        prologue=b"p",
        remote_static=public_bytes(stranger),
    )
    responder = Handshake("Noise_IK_25519_ChaChaPoly_BLAKE2s", initiator=False, static=host, prologue=b"p")
    with pytest.raises(NoiseError):
        responder.read_message(client.write_message(b"hello"))


# ---- framing, over an in-process pipe ----


class Pipe:
    """One end of an in-memory WebSocket: messages in order, and closes with a code."""

    def __init__(self) -> None:
        self.inbox: asyncio.Queue[str | bytes | ChannelClosed] = asyncio.Queue()
        self.peer: Pipe
        self.closed: ChannelClosed | None = None
        self.sent: list[str | bytes] = []

    @staticmethod
    def pair() -> tuple[Pipe, Pipe]:
        a, b = Pipe(), Pipe()
        a.peer, b.peer = b, a
        return a, b

    async def send(self, data: str | bytes) -> None:
        if self.closed is not None:
            raise self.closed
        self.sent.append(data)
        self.peer.inbox.put_nowait(data)

    async def recv(self) -> str | bytes:
        if self.closed is not None:
            raise self.closed
        item = await self.inbox.get()
        if isinstance(item, ChannelClosed):
            self.closed = item
            raise item
        return item

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self.closed is None:
            self.closed = ChannelClosed(code, reason)
            self.peer.inbox.put_nowait(ChannelClosed(code, reason))


def connected() -> tuple[_EncryptedFrames, _EncryptedFrames, Pipe, Pipe]:
    """Two framed ends keyed by a real IK handshake."""
    host = X25519PrivateKey.generate()
    client = Handshake(
        "Noise_IK_25519_ChaChaPoly_BLAKE2s",
        initiator=True,
        static=X25519PrivateKey.generate(),
        prologue=b"OAL-E2E/1 h",
        remote_static=public_bytes(host),
    )
    responder = Handshake("Noise_IK_25519_ChaChaPoly_BLAKE2s", initiator=False, static=host, prologue=b"OAL-E2E/1 h")
    responder.read_message(client.write_message(b"{}"))
    client.read_message(responder.write_message(b"{}"))
    assert client.transport and responder.transport
    a, b = Pipe.pair()
    c = _EncryptedFrames(a, *client.transport, authenticated=None, host_key="")
    h = _EncryptedFrames(b, *responder.transport, authenticated=None, host_key="")
    return c, h, a, b


async def test_frames_of_any_size_come_back_whole() -> None:
    c, h, a, _ = connected()
    overhead = len(json.dumps({"d": ""}, separators=(",", ":")))
    for size in (0, 1, PART - overhead - 1, PART - overhead, PART - overhead + 1, 2 * PART - overhead, 300_000):
        frame = {"d": "x" * size}
        before = len(a.sent)
        await c.send(frame)
        assert await h.recv() == frame
        parts = a.sent[before:]
        assert all(isinstance(p, bytes) and len(p) <= 65535 for p in parts)
        assert len(parts) == max(1, -(-(size + overhead) // PART))
        await h.send(frame)
        assert await c.recv() == frame


async def test_both_sides_rekey_on_the_same_message() -> None:
    c, h, _, _ = connected()
    c.rekey_every = h.rekey_every = 3
    for i in range(20):
        await c.send({"i": i})
        assert await h.recv() == {"i": i}
        await h.send({"i": i, "big": "y" * 70_000})
        assert (await c.recv())["i"] == i


async def test_a_side_that_does_not_rekey_cannot_read_past_the_rekey() -> None:
    c, h, _, b = connected()
    c.rekey_every = 3
    for i in range(3):
        await c.send({"i": i})
        assert await h.recv() == {"i": i}
    await c.send({"after": True})
    with pytest.raises(ChannelClosed) as caught:
        await h.recv()
    assert caught.value.code == 4001
    assert b.closed is not None and b.closed.code == 4001
    with pytest.raises(ChannelClosed):
        await h.recv()


async def test_a_bad_part_byte_or_a_text_message_ends_it_with_1002() -> None:
    c, h, a, _ = connected()
    await a.send(c._send.encrypt(b"", b"\x02{}"))
    with pytest.raises(ChannelClosed) as caught:
        await h.recv()
    assert caught.value.code == 1002

    c, h, a, _ = connected()
    await a.send('{"jsonrpc":"2.0"}')
    with pytest.raises(ChannelClosed) as caught:
        await h.recv()
    assert caught.value.code == 1002


async def test_a_frame_over_the_limit_ends_it_with_1009() -> None:
    c, h, _, _ = connected()
    h.max_frame = 100_000
    await c.send({"d": "x" * 100_000})
    with pytest.raises(ChannelClosed) as caught:
        await h.recv()
    assert caught.value.code == 1009


# ---- pairing: the host must name the key the handshake authenticated ----


async def fake_pairing_host(socket: Pipe, code: bytes, *, claim: str | None = None) -> ChannelClosed | None:
    """The host side of section 17.5, answering ``host/pair`` with ``claim`` as its key."""
    host = X25519PrivateKey.generate()
    msg_a = await socket.recv()
    assert isinstance(msg_a, bytes) and len(msg_a) == 34
    cpace = Cpace(code, b"OAL-PAIR/1", b"")
    isk = cpace.finish(b"", b"", msg_a[1:33], b"", initiator=False)
    assert isk is not None
    await socket.send(lv_cat(cpace.share, b""))
    psk = hashlib.sha512(lv_cat(b"OAL-PAIR/1 psk", isk)).digest()[:32]
    handshake = Handshake(
        "Noise_XXpsk0_25519_ChaChaPoly_BLAKE2s", initiator=False, static=host, prologue=b"OAL-PAIR/1", psks=[psk]
    )
    try:
        message = await socket.recv()
        assert isinstance(message, bytes)
        handshake.read_message(message)
    except NoiseError:
        await socket.close(4001, "Pairing failed.")
        return None
    await socket.send(handshake.write_message(b""))
    message = await socket.recv()
    assert isinstance(message, bytes)
    handshake.read_message(message)
    assert handshake.transport and handshake.remote_static
    frames = _EncryptedFrames(socket, *handshake.transport, authenticated=None, host_key="")
    request = await frames.recv()
    assert request["method"] == "host/pair"
    assert request["params"]["device"]["publicKey"] == b64url(handshake.remote_static)
    key = claim if claim is not None else b64url(public_bytes(host))
    info = {"host": {"id": "h-1", "name": "Pipe Host", "publicKey": key}}
    device = {"id": "d-1", "name": request["params"]["device"]["name"], "token": "unused"}
    await frames.send({"jsonrpc": "2.0", "id": request["id"], "result": {"protocol": "0.1", "device": device, "info": info}})
    try:
        await socket.recv()
    except ChannelClosed as closed:
        return closed
    return None


async def pair_over_pipe(code: str, host_code: bytes, claim: str | None = None) -> tuple[Any, ChannelClosed | None]:
    client_end, host_end = Pipe.pair()

    async def dialer(url: str, protocols: list[str]) -> Socket:
        return client_end

    host = asyncio.ensure_future(fake_pairing_host(host_end, host_code, claim=claim))
    try:
        result: Any = await pair(url="ws://pipe/oal", code=code, device_name="Pipe laptop", dialer=dialer)
    except Exception as error:
        result = error
    return result, await host


async def test_pairs_over_the_pipe() -> None:
    identity, closed = await pair_over_pipe("k7qm-3xrd", b"K7QM3XRD")
    assert identity.host.id == "h-1" and identity.device.id == "d-1"
    assert closed is not None and closed.code == 1000


async def test_pairing_refuses_a_host_that_names_another_key() -> None:
    error, closed = await pair_over_pipe("K7QM-3XRD", b"K7QM3XRD", claim=b64url(bytes(32)))
    assert isinstance(error, PairingRefused)
    assert closed is not None and closed.code == 4001


async def test_pairing_with_the_wrong_code_is_refused() -> None:
    error, _ = await pair_over_pipe("K7QM-3XRE", b"K7QM3XRD")
    assert isinstance(error, PairingRefused)
