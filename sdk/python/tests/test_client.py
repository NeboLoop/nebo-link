from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest

from openagentlink import (
    AlreadyAnswered,
    ChannelContext,
    Client,
    Done,
    FrameChannel,
    Host,
    Identity,
    InvalidParams,
    NotOffered,
    PairingRefused,
    PermissionAsked,
    PermissionResolved,
    PresenceChanged,
    Socket,
    TextDelta,
    ToolResult,
    ToolStart,
    Turn,
    TurnError,
    TurnInProgress,
    UsageReport,
    UserMessage,
    connect,
    pair,
    plaintext,
    websocket_dialer,
)

from .conftest import FakeHost, start_fake_host, start_fake_relay, stop_fake_host, stop_fake_relay


@pytest.fixture
async def identity(fake_host: FakeHost) -> Identity:
    # The conformance suite's fake host speaks OAL 0.1 without encryption.
    return await pair(url=fake_host.url, code=fake_host.code, device_name="Test laptop", secure=plaintext)


@pytest.fixture
async def clients() -> AsyncIterator[list[Client]]:
    opened: list[Client] = []
    yield opened
    for client in opened:
        await client.close()


class Droppable:
    """A dialer that remembers its sockets, so a test can drop the connection."""

    def __init__(self) -> None:
        self.sockets: list[Socket] = []

    async def __call__(self, url: str, protocols: list[str]) -> Socket:
        socket = await websocket_dialer(url, protocols)
        self.sockets.append(socket)
        return socket

    async def drop(self) -> None:
        await self.sockets[-1].close(4000, "test drops the connection")


async def open_host(fake_host: FakeHost, identity: Identity, clients: list[Client], **options: Any) -> Host:
    client = await connect(url=fake_host.url, credentials=identity, **{"secure": plaintext, **options})
    clients.append(client)
    return client.hosts()[0]


async def fake_session(host: Host) -> Any:
    agent = next(a for a in await host.agents() if a.id == "fake")
    return await agent.session()


async def drain(turn: Turn, on: Callable[[Any], Awaitable[None] | None] = lambda e: None) -> list[Any]:
    events = []
    async for event in turn:
        events.append(event)
        result = on(event)
        if result is not None:
            await result
    return events


async def reconnected(host: Host) -> None:
    was_offline = False
    async for update in host.updates():
        if isinstance(update, PresenceChanged):
            if not update.online:
                was_offline = True
            elif was_offline:
                return


def kinds(events: list[Any]) -> list[str]:
    return [e.type for e in events]


# ---- pairing and the host layer ----


async def test_pairs_reconnects_and_lists_agents(fake_host: FakeHost, identity: Identity, clients: list[Client]) -> None:
    assert identity.host.id == "h-fake"
    assert identity.host.name == "Fake Host"
    assert len(identity.device.public_key) == 43

    host = await open_host(fake_host, identity, clients)
    assert [h.name for h in clients[0].hosts()] == ["Fake Host"]
    assert host.online
    assert host.info and host.info["protocol"] == {"min": "0.1", "max": "0.1"}

    agents = await host.agents()
    agent = next(a for a in agents if a.id == "fake")
    assert (agent.label, agent.folder, agent.online) == ("Fake Agent", "/tmp/oal-fake-agent", True)
    assert agent.modes and [m["id"] for m in agent.modes["availableModes"]] == ["ask", "folder", "full"]

    devices = await host.devices()
    assert [(d["name"], d["current"]) for d in devices] == [("Test laptop", True)]


async def test_identity_round_trips(identity: Identity, tmp_path: Any) -> None:
    path = tmp_path / "id.json"
    identity.save(path)
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    assert Identity.load(path) == identity
    assert set(identity.to_dict()["device"]) == {"id", "name", "token", "publicKey", "privateKey"}


async def test_refuses_a_wrong_code() -> None:
    other = await start_fake_host("AAAA-BBBB")
    try:
        with pytest.raises(PairingRefused) as caught:
            await pair(url=other.url, code="ZZZZ-ZZZZ", device_name="x", secure=plaintext)
        assert caught.value.message == "That code didn't work. Get a new one on the computer."
        assert caught.value.code == "pairing_refused"
    finally:
        await stop_fake_host(other)


async def test_custom_secure_channel_carries_every_frame(
    fake_host: FakeHost, identity: Identity, clients: list[Client]
) -> None:
    sent: list[dict[str, Any]] = []

    class Counting:
        async def open(self, socket: Socket, context: ChannelContext) -> FrameChannel:
            inner = await plaintext.open(socket, context)
            original = inner.send

            async def send(frame: dict[str, Any]) -> None:
                sent.append(frame)
                await original(frame)

            inner.send = send  # type: ignore[method-assign]
            return inner

    host = await open_host(fake_host, identity, clients, secure=Counting())
    await host.agents()
    assert [f["method"] for f in sent] == ["host/hello", "host/agents"]


# ---- turns ----


async def test_prompt_permission_approve_done(fake_host: FakeHost, identity: Identity, clients: list[Client]) -> None:
    session = await fake_session(await open_host(fake_host, identity, clients))

    async def on(event: Any) -> None:
        if isinstance(event, PermissionAsked):
            assert event.request.tool_call["title"] == "echo hi"
            assert [o["kind"] for o in event.request.options] == ["allow_once", "reject_once"]
            await event.request.allow_once()

    events = await drain(session.prompt("run: echo hi"), on)
    assert [k for k in kinds(events) if k != "permission_resolved"] == [
        "tool_start", "permission", "tool_result", "text", "usage", "done",
    ]
    result = next(e for e in events if isinstance(e, ToolResult))
    assert (result.output, result.tool["status"]) == ("hi", "completed")
    assert next(e for e in events if isinstance(e, TextDelta)) == TextDelta("Done.")
    assert next(e for e in events if isinstance(e, UsageReport)).usage == {"inputTokens": 12, "outputTokens": 5, "totalTokens": 17}
    assert events[-1] == Done("end_turn")
    resolved = next(e for e in events if isinstance(e, PermissionResolved))
    assert resolved.outcome == {"outcome": "selected", "optionId": "allow-once"}
    assert resolved.answered_by and resolved.answered_by["deviceId"] == identity.device.id


async def test_deny(fake_host: FakeHost, identity: Identity, clients: list[Client]) -> None:
    session = await fake_session(await open_host(fake_host, identity, clients))

    async def on(event: Any) -> None:
        if isinstance(event, PermissionAsked):
            await event.request.deny()

    events = await drain(session.prompt("run: rm -rf /"), on)
    assert next(e for e in events if isinstance(e, ToolResult)).tool["status"] == "failed"
    assert next(e for e in events if isinstance(e, TextDelta)) == TextDelta("Okay, I won't run it.")
    assert events[-1] == Done("end_turn")


async def test_cancel(fake_host: FakeHost, identity: Identity, clients: list[Client]) -> None:
    session = await fake_session(await open_host(fake_host, identity, clients))
    turn = session.prompt("wait")

    async def on(event: Any) -> None:
        if isinstance(event, TextDelta):
            await turn.cancel()

    assert await drain(turn, on) == [TextDelta("Working on it."), Done("cancelled")]


async def test_second_prompt_while_running(fake_host: FakeHost, identity: Identity, clients: list[Client]) -> None:
    session = await fake_session(await open_host(fake_host, identity, clients))
    first = session.prompt("wait")
    second = await drain(session.prompt("hello"))
    assert len(second) == 1 and isinstance(second[0], TurnError) and isinstance(second[0].error, TurnInProgress)

    async def on(event: Any) -> None:
        if isinstance(event, TextDelta):
            await first.cancel()

    await drain(first, on)


async def test_mode_change(fake_host: FakeHost, identity: Identity, clients: list[Client]) -> None:
    session = await fake_session(await open_host(fake_host, identity, clients))
    assert session.modes["currentModeId"] == "ask"
    await session.set_mode("full")
    assert session.modes["currentModeId"] == "full"
    events = await drain(session.prompt("run: echo hi"))
    assert kinds(events) == ["tool_start", "tool_result", "text", "usage", "done"]
    with pytest.raises(NotOffered):
        await session.set_mode("yolo")


# ---- resuming ----


async def test_reconnect_mid_turn_permission(fake_host: FakeHost, identity: Identity, clients: list[Client]) -> None:
    dialer = Droppable()
    host = await open_host(fake_host, identity, clients, dialer=dialer)
    session = await fake_session(host)

    async def on(event: Any) -> None:
        if isinstance(event, PermissionAsked):
            back = asyncio.ensure_future(reconnected(host))
            await dialer.drop()
            await asyncio.wait_for(back, 10)
            await event.request.allow_once()

    events = await drain(session.prompt("run: echo hi"), on)
    assert [k for k in kinds(events) if k != "permission_resolved"] == [
        "tool_start", "permission", "tool_result", "text", "usage", "done",
    ]
    assert events[-1] == Done("end_turn")


async def test_reconnect_mid_turn_cancel(fake_host: FakeHost, identity: Identity, clients: list[Client]) -> None:
    dialer = Droppable()
    host = await open_host(fake_host, identity, clients, dialer=dialer)
    session = await fake_session(host)
    turn = session.prompt("wait")

    async def on(event: Any) -> None:
        if isinstance(event, TextDelta):
            back = asyncio.ensure_future(reconnected(host))
            await dialer.drop()
            await asyncio.wait_for(back, 10)
            await turn.cancel()

    assert await drain(turn, on) == [TextDelta("Working on it."), Done("cancelled")]


async def test_another_client_loads_mid_turn_first_answer_wins(
    fake_host: FakeHost, identity: Identity, clients: list[Client]
) -> None:
    host_a = await open_host(fake_host, identity, clients)
    session_a = await fake_session(host_a)
    events_a = session_a.prompt("run: echo hi").__aiter__()
    assert isinstance(await anext(events_a), ToolStart)
    asked = await anext(events_a)
    assert isinstance(asked, PermissionAsked)

    host_b = await open_host(fake_host, identity, clients)
    inbox = await host_b.notifications()
    assert [r.tool_call["title"] for r in inbox] == ["echo hi"]

    agent_b = next(a for a in await host_b.agents() if a.id == "fake")
    session_b = await agent_b.session(session_a.id)
    assert kinds(session_b.history) == ["user", "tool_start"]
    assert session_b.history[0] == UserMessage("run: echo hi")
    assert session_b.turn is not None

    async def on(event: Any) -> None:
        if isinstance(event, PermissionAsked):
            await event.request.allow_once()

    events_b = await drain(session_b.turn, on)
    assert events_b[-1] == Done("end_turn")

    with pytest.raises(AlreadyAnswered):
        await asked.request.allow_once()

    rest = [e async for e in events_a]
    resolved = next(e for e in rest if isinstance(e, PermissionResolved))
    assert resolved.outcome == {"outcome": "selected", "optionId": "allow-once"}
    assert rest[-1] == Done("end_turn")


async def test_through_a_relay_proves_its_key_on_every_connection() -> None:
    other = await start_fake_host("AAAA-BBBB")
    relay = None
    client = None
    try:
        relay = await start_fake_relay(other)
        # The relay refuses a pairing URL with more than the nameplate, and
        # every request without a fresh proof of the device's key.
        paired = await pair(relay=relay.url, code="aaaa-bbbb", device_name="Relay laptop", secure=plaintext)
        assert paired.host.id == "h-fake"

        dropper = Droppable()
        client = await connect(relay=relay.url, credentials=paired, dialer=dropper, secure=plaintext)
        host = client.hosts()[0]
        session = await fake_session(host)

        async def on(event: Any) -> None:
            if isinstance(event, PermissionAsked):
                # A new connection needs a new challenge: nonces work once.
                back = asyncio.ensure_future(reconnected(host))
                await dropper.drop()
                await back
                await event.request.allow_once()

        events = await drain(session.prompt("run: echo hi"), on)
        result = next(e for e in events if isinstance(e, ToolResult))
        assert result.output == "hi"
        assert events[-1] == Done("end_turn")

        metrics = relay.metrics()
        assert metrics["oal_relay_auth_failures_total"] == 0
        assert metrics["oal_relay_pairing_connections_total"] == 1
        assert metrics["oal_relay_client_connections_total"] == 3  # pair, connect, reconnect
        assert other.violations == []
    finally:
        if client is not None:
            await client.close()
        if relay is not None:
            await stop_fake_relay(relay)
        await stop_fake_host(other)


async def test_refuses_a_relay_that_isnt_encrypted() -> None:
    with pytest.raises(InvalidParams):
        await pair(relay="ws://relay.example.com", code="K7QM-3XRD", device_name="x")
