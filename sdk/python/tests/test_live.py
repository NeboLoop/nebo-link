"""Against a real host through a real relay, encrypted end to end, with the
public API only. Skipped unless these are set:

- ``OAL_LIVE_RELAY``: the relay's base URL (``http://127.0.0.1:8480``);
- ``OAL_LIVE_CODE``: a fresh pairing code from the host;
- ``OAL_LIVE_AGENT``: the id of an agent on it that behaves as the conformance
  suite's scripted agent (crates/oal-conformance/src/fake_agent.rs): ``run:
  <cmd>`` asks permission, then echoes, and every turn reports the same usage.

The device it pairs is unpaired at the end.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
from typing import Any

import pytest

from openagentlink import (
    Done,
    OALError,
    PermissionAsked,
    PermissionRequest,
    TextDelta,
    ToolResult,
    UsageReport,
    connect,
    pair,
)

from .test_client import Droppable, drain, kinds, reconnected

_ENV = ("OAL_LIVE_RELAY", "OAL_LIVE_CODE", "OAL_LIVE_AGENT")
pytestmark = pytest.mark.skipif(not all(os.environ.get(k) for k in _ENV), reason="set " + ", ".join(_ENV))

USAGE = {"inputTokens": 12, "outputTokens": 5, "totalTokens": 17}


async def test_live_pair_prompt_permission_and_resume() -> None:
    relay = re.sub(r"^http", "ws", os.environ["OAL_LIVE_RELAY"])  # http → ws, https → wss
    identity = await pair(relay=relay, code=os.environ["OAL_LIVE_CODE"], device_name="openagentlink-python live test")
    dialer = Droppable()
    client = await connect(relay=relay, credentials=identity, dialer=dialer)
    host = client.hosts()[0]
    try:
        assert host.online
        assert host.device is not None and host.device["id"] == identity.device.id
        assert host.info is not None and host.info["host"]["publicKey"] == identity.host.public_key

        agent = next(a for a in await host.agents() if a.id == os.environ["OAL_LIVE_AGENT"])
        assert agent.folder, "session/new runs in the agent's folder from host/agents"
        session = await agent.session()

        # A turn: permission, answered here; the tool runs; the turn ends with its usage.
        async def allow(event: Any) -> None:
            if isinstance(event, PermissionAsked):
                assert event.request.tool_call["title"] == "echo hi"
                await event.request.allow_once()

        events = await asyncio.wait_for(drain(session.prompt("run: echo hi"), allow), 60)
        assert [k for k in kinds(events) if k != "permission_resolved"] == [
            "tool_start", "permission", "tool_result", "text", "usage", "done",
        ]
        assert next(e for e in events if isinstance(e, ToolResult)).output == "hi"
        assert next(e for e in events if isinstance(e, TextDelta)) == TextDelta("Done.")
        assert next(e for e in events if isinstance(e, UsageReport)).usage == USAGE
        assert events[-1] == Done("end_turn")

        # Resuming: the connection drops while the permission request waits. The
        # client reconnects (a new handshake), loads the session, finds the turn
        # still running and the request sent again, and answers it.
        turn = session.prompt("run: echo again")
        answered: list[PermissionRequest] = []

        async def drop_then_allow(event: Any) -> None:
            if isinstance(event, PermissionAsked):
                back = asyncio.ensure_future(reconnected(host))
                await dialer.drop()
                await asyncio.wait_for(back, 30)
                assert len(dialer.sockets) == 2
                assert session.turn is turn and turn.id is not None
                assert event.request._rpc_id is not None, "the host sends the waiting request again after session/load"
                await event.request.allow_once()
                answered.append(event.request)

        events = await asyncio.wait_for(drain(turn, drop_then_allow), 90)
        assert len(answered) == 1
        assert [k for k in kinds(events) if k != "permission_resolved"] == [
            "tool_start", "permission", "tool_result", "text", "usage", "done",
        ]
        assert next(e for e in events if isinstance(e, ToolResult)).output == "again"
        assert next(e for e in events if isinstance(e, UsageReport)).usage == USAGE
        assert events[-1] == Done("end_turn")
    finally:
        with contextlib.suppress(OALError):
            await host.unpair()
        await client.close()
