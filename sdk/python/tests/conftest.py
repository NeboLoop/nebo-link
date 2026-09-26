"""Runs the OAL conformance suite's fake host (crates/oal-conformance) as a
subprocess. It prints every frame a client sends that breaks the spec."""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]


@dataclass
class FakeHost:
    url: str
    code: str
    process: asyncio.subprocess.Process
    violations: list[str] = field(default_factory=list)


async def start_fake_host(code: str = "K7QM-3XRD") -> FakeHost:
    binary = Path(os.environ.get("OAL_CONFORMANCE", REPO / "target" / "debug" / "oal-conformance"))
    if not binary.exists():
        pytest.fail(f"No fake host at {binary}. Build it with `cargo build -p oal-conformance`, or set OAL_CONFORMANCE.")
    process = await asyncio.create_subprocess_exec(
        str(binary), "client", "--listen", "127.0.0.1:0", "--code", code,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdout and process.stderr
    while True:
        line = (await asyncio.wait_for(process.stdout.readline(), 10)).decode()
        if not line:
            raise RuntimeError("The fake host exited.")
        match = re.search(r"(ws://\S+/oal)", line)
        if match:
            break
    host = FakeHost(match.group(1), code, process)

    async def watch() -> None:
        assert process.stderr
        async for raw in process.stderr:
            if b"SPEC VIOLATION" in raw:
                host.violations.append(raw.decode().rstrip())

    host._watch = asyncio.ensure_future(watch())  # type: ignore[attr-defined]
    return host


async def stop_fake_host(host: FakeHost) -> None:
    host.process.kill()
    await host.process.wait()
    host._watch.cancel()  # type: ignore[attr-defined]


@pytest.fixture
async def fake_host() -> AsyncIterator[FakeHost]:
    host = await start_fake_host()
    yield host
    await stop_fake_host(host)
    assert host.violations == []
