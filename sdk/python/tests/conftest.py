"""Runs the OAL conformance suite's fake host (crates/oal-conformance) as a
subprocess. It prints every frame a client sends that breaks the spec.
``start_fake_relay`` puts it behind a real relay (crates/oal-relay)."""

from __future__ import annotations

import asyncio
import os
import re
import shutil
import tempfile
import urllib.request
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


@dataclass
class FakeRelay:
    #: The relay's base URL, as ``pair`` and ``connect`` take it.
    url: str
    addr: str
    directory: str
    processes: list[asyncio.subprocess.Process]

    def metrics(self) -> dict[str, float]:
        """The relay's Prometheus counters, by name (``oal_relay_auth_failures_total``)."""
        with urllib.request.urlopen(f"http://{self.addr}/metrics") as response:
            text = response.read().decode()
        lines = [line for line in text.splitlines() if line and not line.startswith("#")]
        return {line.rsplit(" ", 1)[0]: float(line.rsplit(" ", 1)[1]) for line in lines}


async def start_fake_relay(host: FakeHost) -> FakeRelay:
    """A real relay (``oal-relay serve``) with ``host`` behind it through the host
    bridge (``oal-relay host --forward``), holding the nameplate of ``host.code``."""
    binary = Path(os.environ.get("OAL_RELAY", REPO / "target" / "debug" / "oal-relay"))
    if not binary.exists():
        pytest.fail(f"No relay at {binary}. Build it with `cargo build -p oal-relay`, or set OAL_RELAY.")
    relay = FakeRelay("", "", tempfile.mkdtemp(prefix="oal-relay-"), [])
    try:
        serve = await asyncio.create_subprocess_exec(
            str(binary), "serve", "--listen", "127.0.0.1:0", "--data-dir", relay.directory,
            stdout=asyncio.subprocess.PIPE,
        )
        relay.processes.append(serve)
        relay.addr = await _first_match(serve, r'"listen":"([^"]+)"')
        relay.url = f"ws://{relay.addr}"
        bridge = await asyncio.create_subprocess_exec(
            str(binary), "host", "--relay", f"http://{relay.addr}", "--id", "h-fake",
            "--key-file", str(Path(relay.directory) / "host.key"), "--forward", host.url, "--nameplate", host.code[:4],
            stdout=asyncio.subprocess.PIPE,
        )
        relay.processes.append(bridge)
        await _first_match(bridge, r"Pairing through the relay for codes starting (\S+)")
    except BaseException:
        await stop_fake_relay(relay)
        raise
    return relay


async def stop_fake_relay(relay: FakeRelay) -> None:
    for process in relay.processes:
        if process.returncode is None:
            process.kill()
        await process.wait()
    shutil.rmtree(relay.directory, ignore_errors=True)


async def _first_match(process: asyncio.subprocess.Process, pattern: str) -> str:
    """The first capture of ``pattern`` in a child's stdout; the rest is drained."""
    assert process.stdout
    while True:
        line = (await asyncio.wait_for(process.stdout.readline(), 10)).decode()
        if not line:
            raise RuntimeError(f"The relay exited with {await process.wait()}.")
        match = re.search(pattern, line)
        if match:
            break

    async def drain() -> None:
        assert process.stdout
        async for _ in process.stdout:
            pass

    process._drain = asyncio.ensure_future(drain())  # type: ignore[attr-defined]
    return match.group(1)
