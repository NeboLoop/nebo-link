"""One host's connection: dial, handshake, heartbeat, reconnect, and routing
JSON-RPC between the host channel and the agent channels (spec sections 4, 5,
11 and 12).
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from collections.abc import Coroutine
from typing import Any, Protocol

from .channel import ChannelClosed, ChannelContext, Dialer, Frame, FrameChannel, SecureChannel
from .errors import (
    Closed,
    ConnectionLost,
    HostOffline,
    InvalidParams,
    OALError,
    PairingRefused,
    Unauthenticated,
    Unpaired,
    VersionMismatch,
)
from .types import PROTOCOL, HostInfo

#: A client sends ``host/ping`` after this long without sending anything...
PING_AFTER = 20.0
#: ...and gives up on the connection when the ping has no answer by then.
PING_TIMEOUT = 10.0
#: Requests wait this long for a reconnect before failing with ``HostOffline``.
RECONNECT_GRACE = 10.0
BACKOFF_START = 1.0
BACKOFF_MAX = 30.0

_PERMANENT = (Unauthenticated, VersionMismatch, Unpaired, PairingRefused, InvalidParams, Closed)


class LinkHandlers(Protocol):
    def host_notification(self, method: str, params: dict[str, Any]) -> None: ...

    def agent_message(self, agent: str, message: dict[str, Any]) -> None: ...

    async def connected(self) -> None:
        """Runs before the link reports ready; requests made here pass ``early=True``."""
        ...

    def disconnected(self) -> None: ...


def close_error(code: int, host_name: str) -> OALError:
    if code == 4001:
        return Unauthenticated(f"This device isn't paired with {host_name}. Pair it again.")
    if code == 4002:
        return VersionMismatch(f"This app and {host_name} don't speak the same OAL version.")
    if code == 4003:
        return Unpaired(f"This device was unpaired from {host_name}. Pair it again.")
    return ConnectionLost(f"Lost the connection to {host_name}.")


async def exchange(channel: FrameChannel, method: str, params: dict[str, Any], host_name: str) -> dict[str, Any]:
    """Sends the first request on a channel that isn't pumped yet and reads until its answer."""
    await channel.send({"jsonrpc": "2.0", "id": 0, "method": method, "params": params})
    while True:
        try:
            frame = await channel.recv()
        except ChannelClosed as closed:
            raise close_error(closed.code, host_name) from None
        if frame.get("id") == 0 and "method" not in frame and "agent" not in frame:
            if frame.get("error"):
                raise OALError.from_rpc(frame["error"])
            result: dict[str, Any] = frame.get("result") or {}
            return result


class Link:
    def __init__(
        self,
        *,
        url: str,
        host_name: str,
        context: ChannelContext,
        auth: dict[str, Any],
        secure: SecureChannel,
        dialer: Dialer,
    ) -> None:
        self.url = url
        self.host_name = host_name
        self.context = context
        self.auth = auth
        self.secure = secure
        self.dialer = dialer
        self.handlers: LinkHandlers
        self.info: HostInfo | None = None
        self.device: dict[str, str] | None = None
        self.online = False

        self._channel: FrameChannel | None = None
        self._outbox: asyncio.Queue[Frame] | None = None
        self._waiters: dict[str, asyncio.Future[Any]] = {}
        self._next_id = 0
        self._last_sent = 0.0
        self._changed = asyncio.Event()
        self._failure: OALError | None = None
        self._closing = False
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        """Starts connecting. Returns after the first attempt; raises if it can never succeed."""
        first: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._task = asyncio.create_task(self._run(first))
        await first

    async def close(self) -> None:
        self._closing = True
        self._fail(Closed("The client was closed."))
        self._wake.set()
        if self._channel is not None:
            with contextlib.suppress(Exception):
                await self._channel.close(1000, "")
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(BaseException):
                await self._task
        for task in list(self._tasks):
            task.cancel()

    async def request(self, method: str, params: dict[str, Any], *, agent: str | None = None, early: bool = False) -> Any:
        """A request on the host channel, or on ``agent``'s channel."""
        if not early:
            await self.when_ready()
        if self._outbox is None:
            raise self._lost()
        self._next_id += 1
        request_id = self._next_id
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._waiters[f"{agent or ''}#{request_id}"] = future
        message = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}
        self.send(message if agent is None else {"agent": agent, "acp": message})
        return await future

    async def notify(self, agent: str, method: str, params: dict[str, Any]) -> None:
        """A notification on ``agent``'s channel."""
        await self.when_ready()
        if self._outbox is None:
            raise self._lost()
        self.send({"agent": agent, "acp": {"jsonrpc": "2.0", "method": method, "params": params}})

    def respond(self, agent: str, request_id: Any, answer: dict[str, Any]) -> None:
        """Answers a request the host sent on ``agent``'s channel."""
        if self._outbox is not None:
            self.send({"agent": agent, "acp": {"jsonrpc": "2.0", "id": request_id, **answer}})

    def send(self, frame: Frame) -> None:
        if self._outbox is not None:
            self._last_sent = time.monotonic()
            self._outbox.put_nowait(frame)

    def spawn(self, coroutine: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        """Runs a task the link keeps a reference to, cancelled on close."""
        task = asyncio.ensure_future(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def when_ready(self) -> None:
        """Waits for the connection, up to the reconnect grace period."""
        deadline = time.monotonic() + RECONNECT_GRACE
        while True:
            if self._failure is not None:
                raise self._failure
            if self.online:
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise HostOffline(f"{self.host_name} is offline.")
            self._changed.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._changed.wait(), remaining)

    def _lost(self) -> OALError:
        return self._failure or ConnectionLost(f"Lost the connection to {self.host_name}.")

    def _fail(self, error: OALError) -> None:
        if self._failure is None:
            self._failure = error
        self._reject_waiters(self._failure)
        self._changed.set()

    def _reject_waiters(self, error: OALError) -> None:
        waiters, self._waiters = self._waiters, {}
        for future in waiters.values():
            if not future.done():
                future.set_exception(error)

    async def _run(self, first: asyncio.Future[None]) -> None:
        delay = BACKOFF_START

        def settle_first(error: OALError | None = None) -> None:
            if not first.done():
                if error is None:
                    first.set_result(None)
                else:
                    first.set_exception(error)

        while not self._closing:
            try:
                channel = await self._open()
            except Exception as error:
                if isinstance(error, _PERMANENT):
                    self._fail(error)
                    settle_first(error)
                    return
                settle_first()  # Offline, or a transient failure: try again.
                await self._sleep(delay)
                delay = min(delay * 2, BACKOFF_MAX)
                continue
            delay = BACKOFF_START
            self._channel = channel
            self._outbox = asyncio.Queue()
            closed = asyncio.ensure_future(self._pump(channel))
            writer = asyncio.ensure_future(self._write(channel, self._outbox))
            heartbeat = asyncio.ensure_future(self._heartbeat(channel))
            try:
                with contextlib.suppress(Exception):
                    await self.handlers.connected()
                if not closed.done():
                    self.online = True
                    self._changed.set()
                settle_first()
                code = await closed
            finally:
                writer.cancel()
                heartbeat.cancel()
                closed.cancel()
            self._channel = None
            self._outbox = None
            self.online = False
            self._changed.set()
            lost = close_error(code, self.host_name)
            self._reject_waiters(lost)
            self.handlers.disconnected()
            if isinstance(lost, _PERMANENT):
                self._fail(lost)
                return
            await self._sleep(delay)

    async def _open(self) -> FrameChannel:
        try:
            socket = await self.dialer(self.url, ["oal"])
        except HostOffline:
            raise HostOffline(f"Couldn't reach {self.host_name}.") from None
        channel = await self.secure.open(socket, self.context)
        try:
            if channel.authenticated is not None:
                device = channel.authenticated.device
                self.device = {"id": device["id"], "name": device.get("name", "")}
                self.info = await exchange(channel, "host/info", {}, self.host_name)  # type: ignore[assignment]
            else:
                result = await exchange(
                    channel,
                    "host/hello",
                    {"protocol": PROTOCOL, "client": self.context.client, "auth": self.auth},
                    self.host_name,
                )
                self.device = result["device"]
                self.info = result["info"]
        except BaseException:
            with contextlib.suppress(Exception):
                await channel.close(1000, "")
            raise
        return channel

    async def _pump(self, channel: FrameChannel) -> int:
        while True:
            try:
                frame = await channel.recv()
            except ChannelClosed as closed:
                return closed.code
            except Exception:
                return 1006
            with contextlib.suppress(Exception):  # One bad frame never takes the connection down.
                self._dispatch(frame)

    async def _write(self, channel: FrameChannel, outbox: asyncio.Queue[Frame]) -> None:
        while True:
            frame = await outbox.get()
            try:
                await channel.send(frame)
            except Exception:
                return

    def _dispatch(self, frame: Frame) -> None:
        agent = frame.get("agent")
        if isinstance(agent, str):
            message = frame.get("acp")
            if not isinstance(message, dict):
                return
            if "method" in message:
                self.handlers.agent_message(agent, message)
            else:
                self._settle(f"{agent}#{message.get('id')}", message)
        elif "method" in frame:
            if "id" not in frame:
                self.handlers.host_notification(frame["method"], frame.get("params") or {})
        elif "id" in frame:
            self._settle(f"#{frame['id']}", frame)

    def _settle(self, key: str, message: dict[str, Any]) -> None:
        future = self._waiters.pop(key, None)
        if future is None or future.done():
            return
        if message.get("error"):
            future.set_exception(OALError.from_rpc(message["error"]))
        else:
            future.set_result(message.get("result"))

    async def _heartbeat(self, channel: FrameChannel) -> None:
        self._last_sent = time.monotonic()
        while True:
            await asyncio.sleep(1)
            if time.monotonic() - self._last_sent < PING_AFTER:
                continue
            try:
                await asyncio.wait_for(self.request("host/ping", {}, early=True), PING_TIMEOUT)
            except TimeoutError:
                await channel.close(4000, "No answer to ping.")
                return
            except OALError:
                return

    async def _sleep(self, seconds: float) -> None:
        """Waits ``seconds`` with jitter, or until the link is closed."""
        self._wake.clear()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wake.wait(), seconds / 2 + random.random() * seconds / 2)
