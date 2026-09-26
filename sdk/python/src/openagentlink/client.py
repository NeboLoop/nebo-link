"""The client API: Client → Host → Agent → Session → Turn."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import Any
from urllib.parse import quote

from .channel import ChannelContext, Dialer, SecureChannel, websocket_dialer
from .connection import Link
from .encrypted import encrypted
from .errors import (
    AlreadyAnswered,
    Closed,
    ConnectionLost,
    InvalidParams,
    NotOffered,
    OALError,
    TurnInProgress,
    UnknownRequest,
)
from .events import (
    AgentChanged,
    Done,
    HostUpdate,
    PermissionAsked,
    PermissionResolved,
    PresenceChanged,
    TurnError,
    TurnEvent,
    UsageReport,
    to_events,
)
from .identity import DEFAULT_CLIENT, Identity, check_endpoint, relay_url
from .relay import relay_dialer
from .types import (
    PROTOCOL,
    AgentInfo,
    Attachment,
    ClientInfo,
    Device,
    DeviceRef,
    HostInfo,
    PendingRequestInfo,
    PermissionOption,
    PermissionOutcome,
    SessionModeState,
    SessionSummary,
    StopReason,
    ToolCall,
    Usage,
    VersionRange,
)

_END = object()


async def connect(
    *,
    credentials: Identity | Sequence[Identity],
    relay: str | None = None,
    url: str | None = None,
    client: ClientInfo = DEFAULT_CLIENT,
    secure: SecureChannel = encrypted,
    dialer: Dialer = websocket_dialer,
) -> Client:
    """Connects to every host in ``credentials`` (one identity, or several with a
    relay). Hosts that are offline keep retrying in the background."""
    check_endpoint(relay, url)
    identities = [credentials] if isinstance(credentials, Identity) else list(credentials)
    if url is not None and len(identities) != 1:
        raise InvalidParams("A url reaches one host. Pass a relay to reach several.")
    hosts = [
        Host(
            identity,
            url=url if url is not None else relay_url(relay or "", "/oal/hosts/" + quote(identity.host.id)),
            client=client,
            secure=secure,
            # Through a relay, every connection first proves this device's key.
            dialer=dialer
            if relay is None
            else relay_dialer(relay, {"publicKey": identity.device.public_key, "privateKey": identity.device.private_key}, dialer),
        )
        for identity in identities
    ]
    result = Client(hosts)
    try:
        await asyncio.gather(*(host._link.start() for host in hosts))
    except BaseException:
        await result.close()
        raise
    return result


class Client:
    def __init__(self, hosts: list[Host]) -> None:
        self._hosts = hosts

    def hosts(self) -> list[Host]:
        """The paired hosts. ``host.online`` says which are reachable now."""
        return list(self._hosts)

    async def close(self) -> None:
        for host in self._hosts:
            await host._close()

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


def _key(*parts: str) -> str:
    return "\0".join(parts)


class Host:
    def __init__(self, identity: Identity, *, url: str, client: ClientInfo, secure: SecureChannel, dialer: Dialer) -> None:
        self.id = identity.host.id
        self.name = identity.host.name
        self._client = client
        protocol: VersionRange = {"min": PROTOCOL["min"], "max": PROTOCOL["max"]}
        self._link = Link(
            url=url,
            host_name=identity.host.name,
            context=ChannelContext(
                protocol=protocol,
                client=client,
                host={"id": identity.host.id, "publicKey": identity.host.public_key},
                device={
                    "id": identity.device.id,
                    "publicKey": identity.device.public_key,
                    "privateKey": identity.device.private_key,
                },
            ),
            auth={"type": "device", "deviceId": identity.device.id, "token": identity.device.token},
            secure=secure,
            dialer=dialer,
        )
        self._link.handlers = self
        self._agents: dict[str, Agent] = {}
        self._sessions: dict[str, Session] = {}
        self._requests: dict[str, PermissionRequest] = {}
        self._initialized: dict[str, asyncio.Future[None]] = {}
        self._subscribers: set[asyncio.Queue[Any]] = set()

    @property
    def online(self) -> bool:
        return self._link.online

    @property
    def info(self) -> HostInfo | None:
        """``host/info``, as of the last connection."""
        return self._link.info

    @property
    def device(self) -> dict[str, str] | None:
        """The device this connection is authenticated as: ``{"id", "name"}``."""
        return self._link.device

    async def agents(self) -> list[Agent]:
        """The agents on this host."""
        result = await self._link.request("host/agents", {})
        return [self._agent_from(info) for info in result["agents"]]

    async def notifications(self) -> list[PermissionRequest]:
        """Permission requests waiting for an answer, across every agent (the inbox)."""
        result = await self._link.request("host/pending", {})
        return [self._request_from(info) for info in result["requests"]]

    async def devices(self) -> list[Device]:
        result = await self._link.request("host/devices", {})
        devices: list[Device] = result["devices"]
        return devices

    async def unpair(self, device_id: str | None = None) -> None:
        """Revokes a paired device; by default this one."""
        await self._link.request("host/unpair", {"deviceId": device_id or (self.device or {}).get("id")})

    def updates(self) -> AsyncIterator[HostUpdate]:
        """Presence, agent changes and permission requests, as they happen from now on."""
        return _Subscription(self._subscribers)

    # ---- internal ----

    async def _close(self) -> None:
        await self._link.close()
        for queue in self._subscribers:
            queue.put_nowait(_END)
        for session in self._sessions.values():
            session._closed()

    async def _initialize(self, agent: str, early: bool = False) -> None:
        """Sends ``initialize`` on an agent's channel once per connection."""
        done = self._initialized.get(agent)
        if done is None:
            done = asyncio.ensure_future(
                self._link.request(
                    "initialize",
                    {
                        "protocolVersion": 1,
                        "clientCapabilities": {"fs": {"readTextFile": False, "writeTextFile": False}, "terminal": False},
                        "clientInfo": self._client,
                    },
                    agent=agent,
                    early=early,
                )
            )
            self._initialized[agent] = done
        try:
            await asyncio.shield(done)
        except BaseException:
            if self._initialized.get(agent) is done:
                del self._initialized[agent]
            raise

    def _session(self, agent: str, session_id: str) -> Session | None:
        return self._sessions.get(_key(agent, session_id))

    def _track(self, session: Session) -> None:
        self._sessions[_key(session.agent.id, session.id)] = session

    def _untrack(self, session: Session) -> None:
        self._sessions.pop(_key(session.agent.id, session.id), None)

    async def _pending_id(self, request: PermissionRequest) -> str | None:
        """The host's pending id for a request, looked up when the host hasn't announced it yet."""
        if request.id is None:
            await self.notifications()
        return request.id

    def _emit(self, update: HostUpdate) -> None:
        for queue in self._subscribers:
            queue.put_nowait(update)

    def _agent_from(self, info: AgentInfo) -> Agent:
        agent = self._agents.get(info["id"])
        if agent is None:
            agent = Agent(self, info)
            self._agents[agent.id] = agent
        else:
            agent._set(info)
        return agent

    def _request_from(self, info: PendingRequestInfo) -> PermissionRequest:
        key = _key(info["agent"], info["sessionId"], info["toolCall"]["toolCallId"])
        request = self._requests.get(key)
        if request is None:
            request = PermissionRequest(self, info["agent"], info["sessionId"], info["toolCall"], info["options"])
            self._requests[key] = request
        request.id = info["id"]
        request.turn_id = info.get("turnId")
        request.created_at = info.get("createdAt")
        return request

    def _resolved(self, request: PermissionRequest, outcome: PermissionOutcome | None, answered_by: DeviceRef | None) -> None:
        if request.resolved is not None:
            return
        request.resolved = (outcome, answered_by)
        request._rpc_id = None
        self._requests.pop(_key(request.agent, request.session_id, request.tool_call["toolCallId"]), None)
        session = self._session(request.agent, request.session_id)
        if session is not None:
            session._resolved(request)
        self._emit(PermissionResolved(request, outcome, answered_by))

    # ---- LinkHandlers ----

    def host_notification(self, method: str, params: dict[str, Any]) -> None:
        if method == "host/turn":
            session = self._session(params.get("agent", ""), params.get("sessionId", ""))
            if session is not None:
                session._turn_notice(params)
        elif method == "host/pending_update":
            request = self._request_from(params["request"])
            if params.get("change") == "added":
                self._emit(PermissionAsked(request))
            elif params.get("change") == "resolved":
                self._resolved(request, params.get("outcome"), params.get("answeredBy"))
        elif method == "host/agent_update":
            agent = self._agent_from(params["agent"])
            if params.get("change") == "removed":
                self._agents.pop(agent.id, None)
            self._emit(AgentChanged(params["change"], agent))

    def agent_message(self, agent: str, message: dict[str, Any]) -> None:
        params: dict[str, Any] = message.get("params") or {}
        method = message.get("method")
        if method == "session/update":
            session = self._session(agent, params.get("sessionId", ""))
            if session is not None:
                session._update(params.get("update") or {})
        elif method == "session/request_permission":
            if "id" not in message:
                return
            key = _key(agent, params["sessionId"], params["toolCall"]["toolCallId"])
            request = self._requests.get(key)
            if request is None or request.resolved is not None:
                request = PermissionRequest(self, agent, params["sessionId"], params["toolCall"], params["options"])
                self._requests[key] = request
            # Announced on the host channel first, or the same request again after a reconnect or load.
            request._rpc_id = message["id"]
            session = self._session(agent, params["sessionId"])
            if session is not None:
                session._permission(request)
        elif method == "$/cancel_request":
            for request in self._requests.values():
                if request.agent == agent and request._rpc_id == params.get("requestId"):
                    request._rpc_id = None
            self._link.respond(agent, params.get("requestId"), {"error": {"code": -32800, "message": "Request cancelled"}})
        elif "id" in message:
            # This client offers no fs, terminal or elicitation.
            self._link.respond(agent, message["id"], {"error": {"code": -32601, "message": "Method not found"}})

    async def connected(self) -> None:
        self._initialized.clear()
        await asyncio.gather(*(session._resume() for session in list(self._sessions.values())))
        if self._requests:
            # Requests answered while away are no longer pending.
            result = await self._link.request("host/pending", {}, early=True)
            waiting = {id(self._request_from(info)) for info in result["requests"]}
            for request in list(self._requests.values()):
                if id(request) not in waiting:
                    self._resolved(request, None, None)
        self._emit(PresenceChanged(True))

    def disconnected(self) -> None:
        self._initialized.clear()
        for request in self._requests.values():
            request._rpc_id = None
        for session in self._sessions.values():
            session._disconnected()
        self._emit(PresenceChanged(False))


class _Subscription:
    """Receives host updates from the moment it is created until closed."""

    def __init__(self, subscribers: set[asyncio.Queue[Any]]) -> None:
        self._subscribers = subscribers
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        subscribers.add(self._queue)

    def __aiter__(self) -> _Subscription:
        return self

    async def __anext__(self) -> HostUpdate:
        item = await self._queue.get()
        if item is _END:
            self._subscribers.discard(self._queue)
            raise StopAsyncIteration
        update: HostUpdate = item
        return update

    async def aclose(self) -> None:
        self._subscribers.discard(self._queue)


class Agent:
    def __init__(self, host: Host, info: AgentInfo) -> None:
        self.host = host
        self.id = info["id"]
        self._set(info)

    def _set(self, info: AgentInfo) -> None:
        self.label: str = info["label"]
        self.runtime: str = info["runtime"]
        self.folder: str | None = info.get("folder")
        self.online: bool = info["online"]
        self.offline_reason: str | None = info.get("offlineReason")
        self.capabilities: dict[str, Any] = info.get("capabilities") or {}
        #: The modes a new session starts in, when the agent has modes.
        self.modes: SessionModeState | None = info.get("modes")

    def __repr__(self) -> str:
        return f"Agent(id={self.id!r}, label={self.label!r}, runtime={self.runtime!r}, online={self.online})"

    async def sessions(self) -> list[SessionSummary]:
        """The agent's sessions (ACP ``session/list``)."""
        await self.host._initialize(self.id)
        params = {"cwd": self.folder} if self.folder else {}
        result = await self.host._link.request("session/list", params, agent=self.id)
        sessions: list[SessionSummary] = result["sessions"]
        return sessions

    async def session(self, id: str | None = None) -> Session:
        """A new session, or with ``id`` an existing one, loaded: its ``history``, and
        its running ``turn`` with any permission request still waiting."""
        await self.host._initialize(self.id)
        params: dict[str, Any] = {"cwd": self.folder or "/", "mcpServers": []}
        if id is None:
            result = await self.host._link.request("session/new", params, agent=self.id)
            session = Session(self, result["sessionId"], result.get("modes") or self.modes)
            self.host._track(session)
            return session
        known = self.host._session(self.id, id)
        if known is not None:
            return known
        session = Session(self, id, None)
        self.host._track(session)
        session._replay = _Replay(first=True)
        try:
            result = await self.host._link.request("session/load", {**params, "sessionId": id}, agent=self.id)
        except BaseException:
            self.host._untrack(session)
            raise
        session._loaded(result)
        return session


class _Replay:
    def __init__(self, first: bool) -> None:
        self.first = first
        self.index = 0
        self.saw_running = False


class Session:
    def __init__(self, agent: Agent, id: str, modes: SessionModeState | None) -> None:
        self.agent = agent
        self.id = id
        #: The session's modes, kept current.
        self.modes = modes
        #: The conversation as it was when this session was loaded.
        self.history: list[TurnEvent] = []
        #: The turn running now, whoever started it; None between turns.
        self.turn: Turn | None = None
        self._log: list[dict[str, Any]] = []
        self._replay: _Replay | None = None

    def prompt(self, text: str, attachments: Sequence[Attachment] = ()) -> Turn:
        """Sends a prompt. The returned turn streams its events: ``async for event in turn``."""
        turn = Turn(self, local=True)
        if self.turn is not None:
            turn._fail(TurnInProgress(f"{self.agent.label} is still working on the last message. Wait for it or stop it."))
            return turn
        self.turn = turn
        prompt: list[dict[str, Any]] = [{"type": "text", "text": text}]
        for file in attachments:
            block: dict[str, Any] = {"type": "resource_link", "uri": file.url, "name": file.name}
            if file.mime_type is not None:
                block["mimeType"] = file.mime_type
            if file.size is not None:
                block["size"] = file.size
            prompt.append(block)
        self.agent.host._link.spawn(self._send_prompt(turn, prompt))
        return turn

    async def _send_prompt(self, turn: Turn, prompt: list[dict[str, Any]]) -> None:
        try:
            result = await self.agent.host._link.request(
                "session/prompt", {"sessionId": self.id, "prompt": prompt}, agent=self.agent.id
            )
        except ConnectionLost:
            return  # Settled by the resume (section 12).
        except OALError as error:
            turn._responded(error=error)
            return
        turn._responded(stop_reason=result.get("stopReason"), usage=result.get("usage"))

    async def cancel(self) -> None:
        """Stops the running turn. It ends with ``Done`` and stop reason ``cancelled``."""
        await self.agent.host._link.notify(self.agent.id, "session/cancel", {"sessionId": self.id})

    async def set_mode(self, mode_id: str) -> None:
        """Switches the session's mode (one of ``modes["availableModes"]``)."""
        if self.modes is not None and not any(m["id"] == mode_id for m in self.modes["availableModes"]):
            names = ", ".join(m["id"] for m in self.modes["availableModes"])
            raise NotOffered(f"{self.agent.label} has no mode called {mode_id}. Its modes are {names}.")
        await self.agent.host._link.request("session/set_mode", {"sessionId": self.id, "modeId": mode_id}, agent=self.agent.id)
        if self.modes is not None:
            self.modes = {**self.modes, "currentModeId": mode_id}

    # ---- internal ----

    def _update(self, update: dict[str, Any]) -> None:
        replay = self._replay
        if replay is not None and not replay.first and replay.index < len(self._log):
            if update == self._log[replay.index]:
                replay.index += 1
                return
            if update.get("sessionUpdate") == "user_message_chunk":
                return  # The echo of our own prompt: recorded by the host, never sent to us.
        self._log.append(update)
        if update.get("sessionUpdate") == "current_mode_update" and self.modes is not None:
            self.modes = {**self.modes, "currentModeId": update["currentModeId"]}
        events = to_events(update)
        if replay is not None and replay.first:
            self.history.extend(events)
        elif self.turn is not None:
            self.turn._push(*events)

    def _turn_notice(self, params: dict[str, Any]) -> None:
        turn = self.turn

        def mine(t: Turn) -> bool:
            return t.id == params.get("turnId") or (t.id is None and t._local)

        if params.get("state") == "running":
            if self._replay is not None:
                self._replay.saw_running = True
            if turn is not None and mine(turn):
                turn.id = params.get("turnId")
            else:
                self.turn = Turn(self, local=False)
                self.turn.id = params.get("turnId")
        elif params.get("state") == "ended" and turn is not None and mine(turn):
            turn.id = params.get("turnId")
            error = params.get("error")
            turn._ended(
                stop_reason=params.get("stopReason"),
                usage=params.get("usage"),
                error=OALError.from_rpc(error) if error else None,
            )

    def _permission(self, request: PermissionRequest) -> None:
        if self.turn is None or request._turn is self.turn:
            return
        request._turn = self.turn
        self.turn._push(PermissionAsked(request))

    def _resolved(self, request: PermissionRequest) -> None:
        if self.turn is not None and request.resolved is not None:
            self.turn._push(PermissionResolved(request, request.resolved[0], request.resolved[1]))

    def _loaded(self, result: dict[str, Any] | None) -> None:
        if result and result.get("modes"):
            self.modes = result["modes"]
        replay, self._replay = self._replay, None
        turn = self.turn
        if replay is not None and not replay.first and turn is not None and not replay.saw_running:
            # No turn running on the host: ours ended while we were away, or never got there.
            if turn.id is not None:
                turn._ended(stop_reason=None, ended_unseen=True)
            else:
                host = self.agent.host.name
                turn._fail(ConnectionLost(f"Lost the connection before {host} got the message. Send it again."))

    async def _resume(self) -> None:
        """Loads the session again on a new connection (section 12)."""
        try:
            await self.agent.host._initialize(self.agent.id, early=True)
            self._replay = _Replay(first=False)
            result = await self.agent.host._link.request(
                "session/load",
                {"sessionId": self.id, "cwd": self.agent.folder or "/", "mcpServers": []},
                agent=self.agent.id,
                early=True,
            )
            self._loaded(result)
        except OALError as error:
            self._replay = None
            if not isinstance(error, ConnectionLost) and self.turn is not None:
                self.turn._fail(error)

    def _disconnected(self) -> None:
        if self.turn is not None:
            self.turn._lost()

    def _closed(self) -> None:
        if self.turn is not None:
            self.turn._fail(Closed("The client was closed."))


class Turn:
    """One prompt and everything it streams. Iterate it once with ``async for``."""

    def __init__(self, session: Session, *, local: bool) -> None:
        self.session = session
        #: The host's id for the turn, once it has started.
        self.id: str | None = None
        self.finished = False
        self._local = local
        self._queue: asyncio.Queue[Any] = asyncio.Queue()
        self._awaiting_response = local
        self._ended_with: dict[str, Any] | None = None
        self._response: dict[str, Any] | None = None

    def __aiter__(self) -> AsyncIterator[TurnEvent]:
        return self._events()

    async def _events(self) -> AsyncIterator[TurnEvent]:
        while (item := await self._queue.get()) is not _END:
            yield item

    async def cancel(self) -> None:
        """Stops the turn (``session.cancel()``)."""
        await self.session.cancel()

    def _push(self, *events: TurnEvent) -> None:
        if not self.finished:
            for event in events:
                self._queue.put_nowait(event)

    def _ended(
        self,
        *,
        stop_reason: StopReason | None,
        usage: Usage | None = None,
        error: OALError | None = None,
        ended_unseen: bool = False,
    ) -> None:
        self._ended_with = {"stop_reason": stop_reason, "usage": usage, "error": error, "unseen": ended_unseen}
        self._settle()

    def _responded(self, *, stop_reason: StopReason | None = None, usage: Usage | None = None, error: OALError | None = None) -> None:
        self._response = {"stop_reason": stop_reason, "usage": usage, "error": error}
        self._awaiting_response = False
        self._settle()

    def _lost(self) -> None:
        self._awaiting_response = False

    def _fail(self, error: OALError) -> None:
        self._finish(None, None, error)

    def _settle(self) -> None:
        response = self._response or {}
        if response.get("error") is not None and self.id is None:
            self._finish(None, None, response["error"])
            return
        if self._ended_with is None or self._awaiting_response:
            return
        ended = self._ended_with
        stop_reason = ended["stop_reason"] if ended["unseen"] or ended["stop_reason"] else response.get("stop_reason")
        self._finish(stop_reason, ended["usage"] or response.get("usage"), ended["error"] or response.get("error"))

    def _finish(self, stop_reason: StopReason | None, usage: Usage | None, error: OALError | None) -> None:
        if self.finished:
            return
        if usage:
            self._queue.put_nowait(UsageReport(usage))
        self._queue.put_nowait(TurnError(error) if error is not None else Done(stop_reason))
        self.finished = True
        self._queue.put_nowait(_END)
        if self.session.turn is self:
            self.session.turn = None


class PermissionRequest:
    """An agent asking permission to use a tool. Answer it with ``allow_once``,
    ``allow_always``, ``deny`` or ``choose``. The first answer from any device wins."""

    def __init__(
        self, host: Host, agent: str, session_id: str, tool_call: ToolCall, options: list[PermissionOption]
    ) -> None:
        self._host = host
        self.agent = agent
        self.session_id = session_id
        self.tool_call = tool_call
        self.options = options
        #: The host's id for the request, once known.
        self.id: str | None = None
        self.turn_id: str | None = None
        self.created_at: str | None = None
        #: ``(outcome, answered_by)`` once answered, here or elsewhere.
        self.resolved: tuple[PermissionOutcome | None, DeviceRef | None] | None = None
        self._rpc_id: Any = None
        self._turn: Turn | None = None

    def __repr__(self) -> str:
        return f"PermissionRequest(agent={self.agent!r}, title={self.tool_call.get('title')!r})"

    async def allow_once(self) -> None:
        await self._choose_kind(("allow_once",), "Allow once")

    async def allow_always(self) -> None:
        await self._choose_kind(("allow_always",), "Always allow")

    async def deny(self) -> None:
        await self._choose_kind(("reject_once", "reject_always"), "Deny")

    async def choose(self, option_id: str) -> None:
        """Answers with one of ``options`` by its ``optionId``."""
        if not any(o["optionId"] == option_id for o in self.options):
            raise NotOffered("That isn't one of this request's choices.")
        if self.resolved is not None:
            raise AlreadyAnswered("This was already answered on another device.")
        for attempt in range(2):
            pending = await self._host._pending_id(self)
            if pending is None:
                if attempt > 0:
                    return  # Answered while the connection was down.
                raise UnknownRequest("That request is no longer waiting.")
            try:
                await self._host._link.request("host/answer", {"id": pending, "optionId": option_id})
                return
            except ConnectionLost:
                if attempt > 0:
                    raise
                self.id = None  # Look it up again once reconnected.

    async def _choose_kind(self, kinds: tuple[str, ...], label: str) -> None:
        for kind in kinds:
            for option in self.options:
                if option["kind"] == kind:
                    await self.choose(option["optionId"])
                    return
        raise NotOffered(f"This request doesn't offer {label}.")
