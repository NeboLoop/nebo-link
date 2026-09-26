"""Shapes of the messages OAL 0.1 defines, written from spec/schemas/ (the JSON
Schemas of spec/oal-0.1.md), and the ACP types a client sees on an agent
channel. ACP types keep ACP's own field names, because OAL carries ACP
unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, NotRequired, TypedDict

#: The OAL versions this SDK speaks.
PROTOCOL: dict[str, str] = {"min": "0.1", "max": "0.1"}


class VersionRange(TypedDict):
    min: str
    max: str


class ClientInfo(TypedDict):
    name: str
    version: str


class Runtime(TypedDict):
    id: str
    name: str
    #: ``acp``, or the adapter name (``openclaw``, ``hermes``).
    kind: str
    version: NotRequired[str]


class HostKeys(TypedDict):
    id: str
    name: str
    publicKey: str
    tlsFingerprint: NotRequired[str]


class HostInfo(TypedDict):
    """``host/info`` (section 7.1)."""

    host: HostKeys
    software: ClientInfo
    protocol: VersionRange
    acp: dict[str, int]
    runtimes: list[Runtime]
    maxFrameBytes: int
    attachments: dict[str, Any]


class SessionMode(TypedDict):
    id: str
    name: str
    description: NotRequired[str]


class SessionModeState(TypedDict):
    """ACP ``SessionModeState``."""

    currentModeId: str
    availableModes: list[SessionMode]


class AgentInfo(TypedDict):
    """An agent as ``host/agents`` describes it (section 7.2)."""

    id: str
    label: str
    runtime: str
    folder: NotRequired[str]
    online: bool
    offlineReason: NotRequired[str]
    capabilities: dict[str, Any]
    modes: NotRequired[SessionModeState | None]


class PermissionOption(TypedDict):
    """ACP ``PermissionOption``."""

    optionId: str
    name: str
    kind: Literal["allow_once", "allow_always", "reject_once", "reject_always"]


class ToolCall(TypedDict, total=False):
    """ACP ``ToolCall`` / ``ToolCallUpdate``. Only ``toolCallId`` is always present."""

    toolCallId: str
    title: str
    kind: str
    status: Literal["pending", "in_progress", "completed", "failed"]
    content: list[Any]
    locations: list[Any]
    rawInput: Any
    rawOutput: Any


class PlanEntry(TypedDict):
    """ACP ``PlanEntry``."""

    content: str
    priority: Literal["high", "medium", "low"]
    status: Literal["pending", "in_progress", "completed"]


#: ACP ``StopReason``.
StopReason = Literal["end_turn", "max_tokens", "max_turn_requests", "refusal", "cancelled"]


class Usage(TypedDict, total=False):
    """The tokens and cost of one turn (section 9)."""

    inputTokens: int
    outputTokens: int
    thoughtTokens: int
    cachedReadTokens: int
    cachedWriteTokens: int
    totalTokens: int
    cost: dict[str, Any]


class DeviceRef(TypedDict):
    """Who did something: a paired device."""

    deviceId: str
    name: str


class Device(TypedDict):
    """A paired device, from ``host/devices`` (section 6.4)."""

    id: str
    name: str
    pairedAt: str
    lastSeenAt: NotRequired[str]
    current: bool


class SessionSummary(TypedDict):
    """ACP ``SessionInfo``, from ``session/list``."""

    sessionId: str
    cwd: str
    title: NotRequired[str]
    updatedAt: NotRequired[str]


class PendingRequestInfo(TypedDict):
    """A pending permission request as the host lists it (section 10)."""

    id: str
    agent: str
    sessionId: str
    turnId: NotRequired[str]
    toolCall: ToolCall
    options: list[PermissionOption]
    createdAt: str


#: ACP ``RequestPermissionOutcome``: ``{"outcome": "selected", "optionId": ...}`` or ``{"outcome": "cancelled"}``.
PermissionOutcome = dict[str, str]


@dataclass(frozen=True)
class Attachment:
    """A file sent by reference (section 14): a URL the host can fetch."""

    url: str
    name: str
    mime_type: str | None = None
    size: int | None = None
