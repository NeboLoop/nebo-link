"""What a turn streams, in order, and what ``Host.updates()`` streams.

A turn ends with ``Done`` or ``TurnError``. Match on the classes::

    async for event in session.prompt("Run the tests"):
        match event:
            case TextDelta(text=text):
                print(text, end="")
            case PermissionAsked(request=request):
                await request.allow_once()
            case Done(stop_reason=reason):
                print(reason)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Literal

from .errors import OALError
from .types import DeviceRef, PermissionOutcome, PlanEntry, StopReason, ToolCall, Usage

if TYPE_CHECKING:
    from .client import Agent, PermissionRequest


@dataclass(frozen=True)
class TextDelta:
    type: ClassVar[str] = "text"
    text: str


@dataclass(frozen=True)
class Thinking:
    type: ClassVar[str] = "thinking"
    text: str


@dataclass(frozen=True)
class UserMessage:
    """A prompt from another device (or, in ``history``, any earlier prompt)."""

    type: ClassVar[str] = "user"
    text: str


@dataclass(frozen=True)
class ToolStart:
    type: ClassVar[str] = "tool_start"
    tool: ToolCall


@dataclass(frozen=True)
class ToolUpdate:
    type: ClassVar[str] = "tool_update"
    tool: ToolCall


@dataclass(frozen=True)
class ToolResult:
    """The tool call finished: ``tool["status"]`` is ``completed`` or ``failed``. ``output`` is its text."""

    type: ClassVar[str] = "tool_result"
    tool: ToolCall
    output: str


@dataclass(frozen=True)
class PermissionAsked:
    """The agent asks permission. Answer with ``request.allow_once()``, ``allow_always()``, ``deny()`` or ``choose()``."""

    type: ClassVar[str] = "permission"
    request: PermissionRequest


@dataclass(frozen=True)
class PermissionResolved:
    """A permission request was answered, here or on another device."""

    type: ClassVar[str] = "permission_resolved"
    request: PermissionRequest
    outcome: PermissionOutcome | None
    answered_by: DeviceRef | None


@dataclass(frozen=True)
class PlanUpdate:
    type: ClassVar[str] = "plan"
    entries: list[PlanEntry]


@dataclass(frozen=True)
class ModeChanged:
    type: ClassVar[str] = "mode"
    mode_id: str


@dataclass(frozen=True)
class UsageReport:
    type: ClassVar[str] = "usage"
    usage: Usage


@dataclass(frozen=True)
class Done:
    """The turn finished. ``stop_reason`` is None when it finished while this client
    was disconnected, so its outcome wasn't seen."""

    type: ClassVar[str] = "done"
    stop_reason: StopReason | None


@dataclass(frozen=True)
class TurnError:
    type: ClassVar[str] = "error"
    error: OALError


@dataclass(frozen=True)
class Update:
    """An ACP session update this SDK has no event for, unchanged."""

    type: ClassVar[str] = "update"
    update: dict[str, Any]


TurnEvent = (
    TextDelta
    | Thinking
    | UserMessage
    | ToolStart
    | ToolUpdate
    | ToolResult
    | PermissionAsked
    | PermissionResolved
    | PlanUpdate
    | ModeChanged
    | UsageReport
    | Done
    | TurnError
    | Update
)


@dataclass(frozen=True)
class PresenceChanged:
    online: bool


@dataclass(frozen=True)
class AgentChanged:
    change: Literal["added", "updated", "removed"]
    agent: Agent


HostUpdate = PresenceChanged | AgentChanged | PermissionAsked | PermissionResolved


def _text(content: Any) -> str | None:
    if isinstance(content, dict) and content.get("type") == "text":
        return str(content.get("text", ""))
    return None


def to_events(update: dict[str, Any]) -> list[TurnEvent]:
    """An ACP session update as events."""
    kind = update.get("sessionUpdate")
    rest = {k: v for k, v in update.items() if k != "sessionUpdate"}
    if kind in ("agent_message_chunk", "agent_thought_chunk", "user_message_chunk"):
        text = _text(update.get("content"))
        if text is not None:
            if kind == "agent_message_chunk":
                return [TextDelta(text)]
            if kind == "agent_thought_chunk":
                return [Thinking(text)]
            return [UserMessage(text)]
    elif kind == "tool_call":
        return [ToolStart(rest)]  # type: ignore[arg-type]
    elif kind == "tool_call_update":
        if rest.get("status") in ("completed", "failed"):
            output = "".join(
                _text(item.get("content")) or ""
                for item in rest.get("content") or []
                if isinstance(item, dict) and item.get("type") == "content"
            )
            return [ToolResult(rest, output)]  # type: ignore[arg-type]
        return [ToolUpdate(rest)]  # type: ignore[arg-type]
    elif kind == "plan":
        return [PlanUpdate(list(update.get("entries") or []))]
    elif kind == "current_mode_update":
        return [ModeChanged(str(update.get("currentModeId")))]
    return [Update(update)]
