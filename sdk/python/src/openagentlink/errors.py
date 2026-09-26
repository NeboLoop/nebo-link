"""Typed errors. Catch a class, or branch on ``code``; never on the message.

Every error's message is one plain sentence for people.
"""

from __future__ import annotations

from typing import Any, ClassVar


class OALError(Exception):
    """Every error this SDK raises or yields."""

    code: ClassVar[str] = "agent_error"

    def __init__(self, message: str, *, rpc_code: int | None = None, data: Any = None) -> None:
        super().__init__(message)
        self.message = message
        #: The JSON-RPC error code, when the error came from the host or agent.
        self.rpc_code = rpc_code
        self.data = data

    @staticmethod
    def from_rpc(error: dict[str, Any]) -> OALError:
        """The error for a JSON-RPC error object from a host or agent."""
        cls = _BY_RPC_CODE.get(error.get("code", 0), AgentError)
        return cls(str(error.get("message", "")), rpc_code=error.get("code"), data=error.get("data"))


# The OAL codes (spec section 15).
class VersionMismatch(OALError):
    code = "version_mismatch"


class Unauthenticated(OALError):
    code = "unauthenticated"


class PairingRefused(OALError):
    code = "pairing_refused"


class UnknownAgent(OALError):
    code = "unknown_agent"


class AgentUnavailable(OALError):
    code = "agent_unavailable"


class TurnInProgress(OALError):
    code = "turn_in_progress"


class AlreadyAnswered(OALError):
    code = "already_answered"


class UnknownRequest(OALError):
    code = "unknown_request"


class AttachmentFailed(OALError):
    code = "attachment_failed"


class NotPermitted(OALError):
    code = "not_permitted"


class TurnFailed(OALError):
    code = "turn_failed"


# From this SDK, or JSON-RPC.
class Unpaired(OALError):
    """The host revoked this device (close 4003). Pair again."""

    code = "unpaired"


class ConnectionLost(OALError):
    """The connection dropped during a request."""

    code = "connection_lost"


class HostOffline(OALError):
    """The host couldn't be reached."""

    code = "host_offline"


class NotOffered(OALError):
    """The choice or mode isn't one the request or session offers."""

    code = "not_offered"


class NotFound(OALError):
    """ACP -32002: no such session on this connection or agent."""

    code = "not_found"


class InvalidParams(OALError):
    code = "invalid_params"


class Closed(OALError):
    """The client was closed."""

    code = "closed"


class AgentError(OALError):
    """Any other error from the agent, passed through."""

    code = "agent_error"


_BY_RPC_CODE: dict[int, type[OALError]] = {
    -33001: VersionMismatch,
    -33002: Unauthenticated,
    -33003: PairingRefused,
    -33004: UnknownAgent,
    -33005: AgentUnavailable,
    -33006: TurnInProgress,
    -33007: AlreadyAnswered,
    -33008: UnknownRequest,
    -33009: AttachmentFailed,
    -33010: NotPermitted,
    -33011: TurnFailed,
    -32002: NotFound,
    -32602: InvalidParams,
}
