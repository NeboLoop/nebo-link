# openagentlink

The Python client for [Open Agent Link](https://openagent.link) (OAL). Drive the agents on another computer (Claude Code, Codex, Gemini CLI, OpenCode, OpenClaw, Hermes, or any agent that speaks [ACP](https://agentclientprotocol.com)) from your own app: sessions, streaming, permission prompts, modes, and resuming after a dropped connection.

OAL is ACP made reachable, plus a thin host layer. The spec is [`spec/oal-0.1.md`](../../spec/oal-0.1.md).

```sh
pip install openagentlink        # or: uv add openagentlink
```

Python 3.11 or later.

## Quickstart

Pair once with the code the computer shows (`nebo-link pair`), then ask Claude Code to run the tests and approve from your code:

```python
import asyncio
from openagentlink import Identity, PermissionAsked, TextDelta, Done, connect, pair

async def main():
    identity = await pair(relay="wss://relay.example.com", code="K7QM-3XRD", device_name="CI bot")
    identity.save("~/.config/openagentlink/identity.json")  # later: Identity.load(...)

    async with await connect(relay="wss://relay.example.com", credentials=identity) as client:
        agents = await client.hosts()[0].agents()
        claude = next(a for a in agents if a.runtime == "claude-code")
        session = await claude.session()
        async for event in session.prompt("Run the tests and fix what fails"):
            match event:
                case TextDelta(text=text): print(text, end="")
                case PermissionAsked(request=request): await request.allow_once()
                case Done(stop_reason=reason): print(f"\n{reason}")

asyncio.run(main())
```

`url="wss://..."` instead of `relay=` connects to one host directly (on the LAN, or the conformance suite's fake host).

## The API

| Call | What it does |
|---|---|
| `await pair(relay= \| url=, code=, device_name=)` | Turns a one-time code into an `Identity` (device id, token, X25519 key pair). `identity.save(path)` writes it readable only by you; `Identity.load(path)` reads it back. The file format is the same as the TypeScript SDK's. |
| `await connect(relay= \| url=, credentials=)` | Connects to one host, or several (a list of identities, through a relay). Returns a `Client`; `async with` closes it. Offline hosts keep retrying in the background. |
| `client.hosts()` | The hosts: `id`, `name`, `online`, `info`, `device`. |
| `await host.agents()` | The agents: `id`, `label`, `runtime`, `folder`, `online`, `offline_reason`, `modes`, `capabilities`. |
| `await host.notifications()` | Permission requests waiting on any agent (the inbox), each answerable. |
| `host.updates()` | `async for` presence, agent changes and permission requests as they happen. |
| `await host.devices()`, `await host.unpair(device_id=None)` | Paired devices; revoke one (by default this one). |
| `await agent.session()` / `await agent.session(id)` | A new session, or an existing one loaded: `session.history`, and `session.turn` if a turn is running (with any permission request still waiting). |
| `await agent.sessions()` | The agent's sessions (ACP `session/list`). |
| `session.prompt(text, attachments=())` | Sends a prompt; returns a `Turn` to `async for` over. Attachments are `Attachment(url, name, mime_type, size)`: URLs the host fetches. |
| `await session.cancel()` | Stops the running turn; it ends with `Done("cancelled")`. |
| `await session.set_mode(mode_id)` | Switches mode (`session.modes["availableModes"]`), e.g. Claude Code's `default` / `acceptEdits` / `bypassPermissions`. |

A turn streams, in order: `TextDelta`, `Thinking`, `UserMessage` (another device's prompt), `ToolStart`, `ToolUpdate`, `ToolResult` (`output` is its text), `PermissionAsked`, `PermissionResolved`, `PlanUpdate`, `ModeChanged`, `UsageReport`, then `Done(stop_reason)` or `TurnError(error)`. Unknown ACP updates arrive as `Update` so nothing is lost.

`PermissionAsked.request` has `allow_once()`, `allow_always()`, `deny()` and `choose(option_id)`. The first answer from any device wins; answering after someone else did raises `AlreadyAnswered`.

### Resuming

A dropped connection reconnects on its own (backoff from 1 to 30 seconds). Sessions are loaded again; what you already saw is not repeated, the running turn keeps streaming into the same `Turn`, and a permission request still waiting stays answerable through the same object. A turn that finished while you were away ends with `Done(None)`.

### Errors

Every error is an `OALError` with a plain one-sentence `message` and a stable `code`. Catch the subclass: `PairingRefused`, `Unauthenticated`, `Unpaired`, `VersionMismatch`, `UnknownAgent`, `AgentUnavailable`, `TurnInProgress`, `AlreadyAnswered`, `UnknownRequest`, `AttachmentFailed`, `NotPermitted`, `TurnFailed`, `HostOffline`, `ConnectionLost`, `NotOffered`, `NotFound`, `InvalidParams`, `Closed`, `AgentError`.

### Transport and encryption

`connect(..., dialer=..., secure=...)` takes the two transport layers. A `Dialer` opens a `Socket` (WebSocket messages); a `SecureChannel` turns it into a `FrameChannel` (whole OAL frames). The default, `plaintext`, is OAL 0.1: one JSON text message per frame, authenticated with `host/hello`. End-to-end encryption (spec section 17, Noise IK over X25519) plugs in as another `SecureChannel`: it gets the host's and this device's static keys in `ChannelContext`, does its handshake in `open`, and sets `FrameChannel.authenticated` so the client skips `host/hello`. Nothing else in your code changes.

Through a `relay`, every connection first proves this device's key to the relay ([`crates/oal-relay`](../../crates/oal-relay/README.md#protocol)): a fresh challenge from `GET /oal/challenge`, answered with an HMAC over an X25519 Diffie-Hellman between the device's key and the relay's, bound to the request. It happens beneath your `Dialer`, which receives the proven URL. A relay must be `wss://`, except one on your own machine.

## Example

[`examples/chat.py`](examples/chat.py) is a terminal chat: `python examples/chat.py --url ws://127.0.0.1:7878/oal --pair K7QM-3XRD`, then run it again without `--pair`.

## Development

```sh
cargo build -p oal-conformance -p oal-relay   # the fake host the tests run against, and the relay
uv run pytest
uv run mypy
```

The tests start `oal-conformance client` (the conformance suite's fake host) as a subprocess and fail on any frame it reports as breaking the spec; the relay test puts it behind `oal-relay serve` with `oal-relay host --forward`, then pairs and prompts through the relay. Set `OAL_CONFORMANCE` and `OAL_RELAY` to use binaries somewhere else.

License: Apache-2.0.
