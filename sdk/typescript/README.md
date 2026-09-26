# @openagentlink/client

The TypeScript client for [Open Agent Link](https://openagent.link) (OAL). Drive the agents on another computer (Claude Code, Codex, Gemini CLI, OpenCode, OpenClaw, Hermes, or any agent that speaks [ACP](https://agentclientprotocol.com)) from your own app: sessions, streaming, permission prompts, modes, and resuming after a dropped connection.

OAL is ACP made reachable, plus a thin host layer. The spec is [`spec/oal-0.1.md`](../../spec/oal-0.1.md).

```sh
pnpm add @openagentlink/client
```

Runs in browsers and in Node 22 or later (it uses the platform `WebSocket` and WebCrypto). No runtime dependencies.

## Quickstart

Pair once with the code the computer shows (`nebo-link pair`), then ask Claude Code to run the tests and approve from your code:

```ts
import { connect, pair } from '@openagentlink/client';

const relay = 'wss://relay.example.com';
const identity = await pair({ relay, code: 'K7QM-3XRD', deviceName: 'CI bot' }); // store it: it's plain JSON

const client = await connect({ relay, credentials: identity });
const agents = await client.hosts()[0].agents();
const claude = agents.find((a) => a.runtime === 'claude-code')!;
const session = await claude.session();

for await (const event of session.prompt('Run the tests and fix what fails')) {
  if (event.type === 'text') process.stdout.write(event.text);
  if (event.type === 'permission') await event.request.allowOnce();
  if (event.type === 'done') console.log(`\n${event.stopReason}`);
}
client.close();
```

`url: 'wss://...'` instead of `relay` connects to one host directly (on the LAN, or the conformance suite's fake host).

## The API

| Call | What it does |
|---|---|
| `pair({ relay \| url, code, deviceName })` | Turns a one-time code into an `Identity` (device id, token, X25519 key pair). It is plain JSON; keep it secret. The format is the same as the Python SDK's `Identity.save()`. |
| `connect({ relay \| url, credentials })` | Connects to one host, or several (an array of identities, through a relay). Offline hosts keep retrying in the background. |
| `client.hosts()` | The hosts: `id`, `name`, `online`, `info`, `device`. |
| `host.agents()` | The agents: `id`, `label`, `runtime`, `folder`, `online`, `offlineReason`, `modes`, `capabilities`. |
| `host.notifications()` | Permission requests waiting on any agent (the inbox), each answerable. |
| `host.updates()` | `for await` presence, agent changes and permission requests as they happen. |
| `host.devices()`, `host.unpair(deviceId?)` | Paired devices; revoke one (by default this one). |
| `agent.session()` / `agent.session(id)` | A new session, or an existing one loaded: `session.history`, and `session.turn` if a turn is running (with any permission request still waiting). |
| `agent.sessions()` | The agent's sessions (ACP `session/list`). |
| `session.prompt(text, attachments?)` | Sends a prompt; returns a `Turn` to `for await` over. Attachments are `{ url, name, mimeType?, size? }`: URLs the host fetches. |
| `session.cancel()` | Stops the running turn; it ends with `done` and `stopReason: 'cancelled'`. |
| `session.setMode(modeId)` | Switches mode (`session.modes.availableModes`), e.g. Claude Code's `default` / `acceptEdits` / `bypassPermissions`. |
| `client.close()` | Closes every connection. |

A turn streams, in order, events with a `type`: `text`, `thinking`, `user` (another device's prompt), `tool_start`, `tool_update`, `tool_result` (`output` is its text), `permission`, `permission_resolved`, `plan`, `mode`, `usage`, then `done` (`stopReason`) or `error` (`error`). Unknown ACP updates arrive as `update` so nothing is lost.

`permission` events carry a `request` with `allowOnce()`, `allowAlways()`, `deny()` and `choose(optionId)`. The first answer from any device wins; answering after someone else did rejects with code `already_answered`.

### Resuming

A dropped connection reconnects on its own (backoff from 1 to 30 seconds). Sessions are loaded again; what you already saw is not repeated, the running turn keeps streaming into the same `Turn`, and a permission request still waiting stays answerable through the same object. A turn that finished while you were away ends with `done` and `stopReason: null`.

### Errors

Every error is an `OALError` with a plain one-sentence `message` and a stable `code` to branch on: `pairing_refused`, `unauthenticated`, `unpaired`, `version_mismatch`, `unknown_agent`, `agent_unavailable`, `turn_in_progress`, `already_answered`, `unknown_request`, `attachment_failed`, `not_permitted`, `turn_failed`, `host_offline`, `connection_lost`, `not_offered`, `not_found`, `invalid_params`, `closed`, `agent_error`. `rpcCode` and `data` carry the JSON-RPC details.

### Transport and encryption

`connect({ ..., dialer, secure })` takes the two transport layers. A `Dialer` opens a `Socket` (WebSocket messages); a `SecureChannel` turns it into a `FrameChannel` (whole OAL frames). The default, `plaintext`, is OAL 0.1: one JSON text message per frame, authenticated with `host/hello`. End-to-end encryption (spec section 17, Noise IK over X25519) plugs in as another `SecureChannel`: it gets the host's and this device's static keys in `ChannelContext`, does its handshake in `open`, and sets `FrameChannel.authenticated` so the client skips `host/hello`. Nothing else in your code changes.

## Examples

- [`examples/chat.ts`](examples/chat.ts): a terminal chat. `pnpm example:chat --url ws://127.0.0.1:7878/oal --pair K7QM-3XRD`, then again without `--pair`.
- [`examples/web/`](examples/web/): a one-page web client (pair, pick an agent, chat, answer permission cards). `pnpm example:web`.

To try either without a real host, run the conformance suite's fake host: `cargo run -p oal-conformance -- client --listen 127.0.0.1:7878` (code `K7QM-3XRD`; `run: echo hi` asks permission, `wait` runs until stopped).

## Development

```sh
cargo build -p oal-conformance      # the fake host the tests run against
pnpm install
pnpm build && pnpm check && pnpm test
```

The tests start `oal-conformance client` as a subprocess and fail on any frame it reports as breaking the spec. Set `OAL_CONFORMANCE` to use a binary somewhere else.

License: Apache-2.0.
