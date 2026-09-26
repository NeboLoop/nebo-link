# Open Agent Link (OAL) 0.1

Status: draft. Version 0.1, 2026-09-26.
Canonical home: https://openagent.link. Schemas: `https://openagent.link/schemas/0.1/`.
Editors: NeboLoop. Reference implementation: Nebo Link (`crates/nebo-link` in https://github.com/NeboLoop/nebo-link).
License of this text: CC-BY-4.0 (`spec/LICENSE`).

## 1. Introduction

The Agent Client Protocol (ACP) defines how a client drives a coding agent: sessions, prompts, streamed updates, tool calls, permission requests, plans, cancellation and modes. ACP runs over stdio. The client starts the agent as a child process on the same machine.

Open Agent Link lets a client drive agents on another computer. The client can be a phone, a desktop app, an IDE panel or another agent. A computer can run several agents. The agents keep their own sign-ins. The computer opens no inbound port.

**OAL is ACP, made reachable, plus a thin host layer.** OAL carries ACP messages unchanged. It adds only:

| Addition | Section |
|---|---|
| Transport binding: ACP JSON-RPC over an authenticated WebSocket, one channel per agent, through a relay or directly on a LAN | 4 |
| Handshake and versioning: `host/hello` | 5, 13 |
| Pairing and device identity: `host/pair`, `host/devices`, `host/unpair` | 6 |
| Host layer: `host/info`, `host/agents`, `host/agent_update` | 7 |
| Host behaviour for ACP methods when many clients share one agent | 8 |
| Turns and usage: `host/turn` | 9 |
| Permission requests across clients: `host/pending`, `host/answer`, `host/pending_update` | 10 |
| Presence and heartbeat: `host/ping` | 11 |
| Resuming after a reconnect | 12 |
| Files by reference | 14 |
| Error model | 15 |
| End-to-end encryption (shape only; required for 1.0) | 17 |

### 1.1 Conventions

The key words MUST, MUST NOT, REQUIRED, SHOULD, SHOULD NOT, MAY and OPTIONAL are used as described in RFC 2119 and RFC 8174 when they appear in capitals.

"ACP" means ACP protocol version 1, schema release `schema-v1.23.0` ([schema](https://raw.githubusercontent.com/agentclientprotocol/agent-client-protocol/schema-v1.23.0/schema/v1/schema.json), [method names](https://raw.githubusercontent.com/agentclientprotocol/agent-client-protocol/schema-v1.23.0/schema/v1/meta.json)). An ACP type written as `acp:ToolCallUpdate` means `#/$defs/ToolCallUpdate` in that schema. A later ACP v1 schema release is compatible with OAL 0.1 as long as it stays ACP protocol version 1.

JSON Schemas for every message OAL adds are in `spec/schemas/`, with `$id`s under `https://openagent.link/schemas/0.1/`. Annotated transcripts are in `spec/examples/`. The conformance suite is `crates/oal-conformance`.

Times are RFC 3339 strings in UTC (`2026-09-26T17:04:05Z`).

### 1.2 Roles

- **Agent.** A program that speaks ACP on stdio (Claude Code through its ACP adapter, Codex through its ACP adapter, Gemini CLI, OpenCode, any custom ACP command), or a runtime the host adapts into ACP (OpenClaw, Hermes; Appendix A).
- **Host.** A process on the agents' computer. It starts and supervises the agents, is the only ACP client of each agent process, and serves OAL to clients. There is one host per computer per OS user. Nebo Link is a host. Nebo is a host for agents on its own computer.
- **Client.** Anything that connects to a host and drives its agents.
- **Relay.** A server that carries WebSocket connections between clients and a host that has dialled out to it. A relay is optional. It can be hosted (NeboAI runs one) or self-hosted.
- **Device.** A client installation that has paired with a host. It has a device id and credentials. Several connections can share one device, for example a phone and its share extension.

### 1.3 What OAL does not change

An ACP agent does not know it is being reached over OAL. On each agent channel a client sends the requests an ACP client sends and receives what an ACP agent sends. The host adds no fields to ACP types. Where OAL needs to say something ACP has no message for, it uses the host channel (section 4.2).

## 2. Overview

```
client ──wss──> relay ──(host's outbound connection)──> host ──stdio (ACP)──> agent "app"   (Claude Code)
client ──wss (LAN, optional)──────────────────────────> host ──stdio (ACP)──> agent "api"   (Codex)
                                                          └──adapter──────> agent "main"  (OpenClaw)
```

A session on a connection, in order:

1. The client opens a WebSocket to the host, through a relay or on the LAN.
2. It pairs once with a one-time code (`host/pair`). Every later connection starts with `host/hello` and the device credential.
3. It lists the agents (`host/agents`).
4. On an agent's channel it speaks ACP: `initialize`, `session/new` or `session/load`, `session/prompt`. Updates stream back as `session/update`. Permission requests arrive as `session/request_permission`.
5. The host tells every client watching the session when a turn starts and ends (`host/turn`), and tells every client about permission requests waiting for an answer (`host/pending_update`). The first answer wins.
6. If the connection drops, the client reconnects and calls `session/load` again. It receives the conversation, the turn still running, and the permission request still waiting.

## 3. Versioning in brief

OAL versions are `MAJOR.MINOR` (section 13). This document is version `0.1`. Clients and hosts advertise the range they support. The host picks the highest version in both ranges. If there is none, it refuses the connection and says which side to update.

## 4. Transport binding

### 4.1 WebSocket

- OAL runs over a WebSocket (RFC 6455). Over the internet the scheme MUST be `wss`. Section 4.5 covers the LAN.
- The client SHOULD offer the subprotocol `oal` in `Sec-WebSocket-Protocol`. A host that receives the offer MUST select `oal`.
- Each WebSocket text message carries exactly one frame: one JSON object encoded as UTF-8. OAL uses no batches and no binary messages. Binary messages are reserved for end-to-end encryption (section 17).
- A host advertises the largest frame it accepts in `host/info` (`maxFrameBytes`). It MUST accept frames of at least 4 MiB. A frame over the limit closes the connection with 1009.

### 4.2 Frames and channels

A connection carries one **host channel** and one **agent channel** per agent.

**Host channel frame:** a JSON-RPC 2.0 message (request, notification, or response) with no `agent` member. Its methods are the `host/*` methods in this document.

```json
{"jsonrpc":"2.0","id":1,"method":"host/agents","params":{}}
```

**Agent channel frame:** an object with exactly two members. `agent` is the agent id. `acp` is one ACP JSON-RPC 2.0 message, exactly as ACP defines it.

```json
{"agent":"app","acp":{"jsonrpc":"2.0","id":3,"method":"session/prompt","params":{"sessionId":"sess_1","prompt":[{"type":"text","text":"Run the tests"}]}}}
```

Rules:

- The schema is `schemas/frame.schema.json`.
- Every (connection, channel) pair has its own JSON-RPC id space in each direction. The host maps ids between a client's channel and the agent process. A client never sees the ids the host uses with the agent process.
- Frames on one channel are delivered in the order they were sent. OAL does not define an order between frames on different channels. For example, a `host/turn` notification can arrive before or after the `session/update` it relates to.
- A frame naming an unknown agent gets the error `unknown_agent` (section 15) if it is a request. Otherwise it is dropped.
- Anything else malformed gets JSON-RPC `-32600` if an id can be read. Otherwise it is dropped.

### 4.3 Connection lifecycle

1. The client opens the WebSocket.
2. Its first frame MUST be a `host/hello` or `host/pair` request on the host channel. On any other frame the host answers `unauthenticated` (if the frame is a request) and closes with 4001. If no first request arrives within 10 seconds, the host closes with 4001.
3. When `host/hello` or `host/pair` succeeds, the connection is authenticated as a device and uses the version returned in `protocol`.
4. Either side may close at any time. The host keeps running turns when a client goes away (section 12).

WebSocket close codes:

| Code | Meaning |
|---|---|
| 1000 | Normal close. |
| 1001 | The host is shutting down or restarting. The client reconnects. |
| 1009 | A frame was over `maxFrameBytes`. |
| 4001 | Not authenticated: the first frame was not `host/hello` or `host/pair`, or it failed. |
| 4002 | No common protocol version. |
| 4003 | This device was unpaired. The client MUST NOT reconnect with the same credentials. |
| 4008 | Heartbeat timeout (section 11). |

### 4.4 Relay path

A host that uses a relay keeps one outbound connection to it. How that connection is authenticated and how client connections are carried inside it is defined by the relay. The reference relay carries each client connection as a yamux stream inside the host's tunnel. To the host, each client connection is a separate OAL connection.

A relay MUST:

- give the client an endpoint for each host. The reference form is `wss://<relay>/oal/hosts/<hostId>`. NeboAI's relay also reaches a host at its tunnel path `/t/<botId>/oal`;
- pass WebSocket messages both ways, in order and unchanged, text and binary;
- close the other side when either side closes, passing the close code on;
- answer an upgrade for a host that is not connected with HTTP 503 and the JSON body `{"code":"host_offline","message":"<host name> is offline."}`;
- work without reading frame contents, so that end-to-end encryption can be added (section 17).

A relay MAY authenticate the client (for example with a bearer token for an account) before the upgrade. It MAY stamp the stream with the identity it verified, so the host can accept `{"type":"relay"}` authentication (section 5).

A relay endpoint for pairing is `wss://<relay>/oal/pair/<code>`. The relay routes it to the host that registered that code (section 6.2).

### 4.5 LAN direct path

A host MAY accept connections on the local network. The owner turns this on; it is off by default. When on:

- The host advertises DNS-SD service type `_oal._tcp` with TXT records `id=<hostId>` and `v=<max protocol version>`.
- It serves `wss://<address>:<port>/oal` with a self-signed certificate. `tlsFingerprint` in `host/info` is the SHA-256 of the certificate's DER encoding, base64url without padding.
- A client connecting on the LAN MUST have learned `tlsFingerprint` from an earlier authenticated connection, and MUST refuse a certificate that does not match it. Pairing on the LAN is therefore done through the relay, or by entering the code together with the fingerprint shown on the host.
- After the upgrade, everything is the same as on the relay path.

## 5. Handshake: `host/hello`

The first request on a connection from a paired device. Schema: `schemas/host-hello.schema.json`.

Params:

| Field | Type | Meaning |
|---|---|---|
| `protocol` | `{min, max}` | OAL versions the client supports. |
| `client` | `{name, version}` | The client software, for logs and the device list. |
| `auth` | object | `{"type":"device","deviceId":…,"token":…}`, or `{"type":"relay"}`. |

`{"type":"relay"}` is valid only on a connection the relay stamped with an identity. The host's owner policy must also accept that identity, for example "the account that owns this host". The device is then that relay identity. Its id is `relay:<identity>`.

Result:

| Field | Type | Meaning |
|---|---|---|
| `protocol` | string | The version the connection uses from now on. |
| `device` | `{id, name}` | The device this connection is authenticated as. |
| `info` | HostInfo | The same object `host/info` returns. |

Errors: `version_mismatch` (then close 4002), `unauthenticated` (then close 4001). The host checks the version before the credentials.

## 6. Pairing

### 6.1 `host/pair`

Pairing turns a one-time code into a device credential. `host/pair` is an alternative first request to `host/hello`. On success the connection is authenticated as the new device. Schema: `schemas/host-pair.schema.json`.

Params: `protocol` and `client` as in `host/hello`, plus:

| Field | Type | Meaning |
|---|---|---|
| `code` | string | The code shown by the host or issued by the relay. Case-insensitive; hyphens and spaces are ignored. |
| `device.name` | string | What the owner calls this device ("Alma's phone"). |
| `device.publicKey` | string | The device's X25519 static public key, 32 bytes, base64url without padding. |

Result: as `host/hello`, except that `device` also carries `token`, the device's secret credential.

The device stores `device.id`, `device.token` and the host's `info.host.publicKey`. In 0.1 the public keys are exchanged and stored, and nothing is encrypted with them yet. They are the static keys the end-to-end handshake uses (section 17), so an 0.1 pairing will not need to be redone.

### 6.2 Codes

- A code is 8 characters from the Crockford base32 alphabet (40 bits). It is shown as `XXXX-XXXX`.
- A host issues codes when the owner asks for one (for example `nebo-link pair`). A relay may issue codes on the host's behalf. A relay-issued code is registered with the relay, so `wss://<relay>/oal/pair/<code>` reaches the right host.
- A code MUST be single-use and MUST expire within 10 minutes.
- A host MUST compare codes in constant time.
- A host MUST allow at most one `host/pair` attempt per connection. A wrong code closes the connection with 4001.
- A host MUST accept no more than 10 failed attempts per minute across all connections. Once a code has had 5 failed attempts, the host MUST stop accepting it.
- On failure the host answers `pairing_refused` with a plain message ("That code didn't work. Get a new one on the computer.").

### 6.3 Credentials

- `token` MUST carry at least 256 bits from a cryptographic random source. The host SHOULD store only a hash of it.
- A device may connect any number of times at once.

### 6.4 `host/devices` and `host/unpair`

`host/devices` lists the paired devices as `{id, name, pairedAt, lastSeenAt, current}`. `current` is true for the device of the calling connection. Schema: `schemas/host-devices.schema.json`.

`host/unpair {deviceId}` revokes a device. A device can always unpair itself. Whether one device can unpair another is host policy; a device that is not allowed gets `not_permitted`. The host closes every connection of the revoked device with 4003 and discards its credential. Schema: `schemas/host-unpair.schema.json`.

## 7. Host layer

### 7.1 `host/info`

Returns HostInfo. Schema: `schemas/host-info.schema.json`; the type is `HostInfo` in `schemas/defs.schema.json`.

| Field | Meaning |
|---|---|
| `host.id` | Stable id of the host. |
| `host.name` | What the owner calls the computer. |
| `host.publicKey` | The host's X25519 static public key (base64url). |
| `host.tlsFingerprint` | Present when LAN direct is on (section 4.5). |
| `software` | `{name, version}` of the host software, for example `{"name":"nebo-link","version":"0.4.0"}`. |
| `protocol` | `{min, max}`: the OAL versions the host supports. |
| `acp.protocolVersion` | The ACP protocol version spoken on agent channels (1). |
| `runtimes` | The runtimes this host can run: `{id, name, kind, version}`. `kind` is `acp` for ACP agents, or the adapter name (`openclaw`, `hermes`). `version` is the runtime's version when known. |
| `maxFrameBytes` | Section 4.1. |
| `attachments` | `{schemes, maxBytes}`: the URI schemes the host fetches for files, and the largest file it accepts (section 14). |

### 7.2 `host/agents`

Returns `{agents: Agent[]}`. Schema: `schemas/host-agents.schema.json`.

Agent:

| Field | Meaning |
|---|---|
| `id` | Stable id, unique on the host. Lowercase letters, digits and hyphens, at most 63 characters. It is the `agent` of the agent channel. |
| `label` | What the owner calls the agent ("app"). |
| `runtime` | A `runtimes[].id` from `host/info` ("claude-code", "codex", "openclaw"). |
| `folder` | Absolute path of the folder its sessions work in, on the host. Absent for runtimes without one. |
| `online` | True while the agent can take a prompt now, or will be started on first use. |
| `offlineReason` | When `online` is false, one plain sentence saying why and what to do ("Claude Code isn't signed in on this computer. Run `claude` once to sign in."). |
| `capabilities` | The agent's `acp:AgentCapabilities`, as its last `initialize` answered. For an adapted runtime, as the adapter provides them. |
| `modes` | The `acp:SessionModeState` a new session starts in, when the agent has modes. |

### 7.3 `host/agent_update` (notification, host to client)

Sent to every authenticated connection when an agent is added, changes (`online`, `offlineReason`, `label`, `modes`, `capabilities`), or is removed. Params: `{change: "added" | "updated" | "removed", agent: Agent}`. For `removed`, `agent` is its last known state. Schema: `schemas/host-agent-update.schema.json`.

## 8. ACP on agent channels

The host is the only ACP client of each agent process. Toward clients it behaves as that agent on the agent channel. Several clients can watch one session. The rules below define what happens when they do.

**Attached.** A connection is *attached* to a session after it has created, loaded or resumed that session on this connection (`session/new`, `session/load`, `session/resume`). It stays attached until the connection closes, or the session is closed or deleted. Clients receive `session/update` and `session/request_permission` only for sessions they are attached to.

**Session record.** For every session open in an agent process, the host keeps a record: every `session/update` it has sent to clients about the session, in order. This includes the updates the agent replayed when the session was opened, and the prompt echoes below. It also keeps the session's current modes and config options, the running turn, and pending permission requests. The host MAY close sessions nobody is attached to that have no running turn (`session/close` to the agent, when the agent supports it) and drop their record.

| ACP method (direction) | Host behaviour |
|---|---|
| `initialize` (client to host) | The first request on each agent channel. The host answers it itself with the agent's `acp:InitializeResponse`: `protocolVersion` per ACP negotiation, the agent's `agentCapabilities` and `agentInfo`, and `authMethods: []`. The client's `clientCapabilities` are not passed to the agent. The host initializes the agent process with `fs.readTextFile`, `fs.writeTextFile` and `terminal` false and no `elicitation`. The agent then uses its own tools on its own computer, and never sends `fs/*`, `terminal/*` or `elicitation/*`. |
| `authenticate`, `logout` | Refused with `not_permitted` ("Sign in to Claude Code on the computer itself."). The agent runs under its owner's own sign-in on the host. |
| `session/new` | Forwarded. `cwd` MUST be the agent's `folder` or inside it, or the host answers `not_permitted`. The host MUST NOT pass `stdio` MCP servers from a client, because they are commands that would run on the host. It MAY pass `http` and `sse` servers, and 0.1 hosts pass none. On success the caller is attached. |
| `session/load` | If the session is open in the agent process, the host answers from its record. It sends the record to the caller as `session/update` notifications. Then, if a turn is running, it sends `host/turn` with `state: "running"`. Then it answers with the session's current `modes` and `configOptions`. Then it sends every pending permission request of the session as a new `session/request_permission` on this channel. If the session is not open, the host forwards the request to the agent (same `cwd` and MCP rules as `session/new`). It delivers the agent's replay to the caller only, and records it. The caller is attached. |
| `session/resume` | As `session/load`, without sending the record. |
| `session/list` | Forwarded. If `cwd` is absent, the host sets it to the agent's `folder`. |
| `session/prompt` | If a turn is running in the session, the host refuses with `turn_in_progress`. Otherwise it assigns a turn id and sends `host/turn` `running` to every attached connection. It records each prompt content block as a `user_message_chunk` update and sends those updates to every other attached connection, unless the agent itself echoes the prompt. Then it forwards the prompt (after resolving files, section 14). The agent's updates go to every attached connection. The agent's `acp:PromptResponse` goes to the caller if it is still connected, and `host/turn` `ended` goes to every attached connection. |
| `session/cancel` | Accepted from any attached connection and forwarded. The host answers the agent's pending permission requests in that session with `{"outcome":"cancelled"}`, as ACP requires of a client, and resolves them (section 10). |
| `session/set_mode`, `session/set_config_option` | Forwarded. On success the host sends the resulting `current_mode_update` or `config_option_update` to every other attached connection, and updates its record. A host MAY refuse, with `not_permitted`, a remote change to a mode that lets the agent act without asking. Owners decide that on the host. |
| `session/close`, `session/delete` | Forwarded. Every connection is detached from the session and the record is dropped. |
| `session/update` (agent to host) | Recorded and sent to every attached connection. |
| `session/request_permission` (agent to host) | Section 10. |
| `$/cancel_request` | From a client, for one of its own requests: the host MAY honour it. The host sends it to clients as described in section 10. |
| Extension methods (`_…`) | The host MAY forward them unchanged, or answer `-32601`. |

A host answers a request on an agent channel whose agent is not running and cannot be started with `agent_unavailable`, including the agent's plain reason. `session/prompt`, `session/set_mode`, `session/set_config_option` and `session/cancel` for a session the connection is not attached to get ACP `-32002` (resource not found); the client loads the session first.

## 9. Turns and usage: `host/turn`

A turn is one `session/prompt`. `host/turn` (notification, host to client) tells every attached connection when a turn starts, when it is still running (on attach), and when it ends. The connection that sent the prompt receives it too. Schema: `schemas/host-turn.schema.json`.

| Field | Meaning |
|---|---|
| `agent`, `sessionId` | The session. |
| `turnId` | Host-assigned, unique on the host. |
| `state` | `running` or `ended`. |
| `startedAt` | When the prompt was forwarded. |
| `by` | `{deviceId, name}` of the device that sent the prompt. |
| `stopReason` | On `ended`, the `acp:StopReason`, when the turn ended with a `PromptResponse`. |
| `error` | On `ended`, `{code, message}` when the turn ended with an error. |
| `usage` | On `ended`, the tokens and cost of this turn, when the agent reports them. |

`usage` has ACP's `Usage` fields and meanings (`inputTokens`, `outputTokens`, `thoughtTokens`, `cachedReadTokens`, `cachedWriteTokens`, `totalTokens`), plus `cost: {amount, currency}`. It always covers this one turn. If an agent reports totals for the whole session, the host subtracts the previous turn's totals. ACP marks its prompt `usage` as unstable. OAL's `usage` is stable, and the host fills it from whatever the agent provides.

## 10. Permission requests

A permission request from an agent reaches every attached connection and the host-wide pending list. The first answer wins, and everyone else sees it resolved.

When the agent sends `session/request_permission`:

1. The host creates a **pending request** with a host-assigned `id`. It holds the agent, session, turn, the agent's `toolCall` and `options`, and `createdAt`.
2. It sends `host/pending_update` `{change: "added", request}` to every authenticated connection, attached or not.
3. It sends the agent's `session/request_permission` params, unchanged, to every attached connection as a new request on that agent channel.

A client that wants to connect the ACP request with the pending request matches on `(agent, sessionId, toolCall.toolCallId)`. A host MUST NOT hold two pending requests for the same tool call.

A request is answered by whichever comes first:

- a response to one of those `session/request_permission` requests on an agent channel;
- `host/answer {id, optionId}` from any authenticated connection. The schema is `schemas/host-answer.schema.json`. An `optionId` that is not one of the request's options gets `-32602`;
- `session/cancel` for the session (outcome `cancelled`);
- the runtime's own interface on the host, for adapted runtimes that have one (Appendix A).

Then the host:

1. answers the agent with the `acp:RequestPermissionOutcome`;
2. sends `$/cancel_request {requestId}` to every other connection that still has the request open. Those clients answer it with error `-32800`, as ACP requires;
3. sends `host/pending_update` `{change: "resolved", request, outcome, answeredBy}` to every authenticated connection. `answeredBy` is `{deviceId, name}`, or null when the answer came from the computer itself;
4. treats any later answer as follows: a late response on an agent channel is ignored; a late `host/answer` gets `already_answered`, and an unknown id gets `unknown_request`.

`host/pending` returns `{requests: PendingRequest[]}` for every agent. A client that was away uses it to show what is waiting (an inbox). Schema: `schemas/host-pending.schema.json`.

A pending request stays until it is answered or its turn ends. A host SHOULD NOT answer on the owner's behalf because time passed. It MAY also tell its relay that a request is waiting, so the relay can send a push notification. That interface is defined by the relay (section 17.4).

## 11. Presence and heartbeat

- **Host presence.** A host is online while its relay connection is up, or while it answers on the LAN. A relay answers a connection to an offline host with 503 `host_offline` (section 4.4). A host shutting down closes client connections with 1001.
- **Agent presence.** `online` and `offlineReason` in `host/agents`, kept current with `host/agent_update`.
- **Heartbeat.** `host/ping` is a request on the host channel with empty params and an empty result. A client MUST send it when it has sent no frame for 20 seconds. It SHOULD treat the connection as lost when a ping gets no answer within 10 seconds. A host MUST close a connection that has sent no frame for 60 seconds, with 4008. Schema: `schemas/host-ping.schema.json`.
- **Reconnecting.** A client that loses its connection reconnects with exponential backoff, starting at 1 second and capped at 30 seconds, with jitter. It SHOULD NOT tell the user about a reconnect that succeeds within 10 seconds.

## 12. Resuming after a reconnect

A running turn does not depend on the connection that started it. When a client disconnects, the host keeps the turn running, keeps recording its updates, and keeps its pending permission requests.

To resume, the client:

1. opens a new connection and sends `host/hello`;
2. sends `initialize` on the agent channel;
3. sends `session/load` for each session it shows.

The host then sends, in this order on that channel: the session record (including the part of the running turn so far), `host/turn` `running` on the host channel if a turn is still running, the `session/load` response, and a `session/request_permission` for each pending request. The client continues from there. It receives the rest of the turn's updates and `host/turn` `ended`. The `PromptResponse` of a prompt sent on a closed connection is not delivered, because `host/turn` `ended` carries the same outcome.

Frames lost while the client was away are covered by the record. OAL 0.1 does not resend individual frames.

## 13. Versioning

- An OAL version is `MAJOR.MINOR`. Before 1.0, a new minor version MAY break compatibility. From 1.0, only a new major version may.
- A client advertises `protocol: {min, max}`. The host selects the highest version in both its own range and the client's. If there is none, the host answers `version_mismatch` and closes with 4002. The error message says plainly which side is out of date: "This app speaks OAL 0.1 and this computer speaks 0.3 to 0.4. Update the app." `data` carries both ranges.
- A deprecated feature keeps working for at least one minor version after the version that deprecates it. The deprecation is recorded in that version's changes.
- ACP inside OAL is versioned by ACP's own `initialize` negotiation. OAL 0.1 requires ACP protocol version 1.
- Unknown fields in any OAL object MUST be ignored. New fields are added only in new minor versions. Changes are proposed as RFCs (`spec/CONTRIBUTING.md`).

## 14. Files by reference

A client sends a file as an ACP `resource_link` content block in `session/prompt`. `uri` is a URL the host can fetch; the block also has `name`, `mimeType` and `size`. ACP requires every agent to accept `resource_link`, so this needs no new message.

```json
{"type":"resource_link","uri":"https://files.example.com/f/8c1e?sig=…","name":"report.pdf","mimeType":"application/pdf","size":482113}
```

- The host fetches every `resource_link` whose scheme is in `attachments.schemes` before forwarding the prompt. It saves the file to a local path the agent can read. It rewrites the block's `uri` to that path as a `file://` URI and leaves the other fields unchanged.
- The host fetches with its own relay credential when the URL belongs to its relay's file service. Otherwise it sends no credentials, so the URL itself must grant access (a signed URL).
- A file over `attachments.maxBytes`, or a fetch that fails, fails the prompt with `attachment_failed` before the agent sees it: "Couldn't get report.pdf. Send it again."
- `file://` links from a client are refused with `not_permitted`.
- Small content MAY be sent inline as ACP `image`, `audio` or `resource` blocks, when the agent's `promptCapabilities` allow it and the frame fits `maxFrameBytes`.

Files the agent produces are not carried in 0.1. The planned design is in section 18.

## 15. Errors

Errors are JSON-RPC 2.0 error objects: `{code, message, data?}`.

- **`message` is shown to people.** It is one plain sentence that says what happened and what to do. It names the agent or computer when that helps. It uses no internal terms, no stack traces and no raw runtime output. Details go in `data`.
- **`code` is stable.** Clients branch on `code`, never on `message`.
- ACP errors from an agent (`-32000` authentication required, `-32002` not found, the JSON-RPC codes, and others) are passed through unchanged on agent channels. A host MAY replace `message` with a plain one ("Claude Code isn't signed in on this computer. Run `claude` once to sign in.") and keep the agent's text in `data.detail`.

OAL codes (schema `schemas/error.schema.json`):

| Code | Name | When | Example message |
|---|---|---|---|
| -33001 | `version_mismatch` | No common version (then close 4002). | "This app speaks OAL 0.1 and this computer speaks 0.3 to 0.4. Update the app." |
| -33002 | `unauthenticated` | First request not `host/hello`/`host/pair`, or bad credentials (then close 4001). | "This device isn't paired with Studio Mac. Pair it again." |
| -33003 | `pairing_refused` | Wrong, expired or used code. | "That code didn't work. Get a new one on the computer." |
| -33004 | `unknown_agent` | The frame names an agent the host doesn't have. | "There's no agent called api on Studio Mac." |
| -33005 | `agent_unavailable` | The agent isn't running and can't be started. | "Codex isn't signed in on this computer. Run `codex` once to sign in." |
| -33006 | `turn_in_progress` | A prompt while a turn is running in the session. | "app is still working on the last message. Wait for it or stop it." |
| -33007 | `already_answered` | `host/answer` for a request already resolved. | "This was already answered on another device." |
| -33008 | `unknown_request` | `host/answer` for an id the host doesn't know. | "That request is no longer waiting." |
| -33009 | `attachment_failed` | A file couldn't be fetched or is too large. | "Couldn't get report.pdf. Send it again." |
| -33010 | `not_permitted` | Host policy refuses the request. | "Sign in to Claude Code on the computer itself." |
| -33011 | `turn_failed` | An adapted runtime failed the turn (Appendix A). | "OpenClaw stopped with an error: the model is unavailable." |

## 16. Security considerations (0.1)

- **Transport.** Each hop uses TLS (`wss`). In 0.1 the relay terminates the client's TLS connection. **A relay can read and alter OAL traffic in 0.1**, including prompts, replies, tool output, file URLs and device tokens. Section 17 removes this, and is required for 1.0. A self-hosted relay or LAN direct avoids a third party in the meantime.
- **No inbound ports.** Hosts dial out to relays. LAN direct is opt-in.
- **Agents run as the local user, under the local user's own sign-in to each agent.** The host never stores an agent's credentials and never runs an agent for another person.
- **Folder and MCP rules** (section 8) keep a remote client from choosing where an agent works, or from starting commands through MCP configuration.
- **Permission modes.** A client can change a session's mode (`session/set_mode`). A host SHOULD let the owner refuse remote changes to modes that let the agent act without asking. It SHOULD label such modes plainly: "Runs anything on this computer without asking."
- **Audit.** A host SHOULD keep a local log of who prompted, which device answered which permission request with which option, mode changes, pairings and unpairings. The log records device ids and never message content.
- **Revocation.** `host/unpair`, or removing the device on the host, closes its connections (4003) and invalidates its token.
- **Pairing codes** are short and single-use. The limits in section 6.2 make guessing a code impractical within its lifetime.

## 17. End-to-end encryption (specified shape; to be implemented in 0.2; required for 1.0)

OAL 1.0 MUST encrypt every connection between client and host, so that a relay forwards ciphertext it cannot read. This section fixes the shape so that 0.1 pairings and implementations carry over. The details marked **open** are settled by RFC before 0.2.

### 17.1 Keys

Each device and each host has an X25519 static key pair. Public keys are exchanged at pairing (section 6.1) and stored by both sides. They rotate by pairing again (an RFC defines in-band rotation).

### 17.2 Handshake

- Pattern `Noise_IK_25519_ChaChaPoly_BLAKE2s`. The client is the initiator and already knows the host's static key.
- Prologue: the UTF-8 bytes of `OAL-E2E/1 ` followed by the host id.
- After the WebSocket upgrade, message 1 (client to host, one binary WebSocket message) is `-> e, es, s, ss`. Its payload is the JSON `{"protocol":{"min":…,"max":…},"client":{…}}`.
- Message 2 (host to client, one binary message) is `<- e, ee, se`. Its payload is the JSON `{"protocol":"<selected>","device":{"id":…}}`.
- The host authenticates the device by its static key, which must belong to a paired device. `host/hello` is not sent. The token is not used on encrypted connections.
- A host that requires encryption closes a connection whose first message is text, with 4001.

### 17.3 Framing after the handshake

Every OAL frame is encrypted as one or more Noise transport messages, each in its own binary WebSocket message. A Noise message is at most 65535 bytes. The plaintext of each message starts with one byte: `0x01` means more parts follow, `0x00` means this is the last part of the frame. The receiver joins the parts and parses the frame. Each side rekeys (Noise `Rekey`) after 2^20 messages. A new connection always performs a new handshake.

### 17.4 What the relay still sees

With encryption on, the relay still sees: which host each connection goes to (the host id in the path), the client's IP address, when connections open and close, and the size and timing of messages. With relay-issued identity, it also sees the account. It sees no agent ids, session ids, prompts, replies, tool calls, permission requests, or file names inside frames. A host that asks its relay to send a push notification sends a content-free notice ("An agent on Studio Mac needs your answer"), unless the owner chooses otherwise.

### 17.5 Open for 0.2

- **Pairing over an untrusted relay (open).** A 40-bit code that the relay routes must not let the relay insert its own keys. Pairing will bind the exchanged keys to the code with a PAKE (CPace or SPAKE2), or the client will confirm a short fingerprint of the host key on the host's screen. The choice is settled by RFC.
- **Files (open).** Files uploaded to a relay's file service are readable by that service. 0.2 will encrypt files on the client with a per-file key, and send the key inside the encrypted prompt.
- **Relay identity with encryption (open).** Devices that authenticate only through the relay need a way to register a static key with the host.

## 18. Deferred to 0.2

- End-to-end encryption and PAKE pairing (section 17).
- Files produced by agents: a host method that publishes a file inside the agent's folder to a URL the client can fetch.
- Sending to a running turn (steering, or queueing a message for after the turn), instead of `turn_in_progress`.
- Replay from a cursor (`session/load` since the last update seen), so a reconnect doesn't resend the whole record.
- Owner-level agent management over OAL: adding and removing agents, and setting an agent's default mode.
- ACP `elicitation` and client-side `fs`/`terminal` over OAL.
- A defined interface between relay and host for push notifications.
- ACP v2. Its current alphas remove `session/load` and `session/set_mode`. OAL will follow ACP v2 when it is released, with a deprecation window per section 13.

## Appendix A. Mapping non-ACP runtimes

A host adapts runtimes that don't speak ACP, so that every client sees ACP on the agent channel. The mappings below are the ones Nebo Link implements for OpenClaw (gateway protocol 4, 2026.9.6) and Hermes (API server, 0.19.0 and later). "Synthesized" means the host creates the value.

### A.1 Host layer

| OAL | OpenClaw | Hermes |
|---|---|---|
| `host/agents` entries | `agents.list` (one OAL agent per OpenClaw agent) | the default profile and each profile under `profiles/` |
| `Agent.capabilities` | synthesized: `loadSession: true`, `sessionCapabilities.list: {}` | the same; `resume` only on versions whose runs load the session |
| `Agent.modes` | absent (no modes) | absent |
| `Agent.online` | the gateway answers the operator connection | the API server answers `/v1/capabilities` with the flags the host needs |

### A.2 Session methods

| ACP | OpenClaw | Hermes |
|---|---|---|
| `session/new` | a new session key under the agent (`agent:<id>:<key>`), created by its first `chat.send` | `POST /api/sessions` |
| `session/list` | `sessions.list` filtered to the agent | `GET /api/sessions` |
| `session/load` (replay) | `chat.history`, replayed as updates | `GET /api/sessions/{id}/messages`, replayed as updates |
| `session/prompt` | `chat.send` with one message and an idempotency key | `POST /v1/runs {input, session_id}`. On 0.19.0 the host uses the session-native stream, because 0.19.0 runs start with an empty history. |
| `session/cancel` | `chat.abort` | `POST /v1/runs/{id}/stop` |
| permission answer | `approval.resolve {id, kind, decision}` | `POST /v1/runs/{id}/approval {choice, request_id?}` |

### A.3 Updates

| ACP `session/update` / result | OpenClaw event | Hermes event |
|---|---|---|
| `agent_message_chunk` | `chat` `state:"delta"`, `deltaText` | `message.delta {delta}` |
| `agent_thought_chunk` | `agent` `stream:"thinking"` | `reasoning.available {text}` |
| `tool_call` (`status: in_progress`) | `agent` `stream:"tool"`, `phase:"start"`, `name`, `toolCallId`, `args` → `title`, `toolCallId`, `rawInput` | `tool.started {tool, preview}` → `title: tool`, `rawInput: {preview}`, `toolCallId` synthesized per run |
| `tool_call_update` (`completed` / `failed`) | `phase:"result"` → `rawOutput`, `status` | `tool.completed {tool, duration, error, preview}` → `status`, `content` (previews only, up to 500 characters) |
| `session/request_permission` | `exec.approval.requested` / `plugin.approval.requested` | `approval.request {request_id?, command, description, choices}` |
| `session_info_update` (title) | the session's title when it changes | the session's title when it changes |
| `PromptResponse` `end_turn` | `chat` `state:"final"`, `stopReason` | `run.completed` |
| `PromptResponse` `cancelled` | `chat` `state:"aborted"` | `run.cancelled` |
| error `turn_failed` | `chat` `state:"error"` | `run.failed {error}` |
| `host/turn` `usage` | the `usage` stream and the assistant message's usage | `run.completed.usage` (per run) |

Permission options (`acp:PermissionOption`, `optionId` = the runtime's own value):

| Runtime choice | `kind` | `name` |
|---|---|---|
| OpenClaw `allow-once` | `allow_once` | Allow once |
| OpenClaw `allow-always` | `allow_always` | Always allow |
| OpenClaw `deny` | `reject_once` | Deny |
| Hermes `once` | `allow_once` | Allow once |
| Hermes `session` | `allow_always` | Allow for this conversation |
| Hermes `always` | `allow_always` | Always allow |
| Hermes `deny` | `reject_once` | Deny |

The host offers exactly the choices the event carries. In its default mode, Hermes offers only `once` and `deny`. A permission request answered in the runtime's own UI resolves the pending request with `answeredBy: null`.

## Appendix B. Differences from the Nebo phone contract

Nebo Link first served Nebo's own phone API (the "chat contract": `/api/v1/agents`, `/api/v1/chats/…` and a `/ws` frame set), so that the Nebo phone app could reach a linked agent unchanged. OAL replaces it. The old contract stays in Nebo Link as a compatibility shim while Nebo's clients move to OAL, and is then removed.

| Phone contract | OAL |
|---|---|
| HTTP endpoints plus a WebSocket, frames `{type, data}` | One WebSocket. JSON-RPC on the host channel, ACP on agent channels. |
| Auth: the bearer the relay tunnel checked (`x-nebo-tunnel-auth`), then `auth {token}` → `auth_ok` | `host/pair` (code → device credential), then `host/hello`. The relay-verified identity is still accepted as `{"type":"relay"}`. |
| Capability probe `GET /health` → `{version, runtime, chat}` | `host/hello` / `host/info`: protocol range, software version, runtimes. |
| `GET /api/v1/agents` → `{agents, primaryChristened}`; the default agent exposed as `id:"assistant"` | `host/agents`. Ids are the host's own; there is no "primary" agent and no `assistant` alias. |
| Nebo employee fields (`displayName`, `color`, `handle`, `isEnabled`, `editable`, `isApp`, `nameLocked`) | `label`, `runtime`, `folder`, `online`, `capabilities`, `modes`. Presentation is the client's business. |
| `GET /agents/{id}/chats`, `POST /agents/{id}/chats` | ACP `session/list`, `session/new` |
| `session_id` = `agent:<agentId>:thread:<chatId>` | ACP `sessionId`, the agent's own id, scoped by the channel's agent. |
| `GET /chats/{id}/messages` → `{messages, hasMore, activeRun, pendingAsk}` | ACP `session/load` (replay), `host/turn` `running`, and the re-sent `session/request_permission`. |
| WS `chat {prompt, agent_id, session_id?, attachments?}`, with `chat_created` when no session | ACP `session/prompt` after `session/new`. |
| `chat_stream`, `thinking`, `tool_start`, `tool_result` | ACP `session/update` (`agent_message_chunk`, `agent_thought_chunk`, `tool_call`, `tool_call_update`, `plan`). |
| `chat_complete`, `chat_cancelled`, `chat_error` | ACP `PromptResponse.stopReason` or error, and `host/turn` `ended`. |
| `ask_request {request_id, prompt, widgets}` / `ask_response {request_id, value}` | ACP `session/request_permission` / its response, or `host/answer`. |
| Notifications `/api/v1/notifications`, items `approval:<id>` | `host/pending`, `host/pending_update`. |
| `usage` absent from the phone; tokens only inside Nebo | `host/turn` `usage` per turn. |
| `cancel {session_id}` | ACP `session/cancel`. |
| Attachments `{fileId, url, filename, mimeType, size}` | ACP `resource_link` (section 14). |
| Model shown via `GET /chats/{id}`; `PUT` refused | ACP `configOptions` / `config_option_update`; changes with `session/set_config_option` where the agent allows it. |
| `ping` → `pong` | `host/ping`. |
| `agent_updated`, `agent_installed`, `agent_uninstalled` | `host/agent_update`. |
| 404 `{"error":"not on a linked bot"}` for unsupported routes | Not applicable: the protocol has no Nebo-only routes. |
| Only one client shape (the Nebo phone) | Any client. Several clients can watch one session. The first answer to a permission request wins. |

## Appendix C. Message index

Host channel, client to host (requests): `host/hello`, `host/pair`, `host/info`, `host/agents`, `host/pending`, `host/answer`, `host/ping`, `host/devices`, `host/unpair`.

Host channel, host to client (notifications): `host/agent_update`, `host/turn`, `host/pending_update`.

Agent channels: ACP v1, unchanged. The host serves `initialize`, `session/new`, `session/load`, `session/resume`, `session/list`, `session/prompt`, `session/cancel`, `session/set_mode`, `session/set_config_option`, `session/close` and `session/delete`, and refuses `authenticate` and `logout`. It sends `session/update`, `session/request_permission` and `$/cancel_request`.
