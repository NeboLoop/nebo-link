# Open Agent Link (OAL)

Open Agent Link lets any client reach and drive agents on any computer: from a phone, a desktop app, an IDE panel or another agent, through a relay or on the local network, without opening a port.

**OAL is ACP, made reachable, plus a thin host layer.** It carries the [Agent Client Protocol](https://agentclientprotocol.com) unchanged, over an authenticated WebSocket, one channel per agent. It adds what ACP needs once the client is not on the agent's machine: pairing, a host that lists its agents, presence, resuming after a dropped connection, permission requests that reach every device (the first answer wins), files by reference, per-turn usage, plain errors and versioning.

Canonical home: **https://openagent.link**. Reference implementation: Nebo Link (this repository).

## Contents

| Path | What it is |
|---|---|
| [`oal-0.1.md`](oal-0.1.md) | The specification, version 0.1 (draft). |
| [`schemas/`](schemas/) | JSON Schemas for every message OAL adds (`https://openagent.link/schemas/0.1/…`). ACP types are referenced from ACP's own schema, not copied. |
| [`examples/`](examples/) | Annotated transcripts: pairing, listing agents, a prompt with a permission request, reconnecting mid-turn, cancelling, changing mode, two clients answering one request, and a version mismatch. The conformance suite runs them. |
| [`rfcs/`](rfcs/) | Proposals for changes. Start from [`0000-template.md`](rfcs/0000-template.md). |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | How the protocol changes, versioning, and the deprecation window. |
| [`LICENSE`](LICENSE) | CC-BY-4.0. |

## What 0.1 adds to ACP

Host channel methods: `host/hello`, `host/pair`, `host/info`, `host/agents`, `host/pending`, `host/answer`, `host/ping`, `host/devices`, `host/unpair`. Notifications: `host/agent_update`, `host/turn`, `host/pending_update`. Plus the frame envelope `{"agent": "<id>", "acp": <ACP message>}`, the rules a host follows when several clients share one agent, stable error codes (-33001 to -33011) and close codes (4001, 4002, 4003, 4008).

End-to-end encryption between client and host (Noise over the WebSocket) is specified in outline in section 17, will be implemented in 0.2, and is required for 1.0. Until then a relay can read the traffic it carries.

## Conformance

The suite is the crate [`crates/oal-conformance`](../crates/oal-conformance). Passing it is what "compatible" means.

```sh
cargo build --release -p oal-conformance

# Test a host. Add the suite's scripted agent to the host as an ACP agent
# (it runs `oal-conformance agent`), get a pairing code from the host, then:
oal-conformance host wss://<relay>/oal/hosts/<host id> --code ABCD-1234 --agent <agent id> \
    [--pair-url wss://<relay>/oal/pair/ABCD-1234] [--header "Authorization: Bearer …"]

# Test a client. This serves a fake host with one fake agent; point the client
# at it, pair with the code, and watch for spec violations in the output.
oal-conformance client --listen 127.0.0.1:7878 --code K7QM-3XRD

oal-conformance examples    # list the examples
```
