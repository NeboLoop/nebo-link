# Examples

Each file is one recorded conversation between a client and a host, annotated step by step. The conformance suite (`crates/oal-conformance`) plays the client's side against a host and checks the host's side, so these files are both documentation and tests.

| File | Shows |
|---|---|
| `pair.json` | Pairing with a one-time code; heartbeat; the device list. |
| `agents.json` | `host/hello` on a later connection; listing agents; the pending list. |
| `add-remove.json` | Adding an agent of an addable runtime (it gets a folder of its own) and removing it (its folder stays). |
| `prompt-permission.json` | A prompt that runs a command: stream, tool call, permission request, approval, result, reply, end of turn with usage. |
| `reconnect.json` | The connection drops while the agent waits for permission; the client reconnects, loads the session, and answers the same request. |
| `turn-ended-while-away.json` | The phone leaves mid-turn; another connection answers from the pending list with `host/answer` and the turn ends; the phone's `session/load` returns the record and the turn's `ended` notice. |
| `cancel.json` | Cancelling a running turn. |
| `mode.json` | Switching a session to full access; the next command runs without asking. |
| `first-answer-wins.json` | Two clients on one session; one answers, the other's request is withdrawn; a late answer is refused. |
| `version-mismatch.json` | A client with no common version is refused plainly and disconnected. |

## Format

```json
{
  "title": "…",
  "description": "…",
  "keeps": ["deviceId", "token"],
  "steps": [
    {"note": "What this step shows.", "conn": "phone", "send": {"jsonrpc": "2.0", "id": 1, "method": "host/ping", "params": {}}},
    {"note": "…", "conn": "phone", "expect": {"jsonrpc": "2.0", "id": 1, "result": {}}}
  ]
}
```

- `conn` names the connection (default `client`). Steps: `connect` (`"host"` or `"pair"`), `send`, `expect`, `close`, `expectClose` (a close code).
- `"ordered": true` on an `expect` means the frame must have arrived after the frame the previous `expect` on that connection matched. The examples use it for the one order the spec guarantees across channels: `host/turn` `ended` after the turn's last update and after the prompt's answer.
- An `expect` lists what must be there; the host may send more members, and more array elements, than the example shows.
- `"{{name}}"` captures the value the host sent the first time and must equal it afterwards; in a `send` it is replaced by the captured value. `"{{*}}"` matches anything. `{{code}}`, `{{agent}}` and `{{runtime}}` are given on the command line.
- Captures belong to one example, except the names in `keeps`, which later examples use (`pair` keeps the device credential, `agents` the agent's folder).
- Frames on one agent channel arrive in the order shown. Host-channel notifications may arrive in any order relative to each other and to agent channels.

The scripted agent the examples expect (`oal-conformance agent`) answers `run: <command>` with a tool call and a permission request (options `allow-once`, `reject-once`), `wait` with a turn that runs until cancelled, `work in <folder>` by calling the host's `move_to_folder` tool (section 8.1), `where` with the folder it works in, and anything else with an echo. Its modes are `ask`, `folder` and `full`; every turn reports 12 input and 5 output tokens. A host under test also has it installed as an addable runtime, named with `--runtime` (`add-remove.json`): installed the way the host detects its runtimes. Nebo Link detects coding agents by their programs on `PATH`, so its proof installs the scripted agent as `opencode`.
