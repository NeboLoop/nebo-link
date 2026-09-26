# Nebo Link

Link the agents you already run on a computer, OpenClaw, Hermes and your coding agents (Claude Code, Codex, Gemini CLI, OpenCode, or any agent that speaks ACP), to [NeboAI](https://neboai.com), then reach and manage them from the NeboAI phone app and the web. A computer is one bot; each agent on it is its own employee. You don't open ports, set up a VPN or configure a tunnel.

Status: early development.

## How it works

`nebo-link` runs next to your agent and makes two outbound connections to NeboAI. One reports that your agent is online. The other is a tunnel that carries your requests from the app to your agent's own local UI. Nothing on your machine listens publicly. Your agent's tokens and passwords stay on your machine, and your NeboAI sign-in sits in front of everything.

## Usage

Get a code in the NeboAI app (**Connect OpenClaw or Hermes**), then run on the machine where your agent runs:

```sh
nebo-link ABCD-1234
```

This pairs the computer with your NeboAI account as one bot, with its first agent: it finds OpenClaw (`~/.openclaw`) or Hermes (`~/.hermes`), lets it open behind NeboAI, and installs `nebo-link` as a background service that starts with your computer. If both are installed, pick the first with `--runtime openclaw` or `--runtime hermes` and add the other with `nebo-link add`. `--name` sets the bot's name (default: your computer's name); `--label` the agent's.

Every other agent on the computer joins the same bot, with no new code and no new service:

```sh
nebo-link add hermes
nebo-link add claude-code --dir ~/code/site
nebo-link add claude-code --dir ~/code/api --label "Claude Code · api"
nebo-link add codex --dir ~/code/api
nebo-link remove claude-code-api
```

Each agent is its own employee in the NeboAI app (**Hire from another app**), named by its label ("Claude Code", "Claude Code · api", default: the agent's name, with its folder's when the bot already has one of it), with its own conversations. Two of the same agent in different folders share nothing: not a process, a conversation, a question or a folder. A coding agent joins or leaves the running bot at once; adding or removing OpenClaw or Hermes restarts the service. Opening the bot in the app shows its first OpenClaw or Hermes install's own UI.

| Command | What it does |
|---|---|
| `nebo-link` or `nebo-link status` | Shows what is linked, the agents it hosts (with their ids), and whether it is online. |
| `nebo-link add <agent> [--dir <folder>] [--label <name>]` | Adds an agent to this computer's bot: `openclaw`, `hermes`, `claude-code`, `codex`, `gemini`, `opencode`, or any ACP agent with `--acp-command`. |
| `nebo-link remove <agent id>` | Removes an agent from the bot. OpenClaw and Hermes get their config back. |
| `nebo-link models on\|off [--bot <id>]` | Points your agent's models at NeboAI (no API keys to paste), or restores the provider it had. |
| `nebo-link logs [--bot <id>] [--lines N]` | Prints the service's recent log. |
| `nebo-link unlink [--bot <id>]` | Restores your agent's config exactly as it was, stops what the link started, removes the service and forgets the bot. |
| `nebo-link update` | Updates nebo-link to the latest signed release and restarts your linked bots. The service also updates itself daily. |
| `nebo-link run --bot <id>` | Runs one bot's connection in the foreground; this is what the service runs. |

`--bot` is needed only when more than one bot is linked here (links made before a computer was one bot).

### Coding agents (ACP)

Claude Code, Codex, Gemini CLI and OpenCode are linked by name, and any other agent that speaks the [Agent Client Protocol](https://agentclientprotocol.com) by the command that starts it in ACP mode, first with the code or later with `add`:

```sh
nebo-link ABCD-1234 --runtime claude-code --dir ~/code/my-project
nebo-link add codex
nebo-link add --acp-command "goose acp"
```

`--dir` is the project folder its conversations work in (default: `~/NeboAI/<agent>`, made if missing). The link starts the agent in ACP mode (`gemini --acp`, `opencode acp`; for Claude Code and Codex their ACP adapters, `@agentclientprotocol/claude-agent-acp` and `@agentclientprotocol/codex-acp`, run with `npx` unless installed) and keeps it running. It runs on the agent's own sign-in on this computer: NeboAI never sees those credentials, nothing in the agent's settings is changed, and an agent that isn't signed in says so in the chat ("Claude Code isn't signed in on this computer. Run `claude` once to sign in."). A coding agent has no web page of its own; it is one employee in the NeboAI app, a conversation is one of its sessions (conversations started in the terminal in that folder show too), and its permission prompts arrive as cards in the chat and items in your inbox. How much it may do without asking is the employee's permission mode in NeboAI (Ask, Automatic, Plan, Full access), which the link sets as the agent's own mode for each conversation (Claude Code `default` / `acceptEdits` / `plan` / `bypassPermissions`; Codex `read-only` / `agent` / `agent-full-access`). An agent is started when it is first asked for. `nebo-link models` doesn't apply to it.

Your agent can also do this for you: the [Connect to NeboAI](skills/connect-to-neboai/SKILL.md) skill teaches OpenClaw and Hermes to install Nebo Link with your code and confirm it's connected.

Removing the bot in the NeboAI app unlinks it here too: the service restores your agent's config, forgets the bot, removes itself, and `nebo-link status` tells you it was removed.

## Chat from the phone

A linked agent gets a native chat in the NeboAI app: its agents (OpenClaw agents, Hermes profiles) are listed as employees, a conversation is one of the agent's own sessions, replies stream with their tool cards, and when the agent stops to ask about a command the question reaches the phone as a card in the chat and an item in your inbox. Answer it there or in the agent's own UI; both see the same answer. For Hermes the link turns on its local API server with a key of its own (`API_SERVER_KEY` in the profile's `.env`, loopback only, recorded like every other change); for OpenClaw the link opens its own operator connection to the gateway on loopback, as the trusted proxy it already is, with a device key kept in the bot's directory. `chat` is announced to NeboAI only while the agent answers the link; `nebo-link status` shows `chat: on`, or why it is off.

### What it keeps running

The chat and the UI need the agent's own processes up: Hermes' gateway (which hosts its API server) and its dashboard, OpenClaw's gateway. The link keeps them running whenever the machine is up. A process that is not answering is started with the agent's own service where it has one (`hermes gateway install`, `openclaw gateway install`; the link records that it installed it), or otherwise in the foreground, detached, with its output in the bot's directory (`logs/<agent id>-<process>.log`), and started again when it ends. A service you installed yourself is yours: the link leaves it alone and says so. `nebo-link status` shows each process, and the chat is announced to NeboAI as soon as the agent answers. `unlink` stops the processes the link started and removes the services the link installed, never one you had.

### What it changes

Every change to your agent's config is recorded with the value it replaced, so turning models off and `unlink` put it back exactly. For OpenClaw, linking sets the Control UI's base path and trusted-proxy sign-in for requests that come through NeboAI, and sets a local password (`gateway.auth.password`) so your own `openclaw` commands keep working. For Hermes, linking writes `API_SERVER_KEY` into each profile's `.env` for the chat above; the dashboard itself needs no config change. Where a change needs a restart, the link runs the agent's own restart command.

### Where things are

Each linked bot has a directory in your data directory (`~/Library/Application Support/nebo-link/<bot id>` on macOS, `~/.local/share/nebo-link/<bot id>` on Linux, `%APPDATA%\nebo-link\<bot id>` on Windows; `--home` or `NEBO_LINK_HOME` moves it), with each agent's own files in `agents/<agent id>/`. The bot token is kept in an owner-only `token` file in that directory. Logs rotate daily in `logs/` (each agent's own output in `logs/<agent id>.log`) and never contain tokens, headers or message content.

The service is a launchd agent on macOS, a systemd user unit on Linux (a system unit when run as root), and a logon task on Windows.

## Open Agent Link

Nebo Link is the reference implementation of [Open Agent Link](https://openagent.link) (OAL), an open protocol for reaching agents on any computer: ACP, made reachable, plus a thin host layer. The specification, JSON Schemas, examples and RFC process are in [`spec/`](spec/); the conformance suite is [`crates/oal-conformance`](crates/oal-conformance).

## License

Apache-2.0 (see `LICENSE` and `NOTICE`). The specification in `spec/` is CC-BY-4.0. Contributions are signed off under the DCO; see `CONTRIBUTING.md`.
