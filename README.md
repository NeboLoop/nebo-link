# Nebo Link

Link the OpenClaw or Hermes agent you already run to [NeboAI](https://neboai.com), then reach and manage it from the NeboAI phone app and the web. You don't open ports, set up a VPN or configure a tunnel.

Status: early development.

## How it works

`nebo-link` runs next to your agent and makes two outbound connections to NeboAI. One reports that your agent is online. The other is a tunnel that carries your requests from the app to your agent's own local UI. Nothing on your machine listens publicly. Your agent's tokens and passwords stay on your machine, and your NeboAI sign-in sits in front of everything.

## Usage

Get a code in the NeboAI app (**Connect OpenClaw or Hermes**), then run on the machine where your agent runs:

```sh
nebo-link ABCD-1234
```

This finds OpenClaw (`~/.openclaw`) or Hermes (`~/.hermes`), links it to your NeboAI account, lets it open behind NeboAI, and installs `nebo-link` as a background service that starts with your computer. If both are installed, link one per run with `--runtime openclaw` or `--runtime hermes`; each linked agent is its own bot. `--name` sets the bot's name (default: your computer's name and the agent).

| Command | What it does |
|---|---|
| `nebo-link` or `nebo-link status` | Shows what is linked and whether it is online. |
| `nebo-link models on\|off [--bot <id>]` | Points your agent's models at NeboAI (no API keys to paste), or restores the provider it had. |
| `nebo-link logs [--bot <id>] [--lines N]` | Prints the service's recent log. |
| `nebo-link unlink [--bot <id>]` | Restores your agent's config exactly as it was, stops what the link started, removes the service and forgets the bot. |
| `nebo-link update` | Updates nebo-link to the latest signed release and restarts your linked bots. The service also updates itself daily. |
| `nebo-link run --bot <id>` | Runs one bot's connection in the foreground; this is what the service runs. |

`--bot` is needed only when more than one agent is linked.

Your agent can also do this for you: the [Connect to NeboAI](skills/connect-to-neboai/SKILL.md) skill teaches OpenClaw and Hermes to install Nebo Link with your code and confirm it's connected.

Removing the bot in the NeboAI app unlinks it here too: the service restores your agent's config, forgets the bot, removes itself, and `nebo-link status` tells you it was removed.

## Chat from the phone

A linked agent gets a native chat in the NeboAI app: its agents (OpenClaw agents, Hermes profiles) are listed as employees, a conversation is one of the agent's own sessions, replies stream with their tool cards, and when the agent stops to ask about a command the question reaches the phone as a card in the chat and an item in your inbox. Answer it there or in the agent's own UI; both see the same answer. For Hermes the link turns on its local API server with a key of its own (`API_SERVER_KEY` in the profile's `.env`, loopback only, recorded like every other change); for OpenClaw the link opens its own operator connection to the gateway on loopback, as the trusted proxy it already is, with a device key kept in the bot's directory. `chat` is announced to NeboAI only while the agent answers the link; `nebo-link status` shows `chat: on`, or why it is off.

### What it keeps running

The chat and the UI need the agent's own processes up: Hermes' gateway (which hosts its API server) and its dashboard, OpenClaw's gateway. The link keeps them running whenever the machine is up. A process that is not answering is started with the agent's own service where it has one (`hermes gateway install`, `openclaw gateway install`; the link records that it installed it), or otherwise in the foreground, detached, with its output in the bot's directory (`logs/<agent>-<process>.log`), and started again when it ends. A service you installed yourself is yours: the link leaves it alone and says so. `nebo-link status` shows each process, and the chat is announced to NeboAI as soon as the agent answers. `unlink` stops the processes the link started and removes the services the link installed, never one you had.

### What it changes

Every change to your agent's config is recorded with the value it replaced, so turning models off and `unlink` put it back exactly. For OpenClaw, linking sets the Control UI's base path and trusted-proxy sign-in for requests that come through NeboAI, and sets a local password (`gateway.auth.password`) so your own `openclaw` commands keep working. For Hermes, linking writes `API_SERVER_KEY` into each profile's `.env` for the chat above; the dashboard itself needs no config change. Where a change needs a restart, the link runs the agent's own restart command.

### Where things are

Each linked bot has a directory in your data directory (`~/Library/Application Support/nebo-link/<bot id>` on macOS, `~/.local/share/nebo-link/<bot id>` on Linux, `%APPDATA%\nebo-link\<bot id>` on Windows; `--home` or `NEBO_LINK_HOME` moves it). The bot token is kept in an owner-only `token` file in that directory. Logs rotate daily in `logs/` and never contain tokens, headers or message content.

The service is a launchd agent on macOS, a systemd user unit on Linux (a system unit when run as root), and a logon task on Windows.

## License

MIT
