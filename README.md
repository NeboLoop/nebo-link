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
| `nebo-link unlink [--bot <id>]` | Restores your agent's config exactly as it was, removes the service and forgets the bot. |
| `nebo-link run --bot <id>` | Runs one bot's connection in the foreground; this is what the service runs. |

`--bot` is needed only when more than one agent is linked.

Removing the bot in the NeboAI app unlinks it here too: the service restores your agent's config, forgets the bot, removes itself, and `nebo-link status` tells you it was removed.

### What it changes

Every change to your agent's config is recorded with the value it replaced, so turning models off and `unlink` put it back exactly. For OpenClaw, linking sets the Control UI's base path and trusted-proxy sign-in for requests that come through NeboAI, and sets a local password (`gateway.auth.password`) so your own `openclaw` commands keep working. Hermes needs no config change. Where a change needs a restart, the link runs the agent's own restart command.

### Where things are

Each linked bot has a directory in your data directory (`~/Library/Application Support/nebo-link/<bot id>` on macOS, `~/.local/share/nebo-link/<bot id>` on Linux, `%APPDATA%\nebo-link\<bot id>` on Windows; `--home` or `NEBO_LINK_HOME` moves it). The bot token is kept in the system keychain; where there is none, it's in an owner-only `token` file in that directory. Logs rotate daily in `logs/` and never contain tokens, headers or message content.

The service is a launchd agent on macOS, a systemd user unit on Linux (a system unit when run as root), and a logon task on Windows.

## License

MIT
