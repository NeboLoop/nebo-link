---
name: connect-to-neboai
description: Connect this agent to the owner's NeboAI account with a one-time code from the NeboAI app, so the owner can reach and manage it from the NeboAI phone app and neboai.com. Use when the owner says "connect to NeboAI", "link to NeboAI", "install Nebo Link", or pastes a code that looks like NEBO-XXXX-XXXX-XXXX.
license: MIT
compatibility: Runs on macOS, Linux and Windows. Needs shell access, internet access, and OpenClaw or Hermes installed for the same user.
metadata:
  homepage: https://github.com/NeboLoop/nebo-link
  version: "1.0.0"
---

# Connect to NeboAI

Nebo Link is a small open-source program (github.com/NeboLoop/nebo-link) that connects this agent to the owner's NeboAI account. Once it's linked, the owner can open this agent from the NeboAI phone app and from neboai.com without opening ports or setting up a VPN. It only makes outbound connections, and this agent's own passwords and tokens stay on this machine.

## What you need from the owner

Only the one-time code from the NeboAI app. The owner gets it in the app under **Connect OpenClaw or Hermes**. It looks like `NEBO-XXXX-XXXX-XXXX` and it expires 5 minutes after it's shown.

Never ask for the owner's NeboAI password, email sign-in link, API keys or any other secret. The code is all Nebo Link needs. If the owner offers a password or key, tell them it isn't needed and don't use it.

If the owner asked to connect but didn't give a code, ask for it in one line: "Open the NeboAI app, choose Connect OpenClaw or Hermes, and send me the code it shows."

## Steps

Use the runtime you are for `--runtime`: `openclaw` if you are OpenClaw, `hermes` if you are Hermes. Replace `NEBO-XXXX-XXXX-XXXX` with the owner's code.

### 1. Tell the owner what happens

Before you run anything, tell the owner in one or two sentences: you're installing Nebo Link from neboai.com and linking this agent to their NeboAI account. If you are OpenClaw, add that linking restarts OpenClaw once, and if this chat goes quiet they can ask "Is NeboAI connected?"

### 2. Download the installer

The installer checks the download's signature and checksum before it installs anything. It installs `nebo-link` into the user's own folder and needs no administrator rights.

macOS or Linux:

```sh
cd ~ && curl -fsSL -o nebo-link-install.sh https://neboai.com/link.sh
```

Windows (PowerShell):

```powershell
cd ~; Invoke-WebRequest -UseBasicParsing -Uri https://neboai.com/link.ps1 -OutFile nebo-link-install.ps1
```

### 3. Run it with the code

Run it in the background with its output in a log file, so it finishes even if OpenClaw restarts while it runs.

macOS or Linux:

```sh
cd ~ && nohup sh nebo-link-install.sh NEBO-XXXX-XXXX-XXXX --runtime openclaw > nebo-link-install.log 2>&1 &
```

Windows (PowerShell):

```powershell
cd ~; Start-Process powershell -WindowStyle Hidden -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File','.\nebo-link-install.ps1','NEBO-XXXX-XXXX-XXXX','--runtime','openclaw' -RedirectStandardOutput nebo-link-install.log -RedirectStandardError nebo-link-install.err
```

Wait about 15 seconds, then read `nebo-link-install.log` (and `nebo-link-install.err` on Windows).

### 4. Confirm it's connected

```sh
nebo-link status
```

If the shell can't find `nebo-link`, use its full path: `~/.local/bin/nebo-link status` on macOS or Linux, `$env:LOCALAPPDATA\Programs\nebo-link\nebo-link.exe status` on Windows.

Read the `status:` line for this agent:

- `online`: done.
- `online, connecting the tunnel`, or `not running` within the first minute: wait 10 seconds and check again, up to three times.
- `offline: <reason>` or `not running (no service installed)`: it didn't finish. Use the reason and the log for step 5.

### 5. Tell the owner, in plain words

When it's online, say something like: "Done. I'm connected to your NeboAI account as <name from status>. Open the NeboAI app and you'll find me in your bots."

When it failed, say what failed and what to do next, in one or two sentences, based on what the installer or `nebo-link status` printed:

| What it printed | What to tell the owner |
|---|---|
| `invalid or expired code` | "That code has expired. Codes last 5 minutes. Send me a new one from the NeboAI app." |
| `Nebo Link isn't available yet.` | "Nebo Link isn't available to install yet, so I couldn't connect." |
| `Could not verify the download` | "The download didn't pass its signature check, so nothing was installed. Try again in a few minutes." |
| `No OpenClaw or Hermes install found for this user.` | "Nebo Link couldn't find me under this user account. It needs to run as the same user I run as." |
| Anything else | Quote the one line that says what went wrong and offer to try again. |

Don't paste the whole log to the owner. Don't retry a failed code on your own; codes are one-time.

## Later

- `nebo-link status` shows whether this agent is connected.
- `nebo-link unlink` disconnects it and puts this agent's settings back exactly as they were. Only run it when the owner asks. They can also remove the bot in the NeboAI app.
- Nebo Link keeps itself up to date. Run `nebo-link update` only if the owner asks for it.
- The installer files in the home folder (`nebo-link-install.*`) can be deleted once it's connected.
