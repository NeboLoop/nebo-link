"""A terminal chat with any linked agent.

    python examples/chat.py --url ws://127.0.0.1:7878/oal --pair K7QM-3XRD   pair once
    python examples/chat.py --url ws://127.0.0.1:7878/oal [--agent <id>]      chat

Use --relay wss://relay.example.com instead of --url to go through a relay.
Permission prompts are answered here: y (allow once), a (always), n (deny).
/mode <id> switches the session's mode; Ctrl-C stops a turn, Ctrl-D quits.
"""

from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from pathlib import Path

from openagentlink import (
    Done,
    Identity,
    OALError,
    PermissionAsked,
    PermissionResolved,
    PlanUpdate,
    TextDelta,
    Thinking,
    ToolResult,
    ToolStart,
    TurnError,
    UsageReport,
    connect,
    pair,
)

IDENTITY = Path("~/.config/openagentlink/identity.json").expanduser()


async def read_line(question: str) -> str:
    print(question, end="", flush=True)
    line = await asyncio.to_thread(sys.stdin.readline)
    if not line:
        raise EOFError
    return line.strip()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url")
    parser.add_argument("--relay")
    parser.add_argument("--pair")
    parser.add_argument("--agent")
    args = parser.parse_args()
    endpoint = {"relay": args.relay} if args.relay else {"url": args.url or "ws://127.0.0.1:7878/oal"}

    if args.pair:
        identity = await pair(**endpoint, code=args.pair, device_name="OAL example chat")
        identity.save(IDENTITY)
        print(f"Paired with {identity.host.name}. Now run without --pair.")
        return
    if not IDENTITY.exists():
        sys.exit("Pair first: python examples/chat.py --url <host url> --pair <code>")

    async with await connect(**endpoint, credentials=Identity.load(IDENTITY)) as client:
        host = client.hosts()[0]
        agents = await host.agents()
        agent = next((a for a in agents if a.id == args.agent), None) or next((a for a in agents if a.online), None)
        if agent is None:
            sys.exit(f"No agent online on {host.name}.")
        print(f"{agent.label} on {host.name} ({agent.runtime}{', ' + agent.folder if agent.folder else ''})")
        session = await agent.session()
        if session.modes:
            modes = ", ".join(m["id"] for m in session.modes["availableModes"])
            print(f"Mode: {session.modes['currentModeId']}. Modes: {modes}.")
        if sys.platform != "win32":
            # Ctrl-C stops the running turn instead of quitting.
            asyncio.get_running_loop().add_signal_handler(
                signal.SIGINT, lambda: asyncio.ensure_future(session.cancel()) if session.turn else None
            )

        while True:
            try:
                line = await read_line("\n> ")
            except EOFError:
                return
            if not line:
                continue
            if line.startswith("/mode "):
                try:
                    await session.set_mode(line[6:].strip())
                    print(f"Mode: {session.modes['currentModeId'] if session.modes else '?'}")
                except OALError as error:
                    print(error.message)
                continue
            try:
                async for event in session.prompt(line):
                    match event:
                        case TextDelta(text=text):
                            print(text, end="", flush=True)
                        case Thinking(text=text):
                            print(f"\x1b[2m{text}\x1b[0m", end="", flush=True)
                        case ToolStart(tool=tool):
                            print(f"\n[{tool.get('kind', 'tool')}] {tool.get('title', tool['toolCallId'])}")
                        case ToolResult(tool=tool, output=output):
                            print(f"[{tool.get('status')}] {output}")
                        case PlanUpdate(entries=entries):
                            print("\nPlan:\n" + "\n".join(f"  [{e['status']}] {e['content']}" for e in entries))
                        case PermissionAsked(request=request):
                            title = request.tool_call.get("title", "use a tool")
                            answer = await read_line(f"\n{agent.label} wants to: {title}. Allow? [y]es / [a]lways / [n]o ")
                            try:
                                if answer == "a":
                                    await request.allow_always()
                                elif answer == "y":
                                    await request.allow_once()
                                else:
                                    await request.deny()
                            except OALError as error:
                                print(error.message)
                        case PermissionResolved(answered_by=by) if by and by["deviceId"] != (host.device or {}).get("id"):
                            print(f"\nAnswered on {by['name']}.")
                        case UsageReport(usage=usage):
                            print(f"\n({usage.get('totalTokens', '?')} tokens)")
                        case Done(stop_reason="cancelled"):
                            print("\nStopped.")
                        case TurnError(error=error):
                            print(f"\n{error.message}")
            except EOFError:
                return


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
