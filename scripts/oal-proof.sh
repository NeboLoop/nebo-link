#!/usr/bin/env bash
# The proof that nebo-link is an Open Agent Link host: a real nebo-link, with
# the conformance suite's scripted agent (`oal-conformance agent`) linked as
# an ACP agent, behind a relay on this machine (oal-relay) and on the LAN.
# The scripted agent is also installed the way a coding agent is (its program
# on PATH, as `opencode`, which nebo-link detects and starts as `opencode
# acp`), so `host/agents/add` adds more of it (`add-remove.json`).
#
#   1. `oal-conformance host --e2e --relay`: every example, through the relay.
#   2. `oal-conformance host --e2e --tls-fingerprint`: every example, on the LAN.
#   3. The TypeScript and Python SDKs' live tests, through the relay: pair,
#      prompt, permission, reconnect.
#
# Everything runs in a temporary NEBO_LINK_HOME with a link made for the
# proof: its NeboAI endpoints point at a closed local port and its token is a
# placeholder, so nothing reaches NeboAI. Usage:
#
#   scripts/oal-proof.sh [target dir]      (default: target/debug)
#
# Set OAL_PROOF_SKIP_SDKS=1 to run only the conformance suite.

set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
bin="${1:-$root/target/debug}"
# Absolute: the service starts the agent from its own working folder.
bin="$(cd "$bin" && pwd)"
for b in nebo-link oal-relay oal-conformance; do
  [ -x "$bin/$b" ] || { echo "no $bin/$b: build it first (cargo build -p nebo-link -p oal-relay -p oal-conformance)" >&2; exit 2; }
done

work="$(mktemp -d "${TMPDIR:-/tmp}/oal-proof.XXXXXX")"
home="$work/home"
bot="oal-proof"
pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for file in "$work"/pair.*.pid; do [ -f "$file" ] && kill "$(cat "$file")" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [ "${OAL_PROOF_KEEP:-}" = "" ]; then rm -rf "$work"; else echo "kept $work"; fi
}
trap cleanup EXIT

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}
relay_port="$(free_port)"
lan_port="$(free_port)"
relay="http://127.0.0.1:$relay_port"

# The relay.
"$bin/oal-relay" serve --listen "127.0.0.1:$relay_port" --data-dir "$work/relay" >"$work/relay.log" 2>&1 &
pids+=($!)

# The link: one bot hosting the scripted agent, in its own folder.
mkdir -p "$home/$bot" "$work/agent"
chmod 700 "$home" "$home/$bot"
cat >"$home/$bot/link.json" <<EOF
{
  "botId": "$bot",
  "name": "OAL proof",
  "ownerId": "proof",
  "endpoints": { "api": "http://127.0.0.1:9", "comms": "ws://127.0.0.1:9", "tunnel": "ws://127.0.0.1:9", "janus": "http://127.0.0.1:9" },
  "agents": [{
    "id": "assistant",
    "label": "Fake Agent",
    "runtime": { "acp": "other" },
    "via": { "acp": { "program": "$bin/oal-conformance", "args": ["agent"], "env": [], "workdir": "$work/agent" } }
  }]
}
EOF
printf 'placeholder-not-a-neboai-token' >"$home/$bot/token"
chmod 600 "$home/$bot/token" "$home/$bot/link.json"

# The scripted agent installed as a coding agent: `opencode` on PATH, run by
# nebo-link as `opencode acp`. The service's own home is the proof's, so an
# agent it adds gets its folder there (~/NeboAI/<id>), and the coding agents
# installed for the user running the proof stay out of it.
mkdir -p "$work/installed" "$work/user"
printf '#!/bin/sh\nexec "%s" agent\n' "$bin/oal-conformance" >"$work/installed/opencode"
chmod +x "$work/installed/opencode"

export NEBO_LINK_HOME="$home"
HOME="$work/user" PATH="$work/installed:$PATH" \
  "$bin/nebo-link" run --bot "$bot" --relay "$relay" --lan "127.0.0.1:$lan_port" >"$work/nebo-link.log" 2>&1 &
pids+=($!)

# Up: the relay's tunnel and LAN direct, as the service's status says.
status="$home/$bot/status.json"
for _ in $(seq 1 150); do
  if [ -f "$status" ] && python3 - "$status" <<'PY'
import json, sys
s = json.load(open(sys.argv[1])).get("oal", {})
sys.exit(0 if s.get("relayConnected") and s.get("fingerprint") else 1)
PY
  then break; fi
  sleep 0.2
done
fingerprint="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["oal"]["fingerprint"])' "$status")" || {
  echo "nebo-link did not come up:" >&2; tail -50 "$work/nebo-link.log" >&2; exit 1; }

# A pairing code from the service, as `nebo-link pair` shows it; the command
# keeps running until a device pairs.
code() {
  local out="$work/pair.$RANDOM"
  "$bin/nebo-link" pair --bot "$bot" >"$out" 2>&1 &
  echo $! >"$out.pid"
  for _ in $(seq 1 100); do
    if [ -s "$out" ]; then head -1 "$out"; return; fi
    sleep 0.1
  done
  echo "no pairing code:" >&2; cat "$out" >&2; exit 1
}

echo "== conformance, through the relay"
"$bin/oal-conformance" host "$relay" --relay --e2e --code "$(code)" --agent assistant --runtime opencode

echo "== conformance, on the LAN"
"$bin/oal-conformance" host "wss://127.0.0.1:$lan_port/oal" --e2e --tls-fingerprint "$fingerprint" --code "$(code)" --agent assistant --runtime opencode

if [ "${OAL_PROOF_SKIP_SDKS:-}" = "" ]; then
  echo "== TypeScript SDK, live, through the relay"
  (cd "$root/sdk/typescript" && OAL_LIVE_RELAY="$relay" OAL_LIVE_CODE="$(code)" OAL_LIVE_AGENT=assistant pnpm exec vitest --run test/live.test.ts)

  echo "== Python SDK, live, through the relay"
  (cd "$root/sdk/python" && OAL_LIVE_RELAY="$relay" OAL_LIVE_CODE="$(code)" OAL_LIVE_AGENT=assistant uv run pytest -W error tests/test_live.py)
fi

echo "== every proof passed"
