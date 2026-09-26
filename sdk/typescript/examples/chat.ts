// A terminal chat with any linked agent.
//
//   pnpm example:chat --url ws://127.0.0.1:7878/oal --pair K7QM-3XRD   pair once
//   pnpm example:chat --url ws://127.0.0.1:7878/oal [--agent <id>]      chat
//
// Use --relay wss://relay.example.com instead of --url to go through a relay.
// Permission prompts are answered here: y (allow once), a (always), n (deny).
// /mode <id> switches the session's mode; Ctrl-C stops a turn, or quits.

import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';
import { createInterface } from 'node:readline';
import { parseArgs } from 'node:util';

import { connect, pair, type Client, type Endpoint, type Identity, type Session } from '@openagentlink/client';

const { values: args } = parseArgs({
  options: { url: { type: 'string' }, relay: { type: 'string' }, pair: { type: 'string' }, agent: { type: 'string' } },
});
const endpoint = (args.relay ? { relay: args.relay } : { url: args.url ?? 'ws://127.0.0.1:7878/oal' }) as Endpoint;
const file = join(homedir(), '.config', 'openagentlink', 'identity.json');
const rl = createInterface({ input: process.stdin, output: process.stdout });
const lines = rl[Symbol.asyncIterator]();

/** Asks a question and reads one line; quits when input ends. */
async function ask(question: string): Promise<string> {
  process.stdout.write(question);
  const next = await lines.next();
  if (next.done) {
    client?.close();
    process.exit(0);
  }
  return next.value.trim();
}

if (args.pair) {
  const identity = await pair({ ...endpoint, code: args.pair, deviceName: 'OAL example chat' });
  mkdirSync(dirname(file), { recursive: true });
  writeFileSync(file, JSON.stringify(identity, null, 2), { mode: 0o600 });
  console.log(`Paired with ${identity.host.name}. Now run without --pair.`);
  process.exit(0);
}

let client: Client | undefined;
let identity: Identity;
try {
  identity = JSON.parse(readFileSync(file, 'utf8'));
} catch {
  console.error('Pair first: pnpm example:chat --url <host url> --pair <code>');
  process.exit(1);
}

client = await connect({ ...endpoint, credentials: identity });
const host = client.hosts()[0]!;
const agents = await host.agents();
const agent = agents.find((a) => a.id === args.agent) ?? agents.find((a) => a.online);
if (!agent) {
  console.error(`No agent online on ${host.name}.`);
  process.exit(1);
}
console.log(`${agent.label} on ${host.name} (${agent.runtime}${agent.folder ? `, ${agent.folder}` : ''})`);
const session: Session = await agent.session();
if (session.modes) console.log(`Mode: ${session.modes.currentModeId}. Modes: ${session.modes.availableModes.map((m) => m.id).join(', ')}.`);

rl.on('SIGINT', () => {
  if (session.turn) void session.cancel();
  else {
    client?.close();
    process.exit(0);
  }
});

for (;;) {
  const line = await ask('\n> ');
  if (!line) continue;
  if (line.startsWith('/mode ')) {
    await session.setMode(line.slice(6).trim()).then(
      () => console.log(`Mode: ${session.modes?.currentModeId}`),
      (error: Error) => console.log(error.message),
    );
    continue;
  }
  for await (const event of session.prompt(line)) {
    switch (event.type) {
      case 'text':
        process.stdout.write(event.text);
        break;
      case 'thinking':
        process.stdout.write(`\x1b[2m${event.text}\x1b[0m`);
        break;
      case 'tool_start':
        console.log(`\n[${event.tool.kind ?? 'tool'}] ${event.tool.title ?? event.tool.toolCallId}`);
        break;
      case 'tool_result':
        console.log(`[${event.tool.status}] ${event.output}`);
        break;
      case 'plan':
        console.log(`\nPlan:\n${event.entries.map((e) => `  [${e.status}] ${e.content}`).join('\n')}`);
        break;
      case 'permission': {
        const request = event.request;
        const answer = await ask(`\n${agent.label} wants to: ${request.toolCall.title}. Allow? [y]es / [a]lways / [n]o `);
        const choice = answer === 'a' ? request.allowAlways() : answer === 'y' ? request.allowOnce() : request.deny();
        await choice.catch((error: Error) => console.log(error.message));
        break;
      }
      case 'permission_resolved':
        if (event.answeredBy && event.answeredBy.deviceId !== host.device?.id) console.log(`\nAnswered on ${event.answeredBy.name}.`);
        break;
      case 'usage':
        console.log(`\n(${event.usage.totalTokens ?? '?'} tokens)`);
        break;
      case 'done':
        if (event.stopReason === 'cancelled') console.log('\nStopped.');
        break;
      case 'error':
        console.log(`\n${event.error.message}`);
        break;
    }
  }
}
