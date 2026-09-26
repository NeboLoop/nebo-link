// End to end against a real host (nebo-link) through a real relay, with
// end-to-end encryption, using only the SDK's public API. Skipped unless
// these are set:
//
//   OAL_LIVE_RELAY  the relay's base URL (http://127.0.0.1:8480)
//   OAL_LIVE_CODE   a fresh pairing code from the host
//   OAL_LIVE_AGENT  the id of an agent on the host running the conformance
//                   suite's scripted agent (crates/oal-conformance/src/fake_agent.rs):
//                   `run: <cmd>` asks permission, then echoes; every turn
//                   reports usage {inputTokens: 12, outputTokens: 5, totalTokens: 17}.

import { describe, expect, it } from 'vitest';

import {
  connect,
  encrypted,
  pair,
  webSocketDialer,
  type Dialer,
  type Frame,
  type FrameChannel,
  type Host,
  type SecureChannel,
  type Socket,
  type Turn,
  type TurnEvent,
} from '../src/index.js';

const { OAL_LIVE_RELAY, OAL_LIVE_CODE, OAL_LIVE_AGENT } = process.env;
const USAGE = { inputTokens: 12, outputTokens: 5, totalTokens: 17 };

type Json = Record<string, any>;

/** A secure channel that records every frame each connection sends and receives. */
function tap(inner: SecureChannel) {
  const connections: { sent: Frame[]; received: Frame[] }[] = [];
  const waiters: { test(frame: Json): boolean; resolve(frame: Json): void }[] = [];
  const secure: SecureChannel = {
    async open(socket, context) {
      const channel: FrameChannel = await inner.open(socket, context);
      const log = { sent: [] as Frame[], received: [] as Frame[] };
      connections.push(log);
      return {
        authenticated: channel.authenticated,
        peerKey: channel.peerKey,
        send(frame) {
          log.sent.push(frame);
          channel.send(frame);
        },
        async recv() {
          const frame = await channel.recv();
          log.received.push(frame);
          for (const waiter of [...waiters]) {
            if (waiter.test(frame)) {
              waiters.splice(waiters.indexOf(waiter), 1);
              waiter.resolve(frame);
            }
          }
          return frame;
        },
        close: (code, reason) => channel.close(code, reason),
      };
    },
  };
  /** The first frame the latest connection received that passes `test`, now or when it arrives. */
  const received = (test: (frame: Json) => boolean): Promise<Json> => {
    const seen = connections.at(-1)?.received.find(test);
    if (seen) return Promise.resolve(seen);
    return new Promise((resolve) => waiters.push({ test, resolve }));
  };
  return { secure, connections, received };
}

/** A dialer that remembers its sockets, so the test can drop the connection. */
function droppable(): { dialer: Dialer; drop(): void } {
  const sockets: Socket[] = [];
  return {
    dialer: async (url, protocols) => {
      const socket = await webSocketDialer(url, protocols);
      sockets.push(socket);
      return socket;
    },
    drop: () => sockets.at(-1)!.close(4000, 'test drops the connection'),
  };
}

async function drain(turn: Turn, on: (event: TurnEvent) => unknown = () => {}): Promise<TurnEvent[]> {
  const events: TurnEvent[] = [];
  for await (const event of turn) {
    events.push(event);
    await on(event);
  }
  return events;
}

/** Waits for the host to go offline and come back. */
async function reconnected(h: Host): Promise<void> {
  let wasOffline = false;
  for await (const update of h.updates()) {
    if (update.type !== 'presence') continue;
    if (!update.online) wasOffline = true;
    else if (wasOffline) return;
  }
}

const types = (events: TurnEvent[]) => events.map((e) => e.type).filter((t) => t !== 'permission_resolved');

describe.skipIf(!OAL_LIVE_RELAY || !OAL_LIVE_CODE || !OAL_LIVE_AGENT)('live: a real host through a relay, encrypted end to end', () => {
  const relay = (OAL_LIVE_RELAY ?? '').replace(/^http/, 'ws');
  const agentId = OAL_LIVE_AGENT ?? '';

  it('pairs, runs a turn with a permission prompt, reconnects, and resumes a turn across a dropped connection', async () => {
    const identity = await pair({ relay, code: OAL_LIVE_CODE!, deviceName: 'SDK live test' });
    expect(identity.host.publicKey).toMatch(/^[A-Za-z0-9_-]{43}$/);

    // A whole turn on the first connection.
    const first = tap(encrypted);
    const client = await connect({ relay, credentials: identity, secure: first.secure });
    let sessionId: string;
    try {
      const h = client.hosts()[0]!;
      expect(h.online).toBe(true);
      expect(h.device?.id).toBe(identity.device.id);
      // The encrypted handshake authenticated the device: no host/hello.
      expect(first.connections[0]!.sent.map((f) => f.method)[0]).toBe('host/info');

      const agent = (await h.agents()).find((a) => a.id === agentId);
      expect(agent, `no agent ${agentId} on ${h.name}`).toBeDefined();
      const session = await agent!.session();
      sessionId = session.id;
      const newSession = first.connections[0]!.sent.find((f) => (f.acp as Json | undefined)?.method === 'session/new');
      expect((newSession!.acp as Json).params.cwd).toBe(agent!.folder);

      const events = await drain(session.prompt('run: echo hi'), async (event) => {
        if (event.type === 'permission') {
          expect(event.request.toolCall.title).toBe('echo hi');
          await event.request.allowOnce();
        }
      });
      expect(types(events)).toEqual(['tool_start', 'permission', 'tool_result', 'text', 'usage', 'done']);
      expect(events.find((e) => e.type === 'tool_result')).toMatchObject({ output: 'hi', tool: { status: 'completed' } });
      expect(events.find((e) => e.type === 'text')).toEqual({ type: 'text', text: 'Done.' });
      expect(events.find((e) => e.type === 'usage')).toEqual({ type: 'usage', usage: USAGE });
      expect(events.at(-1)).toEqual({ type: 'done', stopReason: 'end_turn' });

      const prompt = first.connections[0]!.sent.find((f) => (f.acp as Json | undefined)?.method === 'session/prompt')!;
      const answer = await first.received((f) => f.agent === agentId && f.acp?.id === (prompt.acp as Json).id && 'result' in f.acp);
      expect(answer.acp.result.stopReason).toBe('end_turn');
      const ended = await first.received((f) => f.method === 'host/turn' && f.params.sessionId === sessionId && f.params.state === 'ended');
      expect(ended.params.usage).toEqual(USAGE);
    } finally {
      client.close();
    }

    // Reconnect with the stored identity, load the session, and drop the
    // connection while a permission request waits.
    const second = tap(encrypted);
    const { dialer, drop } = droppable();
    const again = await connect({ relay, credentials: identity, secure: second.secure, dialer });
    try {
      const h = again.hosts()[0]!;
      const agent = (await h.agents()).find((a) => a.id === agentId)!;
      const session = await agent.session(sessionId);
      expect(session.history[0]).toEqual({ type: 'user', text: 'run: echo hi' });
      expect(session.history).toContainEqual({ type: 'text', text: 'Done.' });
      expect(session.turn).toBeNull();

      const turn = session.prompt('run: echo again');
      const events = await drain(turn, async (event) => {
        if (event.type !== 'permission') return;
        const toolCallId = event.request.toolCall.toolCallId;
        const before = second.connections.length;
        const back = reconnected(h);
        drop();
        await back;
        expect(second.connections.length).toBe(before + 1);

        // On the new connection: session/load answered with the replay, the
        // turn still running, and the same permission request sent again.
        const latest = second.connections.at(-1)!;
        const load = latest.sent.find((f) => (f.acp as Json | undefined)?.method === 'session/load')!;
        expect((load.acp as Json).params.sessionId).toBe(sessionId);
        const loaded = await second.received((f) => f.agent === agentId && f.acp?.id === (load.acp as Json).id && !('method' in f.acp));
        expect(loaded.acp.error).toBeUndefined();
        await second.received((f) => f.method === 'host/turn' && f.params.sessionId === sessionId && f.params.state === 'running');
        const asked = await second.received(
          (f) => f.agent === agentId && f.acp?.method === 'session/request_permission' && f.acp.params.toolCall.toolCallId === toolCallId,
        );
        expect(asked.acp.params.sessionId).toBe(sessionId);
        expect(session.turn).toBe(turn);
        expect(turn.finished).toBe(false);

        await event.request.allowOnce();
      });
      expect(types(events)).toEqual(['tool_start', 'permission', 'tool_result', 'text', 'usage', 'done']);
      expect(events.find((e) => e.type === 'tool_result')).toMatchObject({ output: 'again', tool: { status: 'completed' } });
      expect(events.find((e) => e.type === 'usage')).toEqual({ type: 'usage', usage: USAGE });
      expect(events.at(-1)).toEqual({ type: 'done', stopReason: 'end_turn' });
      const ended = await second.received((f) => f.method === 'host/turn' && f.params.sessionId === sessionId && f.params.state === 'ended');
      expect(ended.params.usage).toEqual(USAGE);
    } finally {
      again.close();
    }
  }, 90_000);
});
