import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import {
  connect,
  pair,
  plaintext,
  webSocketDialer,
  OALError,
  type Client,
  type Dialer,
  type Host,
  type Identity,
  type SecureChannel,
  type Socket,
  type Turn,
  type TurnEvent,
} from '../src/index.js';
import { fakeHost, type FakeHost } from './fake-host.js';

let host: FakeHost;
let identity: Identity;
const clients: Client[] = [];

/** A dialer that remembers its sockets, so a test can drop the connection. */
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

async function open(dialer?: Dialer): Promise<{ client: Client; host: Host }> {
  const client = await connect({ url: host.url, credentials: identity, dialer });
  clients.push(client);
  return { client, host: client.hosts()[0]! };
}

async function fakeAgent(h: Host) {
  const agents = await h.agents();
  return agents.find((a) => a.id === 'fake')!;
}

/** Reads a turn to the end; `on` may act on each event. */
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

const types = (events: TurnEvent[]) => events.map((e) => e.type);

beforeEach(async () => {
  host = await fakeHost();
  identity = await pair({ url: host.url, code: host.code, deviceName: 'Test laptop' });
});

afterEach(() => {
  for (const client of clients.splice(0)) client.close();
  host.stop();
  expect(host.violations).toEqual([]);
});

describe('pairing and the host layer', () => {
  it('pairs, reconnects with the identity, and lists the agents', async () => {
    expect(identity.host).toMatchObject({ id: 'h-fake', name: 'Fake Host' });
    expect(identity.device.token.length).toBeGreaterThan(40);
    expect(identity.device.publicKey).toMatch(/^[A-Za-z0-9_-]{43}$/);

    const { client, host: h } = await open();
    expect(client.hosts().map((x) => x.name)).toEqual(['Fake Host']);
    expect(h.online).toBe(true);
    expect(h.info?.protocol).toEqual({ min: '0.1', max: '0.1' });

    const agent = await fakeAgent(h);
    expect(agent).toMatchObject({ id: 'fake', label: 'Fake Agent', folder: '/tmp/oal-fake-agent', online: true });
    expect(agent.modes?.availableModes.map((m) => m.id)).toEqual(['ask', 'folder', 'full']);

    const devices = await h.devices();
    expect(devices).toEqual([expect.objectContaining({ name: 'Test laptop', current: true })]);
  });

  it('refuses a wrong code in plain words', async () => {
    const other = await fakeHost('AAAA-BBBB');
    try {
      const error = await pair({ url: other.url, code: 'ZZZZ-ZZZZ', deviceName: 'x' }).catch((e: unknown) => e);
      expect(error).toBeInstanceOf(OALError);
      expect((error as OALError).code).toBe('pairing_refused');
      expect((error as OALError).message).toBe("That code didn't work. Get a new one on the computer.");
    } finally {
      other.stop();
    }
  });

  it('carries every frame through a custom secure channel', async () => {
    let sent = 0;
    const counting: SecureChannel = {
      async open(socket, context) {
        const inner = await plaintext.open(socket, context);
        return { ...inner, send: (frame) => (sent++, inner.send(frame)) };
      },
    };
    const client = await connect({ url: host.url, credentials: identity, secure: counting });
    clients.push(client);
    await fakeAgent(client.hosts()[0]!);
    expect(sent).toBe(2); // host/hello, host/agents
  });
});

describe('turns', () => {
  it('streams a prompt, asks permission, runs once approved, and reports usage', async () => {
    const { host: h } = await open();
    const session = await (await fakeAgent(h)).session();
    const events = await drain(session.prompt('run: echo hi'), async (event) => {
      if (event.type === 'permission') {
        expect(event.request.toolCall.title).toBe('echo hi');
        expect(event.request.options.map((o) => o.kind)).toEqual(['allow_once', 'reject_once']);
        await event.request.allowOnce();
      }
    });
    expect(types(events).filter((t) => t !== 'permission_resolved')).toEqual([
      'tool_start',
      'permission',
      'tool_result',
      'text',
      'usage',
      'done',
    ]);
    expect(events.find((e) => e.type === 'tool_result')).toMatchObject({ output: 'hi', tool: { status: 'completed' } });
    expect(events.find((e) => e.type === 'text')).toEqual({ type: 'text', text: 'Done.' });
    expect(events.find((e) => e.type === 'usage')).toEqual({ type: 'usage', usage: { inputTokens: 12, outputTokens: 5, totalTokens: 17 } });
    expect(events.at(-1)).toEqual({ type: 'done', stopReason: 'end_turn' });
    expect(events.find((e) => e.type === 'permission_resolved')).toMatchObject({
      outcome: { outcome: 'selected', optionId: 'allow-once' },
      answeredBy: { deviceId: identity.device.id },
    });
  });

  it('denies', async () => {
    const { host: h } = await open();
    const session = await (await fakeAgent(h)).session();
    const events = await drain(session.prompt('run: rm -rf /'), (event) => event.type === 'permission' && event.request.deny());
    expect(events.find((e) => e.type === 'tool_result')).toMatchObject({ tool: { status: 'failed' } });
    expect(events.find((e) => e.type === 'text')).toEqual({ type: 'text', text: "Okay, I won't run it." });
    expect(events.at(-1)).toEqual({ type: 'done', stopReason: 'end_turn' });
  });

  it('cancels', async () => {
    const { host: h } = await open();
    const session = await (await fakeAgent(h)).session();
    const turn = session.prompt('wait');
    const events = await drain(turn, (event) => event.type === 'text' && turn.cancel());
    expect(events).toEqual([
      { type: 'text', text: 'Working on it.' },
      { type: 'done', stopReason: 'cancelled' },
    ]);
  });

  it('refuses a second prompt while a turn runs', async () => {
    const { host: h } = await open();
    const session = await (await fakeAgent(h)).session();
    const first = session.prompt('wait');
    const second = await drain(session.prompt('hello'));
    expect(second).toHaveLength(1);
    expect(second[0]).toMatchObject({ type: 'error', error: { code: 'turn_in_progress' } });
    await drain(first, (event) => event.type === 'text' && first.cancel());
  });

  it('changes mode: Full access runs without asking', async () => {
    const { host: h } = await open();
    const session = await (await fakeAgent(h)).session();
    expect(session.modes?.currentModeId).toBe('ask');
    await session.setMode('full');
    expect(session.modes?.currentModeId).toBe('full');
    const events = await drain(session.prompt('run: echo hi'));
    expect(types(events)).toEqual(['tool_start', 'tool_result', 'text', 'usage', 'done']);

    const error = await session.setMode('yolo').catch((e: unknown) => e);
    expect((error as OALError).code).toBe('not_offered');
  });
});

describe('resuming', () => {
  it('reconnects mid-turn and gets the same permission request back', async () => {
    const { dialer, drop } = droppable();
    const { host: h } = await open(dialer);
    const session = await (await fakeAgent(h)).session();
    const events = await drain(session.prompt('run: echo hi'), async (event) => {
      if (event.type === 'permission') {
        const back = reconnected(h);
        drop();
        await back;
        await event.request.allowOnce();
      }
    });
    expect(types(events).filter((t) => t !== 'permission_resolved')).toEqual([
      'tool_start',
      'permission',
      'tool_result',
      'text',
      'usage',
      'done',
    ]);
    expect(events.at(-1)).toEqual({ type: 'done', stopReason: 'end_turn' });
  });

  it('reconnects mid-turn and cancels', async () => {
    const { dialer, drop } = droppable();
    const { host: h } = await open(dialer);
    const session = await (await fakeAgent(h)).session();
    const turn = session.prompt('wait');
    const events = await drain(turn, async (event) => {
      if (event.type === 'text') {
        const back = reconnected(h);
        drop();
        await back;
        await turn.cancel();
      }
    });
    expect(events).toEqual([
      { type: 'text', text: 'Working on it.' },
      { type: 'done', stopReason: 'cancelled' },
    ]);
  });

  it('another client loads the session mid-turn; the first answer wins', async () => {
    const a = await open();
    const sessionA = await (await fakeAgent(a.host)).session();
    const turnA = sessionA.prompt('run: echo hi');
    const iterA = turnA[Symbol.asyncIterator]();
    const first = (await iterA.next()).value as TurnEvent;
    expect(first.type).toBe('tool_start');
    const asked = (await iterA.next()).value as TurnEvent;
    expect(asked.type).toBe('permission');

    const b = await open();
    const inbox = await b.host.notifications();
    expect(inbox).toHaveLength(1);
    expect(inbox[0]!.toolCall.title).toBe('echo hi');

    const sessionB = await (await fakeAgent(b.host)).session(sessionA.id);
    expect(types(sessionB.history)).toEqual(['user', 'tool_start']);
    expect(sessionB.history[0]).toEqual({ type: 'user', text: 'run: echo hi' });
    expect(sessionB.turn).not.toBeNull();
    const eventsB = await drain(sessionB.turn!, (event) => event.type === 'permission' && event.request.allowOnce());
    expect(eventsB.at(-1)).toEqual({ type: 'done', stopReason: 'end_turn' });

    const late = await (asked as Extract<TurnEvent, { type: 'permission' }>).request.allowOnce().catch((e: unknown) => e);
    expect((late as OALError).code).toBe('already_answered');

    const rest: TurnEvent[] = [];
    for (let next = await iterA.next(); !next.done; next = await iterA.next()) rest.push(next.value);
    expect(rest.find((e) => e.type === 'permission_resolved')).toMatchObject({ outcome: { optionId: 'allow-once' } });
    expect(rest.at(-1)).toEqual({ type: 'done', stopReason: 'end_turn' });
  });
});
