// The Noise state machine against published test vectors, and the encrypted
// frame layer (spec section 17.3) over an in-memory socket pair.

import { readFileSync } from 'node:fs';

import { x25519 } from '@noble/curves/ed25519.js';
import { bytesToHex, hexToBytes, utf8ToBytes } from '@noble/hashes/utils.js';
import { describe, expect, it } from 'vitest';

import { ChannelClosed, type Socket } from '../src/channel.js';
import { EncryptedChannel, password } from '../src/e2e.js';
import { OALError } from '../src/errors.js';
import { CipherState, HandshakeState, IK, XXpsk0, type Pattern } from '../src/noise.js';

interface Vector {
  protocol_name: string;
  init_prologue: string;
  init_psks?: string[];
  init_static?: string;
  init_ephemeral: string;
  init_remote_static?: string;
  resp_prologue: string;
  resp_psks?: string[];
  resp_static?: string;
  resp_ephemeral: string;
  handshake_hash?: string;
  messages: { payload: string; ciphertext: string }[];
}

const { vectors } = JSON.parse(readFileSync(new URL('./noise-vectors.json', import.meta.url), 'utf8')) as { vectors: Vector[] };

const patterns: Record<string, Pattern> = {
  Noise_IK_25519_ChaChaPoly_BLAKE2s: IK,
  Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s: {
    name: 'Noise_XXpsk3_25519_ChaChaPoly_BLAKE2s',
    messages: [['e'], ['e', 'ee', 's', 'es'], ['s', 'se', 'psk']],
  },
  'Noise_NNpsk0+psk2_25519_ChaChaPoly_BLAKE2s': {
    name: 'Noise_NNpsk0+psk2_25519_ChaChaPoly_BLAKE2s',
    messages: [
      ['psk', 'e'],
      ['e', 'ee', 'psk'],
    ],
  },
};

const bytes = (hex: string | undefined) => (hex === undefined ? undefined : hexToBytes(hex));

describe('Noise test vectors', () => {
  for (const v of vectors) {
    it(v.protocol_name, () => {
      const pattern = patterns[v.protocol_name]!;
      const initiator = new HandshakeState(pattern, {
        initiator: true,
        prologue: hexToBytes(v.init_prologue),
        s: bytes(v.init_static),
        rs: bytes(v.init_remote_static),
        psks: v.init_psks?.map(hexToBytes),
        e: hexToBytes(v.init_ephemeral),
      });
      const responder = new HandshakeState(pattern, {
        initiator: false,
        prologue: hexToBytes(v.resp_prologue),
        s: bytes(v.resp_static),
        psks: v.resp_psks?.map(hexToBytes),
        e: hexToBytes(v.resp_ephemeral),
      });
      const handshake = pattern.messages.length;
      let ciphers: { i: { send: CipherState; receive: CipherState }; r: { send: CipherState; receive: CipherState } } | undefined;
      v.messages.forEach((m, index) => {
        const fromInitiator = index % 2 === 0;
        const payload = hexToBytes(m.payload);
        let ciphertext: Uint8Array;
        let read: Uint8Array;
        if (index < handshake) {
          const [writer, reader] = fromInitiator ? [initiator, responder] : [responder, initiator];
          ciphertext = writer.writeMessage(payload);
          read = reader.readMessage(ciphertext);
          if (index === handshake - 1) {
            ciphers = { i: initiator.split(), r: responder.split() };
            if (v.handshake_hash) {
              expect(bytesToHex(initiator.handshakeHash)).toBe(v.handshake_hash);
              expect(bytesToHex(responder.handshakeHash)).toBe(v.handshake_hash);
            }
          }
        } else {
          const [writer, reader] = fromInitiator ? [ciphers!.i.send, ciphers!.r.receive] : [ciphers!.r.send, ciphers!.i.receive];
          ciphertext = writer.encryptWithAd(new Uint8Array(0), payload);
          read = reader.decryptWithAd(new Uint8Array(0), ciphertext);
        }
        expect(bytesToHex(ciphertext)).toBe(m.ciphertext);
        expect(read).toEqual(payload);
      });
    });
  }

  it('REKEY is ENCRYPT(k, 2^64 - 1, empty, zeros), and the counter carries on', () => {
    const a = new CipherState();
    const b = new CipherState();
    const k = new Uint8Array(32).fill(7);
    a.initializeKey(k);
    b.initializeKey(k);
    const empty = new Uint8Array(0);
    b.decryptWithAd(empty, a.encryptWithAd(empty, utf8ToBytes('one')));
    a.rekey();
    const after = a.encryptWithAd(empty, utf8ToBytes('two'));
    expect(() => b.decryptWithAd(empty, after)).toThrow();
    b.rekey();
    expect(b.decryptWithAd(empty, after)).toEqual(utf8ToBytes('two'));
  });
});

/** Two sockets joined back to back. */
function socketPair(): { a: Socket; b: Socket; closes: { a?: number; b?: number }; wire: Uint8Array[] } {
  const closes: { a?: number; b?: number } = {};
  const wire: Uint8Array[] = [];
  const make = (name: 'a' | 'b') => {
    const inbox: (string | Uint8Array)[] = [];
    const waiters: { resolve(m: string | Uint8Array): void; reject(e: Error): void }[] = [];
    let closed: ChannelClosed | undefined;
    const socket: Socket & { deliver(m: string | Uint8Array): void; closed(c: ChannelClosed): void } = {
      send: (data) => {
        if (typeof data !== 'string' && name === 'a') wire.push(data);
        (name === 'a' ? b : a).deliver(data);
      },
      recv: () => {
        const next = inbox.shift();
        if (next !== undefined) return Promise.resolve(next);
        if (closed) return Promise.reject(closed);
        return new Promise((resolve, reject) => waiters.push({ resolve, reject }));
      },
      close: (code = 1000, reason = '') => {
        if (closes[name] !== undefined) return;
        closes[name] = code;
        const c = new ChannelClosed(code, reason);
        socket.closed(c);
        (name === 'a' ? b : a).closed(c);
      },
      deliver: (m) => {
        const waiter = waiters.shift();
        if (waiter) waiter.resolve(m);
        else inbox.push(m);
      },
      closed: (c) => {
        closed ??= c;
        for (const w of waiters.splice(0)) w.reject(c);
      },
    };
    return socket;
  };
  const a = make('a');
  const b = make('b');
  return { a, b, closes, wire };
}

/** An IK handshake in memory, and an encrypted channel on each side. */
function channels(options: { rekeyEvery?: number; maxFrame?: number } = {}) {
  const hostKey = x25519.utils.randomSecretKey();
  const prologue = utf8ToBytes('OAL-E2E/1 h1');
  const client = new HandshakeState(IK, { initiator: true, prologue, s: x25519.utils.randomSecretKey(), rs: x25519.getPublicKey(hostKey) });
  const host = new HandshakeState(IK, { initiator: false, prologue, s: hostKey });
  host.readMessage(client.writeMessage(new Uint8Array(0)));
  client.readMessage(host.writeMessage(new Uint8Array(0)));
  const c = client.split();
  const h = host.split();
  const pair = socketPair();
  return {
    ...pair,
    client: new EncryptedChannel(pair.a, c.send, c.receive, options),
    host: new EncryptedChannel(pair.b, h.send, h.receive, options),
    hostSend: h.send,
  };
}

describe('the encrypted frame layer', () => {
  it('splits big frames into parts of at most 65518 bytes and joins them', async () => {
    const { client, host, wire } = channels();
    const data = 'x'.repeat(200_000);
    client.send({ jsonrpc: '2.0', id: 1, result: { data } });
    expect(await host.recv()).toEqual({ jsonrpc: '2.0', id: 1, result: { data } });
    const json = JSON.stringify({ jsonrpc: '2.0', id: 1, result: { data } });
    expect(wire.map((m) => m.length)).toEqual([65535, 65535, 65535, json.length - 3 * 65518 + 17]);

    // A frame of exactly one part's worth is one message; one byte more is two.
    wire.length = 0;
    const pad = 65518 - JSON.stringify({ d: '' }).length;
    client.send({ d: 'y'.repeat(pad) });
    client.send({ d: 'y'.repeat(pad + 1) });
    expect(wire.map((m) => m.length)).toEqual([65535, 65535, 18]);
    expect(((await host.recv()) as { d: string }).d.length).toBe(pad);
    expect(((await host.recv()) as { d: string }).d.length).toBe(pad + 1);
  });

  it('rekeys both directions on the same message', async () => {
    const { client, host } = channels({ rekeyEvery: 3 });
    for (let i = 0; i < 20; i++) {
      client.send({ i });
      expect(await host.recv()).toEqual({ i });
      host.send({ i, back: true });
      expect(await client.recv()).toEqual({ i, back: true });
    }
  });

  it('ends the connection with 4001 when a message fails to decrypt, and reads nothing after', async () => {
    const { client, host, closes, a } = channels();
    client.send({ n: 1 });
    a.send(new Uint8Array(40)); // Not sealed with the client's key.
    client.send({ n: 2 });
    expect(await host.recv()).toEqual({ n: 1 });
    const error = await host.recv().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ChannelClosed);
    expect((error as ChannelClosed).code).toBe(4001);
    expect(closes.b).toBe(4001);
    expect(((await host.recv().catch((e: unknown) => e)) as ChannelClosed).code).toBe(4001);
  });

  it('ends a side that does not rekey at the rekey point', async () => {
    const pair = channels();
    const { hostSend } = pair;
    // The host rekeys after three messages; the client does not.
    for (let i = 0; i < 3; i++) {
      pair.host.send({ i });
      expect(await pair.client.recv()).toEqual({ i });
    }
    hostSend.rekey();
    pair.host.send({ after: true });
    expect(((await pair.client.recv().catch((e: unknown) => e)) as ChannelClosed).code).toBe(4001);
  });

  it('ends the connection with 1002 on a bad part byte or a text message', async () => {
    const first = channels();
    first.b.send(first.hostSend.encryptWithAd(new Uint8Array(0), Uint8Array.of(0x02, 0x7b, 0x7d)));
    expect(((await first.client.recv().catch((e: unknown) => e)) as ChannelClosed).code).toBe(1002);
    expect(first.closes.a).toBe(1002);

    const second = channels();
    second.b.send('{"jsonrpc":"2.0","method":"host/ping"}');
    expect(((await second.client.recv().catch((e: unknown) => e)) as ChannelClosed).code).toBe(1002);
  });

  it('ends the connection with 1009 on a frame over the limit', async () => {
    const { client, host, closes } = channels({ maxFrame: 100_000 });
    host.send({ data: 'z'.repeat(99_000) });
    expect(((await client.recv()) as { data: string }).data.length).toBe(99_000);
    host.send({ data: 'z'.repeat(100_001) });
    expect(((await client.recv().catch((e: unknown) => e)) as ChannelClosed).code).toBe(1009);
    expect(closes.a).toBe(1009);
  });

  it('XXpsk0 authenticates both static keys when the PSKs match, and fails when they differ', () => {
    const prologue = utf8ToBytes('OAL-PAIR/1');
    const run = (pskA: Uint8Array, pskB: Uint8Array) => {
      const clientKey = x25519.utils.randomSecretKey();
      const hostKey = x25519.utils.randomSecretKey();
      const client = new HandshakeState(XXpsk0, { initiator: true, prologue, s: clientKey, psks: [pskA] });
      const host = new HandshakeState(XXpsk0, { initiator: false, prologue, s: hostKey, psks: [pskB] });
      const m1 = client.writeMessage(new Uint8Array(0));
      expect(m1.length).toBe(48);
      host.readMessage(m1);
      const m2 = host.writeMessage(new Uint8Array(0));
      expect(m2.length).toBe(96);
      client.readMessage(m2);
      const m3 = client.writeMessage(new Uint8Array(0));
      expect(m3.length).toBe(64);
      host.readMessage(m3);
      expect(client.remoteStatic).toEqual(x25519.getPublicKey(hostKey));
      expect(host.remoteStatic).toEqual(x25519.getPublicKey(clientKey));
    };
    run(new Uint8Array(32).fill(1), new Uint8Array(32).fill(1));
    expect(() => run(new Uint8Array(32).fill(1), new Uint8Array(32).fill(2))).toThrow();
  });
});

describe('pairing codes', () => {
  it('normalize as section 6.1 says', () => {
    expect(new TextDecoder().decode(password('k7qm-3xrd'))).toBe('K7QM3XRD');
    expect(new TextDecoder().decode(password(' OIL0 0000 '))).toBe('01100000');
  });

  it('refuse the wrong length or letters', () => {
    for (const code of ['ABCD-EFG', 'ABCD-EFGHJ', 'ABCD-EFGU', 'ABCD-EFGÉ']) {
      expect(() => password(code)).toThrow(OALError);
    }
  });
});
