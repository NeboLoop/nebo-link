// Interop with the Rust reference (crates/oal-secure): its example host peer,
// `cargo run -p oal-secure --example peer`, built and named by OAL_PEER.
// Skipped when OAL_PEER is not set.
//
// The peer serves `/oal/pair/<nameplate>` (pairing, then `host/pair`) and a
// session on any other path. On a session it answers `echo` with its params,
// `big {bytes}` with that many characters, and `unpair` by revoking the
// device (close 4003).

import { spawn, type ChildProcess } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  ChannelClosed,
  OALError,
  PROTOCOL,
  connect,
  encrypted,
  pair,
  webSocketDialer,
  type ChannelContext,
  type Dialer,
  type FrameChannel,
  type Identity,
} from '../src/index.js';

const bin = process.env.OAL_PEER;
const CODE = 'K7QM-3XRD';
const HOST_ID = 'h-peer';
const client = { name: 'sdk-interop', version: '0.0.0' };
/** The peer selects no subprotocol, and the platform WebSocket refuses a reply without the one it offered. */
const dialer: Dialer = (url) => webSocketDialer(url, []);

describe.skipIf(!bin)('interop with the Rust host (OAL_PEER)', () => {
  let child: ChildProcess;
  let dir: string;
  let url: string;
  let hostKey: string;
  let identity: Identity;

  beforeAll(async () => {
    dir = mkdtempSync(join(tmpdir(), 'oal-peer-'));
    child = spawn(bin!, [join(dir, 'keys'), HOST_ID, CODE], { stdio: ['ignore', 'pipe', 'inherit'] });
    const lines: string[] = [];
    await new Promise<void>((resolve, reject) => {
      createInterface({ input: child.stdout! }).on('line', (line) => {
        lines.push(line);
        if (lines.length === 2) resolve();
      });
      child.on('exit', (status) => reject(new Error(`The peer exited with ${status}.`)));
    });
    [url, hostKey] = lines as [string, string];
  });

  afterAll(() => {
    child?.kill();
    rmSync(dir, { recursive: true, force: true });
  });

  /** Opens an encrypted session with `identity`. */
  async function session(context: Partial<ChannelContext> = {}): Promise<FrameChannel> {
    const socket = await dialer(`${url}/oal`, []);
    return encrypted.open(socket, { protocol: PROTOCOL, client, host: identity.host, device: identity.device, ...context });
  }

  async function request(channel: FrameChannel, id: number, method: string, params: unknown): Promise<unknown> {
    channel.send({ jsonrpc: '2.0', id, method, params });
    for (;;) {
      const frame = await channel.recv();
      if (frame.id === id) return frame.result;
    }
  }

  it('pairs with the code: CPace, XXpsk0, then host/pair inside', async () => {
    identity = await pair({ url: `${url}/oal/pair/K7QM`, code: 'k7qm 3xrd', deviceName: 'Interop laptop', client, dialer });
    expect(identity.host).toEqual({ id: HOST_ID, name: 'OAL peer', publicKey: hostKey });
    expect(identity.device).toMatchObject({ id: expect.stringMatching(/^d-\d+$/), name: 'Interop laptop' });
  });

  it('connects with the stored identity (IK) and carries frames both ways', async () => {
    const channel = await session();
    try {
      expect(channel.authenticated).toEqual({ protocol: '0.1', device: { id: identity.device.id, name: 'Interop laptop' } });
      expect(await request(channel, 1, 'echo', { hello: 'world' })).toEqual({ hello: 'world' });

      // Frames over one Noise message (65518 bytes) are split into parts, both ways.
      const up = 'u'.repeat(300_000);
      expect(await request(channel, 2, 'echo', { up })).toEqual({ up });
      const down = (await request(channel, 3, 'big', { bytes: 250_000 })) as { data: string };
      expect(down.data).toBe('x'.repeat(250_000));
      expect(await request(channel, 4, 'echo', [1, 2, 3])).toEqual([1, 2, 3]);
    } finally {
      channel.close(1000, '');
    }
  });

  it('refuses a wrong code: the host closes with 4001', async () => {
    const socket = await dialer(`${url}/oal/pair/K7QM`, []);
    const keys = { publicKey: identity.device.publicKey, privateKey: identity.device.privateKey };
    const error = await encrypted.open(socket, { protocol: PROTOCOL, client, code: 'K7QM-3XRE', device: keys }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ChannelClosed);
    expect((error as ChannelClosed).code).toBe(4001);

    const refused = await pair({ url: `${url}/oal/pair/K7QM`, code: 'K7QM-0000', deviceName: 'x', client, dialer }).catch((e: unknown) => e);
    expect(refused).toBeInstanceOf(OALError);
    expect((refused as OALError).code).toBe('pairing_refused');
  });

  it('reports version_mismatch from the handshake reply', async () => {
    const error = await session({ protocol: { min: '9.0', max: '9.0' } }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OALError);
    expect((error as OALError).code).toBe('version_mismatch');
    expect((error as OALError).rpcCode).toBe(-33001);
    expect((error as OALError).message).toBe('This app speaks another version of OAL and this computer speaks 0.1. Update the app.');
  });

  it('is closed with 4003 when unpaired, and refused (4001) after', async () => {
    const channel = await session();
    channel.send({ jsonrpc: '2.0', id: 1, method: 'unpair', params: {} });
    const error = await channel.recv().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ChannelClosed);
    expect((error as ChannelClosed).code).toBe(4003);

    const again = await session().catch((e: unknown) => e);
    expect((again as ChannelClosed).code).toBe(4001);
    // Through the public API: the device isn't paired any more, which no reconnect can fix.
    const refused = await connect({ url: `${url}/oal`, credentials: identity, client, dialer }).catch((e: unknown) => e);
    expect((refused as OALError).code).toBe('unauthenticated');
  });
});
