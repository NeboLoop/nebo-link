// Runs the OAL conformance suite's fake host (crates/oal-conformance) as a
// subprocess. It prints every frame a client sends that breaks the spec.
// `fakeRelay` puts it behind a real relay (crates/oal-relay).

import { spawn, type ChildProcess } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';

const repo = fileURLToPath(new URL('../../../', import.meta.url));

export interface FakeHost {
  url: string;
  code: string;
  /** Lines where the fake host found a spec violation. */
  violations: string[];
  stop(): void;
}

export async function fakeHost(code = 'K7QM-3XRD'): Promise<FakeHost> {
  const bin = process.env.OAL_CONFORMANCE ?? `${repo}target/debug/oal-conformance`;
  if (!existsSync(bin)) {
    throw new Error(`No fake host at ${bin}. Build it with \`cargo build -p oal-conformance\`, or set OAL_CONFORMANCE.`);
  }
  const child = spawn(bin, ['client', '--listen', '127.0.0.1:0', '--code', code], { stdio: ['ignore', 'pipe', 'pipe'] });
  const violations: string[] = [];
  createInterface({ input: child.stderr }).on('line', (line) => {
    if (line.includes('SPEC VIOLATION')) violations.push(line);
  });
  const url = await new Promise<string>((resolve, reject) => {
    const lines = createInterface({ input: child.stdout });
    lines.on('line', (line) => {
      const match = /(ws:\/\/\S+\/oal)/.exec(line);
      if (match) resolve(match[1]!);
    });
    child.on('exit', (status) => reject(new Error(`The fake host exited with ${status}.`)));
  });
  return { url, code, violations, stop: () => child.kill() };
}

export interface FakeRelay {
  /** The relay's base URL, as `pair` and `connect` take it. */
  url: string;
  /** The relay's Prometheus counters, by name (`oal_relay_auth_failures_total`). */
  metrics(): Promise<Record<string, number>>;
  stop(): void;
}

/**
 * A real relay (`oal-relay serve`) with `host` behind it through the host
 * bridge (`oal-relay host --forward`), holding the nameplate of `host.code`.
 */
export async function fakeRelay(host: FakeHost): Promise<FakeRelay> {
  const bin = process.env.OAL_RELAY ?? `${repo}target/debug/oal-relay`;
  if (!existsSync(bin)) {
    throw new Error(`No relay at ${bin}. Build it with \`cargo build -p oal-relay\`, or set OAL_RELAY.`);
  }
  const dir = mkdtempSync(join(tmpdir(), 'oal-relay-'));
  const children: ChildProcess[] = [];
  const stop = () => {
    for (const child of children) child.kill();
    rmSync(dir, { recursive: true, force: true });
  };
  try {
    const relay = spawn(bin, ['serve', '--listen', '127.0.0.1:0', '--data-dir', dir], { stdio: ['ignore', 'pipe', 'inherit'] });
    children.push(relay);
    const addr = await firstMatch(relay, /"listen":"([^"]+)"/);
    const bridge = spawn(
      bin,
      ['host', '--relay', `http://${addr}`, '--id', 'h-fake', '--key-file', join(dir, 'host.key'), '--forward', host.url, '--nameplate', host.code.slice(0, 4)],
      { stdio: ['ignore', 'pipe', 'inherit'] },
    );
    children.push(bridge);
    await firstMatch(bridge, /Pairing through the relay for codes starting (\S+)/);
    const metrics = async () => {
      const text = await (await fetch(`http://${addr}/metrics`)).text();
      return Object.fromEntries(
        text
          .split('\n')
          .filter((line) => line && !line.startsWith('#'))
          .map((line) => [line.slice(0, line.lastIndexOf(' ')), Number(line.slice(line.lastIndexOf(' ') + 1))]),
      );
    };
    return { url: `ws://${addr}`, metrics, stop };
  } catch (error) {
    stop();
    throw error;
  }
}

/** The first capture of `pattern` in a child's stdout. */
function firstMatch(child: ChildProcess, pattern: RegExp): Promise<string> {
  return new Promise((resolve, reject) => {
    createInterface({ input: child.stdout! }).on('line', (line) => {
      const match = pattern.exec(line);
      if (match) resolve(match[1]!);
    });
    child.on('exit', (status) => reject(new Error(`${child.spawnargs.slice(0, 2).join(' ')} exited with ${status}.`)));
  });
}
