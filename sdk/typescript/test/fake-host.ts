// Runs the OAL conformance suite's fake host (crates/oal-conformance) as a
// subprocess. It prints every frame a client sends that breaks the spec.

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
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
