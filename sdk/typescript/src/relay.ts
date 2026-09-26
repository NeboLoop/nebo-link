// Proving this device's key to a relay (crates/oal-relay, "Proving a key").
//
// Before every connection the relay wants a fresh proof that the device holds
// its X25519 key: `GET /oal/challenge` for a single-use nonce and the relay's
// key, then
//
//   shared = X25519(device_secret, relayKey)
//   prk    = HMAC-SHA256(key = "oal-relay-auth/1", message = shared)
//   proof  = HMAC-SHA256(key = prk, message =
//              "oal-relay-auth/1\n" role "\n" deviceKey "\n" relayKey "\n" nonce "\n" target "\n")
//
// sent as the `key`, `nonce` and `proof` query parameters (a browser cannot
// set headers on a WebSocket). The key is the same static key OAL's
// end-to-end encryption uses.

import type { Dialer } from './channel.js';
import { OALError } from './errors.js';

const DOMAIN = 'oal-relay-auth/1';

/** The device key pair, base64url without padding (`Identity.device`). */
export interface DeviceKeys {
  publicKey: string;
  privateKey: string;
}

/**
 * Wraps `dialer` so that every connection to `relay` carries a proof of
 * `device`'s key. Refuses a relay that isn't `wss://`, except one on this
 * machine.
 */
export function relayDialer(relay: string, device: DeviceKeys, dialer: Dialer): Dialer {
  checkRelay(relay);
  return async (url, protocols) => {
    const target = new URL(url);
    const route = /^(.*)\/oal\/(hosts|pair)\/([^/]+)$/.exec(target.pathname);
    if (!route) throw new OALError('invalid_params', `That isn't a relay connection: ${url}`);
    const [, prefix, kind, name] = route;
    const challengeUrl = new URL(`${prefix}/oal/challenge`, target);
    challengeUrl.protocol = target.protocol === 'wss:' ? 'https:' : 'http:';
    const { nonce, relayKey } = await challenge(challengeUrl);
    const proof = await prove(device, relayKey, nonce, `${kind === 'hosts' ? 'connect' : 'pair'}:${name}`);
    target.searchParams.set('key', device.publicKey);
    target.searchParams.set('nonce', nonce);
    target.searchParams.set('proof', proof);
    return dialer(target.toString(), protocols);
  };
}

function checkRelay(relay: string): void {
  const url = new URL(/^wss?:\/\//.test(relay) ? relay : `wss://${relay}`);
  const host = url.hostname.replace(/^\[|\]$/g, '');
  const local = host === 'localhost' || /^127(\.\d{1,3}){3}$/.test(host) || host === '::1';
  if (url.protocol !== 'wss:' && !local) {
    throw new OALError('invalid_params', `${relay} isn't encrypted. Use wss:// (or ws:// only for a relay on this machine).`);
  }
}

async function challenge(url: URL): Promise<{ nonce: string; relayKey: string }> {
  let response: Response;
  try {
    response = await fetch(url);
  } catch {
    throw new OALError('host_offline', `Couldn't reach ${url.host}.`);
  }
  const body = (await response.json().catch(() => ({}))) as { nonce?: unknown; relayKey?: unknown };
  if (!response.ok || typeof body.nonce !== 'string' || typeof body.relayKey !== 'string') {
    throw new OALError('host_offline', `Couldn't reach ${url.host}.`);
  }
  return { nonce: body.nonce, relayKey: body.relayKey };
}

/** The proof for `target` (`connect:<hostId>`, `pair:<NAMEPLATE>`), as a client. */
async function prove(device: DeviceKeys, relayKey: string, nonce: string, target: string): Promise<string> {
  const secret = await crypto.subtle.importKey(
    'jwk',
    { kty: 'OKP', crv: 'X25519', d: device.privateKey, x: device.publicKey },
    { name: 'X25519' },
    false,
    ['deriveBits'],
  );
  let shared: ArrayBuffer;
  try {
    const relay = await crypto.subtle.importKey('raw', fromBase64url(relayKey), { name: 'X25519' }, false, []);
    shared = await crypto.subtle.deriveBits({ name: 'X25519', public: relay }, secret, 256);
  } catch {
    throw new OALError('host_offline', "The relay's key isn't usable.");
  }
  const prk = await hmac(new TextEncoder().encode(DOMAIN), shared);
  const transcript = [DOMAIN, 'client', device.publicKey, relayKey, nonce, target].map((part) => `${part}\n`).join('');
  return base64url(new Uint8Array(await hmac(prk, new TextEncoder().encode(transcript))));
}

async function hmac(key: BufferSource, message: BufferSource): Promise<ArrayBuffer> {
  const k = await crypto.subtle.importKey('raw', key, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  return crypto.subtle.sign('HMAC', k, message);
}

export function fromBase64url(text: string): Uint8Array<ArrayBuffer> {
  const binary = atob(text.replace(/-/g, '+').replace(/_/g, '/'));
  return Uint8Array.from(binary, (c) => c.charCodeAt(0));
}

export function base64url(bytes: Uint8Array): string {
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}
