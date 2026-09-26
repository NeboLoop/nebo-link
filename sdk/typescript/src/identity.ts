// Pairing (spec section 6): a one-time code becomes a device identity.

import { ChannelClosed, webSocketDialer, type Dialer, type SecureChannel } from './channel.js';
import { closeError, exchange } from './connection.js';
import { encrypted } from './e2e.js';
import { OALError } from './errors.js';
import { base64url, relayDialer } from './relay.js';
import { PROTOCOL, type ClientInfo } from './types.js';

/**
 * What pairing with one host produces. Keep it secret (it holds the device
 * token and private key) and pass it to `connect` as `credentials`. It is
 * plain JSON: store it with `JSON.stringify` and read it back with `JSON.parse`.
 */
export interface Identity {
  host: { id: string; name: string; publicKey: string };
  device: { id: string; name: string; token: string; publicKey: string; privateKey: string };
}

/** Where to reach hosts: a relay (`wss://relay.example.com`), or one host's own URL. */
export type Endpoint = { relay: string; url?: undefined } | { url: string; relay?: undefined };

export type PairOptions = Endpoint & {
  /** The code the host shows (`K7QM-3XRD`). */
  code: string;
  /** What the owner will see this device called ("Alma's laptop"). */
  deviceName: string;
  client?: ClientInfo;
  /** The secure-channel layer. Default: `encrypted` (end-to-end, OAL section 17). */
  secure?: SecureChannel;
  dialer?: Dialer;
};

export const DEFAULT_CLIENT: ClientInfo = { name: '@openagentlink/client', version: '0.1.0' };

/** A relay path (`/oal/hosts/<id>`, `/oal/pair/<nameplate>`) on `relay`. */
export function relayUrl(relay: string, path: string): string {
  const base = /^wss?:\/\//.test(relay) ? relay : `wss://${relay}`;
  return base.replace(/\/+$/, '') + path;
}

/**
 * The code's nameplate (spec 4.4, 6.2): its first four characters, read as
 * Crockford base32. A relay routes a pairing by the nameplate alone; the rest
 * of the code never goes to it.
 */
export function nameplate(code: string): string {
  return code.toUpperCase().replace(/[^0-9A-Z]/g, '').replace(/[IL]/g, '1').replace(/O/g, '0').slice(0, 4);
}

export function checkEndpoint(options: { relay?: string; url?: string }): void {
  if (!options.relay === !options.url) throw new OALError('invalid_params', 'Pass either relay or url.');
}

/** Pairs with a host and returns this device's identity for it. */
export async function pair(options: PairOptions): Promise<Identity> {
  checkEndpoint(options);
  const { code, deviceName, client = DEFAULT_CLIENT, secure = encrypted, dialer = webSocketDialer } = options;
  const url = options.relay ? relayUrl(options.relay, `/oal/pair/${nameplate(code)}`) : options.url!;
  const keys = await generateKeyPair();
  // Through a relay, the connection first proves the new device key.
  const socket = await (options.relay ? relayDialer(options.relay, keys, dialer) : dialer)(url, ['oal']);
  const channel = await secure.open(socket, { protocol: PROTOCOL, client, code, device: keys }).catch((error: unknown) => {
    // A wrong code fails the pairing handshake, and the host closes with 4001.
    if (error instanceof ChannelClosed) throw error.code === 4001 ? refused() : closeError(error, 'the computer');
    throw error;
  });
  try {
    const result = await exchange(
      channel,
      'host/pair',
      { protocol: PROTOCOL, client, code, device: { name: deviceName, publicKey: keys.publicKey } },
      'the computer',
    );
    const host = result.info.host;
    // The key the host names must be the one the pairing handshake authenticated.
    if (channel.peerKey !== undefined && channel.peerKey !== host.publicKey) {
      channel.close(4001, 'Pairing refused.');
      throw refused();
    }
    return {
      host: { id: host.id, name: host.name, publicKey: host.publicKey },
      device: { id: result.device.id, name: result.device.name, token: result.device.token, ...keys },
    };
  } finally {
    channel.close(1000, '');
  }
}

function refused(): OALError {
  return new OALError('pairing_refused', "That code didn't work. Get a new one on the computer.");
}

/** A new X25519 key pair, base64url without padding: the device's static key (spec 6.1, 17.1). */
async function generateKeyPair(): Promise<{ publicKey: string; privateKey: string }> {
  const pair = (await crypto.subtle.generateKey({ name: 'X25519' }, true, ['deriveBits'])) as CryptoKeyPair;
  const publicKey = new Uint8Array(await crypto.subtle.exportKey('raw', pair.publicKey));
  const privateKey = await crypto.subtle.exportKey('jwk', pair.privateKey);
  return { publicKey: base64url(publicKey), privateKey: privateKey.d! };
}
