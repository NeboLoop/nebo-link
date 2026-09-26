// End-to-end encryption (spec section 17): the `encrypted` secure channel.
//
// A port of the client side of crates/oal-secure. With a pairing code the
// channel pairs (section 17.5): CPace turns the code into a key only the two
// devices share, and that key is the PSK of a `Noise_XXpsk0` handshake that
// authenticates both static keys. Otherwise it opens a session (section
// 17.2): `Noise_IK` with the host key pinned at pairing, which authenticates
// this device to the host, so no `host/hello` follows.
//
// After either handshake every frame travels as Noise transport messages
// (section 17.3), one binary WebSocket message each. A relay in the middle
// sees their sizes and timing, never their contents.

import { concatBytes, utf8ToBytes } from '@noble/hashes/utils.js';
import { sha512 } from '@noble/hashes/sha2.js';

import { ChannelClosed, type ChannelContext, type FrameChannel, type SecureChannel, type Socket } from './channel.js';
import { Cpace, lvCat } from './cpace.js';
import { OALError } from './errors.js';
import { CipherState, HandshakeState, IK, MAX_MESSAGE, XXpsk0 } from './noise.js';
import { base64url, fromBase64url } from './relay.js';
import type { Frame, RpcError } from './types.js';

/** CPace CI, and the pairing handshake's Noise prologue. */
const PAIR_CONTEXT = utf8ToBytes('OAL-PAIR/1');
const PSK_LABEL = utf8ToBytes('OAL-PAIR/1 psk');
const SESSION_PROLOGUE = 'OAL-E2E/1 ';
const EMPTY = new Uint8Array(0);
const TAG = 16;
const MORE = 0x01;
const LAST = 0x00;
/** Frame bytes in one Noise message: the message, less the tag and the part byte. */
const PART = MAX_MESSAGE - TAG - 1;
/** Messages per direction between rekeys. */
const REKEY_EVERY = 2 ** 20;
/** The largest frame this client accepts: the minimum every OAL host must accept (section 4.1). */
const MAX_FRAME = 4 * 1024 * 1024;
/** Crockford base32, the alphabet of OAL codes (section 6.2). */
const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

/**
 * End-to-end encryption (OAL section 17), the default for `pair` and
 * `connect`. Pairing binds the connection to the code; every later
 * connection is a Noise session with the keys pinned at pairing. A relay
 * forwards ciphertext it can't read.
 */
export const encrypted: SecureChannel = {
  async open(socket, context) {
    try {
      return context.code !== undefined ? await pairing(socket, context) : await session(socket, context);
    } catch (error) {
      if (!(error instanceof ChannelClosed)) socket.close(1000, '');
      throw error;
    }
  },
};

/** Pairing (section 17.5): CPace, then `Noise_XXpsk0` keyed by its result. */
async function pairing(socket: Socket, context: ChannelContext): Promise<FrameChannel> {
  const prs = password(context.code!);
  const s = deviceKey(context);

  const cpace = new Cpace(prs, PAIR_CONTEXT, EMPTY);
  socket.send(lvCat(cpace.share, EMPTY));
  const msgb = await binary(socket);
  // MSGb = 0x20 || Yb || 0x00: any other shape ends the pairing.
  const yb = msgb.length === 34 && msgb[0] === 0x20 && msgb[33] === 0x00 ? msgb.subarray(1, 33) : undefined;
  const isk = yb && cpace.finish(EMPTY, EMPTY, yb, EMPTY, true);
  if (!isk) throw end(socket, 4001, 'Pairing failed.');

  const psk = sha512(lvCat(PSK_LABEL, isk)).subarray(0, 32);
  const hs = new HandshakeState(XXpsk0, { initiator: true, prologue: PAIR_CONTEXT, s, psks: [psk] });
  socket.send(hs.writeMessage(EMPTY));
  const message2 = await binary(socket);
  try {
    hs.readMessage(message2);
  } catch {
    throw end(socket, 4001, 'Pairing failed.');
  }
  socket.send(hs.writeMessage(EMPTY));
  const { send, receive } = hs.split();
  return new EncryptedChannel(socket, send, receive, { peerKey: base64url(hs.remoteStatic!) });
}

/** A session (section 17.2): `Noise_IK` to the host key pinned at pairing. */
async function session(socket: Socket, context: ChannelContext): Promise<FrameChannel> {
  const host = context.host;
  if (!host) throw new OALError('invalid_params', 'An encrypted connection needs the host from pairing.');
  const hs = new HandshakeState(IK, {
    initiator: true,
    prologue: utf8ToBytes(SESSION_PROLOGUE + host.id),
    s: deviceKey(context),
    rs: key(host.publicKey, "The host's key"),
  });
  const hello = { protocol: context.protocol, client: context.client };
  socket.send(hs.writeMessage(utf8ToBytes(JSON.stringify(hello))));
  const message2 = await binary(socket);
  let reply: unknown;
  try {
    reply = JSON.parse(new TextDecoder().decode(hs.readMessage(message2)));
  } catch (error) {
    throw error instanceof SyntaxError ? end(socket, 1002, 'The handshake reply is not JSON.') : end(socket, 4001, 'The host failed authentication.');
  }
  const answer = reply as { protocol?: unknown; device?: { id?: unknown; name?: unknown }; error?: RpcError } | null;
  if (answer?.error && typeof answer.error === 'object') throw OALError.fromRpc(answer.error);
  if (typeof answer?.protocol !== 'string' || typeof answer.device?.id !== 'string') {
    throw end(socket, 1002, 'The handshake reply has the wrong shape.');
  }
  const device = { id: answer.device.id, ...(typeof answer.device.name === 'string' && { name: answer.device.name }) };
  const { send, receive } = hs.split();
  return new EncryptedChannel(socket, send, receive, { authenticated: { protocol: answer.protocol, device } });
}

/**
 * The frame layer after a handshake (section 17.3). Each frame is split into
 * parts of at most 65518 bytes; each part is one Noise transport message whose
 * plaintext starts with 0x01 (more parts follow) or 0x00 (the last part).
 */
export class EncryptedChannel implements FrameChannel {
  readonly authenticated?: { protocol: string; device: { id: string; name?: string } };
  /** The host's static key (base64url) as the pairing handshake authenticated it. */
  readonly peerKey?: string;
  private sent = 0;
  private received = 0;
  private failure?: ChannelClosed;
  private readonly rekeyEvery: number;
  private readonly maxFrame: number;

  constructor(
    private readonly socket: Socket,
    private readonly sender: CipherState,
    private readonly receiver: CipherState,
    options: {
      authenticated?: EncryptedChannel['authenticated'];
      peerKey?: string;
      rekeyEvery?: number;
      maxFrame?: number;
    } = {},
  ) {
    this.authenticated = options.authenticated;
    this.peerKey = options.peerKey;
    this.rekeyEvery = options.rekeyEvery ?? REKEY_EVERY;
    this.maxFrame = options.maxFrame ?? MAX_FRAME;
  }

  send(frame: Frame): void {
    if (this.failure) return;
    const bytes = utf8ToBytes(JSON.stringify(frame));
    const parts = Math.max(1, Math.ceil(bytes.length / PART));
    for (let i = 0; i < parts; i++) {
      const chunk = bytes.subarray(i * PART, (i + 1) * PART);
      const plain = new Uint8Array(1 + chunk.length);
      plain[0] = i + 1 === parts ? LAST : MORE;
      plain.set(chunk, 1);
      this.socket.send(this.sender.encryptWithAd(EMPTY, plain));
      if (++this.sent % this.rekeyEvery === 0) this.sender.rekey();
    }
  }

  async recv(): Promise<Frame> {
    for (;;) {
      const bytes = await this.recvFrame();
      try {
        const frame: unknown = JSON.parse(new TextDecoder().decode(bytes));
        if (frame && typeof frame === 'object' && !Array.isArray(frame)) return frame as Frame;
      } catch {
        // Not JSON: the spec says to drop it.
      }
    }
  }

  close(code?: number, reason?: string): void {
    this.socket.close(code, reason);
  }

  private async recvFrame(): Promise<Uint8Array> {
    const parts: Uint8Array[] = [];
    let size = 0;
    for (;;) {
      // Nothing after a failure is read, even what already arrived.
      if (this.failure) throw this.failure;
      const data = await this.socket.recv();
      if (this.failure) throw this.failure;
      if (typeof data === 'string') throw this.fail(1002, 'A text message on an encrypted connection.');
      let plain: Uint8Array;
      try {
        plain = this.receiver.decryptWithAd(EMPTY, data);
      } catch {
        throw this.fail(4001, 'A message failed authentication.');
      }
      if (++this.received % this.rekeyEvery === 0) this.receiver.rekey();
      const flag = plain[0];
      if (flag !== LAST && flag !== MORE) throw this.fail(1002, 'A message part without a valid part byte.');
      size += plain.length - 1;
      if (size > this.maxFrame) throw this.fail(1009, 'A frame is too large.');
      parts.push(plain.subarray(1));
      if (flag === LAST) return concatBytes(...parts);
    }
  }

  private fail(code: number, reason: string): ChannelClosed {
    this.failure = end(this.socket, code, reason);
    return this.failure;
  }
}

/** Closes the socket with `code` and returns the error that reports it. */
function end(socket: Socket, code: number, reason: string): ChannelClosed {
  socket.close(code, reason);
  return new ChannelClosed(code, reason);
}

/** The next handshake message, which must be binary. */
async function binary(socket: Socket): Promise<Uint8Array> {
  const data = await socket.recv();
  if (typeof data === 'string') throw end(socket, 1002, 'A text message on an encrypted connection.');
  if (data.length > MAX_MESSAGE) throw end(socket, 1002, 'A message longer than 65535 bytes.');
  return data;
}

/**
 * The CPace password: the code's eight characters, normalized as section 6.1
 * says (upper case, `I` and `L` read as `1`, `O` as `0`, no hyphen or spaces).
 */
export function password(code: string): Uint8Array {
  let out = '';
  for (const c of code) {
    if (c === '-' || c === ' ') continue;
    const upper = c >= 'a' && c <= 'z' ? c.toUpperCase() : c;
    const d = upper === 'I' || upper === 'L' ? '1' : upper === 'O' ? '0' : upper;
    if (!ALPHABET.includes(d)) throw invalidCode();
    out += d;
  }
  if (out.length !== 8) throw invalidCode();
  return utf8ToBytes(out);
}

function invalidCode(): OALError {
  return new OALError('invalid_params', "That isn't a pairing code. It has eight letters and digits, like K7QM-3XRD.");
}

/** This device's static private key. */
function deviceKey(context: ChannelContext): Uint8Array {
  if (!context.device) throw new OALError('invalid_params', 'An encrypted connection needs the device key.');
  return key(context.device.privateKey, "This device's key");
}

function key(text: string, what: string): Uint8Array {
  let bytes: Uint8Array;
  try {
    bytes = fromBase64url(text);
  } catch {
    bytes = EMPTY;
  }
  if (bytes.length !== 32) throw new OALError('invalid_params', `${what} isn't a 32-byte base64url key.`);
  return bytes;
}
