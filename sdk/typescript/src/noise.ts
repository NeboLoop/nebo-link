// The Noise Protocol Framework (revision 34), for the two handshakes OAL
// uses (spec section 17): `Noise_IK_25519_ChaChaPoly_BLAKE2s` and
// `Noise_XXpsk0_25519_ChaChaPoly_BLAKE2s`.
//
// This is the state machine only: CipherState, SymmetricState and
// HandshakeState as the specification's section 5 defines them. The
// primitives are the audited noble libraries (X25519, ChaCha20-Poly1305,
// BLAKE2s, HMAC). It matches the Rust reference (the `snow` crate) byte for
// byte and is checked against the cacophony and snow test vectors.

import { chacha20poly1305 } from '@noble/ciphers/chacha.js';
import { x25519 } from '@noble/curves/ed25519.js';
import { blake2s } from '@noble/hashes/blake2.js';
import { hmac } from '@noble/hashes/hmac.js';
import { concatBytes, utf8ToBytes } from '@noble/hashes/utils.js';

/** The largest Noise message. */
export const MAX_MESSAGE = 65535;
const HASHLEN = 32;
const DHLEN = 32;
const TAG = 16;
const EMPTY = new Uint8Array(0);

/** The nonce for counter `n`: four zero bytes, then `n` as 64-bit little-endian. */
function nonce(n: number): Uint8Array {
  const out = new Uint8Array(12);
  const view = new DataView(out.buffer);
  view.setUint32(4, n >>> 0, true);
  view.setUint32(8, Math.floor(n / 2 ** 32), true);
  return out;
}

/** The nonce 2^64 - 1, reserved for REKEY. */
const REKEY_NONCE = new Uint8Array([0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);

/** A key and its nonce counter (spec section 5.1). */
export class CipherState {
  private k?: Uint8Array;
  private n = 0;

  initializeKey(k: Uint8Array): void {
    this.k = k;
    this.n = 0;
  }

  hasKey(): boolean {
    return this.k !== undefined;
  }

  encryptWithAd(ad: Uint8Array, plaintext: Uint8Array): Uint8Array {
    if (!this.k) return plaintext;
    const ciphertext = chacha20poly1305(this.k, nonce(this.n), ad).encrypt(plaintext);
    this.n++;
    return ciphertext;
  }

  /** Throws when the message fails authentication; the counter then stays. */
  decryptWithAd(ad: Uint8Array, ciphertext: Uint8Array): Uint8Array {
    if (!this.k) return ciphertext;
    const plaintext = chacha20poly1305(this.k, nonce(this.n), ad).decrypt(ciphertext);
    this.n++;
    return plaintext;
  }

  /** REKEY: the new key is the first 32 bytes of ENCRYPT(k, 2^64 - 1, empty, 32 zeros). */
  rekey(): void {
    if (!this.k) return;
    this.k = chacha20poly1305(this.k, REKEY_NONCE, EMPTY).encrypt(new Uint8Array(32)).subarray(0, 32);
  }
}

/** HKDF over HMAC-BLAKE2s, as Noise defines it (section 4.3). */
function hkdf(ck: Uint8Array, ikm: Uint8Array, outputs: 2 | 3): Uint8Array[] {
  const temp = hmac(blake2s, ck, ikm);
  const out1 = hmac(blake2s, temp, Uint8Array.of(1));
  const out2 = hmac(blake2s, temp, concatBytes(out1, Uint8Array.of(2)));
  if (outputs === 2) return [out1, out2];
  return [out1, out2, hmac(blake2s, temp, concatBytes(out2, Uint8Array.of(3)))];
}

/** The chaining key and handshake hash (spec section 5.2). */
class SymmetricState {
  ck: Uint8Array;
  h: Uint8Array;
  readonly cipher = new CipherState();

  constructor(protocolName: string) {
    const name = utf8ToBytes(protocolName);
    if (name.length <= HASHLEN) {
      this.h = new Uint8Array(HASHLEN);
      this.h.set(name);
    } else {
      this.h = blake2s(name);
    }
    this.ck = this.h;
  }

  mixKey(ikm: Uint8Array): void {
    const [ck, k] = hkdf(this.ck, ikm, 2) as [Uint8Array, Uint8Array];
    this.ck = ck;
    this.cipher.initializeKey(k);
  }

  mixHash(data: Uint8Array): void {
    this.h = blake2s(concatBytes(this.h, data));
  }

  mixKeyAndHash(ikm: Uint8Array): void {
    const [ck, h, k] = hkdf(this.ck, ikm, 3) as [Uint8Array, Uint8Array, Uint8Array];
    this.ck = ck;
    this.mixHash(h);
    this.cipher.initializeKey(k);
  }

  encryptAndHash(plaintext: Uint8Array): Uint8Array {
    const ciphertext = this.cipher.encryptWithAd(this.h, plaintext);
    this.mixHash(ciphertext);
    return ciphertext;
  }

  decryptAndHash(ciphertext: Uint8Array): Uint8Array {
    const plaintext = this.cipher.decryptWithAd(this.h, ciphertext);
    this.mixHash(ciphertext);
    return plaintext;
  }

  split(): [CipherState, CipherState] {
    const [k1, k2] = hkdf(this.ck, EMPTY, 2) as [Uint8Array, Uint8Array];
    const c1 = new CipherState();
    const c2 = new CipherState();
    c1.initializeKey(k1);
    c2.initializeKey(k2);
    return [c1, c2];
  }
}

export type Token = 'e' | 's' | 'ee' | 'es' | 'se' | 'ss' | 'psk';

/** A handshake pattern: whose static keys are known beforehand, and the message tokens. */
export interface Pattern {
  name: string;
  /** Pre-message `-> s` (the responder knows the initiator's static key). */
  initiatorStatic?: boolean;
  /** Pre-message `<- s` (the initiator knows the responder's static key). */
  responderStatic?: boolean;
  messages: Token[][];
}

export const IK: Pattern = {
  name: 'Noise_IK_25519_ChaChaPoly_BLAKE2s',
  responderStatic: true,
  messages: [
    ['e', 'es', 's', 'ss'],
    ['e', 'ee', 'se'],
  ],
};

export const XXpsk0: Pattern = {
  name: 'Noise_XXpsk0_25519_ChaChaPoly_BLAKE2s',
  messages: [['psk', 'e'], ['e', 'ee', 's', 'es'], ['s', 'se']],
};

export interface HandshakeOptions {
  initiator: boolean;
  prologue: Uint8Array;
  /** This side's static private key. */
  s?: Uint8Array;
  /** The peer's static public key, when known beforehand. */
  rs?: Uint8Array;
  /** Pre-shared keys, in the order the pattern uses them. */
  psks?: Uint8Array[];
  /** This side's ephemeral private key. Only test vectors set it. */
  e?: Uint8Array;
}

/** One side of a handshake (spec section 5.3). */
export class HandshakeState {
  private readonly ss: SymmetricState;
  private readonly initiator: boolean;
  private readonly psk: boolean;
  private readonly psks: Uint8Array[];
  private s?: { secret: Uint8Array; public: Uint8Array };
  private e?: { secret: Uint8Array; public: Uint8Array };
  private fixedE?: Uint8Array;
  private rs?: Uint8Array;
  private re?: Uint8Array;
  private index = 0;

  constructor(
    private readonly pattern: Pattern,
    options: HandshakeOptions,
  ) {
    this.ss = new SymmetricState(pattern.name);
    this.initiator = options.initiator;
    this.psk = pattern.messages.some((tokens) => tokens.includes('psk'));
    this.psks = [...(options.psks ?? [])];
    if (options.s) this.s = { secret: options.s, public: x25519.getPublicKey(options.s) };
    this.rs = options.rs;
    this.fixedE = options.e;
    this.ss.mixHash(options.prologue);
    const initiatorStatic = this.initiator ? this.s?.public : this.rs;
    const responderStatic = this.initiator ? this.rs : this.s?.public;
    if (pattern.initiatorStatic) this.ss.mixHash(required(initiatorStatic, 'the initiator static key'));
    if (pattern.responderStatic) this.ss.mixHash(required(responderStatic, 'the responder static key'));
  }

  /** True once every message of the pattern has been written or read. */
  get finished(): boolean {
    return this.index === this.pattern.messages.length;
  }

  /** The handshake hash h, which both sides share at the end. */
  get handshakeHash(): Uint8Array {
    return this.ss.h;
  }

  /** The peer's static public key, once it is known. */
  get remoteStatic(): Uint8Array | undefined {
    return this.rs;
  }

  writeMessage(payload: Uint8Array): Uint8Array {
    const tokens = this.next(true);
    const out: Uint8Array[] = [];
    for (const token of tokens) {
      switch (token) {
        case 'e': {
          const secret = this.fixedE ?? x25519.utils.randomSecretKey();
          this.e = { secret, public: x25519.getPublicKey(secret) };
          out.push(this.e.public);
          this.ss.mixHash(this.e.public);
          if (this.psk) this.ss.mixKey(this.e.public);
          break;
        }
        case 's':
          out.push(this.ss.encryptAndHash(required(this.s, 'a static key').public));
          break;
        case 'psk':
          this.ss.mixKeyAndHash(required(this.psks.shift(), 'a pre-shared key'));
          break;
        default:
          this.ss.mixKey(this.dh(token));
      }
    }
    out.push(this.ss.encryptAndHash(payload));
    const message = concatBytes(...out);
    if (message.length > MAX_MESSAGE) throw new Error('A handshake message is longer than 65535 bytes.');
    return message;
  }

  /** Reads the peer's message and returns its payload. Throws if it fails authentication. */
  readMessage(message: Uint8Array): Uint8Array {
    const tokens = this.next(false);
    let at = 0;
    const take = (n: number) => {
      if (message.length - at < n) throw new Error('A handshake message is too short.');
      const part = message.subarray(at, at + n);
      at += n;
      return part;
    };
    for (const token of tokens) {
      switch (token) {
        case 'e':
          this.re = take(DHLEN).slice();
          this.ss.mixHash(this.re);
          if (this.psk) this.ss.mixKey(this.re);
          break;
        case 's':
          this.rs = this.ss.decryptAndHash(take(this.ss.cipher.hasKey() ? DHLEN + TAG : DHLEN)).slice();
          break;
        case 'psk':
          this.ss.mixKeyAndHash(required(this.psks.shift(), 'a pre-shared key'));
          break;
        default:
          this.ss.mixKey(this.dh(token));
      }
    }
    return this.ss.decryptAndHash(message.subarray(at));
  }

  /** After the last message: the cipher this side sends with and the one it receives with. */
  split(): { send: CipherState; receive: CipherState } {
    if (!this.finished) throw new Error('The handshake has not finished.');
    const [c1, c2] = this.ss.split();
    return this.initiator ? { send: c1, receive: c2 } : { send: c2, receive: c1 };
  }

  private next(writing: boolean): Token[] {
    const tokens = this.pattern.messages[this.index];
    if (!tokens || (this.index % 2 === 0) !== (this.initiator === writing)) {
      throw new Error('That handshake message is out of turn.');
    }
    this.index++;
    return tokens;
  }

  private dh(token: 'ee' | 'es' | 'se' | 'ss'): Uint8Array {
    const e = () => required(this.e, 'an ephemeral key').secret;
    const s = () => required(this.s, 'a static key').secret;
    const re = () => required(this.re, "the peer's ephemeral key");
    const rs = () => required(this.rs, "the peer's static key");
    // `es` is the initiator's ephemeral with the responder's static; `se` the reverse.
    switch (token) {
      case 'ee':
        return x25519.getSharedSecret(e(), re());
      case 'ss':
        return x25519.getSharedSecret(s(), rs());
      case 'es':
        return this.initiator ? x25519.getSharedSecret(e(), rs()) : x25519.getSharedSecret(s(), re());
      case 'se':
        return this.initiator ? x25519.getSharedSecret(s(), re()) : x25519.getSharedSecret(e(), rs());
    }
  }
}

function required<T>(value: T | undefined, what: string): T {
  if (value === undefined) throw new Error(`The handshake needs ${what}.`);
  return value;
}
