// CPace, the CFRG's balanced PAKE, as draft-irtf-cfrg-cpace-21 specifies it:
// cipher suite CPACE-RISTR255-SHA512, initiator-responder setting (sections
// 7, 8.1, 8.3 and A.1 of the draft). A port of crates/oal-secure/src/cpace.rs.
//
// This is the protocol layer only. The group (ristretto255, RFC 9496) and
// SHA-512 are the noble libraries'. The draft's test vectors (appendix B.3)
// are in test/cpace.test.ts.

import { ristretto255, ristretto255_hasher } from '@noble/curves/ed25519.js';
import { bytesToNumberLE } from '@noble/curves/utils.js';
import { sha512 } from '@noble/hashes/sha2.js';
import { concatBytes, randomBytes, utf8ToBytes } from '@noble/hashes/utils.js';

/** G_Ristretto255.DSI. */
const DSI = utf8ToBytes('CPaceRistretto255');
/** G.DSI || "_ISK". */
const DSI_ISK = utf8ToBytes('CPaceRistretto255_ISK');
/** SHA-512's input block size, H.s_in_bytes. */
const S_IN_BYTES = 128;

const { Point } = ristretto255;

/** One party's CPace run, between sending its share and receiving the other. */
export class Cpace {
  /** This party's encoded share, Ya or Yb. */
  readonly share: Uint8Array;
  private readonly y: bigint;

  /**
   * Starts a run: samples y and computes Y = y * g. G.sample_scalar() is as
   * section 8.3 recommends: 32 random bytes with the bits above 252 cleared,
   * read little-endian (always below the group order). `random` is for the
   * test vectors only.
   */
  constructor(prs: Uint8Array, ci: Uint8Array, sid: Uint8Array, random: Uint8Array = randomBytes(32)) {
    const bytes = random.slice();
    bytes[31]! &= 0x0f;
    this.y = Point.Fn.create(bytesToNumberLE(bytes));
    this.share = calculateGenerator(prs, ci, sid).multiply(this.y).toBytes();
  }

  /**
   * Finishes the run with the other party's share and returns ISK, or
   * undefined if the share does not decode or the result is the identity
   * (the draft's abort conditions). `initiator` fixes the transcript order:
   * transcript_ir(Ya, ADa, Yb, ADb).
   */
  finish(sid: Uint8Array, adOurs: Uint8Array, theirs: Uint8Array, adTheirs: Uint8Array, initiator: boolean): Uint8Array | undefined {
    const k = scalarMultVfy(this.y, theirs);
    if (!k) return undefined;
    const [ya, ada, yb, adb] = initiator ? [this.share, adOurs, theirs, adTheirs] : [theirs, adTheirs, this.share, adOurs];
    return sha512(concatBytes(lvCat(DSI_ISK, sid, k), lvCat(ya, ada), lvCat(yb, adb)));
  }
}

/** G.calculate_generator(H, PRS, CI, sid). */
function calculateGenerator(prs: Uint8Array, ci: Uint8Array, sid: Uint8Array) {
  const used = 1 + prependLenSize(prs.length) + prs.length + prependLenSize(DSI.length) + DSI.length;
  const zpad = new Uint8Array(Math.max(0, S_IN_BYTES - used));
  return ristretto255_hasher.deriveToCurve!(sha512(lvCat(DSI, prs, zpad, ci, sid)));
}

/**
 * G.scalar_mult_vfy(y, X), returning undefined where the draft returns G.I
 * (X does not decode, or y * X is the identity), since both abort.
 */
export function scalarMultVfy(y: bigint, x: Uint8Array): Uint8Array | undefined {
  let point;
  try {
    point = Point.fromBytes(x);
  } catch {
    return undefined;
  }
  const k = point.multiply(y);
  return k.is0() ? undefined : k.toBytes();
}

/** The bytes LEB128 takes to encode `len`. */
function prependLenSize(len: number): number {
  let n = 1;
  while (len >= 128) {
    len >>>= 7;
    n++;
  }
  return n;
}

/** lv_cat(a0, a1, …): each part as its LEB128 length, then its bytes. */
export function lvCat(...parts: Uint8Array[]): Uint8Array {
  const out: number[] = [];
  for (const part of parts) {
    let len = part.length;
    while (len >= 128) {
      out.push((len & 0x7f) | 0x80);
      len >>>= 7;
    }
    out.push(len);
    out.push(...part);
  }
  return Uint8Array.from(out);
}
