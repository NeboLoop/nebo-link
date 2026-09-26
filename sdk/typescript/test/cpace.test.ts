// CPace against draft-irtf-cfrg-cpace-21: appendix A.1 (lv_cat) and the
// ristretto255 test vectors of appendix B.3, as crates/oal-secure/src/cpace.rs
// checks them.

import { ristretto255 } from '@noble/curves/ed25519.js';
import { bytesToNumberLE } from '@noble/curves/utils.js';
import { hexToBytes, utf8ToBytes } from '@noble/hashes/utils.js';
import { describe, expect, it } from 'vitest';

import { Cpace, lvCat, scalarMultVfy } from '../src/cpace.js';

const hex = (s: string) => hexToBytes(s.replace(/\s+/g, ''));

describe('lv_cat (appendix A.1)', () => {
  it('encodes lengths as LEB128', () => {
    expect(lvCat(new Uint8Array(0))).toEqual(hex('00'));
    expect(lvCat(utf8ToBytes('1234'))).toEqual(hex('0431323334'));
    const r127 = Uint8Array.from({ length: 127 }, (_, i) => i);
    expect([...lvCat(r127).subarray(0, 2)]).toEqual([0x7f, 0x00]);
    const r128 = Uint8Array.from({ length: 128 }, (_, i) => i);
    const encoded = lvCat(r128);
    expect(encoded.length).toBe(130);
    expect([...encoded.subarray(0, 3)]).toEqual([0x80, 0x01, 0x00]);
    expect(lvCat(utf8ToBytes('1234'), utf8ToBytes('5'), new Uint8Array(0), utf8ToBytes('678'))).toEqual(hex('043132333401350003363738'));
  });
});

describe('CPace ristretto255 SHA-512 (appendix B.3)', () => {
  const prs = utf8ToBytes('Password');
  const sid = hex('7e4b4791d6a8ef019b936c79fb7f2c57');
  const ci = hex('0b415f696e69746961746f720b425f726573706f6e646572');

  it('calculates the generator (B.3.1)', () => {
    // With the scalar 1, the share is the generator itself.
    const one = new Uint8Array(32);
    one[0] = 1;
    expect(new Cpace(prs, ci, sid, one).share).toEqual(hex('222b6b195fe84b1652badb6f6a3ae3d24341e7306967f0b8115b40d5698c7e56'));
  });

  it('gives the shares, the secret point and ISK (B.3.2 to B.3.5)', () => {
    const ya = hex('da3d23700a9e5699258aef94dc060dfda5ebb61f02a5ea77fad53f4ff0976d08');
    const yb = hex('d2316b454718c35362d83d69df6320f38578ed5984651435e2949762d900b80d');
    const a = new Cpace(prs, ci, sid, ya);
    const b = new Cpace(prs, ci, sid, yb);
    expect(a.share).toEqual(hex('d6bac480f2c386c394efc7c47adb9925dcd2630b64f240c50f8d0eec482b9157'));
    expect(b.share).toEqual(hex('3ea7e0b19560d7c0b0f5734f63b955286dfa8232b5ebe63324e2d9e7433f7258'));
    const k = hex('80b69a8a76457ab6a4d7f887a4bf6b55a2f80ac19c333f917a05fc9887c8b40f');
    expect(scalarMultVfy(bytesToNumberLE(ya), b.share)).toEqual(k);
    expect(scalarMultVfy(bytesToNumberLE(yb), a.share)).toEqual(k);
    const isk = hex(
      `b69effbf61b51d56401c0f65601abe428de8206feaaf0e32198896dcae7b35cd
       2b38950a39dfd5d4a79164614c2984f7daa460b588c1e80c3fa2068af7900447`,
    );
    expect(a.finish(sid, utf8ToBytes('ADa'), b.share, utf8ToBytes('ADb'), true)).toEqual(isk);
    expect(b.finish(sid, utf8ToBytes('ADb'), a.share, utf8ToBytes('ADa'), false)).toEqual(isk);
  });

  it('multiplies a valid point and refuses invalid ones and the identity (B.3.10, B.3.11)', () => {
    const s = ristretto255.Point.Fn.create(bytesToNumberLE(hex('7cd0e075fa7955ba52c02759a6c90dbbfc10e6d40aea8d283e407d88cf538a05')));
    const x = hex('2c3c6b8c4f3800e7aef6864025b4ed79bd599117e427c41bd47d93d654b4a51c');
    expect(scalarMultVfy(s, x)).toEqual(hex('7c13645fe790a468f62c39beb7388e541d8405d1ade69d1778c5fe3e7f6b600e'));
    expect(scalarMultVfy(s, hex('2b3c6b8c4f3800e7aef6864025b4ed79bd599117e427c41bd47d93d654b4a51c'))).toBeUndefined();
    expect(scalarMultVfy(s, new Uint8Array(32))).toBeUndefined();
  });

  it('gives different keys for different passwords', () => {
    const empty = new Uint8Array(0);
    const a = new Cpace(utf8ToBytes('ABCD1234'), utf8ToBytes('ci'), empty);
    const b = new Cpace(utf8ToBytes('ABCD1235'), utf8ToBytes('ci'), empty);
    const ka = a.finish(empty, empty, b.share, empty, true)!;
    const kb = b.finish(empty, empty, a.share, empty, false)!;
    expect(ka).not.toEqual(kb);
    const c = new Cpace(utf8ToBytes('ABCD1234'), utf8ToBytes('ci'), empty);
    const d = new Cpace(utf8ToBytes('ABCD1234'), utf8ToBytes('ci'), empty);
    expect(c.finish(empty, empty, d.share, empty, true)).toEqual(d.finish(empty, empty, c.share, empty, false));
  });
});
