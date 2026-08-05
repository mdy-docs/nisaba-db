/**
 * keyenc.test.js — the composite-key encoding, through the JS surface.
 *
 * orderedKey/compositeKey/compositeUpperBound used to be a pure-JS
 * implementation of the encoding that keyenc.c also implements in C: two
 * encoders that had to agree byte-for-byte forever, with nothing checking
 * that they did. They are now marshalling over the C one.
 *
 * Every expectation below is a byte string produced by running the
 * ORIGINAL pure-JS implementation. So this file is the record that the
 * de-duplication changed nothing observable -- including nothing on disk,
 * since these bytes are the keys of every secondary index file ever
 * written by this package. test/native/main.c asserts the identical
 * vectors one layer down, against the C directly.
 *
 * Before this file, nothing in the repo called these three functions at
 * all: they were public API with no coverage, which is exactly how two
 * implementations drift apart unnoticed.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import {
  ready, orderedKey, compositeKey, compositeUpperBound
} from '../src/nisaba-wasm.js';

const hex = (bytes) => Buffer.from(bytes).toString('hex');

beforeAll(async () => { await ready(); });

describe('keyenc: orderedKey', () => {
  it('encodes numbers exactly as the original JS encoder did', () => {
    expect(hex(orderedKey(0))).toBe('008000000000000000');
    expect(hex(orderedKey(1))).toBe('00bff0000000000000');
    expect(hex(orderedKey(-1))).toBe('00400fffffffffffff');
    expect(hex(orderedKey(3.5))).toBe('00c00c000000000000');
    expect(hex(orderedKey(-3.5))).toBe('003ff3ffffffffffff');
    expect(hex(orderedKey(1e300))).toBe('00fe37e43c8800759c');
    expect(hex(orderedKey(-1e300))).toBe('0001c81bc377ff8a63');
    expect(hex(orderedKey(9007199254740991))).toBe('00c33fffffffffffff');
    expect(hex(orderedKey(5e-324))).toBe('008000000000000001');
  });

  it('normalizes -0 to +0 so they encode identically', () => {
    expect(hex(orderedKey(-0))).toBe(hex(orderedKey(0)));
  });

  it('encodes Infinity at the extremes of the number range', () => {
    expect(hex(orderedKey(Infinity))).toBe('00fff0000000000000');
    expect(hex(orderedKey(-Infinity))).toBe('00000fffffffffffff');
  });

  it('encodes strings as tag + UTF-8 + NUL terminator', () => {
    expect(hex(orderedKey(''))).toBe('0100');
    expect(hex(orderedKey('a'))).toBe('016100');
    expect(hex(orderedKey('core'))).toBe('01636f726500');
    expect(hex(orderedKey('héllo'))).toBe('0168c3a96c6c6f00');
    expect(hex(orderedKey('😀'))).toBe('01f09f988000');
  });

  it('rejects values with no ordering', () => {
    expect(() => orderedKey(NaN)).toThrow(/no order-preserving encoding/);
    expect(() => orderedKey('a\u0000b')).toThrow(/no order-preserving encoding/);
  });

  it('rejects unsupported part types', () => {
    expect(() => orderedKey(null)).toThrow(/unsupported part type/);
    expect(() => orderedKey({})).toThrow(/unsupported part type/);
    expect(() => orderedKey(true)).toThrow(/unsupported part type/);
  });

  it('sorts byte-wise in the same order the values sort numerically', () => {
    const ascending = [-1e300, -3.5, -1, 0, 5e-324, 1, 3.5, 1e300];
    const encoded = ascending.map((n) => hex(orderedKey(n)));
    expect([...encoded].sort()).toEqual(encoded);
  });

  it('sorts every number before every string', () => {
    expect(hex(orderedKey(1e300)) < hex(orderedKey(''))).toBe(true);
  });
});

describe('keyenc: compositeKey', () => {
  it('concatenates parts exactly as the original JS encoder did', () => {
    expect(hex(compositeKey('core', 36))).toBe('01636f72650000c042000000000000');
  });

  it('is the concatenation of its parts encoded singly', () => {
    expect(hex(compositeKey('core', 36)))
      .toBe(hex(orderedKey('core')) + hex(orderedKey(36)));
  });

  it('builds an empty key from no parts', () => {
    expect(hex(compositeKey())).toBe('');
  });

  it('reuses one builder without leaking state between calls', () => {
    // The C side keeps a single reused buffer, rewound per call; a missed
    // reset would make each key the concatenation of all keys before it.
    const first = hex(compositeKey('core', 36));
    for (let i = 0; i < 50; i++) compositeKey('other', i);
    expect(hex(compositeKey('core', 36))).toBe(first);
  });
});

describe('keyenc: compositeUpperBound', () => {
  it('appends the 0xff sentinel', () => {
    expect(hex(compositeUpperBound('core'))).toBe('01636f726500ff');
  });

  it('sorts after every key extending the prefix', () => {
    const bound = hex(compositeUpperBound('core'));
    for (const suffix of ['', 'a', 'zzz', '\u{1F600}']) {
      expect(hex(compositeKey('core', suffix)) < bound).toBe(true);
    }
    for (const n of [-1e300, 0, 1e300]) {
      expect(hex(compositeKey('core', n)) < bound).toBe(true);
    }
  });

  it('sorts before the next distinct prefix value', () => {
    expect(hex(compositeUpperBound('core')) < hex(compositeKey('cores'))).toBe(true);
  });
});
