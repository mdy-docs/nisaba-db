/**
 * extended-json.js — how JSON-facing surfaces spell the wire values JSON
 * cannot: MongoDB Extended JSON's {$oid} / {$date} / {$binary} literals.
 *
 * ONE OWNER. The convention appeared first in bin/db.js (its JSON
 * arguments and its dump/restore pair) and the HTTP front end speaks it
 * too; this file is the single copy both read, so the CLI and HTTP can
 * never disagree about what {"$oid": …} means. It converts, and nothing
 * else — no parsing policy, no error handling opinions; both belong to
 * the surface that has the user.
 *
 * The three literals are exactly the wire types plain JSON loses:
 *
 *     {"$oid":    "<24 hex chars>"}                 ObjectId
 *     {"$date":   "<ISO 8601>"}                     Date
 *     {"$binary": {"base64": "…", "subType": "00"}} Uint8Array
 *
 * A round trip is byte-identical: toExtendedJson's output restores
 * through fromExtendedJson to the same values, which is what `db dump`
 * and `db restore` rest on.
 */
import { ObjectId } from '../third_party/binjson/js/binjson.js';

/* Base64 without assuming a platform: this file started life behind
 * bin/db.js and the HTTP front end, where Buffer is a given, but the
 * HTTP CLIENT (db-http-client.js) puts it in a browser, where it is
 * not. Buffer when it exists, atob/btoa when it does not — chunked,
 * because String.fromCharCode over a whole file's bytes is a stack
 * overflow, not a conversion. */
function bytesToBase64(bytes) {
  if (typeof Buffer !== 'undefined') return Buffer.from(bytes).toString('base64');
  let s = '';
  for (let i = 0; i < bytes.length; i += 0x8000) {
    s += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
  }
  return btoa(s);
}
function base64ToBytes(b64) {
  if (typeof Buffer !== 'undefined') return new Uint8Array(Buffer.from(b64, 'base64'));
  const s = atob(b64);
  const out = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
  return out;
}

/** Extended JSON literals to wire values, recursively; everything else
 * is itself. Throws on a malformed literal (a bad $oid hex string), as
 * ObjectId does — saying which is the caller's job. */
export function fromExtendedJson(value) {
  if (Array.isArray(value)) return value.map(fromExtendedJson);
  if (value !== null && typeof value === 'object') {
    const keys = Object.keys(value);
    if (keys.length === 1 && keys[0] === '$oid' && typeof value.$oid === 'string') {
      return new ObjectId(value.$oid);
    }
    if (keys.length === 1 && keys[0] === '$date' && typeof value.$date === 'string') {
      return new Date(value.$date);
    }
    if (keys.length === 1 && keys[0] === '$binary' && value.$binary && typeof value.$binary.base64 === 'string') {
      return base64ToBytes(value.$binary.base64);
    }
    const out = {};
    for (const k of keys) out[k] = fromExtendedJson(value[k]);
    return out;
  }
  return value;
}

/** Wire values to Extended JSON literals, recursively. Spelled as a walk
 * because JSON.stringify consults toJSON before any replacer sees the
 * instance — ObjectId's toJSON says a bare hex string a reader could not
 * tell from one the document happened to contain. */
export function toExtendedJson(value) {
  if (value instanceof ObjectId || (value && typeof value.toHexString === 'function')) {
    return { $oid: value.toHexString() };
  }
  if (value instanceof Date) return { $date: value.toISOString() };
  if (value instanceof Uint8Array) return { $binary: { base64: bytesToBase64(value), subType: '00' } };
  if (Array.isArray(value)) return value.map(toExtendedJson);
  if (value !== null && typeof value === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(value)) out[k] = toExtendedJson(v);
    return out;
  }
  return value;
}
