/**
 * nisaba-wasm.js — standalone WASM-backed document database engine,
 * buildable and usable independently of the rest of the parent project
 * (see build-wasm.sh in this directory).
 *
 * This file owns this package's combined binary (build/lib), the module
 * lifecycle (ready/requireModule), a self-contained copy of the binjson
 * codec bound to that binary, the host I/O bridge (bridgeHandle), and
 * the Db/Collection/ChangeStream/StorageProvider/Client layer. The
 * structure wrapper classes (BPlusTree/RTree/TextLog/EntryLog/
 * SnapshotStore/TextIndex, diff, stemmer) are NOT copied here: they
 * come from the binjson-structures submodule's structures-core.js,
 * bound to this package's own module via bindStructures() below. The
 * binary is combined because db.c's CRUD functions call bplustree/
 * rtree/textindex functions directly (real link-time calls, not just
 * shared headers), so the wrapper classes must run against this exact
 * same WASM module instance -- a BPlusTree opened against a *different*
 * module's linear memory can't be attached to this module's
 * dc_collection as a secondary index.
 *
 * Depends on binjson (this package's own third_party/binjson submodule),
 * used here purely for its JS value types (ObjectId/Pointer/TYPE) and
 * OPFS helpers (MemoryHandle/exists/deleteFile/getFileHandle) -- this
 * module keeps its own self-contained copy of the codec (bound to its
 * own binary) rather than importing another package's WASM instance,
 * same reasoning third_party/binjson/wasm/binjson-wasm.js documents.
 *
 * The cloud SaaS layer that runs this as a service (control plane,
 * REST/WebSocket gateways, the MongoClient-shaped driver) is NOT part
 * of this package -- it lives in the parent project's service/ and
 * client/ directories, built on top of the Db/Client this file exports,
 * the same way any other application would consume it.
 *
 * The WASM module loads asynchronously; call and await `ready()` once
 * before using any of these synchronously-shaped APIs.
 */
import createModule from '../build/lib/nisaba.wasm.mjs';
import {
  TYPE,
  ObjectId,
  Pointer,
  MemoryHandle,
  exists,
  deleteFile,
  getFileHandle
} from '../third_party/binjson/js/binjson.js';
import { bindStructures } from '../third_party/binjson-structures/wasm/structures-core.js';

// Event tags — must match the BJW_EV_* constants in c/binjson_wasm.c.
const EV = {
  NULL: 0, FALSE: 1, TRUE: 2, INT: 3, FLOAT: 4, STRING: 5, OID: 6,
  DATE: 7, POINTER: 8, BINARY: 9, ARR_BEGIN: 10, ARR_END: 11,
  OBJ_BEGIN: 12, KEY: 13, OBJ_END: 14
};

// Error messages come from C (dc_strerror in engine/src/db_validate.c).
// They used to be a literal map here, under a comment reading "must match
// the BJ_ERR_* constants in c/binjson.h" -- a hand-maintained second copy
// of C's own error vocabulary, kept in sync by nothing.
//
// Memoized because a message is fetched on every thrown error, and the
// text for a given code cannot change within a process.
const errTextCache = new Map();

function errText(code) {
  let text = errTextCache.get(code);
  if (text === undefined) {
    if (!Module) return `error ${code}`;   // pre-ready: don't mask the real failure
    const len = Module._dvw_strerror_len(code);
    const ptr = Module._dvw_strerror(code);
    text = textDecoder.decode(Module.HEAPU8.slice(ptr, ptr + len));
    errTextCache.set(code, text);
  }
  return text;
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

let Module = null;
let readyPromise = null;

/**
 * Instantiate the WASM module. Idempotent; returns a promise that resolves when
 * encode/decode are usable. Must be awaited before the first encode/decode.
 */
function ready() {
  if (!readyPromise) {
    readyPromise = createModule().then((m) => { Module = m; return m; });
  }
  return readyPromise;
}

/** True once the module is instantiated and encode/decode may be called. */
function isReady() {
  return Module !== null;
}

function requireModule() {
  if (!Module) {
    throw new Error('binjson-wasm not initialized: await ready() before encode/decode');
  }
  return Module;
}

/**
 * Base class for every coded error this module raises (docs/roadmap.md
 * P0 #2): `code` carries the C-side error code (the ERR map above), and
 * `name` identifies the class -- so callers branch on `err.code` or
 * `err.name` instead of matching message strings. Errors proxied across
 * the coordinator's RPC wire keep their `code`/`name` (rebuilt on the
 * follower -- see db-coordinator.js) but not their prototype: test with
 * `err.name`, not `instanceof`, when the error may have crossed tabs.
 */
class NisabaError extends Error {
  constructor(message, code = 0) {
    super(message);
    this.name = new.target.name;
    this.code = code;
  }
}
/** Duplicate _id (-10) or unique-index violation (-12). */
class DuplicateKeyError extends NisabaError {}
/** Document lacks a field a non-sparse index requires (-13). */
class MissingIndexedFieldError extends NisabaError {}
/** Indexed field value can't be key-encoded (-14). */
class UnindexableValueError extends NisabaError {}
/** A ChangeStream's iterator buffer overflowed (see ChangeStream). */
class ChangeStreamOverflowError extends NisabaError {}
/** `_id` isn't an ObjectId (see toObjectId). Unlike MongoDB, scalar _ids
 * (numbers, arbitrary strings, Dates) are not supported: the on-disk
 * format keys the primary tree and every index back-pointer with fixed
 * 12-byte OIDs (db.c's dc_get_id/oid_key), so lifting the restriction is
 * a format-version bump with a migration, not a validation tweak
 * (docs/roadmap.md P0 #3 records the spike). Keep natural keys in their
 * own field with a unique index instead. */
class InvalidIdError extends NisabaError {}
/** A collection or database name broke the rules in db_validate.h
 * (-15/-16 malformed, -17 the reserved format-stamp key). */
class InvalidNameError extends NisabaError {}
/** A createIndex key spec was empty (-18) or named a non-ascending
 * field (-19). */
class InvalidIndexSpecError extends NisabaError {}

/**
 * code -> class for codeError; anything unlisted raises plain NisabaError.
 *
 * This stays in JavaScript deliberately, unlike the message text: it is a
 * JavaScript type taxonomy, not a database rule, and C has no opinion
 * about which of its codes deserve their own subclass here.
 */
const ERR_CLASS = {
  [-10]: DuplicateKeyError,
  [-12]: DuplicateKeyError,
  [-13]: MissingIndexedFieldError,
  [-14]: UnindexableValueError,
  [-15]: InvalidNameError,
  [-16]: InvalidNameError,
  [-17]: InvalidNameError,
  [-18]: InvalidIndexSpecError,
  [-19]: InvalidIndexSpecError,
  // db.h's DC_ERR_UNSUPPORTED_ID: an upsert filter pinning a non-ObjectId
  // _id is the same complaint toObjectId makes about a document's, so it
  // arrives as the same error class.
  [-35]: InvalidIdError
};

function codeError(code, context) {
  const msg = errText(code);
  const cls = ERR_CLASS[code] || NisabaError;
  const error = new cls(context ? `${msg} (${context})` : msg, code);
  if (bjioLastError) {
    // A bridged handle swallowed a real exception (bridgeHandle) and the
    // operation then failed: that exception is the actual story.
    error.cause = bjioLastError;
    bjioLastError = null;
  }
  return error;
}

function check(code) {
  if (code !== 0) throw codeError(code);
}

/**
 * Is `err` a DETERMINISTIC command failure -- one every replica applying
 * the same command against the same state would reach -- rather than
 * this replica alone failing?
 *
 * A replicated apply loop rests on the distinction: a deterministic
 * failure is a result to report, anything else is divergence and the
 * node must stop rather than skip an entry and fork the state.
 *
 * Three conditions, all required. It must carry a numeric code, C must
 * classify that code as deterministic (db_validate.h -- an allowlist,
 * because an unclassified code has to be presumed divergence), and it
 * must carry no `cause`: a cause means a bridged host exception was
 * swallowed into this error (bridgeHandle turns a storage failure into a
 * short write so C can unwind), and the storage failing is exactly the
 * kind of thing one replica does alone.
 */
function isDeterministicError(err) {
  if (!err || typeof err.code !== 'number' || err.code === 0) return false;
  if (err.cause) return false;
  return requireModule()._dvw_is_deterministic(err.code) === 1;
}

/**
 * Copy `bytes` into the WASM heap, invoke `fn(ptr, len)`, then free. The C
 * builder copies immediately, so the scratch allocation is safe to release.
 */
function withBytes(M, bytes, fn) {
  const n = bytes.length;
  const ptr = n ? M._malloc(n) : 0;
  if (n) M.HEAPU8.set(bytes, ptr);
  try {
    return fn(ptr, n);
  } finally {
    if (n) M._free(ptr);
  }
}

function writeValue(M, val) {
  if (val === null) { check(M._bjw_put_null()); return; }
  if (val === false) { check(M._bjw_put_bool(0)); return; }
  if (val === true) { check(M._bjw_put_bool(1)); return; }

  if (val instanceof ObjectId || (val && typeof val.toBytes === 'function' && typeof val.toHexString === 'function')) {
    withBytes(M, val.toBytes(), (p) => check(M._bjw_put_oid(p)));
    return;
  }
  if (val instanceof Date) { check(M._bjw_put_date(val.getTime())); return; }
  if (val instanceof Pointer) { check(M._bjw_put_pointer(val.offset)); return; }
  if (val instanceof Uint8Array) {
    withBytes(M, val, (p, n) => check(M._bjw_put_binary(p, n)));
    return;
  }

  const t = typeof val;
  if (t === 'number') {
    if (Number.isInteger(val) && Number.isSafeInteger(val)) check(M._bjw_put_int(val));
    else check(M._bjw_put_float(val));
    return;
  }
  if (t === 'string') {
    withBytes(M, textEncoder.encode(val), (p, n) => check(M._bjw_put_string(p, n)));
    return;
  }
  if (Array.isArray(val)) {
    check(M._bjw_begin_array());
    for (const item of val) writeValue(M, item);
    check(M._bjw_end_array());
    return;
  }
  if (t === 'object') {
    check(M._bjw_begin_object());
    for (const key of Object.keys(val)) {
      withBytes(M, textEncoder.encode(key), (p, n) => check(M._bjw_put_key(p, n)));
      writeValue(M, val[key]);
    }
    check(M._bjw_end_object());
    return;
  }
  throw new Error(`Unsupported type: ${t}`);
}

/**
 * Encode a JavaScript value to binjson binary format.
 * @returns {Uint8Array}
 */
function encode(value) {
  const M = requireModule();
  check(M._bjw_enc_reset());
  writeValue(M, value);
  const len = M._bjw_enc_finish();
  if (len < 0) throw codeError(len, 'encode');
  const ptr = M._bjw_enc_ptr();
  // Copy out: the builder buffer is reused on the next encode call.
  return M.HEAPU8.slice(ptr, ptr + len);
}

/** Rebuild a JS value from the flat event stream emitted by the C decoder. */
function readEvents(M, ptr, len) {
  const heap = M.HEAPU8;
  const dv = new DataView(heap.buffer, heap.byteOffset, heap.byteLength);
  const stack = [];
  let root;
  let off = ptr;
  const end = ptr + len;

  const emit = (v) => {
    if (stack.length === 0) { root = v; return; }
    const top = stack[stack.length - 1];
    if (top.isObject) { top.value[top.key] = v; top.key = undefined; }
    else top.value.push(v);
  };

  while (off < end) {
    const tag = heap[off++];
    switch (tag) {
      case EV.NULL: emit(null); break;
      case EV.FALSE: emit(false); break;
      case EV.TRUE: emit(true); break;
      case EV.INT: emit(dv.getFloat64(off, true)); off += 8; break;
      case EV.FLOAT: emit(dv.getFloat64(off, true)); off += 8; break;
      case EV.DATE: emit(new Date(dv.getFloat64(off, true))); off += 8; break;
      case EV.POINTER: emit(new Pointer(dv.getFloat64(off, true))); off += 8; break;
      case EV.STRING: {
        const n = dv.getUint32(off, true); off += 4;
        emit(textDecoder.decode(heap.subarray(off, off + n))); off += n;
        break;
      }
      case EV.KEY: {
        const n = dv.getUint32(off, true); off += 4;
        stack[stack.length - 1].key = textDecoder.decode(heap.subarray(off, off + n));
        off += n;
        break;
      }
      case EV.BINARY: {
        const n = dv.getUint32(off, true); off += 4;
        emit(heap.slice(off, off + n)); off += n;
        break;
      }
      case EV.OID: {
        emit(new ObjectId(heap.slice(off, off + 12))); off += 12;
        break;
      }
      case EV.ARR_BEGIN: off += 4; stack.push({ isObject: false, value: [] }); break;
      case EV.OBJ_BEGIN: off += 4; stack.push({ isObject: true, value: {}, key: undefined }); break;
      case EV.ARR_END:
      case EV.OBJ_END: emit(stack.pop().value); break;
      default: throw new Error(`binjson: bad event tag ${tag}`);
    }
  }
  return root;
}

/**
 * Decode binjson binary data to a JavaScript value.
 * @param {Uint8Array|ArrayBuffer} data
 */
function decode(data) {
  const M = requireModule();
  const u8 = data instanceof Uint8Array ? data : new Uint8Array(data);
  const n = u8.length;
  const inPtr = n ? M._malloc(n) : 0;
  if (n) M.HEAPU8.set(u8, inPtr);

  let rc;
  try {
    rc = M._bjw_decode(inPtr, n);
  } finally {
    if (n) M._free(inPtr);
  }
  if (rc !== 0) throw codeError(rc, 'decode');

  const evPtr = M._bjw_events_ptr();
  const evLen = M._bjw_events_len();
  return readEvents(M, evPtr, evLen);
}

/**
 * Total on-wire size of the value whose leading bytes are `header`, computed by
 * the C codec (bj_value_size). `header` only needs the type byte plus, for
 * length-prefixed/container types, the 4-byte size field (i.e. up to 5 bytes).
 */
function wasmValueSize(M, header) {
  const n = header.length;
  const inPtr = M._malloc(n + 4);
  M.HEAPU8.set(header, inPtr);
  const outPtr = inPtr + n;
  const rc = M._bjw_value_size(inPtr, n, 0, outPtr);
  let size = 0;
  if (rc === 0) {
    size = new DataView(M.HEAPU8.buffer).getUint32(outPtr, true);
  }
  M._free(inPtr);
  if (rc !== 0) throw codeError(rc, 'value_size');
  return size;
}

/**
 * On-wire size (in bytes) of the top-level value whose leading bytes are
 * `header`, computed by the C codec. `header` only needs the type byte plus,
 * for length-prefixed/container types, the 4-byte size field (i.e. up to 5
 * bytes). Await ready() before calling. Useful for scanning append-only files
 * of concatenated records without decoding each one.
 */
function valueSize(header) {
  const M = requireModule();
  return wasmValueSize(M, header instanceof Uint8Array ? header : new Uint8Array(header));
}

/**
 * OPFS-backed file using a FileSystemSyncAccessHandle, with the binjson codec
 * running in WASM. Byte-level work (encode/decode + scan record sizing) is done
 * in C; only the raw synchronous handle calls (read/write/truncate/getSize/
 * flush) — which are browser APIs with no WASM equivalent — stay in JS.
 *
 * As with the reference, this requires FileSystemSyncAccessHandle (Web Workers)
 * and the WASM module to be initialized (await ready() first).
 */
class BinJsonFile {
  constructor(syncAccessHandle) {
    if (!syncAccessHandle) {
      throw new Error('FileSystemSyncAccessHandle is required');
    }
    this.syncAccessHandle = syncAccessHandle;
  }

  /** Read a range of bytes, returning only what was actually read. */
  #readRange(start, length) {
    const buffer = new Uint8Array(length);
    const bytesRead = this.syncAccessHandle.read(buffer, { at: start });
    return bytesRead < length ? buffer.slice(0, bytesRead) : buffer;
  }

  getFileSize() {
    return this.syncAccessHandle.getSize();
  }

  /** Encode and write `data`, replacing any existing content. */
  write(data) {
    const binaryData = encode(data);
    this.syncAccessHandle.truncate(0);
    this.syncAccessHandle.write(binaryData, { at: 0 });
  }

  /** Read and decode the value at `pointer` (default: start of file). */
  read(pointer = new Pointer(0)) {
    const fileSize = this.getFileSize();
    if (fileSize === 0) {
      throw new Error('File is empty');
    }
    const pointerValue = pointer.valueOf();
    if (pointerValue < 0 || pointerValue >= fileSize) {
      throw new Error(`Pointer offset ${pointer} out of file bounds [0, ${fileSize})`);
    }
    const binaryData = this.#readRange(pointerValue, fileSize - pointerValue);
    return decode(binaryData);
  }

  /** Encode and append `data` without truncating existing content. */
  append(data) {
    const binaryData = encode(data);
    const existingSize = this.getFileSize();
    this.syncAccessHandle.write(binaryData, { at: existingSize });
  }

  flush() {
    this.syncAccessHandle.flush();
  }

  /**
   * Yield each top-level record in the file, decoded one at a time as
   * `{ value, offset, size }`, where `offset` is the record's byte position in
   * the file and `size` is the number of bytes it occupies.
   */
  *scan() {
    const fileSize = this.getFileSize();
    if (fileSize === 0) return;

    const M = requireModule();
    let offset = 0;
    while (offset < fileSize) {
      // The value-size header needs at most type byte + 4-byte length field.
      const headerLen = Math.min(5, fileSize - offset);
      const header = this.#readRange(offset, headerLen);
      const valueSize = wasmValueSize(M, header);

      const valueData = this.#readRange(offset, valueSize);
      const valueOffset = offset;
      offset += valueSize;
      yield { value: decode(valueData), offset: valueOffset, size: valueSize };
    }
  }
}

// ---------------------------------------------------------------------------
// Shared helpers: the host I/O bridge injected into the structure classes
// (bindStructures below) and the heap marshalling the Db layer uses.
// ---------------------------------------------------------------------------

// Short aliases used throughout the marshalling helpers below.
const encoder = textEncoder;
const decoder = textDecoder;

/**
 * Host I/O registry for the file-resident C structures (c/hostio.c).
 *
 * Each open FileSystemSyncAccessHandle is registered under an integer slot in
 * `Module.bjioHandles`; the C side reads and writes the file through EM_JS
 * imports that index this table and pass HEAPU8 subarray views straight to the
 * handle's synchronous read/write — the bytes move directly between the file
 * and WASM memory with no intermediate copies, and no copy of the file is ever
 * held in memory on either side of the bridge.
 */
let nextBjioFd = 1;

/** The most recent error a bridged handle call swallowed (see
 * bridgeHandle); codeError attaches it to the next error it builds as
 * `cause`, so a QuotaExceededError surfaces on the operation's own error
 * instead of vanishing behind a generic short-write code. */
let bjioLastError = null;

/**
 * Wrap a sync access handle for the EM_JS bridge (Module.bjioHandles): a
 * JS exception thrown by the handle -- QuotaExceededError mid-write is
 * the realistic one -- must never propagate up through the WASM frames.
 * If it did, C would be abandoned mid-mutation with no chance to run its
 * own error paths (mut_end's in-process rollback in db.c), leaving a
 * phantom document in the live trees that the next successful write
 * would then commit durably (docs/roadmap.md P1 #9 -- observed exactly
 * so before this wrapper existed). Instead every failure is reported
 * through the bridge's existing error contract -- a short read/write,
 * which hostio.c already turns into an error return -- so C unwinds
 * cleanly, rolls the mutation back in-process, and the JS caller gets a
 * normal coded error with the real exception attached as `cause`.
 *
 * getSize is deliberately NOT wrapped: it can't hit quota, and inventing
 * a size of 0 for a file that failed to answer could make recovery
 * rewind against fictional lengths -- a thrown getSize means something
 * catastrophic where an exception is the right outcome.
 */
function bridgeHandle(h) {
  return {
    getSize: () => h.getSize(),
    read(buf, opts) {
      try { return h.read(buf, opts); } catch (err) { bjioLastError = err; return 0; }
    },
    write(buf, opts) {
      try { return h.write(buf, opts); } catch (err) { bjioLastError = err; return 0; }
    },
    truncate(len) {
      // hostio.c's truncate contract is void/best-effort (mut_end's
      // rewind is itself best-effort); record and continue.
      try { h.truncate(len); } catch (err) { bjioLastError = err; }
    },
    // Durability, reached from C through bj_io.sync (bjfile_sync, and
    // through it elog_sync). Always defined here even when the underlying
    // handle has none: a MemoryHandle's writes are already as durable as
    // memory gets, so a no-op is the correct answer for it. Defining it
    // unconditionally is what stops hostio.c's "no flush" branch from
    // silently standing in for a real fsync on a real file.
    //
    // Unlike write(), a failure here is REPORTED rather than swallowed
    // into a short op: the caller asked for durability and did not get it,
    // and elog_sync's return is what Raft's sync-before-ack rests on.
    flush() {
      try { h.flush?.(); return 0; } catch (err) { bjioLastError = err; return -2; }
    }
  };
}

function registerHandle(M, syncHandle) {
  if (!M.bjioHandles) M.bjioHandles = {};
  const fd = nextBjioFd++;
  M.bjioHandles[fd] = bridgeHandle(syncHandle);
  return fd;
}

function unregisterHandle(M, fd) {
  if (M.bjioHandles) delete M.bjioHandles[fd];
}

/** Copy a JS string into the heap as UTF-8; returns { ptr, len, free }. */
function allocStr(M, str) {
  const bytes = textEncoder.encode(str);
  const len = bytes.length;
  const ptr = M._malloc(len || 1);
  if (len) M.HEAPU8.set(bytes, ptr);
  return { ptr, len, free() { M._free(ptr); } };
}

/** Little-endian u32 read from the heap (HEAPU32 isn't exported). */
function readU32(M, addr) {
  const b = M.HEAPU8;
  return (b[addr] | (b[addr + 1] << 8) | (b[addr + 2] << 16) | (b[addr + 3] * 0x1000000)) >>> 0;
}

/** Little-endian u32 write into the heap (HEAPU32 isn't exported), for
 * building the small pointer arrays C takes as array arguments. */
function writeU32(M, addr, v) {
  const b = M.HEAPU8;
  b[addr] = v & 0xff;
  b[addr + 1] = (v >>> 8) & 0xff;
  b[addr + 2] = (v >>> 16) & 0xff;
  b[addr + 3] = (v >>> 24) & 0xff;
}

/** f64 read/write against the heap. HEAPF64 isn't exported either, and a
 * one-element Float64Array view is how the rest of this file already
 * moves doubles across (see raft.commitCandidate). */
function readF64(M, addr) {
  return new Float64Array(M.HEAPU8.slice(addr, addr + 8).buffer)[0];
}

function writeF64(M, addr, v) {
  M.HEAPU8.set(new Uint8Array(new Float64Array([v]).buffer), addr);
}

/** Little-endian signed i32 read from the heap -- for out-params carrying a BJ_ERR_* code (can be negative). */
function readI32(M, addr) {
  return readU32(M, addr) | 0;
}

// ---------------------------------------------------------------------------
// Structure classes -- bound to this package's own combined binary
// ---------------------------------------------------------------------------

// The wrapper classes for the binjson-structures data structures live in
// the submodule (structures-core.js), parameterized over the module they
// run against (see its header for the runtime contract); this package
// binds them to its own combined binary, which links those same C
// sources directly (db.c makes real link-time calls into bplustree/
// rtree/textindex, so there is exactly one binary and one linear
// memory). registerHandle/unregisterHandle are this file's bridging
// versions, so the structure classes get the same QuotaExceededError
// containment as the db layer (see bridgeHandle). codeError is this
// file's, so structure errors surface as NisabaError subclasses with
// bjioLastError attached as cause.
const {
  orderedKey, compositeKey, compositeUpperBound, BPlusTree,
  haversineDistance, RTree, TextLog, TiledTextLog, ENTRY_TYPE,
  EntryLog, ENTRYLOG_TYPE, SnapshotStore, crc32, snapshotCheckFiles, TextIndex,
  stemmer, createPatch, unifiedDiff, applyPatch, createDelta, applyDelta
} = bindStructures({
  ready, requireModule, codeError, check,
  encode, decode, valueSize,
  ObjectId, Pointer, TYPE,
  registerHandle, unregisterHandle
});

// ---------------------------------------------------------------------------
// Db / Collection — a MongoDB-driver-shaped document database.
//
// A collection is a bpt keyed by the raw 12-byte ObjectId, plus zero or more
// attached secondary indexes (see db.h). CRUD, secondary-index maintenance,
// operator-aware filter matching, sort/skip/limit/projection and the
// equality-index planner are all implemented in C (db.c/db_query.c/db_wasm.c)
// — this layer only marshals bytes across the WASM bridge, the same way
// BPlusTree/RTree/TextIndex above do. A database is a root catalog tree
// (collection name -> backing file name + index list) plus a storage
// provider that turns file names into sync-handle-shaped objects; that
// bookkeeping is plain B+ tree key lookups, already fully served by
// BPlusTree, so it stays here rather than growing new C surface. Collection
// only does two bits of real work of its own: the driver-shaped createIndex
// key-spec validation ({field: 1}, ascending only) and default index naming
// (team_1, team_1_age_1) — pure conventions, not query/index logic.
//
// _id defaulting stays in JS for the same reason ts_ms does for textlog: it
// needs a clock and randomness, neither of which WASM has a portable source
// for. replaceOne's upsert case can't know ahead of the C-side match
// whether a fresh id will actually be needed, so JS always generates one and
// passes it as `default_id`; C only consults it when it decides to upsert.
// ---------------------------------------------------------------------------

const DB_DEFAULT_ORDER = 32;

// ---------------------------------------------------------------------------
// File naming and the format stamp: marshalling only.
//
// The scheme itself lives in engine/src/db_names.c (see db_names.h for the
// generation-prefix rationale and docs/format-compatibility.md for what the
// stamp covers). It used to live here, on the argument -- recorded in
// docs/db-plan.md -- that JS must compute a file name before it can open
// the file, so the name can't be learned from the catalog first. That's
// true, and it's an argument for JS *asking* for a name, not for JS
// *owning* the scheme: the catalog, the compaction generation flip and the
// orphan sweep all reason about these names, and all three are C's now (or
// about to be). DB_FORMAT_VERSION in particular was a JS constant stamping
// a C-owned format.
//
// The constants are functions rather than consts because they come from
// the module, which isn't loaded at module-evaluation time.
// ---------------------------------------------------------------------------

let namesCtx = 0;

function namesBuilder(M) {
  if (!namesCtx) {
    namesCtx = M._dnw_new();
    if (!namesCtx) throw codeError(-1, 'dnw_new'); // BJ_ERR_OOM
  }
  return namesCtx;
}

/** Run a C name-builder call and decode the result. Re-reads HEAPU8: the
 *  call may have grown the heap and swapped the ArrayBuffer. */
function takeName(M, rc) {
  check(rc);
  const len = M._dnw_len(namesCtx);
  check(len < 0 ? len : 0);
  const ptr = M._dnw_ptr(namesCtx);
  return textDecoder.decode(M.HEAPU8.slice(ptr, ptr + len));
}

/** Call a C name builder with one or two (string, gen) style arguments. */
function buildName(fn) {
  const M = requireModule();
  const ctx = namesBuilder(M);
  return fn(M, ctx);
}

let formatVersionCache = null;
let catalogFileCache = null;
let formatKeyCache = null;

/** On-disk format version (docs/format-compatibility.md). */
function dbFormatVersion() {
  if (formatVersionCache === null) formatVersionCache = requireModule()._dnw_format_version();
  return formatVersionCache;
}

function dbCatalogFile() {
  if (catalogFileCache === null) {
    catalogFileCache = buildName((M, ctx) => takeName(M, M._dnw_catalog_file(ctx)));
  }
  return catalogFileCache;
}

/** Reserved catalog key holding the format stamp -- not a collection. */
function dbFormatKey() {
  if (formatKeyCache === null) {
    formatKeyCache = buildName((M, ctx) => takeName(M, M._dnw_format_key(ctx)));
  }
  return formatKeyCache;
}

/**
 * Duck-types rather than strict `instanceof ObjectId`: a caller that built
 * its own value against a *different* copy of binjson's ObjectId (same
 * class, different module instance -- e.g. a thin client package that only
 * depends on binjson, not this whole engine) would otherwise fail this
 * check even though it's a perfectly valid 12-byte id, re-wrapped here into
 * this module's own canonical ObjectId via the hex round-trip. Mirrors how
 * the real MongoDB driver tolerates cross-realm/cross-copy ObjectId values.
 */
function toObjectId(id) {
  if (id instanceof ObjectId) return id;
  if (typeof id === 'string' && /^[0-9a-fA-F]{24}$/.test(id)) return new ObjectId(id);
  if (id && typeof id.toHexString === 'function') return new ObjectId(id.toHexString());
  throw new InvalidIdError(
    `Invalid _id: ${JSON.stringify(id)} -- _id must be an ObjectId (or its 24-hex string). ` +
    'Unlike MongoDB, scalar _ids (numbers, arbitrary strings, Dates) are not supported by the ' +
    'on-disk format; keep natural keys in their own field with a unique index: ' +
    "createIndex({ field: 1 }, { unique: true }). See docs/db-api.md."
  );
}

// Name and key-spec validation lives in engine/src/db_validate.c. Only the
// JS-type gate stays here: C is handed bytes, so "is this even a string"
// is a question only JavaScript can ask. Everything past that -- empty,
// contains '/', contains NUL, is the reserved format-stamp key -- is C's.
//
// The offending value rides along as codeError's `context`, so messages
// keep naming what was rejected without the message text living here.

function checkCollectionName(name) {
  if (typeof name !== 'string') throw codeError(-15, JSON.stringify(name));
  const M = requireModule();
  const s = allocStr(M, name);
  try {
    const rc = M._dvw_check_collection_name(s.ptr, s.len);
    if (rc !== 0) throw codeError(rc, JSON.stringify(name));
  } finally { s.free(); }
}

/** Same constraints as a collection name, minus the reserved-key rule -- a
 * database name becomes a real path segment (OPFSStorageProvider
 * .subProvider) or a Map key (MemoryStorageProvider.subProvider) either
 * way. Hosts may add their own rules on top; NodeFSStorageProvider also
 * rejects '\' and '..' because its names become real filesystem paths. */
function checkDbName(name) {
  if (typeof name !== 'string') throw codeError(-16, JSON.stringify(name));
  const M = requireModule();
  const s = allocStr(M, name);
  try {
    const rc = M._dvw_check_db_name(s.ptr, s.len);
    if (rc !== 0) throw codeError(rc, JSON.stringify(name));
  } finally { s.free(); }
}

/** Compaction generations: see db_names.h and docs/compaction.md. */
function collectionFileName(name, gen = 0) {
  return buildName((M, ctx) => {
    const s = allocStr(M, name);
    try { return takeName(M, M._dnw_collection_file(ctx, s.ptr, s.len, gen)); }
    finally { s.free(); }
  });
}

function indexFileName(collectionName, indexName, gen = 0) {
  return buildName((M, ctx) => {
    const c = allocStr(M, collectionName);
    const i = allocStr(M, indexName);
    try { return takeName(M, M._dnw_index_file(ctx, c.ptr, c.len, i.ptr, i.len, gen)); }
    finally { i.free(); c.free(); }
  });
}

/** A text index needs the same three files a TextIndex always does.
 *  Roles must match dc_text_role in db_names.h. */
function textIndexFileNames(collectionName, indexName, gen = 0) {
  const one = (role) => buildName((M, ctx) => {
    const c = allocStr(M, collectionName);
    const i = allocStr(M, indexName);
    try { return takeName(M, M._dnw_text_index_file(ctx, c.ptr, c.len, i.ptr, i.len, gen, role)); }
    finally { i.free(); c.free(); }
  });
  return { index: one(0), docTerms: one(1), docLengths: one(2) };
}

/** Cross-file commit journal (milestone 5, docs/db-plan.md): makes every
 * document write atomic across the primary tree + attached index files.
 * A journal's recorded lengths are only meaningful for the exact files it
 * was written against (docs/textindex-atomicity.md), so compact() gives
 * each generation its own journal, recorded in the catalog entry as
 * `journal`; readers fall back to the gen-0 name for entries written
 * before that field existed. */
function journalFileName(collectionName, gen = 0) {
  return buildName((M, ctx) => {
    const s = allocStr(M, collectionName);
    try { return takeName(M, M._dnw_journal_file(ctx, s.ptr, s.len, gen)); }
    finally { s.free(); }
  });
}

/** True for any file name this layer can create for itself -- any
 * generation of a collection/index/journal file. What Db.open()'s orphan
 * sweep is allowed to delete when the catalog doesn't reference it;
 * deliberately excludes the catalog file and anything a host put in the
 * same directory. */
function isDbFile(name) {
  const M = requireModule();
  const s = allocStr(M, name);
  try {
    const rc = M._dnw_is_db_file(s.ptr, s.len);
    check(rc < 0 ? rc : 0);
    return rc === 1;
  } finally { s.free(); }
}

/** Default index name mirroring the real driver's convention: "team_1",
 * "team_1_age_1" for a compound index. Only ascending (1) fields are
 * supported so far — descending order only changes scan direction, which a
 * caller can already get by reversing results, so it's deferred rather than
 * plumbed through the composite-key encoding (keyenc.h) for no behavioral
 * gain yet. */
let catalogCtx = 0;

/* Namespace scope ids for bjns_bridge.c: one per open Db, so two
 * databases in one module never share a pre-opened-name table or a
 * pending-delete queue. Monotonic and never reused, like the fd counter,
 * so a stale reference can only miss -- never hit the wrong scope. */
let nextNsScope = 1;

/** Run a pure catalog-schema call and decode its result. */
function catalogCall(fn) {
  const M = requireModule();
  if (!catalogCtx) {
    catalogCtx = M._catw_new();
    if (!catalogCtx) throw codeError(-1, 'catw_new');
  }
  const rc = fn(M, catalogCtx);
  if (rc !== 0) throw codeError(rc, 'catalog');
  const len = M._catw_len(catalogCtx);
  check(len < 0 ? len : 0);
  const ptr = M._catw_ptr(catalogCtx);
  return decode(M.HEAPU8.slice(ptr, ptr + len));
}

/**
 * The plan for opening a collection: which files, in which order, with
 * everything needed to attach each one. Pure -- no I/O -- which is what
 * lets a host whose opens are asynchronous compute it first and then open
 * exactly the named files (bjns.h).
 *
 * The catalog entry schema lives in C (db_catalog.h) because it was
 * defined in three JS places that could not consult each other:
 * _persistIndexes wrote it, _open parsed it, listIndexes projected it.
 */
/**
 * Plan a new index: kind, name and the files to create. Pure -- it names
 * files, it does not create them, so this side creates exactly what it
 * named. The naming convention lives with listIndexes' reconstruction of
 * `key`, which is the other half of the same convention (db_catalog.h).
 */
function indexCreatePlan(keys, options, collName) {
  return catalogCall((M, ctx) => {
    const ke = encode(keys), oe = encode(options || {});
    const kp = M._malloc(ke.length || 1);
    const op = M._malloc(oe.length || 1);
    const n = allocStr(M, collName);
    try {
      if (ke.length) M.HEAPU8.set(ke, kp);
      if (oe.length) M.HEAPU8.set(oe, op);
      return M._catw_create_plan(ctx, kp, ke.length, op, oe.length, n.ptr, n.len);
    } finally { n.free(); M._free(op); M._free(kp); }
  });
}

function catalogOpenPlan(entry, collName) {
  return catalogCall((M, ctx) => {
    const enc = encode(entry);
    const ep = M._malloc(enc.length || 1);
    const n = allocStr(M, collName);
    try {
      if (enc.length) M.HEAPU8.set(enc, ep);
      return M._catw_open_plan(ctx, ep, enc.length, n.ptr, n.len);
    } finally { n.free(); M._free(ep); }
  });
}

let bulkCtx = 0;

/**
 * Run a bulkWrite against `target` -- any object with the collection
 * write methods, so both Collection and WalCollection share this one loop
 * instead of the two near-identical copies they used to carry
 * (src/db-wal.js's own comment: "Same loop as the inner bulkWrite").
 *
 * The grammar -- which operation names exist, which fields each requires
 * -- is C's (db_bulk.h), validated for the whole list before anything
 * runs. That ordering matters for more than tidiness: an unordered
 * bulkWrite is supposed to attempt every operation, which it cannot do if
 * operation seven is malformed in a way that only surfaces after one
 * through six have already been applied.
 *
 * The loop itself stays here because it is orchestration over async
 * public methods, which is exactly the layer JavaScript should own.
 */
async function runBulkWrite(target, operations, ordered) {
  const M = requireModule();
  if (!bulkCtx) {
    bulkCtx = M._bkw_new();
    if (!bulkCtx) throw codeError(-1, 'bkw_new');
  }
  const encoded = encode(operations);
  const ptr = M._malloc(encoded.length || 1);
  let types;
  try {
    if (encoded.length) M.HEAPU8.set(encoded, ptr);
    const rc = M._bkw_parse(bulkCtx, ptr, encoded.length);
    if (rc !== 0) {
      const at = M._bkw_bad_index(bulkCtx);
      throw codeError(rc, at >= 0 ? `operation ${at}` : 'bulkWrite');
    }
    const len = M._bkw_len(bulkCtx);
    check(len < 0 ? len : 0);
    types = decode(M.HEAPU8.slice(M._bkw_ptr(bulkCtx), M._bkw_ptr(bulkCtx) + len));
  } finally { M._free(ptr); }

  const result = {
    acknowledged: true, insertedCount: 0, matchedCount: 0, modifiedCount: 0,
    deletedCount: 0, upsertedCount: 0, insertedIds: {}, upsertedIds: {}
  };
  const errors = [];
  for (let i = 0; i < operations.length; i++) {
    const spec = Object.values(operations[i])[0];
    try {
      switch (types[i]) {
        case 0: {  // DC_BULK_INSERT_ONE
          const { insertedId } = await target.insertOne(spec.document);
          result.insertedIds[i] = insertedId;
          result.insertedCount++;
          break;
        }
        case 1:    // DC_BULK_UPDATE_ONE
        case 2:    // DC_BULK_UPDATE_MANY
        case 3: {  // DC_BULK_REPLACE_ONE
          const r = types[i] === 1
            ? await target.updateOne(spec.filter, spec.update, { upsert: spec.upsert })
            : types[i] === 2
              ? await target.updateMany(spec.filter, spec.update, { upsert: spec.upsert })
              : await target.replaceOne(spec.filter, spec.replacement, { upsert: spec.upsert });
          result.matchedCount += r.matchedCount;
          result.modifiedCount += r.modifiedCount;
          if (r.upsertedId) { result.upsertedIds[i] = r.upsertedId; result.upsertedCount++; }
          break;
        }
        case 4: {  // DC_BULK_DELETE_ONE
          const r = await target.deleteOne(spec.filter);
          result.deletedCount += r.deletedCount;
          break;
        }
        default: { // DC_BULK_DELETE_MANY
          const r = await target.deleteMany(spec.filter);
          result.deletedCount += r.deletedCount;
          break;
        }
      }
    } catch (err) {
      errors.push({ index: i, error: err });
      if (ordered) break;
    }
  }
  if (errors.length > 0) {
    const err = new Error(
      `bulkWrite: ${errors.length} operation(s) failed (first at index ${errors[0].index}: ${errors[0].error.message})`
    );
    err.result = result;
    err.writeErrors = errors;
    throw err;
  }
  return result;
}

/**
 * WAL command opcodes, as db_wal.h numbers them. The wire spellings ("i",
 * "u", "createIndex", ...) exist only in engine/src/db_wal.c -- this side
 * dispatches on numbers it cannot mistype.
 */
const WAL_OP = Object.freeze({
  INSERT: 0, UPDATE: 1, REPLACE: 2, DELETE: 3,
  CREATE_INDEX: 4, DROP_INDEX: 5, DROP_COLLECTION: 6,
  INDEX_BEGIN: 7, INDEX_CHUNK: 8
});

/** What the host asks for, before the planner resolves it (dc_wal_req). */
const WAL_REQ = Object.freeze({
  INSERT_ONE: 0, INSERT_MANY: 1, UPDATE_ONE: 2, UPDATE_MANY: 3,
  REPLACE_ONE: 4, DELETE_ONE: 5, DELETE_MANY: 6,
  CREATE_INDEX: 7, DROP_INDEX: 8, DROP_COLLECTION: 9
});

/** dc_wal_plan_outcome. NOTHING = the request logs nothing at all. */
const WAL_PLAN = Object.freeze({ NOTHING: 0, MATCHED: 1, UPSERT: 2 });

/**
 * Resolve a write request into the exact commands to append to the log.
 *
 * This is the whole of Phase 6's marshalling: one call replaces the
 * findOne (or projected find) the WAL layer used to run to pin its target
 * before logging, AND the filter that used to travel into the log for an
 * upsert to re-evaluate at apply time. See db_wal.h for both.
 *
 * `collection` is the inner Collection whose documents are being planned
 * against, or null for a request that touches none (the DDL three, and
 * the insert forms, whose ids this side already assigned).
 *
 * Returns { outcome, commands, targetId, preimage }:
 *   commands  one Uint8Array per log entry, in append order
 *   targetId  the matched or upserted _id, or null
 *   preimage  the matched document as it was before, or null -- what
 *             findOneAnd*'s `returnDocument: 'before'` returns, free,
 *             because resolving the target already read it
 */
function walPlan(collection, collName, req, a, b, { upsert = false, defaultId } = {}) {
  const M = requireModule();
  const oid = defaultId ?? new ObjectId();
  // DROP_INDEX's `a` is an index name, not a document; everything else
  // crosses as binjson (db_wal.h's request table).
  const enc = (v, raw) => v === undefined || v === null
    ? new Uint8Array(0)
    : (raw ? textEncoder.encode(v) : encode(v));
  const aBytes = enc(a, req === WAL_REQ.DROP_INDEX);
  const bBytes = enc(b, false);

  const n = allocStr(M, collName);
  const ap = M._malloc(aBytes.length || 1);
  const bp = M._malloc(bBytes.length || 1);
  const dp = M._malloc(12);
  const rp = M._malloc(4);
  let plan = 0;
  try {
    if (aBytes.length) M.HEAPU8.set(aBytes, ap);
    if (bBytes.length) M.HEAPU8.set(bBytes, bp);
    M.HEAPU8.set(oid.toBytes(), dp);
    plan = M._walw_plan(collection ? collection._collCtx : 0, n.ptr, n.len, req,
                        ap, aBytes.length, bp, bBytes.length,
                        upsert ? 1 : 0, dp, rp);
    const rc = readI32(M, rp);
    if (rc !== 0) throw codeError(rc, collName);

    // Heap growth during the call may have swapped the ArrayBuffer, so
    // every read below goes through a freshly-read M.HEAPU8.
    const commands = [];
    for (let i = 0, count = M._walw_count(plan); i < count; i++) {
      const p = M._walw_cmd_ptr(plan, i);
      const len = M._walw_cmd_len(plan, i);
      commands.push(M.HEAPU8.slice(p, p + len));
    }
    const prePtr = M._walw_preimage_ptr(plan);
    const preLen = M._walw_preimage_len(plan);
    const tid = M._walw_target_id(plan);
    return {
      outcome: M._walw_outcome(plan),
      commands,
      preimage: prePtr ? decode(M.HEAPU8.slice(prePtr, prePtr + preLen)) : null,
      targetId: tid ? new ObjectId(M.HEAPU8.slice(tid, tid + 12)) : null
    };
  } finally {
    if (plan) M._walw_plan_free(plan);
    M._free(rp); M._free(dp); M._free(bp); M._free(ap); n.free();
  }
}

/**
 * The opcode of a logged command, having validated that every field that
 * opcode requires is present and correctly typed.
 *
 * Applied to every entry before it is replayed, so an entry this version
 * cannot execute is refused rather than skipped: a follower that ignores
 * what it does not understand has silently diverged from one that does.
 */
function walParse(payload) {
  const M = requireModule();
  const op = withBytes(M, payload, (p, n) => M._walw_parse(p, n));
  if (op < 0) throw codeError(op, 'WAL command');
  return op;
}

/** Does the C applier drive this opcode, or does it make and unmake
 * files — which belongs to whoever owns the namespace? */
function walIsDocument(op) {
  return requireModule()._walw_is_document(op) === 1;
}

/**
 * Apply one logged command to an open collection: stage the entry's
 * index, perform the mutation the command names, and return the result —
 * all in C (db_wal.h's dc_wal_apply), which is what lets a host with no
 * JavaScript apply a committed entry.
 */
function walApply(collection, index, payload) {
  const M = requireModule();
  const rc = M._malloc(4);
  const p = M._malloc(payload.length || 1);
  let handle = 0;
  try {
    if (payload.length) M.HEAPU8.set(payload, p);
    handle = M._walw_apply(collection._collCtx, index, p, payload.length, rc);
    const code = readI32(M, rc);
    if (code !== 0) throw codeError(code, 'apply');
    const ptr = M._walw_result_ptr(handle);
    const len = M._walw_result_len(handle);
    return decode(M.HEAPU8.slice(ptr, ptr + len));
  } finally {
    if (handle) M._walw_result_free(handle);
    M._free(p); M._free(rc);
  }
}

let raftCtx = 0;

function raftCall(fn, context) {
  const M = requireModule();
  if (!raftCtx) {
    raftCtx = M._rcw_new();
    if (!raftCtx) throw codeError(-1, 'rcw_new');
  }
  const rc = fn(M, raftCtx);
  if (rc !== 0) throw codeError(rc, context);
  const ptr = M._rcw_out_ptr(raftCtx);
  const len = M._rcw_out_len(raftCtx);
  return len ? decode(M.HEAPU8.slice(ptr, ptr + len)) : null;
}

function raftDecide(name, input, context) {
  return raftCall((M, ctx) => withBytes(M, encode(input), (p, n) => M[name](ctx, p, n)), context);
}

/**
 * The Raft rules whose violation is a consensus bug (engine/include/
 * raft_core.h). src/raft.js decides nothing about safety on its own any
 * more: it gathers the inputs, asks, and carries out the answer in the
 * order it is given -- the order being itself a rule, since a vote must
 * reach the disk before the reply leaves.
 *
 * These are exported rather than hidden behind the RaftNode class because
 * the point of moving them is that a host with no JavaScript runs the
 * identical rules; keeping them addressable here is what makes the two
 * paths comparable.
 */
const raft = {
  /** §5.2/§5.4.1: grant this vote? Returns what to do, in order. */
  decideVote: (input) => raftDecide('_rcw_decide_vote', input, 'requestVote'),

  /** §5.3: accept these entries? The consistency check and its hint. */
  decideAppend: (input) => raftDecide('_rcw_decide_append', input, 'appendEntries'),

  /** §5.3's conflict rule against a real log: skip what we already hold,
   * truncate at the first disagreement, append the rest, sync if
   * anything landed. Returns the index truncation began at, or 0. */
  appendEntries(log, entries) {
    const M = requireModule();
    const rc = withBytes(M, encode(entries), (p, n) => M._rcw_append_entries(log.ctx, p, n));
    if (rc < 0) throw codeError(rc, 'appendEntries');
    return rc;
  },

  /** The follower's new commit index, or null if it does not advance. */
  followerCommit(leaderCommit, ourCommit, ourLastIndex) {
    const n = requireModule()._rcw_follower_commit(leaderCommit, ourCommit, ourLastIndex);
    return n < 0 ? null : n;
  },

  /** The highest index a quorum of voters holds, or null. `matches` is
   * one entry per VOTING peer; the leader's own last index is separate
   * because a leader always holds its own. */
  commitCandidate(leaderLast, matches, quorum) {
    const M = requireModule();
    const buf = new Float64Array(matches);
    const p = M._malloc(buf.length * 8 || 1);
    try {
      if (buf.length) M.HEAPU8.set(new Uint8Array(buf.buffer), p);
      const n = M._rcw_commit_candidate(leaderLast, p, buf.length, quorum);
      return n < 0 ? null : n;
    } finally { M._free(p); }
  },

  /** §5.4.2: may this candidate index actually commit? */
  mayCommit: (candidate, commitIndex, baseIndex, termAtCandidate, currentTerm) =>
    requireModule()._rcw_may_commit(candidate, commitIndex, baseIndex, termAtCandidate, currentTerm) === 1,

  quorum: (voterCount) => requireModule()._rcw_quorum(voterCount),

  /** Where a leader resumes after a rejection, and whether the peer's
   * match index had to regress with it. */
  backoff: (hint, next, match) => raftCall(
    (M, ctx) => M._rcw_backoff(ctx, hint ?? 0, hint === undefined ? 0 : 1, next, match),
    'backoff'
  ),

  /** { members, voters, peers } from a member set. */
  membersAdopt: (members, selfId) => raftCall(
    (M, ctx) => withBytes(M, encode(members), (p, n) => M._rcw_members_adopt(ctx, p, n, selfId)),
    'members'
  ),

  /** A proposed set merged with what is known, so an id-only entry can
   * never erase an address the log already carries. */
  membersMerge: (input, known) => raftCall((M, ctx) => {
    const i = encode(input);
    const k = encode(known);
    const ip = M._malloc(i.length || 1);
    const kp = M._malloc(k.length || 1);
    try {
      if (i.length) M.HEAPU8.set(i, ip);
      if (k.length) M.HEAPU8.set(k, kp);
      return M._rcw_members_merge(ctx, ip, i.length, kp, k.length);
    } finally { M._free(kp); M._free(ip); }
  }, 'members')
};

/**
 * The leader's and candidate's own bookkeeping (engine/include/raft_drive.h):
 * tallying an election round, choosing what a peer is owed, and walking a
 * snapshot as a chunk stream.
 *
 * All scalars, so all direct calls -- no context object and no binjson.
 */
const raftDrive = {
  ROUND: Object.freeze({ IGNORE: 0, PENDING: 1, WON: 2, STEP_DOWN: 3 }),
  REPL: Object.freeze({ APPEND: 0, SNAPSHOT: 1, PARK: 2 }),

  /**
   * Begin an election round seeking `term`. Returns a handle with
   * `onReply()` and `free()`; the caller MUST free it, which the
   * election path does by dropping the round when it settles.
   */
  round(term, quorum, preVote) {
    const M = requireModule();
    const ptr = M._rdw_round_new(term, quorum, preVote ? 1 : 0);
    if (!ptr) throw codeError(-1, 'rdw_round_new');
    return {
      get granted() { return requireModule()._rdw_round_granted(ptr); },
      get settled() { return requireModule()._rdw_round_settled(ptr) === 1; },
      /** One vote reply, judged against the node AS IT IS NOW. Returns
       * { action, term } where term matters only for STEP_DOWN. */
      onReply(replyTerm, voteGranted, { currentTerm, isLeader, isCandidate }) {
        const Mi = requireModule();
        const slot = Mi._malloc(8);
        try {
          const action = Mi._rdw_round_on_reply(
            ptr, replyTerm, voteGranted ? 1 : 0, currentTerm,
            isLeader ? 1 : 0, isCandidate ? 1 : 0, slot
          );
          return { action, term: readF64(Mi, slot) };
        } finally { Mi._free(slot); }
      },
      free() { requireModule()._rdw_round_free(ptr); }
    };
  },

  /** APPEND, SNAPSHOT or PARK for a peer whose next index is `next`. */
  replAction: (next, baseIndex, hasSnapshot) =>
    requireModule()._rdw_repl_action(next, baseIndex, hasSnapshot ? 1 : 0),

  /** Where a peer stands once an install completes. */
  installed(boundary, match) {
    const M = requireModule();
    const p = M._malloc(16);
    try {
      writeF64(M, p, match);
      M._rdw_repl_installed(boundary, p);
      return { match: readF64(M, p), next: readF64(M, p + 8) };
    } finally { M._free(p); }
  },

  /**
   * The chunk following the cursor, or null at the end of the stream.
   * Start at { file: 0, offset: 0 } and advance with the returned
   * `nextFile` / `nextOffset` -- never by adding len to offset, which is
   * ambiguous for an empty file (see raft_drive.h).
   */
  chunkNext(fileSizes, chunkBytes, cursorFile, cursorOffset) {
    const M = requireModule();
    const n = fileSizes.length;
    const sizes = M._malloc(Math.max(8, n * 8));
    const out = M._malloc(7 * 8);
    try {
      for (let i = 0; i < n; i++) writeF64(M, sizes + i * 8, fileSizes[i]);
      if (!M._rdw_chunk_next(sizes, n, chunkBytes, cursorFile, cursorOffset, out)) return null;
      return {
        fileIndex: readF64(M, out),
        offset: readF64(M, out + 8),
        len: readF64(M, out + 16),
        isFirst: readF64(M, out + 24) === 1,
        isDone: readF64(M, out + 32) === 1,
        nextFile: readF64(M, out + 40),
        nextOffset: readF64(M, out + 48)
      };
    } finally { M._free(out); M._free(sizes); }
  }
};

/**
 * The replication state machine and its outbox (engine/include/raft_node.h).
 *
 * This is the seam that lets Raft leave JavaScript. Instead of
 * `await transport.call(peer, msg)` -- which suspends the state machine
 * on a promise, and there are no promises under WASI -- C queues what it
 * wants sent, the host delivers it however it likes, and the answer
 * comes back keyed by a correlation id.
 *
 * The node pointer IS the handle: the outbox lives inside it, so two
 * nodes in one process (which is every Raft test) cannot trample each
 * other the way a shared scratch context would.
 */
const RAFT_ROLE = Object.freeze({ FOLLOWER: 0, CANDIDATE: 1, LEADER: 2 });
const RN_EFFECT = Object.freeze({
  ROLE: 0, COMMIT: 1, NEEDS_SNAPSHOT: 2, PROMOTE: 3, REACHABLE: 4, TRUNCATED: 5,
  ELECTION: 6, INSTALLED: 7, SETTLED: 8
});

class RaftCore {
  /** `log` is an EntryLog; it is BORROWED and outlives nothing here. */
  constructor(id, log, { electionTimeoutMs = [150, 300], heartbeatMs = 50,
                         maxBatchBytes = 65536 } = {}) {
    const M = requireModule();
    this._ptr = M._rnw_new(id, log.ctx);
    if (!this._ptr) throw codeError(-1, 'rnw_new');
    M._rnw_set_timing(this._ptr, electionTimeoutMs[0], electionTimeoutMs[1], heartbeatMs);
    M._rnw_set_limits(this._ptr, maxBatchBytes);
    /** This node's bj_ns scope id, once it has files (attachFiles). */
    this._scope = 0;
    this._files = null;
    this._kept = null;
    this._standing = null;   // name -> { fd, handle }, held open (declareSnapshot)
  }

  /**
   * Release the C node. Idempotent, and after it every call below
   * refuses rather than reaching through a freed pointer.
   */
  free() {
    if (!this._ptr) return;
    const M = requireModule();
    // rnw_free releases the bj_ns it built; the table that ns resolved
    // names from is this side's, and nothing else will drop it.
    M._rnw_free(this._ptr);
    this._ptr = 0;
    if (this._scope) {
      delete M.bjnsScopes?.[this._scope];
      delete M.bjnsPending?.[this._scope];
    }
  }

  /**
   * The node, or an explicit refusal.
   *
   * A freed pointer is 0, and 0 is a readable address in linear memory:
   * calling on through it returns plausible garbage -- role 0 is
   * "follower", commit index 0 is "nothing committed" -- rather than
   * failing. Answers like those are worse than a crash, because a host
   * acts on them. Every method goes through here so that use after free
   * is a sentence, not a silent lie.
   */
  get _p() {
    if (!this._ptr) throw new Error('raft: this node has been freed');
    return this._ptr;
  }

  /** Point the node at a different log (EntryLog cannot rebase in place,
   * so both compaction paths swap it). The old log must already be quiet. */
  setLog(log) { requireModule()._rnw_set_log(this._p, log.ctx); }

  // ---- files: serving and receiving a snapshot install ---------------------
  //
  // A node given a namespace and a snapshot store does the whole install
  // itself (raft_node.h): it reads the generation's files to serve one,
  // stages chunks and writes the manifest to receive one, and copies the
  // generation onto the live filenames to adopt one. What stays here is
  // the part C cannot do -- OPENING a file, which is asynchronous in a
  // browser and must not be inside a synchronous C call (bjns.h).
  //
  // So every one of those beats is the same shape: ask C which names it
  // will touch, open exactly those, then make ONE synchronous call.

  /**
   * Give the node a file namespace and the host's snapshot store.
   *
   * `open(name)` must create-if-missing and return a sync access handle;
   * `remove(name)` unlinks. The store is BORROWED -- the same `sst` the
   * host reads `latest` from, so that an install committing inside C
   * moves the host's store too rather than leaving two answers to which
   * generation is live.
   */
  attachFiles({ store, open, remove }) {
    const M = requireModule();
    if (!this._scope) this._scope = nextNsScope++;
    const rc = M._rnw_set_ns(this._p, this._scope);
    if (rc !== 0) throw codeError(rc, 'attachFiles');
    M._rnw_set_snapstore(this._p, store.storeCtx);
    this._files = { open, remove };
  }

  /** Give the store back. The namespace stays -- a node with a scope and
   * no store serves nothing (rn_serves_snapshots wants both), and
   * dropping the ns as well would mean rebuilding it to reattach. */
  detachFiles() {
    requireModule()._rnw_set_snapstore(this._p, 0);
    this._files = null;
  }

  /** Whether this node can serve an install itself -- what a host asks to
   * know whether it still has to. */
  get servesSnapshots() { return requireModule()._rnw_serves_snapshots(this._p) === 1; }

  set chunkBytes(n) { requireModule()._rnw_set_chunk_bytes(this._p, n); }

  /** The plan buffer, as names. Copied out immediately: it is one shared
   * scratch (raft_node_wasm.c) and the next call into C reuses it. */
  _plan(len) {
    if (len < 0) throw codeError(len, 'plan');
    if (len === 0) return [];
    const M = requireModule();
    const ptr = M._rnw_plan_ptr();
    return textDecoder.decode(M.HEAPU8.slice(ptr, ptr + len))
      .split('\0').filter((s) => s.length);
  }

  /** Which files an incoming install message will touch. Pure. */
  installPlan(bytes) {
    const M = requireModule();
    return this._plan(withBytes(M, bytes, (p, n) => M._rnw_install_plan(this._p, p, n)));
  }

  /** Which files an adoption will touch. Pure. */
  adoptPlan() { return this._plan(requireModule()._rnw_adopt_plan(this._p)); }

  get installing() { return requireModule()._rnw_installing(this._p) === 1; }
  get adoptPending() { return requireModule()._rnw_adopt_pending(this._p) === 1; }
  get adoptBoundary() { return requireModule()._rnw_adopt_boundary(this._p); }

  /** This node's name -> fd table; C resolves every open through it. */
  _table() {
    const M = requireModule();
    const scopes = (M.bjnsScopes ||= {});
    return (scopes[this._scope] ||= {});
  }

  /** bj_ns.remove MAY BE DEFERRED here -- OPFS removeEntry returns a
   * promise (bjns.h) -- so the adapter queues names and this side drains
   * the queue once the synchronous call has returned. */
  async _drainRemoves() {
    const M = requireModule();
    const pending = M.bjnsPending && M.bjnsPending[this._scope];
    if (!pending || !pending.length) return;
    M.bjnsPending[this._scope] = [];
    for (const f of pending) {
      try { await this._files.remove(f); } catch { /* an orphan, not a fault */ }
    }
  }

  /**
   * Declare the files this node may read WITHOUT BEING ASKED FIRST: the
   * current snapshot generation's, which a leader streams chunks from
   * inside rn_tick and rn_on_reply. Those are synchronous, so there is no
   * plan beat to open on -- the names have to be standing already.
   *
   * Call it whenever the store's `latest` moves (a local snapshot, an
   * adopted install) and the previous generation's handles are released.
   * A generation's data files are immutable and nothing else opens them,
   * so holding them is free; it is the sending side's whole cost.
   */
  async declareSnapshot(names) {
    const M = requireModule();
    const table = this._table();
    const previous = this._standing || new Map();
    const standing = new Map();
    for (const name of names) {
      const already = previous.get(name);
      if (already) { previous.delete(name); standing.set(name, already); continue; }
      const handle = await this._files.open(name);
      const fd = registerHandle(M, handle);
      table[name] = fd;
      standing.set(name, { fd, handle });
    }
    this._standing = standing;
    await this._releaseAll(previous, table);
  }

  async _releaseAll(held, table) {
    const M = requireModule();
    let failure = null;
    for (const [name, { fd, handle }] of held) {
      delete table[name];
      unregisterHandle(M, fd);
      try { await handle.close(); } catch (err) { failure ||= err; }
    }
    held.clear();
    if (failure) throw failure;
  }

  /** Give every standing handle back. The host opened them, so the host
   * closes them -- bns_close is deliberately a no-op on the handle. */
  async releaseFiles() {
    if (this._standing) await this._releaseAll(this._standing, this._table());
  }

  /**
   * Open every planned name into this node's scope table, run `fn()`,
   * then give the handles back. C resolves names from that table rather
   * than opening (bjns_bridge.c), which is the whole reason the opens
   * happen out here. Names already standing (declareSnapshot) are left
   * alone: a browser refuses a second sync access handle on a live one.
   *
   * A name in `keep` is NOT closed: rn_adopt leaves the node holding the
   * log file it just created, and closing that handle would pull the
   * file out from under the log. Its handle and fd come back instead, for
   * whoever now owns them.
   */
  async withFiles(names, fn, { keep = [] } = {}) {
    const M = requireModule();
    const table = this._table();
    const opened = [];  // { name, fd, handle }
    const kept = new Map();
    try {
      for (const name of names) {
        if (table[name] !== undefined) continue;   // already standing (see declare)
        const handle = await this._files.open(name);
        const fd = registerHandle(M, handle);
        table[name] = fd;
        opened.push({ name, fd, handle });
      }
      return fn();
    } finally {
      let failure = null;
      while (opened.length) {
        const { name, fd, handle } = opened.pop();
        delete table[name];
        if (keep.includes(name)) { kept.set(name, { fd, handle }); continue; }
        unregisterHandle(M, fd);
        try { handle.flush?.(); await handle.close(); }
        catch (err) { failure ||= err; }
      }
      this._kept = kept;
      await this._drainRemoves();
      if (failure) throw failure;
    }
  }

  /** The handle withFiles was told to keep, by name. */
  keptFile(name) { return this._kept?.get(name) ?? null; }

  /**
   * The flip: stale live files out, the generation's onto the live
   * names, the log rebased onto the boundary -- one synchronous call,
   * because between the first restored file and the last the database is
   * neither the old one nor the new one (raft_node.h).
   *
   * `victims` are the live files to remove first; deciding which of them
   * are the database's is the host's knowledge. Returns the pointer to
   * the log this replaced, or 0 when the node had already rebased once
   * and owns that log itself.
   */
  adopt(victims) {
    const M = requireModule();
    const joined = textEncoder.encode(victims.length ? victims.join('\0') + '\0' : '');
    const out = M._malloc(4);
    const p = M._malloc(joined.length || 1);
    try {
      if (joined.length) M.HEAPU8.set(joined, p);
      const rc = M._rnw_adopt(this._p, p, joined.length, out);
      if (rc !== 0) throw codeError(rc, 'adopt');
      return readU32(M, out);
    } finally { M._free(p); M._free(out); }
  }

  /** The log the node is using -- after adopt(), one the NODE opened. */
  get logCtx() { return requireModule()._rnw_log(this._p); }

  /**
   * Adopt a member set, or throw and adopt none of it. A refusal
   * (malformed, a voter who is not a member, or more members than
   * maxPeers + 1) leaves the node's previous set exactly as it was —
   * which is what lets a host treat "C took it" as permission to record
   * the same set on its own side.
   */
  setMembers(records) {
    const M = requireModule();
    const enc = encode(records);
    const p = M._malloc(enc.length || 1);
    try {
      if (enc.length) M.HEAPU8.set(enc, p);
      const rc = M._rnw_set_members(this._p, p, enc.length);
      if (rc !== 0) throw codeError(rc, 'setMembers');
    } finally { M._free(p); }
  }

  /** The largest peer count (members excluding self) this build holds. */
  get maxPeers() { return requireModule()._rnw_max_peers(); }

  /**
   * The adopted set as the node normalized it: { members, voters, peers }.
   * Read rather than re-derived — a host that ran membersAdopt itself
   * would be a second place the cluster's shape is written down.
   */
  get adopted() {
    const M = requireModule();
    const ptr = M._rnw_adopted_ptr(this._p);
    const len = M._rnw_adopted_len(this._p);
    if (!len) return { members: [], voters: [], peers: [] };
    return decode(M.HEAPU8.slice(ptr, ptr + len));
  }

  /**
   * Propose a new member set (records or bare ids), merged with what the
   * log already carries. Returns the CONFIG entry's index; throws
   * RAFT_ERR_BUSY's error if a change is already in flight, and
   * RAFT_ERR_CAPACITY's if the result would not fit.
   */
  changeMembership(members) {
    const M = requireModule();
    const enc = encode(members);
    const p = M._malloc(enc.length || 1);
    const out = M._malloc(8);
    try {
      if (enc.length) M.HEAPU8.set(enc, p);
      const rc = M._rnw_change_membership(this._p, p, enc.length, out);
      if (rc !== 0) throw codeError(rc, 'changeMembership');
      return readF64(M, out);
    } finally { M._free(out); M._free(p); }
  }

  /** Is a membership change in flight? One at a time is a safety rule. */
  get configInFlight() { return requireModule()._rnw_config_in_flight(this._p) === 1; }

  start(now, random01) { requireModule()._rnw_start(this._p, now, random01); }
  stop() { requireModule()._rnw_stop(this._p); }
  tick(now, random01) {
    const rc = requireModule()._rnw_tick(this._p, now, random01);
    if (rc !== 0) throw codeError(rc, 'tick');
  }
  quiesce() { requireModule()._rnw_quiesce(this._p); }
  wake(now, random01) { requireModule()._rnw_wake(this._p, now, random01); }

  /**
   * An incoming request. Its reply lands in the outbox addressed back to
   * whoever the MESSAGE says sent it — the node reads that itself, so a
   * host with no sender id (this transport carries none) cannot get it
   * wrong by inventing one. `corr` is the sender's correlation id, which
   * the reply carries back; `random01` seeds any election timer the
   * message re-arms — passed in, never drawn, so a simulated cluster
   * replays exactly.
   */
  handle(corr, bytes, random01 = 0.5) {
    const M = requireModule();
    const p = M._malloc(bytes.length || 1);
    try {
      if (bytes.length) M.HEAPU8.set(bytes, p);
      return M._rnw_handle(this._p, corr, p, bytes.length, random01);
    } finally { M._free(p); }
  }

  /** A reply to something this node sent. */
  onReply(corr, bytes, random01 = 0.5) {
    const M = requireModule();
    const p = M._malloc(bytes.length || 1);
    try {
      if (bytes.length) M.HEAPU8.set(bytes, p);
      return M._rnw_on_reply(this._p, corr, p, bytes.length, random01);
    } finally { M._free(p); }
  }

  /** The request with this correlation id will never be answered. */
  onFail(corr) { return requireModule()._rnw_on_fail(this._p, corr); }

  /**
   * The apply pump reached `index`. Whatever proposals that finishes come
   * back as SETTLED effects on the next drain, each carrying whether the
   * entry at that index is STILL the one that was proposed -- the rule
   * this side used to own, and the one that decides whether a client is
   * told its write happened (raft_node.h).
   */
  applied(index) { requireModule()._rnw_applied(this._p, index); }

  /** Owe an answer for an index proposed at a term. propose() does this
   * itself; this is for re-registering across a restart. */
  await_(index, term) {
    const rc = requireModule()._rnw_await(this._p, index, term);
    if (rc !== 0) throw codeError(rc, 'await');
  }

  /** How many answers this node still owes. */
  get awaiting() { return requireModule()._rnw_awaiting(this._p); }

  /**
   * Everything queued, as plain objects, and the queue is cleared. The
   * bytes are COPIED out: they point into C-owned buffers that the very
   * next call may reuse, and a host that delivers asynchronously would
   * otherwise send whatever landed there later.
   */
  drainOutbox() {
    const M = requireModule();
    const n = M._rnw_out_count(this._p);
    const out = [];
    for (let i = 0; i < n; i++) {
      const ptr = M._rnw_out_ptr(this._p, i);
      const len = M._rnw_out_len(this._p, i);
      out.push({
        peer: M._rnw_out_peer(this._p, i),
        corr: M._rnw_out_corr(this._p, i),
        isReply: M._rnw_out_is_reply(this._p, i) === 1,
        bytes: len ? M.HEAPU8.slice(ptr, ptr + len) : new Uint8Array(0)
      });
    }
    M._rnw_out_clear(this._p);
    return out;
  }

  /** What C could not do itself: apply, read a file, settle a promise. */
  drainEffects() {
    const M = requireModule();
    const n = M._rnw_effect_count(this._p);
    const out = [];
    for (let i = 0; i < n; i++) {
      out.push({
        kind: M._rnw_effect_kind(this._p, i),
        arg: M._rnw_effect_arg(this._p, i),
        flag: M._rnw_effect_flag(this._p, i) === 1
      });
    }
    M._rnw_effects_clear(this._p);
    return out;
  }

  /** Sticky: the node once had an effect to report and no room for it,
   * so the host's picture of it has a hole that draining cannot fill.
   * Unreachable for a host that drains after every call. */
  get effectsLost() { return requireModule()._rnw_effects_lost(this._p) === 1; }

  get role() { return requireModule()._rnw_role(this._p); }
  get leaderId() { return requireModule()._rnw_leader_id(this._p); }
  get commitIndex() { return requireModule()._rnw_commit_index(this._p); }
  get quorum() { return requireModule()._rnw_quorum(this._p); }
  get quiesced() { return requireModule()._rnw_is_quiesced(this._p) === 1; }
  matchOf(peer) { return requireModule()._rnw_match(this._p, peer); }
  nextOf(peer) { return requireModule()._rnw_next(this._p, peer); }
  /** The correlation id outstanding at `peer`, or 0. */
  inflightOf(peer) { return requireModule()._rnw_inflight(this._p, peer); }
  hasQuorumContact(withinMs) {
    return requireModule()._rnw_has_quorum_contact(this._p, withinMs) === 1;
  }
  replicate(peer) { return requireModule()._rnw_replicate(this._p, peer); }
  installed(peer, boundary) {
    return requireModule()._rnw_installed(this._p, peer, boundary);
  }

  /**
   * Append one entry at the current term, sync it, replicate it, and run
   * the commit check (a single-voter group has no reply coming to run it
   * for them). Returns the index it landed at.
   */
  propose(type, payload) {
    const M = requireModule();
    const p = M._malloc(payload.length || 1);
    const out = M._malloc(8);
    try {
      if (payload.length) M.HEAPU8.set(payload, p);
      const rc = M._rnw_propose(this._p, type, p, payload.length, out);
      if (rc !== 0) throw codeError(rc, 'propose');
      return readF64(M, out);
    } finally { M._free(out); M._free(p); }
  }

  /** Seed the commit index at startup (never lowers it). */
  seedCommit(index) { requireModule()._rnw_seed_commit(this._p, index); }

  /** Stand for election now, skipping pre-vote (TimeoutNow, §3.10). */
  campaign(random01 = 0.5) {
    const rc = requireModule()._rnw_campaign(this._p, random01);
    if (rc !== 0) throw codeError(rc, 'campaign');
  }

  /** A leader's term on a message the HOST answered (InstallSnapshot):
   * adopt it as an AppendEntries would. False means it was stale. */
  observeLeader(term, leaderId, random01 = 0.5) {
    return requireModule()._rnw_observe_leader(this._p, term, leaderId, random01) === 1;
  }

  /** A higher term on a reply the HOST awaited: step down. */
  stepDown(term, random01 = 0.5) {
    return requireModule()._rnw_step_down(this._p, term, random01) === 1;
  }
}

let raftMsgCtx = 0;

/** Slot order of the packed node state rmw_* reads (raft_msg_wasm.c). */
const RAFT_STATE_SLOTS = 9;

function raftMsgCall(fn, context) {
  const M = requireModule();
  if (!raftMsgCtx) {
    raftMsgCtx = M._rmw_new();
    if (!raftMsgCtx) throw codeError(-1, 'rmw_new');
  }
  const rc = fn(M, raftMsgCtx);
  if (rc !== 0) throw codeError(rc, context);
  const rp = M._rmw_reply_ptr(raftMsgCtx);
  const rl = M._rmw_reply_len(raftMsgCtx);
  const ep = M._rmw_eff_ptr(raftMsgCtx);
  const el = M._rmw_eff_len(raftMsgCtx);
  return {
    reply: rl ? M.HEAPU8.slice(rp, rp + rl) : new Uint8Array(0),
    effect: el ? decode(M.HEAPU8.slice(ep, ep + el)) : null
  };
}

/**
 * Call `fn` with the node state packed into a heap array of f64s in the
 * slot order raft_msg_wasm.c documents.
 */
function withRaftState(st, fn) {
  const M = requireModule();
  const p = M._malloc(RAFT_STATE_SLOTS * 8);
  try {
    const view = new Float64Array([
      st.selfId, st.isFollower ? 1 : 0, st.isLeader ? 1 : 0, st.selfIsVoter ? 1 : 0,
      st.leaderId, st.commitIndex, st.now, st.lastLeaderContact, st.minElectionTimeout
    ]);
    M.HEAPU8.set(new Uint8Array(view.buffer), p);
    return fn(M, p);
  } finally { M._free(p); }
}

/**
 * The Raft wire grammar and the two RPC handlers that run entirely in C
 * (engine/include/raft_msg.h).
 *
 * These take the message as the bytes it arrived as and return the reply
 * as the bytes to send back. Nothing decodes an AppendEntries in
 * JavaScript any more -- which is what stopped the entries crossing the
 * bridge twice, and what a host with no JavaScript needs in order to
 * speak the same protocol.
 */
const raftMsg = {
  KIND: Object.freeze({
    REQUEST_VOTE: 0, APPEND_ENTRIES: 1, INSTALL_SNAPSHOT: 2, JOIN: 3, LEAVE: 4,
    TIMEOUT_NOW: 5
  }),

  /** Which kind is this, without interpreting it. Negative on a message
   * this build does not understand. */
  kind(bytes) {
    const M = requireModule();
    return withBytes(M, bytes, (p, n) => M._rmw_kind(p, n));
  },

  /** Handle a RequestVote. The vote is on disk before this returns. */
  requestVote: (log, state, bytes) => raftMsgCall(
    (M, ctx) => withRaftState(state, (MM, sp) =>
      withBytes(MM, bytes, (p, n) => MM._rmw_request_vote(ctx, log.ctx, sp, p, n))),
    'requestVote'
  ),

  /** Handle an AppendEntries. The entries are on disk before this
   * returns, and never left the buffer they arrived in. */
  appendEntries: (log, state, bytes) => raftMsgCall(
    (M, ctx) => withRaftState(state, (MM, sp) =>
      withBytes(MM, bytes, (p, n) => MM._rmw_append_entries(ctx, log.ctx, sp, p, n))),
    'appendEntries'
  ),

  buildRequestVote: (term, candidateId, lastLogIndex, lastLogTerm, preVote) => raftMsgCall(
    (M, ctx) => M._rmw_build_request_vote(ctx, term, candidateId, lastLogIndex,
                                          lastLogTerm, preVote ? 1 : 0),
    'requestVote'
  ).reply,

  /** Build an AppendEntries, taking the batch straight out of the log.
   * Returns { message, matchIndex } -- the index a success will imply. */
  buildAppendEntries(log, { term, leaderId, nextIndex, prevLogTerm, leaderCommit,
                            maxBytes, quiesce = false }) {
    const { reply, effect } = raftMsgCall(
      (M, ctx) => M._rmw_build_append_entries(ctx, log.ctx, term, leaderId, nextIndex,
                                              prevLogTerm, leaderCommit, maxBytes,
                                              quiesce ? 1 : 0),
      'appendEntries'
    );
    return { message: reply, matchIndex: effect.matchIndex };
  }
};

let ttlCtx = 0;

/**
 * The expiry filters a collection's TTL indexes imply at instant `nowMs`,
 * one per TTL index, in index order.
 *
 * Both the cutoff arithmetic and the filter shape are C's (db_ttl.h) --
 * they were duplicated verbatim between Collection.pruneExpired and
 * WalCollection.pruneExpired, which differ only in what they then DO with
 * the filter (delete directly, or log a delete command first). That
 * difference is real and stays theirs; the policy is shared.
 *
 * The index *registry* is still JS, so the loop is too. When Phase 3 moves
 * index metadata into the C catalog, this becomes a single dc_ttl_plan
 * call and the loop goes with it.
 *
 * @param {Map} indexes Collection._indexes
 * @param {number} nowMs the host's clock reading
 * @returns {object[]} decoded filters, ready for deleteMany
 */
function ttlFilters(indexes, nowMs) {
  const M = requireModule();
  const filters = [];
  for (const ix of indexes.values()) {
    if (ix.kind !== 'equality' || ix.expireAfterSeconds === undefined) continue;
    if (!ttlCtx) {
      ttlCtx = M._ttlw_new();
      if (!ttlCtx) throw codeError(-1, 'ttlw_new');
    }
    const field = allocStr(M, ix.fields[0]);
    try {
      check(M._ttlw_filter(ttlCtx, field.ptr, field.len, nowMs, ix.expireAfterSeconds));
      const len = M._ttlw_len(ttlCtx);
      check(len < 0 ? len : 0);
      const ptr = M._ttlw_ptr(ttlCtx);
      filters.push(decode(M.HEAPU8.slice(ptr, ptr + len)));
    } finally { field.free(); }
  }
  return filters;
}

let validateCtx = 0;

/**
 * Validate a createIndex key spec and return its field names in spec
 * order. C does both in one pass (dc_check_index_key_spec), because the
 * binjson ARRAY it emits is exactly what dc_collection_add_index takes for
 * `fields` -- so the array JS used to build with Object.keys() and then
 * re-encode is now produced once, by the code that validated it.
 */
function checkIndexKeySpec(keys) {
  const M = requireModule();
  if (!validateCtx) {
    validateCtx = M._dvw_new();
    if (!validateCtx) throw codeError(-1, 'dvw_new');
  }
  const spec = encode(keys);
  const ptr = M._malloc(spec.length || 1);
  try {
    if (spec.length) M.HEAPU8.set(spec, ptr);
    const rc = M._dvw_check_index_key_spec(validateCtx, ptr, spec.length);
    if (rc !== 0) throw codeError(rc, JSON.stringify(keys));
    const len = M._dvw_len(validateCtx);
    check(len < 0 ? len : 0);
    return decode(M.HEAPU8.slice(M._dvw_ptr(validateCtx), M._dvw_ptr(validateCtx) + len));
  } finally { M._free(ptr); }
}

/**
 * In-memory named-file storage: handles persist for the process lifetime
 * (MemoryHandle.close() is a no-op, so data survives collection/Db close).
 * Intended for tests and embeddings that don't need durability.
 */
class MemoryStorageProvider {
  constructor() {
    this._files = new Map(); // name -> MemoryHandle
    this._children = new Map(); // name -> MemoryStorageProvider, see subProvider()
  }

  async openFile(name, { create = false } = {}) {
    let handle = this._files.get(name);
    if (!handle) {
      if (!create) throw new Error(`File not found: ${name}`);
      handle = new MemoryHandle();
      this._files.set(name, handle);
    }
    return handle;
  }

  async deleteFile(name) {
    this._files.delete(name);
  }

  /** Names of every file currently stored. Optional on a provider; having
   * it lets Db.open() sweep files orphaned by a crashed compact() or
   * dropCollection (see Db._sweepOrphans). */
  async listFiles() {
    return [...this._files.keys()];
  }

  /** A named, isolated storage scope nested under this one -- Client.db(name)'s equivalent of OPFSStorageProvider.subProvider's real subdirectory, backed by its own independent file map rather than a real filesystem. Cached: repeat calls with the same name return the same instance. */
  async subProvider(name) {
    let child = this._children.get(name);
    if (!child) {
      child = new MemoryStorageProvider();
      this._children.set(name, child);
    }
    return child;
  }

  /** Names of the nested scopes that exist -- the other half of
   * subProvider(), and what Client.listDatabases() reports. Here the
   * child map IS the filesystem, so a scope exists once it has been
   * asked for. */
  async listSubProviders() {
    return [...this._children.keys()];
  }

  /** Remove a nested scope and everything in it; false if there was none.
   * The caller closes whatever it had open in there FIRST -- this drops
   * the storage, and a Db still holding handles into it is a Db reading
   * files nothing will write again. */
  async deleteSubProvider(name) {
    return this._children.delete(name);
  }
}

/**
 * OPFS-backed named-file storage, rooted at a directory handle (defaults to
 * the OPFS root, resolved lazily so construction works outside a worker).
 */
class OPFSStorageProvider {
  constructor(dirHandle) {
    this._dirHandle = dirHandle || null;
  }

  async _dir() {
    if (!this._dirHandle) this._dirHandle = await navigator.storage.getDirectory();
    return this._dirHandle;
  }

  async openFile(name, { create = false } = {}) {
    const dir = await this._dir();
    const fileHandle = await getFileHandle(dir, name, { create });
    return fileHandle.createSyncAccessHandle();
  }

  async deleteFile(name) {
    await deleteFile(await this._dir(), name);
  }

  /** Names of every file in this provider's directory. Optional on a
   * provider; having it lets Db.open() sweep files orphaned by a crashed
   * compact() or dropCollection (see Db._sweepOrphans). */
  async listFiles() {
    const names = [];
    for await (const [name, handle] of (await this._dir()).entries()) {
      if (handle.kind === 'file') names.push(name);
    }
    return names;
  }

  /** A real OPFS subdirectory (created if needed) as its own provider -- Client.db(name)'s on-disk unit, one subdirectory per logical database under this provider's own directory, mirroring the cloud service's per-tenant `<tenantId>/<dbName>/` layout (service/tenant-worker.js). */
  async subProvider(name) {
    const dir = await this._dir();
    const childDir = await dir.getDirectoryHandle(name, { create: true });
    return new OPFSStorageProvider(childDir);
  }

  /** Names of the subdirectories under this one -- the other half of
   * subProvider(), and what Client.listDatabases() reports. The same
   * enumeration listFiles() does, filtered the other way. */
  async listSubProviders() {
    const names = [];
    for await (const [name, handle] of (await this._dir()).entries()) {
      if (handle.kind === 'directory') names.push(name);
    }
    return names;
  }

  /** Remove a subdirectory and everything in it; false if there was none.
   * The caller closes whatever it had open in there FIRST: OPFS sync
   * access handles are exclusive, and removing a directory out from
   * under one is the one way to get a handle to a file that no longer
   * has a name. */
  async deleteSubProvider(name) {
    const dir = await this._dir();
    try {
      await dir.removeEntry(name, { recursive: true });
      return true;
    } catch (err) {
      if (err?.name === 'NotFoundError') return false;
      throw err;
    }
  }
}

/**
 * Resolves $currentDate into $set before an update document ever crosses
 * the WASM bridge -- only the JS host has a clock (the same reasoning that
 * already puts _id generation in JS, not C; see c/db_update.h's top
 * comment). Returns `update` unchanged if it has no $currentDate; never
 * mutates the caller's object. Each targeted field must be `true` or
 * `{ $type: 'date' }` (no timestamp wire type exists) and must not already
 * be targeted by another top-level operator.
 */
let currentDateCtx = 0;

/**
 * Rewrite {$currentDate: {...}} into {$set: {...}} carrying `nowMs`.
 *
 * The rewrite lives in C (upd_resolve_current_date) because its rules are
 * the update layer's rules: a $currentDate field must not collide with
 * any other operator's target, which is the same path-collision
 * invariant db_update.c already enforces for everything else. Only the
 * clock is the host's -- WASM has no portable one.
 *
 * An update with no $currentDate is returned unchanged, and the rewrite
 * is idempotent on an already-resolved update, which is what lets the WAL
 * resolve at proposal time and the apply path run it again without a
 * second clock reading changing the result.
 */
function resolveCurrentDate(update, nowMs = Date.now()) {
  if (!update || typeof update !== 'object' || !('$currentDate' in update)) return update;
  const M = requireModule();
  if (!currentDateCtx) {
    currentDateCtx = M._cdw_new();
    if (!currentDateCtx) throw codeError(-1, 'cdw_new');
  }
  const encoded = encode(update);
  const ptr = M._malloc(encoded.length || 1);
  try {
    if (encoded.length) M.HEAPU8.set(encoded, ptr);
    const rc = M._cdw_resolve(currentDateCtx, ptr, encoded.length, nowMs);
    if (rc !== 0) throw codeError(rc, JSON.stringify(update.$currentDate));
    const len = M._cdw_len(currentDateCtx);
    check(len < 0 ? len : 0);
    const out = M._cdw_ptr(currentDateCtx);
    return decode(M.HEAPU8.slice(out, out + len));
  } finally { M._free(ptr); }
}

/** Default bound on unconsumed change events buffered for a stream's
 * async iterator (ChangeStream below; override per stream with watch()'s
 * `maxBuffered` option). Generous for any real consumer -- an iterator
 * only falls this far behind when it has effectively stopped. */
const CHANGE_STREAM_MAX_BUFFERED = 4096;

/**
 * A live feed of change events from a Collection (Collection.watch()) or a
 * SharedCollection (db-coordinator.js's Coordinator.watch()) -- both an
 * EventEmitter-lite (.on('change', cb)) and an async iterator (for await),
 * matching the real driver's ChangeStream dual API. `unsubscribe` (called
 * once, on close()) removes this stream from whatever registry created it.
 *
 * Backpressure (docs/roadmap.md P1 #11): events are buffered only for the
 * async-iterator side, and only while that side is plausibly consuming --
 * a stream used purely via .on('change') never buffers (its listeners got
 * each event synchronously; buffering forever for an iterator nobody runs
 * was the old unbounded-growth bug). The buffer is bounded: at
 * `maxBuffered` unconsumed events the stream errors out -- it closes, and
 * pending/subsequent next() calls reject with ChangeStreamOverflowError --
 * rather than growing without limit or silently dropping events. There
 * are no resume tokens (a documented non-goal): an overflowed consumer
 * re-watches and re-reads current state.
 */
class ChangeStream {
  constructor(unsubscribe, { maxBuffered = CHANGE_STREAM_MAX_BUFFERED } = {}) {
    this._listeners = new Set();
    this._queue = [];
    this._waiting = []; // pending next() {resolve, reject} entries, FIFO
    this._closed = false;
    this._error = null;      // set on overflow; next() throws it from then on
    this._iterating = false; // true once next() has ever been called
    this._maxBuffered = maxBuffered;
    this._unsubscribe = unsubscribe;
  }

  _emit(change) {
    if (this._closed) return;
    for (const cb of this._listeners) cb(change);
    if (this._waiting.length) { this._waiting.shift().resolve({ value: change, done: false }); return; }
    // Buffer for the iterator unless this stream is listener-only (never
    // pulled, has listeners): those events were just delivered above, and
    // queueing them too would grow forever. A mixed consumer (listener +
    // iterator) buffers normally so the iterator misses nothing between
    // pulls.
    if (!this._iterating && this._listeners.size > 0) return;
    if (this._queue.length >= this._maxBuffered) {
      // No parked next() to reject here: waiters only ever park on an
      // empty queue (next() drains it first), so a full queue implies
      // none. The error surfaces from every next() after this instead.
      this._error = new ChangeStreamOverflowError(
        `ChangeStream overflow: more than ${this._maxBuffered} unconsumed change events -- ` +
        'consume faster (for await / next()), raise watch()\'s maxBuffered, or close() the stream'
      );
      this._queue = [];
      this.close();
      return;
    }
    this._queue.push(change);
  }

  on(event, cb) {
    if (event !== 'change') throw new Error(`ChangeStream: unsupported event "${event}"`);
    this._listeners.add(cb);
    return this;
  }

  off(cb) {
    this._listeners.delete(cb);
    return this;
  }

  async next() {
    this._iterating = true;
    if (this._error) throw this._error;
    if (this._queue.length) return { value: this._queue.shift(), done: false };
    if (this._closed) return { value: undefined, done: true };
    return new Promise((resolve, reject) => this._waiting.push({ resolve, reject }));
  }

  [Symbol.asyncIterator]() { return this; }

  async return() {
    this.close();
    return { value: undefined, done: true };
  }

  close() {
    if (this._closed) return;
    this._closed = true;
    const waiting = this._waiting;
    this._waiting = [];
    for (const w of waiting) w.resolve({ value: undefined, done: true });
    if (this._unsubscribe) this._unsubscribe();
  }
}

/**
 * Safety net for abandoned streaming cursors (docs/roadmap.md P1 #10): a
 * find() cursor that was pulled from but never exhausted or close()d
 * holds a live WASM-side dc_cursor, which pins WASM memory and blocks
 * compact() (its scan is physically positioned inside the current files).
 * Well-behaved callers close or exhaust their cursors -- that stays the
 * documented contract, because GC gives no timing guarantee -- but when a
 * cursor object becomes unreachable anyway, this registry frees its
 * dc_cursor and removes its token from the collection's _openCursors so a
 * later compact() can proceed.
 *
 * The registration is deliberately structured so the entry never keeps
 * the cursor alive: _openCursors holds a plain { ptr, close() } token
 * (data only -- its close() references module globals, not find()'s
 * scope, since any closure over that scope would reach the cursor object
 * through the shared environment record), and the held value references
 * the token and the Set, never the cursor.
 */
const cursorFinalizer = typeof FinalizationRegistry === 'function'
  ? new FinalizationRegistry(({ token, cursors }) => {
      if (cursors.delete(token)) token.close();
    })
  : null;

/** Mint an _openCursors token. Module-level on purpose: a function
 * defined lexically inside find() -- even one referencing no variables
 * from that scope -- would keep the whole scope, and therefore the cursor
 * object, reachable from the registry's held value. */
function makeCursorToken(ptr) {
  return {
    ptr,
    close() {
      if (this.ptr) {
        requireModule()._dcw_cursor_close(this.ptr);
        this.ptr = 0;
      }
    }
  };
}

class Collection {
  constructor(name, tree, { catalog, provider, order, nsScope = 0 }) {
    this.name = name;
    // The owning Db's bj_ns scope: the name -> fd table compact() fills in
    // before handing control to C (bjns_bridge.c).
    this._nsScope = nsScope;
    this._tree = tree;       // BPlusTree, opened by Db.collection()
    this._catalog = catalog; // shared Db catalog tree, for this collection's index list
    this._provider = provider;
    this._order = order;
    this._outCtx = 0;        // per-collection query-output slot in the WASM heap
    this._collCtx = 0;       // dc_collection* coordinating the primary tree + indexes
    // indexName -> one of:
    //   { kind: 'equality', fields, tree, file }
    //   { kind: 'text', field, trees: {index,docTerms,docLengths}, files: {...} }
    //   { kind: 'geo', field, rt, file }
    this._indexes = new Map();
    this._journal = null;    // sync access handle for the cross-file commit journal
    this._journalFd = -1;
    this._watchers = new Set(); // open ChangeStreams (see watch())
    this._openCursors = new Set(); // open find() cursors holding a live WASM-side dc_cursor (see find())
    // Compaction critical section: compact() must never overlap any other
    // operation on this collection (a mutation during the build phase
    // would leave the new generation internally inconsistent -- its files
    // are streamed one at a time from the live trees -- and a read during
    // the adopt window would touch freed WASM handles; _indexes is also
    // transiently empty during the reopen). Two pieces enforce it:
    //
    //   - `_compacting`: null, or a promise settling when the in-flight
    //     compact() finishes. Every public operation waits it out before
    //     starting (see the prototype wrapper after this class) --
    //     queueing rather than failing loud, so a compact triggered
    //     elsewhere (another tab via connectShared, the autoCompact sweep)
    //     is a brief wait for concurrent callers, never a spurious error.
    //   - `_inFlight`: the count of operations currently past that gate.
    //     compact() waits for it to drain to zero before setting
    //     `_compacting` and touching any file, so an operation that
    //     awaits internally mid-body (e.g. a mutation resolving its watch
    //     documentKey via findOne) can never have a compact start inside
    //     that window.
    //
    // The invariant that makes re-entrancy safe: `_compacting` is only
    // ever set at a drained instant (gate-set and the `_inFlight === 0`
    // check happen in one synchronous region). So while any counted
    // operation runs, the gate is provably open, and its internal calls
    // into other public methods (bulkWrite -> insertOne, pruneExpired ->
    // deleteMany, watch bookkeeping -> findOne) just nest the count --
    // they can never deadlock against a compact waiting on their parent.
    // New operations arriving during compact()'s drain wait also pass the
    // still-open gate; compact simply waits for the next quiescent
    // instant (ops here are short -- worst case it's delayed, never
    // starved forever by a caller that awaits its own operations).
    this._compacting = null;
    this._inFlight = 0;
    this._drainWaiters = []; // resolvers parked by compact() until _inFlight drains to 0
  }

  /** Decrement _inFlight and, on reaching zero, wake compact()s parked in
   * their drain wait (see the prototype wrapper after this class). */
  _opDone() {
    if (--this._inFlight === 0 && this._drainWaiters.length) {
      const waiters = this._drainWaiters;
      this._drainWaiters = [];
      for (const w of waiters) w();
    }
  }

  /**
   * A live feed of change events (insert/update/replace/delete) for this
   * collection. Unlike the real driver, `pipeline` stages ($match, etc.)
   * aren't supported yet -- filter inside your own `on('change', cb)`
   * instead -- and there's no `updateDescription` (would need diffing
   * before/after images; skipped as a documented scope limit). Costs
   * nothing when nothing is watching: see _emitChange's fast path and each
   * CRUD method's "only when _watchers.size" extra lookups.
   */
  watch(pipeline = [], options = {}) {
    if (pipeline.length) {
      throw new Error('Collection.watch: pipeline stages are not supported yet');
    }
    const stream = new ChangeStream(() => this._watchers.delete(stream), { maxBuffered: options.maxBuffered });
    this._watchers.add(stream);
    return stream;
  }

  /** No-op fast path when nothing is watching (the common case). */
  _emitChange(event) {
    if (this._watchers.size === 0) return;
    const change = { ns: { coll: this.name }, ...event };
    for (const stream of this._watchers) stream._emit(change);
  }

  /** Close every already-opened index tree/rtree (not the primary tree or
   * the journal) -- shared by normal close and open()'s failure cleanup. */
  async _closeIndexes() {
    for (const ix of this._indexes.values()) {
      if (ix.kind === 'equality') await ix.tree.close();
      else if (ix.kind === 'text') { for (const role of Object.keys(ix.trees)) await ix.trees[role].close(); }
      else await ix.rt.close();
    }
    this._indexes.clear();
  }

  async _open() {
    await this._tree.open();
    const M = requireModule();
    this._outCtx = M._dcw_out_new();
    if (!this._outCtx) throw new Error('Failed to allocate collection output slot');
    this._collCtx = M._dcw_collection_open(this._tree.ctx);
    if (!this._collCtx) throw new Error('Failed to allocate collection handle');

    // C computes the whole open plan from the catalog entry -- which files,
    // in attach order, with each index's options -- and this loop opens
    // exactly what it named. The backward-compatibility rules (a missing
    // `kind` means equality; a missing `journal` means the generation-0
    // name) are the schema's, so they are in db_catalog.c with it.
    const entry = this._catalog.search(this.name);
    const plan = catalogOpenPlan(entry, this.name);

    for (const def of plan.indexes) {
      const handles = [];
      for (const file of def.files) {
        handles.push(await this._provider.openFile(file, { create: false }));
      }
      if (def.kind === 0) {          // DC_INDEX_EQUALITY
        const tree = new BPlusTree(handles[0], this._order);
        await tree.open();
        const n = allocStr(M, def.name);
        let rc;
        try {
          rc = this._marshalPair(def.fields, def.partialFilterExpression, (M2, fp, flen, pp, plen) =>
            M2._dcw_collection_attach_index(this._collCtx, n.ptr, n.len, tree.ctx, fp, flen,
                                            def.unique ? 1 : 0, def.sparse ? 1 : 0, pp, plen));
        } finally {
          n.free();
        }
        if (rc !== 0) throw codeError(rc, 'attachIndex');
        // A build the catalog says is still in flight reopens exactly as
        // the begin entry left it: maintained by every write, invisible
        // to the planner, waiting for chunk entries to finish it.
        if (def.building) {
          const n2 = allocStr(M, def.name);
          try {
            const rc2 = M._dcw_collection_index_set_building(this._collCtx, n2.ptr, n2.len, 1);
            if (rc2 !== 0) throw codeError(rc2, 'attachIndex');
          } finally { n2.free(); }
        }
        this._indexes.set(def.name, {
          kind: 'equality', fields: def.fields, tree, file: def.files[0],
          unique: !!def.unique, sparse: !!def.sparse,
          partialFilterExpression: def.partialFilterExpression || null,
          expireAfterSeconds: def.expireAfterSeconds,
          building: !!def.building
        });
      } else if (def.kind === 1) {   // DC_INDEX_TEXT
        // files[] is in attach order -- that ordering is the plan's job,
        // not this loop's.
        const trees = {};
        const roles = ['index', 'docTerms', 'docLengths'];
        for (let i = 0; i < roles.length; i++) {
          trees[roles[i]] = new BPlusTree(handles[i], this._order);
          await trees[roles[i]].open();
        }
        const n = allocStr(M, def.name);
        const f = allocStr(M, def.field);
        let rc;
        try {
          rc = M._dcw_collection_attach_text_index(
            this._collCtx, n.ptr, n.len,
            trees.index.ctx, trees.docTerms.ctx, trees.docLengths.ctx,
            f.ptr, f.len
          );
        } finally {
          n.free(); f.free();
        }
        if (rc !== 0) throw codeError(rc, 'attachTextIndex');
        this._indexes.set(def.name, {
          kind: 'text', field: def.field, trees,
          files: { index: def.files[0], docTerms: def.files[1], docLengths: def.files[2] }
        });
      } else {                       // DC_INDEX_GEO
        const rt = new RTree(handles[0]);
        await rt.open();
        const n = allocStr(M, def.name);
        const f = allocStr(M, def.field);
        let rc;
        try {
          rc = M._dcw_collection_attach_geo_index(this._collCtx, n.ptr, n.len, rt.ctx, f.ptr, f.len);
        } finally {
          n.free(); f.free();
        }
        if (rc !== 0) throw codeError(rc, 'attachGeoIndex');
        this._indexes.set(def.name, { kind: 'geo', field: def.field, rt, file: def.files[0] });
      }
    }

    // Cross-file commit journal (milestone 5): must be recovered only after
    // every index above is attached, mirroring TextIndex's tix_recover
    // contract ("right after all trees are open"). Always on -- every
    // collection gets this consistency guarantee automatically.
    this._journal = await this._provider.openFile(plan.journal, { create: true });
    this._journalFd = registerHandle(M, this._journal);
    const rc = M._dcw_collection_recover(this._collCtx, this._journalFd);
    if (rc !== 0) {
      unregisterHandle(M, this._journalFd);
      this._journalFd = -1;
      this._journal.close();
      this._journal = null;
      await this._closeIndexes();
      requireModule()._dcw_collection_free(this._collCtx);
      this._collCtx = 0;
      requireModule()._dcw_out_free(this._outCtx);
      this._outCtx = 0;
      await this._tree.close();
      throw codeError(rc, 'recover');
    }
  }

  async _close() {
    for (const stream of [...this._watchers]) stream.close();
    // _openCursors holds { ptr, close() } tokens, not the cursor objects
    // (see cursorFinalizer above the class). Closing the token frees the
    // WASM-side dc_cursor; the cursor object, if still referenced, sees
    // ptr === 0 on its next pull and reports itself exhausted.
    for (const token of [...this._openCursors]) {
      if (cursorFinalizer) cursorFinalizer.unregister(token);
      token.close();
    }
    this._openCursors.clear();
    await this._closeHandles();
  }

  /** Release every file handle and WASM context (indexes, journal,
   * dc_collection, output slot, primary tree) without touching the
   * watcher/cursor bookkeeping -- shared by _close() and compact()'s
   * adopt step, which must keep watchers alive across its reopen. */
  async _closeHandles() {
    await this._closeIndexes();
    if (this._journalFd >= 0) {
      unregisterHandle(requireModule(), this._journalFd);
      this._journalFd = -1;
      this._journal.flush();
      this._journal.close();
      this._journal = null;
    }
    if (this._collCtx) {
      requireModule()._dcw_collection_free(this._collCtx);
      this._collCtx = 0;
    }
    if (this._outCtx) {
      requireModule()._dcw_out_free(this._outCtx);
      this._outCtx = 0;
    }
    await this._tree.close();
  }

  _readOut(M) {
    const ptr = M._dcw_out_ptr(this._outCtx);
    const len = M._dcw_out_len(this._outCtx);
    if (len < 0) throw codeError(len, 'find');
    if (len === 0) return undefined;
    return decode(M.HEAPU8.slice(ptr, ptr + len));
  }

  /**
   * Record one index in the catalog entry. C converts the plan-shaped
   * definition into the stored form (db_catalog.h) -- the only place that
   * conversion exists.
   *
   * This replaced _persistIndexes, which rebuilt the entire `indexes`
   * array from this class's in-memory Map on every change. That made the
   * Map the effective source of truth for on-disk data; the entry is now,
   * and the Map is a cache of live handles.
   */
  _catalogPutIndex(def) {
    const entry = this._catalog.search(this.name);
    const updated = catalogCall((M, ctx) => {
      const ee = encode(entry), de = encode(def);
      const ep = M._malloc(ee.length || 1);
      const dp = M._malloc(de.length || 1);
      try {
        if (ee.length) M.HEAPU8.set(ee, ep);
        if (de.length) M.HEAPU8.set(de, dp);
        return M._catw_put_index(ctx, ep, ee.length, dp, de.length);
      } finally { M._free(dp); M._free(ep); }
    });
    this._catalog.add(this.name, updated);
  }

  _catalogDropIndex(name) {
    const entry = this._catalog.search(this.name);
    const updated = catalogCall((M, ctx) => {
      const ee = encode(entry);
      const ep = M._malloc(ee.length || 1);
      const n = allocStr(M, name);
      try {
        if (ee.length) M.HEAPU8.set(ee, ep);
        return M._catw_drop_index(ctx, ep, ee.length, n.ptr, n.len);
      } finally { n.free(); M._free(ep); }
    });
    this._catalog.add(this.name, updated);
  }


  /**
   * Create a secondary index:
   *   - equality: createIndex({ team: 1 }) or a compound createIndex({ team: 1, age: 1 }).
   *     Options: `unique` (reject a write whose field values collide with
   *     another document's), `sparse` (skip, don't error, a document
   *     missing a field instead of the default all-or-nothing backfill/
   *     maintenance), `partialFilterExpression` (a filter — only matching
   *     documents are indexed), `expireAfterSeconds` (TTL — single-field
   *     only; see pruneExpired()). A document skipped by sparse/
   *     partialFilterExpression can never violate unique on that index.
   *   - text: createIndex({ body: 'text' }) — single field, BM25-scored via $text.
   *     At most one text index per collection (matches MongoDB).
   *   - geo: createIndex({ location: '2dsphere' }) — single field, GeoJSON Point
   *     values, queried via $near/$geoWithin (see docs/db-plan.md milestone 6).
   * Backfills against any existing documents — all-or-nothing for equality/geo
   * indexes (a disqualifying field, or -- for a unique index -- a pre-existing
   * duplicate value, fails the whole call), but a text index tolerates
   * documents missing the field or holding a non-string value (they just
   * aren't text-searchable), matching MongoDB's own behavior for each.
   * Returns the index name (options.name, or a MongoDB-shaped default).
   */
  async createIndex(keys, options = {}) {
    // C decides what kind of index this is, what it is called, and which
    // files it needs -- all pure, all before anything is created. This
    // method then creates exactly the files the plan named.
    const plan = indexCreatePlan(keys, options, this.name);
    const name = plan.name;
    if (this._indexes.has(name)) throw new Error(`Index already exists: ${name}`);
    if (plan.kind === 1) return this._createTextIndex(plan, options);
    if (plan.kind === 2) return this._createGeoIndex(plan, options);

    const fields = plan.fields;
    const fileName = plan.files[0];
    await this._provider.deleteFile(fileName); // clean slate in case a prior attempt was aborted
    const tree = new BPlusTree(await this._provider.openFile(fileName, { create: true }), this._order);
    await tree.open();

    const M = requireModule();
    const n = allocStr(M, name);
    const unique = !!plan.unique, sparse = !!plan.sparse;
    const partialFilterExpression = plan.partialFilterExpression || null;
    let rc;
    try {
      rc = this._marshalPair(fields, partialFilterExpression, (M2, fp, flen, pp, plen) =>
        M2._dcw_collection_add_index(this._collCtx, n.ptr, n.len, tree.ctx, fp, flen,
                                     unique ? 1 : 0, sparse ? 1 : 0, pp, plen));
    } finally {
      n.free();
    }

    if (rc !== 0) {
      await tree.close();
      await this._provider.deleteFile(fileName);
      throw codeError(rc, 'createIndex');
    }

    this._indexes.set(name, {
      kind: 'equality', fields, tree, file: fileName, unique, sparse, partialFilterExpression,
      expireAfterSeconds: plan.expireAfterSeconds
    });
    // The plan is already the stored shape's input -- one definition
    // flows create -> catalog -> open, rather than being rebuilt here.
    this._catalogPutIndex(plan);
    return name;
  }

  /* Record one definition's staged-build state in the catalog entry:
   * building on/off, and the backfill cursor (12 raw bytes) while on.
   * The C transform is the same one the server's session uses, so the
   * two hosts cannot spell the state differently. */
  _catalogSetIndexBuilding(name, building, cursorBytes) {
    const entry = this._catalog.search(this.name);
    const updated = catalogCall((M, ctx) => {
      const ee = encode(entry);
      const ep = M._malloc(ee.length || 1);
      const n = allocStr(M, name);
      const cp = cursorBytes ? M._malloc(12) : 0;
      try {
        if (ee.length) M.HEAPU8.set(ee, ep);
        if (cursorBytes) M.HEAPU8.set(cursorBytes, cp);
        return M._catw_index_building_set(ctx, ep, ee.length, n.ptr, n.len,
                                          building ? 1 : 0, cp);
      } finally { if (cp) M._free(cp); n.free(); M._free(ep); }
    });
    this._catalog.add(this.name, updated);
  }

  /*
   * THE STAGED BUILD'S JS TWIN (db_session.h's dbs_index_begin /
   * dbs_index_chunk). These exist so a log written by a replicated
   * server -- where one createIndex is a begin entry and N chunk
   * entries -- replays through this host too (db-wal.js's
   * _applyCommand): the one-artifact contract says a database is
   * openable by every host, logs included.
   *
   * This host is SINGLE-COPY, so it skips the C session's
   * already-applied guard: a recovery that re-runs an applied chunk
   * advances the cursor further and later chunks answer done sooner,
   * which converges on the identical final index (every add is
   * if-absent) -- there is no second member whose cursor could
   * disagree.
   */
  async indexBegin(keys, options = {}) {
    const plan = indexCreatePlan(keys, options, this.name);
    const name = plan.name;
    if (plan.kind !== 0) throw codeError(-58, 'indexBegin');  // DC_ERR_INDEX_KIND
    const held = this._indexes.get(name);
    if (held) {
      if (held.building) return name;   // replay: the build is under way
      throw new Error(`Index already exists: ${name}`);
    }

    const fields = plan.fields;
    const fileName = plan.files[0];
    await this._provider.deleteFile(fileName);
    const tree = new BPlusTree(await this._provider.openFile(fileName, { create: true }), this._order);
    await tree.open();

    const M = requireModule();
    const n = allocStr(M, name);
    const unique = !!plan.unique, sparse = !!plan.sparse;
    const partialFilterExpression = plan.partialFilterExpression || null;
    let rc;
    try {
      rc = this._marshalPair(fields, partialFilterExpression, (M2, fp, flen, pp, plen) =>
        M2._dcw_collection_add_index_staged(this._collCtx, n.ptr, n.len, tree.ctx, fp, flen,
                                            unique ? 1 : 0, sparse ? 1 : 0, pp, plen));
    } finally {
      n.free();
    }
    if (rc !== 0) {
      await tree.close();
      await this._provider.deleteFile(fileName);
      throw codeError(rc, 'indexBegin');
    }

    this._indexes.set(name, {
      kind: 'equality', fields, tree, file: fileName, unique, sparse, partialFilterExpression,
      expireAfterSeconds: plan.expireAfterSeconds, building: true
    });
    this._catalogPutIndex(plan);
    this._catalogSetIndexBuilding(name, true, null);
    return name;
  }

  async indexChunk(name, k) {
    // Benign on every path that is not "a build in progress" -- the
    // same answers the C session gives, for the same reasons.
    const entry = this._catalog.search(this.name);
    if (!entry) return { advanced: 0, done: true };
    const state = catalogCall((M, ctx) => {
      const ee = encode(entry);
      const ep = M._malloc(ee.length || 1);
      const n = allocStr(M, name);
      try {
        if (ee.length) M.HEAPU8.set(ee, ep);
        return M._catw_index_building_get(ctx, ep, ee.length, n.ptr, n.len);
      } finally { n.free(); M._free(ep); }
    });
    if (!state.found || !state.building) return { advanced: 0, done: true };
    if (!this._indexes.get(name)) return { advanced: 0, done: true };

    const M = requireModule();
    const cursorBytes = state.cursor ? state.cursor.toBytes() : null;
    const n = allocStr(M, name);
    const cp = cursorBytes ? M._malloc(12) : 0;
    let rc;
    try {
      if (cursorBytes) M.HEAPU8.set(cursorBytes, cp);
      rc = M._dcw_backfill_step(this._outCtx, this._collCtx, n.ptr, n.len, cp, k);
    } finally { if (cp) M._free(cp); n.free(); }
    if (rc !== 0) {
      // The build cannot complete (a genuine duplicate under unique, an
      // unindexable document under a non-sparse spec): unwind it --
      // definition, handles, file -- and surface the code, exactly as
      // the C chunk apply does.
      await this.dropIndex(name);
      throw codeError(rc, 'indexChunk');
    }
    const step = this._readOut(requireModule()) ?? {};
    if (step.done) {
      const n2 = allocStr(M, name);
      try {
        const rc2 = M._dcw_collection_index_set_building(this._collCtx, n2.ptr, n2.len, 0);
        if (rc2 !== 0) throw codeError(rc2, 'indexChunk');
      } finally { n2.free(); }
      const held = this._indexes.get(name);
      if (held) held.building = false;
      this._catalogSetIndexBuilding(name, false, null);
    } else {
      this._catalogSetIndexBuilding(name, true, step.last ? step.last.toBytes() : cursorBytes);
    }
    return { advanced: step.advanced ?? 0, done: !!step.done };
  }

  async _createTextIndex(plan, options = {}) {
    const name = plan.name;
    const field = plan.field;
    // plan.files is in attach order; the role names are only for the
    // in-memory bookkeeping below.
    const roles = ['index', 'docTerms', 'docLengths'];
    const files = { index: plan.files[0], docTerms: plan.files[1], docLengths: plan.files[2] };
    const trees = {};
    for (let i = 0; i < roles.length; i++) {
      await this._provider.deleteFile(plan.files[i]);
      trees[roles[i]] = new BPlusTree(await this._provider.openFile(plan.files[i], { create: true }), this._order);
      await trees[roles[i]].open();
    }

    const M = requireModule();
    const n = allocStr(M, name);
    const f = allocStr(M, field);
    let rc;
    try {
      rc = M._dcw_collection_add_text_index(
        this._collCtx, n.ptr, n.len,
        trees.index.ctx, trees.docTerms.ctx, trees.docLengths.ctx,
        f.ptr, f.len
      );
    } finally {
      n.free(); f.free();
    }

    if (rc !== 0) {
      for (const role of Object.keys(files)) await trees[role].close();
      for (const role of Object.keys(files)) await this._provider.deleteFile(files[role]);
      throw codeError(rc, 'createIndex');
    }

    this._indexes.set(name, { kind: 'text', field, trees, files });
    this._catalogPutIndex(plan);
    return name;
  }

  async _createGeoIndex(plan, options = {}) {
    const name = plan.name;
    const field = plan.field;
    const fileName = plan.files[0];
    await this._provider.deleteFile(fileName);
    const rt = new RTree(await this._provider.openFile(fileName, { create: true }));
    await rt.open();

    const M = requireModule();
    const n = allocStr(M, name);
    const f = allocStr(M, field);
    let rc;
    try {
      rc = M._dcw_collection_add_geo_index(this._collCtx, n.ptr, n.len, rt.ctx, f.ptr, f.len);
    } finally {
      n.free(); f.free();
    }

    if (rc !== 0) {
      await rt.close();
      await this._provider.deleteFile(fileName);
      throw codeError(rc, 'createIndex');
    }

    this._indexes.set(name, { kind: 'geo', field, rt, file: fileName });
    this._catalogPutIndex(plan);
    return name;
  }

  async dropIndex(name) {
    const entry = this._indexes.get(name);
    if (!entry) throw new Error(`Index not found: ${name}`);
    const M = requireModule();
    const n = allocStr(M, name);
    let rc;
    try {
      rc = M._dcw_collection_remove_index(this._collCtx, n.ptr, n.len);
    } finally {
      n.free();
    }
    if (rc !== 0) throw codeError(rc, 'dropIndex');

    if (entry.kind === 'equality') {
      await entry.tree.close();
      await this._provider.deleteFile(entry.file);
    } else if (entry.kind === 'text') {
      for (const role of Object.keys(entry.trees)) await entry.trees[role].close();
      for (const role of Object.keys(entry.files)) await this._provider.deleteFile(entry.files[role]);
    } else {
      await entry.rt.close();
      await this._provider.deleteFile(entry.file);
    }
    this._indexes.delete(name);
    this._catalogDropIndex(name);
  }

  async listIndexes() {
    // Reconstructing `key` from the stored `fields` is the inverse of what
    // createIndex did, so the two have to agree -- which is why it lives
    // with the schema (db_catalog.h) rather than here.
    const entry = this._catalog.search(this.name);
    return catalogCall((M, ctx) => {
      const enc = encode(entry);
      const ep = M._malloc(enc.length || 1);
      try {
        if (enc.length) M.HEAPU8.set(enc, ep);
        return M._catw_list_indexes(ctx, ep, enc.length);
      } finally { M._free(ep); }
    });
  }


  /**
   * Every document whose indexed fields equal `values` (in the index's
   * field order), via an O(log n + k) index scan of an *equality* index —
   * for $text/$near/$geoWithin, use find()/findOne() with the matching
   * filter operator instead (db.c dispatches to the right index).
   */
  /**
   * Direct equality lookup through a named index, bypassing the planner.
   *
   * No index registry check here: whether this collection has an index
   * of that name, and whether it is the right kind, is C's to answer
   * (DC_ERR_NO_INDEX / DC_ERR_INDEX_KIND / DC_ERR_INDEX_ARITY) -- it is
   * the collection that holds the indexes. This side used to check both
   * against its own `_indexes` map, which was a second opinion that
   * could disagree with the collection it was describing.
   */
  async findByIndex(name, values) {
    const M = requireModule();
    const n = allocStr(M, name);
    const valuesBytes = encode(values);
    let rc;
    try {
      rc = withBytes(M, valuesBytes, (vp, vlen) =>
        M._dcw_find_by_index(this._outCtx, this._collCtx, n.ptr, n.len, vp, vlen));
    } finally {
      n.free();
    }
    if (rc !== 0) throw codeError(rc, 'findByIndex');
    return this._readOut(M) ?? [];
  }

  async insertOne(doc) {
    if (doc === null || typeof doc !== 'object' || Array.isArray(doc)) {
      throw new Error('insertOne requires a document object');
    }
    const M = requireModule();
    const _id = doc._id !== undefined ? toObjectId(doc._id) : new ObjectId();
    const bytes = encode({ ...doc, _id });
    const rc = withBytes(M, bytes, (p, n) => M._dcw_insert_one(this._collCtx, p, n));
    if (rc !== 0) throw codeError(rc, 'insertOne');
    this._emitChange({ operationType: 'insert', documentKey: { _id }, fullDocument: { ...doc, _id } });
    return { acknowledged: true, insertedId: _id };
  }

  /**
   * Apply one logged WAL command — the replay path, shared by crash
   * recovery and by every replica's Raft apply loop (src/db-wal.js's
   * _applyCommand).
   *
   * The whole mutation is C's (db_wal.h's dc_wal_apply): it stages the
   * entry's index, performs the write the command names by _id, and
   * shapes the result. That is what makes a committed entry applicable
   * without a JavaScript runtime — the reason this method exists rather
   * than the apply loop calling insertOne/updateOne/deleteOne, which
   * would put the command's meaning back on this side of the bridge.
   *
   * What stays here is what only a browser has: change streams. They are
   * emitted from the command and the result, and the command is not even
   * decoded when nobody is watching.
   */
  async applyCommand(index, payload) {
    const result = walApply(this, index, payload);
    if (this._watchers.size) await this._emitApplied(payload, result);
    return result;
  }

  /** The change event one applied command implies. Reads the command
   * only to name the document; what HAPPENED comes from the result. */
  async _emitApplied(payload, result) {
    const cmd = decode(payload);
    if (result.insertedId !== undefined) {
      this._emitChange({
        operationType: 'insert',
        documentKey: { _id: result.insertedId },
        fullDocument: cmd.doc
      });
      return;
    }
    if (result.deletedCount !== undefined) {
      if (result.deletedCount) this._emitChange({ operationType: 'delete', documentKey: { _id: cmd.id } });
      return;
    }
    if (!result.matchedCount) return;   // the document was already gone
    const _id = cmd.id;
    this._emitChange(cmd.update
      // An update names its changes, not its outcome, so the full
      // document has to be read back — exactly as updateOne does.
      ? { operationType: 'update', documentKey: { _id }, fullDocument: await this.findOne({ _id }) }
      : { operationType: 'replace', documentKey: { _id }, fullDocument: cmd.doc });
  }

  /**
   * Insert every document in `docs`. `ordered` (default true) stops at the
   * first failing document; `false` attempts every document regardless of
   * earlier failures. Each document's _id is assigned client-side up front
   * (same convention as insertOne) so the result's insertedIds can be built
   * directly from ids already known here — dcw_insert_many's out slot only
   * needs to report success/failure per index (see dc_insert_many).
   */
  async insertMany(docs, { ordered = true } = {}) {
    if (!Array.isArray(docs) || docs.length === 0) {
      throw new Error('insertMany requires a non-empty array of documents');
    }
    const M = requireModule();
    const ids = docs.map(doc => doc._id !== undefined ? toObjectId(doc._id) : new ObjectId());
    const bytes = encode(docs.map((doc, i) => ({ ...doc, _id: ids[i] })));
    const rc = withBytes(M, bytes, (p, n) => M._dcw_insert_many(this._outCtx, this._collCtx, p, n, ordered ? 1 : 0));
    if (rc !== 0) throw codeError(rc, 'insertMany');

    const results = this._readOut(M); // one error code per attempted document
    const insertedIds = {};
    let insertedCount = 0;
    for (let i = 0; i < results.length; i++) {
      if (results[i] === 0) {
        insertedIds[i] = ids[i];
        insertedCount++;
      } else {
        const err = codeError(results[i], `insertMany (document ${i})`);
        err.result = { acknowledged: true, insertedCount, insertedIds };
        throw err;
      }
    }
    for (let i = 0; i < results.length; i++) {
      if (results[i] === 0) {
        this._emitChange({ operationType: 'insert', documentKey: { _id: ids[i] }, fullDocument: { ...docs[i], _id: ids[i] } });
      }
    }
    return { acknowledged: true, insertedCount, insertedIds };
  }

  /** `options.projection` follows the same inclusion-XOR-exclusion rules as find()'s (`_id` defaults included). */
  async findOne(filter = {}, options = {}) {
    const M = requireModule();
    const fbytes = encode(filter);
    const projBytes = options.projection ? encode(options.projection) : new Uint8Array(0);

    const fp = fbytes.length ? M._malloc(fbytes.length) : 0;
    const pp = projBytes.length ? M._malloc(projBytes.length) : 0;
    if (fbytes.length) M.HEAPU8.set(fbytes, fp);
    if (projBytes.length) M.HEAPU8.set(projBytes, pp);

    let found;
    try {
      found = M._dcw_find_one(this._outCtx, this._collCtx, fp, fbytes.length, pp, projBytes.length);
    } finally {
      if (fp) M._free(fp);
      if (pp) M._free(pp);
    }
    if (found < 0) throw codeError(found, 'findOne');
    return found ? this._readOut(M) : null;
  }

  /**
   * The _id a filter-based mutation (updateOne/deleteOne/etc.) is about to
   * affect, resolved *before* the mutation runs (the filter may no longer
   * match afterward) -- for building a watch() change event. Free when the
   * filter already names `_id` directly; otherwise one extra findOne, only
   * ever called when this collection actually has active watchers.
   */
  async _resolveDocumentKeyForWatch(filter) {
    if (filter && filter._id !== undefined) return toObjectId(filter._id);
    const doc = await this.findOne(filter);
    return doc ? doc._id : null;
  }

  /**
   * Mirrors the driver's find(): returns a cursor, not a promise. Accepts
   * options up front ({ sort, skip, limit, projection }) and/or the
   * driver's chainable .sort()/.skip()/.limit()/.project() — both set the
   * same underlying state, so they can be mixed.
   */
  /**
   * Sorted queries fall back to the eager path below (dcw_find,
   * materializing every match before returning) since an arbitrary
   * in-memory sort fundamentally needs every match before it can emit the
   * first ordered result. An unsorted find() streams instead, in bounded
   * batches, via the WASM-side dc_cursor -- see db.h's comment on it.
   */
  find(filter = {}, options = {}) {
    const collection = this;
    const state = {
      sort: options.sort || null,
      skip: options.skip || 0,
      limit: options.limit || 0,
      projection: options.projection || null
    };
    const BATCH = 100;

    async function eagerToArray() {
      while (collection._compacting) await collection._compacting;
      collection._inFlight++; // same gate + count as the prototype wrapper -- see the constructor
      try {
        const M = requireModule();
        const fBytes = encode(filter);
        const sortBytes = state.sort ? encode(state.sort) : new Uint8Array(0);
        const projBytes = state.projection ? encode(state.projection) : new Uint8Array(0);

        const fp = fBytes.length ? M._malloc(fBytes.length) : 0;
        const sp = sortBytes.length ? M._malloc(sortBytes.length) : 0;
        const pp = projBytes.length ? M._malloc(projBytes.length) : 0;
        if (fBytes.length) M.HEAPU8.set(fBytes, fp);
        if (sortBytes.length) M.HEAPU8.set(sortBytes, sp);
        if (projBytes.length) M.HEAPU8.set(projBytes, pp);

        let rc;
        try {
          rc = M._dcw_find(
            collection._outCtx, collection._collCtx,
            fp, fBytes.length,
            sp, sortBytes.length,
            state.skip, state.limit,
            pp, projBytes.length
          );
        } finally {
          if (fp) M._free(fp);
          if (sp) M._free(sp);
          if (pp) M._free(pp);
        }
        if (rc !== 0) throw codeError(rc, 'find');
        return collection._readOut(M) ?? [];
      } finally {
        collection._opDone();
      }
    }

    // The live WASM-side dc_cursor, held via a plain { ptr, close() }
    // token (makeCursorToken) in collection._openCursors rather than the
    // cursor object itself, so an abandoned cursor stays collectable and
    // cursorFinalizer can reclaim it (see both doc comments above class
    // Collection).
    let cursorToken = null;
    let exhausted = false;
    let pending = []; // docs fetched but not yet handed out
    let pendingIdx = 0;

    function openWasmCursor() {
      const M = requireModule();
      const fBytes = encode(filter);
      const projBytes = state.projection ? encode(state.projection) : new Uint8Array(0);
      const fp = fBytes.length ? M._malloc(fBytes.length) : 0;
      const pp = projBytes.length ? M._malloc(projBytes.length) : 0;
      const errP = M._malloc(4);
      if (fBytes.length) M.HEAPU8.set(fBytes, fp);
      if (projBytes.length) M.HEAPU8.set(projBytes, pp);
      let ptr;
      try {
        ptr = M._dcw_cursor_open(
          collection._collCtx,
          fp, fBytes.length,
          state.skip, state.limit,
          pp, projBytes.length,
          errP
        );
        if (!ptr) throw codeError(readI32(M, errP), 'find');
      } finally {
        if (fp) M._free(fp);
        if (pp) M._free(pp);
        M._free(errP);
      }
      cursorToken = makeCursorToken(ptr); // module-level factory -- see its doc comment
      collection._openCursors.add(cursorToken);
      if (cursorFinalizer) {
        cursorFinalizer.register(fcursor, { token: cursorToken, cursors: collection._openCursors }, cursorToken);
      }
    }

    function closeWasmCursor() {
      if (cursorToken) {
        if (cursorFinalizer) cursorFinalizer.unregister(cursorToken);
        collection._openCursors.delete(cursorToken);
        cursorToken.close();
        cursorToken = null;
      }
    }

    async function fetchBatch(maxCount) {
      while (collection._compacting) await collection._compacting;
      collection._inFlight++; // same gate + count as the prototype wrapper -- see the constructor
      try {
        const M = requireModule();
        if (cursorToken && !cursorToken.ptr) {
          // Closed underneath us (Collection._close during shutdown).
          exhausted = true;
          cursorToken = null;
          return [];
        }
        if (!cursorToken) openWasmCursor();
        const doneP = M._malloc(4);
        let rc, done;
        try {
          rc = M._dcw_cursor_next_batch(collection._outCtx, cursorToken.ptr, maxCount, doneP);
          if (rc !== 0) throw codeError(rc, 'find');
          done = !!readU32(M, doneP);
        } finally {
          M._free(doneP);
        }
        const batch = collection._readOut(M) ?? [];
        if (done) { exhausted = true; closeWasmCursor(); }
        return batch;
      } finally {
        collection._opDone();
      }
    }

    const fcursor = {
      sort(spec) { state.sort = spec; return fcursor; },
      skip(n) { state.skip = n; return fcursor; },
      limit(n) { state.limit = n; return fcursor; },
      project(spec) { state.projection = spec; return fcursor; },

      async toArray() {
        if (state.sort) return eagerToArray();
        const all = pending.slice(pendingIdx);
        pendingIdx = pending.length;
        while (!exhausted) all.push(...(await fetchBatch(BATCH)));
        return all;
      },

      /** Manual pull, `{ value, done }` -- same shape as ChangeStream's. Sorted cursors don't support this: call toArray() instead. */
      async next() {
        if (state.sort) throw new Error('find().next() is not supported with .sort() -- use toArray() or for-await instead');
        if (pendingIdx >= pending.length) {
          if (exhausted) return { value: undefined, done: true };
          pending = await fetchBatch(BATCH);
          pendingIdx = 0;
          if (pending.length === 0) return { value: undefined, done: true };
        }
        return { value: pending[pendingIdx++], done: false };
      },

      [Symbol.asyncIterator]() {
        return state.sort ? eagerIterator() : fcursor;
      },

      /** The plan this cursor's filter gets -- see Collection.explain. */
      async explain() {
        return collection.explain(filter);
      },

      /** Releases the underlying WASM cursor if one is open. Safe to call more than once, or on an already-exhausted/sorted cursor. */
      async close() {
        exhausted = true;
        closeWasmCursor();
      },

      /** Invoked by `for await` on early exit (break/throw) -- releases the WASM cursor instead of leaking it. */
      async return() {
        await fcursor.close();
        return { value: undefined, done: true };
      }
    };

    async function* eagerIterator() {
      for (const doc of await eagerToArray()) yield doc;
    }

    return fcursor;
  }

  /**
   * Which candidate source the query dispatch would use for `filter`,
   * without executing it -- dcw_explain consults the very planners the
   * queries run (db.c's dc_explain), so this can never drift from
   * reality. Returns { source, index }: source is 'ids' ({_id} point
   * lookup), 'equality' | 'text' | 'geo' (with the serving index's
   * name), or 'scan' (full collection scan -- the signal to add an
   * index). The same plan serves find()'s streaming and sorted paths
   * (sorting happens after gathering), findOne, countDocuments,
   * updateMany/deleteMany and distinct. Sugar: find(filter).explain().
   */
  async explain(filter = {}) {
    const M = requireModule();
    const fbytes = encode(filter);
    const rc = withBytes(M, fbytes, (p, n) => M._dcw_explain(this._outCtx, this._collCtx, p, n));
    if (rc !== 0) throw codeError(rc, 'explain');
    // The plan's NAME is C's (dc_explain_source), not an array here that
    // a second host could spell differently -- which is exactly what
    // happened when the server needed one.
    const { source, index } = this._readOut(M);
    return { source, index: index ?? null };
  }

  /**
   * A deliberately small aggregation subset, executed in JS (docs/
   * db-api.md "Aggregation"): $match, $sort, $skip, $limit, $project,
   * $group (accumulators $sum/$avg/$min/$max/$first/$last/$push/
   * $addToSet/$count), $count. A pipeline-LEADING $match is pushed down
   * into find() -- full engine operator grammar ($text/$regex/$near...)
   * and index planning apply there; every later stage runs over the
   * materialized documents with the explicit semantics of the agg*
   * helpers above (later $match stages accept the documented subset
   * only). Returns a cursor-like handle: toArray()/next()/for-await/
   * close(), resolving with one execution on first pull. The inner
   * find() carries the compaction gate + in-flight count, so this
   * method needs neither itself.
   */
  aggregate(pipeline = []) {
    if (!Array.isArray(pipeline)) throw new Error('aggregate requires a pipeline array');
    const collection = this;
    let items = null;
    let idx = 0;
    // The whole pipeline runs in C (db_agg.h), including the decision to
    // push a leading $match into the scan so the planner and any index
    // can serve it. This layer only marshals the stages in and the
    // documents out.
    const run = async () => {
      const M = requireModule();
      const encoded = encode(pipeline);
      const ptr = M._malloc(encoded.length || 1);
      const badP = M._malloc(4);
      try {
        if (encoded.length) M.HEAPU8.set(encoded, ptr);
        const rc = M._dcw_aggregate(collection._outCtx, collection._collCtx, ptr, encoded.length, badP);
        if (rc !== 0) {
          // C reports which stage failed; this side holds the pipeline, so
          // it can quote the stage without C formatting user data.
          const at = readI32(M, badP);
          const stage = at >= 0 && at < pipeline.length ? JSON.stringify(pipeline[at]) : null;
          throw codeError(rc, stage ? `stage ${at}: ${stage}` : 'aggregate');
        }
        return collection._readOut(M) ?? [];
      } finally { M._free(badP); M._free(ptr); }
    };
    const cursor = {
      async toArray() {
        if (items === null) items = await run();
        const rest = items.slice(idx);
        idx = items.length;
        return rest;
      },
      async next() {
        if (items === null) items = await run();
        return idx < items.length ? { value: items[idx++], done: false } : { value: undefined, done: true };
      },
      [Symbol.asyncIterator]() { return cursor; },
      async close() { items = items || []; idx = items.length; },
      async return() { await cursor.close(); return { value: undefined, done: true }; }
    };
    return cursor;
  }

  async deleteOne(filter = {}) {
    const M = requireModule();
    const watching = this._watchers.size > 0;
    const preId = watching ? await this._resolveDocumentKeyForWatch(filter) : null;
    const fbytes = encode(filter);
    const rc = withBytes(M, fbytes, (p, n) => M._dcw_delete_one(this._collCtx, p, n));
    if (rc < 0) throw codeError(rc, 'deleteOne');
    if (watching && rc === 1 && preId) this._emitChange({ operationType: 'delete', documentKey: { _id: preId } });
    return { acknowledged: true, deletedCount: rc };
  }

  /** Delete every document matching `filter`. */
  async deleteMany(filter = {}) {
    const M = requireModule();
    const watching = this._watchers.size > 0;
    // C records each removed _id as it commits (dc_delete_many's `ids`),
    // so a watched deleteMany no longer needs the projected find() that
    // used to run first just to know what was about to disappear.
    const fbytes = encode(filter);
    const n = withBytes(M, fbytes, (p, len) =>
      M._dcw_delete_many(this._outCtx, this._collCtx, p, len, watching ? 1 : 0));
    if (n < 0) throw codeError(n, 'deleteMany');
    if (watching) {
      for (const _id of this._readOut(requireModule()) ?? []) {
        this._emitChange({ operationType: 'delete', documentKey: { _id } });
      }
    }
    return { acknowledged: true, deletedCount: n };
  }

  /** Atomically find the first document matching `filter` and delete it,
   * returning the deleted document (or null if nothing matched). */
  async findOneAndDelete(filter = {}) {
    const M = requireModule();
    const fbytes = encode(filter);
    const found = withBytes(M, fbytes, (p, n) => M._dcw_find_one_and_delete(this._outCtx, this._collCtx, p, n));
    if (found < 0) throw codeError(found, 'findOneAndDelete');
    if (!found) return null;
    const doc = this._readOut(M);
    this._emitChange({ operationType: 'delete', documentKey: { _id: doc._id } });
    return doc;
  }

  /**
   * Malloc `a`/`b` (encoded; `b` may be null/undefined for "no bytes"),
   * call fn(M, ap, aLen, bp, bLen), free everything, and return the
   * result. Shared by createIndex/reattach-on-open, which both need to
   * pass an equality index's fields plus an optional
   * partialFilterExpression across the bridge.
   */
  _marshalPair(a, b, fn) {
    const M = requireModule();
    const aBytes = encode(a);
    const bBytes = b != null ? encode(b) : new Uint8Array(0);
    const ap = aBytes.length ? M._malloc(aBytes.length) : 0;
    const bp = bBytes.length ? M._malloc(bBytes.length) : 0;
    if (aBytes.length) M.HEAPU8.set(aBytes, ap);
    if (bBytes.length) M.HEAPU8.set(bBytes, bp);
    try {
      return fn(M, ap, aBytes.length, bp, bBytes.length);
    } finally {
      if (ap) M._free(ap);
      if (bp) M._free(bp);
    }
  }

  /**
   * Malloc `a`/`b` (encoded) plus 24 bytes of id space, call
   * fn(M, aPtr, aLen, bPtr, bLen, idPtr, outIdPtr), free everything, and
   * return { rc, upsertedId }. Shared by replaceOne/updateOne/updateMany,
   * which all pass a filter + a second document and may need a fresh id
   * for an upsert (see the Db/Collection section's top comment for why JS
   * always generates one rather than C inventing it).
   *
   * `upsertedId` is what C says it used, NOT the id passed in. Those
   * differ whenever the filter or the replacement pinned an `_id`: the
   * upserted document takes that one. This side used to work the answer
   * out for itself (`replacement._id !== undefined ? ... : defaultId`),
   * which is the rule written twice -- and the update form's copy was
   * simply wrong, reporting a generated id for a document stored under
   * the filter's.
   *
   * The id used to be overridable, so the WAL layer could pin the one a
   * logged upsert command had resolved at proposal time. It no longer
   * needs to: the WAL resolves an upsert into a plain insert before
   * logging (db_wal.h), so no logged command reaches these methods with
   * an upsert to perform.
   */
  _marshalTriple(a, b, fn) {
    const M = requireModule();
    const aBytes = encode(a);
    const bBytes = encode(b);

    const ap = aBytes.length ? M._malloc(aBytes.length) : 0;
    const bp = bBytes.length ? M._malloc(bBytes.length) : 0;
    const dp = M._malloc(24);          // [0,12) the default id, [12,24) C's answer
    if (aBytes.length) M.HEAPU8.set(aBytes, ap);
    if (bBytes.length) M.HEAPU8.set(bBytes, bp);
    M.HEAPU8.set(new ObjectId().toBytes(), dp);

    try {
      const rc = fn(M, ap, aBytes.length, bp, bBytes.length, dp, dp + 12);
      // Only meaningful when C reports an upsert (rc === 2); read
      // unconditionally so the caller has one thing to look at.
      return { rc, upsertedId: new ObjectId(M.HEAPU8.slice(dp + 12, dp + 24)) };
    } finally {
      if (ap) M._free(ap);
      if (bp) M._free(bp);
      M._free(dp);
    }
  }

  async replaceOne(filter, replacement, { upsert = false } = {}) {
    if (replacement === null || typeof replacement !== 'object' || Array.isArray(replacement)) {
      throw new Error('replaceOne requires a replacement document object');
    }
    const watching = this._watchers.size > 0;
    const preId = watching ? await this._resolveDocumentKeyForWatch(filter) : null;
    const { rc, upsertedId } = this._marshalTriple(filter, replacement, (M, fp, fn, rp, rn, dp, op) =>
      M._dcw_replace_one(this._collCtx, fp, fn, rp, rn, dp, upsert ? 1 : 0, op));
    if (rc < 0) throw codeError(rc, 'replaceOne');

    if (rc === 0) return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedId: null };
    if (rc === 2) {
      if (watching) {
        this._emitChange({ operationType: 'insert', documentKey: { _id: upsertedId }, fullDocument: { ...replacement, _id: upsertedId } });
      }
      return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedId };
    }
    if (watching && preId) {
      this._emitChange({ operationType: 'replace', documentKey: { _id: preId }, fullDocument: { ...replacement, _id: preId } });
    }
    return { acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedId: null };
  }

  /**
   * Atomically find the first document matching `filter` and replace it,
   * returning the pre-image (`returnDocument: 'before'`, the default) or
   * the post-image (`'after'`) — or null if nothing matched and no upsert
   * happened (or `returnDocument: 'before'` with an upsert: no prior state
   * to return, matching real MongoDB).
   */
  async findOneAndReplace(filter, replacement, { upsert = false, returnDocument = 'before' } = {}) {
    if (replacement === null || typeof replacement !== 'object' || Array.isArray(replacement)) {
      throw new Error('findOneAndReplace requires a replacement document object');
    }
    const returnNew = returnDocument === 'after';
    const { rc } = this._marshalTriple(filter, replacement, (M, fp, fn, rp, rn, dp) =>
      M._dcw_find_one_and_replace(this._outCtx, this._collCtx, fp, fn, rp, rn, dp, upsert ? 1 : 0, returnNew ? 1 : 0));
    // No upserted-id out-param: these return the document itself, which
    // carries the id whatever it turned out to be.
    if (rc < 0) throw codeError(rc, 'findOneAndReplace');
    if (!rc) return null;
    const doc = this._readOut(requireModule());
    // Documented simplification: always 'replace', not distinguishing an
    // upsert-triggered insert (see docs/db-plan.md's change-streams entry).
    this._emitChange({
      operationType: 'replace',
      documentKey: { _id: doc._id },
      fullDocument: returnNew ? doc : { ...replacement, _id: doc._id }
    });
    return doc;
  }

  /**
   * Apply update operators (see c/db_update.h for the exact rules;
   * $currentDate is resolved here into $set before crossing the WASM
   * bridge) to the first document matching `filter`. `update`'s top level
   * must be entirely $-prefixed operators; for a full replacement document
   * use replaceOne instead.
   */
  async updateOne(filter, update, { upsert = false } = {}) {
    if (update === null || typeof update !== 'object' || Array.isArray(update)) {
      throw new Error('updateOne requires an update document object');
    }
    update = resolveCurrentDate(update);
    const watching = this._watchers.size > 0;
    const preId = watching ? await this._resolveDocumentKeyForWatch(filter) : null;
    const { rc, upsertedId } = this._marshalTriple(filter, update, (M, fp, fn, up, un, dp, op) =>
      M._dcw_update_one(this._collCtx, fp, fn, up, un, dp, upsert ? 1 : 0, op));
    if (rc < 0) throw codeError(rc, 'updateOne');

    if (rc === 0) return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedId: null };
    if (rc === 2) {
      if (watching) {
        this._emitChange({ operationType: 'insert', documentKey: { _id: upsertedId }, fullDocument: await this.findOne({ _id: upsertedId }) });
      }
      return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedId };
    }
    if (watching && preId) {
      this._emitChange({ operationType: 'update', documentKey: { _id: preId }, fullDocument: await this.findOne({ _id: preId }) });
    }
    return { acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedId: null };
  }

  /**
   * Atomically find the first document matching `filter` and apply
   * `update` to it, returning the pre-image (`returnDocument: 'before'`,
   * the default) or the post-image (`'after'`) — or null, following
   * findOneAndReplace's exact convention for "nothing to return".
   */
  async findOneAndUpdate(filter, update, { upsert = false, returnDocument = 'before' } = {}) {
    if (update === null || typeof update !== 'object' || Array.isArray(update)) {
      throw new Error('findOneAndUpdate requires an update document object');
    }
    update = resolveCurrentDate(update);
    const returnNew = returnDocument === 'after';
    const { rc } = this._marshalTriple(filter, update, (M, fp, fn, up, un, dp) =>
      M._dcw_find_one_and_update(this._outCtx, this._collCtx, fp, fn, up, un, dp, upsert ? 1 : 0, returnNew ? 1 : 0));
    if (rc < 0) throw codeError(rc, 'findOneAndUpdate');
    if (!rc) return null;
    const doc = this._readOut(requireModule());
    if (this._watchers.size > 0) {
      // Documented simplification: always 'update', not distinguishing an
      // upsert-triggered insert (see docs/db-plan.md's change-streams entry).
      const fullDocument = returnNew ? doc : await this.findOne({ _id: doc._id });
      this._emitChange({ operationType: 'update', documentKey: { _id: doc._id }, fullDocument });
    }
    return doc;
  }

  /**
   * Like updateOne, but applies to every matching document. Does not
   * detect no-op updates (e.g. $set to a field's current value already
   * matching) — modifiedCount always mirrors matchedCount.
   */
  async updateMany(filter, update, { upsert = false } = {}) {
    if (update === null || typeof update !== 'object' || Array.isArray(update)) {
      throw new Error('updateMany requires an update document object');
    }
    update = resolveCurrentDate(update);
    const watching = this._watchers.size > 0;
    // With watchers, C hands back every post-image alongside the counts
    // (dc_update_many's `images`). It already holds each updated document,
    // so collecting them costs nothing -- where this side previously ran
    // one find() for the ids and then one findOne() per matched document,
    // which its own comment called "O(matched) extra round trips".
    const { rc } = this._marshalTriple(filter, update, (M, fp, fn, up, un, dp) =>
      M._dcw_update_many(this._outCtx, this._collCtx, fp, fn, up, un, dp,
                         upsert ? 1 : 0, watching ? 1 : 0));
    if (rc !== 0) throw codeError(rc, 'updateMany');

    const result = this._readOut(requireModule());
    if (watching) {
      const images = result.documents || [];
      if (result.upserted) {
        this._emitChange({
          operationType: 'insert',
          documentKey: { _id: result.upsertedId },
          fullDocument: images[0] ?? null
        });
      } else {
        for (const fullDocument of images) {
          this._emitChange({
            operationType: 'update',
            documentKey: { _id: fullDocument._id },
            fullDocument
          });
        }
      }
    }
    return {
      acknowledged: true,
      matchedCount: result.matchedCount,
      modifiedCount: result.matchedCount,
      // C's own answer (dcw_update_many's `upsertedId`), not the id this
      // side generated: a filter that pinned an _id gets that one.
      upsertedId: result.upserted ? result.upsertedId : null
    };
  }

  async countDocuments(filter = {}) {
    const M = requireModule();
    const fbytes = encode(filter);
    const n = withBytes(M, fbytes, (p, len) => M._dcw_count(this._collCtx, p, len));
    if (n < 0) throw codeError(n, 'countDocuments');
    return n;
  }

  /** Real MongoDB's estimatedDocumentCount() is a metadata-based estimate
   * vs. countDocuments()'s exact scan; here {} is already an O(1)
   * bpt_size lookup on both, so this is a plain alias. */
  async estimatedDocumentCount() {
    return this.countDocuments({});
  }

  /**
   * Replicated-log integration (dc_applied_index in engine/include/db.h):
   * the last log index applied to this collection -- read from the
   * primary tree, which every document mutation commits. 0 = the
   * collection is not log-driven. The apply loop resumes replay from
   * appliedIndex() + 1 after a crash; the cross-file commit journal
   * guarantees that resume point is exact (see the db.h contract).
   */
  async appliedIndex() {
    return requireModule()._dcw_applied_index(this._collCtx);
  }

  /**
   * Stage a log entry's index onto the primary tree and every attached
   * index structure, so each file's next commit persists it atomically
   * with the entry's own mutation. Call once per log entry, immediately
   * before applying its mutation. Never decreases; refused while the
   * collection has a snapshot open (both BJ_ERR_STATE, checked against
   * every structure before anything is staged).
   */
  async setAppliedIndex(index) {
    const rc = requireModule()._dcw_set_applied_index(this._collCtx, index);
    if (rc !== 0) throw codeError(rc, 'setAppliedIndex');
  }

  /** Unique values of `field` (dot-separated path) across every document
   * matching `filter`. */
  async distinct(field, filter = {}) {
    const M = requireModule();
    const f = allocStr(M, field);
    const fbytes = encode(filter);
    const fp = fbytes.length ? M._malloc(fbytes.length) : 0;
    if (fbytes.length) M.HEAPU8.set(fbytes, fp);
    let rc;
    try {
      rc = M._dcw_distinct(this._outCtx, this._collCtx, f.ptr, f.len, fp, fbytes.length);
    } finally {
      f.free();
      if (fp) M._free(fp);
    }
    if (rc !== 0) throw codeError(rc, 'distinct');
    return this._readOut(M) ?? [];
  }

  /**
   * Mixed-operation bulk write: each element of `operations` is exactly one
   * of {insertOne, updateOne, updateMany, replaceOne, deleteOne,
   * deleteMany}, shaped like the real driver's bulkWrite(). Pure JS
   * orchestration over the already-atomic Collection methods above — no
   * new C logic needed, since each sub-operation is already a complete,
   * atomic unit on its own (same reasoning as updateMany's per-document
   * journal commits). `ordered` (default true) stops at the first failing
   * operation; `false` attempts every operation and throws an aggregate
   * error afterward if any failed.
   */
  async bulkWrite(operations, { ordered = true } = {}) {
    if (!Array.isArray(operations)) {
      throw new Error('bulkWrite requires a non-empty array of operations');
    }
    return runBulkWrite(this, operations, ordered);
  }

  /**
   * Delete every document past its TTL cutoff, for every index created
   * with `expireAfterSeconds` (createIndex). There is no OPFS-level cron:
   * the host is responsible for calling this periodically (e.g.
   * setInterval, or only from whichever tab currently holds coordinator
   * leadership — src/db-coordinator.js) rather than this repo starting a
   * background timer on its own. Returns the total number of documents
   * removed across all TTL indexes.
   */
  async pruneExpired() {
    let deletedCount = 0;
    for (const filter of ttlFilters(this._indexes, Date.now())) {
      const { deletedCount: n } = await this.deleteMany(filter);
      deletedCount += n;
    }
    return deletedCount;
  }

  /** Total bytes across this collection's open backing files (primary
   * tree + every index file + journal) -- what compact() reclaims from,
   * and what Db.compact()'s growth heuristic measures. */
  _storageBytes() {
    let n = this._tree.syncAccessHandle.getSize();
    for (const ix of this._indexes.values()) {
      if (ix.kind === 'equality') n += ix.tree.syncAccessHandle.getSize();
      else if (ix.kind === 'text') { for (const role of Object.keys(ix.trees)) n += ix.trees[role].syncAccessHandle.getSize(); }
      else n += ix.rt.syncAccessHandle.getSize();
    }
    if (this._journal) n += this._journal.getSize();
    return n;
  }

  /**
   * Reclaim the space append-only history costs: stream every live entry
   * of this collection's whole file set (primary tree + every index) into
   * fresh, minimal, fully-packed files, atomically swap them in, and
   * delete the old ones. See docs/compaction.md for the design; in short:
   *
   *   - The swap is one catalog commit flipping this collection's entry to
   *     generation-prefixed new file names plus a fresh, empty cross-file
   *     journal of its own -- a crash on either side of that commit leaves
   *     a complete, consistent generation live, and Db.open() sweeps
   *     whichever side lost.
   *   - The whole set moves together because the journal's recorded file
   *     lengths are only meaningful for the exact files it was written
   *     against (docs/textindex-atomicity.md).
   *   - Concurrency: throws if a find() cursor is open (its WASM scan is
   *     positioned inside the old files -- db.h's documented hazard);
   *     any operation arriving mid-compact queues behind it and runs
   *     against the new generation (see _compacting's doc in the
   *     constructor), so other callers see a brief wait, not an error.
   *   - History is destroyed: snapshots/boundaries of the old files become
   *     invalid, matching bpt_compact's own contract.
   *
   * Returns { generation, bytesBefore, bytesAfter, bytesFreed }.
   */
  async compact() {
    // Wait until BOTH hold in one synchronous region: no other compact in
    // flight (back-to-back compacts serialize) and no counted operation
    // in flight (see _inFlight's doc in the constructor). Each is
    // re-checked after every wait -- resuming from either await, another
    // compact may have taken the gate or another operation may have
    // entered the still-open gate, in any order.
    for (;;) {
      if (this._compacting) { await this._compacting; continue; }
      if (this._inFlight > 0) { await new Promise((resolve) => this._drainWaiters.push(resolve)); continue; }
      break;
    }
    if (this._openCursors.size > 0) {
      throw new Error(`compact: collection "${this.name}" has open find() cursors -- close or exhaust them first`);
    }
    let settleCompacting;
    this._compacting = new Promise((resolve) => { settleCompacting = resolve; });
    const M = requireModule();
    const created = [];  // new-generation files, deleted on pre-flip failure
    const declared = []; // { fd, handle } pre-opened for C below, released by release()
    let flipped = false;

    // Give the handles back. The host opened them, so the host closes
    // them -- bns_close is deliberately a no-op on the handle
    // (bjns_bridge.c), the same ownership rule hostio.c has always had,
    // so dropping the scope table alone would leak every one of them.
    // That matters immediately: the adopt step re-opens these same files
    // BY NAME, and a browser refuses a second sync access handle while
    // the first is live.
    //
    // Idempotent, because the pre-flip error path calls it too: a file
    // still holding an open handle can't be deleted either. Every handle
    // is closed even if one throws, and the first failure is reported
    // once the rest are shut.
    const release = async () => {
      delete M.bjnsScopes?.[this._nsScope];
      let failure = null;
      while (declared.length) {
        const { fd, handle } = declared.pop();
        unregisterHandle(M, fd);
        try { handle.flush(); await handle.close(); }
        catch (err) { failure ||= err; }
      }
      if (failure) throw failure;
    };

    try {
      const entry = this._catalog.search(this.name);
      // C names the entire new generation before any of it exists: the
      // generation number, every new file, the catalog entry to flip to,
      // and the old files to drop afterwards. This side then creates
      // exactly those files and deletes exactly those -- with one catalog
      // write in between.
      //
      // The new entry is COPIED from the old one with names replaced,
      // rather than rebuilt, so index options and any future field
      // survive a compaction untouched. The JS this replaced spread the
      // old entry and patched names into the copy, at the one moment
      // where getting the schema wrong strands a whole generation.
      const cplan = catalogCall((M, ctx) => {
        const ee = encode(entry);
        const ep = M._malloc(ee.length || 1);
        const n = allocStr(M, this.name);
        try {
          if (ee.length) M.HEAPU8.set(ee, ep);
          return M._catw_compact_plan(ctx, ep, ee.length, n.ptr, n.len);
        } finally { n.free(); M._free(ep); }
      });
      const generation = cplan.gen;
      const bytesBefore = this._storageBytes();

      const oldFiles = cplan.oldFiles;
      const newEntry = cplan.newEntry;

      // ---- Pre-open every file the plan named, and register each under
      // its name in this Db's namespace scope. The browser's bj_ns
      // adapter resolves names from that table rather than opening
      // (bjns_bridge.c): OPFS opens are async and bj_ns.open must be
      // synchronous, so the awaits happen here, once, before C runs.
      const TEXT_ROLES = ['index', 'docTerms', 'docLengths'];
      const scope = this._nsScope;
      const table = (M.bjnsScopes ||= {})[scope] = {};
      const sources = [];   // live structures, in the plan's build order
      const kinds = [];     // 0 = bpt, 1 = rtree

      const declare = async (fileName) => {
        const handle = await this._provider.openFile(fileName, { create: true });
        created.push(fileName);
        const fd = registerHandle(M, handle);
        table[fileName] = fd;
        declared.push({ fd, handle }); // release() closes it; see above
        return handle;
      };

      await declare(newEntry.file);
      sources.push(this._tree.ctx); kinds.push(0);
      for (const def of cplan.build) {
        const ix = this._indexes.get(def.name);
        for (let j = 0; j < def.files.length; j++) {
          await declare(def.files[j]);
          if (def.kind === 1) { sources.push(ix.trees[TEXT_ROLES[j]].ctx); kinds.push(0); }
          else if (def.kind === 2) { sources.push(ix.rt.ctx); kinds.push(1); }
          else { sources.push(ix.tree.ctx); kinds.push(0); }
        }
      }
      await declare(newEntry.journal);

      // ---- Build and flip: ONE synchronous call. Between the last byte
      // of the new generation and the catalog write that adopts it,
      // nothing may see the collection half-migrated -- and a synchronous
      // WASM call cannot be interleaved at all, where the await-spanning
      // version relied on the gate holding across each one.
      //
      // This does not make the gate unnecessary: the pre-open above and
      // the adopt below still await, so concurrent operations must still
      // be kept out across them.
      const srcPtr = M._malloc(Math.max(1, sources.length * 4));
      const kindPtr = M._malloc(Math.max(1, kinds.length * 4));
      let bytesBuilt = 0;
      try {
        for (let i = 0; i < sources.length; i++) {
          writeU32(M, srcPtr + i * 4, sources[i]);
          writeU32(M, kindPtr + i * 4, kinds[i]);
        }
        const planEnc = encode(cplan);
        const pp = M._malloc(planEnc.length || 1);
        const cn = allocStr(M, this.name);
        try {
          if (planEnc.length) M.HEAPU8.set(planEnc, pp);
          const rc = M._catw_compact_execute(
            scope, this._catalog.ctx, cn.ptr, cn.len, pp, planEnc.length,
            srcPtr, kindPtr, sources.length
          );
          if (rc < 0) throw codeError(rc, 'compact');
          bytesBuilt = rc;
        } finally { cn.free(); M._free(pp); }
      } finally {
        M._free(kindPtr); M._free(srcPtr);
        // After catw_compact_execute returns -- C wrote the new
        // generation through these handles -- and before _closeHandles()
        // and the re-open below.
        await release();
      }
      newEntry.compactedBytes = bytesBuilt;
      // The flip already happened, inside C, and was made durable there
      // before these old files can be deleted.
      flipped = true;

      // ---- Adopt: reopen everything from the flipped entry. Watchers
      // survive (_closeHandles leaves them alone); _open() re-attaches
      // every index and recovers the fresh journal.
      await this._closeHandles();
      this._tree = new BPlusTree(await this._provider.openFile(newEntry.file, { create: false }), this._order);
      await this._open();

      // ---- Cleanup: drop the old generation. Best-effort -- anything
      // left behind (e.g. a crash right here) is unreferenced by the
      // catalog and swept at the next Db.open().
      for (const f of oldFiles) {
        try { await this._provider.deleteFile(f); } catch { /* swept later */ }
      }

      const bytesAfter = this._storageBytes();
      return { generation, bytesBefore, bytesAfter, bytesFreed: Math.max(0, bytesBefore - bytesAfter) };
    } catch (err) {
      // Pre-flip failure: the old generation is still live -- close
      // whatever is still open (a declare that threw part-way never
      // reached the release above, and a file with a live handle can't be
      // deleted), then drop the half-built files. Post-flip failure
      // (adopt threw): the new generation is authoritative -- keep it and
      // surface the error; reopening the Db recovers.
      if (!flipped) {
        // Best-effort, like the deletes it precedes: this path is already
        // reporting `err`, and anything left behind is unreferenced by
        // the catalog and swept at the next Db.open().
        try { await release(); } catch { /* swept later */ }
        for (const f of created) {
          try { await this._provider.deleteFile(f); } catch { /* swept later */ }
        }
      }
      throw err;
    } finally {
      this._compacting = null; // cleared before settling so resumed waiters see the gate open
      settleCompacting();
    }
  }
}

// Compaction critical section, applied in one place rather than pasted
// into every method (see _compacting/_inFlight's doc in the constructor):
// wait out an in-flight compact(), then run counted so a compact that
// starts meanwhile waits for the drain before touching files. The gate
// check, the increment, and the original method's synchronous body all
// run in one synchronous region (async functions run synchronously until
// their first await, and `orig` is invoked directly from this wrapper's
// own frame), so a compact can never take the gate between the check and
// the body. Not wrapped: compact() (it owns the gate), find() (returns
// its cursor synchronously; the cursor's eagerToArray/fetchBatch carry
// the same gate + count inline), and watch() (touches no files).
for (const name of [
  '_close', 'createIndex', 'dropIndex', 'listIndexes', 'findByIndex',
  'insertOne', 'insertMany', 'findOne', 'explain',
  'deleteOne', 'deleteMany', 'findOneAndDelete',
  'replaceOne', 'findOneAndReplace',
  'updateOne', 'findOneAndUpdate', 'updateMany',
  'countDocuments', 'distinct', 'bulkWrite', 'pruneExpired',
  'appliedIndex', 'setAppliedIndex', 'applyCommand'
]) {
  const orig = Collection.prototype[name];
  Collection.prototype[name] = async function (...args) {
    while (this._compacting) await this._compacting;
    this._inFlight++;
    try {
      return await orig.apply(this, args);
    } finally {
      this._opDone();
    }
  };
}

class Db {
  constructor(provider, { order = DB_DEFAULT_ORDER, autoCompact = null } = {}) {
    this._nsScope = nextNsScope++;
    this._provider = provider;
    this._order = order;
    this._catalog = null;
    this._collections = new Map();
    this._opening = new Map(); // name -> in-flight collection() promise (see collection())
    this.isOpen = false;
    // { minBytes?, factor? } | null -- when set, open() schedules one
    // growth-heuristic compaction sweep (docs/compaction.md, "When to
    // compact") instead of leaving the timing entirely to the host. Under
    // connectShared the options ride along to whichever context wins
    // leadership (Coordinator._becomeLeader calls connect()), so a newly
    // elected leader re-runs the sweep -- the "compact on leadership
    // acquisition" convention, built in.
    this._autoCompact = autoCompact;
    // Promise of the deferred sweep's Db.compact() results (null once it
    // has settled after a failure); stays null when autoCompact is off.
    // Awaitable for tests and hosts that want to observe the outcome --
    // open() itself never waits on it.
    this.autoCompacted = null;
  }

  async open() {
    if (this.isOpen) throw new Error('Db is already open');
    const handle = await this._provider.openFile(dbCatalogFile(), { create: true });
    this._catalog = new BPlusTree(handle, this._order);
    await this._catalog.open();
    // Format gate before anything else touches the files (see
    // db_names.h's DC_FORMAT_VERSION and docs/format-compatibility.md). A
    // database without a stamp predates the stamp and is by definition
    // version 1; it gets stamped now, as does a fresh one.
    const stamp = this._catalog.search(dbFormatKey());
    if (stamp && stamp.v > dbFormatVersion()) {
      await this._catalog.close();
      this._catalog = null;
      throw new Error(
        `Database format is version ${stamp.v}, but this build of nisaba only understands up to ` +
        `version ${dbFormatVersion()} -- upgrade nisaba to open it (docs/format-compatibility.md)`
      );
    }
    if (!stamp || stamp.v < dbFormatVersion()) {
      this._catalog.add(dbFormatKey(), { v: dbFormatVersion() });
      this._catalog.flush();
    }
    await this._sweepOrphans();
    this.isOpen = true;
    if (this._autoCompact) {
      const { minBytes = 0, factor = 0 } = this._autoCompact;
      // Fire-and-forget: open() resolves immediately; operations the host
      // issues meanwhile queue per-collection at worst (see _compacting's
      // doc in Collection). skipBusy skips, rather than disturbs, any
      // collection a host cursor is already reading.
      this.autoCompacted = this.compact({ minBytes, factor, skipBusy: true }).catch((err) => {
        // A db closed mid-sweep is the expected way to interrupt it (the
        // host is shutting down); anything else deserves a trace.
        if (this.isOpen) console.warn(`nisaba: autoCompact sweep failed: ${err.message}`);
        return null;
      });
    }
  }

  /**
   * Delete any database-owned file (isDbFile) the catalog doesn't
   * reference -- the leftovers of a compact() or dropCollection that
   * crashed between its atomic catalog commit and its file deletes. The
   * catalog is the sole source of truth for which generation of a
   * collection's files is live, so an unreferenced file is garbage by
   * definition. Skipped silently for storage providers without
   * listFiles(): sweeping is a space optimization, never a correctness
   * requirement.
   */
  async _sweepOrphans() {
    if (typeof this._provider.listFiles !== 'function') return;
    const M = requireModule();
    const names = await this._provider.listFiles();

    // C both decides and deletes here, through the browser's bj_ns
    // adapter (bjns_bridge.c). The adapter cannot unlink synchronously --
    // OPFS removeEntry returns a promise -- so it queues each name and
    // this side drains the queue once the synchronous call returns.
    //
    // Deferring is safe for exactly this operation: a sweep only removes
    // files the catalog already does not reference, so one left behind is
    // an orphan the next sweep collects, never a correctness problem.
    const scope = this._nsScope;
    const cat = encode(this._catalog.toArray());
    const cp = M._malloc(cat.length || 1);
    const joined = textEncoder.encode(names.length ? names.join('\0') + '\0' : '');
    const np = M._malloc(joined.length || 1);
    try {
      if (cat.length) M.HEAPU8.set(cat, cp);
      if (joined.length) M.HEAPU8.set(joined, np);
      const rc = M._catw_sweep_execute(scope, cp, cat.length, np, joined.length);
      if (rc < 0) throw codeError(rc, 'sweepOrphans');
    } finally { M._free(np); M._free(cp); }

    const pending = M.bjnsPending && M.bjnsPending[scope];
    if (pending && pending.length) {
      M.bjnsPending[scope] = [];
      for (const f of pending) await this._provider.deleteFile(f);
    }
  }



  async close() {
    if (!this.isOpen) return;
    for (const collection of this._collections.values()) await collection._close();
    this._collections.clear();
    await this._catalog.close();
    this._catalog = null;
    this.isOpen = false;
  }

  async collection(name) {
    if (!this.isOpen) throw new Error('Db is not open');
    checkCollectionName(name);
    const cached = this._collections.get(name);
    if (cached) return cached;
    // Dedupe concurrent first-opens: the cache is only set once _open()
    // resolves, so without this, two same-tick collection(name) calls
    // (e.g. the autoCompact sweep racing the host's own first call) would
    // each open the whole file set -- fatal on OPFS, whose sync access
    // handles are exclusive per file.
    const inFlight = this._opening.get(name);
    if (inFlight) return inFlight;
    const opening = (async () => {
      try {
        let entry = this._catalog.search(name);
        if (!entry) {
          // A fresh entry is just the primary file name; every later
          // field (journal, gen, compactedBytes, indexes) is added as it
          // is earned, which is why every reader treats them as optional.
          entry = catalogCall((M, ctx) => {
            const n = allocStr(M, name);
            try { return M._catw_new_entry(ctx, n.ptr, n.len); }
            finally { n.free(); }
          });
          this._catalog.add(name, entry);
        }
        const handle = await this._provider.openFile(entry.file, { create: true });
        const tree = new BPlusTree(handle, this._order);
        const collection = new Collection(name, tree, {
          catalog: this._catalog,
          provider: this._provider,
          order: this._order,
          nsScope: this._nsScope
        });
        await collection._open();
        this._collections.set(name, collection);
        return collection;
      } finally {
        this._opening.delete(name);
      }
    })();
    this._opening.set(name, opening);
    return opening;
  }

  async listCollections() {
    return this._catalog.toArray().map(({ key }) => key).filter((k) => k !== dbFormatKey());
  }

  /**
   * THE CATALOG CARRIES AN APPLIED INDEX OF ITS OWN, and a dropped
   * collection is the whole reason it has to. The C twin is
   * db_session.c's catalog_note_applied, and this is that fix on this
   * side of the bridge -- the JS hosts drive their own applies (see
   * WalDb._applyCommand) and so never reach it.
   *
   * Every other structure records the last log index applied to it in its
   * own metadata, staged before the mutation so the mutation's commit
   * persists both atomically (Collection.setAppliedIndex). That is exactly
   * right until the mutation IS the deletion of the structure holding the
   * record: appliedFloor() is a MAX over the collections that still exist,
   * so dropping the collection carrying the highest index makes the floor
   * go BACKWARDS -- by however far that collection was ahead.
   *
   * Harmless while the log still holds the entries in between, because
   * replay is idempotent under each collection's own guard. Fatal once the
   * log has been COMPACTED past them: the floor is what a restarting node
   * resumes from (RaftNode.start reads it as lastApplied), the apply pump
   * then asks the log for an entry at or below its base, and EntryLog
   * refuses that by contract -- which reaches _pumpApply and halts the
   * node. Replay being deterministic, it halts again on every later boot.
   * Measured in C before this existed: a drop and a polite restart left a
   * database that could not be opened again.
   *
   * The catalog is the one structure a drop cannot delete AND does write,
   * so the record belongs here. All three DDL ops make the catalog's
   * commit their decisive durable act -- createIndex writes it last,
   * having built and attached; dropIndex and dropCollection write it and
   * then remove files, so what a crash leaves behind is an orphan nothing
   * references and the orphan sweep collects. In every case, a catalog
   * commit means the entry was applied, which is exactly what may be
   * recorded in it.
   *
   * ONLY for those three (WalDb._applyCommand's caller list). Recording an
   * index the catalog's next commit would carry without that commit
   * containing the entry's mutation would let the floor claim an entry
   * whose effects are not durable -- a lost write, which is worse than the
   * halt this fixes. Implicit collection creation on a first insert
   * therefore does NOT note, though it writes the catalog too.
   */
  async noteApplied(index) {
    if (!this.isOpen || !index) return;
    // The setter refuses a decrease and the value is sticky. A replay that
    // re-offers an index at or below what is already recorded is the guard
    // working, not an error to fail the apply with.
    if (index <= this._catalog.appliedIndex()) return;
    this._catalog.setAppliedIndex(index);
  }

  /**
   * This database's replay floor: the highest log index it has applied.
   * Apply is strictly ordered, so the max over its structures is the
   * applied prefix (dbs_applied_floor in engine/src/db_session.c).
   *
   * THE CATALOG COUNTS TOO, and it is the only term here that a
   * dropCollection cannot take away with it -- see noteApplied for what
   * leaving it out costs.
   */
  async appliedFloor() {
    let floor = this._catalog ? this._catalog.appliedIndex() : 0;
    for (const name of await this.listCollections()) {
      floor = Math.max(floor, await (await this.collection(name)).appliedIndex());
    }
    return floor;
  }

  async dropCollection(name) {
    checkCollectionName(name); // also shields the reserved format stamp
    const entry = this._catalog.search(name);
    if (!entry) return false;
    const cached = this._collections.get(name);
    if (cached) {
      await cached._close();
      this._collections.delete(name);
    }
    // Which files an entry claims is the same question the orphan sweep
    // asks, answered by the same C (db_catalog.h). Sharing it is what
    // stops the two from disagreeing: a file kind drop misses becomes an
    // orphan on every drop, and one the sweep misses gets deleted from
    // under a live collection.
    const files = catalogCall((M, ctx) => {
      const ee = encode(entry);
      const ep = M._malloc(ee.length || 1);
      const n = allocStr(M, name);
      try {
        if (ee.length) M.HEAPU8.set(ee, ep);
        return M._catw_collection_files(ctx, ep, ee.length, n.ptr, n.len);
      } finally { n.free(); M._free(ep); }
    });
    this._catalog.delete(name);
    for (const f of files) await this._provider.deleteFile(f);
    return true;
  }

  /**
   * Compact every collection (see Collection.compact and
   * docs/compaction.md). With no options it is unconditional, mirroring
   * collection.compact(). `minBytes`/`factor` make it cheap to call
   * eagerly (on a timer, or from whichever tab holds coordinator
   * leadership -- the same host-driven convention as pruneExpired()): a
   * collection is skipped (result null) unless its file set is at least
   * `minBytes` and at least `factor` times its size right after its
   * previous compaction; a never-compacted collection only needs to clear
   * `minBytes`. `skipBusy` also skips (null) any collection with open
   * find() cursors or a compact already in flight, instead of
   * Collection.compact()'s throw -- what an unattended sweep (the
   * autoCompact option, a host timer) wants: a busy collection gets its
   * turn on the next sweep. Returns { [collectionName]: stats | null }.
   */
  /**
   * navigator.storage.estimate() where the platform provides it -- the
   * cheap early-warning knob for OPFS quota pressure: check it on a
   * timer or around large writes and warn users before mutations start
   * failing (a quota failure surfaces as a coded error whose `cause` is
   * the handle's QuotaExceededError; the write itself is rolled back --
   * see bridgeHandle). Origin-wide, not per-database. Returns
   * { usage, quota, ... } or null where unavailable (plain Node, no
   * OPFS shim).
   */
  async storageEstimate() {
    if (typeof navigator === 'undefined' || !navigator.storage || typeof navigator.storage.estimate !== 'function') return null;
    return navigator.storage.estimate();
  }

  async compact({ minBytes = 0, factor = 0, skipBusy = false } = {}) {
    const results = {};
    for (const name of await this.listCollections()) {
      const coll = await this.collection(name);
      if (skipBusy && (coll._openCursors.size > 0 || coll._compacting)) { results[name] = null; continue; }
      if (minBytes || factor) {
        const entry = this._catalog.search(name);
        const floor = Math.max(minBytes, factor * (entry.compactedBytes || 0));
        if (coll._storageBytes() < floor) { results[name] = null; continue; }
      }
      results[name] = await coll.compact();
    }
    return results;
  }
}

/**
 * Options (all optional):
 *   - order: B+ tree fan-out (DB_DEFAULT_ORDER).
 *   - autoCompact: { minBytes?, factor? } -- schedule one deferred
 *     Db.compact({ minBytes, factor, skipBusy: true }) sweep as soon as
 *     open() completes, without delaying connect() itself. The built-in
 *     version of the "compact on open / on a timer" host convention
 *     (docs/compaction.md, "When to compact"); observable via
 *     db.autoCompacted. Under connectShared the same options reach every
 *     newly elected leader's connect(), so leadership acquisition
 *     re-triggers the sweep.
 */
async function connect(provider, options) {
  const db = new Db(provider, options);
  await db.open();
  return db;
}

/**
 * `MongoClient`-shaped: one root provider, many logical databases beneath
 * it. Each `db(name)` is a real, isolated storage scope of its own --
 * `provider.subProvider(name)`, a genuine OPFS subdirectory or an
 * independent in-memory file map depending on the provider -- so two
 * different names never share a catalog or collection files, the same
 * guarantee the cloud service's per-tenant `db(name)` routing makes
 * (service/tenant-worker.js's `createProvider(tenantId, dbName)`), just
 * without the tenant axis: here the root provider you hand to
 * `connectClient` already picks the "account". Opened `Db`s are cached
 * per name, same reasoning as `Db.collection()` caching `Collection`s --
 * repeat calls with the same name return the same live instance rather
 * than reopening files.
 */
class Client {
  constructor(provider, options) {
    this._provider = provider;
    this._options = options;
    this._dbs = new Map(); // name -> Db
  }

  async db(name) {
    checkDbName(name);
    const cached = this._dbs.get(name);
    if (cached) return cached;
    const sub = await this._provider.subProvider(name);
    const db = await connect(sub, this._options);
    this._dbs.set(name, db);
    return db;
  }

  /**
   * Every named database under this root -- `Db.listCollections()` one
   * level up, and the instance half of a pair that until now existed
   * only at the database level.
   *
   * These are the storage SCOPES that exist -- what the provider's
   * directory says -- and not a set proven to hold a catalog. Proving it
   * would mean opening each one, and opening is exactly what a listing
   * must not do: under NodeFSStorageProvider every open acquires that
   * directory's exclusive lock, so listing a thousand databases would
   * take a thousand locks and fail outright on any database another
   * process legitimately holds. A directory listing is not a catalog,
   * and this reports the listing.
   */
  async listDatabases() {
    if (typeof this._provider.listSubProviders !== 'function') {
      throw new Error('this provider cannot list databases: it has no listSubProviders()');
    }
    return this._provider.listSubProviders();
  }

  /**
   * Delete a database and every file in it. `false` if there was none,
   * which is `dropCollection`'s answer to the same question and for the
   * same reason: dropping the already-gone is what a caller asked for.
   *
   * CLOSED FIRST, always. A `Db` this client has open holds engine
   * contexts and file handles pointing into storage that is about to
   * stop existing -- and under OPFS a sync access handle survives its
   * own file being unlinked, which is how you get a database that reads
   * fine and persists nothing. So the close is not tidiness, and it is
   * the client's rather than the provider's: the provider does not know
   * what a Db is.
   */
  async dropDatabase(name) {
    checkDbName(name);
    if (typeof this._provider.deleteSubProvider !== 'function') {
      throw new Error('this provider cannot drop a database: it has no deleteSubProvider()');
    }
    const open = this._dbs.get(name);
    if (open) {
      this._dbs.delete(name);
      if (open.isOpen) await open.close();
    }
    return this._provider.deleteSubProvider(name);
  }

  async close() {
    for (const db of this._dbs.values()) await db.close();
    this._dbs.clear();
  }
}

async function connectClient(provider, options) {
  return new Client(provider, options);
}


export {
  ready,
  isReady,
  TYPE,
  NisabaError,
  DuplicateKeyError,
  MissingIndexedFieldError,
  UnindexableValueError,
  ChangeStreamOverflowError,
  InvalidIdError,
  InvalidNameError,
  InvalidIndexSpecError,
  ObjectId,
  Pointer,
  encode,
  decode,
  valueSize,
  BinJsonFile,
  MemoryHandle,
  exists,
  deleteFile,
  getFileHandle,
  orderedKey,
  compositeKey,
  compositeUpperBound,
  BPlusTree,
  haversineDistance,
  RTree,
  TextLog,
  TiledTextLog,
  ENTRY_TYPE,
  EntryLog,
  ENTRYLOG_TYPE,
  SnapshotStore,
  crc32,
  snapshotCheckFiles,
  TextIndex,
  stemmer,
  createPatch,
  unifiedDiff,
  applyPatch,
  createDelta,
  applyDelta,
  MemoryStorageProvider,
  OPFSStorageProvider,
  resolveCurrentDate,
  isDeterministicError,
  runBulkWrite,
  ttlFilters,
  // The Raft rules whose violation is a consensus bug (raft_core.h),
  // and the wire grammar plus the two handlers C owns end to end
  // (raft_msg.h).
  raft,
  raftMsg,
  raftDrive,
  RaftCore,
  RAFT_ROLE,
  RN_EFFECT,
  // The WAL command grammar (db_wal.h). src/db-wal.js plans through
  // walPlan and dispatches on WAL_OP, so the opcode spellings live in C
  // and nowhere else.
  walPlan,
  walParse,
  walIsDocument,
  WAL_OP,
  WAL_REQ,
  WAL_PLAN,
  // File naming and the format stamp (db_names.h). Exported so no other
  // layer has to restate the convention -- src/db-wal.js carried its own
  // copy of the catalog name and the orphan-sweep pattern, with a comment
  // saying so.
  dbCatalogFile,
  // Exported so a host provider can compose its own platform rules on top
  // of the format-level ones rather than restating them (src/db-node.js).
  checkDbName,
  dbFormatKey,
  dbFormatVersion,
  collectionFileName,
  indexFileName,
  textIndexFileNames,
  journalFileName,
  isDbFile,
  ChangeStream,
  Collection,
  Db,
  connect,
  Client,
  connectClient
};
