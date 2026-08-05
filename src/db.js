/**
 * db.js — public entry point for the document database (mirrors the
 * parent binjson project's own src/db.js, which re-exports the same set
 * from its own combined WASM module -- see src/nisaba-wasm.js's own top
 * comment for why the parent doesn't just re-export from here instead).
 *
 * This is the full-fat, in-process entry (node console, the Worker side
 * of a browser split, bin/db.js): importing it loads the whole WASM
 * module graph. A browser main thread that only marshals postMessage
 * payloads to a Worker should import `nisaba/remote` (src/db-remote.js)
 * instead -- everything below the divider is re-exported from there so
 * this entry stays a superset, but importing it from here drags in the
 * WASM glue too.
 */
export {
  Db,
  Collection,
  ChangeStream,
  MemoryStorageProvider,
  OPFSStorageProvider,
  connect,
  Client,
  connectClient,
  NisabaError,
  DuplicateKeyError,
  MissingIndexedFieldError,
  UnindexableValueError,
  ChangeStreamOverflowError,
  InvalidIdError
} from './nisaba-wasm.js';

// Main-thread-safe surface: the pure-JS (no WASM) codec and the Worker RPC
// bridge. See src/db-remote.js for the doc comments.
export { encode, decode, ObjectId, Pointer, createRemoteBridge } from './db-remote.js';
