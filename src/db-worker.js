// Worker-side half of the change-streams (watch()) multi-tab demo
// (index.html). Runs the real document database + coordinator inside a
// dedicated Worker -- required by db-coordinator.js's own doc comment
// (OPFS + navigator.locks + BroadcastChannel all need to run off the main
// thread), same reasoning any other OPFS-backed demo needs to follow. This
// isn't just a "multi-tab" nicety, either: real browsers only expose
// FileSystemSyncAccessHandle.createSyncAccessHandle() inside a Worker in
// the first place (it's a blocking call, disallowed on the main thread by
// spec) -- confirmed empirically when a main-thread-only version of this
// demo threw "createSyncAccessHandle is not a function" in real Chromium.
// So the generic console RPC bridge below has to live here too, not in
// db.html directly.
//
// Every tab that opens index.html spins up its own instance of this
// worker; connectShared('watch-demo', ...) elects exactly one of them the
// leader (see db-coordinator.js), so all tabs share one underlying `db`
// no matter how many are open.
//
// Wire protocol: {id, cmd, argsPayload} in, {id, ok, result, error} out for
// the notes-UI's fixed commands; unsolicited {change: <encoded change
// event>} messages (no id) stream out once `watch` has been started; and
// {id, rpc: true, handle, method, argsPayload} in, {id, ok, rpcResult, error}
// out for the generic console bridge (see rpcInvoke below).
import { ready, OPFSStorageProvider, connectClient } from './nisaba-wasm.js';
// Pure JS, not the WASM build: the message handler's decode(argsPayload)
// can run before ready() has ever been awaited (e.g. the very first
// message this worker receives) -- same reasoning as index.html's own
// import of these two.
import { encode, decode } from '../third_party/binjson/js/binjson.js';
import { connectShared } from './db-coordinator.js';

let db = null;
let notes = null;
let watchStream = null;
let client = null;

async function getDb() {
  if (db) return db;
  await ready();
  const root = await navigator.storage.getDirectory();
  const dir = await root.getDirectoryHandle('binjson-watch-demo', { create: true });
  db = await connectShared('watch-demo', new OPFSStorageProvider(dir), {});
  return db;
}

// A separate, plain (non-multi-tab-shared) Client for window.client's
// db(name) -- deliberately not layered on connectShared/getDb above: that
// one is a single fixed-name coordinated Db backing the notes-UI demo
// specifically (tab leadership, BroadcastChannel rebroadcast), a different
// concern from "open several independent named databases," and mixing the
// two would make it unclear whether a given db(name) call is coordinated
// across tabs or not. Rooted at its own top-level OPFS directory so it
// can't collide with the notes demo's files.
async function getClient() {
  if (client) return client;
  await ready();
  const root = await navigator.storage.getDirectory();
  const dir = await root.getDirectoryHandle('binjson-console-client', { create: true });
  client = await connectClient(new OPFSStorageProvider(dir), {});
  return client;
}

async function getNotes() {
  if (notes) return notes;
  notes = await (await getDb()).collection('notes');
  return notes;
}

async function run(cmd, args) {
  const coll = await getNotes();
  switch (cmd) {
    case 'insertOne':
      return coll.insertOne(args[0]);
    case 'find':
      return coll.find({}).toArray();
    case 'deleteOne':
      return coll.deleteOne({ _id: args[0] });
    case 'watch': {
      if (watchStream) return null; // already watching (e.g. a page reload of the same worker)
      watchStream = coll.watch();
      (async () => {
        for await (const change of watchStream) {
          self.postMessage({ change: encode(change) });
        }
      })();
      return null;
    }
    default:
      throw new Error(`db-worker: unknown cmd "${cmd}"`);
  }
}

// --- Generic RPC bridge, for driving the db API from the browser console
// (see db.html's `client` global). Every Client/Db/Collection/cursor/
// ChangeStream object lives here in the worker; the main thread only ever
// holds handles + Proxies that turn property access into a postMessage
// round trip (db.html's makeProxy). `'client'` is the one reserved handle,
// lazily connected on first use rather than numbered like the rest --
// it's the plain multi-database Client (getClient) backing
// `window.client.db(name)`. The notes-UI's own shared `db` (getDb) is
// deliberately NOT reachable through this bridge -- see getClient's doc
// comment above for why the two are kept separate; the notes UI only
// ever talks to it through the fixed-command protocol below (run()).
//
// Handles are never released back (a page reload tears down the whole
// worker) -- fine for a manual testing tool, not something to reuse as-is
// for a long-lived RPC channel.
let handleSeq = 1;
const handles = new Map();

// A method call's result needs a handle (rather than being sent as plain
// data) if it's itself a live remote object -- a Client/Db/Collection/
// cursor/ChangeStream, whether the real src/nisaba-wasm.js classes
// (direct connect()/connectClient()) or db-coordinator.js's SharedDb/
// SharedCollection/cursor duck-types (connectShared(), used by the notes
// demo) -- so duck-type on the method names those shapes actually share,
// rather than depend on any of them being exported classes usable with
// instanceof.
function isRemoteHandle(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  return ['db', 'collection', 'insertOne', 'watch', 'toArray', 'next', 'dropCollection']
    .some((m) => typeof value[m] === 'function');
}

async function rpcInvoke(handleId, method, args) {
  const target = handleId === 'client' ? await getClient() : handles.get(handleId);
  if (target === undefined) throw new Error(`rpc: unknown handle ${handleId}`);
  if (typeof target[method] !== 'function') {
    // Not a method -- e.g. `.name()` on a collection proxy reading a plain
    // property. See db.html's makeProxy doc comment for why every access
    // goes through a call rather than a plain property read.
    return { kind: 'value', value: target[method] };
  }
  let result = target[method](...args);
  if (result && typeof result.then === 'function') result = await result;
  if (isRemoteHandle(result)) {
    const id = handleSeq++;
    handles.set(id, result);
    return { kind: 'handle', handleId: id };
  }
  return { kind: 'value', value: result === undefined ? null : result };
}

self.addEventListener('message', async (event) => {
  const { id, cmd, argsPayload, rpc, handle, method } = event.data;
  try {
    if (rpc) {
      const args = argsPayload ? decode(argsPayload) : [];
      const outcome = await rpcInvoke(handle, method, args);
      if (outcome.kind === 'handle') {
        self.postMessage({ id, ok: true, rpcResult: { kind: 'handle', handleId: outcome.handleId } });
      } else {
        self.postMessage({ id, ok: true, rpcResult: { kind: 'value', valuePayload: encode(outcome.value) } });
      }
      return;
    }
    const args = argsPayload ? decode(argsPayload) : [];
    const result = await run(cmd, args);
    self.postMessage({ id, ok: true, result: encode(result === undefined ? null : result) });
  } catch (err) {
    self.postMessage({ id, ok: false, error: err.message });
  }
});
