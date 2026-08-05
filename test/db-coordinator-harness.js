/**
 * Worker-side test harness for test/db-coordinator.browser.test.js.
 *
 * A stand-in for "one tab": imports connectShared exactly like a real
 * consumer would from inside its own dedicated worker (db-coordinator.js
 * requires navigator.locks/BroadcastChannel, which this repo's OPFS
 * constraint already puts inside a Worker for every consumer). The test
 * drives several of these via postMessage to simulate several tabs sharing
 * one OPFS directory.
 *
 * Wire protocol: {id, cmd, argsPayload} in, {id, ok, result, error} out.
 * `argsPayload`/`result` are binjson-encoded (encode/decode) so ObjectId/
 * Date survive the trip -- raw structured-clone postMessage would silently
 * strip an ObjectId instance down to a plain object (only certain built-in
 * types, not arbitrary classes, are structured-clone-aware), matching how
 * db-coordinator.js's own RPC payloads are encoded for the same reason.
 */
import { ready, OPFSStorageProvider } from '../src/nisaba-wasm.js';
// Pure JS, not the WASM build: the message handler's decode(argsPayload)
// runs unconditionally, including on the very first message (before
// ready() is ever awaited) -- same reasoning as src/db-worker.js's own
// import of these two. Must be this same third_party/binjson copy that
// nisaba-wasm.js itself imports -- nisaba-wasm.js's internal encode() does
// `instanceof ObjectId` against its own ObjectId class; decoding an _id
// here with a *different* copy's ObjectId class (structurally identical,
// but a different class in this Worker's realm) makes that check silently
// fail and the _id gets encoded as a plain object instead of an OID, so
// findOne({_id}) never matches anything.
import { encode, decode } from '../third_party/binjson/js/binjson.js';
import { connectShared } from '../src/db-coordinator.js';

let sharedDb = null;
let coordinator = null;

async function run(cmd, args) {
  switch (cmd) {
    case 'connect': {
      await ready();
      const root = await navigator.storage.getDirectory();
      const dir = await root.getDirectoryHandle(args.dirName, { create: true });
      sharedDb = await connectShared(args.dbName, new OPFSStorageProvider(dir), {});
      coordinator = sharedDb._coord;
      return { role: coordinator.role };
    }
    case 'role':
      return { role: coordinator.role };
    case 'db':
      return sharedDb[args.method](...(args.args || []));
    case 'collection': {
      const coll = await sharedDb.collection(args.collection);
      if (args.method === 'find') {
        return coll.find(args.args[0], args.args[1]).toArray();
      }
      return coll[args.method](...(args.args || []));
    }
    // Starts a change stream and forwards every change back as an
    // unsolicited {change: <encoded>} message (no `id` -- not a response to
    // this particular request) rather than trying to return the
    // ChangeStream itself, which isn't binjson-encodable.
    case 'watch': {
      const coll = await sharedDb.collection(args.collection);
      const stream = coll.watch();
      (async () => {
        for await (const change of stream) {
          self.postMessage({ change: encode(change) });
        }
      })();
      return null;
    }
    case 'close':
      if (sharedDb) await sharedDb.close();
      return null;
    default:
      throw new Error(`db-coordinator-harness: unknown cmd ${cmd}`);
  }
}

self.addEventListener('message', async (event) => {
  const { id, cmd, argsPayload } = event.data;
  const args = decode(argsPayload);
  try {
    const result = await run(cmd, args);
    self.postMessage({ id, ok: true, result: encode(result === undefined ? null : result) });
  } catch (err) {
    self.postMessage({ id, ok: false, error: err.message });
  }
});
