/**
 * Worker-side harness for test/db.compact.browser.test.js.
 *
 * Compaction is a single-tab operation on the real Db, so unlike
 * db-coordinator-harness.js this drives a plain connect() (no
 * connectShared/coordinator layer) against a real OPFS directory -- the
 * path a single-tab app and the compaction feature itself actually take.
 * OPFS sync access handles are Worker-only, so the whole Db lives here and
 * the test drives it over postMessage; the test's own main thread only
 * inspects the OPFS *directory* (enumerating names, planting an orphan
 * file), which needs no file locks.
 *
 * Wire protocol mirrors db-coordinator-harness.js: {id, cmd, argsPayload}
 * in, {id, ok, result, error} out, binjson-encoded so ObjectId/Date
 * survive the trip (raw structured-clone would strip an ObjectId to a
 * plain object -- see that harness's comment).
 */
import { ready, connect, OPFSStorageProvider } from '../src/nisaba-wasm.js';
import { encode, decode } from '../third_party/binjson/js/binjson.js';

let db = null;

async function run(cmd, args) {
  switch (cmd) {
    case 'connect': {
      await ready();
      const root = await navigator.storage.getDirectory();
      const dir = await root.getDirectoryHandle(args.dirName, { create: true });
      db = await connect(new OPFSStorageProvider(dir));
      return null;
    }
    case 'db':
      return db[args.method](...(args.args || []));
    case 'collection': {
      const coll = await db.collection(args.collection);
      if (args.method === 'find') {
        return coll.find(args.args[0], args.args[1] || {}).toArray();
      }
      return coll[args.method](...(args.args || []));
    }
    case 'close':
      if (db) await db.close();
      db = null;
      return null;
    default:
      throw new Error(`db-compact-harness: unknown cmd ${cmd}`);
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
