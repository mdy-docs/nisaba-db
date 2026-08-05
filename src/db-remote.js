/**
 * db-remote.js — the main-thread half of a main-thread/Worker split, kept
 * as its own entry point (`nisaba/remote`) so a browser main thread can
 * import it without pulling in src/nisaba-wasm.js's whole module graph
 * (~160 KB of JS incl. the emscripten glue). This module's only import is
 * the pure-JS binjson codec; the WASM-backed Db/Collection/etc. live
 * behind the package's main entry (src/db.js) and belong in the Worker
 * (src/db-worker.js) — real browsers only allow OPFS sync access handles
 * in a Worker anyway (see src/db-worker.js's top comment).
 */

/**
 * The pure-JS (no WASM) codec, re-exported for callers building their own
 * main-thread/Worker split on top of this package -- e.g. index.html runs
 * on the main thread, which never calls ready() and shouldn't have to just
 * to marshal postMessage payloads to/from a Worker (see createRemoteBridge
 * below, and index.html's own use of it). Deliberately not the WASM-backed
 * encode/decode src/nisaba-wasm.js's Db/Collection classes use internally
 * (those require ready() first); this is the same codec src/db-worker.js
 * uses for the same reason on its side of the postMessage boundary.
 */
import { encode, decode, ObjectId, Pointer } from '../third_party/binjson/js/binjson.js';

export { encode, decode, ObjectId, Pointer };

/**
 * Wrap a Worker running this package's generic RPC handler (rpcInvoke in
 * src/db-worker.js) as local-feeling proxy objects: every property access
 * on a returned proxy does one postMessage round trip; calling it resolves
 * to either plain decoded data or another proxy (when the real call
 * returned a live Client/Db/Collection/cursor/ChangeStream -- see
 * isRemoteHandle in src/db-worker.js). That means every call is async even
 * where the real in-process API is synchronous (e.g. find(), or a cursor's
 * .sort()/.limit()) -- always `await`.
 *
 * Symbol.asyncIterator is special-cased to materialize via one toArray()
 * call rather than proxying .next() one round trip per document -- works
 * uniformly across every remote cursor shape this bridge might hand back,
 * whether or not it implements streaming .next() internally.
 *
 * Not supported through a proxy: passing callbacks (e.g. `.on('change',
 * cb)`) -- functions can't cross the postMessage boundary. Use
 * `for await (const change of await coll.watch())` instead.
 *
 * This bridge keeps its own request-id counter and message listener on
 * `worker`, entirely independent of any other protocol a caller layers
 * over the same Worker (e.g. index.html's own fixed notes-UI commands) --
 * ids are tagged with an `rpc:` prefix so they can never collide with a
 * caller's own numbering, and this bridge's listener only ever reacts to
 * messages whose id it recognizes, ignoring everything else. Multiple
 * 'message' listeners on the same Worker is standard and safe; each just
 * sees every message and claims only what it recognizes.
 *
 * @param {Worker} worker
 * @returns {{ makeProxy: (handleId: string) => any, rpcCall: (handleId: string, method: string, args: any[]) => Promise<any> }}
 */
export function createRemoteBridge(worker) {
  let nextId = 1;
  const pending = new Map();

  worker.addEventListener('message', (event) => {
    const { id, ok, error, rpcResult } = event.data;
    const p = pending.get(id);
    if (!p) return;
    pending.delete(id);
    if (!ok) { p.reject(new Error(error)); return; }
    p.resolve(rpcResult.kind === 'handle' ? makeProxy(rpcResult.handleId) : decode(rpcResult.valuePayload));
  });

  function makeProxy(handleId) {
    return new Proxy(() => {}, {
      get(_target, prop) {
        if (prop === 'then' || prop === 'catch' || prop === 'finally') return undefined;
        if (prop === Symbol.asyncIterator) {
          return () => {
            let items = null, idx = 0;
            return {
              async next() {
                if (items === null) items = await rpcCall(handleId, 'toArray', []);
                return idx < items.length ? { value: items[idx++], done: false } : { value: undefined, done: true };
              }
            };
          };
        }
        if (typeof prop === 'symbol') return undefined;
        return (...args) => rpcCall(handleId, prop, args);
      }
    });
  }

  function rpcCall(handleId, method, args) {
    const id = `rpc:${nextId++}`;
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      worker.postMessage({ id, rpc: true, handle: handleId, method, argsPayload: encode(args) });
    });
  }

  return { makeProxy, rpcCall };
}
