# Bug: compaction leaks its pre-opened OPFS handles

A work brief. Unlike its siblings this one is not a design step — it is a
diagnosed bug with a named cause, reproducible in about ten seconds. It
is written down rather than fixed immediately only because it was found
mid-way through unrelated work.

## Symptom

Three tests in `test/db.compact.browser.test.js` fail, and have been
failing since before the phase-7c work (verified by stashing that work
and re-running):

```
npx vitest run --config vitest.browser.config.js

× rewrites the live file set into a new generation, deletes the old, and preserves all data
× survives close + reopen: the reopened Db reads the new generation
× Db.open()'s sweep deletes a real orphaned file but keeps referenced and foreign ones

NoModificationAllowedError: Failed to execute 'createSyncAccessHandle' on
'FileSystemFileHandle': Access Handles cannot be created if there is
another open Access Handle or Writable stream associated with the same
file.
```

All three fail in the same call — `collection.compact()` — which the
worker harness's error relay hides. To see it yourself, temporarily
include `cmd`/`args.method` in the rejection at the bottom of
`test/db-compact-harness.js`.

## Cause

`Collection.compact()` (`wasm/nisaba-wasm.js`, the `declare` block around
line 3379) pre-opens every file the compaction plan names and registers
each in a per-Db namespace scope table, because `bj_ns.open` must be
synchronous and OPFS opens are not:

```js
const table = (M.bjnsScopes ||= {})[scope] = {};
const declare = async (fileName) => {
  const handle = await this._provider.openFile(fileName, { create: true });
  created.push(fileName);
  table[fileName] = registerHandle(M, handle);
  return handle;
};
```

C writes the new generation through those handles and flips the catalog
(`catw_compact_execute`). Then the `finally` does:

```js
delete M.bjnsScopes[scope];
```

That drops the TABLE. It does not close the handles, and it does not
`unregisterHandle` the fds either. The bridge cannot do it for us —
`bns_close` in `third_party/binjson-structures/src/bjns_bridge.c` is
deliberately a no-op on the handle, with the comment "The host opened the
handle and the host closes it", which is the same ownership rule
`hostio.c` has always had.

So every new-generation file is still held open when the very next lines
run:

```js
await this._closeHandles();                       // closes the OLD generation's
this._tree = new BPlusTree(await this._provider.openFile(newEntry.file, { create: false }), ...);
await this._open();                               // re-opens each index file by name
```

`openFile` on a file that already has a live sync access handle is
exactly what OPFS refuses.

## Why only the browser catches it

`docs/roadmap.md` P0 #4 records the reason: browser OPFS sync handles are
exclusive per file, and Node has no analog. The node-opfs shim the Node
suite runs against does not enforce exclusivity, so
`test/db.compact.test.js` — which covers the algorithm exhaustively,
including byte-level crash windows — passes over this without noticing.
The leak is real in both; only one environment reports it.

That is also the second symptom worth knowing about. The pre-flip error
path tries to delete the half-built files, and the code comment already
concedes defeat: "one whose dest handle a failed compact left open can't
be deleted yet; the sweep gets it". That is this same leak, observed and
worked around rather than fixed.

## The fix

Close what was opened, before the adopt step. Something like: keep the
handles (not just their fds) alongside the table, and in the same
`finally` that drops the scope, close each handle and
`unregisterHandle` its fd.

Two things to get right:

- **Order.** The close must happen after `catw_compact_execute` returns
  (C is writing through those handles) and before `_closeHandles()` /
  the re-open. It belongs in the existing `finally`, not later.
- **The error path too.** A pre-flip failure must close them as well, or
  the subsequent `deleteFile` of the half-built generation hits the same
  exclusivity rule. Fixing this should let that apologetic comment go
  away — check whether it can.

An alternative worth considering and probably rejecting: reuse the
already-open handles for the adopt instead of closing and re-opening.
It saves a round trip, but `_open()` re-attaches every index BY NAME, so
threading the pre-opened handles through would touch much more code for a
smaller gain. Prefer the simple fix unless measurement says otherwise.

## Verification

```
npx vitest run --config vitest.browser.config.js   # 9 tests, all must pass
npx vitest run                                     # 435, must stay green
```

The browser run needs Playwright's Chromium (`npx playwright install
chromium` if it is not there).

**Falsify it**: re-introduce the leak (drop the close) and watch those
three tests fail again. If they do not, the fix is not the fix.

Then consider whether the Node suite can be made to catch this class at
all — a provider wrapper that enforces one-open-handle-per-file, in the
spirit of `test/db.quota.test.js`'s wrapper that throws after N bytes,
would turn an entire category of browser-only bug into a Node-visible
one. That is the more valuable half of this work, and it is optional.

## Scope

Do not restructure compaction. The algorithm, the plan/execute split and
the one-synchronous-call flip are all correct and tested; this is a
missing `close()` on the host side of a seam whose ownership rule is
already written down.
