/**
 * db-wal-instance.js — the INSTANCE layer over the WAL host: one root
 * directory, many databases, ONE log (docs/s3-backup.md, "One snapshot,
 * two hosts", step 2). This is the JavaScript twin of what the C server
 * already is (db_instance.h + server/replica.c): the root holds the
 * entry log and the snapshot store beside per-database subdirectories,
 * every logged entry carries an envelope naming its database, and a
 * generation spans every database in the root with `"db/file"` live
 * names. A root written by this host and a root written by the C server
 * are the same artifact.
 *
 * THE ENVELOPE IS C'S, adopted rather than reinvented (db_instance.h):
 *
 *     { d: "<database>", c: <the command, as db_wal.h defines it> }
 *     { d: "<database>", i: "drop" }        -- dropDatabase, replicated
 *
 * The command inside is untouched — "which database" is addressing, not
 * meaning — which is what lets WalDb and WalCollection stay exactly what
 * they are: each database here IS a WalDb (InstanceDb below), applying
 * the same commands with the same code; only the commit engine and the
 * serialize chain are the instance's, because there is one log and log
 * order must be apply order ACROSS databases, not merely within one.
 *
 * REPLICATION FOLLOWS THE INSTANCE, NOT THE DATABASE — one log, one
 * leader, one member set (db_instance.h says why, and what it costs:
 * one database's write rate is every database's). The replicated
 * instance (RaftNode over this log) is the second half of step 2 and is
 * not in this file yet; what is here is the storage shape both halves
 * share.
 *
 * Raft entry types: a log written by a cluster member carries NOOP and
 * CONFIG entries between the commands (ENTRYLOG_TYPE). They are the
 * node's, not the database's — replay applies NORMAL entries only,
 * exactly as the apply pump does (src/raft.js).
 *
 * A ROOT THAT IS ITSELF A DATABASE IS REFUSED, the same check the C
 * server makes (root_holds_a_database, server/main.c): a directory
 * holding __catalog__.bj is one database, and opening it as an instance
 * would nest a database inside itself. connectWal is that directory's
 * opener.
 */
import {
  connectClient,
  EntryLog,
  ENTRYLOG_TYPE,
  SnapshotStore,
  decode,
  encode,
  dbCatalogFile,
  isDbFile
} from './nisaba-wasm.js';
import {
  WalDb, WAL_FILE, SNAP_PREFIX, openWalStorage, providerDirectory
} from './db-wal.js';

/** One database of an instance: a WalDb whose commit engine, serialize
 * chain, and snapshots are the INSTANCE's. Handles, not resources —
 * close() closes the instance, exactly as a server client's db(name)
 * handle closes its connection. */
class InstanceDb extends WalDb {
  constructor(instance, name, inner, provider) {
    super(inner, instance._log, { provider, store: null });
    this._instance = instance;
    this.name = name;
  }

  /** One chain for the whole instance: there is one log, and its order
   * must be apply order across databases, not merely within one. */
  _serialize(fn) { return this._instance._serialize(fn); }

  /** The instance wraps each command in its envelope and appends to the
   * shared log; the apply comes back through this database's own
   * _applyCommand — the same code replay reaches through the envelope. */
  _commit(cmds, opts) { return this._instance._commitFor(this, cmds, opts); }

  /** A snapshot is the INSTANCE's — one generation for the whole root.
   * A per-database generation would be a second artifact shape beside
   * the C server's, which is the thing step 2 exists to remove. */
  snapshot() { return this._instance.snapshot(); }
  get snapshots() { return this._instance.snapshots; }

  get log() { return this._instance.log; }

  close() { return this._instance.close(); }
}

class WalInstance {
  constructor(client, log, { provider, store }) {
    this._client = client;
    this._log = log;
    this._provider = provider;
    this._store = store;
    this._dbs = new Map();   // name -> InstanceDb
    this._chain = Promise.resolve();
    this._broken = null;     // Error that poisoned the log, or null
    this.isOpen = true;
  }

  get log() { return this._log; }
  get snapshots() { return this._store; }

  /** The database, opened (and its directory created) on first use —
   * dbi_database's contract, and Client.db(name)'s. */
  async db(name) {
    let idb = this._dbs.get(name);
    if (!idb) {
      const inner = await this._client.db(name);
      const sub = await this._provider.subProvider(name);
      idb = new InstanceDb(this, name, inner, sub);
      this._dbs.set(name, idb);
    }
    return idb;
  }

  /** The storage scopes that exist — a directory listing, not a set
   * proven to hold catalogs (Client.listDatabases says why). */
  async listDatabases() { return this._client.listDatabases(); }

  /**
   * Drop a database: a LOGGED act, `{ d, i: "drop" }` — replicated and
   * replayed like any command, so a crash between the log write and the
   * directory removal re-drops on recovery (db_instance.h: the entry
   * carries no command because its act is about a database, not for
   * one). `dropped: false` when there was none; dropping the
   * already-gone is what the caller asked for.
   */
  async dropDatabase(name) {
    return this._serialize(async () => {
      const first = this._propose([encode({ d: name, i: 'drop' })]);
      try {
        return await this._applyDrop(name);
      } catch (err) {
        this._retractFrom(first);
        throw err;
      }
    });
  }

  async _applyDrop(name) {
    const idb = this._dbs.get(name);
    if (idb) {
      this._dbs.delete(name);
      idb.isOpen = false;
    }
    const dropped = await this._client.dropDatabase(name);
    return { ok: true, dropped };
  }

  _serialize(fn) {
    const run = this._chain.then(fn, fn);
    this._chain = run.then(() => {}, () => {});
    return run;
  }

  /** Same contract as WalDb._propose, over the instance's one log. */
  _propose(payloads) {
    if (this._broken) {
      throw new Error(`WAL is poisoned by an earlier sync failure: ${this._broken.message}`, { cause: this._broken });
    }
    const term = this._log.currentTerm;
    let first = 0;
    for (const payload of payloads) {
      const index = this._log.append(term, payload);
      if (!first) first = index;
    }
    try {
      this._log.sync();
    } catch (err) {
      this._broken = err;
      throw err;
    }
    return first;
  }

  _retractFrom(index) {
    try { this._log.truncateFrom(index); } catch { /* best-effort; replay re-fails identically */ }
  }

  /**
   * The instance commit engine (WalDb._commit with the envelope added):
   * wrap each command as `{ d, c }`, group-commit them to the one log,
   * then apply each in order through the proposing database's own
   * _applyCommand — the raw command bytes, because the envelope is
   * addressing and the address is already resolved here. Failed applies
   * are truncated back out, exactly as single-node WalDb does.
   */
  async _commitFor(idb, cmds, { ordered = true } = {}) {
    const wrapped = cmds.map((payload) => encode({ d: idb.name, c: decode(payload) }));
    const first = this._propose(wrapped);
    const results = new Array(cmds.length);
    let firstError = null;
    for (let i = 0; i < cmds.length; i++) {
      const index = first + i;
      try {
        results[i] = await idb._applyCommand(index, cmds[i]);
      } catch (err) {
        if (firstError === null) firstError = { index: i, error: err };
        if (ordered) {
          this._retractFrom(index);
          break;
        }
      }
    }
    return { results, firstError };
  }

  /**
   * The replay floor: the highest index this INSTANCE has applied,
   * across every database in the root — not merely the open ones
   * (dbi_applied_floor, including its cost: it opens every database to
   * ask, which is why it is a startup call and not a hot one). Apply is
   * strictly ordered across the one log, so the max is the applied
   * prefix; a deterministic failure below it already produced its
   * result and is never retried.
   */
  async _appliedFloor() {
    let floor = 0;
    for (const name of await this.listDatabases()) {
      const idb = await this.db(name);
      for (const cname of await idb.listCollections()) {
        const col = await idb._db.collection(cname);
        floor = Math.max(floor, await col.appliedIndex());
      }
    }
    return floor;
  }

  /** Recovery: replay every NORMAL entry above the instance floor,
   * routed by envelope. NOOP/CONFIG entries are the Raft node's and are
   * skipped by TYPE, never by failing to parse. An entry with no
   * envelope is refused loudly — this log belongs to a single-database
   * host, and quietly misapplying it is the divergence nothing would
   * catch. */
  async _recover() {
    const log = this._log;
    const floor = await this._appliedFloor();
    let from = Math.max(log.baseIndex, floor) + 1;
    while (from <= log.lastIndex) {
      const batch = log.getBatch(from, 1 << 20);
      if (batch.length === 0) break;
      for (const entry of batch) {
        if (entry.type !== ENTRYLOG_TYPE.NORMAL) continue;
        const env = decode(entry.payload);
        if (typeof env.d !== 'string') {
          throw new Error(
            `log entry ${entry.index} carries no instance envelope -- ` +
            'this log was written by a single-database host; open it with connectWal'
          );
        }
        try {
          await this._applyEnvelope(entry.index, env);
        } catch { /* deterministic re-failure; see WalDb._recover */ }
      }
      from = batch[batch.length - 1].index + 1;
    }
  }

  /** Perform one committed entry: unwrap, resolve the database —
   * creating it if a first insert is making it — and apply
   * (dbi_apply's contract). Refuses a payload with no envelope: on the
   * replicated path this error is NOT deterministic-classified, so it
   * stops the node rather than letting a replica quietly diverge. */
  async _applyEnvelope(index, env) {
    if (typeof env.d !== 'string') {
      throw new Error(`log entry ${index} carries no instance envelope`);
    }
    if (env.i === 'drop') return this._applyDrop(env.d);
    const idb = await this.db(env.d);
    return idb._applyCommand(index, encode(env.c));
  }

  _requireStore(what) {
    if (!this._store) {
      throw new Error(`${what} requires a storage provider with listFiles()`);
    }
  }

  /**
   * One generation for the whole root: every database's catalog and
   * collection structures, live names `"db/file"` — the C server's
   * scope (server/replica.c's snapshot_take), so either host can adopt
   * it. Then the log is compacted through the boundary into the store's
   * paired file and adopted, exactly as WalDb.snapshot() does.
   */
  async snapshot() {
    this._requireStore('snapshot');
    return this._serialize(() => this._snapshotLocked());
  }

  async _snapshotLocked(boundaryIndex = null) {
    const log = this._log;
    const lastIncludedIndex = boundaryIndex ?? log.lastIndex;
    const lastIncludedTerm = log.termAt(lastIncludedIndex);

    const tx = await this._store.begin();
    const live = [];
    let n = 0;
    const add = async (liveName, structure) => {
      const role = `f${n++}`;
      await structure.compact(await tx.createFile(role));
      live.push({ role, name: liveName });
    };
    try {
      // Sorted for a stable role order; the C server takes listing
      // order. Neither matters to a reader -- roles are positional and
      // config.live is the meaning (snapstore.h) -- but stable is
      // easier to test.
      for (const dbName of (await this.listDatabases()).sort()) {
        const idb = await this.db(dbName);
        const inner = idb._db;
        await add(`${dbName}/${dbCatalogFile()}`, inner._catalog);
        for (const cname of await inner.listCollections()) {
          const col = await inner.collection(cname);
          await add(`${dbName}/${inner._catalog.search(cname).file}`, col._tree);
          for (const ix of col._indexes.values()) {
            if (ix.kind === 'equality') await add(`${dbName}/${ix.file}`, ix.tree);
            else if (ix.kind === 'text') {
              await add(`${dbName}/${ix.files.index}`, ix.trees.index);
              await add(`${dbName}/${ix.files.docTerms}`, ix.trees.docTerms);
              await add(`${dbName}/${ix.files.docLengths}`, ix.trees.docLengths);
            } else await add(`${dbName}/${ix.file}`, ix.rt);
          }
        }
      }
      await tx.commit({ lastIncludedIndex, lastIncludedTerm, config: { live } });
    } catch (err) {
      await tx.abort();
      throw err;
    }

    // Pair the compacted log with the generation and adopt it — the
    // same window WalDb._snapshotLocked runs, with the same crash
    // story: the old log is only pruned once its successor is durable.
    const { name: logName, handle } = await this._store.createLogFile();
    await log.compact(handle, lastIncludedIndex, lastIncludedTerm);
    await log.close();
    const fresh = new EntryLog(await this._provider.openFile(logName, { create: false }));
    await fresh.open();
    this._log = fresh;
    for (const idb of this._dbs.values()) idb._log = fresh;
    await this._store.pruneLogs(logName);
    if (logName !== WAL_FILE) {
      try { await this._provider.deleteFile(WAL_FILE); } catch { /* best-effort */ }
    }
    return this._store.latest;
  }

  async close() {
    if (!this.isOpen) return;
    this.isOpen = false;
    await this._chain.catch(() => {});
    await this._client.close();
    await this._log.close();
    this._store?.close();
  }
}

/**
 * Open an instance root: many databases under one directory, one log,
 * one snapshot store — the layout the C server owns when it runs
 * (docs/db-server.md). Requires a provider with listFiles(),
 * subProvider() and listSubProviders(); refuses a directory that is
 * itself a database.
 */
export async function connectWalInstance(provider, options = {}) {
  const { snapshotPrefix = SNAP_PREFIX, ...dbOptions } = options;
  if (typeof provider.listFiles !== 'function' ||
      typeof provider.subProvider !== 'function' ||
      typeof provider.listSubProviders !== 'function') {
    throw new Error('connectWalInstance requires a storage provider with listFiles(), subProvider() and listSubProviders()');
  }
  if ((await provider.listFiles()).includes(dbCatalogFile())) {
    throw new Error(
      'this directory is a database, not an instance root (it holds ' +
      `${dbCatalogFile()}); open it with connectWal, or point at the directory above`
    );
  }
  const client = await connectClient(provider, dbOptions);
  try {
    const { store, log, logName } = await openWalStorage(provider, { snapshotPrefix });
    const inst = new WalInstance(client, log, { provider, store });
    // The instance twin of reconcileLogWithAppliedFloor (db-wal.js): a
    // log behind the replay floor is the restored-applied-state shape,
    // and left alone it poisons every write. The applied state IS a
    // snapshot through the floor; re-base the log there.
    const floor = await inst._appliedFloor();
    if (floor > inst._log.lastIndex) {
      const { currentTerm, votedFor, lastTerm } = inst._log;
      await inst._log.close();
      const handle = await provider.openFile(logName, { create: true });
      handle.truncate(0);
      const fresh = new EntryLog(handle, { baseIndex: floor, baseTerm: lastTerm });
      await fresh.open();
      if (currentTerm > 0) fresh.setHardState(currentTerm, votedFor);
      inst._log = fresh;
    }
    if (inst._log.currentTerm === 0) inst._log.setHardState(1);
    await inst._recover();
    return inst;
  } catch (err) {
    await client.close();
    throw err;
  }
}

/**
 * The disaster-recovery path for an instance root: put every database's
 * live files exactly at the store's adopted generation. Live names are
 * `"db/file"`; each database's current catalog/collection/journal files
 * are deleted first (a stale journal against restored files could
 * rewind them — WalDb's restoreLatestSnapshot says why), then every
 * generation file is stream-copied to its live name, CRC-verified
 * against the manifest. Entry-log files are untouched: a following
 * connectWalInstance() replays the suffix beyond the boundary; delete
 * the log files first for a boundary-exact restore.
 */
export async function restoreLatestInstanceSnapshot(provider, { snapshotPrefix = SNAP_PREFIX } = {}) {
  if (typeof provider.listFiles !== 'function' || typeof provider.subProvider !== 'function') {
    throw new Error('restoreLatestInstanceSnapshot requires a storage provider with listFiles() and subProvider()');
  }
  const store = new SnapshotStore(providerDirectory(provider), { prefix: snapshotPrefix });
  await store.open();
  try {
    if (!store.latest) throw new Error('No snapshot to restore');
    for (const dbName of await provider.listSubProviders()) {
      const sub = await provider.subProvider(dbName);
      for (const f of await sub.listFiles()) {
        if (f === dbCatalogFile() || isDbFile(f)) await sub.deleteFile(f);
      }
    }
    for (const { role, name } of store.latest.config.live) {
      const slash = name.indexOf('/');
      if (slash < 0) {
        throw new Error(
          `not an instance generation: live name '${name}' names no database -- ` +
          'restore it with restoreLatestSnapshot into a single-database directory'
        );
      }
      const sub = await provider.subProvider(name.slice(0, slash));
      await store.copyFile(role, await sub.openFile(name.slice(slash + 1), { create: true }));
    }
    return store.latest;
  } finally {
    store.close();
  }
}

export { WalInstance, InstanceDb };
