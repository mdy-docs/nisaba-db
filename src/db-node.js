/**
 * db-node.js — first-class Node.js persistence (`nisaba/node`, docs/
 * roadmap.md P0 #4): a storage provider over plain `node:fs` that speaks
 * the same sync-access-handle shape OPFSStorageProvider's handles do
 * (read/write at offsets, getSize, truncate, flush, close), so the whole
 * engine — including the crash-recovery journal — runs against real
 * files with no OPFS shim in sight. Kept out of src/db.js on purpose: a
 * browser bundle importing `nisaba` must never see `node:fs`.
 *
 * Durability: `flush()` is a real fsync. The engine's whole recovery
 * story assumes flushed bytes survive a crash (docs/textindex-atomicity
 * .md, docs/compaction.md); a provider that lied about flush would
 * quietly void it.
 *
 * Exclusivity: browser OPFS sync access handles are exclusive per file
 * at the platform level; Node has no analog, and two processes appending
 * to one database directory would corrupt it. Each provider therefore
 * takes an advisory lock on its directory at first use: a `.nisaba-lock`
 * file created with O_EXCL holding `pid` — a second open of the same
 * directory fails loudly while the first holder lives, and a lock left
 * by a dead process (checked with signal 0) is reclaimed. Advisory and
 * same-machine only, like every PID lockfile; it protects against the
 * accident, not an adversary. Each subProvider directory carries its own
 * lock, since nothing forces every opener to come through the parent.
 */
import fs from 'node:fs';
import path from 'node:path';

const LOCK_FILE = '.nisaba-lock';

/** node:fs-backed stand-in for a FileSystemSyncAccessHandle. */
class NodeFSSyncHandle {
  constructor(fd) {
    this._fd = fd;
  }

  getSize() {
    return fs.fstatSync(this._fd).size;
  }

  read(buffer, { at } = {}) {
    return fs.readSync(this._fd, buffer, 0, buffer.length, at ?? 0);
  }

  write(buffer, { at } = {}) {
    return fs.writeSync(this._fd, buffer, 0, buffer.length, at ?? 0);
  }

  truncate(len) {
    fs.ftruncateSync(this._fd, len);
  }

  flush() {
    fs.fsyncSync(this._fd); // the durability contract -- never fake this
  }

  close() {
    if (this._fd >= 0) {
      fs.closeSync(this._fd);
      this._fd = -1;
    }
  }
}

class NodeFSStorageProvider {
  /** @param {string} rootDir directory holding this database's files (created if missing) */
  constructor(rootDir) {
    if (typeof rootDir !== 'string' || rootDir.length === 0) {
      throw new Error('NodeFSStorageProvider requires a root directory path');
    }
    this._dir = path.resolve(rootDir);
    this._lockFd = null;     // held for this provider's lifetime once _init ran
    this._children = new Map(); // name -> NodeFSStorageProvider (subProvider cache, mirrors the other providers)
  }

  _init() {
    if (this._lockFd !== null) return;
    fs.mkdirSync(this._dir, { recursive: true });
    const lockPath = path.join(this._dir, LOCK_FILE);
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        this._lockFd = fs.openSync(lockPath, 'wx'); // O_EXCL: exactly one creator
        fs.writeSync(this._lockFd, String(process.pid));
        fs.fsyncSync(this._lockFd);
        return;
      } catch (err) {
        if (err.code !== 'EEXIST') throw err;
        // A lock exists: stale (holder dead) or genuinely held? Our own
        // pid counts as held too -- a second provider on this directory
        // in the same process corrupts exactly like a second process
        // would (two catalogs over one file set), so it gets the same
        // refusal, not a bypass.
        let holder = NaN;
        try { holder = parseInt(fs.readFileSync(lockPath, 'utf8'), 10); } catch { /* racing unlink -- retry */ }
        if (Number.isInteger(holder) && holder > 0) {
          let alive = holder === process.pid;
          if (!alive) {
            try { process.kill(holder, 0); alive = true; } // throws ESRCH if no such process
            catch (probe) { alive = probe.code !== 'ESRCH'; } // EPERM etc: assume alive
          }
          if (alive) {
            throw new Error(
              `NodeFSStorageProvider: "${this._dir}" is locked by pid ${holder}${holder === process.pid ? ' (this process -- another provider instance holds it)' : ''} (${lockPath}). ` +
              'One opener per database directory -- close the other one, or delete the lock file if that pid is not a nisaba process.'
            );
          }
        }
        // Stale or unreadable: reclaim and retry the exclusive create once.
        try { fs.unlinkSync(lockPath); } catch { /* someone else reclaimed first */ }
      }
    }
    throw new Error(`NodeFSStorageProvider: could not acquire ${lockPath} (raced repeatedly)`);
  }

  _path(name) {
    if (typeof name !== 'string' || name.length === 0 || name.includes('/') || name.includes('\\') || name.includes('\0') || name === '..') {
      throw new Error(`Invalid file name: ${JSON.stringify(name)}`);
    }
    return path.join(this._dir, name);
  }

  async openFile(name, { create = false } = {}) {
    this._init();
    const p = this._path(name);
    let fd;
    try {
      fd = fs.openSync(p, 'r+');
    } catch (err) {
      if (err.code !== 'ENOENT' || !create) throw err;
      fd = fs.openSync(p, 'w+');
      // POSIX durability for the CREATION itself: without fsyncing the
      // parent directory, a crash can lose the directory entry even
      // after the file's own bytes were fsynced -- which would silently
      // drop a freshly created WAL log or snapshot generation file out
      // from under the crash-safety reasoning built on them (roadmap
      // step 4's fsync audit). Deletions are deliberately not dir-synced:
      // a resurrected deleted file is a benign orphan the sweeps handle.
      this._fsyncDir();
    }
    return new NodeFSSyncHandle(fd);
  }

  _fsyncDir() {
    let dirFd = null;
    try {
      dirFd = fs.openSync(this._dir, 'r');
      fs.fsyncSync(dirFd);
    } catch { /* some platforms refuse dir fsync (e.g. Windows); best-effort */ }
    finally {
      if (dirFd !== null) fs.closeSync(dirFd);
    }
  }

  async deleteFile(name) {
    this._init();
    try {
      fs.unlinkSync(this._path(name));
    } catch (err) {
      if (err.code !== 'ENOENT') throw err; // deleting the already-gone is a no-op, matching MemoryStorageProvider
    }
  }

  /** Names of every file directly in this directory (lock file excluded)
   * -- lets Db.open() sweep compaction/drop orphans, same contract as the
   * other providers' listFiles(). */
  async listFiles() {
    this._init();
    return fs.readdirSync(this._dir, { withFileTypes: true })
      .filter((e) => e.isFile() && e.name !== LOCK_FILE)
      .map((e) => e.name);
  }

  /** A subdirectory as its own provider (Client.db(name)'s on-disk unit),
   * with its own lock -- see the exclusivity note in the module comment. */
  async subProvider(name) {
    this._init();
    const cached = this._children.get(name);
    if (cached) return cached;
    if (typeof name !== 'string' || name.length === 0 || name.includes('/') || name.includes('\\') || name.includes('\0') || name === '..') {
      throw new Error(`Invalid database name: ${JSON.stringify(name)}`);
    }
    const child = new NodeFSStorageProvider(path.join(this._dir, name));
    this._children.set(name, child);
    return child;
  }

  /** Release the directory lock (and children's). Call after Db.close()/
   * Client.close() when the process will keep running; process exit
   * releases implicitly (a dead pid's lock is reclaimed on next open). */
  async close() {
    for (const child of this._children.values()) await child.close();
    this._children.clear();
    if (this._lockFd !== null) {
      fs.closeSync(this._lockFd);
      this._lockFd = null;
      try { fs.unlinkSync(path.join(this._dir, LOCK_FILE)); } catch { /* already gone */ }
    }
  }
}

export { NodeFSStorageProvider };
// One-stop node entry: everything the main entry exports, plus the provider.
export * from './db.js';
