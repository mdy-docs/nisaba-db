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
 * lock, since nothing forces every opener to come through the parent —
 * which is also why deleteSubProvider() refuses a directory somebody
 * else holds, rather than only guarding the way in.
 */
import fs from 'node:fs';
import path from 'node:path';
import { checkDbName } from '../wasm/nisaba-wasm.js';

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

  /* Who holds `dir`'s lock, or null if nobody does -- no lock file, an
   * unreadable one, or one left by a process that has died. Our own pid
   * counts as a holder, because a second provider on one directory in
   * this process corrupts exactly like a second process would.
   *
   * Shared by the two places that have to know: acquiring a lock, and
   * DELETING a directory out from under one. */
  static _lockHolder(dir) {
    let holder = NaN;
    try { holder = parseInt(fs.readFileSync(path.join(dir, LOCK_FILE), 'utf8'), 10); }
    catch { return null; }   /* no lock, or it went while we read it */
    if (!Number.isInteger(holder) || holder <= 0) return null;
    if (holder === process.pid) return holder;
    try { process.kill(holder, 0); return holder; }  // throws ESRCH if no such process
    catch (probe) { return probe.code === 'ESRCH' ? null : holder; }  // EPERM etc: assume alive
  }

  static _lockedError(dir, holder) {
    return new Error(
      `NodeFSStorageProvider: "${dir}" is locked by pid ${holder}${holder === process.pid ? ' (this process -- another provider instance holds it)' : ''} (${path.join(dir, LOCK_FILE)}). ` +
      'One opener per database directory -- close the other one, or delete the lock file if that pid is not a nisaba process.'
    );
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
        // A lock exists: stale (holder dead) or genuinely held?
        const holder = NodeFSStorageProvider._lockHolder(this._dir);
        if (holder !== null) throw NodeFSStorageProvider._lockedError(this._dir, holder);
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

  /* The format-level rules (non-empty, no '/', no NUL) are C's --
   * db_validate.h -- and shared with every other provider. The two extra
   * rules here are this platform's: a name becomes a real filesystem path
   * segment, where '\\' is also a separator and '..' escapes the
   * directory. Checked in one place because subProvider() and
   * deleteSubProvider() must agree about what a name is -- a name one
   * accepts and the other rejects is a database that can be made and not
   * unmade. */
  _checkSubName(name) {
    checkDbName(name);
    if (name.includes('\\') || name === '..') {
      throw new Error(`Invalid database name: ${JSON.stringify(name)} -- not usable as a filesystem path segment`);
    }
  }

  /** A subdirectory as its own provider (Client.db(name)'s on-disk unit),
   * with its own lock -- see the exclusivity note in the module comment. */
  async subProvider(name) {
    this._init();
    const cached = this._children.get(name);
    if (cached) return cached;
    this._checkSubName(name);
    const child = new NodeFSStorageProvider(path.join(this._dir, name));
    this._children.set(name, child);
    return child;
  }

  /** Names of the subdirectories under this one -- the other half of
   * subProvider(), and what Client.listDatabases() reports. Nothing is
   * opened and nothing is locked: this reads the directory, which is
   * what makes listing a thousand databases cost one readdir rather than
   * a thousand lock acquisitions. */
  async listSubProviders() {
    this._init();
    return fs.readdirSync(this._dir, { withFileTypes: true })
      .filter((e) => e.isDirectory())
      .map((e) => e.name);
  }

  /**
   * Remove a subdirectory and everything in it; false if there was none.
   *
   * The child's LOCK goes first, and it has to: this provider holds one
   * fd per opened directory, and unlinking the lock file underneath a
   * live fd would leave the next opener free to acquire a lock on a
   * directory this one still believes it owns. Closing the child both
   * releases the fd and drops it from the cache, so a later subProvider()
   * of the same name builds a fresh one rather than handing back a
   * provider pointed at a directory that is gone.
   *
   * The caller closes whatever it had OPEN in there first -- this drops
   * the storage, not the handles into it.
   *
   * ONE OPENER PER DIRECTORY applies to removing one as much as to
   * writing to it. A database can be opened directly rather than through
   * this parent -- `nisaba-server` does exactly that, one process per
   * database directory -- so a lock held by anybody other than our own
   * child refuses the drop. Deleting it anyway would pull the files out
   * from under a live writer and leave it holding fds to a directory
   * with no name, which is the failure the lock exists to prevent.
   */
  async deleteSubProvider(name) {
    this._init();
    this._checkSubName(name);
    const dir = path.join(this._dir, name);
    const child = this._children.get(name);
    // Checked BEFORE anything is closed: a refusal has to leave the
    // caller's own database exactly as it was.
    if (!child || child._lockFd === null) {
      const holder = NodeFSStorageProvider._lockHolder(dir);
      if (holder !== null) throw NodeFSStorageProvider._lockedError(dir, holder);
    }
    if (child) {
      await child.close();
      this._children.delete(name);
    }
    if (!fs.existsSync(dir)) return false;
    fs.rmSync(dir, { recursive: true, force: true });
    // The directory ENTRY is what went away, and losing it to a crash
    // would resurrect a database that was dropped -- the same reasoning
    // openFile() fsyncs the directory on creation for, in the other
    // direction. A deleted FILE is a benign orphan the sweeps handle; a
    // deleted database is not something any sweep looks for.
    this._fsyncDir();
    return true;
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
