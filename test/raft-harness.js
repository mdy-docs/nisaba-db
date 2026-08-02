/**
 * Deterministic simulation harness for the Raft core (roadmap step 7):
 * a virtual clock with a seeded rng, an in-memory network with delays,
 * partitions, and unreachable-peer failures, a tiny KV state machine,
 * and a cluster factory over per-node in-memory DISKS. Nothing here
 * touches a real clock: nodes see time only through tick(), the network
 * delivers only when the simulation advances, and every random draw
 * comes from the seed — a failing schedule replays exactly.
 *
 * A disk rather than a log handle, because an install replaces FILES —
 * the state machine's, and the log itself. A member that survives a
 * crash-stop here survives as everything on its disk: its KV file, its
 * snapshot generations, and whichever log the latest generation is
 * paired with. That is what lets the simulator drive the install the
 * node performs itself (raft_node.h) rather than a test double of one.
 */
import {
  EntryLog, MemoryStorageProvider, SnapshotStore, encode, decode
} from '../wasm/nisaba-wasm.js';
import { RaftNode } from '../src/raft.js';

/** The KV machine's live state, and the snapshot store's prefix. */
const KV_FILE = 'kv.bj';
const WAL_FILE = 'wal.bj';
const SNAP_PREFIX = 'snap';

/** A node's disk: named files that survive its process. */
export const makeDisk = () => new MemoryStorageProvider();

/** The getFileHandle-shaped view SnapshotStore consumes. */
function directoryOf(disk) {
  return {
    async getFileHandle(name, options = {}) {
      return { createSyncAccessHandle: () => disk.openFile(name, options) };
    },
    async removeEntry(name) { return disk.deleteFile(name); },
    async *entries() { for (const name of await disk.listFiles()) yield [name]; }
  };
}

/**
 * Send `msg` to `node` the way a transport would: encoded on the way in,
 * decoded on the way out. Messages cross the node's boundary as bytes
 * now -- the transport frames, it does not interpret (raft_msg.h) -- so
 * a test driving handleMessage directly speaks the same protocol the
 * wire does rather than a privileged object-shaped one.
 */
export async function rpc(node, msg) {
  return decode(await node.handleMessage(encode(msg)));
}

/** Small, well-known seedable PRNG. */
export function mulberry32(seed) {
  let a = seed >>> 0;
  return function () {
    a |= 0; a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Let promise chains through async handlers settle between sim steps. */
async function drainMicrotasks() {
  for (let i = 0; i < 25; i++) await null;
}

export class Sim {
  constructor(seed = 42) {
    this.time = 0;
    this.rng = mulberry32(seed);
    this._events = []; // { at, seq, fn }
    this._seq = 0;
  }

  schedule(delay, fn) {
    this._events.push({ at: this.time + delay, seq: this._seq++, fn });
  }

  _takeDue() {
    let best = -1;
    for (let i = 0; i < this._events.length; i++) {
      const e = this._events[i];
      if (e.at > this.time) continue;
      if (best < 0) { best = i; continue; }
      const b = this._events[best];
      if (e.at < b.at || (e.at === b.at && e.seq < b.seq)) best = i;
    }
    return best < 0 ? null : this._events.splice(best, 1)[0];
  }

  /** Advance virtual time by `ms`, delivering due events and ticking every
   * node each `tickEvery` virtual milliseconds. */
  async advance(ms, nodes = [], tickEvery = 10) {
    const end = this.time + ms;
    while (this.time < end) {
      this.time = Math.min(this.time + tickEvery, end);
      let e;
      while ((e = this._takeDue())) {
        e.fn();
        await drainMicrotasks();
      }
      for (const n of nodes) n.tick(this.time);
      await drainMicrotasks();
    }
  }
}

export class MemoryNetwork {
  constructor(sim, { minDelay = 1, maxDelay = 5 } = {}) {
    this.sim = sim;
    this.minDelay = minDelay;
    this.maxDelay = maxDelay;
    this._handlers = new Map();  // id -> (msg) -> reply
    this._groups = null;         // array of Sets, or null = fully connected
  }

  register(id, handler) { this._handlers.set(id, handler); }
  unregister(id) { this._handlers.delete(id); }

  /** Partition the cluster into the given groups (arrays of ids); links
   * only work within a group. A node listed nowhere is isolated. */
  partition(...groups) { this._groups = groups.map((g) => new Set(g)); }
  heal() { this._groups = null; }

  _connected(a, b) {
    if (!this._groups) return true;
    const g = this._groups.find((s) => s.has(a));
    return !!g && g.has(b);
  }

  _delay() { return this.minDelay + this.sim.rng() * (this.maxDelay - this.minDelay); }

  call(from, to, msg) {
    return new Promise((resolve, reject) => {
      this.sim.schedule(this._delay(), () => {
        const handler = this._handlers.get(to);
        if (!handler || !this._connected(from, to)) {
          this.sim.schedule(this.maxDelay * 4, () => reject(new Error(`peer ${to} unreachable`)));
          return;
        }
        let replied;
        try { replied = Promise.resolve(handler(msg)); }
        catch (err) { replied = Promise.reject(err); }
        replied.then(
          (reply) => this.sim.schedule(this._delay(), () => {
            // The link may have been cut while the reply was in flight.
            if (this._connected(from, to)) resolve(reply);
            else reject(new Error(`peer ${to} unreachable`));
          }),
          (err) => this.sim.schedule(this._delay(), () => reject(err))
        );
      });
    });
  }
}

/**
 * Trivial deterministic state machine: {op:'set'|'del', k, v} commands,
 * kept in ONE FILE on the node's disk.
 *
 * The file is the point. An install replaces files — that is what an
 * install is — so a machine whose state lives only in a Map is a machine
 * no install can reach, and a harness built on one would exercise a
 * transfer nothing downstream performs. One file rewritten whole per
 * apply is wasteful and completely deterministic, which is the trade a
 * simulator wants.
 *
 * Without a disk it is the pure in-memory machine it always was, for the
 * tests that only need something to apply into (raft-host.test.js).
 */
export class KvMachine {
  constructor(disk = null) {
    this.disk = disk;
    this.map = new Map();
    this.applied = 0;
  }

  /** Read whatever is on disk — at boot, and again after an install has
   * put a different file there. */
  async reload() {
    if (!this.disk) return;
    let bytes = null;
    try {
      const h = await this.disk.openFile(KV_FILE, { create: false });
      bytes = new Uint8Array(h.getSize());
      if (bytes.length) h.read(bytes, { at: 0 });
    } catch { return; }        // no file yet: an empty machine
    if (!bytes.length) return;
    const state = decode(bytes);
    this.map = new Map(state.entries);
    this.applied = state.applied;
  }

  appliedIndex() { return this.applied; }

  async apply(entry) {
    const c = decode(entry.payload);
    if (c.op === 'set') this.map.set(c.k, c.v);
    else if (c.op === 'del') this.map.delete(c.k);
    this.applied = entry.index;
    await this._persist();
  }

  async _persist() {
    if (!this.disk) return;
    const bytes = encode({ entries: [...this.map.entries()], applied: this.applied });
    const h = await this.disk.openFile(KV_FILE, { create: true });
    h.truncate(0);
    h.write(bytes, { at: 0 });
    h.flush();
  }

  snapshot() { return [...this.map.entries()].sort(); }
}

export const kvSet = (k, v) => encode({ op: 'set', k, v });
export const kvDel = (k) => encode({ op: 'del', k });

/**
 * Quiesced leader-side snapshot: freeze the state machine at the node's
 * lastApplied into a new store generation, and compact the EntryLog
 * through that boundary into the log file the store pairs with it
 * (raising baseIndex -- what forces InstallSnapshot for lagging peers).
 * This is WalDb.snapshot() scaled down to one role file.
 *
 * `config.live` is the part that matters to an install: it says which
 * live filename each role is restored ONTO, and a follower adopting this
 * generation has no other way to know (raft_node.h).
 */
export async function takeSnapshot(member) {
  const { node, log, store, machine } = member;
  const boundary = node.lastApplied;
  const boundaryTerm = log.termAt(boundary);

  const tx = await store.begin();
  const handle = await tx.createFile('kv');
  const bytes = encode({ entries: [...machine.map.entries()], applied: boundary });
  handle.write(bytes, { at: 0 });
  handle.flush();
  await handle.close();
  await tx.commit({
    lastIncludedIndex: boundary, lastIncludedTerm: boundaryTerm,
    config: { live: [{ role: 'kv', name: KV_FILE }] }
  });

  const { name, handle: dest } = await store.createLogFile();
  dest.truncate(0);                       // may be a torn leftover
  await log.compact(dest, boundary, boundaryTerm);
  const fresh = new EntryLog(dest);
  await fresh.open();
  await log.close();
  member.log = fresh;
  node.log = fresh;                       // the setter repoints C too
  await store.pruneLogs(name);
  try { await member.disk.deleteFile(WAL_FILE); } catch { /* best-effort */ }
  // A new generation is a new set of filenames, and a leader streams
  // chunks from them inside a synchronous tick.
  await node.refreshSnapshotFiles();
  return boundary;
}

/**
 * Build an n-node cluster on the network. Returns Map(id -> { node, log,
 * disk, machine, store }); `disk` outlives restarts (bootNode reopens it).
 */
export async function makeCluster(n, sim, net, nodeOptions = {}) {
  const ids = Array.from({ length: n }, (_, i) => i + 1);
  const cluster = new Map();
  for (const id of ids) {
    cluster.set(id, await bootNode(id, ids, sim, net, makeDisk(), nodeOptions));
  }
  return cluster;
}

/**
 * Boot one member over `disk`. Everything durable is on it: the KV
 * file, the snapshot store's generations, and the log -- which is the
 * store's paired one if a generation has been adopted, and a plain WAL
 * otherwise. That is openWalStorage's rule, scaled down.
 */
export async function bootNode(id, ids, sim, net, disk, nodeOptions = {}) {
  const store = new SnapshotStore(directoryOf(disk), { prefix: SNAP_PREFIX });
  await store.open();

  const logName = store.latest ? store.logName : WAL_FILE;
  const log = new EntryLog(await disk.openFile(logName, { create: true }), store.latest
    ? { baseIndex: store.latest.lastIncludedIndex, baseTerm: store.latest.lastIncludedTerm }
    : {});
  await log.open();

  const machine = new KvMachine(disk);
  await machine.reload();
  const member = { disk, store, log, machine };

  // The node serves, receives and adopts an install itself: it reads the
  // generation's files, stages an incoming one, verifies it against the
  // leader's manifest, and rebases its own log onto the boundary
  // (raft_node.h). What is left here is opening a file -- asynchronous
  // in a browser, so never inside a synchronous C call -- and the window
  // the flip happens in.
  member.files = {
    store,
    open: (name) => disk.openFile(name, { create: true }),
    remove: (name) => disk.deleteFile(name),
    async swap(adopt) {
      // The database, such as it is, is one file. "Closing" it is having
      // nothing to reload from; reopening it is reloading.
      const victims = (await disk.listFiles())
        .filter((n) => !n.startsWith(`${SNAP_PREFIX}-`));
      await adopt(victims);
      await machine.reload();
      member.log = member.node.log;       // rn_adopt rebased it
    }
  };

  const node = new RaftNode({
    id, peers: ids, log, stateMachine: machine,
    transport: { call: (to, msg) => net.call(id, to, msg) },
    files: member.files,
    random: sim.rng,
    ...nodeOptions
  });
  member.node = node;
  net.register(id, (msg) => node.handleMessage(msg));
  await node.start(sim.time);
  return member;
}

/** Fully stop a member (network + node + log); its DISK survives for a
 * later bootNode — a crash-stop with durable storage. */
export async function stopNode(net, member) {
  net.unregister(member.node.id);
  await member.node.stop();
  await member.log.close();
  await member.store.close();
}

export function leaders(cluster) {
  return [...cluster.values()].filter((m) => m.node.isRunning && m.node.role === 'leader');
}

/** Wait (in virtual time) until `predicate()` holds; throws after `maxMs`. */
export async function until(sim, cluster, predicate, maxMs = 10_000, step = 50) {
  const nodes = [...cluster.values()].map((m) => m.node);
  for (let waited = 0; waited < maxMs; waited += step) {
    if (predicate()) return;
    await sim.advance(step, nodes);
  }
  if (!predicate()) throw new Error(`condition not reached within ${maxMs}ms of virtual time`);
}

/** Run a propose() (which needs the sim to advance to resolve) to
 * completion; returns { value } or { error }. */
export async function settle(sim, cluster, promise, maxMs = 5_000) {
  let outcome = null;
  const tracked = promise.then((value) => { outcome = { value }; }, (error) => { outcome = { error }; });
  await until(sim, cluster, () => outcome !== null, maxMs).catch(() => {});
  await tracked.catch(() => {});
  if (outcome === null) throw new Error('promise did not settle in virtual time');
  return outcome;
}
