/**
 * Deterministic simulation harness for the Raft core (roadmap step 7):
 * a virtual clock with a seeded rng, an in-memory network with delays,
 * partitions, and unreachable-peer failures, a tiny KV state machine,
 * and a cluster factory over MemoryHandle-backed EntryLogs. Nothing here
 * touches a real clock: nodes see time only through tick(), the network
 * delivers only when the simulation advances, and every random draw
 * comes from the seed — a failing schedule replays exactly.
 */
import { EntryLog, MemoryHandle, encode, decode } from '../wasm/nisaba-wasm.js';
import { RaftNode } from '../src/raft.js';

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

/** Trivial deterministic state machine: {op:'set'|'del', k, v} commands. */
export class KvMachine {
  constructor() {
    this.map = new Map();
    this.applied = 0;
  }
  appliedIndex() { return this.applied; }
  apply(entry) {
    const c = decode(entry.payload);
    if (c.op === 'set') this.map.set(c.k, c.v);
    else if (c.op === 'del') this.map.delete(c.k);
    this.applied = entry.index;
  }
  snapshot() { return [...this.map.entries()].sort(); }
}

export const kvSet = (k, v) => encode({ op: 'set', k, v });
export const kvDel = (k) => encode({ op: 'del', k });

/**
 * Build an n-node cluster on the network. Returns Map(id -> { node, log,
 * handle, machine }); `handle` outlives restarts (restartNode reopens it).
 */
export async function makeCluster(n, sim, net, nodeOptions = {}) {
  const ids = Array.from({ length: n }, (_, i) => i + 1);
  const cluster = new Map();
  for (const id of ids) {
    cluster.set(id, await bootNode(id, ids, sim, net, new MemoryHandle(), nodeOptions));
  }
  return cluster;
}

export async function bootNode(id, ids, sim, net, handle, nodeOptions = {}) {
  const log = new EntryLog(handle);
  await log.open();
  const machine = new KvMachine();
  const node = new RaftNode({
    id, peers: ids, log, stateMachine: machine,
    transport: { call: (to, msg) => net.call(id, to, msg) },
    random: sim.rng,
    ...nodeOptions
  });
  net.register(id, (msg) => node.handleMessage(msg));
  await node.start(sim.time);
  return { node, log, handle, machine };
}

/** Fully stop a member (network + node + log); its handle survives for a
 * later bootNode — a crash-stop with durable storage. */
export async function stopNode(net, member) {
  net.unregister(member.node.id);
  await member.node.stop();
  await member.log.close();
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
