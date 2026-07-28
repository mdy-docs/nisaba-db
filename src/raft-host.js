/**
 * raft-host.js — multi-group Raft hosting: replication roadmap step 5d.
 * One RaftGroupHost is one server process's seat across MANY Raft groups
 * (the step-4 decision: one group per tenant database, which only scales
 * if idle groups cost nothing). The host multiplexes every group over
 * one node-to-node transport and one clock, and quiesces idle groups:
 *
 *   - Envelopes: every inter-node message is { group, msg }. Outgoing,
 *     each group's RaftNode gets a transport shim that wraps its
 *     messages; incoming, handleEnvelope() routes to the group's node.
 *     One connection per node-pair carries every group's traffic (the
 *     transport is envelope-agnostic — see raft-transport-tcp.js).
 *
 *   - Time: one tick loop drives every group. tick(now) is manual (the
 *     simulator's virtual clock); start() runs it on a real interval.
 *
 *   - Quiescence: a group with no traffic and nothing outstanding for
 *     `quiesceAfterMs` has its node quiesced — the leader stops
 *     heartbeating, followers stop their election countdowns, and the
 *     group falls silent on the wire. Any envelope or local use (mark it
 *     with touch()) wakes it. The two ends quiesce independently but in
 *     the right order by construction: followers keep receiving
 *     heartbeats — activity — until the leader's side goes quiet first.
 *     A leader that dies while a group is quiesced is detected lazily,
 *     on next use: the woken follower's stale election deadline fires,
 *     pre-vote runs (waking peers through their hosts), and a new leader
 *     emerges — the standing trade-off of quiescence, acceptable
 *     precisely because the group was idle.
 *
 * Heartbeat coalescing across groups is a transport concern (batching
 * frames per node-pair); with quiescence doing the heavy lifting —
 * idle groups send nothing at all — it is deliberately not implemented
 * here.
 */

export class RaftGroupHost {
  /**
   * @param {object} options
   * @param {object} options.transport - node-to-node envelope transport:
   *   { call(peerNodeId, envelope) -> Promise<reply> }. Incoming envelopes
   *   must be routed to host.handleEnvelope.
   * @param {number} [options.tickMs=25] - real-time tick interval (start())
   * @param {number} [options.quiesceAfterMs=5000] - idle threshold; 0 or
   *   Infinity disables quiescence
   * @param {() => number} [options.now=Date.now] - clock (injectable for
   *   the simulator)
   */
  constructor({ transport, tickMs = 25, quiesceAfterMs = 5000, now = Date.now }) {
    this._transport = transport;
    this.tickMs = tickMs;
    this.quiesceAfterMs = quiesceAfterMs;
    this._now = now;
    this._groups = new Map(); // groupId -> { node, value, lastActivity }
    this._interval = null;
  }

  /** Per-group transport shim: wraps each message in this group's
   * envelope. Hand it to connectReplicated / new RaftNode. */
  groupTransport(groupId) {
    return {
      call: (peerNodeId, msg) => this._transport.call(peerNodeId, { group: groupId, msg })
    };
  }

  /**
   * Register a group. `value` is whatever owns the RaftNode — a
   * ReplicatedDb (uses .raft) or a bare RaftNode — created by the caller
   * with this host's groupTransport(groupId). Returns `value`.
   */
  addGroup(groupId, value) {
    if (this._groups.has(groupId)) throw new Error(`group already hosted: ${groupId}`);
    const node = value.raft ?? value;
    if (typeof node.tick !== 'function') throw new Error('addGroup value must be a RaftNode or expose .raft');
    this._groups.set(groupId, { node, value, lastActivity: this._now() });
    return value;
  }

  /** Deregister (the caller closes the node/db itself). */
  removeGroup(groupId) {
    this._groups.delete(groupId);
  }

  get groupIds() { return [...this._groups.keys()]; }

  group(groupId) { return this._groups.get(groupId)?.value; }

  /** Mark local activity (a client request touching this group) so an
   * idle-quiesced group wakes and stays awake while in use. */
  touch(groupId) {
    const g = this._groups.get(groupId);
    if (!g) return;
    g.lastActivity = this._now();
    g.node.wake(g.lastActivity);
  }

  /** The transport's receiving half: route an incoming envelope to its
   * group's node; the reply is the RPC reply. Unknown groups reject —
   * the sender treats it like an unreachable peer and retries. */
  handleEnvelope(envelope) {
    const g = this._groups.get(envelope.group);
    if (!g) throw new Error(`unknown raft group: ${envelope.group}`);
    g.lastActivity = this._now();
    return g.node.handleMessage(envelope.msg);
  }

  /** Drive every group's timers once; quiesce the idle. Manual for
   * simulators; start() runs it on an interval. */
  tick(now = this._now()) {
    for (const g of this._groups.values()) {
      const node = g.node;
      if (!node.isRunning) continue;
      if (this._shouldQuiesce(g, now)) {
        if (!node._quiesced) node.quiesce();
        continue;
      }
      node.tick(now);
    }
  }

  _shouldQuiesce(g, now) {
    if (!this.quiesceAfterMs || this.quiesceAfterMs === Infinity) return false;
    if (now - g.lastActivity < this.quiesceAfterMs) return false;
    const node = g.node;
    // Only the LEADER quiesces by idleness — its quiesce() parks the
    // followers via a flagged final heartbeat (their election timeouts
    // are shorter than any sane idle threshold, so host-side idling of
    // followers would misread leader silence as leader death and churn).
    // And only a settled leader: everything committed, applied, and on
    // every follower, so nobody sleeps while owed entries.
    if (node.role !== 'leader') return false;
    if (node.lastApplied !== node.commitIndex) return false;
    if (node.commitIndex !== node.log.lastIndex) return false;
    for (const p of node.peers) {
      if ((node._match.get(p) ?? 0) < node.log.lastIndex) return false;
    }
    return true;
  }

  start() {
    if (this._interval) return;
    this._interval = setInterval(() => this.tick(), this.tickMs);
    if (this._interval.unref) this._interval.unref();
  }

  stop() {
    if (this._interval) clearInterval(this._interval);
    this._interval = null;
  }
}
