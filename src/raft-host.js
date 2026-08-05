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
import { encode, decode } from './nisaba-wasm.js';


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
  constructor({ transport, tickMs = 25, quiesceAfterMs = 5000, now = Date.now, onEvent = null }) {
    this._transport = transport;
    this.tickMs = tickMs;
    this.quiesceAfterMs = quiesceAfterMs;
    this._now = now;
    this._groups = new Map(); // groupId -> { node, value, lastActivity }
    this._interval = null;
    /** Aggregated observability stream: every group's RaftNode events,
     * tagged with { group } — set it (or pass the option) and addGroup
     * chains each node's onEvent into it. RaftMonitor consumes this. */
    this.onEvent = onEvent;
  }

  /**
   * One JSON-able snapshot across every hosted group — node.status()
   * per group plus the host's own idle bookkeeping. The "what is true
   * right now" half; the onEvent stream is the "what changed" half.
   */
  status() {
    const now = this._now();
    const groups = {};
    for (const [gid, g] of this._groups) {
      groups[gid] = {
        ...g.node.status(),
        idleMs: now - g.lastActivity
      };
    }
    return {
      time: now,
      tickMs: this.tickMs,
      quiesceAfterMs: this.quiesceAfterMs,
      groupCount: this._groups.size,
      groups
    };
  }

  /** Per-group transport shim: wraps each message in this group's
   * envelope. Hand it to connectReplicated / new RaftNode. */
  groupTransport(groupId) {
    return {
      // `msg` is already the encoded bytes the node produced; the
      // envelope only says which group it belongs to.
      call: (peerNodeId, msg) => this._transport.call(peerNodeId, { group: groupId, msg })
    };
  }

  /**
   * Register a group. `value` is whatever owns the RaftNode — a
   * ReplicatedDb (uses .raft) or a bare RaftNode — created by the caller
   * with this host's groupTransport(groupId). Returns `value`.
   *
   * Address sync BY CONSTRUCTION: the node's onConfig hook is chained so
   * every membership adoption (bootstrap, apply, restart, install)
   * pushes each member record's address into the transport's peer table
   * (transport.setPeer, when it exists) — the log IS the address book,
   * and it is applied immediately for whatever the node already knows.
   * Addresses are only ever ADDED or UPDATED, never auto-removed: the
   * transport is shared by every group on this host, and another group
   * may still talk to a node this group just dropped — pruning dead
   * addresses is host policy (transport.removePeer), not ours.
   */
  addGroup(groupId, value) {
    if (this._groups.has(groupId)) throw new Error(`group already hosted: ${groupId}`);
    const node = value.raft ?? value;
    if (typeof node.tick !== 'function') throw new Error('addGroup value must be a RaftNode or expose .raft');
    if (typeof this._transport?.setPeer === 'function') {
      const previous = node.onConfig;
      node.onConfig = (members) => {
        this._applyAddresses(members);
        if (previous) previous(members);
      };
      node.onConfig(node.memberInfo); // sync what it already knows
    }
    {
      // Aggregate the node's event stream, tagged with the group id.
      const previous = node.onEvent;
      node.onEvent = (event) => {
        if (this.onEvent) {
          try { this.onEvent({ group: groupId, ...event }); } catch { /* observer's problem */ }
        }
        if (previous) previous(event);
      };
    }
    this._groups.set(groupId, { node, value, lastActivity: this._now() });
    return value;
  }

  _applyAddresses(members) {
    for (const m of members) {
      if (m.id !== undefined && m.host !== undefined && m.port !== undefined) {
        this._transport.setPeer(m.id, { host: m.host, port: m.port });
      }
    }
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
        if (!node.quiesced) node.quiesce();
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
      if (node.matchOf(p) < node.log.lastIndex) return false;
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

/**
 * The seed loop shared by joinGroup/leaveGroup: send `msg` for `groupId`
 * to any address that answers, follow leader redirects, retry through
 * elections. `targets` starts as the seeds; a redirect that names the
 * leader's address jumps the queue. Resolves with the reply's adopted
 * member records.
 *
 * `groupId` NULL sends the message BARE, with no { group, msg } around
 * it. The envelope exists because RaftGroupHost multiplexes many groups
 * over one transport; a member that hosts one group has nothing to
 * multiplex and no envelope, which is the shape a native member speaks
 * (server/peers.h: the frame's `env` is the message and nothing around
 * it). It is the same distinction as groupTransport() versus handing a
 * node the raw transport, applied to the one message a joiner sends
 * before it is a member of anything.
 */
async function seedRequest(transport, groupId, msg, { seeds, attempts = 20, delayMs = 250 }) {
  if (typeof transport.callAddress !== 'function') {
    throw new Error('joining needs a transport with callAddress(addr, envelope)');
  }
  if (!seeds || seeds.length === 0) throw new Error('at least one seed address is required');
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  let preferred = null; // last leader hint
  let lastError = null;
  for (let attempt = 0; attempt < attempts; attempt++) {
    const targets = preferred ? [preferred, ...seeds] : [...seeds];
    for (const addr of targets) {
      let reply;
      try {
        // Messages cross as bytes -- the transport frames, it does not
        // interpret (raft_msg.h). This helper is the one place that has
        // to look inside a reply, so it decodes here.
        const envelope = groupId === null
          ? encode(msg)
          : { group: groupId, msg: encode(msg) };
        reply = decode(await transport.callAddress(addr, envelope));
      } catch (err) {
        lastError = err;
        continue; // seed down; try the next
      }
      if (reply.ok) return reply.members;
      if (reply.error) throw new Error(reply.error); // a validation refusal never heals by retrying
      if (reply.leaderAddress) preferred = reply.leaderAddress;
      lastError = new Error(reply.retry
        ? 'membership change in flight'
        : `no leader yet (last hint: node ${reply.leaderId ?? 0})`);
    }
    await sleep(delayMs); // mid-election or change in flight; let it settle
  }
  throw new Error(`could not ${msg.kind} group "${groupId}" after ${attempts} attempts: ${lastError?.message ?? 'no seed reachable'}`);
}

/**
 * Join a Raft group knowing only seed ADDRESSES — the "spin up a node
 * and it finds the club" flow. `member` is this node's record ({ id,
 * host, port }); the request lands on any seed, follows the leader
 * redirect, and resolves once the leader's CONFIG entry commits, with
 * the adopted member records. Call BEFORE creating the group's RaftNode
 * and pass the result as its `peers` — the whole flow:
 *
 *   const transport = new TcpRaftTransport({ listenPort, onMessage: (e) => host.handleEnvelope(e) });
 *   await transport.start();
 *   const me = { id: 4, host: myHost, port: transport.address().port };
 *   const members = await joinGroup(transport, 'tenant-1', me, { seeds: [{ host, port }] });
 *   const rdb = await connectReplicated(provider, { id: 4, peers: members, transport: host.groupTransport('tenant-1') });
 *   host.addGroup('tenant-1', rdb);   // addGroup syncs the peer table from the records
 *
 * Idempotent (a retry after a lost reply re-joins harmlessly), and a
 * RESTART needs no join at all: the node's own log carries the last
 * CONFIG entry, and onConfig rebuilds the peer table from it.
 *
 * `groupId` null joins a cluster that hosts ONE group and wraps nothing
 * — which is what a native member (server/main.c's --join) is. The reply
 * is decided by the same rn_handle either way; only the envelope
 * differs. See seedRequest.
 */
export async function joinGroup(transport, groupId, member, options) {
  const members = await seedRequest(transport, groupId, { kind: 'join', member }, options);
  // Prime the local peer table so the node can talk to everyone the
  // moment it boots (addGroup re-syncs from the records again anyway).
  if (typeof transport.setPeer === 'function') {
    for (const m of members) {
      if (m.host !== undefined && m.id !== member.id) transport.setPeer(m.id, { host: m.host, port: m.port });
    }
  }
  return members;
}

/**
 * Remove a member (graceful decommission from the departing node, or an
 * admin removing a dead one) via any seed address. Resolves with the
 * adopted member records once committed. The departed node's process is
 * the caller's to shut down; its address stays in surviving transports
 * until host policy prunes it (see addGroup's doc).
 */
export async function leaveGroup(transport, groupId, id, options) {
  return seedRequest(transport, groupId, { kind: 'leave', id }, options);
}
