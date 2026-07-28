/**
 * raft-transport-tcp.js — the reference node-to-node transport for a
 * server cluster: replication roadmap step 5d. Zero dependencies
 * (node:net), so a three-node nisaba cluster is three Node processes and
 * a peer table. nisaba-web can use it as-is or as the template for its
 * own infrastructure (TLS, auth, service mesh) — the Raft layers only
 * ever see `call(peerId, envelope) → reply`.
 *
 * Wire format: length-prefixed binjson frames (4-byte LE length, then
 * encode({...})). binjson is the natural choice — Raft envelopes carry
 * entry payloads and snapshot chunks as raw bytes, which binjson's
 * binary type round-trips without base64 or copies into JSON. Frames:
 *
 *   { t: 'req', id, env }                    request (env = envelope)
 *   { t: 'res', id, ok: true,  value }       reply
 *   { t: 'res', id, ok: false, error }       handler threw (message text)
 *
 * Connections: each side dials ONE outbound socket per peer for its own
 * requests, lazily, and redials on the next call after a drop; inbound
 * sockets only serve requests TO this node. Every group's traffic for a
 * node-pair multiplexes over that one socket (the RaftGroupHost's
 * envelopes ride here). No proactive reconnect loops and no retry
 * queues: a failed or timed-out call rejects, and Raft's own heartbeats
 * are the retry policy.
 */
import net from 'node:net';
import { encode, decode } from '../wasm/nisaba-wasm.js';

const HEADER = 4;
const MAX_FRAME = 64 * 1024 * 1024; // sanity cap: a frame is at most a snapshot chunk + overhead

function frame(obj) {
  const body = encode(obj);
  const out = Buffer.allocUnsafe(HEADER + body.length);
  out.writeUInt32LE(body.length, 0);
  out.set(body, HEADER);
  return out;
}

/** Incremental frame parser over a socket's data events. */
class Parser {
  constructor(onFrame, onError) {
    this._buf = Buffer.alloc(0);
    this._onFrame = onFrame;
    this._onError = onError;
  }

  push(chunk) {
    this._buf = this._buf.length ? Buffer.concat([this._buf, chunk]) : chunk;
    while (this._buf.length >= HEADER) {
      const len = this._buf.readUInt32LE(0);
      if (len > MAX_FRAME) return this._onError(new Error(`frame too large: ${len}`));
      if (this._buf.length < HEADER + len) return;
      const body = new Uint8Array(this._buf.subarray(HEADER, HEADER + len)); // copy out of the Buffer pool
      this._buf = this._buf.subarray(HEADER + len);
      let obj;
      try { obj = decode(body); } catch (err) { return this._onError(err); }
      this._onFrame(obj);
    }
  }
}

export class TcpRaftTransport {
  /**
   * @param {object} options
   * @param {number} options.listenPort - port this node serves on (0 = ephemeral; see address())
   * @param {string} [options.listenHost='127.0.0.1']
   * @param {Object<number, {host: string, port: number}>} options.peers -
   *   nodeId -> address table (may include this node's own id; ignored).
   *   Mutable via setPeer() when membership changes.
   * @param {(envelope: object) => object|Promise<object>} options.onMessage -
   *   the receiving half — wire to RaftGroupHost.handleEnvelope (or a
   *   single node's handleMessage for a one-group cluster)
   * @param {number} [options.requestTimeoutMs=5000]
   */
  constructor({ listenPort, listenHost = '127.0.0.1', peers = {}, onMessage, requestTimeoutMs = 5000 }) {
    this.listenPort = listenPort;
    this.listenHost = listenHost;
    this._peers = new Map(Object.entries(peers).map(([id, addr]) => [Number(id), addr]));
    this._onMessage = onMessage;
    this.requestTimeoutMs = requestTimeoutMs;
    this._server = null;
    this._conns = new Map();   // peerId -> { socket, pending: Map(id -> {resolve, reject, timer}) }
    this._inbound = new Set(); // sockets serving requests TO this node
    this._nextId = 1;
    this.isRunning = false;
  }

  /** Add or update a peer's address (membership changes). */
  setPeer(nodeId, addr) { this._peers.set(nodeId, addr); }
  removePeer(nodeId) {
    this._peers.delete(nodeId);
    const conn = this._conns.get(nodeId);
    if (conn) this._dropConn(nodeId, conn, new Error(`peer ${nodeId} removed`));
  }

  async start() {
    this._server = net.createServer((socket) => this._serve(socket));
    await new Promise((resolve, reject) => {
      this._server.once('error', reject);
      this._server.listen(this.listenPort, this.listenHost, () => {
        this._server.removeListener('error', reject);
        resolve();
      });
    });
    this.isRunning = true;
  }

  /** The actual bound address (useful with listenPort 0 in tests). */
  address() { return this._server?.address(); }

  async stop() {
    this.isRunning = false;
    for (const [peerId, conn] of [...this._conns.entries()]) {
      this._dropConn(peerId, conn, new Error('transport stopped'));
    }
    // server.close() waits for open connections; kill the inbound ones.
    for (const socket of this._inbound) socket.destroy();
    this._inbound.clear();
    if (this._server) {
      await new Promise((resolve) => this._server.close(() => resolve()));
      this._server = null;
    }
  }

  // ---- serving (inbound) --------------------------------------------------

  _serve(socket) {
    this._inbound.add(socket);
    socket.on('close', () => this._inbound.delete(socket));
    const parser = new Parser(
      async (obj) => {
        if (obj.t !== 'req') return; // inbound sockets carry requests only
        let res;
        try {
          const value = await this._onMessage(obj.env);
          res = { t: 'res', id: obj.id, ok: true, value };
        } catch (err) {
          res = { t: 'res', id: obj.id, ok: false, error: String(err?.message ?? err) };
        }
        if (!socket.destroyed) socket.write(frame(res));
      },
      () => socket.destroy()
    );
    socket.on('data', (chunk) => parser.push(chunk));
    socket.on('error', () => socket.destroy());
  }

  // ---- calling (outbound) -------------------------------------------------

  call(peerId, envelope) {
    if (!this.isRunning) return Promise.reject(new Error('transport not started'));
    const addr = this._peers.get(peerId);
    if (!addr) return Promise.reject(new Error(`no address for peer ${peerId}`));
    const conn = this._conn(peerId, addr);
    const id = this._nextId++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        conn.pending.delete(id);
        reject(new Error(`peer ${peerId} timed out`));
      }, this.requestTimeoutMs);
      if (timer.unref) timer.unref();
      conn.pending.set(id, { resolve, reject, timer });
      conn.socket.write(frame({ t: 'req', id, env: envelope }));
    });
  }

  _conn(peerId, addr) {
    let conn = this._conns.get(peerId);
    if (conn) return conn;
    const socket = net.connect(addr.port, addr.host);
    socket.setNoDelay(true);
    conn = { socket, pending: new Map() };
    const parser = new Parser(
      (obj) => {
        if (obj.t !== 'res') return;
        const waiter = conn.pending.get(obj.id);
        if (!waiter) return; // timed out already
        conn.pending.delete(obj.id);
        clearTimeout(waiter.timer);
        if (obj.ok) waiter.resolve(obj.value);
        else waiter.reject(new Error(obj.error));
      },
      () => socket.destroy()
    );
    socket.on('data', (chunk) => parser.push(chunk));
    const fail = (err) => this._dropConn(peerId, conn, err ?? new Error(`peer ${peerId} unreachable`));
    socket.on('error', fail);
    socket.on('close', () => fail());
    this._conns.set(peerId, conn);
    return conn;
  }

  /** Tear down ONE specific connection: a dead socket's late 'close'
   * event must never touch the replacement connection a newer call has
   * already made for the same peer. */
  _dropConn(peerId, conn, err) {
    if (this._conns.get(peerId) === conn) this._conns.delete(peerId);
    conn.socket.destroy();
    for (const { reject, timer } of conn.pending.values()) {
      clearTimeout(timer);
      reject(err);
    }
    conn.pending.clear();
  }
}
