/**
 * raft-transport-http.js — HTTP reference node-to-node transport, the
 * infrastructure-friendly sibling of raft-transport-tcp.js. Raft is
 * purely request/response and leader-initiated, so plain HTTP fits the
 * `call(peerId, envelope) → reply` contract with no push channel:
 * every call is one POST to the peer's `path`, body = encode(envelope)
 * as application/octet-stream (binjson — entry payloads and snapshot
 * chunks stay native binary), reply body = encode(reply). Zero
 * dependencies (node:http).
 *
 * Trade-offs vs the TCP transport (same contract, pick per deployment):
 *   + standard infra understands it: TLS, bearer/auth headers (see
 *     `headers`), firewalls, request logging, curl debugging;
 *   - ~150-200 bytes of header overhead per call vs a 4-byte frame —
 *     mostly moot under quiescence, where idle groups send nothing;
 *   - head-of-line risk: a snapshot chunk in flight can queue other
 *     groups' heartbeats behind it on a small pool — `maxSockets`
 *     (default 8) bounds it, and Raft tolerates the reordering.
 *
 * Operational notes:
 *   - keep-alive is load-bearing: calls share an explicit
 *     http.Agent({ keepAlive }) — without it every heartbeat would pay
 *     a TCP handshake. The server's keepAliveTimeout is raised to match
 *     (an idle-dropped socket surfaces as a one-off ECONNRESET on
 *     reuse; Raft's next heartbeat is the retry policy, as always).
 *   - peers must be DIRECT addresses. A load balancer breaks node
 *     identity, and proxies may buffer or cap bodies.
 *   - errors: a handler throw becomes a 500 whose body is the message,
 *     rejected at the caller; transport-level failures reject as-is.
 */
import http from 'node:http';
import { encode, decode } from '../wasm/nisaba-wasm.js';

const MAX_BODY_DEFAULT = 64 * 1024 * 1024; // a frame is at most a snapshot chunk + overhead

function readBody(stream, maxBytes) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    stream.on('data', (chunk) => {
      size += chunk.length;
      if (size > maxBytes) {
        reject(new Error(`body too large: ${size}`));
        stream.destroy();
        return;
      }
      chunks.push(chunk);
    });
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });
}

export class HttpRaftTransport {
  /**
   * @param {object} options
   * @param {number} options.listenPort - port to serve on (0 = ephemeral; see address())
   * @param {string} [options.listenHost='127.0.0.1']
   * @param {string} [options.path='/raft'] - URL path envelopes POST to
   * @param {Object<number, {host: string, port: number}>} [options.peers]
   *   nodeId -> address table; mutable via setPeer()
   * @param {(envelope: object) => object|Promise<object>} options.onMessage -
   *   the receiving half — wire to RaftGroupHost.handleEnvelope (or a
   *   single node's handleMessage)
   * @param {number} [options.requestTimeoutMs=5000]
   * @param {number} [options.maxSockets=8] - keep-alive pool per peer
   * @param {number} [options.maxBodyBytes=67108864]
   * @param {Object<string, string>} [options.headers] - extra request
   *   headers (e.g. a bearer token; verify in a wrapping onMessage or a
   *   fronting middleware)
   */
  constructor({
    listenPort, listenHost = '127.0.0.1', path = '/raft', peers = {},
    onMessage, requestTimeoutMs = 5000, maxSockets = 8,
    maxBodyBytes = MAX_BODY_DEFAULT, headers = {}
  }) {
    this.listenPort = listenPort;
    this.listenHost = listenHost;
    this.path = path;
    this._peers = new Map(Object.entries(peers).map(([id, addr]) => [Number(id), addr]));
    this._onMessage = onMessage;
    this.requestTimeoutMs = requestTimeoutMs;
    this.maxBodyBytes = maxBodyBytes;
    this._headers = headers;
    this._server = null;
    this._sockets = new Set(); // inbound, for a non-hanging stop() on older Node
    this._agent = new http.Agent({
      keepAlive: true,
      maxSockets,
      keepAliveMsecs: 30_000,
      timeout: requestTimeoutMs
    });
    this.isRunning = false;
  }

  setPeer(nodeId, addr) { this._peers.set(nodeId, addr); }
  removePeer(nodeId) { this._peers.delete(nodeId); }

  async start() {
    this._server = http.createServer((req, res) => this._serve(req, res));
    // Outlive the client pool's idle reuse window so quiesced groups
    // don't churn ECONNRESETs the moment they wake.
    this._server.keepAliveTimeout = 65_000;
    this._server.headersTimeout = 70_000;
    this._server.on('connection', (socket) => {
      this._sockets.add(socket);
      socket.on('close', () => this._sockets.delete(socket));
    });
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
    this._agent.destroy();
    if (this._server) {
      // server.close() waits for live keep-alive sockets; cut them.
      if (this._server.closeAllConnections) this._server.closeAllConnections();
      else for (const s of this._sockets) s.destroy();
      this._sockets.clear();
      await new Promise((resolve) => this._server.close(() => resolve()));
      this._server = null;
    }
  }

  async _serve(req, res) {
    if (req.method !== 'POST' || req.url !== this.path) {
      res.writeHead(404).end();
      return;
    }
    try {
      const body = await readBody(req, this.maxBodyBytes);
      const envelope = decode(new Uint8Array(body));
      const reply = await this._onMessage(envelope);
      const out = encode(reply);
      res.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-length': out.length
      });
      res.end(Buffer.from(out.buffer, out.byteOffset, out.length));
    } catch (err) {
      const message = String(err?.message ?? err);
      res.writeHead(500, {
        'content-type': 'text/plain; charset=utf-8',
        'content-length': Buffer.byteLength(message)
      });
      res.end(message);
    }
  }

  call(peerId, envelope) {
    if (!this.isRunning) return Promise.reject(new Error('transport not started'));
    const addr = this._peers.get(peerId);
    if (!addr) return Promise.reject(new Error(`no address for peer ${peerId}`));
    const body = encode(envelope);
    return new Promise((resolve, reject) => {
      const req = http.request({
        host: addr.host,
        port: addr.port,
        path: this.path,
        method: 'POST',
        agent: this._agent,
        timeout: this.requestTimeoutMs,
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': body.length,
          ...this._headers
        }
      }, (res) => {
        readBody(res, this.maxBodyBytes).then((buf) => {
          if (res.statusCode === 200) {
            try { resolve(decode(new Uint8Array(buf))); }
            catch (err) { reject(err); }
          } else {
            reject(new Error(buf.toString('utf8') || `peer ${peerId} replied ${res.statusCode}`));
          }
        }, reject);
      });
      req.on('timeout', () => req.destroy(new Error(`peer ${peerId} timed out`)));
      req.on('error', reject);
      req.end(Buffer.from(body.buffer, body.byteOffset, body.length));
    });
  }
}
