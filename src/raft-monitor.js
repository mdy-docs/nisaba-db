/**
 * raft-monitor.js — the observability endpoint for a cluster node: a
 * small standalone HTTP server (node:http, zero dependencies) exposing
 * a RaftGroupHost's state two ways:
 *
 *   GET /status   one-off JSON snapshot — "what is true right now".
 *                 host.status(): every group's role/term/leader, log
 *                 bounds, commit/apply positions, member records with
 *                 voting flags, the leader's per-peer replication view
 *                 (match/lag/reachable/needsSnapshot), idle times.
 *                 `curl node:9001/status | jq`.
 *
 *   GET /events   a genuine STREAM via Server-Sent Events — "what
 *                 changed". The first event on connect is a full status
 *                 snapshot (event: status), so consumers never race a
 *                 separate snapshot fetch; after that, every cluster
 *                 event the host aggregates (event: raft — elections,
 *                 role changes, config adoptions, learner promotions,
 *                 snapshot installs, edge-triggered peer reachability,
 *                 quiesce/wake, conflict truncations, apply halts),
 *                 tagged with its group id. `curl -N node:9001/events`,
 *                 or `new EventSource(url)` in a browser admin page —
 *                 SSE auto-reconnects and needs no client library.
 *
 * Deliberately a SEPARATE server from the cluster transport: a
 * TCP-transport cluster gets the same observability, the debug port can
 * be firewalled independently of the cluster port, and the transports
 * stay pure. The monitor is read-only and unauthenticated — bind it to
 * localhost or a management network, or front it with your own auth,
 * exactly as you would any debug port.
 *
 * Event JSON shape: { group, type, time, node, term, ...fields } — the
 * RaftNode's envelope (src/raft.js `onEvent`) plus the host's group
 * tag. `time` is the node's clock (the host's `now`), so simulated
 * clusters emit simulated timestamps.
 */
import http from 'node:http';

const SSE_HEADERS = {
  'content-type': 'text/event-stream',
  'cache-control': 'no-cache',
  'connection': 'keep-alive',
  'access-control-allow-origin': '*' // a read-only debug feed; see header doc
};

export class RaftMonitor {
  /**
   * @param {object} host - a RaftGroupHost (anything with status() and a
   *   chainable onEvent property works)
   * @param {object} options
   * @param {number} options.listenPort - port to serve on (0 = ephemeral)
   * @param {string} [options.listenHost='127.0.0.1'] - default is
   *   deliberately loopback-only; widen it consciously
   * @param {number} [options.heartbeatMs=15000] - SSE keep-alive comment
   *   interval (defeats idle-connection reapers between curl and node)
   */
  constructor(host, { listenPort, listenHost = '127.0.0.1', heartbeatMs = 15_000 } = {}) {
    this._host = host;
    this.listenPort = listenPort;
    this.listenHost = listenHost;
    this.heartbeatMs = heartbeatMs;
    this._server = null;
    this._clients = new Set(); // open SSE responses
    this._timer = null;
    // Chain into the host's aggregated stream (preserving any existing
    // listener) — every event fans out to every connected client.
    const previous = host.onEvent;
    host.onEvent = (event) => {
      this._broadcast('raft', event);
      if (previous) previous(event);
    };
  }

  async start() {
    this._server = http.createServer((req, res) => this._serve(req, res));
    await new Promise((resolve, reject) => {
      this._server.once('error', reject);
      this._server.listen(this.listenPort, this.listenHost, () => {
        this._server.removeListener('error', reject);
        resolve();
      });
    });
    this._timer = setInterval(() => {
      for (const res of this._clients) res.write(': keep-alive\n\n');
    }, this.heartbeatMs);
    if (this._timer.unref) this._timer.unref();
  }

  /** The actual bound address (useful with listenPort 0 in tests). */
  address() { return this._server?.address(); }

  async stop() {
    if (this._timer) clearInterval(this._timer);
    this._timer = null;
    for (const res of this._clients) res.end();
    this._clients.clear();
    if (this._server) {
      if (this._server.closeAllConnections) this._server.closeAllConnections();
      await new Promise((resolve) => this._server.close(() => resolve()));
      this._server = null;
    }
  }

  _serve(req, res) {
    if (req.method !== 'GET') {
      res.writeHead(405).end();
      return;
    }
    if (req.url === '/status') {
      const body = JSON.stringify(this._host.status(), null, 2);
      res.writeHead(200, {
        'content-type': 'application/json',
        'access-control-allow-origin': '*'
      });
      res.end(body);
      return;
    }
    if (req.url === '/events') {
      res.writeHead(200, SSE_HEADERS);
      // Snapshot-first: event zero is the full status, so a consumer
      // never has to do the racy snapshot-then-subscribe dance.
      res.write(`event: status\ndata: ${JSON.stringify(this._host.status())}\n\n`);
      this._clients.add(res);
      req.on('close', () => this._clients.delete(res));
      return;
    }
    res.writeHead(404, { 'content-type': 'text/plain' }).end('try /status or /events');
  }

  _broadcast(eventName, payload) {
    if (this._clients.size === 0) return;
    const frame = `event: ${eventName}\ndata: ${JSON.stringify(payload)}\n\n`;
    for (const res of this._clients) {
      try { res.write(frame); } catch { this._clients.delete(res); }
    }
  }
}
