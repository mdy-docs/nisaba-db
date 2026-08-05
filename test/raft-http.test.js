/**
 * The HTTP reference transport (src/raft-transport-http.js): framing via
 * HTTP itself, binary payload integrity, error propagation, concurrency
 * over the keep-alive pool, unreachable peers and server restarts, and a
 * REAL 3-node KV cluster over localhost HTTP with real timers —
 * mirroring the TCP transport's suite.
 */
import { describe, it, expect } from 'vitest';
import http from 'node:http';
import { ready, EntryLog, MemoryHandle } from '../src/nisaba-wasm.js';
import { RaftNode } from '../src/raft.js';
import { RaftGroupHost } from '../src/raft-host.js';
import { HttpRaftTransport } from '../src/raft-transport-http.js';
import { KvMachine, kvSet } from './raft-harness.js';

await ready();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function startTransport(onMessage, options = {}) {
  const t = new HttpRaftTransport({ listenPort: 0, peers: {}, onMessage, ...options });
  await t.start();
  return t;
}

describe('http transport', () => {
  it('round-trips envelopes with binary payloads intact', async () => {
    const a = await startTransport(async (env) => ({ echoed: env, extra: new Uint8Array([9, 8, 7]) }));
    const b = await startTransport(() => ({}));
    b.setPeer(1, { host: '127.0.0.1', port: a.address().port });

    const payload = new Uint8Array(1000).map((_, i) => i % 251);
    const reply = await b.call(1, { group: 'g', msg: { kind: 'x', data: payload, n: 42 } });
    expect(reply.echoed.msg.n).toBe(42);
    expect(reply.echoed.msg.data).toBeInstanceOf(Uint8Array);
    expect([...reply.echoed.msg.data]).toEqual([...payload]);
    expect([...reply.extra]).toEqual([9, 8, 7]);
    await a.stop();
    await b.stop();
  });

  it('handler errors reject with the message; concurrent calls interleave over the pool', async () => {
    const a = await startTransport(async (env) => {
      if (env.boom) throw new Error(`boom ${env.i}`);
      await sleep(env.i % 3);
      return { i: env.i };
    });
    const b = await startTransport(() => ({}));
    b.setPeer(1, { host: '127.0.0.1', port: a.address().port });

    const calls = Array.from({ length: 20 }, (_, i) =>
      b.call(1, { i, boom: i % 5 === 0 }).then((r) => ({ ok: r.i }), (e) => ({ err: e.message })));
    const results = await Promise.all(calls);
    results.forEach((r, i) => {
      if (i % 5 === 0) expect(r.err).toBe(`boom ${i}`);
      else expect(r.ok).toBe(i);
    });
    await a.stop();
    await b.stop();
  });

  it('an unreachable peer rejects; a restarted server serves again on the next call', async () => {
    const a = await startTransport(async () => ({ pong: 1 }));
    const b = await startTransport(() => ({}));
    b.setPeer(1, { host: '127.0.0.1', port: a.address().port });
    b.setPeer(9, { host: '127.0.0.1', port: 1 });

    await expect(b.call(9, { x: 1 })).rejects.toThrow();
    expect((await b.call(1, { x: 1 })).pong).toBe(1);

    const port = a.address().port;
    await a.stop();
    await expect(b.call(1, { x: 2 })).rejects.toThrow();
    const a2 = new HttpRaftTransport({ listenPort: port, peers: {}, onMessage: async () => ({ pong: 2 }) });
    await a2.start();
    expect((await b.call(1, { x: 3 })).pong).toBe(2);
    await a2.stop();
    await b.stop();
  });

  it('speaks plain HTTP: wrong path 404s, custom headers reach the peer', async () => {
    const seen = [];
    const a = await startTransport(async (env) => ({ ok: env.n }), { path: '/cluster' });
    // Header visibility needs the raw request; probe with a bare client.
    const b = await startTransport(() => ({}), { headers: { 'x-cluster-token': 'sesame' } });
    b.setPeer(1, { host: '127.0.0.1', port: a.address().port });
    // Patch a's server to record header arrival (a fronting auth
    // middleware would live exactly here in nisaba-web).
    a._server.prependListener('request', (req) => seen.push(req.headers['x-cluster-token']));

    // The transport's own path works; the wrong path is a plain 404.
    b.path = '/cluster';
    expect((await b.call(1, { n: 5 })).ok).toBe(5);
    expect(seen).toContain('sesame');
    const status = await new Promise((resolve) => {
      http.get({ host: '127.0.0.1', port: a.address().port, path: '/nope' }, (res) => {
        res.resume();
        resolve(res.statusCode);
      });
    });
    expect(status).toBe(404);
    await a.stop();
    await b.stop();
  });

  it('a real 3-node KV cluster over HTTP elects and commits with real timers', async () => {
    const ids = [1, 2, 3];
    const nodes = new Map();
    for (const id of ids) {
      const host = new RaftGroupHost({ transport: null, tickMs: 20, quiesceAfterMs: 0 });
      const transport = new HttpRaftTransport({
        listenPort: 0,
        peers: {},
        onMessage: (env) => host.handleEnvelope(env),
        requestTimeoutMs: 1000
      });
      await transport.start();
      host._transport = transport;
      nodes.set(id, { host, transport });
    }
    for (const [id, n] of nodes) {
      for (const [other, o] of nodes) {
        if (other !== id) n.transport.setPeer(other, { host: '127.0.0.1', port: o.transport.address().port });
      }
    }
    for (const [id, n] of nodes) {
      const log = new EntryLog(new MemoryHandle());
      await log.open();
      const machine = new KvMachine();
      const node = new RaftNode({
        id, peers: ids, log, stateMachine: machine,
        transport: n.host.groupTransport('kv')
      });
      await node.start(Date.now());
      n.host.addGroup('kv', node);
      n.host.start();
      n.node = node;
      n.machine = machine;
      n.log = log;
    }

    try {
      let leader = null;
      for (let i = 0; i < 100 && !leader; i++) {
        await sleep(50);
        leader = [...nodes.values()].find((n) => n.node.role === 'leader');
      }
      expect(leader).toBeDefined();

      await leader.node.propose(kvSet('over', 'http'));
      for (let i = 0; i < 100; i++) {
        if ([...nodes.values()].every((n) => n.machine.map.get('over') === 'http')) break;
        await sleep(50);
      }
      for (const n of nodes.values()) expect(n.machine.map.get('over')).toBe('http');
    } finally {
      for (const n of nodes.values()) {
        n.host.stop();
        await n.node.stop();
        await n.transport.stop();
        await n.log.close();
      }
    }
  }, 20_000);
});
