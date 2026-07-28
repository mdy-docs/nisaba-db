/**
 * Replication roadmap step 5d: the reference TCP transport
 * (src/raft-transport-tcp.js) — framing, binary payload integrity,
 * error propagation, concurrency, unreachable peers — plus one REAL
 * cluster: three KV RaftNodes on RaftGroupHosts over localhost sockets
 * with real timers, electing and committing outside the simulator.
 */
import { describe, it, expect } from 'vitest';
import { ready, EntryLog, MemoryHandle } from '../wasm/nisaba-wasm.js';
import { RaftNode } from '../src/raft.js';
import { RaftGroupHost } from '../src/raft-host.js';
import { TcpRaftTransport } from '../src/raft-transport-tcp.js';
import { KvMachine, kvSet } from './raft-harness.js';

await ready();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function startTransport(onMessage) {
  const t = new TcpRaftTransport({ listenPort: 0, peers: {}, onMessage });
  await t.start();
  return t;
}

describe('tcp transport', () => {
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

  it('handler errors reject the call; concurrent calls interleave correctly', async () => {
    const a = await startTransport(async (env) => {
      if (env.boom) throw new Error(`boom ${env.i}`);
      await sleep(env.i % 3); // shuffle completion order
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

  it('an unreachable peer rejects and a later call redials', async () => {
    const a = await startTransport(async () => ({ pong: true }));
    const b = await startTransport(() => ({}));
    b.setPeer(1, { host: '127.0.0.1', port: a.address().port });
    b.setPeer(9, { host: '127.0.0.1', port: 1 }); // nothing listens there

    await expect(b.call(9, { x: 1 })).rejects.toThrow();
    expect((await b.call(1, { x: 1 })).pong).toBe(true);

    // Kill A and verify the dropped connection surfaces, then that a
    // restart at the same port serves again on redial.
    const port = a.address().port;
    await a.stop();
    await expect(b.call(1, { x: 2 })).rejects.toThrow();
    const a2 = new TcpRaftTransport({ listenPort: port, peers: {}, onMessage: async () => ({ pong: 2 }) });
    await a2.start();
    expect((await b.call(1, { x: 3 })).pong).toBe(2);
    await a2.stop();
    await b.stop();
  });

  it('a real 3-node KV cluster over TCP elects and commits with real timers', async () => {
    const ids = [1, 2, 3];
    const nodes = new Map(); // id -> { host, transport, node, machine }
    // Boot transports first (ephemeral ports), then exchange addresses.
    for (const id of ids) {
      const host = new RaftGroupHost({ transport: null, tickMs: 20, quiesceAfterMs: 0 });
      const transport = new TcpRaftTransport({
        listenPort: 0,
        peers: {},
        onMessage: (env) => host.handleEnvelope(env),
        requestTimeoutMs: 1000
      });
      await transport.start();
      host._transport = transport; // envelope transport (bound after start)
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
    }

    try {
      let leader = null;
      for (let i = 0; i < 100 && !leader; i++) {
        await sleep(50);
        leader = [...nodes.values()].find((n) => n.node.role === 'leader');
      }
      expect(leader).toBeDefined();

      await leader.node.propose(kvSet('over', 'tcp'));
      for (let i = 0; i < 100; i++) {
        if ([...nodes.values()].every((n) => n.machine.map.get('over') === 'tcp')) break;
        await sleep(50);
      }
      for (const n of nodes.values()) expect(n.machine.map.get('over')).toBe('tcp');
    } finally {
      for (const n of nodes.values()) {
        n.host.stop();
        await n.node.stop();
        await n.transport.stop();
        await n.log?.close?.();
      }
    }
  }, 20_000);
});
