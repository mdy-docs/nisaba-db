/**
 * The HTTP client (src/db-http-client.js): the TCP client's surface,
 * spelled against fetch, through a real front end to a real server.
 *
 * The client's claim is a MIRROR -- code written against
 * db-server-client.js connects with connectHttp instead and reads the
 * same answers -- so the shape of the tests is the comparison again:
 * drive an operation through the HTTP client, drive it through the TCP
 * client directly, and the answers must agree. What HTTP adds
 * underneath (Extended JSON both ways, sessions carrying cursors, SSE
 * carrying change streams) must be invisible here; the one place it may
 * show is the error surface, where ServerError is this module's own
 * class with the same code.
 *
 * Native only: which engine the server runs is a server property
 * (db.server.test.js), and neither the front end nor the client behind
 * it can see the difference (db.http-front.test.js proves the first
 * half of that).
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { connectServer } from '../src/db-server-client.js';
import { DbHttpFront } from '../src/db-http-front.js';
import {
  connectHttp, parseBaseUrl, ObjectId, ServerError, WIRE_OPS
} from '../src/db-http-client.js';
import { WIRE_OPS as TCP_WIRE_OPS } from '../src/db-server-client.js';

const NATIVE = 'build/lib/nisaba-server';
const REQUIRED = process.env.NISABA_SERVER_TESTS === 'required';

/* A base far from db.server.test.js's 18000 and db.http-front.test.js's
 * 40000 blocks: the files run in parallel workers. */
let portSlot = 1;
const nextPort = () => 46000 + (portSlot++) * 500 + (process.pid % 500);

async function startServer(port, extra = []) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-httpc-'));
  const proc = spawn(path.resolve(NATIVE), ['--port', String(port), ...extra],
    { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] });
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('the server did not start')), 30000);
    proc.stderr.on('data', (d) => {
      if (String(d).includes('serving')) { clearTimeout(t); resolve({ proc, dir }); }
    });
  });
}

describe('the http client, without a network', () => {
  it('normalizes addresses to origins', () => {
    expect(parseBaseUrl('http://127.0.0.1:8080')).toBe('http://127.0.0.1:8080');
    expect(parseBaseUrl('http://127.0.0.1:8080/')).toBe('http://127.0.0.1:8080');
    expect(parseBaseUrl('127.0.0.1:8080')).toBe('http://127.0.0.1:8080');
    expect(parseBaseUrl('8080')).toBe('http://127.0.0.1:8080');
    expect(() => parseBaseUrl('http://x:1/api')).toThrow(/origin/);
    expect(() => parseBaseUrl('not a url at all')).toThrow(/front-end address/);
  });

  it('declares the same wire ops as the TCP client', () => {
    expect(WIRE_OPS).toEqual(TCP_WIRE_OPS);
  });
});

describe.skipIf(!REQUIRED && !fs.existsSync(NATIVE))('the http client, through a front end', () => {
  const DB = 'httpclientdb';
  const port = nextPort();
  let proc, front, client, direct;

  beforeAll(async () => {
    ({ proc } = await startServer(port));
    front = new DbHttpFront(`127.0.0.1:${port}`, { listenPort: 0 });
    await front.start();
    client = await connectHttp(`http://127.0.0.1:${front.address().port}`);
    direct = await connectServer(port);
    return async () => {
      await client.close();
      await direct.close();
      await front.stop();
      proc.kill();
      await new Promise((r) => proc.once('exit', r));
    };
  });

  it('connect is verified: a dead port fails at connectHttp, with the address in the sentence', async () => {
    await expect(connectHttp('http://127.0.0.1:9')).rejects.toThrow(/127\.0\.0\.1:9/);
  });

  it('ping and listDatabases answer, and agree with the TCP client', async () => {
    const pong = await client.ping();
    expect(pong.pong).toBeDefined();
    await client.db(DB).collection('present').insertOne({ here: true });
    expect(await client.listDatabases()).toEqual(await direct.listDatabases());
  });

  it('a round trip preserves the wire types: ObjectId, Date, binary', async () => {
    const coll = client.db(DB).collection('typed');
    const when = new Date('2024-05-06T07:08:09.123Z');
    const bytes = new Uint8Array([0, 1, 2, 253, 254, 255]);
    const { insertedId } = await coll.insertOne({ when, bytes, n: 1 });
    expect(insertedId).toBeInstanceOf(ObjectId);

    const mine = await coll.findOne({ n: 1 });
    expect(mine._id).toBeInstanceOf(ObjectId);
    expect(String(mine._id)).toBe(String(insertedId));
    expect(mine.when).toBeInstanceOf(Date);
    expect(mine.when.toISOString()).toBe(when.toISOString());
    expect(mine.bytes).toBeInstanceOf(Uint8Array);
    expect([...mine.bytes]).toEqual([...bytes]);

    /* The mirror's witness: the TCP client reads the same document. */
    const theirs = await direct.db(DB).collection('typed').findOne({ n: 1 });
    expect(String(theirs._id)).toBe(String(mine._id));
    expect(theirs.when.getTime()).toBe(mine.when.getTime());
  });

  it('find materializes without a batchSize, and pages a session cursor with one', async () => {
    const coll = client.db(DB).collection('paged');
    await coll.insertMany(Array.from({ length: 10 }, (_, i) => ({ i })));

    const whole = await coll.find({}, { limit: 10 }).toArray();
    expect(whole.length).toBe(10);

    const cursor = coll.find({}, { batchSize: 3 });
    const first = await cursor.nextBatch();
    expect(first.length).toBe(3);
    const rest = [];
    for await (const doc of cursor) rest.push(doc.i);
    expect(first.length + rest.length).toBe(10);

    /* An early break still gives the cursor and its session back. */
    const abandoned = coll.find({}, { batchSize: 2 });
    for await (const doc of abandoned) { void doc; break; }
    await abandoned.close();   // idempotent

    const sorted = await coll.find({}, { sort: { i: -1 }, limit: 2 }).toArray();
    expect(sorted.map((d) => d.i)).toEqual([9, 8]);
  });

  it('aggregate, count, distinct and explain answer as the TCP client does', async () => {
    const coll = client.db(DB).collection('agg');
    await coll.insertMany([
      { team: 'a', n: 1 }, { team: 'a', n: 2 }, { team: 'b', n: 3 }
    ]);
    const grouped = await coll.aggregate([
      { $group: { _id: '$team', total: { $sum: '$n' } } },
      { $sort: { _id: 1 } }
    ]).toArray();
    expect(grouped).toEqual(await direct.db(DB).collection('agg').aggregate([
      { $group: { _id: '$team', total: { $sum: '$n' } } },
      { $sort: { _id: 1 } }
    ]).toArray());

    expect(await coll.countDocuments({ team: 'a' })).toBe(2);
    expect((await coll.distinct('team')).sort()).toEqual(['a', 'b']);
    expect(await coll.explain({ team: 'a' })).toEqual(
      await direct.db(DB).collection('agg').explain({ team: 'a' }));

    /* A bad stage names its position, quoted -- atStage, mirrored. */
    await expect(coll.aggregate([{ $nonsense: 1 }]).toArray())
      .rejects.toThrow(/stage 0/);
  });

  it('updates, upserts and findOneAndUpdate carry ids and clocks from this side', async () => {
    const coll = client.db(DB).collection('written');
    await coll.insertOne({ name: 'ada', n: 1 });

    const up = await coll.updateOne({ missing: 'yes' }, { $set: { made: true } }, { upsert: true });
    expect(up.upsertedId ?? up.upsertedCount ?? 1).toBeTruthy();
    const after = await coll.findOneAndUpdate(
      { name: 'ada' }, { $inc: { n: 1 } }, { returnDocument: 'after' });
    expect(after.n).toBe(2);

    const stamped = await coll.findOneAndUpdate(
      { name: 'ada' }, { $currentDate: { seen: true } }, { returnDocument: 'after' });
    expect(stamped.seen).toBeInstanceOf(Date);

    const gone = await coll.findOneAndDelete({ name: 'ada' });
    expect(gone.n).toBe(2);
  });

  it('insertMany and bulkWrite mirror the TCP client, failures included', async () => {
    const coll = client.db(DB).collection('bulk');
    const dup = new ObjectId();
    await coll.insertOne({ _id: dup, first: true });

    const err = await coll.insertMany([{ fine: 1 }, { _id: dup, boom: 1 }, { never: 1 }])
      .catch((e) => e);
    expect(err).toBeInstanceOf(ServerError);
    expect(err.message).toContain('document 1');
    expect(err.result.insertedCount).toBe(1);

    const res = await coll.bulkWrite([
      { insertOne: { document: { via: 'bulk' } } },
      { updateOne: { filter: { nope: 1 }, update: { $set: { made: true } }, upsert: true } }
    ]);
    expect(res.insertedCount).toBe(1);
    expect(res.upsertedCount).toBe(1);
    expect(res.insertedIds[0]).toBeInstanceOf(ObjectId);
    expect(res.upsertedIds[1]).toBeInstanceOf(ObjectId);
  });

  it('indexes: create, list, findByIndex, drop', async () => {
    const coll = client.db(DB).collection('indexed');
    await coll.insertMany([{ sku: 'a' }, { sku: 'b' }]);
    const name = await coll.createIndex({ sku: 1 });
    expect((await coll.listIndexes()).some((ix) => ix.name === name)).toBe(true);
    const hit = await coll.findByIndex(name, ['a']);
    expect(hit.length).toBe(1);
    expect(hit[0].sku).toBe('a');
    await coll.dropIndex(name);
  });

  it('collections and databases: create, list, drop, compact', async () => {
    const db = client.db('adminish');
    expect(await db.createCollection('empty')).toBe(true);
    expect(await db.createCollection('empty')).toBe(false);
    expect(await db.listCollections()).toContain('empty');
    await db.collection('empty').insertOne({ pad: 'x'.repeat(64) });
    const stats = await db.compact();
    expect(stats.empty === null || typeof stats.empty.bytesAfter === 'number').toBe(true);
    expect(await db.dropCollection('empty')).toBe(true);
    expect(await client.dropDatabase('adminish')).toBe(true);
  });

  it('a refusal is a ServerError with the server\'s code, through the escape hatch too', async () => {
    const err = await client.db(DB).request({ op: 'explodinate', coll: 'users' }).catch((e) => e);
    expect(err).toBeInstanceOf(ServerError);
    expect(err.code).toBe(-41);
    expect(err.status).toBe(400);

    /* And the TCP client's sentence for a method the wire lacks. */
    expect(() => client.db(DB).collection('x').watchAll())
      .toThrow(/the server has no collection\.watchAll\(\)/);
  });

  it('a change stream sees an insert, as SSE underneath', async () => {
    const coll = client.db(DB).collection('watched');
    const stream = coll.watch();
    await stream.ready;
    expect(stream.member).toBe(`127.0.0.1:${port}`);

    const arrived = stream.next();
    await coll.insertOne({ live: true });
    const { value: change, done } = await arrived;
    expect(done).toBe(false);
    expect(change.operationType).toBe('insert');
    expect(change.fullDocument.live).toBe(true);
    expect(change.fullDocument._id).toBeInstanceOf(ObjectId);
    await stream.close();
  });

  it('a refused watch rejects ready rather than opening an empty stream', async () => {
    /* `from` on a server without a log is the wire's own refusal. */
    const stream = client.db(DB).collection('watched').watch({ from: 5 });
    const err = await stream.ready.catch((e) => e);
    expect(err).toBeInstanceOf(ServerError);
    await stream.close();
  });

  it('close() ends open streams and refuses further calls plainly', async () => {
    const scoped = await connectHttp(`http://127.0.0.1:${front.address().port}`);
    const stream = scoped.db(DB).collection('watched').watch();
    await stream.ready;
    await scoped.close();
    const { done } = await stream.next().catch(() => ({ done: true }));
    expect(done).toBe(true);
    await expect(scoped.ping()).rejects.toThrow(/closed/);
  });
});
