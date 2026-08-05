# nisaba-client-js

The JavaScript **client** for the nisaba database server: the
MongoDB-driver-shaped API you already know, translated call by call into
binjson frames over a TCP socket. The wire it speaks is documented in
the repository's `docs/db-server.md`; the server on the other end is a
native or wasm32-wasip2 process with no JavaScript in it.

**No engine in this package.** Its only dependency is the pure-JS
binjson codec it ships with — no WASM module, no `ready()`, no storage
provider. That is the whole claim being made: the database is somewhere
else, and a client needs to agree with it about a format, not share an
implementation with it. If you want the database *in* your process,
that is [`@mdy-docs/nisaba-db`](https://www.npmjs.com/package/@mdy-docs/nisaba-db).

## Quick start

```js
import { connectServer } from '@mdy-docs/nisaba-client-js';

const client = await connectServer('127.0.0.1:8097');
const db = client.db('app');
const users = db.collection('users');

await users.insertOne({ name: 'Ada', team: 'core' });
const ada = await users.findOne({ name: 'Ada' });
for await (const doc of users.find({ team: 'core' }, { batchSize: 100 })) {
  // a cursor the server holds, paged underneath the iteration
}

await client.close();
```

`client.db(name)` sends nothing — it is a handle, and the server holds
an instance: one root directory, a subdirectory per database. The
collection surface is the wire's op set exactly — `find`,
`findOne`, `aggregate`, `insertOne`/`insertMany`, `updateOne`/`updateMany`,
`replaceOne`, `deleteOne`/`deleteMany`, `findOneAnd*`, `bulkWrite`,
indexes, `countDocuments`, `distinct`, `explain`, `compact`, TTL's
`pruneExpired` — and asking for anything else gets a sentence saying so
rather than a TypeError. `db.request({ op, ... })` is the escape hatch:
a new op is usable from JavaScript the day it lands in the server's C,
before it has a method here.

## Change streams

```js
const stream = users.watch();
await stream.ready;
for await (const change of stream) {
  // { type, id, doc, ... } — a frame the server pushed
}
```

A stream that falls behind is closed by the server after delivering
everything it had queued; on a replicated server the
`ChangeStreamOverflowError` carries `resumeFrom`, and
`watch({ from })` continues exactly where it got to.

## What the client owns

- **Ids** — every write that might need an `_id` carries one minted
  here, because the engine keeps clocks out deliberately.
- **Keep-alive** — the server reaps a connection that asks nothing for
  `--idle-timeout` seconds, so the client pings on an unref'd timer
  (`keepAliveMs`, `0` to disable — a connect-ask-exit CLI never sends
  one).
- **Refusals as errors** — a server refusal is a `ServerError` with the
  server's own `code` and message, not a hang or a mystery.

## Building this package

Every shipped file is copied from the repository at pack time
(`prepack` runs `../nisaba-client-js.build.mjs` — the assembly script
lives beside this directory, not in it, so the package holds only what
ships), and the package cannot drift from what the repository builds
and tests. From a repo checkout:

```sh
cd packages/nisaba-client-js
npm run build      # assembles src/, types/, third_party/
npm test           # a smoke test; a live round-trip if the server is built
npm pack           # the publishable tarball
```

## License

BSD-2-Clause.
