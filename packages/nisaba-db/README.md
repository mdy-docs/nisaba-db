# nisaba-db

A WASM/C, MongoDB-driver-shaped **embedded document database** for Node
and the browser: CRUD, query operators, `$regex`, secondary/geo/text
indexes, aggregation, change streams — the driver surface you already
know, with the engine compiled to WebAssembly and your data in files you
own.

**One build serves both platforms.** The wasm binary and its loader
detect their environment (Node, browser main thread, worker) themselves;
there are no separate node/browser artifacts. The only thing that
differs per platform is which storage provider you hand to `connect`.

## Node

```js
import { ready } from '@mdy-docs/nisaba-db/wasm';
import { connect, NodeFSStorageProvider } from '@mdy-docs/nisaba-db/node';

await ready();                       // loads the wasm engine, once per process
const db = await connect(new NodeFSStorageProvider('./data'));
const users = await db.collection('users');
await users.insertOne({ name: 'Ada', team: 'core' });
const ada = await users.findOne({ name: 'Ada' });
await db.close();
```

`NodeFSStorageProvider` stores each collection in ordinary files under
the directory you name, holds an advisory lock so two processes cannot
open the same database, and its `flush()` is a real `fsync`.

## Browser

```js
import { ready } from '@mdy-docs/nisaba-db/wasm';
import { connect, OPFSStorageProvider, MemoryStorageProvider } from '@mdy-docs/nisaba-db';

await ready();
const db = await connect(new OPFSStorageProvider());   // origin-private file system
// or: await connect(new MemoryStorageProvider());     // ephemeral, e.g. tests
```

OPFS sync access handles are exclusive per file, so one context owns the
database's files at a time. For **multiple tabs**, each tab runs a
worker, and `connectShared` elects exactly one of those workers the
owner — the rest proxy to it transparently, and the election re-runs
when the owning tab closes:

```js
// inside each tab's worker
import { connectShared } from '@mdy-docs/nisaba-db/coordinator';
import { OPFSStorageProvider } from '@mdy-docs/nisaba-db';

const db = await connectShared('my-app', new OPFSStorageProvider());
// same Db shape as connect(): collections, queries, watch(), ...
```

## Everything else

Queries, updates, indexes, cursors, aggregation, change streams,
`watch()`, TTL, compaction — the API is the MongoDB driver shape
throughout; see the repository's `docs/db-api.md` for the full surface
and its deliberate differences.

The replicated server, its client, and the HTTP front end are not in
this package — this is the database you embed. They live in the
repository package.

## Building this package

Every shipped file is copied from the repository at pack time
(`prepack` runs `../nisaba-db.build.mjs` — the assembly script lives
beside this directory, not in it, so the package holds only what
ships), and the package cannot drift from what the repository builds
and tests. From a repo checkout:

```sh
cd packages/nisaba-db
npm run build      # assembles src/, build/, types/, third_party/
npm test           # a smoke round-trip over both providers
npm pack           # the publishable tarball
```

## License

BSD-2-Clause.
