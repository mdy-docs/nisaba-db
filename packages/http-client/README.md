# nisaba-client-http

The **browser-capable** JavaScript client for the nisaba database's
HTTP front end
([`@mdy-docs/nisaba-http-front`](https://www.npmjs.com/package/@mdy-docs/nisaba-http-front)):
the MongoDB-driver-shaped API you already know, spelled against
`fetch`. It is the one client in the family a web page can import; it
runs in Node (≥18, global fetch) identically, because nothing in it is
platform-shaped.

**No engine, and no socket either.** Pure JS, no WASM, no `node:`
modules — the front end holds the real sockets; this side holds URLs.
For server-side JavaScript that can reach the server's TCP port
directly, skip the hop:
[`@mdy-docs/nisaba-client-js`](https://www.npmjs.com/package/@mdy-docs/nisaba-client-js)
is the same surface over the wire itself. The two clients are a
deliberate mirror: code written against one connects with the other by
swapping `connectServer` for `connectHttp`.

## Quick start

```js
import { connectHttp } from '@mdy-docs/nisaba-client-http';

const client = await connectHttp('http://127.0.0.1:8080');
const users = client.db('app').collection('users');

await users.insertOne({ name: 'Ada', team: 'core' });
const ada = await users.findOne({ name: 'Ada' });
for await (const doc of users.find({ team: 'core' }, { batchSize: 100 })) {
  // a server-held cursor, paged through a front-end session underneath
}

await client.close();
```

What crosses the wire is JSON in MongoDB's Extended spelling
(`{"$oid"}`, `{"$date"}`, `{"$binary"}`), converted at the boundary —
the caller sees real `ObjectId`, `Date` and `Uint8Array` values, the
same values the TCP client hands over. Ids are minted client-side, as
in every client in this family. A server refusal is a `ServerError`
with the server's own `code`, plus the HTTP `status` it rode in under.

## Change streams

```js
const stream = users.watch();
await stream.ready;               // a refused watch rejects here, plainly
for await (const change of stream) {
  // pushed by the front end as Server-Sent Events, parsed for you
}
```

Streams are read with `fetch`, not `EventSource` — deliberately:
`EventSource` cannot say "the watch was refused", and the refusal is
the part a caller must see. On a replicated server every event carries
its log index; `stream.resumeFrom` keeps the last one handed over, and
`watch({ from })` resumes right after it, missing nothing.

## Building this package

Every shipped file is copied from the repository at pack time
(`prepack` runs `../http-client.build.mjs` — the assembly
script lives beside this directory, not in it), so the package cannot
drift from what the repository builds and tests. From a repo checkout:

```sh
cd packages/http-client
npm run build      # assembles src/, types/, third_party/
npm test           # a smoke test; a live round-trip if the server is built
npm pack           # the publishable tarball
```

## License

BSD-2-Clause.
