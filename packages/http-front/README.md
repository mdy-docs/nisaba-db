# nisaba-http-front

`curl`'s way into a nisaba database server, and into a cluster of them:
a Node process that speaks HTTP on one side and binjson frames over TCP
— the server's own client, which ships inside this package — on the
other. It holds sockets, follows the leader, and translates JSON;
everything it answers is the server's answer.

**No engine in this package.** No WASM, no `ready()`, no file handles.
The server on the other end is a native or wasm32-wasip2 process with
no JavaScript in it; if you want the database *in* your process, that
is [`@mdy-docs/nisaba-db`](https://www.npmjs.com/package/@mdy-docs/nisaba-db).

## Running it

```sh
npx db-http --listen 8080 --server 127.0.0.1:8097

# a cluster: one --server flag per member's CLIENT address
npx db-http --listen 8080 \
            --server 127.0.0.1:9001 --server 127.0.0.1:9002 --server 127.0.0.1:9003
```

`--listen` defaults its host to loopback, deliberately; widen it
consciously. With several `--server` flags, requests reach whichever
member currently leads; list every member — an unlisted member that
becomes leader is one this process cannot find.

## The URL grammar — RPC-shaped

```
POST /<op>                     instance ops        POST /ping, /listDatabases
POST /db/<db>/<op>             database ops        POST /db/shop/listCollections
POST /db/<db>/<coll>/<op>      collection ops      POST /db/shop/orders/find
GET  /db/<db>/<coll>/watch     a change stream, as Server-Sent Events
POST /session                  open a session; DELETE /session/<id> closes it
```

The path contributes `op`, `db` and `coll` to the wire request and the
JSON body is the rest of it, verbatim — the front end checks no op
names; the server owns that list, and an op the wire grows is usable
over HTTP the day it lands. Bodies and responses are JSON with the
wire's extra types as MongoDB Extended JSON literals (`{"$oid": …}`,
`{"$date": …}`, `{"$binary": …}`).

```sh
curl -s localhost:8080/db/shop/orders/insert \
     -d '{"doc": {"sku": "A-17", "qty": 2}}'

S=$(curl -s -X POST localhost:8080/session | jq -r .session)
curl -s "localhost:8080/db/shop/orders/find?session=$S" \
     -d '{"filter": {}, "opts": {"batchSize": 100}}'

curl -N localhost:8080/db/shop/orders/watch
```

Sessions exist because a paged cursor is state on one server
connection and HTTP has none; a change stream is Server-Sent Events on
a dedicated socket, resumable on a replicated server by SSE's own
`Last-Event-ID` machinery. The repository's `docs/http-front.md` has
the whole grammar, the refusal statuses, and the cluster behavior.

Embedding instead of running: the package exports `DbHttpFront`, the
class `db-http` wraps — construct it with the member addresses and
`start()` it inside your own process.

For a JavaScript caller there is a real client for this front end —
[`@mdy-docs/nisaba-client-http`](https://www.npmjs.com/package/@mdy-docs/nisaba-client-http),
the driver-shaped surface over fetch, browser-included. For
server-side JavaScript that can reach the TCP port directly, skip the
hop: [`@mdy-docs/nisaba-client-js`](https://www.npmjs.com/package/@mdy-docs/nisaba-client-js).

Authentication, TLS, rate limiting, tenancy: a gateway's job, as the
repository says consistently. CORS is open for the same reason — there
is no credential here to protect.

## Building this package

Every shipped file is copied from the repository at pack time
(`prepack` runs `../http-front.build.mjs` — the assembly script
lives beside this directory, not in it), so the package cannot drift
from what the repository builds and tests. From a repo checkout:

```sh
cd packages/http-front
npm run build      # assembles src/, bin/, types/, third_party/
npm test           # a smoke test; a live round-trip if the server is built
npm pack           # the publishable tarball
```

## License

BSD-2-Clause.
