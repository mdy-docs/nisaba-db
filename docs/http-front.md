# The HTTP front end

`curl`'s way into a nisaba-server, and into a cluster of them. A Node
process (`src/db-http-front.js`, run as `bin/http-front.js`) that speaks
HTTP on one side and binjson frames over TCP — `src/db-server-client.js`,
the server's own client — on the other. It implements Decision B of
[`deployment-shapes.md`](deployment-shapes.md): HTTP lives in front of
the C server, not inside it.

**No engine in the process.** No WASM, no `ready()`, no file handles.
The front end holds sockets and maps requests onto them; everything it
answers is the server's answer. The costs, stated plainly and accepted:
a hop, a process, and Node back in a deployment the C server had removed
it from.

```
   browser / REST client
          │  HTTP
          ▼
   ┌─ node http front ─────────────┐   bin/http-front.js
   │  src/db-server-client.js      │   no WASM, no ready()
   └──────────┬────────────────────┘
              │  binjson frames over TCP
              ▼
   nisaba-server (C), one or many       unchanged
```

## Running it

```sh
db-http --listen 8080 --server 127.0.0.1:8097

# a cluster: one --server flag per member's CLIENT address
db-http --listen 8080 \
        --server 127.0.0.1:9001 --server 127.0.0.1:9002 --server 127.0.0.1:9003
```

`--listen` defaults its host to loopback, deliberately; widen it
consciously. Every member should be listed: an unlisted member that
becomes leader is one this process cannot find.

## The URL grammar — RPC-shaped, decided once

```
POST /<op>                     instance ops        POST /ping, /listDatabases
POST /db/<db>/<op>             database ops        POST /db/shop/listCollections
POST /db/<db>/<coll>/<op>      collection ops      POST /db/shop/orders/find
GET  /db/<db>/<coll>/watch     a change stream, as Server-Sent Events
POST /session                  open a session; DELETE /session/<id> closes it
```

The path contributes `op`, `db` and `coll` to the wire request
([`db-server.md`](db-server.md) documents all of them) and the JSON body
is the rest of it, verbatim. That is the whole translation, and it is
why this grammar was chosen over a REST-shaped
`GET/POST/PATCH/DELETE /db/:name/:coll/:id` one: it maps one-to-one onto
the wire, so nothing can drift, and an op the wire grows is usable over
HTTP the day it lands in C — the front end does not check op names; the
server owns that list and refuses an unknown one with `-41`. A
REST-shaped grammar is a second grammar whose every URL is invented by
hand; it can be layered over this one later without unbuilding anything,
and until someone wants it, it is not built.

Two rules follow from "the URL owns `op`, `db`, `coll`":

- A body that restates any of them is refused (400) — two copies of one
  fact is how they disagree.
- Ops that name a collection always take the four-segment form, even the
  ones the JS API puts on `Db`: `POST /db/shop/orders/createCollection`,
  not `POST /db/shop/createCollection` with `coll` in the body.

```sh
curl -s localhost:8080/db/shop/orders/insert \
     -d '{"doc": {"sku": "A-17", "qty": 2}}'
# {"ok":true,"result":{...},"minted":{"id":{"$oid":"66..."}}}

curl -s localhost:8080/db/shop/orders/find \
     -d '{"filter": {"qty": {"$gt": 1}}, "opts": {"sort": {"sku": 1}}}'
```

## JSON, in MongoDB's Extended spelling

Bodies and responses are JSON. The wire values JSON cannot say cross as
Extended JSON literals, in both directions:

| literal | value |
| --- | --- |
| `{"$oid": "<24 hex>"}` | ObjectId |
| `{"$date": "<ISO 8601>"}` | Date |
| `{"$binary": {"base64": "…", "subType": "00"}}` | binary |

The convention has one owner — `src/extended-json.js`, shared with
`bin/db.js` — so the CLI and HTTP cannot disagree about what a literal
means.

## What the front end fills in: `id` and `now`

On this wire two facts are the *client's* to provide, and for an HTTP
caller this process is the client. The engine will not invent a document
identity, because that needs a clock and the engine keeps clocks out
deliberately — so an insert whose document has no `_id`, and an upsert
with no `id`, get an ObjectId minted here. Ops that resolve
`$currentDate` or sweep TTLs get `now` stamped here. Both are fallbacks,
never overrides: name your own `_id` or send your own `now` and nothing
is added.

Whatever was minted is echoed under `minted` in the response —
`minted.id` for a single identity, `minted.ids` keyed by position for a
list — because a caller that was given an identity has to be told which.
That echo is the only thing the front end ever adds to a response.

## Refusals: the wire's code, under an HTTP status

A refusal's body is the wire's refusal object verbatim —
`{ok:false, code, msg}`, plus `index` when a list member is named and
`leaderId`/`leader` when the server says who leads. The status carries
the class:

| status | meaning |
| --- | --- |
| 400 | the request is wrong (the code says how; `dc_strerror` text in `msg`) |
| 503 | true right now, ask again: `-63` not-the-leader, `-64` write-lost-to-an-election, `-66` leader-without-quorum-proof, `-44` no free slot |
| 502 | the member went away mid-request — for a write this means UNKNOWN, not failed, and the front end never retries one on your behalf |
| 404 | the URL grammar or a session id; never about data |

## Sessions: cursors need a connection, HTTP has none

A paged cursor is state on one server connection (`db_session.h`); a
change stream likewise. HTTP requests share pooled connections, so a
`find` with `batchSize` on the shared pool would open a cursor that the
next request's connection has never heard of. Hence sessions:

```sh
S=$(curl -s -X POST localhost:8080/session | jq -r .session)
curl -s "localhost:8080/db/shop/orders/find?session=$S" \
     -d '{"filter": {}, "opts": {"batchSize": 100}}'
# {"ok":true,"docs":[...],"cursor":3}
curl -s "localhost:8080/db/shop/orders/getMore?session=$S" -d '{"cursor": 3}'
curl -s -X DELETE localhost:8080/session/$S
```

A session is one real socket, opened on the leader. Its reaper is the
**server's**: session sockets send no keep-alive pings, so a session
nobody comes back to is closed by the server's own `--idle-timeout`, and
the front end learns from the socket closing — one timeout policy, owned
where it always was. A dead or unknown session answers 404; open another.

A session is pinned to the member it opened on, because the cursor state
lives there and nowhere else. If leadership moves, its next read is
refused (503, leader named) rather than redirected to a member that has
never heard of the cursor; the remedy is a new session.

## Change streams: Server-Sent Events, resumable by the protocol's own machinery

```sh
curl -N localhost:8080/db/shop/orders/watch
# id: 12
# event: watching
# data: {"db":"shop","coll":"orders","member":"127.0.0.1:9001","index":12}
#
# id: 13
# event: change
# data: {"operationType":"insert","ns":{...},"index":13,"fullDocument":{...}}
```

On a replicated server every event's **log index rides as its SSE
`id:`** — the resume token. `?from=N` (or a `Last-Event-ID` header,
which wins, because a reconnecting consumer knows better than its
original URL where it got to) replays everything after `N` before
anything live: exactly once, in order, across the reconnect. A browser
`EventSource` gets this for free — it reconnects with `Last-Event-ID`
by itself. A token that was compacted out of the log is **410 Gone**
(the wire's `-68`): watch afresh and re-read, because a gap is never
bridged in silence.

Each stream holds its own dedicated socket — an SSE connection is its
own session — subscribed before the 200 is committed, so a refused watch
is a JSON refusal under the right status, not an empty stream. Closing
the HTTP request closes the stream and the socket.

Nothing ends in silence: a server-side overflow with a token to resume
from is a page boundary the front end crosses without the consumer ever
seeing it (the server's stream queue is bounded; a long replay pages
through it); an overflow with no token — a server with no log — is
`event: overflow` then the end. `event: end` carries a reason for every
other way it stops, and `: keep-alive` comments defeat idle reapers in
between.

## A cluster: who takes the request

The leader — reads and writes alike, since reads are linearizable and
leader-only; a follower refuses both with `-63`. The front end keeps a
*hint*: the address that last behaved like the leader. When a member
refuses with `-63` or cannot be reached, it re-asks the members
themselves — ping every configured address, believe whichever answers
`role: "leader"` about itself — and retries, up to `--retry-ms`
(default 8000, sized for an election rather than an outage). The
refusal's `leader` record names the peer wire, not the client port, so
it is treated as the signal to go look, not as a dial string.

Deliberately **not** built: an election, a health checker, a proxy
table. A second opinion about who leads is the bug that costs a split
brain. `-66` — a leader that cannot prove it still leads — is retried in
place: the member is right, the quorum is catching its breath.

Only refusals are retried. A `-63` is the server declining *before*
doing anything, and a `-64` is the server saying a proposed write was
lost to an election — "did not happen and no replica holds it, so a
retry is safe" (`db_session.h`) — so re-asking is safe for both. A
transport failure mid-request is 502 because the answer is unknown, and
only the caller knows whether its write is safe to repeat.

## Out of scope

Authentication, TLS, rate limiting, tenancy: a gateway's job, as this
repository has said consistently. CORS is open for the same reason —
there is no credential here to protect, and a browser REST client cannot
reach the front end otherwise.

## Verification

`test/db.http-front.test.js`: every operation over HTTP against a single
server (both engines), asserted equal to what `db-server-client.js` gets
directly — the front end's job is to change the transport and nothing
else — plus a three-member cluster where a write reaches the leader, a
killed leader is survived, and a change stream sees a replicated write.
