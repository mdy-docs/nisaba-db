# Next step: HTTP in front, in Node

A work brief. It implements Decision B of
[`../deployment-shapes.md`](../deployment-shapes.md): clients reach the
database over HTTP, through a Node process, **not** through the C server.

## Where this sits

`nisaba-server` speaks binjson frames over a socket and thirty-one
operations. `src/db-server-client.js` speaks the other end of that wire
with **no engine in the process** — no WASM, no `ready()`, one file, the
pure-JS codec. `bin/db.js` is a CLI over it.

What nobody can do is `curl` the database.

The decision, already made and not up for revisiting here: HTTP goes in
a Node process in front, over `db-server-client.js`, rather than in
`server/main.c`. The subset written for the C server is preserved on the
branch `wip/http-transport` as a record of what that path costs — chiefly
session identity, since HTTP has no stable connection for a cursor or a
change stream to belong to. A Node front end has the same problem and a
much easier place to solve it: it holds one real socket per session and
can keep it.

The costs, stated plainly and accepted: a hop, a process, and Node back
in a deployment the C server had removed it from.

## Goal

One HTTP endpoint in front of one server, or of a cluster.

**Done when** `curl` can run every operation the wire has against a
single `nisaba-server`, and against a CLUSTER of them, with writes
reaching the leader without the client knowing which member that is.
The cluster exists now (`--raft`, `--raft-port`, `--peer`; see
[`../db-server.md`](../db-server.md)), and a follower's refusal already
carries the leader's id and address, so the redirect is real rather than
anticipated.

## What it is, concretely

```
   browser / REST client
          │  HTTP
          ▼
   ┌─ node http front ─────────────┐   this brief
   │  src/db-server-client.js      │   no WASM, no ready()
   └──────────┬────────────────────┘
              │  binjson frames over TCP
              ▼
   nisaba-server (C), one or many         unchanged
```

The front end holds sockets and maps requests onto them. It does not
hold a database, and it must never grow one — the moment it opens a file
it has become a second implementation of the thing behind it.

## The three problems worth thinking about

**1. Sessions.** A cursor and a change stream belong to a CONNECTION
(`db_session.h`: `client` is an opaque token, and `dbs_drop_client`
releases what a departed client held). HTTP has no connection. So the
front end owns the mapping: a session id in the request, a real socket
per session, and a reaper for sessions nobody comes back to — which the
server already has an opinion about, in `--idle-timeout`. Do not invent a
second timeout policy; read the server's.

**2. Streaming.** A change stream is a server push. Server-sent events
are the obvious fit and `src/raft-monitor.js` already serves an SSE
`/events` stream in this repo, so the shape exists to copy. A paged
cursor is not a stream and should not become one: it is `next` on a
handle, and the wire already has that.

**3. Which member takes a write.** Only the leader accepts one, and a
follower refuses with the leader's id AND address — the node knows both
(`rn_adopted` carries records) and already answers a join that way. So
the front end follows the redirect and keeps a hint, the way
`joinGroup` does. **Do not build a leader election, a health check or a
proxy table**: the answer is in the refusal, and a second opinion about
who leads is the bug that costs you a split brain.

Reads are different and the policy is not decided
(`read-semantics-and-change-streams.md`). Until it is, serve them from
whichever member the request landed on and SAY SO in the response — a
follower's read is stale by its replication lag, which is a fact a caller
may be happy with and must never be surprised by.

## Do not build these — they exist

| Piece | Where |
| --- | --- |
| Every one of the thirty-one ops, client side | `src/db-server-client.js` (`WIRE_OPS`) |
| Framing, reconnection, error shapes | same file |
| An SSE endpoint in this repo's idiom | `src/raft-monitor.js` |
| The leader's address, in a refusal | the node; the front end only reads it |
| Address parsing | `parseAddress` (`src/db-server-client.js`) |

## Shape

`node:http` and nothing else. This repo's transports have zero
dependencies (`src/raft-transport-tcp.js` says why), and an HTTP front
end that pulls in a framework has made the deployment heavier than the
database.

The URL grammar is a real decision and should be made once, in writing,
before any of it is implemented. Two defensible answers:

- **RPC-shaped**: `POST /db/:name/:collection/:op` with a binjson or JSON
  body. Maps one-to-one onto the wire, so nothing has to be translated
  and nothing can drift.
- **REST-shaped**: `GET/POST/PATCH/DELETE /db/:name/:collection/:id`.
  Friendlier from `curl`, but it is a second grammar, and every operation
  the wire grows has to be given a URL by hand.

Say which and why. The first is smaller and cannot drift; the second is
what "REST client" in the deployment doc suggests a reader expects. They
are not mutually exclusive — the second can be a thin layer over the
first — but doing both at once is how neither gets finished.

## Invariants that must hold

- **No engine in this process.** No WASM, no `ready()`, no file handles.
  If it needs one, the design is wrong.
- **One owner per fact.** Who leads, what an op is called, what an error
  means, how long an idle session lives — all of these are already
  decided somewhere. Read them.
- **A refusal is a response.** The wire's shape is
  `{ ok: false, code, msg }` with `dc_strerror` text; the HTTP status
  should carry the class and the body the code. A caller that has to
  parse prose has been given nothing.
- **Nothing is dropped in silence.** A session reaped, a stream that
  overflowed, a write refused because leadership moved — each is a
  distinct, visible answer. `ChangeStreamOverflowError` already exists
  for one of them.
- **Falsify both ways.**

## Ordering

Independent of everything else. An HTTP front end over ONE server is
worth having on its own, and the leader-following half is a small
addition on top — build it in that order even though the cluster now
exists, because one server is the case that has to keep working. The
redirect is testable either way: `src/db-server-client.js` puts
`leaderId` and the leader's `leader` record on the thrown `ServerError`,
and `test/db.server.test.js` already asserts both against three real
processes.

## Verification

```
npx vitest run
```

Beside `test/db.server.test.js`, which already spawns the binary on all
three targets: start a server, start the front end, drive every op over
HTTP, and assert the answers match the ones `db-server-client.js` gets
directly. That comparison is the whole test — the front end's job is to
change the transport and nothing else.

## Out of scope

Authentication, TLS, rate limiting, multi-tenancy. Those are a gateway's
job and this repo has said so consistently; a front end that grows them
becomes the thing every deployment has to adopt whole. Tenants in
particular are a layer on top of the database, not a feature of it.
