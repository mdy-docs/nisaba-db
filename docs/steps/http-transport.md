# Next step: HTTP as the transport — an envelope, not a protocol

A work brief. The database server speaks binjson frames over a raw
socket. This makes it speak them over HTTP instead, so that a browser can
be a client of it, and so that everything infrastructure already knows
how to do — TLS, auth headers, firewalls, request logs, `curl` — works
without a translating gateway in the middle.

## Where this sits

`docs/steps/README.md` records the decision this follows from: the
browser is **both** a host and a client. The client half needs nothing
from C — `src/db-server-client.js` is already the shape — except that a
browser cannot open a TCP socket at all. So the REST client that
decision authorises needs an HTTP server on the other end, and the
choice is whether the database speaks it or a gateway translates to it.

A translating gateway would be a second thing that knows the wire: every
op, every refusal code, the cursor and stream ownership rules. That is
the duplication this project keeps refusing.

## What does not change

**The protocol.** `dbs_handle(dbs*, client, req, req_len, dbuf *out)` —
one binjson object in, one out — is untouched. All thirty-one ops, the
`{ok:false, code, msg}` refusal shape, cursors, change streams, the
grammar in `wasm/src/db_request.c`: none of it knows what carries it.
That is the whole reason the transport was built as a thing that never
reads a field.

**The body stays binjson.** HTTP is the envelope. `Content-Type:
application/octet-stream`, `encode(request)` in, `encode(response)` out —
which is exactly what `src/raft-transport-http.js` already does for the
peer transport, and why that one was a sibling of the TCP transport
rather than a rewrite of it.

**Refusals stay in the body, at HTTP 200.** `{ok:false, code:-49, msg:
"Cannot compact a collection while a cursor is scanning it..."}` is the
answer to the question. Mapping thirty-odd `DC_ERR_*` codes onto HTTP
statuses would put a second, lossier opinion about what went wrong next
to the one `dc_strerror` already owns. HTTP status is for failures of
the ENVELOPE only:

| status | when |
| --- | --- |
| 400 | the request did not parse as HTTP, or its body did not parse as binjson |
| 404 | a path this server does not serve |
| 405 | a method it does not accept |
| 413 | a body past the frame bound |
| 503 | no session slots (today's `-44`, which also keeps its body form) |

**One path, not one path per op.** `POST /` with `op` in the body. The op
names have exactly one owner (`OP_NAMES`); spelling them in URLs would
make a second, and a REST-shaped `/collections/users/count` would make a
third out of the collection name.

## What changes, and what it costs

### 1. An HTTP/1.1 subset, in C

There is no HTTP anywhere in this repository's C today. This adds it:
request line, headers, `Content-Length`, keep-alive, one response
writer. POST-only, no chunked request bodies, no pipelining — the server
already answers one request per connection at a time, which is exactly
HTTP/1.1 keep-alive's shape.

**It goes in its own module** (`server/http.c` / `server/http.h`),
knowing nothing about nisaba, so it can be lifted into a library later
without unpicking it. Parse from a buffer, write to a buffer, no
sockets: the same discipline that lets `dbs_handle` be tested with no
port.

**Not `wasi:http`.** `docs/db-server.md` records the measurement:
`wasmtime serve` instantiates the component once per request, which for
a database means no open collections, no page cache, a fresh open on
every call, and two writers on the same B+ tree files the moment two
requests overlap. A long-lived process with ordinary sockets has none of
those problems, and the HTTP it needs is small.

### 2. Client identity becomes explicit — the part that needs thought

Cursors and change streams are owned by a CLIENT, and today that token
is the connection: one socket, one id, and everything it holds dies with
it. HTTP breaks that. With keep-alive pooling two requests from one
caller can land on different sockets, and a proxy in between makes it
worse.

`wasm/include/db_session.h` anticipated this — "an opaque token
identifying whoever is asking — a connection id, for a socket server;
anything unique and stable, for anyone else" — so the engine needs no
change. What needs designing is the session:

- **Where the token comes from.** A header (`X-Nisaba-Session`) minted by
  the client, or issued by the server on first contact? Client-minted is
  simpler and needs no round trip; server-issued is the only way to stop
  one client from naming another's session and taking its cursors. Note
  what is actually at risk: a guessed token reads another client's
  scan, not the database — but it is still a client seeing something
  that was not addressed to it. **Decide it, and write down why.**
- **When a session ends.** Today `--idle-timeout` closes a quiet
  connection and `dbs_drop_client` releases what it held. A session has
  no socket to close, so it needs its own expiry, and `--max-clients`
  becomes a bound on sessions rather than on connections. The table is
  the same shape as the cursor and stream tables: fixed, refused when
  full, and it says so.
- **Requests with no session.** The overwhelming majority — a `count`,
  an `insert` — hold nothing afterwards and should cost nothing. A
  request with no session header should work, and simply be unable to
  open a cursor or a stream.

### 3. Change streams need a push channel — SSE

HTTP request/response has no unsolicited frame, which is the one thing
change streams need. `GET /watch?coll=notes` as `text/event-stream`, with
the **snapshot-first contract** `src/raft-monitor.js` already
established for `/events`: the first event says what you are watching,
then live events follow.

This is arguably cleaner than what the socket wire does today, where an
answer and an event share a connection and are told apart by shape
(`ok` versus `stream`). Over HTTP they arrive on different requests, so
the discriminator becomes unnecessary — but the SERVER's side of it does
not change: `dbs_stream_take` already builds the frame, the transport
already must not sleep while one is owed, and the per-stream bound and
its overflow are exactly as they are now.

The stream's owner is the session, not the SSE connection — otherwise a
dropped stream connection would silently orphan a bounded slot.

### 4. `--stdio` stays as it is

It earns its place as the wasip1 proof: preview1 has no `socket()` at
all, so a target that compiles proves the transport does not depend on
sockets. HTTP over a pipe is legal and pointless. So `--stdio` keeps
speaking raw frames, and the result is two envelopes over one protocol
— a small, honest wart, and the alternative is losing the proof.

### The overhead, stated plainly

~150–200 bytes of headers per request. On `{op:'count', coll:'users'}`
— about thirty bytes — that is a sixfold inflation; on a find returning
documents it is noise. No extra round trip, with keep-alive. What it
buys is a browser client, and a deployment story that does not begin
with "first, write a gateway".

## Goal

**Done when** `bin/db.js --server http://127.0.0.1:8097` drives the
server through HTTP for every command it drives today, including `watch`;
a browser client can do the same; and `curl -X POST --data-binary` with a
binjson body gets an answer.

Milestones, each landing green:

1. `server/http.c` — the subset, tested over buffers with no socket, in
   `test/native/main.c` like everything else.
2. The listener speaks HTTP: request in, `dbs_handle`, response out.
   Sessionless — cursors and streams refused with a sentence saying why.
3. Sessions: the table, the expiry, `--max-clients` as a session bound.
   Cursors and streams work.
4. SSE for `watch`, and the client's `RemoteChangeStream` fed from it.
5. `src/db-server-client.js` learns an `http://` address; `bin/db.js`
   passes one through. The raw-socket path stays for `--stdio` and for
   anyone who wants it.

## What must still be true afterwards

- `dbs_handle` still never learns what carried its request, and the
  native protocol tests still run with no socket and no port.
- The refusal shape is still `{ok:false, code, msg}`, still with
  `dc_strerror`'s text, whatever the status line says.
- A change stream is still bounded, still says when it overflowed, and
  still dies with its owner — now a session rather than a socket.
- `./wasm/build-server.sh --wasip1` still compiles. If HTTP has crept
  into anything that target cannot build, the transport has stopped
  being separable from the server.
