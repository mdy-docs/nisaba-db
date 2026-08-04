# The database server

A process that holds one directory — an INSTANCE, with a subdirectory per
database — and answers binjson frames over a socket, with no JavaScript in
it at all. It is the same C the browser and Node builds link:
`server/main.c` adds a `main()` and a transport, and nothing else.

The deployment target is **`wasm32-wasip2`**, run by `wasmtime run`. The
native build exists so the same code runs wherever a `cc` does, and the
`wasm32-wasip1` build exists because it proves the transport does not
depend on sockets (preview1 has none).

## Building and running

```sh
./wasm/build-server.sh              # wasm32-wasip2  (sockets + --stdio)
./wasm/build-server.sh --native     # a native binary, same sources
./wasm/build-server.sh --wasip1     # wasm32-wasip1, --stdio only
```

The ROOT directory is the working directory (native) or the preopen
mapped to `.` (WASI). One process per root, for its whole lifetime:

```sh
wasmtime run -S inherit-network --dir ~/.nisaba::. \
  wasm/lib/nisaba-server-wasip2.wasm --port 8097

cd ~/.nisaba && nisaba-server --port 8097          # or natively
cd /tmp/brand-new && nisaba-server --port 8097     # an empty directory is an empty instance
nisaba-server --stdio                              # frames on stdin/stdout
```

**There is deliberately no `--dir` flag.** Under WASI the host grants
exactly one directory and names it `.` — so a flag here would be a
second place the same fact is decided, on the same command line as the
host's own. Point the host at the directory, or `cd` into it.

```
~/.nisaba/                 the root -- one process owns it
├── analytics/             a database: its own catalog and files
│   ├── __catalog__.bj
│   └── coll-events.bj
├── billing/
│   └── ...
├── __wal__.bj             the log, with --raft ... until the first snapshot
├── __snap__-3-f0.bj       ...after which: the newest generation's files,
├── __snap__-3.manifest.bj    its manifest (the commit point),
└── __snap__-log-3.bj         and the compacted log, based at its boundary
```

**A directory that is itself a database is refused**, with the sentence
saying to move its files into a subdirectory named for the database and
serve the parent. A server that quietly reinterpreted one as a root would
open an instance with no databases in it, standing on top of somebody's
data and reporting nothing wrong.

| Flag | |
| --- | --- |
| `--port N` | TCP listener on loopback (default 8097). Needs sockets: wasip2 or native |
| `--stdio` | frames on stdin/stdout. Every target, including wasip1 |
| `--order N` | B+ tree order for files this process **creates** (default 32, min 3) |
| `--max-clients N` | connections held at once (default and ceiling 64) |
| `--idle-timeout N` | seconds of silence before a connection's slot is taken back (default 60; 0 disables) |
| `--raft ID` | this process is a cluster member with that node id |
| `--raft-port N` | where the other members reach this one (loopback) |
| `--peer ID@HOST:PORT` | another member and where to reach IT; repeat once per member |
| `--join HOST:PORT` | ask a RUNNING cluster to admit this node, knowing only a seed address; repeat for more seeds. Use **instead of** `--peer` |
| `--snapshot-entries N` | applied entries between local snapshots, which bound the log (default 8192; 0 never compacts) |
| `--leave ID` | ask that cluster to remove member `ID`, then exit without serving; needs `--join` to say who to ask |

**`--order` is a creation parameter, not something a reader has to be
told.** A tree records its own order in its metadata and reads it back
when opened, so files made with one order open correctly under any
other — including by a different implementation that was never told
either. What the flag sets is the order of files this process goes on to
CREATE: a new database in an empty directory, and any collection or
index made later in that process's life. An existing database's files
keep whatever they were made with.

So it is worth passing when a directory is first served, and it is safe
to forget afterwards. `bin/db.js --order N` and a host passing `order` to
`connect()` mean the same thing.

## A cluster

```sh
cd node1 && nisaba-server --port 8097 --raft 1 --raft-port 9001 \
                          --peer 2@127.0.0.1:9002 --peer 3@127.0.0.1:9003
cd node2 && nisaba-server --port 8098 --raft 2 --raft-port 9002 \
                          --peer 1@127.0.0.1:9001 --peer 3@127.0.0.1:9003
cd node3 && nisaba-server --port 8099 --raft 3 --raft-port 9003 \
                          --peer 1@127.0.0.1:9001 --peer 2@127.0.0.1:9002
```

Three directories, three processes, no JavaScript in any of them. They
elect a leader, put every write through a log before applying it, and
survive one of them dying — two of three is still a quorum.

**`./examples/cluster.sh` does all of this and prints what it is doing**
— builds, starts three members, finds the leader, inserts and reads,
shows a follower refusing, kills the leader, joins a fourth member with
`--join` and removes it again, then cleans up. Every line in it is a
command you could type.

**One log for the whole instance**, not one per database: one leader,
one election, one member set, one failover story for the executable. A
log entry carries the database its command is for, and that is the whole
of what replication had to learn — `server/replica.c` and
`server/peers.c` do not know a database has a name.

What that costs, accepted deliberately: one database's write rate is
every database's, and a halt in one halts all of them. Per-database
placement is not available and is not planned — it was a tenancy
requirement, and tenancy is a layer above this repository.

**`--peer` bootstraps; after that the LOG is the member set.** Every
member of a cluster started this way has to be given the same list,
because a member missing from one node's list is a vote that node will
never count. That stops mattering the moment the log carries a `CONFIG`
entry of its own: a member restarted with a stale `--peer` list cannot
overwrite what the cluster agreed, and a member that joined needs no
list at all.

## Growing and shrinking one

```sh
cd node4 && nisaba-server --port 8100 --raft 4 --raft-port 9004 \
                          --join 127.0.0.1:9001 --join 127.0.0.1:9002
```

One address, no ids, no member list. It asks; if that seed is not the
leader it is redirected to the one that is, and the reply comes back
when the leader's `CONFIG` entry has committed and applied — which is
also the moment this node becomes a member. Give more than one seed and
a seed that is down costs one dial rather than the whole attempt.

**It enters as a LEARNER**, whatever it asked for: it replicates and
applies, it does not vote, and it counts for nothing in the quorum until
the leader's own bookkeeping proves its log has caught up. That is not
politeness. A cluster of three that admitted a fourth as a voter
immediately would need a quorum of three, one of them a member with an
empty log that cannot help. Promotion is automatic and is the node's
decision, on match index; nothing on the host side has an opinion about
when.

**A restart needs no `--join` and no `--peer`** — `nisaba-server --raft 4
--raft-port 9004` is enough, because the member's own log says who its
cluster is. Passing `--join` again is harmless: a re-join naming an
identical record is answered with the current set and changes nothing,
which is what makes a retried join safe.

**`--peer` and `--join` together are refused**, rather than resolved by
whichever was typed first. They are two ways to learn the same thing.

```sh
nisaba-server --leave 4 --join 127.0.0.1:9001
```

Removes a member and exits. It is an ordinary client of the cluster —
it needs no directory and serves nothing — so it works equally for a
node decommissioning itself and for an administrator removing one that
is never coming back. The survivors' quorum arithmetic changes with it:
three voters need two, and two voters still need two.

**One change at a time.** A join or leave while another is in flight is
told to ask again, and does. That is a safety rule rather than a
policy — a single-server change is safe precisely because it commits
under the old quorum, which requires that the next one cannot start
until this one has.

**Seeds must be DIRECT addresses.** A load balancer in front of a member
breaks node identity: the answer to a join comes from the node that
parked it, and a redirect names a member rather than a service.

**Only the leader takes a write.** Any other member refuses it with
`code: -63`, its `leaderId`, and — when it knows one — a `leader` record
carrying that member's address. It does not forward: a server holding a
request it cannot promise anything about is worse than a refusal a
client can act on. `src/db-server-client.js` puts both on the thrown
`ServerError`, so following the redirect is one line.

**Only the leader serves a READ, too**, and it serves one only after a
quorum has confirmed it still leads. Every read is linearizable: it sees
everything committed before it was asked, and a client always reads its
own writes without having to think about which member took them.

A follower refuses a read with the same `-63` and the same address it
refuses a write with, because it is behind by at least a round trip and
cannot tell by how much — answering would present staleness as
authority. A leader whose quorum has gone quiet refuses with `-66`,
which is a different problem and a different answer: not "ask somebody
else" but "there may be nobody to ask". Raft does not depose such a
leader, so without the check it would go on answering from a log a newer
leader may already have moved past.

What that costs: a read on a leader waits for one heartbeat round to a
quorum, concurrent reads share it, and `getMore` pays it per batch. A
server without `--raft`, and a `--raft` group of one, pay nothing —
there is nobody to hear from.

**`ping` is what a follower still answers**, and on a replica it says
what the member is: `{ pong, role, leaderId, applied, commit, base,
last }`. It is
how you watch a cluster replicate now that followers do not serve data.
Those numbers report and promise nothing — `applied` is that member's
own floor when it was asked, which is precisely the number that may not
be used to serve a read.

Reads that scale by adding members are a different thing: an
asynchronous replica tier outside consensus, explicitly not
linearizable, deferred until a workload wants it
([`replicaton-roadmap.md`](replicaton-roadmap.md)'s step 6 decision
records why and what was rejected).

**Without `--raft` nothing changes**, including the file layout: a
directory served by a single process today can be joined to a cluster
tomorrow rather than re-created.

**`--stdio` cannot join a cluster** and says so rather than half-working
— there is no poll loop there to serve peers with, and `wasm32-wasip1`
has no `socket()` at all.

The peer wire is `src/raft-transport-tcp.js`'s: a 4-byte little-endian
length, then a binjson `{ t: "req"|"res", id, env|value|error }`, with
`id` the sender's correlation id echoed back. The client port and the
peer port are separate listeners and separate grammars.

`env` (and a reply's `value`) carries the message as **opaque bytes** —
binjson BINARY, not the message spliced in as a nested object. That is
what the other end does: `src/raft.js` hands its transport an
already-encoded message, and the receiving host passes it to
`handleMessage(bytes)` without ever decoding it.

**A member running in Node can sit in this cluster**, and
`test/db.server.test.js` proves it: two C members and one `RaftNode` over
`src/raft-transport-tcp.js`, electing, replicating, and — with one C
member stopped — unable to commit anything without the Node member's
vote and acks. It can JOIN one too, with `joinGroup(transport, null, …)`
from `src/raft-host.js`; the `null` is the group id, because a native
member hosts one group and wraps no `{ group, msg }` envelope around
anything.

What that does and does not show is worth stating. The Node member's
`RaftNode` wraps `RaftCore`, which is the SAME C `raft_node` compiled to
WASM, so this is not two implementations of Raft agreeing; it is one
implementation with two hosts around it, and the only thing that differs
is the transport. Which is exactly where they HAD drifted: C spliced the
message into `env` as a nested object, C-to-C agreed with itself, and no
C-only test could ever have noticed.

## Databases

One connection reaches all of them, and switches between them freely:

```js
const client = await connectServer('127.0.0.1:8097');
const analytics = client.db('analytics');
const billing   = client.db('billing');

await analytics.collection('events').insertOne({ n: 1 });
await billing.collection('invoices').insertOne({ n: 2 });   // same socket
```

`client.db(name)` **sends nothing** — it is a handle, exactly as
`Client.db(name)` is in process (`docs/db-api.md`). What makes that
possible is that the CONNECTION is not stateful about which database:
every request carries a `db` field, so two handles can be held at once
and interleaved. There is no "use" op and there will not be one — a
request whose meaning depends on an earlier request could not be
interleaved with another database's at all.

| Op | |
| --- | --- |
| `{op:'listDatabases'}` | `{ok:true, databases:[...]}` — the subdirectories of the root |
| `{op:'dropDatabase', db}` | `{ok:true, dropped}` — the directory and everything in it; `false` if there was none |

**`dropDatabase` is replicated.** On a `--raft` member it travels the
log as an instance-level entry (`{d, i:'drop'}` — an act *about* a
database, where the ordinary envelope `{d, c}` carries a command *for*
one), so every member removes its own directory at apply, a member that
was down learns the drop from the log it is caught up with, and the
reply is the leader's apply result. A request that was in flight on the
database when the drop committed is answered `code: -71` — its session
was closed by the apply, whatever it wrote is gone with the database
either way, and the refusal is deliberately not retried automatically:
retrying a write would recreate the database the drop just removed.
Change streams that were live on it end with their session; there is no
invalidate event.

**Named is made.** A request naming a database that is not there creates
it, the same way an insert makes a collection: there is nothing to be
gained from a separate act of creation whose only effect is a directory.

**A request naming NO database is refused** (`code: -42`) rather than
falling back to a default — a write landing somewhere nobody named is
worse than a refusal a client can act on. `ping` is the exception, and
the reason it is one: it touches nothing, so a connection can stay warm
before it has opened anything.

**`listDatabases` reports the directories**, not a set proven to hold a
catalog. Proving it would mean opening each one, and a listing that opens
every database it names is a listing nobody can afford to call. Files in
the root — the log, a snapshot generation — are never listed as
databases.

**Bounded, and it says so.** Sixteen databases OPEN at once (not a limit
on how many may exist: one nobody has asked for costs a directory), with
`code: -65` when a request would need a seventeenth.

Cursor and change-stream ids are minted from one counter for the whole
process, so an id from one database is never an id in another.

## Clients

`bin/db.js --server <host:port>` drives it with the same commands it uses
locally — see [`bin/db.md`](../bin/db.md).

`@mdy-docs/nisaba-db/server-client` is the JavaScript client
(`src/db-server-client.js`): a socket, the pure-JS binjson codec, and
nothing else. No WASM module, no `ready()`, no storage provider.

```js
import { connectServer } from '@mdy-docs/nisaba-db/server-client';

// Pings every 20s so the server's idle timeout does not take the slot
// back; `{ keepAliveMs: 0 }` turns that off. The timer is unref'd, so a
// script that connects, asks and finishes still exits on its own.
const db = await connectServer('127.0.0.1:8097');
const users = db.collection('users');
await users.insertOne({ name: 'Ada', team: 'core' });   // _id minted here
console.log(await users.find({ team: 'core' }).toArray());

// A large result, paged: one batch per round trip, and the cursor closes
// itself on the last one. `break` mid-scan closes it rather than leaving
// it held.
for await (const doc of users.find({}, { batchSize: 500 })) {
  process.stdout.write(doc.name + '\n');
}

await db.close();
```

## The wire

**Framing is the format's own.** A binjson value carries its total size in
its header, so a reader takes the header, asks `bj_value_size` how long
the value is, and reads the rest. There is no length prefix to disagree
about and no framing version.

A frame that cannot be *measured* ends the connection rather than
producing an error response: a reader that has lost the frame boundary
cannot resynchronise, and answering would be pretending it had. Every
other refusal is a response.

**One request object in, one response object out** — except for change
events, which are the other kind of frame and are described below.
Thirty-one operations: twenty about a collection's documents, five about
its schema, two about a cursor, two about a change stream, and two about
none of those (`listCollections` and `ping`).

| Request | Response |
| --- | --- |
| `{op:'ping'}` | `{ok:true, pong:true}`, and on a `--raft` member also `{role, leaderId, applied, commit}` |
| `{op:'listCollections'}` | `{ok:true, collections:[...]}` |
| `{op:'find', coll, filter, opts:{sort,projection,skip,limit,batchSize}}` | `{ok:true, docs:[...]}`, or with `batchSize`: `{ok:true, docs:[...], cursor}` |
| `{op:'getMore', cursor, opts:{batchSize}}` | `{ok:true, docs:[...], cursor}` |
| `{op:'closeCursor', cursor}` | `{ok:true, closed:true}` |
| `{op:'findOne', coll, filter}` | `{ok:true, found, doc}` |
| `{op:'count', coll, filter}` | `{ok:true, n}` |
| `{op:'distinct', coll, field, filter}` | `{ok:true, values:[...]}` |
| `{op:'aggregate', coll, stages:[...]}` | `{ok:true, docs:[...]}` |
| `{op:'findByIndex', coll, index, values:[...]}` | `{ok:true, docs:[...]}` |
| `{op:'pruneExpired', coll, now}` | `{ok:true, deletedCount}` |
| `{op:'watch', coll, from?}` | `{ok:true, stream, index?}` |
| `{op:'closeStream', stream}` | `{ok:true, closed:true}` |
| `{op:'explain', coll, filter}` | `{ok:true, plan:{source, index}}` |
| `{op:'insert', coll, doc, id}` | `{ok:true, result}` |
| `{op:'insertMany', coll, docs:[...], ordered}` | `{ok:true, result, attempted, upserted, errors}` |
| `{op:'bulkWrite', coll, writes:[...], ordered, now}` | `{ok:true, result, attempted, upserted, errors}` |
| `{op:'update'\|'updateMany', coll, filter, update, upsert, id, now}` | `{ok:true, result}` |
| `{op:'replace', coll, filter, doc, upsert, id}` | `{ok:true, result}` |
| `{op:'findOneAndUpdate'\|'findOneAndReplace', coll, filter, update\|doc, upsert, id, now, returnNew}` | `{ok:true, found, doc}` |
| `{op:'findOneAndDelete', coll, filter}` | `{ok:true, found, doc}` |
| `{op:'delete'\|'deleteMany', coll, filter}` | `{ok:true, result}` |
| `{op:'createCollection', coll}` | `{ok:true, created}` |
| `{op:'dropCollection', coll}` | `{ok:true, dropped}` |
| `{op:'createIndex', coll, keys, options}` | `{ok:true, name}` |
| `{op:'dropIndex', coll, index}` | `{ok:true, dropped:true}` |
| `{op:'listIndexes', coll}` | `{ok:true, indexes:[...]}` |
| `{op:'compact', coll}` | `{ok:true, result:{generation, bytesBefore, bytesAfter, bytesFreed}}` |
| `{op:'compact', minBytes, factor, skipBusy}` (no `coll`) | `{ok:true, result:{[coll]: stats\|null}}` |

`result` is `{acknowledged, matchedCount, modifiedCount, deletedCount,
insertedCount, upsertedId}` for a single write, and
`{acknowledged, insertedCount, matchedCount, modifiedCount, deletedCount,
upsertedCount}` for a list of them. An upsert is counted once, as an
upsert: it is *applied* as an insert, and only the plan still knows which
it was.

**A list of writes is one round trip, and one loop — the server's.**
`insertMany` and `bulkWrite` are not the same operation. One list holds
documents and goes through a single `DC_WREQ_INSERT_MANY` plan; the other
holds writes of six different kinds, each planned and applied on its own.
What they share is how they can go wrong, so they answer in the same
shape:

```js
{ ok: true,
  result:    { acknowledged, insertedCount, matchedCount, modifiedCount,
               deletedCount, upsertedCount },
  attempted: 2,                                  // how many of the list ran
  upserted:  [ {index, id} ] | null,
  errors:    [ {index, code, msg} ] | null }
```

**A failed member is a result, not a refusal**, which is what makes
`ordered` mean anything: `false` attempts every member regardless of
earlier failures, `true` stops at the first. `attempted` is the one fact
a client cannot derive — with `ordered:true` "never tried" and "tried and
succeeded" are different answers — and everything attempted but not named
in `errors` succeeded. Inserted ids are absent and upserted ids are
present because an insert's id was chosen by whoever asked, while an
upsert's was resolved here.

In a host that shares a process with the engine, that loop is JavaScript's
(`wasm/include/db_bulk.h` says why, and it stays true there). Over a
socket the same loop would be N round trips — and a client with no engine
in it has no `dc_bulk_parse` to check a list of operations with. So the
list goes over whole and C runs it.

**The grammar is checked before any of the list runs.** Which operation
names exist and which fields each one needs is `dc_bulk_parse`'s
(`db_bulk.h`), and so are the wire's own rules — that a write which might
need an `_id` was given one, and that one which dates a field was given a
clock reading. A malformed list is refused entirely, with
`index` naming the operation that was wrong — the one refusal that names
a position, because a list of operations has positions. That ordering is
not tidiness: an unordered run is supposed to attempt every operation,
which it cannot do if operation seven is malformed in a way that only
surfaces once one through six have already landed.

A `bulkWrite` that inserts makes a missing collection, exactly as an
`insert` does; a `bulkWrite` of nothing but deletes and updates does not,
exactly as a `find` does not.

**Cursors page a scan, not a result.** `batchSize` on a find opens a
cursor: one batch comes back with an id, `getMore` asks for the next, and
`cursor` comes back **null** on the last batch — so a drained cursor
needs no `closeCursor` and costs no round trip to discover it is finished.
What the server holds between calls is a *position in a B+ tree scan*
(`dc_cursor_open`), not a materialised result, which is the difference
between paging a million documents and being sent a million documents.

A cursor belongs to the connection that opened it: another connection
asking for it gets `-46`, the same answer as for an id that never
existed, because telling those apart would tell a client about somebody
else's cursors. Cursors are released when drained, closed, when their
connection ends (however it ends), or when the server closes.

**A cursor is a snapshot**, which most databases will not give you for
free. It pins the root of the B+ tree it scans and walks nodes that
mutations never overwrite, so writes from other connections during a
scan are simply not seen, and every document is returned exactly once —
no missed documents, no duplicates, no read concern to ask for. MongoDB
makes no such promise without a snapshot read concern or a transaction:
a document can be missed or returned twice if a concurrent update moves
it in the index being walked.

The one operation that can break it is **compaction**, which rebuilds a
collection into fresh files and deletes the old ones. That is refused
while any cursor is open over a tree it would rebuild
(`DC_ERR_CURSORS_OPEN`, -49, before anything is written) — enforced in
`dc_compact_execute` rather than left to callers, because a cursor can
now outlive the request that made it.

**A pipeline runs whole, in C.** `aggregate` hands `dc_aggregate` the
stages and hands back what it produced — including the decision to push a
*leading* `$match` into the underlying scan, so the planner and any index
serve it. That decision lives with the planner it feeds
(`wasm/include/db_agg.h`), not in a client, and the subset itself
(`$match`, `$sort`, `$skip`, `$limit`, `$project`, `$group`, `$count`) is
named in exactly one place.

It answers in **one frame and opens no cursor**: `$sort` and `$group` need
every match before the first result exists, so there is no scan left to
resume — the same reason a sorted find cannot be batched. A stage the
subset does not have is refused with `index` naming its position, and the
client quotes what was at that position, because C does not format
messages around user data.

**The find-one-and-\* family answers with the document, not a count.**
`updateOne` says how many documents changed, not which, so reading one
back otherwise means a second query with a gap in the middle. These
return the document itself — `returnNew` picks the image (`before` is the
default, as in the driver), and `null` means nothing matched.

Neither image costs a query. The **before** image is the one the planner
already read to resolve its target (`dc_wal_plan_preimage`, whose comment
names these three methods); the **after** image is a read back by the id
the plan resolved, which is a `bpt_search` rather than a scan. A delete
has only one image — the document is gone — so `returnNew` is not an
error there, it is the same question.

An upsert asked for `before` answers `null`: no prior state exists to
show. MongoDB answers the same way.

**No `sort`**, which is the one substantive gap against MongoDB, where
`findOneAndUpdate(filter, update, {sort})` is the atomic work-queue
primitive — claim the highest-priority match. Without it you claim
whichever document the scan reaches first. The in-process API has no
`sort` either, and a wire that sorted while the local API did not would
be a worse divergence than the shared gap; adding it means teaching the
planner to order matches before picking one, which is not plumbing.

**A frame is an answer or an event.** Every frame used to be an answer,
in request order — which is why there are no request ids on this wire and
none are needed. `watch` adds the other kind: a frame the server sends
because *somebody else* wrote something.

```
  client                                    server
    │  {op:'watch', coll:'notes'}   ──────▶ │  this connection is watching
    │ ◀──────  {ok:true, stream:7}          │
    │  {op:'count', coll:'notes'}   ──────▶ │
    │ ◀──────  {ok:true, n:3}               │
    │                                       │  ← another client inserts
    │ ◀──────  {stream:7, event:{…}}        │  pushed, unasked
    │  {op:'closeStream', stream:7} ──────▶ │
    │ ◀──────  {ok:true, closed:true}       │
```

They are told apart by **shape**: an answer carries `ok`, an event
carries `stream`. A client that never watches never sees one, so the old
sentence still holds for it word for word. The event is the object an
in-process watcher gets, `ns` and all:

```js
{ ns: { coll: 'notes' }, operationType: 'insert' | 'update' | 'replace' | 'delete',
  documentKey: { _id }, index: 12,              // only on a replicated server
  fullDocument: {...} }                         // absent on a delete
```

**On a replicated server the stream is RESUMABLE, and the log index is
the token.** Every event carries `index` — the log index its command
committed at — and `{op:'watch', coll, from: N}` replays every entry
after `N` into the new stream before any live event: everything after
the token, exactly once, in order. The reply's own `index` is the replay
ceiling — where "live" begins — so a consumer holds a resume point
before its first event too. Three refusals, each its own code, because
the remedies differ: `-67` this server keeps no log (unreplicated —
resume is a property of the log, not the stream machinery), `-68` the
index was compacted away (watch afresh and re-read; a gap is never
bridged in silence), `-69` the index is ahead of this member's log.

What replay cannot reproduce is stated rather than discovered: the log
records commands, not outcomes, so a replayed update or delete appears
even if it matched nothing by apply time, and a replayed update's
`fullDocument` is the document as it *now* stands (or absent if gone) —
the same "current image" contract MongoDB's `updateLookup` makes.

**The events cost the engine nothing new.** A logged command already
names the one document it touched — the planner expanded `updateMany`
into one command per matched document before any of it ran — so an event
is built from the command and its result, which is the derivation every
other host of this library makes. The one read it cannot avoid is an
update's post-image: an update names its *changes*, not its outcome. That
read is a `bpt_search` by `_id`, and it happens only while somebody is
watching.

**A stream is a cursor's twin**: a bounded table (`DBS_MAX_STREAMS`), an
owner, and a death with its connection. Its opposite in one respect — a
cursor is pulled and a stream is pushed — which is where the two bounds
come from:

- The **session** holds at most `DBS_STREAM_EVENTS` (256) events or
  `DBS_STREAM_BYTES` (1 MB) per stream. Past that the stream *overflows*:
  it stops collecting, sends `{stream, overflow: true, index: N}` once —
  after delivering everything it had queued — and closes. `index` is the
  last log index actually delivered, present only when a log minted one;
  with it the overflow is a **page boundary** rather than a loss: resume
  with `from: N` and nothing is missed, which is also how a replay
  longer than one queue pages itself out. Without a log the old remedy
  stands: re-watch and re-read current state. A flag rather than a
  count, deliberately — once a stream overflows, nothing more is built
  for it, so any count would mean "however many happened before the
  transport next looked".
- The **transport** stops handing a connection events once it is holding
  `OUT_HIGH_WATER` (64 KB) of unsent bytes. Without that the backlog
  would simply move into a buffer nothing counts, and a consumer that
  stopped reading would cost the server memory instead of costing itself
  its stream.
- And the transport never sends an event past a **deferred answer**: on
  a replicated server a watch's reply is held for the read barrier, and
  that reply is the one carrying the stream id — an event delivered
  ahead of it would name a stream the client has not been told exists.
  Events queue in the session until the answer flows, then follow it.

**`--stdio` delivers events too, but only behind an answer.** That
transport is one client by construction and has no poll loop — the only
thing that ever happens is a request — so an event goes out immediately
after the answer to whatever produced it, and a watcher that wants one
promptly sends `ping`. The alternative would be a reader thread, and
`server/main.c` has neither threads nor the wish for any.

**And the loop does not sleep while a frame is owed.** Everything else
here becomes deliverable because a socket did something, which is what
`poll` waits for; an event becomes deliverable because *another* client
wrote, and the connection it is owed to may be silent, backed up, or
both. `dbs_stream_pending` is what the loop asks before blocking — added
after an overflow notice sat undelivered until an unrelated request
happened to wake the loop.

**`compact` with no collection named is the sweep**, and it lives here
rather than in a client loop because of its three options — two of which
read state a client cannot see:

| | |
| --- | --- |
| `minBytes` | a file set smaller than this is not worth rewriting |
| `factor` | nor is one that has not grown to `factor` times its size *right after its last compaction* — a number the catalog records at the flip (`compactedBytes`), so the heuristic reads a fact rather than estimating one |
| `skipBusy` | a collection someone is scanning is skipped (`null`) rather than refused |

With none of them it is unconditional, exactly like asking for each
collection in turn. `skipBusy` is the difference between a sweep and a
request that named one collection: an unattended sweep wants that
collection's turn to come round on the next pass, while a caller who
named it wants the `-49`.

**`pruneExpired` is a sweep somebody asks for, not a background
thread.** The engine runs no timers, so *when* to expire stays with
whoever is driving — and `now` travels with the request, for the third
time and the same reason as `_id` and `$currentDate`. Which indexes
expire what, and the cutoff arithmetic, are `db_ttl.h`'s and the
session's; the deleting goes through the same plan/apply path as any
other write, so a swept document is one a log could have carried. A
collection with no TTL index is owed nothing, which is `0` rather than a
refusal.

**`findByIndex` names its index instead of describing what it wants** —
an O(log n + k) range scan of that index with no planner in the way,
which is the escape hatch for a query the equality planner is too
conservative to serve.

There are three ways to ask it wrongly, and they were one `BJ_ERR_STATE`
between them until this went on a wire: no index of that name (`-57`),
the wrong *kind* of index (`-58` — a text or geo index answers a
different question and has no equality tree to scan), and the wrong
number of values (`-59` — one per indexed field, in the index's order).
"-2, builder state error" is not something a client on the far end of a
socket can act on. The in-process host used to avoid two of the three by
checking them itself, against its own copy of the index list; the
collection is what holds the indexes, so the collection answers now, and
that copy is gone.

**`explain` answers without executing.** It reports which source the
dispatch *would* use for a filter — `scan`, `ids`, `equality`, `text`,
`geo`, with the serving index named for the last three — by consulting
the very planners the queries consult, so the report cannot drift from
what a query would really do.

The plan's *name* is C's too (`dc_explain_source`). It was a JavaScript
array until this wire needed one, and two hosts spelling one plan
differently is a fact with two owners; changing `equality` in C now fails
tests in the in-process suite, the coordinator suite and this one.

**A sorted find cannot be batched** (`-48`). An arbitrary sort needs
every match before the first ordered result exists — the reason
`dc_cursor_open` has no sort parameter, and the reason the in-process
cursor refuses `next()` on a sorted find too. One rule, said once by each
layer, rather than a server that quietly materialises everything and
calls it a cursor. Ask without `batchSize`, or without `sort`.

**A refusal is a response.** Anything the request gets wrong — an unknown
op, a missing field, no such collection, a duplicate key — comes back as
`{ok:false, code, msg}` where `code` is a `DC_ERR_*` and `msg` is
`dc_strerror`'s text, the same sentence a native caller would get. The
connection survives it.

Two refusals are about the transport rather than a request, and both are
sent to a connection that is then closed: `code: -44` to one that arrived
when all `--max-clients` slots were taken, and `code: -45` to one whose
slot is being taken back after `--idle-timeout`. Both are in the same
shape as every other refusal, so a client reads them with the code it
already has — and both say what happened, rather than leaving a client to
infer it from a socket that closed.

**`listCollections` names no collection**, which is the question you ask
when you do not know what is there. The catalog's keys *are* the
collection names — there is no list kept beside them to fall out of step
— minus the format stamp, a reserved key no collection can be called.

**It can build a database, not just serve one.** Point the server at an
empty directory and it writes the catalog (and the format stamp) at
startup; `createCollection` makes a collection, and an `insert` into a
name that does not exist makes one too — the way it does in every other
host of this library, and in the database this is shaped after. A *read*
of a name that does not exist is refused (`-37`) rather than answered out
of a collection created on the reader's behalf: at that point it is far
more likely to be a typo than an intention.

`createIndex` plans the index — kind, name, files, all three decided by
`db_catalog.h`, as they are for every host — creates exactly those files,
**backfills it against every document already there**, and records the
definition in the catalog entry. A failed build (a missing field, an
unindexable value, a duplicate on a `unique` index) leaves the collection
without the index and the catalog untouched.

**Compaction is a request like any other.** `compact` rewrites a
collection's whole file set without its append-only history and adopts
the result — plan, stream, flip, reopen, delete — in one call
([`compaction.md`](compaction.md)). The browser needs an awaited
pre-open pass between the plan and the execute because OPFS opens are
promises; here `ns->open` really opens, so the two calls sit next to each
other with nothing between them. That difference, and only that
difference, is what the plan/execute split buys.

The session reopens the new generation for itself, so the next request is
answered from it without anyone reconnecting, and the old files are
deleted after the flip. Refused with `-49` while any cursor — anyone's —
is scanning that collection.

**Ids stay with the caller, and so does the clock.** `id` supplies the 12
bytes a write needs if it *turns out* to need one — an upsert that
matched nothing, which is the only write whose need for an id cannot be
known until it has run. Generating one needs a clock, which
`wasm/include/db.h` keeps out of the engine deliberately, so a write that
needed an id and was not given one is refused rather than given an id
invented in C.

An **insert** is not one of those writes: its document carries its own
`_id`, exactly as every member of an `insertMany` list must, and `id` is
not a second place to put it — two places for one fact would need a
precedence rule between them. `{op:'insert', doc:{name:'Ada'}, id:…}` is
`-42`, "Request is missing a field its op requires". (Until recently it
was `-2`, "builder state error": the refusal was right and the sentence
was about a builder.)

`now` is the same bargain: milliseconds, for an update carrying
`$currentDate`. That is not an operator the engine knows —
`upd_apply`'s table has no entry for it — because a host is supposed to
rewrite it into a concrete `$set` *before* proposing, so that what gets
written down is a date rather than a rule that would read a different
clock on replay (`db_wal.h`). This server is a host too, so it does that
rewrite, with `upd_resolve_current_date` (which also owns the rule that a
field cannot be both `$set` and dated) and with the caller's
milliseconds. An update that needed them and was not given them is
refused. A `bulkWrite` carries one reading for the whole list, so two
members dating the same field cannot disagree about when it was.

The date is therefore the *client's* clock, not the server's — the same
clock already embedded in every ObjectId this client mints. A deployment
that needs the database's own notion of now wants a gateway that stamps
it, not a clock inside the engine.

**Nothing is re-encoded on the way through.** Filters, documents and
updates are handed to the engine as the bytes they arrived as, and
results leave as the bytes the engine produced. Writes go through
`dc_wal_plan_build` + `dc_wal_apply` — the same path a replicated write
takes — so every mutation this serves is one a log could have carried.

**Including the DDL three.** `createIndex`, `dropIndex` and
`dropCollection` are planned into commands and applied through
`dbs_apply`, exactly as a write is. They used to call `dbs_*` directly,
which left a follower nothing to be sent —
[`docs/replicaton-roadmap.md`](replicaton-roadmap.md) step 4 names it:
"the single-node *unlogged DDL is safe* argument dies with the first
follower." There is no log here yet, so the command is built and applied
in the same breath; when there is one, the change is to propose it
instead, in one place.

`createCollection` is the exception and has no command, because an
insert makes a collection implicitly and *that* is logged. What does not
travel is a collection that was named and never written to, which
carries nothing.

**`dbs_apply` performs a committed command of any kind.** `dc_wal_apply`
drives the four document ops and refuses the DDL three
(`DC_ERR_WAL_NOT_APPLIABLE`), because they make and unmake *files* —
whoever owns the namespace has to do that, and in C that is
`db_session.c`. So the session applies DDL itself and hands documents
down, which is what makes a C process able to be a replica: until now it
needed a JavaScript host for exactly three opcodes.

A deterministic failure comes back as its code rather than being
swallowed: a re-applied `createIndex` finds the index already there
(`-56`), a re-applied `dropIndex` finds it gone (`-57`). Both are what
convergence looks like from inside an apply loop, so both are now
`dc_is_deterministic` — an answer, not a halt. `-37` ("no such
collection") deliberately is not: that is either a log this replica
cannot apply or a state that has drifted, and the ambiguity resolves
toward stopping.

Everything the server decides lives behind one function,
`dbs_handle(dbs*, req, req_len, dbuf *out)` (`wasm/include/db_session.h`),
which is why the protocol is tested in `test/native/main.c` over buffers
with no socket and no port.

## Invariants

- **One process per database directory.** The whole answer to concurrent
  writers: wasi-filesystem has no locking to arbitrate them with, so
  there is never more than one. The same rule OPFS enforces in the
  browser and `NodeFSStorageProvider`'s advisory lock enforces in Node.
- **Many connections, one at a time through the engine.** `poll()` over
  the listener and every accepted socket; whichever is ready is served,
  and `dbs_handle` runs to completion for one request before the next is
  looked at. There are no threads and there is no second engine, so the
  database sees the same serial stream it saw when there was one
  connection — what changed is who waits for whom. The sockets are
  non-blocking and a connection carries the bytes of a request that has
  only partly arrived and a response that has only partly gone out; a
  client that stops reading delays nobody but itself.
- **Bounded, and it says so.** `--max-clients` is a fixed table sized at
  startup, for the reason every other table here is bounded: a server
  that grows one per client has a failure mode nobody tests. Nothing is
  read from a client whose last answer has not gone out, so a pipelining
  client cannot make the server hold an unbounded number of answers for
  it either.
- **A slot has to be earned.** `--idle-timeout` closes a connection that
  has asked nothing for that long. It is aimed at the connection whose
  peer is *gone* — a crashed client, a dropped NAT mapping, a half-open
  socket — all of which look exactly like a quiet one to TCP, and all of
  which would otherwise hold a slot until the process restarts.
  `SO_KEEPALIVE` is not the answer: it defaults to hours, and the knobs
  that shorten it are per-OS and not reliably available through
  wasi-sockets. The timer measures **silence**, not connectedness — it is
  reset by a request and by its answer going out, so a client dribbling
  one byte at a time is closed like any other client that asked nothing.
  A client that wants to stay warm sends `{op:'ping'}`.
- **A clock is the transport's, not the engine's.** `server/main.c` reads
  `CLOCK_MONOTONIC`; nothing below it learns what time it is, which is
  why an insert's `_id` is still the caller's. Monotonic so that an NTP
  correction cannot take a connection's slot away, and a clock that
  cannot be read *stops* rather than jumping, so nothing times out.
- **The transport frames, it does not interpret.** `server/main.c` never
  reads a field of a request or a response.
- **Nothing is dropped in silence.** Every refusal is a distinct code
  with `dc_strerror` text.

## What it does not do yet

Stated here rather than discovered later.

- **Cursors are bounded and not timed out on their own.** Sixteen at
  once across all clients (`DBS_MAX_CURSORS`); the seventeenth is `-47`.
  An abandoned cursor is held until its connection ends, which the idle
  timeout bounds but does not target.
- **A sorted find still returns one frame.** Batching it is refused
  rather than faked, so a large sorted result is as large as it was.
- **No fairness between clients.** Ready connections are served in table
  order every time round the loop, so a client that always has a request
  waiting is always looked at before one further down. Nothing starves
  while requests are small; a stream of large ones from slot 0 would make
  slot 5 wait.
- **No compaction scheduler for COLLECTIONS.** The engine runs no
  timers, so *when* to sweep stays with whoever is driving
  (`docs/compaction.md`) — which is what `minBytes`/`factor` are for:
  they make calling it on a timer cheap, because a sweep that finds
  nothing worth doing costs one `size()` per collection. (The LOG is
  different: `--snapshot-entries` is a transport-side policy and the
  transport has a clock, so log compaction does happen by itself.)
- **A sweep opens every collection**, and the session holds at most
  `DBS_MAX_COLLECTIONS` (32) open at once. A database with more than
  that refuses the sweep (`-38`) rather than half-doing it.
- **No change-stream pipelines, no `updateDescription`.** Two of the
  three non-goals the in-process `watch()` has (README); the third —
  resume tokens — stopped being one on a REPLICATED server, where the
  log index is the token and an overflow is a page boundary (above). A
  stream still watches one collection, whole events; without a log, an
  overflowed consumer still re-watches rather than resuming.
- **No `sort` on the find-one-and-\* family**, as above.
- **No TLS, no auth, no tenants.** Loopback only. Those belong to the
  gateway in front, not to the database
  (`docs/replicaton-roadmap.md` step 4 records that boundary).

## Why not `wasi:http`

`wasmtime serve` **instantiates the component once per request** —
measured, with a counter in a static that read `1` on every request. For
a database that is disqualifying rather than inconvenient: no open
collections, no page cache, no handle table, and a fresh open on every
call. It also puts two writers on the same B+ tree files the moment two
requests overlap, and wasi-filesystem has no locking to stop them.

A long-lived `wasmtime run` process with ordinary BSD sockets has none of
those problems, and the runtime is not the expensive part: the same
90-test workload takes 0.02s native, 0.16s under wasmtime, 0.33s under
Node's WASI host — and startup amortises to nothing in a process that
stays up. What does not amortise is the I/O path, and that is where the
hosts really differ:

```
Node/browser:  hostio.c      EM_JS(bjio_js_read)  ->  wasm -> JS -> fs.readSync
WASI/native:   bjio_posix.c  pread(...)           ->  wasm -> syscall
```

Every B+ tree page read in a JS host crosses into JavaScript and back.
This path deletes that bridge rather than optimising it.

`wasi:http` still earns a place as a **stateless gateway in front** of
this, and for outgoing calls. It is not the database's own interface.

## Tests

`test/db.server.test.js` drives a real process over a pipe and over a
socket, twice over — native, and wasip2 under wasmtime — with a client
that shares no code with the server, against databases the JavaScript
implementation wrote. CI builds both artifacts and sets
`NISABA_SERVER_TESTS=required`, so those suites cannot quietly stop
running.
