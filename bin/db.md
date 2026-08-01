# db

A command-line tool for the document database (`db.js`): create
collections and indexes, and insert/find/update/delete documents, all from
the shell. See the parent binjson project's `docs/db-api.md` for the
complete JS API this wraps
(every query operator, update operator, and index option).

```
db <name> <command> [args] [options]
db --server <host:port> <command> [args] [options]
```

`<name>` selects (creating if needed) an OPFS subdirectory holding that
database's catalog and collection/index files. If `<command>` is omitted it
defaults to `collections`.

With `--server` the same commands run against a database in another
process — see [Talking to a server](#talking-to-a-server).

## Where files go

This tool persists through `NodeFSStorageProvider` (`nisaba/node`) — plain
`node:fs`, no OPFS shim. The data root is `$NISABA_DIR`, defaulting to
`~/.nisaba`; a database named `mydb` lives at `~/.nisaba/mydb/` — one
`__catalog__.bj` file plus one file per collection and per index, and a
`.nisaba-lock` advisory lock while a process holds the directory open
(one opener per database directory; a lock left by a dead process is
reclaimed automatically).

Databases created by earlier versions of this tool (which ran through the
`node-opfs` shim under `~/.node-opfs`) still open fine — the bytes are the
same format. Point `NISABA_DIR=~/.node-opfs` to keep using them.

## Commands

| Command | Description |
| --- | --- |
| `collections` | List collection names (default) |
| `drop-collection <coll>` | Drop a collection and its indexes |
| `compact [coll]` | Rewrite a collection's files (all collections if omitted) without their append-only history, reclaiming space — see `docs/compaction.md` |
| `dump [coll]` | Write the database (or one collection) to stdout as Extended-JSON JSONL: one `{"collection", "indexes"}` header line per collection, one `{"collection", "doc"}` line per document |
| `restore` | Read a dump from stdin into this database. Documents keep their `_id`s and indexes are recreated first, so restore into a **fresh** database name (existing `_id`s fail loudly) |
| `insert <coll> <doc>` | Insert one document |
| `insert-many <coll> <docs>` | Insert an array of documents |
| `find <coll> [filter]` | Find matching documents (`{}` if omitted) |
| `find-one <coll> [filter]` | Find the first matching document |
| `count <coll> [filter]` | Count matching documents |
| `distinct <coll> <field> [filter]` | Unique values of `field` across matching documents |
| `delete-one <coll> [filter]` | Delete the first matching document |
| `delete-many <coll> [filter]` | Delete every matching document |
| `replace-one <coll> <filter> <doc>` | Replace the first matching document |
| `update-one <coll> <filter> <update>` | Apply update operators to the first matching document |
| `update-many <coll> <filter> <update>` | Apply update operators to every matching document |
| `find-one-and-update <coll> <filter> <update>` | Atomically update and return a document |
| `find-one-and-replace <coll> <filter> <doc>` | Atomically replace and return a document |
| `find-one-and-delete <coll> [filter]` | Atomically delete and return a document |
| `bulk-write <coll> <operations>` | Mixed insert/update/delete operations in one call |
| `watch <coll>` | Stream change events (insert/update/replace/delete) until Ctrl+C |
| `create-index <coll> <keys>` | Create an index, e.g. `'{"team":1}'` |
| `drop-index <coll> <indexName>` | Drop an index |
| `list-indexes <coll>` | List a collection's indexes |
| `find-by-index <coll> <indexName> <values>` | Equality lookup via an index |
| `prune-expired <coll>` | Delete every document past a TTL index's cutoff |

Aliases: `collections` also accepts `list`.

## Documents, filters, and operators

`<doc>`/`<filter>`/`<keys>`/`<values>`/`<docs>`/`<operations>` are all JSON.
Filters support the full query engine (comparison/logical/array operators,
`$text`, `$near`/`$geoWithin`, `$regex`, etc.) and update documents support
the full update-operator set (`$set`, `$inc`, `$rename`, `$addToSet`,
`$push` with `$each`/`$slice`/`$sort`/`$position`, `$bit`, ...) — see
`docs/db-api.md` for the exact list and every operator's rules/limitations.

```sh
db mydb find users '{"age":{"$gte":18,"$lt":65}}'
db mydb find users '{"$or":[{"team":"core"},{"team":"kernel"}]}'
db mydb update-one users '{"name":"Ada"}' '{"$set":{"team":"core"},"$inc":{"visits":1}}'
```

`$text` (requires a `'text'` index) and `$near`/`$geoWithin` (require a
`'2dsphere'` index, GeoJSON Point values only):

```sh
db mydb create-index posts '{"body":"text"}'
db mydb find posts '{"$text":{"$search":"fox"}}'

db mydb create-index places '{"location":"2dsphere"}'
db mydb find places '{"location":{"$near":{"$geometry":{"type":"Point","coordinates":[-0.12,51.5]},"$maxDistance":1000}}}'
db mydb find places '{"location":{"$geoWithin":{"$box":[[-10,40],[10,60]]}}}'
```

Note: `$near`/`$geoWithin` distances here are in **kilometers**, not the
meters/radians real MongoDB uses for the equivalent operators.

`ObjectId` and `Date` values use MongoDB's Extended JSON literals:

```sh
db mydb find-one users '{"_id":{"$oid":"507f1f77bcf86cd799439011"}}'
db mydb insert events '{"name":"launch","at":{"$date":"2026-01-01T00:00:00Z"}}'
```

A bare hex string does **not** match an `ObjectId` field — same as the real
MongoDB driver, `_id` and `ObjectId` values are a distinct type from strings.

`update-one`/`update-many`/`find-one-and-update` reject a plain replacement
document — use `replace-one`/`find-one-and-replace` for that.

## Watching for changes

```sh
db mydb watch notes
# Watching notes for changes... (Ctrl+C to stop)
```

Streams every insert/update/replace/delete on the collection as it happens
(no filtering yet), one JSON-ish line per change, until you press Ctrl+C.
See `docs/db-api.md`'s "Change streams" section for the event shape and
cost model.

**This can only ever see writes made by this same `db watch` process.**
Unlike a browser tab using `connectShared` (`db-coordinator.js`), this
CLI opens the database with a plain, exclusive `connect()` — a *second*
`db` invocation against the same database while `watch` is running will
fail outright (the same OPFS exclusive-file-handle conflict `connectShared`
exists to work around in the browser), not silently miss events. There is
currently no way to run other `db` commands concurrently against a
database a `watch` is attached to.

## Talking to a server

The database also builds as a server: one process holding one directory,
speaking binjson over a socket, with no JavaScript in it at all
(`server/main.c`, built by `./wasm/build-server.sh` — see
[`docs/db-server.md`](../docs/db-server.md)). `--server` points this CLI
at one instead of opening files itself.

```sh
# start one over a database directory -- natively,
./wasm/lib/nisaba-server --port 8097            # (cwd = the directory)
# or as the wasm32-wasip2 command it is meant to be deployed as
wasmtime run -S inherit-network --dir ~/.nisaba/mydb::. \
  wasm/lib/nisaba-server-wasip2.wasm --port 8097

db --server 127.0.0.1:8097 count users
db --server 8097 insert users '{"name":"Ada","team":"core"}'   # bare port = loopback
db --server 8097 find users '{"team":"core"}' --sort '{"name":1}'
```

There is **no `<name>`**: the server was pointed at one directory when it
started and serves that one for its lifetime (one process per database
directory — the same one-writer rule the advisory lock enforces locally).
That directory may be **empty**: the server writes the catalog at
startup, and an `insert` into a collection that does not exist creates
it, so a database can be built from nothing over the wire.
For the same reason `--order` is refused with `--server`: the order the
files were written with is the server's to know, and it takes its own
`--order` if they were not made with the default 32.

The wire carries twenty-eight operations, and the CLI commands that ride
on them:

| Works over `--server` | |
| --- | --- |
| `find`, `find-one`, `count`, `distinct` | reads, including `--sort`/`--skip`/`--limit`/`--project` |
| `insert` | the `_id` is minted by this end — C will not invent one, since that needs a clock |
| `insert-many`, `bulk-write` | the whole list in one round trip, `--no-ordered` and all; the server runs the loop |
| `compact <coll>` | one collection at a time; refused while a cursor is reading it |
| `create-index`, `drop-index`, `list-indexes` | the server plans, builds and backfills the index |
| `find-by-index <coll> <indexName> <values>` | equality lookup with no planner in the way |
| `drop-collection` | |
| `collections` | |
| `dump [coll]` | walks `collections` → `list-indexes` → a `find` cursor |
| `restore` | header lines rebuild the indexes, documents go in batches of 500 |
| `update-one`, `update-many`, `replace-one` | including `--upsert` |
| `find-one-and-update`, `find-one-and-replace`, `find-one-and-delete` | the document itself, before or `--return-document after`; no `sort`, as locally |
| `delete-one`, `delete-many` | |

Everything else — `watch`, `prune-expired`, and `compact` with no
collection named — is not on the wire yet, and says so rather than pretending (`aggregate` and
`explain` are on the wire but have never been CLI commands):

```
$ db --server 8097 prune-expired events
Error: the server has no collection.pruneExpired() -- its wire carries ping,
find, findOne, count, distinct, aggregate, explain, insert, insertMany,
update, updateMany, replace, delete, deleteMany, findOneAndUpdate,
findOneAndReplace, findOneAndDelete, bulkWrite, findByIndex, getMore,
closeCursor, compact, createCollection, dropCollection, createIndex,
dropIndex, listIndexes, listCollections. Open the database directly for
the rest.
```

**The server serves many connections, up to its `--max-clients`.** A CLI
invocation connects, asks, and disconnects, so a shell full of them is
fine, and so is a long-lived client alongside them. Past the limit,
connecting still succeeds and the first command fails with the server's
own sentence — it is refused, not queued. A connection that asks nothing
for `--idle-timeout` seconds (60 by default) is closed and told why,
which no `db --server` invocation is ever around long enough to see.

The client itself is `src/db-server-client.js`
(`@mdy-docs/nisaba-db/server-client`) — a socket, the pure-JS binjson
codec, and nothing else. With `--server` this CLI never instantiates the
WASM engine at all.

## Options

| Option | Applies to | Description |
| --- | --- | --- |
| `--sort <json>` | `find` | Sort spec, e.g. `'{"age":1}'` or `'{"age":-1}'` |
| `--skip <n>` | `find` | Number of matches to skip (after sort) |
| `--limit <n>` | `find` | Max matches to return (after skip) |
| `--project <json>` | `find` | Projection, e.g. `'{"name":1}'` or `'{"age":0}'` |
| `--upsert` | `replace-one`, `update-one`, `update-many`, `find-one-and-update`, `find-one-and-replace` | Insert if nothing matched |
| `--return-document <before\|after>` | `find-one-and-update`, `find-one-and-replace` | Which document image to return (default `before`) |
| `--unordered` | `insert-many`, `bulk-write` | Attempt every operation instead of stopping at the first failure |
| `--name <name>` | `create-index` | Index name (default: `field_1[_field2_1...]`) |
| `--unique` | `create-index` | Reject a duplicate value for the indexed field(s) |
| `--sparse` | `create-index` | Don't index documents missing the field |
| `--partial-filter <json>` | `create-index` | Only index documents matching this filter |
| `--ttl <seconds>` | `create-index` | `expireAfterSeconds` — single-field index only |
| `--order <n>` | any file-creating command | B+ tree order for new files (default 32, min 3) — refused with `--server` |
| `--server <host:port>` | any command the wire carries | Talk to a running server instead of opening files here; a bare port means `127.0.0.1` |
| `-h`, `--help` | | Show help |

## Examples

```sh
db mydb insert users '{"name":"Ada","team":"core","age":36}'
db mydb insert-many users '[{"name":"Grace","team":"core","age":85},{"name":"Linus","team":"kernel","age":54}]'

db mydb collections
# 0: users

db mydb find users '{"team":"core"}' --sort '{"age":-1}'
# 0: { name: "Grace", team: "core", age: 85, _id: ObjectId(...) }
# 1: { name: "Ada", team: "core", age: 36, _id: ObjectId(...) }

db mydb distinct users team
# 0: "core"
# 1: "kernel"

db mydb create-index users '{"email":1}' --unique --sparse
db mydb create-index users '{"team":1}'
db mydb find-by-index users team_1 '["core"]'

db mydb replace-one users '{"name":"Ada"}' '{"name":"Ada","team":"core","age":37}'
db mydb update-one users '{"name":"Ada"}' '{"$inc":{"age":1}}'
db mydb find-one-and-update users '{"name":"Ada"}' '{"$set":{"onCall":true}}' --return-document after
db mydb bulk-write users '[{"deleteOne":{"filter":{"name":"Grace"}}},{"updateMany":{"filter":{"team":"kernel"},"update":{"$set":{"onCall":false}}}}]'
db mydb delete-many users '{"team":"kernel"}'
db mydb count users
```

## Running

No extra dependencies — run it directly:

```sh
node bin/db.js mydb collections
NISABA_DIR=/somewhere/else node bin/db.js mydb collections
```

or, once the package is installed, via the `db` bin.
