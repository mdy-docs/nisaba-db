/*
 * db_session.h — resolving a collection BY NAME, in C.
 *
 * Every host so far has done this in its own language: read the catalog,
 * find the entry, work out which files the collection is made of, open
 * each one, attach each index in the right order, recover the journal.
 * `Db.collection()` in wasm/nisaba-wasm.js is that code, in JavaScript,
 * and it is the reason a process with no JavaScript can perform a
 * committed entry (dc_wal_apply) but cannot find the collection to
 * perform it against.
 *
 * That gap is what this file closes, and it is why three separate pieces
 * of work were blocked on it:
 *
 *   - the database server (docs/db-server.md) — it has to open a
 *     database before it can serve one. Built, on this.
 *   - docs/steps/install-snapshot-in-c.md — "it would need to resolve a
 *     collection BY NAME, which needs a namespace".
 *   - docs/steps/completions-in-c.md — the apply pump stays in the host
 *     for the same reason.
 *
 * WHAT IT DOES NOT DO
 *
 * It does not decide any of the schema. Which files a collection is made
 * of, in what order, with which options, and what an entry written before
 * a field existed means, are all dc_catalog_open_plan's -- this walks the
 * plan and opens what it names. Nothing here knows a file naming rule,
 * and adding one would put the schema in two places.
 *
 * It does not create anything. A name with no catalog entry is
 * DC_ERR_NO_COLLECTION, not a fresh collection: creation writes to the
 * catalog, which belongs with the write path rather than with opening.
 *
 * OWNERSHIP
 *
 * The session owns every io, tree and collection it opens, and
 * dbs_close releases all of them. The NAMESPACE is the caller's -- the
 * host opened it and the host closes it, the same rule bjns.h states for
 * every file handle. A session never outlives its namespace.
 *
 * BOUNDED
 *
 * Fixed tables, refused explicitly when full, for the reason raft_node.c
 * gives for `pending`: a server that grows a table per request has a
 * failure mode a test will never find. Both bounds are generous for the
 * shape this is for -- one process per database directory.
 */
#ifndef DB_SESSION_H
#define DB_SESSION_H

#include <stddef.h>
#include "bjns.h"
#include "dbuf.h"
#include "db.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Collections held open at once, and indexes on any one collection. */
#define DBS_MAX_COLLECTIONS 32
#define DBS_MAX_INDEXES     16
/* Cursors open at once, across every client. A cursor is the only state
 * here that outlives the request that made it, which is why it is
 * counted, owned, and released with the client that owns it. */
#define DBS_MAX_CURSORS     16

/* No catalog entry of that name. Distinct from "the entry is unusable"
 * (DC_ERR_CATALOG_ENTRY) because a caller answers them differently: one
 * is a client's typo, the other is a corrupt database. */
#define DC_ERR_NO_COLLECTION        (-37)
/* The session's tables are full. */
#define DC_ERR_TOO_MANY_COLLECTIONS (-38)
#define DC_ERR_TOO_MANY_INDEXES     (-39)
/* The request itself is wrong, as opposed to the database being unable
 * to satisfy a well-formed one. */
#define DC_ERR_REQ_MALFORMED        (-40)
#define DC_ERR_REQ_UNKNOWN_OP       (-41)
#define DC_ERR_REQ_MISSING_FIELD    (-42)
/* dbs_open found no catalog: this directory holds no database. Distinct
 * from a catalog that will not parse, because the answers differ --
 * create one, versus this one is damaged. */
#define DC_ERR_NO_DATABASE          (-43)
/* The two refusals that are about the TRANSPORT rather than about a
 * request: the server is already holding as many connections as it will
 * (sent before the client has asked anything), and this connection has
 * said nothing for long enough that its slot is being taken back. */
#define DC_ERR_TOO_MANY_CLIENTS     (-44)
#define DC_ERR_IDLE_TIMEOUT         (-45)
/* Cursors. NO_CURSOR covers "no such id" and "not yours" deliberately:
 * a client learning which of the two it was would be learning about
 * another client's cursors. */
/* Creation and schema. -55 and up: -50..-54 are the consensus layer's
 * (raft_core.h, raft_msg.h, raft_node.h), and a code that means two
 * things reaches a client as the wrong sentence -- which is exactly how
 * DC_ERR_UNSUPPORTED_ID and WAL_NOT_APPLIABLE shared -35 until a test
 * asked every code for its text. */
#define DC_ERR_FORMAT_NEWER         (-55)
#define DC_ERR_INDEX_EXISTS         (-56)
#define DC_ERR_NO_INDEX             (-57)

#define DC_ERR_NO_CURSOR            (-46)
#define DC_ERR_TOO_MANY_CURSORS     (-47)
#define DC_ERR_CURSOR_SORTED        (-48)

typedef struct dbs dbs;

/*
 * Open the database in `ns` -- its catalog, and nothing else yet.
 * `order` is the B+ tree order to open trees with (the host's choice,
 * matching whatever wrote them).
 *
 * `create` decides what an empty directory means. With it, a directory
 * with no catalog gets one (and the format stamp that goes with it),
 * which is how a server starting on an empty directory ends up serving a
 * database rather than refusing to start. Without it, no catalog is
 * DC_ERR_NO_DATABASE and nothing is written -- for a caller that means
 * to open an existing database and would rather hear that it is not
 * there than quietly make one.
 *
 * The format stamp is checked either way: a database written by a newer
 * build is refused (DC_ERR_FORMAT_NEWER) rather than opened and
 * misread. See docs/format-compatibility.md.
 */
int dbs_open(bj_ns *ns, int order, int create, dbs **out);

/*
 * Resolve `name` to an open collection, opening it on first use and
 * returning the same handle every time after that.
 *
 * ALL OR NOTHING: a failure part-way through opening a collection's file
 * set closes everything it had already opened and leaves the session
 * exactly as it was, so a retry is a retry rather than a second attempt
 * on top of a first one's wreckage.
 *
 * The returned collection is the session's; do not free it.
 */
int dbs_collection(dbs *s, const char *name, size_t name_len,
                   dc_collection **out);

/* How many collections are currently held open -- for tests, and for a
 * server that wants to say so. */
int dbs_open_count(const dbs *s);

/* ---- creation and schema ------------------------------------------------
 *
 * The other half of a database: making one. Everything above this line
 * opens what a catalog names; these write the catalog.
 *
 * They are here rather than in db.c for the same reason the open path is:
 * a collection is a SET of files that must be created, attached and
 * recorded together, and the only thing that can do all three is
 * whatever owns the namespace. C has always known the schema (the plan
 * functions in db_catalog.h decide names, kinds and options); what it
 * has never had is the host choreography around them, which is why a
 * server could serve a database it did not write and could not write
 * one of its own.
 *
 * ALL OR NOTHING, in the direction that matters: a failure part-way
 * leaves the CATALOG unchanged, so an interrupted create leaves files
 * nothing references -- an orphan the sweep collects -- rather than an
 * entry pointing at something half-built.
 */

/* Create the collection if the catalog has no entry for it: the entry,
 * its primary file, and nothing else (journal, generation and indexes
 * are added as they are earned). *created says whether this call made
 * it, so a caller can tell "made" from "already there" -- both are
 * success. */
int dbs_create_collection(dbs *s, const char *name, size_t name_len, int *created);

/* Remove the collection's catalog entry and delete every file it claims
 * -- the same file list the orphan sweep computes, from the same C, so
 * the two cannot disagree about what a collection is made of. *dropped
 * is 0 if there was no such collection, which is not an error. */
int dbs_drop_collection(dbs *s, const char *name, size_t name_len, int *dropped);

/* Create an index over an existing collection: plan it (kind, name,
 * files -- db_catalog.h decides all three), create exactly those files,
 * backfill it against every document already there, and record the
 * definition in the catalog entry. The chosen name is appended to
 * `name_out`.
 *
 * A failed backfill (a missing field, an unindexable value, a duplicate
 * on a unique index) leaves the collection without the index and the
 * catalog untouched -- the files it made are deleted here rather than
 * left for the sweep, because unlike a crash this path knows they are
 * garbage. */
int dbs_create_index(dbs *s, const char *coll, size_t coll_len,
                     const uint8_t *keys, size_t keys_len,
                     const uint8_t *options, size_t options_len,
                     dbuf *name_out);

/* Drop an index by name: out of the catalog entry first, then its files.
 * DC_ERR_NO_INDEX if the collection has no index of that name. */
int dbs_drop_index(dbs *s, const char *coll, size_t coll_len,
                   const char *name, size_t name_len);

/* The collection's indexes, in the shape a MongoDB driver's listIndexes
 * returns (db_catalog.h owns that projection). */
int dbs_list_indexes(dbs *s, const char *coll, size_t coll_len, dbuf *out);

/* ---- compaction ---------------------------------------------------------
 *
 * Rewrite a collection's whole file set without its append-only history
 * and adopt the result: plan, stream, flip, reopen, delete
 * (docs/compaction.md). One synchronous call, because between the last
 * byte of the new generation and the catalog write that adopts it,
 * nothing may observe the collection half-migrated.
 *
 * REFUSED while a cursor is scanning this collection
 * (DC_ERR_CURSORS_OPEN, before anything is written): the scan is
 * positioned inside files this replaces. Drain or close the cursor and
 * ask again.
 *
 * All or nothing in the way that matters: a failure before the flip
 * leaves the collection entirely on its old generation with the new
 * files orphaned for the next sweep. After the flip the new generation
 * is authoritative and this reopens it, so a failure there is a session
 * that must be reopened, not a database that lost anything.
 */
typedef struct {
    int      generation;
    uint64_t bytes_before;
    uint64_t bytes_after;
} dbs_compact_stats;

int dbs_compact(dbs *s, const char *name, size_t name_len, dbs_compact_stats *out);

/* ---- cursors ------------------------------------------------------------
 *
 * A cursor is the only thing here that outlives the request that made
 * it, so it needs an owner, and the owner is a CLIENT: the opaque token
 * a transport passes to dbs_handle. The transport picks the tokens
 * (server/main.c numbers its connections) and never has to know what a
 * cursor is; it only has to say when a client is gone.
 *
 * That is the whole ownership rule. A cursor is released when its client
 * drains it, closes it, or disappears -- and dbs_close releases whatever
 * is left, so no path leaks a scan.
 */

/* Internal to this module (db_request.c is the only caller): put a
 * freshly opened cursor in the table, find one back, drop one. Declared
 * here rather than in a private header because struct dbs lives in
 * db_session.c and these are the seam between the two files. */
int dbs_cursor_add(dbs *s, uint64_t client, dc_cursor *cur, uint32_t batch,
                   uint64_t *id_out);
int dbs_cursor_get(dbs *s, uint64_t client, uint64_t id,
                   dc_cursor **out, uint32_t *batch_out);
int dbs_cursor_drop(dbs *s, uint64_t client, uint64_t id);

/* Close every cursor `client` owns. Called by a transport when a
 * connection ends, however it ended. Safe on a client with none. */
void dbs_drop_client(dbs *s, uint64_t client);

/* How many cursors are open, across every client -- for tests, and for
 * a server that wants to say so. */
int dbs_cursor_count(const dbs *s);

/* Close every collection, the catalog, and the session. Safe on NULL.
 * Does not touch the namespace. */
void dbs_close(dbs *s);

/* ---- requests (db_request.c) ------------------------------------------- */

/*
 * Perform one request and append one response, both binjson objects.
 *
 * This is everything the server DECIDES. A transport reads a request,
 * calls this, and writes the response; it never reads a field of either,
 * for the same reason the Raft transport has never read a field of a
 * message. Sockets today, a preopened listener, a wasi:http gateway or a
 * native binary tomorrow -- all the same function, which is also why the
 * protocol can be tested with no socket and no port.
 *
 *   { op: "find",   coll: "users", filter: {...}, opts: {...} }
 *      -> { ok: true, docs: [...] }
 *   { op: "count",  coll: "users", filter: {...} }
 *      -> { ok: true, n: 3 }
 *   { op: "insert", coll: "users", doc: {...}, id: <12 bytes> }
 *      -> { ok: true, result: { acknowledged: true, insertedId: ... } }
 *
 * A REFUSAL IS A RESPONSE. Anything the request itself gets wrong -- an
 * unknown op, a missing field, no such collection, a duplicate key --
 * comes back as { ok: false, code: <DC_ERR_*>, msg: <dc_strerror text> }
 * and returns BJ_OK, because the caller asked a question and is owed an
 * answer. A nonzero return means no response could be built at all (out
 * of memory), which is the transport's problem rather than the client's.
 *
 * Reads go straight to dc_find/dc_count/dc_distinct: filters arrive as
 * binjson and results leave as binjson, so nothing is re-encoded on the
 * way through. Writes go through dc_wal_plan_build and dc_wal_apply --
 * the same path a replicated write takes, so every mutation this serves
 * is one a log could have carried, and the result shape is the one every
 * replica computes rather than this file's opinion.
 *
 * IDS STAY WITH THE CALLER. `id` supplies the 12 bytes a write uses if it
 * turns out to need one (an insert whose document has no _id, an upsert
 * that matched nothing) -- generating them needs a clock, which db.h's
 * top comment keeps out of this layer deliberately. A write that needs
 * one and was not given one is DC_ERR_REQ_MISSING_FIELD, not an id
 * invented here.
 *
 * CURSORS BELONG TO CLIENTS. `client` is an opaque token identifying
 * whoever is asking -- a connection id, for a socket server; anything
 * unique and stable, for anyone else. It matters for exactly one thing:
 * a cursor opened by one client cannot be advanced or closed by another,
 * and dbs_drop_client releases the cursors of a client that has gone.
 * Everything else answers the same regardless of who asked.
 */
int dbs_handle(dbs *s, uint64_t client, const uint8_t *req, size_t req_len,
               dbuf *out);

/*
 * Append the refusal { ok:false, code, msg } for `code`, with no request
 * and no session -- the shape dbs_handle answers every other refusal in,
 * so a transport that has to refuse before a request arrives (its
 * connection table is full) says so in the one format a client already
 * reads. The SHAPE stays owned here; only the decision is the caller's.
 */
int dbs_refusal(int code, dbuf *out);

#ifdef __cplusplus
}
#endif

#endif /* DB_SESSION_H */
