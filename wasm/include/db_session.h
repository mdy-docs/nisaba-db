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
 *   - docs/steps/wasip2-database-server.md — a server has to open a
 *     database before it can serve one.
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

typedef struct dbs dbs;

/*
 * Open the database in `ns` -- its catalog, and nothing else yet.
 * `order` is the B+ tree order to open trees with (the host's choice,
 * matching whatever wrote them). BJ_ERR_STATE if there is no catalog:
 * this opens databases, it does not make them.
 */
int dbs_open(bj_ns *ns, int order, dbs **out);

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
 */
int dbs_handle(dbs *s, const uint8_t *req, size_t req_len, dbuf *out);

#ifdef __cplusplus
}
#endif

#endif /* DB_SESSION_H */
