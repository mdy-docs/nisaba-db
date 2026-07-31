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

#ifdef __cplusplus
}
#endif

#endif /* DB_SESSION_H */
