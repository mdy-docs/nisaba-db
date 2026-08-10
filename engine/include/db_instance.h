/*
 * db_instance.h — one process, one root directory, many databases.
 *
 * `dbs` (db_session.h) is ONE database: one catalog, one set of
 * collection files, one directory. This is the layer above it, and it is
 * the shape a client sees:
 *
 *     const client = await connectServer('127.0.0.1:8097');
 *     const analytics = client.db('analytics');
 *     const billing   = client.db('billing');
 *
 * Both halves of that matter. `client.db(name)` is a CLIENT-SIDE HANDLE
 * -- nothing is sent when you call it -- and the CONNECTION is not
 * stateful about which database, because every request names its own in
 * a `db` field. That is the only reason two handles can share one
 * connection and be switched between freely, and it is why there is no
 * "use" op here: a request whose meaning depends on an earlier request
 * is the thing the shape exists to avoid.
 *
 * A DATABASE IS A DIRECTORY. Two names never share a catalog or a
 * collection file, which is the same guarantee Client.db(name) makes in
 * process by handing each name its own `provider.subProvider(name)`. A
 * prefix scheme inside one flat directory would be a second file-naming
 * convention living beside db_names.h, and two is one too many.
 *
 * WHAT THIS LAYER CANNOT DO ITSELF, AND WHY
 *
 * Opening a subdirectory, listing what is in one, and removing one whole
 * are not `bj_ns` operations and must not become any: bj_ns is one
 * directory by construction, and it deliberately cannot enumerate,
 * because OPFS enumeration is asynchronous and bjns.h passes listings in
 * rather than offering them. So they arrive through `dbi_root`, which
 * the host fills in -- POSIX in server/main.c, and nothing anywhere else,
 * because no browser runs this.
 *
 * REPLICATION FOLLOWS THE INSTANCE, NOT THE DATABASE. One log, one
 * leader, one member set, one failover story for the whole executable.
 * That is why nothing in server/replica.c or server/peers.c knows this
 * file exists: a log entry gained an envelope saying which database its
 * command is for (dbi_apply), and that is the whole of the difference.
 *
 * The alternative was a log per database -- what src/raft-host.js does,
 * one WalDb = one log = one group. It is not this because its
 * justification was tenancy, which is a layer above this repository, and
 * because it fights the shape at the top of this comment: one connection
 * holds many databases, and with leadership per database one socket
 * could lead `analytics` and follow `billing`, so every caller would
 * need a redirect table keyed by database.
 *
 * What that costs, accepted: one database's write rate is every
 * database's, and a HALT in one halts all of them.
 */
#ifndef DB_INSTANCE_H
#define DB_INSTANCE_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "dbuf.h"
#include "bjns.h"
#include "db_session.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Databases held OPEN at once. Not a limit on how many may exist -- one
 * nobody has asked for costs a directory and nothing else -- but on how
 * many this process holds catalogs and file handles for, and it is
 * refused explicitly when full, like every other table in this server.
 */
#define DBI_MAX_DATABASES 16

/* The longest database name this build will open. A name is a directory
 * entry, so a bound here is a bound on a path segment. */
#define DBI_NAME_MAX 64

/* Every slot is taken by a database somebody is using. Distinct from
 * DC_ERR_TOO_MANY_COLLECTIONS because the remedy is different: close a
 * database, not a collection. */
#define DC_ERR_TOO_MANY_DATABASES (-65)

typedef struct dbi dbi;

/*
 * The root directory, as the four things this layer needs from it and
 * cannot do itself. The host owns all of them.
 *
 *   open_ns   a namespace for database `name`, created if `create`.
 *             DC_ERR_NO_DATABASE when it is absent and create is 0.
 *   close_ns  give it back
 *   list      the database names, appended as a binjson ARRAY of STRINGs
 *   remove    the whole directory, and everything in it. *removed is 0
 *             when there was none, which is not an error: dropping the
 *             already-gone is what the caller asked for.
 *
 * `list` reports the SCOPES that exist -- what the directory says -- and
 * not a set proven to hold a catalog. Proving it would mean opening each
 * one, and a listing that opens every database it names is a listing
 * that cannot be asked for casually.
 */
typedef struct {
    void *ctx;
    int  (*open_ns)(void *ctx, const char *name, uint32_t len, int create, bj_ns *out);
    void (*close_ns)(void *ctx, bj_ns *ns);
    int  (*list)(void *ctx, dbuf *out);
    int  (*remove)(void *ctx, const char *name, uint32_t len, int *removed);
} dbi_root;

/* `root` is COPIED; the ctx behind it is borrowed and must outlive this. */
int  dbi_open(const dbi_root *root, int order, dbi **out);
void dbi_close(dbi *i);

/*
 * The session for `name`, opening it if this process does not have it
 * open already. Cached, so the same name returns the same `dbs` -- which
 * is what makes two handles on one connection cheap, and what makes a
 * cursor opened through one of them findable through the other.
 *
 * DC_ERR_INVALID_DB_NAME, DC_ERR_NO_DATABASE (absent, and !create),
 * DC_ERR_TOO_MANY_DATABASES (no slot).
 */
int dbi_database(dbi *i, const char *name, size_t len, int create, dbs **out);

/* Every database under the root, as a binjson ARRAY of STRINGs. */
int dbi_list(dbi *i, dbuf *out);

/*
 * Delete a database and every file in it; *dropped is 0 if there was
 * none. CLOSED FIRST, always: this process holds catalogs and tree
 * contexts pointing into storage that is about to stop existing.
 */
int dbi_drop(dbi *i, const char *name, size_t len, int *dropped);

int dbi_open_count(const dbi *i);

/* ---- the request path ---------------------------------------------------
 *
 * The same three entry points db_session.h has, with the routing in
 * front. Each reads the request's `db` field, resolves it, and hands the
 * rest to the session -- which is unchanged, and still the one place
 * that knows what an op means.
 *
 * Three ops name no database and are answered here: `ping`, which
 * touches nothing at all, and `listDatabases`/`dropDatabase`, which are
 * about the instance and cannot be answered by any one database in it.
 * Every other op must name one, and is refused with
 * DC_ERR_REQ_MISSING_FIELD if it does not -- rather than falling back to
 * a default, which would be a write landing somewhere nobody named.
 */
int dbi_handle(dbi *i, uint64_t client, const uint8_t *req, size_t req_len,
               dbuf *out);

/*
 * What this request does, before it is performed: db_session.h's
 * dbs_request_kind with the instance's own three ops in front of it.
 *
 * `dropDatabase` is a WRITE and is classified as one, which is what
 * makes a follower refuse it -- and on the replicated path it IS
 * replicated: dbi_propose turns it into an instance-level log entry
 * that every member applies against its own root (see below). It was
 * once answered outright, which removed a directory only the leader
 * stopped having; that divergence is closed.
 */
void dbi_request_kind(const uint8_t *req, size_t req_len, int *kind);

/*
 * db_session.h's dbs_read_is_long with the instance's routing in front:
 * resolves the request's `db` and asks that session. 0 for the instance's
 * own ops, which name no collection.
 *
 * It does NOT create the database. Sizing a read is a question, and a
 * question must not make a directory -- dbi_handle still creates one when a
 * request deserves it, which keeps "using a database makes it" in the one
 * place that performs requests.
 */
int dbi_read_is_long(dbi *i, const uint8_t *req, size_t req_len, int64_t min_docs);

/*
 * A READ VIEW of the collection this request names (db_session.h's
 * dbs_read_view, with the instance's routing in front) -- so a read can be
 * performed against a fixed state somewhere other than here.
 *
 * The view is the CALLER's: free it with dbs_read_view_close, and free it
 * before anything truncates or replaces the files under it. Creates no
 * database. Refuses with DC_ERR_REQ_MISSING_FIELD if the request names no
 * database or no collection, and otherwise with whatever opening it or
 * snapshotting it refused (DC_ERR_NO_COLLECTION, DC_ERR_NO_READ_VIEW).
 */
int dbi_read_view(dbi *i, const uint8_t *req, size_t req_len, dc_collection **out);

/*
 * db_session.h's dbs_request_wrecks_files plus the instance's own ops --
 * dropDatabase removes a whole directory, which is every file in it.
 */
int dbi_request_wrecks_files(const uint8_t *req, size_t req_len);

/*
 * The same question about a COMMITTED ENTRY rather than a request, for the
 * replicated path: a follower's destructive work arrives through the log,
 * not through a client.
 *
 * Answers 1 for the instance-level dropDatabase form, for any DDL command
 * (asked of db_wal.h's dc_wal_is_document, so this and the apply path
 * cannot disagree about what a command does), and for anything it cannot
 * parse -- a log this build does not understand is the worst case, not the
 * best.
 */
int dbi_entry_wrecks_files(const uint8_t *payload, uint32_t len);

/*
 * The replicated fork (db_session.h's dbs_propose / dbs_step), with one
 * addition: every command comes back WRAPPED.
 *
 *     { d: "<database>", c: <the command, as db_wal.h defines it> }
 *
 * The envelope is this layer's and the command inside it is untouched,
 * because "which database" is not part of what a command MEANS to a
 * collection -- it is addressing. Keeping them apart is what lets
 * db_wal.h's grammar stay exactly what it is, and what lets a command
 * built by a host whose log is per-database (src/db-wal.js) still be the
 * same bytes.
 *
 * One entry carries no command at all, because its act is about a
 * database rather than for one:
 *
 *     { d: "<database>", i: "drop" }        -- dropDatabase, replicated
 *
 * Applying it closes the database's session (whatever that session has
 * in flight -- see DC_ERR_DB_DROPPED) and removes its directory; the
 * apply result ({ok, dropped}) is the reply. A replay reader
 * (dbi_entry_cmd) sees "no command" and skips it, so a resumed change
 * stream never has to know the form exists. Streams that were LIVE on
 * the dropped database end silently with their session; there is no
 * invalidate event, deliberately -- the log records commands, not
 * outcomes, and a drop's outcome is that there is nothing left to say
 * events about.
 */
int dbi_propose(dbi *i, uint64_t client, const uint8_t *req, size_t req_len,
                uint64_t *token, dbuf *cmds, dbuf *out);
int dbi_step(dbi *i, uint64_t token, const uint64_t *indices,
             const dbs_result *results, uint32_t n,
             uint64_t *next, dbuf *cmds, dbuf *out);
void dbi_abandon(dbi *i, uint64_t token);
uint32_t dbi_inflight(const dbi *i);

/* Perform one committed entry: unwrap the envelope, resolve the database
 * -- creating it if a first insert is making it, exactly as a collection
 * is made -- and apply. */
int dbi_apply(dbi *i, uint64_t index, const uint8_t *payload, uint32_t len,
              dbuf *result);

/* Unwrap one entry's envelope WITHOUT applying it: the command it
 * carries for database `db`, or NULL when the entry is another
 * database's, is not an envelope, or carries no command. `db` of NULL
 * matches ANY database, for a caller classifying what an entry DOES rather
 * than replaying one database's history. The envelope's shape has one owner
 * and this is its reader -- a resumed change stream replays the log through
 * it (db_session.h's dbs_log). The pointers alias `payload`. */
int dbi_entry_cmd(const uint8_t *payload, uint32_t len,
                  const char *db, size_t db_len,
                  const uint8_t **cmd, size_t *cmd_len);

/* Register the entry log's reader with this instance: it reaches every
 * database already open and every one opened later, which is what makes
 * a watch on any of them resumable. NULL unregisters. */
int dbi_set_log(dbi *i, const dbs_log *log);

/*
 * The replay floor: the highest index THIS INSTANCE has applied, across
 * every database in the root -- not merely the ones that happen to be
 * open, because a replica that resumed above a closed database's floor
 * would never replay what that database is still owed. Apply is strictly
 * ordered across the one log, so the max is the applied prefix.
 *
 * It opens every database to ask, which is why it is a startup call and
 * not a hot one.
 */
uint64_t dbi_applied_floor(dbi *i);

/* A client that has gone, everywhere it might have left something. */
void dbi_drop_client(dbi *i, uint64_t client);

/* Change events owed to `client`, from whichever database produced them.
 * Drained like the session's own, repeatedly, until *have says no. */
int dbi_stream_take(dbi *i, uint64_t client, dbuf *out, int *have);
int dbi_stream_pending(const dbi *i);

#ifdef __cplusplus
}
#endif

#endif /* DB_INSTANCE_H */
