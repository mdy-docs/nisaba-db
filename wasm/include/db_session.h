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
#include "db_wal.h"

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
/* Change streams open at once, and what one will hold for a consumer
 * that has stopped reading. Both bounds exist for the same reason every
 * other table here is bounded, and the byte bound exists because an
 * event carries a DOCUMENT: counting events alone would bound the wrong
 * thing on a collection of large ones. */
#define DBS_MAX_STREAMS     16
#define DBS_STREAM_EVENTS   256
#define DBS_STREAM_BYTES    (1u * 1024u * 1024u)
/* Planned writes awaiting replication (dbs_propose). One per connection
 * is the shape the transport already has -- a connection is served one
 * request at a time and is not read from while it owes bytes -- so this
 * is server/main.c's MAX_CLIENTS, and a request over it is refused
 * rather than queued. */
#define DBS_MAX_INFLIGHT    64

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
/* DC_ERR_NO_INDEX (-57) began here and now lives in db.h, next to the
 * lookup that also has to raise it: a collection is what knows its own
 * indexes. -58 and -59 are its neighbours there. */

/* A change stream id that is not this client's. Same conflation as
 * DC_ERR_NO_CURSOR, and for the same reason. */
#define DC_ERR_NO_STREAM            (-60)
#define DC_ERR_TOO_MANY_STREAMS     (-61)

/* This member is not the leader of its group, so it cannot take a write.
 * The response carries `leaderId` when there is one, which is the whole
 * of what a client needs to try again in the right place: FORWARDING IS
 * NOT THIS SERVER'S JOB. A server that forwarded would be holding a
 * request whose outcome it cannot promise anything about, on a
 * connection it would have to keep alive to say so. */
#define DC_ERR_NOT_LEADER           (-63)

/* Leadership moved between a write being proposed and it being applied,
 * and the entry at its index is a different leader's. Distinct from
 * DC_ERR_NOT_LEADER because it says something stronger: the write did
 * not happen and no replica holds it, so a retry is safe. */
#define DC_ERR_WRITE_LOST           (-64)

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
/*
 * Mint cursor and change-stream ids from `shared` rather than from this
 * session's own counters. For a host that holds SEVERAL sessions at once
 * (db_instance.h): every session starting at 1 hands one client two live
 * cursors with the same id, which is a thing no request can then name
 * unambiguously. NULL restores the session's own.
 */
void dbs_set_id_source(dbs *s, uint64_t *shared);

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

/*
 * Every TTL sweep this collection is owed: one filter per equality index
 * carrying expireAfterSeconds, as a binjson ARRAY of OBJECTs, each ready
 * to be handed to a deleteMany.
 *
 * db_ttl.h predicted this: "the index registry is still JS, so the loop
 * is too. When index metadata moves into the C catalog, this becomes a
 * single call and the loop goes with it." The catalog is here, so this
 * is that call. What it deliberately does NOT do is the deleting -- the
 * two hosts differ in what they do with a filter (delete it directly, or
 * log a delete command first), and that difference is theirs.
 *
 * `now_ms` is the CALLER's clock reading, for the third time in this
 * header and for the same reason: nothing below this line knows what
 * time it is. An empty array means no TTL index, which is not an error.
 */
int dbs_ttl_filters(dbs *s, const char *coll, size_t coll_len,
                    int64_t now_ms, dbuf *out);

/* Every collection name in the catalog, as a binjson array of strings,
 * in the catalog's own order. The format stamp is not one of them: it
 * is a reserved key no collection can be called, filtered here the same
 * way every host filters it. */
int dbs_list_collections(dbs *s, dbuf *out);

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

/*
 * Compact EVERY collection, and report what each one did as a binjson
 * OBJECT of { <name>: {generation, bytesBefore, bytesAfter} | null } --
 * null meaning skipped, which is the shape the in-process Db.compact()
 * returns and for the same three reasons:
 *
 *   min_bytes  a file set smaller than this is not worth rewriting.
 *   factor     nor is one that has not grown to `factor` times its size
 *              right after its last compaction. The catalog records that
 *              size (compactedBytes, written at the flip), so the
 *              heuristic reads a fact rather than estimating one. A
 *              never-compacted collection only has to clear min_bytes.
 *   skip_busy  a collection someone is scanning is skipped rather than
 *              refused. An unattended sweep wants its turn on the next
 *              pass, not an error; a caller who asked for one collection
 *              by name wants the error (dbs_compact keeps it).
 *
 * With none of them it is unconditional, exactly like asking for each
 * collection in turn.
 *
 * It is HERE rather than looped by a client because those three options
 * are the whole difference between a sweep and a loop, and two of them
 * read state -- the catalog's compactedBytes, and whether a tree is
 * pinned -- that a client cannot see. A loop without them would be a
 * second, weaker thing wearing the same name.
 */
int dbs_compact_all(dbs *s, int64_t min_bytes, double factor, int skip_busy,
                    dbuf *out);

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

/* Close every cursor AND every change stream `client` owns. Called by a
 * transport when a connection ends, however it ended. Safe on a client
 * with none. */
void dbs_drop_client(dbs *s, uint64_t client);

/* ---- change streams -----------------------------------------------------
 *
 * The one thing here that a client does not ask for: a frame it did not
 * request, carrying something another client did.
 *
 * Events are produced where writes are performed (db_request.c's
 * run_write), from the command and its result -- the same derivation
 * every other host makes, because a logged command already names the one
 * document it touched. Nothing new is asked of the engine.
 *
 * A stream belongs to a CLIENT, like a cursor, and dies with it. What it
 * does not do is wait: dbs_emit appends to whatever streams are watching
 * and returns, so a write is never slowed by a consumer, and the
 * transport hands out what has accumulated whenever it next looks.
 *
 * BOUNDED, AND IT SAYS WHEN. A consumer that stops reading fills its
 * queue; at DBS_STREAM_EVENTS events or DBS_STREAM_BYTES bytes the stream
 * OVERFLOWS -- it stops collecting, says so once, and closes. That is the in-process contract too (wasm/nisaba-wasm.js's
 * ChangeStream): there are no resume tokens, so an overflowed consumer
 * re-watches and re-reads current state, and a stream that grew without
 * limit or dropped events in silence would be worse than either.
 */

/* Start watching `coll`. The id is the client's handle to it, unique for
 * the session's lifetime and never reused. */
int dbs_watch(dbs *s, uint64_t client, const char *coll, size_t coll_len,
              uint64_t *id_out);

/* Stop. DC_ERR_NO_STREAM if that id is not this client's -- the same
 * conflation of "no such" and "not yours" cursors make, for the same
 * reason. */
int dbs_close_stream(dbs *s, uint64_t client, uint64_t id);

/* Is anyone watching this collection? A write asks first, because
 * building an event nobody wants costs a read per document. */
int dbs_watched(const dbs *s, const char *coll, size_t coll_len);

/* Append one already-encoded event OBJECT to every stream watching
 * `coll`. Never fails a write: a stream that cannot take it overflows,
 * which is that stream's problem and not the writer's. */
void dbs_emit(dbs *s, const char *coll, size_t coll_len,
              const uint8_t *event, size_t len);

/*
 * The next frame owed to `client`, appended to `out`, with *have set to
 * 1 if there was one. Called by a transport with nothing to say about
 * what it is carrying -- the frame is built here, in the shape
 * db_request.c answers in, so the wire has one owner.
 *
 *   { stream: <id>, event: {...} }        one change
 *   { stream: <id>, overflow: true }      events were lost; it is gone
 */
int dbs_stream_take(dbs *s, uint64_t client, dbuf *out, int *have);

/* Is any client owed a frame right now? A transport that sleeps until a
 * socket is ready must ask this before sleeping: a change event is the
 * one thing here that becomes deliverable without anybody sending
 * anything, so a loop that waits for traffic would sit on it. */
int dbs_stream_pending(const dbs *s);

/* How many streams are open, across every client -- for tests, and for a
 * server that wants to say so. */
int dbs_stream_count(const dbs *s);

/* How many cursors are open, across every client -- for tests, and for
 * a server that wants to say so. */
int dbs_cursor_count(const dbs *s);

/* ---- applying a committed command ---------------------------------------
 *
 * dc_wal_apply (db_wal.h) performs the four DOCUMENT commands against an
 * already-open collection. It refuses the DDL three with
 * DC_ERR_WAL_NOT_APPLIABLE, because they make and unmake FILES -- which
 * belongs to whoever owns the namespace, and in C that is this file.
 *
 * This is the other half. Give it any command and it performs it: it
 * resolves the collection by name and hands a document command on, and
 * it performs a DDL one itself. That completes "a committed entry no
 * longer needs a host to be applied" -- until now it needed one for
 * exactly three opcodes, which is three too many for a C process that
 * wants to be a replica.
 *
 * `index` is staged with a document command's mutation, atomically
 * (dc_wal_apply's contract). DDL does not stage one and ignores it: an
 * index build commits catalog and index files but not the primary tree,
 * so a staged value would not persist with it anyway. Their replay is
 * bounded by the next snapshot's log compaction instead.
 *
 * `result` receives what applying the command produced, in the shape a
 * driver caller gets back -- dc_wal_apply's four for documents, and:
 *
 *   createIndex     -> { name }        the name the catalog chose
 *   dropIndex       -> { dropped }     always true; a missing one is a code
 *   dropCollection  -> { dropped }     false if there was nothing to drop
 *
 * A FIRST INSERT MAKES THE COLLECTION, here as on the wire. That is what
 * makes it safe for createCollection to have no command of its own: a
 * collection reaches a replica with its first document, and an empty
 * collection that was only ever named has nothing to carry.
 *
 * A DETERMINISTIC FAILURE IS RETURNED, NOT SWALLOWED -- a duplicate id,
 * a unique-index collision, an index that already exists. Every replica
 * applying the same prefix reaches the same verdict, so the apply pump
 * is what decides whether such a code is an answer for the client or a
 * halt (dc_is_deterministic, db_validate.h). Hiding it here would take
 * that decision away from the one layer that can make it.
 */
int dbs_apply(dbs *s, uint64_t index, const uint8_t *cmd, uint32_t len,
              dbuf *result);

/* Close every collection, the catalog, and the session. Safe on NULL.
 * Does not touch the namespace. */
void dbs_close(dbs *s);

/* ---- requests (db_request.c) ------------------------------------------- */

/*
 * Perform one request and append one response, both binjson objects.
 *
 * This is everything the server DECIDES. A transport reads a request,
 * calls this, and writes the response -- and, since change streams, also
 * asks dbs_stream_take for frames nobody requested. It never reads a
 * field of any of them,
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
 * A LIST OF WRITES ANSWERS IN ONE SHAPE. insertMany (`docs`) and
 * bulkWrite (`writes`) are different operations -- one list holds
 * documents, the other holds writes of six kinds -- but they fail the
 * same way, so both answer { ok, result, attempted, upserted, errors }.
 * A failed member is a RESULT, not a refusal, which is what makes
 * `ordered` mean anything; `attempted` says how much of the list ran,
 * because "never tried" and "tried and succeeded" are different answers
 * and nothing else in the response tells them apart. A malformed list is
 * refused whole, with `index` naming the operation that was wrong.
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
 * IDS STAY WITH THE CALLER, AND SO DOES THE CLOCK. `id` supplies the 12
 * bytes a write uses if it TURNS OUT to need one -- which is only ever
 * an upsert that matched nothing, because that is the only write whose
 * need for an id is unknowable until it has run. Generating them needs a
 * clock, which db.h's top comment keeps out of this layer deliberately.
 *
 * An INSERT is not one of them: its document carries its own `_id`, the
 * same rule insertMany applies to every member of its list. `id` is not
 * a second place to put it, because two places for one fact would need a
 * precedence rule between them. An insert without one is
 * DC_ERR_REQ_MISSING_FIELD.
 *
 * (This paragraph used to say `id` covered "an insert whose document has
 * no _id". It never did: dc_wal_plan_build refused such a document, and
 * the wire answered -2 "builder state error" -- a sentence about a
 * builder, for a request that was merely incomplete. The behaviour is
 * now what it always was and the sentence is what it always should have
 * been, at both layers.)
 *
 * `now` supplies the milliseconds an update carrying $currentDate is
 * rewritten against (upd_resolve_current_date, the same call every other
 * host makes before proposing a write). A write that needs either and
 * was not given it is DC_ERR_REQ_MISSING_FIELD, not a value invented
 * here.
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

/* ---- the replicated fork (db_request.c) ---------------------------------
 *
 * dbs_handle plans a write and applies it in one call, because on a
 * single-process server there is nobody to ask. A REPLICA has to ask:
 * the commands its plan produced are the log's to carry, and they may
 * not touch a file until a quorum holds them.
 *
 *     dbs_propose(s, client, req, len, &token, &cmds, &out)
 *         *token == 0  -- nothing to replicate. The answer is in `out`
 *                         already: a read, a ping, or a refusal.
 *         *token != 0  -- `cmds` is a binjson ARRAY of commands, in the
 *                         order they must be applied. Propose them.
 *     ... a quorum, and then the APPLY PUMP applies each committed
 *         entry with dbs_apply, exactly as it does on a follower ...
 *     dbs_step(s, token, indices, results, n, &token, &cmds, &out)
 *
 * THE PUMP IS THE ONLY THING THAT APPLIES. dbs_step does not touch the
 * database: it is handed what applying each command produced and
 * assembles the response from that. Any other arrangement has two places
 * performing a committed command -- the pump on every replica, and the
 * proposer again on the one that took the request -- which on a leader
 * means every local write lands twice. (It did, once. The second insert
 * came back a duplicate of the first.)
 *
 * PLANNING IS WHERE NONDETERMINISM DIES. An upsert's id and a
 * $currentDate are resolved once, by the node that took the request, and
 * travel in the command -- which is why the REQUEST is never what gets
 * replicated. Two replicas planning the same request separately would
 * resolve them differently and have forked.
 *
 * dbs_complete re-runs the request over the plan this session kept, so
 * the response is built by exactly the code that builds it on a
 * single-process server. `indices` are the log indices the commands
 * committed at, in the same order; dc_wal_apply stages each with its
 * mutation, so a replica's applied floor is durable with the data it
 * describes rather than beside it.
 *
 * A DETERMINISTIC FAILURE IS STILL AN ANSWER. A duplicate key refused at
 * apply is a result every replica computes identically (dc_is_deterministic,
 * db_validate.h), and it comes back in `out` like any other.
 *
 * BOUNDED, like every other table here: DBS_MAX_INFLIGHT requests may be
 * waiting at once and a further one is DC_ERR_TOO_MANY_CLIENTS -- a
 * refusal the caller can act on, rather than a plan nobody will finish.
 * dbs_abandon releases one whose client has gone; dbs_drop_client does
 * it for every request that client had in flight.
 */
int  dbs_propose(dbs *s, uint64_t client, const uint8_t *req, size_t req_len,
                 uint64_t *token, dbuf *cmds, dbuf *out);
/*
 * What applying one command produced: dbs_apply's result bytes, and its
 * return code -- because a deterministic failure is an ANSWER every
 * replica computes identically, and the response has to say so.
 */
typedef struct {
    int            rc;
    const uint8_t *data;
    uint32_t       len;
} dbs_result;

int  dbs_step(dbs *s, uint64_t token, const uint64_t *indices,
              const dbs_result *results, uint32_t n,
              uint64_t *next, dbuf *cmds, dbuf *out);
void dbs_abandon(dbs *s, uint64_t token);

/* How many planned requests are waiting to be completed. */
uint32_t dbs_inflight(const dbs *s);

/* How far a collection's applied index has got, or 0 if it is not open. */
uint64_t dbs_applied_index(dbs *s, const char *coll, size_t coll_len);

/* The replay floor: the highest index this database has applied, across
 * every collection. Apply is strictly ordered, so the max IS the applied
 * prefix -- what a restarting replica resumes from, and the one thing
 * about an apply that outlives the response. */
uint64_t dbs_applied_floor(dbs *s);

/* ---- what db_request.c uses to implement the two above ------------------
 *
 * Not a host's to call. They are here rather than in a private header
 * because db_request.c and db_session.c are one component in two files,
 * and a third file for four functions would be filing rather than
 * design.
 */
int          dbs_repl_active(const dbs *s);     /* mid-propose/step    */
int          dbs_repl_hold(dbs *s, dc_wal_plan *p);
dc_wal_plan *dbs_repl_resuming(dbs *s);
uint64_t     dbs_repl_next_index(dbs *s);
int          dbs_repl_applied(dbs *s, dbuf *result, int *rc);

/* Not an error: a write whose commands were planned and kept, waiting
 * for a quorum. Positive, so every `if (e)` in the write paths still
 * treats it as "not BJ_OK" while the three that must know can. */
#define DC_PENDING 1

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
