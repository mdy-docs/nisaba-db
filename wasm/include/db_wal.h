/*
 * db_wal.h — the write-ahead log's command grammar, and the planner that
 * turns a request into commands.
 *
 * A WAL entry must replay to the identical state on any replica and after
 * any crash, so every source of nondeterminism has to be resolved BEFORE
 * the entry is logged: the clock, the id generator, and — the one that
 * matters here — which documents a filter matches. src/db-wal.js has
 * always done that, but it did it in JavaScript, which meant the rules
 * lived in two places and one of them ran the query twice.
 *
 * WHAT MOVED, AND WHY IT IS NOT JUST A PORT
 *
 * The old grammar had two commands that carried a FILTER rather than an
 * id: `uu` (upsert-update) and `ru` (upsert-replace). Applying one
 * re-evaluated the filter against the state replay was in the middle of
 * reproducing. It worked, because replay is strictly ordered — but it is
 * a weaker guarantee than the log is supposed to give, and it cost a
 * second full query on every upsert (the proposer had already run one to
 * discover there was no match).
 *
 * dc_wal_plan resolves an upsert the whole way instead: no match means it
 * builds the document that the upsert would have inserted and logs a
 * plain INSERT. So the grammar loses two opcodes, and gains an invariant
 * worth more than either:
 *
 *   EVERY LOGGED DOCUMENT COMMAND NAMES EXACTLY ONE _id.
 *   No filter survives into the log; apply never runs a query.
 *
 * The document an upsert inserts is built by dc_upsert_document /
 * dc_replace_document (db.h) — the same functions dc_update_one and
 * dc_replace_one use for their own upserts. Planning and applying cannot
 * disagree about what an upsert means, because there is only one thing to
 * disagree with.
 *
 * THE COST THAT DISAPPEARS
 *
 *   before, matched upsert:  full query (proposer) + full query (apply)
 *   before, no-match upsert: full query (proposer) + full query (apply)
 *   after,  matched:         full query (planner)  + point lookup
 *   after,  no-match upsert: full query (planner)  + insert
 *
 * updateMany/deleteMany get the same treatment: the planner emits one
 * id-targeted command per match from the single scan it already ran,
 * where the host used to run its own find() first and then hand the ids
 * back across the bridge.
 *
 * WHAT DID NOT MOVE
 *
 * Appending to the log, sync(), and dispatching an applied command to the
 * collection stay in the host. Appending is EntryLog's job (already C,
 * driven by the host because a Raft node drives it differently from a
 * single-node WAL); dispatch is async and emits change events. What C
 * owns is the grammar — which opcodes exist, what fields each requires,
 * and what a request resolves to. dc_wal_parse gives the host the opcode
 * of a logged entry as a number, so the opcode strings appear in exactly
 * one file in the repository: this one's .c.
 */
#ifndef DB_WAL_H
#define DB_WAL_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "dbuf.h"
#include "db.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Continuing the DC_ERR_* range (db_catalog.h ends at -31). */
#define DC_ERR_WAL_UNKNOWN_OP     (-32)
#define DC_ERR_WAL_MISSING_FIELD  (-33)
#define DC_ERR_WAL_BAD_REQUEST    (-34)
/* dc_wal_apply was handed a command it does not apply -- one of the DDL
 * three, whose apply creates and destroys FILES and therefore belongs to
 * whoever owns the namespace. Not a malformed command: a misrouted one. */
#define DC_ERR_WAL_NOT_APPLIABLE  (-35)

/*
 * Opcodes, as they appear in a log entry's `op` field. The wire spellings
 * live in db_wal.c's OP_NAME table and nowhere else.
 *
 *   INSERT           { c, op:"i",  doc }              doc._id resolved
 *   UPDATE           { c, op:"u",  id, update }       update ops, one document
 *   REPLACE          { c, op:"r",  id, doc }
 *   DELETE           { c, op:"d",  id }
 *   CREATE_INDEX     { c, op:"createIndex", keys, options }
 *   DROP_INDEX       { c, op:"dropIndex", name }
 *   DROP_COLLECTION  { c, op:"dropCollection" }
 *
 * The three DDL opcodes are logged (so replicas and crash replay perform
 * them too) and applied idempotently rather than under the appliedIndex
 * guard — see src/db-wal.js's _applyCommand for that reasoning, which is
 * unchanged.
 */
typedef enum {
    DC_WAL_INSERT          = 0,
    DC_WAL_UPDATE          = 1,
    DC_WAL_REPLACE         = 2,
    DC_WAL_DELETE          = 3,
    DC_WAL_CREATE_INDEX    = 4,
    DC_WAL_DROP_INDEX      = 5,
    DC_WAL_DROP_COLLECTION = 6
} dc_wal_op;

/*
 * What the host asked for, before anything is resolved. The `a`/`b`
 * arguments to dc_wal_plan mean something different per request:
 *
 *   request           a                      b              upsert?
 *   INSERT_ONE        doc (OBJECT, _id set)  —              no
 *   INSERT_MANY       docs (ARRAY)           —              no
 *   UPDATE_ONE        filter                 update         yes
 *   UPDATE_MANY       filter                 update         yes
 *   REPLACE_ONE       filter                 replacement    yes
 *   DELETE_ONE        filter                 —              no
 *   DELETE_MANY       filter                 —              no
 *   CREATE_INDEX      keys                   options        no
 *   DROP_INDEX        index name (raw UTF-8) —              no
 *   DROP_COLLECTION   —                      —              no
 *
 * $currentDate must already be resolved in `update` (upd_resolve_current_
 * date, called by the host, which owns the clock); the planner does not
 * read a clock and has none to read.
 */
typedef enum {
    DC_WREQ_INSERT_ONE      = 0,
    DC_WREQ_INSERT_MANY     = 1,
    DC_WREQ_UPDATE_ONE      = 2,
    DC_WREQ_UPDATE_MANY     = 3,
    DC_WREQ_REPLACE_ONE     = 4,
    DC_WREQ_DELETE_ONE      = 5,
    DC_WREQ_DELETE_MANY     = 6,
    DC_WREQ_CREATE_INDEX    = 7,
    DC_WREQ_DROP_INDEX      = 8,
    DC_WREQ_DROP_COLLECTION = 9
} dc_wal_req;

/* dc_wal_plan_outcome. NOTHING means the request resolved to no commands
 * at all: a non-upsert write that matched nothing, which must never reach
 * the log (an entry that does nothing is still an entry every replica has
 * to store, ship and replay). */
#define DC_PLAN_NOTHING 0
#define DC_PLAN_MATCHED 1
#define DC_PLAN_UPSERT  2

typedef struct dc_wal_plan dc_wal_plan;

/*
 * Resolve a request into the exact commands to log, running at most one
 * query to do it. `c` may be NULL for requests that touch no documents
 * (the DDL three, and INSERT_ONE/INSERT_MANY, whose ids the host already
 * assigned). `coll` names the collection in every emitted command.
 *
 * `default_id` is the id to use if the request turns out to need one --
 * the host generates it unconditionally because whether it is needed is
 * only knowable after the match, and generating ids stays in the host
 * (db.h's top comment). An unneeded one costs 12 bytes and is discarded.
 *
 * On BJ_OK the caller owns *out and must dc_wal_plan_free it, INCLUDING
 * when the outcome is DC_PLAN_NOTHING. On error *out is NULL.
 */
int dc_wal_plan_build(dc_collection *c, const char *coll, uint32_t coll_len,
                int req,
                const uint8_t *a, uint32_t a_len,
                const uint8_t *b, uint32_t b_len,
                int upsert, const uint8_t default_id[12],
                dc_wal_plan **out);

/* DC_PLAN_NOTHING / _MATCHED / _UPSERT. */
int dc_wal_plan_outcome(const dc_wal_plan *p);

/* The commands, in the order they must be appended -- one log entry each,
 * because one entry is one collection commit (src/db-wal.js's top
 * comment). dc_wal_plan_cmd returns NULL for an out-of-range index. The
 * returned pointer is owned by the plan and dies with it. */
uint32_t dc_wal_plan_count(const dc_wal_plan *p);
const uint8_t *dc_wal_plan_cmd(const dc_wal_plan *p, uint32_t i, uint32_t *len);

/*
 * The single document the request matched, as it was BEFORE the plan's
 * commands are applied -- what findOneAndUpdate/findOneAndReplace/
 * findOneAndDelete return for `returnDocument: 'before'`. NULL unless the
 * request was one of the three single-document match-then-write forms and
 * it matched something. The planner already had this document in hand
 * (it is how the target id was resolved), so returning it costs nothing
 * and saves the host the findOne it used to run for exactly this.
 */
const uint8_t *dc_wal_plan_preimage(const dc_wal_plan *p, uint32_t *len);

/*
 * The 12 id bytes the plan resolved: the matched document's for a
 * single-document write, the upserted document's for DC_PLAN_UPSERT.
 * NULL when there is no single such id (DC_PLAN_NOTHING, the many-forms,
 * DDL). The host reports it as `upsertedId`.
 */
const uint8_t *dc_wal_plan_target_id(const dc_wal_plan *p);

void dc_wal_plan_free(dc_wal_plan *p);

/*
 * Validate one logged command and report its opcode, so a host dispatches
 * on a number it cannot mistype rather than on a string it must keep in
 * sync with this file. Checks that every field the opcode requires is
 * present and correctly typed; DC_ERR_WAL_UNKNOWN_OP for an `op` this
 * version does not know, DC_ERR_WAL_MISSING_FIELD for a truncated one.
 *
 * The command's VALUES are deliberately not returned. The host decodes
 * the payload anyway -- it has to, to hand documents to an async
 * collection method -- so handing back spans it would decode a second
 * time would be waste dressed as an interface. What C owns here is the
 * definition of a command, not the transport of its contents.
 *
 * `*coll` / `*coll_len` receive the collection name, pointing into `buf`.
 */
int dc_wal_parse(const uint8_t *buf, uint32_t len,
                 int *op_out, const uint8_t **coll, uint32_t *coll_len);

/* ---- applying a logged command ------------------------------------------ */

/*
 * Does dc_wal_apply drive this opcode? The four document ops, yes; the
 * DDL three, no -- they make and unmake files, which is the namespace
 * owner's job, not this layer's.
 */
int dc_wal_is_document(int op);

/*
 * Apply one logged command to `c`, an ALREADY-OPEN collection for the
 * name the command carries. This is the other end of dc_wal_plan_build:
 * the planner said what to write down, and this performs what was
 * written down, with nothing in between that a host could get wrong.
 *
 * It does the three things the apply loop owes each entry, in the one
 * order that survives a crash:
 *
 *   1. stage `index` (dc_set_applied_index), so the mutation's own commit
 *      persists "this entry has been applied" atomically WITH its effect.
 *      Pass 0 for a collection that is not log-driven and it is skipped.
 *   2. perform the mutation, addressed by the _id the command names --
 *      never by a filter, because the planner resolved every command to
 *      exactly one id and apply must never run a query (db_wal.h's top
 *      comment). Upsert is off for the same reason: an upsert that found
 *      no match was resolved into a plain insert before it was logged.
 *   3. append the RESULT to `result` as a binjson object, in the shape a
 *      caller of the driver gets back.
 *
 * The result shape is C's rather than a host decoration because, under
 * replication, the result of applying a command IS part of the command's
 * semantics: every replica computes it, and the leader hands its copy to
 * the client. Two hosts that shaped it differently would be two clusters
 * that answer the same committed write differently.
 *
 *   i  -> { acknowledged, insertedId }
 *   u  -> { acknowledged, matchedCount, modifiedCount, upsertedId: null }
 *   r  -> the same as u
 *   d  -> { acknowledged, deletedCount }
 *
 * A deterministic failure (a duplicate id, a unique-index collision) is
 * returned as its DC_ERR_* code with nothing appended to `result`: it is
 * a fact about the command and the state it landed on, which every
 * replica reaches identically (db_validate.h's dc_is_deterministic).
 * DC_ERR_WAL_NOT_APPLIABLE for a DDL command.
 */
int dc_wal_apply(dc_collection *c, uint64_t index,
                 const uint8_t *cmd, uint32_t len, dbuf *result);

#ifdef __cplusplus
}
#endif

#endif /* DB_WAL_H */
