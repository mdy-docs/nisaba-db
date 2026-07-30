/*
 * db.h — document-collection CRUD, secondary-index maintenance and filter
 * matching on top of B+ trees (bplustree.h).
 *
 * A collection is a `dc_collection`: one primary bpt (documents keyed by the
 * raw 12-byte ObjectId, an opaque byte-string key) plus zero or more
 * attached secondary indexes, each its own bpt keyed by a composite of
 * ordered field values + an id suffix (keyenc.h), per the convention
 * documented in bplustree.h. `dc_collection` only *coordinates* already-open
 * bpt handles — every bpt (primary and each index) is opened/closed by the
 * host (JS), exactly as milestone 1's plain `bpt*` was.
 *
 * _id is a client-side concern, like textlog's ts_ms: the caller supplies a
 * document whose top-level `_id` field is already an OID — dc_insert_one
 * extracts it as the key and errors if it is missing or the wrong type. This
 * mirrors how MongoDB drivers generate _id before the wire write rather than
 * the storage engine inventing it, and keeps randomness/clock access (which
 * WASM has no portable source for) out of C entirely.
 *
 * Filters are matched by db_query.h's operator-aware evaluator ($eq/$ne/$gt/
 * $gte/$lt/$lte/$in/$nin/$exists/$not/$and/$or/$nor, dotted field paths,
 * implicit array-element matching — see db_query.h for the exact rules and
 * deliberate omissions). dc_find additionally applies sort/skip/limit/
 * projection (db_query.h again) to the matched set.
 *
 * dc_find/dc_find_one/dc_count/dc_update_one/dc_update_many use an attached
 * index instead of a full collection scan when `filter`'s top level is a
 * pure AND of bare-value/{$eq: v} conditions that together pin every field
 * of some index (see plan_equality_index in db.c) — an equality-only
 * planner; range conditions ($gt et al.) and filters combined with
 * $and/$or/$nor at the top level always fall back to a full scan for now.
 * Every path re-applies the *full* filter to whatever candidate set it
 * gathers, so correctness never depends on which plan was chosen — only
 * speed does.
 *
 * dc_update_one/dc_update_many apply db_update.h's $set/$unset/$inc/$push/
 * $pull operators (top-level fields only) instead of replacing the whole
 * document — see db_update.h for the exact rules and deliberate omissions.
 *
 * Besides the composite-key equality index from milestone 2, a collection
 * may attach a *text* index (single field, backed by a TextIndex's three
 * trees — textindex.h) or a *geo* index (single field, backed by an rtree
 * — rtree.h, GeoJSON Point values only: {type:"Point", coordinates:
 * [lng, lat]}). A collection may have at most one text index (matching
 * MongoDB's own restriction). dc_find/dc_find_one/dc_count/dc_update_many
 * dispatch to whichever index a filter's `$text`/`$near`/`$geoWithin`
 * clause names *before* trying the equality planner — see
 * resolve_special_source in db.c for the exact recognized shapes
 * ({$text: {$search: "..."}}, {field: {$near: {$geometry: {...},
 * $maxDistance: km}}}, {field: {$geoWithin: {$box: [[minLng,minLat],
 * [maxLng,maxLat]]}}} or {$geoWithin: {$center: [[lng,lat], radiusKm]}}).
 * $near/$geoWithin distances are in **kilometers**, not meters — a
 * deliberate deviation from real MongoDB (which uses meters/radians
 * depending on the operator) chosen for consistency with rtree.h's own
 * km-based API; $near and $geoWithin both require an attached geo index on
 * the named field (BJ_ERR_STATE otherwise) — real MongoDB only requires
 * one for $near, but requiring it for $geoWithin too avoids duplicating
 * point-in-shape math in db_query.c for what is, in practice, an uncommon
 * unindexed-geo-scan use case.
 *
 * Crash atomicity (milestone 5): a host that supplies a journal via
 * dc_collection_recover gets every document write (dc_insert_one/
 * dc_delete_one/dc_replace_one/dc_update_one, and each matched document
 * within dc_update_many) made atomic across the primary tree and every
 * currently attached index's file(s) — a crash between updating the primary
 * tree and an index tree rolls back to the last consistent write on reopen,
 * rather than leaving them permanently out of sync. This generalizes
 * textindex.c's fixed-3-tree journal (docs/textindex-atomicity.md) to a
 * variable number of files; see dc_collection_recover's doc comment below
 * and docs/db-plan.md milestone 5 for the full design.
 *
 * In-process atomicity: the same writes are also atomic against *failure
 * without a crash* — an error partway through (an index rejecting a
 * document, a unique-key conflict discovered after the old entries were
 * cleared, OOM) rewinds every file to its pre-operation length before the
 * error is returned (mut_begin/mut_end in db.c, built on the same
 * bpt_rewind primitive recovery uses), so the live session never observes
 * a half-applied write. This holds with or without a journal — the rewind
 * lengths come from the live trees. Scope note: this is
 * per-document-write atomicity, not multi-document ACID transactions/
 * sessions — dc_update_many's documents are not atomic *with each other*,
 * matching real MongoDB's own non-session updateMany semantics. Index
 * *creation* (dc_collection_add_index and friends) keeps its own pre-
 * existing all-or-nothing bookkeeping-rollback story, unrelated to and
 * unchanged by the journal. journal == NULL (dc_collection_recover never
 * called, or called with NULL) disables journaling entirely — the pre-
 * milestone-5 behavior.
 *
 * All operations return BJ_OK (0) or a negative BJ_ERR_* / DC_ERR_* code.
 */
#ifndef DB_H
#define DB_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "bplustree.h"
#include "rtree.h"
#include "textindex.h"
#include "db_query.h"
#include "db_update.h"

#ifdef __cplusplus
extern "C" {
#endif

/* insertOne (or a replaceOne upsert) targets an _id that already exists. */
#define DC_ERR_DUPLICATE   (-10)
/* replaceOne's replacement document names an _id different from the
 * document `filter` matched. */
#define DC_ERR_ID_MISMATCH (-11)
/* A write's field values collide with another document's on a unique
 * index (milestone 9). */
#define DC_ERR_DUPLICATE_KEY (-12)
/* A document is missing a field required by a non-sparse equality index —
 * on write maintenance or createIndex backfill. The rejection itself is
 * the documented all-or-nothing contract (create the index with `sparse`
 * to skip such documents); this code exists so it surfaces as what it is
 * rather than as keyenc.h's generic BJ_ERR_STATE. */
#define DC_ERR_MISSING_INDEXED_FIELD (-13)
/* An indexed field's value has no order-preserving key encoding
 * (keyenc.h: number/string/Date only; no NaN, no strings containing
 * U+0000). Same rationale as DC_ERR_MISSING_INDEXED_FIELD. */
#define DC_ERR_UNINDEXABLE_VALUE (-14)

typedef struct dc_collection dc_collection;

/* Wrap an already-open primary bpt as a collection with no indexes attached
 * yet. Returns NULL on OOM. Does not take ownership of `primary`. */
dc_collection *dc_collection_open(bpt *primary);
/* Free the collection's own bookkeeping (index registrations). Does not
 * close/free the primary tree or any attached index tree — those are owned
 * by the host. Safe on NULL. */
void dc_collection_free(dc_collection *c);

/*
 * Register `index_tree` (already open, caller-owned, expected to already
 * hold exactly the composite-key entries for every document currently in
 * `c`) as a secondary index named `name`, keyed by the ordered fields named
 * in `fields` (a binjson ARRAY of at least one STRING, in composite-key
 * order) — for reattaching an index that was already built, e.g. on
 * collection reopen. Use dc_collection_add_index instead to create and
 * backfill a brand-new index. BJ_ERR_STATE if `name` is already registered
 * or `fields` is empty/malformed.
 *
 * `unique` rejects a write whose field values collide with another
 * document's already in the index (DC_ERR_DUPLICATE_KEY); `sparse` skips
 * (does not index, does not error) a document missing one of `fields`,
 * instead of the default all-or-nothing behavior; `partial_filter` (a
 * binjson OBJECT filter, db_query.h's matcher, or NULL for "always
 * applies") skips a document that doesn't match it. A document skipped by
 * either option can never violate `unique` on this index (matches real
 * MongoDB's sparse/partial + unique semantics).
 */
int dc_collection_attach_index(dc_collection *c, const char *name, int name_len,
                               bpt *index_tree,
                               const uint8_t *fields, uint32_t fields_len,
                               int unique, int sparse,
                               const uint8_t *partial_filter, uint32_t partial_filter_len);

/*
 * Enable/disable the collection's cross-file commit journal and reconcile it
 * against the primary tree + every currently attached index. Must be called
 * once, after every dc_collection_attach_index/_attach_text_index/
 * _attach_geo_index call for this collection has already run (mirrors
 * textindex.h's tix_recover contract: right after every file is open) and
 * before any write. journal == NULL disables journaling (the default set by
 * dc_collection_open). Empty/absent journal: BJ_OK, adopt state as-is. A
 * crash that lost more committed data than the journal can reconcile:
 * BJ_ERR_STATE (refuses, matching tix_recover's own refusal case). See
 * db.h's top comment and docs/db-plan.md milestone 5.
 */
int dc_collection_recover(dc_collection *c, const bj_io *journal);

/*
 * Replicated-log integration (mirrors the per-structure contract in
 * bplustree.h/rtree.h): the last log index applied to this collection.
 *
 * dc_set_applied_index stages `index` onto the primary tree and every
 * attached index structure, so each file's next commit persists it in
 * that file's own metadata record, atomically with the mutation it
 * belongs to. The apply loop calls it once per log entry, immediately
 * before the entry's mutation. The value never decreases (BJ_ERR_STATE),
 * and staging is refused while any structure is a snapshot; refusal is
 * checked against every structure before anything is staged, so an error
 * never leaves the collection partially staged.
 *
 * dc_applied_index reads the PRIMARY tree's persisted/staged value --
 * the primary is the authority because every document mutation commits
 * it, while an index file a mutation didn't touch lags until its own
 * next commit (staged values are sticky, so it catches up then). That
 * lag is safe: the cross-file commit journal (dc_collection_recover)
 * rewinds every file to one consistent commit on crash recovery, so the
 * collection either fully has an entry's mutation or fully lacks it --
 * replay from dc_applied_index + 1 is exact, never double-applying.
 * 0 = the collection is not log-driven.
 */
uint64_t dc_applied_index(const dc_collection *c);
int      dc_set_applied_index(dc_collection *c, uint64_t index);

/*
 * Like dc_collection_attach_index, but also backfills `index_tree` (expected
 * empty) against every document already in `c`'s primary tree before
 * attaching it for ongoing maintenance — for creating a brand-new index.
 * All-or-nothing unless `sparse`/`partial_filter` says otherwise: a
 * document missing one of `fields` (and not skipped by `sparse`), holding
 * a non-number/string value for one, or colliding with another document's
 * values on a `unique` index, fails the whole call
 * (DC_ERR_MISSING_INDEXED_FIELD / DC_ERR_UNINDEXABLE_VALUE /
 * DC_ERR_DUPLICATE_KEY) and leaves `c` without the index registered (the
 * caller should discard `index_tree`'s file) — matching MongoDB's own
 * index-build failure behavior, including refusing to build a `unique`
 * index over pre-existing duplicate values.
 */
int dc_collection_add_index(dc_collection *c, const char *name, int name_len,
                            bpt *index_tree,
                            const uint8_t *fields, uint32_t fields_len,
                            int unique, int sparse,
                            const uint8_t *partial_filter, uint32_t partial_filter_len);

/*
 * Like dc_collection_attach_index, but for a single-field *text* index
 * backed by an already-open TextIndex's three trees (textindex.h) —
 * expected to already hold exactly the postings for every document
 * currently in `c`. BJ_ERR_STATE if `name` is already registered or `c`
 * already has a text index (MongoDB allows at most one per collection).
 */
int dc_collection_attach_text_index(dc_collection *c, const char *name, int name_len,
                                    bpt *tix_index, bpt *tix_doc_terms, bpt *tix_doc_lengths,
                                    const char *field, int field_len);

/*
 * Like dc_collection_add_index, but for a text index: attaches it (as
 * dc_collection_attach_text_index) and backfills it against every existing
 * document. Documents whose `field` is missing or not a string are
 * silently skipped (not indexed) rather than failing the whole call —
 * MongoDB's own text-index behavior, unlike the equality index's
 * all-or-nothing validation.
 */
int dc_collection_add_text_index(dc_collection *c, const char *name, int name_len,
                                 bpt *tix_index, bpt *tix_doc_terms, bpt *tix_doc_lengths,
                                 const char *field, int field_len);

/*
 * Like dc_collection_attach_index, but for a single-field *geo* index
 * backed by an already-open rtree (rtree.h) — expected to already hold
 * exactly the points for every document currently in `c`.
 */
int dc_collection_attach_geo_index(dc_collection *c, const char *name, int name_len,
                                   rtree *rt, const char *field, int field_len);

/*
 * Like dc_collection_add_index, but for a geo index: attaches it (as
 * dc_collection_attach_geo_index) and backfills it against every existing
 * document. Documents whose `field` is missing are silently skipped (not
 * indexed); a present-but-malformed GeoJSON Point value fails the whole
 * call (BJ_ERR_STATE), matching a real 2dsphere index's validation.
 */
int dc_collection_add_geo_index(dc_collection *c, const char *name, int name_len,
                                rtree *rt, const char *field, int field_len);

/* Unregister a previously attached/added index by name. Does not touch its
 * tree(s) (the host closes/deletes its file(s)). BJ_ERR_STATE if no such
 * index. */
int dc_collection_remove_index(dc_collection *c, const char *name, int name_len);

/*
 * Every document whose indexed fields equal `values` (a binjson ARRAY of
 * scalars, same count and order as index `name`'s fields), found via an
 * O(log n + k) range scan of the index rather than a collection scan.
 * Writes a freshly malloc'd binjson ARRAY of documents through
 * *out / *out_len (caller frees). BJ_ERR_STATE if no such index or `values`
 * doesn't match its field count.
 */
int dc_collection_find_by_index(dc_collection *c, const char *name, int name_len,
                                const uint8_t *values, uint32_t values_len,
                                uint8_t **out, size_t *out_len);

/*
 * The 12-byte `_id` of a document. BJ_ERR_STATE if the top-level `_id` is
 * missing or is not an OID. `dc_document_id_opt` instead reports absence
 * through *has (0/1) and only errors on a present-but-wrong-typed one --
 * the distinction replaceOne needs, where an absent _id is legal and a
 * string one is not.
 */
int dc_document_id(const uint8_t *doc, uint32_t doc_len, uint8_t id_out[12]);
int dc_document_id_opt(const uint8_t *doc, uint32_t doc_len, uint8_t id_out[12], int *has);

/*
 * The document an upsert WOULD insert, without inserting it: for an
 * update, `filter`'s bare top-level equality conditions seeded through
 * `update`'s operators (see build_upsert_seed in db.c); for a replace,
 * `replacement` itself. Both then carry an `_id` -- `default_id` for the
 * update form, `replacement`'s own if it has one for the replace form.
 * Writes a freshly malloc'd OBJECT through *out / *out_len (caller frees).
 *
 * dc_update_one/dc_update_many/dc_replace_one build their upserted
 * document by calling these, so there is exactly one definition of what
 * an upsert inserts. That matters because db_wal.h's planner calls them
 * too, at proposal time, to turn an upsert into a plain insert command
 * (db_wal.h explains why) -- and plan and apply agreeing is not a
 * property to test for, it is one to make unrepresentable.
 */
int dc_upsert_document(const uint8_t *filter, uint32_t filter_len,
                       const uint8_t *update, uint32_t update_len,
                       const uint8_t default_id[12],
                       uint8_t **out, size_t *out_len);
int dc_replace_document(const uint8_t *replacement, uint32_t replacement_len,
                        const uint8_t default_id[12],
                        uint8_t **out, size_t *out_len);

/*
 * Insert `doc` (a binjson OBJECT whose top-level `_id` is an OID — BJ_ERR_
 * STATE if missing or not an OID) into `c`'s primary tree and every
 * attached index. DC_ERR_DUPLICATE if that id already exists.
 */
int dc_insert_one(dc_collection *c, const uint8_t *doc, uint32_t doc_len);

/*
 * Insert every document in `docs` (a binjson ARRAY; each document already
 * _id-assigned by the caller, exactly like dc_insert_one's contract). If
 * `ordered`, stops at the first failing document, so the result array can
 * end up shorter than `docs`; otherwise every document is attempted
 * regardless of earlier failures. Writes a binjson ARRAY of one INT error
 * code per attempted document (BJ_OK on success) through *out / *out_len.
 * insertedIds stay a client-side concern (this file's top comment) — the
 * caller already generated each id before calling, so results only need to
 * report success/failure per index.
 */
int dc_insert_many(dc_collection *c, const uint8_t *docs, uint32_t docs_len,
                   int ordered, uint8_t **out, size_t *out_len);

/*
 * First document matching `filter` (a binjson OBJECT; {} matches every
 * document), with `projection` (may be NULL/0 for none, same shape as
 * dc_find/dc_cursor_open's) applied to it. *found is 1/0; when found,
 * writes a freshly malloc'd copy of the (projected, if requested)
 * document through *out / *out_len (caller frees). filter == {_id: <oid>}
 * is always an O(log n) bpt_search; otherwise see db.h's top comment for
 * when an attached index is used.
 */
int dc_find_one(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                const uint8_t *projection, uint32_t projection_len,
                int *found, uint8_t **out, size_t *out_len);

/*
 * Every document matching `filter`, as a binjson ARRAY of documents (not
 * {key,value} pairs), with `opts` (may be NULL for none) applied — see
 * db_query.h for sort/skip/limit/projection semantics. Writes a freshly
 * malloc'd buffer through *out / *out_len (caller frees).
 */
int dc_find(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
            const qry_options *opts, uint8_t **out, size_t *out_len);

/*
 * Report which candidate source the shared query dispatch would use for
 * `filter`, without executing anything -- consults the same planners the
 * queries run, so it cannot drift. *kind_out: 0 full scan, 1 {_id} point
 * lookup, 2 equality index, 3 text index, 4 geo index. For kinds 2-4 the
 * serving index's name is written through *name_out / *name_len_out
 * (malloc'd; caller frees); NULL otherwise.
 */
int dc_explain(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
               int *kind_out, uint8_t **name_out, size_t *name_len_out);

/*
 * A resumable, bounded-memory cursor over documents matching `filter`
 * (opened via dc_cursor_open, advanced in batches via
 * dc_cursor_next_batch, released via dc_cursor_close). Unlike dc_find,
 * this does not materialize every match up front -- opaque, defined in
 * db.c.
 *
 * No sort support: an arbitrary in-memory sort fundamentally needs every
 * match before it can emit the first ordered result (the same reason
 * unindexed sorts in most databases carry a buffered-sort memory
 * ceiling). Callers needing a sorted result should use dc_find instead.
 *
 * Not safe across a concurrent compact() on the same collection while
 * the cursor is open -- compaction can rewrite the file the cursor's
 * underlying B+ tree scan is positioned against. Fine for the
 * within-one-request lifetime dc_find already has; a cursor meant to
 * outlive a single request (paged over multiple separate calls, as the
 * cloud service's REST cursor protocol does) needs the caller to keep
 * that window short or avoid compacting a collection with cursors open
 * against it. Not enforced at this C layer; the JS Collection.compact()
 * refuses to run while it has open cursors (wasm/nisaba-wasm.js,
 * docs/compaction.md).
 */
typedef struct dc_cursor dc_cursor;

/*
 * Open a cursor over every document in `c` matching `filter`, with
 * `projection` (may be NULL/0) applied to each returned document.
 * `skip`/`limit` (0 = none/unlimited, matching qry_options's convention)
 * are honored incrementally as the cursor advances rather than up front.
 */
int dc_cursor_open(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                   const uint8_t *projection, uint32_t projection_len,
                   int64_t skip, int64_t limit, dc_cursor **out_cursor);

/*
 * Advance the cursor, collecting up to `max_count` further matching
 * documents (each already projected) into a freshly built binjson ARRAY
 * through *out / *out_len (caller frees). *out_done is set to 1 once the
 * underlying source is exhausted or the cursor's limit has been reached
 * -- the cursor releases its underlying scan resources at that point
 * (safe to still call dc_cursor_close afterward; also safe to call
 * dc_cursor_next_batch again on an already-done cursor, a no-op
 * returning an empty array with *out_done still 1).
 */
int dc_cursor_next_batch(dc_cursor *cur, uint32_t max_count,
                         uint8_t **out, size_t *out_len, int *out_done);

/* Release a cursor's resources. Safe to pass NULL. */
void dc_cursor_close(dc_cursor *cur);

/* Delete the first document matching `filter`, from the primary tree and
 * every attached index. *deleted is 1/0. */
int dc_delete_one(dc_collection *c, const uint8_t *filter, uint32_t filter_len, int *deleted);

/* Delete every document matching `filter`, from the primary tree and every
 * attached index, committing the journal once per deleted document (same
 * per-document granularity as dc_update_many). *deleted_count is the
 * number removed.
 *
 * `ids`, when non-NULL, receives a binjson ARRAY of every removed
 * document's _id -- freshly malloc'd for the caller to free, and recorded
 * only after each delete commits. Same reasoning as dc_update_many's
 * `images`: a change-stream consumer needs exactly these, and this loop
 * already has each one, so the host need not re-query for them. A delete
 * event carries no full document, so ids are all that is collected. */
int dc_delete_many(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                   int64_t *deleted_count, uint8_t **ids, size_t *ids_len);

/*
 * Atomically find the first document matching `filter` and delete it.
 * *found/out/out_len follow dc_find_one's convention exactly — the
 * deleted document's pre-image, or not-found.
 */
int dc_find_one_and_delete(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                           int *found, uint8_t **out, size_t *out_len);

/*
 * Replace the first document matching `filter` with `replacement` (a
 * binjson OBJECT), updating every attached index to match. The stored
 * document keeps the matched document's _id regardless of what
 * `replacement` carries for _id, unless `replacement` names a *different*
 * OID, which fails with DC_ERR_ID_MISMATCH.
 *
 * No match: `upsert` 0 is a no-op. `upsert` 1 inserts `replacement` — using
 * its own `_id` field if it has one, otherwise `default_id` (the host
 * supplies this, generated the same way an insertOne id would be, since
 * whether it will actually be needed is only known after the match here).
 *
 * *result is written to 0 (no match, no upsert), 1 (matched and replaced),
 * or 2 (upserted).
 */
int dc_replace_one(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                   const uint8_t *replacement, uint32_t replacement_len,
                   const uint8_t default_id[12], int upsert, int *result);

/*
 * Atomically find the first document matching `filter` and replace it with
 * `replacement`, exactly like dc_replace_one, but returns the document
 * instead of a result code: the pre-image if `return_new` is 0, the
 * post-image if 1. upsert/default_id follow dc_replace_one's convention.
 * *found is 1 iff *out holds a meaningful document — 0 when there was no
 * match and no upsert, OR return_new is 0 and upsert created a document
 * (no "before" state to return, matching real MongoDB: a
 * returnDocument:'before' findOneAndReplace with upsert:true and no match
 * returns null).
 */
int dc_find_one_and_replace(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                            const uint8_t *replacement, uint32_t replacement_len,
                            const uint8_t default_id[12], int upsert, int return_new,
                            int *found, uint8_t **out, size_t *out_len);

/*
 * Apply `update` (db_update.h: an OBJECT of $set/$unset/$inc/$push/$pull
 * operators, top-level fields only) to the first document matching
 * `filter`, updating every attached index to match. `upsert`/`default_id`
 * and *result follow dc_replace_one's convention exactly (0/1/2), except
 * the upserted document's seed comes from `filter`'s bare top-level
 * equality conditions (matching MongoDB's own upsert-from-filter
 * behavior) rather than being supplied by the caller — see
 * build_upsert_seed in db.c.
 */
int dc_update_one(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                  const uint8_t *update, uint32_t update_len,
                  const uint8_t default_id[12], int upsert, int *result);

/*
 * Atomically find the first document matching `filter` and apply `update`
 * to it, exactly like dc_update_one, but returns the document instead of a
 * result code: the pre-image if `return_new` is 0, the post-image if 1.
 * upsert/default_id follow dc_update_one's convention. *found follows
 * dc_find_one_and_replace's convention exactly (see its comment).
 */
int dc_find_one_and_update(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                           const uint8_t *update, uint32_t update_len,
                           const uint8_t default_id[12], int upsert, int return_new,
                           int *found, uint8_t **out, size_t *out_len);

/*
 * Like dc_update_one, but applies `update` to *every* matching document.
 * *matched_count is the number of documents matched (0 if none and
 * `upsert` inserted one instead, in which case *upserted is written 1).
 * This implementation does not detect no-op updates (e.g. $set to the
 * field's current value): every matched document counts as modified, so a
 * caller-facing modifiedCount can simply mirror *matched_count.
 *
 * `images`, when non-NULL, receives a binjson ARRAY of every document's
 * POST-image -- the updated document, or the inserted one on an upsert --
 * freshly malloc'd for the caller to free. Pass NULL to skip building it,
 * which is the normal case.
 *
 * It exists because a change-stream consumer needs exactly those
 * documents, and this loop already has each one in hand: without it the
 * host had to re-read every matched document afterwards, one query each,
 * a cost its own comment described as "O(matched) extra round trips".
 * Collecting here is free.
 */
int dc_update_many(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                   const uint8_t *update, uint32_t update_len,
                   const uint8_t default_id[12], int upsert,
                   int64_t *matched_count, int *upserted,
                   uint8_t **images, size_t *images_len);

/* Count of documents matching `filter` ({} is bpt_size of the primary tree,
 * O(1)). */
int dc_count(dc_collection *c, const uint8_t *filter, uint32_t filter_len, int64_t *out_count);

/*
 * Unique values of `field` (a dot-separated path — db_query.h's field-path
 * resolution rules) across every document matching `filter`, as a binjson
 * ARRAY. Documents missing the field contribute nothing (no synthetic
 * null for a merely-absent field, matching real MongoDB's distinct()); an
 * array field's elements are the candidates, not the array itself.
 * Uniqueness is exact encoded-byte equality, the same rationale as every
 * other equality in this codebase. Writes a freshly malloc'd buffer
 * through *out / *out_len (caller frees).
 */
int dc_distinct(dc_collection *c, const char *field, int field_len,
                const uint8_t *filter, uint32_t filter_len,
                uint8_t **out, size_t *out_len);

#ifdef __cplusplus
}
#endif

#endif /* DB_H */
