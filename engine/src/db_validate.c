/*
 * db_validate.c — see db_validate.h.
 */
#include "db_validate.h"
#include "db_names.h"
#include "db_bulk.h"
#include "db_agg.h"
#include "db_update.h"
#include "db_catalog.h"
#include "db_wal.h"
#include "db_session.h"
#include "db_instance.h"
#include "db.h"
#include "raft_core.h"
#include "raft_msg.h"
#include "raft_node.h"
#include "bjcursor.h"

#include <string.h>

const char *dc_strerror(int code) {
    switch (code) {
        case BJ_OK:                 return "ok";
        /* binjson.h */
        case BJ_ERR_OOM:            return "out of memory";
        case BJ_ERR_STATE:          return "builder state error";
        case BJ_ERR_EOF:            return "Unexpected end of data";
        case BJ_ERR_UNKNOWN_TYPE:   return "Unknown type byte";
        case BJ_ERR_INT_RANGE:      return "Decoded integer exceeds safe range";
        case BJ_ERR_POINTER_RANGE:  return "Pointer offset out of valid range";
        case BJ_ERR_DEPTH:          return "Maximum nesting depth exceeded";
        case BJ_ERR_VERIFY:         return "Structural invariant violated";
        case BJ_ERR_RANGE:          return "Argument out of range";
        /* db.h */
        case DC_ERR_DUPLICATE:      return "Duplicate _id";
        case DC_ERR_ID_MISMATCH:
            return "replaceOne cannot change the _id of an existing document";
        case DC_ERR_DUPLICATE_KEY:
            return "Duplicate key: a unique index already has a document with these field values";
        case DC_ERR_MISSING_INDEXED_FIELD:
            return "Document is missing a field required by a non-sparse index "
                   "(create the index with sparse: true to skip such documents)";
        case DC_ERR_UNSUPPORTED_ID:
            return "Upsert: the filter pins an _id that is not an ObjectId. Unlike "
                   "MongoDB, scalar _ids (numbers, arbitrary strings, Dates) are not "
                   "supported by the on-disk format; keep natural keys in their own "
                   "field with a unique index. See docs/db-api.md.";
        case DC_ERR_UNINDEXABLE_VALUE:
            return "Indexed field value cannot be key-encoded: only numbers, strings, "
                   "and Dates are indexable (no NaN, no strings containing U+0000)";
        case DC_ERR_NO_INDEX:
            return "No index of that name on this collection";
        case DC_ERR_INDEX_KIND:
            return "the index named is not an equality index "
                   "(a text or geo index answers a different question)";
        case DC_ERR_INDEX_ARITY:
            return "findByIndex needs one value per indexed field, "
                   "in the index's own order";
        /* db_validate.h */
        case DC_ERR_INVALID_COLLECTION_NAME:
            return "Invalid collection name: must be a non-empty string containing "
                   "no '/' and no NUL";
        case DC_ERR_INVALID_DB_NAME:
            return "Invalid database name: must be a non-empty string containing "
                   "no '/' and no NUL";
        case DC_ERR_RESERVED_NAME:
            return "Invalid collection name: \"" DC_FORMAT_KEY "\" is reserved for the "
                   "format stamp (docs/format-compatibility.md)";
        case DC_ERR_EMPTY_KEY_SPEC:
            return "createIndex requires at least one field";
        case DC_ERR_NON_ASCENDING_KEY:
            return "createIndex: only ascending (1) fields are supported so far";
        /* db_bulk.h */
        /* db_update.h */
        case DC_ERR_BAD_CURRENT_DATE:
            return "$currentDate: each field must be true or {$type: \"date\"}";
        case DC_ERR_CURRENT_DATE_CONFLICT:
            return "$currentDate: field is already targeted by another operator";
        /* db_catalog.h */
        case DC_ERR_CATALOG_ENTRY:
            return "Catalog entry is malformed or written by an incompatible "
                   "version (docs/format-compatibility.md)";
        case DC_ERR_INDEX_OPTION_UNSUPPORTED:
            return "createIndex: unique/sparse/partialFilterExpression/expireAfterSeconds "
                   "are only supported for equality indexes";
        case DC_ERR_TTL_NEEDS_SINGLE_FIELD:
            return "createIndex: expireAfterSeconds requires a single-field index";
        case DC_ERR_BULK_EMPTY:
            return "bulkWrite requires a non-empty array of operations";
        case DC_ERR_BULK_UNKNOWN_OP:
            return "bulkWrite: each operation must be an object with exactly one known "
                   "key (insertOne, updateOne, updateMany, replaceOne, deleteOne, deleteMany)";
        /* db_agg.h */
        case DC_ERR_AGG_BAD_STAGE:
            return "aggregate: each stage must be an object with exactly one key, "
                   "and its argument must have the right shape";
        case DC_ERR_AGG_UNKNOWN_STAGE:
            return "aggregate: unsupported stage (supported: $match, $sort, $skip, "
                   "$limit, $project, $group, $count)";
        case DC_ERR_AGG_BAD_ACCUMULATOR:
            return "aggregate: each $group field must be exactly one of $sum, $avg, "
                   "$min, $max, $first, $last, $push, $addToSet, $count";
        case DC_ERR_AGG_PROJECT_MIXED:
            return "aggregate: $project cannot mix inclusion and exclusion (except _id)";
        case DC_ERR_BULK_MISSING_FIELD:
            return "bulkWrite: operation is missing a required field "
                   "(document / filter / update / replacement)";
        /* db_wal.h */
        case DC_ERR_WAL_UNKNOWN_OP:
            return "WAL: log entry names an unknown command op";
        case DC_ERR_WAL_MISSING_FIELD:
            return "WAL: log entry is missing a field its command op requires";
        case DC_ERR_WAL_BAD_REQUEST:
            return "WAL: malformed write request (empty batch, or a document "
                   "request with no collection)";
        case DC_ERR_WAL_NO_ID:
            return "Insert: the document carries no _id. A document's identity "
                   "is in the document, and generating one needs a clock, which "
                   "this layer does not have -- the caller assigns it";
        case DC_ERR_WAL_NOT_APPLIABLE:
            return "WAL: this command is not applied here -- creating or dropping "
                   "a collection or index touches FILES, which belongs to whoever "
                   "owns the namespace";
        /* db_session.h */
        case DC_ERR_NO_COLLECTION:
            return "No collection of that name in this database's catalog";
        case DC_ERR_TOO_MANY_DATABASES:
            return "Too many databases open at once in this process; close one "
                   "(a database nobody has asked for costs nothing, but one that "
                   "is open holds a catalog and its collections' files)";
        case DC_ERR_TOO_MANY_COLLECTIONS:
            return "Too many collections open at once in this session";
        case DC_ERR_TOO_MANY_INDEXES:
            return "Collection has more indexes than a session can hold open";
        case DC_ERR_REQ_MALFORMED:
            return "Malformed request: not an object, or a field has the wrong type";
        case DC_ERR_REQ_UNKNOWN_OP:
            return "Request names an op this server does not know";
        case DC_ERR_NO_DATABASE:
            return "No database in this directory: it has no " DC_CATALOG_FILE;
        case DC_ERR_TOO_MANY_CLIENTS:
            return "The server is already holding as many connections as it will "
                   "(--max-clients); this one is refused rather than queued";
        case DC_ERR_FORMAT_NEWER:
            return "Database format is newer than this build understands; upgrade "
                   "nisaba to open it (docs/format-compatibility.md)";
        case DC_ERR_INDEX_EXISTS:
            return "An index of that name already exists on this collection; drop "
                   "it before creating another";
        case DC_ERR_NO_STREAM:
            return "No change stream of that id belongs to this client "
                   "(it was closed, it overflowed, or it was never yours)";
        case DC_ERR_TOO_MANY_STREAMS:
            return "Too many change streams open at once on this server";
        case DC_ERR_NOT_LEADER:
            return "This member is not the leader of its group, so it can serve "
                   "neither a read nor a write; retry against the leader";
        case DC_ERR_WRITE_LOST:
            return "Leadership moved before this write was applied, and no "
                   "replica holds it; it is safe to retry";
        case DC_ERR_NOT_CURRENT:
            return "This member leads but could not prove it still does, so it "
                   "cannot show its data is current; a quorum has gone quiet -- "
                   "retry, here or anywhere";
        case DC_ERR_RESUME_NO_LOG:
            return "watch: resume needs an entry log and this server keeps none "
                   "(an unreplicated server applies writes directly); watch "
                   "without `from` and re-read current state";
        case DC_ERR_RESUME_COMPACTED:
            return "watch: the resume index is below the log's base -- the "
                   "entries after it were compacted away, so the events they "
                   "would have made cannot be served; watch afresh and re-read "
                   "current state";
        case DC_ERR_RESUME_AHEAD:
            return "watch: the resume index is ahead of this member's log; a "
                   "token can only come from a delivered event, so this one is "
                   "not this log's";
        case DC_ERR_BATCH_TOO_LARGE:
            return "This request plans more log entries than the node can "
                   "track in flight; nothing was applied. Split the list into "
                   "smaller requests, or retry when in-flight writes have "
                   "settled";
        case DC_ERR_DB_DROPPED:
            return "The database this request was using was dropped while the "
                   "request was in flight; whatever the request applied is "
                   "gone with it, and no result can be produced";
        case DC_ERR_NO_SNAPSHOT_STORE:
            return "This server has no snapshot store: snapshots exist to "
                   "compact the log, and this server runs without one. Start "
                   "it with --raft (a cluster of one is fine) to back it up";
        case DC_ERR_SNAPSHOT_GONE:
            return "No committed snapshot generation here, or the one named "
                   "is not the committed one any more; ask latestSnapshot and "
                   "restart from what it says";
        case DC_ERR_NOT_REPLICATED:
            return "This server runs without a log: it is its own leader and "
                   "there is nobody to hand leadership to. Start it with "
                   "--raft to make it a cluster member";
        case DC_ERR_TRANSFER_FAILED:
            return "Leadership did not move before the deadline (the target "
                   "is down, unreachable, or refused to stand); this member "
                   "still leads and it is safe to retry";
        case DC_ERR_CURSORS_OPEN:
            return "Cannot compact a collection while a cursor is scanning it: "
                   "compaction rewrites the files the cursor is positioned in";
        case DC_ERR_NO_CURSOR:
            return "No cursor of that id belongs to this client (it was drained, "
                   "killed, or opened by somebody else)";
        case DC_ERR_TOO_MANY_CURSORS:
            return "Too many cursors open at once on this server; drain or kill "
                   "one before opening another";
        case DC_ERR_CURSOR_SORTED:
            return "A sorted find cannot be batched: an arbitrary sort needs every "
                   "match before the first ordered result exists. Ask without "
                   "batchSize, or without sort";
        case DC_ERR_IDLE_TIMEOUT:
            return "Connection closed: it asked nothing for longer than the "
                   "server's --idle-timeout (send {op:\"ping\"} to keep one warm)";
        case DC_ERR_REQ_MISSING_FIELD:
            return "Request is missing a field its op requires "
                   "(a write that needs an _id or a clock reading must carry "
                   "one: both are the caller's)";
        /* raft_core.h / raft_msg.h / raft_node.h. These reach a host the
         * same way every other code does, and a consensus refusal that
         * prints "unknown error" is a refusal nobody can act on. */
        case RAFT_ERR_MEMBER:
            return "Raft: a member record is malformed, or a voter is not a member "
                   "(ids must be positive integers; 0 means \"voted for nobody\")";
        case RAFT_ERR_MESSAGE:
            return "Raft: not a message this build understands (unknown kind, or a "
                   "known kind missing a field it cannot proceed without)";
        case RAFT_ERR_PEER:
            return "Raft: no such peer, or a correlation id nobody issued";
        case RAFT_ERR_BUSY:
            return "Raft: a membership change is already in flight; changes serialize "
                   "by design, so this one has to wait for that one to commit";
        case RAFT_ERR_CAPACITY:
            return "Raft: the member set is larger than this build can hold; it is "
                   "refused whole rather than trimmed, because a node replicating "
                   "to a trimmed set has a different cluster from everyone else";
        default:                    return "unknown error";
    }
}

int dc_is_deterministic(int code) {
    switch (code) {
        /* Facts about the command and the state it lands on. Every
         * replica applying this entry reaches the same verdict. */
        case DC_ERR_DUPLICATE:
        case DC_ERR_ID_MISMATCH:
        case DC_ERR_DUPLICATE_KEY:
        case DC_ERR_MISSING_INDEXED_FIELD:
        case DC_ERR_UNINDEXABLE_VALUE:
        case DC_ERR_UNSUPPORTED_ID:
        /* Facts about the command's shape. The bytes are identical on
         * every replica, so the rejection is too. */
        case DC_ERR_INVALID_COLLECTION_NAME:
        case DC_ERR_INVALID_DB_NAME:
        case DC_ERR_RESERVED_NAME:
        case DC_ERR_EMPTY_KEY_SPEC:
        case DC_ERR_NON_ASCENDING_KEY:
        case DC_ERR_BULK_EMPTY:
        case DC_ERR_BULK_UNKNOWN_OP:
        case DC_ERR_BULK_MISSING_FIELD:
        case DC_ERR_AGG_BAD_STAGE:
        case DC_ERR_AGG_UNKNOWN_STAGE:
        case DC_ERR_AGG_BAD_ACCUMULATOR:
        case DC_ERR_AGG_PROJECT_MIXED:
        case DC_ERR_BAD_CURRENT_DATE:
        case DC_ERR_CURRENT_DATE_CONFLICT:
        case DC_ERR_INDEX_OPTION_UNSUPPORTED:
        case DC_ERR_TTL_NEEDS_SINGLE_FIELD:
        case DC_ERR_WAL_UNKNOWN_OP:
        case DC_ERR_WAL_MISSING_FIELD:
        case DC_ERR_WAL_BAD_REQUEST:
        case DC_ERR_WAL_NO_ID:
        /* The DDL three, whose outcomes are facts about the catalog a
         * replica reached by applying the same prefix. A re-applied
         * createIndex finds the index already there; a re-applied
         * dropIndex finds it gone. Both are what CONVERGENCE looks like
         * from inside an apply loop, so an apply pump treats them the
         * way it treats a duplicate _id: an answer, not a halt.
         *
         * DC_ERR_NO_COLLECTION is deliberately NOT here. It is the same
         * ambiguity DC_ERR_CATALOG_ENTRY has -- a command naming a
         * collection this replica does not have is either a log it
         * cannot apply or a state that has drifted -- and this
         * classification resolves ambiguity toward halting. */
        case DC_ERR_INDEX_EXISTS:
        case DC_ERR_NO_INDEX:
            return 1;

        /* Deliberately NOT deterministic, and each for its own reason:
         *
         *   DC_ERR_CATALOG_ENTRY   this replica's catalog is damaged or
         *                          older; a healthy one would succeed.
         *   BJ_ERR_OOM             a local resource, not the command.
         *   BJ_ERR_STATE           a programming error; halting is how
         *                          it gets found.
         *   BJ_ERR_EOF and the other structural codes -- they arise
         *                          from parsing a command (identical
         *                          everywhere) OR from reading a
         *                          damaged local file (not), and an
         *                          ambiguity in this classification
         *                          must resolve toward halting.
         *
         * Anything unlisted, including anything added later, lands here
         * too: a new code is presumed to be divergence until somebody
         * decides otherwise. */
        default:
            return 0;
    }
}

/* The rules shared by collection and database names. */
static int name_is_wellformed(const char *name, size_t len) {
    if (len == 0) return 0;
    for (size_t i = 0; i < len; i++) {
        if (name[i] == '/' || name[i] == '\0') return 0;
    }
    return 1;
}

int dc_check_collection_name(const char *name, size_t len) {
    if (!name_is_wellformed(name, len)) return DC_ERR_INVALID_COLLECTION_NAME;
    if (len == strlen(DC_FORMAT_KEY) && memcmp(name, DC_FORMAT_KEY, len) == 0)
        return DC_ERR_RESERVED_NAME;
    return BJ_OK;
}

int dc_check_db_name(const char *name, size_t len) {
    return name_is_wellformed(name, len) ? BJ_OK : DC_ERR_INVALID_DB_NAME;
}

int dc_check_index_key_spec(const uint8_t *spec, size_t len, dbuf *fields_out) {
    cur c = { spec, len, 0 };
    uint32_t count;
    int e = object_begin(&c, &count);
    if (e) return e;
    if (count == 0) return DC_ERR_EMPTY_KEY_SPEC;

    /* Two passes: validate everything before emitting anything, so a
     * rejected spec never leaves a half-built array behind for a caller
     * that ignored the return code. */
    cur scan = c;
    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        if ((e = take_key(&scan, &kp, &klen))) return e;
        if (klen == 0) return DC_ERR_INVALID_COLLECTION_NAME;
        double v;
        cur val = scan;
        if (read_number(&val, &v) != BJ_OK) return DC_ERR_NON_ASCENDING_KEY;
        if (v != 1.0) return DC_ERR_NON_ASCENDING_KEY;
        if ((e = skip_value(&scan))) return e;
    }

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_array(b);
    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        if ((e = take_key(&c, &kp, &klen))) { bj_builder_free(b); return e; }
        bj_put_string(b, kp, klen);
        if ((e = skip_value(&c))) { bj_builder_free(b); return e; }
    }
    bj_end_array(b);
    if ((e = bj_builder_error(b))) { bj_builder_free(b); return e; }

    size_t out_len = 0;
    const uint8_t *out = bj_builder_data(b, &out_len);
    if (!out) { bj_builder_free(b); return BJ_ERR_STATE; }
    e = dbuf_put(fields_out, out, out_len);
    bj_builder_free(b);
    return e;
}
