/*
 * db.c — see db.h.
 *
 * Documents and filters are read with the shared bjcursor.h primitives
 * (object_begin/take_key/skip_value); nothing here decodes a value into a
 * host tree the way binjson.h's visitor-based bj_decode does — field lookup
 * and filter matching only ever need "where does this field's value start
 * and end", which skip_value gives directly. Composite index keys are built
 * with keyenc.h (binjson-structures).
 */
#include "db.h"
#include "keyenc.h"
#include "db_query.h"
#include "db_update.h"
#include "bjcursor.h"
#include "bjfile.h"
#include "dbuf.h"

#include <stdlib.h>
#include <string.h>

/* ---- dc_collection: primary tree + attached secondary indexes --------- */

typedef enum { DC_IDX_EQUALITY, DC_IDX_TEXT, DC_IDX_GEO } dc_index_kind;

typedef struct {
    char *name;
    uint32_t name_len;
    dc_index_kind kind;

    /* DC_IDX_EQUALITY */
    bpt *tree;                  /* not owned */
    uint8_t **field_names;      /* owned copies, one per composite-key part */
    uint32_t *field_name_lens;
    uint32_t field_count;
    int unique;                 /* reject a write whose field values collide with another document's */
    int sparse;                 /* skip (don't error) documents missing a field, rather than index them */
    uint8_t *partial_filter;    /* owned copy, binjson OBJECT, or NULL for "always applies" */
    uint32_t partial_filter_len;

    /* DC_IDX_TEXT */
    bpt *tix_index, *tix_doc_terms, *tix_doc_lengths; /* not owned */
    char *text_field;           /* owned */
    uint32_t text_field_len;

    /* DC_IDX_GEO */
    rtree *rt;                  /* not owned */
    char *geo_field;            /* owned */
    uint32_t geo_field_len;
} dc_index;

struct dc_collection {
    bpt *primary;                /* not owned */
    dc_index *indexes;           /* owned, dense array */
    uint32_t index_count;
    uint32_t index_cap;
    bj_io journal;                /* stored by value -- see dc_collection_recover */
    int has_journal;
};

static void free_index(dc_index *ix) {
    free(ix->name);
    for (uint32_t i = 0; i < ix->field_count; i++) free(ix->field_names[i]);
    free(ix->field_names);
    free(ix->field_name_lens);
    free(ix->partial_filter);
    free(ix->text_field);
    free(ix->geo_field);
    memset(ix, 0, sizeof(*ix));
}

dc_collection *dc_collection_open(bpt *primary) {
    dc_collection *c = (dc_collection *)calloc(1, sizeof(dc_collection));
    if (!c) return NULL;
    c->primary = primary;
    return c;
}

void dc_collection_free(dc_collection *c) {
    if (!c) return;
    for (uint32_t i = 0; i < c->index_count; i++) free_index(&c->indexes[i]);
    free(c->indexes);
    free(c);
}

static dc_index *find_index(dc_collection *c, const char *name, int name_len) {
    for (uint32_t i = 0; i < c->index_count; i++) {
        if (c->indexes[i].name_len == (uint32_t)name_len &&
            memcmp(c->indexes[i].name, name, (size_t)name_len) == 0) {
            return &c->indexes[i];
        }
    }
    return NULL;
}

/* Append `ix` (already fully populated) to `c->indexes`, growing the array
 * as needed. Consumes `ix` on success; on OOM, frees it and returns
 * BJ_ERR_OOM (the caller need not also free it). */
static int append_index(dc_collection *c, dc_index *ix) {
    if (c->index_count == c->index_cap) {
        uint32_t ncap = c->index_cap ? c->index_cap * 2 : 4;
        dc_index *nb = (dc_index *)realloc(c->indexes, ncap * sizeof(dc_index));
        if (!nb) { free_index(ix); return BJ_ERR_OOM; }
        c->indexes = nb;
        c->index_cap = ncap;
    }
    c->indexes[c->index_count++] = *ix;
    return BJ_OK;
}

/*
 * ---- Cross-collection commit journal (crash atomicity) --------------------
 *
 * Generalizes textindex.c's fixed-3-tree TIXJ journal (docs/textindex-
 * atomicity.md) to a variable number of files: the primary tree plus every
 * currently attached index's file(s) (equality/geo: 1, text: 3). One slot:
 *
 *   magic "DCTJ"(4) + version(4) + txn(8) + file_count(4) + N*8-byte lengths + crc32(4)
 *
 * Two slots ping-ponged at offset 0 and dctj_slot_size(n), exactly like
 * TIXJ. `file_count` is part of the CRC'd payload: a slot whose stored count
 * doesn't match the *current* live count (c->index_count-derived) is treated
 * as undecodable, same as a CRC failure -- this matters because N changes
 * whenever an index is created/dropped (see dctj_truncate's call sites,
 * which keep every pair of slots ever compared at the same N).
 *
 * A collection with no secondary indexes needs no journal I/O at all: its
 * primary tree is already atomic on its own (a single file with its own
 * CRC'd commit trailer), so commit_journal/dc_collection_recover both skip
 * work when c->index_count == 0.
 */
#define DCTJ_MAGIC   "DCTJ"
#define DCTJ_VERSION 1

static void wr32le(uint8_t *p, uint32_t v) {
    p[0] = (uint8_t)v; p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16); p[3] = (uint8_t)(v >> 24);
}
static void wr64le(uint8_t *p, uint64_t v) {
    for (int i = 0; i < 8; i++) p[i] = (uint8_t)(v >> (i * 8));
}

/* One length per journaled resource: primary + one per attached index
 * (equality/geo = 1 file, text = 3), in c->indexes registration order. */
static uint32_t dctj_file_count(const dc_collection *c) {
    uint32_t n = 1;
    for (uint32_t i = 0; i < c->index_count; i++)
        n += c->indexes[i].kind == DC_IDX_TEXT ? 3 : 1;
    return n;
}
static size_t dctj_slot_size(uint32_t n) { return 24 + 8 * (size_t)n; }

static void dctj_encode(uint8_t *s, uint64_t txn, uint32_t n, const uint64_t *lens) {
    size_t sz = dctj_slot_size(n);
    memset(s, 0, sz);
    memcpy(s, DCTJ_MAGIC, 4);
    wr32le(s + 4, DCTJ_VERSION);
    wr64le(s + 8, txn);
    wr32le(s + 16, n);
    for (uint32_t i = 0; i < n; i++) wr64le(s + 20 + 8 * i, lens[i]);
    wr32le(s + 20 + 8 * n, bjfile_crc32(0, s, 20 + 8 * n));
}
static int dctj_decode(const uint8_t *s, uint32_t n, uint64_t *txn, uint64_t *lens) {
    if (memcmp(s, DCTJ_MAGIC, 4) != 0) return 0;
    if (rdu32(s + 4) != DCTJ_VERSION) return 0;
    if (rdu32(s + 16) != n) return 0; /* stale-N self-healing check */
    if (rdu32(s + 20 + 8 * n) != bjfile_crc32(0, s, 20 + 8 * n)) return 0;
    *txn = rdu64(s + 8);
    for (uint32_t i = 0; i < n; i++) lens[i] = rdu64(s + 20 + 8 * i);
    return 1;
}

/* Read both slots (sized for n files each), newest first. lens must have
 * room for 2*n uint64_t. Returns 0/1/2 decodable slots, or a negative
 * BJ_ERR_* on I/O failure. */
static int dctj_read(const bj_io *j, uint32_t n, uint64_t txn[2], uint64_t *lens) {
    size_t slot_sz = dctj_slot_size(n);
    uint8_t *buf = (uint8_t *)calloc(1, 2 * slot_sz);
    if (!buf) return BJ_ERR_OOM;
    uint64_t fsz = j->size(j->ctx);
    if (fsz > 0) {
        uint64_t want64 = fsz < 2 * slot_sz ? fsz : 2 * slot_sz;
        int64_t got = j->read(j->ctx, 0, buf, (uint32_t)want64);
        if (got < 0) { free(buf); return (int)got; }
    }
    uint64_t t0, t1;
    uint64_t *l0 = (uint64_t *)malloc(n * sizeof(uint64_t));
    uint64_t *l1 = (uint64_t *)malloc(n * sizeof(uint64_t));
    if (!l0 || !l1) { free(buf); free(l0); free(l1); return BJ_ERR_OOM; }
    int v0 = dctj_decode(buf, n, &t0, l0);
    int v1 = dctj_decode(buf + slot_sz, n, &t1, l1);
    int result;
    if (v0 && v1) {
        int newer0 = t0 >= t1;
        txn[0] = newer0 ? t0 : t1;
        memcpy(lens, newer0 ? l0 : l1, n * sizeof(uint64_t));
        txn[1] = newer0 ? t1 : t0;
        memcpy(lens + n, newer0 ? l1 : l0, n * sizeof(uint64_t));
        result = 2;
    } else if (v0 || v1) {
        txn[0] = v0 ? t0 : t1;
        memcpy(lens, v0 ? l0 : l1, n * sizeof(uint64_t));
        result = 1;
    } else {
        result = 0;
    }
    free(buf); free(l0); free(l1);
    return result;
}

/* Reset the journal to empty ahead of an index-set change (see the file
 * comment above): an empty journal imposes no constraint regardless of what
 * N becomes next, keeping every pair of slots ever compared at the same N. */
static int dctj_truncate(dc_collection *c) {
    if (!c->has_journal || !c->journal.truncate) return BJ_OK;
    int32_t e = c->journal.truncate(c->journal.ctx, 0);
    return e ? (int)e : BJ_OK;
}

static void dctj_gather_lens(dc_collection *c, uint64_t *lens) {
    uint32_t i = 0;
    lens[i++] = bpt_file_len(c->primary);
    for (uint32_t k = 0; k < c->index_count; k++) {
        dc_index *ix = &c->indexes[k];
        if (ix->kind == DC_IDX_EQUALITY) lens[i++] = bpt_file_len(ix->tree);
        else if (ix->kind == DC_IDX_TEXT) {
            lens[i++] = bpt_file_len(ix->tix_index);
            lens[i++] = bpt_file_len(ix->tix_doc_terms);
            lens[i++] = bpt_file_len(ix->tix_doc_lengths);
        } else lens[i++] = rtree_file_len(ix->rt);
    }
}
static int dctj_rewind_all(dc_collection *c, const uint64_t *lens) {
    uint32_t i = 0;
    int e = bpt_rewind(c->primary, lens[i++]);
    for (uint32_t k = 0; !e && k < c->index_count; k++) {
        dc_index *ix = &c->indexes[k];
        if (ix->kind == DC_IDX_EQUALITY) e = bpt_rewind(ix->tree, lens[i++]);
        else if (ix->kind == DC_IDX_TEXT) {
            e = bpt_rewind(ix->tix_index, lens[i++]);
            if (!e) e = bpt_rewind(ix->tix_doc_terms, lens[i++]);
            if (!e) e = bpt_rewind(ix->tix_doc_lengths, lens[i++]);
        } else e = rtree_rewind(ix->rt, lens[i++]);
    }
    return e;
}

/* Record the current file lengths as one committed transaction. No-op if no
 * journal is set, or the collection has no secondary indexes. */
static int commit_journal(dc_collection *c) {
    if (!c->has_journal || c->index_count == 0) return BJ_OK;
    uint32_t n = dctj_file_count(c);
    uint64_t txn[2];
    uint64_t *lens = (uint64_t *)malloc(2 * n * sizeof(uint64_t));
    if (!lens) return BJ_ERR_OOM;
    int cnt = dctj_read(&c->journal, n, txn, lens);
    if (cnt < 0) { free(lens); return cnt; }
    uint64_t next = cnt ? txn[0] + 1 : 1;
    uint64_t *cur = (uint64_t *)malloc(n * sizeof(uint64_t));
    if (!cur) { free(lens); return BJ_ERR_OOM; }
    dctj_gather_lens(c, cur);
    size_t sz = dctj_slot_size(n);
    uint8_t *s = (uint8_t *)malloc(sz);
    if (!s) { free(lens); free(cur); return BJ_ERR_OOM; }
    dctj_encode(s, next, n, cur);
    int32_t w = c->journal.write(c->journal.ctx, (next & 1) ? 0 : sz, s, (uint32_t)sz);
    free(lens); free(cur); free(s);
    return w ? (int)w : BJ_OK;
}

/*
 * In-process rollback for a failed multi-file mutation: capture every
 * backing file's committed length before the first write, and rewind all
 * files back to those lengths if the operation errors partway. This is the
 * live-session counterpart of dc_collection_recover's reopen-time rewind:
 * without it a failed write (say, DC_ERR_MISSING_INDEXED_FIELD from
 * add_to_indexes after the primary tree already took the document, or a
 * DC_ERR_DUPLICATE_KEY rejection after remove_from_indexes already cleared
 * the old entries) would leave the live collection inconsistent until the
 * next open. Safe with concurrently open cursors: they pin roots at commit
 * boundaries at or below the captured lengths, which the rewind never
 * truncates away.
 *
 * No capture (NULL lens, still BJ_OK) when the collection has no secondary
 * indexes -- a single-file bpt operation already discards its pending
 * appends on failure, leaving the file untouched. Works with or without a
 * journal: the lengths come from the live trees, not the journal.
 */
static int mut_begin(dc_collection *c, uint64_t **lens) {
    *lens = NULL;
    if (c->index_count == 0) return BJ_OK;
    uint64_t *l = (uint64_t *)malloc(dctj_file_count(c) * sizeof(uint64_t));
    if (!l) return BJ_ERR_OOM;
    dctj_gather_lens(c, l);
    *lens = l;
    return BJ_OK;
}

/* Finish a mutation begun with mut_begin: on error, best-effort rewind of
 * every file to its captured length. The original error is returned either
 * way -- if the rewind itself fails (host I/O), the journal still heals the
 * state at the next open, exactly as if the process had crashed here. */
static int mut_end(dc_collection *c, uint64_t *lens, int e) {
    if (lens) {
        if (e) (void)dctj_rewind_all(c, lens);
        free(lens);
    }
    return e;
}

int dc_collection_recover(dc_collection *c, const bj_io *journal) {
    if (!journal) { c->has_journal = 0; return BJ_OK; }
    c->journal = *journal;
    c->has_journal = 1;
    if (c->index_count == 0) return BJ_OK;
    uint32_t n = dctj_file_count(c);
    uint64_t txn[2];
    uint64_t *lens = (uint64_t *)malloc(2 * n * sizeof(uint64_t));
    if (!lens) return BJ_ERR_OOM;
    int cnt = dctj_read(&c->journal, n, txn, lens);
    if (cnt <= 0) { free(lens); return cnt; } /* 0: empty journal, adopt as-is */
    uint64_t *cur = (uint64_t *)malloc(n * sizeof(uint64_t));
    if (!cur) { free(lens); return BJ_ERR_OOM; }
    dctj_gather_lens(c, cur);
    int e = BJ_ERR_STATE;
    for (int slot = 0; slot < cnt; slot++) {
        uint64_t *want = lens + (size_t)slot * n;
        int ok = 1;
        for (uint32_t i = 0; i < n; i++) if (cur[i] < want[i]) { ok = 0; break; }
        if (!ok) continue;
        e = dctj_rewind_all(c, want);
        break;
    }
    free(lens); free(cur);
    return e;
}

uint64_t dc_applied_index(const dc_collection *c) {
    return bpt_applied_index(c->primary);
}

/* One structure's staging check/stage, switched on what the dc_index
 * actually holds. `stage == 0` only validates (see dc_set_applied_index's
 * no-partial-staging contract); `stage == 1` performs the staging, which
 * cannot fail after a passing validation pass. */
static int stage_applied_bpt(bpt *t, uint64_t index, int stage) {
    if (stage) return bpt_set_applied_index(t, index);
    if (bpt_is_snapshot(t) || index < bpt_applied_index(t)) return BJ_ERR_STATE;
    return BJ_OK;
}

static int stage_applied_all(dc_collection *c, uint64_t index, int stage) {
    int e = stage_applied_bpt(c->primary, index, stage);
    if (e) return e;
    for (uint32_t i = 0; i < c->index_count; i++) {
        dc_index *ix = &c->indexes[i];
        switch (ix->kind) {
        case DC_IDX_EQUALITY:
            e = stage_applied_bpt(ix->tree, index, stage);
            break;
        case DC_IDX_TEXT:
            e = stage_applied_bpt(ix->tix_index, index, stage);
            if (!e) e = stage_applied_bpt(ix->tix_doc_terms, index, stage);
            if (!e) e = stage_applied_bpt(ix->tix_doc_lengths, index, stage);
            break;
        case DC_IDX_GEO:
            if (stage) e = rtree_set_applied_index(ix->rt, index);
            else e = (index < rtree_applied_index(ix->rt)) ? BJ_ERR_STATE : BJ_OK;
            break;
        }
        if (e) return e;
    }
    return BJ_OK;
}

int dc_set_applied_index(dc_collection *c, uint64_t index) {
    int e = stage_applied_all(c, index, 0);
    if (e) return e;
    return stage_applied_all(c, index, 1);
}

/* Defined in the index-maintenance section below; forward-declared here so
 * backfill_index (used by the dc_collection_add_*_index family, defined
 * earlier in the file for readability alongside their attach_* siblings)
 * can call it. */
static int add_to_one_index(dc_index *ix, const uint8_t *doc, size_t doc_len, const uint8_t id[12]);

/* Scan every document in `c->primary` and add it to `ix` (already
 * appended to `c`, expected empty). On failure, unregisters `ix` from `c`
 * so the caller's own cleanup only needs to discard the index's file(s). */
static int backfill_index(dc_collection *c, const char *name, int name_len) {
    dc_index *ix = find_index(c, name, name_len);
    bpt_cursor *cur_h = bpt_cursor_open(c->primary, NULL, NULL);
    if (!cur_h) { dc_collection_remove_index(c, name, name_len); return BJ_ERR_OOM; }
    int rc = BJ_OK;
    for (;;) {
        bpt_key k; const uint8_t *val; size_t vlen;
        int r = bpt_cursor_next(cur_h, &k, &val, &vlen);
        if (r < 0) { rc = r; break; }
        if (r == 0) break;
        rc = add_to_one_index(ix, val, vlen, k.str);
        if (rc) break;
    }
    bpt_cursor_close(cur_h);
    if (rc) dc_collection_remove_index(c, name, name_len);
    return rc;
}

int dc_collection_attach_index(dc_collection *c, const char *name, int name_len,
                               bpt *index_tree,
                               const uint8_t *fields, uint32_t fields_len,
                               int unique, int sparse,
                               const uint8_t *partial_filter, uint32_t partial_filter_len) {
    if (find_index(c, name, name_len)) return BJ_ERR_STATE;

    cur fc = { fields, fields_len, 0 };
    uint32_t fcount;
    int e = array_begin(&fc, &fcount);
    if (e) return e;
    if (fcount == 0) return BJ_ERR_STATE;

    dc_index ix;
    memset(&ix, 0, sizeof(ix));
    ix.kind = DC_IDX_EQUALITY;
    ix.name = (char *)malloc(name_len ? (size_t)name_len : 1);
    if (!ix.name) return BJ_ERR_OOM;
    memcpy(ix.name, name, (size_t)name_len);
    ix.name_len = (uint32_t)name_len;
    ix.tree = index_tree;
    ix.unique = unique;
    ix.sparse = sparse;
    if (partial_filter && partial_filter_len > 0) {
        ix.partial_filter = (uint8_t *)malloc(partial_filter_len);
        if (!ix.partial_filter) { free_index(&ix); return BJ_ERR_OOM; }
        memcpy(ix.partial_filter, partial_filter, partial_filter_len);
        ix.partial_filter_len = partial_filter_len;
    }
    ix.field_names = (uint8_t **)calloc(fcount, sizeof(uint8_t *));
    ix.field_name_lens = (uint32_t *)calloc(fcount, sizeof(uint32_t));
    if (!ix.field_names || !ix.field_name_lens) { free_index(&ix); return BJ_ERR_OOM; }

    for (uint32_t i = 0; i < fcount; i++) {
        const uint8_t *sp; uint32_t slen;
        e = take_string(&fc, &sp, &slen);
        if (e) { free_index(&ix); return e; }
        ix.field_names[i] = (uint8_t *)malloc(slen ? slen : 1);
        if (!ix.field_names[i]) { free_index(&ix); return BJ_ERR_OOM; }
        memcpy(ix.field_names[i], sp, slen);
        ix.field_name_lens[i] = slen;
        ix.field_count = i + 1;
    }

    return append_index(c, &ix);
}

int dc_collection_attach_text_index(dc_collection *c, const char *name, int name_len,
                                    bpt *tix_index, bpt *tix_doc_terms, bpt *tix_doc_lengths,
                                    const char *field, int field_len) {
    if (find_index(c, name, name_len)) return BJ_ERR_STATE;
    if (field_len <= 0) return BJ_ERR_STATE;
    for (uint32_t i = 0; i < c->index_count; i++) {
        if (c->indexes[i].kind == DC_IDX_TEXT) return BJ_ERR_STATE; /* one text index per collection */
    }

    dc_index ix;
    memset(&ix, 0, sizeof(ix));
    ix.kind = DC_IDX_TEXT;
    ix.name = (char *)malloc(name_len ? (size_t)name_len : 1);
    if (!ix.name) return BJ_ERR_OOM;
    memcpy(ix.name, name, (size_t)name_len);
    ix.name_len = (uint32_t)name_len;
    ix.tix_index = tix_index; ix.tix_doc_terms = tix_doc_terms; ix.tix_doc_lengths = tix_doc_lengths;
    ix.text_field = (char *)malloc((size_t)field_len);
    if (!ix.text_field) { free_index(&ix); return BJ_ERR_OOM; }
    memcpy(ix.text_field, field, (size_t)field_len);
    ix.text_field_len = (uint32_t)field_len;

    return append_index(c, &ix);
}

int dc_collection_add_text_index(dc_collection *c, const char *name, int name_len,
                                 bpt *tix_index, bpt *tix_doc_terms, bpt *tix_doc_lengths,
                                 const char *field, int field_len) {
    int e = dc_collection_attach_text_index(c, name, name_len, tix_index, tix_doc_terms, tix_doc_lengths, field, field_len);
    if (e) return e;
    e = backfill_index(c, name, name_len);
    if (e) return e; /* backfill_index already unregistered ix on failure */
    /* Index count changed -- reset the journal (see the journal section
     * above) so a stale slot from before this index existed can never be
     * misread; roll the index back off if we can't (matches backfill's own
     * failure-rollback convention). */
    e = dctj_truncate(c);
    if (e) { dc_collection_remove_index(c, name, name_len); return e; }
    return BJ_OK;
}

int dc_collection_attach_geo_index(dc_collection *c, const char *name, int name_len,
                                   rtree *rt, const char *field, int field_len) {
    if (find_index(c, name, name_len)) return BJ_ERR_STATE;
    if (field_len <= 0) return BJ_ERR_STATE;

    dc_index ix;
    memset(&ix, 0, sizeof(ix));
    ix.kind = DC_IDX_GEO;
    ix.name = (char *)malloc(name_len ? (size_t)name_len : 1);
    if (!ix.name) return BJ_ERR_OOM;
    memcpy(ix.name, name, (size_t)name_len);
    ix.name_len = (uint32_t)name_len;
    ix.rt = rt;
    ix.geo_field = (char *)malloc((size_t)field_len);
    if (!ix.geo_field) { free_index(&ix); return BJ_ERR_OOM; }
    memcpy(ix.geo_field, field, (size_t)field_len);
    ix.geo_field_len = (uint32_t)field_len;

    return append_index(c, &ix);
}

int dc_collection_add_geo_index(dc_collection *c, const char *name, int name_len,
                                rtree *rt, const char *field, int field_len) {
    int e = dc_collection_attach_geo_index(c, name, name_len, rt, field, field_len);
    if (e) return e;
    e = backfill_index(c, name, name_len);
    if (e) return e; /* backfill_index already unregistered ix on failure */
    e = dctj_truncate(c);
    if (e) { dc_collection_remove_index(c, name, name_len); return e; }
    return BJ_OK;
}

int dc_collection_remove_index(dc_collection *c, const char *name, int name_len) {
    for (uint32_t i = 0; i < c->index_count; i++) {
        if (c->indexes[i].name_len == (uint32_t)name_len &&
            memcmp(c->indexes[i].name, name, (size_t)name_len) == 0) {
            free_index(&c->indexes[i]);
            for (uint32_t j = i; j + 1 < c->index_count; j++) c->indexes[j] = c->indexes[j + 1];
            c->index_count--;
            /* Index count changed -- reset the journal. Best-effort: a
             * failure here just falls back to the count-mismatch self-
             * healing check next commit (see the journal section above),
             * not worth failing an otherwise-successful removal over. */
            dctj_truncate(c);
            return BJ_OK;
        }
    }
    return BJ_ERR_STATE;
}

/* ---- shared helpers ----------------------------------------------------- */

static void oid_key(const uint8_t id[12], bpt_key *k) {
    k->is_string = 1;
    k->num = 0;
    k->str = id;
    k->str_len = 12;
}

/* `doc`'s top-level _id, which must be an OID (13 encoded bytes: type + 12
 * raw) if present. *has reports presence; a present value of any other
 * type is BJ_ERR_STATE either way. */
int dc_document_id_opt(const uint8_t *doc, uint32_t doc_len, uint8_t id_out[12], int *has) {
    const uint8_t *vp; size_t vlen; int found;
    int e = obj_get_field(doc, doc_len, (const uint8_t *)"_id", 3, &vp, &vlen, &found);
    if (e) return e;
    *has = found;
    if (!found) return BJ_OK;
    if (vlen != 13 || vp[0] != BJ_TYPE_OID) return BJ_ERR_STATE;
    memcpy(id_out, vp + 1, 12);
    return BJ_OK;
}

int dc_document_id(const uint8_t *doc, uint32_t doc_len, uint8_t id_out[12]) {
    int has = 0;
    int e = dc_document_id_opt(doc, doc_len, id_out, &has);
    if (e) return e;
    return has ? BJ_OK : BJ_ERR_STATE;
}

/* True (via *is_id_filter) iff `filter` is exactly {_id: <OID>}; when true,
 * writes the 12 id bytes. Any other shape is *is_id_filter = 0, not an
 * error — callers fall back to a scan. */
static int filter_is_id_only(const uint8_t *filter, size_t filter_len,
                             uint8_t id_out[12], int *is_id_filter) {
    cur c = { filter, filter_len, 0 };
    uint32_t count;
    int e = object_begin(&c, &count);
    if (e) return e;
    *is_id_filter = 0;
    if (count != 1) return BJ_OK;
    const uint8_t *kp; uint32_t klen;
    e = take_key(&c, &kp, &klen);
    if (e) return e;
    if (!(klen == 3 && memcmp(kp, "_id", 3) == 0)) return BJ_OK;
    size_t vstart = c.pos;
    e = skip_value(&c);
    if (e) return e;
    size_t vlen = c.pos - vstart;
    if (vlen != 13 || c.d[vstart] != BJ_TYPE_OID) return BJ_OK;
    memcpy(id_out, c.d + vstart + 1, 12);
    *is_id_filter = 1;
    return BJ_OK;
}

/* Build a binjson {_id: <oid>} filter from a raw 12-byte id -- the inverse
 * of filter_is_id_only. Used by the find_one_and_* family to re-target the
 * exact document an initial dc_find_one already captured, so a second
 * internal find/update/delete can never land on a different document. */
static int build_id_filter(const uint8_t id[12], uint8_t **out, size_t *out_len) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"_id", 3);
    if (!e) e = bj_put_oid(b, id);
    if (!e) e = bj_end_object(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, out, out_len);
    }
    bj_builder_free(b);
    return e;
}

/* ---- geo/text helpers ------------------------------------------------------ */

static void oid_to_hex(const uint8_t id[12], char hex[24]) {
    static const char digits[] = "0123456789abcdef";
    for (int i = 0; i < 12; i++) {
        hex[i * 2] = digits[id[i] >> 4];
        hex[i * 2 + 1] = digits[id[i] & 0xf];
    }
}

static int hex_nibble(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static int hex_to_oid(const char *hex, int hex_len, uint8_t id[12]) {
    if (hex_len != 24) return BJ_ERR_STATE;
    for (int i = 0; i < 12; i++) {
        int hi = hex_nibble(hex[i * 2]), lo = hex_nibble(hex[i * 2 + 1]);
        if (hi < 0 || lo < 0) return BJ_ERR_STATE;
        id[i] = (uint8_t)((hi << 4) | lo);
    }
    return BJ_OK;
}

/* Parse a binjson ARRAY of exactly 2 numbers, e.g. a GeoJSON coordinate
 * pair or a legacy [x, y] point. */
static int read_number_pair(const uint8_t *val, size_t val_len, double *a, double *b) {
    if (val_len < 1 || val[0] != BJ_TYPE_ARRAY) return BJ_ERR_STATE;
    cur c = { val, val_len, 0 };
    uint32_t count;
    int e = array_begin(&c, &count);
    if (e) return e;
    if (count != 2) return BJ_ERR_STATE;
    size_t p0 = c.pos;
    e = skip_value(&c);
    if (e) return e;
    cur c0 = { c.d + p0, c.pos - p0, 0 };
    e = read_number(&c0, a);
    if (e) return e;
    size_t p1 = c.pos;
    e = skip_value(&c);
    if (e) return e;
    cur c1 = { c.d + p1, c.pos - p1, 0 };
    return read_number(&c1, b);
}

/* {type: "Point", coordinates: [lng, lat]} -- GeoJSON, longitude first. */
static int parse_geojson_point(const uint8_t *val, size_t val_len, double *lat, double *lng) {
    if (val_len < 1 || val[0] != BJ_TYPE_OBJECT) return BJ_ERR_STATE;
    const uint8_t *tp; size_t tlen; int tfound;
    int e = obj_get_field(val, val_len, (const uint8_t *)"type", 4, &tp, &tlen, &tfound);
    if (e) return e;
    if (!tfound || tlen < 1 || tp[0] != BJ_TYPE_STRING) return BJ_ERR_STATE;
    cur tc = { tp, tlen, 0 };
    const uint8_t *ts; uint32_t tslen;
    e = take_string(&tc, &ts, &tslen);
    if (e) return e;
    if (!(tslen == 5 && memcmp(ts, "Point", 5) == 0)) return BJ_ERR_STATE;

    const uint8_t *cp; size_t clen; int cfound;
    e = obj_get_field(val, val_len, (const uint8_t *)"coordinates", 11, &cp, &clen, &cfound);
    if (e) return e;
    if (!cfound) return BJ_ERR_STATE;
    double x, y;
    e = read_number_pair(cp, clen, &x, &y);
    if (e) return e;
    *lng = x; *lat = y;
    return BJ_OK;
}

/* ---- index maintenance --------------------------------------------------- */

/* Build one composite-key equality index's value-only prefix (its ordered
 * field values, no id suffix) for `doc` into `out`, an ordinary dbuf the
 * caller zero-inits and frees. Shared by build_index_key (below) and
 * check_unique_one, which both need the same value encoding without
 * committing to (or excluding) any particular id suffix. */
static int build_index_key_prefix(const dc_index *ix, const uint8_t *doc, size_t doc_len, dbuf *out) {
    for (uint32_t i = 0; i < ix->field_count; i++) {
        const uint8_t *vp; size_t vlen; int found;
        int e = obj_get_field(doc, doc_len, ix->field_names[i], ix->field_name_lens[i], &vp, &vlen, &found);
        if (e) return e;
        if (!found) return DC_ERR_MISSING_INDEXED_FIELD;
        e = qk_put_value(out, vp, vlen);
        /* qk_put_value's generic BJ_ERR_STATE means "no ordered encoding for
         * this value" here -- surface that as its own code (OOM etc. pass
         * through unchanged). */
        if (e == BJ_ERR_STATE) return DC_ERR_UNINDEXABLE_VALUE;
        if (e) return e;
    }
    return BJ_OK;
}

/* Build one composite-key equality index's key for `doc`/`id` (its ordered
 * field values followed by the id-suffix tag + `id`) into `out`, an
 * ordinary dbuf the caller zero-inits and frees. */
static int build_index_key(const dc_index *ix, const uint8_t *doc, size_t doc_len,
                           const uint8_t id[12], dbuf *out) {
    int e = build_index_key_prefix(ix, doc, doc_len, out);
    if (e) return e;
    return qk_put_id(out, id);
}

/* Whether `doc` should have an entry in equality index `ix`, per its
 * sparse/partialFilterExpression options. Does NOT gate on a missing field
 * for a non-sparse index -- that stays build_index_key's existing
 * all-or-nothing DC_ERR_MISSING_INDEXED_FIELD, unchanged. */
static int equality_index_applies(const dc_index *ix, const uint8_t *doc, size_t doc_len, int *applies) {
    *applies = 1;
    if (ix->partial_filter) {
        int m = 0;
        int e = qry_matches(doc, doc_len, ix->partial_filter, ix->partial_filter_len, &m);
        if (e) return e;
        if (!m) { *applies = 0; return BJ_OK; }
    }
    if (ix->sparse) {
        for (uint32_t i = 0; i < ix->field_count; i++) {
            const uint8_t *vp; size_t vl; int found;
            int e = obj_get_field(doc, doc_len, ix->field_names[i], ix->field_name_lens[i], &vp, &vl, &found);
            if (e) return e;
            if (!found) { *applies = 0; return BJ_OK; }
        }
    }
    return BJ_OK;
}

/*
 * Whether `doc`'s field values already have an entry in unique index `ix`
 * belonging to some other document. Range-scans [prefix, prefix+upper
 * bound) on ix->tree -- the same bound-building dc_collection_find_by_index
 * already does via keyenc.h -- for any entry at all. No self-id
 * exclusion is needed: by the time add_to_one_index runs for a document's
 * *new* state, its own *old* entry has already been removed
 * (dc_update_one/dc_replace_one/dc_update_many all call
 * remove_from_indexes before add_to_indexes), so any entry found here
 * already belongs to a different document.
 */
static int check_unique_one(const dc_index *ix, const uint8_t *doc, size_t doc_len, int *conflict) {
    *conflict = 0;
    dbuf prefix; memset(&prefix, 0, sizeof(prefix));
    int e = build_index_key_prefix(ix, doc, doc_len, &prefix);
    if (e) { dbuf_free(&prefix); return e; }

    dbuf upper; memset(&upper, 0, sizeof(upper));
    e = dbuf_put(&upper, prefix.data, prefix.len);
    if (!e) e = qk_put_upper_bound(&upper);
    if (e) { dbuf_free(&prefix); dbuf_free(&upper); return e; }

    bpt_key min_key; min_key.is_string = 1; min_key.num = 0;
    min_key.str = prefix.data; min_key.str_len = (uint32_t)prefix.len;
    bpt_key max_key; max_key.is_string = 1; max_key.num = 0;
    max_key.str = upper.data; max_key.str_len = (uint32_t)upper.len;

    bpt_cursor *cur_h = bpt_cursor_open(ix->tree, &min_key, &max_key);
    if (!cur_h) { dbuf_free(&prefix); dbuf_free(&upper); return BJ_ERR_OOM; }
    bpt_key k; const uint8_t *val; size_t vlen;
    int r = bpt_cursor_next(cur_h, &k, &val, &vlen);
    if (r < 0) e = r;
    else if (r > 0) *conflict = 1;
    bpt_cursor_close(cur_h);
    dbuf_free(&prefix);
    dbuf_free(&upper);
    return e;
}

/*
 * Reject `doc` up front if it would collide with another document on any
 * attached unique equality index. Called before any primary-tree/index
 * mutation for a write (dc_insert_one; dc_replace_one/dc_update_one/
 * dc_update_many right after remove_from_indexes, before the primary
 * bpt_add) so a rejection never leaves a forbidden duplicate value sitting
 * in the primary tree -- add_to_one_index's own unique check (needed for
 * backfill, which doesn't go through these entry points) runs too late for
 * that: after the primary tree already holds the new document.
 */
static int check_unique_indexes(dc_collection *c, const uint8_t *doc, size_t doc_len) {
    for (uint32_t i = 0; i < c->index_count; i++) {
        dc_index *ix = &c->indexes[i];
        if (ix->kind != DC_IDX_EQUALITY || !ix->unique) continue;
        int applies = 1;
        int e = equality_index_applies(ix, doc, doc_len, &applies);
        if (e) return e;
        if (!applies) continue;
        int conflict = 0;
        e = check_unique_one(ix, doc, doc_len, &conflict);
        if (e) return e;
        if (conflict) return DC_ERR_DUPLICATE_KEY;
    }
    return BJ_OK;
}

static int add_to_one_index(dc_index *ix, const uint8_t *doc, size_t doc_len, const uint8_t id[12]) {
    if (ix->kind == DC_IDX_EQUALITY) {
        int applies = 1;
        int e = equality_index_applies(ix, doc, doc_len, &applies);
        if (e) return e;
        if (!applies) return BJ_OK; /* sparse/partialFilterExpression: not indexed */

        if (ix->unique) {
            int conflict = 0;
            e = check_unique_one(ix, doc, doc_len, &conflict);
            if (e) return e;
            if (conflict) return DC_ERR_DUPLICATE_KEY;
        }

        dbuf key_bytes; memset(&key_bytes, 0, sizeof(key_bytes));
        e = build_index_key(ix, doc, doc_len, id, &key_bytes);
        if (e) { dbuf_free(&key_bytes); return e; }
        bpt_key key; key.is_string = 1; key.num = 0;
        key.str = key_bytes.data; key.str_len = (uint32_t)key_bytes.len;
        /* The index's value is a proper binjson-encoded OID (the row
         * reference, per bplustree.h's composite-key convention) so index
         * files stay independently inspectable with bin/bplustree.js. */
        uint8_t idval[13]; idval[0] = BJ_TYPE_OID; memcpy(idval + 1, id, 12);
        e = bpt_add(ix->tree, &key, idval, 13);
        dbuf_free(&key_bytes);
        return e;
    }

    if (ix->kind == DC_IDX_TEXT) {
        const uint8_t *vp; size_t vlen; int found;
        int e = obj_get_field(doc, doc_len, (const uint8_t *)ix->text_field, ix->text_field_len, &vp, &vlen, &found);
        if (e) return e;
        /* Missing or non-string: not text-indexable, silently skipped --
         * matches a real MongoDB text index's own tolerance. */
        if (!found || vlen < 1 || vp[0] != BJ_TYPE_STRING) return BJ_OK;
        cur c = { vp, vlen, 0 };
        const uint8_t *text; uint32_t text_len;
        e = take_string(&c, &text, &text_len);
        if (e) return e;
        char hex[24];
        oid_to_hex(id, hex);
        return tix_add(ix->tix_index, ix->tix_doc_terms, ix->tix_doc_lengths, NULL,
                       hex, 24, (const char *)text, (int)text_len);
    }

    /* DC_IDX_GEO */
    {
        const uint8_t *vp; size_t vlen; int found;
        int e = obj_get_field(doc, doc_len, (const uint8_t *)ix->geo_field, ix->geo_field_len, &vp, &vlen, &found);
        if (e) return e;
        if (!found) return BJ_OK; /* missing field: not geo-indexable, silently skipped */
        double lat, lng;
        e = parse_geojson_point(vp, vlen, &lat, &lng);
        if (e) return e; /* present but malformed: an error, like a real 2dsphere index */
        return rtree_insert(ix->rt, lat, lng, id);
    }
}

static int remove_from_one_index(dc_index *ix, const uint8_t *doc, size_t doc_len, const uint8_t id[12]) {
    if (ix->kind == DC_IDX_EQUALITY) {
        int applies = 1;
        int e = equality_index_applies(ix, doc, doc_len, &applies);
        if (e) return e;
        if (!applies) return BJ_OK; /* sparse/partialFilterExpression: was never indexed */

        dbuf key_bytes; memset(&key_bytes, 0, sizeof(key_bytes));
        e = build_index_key(ix, doc, doc_len, id, &key_bytes);
        if (e) { dbuf_free(&key_bytes); return e; }
        bpt_key key; key.is_string = 1; key.num = 0;
        key.str = key_bytes.data; key.str_len = (uint32_t)key_bytes.len;
        e = bpt_delete(ix->tree, &key);
        dbuf_free(&key_bytes);
        return e;
    }

    if (ix->kind == DC_IDX_TEXT) {
        const uint8_t *vp; size_t vlen; int found;
        int e = obj_get_field(doc, doc_len, (const uint8_t *)ix->text_field, ix->text_field_len, &vp, &vlen, &found);
        if (e) return e;
        if (!found || vlen < 1 || vp[0] != BJ_TYPE_STRING) return BJ_OK; /* was never indexed */
        char hex[24];
        oid_to_hex(id, hex);
        int removed = 0;
        return tix_remove(ix->tix_index, ix->tix_doc_terms, ix->tix_doc_lengths, NULL, hex, 24, &removed);
    }

    /* DC_IDX_GEO */
    {
        const uint8_t *vp; size_t vlen; int found;
        int e = obj_get_field(doc, doc_len, (const uint8_t *)ix->geo_field, ix->geo_field_len, &vp, &vlen, &found);
        if (e) return e;
        if (!found) return BJ_OK;
        double lat, lng;
        e = parse_geojson_point(vp, vlen, &lat, &lng);
        if (e) return e;
        int removed = 0;
        return rtree_remove_at(ix->rt, lat, lng, id, &removed);
    }
}

static int add_to_indexes(dc_collection *c, const uint8_t *doc, size_t doc_len, const uint8_t id[12]) {
    for (uint32_t i = 0; i < c->index_count; i++) {
        int e = add_to_one_index(&c->indexes[i], doc, doc_len, id);
        if (e) return e;
    }
    return BJ_OK;
}

static int remove_from_indexes(dc_collection *c, const uint8_t *doc, size_t doc_len, const uint8_t id[12]) {
    for (uint32_t i = 0; i < c->index_count; i++) {
        int e = remove_from_one_index(&c->indexes[i], doc, doc_len, id);
        if (e) return e;
    }
    return BJ_OK;
}

int dc_collection_add_index(dc_collection *c, const char *name, int name_len,
                            bpt *index_tree,
                            const uint8_t *fields, uint32_t fields_len,
                            int unique, int sparse,
                            const uint8_t *partial_filter, uint32_t partial_filter_len) {
    int e = dc_collection_attach_index(c, name, name_len, index_tree, fields, fields_len,
                                       unique, sparse, partial_filter, partial_filter_len);
    if (e) return e;
    e = backfill_index(c, name, name_len);
    if (e) return e; /* backfill_index already unregistered ix on failure */
    e = dctj_truncate(c);
    if (e) { dc_collection_remove_index(c, name, name_len); return e; }
    return BJ_OK;
}

int dc_collection_find_by_index(dc_collection *c, const char *name, int name_len,
                                const uint8_t *values, uint32_t values_len,
                                uint8_t **out, size_t *out_len) {
    *out = NULL; *out_len = 0;
    dc_index *ix = find_index(c, name, name_len);
    if (!ix) return BJ_ERR_STATE;

    cur vc = { values, values_len, 0 };
    uint32_t vcount;
    int e = array_begin(&vc, &vcount);
    if (e) return e;
    if (vcount != ix->field_count) return BJ_ERR_STATE;

    dbuf prefix; memset(&prefix, 0, sizeof(prefix));
    for (uint32_t i = 0; i < vcount && !e; i++) {
        size_t vstart = vc.pos;
        e = skip_value(&vc);
        if (!e) e = qk_put_value(&prefix, vc.d + vstart, vc.pos - vstart);
        /* Same translation as build_index_key_prefix: a lookup value with no
         * ordered encoding is DC_ERR_UNINDEXABLE_VALUE, not a generic
         * BJ_ERR_STATE. */
        if (e == BJ_ERR_STATE) e = DC_ERR_UNINDEXABLE_VALUE;
    }
    if (e) { dbuf_free(&prefix); return e; }

    dbuf upper; memset(&upper, 0, sizeof(upper));
    e = dbuf_put(&upper, prefix.data, prefix.len);
    if (!e) e = qk_put_upper_bound(&upper);
    if (e) { dbuf_free(&prefix); dbuf_free(&upper); return e; }

    bpt_key min_key; min_key.is_string = 1; min_key.num = 0;
    min_key.str = prefix.data; min_key.str_len = (uint32_t)prefix.len;
    bpt_key max_key; max_key.is_string = 1; max_key.num = 0;
    max_key.str = upper.data; max_key.str_len = (uint32_t)upper.len;

    bj_builder *b = bj_builder_new();
    if (!b) { dbuf_free(&prefix); dbuf_free(&upper); return BJ_ERR_OOM; }
    e = bj_begin_array(b);

    bpt_cursor *cur_h = NULL;
    if (!e) {
        cur_h = bpt_cursor_open(ix->tree, &min_key, &max_key);
        if (!cur_h) e = BJ_ERR_OOM;
    }
    while (!e && cur_h) {
        bpt_key k; const uint8_t *val; size_t vlen;
        int r = bpt_cursor_next(cur_h, &k, &val, &vlen);
        if (r < 0) { e = r; break; }
        if (r == 0) break;
        if (vlen != 13 || val[0] != BJ_TYPE_OID) { e = BJ_ERR_STATE; break; }
        bpt_key pkey; oid_key(val + 1, &pkey);
        int found = 0; const uint8_t *dp; size_t dn;
        e = bpt_search(c->primary, &pkey, &found, &dp, &dn);
        if (e) break;
        if (found) e = bj_put_raw(b, dp, (uint32_t)dn);
    }
    if (cur_h) bpt_cursor_close(cur_h);
    dbuf_free(&prefix);
    dbuf_free(&upper);
    if (!e) e = bj_end_array(b);

    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, out, out_len);
    }
    bj_builder_free(b);
    return e;
}

/* ---- equality-index planner ------------------------------------------------ */

/* If `cond` (a field's filter value) is a bare literal or {$eq: v}, writes
 * v's span through *val / *val_len and returns 1; 0 otherwise (any other
 * operator expression -- not an error, just "can't use this for planning",
 * the caller falls back to a full scan). */
static int cond_as_eq(const uint8_t *cond, size_t cond_len, const uint8_t **val, size_t *val_len) {
    int is_expr = 0;
    if (qry_is_operator_expr(cond, cond_len, &is_expr)) return 0;
    if (!is_expr) { *val = cond; *val_len = cond_len; return 1; }
    cur c = { cond, cond_len, 0 };
    uint32_t count;
    if (object_begin(&c, &count)) return 0;
    if (count != 1) return 0;
    const uint8_t *kp; uint32_t klen;
    if (take_key(&c, &kp, &klen)) return 0;
    size_t vstart = c.pos;
    if (skip_value(&c)) return 0;
    if (!(klen == 3 && memcmp(kp, "$eq", 3) == 0)) return 0;
    *val = c.d + vstart; *val_len = c.pos - vstart;
    return 1;
}

/*
 * If `filter`'s top level is a pure AND of {field: literal} / {field:
 * {$eq: v}} conditions that together pin every field of some attached
 * index (in that index's field order), builds the binjson ARRAY of those
 * values (index field order) into *values_out (caller frees) and returns
 * the index via *ix_out, with *applicable = 1. *applicable = 0 (not an
 * error) whenever no such index applies -- every caller re-applies the
 * full filter to whatever candidate set it gathers, so the result is
 * correct either way; this only chooses how candidates are gathered.
 * Deliberately conservative for this milestone: bails out of planning
 * entirely the moment the filter's top level has any $-prefixed key
 * ($and/$or/$nor), rather than looking inside them, and only ever performs
 * an equality lookup (no partial-prefix + range index usage yet).
 */
static int plan_equality_index(dc_collection *c, const uint8_t *filter, size_t filter_len,
                               dc_index **ix_out, uint8_t **values_out, size_t *values_out_len,
                               int *applicable) {
    *applicable = 0;
    cur fc = { filter, filter_len, 0 };
    uint32_t fcount;
    int e = object_begin(&fc, &fcount);
    if (e) return e;
    for (uint32_t i = 0; i < fcount; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&fc, &kp, &klen);
        if (e) return e;
        e = skip_value(&fc);
        if (e) return e;
        if (klen > 0 && kp[0] == '$') return BJ_OK;
    }

    for (uint32_t ixi = 0; ixi < c->index_count; ixi++) {
        dc_index *ix = &c->indexes[ixi];
        if (ix->kind != DC_IDX_EQUALITY) continue; /* text/geo indexes have no fields to "pin" */
        bj_builder *b = bj_builder_new();
        if (!b) return BJ_ERR_OOM;
        e = bj_begin_array(b);
        int ok = 1;
        for (uint32_t fi = 0; ok && !e && fi < ix->field_count; fi++) {
            const uint8_t *cond; size_t cond_len; int found;
            e = obj_get_field(filter, filter_len, ix->field_names[fi], ix->field_name_lens[fi], &cond, &cond_len, &found);
            if (e) break;
            if (!found) { ok = 0; break; }
            const uint8_t *val; size_t val_len;
            if (!cond_as_eq(cond, cond_len, &val, &val_len)) { ok = 0; break; }
            e = bj_put_raw(b, val, (uint32_t)val_len);
        }
        if (!e && ok) e = bj_end_array(b);
        if (!e && ok) {
            size_t n; const uint8_t *p = bj_builder_data(b, &n);
            if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
            if (!e) {
                e = dbuf_dup(p, n, values_out, values_out_len);
                if (!e) {
                    bj_builder_free(b);
                    *ix_out = ix;
                    *applicable = 1;
                    return BJ_OK;
                }
            }
        }
        bj_builder_free(b);
        if (e) return e;
        /* not ok for this index: try the next one */
    }
    return BJ_OK;
}

/* Growable list of matched documents, each an independently owned copy
 * (dbuf_dup) so it survives past the scan/cursor that produced it. */
static int push_match(qry_doc **matches, size_t *count, size_t *cap, const uint8_t *p, size_t n) {
    if (*count == *cap) {
        size_t ncap = *cap ? *cap * 2 : 8;
        qry_doc *nb = (qry_doc *)realloc(*matches, ncap * sizeof(qry_doc));
        if (!nb) return BJ_ERR_OOM;
        *matches = nb; *cap = ncap;
    }
    uint8_t *dp = NULL; size_t dl = 0;
    int e = dbuf_dup(p, n, &dp, &dl);
    if (e) return e;
    (*matches)[*count].ptr = dp;
    (*matches)[*count].len = dl;
    (*count)++;
    return BJ_OK;
}

static void free_matches(qry_doc *matches, size_t count) {
    for (size_t i = 0; i < count; i++) free((void *)matches[i].ptr);
    free(matches);
}

/* ---- $text / $near / $geoWithin: index-required special clauses --------- */

typedef enum { DC_ID_HEX_STRING, DC_ID_OID_FIELD } dc_id_shape;

/* Walk `entries` (a binjson ARRAY, each element an OBJECT carrying a row's
 * id under `id_field` — a hex-string doc id for a text-index posting, or a
 * raw OID field for an rtree hit), resolve each id to its document in
 * `c->primary`, and emit those documents (preserving `entries`' order,
 * e.g. BM25 rank or nearest-first) as a binjson ARRAY through
 * *out / *out_len (malloc'd, caller frees). An id that no longer resolves
 * to a document (deleted between being indexed and this read) is skipped,
 * not an error. */
static int ids_to_docs(dc_collection *c, const uint8_t *entries, size_t entries_len,
                       const char *id_field, dc_id_shape shape,
                       uint8_t **out, size_t *out_len) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_array(b);

    cur ec = { entries, entries_len, 0 };
    uint32_t ecount;
    if (!e) e = array_begin(&ec, &ecount);
    for (uint32_t i = 0; !e && i < ecount; i++) {
        size_t estart = ec.pos;
        e = skip_value(&ec);
        if (e) break;
        const uint8_t *idp; size_t idlen; int idfound;
        e = obj_get_field(ec.d + estart, ec.pos - estart,
                          (const uint8_t *)id_field, (uint32_t)strlen(id_field), &idp, &idlen, &idfound);
        if (e) break;
        if (!idfound) continue;

        uint8_t oid[12];
        if (shape == DC_ID_OID_FIELD) {
            if (idlen != 13 || idp[0] != BJ_TYPE_OID) { e = BJ_ERR_STATE; break; }
            memcpy(oid, idp + 1, 12);
        } else {
            if (idlen < 1 || idp[0] != BJ_TYPE_STRING) { e = BJ_ERR_STATE; break; }
            cur idc = { idp, idlen, 0 };
            const uint8_t *hexs; uint32_t hexlen;
            e = take_string(&idc, &hexs, &hexlen);
            if (e) break;
            if (hex_to_oid((const char *)hexs, (int)hexlen, oid)) continue; /* malformed id: skip defensively */
        }

        bpt_key pkey; oid_key(oid, &pkey);
        int found = 0; const uint8_t *dp; size_t dn;
        e = bpt_search(c->primary, &pkey, &found, &dp, &dn);
        if (e) break;
        if (found) e = bj_put_raw(b, dp, (uint32_t)dn);
    }
    if (!e) e = bj_end_array(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, out, out_len);
    }
    bj_builder_free(b);
    return e;
}

/* {$geometry: {type:"Point", coordinates:[lng,lat]}, $maxDistance: km?} --
 * the value of a field's $near key. Distance is in kilometers (see db.h). */
static int parse_near(const uint8_t *cond, size_t cond_len, double *lat, double *lng,
                      int *has_max, double *max_km) {
    if (cond_len < 1 || cond[0] != BJ_TYPE_OBJECT) return BJ_ERR_STATE;
    const uint8_t *gp; size_t glen; int gfound;
    int e = obj_get_field(cond, cond_len, (const uint8_t *)"$geometry", 9, &gp, &glen, &gfound);
    if (e) return e;
    if (!gfound) return BJ_ERR_STATE;
    e = parse_geojson_point(gp, glen, lat, lng);
    if (e) return e;

    const uint8_t *mp; size_t mlen; int mfound;
    e = obj_get_field(cond, cond_len, (const uint8_t *)"$maxDistance", 12, &mp, &mlen, &mfound);
    if (e) return e;
    *has_max = mfound;
    if (mfound) {
        cur mc = { mp, mlen, 0 };
        e = read_number(&mc, max_km);
        if (e) return e;
    }
    return BJ_OK;
}

/* {$box: [[minLng,minLat],[maxLng,maxLat]]} -- corners in either order. */
static int parse_box(const uint8_t *val, size_t val_len,
                     double *min_lat, double *max_lat, double *min_lng, double *max_lng) {
    if (val_len < 1 || val[0] != BJ_TYPE_ARRAY) return BJ_ERR_STATE;
    cur c = { val, val_len, 0 };
    uint32_t count;
    int e = array_begin(&c, &count);
    if (e) return e;
    if (count != 2) return BJ_ERR_STATE;

    size_t p0 = c.pos;
    e = skip_value(&c);
    if (e) return e;
    double lng0, lat0;
    e = read_number_pair(c.d + p0, c.pos - p0, &lng0, &lat0);
    if (e) return e;

    size_t p1 = c.pos;
    e = skip_value(&c);
    if (e) return e;
    double lng1, lat1;
    e = read_number_pair(c.d + p1, c.pos - p1, &lng1, &lat1);
    if (e) return e;

    *min_lng = lng0 < lng1 ? lng0 : lng1;
    *max_lng = lng0 < lng1 ? lng1 : lng0;
    *min_lat = lat0 < lat1 ? lat0 : lat1;
    *max_lat = lat0 < lat1 ? lat1 : lat0;
    return BJ_OK;
}

/* {$center: [[lng,lat], radiusKm]} -- legacy-shaped, radius in kilometers
 * (see db.h) rather than $centerSphere's radians. */
static int parse_center(const uint8_t *val, size_t val_len, double *lat, double *lng, double *radius_km) {
    if (val_len < 1 || val[0] != BJ_TYPE_ARRAY) return BJ_ERR_STATE;
    cur c = { val, val_len, 0 };
    uint32_t count;
    int e = array_begin(&c, &count);
    if (e) return e;
    if (count != 2) return BJ_ERR_STATE;

    size_t pstart = c.pos;
    e = skip_value(&c);
    if (e) return e;
    double x, y;
    e = read_number_pair(c.d + pstart, c.pos - pstart, &x, &y);
    if (e) return e;

    size_t rstart = c.pos;
    e = skip_value(&c);
    if (e) return e;
    cur rc = { c.d + rstart, c.pos - rstart, 0 };
    e = read_number(&rc, radius_km);
    if (e) return e;

    *lng = x; *lat = y;
    return BJ_OK;
}

/* {$box: [...]} or {$center: [...]} -- the value of a field's $geoWithin key. */
static int parse_geo_within(const uint8_t *cond, size_t cond_len, int *is_box,
                            double *a1, double *a2, double *a3, double *a4) {
    if (cond_len < 1 || cond[0] != BJ_TYPE_OBJECT) return BJ_ERR_STATE;
    const uint8_t *bp; size_t blen; int bfound;
    int e = obj_get_field(cond, cond_len, (const uint8_t *)"$box", 4, &bp, &blen, &bfound);
    if (e) return e;
    if (bfound) {
        *is_box = 1;
        return parse_box(bp, blen, a1, a2, a3, a4); /* min_lat, max_lat, min_lng, max_lng */
    }
    const uint8_t *cp; size_t clen; int cfound;
    e = obj_get_field(cond, cond_len, (const uint8_t *)"$center", 7, &cp, &clen, &cfound);
    if (e) return e;
    if (!cfound) return BJ_ERR_STATE;
    *is_box = 0;
    return parse_center(cp, clen, a1, a2, a3); /* lat, lng, radius_km */
}

/* Trim an rtree_nearest-shaped ARRAY (ascending-sorted by `distance`) to
 * only entries with distance <= max_km, stopping at the first exceedance. */
static int trim_by_max_distance(const uint8_t *entries, size_t entries_len, double max_km,
                                uint8_t **out, size_t *out_len) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_array(b);
    cur c = { entries, entries_len, 0 };
    uint32_t count;
    if (!e) e = array_begin(&c, &count);
    for (uint32_t i = 0; !e && i < count; i++) {
        size_t estart = c.pos;
        e = skip_value(&c);
        if (e) break;
        const uint8_t *dp; size_t dlen; int dfound;
        e = obj_get_field(c.d + estart, c.pos - estart, (const uint8_t *)"distance", 8, &dp, &dlen, &dfound);
        if (e) break;
        if (!dfound) { e = BJ_ERR_STATE; break; }
        cur dc2 = { dp, dlen, 0 };
        double dist;
        e = read_number(&dc2, &dist);
        if (e) break;
        if (dist > max_km) break; /* ascending sorted: nothing further qualifies */
        e = bj_put_raw(b, c.d + estart, (uint32_t)(c.pos - estart));
    }
    if (!e) e = bj_end_array(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, out, out_len);
    }
    bj_builder_free(b);
    return e;
}

/* `filter` with the top-level field `skip_key` removed -- the filter to
 * re-apply to each candidate a special clause produces, since the clause
 * itself already accounts for that one field/key. */
static int build_residual_filter(const uint8_t *filter, size_t filter_len,
                                 const uint8_t *skip_key, uint32_t skip_key_len,
                                 uint8_t **out, size_t *out_len) {
    cur c = { filter, filter_len, 0 };
    uint32_t count;
    int e = object_begin(&c, &count);
    if (e) return e;
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    e = bj_begin_object(b);
    for (uint32_t i = 0; !e && i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&c, &kp, &klen);
        if (e) break;
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) break;
        if (klen == skip_key_len && memcmp(kp, skip_key, klen) == 0) continue;
        e = bj_put_key(b, kp, klen);
        if (!e) e = bj_put_raw(b, c.d + vstart, (uint32_t)(c.pos - vstart));
    }
    if (!e) e = bj_end_object(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, out, out_len);
    }
    bj_builder_free(b);
    return e;
}

typedef struct {
    int use_index;                  /* 0 = full scan (ignore the rest); 1 = use cand/residual */
    uint8_t *cand; size_t cand_len;  /* binjson ARRAY of candidate documents, in result order */
    uint8_t *residual; size_t residual_len; /* filter to re-apply to each candidate */
} dc_source;

static void dc_source_free(dc_source *src) {
    free(src->cand);
    free(src->residual);
}

/*
 * If `filter`'s top level names a $text search, or a field's condition is
 * exactly {$near: ...} or {$geoWithin: ...}, resolves it via the matching
 * attached index (BJ_ERR_STATE if none exists -- these operators require
 * an index, like real MongoDB's own $near) and writes the candidate
 * document set + residual filter through `src`, with src->use_index = 1.
 * src->use_index stays 0 (not an error) when `filter` has no such clause
 * -- the caller falls back to the equality planner or a full scan. At
 * most one clause is recognized (MongoDB itself doesn't allow combining
 * $text with $near), and only at the filter's top level -- one nested
 * under $and/$or/$nor is not detected (falls through to a full scan, where
 * db_query.c's evaluator will reject $near/$geoWithin as unrecognized
 * operators rather than silently ignoring them).
 */
/*
 * Detection half of the special-source dispatch, shared by
 * resolve_special_source (which goes on to execute the query) and
 * dc_explain (which only reports the choice): scans `filter`'s top level
 * for the first special operator ($text, or a field whose value is
 * exactly {$near: ...}/{$geoWithin: ...}) and resolves the index that
 * would serve it. *found stays 0 when nothing special applies; a special
 * operator without its required index is BJ_ERR_STATE, exactly the error
 * executing the query would raise.
 */
static int detect_special_source(dc_collection *c, const uint8_t *filter, size_t filter_len,
                                 int *found, int *is_text,
                                 const uint8_t **skey_out, uint32_t *skey_len_out,
                                 const uint8_t **sval_out, size_t *sval_len_out,
                                 dc_index **ix_out) {
    *found = 0; *is_text = 0; *ix_out = NULL;

    cur fc = { filter, filter_len, 0 };
    uint32_t fcount;
    int e = object_begin(&fc, &fcount);
    if (e) return e;

    const uint8_t *skey = NULL; uint32_t skey_len = 0;
    const uint8_t *sval = NULL; size_t sval_len = 0;

    for (uint32_t i = 0; i < fcount; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&fc, &kp, &klen);
        if (e) return e;
        size_t vstart = fc.pos;
        e = skip_value(&fc);
        if (e) return e;
        const uint8_t *v = fc.d + vstart; size_t vlen = fc.pos - vstart;

        if (klen == 5 && memcmp(kp, "$text", 5) == 0) {
            skey = kp; skey_len = klen; sval = v; sval_len = vlen; *is_text = 1;
            break;
        }
        if (vlen >= 1 && v[0] == BJ_TYPE_OBJECT) {
            cur vc = { v, vlen, 0 };
            uint32_t vcount;
            if (!object_begin(&vc, &vcount) && vcount == 1) {
                const uint8_t *okp; uint32_t oklen;
                if (!take_key(&vc, &okp, &oklen)) {
                    if ((oklen == 5 && memcmp(okp, "$near", 5) == 0) ||
                        (oklen == 10 && memcmp(okp, "$geoWithin", 10) == 0)) {
                        skey = kp; skey_len = klen; sval = v; sval_len = vlen; *is_text = 0;
                        break;
                    }
                }
            }
        }
    }

    if (!skey) return BJ_OK; /* nothing special */

    if (*is_text) {
        for (uint32_t i = 0; i < c->index_count; i++) {
            if (c->indexes[i].kind == DC_IDX_TEXT) { *ix_out = &c->indexes[i]; break; }
        }
        if (!*ix_out) return BJ_ERR_STATE; /* $text requires a text index */
    } else {
        for (uint32_t i = 0; i < c->index_count; i++) {
            if (c->indexes[i].kind == DC_IDX_GEO &&
                c->indexes[i].geo_field_len == skey_len &&
                memcmp(c->indexes[i].geo_field, skey, skey_len) == 0) {
                *ix_out = &c->indexes[i]; break;
            }
        }
        if (!*ix_out) return BJ_ERR_STATE; /* $near/$geoWithin requires a geo index on this field */
    }

    *found = 1;
    *skey_out = skey; *skey_len_out = skey_len;
    *sval_out = sval; *sval_len_out = sval_len;
    return BJ_OK;
}

static int resolve_special_source(dc_collection *c, const uint8_t *filter, size_t filter_len, dc_source *src) {
    memset(src, 0, sizeof(*src));

    int found = 0, is_text = 0;
    const uint8_t *skey = NULL; uint32_t skey_len = 0;
    const uint8_t *sval = NULL; size_t sval_len = 0;
    dc_index *six = NULL;
    int e = detect_special_source(c, filter, filter_len, &found, &is_text,
                                  &skey, &skey_len, &sval, &sval_len, &six);
    if (e) return e;
    if (!found) return BJ_OK; /* nothing special; src->use_index stays 0 */

    uint8_t *raw = NULL; size_t raw_len = 0; /* entries array, before id resolution */

    if (is_text) {
        dc_index *tix = six;

        const uint8_t *sp; size_t slen; int sfound;
        e = obj_get_field(sval, sval_len, (const uint8_t *)"$search", 7, &sp, &slen, &sfound);
        if (e) return e;
        if (!sfound || slen < 1 || sp[0] != BJ_TYPE_STRING) return BJ_ERR_STATE;
        cur sc = { sp, slen, 0 };
        const uint8_t *stext; uint32_t stext_len;
        e = take_string(&sc, &stext, &stext_len);
        if (e) return e;

        e = tix_query(tix->tix_index, tix->tix_doc_terms, tix->tix_doc_lengths,
                     (const char *)stext, (int)stext_len, &raw, &raw_len);
        if (e) { free(raw); return e; }

        e = ids_to_docs(c, raw, raw_len, "id", DC_ID_HEX_STRING, &src->cand, &src->cand_len);
        free(raw);
        if (e) return e;
    } else {
        dc_index *ix = six;

        cur vc = { sval, sval_len, 0 };
        uint32_t vcount;
        e = object_begin(&vc, &vcount);
        if (e) return e;
        const uint8_t *okp; uint32_t oklen;
        e = take_key(&vc, &okp, &oklen);
        if (e) return e;
        size_t ovstart = vc.pos;
        e = skip_value(&vc);
        if (e) return e;
        const uint8_t *oval = vc.d + ovstart; size_t oval_len = vc.pos - ovstart;

        const uint8_t *tmp = NULL; size_t tmp_len = 0; /* rtree's transient output */

        if (oklen == 5 && memcmp(okp, "$near", 5) == 0) {
            double lat, lng; int has_max = 0; double max_km = 0;
            e = parse_near(oval, oval_len, &lat, &lng, &has_max, &max_km);
            if (e) return e;
            e = rtree_nearest(ix->rt, lat, lng, (int)rtree_size(ix->rt), &tmp, &tmp_len);
            if (e) return e;
            if (has_max) e = trim_by_max_distance(tmp, tmp_len, max_km, &raw, &raw_len);
            else e = dbuf_dup(tmp, tmp_len, &raw, &raw_len);
            if (e) { free(raw); return e; }
        } else if (oklen == 10 && memcmp(okp, "$geoWithin", 10) == 0) {
            int is_box = 0; double a1 = 0, a2 = 0, a3 = 0, a4 = 0;
            e = parse_geo_within(oval, oval_len, &is_box, &a1, &a2, &a3, &a4);
            if (e) return e;
            if (is_box) e = rtree_search_bbox(ix->rt, a1, a2, a3, a4, &tmp, &tmp_len);
            else e = rtree_search_radius(ix->rt, a1, a2, a3, &tmp, &tmp_len);
            if (e) return e;
            e = dbuf_dup(tmp, tmp_len, &raw, &raw_len);
            if (e) { free(raw); return e; }
        } else {
            return BJ_ERR_STATE; /* {field: {$near/$geoWithin: ...}} was the only shape checked for above */
        }

        e = ids_to_docs(c, raw, raw_len, "objectId", DC_ID_OID_FIELD, &src->cand, &src->cand_len);
        free(raw);
        if (e) return e;
    }

    e = build_residual_filter(filter, filter_len, skey, skey_len, &src->residual, &src->residual_len);
    if (e) { free(src->cand); src->cand = NULL; return e; }
    src->use_index = 1;
    return BJ_OK;
}

/*
 * Gather every document in `c` matching `filter` (special $text/$near/
 * $geoWithin source, else the equality-index planner, else a full scan --
 * the same dispatch dc_find_one/dc_count use) into a freshly built qry_doc
 * array. Each entry is a dbuf_dup'd copy (push_match's own doing),
 * independent of any live scan state, so a caller may safely mutate the
 * primary tree/indexes while iterating the result (dc_update_many/
 * dc_delete_many both do). Caller frees via free_matches. Shared by
 * dc_find, dc_update_many, dc_delete_many and dc_distinct -- dc_count
 * deliberately does not use this: it only needs a count, not materialized
 * document bytes, and this would regress a large unfiltered count from
 * O(1) to O(every match held in memory) for no benefit.
 */
/*
 * Report which candidate source the shared dispatch (gather_matches,
 * dc_cursor_open, dc_find_one) would use for `filter`, without gathering
 * or executing anything -- the whole point is that this consults the very
 * same planners the queries run (detect_special_source,
 * plan_equality_index, filter_is_id_only), so it can never drift from
 * what actually happens. kinds: 0 full scan, 1 {_id} point lookup,
 * 2 equality index, 3 text index, 4 geo index. For kinds 2-4 the serving
 * index's name is dbuf_dup'd into *name_out (caller frees).
 */
int dc_explain(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
               int *kind_out, uint8_t **name_out, size_t *name_len_out) {
    *kind_out = 0; *name_out = NULL; *name_len_out = 0;

    uint8_t id[12]; int is_id = 0;
    int e = filter_is_id_only(filter, filter_len, id, &is_id);
    if (e) return e;
    if (is_id) { *kind_out = 1; return BJ_OK; }

    int found = 0, is_text = 0;
    const uint8_t *skey; uint32_t skey_len; const uint8_t *sval; size_t sval_len;
    dc_index *ix = NULL;
    e = detect_special_source(c, filter, filter_len, &found, &is_text,
                              &skey, &skey_len, &sval, &sval_len, &ix);
    if (e) return e;
    if (found) {
        *kind_out = is_text ? 3 : 4;
        return dbuf_dup((const uint8_t *)ix->name, ix->name_len, name_out, name_len_out);
    }

    uint8_t *values = NULL; size_t values_len = 0; int planned = 0;
    e = plan_equality_index(c, filter, filter_len, &ix, &values, &values_len, &planned);
    if (e) return e;
    free(values);
    if (planned) {
        *kind_out = 2;
        return dbuf_dup((const uint8_t *)ix->name, ix->name_len, name_out, name_len_out);
    }
    return BJ_OK;
}

static int gather_matches(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                          qry_doc **out_matches, size_t *out_count) {
    *out_matches = NULL; *out_count = 0;

    /* {_id: <oid>} point lookup -- the same fast path dc_find_one takes,
     * so find()/updateMany/deleteMany/distinct on an exact id no longer
     * pay a full scan (and dc_explain's kind 1 is the truth for every
     * caller of this dispatch). The lookup IS the whole filter (a single
     * {_id: oid} key -- filter_is_id_only's contract), so no re-check. */
    uint8_t fid[12]; int fis_id = 0;
    int e = filter_is_id_only(filter, filter_len, fid, &fis_id);
    if (e) return e;
    if (fis_id) {
        bpt_key key; oid_key(fid, &key);
        int f = 0; const uint8_t *p; size_t n;
        e = bpt_search(c->primary, &key, &f, &p, &n);
        if (e) return e;
        qry_doc *matches = NULL; size_t match_count = 0, match_cap = 0;
        if (f) {
            e = push_match(&matches, &match_count, &match_cap, p, n);
            if (e) { free_matches(matches, match_count); return e; }
        }
        *out_matches = matches; *out_count = match_count;
        return BJ_OK;
    }

    dc_source src;
    e = resolve_special_source(c, filter, filter_len, &src);
    if (e) { dc_source_free(&src); return e; }

    qry_doc *matches = NULL;
    size_t match_count = 0, match_cap = 0;

    if (src.use_index) {
        cur cc = { src.cand, src.cand_len, 0 };
        uint32_t ccount;
        e = array_begin(&cc, &ccount);
        for (uint32_t i = 0; !e && i < ccount; i++) {
            size_t dstart = cc.pos;
            e = skip_value(&cc);
            if (e) break;
            int m = 0;
            e = qry_matches(cc.d + dstart, cc.pos - dstart, src.residual, src.residual_len, &m);
            if (e) break;
            if (m) e = push_match(&matches, &match_count, &match_cap, cc.d + dstart, cc.pos - dstart);
            if (e) break;
        }
        dc_source_free(&src);
        if (e) { free_matches(matches, match_count); return e; }
        *out_matches = matches; *out_count = match_count;
        return BJ_OK;
    }
    dc_source_free(&src);

    dc_index *ix = NULL; uint8_t *ix_values = NULL; size_t ix_values_len = 0;
    int planned = 0;
    e = plan_equality_index(c, filter, filter_len, &ix, &ix_values, &ix_values_len, &planned);
    if (e) return e;

    if (planned) {
        uint8_t *cand = NULL; size_t cand_len = 0;
        e = dc_collection_find_by_index(c, ix->name, (int)ix->name_len,
                                        ix_values, (uint32_t)ix_values_len, &cand, &cand_len);
        free(ix_values);
        if (e) { free(cand); return e; }

        cur cc = { cand, cand_len, 0 };
        uint32_t ccount;
        e = array_begin(&cc, &ccount);
        for (uint32_t i = 0; !e && i < ccount; i++) {
            size_t dstart = cc.pos;
            e = skip_value(&cc);
            if (e) break;
            int m = 0;
            e = qry_matches(cc.d + dstart, cc.pos - dstart, filter, filter_len, &m);
            if (e) break;
            if (m) e = push_match(&matches, &match_count, &match_cap, cc.d + dstart, cc.pos - dstart);
            if (e) break;
        }
        free(cand);
    } else {
        free(ix_values);
        bpt_cursor *cur_h = bpt_cursor_open(c->primary, NULL, NULL);
        if (!cur_h) e = BJ_ERR_OOM;
        while (!e && cur_h) {
            bpt_key k; const uint8_t *val; size_t vlen;
            int r = bpt_cursor_next(cur_h, &k, &val, &vlen);
            if (r < 0) { e = r; break; }
            if (r == 0) break;
            int m = 0;
            e = qry_matches(val, vlen, filter, filter_len, &m);
            if (e) break;
            if (m) e = push_match(&matches, &match_count, &match_cap, val, vlen);
        }
        if (cur_h) bpt_cursor_close(cur_h);
    }

    if (e) { free_matches(matches, match_count); return e; }
    *out_matches = matches; *out_count = match_count;
    return BJ_OK;
}

/* ---- CRUD ---------------------------------------------------------------- */

int dc_insert_one(dc_collection *c, const uint8_t *doc, uint32_t doc_len) {
    uint8_t id[12];
    int e = dc_document_id(doc, doc_len, id);
    if (e) return e;
    bpt_key key; oid_key(id, &key);
    int found = 0;
    const uint8_t *p; size_t n;
    e = bpt_search(c->primary, &key, &found, &p, &n);
    if (e) return e;
    if (found) return DC_ERR_DUPLICATE;
    e = check_unique_indexes(c, doc, doc_len);
    if (e) return e;
    uint64_t *undo;
    e = mut_begin(c, &undo);
    if (e) return e;
    e = bpt_add(c->primary, &key, doc, doc_len);
    if (!e) e = add_to_indexes(c, doc, doc_len, id);
    if (!e) e = commit_journal(c);
    return mut_end(c, undo, e);
}

int dc_insert_many(dc_collection *c, const uint8_t *docs, uint32_t docs_len,
                   int ordered, uint8_t **out, size_t *out_len) {
    *out = NULL; *out_len = 0;
    cur cu = { docs, docs_len, 0 };
    uint32_t count;
    int e = array_begin(&cu, &count);
    if (e) return e;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    e = bj_begin_array(b);
    for (uint32_t i = 0; !e && i < count; i++) {
        size_t dstart = cu.pos;
        e = skip_value(&cu);
        if (e) break;
        int rc = dc_insert_one(c, cu.d + dstart, (uint32_t)(cu.pos - dstart));
        e = bj_put_int(b, rc);
        if (!e && rc != BJ_OK && ordered) break; /* result array ends up shorter than docs */
    }
    if (!e) e = bj_end_array(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, out, out_len);
    }
    bj_builder_free(b);
    return e;
}

/* Emits a found document through out/out_len: projected if projection_len
 * is non-zero, a plain byte copy otherwise -- shared by every match point
 * in dc_find_one below so "project if asked, copy if not" is expressed
 * once rather than at each of its four branches. */
static int emit_found(const uint8_t *doc, size_t doc_len,
                      const uint8_t *projection, uint32_t projection_len,
                      uint8_t **out, size_t *out_len) {
    if (projection_len) return qry_project_one(doc, doc_len, projection, projection_len, out, out_len);
    return dbuf_dup(doc, doc_len, out, out_len);
}

int dc_find_one(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                const uint8_t *projection, uint32_t projection_len,
                int *found, uint8_t **out, size_t *out_len) {
    *found = 0; *out = NULL; *out_len = 0;

    uint8_t id[12]; int is_id;
    int e = filter_is_id_only(filter, filter_len, id, &is_id);
    if (e) return e;

    if (is_id) {
        bpt_key key; oid_key(id, &key);
        int f = 0; const uint8_t *p; size_t n;
        e = bpt_search(c->primary, &key, &f, &p, &n);
        if (e || !f) return e;
        *found = 1;
        return emit_found(p, n, projection, projection_len, out, out_len);
    }

    dc_source src;
    e = resolve_special_source(c, filter, filter_len, &src);
    if (e) { dc_source_free(&src); return e; }
    if (src.use_index) {
        cur cc = { src.cand, src.cand_len, 0 };
        uint32_t ccount;
        e = array_begin(&cc, &ccount);
        for (uint32_t i = 0; !e && i < ccount; i++) {
            size_t dstart = cc.pos;
            e = skip_value(&cc);
            if (e) break;
            int m = 0;
            e = qry_matches(cc.d + dstart, cc.pos - dstart, src.residual, src.residual_len, &m);
            if (e) break;
            if (m) {
                *found = 1;
                e = emit_found(cc.d + dstart, cc.pos - dstart, projection, projection_len, out, out_len);
                break;
            }
        }
        dc_source_free(&src);
        return e;
    }
    dc_source_free(&src);

    dc_index *ix = NULL; uint8_t *ix_values = NULL; size_t ix_values_len = 0;
    int planned = 0;
    e = plan_equality_index(c, filter, filter_len, &ix, &ix_values, &ix_values_len, &planned);
    if (e) return e;

    if (planned) {
        uint8_t *cand = NULL; size_t cand_len = 0;
        e = dc_collection_find_by_index(c, ix->name, (int)ix->name_len,
                                        ix_values, (uint32_t)ix_values_len, &cand, &cand_len);
        free(ix_values);
        if (e) { free(cand); return e; }

        cur cc = { cand, cand_len, 0 };
        uint32_t ccount;
        e = array_begin(&cc, &ccount);
        for (uint32_t i = 0; !e && i < ccount; i++) {
            size_t dstart = cc.pos;
            e = skip_value(&cc);
            if (e) break;
            int m = 0;
            e = qry_matches(cc.d + dstart, cc.pos - dstart, filter, filter_len, &m);
            if (e) break;
            if (m) {
                *found = 1;
                e = emit_found(cc.d + dstart, cc.pos - dstart, projection, projection_len, out, out_len);
                break;
            }
        }
        free(cand);
        return e;
    }
    free(ix_values);

    bpt_cursor *cur_h = bpt_cursor_open(c->primary, NULL, NULL);
    if (!cur_h) return BJ_ERR_OOM;
    int rc = BJ_OK;
    for (;;) {
        bpt_key k; const uint8_t *val; size_t vlen;
        int r = bpt_cursor_next(cur_h, &k, &val, &vlen);
        if (r < 0) { rc = r; break; }
        if (r == 0) break;
        int m = 0;
        rc = qry_matches(val, vlen, filter, filter_len, &m);
        if (rc) break;
        if (m) {
            *found = 1;
            rc = emit_found(val, vlen, projection, projection_len, out, out_len);
            break;
        }
    }
    bpt_cursor_close(cur_h);
    return rc;
}

int dc_find(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
            const qry_options *opts, uint8_t **out, size_t *out_len) {
    *out = NULL; *out_len = 0;

    int e = qry_validate_options(opts);
    if (e) return e;

    qry_doc *matches; size_t match_count;
    e = gather_matches(c, filter, filter_len, &matches, &match_count);
    if (e) return e;

    e = qry_collect(matches, match_count, opts, out, out_len);
    free_matches(matches, match_count);
    return e;
}

/*
 * dc_cursor -- see db.h's top comment on it. Mirrors gather_matches's own
 * three-way dispatch (special $text/$near/$geoWithin source, else the
 * equality-index planner, else a full scan) but keeps whichever source
 * turned out to apply as persistent, resumable state instead of draining
 * it into a qry_doc array before returning. scan_filter/scan_filter_len
 * is whichever filter bytes should be re-checked against each raw
 * candidate in this cursor's mode: the caller's full filter for the
 * full-scan and index-planned cases (matching gather_matches's own
 * behavior), or the special source's residual for that case (the part of
 * the filter not already satisfied by the source) -- exactly one
 * consistent thing to re-check regardless of which mode is active.
 */
typedef enum { DC_CURSOR_SCAN, DC_CURSOR_CANDIDATES } dc_cursor_mode;

struct dc_cursor {
    dc_cursor_mode mode;
    int done;

    uint8_t *scan_filter; uint32_t scan_filter_len;   /* owned */
    uint8_t *projection; uint32_t projection_len;      /* owned; projection_len 0 = none */
    int64_t skip_remaining;
    int64_t limit_remaining; /* -1 = unlimited */

    bpt_cursor *bpt_cur; /* DC_CURSOR_SCAN only */

    uint8_t *cand; size_t cand_len; /* DC_CURSOR_CANDIDATES only: owned binjson ARRAY */
    cur cand_walk;                  /* resumable walk state over `cand` */
    uint32_t cand_count, cand_seen;
};

void dc_cursor_close(dc_cursor *dcur) {
    if (!dcur) return;
    if (dcur->bpt_cur) bpt_cursor_close(dcur->bpt_cur);
    free(dcur->cand);
    free(dcur->scan_filter);
    free(dcur->projection);
    free(dcur);
}

/* Releases just the underlying scan resource once exhausted -- the cursor struct itself lives until dc_cursor_close. */
static void dc_cursor_release_scan(dc_cursor *dcur) {
    if (dcur->bpt_cur) { bpt_cursor_close(dcur->bpt_cur); dcur->bpt_cur = NULL; }
    free(dcur->cand); dcur->cand = NULL;
}

int dc_cursor_open(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                   const uint8_t *projection, uint32_t projection_len,
                   int64_t skip, int64_t limit, dc_cursor **out_cursor) {
    *out_cursor = NULL;

    dc_cursor *dcur = (dc_cursor *)calloc(1, sizeof(dc_cursor));
    if (!dcur) return BJ_ERR_OOM;
    dcur->skip_remaining = skip > 0 ? skip : 0;
    dcur->limit_remaining = limit > 0 ? limit : -1;

    if (projection_len) {
        dcur->projection = (uint8_t *)malloc(projection_len);
        if (!dcur->projection) { dc_cursor_close(dcur); return BJ_ERR_OOM; }
        memcpy(dcur->projection, projection, projection_len);
        dcur->projection_len = projection_len;
    }

    /* {_id: <oid>} point lookup, mirroring gather_matches: a one-entry
     * (or empty) candidates array in place of any scan. scan_filter keeps
     * the original filter -- re-checking {_id: oid} against the looked-up
     * document is a trivially true no-op, and it keeps this mode's
     * invariants identical to the planned-index mode below. */
    uint8_t fid[12]; int fis_id = 0;
    int e = filter_is_id_only(filter, filter_len, fid, &fis_id);
    if (e) { dc_cursor_close(dcur); return e; }
    if (fis_id) {
        bpt_key key; oid_key(fid, &key);
        int f = 0; const uint8_t *p; size_t n;
        e = bpt_search(c->primary, &key, &f, &p, &n);
        if (e) { dc_cursor_close(dcur); return e; }
        bj_builder *b = bj_builder_new();
        if (!b) { dc_cursor_close(dcur); return BJ_ERR_OOM; }
        e = bj_begin_array(b);
        if (!e && f) e = bj_put_raw(b, p, (uint32_t)n);
        if (!e) e = bj_end_array(b);
        if (!e) {
            size_t bn; const uint8_t *bp = bj_builder_data(b, &bn);
            if (!bp) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
            else e = dbuf_dup(bp, bn, &dcur->cand, &dcur->cand_len);
        }
        bj_builder_free(b);
        if (e) { dc_cursor_close(dcur); return e; }

        dcur->scan_filter = (uint8_t *)malloc(filter_len ? filter_len : 1);
        if (!dcur->scan_filter) { dc_cursor_close(dcur); return BJ_ERR_OOM; }
        memcpy(dcur->scan_filter, filter, filter_len);
        dcur->scan_filter_len = filter_len;

        dcur->mode = DC_CURSOR_CANDIDATES;
        dcur->cand_walk = (cur){ dcur->cand, dcur->cand_len, 0 };
        e = array_begin(&dcur->cand_walk, &dcur->cand_count);
        if (e) { dc_cursor_close(dcur); return e; }
        *out_cursor = dcur;
        return BJ_OK;
    }

    dc_source src;
    e = resolve_special_source(c, filter, filter_len, &src);
    if (e) { dc_source_free(&src); dc_cursor_close(dcur); return e; }

    if (src.use_index) {
        dcur->mode = DC_CURSOR_CANDIDATES;
        dcur->cand = src.cand; dcur->cand_len = src.cand_len; src.cand = NULL;
        dcur->scan_filter = src.residual; dcur->scan_filter_len = (uint32_t)src.residual_len; src.residual = NULL;
        dc_source_free(&src); /* no-op now: both owned fields already taken */

        dcur->cand_walk = (cur){ dcur->cand, dcur->cand_len, 0 };
        e = array_begin(&dcur->cand_walk, &dcur->cand_count);
        if (e) { dc_cursor_close(dcur); return e; }
        *out_cursor = dcur;
        return BJ_OK;
    }
    dc_source_free(&src);

    dc_index *ix = NULL; uint8_t *ix_values = NULL; size_t ix_values_len = 0;
    int planned = 0;
    e = plan_equality_index(c, filter, filter_len, &ix, &ix_values, &ix_values_len, &planned);
    if (e) { dc_cursor_close(dcur); return e; }

    dcur->scan_filter = (uint8_t *)malloc(filter_len ? filter_len : 1);
    if (!dcur->scan_filter) { free(ix_values); dc_cursor_close(dcur); return BJ_ERR_OOM; }
    memcpy(dcur->scan_filter, filter, filter_len);
    dcur->scan_filter_len = filter_len;

    if (planned) {
        uint8_t *cand = NULL; size_t cand_len = 0;
        e = dc_collection_find_by_index(c, ix->name, (int)ix->name_len,
                                        ix_values, (uint32_t)ix_values_len, &cand, &cand_len);
        free(ix_values);
        if (e) { free(cand); dc_cursor_close(dcur); return e; }

        dcur->mode = DC_CURSOR_CANDIDATES;
        dcur->cand = cand; dcur->cand_len = cand_len;
        dcur->cand_walk = (cur){ dcur->cand, dcur->cand_len, 0 };
        e = array_begin(&dcur->cand_walk, &dcur->cand_count);
        if (e) { dc_cursor_close(dcur); return e; }
        *out_cursor = dcur;
        return BJ_OK;
    }
    free(ix_values);

    dcur->mode = DC_CURSOR_SCAN;
    dcur->bpt_cur = bpt_cursor_open(c->primary, NULL, NULL);
    if (!dcur->bpt_cur) { dc_cursor_close(dcur); return BJ_ERR_OOM; }
    *out_cursor = dcur;
    return BJ_OK;
}

/* Next raw candidate document from whichever source is active: 1 with val/vlen set, 0 at end, <0 on error. */
static int dc_cursor_pull(dc_cursor *dcur, const uint8_t **val, size_t *vlen) {
    if (dcur->mode == DC_CURSOR_SCAN) {
        bpt_key k;
        return bpt_cursor_next(dcur->bpt_cur, &k, val, vlen);
    }
    if (dcur->cand_seen >= dcur->cand_count) return 0;
    size_t dstart = dcur->cand_walk.pos;
    int e = skip_value(&dcur->cand_walk);
    if (e) return e;
    *val = dcur->cand_walk.d + dstart;
    *vlen = dcur->cand_walk.pos - dstart;
    dcur->cand_seen++;
    return 1;
}

int dc_cursor_next_batch(dc_cursor *dcur, uint32_t max_count,
                         uint8_t **out, size_t *out_len, int *out_done) {
    *out = NULL; *out_len = 0;
    if (dcur->done) { *out_done = 1; return BJ_OK; }

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_array(b);

    uint32_t collected = 0;
    while (!e && collected < max_count) {
        const uint8_t *val; size_t vlen;
        int r = dc_cursor_pull(dcur, &val, &vlen);
        if (r < 0) { e = r; break; }
        if (r == 0) { dcur->done = 1; break; }

        int m = 0;
        e = qry_matches(val, vlen, dcur->scan_filter, dcur->scan_filter_len, &m);
        if (e) break;
        if (!m) continue;

        if (dcur->skip_remaining > 0) { dcur->skip_remaining--; continue; }

        if (dcur->projection_len) {
            uint8_t *pj = NULL; size_t pjlen = 0;
            e = qry_project_one(val, vlen, dcur->projection, dcur->projection_len, &pj, &pjlen);
            if (!e) e = bj_put_raw(b, pj, (uint32_t)pjlen);
            free(pj);
        } else {
            e = bj_put_raw(b, val, (uint32_t)vlen);
        }
        if (e) break;
        collected++;

        if (dcur->limit_remaining > 0 && --dcur->limit_remaining == 0) { dcur->done = 1; break; }
    }

    if (!e) e = bj_end_array(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, out, out_len);
    }
    bj_builder_free(b);

    if (dcur->done) dc_cursor_release_scan(dcur);
    *out_done = dcur->done;
    return e;
}

int dc_delete_one(dc_collection *c, const uint8_t *filter, uint32_t filter_len, int *deleted) {
    *deleted = 0;
    int found = 0; uint8_t *doc = NULL; size_t doc_len = 0;
    int e = dc_find_one(c, filter, filter_len, NULL, 0, &found, &doc, &doc_len);
    if (e) { free(doc); return e; }
    if (!found) { free(doc); return BJ_OK; }

    uint8_t id[12];
    e = dc_document_id(doc, (uint32_t)doc_len, id);
    if (e) { free(doc); return e; }
    uint64_t *undo;
    e = mut_begin(c, &undo);
    if (e) { free(doc); return e; }
    e = remove_from_indexes(c, doc, doc_len, id);
    free(doc);
    if (!e) {
        bpt_key key; oid_key(id, &key);
        e = bpt_delete(c->primary, &key);
    }
    if (!e) e = commit_journal(c);
    e = mut_end(c, undo, e);
    if (e) return e;
    *deleted = 1;
    return BJ_OK;
}

int dc_delete_many(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                   int64_t *deleted_count, uint8_t **ids, size_t *ids_len) {
    *deleted_count = 0;
    if (ids) { *ids = NULL; *ids_len = 0; }

    bj_builder *idb = NULL;
    if (ids) {
        idb = bj_builder_new();
        if (!idb) return BJ_ERR_OOM;
        bj_begin_array(idb);
    }

    qry_doc *matches; size_t match_count;
    int e = gather_matches(c, filter, filter_len, &matches, &match_count);
    if (e) { bj_builder_free(idb); return e; }

    for (size_t i = 0; !e && i < match_count; i++) {
        uint8_t id[12];
        e = dc_document_id(matches[i].ptr, (uint32_t)matches[i].len, id);
        if (e) break;
        uint64_t *undo;
        e = mut_begin(c, &undo);
        if (e) break;
        e = remove_from_indexes(c, matches[i].ptr, matches[i].len, id);
        if (!e) {
            bpt_key key; oid_key(id, &key);
            e = bpt_delete(c->primary, &key);
        }
        if (!e) e = commit_journal(c);
        e = mut_end(c, undo, e);
        if (!e) {
            (*deleted_count)++;
            /* After the commit, so no event names a document still present. */
            if (idb) bj_put_oid(idb, id);
        }
    }

    free_matches(matches, match_count);

    if (idb) {
        if (!e) {
            bj_end_array(idb);
            e = bj_builder_error(idb);
            if (!e) {
                size_t l = 0;
                const uint8_t *d = bj_builder_data(idb, &l);
                if (!d) e = BJ_ERR_STATE;
                else e = dbuf_dup(d, l, ids, ids_len);
            }
        }
        bj_builder_free(idb);
    }
    return e;
}

int dc_find_one_and_delete(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                           int *found, uint8_t **out, size_t *out_len) {
    *found = 0; *out = NULL; *out_len = 0;
    int f = 0; uint8_t *doc = NULL; size_t doc_len = 0;
    int e = dc_find_one(c, filter, filter_len, NULL, 0, &f, &doc, &doc_len);
    if (e) { free(doc); return e; }
    if (!f) { free(doc); return BJ_OK; }

    uint8_t id[12];
    e = dc_document_id(doc, (uint32_t)doc_len, id);
    if (e) { free(doc); return e; }
    uint8_t *idfilter = NULL; size_t idfilter_len = 0;
    e = build_id_filter(id, &idfilter, &idfilter_len);
    if (e) { free(doc); return e; }
    int deleted = 0;
    e = dc_delete_one(c, idfilter, (uint32_t)idfilter_len, &deleted);
    free(idfilter);
    if (e) { free(doc); return e; }

    *found = 1; *out = doc; *out_len = doc_len;
    return BJ_OK;
}

/* Encode `replacement` with its top-level _id forced to `id` (replacing any
 * _id field it carries, or adding one if it has none); every other field's
 * bytes are spliced verbatim (bj_put_raw) rather than decoded/re-encoded. */
static int splice_id(const uint8_t *replacement, size_t replacement_len,
                     const uint8_t id[12], uint8_t **out, size_t *out_len) {
    cur c = { replacement, replacement_len, 0 };
    uint32_t count;
    int e = object_begin(&c, &count);
    if (e) return e;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"_id", 3);
    if (!e) e = bj_put_oid(b, id);
    for (uint32_t i = 0; !e && i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&c, &kp, &klen);
        if (e) break;
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) break;
        if (klen == 3 && memcmp(kp, "_id", 3) == 0) continue;
        e = bj_put_key(b, kp, klen);
        if (!e) e = bj_put_raw(b, c.d + vstart, (uint32_t)(c.pos - vstart));
    }
    if (!e) e = bj_end_object(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, out, out_len);
    }
    bj_builder_free(b);
    return e;
}

int dc_replace_document(const uint8_t *replacement, uint32_t replacement_len,
                        const uint8_t default_id[12],
                        uint8_t **out, size_t *out_len) {
    uint8_t repl_id[12]; int repl_has_id = 0;
    int e = dc_document_id_opt(replacement, replacement_len, repl_id, &repl_has_id);
    if (e) return e;
    return splice_id(replacement, replacement_len,
                     repl_has_id ? repl_id : default_id, out, out_len);
}

int dc_replace_one(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                   const uint8_t *replacement, uint32_t replacement_len,
                   const uint8_t default_id[12], int upsert, int *result) {
    *result = 0;

    uint8_t repl_id[12]; int repl_has_id;
    int e = dc_document_id_opt(replacement, replacement_len, repl_id, &repl_has_id);
    if (e) return e;

    int found = 0; uint8_t *doc = NULL; size_t doc_len = 0;
    e = dc_find_one(c, filter, filter_len, NULL, 0, &found, &doc, &doc_len);
    if (e) { free(doc); return e; }

    if (!found) {
        free(doc);
        if (!upsert) return BJ_OK;
        uint8_t *spliced; size_t spliced_len;
        e = dc_replace_document(replacement, replacement_len, default_id, &spliced, &spliced_len);
        if (e) return e;
        e = dc_insert_one(c, spliced, (uint32_t)spliced_len);
        free(spliced);
        if (e) return e;
        *result = 2;
        return BJ_OK;
    }

    uint8_t existing_id[12];
    e = dc_document_id(doc, (uint32_t)doc_len, existing_id);
    if (e) { free(doc); return e; }

    if (repl_has_id && memcmp(repl_id, existing_id, 12) != 0) {
        free(doc);
        return DC_ERR_ID_MISMATCH;
    }

    uint8_t *spliced; size_t spliced_len;
    e = splice_id(replacement, replacement_len, existing_id, &spliced, &spliced_len);
    if (e) { free(doc); return e; }

    uint64_t *undo;
    e = mut_begin(c, &undo);
    if (e) { free(doc); free(spliced); return e; }

    e = remove_from_indexes(c, doc, doc_len, existing_id);
    free(doc);

    /* Checked here, after the old entries are cleared (so nothing self-
     * conflicts) but before the primary tree sees the new content -- a
     * rejection must never leave a forbidden duplicate value in primary.
     * mut_end's rewind restores the just-removed entries on rejection. */
    if (!e) e = check_unique_indexes(c, spliced, spliced_len);

    if (!e) {
        bpt_key key; oid_key(existing_id, &key);
        e = bpt_add(c->primary, &key, spliced, (uint32_t)spliced_len);
    }
    if (!e) e = add_to_indexes(c, spliced, (uint32_t)spliced_len, existing_id);
    if (!e) e = commit_journal(c);
    free(spliced);
    e = mut_end(c, undo, e);
    if (e) return e;
    *result = 1;
    return BJ_OK;
}

int dc_find_one_and_replace(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                            const uint8_t *replacement, uint32_t replacement_len,
                            const uint8_t default_id[12], int upsert, int return_new,
                            int *found, uint8_t **out, size_t *out_len) {
    *found = 0; *out = NULL; *out_len = 0;

    int before_found = 0; uint8_t *before = NULL; size_t before_len = 0;
    int e = dc_find_one(c, filter, filter_len, NULL, 0, &before_found, &before, &before_len);
    if (e) { free(before); return e; }
    if (!before_found && !upsert) { free(before); return BJ_OK; }

    int result = 0;
    uint8_t target_id[12];
    if (before_found) {
        e = dc_document_id(before, (uint32_t)before_len, target_id);
        uint8_t *idfilter = NULL; size_t idfilter_len = 0;
        if (!e) e = build_id_filter(target_id, &idfilter, &idfilter_len);
        if (!e) e = dc_replace_one(c, idfilter, (uint32_t)idfilter_len, replacement, replacement_len, default_id, 0, &result);
        free(idfilter);
    } else {
        e = dc_replace_one(c, filter, filter_len, replacement, replacement_len, default_id, 1, &result);
        memcpy(target_id, default_id, 12);
    }
    if (e) { free(before); return e; }

    if (!return_new) {
        if (before_found) { *found = 1; *out = before; *out_len = before_len; return BJ_OK; }
        free(before);
        return BJ_OK;
    }
    free(before);

    uint8_t *idfilter2 = NULL; size_t idfilter2_len = 0;
    e = build_id_filter(target_id, &idfilter2, &idfilter2_len);
    if (e) return e;
    e = dc_find_one(c, idfilter2, (uint32_t)idfilter2_len, NULL, 0, found, out, out_len);
    free(idfilter2);
    return e;
}

/*
 * A base document for an upsert: `filter`'s top-level bare-literal
 * conditions (fields not wrapped in an operator expression). Fields under
 * $and/$or/$nor, or given as an operator expression ({$gt: 5}), are
 * skipped -- matching how MongoDB seeds an upsert from a query's equality
 * conditions, conservatively scoped like the equality-index planner
 * (plan_equality_index) just above it in spirit.
 */
static int build_upsert_seed(const uint8_t *filter, size_t filter_len, uint8_t **out, size_t *out_len) {
    cur c = { filter, filter_len, 0 };
    uint32_t count;
    int e = object_begin(&c, &count);
    if (e) return e;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    e = bj_begin_object(b);
    for (uint32_t i = 0; !e && i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&c, &kp, &klen);
        if (e) break;
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) break;
        if (klen > 0 && kp[0] == '$') continue;
        int is_expr = 0;
        e = qry_is_operator_expr(c.d + vstart, c.pos - vstart, &is_expr);
        if (e) break;
        if (is_expr) continue;
        e = bj_put_key(b, kp, klen);
        if (!e) e = bj_put_raw(b, c.d + vstart, (uint32_t)(c.pos - vstart));
    }
    if (!e) e = bj_end_object(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, out, out_len);
    }
    bj_builder_free(b);
    return e;
}

/*
 * Note that the _id is always `default_id`, even when `filter` pins one:
 * splice_id overwrites whatever the seed carried. Real MongoDB would
 * insert the filter's _id. Preserved deliberately rather than fixed in
 * passing -- it is a semantics change with its own tests to write, and
 * folding it into a refactor whose whole point is that plan and apply
 * agree would be exactly the wrong place to change what they agree on.
 */
int dc_upsert_document(const uint8_t *filter, uint32_t filter_len,
                       const uint8_t *update, uint32_t update_len,
                       const uint8_t default_id[12],
                       uint8_t **out, size_t *out_len) {
    uint8_t *seed = NULL; size_t seed_len = 0;
    int e = build_upsert_seed(filter, filter_len, &seed, &seed_len);
    uint8_t *updated = NULL; size_t updated_len = 0;
    if (!e) e = upd_apply(seed, seed_len, update, update_len, 1, &updated, &updated_len);
    free(seed);
    if (e) { free(updated); return e; }
    e = splice_id(updated, updated_len, default_id, out, out_len);
    free(updated);
    return e;
}

int dc_update_one(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                  const uint8_t *update, uint32_t update_len,
                  const uint8_t default_id[12], int upsert, int *result) {
    *result = 0;

    int e = upd_validate(update, update_len);
    if (e) return e;

    int found = 0; uint8_t *doc = NULL; size_t doc_len = 0;
    e = dc_find_one(c, filter, filter_len, NULL, 0, &found, &doc, &doc_len);
    if (e) { free(doc); return e; }

    if (!found) {
        free(doc);
        if (!upsert) return BJ_OK;
        uint8_t *spliced = NULL; size_t spliced_len = 0;
        e = dc_upsert_document(filter, filter_len, update, update_len,
                               default_id, &spliced, &spliced_len);
        if (e) return e;
        e = dc_insert_one(c, spliced, (uint32_t)spliced_len);
        free(spliced);
        if (e) return e;
        *result = 2;
        return BJ_OK;
    }

    uint8_t id[12];
    e = dc_document_id(doc, (uint32_t)doc_len, id);
    if (e) { free(doc); return e; }

    uint8_t *updated = NULL; size_t updated_len = 0;
    e = upd_apply(doc, doc_len, update, update_len, 0, &updated, &updated_len);
    if (e) { free(doc); free(updated); return e; }

    uint64_t *undo;
    e = mut_begin(c, &undo);
    if (e) { free(doc); free(updated); return e; }

    e = remove_from_indexes(c, doc, doc_len, id);
    free(doc);

    /* Same placement rationale (and mut_end restore) as dc_replace_one's. */
    if (!e) e = check_unique_indexes(c, updated, updated_len);

    if (!e) {
        bpt_key key; oid_key(id, &key);
        e = bpt_add(c->primary, &key, updated, (uint32_t)updated_len);
    }
    if (!e) e = add_to_indexes(c, updated, (uint32_t)updated_len, id);
    if (!e) e = commit_journal(c);
    free(updated);
    e = mut_end(c, undo, e);
    if (e) return e;
    *result = 1;
    return BJ_OK;
}

int dc_find_one_and_update(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                           const uint8_t *update, uint32_t update_len,
                           const uint8_t default_id[12], int upsert, int return_new,
                           int *found, uint8_t **out, size_t *out_len) {
    *found = 0; *out = NULL; *out_len = 0;

    int e = upd_validate(update, update_len);
    if (e) return e;

    int before_found = 0; uint8_t *before = NULL; size_t before_len = 0;
    e = dc_find_one(c, filter, filter_len, NULL, 0, &before_found, &before, &before_len);
    if (e) { free(before); return e; }
    if (!before_found && !upsert) { free(before); return BJ_OK; }

    int result = 0;
    uint8_t target_id[12];
    if (before_found) {
        /* Re-target the exact matched document by _id so dc_update_one's
         * own internal re-scan can never land on a different document. */
        e = dc_document_id(before, (uint32_t)before_len, target_id);
        uint8_t *idfilter = NULL; size_t idfilter_len = 0;
        if (!e) e = build_id_filter(target_id, &idfilter, &idfilter_len);
        if (!e) e = dc_update_one(c, idfilter, (uint32_t)idfilter_len, update, update_len, default_id, 0, &result);
        free(idfilter);
    } else {
        e = dc_update_one(c, filter, filter_len, update, update_len, default_id, 1, &result);
        memcpy(target_id, default_id, 12); /* only consulted below when return_new */
    }
    if (e) { free(before); return e; }

    if (!return_new) {
        if (before_found) { *found = 1; *out = before; *out_len = before_len; return BJ_OK; }
        free(before);
        return BJ_OK; /* upsert + "before": no prior state, matches real MongoDB's null */
    }
    free(before);

    uint8_t *idfilter2 = NULL; size_t idfilter2_len = 0;
    e = build_id_filter(target_id, &idfilter2, &idfilter2_len);
    if (e) return e;
    e = dc_find_one(c, idfilter2, (uint32_t)idfilter2_len, NULL, 0, found, out, out_len);
    free(idfilter2);
    return e;
}

int dc_update_many(dc_collection *c, const uint8_t *filter, uint32_t filter_len,
                   const uint8_t *update, uint32_t update_len,
                   const uint8_t default_id[12], int upsert,
                   int64_t *matched_count, int *upserted,
                   uint8_t **images, size_t *images_len) {
    *matched_count = 0; *upserted = 0;
    if (images) { *images = NULL; *images_len = 0; }

    /* Post-images are collected only when asked for; the builder stays
     * NULL otherwise so a caller with no watchers pays nothing. */
    bj_builder *img = NULL;
    if (images) {
        img = bj_builder_new();
        if (!img) return BJ_ERR_OOM;
        bj_begin_array(img);
    }

    int e = upd_validate(update, update_len);
    if (e) { bj_builder_free(img); return e; }

    qry_doc *matches; size_t match_count;
    e = gather_matches(c, filter, filter_len, &matches, &match_count);
    if (e) { bj_builder_free(img); return e; }

    if (match_count == 0) {
        if (upsert) {
            uint8_t *spliced = NULL; size_t spliced_len = 0;
            e = dc_upsert_document(filter, filter_len, update, update_len,
                                   default_id, &spliced, &spliced_len);
            if (!e) e = dc_insert_one(c, spliced, (uint32_t)spliced_len);
            /* The inserted document is the upsert's post-image. */
            if (!e && img) bj_put_raw(img, spliced, (uint32_t)spliced_len);
            free(spliced);
            if (!e) *upserted = 1;
        }
    } else {
        *matched_count = (int64_t)match_count;
        for (size_t i = 0; !e && i < match_count; i++) {
            uint8_t id[12];
            e = dc_document_id(matches[i].ptr, (uint32_t)matches[i].len, id);
            if (e) break;
            uint8_t *updated = NULL; size_t updated_len = 0;
            e = upd_apply(matches[i].ptr, matches[i].len, update, update_len, 0, &updated, &updated_len);
            if (e) { free(updated); break; }
            uint64_t *undo;
            e = mut_begin(c, &undo);
            if (e) { free(updated); break; }
            e = remove_from_indexes(c, matches[i].ptr, matches[i].len, id);
            if (!e) e = check_unique_indexes(c, updated, updated_len);
            if (!e) {
                bpt_key key; oid_key(id, &key);
                e = bpt_add(c->primary, &key, updated, (uint32_t)updated_len);
            }
            if (!e) e = add_to_indexes(c, updated, (uint32_t)updated_len, id);
            if (!e) e = commit_journal(c);
            /* Recorded only after the write committed, so a caller never
             * sees an event for a document that did not land. */
            if (!e && img) bj_put_raw(img, updated, (uint32_t)updated_len);
            free(updated);
            e = mut_end(c, undo, e);
        }
    }

    free_matches(matches, match_count);

    if (img) {
        if (!e) {
            bj_end_array(img);
            e = bj_builder_error(img);
            if (!e) {
                size_t l = 0;
                const uint8_t *d = bj_builder_data(img, &l);
                if (!d) e = BJ_ERR_STATE;
                else e = dbuf_dup(d, l, images, images_len);
            }
        }
        bj_builder_free(img);
    }
    return e;
}

int dc_count(dc_collection *c, const uint8_t *filter, uint32_t filter_len, int64_t *out_count) {
    cur cu = { filter, filter_len, 0 };
    uint32_t fcount;
    int e = object_begin(&cu, &fcount);
    if (e) return e;
    if (fcount == 0) { *out_count = bpt_size(c->primary); return BJ_OK; }

    dc_source src;
    e = resolve_special_source(c, filter, filter_len, &src);
    if (e) { dc_source_free(&src); return e; }
    if (src.use_index) {
        int64_t n = 0;
        cur cc = { src.cand, src.cand_len, 0 };
        uint32_t ccount;
        e = array_begin(&cc, &ccount);
        for (uint32_t i = 0; !e && i < ccount; i++) {
            size_t dstart = cc.pos;
            e = skip_value(&cc);
            if (e) break;
            int m = 0;
            e = qry_matches(cc.d + dstart, cc.pos - dstart, src.residual, src.residual_len, &m);
            if (e) break;
            if (m) n++;
        }
        dc_source_free(&src);
        if (e) return e;
        *out_count = n;
        return BJ_OK;
    }
    dc_source_free(&src);

    dc_index *ix = NULL; uint8_t *ix_values = NULL; size_t ix_values_len = 0;
    int planned = 0;
    e = plan_equality_index(c, filter, filter_len, &ix, &ix_values, &ix_values_len, &planned);
    if (e) return e;

    int64_t n = 0;
    if (planned) {
        uint8_t *cand = NULL; size_t cand_len = 0;
        e = dc_collection_find_by_index(c, ix->name, (int)ix->name_len,
                                        ix_values, (uint32_t)ix_values_len, &cand, &cand_len);
        free(ix_values);
        if (e) { free(cand); return e; }
        cur cc = { cand, cand_len, 0 };
        uint32_t ccount;
        e = array_begin(&cc, &ccount);
        for (uint32_t i = 0; !e && i < ccount; i++) {
            size_t dstart = cc.pos;
            e = skip_value(&cc);
            if (e) break;
            int m = 0;
            e = qry_matches(cc.d + dstart, cc.pos - dstart, filter, filter_len, &m);
            if (e) break;
            if (m) n++;
        }
        free(cand);
        if (e) return e;
        *out_count = n;
        return BJ_OK;
    }
    free(ix_values);

    bpt_cursor *cur_h = bpt_cursor_open(c->primary, NULL, NULL);
    if (!cur_h) return BJ_ERR_OOM;
    int rc = BJ_OK;
    for (;;) {
        bpt_key k; const uint8_t *val; size_t vlen;
        int r = bpt_cursor_next(cur_h, &k, &val, &vlen);
        if (r < 0) { rc = r; break; }
        if (r == 0) break;
        int m = 0;
        rc = qry_matches(val, vlen, filter, filter_len, &m);
        if (rc) break;
        if (m) n++;
    }
    bpt_cursor_close(cur_h);
    if (rc) return rc;
    *out_count = n;
    return BJ_OK;
}

int dc_distinct(dc_collection *c, const char *field, int field_len,
                const uint8_t *filter, uint32_t filter_len,
                uint8_t **out, size_t *out_len) {
    *out = NULL; *out_len = 0;
    if (field_len <= 0) return BJ_ERR_STATE;

    qry_doc *matches; size_t match_count;
    int e = gather_matches(c, filter, filter_len, &matches, &match_count);
    if (e) return e;

    /* Unique values by exact encoded-byte equality (same rationale as every
     * other equality in this codebase); an array field's *elements* are
     * the candidates, not the array itself, matching real MongoDB's
     * distinct(). Result set is normally small, so a linear value_eq scan
     * per candidate is fine without a hash set. */
    val_list uniq; memset(&uniq, 0, sizeof(uniq));
    for (size_t i = 0; !e && i < match_count; i++) {
        const uint8_t *vp; size_t vl; int found = 0;
        e = qry_resolve_path(matches[i].ptr, matches[i].len,
                             (const uint8_t *)field, (uint32_t)field_len, &vp, &vl, &found);
        if (e) break;
        if (!found) continue;

        if (vl >= 1 && vp[0] == BJ_TYPE_ARRAY) {
            cur ac = { vp, vl, 0 };
            uint32_t acount;
            e = array_begin(&ac, &acount);
            for (uint32_t j = 0; !e && j < acount; j++) {
                size_t estart = ac.pos;
                e = skip_value(&ac);
                if (e) break;
                int dup = 0;
                for (uint32_t k = 0; k < uniq.count; k++) {
                    if (value_eq(uniq.items[k].ptr, uniq.items[k].len, ac.d + estart, ac.pos - estart)) { dup = 1; break; }
                }
                if (!dup) e = val_list_push(&uniq, ac.d + estart, ac.pos - estart);
            }
        } else {
            int dup = 0;
            for (uint32_t k = 0; k < uniq.count; k++) {
                if (value_eq(uniq.items[k].ptr, uniq.items[k].len, vp, vl)) { dup = 1; break; }
            }
            if (!dup) e = val_list_push(&uniq, vp, vl);
        }
    }
    if (e) { val_list_free(&uniq); free_matches(matches, match_count); return e; }

    bj_builder *b = bj_builder_new();
    if (!b) { val_list_free(&uniq); free_matches(matches, match_count); return BJ_ERR_OOM; }
    e = bj_begin_array(b);
    for (uint32_t k = 0; !e && k < uniq.count; k++) {
        e = bj_put_raw(b, uniq.items[k].ptr, (uint32_t)uniq.items[k].len);
    }
    if (!e) e = bj_end_array(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, out, out_len);
    }
    bj_builder_free(b);
    val_list_free(&uniq);
    free_matches(matches, match_count);
    return e;
}
