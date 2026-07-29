/*
 * main.c — the native C test harness for the document layer.
 *
 * Drives wasm/include/db.h's dc_* API directly, with no JavaScript, no
 * emscripten and no WASM anywhere in the process: a plain executable
 * linking the same C sources the browser build links, over an in-memory
 * bj_io (memfs.h). Built and run by wasm/build-native.sh.
 *
 * Why this exists. Until now every line of C in this repo was tested only
 * through the JS wrapper, which means (a) no ASan/UBSan, ever, (b) a full
 * emcc rebuild for a one-line C change, and (c) no way to test the C
 * layer's behavior independently of the JS layer's interpretation of it.
 * All three of those matter a great deal more now that logic is moving
 * *into* C -- see docs/db-plan.md's architecture rule.
 *
 * This is not a replacement for test/db.test.js, which remains the
 * specification of the public API. It is the layer below it.
 */
#include "db.h"
#include "bplustree.h"
#include "binjson.h"

#include "docs.h"
#include "memfs.h"
#include "tap.h"

#include <stdlib.h>
#include <string.h>

#define ORDER 32   /* matches DB_DEFAULT_ORDER in wasm/nisaba-wasm.js */

/* ---- fixtures --------------------------------------------------------- */

typedef struct {
    memfs         *fs;
    bpt           *primary;
    dc_collection *coll;
} fixture;

static int fx_open(fixture *fx, const char *file) {
    memset(fx, 0, sizeof(*fx));
    fx->fs = memfs_new();
    if (!fx->fs) return -1;
    bj_io io;
    if (memfs_open(fx->fs, file, &io) != BJ_OK) return -1;
    fx->primary = bpt_create(&io, ORDER);
    if (!fx->primary) return -1;
    fx->coll = dc_collection_open(fx->primary);
    return fx->coll ? 0 : -1;
}

static void fx_close(fixture *fx) {
    dc_collection_free(fx->coll);
    bpt_free(fx->primary);
    memfs_free(fx->fs);
    memset(fx, 0, sizeof(*fx));
}

/* Insert {_id: oid(n), name, team, age}. Returns the dc_insert_one rc. */
static int insert_person(dc_collection *c, uint32_t n,
                         const char *name, const char *team, int64_t age) {
    uint8_t id[12];
    mk_oid(id, n);
    doc *d = doc_new();
    doc_oid(d, "_id", id);
    doc_str(d, "name", name);
    doc_str(d, "team", team);
    doc_int(d, "age", age);
    uint32_t len;
    const uint8_t *buf = doc_done(d, &len);
    int rc = dc_insert_one(c, buf, len);
    doc_free(d);
    return rc;
}

/* A binjson ARRAY of field names, for the index APIs. Caller frees the
 * returned builder with bj_builder_free. */
static bj_builder *fields_of(const char *const *names, int n,
                             const uint8_t **out, uint32_t *out_len) {
    bj_builder *b = bj_builder_new();
    bj_begin_array(b);
    for (int i = 0; i < n; i++)
        bj_put_string(b, (const uint8_t *)names[i], (uint32_t)strlen(names[i]));
    bj_end_array(b);
    size_t len = 0;
    *out = bj_builder_data(b, &len);
    *out_len = (uint32_t)len;
    return b;
}

/* The empty filter {}. */
static bj_builder *empty_filter(const uint8_t **out, uint32_t *out_len) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_end_object(b);
    size_t len = 0;
    *out = bj_builder_data(b, &len);
    *out_len = (uint32_t)len;
    return b;
}

/* ---- tests ------------------------------------------------------------ */

TEST(insert_find_count) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);

    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));
    CHECK_OK(insert_person(fx.coll, 2, "Grace", "core", 45));
    CHECK_OK(insert_person(fx.coll, 3, "Alan", "research", 41));

    const uint8_t *f; uint32_t flen;
    bj_builder *fb = empty_filter(&f, &flen);
    int64_t count = 0;
    CHECK_OK(dc_count(fx.coll, f, flen, &count));
    CHECK_I64(count, 3);
    bj_builder_free(fb);

    /* find_one by an indexed-by-nothing equality condition */
    doc *q = doc_new();
    doc_str(q, "team", "research");
    uint32_t qlen;
    const uint8_t *qbuf = doc_done(q, &qlen);

    int found = 0; uint8_t *out = NULL; size_t out_len = 0;
    CHECK_OK(dc_find_one(fx.coll, qbuf, qlen, NULL, 0, &found, &out, &out_len));
    CHECK_I64(found, 1);
    if (found) {
        char name[64];
        CHECK(doc_get_str(out, out_len, "name", name, sizeof(name)));
        CHECK_STR(name, "Alan");
    }
    free(out);
    doc_free(q);
    fx_close(&fx);
}

TEST(duplicate_id_rejected) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));
    CHECK_RC(insert_person(fx.coll, 1, "Ada again", "core", 36), DC_ERR_DUPLICATE);
    fx_close(&fx);
}

TEST(equality_index_serves_the_planner) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));
    CHECK_OK(insert_person(fx.coll, 2, "Grace", "core", 45));
    CHECK_OK(insert_person(fx.coll, 3, "Alan", "research", 41));

    bj_io idx_io;
    CHECK_FATAL(memfs_open(fx.fs, "idx-people-team_1.bj", &idx_io) == BJ_OK);
    bpt *idx = bpt_create(&idx_io, ORDER);
    CHECK_FATAL(idx != NULL);

    const char *names[] = { "team" };
    const uint8_t *fields; uint32_t fields_len;
    bj_builder *fb = fields_of(names, 1, &fields, &fields_len);

    CHECK_OK(dc_collection_add_index(fx.coll, "team_1", 6, idx,
                                     fields, fields_len, 0, 0, NULL, 0));

    /* The planner should now choose the equality index (kind 2) and name it. */
    doc *q = doc_new();
    doc_str(q, "team", "core");
    uint32_t qlen;
    const uint8_t *qbuf = doc_done(q, &qlen);

    int kind = -1; uint8_t *iname = NULL; size_t iname_len = 0;
    CHECK_OK(dc_explain(fx.coll, qbuf, qlen, &kind, &iname, &iname_len));
    CHECK_I64(kind, 2);
    CHECK_I64(iname_len, 6);
    if (iname && iname_len == 6) CHECK(memcmp(iname, "team_1", 6) == 0);
    free(iname);

    /* ...and the query it serves must return both core members. */
    uint8_t *out = NULL; size_t out_len = 0;
    CHECK_OK(dc_find(fx.coll, qbuf, qlen, NULL, &out, &out_len));
    CHECK_I64(arr_count(out, out_len), 2);
    free(out);

    doc_free(q);
    bj_builder_free(fb);
    bpt_free(idx);
    fx_close(&fx);
}

TEST(unique_index_rejects_collision) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));

    bj_io idx_io;
    CHECK_FATAL(memfs_open(fx.fs, "idx-people-name_1.bj", &idx_io) == BJ_OK);
    bpt *idx = bpt_create(&idx_io, ORDER);
    CHECK_FATAL(idx != NULL);

    const char *names[] = { "name" };
    const uint8_t *fields; uint32_t fields_len;
    bj_builder *fb = fields_of(names, 1, &fields, &fields_len);
    CHECK_OK(dc_collection_add_index(fx.coll, "name_1", 6, idx,
                                     fields, fields_len, /*unique*/1, 0, NULL, 0));

    /* A second "Ada" must be refused by the unique index, not by _id. */
    CHECK_RC(insert_person(fx.coll, 2, "Ada", "research", 30), DC_ERR_DUPLICATE_KEY);
    /* ...and the refused write must not have landed. */
    const uint8_t *f; uint32_t flen;
    bj_builder *efb = empty_filter(&f, &flen);
    int64_t count = 0;
    CHECK_OK(dc_count(fx.coll, f, flen, &count));
    CHECK_I64(count, 1);
    bj_builder_free(efb);

    bj_builder_free(fb);
    bpt_free(idx);
    fx_close(&fx);
}

TEST(update_and_delete) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));
    CHECK_OK(insert_person(fx.coll, 2, "Grace", "core", 45));
    CHECK_OK(insert_person(fx.coll, 3, "Alan", "research", 41));

    /* $set team on every core member */
    doc *q = doc_new();
    doc_str(q, "team", "core");
    uint32_t qlen;
    const uint8_t *qbuf = doc_done(q, &qlen);

    doc *u = doc_new();
    doc_begin_obj(u, "$set");
    doc_key(u, "team");
    bj_put_string(u->b, (const uint8_t *)"platform", 8);
    doc_end_obj(u);
    uint32_t ulen;
    const uint8_t *ubuf = doc_done(u, &ulen);

    uint8_t default_id[12];
    mk_oid(default_id, 99);
    int64_t matched = 0; int upserted = 0;
    CHECK_OK(dc_update_many(fx.coll, qbuf, qlen, ubuf, ulen,
                            default_id, 0, &matched, &upserted));
    CHECK_I64(matched, 2);
    CHECK_I64(upserted, 0);

    /* nobody is on "core" any more */
    int64_t count = 0;
    CHECK_OK(dc_count(fx.coll, qbuf, qlen, &count));
    CHECK_I64(count, 0);

    /* delete the one researcher */
    doc *dq = doc_new();
    doc_str(dq, "team", "research");
    uint32_t dqlen;
    const uint8_t *dqbuf = doc_done(dq, &dqlen);
    int deleted = 0;
    CHECK_OK(dc_delete_one(fx.coll, dqbuf, dqlen, &deleted));
    CHECK_I64(deleted, 1);

    const uint8_t *f; uint32_t flen;
    bj_builder *efb = empty_filter(&f, &flen);
    CHECK_OK(dc_count(fx.coll, f, flen, &count));
    CHECK_I64(count, 2);
    bj_builder_free(efb);

    doc_free(dq); doc_free(u); doc_free(q);
    fx_close(&fx);
}

TEST(upsert_seeds_from_filter) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);

    doc *q = doc_new();
    doc_str(q, "team", "brand-new");
    uint32_t qlen;
    const uint8_t *qbuf = doc_done(q, &qlen);

    doc *u = doc_new();
    doc_begin_obj(u, "$set");
    doc_key(u, "name");
    bj_put_string(u->b, (const uint8_t *)"Nobody", 6);
    doc_end_obj(u);
    uint32_t ulen;
    const uint8_t *ubuf = doc_done(u, &ulen);

    uint8_t default_id[12];
    mk_oid(default_id, 7);
    int result = -1;
    CHECK_OK(dc_update_one(fx.coll, qbuf, qlen, ubuf, ulen, default_id, 1, &result));
    CHECK_I64(result, 2);   /* 2 == upserted */

    /* The upserted document must carry BOTH the filter's equality seed and
     * the update's $set -- that is build_upsert_seed's contract. */
    int found = 0; uint8_t *out = NULL; size_t out_len = 0;
    CHECK_OK(dc_find_one(fx.coll, qbuf, qlen, NULL, 0, &found, &out, &out_len));
    CHECK_I64(found, 1);
    if (found) {
        char name[64], team[64];
        CHECK(doc_get_str(out, out_len, "name", name, sizeof(name)));
        CHECK_STR(name, "Nobody");
        CHECK(doc_get_str(out, out_len, "team", team, sizeof(team)));
        CHECK_STR(team, "brand-new");
    }
    free(out);
    doc_free(u); doc_free(q);
    fx_close(&fx);
}

TEST(cursor_streams_in_batches) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);
    for (uint32_t i = 1; i <= 25; i++)
        CHECK_OK(insert_person(fx.coll, i, "person", "core", (int64_t)i));

    const uint8_t *f; uint32_t flen;
    bj_builder *fb = empty_filter(&f, &flen);

    dc_cursor *cur = NULL;
    CHECK_FATAL(dc_cursor_open(fx.coll, f, flen, NULL, 0, 0, 0, &cur) == BJ_OK);
    CHECK_FATAL(cur != NULL);

    long total = 0;
    int done = 0, rounds = 0;
    while (!done && rounds < 20) {
        uint8_t *out = NULL; size_t out_len = 0;
        CHECK_OK(dc_cursor_next_batch(cur, 10, &out, &out_len, &done));
        long n = arr_count(out, out_len);
        CHECK(n >= 0);
        if (n > 0) total += n;
        free(out);
        rounds++;
    }
    CHECK_I64(total, 25);
    CHECK_I64(done, 1);
    /* 25 documents at 10 per batch must not have arrived in one gulp. */
    CHECK(rounds >= 3);

    dc_cursor_close(cur);
    bj_builder_free(fb);
    fx_close(&fx);
}

TEST(data_survives_reopen) {
    memfs *fs = memfs_new();
    CHECK_FATAL(fs != NULL);

    bj_io io;
    CHECK_FATAL(memfs_open(fs, "coll-people.bj", &io) == BJ_OK);

    {   /* first session: write */
        bpt *t = bpt_create(&io, ORDER);
        CHECK_FATAL(t != NULL);
        dc_collection *c = dc_collection_open(t);
        CHECK_FATAL(c != NULL);
        CHECK_OK(insert_person(c, 1, "Ada", "core", 36));
        CHECK_OK(insert_person(c, 2, "Grace", "core", 45));
        dc_collection_free(c);
        bpt_free(t);
    }

    {   /* second session: same bytes, fresh objects */
        bpt *t = bpt_open(&io);
        CHECK_FATAL(t != NULL);
        dc_collection *c = dc_collection_open(t);
        CHECK_FATAL(c != NULL);

        const uint8_t *f; uint32_t flen;
        bj_builder *fb = empty_filter(&f, &flen);
        int64_t count = 0;
        CHECK_OK(dc_count(c, f, flen, &count));
        CHECK_I64(count, 2);
        bj_builder_free(fb);

        dc_collection_free(c);
        bpt_free(t);
    }

    CHECK(memfs_size(fs, "coll-people.bj") > 0);
    memfs_free(fs);
}

TEST(cross_file_journal_stays_bounded) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);

    /* The journal is a *cross-file* commit journal: commit_journal in db.c
     * is a documented no-op when the collection has no secondary indexes,
     * because a single-file bpt write is already atomic on its own. So the
     * journal only has a job once there is a second file to keep in step. */
    bj_io idx_io;
    CHECK_FATAL(memfs_open(fx.fs, "idx-people-team_1.bj", &idx_io) == BJ_OK);
    bpt *idx = bpt_create(&idx_io, ORDER);
    CHECK_FATAL(idx != NULL);

    const char *names[] = { "team" };
    const uint8_t *fields; uint32_t fields_len;
    bj_builder *fb = fields_of(names, 1, &fields, &fields_len);
    CHECK_OK(dc_collection_add_index(fx.coll, "team_1", 6, idx,
                                     fields, fields_len, 0, 0, NULL, 0));

    bj_io journal;
    CHECK_FATAL(memfs_open(fx.fs, "jrnl-people.bj", &journal) == BJ_OK);
    /* "Empty/absent journal: BJ_OK, adopt state as-is" (db.h). */
    CHECK_OK(dc_collection_recover(fx.coll, &journal));
    CHECK_I64(memfs_size(fx.fs, "jrnl-people.bj"), 0);

    for (uint32_t i = 1; i <= 40; i++)
        CHECK_OK(insert_person(fx.coll, i, "person", "core", (int64_t)i));

    /* Two ping-ponged fixed-size slots, so 40 writes must not grow it
     * without bound -- that boundedness is the whole design. */
    size_t jsize = memfs_size(fx.fs, "jrnl-people.bj");
    CHECK(jsize > 0);
    CHECK(jsize < 512);

    const uint8_t *f; uint32_t flen;
    bj_builder *efb = empty_filter(&f, &flen);
    int64_t count = 0;
    CHECK_OK(dc_count(fx.coll, f, flen, &count));
    CHECK_I64(count, 40);
    bj_builder_free(efb);

    bj_builder_free(fb);
    bpt_free(idx);
    fx_close(&fx);
}

TEST(applied_index_advances_and_never_regresses) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);

    CHECK_I64(dc_applied_index(fx.coll), 0);
    CHECK_OK(dc_set_applied_index(fx.coll, 5));
    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));
    CHECK_I64(dc_applied_index(fx.coll), 5);

    CHECK_OK(dc_set_applied_index(fx.coll, 6));
    CHECK_OK(insert_person(fx.coll, 2, "Grace", "core", 45));
    CHECK_I64(dc_applied_index(fx.coll), 6);

    /* Going backwards is refused, and refusal leaves the value intact. */
    CHECK_RC(dc_set_applied_index(fx.coll, 4), BJ_ERR_STATE);
    CHECK_I64(dc_applied_index(fx.coll), 6);

    fx_close(&fx);
}

TEST(distinct_reports_unique_values) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));
    CHECK_OK(insert_person(fx.coll, 2, "Grace", "core", 45));
    CHECK_OK(insert_person(fx.coll, 3, "Alan", "research", 41));

    const uint8_t *f; uint32_t flen;
    bj_builder *fb = empty_filter(&f, &flen);

    uint8_t *out = NULL; size_t out_len = 0;
    CHECK_OK(dc_distinct(fx.coll, "team", 4, f, flen, &out, &out_len));
    CHECK_I64(arr_count(out, out_len), 2);   /* core, research */
    free(out);

    bj_builder_free(fb);
    fx_close(&fx);
}

int main(void) {
    RUN(insert_find_count);
    RUN(duplicate_id_rejected);
    RUN(equality_index_serves_the_planner);
    RUN(unique_index_rejects_collision);
    RUN(update_and_delete);
    RUN(upsert_seeds_from_filter);
    RUN(cursor_streams_in_batches);
    RUN(data_survives_reopen);
    RUN(cross_file_journal_stays_bounded);
    RUN(applied_index_advances_and_never_regresses);
    RUN(distinct_reports_unique_values);
    return tap_summary();
}
