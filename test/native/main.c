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
#include "keyenc.h"
#include "db_names.h"
#include "db_validate.h"
#include "db_ttl.h"
#include "db_bulk.h"
#include "db_agg.h"
#include "db_catalog.h"
#include "db_update.h"
#include "dbuf.h"
#include "bjio_posix.h"

#include "docs.h"
#include "memfs.h"
#include "tap.h"

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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

/* ---- keyenc ------------------------------------------------------------
 *
 * These vectors were generated by running the ORIGINAL pure-JavaScript
 * orderedKey/compositeKey/compositeUpperBound (structures-core.js before
 * they became marshalling over keyenc.c) over each input and printing the
 * bytes. They are the proof that removing the second encoder changed
 * nothing on disk: every index file ever written by the JS encoder is
 * still readable, and every key the C encoder writes is one the JS
 * encoder would have written.
 *
 * If a change to keyenc.c breaks one of these, it is a format break --
 * see docs/format-compatibility.md before touching the expectations.
 */
static void check_key_hex(const char *what, const dbuf *got, const char *want_hex) {
    size_t want_len = strlen(want_hex) / 2;
    if (got->len != want_len) {
        TAP_FAIL("%s: encoded %zu bytes, want %zu", what, got->len, want_len);
        return;
    }
    char hex[256];
    if (got->len * 2 + 1 > sizeof(hex)) { TAP_FAIL("%s: too long to compare", what); return; }
    for (size_t i = 0; i < got->len; i++)
        snprintf(hex + i * 2, 3, "%02x", got->data[i]);
    hex[got->len * 2] = '\0';
    if (strcmp(hex, want_hex) != 0)
        TAP_FAIL("%s: got %s, want %s", what, hex, want_hex);
}

TEST(keyenc_matches_the_original_js_encoder) {
    struct { double v; const char *hex; } nums[] = {
        { 0.0,                "008000000000000000" },
        { -0.0,               "008000000000000000" },   /* -0 normalizes to +0 */
        { 1.0,                "00bff0000000000000" },
        { -1.0,               "00400fffffffffffff" },
        { 3.5,                "00c00c000000000000" },
        { -3.5,               "003ff3ffffffffffff" },
        { 1e300,              "00fe37e43c8800759c" },
        { -1e300,             "0001c81bc377ff8a63" },
        { 9007199254740991.0, "00c33fffffffffffff" },
        { 5e-324,             "008000000000000001" },
    };
    for (size_t i = 0; i < sizeof(nums) / sizeof(nums[0]); i++) {
        dbuf b = {0};
        CHECK_OK(qk_put_number(&b, nums[i].v));
        check_key_hex("number", &b, nums[i].hex);
        dbuf_free(&b);
    }

    struct { const char *s; const char *hex; } strs[] = {
        { "",      "0100" },
        { "a",     "016100" },
        { "core",  "01636f726500" },
        { "h\xc3\xa9llo",         "0168c3a96c6c6f00" },   /* héllo  */
        { "\xf0\x9f\x98\x80",     "01f09f988000" },       /* U+1F600 */
    };
    for (size_t i = 0; i < sizeof(strs) / sizeof(strs[0]); i++) {
        dbuf b = {0};
        CHECK_OK(qk_put_string(&b, (const uint8_t *)strs[i].s, (uint32_t)strlen(strs[i].s)));
        check_key_hex("string", &b, strs[i].hex);
        dbuf_free(&b);
    }

    /* compositeKey('core', 36) */
    {
        dbuf b = {0};
        CHECK_OK(qk_put_string(&b, (const uint8_t *)"core", 4));
        CHECK_OK(qk_put_number(&b, 36.0));
        check_key_hex("composite", &b, "01636f72650000c042000000000000");
        dbuf_free(&b);
    }
    /* compositeUpperBound('core') */
    {
        dbuf b = {0};
        CHECK_OK(qk_put_string(&b, (const uint8_t *)"core", 4));
        CHECK_OK(qk_put_upper_bound(&b));
        check_key_hex("upper bound", &b, "01636f726500ff");
        dbuf_free(&b);
    }
}

TEST(keyenc_byte_order_matches_numeric_order) {
    /* The whole point of the encoding: memcmp over the bytes must
     * reproduce the values' natural order, across the sign boundary and
     * out to the extremes. */
    static const double ascending[] = {
        -1e300, -3.5, -1.0, -0.0, 0.0, 5e-324, 1.0, 3.5, 1e300
    };
    size_t n = sizeof(ascending) / sizeof(ascending[0]);
    dbuf prev = {0};
    for (size_t i = 0; i < n; i++) {
        dbuf cur = {0};
        CHECK_OK(qk_put_number(&cur, ascending[i]));
        if (i > 0) {
            size_t min = prev.len < cur.len ? prev.len : cur.len;
            int cmp = memcmp(prev.data, cur.data, min);
            /* -0 and 0 encode identically, so the only legal relation
             * between neighbours is <= , and == only for that pair. */
            if (cmp > 0) TAP_FAIL("index %zu sorts before its predecessor", i);
        }
        dbuf_free(&prev);
        prev = cur;
    }
    dbuf_free(&prev);

    /* Strings sort after numbers, because 0x01 > 0x00. */
    dbuf num = {0}, str = {0};
    CHECK_OK(qk_put_number(&num, 1e300));
    CHECK_OK(qk_put_string(&str, (const uint8_t *)"", 0));
    CHECK(num.data[0] < str.data[0]);
    dbuf_free(&num); dbuf_free(&str);
}

TEST(keyenc_rejects_unorderable_values) {
    dbuf b = {0};
    /* NaN has no ordering. */
    CHECK_RC(qk_put_number(&b, 0.0 / 0.0), BJ_ERR_STATE);
    /* U+0000 is reserved as the string terminator. */
    CHECK_RC(qk_put_string(&b, (const uint8_t *)"a\0b", 3), BJ_ERR_STATE);
    dbuf_free(&b);
}

/* ---- db_names ---------------------------------------------------------- */

static void check_name(const char *what, int rc, const dbuf *got, const char *want) {
    if (rc != 0) { TAP_FAIL("%s: rc %d", what, rc); return; }
    if (got->len != strlen(want) || memcmp(got->data, want, got->len) != 0)
        TAP_FAIL("%s: got \"%.*s\", want \"%s\"", what, (int)got->len, (char *)got->data, want);
}

TEST(file_names_match_the_original_js_scheme) {
    struct { uint32_t gen; const char *want; } coll[] = {
        { 0, "coll-users.bj" },
        { 1, "g1-coll-users.bj" },
        { 42, "g42-coll-users.bj" },
        { 1000000, "g1000000-coll-users.bj" },
    };
    for (size_t i = 0; i < sizeof(coll) / sizeof(coll[0]); i++) {
        dbuf b = {0};
        int rc = dc_collection_file_name(&b, "users", 5, coll[i].gen);
        check_name("collection", rc, &b, coll[i].want);
        dbuf_free(&b);
    }
    {
        dbuf b = {0};
        check_name("index", dc_index_file_name(&b, "users", 5, "team_1", 6, 0),
                   &b, "idx-users-team_1.bj");
        dbuf_free(&b);
    }
    {
        dbuf b = {0};
        check_name("index gen", dc_index_file_name(&b, "users", 5, "team_1", 6, 3),
                   &b, "g3-idx-users-team_1.bj");
        dbuf_free(&b);
    }
    {
        dbuf b = {0};
        check_name("journal", dc_journal_file_name(&b, "users", 5, 0),
                   &b, "coll-users-journal.bj");
        dbuf_free(&b);
    }
    {
        dbuf b = {0};
        check_name("journal gen", dc_journal_file_name(&b, "users", 5, 2),
                   &b, "g2-coll-users-journal.bj");
        dbuf_free(&b);
    }
    struct { dc_text_role role; const char *want; } text[] = {
        { DC_TEXT_ROLE_TERMS,     "idx-posts-body_text-terms.bj" },
        { DC_TEXT_ROLE_DOCUMENTS, "idx-posts-body_text-documents.bj" },
        { DC_TEXT_ROLE_LENGTHS,   "idx-posts-body_text-lengths.bj" },
    };
    for (size_t i = 0; i < sizeof(text) / sizeof(text[0]); i++) {
        dbuf b = {0};
        int rc = dc_text_index_file_name(&b, "posts", 5, "body_text", 9, 0, text[i].role);
        check_name("text index", rc, &b, text[i].want);
        dbuf_free(&b);
    }
    /* A collection name may legally contain dots -- the whole reason the
     * generation marker is a prefix rather than a `.g<N>` suffix. */
    {
        dbuf b = {0};
        check_name("dotted name", dc_collection_file_name(&b, "users.g2", 8, 0),
                   &b, "coll-users.g2.bj");
        dbuf_free(&b);
    }
}

TEST(orphan_sweep_pattern_matches_exactly_what_js_matched) {
    /* Matches: what this layer creates, at any generation. */
    static const char *yes[] = {
        "coll-users.bj", "idx-users-team_1.bj", "coll-users-journal.bj",
        "g1-coll-users.bj", "g42-idx-users-team_1.bj", "g7-coll-users-journal.bj",
        "idx-posts-body_text-terms.bj", "coll-users.g2.bj",
        "coll-.bj",                     /* the JS `.*` accepted an empty middle */
        "idx-.bj",
    };
    for (size_t i = 0; i < sizeof(yes) / sizeof(yes[0]); i++)
        if (!dc_is_db_file(yes[i], strlen(yes[i]))) TAP_FAIL("should match: %s", yes[i]);

    /* Non-matches: the catalog, host files, and near misses. The catalog
     * one matters most -- the sweep deleting it would destroy the
     * database. */
    static const char *no[] = {
        "__catalog__.bj",
        "__wal__.bj",
        "notes.txt",
        "coll-users.txt",               /* wrong extension     */
        "collusers.bj",                 /* no separator        */
        "xcoll-users.bj",               /* prefix isn't at the start */
        "g-coll-users.bj",              /* g with no digits    */
        "g12coll-users.bj",             /* digits with no dash */
        "g1-notes.bj",                  /* generation on a non-db name */
        "",
        "coll-",                        /* no extension        */
        ".bj",
    };
    for (size_t i = 0; i < sizeof(no) / sizeof(no[0]); i++)
        if (dc_is_db_file(no[i], strlen(no[i]))) TAP_FAIL("should NOT match: %s", no[i]);
}

/* ---- db_validate ------------------------------------------------------- */

/* memmem is a BSD/GNU extension, absent under -std=c11 on glibc without
 * _GNU_SOURCE -- and this harness is meant to build with any compiler. */
static const uint8_t *find_bytes(const uint8_t *hay, size_t hay_len,
                                 const char *needle, size_t n) {
    if (n > hay_len) return NULL;
    for (size_t i = 0; i + n <= hay_len; i++)
        if (memcmp(hay + i, needle, n) == 0) return hay + i;
    return NULL;
}

TEST(strerror_covers_every_code_the_layer_can_raise) {
    /* Every code the JS ERR table used to carry, plus the validation codes
     * that replaced its hand-written throws. A code with no text would
     * surface to a user as "unknown error", which is worse than useless. */
    static const int codes[] = {
        BJ_ERR_OOM, BJ_ERR_STATE, BJ_ERR_EOF, BJ_ERR_UNKNOWN_TYPE,
        BJ_ERR_INT_RANGE, BJ_ERR_POINTER_RANGE, BJ_ERR_DEPTH, BJ_ERR_VERIFY,
        BJ_ERR_RANGE,
        DC_ERR_DUPLICATE, DC_ERR_ID_MISMATCH, DC_ERR_DUPLICATE_KEY,
        DC_ERR_MISSING_INDEXED_FIELD, DC_ERR_UNINDEXABLE_VALUE,
        DC_ERR_INVALID_COLLECTION_NAME, DC_ERR_INVALID_DB_NAME,
        DC_ERR_RESERVED_NAME, DC_ERR_EMPTY_KEY_SPEC, DC_ERR_NON_ASCENDING_KEY,
        DC_ERR_BULK_EMPTY, DC_ERR_BULK_UNKNOWN_OP, DC_ERR_BULK_MISSING_FIELD,
    };
    for (size_t i = 0; i < sizeof(codes) / sizeof(codes[0]); i++) {
        const char *s = dc_strerror(codes[i]);
        if (!s || !*s) { TAP_FAIL("code %d has no text", codes[i]); continue; }
        if (strcmp(s, "unknown error") == 0)
            TAP_FAIL("code %d falls through to the default", codes[i]);
    }
    /* Callers match on these prefixes; db.test.js and db.client-wasm
     * .test.js assert them by regex. */
    CHECK(strstr(dc_strerror(DC_ERR_INVALID_COLLECTION_NAME), "Invalid collection name") != NULL);
    CHECK(strstr(dc_strerror(DC_ERR_RESERVED_NAME), "Invalid collection name") != NULL);
    CHECK(strstr(dc_strerror(DC_ERR_INVALID_DB_NAME), "Invalid database name") != NULL);
    CHECK_STR(dc_strerror(DC_ERR_EMPTY_KEY_SPEC), "createIndex requires at least one field");
    /* An unmapped code still returns something printable. */
    CHECK(dc_strerror(-9999) != NULL);
}

TEST(name_validation_matches_the_js_rules) {
    CHECK_OK(dc_check_collection_name("users", 5));
    CHECK_OK(dc_check_collection_name("users.g2", 8));
    CHECK_OK(dc_check_collection_name("a", 1));
    CHECK_RC(dc_check_collection_name("", 0), DC_ERR_INVALID_COLLECTION_NAME);
    CHECK_RC(dc_check_collection_name("a/b", 3), DC_ERR_INVALID_COLLECTION_NAME);
    CHECK_RC(dc_check_collection_name("a\0b", 3), DC_ERR_INVALID_COLLECTION_NAME);
    /* The format stamp shares the catalog keyspace with collections. */
    CHECK_RC(dc_check_collection_name(DC_FORMAT_KEY, strlen(DC_FORMAT_KEY)),
             DC_ERR_RESERVED_NAME);

    CHECK_OK(dc_check_db_name("main", 4));
    CHECK_RC(dc_check_db_name("", 0), DC_ERR_INVALID_DB_NAME);
    CHECK_RC(dc_check_db_name("a/b", 3), DC_ERR_INVALID_DB_NAME);
    /* A database name is a directory, not a catalog key, so the reserved
     * key is a perfectly good database name. */
    CHECK_OK(dc_check_db_name(DC_FORMAT_KEY, strlen(DC_FORMAT_KEY)));
}

TEST(index_key_spec_validates_and_emits_its_fields) {
    /* {team: 1} -> ["team"] */
    {
        doc *d = doc_new();
        doc_int(d, "team", 1);
        uint32_t len; const uint8_t *spec = doc_done(d, &len);
        dbuf fields = {0};
        CHECK_OK(dc_check_index_key_spec(spec, len, &fields));
        CHECK_I64(arr_count(fields.data, fields.len), 1);
        dbuf_free(&fields);
        doc_free(d);
    }
    /* Compound, and field order is spec order -- composite keys depend on it. */
    {
        doc *d = doc_new();
        doc_int(d, "team", 1);
        doc_int(d, "age", 1);
        uint32_t len; const uint8_t *spec = doc_done(d, &len);
        dbuf fields = {0};
        CHECK_OK(dc_check_index_key_spec(spec, len, &fields));
        CHECK_I64(arr_count(fields.data, fields.len), 2);
        /* "team" must appear before "age" in the emitted array. */
        const uint8_t *t = find_bytes(fields.data, fields.len, "team", 4);
        const uint8_t *a = find_bytes(fields.data, fields.len, "age", 3);
        CHECK(t != NULL && a != NULL && t < a);
        dbuf_free(&fields);
        doc_free(d);
    }
    /* Empty spec. */
    {
        doc *d = doc_new();
        uint32_t len; const uint8_t *spec = doc_done(d, &len);
        dbuf fields = {0};
        CHECK_RC(dc_check_index_key_spec(spec, len, &fields), DC_ERR_EMPTY_KEY_SPEC);
        dbuf_free(&fields);
        doc_free(d);
    }
    /* Descending, and a non-numeric direction. */
    {
        doc *d = doc_new();
        doc_int(d, "team", -1);
        uint32_t len; const uint8_t *spec = doc_done(d, &len);
        dbuf fields = {0};
        CHECK_RC(dc_check_index_key_spec(spec, len, &fields), DC_ERR_NON_ASCENDING_KEY);
        dbuf_free(&fields);
        doc_free(d);
    }
    {
        doc *d = doc_new();
        doc_str(d, "team", "text");
        uint32_t len; const uint8_t *spec = doc_done(d, &len);
        dbuf fields = {0};
        CHECK_RC(dc_check_index_key_spec(spec, len, &fields), DC_ERR_NON_ASCENDING_KEY);
        dbuf_free(&fields);
        doc_free(d);
    }
    /* A rejected spec must leave nothing behind: the caller may reuse the
     * buffer, and a half-built array would be silently wrong. */
    {
        doc *d = doc_new();
        doc_int(d, "ok", 1);
        doc_int(d, "bad", -1);
        uint32_t len; const uint8_t *spec = doc_done(d, &len);
        dbuf fields = {0};
        CHECK_RC(dc_check_index_key_spec(spec, len, &fields), DC_ERR_NON_ASCENDING_KEY);
        CHECK_I64(fields.len, 0);
        dbuf_free(&fields);
        doc_free(d);
    }
}

/* ---- db_ttl ------------------------------------------------------------ */

TEST(ttl_cutoff_and_filter) {
    int64_t cutoff = 0;

    /* 1 hour TTL at epoch 10_000_000 ms -> 10_000_000 - 3_600_000. */
    CHECK_OK(dc_ttl_cutoff_ms(10000000, 3600, &cutoff));
    CHECK_I64(cutoff, 6400000);

    /* Zero means "expire immediately": the cutoff is now. */
    CHECK_OK(dc_ttl_cutoff_ms(1700000000000LL, 0, &cutoff));
    CHECK_I64(cutoff, 1700000000000LL);

    /* Fractional seconds are legal (MongoDB permits them). */
    CHECK_OK(dc_ttl_cutoff_ms(1000, 0.5, &cutoff));
    CHECK_I64(cutoff, 500);

    /* NaN and infinities would silently produce a cutoff matching
     * everything or nothing -- refuse rather than delete the collection. */
    CHECK_RC(dc_ttl_cutoff_ms(0, 0.0 / 0.0, &cutoff), BJ_ERR_RANGE);
    CHECK_RC(dc_ttl_cutoff_ms(0, 1.0 / 0.0, &cutoff), BJ_ERR_RANGE);
    CHECK_RC(dc_ttl_cutoff_ms(0, -1.0 / 0.0, &cutoff), BJ_ERR_RANGE);
    /* A TTL past the representable range is refused, not cast (which
     * would be undefined behavior). */
    CHECK_RC(dc_ttl_cutoff_ms(0, 1e18, &cutoff), BJ_ERR_RANGE);

    /* The filter is {field: {$lt: Date(cutoff)}} -- decodable, one
     * top-level field, and the $lt value really is a DATE. */
    dbuf f = {0};
    CHECK_OK(dc_ttl_filter(&f, "createdAt", 9, 6400000));
    CHECK(f.len > 0);
    CHECK_I64(f.data[0], BJ_TYPE_OBJECT);
    CHECK(find_bytes(f.data, f.len, "createdAt", 9) != NULL);
    CHECK(find_bytes(f.data, f.len, "$lt", 3) != NULL);
    /* The DATE type byte must appear -- a number here would compare
     * against Date-encoded index keys and silently match nothing. */
    {
        int saw_date = 0;
        for (size_t i = 0; i < f.len; i++) if (f.data[i] == BJ_TYPE_DATE) saw_date = 1;
        CHECK(saw_date);
    }
    dbuf_free(&f);

    dbuf empty = {0};
    CHECK_RC(dc_ttl_filter(&empty, "", 0, 0), BJ_ERR_RANGE);
    dbuf_free(&empty);
}

/* ---- db_bulk ----------------------------------------------------------- */

/* One bulkWrite operation: {<name>: {<field>: {...}}}. Nested objects
 * only, which is all the grammar cares about. */
static void put_op(bj_builder *b, const char *name, const char *field, const char *field2) {
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)name, (uint32_t)strlen(name));
    bj_begin_object(b);
    if (field) {
        bj_put_key(b, (const uint8_t *)field, (uint32_t)strlen(field));
        bj_begin_object(b);
        bj_end_object(b);
    }
    if (field2) {
        bj_put_key(b, (const uint8_t *)field2, (uint32_t)strlen(field2));
        bj_begin_object(b);
        bj_end_object(b);
    }
    bj_end_object(b);
    bj_end_object(b);
}

TEST(bulk_grammar_accepts_every_operation_and_orders_the_codes) {
    bj_builder *b = bj_builder_new();
    bj_begin_array(b);
    put_op(b, "insertOne",  "document", NULL);
    put_op(b, "updateOne",  "filter",   "update");
    put_op(b, "updateMany", "filter",   "update");
    put_op(b, "replaceOne", "filter",   "replacement");
    put_op(b, "deleteOne",  "filter",   NULL);
    put_op(b, "deleteMany", "filter",   NULL);
    bj_end_array(b);
    size_t len = 0;
    const uint8_t *ops = bj_builder_data(b, &len);

    dbuf out = {0};
    int bad = -2;
    CHECK_OK(dc_bulk_parse(ops, len, &out, &bad));
    CHECK_I64(bad, -1);
    CHECK_I64(arr_count(out.data, out.len), 6);
    dbuf_free(&out);
    bj_builder_free(b);
}

TEST(bulk_grammar_rejects_malformed_lists_and_names_the_index) {
    /* Empty list. */
    {
        bj_builder *b = bj_builder_new();
        bj_begin_array(b); bj_end_array(b);
        size_t len = 0; const uint8_t *ops = bj_builder_data(b, &len);
        dbuf out = {0}; int bad = -2;
        CHECK_RC(dc_bulk_parse(ops, len, &out, &bad), DC_ERR_BULK_EMPTY);
        dbuf_free(&out); bj_builder_free(b);
    }
    /* Unknown operation name, at index 1. */
    {
        bj_builder *b = bj_builder_new();
        bj_begin_array(b);
        put_op(b, "insertOne", "document", NULL);
        put_op(b, "upsertOne", "document", NULL);
        bj_end_array(b);
        size_t len = 0; const uint8_t *ops = bj_builder_data(b, &len);
        dbuf out = {0}; int bad = -2;
        CHECK_RC(dc_bulk_parse(ops, len, &out, &bad), DC_ERR_BULK_UNKNOWN_OP);
        CHECK_I64(bad, 1);
        /* Nothing emitted: an unordered bulkWrite must be able to attempt
         * every operation, so a malformed one has to surface before any
         * of them run. */
        CHECK_I64(out.len, 0);
        dbuf_free(&out); bj_builder_free(b);
    }
    /* Missing required field, at index 2. */
    {
        bj_builder *b = bj_builder_new();
        bj_begin_array(b);
        put_op(b, "deleteOne", "filter", NULL);
        put_op(b, "insertOne", "document", NULL);
        put_op(b, "updateOne", "filter", NULL);   /* no `update` */
        bj_end_array(b);
        size_t len = 0; const uint8_t *ops = bj_builder_data(b, &len);
        dbuf out = {0}; int bad = -2;
        CHECK_RC(dc_bulk_parse(ops, len, &out, &bad), DC_ERR_BULK_MISSING_FIELD);
        CHECK_I64(bad, 2);
        dbuf_free(&out); bj_builder_free(b);
    }
    /* An operation object with two keys is ambiguous, not a merge. */
    {
        bj_builder *b = bj_builder_new();
        bj_begin_array(b);
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"insertOne", 9);
        bj_begin_object(b); bj_end_object(b);
        bj_put_key(b, (const uint8_t *)"deleteOne", 9);
        bj_begin_object(b); bj_end_object(b);
        bj_end_object(b);
        bj_end_array(b);
        size_t len = 0; const uint8_t *ops = bj_builder_data(b, &len);
        dbuf out = {0}; int bad = -2;
        CHECK_RC(dc_bulk_parse(ops, len, &out, &bad), DC_ERR_BULK_UNKNOWN_OP);
        CHECK_I64(bad, 0);
        dbuf_free(&out); bj_builder_free(b);
    }
}

/* ---- db_agg ------------------------------------------------------------ */

/* Run a pipeline built by `build` and return the result array. */
static int run_pipeline(dc_collection *c, bj_builder *stages,
                        uint8_t **out, size_t *out_len, int *bad) {
    size_t len = 0;
    const uint8_t *p = bj_builder_data(stages, &len);
    return dc_aggregate(c, p, len, bad, out, out_len);
}

TEST(aggregate_group_uses_encoded_bytes_for_identity) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-sales.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada",   "core",     36));
    CHECK_OK(insert_person(fx.coll, 2, "Grace", "core",     45));
    CHECK_OK(insert_person(fx.coll, 3, "Alan",  "research", 41));

    /* [{$group: {_id: "$team", n: {$sum: 1}}}] */
    bj_builder *b = bj_builder_new();
    bj_begin_array(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"$group", 6);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"_id", 3);
    bj_put_string(b, (const uint8_t *)"$team", 5);
    bj_put_key(b, (const uint8_t *)"n", 1);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"$sum", 4);
    bj_put_int(b, 1);
    bj_end_object(b);
    bj_end_object(b);
    bj_end_object(b);
    bj_end_array(b);

    uint8_t *out = NULL; size_t out_len = 0; int bad = -2;
    CHECK_OK(run_pipeline(fx.coll, b, &out, &out_len, &bad));
    CHECK_I64(bad, -1);
    CHECK_I64(arr_count(out, out_len), 2);   /* core, research */
    free(out);
    bj_builder_free(b);
    fx_close(&fx);
}

TEST(aggregate_reports_the_stage_that_failed) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-sales.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));

    /* [{$match: {}}, {$unwind: "$x"}] -- the failure is stage 1, not 0. */
    bj_builder *b = bj_builder_new();
    bj_begin_array(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"$match", 6);
    bj_begin_object(b); bj_end_object(b);
    bj_end_object(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"$unwind", 7);
    bj_put_string(b, (const uint8_t *)"$x", 2);
    bj_end_object(b);
    bj_end_array(b);

    uint8_t *out = NULL; size_t out_len = 0; int bad = -2;
    CHECK_RC(run_pipeline(fx.coll, b, &out, &out_len, &bad), DC_ERR_AGG_UNKNOWN_STAGE);
    CHECK_I64(bad, 1);
    free(out);
    bj_builder_free(b);

    /* An unknown $group accumulator is its own code. */
    bj_builder *g = bj_builder_new();
    bj_begin_array(g);
    bj_begin_object(g);
    bj_put_key(g, (const uint8_t *)"$group", 6);
    bj_begin_object(g);
    bj_put_key(g, (const uint8_t *)"_id", 3);
    bj_put_null(g);
    bj_put_key(g, (const uint8_t *)"n", 1);
    bj_begin_object(g);
    bj_put_key(g, (const uint8_t *)"$median", 7);
    bj_put_int(g, 1);
    bj_end_object(g);
    bj_end_object(g);
    bj_end_object(g);
    bj_end_array(g);
    out = NULL; out_len = 0; bad = -2;
    CHECK_RC(run_pipeline(fx.coll, g, &out, &out_len, &bad), DC_ERR_AGG_BAD_ACCUMULATOR);
    CHECK_I64(bad, 0);
    free(out);
    bj_builder_free(g);
    fx_close(&fx);
}

TEST(aggregate_later_match_has_the_full_engine_grammar) {
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-sales.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada",   "core",     36));
    CHECK_OK(insert_person(fx.coll, 2, "Alan",  "research", 41));

    /* [{$group: {_id: "$team"}}, {$match: {_id: {$regex: "^re"}}}]
     * $regex did not exist in the JS pipeline's nine-operator subset. */
    bj_builder *b = bj_builder_new();
    bj_begin_array(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"$group", 6);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"_id", 3);
    bj_put_string(b, (const uint8_t *)"$team", 5);
    bj_end_object(b);
    bj_end_object(b);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"$match", 6);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"_id", 3);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"$regex", 6);
    bj_put_string(b, (const uint8_t *)"^re", 3);
    bj_end_object(b);
    bj_end_object(b);
    bj_end_object(b);
    bj_end_array(b);

    uint8_t *out = NULL; size_t out_len = 0; int bad = -2;
    CHECK_OK(run_pipeline(fx.coll, b, &out, &out_len, &bad));
    CHECK_I64(arr_count(out, out_len), 1);   /* just "research" */
    free(out);
    bj_builder_free(b);
    fx_close(&fx);
}

/* ---- $currentDate ------------------------------------------------------ */

TEST(current_date_rewrites_into_set) {
    /* {$currentDate: {seen: true}, $inc: {n: 1}} */
    doc *u = doc_new();
    doc_begin_obj(u, "$currentDate");
    doc_key(u, "seen");
    bj_put_bool(u->b, 1);
    doc_end_obj(u);
    doc_begin_obj(u, "$inc");
    doc_int(u, "n", 1);
    doc_end_obj(u);
    uint32_t len; const uint8_t *up = doc_done(u, &len);

    dbuf out = {0};
    CHECK_OK(upd_resolve_current_date(up, len, 1700000000000LL, &out));
    /* $currentDate is gone, $set carries a DATE, $inc survives. */
    CHECK(find_bytes(out.data, out.len, "$currentDate", 12) == NULL);
    CHECK(find_bytes(out.data, out.len, "$set", 4) != NULL);
    CHECK(find_bytes(out.data, out.len, "$inc", 4) != NULL);
    {
        int saw_date = 0;
        for (size_t i = 0; i < out.len; i++) if (out.data[i] == BJ_TYPE_DATE) saw_date = 1;
        CHECK(saw_date);
    }
    dbuf_free(&out);
    doc_free(u);
}

TEST(current_date_is_idempotent_and_passes_others_through) {
    /* No $currentDate: byte-identical passthrough, so a caller can run
     * every update through this unconditionally. */
    doc *u = doc_new();
    doc_begin_obj(u, "$set");
    doc_int(u, "n", 1);
    doc_end_obj(u);
    uint32_t len; const uint8_t *up = doc_done(u, &len);

    dbuf out = {0};
    CHECK_OK(upd_resolve_current_date(up, len, 123, &out));
    CHECK_I64(out.len, len);
    CHECK(memcmp(out.data, up, len) == 0);

    /* Running it again on the resolved form changes nothing -- which is
     * what lets the WAL resolve at proposal time and apply-time run it
     * again without a second clock reading moving the value. */
    dbuf twice = {0};
    CHECK_OK(upd_resolve_current_date(out.data, out.len, 999, &twice));
    CHECK_I64(twice.len, out.len);
    CHECK(memcmp(twice.data, out.data, out.len) == 0);
    dbuf_free(&twice);
    dbuf_free(&out);
    doc_free(u);
}

TEST(current_date_refuses_bad_specs_and_collisions) {
    /* A field spec that is neither true nor {$type: "date"}. */
    {
        doc *u = doc_new();
        doc_begin_obj(u, "$currentDate");
        doc_str(u, "seen", "yes");
        doc_end_obj(u);
        uint32_t len; const uint8_t *up = doc_done(u, &len);
        dbuf out = {0};
        CHECK_RC(upd_resolve_current_date(up, len, 0, &out), DC_ERR_BAD_CURRENT_DATE);
        CHECK_I64(out.len, 0);
        dbuf_free(&out);
        doc_free(u);
    }
    /* {$type: "timestamp"} is a real MongoDB option this engine does not
     * implement -- refused, not silently treated as a date. */
    {
        doc *u = doc_new();
        doc_begin_obj(u, "$currentDate");
        doc_begin_obj(u, "seen");
        doc_str(u, "$type", "timestamp");
        doc_end_obj(u);
        doc_end_obj(u);
        uint32_t len; const uint8_t *up = doc_done(u, &len);
        dbuf out = {0};
        CHECK_RC(upd_resolve_current_date(up, len, 0, &out), DC_ERR_BAD_CURRENT_DATE);
        dbuf_free(&out);
        doc_free(u);
    }
    /* The same field targeted by $currentDate and $inc. */
    {
        doc *u = doc_new();
        doc_begin_obj(u, "$currentDate");
        doc_key(u, "seen");
        bj_put_bool(u->b, 1);
        doc_end_obj(u);
        doc_begin_obj(u, "$inc");
        doc_int(u, "seen", 1);
        doc_end_obj(u);
        uint32_t len; const uint8_t *up = doc_done(u, &len);
        dbuf out = {0};
        CHECK_RC(upd_resolve_current_date(up, len, 0, &out), DC_ERR_CURRENT_DATE_CONFLICT);
        dbuf_free(&out);
        doc_free(u);
    }
    /* ...and by an existing $set, which merges rather than collides
     * unless it names the same field. */
    {
        doc *u = doc_new();
        doc_begin_obj(u, "$currentDate");
        doc_key(u, "seen");
        bj_put_bool(u->b, 1);
        doc_end_obj(u);
        doc_begin_obj(u, "$set");
        doc_int(u, "seen", 1);
        doc_end_obj(u);
        uint32_t len; const uint8_t *up = doc_done(u, &len);
        dbuf out = {0};
        CHECK_RC(upd_resolve_current_date(up, len, 0, &out), DC_ERR_CURRENT_DATE_CONFLICT);
        dbuf_free(&out);
        doc_free(u);
    }
    {
        doc *u = doc_new();
        doc_begin_obj(u, "$currentDate");
        doc_key(u, "seen");
        bj_put_bool(u->b, 1);
        doc_end_obj(u);
        doc_begin_obj(u, "$set");
        doc_int(u, "other", 1);
        doc_end_obj(u);
        uint32_t len; const uint8_t *up = doc_done(u, &len);
        dbuf out = {0};
        CHECK_OK(upd_resolve_current_date(up, len, 42, &out));
        CHECK(find_bytes(out.data, out.len, "other", 5) != NULL);
        CHECK(find_bytes(out.data, out.len, "seen", 4) != NULL);
        dbuf_free(&out);
        doc_free(u);
    }
}

/* ---- the POSIX adapter (bjio_posix.c) ---------------------------------- */

TEST(posix_namespace_backs_a_real_database) {
    /* The whole point of Phase 2: the same dc_* layer, over real files,
     * through bj_ns instead of a JS bridge. If this passes, the server
     * target is an adapter swap rather than a port. */
    char tmpl[] = "/tmp/nisaba-native-XXXXXX";
    CHECK_FATAL(mkdtemp(tmpl) != NULL);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);

    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);

    /* An adapter that forgot sync would be silently non-durable, which is
     * the defect this phase exists to close -- so check the wiring, not
     * just that writes land. */
    bj_io io;
    CHECK_FATAL(ns.open(ns.ctx, "coll-people.bj", 14, BJ_NS_CREATE, &io) == BJ_OK);
    CHECK(io.sync != NULL);
    CHECK(io.close != NULL);
    CHECK_OK(bjio_check(&io));

    {
        bpt *t = bpt_create(&io, ORDER);
        CHECK_FATAL(t != NULL);
        dc_collection *c = dc_collection_open(t);
        CHECK_FATAL(c != NULL);
        CHECK_OK(insert_person(c, 1, "Ada", "core", 36));
        CHECK_OK(insert_person(c, 2, "Grace", "core", 45));

        const uint8_t *f; uint32_t flen;
        bj_builder *fb = empty_filter(&f, &flen);
        int64_t count = 0;
        CHECK_OK(dc_count(c, f, flen, &count));
        CHECK_I64(count, 2);
        bj_builder_free(fb);

        /* A real fsync must succeed on a real file. */
        CHECK_OK(io.sync(io.ctx));
        dc_collection_free(c);
        bpt_free(t);
    }
    CHECK_OK(ns.close(ns.ctx, &io));

    /* Reopen through the namespace: the bytes are on disk, not in a
     * buffer that died with the handle. */
    bj_io again;
    CHECK_FATAL(ns.open(ns.ctx, "coll-people.bj", 14, 0, &again) == BJ_OK);
    CHECK(again.size(again.ctx) > 0);
    {
        bpt *t = bpt_open(&again);
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
    CHECK_OK(ns.close(ns.ctx, &again));

    /* remove, and removing the already-gone (a sweep racing a sweep). */
    CHECK_OK(ns.remove(ns.ctx, "coll-people.bj", 14));
    CHECK_OK(ns.remove(ns.ctx, "coll-people.bj", 14));
    CHECK_RC(ns.open(ns.ctx, "coll-people.bj", 14, 0, &io), BJ_ERR_STATE);

    /* A name that would escape the scope is refused at the adapter, even
     * though db_validate.h already refuses it upstream. */
    CHECK_RC(ns.open(ns.ctx, "../escape", 9, BJ_NS_CREATE, &io), BJ_ERR_RANGE);
    CHECK_RC(ns.remove(ns.ctx, "a/b", 3), BJ_ERR_RANGE);
    CHECK_RC(ns.open(ns.ctx, "", 0, BJ_NS_CREATE, &io), BJ_ERR_RANGE);

    /* BJ_NS_TRUNC is what replaces delete-then-create, so it must
     * actually zero an existing file. */
    CHECK_FATAL(ns.open(ns.ctx, "scratch.bj", 10, BJ_NS_CREATE, &io) == BJ_OK);
    CHECK_OK(io.write(io.ctx, 0, (const uint8_t *)"hello", 5));
    CHECK_I64(io.size(io.ctx), 5);
    CHECK_OK(ns.close(ns.ctx, &io));
    CHECK_FATAL(ns.open(ns.ctx, "scratch.bj", 10, BJ_NS_CREATE | BJ_NS_TRUNC, &io) == BJ_OK);
    CHECK_I64(io.size(io.ctx), 0);
    CHECK_OK(ns.close(ns.ctx, &io));

    /* BJ_NS_EXCL refuses an existing name. */
    CHECK_RC(ns.open(ns.ctx, "scratch.bj", 10, BJ_NS_CREATE | BJ_NS_EXCL, &io), BJ_ERR_STATE);

    CHECK_OK(ns.sync(ns.ctx));   /* directory-entry durability */

    ns.remove(ns.ctx, "scratch.bj", 10);
    bjns_posix_free(&ns);
    close(dirfd);
    rmdir(tmpl);
}

TEST(memory_io_is_accepted_without_a_sync_callback) {
    /*
     * A writable io with no sync is legitimate for memory and a durability
     * bug for a file. Which one a binary contains is a property of the
     * BUILD, not of an individual call, so bjio_check is compiled in or
     * out (BJIO_REQUIRE_SYNC) rather than decided per open.
     *
     * This binary does not enable it -- its harness runs entirely on
     * memfs -- so a sync-less memory io must open fine. The WASI build
     * does enable it, and would refuse the very same io. Asserting the
     * permissive half here is what stops a later phase from turning the
     * flag on for this target and silently breaking every memory-backed
     * test with BJ_ERR_STATE from bjfile_init.
     */
    memfs *fs = memfs_new();
    CHECK_FATAL(fs != NULL);
    bj_io io;
    CHECK_FATAL(memfs_open(fs, "x.bj", &io) == BJ_OK);
    CHECK(io.write != NULL);
    CHECK(io.sync == NULL);        /* memfs deliberately has none */
    CHECK_OK(bjio_check(&io));     /* ...and that is accepted here */

    bpt *t = bpt_create(&io, ORDER);
    CHECK(t != NULL);              /* bjfile_init did not refuse it */
    bpt_free(t);
    memfs_free(fs);
}

/* ---- db_catalog -------------------------------------------------------- */

/* Add an equality index definition to an `indexes` array under construction. */
static void put_eq_index(bj_builder *b, const char *name, const char *file,
                         const char *const *fields, int nfields,
                         int unique, int with_kind) {
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"name", 4);
    bj_put_string(b, (const uint8_t *)name, (uint32_t)strlen(name));
    if (with_kind) {
        bj_put_key(b, (const uint8_t *)"kind", 4);
        bj_put_string(b, (const uint8_t *)"equality", 8);
    }
    bj_put_key(b, (const uint8_t *)"file", 4);
    bj_put_string(b, (const uint8_t *)file, (uint32_t)strlen(file));
    bj_put_key(b, (const uint8_t *)"fields", 6);
    bj_begin_array(b);
    for (int i = 0; i < nfields; i++)
        bj_put_string(b, (const uint8_t *)fields[i], (uint32_t)strlen(fields[i]));
    bj_end_array(b);
    if (unique) {
        bj_put_key(b, (const uint8_t *)"unique", 6);
        bj_put_bool(b, 1);
    }
    bj_end_object(b);
}

TEST(catalog_plan_names_every_file_in_attach_order) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"file", 4);
    bj_put_string(b, (const uint8_t *)"coll-users.bj", 13);
    bj_put_key(b, (const uint8_t *)"journal", 7);
    bj_put_string(b, (const uint8_t *)"coll-users-journal.bj", 21);
    bj_put_key(b, (const uint8_t *)"indexes", 7);
    bj_begin_array(b);
    {
        const char *fields[] = { "team", "age" };
        put_eq_index(b, "team_1_age_1", "idx-users-team_1_age_1.bj", fields, 2, 1, 1);
    }
    /* A text index carries three files, and the plan must order them the
     * way dc_collection_attach_text_index takes its trees -- a host that
     * had to know that ordering would be reimplementing the schema. */
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"name", 4);
    bj_put_string(b, (const uint8_t *)"body_text", 9);
    bj_put_key(b, (const uint8_t *)"kind", 4);
    bj_put_string(b, (const uint8_t *)"text", 4);
    bj_put_key(b, (const uint8_t *)"field", 5);
    bj_put_string(b, (const uint8_t *)"body", 4);
    bj_put_key(b, (const uint8_t *)"files", 5);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"docLengths", 10);   /* deliberately out of order */
    bj_put_string(b, (const uint8_t *)"L.bj", 4);
    bj_put_key(b, (const uint8_t *)"index", 5);
    bj_put_string(b, (const uint8_t *)"T.bj", 4);
    bj_put_key(b, (const uint8_t *)"docTerms", 8);
    bj_put_string(b, (const uint8_t *)"D.bj", 4);
    bj_end_object(b);
    bj_end_object(b);
    bj_end_array(b);
    bj_end_object(b);

    size_t len = 0;
    const uint8_t *entry = bj_builder_data(b, &len);
    dbuf plan = {0};
    CHECK_OK(dc_catalog_open_plan(entry, len, "users", 5, &plan));

    CHECK(find_bytes(plan.data, plan.len, "coll-users.bj", 13) != NULL);
    CHECK(find_bytes(plan.data, plan.len, "coll-users-journal.bj", 21) != NULL);
    CHECK(find_bytes(plan.data, plan.len, "idx-users-team_1_age_1.bj", 25) != NULL);

    /* The three text files must appear in attach order regardless of the
     * order the catalog happened to store them in. */
    const uint8_t *t = find_bytes(plan.data, plan.len, "T.bj", 4);
    const uint8_t *d = find_bytes(plan.data, plan.len, "D.bj", 4);
    const uint8_t *l = find_bytes(plan.data, plan.len, "L.bj", 4);
    CHECK(t != NULL && d != NULL && l != NULL);
    if (t && d && l) { CHECK(t < d); CHECK(d < l); }

    dbuf_free(&plan);
    bj_builder_free(b);
}

TEST(catalog_plan_keeps_old_databases_openable) {
    /*
     * Two backward-compatibility rules that databases in the wild depend
     * on. Both were JS conditionals; if either is lost, an existing
     * database stops opening -- which no other test would catch, because
     * every test fixture is written by the current code.
     */
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"file", 4);
    bj_put_string(b, (const uint8_t *)"coll-users.bj", 13);
    /* No `journal` field: written before each generation got its own. */
    bj_put_key(b, (const uint8_t *)"indexes", 7);
    bj_begin_array(b);
    {
        /* No `kind` field: written before milestone 6, means equality. */
        const char *fields[] = { "team" };
        put_eq_index(b, "team_1", "idx-users-team_1.bj", fields, 1, 0, /*with_kind*/0);
    }
    bj_end_array(b);
    bj_end_object(b);

    size_t len = 0;
    const uint8_t *entry = bj_builder_data(b, &len);
    dbuf plan = {0};
    CHECK_OK(dc_catalog_open_plan(entry, len, "users", 5, &plan));

    /* The generation-0 journal name is derived, not stored. */
    CHECK(find_bytes(plan.data, plan.len, "coll-users-journal.bj", 21) != NULL);
    /* ...and the kind-less index planned as equality, so it carries
     * `fields` rather than a single `field`. */
    CHECK(find_bytes(plan.data, plan.len, "fields", 6) != NULL);
    CHECK(find_bytes(plan.data, plan.len, "team", 4) != NULL);

    dbuf_free(&plan);
    bj_builder_free(b);
}

TEST(catalog_plan_refuses_an_entry_it_cannot_honor) {
    dbuf plan = {0};
    /* Not an object. */
    {
        bj_builder *b = bj_builder_new();
        bj_put_int(b, 1);
        size_t len = 0; const uint8_t *e = bj_builder_data(b, &len);
        CHECK_RC(dc_catalog_open_plan(e, len, "u", 1, &plan), DC_ERR_CATALOG_ENTRY);
        bj_builder_free(b);
    }
    /* No primary file -- the one thing an entry cannot be without. */
    {
        bj_builder *b = bj_builder_new();
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"indexes", 7);
        bj_begin_array(b); bj_end_array(b);
        bj_end_object(b);
        size_t len = 0; const uint8_t *e = bj_builder_data(b, &len);
        CHECK_RC(dc_catalog_open_plan(e, len, "u", 1, &plan), DC_ERR_CATALOG_ENTRY);
        bj_builder_free(b);
    }
    /* An index definition with no file. Refusing beats planning to open a
     * file called "undefined". */
    {
        bj_builder *b = bj_builder_new();
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"file", 4);
        bj_put_string(b, (const uint8_t *)"coll-u.bj", 9);
        bj_put_key(b, (const uint8_t *)"indexes", 7);
        bj_begin_array(b);
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"name", 4);
        bj_put_string(b, (const uint8_t *)"x_1", 3);
        bj_end_object(b);
        bj_end_array(b);
        bj_end_object(b);
        size_t len = 0; const uint8_t *e = bj_builder_data(b, &len);
        CHECK_RC(dc_catalog_open_plan(e, len, "u", 1, &plan), DC_ERR_CATALOG_ENTRY);
        bj_builder_free(b);
    }
    dbuf_free(&plan);
}

TEST(catalog_list_indexes_inverts_what_create_index_stored) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"file", 4);
    bj_put_string(b, (const uint8_t *)"coll-users.bj", 13);
    bj_put_key(b, (const uint8_t *)"indexes", 7);
    bj_begin_array(b);
    {
        const char *fields[] = { "team", "age" };
        put_eq_index(b, "team_1_age_1", "i.bj", fields, 2, /*unique*/1, 1);
    }
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"name", 4);
    bj_put_string(b, (const uint8_t *)"loc_2dsphere", 12);
    bj_put_key(b, (const uint8_t *)"kind", 4);
    bj_put_string(b, (const uint8_t *)"geo", 3);
    bj_put_key(b, (const uint8_t *)"field", 5);
    bj_put_string(b, (const uint8_t *)"loc", 3);
    bj_put_key(b, (const uint8_t *)"file", 4);
    bj_put_string(b, (const uint8_t *)"g.bj", 4);
    bj_end_object(b);
    bj_end_array(b);
    bj_end_object(b);

    size_t len = 0;
    const uint8_t *entry = bj_builder_data(b, &len);
    dbuf out = {0};
    CHECK_OK(dc_catalog_list_indexes(entry, len, &out));
    CHECK_I64(arr_count(out.data, out.len), 2);

    /* Stored `fields` come back as key entries; a geo `field` comes back
     * as the '2dsphere' marker the caller originally passed in. */
    CHECK(find_bytes(out.data, out.len, "team", 4) != NULL);
    CHECK(find_bytes(out.data, out.len, "age", 3) != NULL);
    CHECK(find_bytes(out.data, out.len, "2dsphere", 8) != NULL);
    CHECK(find_bytes(out.data, out.len, "unique", 6) != NULL);
    /* An option that was not set must not be reported at all. */
    CHECK(find_bytes(out.data, out.len, "sparse", 6) == NULL);
    /* Backing file names are an implementation detail, not part of the
     * driver-shaped answer. */
    CHECK(find_bytes(out.data, out.len, "i.bj", 4) == NULL);

    dbuf_free(&out);
    bj_builder_free(b);
}

/* A minimal catalog entry with no indexes. */
static bj_builder *empty_entry(const uint8_t **out, size_t *out_len) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"file", 4);
    bj_put_string(b, (const uint8_t *)"coll-users.bj", 13);
    bj_put_key(b, (const uint8_t *)"journal", 7);
    bj_put_string(b, (const uint8_t *)"coll-users-journal.bj", 21);
    bj_end_object(b);
    *out = bj_builder_data(b, out_len);
    return b;
}

/* A plan-shaped equality definition, as createIndex now hands over. */
static bj_builder *eq_def(const char *name, const char *file, const char *field,
                          int unique, const uint8_t **out, size_t *out_len) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"name", 4);
    bj_put_string(b, (const uint8_t *)name, (uint32_t)strlen(name));
    bj_put_key(b, (const uint8_t *)"kind", 4);
    bj_put_int(b, DC_INDEX_EQUALITY);
    bj_put_key(b, (const uint8_t *)"files", 5);
    bj_begin_array(b);
    bj_put_string(b, (const uint8_t *)file, (uint32_t)strlen(file));
    bj_end_array(b);
    bj_put_key(b, (const uint8_t *)"fields", 6);
    bj_begin_array(b);
    bj_put_string(b, (const uint8_t *)field, (uint32_t)strlen(field));
    bj_end_array(b);
    bj_put_key(b, (const uint8_t *)"unique", 6);
    bj_put_bool(b, unique);
    bj_put_key(b, (const uint8_t *)"sparse", 6);
    bj_put_bool(b, 0);
    bj_end_object(b);
    *out = bj_builder_data(b, out_len);
    return b;
}

TEST(catalog_write_and_read_sides_agree) {
    /*
     * The point of putting the schema in one file: what put_index writes,
     * open_plan must read back identically. When these were separate JS
     * functions nothing checked that, and a field added to one and missed
     * in the other is a silently half-persisted index.
     */
    const uint8_t *entry; size_t entry_len;
    bj_builder *eb = empty_entry(&entry, &entry_len);

    const uint8_t *def; size_t def_len;
    bj_builder *db_ = eq_def("team_1", "idx-users-team_1.bj", "team", 1, &def, &def_len);

    dbuf updated = {0};
    CHECK_OK(dc_catalog_put_index(entry, entry_len, def, def_len, &updated));

    /* Stored form: a STRING kind and a single `file`, not the plan's int
     * and array. */
    CHECK(find_bytes(updated.data, updated.len, "equality", 8) != NULL);
    CHECK(find_bytes(updated.data, updated.len, "idx-users-team_1.bj", 19) != NULL);
    /* Fields that were false must not be stored at all. */
    CHECK(find_bytes(updated.data, updated.len, "sparse", 6) == NULL);
    CHECK(find_bytes(updated.data, updated.len, "unique", 6) != NULL);
    /* The rest of the entry survived the rewrite. */
    CHECK(find_bytes(updated.data, updated.len, "coll-users-journal.bj", 21) != NULL);

    /* ...and it plans back to what went in. */
    dbuf plan = {0};
    CHECK_OK(dc_catalog_open_plan(updated.data, updated.len, "users", 5, &plan));
    CHECK(find_bytes(plan.data, plan.len, "team_1", 6) != NULL);
    CHECK(find_bytes(plan.data, plan.len, "idx-users-team_1.bj", 19) != NULL);
    CHECK(find_bytes(plan.data, plan.len, "fields", 6) != NULL);

    dbuf_free(&plan);
    dbuf_free(&updated);
    bj_builder_free(db_);
    bj_builder_free(eb);
}

TEST(catalog_put_index_replaces_rather_than_duplicates) {
    const uint8_t *entry; size_t entry_len;
    bj_builder *eb = empty_entry(&entry, &entry_len);

    const uint8_t *d1; size_t d1_len;
    bj_builder *b1 = eq_def("team_1", "old.bj", "team", 0, &d1, &d1_len);
    dbuf once = {0};
    CHECK_OK(dc_catalog_put_index(entry, entry_len, d1, d1_len, &once));

    /* Same name, different backing file: createIndex's delete-then-create
     * clean slate. Appending instead would leave the entry describing one
     * index twice, and _open would attach it twice. */
    const uint8_t *d2; size_t d2_len;
    bj_builder *b2 = eq_def("team_1", "new.bj", "team", 0, &d2, &d2_len);
    dbuf twice = {0};
    CHECK_OK(dc_catalog_put_index(once.data, once.len, d2, d2_len, &twice));

    dbuf listed = {0};
    CHECK_OK(dc_catalog_list_indexes(twice.data, twice.len, &listed));
    CHECK_I64(arr_count(listed.data, listed.len), 1);
    CHECK(find_bytes(twice.data, twice.len, "new.bj", 6) != NULL);
    CHECK(find_bytes(twice.data, twice.len, "old.bj", 6) == NULL);

    dbuf_free(&listed);
    dbuf_free(&twice); dbuf_free(&once);
    bj_builder_free(b2); bj_builder_free(b1); bj_builder_free(eb);
}

TEST(catalog_drop_index_leaves_the_rest_intact) {
    const uint8_t *entry; size_t entry_len;
    bj_builder *eb = empty_entry(&entry, &entry_len);

    const uint8_t *d1; size_t d1_len;
    bj_builder *b1 = eq_def("team_1", "a.bj", "team", 0, &d1, &d1_len);
    dbuf one = {0};
    CHECK_OK(dc_catalog_put_index(entry, entry_len, d1, d1_len, &one));

    const uint8_t *d2; size_t d2_len;
    bj_builder *b2 = eq_def("age_1", "b.bj", "age", 0, &d2, &d2_len);
    dbuf two = {0};
    CHECK_OK(dc_catalog_put_index(one.data, one.len, d2, d2_len, &two));

    dbuf dropped = {0};
    CHECK_OK(dc_catalog_drop_index(two.data, two.len, "team_1", 6, &dropped));
    dbuf listed = {0};
    CHECK_OK(dc_catalog_list_indexes(dropped.data, dropped.len, &listed));
    CHECK_I64(arr_count(listed.data, listed.len), 1);
    CHECK(find_bytes(listed.data, listed.len, "age_1", 5) != NULL);
    CHECK(find_bytes(listed.data, listed.len, "team_1", 6) == NULL);
    /* The collection's own fields are untouched by an index change. */
    CHECK(find_bytes(dropped.data, dropped.len, "coll-users.bj", 13) != NULL);

    /* Dropping an absent name is a no-op, so a retry after a partial
     * failure is not refused. */
    dbuf again = {0};
    CHECK_OK(dc_catalog_drop_index(dropped.data, dropped.len, "team_1", 6, &again));

    dbuf_free(&again); dbuf_free(&listed); dbuf_free(&dropped);
    dbuf_free(&two); dbuf_free(&one);
    bj_builder_free(b2); bj_builder_free(b1); bj_builder_free(eb);
}

TEST(create_plan_names_and_classifies_indexes) {
    struct { const char *field; int dir_is_string; const char *dir; const char *want_name; int want_kind; }
    cases[] = {
        { "team", 0, NULL,       "team_1",       DC_INDEX_EQUALITY },
        { "body", 1, "text",     "body_text",    DC_INDEX_TEXT     },
        { "loc",  1, "2dsphere", "loc_2dsphere", DC_INDEX_GEO      },
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        doc *k = doc_new();
        if (cases[i].dir_is_string) doc_str(k, cases[i].field, cases[i].dir);
        else                        doc_int(k, cases[i].field, 1);
        uint32_t klen; const uint8_t *keys = doc_done(k, &klen);
        doc *o = doc_new();
        uint32_t olen; const uint8_t *opts = doc_done(o, &olen);

        dbuf plan = {0};
        CHECK_OK(dc_index_create_plan(keys, klen, opts, olen, "users", 5, &plan));
        CHECK(find_bytes(plan.data, plan.len, cases[i].want_name,
                         strlen(cases[i].want_name)) != NULL);
        dbuf_free(&plan);
        doc_free(o); doc_free(k);
    }

    /* Compound naming joins each field's "_1". */
    {
        doc *k = doc_new();
        doc_int(k, "team", 1);
        doc_int(k, "age", 1);
        uint32_t klen; const uint8_t *keys = doc_done(k, &klen);
        doc *o = doc_new();
        uint32_t olen; const uint8_t *opts = doc_done(o, &olen);
        dbuf plan = {0};
        CHECK_OK(dc_index_create_plan(keys, klen, opts, olen, "users", 5, &plan));
        CHECK(find_bytes(plan.data, plan.len, "team_1_age_1", 12) != NULL);
        CHECK(find_bytes(plan.data, plan.len, "idx-users-team_1_age_1.bj", 25) != NULL);
        dbuf_free(&plan);
        doc_free(o); doc_free(k);
    }

    /* A text index names its three files in attach order. */
    {
        doc *k = doc_new();
        doc_str(k, "body", "text");
        uint32_t klen; const uint8_t *keys = doc_done(k, &klen);
        doc *o = doc_new();
        uint32_t olen; const uint8_t *opts = doc_done(o, &olen);
        dbuf plan = {0};
        CHECK_OK(dc_index_create_plan(keys, klen, opts, olen, "posts", 5, &plan));
        const uint8_t *t = find_bytes(plan.data, plan.len, "body_text-terms.bj", 18);
        const uint8_t *d = find_bytes(plan.data, plan.len, "body_text-documents.bj", 22);
        const uint8_t *l = find_bytes(plan.data, plan.len, "body_text-lengths.bj", 20);
        CHECK(t && d && l);
        if (t && d && l) { CHECK(t < d); CHECK(d < l); }
        dbuf_free(&plan);
        doc_free(o); doc_free(k);
    }

    /* options.name overrides the convention, and the file follows it. */
    {
        doc *k = doc_new();
        doc_int(k, "team", 1);
        uint32_t klen; const uint8_t *keys = doc_done(k, &klen);
        doc *o = doc_new();
        doc_str(o, "name", "by_team");
        uint32_t olen; const uint8_t *opts = doc_done(o, &olen);
        dbuf plan = {0};
        CHECK_OK(dc_index_create_plan(keys, klen, opts, olen, "users", 5, &plan));
        CHECK(find_bytes(plan.data, plan.len, "idx-users-by_team.bj", 20) != NULL);
        dbuf_free(&plan);
        doc_free(o); doc_free(k);
    }
}

TEST(create_plan_enforces_the_option_rules) {
    /* Equality-only options on a special index. */
    {
        doc *k = doc_new();
        doc_str(k, "body", "text");
        uint32_t klen; const uint8_t *keys = doc_done(k, &klen);
        doc *o = doc_new();
        doc_key(o, "unique");
        bj_put_bool(o->b, 1);
        uint32_t olen; const uint8_t *opts = doc_done(o, &olen);
        dbuf plan = {0};
        CHECK_RC(dc_index_create_plan(keys, klen, opts, olen, "p", 1, &plan),
                 DC_ERR_INDEX_OPTION_UNSUPPORTED);
        dbuf_free(&plan);
        doc_free(o); doc_free(k);
    }
    /* ...but an explicitly FALSE one is not "supplied": a caller spreading
     * a defaults object must still be able to create a text index. */
    {
        doc *k = doc_new();
        doc_str(k, "body", "text");
        uint32_t klen; const uint8_t *keys = doc_done(k, &klen);
        doc *o = doc_new();
        doc_key(o, "unique");
        bj_put_bool(o->b, 0);
        doc_key(o, "sparse");
        bj_put_bool(o->b, 0);
        uint32_t olen; const uint8_t *opts = doc_done(o, &olen);
        dbuf plan = {0};
        CHECK_OK(dc_index_create_plan(keys, klen, opts, olen, "p", 1, &plan));
        dbuf_free(&plan);
        doc_free(o); doc_free(k);
    }
    /* A TTL needs one field to expire on. */
    {
        doc *k = doc_new();
        doc_int(k, "a", 1);
        doc_int(k, "b", 1);
        uint32_t klen; const uint8_t *keys = doc_done(k, &klen);
        doc *o = doc_new();
        doc_int(o, "expireAfterSeconds", 60);
        uint32_t olen; const uint8_t *opts = doc_done(o, &olen);
        dbuf plan = {0};
        CHECK_RC(dc_index_create_plan(keys, klen, opts, olen, "u", 1, &plan),
                 DC_ERR_TTL_NEEDS_SINGLE_FIELD);
        dbuf_free(&plan);
        doc_free(o); doc_free(k);
    }
    /* A descending spec is still refused, by the same validator
     * createIndex always used. */
    {
        doc *k = doc_new();
        doc_int(k, "team", -1);
        uint32_t klen; const uint8_t *keys = doc_done(k, &klen);
        doc *o = doc_new();
        uint32_t olen; const uint8_t *opts = doc_done(o, &olen);
        dbuf plan = {0};
        CHECK_RC(dc_index_create_plan(keys, klen, opts, olen, "u", 1, &plan),
                 DC_ERR_NON_ASCENDING_KEY);
        dbuf_free(&plan);
        doc_free(o); doc_free(k);
    }
}

TEST(create_plan_output_is_what_the_catalog_stores_and_replays) {
    /*
     * One shape, all the way round: create -> store -> open. If these three
     * ever disagree, an index is created under one name and reopened under
     * another, or with a file that was never made.
     */
    doc *k = doc_new();
    doc_str(k, "body", "text");
    uint32_t klen; const uint8_t *keys = doc_done(k, &klen);
    doc *o = doc_new();
    uint32_t olen; const uint8_t *opts = doc_done(o, &olen);

    dbuf plan = {0};
    CHECK_OK(dc_index_create_plan(keys, klen, opts, olen, "posts", 5, &plan));

    const uint8_t *entry; size_t entry_len;
    bj_builder *eb = empty_entry(&entry, &entry_len);
    dbuf stored = {0};
    CHECK_OK(dc_catalog_put_index(entry, entry_len, plan.data, plan.len, &stored));

    dbuf reopened = {0};
    CHECK_OK(dc_catalog_open_plan(stored.data, stored.len, "posts", 5, &reopened));

    /* Every file the create plan named must appear in the open plan. */
    CHECK(find_bytes(reopened.data, reopened.len, "body_text-terms.bj", 18) != NULL);
    CHECK(find_bytes(reopened.data, reopened.len, "body_text-documents.bj", 22) != NULL);
    CHECK(find_bytes(reopened.data, reopened.len, "body_text-lengths.bj", 20) != NULL);
    CHECK(find_bytes(reopened.data, reopened.len, "body_text", 9) != NULL);

    dbuf_free(&reopened); dbuf_free(&stored); dbuf_free(&plan);
    bj_builder_free(eb); doc_free(o); doc_free(k);
}

int main(void) {
    RUN(create_plan_names_and_classifies_indexes);
    RUN(create_plan_enforces_the_option_rules);
    RUN(create_plan_output_is_what_the_catalog_stores_and_replays);
    RUN(catalog_write_and_read_sides_agree);
    RUN(catalog_put_index_replaces_rather_than_duplicates);
    RUN(catalog_drop_index_leaves_the_rest_intact);
    RUN(catalog_plan_names_every_file_in_attach_order);
    RUN(catalog_plan_keeps_old_databases_openable);
    RUN(catalog_plan_refuses_an_entry_it_cannot_honor);
    RUN(catalog_list_indexes_inverts_what_create_index_stored);
    RUN(posix_namespace_backs_a_real_database);
    RUN(memory_io_is_accepted_without_a_sync_callback);
    RUN(current_date_rewrites_into_set);
    RUN(current_date_is_idempotent_and_passes_others_through);
    RUN(current_date_refuses_bad_specs_and_collisions);
    RUN(aggregate_group_uses_encoded_bytes_for_identity);
    RUN(aggregate_reports_the_stage_that_failed);
    RUN(aggregate_later_match_has_the_full_engine_grammar);
    RUN(bulk_grammar_accepts_every_operation_and_orders_the_codes);
    RUN(bulk_grammar_rejects_malformed_lists_and_names_the_index);
    RUN(ttl_cutoff_and_filter);
    RUN(strerror_covers_every_code_the_layer_can_raise);
    RUN(name_validation_matches_the_js_rules);
    RUN(index_key_spec_validates_and_emits_its_fields);
    RUN(file_names_match_the_original_js_scheme);
    RUN(orphan_sweep_pattern_matches_exactly_what_js_matched);
    RUN(keyenc_matches_the_original_js_encoder);
    RUN(keyenc_byte_order_matches_numeric_order);
    RUN(keyenc_rejects_unorderable_values);
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
