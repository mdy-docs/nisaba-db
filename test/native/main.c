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
#include "db_wal.h"
#include "db_session.h"
#include "snapstore.h"
#include "raft_core.h"
#include "raft_msg.h"
#include "raft_drive.h"
#include "raft_node.h"
#include "bjcursor.h"
#include "db_update.h"
#include "dbuf.h"
#include "bjio_posix.h"

#include "docs.h"
#include "memfs.h"
#include "nscheck.h"
#include "tap.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define ORDER DC_DEFAULT_ORDER   /* db_names.h; matches nisaba-wasm.js */

/*
 * Somewhere to put real files, for the tests that exercise bjio_posix.
 *
 * Not mkdtemp. That is POSIX-2008 and wasi-libc has no version of it,
 * because WASI has no global /tmp to invent a random name in: every path
 * resolves under a directory the runtime preopened, and a program cannot
 * reach outside one. So the base is the preopened cwd under WASI and
 * /tmp natively, and uniqueness comes from a counter plus mkdir's
 * refusal to clobber -- which is everything mkdtemp was providing to a
 * single-threaded, single-process harness.
 */
#ifdef __wasi__
#define SCRATCH_BASE "."
#else
#define SCRATCH_BASE "/tmp"
#endif

static int scratch_dir(const char *tag, char *out, size_t out_len) {
    static unsigned seq = 0;
    for (unsigned tries = 0; tries < 64; tries++) {
        /* The pid is what makes this survive being run twice. The tests
         * deliberately leave their directories behind ("the files are all
         * in tmpl"), /tmp is not cleaned between runs, and a bare counter
         * collides with the PREVIOUS run's names -- so the 65th run on a
         * developer machine fails right here, in a test that is otherwise
         * about compaction. (Observed, at 251 leftover directories.)
         *
         * Native only. WASI gets a fresh preopened directory every run so
         * nothing accumulates, and it has no process ids at all: getpid
         * there is a deprecated stub that needs -D_WASI_EMULATED_GETPID
         * and a library to match, which -Werror turns into a build
         * failure rather than a surprise. */
#ifdef __wasi__
        int n = snprintf(out, out_len, SCRATCH_BASE "/%s-%u", tag, seq++);
#else
        int n = snprintf(out, out_len, SCRATCH_BASE "/%s-%ld-%u",
                         tag, (long)getpid(), seq++);
#endif
        if (n < 0 || (size_t)n >= out_len) return -1;
        if (mkdir(out, 0700) == 0) return 0;
        if (errno != EEXIST) return -1;
    }
    return -1;
}


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
                            default_id, 0, &matched, &upserted, NULL, NULL, NULL));
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
    CHECK_OK(dc_update_one(fx.coll, qbuf, qlen, ubuf, ulen, default_id, 1, &result, NULL));
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

/* obj_get_field wants a length-counted key; this wraps the common
 * C-string case. bjcursor.h is an internal header, hence the local
 * helper rather than an addition to it. */
static int obj_get_field_probe(const uint8_t *obj, size_t len, const char *key,
                               const uint8_t **val, size_t *val_len, int *found) {
    return obj_get_field(obj, len, (const uint8_t *)key, (uint32_t)strlen(key),
                         val, val_len, found);
}

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
        DC_ERR_UNSUPPORTED_ID,
        DC_ERR_INVALID_COLLECTION_NAME, DC_ERR_INVALID_DB_NAME,
        DC_ERR_RESERVED_NAME, DC_ERR_EMPTY_KEY_SPEC, DC_ERR_NON_ASCENDING_KEY,
        DC_ERR_BULK_EMPTY, DC_ERR_BULK_UNKNOWN_OP, DC_ERR_BULK_MISSING_FIELD,
        DC_ERR_WAL_UNKNOWN_OP, DC_ERR_WAL_MISSING_FIELD, DC_ERR_WAL_BAD_REQUEST,
        DC_ERR_WAL_NOT_APPLIABLE, DC_ERR_WAL_NO_ID,
        /* db_session.h. NOT_APPLIABLE is here because it shared -35 with
         * DC_ERR_UNSUPPORTED_ID until these three needed the next free
         * codes: it had text only by accident, and the wrong text. */
        DC_ERR_NO_COLLECTION, DC_ERR_TOO_MANY_COLLECTIONS, DC_ERR_TOO_MANY_INDEXES,
        DC_ERR_REQ_MALFORMED, DC_ERR_REQ_UNKNOWN_OP, DC_ERR_REQ_MISSING_FIELD,
        DC_ERR_NO_DATABASE, DC_ERR_TOO_MANY_CLIENTS, DC_ERR_IDLE_TIMEOUT,
        DC_ERR_NO_CURSOR, DC_ERR_TOO_MANY_CURSORS, DC_ERR_CURSOR_SORTED,
        DC_ERR_NO_STREAM, DC_ERR_TOO_MANY_STREAMS,
        DC_ERR_CURSORS_OPEN, DC_ERR_FORMAT_NEWER, DC_ERR_INDEX_EXISTS,
        DC_ERR_NO_INDEX, DC_ERR_INDEX_KIND, DC_ERR_INDEX_ARITY,
        /* The consensus layer's refusals reach a host the same way, and
         * one that prints "unknown error" is one nobody can act on. */
        RAFT_ERR_MEMBER, RAFT_ERR_MESSAGE, RAFT_ERR_PEER, RAFT_ERR_CAPACITY,
        RAFT_ERR_BUSY,
    };
    const size_t n_codes = sizeof(codes) / sizeof(codes[0]);
    for (size_t i = 0; i < n_codes; i++) {
        const char *s = dc_strerror(codes[i]);
        if (!s || !*s) { TAP_FAIL("code %d has no text", codes[i]); continue; }
        if (strcmp(s, "unknown error") == 0)
            TAP_FAIL("code %d falls through to the default", codes[i]);
    }
    /* And no two of them may BE the same number. Nothing checked this,
     * which is how DC_ERR_WAL_NOT_APPLIABLE spent its life sharing -35
     * with DC_ERR_UNSUPPORTED_ID: it had text, so the loop above passed,
     * but the text was somebody else's and JS reported a misrouted DDL
     * command as InvalidIdError. "Every refusal is a distinct code" is
     * only true if something asserts the distinct half. */
    for (size_t i = 0; i < n_codes; i++)
        for (size_t j = i + 1; j < n_codes; j++)
            if (codes[i] == codes[j])
                TAP_FAIL("two refusals share code %d", codes[i]);
    /* Callers match on these prefixes; db.test.js and db.client-wasm
     * .test.js assert them by regex. */
    CHECK(strstr(dc_strerror(DC_ERR_INVALID_COLLECTION_NAME), "Invalid collection name") != NULL);
    CHECK(strstr(dc_strerror(DC_ERR_RESERVED_NAME), "Invalid collection name") != NULL);
    CHECK(strstr(dc_strerror(DC_ERR_INVALID_DB_NAME), "Invalid database name") != NULL);
    CHECK_STR(dc_strerror(DC_ERR_EMPTY_KEY_SPEC), "createIndex requires at least one field");
    /* An unmapped code still returns something printable. */
    CHECK(dc_strerror(-9999) != NULL);
}

TEST(divergence_classification_defaults_to_halting) {
    /*
     * A replicated apply loop uses this to tell a RESULT (every replica
     * reaches it) from DIVERGENCE (this replica alone failed). Getting
     * it wrong toward halting costs availability and a human notices;
     * getting it wrong the other way forks the state silently.
     */
    static const int deterministic[] = {
        DC_ERR_DUPLICATE, DC_ERR_ID_MISMATCH, DC_ERR_DUPLICATE_KEY,
        DC_ERR_MISSING_INDEXED_FIELD, DC_ERR_UNINDEXABLE_VALUE, DC_ERR_UNSUPPORTED_ID,
        DC_ERR_INVALID_COLLECTION_NAME, DC_ERR_INVALID_DB_NAME, DC_ERR_RESERVED_NAME,
        DC_ERR_EMPTY_KEY_SPEC, DC_ERR_NON_ASCENDING_KEY,
        DC_ERR_BULK_EMPTY, DC_ERR_BULK_UNKNOWN_OP, DC_ERR_BULK_MISSING_FIELD,
        DC_ERR_AGG_BAD_STAGE, DC_ERR_AGG_UNKNOWN_STAGE, DC_ERR_AGG_BAD_ACCUMULATOR,
        DC_ERR_AGG_PROJECT_MIXED,
        DC_ERR_BAD_CURRENT_DATE, DC_ERR_CURRENT_DATE_CONFLICT,
        DC_ERR_INDEX_OPTION_UNSUPPORTED, DC_ERR_TTL_NEEDS_SINGLE_FIELD,
        DC_ERR_WAL_UNKNOWN_OP, DC_ERR_WAL_MISSING_FIELD, DC_ERR_WAL_BAD_REQUEST,
        DC_ERR_WAL_NO_ID,
        /* The DDL three's outcomes: a re-applied createIndex finds the
         * index already there, a re-applied dropIndex finds it gone.
         * That is what convergence looks like from inside an apply
         * loop. */
        DC_ERR_INDEX_EXISTS, DC_ERR_NO_INDEX
    };
    for (size_t i = 0; i < sizeof(deterministic) / sizeof(deterministic[0]); i++) {
        if (!dc_is_deterministic(deterministic[i]))
            TAP_FAIL("code %d should be a deterministic result", deterministic[i]);
    }

    /* Each of these fails on ONE replica and not its peers, so each must
     * stop the node rather than be reported as an outcome. */
    static const int divergent[] = {
        BJ_ERR_OOM,           /* a local resource                        */
        BJ_ERR_STATE,         /* a programming error; halting finds it   */
        DC_ERR_CATALOG_ENTRY, /* THIS replica's catalog is damaged       */
        /* Same ambiguity, same answer: a command naming a collection
         * this replica does not have is either a log it cannot apply or
         * a state that has drifted, and ambiguity resolves toward
         * halting. dbs_apply's one exception is an INSERT, which makes
         * the collection rather than asking about it. */
        DC_ERR_NO_COLLECTION,
        BJ_ERR_EOF, BJ_ERR_UNKNOWN_TYPE, BJ_ERR_VERIFY, BJ_ERR_DEPTH,
        BJ_ERR_INT_RANGE, BJ_ERR_POINTER_RANGE, BJ_ERR_RANGE,
        BJ_OK                 /* not a failure at all                    */
    };
    for (size_t i = 0; i < sizeof(divergent) / sizeof(divergent[0]); i++) {
        if (dc_is_deterministic(divergent[i]))
            TAP_FAIL("code %d must halt the replica, not become a result", divergent[i]);
    }

    /* The default. A code nobody has classified -- including one added
     * next year -- is presumed divergence. */
    CHECK_I64(dc_is_deterministic(-9999), 0);
    CHECK_I64(dc_is_deterministic(-36), 0);
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
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-native", tmpl, sizeof tmpl) == 0);
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

/* Write a database the way a host writes one: a catalog, a collection
 * with an index the same planner createIndex uses, and three documents.
 * Leaves nothing open -- the point is that the bytes on disk are all the
 * session gets. Returns 0, or -1 having reported the failure itself. */
static int build_users_db(bj_ns *ns) {
    bj_io cat_io, coll_io, idx_io;
    if (ns->open(ns->ctx, DC_CATALOG_FILE, (uint32_t)strlen(DC_CATALOG_FILE),
                 BJ_NS_CREATE, &cat_io) != BJ_OK) return -1;
    bpt *catalog = bpt_create(&cat_io, ORDER);
    if (!catalog) return -1;

    if (ns->open(ns->ctx, "coll-users.bj", 13, BJ_NS_CREATE, &coll_io) != BJ_OK) return -1;
    bpt *primary = bpt_create(&coll_io, ORDER);
    if (!primary) return -1;
    dc_collection *coll = dc_collection_open(primary);
    if (!coll) return -1;

    /* The index definition comes from the create planner, so the name and
     * the file name are the ones a real createIndex would have chosen --
     * not this test's guess about the naming scheme. */
    doc *k = doc_new();
    doc_int(k, "team", 1);
    uint32_t klen; const uint8_t *keys = doc_done(k, &klen);
    doc *o = doc_new();
    uint32_t olen; const uint8_t *opts = doc_done(o, &olen);
    dbuf iplan = {0};
    int e = dc_index_create_plan(keys, klen, opts, olen, "users", 5, &iplan);
    doc_free(k); doc_free(o);
    if (e) return -1;

    if (ns->open(ns->ctx, "idx-users-team_1.bj", 19, BJ_NS_CREATE, &idx_io) != BJ_OK) return -1;
    bpt *idx = bpt_create(&idx_io, ORDER);
    if (!idx) return -1;
    const char *names[] = { "team" };
    const uint8_t *fields; uint32_t fields_len;
    bj_builder *fb = fields_of(names, 1, &fields, &fields_len);
    e = dc_collection_add_index(coll, "team_1", 6, idx, fields, fields_len, 0, 0, NULL, 0);
    bj_builder_free(fb);
    if (e) return -1;

    if (insert_person(coll, 1, "Ada", "core", 36) != BJ_OK) return -1;
    if (insert_person(coll, 2, "Grace", "core", 45) != BJ_OK) return -1;
    if (insert_person(coll, 3, "Alan", "research", 41) != BJ_OK) return -1;

    dbuf entry = {0}, full = {0};
    if (dc_catalog_new_entry("users", 5, &entry) != BJ_OK) return -1;
    if (dc_catalog_put_index(entry.data, entry.len, iplan.data, iplan.len, &full) != BJ_OK) return -1;
    bpt_key ckey = { .is_string = 1, .num = 0, .str = (const uint8_t *)"users", .str_len = 5 };
    e = bpt_add(catalog, &ckey, full.data, (uint32_t)full.len);
    dbuf_free(&full); dbuf_free(&entry); dbuf_free(&iplan);
    if (e) return -1;

    /* Trees first, then the ios: freeing a tree writes through the io it
     * borrows, and the namespace opened these so the namespace closes
     * them. */
    dc_collection_free(coll);
    bpt_free(idx); bpt_free(primary); bpt_free(catalog);
    ns->close(ns->ctx, &idx_io);
    ns->close(ns->ctx, &coll_io);
    ns->close(ns->ctx, &cat_io);
    return 0;
}

TEST(a_session_resolves_a_collection_by_name_with_no_host_language) {
    /*
     * The piece every host has been re-implementing: read the catalog,
     * find the entry, work out which files the collection is made of,
     * open each one, attach each index, recover the journal. Db.collection()
     * in nisaba-wasm.js is that code in JavaScript, and its absence in C
     * is why a process with no JS can apply a committed entry but cannot
     * find the collection to apply it to.
     *
     * The database here is written by one set of calls and opened by
     * another, over real files, which is the only way to catch the two
     * disagreeing about what a collection is made of.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-session", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ns) == 0);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 0, &s) == BJ_OK);
    CHECK_I64(dbs_open_count(s), 0);

    dc_collection *users = NULL;
    CHECK_OK(dbs_collection(s, "users", 5, &users));
    CHECK_FATAL(users != NULL);
    CHECK_I64(dbs_open_count(s), 1);

    /* The documents are there, which means the primary opened. */
    {
        const uint8_t *f; uint32_t flen;
        bj_builder *fb = empty_filter(&f, &flen);
        int64_t count = 0;
        CHECK_OK(dc_count(users, f, flen, &count));
        CHECK_I64(count, 3);
        bj_builder_free(fb);
    }

    /* And the INDEX is attached, not merely opened: the planner picks it
     * and names it. A file opened but never attached would pass a count
     * and fail here, which is the whole difference between reading a plan
     * and honouring one. */
    {
        doc *q = doc_new();
        doc_str(q, "team", "core");
        uint32_t qlen; const uint8_t *qbuf = doc_done(q, &qlen);
        int kind = -1; uint8_t *iname = NULL; size_t iname_len = 0;
        CHECK_OK(dc_explain(users, qbuf, qlen, &kind, &iname, &iname_len));
        CHECK_I64(kind, 2);                    /* equality index */
        CHECK_I64((int64_t)iname_len, 6);
        if (iname && iname_len == 6) CHECK(memcmp(iname, "team_1", 6) == 0);
        free(iname);
        doc_free(q);
    }

    /* Asking twice returns the same open collection rather than opening a
     * second copy of the same files -- which OPFS would refuse outright
     * and POSIX would silently allow, giving one collection two views of
     * one tree. */
    dc_collection *again = NULL;
    CHECK_OK(dbs_collection(s, "users", 5, &again));
    CHECK(again == users);
    CHECK_I64(dbs_open_count(s), 1);

    /* A name the catalog does not have is its own refusal, distinct from
     * an entry that cannot be honoured. */
    dc_collection *missing = NULL;
    CHECK_RC(dbs_collection(s, "nope", 4, &missing), DC_ERR_NO_COLLECTION);
    CHECK(missing == NULL);
    CHECK_I64(dbs_open_count(s), 1);

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(a_collection_that_cannot_be_opened_leaves_the_session_untouched) {
    /*
     * All or nothing. A file set that is half there must not leave a
     * session holding half a collection: the next attempt has to be a
     * retry, not a second attempt on top of a first one's wreckage.
     *
     * The index file is what goes missing here, because it is opened
     * AFTER the primary and the collection handle -- so a session that
     * unwinds badly leaks exactly the two things acquired before it.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-session-fail", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ns) == 0);

    CHECK_OK(ns.remove(ns.ctx, "idx-users-team_1.bj", 19));

    /* Through the checking adapter, which buys two things at once. It
     * COUNTS, so "leaves nothing open" is asserted rather than asserted
     * about -- a leaked handle is otherwise LeakSanitizer's to find, and
     * macOS ASan has no LeakSanitizer at all. And it refuses any name
     * that was not declared, so this also proves the session opens
     * exactly what the plan named and nothing of its own invention.
     * Four names: the catalog, the primary, the index, the journal. */
    bj_ns counted;
    nscheck *k = nscheck_new(&ns, &counted);
    CHECK_FATAL(k != NULL);
    nscheck_begin(k);
    CHECK_OK(nscheck_declare(k, DC_CATALOG_FILE, (uint32_t)strlen(DC_CATALOG_FILE)));
    CHECK_OK(nscheck_declare(k, "coll-users.bj", 13));
    CHECK_OK(nscheck_declare(k, "idx-users-team_1.bj", 19));
    CHECK_OK(nscheck_declare(k, "coll-users-journal.bj", 21));

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&counted, ORDER, 0, &s) == BJ_OK);
    CHECK_I64(nscheck_opens(k) - nscheck_closes(k), 1);   /* the catalog */

    dc_collection *users = NULL;
    CHECK_RC(dbs_collection(s, "users", 5, &users), BJ_ERR_STATE);
    CHECK(users == NULL);
    CHECK_I64(dbs_open_count(s), 0);
    /* The primary was opened and the index was not: whatever the failed
     * attempt took, it gave back, and only the catalog is still held. */
    CHECK_I64(nscheck_opens(k) - nscheck_closes(k), 1);

    /* The session is still usable afterwards -- a refusal is not a
     * poisoned session. */
    dc_collection *missing = NULL;
    CHECK_RC(dbs_collection(s, "nope", 4, &missing), DC_ERR_NO_COLLECTION);

    dbs_close(s);
    CHECK_I64(nscheck_opens(k) - nscheck_closes(k), 0);   /* including the catalog */
    if (nscheck_violations(k))
        TAP_FAIL("session opened a name no plan declared: %s", nscheck_first_violation(k));
    nscheck_end(k);
    nscheck_free(k);
    bjns_posix_free(&ns);
    close(dirfd);
}

/* Build one request object: {op, coll, <extra key>: <extra raw value>}.
 * The extra is spliced in raw, because that is how a client sends a
 * filter or a document -- already encoded. */
static bj_builder *request(const char *op, const char *coll,
                           const char *key, const uint8_t *val, size_t val_len,
                           const uint8_t **out, uint32_t *out_len) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"op", 2);
    bj_put_string(b, (const uint8_t *)op, (uint32_t)strlen(op));
    if (coll) {
        bj_put_key(b, (const uint8_t *)"coll", 4);
        bj_put_string(b, (const uint8_t *)coll, (uint32_t)strlen(coll));
    }
    if (key) {
        bj_put_key(b, (const uint8_t *)key, (uint32_t)strlen(key));
        bj_put_raw(b, val, (uint32_t)val_len);
    }
    bj_end_object(b);
    size_t len = 0;
    *out = bj_builder_data(b, &len);
    *out_len = (uint32_t)len;
    return b;
}

/* The same, plus the `id` a client sends alongside a write: the 12 bytes
 * an upsert falls back on. An insert is handed one here only to prove it
 * is not consulted -- the document's own _id is the only place an
 * insert's identity comes from. */
static bj_builder *request_with_id(const char *op, const char *coll,
                                   const char *key, const uint8_t *val, size_t val_len,
                                   const uint8_t id[12],
                                   const uint8_t **out, uint32_t *out_len) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"op", 2);
    bj_put_string(b, (const uint8_t *)op, (uint32_t)strlen(op));
    if (coll) {
        bj_put_key(b, (const uint8_t *)"coll", 4);
        bj_put_string(b, (const uint8_t *)coll, (uint32_t)strlen(coll));
    }
    if (key) {
        bj_put_key(b, (const uint8_t *)key, (uint32_t)strlen(key));
        bj_put_raw(b, val, (uint32_t)val_len);
    }
    bj_put_key(b, (const uint8_t *)"id", 2);
    bj_put_oid(b, id);
    bj_end_object(b);
    size_t len = 0;
    *out = bj_builder_data(b, &len);
    *out_len = (uint32_t)len;
    return b;
}

/* Read {ok:...} out of a response. */
static int response_ok(const dbuf *res) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(res->data, res->len, (const uint8_t *)"ok", 2, &v, &vlen, &found) || !found)
        return -1;
    cur c = { v, vlen, 0 };
    int ok = 0;
    if (read_bool(&c, &ok)) return -1;
    return ok;
}

/* A boolean field of a response -- `created`, `dropped`, `found`. -1 if
 * it is absent or is not a bool, which is a failure a caller wants to
 * see rather than a silent 0. */
static int response_flag(const dbuf *res, const char *key) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(res->data, res->len, (const uint8_t *)key,
                      (uint32_t)strlen(key), &v, &vlen, &found) || !found)
        return -1;
    cur c = { v, vlen, 0 };
    int flag = 0;
    if (read_bool(&c, &flag)) return -1;
    return flag;
}

/* Read a numeric field out of a response (or its nested `result`). */
static int64_t response_num(const dbuf *res, const char *key, int *found_out) {
    const uint8_t *v; size_t vlen; int found = 0;
    *found_out = 0;
    if (obj_get_field(res->data, res->len, (const uint8_t *)key,
                      (uint32_t)strlen(key), &v, &vlen, &found) || !found)
        return 0;
    cur c = { v, vlen, 0 };
    double d = 0;
    if (read_number(&c, &d)) return 0;
    *found_out = 1;
    return (int64_t)d;
}

TEST(a_request_is_answered_in_binjson_with_no_transport) {
    /*
     * The whole server surface, driven as what it is: one binjson object
     * in, one binjson object out. No socket, no port, no process -- which
     * is the entire reason dbs_handle is a function over buffers and
     * main() only frames. If this needed a listener to test, every future
     * change to the grammar would too.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-req", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ns) == 0);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 0, &s) == BJ_OK);

    /* ---- ping: the one op with no collection in it. It exists for the
     * server's idle timer (a client keeps its slot warm without
     * pretending to query), so it must not touch the catalog -- and the
     * collection field every other op requires is not required here. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("ping", NULL, NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, 0, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        const uint8_t *v; size_t vlen; int f = 0;
        CHECK_OK(obj_get_field(res.data, res.len, (const uint8_t *)"pong", 4, &v, &vlen, &f));
        CHECK_I64(f, 1);
        /* And it opened nothing to answer: no collection was named, so
         * none was resolved. */
        CHECK_I64(dbs_open_count(s), 0);
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- count over everything. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("count", "users", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, 0, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        int f = 0;
        CHECK_I64(response_num(&res, "n", &f), 3);
        CHECK_I64(f, 1);
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- find with a filter, served by the index the session attached. */
    {
        doc *q = doc_new();
        doc_str(q, "team", "core");
        uint32_t qlen; const uint8_t *qbuf = doc_done(q, &qlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("find", "users", "filter", qbuf, qlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, 0, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        /* `docs` is the array dc_find produced, spliced in rather than
         * rebuilt -- so it decodes as an array of the two core people. */
        const uint8_t *v; size_t vlen; int found = 0;
        CHECK_OK(obj_get_field(res.data, res.len, (const uint8_t *)"docs", 4, &v, &vlen, &found));
        CHECK_I64(found, 1);
        if (found) {
            cur c = { v, vlen, 0 };
            uint32_t n = 0;
            CHECK_OK(array_begin(&c, &n));
            CHECK_I64((int64_t)n, 2);
        }
        CHECK(find_bytes(res.data, res.len, "Ada", 3) != NULL);
        CHECK(find_bytes(res.data, res.len, "Grace", 5) != NULL);
        CHECK(find_bytes(res.data, res.len, "Alan", 4) == NULL);
        dbuf_free(&res); bj_builder_free(rb); doc_free(q);
    }

    /* ---- insert, through the WAL grammar, and see the count move. */
    {
        uint8_t id[12];
        mk_oid(id, 99);
        doc *d = doc_new();
        doc_oid(d, "_id", id);
        doc_str(d, "name", "Edsger");
        doc_str(d, "team", "core");
        uint32_t dlen; const uint8_t *dbuf_ = doc_done(d, &dlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("insert", "users", "doc", dbuf_, dlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, 0, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        dbuf_free(&res); bj_builder_free(rb); doc_free(d);

        bj_builder *cb = request("count", "users", NULL, NULL, 0, &req, &req_len);
        dbuf cres = {0};
        CHECK_OK(dbs_handle(s, 0, req, req_len, &cres));
        int f = 0;
        CHECK_I64(response_num(&cres, "n", &f), 4);
        dbuf_free(&cres); bj_builder_free(cb);
    }

    /* ---- deleteMany: many commands, ONE result, summed in C. */
    {
        doc *q = doc_new();
        doc_str(q, "team", "core");
        uint32_t qlen; const uint8_t *qbuf = doc_done(q, &qlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("deleteMany", "users", "filter", qbuf, qlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, 0, req, req_len, &res));
        if (response_ok(&res) != 1) { int df=0; TAP_FAIL("deleteMany refused: code %lld", (long long)response_num(&res, "code", &df)); }
        CHECK_I64(response_ok(&res), 1);
        /* Three core people were there; one result says three, rather
         * than three results a client would have had to add up itself. */
        const uint8_t *v; size_t vlen; int found = 0;
        CHECK_OK(obj_get_field(res.data, res.len, (const uint8_t *)"result", 6, &v, &vlen, &found));
        CHECK_I64(found, 1);
        if (found) {
            const uint8_t *n; size_t nlen; int nf = 0;
            CHECK_OK(obj_get_field(v, vlen, (const uint8_t *)"deletedCount", 12, &n, &nlen, &nf));
            CHECK_I64(nf, 1);
            if (nf) {
                cur c = { n, nlen, 0 };
                double d = 0;
                CHECK_OK(read_number(&c, &d));
                CHECK_I64((int64_t)d, 3);
            }
        }
        dbuf_free(&res); bj_builder_free(rb); doc_free(q);
    }

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

/* An {opts:{...}} sub-object for a request: batchSize, and optionally a
 * sort, which is the pair the cursor rules turn on. */
static bj_builder *opts_of(int64_t batch_size, int with_sort,
                           const uint8_t **out, uint32_t *out_len) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    if (batch_size) {
        bj_put_key(b, (const uint8_t *)"batchSize", 9);
        bj_put_int(b, batch_size);
    }
    if (with_sort) {
        bj_put_key(b, (const uint8_t *)"sort", 4);
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"name", 4);
        bj_put_int(b, 1);
        bj_end_object(b);
    }
    bj_end_object(b);
    size_t len = 0;
    *out = bj_builder_data(b, &len);
    *out_len = (uint32_t)len;
    return b;
}

/* {cursor: id} -- getMore and closeCursor name a cursor, not a collection. */
static bj_builder *cursor_request(const char *op, int64_t id,
                                  const uint8_t **out, uint32_t *out_len) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"op", 2);
    bj_put_string(b, (const uint8_t *)op, (uint32_t)strlen(op));
    bj_put_key(b, (const uint8_t *)"cursor", 6);
    bj_put_int(b, id);
    bj_end_object(b);
    size_t len = 0;
    *out = bj_builder_data(b, &len);
    *out_len = (uint32_t)len;
    return b;
}

/* How many elements are in a response's `docs` array. */
static int64_t response_docs(const dbuf *res) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(res->data, res->len, (const uint8_t *)"docs", 4, &v, &vlen, &found)
        || !found) return -1;
    cur c = { v, vlen, 0 };
    uint32_t count = 0;
    if (array_begin(&c, &count)) return -1;
    return (int64_t)count;
}

TEST(a_cursor_pages_a_scan_and_belongs_to_whoever_opened_it) {
    /*
     * batchSize turns a find into a cursor: one batch now, an id to ask
     * for the next. The point is that the SCAN is what is resumed, not a
     * materialized result -- dc_cursor_open holds a position in the B+
     * tree, so a client paging a million documents costs the server one
     * batch of memory, not a million documents of it.
     *
     * Everything here runs over buffers with no socket, which is the
     * whole reason dbs_handle takes a client id rather than reading one
     * off a connection.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-cursor", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ns) == 0);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 0, &s) == BJ_OK);

    const uint64_t ALICE = 7, BOB = 9;   /* two clients, one session */
    int64_t id = 0;

    /* ---- three documents, two at a time. */
    {
        const uint8_t *opts; uint32_t opts_len;
        bj_builder *ob = opts_of(2, 0, &opts, &opts_len);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("find", "users", "opts", opts, opts_len, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, ALICE, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(response_docs(&res), 2);
        int f = 0;
        id = response_num(&res, "cursor", &f);
        CHECK_I64(f, 1);          /* more to come, and here is how to ask */
        CHECK(id > 0);
        CHECK_I64(dbs_cursor_count(s), 1);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ob);
    }

    /* ---- a cursor is not a public name: Bob cannot advance Alice's. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = cursor_request("getMore", id, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, BOB, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_NO_CURSOR);
        CHECK_I64(dbs_cursor_count(s), 1);   /* and it is still Alice's */
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- the last document, and the cursor closes ITSELF: `cursor` comes
     * back null, so a client learns the scan is over without a second
     * round trip to find out. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = cursor_request("getMore", id, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, ALICE, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(response_docs(&res), 1);
        int f = 0;
        response_num(&res, "cursor", &f);
        CHECK_I64(f, 0);                     /* null, not an id */
        CHECK_I64(dbs_cursor_count(s), 0);   /* released, not leaked */
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- and asking again is refused rather than answered with nothing. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = cursor_request("getMore", id, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, ALICE, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_NO_CURSOR);
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- a SORTED find cannot be batched, and says why. An arbitrary
     * sort needs every match before the first ordered result exists --
     * db.c's rule, said here rather than quietly materialized. */
    {
        const uint8_t *opts; uint32_t opts_len;
        bj_builder *ob = opts_of(2, 1, &opts, &opts_len);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("find", "users", "opts", opts, opts_len, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, ALICE, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_CURSOR_SORTED);
        CHECK_I64(dbs_cursor_count(s), 0);   /* nothing was opened to refuse it */
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ob);
    }

    /* ---- the table is bounded, and full is a sentence. */
    {
        int64_t ids[DBS_MAX_CURSORS];
        for (int i = 0; i < DBS_MAX_CURSORS; i++) {
            const uint8_t *opts; uint32_t opts_len;
            bj_builder *ob = opts_of(1, 0, &opts, &opts_len);
            const uint8_t *req; uint32_t req_len;
            bj_builder *rb = request("find", "users", "opts", opts, opts_len, &req, &req_len);
            dbuf res = {0};
            CHECK_OK(dbs_handle(s, ALICE, req, req_len, &res));
            CHECK_I64(response_ok(&res), 1);
            int f = 0;
            ids[i] = response_num(&res, "cursor", &f);
            CHECK_I64(f, 1);
            dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ob);
        }
        CHECK_I64(dbs_cursor_count(s), DBS_MAX_CURSORS);
        {
            const uint8_t *opts; uint32_t opts_len;
            bj_builder *ob = opts_of(1, 0, &opts, &opts_len);
            const uint8_t *req; uint32_t req_len;
            bj_builder *rb = request("find", "users", "opts", opts, opts_len, &req, &req_len);
            dbuf res = {0};
            CHECK_OK(dbs_handle(s, ALICE, req, req_len, &res));
            CHECK_I64(response_ok(&res), 0);
            int f = 0;
            CHECK_I64(response_num(&res, "code", &f), DC_ERR_TOO_MANY_CURSORS);
            dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ob);
        }

        /* closeCursor gives one back. */
        {
            const uint8_t *req; uint32_t req_len;
            bj_builder *rb = cursor_request("closeCursor", ids[0], &req, &req_len);
            dbuf res = {0};
            CHECK_OK(dbs_handle(s, ALICE, req, req_len, &res));
            CHECK_I64(response_ok(&res), 1);
            CHECK_I64(dbs_cursor_count(s), DBS_MAX_CURSORS - 1);
            dbuf_free(&res); bj_builder_free(rb);
        }

        /* And a client that goes away gives back everything it held --
         * which is the only reason an abandoned scan is not a permanent
         * slot. The transport calls this; nothing else can know. */
        dbs_drop_client(s, ALICE);
        CHECK_I64(dbs_cursor_count(s), 0);
    }

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(ddl_is_a_command_a_second_database_can_be_caught_up_by) {
    /*
     * The DDL three used to be the one thing this server did that left
     * nothing behind to send anywhere. Writes went through
     * dc_wal_plan_build and dc_wal_apply -- "every mutation this serves
     * is one a log could have carried" -- and createIndex, dropIndex and
     * dropCollection called dbs_* directly, so a follower would never
     * hear about them. docs/replicaton-roadmap.md step 4 names it: the
     * single-node "unlogged DDL is safe" argument dies with the first
     * follower.
     *
     * So this is the test that says what "logged" has to mean. Two
     * separate databases: one is asked over the wire, and the OTHER is
     * handed nothing but the commands that produced the answer. If it
     * ends up in the same shape, the command carries everything a
     * replica needs; if it does not, the command is missing something no
     * amount of transport will supply.
     */
    char a_dir[64], b_dir[64];
    CHECK_FATAL(scratch_dir("nisaba-ddl-a", a_dir, sizeof a_dir) == 0);
    CHECK_FATAL(scratch_dir("nisaba-ddl-b", b_dir, sizeof b_dir) == 0);
    int afd = open(a_dir, O_RDONLY), bfd = open(b_dir, O_RDONLY);
    CHECK_FATAL(afd >= 0 && bfd >= 0);
    bj_ns ans, bns;
    CHECK_FATAL(bjns_posix_open(afd, &ans) == BJ_OK);
    CHECK_FATAL(bjns_posix_open(bfd, &bns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ans) == 0);
    CHECK_FATAL(build_users_db(&bns) == 0);

    dbs *leader = NULL, *replica = NULL;
    CHECK_FATAL(dbs_open(&ans, ORDER, 0, &leader) == BJ_OK);
    CHECK_FATAL(dbs_open(&bns, ORDER, 0, &replica) == BJ_OK);
    const uint64_t CLIENT = 4;

    /* ---- the leader is asked, the way a client asks. */
    doc *k = doc_new();
    doc_int(k, "name", 1);
    uint32_t klen; const uint8_t *kb = doc_done(k, &klen);
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("createIndex", "users", "keys", kb, klen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(leader, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK(find_bytes(res.data, res.len, "name_1", 6) != NULL);
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- the replica is handed the COMMAND, which is all a follower
     * ever gets. Built here the way the leader built it, because that is
     * the claim: this command is what the leader would have logged.
     *
     * Note what it does NOT need: `options` was never sent by the
     * client, and the command carries `{}` rather than leaving the field
     * out -- a replica must not have to guess at a field's absence. */
    {
        static const uint8_t EMPTY[9] = { BJ_TYPE_OBJECT, 4,0,0,0, 0,0,0,0 };
        dc_wal_plan *p = NULL;
        CHECK_OK(dc_wal_plan_build(NULL, "users", 5, DC_WREQ_CREATE_INDEX,
                                   kb, klen, EMPTY, sizeof EMPTY, 0, NULL, &p));
        uint32_t clen; const uint8_t *cmd = dc_wal_plan_cmd(p, 0, &clen);
        dbuf res = {0};
        CHECK_OK(dbs_apply(replica, 0, cmd, clen, &res));
        /* The applier's result, not the wire's: which name the catalog
         * chose is part of what the command did, and every replica
         * computes it rather than being told. */
        CHECK(find_bytes(res.data, res.len, "name_1", 6) != NULL);
        dbuf_free(&res);

        /* Applied twice is what a replay is, and it is a deterministic
         * refusal rather than a second index or a silent success --
         * which is exactly why dc_is_deterministic classifies it. */
        dbuf again = {0};
        CHECK_RC(dbs_apply(replica, 0, cmd, clen, &again), DC_ERR_INDEX_EXISTS);
        CHECK_I64(dc_is_deterministic(DC_ERR_INDEX_EXISTS), 1);
        dbuf_free(&again);
        dc_wal_plan_free(p);
    }

    /* ---- and both databases now say the same thing about themselves. */
    for (int side = 0; side < 2; side++) {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("listIndexes", "users", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(side ? replica : leader, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK(find_bytes(res.data, res.len, "name_1", 6) != NULL);
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- dropIndex, the same way round. */
    {
        /* {index: "team_1"} as a bare encoded string, spliced in the way
         * a client's codec would produce it. */
        bj_builder *nb = bj_builder_new();
        bj_put_string(nb, (const uint8_t *)"name_1", 6);
        size_t nlen = 0; const uint8_t *nval = bj_builder_data(nb, &nlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("dropIndex", "users", "index", nval, nlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(leader, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(response_flag(&res, "dropped"), 1);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(nb);

        dc_wal_plan *p = NULL;
        CHECK_OK(dc_wal_plan_build(NULL, "users", 5, DC_WREQ_DROP_INDEX,
                                   (const uint8_t *)"name_1", 6, NULL, 0, 0, NULL, &p));
        uint32_t clen; const uint8_t *cmd = dc_wal_plan_cmd(p, 0, &clen);
        dbuf ares = {0};
        CHECK_OK(dbs_apply(replica, 0, cmd, clen, &ares));
        dbuf_free(&ares);
        /* Replayed: gone is gone, deterministically. */
        dbuf twice = {0};
        CHECK_RC(dbs_apply(replica, 0, cmd, clen, &twice), DC_ERR_NO_INDEX);
        dbuf_free(&twice);
        dc_wal_plan_free(p);
    }

    /* ---- a document command needs no collection to exist first: a
     * first insert makes one, which is what lets createCollection have
     * no command of its own. The replica has never heard of `fresh`. */
    {
        uint8_t id[12]; mk_oid(id, 61);
        doc *d = doc_new();
        doc_oid(d, "_id", id);
        doc_str(d, "who", "new");
        uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
        dc_wal_plan *p = NULL;
        CHECK_OK(dc_wal_plan_build(NULL, "fresh", 5, DC_WREQ_INSERT_ONE,
                                   db_, dlen, NULL, 0, 0, NULL, &p));
        uint32_t clen; const uint8_t *cmd = dc_wal_plan_cmd(p, 0, &clen);
        dbuf res = {0};
        CHECK_OK(dbs_apply(replica, 7, cmd, clen, &res));
        dbuf_free(&res);
        dc_wal_plan_free(p);

        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("count", "fresh", NULL, NULL, 0, &req, &req_len);
        dbuf cres = {0};
        CHECK_OK(dbs_handle(replica, CLIENT, req, req_len, &cres));
        int f = 0;
        CHECK_I64(response_num(&cres, "n", &f), 1);
        dbuf_free(&cres); bj_builder_free(rb);
        doc_free(d);
    }

    /* ---- dropCollection reports what it did, and a replay of it says
     * "nothing to drop" rather than failing. */
    {
        dc_wal_plan *p = NULL;
        CHECK_OK(dc_wal_plan_build(NULL, "fresh", 5, DC_WREQ_DROP_COLLECTION,
                                   NULL, 0, NULL, 0, 0, NULL, &p));
        uint32_t clen; const uint8_t *cmd = dc_wal_plan_cmd(p, 0, &clen);
        dbuf res = {0};
        CHECK_OK(dbs_apply(replica, 0, cmd, clen, &res));
        {
            const uint8_t *v; size_t vlen; int f = 0;
            CHECK_OK(obj_get_field(res.data, res.len, (const uint8_t *)"dropped", 7,
                                   &v, &vlen, &f));
            CHECK_I64(f, 1);
            cur c = { v, vlen, 0 }; int b = 0;
            CHECK_OK(read_bool(&c, &b));
            CHECK_I64(b, 1);
        }
        dbuf_free(&res);
        dbuf again = {0};
        CHECK_OK(dbs_apply(replica, 0, cmd, clen, &again));
        {
            const uint8_t *v; size_t vlen; int f = 0;
            CHECK_OK(obj_get_field(again.data, again.len, (const uint8_t *)"dropped", 7,
                                   &v, &vlen, &f));
            cur c = { v, vlen, 0 }; int b = 1;
            CHECK_OK(read_bool(&c, &b));
            CHECK_I64(b, 0);        /* there was nothing left to drop */
        }
        dbuf_free(&again);
        dc_wal_plan_free(p);
    }

    /* ---- and a command this build cannot execute is REFUSED, not
     * skipped: the difference between a node that stops and one that has
     * quietly diverged from its peers. */
    {
        doc *bad = doc_new();
        doc_str(bad, "c", "users");
        doc_str(bad, "op", "reticulate");
        uint32_t bl; const uint8_t *bcmd = doc_done(bad, &bl);
        dbuf res = {0};
        CHECK_RC(dbs_apply(replica, 0, bcmd, bl, &res), DC_ERR_WAL_UNKNOWN_OP);
        dbuf_free(&res);
        doc_free(bad);
    }

    doc_free(k);
    dbs_close(replica); dbs_close(leader);
    bjns_posix_free(&bns); bjns_posix_free(&ans);
    close(bfd); close(afd);
}

TEST(a_database_can_be_built_from_an_empty_directory) {
    /*
     * The other half of "the engine is C": until now this layer could
     * open a database somebody else had written and could not write one.
     * Every schema decision was already here (db_catalog.h names files,
     * kinds and options); what was missing was the choreography around
     * it -- create the files, attach, backfill, record.
     *
     * So: an empty directory, and a whole database built through
     * dbs_handle. Nothing in this test knows a file name.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-fresh", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);

    /* Without `create`, an empty directory is not a database and says
     * so rather than quietly becoming one. */
    dbs *refused = NULL;
    CHECK_RC(dbs_open(&ns, ORDER, 0, &refused), DC_ERR_NO_DATABASE);
    CHECK(refused == NULL);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 1, &s) == BJ_OK);
    const uint64_t CLIENT = 5;

    /* ---- a collection, made and made again. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("createCollection", "users", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(response_flag(&res, "created"), 1);
        dbuf_free(&res); bj_builder_free(rb);

        /* Idempotent: already there is success, and says it made nothing. */
        rb = request("createCollection", "users", NULL, NULL, 0, &req, &req_len);
        dbuf again = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &again));
        CHECK_I64(response_ok(&again), 1);
        CHECK_I64(response_flag(&again, "created"), 0);
        dbuf_free(&again); bj_builder_free(rb);
    }

    /* ---- documents, and a collection nobody created: an insert makes
     * one, the way it does in every other host of this library. */
    for (int i = 0; i < 4; i++) {
        doc *d = doc_new();
        uint8_t oid[12] = { 0 };
        oid[11] = (uint8_t)(i + 1);
        doc_oid(d, "_id", oid);
        doc_str(d, "name", i < 2 ? "core person" : "other person");
        doc_str(d, "team", i < 2 ? "core" : "research");
        uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("insert", i < 3 ? "users" : "notes", "doc", db_, dlen,
                                 &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        dbuf_free(&res); bj_builder_free(rb); doc_free(d);
    }

    /* ---- an index, planned and backfilled against what is already there. */
    {
        doc *k = doc_new();
        doc_int(k, "team", 1);
        uint32_t klen; const uint8_t *kb = doc_done(k, &klen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("createIndex", "users", "keys", kb, klen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        /* The name is C's to choose, and it chose the driver's. */
        CHECK(find_bytes(res.data, res.len, "team_1", 6) != NULL);
        dbuf_free(&res); bj_builder_free(rb);

        /* And a second one of the same shape is refused by name. */
        rb = request("createIndex", "users", "keys", kb, klen, &req, &req_len);
        dbuf dup = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &dup));
        CHECK_I64(response_ok(&dup), 0);
        int f = 0;
        CHECK_I64(response_num(&dup, "code", &f), DC_ERR_INDEX_EXISTS);
        dbuf_free(&dup); bj_builder_free(rb); doc_free(k);
    }

    /* ---- what is in this database, asked without naming anything.
     * The catalog's keys are the names, so the format stamp -- a key in
     * that same tree -- must not appear among them. */
    {
        bj_builder *rb = bj_builder_new();
        bj_begin_object(rb);
        bj_put_key(rb, (const uint8_t *)"op", 2);
        bj_put_string(rb, (const uint8_t *)"listCollections", 15);
        bj_end_object(rb);
        size_t rl = 0; const uint8_t *req = bj_builder_data(rb, &rl);

        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, (uint32_t)rl, &res));
        CHECK_I64(response_ok(&res), 1);
        const uint8_t *v; size_t vlen; int f = 0;
        CHECK_OK(obj_get_field(res.data, res.len, (const uint8_t *)"collections", 11,
                               &v, &vlen, &f));
        CHECK_I64(f, 1);
        cur c = { v, vlen, 0 };
        uint32_t n = 0;
        CHECK_OK(array_begin(&c, &n));
        CHECK_I64((int64_t)n, 2);                       /* users and notes */
        CHECK(find_bytes(v, vlen, "users", 5) != NULL);
        CHECK(find_bytes(v, vlen, "notes", 5) != NULL);
        CHECK(find_bytes(v, vlen, DC_FORMAT_KEY, strlen(DC_FORMAT_KEY)) == NULL);
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- listIndexes, in the shape a driver expects. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("listIndexes", "users", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK(find_bytes(res.data, res.len, "team_1", 6) != NULL);
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- the index serves a query, which is the only proof that the
     * backfill happened. */
    {
        doc *q = doc_new();
        doc_str(q, "team", "core");
        uint32_t qlen; const uint8_t *qb = doc_done(q, &qlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("count", "users", "filter", qb, qlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        int f = 0;
        CHECK_I64(response_num(&res, "n", &f), 2);
        dbuf_free(&res); bj_builder_free(rb); doc_free(q);
    }

    /* ---- dropped by name, and refused twice. */
    {
        bj_builder *rb = bj_builder_new();
        bj_begin_object(rb);
        bj_put_key(rb, (const uint8_t *)"op", 2);
        bj_put_string(rb, (const uint8_t *)"dropIndex", 9);
        bj_put_key(rb, (const uint8_t *)"coll", 4);
        bj_put_string(rb, (const uint8_t *)"users", 5);
        bj_put_key(rb, (const uint8_t *)"index", 5);
        bj_put_string(rb, (const uint8_t *)"team_1", 6);
        bj_end_object(rb);
        size_t rl = 0; const uint8_t *req = bj_builder_data(rb, &rl);

        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, (uint32_t)rl, &res));
        CHECK_I64(response_ok(&res), 1);
        dbuf_free(&res);

        dbuf twice = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, (uint32_t)rl, &twice));
        CHECK_I64(response_ok(&twice), 0);
        int f = 0;
        CHECK_I64(response_num(&twice, "code", &f), DC_ERR_NO_INDEX);
        dbuf_free(&twice); bj_builder_free(rb);
    }

    /* ---- a collection dropped is gone from the catalog, and the other
     * one is untouched. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("dropCollection", "users", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(response_flag(&res, "dropped"), 1);
        dbuf_free(&res); bj_builder_free(rb);

        rb = request("count", "users", NULL, NULL, 0, &req, &req_len);
        dbuf gone = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &gone));
        CHECK_I64(response_ok(&gone), 0);
        int f = 0;
        CHECK_I64(response_num(&gone, "code", &f), DC_ERR_NO_COLLECTION);
        dbuf_free(&gone); bj_builder_free(rb);

        /* And the listing agrees with the drop: one collection left. */
        {
            bj_builder *lb = bj_builder_new();
            bj_begin_object(lb);
            bj_put_key(lb, (const uint8_t *)"op", 2);
            bj_put_string(lb, (const uint8_t *)"listCollections", 15);
            bj_end_object(lb);
            size_t ll = 0; const uint8_t *lreq = bj_builder_data(lb, &ll);
            dbuf list = {0};
            CHECK_OK(dbs_handle(s, CLIENT, lreq, (uint32_t)ll, &list));
            const uint8_t *v; size_t vlen; int lf = 0;
            CHECK_OK(obj_get_field(list.data, list.len, (const uint8_t *)"collections", 11,
                                   &v, &vlen, &lf));
            cur c = { v, vlen, 0 };
            uint32_t n = 0;
            CHECK_OK(array_begin(&c, &n));
            CHECK_I64((int64_t)n, 1);
            CHECK(find_bytes(v, vlen, "users", 5) == NULL);
            dbuf_free(&list); bj_builder_free(lb);
        }

        rb = request("count", "notes", NULL, NULL, 0, &req, &req_len);
        dbuf kept = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &kept));
        f = 0;
        CHECK_I64(response_num(&kept, "n", &f), 1);
        dbuf_free(&kept); bj_builder_free(rb);
    }

    dbs_close(s);

    /* ---- and what it wrote is a database: reopened WITHOUT create,
     * which is the check that the catalog and the format stamp are real
     * rather than something this session was holding in memory. */
    {
        dbs *reopened = NULL;
        CHECK_FATAL(dbs_open(&ns, ORDER, 0, &reopened) == BJ_OK);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("count", "notes", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(reopened, CLIENT, req, req_len, &res));
        int f = 0;
        CHECK_I64(response_num(&res, "n", &f), 1);
        dbuf_free(&res); bj_builder_free(rb);
        dbs_close(reopened);
    }

    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(compact_over_the_wire_reclaims_and_refuses_while_a_cursor_reads) {
    /*
     * Compaction as a REQUEST: the same plan/stream/flip/reopen/delete
     * the browser drives with awaits between every step, driven here by
     * one binjson object -- because under POSIX ns->open really opens,
     * so nothing has to happen between the plan and the execute.
     *
     * And the guard, end to end: a cursor open over the collection makes
     * this refuse with the code and the sentence, rather than pulling
     * the files out from under a scan.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-wire-cmp", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ns) == 0);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 0, &s) == BJ_OK);
    const uint64_t CLIENT = 3;

    /* Churn: every document replaced, so the append-only file holds far
     * more than the live set it will compact to. */
    for (int round = 0; round < 8; round++) {
        doc *f = doc_new();
        doc_str(f, "team", "core");
        uint32_t flen; const uint8_t *fb = doc_done(f, &flen);
        doc *u = doc_new();
        doc *set = doc_new();
        doc_int(set, "round", round);
        uint32_t slen; const uint8_t *sb = doc_done(set, &slen);
        bj_builder *ub = bj_builder_new();
        bj_begin_object(ub);
        bj_put_key(ub, (const uint8_t *)"$set", 4);
        bj_put_raw(ub, sb, slen);
        bj_end_object(ub);
        size_t ulen_s = 0; const uint8_t *ubytes = bj_builder_data(ub, &ulen_s);

        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = bj_builder_new();
        bj_begin_object(rb);
        bj_put_key(rb, (const uint8_t *)"op", 2);
        bj_put_string(rb, (const uint8_t *)"updateMany", 10);
        bj_put_key(rb, (const uint8_t *)"coll", 4);
        bj_put_string(rb, (const uint8_t *)"users", 5);
        bj_put_key(rb, (const uint8_t *)"filter", 6);
        bj_put_raw(rb, fb, flen);
        bj_put_key(rb, (const uint8_t *)"update", 6);
        bj_put_raw(rb, ubytes, (uint32_t)ulen_s);
        bj_end_object(rb);
        size_t rl = 0; req = bj_builder_data(rb, &rl); req_len = (uint32_t)rl;

        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        dbuf_free(&res);
        bj_builder_free(rb); bj_builder_free(ub);
        doc_free(f); doc_free(u); doc_free(set);
    }

    /* ---- a cursor over the collection, and compaction refuses. */
    int64_t cursor_id = 0;
    {
        const uint8_t *opts; uint32_t opts_len;
        bj_builder *ob = opts_of(1, 0, &opts, &opts_len);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("find", "users", "opts", opts, opts_len, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        int f = 0;
        cursor_id = response_num(&res, "cursor", &f);
        CHECK_I64(f, 1);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ob);
    }
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("compact", "users", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_CURSORS_OPEN);
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- kill it, and the same request goes through. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = cursor_request("closeCursor", cursor_id, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        dbuf_free(&res); bj_builder_free(rb);
    }

    int64_t before = 0, after = 0;
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("compact", "users", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        const uint8_t *r; size_t rlen; int f = 0;
        CHECK_OK(obj_get_field(res.data, res.len, (const uint8_t *)"result", 6, &r, &rlen, &f));
        CHECK_I64(f, 1);
        dbuf rres = {0};
        CHECK_OK(dbuf_put(&rres, r, rlen));
        int nf = 0;
        CHECK_I64(response_num(&rres, "generation", &nf), 1);
        before = response_num(&rres, "bytesBefore", &nf);
        after  = response_num(&rres, "bytesAfter", &nf);
        CHECK_I64(response_num(&rres, "bytesFreed", &nf), before - after);
        dbuf_free(&rres);
        dbuf_free(&res); bj_builder_free(rb);
    }
    /* Reclaiming is the point: eight rounds of updates over three
     * documents leave far more file than the live set needs. */
    CHECK(after < before);

    /* ---- and the collection still answers, from the new generation the
     * session reopened for itself. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("count", "users", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        int f = 0;
        CHECK_I64(response_num(&res, "n", &f), 3);
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- the index came with it: an equality lookup still resolves
     * through the compacted index file, not the deleted one. */
    {
        doc *q = doc_new();
        doc_str(q, "team", "core");
        uint32_t qlen; const uint8_t *qb = doc_done(q, &qlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("count", "users", "filter", qb, qlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        int f = 0;
        CHECK_I64(response_num(&res, "n", &f), 2);
        dbuf_free(&res); bj_builder_free(rb); doc_free(q);
    }

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

/* {op, coll, docs|writes: <array>, ordered: <bool>} -- the request shape
 * a list of writes needs, which `request` above cannot spell. */
static bj_builder *list_request(const char *op, const char *coll, const char *key,
                                const uint8_t *val, size_t val_len, int ordered,
                                const uint8_t **out, uint32_t *out_len) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"op", 2);
    bj_put_string(b, (const uint8_t *)op, (uint32_t)strlen(op));
    bj_put_key(b, (const uint8_t *)"coll", 4);
    bj_put_string(b, (const uint8_t *)coll, (uint32_t)strlen(coll));
    bj_put_key(b, (const uint8_t *)key, (uint32_t)strlen(key));
    bj_put_raw(b, val, (uint32_t)val_len);
    bj_put_key(b, (const uint8_t *)"ordered", 7);
    bj_put_bool(b, ordered);
    bj_end_object(b);
    size_t len = 0;
    *out = bj_builder_data(b, &len);
    *out_len = (uint32_t)len;
    return b;
}

/* A numeric field of the nested `result` object: insertedCount and its
 * siblings live there, not at the top of the response. */
static int64_t result_num(const dbuf *res, const char *key) {
    const uint8_t *r; size_t rlen; int f = 0;
    if (obj_get_field(res->data, res->len, (const uint8_t *)"result", 6, &r, &rlen, &f) || !f)
        return -1;
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(r, rlen, (const uint8_t *)key, (uint32_t)strlen(key),
                      &v, &vlen, &found) || !found)
        return -1;
    cur c = { v, vlen, 0 };
    double d = 0;
    if (read_number(&c, &d)) return -1;
    return (int64_t)d;
}

/* How many entries in a list response's `errors` / `upserted`. Absent is
 * a null rather than an empty array, and both mean none. */
static long list_count(const dbuf *res, const char *list) {
    const uint8_t *v; size_t vlen; int f = 0;
    if (obj_get_field(res->data, res->len, (const uint8_t *)list,
                      (uint32_t)strlen(list), &v, &vlen, &f) || !f) return -1;
    if (vlen >= 1 && v[0] == BJ_TYPE_NULL) return 0;
    return arr_count(v, vlen);
}

/* A numeric field of the i-th entry of one of those arrays. */
static int64_t list_num(const dbuf *res, const char *list, int i, const char *key) {
    const uint8_t *v; size_t vlen; int f = 0;
    if (obj_get_field(res->data, res->len, (const uint8_t *)list,
                      (uint32_t)strlen(list), &v, &vlen, &f) || !f) return -1;
    cur c = { v, vlen, 0 };
    uint32_t n = 0;
    if (array_begin(&c, &n) || (uint32_t)i >= n) return -1;
    for (int k = 0; k < i; k++) if (skip_value(&c)) return -1;
    size_t start = c.pos;
    if (skip_value(&c)) return -1;
    const uint8_t *fv; size_t flen; int found = 0;
    if (obj_get_field(v + start, c.pos - start, (const uint8_t *)key,
                      (uint32_t)strlen(key), &fv, &flen, &found) || !found) return -1;
    cur ec = { fv, flen, 0 };
    double d = 0;
    if (read_number(&ec, &d)) return -1;
    return (int64_t)d;
}

/* One {<name>: {<field>: <raw value>}} operation for a bulkWrite list,
 * with an optional second field -- the shape db_bulk.h defines. */
static void put_bulk_op(bj_builder *b, const char *name,
                        const char *k1, const uint8_t *v1, uint32_t n1,
                        const char *k2, const uint8_t *v2, uint32_t n2,
                        const uint8_t *upsert_id) {
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)name, (uint32_t)strlen(name));
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)k1, (uint32_t)strlen(k1));
    bj_put_raw(b, v1, n1);
    if (k2) {
        bj_put_key(b, (const uint8_t *)k2, (uint32_t)strlen(k2));
        bj_put_raw(b, v2, n2);
    }
    if (upsert_id) {
        bj_put_key(b, (const uint8_t *)"upsert", 6);
        bj_put_bool(b, 1);
        bj_put_key(b, (const uint8_t *)"id", 2);
        bj_put_oid(b, upsert_id);
    }
    bj_end_object(b);
    bj_end_object(b);
}

/* {op, coll, filter, <arg key>: <arg>, returnNew?, id?} -- the
 * find-one-and-* request shape, which `request` cannot spell. */
static bj_builder *modify_request(const char *op, const char *coll,
                                  const uint8_t *filter, uint32_t flen,
                                  const char *arg_key, const uint8_t *arg, uint32_t alen,
                                  int return_new, const uint8_t *id,
                                  const uint8_t **out, uint32_t *out_len) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"op", 2);
    bj_put_string(b, (const uint8_t *)op, (uint32_t)strlen(op));
    bj_put_key(b, (const uint8_t *)"coll", 4);
    bj_put_string(b, (const uint8_t *)coll, (uint32_t)strlen(coll));
    bj_put_key(b, (const uint8_t *)"filter", 6);
    bj_put_raw(b, filter, flen);
    if (arg_key) {
        bj_put_key(b, (const uint8_t *)arg_key, (uint32_t)strlen(arg_key));
        bj_put_raw(b, arg, alen);
    }
    if (return_new) {
        bj_put_key(b, (const uint8_t *)"returnNew", 9);
        bj_put_bool(b, 1);
    }
    if (id) {
        bj_put_key(b, (const uint8_t *)"upsert", 6);
        bj_put_bool(b, 1);
        bj_put_key(b, (const uint8_t *)"id", 2);
        bj_put_oid(b, id);
    }
    bj_end_object(b);
    size_t len = 0;
    *out = bj_builder_data(b, &len);
    *out_len = (uint32_t)len;
    return b;
}

/* A response's `doc`, as a string field of it. Returns 0 if the response
 * carried no document (found:false) or no such field. */
static int response_doc_str(const dbuf *res, const char *key, char *out, size_t cap) {
    const uint8_t *d; size_t dlen; int f = 0;
    out[0] = '\0';
    if (obj_get_field(res->data, res->len, (const uint8_t *)"doc", 3, &d, &dlen, &f) || !f)
        return 0;
    return doc_get_str(d, dlen, key, out, cap);
}

/* {op, coll, index: "<name>", values: [...]} */
static bj_builder *index_request(const char *op, const char *coll, const char *index,
                                 const uint8_t *values, uint32_t vlen,
                                 const uint8_t **out, uint32_t *out_len) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"op", 2);
    bj_put_string(b, (const uint8_t *)op, (uint32_t)strlen(op));
    bj_put_key(b, (const uint8_t *)"coll", 4);
    bj_put_string(b, (const uint8_t *)coll, (uint32_t)strlen(coll));
    bj_put_key(b, (const uint8_t *)"index", 5);
    bj_put_string(b, (const uint8_t *)index, (uint32_t)strlen(index));
    if (values) {
        bj_put_key(b, (const uint8_t *)"values", 6);
        bj_put_raw(b, values, vlen);
    }
    bj_end_object(b);
    size_t len = 0;
    *out = bj_builder_data(b, &len);
    *out_len = (uint32_t)len;
    return b;
}

/* One {stream, event} frame, or none. The event's operationType goes in
 * `type`; *has_doc says whether it carried a fullDocument. */
static int take_event(dbs *s, uint64_t client, char *type, size_t cap,
                      int *has_doc, int *overflow, int64_t *stream_id) {
    dbuf frame = {0};
    int have = 0;
    type[0] = '\0'; *has_doc = 0; *overflow = 0; *stream_id = 0;
    if (dbs_stream_take(s, client, &frame, &have) != BJ_OK) { dbuf_free(&frame); return -1; }
    if (!have) { dbuf_free(&frame); return 0; }

    int f = 0;
    *stream_id = response_num(&frame, "stream", &f);
    const uint8_t *v; size_t vlen; int found = 0;
    if (!obj_get_field(frame.data, frame.len, (const uint8_t *)"overflow", 8,
                       &v, &vlen, &found) && found) {
        cur c = { v, vlen, 0 };
        int flag = 0;
        read_bool(&c, &flag);
        *overflow = flag;
        dbuf_free(&frame);
        return 1;
    }
    found = 0;
    if (obj_get_field(frame.data, frame.len, (const uint8_t *)"event", 5,
                      &v, &vlen, &found) || !found) { dbuf_free(&frame); return -1; }
    doc_get_str(v, vlen, "operationType", type, cap);
    int hd = 0;
    const uint8_t *dv; size_t dvlen;
    if (!obj_get_field(v, vlen, (const uint8_t *)"fullDocument", 12, &dv, &dvlen, &hd))
        *has_doc = hd;
    dbuf_free(&frame);
    return 1;
}

/* A sweep's verdict for one collection: 1 compacted, 0 skipped (null),
 * -1 not mentioned at all. */
static int sweep_verdict(const dbuf *res, const char *name) {
    const uint8_t *r; size_t rlen; int f = 0;
    if (obj_get_field(res->data, res->len, (const uint8_t *)"result", 6, &r, &rlen, &f) || !f)
        return -1;
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(r, rlen, (const uint8_t *)name, (uint32_t)strlen(name),
                      &v, &vlen, &found) || !found)
        return -1;
    if (vlen >= 1 && v[0] == BJ_TYPE_NULL) return 0;
    return 1;
}

/* {op:'compact', minBytes?, factor?, skipBusy?} -- no collection named. */
static bj_builder *sweep_request(int64_t min_bytes, double factor, int skip_busy,
                                 const uint8_t **out, uint32_t *out_len) {
    bj_builder *b = bj_builder_new();
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"op", 2);
    bj_put_string(b, (const uint8_t *)"compact", 7);
    if (min_bytes) { bj_put_key(b, (const uint8_t *)"minBytes", 8); bj_put_int(b, min_bytes); }
    if (factor > 0) { bj_put_key(b, (const uint8_t *)"factor", 6); bj_put_float(b, factor); }
    if (skip_busy) { bj_put_key(b, (const uint8_t *)"skipBusy", 8); bj_put_bool(b, 1); }
    bj_end_object(b);
    size_t len = 0;
    *out = bj_builder_data(b, &len);
    *out_len = (uint32_t)len;
    return b;
}

TEST(a_sweep_is_not_a_loop_over_collections) {
    /*
     * compact with no collection named. What makes it worth having in C
     * rather than looping in a client is the three options, two of which
     * read state a client cannot see: `factor` compares a file set
     * against the size the catalog recorded right after its last
     * compaction (compactedBytes, written at the flip), and `skipBusy`
     * asks whether anyone is scanning it.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-sweep", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 1, &s) == BJ_OK);
    const uint64_t CLIENT = 23;

    /* One collection that churns and one that does not. */
    for (int i = 0; i < 60; i++) {
        doc *d = doc_new();
        uint8_t oid[12]; mk_oid(oid, (uint32_t)(i + 1));
        doc_oid(d, "_id", oid);
        doc_int(d, "n", i);
        doc_str(d, "pad", "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("insert", "churn", "doc", db_, dlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        dbuf_free(&res); bj_builder_free(rb); doc_free(d);
    }
    {
        doc *d = doc_new();
        uint8_t oid[12]; mk_oid(oid, 999);
        doc_oid(d, "_id", oid);
        uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("insert", "still", "doc", db_, dlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        dbuf_free(&res); bj_builder_free(rb); doc_free(d);
    }

    /* ---- with no options at all it is unconditional, exactly like
     * asking for each collection in turn. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = sweep_request(0, 0, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(sweep_verdict(&res, "churn"), 1);
        CHECK_I64(sweep_verdict(&res, "still"), 1);
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- minBytes: nothing here is anywhere near a megabyte. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = sweep_request(1000000, 0, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(sweep_verdict(&res, "churn"), 0);
        CHECK_I64(sweep_verdict(&res, "still"), 0);
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- factor: rewrite every document several times, and only the
     * collection that grew past twice its post-compaction size is worth
     * doing again. The other one has not changed at all. */
    {
        for (int round = 0; round < 6; round++) {
            doc *u = doc_new();
            doc_begin_obj(u, "$set"); doc_int(u, "round", round); doc_end_obj(u);
            uint32_t ulen; const uint8_t *ub = doc_done(u, &ulen);
            const uint8_t *req; uint32_t req_len;
            bj_builder *rb = request("updateMany", "churn", "update", ub, ulen, &req, &req_len);
            dbuf res = {0};
            CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
            CHECK_I64(response_ok(&res), 1);
            dbuf_free(&res); bj_builder_free(rb); doc_free(u);
        }
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = sweep_request(0, 2.0, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(sweep_verdict(&res, "churn"), 1);
        CHECK_I64(sweep_verdict(&res, "still"), 0);
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* ---- skipBusy: a collection someone is scanning gets its turn on
     * the next sweep. Without it, the same sweep is refused outright --
     * which is what a caller who named one collection wants to hear. */
    {
        const uint8_t *req; uint32_t req_len;
        const uint8_t *opts; uint32_t opts_len;
        bj_builder *ob = opts_of(5, 0, &opts, &opts_len);
        bj_builder *fb = request("find", "churn", "opts", opts, opts_len, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        int f = 0;
        int64_t cursor_id = response_num(&res, "cursor", &f);
        CHECK_I64(f, 1);
        dbuf_free(&res); bj_builder_free(fb); bj_builder_free(ob);

        bj_builder *rb = sweep_request(0, 0, 1, &req, &req_len);
        dbuf sres = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &sres));
        CHECK_I64(response_ok(&sres), 1);
        CHECK_I64(sweep_verdict(&sres, "churn"), 0);   /* busy: next time */
        CHECK_I64(sweep_verdict(&sres, "still"), 1);
        dbuf_free(&sres); bj_builder_free(rb);

        rb = sweep_request(0, 0, 0, &req, &req_len);
        dbuf bres = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &bres));
        CHECK_I64(response_ok(&bres), 0);
        CHECK_I64(response_num(&bres, "code", &f), DC_ERR_CURSORS_OPEN);
        dbuf_free(&bres); bj_builder_free(rb);

        bj_builder *kb = cursor_request("closeCursor", cursor_id, &req, &req_len);
        dbuf kres = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &kres));
        dbuf_free(&kres); bj_builder_free(kb);
    }

    /* ---- and every document is still there. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("count", "churn", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        int f = 0;
        CHECK_I64(response_num(&res, "n", &f), 60);
        dbuf_free(&res); bj_builder_free(rb);
    }

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(a_watcher_is_told_what_another_client_wrote) {
    /*
     * The one thing on this wire a client does not ask for. Everything
     * else here is a question and its answer; a change event is a frame
     * the server sends because somebody ELSE wrote something.
     *
     * It costs the engine nothing new: a logged command already names
     * the one document it touched, because the planner expanded the
     * many-forms into one command each before any of this ran. So the
     * event is built from the command and its result -- the derivation
     * every other host of this library makes.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-watch", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ns) == 0);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 0, &s) == BJ_OK);
    const uint64_t WATCHER = 1, WRITER = 2;
    char type[32]; int has_doc = 0, overflow = 0; int64_t sid = 0;

    /* ---- watching resolves nothing: a collection that does not exist
     * yet is the case a change stream is most useful for. */
    int64_t stream = 0;
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("watch", "later", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, WATCHER, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        int f = 0;
        stream = response_num(&res, "stream", &f);
        CHECK_I64(f, 1);
        CHECK(stream > 0);
        dbuf_free(&res); bj_builder_free(rb);
        CHECK_I64(dbs_stream_count(s), 1);

        /* The insert that CREATES the collection is an event like any
         * other. */
        doc *d = doc_new();
        uint8_t oid[12]; mk_oid(oid, 40);
        doc_oid(d, "_id", oid);
        doc_str(d, "body", "the first");
        uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
        rb = request("insert", "later", "doc", db_, dlen, &req, &req_len);
        dbuf ires = {0};
        CHECK_OK(dbs_handle(s, WRITER, req, req_len, &ires));
        CHECK_I64(response_ok(&ires), 1);
        dbuf_free(&ires); bj_builder_free(rb); doc_free(d);

        CHECK_I64(take_event(s, WATCHER, type, sizeof type, &has_doc, &overflow, &sid), 1);
        CHECK(strcmp(type, "insert") == 0);
        CHECK_I64(has_doc, 1);
        CHECK_I64(sid, stream);
        /* And exactly one: an event is not delivered twice. */
        CHECK_I64(take_event(s, WATCHER, type, sizeof type, &has_doc, &overflow, &sid), 0);
    }

    /* ---- a write to a collection nobody is watching says nothing. */
    {
        doc *d = doc_new();
        uint8_t oid[12]; mk_oid(oid, 41);
        doc_oid(d, "_id", oid);
        uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("insert", "users", "doc", db_, dlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, WRITER, req, req_len, &res));
        dbuf_free(&res); bj_builder_free(rb); doc_free(d);
        CHECK_I64(take_event(s, WATCHER, type, sizeof type, &has_doc, &overflow, &sid), 0);
    }

    /* ---- update, replace, delete: the kinds, and what each carries. */
    {
        doc *f = doc_new(); doc_str(f, "body", "the first");
        uint32_t flen; const uint8_t *fb = doc_done(f, &flen);
        doc *u = doc_new();
        doc_begin_obj(u, "$set"); doc_str(u, "body", "edited"); doc_end_obj(u);
        uint32_t ulen; const uint8_t *ub = doc_done(u, &ulen);
        const uint8_t *req;
        bj_builder *b = bj_builder_new();
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"op", 2);
        bj_put_string(b, (const uint8_t *)"update", 6);
        bj_put_key(b, (const uint8_t *)"coll", 4);
        bj_put_string(b, (const uint8_t *)"later", 5);
        bj_put_key(b, (const uint8_t *)"filter", 6);
        bj_put_raw(b, fb, flen);
        bj_put_key(b, (const uint8_t *)"update", 6);
        bj_put_raw(b, ub, ulen);
        bj_end_object(b);
        size_t rl = 0; req = bj_builder_data(b, &rl);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, WRITER, req, (uint32_t)rl, &res));
        CHECK_I64(response_ok(&res), 1);
        dbuf_free(&res); bj_builder_free(b); doc_free(f); doc_free(u);

        /* An update names its CHANGES, so the post-image was read back:
         * without that a watcher would learn that something changed and
         * not what it now is. */
        CHECK_I64(take_event(s, WATCHER, type, sizeof type, &has_doc, &overflow, &sid), 1);
        CHECK(strcmp(type, "update") == 0);
        CHECK_I64(has_doc, 1);

        doc *f2 = doc_new(); doc_str(f2, "body", "edited");
        uint32_t f2len; const uint8_t *f2b = doc_done(f2, &f2len);
        bj_builder *db2 = bj_builder_new();
        bj_begin_object(db2);
        bj_put_key(db2, (const uint8_t *)"op", 2);
        bj_put_string(db2, (const uint8_t *)"delete", 6);
        bj_put_key(db2, (const uint8_t *)"coll", 4);
        bj_put_string(db2, (const uint8_t *)"later", 5);
        bj_put_key(db2, (const uint8_t *)"filter", 6);
        bj_put_raw(db2, f2b, f2len);
        bj_end_object(db2);
        size_t dl = 0; const uint8_t *dreq = bj_builder_data(db2, &dl);
        dbuf dres = {0};
        CHECK_OK(dbs_handle(s, WRITER, dreq, (uint32_t)dl, &dres));
        dbuf_free(&dres); bj_builder_free(db2); doc_free(f2);

        CHECK_I64(take_event(s, WATCHER, type, sizeof type, &has_doc, &overflow, &sid), 1);
        CHECK(strcmp(type, "delete") == 0);
        CHECK_I64(has_doc, 0);      /* there is no document to carry */
    }

    /* ---- a delete that matched nothing is not an event. */
    {
        doc *f = doc_new(); doc_str(f, "body", "never existed");
        uint32_t flen; const uint8_t *fb = doc_done(f, &flen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("delete", "later", "filter", fb, flen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, WRITER, req, req_len, &res));
        dbuf_free(&res); bj_builder_free(rb); doc_free(f);
        CHECK_I64(take_event(s, WATCHER, type, sizeof type, &has_doc, &overflow, &sid), 0);
    }

    /* ---- closed, and then silent. */
    {
        bj_builder *b = bj_builder_new();
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"op", 2);
        bj_put_string(b, (const uint8_t *)"closeStream", 11);
        bj_put_key(b, (const uint8_t *)"stream", 6);
        bj_put_int(b, stream);
        bj_end_object(b);
        size_t rl = 0; const uint8_t *req = bj_builder_data(b, &rl);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, WATCHER, req, (uint32_t)rl, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(response_flag(&res, "closed"), 1);
        dbuf_free(&res);
        CHECK_I64(dbs_stream_count(s), 0);

        /* Twice is a refusal, not a second success -- and the same
         * refusal another client's id would get. The request bytes are
         * the builder's, so it outlives both uses. */
        dbuf again = {0};
        CHECK_OK(dbs_handle(s, WATCHER, req, (uint32_t)rl, &again));
        CHECK_I64(response_ok(&again), 0);
        int f = 0;
        CHECK_I64(response_num(&again, "code", &f), DC_ERR_NO_STREAM);
        dbuf_free(&again); bj_builder_free(b);

        doc *d = doc_new();
        uint8_t oid[12]; mk_oid(oid, 42);
        doc_oid(d, "_id", oid);
        uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
        const uint8_t *ireq; uint32_t ilen;
        bj_builder *rb = request("insert", "later", "doc", db_, dlen, &ireq, &ilen);
        dbuf ires = {0};
        CHECK_OK(dbs_handle(s, WRITER, ireq, ilen, &ires));
        dbuf_free(&ires); bj_builder_free(rb); doc_free(d);
        CHECK_I64(take_event(s, WATCHER, type, sizeof type, &has_doc, &overflow, &sid), 0);
    }

    /* ---- a consumer that stops reading loses its stream rather than
     * costing the server unbounded memory. Nobody takes anything here,
     * so the queue fills and the stream says so once and goes. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("watch", "flood", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, WATCHER, req, req_len, &res));
        int f = 0;
        int64_t id = response_num(&res, "stream", &f);
        dbuf_free(&res); bj_builder_free(rb);

        for (int i = 0; i < DBS_STREAM_EVENTS + 20; i++) {
            doc *d = doc_new();
            uint8_t oid[12]; mk_oid(oid, (uint32_t)(1000 + i));
            doc_oid(d, "_id", oid);
            doc_int(d, "n", i);
            uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
            const uint8_t *ireq; uint32_t ilen;
            bj_builder *ib = request("insert", "flood", "doc", db_, dlen, &ireq, &ilen);
            dbuf ires = {0};
            CHECK_OK(dbs_handle(s, WRITER, ireq, ilen, &ires));
            dbuf_free(&ires); bj_builder_free(ib); doc_free(d);
        }

        /* Everything it managed to hold, in order, and then the news. */
        int events = 0;
        for (;;) {
            int r = take_event(s, WATCHER, type, sizeof type, &has_doc, &overflow, &sid);
            CHECK_I64(r, 1);
            if (overflow) break;
            CHECK_I64(sid, id);
            events++;
            if (events > DBS_STREAM_EVENTS + 1) { TAP_FAIL("queue never overflowed%s", ""); break; }
        }
        CHECK_I64(events, DBS_STREAM_EVENTS);
        /* Said once: the stream is gone, not merely quiet. */
        CHECK_I64(dbs_stream_count(s), 0);
        CHECK_I64(take_event(s, WATCHER, type, sizeof type, &has_doc, &overflow, &sid), 0);
    }

    /* ---- the table is bounded, and refuses in the shape everything
     * else here refuses in. */
    {
        for (int i = 0; i < DBS_MAX_STREAMS; i++) {
            const uint8_t *req; uint32_t req_len;
            bj_builder *rb = request("watch", "users", NULL, NULL, 0, &req, &req_len);
            dbuf res = {0};
            CHECK_OK(dbs_handle(s, WATCHER, req, req_len, &res));
            CHECK_I64(response_ok(&res), 1);
            dbuf_free(&res); bj_builder_free(rb);
        }
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("watch", "users", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, WATCHER, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_TOO_MANY_STREAMS);
        dbuf_free(&res); bj_builder_free(rb);

        /* And they all go when their client does, like cursors. */
        dbs_drop_client(s, WATCHER);
        CHECK_I64(dbs_stream_count(s), 0);
    }

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(prune_expired_sweeps_what_a_ttl_index_says_is_over) {
    /*
     * Expiry is not a background thread: the engine runs no timers and
     * reads no clock, so a TTL sweep is something a host asks for, with
     * its own `now`. db_ttl.h predicted this call ("when index metadata
     * moves into the C catalog, this becomes a single call and the loop
     * goes with it") -- the catalog is here now, so the loop is too.
     *
     * The clock being a parameter is also what makes this testable at
     * all: `now` is a fixed number below, so the sweep is reproducible
     * rather than a race against the wall.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-ttl", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 1, &s) == BJ_OK);
    const uint64_t CLIENT = 19;
    const int64_t NOW = 1750000000000LL;
    const int64_t HOUR = 3600000LL;

    /* Four events: two past an hour old, one fresh, one with no date at
     * all -- which a sparse index tolerates and a sweep must not touch. */
    {
        const int64_t ages[] = { 2 * HOUR, HOUR + 60000, 60000 };
        for (int i = 0; i < 3; i++) {
            doc *d = doc_new();
            uint8_t oid[12]; mk_oid(oid, (uint32_t)(i + 1));
            doc_oid(d, "_id", oid);
            doc_int(d, "n", i);
            doc_key(d, "at"); bj_put_date(d->b, NOW - ages[i]);
            uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
            const uint8_t *req; uint32_t req_len;
            bj_builder *rb = request("insert", "events", "doc", db_, dlen, &req, &req_len);
            dbuf res = {0};
            CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
            CHECK_I64(response_ok(&res), 1);
            dbuf_free(&res); bj_builder_free(rb); doc_free(d);
        }
        doc *d = doc_new();
        uint8_t oid[12]; mk_oid(oid, 9);
        doc_oid(d, "_id", oid);
        doc_int(d, "n", 9);
        uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("insert", "events", "doc", db_, dlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        dbuf_free(&res); bj_builder_free(rb); doc_free(d);
    }

    /* No TTL index yet: a sweep is owed nothing, which is an answer of
     * zero rather than a refusal. */
    {
        dbuf filters = {0};
        CHECK_OK(dbs_ttl_filters(s, "events", 6, NOW, &filters));
        CHECK_I64(arr_count(filters.data, filters.len), 0);
        dbuf_free(&filters);
    }

    /* A TTL index over `at`, sparse so the dateless document is legal. */
    {
        doc *k = doc_new(); doc_int(k, "at", 1);
        uint32_t klen; const uint8_t *kb = doc_done(k, &klen);
        doc *o = doc_new();
        doc_int(o, "expireAfterSeconds", 3600);
        doc_key(o, "sparse"); bj_put_bool(o->b, 1);
        uint32_t olen; const uint8_t *ob = doc_done(o, &olen);

        bj_builder *b = bj_builder_new();
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"op", 2);
        bj_put_string(b, (const uint8_t *)"createIndex", 11);
        bj_put_key(b, (const uint8_t *)"coll", 4);
        bj_put_string(b, (const uint8_t *)"events", 6);
        bj_put_key(b, (const uint8_t *)"keys", 4);
        bj_put_raw(b, kb, klen);
        bj_put_key(b, (const uint8_t *)"options", 7);
        bj_put_raw(b, ob, olen);
        bj_end_object(b);
        size_t rl = 0; const uint8_t *req = bj_builder_data(b, &rl);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, (uint32_t)rl, &res));
        if (response_ok(&res) != 1) {
            int f = 0;
            TAP_FAIL("createIndex refused: %lld", (long long)response_num(&res, "code", &f));
        }
        dbuf_free(&res); bj_builder_free(b); doc_free(k); doc_free(o);
    }

    /* One filter now, and it is the one db_ttl.h describes. */
    {
        dbuf filters = {0};
        CHECK_OK(dbs_ttl_filters(s, "events", 6, NOW, &filters));
        CHECK_I64(arr_count(filters.data, filters.len), 1);
        dbuf_free(&filters);
    }

    /* The sweep itself, through the wire, with the caller's clock. */
    {
        bj_builder *b = bj_builder_new();
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"op", 2);
        bj_put_string(b, (const uint8_t *)"pruneExpired", 12);
        bj_put_key(b, (const uint8_t *)"coll", 4);
        bj_put_string(b, (const uint8_t *)"events", 6);
        bj_put_key(b, (const uint8_t *)"now", 3);
        bj_put_int(b, NOW);
        bj_end_object(b);
        size_t rl = 0; const uint8_t *req = bj_builder_data(b, &rl);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, (uint32_t)rl, &res));
        if (response_ok(&res) != 1) {
            int f = 0;
            TAP_FAIL("pruneExpired refused: %lld", (long long)response_num(&res, "code", &f));
        }
        int f = 0;
        CHECK_I64(response_num(&res, "deletedCount", &f), 2);
        dbuf_free(&res); bj_builder_free(b);
    }

    /* The fresh document and the dateless one are still there, and a
     * second sweep at the same instant finds nothing left to do. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("count", "events", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        int f = 0;
        CHECK_I64(response_num(&res, "n", &f), 2);
        dbuf_free(&res); bj_builder_free(rb);

        bj_builder *b = bj_builder_new();
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"op", 2);
        bj_put_string(b, (const uint8_t *)"pruneExpired", 12);
        bj_put_key(b, (const uint8_t *)"coll", 4);
        bj_put_string(b, (const uint8_t *)"events", 6);
        bj_put_key(b, (const uint8_t *)"now", 3);
        bj_put_int(b, NOW);
        bj_end_object(b);
        size_t rl = 0; const uint8_t *r2 = bj_builder_data(b, &rl);
        dbuf again = {0};
        CHECK_OK(dbs_handle(s, CLIENT, r2, (uint32_t)rl, &again));
        CHECK_I64(response_num(&again, "deletedCount", &f), 0);
        dbuf_free(&again); bj_builder_free(b);
    }

    /* A sweep with no clock reading is refused rather than dated from
     * thin air -- the same rule $currentDate answers to. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("pruneExpired", "events", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_REQ_MISSING_FIELD);
        dbuf_free(&res); bj_builder_free(rb);
    }

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(find_by_index_says_which_of_the_three_ways_it_was_asked_wrong) {
    /*
     * findByIndex names its index instead of describing what it wants,
     * so there are three separate ways to get it wrong -- no such index,
     * the wrong kind of index, the wrong number of values -- and they
     * were ONE BJ_ERR_STATE between them until this went on a wire. "-2,
     * builder state error" is not an answer a client can act on, and the
     * JavaScript host only avoided it by checking two of the three
     * itself, against its own copy of the index list.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-fbi", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ns) == 0);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 0, &s) == BJ_OK);
    const uint64_t CLIENT = 17;

    /* A text index, so there is a wrong KIND to name. */
    {
        doc *k = doc_new(); doc_str(k, "name", "text");
        uint32_t klen; const uint8_t *kb = doc_done(k, &klen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("createIndex", "users", "keys", kb, klen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        dbuf_free(&res); bj_builder_free(rb); doc_free(k);
    }

    /* The lookup itself: build_users_db's team_1 index, two documents. */
    {
        bj_builder *vb = bj_builder_new();
        bj_begin_array(vb);
        bj_put_string(vb, (const uint8_t *)"core", 4);
        bj_end_array(vb);
        size_t vlen = 0; const uint8_t *vdata = bj_builder_data(vb, &vlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = index_request("findByIndex", "users", "team_1",
                                       vdata, (uint32_t)vlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(response_docs(&res), 2);      /* Ada and Grace */
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(vb);
    }

    /* A value nothing has is an empty answer, not a refusal. */
    {
        bj_builder *vb = bj_builder_new();
        bj_begin_array(vb);
        bj_put_string(vb, (const uint8_t *)"nobody", 6);
        bj_end_array(vb);
        size_t vlen = 0; const uint8_t *vdata = bj_builder_data(vb, &vlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = index_request("findByIndex", "users", "team_1",
                                       vdata, (uint32_t)vlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(response_docs(&res), 0);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(vb);
    }

    /* The three refusals, each with its own code. */
    {
        bj_builder *one = bj_builder_new();
        bj_begin_array(one);
        bj_put_string(one, (const uint8_t *)"core", 4);
        bj_end_array(one);
        size_t olen = 0; const uint8_t *odata = bj_builder_data(one, &olen);

        bj_builder *two = bj_builder_new();
        bj_begin_array(two);
        bj_put_string(two, (const uint8_t *)"core", 4);
        bj_put_string(two, (const uint8_t *)"extra", 5);
        bj_end_array(two);
        size_t tlen = 0; const uint8_t *tdata = bj_builder_data(two, &tlen);

        struct { const char *index; const uint8_t *vals; uint32_t vlen; int want; } cases[] = {
            { "no_such_1", odata, (uint32_t)olen, DC_ERR_NO_INDEX    },
            { "name_text", odata, (uint32_t)olen, DC_ERR_INDEX_KIND  },
            { "team_1",    tdata, (uint32_t)tlen, DC_ERR_INDEX_ARITY },
        };
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            const uint8_t *req; uint32_t req_len;
            bj_builder *rb = index_request("findByIndex", "users", cases[i].index,
                                           cases[i].vals, cases[i].vlen, &req, &req_len);
            dbuf res = {0};
            CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
            CHECK_I64(response_ok(&res), 0);
            int f = 0;
            int64_t code = response_num(&res, "code", &f);
            if (code != cases[i].want)
                TAP_FAIL("case %zu (%s): code %lld, want %d",
                         i, cases[i].index, (long long)code, cases[i].want);
            dbuf_free(&res); bj_builder_free(rb);
        }
        bj_builder_free(one); bj_builder_free(two);
    }

    /* And no `values` at all is the request being incomplete, which is a
     * different thing again from any of the three. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = index_request("findByIndex", "users", "team_1", NULL, 0,
                                       &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_REQ_MISSING_FIELD);
        dbuf_free(&res); bj_builder_free(rb);
    }

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(find_one_and_modify_answers_with_the_document_not_a_count) {
    /*
     * The family exists because updateOne says how MANY documents
     * changed, not WHICH, so reading the document back otherwise means a
     * second query with a gap in the middle of it.
     *
     * Neither image costs a query here. The BEFORE image is the one the
     * planner already read to resolve its target (dc_wal_plan_preimage,
     * which names these three methods in its comment); the AFTER image
     * is a read back by the id the plan resolved, which is a bpt_search.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-foam", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ns) == 0);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 0, &s) == BJ_OK);
    const uint64_t CLIENT = 13;
    char got[64];

    /* ---- before: the image the planner already had. */
    {
        doc *f = doc_new(); doc_str(f, "name", "Ada");
        uint32_t flen; const uint8_t *fb = doc_done(f, &flen);
        doc *u = doc_new();
        doc_begin_obj(u, "$set"); doc_str(u, "team", "kernel"); doc_end_obj(u);
        uint32_t ulen; const uint8_t *ub = doc_done(u, &ulen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = modify_request("findOneAndUpdate", "users", fb, flen,
                                        "update", ub, ulen, 0, NULL, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(response_flag(&res, "found"), 1);
        CHECK_I64(response_doc_str(&res, "team", got, sizeof got), 1);
        CHECK(strcmp(got, "core") == 0);        /* as it was */
        dbuf_free(&res); bj_builder_free(rb); doc_free(f); doc_free(u);
    }

    /* ---- after: read back by the id the plan resolved, so it carries
     * the write this same request performed. */
    {
        doc *f = doc_new(); doc_str(f, "name", "Ada");
        uint32_t flen; const uint8_t *fb = doc_done(f, &flen);
        doc *u = doc_new();
        doc_begin_obj(u, "$set"); doc_str(u, "team", "ops"); doc_end_obj(u);
        uint32_t ulen; const uint8_t *ub = doc_done(u, &ulen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = modify_request("findOneAndUpdate", "users", fb, flen,
                                        "update", ub, ulen, 1, NULL, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_flag(&res, "found"), 1);
        CHECK_I64(response_doc_str(&res, "team", got, sizeof got), 1);
        CHECK(strcmp(got, "ops") == 0);
        dbuf_free(&res); bj_builder_free(rb); doc_free(f); doc_free(u);
    }

    /* ---- nothing matched: found:false, and nothing written. */
    {
        doc *f = doc_new(); doc_str(f, "name", "Nobody");
        uint32_t flen; const uint8_t *fb = doc_done(f, &flen);
        doc *u = doc_new();
        doc_begin_obj(u, "$set"); doc_str(u, "team", "ghost"); doc_end_obj(u);
        uint32_t ulen; const uint8_t *ub = doc_done(u, &ulen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = modify_request("findOneAndUpdate", "users", fb, flen,
                                        "update", ub, ulen, 0, NULL, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(response_flag(&res, "found"), 0);
        dbuf_free(&res); bj_builder_free(rb); doc_free(f); doc_free(u);

        rb = request("count", "users", NULL, NULL, 0, &req, &req_len);
        dbuf cres = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &cres));
        int nf = 0;
        CHECK_I64(response_num(&cres, "n", &nf), 3);
        dbuf_free(&cres); bj_builder_free(rb);
    }

    /* ---- an upsert asked for `before` answers null: there is no prior
     * state to show. The document is still made. */
    {
        uint8_t oid[12]; mk_oid(oid, 77);
        doc *f = doc_new(); doc_str(f, "name", "Barbara");
        uint32_t flen; const uint8_t *fb = doc_done(f, &flen);
        doc *u = doc_new();
        doc_begin_obj(u, "$set"); doc_str(u, "team", "core"); doc_end_obj(u);
        uint32_t ulen; const uint8_t *ub = doc_done(u, &ulen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = modify_request("findOneAndUpdate", "users", fb, flen,
                                        "update", ub, ulen, 0, oid, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(response_flag(&res, "found"), 0);
        dbuf_free(&res); bj_builder_free(rb); doc_free(u);

        bj_builder *cb = request("count", "users", "filter", fb, flen, &req, &req_len);
        dbuf cres = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &cres));
        int nf = 0;
        CHECK_I64(response_num(&cres, "n", &nf), 1);
        dbuf_free(&cres); bj_builder_free(cb); doc_free(f);
    }

    /* ---- replace, and delete: the deleted document comes back, which
     * is the only image a delete has. */
    {
        doc *f = doc_new(); doc_str(f, "name", "Grace");
        uint32_t flen; const uint8_t *fb = doc_done(f, &flen);
        doc *r = doc_new(); doc_str(r, "name", "Grace"); doc_str(r, "team", "compilers");
        uint32_t rlen; const uint8_t *rb_ = doc_done(r, &rlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = modify_request("findOneAndReplace", "users", fb, flen,
                                        "doc", rb_, rlen, 1, NULL, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_flag(&res, "found"), 1);
        CHECK_I64(response_doc_str(&res, "team", got, sizeof got), 1);
        CHECK(strcmp(got, "compilers") == 0);
        dbuf_free(&res); bj_builder_free(rb); doc_free(r); doc_free(f);
    }
    {
        doc *f = doc_new(); doc_str(f, "name", "Alan");
        uint32_t flen; const uint8_t *fb = doc_done(f, &flen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = modify_request("findOneAndDelete", "users", fb, flen,
                                        NULL, NULL, 0, 0, NULL, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_flag(&res, "found"), 1);
        CHECK_I64(response_doc_str(&res, "name", got, sizeof got), 1);
        CHECK(strcmp(got, "Alan") == 0);
        dbuf_free(&res); bj_builder_free(rb);

        /* Gone, and asking again says so rather than repeating itself. */
        rb = modify_request("findOneAndDelete", "users", fb, flen,
                            NULL, NULL, 0, 0, NULL, &req, &req_len);
        dbuf again = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &again));
        CHECK_I64(response_ok(&again), 1);
        CHECK_I64(response_flag(&again, "found"), 0);
        dbuf_free(&again); bj_builder_free(rb); doc_free(f);
    }

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(explain_names_the_plan_the_same_way_for_every_host) {
    /*
     * dc_explain consults the very planners the queries consult, so its
     * report cannot drift from what a query would actually do. What
     * could drift is the NAME: "equality" was an array in
     * wasm/nisaba-wasm.js until a second host needed it, and two hosts
     * spelling one plan differently is a fact with two owners. It lives
     * in C now (dc_explain_source), which is what this checks -- through
     * the wire, on a database with a real index on it.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-explain", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ns) == 0);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 0, &s) == BJ_OK);
    const uint64_t CLIENT = 11;

    struct { const char *field; int by_id; const char *want; const char *index; } cases[] = {
        { "team", 0, "equality", "team_1" },   /* the index build_users_db made */
        { "age",  0, "scan",     NULL      },  /* no index on it */
        { NULL,   1, "ids",      NULL      },  /* {_id: <oid>} point lookup */
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        doc *q = doc_new();
        if (cases[i].by_id) {
            uint8_t oid[12]; mk_oid(oid, 1);
            doc_oid(q, "_id", oid);
        } else {
            doc_str(q, cases[i].field, "core");
        }
        uint32_t qlen; const uint8_t *qb = doc_done(q, &qlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("explain", "users", "filter", qb, qlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);

        const uint8_t *plan; size_t plan_len; int f = 0;
        CHECK_OK(obj_get_field(res.data, res.len, (const uint8_t *)"plan", 4,
                               &plan, &plan_len, &f));
        CHECK_I64(f, 1);
        char source[32] = "";
        CHECK_I64(doc_get_str(plan, plan_len, "source", source, sizeof source), 1);
        if (strcmp(source, cases[i].want) != 0)
            TAP_FAIL("case %zu: source '%s', want '%s'", i, source, cases[i].want);
        char name[64] = "";
        int had = doc_get_str(plan, plan_len, "index", name, sizeof name);
        if (cases[i].index) {
            CHECK_I64(had, 1);
            if (had) CHECK(strcmp(name, cases[i].index) == 0);
        } else {
            CHECK_I64(had, 0);   /* null, not a name */
        }
        dbuf_free(&res); bj_builder_free(rb); doc_free(q);
    }

    /* Nothing was executed to find that out: explain is the one read
     * that answers without touching a document. */
    CHECK_I64(dbs_cursor_count(s), 0);

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(an_aggregate_pipeline_runs_whole_in_one_request) {
    /*
     * The pipeline was already C's (db_agg.h) -- including the decision
     * to push a leading $match into the scan so an index can serve it --
     * so this op is marshalling and nothing else. What it must NOT do is
     * grow a second opinion about stage names: the grammar is one list,
     * in one place, and the only thing that crosses this seam is which
     * stage went wrong.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-agg", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ns) == 0);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 0, &s) == BJ_OK);
    const uint64_t CLIENT = 7;

    /* $group with $sum, then $sort: Ada 36 and Grace 45 are core, Alan 41
     * is research, so the order proves the sort ran over the groups. */
    {
        bj_builder *ab = bj_builder_new();
        bj_begin_array(ab);
        bj_begin_object(ab);
        bj_put_key(ab, (const uint8_t *)"$group", 6);
        bj_begin_object(ab);
        bj_put_key(ab, (const uint8_t *)"_id", 3);
        bj_put_string(ab, (const uint8_t *)"$team", 5);
        bj_put_key(ab, (const uint8_t *)"total", 5);
        bj_begin_object(ab);
        bj_put_key(ab, (const uint8_t *)"$sum", 4);
        bj_put_string(ab, (const uint8_t *)"$age", 4);
        bj_end_object(ab);
        bj_end_object(ab);
        bj_end_object(ab);
        bj_begin_object(ab);
        bj_put_key(ab, (const uint8_t *)"$sort", 5);
        bj_begin_object(ab);
        bj_put_key(ab, (const uint8_t *)"total", 5);
        bj_put_int(ab, -1);
        bj_end_object(ab);
        bj_end_object(ab);
        bj_end_array(ab);
        size_t alen = 0; const uint8_t *adata = bj_builder_data(ab, &alen);

        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("aggregate", "users", "stages", adata, alen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(response_docs(&res), 2);
        CHECK_I64(list_num(&res, "docs", 0, "total"), 81);
        CHECK_I64(list_num(&res, "docs", 1, "total"), 41);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ab);
    }

    /* A stage this subset does not have is refused with its POSITION,
     * the same way a malformed bulkWrite names its operation -- and the
     * stage's CONTENTS stay with the client that sent them, because C
     * does not format messages around user data. */
    {
        bj_builder *ab = bj_builder_new();
        bj_begin_array(ab);
        bj_begin_object(ab);
        bj_put_key(ab, (const uint8_t *)"$match", 6);
        bj_begin_object(ab); bj_end_object(ab);
        bj_end_object(ab);
        bj_begin_object(ab);
        bj_put_key(ab, (const uint8_t *)"$obliterate", 11);
        bj_begin_object(ab); bj_end_object(ab);
        bj_end_object(ab);
        bj_end_array(ab);
        size_t alen = 0; const uint8_t *adata = bj_builder_data(ab, &alen);

        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("aggregate", "users", "stages", adata, alen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_AGG_UNKNOWN_STAGE);
        CHECK_I64(response_num(&res, "index", &f), 1);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ab);
    }

    /* No pipeline at all is a missing field, not an empty one. */
    {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("aggregate", "users", NULL, NULL, 0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_REQ_MISSING_FIELD);
        dbuf_free(&res); bj_builder_free(rb);
    }

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(current_date_is_resolved_with_the_callers_clock_or_refused) {
    /*
     * $currentDate is not an operator the engine knows: upd_apply's table
     * has no entry for it, deliberately, because by the time an update
     * reaches the engine it is supposed to have been rewritten into $set
     * against a concrete clock reading -- so that what gets written down
     * is a date rather than a rule that would read a different clock on
     * replay (db_wal.h).
     *
     * Every host of this library calls upd_resolve_current_date before
     * proposing. This one is a host too; what it does not have is a
     * clock, for the same reason it does not mint an _id. So the
     * milliseconds come with the request, and an update that needs them
     * and was not given them is refused rather than dated from thin air.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-cdate", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ns) == 0);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 0, &s) == BJ_OK);
    const uint64_t CLIENT = 3;
    const int64_t NOW = 1750000000123LL;   /* a clock reading, not a clock */

    /* An update carrying $currentDate and no `now` is refused, and
     * nothing is written. */
    {
        doc *u = doc_new();
        doc_begin_obj(u, "$currentDate"); doc_key(u, "at"); bj_put_bool(u->b, 1); doc_end_obj(u);
        uint32_t ulen; const uint8_t *ub = doc_done(u, &ulen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("updateMany", "users", "update", ub, ulen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_REQ_MISSING_FIELD);
        dbuf_free(&res); bj_builder_free(rb); doc_free(u);
    }

    /* With one, the rewrite is upd_resolve_current_date's and the date is
     * exactly the millisecond that was sent -- which is what a filter on
     * that value proves, and a filter on any other value would not. */
    {
        doc *u = doc_new();
        doc_begin_obj(u, "$currentDate"); doc_key(u, "at"); bj_put_bool(u->b, 1); doc_end_obj(u);
        uint32_t ulen; const uint8_t *ub = doc_done(u, &ulen);

        bj_builder *b = bj_builder_new();
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"op", 2);
        bj_put_string(b, (const uint8_t *)"updateMany", 10);
        bj_put_key(b, (const uint8_t *)"coll", 4);
        bj_put_string(b, (const uint8_t *)"users", 5);
        bj_put_key(b, (const uint8_t *)"update", 6);
        bj_put_raw(b, ub, ulen);
        bj_put_key(b, (const uint8_t *)"now", 3);
        bj_put_int(b, NOW);
        bj_end_object(b);
        size_t rl = 0; const uint8_t *req = bj_builder_data(b, &rl);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, (uint32_t)rl, &res));
        CHECK_I64(response_ok(&res), 1);
        dbuf_free(&res); bj_builder_free(b); doc_free(u);

        doc *q = doc_new();
        doc_key(q, "at"); bj_put_date(q->b, NOW);
        uint32_t qlen; const uint8_t *qb = doc_done(q, &qlen);
        const uint8_t *creq; uint32_t creq_len;
        bj_builder *cb = request("count", "users", "filter", qb, qlen, &creq, &creq_len);
        dbuf cres = {0};
        CHECK_OK(dbs_handle(s, CLIENT, creq, creq_len, &cres));
        int f = 0;
        CHECK_I64(response_num(&cres, "n", &f), 3);
        dbuf_free(&cres); bj_builder_free(cb); doc_free(q);
    }

    /* And the collision rule stays where it was written: a field cannot
     * be both $set and dated. Nothing here restates it. */
    {
        doc *u = doc_new();
        doc_begin_obj(u, "$set"); doc_int(u, "at", 1); doc_end_obj(u);
        doc_begin_obj(u, "$currentDate"); doc_key(u, "at"); bj_put_bool(u->b, 1); doc_end_obj(u);
        uint32_t ulen; const uint8_t *ub = doc_done(u, &ulen);

        bj_builder *b = bj_builder_new();
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"op", 2);
        bj_put_string(b, (const uint8_t *)"updateMany", 10);
        bj_put_key(b, (const uint8_t *)"coll", 4);
        bj_put_string(b, (const uint8_t *)"users", 5);
        bj_put_key(b, (const uint8_t *)"update", 6);
        bj_put_raw(b, ub, ulen);
        bj_put_key(b, (const uint8_t *)"now", 3);
        bj_put_int(b, NOW);
        bj_end_object(b);
        size_t rl = 0; const uint8_t *req = bj_builder_data(b, &rl);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, (uint32_t)rl, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_CURRENT_DATE_CONFLICT);
        dbuf_free(&res); bj_builder_free(b); doc_free(u);
    }

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(a_list_of_writes_is_one_request_and_reports_every_member) {
    /*
     * insertMany and bulkWrite are not the same operation -- one list
     * holds documents and the other holds writes of six different kinds
     * -- but they fail the same way, so they answer in the same shape.
     *
     * A FAILED MEMBER IS A RESULT, NOT A REFUSAL, which is what makes
     * `ordered` mean anything. And `attempted` is the one fact a client
     * cannot derive for itself: with ordered:true the run stops at the
     * first failure, and "never tried" is a different answer from "tried
     * and succeeded".
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-many", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 1, &s) == BJ_OK);
    const uint64_t CLIENT = 9;

    /* ---- three documents, one frame, into a collection nobody made:
     * a list of inserts is still an insert. `ordered` is absent here, so
     * this also pins its default -- true, as it is in the driver. */
    {
        bj_builder *ab = bj_builder_new();
        bj_begin_array(ab);
        for (int i = 0; i < 3; i++) {
            doc *d = doc_new();
            uint8_t oid[12]; mk_oid(oid, (uint32_t)(i + 1));
            doc_oid(d, "_id", oid);
            doc_int(d, "i", i);
            uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
            bj_put_raw(ab, db_, dlen);
            doc_free(d);
        }
        bj_end_array(ab);
        size_t alen = 0; const uint8_t *adata = bj_builder_data(ab, &alen);

        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("insertMany", "people", "docs", adata, alen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(result_num(&res, "insertedCount"), 3);
        int f = 0;
        CHECK_I64(response_num(&res, "attempted", &f), 3);
        CHECK_I64(list_count(&res, "errors"), 0);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ab);
    }

    /* ---- a duplicate in the middle, twice: ordered stops at it,
     * unordered attempts what comes after. The difference is visible in
     * `attempted` and nowhere else. */
    for (int ordered = 1; ordered >= 0; ordered--) {
        bj_builder *ab = bj_builder_new();
        bj_begin_array(ab);
        for (int i = 0; i < 3; i++) {
            doc *d = doc_new();
            uint8_t oid[12];
            /* index 1 collides with what the first insertMany wrote */
            mk_oid(oid, i == 1 ? 1u : (uint32_t)(100 + ordered * 10 + i));
            doc_oid(d, "_id", oid);
            doc_int(d, "i", i);
            uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
            bj_put_raw(ab, db_, dlen);
            doc_free(d);
        }
        bj_end_array(ab);
        size_t alen = 0; const uint8_t *adata = bj_builder_data(ab, &alen);

        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = list_request("insertMany", "people", "docs", adata, alen,
                                      ordered, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        int f = 0;
        CHECK_I64(response_num(&res, "attempted", &f), ordered ? 2 : 3);
        CHECK_I64(result_num(&res, "insertedCount"), ordered ? 1 : 2);
        CHECK_I64(list_count(&res, "errors"), 1);
        CHECK_I64(list_num(&res, "errors", 0, "index"), 1);
        CHECK_I64(list_num(&res, "errors", 0, "code"), DC_ERR_DUPLICATE);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ab);
    }

    /* ---- a document with no _id is the CLIENT's mistake, so it is a
     * refusal rather than one entry in `errors` -- and it is caught
     * before any of the list runs, which an unordered run depends on. */
    {
        bj_builder *ab = bj_builder_new();
        bj_begin_array(ab);
        {
            doc *d = doc_new();
            uint8_t oid[12]; mk_oid(oid, 900);
            doc_oid(d, "_id", oid);
            uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
            bj_put_raw(ab, db_, dlen);
            doc_free(d);
            d = doc_new();
            doc_str(d, "name", "no id here");
            db_ = doc_done(d, &dlen);
            bj_put_raw(ab, db_, dlen);
            doc_free(d);
        }
        bj_end_array(ab);
        size_t alen = 0; const uint8_t *adata = bj_builder_data(ab, &alen);

        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = list_request("insertMany", "people", "docs", adata, alen,
                                      0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_REQ_MISSING_FIELD);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ab);

        /* Nothing ran: the first document of that list is not there. */
        doc *q = doc_new();
        uint8_t oid[12]; mk_oid(oid, 900);
        doc_oid(q, "_id", oid);
        uint32_t qlen; const uint8_t *qb = doc_done(q, &qlen);
        bj_builder *cb = request("count", "people", "filter", qb, qlen, &req, &req_len);
        dbuf cres = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &cres));
        CHECK_I64(response_num(&cres, "n", &f), 0);
        dbuf_free(&cres); bj_builder_free(cb); doc_free(q);
    }

    /* ---- bulkWrite: six kinds of write in one list, and the totals are
     * the sum of what each did. */
    {
        uint8_t new_id[12]; mk_oid(new_id, 500);
        uint8_t ups_id[12]; mk_oid(ups_id, 501);

        doc *ins = doc_new();
        doc_oid(ins, "_id", new_id);
        doc_int(ins, "i", 50);
        uint32_t ins_len; const uint8_t *ins_b = doc_done(ins, &ins_len);

        doc *f0 = doc_new(); doc_int(f0, "i", 0);
        uint32_t f0_len; const uint8_t *f0_b = doc_done(f0, &f0_len);
        doc *u0 = doc_new();
        doc_begin_obj(u0, "$set"); doc_str(u0, "tag", "touched"); doc_end_obj(u0);
        uint32_t u0_len; const uint8_t *u0_b = doc_done(u0, &u0_len);

        doc *f2 = doc_new(); doc_int(f2, "i", 2);
        uint32_t f2_len; const uint8_t *f2_b = doc_done(f2, &f2_len);

        doc *fx = doc_new(); doc_int(fx, "i", 4242);
        uint32_t fx_len; const uint8_t *fx_b = doc_done(fx, &fx_len);
        doc *ux = doc_new();
        doc_begin_obj(ux, "$set"); doc_str(ux, "tag", "made"); doc_end_obj(ux);
        uint32_t ux_len; const uint8_t *ux_b = doc_done(ux, &ux_len);

        bj_builder *ab = bj_builder_new();
        bj_begin_array(ab);
        put_bulk_op(ab, "insertOne", "document", ins_b, ins_len, NULL, NULL, 0, NULL);
        put_bulk_op(ab, "updateOne", "filter", f0_b, f0_len, "update", u0_b, u0_len, NULL);
        put_bulk_op(ab, "deleteOne", "filter", f2_b, f2_len, NULL, NULL, 0, NULL);
        put_bulk_op(ab, "updateOne", "filter", fx_b, fx_len, "update", ux_b, ux_len, ups_id);
        bj_end_array(ab);
        size_t alen = 0; const uint8_t *adata = bj_builder_data(ab, &alen);

        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = list_request("bulkWrite", "people", "writes", adata, alen,
                                      1, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        int f = 0;
        CHECK_I64(response_num(&res, "attempted", &f), 4);
        CHECK_I64(list_count(&res, "errors"), 0);
        CHECK_I64(result_num(&res, "insertedCount"), 1);
        CHECK_I64(result_num(&res, "matchedCount"), 1);
        CHECK_I64(result_num(&res, "modifiedCount"), 1);
        CHECK_I64(result_num(&res, "deletedCount"), 1);
        /* An upsert is counted ONCE, as an upsert -- it is applied as an
         * insert, and only the plan still knows which it was. */
        CHECK_I64(result_num(&res, "upsertedCount"), 1);
        CHECK_I64(list_count(&res, "upserted"), 1);
        CHECK_I64(list_num(&res, "upserted", 0, "index"), 3);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ab);
        doc_free(ins); doc_free(f0); doc_free(u0); doc_free(f2);
        doc_free(fx); doc_free(ux);
    }

    /* ---- an operation nobody has heard of is refused with the POSITION
     * of the one that was wrong, and nothing in the list runs -- the
     * whole point of validating the grammar up front (db_bulk.h). */
    {
        uint8_t id[12]; mk_oid(id, 600);
        doc *ok = doc_new();
        doc_oid(ok, "_id", id);
        uint32_t ok_len; const uint8_t *ok_b = doc_done(ok, &ok_len);
        doc *filter = doc_new(); doc_int(filter, "i", 0);
        uint32_t fl_len; const uint8_t *fl_b = doc_done(filter, &fl_len);

        bj_builder *ab = bj_builder_new();
        bj_begin_array(ab);
        put_bulk_op(ab, "insertOne", "document", ok_b, ok_len, NULL, NULL, 0, NULL);
        put_bulk_op(ab, "obliterateOne", "filter", fl_b, fl_len, NULL, NULL, 0, NULL);
        bj_end_array(ab);
        size_t alen = 0; const uint8_t *adata = bj_builder_data(ab, &alen);

        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = list_request("bulkWrite", "people", "writes", adata, alen,
                                      0, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_BULK_UNKNOWN_OP);
        CHECK_I64(response_num(&res, "index", &f), 1);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ab);

        /* The insertOne at index 0 did not run. */
        doc *q = doc_new();
        doc_oid(q, "_id", id);
        uint32_t qlen; const uint8_t *qb = doc_done(q, &qlen);
        bj_builder *cb = request("count", "people", "filter", qb, qlen, &req, &req_len);
        dbuf cres = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &cres));
        CHECK_I64(response_num(&cres, "n", &f), 0);
        dbuf_free(&cres); bj_builder_free(cb); doc_free(q);
        doc_free(ok); doc_free(filter);
    }

    /* ---- a list with no insert in it does not make a collection: a
     * bulkWrite of deletes against a name that is not there is a typo,
     * exactly as a find of one is. */
    {
        doc *filter = doc_new(); doc_int(filter, "i", 0);
        uint32_t fl_len; const uint8_t *fl_b = doc_done(filter, &fl_len);
        bj_builder *ab = bj_builder_new();
        bj_begin_array(ab);
        put_bulk_op(ab, "deleteMany", "filter", fl_b, fl_len, NULL, NULL, 0, NULL);
        bj_end_array(ab);
        size_t alen = 0; const uint8_t *adata = bj_builder_data(ab, &alen);

        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = list_request("bulkWrite", "ghosts", "writes", adata, alen,
                                      1, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_NO_COLLECTION);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ab); doc_free(filter);
    }

    /* ---- but a list that DOES insert makes one, exactly as a single
     * insert does. */
    {
        uint8_t id[12]; mk_oid(id, 700);
        doc *d = doc_new();
        doc_oid(d, "_id", id);
        doc_int(d, "i", 700);
        uint32_t dlen; const uint8_t *db_ = doc_done(d, &dlen);
        bj_builder *ab = bj_builder_new();
        bj_begin_array(ab);
        put_bulk_op(ab, "insertOne", "document", db_, dlen, NULL, NULL, 0, NULL);
        bj_end_array(ab);
        size_t alen = 0; const uint8_t *adata = bj_builder_data(ab, &alen);

        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = list_request("bulkWrite", "arrivals", "writes", adata, alen,
                                      1, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &res));
        CHECK_I64(response_ok(&res), 1);
        CHECK_I64(result_num(&res, "insertedCount"), 1);
        dbuf_free(&res); bj_builder_free(rb); bj_builder_free(ab); doc_free(d);

        bj_builder *cb = request("count", "arrivals", NULL, NULL, 0, &req, &req_len);
        dbuf cres = {0};
        CHECK_OK(dbs_handle(s, CLIENT, req, req_len, &cres));
        int f = 0;
        CHECK_I64(response_num(&cres, "n", &f), 1);
        dbuf_free(&cres); bj_builder_free(cb);
    }

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(every_way_a_request_can_be_wrong_is_answered_not_thrown) {
    /*
     * A refusal is a RESPONSE. The client asked a question; it is owed a
     * sentence, not a dropped connection -- and dbs_handle returning
     * BJ_OK for all of these is what lets a transport stay a transport.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-req-bad", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    CHECK_FATAL(build_users_db(&ns) == 0);

    dbs *s = NULL;
    CHECK_FATAL(dbs_open(&ns, ORDER, 0, &s) == BJ_OK);

    struct { const char *op; const char *coll; const char *key; int want; } cases[] = {
        { "explodinate", "users", NULL,  DC_ERR_REQ_UNKNOWN_OP    },
        { "count",       NULL,    NULL,  DC_ERR_REQ_MISSING_FIELD },
        { "count",       "nope",  NULL,  DC_ERR_NO_COLLECTION     },
        { "insert",      "users", NULL,  DC_ERR_REQ_MISSING_FIELD },  /* no doc */
        { "update",      "users", NULL,  DC_ERR_REQ_MISSING_FIELD },  /* no update */
        { "distinct",    "users", NULL,  DC_ERR_REQ_MISSING_FIELD },  /* no field */
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request(cases[i].op, cases[i].coll, cases[i].key, NULL, 0,
                                 &req, &req_len);
        dbuf res = {0};
        /* BJ_OK: the call succeeded, the request did not. */
        CHECK_OK(dbs_handle(s, 0, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        int64_t code = response_num(&res, "code", &f);
        CHECK_I64(f, 1);
        if (code != cases[i].want)
            TAP_FAIL("case %zu (%s): code %lld, want %d",
                     i, cases[i].op, (long long)code, cases[i].want);
        /* And the sentence is dc_strerror's, not a second wording. */
        const uint8_t *m; size_t mlen; int mf = 0;
        CHECK_OK(obj_get_field(res.data, res.len, (const uint8_t *)"msg", 3, &m, &mlen, &mf));
        CHECK_I64(mf, 1);
        if (mf) {
            cur c = { m, mlen, 0 };
            const uint8_t *str; uint32_t slen;
            CHECK_OK(take_string(&c, &str, &slen));
            const char *want = dc_strerror((int)code);
            CHECK_I64((int64_t)slen, (int64_t)strlen(want));
            if (slen == strlen(want)) CHECK(memcmp(str, want, slen) == 0);
        }
        dbuf_free(&res); bj_builder_free(rb);
    }

    /* An insert with no _id and no `id` is refused rather than given an
     * id this layer invented: generating one needs a clock. */
    {
        doc *d = doc_new();
        doc_str(d, "name", "no id here");
        uint32_t dlen; const uint8_t *dbuf_ = doc_done(d, &dlen);
        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request("insert", "users", "doc", dbuf_, dlen, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, 0, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_REQ_MISSING_FIELD);
        dbuf_free(&res); bj_builder_free(rb); doc_free(d);
    }

    /* Bytes that are not an object at all. */
    {
        static const uint8_t junk[] = { 0xff, 0x01, 0x02 };
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, 0, junk, sizeof junk, &res));
        CHECK_I64(response_ok(&res), 0);
        dbuf_free(&res);
    }

    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
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

/* A catalog as BPlusTree.toArray() yields it: [{key, value}, ...]. */
static void put_row(bj_builder *b, const char *name, const char *file,
                    const char *journal, const char *idx_file) {
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"key", 3);
    bj_put_string(b, (const uint8_t *)name, (uint32_t)strlen(name));
    bj_put_key(b, (const uint8_t *)"value", 5);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"file", 4);
    bj_put_string(b, (const uint8_t *)file, (uint32_t)strlen(file));
    if (journal) {
        bj_put_key(b, (const uint8_t *)"journal", 7);
        bj_put_string(b, (const uint8_t *)journal, (uint32_t)strlen(journal));
    }
    bj_put_key(b, (const uint8_t *)"indexes", 7);
    bj_begin_array(b);
    if (idx_file) {
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"name", 4);
        bj_put_string(b, (const uint8_t *)"i_1", 3);
        bj_put_key(b, (const uint8_t *)"file", 4);
        bj_put_string(b, (const uint8_t *)idx_file, (uint32_t)strlen(idx_file));
        bj_put_key(b, (const uint8_t *)"fields", 6);
        bj_begin_array(b);
        bj_put_string(b, (const uint8_t *)"i", 1);
        bj_end_array(b);
        bj_end_object(b);
    }
    bj_end_array(b);
    bj_end_object(b);
    bj_end_object(b);
}

/* Build a NUL-separated listing. */
static void listing(dbuf *out, const char *const *names, int n) {
    for (int i = 0; i < n; i++) {
        dbuf_put(out, (const uint8_t *)names[i], strlen(names[i]));
        static const uint8_t nul = 0;
        dbuf_put(out, &nul, 1);
    }
}

static int victim_listed(const dbuf *victims, const char *name) {
    return find_bytes(victims->data, victims->len, name, strlen(name)) != NULL;
}

TEST(sweep_plan_deletes_orphans_and_nothing_else) {
    bj_builder *cat = bj_builder_new();
    bj_begin_array(cat);
    put_row(cat, "users", "coll-users.bj", "coll-users-journal.bj", "idx-users-i_1.bj");
    /* A format-stamp row: a catalog key that owns no files. */
    bj_begin_object(cat);
    bj_put_key(cat, (const uint8_t *)"key", 3);
    bj_put_string(cat, (const uint8_t *)"__format__", 10);
    bj_put_key(cat, (const uint8_t *)"value", 5);
    bj_begin_object(cat);
    bj_put_key(cat, (const uint8_t *)"v", 1);
    bj_put_int(cat, 1);
    bj_end_object(cat);
    bj_end_object(cat);
    bj_end_array(cat);
    size_t cat_len = 0;
    const uint8_t *catalog = bj_builder_data(cat, &cat_len);

    static const char *const names[] = {
        "coll-users.bj",            /* live primary          */
        "coll-users-journal.bj",    /* live journal          */
        "idx-users-i_1.bj",         /* live index            */
        "g1-coll-users.bj",         /* orphaned generation   */
        "g1-idx-users-i_1.bj",      /* orphaned generation   */
        "__catalog__.bj",           /* never a victim        */
        "__wal__.bj",               /* not ours              */
        "notes.txt",                /* a host's own file     */
    };
    dbuf ls = {0};
    listing(&ls, names, 8);

    dbuf victims = {0};
    CHECK_OK(dc_sweep_plan(catalog, cat_len, (const char *)ls.data, ls.len, &victims));

    /* Exactly the two orphaned generation files. */
    CHECK_I64(arr_count(victims.data, victims.len), 2);
    CHECK(victim_listed(&victims, "g1-coll-users.bj"));
    CHECK(victim_listed(&victims, "g1-idx-users-i_1.bj"));

    /* Everything whose deletion would be data loss or rudeness. */
    CHECK(!victim_listed(&victims, "__catalog__.bj"));
    CHECK(!victim_listed(&victims, "__wal__.bj"));
    CHECK(!victim_listed(&victims, "notes.txt"));

    dbuf_free(&victims); dbuf_free(&ls);
    bj_builder_free(cat);
}

TEST(sweep_plan_spares_a_journal_an_old_entry_never_recorded) {
    /*
     * An entry written before compact() gave each generation its own
     * journal has no `journal` field, but the generation-0 journal is
     * still live. Sweeping it would silently destroy crash recovery for
     * that collection -- the database would keep working until the moment
     * it needed to recover.
     */
    bj_builder *cat = bj_builder_new();
    bj_begin_array(cat);
    put_row(cat, "users", "coll-users.bj", /*journal*/NULL, NULL);
    bj_end_array(cat);
    size_t cat_len = 0;
    const uint8_t *catalog = bj_builder_data(cat, &cat_len);

    static const char *const names[] = { "coll-users.bj", "coll-users-journal.bj" };
    dbuf ls = {0};
    listing(&ls, names, 2);

    dbuf victims = {0};
    CHECK_OK(dc_sweep_plan(catalog, cat_len, (const char *)ls.data, ls.len, &victims));
    CHECK_I64(arr_count(victims.data, victims.len), 0);

    dbuf_free(&victims); dbuf_free(&ls);
    bj_builder_free(cat);
}

TEST(sweep_plan_on_an_empty_catalog_still_spares_foreign_files) {
    /* The dangerous case: nothing is referenced, so only "is it ours?"
     * stands between the sweep and a host's directory. */
    bj_builder *cat = bj_builder_new();
    bj_begin_array(cat);
    bj_end_array(cat);
    size_t cat_len = 0;
    const uint8_t *catalog = bj_builder_data(cat, &cat_len);

    static const char *const names[] = {
        "coll-gone.bj", "idx-gone-x_1.bj", "__catalog__.bj", "notes.txt", "app.db"
    };
    dbuf ls = {0};
    listing(&ls, names, 5);

    dbuf victims = {0};
    CHECK_OK(dc_sweep_plan(catalog, cat_len, (const char *)ls.data, ls.len, &victims));
    CHECK_I64(arr_count(victims.data, victims.len), 2);
    CHECK(victim_listed(&victims, "coll-gone.bj"));
    CHECK(victim_listed(&victims, "idx-gone-x_1.bj"));
    CHECK(!victim_listed(&victims, "__catalog__.bj"));
    CHECK(!victim_listed(&victims, "notes.txt"));
    CHECK(!victim_listed(&victims, "app.db"));

    dbuf_free(&victims); dbuf_free(&ls);
    bj_builder_free(cat);
}

TEST(collection_files_and_the_sweep_agree_by_construction) {
    /*
     * dropCollection deletes what an entry claims; the sweep spares what
     * an entry claims. They must be the same set, so they share one
     * implementation -- and this test pins the consequence: every file
     * dc_collection_files lists is a file the sweep refuses to delete.
     */
    const uint8_t *entry; size_t entry_len;
    bj_builder *eb = empty_entry(&entry, &entry_len);
    const uint8_t *def; size_t def_len;
    bj_builder *db_ = eq_def("team_1", "idx-users-team_1.bj", "team", 0, &def, &def_len);
    dbuf full = {0};
    CHECK_OK(dc_catalog_put_index(entry, entry_len, def, def_len, &full));

    dbuf files = {0};
    CHECK_OK(dc_collection_files(full.data, full.len, "users", 5, &files));
    /* primary + journal + one index */
    CHECK_I64(arr_count(files.data, files.len), 3);
    CHECK(find_bytes(files.data, files.len, "coll-users.bj", 13) != NULL);
    CHECK(find_bytes(files.data, files.len, "coll-users-journal.bj", 21) != NULL);
    CHECK(find_bytes(files.data, files.len, "idx-users-team_1.bj", 19) != NULL);

    /* The same entry, seen by the sweep: none of those three is a victim. */
    bj_builder *cat = bj_builder_new();
    bj_begin_array(cat);
    bj_begin_object(cat);
    bj_put_key(cat, (const uint8_t *)"key", 3);
    bj_put_string(cat, (const uint8_t *)"users", 5);
    bj_put_key(cat, (const uint8_t *)"value", 5);
    bj_put_raw(cat, full.data, (uint32_t)full.len);
    bj_end_object(cat);
    bj_end_array(cat);
    size_t cat_len = 0;
    const uint8_t *catalog = bj_builder_data(cat, &cat_len);

    static const char *const names[] = {
        "coll-users.bj", "coll-users-journal.bj", "idx-users-team_1.bj", "g9-coll-users.bj"
    };
    dbuf ls = {0};
    listing(&ls, names, 4);
    dbuf victims = {0};
    CHECK_OK(dc_sweep_plan(catalog, cat_len, (const char *)ls.data, ls.len, &victims));
    CHECK_I64(arr_count(victims.data, victims.len), 1);
    CHECK(victim_listed(&victims, "g9-coll-users.bj"));

    dbuf_free(&victims); dbuf_free(&ls); dbuf_free(&files); dbuf_free(&full);
    bj_builder_free(cat); bj_builder_free(db_); bj_builder_free(eb);
}

TEST(a_fresh_entry_carries_only_what_it_has_earned) {
    dbuf entry = {0};
    CHECK_OK(dc_catalog_new_entry("users", 5, &entry));
    CHECK(find_bytes(entry.data, entry.len, "coll-users.bj", 13) != NULL);
    /* No journal, gen or indexes yet -- they arrive when earned, which is
     * why every reader treats them as optional. */
    CHECK(find_bytes(entry.data, entry.len, "journal", 7) == NULL);
    CHECK(find_bytes(entry.data, entry.len, "gen", 3) == NULL);

    /* ...and it still plans, deriving the journal it never stored. */
    dbuf plan = {0};
    CHECK_OK(dc_catalog_open_plan(entry.data, entry.len, "users", 5, &plan));
    CHECK(find_bytes(plan.data, plan.len, "coll-users-journal.bj", 21) != NULL);

    dbuf_free(&plan); dbuf_free(&entry);
}

TEST(compact_plan_regenerates_every_name_and_keeps_every_option) {
    /* An entry with a unique, sparse, TTL equality index -- the options a
     * hand-rebuilt entry is most likely to drop. */
    bj_builder *e0 = bj_builder_new();
    bj_begin_object(e0);
    bj_put_key(e0, (const uint8_t *)"file", 4);
    bj_put_string(e0, (const uint8_t *)"coll-users.bj", 13);
    bj_put_key(e0, (const uint8_t *)"journal", 7);
    bj_put_string(e0, (const uint8_t *)"coll-users-journal.bj", 21);
    bj_put_key(e0, (const uint8_t *)"compactedBytes", 14);
    bj_put_int(e0, 4096);
    bj_put_key(e0, (const uint8_t *)"indexes", 7);
    bj_begin_array(e0);
    bj_begin_object(e0);
    bj_put_key(e0, (const uint8_t *)"name", 4);
    bj_put_string(e0, (const uint8_t *)"seen_1", 6);
    bj_put_key(e0, (const uint8_t *)"kind", 4);
    bj_put_string(e0, (const uint8_t *)"equality", 8);
    bj_put_key(e0, (const uint8_t *)"file", 4);
    bj_put_string(e0, (const uint8_t *)"idx-users-seen_1.bj", 19);
    bj_put_key(e0, (const uint8_t *)"fields", 6);
    bj_begin_array(e0);
    bj_put_string(e0, (const uint8_t *)"seen", 4);
    bj_end_array(e0);
    bj_put_key(e0, (const uint8_t *)"unique", 6);
    bj_put_bool(e0, 1);
    bj_put_key(e0, (const uint8_t *)"expireAfterSeconds", 18);
    bj_put_int(e0, 3600);
    bj_end_object(e0);
    bj_end_array(e0);
    bj_end_object(e0);

    size_t len = 0;
    const uint8_t *entry = bj_builder_data(e0, &len);
    dbuf plan = {0};
    CHECK_OK(dc_compact_plan(entry, len, "users", 5, &plan));

    /* Generation 0 -> 1, with every name regenerated. */
    CHECK(find_bytes(plan.data, plan.len, "g1-coll-users.bj", 16) != NULL);
    CHECK(find_bytes(plan.data, plan.len, "g1-coll-users-journal.bj", 24) != NULL);
    CHECK(find_bytes(plan.data, plan.len, "g1-idx-users-seen_1.bj", 22) != NULL);

    /* Options survive: a compaction rewrites bytes, not definitions. A
     * lost `unique` here silently drops a constraint, and a lost
     * expireAfterSeconds silently stops expiring. */
    CHECK(find_bytes(plan.data, plan.len, "unique", 6) != NULL);
    CHECK(find_bytes(plan.data, plan.len, "expireAfterSeconds", 18) != NULL);
    /* Unrelated fields are carried through, not dropped. */
    CHECK(find_bytes(plan.data, plan.len, "compactedBytes", 14) != NULL);

    /* Old files are listed for deletion AFTER the flip. */
    CHECK(find_bytes(plan.data, plan.len, "coll-users-journal.bj", 21) != NULL);
    CHECK(find_bytes(plan.data, plan.len, "idx-users-seen_1.bj", 19) != NULL);

    dbuf_free(&plan);
    bj_builder_free(e0);
}

TEST(compact_plan_advances_from_the_recorded_generation) {
    bj_builder *e0 = bj_builder_new();
    bj_begin_object(e0);
    bj_put_key(e0, (const uint8_t *)"file", 4);
    bj_put_string(e0, (const uint8_t *)"g7-coll-users.bj", 16);
    bj_put_key(e0, (const uint8_t *)"journal", 7);
    bj_put_string(e0, (const uint8_t *)"g7-coll-users-journal.bj", 24);
    bj_put_key(e0, (const uint8_t *)"gen", 3);
    bj_put_int(e0, 7);
    bj_end_object(e0);
    size_t len = 0;
    const uint8_t *entry = bj_builder_data(e0, &len);

    dbuf plan = {0};
    CHECK_OK(dc_compact_plan(entry, len, "users", 5, &plan));
    CHECK(find_bytes(plan.data, plan.len, "g8-coll-users.bj", 16) != NULL);
    CHECK(find_bytes(plan.data, plan.len, "g8-coll-users-journal.bj", 24) != NULL);

    /* The planned entry must itself be openable -- a generation that
     * cannot be planned back is a stranded one. */
    const uint8_t *ne; size_t ne_len; int found = 0;
    CHECK_OK(obj_get_field_probe(plan.data, plan.len, "newEntry", &ne, &ne_len, &found));
    CHECK(found);
    if (found) {
        dbuf reopened = {0};
        CHECK_OK(dc_catalog_open_plan(ne, ne_len, "users", 5, &reopened));
        CHECK(find_bytes(reopened.data, reopened.len, "g8-coll-users.bj", 16) != NULL);
        dbuf_free(&reopened);
    }
    dbuf_free(&plan);
    bj_builder_free(e0);
}

TEST(sweep_execute_drives_a_real_namespace) {
    /*
     * The same dc_sweep_execute the browser calls, here over bjio_posix
     * and real files. One C function, two adapters -- which is the whole
     * claim of the bj_ns seam, and the thing that would quietly stop
     * being true if the two paths ever diverged.
     *
     * The adapters differ in one visible way and it is deliberate: this
     * one unlinks immediately, while the browser's queues the name for
     * the host to drain. dc_sweep_execute cannot tell.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-sweep", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);

    static const char *const files[] = {
        "coll-users.bj",          /* live      */
        "coll-users-journal.bj",  /* live      */
        "g1-coll-users.bj",       /* orphan    */
        "g2-idx-users-i_1.bj",    /* orphan    */
        "notes.txt",              /* not ours  */
        "__catalog__.bj",         /* never     */
    };
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        bj_io io;
        CHECK_FATAL(ns.open(ns.ctx, files[i], (uint32_t)strlen(files[i]),
                            BJ_NS_CREATE, &io) == BJ_OK);
        CHECK_OK(io.write(io.ctx, 0, (const uint8_t *)"x", 1));
        CHECK_OK(ns.close(ns.ctx, &io));
    }

    bj_builder *cat = bj_builder_new();
    bj_begin_array(cat);
    put_row(cat, "users", "coll-users.bj", "coll-users-journal.bj", NULL);
    bj_end_array(cat);
    size_t cat_len = 0;
    const uint8_t *catalog = bj_builder_data(cat, &cat_len);

    dbuf ls = {0};
    listing(&ls, files, 6);

    /*
     * Through the checking adapter, with NOTHING declared. A sweep is
     * pure deletion: it may unlink anything (bjns.h lets the browser
     * defer those) but it must never open a file, because a sweep runs
     * with no plan behind it and so has no name it could legally open.
     * If one ever appeared it would be BJ_ERR_STATE in a browser and
     * silently fine here -- which is the whole reason this wrapper
     * exists.
     */
    bj_ns checked;
    nscheck *k = nscheck_new(&ns, &checked);
    CHECK_FATAL(k != NULL);
    nscheck_begin(k);

    uint32_t deleted = 0;
    CHECK_OK(dc_sweep_execute(&checked, catalog, cat_len, (const char *)ls.data, ls.len, &deleted));
    CHECK_I64(deleted, 2);
    CHECK_I64(nscheck_opens(k), 0);
    CHECK_I64(nscheck_removes(k), 2);
    CHECK_I64(nscheck_violations(k), 0);
    nscheck_free(k);

    /* The orphans are really gone from the filesystem... */
    bj_io probe;
    CHECK_RC(ns.open(ns.ctx, "g1-coll-users.bj", 16, 0, &probe), BJ_ERR_STATE);
    CHECK_RC(ns.open(ns.ctx, "g2-idx-users-i_1.bj", 19, 0, &probe), BJ_ERR_STATE);
    /* ...and everything else is untouched. */
    for (const char *keep[] = { "coll-users.bj", "coll-users-journal.bj",
                                "notes.txt", "__catalog__.bj" }, **k = keep;
         k < keep + 4; k++) {
        CHECK_OK(ns.open(ns.ctx, *k, (uint32_t)strlen(*k), 0, &probe));
        CHECK_OK(ns.close(ns.ctx, &probe));
    }

    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++)
        ns.remove(ns.ctx, files[i], (uint32_t)strlen(files[i]));
    dbuf_free(&ls);
    bj_builder_free(cat);
    bjns_posix_free(&ns);
    close(dirfd);
    rmdir(tmpl);
}

TEST(compaction_refuses_while_a_cursor_is_reading_the_tree) {
    /*
     * The hazard db.h used to document and leave to callers: a cursor
     * pins the root of the tree it scans and walks nodes that mutations
     * never overwrite -- which is what makes it a snapshot, and exactly
     * what a compaction takes away. It rebuilds the collection into
     * fresh files and the old ones are deleted; every cursor still open
     * is left reading bytes that are gone.
     *
     * So the tree counts its readers (bpt_pinned) and dc_compact_execute
     * refuses on it, BEFORE writing anything. Enforced at the one point
     * that can see both, rather than remembered by each caller -- which
     * matters now that a caller can be a client on a socket that opened
     * a cursor and went quiet.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-cur-cmp", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);

    bj_io cio;
    CHECK_FATAL(ns.open(ns.ctx, "coll-users.bj", 13, BJ_NS_CREATE, &cio) == BJ_OK);
    bpt *primary = bpt_create(&cio, ORDER);
    CHECK_FATAL(primary != NULL);
    dc_collection *coll = dc_collection_open(primary);
    CHECK_FATAL(coll != NULL);
    for (uint32_t i = 1; i <= 20; i++)
        CHECK_OK(insert_person(coll, i, "person", "core", (int64_t)i));

    bj_io kio;
    CHECK_FATAL(ns.open(ns.ctx, "__catalog__.bj", 14, BJ_NS_CREATE, &kio) == BJ_OK);
    bpt *catalog = bpt_create(&kio, ORDER);
    CHECK_FATAL(catalog != NULL);
    dbuf entry = {0};
    CHECK_OK(dc_catalog_new_entry("users", 5, &entry));
    {
        bpt_key k = { .is_string = 1, .num = 0, .str = (const uint8_t *)"users", .str_len = 5 };
        CHECK_OK(bpt_add(catalog, &k, entry.data, (uint32_t)entry.len));
    }
    dbuf plan = {0};
    CHECK_OK(dc_compact_plan(entry.data, entry.len, "users", 5, &plan));

    void *sources[1] = { primary };
    int kinds[1] = { DC_SRC_BPT };
    uint64_t built = 0;

    /* An unfiltered find is a SCAN, which is the mode that holds a
     * position in the tree; a cursor over an index or an _id lookup
     * carries its candidates and does not. */
    doc *q = doc_new();
    uint32_t qlen; const uint8_t *qb = doc_done(q, &qlen);
    dc_cursor *cur1 = NULL;
    CHECK_OK(dc_cursor_open(coll, qb, qlen, NULL, 0, 0, 0, &cur1));
    CHECK_I64(bpt_pinned(primary), 1);

    /* Read one batch, so the cursor is genuinely mid-scan rather than
     * merely open. */
    {
        uint8_t *docs = NULL; size_t dlen = 0; int done = 0;
        CHECK_OK(dc_cursor_next_batch(cur1, 5, &docs, &dlen, &done));
        CHECK_I64(done, 0);
        free(docs);
    }

    CHECK_RC(dc_compact_execute(&ns, catalog, "users", 5, plan.data, plan.len,
                                sources, kinds, 1, &built),
             DC_ERR_CURSORS_OPEN);
    /* Refused before anything was built: the collection is untouched and
     * the refusal costs the caller nothing to retry. */
    CHECK_I64((int64_t)built, 0);

    /* Two readers, one closed, still refused: the count is a count. */
    dc_cursor *cur2 = NULL;
    CHECK_OK(dc_cursor_open(coll, qb, qlen, NULL, 0, 0, 0, &cur2));
    CHECK_I64(bpt_pinned(primary), 2);
    dc_cursor_close(cur1);
    CHECK_I64(bpt_pinned(primary), 1);
    CHECK_RC(dc_compact_execute(&ns, catalog, "users", 5, plan.data, plan.len,
                                sources, kinds, 1, &built),
             DC_ERR_CURSORS_OPEN);

    /* And with the last one gone it proceeds, which is the other half of
     * the claim: this refuses a compaction, it does not prevent one. */
    dc_cursor_close(cur2);
    CHECK_I64(bpt_pinned(primary), 0);
    CHECK_OK(dc_compact_execute(&ns, catalog, "users", 5, plan.data, plan.len,
                                sources, kinds, 1, &built));
    CHECK(built > 0);

    doc_free(q);
    dbuf_free(&plan); dbuf_free(&entry);
    /* dc_collection_free releases the collection and its indexes, not the
     * primary tree it was handed -- and a tree BORROWS its io, so the
     * order is tree first, io after. */
    dc_collection_free(coll);
    bpt_free(primary);
    bpt_free(catalog);
    CHECK_OK(ns.close(ns.ctx, &cio));
    CHECK_OK(ns.close(ns.ctx, &kio));
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(compact_execute_builds_and_flips_over_real_files) {
    /*
     * The same dc_compact_execute the browser calls, here over
     * bjio_posix. Natively there is no pre-open step at all -- ns->open
     * really opens -- which is the clearest demonstration that the
     * plan/execute discipline costs the server nothing and buys the
     * browser everything.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-compact", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);

    /* A collection with garbage to reclaim: insert, then delete most. */
    bj_io cio;
    CHECK_FATAL(ns.open(ns.ctx, "coll-users.bj", 13, BJ_NS_CREATE, &cio) == BJ_OK);
    bpt *primary = bpt_create(&cio, ORDER);
    CHECK_FATAL(primary != NULL);
    dc_collection *coll = dc_collection_open(primary);
    CHECK_FATAL(coll != NULL);
    for (uint32_t i = 1; i <= 60; i++)
        CHECK_OK(insert_person(coll, i, "person", "core", (int64_t)i));
    {
        doc *q = doc_new();
        doc_str(q, "team", "core");
        uint32_t qlen; const uint8_t *qb = doc_done(q, &qlen);
        int64_t deleted = 0;
        CHECK_OK(dc_delete_many(coll, qb, qlen, &deleted, NULL, NULL));
        CHECK_I64(deleted, 60);
        doc_free(q);
    }
    uint64_t before = cio.size(cio.ctx);
    CHECK(before > 0);

    /* A catalog holding this collection's entry. */
    bj_io kio;
    CHECK_FATAL(ns.open(ns.ctx, "__catalog__.bj", 14, BJ_NS_CREATE, &kio) == BJ_OK);
    bpt *catalog = bpt_create(&kio, ORDER);
    CHECK_FATAL(catalog != NULL);
    dbuf entry = {0};
    CHECK_OK(dc_catalog_new_entry("users", 5, &entry));
    {
        bpt_key k = { .is_string = 1, .num = 0, .str = (const uint8_t *)"users", .str_len = 5 };
        CHECK_OK(bpt_add(catalog, &k, entry.data, (uint32_t)entry.len));
    }

    dbuf plan = {0};
    CHECK_OK(dc_compact_plan(entry.data, entry.len, "users", 5, &plan));

    /*
     * Execute through the checking adapter rather than the bare POSIX
     * one, with the declaration read straight out of the plan -- so this
     * asserts that the PLAN and the EXECUTOR agree about which files
     * exist, which is the property the browser depends on and the one
     * openat cannot fail to satisfy.
     */
    bj_ns checked;
    nscheck *k = nscheck_new(&ns, &checked);
    CHECK_FATAL(k != NULL);
    nscheck_begin(k);
    CHECK_OK(nscheck_declare_compact_plan(k, plan.data, plan.len));

    void *sources[1] = { primary };
    int kinds[1] = { DC_SRC_BPT };
    uint64_t built = 0;
    CHECK_OK(dc_compact_execute(&checked, catalog, "users", 5, plan.data, plan.len,
                                sources, kinds, 1, &built));
    CHECK(built > 0);

    /* No undeclared open, and no declared name left unopened: a plan
     * that named a file nobody wanted would cost the browser an awaited
     * OPFS handle for nothing. Two files here -- the primary and the
     * journal; the collection carries no indexes. */
    if (nscheck_violations(k))
        TAP_FAIL("plan/execute violation: %s", nscheck_first_violation(k));
    CHECK_I64(nscheck_opens(k), 2);
    CHECK_I64(nscheck_unopened(k), 0);
    nscheck_end(k);
    nscheck_free(k);
    /* Reclaiming is the point: 60 inserts and 60 deletes leave an
     * append-only file far larger than the empty tree it compacts to. */
    CHECK(built < before);

    /* The new generation exists on disk... */
    bj_io probe;
    CHECK_OK(ns.open(ns.ctx, "g1-coll-users.bj", 16, 0, &probe));
    CHECK(probe.size(probe.ctx) > 0);
    CHECK_OK(ns.close(ns.ctx, &probe));
    /* ...its own empty journal was created... */
    CHECK_OK(ns.open(ns.ctx, "g1-coll-users-journal.bj", 24, 0, &probe));
    CHECK_I64(probe.size(probe.ctx), 0);
    CHECK_OK(ns.close(ns.ctx, &probe));

    /* ...and the catalog was flipped to it, durably, before any old file
     * is deleted -- a lost flip after those deletes would leave the
     * catalog pointing at files that no longer exist. */
    {
        bpt_key k = { .is_string = 1, .num = 0, .str = (const uint8_t *)"users", .str_len = 5 };
        int found = 0; const uint8_t *vp = NULL; size_t vlen = 0;
        CHECK_OK(bpt_search(catalog, &k, &found, &vp, &vlen));
        CHECK(found);
        if (found) {
            CHECK(find_bytes(vp, vlen, "g1-coll-users.bj", 16) != NULL);
            CHECK(find_bytes(vp, vlen, "compactedBytes", 14) != NULL);
        }
    }

    dbuf_free(&plan); dbuf_free(&entry);
    dc_collection_free(coll);
    bpt_free(catalog); bpt_free(primary);
    /* The trees BORROW these ios -- bpt_free does not close one, because
     * the namespace opened it and the namespace closes it (bjns.h). Trees
     * first: freeing one writes through the io it was given. */
    CHECK_OK(ns.close(ns.ctx, &cio));
    CHECK_OK(ns.close(ns.ctx, &kio));
    bjns_posix_free(&ns);
    close(dirfd);
    /* Leave the directory for the OS; the files are all in tmpl. */
}

/* Build a twenty-document collection with a catalog holding its entry, and
 * plan its compaction. Returns 0, or -1 with everything the caller must
 * still free left in whatever state it reached. Used by the plan/execute
 * tests below, which need a real compaction to intercept rather than a
 * mock of one.
 *
 * The caller owns both ios and must ns.close each after freeing the tree
 * that borrows it: the namespace opened them, so the namespace closes
 * them, and bpt_free will not do it. */
static int compact_fixture(bj_ns *ns, bj_io *cio, bpt **primary,
                           dc_collection **coll, bj_io *kio, bpt **catalog,
                           dbuf *entry, dbuf *plan) {
    if (ns->open(ns->ctx, "coll-users.bj", 13, BJ_NS_CREATE, cio) != BJ_OK) return -1;
    *primary = bpt_create(cio, ORDER);
    if (!*primary) return -1;
    *coll = dc_collection_open(*primary);
    if (!*coll) return -1;
    for (uint32_t i = 1; i <= 20; i++)
        if (insert_person(*coll, i, "person", "core", (int64_t)i) != BJ_OK) return -1;

    if (ns->open(ns->ctx, "__catalog__.bj", 14, BJ_NS_CREATE, kio) != BJ_OK) return -1;
    *catalog = bpt_create(kio, ORDER);
    if (!*catalog) return -1;
    if (dc_catalog_new_entry("users", 5, entry) != BJ_OK) return -1;
    {
        bpt_key key = { .is_string = 1, .num = 0, .str = (const uint8_t *)"users", .str_len = 5 };
        if (bpt_add(*catalog, &key, entry->data, (uint32_t)entry->len) != BJ_OK) return -1;
    }
    return dc_compact_plan(entry->data, entry->len, "users", 5, plan) == BJ_OK ? 0 : -1;
}

TEST(an_undeclared_open_is_caught_the_way_a_browser_catches_it) {
    /*
     * The checker guarding the compaction test is only worth anything if
     * it would actually fail. So: drive it into every violation it can
     * report, including one raised by the real dc_compact_execute rather
     * than by a hand-made call.
     *
     * This is the test docs/db-plan.md's plan calls the highest-leverage
     * one in the port, and the reason is entirely about a build it never
     * runs: an undeclared open works perfectly under openat and is
     * BJ_ERR_STATE in a browser, in the one operation hardest to reach
     * from a test.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-nscheck", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
    bj_io io;

    /* ---- A name no plan named is refused, and named in the report. */
    {
        bj_ns checked;
        nscheck *k = nscheck_new(&ns, &checked);
        CHECK_FATAL(k != NULL);
        nscheck_begin(k);
        CHECK_OK(nscheck_declare(k, "declared.bj", 11));

        CHECK_OK(checked.open(checked.ctx, "declared.bj", 11, BJ_NS_CREATE, &io));
        CHECK_OK(checked.close(checked.ctx, &io));
        CHECK_I64(nscheck_opens(k), 1);
        CHECK_I64(nscheck_violations(k), 0);

        CHECK_RC(checked.open(checked.ctx, "sneaky.bj", 9, BJ_NS_CREATE, &io), BJ_ERR_STATE);
        CHECK_I64(nscheck_violations(k), 1);
        CHECK_STR(nscheck_first_violation(k), "open of undeclared name \"sneaky.bj\"");
        /* Refused, not merely counted: the inner namespace never saw it,
         * so the file does not exist. */
        CHECK_RC(ns.open(ns.ctx, "sneaky.bj", 9, 0, &io), BJ_ERR_STATE);
        nscheck_free(k);
    }

    /* ---- A declaration covers exactly one operation. The browser host
     * deletes Module.bjnsScopes[scope] in a finally, so a stale entry can
     * never satisfy a later, undeclared request -- "the IMMEDIATELY
     * preceding plan call" is literal. */
    {
        bj_ns checked;
        nscheck *k = nscheck_new(&ns, &checked);
        CHECK_FATAL(k != NULL);
        nscheck_begin(k);
        CHECK_OK(nscheck_declare(k, "declared.bj", 11));
        nscheck_end(k);
        CHECK_RC(checked.open(checked.ctx, "declared.bj", 11, 0, &io), BJ_ERR_STATE);
        CHECK_I64(nscheck_violations(k), 1);
        nscheck_free(k);
    }

    /* ---- And the real executor is really intercepted. Declare the
     * primary but not the journal -- the mistake a host would make by
     * pre-opening from the wrong field -- and dc_compact_execute fails
     * exactly where a browser fails it, on the name it could not
     * resolve. */
    {
        bj_io cio, kio;
        bpt *primary = NULL, *catalog = NULL;
        dc_collection *coll = NULL;
        dbuf entry = {0}, plan = {0};
        CHECK_FATAL(compact_fixture(&ns, &cio, &primary, &coll, &kio, &catalog,
                                    &entry, &plan) == 0);

        bj_ns checked;
        nscheck *k = nscheck_new(&ns, &checked);
        CHECK_FATAL(k != NULL);
        nscheck_begin(k);
        CHECK_OK(nscheck_declare(k, "g1-coll-users.bj", 16));

        void *sources[1] = { primary };
        int kinds[1] = { DC_SRC_BPT };
        uint64_t built = 0;
        CHECK_RC(dc_compact_execute(&checked, catalog, "users", 5, plan.data, plan.len,
                                    sources, kinds, 1, &built),
                 BJ_ERR_STATE);
        CHECK_STR(nscheck_first_violation(k),
                  "open of undeclared name \"g1-coll-users-journal.bj\"");

        /* The failure left the collection wholly on its old generation --
         * dc_compact_execute's ordering contract, which holds however it
         * fails. */
        {
            bpt_key key = { .is_string = 1, .num = 0, .str = (const uint8_t *)"users", .str_len = 5 };
            int found = 0; const uint8_t *vp = NULL; size_t vlen = 0;
            CHECK_OK(bpt_search(catalog, &key, &found, &vp, &vlen));
            CHECK(found);
            if (found) CHECK(find_bytes(vp, vlen, "g1-coll-users.bj", 16) == NULL);
        }

        nscheck_free(k);
        dbuf_free(&plan); dbuf_free(&entry);
        dc_collection_free(coll);
        bpt_free(catalog); bpt_free(primary);
        CHECK_OK(ns.close(ns.ctx, &cio));
        CHECK_OK(ns.close(ns.ctx, &kio));
    }

    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(compaction_reclaims_space_without_the_truncate_flag) {
    /*
     * bjns_bridge.c ignores flags outright -- the host already opened the
     * file, with `{create: true}` and nothing else, and re-opening is the
     * one thing an async open forbids. So BJ_NS_TRUNC is a POSIX
     * optimisation that a browser silently does not perform, and any code
     * depending on it is correct here and wrong there.
     *
     * dc_compact_execute depended on it. Retrying a compaction after one
     * crashed mid-build reopens the SAME generation name -- the catalog
     * still records the old gen, so the plan names gen+1 again -- over a
     * partially written file. Natively TRUNC cleared it. In a browser the
     * stale bytes stayed, so the rebuilt file kept whatever tail the dead
     * attempt had left and dst.size() reported it: a compaction that
     * reclaimed nothing and recorded a compactedBytes larger than the
     * file it had just rewritten, which is Db.compact()'s growth
     * baseline.
     *
     * The checking adapter strips the flag, so this runs under browser
     * rules on a real filesystem.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-trunc", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);

    bj_io cio, kio;
    bpt *primary = NULL, *catalog = NULL;
    dc_collection *coll = NULL;
    dbuf entry = {0}, plan = {0};
    CHECK_FATAL(compact_fixture(&ns, &cio, &primary, &coll, &kio, &catalog,
                                &entry, &plan) == 0);

    /* The wreckage of an earlier attempt: the generation the plan is
     * about to name again, already on disk and far larger than the tree
     * that will replace it. */
    {
        bj_io junk;
        CHECK_FATAL(ns.open(ns.ctx, "g1-coll-users.bj", 16, BJ_NS_CREATE, &junk) == BJ_OK);
        static uint8_t filler[64 * 1024];
        memset(filler, 0xAB, sizeof(filler));
        for (int i = 0; i < 4; i++)
            CHECK_OK(junk.write(junk.ctx, (uint64_t)i * sizeof(filler), filler, sizeof(filler)));
        CHECK_I64(junk.size(junk.ctx), 4 * (int)sizeof(filler));
        CHECK_OK(ns.close(ns.ctx, &junk));
    }

    bj_ns checked;
    nscheck *k = nscheck_new(&ns, &checked);
    CHECK_FATAL(k != NULL);
    nscheck_begin(k);
    CHECK_OK(nscheck_declare_compact_plan(k, plan.data, plan.len));

    void *sources[1] = { primary };
    int kinds[1] = { DC_SRC_BPT };
    uint64_t built = 0;
    CHECK_OK(dc_compact_execute(&checked, catalog, "users", 5, plan.data, plan.len,
                                sources, kinds, 1, &built));
    if (nscheck_violations(k))
        TAP_FAIL("plan/execute violation: %s", nscheck_first_violation(k));

    /* Twenty small documents do not occupy 256 KB. Both halves matter:
     * the file really shrank, and the number the catalog records is that
     * file's size rather than the corpse's. */
    bj_io probe;
    CHECK_FATAL(ns.open(ns.ctx, "g1-coll-users.bj", 16, 0, &probe) == BJ_OK);
    CHECK(probe.size(probe.ctx) < 4 * 64 * 1024);
    CHECK_I64(built, (long long)probe.size(probe.ctx));
    CHECK_OK(ns.close(ns.ctx, &probe));

    /* And it is a tree, not a tree with a tail: reopening finds exactly
     * the twenty documents and nothing the dead attempt left behind. */
    CHECK_FATAL(ns.open(ns.ctx, "g1-coll-users.bj", 16, 0, &probe) == BJ_OK);
    {
        bpt *t = bpt_open(&probe);
        CHECK_FATAL(t != NULL);
        dc_collection *c = dc_collection_open(t);
        CHECK_FATAL(c != NULL);
        const uint8_t *f; uint32_t flen;
        bj_builder *fb = empty_filter(&f, &flen);
        int64_t count = 0;
        CHECK_OK(dc_count(c, f, flen, &count));
        CHECK_I64(count, 20);
        bj_builder_free(fb);
        dc_collection_free(c);
        bpt_free(t);
    }
    CHECK_OK(ns.close(ns.ctx, &probe));

    nscheck_free(k);
    dbuf_free(&plan); dbuf_free(&entry);
    dc_collection_free(coll);
    bpt_free(catalog); bpt_free(primary);
    CHECK_OK(ns.close(ns.ctx, &cio));
    CHECK_OK(ns.close(ns.ctx, &kio));
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(update_many_hands_back_post_images_when_asked) {
    /*
     * A change-stream consumer needs every updated document. The host used
     * to get them by re-reading each matched _id afterwards -- one query
     * per document, a cost the JS comment itself called "O(matched) extra
     * round trips". This loop already holds each post-image, so collecting
     * them is free.
     */
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));
    CHECK_OK(insert_person(fx.coll, 2, "Grace", "core", 45));
    CHECK_OK(insert_person(fx.coll, 3, "Alan", "research", 41));

    doc *q = doc_new();
    doc_str(q, "team", "core");
    uint32_t qlen; const uint8_t *qbuf = doc_done(q, &qlen);
    doc *u = doc_new();
    doc_begin_obj(u, "$set");
    doc_key(u, "seen");
    bj_put_bool(u->b, 1);
    doc_end_obj(u);
    uint32_t ulen; const uint8_t *ubuf = doc_done(u, &ulen);

    uint8_t default_id[12];
    mk_oid(default_id, 99);
    int64_t matched = 0; int upserted = 0;
    uint8_t *images = NULL; size_t images_len = 0;
    CHECK_OK(dc_update_many(fx.coll, qbuf, qlen, ubuf, ulen, default_id, 0,
                            &matched, &upserted, NULL, &images, &images_len));
    CHECK_I64(matched, 2);
    CHECK_I64(arr_count(images, images_len), 2);
    /* POST-images: the $set is already applied in what comes back. */
    CHECK(find_bytes(images, images_len, "seen", 4) != NULL);
    free(images);

    /* Not asking costs nothing and returns nothing. */
    images = NULL; images_len = 0;
    CHECK_OK(dc_update_many(fx.coll, qbuf, qlen, ubuf, ulen, default_id, 0,
                            &matched, &upserted, NULL, NULL, NULL));
    CHECK(images == NULL);

    /* An upsert's post-image is the inserted document. */
    doc *q2 = doc_new();
    doc_str(q2, "team", "brand-new");
    uint32_t q2len; const uint8_t *q2buf = doc_done(q2, &q2len);
    images = NULL; images_len = 0;
    CHECK_OK(dc_update_many(fx.coll, q2buf, q2len, ubuf, ulen, default_id, 1,
                            &matched, &upserted, NULL, &images, &images_len));
    CHECK_I64(upserted, 1);
    CHECK_I64(arr_count(images, images_len), 1);
    CHECK(find_bytes(images, images_len, "brand-new", 9) != NULL);
    free(images);

    doc_free(q2); doc_free(u); doc_free(q);
    fx_close(&fx);
}

/* ---- Raft decision rules (raft_core.h) -------------------------------- */

/* A baseline vote request: a current-term candidate with a log as long
 * as ours, at a node that is a voter and hears from nobody. Each test
 * changes exactly the field it is about. */
static raft_vote_in vote_baseline(void) {
    raft_vote_in in;
    memset(&in, 0, sizeof(in));
    in.msg_term = 5;
    in.candidate_id = 2;
    in.last_log_index = 10;
    in.last_log_term = 4;
    in.current_term = 5;
    in.voted_for = 0;
    in.our_last_index = 10;
    in.our_last_term = 4;
    in.self_is_voter = 1;
    in.now = 1000;
    in.last_leader_contact = -1000000;
    in.min_election_timeout = 150;
    return in;
}

TEST(raft_vote_follows_the_up_to_date_rule) {
    raft_vote_out out;

    /* Equal logs: granted. */
    raft_vote_in in = vote_baseline();
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 1);
    CHECK_I64(out.persist, 1);            /* the vote reaches disk first */
    CHECK_I64((long long)out.persist_voted_for, 2);
    CHECK_I64(out.reset_election_timer, 1);

    /* §5.4.1: a candidate with an older last TERM loses, however long
     * its log. This is the rule that stops a node with more entries but
     * a stale term from erasing committed history. */
    in = vote_baseline();
    in.last_log_term = 3;
    in.last_log_index = 999;
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 0);

    /* Same term, shorter log: loses. */
    in = vote_baseline();
    in.last_log_index = 9;
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 0);

    /* Same term, longer log: wins. */
    in = vote_baseline();
    in.last_log_index = 11;
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 1);

    /* Higher term: wins regardless of index. */
    in = vote_baseline();
    in.last_log_term = 5;
    in.last_log_index = 0;
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 1);
}

TEST(raft_vote_is_at_most_one_per_term) {
    raft_vote_out out;

    /* Already voted for somebody else this term. */
    raft_vote_in in = vote_baseline();
    in.voted_for = 7;
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 0);
    CHECK_I64(out.persist, 0);

    /* The same candidate asking again is idempotent, and costs no second
     * fsync -- a retried RequestVote is ordinary on a lossy network. */
    in = vote_baseline();
    in.voted_for = 2;
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 1);
    CHECK_I64(out.persist, 0);

    /* A NEW term clears the old vote, so the same node may vote again --
     * and the step-down must be reported so the caller persists the term
     * before replying. */
    in = vote_baseline();
    in.voted_for = 7;
    in.msg_term = 6;
    raft_decide_vote(&in, &out);
    CHECK_I64(out.step_down, 1);
    CHECK_I64((long long)out.step_down_term, 6);
    CHECK_I64(out.grant, 1);
    CHECK_I64((long long)out.persist_term, 6);
    CHECK_I64((long long)out.reply_term, 6);

    /* A candidate from the past learns our term and gets nothing. */
    in = vote_baseline();
    in.msg_term = 4;
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 0);
    CHECK_I64(out.step_down, 0);
    CHECK_I64((long long)out.reply_term, 5);
}

TEST(raft_prevote_persists_nothing_and_respects_a_live_leader) {
    raft_vote_out out;

    /* A pre-vote round never persists and never bumps a term: that is
     * the whole point -- an isolated node rejoining must not dethrone a
     * working leader just by arriving with a higher term. */
    raft_vote_in in = vote_baseline();
    in.pre_vote = 1;
    in.msg_term = 99;
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 1);
    CHECK_I64(out.persist, 0);
    CHECK_I64(out.step_down, 0);
    CHECK_I64((long long)out.reply_term, 5);

    /* Stickiness: a node hearing from a leader refuses, even to a
     * perfectly up-to-date candidate. The disruptive case this exists
     * for is a removed-but-unaware member pre-voting at a healthy
     * leader's followers. */
    in = vote_baseline();
    in.pre_vote = 1;
    in.leader_id = 3;
    in.last_leader_contact = 900;      /* 100ms ago, under the 150 floor */
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 0);

    /* Silence past the floor releases it. */
    in.last_leader_contact = 800;      /* 200ms ago */
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 1);

    /* A leader itself always refuses. */
    in = vote_baseline();
    in.pre_vote = 1;
    in.is_leader = 1;
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 0);

    /* A learner holds no franchise, in either round. */
    in = vote_baseline();
    in.self_is_voter = 0;
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 0);
    in.pre_vote = 1;
    raft_decide_vote(&in, &out);
    CHECK_I64(out.grant, 0);
}

static raft_append_in append_baseline(void) {
    raft_append_in in;
    memset(&in, 0, sizeof(in));
    in.msg_term = 5;
    in.leader_id = 3;
    in.prev_log_index = 10;
    in.prev_log_term = 4;
    in.entry_count = 2;
    in.current_term = 5;
    in.is_follower = 1;
    in.our_base_index = 0;
    in.our_last_index = 10;
    in.our_prev_term = 4;
    return in;
}

TEST(raft_append_consistency_check_hints_where_to_resume) {
    raft_append_out out;

    raft_append_in in = append_baseline();
    raft_decide_append(&in, &out);
    CHECK_I64(out.success, 1);
    CHECK_I64((long long)out.match_index, 12);   /* prev + entry_count */
    CHECK_I64(out.has_hint, 0);
    CHECK_I64(out.stale, 0);

    /* Behind our snapshot boundary: tell the leader where our log
     * actually starts, so nextIndex jumps forward in one round trip
     * rather than walking down entries that no longer exist. */
    in = append_baseline();
    in.our_base_index = 20;
    in.our_last_index = 30;
    raft_decide_append(&in, &out);
    CHECK_I64(out.success, 0);
    CHECK_I64(out.has_hint, 1);
    CHECK_I64((long long)out.hint_index, 21);

    /* Past our tail: the first index we are missing. */
    in = append_baseline();
    in.our_last_index = 7;
    raft_decide_append(&in, &out);
    CHECK_I64(out.success, 0);
    CHECK_I64((long long)out.hint_index, 8);

    /* Term disagreement at prev: rewind to the disputed index itself. */
    in = append_baseline();
    in.our_prev_term = 3;
    raft_decide_append(&in, &out);
    CHECK_I64(out.success, 0);
    CHECK_I64((long long)out.hint_index, 10);

    /* A stale leader: no side effects at all, not even a refreshed
     * election timer. Distinct from a rejection, where the leader is
     * current and only misaligned. */
    in = append_baseline();
    in.msg_term = 4;
    raft_decide_append(&in, &out);
    CHECK_I64(out.stale, 1);
    CHECK_I64(out.success, 0);
    CHECK_I64(out.step_down, 0);
    CHECK_I64((long long)out.reply_term, 5);

    /* A higher term, or any term while we think we are a candidate or
     * leader, means conceding. */
    in = append_baseline();
    in.msg_term = 6;
    raft_decide_append(&in, &out);
    CHECK_I64(out.step_down, 1);
    CHECK_I64((long long)out.step_down_term, 6);
    CHECK_I64((long long)out.step_down_leader, 3);
    CHECK_I64((long long)out.reply_term, 6);

    in = append_baseline();
    in.is_follower = 0;             /* we thought we were the leader */
    raft_decide_append(&in, &out);
    CHECK_I64(out.step_down, 1);
    CHECK_I64((long long)out.step_down_term, 5);
    CHECK_I64(out.success, 1);
}

/* An AppendEntries `entries` array: {index, term, type, payload}. */
static void entry(bj_builder *b, uint64_t index, uint64_t term, const char *payload) {
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"index", 5); bj_put_int(b, (int64_t)index);
    bj_put_key(b, (const uint8_t *)"term", 4);  bj_put_int(b, (int64_t)term);
    bj_put_key(b, (const uint8_t *)"type", 4);  bj_put_int(b, EL_NORMAL);
    bj_put_key(b, (const uint8_t *)"payload", 7);
    bj_put_binary(b, (const uint8_t *)payload, (uint32_t)strlen(payload));
    bj_end_object(b);
}

TEST(raft_conflict_rule_truncates_only_what_disagrees) {
    /*
     * §5.3 against a real log. The rule is subtle in exactly one place:
     * an entry we ALREADY hold at the same term must be skipped, not
     * re-appended and not treated as a conflict -- a retried or
     * overlapping AppendEntries is completely ordinary, and truncating
     * on one would throw away entries the leader believes are
     * replicated.
     */
    memfs *fs = memfs_new();
    CHECK_FATAL(fs != NULL);
    bj_io io;
    CHECK_FATAL(memfs_open(fs, "log.bj", &io) == BJ_OK);
    elog *log = elog_create(&io);
    CHECK_FATAL(log != NULL);
    CHECK_OK(elog_set_hard_state(log, 2, 0));

    uint64_t at = 0;
    for (int i = 1; i <= 4; i++) CHECK_OK(elog_append(log, 1, EL_NORMAL, (const uint8_t *)"x", 1, &at));
    CHECK_OK(elog_sync(log));
    CHECK_I64((long long)elog_last_index(log), 4);

    uint64_t truncated = 999;

    /* A batch we already hold, verbatim: nothing changes. */
    bj_builder *same = bj_builder_new();
    bj_begin_array(same);
    entry(same, 2, 1, "x");
    entry(same, 3, 1, "x");
    bj_end_array(same);
    size_t slen; const uint8_t *sbuf = bj_builder_data(same, &slen);
    CHECK_OK(raft_append_entries_to_log(log, sbuf, (uint32_t)slen, &truncated));
    CHECK_I64((long long)truncated, 0);
    CHECK_I64((long long)elog_last_index(log), 4);

    /* An overlap that agrees, then extends: only the tail is appended. */
    bj_builder *ext = bj_builder_new();
    bj_begin_array(ext);
    entry(ext, 4, 1, "x");
    entry(ext, 5, 2, "y");
    bj_end_array(ext);
    size_t elen; const uint8_t *ebuf = bj_builder_data(ext, &elen);
    CHECK_OK(raft_append_entries_to_log(log, ebuf, (uint32_t)elen, &truncated));
    CHECK_I64((long long)truncated, 0);
    CHECK_I64((long long)elog_last_index(log), 5);

    /* A genuine conflict at 3: our suffix from there goes, and the
     * leader's replaces it. Reported so the caller can say so. */
    bj_builder *conflict = bj_builder_new();
    bj_begin_array(conflict);
    entry(conflict, 3, 2, "z");
    entry(conflict, 4, 2, "z");
    bj_end_array(conflict);
    size_t clen; const uint8_t *cbuf = bj_builder_data(conflict, &clen);
    CHECK_OK(raft_append_entries_to_log(log, cbuf, (uint32_t)clen, &truncated));
    CHECK_I64((long long)truncated, 3);
    CHECK_I64((long long)elog_last_index(log), 4);
    uint64_t t = 0;
    CHECK_OK(elog_term_at(log, 3, &t));
    CHECK_I64((long long)t, 2);
    CHECK_OK(elog_term_at(log, 2, &t));
    CHECK_I64((long long)t, 1);       /* below the conflict: untouched */

    /* An empty batch is a heartbeat: no truncation, no sync-worthy
     * change. */
    bj_builder *none = bj_builder_new();
    bj_begin_array(none); bj_end_array(none);
    size_t nlen; const uint8_t *nbuf = bj_builder_data(none, &nlen);
    CHECK_OK(raft_append_entries_to_log(log, nbuf, (uint32_t)nlen, &truncated));
    CHECK_I64((long long)truncated, 0);
    CHECK_I64((long long)elog_last_index(log), 4);

    bj_builder_free(none); bj_builder_free(conflict);
    bj_builder_free(ext); bj_builder_free(same);
    elog_free(log);
    memfs_free(fs);
}

TEST(raft_follower_commit_never_runs_past_its_own_log) {
    uint64_t out = 0;
    /* The leader may have committed entries it has not sent us. */
    CHECK_I64(raft_follower_commit(20, 5, 12, &out), 1);
    CHECK_I64((long long)out, 12);
    CHECK_I64(raft_follower_commit(8, 5, 12, &out), 1);
    CHECK_I64((long long)out, 8);
    CHECK_I64(raft_follower_commit(5, 5, 12, &out), 0);
    CHECK_I64(raft_follower_commit(3, 5, 12, &out), 0);
    /* A leaderCommit above ours but a log shorter than our commit index
     * cannot move it backwards. */
    CHECK_I64(raft_follower_commit(20, 12, 12, &out), 0);
}

TEST(raft_leader_commits_only_current_term_entries) {
    uint64_t cand = 0;
    /* Three voters, quorum 2: the leader plus one peer at 7. */
    uint64_t matches3[] = { 7, 3 };
    CHECK_I64(raft_commit_candidate(9, matches3, 2, 2, &cand), 1);
    CHECK_I64((long long)cand, 7);
    /* Quorum 3 of 3 needs the slowest. */
    CHECK_I64(raft_commit_candidate(9, matches3, 2, 3, &cand), 1);
    CHECK_I64((long long)cand, 3);
    /* A single-node cluster commits on its own. */
    CHECK_I64(raft_commit_candidate(4, NULL, 0, 1, &cand), 1);
    CHECK_I64((long long)cand, 4);

    /*
     * The figure-8 rule. A candidate index replicated to a quorum is
     * still NOT committable unless it belongs to the current term --
     * otherwise a new leader counts an old entry to a majority, commits
     * it, and then loses it to a node that never held it.
     */
    CHECK_I64(raft_may_commit(7, 5, 0, /*term_at*/ 5, /*current*/ 5), 1);
    CHECK_I64(raft_may_commit(7, 5, 0, /*term_at*/ 4, /*current*/ 5), 0);
    /* Never backwards, never at or below the snapshot boundary. */
    CHECK_I64(raft_may_commit(5, 5, 0, 5, 5), 0);
    CHECK_I64(raft_may_commit(7, 5, 7, 5, 5), 0);
    CHECK_I64(raft_may_commit(8, 5, 7, 5, 5), 1);
}

TEST(raft_backoff_believes_a_follower_that_lost_its_disk) {
    raft_backoff_out out;

    /* An ordinary hint rewinds to it. */
    raft_backoff(6, 1, 20, 3, &out);
    CHECK_I64((long long)out.next, 6);
    CHECK_I64((long long)out.match, 3);
    CHECK_I64(out.match_regressed, 0);

    /* A hint forward of a plain decrement is not authority: cap it. */
    raft_backoff(99, 1, 20, 3, &out);
    CHECK_I64((long long)out.next, 19);

    /* No hint at all: plain decrement. */
    raft_backoff(0, 0, 20, 3, &out);
    CHECK_I64((long long)out.next, 19);

    /* Never below 1. */
    raft_backoff(0, 0, 1, 0, &out);
    CHECK_I64((long long)out.next, 1);

    /*
     * The hint falls at or below what we saw the peer hold. Only losing
     * a disk does that -- a blank replacement reusing the id. Believe
     * it: match must drop too, or the leader keeps counting a node with
     * nothing toward its quorums and can commit an entry that exists in
     * one place.
     */
    raft_backoff(4, 1, 20, 10, &out);
    CHECK_I64((long long)out.next, 4);
    CHECK_I64((long long)out.match, 3);
    CHECK_I64(out.match_regressed, 1);

    raft_backoff(1, 1, 20, 10, &out);
    CHECK_I64((long long)out.next, 1);
    CHECK_I64((long long)out.match, 0);
    CHECK_I64(out.match_regressed, 1);
}

TEST(raft_quorum_counts_voters_only) {
    CHECK_I64((long long)raft_quorum(1), 1);
    CHECK_I64((long long)raft_quorum(2), 2);
    CHECK_I64((long long)raft_quorum(3), 2);
    CHECK_I64((long long)raft_quorum(4), 3);
    CHECK_I64((long long)raft_quorum(5), 3);
}

/* Read an ARRAY of INTs into `out`; returns the count, or -1. */
static long read_int_array(const uint8_t *obj, size_t len, const char *key,
                           int64_t *out, uint32_t cap) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(obj, len, (const uint8_t *)key, (uint32_t)strlen(key),
                      &v, &vlen, &found) != BJ_OK || !found) return -1;
    cur c = { v, vlen, 0 };
    uint32_t n;
    if (array_begin(&c, &n) != BJ_OK) return -1;
    for (uint32_t i = 0; i < n && i < cap; i++) {
        double d;
        if (read_number(&c, &d) != BJ_OK) return -1;
        out[i] = (int64_t)d;
    }
    return (long)n;
}

TEST(raft_membership_derives_the_same_lists_everywhere) {
    /* Two nodes adopting the same set must derive the same lists: the
     * quorum count is taken from a position in one of them, so an
     * ordering difference is a split-brain waiting to happen. */
    bj_builder *b = bj_builder_new();
    bj_begin_array(b);
    /* Deliberately out of order, and mixing records with bare ids. */
    bj_begin_object(b);
      bj_put_key(b, (const uint8_t *)"id", 2); bj_put_int(b, 3);
      bj_put_key(b, (const uint8_t *)"host", 4); bj_put_string(b, (const uint8_t *)"c", 1);
      bj_put_key(b, (const uint8_t *)"voting", 6); bj_put_bool(b, 0);
    bj_end_object(b);
    bj_put_int(b, 1);
    bj_begin_object(b);
      bj_put_key(b, (const uint8_t *)"id", 2); bj_put_int(b, 2);
      bj_put_key(b, (const uint8_t *)"host", 4); bj_put_string(b, (const uint8_t *)"b", 1);
    bj_end_object(b);
    bj_end_array(b);
    size_t blen; const uint8_t *bbuf = bj_builder_data(b, &blen);

    dbuf out = {0};
    CHECK_OK(raft_members_adopt(bbuf, (uint32_t)blen, 2, &out));

    int64_t ids[8];
    CHECK_I64(read_int_array(out.data, out.len, "voters", ids, 8), 2);
    CHECK_I64((long long)ids[0], 1);      /* sorted, and 3 excluded */
    CHECK_I64((long long)ids[1], 2);
    CHECK_I64(read_int_array(out.data, out.len, "peers", ids, 8), 2);
    CHECK_I64((long long)ids[0], 1);      /* self (2) excluded */
    CHECK_I64((long long)ids[1], 3);

    /* Records survive whole -- the addresses ARE the address book. */
    const uint8_t *v; size_t vlen; int found = 0;
    CHECK_OK(obj_get_field(out.data, out.len, (const uint8_t *)"members", 7, &v, &vlen, &found));
    CHECK_I64(found, 1);
    CHECK_I64(arr_count(v, vlen), 3);
    CHECK(find_bytes(v, vlen, "\x01""c", 2) != NULL || find_bytes(v, vlen, "c", 1) != NULL);
    dbuf_free(&out);

    /* A node applying its own removal must see itself gone: adopt does
     * not re-add self. */
    bj_builder *b2 = bj_builder_new();
    bj_begin_array(b2); bj_put_int(b2, 1); bj_put_int(b2, 3); bj_end_array(b2);
    size_t b2len; const uint8_t *b2buf = bj_builder_data(b2, &b2len);
    dbuf out2 = {0};
    CHECK_OK(raft_members_adopt(b2buf, (uint32_t)b2len, 2, &out2));
    CHECK_I64(read_int_array(out2.data, out2.len, "voters", ids, 8), 2);
    CHECK_I64(read_int_array(out2.data, out2.len, "peers", ids, 8), 2);
    CHECK_I64((long long)ids[0], 1);
    CHECK_I64((long long)ids[1], 3);
    dbuf_free(&out2);

    bj_builder_free(b2);
    bj_builder_free(b);
}

TEST(raft_membership_merge_cannot_erase_an_address) {
    /* changeMembership([1,2,3,4]) must not silently destroy the
     * addresses the log carries -- the log being the single source of
     * truth for the cluster's shape is what removes the separate address
     * book, and that holds only if an id-only proposal is harmless. */
    bj_builder *k = bj_builder_new();
    bj_begin_array(k);
    for (int i = 1; i <= 2; i++) {
        bj_begin_object(k);
        bj_put_key(k, (const uint8_t *)"id", 2); bj_put_int(k, i);
        bj_put_key(k, (const uint8_t *)"host", 4);
        bj_put_string(k, (const uint8_t *)(i == 1 ? "alpha" : "beta"), i == 1 ? 5 : 4);
        bj_put_key(k, (const uint8_t *)"port", 4); bj_put_int(k, 9000 + i);
        bj_end_object(k);
    }
    bj_end_array(k);
    size_t klen; const uint8_t *kbuf = bj_builder_data(k, &klen);

    /* Bare ids plus one brand-new member with an address. */
    bj_builder *i = bj_builder_new();
    bj_begin_array(i);
    bj_put_int(i, 1);
    bj_put_int(i, 2);
    bj_begin_object(i);
    bj_put_key(i, (const uint8_t *)"id", 2); bj_put_int(i, 3);
    bj_put_key(i, (const uint8_t *)"host", 4); bj_put_string(i, (const uint8_t *)"gamma", 5);
    bj_end_object(i);
    bj_end_array(i);
    size_t ilen; const uint8_t *ibuf = bj_builder_data(i, &ilen);

    dbuf out = {0};
    CHECK_OK(raft_members_merge(ibuf, (uint32_t)ilen, kbuf, (uint32_t)klen, &out));
    CHECK_I64(arr_count(out.data, out.len), 3);
    CHECK(find_bytes(out.data, out.len, "alpha", 5) != NULL);
    CHECK(find_bytes(out.data, out.len, "beta", 4) != NULL);
    CHECK(find_bytes(out.data, out.len, "gamma", 5) != NULL);
    dbuf_free(&out);

    /* A record that DOES state a host is making a statement -- that is
     * how a restarted node corrects the log -- so it wins. */
    bj_builder *m = bj_builder_new();
    bj_begin_array(m);
    bj_begin_object(m);
    bj_put_key(m, (const uint8_t *)"id", 2); bj_put_int(m, 1);
    bj_put_key(m, (const uint8_t *)"host", 4); bj_put_string(m, (const uint8_t *)"moved", 5);
    bj_end_object(m);
    bj_end_array(m);
    size_t mlen; const uint8_t *mbuf = bj_builder_data(m, &mlen);
    dbuf out2 = {0};
    CHECK_OK(raft_members_merge(mbuf, (uint32_t)mlen, kbuf, (uint32_t)klen, &out2));
    CHECK(find_bytes(out2.data, out2.len, "moved", 5) != NULL);
    CHECK(find_bytes(out2.data, out2.len, "alpha", 5) == NULL);
    dbuf_free(&out2);

    /* An empty set, or one with an unusable id, is refused. */
    bj_builder *e = bj_builder_new();
    bj_begin_array(e); bj_end_array(e);
    size_t elen; const uint8_t *ebuf = bj_builder_data(e, &elen);
    dbuf out3 = {0};
    CHECK_RC(raft_members_merge(ebuf, (uint32_t)elen, kbuf, (uint32_t)klen, &out3), RAFT_ERR_MEMBER);
    dbuf_free(&out3);

    bj_builder *z = bj_builder_new();
    bj_begin_array(z); bj_put_int(z, 0); bj_end_array(z);
    size_t zlen; const uint8_t *zbuf = bj_builder_data(z, &zlen);
    dbuf out4 = {0};
    CHECK_RC(raft_members_merge(zbuf, (uint32_t)zlen, kbuf, (uint32_t)klen, &out4), RAFT_ERR_MEMBER);
    dbuf_free(&out4);

    bj_builder_free(z); bj_builder_free(e); bj_builder_free(m);
    bj_builder_free(i); bj_builder_free(k);
}

/* ---- the RPC handlers, end to end in C (raft_msg.h) ------------------- */

/* A follower with `n` entries at term 1, hard state at `term`. */
typedef struct { memfs *fs; elog *log; } follower;

static int follower_open(follower *f, uint64_t term, int n) {
    memset(f, 0, sizeof(*f));
    f->fs = memfs_new();
    if (!f->fs) return -1;
    bj_io io;
    if (memfs_open(f->fs, "log.bj", &io) != BJ_OK) return -1;
    f->log = elog_create(&io);
    if (!f->log) return -1;
    if (term && elog_set_hard_state(f->log, term, EL_VOTED_NONE)) return -1;
    uint64_t at;
    for (int i = 0; i < n; i++) {
        if (elog_append(f->log, 1, EL_NORMAL, (const uint8_t *)"x", 1, &at)) return -1;
    }
    return n ? elog_sync(f->log) : 0;
}
static void follower_close(follower *f) {
    elog_free(f->log);
    memfs_free(f->fs);
}

static raft_msg_state msg_state(void) {
    raft_msg_state st;
    memset(&st, 0, sizeof(st));
    st.self_id = 2;
    st.is_follower = 1;
    st.self_is_voter = 1;
    st.now = 1000;
    st.last_leader_contact = -1000000;
    st.min_election_timeout = 150;
    return st;
}

static void msg_kv_int(bj_builder *b, const char *k, int64_t v) {
    bj_put_key(b, (const uint8_t *)k, (uint32_t)strlen(k));
    bj_put_int(b, v);
}
static void msg_kind(bj_builder *b, const char *kind) {
    bj_put_key(b, (const uint8_t *)"kind", 4);
    bj_put_string(b, (const uint8_t *)kind, (uint32_t)strlen(kind));
}

TEST(raft_request_vote_runs_end_to_end_in_c) {
    /*
     * The whole handler, with no host: the message arrives as bytes, the
     * vote reaches the disk, the reply leaves as bytes. Driving it from
     * a plain executable is the point -- this is what a server with no
     * JavaScript runtime will call.
     */
    follower f;
    CHECK_FATAL(follower_open(&f, 5, 3) == 0);
    raft_msg_state st = msg_state();
    raft_msg_effect eff;
    dbuf reply = {0};

    bj_builder *m = bj_builder_new();
    bj_begin_object(m);
    msg_kind(m, "requestVote");
    msg_kv_int(m, "term", 5);
    msg_kv_int(m, "candidateId", 7);
    msg_kv_int(m, "lastLogIndex", 3);
    msg_kv_int(m, "lastLogTerm", 1);
    bj_end_object(m);
    size_t mlen; const uint8_t *mbuf = bj_builder_data(m, &mlen);

    CHECK_OK(rmsg_handle_request_vote(f.log, &st, mbuf, (uint32_t)mlen, &eff, &reply));
    CHECK_I64(eff.granted_vote, 1);
    /* Durable BEFORE the reply exists -- the log already says so. */
    CHECK_I64((long long)elog_voted_for(f.log), 7);
    {
        const uint8_t *v; size_t vlen; int found = 0;
        CHECK_OK(obj_get_field(reply.data, reply.len, (const uint8_t *)"voteGranted", 11,
                               &v, &vlen, &found));
        CHECK_I64(found, 1);
        CHECK(vlen >= 1 && v[0] == BJ_TYPE_TRUE);
    }
    dbuf_free(&reply);

    /* A second candidate this term is refused, and nothing is written. */
    bj_builder *m2 = bj_builder_new();
    bj_begin_object(m2);
    msg_kind(m2, "requestVote");
    msg_kv_int(m2, "term", 5);
    msg_kv_int(m2, "candidateId", 9);
    msg_kv_int(m2, "lastLogIndex", 3);
    msg_kv_int(m2, "lastLogTerm", 1);
    bj_end_object(m2);
    size_t m2len; const uint8_t *m2buf = bj_builder_data(m2, &m2len);
    CHECK_OK(rmsg_handle_request_vote(f.log, &st, m2buf, (uint32_t)m2len, &eff, &reply));
    CHECK_I64(eff.granted_vote, 0);
    CHECK_I64((long long)elog_voted_for(f.log), 7);
    dbuf_free(&reply);

    /* A message this build does not understand is refused rather than
     * half-read: a peer speaking an unknown grammar cannot be answered. */
    bj_builder *bad = bj_builder_new();
    bj_begin_object(bad);
    msg_kind(bad, "teleport");
    bj_end_object(bad);
    size_t blen; const uint8_t *bbuf = bj_builder_data(bad, &blen);
    CHECK_RC(rmsg_handle_request_vote(f.log, &st, bbuf, (uint32_t)blen, &eff, &reply),
             RAFT_ERR_MESSAGE);
    int kind = -1;
    CHECK_RC(rmsg_kind(bbuf, (uint32_t)blen, &kind), RAFT_ERR_MESSAGE);
    dbuf_free(&reply);

    /*
     * Every kind the grammar names classifies, including the ones C does
     * not itself handle. That matters more than it looks: the host routes
     * on this number, so a kind missing from KIND_NAME is not "handled
     * elsewhere" -- it is a message the host cannot recognise at all, and
     * the node answers a well-formed peer with "unrecognized message".
     * timeoutNow arrived exactly that way, added to the JS side while the
     * C table stayed at five.
     */
    {
        static const char *const kinds[] = {
            "requestVote", "appendEntries", "installSnapshot",
            "join", "leave", "timeoutNow"
        };
        for (int i = 0; i < (int)(sizeof(kinds) / sizeof(kinds[0])); i++) {
            bj_builder *k = bj_builder_new();
            bj_begin_object(k);
            msg_kind(k, kinds[i]);
            bj_end_object(k);
            size_t klen; const uint8_t *kbuf = bj_builder_data(k, &klen);
            int got = -1;
            CHECK_OK(rmsg_kind(kbuf, (uint32_t)klen, &got));
            CHECK_I64(got, i);
            bj_builder_free(k);
        }
    }

    bj_builder_free(bad); bj_builder_free(m2); bj_builder_free(m);
    follower_close(&f);
}

TEST(install_snapshot_is_a_grammar_both_hosts_can_read) {
    /*
     * The fifth message kind. Its HANDLER is not in this layer -- it
     * writes files, so it belongs to whoever owns a namespace -- but its
     * ENVELOPE was written twice, once in src/raft.js's _sendSnapshot and
     * once in its _onInstallSnapshot, and a leader in C talking to a
     * follower in JavaScript needs exactly one definition of it.
     *
     * So: build every shape a transfer produces and read each one back.
     * The distinctions being pinned are the ones a transfer actually
     * turns on -- an empty chunk is not an absent one, a chunk that names
     * no file is not a chunk with no data, and only the first carries a
     * manifest.
     */

    /* A manifest is a whole object, spliced in as it stands: what goes in
     * it belongs to the layer that owns a snapshot store, not to the
     * envelope. */
    doc *mf = doc_new();
    doc_str(mf, "config", "whatever-the-store-said");
    uint32_t mflen; const uint8_t *mfbuf = doc_done(mf, &mflen);

    const uint8_t chunk[] = { 0xDE, 0xAD, 0xBE, 0xEF };

    /* ---- the first chunk: a file, some bytes, and the manifest. */
    {
        dbuf msg = {0};
        CHECK_OK(rmsg_build_install_snapshot(7, 3, 42, 5, "primary", 7, 0,
                                             chunk, sizeof chunk, 0,
                                             mfbuf, mflen, &msg));
        int kind = -1;
        CHECK_OK(rmsg_kind(msg.data, (uint32_t)msg.len, &kind));
        CHECK_I64(kind, RAFT_MSG_INSTALL_SNAPSHOT);
        /* The sender is read out of the message, by the same rule every
         * other kind is read by -- never told to us by a caller. */
        uint64_t from = 0;
        CHECK_OK(rmsg_sender(msg.data, (uint32_t)msg.len, &from));
        CHECK_I64((int64_t)from, 3);

        raft_install in;
        CHECK_OK(rmsg_install_read(msg.data, (uint32_t)msg.len, &in));
        CHECK_I64((int64_t)in.term, 7);
        CHECK_I64((int64_t)in.leader_id, 3);
        CHECK_I64((int64_t)in.last_included_index, 42);
        CHECK_I64((int64_t)in.last_included_term, 5);
        CHECK_I64((int64_t)in.offset, 0);
        CHECK_I64(in.done, 0);
        CHECK_I64((int64_t)in.role_len, 7);
        CHECK(in.role && memcmp(in.role, "primary", 7) == 0);
        CHECK_I64((int64_t)in.data_len, (int64_t)sizeof chunk);
        CHECK(in.data && memcmp(in.data, chunk, sizeof chunk) == 0);
        /* Spliced, so it comes back byte for byte -- the receiver hands
         * it to sst_check_files without anything having decoded it. */
        CHECK_I64((int64_t)in.manifest_len, (int64_t)mflen);
        CHECK(in.manifest && memcmp(in.manifest, mfbuf, mflen) == 0);
        dbuf_free(&msg);
    }

    /* ---- a later chunk: no manifest, an offset, and `done`. */
    {
        dbuf msg = {0};
        CHECK_OK(rmsg_build_install_snapshot(7, 3, 42, 5, "idx-team_1", 10, 4096,
                                             chunk, sizeof chunk, 1,
                                             NULL, 0, &msg));
        raft_install in;
        CHECK_OK(rmsg_install_read(msg.data, (uint32_t)msg.len, &in));
        CHECK_I64((int64_t)in.offset, 4096);
        CHECK_I64(in.done, 1);
        CHECK(in.manifest == NULL);
        CHECK_I64((int64_t)in.manifest_len, 0);
        dbuf_free(&msg);
    }

    /* ---- an EMPTY chunk for a real file. Not the same as no chunk: a
     * zero-length file gets one of its own so the receiver creates it,
     * because "absent" and "empty" are different things to the manifest
     * check on the other side. */
    {
        dbuf msg = {0};
        CHECK_OK(rmsg_build_install_snapshot(7, 3, 42, 5, "journal", 7, 0,
                                             NULL, 0, 1, NULL, 0, &msg));
        raft_install in;
        CHECK_OK(rmsg_install_read(msg.data, (uint32_t)msg.len, &in));
        CHECK(in.role != NULL);              /* it names a file... */
        CHECK_I64((int64_t)in.data_len, 0);  /* ...and carries nothing */
        dbuf_free(&msg);
    }

    /* ---- and a chunk that names NO file: a snapshot with no files at
     * all still has to carry its manifest and its boundary, which is the
     * whole point of the transfer. */
    {
        dbuf msg = {0};
        CHECK_OK(rmsg_build_install_snapshot(9, 2, 100, 8, NULL, 0, 0,
                                             NULL, 0, 1, mfbuf, mflen, &msg));
        raft_install in;
        CHECK_OK(rmsg_install_read(msg.data, (uint32_t)msg.len, &in));
        CHECK(in.role == NULL);
        CHECK_I64((int64_t)in.role_len, 0);
        CHECK_I64(in.done, 1);
        CHECK(in.manifest != NULL);
        CHECK_I64((int64_t)in.last_included_index, 100);
        dbuf_free(&msg);
    }

    /* ---- the replies, in their three shapes. `restart` says the
     * receiver has no install to attach a chunk to and needs the manifest
     * again; it is omitted rather than sent false, so a reader never has
     * to know a precedence between it and success. */
    {
        struct { int ok, restart; const char *want_restart; } cases[] = {
            { 1, 0, NULL }, { 0, 0, NULL }, { 0, 1, "restart" }, { 1, 1, NULL }
        };
        for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
            dbuf r = {0};
            CHECK_OK(rmsg_build_install_reply(11, cases[i].ok, cases[i].restart, &r));
            const uint8_t *v; size_t vlen; int f = 0;
            CHECK_OK(obj_get_field(r.data, r.len, (const uint8_t *)"success", 7, &v, &vlen, &f));
            CHECK_I64(f, 1);
            CHECK(vlen >= 1 && v[0] == (cases[i].ok ? BJ_TYPE_TRUE : BJ_TYPE_FALSE));
            f = 0;
            CHECK_OK(obj_get_field(r.data, r.len, (const uint8_t *)"restart", 7, &v, &vlen, &f));
            CHECK_I64(f, cases[i].want_restart ? 1 : 0);
            f = 0;
            CHECK_OK(obj_get_field(r.data, r.len, (const uint8_t *)"term", 4, &v, &vlen, &f));
            CHECK_I64(f, 1);
            dbuf_free(&r);
        }
    }

    /*
     * ---- and the shape src/raft.js ACTUALLY sends, built here by hand
     * exactly as _sendSnapshot builds it. This is the cross-check that
     * matters: the round trips above would all pass with a field
     * misspelled on both sides, and the first symptom would be a
     * follower silently ignoring every chunk of a real transfer.
     *
     *   { kind, term, leaderId, lastIncludedIndex, lastIncludedTerm,
     *     role, offset, data, done, manifest? }
     */
    {
        doc *js = doc_new();
        doc_str(js, "kind", "installSnapshot");
        doc_int(js, "term", 4);
        doc_int(js, "leaderId", 2);
        doc_int(js, "lastIncludedIndex", 17);
        doc_int(js, "lastIncludedTerm", 3);
        doc_str(js, "role", "primary");
        doc_int(js, "offset", 8);
        doc_key(js, "data");
        bj_put_binary(js->b, chunk, (uint32_t)sizeof chunk);
        doc_key(js, "done");
        bj_put_bool(js->b, 1);
        doc_begin_obj(js, "manifest");
        doc_key(js, "config");
        bj_put_null(js->b);
        doc_begin_arr(js, "files");
        bj_begin_object(js->b);
        doc_str(js, "role", "primary");
        doc_int(js, "size", 12);
        doc_int(js, "crc", 99);
        bj_end_object(js->b);
        doc_end_arr(js);
        doc_end_obj(js);
        uint32_t jlen; const uint8_t *jbuf = doc_done(js, &jlen);

        raft_install in;
        CHECK_OK(rmsg_install_read(jbuf, jlen, &in));
        CHECK_I64((int64_t)in.term, 4);
        CHECK_I64((int64_t)in.leader_id, 2);
        CHECK_I64((int64_t)in.last_included_index, 17);
        CHECK_I64((int64_t)in.last_included_term, 3);
        CHECK_I64((int64_t)in.offset, 8);
        CHECK_I64(in.done, 1);
        CHECK(in.role && in.role_len == 7 && memcmp(in.role, "primary", 7) == 0);
        CHECK_I64((int64_t)in.data_len, (int64_t)sizeof chunk);
        CHECK(in.manifest != NULL);
        /* And the manifest that comes back is a manifest -- the span is
         * handed to sst_check_files, which reads `files` out of it. */
        {
            const uint8_t *v; size_t vlen; int f = 0;
            CHECK_OK(obj_get_field(in.manifest, in.manifest_len,
                                   (const uint8_t *)"files", 5, &v, &vlen, &f));
            CHECK_I64(f, 1);
        }
        doc_free(js);
    }

    /* ---- what it refuses. A message of another kind is not an install,
     * however well-formed, and one that names no sender cannot be
     * answered -- 0 is "nobody" everywhere in this grammar. */
    {
        dbuf other = {0};
        CHECK_OK(rmsg_build_request_vote(1, 2, 3, 4, 0, &other));
        raft_install in;
        CHECK_RC(rmsg_install_read(other.data, (uint32_t)other.len, &in), RAFT_ERR_MESSAGE);
        dbuf_free(&other);

        dbuf anon = {0};
        CHECK_OK(rmsg_build_install_snapshot(7, 0, 42, 5, NULL, 0, 0,
                                             NULL, 0, 1, NULL, 0, &anon));
        CHECK_RC(rmsg_install_read(anon.data, (uint32_t)anon.len, &in), RAFT_ERR_MESSAGE);
        dbuf_free(&anon);
    }

    doc_free(mf);
}

TEST(raft_append_entries_round_trips_between_two_logs) {
    /*
     * A leader frames a batch straight out of its log; a follower
     * accepts it straight into its own. Neither side ever holds the
     * entries as anything but the bytes in the message -- which is what
     * removed the decode/re-encode the JavaScript path used to pay on
     * every AppendEntries.
     */
    follower leader, peer;
    CHECK_FATAL(follower_open(&leader, 3, 5) == 0);
    CHECK_FATAL(follower_open(&peer, 3, 2) == 0);

    raft_msg_state st = msg_state();
    raft_msg_effect eff;
    dbuf msg = {0}, reply = {0};

    /* The follower holds 2 of the leader's 5; resume at 3. */
    uint64_t prev_term = 0;
    CHECK_OK(elog_term_at(leader.log, 2, &prev_term));
    uint32_t count = 0;
    CHECK_OK(rmsg_build_append_entries(leader.log, 3, /*leaderId*/ 1, /*next*/ 3,
                                       prev_term, /*leaderCommit*/ 5, 65536, 0, &count, &msg));
    CHECK_I64((long long)count, 3);

    CHECK_OK(rmsg_handle_append_entries(peer.log, &st, msg.data, (uint32_t)msg.len, &eff, &reply));
    CHECK_I64(eff.success, 1);
    CHECK_I64((long long)eff.match_index, 5);
    CHECK_I64((long long)elog_last_index(peer.log), 5);
    /* The leader's commit index travels with the message, bounded by
     * what the follower now actually holds. */
    CHECK_I64((long long)eff.new_commit_index, 5);
    CHECK_I64(eff.touched_leader, 1);
    CHECK_I64((long long)eff.new_leader_id, 1);
    dbuf_free(&reply); msg.len = 0;

    /* Replaying the identical message changes nothing: every entry is
     * already held at the same term. Overlapping AppendEntries are
     * ordinary, and truncating on one would discard entries the leader
     * believes are replicated. */
    CHECK_OK(rmsg_build_append_entries(leader.log, 3, 1, 3, prev_term, 5, 65536, 0, &count, &msg));
    CHECK_OK(rmsg_handle_append_entries(peer.log, &st, msg.data, (uint32_t)msg.len, &eff, &reply));
    CHECK_I64(eff.success, 1);
    CHECK_I64((long long)eff.truncated_from, 0);
    CHECK_I64((long long)elog_last_index(peer.log), 5);
    dbuf_free(&reply); msg.len = 0;

    /* A prevLogIndex past the follower's tail is rejected with the hint
     * that lets the leader resume in one round trip. */
    CHECK_OK(rmsg_build_append_entries(leader.log, 3, 1, 9, prev_term, 5, 65536, 0, &count, &msg));
    CHECK_OK(rmsg_handle_append_entries(peer.log, &st, msg.data, (uint32_t)msg.len, &eff, &reply));
    CHECK_I64(eff.success, 0);
    {
        const uint8_t *v; size_t vlen; int found = 0;
        CHECK_OK(obj_get_field(reply.data, reply.len, (const uint8_t *)"hintIndex", 9,
                               &v, &vlen, &found));
        CHECK_I64(found, 1);
        cur c = { v, vlen, 0 };
        double d;
        CHECK_OK(read_number(&c, &d));
        CHECK_I64((long long)d, 6);
    }
    /* A rejection still refreshes the election timer: the leader is
     * current, only misaligned. */
    CHECK_I64(eff.reset_election_timer, 1);
    dbuf_free(&reply); msg.len = 0;

    /* A leader from the past gets our term and NOTHING else. */
    bj_builder *stale = bj_builder_new();
    bj_begin_object(stale);
    msg_kind(stale, "appendEntries");
    msg_kv_int(stale, "term", 1);
    msg_kv_int(stale, "leaderId", 1);
    msg_kv_int(stale, "prevLogIndex", 0);
    msg_kv_int(stale, "prevLogTerm", 0);
    msg_kv_int(stale, "leaderCommit", 0);
    bj_end_object(stale);
    size_t slen; const uint8_t *sbuf = bj_builder_data(stale, &slen);
    CHECK_OK(rmsg_handle_append_entries(peer.log, &st, sbuf, (uint32_t)slen, &eff, &reply));
    CHECK_I64(eff.success, 0);
    CHECK_I64(eff.touched_leader, 0);
    CHECK_I64(eff.reset_election_timer, 0);
    dbuf_free(&reply);

    bj_builder_free(stale);
    dbuf_free(&msg);
    follower_close(&peer);
    follower_close(&leader);
}

/* ---- snapshot store policy (snapstore.h) ------------------------------ */

/* A NUL-separated listing, and a parallel size array, built by hand
 * because that is exactly the shape a host passes in. */
typedef struct { dbuf names; const char *n_at[64]; double sizes[64]; uint32_t n; } dirlist;

static void dirlist_add(dirlist *l, const char *name, double size) {
    dbuf_put(&l->names, (const uint8_t *)name, strlen(name));
    dbuf_put(&l->names, (const uint8_t *)"", 1);
    l->n_at[l->n] = name;
    l->sizes[l->n++] = size;
}

/* Play the host's half of the two-beat open: read the manifest the store
 * asks for, size the files it then names, and report which candidate (if
 * any) was adopted. `manifests` maps candidate manifest name -> bytes. */
static int dirlist_size_of(const dirlist *l, const char *name, uint32_t len) {
    for (uint32_t i = 0; i < l->n; i++) {
        if (strlen(l->n_at[i]) == len && memcmp(l->n_at[i], name, len) == 0) return i;
    }
    return -1;
}

typedef struct { const char *name; const dbuf *bytes; } manifest_src;

static int run_open(sst *s, const dirlist *l, const manifest_src *srcs, uint32_t n_srcs) {
    for (uint32_t i = 0; i < sst_candidate_count(s); i++) {
        uint32_t nlen; const char *name = sst_candidate_manifest(s, i, &nlen);
        const dbuf *bytes = NULL;
        for (uint32_t k = 0; k < n_srcs; k++) {
            if (strlen(srcs[k].name) == nlen && memcmp(srcs[k].name, name, nlen) == 0) {
                bytes = srcs[k].bytes; break;
            }
        }
        if (!bytes) continue;
        if (sst_try_manifest(s, i, bytes->data, (uint32_t)bytes->len) != BJ_OK) continue;

        double sizes[64];
        uint32_t pc = sst_pending_count(s);
        for (uint32_t k = 0; k < pc && k < 64; k++) {
            uint32_t plen; const char *pn = sst_pending_name(s, k, &plen);
            int at = dirlist_size_of(l, pn, plen);
            sizes[k] = at < 0 ? -1 : l->sizes[at];
        }
        if (sst_confirm(s, sizes, pc) == BJ_OK) return (int)i;
    }
    return -1;
}
static void dirlist_free(dirlist *l) { dbuf_free(&l->names); }

/* Is `name` one of the NUL-separated names in `buf`? */
static int dirlist_has(const dbuf *buf, const char *name) {
    size_t want = strlen(name), at = 0;
    while (at < buf->len) {
        size_t end = at;
        while (end < buf->len && buf->data[end] != '\0') end++;
        if (end - at == want && memcmp(buf->data + at, name, want) == 0) return 1;
        at = end + 1;
    }
    return 0;
}
static uint32_t dirlist_count(const dbuf *buf) {
    uint32_t n = 0;
    for (size_t at = 0; at < buf->len; at++) if (buf->data[at] == '\0') n++;
    return n;
}
static const char *dirlist_at(const dbuf *buf, uint32_t i) {
    uint32_t seen = 0;
    size_t at = 0;
    while (at < buf->len) {
        if (seen == i) return (const char *)buf->data + at;
        while (at < buf->len && buf->data[at] != '\0') at++;
        at++;
        seen++;
    }
    return NULL;
}

/* {files: [{role, name, size, crc}, ...]} for one role -- what a host
 * accumulates as it writes and checksums each generation file. */
static void files_entry(bj_builder *b, const char *role, const char *name,
                        int64_t size, int64_t crc) {
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"role", 4);
    bj_put_string(b, (const uint8_t *)role, (uint32_t)strlen(role));
    if (name) {
        bj_put_key(b, (const uint8_t *)"name", 4);
        bj_put_string(b, (const uint8_t *)name, (uint32_t)strlen(name));
    }
    bj_put_key(b, (const uint8_t *)"size", 4);
    bj_put_int(b, size);
    bj_put_key(b, (const uint8_t *)"crc", 3);
    bj_put_int(b, crc);
    bj_end_object(b);
}

TEST(snapshot_names_round_trip_through_the_scanner) {
    /* Whatever the store names, the store must recognize -- a generation
     * whose files it writes and then cannot classify is one it sweeps at
     * the next open, silently losing a snapshot it just took. */
    sst *s = sst_new("snap", 4);
    CHECK_FATAL(s != NULL);

    dbuf m = {0}, d = {0}, lg = {0};
    CHECK_OK(sst_manifest_name(s, 7, &m));
    CHECK_OK(sst_data_name(s, 7, "f0", 2, &d));
    CHECK_OK(sst_log_name(s, 7, &lg));
    CHECK(m.len == strlen("snap-7.manifest.bj") && memcmp(m.data, "snap-7.manifest.bj", m.len) == 0);
    CHECK(d.len == strlen("snap-7-f0.bj") && memcmp(d.data, "snap-7-f0.bj", d.len) == 0);
    CHECK(lg.len == strlen("snap-log-7.bj") && memcmp(lg.data, "snap-log-7.bj", lg.len) == 0);

    dirlist l;
    memset(&l, 0, sizeof(l));
    dirlist_add(&l, "snap-7.manifest.bj", 100);
    dirlist_add(&l, "snap-7-f0.bj", 200);
    dirlist_add(&l, "snap-log-7.bj", 300);
    /* Files this store does not own must be left entirely alone: the
     * snapshot directory is the database's directory. */
    dirlist_add(&l, "catalog.bj", 10);
    dirlist_add(&l, "coll-users.bj", 20);
    dirlist_add(&l, "othersnap-3.manifest.bj", 30);
    dirlist_add(&l, "snap-01-f0.bj", 40);      /* leading zero: not ours */
    CHECK_OK(sst_scan(s, l.names.data, (uint32_t)l.names.len));

    /* The paired log's generation counts toward next_gen even though the
     * log has its own lifecycle -- reusing 7 would put a fresh snapshot
     * behind a stale log. */
    CHECK_I64((long long)sst_next_gen(s), 8);
    CHECK_I64((long long)sst_candidate_count(s), 1);
    uint32_t clen; const char *cand = sst_candidate_manifest(s, 0, &clen);
    CHECK(cand && clen == m.len && memcmp(cand, m.data, clen) == 0);

    /* Nothing adopted yet, so the sweep would take generation 7 -- but
     * never the foreign files. */
    dbuf sweep = {0};
    CHECK_OK(sst_sweep_plan(s, &sweep));
    CHECK(dirlist_has(&sweep, "snap-7.manifest.bj"));
    CHECK(dirlist_has(&sweep, "snap-7-f0.bj"));
    CHECK(!dirlist_has(&sweep, "catalog.bj"));
    CHECK(!dirlist_has(&sweep, "coll-users.bj"));
    CHECK(!dirlist_has(&sweep, "othersnap-3.manifest.bj"));
    CHECK(!dirlist_has(&sweep, "snap-01-f0.bj"));
    CHECK(!dirlist_has(&sweep, "snap-log-7.bj"));   /* logs are pruned separately */

    /* A role that cannot be a filename is refused at naming time. */
    dbuf bad = {0};
    CHECK_RC(sst_data_name(s, 7, "has/slash", 9, &bad), SST_ERR_ROLE);
    CHECK_RC(sst_data_name(s, 7, "", 0, &bad), SST_ERR_ROLE);
    dbuf_free(&bad);

    dbuf_free(&sweep); dbuf_free(&lg); dbuf_free(&d); dbuf_free(&m);
    dirlist_free(&l);
    sst_free(s);
}

TEST(snapshot_adopts_the_newest_generation_that_committed) {
    /*
     * The commit protocol: the manifest is written LAST and its validity
     * IS the commit. So a crashed attempt (data files, no manifest) and a
     * torn manifest must both fail to adopt, and adoption must fall back
     * to the older generation rather than to nothing.
     */
    sst *s = sst_new("snap", 4);
    CHECK_FATAL(s != NULL);

    /* Generation 1: complete. */
    bj_builder *fb = bj_builder_new();
    bj_begin_array(fb);
    files_entry(fb, "f0", "snap-1-f0.bj", 200, 0xabcd);
    bj_end_array(fb);
    size_t flen; const uint8_t *fbuf = bj_builder_data(fb, &flen);
    dbuf man1 = {0};
    CHECK_OK(sst_manifest_encode(10, 3, NULL, 0, fbuf, (uint32_t)flen, &man1));

    /* Generation 2: a manifest whose last byte was lost. */
    bj_builder *fb2 = bj_builder_new();
    bj_begin_array(fb2);
    files_entry(fb2, "f0", "snap-2-f0.bj", 400, 0x1234);
    bj_end_array(fb2);
    size_t f2len; const uint8_t *f2buf = bj_builder_data(fb2, &f2len);
    dbuf man2 = {0};
    CHECK_OK(sst_manifest_encode(20, 4, NULL, 0, f2buf, (uint32_t)f2len, &man2));
    dbuf torn = {0};
    dbuf_put(&torn, man2.data, man2.len - 1);

    dirlist l;
    memset(&l, 0, sizeof(l));
    dirlist_add(&l, "snap-1.manifest.bj", (double)man1.len);
    dirlist_add(&l, "snap-1-f0.bj", 200);
    dirlist_add(&l, "snap-2.manifest.bj", (double)torn.len);
    dirlist_add(&l, "snap-2-f0.bj", 400);
    /* Generation 3 crashed before writing its manifest at all. */
    dirlist_add(&l, "snap-3-f0.bj", 999);
    CHECK_OK(sst_scan(s, l.names.data, (uint32_t)l.names.len));

    /* Candidates are manifest-bearing generations, newest first; the
     * crashed generation 3 is not among them. */
    CHECK_I64((long long)sst_candidate_count(s), 2);
    CHECK_I64((long long)sst_next_gen(s), 4);

    manifest_src srcs[] = {
        { "snap-1.manifest.bj", &man1 },
        { "snap-2.manifest.bj", &torn }
    };
    int adopted = run_open(s, &l, srcs, 2);
    CHECK_I64(adopted, 1);            /* generation 2 refused, 1 adopted */
    CHECK_I64((long long)sst_latest_gen(s), 1);
    CHECK_I64(sst_has_latest(s), 1);

    /* The sweep takes everything but the adopted generation: the torn
     * attempt AND the manifest-less one. */
    dbuf sweep = {0};
    CHECK_OK(sst_sweep_plan(s, &sweep));
    CHECK(!dirlist_has(&sweep, "snap-1.manifest.bj"));
    CHECK(!dirlist_has(&sweep, "snap-1-f0.bj"));
    CHECK(dirlist_has(&sweep, "snap-2.manifest.bj"));
    CHECK(dirlist_has(&sweep, "snap-2-f0.bj"));
    CHECK(dirlist_has(&sweep, "snap-3-f0.bj"));

    /* What the host gets back: the manifest plus the generation number,
     * which lives in the filenames rather than inside the record. */
    dbuf latest = {0};
    int has = 0;
    CHECK_OK(sst_latest(s, &latest, &has));
    CHECK_I64(has, 1);
    {
        const uint8_t *v; size_t vlen; int found = 0;
        CHECK_OK(obj_get_field(latest.data, latest.len, (const uint8_t *)"gen", 3, &v, &vlen, &found));
        CHECK_I64(found, 1);
        CHECK_OK(obj_get_field(latest.data, latest.len,
                               (const uint8_t *)"lastIncludedIndex", 17, &v, &vlen, &found));
        CHECK_I64(found, 1);
    }
    dbuf_free(&latest);

    dbuf_free(&sweep); dbuf_free(&torn); dbuf_free(&man2); dbuf_free(&man1);
    bj_builder_free(fb2); bj_builder_free(fb);
    dirlist_free(&l);
    sst_free(s);
}

TEST(snapshot_refuses_a_manifest_whose_files_are_not_there) {
    /* A manifest validating is necessary but not sufficient: every file
     * it names must be present at its recorded length. A generation that
     * lost a data file must fall back, not adopt and fail later. */
    sst *s = sst_new("snap", 4);
    CHECK_FATAL(s != NULL);

    bj_builder *fb = bj_builder_new();
    bj_begin_array(fb);
    files_entry(fb, "f0", "snap-1-f0.bj", 200, 0xabcd);
    files_entry(fb, "f1", "snap-1-f1.bj", 300, 0xbeef);
    bj_end_array(fb);
    size_t flen; const uint8_t *fbuf = bj_builder_data(fb, &flen);
    dbuf man = {0};
    CHECK_OK(sst_manifest_encode(10, 3, NULL, 0, fbuf, (uint32_t)flen, &man));

    /* f1 is missing entirely. */
    dirlist a;
    memset(&a, 0, sizeof(a));
    dirlist_add(&a, "snap-1.manifest.bj", (double)man.len);
    dirlist_add(&a, "snap-1-f0.bj", 200);
    CHECK_OK(sst_scan(s, a.names.data, (uint32_t)a.names.len));
    manifest_src src[] = { { "snap-1.manifest.bj", &man } };
    CHECK_I64(run_open(s, &a, src, 1), -1);
    CHECK_I64(sst_has_latest(s), 0);
    dirlist_free(&a);

    /* f1 is present but the wrong length -- a truncated write. */
    dirlist b;
    memset(&b, 0, sizeof(b));
    dirlist_add(&b, "snap-1.manifest.bj", (double)man.len);
    dirlist_add(&b, "snap-1-f0.bj", 200);
    dirlist_add(&b, "snap-1-f1.bj", 299);
    CHECK_OK(sst_scan(s, b.names.data, (uint32_t)b.names.len));
    CHECK_I64(run_open(s, &b, src, 1), -1);
    CHECK_I64(sst_has_latest(s), 0);
    dirlist_free(&b);

    /* Both present and correct: adopted. */
    dirlist c;
    memset(&c, 0, sizeof(c));
    dirlist_add(&c, "snap-1.manifest.bj", (double)man.len);
    dirlist_add(&c, "snap-1-f0.bj", 200);
    dirlist_add(&c, "snap-1-f1.bj", 300);
    CHECK_OK(sst_scan(s, c.names.data, (uint32_t)c.names.len));
    CHECK_I64(run_open(s, &c, src, 1), 0);
    CHECK_I64(sst_has_latest(s), 1);
    dirlist_free(&c);

    dbuf_free(&man);
    bj_builder_free(fb);
    sst_free(s);
}

TEST(snapshot_validates_transferred_files_against_the_leaders_manifest) {
    /*
     * The rule that was written three times: the JS store's verify(), the
     * replicated install path and the Raft harness each had their own
     * copy. A follower deciding whether a transferred snapshot is intact
     * is not a place for three opinions -- one of them being subtly
     * wrong means adopting corrupt state and diverging silently.
     *
     * A leader's manifest names roles, not the follower's filenames, so
     * `name` is optional throughout.
     */
    bj_builder *want = bj_builder_new();
    bj_begin_array(want);
    files_entry(want, "f0", NULL, 200, 0xabcd);
    files_entry(want, "f1", NULL, 300, 0xbeef);
    bj_end_array(want);
    size_t wlen; const uint8_t *wbuf = bj_builder_data(want, &wlen);
    dbuf man = {0};
    CHECK_OK(sst_manifest_encode(10, 3, NULL, 0, wbuf, (uint32_t)wlen, &man));
    /* Validation reads the record, not the CRC-tailed file. */
    const uint8_t *body = man.data;
    uint32_t body_len = (uint32_t)man.len - 4;

    const uint8_t *bad; uint32_t bad_len;

    /* Exactly what was promised. Order need not match: chunks arrive in
     * whatever order the transfer produced them. */
    bj_builder *ok = bj_builder_new();
    bj_begin_array(ok);
    files_entry(ok, "f1", NULL, 300, 0xbeef);
    files_entry(ok, "f0", NULL, 200, 0xabcd);
    bj_end_array(ok);
    size_t olen; const uint8_t *obuf = bj_builder_data(ok, &olen);
    CHECK_OK(sst_check_files(body, body_len, obuf, (uint32_t)olen, &bad, &bad_len));

    /* Right length, wrong bytes: the case a size check alone would miss,
     * and the reason a CRC is carried at all. */
    bj_builder *corrupt = bj_builder_new();
    bj_begin_array(corrupt);
    files_entry(corrupt, "f0", NULL, 200, 0xabcd);
    files_entry(corrupt, "f1", NULL, 300, 0x9999);
    bj_end_array(corrupt);
    size_t clen; const uint8_t *cbuf = bj_builder_data(corrupt, &clen);
    CHECK_RC(sst_check_files(body, body_len, cbuf, (uint32_t)clen, &bad, &bad_len), SST_ERR_CHECKSUM);
    CHECK(bad && bad_len == 2 && memcmp(bad, "f1", 2) == 0);

    /* A file that never arrived is a failure, not an omission: an install
     * missing a structure has not received the snapshot. */
    bj_builder *partial = bj_builder_new();
    bj_begin_array(partial);
    files_entry(partial, "f0", NULL, 200, 0xabcd);
    bj_end_array(partial);
    size_t plen; const uint8_t *pbuf = bj_builder_data(partial, &plen);
    CHECK_RC(sst_check_files(body, body_len, pbuf, (uint32_t)plen, &bad, &bad_len), SST_ERR_CHECKSUM);
    CHECK(bad && bad_len == 2 && memcmp(bad, "f1", 2) == 0);

    /* A prefix-less store: what the standalone validator in
     * structures-core.js creates, since checking bytes against a
     * manifest needs no filenames at all. */
    sst *bare = sst_new("", 0);
    CHECK_FATAL(bare != NULL);
    CHECK_OK(sst_check_files(body, body_len, obuf, (uint32_t)olen, &bad, &bad_len));
    sst_free(bare);

    bj_builder_free(partial); bj_builder_free(corrupt); bj_builder_free(ok);
    dbuf_free(&man);
    bj_builder_free(want);
}

TEST(snapshot_commit_supersedes_the_previous_generation) {
    sst *s = sst_new("snap", 4);
    CHECK_FATAL(s != NULL);

    bj_builder *f1 = bj_builder_new();
    bj_begin_array(f1);
    files_entry(f1, "f0", "snap-1-f0.bj", 200, 0xabcd);
    bj_end_array(f1);
    size_t l1; const uint8_t *b1 = bj_builder_data(f1, &l1);
    dbuf man1 = {0};
    CHECK_OK(sst_manifest_encode(10, 3, NULL, 0, b1, (uint32_t)l1, &man1));

    dirlist l;
    memset(&l, 0, sizeof(l));
    dirlist_add(&l, "snap-1.manifest.bj", (double)man1.len);
    dirlist_add(&l, "snap-1-f0.bj", 200);
    CHECK_OK(sst_scan(s, l.names.data, (uint32_t)l.names.len));
    manifest_src srcs[] = { { "snap-1.manifest.bj", &man1 } };
    CHECK_I64(run_open(s, &l, srcs, 1), 0);
    CHECK_I64((long long)sst_next_gen(s), 2);

    /* Commit generation 2. The predecessor's files come back to delete --
     * and only AFTER the new manifest is durable, which is what makes a
     * crash in this window leave an openable snapshot behind. */
    bj_builder *f2 = bj_builder_new();
    bj_begin_array(f2);
    files_entry(f2, "f0", "snap-2-f0.bj", 400, 0x1234);
    files_entry(f2, "f1", "snap-2-f1.bj", 500, 0x5678);
    bj_end_array(f2);
    size_t l2; const uint8_t *b2 = bj_builder_data(f2, &l2);
    dbuf man2 = {0};
    CHECK_OK(sst_manifest_encode(20, 4, NULL, 0, b2, (uint32_t)l2, &man2));

    dbuf sweep = {0};
    CHECK_OK(sst_adopt_committed(s, 2, man2.data, (uint32_t)man2.len, &sweep));
    CHECK_I64((long long)sst_latest_gen(s), 2);
    CHECK_I64((long long)sst_next_gen(s), 3);
    CHECK_I64((long long)dirlist_count(&sweep), 2);
    CHECK(dirlist_has(&sweep, "snap-1.manifest.bj"));
    CHECK(dirlist_has(&sweep, "snap-1-f0.bj"));
    CHECK(!dirlist_has(&sweep, "snap-2-f0.bj"));

    /* A duplicated role would produce two generations' worth of one file
     * name; caught at encode, not at the next open. */
    bj_builder *dup = bj_builder_new();
    bj_begin_array(dup);
    files_entry(dup, "f0", NULL, 1, 1);
    files_entry(dup, "f0", NULL, 2, 2);
    bj_end_array(dup);
    size_t dlen; const uint8_t *dbufp = bj_builder_data(dup, &dlen);
    dbuf out = {0};
    CHECK_RC(sst_manifest_encode(1, 1, NULL, 0, dbufp, (uint32_t)dlen, &out), SST_ERR_ROLE);
    dbuf_free(&out);

    bj_builder *badrole = bj_builder_new();
    bj_begin_array(badrole);
    files_entry(badrole, "f/0", NULL, 1, 1);
    bj_end_array(badrole);
    const uint8_t *brbuf = bj_builder_data(badrole, &dlen);
    dbuf out2 = {0};
    CHECK_RC(sst_manifest_encode(1, 1, NULL, 0, brbuf, (uint32_t)dlen, &out2), SST_ERR_ROLE);
    dbuf_free(&out2);

    bj_builder_free(badrole); bj_builder_free(dup);
    dbuf_free(&sweep); dbuf_free(&man2); dbuf_free(&man1);
    bj_builder_free(f2); bj_builder_free(f1);
    dirlist_free(&l);
    sst_free(s);
}

TEST(snapshot_log_candidates_are_newest_first) {
    /* A crash mid-compaction leaves a torn newest log. The host tries
     * each candidate with elog_open and takes the first that works, so
     * the ORDER is the fallback policy -- getting it backwards would
     * adopt an obsolete log and replay from too far back. */
    sst *s = sst_new("snap", 4);
    CHECK_FATAL(s != NULL);

    dirlist l;
    memset(&l, 0, sizeof(l));
    dirlist_add(&l, "snap-log-2.bj", 10);
    dirlist_add(&l, "snap-log-11.bj", 10);   /* lexically before "-2", numerically after */
    dirlist_add(&l, "snap-log-7.bj", 10);
    dirlist_add(&l, "snap-3-f0.bj", 10);
    dirlist_add(&l, "othersnap-log-9.bj", 10);
    dirlist_add(&l, "__wal__.bj", 10);

    dbuf out = {0};
    CHECK_OK(sst_log_candidates(s, l.names.data, (uint32_t)l.names.len, &out));
    CHECK_I64((long long)dirlist_count(&out), 3);
    CHECK_STR(dirlist_at(&out, 0), "snap-log-11.bj");
    CHECK_STR(dirlist_at(&out, 1), "snap-log-7.bj");
    CHECK_STR(dirlist_at(&out, 2), "snap-log-2.bj");
    dbuf_free(&out);

    /* Pruning keeps exactly one. */
    dbuf prune = {0};
    CHECK_OK(sst_prune_logs_plan(s, l.names.data, (uint32_t)l.names.len,
                                 "snap-log-11.bj", 14, &prune));
    CHECK_I64((long long)dirlist_count(&prune), 2);
    CHECK(!dirlist_has(&prune, "snap-log-11.bj"));
    CHECK(dirlist_has(&prune, "snap-log-7.bj"));
    CHECK(dirlist_has(&prune, "snap-log-2.bj"));
    CHECK(!dirlist_has(&prune, "othersnap-log-9.bj"));
    CHECK(!dirlist_has(&prune, "__wal__.bj"));
    dbuf_free(&prune);

    dirlist_free(&l);
    sst_free(s);
}

/* ---- WAL command grammar and planner (db_wal.h) ----------------------- */

/* Has `cmd` a top-level field called `name`? The invariant tests below are
 * all of the form "this field must (not) be there". */
static int cmd_has(const uint8_t *cmd, uint32_t len, const char *name) {
    const uint8_t *vp; size_t vlen; int found = 0;
    if (obj_get_field(cmd, len, (const uint8_t *)name, (uint32_t)strlen(name),
                      &vp, &vlen, &found) != BJ_OK) return -1;
    return found;
}

/* The 12 id bytes of `cmd`'s top-level `id` field, or 0. */
static const uint8_t *cmd_id(const uint8_t *cmd, uint32_t len) {
    const uint8_t *vp; size_t vlen; int found = 0;
    if (obj_get_field(cmd, len, (const uint8_t *)"id", 2, &vp, &vlen, &found) != BJ_OK) return NULL;
    if (!found || vlen != 13 || vp[0] != BJ_TYPE_OID) return NULL;
    return vp + 1;
}

/* {$set: {seen: true}} */
static doc *set_seen(void) {
    doc *u = doc_new();
    doc_begin_obj(u, "$set");
    doc_key(u, "seen");
    bj_put_bool(u->b, 1);
    doc_end_obj(u);
    return u;
}

TEST(wal_grammar_round_trips_every_op_it_can_emit) {
    /* Whatever the planner emits, the parser must accept and identify --
     * otherwise a command can be written to the log that no replica can
     * replay, which is the one failure this layer exists to prevent. */
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));

    uint8_t did[12]; mk_oid(did, 90);
    doc *idq = doc_new(); { uint8_t id[12]; mk_oid(id, 1); doc_oid(idq, "_id", id); }
    uint32_t idqlen; const uint8_t *idqbuf = doc_done(idq, &idqlen);
    doc *u = set_seen();
    uint32_t ulen; const uint8_t *ubuf = doc_done(u, &ulen);
    doc *empty = doc_new();
    uint32_t elen; const uint8_t *ebuf = doc_done(empty, &elen);
    doc *nomatch = doc_new(); doc_str(nomatch, "team", "ghosts");
    uint32_t nlen; const uint8_t *nbuf = doc_done(nomatch, &nlen);

    /* One request per opcode the grammar has, and the opcode each must
     * produce. INSERT appears twice on purpose: once asked for directly,
     * once as what an upsert resolves to. */
    struct { int req; const uint8_t *a; uint32_t a_len;
             const uint8_t *b; uint32_t b_len; int upsert; int want_op; } cases[] = {
        { DC_WREQ_UPDATE_ONE,      idqbuf, idqlen, ubuf, ulen, 0, DC_WAL_UPDATE },
        { DC_WREQ_REPLACE_ONE,     idqbuf, idqlen, ebuf, elen, 0, DC_WAL_REPLACE },
        { DC_WREQ_DELETE_ONE,      idqbuf, idqlen, NULL, 0,    0, DC_WAL_DELETE },
        { DC_WREQ_CREATE_INDEX,    ebuf,   elen,   ebuf, elen, 0, DC_WAL_CREATE_INDEX },
        { DC_WREQ_DROP_INDEX,      (const uint8_t *)"ix_a", 4, NULL, 0, 0, DC_WAL_DROP_INDEX },
        { DC_WREQ_DROP_COLLECTION, NULL,   0,      NULL, 0,    0, DC_WAL_DROP_COLLECTION },
        /* No match + upsert: the upsert opcodes are gone, so this is an
         * ordinary insert by the time it reaches the log. */
        { DC_WREQ_UPDATE_ONE,      nbuf,   nlen,   ubuf, ulen, 1, DC_WAL_INSERT },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        dc_wal_plan *p = NULL;
        int rc = dc_wal_plan_build(fx.coll, "people", 6, cases[i].req,
                                   cases[i].a, cases[i].a_len,
                                   cases[i].b, cases[i].b_len,
                                   cases[i].upsert, did, &p);
        if (rc != BJ_OK) { TAP_FAIL("case %zu: plan failed rc=%d", i, rc); continue; }
        CHECK_I64((long long)dc_wal_plan_count(p), 1);
        uint32_t len; const uint8_t *cmd = dc_wal_plan_cmd(p, 0, &len);
        if (cmd) {
            int op = -1; const uint8_t *coll; uint32_t coll_len;
            CHECK_OK(dc_wal_parse(cmd, len, &op, &coll, &coll_len));
            CHECK_I64(op, cases[i].want_op);
            CHECK_I64(coll_len, 6);
            CHECK(coll && memcmp(coll, "people", 6) == 0);
        }
        dc_wal_plan_free(p);
    }

    doc_free(nomatch); doc_free(empty); doc_free(u); doc_free(idq);
    fx_close(&fx);
}

TEST(wal_grammar_refuses_what_it_cannot_replay) {
    int op = -1; const uint8_t *coll; uint32_t coll_len;

    /* An op this version does not know. Rejected rather than ignored: a
     * follower that skips an entry it does not understand has silently
     * diverged from one that does. */
    doc *unknown = doc_new();
    doc_str(unknown, "c", "people");
    doc_str(unknown, "op", "teleport");
    uint32_t ulen; const uint8_t *ubuf = doc_done(unknown, &ulen);
    CHECK_RC(dc_wal_parse(ubuf, ulen, &op, &coll, &coll_len), DC_ERR_WAL_UNKNOWN_OP);

    /* The old upsert opcodes are among the ops this version does not
     * know -- they carried a filter into the log, which the grammar no
     * longer permits (db_wal.h). */
    doc *old = doc_new();
    doc_str(old, "c", "people");
    doc_str(old, "op", "uu");
    uint32_t olen; const uint8_t *obuf = doc_done(old, &olen);
    CHECK_RC(dc_wal_parse(obuf, olen, &op, &coll, &coll_len), DC_ERR_WAL_UNKNOWN_OP);

    /* A known op missing the field it needs. */
    doc *torn = doc_new();
    doc_str(torn, "c", "people");
    doc_str(torn, "op", "d");        /* DELETE, but no id */
    uint32_t tlen; const uint8_t *tbuf = doc_done(torn, &tlen);
    CHECK_RC(dc_wal_parse(tbuf, tlen, &op, &coll, &coll_len), DC_ERR_WAL_MISSING_FIELD);

    /* An id of the wrong type is missing as far as the grammar cares. */
    doc *wrong = doc_new();
    doc_str(wrong, "c", "people");
    doc_str(wrong, "op", "d");
    doc_str(wrong, "id", "not-an-oid");
    uint32_t wlen; const uint8_t *wbuf = doc_done(wrong, &wlen);
    CHECK_RC(dc_wal_parse(wbuf, wlen, &op, &coll, &coll_len), DC_ERR_WAL_MISSING_FIELD);

    /* No collection at all: the applier would have nowhere to send it. */
    doc *nc = doc_new();
    doc_str(nc, "op", "dropCollection");
    uint32_t nlen; const uint8_t *nbuf = doc_done(nc, &nlen);
    CHECK_RC(dc_wal_parse(nbuf, nlen, &op, &coll, &coll_len), DC_ERR_WAL_MISSING_FIELD);

    /* A rejected command reports no opcode -- a caller that ignores the
     * return value must not find a plausible one sitting in `op`. */
    CHECK_I64(op, -1);

    doc_free(nc); doc_free(wrong); doc_free(torn); doc_free(old); doc_free(unknown);
}

/* One numeric field of an apply result, or -1. */
static int64_t result_int(const dbuf *r, const char *key) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(r->data, r->len, (const uint8_t *)key,
                      (uint32_t)strlen(key), &v, &vlen, &found) || !found) return -1;
    cur c = { v, vlen, 0 };
    double d;
    if (read_number(&c, &d) != BJ_OK) return -1;
    return (int64_t)d;
}

TEST(a_logged_command_is_planned_and_applied_with_no_host_language) {
    /*
     * The end-to-end claim of the applier: a write is planned into
     * commands, and those commands are performed against a real
     * collection, with no JavaScript anywhere in the process. Both halves
     * of the WAL round trip are C's -- which is what a committed Raft
     * entry needs, since a replica that cannot apply without a host
     * runtime cannot be a replica without one.
     *
     * The apply half owes three things per entry, and this checks all
     * three: the applied index is staged (so the mutation's own commit
     * persists it), the write lands, and the RESULT is the shape a caller
     * of the driver gets -- which under replication is not decoration but
     * the answer the leader hands back for a committed write.
     */
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-apply.bj") == 0);
    uint8_t id[12];
    mk_oid(id, 77);
    uint8_t spare[12];
    mk_oid(spare, 78);

    /* PLAN an insert, then APPLY what it planned. */
    doc *d = doc_new();
    doc_oid(d, "_id", id);
    doc_str(d, "team", "core");
    doc_int(d, "age", 36);
    uint32_t dlen; const uint8_t *dbuf_ = doc_done(d, &dlen);

    dc_wal_plan *p = NULL;
    CHECK_OK(dc_wal_plan_build(NULL, "people", 6, DC_WREQ_INSERT_ONE,
                               dbuf_, dlen, NULL, 0, 0, spare, &p));
    CHECK_FATAL(p != NULL);
    CHECK_I64((long long)dc_wal_plan_count(p), 1);

    uint32_t clen; const uint8_t *cmd = dc_wal_plan_cmd(p, 0, &clen);
    dbuf res = {0};
    CHECK_OK(dc_wal_apply(fx.coll, 5, cmd, clen, &res));
    /* Staged with the mutation, not after it. */
    CHECK_I64((long long)dc_applied_index(fx.coll), 5);
    {
        const uint8_t *v; size_t vlen; int found = 0;
        CHECK_OK(obj_get_field(res.data, res.len, (const uint8_t *)"insertedId", 10,
                               &v, &vlen, &found));
        CHECK_I64(found, 1);
        CHECK(vlen == 13 && v[0] == BJ_TYPE_OID);
        CHECK(memcmp(v + 1, id, 12) == 0);
    }
    dbuf_free(&res);
    dc_wal_plan_free(p);

    /* The document is really there. */
    int64_t count = 0;
    {
        const uint8_t *f; uint32_t flen;
        bj_builder *fb = empty_filter(&f, &flen);
        CHECK_OK(dc_count(fx.coll, f, flen, &count));
        bj_builder_free(fb);
    }
    CHECK_I64(count, 1);

    /* Re-applying the same command is a DETERMINISTIC failure -- a fact
     * about the command and the state it lands on, which every replica
     * reaches identically, rather than divergence. */
    {
        dc_wal_plan *p2 = NULL;
        CHECK_OK(dc_wal_plan_build(NULL, "people", 6, DC_WREQ_INSERT_ONE,
                                   dbuf_, dlen, NULL, 0, 0, spare, &p2));
        uint32_t l2; const uint8_t *c2 = dc_wal_plan_cmd(p2, 0, &l2);
        dbuf r2 = {0};
        int e = dc_wal_apply(fx.coll, 6, c2, l2, &r2);
        CHECK_I64(e, DC_ERR_DUPLICATE);
        CHECK_I64((long long)r2.len, 0);      /* a failure reports no result */
        CHECK_I64(dc_is_deterministic(e), 1);
        dbuf_free(&r2);
        dc_wal_plan_free(p2);
    }
    doc_free(d);   /* `dbuf_` points into it; both plans above read it */

    /* PLAN an update against the real collection -- the planner runs the
     * one query, resolves the target id, and the command carries no
     * filter -- then APPLY it. */
    {
        doc *filter = doc_new();
        doc_str(filter, "team", "core");
        uint32_t flen; const uint8_t *fbuf = doc_done(filter, &flen);
        doc *set = doc_new();
        doc_begin_obj(set, "$set");
        doc_int(set, "age", 37);
        doc_end_obj(set);
        uint32_t ulen; const uint8_t *ubuf = doc_done(set, &ulen);

        dc_wal_plan *pu = NULL;
        CHECK_OK(dc_wal_plan_build(fx.coll, "people", 6, DC_WREQ_UPDATE_ONE,
                                   fbuf, flen, ubuf, ulen, 0, spare, &pu));
        CHECK_I64(dc_wal_plan_outcome(pu), DC_PLAN_MATCHED);
        uint32_t ul; const uint8_t *ucmd = dc_wal_plan_cmd(pu, 0, &ul);
        dbuf ur = {0};
        CHECK_OK(dc_wal_apply(fx.coll, 7, ucmd, ul, &ur));
        CHECK_I64((long long)result_int(&ur, "matchedCount"), 1);
        CHECK_I64((long long)result_int(&ur, "modifiedCount"), 1);
        CHECK_I64((long long)dc_applied_index(fx.coll), 7);
        dbuf_free(&ur);
        dc_wal_plan_free(pu);
        doc_free(set);
        doc_free(filter);
    }

    /* DELETE the same way, and the count says what happened. */
    {
        doc *filter = doc_new();
        doc_int(filter, "age", 37);
        uint32_t flen; const uint8_t *fbuf = doc_done(filter, &flen);
        dc_wal_plan *pd = NULL;
        CHECK_OK(dc_wal_plan_build(fx.coll, "people", 6, DC_WREQ_DELETE_ONE,
                                   fbuf, flen, NULL, 0, 0, spare, &pd));
        uint32_t dl; const uint8_t *dcmd = dc_wal_plan_cmd(pd, 0, &dl);
        dbuf dr = {0};
        CHECK_OK(dc_wal_apply(fx.coll, 8, dcmd, dl, &dr));
        CHECK_I64((long long)result_int(&dr, "deletedCount"), 1);
        dbuf_free(&dr);

        /* Applying it again deletes nothing, and says so: a result, not
         * an error. A replica that reported 1 here would be claiming a
         * document that was not there. */
        dbuf dr2 = {0};
        CHECK_OK(dc_wal_apply(fx.coll, 9, dcmd, dl, &dr2));
        CHECK_I64((long long)result_int(&dr2, "deletedCount"), 0);
        dbuf_free(&dr2);
        dc_wal_plan_free(pd);
        doc_free(filter);
    }

    /*
     * A command carrying an UNRESOLVED $currentDate is refused. The
     * planner resolves it to a concrete Date before anything is logged
     * (db_wal.h), so one arriving here means a producer broke that rule
     * -- and the old JavaScript applier would have quietly resolved it
     * against THIS replica's clock, writing a different timestamp on
     * every node from the same committed entry. Failing is the only
     * answer that keeps replicas identical.
     */
    {
        doc *bad = doc_new();
        doc_str(bad, "c", "people");
        doc_str(bad, "op", "u");
        doc_oid(bad, "id", id);
        doc_begin_obj(bad, "update");
        doc_begin_obj(bad, "$currentDate");
        doc_int(bad, "seen", 1);
        doc_end_obj(bad);
        doc_end_obj(bad);
        uint32_t bl; const uint8_t *bcmd = doc_done(bad, &bl);
        dbuf br = {0};
        /* Read as a well-formed command -- and still not applied. */
        int op = -1; const uint8_t *cn; uint32_t cnl;
        CHECK_OK(dc_wal_parse(bcmd, bl, &op, &cn, &cnl));
        CHECK_I64(op, DC_WAL_UPDATE);
        CHECK(dc_wal_apply(fx.coll, 11, bcmd, bl, &br) != BJ_OK);
        dbuf_free(&br);
        doc_free(bad);
    }

    /* DDL is refused, explicitly: it makes and unmakes FILES, which is
     * the namespace owner's job and not this layer's. */
    {
        doc *keys = doc_new();
        doc_int(keys, "team", 1);
        uint32_t klen; const uint8_t *kbuf = doc_done(keys, &klen);
        doc *opts = doc_new();
        uint32_t olen; const uint8_t *obuf = doc_done(opts, &olen);
        dc_wal_plan *pi = NULL;
        CHECK_OK(dc_wal_plan_build(NULL, "people", 6, DC_WREQ_CREATE_INDEX,
                                   kbuf, klen, obuf, olen, 0, spare, &pi));
        uint32_t il; const uint8_t *icmd = dc_wal_plan_cmd(pi, 0, &il);
        dbuf ir = {0};
        CHECK_I64(dc_wal_apply(fx.coll, 10, icmd, il, &ir), DC_ERR_WAL_NOT_APPLIABLE);
        CHECK_I64(dc_wal_is_document(DC_WAL_CREATE_INDEX), 0);
        CHECK_I64(dc_wal_is_document(DC_WAL_INSERT), 1);
        dbuf_free(&ir);
        dc_wal_plan_free(pi);
        doc_free(opts);
        doc_free(keys);
    }

    fx_close(&fx);
}

TEST(wal_plan_resolves_every_command_to_one_id_and_no_filter) {
    /*
     * The invariant db_wal.h exists for: no logged document command
     * carries a filter, so applying one never runs a query and never
     * depends on the state replay happens to be in.
     */
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));
    CHECK_OK(insert_person(fx.coll, 2, "Grace", "core", 45));
    CHECK_OK(insert_person(fx.coll, 3, "Alan", "research", 41));

    uint8_t did[12]; mk_oid(did, 90);
    doc *q = doc_new(); doc_str(q, "team", "core");
    uint32_t qlen; const uint8_t *qbuf = doc_done(q, &qlen);
    doc *u = set_seen();
    uint32_t ulen; const uint8_t *ubuf = doc_done(u, &ulen);
    doc *nomatch = doc_new(); doc_str(nomatch, "team", "ghosts");
    uint32_t nlen; const uint8_t *nbuf = doc_done(nomatch, &nlen);

    struct { const char *what; int req; const uint8_t *a; uint32_t a_len;
             const uint8_t *b; uint32_t b_len; int upsert; uint32_t want_count; } cases[] = {
        { "updateOne",    DC_WREQ_UPDATE_ONE,  qbuf, qlen, ubuf, ulen, 0, 1 },
        { "updateMany",   DC_WREQ_UPDATE_MANY, qbuf, qlen, ubuf, ulen, 0, 2 },
        { "deleteOne",    DC_WREQ_DELETE_ONE,  qbuf, qlen, NULL, 0,    0, 1 },
        { "deleteMany",   DC_WREQ_DELETE_MANY, qbuf, qlen, NULL, 0,    0, 2 },
        { "upsertOne",    DC_WREQ_UPDATE_ONE,  nbuf, nlen, ubuf, ulen, 1, 1 },
        { "upsertMany",   DC_WREQ_UPDATE_MANY, nbuf, nlen, ubuf, ulen, 1, 1 },
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        dc_wal_plan *p = NULL;
        int rc = dc_wal_plan_build(fx.coll, "people", 6, cases[i].req,
                                   cases[i].a, cases[i].a_len,
                                   cases[i].b, cases[i].b_len,
                                   cases[i].upsert, did, &p);
        if (rc != BJ_OK) { TAP_FAIL("%s: plan failed rc=%d", cases[i].what, rc); continue; }
        if (dc_wal_plan_count(p) != cases[i].want_count) {
            TAP_FAIL("%s: planned %u commands, want %u",
                     cases[i].what, dc_wal_plan_count(p), cases[i].want_count);
        }
        for (uint32_t k = 0; k < dc_wal_plan_count(p); k++) {
            uint32_t len; const uint8_t *cmd = dc_wal_plan_cmd(p, k, &len);
            if (cmd_has(cmd, len, "filter") != 0)
                TAP_FAIL("%s: command %u carries a filter", cases[i].what, k);
            /* An INSERT names its document's _id instead of an `id`
             * field; every other document command names an id. */
            int op = -1; const uint8_t *coll; uint32_t coll_len;
            CHECK_OK(dc_wal_parse(cmd, len, &op, &coll, &coll_len));
            if (op != DC_WAL_INSERT && cmd_id(cmd, len) == NULL)
                TAP_FAIL("%s: command %u names no id", cases[i].what, k);
        }
        dc_wal_plan_free(p);
    }

    /* A non-upsert write that matches nothing produces no commands at
     * all: an entry that does nothing is still an entry every replica
     * stores, ships and replays. */
    dc_wal_plan *p = NULL;
    CHECK_OK(dc_wal_plan_build(fx.coll, "people", 6, DC_WREQ_UPDATE_ONE,
                               nbuf, nlen, ubuf, ulen, 0, did, &p));
    CHECK_I64(dc_wal_plan_outcome(p), DC_PLAN_NOTHING);
    CHECK_I64((long long)dc_wal_plan_count(p), 0);
    CHECK(dc_wal_plan_target_id(p) == NULL);
    dc_wal_plan_free(p);

    /* deleteMany matching nothing, likewise. */
    p = NULL;
    CHECK_OK(dc_wal_plan_build(fx.coll, "people", 6, DC_WREQ_DELETE_MANY,
                               nbuf, nlen, NULL, 0, 0, did, &p));
    CHECK_I64(dc_wal_plan_outcome(p), DC_PLAN_NOTHING);
    CHECK_I64((long long)dc_wal_plan_count(p), 0);
    dc_wal_plan_free(p);

    doc_free(nomatch); doc_free(u); doc_free(q);
    fx_close(&fx);
}

TEST(upsert_uses_the_id_the_filter_pinned) {
    /*
     * An upsert whose filter says {_id: X} must insert X. It used to
     * insert a generated id instead -- splice_id overwrote whatever the
     * seed carried -- so the document landed under a key the caller never
     * named, and the reported upsertedId was that key rather than the one
     * actually stored. Both halves are checked here.
     */
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);

    uint8_t pinned[12]; mk_oid(pinned, 42);
    uint8_t generated[12]; mk_oid(generated, 99);

    doc *q = doc_new();
    doc_oid(q, "_id", pinned);
    doc_str(q, "team", "core");
    uint32_t qlen; const uint8_t *qbuf = doc_done(q, &qlen);
    doc *u = set_seen();
    uint32_t ulen; const uint8_t *ubuf = doc_done(u, &ulen);

    int result = -1;
    uint8_t reported[12];
    memset(reported, 0, sizeof(reported));
    CHECK_OK(dc_update_one(fx.coll, qbuf, qlen, ubuf, ulen, generated, 1, &result, reported));
    CHECK_I64(result, 2);
    CHECK(memcmp(reported, pinned, 12) == 0);

    /* Findable by the id that was asked for, not by the generated one. */
    doc *byPinned = doc_new(); doc_oid(byPinned, "_id", pinned);
    uint32_t bplen; const uint8_t *bpbuf = doc_done(byPinned, &bplen);
    int found = 0; uint8_t *out = NULL; size_t out_len = 0;
    CHECK_OK(dc_find_one(fx.coll, bpbuf, bplen, NULL, 0, &found, &out, &out_len));
    CHECK_I64(found, 1);
    if (found) {
        char team[32];
        CHECK(doc_get_str(out, out_len, "team", team, sizeof(team)));
        CHECK_STR(team, "core");            /* the filter's other conditions still seed */
    }
    free(out);

    /* updateMany takes the same path. */
    uint8_t pinned2[12]; mk_oid(pinned2, 43);
    doc *q2 = doc_new(); doc_oid(q2, "_id", pinned2);
    uint32_t q2len; const uint8_t *q2buf = doc_done(q2, &q2len);
    int64_t matched = 0; int upserted = 0;
    memset(reported, 0, sizeof(reported));
    CHECK_OK(dc_update_many(fx.coll, q2buf, q2len, ubuf, ulen, generated, 1,
                            &matched, &upserted, reported, NULL, NULL));
    CHECK_I64(upserted, 1);
    CHECK(memcmp(reported, pinned2, 12) == 0);

    /* replaceOne, whose replacement names no _id, likewise takes the
     * filter's -- the same bug wearing a different hat. */
    uint8_t pinned3[12]; mk_oid(pinned3, 44);
    doc *q3 = doc_new(); doc_oid(q3, "_id", pinned3);
    uint32_t q3len; const uint8_t *q3buf = doc_done(q3, &q3len);
    doc *repl = doc_new(); doc_str(repl, "name", "Replaced");
    uint32_t rlen; const uint8_t *rbuf = doc_done(repl, &rlen);
    result = -1;
    memset(reported, 0, sizeof(reported));
    CHECK_OK(dc_replace_one(fx.coll, q3buf, q3len, rbuf, rlen, generated, 1, &result, reported));
    CHECK_I64(result, 2);
    CHECK(memcmp(reported, pinned3, 12) == 0);

    /* No pinned id: the generated one, as before. */
    doc *q4 = doc_new(); doc_str(q4, "team", "ghosts");
    uint32_t q4len; const uint8_t *q4buf = doc_done(q4, &q4len);
    result = -1;
    memset(reported, 0, sizeof(reported));
    CHECK_OK(dc_update_one(fx.coll, q4buf, q4len, ubuf, ulen, generated, 1, &result, reported));
    CHECK_I64(result, 2);
    CHECK(memcmp(reported, generated, 12) == 0);

    /* An _id inside an operator expression pins nothing -- the same rule
     * build_upsert_seed applies to every other field. */
    doc *q5 = doc_new();
    doc_begin_obj(q5, "_id");
    doc_key(q5, "$ne");
    bj_put_oid(q5->b, pinned);
    doc_end_obj(q5);
    doc_str(q5, "team", "phantom");   /* so the filter matches nothing */
    uint32_t q5len; const uint8_t *q5buf = doc_done(q5, &q5len);
    uint8_t generated2[12]; mk_oid(generated2, 100);
    result = -1;
    memset(reported, 0, sizeof(reported));
    CHECK_OK(dc_update_one(fx.coll, q5buf, q5len, ubuf, ulen, generated2, 1, &result, reported));
    CHECK_I64(result, 2);
    CHECK(memcmp(reported, generated2, 12) == 0);

    /* A pinned _id this format cannot store is refused, not quietly
     * swapped for a generated one. */
    doc *q6 = doc_new(); doc_str(q6, "_id", "not-an-objectid");
    uint32_t q6len; const uint8_t *q6buf = doc_done(q6, &q6len);
    result = -1;
    CHECK_RC(dc_update_one(fx.coll, q6buf, q6len, ubuf, ulen, generated, 1, &result, NULL),
             DC_ERR_UNSUPPORTED_ID);
    CHECK_I64(result, 0);

    doc_free(q6); doc_free(q5); doc_free(q4); doc_free(repl); doc_free(q3);
    doc_free(q2); doc_free(byPinned); doc_free(u); doc_free(q);
    fx_close(&fx);
}

TEST(wal_plan_and_direct_upsert_insert_the_same_document) {
    /*
     * The whole point of routing the planner through dc_upsert_document:
     * what the WAL logs and what a non-WAL collection would have done
     * must be the same document, byte for byte. Two collections, the same
     * request, one via the plan and one direct.
     */
    fixture planned, direct;
    CHECK_FATAL(fx_open(&planned, "coll-a.bj") == 0);
    CHECK_FATAL(fx_open(&direct, "coll-b.bj") == 0);

    uint8_t did[12]; mk_oid(did, 90);
    doc *q = doc_new();
    doc_str(q, "team", "ghosts");
    doc_int(q, "age", 100);
    uint32_t qlen; const uint8_t *qbuf = doc_done(q, &qlen);
    doc *u = set_seen();
    uint32_t ulen; const uint8_t *ubuf = doc_done(u, &ulen);

    dc_wal_plan *p = NULL;
    CHECK_OK(dc_wal_plan_build(planned.coll, "people", 6, DC_WREQ_UPDATE_ONE,
                               qbuf, qlen, ubuf, ulen, 1, did, &p));
    CHECK_I64(dc_wal_plan_outcome(p), DC_PLAN_UPSERT);
    CHECK_I64((long long)dc_wal_plan_count(p), 1);

    /* Apply the planned INSERT the way the host's apply path would: pull
     * `doc` straight out of the command. */
    uint32_t clen; const uint8_t *cmd = dc_wal_plan_cmd(p, 0, &clen);
    const uint8_t *dp; size_t dlen; int found = 0;
    CHECK_OK(obj_get_field(cmd, clen, (const uint8_t *)"doc", 3, &dp, &dlen, &found));
    CHECK_I64(found, 1);
    if (found) CHECK_OK(dc_insert_one(planned.coll, dp, (uint32_t)dlen));

    int result = -1;
    CHECK_OK(dc_update_one(direct.coll, qbuf, qlen, ubuf, ulen, did, 1, &result, NULL));
    CHECK_I64(result, 2);

    doc *all = doc_new();
    uint32_t alen; const uint8_t *abuf = doc_done(all, &alen);
    int f1 = 0, f2 = 0; uint8_t *d1 = NULL, *d2 = NULL; size_t l1 = 0, l2 = 0;
    CHECK_OK(dc_find_one(planned.coll, abuf, alen, NULL, 0, &f1, &d1, &l1));
    CHECK_OK(dc_find_one(direct.coll, abuf, alen, NULL, 0, &f2, &d2, &l2));
    CHECK_I64(f1, 1); CHECK_I64(f2, 1);
    CHECK_I64((long long)l1, (long long)l2);
    if (l1 == l2) CHECK(memcmp(d1, d2, l1) == 0);

    /* And the id the plan reports is the id it actually inserted. */
    const uint8_t *target = dc_wal_plan_target_id(p);
    CHECK(target != NULL);
    if (target && f1) {
        uint8_t actual[12];
        CHECK_OK(dc_document_id(d1, (uint32_t)l1, actual));
        CHECK(memcmp(target, actual, 12) == 0);
    }

    free(d1); free(d2);
    dc_wal_plan_free(p);
    doc_free(all); doc_free(u); doc_free(q);
    fx_close(&direct); fx_close(&planned);
}

TEST(an_insert_without_an_id_says_which_field_is_missing) {
    /*
     * The refusal was always right; the sentence was not. An insert whose
     * document carries no _id was dc_document_id's BJ_ERR_STATE, which
     * reaches a client as "builder state error" -- a sentence about a
     * builder, for a request that was merely incomplete. It now has its
     * own code at the planner and the wire's own vocabulary above it.
     *
     * And `id` does not stand in for it, at either layer. That is the
     * half db_session.h used to promise and nothing ever did: an id in
     * two places would need a precedence rule between them, which is two
     * owners for one fact. An upsert is different and keeps `id`,
     * because it cannot know it needs one until it has matched.
     */
    uint8_t given[12]; mk_oid(given, 91);

    doc *d = doc_new();
    doc_str(d, "team", "core");            /* no _id at all */
    uint32_t dlen; const uint8_t *dbuf_ = doc_done(d, &dlen);

    /* ---- the planner: its own code, whether or not an id was offered. */
    {
        dc_wal_plan *p = NULL;
        CHECK_RC(dc_wal_plan_build(NULL, "people", 6, DC_WREQ_INSERT_ONE,
                                   dbuf_, dlen, NULL, 0, 0, given, &p),
                 DC_ERR_WAL_NO_ID);
        CHECK(p == NULL);

        p = NULL;
        CHECK_RC(dc_wal_plan_build(NULL, "people", 6, DC_WREQ_INSERT_ONE,
                                   dbuf_, dlen, NULL, 0, 0, NULL, &p),
                 DC_ERR_WAL_NO_ID);
        CHECK(p == NULL);
    }

    /* ---- and the wire above it, in the wire's own vocabulary: a client
     * that gets -42 is told to add a field, which is exactly what it has
     * to do. */
    {
        char tmpl[64];
        CHECK_FATAL(scratch_dir("nisaba-noid", tmpl, sizeof tmpl) == 0);
        int dirfd = open(tmpl, O_RDONLY);
        CHECK_FATAL(dirfd >= 0);
        bj_ns ns;
        CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);
        CHECK_FATAL(build_users_db(&ns) == 0);
        dbs *s = NULL;
        CHECK_FATAL(dbs_open(&ns, ORDER, 0, &s) == BJ_OK);

        const uint8_t *req; uint32_t req_len;
        bj_builder *rb = request_with_id("insert", "users", "doc", dbuf_, dlen,
                                         given, &req, &req_len);
        dbuf res = {0};
        CHECK_OK(dbs_handle(s, 1, req, req_len, &res));
        CHECK_I64(response_ok(&res), 0);
        int f = 0;
        CHECK_I64(response_num(&res, "code", &f), DC_ERR_REQ_MISSING_FIELD);
        /* Nothing landed: a refusal leaves the collection as it was. */
        dbuf_free(&res); bj_builder_free(rb);

        const uint8_t *creq; uint32_t creq_len;
        bj_builder *cb = request("count", "users", NULL, NULL, 0, &creq, &creq_len);
        dbuf cres = {0};
        CHECK_OK(dbs_handle(s, 1, creq, creq_len, &cres));
        CHECK_I64(response_num(&cres, "n", &f), 3);
        dbuf_free(&cres); bj_builder_free(cb);

        /* The same document WITH an _id is accepted, so what is being
         * refused is the missing field and not the shape of the request. */
        {
            doc *ok = doc_new();
            uint8_t own[12]; mk_oid(own, 92);
            doc_oid(ok, "_id", own);
            doc_str(ok, "team", "core");
            uint32_t olen; const uint8_t *obuf = doc_done(ok, &olen);
            const uint8_t *oreq; uint32_t oreq_len;
            bj_builder *ob = request_with_id("insert", "users", "doc", obuf, olen,
                                             own, &oreq, &oreq_len);
            dbuf ores = {0};
            CHECK_OK(dbs_handle(s, 1, oreq, oreq_len, &ores));
            CHECK_I64(response_ok(&ores), 1);
            dbuf_free(&ores); bj_builder_free(ob); doc_free(ok);
        }

        dbs_close(s);
        bjns_posix_free(&ns);
        close(dirfd);
    }

    doc_free(d);
}

TEST(wal_plan_returns_the_preimage_the_host_would_have_queried_for) {
    /* findOneAndUpdate's `returnDocument: 'before'` used to cost a
     * findOne of its own. The planner already had the matched document
     * in hand -- it is how the target id was resolved. */
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));

    uint8_t did[12]; mk_oid(did, 90);
    doc *q = doc_new(); doc_str(q, "name", "Ada");
    uint32_t qlen; const uint8_t *qbuf = doc_done(q, &qlen);
    doc *u = set_seen();
    uint32_t ulen; const uint8_t *ubuf = doc_done(u, &ulen);

    dc_wal_plan *p = NULL;
    CHECK_OK(dc_wal_plan_build(fx.coll, "people", 6, DC_WREQ_UPDATE_ONE,
                               qbuf, qlen, ubuf, ulen, 0, did, &p));
    uint32_t plen; const uint8_t *pre = dc_wal_plan_preimage(p, &plen);
    CHECK(pre != NULL);
    if (pre) {
        char team[32];
        CHECK(doc_get_str(pre, plen, "team", team, sizeof(team)));
        CHECK_STR(team, "core");
        /* PRE-image: the update is not in it. */
        CHECK_I64(cmd_has(pre, plen, "seen"), 0);
    }
    dc_wal_plan_free(p);

    /* An upsert has no pre-image -- there was no prior state, which is
     * exactly what the driver returns null for. */
    doc *nomatch = doc_new(); doc_str(nomatch, "name", "Nobody");
    uint32_t nlen; const uint8_t *nbuf = doc_done(nomatch, &nlen);
    p = NULL;
    CHECK_OK(dc_wal_plan_build(fx.coll, "people", 6, DC_WREQ_UPDATE_ONE,
                               nbuf, nlen, ubuf, ulen, 1, did, &p));
    CHECK_I64(dc_wal_plan_outcome(p), DC_PLAN_UPSERT);
    CHECK(dc_wal_plan_preimage(p, &plen) == NULL);
    dc_wal_plan_free(p);

    doc_free(nomatch); doc_free(u); doc_free(q);
    fx_close(&fx);
}

TEST(wal_plan_rejects_before_it_logs_rather_than_after) {
    /* A command certain to fail must never reach the log. The proposer
     * still has a caller to hand the error to; the applier does not, and
     * on a replica there is nobody to tell at all. */
    fixture fx;
    CHECK_FATAL(fx_open(&fx, "coll-people.bj") == 0);
    CHECK_OK(insert_person(fx.coll, 1, "Ada", "core", 36));

    uint8_t did[12]; mk_oid(did, 90);
    doc *q = doc_new(); doc_str(q, "name", "Ada");
    uint32_t qlen; const uint8_t *qbuf = doc_done(q, &qlen);

    /* replaceOne may not move a document to a different _id. */
    doc *moved = doc_new();
    { uint8_t other[12]; mk_oid(other, 77); doc_oid(moved, "_id", other); }
    doc_str(moved, "name", "Ada");
    uint32_t mlen; const uint8_t *mbuf = doc_done(moved, &mlen);
    dc_wal_plan *p = NULL;
    CHECK_RC(dc_wal_plan_build(fx.coll, "people", 6, DC_WREQ_REPLACE_ONE,
                               qbuf, qlen, mbuf, mlen, 0, did, &p),
             DC_ERR_ID_MISMATCH);
    CHECK(p == NULL);

    /* A malformed update is rejected before a single command is emitted,
     * so an unordered batch can still attempt everything else -- the
     * reasoning db_bulk.h documents for its own up-front validation. */
    doc *bad = doc_new();
    doc_str(bad, "seen", "no-operator-here");
    uint32_t blen; const uint8_t *bbuf = doc_done(bad, &blen);
    p = NULL;
    CHECK(dc_wal_plan_build(fx.coll, "people", 6, DC_WREQ_UPDATE_MANY,
                            qbuf, qlen, bbuf, blen, 0, did, &p) != BJ_OK);
    CHECK(p == NULL);

    /* An insert with no _id: the host assigns ids, and one that forgot
     * would otherwise log an entry that fails on every replica. The CODE
     * is asserted, not merely "not ok" -- for years this was BJ_ERR_STATE
     * and a client was told "builder state error", which this assertion
     * was loose enough to allow. */
    doc *anon = doc_new();
    doc_str(anon, "name", "Anonymous");
    uint32_t anlen; const uint8_t *anbuf = doc_done(anon, &anlen);
    p = NULL;
    CHECK_RC(dc_wal_plan_build(NULL, "people", 6, DC_WREQ_INSERT_ONE,
                               anbuf, anlen, NULL, 0, 0, did, &p),
             DC_ERR_WAL_NO_ID);
    CHECK(p == NULL);

    /* An empty insertMany. */
    doc *empty_arr = doc_new();
    uint32_t ealen; const uint8_t *eabuf;
    { /* a bare ARRAY, not an object -- built directly */
        bj_builder *b = bj_builder_new();
        bj_begin_array(b); bj_end_array(b);
        size_t n; const uint8_t *d = bj_builder_data(b, &n);
        p = NULL;
        CHECK_RC(dc_wal_plan_build(NULL, "people", 6, DC_WREQ_INSERT_MANY,
                                   d, (uint32_t)n, NULL, 0, 0, did, &p),
                 DC_ERR_WAL_BAD_REQUEST);
        CHECK(p == NULL);
        bj_builder_free(b);
    }
    (void)ealen; (void)eabuf;

    doc_free(empty_arr); doc_free(anon); doc_free(bad); doc_free(moved); doc_free(q);
    fx_close(&fx);
}


/* ---- the leader's and candidate's own bookkeeping (raft_drive.h) -------- */

TEST(election_round_ignores_votes_from_a_world_that_ended) {
    /*
     * The guards that matter are not "did they say yes" -- they are
     * "does this reply still belong to the world it was asked in". A
     * vote reply arrives asynchronously, so by the time it lands the
     * node may have stepped down, won already, or moved on a term. Every
     * one of these cases counted a vote in an earlier draft of the JS.
     */
    raft_round r;

    /* A quorum of grants wins, and the round latches so a late fourth
     * grant cannot win it twice. */
    raft_round_begin(&r, 5, 2, 0);
    CHECK_I64(raft_round_on_reply(&r, 5, 1, 5, 0, 1, NULL), RAFT_ROUND_WON);
    CHECK_I64(r.settled, 1);
    CHECK_I64(raft_round_on_reply(&r, 5, 1, 5, 0, 1, NULL), RAFT_ROUND_IGNORE);
    CHECK_I64((long long)r.granted, 2);

    /* A refusal counts as an answer but not a vote. */
    raft_round_begin(&r, 5, 3, 0);
    CHECK_I64(raft_round_on_reply(&r, 5, 0, 5, 0, 1, NULL), RAFT_ROUND_PENDING);
    CHECK_I64((long long)r.granted, 1);

    /* A higher term deposes, and reports the term to adopt. */
    {
        uint64_t step = 0;
        raft_round_begin(&r, 5, 2, 0);
        CHECK_I64(raft_round_on_reply(&r, 9, 1, 5, 0, 1, &step), RAFT_ROUND_STEP_DOWN);
        CHECK_I64((long long)step, 9);
        CHECK_I64(r.settled, 1);
    }

    /* No longer a candidate: the grant is from a world that ended. */
    raft_round_begin(&r, 5, 2, 0);
    CHECK_I64(raft_round_on_reply(&r, 5, 1, 5, 0, 0, NULL), RAFT_ROUND_IGNORE);
    CHECK_I64((long long)r.granted, 1);

    /* Still a candidate, but in a LATER term -- this round is stale. */
    raft_round_begin(&r, 5, 2, 0);
    CHECK_I64(raft_round_on_reply(&r, 5, 1, 6, 0, 1, NULL), RAFT_ROUND_IGNORE);

    /* A pre-vote polls from term-1 about term. It stays valid only while
     * the node has not advanced... */
    raft_round_begin(&r, 5, 2, 1);
    CHECK_I64(raft_round_on_reply(&r, 4, 1, 4, 0, 0, NULL), RAFT_ROUND_WON);

    /* ...and is void once we already lead: winning it would start an
     * election that deposes us for no reason. */
    raft_round_begin(&r, 5, 2, 1);
    CHECK_I64(raft_round_on_reply(&r, 4, 1, 4, 1, 0, NULL), RAFT_ROUND_IGNORE);

    /* A pre-vote round whose term already advanced is void too. */
    raft_round_begin(&r, 5, 2, 1);
    CHECK_I64(raft_round_on_reply(&r, 5, 1, 5, 0, 1, NULL), RAFT_ROUND_IGNORE);

    /* A single-voter group is its own quorum. */
    raft_round_begin(&r, 2, 1, 0);
    CHECK_I64((long long)r.granted, 1);
    CHECK_I64((long long)r.quorum, 1);
}

TEST(replication_picks_append_snapshot_or_park) {
    /* The boundary is next > base_index. base_index is the last index
     * the log no longer holds, so a peer needing it can never be caught
     * up by rewinding -- off by one here is a leader that backs off
     * forever against a follower it cannot satisfy. */
    CHECK_I64(raft_repl_decide(11, 10, 1), RAFT_REPL_APPEND);
    CHECK_I64(raft_repl_decide(10, 10, 1), RAFT_REPL_SNAPSHOT);
    CHECK_I64(raft_repl_decide(10, 10, 0), RAFT_REPL_PARK);
    CHECK_I64(raft_repl_decide(1, 0, 0), RAFT_REPL_APPEND);   /* fresh log */

    /* An install moves the peer to the boundary, and match never
     * regresses: a peer already past the snapshot keeps what it had. */
    {
        uint64_t match = 0, next = 0;
        raft_repl_installed(40, &match, &next);
        CHECK_I64((long long)match, 40);
        CHECK_I64((long long)next, 41);

        match = 55;
        raft_repl_installed(40, &match, &next);
        CHECK_I64((long long)match, 55);
        CHECK_I64((long long)next, 56);
    }
}

/* Walk a whole snapshot and report what the receiver would have seen. */
static void drain_chunks(const uint64_t *sizes, uint32_t n, uint32_t chunk,
                         int *chunks, int *firsts, int *dones, uint64_t *bytes) {
    *chunks = *firsts = *dones = 0; *bytes = 0;
    uint32_t cf = 0; uint64_t co = 0;
    raft_chunk c;
    /* Bounded: a cursor that fails to advance is the bug this guards. */
    for (int guard = 0; guard < 1000; guard++) {
        if (!raft_chunk_next(sizes, n, chunk, cf, co, &c)) return;
        (*chunks)++;
        *firsts += c.is_first;
        *dones  += c.is_done;
        *bytes  += c.len;
        cf = c.next_file; co = c.next_offset;
    }
    TAP_FAIL("chunk cursor did not advance after %d chunks", 1000);
}

TEST(snapshot_chunking_covers_every_byte_exactly_once) {
    int chunks, firsts, dones; uint64_t bytes;

    /* Two files, cut at 4 bytes: 10 -> 3 chunks, 6 -> 2. Exactly one
     * chunk carries the manifest and exactly one ends the stream. */
    {
        const uint64_t sizes[] = { 10, 6 };
        drain_chunks(sizes, 2, 4, &chunks, &firsts, &dones, &bytes);
        CHECK_I64(chunks, 5);
        CHECK_I64(firsts, 1);
        CHECK_I64(dones, 1);
        CHECK_I64((long long)bytes, 16);
    }

    /* A chunk never spans two files, so the receiver can write it
     * without knowing the layout. */
    {
        const uint64_t sizes[] = { 3, 3 };
        raft_chunk c;
        CHECK(raft_chunk_next(sizes, 2, 100, 0, 0, &c) == 1);
        CHECK_I64((long long)c.len, 3);
        CHECK_I64((long long)c.file_index, 0);
        CHECK_I64(c.is_done, 0);
        CHECK(raft_chunk_next(sizes, 2, 100, c.next_file, c.next_offset, &c) == 1);
        CHECK_I64((long long)c.file_index, 1);
        CHECK_I64(c.is_done, 1);
    }

    /* An empty file gets its own chunk rather than being skipped: an
     * absent file and an empty one are different things to the manifest
     * check on the receiving side. */
    {
        const uint64_t sizes[] = { 0, 5 };
        drain_chunks(sizes, 2, 4, &chunks, &firsts, &dones, &bytes);
        CHECK_I64(chunks, 3);          /* empty, 4, 1 */
        CHECK_I64(dones, 1);
        CHECK_I64((long long)bytes, 5);
    }

    /* A TRAILING empty file is where a derived cursor loops forever:
     * offset + len leaves the cursor where it started. drain_chunks
     * fails loudly rather than hanging if that ever comes back. */
    {
        const uint64_t sizes[] = { 5, 0 };
        drain_chunks(sizes, 2, 4, &chunks, &firsts, &dones, &bytes);
        CHECK_I64(chunks, 3);          /* 4, 1, empty */
        CHECK_I64(dones, 1);
        CHECK_I64((long long)bytes, 5);
    }

    /* No files at all: the manifest still has to travel, and the
     * boundary it declares is the whole point of the transfer. */
    {
        drain_chunks(NULL, 0, 4, &chunks, &firsts, &dones, &bytes);
        CHECK_I64(chunks, 1);
        CHECK_I64(firsts, 1);
        CHECK_I64(dones, 1);
        CHECK_I64((long long)bytes, 0);
    }

    /* Exact multiples do not emit a trailing empty chunk. */
    {
        const uint64_t sizes[] = { 8 };
        drain_chunks(sizes, 1, 4, &chunks, &firsts, &dones, &bytes);
        CHECK_I64(chunks, 2);
        CHECK_I64(dones, 1);
        CHECK_I64((long long)bytes, 8);
    }
}


/* ---- a three-node cluster, in C, with no host language at all --------- */

typedef struct {
    uint64_t   id;
    memfs     *fs;
    elog      *log;
    raft_node *node;
} rn_member;

/*
 * Deliver everything sitting in every node's outbox, once.
 *
 * This IS the outbox model, and it is about twenty lines: read what C
 * decided to send, hand it to whoever it is addressed to, feed the
 * answer back by correlation id. A host with sockets substitutes a
 * write() for the rn_handle call and a read() for the reply; nothing
 * else about the shape changes, which is the entire claim raft_node.h
 * makes.
 */
static void pump(rn_member *m, int count) {
    for (int i = 0; i < count; i++) {
        uint32_t nout = rn_out_count(m[i].node);
        for (uint32_t k = 0; k < nout; k++) {
            uint64_t to   = rn_out_peer(m[i].node, k);
            uint64_t corr = rn_out_corr(m[i].node, k);
            int is_reply  = rn_out_is_reply(m[i].node, k);
            uint32_t len = 0;
            const uint8_t *bytes = rn_out_bytes(m[i].node, k, &len);

            /* Copy: delivering mutates the target's outbox, and on a
             * self-addressed reply that would be this very buffer. */
            uint8_t *copy = (uint8_t *)malloc(len ? len : 1);
            memcpy(copy, bytes, len);

            for (int j = 0; j < count; j++) {
                if (m[j].id != to) continue;
                if (is_reply) rn_on_reply(m[j].node, corr, copy, len, 0.5);
                else          rn_handle(m[j].node, corr, copy, len, 0.5);
                break;
            }
            free(copy);
        }
        rn_out_clear(m[i].node);
    }
}

static int leader_count(rn_member *m, int count, int *which) {
    int n = 0;
    for (int i = 0; i < count; i++) {
        if (rn_role(m[i].node) == RAFT_LEADER) { n++; if (which) *which = i; }
    }
    return n;
}

TEST(a_single_voter_group_commits_the_moment_it_elects_itself) {
    /*
     * Regression. The three-node test above passed while this was
     * broken: a lone voter reaches quorum without sending anything, so
     * nothing ever arrives to trigger the commit check, and it elected
     * itself and then committed nothing forever. src/raft.js ends
     * _becomeLeader with the same call for the same reason.
     */
    memfs *fs = memfs_new();
    CHECK_FATAL(fs != NULL);
    bj_io io;
    CHECK_FATAL(memfs_open(fs, "raft.bj", &io) == BJ_OK);
    elog *log = elog_create(&io);
    CHECK_FATAL(log != NULL);
    raft_node *n = rn_new(1, log);
    CHECK_FATAL(n != NULL);

    bj_builder *members = bj_builder_new();
    bj_begin_array(members);
    bj_begin_object(members);
    bj_put_key(members, (const uint8_t *)"id", 2);
    bj_put_int(members, 1);
    bj_end_object(members);
    bj_end_array(members);
    size_t mlen; const uint8_t *mbuf = bj_builder_data(members, &mlen);
    CHECK_OK(rn_set_members(n, mbuf, (uint32_t)mlen));
    CHECK_I64(rn_quorum(n), 1);

    rn_start(n, 0, 0.0);
    for (int64_t t = 0; t <= 1000 && rn_role(n) != RAFT_LEADER; t += 10)
        rn_tick(n, t, 0.5);

    CHECK_I64(rn_role(n), RAFT_LEADER);
    CHECK(rn_commit_index(n) > 0);
    /* Nobody to talk to, so nothing was ever queued. */
    CHECK_I64(rn_out_count(n), 0);

    bj_builder_free(members);
    rn_free(n);
    elog_free(log);
    memfs_free(fs);
}

TEST(a_leader_streams_its_own_snapshot_with_no_host_to_read_the_files) {
    /*
     * docs/steps/install-snapshot-in-c.md, the send side.
     *
     * A peer whose next index the log no longer holds cannot be caught
     * up by AppendEntries at any rewind. WHICH peer and WHEN were
     * already decided in C; SENDING was not, for one reason -- it means
     * reading files -- so the node raised RN_EFFECT_NEEDS_SNAPSHOT and
     * src/raft.js's _sendSnapshot did the transfer, awaiting each chunk
     * inside a loop that had to re-check the role, the term and the
     * running flag by hand on every iteration.
     *
     * Here the node has a namespace and a store, and does it itself:
     * chunks queue through the outbox and their replies arrive through
     * rn_on_reply, tied together by a correlation id rather than by a
     * loop that has to survive an election.
     *
     * Everything below runs over a REAL directory through
     * bjns_posix_open, with no JavaScript in the process -- which is the
     * whole claim.
     */
    char tmpl[64];
    CHECK_FATAL(scratch_dir("nisaba-install", tmpl, sizeof tmpl) == 0);
    int dirfd = open(tmpl, O_RDONLY);
    CHECK_FATAL(dirfd >= 0);
    bj_ns ns;
    CHECK_FATAL(bjns_posix_open(dirfd, &ns) == BJ_OK);

    /* ---- a snapshot on disk: one file with bytes in it, and one that
     * is EMPTY. The empty one is not a detail -- it gets a chunk of its
     * own so the receiver creates it, and "absent" and "empty" are
     * different things to the manifest check on the other side. */
    static const uint32_t PRIMARY = 300;
    uint8_t *content = (uint8_t *)malloc(PRIMARY);
    CHECK_FATAL(content != NULL);
    for (uint32_t i = 0; i < PRIMARY; i++) content[i] = (uint8_t)(i * 7 + 1);

    sst *store = sst_new("snap", 4);
    CHECK_FATAL(store != NULL);
    {
        dbuf name = {0};
        CHECK_OK(sst_data_name(store, 1, "primary", 7, &name));
        bj_io io;
        CHECK_OK(ns.open(ns.ctx, (const char *)name.data, (uint32_t)name.len,
                         BJ_NS_CREATE | BJ_NS_TRUNC, &io));
        CHECK_OK(io.write(io.ctx, 0, content, PRIMARY));
        if (io.close) io.close(io.ctx);
        dbuf_free(&name);

        name.len = 0;
        CHECK_OK(sst_data_name(store, 1, "journal", 7, &name));
        CHECK_OK(ns.open(ns.ctx, (const char *)name.data, (uint32_t)name.len,
                         BJ_NS_CREATE | BJ_NS_TRUNC, &io));
        if (io.close) io.close(io.ctx);   /* left at zero bytes, on purpose */
        dbuf_free(&name);
    }

    /* The manifest, written last, which is what makes the generation
     * real (snapstore.h's commit protocol). */
    dbuf manifest = {0};
    {
        bj_builder *fb = bj_builder_new();
        bj_begin_array(fb);
        files_entry(fb, "primary", "snap-1-primary.bj", PRIMARY, 0x1111);
        files_entry(fb, "journal", "snap-1-journal.bj", 0, 0x2222);
        bj_end_array(fb);
        size_t flen; const uint8_t *fbuf = bj_builder_data(fb, &flen);
        CHECK_OK(sst_manifest_encode(40, 6, NULL, 0, fbuf, (uint32_t)flen, &manifest));
        dbuf mname = {0};
        CHECK_OK(sst_manifest_name(store, 1, &mname));
        bj_io io;
        CHECK_OK(ns.open(ns.ctx, (const char *)mname.data, (uint32_t)mname.len,
                         BJ_NS_CREATE | BJ_NS_TRUNC, &io));
        CHECK_OK(io.write(io.ctx, 0, manifest.data, (uint32_t)manifest.len));
        if (io.close) io.close(io.ctx);
        dbuf_free(&mname);
        bj_builder_free(fb);
    }

    /* Adopt it, the way a host opening a database does: scan the
     * listing, try the newest manifest, confirm the files are the sizes
     * it claims. */
    {
        dirlist l;
        memset(&l, 0, sizeof(l));
        dirlist_add(&l, "snap-1.manifest.bj", (double)manifest.len);
        dirlist_add(&l, "snap-1-primary.bj", (double)PRIMARY);
        dirlist_add(&l, "snap-1-journal.bj", 0);
        CHECK_OK(sst_scan(store, l.names.data, (uint32_t)l.names.len));
        manifest_src srcs[] = { { "snap-1.manifest.bj", &manifest } };
        CHECK_I64(run_open(store, &l, srcs, 1), 0);
        CHECK_I64(sst_has_latest(store), 1);
        dirlist_free(&l);
    }

    /* ---- a leader whose log begins ABOVE what the peer needs, which is
     * the only situation an install exists for. */
    bj_io lio;
    CHECK_OK(ns.open(ns.ctx, "raft.bj", 7, BJ_NS_CREATE | BJ_NS_TRUNC, &lio));
    elog *log = elog_create_at(&lio, 40, 6);
    CHECK_FATAL(log != NULL);

    raft_node *n = rn_new(1, log);
    CHECK_FATAL(n != NULL);
    /* Two members, and the second is a LEARNER -- which is not a
     * convenience for the test but the case an install exists for: a new
     * member joins carrying nothing, so it cannot be caught up from a
     * log that begins above index 40. It also means this node reaches a
     * quorum of one and can elect itself with nobody answering. */
    bj_builder *members = bj_builder_new();
    bj_begin_array(members);
    bj_begin_object(members);
    bj_put_key(members, (const uint8_t *)"id", 2); bj_put_int(members, 1);
    bj_end_object(members);
    bj_begin_object(members);
    bj_put_key(members, (const uint8_t *)"id", 2); bj_put_int(members, 2);
    bj_put_key(members, (const uint8_t *)"voting", 6); bj_put_bool(members, 0);
    bj_end_object(members);
    bj_end_array(members);
    size_t mlen; const uint8_t *mbuf = bj_builder_data(members, &mlen);
    CHECK_OK(rn_set_members(n, mbuf, (uint32_t)mlen));
    CHECK_I64(rn_quorum(n), 1);

    /* Without a namespace it still only SAYS who needs one -- the
     * behaviour every existing host depends on, asserted before the
     * namespace is attached so it cannot rot. */
    CHECK_I64(rn_serves_snapshots(n), 0);

    rn_set_ns(n, &ns);
    rn_set_snapstore(n, store);
    rn_set_chunk_bytes(n, 128);      /* 300 bytes => three chunks, then the empty file */
    CHECK_I64(rn_serves_snapshots(n), 1);

    rn_start(n, 0, 0.0);
    for (int64_t t = 0; t <= 2000 && rn_role(n) != RAFT_LEADER; t += 10)
        rn_tick(n, t, 0.0);
    CHECK_FATAL(rn_role(n) == RAFT_LEADER);

    /*
     * ---- the follower's half, by hand: take each chunk, write it where
     * it says, answer. This is what stage three moves into C; here it is
     * a stand-in whose only job is to prove the LEADER's side.
     */
    /* The learner's log is EMPTY, so the leader's first AppendEntries
     * fails its consistency check and the leader backs off -- straight
     * past the base of a log that begins at 40, which is the moment an
     * install becomes the only way to catch this peer up. Answering that
     * first heartbeat the way a real empty follower would is what puts
     * the node in that state; nothing here reaches into it. */
    {
        int backed_off = 0;
        for (int pass = 0; pass < 20 && !backed_off; pass++) {
            uint32_t count = rn_out_count(n);
            for (uint32_t i = 0; i < count; i++) {
                uint32_t len = 0;
                const uint8_t *bytes = rn_out_bytes(n, i, &len);
                uint64_t corr = rn_out_corr(n, i);
                int kind = -1;
                CHECK_OK(rmsg_kind(bytes, len, &kind));
                if (kind != RAFT_MSG_APPEND_ENTRIES) continue;
                doc *r = doc_new();
                doc_int(r, "term", (int64_t)elog_current_term(log));
                doc_key(r, "success"); bj_put_bool(r->b, 0);
                doc_int(r, "hintIndex", 0);      /* "I have nothing at all" */
                uint32_t rlen; const uint8_t *rbuf = doc_done(r, &rlen);
                rn_out_clear(n);
                CHECK_OK(rn_on_reply(n, corr, rbuf, rlen, 0.0));
                doc_free(r);
                backed_off = 1;
                break;
            }
            if (!backed_off) { rn_out_clear(n); rn_tick(n, 50 + pass * 60, 0.0); }
        }
        CHECK_I64(backed_off, 1);
    }

    uint8_t received[512];
    memset(received, 0, sizeof received);
    uint64_t high_water = 0;
    int chunks = 0, saw_manifest = 0, saw_journal = 0, saw_done = 0;
    uint64_t seen_index = 0, seen_term = 0;

    for (int pass = 0; pass < 40; pass++) {
        uint32_t count = rn_out_count(n);
        if (!count) { rn_tick(n, 100 + pass * 10, 0.0); continue; }

        for (uint32_t i = 0; i < count; i++) {
            uint32_t len = 0;
            const uint8_t *bytes = rn_out_bytes(n, i, &len);
            uint64_t corr = rn_out_corr(n, i);
            int kind = -1;
            CHECK_OK(rmsg_kind(bytes, len, &kind));
            if (kind != RAFT_MSG_INSTALL_SNAPSHOT) continue;   /* heartbeats */

            raft_install in;
            CHECK_OK(rmsg_install_read(bytes, len, &in));
            chunks++;
            seen_index = in.last_included_index;
            seen_term = in.last_included_term;

            /* The manifest rides the FIRST chunk and no other. */
            if (chunks == 1) {
                CHECK(in.manifest != NULL);
                saw_manifest = 1;
                /* And it carries the cluster's shape, which a
                 * bootstrapped follower cannot get anywhere else -- its
                 * log holds no CONFIG history. */
                const uint8_t *v; size_t vlen; int f = 0;
                CHECK_OK(obj_get_field(in.manifest, in.manifest_len,
                                       (const uint8_t *)"members", 7, &v, &vlen, &f));
                CHECK_I64(f, 1);
                /* The store's own manifest names each file; the wire's
                 * must not -- the receiver names its own. */
                f = 0;
                CHECK_OK(obj_get_field(in.manifest, in.manifest_len,
                                       (const uint8_t *)"files", 5, &v, &vlen, &f));
                CHECK_I64(f, 1);
                CHECK(find_bytes(v, vlen, "snap-1-primary.bj", 17) == NULL);
            } else {
                CHECK(in.manifest == NULL);
            }

            if (in.role && in.role_len == 7 && memcmp(in.role, "primary", 7) == 0) {
                CHECK(in.offset + in.data_len <= sizeof received);
                memcpy(received + in.offset, in.data, in.data_len);
                if (in.offset + in.data_len > high_water) high_water = in.offset + in.data_len;
            } else if (in.role && in.role_len == 7 && memcmp(in.role, "journal", 7) == 0) {
                saw_journal = 1;
                CHECK_I64((int64_t)in.data_len, 0);   /* empty, and still sent */
            }
            if (in.done) saw_done = 1;

            dbuf reply = {0};
            CHECK_OK(rmsg_build_install_reply(elog_current_term(log), 1, 0, &reply));
            rn_out_clear(n);
            CHECK_OK(rn_on_reply(n, corr, reply.data, (uint32_t)reply.len, 0.0));
            dbuf_free(&reply);
            goto next_pass;   /* one message per pass: the outbox moved */
        }
        rn_out_clear(n);
next_pass:
        if (saw_done) break;
    }

    /* Every byte of the real file arrived, at the offsets it was sent
     * with -- and the empty file arrived too. */
    CHECK_I64(saw_manifest, 1);
    CHECK_I64(saw_journal, 1);
    CHECK_I64(saw_done, 1);
    CHECK_I64((int64_t)high_water, (int64_t)PRIMARY);
    CHECK(memcmp(received, content, PRIMARY) == 0);
    CHECK(chunks >= 4);                       /* 300/128 = 3, plus the empty file */
    CHECK_I64((int64_t)seen_index, 40);       /* the boundary the manifest declares */
    CHECK_I64((int64_t)seen_term, 6);

    /* And the peer now stands at the boundary: what an install is FOR.
     * raft_repl_installed decides that, not this file. */
    CHECK_I64((int64_t)rn_match(n, 2), 40);
    CHECK_I64((int64_t)rn_next(n, 2), 41);

    free(content);
    bj_builder_free(members);
    rn_free(n);
    elog_free(log);
    if (lio.close) lio.close(lio.ctx);
    dbuf_free(&manifest);
    sst_free(store);
    bjns_posix_free(&ns);
    close(dirfd);
}

TEST(a_reply_goes_to_whoever_the_message_says_sent_it) {
    /*
     * The host used to be asked who a message came from, and the JS one
     * did not know: its transport pairs request and reply with a
     * promise and carries no sender id, so it passed 0. That 0 rode
     * straight into the outbox, breaking raft_node.h's own "a peer of 0
     * never appears" invariant on every inbound message -- harmlessly
     * there, because that host picks its reply out by correlation id,
     * and not at all harmlessly for a host that routes by address.
     *
     * Every message in the grammar names its sender, so nobody has to be
     * told. A fact nobody has to state is a fact nobody can state
     * wrongly.
     */
    memfs *fs = memfs_new();
    CHECK_FATAL(fs != NULL);
    bj_io io;
    CHECK_FATAL(memfs_open(fs, "raft.bj", &io) == BJ_OK);
    elog *log = elog_create(&io);
    CHECK_FATAL(log != NULL);
    raft_node *n = rn_new(2, log);
    CHECK_FATAL(n != NULL);
    rn_start(n, 0, 0.5);

    /* A vote request from candidate 7 -- who is not even a member of
     * this node's (empty) config, and still gets answered: it may be a
     * member of a newer one we have not seen. */
    bj_builder *rv = bj_builder_new();
    bj_begin_object(rv);
    msg_kind(rv, "requestVote");
    msg_kv_int(rv, "term", 3);
    msg_kv_int(rv, "candidateId", 7);
    msg_kv_int(rv, "lastLogIndex", 0);
    msg_kv_int(rv, "lastLogTerm", 0);
    bj_end_object(rv);
    size_t rvlen; const uint8_t *rvbuf = bj_builder_data(rv, &rvlen);

    CHECK_OK(rn_handle(n, 99, rvbuf, (uint32_t)rvlen, 0.5));
    CHECK_I64((long long)rn_out_count(n), 1);
    CHECK_I64((long long)rn_out_peer(n, 0), 7);    /* the candidate, not 0 */
    CHECK_I64((long long)rn_out_corr(n, 0), 99);   /* the sender's id, echoed */
    CHECK_I64(rn_out_is_reply(n, 0), 1);
    rn_out_clear(n);

    /* An AppendEntries is answered to its leader. */
    bj_builder *ae = bj_builder_new();
    bj_begin_object(ae);
    msg_kind(ae, "appendEntries");
    msg_kv_int(ae, "term", 3);
    msg_kv_int(ae, "leaderId", 5);
    msg_kv_int(ae, "prevLogIndex", 0);
    msg_kv_int(ae, "prevLogTerm", 0);
    msg_kv_int(ae, "leaderCommit", 0);
    bj_end_object(ae);
    size_t aelen; const uint8_t *aebuf = bj_builder_data(ae, &aelen);
    CHECK_OK(rn_handle(n, 100, aebuf, (uint32_t)aelen, 0.5));
    CHECK_I64((long long)rn_out_count(n), 1);
    CHECK_I64((long long)rn_out_peer(n, 0), 5);
    rn_out_clear(n);

    /* A message that names nobody is one nobody can answer: refused,
     * with nothing queued, rather than replied to at address 0. */
    bj_builder *anon = bj_builder_new();
    bj_begin_object(anon);
    msg_kind(anon, "requestVote");
    msg_kv_int(anon, "term", 3);
    msg_kv_int(anon, "candidateId", 0);
    bj_end_object(anon);
    size_t anonlen; const uint8_t *anonbuf = bj_builder_data(anon, &anonlen);
    CHECK_I64(rn_handle(n, 101, anonbuf, (uint32_t)anonlen, 0.5), RAFT_ERR_MESSAGE);
    CHECK_I64((long long)rn_out_count(n), 0);

    bj_builder_free(anon);
    bj_builder_free(ae);
    bj_builder_free(rv);
    rn_free(n);
    elog_free(log);
    memfs_free(fs);
}

/* Build `{kind, ...}` for the membership grammar. */
static void put_member(bj_builder *b, const char *key, uint64_t id,
                       const char *host, int64_t port) {
    bj_put_key(b, (const uint8_t *)key, (uint32_t)strlen(key));
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"id", 2);
    bj_put_int(b, (int64_t)id);
    if (host) {
        bj_put_key(b, (const uint8_t *)"host", 4);
        bj_put_string(b, (const uint8_t *)host, (uint32_t)strlen(host));
        bj_put_key(b, (const uint8_t *)"port", 4);
        bj_put_int(b, port);
    }
    bj_end_object(b);
}

/* A top-level boolean field of a reply. -1 when absent. */
static int reply_bool(const uint8_t *r, uint32_t len, const char *key) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(r, len, (const uint8_t *)key, (uint32_t)strlen(key), &v, &vlen, &found) || !found)
        return -1;
    return vlen >= 1 && v[0] == BJ_TYPE_TRUE;
}

TEST(join_leave_and_timeout_now_are_answered_without_a_host) {
    /*
     * The three kinds rn_handle used to refuse. Each needed something
     * only a host had: a promise for join and leave, and (for the
     * redirect a follower gives) the ADDRESSES, which lived in the
     * host's member list rather than in the node.
     *
     * Both are gone. The node keeps the adopted records -- addresses
     * included -- so it can say where the leader is; and a request it
     * cannot answer yet is parked, answered when the CONFIG entry it
     * needs lands. Nothing here waits on JavaScript.
     */
    memfs *fs = memfs_new();
    CHECK_FATAL(fs != NULL);
    bj_io io;
    CHECK_FATAL(memfs_open(fs, "raft.bj", &io) == BJ_OK);
    elog *log = elog_create(&io);
    CHECK_FATAL(log != NULL);
    raft_node *n = rn_new(1, log);
    CHECK_FATAL(n != NULL);

    /* A two-member group, WITH addresses. */
    bj_builder *members = bj_builder_new();
    bj_begin_array(members);
    for (int i = 1; i <= 2; i++) {
        bj_begin_object(members);
        bj_put_key(members, (const uint8_t *)"id", 2);
        bj_put_int(members, i);
        bj_put_key(members, (const uint8_t *)"host", 4);
        bj_put_string(members, (const uint8_t *)(i == 1 ? "node1" : "node2"), 5);
        bj_put_key(members, (const uint8_t *)"port", 4);
        bj_put_int(members, 7000 + i);
        bj_end_object(members);
    }
    bj_end_array(members);
    size_t mlen; const uint8_t *mbuf = bj_builder_data(members, &mlen);
    CHECK_OK(rn_set_members(n, mbuf, (uint32_t)mlen));
    rn_start(n, 0, 0.5);

    /* A join to a FOLLOWER is redirected -- and the redirect carries the
     * leader's address, because a joiner knows addresses, not ids. */
    /* A fresh node is a follower, which is the case under test. */
    bj_builder *jb = bj_builder_new();
    bj_begin_object(jb);
    bj_put_key(jb, (const uint8_t *)"kind", 4);
    bj_put_string(jb, (const uint8_t *)"join", 4);
    put_member(jb, "member", 3, "node3", 7003);
    bj_end_object(jb);
    size_t jlen; const uint8_t *jbuf = bj_builder_data(jb, &jlen);

    CHECK_OK(rn_handle(n, 41, jbuf, (uint32_t)jlen, 0.5));
    CHECK_I64((long long)rn_out_count(n), 1);
    CHECK_I64((long long)rn_out_corr(n, 0), 41);
    {
        uint32_t rl = 0;
        const uint8_t *r = rn_out_bytes(n, 0, &rl);
        CHECK_I64(reply_bool(r, rl, "ok"), 0);
        /* Nobody leads yet, so there is nobody to point at -- but the
         * SHAPE is the redirect, not a refusal. */
        const uint8_t *v; size_t vlen; int found = 0;
        CHECK_OK(obj_get_field(r, rl, (const uint8_t *)"leaderId", 8, &v, &vlen, &found));
        CHECK_I64(found, 1);
    }
    rn_out_clear(n);

    /* Elect it (single voter? no -- two members, so drive a real term by
     * hand is overkill; campaign and count our own vote is enough here). */
    while (rn_role(n) != RAFT_LEADER && rn_commit_index(n) == 0) {
        /* One voter cannot elect itself out of a two-member group, so
         * shrink to one member first: this test is about the three
         * handlers, not about elections. */
        bj_builder *solo = bj_builder_new();
        bj_begin_array(solo);
        bj_begin_object(solo);
        bj_put_key(solo, (const uint8_t *)"id", 2);
        bj_put_int(solo, 1);
        bj_put_key(solo, (const uint8_t *)"host", 4);
        bj_put_string(solo, (const uint8_t *)"node1", 5);
        bj_put_key(solo, (const uint8_t *)"port", 4);
        bj_put_int(solo, 7001);
        bj_end_object(solo);
        bj_end_array(solo);
        size_t slen; const uint8_t *sbuf = bj_builder_data(solo, &slen);
        CHECK_OK(rn_set_members(n, sbuf, (uint32_t)slen));
        bj_builder_free(solo);
        for (int64_t t = 0; t <= 1000 && rn_role(n) != RAFT_LEADER; t += 10)
            rn_tick(n, t, 0.5);
        break;
    }
    CHECK_I64(rn_role(n), RAFT_LEADER);
    rn_out_clear(n);
    rn_effects_clear(n);

    /* A join to the LEADER is not answered yet: the answer is a fact
     * about a CONFIG entry that has not committed. */
    CHECK_OK(rn_handle(n, 42, jbuf, (uint32_t)jlen, 0.5));
    CHECK_I64((long long)rn_out_count(n), 0);   /* parked, not refused */
    CHECK_I64(rn_config_in_flight(n), 1);

    /* A second request while that one is in flight is told to retry --
     * changes serialize, and saying so beats queueing forever. */
    CHECK_OK(rn_handle(n, 43, jbuf, (uint32_t)jlen, 0.5));
    CHECK_I64((long long)rn_out_count(n), 1);
    {
        uint32_t rl = 0;
        const uint8_t *r = rn_out_bytes(n, 0, &rl);
        CHECK_I64(reply_bool(r, rl, "retry"), 1);
    }
    rn_out_clear(n);

    /*
     * The entry applies (the host's pump would call this), and the
     * parked requester gets its answer -- with the adopted records, and
     * node 3 in them as a LEARNER, whatever it asked for.
     */
    {
        uint32_t alen = 0;
        const uint8_t *adopted = rn_adopted(n, &alen);
        const uint8_t *v; size_t vlen; int found = 0;
        CHECK_OK(obj_get_field(adopted, alen, (const uint8_t *)"members", 7, &v, &vlen, &found));
        /* Re-adopting the proposed set is what apply does. Build it from
         * the entry the node just wrote. */
        uint64_t at = elog_last_index(log);
        uint64_t term = 0; int type = 0; const uint8_t *payload = NULL; size_t plen = 0;
        CHECK_OK(elog_get(log, at, &term, &type, &payload, &plen));
        CHECK_I64(type, EL_CONFIG);
        const uint8_t *set; size_t setlen; int f2 = 0;
        CHECK_OK(obj_get_field(payload, plen, (const uint8_t *)"members", 7, &set, &setlen, &f2));
        CHECK_I64(f2, 1);
        /* Copied: the payload lives in the log's output buffer, which the
         * log owns and the next operation on it reuses. */
        dbuf held = {0};
        CHECK_OK(dbuf_put(&held, set, setlen));
        CHECK_OK(rn_set_members(n, held.data, (uint32_t)held.len));
        dbuf_free(&held);
    }
    CHECK_I64(rn_config_in_flight(n), 0);
    CHECK_I64((long long)rn_out_count(n), 1);        /* the parked join */
    {
        uint32_t rl = 0;
        const uint8_t *r = rn_out_bytes(n, 0, &rl);
        CHECK_I64((long long)rn_out_corr(n, 0), 42);
        CHECK_I64(reply_bool(r, rl, "ok"), 1);
    }
    rn_out_clear(n);
    /* A learner: present, and not counted. */
    CHECK_I64((long long)rn_quorum(n), 1);           /* still one voter */

    /* Re-joining with the identical record changes nothing and says so
     * immediately -- which is what makes a retried join harmless. */
    CHECK_OK(rn_handle(n, 44, jbuf, (uint32_t)jlen, 0.5));
    CHECK_I64((long long)rn_out_count(n), 1);
    {
        uint32_t rl = 0;
        const uint8_t *r = rn_out_bytes(n, 0, &rl);
        CHECK_I64(reply_bool(r, rl, "ok"), 1);
    }
    CHECK_I64(rn_config_in_flight(n), 0);            /* no new entry */
    rn_out_clear(n);

    /* Leaving an id that is not a member is equally idempotent. */
    {
        bj_builder *lb = bj_builder_new();
        bj_begin_object(lb);
        bj_put_key(lb, (const uint8_t *)"kind", 4);
        bj_put_string(lb, (const uint8_t *)"leave", 5);
        bj_put_key(lb, (const uint8_t *)"id", 2);
        bj_put_int(lb, 99);
        bj_end_object(lb);
        size_t llen; const uint8_t *lbuf = bj_builder_data(lb, &llen);
        CHECK_OK(rn_handle(n, 45, lbuf, (uint32_t)llen, 0.5));
        CHECK_I64((long long)rn_out_count(n), 1);
        uint32_t rl = 0;
        const uint8_t *r = rn_out_bytes(n, 0, &rl);
        CHECK_I64(reply_bool(r, rl, "ok"), 1);
        CHECK_I64(rn_config_in_flight(n), 0);
        rn_out_clear(n);
        bj_builder_free(lb);
    }

    /* TimeoutNow: a LEADER refuses (it is already what the sender wants
     * this node to become), and the refusal still carries its term. */
    {
        bj_builder *tb = bj_builder_new();
        bj_begin_object(tb);
        bj_put_key(tb, (const uint8_t *)"kind", 4);
        bj_put_string(tb, (const uint8_t *)"timeoutNow", 10);
        bj_put_key(tb, (const uint8_t *)"term", 4);
        bj_put_int(tb, (int64_t)elog_current_term(log));
        bj_put_key(tb, (const uint8_t *)"leaderId", 8);
        bj_put_int(tb, 2);
        bj_end_object(tb);
        size_t tlen; const uint8_t *tbuf = bj_builder_data(tb, &tlen);
        CHECK_OK(rn_handle(n, 46, tbuf, (uint32_t)tlen, 0.5));
        CHECK_I64((long long)rn_out_count(n), 1);
        uint32_t rl = 0;
        const uint8_t *r = rn_out_bytes(n, 0, &rl);
        CHECK_I64(reply_bool(r, rl, "ok"), 0);
        CHECK_I64((long long)rn_out_peer(n, 0), 2);   /* answered to its sender */
        rn_out_clear(n);
        bj_builder_free(tb);
    }

    bj_builder_free(jb);
    bj_builder_free(members);
    rn_free(n);
    elog_free(log);
    memfs_free(fs);
}

TEST(effects_coalesce_rather_than_pile_up_and_a_loss_is_never_silent) {
    /*
     * The effect queue used to DROP when full, with a comment saying the
     * host was not draining. Silence is the one thing it must not do: a
     * lost COMMIT stalls the host's apply pump until the next one
     * happens along, a lost ROLE leaves it believing this node still
     * leads, and neither leaves a trace anywhere.
     *
     * Two halves. The actionable per-peer kinds and COMMIT coalesce, so
     * a busy node cannot fill the queue no matter how many times it is
     * called between drains -- which is what makes the size argument in
     * raft_node.c hold. And if it fills anyway, the node records that it
     * failed to speak, permanently, and the host halts on it.
     */
    memfs *fs = memfs_new();
    CHECK_FATAL(fs != NULL);
    bj_io io;
    CHECK_FATAL(memfs_open(fs, "raft.bj", &io) == BJ_OK);
    elog *log = elog_create(&io);
    CHECK_FATAL(log != NULL);
    raft_node *n = rn_new(1, log);
    CHECK_FATAL(n != NULL);

    bj_builder *members = bj_builder_new();
    bj_begin_array(members);
    bj_begin_object(members);
    bj_put_key(members, (const uint8_t *)"id", 2);
    bj_put_int(members, 1);
    bj_end_object(members);
    bj_end_array(members);
    size_t mlen; const uint8_t *mbuf = bj_builder_data(members, &mlen);
    CHECK_OK(rn_set_members(n, mbuf, (uint32_t)mlen));

    rn_start(n, 0, 0.0);
    for (int64_t t = 0; t <= 1000 && rn_role(n) != RAFT_LEADER; t += 10)
        rn_tick(n, t, 0.5);
    CHECK_I64(rn_role(n), RAFT_LEADER);
    CHECK_I64(rn_effects_lost(n), 0);
    rn_effects_clear(n);

    /* A lone leader commits on every append. Fifty proposals without a
     * single drain: fifty commit reports, one slot. */
    for (int i = 0; i < 50; i++) {
        uint64_t at = 0;
        CHECK_OK(rn_propose(n, EL_NORMAL, (const uint8_t *)"x", 1, &at));
    }
    uint32_t commits = 0;
    uint64_t reported = 0;
    for (uint32_t i = 0; i < rn_effect_count(n); i++) {
        if (rn_effect_kind_at(n, i) != RN_EFFECT_COMMIT) continue;
        commits++;
        reported = rn_effect_arg(n, i);
    }
    CHECK_I64((long long)commits, 1);
    /* And it carries the LATEST index, not the first: the host acts on
     * the newest truth, not the oldest. */
    CHECK_I64((long long)reported, (long long)rn_commit_index(n));
    CHECK_I64(rn_effects_lost(n), 0);
    rn_effects_clear(n);

    /* Now the belt. Role changes are a trail, not a state, so they
     * append -- a host that never drains can still overrun them, and
     * that host must be told rather than quietly kept in the dark. */
    for (int i = 0; i < 400 && !rn_effects_lost(n); i++) {
        /* Deposed by a newer term, then told to stand again. */
        rn_observe_leader(n, elog_current_term(log) + 1, 2, 0.5);
        rn_campaign(n, 0.5);
    }
    CHECK_I64(rn_effects_lost(n), 1);
    /* Sticky: draining cannot un-lose what was never said. */
    rn_effects_clear(n);
    CHECK_I64(rn_effects_lost(n), 1);

    bj_builder_free(members);
    rn_free(n);
    elog_free(log);
    memfs_free(fs);
}

TEST(a_member_set_is_adopted_whole_or_refused_whole) {
    /*
     * The node's peer table and the host's member list are one fact
     * derived twice, from the same raft_members_adopt. They agree only
     * as long as neither can quietly keep a DIFFERENT version of it --
     * so a set this build cannot hold is refused, not trimmed to fit,
     * and the refusal leaves the previous set untouched.
     *
     * This used to stop at RN_MAX_PEERS and return success. A group over
     * the cap would have left the host replicating, in its own
     * bookkeeping, to members the node had never heard of, with a quorum
     * counted over one list and cursors kept for the other.
     */
    memfs *fs = memfs_new();
    CHECK_FATAL(fs != NULL);
    bj_io io;
    CHECK_FATAL(memfs_open(fs, "raft.bj", &io) == BJ_OK);
    elog *log = elog_create(&io);
    CHECK_FATAL(log != NULL);
    raft_node *n = rn_new(1, log);
    CHECK_FATAL(n != NULL);

    /* A three-node group, adopted normally. */
    bj_builder *ok = bj_builder_new();
    bj_begin_array(ok);
    for (int i = 1; i <= 3; i++) {
        bj_begin_object(ok);
        bj_put_key(ok, (const uint8_t *)"id", 2);
        bj_put_int(ok, i);
        bj_end_object(ok);
    }
    bj_end_array(ok);
    size_t oklen; const uint8_t *okbuf = bj_builder_data(ok, &oklen);
    CHECK_OK(rn_set_members(n, okbuf, (uint32_t)oklen));
    CHECK_I64(rn_quorum(n), 2);
    CHECK_I64((long long)rn_next(n, 2), 1);
    CHECK_I64((long long)rn_next(n, 3), 1);

    /* One member too many for this build: refused by capacity. */
    uint32_t cap = rn_max_peers();
    bj_builder *big = bj_builder_new();
    bj_begin_array(big);
    for (uint32_t i = 1; i <= cap + 2; i++) {   /* cap + 1 peers, plus self */
        bj_begin_object(big);
        bj_put_key(big, (const uint8_t *)"id", 2);
        bj_put_int(big, (int64_t)i);
        bj_end_object(big);
    }
    bj_end_array(big);
    size_t biglen; const uint8_t *bigbuf = bj_builder_data(big, &biglen);
    CHECK_I64(rn_set_members(n, bigbuf, (uint32_t)biglen), RAFT_ERR_CAPACITY);

    /* And the group it already had is exactly as it was. */
    CHECK_I64(rn_quorum(n), 2);
    CHECK_I64((long long)rn_next(n, 2), 1);
    CHECK_I64((long long)rn_next(n, 3), 1);
    CHECK_I64((long long)rn_next(n, 4), 0);   /* never adopted */

    /* Exactly at the cap is fine: the boundary is a refusal of MORE. */
    bj_builder *edge = bj_builder_new();
    bj_begin_array(edge);
    for (uint32_t i = 1; i <= cap + 1; i++) {
        bj_begin_object(edge);
        bj_put_key(edge, (const uint8_t *)"id", 2);
        bj_put_int(edge, (int64_t)i);
        bj_end_object(edge);
    }
    bj_end_array(edge);
    size_t edgelen; const uint8_t *edgebuf = bj_builder_data(edge, &edgelen);
    CHECK_OK(rn_set_members(n, edgebuf, (uint32_t)edgelen));
    CHECK_I64((long long)rn_quorum(n), (long long)((cap + 1) / 2 + 1));

    bj_builder_free(edge);
    bj_builder_free(big);
    bj_builder_free(ok);
    rn_free(n);
    elog_free(log);
    memfs_free(fs);
}

/* A vote reply as it comes off the wire. */
static void vote_reply(bj_builder *b, int64_t term, int granted) {
    bj_begin_object(b);
    msg_kv_int(b, "term", term);
    bj_put_key(b, (const uint8_t *)"voteGranted", 11);
    bj_put_bool(b, granted);
    bj_end_object(b);
}

TEST(a_stale_prevote_grant_cannot_elect_the_real_round) {
    /*
     * Regression, and the second one this port has taken from a host
     * whose replies do not arrive in the order the pump above delivers
     * them. A pre-vote round wins on its FIRST grant and immediately
     * starts a real election. The second grant -- cast in a straw poll,
     * where a peer grants freely because nothing is persisted and no
     * term moves -- was then still in flight, and landed in the real
     * round's tally.
     *
     * Both guards raft_drive.h documents pass it: the node is still a
     * candidate, and still in the term it sought. So it counted, and a
     * second node in the same term could reach a quorum on straw-poll
     * votes: a split brain arrived at through liveness code. The
     * three-node test above cannot see it, because its network is a
     * for-loop that settles every pre-vote reply before the real round
     * exists. test/raft.test.js saw it on the first run.
     *
     * Nothing but the correlation id can tell the two rounds apart --
     * which is the whole argument for having one.
     */
    memfs *fs = memfs_new();
    CHECK_FATAL(fs != NULL);
    bj_io io;
    CHECK_FATAL(memfs_open(fs, "raft.bj", &io) == BJ_OK);
    elog *log = elog_create(&io);
    CHECK_FATAL(log != NULL);
    raft_node *n = rn_new(1, log);
    CHECK_FATAL(n != NULL);

    bj_builder *members = bj_builder_new();
    bj_begin_array(members);
    for (int i = 1; i <= 3; i++) {
        bj_begin_object(members);
        bj_put_key(members, (const uint8_t *)"id", 2);
        bj_put_int(members, i);
        bj_end_object(members);
    }
    bj_end_array(members);
    size_t mlen; const uint8_t *mbuf = bj_builder_data(members, &mlen);
    CHECK_OK(rn_set_members(n, mbuf, (uint32_t)mlen));
    CHECK_I64(rn_quorum(n), 2);

    /* Stand for the pre-vote round and take its two correlation ids. */
    rn_start(n, 0, 0.0);
    for (int64_t t = 0; t <= 1000 && rn_out_count(n) == 0; t += 10) rn_tick(n, t, 0.5);
    CHECK_I64((long long)rn_out_count(n), 2);
    uint64_t pre_a = rn_out_corr(n, 0), pre_b = rn_out_corr(n, 1);
    /* Issued, never reused: the counter only ever goes up, so no reply
     * can be attributed to a request that is not the one it answers. */
    CHECK(pre_b > pre_a);
    rn_out_clear(n);
    CHECK_I64(rn_role(n), RAFT_FOLLOWER);   /* a pre-voter is nothing yet */

    /* One grant carries the straw poll, and the real round begins. */
    bj_builder *grant = bj_builder_new();
    vote_reply(grant, 0, 1);
    size_t glen; const uint8_t *gbuf = bj_builder_data(grant, &glen);
    CHECK_OK(rn_on_reply(n, pre_a, gbuf, (uint32_t)glen, 0.5));
    CHECK_I64(rn_role(n), RAFT_CANDIDATE);
    CHECK_I64((long long)elog_current_term(log), 1);
    CHECK_I64((long long)rn_out_count(n), 2);   /* the real requests */
    rn_out_clear(n);

    /* The straggler. It is a grant, from a live peer, in the term the
     * candidate is seeking -- and it must not count, because it was
     * never cast for this round. */
    CHECK_OK(rn_on_reply(n, pre_b, gbuf, (uint32_t)glen, 0.5));
    CHECK_I64(rn_role(n), RAFT_CANDIDATE);
    CHECK_I64((long long)rn_commit_index(n), 0);

    /* A grant for THIS round elects it, so the tally still works. */
    CHECK_OK(rn_on_reply(n, pre_b + 1, gbuf, (uint32_t)glen, 0.5));
    CHECK_I64(rn_role(n), RAFT_LEADER);

    bj_builder_free(grant);
    bj_builder_free(members);
    rn_free(n);
    elog_free(log);
    memfs_free(fs);
}

TEST(three_nodes_elect_a_leader_and_commit_without_a_host_language) {
    /*
     * The end-to-end claim of the whole port, at the Raft layer: three
     * nodes, one C state machine each, a network that is a for-loop, and
     * no JavaScript anywhere in the process. Everything an
     * `await transport.call` used to do is here as an outbox entry and a
     * correlation id.
     */
    rn_member m[3];
    bj_builder *members = bj_builder_new();
    bj_begin_array(members);
    for (int i = 0; i < 3; i++) {
        bj_begin_object(members);
        bj_put_key(members, (const uint8_t *)"id", 2);
        bj_put_int(members, i + 1);
        bj_end_object(members);
    }
    bj_end_array(members);
    size_t mlen; const uint8_t *mbuf = bj_builder_data(members, &mlen);

    for (int i = 0; i < 3; i++) {
        m[i].id = (uint64_t)(i + 1);
        m[i].fs = memfs_new();
        CHECK_FATAL(m[i].fs != NULL);
        bj_io io;
        CHECK_FATAL(memfs_open(m[i].fs, "raft.bj", &io) == BJ_OK);
        m[i].log = elog_create(&io);
        CHECK_FATAL(m[i].log != NULL);
        m[i].node = rn_new(m[i].id, m[i].log);
        CHECK_FATAL(m[i].node != NULL);
        CHECK_OK(rn_set_members(m[i].node, mbuf, (uint32_t)mlen));
        rn_set_timing(m[i].node, 150, 300, 50);
    }

    /* Node 1 gets the shortest timeout, so the first election is
     * deterministic rather than a race the test has to tolerate. */
    rn_start(m[0].node, 0, 0.0);
    rn_start(m[1].node, 0, 0.9);
    rn_start(m[2].node, 0, 0.95);

    CHECK_I64(rn_quorum(m[0].node), 2);

    int who = -1;
    for (int64_t t = 0; t <= 2000; t += 10) {
        for (int i = 0; i < 3; i++) rn_tick(m[i].node, t, 0.5);
        pump(m, 3);
        if (leader_count(m, 3, &who) == 1 && rn_commit_index(m[who].node) > 0) break;
    }

    /* Exactly one leader, and it committed its own term-boundary no-op --
     * which is what proves a quorum actually replicated, not just that a
     * node declared itself. */
    CHECK_I64(leader_count(m, 3, &who), 1);
    CHECK(who >= 0);
    if (who >= 0) {
        CHECK(rn_commit_index(m[who].node) > 0);

        /* A real entry replicates and commits across the cluster. */
        uint64_t at = 0;
        CHECK_OK(elog_append(m[who].log, elog_current_term(m[who].log), EL_NORMAL,
                             (const uint8_t *)"hello", 5, &at));
        CHECK_OK(elog_sync(m[who].log));
        for (int i = 0; i < 3; i++) rn_replicate(m[who].node, m[i].id);

        for (int64_t t = 2000; t <= 4000; t += 10) {
            for (int i = 0; i < 3; i++) rn_tick(m[i].node, t, 0.5);
            pump(m, 3);
            if (rn_commit_index(m[who].node) >= at) break;
        }
        CHECK(rn_commit_index(m[who].node) >= at);

        /* Every follower holds the entry, at the same term. */
        for (int i = 0; i < 3; i++) {
            CHECK(elog_last_index(m[i].log) >= at);
            uint64_t term = 0;
            CHECK_OK(elog_term_at(m[i].log, at, &term));
            CHECK_I64((long long)term, (long long)elog_current_term(m[who].log));
        }

        /* The leader can prove it still reaches a quorum. */
        CHECK_I64(rn_has_quorum_contact(m[who].node, 1000), 1);
    }

    for (int i = 0; i < 3; i++) {
        rn_free(m[i].node);
        elog_free(m[i].log);
        memfs_free(m[i].fs);
    }
    bj_builder_free(members);
}

int main(void) {
    RUN(raft_vote_follows_the_up_to_date_rule);
    RUN(raft_vote_is_at_most_one_per_term);
    RUN(raft_prevote_persists_nothing_and_respects_a_live_leader);
    RUN(raft_append_consistency_check_hints_where_to_resume);
    RUN(raft_conflict_rule_truncates_only_what_disagrees);
    RUN(raft_follower_commit_never_runs_past_its_own_log);
    RUN(raft_leader_commits_only_current_term_entries);
    RUN(raft_backoff_believes_a_follower_that_lost_its_disk);
    RUN(raft_quorum_counts_voters_only);
    RUN(raft_membership_derives_the_same_lists_everywhere);
    RUN(raft_membership_merge_cannot_erase_an_address);
    RUN(raft_request_vote_runs_end_to_end_in_c);
    RUN(install_snapshot_is_a_grammar_both_hosts_can_read);
    RUN(raft_append_entries_round_trips_between_two_logs);
    RUN(election_round_ignores_votes_from_a_world_that_ended);
    RUN(replication_picks_append_snapshot_or_park);
    RUN(snapshot_chunking_covers_every_byte_exactly_once);
    RUN(three_nodes_elect_a_leader_and_commit_without_a_host_language);
    RUN(a_single_voter_group_commits_the_moment_it_elects_itself);
    RUN(a_stale_prevote_grant_cannot_elect_the_real_round);
    RUN(a_member_set_is_adopted_whole_or_refused_whole);
    RUN(effects_coalesce_rather_than_pile_up_and_a_loss_is_never_silent);
    RUN(join_leave_and_timeout_now_are_answered_without_a_host);
    RUN(a_leader_streams_its_own_snapshot_with_no_host_to_read_the_files);
    RUN(a_reply_goes_to_whoever_the_message_says_sent_it);
    RUN(snapshot_names_round_trip_through_the_scanner);
    RUN(snapshot_adopts_the_newest_generation_that_committed);
    RUN(snapshot_refuses_a_manifest_whose_files_are_not_there);
    RUN(snapshot_validates_transferred_files_against_the_leaders_manifest);
    RUN(snapshot_commit_supersedes_the_previous_generation);
    RUN(snapshot_log_candidates_are_newest_first);
    RUN(wal_grammar_round_trips_every_op_it_can_emit);
    RUN(wal_grammar_refuses_what_it_cannot_replay);
    RUN(wal_plan_resolves_every_command_to_one_id_and_no_filter);
    RUN(a_logged_command_is_planned_and_applied_with_no_host_language);
    RUN(upsert_uses_the_id_the_filter_pinned);
    RUN(wal_plan_and_direct_upsert_insert_the_same_document);
    RUN(an_insert_without_an_id_says_which_field_is_missing);
    RUN(wal_plan_returns_the_preimage_the_host_would_have_queried_for);
    RUN(wal_plan_rejects_before_it_logs_rather_than_after);
    RUN(update_many_hands_back_post_images_when_asked);
    RUN(compaction_refuses_while_a_cursor_is_reading_the_tree);
    RUN(compact_execute_builds_and_flips_over_real_files);
    RUN(an_undeclared_open_is_caught_the_way_a_browser_catches_it);
    RUN(compaction_reclaims_space_without_the_truncate_flag);
    RUN(sweep_execute_drives_a_real_namespace);
    RUN(compact_plan_regenerates_every_name_and_keeps_every_option);
    RUN(compact_plan_advances_from_the_recorded_generation);
    RUN(collection_files_and_the_sweep_agree_by_construction);
    RUN(a_fresh_entry_carries_only_what_it_has_earned);
    RUN(sweep_plan_deletes_orphans_and_nothing_else);
    RUN(sweep_plan_spares_a_journal_an_old_entry_never_recorded);
    RUN(sweep_plan_on_an_empty_catalog_still_spares_foreign_files);
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
    RUN(a_session_resolves_a_collection_by_name_with_no_host_language);
    RUN(a_collection_that_cannot_be_opened_leaves_the_session_untouched);
    RUN(a_request_is_answered_in_binjson_with_no_transport);
    RUN(every_way_a_request_can_be_wrong_is_answered_not_thrown);
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
    RUN(a_cursor_pages_a_scan_and_belongs_to_whoever_opened_it);
    RUN(ddl_is_a_command_a_second_database_can_be_caught_up_by);
    RUN(a_database_can_be_built_from_an_empty_directory);
    RUN(compact_over_the_wire_reclaims_and_refuses_while_a_cursor_reads);
    RUN(a_list_of_writes_is_one_request_and_reports_every_member);
    RUN(current_date_is_resolved_with_the_callers_clock_or_refused);
    RUN(an_aggregate_pipeline_runs_whole_in_one_request);
    RUN(explain_names_the_plan_the_same_way_for_every_host);
    RUN(find_one_and_modify_answers_with_the_document_not_a_count);
    RUN(find_by_index_says_which_of_the_three_ways_it_was_asked_wrong);
    RUN(prune_expired_sweeps_what_a_ttl_index_says_is_over);
    RUN(a_watcher_is_told_what_another_client_wrote);
    RUN(a_sweep_is_not_a_loop_over_collections);
    RUN(strerror_covers_every_code_the_layer_can_raise);
    RUN(divergence_classification_defaults_to_halting);
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
