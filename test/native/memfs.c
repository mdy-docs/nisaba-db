/* memfs.c — see memfs.h. */
#include "memfs.h"

#include "binjson.h"
#include "dbuf.h"

#include <stdlib.h>
#include <string.h>

typedef struct {
    char *name;
    dbuf  data;
} memfile;

struct memfs {
    memfile **files;   /* array of pointers: entry addresses stay stable */
    size_t    len, cap;
};

/* ---- bj_io over one memfile ------------------------------------------ */

static uint64_t mf_size(void *ctx) { return (uint64_t)((memfile *)ctx)->data.len; }

static int64_t mf_read(void *ctx, uint64_t off, uint8_t *buf, uint32_t len) {
    dbuf *d = &((memfile *)ctx)->data;
    if (off >= d->len) return 0;
    uint64_t n = d->len - off;
    if (n > len) n = len;
    memcpy(buf, d->data + off, (size_t)n);
    return (int64_t)n;
}

static int32_t mf_write(void *ctx, uint64_t off, const uint8_t *buf, uint32_t len) {
    dbuf *d = &((memfile *)ctx)->data;
    if (off + len > d->len) {
        size_t grow = (size_t)(off + len) - d->len;
        if (dbuf_ensure(d, grow) != BJ_OK) return BJ_ERR_OOM;
        /* Zero-fill any gap a sparse write would leave, as a real file
         * does. No caller here should produce one; if one ever does, this
         * makes the result deterministic instead of reading back the
         * allocator's leftovers. */
        memset(d->data + d->len, 0, grow);
        d->len = (size_t)(off + len);
    }
    if (len) memcpy(d->data + off, buf, len);
    return BJ_OK;
}

static int32_t mf_truncate(void *ctx, uint64_t len) {
    dbuf *d = &((memfile *)ctx)->data;
    if (len > d->len) return BJ_ERR_RANGE;
    d->len = (size_t)len;
    return BJ_OK;
}

/*
 * Designated initializer, assigned whole -- NOT field-by-field into the
 * caller's struct.
 *
 * This distinction is not style. Field-by-field assignment leaves any
 * member it does not mention holding whatever was on the caller's stack,
 * so when bj_io grew `sync` and `close` this function started handing out
 * ios with garbage function pointers in them -- a crash on the first
 * durability point, or worse, a jump to whatever the garbage addressed.
 * Assigning a complete initializer means a future member arrives as NULL,
 * which for both of these is the correct meaning: no sync (memory writes
 * are already as durable as memory gets, per bjio.h) and no close (the
 * namespace owns the lifetime).
 */
static void bind_io(memfile *f, bj_io *out) {
    bj_io io = {
        .ctx      = f,
        .size     = mf_size,
        .read     = mf_read,
        .write    = mf_write,
        .truncate = mf_truncate,
        .sync     = NULL,
        .close    = NULL,
    };
    *out = io;
}

/* ---- namespace -------------------------------------------------------- */

memfs *memfs_new(void) { return (memfs *)calloc(1, sizeof(memfs)); }

void memfs_free(memfs *fs) {
    if (!fs) return;
    for (size_t i = 0; i < fs->len; i++) {
        dbuf_free(&fs->files[i]->data);
        free(fs->files[i]->name);
        free(fs->files[i]);
    }
    free(fs->files);
    free(fs);
}

static memfile *find(const memfs *fs, const char *name) {
    for (size_t i = 0; i < fs->len; i++)
        if (strcmp(fs->files[i]->name, name) == 0) return fs->files[i];
    return NULL;
}

int memfs_open(memfs *fs, const char *name, bj_io *out) {
    memfile *f = find(fs, name);
    if (f) { bind_io(f, out); return BJ_OK; }

    if (fs->len == fs->cap) {
        size_t nc = fs->cap ? fs->cap * 2 : 8;
        memfile **nf = (memfile **)realloc(fs->files, nc * sizeof(*nf));
        if (!nf) return BJ_ERR_OOM;
        fs->files = nf;
        fs->cap = nc;
    }
    f = (memfile *)calloc(1, sizeof(*f));
    if (!f) return BJ_ERR_OOM;
    size_t n = strlen(name) + 1;
    f->name = (char *)malloc(n);
    if (!f->name) { free(f); return BJ_ERR_OOM; }
    memcpy(f->name, name, n);

    fs->files[fs->len++] = f;
    bind_io(f, out);
    return BJ_OK;
}

int memfs_exists(const memfs *fs, const char *name) { return find(fs, name) != NULL; }

int memfs_remove(memfs *fs, const char *name) {
    for (size_t i = 0; i < fs->len; i++) {
        if (strcmp(fs->files[i]->name, name) != 0) continue;
        dbuf_free(&fs->files[i]->data);
        free(fs->files[i]->name);
        free(fs->files[i]);
        memmove(&fs->files[i], &fs->files[i + 1],
                (fs->len - i - 1) * sizeof(*fs->files));
        fs->len--;
        return BJ_OK;
    }
    return BJ_OK;   /* absent is not an error */
}

size_t memfs_size(const memfs *fs, const char *name) {
    memfile *f = find(fs, name);
    return f ? f->data.len : 0;
}

size_t memfs_count(const memfs *fs) { return fs->len; }

const char *memfs_name_at(const memfs *fs, size_t i) {
    return i < fs->len ? fs->files[i]->name : NULL;
}

size_t memfs_total_bytes(const memfs *fs) {
    size_t total = 0;
    for (size_t i = 0; i < fs->len; i++) total += fs->files[i]->data.len;
    return total;
}
