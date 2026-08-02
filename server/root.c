/* server/root.c — see root.h. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "root.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "binjson.h"
#include "bjio_posix.h"
#include "db_validate.h"
#include "dbuf.h"

/*
 * bjns_posix_open takes a dirfd and does NOT own it, so somebody has to
 * remember the fd for as long as the namespace lives. That somebody is
 * here, keyed by the namespace's own ctx pointer -- the one thing the
 * instance hands back at close time.
 */
typedef struct {
    void *ns_ctx;
    int   fd;
} open_dir;

struct root_state {
    int      fd;                          /* the root; borrowed */
    open_dir open[DBI_MAX_DATABASES];
    int      n;
};

root_state *root_new(int rootfd) {
    root_state *st = (root_state *)calloc(1, sizeof *st);
    if (!st) return NULL;
    st->fd = rootfd;
    return st;
}

void root_free(root_state *st) {
    if (!st) return;
    for (int i = 0; i < st->n; i++) if (st->open[i].fd >= 0) close(st->open[i].fd);
    free(st);
}

/* A NUL-terminated copy of a counted name, bounded by what a database
 * name may be. */
static int as_path(const char *name, uint32_t len, char *buf, size_t cap) {
    if (len == 0 || len >= cap) return -1;
    memcpy(buf, name, len);
    buf[len] = '\0';
    /* dc_check_db_name has already refused '/' and NUL; these two are
     * the ones a NAME may legally contain and a PATH SEGMENT may not. */
    if (strcmp(buf, ".") == 0 || strcmp(buf, "..") == 0) return -1;
    return 0;
}

static int root_open_ns(void *ctx, const char *name, uint32_t len, int create,
                        bj_ns *out) {
    root_state *st = (root_state *)ctx;
    char path[DBI_NAME_MAX];
    if (as_path(name, len, path, sizeof path) != 0) return DC_ERR_INVALID_DB_NAME;
    if (st->n >= DBI_MAX_DATABASES) return DC_ERR_TOO_MANY_DATABASES;

    int fd = openat(st->fd, path, O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        if (errno != ENOENT || !create) return DC_ERR_NO_DATABASE;
        if (mkdirat(st->fd, path, 0777) != 0 && errno != EEXIST) return DC_ERR_NO_DATABASE;
        fd = openat(st->fd, path, O_RDONLY | O_DIRECTORY);
        if (fd < 0) return DC_ERR_NO_DATABASE;
    }
    if (bjns_posix_open(fd, out) != BJ_OK) { close(fd); return BJ_ERR_STATE; }
    st->open[st->n].ns_ctx = out->ctx;
    st->open[st->n].fd = fd;
    st->n++;
    return BJ_OK;
}

static void root_close_ns(void *ctx, bj_ns *ns) {
    root_state *st = (root_state *)ctx;
    for (int i = 0; i < st->n; i++) {
        if (st->open[i].ns_ctx != ns->ctx) continue;
        close(st->open[i].fd);
        st->open[i] = st->open[st->n - 1];
        st->n--;
        break;
    }
    bjns_posix_free(ns);
}

/* The subdirectories of the root, as a binjson ARRAY of STRINGs.
 *
 * DIRECTORIES ONLY, which is also what keeps the log out of the answer:
 * `__wal__.bj` and a snapshot generation live in this same directory
 * beside the databases, and a listing that named them would be telling a
 * client about files it has no business knowing. */
static int root_list(void *ctx, dbuf *out) {
    root_state *st = (root_state *)ctx;
    /* fdopendir CONSUMES the fd, and the root's is borrowed and still
     * needed -- so a second one, through openat rather than dup(), which
     * wasi-libc does not have. */
    int scan = openat(st->fd, ".", O_RDONLY | O_DIRECTORY);
    if (scan < 0) return BJ_ERR_STATE;
    DIR *d = fdopendir(scan);
    if (!d) { close(scan); return BJ_ERR_STATE; }

    bj_builder *b = bj_builder_new();
    if (!b) { closedir(d); return BJ_ERR_OOM; }
    int e = bj_begin_array(b);
    for (struct dirent *ent; !e && (ent = readdir(d)) != NULL; ) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
        /* d_type is not portable (and is DT_UNKNOWN on some filesystems
         * and under WASI), so ask. */
        struct stat sb;
        if (fstatat(st->fd, ent->d_name, &sb, 0) != 0) continue;
        if (!S_ISDIR(sb.st_mode)) continue;
        size_t nlen = strlen(ent->d_name);
        if (nlen >= DBI_NAME_MAX) continue;   /* not a name this build can open */
        e = bj_put_string(b, (const uint8_t *)ent->d_name, (uint32_t)nlen);
    }
    closedir(d);
    if (!e) e = bj_end_array(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t n = 0;
        const uint8_t *p = bj_builder_data(b, &n);
        e = p ? dbuf_put(out, p, n) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    return e;
}

/* Everything in `fd`, then `fd`'s own entry in its parent. Bounded
 * depth, because a database directory is files and a stray directory in
 * one is somebody else's mistake rather than a structure to walk. */
static int remove_tree(int parent, const char *name, int depth) {
    if (depth > 4) return BJ_ERR_DEPTH;
    int fd = openat(parent, name, O_RDONLY | O_DIRECTORY);
    if (fd < 0) return errno == ENOENT ? BJ_OK : BJ_ERR_STATE;

    int scan = openat(fd, ".", O_RDONLY | O_DIRECTORY);
    if (scan < 0) { close(fd); return BJ_ERR_STATE; }
    DIR *d = fdopendir(scan);      /* consumes `scan`; `fd` stays ours */
    if (!d) { close(scan); close(fd); return BJ_ERR_STATE; }

    int e = BJ_OK;
    for (struct dirent *ent; (ent = readdir(d)) != NULL; ) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
        struct stat sb;
        if (fstatat(fd, ent->d_name, &sb, 0) != 0) { e = BJ_ERR_STATE; break; }
        if (S_ISDIR(sb.st_mode)) {
            e = remove_tree(fd, ent->d_name, depth + 1);
            if (e) break;
        } else if (unlinkat(fd, ent->d_name, 0) != 0 && errno != ENOENT) {
            e = BJ_ERR_STATE;
            break;
        }
    }
    closedir(d);
    close(fd);
    if (e) return e;
    if (unlinkat(parent, name, AT_REMOVEDIR) != 0 && errno != ENOENT) return BJ_ERR_STATE;
    return BJ_OK;
}

static int root_remove(void *ctx, const char *name, uint32_t len, int *removed) {
    root_state *st = (root_state *)ctx;
    *removed = 0;
    char path[DBI_NAME_MAX];
    if (as_path(name, len, path, sizeof path) != 0) return DC_ERR_INVALID_DB_NAME;

    struct stat sb;
    if (fstatat(st->fd, path, &sb, 0) != 0) return BJ_OK;   /* nothing there */
    if (!S_ISDIR(sb.st_mode)) return BJ_OK;                 /* not a database */

    int e = remove_tree(st->fd, path, 0);
    if (e) return e;
    *removed = 1;
    return BJ_OK;
}

void root_fill(root_state *st, dbi_root *out) {
    out->ctx = st;
    out->open_ns = root_open_ns;
    out->close_ns = root_close_ns;
    out->list = root_list;
    out->remove = root_remove;
}
