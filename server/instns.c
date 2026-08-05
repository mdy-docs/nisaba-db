/* server/instns.c — see instns.h. Mirrors bjio_posix's directory
 * namespace exactly, except for the one-level nesting rule its header
 * explains — including the WASI create-to-write workaround and the
 * best-effort directory sync, both documented at length in
 * third_party/binjson-structures/src/bjio_posix.c and not restated
 * here. */
/* openat/mkdirat/unlinkat: glibc gates them behind a feature-test
 * macro where macOS exposes them unconditionally -- which is exactly
 * how the omission builds on one and breaks on the other (root.c and
 * main.c carry the same guard). Before any include, or it does
 * nothing. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "instns.h"

#include "bjio_posix.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

typedef struct { int rootfd; } idir;

#define INAME_MAX 512

/*
 * `file` or `db/file`, both segments non-empty, neither `.` nor `..`,
 * no NUL, nothing deeper. The '/' position comes back so CREATE can
 * make the directory; -1 for a flat name.
 */
static int name_to_cstr(const char *name, uint32_t len, char *out, int *slash) {
    if (len == 0 || len >= INAME_MAX) return BJ_ERR_RANGE;
    *slash = -1;
    for (uint32_t i = 0; i < len; i++) {
        if (name[i] == '\0') return BJ_ERR_RANGE;
        if (name[i] != '/') continue;
        if (*slash >= 0) return BJ_ERR_RANGE;          /* one level, not two */
        if (i == 0 || i == len - 1) return BJ_ERR_RANGE;
        *slash = (int)i;
    }
    if (name[0] == '.' &&
        (len == 1 || (len == 2 && name[1] == '.') ||
         (*slash == 1) || (*slash == 2 && name[1] == '.'))) return BJ_ERR_RANGE;
    if (*slash >= 0 && name[*slash + 1] == '.' &&
        ((uint32_t)*slash + 2 == len ||
         (name[*slash + 2] == '.' && (uint32_t)*slash + 3 == len))) return BJ_ERR_RANGE;
    memcpy(out, name, len);
    out[len] = '\0';
    return BJ_OK;
}

static int32_t in_open(void *ctx, const char *name, uint32_t name_len,
                       uint32_t flags, bj_io *out) {
    char path[INAME_MAX];
    int slash = -1;
    int e = name_to_cstr(name, name_len, path, &slash);
    if (e) return (int32_t)e;

    int oflags = O_RDWR;
    if (flags & BJ_NS_CREATE) oflags |= O_CREAT;
    if (flags & BJ_NS_EXCL)   oflags |= O_EXCL;
    if (flags & BJ_NS_TRUNC)  oflags |= O_TRUNC;

    /* A created `db/file` may be the first thing in `db`: an install
     * restoring a database this member never had makes the place it
     * goes. EEXIST is the common case and not an error. */
    if ((flags & BJ_NS_CREATE) && slash > 0) {
        path[slash] = '\0';
        if (mkdirat(((idir *)ctx)->rootfd, path, 0755) != 0 && errno != EEXIST)
            return BJ_ERR_STATE;
        path[slash] = '/';
    }

#ifdef __wasi__
    /* bjio_posix.c explains this dance: legacy preview1 grants FD_WRITE
     * only to an open that CREATES, so ask for a creation we prove is a
     * no-op first -- "open, must exist" must stay BJ_ERR_STATE. */
    if (!(flags & BJ_NS_CREATE)) {
        int probe;
        do { probe = openat(((idir *)ctx)->rootfd, path, O_RDONLY); }
        while (probe < 0 && errno == EINTR);
        if (probe < 0) return BJ_ERR_STATE;
        close(probe);
        oflags |= O_CREAT;
    }
#endif

    int fd;
    do { fd = openat(((idir *)ctx)->rootfd, path, oflags, 0644); }
    while (fd < 0 && errno == EINTR);
    if (fd < 0) return BJ_ERR_STATE;

    e = bjio_posix_open_fd(fd, out);
    if (e) { close(fd); return (int32_t)e; }
    return BJ_OK;
}

static int32_t in_close(void *ctx, bj_io *io) {
    (void)ctx;
    if (!io || !io->close) return BJ_OK;
    int32_t rc = io->close(io->ctx);
    memset(io, 0, sizeof(*io));
    return rc;
}

static int32_t in_remove(void *ctx, const char *name, uint32_t name_len) {
    char path[INAME_MAX];
    int slash = -1;
    int e = name_to_cstr(name, name_len, path, &slash);
    if (e) return (int32_t)e;
    if (unlinkat(((idir *)ctx)->rootfd, path, 0) != 0) {
        if (errno == ENOENT) return BJ_OK;   /* already gone is not an error */
        return BJ_ERR_STATE;
    }
    return BJ_OK;
}

static int32_t in_sync(void *ctx) {
    if (fsync(((idir *)ctx)->rootfd) != 0) {
        if (errno == EINVAL || errno == ENOTSUP) return BJ_OK;
#ifdef __wasi__
        if (errno == EBADF || errno == ENOTCAPABLE) return BJ_OK;
#endif
        return BJ_ERR_STATE;
    }
    return BJ_OK;
}

int instns_open(int rootfd, bj_ns *out) {
    idir *d = (idir *)calloc(1, sizeof(idir));
    if (!d) return BJ_ERR_OOM;
    d->rootfd = rootfd;
    bj_ns ns = {
        .ctx    = d,
        .open   = in_open,
        .close  = in_close,
        .remove = in_remove,
        .sync   = in_sync,
    };
    *out = ns;
    return BJ_OK;
}

void instns_free(bj_ns *ns) {
    if (!ns) return;
    free(ns->ctx);
    memset(ns, 0, sizeof *ns);
}
