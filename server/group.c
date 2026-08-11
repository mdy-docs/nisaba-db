/* server/group.c — see group.h. */
#include "group.h"

#include <string.h>

#include "binjson.h"
#include "bjcursor.h"
#include "bjfile.h"

int group_load(bj_ns *ns, uint64_t *out) {
    *out = 0;
    if (!ns || !ns->open) return BJ_ERR_STATE;
    bj_io io;
    /* Absent is the normal state of a directory that has never been
     * given an identity, so an open failure here is not reported as
     * one -- the caller's next move is to mint one either way. */
    if (ns->open(ns->ctx, GROUP_FILE, (uint32_t)strlen(GROUP_FILE), 0, &io) != BJ_OK)
        return BJ_OK;
    uint64_t size = io.size(io.ctx);
    if (size == 0 || size > 256) {
        if (io.close) io.close(io.ctx);
        /* Empty is a create that never got its bytes written -- the same
         * as absent. Anything large is not this file. */
        return size == 0 ? BJ_OK : BJ_ERR_STATE;
    }
    uint8_t bytes[256];
    int64_t got = io.read(io.ctx, 0, bytes, (uint32_t)size);
    if (io.close) io.close(io.ctx);
    if (got != (int64_t)size) return BJ_ERR_STATE;

    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(bytes, (size_t)size, (const uint8_t *)"group", 5,
                      &v, &vlen, &found) != BJ_OK || !found)
        return BJ_ERR_STATE;
    cur c = { v, vlen, 0 };
    double d = 0;
    if (read_number(&c, &d) != BJ_OK) return BJ_ERR_STATE;
    if (d < 0) return BJ_ERR_STATE;
    *out = (uint64_t)d;
    return BJ_OK;
}

int group_store(bj_ns *ns, uint64_t group) {
    if (!ns || !ns->open) return BJ_ERR_STATE;
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"group", 5);
    if (!e) e = bj_put_int(b, (int64_t)group);
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);
    size_t n = 0;
    const uint8_t *p = e ? NULL : bj_builder_data(b, &n);
    if (!p) { bj_builder_free(b); return e ? e : BJ_ERR_STATE; }

    bj_io io;
    e = ns->open(ns->ctx, GROUP_FILE, (uint32_t)strlen(GROUP_FILE),
                 BJ_NS_CREATE | BJ_NS_TRUNC, &io);
    if (!e) {
        e = io.write(io.ctx, 0, p, (uint32_t)n);
        /* Synced before this returns: the caller is about to serve on the
         * strength of it, and a group id that survives only in memory is
         * one the next boot would mint again from a set that may have
         * changed in between. */
        if (!e && io.sync) e = io.sync(io.ctx);
        if (io.close) io.close(io.ctx);
    }
    bj_builder_free(b);
    return e;
}
