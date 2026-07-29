/*
 * memfs.h — an in-memory named-file namespace and its bj_io binding, for
 * the native C test harness (test/native/).
 *
 * This is the seed of what becomes bjns_mem.c once bj_ns lands: it already
 * has the shape the namespace seam needs (open-by-name, remove-by-name,
 * enumerate), so promoting it later is a rename plus a vtable, not a
 * rewrite. Until then the harness calls it directly.
 *
 * The bj_io half is third_party/binjson-structures/test/fuzz.c's mem_io
 * (lines 33-62), lifted verbatim in behavior with one deliberate change:
 * a write past end-of-file zero-fills the gap instead of refusing, which
 * is what a real file and OPFS both do (and what binjson.js's
 * MemoryHandle emulates). The structures are append-only so no caller
 * should ever create a hole -- but a harness that quietly matches real
 * filesystem semantics is one fewer difference between the native tests
 * and the thing they are standing in for.
 *
 * Handle stability matters: every file is its own allocation, so the
 * bj_io.ctx a caller is holding stays valid when other files are created
 * or removed. Do not "optimize" this into a contiguous array of entries.
 */
#ifndef MEMFS_H
#define MEMFS_H

#include <stddef.h>
#include <stdint.h>

#include "bjio.h"

typedef struct memfs memfs;

/* Allocate an empty namespace, or NULL on OOM. */
memfs *memfs_new(void);
/* Free the namespace and every file in it. Safe on NULL. Any bj_io still
 * bound to one of its files dangles afterwards. */
void memfs_free(memfs *fs);

/*
 * Bind *out to the file named `name`, creating it empty if absent. The
 * binding stays valid until memfs_remove(name) or memfs_free. Returns
 * BJ_OK, or BJ_ERR_OOM.
 */
int memfs_open(memfs *fs, const char *name, bj_io *out);

/* 1 if `name` exists, else 0. */
int memfs_exists(const memfs *fs, const char *name);

/* Drop `name` and its bytes. BJ_OK whether or not it existed (mirrors the
 * swallow-NotFoundError behavior of the OPFS provider). */
int memfs_remove(memfs *fs, const char *name);

/* Byte length of `name`, or 0 if absent. */
size_t memfs_size(const memfs *fs, const char *name);

/* Enumeration, in creation order -- the stand-in for a directory listing.
 * memfs_name_at returns NULL for an out-of-range index. */
size_t      memfs_count(const memfs *fs);
const char *memfs_name_at(const memfs *fs, size_t i);

/* Total bytes across every file -- for asserting that compaction actually
 * reclaimed space. */
size_t memfs_total_bytes(const memfs *fs);

#endif /* MEMFS_H */
