/*
 * server/root.h — the four things an instance needs from a directory of
 * databases, done with POSIX.
 *
 * db_instance.h explains why they are not `bj_ns` operations and must
 * not become any: a bj_ns is ONE directory by construction, and it
 * deliberately cannot enumerate, because OPFS enumeration is
 * asynchronous and bjns.h passes listings in rather than offering them.
 * Opening a subdirectory, listing what is in one, and removing one whole
 * are the host's, and this is the host that has them -- native and both
 * WASI targets, all three through the same `openat`/`fdopendir` calls.
 *
 * No browser runs this file, which is the point: the constraint bj_ns is
 * built around does not apply here, and pretending it did would mean
 * inventing a way to pass a directory listing in to a process that is
 * standing in the directory.
 *
 * ONE OPENER PER DIRECTORY still holds, and still by process ownership:
 * this process owns the root and everything under it for its lifetime,
 * which is the whole answer to concurrent writers because wasi-filesystem
 * has no locking to arbitrate them with.
 */
#ifndef NISABA_SERVER_ROOT_H
#define NISABA_SERVER_ROOT_H

#include "db_instance.h"

/*
 * Fill in `out` for the directory `rootfd`. The fd is BORROWED and must
 * outlive the instance; `state` is storage this needs and is likewise
 * the caller's to keep alive (it holds the fd of every open database).
 */
typedef struct root_state root_state;

root_state *root_new(int rootfd);
void        root_free(root_state *st);
void        root_fill(root_state *st, dbi_root *out);

/*
 * The regular FILES of one directory, NUL-separated into `out`: the
 * root's own with db_len 0 (the log, the snapshot store's generations),
 * a database's with its name. Directories are never included -- the
 * split between "a database" (a directory) and "an instance file" is
 * this repository's, and a listing that blurred it would be how a
 * snapshot ate a database or a database swallowed the store. A missing
 * directory lists as empty rather than failing: the caller is usually
 * sweeping, and a directory already gone is a sweep already done.
 */
int root_list_files(root_state *st, const char *db, uint32_t db_len, dbuf *out);

#endif /* NISABA_SERVER_ROOT_H */
