/*
 * server/instns.h — the INSTANCE's namespace: the root directory, with
 * exactly one level of nesting allowed.
 *
 * bjns_posix deliberately refuses a '/' in a name — a bj_ns is ONE
 * directory and a name that walked out of it would escape the scope.
 * That rule is right and stays; this is a second namespace with a
 * different, equally deliberate scope: the ROOT of an instance, whose
 * files live both in the root itself (the log, the snapshot store's
 * generations) and one level down (each database's files). A snapshot of
 * an instance has to name both kinds through ONE namespace, because the
 * raft node opens generation files and live files through the single
 * bj_ns it was given (raft_node.h's serve and adopt paths).
 *
 * So a name here is either `file` or `db/file` — never absolute, never
 * `..`, never deeper — validated on every call, syscall-resolved by
 * openat/unlinkat which take multi-component relative paths natively and
 * under both WASI targets. Opening `db/file` with BJ_NS_CREATE creates
 * the directory too: an install restoring a database this member has
 * never had must be able to make the place it goes.
 *
 * ONE OPENER PER DIRECTORY still holds, by the same process ownership
 * root.h states: this process owns the root and everything under it.
 */
#ifndef NISABA_SERVER_INSTNS_H
#define NISABA_SERVER_INSTNS_H

#include "bjns.h"

/* Fill `out` with a namespace over `rootfd`. The fd is BORROWED and must
 * outlive the namespace. Free with instns_free. */
int  instns_open(int rootfd, bj_ns *out);
void instns_free(bj_ns *ns);

#endif /* NISABA_SERVER_INSTNS_H */
