/*
 * server/group.h — the cluster's durable identity.
 *
 * WHAT IT IS FOR. A process holding a --peer list and an EMPTY directory
 * cannot tell from its own disk whether it is a founding member of a new
 * cluster or a member of an old one whose disk is gone: a wipe destroys
 * exactly the evidence it would need. It has to ask, and an answer is
 * only worth having if it is durable on the answering side and specific
 * enough to act on. This is the specific part.
 *
 * IT IS GIVEN, NOT DERIVED, and the first attempt at this got that wrong.
 * Deriving a fingerprint from the member set looked free -- every founding
 * member would compute the same value from the same argv, with no
 * consensus needed at the one moment consensus is not available. It does
 * not work, and the suite said so within a run: two members of ONE
 * cluster refused each other, having fingerprinted different sets.
 *
 * The reason is not a bug to fix. The only thing every member agrees on
 * exactly is the set of member IDS: addresses differ in spelling between
 * a member's own record and the --peer entry another member was given
 * (bind versus advertised, name versus address), and voting flags change
 * as learners are promoted. And an id set of {1,2,3} is not unique across
 * clusters -- it is the commonest one there is. So a derived value can be
 * identical across members or unique across clusters, never both.
 *
 * Which leaves the thing that already knows: whoever places the cluster.
 * `--group N` is passed to every member of one cluster and differs
 * between clusters, and for a control plane putting one cluster per
 * tenant that number is something it already has. Given once, persisted
 * on first boot, and compared against what the peers report from then on.
 * Absent, there is no identity to check and only the history question
 * below applies -- which is the part that prevents data loss, and it
 * needs no identity at all.
 *
 * WHAT IT DOES NOT DO. It cannot tell a lost disk from a new member --
 * the wiped member is handed the same argv and derives the same id. That
 * question is answered by whether any peer has HISTORY, which is what
 * RAFT_MSG_IDENTITY reports and server/replica.c decides on. This file
 * only answers "which cluster", so that a refusal can say which one it
 * found and a directory carried to the wrong deployment is caught.
 */
#ifndef NISABA_SERVER_GROUP_H
#define NISABA_SERVER_GROUP_H

#include <stdint.h>

#include "bjio.h"
#include "bjns.h"
#include "dbuf.h"

/* Beside the log and the snapshot generations, in the root the instance
 * owns. Wiped with them, which is correct: a directory with no group is
 * a directory with no cluster. */
#define GROUP_FILE "__group__.bj"

/*
 * Read it. BJ_OK with *out == 0 when the file is absent or empty, which
 * is not an error -- it is what every instance looks like before the
 * first boot that writes one. A file that exists and cannot be parsed IS
 * an error: something is there and it is not this.
 */
int group_load(bj_ns *ns, uint64_t *out);

/*
 * Write it, once, durably -- created, truncated and synced before this
 * returns, because the next thing the caller does is act on the belief
 * that the cluster has an identity.
 */
int group_store(bj_ns *ns, uint64_t group);

/* The largest value that survives the round trip: a group id is written
 * as a binjson number and read back through a double (bjcursor.h's
 * read_number), so anything above 2^53 comes back as something else --
 * and a 64-bit value with the high bit set comes back NEGATIVE. Found by
 * a soak whose members all started once and then would not restart:
 * "cannot open the log: builder state error". */
#define GROUP_MAX ((1ull << 53) - 1)

#endif /* NISABA_SERVER_GROUP_H */
