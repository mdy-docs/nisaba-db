/*
 * server/join.h — reaching a cluster from outside it.
 *
 * A member set used to be whatever argv said, and growing a cluster
 * meant restarting every member with a longer --peer list. This is the
 * other way in: a process that knows only a SEED ADDRESS asks to be let
 * in, and the leader writes the change into the log where the cluster's
 * shape belongs.
 *
 * WHAT IS NOT DECIDED HERE. The node answers a join itself
 * (raft_node.h's rn_handle): whether it leads, whether the applicant is
 * already a member, whether another change is in flight, and that a new
 * member enters as a LEARNER whatever it asked for. This file only reads
 * the answers it gives and does what each one says:
 *
 *   ok            the change landed; the adopted records come back
 *   error         a validation refusal -- it will NEVER heal, so stop
 *   retry         a change is in flight; ask again
 *   leaderId +
 *   leaderAddress ask THAT address instead, and keep asking it
 *
 * Reading those must not become a second opinion about what they mean.
 * src/raft-host.js's seedRequest is the same loop over the same four
 * shapes, and the two are meant to stay recognisably one rule.
 *
 * IT BLOCKS, and that is why it is a separate file from the server: it
 * runs before the poll loop exists, in a process that has nothing to do
 * until it knows whether it is a member. Nothing in the steady state may
 * call it.
 *
 * A SEED IS A DIRECT ADDRESS. A load balancer in front of a member
 * breaks node identity -- the reply to a join has to come from the node
 * that parked it, and a redirect names a member rather than a service.
 * src/raft-transport-http.js documents the same rule for the same
 * reason.
 */
#ifndef NISABA_SERVER_JOIN_H
#define NISABA_SERVER_JOIN_H

#include <stdint.h>
#include <stddef.h>

#include "dbuf.h"
#include "peers.h"

typedef struct {
    char host[PEERS_HOST_MAX];
    int  port;
} seed_addr;

/* Seeds one process may be given. More than a handful is a deployment
 * listing every member, which is the argv it is trying to stop needing. */
#define JOIN_MAX_SEEDS 8

/*
 * Ask to be admitted as `{ id, host, port }`, or ask for `id` to be
 * removed. On success `members` holds the ADOPTED member records -- a
 * binjson ARRAY, exactly as the node normalized it, which is what a
 * joiner boots with so that its first act is not to elect itself.
 *
 * 0 on success. Below zero when it gave up, with `why` (if given)
 * holding the last thing that went wrong -- a refusal's own sentence
 * when there was one, because "could not join" without it tells an
 * operator nothing.
 *
 * EVERY PATH TERMINATES. A cluster mid-election is retried, a redirect
 * is followed, a refusal stops at once, and a seed that answers nothing
 * runs out of attempts.
 */
int join_cluster(const seed_addr *seeds, int nseeds, uint64_t self_id,
                 const char *self_host, int self_port,
                 dbuf *members, char *why, size_t why_cap);

int leave_cluster(const seed_addr *seeds, int nseeds, uint64_t id,
                  dbuf *members, char *why, size_t why_cap);

#endif /* NISABA_SERVER_JOIN_H */
