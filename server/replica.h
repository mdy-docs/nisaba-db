/*
 * server/replica.h — the server's Raft half.
 *
 * server/main.c is the transport and nothing else, and it stays that
 * way: it moves bytes, and it does not know what an op is. This is the
 * other thing a replicated server has to be -- a log, a raft_node, an
 * apply pump, and the bookkeeping that ties a client's request to the
 * entry it became.
 *
 * WHAT A WRITE IS NOW
 *
 * Unreplicated, dbs_handle plans a write and applies it in one call.
 * Replicated, the two halves are separated by a quorum
 * (db_session.h's dbs_propose / dbs_step), and this drives that loop:
 *
 *     replica_submit   plan, propose, and say "not yet"
 *     ... entries commit, the pump applies them ...
 *     RN_EFFECT_SETTLED   the node says whose write finished, and
 *                         whether the entry there is still theirs
 *     replica_ready    hand the finished answer back to the transport
 *
 * A REQUEST IS NOT ONE TRIP TO THE LOG. A bulkWrite's second operation
 * must be planned against a database its first has already changed, so
 * it plans one operation, replicates it, applies it, and plans the next
 * -- however many trips that takes. The loop is dbs_step's; this side
 * only decides when to turn the handle.
 *
 * WHAT IS NOT HERE
 *
 * Peers. This is one process with one member in its group, which is a
 * whole replica minus other replicas: it elects itself, appends,
 * commits by counting only itself, applies, and answers. Adding the
 * peer transport is the next step and changes nothing above -- the node
 * has always queued its messages through an outbox rather than sending
 * them, precisely so a host can grow sockets without the state machine
 * noticing.
 *
 * Snapshots. With no peers there is nobody to install one, so the log is
 * the plain WAL rather than a generation's paired log. The rule for
 * which one to open (snapstore.h's naming, and "the store's log if a
 * generation has been adopted") arrives with the peers that need it.
 */
#ifndef NISABA_SERVER_REPLICA_H
#define NISABA_SERVER_REPLICA_H

#include <stdint.h>
#include <stddef.h>

#include "bjns.h"
#include "dbuf.h"
#include "db_session.h"

typedef struct replica replica;

/*
 * Open the log in `ns` and put a node over it, with `self_id` as its
 * only member. The session and the namespace are BORROWED and must
 * outlive this.
 *
 * `now` is the same monotonic clock replica_tick will be given. The
 * node's timers measure DIFFERENCES, so starting it at zero and then
 * ticking it with a real reading makes its first tick look like a jump
 * of however long the machine has been up -- which elects it instantly
 * and hides the fact that the timer was never running.
 */
int  replica_open(bj_ns *ns, dbs *s, uint64_t self_id, uint64_t now,
                  replica **out);
void replica_close(replica *r);

/* Whether this node can take a write right now. */
int      replica_is_leader(const replica *r);
uint64_t replica_leader_id(const replica *r);

/*
 * How long the transport may sleep before this needs the clock again, in
 * milliseconds. The election and heartbeat timers are the only things
 * here that happen because time passed rather than because a socket did.
 */
int replica_wait_ms(const replica *r, uint64_t now);

/* Drive the clock, then the pump. */
int replica_tick(replica *r, uint64_t now);

/*
 * One request.
 *
 *   0  answered outright -- `out` holds the response. Reads, pings,
 *      refusals, and anything at all on a server with no --raft.
 *   1  accepted, and the answer comes later through replica_ready.
 *  <0  no response could be built, which is the transport's problem.
 *
 * A write on a node that is not the leader is answered outright, with
 * the leader's id: forwarding is a client's business, and a server that
 * did it would be holding a request it cannot promise anything about.
 */
int replica_submit(replica *r, uint64_t client, const uint8_t *req, size_t len,
                   dbuf *out);

/*
 * An answer that became ready since the last call: *have says whether
 * there was one, *client says whose it is. Drained like the outbox --
 * repeatedly, until it says no.
 */
int replica_ready(replica *r, uint64_t *client, dbuf *out, int *have);

/* A client that has gone: its pending writes are answers nobody will
 * collect. The entries they proposed still commit and still apply --
 * that is what committed means -- but the response has nowhere to go. */
void replica_drop_client(replica *r, uint64_t client);

#endif /* NISABA_SERVER_REPLICA_H */
