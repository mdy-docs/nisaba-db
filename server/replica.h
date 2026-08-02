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
 * PEERS
 *
 * Given a peers transport (server/peers.h) this is a member of a real
 * group: it stands for election against the others, replicates to them,
 * answers their requests and commits by counting them. Given NULL it is
 * a group of one -- which is a whole replica minus other replicas, and
 * exactly the shape peers were added to.
 *
 * Nothing above changed to make that true. The node has always queued
 * its messages through an outbox rather than sending them, precisely so
 * a host could grow sockets without the state machine noticing; this
 * file is what empties the outbox, and that is the whole of the
 * difference.
 *
 * WHAT IS NOT HERE
 *
 * Joining. The member set is what the process was started with, so
 * growing a cluster means restarting its members. rn_change_membership
 * and the join message exist and are not wired up: a joiner has to be
 * caught up, and catching one up needs the snapshot half below.
 *
 * Snapshots. Nothing compacts the log here, so no peer can fall below
 * its base and RN_EFFECT_NEEDS_SNAPSHOT cannot fire -- it is reported
 * and refused rather than served. The rule for which log to open
 * (snapstore.h's naming, and "the store's log if a generation has been
 * adopted") arrives with the compaction that needs it.
 */
#ifndef NISABA_SERVER_REPLICA_H
#define NISABA_SERVER_REPLICA_H

#include <stdint.h>
#include <stddef.h>

#include "bjns.h"
#include "dbuf.h"
#include "db_session.h"
#include "peers.h"

typedef struct replica replica;

/*
 * Open the log in `ns` and put a node over it. The member set is
 * `self_id` plus whatever `px` was told about, with each member's
 * address carried in its record so a refusal can name where the leader
 * actually is. `px` may be NULL, which is a group of one. The session,
 * the namespace and the transport are BORROWED and must outlive this.
 *
 * `now` is the same monotonic clock replica_tick will be given. The
 * node's timers measure DIFFERENCES, so starting it at zero and then
 * ticking it with a real reading makes its first tick look like a jump
 * of however long the machine has been up -- which elects it instantly
 * and hides the fact that the timer was never running.
 */
int  replica_open(bj_ns *ns, dbs *s, uint64_t self_id, peers *px, uint64_t now,
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
 * the leader's id AND ADDRESS: forwarding is a client's business, and a
 * server that did it would be holding a request it cannot promise
 * anything about. The address comes out of the member record the node
 * already holds -- an id alone would send a client back to whichever
 * member it just asked.
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
