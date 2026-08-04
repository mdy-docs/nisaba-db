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
 * MEMBERSHIP
 *
 * The member set is the LOG's, not argv's. argv bootstraps a cluster
 * that has none; after that the last CONFIG entry wins, at startup and
 * every time one applies -- so a member restarted with a stale --peer
 * list cannot overwrite what the cluster agreed, and a member that
 * joined needs no --peer list at all.
 *
 * The transport's address table FOLLOWS that set. A member with no
 * address here is a member nothing can replicate to, and the failure is
 * silent -- it looks exactly like a slow follower -- so the addresses
 * are read back off the node (rn_adopted, which is where they live)
 * rather than kept a second time.
 *
 * SNAPSHOTS AND THE LOG'S BOUND
 *
 * The log no longer grows without bound: past `snapshot_entries` applied
 * entries since its base, this member snapshots LOCALLY -- every
 * database's files stream into an immutable snapstore.h generation, the
 * manifest commits at the applied boundary, and the log is compacted
 * into the store's paired log file, based at that boundary and carrying
 * the suffix and hard state forward. Each member does this for itself,
 * as Raft members do; nothing about it is replicated.
 *
 * That one act is what arms everything downstream: the node serves
 * installs to peers below the new base (it always could, given a store
 * with a generation in it -- rn_set_snapstore), a resumed change stream
 * from below the base is refused with -68 instead of being a code path
 * nothing could reach, and WHICH LOG TO OPEN at startup becomes a rule
 * rather than a constant: the store's newest log that opens, else the
 * legacy __wal__.bj (the naming src/db-wal.js wrote down first).
 */
#ifndef NISABA_SERVER_REPLICA_H
#define NISABA_SERVER_REPLICA_H

#include <stdint.h>
#include <stddef.h>

#include "bjns.h"
#include "dbuf.h"
#include "db_session.h"
#include "db_instance.h"
#include "peers.h"
#include "root.h"

typedef struct replica replica;

/*
 * Open the log in `ns` and put a node over it.
 *
 * The BOOTSTRAP member set is `members` (a binjson ARRAY of records --
 * what a join came back with) when one is given, and otherwise `self_id`
 * plus whatever `px` was told about, with each member's address carried
 * in its record so a refusal can name where the leader actually is. `px`
 * may be NULL, which is a group of one.
 *
 * Bootstrap is all it is. If the log already carries a CONFIG entry that
 * set is adopted instead, and the transport's addresses are set from it
 * -- which is what makes a restart need no --peer list and a stale one
 * harmless.
 *
 * The instance, the namespace and the transport are BORROWED and must
 * outlive this.
 *
 * `ns` is the ROOT's namespace -- the log sits beside the database
 * directories, because there is ONE log for the whole instance. Which
 * database an entry is for is the entry's own business
 * (db_instance.h's envelope), and nothing here reads it.
 *
 * `now` is the same monotonic clock replica_tick will be given. The
 * node's timers measure DIFFERENCES, so starting it at zero and then
 * ticking it with a real reading makes its first tick look like a jump
 * of however long the machine has been up -- which elects it instantly
 * and hides the fact that the timer was never running.
 */
int  replica_open(bj_ns *ns, dbi *inst, uint64_t self_id, peers *px,
                  const uint8_t *members, uint32_t members_len,
                  uint64_t now, root_state *rt, uint64_t snapshot_entries,
                  replica **out);
void replica_close(replica *r);

/*
 * A committed install is waiting to be put onto the live files, and the
 * transport is the one that has to orchestrate it -- the instance must
 * be CLOSED across the adoption (the restored files are the ones its
 * open collections are positioned in) and reopened after, and the
 * instance's lifetime is the transport's, not this file's.
 *
 *     if (replica_adopt_pending(rep)) {
 *         victims = every database file, as "db/file", NUL-separated
 *         dbi_close(inst)
 *         replica_adopt(rep, victims, len)
 *         inst = dbi_open(...)
 *         replica_set_instance(rep, inst)
 *     }
 *
 * replica_adopt runs the node's adoption (files restored, log rebased to
 * the boundary), then the bookkeeping that follows it here: the legacy
 * log and superseded generations are swept, and the applied floor moves
 * to the boundary. replica_set_instance re-registers the log reader on
 * the fresh instance, so a resumed change stream works across an
 * adoption exactly as it does across a restart.
 */
int  replica_adopt_pending(const replica *r);
int  replica_adopt(replica *r, const char *victims, size_t victims_len);
void replica_set_instance(replica *r, dbi *inst);

/* Whether this node can take a write right now. */
int      replica_is_leader(const replica *r);
uint64_t replica_leader_id(const replica *r);

/* Members other than this one, as the set in force names them -- what a
 * startup check asks to find out whether the log describes a cluster
 * this process has no way of reaching. */
uint32_t replica_peer_count(const replica *r);

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
