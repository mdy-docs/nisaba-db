/*
 * server/peers.h — the node-to-node transport.
 *
 * server/replica.c is a Raft member with an outbox and no way to empty
 * it. This is the way: sockets to the other members, the frame grammar
 * they speak, and the correlation bookkeeping that turns "a message went
 * out" into "a reply came back" or "it never will".
 *
 * IT KNOWS NOTHING ABOUT RAFT. Not a message kind, not a term, not what
 * an envelope contains -- it moves opaque byte strings between an id and
 * a socket, which is the same rule server/main.c holds to for clients.
 * That is what lets raft_node.h's outbox stay a queue of (peer, corr,
 * bytes) rather than growing a network.
 *
 * THE WIRE IS src/raft-transport-tcp.js's, EXACTLY
 *
 * A 4-byte little-endian length, then a binjson object:
 *
 *     { t: "req", id, env }                  a request; env is the message
 *     { t: "res", id, ok: true,  value }     its answer
 *     { t: "res", id, ok: false, error }     ...or why there isn't one
 *
 * `id` is the sender's correlation id, echoed back untouched. It is
 * copied verbatim from that file because a native member and a Node
 * member should be able to sit in ONE cluster, and a second wire format
 * would make that a translation problem forever. binjson rather than
 * JSON for the reason written down there: Raft envelopes carry entry
 * payloads and snapshot chunks as raw bytes, and binjson round-trips
 * those without base64.
 *
 * TWO SOCKETS PER PAIR, also that file's shape. Each side dials ONE
 * outbound socket per peer for its own requests, lazily, and redials
 * after a drop; inbound sockets only ever serve requests TO this node. A
 * reply therefore goes back on the socket its request arrived on, which
 * is why a request event carries `from` and not a peer id -- the sender
 * of an inbound frame has not identified itself at this layer, and
 * asking it to would be a second opinion about who sent what when the
 * message already says (raft_msg.h's rmsg_sender).
 *
 * NOTHING BLOCKS. Every socket is non-blocking, connect included: a
 * member that is down is the normal case in a cluster, and a leader that
 * stalled in connect() until TCP gave up would be a leader that stopped
 * heartbeating the members that ARE up.
 *
 * NOTHING IS DROPPED IN SILENCE. Every request either produces a reply
 * event or a fail event -- a dropped connection fails everything
 * outstanding on it, an unreachable peer fails the request at the point
 * it was made, and a peer that accepted and went quiet fails on a timer.
 * A correlation id that produces neither is a peer cursor wedged
 * forever, because the node will not replace a request it still believes
 * is in flight.
 *
 * BOUNDED, AND IT SAYS SO, like every other table in this server.
 */
#ifndef NISABA_SERVER_PEERS_H
#define NISABA_SERVER_PEERS_H

#include <stdint.h>
#include <stddef.h>

#include "dbuf.h"

typedef struct peers peers;

/* Other members this build can hold. Must not exceed rn_max_peers(),
 * which replica_open checks -- a member set the node refuses whole is
 * better found at startup than at the first election. */
#define PEERS_MAX 15

/* Inbound connections at once. One per peer is the steady state; the
 * spare slots absorb a peer that redials before the close of its old
 * socket has reached us. */
#define PEERS_IN_MAX 32

/* Descriptors this transport can ask to have watched: the listener,
 * one per peer, and the inbound table. server/main.c sizes its pollfd
 * array with this. */
#define PEERS_MAX_FDS (1 + PEERS_MAX + PEERS_IN_MAX)

#define PEERS_HOST_MAX 64

/*
 * What a drained event is. REQUEST is a peer asking; answer it with
 * peers_answer (or peers_reject) on `from`. REPLY and FAIL are the two
 * ends a request of ours can come to, and exactly one of them arrives
 * for every correlation id peers_request accepted.
 */
typedef enum {
    PEER_EV_REQUEST = 0,
    PEER_EV_REPLY   = 1,
    PEER_EV_FAIL    = 2
} peer_ev_kind;

typedef struct {
    int            kind;
    uint64_t       from;    /* REQUEST only: the conversation to answer on */
    uint64_t       corr;
    const uint8_t *bytes;   /* REQUEST: the message. REPLY: the answer.    */
    uint32_t       len;
} peer_event;

int  peers_new(peers **out);
void peers_free(peers *p);

/* A member and where to reach it. RAFT_ERR-free: BJ_ERR_STATE when the
 * table is full or the id is already there. */
int  peers_add(peers *p, uint64_t id, const char *host, int port);

/*
 * ...and the same, for a table that has to FOLLOW something. A member
 * set is not fixed at startup any more (a join adds one, a leave takes
 * one away), and a member the transport has no address for is a member
 * nothing can replicate to -- a failure that is silent and looks exactly
 * like a slow follower.
 *
 * peers_set upserts: an id already here with the same address is left
 * alone, and one whose address CHANGED loses its connection, because the
 * socket goes to where that member used to be. peers_remove closes and
 * forgets. Both fail everything outstanding on the connection they drop,
 * for the reason every other drop here does.
 *
 * The caller is the one holding the member set (server/replica.c reads
 * it back off the node); this keeps no opinion about who ought to be
 * here.
 */
int  peers_set(peers *p, uint64_t id, const char *host, int port);
int  peers_remove(peers *p, uint64_t id);

/* Start serving peer requests. Without this a node can call out but
 * cannot be called, which is a member that can never be replicated to. */
int  peers_listen(peers *p, const char *host, int port);

/*
 * The address the OTHERS dial, when it is not the one peers_listen
 * bound. peers_self_host feeds the member record a bootstrap or a join
 * writes into the log -- the address of record, forever -- and a bind
 * address is not always dialable: 0.0.0.0 is where to LISTEN, not
 * where to call. BJ_ERR_STATE if it does not fit.
 */
int  peers_set_advertised(peers *p, const char *host);

uint32_t    peers_count(const peers *p);
uint64_t    peers_id_at(const peers *p, uint32_t i);
const char *peers_host_at(const peers *p, uint32_t i);
int         peers_port_at(const peers *p, uint32_t i);
const char *peers_self_host(const peers *p);
int         peers_self_port(const peers *p);

/* ---- the poll seam ------------------------------------------------------
 *
 * peers_watch rebuilds the descriptor list and returns its length; the
 * indices it hands out are stable until the next call to it, which is
 * exactly one pass of the server's loop. server/main.c copies them into
 * its pollfd array, polls, and hands the readiness back.
 *
 * A connection that dies during the readiness pass is emptied but keeps
 * its index until the next peers_watch, so an earlier failure cannot
 * silently renumber a later one -- the same discipline main.c's own
 * connection table follows, arrived at the same way.
 */
uint32_t peers_watch(peers *p);
int      peers_fd_at(const peers *p, uint32_t i, int *want_write);
void     peers_ready(peers *p, uint32_t i, int readable, int writable, int failed);

/* Everything that happens because time passed: dial what is owed a dial,
 * push what is queued, fail what has waited too long. */
int peers_pump(peers *p, uint64_t now);

/* ---- messages -----------------------------------------------------------
 *
 * One event, or none. `bytes` points into storage this owns and is valid
 * until the next call on this transport -- drained immediately, which is
 * what the caller does anyway, having nothing else to do with it.
 */
int peers_next(peers *p, peer_event *ev, int *have);

/*
 * Send `msg` to `id` as a request under `corr`.
 *
 *   0   accepted; a REPLY or a FAIL will arrive for `corr`
 *   1   unreachable RIGHT NOW -- no event will arrive, and the caller
 *       owes its own layer the failure. Distinguished from an error
 *       because it is ordinary: a member is down, which is the case Raft
 *       exists to survive.
 *  <0   this transport is broken
 */
int peers_request(peers *p, uint64_t id, uint64_t corr,
                  const uint8_t *msg, uint32_t len);

/* Answer the request that arrived on `from` under `corr`. */
int peers_answer(peers *p, uint64_t from, uint64_t corr,
                 const uint8_t *msg, uint32_t len);

/* ...or say why there is no answer. A refusal is a response here too: a
 * sender left waiting for its own timeout is a peer cursor idle for five
 * seconds over a message we knew immediately we could not serve. */
int peers_reject(peers *p, uint64_t from, uint64_t corr, const char *error);

/* ---- one call, to an address ---------------------------------------------
 *
 * Dial, send, await one answer, hang up -- src/raft-transport-tcp.js's
 * callAddress, and the same wire as everything above.
 *
 * This is how a process reaches a cluster it is not a member of: a
 * joiner has SEEDS, which are addresses, and ids only arrive later in
 * the member records. Everything else here needs an id in the peer
 * table, which is exactly what a joiner does not have.
 *
 * IT BLOCKS, alone among the calls in this file, and only because of
 * where it is used: before the poll loop exists, by a process that has
 * nothing else to do until it has an answer. Nothing in the steady state
 * may call it -- that is what the pooled path is for.
 *
 *   0   answered; `reply` holds the message bytes
 *   1   the far end refused, and `err` holds the sentence it gave
 *  <0   no answer: unreachable, timed out, or unframeable
 */
int peers_call(const char *host, int port, const uint8_t *msg, uint32_t len,
               int timeout_ms, dbuf *reply, char *err, size_t err_cap);

#endif /* NISABA_SERVER_PEERS_H */
