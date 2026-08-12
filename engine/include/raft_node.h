/*
 * raft_node.h — the replication state machine, with an outbox instead of
 * a call stack.
 *
 * THE PROBLEM THIS SOLVES
 *
 * src/raft.js drives replication with `await transport.call(peer, msg)`.
 * That single line is why the Raft node cannot leave JavaScript: it
 * suspends C-equivalent logic on a promise, and there is no promise
 * under WASI or native. Asyncify and JSPI were both rejected for the
 * same reason -- they exist only in a browser, so adopting either would
 * give the server a different control flow from the browser, which is
 * the exact problem this whole effort exists to remove.
 *
 * The resolution is the one bjns.h already uses for files, applied to
 * the network: C DECIDES, the host DELIVERS, C CONSUMES the answer.
 * Nothing here blocks, and nothing here knows what a socket is.
 *
 *     rn_tick(n, now)              -- timers fire; messages queue
 *     rn_handle(n, bytes)          -- an incoming message; reply queues
 *     for i in 0 .. rn_out_count:  -- the host sends each one, however
 *         host sends rn_out_*(i)      it likes: a promise, a blocking
 *     rn_out_clear(n)                 write, a postMessage
 *     rn_on_reply(n, corr, bytes)  -- ...and feeds each answer back
 *     rn_on_fail(n, corr)          -- ...or says it never came
 *
 * Every entry point that can arm an election timer takes `random01`
 * rather than drawing one: a node that reads its own random source is a
 * node the simulator cannot replay, and test/raft-harness.js's
 * determinism is the single biggest asset this port has.
 *
 * A correlation id, not a closure, is what ties a reply to the request
 * that caused it. That is the whole difference: a closure needs a stack
 * to live on, an integer does not.
 *
 * WHAT IS AND IS NOT HERE
 *
 * Here: role, term transitions, the election timer and its round, the
 * heartbeat timer, and the per-peer replication cursors (next, match,
 * when it last answered, whether a request is outstanding). All of it
 * volatile -- the durable half is the elog this wraps, which already
 * owns currentTerm, votedFor and the entries.
 *
 * Not here, and deliberately: promises, waiters, the apply pump, the
 * snapshot transfer's file reads, and the membership-change orchestration.
 * Those need host resources -- a state machine to apply into, files to
 * read, a promise to settle -- so they stay with the host and call in.
 * raft_drive.h already gives them their decisions.
 *
 * The host calling back IN is the "what the host still owns" section at
 * the bottom: rn_propose for an entry it wants replicated, rn_campaign
 * when a transferring leader tells this node to stand, rn_observe_leader
 * for the one message class (InstallSnapshot) whose handler lives up
 * there but whose term still deposes us. Each is the exact head or tail
 * of a path C already owns -- none of them is a rule this file does not
 * otherwise enforce.
 *
 * This does not replace raft_core.h or raft_msg.h; it drives them. Every
 * safety rule is still theirs, and this file contains no `if` that
 * decides whether a vote may be granted.
 */
#ifndef RAFT_NODE_H
#define RAFT_NODE_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "dbuf.h"
#include "entrylog.h"
#include "bjns.h"
#include "snapstore.h"

#ifdef __cplusplus
extern "C" {
#endif

/* A peer that is not a member, or a correlation id nobody issued. */
#define RAFT_ERR_PEER (-52)

/* The member set is larger than this build can hold (rn_max_peers).
 * Refused whole -- see rn_set_members. */
#define RAFT_ERR_CAPACITY (-53)

/* A membership change is already in flight. Not a failure: changes
 * serialize by design (the single-server-change safety argument rests on
 * it), so this is "ask again once that one has committed". */
#define RAFT_ERR_BUSY (-54)

typedef struct raft_node raft_node;

/* Roles, matching src/raft.js's ROLE. */
typedef enum {
    RAFT_FOLLOWER  = 0,
    RAFT_CANDIDATE = 1,
    RAFT_LEADER    = 2
} raft_role;

/*
 * Create a node with `self_id`, over an already-open log. The log is
 * BORROWED: it outlives nothing here and this never frees it.
 *
 * The node starts as a follower with no election deadline -- call
 * rn_start to arm the timers, so that constructing a node is not the
 * same act as joining an election.
 */
raft_node *rn_new(uint64_t self_id, elog *log);
void       rn_free(raft_node *n);

/*
 * Point the node at a different log. EntryLog cannot rebase in place, so
 * both compaction paths -- a follower adopting an install, a leader
 * snapshotting locally -- close the old log and open a fresh one based at
 * the boundary. The node holds a BORROWED pointer, so it has to be told,
 * and the host must have quiesced everything that could touch the old one
 * before it closed it.
 */
void rn_set_log(raft_node *n, elog *log);

/*
 * The OWNERSHIP-carrying form, for local compaction (rn_set_log is the
 * bare pointer swap and leaves ownership where it was). The node takes
 * `fresh` and the io behind it as its own -- both die with rn_free, as
 * an adopted install's do -- and hands the previous log back through
 * `*old` ONLY if the host had lent it; a log the node already owned (a
 * prior install's) is freed here and `*old` is NULL, exactly rn_adopt's
 * rule. A host compacting locally therefore never has to know whose the
 * outgoing log was: free `*old` and close its io if non-NULL, and
 * nothing otherwise.
 */
void rn_swap_log(raft_node *n, elog *fresh, const bj_io *io, elog **old);

/* ---- serving a snapshot -------------------------------------------------
 *
 * A peer whose next index the log no longer holds cannot be caught up by
 * AppendEntries at any rewind, so it has to be sent a snapshot instead.
 * WHICH peer and WHEN were already decided here (raft_drive.h's
 * raft_repl_decide). What was not here is the sending, for one reason:
 * sending means reading FILES.
 *
 * Give the node a namespace and a snapshot store and it streams them
 * itself. Chunks queue through the outbox like every other message and
 * their replies arrive through rn_on_reply like every other reply -- the
 * correlation id ties one to the other, so nothing is suspended
 * mid-transfer and no closure has to survive an election.
 *
 * Without them NOTHING CHANGES: the node emits RN_EFFECT_NEEDS_SNAPSHOT
 * and the host serves the transfer itself, exactly as src/raft.js does.
 * That is what lets a C node grow this without a working host noticing.
 *
 * Both are BORROWED, like the log. The STORE is the host's to build
 * because scanning one needs a directory listing, and bj_ns deliberately
 * cannot produce one -- OPFS enumeration is asynchronous, so listings
 * are passed in (bjns.h says why). The node reads the store; it does not
 * scan it.
 */
/*
 * The cluster's durable identity, which the host reads off its own disk
 * and hands over here so that an identity question can be answered
 * without this file knowing where it is kept. Reported and never
 * interpreted: 0 means "none", and what a mismatch means is the host's
 * decision (server/replica.c makes it).
 */
void rn_set_group(raft_node *n, uint64_t group);
uint64_t rn_group(const raft_node *n);

/*
 * NO VOTE UNTIL THERE IS SOMETHING TO VOTE WITH.
 *
 * Set by a host that started this member with an EMPTY log beside peers
 * that have one. That shape has two causes and the member cannot tell them
 * apart from its own disk: it is either a member of a bootstrap set whose
 * first boot happened after the others had begun writing -- ordinary, and a
 * race a concurrent placement cannot avoid -- or a member whose directory
 * was emptied, which is not a crash Raft tolerates, because a wipe takes
 * the log, the term AND the vote and brings the member back with no memory
 * of what it promised.
 *
 * Refusing to start told the two apart by punishing both, and the ordinary
 * one is ordinary. So the member starts, replicates, and is installed into
 * -- but holds no franchise while its log is empty: it will not campaign
 * and will not grant a vote. Both halves were measured as failures before
 * either existed: two wiped members electing each other and telling a
 * member holding 7,319 committed entries to adopt their empty log, and one
 * wiped member's vote electing a log short of 16 documents a leader had
 * already acknowledged.
 *
 * IT CLEARS ITSELF on the only evidence that counts -- a non-empty log.
 * That means this member has heard from the leader of its term, holds that
 * leader's entries, and can be compared with other logs meaningfully again.
 * The term it woke into is spent DURABLY by the host before the node starts
 * (server/replica.c), so a restart cannot hand the vote back inside it.
 *
 * A cold bootstrap is untouched, and not by luck: a bootstrapped log holds
 * no CONFIG history and gains nothing until this member LEADS, so a peer
 * can only report history once a leader has existed -- which already took a
 * quorum of votes from members that were up and voting. A staggered cold
 * start therefore cannot hold itself into a deadlock: the first two members
 * see nothing to hold for, and the third arrives after an election it was
 * not needed for, holds for one heartbeat, and clears.
 */
void rn_hold_vote_while_blank(raft_node *n, int hold);

/* Whether the franchise is being held right now -- for a host that wants
 * to say so (nisaba-server puts it in `ping`). A member the SET excludes
 * is not "held": that is the cluster's answer, not this one. */
int rn_vote_held(const raft_node *n);

void rn_set_ns(raft_node *n, bj_ns *ns);
void rn_set_snapstore(raft_node *n, sst *store);

/* The namespace this node was given, or NULL. Read-back rather than a
 * second copy kept by the caller: a host that has to allocate the bj_ns
 * (the browser's does -- bjns_bridge_open builds one over a scope table)
 * needs somewhere to find it again at free time, and the node is already
 * the one place it is written down. Symmetric with rn_log. */
bj_ns *rn_ns(const raft_node *n);

/* Bytes per chunk. 0 restores the default. */
void rn_set_chunk_bytes(raft_node *n, uint32_t bytes);

/* Whether this node can serve an install itself -- what a host asks to
 * know whether it still has to. */
int rn_serves_snapshots(const raft_node *n);

/*
 * RECEIVING one. rn_handle answers RAFT_MSG_INSTALL_SNAPSHOT itself once
 * this node has a namespace and a store -- and refuses it exactly as
 * before without them, so a host that serves installs itself keeps
 * working untouched.
 *
 * C PLANS, THE HOST OPENS, C EXECUTES (bjns.h). A browser cannot open a
 * file inside a synchronous call, so before handing a chunk to rn_handle
 * the host asks which files it will touch and opens those. Under WASI
 * and native the plan is pure bookkeeping -- openat is already
 * synchronous -- but the discipline is the same in both, which is the
 * whole point of having one.
 *
 *     rn_install_plan(n, msg, len, &names)   NUL-separated, pure
 *     ... host opens each name ...
 *     rn_handle(n, corr, msg, len, random01)
 *
 * The names are the store's own (snapstore.h decides them); a caller
 * only passes them back through. An empty plan means the message needs
 * no file -- it is not an install, or it is one this node will refuse.
 */
int rn_install_plan(raft_node *n, const uint8_t *msg, uint32_t len, dbuf *out);

/* Is an install being staged right now, and what boundary does it carry?
 * For a host deciding whether a close/reopen is owed, and for tests. */
int      rn_installing(const raft_node *n);
uint64_t rn_install_boundary(const raft_node *n);

/* ---- adopting one -------------------------------------------------------
 *
 * A committed generation is a snapshot ON DISK; adopting it is putting
 * its files onto the names the database actually opens. That is the last
 * step of an install and the only irreversible one after the manifest.
 *
 * IT RUNS BETWEEN THE HOST'S CLOSE AND ITS REOPEN. Closing and reopening
 * the database stays the host's forever -- reopening rebuilds collection
 * handles and, in a browser, needs asynchronous opens -- so the sequence
 * is exactly dc_compact_execute's:
 *
 *     RN_EFFECT_INSTALLED arrives
 *     host quiesces and CLOSES the database
 *     rn_adopt_plan(n, &names)      names every file this will touch
 *     ... host makes those resolvable ...
 *     rn_adopt(n, victims, len, &old)   ONE synchronous call
 *     host REOPENS the database, and frees `old`
 *
 * One call, because between the first restored file and the last the
 * database is neither the old one nor the new one, and nothing may
 * observe it there. A sequence of awaits would have to be held apart by
 * a host-side gate; a synchronous call cannot be interleaved at all.
 *
 * `victims` is the NUL-separated list of live files to REMOVE first --
 * the ones the new generation does not have, including stale journals,
 * which would otherwise rewind a restored file on the next recovery.
 * The host supplies it because deciding which of its files are the
 * database's is the host's knowledge, not this layer's: a Raft node has
 * no idea what a collection is. A victim that is about to be restored is
 * skipped rather than removed-then-created, which bjns.h forbids
 * outright (a deferred remove could land after the create).
 *
 * The LOG is rebased here too, which is what retires `rebaseLog`: a
 * fresh log based at the snapshot's boundary, carrying the hard state
 * forward, opened through the namespace this node already has. The one
 * it replaces is handed back through `*old` -- rn_new BORROWED that log
 * and this file has never freed one. The new log is the node's own and
 * dies with rn_free.
 *
 * Ordering is the contract, as everywhere else here: the log is rebased
 * LAST, so a failure part-way leaves a node whose files moved but whose
 * log still describes the old state -- which recovery resolves by
 * replaying, rather than a log that promises entries no file holds.
 */
int rn_adopt_plan(raft_node *n, dbuf *out);
int rn_adopt(raft_node *n, const char *victims, size_t victims_len, elog **old);

/* Whether an install has committed and is waiting to be adopted, and the
 * boundary it will move this node to. A host that restarts mid-adoption
 * asks this after reopening. */
int      rn_adopt_pending(const raft_node *n);
uint64_t rn_adopt_boundary(const raft_node *n);

/*
 * The log this node is using. Normally the one the caller lent it and
 * already has -- but after rn_adopt it is one the NODE opened, and a
 * host has no other way to reach it. Read-only in spirit: the durable
 * half of the node's state is in here (currentTerm, votedFor, the
 * entries), and a host that writes to it behind the node's back is
 * writing to state the node believes it owns.
 */
elog *rn_log(const raft_node *n);

/* The most files one snapshot may have. A generation past this is
 * refused with RAFT_ERR_CAPACITY when a transfer would start, rather
 * than being streamed half-way: the bound exists so the chunk walk can
 * work over a fixed array, and a server that quietly sent part of a
 * snapshot would be worse than one that says it cannot. */
#define RN_MAX_SNAP_FILES 256

/*
 * Adopt a member set: a binjson ARRAY of records ({ id, voting? }), the
 * same shape raft_core.h's members_adopt takes. This is the ONE funnel
 * -- bootstrap, a CONFIG entry applying, a snapshot install -- which is
 * what keeps the voter list and the peer cursors from drifting apart.
 *
 * A peer that disappears loses its cursors; one that arrives starts at
 * next = lastIndex + 1, which is what a fresh leader would give it.
 *
 * ALL OR NOTHING. Every input is validated before anything is adopted,
 * so a refusal leaves the previous set intact and the caller holding an
 * error it has to do something about. RAFT_ERR_CAPACITY if the set
 * exceeds rn_max_peers(); RAFT_ERR_MEMBER if it is malformed, or if its
 * voter list names someone who is not a member.
 *
 * A partial adoption would be worse than either: a host derives its own
 * member list from the same raft_members_adopt, and the two agreeing is
 * the property that removes the second source of truth. They cannot
 * agree about a set that only one of them trimmed.
 */
int rn_set_members(raft_node *n, const uint8_t *members, uint32_t len);

/* The largest peer count (members excluding self) this build can hold.
 * rn_change_membership refuses a larger set up front, where a caller is
 * still standing, rather than at apply, where every replica can only
 * halt. */
uint32_t rn_max_peers(void);

/*
 * The adopted set as raft_members_adopt normalized it, as one binjson
 * object: { members: [records], voters: [ids], peers: [ids] }.
 *
 * The node keeps it so that nothing else has to derive it. A host that
 * ran the same normalization would be a second place the cluster's shape
 * is written down, and two places is one too many -- this is where
 * addresses live too, which is what lets a redirect tell a joiner where
 * the leader actually is.
 */
const uint8_t *rn_adopted(const raft_node *n, uint32_t *len);

/*
 * Propose a new member set -- full replacement, records or bare ids.
 * Merged with what is known first, so an id-only proposal cannot erase
 * an address; refused with RAFT_ERR_CAPACITY if the result would not
 * fit, RAFT_ERR_BUSY if a change is already in flight, BJ_ERR_STATE if
 * this node does not lead. On success the CONFIG entry's index comes
 * back through `out_index`, and the host settles its promise when that
 * index applies, exactly as it does for rn_propose.
 *
 * One at a time is a safety rule, not a policy: a single-server change
 * is safe because it commits under the OLD quorum, which requires that
 * the next one cannot start until this one has.
 */
int rn_change_membership(raft_node *n, const uint8_t *members, uint32_t len,
                         uint64_t *out_index);

/* Is a membership change in flight? */
int rn_config_in_flight(const raft_node *n);

/*
 * Timing. `min_election` and `max_election` bound the randomised
 * election timeout; `heartbeat` is the leader's idle interval. The
 * random draw is passed IN to every call that can arm a timer, for the
 * replayability reason at the top of this file.
 *
 * The defaults are what rn_new installs, and are named here because a
 * host that offers to override one of them has to be able to leave the
 * other two alone without writing the numbers down a second time.
 */
#define RN_DEFAULT_MIN_ELECTION 150
#define RN_DEFAULT_MAX_ELECTION 300
#define RN_DEFAULT_HEARTBEAT     50

void rn_set_timing(raft_node *n, int64_t min_election, int64_t max_election,
                   int64_t heartbeat);
void rn_set_limits(raft_node *n, uint32_t max_batch_bytes);

/* Arm the timers at `now`, with `random01` in [0,1) for the first
 * election deadline. */
void rn_start(raft_node *n, int64_t now, double random01);
void rn_stop(raft_node *n);

/* ---- the clock ---------------------------------------------------------- */

/*
 * Advance to `now`. `random01` seeds any election timeout this tick
 * arms. Queues whatever the timers make due: a pre-vote round when the
 * election deadline passes, heartbeats when the leader's interval does.
 *
 * Idempotent in the sense that matters: calling it twice with the same
 * `now` queues nothing the second time.
 */
int rn_tick(raft_node *n, int64_t now, double random01);

/* Park the timers (a quiesced group) and unpark them. */
void rn_quiesce(raft_node *n);
void rn_wake(raft_node *n, int64_t now, double random01);

/* ---- messages ----------------------------------------------------------- */

/*
 * An incoming message. The reply is queued as an outbox entry addressed
 * to whoever sent it, carrying `corr` -- the correlation id the SENDER
 * used, which a host reads off its own framing and echoes back, so a
 * host that cannot distinguish a request from a reply on the wire does
 * not have to: it just sends what the outbox holds.
 *
 * The sender is taken from the message (rmsg_sender), never from the
 * caller. Every message in the grammar names who sent it, so a host has
 * no business restating it -- and a host that had to state it could
 * state it wrongly, which is how the outbox's "every entry is addressed"
 * invariant gets broken from the outside.
 *
 * Answered here: RequestVote, AppendEntries, TimeoutNow, join and leave
 * -- and InstallSnapshot once this node has been given a namespace and a
 * store to stage one into. Anything else is RAFT_ERR_MESSAGE.
 *
 * A JOIN OR A LEAVE MAY BE ANSWERED LATER THAN IT IS HANDLED. The answer
 * is a fact about a CONFIG entry that does not exist yet, so the node
 * parks the requester and queues nothing; the reply comes out of a later
 * call -- the one where that entry applies, or where this node stops
 * leading. A host that assumes rn_handle always leaves a reply in the
 * outbox will lose it, which means the conversation it arrived on has to
 * outlive the call (server/replica.c keeps a table; src/raft.js holds a
 * promise).
 */
int rn_handle(raft_node *n, uint64_t corr,
              const uint8_t *msg, uint32_t len, double random01);

/*
 * A reply to something this node sent. `corr` is the id that went out
 * with it; a stale or unknown id is RAFT_ERR_PEER, which is not an error
 * the host needs to act on -- it means the round it belonged to is over.
 */
int rn_on_reply(raft_node *n, uint64_t corr, const uint8_t *reply, uint32_t len,
                double random01);

/* The request with this correlation id will never be answered. */
int rn_on_fail(raft_node *n, uint64_t corr);

/* ---- the outbox --------------------------------------------------------- */

/*
 * Messages waiting to go out. Valid until the next call that mutates the
 * node -- drain immediately, which every host does anyway because there
 * is nothing else to do with them.
 *
 * Every entry is addressed, replies included, because the sender comes
 * out of the message rather than from whoever called rn_handle -- with
 * one exception, and it is not a lapse: an answer to a join or a leave
 * carries peer 0, because those come from OUTSIDE the cluster and their
 * sender has no id here to be addressed by. They are answered on the
 * correlation id alone, which is how a host answers them anyway.
 *
 * Replies carry the correlation id they are answering; requests carry a
 * fresh one, and ids are issued from a 64-bit counter that never comes
 * back around to one still outstanding.
 */
uint32_t       rn_out_count(const raft_node *n);
uint64_t       rn_out_peer(const raft_node *n, uint32_t i);
uint64_t       rn_out_corr(const raft_node *n, uint32_t i);
int            rn_out_is_reply(const raft_node *n, uint32_t i);
const uint8_t *rn_out_bytes(const raft_node *n, uint32_t i, uint32_t *len);
void           rn_out_clear(raft_node *n);

/* ---- what the host still owns ------------------------------------------- */

/*
 * Effects the host has to act on, drained like the outbox. C cannot
 * apply an entry, read a snapshot file or settle a promise, so it says
 * what happened and the host does it.
 */
typedef enum {
    RN_EFFECT_ROLE          = 0, /* role changed; arg = new role         */
    RN_EFFECT_COMMIT        = 1, /* commitIndex advanced; arg = index    */
    RN_EFFECT_NEEDS_SNAPSHOT= 2, /* peer is below our base; arg = peer   */
    RN_EFFECT_PROMOTE       = 3, /* learner is caught up; arg = peer     */
    RN_EFFECT_REACHABLE     = 4, /* peer reachability changed; arg = peer*/
    RN_EFFECT_TRUNCATED     = 5, /* our log was cut back; arg = from     */
    RN_EFFECT_ELECTION      = 6, /* standing for `arg`; flag = pre-vote  */
    /*
     * A snapshot install landed: its files are staged, verified against
     * the leader's manifest and committed, and `arg` is the boundary
     * index it covers.
     *
     * The host owes the ADOPTION -- closing the database, letting C swap
     * the generation onto the live files, reopening. That half stays the
     * host's permanently, because reopening rebuilds collection handles
     * and, in a browser, needs asynchronous opens; C does its work
     * between the close and the reopen, which is dc_compact_execute's
     * bargain exactly.
     */
    RN_EFFECT_INSTALLED     = 7, /* an install committed; arg = boundary */
    /*
     * A proposal this node owes an answer for has finished: arg is its
     * index, flag is 1 if the entry there is STILL THE ONE THAT WAS
     * PROPOSED and 0 if a new leader overwrote it.
     *
     * The flag is the whole point and the reason this is not the host's
     * arithmetic. An index being applied does not mean YOUR entry was
     * applied there: a conflicting entry at the same index can replace
     * an uncommitted one before it commits, and the proposer has to be
     * told its write did not happen. Reported leniently, that is a lost
     * write reported as a success -- so the rule (elog_term_at(index) ==
     * the term it was proposed at) lives here, once, rather than in
     * every host.
     */
    RN_EFFECT_SETTLED       = 8, /* a proposal finished; arg = index     */
    /*
     * A leadership transfer (rn_transfer) ended: arg is the target it
     * named. flag 1 means leadership LEFT this node -- however that
     * happened; the target's election is the expected way, but any
     * step-down while a transfer is armed is the transfer arriving at
     * its destination state. flag 0 means the deadline passed with this
     * node still leading -- the target is down, unreachable, or
     * refusing -- and the node has disarmed: proposals a host was
     * fencing may flow again, and a retry is safe.
     */
    RN_EFFECT_TRANSFER      = 9  /* a transfer ended; arg = target      */
} rn_effect_kind;

uint32_t rn_effect_count(const raft_node *n);
int      rn_effect_kind_at(const raft_node *n, uint32_t i);
uint64_t rn_effect_arg(const raft_node *n, uint32_t i);
int      rn_effect_flag(const raft_node *n, uint32_t i);
void     rn_effects_clear(raft_node *n);

/*
 * Did this node ever have an effect to report and no room to hold it?
 *
 * It cannot happen to a host that drains after each call: the queue is
 * sized so that every peer's actionable effects fit at once, and those
 * kinds coalesce per peer rather than accumulating (see raft_node.c).
 * A host that batches many calls between drains can still overrun the
 * narrative kinds, and this is how it finds out.
 *
 * Sticky, and never cleared by rn_effects_clear. There is no way to
 * recover what was not said, so a host that reads 1 here should stop
 * rather than keep acting on a picture it knows is incomplete.
 */
int      rn_effects_lost(const raft_node *n);

/* ---- accessors ---------------------------------------------------------- */

int      rn_role(const raft_node *n);
uint64_t rn_leader_id(const raft_node *n);
uint64_t rn_commit_index(const raft_node *n);
uint64_t rn_match(const raft_node *n, uint64_t peer);
uint64_t rn_next(const raft_node *n, uint64_t peer);
/* The correlation id outstanding at `peer`, or 0 -- the leader's own
 * "is this one busy" bit, exposed for the host's status snapshot. */
uint64_t rn_inflight(const raft_node *n, uint64_t peer);
uint32_t rn_quorum(const raft_node *n);
int      rn_is_quiesced(const raft_node *n);
/* Has a quorum of voters answered within `within_ms` (check-quorum)? A
 * peer that has never answered this term counts for nothing, however
 * recently the term began. */
int      rn_has_quorum_contact(const raft_node *n, int64_t within_ms);

/*
 * Force a replication pass at `peer` -- what the host calls after
 * appending an entry, or after an install completes. Queues at most one
 * message; a peer with a request already outstanding is skipped, which
 * is what keeps a busy leader from flooding a slow follower.
 */
int rn_replicate(raft_node *n, uint64_t peer);

/* Tell the node an install finished at `boundary`, so the peer's
 * cursors move to it (raft_drive.h's raft_repl_installed) and whatever
 * that newly puts within reach of a quorum commits. */
int rn_installed(raft_node *n, uint64_t peer, uint64_t boundary);

/* ---- what the host still owns ------------------------------------------- */

/*
 * Append one entry at the current term, make it durable, and replicate
 * it -- the leader's half of a proposal. The host owns whatever it
 * ANSWERS with (a promise, a socket write); this owns everything else,
 * the completion included.
 *
 * The commit check runs here too, because a single-voter group reaches a
 * quorum without sending anything: no reply will ever arrive to trigger
 * it, and such a group would otherwise append forever and commit
 * nothing. BJ_ERR_STATE if this node is not the leader -- the host
 * refuses first with a routing error the caller can act on.
 *
 * The completion is REGISTERED HERE, not by the caller: the index and
 * the term are both known at this line and nowhere else at once, and a
 * caller that has to remember to register is a caller that can forget.
 * RAFT_ERR_CAPACITY if this node already owes more answers than it can
 * hold (RN_MAX_AWAIT) -- and the entry is not appended, because a
 * proposal nobody will ever be told the fate of is worse than a refused
 * one.
 */
int rn_propose(raft_node *n, int type, const uint8_t *payload, uint32_t len,
               uint64_t *out_index);

/*
 * The same, for many entries at once, with ONE fsync covering all of them.
 *
 * A request that plans thousands of commands -- updateMany names one per
 * matched document -- paid one sync per entry through rn_propose, on the
 * caller's thread, with nothing else served in between: 20,000 documents
 * measured as 20,000 syncs and four and a half seconds of a serving loop
 * that answered 2% of its idle read rate. Appending the batch and syncing
 * once keeps the ordering that matters (every append, then the sync, then
 * the awaits that make an entry count, then one commit advance) and turns
 * the per-entry cost into a per-batch one.
 *
 * `out_indices` receives one index per appended entry, in order. Capacity
 * is refused WHOLE and up front (RAFT_ERR_CAPACITY): a batch that does not
 * fit appends nothing. An append that fails part-way is not undone -- those
 * entries are in the log and will commit -- so *appended says how many made
 * it, and the caller owns exactly those.
 */
int rn_propose_batch(raft_node *n, int type,
                     const uint8_t *const *payloads, const uint32_t *lens,
                     uint32_t count, uint64_t *out_indices, uint32_t *appended);

/* ---- read barriers (section 6.4, ReadIndex) ------------------------------
 *
 * A LEADER'S LOCAL STATE IS NOT AUTHORITATIVE BY ITSELF. Raft does not
 * depose a leader that has lost contact with its quorum -- it simply
 * stops being able to commit -- so a partitioned leader will answer a
 * read from state a newer leader has already moved past. Serving that is
 * presenting staleness as authority, which is the one thing a read must
 * never do.
 *
 * A barrier is the proof. Take one, and a quorum of voters must be shown
 * to still follow this leader SINCE the moment it was taken; the caller
 * then serves state at or above `read_index` and the answer is
 * linearizable. Concurrent barriers share the same round -- sixty-four
 * reads at once cost one heartbeat round, not sixty-four -- because what
 * confirms them is per-peer evidence, not a per-barrier message.
 *
 * A FRESH ROUND, NOT A LEASE. rn_has_quorum_contact would answer this
 * more cheaply from timestamps, and that is a lease: it assumes bounded
 * clock skew, which nothing else in this codebase does. This asks.
 *
 *     rn_read_barrier(n, &token, &read_index)
 *     ... the host serves the read, and holds the answer ...
 *     rn_read_state(n, token)   1 confirmed, 0 not yet, -1 lost
 *     rn_read_release(n, token)
 *
 * EVERY BARRIER TERMINATES. Confirmed, lost at step-down or stop, or
 * expired after an election timeout without a quorum answering -- there
 * is no fourth outcome, and no client is left holding a read forever.
 * Polled rather than reported through the effect queue: one entry per
 * client connection would swamp a queue sized for peers and proposals.
 *
 * BJ_ERR_STATE if this node does not lead -- the host refuses with a
 * routing error the caller can act on. RAFT_ERR_CAPACITY when more reads
 * are outstanding than this build holds; also a refusal, for the reason
 * every other bounded table here refuses.
 *
 * A single-voter group is confirmed the instant it is taken: there is
 * nobody to hear from, so the read is answered without a round trip and
 * without waiting.
 */
int      rn_read_barrier(raft_node *n, uint64_t *token, uint64_t *read_index);
int      rn_read_state(const raft_node *n, uint64_t token);
void     rn_read_release(raft_node *n, uint64_t token);
uint32_t rn_reads_outstanding(const raft_node *n);

/* ---- completions --------------------------------------------------------
 *
 * What a client is actually waiting for is not "committed" but "applied,
 * and still mine". The node cannot see the second half happen -- the
 * apply pump is the host's, because applying needs a state machine -- so
 * the host reports the floor and the node reports what that settles.
 *
 *     rn_applied(n, index)         after each entry the pump applies
 *     ... RN_EFFECT_SETTLED per proposal it finished ...
 *
 * EVERY REGISTERED WAIT TERMINATES. Step-down and stop settle everything
 * outstanding as overwritten, because a node that is no longer the
 * leader cannot promise anything about entries it has not applied. A
 * client left holding a request forever is the failure this exists to
 * prevent, not to relocate.
 */
void rn_applied(raft_node *n, uint64_t index);

/*
 * Owe an answer for `index`, proposed at `term`. rn_propose does this
 * itself; this is for a host re-registering across a restart, or for one
 * that appended some other way. RAFT_ERR_CAPACITY when full.
 */
int rn_await(raft_node *n, uint64_t index, uint64_t term);

/* How many answers this node currently owes -- for a host bounding its
 * own intake, and for tests. */
uint32_t rn_awaiting(const raft_node *n);

/* And the most it can owe at once (RN_MAX_AWAIT). With rn_awaiting this
 * is how a host refuses a batch WHOLE before proposing any of it: a
 * batch that hits the cap mid-way leaves its appended prefix committing
 * anyway -- rn_propose refuses the entry, not the entries before it --
 * which is an all-or-nothing violation no error code can undo. */
uint32_t rn_max_await(void);

/*
 * Seed the commit index at startup from the log's persisted (advisory)
 * marker and the state machine's applied floor. Never lowers it. The
 * host knows both numbers and the node knows neither.
 */
void rn_seed_commit(raft_node *n, uint64_t index);

/*
 * Stand for election NOW: a real round, skipping pre-vote, because the
 * transferring leader that sent TimeoutNow (section 3.10) has certified
 * this node is caught up and leader stickiness would otherwise make
 * every peer refuse while that leader still lives. A leader, a stopped
 * node and a non-voter all decline.
 */
int rn_campaign(raft_node *n, double random01);

/*
 * Begin a leadership transfer (section 3.10) to `target`: the other
 * side of rn_campaign, and the piece that lets a host drain a member
 * without copying a byte -- the target already holds the log.
 *
 * Leader only (RAFT_ERR_STATE otherwise). The target must be another
 * member that VOTES -- a learner cannot win the election this triggers
 * -- and an unknown or non-voting target is RAFT_ERR_PEER. One transfer
 * may be armed at a time (RAFT_ERR_BUSY), the same serialization rule
 * membership changes follow.
 *
 * Armed, the node brings the target fully up to date and, the moment
 * its match index reaches the last log index, sends it a TimeoutNow --
 * re-sent if the target answers no or the send fails, on the strength
 * of the acks heartbeats keep producing. The transfer ends in one of
 * two ways, each reported as RN_EFFECT_TRANSFER: leadership left this
 * node (flag 1 -- the target's RequestVote at term+1 is the expected
 * cause, but any step-down while armed counts), or 2x the max election
 * timeout passed first (flag 0) and the node disarmed itself.
 *
 * What the node does NOT do is fence proposals: whether new writes are
 * refused while leadership is moving -- and toward whom their refusal
 * points -- is client-facing policy, and it belongs to the host
 * (server/replica.c refuses them with the TARGET as the leader hint).
 */
int rn_transfer(raft_node *n, uint64_t target);

/* The transfer in flight, or 0. */
uint64_t rn_transfer_target(const raft_node *n);

/*
 * A message the HOST answered (InstallSnapshot) carrying a leader's
 * term: adopt it the way an AppendEntries would -- step down if it is
 * newer or we are not already following, record the leader, refresh the
 * election timer. Returns 0 for a stale term, which is the caller's cue
 * to refuse the message.
 */
int rn_observe_leader(raft_node *n, uint64_t term, uint64_t leader_id, double random01);

/*
 * A reply the HOST awaited (a snapshot chunk) carrying a higher term:
 * step down. Unlike rn_observe_leader this records no leader and
 * refreshes no contact -- the peer that deposed us is not thereby our
 * leader. Returns 1 if it deposed us.
 */
int rn_step_down(raft_node *n, uint64_t term, double random01);

#ifdef __cplusplus
}
#endif

#endif /* RAFT_NODE_H */
