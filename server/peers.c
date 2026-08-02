/* server/peers.c — see peers.h. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "peers.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "binjson.h"
#include "bjcursor.h"
#include "dbuf.h"

#if defined(NISABA_SOCKETS)

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

/* The 4-byte prefix, and the sanity cap src/raft-transport-tcp.js sets on
 * what may follow it: a frame is at most a snapshot chunk plus overhead.
 * Copied rather than chosen, because the two ends have to agree. */
#define PEER_HEADER    4
#define PEER_FRAME_MAX (64u * 1024u * 1024u)

/*
 * How long a request may go unanswered by a peer that ACCEPTED it --
 * src/raft-transport-tcp.js's requestTimeoutMs, for the same reason the
 * frame cap is. It is long because it is not the failure detector: a
 * peer that is down refuses the connection or resets it, and that fails
 * everything outstanding at once. This is only for a peer that took the
 * message and went quiet, which no shorter timer would diagnose better.
 */
#define PEER_TIMEOUT_MS 5000

/* Don't redial faster than this. A leader heartbeats every 50ms, and a
 * member that is down would otherwise get a fresh connect() each time --
 * short enough that a member coming back is noticed within one election
 * timeout, long enough not to be a loop. */
#define PEER_REDIAL_MS 100

/*
 * Requests outstanding to one peer. The node keeps at most one
 * replication request per peer in flight (rn_replicate skips a peer that
 * owes an answer), so this is headroom for a vote round overlapping a
 * heartbeat rather than a queue depth.
 */
#define PEER_MAX_PENDING 8

/* A connection's buffers are reused, but one that once carried a large
 * batch should not hold that memory for the rest of its life. */
#define PEER_IDLE_BUFFER (64u * 1024u)

/* ---- a socket with a frame reader and a write queue --------------------- */

typedef struct {
    int      fd;          /* -1 = not connected */
    int      dialling;    /* a non-blocking connect has not finished */
    uint8_t *in;
    size_t   in_len, in_cap;
    dbuf     out;
    size_t   out_off;
} chan;

typedef struct {
    uint64_t corr;
    uint64_t sent_at;
} inflight;

typedef struct {
    uint64_t id;
    char     host[PEERS_HOST_MAX];
    int      port;
    chan     c;
    uint64_t retry_at;
    inflight pend[PEER_MAX_PENDING];
    uint32_t n_pend;
} peer_slot;

typedef struct {
    uint64_t conv;        /* 0 = the slot is free */
    chan     c;
} in_slot;

typedef struct {
    int        fd;
    int        want_write;
    int        listener;
    peer_slot *pe;        /* exactly one of these three is set */
    in_slot   *ib;
} watch_slot;

struct peers {
    int       srv;
    char      self_host[PEERS_HOST_MAX];
    int       self_port;

    peer_slot p[PEERS_MAX];
    uint32_t  n;
    in_slot   in[PEERS_IN_MAX];
    uint32_t  n_in;
    uint64_t  next_conv;

    /*
     * Correlation ids whose requests died with their connection. Sized
     * for every pending request across every peer, plus room for the
     * ones a single outbox flush can strand -- and the caller drains
     * this between flushes, so the two cannot accumulate. Overflowing it
     * would mean losing a failure, which wedges a peer cursor forever,
     * so it is an error rather than a discard.
     */
    uint64_t  failq[PEERS_MAX * (PEER_MAX_PENDING + 8)];
    uint32_t  n_fail;

    watch_slot w[PEERS_MAX_FDS];
    uint32_t   n_w;

    /* The body handed out by the last peers_next. Copied out of the
     * connection rather than pointed into it: the caller is entitled to
     * hold it across an answer, and an answer can grow -- or a later
     * event can consume -- the buffer it arrived in. */
    dbuf      frame;

    uint32_t  scan;       /* where the last drain left off, for fairness */
    uint64_t  now;
};

/* ---- chan --------------------------------------------------------------- */

static void chan_init(chan *c) { memset(c, 0, sizeof *c); c->fd = -1; }

static void chan_close(chan *c) {
    if (c->fd >= 0) close(c->fd);
    free(c->in);
    dbuf_free(&c->out);
    chan_init(c);
}

static int chan_reserve(chan *c, size_t need) {
    if (need <= c->in_cap) return 0;
    size_t nc = c->in_cap ? c->in_cap : 4096;
    while (nc < need) {
        if (nc > PEER_FRAME_MAX) return -1;
        nc *= 2;
    }
    uint8_t *nb = (uint8_t *)realloc(c->in, nc);
    if (!nb) return -1;
    c->in = nb;
    c->in_cap = nc;
    return 0;
}

static int set_nonblocking(int fd) {
    int fl = fcntl(fd, F_GETFL, 0);
    if (fl < 0) return -1;
    return fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

/* 0 = still here, -1 = gone. Never blocks; what does not go now goes on
 * the next POLLOUT. */
static int chan_flush(chan *c) {
    if (c->fd < 0 || c->dialling) return 0;
    while (c->out_off < c->out.len) {
        ssize_t w = write(c->fd, c->out.data + c->out_off, c->out.len - c->out_off);
        if (w < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
            return -1;
        }
        c->out_off += (size_t)w;
    }
    c->out.len = 0;
    c->out_off = 0;
    if (c->out.cap > PEER_IDLE_BUFFER) dbuf_free(&c->out);
    return 0;
}

static int chan_read(chan *c) {
    uint8_t buf[16384];
    for (;;) {
        ssize_t r = read(c->fd, buf, sizeof buf);
        if (r == 0) return -1;                       /* clean EOF */
        if (r < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
            return -1;
        }
        if (chan_reserve(c, c->in_len + (size_t)r)) return -1;
        memcpy(c->in + c->in_len, buf, (size_t)r);
        c->in_len += (size_t)r;
        if ((size_t)r < sizeof buf) return 0;
    }
}

/* A whole frame at the front? 0 = yes, and body/blen span its payload;
 * 1 = not yet, -1 = unframeable, which ends the connection: a reader
 * that has lost the boundary cannot resynchronise. */
static int chan_frame(const chan *c, const uint8_t **body, uint32_t *blen) {
    if (c->in_len < PEER_HEADER) return 1;
    uint32_t n = rdu32(c->in);
    if (n > PEER_FRAME_MAX) return -1;
    if (c->in_len < (size_t)PEER_HEADER + n) return 1;
    *body = c->in + PEER_HEADER;
    *blen = n;
    return 0;
}

static void chan_consume(chan *c, uint32_t blen) {
    size_t total = (size_t)PEER_HEADER + blen;
    c->in_len -= total;
    memmove(c->in, c->in + total, c->in_len);
    if (c->in_len == 0 && c->in_cap > PEER_IDLE_BUFFER) {
        free(c->in);
        c->in = NULL;
        c->in_cap = 0;
    }
}

static int chan_put(chan *c, const uint8_t *body, size_t blen) {
    uint8_t hdr[PEER_HEADER];
    hdr[0] = (uint8_t)(blen & 0xff);
    hdr[1] = (uint8_t)((blen >> 8) & 0xff);
    hdr[2] = (uint8_t)((blen >> 16) & 0xff);
    hdr[3] = (uint8_t)((blen >> 24) & 0xff);
    int e = dbuf_put(&c->out, hdr, sizeof hdr);
    if (!e) e = dbuf_put(&c->out, body, blen);
    return e;
}

/* ---- the frame grammar -------------------------------------------------- */

/*
 * A message rides in `env` (and in `value`) as OPAQUE BYTES -- binjson's
 * BINARY, not the message spliced in as a nested object.
 *
 * That is not a taste: it is what the other end does. src/raft.js hands
 * its transport an already-encoded message (`transport.call(peer,
 * bytes)`), so binjson tags it BINARY, and the receiving host passes it
 * straight to handleMessage(bytes) without ever decoding it. A nested
 * object would arrive there as a decoded object where bytes are
 * expected. It cost nothing while both ends were C -- they agreed with
 * each other -- and made the two hosts unable to share a cluster, which
 * is the entire reason this wire is a copy of that file's.
 */
static int build_req(uint64_t corr, const uint8_t *env, uint32_t len, dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"t", 1);
    if (!e) e = bj_put_string(b, (const uint8_t *)"req", 3);
    if (!e) e = bj_put_key(b, (const uint8_t *)"id", 2);
    if (!e) e = bj_put_int(b, (int64_t)corr);
    if (!e) e = bj_put_key(b, (const uint8_t *)"env", 3);
    if (!e) e = bj_put_binary(b, env, len);
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t sz = 0;
        const uint8_t *d = bj_builder_data(b, &sz);
        e = d ? dbuf_put(out, d, sz) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    return e;
}

static int build_res(uint64_t corr, const uint8_t *value, uint32_t len,
                     const char *error, dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"t", 1);
    if (!e) e = bj_put_string(b, (const uint8_t *)"res", 3);
    if (!e) e = bj_put_key(b, (const uint8_t *)"id", 2);
    if (!e) e = bj_put_int(b, (int64_t)corr);
    if (!e) e = bj_put_key(b, (const uint8_t *)"ok", 2);
    if (!e) e = bj_put_bool(b, error ? 0 : 1);
    if (error) {
        if (!e) e = bj_put_key(b, (const uint8_t *)"error", 5);
        if (!e) e = bj_put_string(b, (const uint8_t *)error, (uint32_t)strlen(error));
    } else {
        if (!e) e = bj_put_key(b, (const uint8_t *)"value", 5);
        if (!e) e = bj_put_binary(b, value, len);
    }
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t sz = 0;
        const uint8_t *d = bj_builder_data(b, &sz);
        e = d ? dbuf_put(out, d, sz) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    return e;
}

/* `t`, as one of the two spellings; -1 for anything else. */
static int frame_kind(const uint8_t *body, size_t blen) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(body, blen, (const uint8_t *)"t", 1, &v, &vlen, &found) != BJ_OK ||
        !found) return -1;
    cur c = { v, vlen, 0 };
    const uint8_t *s; uint32_t slen;
    if (take_string(&c, &s, &slen) != BJ_OK) return -1;
    if (name_eq(s, slen, "req")) return 0;
    if (name_eq(s, slen, "res")) return 1;
    return -1;
}

static int frame_u64(const uint8_t *body, size_t blen, const char *key, uint64_t *out) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(body, blen, (const uint8_t *)key, (uint32_t)strlen(key),
                      &v, &vlen, &found) != BJ_OK || !found) return -1;
    cur c = { v, vlen, 0 };
    return read_u64(&c, out) == BJ_OK ? 0 : -1;
}

/* The PAYLOAD of a BINARY field: past its type byte and its u32 length,
 * which is take_string's shape with BINARY's tag. What comes back is the
 * message exactly as the sender's node emitted it, which is what
 * rn_handle and rn_on_reply take. */
static int frame_binary(const uint8_t *body, size_t blen, const char *key,
                        const uint8_t **v, uint32_t *len) {
    const uint8_t *span; size_t slen; int found = 0;
    if (obj_get_field(body, blen, (const uint8_t *)key, (uint32_t)strlen(key),
                      &span, &slen, &found) != BJ_OK || !found) return -1;
    if (slen < 5 || span[0] != BJ_TYPE_BINARY) return -1;
    uint32_t n = rdu32(span + 1);
    if ((size_t)n + 5 != slen) return -1;
    *v = span + 5;
    *len = n;
    return 0;
}

/* ---- pending requests --------------------------------------------------- */

static int fail_push(peers *p, uint64_t corr) {
    if (p->n_fail >= sizeof p->failq / sizeof p->failq[0]) return BJ_ERR_STATE;
    p->failq[p->n_fail++] = corr;
    return BJ_OK;
}

/* Everything outstanding on this peer failed with its connection. */
static int pend_fail_all(peers *p, peer_slot *pe) {
    int e = BJ_OK;
    for (uint32_t i = 0; i < pe->n_pend; i++) {
        int r = fail_push(p, pe->pend[i].corr);
        if (r) e = r;
    }
    pe->n_pend = 0;
    return e;
}

static void pend_drop(peer_slot *pe, uint32_t i) {
    pe->pend[i] = pe->pend[pe->n_pend - 1];
    pe->n_pend--;
}

static int peer_drop(peers *p, peer_slot *pe, uint64_t retry_at) {
    chan_close(&pe->c);
    pe->retry_at = retry_at;
    return pend_fail_all(p, pe);
}

/* ---- opening ------------------------------------------------------------ */

int peers_new(peers **out) {
    if (!out) return BJ_ERR_STATE;
    peers *p = (peers *)calloc(1, sizeof *p);
    if (!p) return BJ_ERR_OOM;
    p->srv = -1;
    for (uint32_t i = 0; i < PEERS_MAX; i++) chan_init(&p->p[i].c);
    for (uint32_t i = 0; i < PEERS_IN_MAX; i++) chan_init(&p->in[i].c);
    *out = p;
    return BJ_OK;
}

void peers_free(peers *p) {
    if (!p) return;
    if (p->srv >= 0) close(p->srv);
    for (uint32_t i = 0; i < PEERS_MAX; i++) chan_close(&p->p[i].c);
    for (uint32_t i = 0; i < PEERS_IN_MAX; i++) chan_close(&p->in[i].c);
    dbuf_free(&p->frame);
    free(p);
}

int peers_add(peers *p, uint64_t id, const char *host, int port) {
    if (!p || !id || !host || port <= 0 || port > 65535) return BJ_ERR_STATE;
    if (p->n >= PEERS_MAX) return BJ_ERR_STATE;
    if (strlen(host) >= PEERS_HOST_MAX) return BJ_ERR_STATE;
    for (uint32_t i = 0; i < p->n; i++) if (p->p[i].id == id) return BJ_ERR_STATE;
    peer_slot *pe = &p->p[p->n++];
    pe->id = id;
    snprintf(pe->host, sizeof pe->host, "%s", host);
    pe->port = port;
    return BJ_OK;
}

static peer_slot *peer_by_id(peers *p, uint64_t id) {
    for (uint32_t i = 0; i < p->n; i++) if (p->p[i].id == id) return &p->p[i];
    return NULL;
}

/* The watch list describes the table as it was; anything that moves a
 * slot invalidates it. It cannot bite in the server's loop -- peers_watch
 * runs at the top of a pass and every peers_ready before the membership
 * is ever looked at -- and dropping one pass of readiness would be
 * harmless anyway, because poll is level-triggered and says so again. */
static void watch_invalidate(peers *p) { p->n_w = 0; }

int peers_set(peers *p, uint64_t id, const char *host, int port) {
    if (!p || !id || !host || port <= 0 || port > 65535) return BJ_ERR_STATE;
    if (strlen(host) >= PEERS_HOST_MAX) return BJ_ERR_STATE;
    peer_slot *pe = peer_by_id(p, id);
    if (!pe) return peers_add(p, id, host, port);
    if (pe->port == port && strcmp(pe->host, host) == 0) return BJ_OK;
    /* It moved. The socket goes to where it used to be, so it is no more
     * use than a socket to a member that is down. */
    int e = peer_drop(p, pe, p->now);
    snprintf(pe->host, sizeof pe->host, "%s", host);
    pe->port = port;
    watch_invalidate(p);
    return e;
}

int peers_remove(peers *p, uint64_t id) {
    if (!p || !id) return BJ_ERR_STATE;
    peer_slot *pe = peer_by_id(p, id);
    if (!pe) return BJ_OK;
    int e = peer_drop(p, pe, 0);
    /* The last one moves into the hole and its old slot owns nothing --
     * the same shape server/main.c's connection table uses, arrived at
     * for the same reason. */
    *pe = p->p[p->n - 1];
    memset(&p->p[p->n - 1], 0, sizeof p->p[p->n - 1]);
    chan_init(&p->p[p->n - 1].c);
    p->n--;
    watch_invalidate(p);
    return e;
}

int peers_listen(peers *p, const char *host, int port) {
    if (!p || p->srv >= 0 || !host || port <= 0 || port > 65535) return BJ_ERR_STATE;
    if (strlen(host) >= PEERS_HOST_MAX) return BJ_ERR_STATE;

    struct in_addr a;
    if (inet_pton(AF_INET, host, &a) != 1) return BJ_ERR_STATE;

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) return BJ_ERR_STATE;
    int one = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr = a;
    if (bind(srv, (struct sockaddr *)&addr, sizeof addr) != 0 ||
        listen(srv, 16) != 0 ||
        set_nonblocking(srv) != 0) {
        close(srv);
        return BJ_ERR_STATE;
    }
    p->srv = srv;
    snprintf(p->self_host, sizeof p->self_host, "%s", host);
    p->self_port = port;
    return BJ_OK;
}

uint32_t    peers_count(const peers *p)              { return p ? p->n : 0; }
uint64_t    peers_id_at(const peers *p, uint32_t i)  { return i < p->n ? p->p[i].id : 0; }
const char *peers_host_at(const peers *p, uint32_t i){ return i < p->n ? p->p[i].host : NULL; }
int         peers_port_at(const peers *p, uint32_t i){ return i < p->n ? p->p[i].port : 0; }
const char *peers_self_host(const peers *p)          { return p ? p->self_host : NULL; }
int         peers_self_port(const peers *p)          { return p ? p->self_port : 0; }

/* ---- dialling ----------------------------------------------------------- */

/* 0 = connected or connecting, -1 = not now. Never blocks: a member that
 * is down is the ordinary case, and waiting for TCP to give up would
 * stop this node heartbeating the members that are up. */
static int peer_dial(peers *p, peer_slot *pe) {
    if (pe->c.fd >= 0) return 0;
    if (p->now < pe->retry_at) return -1;

    struct in_addr a;
    if (inet_pton(AF_INET, pe->host, &a) != 1) { pe->retry_at = p->now + PEER_REDIAL_MS; return -1; }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { pe->retry_at = p->now + PEER_REDIAL_MS; return -1; }
    if (set_nonblocking(fd) != 0) { close(fd); pe->retry_at = p->now + PEER_REDIAL_MS; return -1; }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)pe->port);
    addr.sin_addr = a;

    int r = connect(fd, (struct sockaddr *)&addr, sizeof addr);
    if (r != 0 && errno != EINPROGRESS && errno != EINTR) {
        close(fd);
        pe->retry_at = p->now + PEER_REDIAL_MS;
        return -1;
    }
    pe->c.fd = fd;
    pe->c.dialling = (r != 0);
    return 0;
}

/* ---- the poll seam ------------------------------------------------------ */

uint32_t peers_watch(peers *p) {
    if (!p) return 0;
    /* Slots emptied during the last readiness pass are reclaimed here,
     * not there: an index handed out this round must mean the same thing
     * for the whole of it. */
    for (uint32_t i = 0; i < p->n_in; ) {
        if (p->in[i].c.fd < 0) {
            chan_close(&p->in[i].c);
            p->in[i] = p->in[p->n_in - 1];
            p->in[p->n_in - 1].conv = 0;
            chan_init(&p->in[p->n_in - 1].c);
            p->n_in--;
        } else i++;
    }

    p->n_w = 0;
    if (p->srv >= 0) {
        watch_slot *w = &p->w[p->n_w++];
        memset(w, 0, sizeof *w);
        w->fd = p->srv;
        w->listener = 1;
    }
    for (uint32_t i = 0; i < p->n; i++) {
        if (p->p[i].c.fd < 0) continue;
        watch_slot *w = &p->w[p->n_w++];
        memset(w, 0, sizeof *w);
        w->fd = p->p[i].c.fd;
        w->pe = &p->p[i];
        w->want_write = p->p[i].c.dialling || p->p[i].c.out.len > p->p[i].c.out_off;
    }
    for (uint32_t i = 0; i < p->n_in; i++) {
        watch_slot *w = &p->w[p->n_w++];
        memset(w, 0, sizeof *w);
        w->fd = p->in[i].c.fd;
        w->ib = &p->in[i];
        w->want_write = p->in[i].c.out.len > p->in[i].c.out_off;
    }
    return p->n_w;
}

int peers_fd_at(const peers *p, uint32_t i, int *want_write) {
    if (!p || i >= p->n_w) return -1;
    if (want_write) *want_write = p->w[i].want_write;
    return p->w[i].fd;
}

/* A connection has arrived. It is anonymous by design: an inbound socket
 * carries requests only, and who sent them is in the messages. */
static void peers_accept(peers *p) {
    for (;;) {
        int fd = accept(p->srv, NULL, NULL);
        if (fd < 0) return;
        if (p->n_in >= PEERS_IN_MAX || set_nonblocking(fd) != 0) { close(fd); continue; }
        in_slot *s = &p->in[p->n_in++];
        chan_init(&s->c);
        s->c.fd = fd;
        s->conv = ++p->next_conv;
    }
}

/* The non-blocking connect finished -- successfully or not. */
static int peer_connected(peers *p, peer_slot *pe) {
    int err = 0;
    socklen_t len = sizeof err;
    if (getsockopt(pe->c.fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0 || err != 0)
        return peer_drop(p, pe, p->now + PEER_REDIAL_MS);
    pe->c.dialling = 0;
    return BJ_OK;
}

void peers_ready(peers *p, uint32_t i, int readable, int writable, int failed) {
    if (!p || i >= p->n_w) return;
    watch_slot *w = &p->w[i];
    if (w->fd < 0) return;

    if (w->listener) {
        if (readable) peers_accept(p);
        return;
    }

    chan *c = w->pe ? &w->pe->c : &w->ib->c;
    if (c->fd < 0) return;   /* emptied earlier in this same pass */

    if (failed) {
        if (w->pe) peer_drop(p, w->pe, p->now + PEER_REDIAL_MS);
        else chan_close(c);
        return;
    }
    /*
     * A dial that has finished, however the host chose to say so. Not
     * "POLLOUT means connected": a REFUSED connect is reported on macOS
     * as POLLIN|POLLHUP with no POLLOUT at all, so waiting for writable
     * waits forever -- the request is never failed, and the node, which
     * will not replace a request it still believes is in flight, never
     * speaks to that member again. (It did exactly that: a killed member
     * came back and the leader had stopped replicating to it.) Any
     * readiness at all means ask, and SO_ERROR is the only authority.
     */
    if (c->dialling) {
        if (!writable && !readable) return;
        if (peer_connected(p, w->pe) != BJ_OK || c->fd < 0) return;
    }
    if (writable && chan_flush(c) != 0) {
        if (w->pe) peer_drop(p, w->pe, p->now + PEER_REDIAL_MS);
        else chan_close(c);
        return;
    }
    if (readable && chan_read(c) != 0) {
        if (w->pe) peer_drop(p, w->pe, p->now + PEER_REDIAL_MS);
        else chan_close(c);
    }
}

int peers_pump(peers *p, uint64_t now) {
    if (!p) return BJ_ERR_STATE;
    p->now = now;
    int e = BJ_OK;
    for (uint32_t i = 0; i < p->n; i++) {
        peer_slot *pe = &p->p[i];
        /* Anything queued goes now rather than on the next POLLOUT: the
         * loop sleeps until a socket is ready or a timer is due, and a
         * heartbeat left sitting in a buffer is a heartbeat that did not
         * happen. */
        if (pe->c.fd >= 0 && chan_flush(&pe->c) != 0) {
            int r = peer_drop(p, pe, now + PEER_REDIAL_MS);
            if (r) e = r;
            continue;
        }
        for (uint32_t k = 0; k < pe->n_pend; ) {
            if (now - pe->pend[k].sent_at < PEER_TIMEOUT_MS) { k++; continue; }
            /* It took the message and went quiet. The connection is no
             * more trustworthy than the request was, so it goes too --
             * which is also what makes the next attempt a fresh dial. */
            int r = peer_drop(p, pe, now);
            if (r) e = r;
            break;
        }
    }
    for (uint32_t i = 0; i < p->n_in; i++)
        if (p->in[i].c.fd >= 0 && chan_flush(&p->in[i].c) != 0) chan_close(&p->in[i].c);
    return e;
}

/* ---- messages ----------------------------------------------------------- */

/* One frame off `c`, copied into p->frame. 0 = got one, 1 = none yet,
 * -1 = the connection has lost the frame boundary. */
static int take_frame(peers *p, chan *c) {
    const uint8_t *body = NULL;
    uint32_t blen = 0;
    int r = chan_frame(c, &body, &blen);
    if (r) return r;
    p->frame.len = 0;
    if (dbuf_put(&p->frame, body, blen) != BJ_OK) return -1;
    chan_consume(c, blen);
    return 0;
}

int peers_next(peers *p, peer_event *ev, int *have) {
    if (!p || !ev || !have) return BJ_ERR_STATE;
    *have = 0;

    /* Failures first: a request whose connection died has to reach the
     * node before the next one is made, or its peer cursor stays busy
     * over a message nobody will ever answer. */
    if (p->n_fail) {
        memset(ev, 0, sizeof *ev);
        ev->kind = PEER_EV_FAIL;
        ev->corr = p->failq[0];
        memmove(p->failq, p->failq + 1, (p->n_fail - 1) * sizeof p->failq[0]);
        p->n_fail--;
        *have = 1;
        return BJ_OK;
    }

    /* Round-robin over both tables, so a chatty peer cannot starve a
     * quiet one of its turn. */
    uint32_t total = p->n + p->n_in;
    for (uint32_t k = 0; total && k < total; k++) {
        uint32_t i = (p->scan + k) % total;
        peer_slot *pe = i < p->n ? &p->p[i] : NULL;
        in_slot   *ib = pe ? NULL : &p->in[i - p->n];
        chan *c = pe ? &pe->c : &ib->c;
        const uint8_t *body = NULL, *okv = NULL;
        size_t blen = 0, oklen = 0;
        uint64_t corr = 0;
        int kind = -1, found = 0, ok = 0;
        cur oc;

        if (c->fd < 0 || c->dialling) continue;

        int r = take_frame(p, c);
        if (r > 0) continue;
        if (r < 0) goto broken;
        p->scan = (i + 1) % total;

        body = p->frame.data;
        blen = p->frame.len;
        kind = frame_kind(body, blen);
        if (kind < 0 || frame_u64(body, blen, "id", &corr) != 0) goto broken;

        if (kind == 0) {
            /* A request. Only an inbound socket carries one: an outbound
             * socket is this node's own requests and their answers, and
             * a peer that sent one there is speaking a grammar we do not
             * know -- which is a peer we cannot safely answer. */
            if (!ib) goto broken;
            memset(ev, 0, sizeof *ev);
            ev->kind = PEER_EV_REQUEST;
            ev->from = ib->conv;
            ev->corr = corr;
            if (frame_binary(body, blen, "env", &ev->bytes, &ev->len) != 0) goto broken;
            *have = 1;
            return BJ_OK;
        }

        /* An answer, to a request only an outbound socket made. */
        if (!pe) goto broken;
        for (uint32_t j = 0; j < pe->n_pend; j++)
            if (pe->pend[j].corr == corr) { pend_drop(pe, j); break; }

        if (obj_get_field(body, blen, (const uint8_t *)"ok", 2, &okv, &oklen, &found) != BJ_OK ||
            !found) goto broken;
        oc.d = okv; oc.len = oklen; oc.pos = 0;
        if (read_bool(&oc, &ok) != BJ_OK) goto broken;

        memset(ev, 0, sizeof *ev);
        ev->corr = corr;
        if (ok && frame_binary(body, blen, "value", &ev->bytes, &ev->len) == 0) {
            ev->kind = PEER_EV_REPLY;
        } else {
            /* A refusal is an answer, and to the node it is the same
             * answer as silence: the request it made did not happen. */
            ev->kind = PEER_EV_FAIL;
        }
        *have = 1;
        return BJ_OK;

    broken:
        if (pe) peer_drop(p, pe, p->now + PEER_REDIAL_MS);
        else chan_close(c);
        continue;
    }
    return BJ_OK;
}

int peers_request(peers *p, uint64_t id, uint64_t corr,
                  const uint8_t *msg, uint32_t len) {
    if (!p) return BJ_ERR_STATE;
    peer_slot *pe = NULL;
    for (uint32_t i = 0; i < p->n; i++) if (p->p[i].id == id) { pe = &p->p[i]; break; }
    if (!pe) return 1;                                  /* not a member here */
    if (pe->n_pend >= PEER_MAX_PENDING) return 1;
    if (peer_dial(p, pe) != 0) return 1;

    dbuf body = {0};
    int e = build_req(corr, msg, len, &body);
    if (!e) e = chan_put(&pe->c, body.data, body.len);
    dbuf_free(&body);
    if (e) return e;

    pe->pend[pe->n_pend].corr = corr;
    pe->pend[pe->n_pend].sent_at = p->now;
    pe->n_pend++;
    /* Straight out if the socket will take it; the rest on POLLOUT. */
    if (chan_flush(&pe->c) != 0) {
        int r = peer_drop(p, pe, p->now + PEER_REDIAL_MS);
        return r ? r : BJ_OK;   /* the drop queued its own failure */
    }
    return BJ_OK;
}

static in_slot *inbound_for(peers *p, uint64_t conv) {
    for (uint32_t i = 0; i < p->n_in; i++)
        if (p->in[i].conv == conv && p->in[i].c.fd >= 0) return &p->in[i];
    return NULL;
}

static int answer_frame(peers *p, uint64_t from, uint64_t corr,
                        const uint8_t *value, uint32_t len, const char *error) {
    if (!p) return BJ_ERR_STATE;
    in_slot *s = inbound_for(p, from);
    /* The asker hung up while we were answering. Nothing to do and
     * nothing to report: its own transport has already failed the call. */
    if (!s) return BJ_OK;

    dbuf body = {0};
    int e = build_res(corr, value, len, error, &body);
    if (!e) e = chan_put(&s->c, body.data, body.len);
    dbuf_free(&body);
    if (e) return e;
    if (chan_flush(&s->c) != 0) chan_close(&s->c);
    return BJ_OK;
}

int peers_answer(peers *p, uint64_t from, uint64_t corr,
                 const uint8_t *msg, uint32_t len) {
    return answer_frame(p, from, corr, msg, len, NULL);
}

int peers_reject(peers *p, uint64_t from, uint64_t corr, const char *error) {
    return answer_frame(p, from, corr, NULL, 0, error ? error : "refused");
}

/* ---- one call, to an address -------------------------------------------- */

/* Wait for `fd`, or give up. EINTR restarts the whole timeout, which is
 * honest enough here: this runs before the poll loop exists, in a process
 * with nothing else going on. */
static int wait_for(int fd, short events, int timeout_ms) {
    for (;;) {
        struct pollfd pf;
        pf.fd = fd;
        pf.events = events;
        pf.revents = 0;
        int r = poll(&pf, 1, timeout_ms);
        if (r < 0 && errno == EINTR) continue;
        return r > 0 ? 0 : -1;
    }
}

int peers_call(const char *host, int port, const uint8_t *msg, uint32_t len,
               int timeout_ms, dbuf *reply, char *err, size_t err_cap) {
    if (err && err_cap) err[0] = '\0';
    if (!host || !msg || !reply || port <= 0 || port > 65535) return BJ_ERR_STATE;

    struct in_addr a;
    if (inet_pton(AF_INET, host, &a) != 1) return BJ_ERR_STATE;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return BJ_ERR_STATE;
    if (set_nonblocking(fd) != 0) { close(fd); return BJ_ERR_STATE; }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr = a;

    int r = connect(fd, (struct sockaddr *)&addr, sizeof addr);
    if (r != 0 && errno != EINPROGRESS && errno != EINTR) { close(fd); return BJ_ERR_STATE; }
    if (r != 0) {
        /* Any readiness at all, then SO_ERROR -- a refused connect is
         * reported on macOS as POLLIN|POLLHUP with no POLLOUT, and
         * waiting for writable waits for the whole timeout instead of
         * learning immediately that nobody is there. */
        if (wait_for(fd, POLLOUT | POLLIN, timeout_ms) != 0) { close(fd); return BJ_ERR_STATE; }
        int se = 0;
        socklen_t slen = sizeof se;
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &se, &slen) != 0 || se != 0) {
            close(fd);
            return BJ_ERR_STATE;
        }
    }

    chan c;
    chan_init(&c);
    c.fd = fd;

    dbuf body = {0};
    int e = build_req(0, msg, len, &body);
    if (!e) e = chan_put(&c, body.data, body.len);
    dbuf_free(&body);
    if (e) { chan_close(&c); return e; }

    while (c.out_off < c.out.len) {
        if (chan_flush(&c) != 0) { chan_close(&c); return BJ_ERR_STATE; }
        if (c.out_off >= c.out.len) break;
        if (wait_for(c.fd, POLLOUT, timeout_ms) != 0) { chan_close(&c); return BJ_ERR_STATE; }
    }

    const uint8_t *frame = NULL;
    uint32_t flen = 0;
    for (;;) {
        int got = chan_frame(&c, &frame, &flen);
        if (got == 0) break;
        if (got < 0) { chan_close(&c); return BJ_ERR_STATE; }
        if (wait_for(c.fd, POLLIN, timeout_ms) != 0) { chan_close(&c); return BJ_ERR_STATE; }
        if (chan_read(&c) != 0) { chan_close(&c); return BJ_ERR_STATE; }
    }

    uint64_t corr = 0;
    int rc = BJ_ERR_STATE;
    if (frame_kind(frame, flen) == 1 && frame_u64(frame, flen, "id", &corr) == 0 && corr == 0) {
        const uint8_t *okv; size_t oklen; int found = 0, ok = 0;
        if (obj_get_field(frame, flen, (const uint8_t *)"ok", 2, &okv, &oklen, &found) == BJ_OK &&
            found) {
            cur oc = { okv, oklen, 0 };
            if (read_bool(&oc, &ok) == BJ_OK) {
                const uint8_t *v = NULL;
                uint32_t vlen = 0;
                if (ok && frame_binary(frame, flen, "value", &v, &vlen) == 0) {
                    rc = dbuf_put(reply, v, vlen);
                } else if (!ok) {
                    /* A refusal is an ANSWER: the far end heard the
                     * question and said no, which is a different fact
                     * from not having reached it. */
                    if (err && err_cap) {
                        const uint8_t *s; size_t slen; int has = 0;
                        if (obj_get_field(frame, flen, (const uint8_t *)"error", 5,
                                          &s, &slen, &has) == BJ_OK && has) {
                            cur ec = { s, slen, 0 };
                            const uint8_t *t; uint32_t tlen;
                            if (take_string(&ec, &t, &tlen) == BJ_OK) {
                                size_t n = tlen < err_cap - 1 ? tlen : err_cap - 1;
                                memcpy(err, t, n);
                                err[n] = '\0';
                            }
                        }
                    }
                    rc = 1;
                }
            }
        }
    }
    chan_close(&c);
    return rc;
}

#else /* !NISABA_SOCKETS */

/*
 * wasm32-wasip1 has no socket() at all -- not a missing right, no such
 * function -- so a member there cannot be reached and cannot reach
 * anyone. The transport still LINKS, and refuses at the one call that
 * would create it, so --peer on that target fails at startup with a
 * message rather than half-working.
 */
struct peers { int unused; };

int  peers_new(peers **out) { (void)out; return BJ_ERR_STATE; }
void peers_free(peers *p) { (void)p; }
int  peers_add(peers *p, uint64_t id, const char *host, int port) {
    (void)p; (void)id; (void)host; (void)port; return BJ_ERR_STATE;
}
int  peers_set(peers *p, uint64_t id, const char *host, int port) {
    (void)p; (void)id; (void)host; (void)port; return BJ_ERR_STATE;
}
int  peers_remove(peers *p, uint64_t id) { (void)p; (void)id; return BJ_ERR_STATE; }
int  peers_listen(peers *p, const char *host, int port) {
    (void)p; (void)host; (void)port; return BJ_ERR_STATE;
}
uint32_t    peers_count(const peers *p) { (void)p; return 0; }
uint64_t    peers_id_at(const peers *p, uint32_t i) { (void)p; (void)i; return 0; }
const char *peers_host_at(const peers *p, uint32_t i) { (void)p; (void)i; return NULL; }
int         peers_port_at(const peers *p, uint32_t i) { (void)p; (void)i; return 0; }
const char *peers_self_host(const peers *p) { (void)p; return NULL; }
int         peers_self_port(const peers *p) { (void)p; return 0; }
uint32_t peers_watch(peers *p) { (void)p; return 0; }
int      peers_fd_at(const peers *p, uint32_t i, int *want_write) {
    (void)p; (void)i; (void)want_write; return -1;
}
void     peers_ready(peers *p, uint32_t i, int r, int w, int f) {
    (void)p; (void)i; (void)r; (void)w; (void)f;
}
int peers_pump(peers *p, uint64_t now) { (void)p; (void)now; return BJ_OK; }
int peers_next(peers *p, peer_event *ev, int *have) {
    (void)p; (void)ev; if (have) *have = 0; return BJ_OK;
}
int peers_request(peers *p, uint64_t id, uint64_t corr, const uint8_t *m, uint32_t l) {
    (void)p; (void)id; (void)corr; (void)m; (void)l; return 1;
}
int peers_answer(peers *p, uint64_t from, uint64_t corr, const uint8_t *m, uint32_t l) {
    (void)p; (void)from; (void)corr; (void)m; (void)l; return BJ_OK;
}
int peers_reject(peers *p, uint64_t from, uint64_t corr, const char *e) {
    (void)p; (void)from; (void)corr; (void)e; return BJ_OK;
}
int peers_call(const char *host, int port, const uint8_t *msg, uint32_t len,
               int timeout_ms, dbuf *reply, char *err, size_t err_cap) {
    (void)host; (void)port; (void)msg; (void)len; (void)timeout_ms; (void)reply;
    if (err && err_cap) err[0] = '\0';
    return BJ_ERR_STATE;
}

#endif /* NISABA_SOCKETS */
