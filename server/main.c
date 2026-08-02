/*
 * server/main.c — the database server's transport, and nothing else.
 *
 * See docs/db-server.md for the wire and the invariants. Everything this
 * file does is move bytes: read a request frame, hand it to dbs_handle, write
 * the response frame back. It does not know what an op is, what a
 * collection is, or what any field of either object means -- the same
 * rule the Raft transport has always held to, applied to this one.
 *
 * That is why it is small, and why the protocol is not tested here: the
 * grammar has its own tests in test/native/main.c, driven over buffers
 * with no socket and no port, and a round trip through this file proves
 * the wiring rather than the meaning.
 *
 * FRAMING IS THE FORMAT'S, NOT OURS
 *
 * A binjson object carries its own total size in its header, so a reader
 * takes nine bytes, asks bj_value_size how long the whole value is, and
 * reads the rest. There is no length prefix to disagree about and no
 * framing format to version.
 *
 * A frame that cannot be measured ends the CONNECTION rather than
 * producing an error response: a reader that has lost the frame boundary
 * cannot resynchronise, and answering would be pretending it had. Every
 * other refusal is a response (db_session.h), because those are answers
 * to a question that arrived intact.
 *
 * TWO TRANSPORTS, ONE SERVER
 *
 *   --stdio     frames on stdin/stdout. Works on every target, including
 *               wasm32-wasip1 and Node's WASI host, which have no
 *               sockets at all -- so the same binary is testable
 *               everywhere the engine builds.
 *   (default)   a TCP listener. Needs sockets, which means wasm32-wasip2
 *               or native; see wasm/build-server.sh.
 *
 * ONE PROCESS PER DATABASE DIRECTORY. The directory is the preopen (".")
 * and the process owns it for its lifetime. That is the whole answer to
 * concurrent writers: wasi-filesystem has no locking to arbitrate them
 * with, so there is never more than one.
 *
 * MANY CONNECTIONS, ONE AT A TIME THROUGH THE ENGINE. poll() over the
 * listener and every accepted socket; whichever is ready is served, and
 * a client that holds a connection open without asking anything costs a
 * table slot and nothing else. There are no threads and there is no
 * second engine: dbs_handle runs to completion for one request before
 * the next is looked at, so the database sees exactly the serial stream
 * it saw when there was one connection. What changed is who has to wait
 * for whom.
 *
 * That means the sockets are non-blocking and a connection carries state
 * -- the bytes of a request that has only partly arrived, and the bytes
 * of a response that has only partly gone out. A blocking read would
 * hand one client the whole server again, which is the bug this replaced
 * (a test that held a connection open and then ran the CLI against the
 * same server hung until it was killed).
 *
 * BOUNDED, AND IT SAYS SO. --max-clients (default and ceiling
 * MAX_CLIENTS) is a fixed table, refused explicitly when full, for the
 * reason every other table in this codebase is: a server that grows one
 * per client has a failure mode nobody tests. A connection arriving at
 * the cap is ACCEPTED and told -- {ok:false, code:-44, msg} in the shape
 * every other refusal arrives in -- rather than left in the listen
 * backlog, where it would be indistinguishable from a slow server.
 *
 * AND THE SLOTS COME BACK. --idle-timeout (60s; 0 disables) closes a
 * connection that has asked nothing for that long, with code -45 first
 * so the client learns why rather than seeing a bare disconnect. This is
 * about DEAD peers more than rude ones: a crashed client, a dropped NAT
 * mapping and a half-open socket all look exactly like an idle one to
 * TCP, and without a timer they hold their slots until the process
 * restarts. SO_KEEPALIVE is not the answer -- it defaults to hours, and
 * the knobs that shorten it are per-OS and not reliably there through
 * wasi-sockets.
 *
 * The timer measures SILENCE, not connectedness: it is reset when a
 * request is answered and when that answer has gone out, so a client
 * dribbling one byte at a time is closed like any other client that
 * asked nothing. A client that wants to stay warm sends {op:"ping"},
 * which is the one op that touches no collection.
 *
 * A clock, here, is legitimate: this is the transport. db.h keeps clocks
 * out of the ENGINE, which is why an insert's id is the caller's and not
 * C's, and nothing below this file learns what time it is. CLOCK_MONOTONIC
 * and a timed poll() were measured on all four hosts this ships to --
 * native, wasip1 under wasmtime and under Node's WASI host, and wasip2.
 */
/* POSIX before any header, the same way bjio_posix.c asks for it and for
 * the same reason: -std=c11 defines __STRICT_ANSI__, and a sysroot is
 * entitled to hide clock_gettime/CLOCK_MONOTONIC behind that. wasi-libc's
 * wasip2 headers do; its wasip1 headers do not, and neither does Darwin,
 * which is exactly how this class of thing reaches CI unnoticed. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#if defined(NISABA_SOCKETS)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#endif

#include "db_session.h"
#include "replica.h"
#include "peers.h"
#include "db_names.h"
#include "db_validate.h"   /* dc_strerror: a refusal says why, even here */
#include "bjio_posix.h"
#include "binjson.h"
#include "dbuf.h"

/* The header that carries a value's own length: type byte, u32 size,
 * u32 count. Five would do -- the size is in the first five -- but nine
 * is the whole object header and reading it costs nothing. */
#define FRAME_HEADER 9

/* A request larger than this is refused as a framing error rather than
 * allocated. Bounded for the reason every other table here is: a server
 * that allocates whatever a client claims has a failure mode nobody
 * tests. */
#define FRAME_MAX (16u * 1024u * 1024u)

/* How many unsent bytes a connection may be holding before the loop
 * stops handing it change events. Not a limit on a RESPONSE -- a client
 * that asked for a million documents gets them -- but on how far ahead
 * of a slow reader the push side will run. */
#define OUT_HIGH_WATER (64u * 1024u)

#define DEFAULT_PORT 8097

/* Connections held at once. The ceiling AND the default: --max-clients
 * can only lower it, because the table is allocated once at startup and
 * the worst case a reader has to reason about is this number times the
 * largest frame in flight. */
#define MAX_CLIENTS 64

/* Seconds of silence before a connection's slot is taken back; 0 turns
 * the timer off. Sixty is short enough that a dead peer does not hold a
 * slot for long and long enough that a warm client pinging on any sane
 * interval never notices. */
#define DEFAULT_IDLE_TIMEOUT 60

/* A connection's buffers are reused between requests, but a client that
 * once sent a large frame should not hold that memory for the rest of
 * its life -- with MAX_CLIENTS of them, that is the difference between a
 * bounded server and a bounded-in-theory one. */
#define IDLE_BUFFER (64u * 1024u)

/* How long a request frame claims to be, from its header alone.
 *   0 = measured, *total set   1 = not enough bytes yet   -1 = unmeasurable
 * Unmeasurable ends the connection: a reader that has lost the frame
 * boundary cannot resynchronise, and answering would be pretending it
 * had. */
static int frame_total(const uint8_t *p, size_t len, size_t *total) {
    if (len < FRAME_HEADER) return 1;
    if (bj_value_size(p, len, 0, total) != BJ_OK) return -1;
    if (*total < FRAME_HEADER || *total > FRAME_MAX) return -1;
    return 0;
}

static int read_exact(int fd, uint8_t *p, size_t n) {
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(fd, p + got, n - got);
        if (r == 0) return got == 0 ? 1 : -1;   /* 1 = clean EOF, -1 = torn */
        if (r < 0) { if (errno == EINTR) continue; return -1; }
        got += (size_t)r;
    }
    return 0;
}

static int write_all(int fd, const uint8_t *p, size_t n) {
    size_t put = 0;
    while (put < n) {
        ssize_t w = write(fd, p + put, n - put);
        if (w < 0) { if (errno == EINTR) continue; return -1; }
        put += (size_t)w;
    }
    return 0;
}

/*
 * Milliseconds on a monotonic clock -- not a wall clock, which can step
 * backwards over an NTP correction and would take a connection's slot
 * away for it. A clock that cannot be read stops instead of jumping: the
 * last reading is returned, so nothing times out, which is the safe
 * direction to fail in.
 *
 * Above the sockets guard because both transports need it now: the idle
 * timer is the listener's, but the Raft timers are the replica's, and a
 * --stdio server can be a replica too (it just has no peers to reach).
 */
static uint64_t now_ms(void) {
    static uint64_t last = 0;
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return last;
    last = (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
    return last;
}


/*
 * Serve one connection until it closes or loses framing. Returns 0 on a
 * clean close, -1 otherwise -- neither is fatal to the server, because
 * one client's bad frame is not the next client's problem.
 */
static int serve(dbs *s, replica *rep, int in_fd, int out_fd) {
    for (;;) {
        uint8_t head[FRAME_HEADER];
        int r = read_exact(in_fd, head, sizeof head);
        if (r == 1) return 0;
        if (r < 0) return -1;

        size_t total = 0;
        /* The header is exactly what frame_total needs, so it cannot come
         * back "wait for more" here -- one measurement, shared with the
         * socket path, which is the point of it being a function. */
        if (frame_total(head, sizeof head, &total) != 0) return -1;

        uint8_t *req = (uint8_t *)malloc(total);
        if (!req) return -1;
        memcpy(req, head, sizeof head);
        if (total > sizeof head &&
            read_exact(in_fd, req + sizeof head, total - sizeof head) != 0) {
            free(req);
            return -1;
        }

        dbuf res = {0};
        /* --stdio is one client by construction; it takes the first id. */
        int e = rep ? replica_submit(rep, 1, req, total, &res)
                    : dbs_handle(s, 1, req, total, &res);
        free(req);
        if (e < 0) { dbuf_free(&res); return -1; }   /* no response was built */
        /*
         * A replicated write is not answered by the call that took it:
         * the entry has to commit and apply first. There is one client
         * here and nothing else for the process to do, so it turns the
         * clock until the answer exists -- which on a group of one is
         * immediately, and with peers is however long a quorum takes.
         */
        if (e == 1) {
            uint64_t owner = 0;
            int have = 0;
            for (int spin = 0; !have && spin < 100000; spin++) {
                if (replica_tick(rep, now_ms()) != 0) { dbuf_free(&res); return -1; }
                if (replica_ready(rep, &owner, &res, &have) != 0) { dbuf_free(&res); return -1; }
            }
            if (!have) { dbuf_free(&res); return -1; }
        }

        int w = write_all(out_fd, res.data, res.len);
        dbuf_free(&res);
        if (w != 0) return -1;

        /*
         * And whatever this client is owed as a watcher. There is no
         * poll here and nothing to be woken by -- --stdio is one client
         * by construction, so the only thing that ever happens is a
         * request -- which means events are delivered AFTER THE NEXT
         * REQUEST rather than as they occur. A watcher on this transport
         * that wants them promptly sends pings; docs/db-server.md says
         * so. The alternative would be a reader thread, and this file
         * has neither threads nor the wish for any.
         */
        for (;;) {
            dbuf frame = {0};
            int have = 0;
            if (dbs_stream_take(s, 1, &frame, &have) != 0 || !have) {
                dbuf_free(&frame);
                break;
            }
            w = write_all(out_fd, frame.data, frame.len);
            dbuf_free(&frame);
            if (w != 0) return -1;
        }
    }
}

#if defined(NISABA_SOCKETS)

/*
 * One accepted client. `in` holds the bytes of a request that has only
 * partly arrived; `out` holds the bytes of responses that have only
 * partly gone out, with out_off marking how far. Both are empty between
 * requests, which is the state a connection spends nearly all its life
 * in -- 64 of these idle costs 64 file descriptors and two null
 * pointers each.
 */
typedef struct {
    int fd;
    uint64_t client;        /* never reused: cursors are owned by it */
    uint8_t *in;
    size_t in_len, in_cap;
    dbuf out;
    size_t out_off;
    uint64_t quiet_since;   /* monotonic ms; the idle timer's zero */
} conn;

/* Forget what a slot held WITHOUT freeing it: for the slot a live
 * connection has just been moved out of, whose buffers now belong to
 * wherever it moved to. Freeing here would free them out from under it. */
static void conn_clear(conn *c) {
    memset(c, 0, sizeof *c);
    c->fd = -1;
}

/* Release everything a slot owns and leave it empty. */
static void conn_reset(conn *c) {
    free(c->in);
    dbuf_free(&c->out);
    conn_clear(c);
}

static void conn_close(conn *c) {
    if (c->fd >= 0) close(c->fd);
    conn_reset(c);
}

/*
 * A connection ending, however it ended -- cleanly, torn, timed out, or
 * with the server shutting down. Its cursors go with it: the session
 * holds them, the transport is the only thing that knows the client is
 * gone, and a scan nobody can reach again is a scan holding a slot. So
 * does any write it was still waiting on: the entries stay committed
 * and still apply, but the answer has nowhere to go.
 */
static void conn_gone(dbs *s, replica *rep, conn *c) {
    dbs_drop_client(s, c->client);
    if (rep) replica_drop_client(rep, c->client);
    conn_close(c);
}

static int conn_reserve(conn *c, size_t need) {
    if (need <= c->in_cap) return 0;
    size_t nc = c->in_cap ? c->in_cap : 4096;
    while (nc < need) {
        if (nc > FRAME_MAX) return -1;
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

/* Push out whatever is pending. 0 = made progress or nothing to do,
 * -1 = the connection is gone. Never blocks: the fd is non-blocking, and
 * what does not go now goes on the next POLLOUT. */
static int conn_flush(conn *c) {
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
    if (c->out.cap > IDLE_BUFFER) dbuf_free(&c->out);
    c->quiet_since = now_ms();   /* answered: the silence starts again here */
    return 0;
}

/*
 * Read what has arrived, answer every whole request in it, and start the
 * response on its way. 0 = still here, -1 = closed or lost framing.
 *
 * A response is appended to `out` rather than written straight back
 * because the client may not be reading yet; the engine has already
 * finished with the request by then either way, so a slow reader delays
 * nobody but itself.
 */
static int conn_readable(dbs *s, replica *rep, conn *c) {
    uint8_t chunk[8192];
    ssize_t r = read(c->fd, chunk, sizeof chunk);
    if (r == 0) return -1;                            /* clean EOF */
    if (r < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }
    if (conn_reserve(c, c->in_len + (size_t)r)) return -1;
    memcpy(c->in + c->in_len, chunk, (size_t)r);
    c->in_len += (size_t)r;

    for (;;) {
        size_t total = 0;
        int m = frame_total(c->in, c->in_len, &total);
        if (m > 0) break;                             /* wait for more */
        if (m < 0) return -1;
        if (c->in_len < total) break;

        /* A replicated write is not answered here: it is planned,
         * proposed, and left for the log. Its response reaches this
         * connection through replica_ready, once the entries it became
         * have committed and applied. */
        int e = rep ? replica_submit(rep, c->client, c->in, total, &c->out)
                    : dbs_handle(s, c->client, c->in, total, &c->out);
        if (e < 0) return -1;                         /* no response was built */
        c->quiet_since = now_ms();                    /* it asked something */

        c->in_len -= total;
        memmove(c->in, c->in + total, c->in_len);
    }
    if (c->in_len == 0 && c->in_cap > IDLE_BUFFER) {
        free(c->in);
        c->in = NULL;
        c->in_cap = 0;
    }
    return conn_flush(c);
}

/*
 * Tell a connection why it is being closed -- the table was full when it
 * arrived, or it went quiet -- in the shape every other refusal arrives
 * in (db_session.h owns it). Best effort by design: one non-blocking
 * write and then close, because a client that will not read its own
 * refusal must not become a reason to hold a slot.
 */
static void refuse_and_close(int fd, int code) {
    dbuf msg = {0};
    if (dbs_refusal(code, &msg) == BJ_OK && msg.data) {
        ssize_t ignored = write(fd, msg.data, msg.len);
        (void)ignored;
    }
    dbuf_free(&msg);
    close(fd);
}

static int listen_on(int port) {
    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("socket"); return -1; }
    int one = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(0x7f000001);   /* loopback only */

    if (bind(srv, (struct sockaddr *)&addr, sizeof addr) != 0) { perror("bind"); close(srv); return -1; }
    if (listen(srv, 16) != 0) { perror("listen"); close(srv); return -1; }
    return srv;
}

/*
 * The server loop: poll the listener and every client, serve whichever
 * is ready, repeat. Returns only on a failure that ends the server --
 * one client's bad frame ends that client and nothing else.
 *
 * A client is polled for POLLOUT while it owes bytes and POLLIN
 * otherwise, which is also the backpressure: nothing new is read from a
 * client whose last answer has not gone out, so a pipelining client
 * cannot make the server hold an unbounded number of answers for it.
 */
static int serve_forever(dbs *s, replica *rep, peers *px, int srv,
                         int max_clients, int idle_seconds) {
    const uint64_t idle_ms = (uint64_t)(idle_seconds > 0 ? idle_seconds : 0) * 1000u;
    conn *cs = (conn *)calloc((size_t)max_clients, sizeof *cs);
    /* Clients, the listener, and whatever the peer transport asks to
     * have watched -- its own listener and one socket per direction per
     * member (server/peers.h bounds it, which is what makes this one
     * allocation rather than a growing array). */
    struct pollfd *pf = (struct pollfd *)calloc((size_t)max_clients + 1 + PEERS_MAX_FDS,
                                                sizeof *pf);
    if (!cs || !pf) { free(cs); free(pf); return -1; }
    for (int i = 0; i < max_clients; i++) cs[i].fd = -1;
    int n = 0;              /* live connections, kept packed at the front of cs */
    /* 1 is --stdio's, so a client id means the same thing in both
     * transports and neither can be mistaken for "no client". */
    uint64_t clients = 1;

    for (;;) {
        pf[0].fd = srv;
        pf[0].events = POLLIN;
        pf[0].revents = 0;
        for (int i = 0; i < n; i++) {
            pf[i + 1].fd = cs[i].fd;
            pf[i + 1].events = (cs[i].out.len > cs[i].out_off) ? POLLOUT : POLLIN;
            pf[i + 1].revents = 0;
        }
        /* The peers, after the clients: their indices are the
         * transport's own and stay meaningful for exactly this pass. */
        const int pbase = n + 1;
        const uint32_t pn = px ? peers_watch(px) : 0;
        for (uint32_t i = 0; i < pn; i++) {
            int want_write = 0;
            pf[pbase + i].fd = peers_fd_at(px, i, &want_write);
            /* Both directions, always. A client is polled for one at a
             * time on purpose -- that is the backpressure -- but a peer
             * pair is symmetric, and two members that each stopped
             * reading while they owed bytes would wedge each other. What
             * bounds a peer's traffic is the node: one request in flight
             * per peer, and no more until it is answered. */
            pf[pbase + i].events = (short)(POLLIN | (want_write ? POLLOUT : 0));
            pf[pbase + i].revents = 0;
        }

        /* Sleep until something happens or the earliest deadline, rather
         * than waking on a tick to find nothing to do. No timer, no
         * timeout: poll blocks until a socket is ready. */
        int wait_ms = -1;
        /*
         * Except when somebody is owed a change event. Everything else
         * here becomes deliverable because a socket did something, which
         * is what poll waits for; an event becomes deliverable because
         * ANOTHER client wrote, and the connection it is owed to may be
         * silent, backed up, or both. Sleeping on that is how an
         * overflow notice sits undelivered until an unrelated request
         * happens to wake the loop.
         */
        if (dbs_stream_pending(s)) wait_ms = 0;
        else if (idle_ms && n > 0) {
            uint64_t now = now_ms(), earliest = 0;
            for (int i = 0; i < n; i++)
                if (i == 0 || cs[i].quiet_since < earliest) earliest = cs[i].quiet_since;
            uint64_t due = earliest + idle_ms;
            wait_ms = due <= now ? 0 : (int)(due - now);
        }
        /*
         * And the Raft timers, which are the second thing here that
         * happens because time passed rather than because a socket did.
         * An election that never fires because nothing connected is a
         * cluster that never elects; whichever deadline is nearer wins.
         */
        if (rep) {
            int tick = replica_wait_ms(rep, now_ms());
            if (wait_ms < 0 || tick < wait_ms) wait_ms = tick;
        }

        int r = poll(pf, (nfds_t)(n + 1 + (int)pn), wait_ms);
        if (r < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }

        /*
         * The peers, before the clock: bytes that have arrived are what
         * the node's next tick has to reason about, and a vote or an
         * append left sitting in a socket buffer for one more pass is an
         * election timeout's worth of latency on every message.
         */
        /* The transport's own clock first: it is what a request in
         * flight is timed against, and what a connection dropped during
         * this pass dates its next dial from. */
        if (px) peers_pump(px, now_ms());
        for (uint32_t i = 0; i < pn; i++) {
            short ev = pf[pbase + i].revents;
            if (!ev) continue;
            peers_ready(px, i, (ev & (POLLIN | POLLHUP)) != 0,
                        (ev & POLLOUT) != 0, (ev & (POLLERR | POLLNVAL)) != 0);
        }

        /* The clock, before anything reads a role or a commit index off
         * the node: an election that is due is due whether or not a
         * socket woke us. */
        if (rep) {
            int te = replica_tick(rep, now_ms());
            if (te != 0) {
                /* What halted it, not merely that it did: every reason
                 * is a different operator problem, and a replica that
                 * stops without saying which has told nobody anything. */
                fprintf(stderr, "replica: halted (%d: %s)\n", te, dc_strerror(te));
                break;
            }
        }

        /* Whatever has gone quiet, before anything else: a slot held by a
         * client that is not there any more is the one this exists for. */
        if (idle_ms) {
            uint64_t now = now_ms();
            for (int i = n - 1; i >= 0; i--) {
                if (now - cs[i].quiet_since < idle_ms) continue;
                int fd = cs[i].fd;
                cs[i].fd = -1;              /* conn_gone must not close it first */
                conn_gone(s, rep, &cs[i]);
                refuse_and_close(fd, DC_ERR_IDLE_TIMEOUT);
                cs[i] = cs[n - 1];
                conn_clear(&cs[n - 1]);
                n--;
                pf[i + 1].revents = 0;      /* its slot now holds a different client */
            }
        }

        /* Clients first, so a burst of connections cannot starve the
         * clients already being served. Backwards, because dropping one
         * moves the last into its slot. */
        for (int i = n - 1; i >= 0; i--) {
            short ev = pf[i + 1].revents;
            if (!ev) continue;
            int dead = 0;
            if (ev & (POLLERR | POLLNVAL)) dead = 1;
            else if (ev & POLLOUT) dead = conn_flush(&cs[i]) != 0;
            else if (ev & (POLLIN | POLLHUP)) dead = conn_readable(s, rep, &cs[i]) != 0;
            if (dead) {
                conn_gone(s, rep, &cs[i]);
                cs[i] = cs[n - 1];      /* the last one moves into the hole */
                conn_clear(&cs[n - 1]); /* and its old slot owns nothing now */
                n--;
            }
        }

        /*
         * Frames nobody asked for: the change events other clients'
         * writes produced this pass. Appended to whatever each
         * connection already owes, so the ordinary flush carries them
         * out -- the transport still writes bytes it has not read.
         *
         * After the request loop, so an event caused by a write reaches
         * every watcher on the same pass that performed it. A connection
         * whose buffer is already growing is one whose peer has stopped
         * reading; the session's per-stream bound is what stops that
         * becoming this process's problem, and it says so (-60, on the
         * stream, when it overflows).
         */
        for (int i = n - 1; i >= 0; i--) {
            int any = 0;
            for (;;) {
                /* Stop filling a connection that is not draining. This
                 * is what makes the SESSION's per-stream bound the real
                 * one: without it the backlog would simply move here
                 * instead, into a buffer nothing counts, and a consumer
                 * that stopped reading would cost this process memory
                 * rather than costing itself its stream. */
                if (cs[i].out.len - cs[i].out_off >= OUT_HIGH_WATER) break;
                int have = 0;
                if (dbs_stream_take(s, cs[i].client, &cs[i].out, &have) != 0) break;
                if (!have) break;
                any = 1;
            }
            /* Written now rather than left for the next POLLOUT: poll
             * sleeps until a socket is ready or a connection goes idle,
             * and an event nobody is waiting on would sit there for the
             * whole idle timeout. Backwards, and reaping the same way,
             * because a failed write is a connection that has gone. */
            if (!any) continue;
            if (conn_flush(&cs[i]) != 0) {
                conn_gone(s, rep, &cs[i]);
                cs[i] = cs[n - 1];
                conn_clear(&cs[n - 1]);
                n--;
            }
        }

        /*
         * Answers to writes that have now committed and applied. They
         * are appended to whatever their connection already owes, so the
         * ordinary flush carries them out -- the same path a change
         * event takes, for the same reason: the transport still writes
         * bytes nobody has just read.
         */
        if (rep) {
            for (;;) {
                uint64_t owner = 0;
                int have = 0;
                dbuf answer = {0};
                if (replica_ready(rep, &owner, &answer, &have) != 0) { dbuf_free(&answer); break; }
                if (!have) { dbuf_free(&answer); break; }
                int found = -1;
                for (int i = 0; i < n; i++) if (cs[i].client == owner) { found = i; break; }
                /* Nobody to give it to: the connection went while its
                 * write was in the log. The entries still committed and
                 * still applied -- that is what committed means. */
                if (found >= 0) {
                    if (dbuf_put(&cs[found].out, answer.data, answer.len) == 0 &&
                        conn_flush(&cs[found]) != 0) {
                        conn_gone(s, rep, &cs[found]);
                        cs[found] = cs[n - 1];
                        conn_clear(&cs[n - 1]);
                        n--;
                    }
                }
                dbuf_free(&answer);
            }
        }

        if (pf[0].revents & POLLIN) {
            int c = accept(srv, NULL, NULL);
            if (c < 0) {
                if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) perror("accept");
            } else if (n >= max_clients) {
                refuse_and_close(c, DC_ERR_TOO_MANY_CLIENTS);
            } else if (set_nonblocking(c) != 0) {
                close(c);
            } else {
                conn_reset(&cs[n]);
                cs[n].fd = c;
                cs[n].client = ++clients;   /* ids are never reused */
                cs[n].quiet_since = now_ms();
                n++;
            }
        }
    }

    for (int i = 0; i < n; i++) conn_gone(s, rep, &cs[i]);
    free(cs);
    free(pf);
    return -1;
}
#endif

static void usage(const char *me) {
    fprintf(stderr,
            "usage: %s [--stdio] [--port N] [--order N] [--max-clients N]\n"
            "           [--idle-timeout SECONDS] [--raft NODE_ID]\n"
            "           [--raft-port N] [--peer ID@HOST:PORT ...]\n"
            "  serves the database in the preopened directory \".\"\n"
            "  --raft replicates every write through a log before applying it\n"
            "  --raft-port is where the other members reach this one\n"
            "  --peer names a member and where to reach IT; repeat per member\n", me);
}

/*
 * `ID@HOST:PORT`. One member and where to reach it, which is the whole
 * of what this process is told about the cluster: there is no join here,
 * so the set every member is started with has to be the same set.
 */
static int parse_peer(const char *spec, uint64_t *id, char *host, size_t host_cap,
                      int *port) {
    const char *at = strchr(spec, '@');
    if (!at || at == spec) return -1;
    char *end = NULL;
    long long n = strtoll(spec, &end, 10);
    if (end != at || n <= 0) return -1;
    const char *colon = strrchr(at + 1, ':');
    if (!colon || colon == at + 1) return -1;
    size_t hlen = (size_t)(colon - (at + 1));
    if (hlen >= host_cap) return -1;
    memcpy(host, at + 1, hlen);
    host[hlen] = '\0';
    int p = atoi(colon + 1);
    if (p <= 0 || p > 65535) return -1;
    *id = (uint64_t)n;
    *port = p;
    return 0;
}

int main(int argc, char **argv) {
    int use_stdio = 0;
    int port = DEFAULT_PORT;
    int max_clients = MAX_CLIENTS;
    int idle_seconds = DEFAULT_IDLE_TIMEOUT;
    /* The order the files were WRITTEN with, which is not a preference:
     * open a tree with the wrong one and its pages read as nonsense. The
     * default is what every host in this repo creates with, so the flag
     * exists for a database made with `db ... --order N`. */
    int order = DC_DEFAULT_ORDER;
    /* 0 = not a replica: every write is applied where it lands, which is
     * what this server has always done and what it still does by
     * default. A node id turns the log on. */
    int node_id = 0;
    /* Where the other members reach this one, and who they are. Both
     * empty is a group of one: a replica with nobody to count but
     * itself, which is what --raft alone has always meant. */
    int raft_port = 0;
    const char *peer_spec[PEERS_MAX];
    int n_peers = 0;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--stdio") == 0) use_stdio = 1;
        else if (strcmp(argv[i], "--port") == 0 && i + 1 < argc) port = atoi(argv[++i]);
        else if (strcmp(argv[i], "--order") == 0 && i + 1 < argc) {
            order = atoi(argv[++i]);
            if (order < 3) { fprintf(stderr, "--order must be at least 3\n"); return 2; }
        }
        else if (strcmp(argv[i], "--idle-timeout") == 0 && i + 1 < argc) {
            idle_seconds = atoi(argv[++i]);
            if (idle_seconds < 0) { fprintf(stderr, "--idle-timeout cannot be negative\n"); return 2; }
        }
        else if (strcmp(argv[i], "--max-clients") == 0 && i + 1 < argc) {
            max_clients = atoi(argv[++i]);
            if (max_clients < 1 || max_clients > MAX_CLIENTS) {
                fprintf(stderr, "--max-clients must be between 1 and %d\n", MAX_CLIENTS);
                return 2;
            }
        }
        else if (strcmp(argv[i], "--raft") == 0 && i + 1 < argc) {
            node_id = atoi(argv[++i]);
            if (node_id <= 0) { fprintf(stderr, "--raft needs a positive node id\n"); return 2; }
        }
        else if (strcmp(argv[i], "--raft-port") == 0 && i + 1 < argc) {
            raft_port = atoi(argv[++i]);
            if (raft_port <= 0 || raft_port > 65535) {
                fprintf(stderr, "--raft-port must be a port number\n");
                return 2;
            }
        }
        else if (strcmp(argv[i], "--peer") == 0 && i + 1 < argc) {
            if (n_peers >= PEERS_MAX) {
                fprintf(stderr, "at most %d peers\n", PEERS_MAX);
                return 2;
            }
            peer_spec[n_peers++] = argv[++i];
        }
        else { usage(argv[0]); return 2; }
    }

    if ((raft_port || n_peers) && node_id <= 0) {
        fprintf(stderr, "--raft-port and --peer need --raft NODE_ID\n");
        return 2;
    }
    /* A member that cannot be reached is a member nothing can replicate
     * to: it would vote, and then silently never receive an entry. */
    if (n_peers && !raft_port) {
        fprintf(stderr, "--peer needs --raft-port: the others have to reach this one\n");
        return 2;
    }
    /* --stdio is one client and no poll loop, so nothing would ever read
     * a peer socket. Refused rather than half-working. */
    if ((raft_port || n_peers) && use_stdio) {
        fprintf(stderr, "--stdio cannot join a cluster: there is no poll loop to"
                        " serve peers with\n");
        return 2;
    }

    int dirfd = open(".", O_RDONLY);
    if (dirfd < 0) { perror("open ."); return 1; }

    bj_ns ns;
    if (bjns_posix_open(dirfd, &ns) != BJ_OK) {
        fprintf(stderr, "cannot open the database directory\n");
        return 1;
    }

    dbs *s = NULL;
    /* create: a directory the operator pointed this at becomes a
     * database. Refusing to start because a fresh directory has no
     * catalog would make "serve this directory" a two-step operation
     * with a different tool for the first step. */
    int e = dbs_open(&ns, order, 1, &s);
    if (e != BJ_OK) {
        fprintf(stderr, "cannot open the database: %s\n", dc_strerror(e));
        bjns_posix_free(&ns);
        return 1;
    }

    /*
     * The Raft half, if this is a member. It opens the log, which is the
     * one thing in the directory the session does not own -- so it comes
     * after dbs_open and goes before it at the end.
     */
    /*
     * The peer transport, if this member has anyone to talk to. It comes
     * before the node because the node's member set is built from it --
     * one place the cluster's shape is stated, and it is this one.
     */
    peers *px = NULL;
    if (raft_port) {
        if (peers_new(&px) != BJ_OK ||
            peers_listen(px, "127.0.0.1", raft_port) != BJ_OK) {
            fprintf(stderr, "cannot listen for peers on 127.0.0.1:%d\n", raft_port);
            peers_free(px);
            dbs_close(s);
            bjns_posix_free(&ns);
            return 1;
        }
        for (int i = 0; i < n_peers; i++) {
            uint64_t id = 0;
            char host[PEERS_HOST_MAX];
            int pport = 0;
            if (parse_peer(peer_spec[i], &id, host, sizeof host, &pport) != 0 ||
                (uint64_t)node_id == id ||
                peers_add(px, id, host, pport) != BJ_OK) {
                fprintf(stderr, "bad --peer %s (want ID@HOST:PORT, and not this node)\n",
                        peer_spec[i]);
                peers_free(px);
                dbs_close(s);
                bjns_posix_free(&ns);
                return 1;
            }
        }
    }

    replica *rep = NULL;
    if (node_id > 0) {
        e = replica_open(&ns, s, (uint64_t)node_id, px, now_ms(), &rep);
        if (e != BJ_OK) {
            fprintf(stderr, "cannot open the log: %s\n", dc_strerror(e));
            peers_free(px);
            dbs_close(s);
            bjns_posix_free(&ns);
            return 1;
        }
        fprintf(stderr, "nisaba: node %d, replicating (%u peer(s))\n",
                node_id, peers_count(px));
        fflush(stderr);
    }

    int rc = 0;
    if (use_stdio) {
        rc = serve(s, rep, STDIN_FILENO, STDOUT_FILENO) == 0 ? 0 : 1;
    } else {
#if defined(NISABA_SOCKETS)
        int srv = listen_on(port);
        if (srv < 0) { rc = 1; goto done; }
        /* The line the tests (and a person) wait for: it means bound and
         * listening, not merely started. */
        fprintf(stderr, "nisaba: serving 127.0.0.1:%d (max %d clients, idle timeout %ds)\n",
                port, max_clients, idle_seconds);
        fflush(stderr);
        rc = serve_forever(s, rep, px, srv, max_clients, idle_seconds) == 0 ? 0 : 1;
        close(srv);
#else
        (void)port;   /* accepted and refused, rather than not accepted */
        (void)max_clients;
        (void)idle_seconds;
        fprintf(stderr,
                "this build has no sockets (wasm32-wasip1 has none at all);"
                " use --stdio\n");
        rc = 2;
#endif
    }

#if defined(NISABA_SOCKETS)
done:
#endif
    /* The node before the session: it holds plans that point at the
     * collections dbs_close is about to free. And the transport after
     * the node, which borrows it. */
    replica_close(rep);
    peers_free(px);
    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
    return rc;
}
