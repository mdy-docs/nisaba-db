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
 * Serve one connection until it closes or loses framing. Returns 0 on a
 * clean close, -1 otherwise -- neither is fatal to the server, because
 * one client's bad frame is not the next client's problem.
 */
static int serve(dbs *s, int in_fd, int out_fd) {
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
        int e = dbs_handle(s, req, total, &res);
        free(req);
        if (e) { dbuf_free(&res); return -1; }   /* no response could be built */

        int w = write_all(out_fd, res.data, res.len);
        dbuf_free(&res);
        if (w != 0) return -1;
    }
}

#if defined(NISABA_SOCKETS)
/*
 * Milliseconds on a monotonic clock -- not a wall clock, which can step
 * backwards over an NTP correction and would take a connection's slot
 * away for it. A clock that cannot be read stops instead of jumping: the
 * last reading is returned, so nothing times out, which is the safe
 * direction to fail in.
 */
static uint64_t now_ms(void) {
    static uint64_t last = 0;
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return last;
    last = (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
    return last;
}

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
static int conn_readable(dbs *s, conn *c) {
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

        int e = dbs_handle(s, c->in, total, &c->out);
        if (e) return -1;                             /* no response could be built */
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
static int serve_forever(dbs *s, int srv, int max_clients, int idle_seconds) {
    const uint64_t idle_ms = (uint64_t)(idle_seconds > 0 ? idle_seconds : 0) * 1000u;
    conn *cs = (conn *)calloc((size_t)max_clients, sizeof *cs);
    struct pollfd *pf = (struct pollfd *)calloc((size_t)max_clients + 1, sizeof *pf);
    if (!cs || !pf) { free(cs); free(pf); return -1; }
    for (int i = 0; i < max_clients; i++) cs[i].fd = -1;
    int n = 0;   /* live connections, kept packed at the front of cs */

    for (;;) {
        pf[0].fd = srv;
        pf[0].events = POLLIN;
        pf[0].revents = 0;
        for (int i = 0; i < n; i++) {
            pf[i + 1].fd = cs[i].fd;
            pf[i + 1].events = (cs[i].out.len > cs[i].out_off) ? POLLOUT : POLLIN;
            pf[i + 1].revents = 0;
        }

        /* Sleep until something happens or the earliest deadline, rather
         * than waking on a tick to find nothing to do. No timer, no
         * timeout: poll blocks until a socket is ready. */
        int wait_ms = -1;
        if (idle_ms && n > 0) {
            uint64_t now = now_ms(), earliest = 0;
            for (int i = 0; i < n; i++)
                if (i == 0 || cs[i].quiet_since < earliest) earliest = cs[i].quiet_since;
            uint64_t due = earliest + idle_ms;
            wait_ms = due <= now ? 0 : (int)(due - now);
        }

        int r = poll(pf, (nfds_t)(n + 1), wait_ms);
        if (r < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }

        /* Whatever has gone quiet, before anything else: a slot held by a
         * client that is not there any more is the one this exists for. */
        if (idle_ms) {
            uint64_t now = now_ms();
            for (int i = n - 1; i >= 0; i--) {
                if (now - cs[i].quiet_since < idle_ms) continue;
                int fd = cs[i].fd;
                cs[i].fd = -1;              /* conn_close must not close it first */
                conn_close(&cs[i]);
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
            else if (ev & (POLLIN | POLLHUP)) dead = conn_readable(s, &cs[i]) != 0;
            if (dead) {
                conn_close(&cs[i]);
                cs[i] = cs[n - 1];      /* the last one moves into the hole */
                conn_clear(&cs[n - 1]); /* and its old slot owns nothing now */
                n--;
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
                cs[n].quiet_since = now_ms();
                n++;
            }
        }
    }

    for (int i = 0; i < n; i++) conn_close(&cs[i]);
    free(cs);
    free(pf);
    return -1;
}
#endif

static void usage(const char *me) {
    fprintf(stderr,
            "usage: %s [--stdio] [--port N] [--order N] [--max-clients N]\n"
            "           [--idle-timeout SECONDS]\n"
            "  serves the database in the preopened directory \".\"\n", me);
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
        else { usage(argv[0]); return 2; }
    }

    int dirfd = open(".", O_RDONLY);
    if (dirfd < 0) { perror("open ."); return 1; }

    bj_ns ns;
    if (bjns_posix_open(dirfd, &ns) != BJ_OK) {
        fprintf(stderr, "cannot open the database directory\n");
        return 1;
    }

    dbs *s = NULL;
    int e = dbs_open(&ns, order, &s);
    if (e != BJ_OK) {
        fprintf(stderr, "cannot open the database: %s\n", dc_strerror(e));
        bjns_posix_free(&ns);
        return 1;
    }

    int rc = 0;
    if (use_stdio) {
        rc = serve(s, STDIN_FILENO, STDOUT_FILENO) == 0 ? 0 : 1;
    } else {
#if defined(NISABA_SOCKETS)
        int srv = listen_on(port);
        if (srv < 0) { rc = 1; goto done; }
        /* The line the tests (and a person) wait for: it means bound and
         * listening, not merely started. */
        fprintf(stderr, "nisaba: serving 127.0.0.1:%d (max %d clients, idle timeout %ds)\n",
                port, max_clients, idle_seconds);
        fflush(stderr);
        rc = serve_forever(s, srv, max_clients, idle_seconds) == 0 ? 0 : 1;
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
    dbs_close(s);
    bjns_posix_free(&ns);
    close(dirfd);
    return rc;
}
