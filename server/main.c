/*
 * server/main.c — the database server's transport, and nothing else.
 *
 * Step 3 of docs/steps/wasip2-database-server.md. Everything this file
 * does is move bytes: read a request frame, hand it to dbs_handle, write
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
 * ONE CONNECTION AT A TIME, and say it plainly because it is felt from
 * outside: accept, serve until that client goes away, accept again. A
 * second client waits in the listen backlog for as long as the first
 * holds its socket open -- fine for a CLI, which connects per command,
 * and wrong for a client that keeps a connection warm. Serving several
 * would mean poll() over the accepted fds (there are no threads here and
 * one process still owns the files, so the engine calls stay serialised
 * either way); it is the next thing this file should learn, and it is not
 * a change to anything above it. A test that held one connection open
 * and then ran the CLI against the same server hung on exactly this.
 */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#if defined(NISABA_SOCKETS)
#include <arpa/inet.h>
#include <netinet/in.h>
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
        if (bj_value_size(head, sizeof head, 0, &total) != BJ_OK) return -1;
        if (total < sizeof head || total > FRAME_MAX) return -1;

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
#endif

static void usage(const char *me) {
    fprintf(stderr,
            "usage: %s [--stdio] [--port N] [--order N]\n"
            "  serves the database in the preopened directory \".\"\n", me);
}

int main(int argc, char **argv) {
    int use_stdio = 0;
    int port = DEFAULT_PORT;
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
        fprintf(stderr, "nisaba: serving 127.0.0.1:%d\n", port);
        for (;;) {
            int c = accept(srv, NULL, NULL);
            if (c < 0) { if (errno == EINTR) continue; perror("accept"); break; }
            serve(s, c, c);
            close(c);
        }
        close(srv);
#else
        (void)port;   /* accepted and refused, rather than not accepted */
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
