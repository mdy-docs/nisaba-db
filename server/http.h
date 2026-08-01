/*
 * http.h — an HTTP/1.1 subset, over buffers.
 *
 * NOTHING HERE KNOWS WHAT NISABA IS. It parses a request out of bytes
 * and writes a response into bytes; it opens no socket, reads no clock,
 * and allocates nothing of its own. That is what makes it testable with
 * no port -- the same discipline dbs_handle is built on -- and it is
 * also what would let this move into a library of its own without
 * unpicking anything. The one type it borrows is `dbuf`, a growable
 * byte buffer; swapping that is a mechanical change.
 *
 * WHAT SUBSET, AND WHY THAT ONE
 *
 * Enough for a database server to be spoken to, and no more:
 *
 *   - the request line, the headers, and a body measured by
 *     Content-Length;
 *   - keep-alive, which is the default in HTTP/1.1 and which every
 *     client of this will use, because a database connection that paid
 *     a handshake per query would be a joke;
 *   - one response writer.
 *
 * Deliberately absent: chunked request bodies (a client that must
 * announce its length is a client that cannot make the server guess),
 * pipelining (the server answers one request per connection at a time,
 * which is exactly keep-alive's shape), trailers, upgrades, and
 * continuation lines (obsolete since RFC 7230).
 *
 * INCREMENTAL, BECAUSE BYTES ARRIVE IN PIECES
 *
 * http_parse answers one of three things -- complete, need more, or
 * malformed -- against whatever has arrived so far, exactly like the
 * frame reader it replaces. A caller reads into a growing buffer and
 * asks again; it never has to know how far it got.
 *
 * NO COPIES. Every span the parser hands back points into the caller's
 * buffer. They are valid until that buffer is reused, which is the same
 * contract the request grammar has with the bytes it decodes.
 */
#ifndef SERVER_HTTP_H
#define SERVER_HTTP_H

#include <stddef.h>
#include <stdint.h>

#include "dbuf.h"

#ifdef __cplusplus
extern "C" {
#endif

/* What http_parse decided about the bytes it was given. */
#define HTTP_OK        1   /* a whole request is there; *total says how long */
#define HTTP_PARTIAL   0   /* nothing wrong yet -- read more and ask again */
#define HTTP_BAD     (-1)  /* not HTTP, or HTTP this subset will not accept */

/* Bounds. A request whose head or headers exceed these is HTTP_BAD
 * rather than an allocation: a parser that grows to whatever a peer
 * claims has a failure mode nobody tests. The BODY is not bounded here
 * -- how large a body is acceptable is the caller's policy, and it is
 * the caller that has to hold it. */
#define HTTP_MAX_HEAD    (16u * 1024u)
#define HTTP_MAX_HEADERS 32

typedef struct {
    const char *name;  size_t name_len;
    const char *value; size_t value_len;
} http_header;

typedef struct {
    const char *method; size_t method_len;
    /* The request target as it arrived, and the two halves of it. A
     * target of "/watch?coll=notes" has path "/watch" and query
     * "coll=notes"; neither is unescaped, because what %-escaping means
     * is the caller's business and this file does not have an opinion
     * about it. */
    const char *target; size_t target_len;
    const char *path;   size_t path_len;
    const char *query;  size_t query_len;

    http_header headers[HTTP_MAX_HEADERS];
    int         n_headers;

    const uint8_t *body; size_t body_len;

    /* HTTP/1.1 keeps the connection open unless told otherwise; HTTP/1.0
     * closes it unless told otherwise. Both rules are applied here so a
     * caller only has to read the answer. */
    int keep_alive;
} http_request;

/*
 * Is there a whole request in `buf`?
 *
 * HTTP_OK: `out` is filled and *total is the request's total length, so
 * the caller can consume exactly that many bytes and leave the rest of a
 * pipelined read alone.
 * HTTP_PARTIAL: *total is untouched; read more.
 * HTTP_BAD: the caller owes a 400 and should close.
 */
int http_parse(const uint8_t *buf, size_t len, http_request *out, size_t *total);

/*
 * A header's value, matched case-insensitively as HTTP requires. Returns
 * 1 and fills *value / *value_len when present, 0 when not. The last
 * occurrence wins, which is what a caller means by "the" value; a
 * request that repeats a header it should not is a request that gets one
 * answer rather than an argument.
 */
int http_header_get(const http_request *r, const char *name,
                    const char **value, size_t *value_len);

/*
 * Append a complete response to `out`: status line, Content-Length,
 * Content-Type, the connection disposition, and the body.
 *
 * `content_type` may be NULL for a response with no body (the header is
 * then omitted rather than sent empty). `body` may be NULL with
 * body_len 0. `keep_alive` should be the request's, unless the caller
 * has decided to close regardless -- a 400 on an unparseable request,
 * for instance, where there is no way to know where the next one starts.
 */
int http_respond(dbuf *out, int status, const char *content_type,
                 const uint8_t *body, size_t body_len, int keep_alive);

/*
 * The head of a streaming response -- status, Content-Type, no
 * Content-Length, and a connection that stays open. What follows is
 * written by the caller, in whatever shape that content type means
 * (text/event-stream, for the one this exists for).
 *
 * A streamed response cannot be pipelined behind: it ends when the
 * connection does. That is the trade, and it is the one every SSE
 * endpoint makes.
 */
int http_respond_stream(dbuf *out, int status, const char *content_type);

/* The reason phrase for a status, or "Unknown" -- so a caller that has
 * a number does not also need a table. */
const char *http_reason(int status);

#ifdef __cplusplus
}
#endif

#endif /* SERVER_HTTP_H */
