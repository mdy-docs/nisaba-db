/*
 * http.c — see http.h.
 *
 * Every function here reads bytes the caller owns and writes bytes the
 * caller owns. There is no state between calls, which is why a request
 * that arrives in forty pieces costs nothing to reparse: forty times
 * over a few hundred bytes is not a cost, and a resumable parser with a
 * saved position is a thing that can be wrong.
 */
#include "http.h"

#include <stdio.h>
#include <string.h>

/* ---- small helpers ------------------------------------------------------ */

static int ci_equal(const char *a, size_t a_len, const char *b) {
    size_t b_len = strlen(b);
    if (a_len != b_len) return 0;
    for (size_t i = 0; i < a_len; i++) {
        char x = a[i], y = b[i];
        if (x >= 'A' && x <= 'Z') x = (char)(x - 'A' + 'a');
        if (y >= 'A' && y <= 'Z') y = (char)(y - 'A' + 'a');
        if (x != y) return 0;
    }
    return 1;
}

/* Whitespace HTTP allows around a header value: spaces and tabs only.
 * Not \r or \n -- those delimit, and treating them as space is how a
 * parser ends up believing in a header that spans a line it should not. */
static int is_ows(char c) { return c == ' ' || c == '\t'; }

static void trim(const char **s, size_t *len) {
    while (*len && is_ows(**s)) { (*s)++; (*len)--; }
    while (*len && is_ows((*s)[*len - 1])) (*len)--;
}

/* A non-negative decimal, with no room for cleverness: no sign, no
 * whitespace, no leading +, and an overflow is a refusal rather than a
 * wrap. Content-Length is a number a peer chose; it does not get to
 * choose what it means. */
static int parse_u64(const char *s, size_t len, uint64_t *out) {
    if (len == 0 || len > 20) return -1;
    uint64_t n = 0;
    for (size_t i = 0; i < len; i++) {
        if (s[i] < '0' || s[i] > '9') return -1;
        if (n > (UINT64_MAX - (uint64_t)(s[i] - '0')) / 10u) return -1;
        n = n * 10u + (uint64_t)(s[i] - '0');
    }
    *out = n;
    return 0;
}

/* The end of a line, as a CRLF. A bare LF is tolerated on the way in --
 * curl and telnet both produce it, and every real server accepts it --
 * but nothing here ever WRITES one. *skip is how many bytes the
 * terminator occupied. Returns the offset of the terminator, or -1 if
 * the line has not arrived yet. */
static long find_eol(const uint8_t *buf, size_t len, size_t from, size_t *skip) {
    for (size_t i = from; i < len; i++) {
        if (buf[i] == '\n') {
            if (i > from && buf[i - 1] == '\r') { *skip = 2; return (long)(i - 1); }
            *skip = 1;
            return (long)i;
        }
    }
    return -1;
}

/* ---- parsing ------------------------------------------------------------ */

int http_parse(const uint8_t *buf, size_t len, http_request *out, size_t *total) {
    if (!buf || !out || !total) return HTTP_BAD;
    memset(out, 0, sizeof *out);

    const char *s = (const char *)buf;

    /* ---- the request line: METHOD SP TARGET SP VERSION */
    size_t skip = 0;
    long eol = find_eol(buf, len, 0, &skip);
    if (eol < 0) return len > HTTP_MAX_HEAD ? HTTP_BAD : HTTP_PARTIAL;

    size_t line_len = (size_t)eol;
    const char *sp1 = memchr(s, ' ', line_len);
    if (!sp1) return HTTP_BAD;
    out->method = s;
    out->method_len = (size_t)(sp1 - s);
    if (out->method_len == 0) return HTTP_BAD;

    const char *rest = sp1 + 1;
    size_t rest_len = line_len - out->method_len - 1;
    const char *sp2 = memchr(rest, ' ', rest_len);
    if (!sp2) return HTTP_BAD;              /* HTTP/0.9 is not a thing here */
    out->target = rest;
    out->target_len = (size_t)(sp2 - rest);
    if (out->target_len == 0) return HTTP_BAD;

    const char *version = sp2 + 1;
    size_t version_len = line_len - out->method_len - 1 - out->target_len - 1;
    int http_11;
    if (version_len == 8 && memcmp(version, "HTTP/1.1", 8) == 0) http_11 = 1;
    else if (version_len == 8 && memcmp(version, "HTTP/1.0", 8) == 0) http_11 = 0;
    else return HTTP_BAD;

    /* The target's two halves. A '?' with nothing after it is an empty
     * query, not the absence of one -- but nothing here can tell those
     * apart and nothing needs to. */
    const char *q = memchr(out->target, '?', out->target_len);
    out->path = out->target;
    out->path_len = q ? (size_t)(q - out->target) : out->target_len;
    out->query = q ? q + 1 : NULL;
    out->query_len = q ? out->target_len - out->path_len - 1 : 0;

    /* ---- headers, until a blank line */
    size_t at = (size_t)eol + skip;
    uint64_t content_length = 0;
    int have_length = 0;
    out->keep_alive = http_11;

    for (;;) {
        if (at > HTTP_MAX_HEAD) return HTTP_BAD;
        eol = find_eol(buf, len, at, &skip);
        if (eol < 0) return len > HTTP_MAX_HEAD ? HTTP_BAD : HTTP_PARTIAL;
        size_t hlen = (size_t)eol - at;
        if (hlen == 0) { at = (size_t)eol + skip; break; }   /* the blank line */

        const char *h = (const char *)buf + at;
        const char *colon = memchr(h, ':', hlen);
        if (!colon) return HTTP_BAD;
        /* No space before the colon: RFC 7230 says a server must reject
         * it, because a proxy that tolerated it and one that did not
         * would disagree about where a header ends. */
        const char *name = h;
        size_t name_len = (size_t)(colon - h);
        if (name_len == 0 || is_ows(name[name_len - 1])) return HTTP_BAD;

        const char *value = colon + 1;
        size_t value_len = hlen - name_len - 1;
        trim(&value, &value_len);

        if (out->n_headers >= HTTP_MAX_HEADERS) return HTTP_BAD;
        out->headers[out->n_headers].name = name;
        out->headers[out->n_headers].name_len = name_len;
        out->headers[out->n_headers].value = value;
        out->headers[out->n_headers].value_len = value_len;
        out->n_headers++;

        if (ci_equal(name, name_len, "content-length")) {
            uint64_t n = 0;
            if (parse_u64(value, value_len, &n) != 0) return HTTP_BAD;
            /* Repeated with a different value is the classic smuggling
             * shape: two readers, two answers about where the body ends. */
            if (have_length && n != content_length) return HTTP_BAD;
            content_length = n;
            have_length = 1;
        } else if (ci_equal(name, name_len, "transfer-encoding")) {
            /* Not supported, and not silently ignored: a body this
             * parser cannot measure is a request it must not guess at. */
            return HTTP_BAD;
        } else if (ci_equal(name, name_len, "connection")) {
            if (ci_equal(value, value_len, "close")) out->keep_alive = 0;
            else if (ci_equal(value, value_len, "keep-alive")) out->keep_alive = 1;
        }

        at = (size_t)eol + skip;
    }

    /* ---- the body, exactly as long as it said it would be */
    if (content_length > (uint64_t)(SIZE_MAX - at)) return HTTP_BAD;
    size_t need = at + (size_t)content_length;
    if (len < need) return HTTP_PARTIAL;

    out->body = content_length ? buf + at : NULL;
    out->body_len = (size_t)content_length;
    *total = need;
    return HTTP_OK;
}

int http_header_get(const http_request *r, const char *name,
                    const char **value, size_t *value_len) {
    if (!r || !name) return 0;
    int found = 0;
    for (int i = 0; i < r->n_headers; i++) {
        if (!ci_equal(r->headers[i].name, r->headers[i].name_len, name)) continue;
        if (value) *value = r->headers[i].value;
        if (value_len) *value_len = r->headers[i].value_len;
        found = 1;                       /* last one wins */
    }
    return found;
}

/* ---- responding --------------------------------------------------------- */

const char *http_reason(int status) {
    switch (status) {
        case 200: return "OK";
        case 400: return "Bad Request";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 413: return "Content Too Large";
        case 500: return "Internal Server Error";
        case 503: return "Service Unavailable";
        default:  return "Unknown";
    }
}

static int put_str(dbuf *out, const char *s) {
    return dbuf_put(out, (const uint8_t *)s, strlen(s));
}

static int put_status(dbuf *out, int status) {
    char line[64];
    int n = snprintf(line, sizeof line, "HTTP/1.1 %d %s\r\n", status, http_reason(status));
    if (n < 0 || (size_t)n >= sizeof line) return -1;
    return dbuf_put(out, (const uint8_t *)line, (size_t)n);
}

int http_respond(dbuf *out, int status, const char *content_type,
                 const uint8_t *body, size_t body_len, int keep_alive) {
    if (!out) return -1;
    int e = put_status(out, status);
    if (!e && content_type) {
        e = put_str(out, "Content-Type: ");
        if (!e) e = put_str(out, content_type);
        if (!e) e = put_str(out, "\r\n");
    }
    if (!e) {
        /* Always sent, including zero: a response whose length a client
         * has to infer from the connection closing is a response that
         * cannot be followed by another. */
        char len[48];
        int n = snprintf(len, sizeof len, "Content-Length: %zu\r\n", body_len);
        if (n < 0 || (size_t)n >= sizeof len) return -1;
        e = dbuf_put(out, (const uint8_t *)len, (size_t)n);
    }
    if (!e) e = put_str(out, keep_alive ? "Connection: keep-alive\r\n"
                                        : "Connection: close\r\n");
    if (!e) e = put_str(out, "\r\n");
    if (!e && body && body_len) e = dbuf_put(out, body, body_len);
    return e;
}

int http_respond_stream(dbuf *out, int status, const char *content_type) {
    if (!out || !content_type) return -1;
    int e = put_status(out, status);
    if (!e) e = put_str(out, "Content-Type: ");
    if (!e) e = put_str(out, content_type);
    if (!e) e = put_str(out, "\r\n");
    /* No Content-Length, and no keep-alive to offer: this response ends
     * when the connection does. */
    if (!e) e = put_str(out, "Cache-Control: no-store\r\n");
    if (!e) e = put_str(out, "Connection: keep-alive\r\n");
    if (!e) e = put_str(out, "\r\n");
    return e;
}
