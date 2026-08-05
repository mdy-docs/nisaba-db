/*
 * regex.c — see regex.h. A thin adapter: UTF-8<->UTF-16 conversion plus a
 * bounded compile cache around third_party/regex-engine's own WASM-shim
 * API (regex_wasm.h) -- db_query.c's `$regex` needs boolean "did it match
 * anywhere" only, never capture groups, so this never touches
 * regex_captures_ptr()/regex_group_count().
 *
 * The compile cache has per-thread storage on any target where more than
 * one thread is possible (see REGEX_CACHE_TLS below), so a native server
 * handling concurrent requests gets a cache per thread rather than a data
 * race. On the single-threaded browser build it stays a plain global at
 * zero cost, which is what it always was.
 */
#include "regex.h"
#include "binjson.h"
#include "regexp.h"
#include "regex_wasm.h"

#include <stdlib.h>
#include <string.h>

/* ---- UTF-8 -> UTF-16 -------------------------------------------------- */

/*
 * Decodes one codepoint from s[0..len) (len > 0). Always consumes at least
 * one byte and writes *cp, so a caller's `pos += consumed` loop always
 * makes progress. Invalid/truncated/overlong sequences and encoded
 * surrogate halves (never legal in UTF-8) decode as a single U+FFFD byte
 * rather than erroring -- hostile/malformed input degrades the match
 * result, it never reads out of bounds or aborts the query.
 */
static int utf8_decode(const uint8_t *s, size_t len, uint32_t *cp) {
    uint8_t b0 = s[0];
    if (b0 < 0x80) { *cp = b0; return 1; }
    if ((b0 & 0xE0) == 0xC0) {
        if (len < 2 || (s[1] & 0xC0) != 0x80) { *cp = 0xFFFD; return 1; }
        uint32_t c = ((uint32_t)(b0 & 0x1F) << 6) | (uint32_t)(s[1] & 0x3F);
        if (c < 0x80) { *cp = 0xFFFD; return 1; } /* overlong */
        *cp = c; return 2;
    }
    if ((b0 & 0xF0) == 0xE0) {
        if (len < 3 || (s[1] & 0xC0) != 0x80 || (s[2] & 0xC0) != 0x80) { *cp = 0xFFFD; return 1; }
        uint32_t c = ((uint32_t)(b0 & 0x0F) << 12) | ((uint32_t)(s[1] & 0x3F) << 6) | (uint32_t)(s[2] & 0x3F);
        if (c < 0x800 || (c >= 0xD800 && c <= 0xDFFF)) { *cp = 0xFFFD; return 1; } /* overlong, or a surrogate half */
        *cp = c; return 3;
    }
    if ((b0 & 0xF8) == 0xF0) {
        if (len < 4 || (s[1] & 0xC0) != 0x80 || (s[2] & 0xC0) != 0x80 || (s[3] & 0xC0) != 0x80) { *cp = 0xFFFD; return 1; }
        uint32_t c = ((uint32_t)(b0 & 0x07) << 18) | ((uint32_t)(s[1] & 0x3F) << 12) |
                     ((uint32_t)(s[2] & 0x3F) << 6) | (uint32_t)(s[3] & 0x3F);
        if (c < 0x10000 || c > 0x10FFFF) { *cp = 0xFFFD; return 1; } /* overlong or out of Unicode range */
        *cp = c; return 4;
    }
    *cp = 0xFFFD; return 1; /* stray continuation byte or 0xF8-0xFF: never a valid lead byte */
}

/* Appends one codepoint's UTF-16 encoding (a surrogate pair above U+FFFF) to a growable buffer. */
static int utf16_append(uint16_t **buf, size_t *ulen, size_t *ucap, uint32_t cp) {
    size_t need = (cp > 0xFFFF) ? 2 : 1;
    if (*ulen + need > *ucap) {
        size_t ncap = (*ucap == 0) ? 64 : *ucap * 2;
        while (ncap < *ulen + need) ncap *= 2;
        uint16_t *nb = (uint16_t *)realloc(*buf, ncap * sizeof(uint16_t));
        if (!nb) return BJ_ERR_OOM;
        *buf = nb; *ucap = ncap;
    }
    if (cp > 0xFFFF) {
        cp -= 0x10000;
        (*buf)[(*ulen)++] = (uint16_t)(0xD800 + (cp >> 10));
        (*buf)[(*ulen)++] = (uint16_t)(0xDC00 + (cp & 0x3FF));
    } else {
        (*buf)[(*ulen)++] = (uint16_t)cp;
    }
    return BJ_OK;
}

/*
 * Converts a UTF-8 byte string to a freshly malloc'd UTF-16 buffer.
 * `nul_terminate` appends a trailing 0 code unit (compile_into requires
 * this for the pattern -- see regex_wasm.c's own comment; regex_exec does
 * not need it, since it takes an explicit unit count). *out_units excludes
 * that terminator. The returned buffer is never NULL, even for a
 * zero-length or fully-empty-after-decoding input, so regex_exec's `!text`
 * guard never misfires into treating a legitimate empty subject as
 * "no text at all".
 */
static int utf8_to_utf16(const uint8_t *s, size_t len, int nul_terminate,
                         uint16_t **out_buf, size_t *out_units) {
    uint16_t *buf = (uint16_t *)malloc(sizeof(uint16_t));
    if (!buf) return BJ_ERR_OOM;
    size_t ulen = 0, ucap = 1;
    size_t pos = 0;
    int e = BJ_OK;
    while (pos < len && !e) {
        uint32_t cp;
        pos += (size_t)utf8_decode(s + pos, len - pos, &cp);
        e = utf16_append(&buf, &ulen, &ucap, cp);
    }
    if (!e && nul_terminate) e = utf16_append(&buf, &ulen, &ucap, 0);
    if (e) { free(buf); return e; }
    if (nul_terminate) ulen--; /* keep the NUL in the buffer, just not in the reported unit count */
    *out_buf = buf;
    *out_units = ulen;
    return BJ_OK;
}

/* ---- Compiled-pattern cache -------------------------------------------
 *
 * db_query.c calls rx_match once per candidate value per document, and
 * compiling a pattern means lexing, parsing and code-generating it.
 * Redoing that on every one of those calls during a collection scan would
 * make $regex queries catastrophically slow. Caching a handful of compiled
 * patterns (keyed on the exact pattern bytes + flags) means only the
 * *first* document matched against a new pattern pays the compile cost --
 * every later call against the same pattern is just regex_exec.
 *
 * This note used to add that a Program was ~2 MB whatever the pattern, so
 * the cache was also avoiding a large allocation per document, and put the
 * capacity's ceiling at ~16 MB. Engine v0.3.0 moved the opcode and class
 * tables onto the heap and sized them to the pattern: sizeof(Program) is
 * 19,488 bytes now, so the ceiling is ~156 KB and the memory argument is
 * gone. The compute argument is not, and it was always the stronger one.
 * Still bounded rather than "cache everything forever", so a workload
 * cycling through many distinct patterns has a fixed ceiling.
 */
#define REGEX_CACHE_CAPACITY 8

typedef struct {
    uint8_t *pattern; /* owned copy of the original UTF-8 pattern bytes -- the cache key, alongside flags */
    uint32_t pattern_len;
    int flags;
    uintptr_t handle; /* regex_wasm.h handle; 0 = unused slot */
    unsigned long last_used;
} regex_cache_entry;

/*
 * Per-thread cache storage where threads exist.
 *
 * The cache holds compiled-pattern handles owned by regex-engine, and its
 * LRU clock is mutated on every lookup -- so two threads sharing it would
 * race on both the entries and the clock, and could free a handle another
 * thread is matching against. A lock would serialize every $regex
 * evaluation in the process; giving each thread its own small cache costs
 * nothing and races on nothing.
 *
 * The browser build is single-threaded by construction (JS drives one
 * WASM instance), so there it resolves to plain statics and the generated
 * code is unchanged.
 */
#if defined(__EMSCRIPTEN__) && !defined(__EMSCRIPTEN_PTHREADS__)
#define REGEX_CACHE_TLS   /* single-threaded: plain statics */
#else
#define REGEX_CACHE_TLS _Thread_local
#endif

static REGEX_CACHE_TLS regex_cache_entry g_regex_cache[REGEX_CACHE_CAPACITY];
static REGEX_CACHE_TLS unsigned long g_regex_cache_clock = 0;

static uintptr_t regex_cache_lookup(const uint8_t *pattern, uint32_t pattern_len, int flags) {
    for (int i = 0; i < REGEX_CACHE_CAPACITY; i++) {
        regex_cache_entry *e = &g_regex_cache[i];
        if (e->handle && e->flags == flags && e->pattern_len == pattern_len &&
            memcmp(e->pattern, pattern, pattern_len) == 0) {
            e->last_used = ++g_regex_cache_clock;
            return e->handle;
        }
    }
    return 0;
}

/* Takes ownership of `handle` (frees it on OOM, or on evicting it later). */
static int regex_cache_insert(const uint8_t *pattern, uint32_t pattern_len, int flags, uintptr_t handle) {
    uint8_t *pcopy = (uint8_t *)malloc(pattern_len ? pattern_len : 1);
    if (!pcopy) { regex_free(handle); return BJ_ERR_OOM; }
    if (pattern_len) memcpy(pcopy, pattern, pattern_len);

    int victim = -1;
    for (int i = 0; i < REGEX_CACHE_CAPACITY; i++) {
        if (!g_regex_cache[i].handle) { victim = i; break; }
    }
    if (victim < 0) {
        victim = 0;
        for (int i = 1; i < REGEX_CACHE_CAPACITY; i++) {
            if (g_regex_cache[i].last_used < g_regex_cache[victim].last_used) victim = i;
        }
        regex_free(g_regex_cache[victim].handle);
        free(g_regex_cache[victim].pattern);
    }
    g_regex_cache[victim].pattern = pcopy;
    g_regex_cache[victim].pattern_len = pattern_len;
    g_regex_cache[victim].flags = flags;
    g_regex_cache[victim].handle = handle;
    g_regex_cache[victim].last_used = ++g_regex_cache_clock;
    return BJ_OK;
}

/* ---- rx_match ----------------------------------------------------------- */

int rx_match(const char *pattern, int pattern_len, int ignorecase,
             const char *subject, int subject_len, int *out_matches) {
    *out_matches = 0;
    if (pattern_len < 0 || subject_len < 0) return BJ_ERR_STATE;

    int flags = ignorecase ? REGEX_FLAG_IGNORECASE : 0;
    uintptr_t handle = regex_cache_lookup((const uint8_t *)pattern, (uint32_t)pattern_len, flags);
    if (!handle) {
        uint16_t *pat16; size_t pat16_units;
        int e = utf8_to_utf16((const uint8_t *)pattern, (size_t)pattern_len, 1, &pat16, &pat16_units);
        if (e) return e;
        handle = regex_compile(pat16, (int)pat16_units, flags);
        free(pat16);
        if (!handle) return BJ_ERR_STATE; /* invalid pattern syntax; regex_last_error() has detail this boolean API doesn't surface */
        int e2 = regex_cache_insert((const uint8_t *)pattern, (uint32_t)pattern_len, flags, handle);
        if (e2) return e2; /* regex_cache_insert already freed handle on this path */
    }

    uint16_t *subj16; size_t subj16_units;
    int e = utf8_to_utf16((const uint8_t *)subject, (size_t)subject_len, 0, &subj16, &subj16_units);
    if (e) return e;
    int matched = regex_exec(handle, subj16, (int)subj16_units, 0);
    free(subj16);

    *out_matches = matched ? 1 : 0;
    return BJ_OK;
}
