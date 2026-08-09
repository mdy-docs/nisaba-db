/**
 * test/helpers/memory-s3.js — an object store in a Map, so the backup
 * agent can be tested without one.
 *
 * WHY THIS EXISTS AT ALL. `src/s3.js` used to live in this repository
 * and `test/db.backup.test.js` drove the backup agent through it at
 * real MinIO. That client has gone: this package has no runtime
 * dependencies and is not the right home for an AWS SDK, so the client
 * now lives in the consumer that already talks to AWS (nisaba-web's
 * `service/s3-client.js`, whose own tests cover the dialect, the retry
 * policy and the socket timeout).
 *
 * What is left here is the half that was always ours, and it is the
 * more interesting half: `src/db-backup.js` never knew anything about
 * S3 beyond eight method names. What it knows is the COMMIT RULE — the
 * generation's files go up before its manifest, so a listing without a
 * manifest never existed; pruning deletes the manifest first, so a
 * half-pruned generation reads as absent rather than intact; generation
 * numbers are per-member, so one prefix holds exactly one member; every
 * file's CRC is checked against what the engine committed as the bytes
 * stream past.
 *
 * NONE OF THAT NEEDS A WIRE, and testing it over one made it worse: the
 * suite skipped unless MinIO happened to be running, which on an
 * ordinary run meant the agent's own rules were tested exactly never.
 * They are tested on every run now, and the things that genuinely
 * require a real store — signing, paging, multipart, 503s — are tested
 * where the real client is.
 *
 * Faithful where fidelity changes an ANSWER: keys are opaque strings,
 * metadata keys are lowercased as S3 lowercases them, a missing object
 * heads as null rather than throwing, ranges are inclusive of both
 * ends, and `delimiter` returns common prefixes rather than keys.
 * Unfaithful where it does not: no signing, no paging, no multipart,
 * everything held whole in memory — which is the very thing the
 * streaming path exists to avoid, and is fine for a few kilobytes of
 * test fixture.
 */

/** @returns an object satisfying the eight methods `src/db-backup.js` calls. */
export function memoryS3() {
  /** key -> { body: Buffer, metadata: object } */
  const store = new Map();

  return {
    /** For assertions: the raw contents, so a test can say what is and
     * is not there without going through the reads under test. */
    _store: store,
    _keys: () => [...store.keys()].sort(),

    async list(prefix, { delimiter = null } = {}) {
      const keys = [];
      const prefixes = new Set();
      for (const key of [...store.keys()].sort()) {
        if (prefix && !key.startsWith(prefix)) continue;
        if (delimiter) {
          const rest = key.slice(prefix.length);
          const cut = rest.indexOf(delimiter);
          if (cut >= 0) {
            // A common prefix, not a key -- this is what "what
            // generations are here" is asking, and returning the keys
            // instead would make it a listing of every file in each.
            prefixes.add(prefix + rest.slice(0, cut + delimiter.length));
            continue;
          }
        }
        keys.push({ key, size: store.get(key).body.length });
      }
      return { keys, prefixes: [...prefixes] };
    },

    async headObject(key) {
      const row = store.get(key);
      // Absence is an ANSWER: `shipLatest` heads the manifest to decide
      // whether this generation is already shipped, and that miss is
      // the ordinary case rather than an error.
      if (!row) return null;
      return { size: row.body.length, etag: '"mem"', metadata: { ...row.metadata } };
    },

    async getObject(key) {
      const row = store.get(key);
      if (!row) {
        const err = new Error(`NoSuchKey: ${key}`);
        err.name = 'NoSuchKey';
        err.status = 404;
        throw err;
      }
      return Buffer.from(row.body);
    },

    async getObjectRange(key, start, end) {
      const row = store.get(key);
      if (!row) {
        const err = new Error(`NoSuchKey: ${key}`);
        err.name = 'NoSuchKey';
        err.status = 404;
        throw err;
      }
      return Buffer.from(row.body.subarray(start, end + 1));   // inclusive
    },

    async putObject(key, body, { metadata = null } = {}) {
      store.set(key, { body: Buffer.from(body), metadata: lower(metadata) });
    },

    async putObjectStream(key, chunks, { metadata = null } = {}) {
      const parts = [];
      let bytes = 0;
      for await (const chunk of chunks) {
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        bytes += buf.length;
        parts.push(buf);
      }
      store.set(key, { body: Buffer.concat(parts), metadata: lower(metadata) });
      // Counted here, from the bytes that actually arrived — which is
      // what `uploadGeneration` compares to the manifest's size.
      return { bytes };
    },

    async deleteObject(key) {
      store.delete(key);
    }
  };
}

/** S3 lowercases metadata keys, and `guardMember` reads
 * `head.metadata.member`. A double that preserved case would let a
 * mixed-case write pass here and fail against the real thing. */
function lower(metadata) {
  if (!metadata) return {};
  return Object.fromEntries(Object.entries(metadata).map(([k, v]) => [k.toLowerCase(), String(v)]));
}
