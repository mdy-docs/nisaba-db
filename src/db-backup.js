/**
 * db-backup.js — the S3 backup agent (docs/s3-backup.md step 5): a Node
 * process beside the cluster that ships a member's committed snapshot
 * generation to S3-compatible object storage. NO ENGINE IN THIS
 * PROCESS: its imports are the server client and the S3 client, and the
 * artifact it moves is the one the Raft machinery already produces —
 * this agent invents nothing about what a consistent copy is.
 *
 * ONE MEMBER, EXPLICIT (`--target`). The leader gives the freshest
 * boundary; a follower offloads the read I/O and is at worst behind,
 * never inconsistent — a follower's generation is a true prefix of
 * history. The agent does not chase leadership: staleness here is a
 * cadence question, not a correctness one. Backing up "the cluster" is
 * backing up one member, and the S3 prefix records WHICH one — a run
 * pointed at a different member than the prefix holds is stopped,
 * because generation numbers are per-member and interleaving two
 * members' generations under one prefix would make the numbering lie.
 *
 * THE S3 LAYOUT COPIES THE DISK'S COMMIT RULE:
 *
 *     <instance>/gen-<N>/<role>.bj        the generation's files
 *     <instance>/gen-<N>/manifest.bj      uploaded LAST — the commit point
 *
 * A listing without manifest.bj never existed; pruning deletes the
 * manifest FIRST, so a half-pruned generation reads as absent, never as
 * intact. The manifest stored is the server's, plus the facts only this
 * side knows: which member it came from, and when it was shipped.
 *
 * INTEGRITY IS CHECKED IN TRANSIT, against the manifest the server
 * answered: every file's bytes are CRC-32'd as the chunks stream and
 * compared to the manifest's `crc` before the file is uploaded. The
 * polynomial is bjfile's (0xEDB88320, the zlib one) — this is a
 * transport check on top of the store's own verify rule, not a fourth
 * opinion about what makes a snapshot intact; restore ends by letting
 * the server's own adoption verify (sst_check_files).
 *
 * A GENERATION PRUNED MID-TRANSFER (the member committed a newer one)
 * refuses further reads with -73; the agent restarts from
 * latestSnapshot — a retry, not a pin (docs/s3-backup.md says why).
 */
import { ServerError, encode, decode } from './db-server-client.js';

/* bjfile_crc32's twin: zlib CRC-32, chainable from 0. */
const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    t[i] = c;
  }
  return t;
})();
export function crc32(bytes, crc = 0) {
  let c = (~crc) >>> 0;
  for (let i = 0; i < bytes.length; i++) c = CRC_TABLE[(c ^ bytes[i]) & 0xff] ^ (c >>> 8);
  return (~c) >>> 0;
}

const sleep = (ms, signal) => new Promise((resolve) => {
  const t = setTimeout(resolve, ms);
  signal?.addEventListener('abort', () => { clearTimeout(t); resolve(); }, { once: true });
});

export class BackupAgent {
  /**
   * @param {object} options
   * @param {object} options.client - a connected server client
   *   (connectServer) for the ONE member this agent backs up
   * @param {object} options.s3 - an S3Client whose bucket exists
   * @param {string} options.instance - the S3 key prefix for this
   *   instance (operator-chosen; nothing on the wire names an instance)
   * @param {string} options.member - the member's address, recorded in
   *   every shipped manifest and enforced against the prefix
   * @param {(kind: string, detail: object) => void} [options.log]
   */
  constructor({ client, s3, instance, member, log = () => {} }) {
    this.client = client;
    this.s3 = s3;
    this.instance = instance;
    this.member = member;
    this._log = log;
    this._memberChecked = false;
    this._lastShipped = 0;   // boundary of the last generation known shipped
  }

  _key(gen, name) { return `${this.instance}/gen-${gen}/${name}`; }

  /** The generation numbers present under this instance prefix,
   * committed (manifest present) or not, newest first. */
  async _generations() {
    const { prefixes } = await this.s3.list(`${this.instance}/`, { delimiter: '/' });
    return prefixes
      .map((p) => /\/gen-(\d+)\/$/.exec(p))
      .filter(Boolean)
      .map((m) => Number(m[1]))
      .sort((a, b) => b - a);
  }

  /** The prefix belongs to ONE member. Checked once per run, against
   * the newest committed manifest already there. */
  async _guardMember() {
    if (this._memberChecked) return;
    for (const gen of await this._generations()) {
      let body = null;
      try { body = await this.s3.getObject(this._key(gen, 'manifest.bj')); } catch { continue; }
      const theirs = decode(body).member;
      if (theirs && theirs !== this.member) {
        throw new Error(
          `s3 prefix '${this.instance}' holds generations from member ${theirs}; ` +
          `this agent targets ${this.member}. Generation numbers are per-member -- ` +
          'give each member its own --instance prefix'
        );
      }
      break;   // the newest committed manifest is the authority
    }
    this._memberChecked = true;
  }

  /** Pull one generation file whole, chunk by chunk, verifying size and
   * CRC against the manifest entry before anything is uploaded. */
  async _fetchFile(gen, f) {
    const chunks = [];
    let offset = 0;
    let crc = 0;
    for (;;) {
      const { data, eof, size } = await this.client.readSnapshotFile(gen, f.role, offset);
      const chunk = Buffer.from(data.buffer ?? data, data.byteOffset ?? 0, data.byteLength ?? data.length);
      crc = crc32(chunk, crc);
      chunks.push(chunk);
      offset += chunk.length;
      if (eof) {
        if (offset !== f.size || size !== f.size) {
          throw new Error(`generation file ${f.role}: server sent ${offset} bytes, manifest says ${f.size}`);
        }
        break;
      }
    }
    if (crc !== f.crc) {
      throw new Error(`generation file ${f.role}: CRC mismatch in transit (got ${crc}, manifest says ${f.crc})`);
    }
    return Buffer.concat(chunks);
  }

  /**
   * Ship the member's committed generation, unless it is already in S3.
   * Files first, manifest last; `{ shipped, gen, boundary }`, with
   * `superseded: true` when the member committed a newer generation
   * mid-transfer (call again), and `absent: true` when the member has
   * no committed generation yet.
   */
  async shipLatest() {
    await this._guardMember();
    let manifest;
    try {
      manifest = await this.client.latestSnapshot();
    } catch (err) {
      if (err instanceof ServerError && err.code === -73) return { shipped: false, absent: true };
      throw err;
    }
    const { gen, lastIncludedIndex: boundary } = manifest;
    if (await this.s3.headObject(this._key(gen, 'manifest.bj'))) {
      this._lastShipped = boundary;
      return { shipped: false, gen, boundary };
    }

    this._log('shipping', { gen, boundary, files: manifest.files.length });
    try {
      for (const f of manifest.files) {
        const body = await this._fetchFile(gen, f);
        await this.s3.putObject(this._key(gen, `${f.role}.bj`), body);
      }
    } catch (err) {
      if (err instanceof ServerError && err.code === -73) {
        this._log('superseded', { gen });
        return { shipped: false, gen, superseded: true };
      }
      throw err;
    }
    await this.s3.putObject(this._key(gen, 'manifest.bj'), Buffer.from(encode({
      ...manifest,
      member: this.member,
      instance: this.instance,
      shippedAt: new Date()
    })));
    this._lastShipped = boundary;
    this._log('shipped', { gen, boundary });
    return { shipped: true, gen, boundary };
  }

  /** Keep the newest `keep` generations; manifest deleted FIRST, so a
   * half-pruned generation reads as absent rather than intact. */
  async prune(keep) {
    const doomed = (await this._generations()).slice(Math.max(1, keep));
    for (const gen of doomed) {
      await this.s3.deleteObject(this._key(gen, 'manifest.bj'));
      const { keys } = await this.s3.list(`${this.instance}/gen-${gen}/`);
      for (const { key } of keys) await this.s3.deleteObject(key);
      this._log('pruned', { gen });
    }
    return doomed;
  }

  /**
   * One backup, now: take a generation (unless told not to — `snapshot`
   * is idempotent at an unchanged boundary, so this is cheap), ship it,
   * prune. A supersession mid-ship retries from the new latest.
   */
  async once({ takeSnapshot = true, keep = null } = {}) {
    if (takeSnapshot) await this.client.snapshot();
    let result;
    for (let attempt = 0; ; attempt++) {
      result = await this.shipLatest();
      if (!result.superseded) break;
      if (attempt >= 4) throw new Error('generation superseded 5 times in a row; the member is snapshotting faster than this agent can ship');
    }
    if (keep !== null) await this.prune(keep);
    return result;
  }

  /**
   * The loop: watch the member's `base` (it moves when a generation
   * commits — the entries-driven cadence), ship what appears, prune by
   * retention. `everyMs` adds a wall-clock cadence on top: this side
   * has the clock, the server deliberately does not
   * (docs/s3-backup.md). Transient errors are logged and the loop
   * continues; it ends only with the signal.
   */
  async watch({ pollMs = 5000, everyMs = null, keep = null, signal } = {}) {
    let lastTake = Date.now();
    while (!signal?.aborted) {
      try {
        if (everyMs !== null && Date.now() - lastTake >= everyMs) {
          await this.client.snapshot();
          lastTake = Date.now();
        }
        const { base } = await this.client.ping();
        if (base > 0 && base !== this._lastShipped) {
          const r = await this.shipLatest();
          if (r.shipped && keep !== null) await this.prune(keep);
        }
      } catch (err) {
        this._log('error', { message: err.message, code: err.code });
      }
      await sleep(pollMs, signal);
    }
  }
}
