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
 * intact. manifest.bj is the member's manifest FILE, byte-identical --
 * the binjson record plus the CRC-32 trailer whose validity IS the
 * commit (snapstore.h) -- because restore puts those bytes straight
 * back and the server's own adoption validates them. The facts only
 * this side knows -- which member, which boundary, when -- ride as S3
 * object metadata, ABOUT the object and never in its bytes.
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
import fs from 'node:fs';
import path from 'node:path';
import { ServerError, decode } from './db-server-client.js';

/**
 * The canonical snapshot prefix: the C server's REPLICA_SNAP_PREFIX and
 * the JS WAL host's SNAP_PREFIX, verbatim ("One snapshot, two hosts",
 * docs/s3-backup.md). Spelled here rather than imported because
 * importing db-wal.js loads the WASM engine, and this process's whole
 * claim is that it has none; the tripwire test pins all three
 * spellings together.
 */
export const SNAP_PREFIX = '__snap__';

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

/**
 * A manifest file is binjson followed by a CRC-32 (little-endian) of
 * exactly those bytes (snapstore.h); a torn one cannot validate. Checked
 * on ship and again on restore -- transport integrity at both ends of
 * the wire whose middle is S3. Returns the decoded record.
 */
export function verifyManifestBytes(bytes) {
  if (bytes.length < 5) throw new Error('manifest file too short to hold a record and its CRC');
  const body = bytes.subarray(0, bytes.length - 4);
  const want = bytes[bytes.length - 4] | (bytes[bytes.length - 3] << 8) |
               (bytes[bytes.length - 2] << 16) | (bytes[bytes.length - 1] << 24);
  if (crc32(body) !== (want >>> 0)) {
    throw new Error('manifest file failed its CRC -- torn in transit');
  }
  return decode(body);
}

/** The generation numbers under `instance`, newest first, committed or
 * not -- callers that need committed ones probe for the manifest. */
async function generationsIn(s3, instance) {
  const { prefixes } = await s3.list(`${instance}/`, { delimiter: '/' });
  return prefixes
    .map((p) => /\/gen-(\d+)\/$/.exec(p))
    .filter(Boolean)
    .map((m) => Number(m[1]))
    .sort((a, b) => b - a);
}

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

  _generations() { return generationsIn(this.s3, this.instance); }

  /** The prefix belongs to ONE member. Checked once per run, against
   * the newest committed manifest's metadata. */
  async _guardMember() {
    if (this._memberChecked) return;
    for (const gen of await this._generations()) {
      const head = await this.s3.headObject(this._key(gen, 'manifest.bj'));
      if (!head) continue;
      const theirs = head.metadata.member;
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
    let manifestBytes;
    try {
      for (const f of manifest.files) {
        const body = await this._fetchFile(gen, f);
        await this.s3.putObject(this._key(gen, `${f.role}.bj`), body);
      }
      manifestBytes = Buffer.from(await this.client.readSnapshotManifest(gen));
    } catch (err) {
      if (err instanceof ServerError && err.code === -73) {
        this._log('superseded', { gen });
        return { shipped: false, gen, superseded: true };
      }
      throw err;
    }
    verifyManifestBytes(manifestBytes);
    await this.s3.putObject(this._key(gen, 'manifest.bj'), manifestBytes, {
      metadata: {
        member: this.member,
        gen: String(gen),
        boundary: String(boundary),
        shippedat: new Date().toISOString()
      }
    });
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

/**
 * The restore half (docs/s3-backup.md step 6): put a shipped generation
 * into an EMPTY directory, in the snapstore's own on-disk shape --
 * `__snap__-<gen>-<role>.bj` files and the manifest, byte-identical,
 * written LAST. What the operator does next is start a server on it:
 * the startup adoption (restore_if_stale) restores every live name from
 * the manifest's config.live and bases a fresh log at the boundary. The
 * restored process is a NEW cluster of one; docs/s3-backup.md says why
 * restoring beside a live cluster is refused by design, not by code.
 *
 * Every file is verified against the manifest (size and CRC) before the
 * manifest is written, so an interrupted restore leaves data files with
 * no manifest -- exactly the crashed-snapshot-attempt shape the store's
 * own startup sweep already handles ("never existed").
 */
export async function restoreFromS3({ s3, instance, into, gen = null, log = () => {} }) {
  fs.mkdirSync(into, { recursive: true });
  if (fs.readdirSync(into).length > 0) {
    throw new Error(`refusing to restore into ${into}: it is not empty, and restore never merges`);
  }

  let chosen = gen;
  if (chosen === null) {
    for (const g of await generationsIn(s3, instance)) {
      if (await s3.headObject(`${instance}/gen-${g}/manifest.bj`)) { chosen = g; break; }
    }
    if (chosen === null) throw new Error(`no committed generation under '${instance}'`);
  }

  const manifestBytes = await s3.getObject(`${instance}/gen-${chosen}/manifest.bj`);
  const manifest = verifyManifestBytes(manifestBytes);
  log('restoring', { gen: chosen, lastIncludedIndex: manifest.lastIncludedIndex, files: manifest.files.length });

  for (const f of manifest.files) {
    // The name is written into a directory: a manifest that names a
    // path is not a manifest, whatever else it is.
    if (f.name.includes('/') || f.name.includes('\\') || f.name.includes('..')) {
      throw new Error(`manifest names a path, not a file: '${f.name}'`);
    }
    const body = await s3.getObject(`${instance}/gen-${chosen}/${f.role}.bj`);
    if (body.length !== f.size) {
      throw new Error(`${f.role}: S3 holds ${body.length} bytes, the manifest says ${f.size}`);
    }
    if (crc32(body) !== f.crc) {
      throw new Error(`${f.role}: CRC mismatch against the manifest -- the stored copy is damaged`);
    }
    fs.writeFileSync(path.join(into, f.name), body);
  }

  // The commit point, byte-identical, last.
  fs.writeFileSync(path.join(into, `${SNAP_PREFIX}-${chosen}.manifest.bj`), manifestBytes);
  log('restored', { gen: chosen, into });
  return {
    gen: chosen,
    lastIncludedIndex: manifest.lastIncludedIndex,
    lastIncludedTerm: manifest.lastIncludedTerm,
    files: manifest.files.length
  };
}
