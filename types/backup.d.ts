/**
 * Types for `nisaba/backup` (src/db-backup.js) -- the S3 backup agent:
 * ships a member's committed snapshot generations to S3-compatible
 * storage, prunes by retention, and restores a generation into an
 * empty directory for a fresh server to adopt. No engine in the
 * process; docs/s3-backup.md has the story.
 *
 * The `client` an agent holds is the TCP client's handle for the ONE
 * member it backs up (connectServer in db-server-client.js); only the
 * snapshot-facing slice of it is needed, and only that slice is
 * required here.
 *
 * THE OBJECT STORE IS INJECTED, and this package no longer ships one.
 * `src/s3.js` was 554 lines of hand-rolled SigV4 that existed because
 * this package has no runtime dependencies and could not take an AWS
 * SDK; it now lives in the consumer that already talks to AWS
 * (nisaba-web's `service/s3-client.js`). Nothing was lost here — the
 * agent never constructed a client, it took one — and what is written
 * below is the whole of what it ever asked of it.
 */

/**
 * The eight methods the backup agent calls. STRUCTURAL on purpose: any
 * object satisfying this will do, which is what lets a caller supply
 * an SDK-backed client, a MinIO-backed one, or a Map in a test
 * (test/helpers/memory-s3.js).
 */
export interface ObjectStore {
  /** `delimiter` makes this "what directories are here": keys stop at
   * the delimiter and common prefixes come back separately. Paged
   * internally — a partial listing is never an answer. */
  list(prefix: string, opts?: { delimiter?: string | null; maxKeysPerPage?: number }):
    Promise<{ keys: Array<{ key: string; size: number }>; prefixes: string[] }>;
  /** null for a missing object: callers PROBE for a manifest that may
   * not be there yet, and that miss is the ordinary case. Metadata keys
   * arrive lowercased, as S3 stores them. */
  headObject(key: string): Promise<{ size: number; etag: string | null; metadata: Record<string, string> } | null>;
  getObject(key: string): Promise<Uint8Array>;
  /** Inclusive of both ends. A store that ignores the range and returns
   * the whole object must be refused, not accepted — see
   * docs/s3-backup.md. */
  getObjectRange(key: string, start: number, end: number): Promise<Uint8Array>;
  putObject(key: string, body: Uint8Array, opts?: { contentType?: string; metadata?: Record<string, string> | null }): Promise<void>;
  /** `bytes` is checked against the manifest's size by the caller, so
   * it must count what actually went, not what was promised. */
  putObjectStream(key: string, chunks: AsyncIterable<Uint8Array>, opts?: { contentType?: string; metadata?: Record<string, string> | null }):
    Promise<{ bytes: number }>;
  deleteObject(key: string): Promise<void>;
}

/** @deprecated The name the injected store used to have, kept so an
 * existing annotation still resolves. Prefer `ObjectStore`. */
export type S3Client = ObjectStore;

/** The canonical snapshot prefix -- the C server's REPLICA_SNAP_PREFIX
 * and the JS WAL host's SNAP_PREFIX, verbatim. */
export const SNAP_PREFIX: string;

/** zlib CRC-32 (bjfile's polynomial), chainable from 0. */
export function crc32(bytes: Uint8Array, crc?: number): number;

/** Validate a manifest FILE's bytes (binjson record + CRC-32 trailer)
 * and return the decoded record. Throws on a torn one. */
export function verifyManifestBytes(bytes: Uint8Array): SnapshotManifest;

/** A committed generation, as snapstore.h manifests it. */
export interface SnapshotManifest {
  gen: number;
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  config: { live: Array<{ role: string; name: string }> };
  files: Array<{ role: string; name: string; size: number; crc: number }>;
}

/** What one shipping attempt says happened. `superseded` means the
 * member committed a newer generation mid-transfer (ship again);
 * `absent` means it has no committed generation yet. */
export interface ShipResult {
  shipped: boolean;
  gen?: number;
  boundary?: number;
  superseded?: boolean;
  absent?: boolean;
}

/** The snapshot-facing slice of the TCP client the agent drives. */
export interface BackupMember {
  ping(): Promise<{ base?: number } & Record<string, unknown>>;
  snapshot(): Promise<SnapshotManifest>;
  latestSnapshot(): Promise<SnapshotManifest>;
  readSnapshotFile(gen: number, role: string, offset?: number):
    Promise<{ data: Uint8Array; eof: boolean; size: number }>;
  readSnapshotManifest(gen: number): Promise<Uint8Array>;
}

export class BackupAgent {
  constructor(options: {
    /** A connected server client for the ONE member this agent backs up. */
    client: BackupMember;
    /** An S3Client whose bucket exists. */
    s3: S3Client;
    /** The S3 key prefix for this instance. Generation numbers are
     * per-member: one member per instance prefix, enforced. */
    instance: string;
    /** The member's address, recorded in every shipped manifest's
     * metadata and enforced against the prefix. */
    member: string;
    log?: (kind: string, detail: object) => void;
  });
  /** Ship the member's committed generation, unless it is already in
   * S3. Files first, manifest last -- the commit point. */
  shipLatest(): Promise<ShipResult>;
  /** Keep the newest `keep` generations; the manifest is deleted
   * FIRST, so a half-pruned generation reads as absent, never intact.
   * Resolves with the pruned generation numbers. */
  prune(keep: number): Promise<number[]>;
  /** One backup, now: take a generation (unless told not to), ship it,
   * prune. A supersession mid-ship retries from the new latest. */
  once(options?: { takeSnapshot?: boolean; keep?: number | null }): Promise<ShipResult>;
  /** The loop: ship every generation the member commits until the
   * signal aborts. `everyMs` adds a wall-clock snapshot cadence --
   * this side has the clock; the server deliberately does not. */
  watch(options?: {
    pollMs?: number;
    everyMs?: number | null;
    keep?: number | null;
    signal?: AbortSignal;
  }): Promise<void>;
}

/** Download a generation into an EMPTY directory, in the snapstore's
 * own on-disk shape, manifest written last -- then start a server on
 * it; the startup adoption does the rest. The restored process is a
 * NEW cluster of one: never restore beside a live cluster. */
export function restoreFromS3(options: {
  s3: S3Client;
  instance: string;
  into: string;
  gen?: number | null;
  log?: (kind: string, detail: object) => void;
}): Promise<{ gen: number; lastIncludedIndex: number; lastIncludedTerm: number; files: number }>;

/** Ship a generation straight from a DIRECTORY -- the path for a
 * member with no client wire to ask: a JS-hosted instance, or a
 * stopped server's root. The live files are never touched. */
export function shipGenerationFromDir(options: {
  dir: string;
  s3: S3Client;
  instance: string;
  member: string;
  log?: (kind: string, detail: object) => void;
}): Promise<ShipResult>;
