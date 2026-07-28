/**
 * Types for `nisaba/wal` (src/db-wal.js) -- the single-node write-ahead-
 * logged database: replication roadmap step 2. Every write is logged and
 * durable before it applies; recovery replays the committed suffix each
 * collection hasn't seen. WalDb/WalCollection mirror Db/Collection's
 * public surface, minus dropCollection (refused until log compaction,
 * roadmap step 3).
 */
import type { Db, Collection, Document } from './nisaba.js';

export declare class WalCollection<T extends Document = Document> {
  readonly name: string;
  /** Everything Collection exposes, routed through the log where it writes. */
  [key: string]: any;
}

/** The adopted snapshot's descriptor (SnapshotStore.latest). */
export interface SnapshotInfo {
  gen: number;
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  config: { live: Array<{ role: string; name: string }> };
  files: Array<{ role: string; name: string; size: number; crc: number }>;
}

export declare class WalDb {
  isOpen: boolean;
  /** The underlying EntryLog (see `nisaba/wasm`'s EntryLog). Re-read after
   * snapshot(): the compacted log is a new object. */
  readonly log: any;
  /** The SnapshotStore, or null if the provider lacks listFiles(). */
  readonly snapshots: any;
  collection<T extends Document = Document>(name: string): Promise<WalCollection<T>>;
  listCollections(): Promise<string[]>;
  /** Unlogged drop followed by a snapshot -- the log-compaction barrier
   * that stops old entries resurrecting the collection on replay. */
  dropCollection(name: string): Promise<boolean>;
  /** Stream the database into a new immutable snapshot generation and
   * compact the log through its boundary. Host-driven, like compact(). */
  snapshot(): Promise<SnapshotInfo>;
  compact(options?: { minBytes?: number; factor?: number; skipBusy?: boolean }): Promise<Record<string, object | null>>;
  close(): Promise<void>;
}

/** The log's reserved file name within the provider ("__wal__.bj") --
 * only until the first snapshot, whose paired log supersedes it. */
export declare const WAL_FILE: string;

export declare function connectWal(
  provider: any,
  options?: {
    order?: number;
    autoCompact?: { minBytes?: number; factor?: number } | null;
    snapshotPrefix?: string;
  }
): Promise<WalDb>;

/** Put the live database files exactly at the adopted snapshot; a
 * following connectWal() replays any log suffix beyond its boundary. */
export declare function restoreLatestSnapshot(
  provider: any,
  options?: { snapshotPrefix?: string }
): Promise<SnapshotInfo>;
