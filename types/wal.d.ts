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

export declare class WalDb {
  isOpen: boolean;
  /** The underlying EntryLog (see `nisaba/wasm`'s EntryLog). */
  readonly log: any;
  collection<T extends Document = Document>(name: string): Promise<WalCollection<T>>;
  listCollections(): Promise<string[]>;
  /** Always throws: unsupported until log compaction (roadmap step 3). */
  dropCollection(name: string): Promise<never>;
  compact(options?: { minBytes?: number; factor?: number; skipBusy?: boolean }): Promise<Record<string, object | null>>;
  close(): Promise<void>;
}

/** The log's reserved file name within the provider ("__wal__.bj"). */
export declare const WAL_FILE: string;

export declare function connectWal(
  provider: any,
  options?: { order?: number; autoCompact?: { minBytes?: number; factor?: number } | null }
): Promise<WalDb>;
