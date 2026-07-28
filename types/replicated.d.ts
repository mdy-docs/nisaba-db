/**
 * Types for `nisaba/replicated` (src/db-replicated.js) -- one member of a
 * Raft group replicating one database: replication roadmap step 5c.
 * WalDb's driver surface with the commit engine swapped for Raft
 * propose/commit/apply; writes are leader-only (NotLeaderError carries
 * the retry hint), reads serve the local replica.
 */
import type { WalDb, WalCollection, SnapshotInfo } from './wal.js';
import type { RaftNode, RaftTransport, NotLeaderError } from './raft.js';

export declare class DbStateMachine {
  appliedIndex(): Promise<number>;
  apply(entry: { index: number; term: number; type: number; payload: Uint8Array }): Promise<void>;
}

export declare class ReplicatedDb extends WalDb {
  /** The RaftNode: role/term/leaderId for routing, tick(now) for the
   * host's clock, handleMessage for the transport's receiving half. */
  readonly raft: RaftNode;
  /** Local snapshot + log compaction, quiesced via runExclusive; the
   * boundary is lastApplied (an uncommitted suffix is never baked in). */
  snapshot(): Promise<SnapshotInfo>;
}

export declare function connectReplicated(
  provider: any,
  options: {
    id: number;
    peers: number[];
    transport: RaftTransport;
    raft?: object;
    snapshotPrefix?: string;
    startNow?: number;
    order?: number;
    autoCompact?: { minBytes?: number; factor?: number } | null;
  }
): Promise<ReplicatedDb>;

export { NotLeaderError };
