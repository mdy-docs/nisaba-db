/**
 * Types for `nisaba/raft` (src/raft.js) -- the transport-agnostic Raft
 * core: replication roadmap step 5. One RaftNode replicates one EntryLog
 * across a static cluster; the host wires the transport (call/handleMessage)
 * and drives time (tick). See src/raft.js's header for the full contract.
 */

export declare class NotLeaderError extends Error {
  name: 'NotLeaderError';
  /** Last known leader id (0 if unknown) -- the retry hint. */
  leaderId: number;
}

export declare const ROLE: { FOLLOWER: 'follower'; CANDIDATE: 'candidate'; LEADER: 'leader' };

export interface RaftEntry {
  index: number;
  term: number;
  type: number;
  payload: Uint8Array;
}

export interface RaftStateMachine {
  /** Committed NORMAL entries, strictly in index order. */
  apply(entry: RaftEntry): unknown | Promise<unknown>;
  appliedIndex(): number | Promise<number>;
}

export interface RaftTransport {
  call(peerId: number, message: object): Promise<object>;
}

export interface RaftSnapshotManifest {
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  config: unknown;
  files: Array<{ role: string; size: number; crc: number }>;
}

/** Snapshot serving (leader side) + install (follower side); see
 * src/raft.js's constructor doc for the exact contract. */
export interface RaftSnapshotter {
  latest(): (RaftSnapshotManifest & { [k: string]: any }) | null;
  openFile(role: string): { read(buf: Uint8Array, opts: { at: number }): number; close?(): void } | Promise<any>;
  beginInstall(manifest: RaftSnapshotManifest): {
    writeChunk(role: string, offset: number, data: Uint8Array): void | Promise<void>;
    commit(): void | Promise<void>;
    abort(): void | Promise<void>;
  } | Promise<any>;
}

export declare class RaftNode {
  constructor(options: {
    id: number;
    peers: number[];
    log: any; // an open EntryLog (`nisaba/wasm`)
    stateMachine: RaftStateMachine;
    transport: RaftTransport;
    snapshotter?: RaftSnapshotter | null;
    /** Return a fresh OPEN EntryLog based at the install boundary; the
     * node closes the old log first and re-persists hard state. */
    rebaseLog?: (lastIncludedIndex: number, lastIncludedTerm: number) => Promise<any>;
    electionTimeoutMs?: [number, number];
    heartbeatMs?: number;
    maxBatchBytes?: number;
    snapshotChunkBytes?: number;
    random?: () => number;
  });
  readonly id: number;
  readonly role: 'follower' | 'candidate' | 'leader';
  readonly term: number;
  leaderId: number;
  commitIndex: number;
  lastApplied: number;
  isRunning: boolean;
  /** Peers that need an InstallSnapshot (roadmap 5b). */
  readonly peersNeedingSnapshot: number[];
  start(now?: number): Promise<void>;
  stop(): Promise<void>;
  tick(now: number): void;
  propose(payload: Uint8Array | string, type?: number): Promise<{ index: number; term: number }>;
  handleMessage(message: object): object;
}
