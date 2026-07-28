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

export declare class RaftNode {
  constructor(options: {
    id: number;
    peers: number[];
    log: any; // an open EntryLog (`nisaba/wasm`)
    stateMachine: RaftStateMachine;
    transport: RaftTransport;
    electionTimeoutMs?: [number, number];
    heartbeatMs?: number;
    maxBatchBytes?: number;
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
