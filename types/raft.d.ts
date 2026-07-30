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

/** The transport FRAMES, it does not interpret: messages cross as the
 * bytes C produced (the grammar is raft_msg.h's). */
export interface RaftTransport {
  call(peerId: number, message: Uint8Array): Promise<Uint8Array>;
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

/** A cluster member: id plus (optionally) its transport address.
 * voting: false marks a learner — replicated to, but no quorum weight,
 * no campaigning, no votes; auto-promoted when caught up
 * (docs/clustering.md). Extra fields ride through CONFIG entries
 * untouched. */
export interface MemberRecord {
  id: number;
  host?: string;
  port?: number;
  voting?: boolean;
  [k: string]: unknown;
}

export declare class RaftNode {
  constructor(options: {
    id: number;
    /** Bootstrap member set (ids or records; a CONFIG entry recovered
     * from the log or a snapshot overrides it). The first node of a
     * fresh cluster should list itself WITH its address. */
    peers: Array<number | MemberRecord>;
    /** Fired with the full member-record list on every membership
     * adoption — the peer-table sync hook (docs/clustering.md). */
    onConfig?: (members: MemberRecord[]) => void;
    /** Observability stream: state-transition events ({ type, time,
     * node, term, ... }) — elections, role changes, config, promotions,
     * installs, peer reachability edges, quiesce/wake, halts. */
    onEvent?: (event: object) => void;
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
  /** Current member ids (derived from memberInfo). */
  readonly members: number[];
  /** The electorate: member ids without voting: false. */
  readonly voters: number[];
  /** Current member records — ids and addresses, from the log. */
  readonly memberInfo: MemberRecord[];
  onConfig: ((members: MemberRecord[]) => void) | null;
  onEvent: ((event: object) => void) | null;
  /** One JSON-able "what is true right now" snapshot (role, term, log
   * bounds, members, the leader's per-peer replication view). */
  status(): object;
  readonly leaderId: number;
  readonly commitIndex: number;
  lastApplied: number;
  isRunning: boolean;
  /** Timers parked (see quiesce/wake). */
  readonly quiesced: boolean;
  /** The EntryLog. Assigning it repoints the C node too — it holds a
   * borrowed pointer, and both compaction paths swap the log. */
  log: any;
  /** The leader's replication cursors for a peer (0 elsewhere). */
  matchOf(peer: number): number;
  nextOf(peer: number): number;
  /** Peers that need an InstallSnapshot this node cannot serve (5b). */
  readonly peersNeedingSnapshot: number[];
  /** Can this leader still prove it reaches a quorum? (check-quorum) */
  hasQuorumContact(withinMs: number): boolean;
  start(now?: number): Promise<void>;
  stop(): Promise<void>;
  /** Release the C node — the end of this object's life. Throws if the
   * node is still running (stop() first), and every accessor throws
   * afterwards rather than reading through a freed pointer. stop() does
   * NOT do this: a stopped node keeps answering status questions. */
  free(): void;
  tick(now: number): void;
  quiesce(): void;
  wake(now?: number): void;
  /** Run `fn` with the node quiesced — nothing else touches its log. */
  runExclusive<T>(fn: () => T | Promise<T>): Promise<T>;
  propose(payload: Uint8Array | string, type?: number): Promise<{ index: number; term: number }>;
  /** Propose a full-replacement member set (ids inherit known records,
   * so addresses can't be erased). One change in flight at a time. */
  changeMembership(members: Array<number | MemberRecord>): Promise<{ index: number; term: number }>;
  /** Graceful leadership transfer (§3.10). Resolves once leadership has
   * left this node; rejects (lifting the fence) on timeout. */
  transferLeadership(targetId: number, options?: { timeoutMs?: number }): Promise<void>;
  handleMessage(message: Uint8Array): Uint8Array | Promise<Uint8Array>;
}
