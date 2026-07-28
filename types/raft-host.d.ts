/**
 * Types for `nisaba/raft-host` (src/raft-host.js) -- one server process's
 * seat across many Raft groups, multiplexed over one envelope transport
 * and one clock, with idle-group quiescence. Roadmap step 5d.
 */
import type { RaftNode, RaftTransport } from './raft.js';

export interface EnvelopeTransport {
  call(peerNodeId: number, envelope: { group: string; msg: object }): Promise<object>;
}

export declare class RaftGroupHost {
  constructor(options: {
    transport: EnvelopeTransport;
    tickMs?: number;
    quiesceAfterMs?: number;
    now?: () => number;
  });
  /** Per-group transport shim for connectReplicated / new RaftNode. */
  groupTransport(groupId: string): RaftTransport;
  /** Register a ReplicatedDb (uses .raft) or a bare RaftNode; returns it. */
  addGroup<T>(groupId: string, value: T): T;
  removeGroup(groupId: string): void;
  readonly groupIds: string[];
  group(groupId: string): unknown;
  /** Mark local activity so a quiesced group wakes for a client request. */
  touch(groupId: string): void;
  /** The transport's receiving half. */
  handleEnvelope(envelope: { group: string; msg: object }): object | Promise<object>;
  tick(now?: number): void;
  start(): void;
  stop(): void;
}
