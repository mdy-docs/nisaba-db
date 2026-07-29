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
  /** Aggregated observability stream: every group's node events tagged
   * with { group } — RaftMonitor consumes this. */
  onEvent: ((event: object) => void) | null;
  /** One JSON-able snapshot across every hosted group. */
  status(): object;
  /** The transport's receiving half. */
  handleEnvelope(envelope: { group: string; msg: object }): object | Promise<object>;
  tick(now?: number): void;
  start(): void;
  stop(): void;
}

/** Join a group knowing only seed ADDRESSES; resolves with the adopted
 * member records once the leader commits (docs/clustering.md). Call
 * before creating the group's node and pass the result as its peers. */
export declare function joinGroup(
  transport: { callAddress(addr: object, envelope: object): Promise<object>; setPeer?(id: number, addr: object): void },
  groupId: string,
  member: { id: number; host: string; port: number },
  options: { seeds: Array<{ host: string; port: number }>; attempts?: number; delayMs?: number }
): Promise<Array<{ id: number; host?: string; port?: number }>>;

/** Remove a member via any seed address; resolves with the adopted
 * member records once committed. */
export declare function leaveGroup(
  transport: { callAddress(addr: object, envelope: object): Promise<object> },
  groupId: string,
  id: number,
  options: { seeds: Array<{ host: string; port: number }>; attempts?: number; delayMs?: number }
): Promise<Array<{ id: number; host?: string; port?: number }>>;
