/**
 * Types for `nisaba/raft-tcp` (src/raft-transport-tcp.js) -- the
 * zero-dependency reference node-to-node transport (node:net,
 * length-prefixed binjson frames). Node-only. Roadmap step 5d.
 */
export declare class TcpRaftTransport {
  constructor(options: {
    listenPort: number;
    listenHost?: string;
    peers?: Record<number, { host: string; port: number }>;
    onMessage: (envelope: object) => object | Promise<object>;
    requestTimeoutMs?: number;
  });
  isRunning: boolean;
  setPeer(nodeId: number, addr: { host: string; port: number }): void;
  removePeer(nodeId: number): void;
  start(): Promise<void>;
  /** The bound address (useful with listenPort 0). */
  address(): { address: string; port: number } | null;
  call(peerId: number, envelope: object): Promise<object>;
  /** One-shot request to a bare address (join flow — docs/clustering.md). */
  callAddress(addr: { host: string; port: number }, envelope: object): Promise<object>;
  stop(): Promise<void>;
}
