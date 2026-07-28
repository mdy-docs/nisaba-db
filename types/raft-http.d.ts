/**
 * Types for `nisaba/raft-http` (src/raft-transport-http.js) -- the HTTP
 * reference node-to-node transport (node:http, binjson bodies as
 * application/octet-stream). Node-only. Same contract as
 * `nisaba/raft-tcp`; see the source header for the trade-offs.
 */
export declare class HttpRaftTransport {
  constructor(options: {
    listenPort: number;
    listenHost?: string;
    path?: string;
    peers?: Record<number, { host: string; port: number }>;
    onMessage: (envelope: object) => object | Promise<object>;
    requestTimeoutMs?: number;
    maxSockets?: number;
    maxBodyBytes?: number;
    headers?: Record<string, string>;
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
