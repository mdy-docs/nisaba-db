/**
 * Types for `nisaba/raft-monitor` (src/raft-monitor.js) -- the
 * observability endpoint for a cluster node: GET /status (one-off JSON
 * snapshot) and GET /events (SSE stream, snapshot-first). Node-only,
 * read-only, unauthenticated -- bind to loopback or front with auth.
 */
export declare class RaftMonitor {
  constructor(
    host: { status(): object; onEvent: ((event: object) => void) | null },
    options: { listenPort: number; listenHost?: string; heartbeatMs?: number }
  );
  start(): Promise<void>;
  /** The bound address (useful with listenPort 0). */
  address(): { address: string; port: number } | null;
  stop(): Promise<void>;
}
