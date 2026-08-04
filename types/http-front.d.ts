/**
 * Types for `nisaba/http-front` (src/db-http-front.js) -- HTTP in front
 * of one nisaba-server or a cluster of them, over the server client. No
 * engine in the process. RPC-shaped URL grammar (POST /<op>,
 * /db/<db>/<op>, /db/<db>/<coll>/<op>), Extended JSON bodies, sessions
 * for cursors, Server-Sent Events for change streams, writes and reads
 * following the leader. docs/http-front.md has the grammar.
 */
export declare class DbHttpFront {
  constructor(
    targets: string | string[],
    options?: {
      listenPort?: number;
      listenHost?: string;
      /** How long a request chases a moved leader before the refusal is
       * the answer, in ms (default 8000). */
      retryMs?: number;
      /** SSE keep-alive comment interval, in ms (default 15000). */
      heartbeatMs?: number;
    }
  );
  start(): Promise<void>;
  /** The bound address (useful with listenPort 0). */
  address(): { address: string; port: number } | null;
  stop(): Promise<void>;
}
