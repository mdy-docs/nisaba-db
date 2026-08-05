/**
 * Types for `nisaba/s3` (src/s3.js) -- the S3 client the backup agent
 * stands on: SigV4 over node:http/https, path-style addressing, and
 * nothing else. No AWS SDK; the surface is five verbs.
 */

/** A non-2xx answer from the store: `status` is HTTP's, `code` is
 * S3's (<Code> in the error body; '' when the body carried none). */
export class S3Error extends Error {
  readonly status: number;
  readonly code: string;
}

export class S3Client {
  /** Credentials default from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY;
   * `endpoint` picks the target -- MinIO in development
   * ('http://127.0.0.1:9000'), AWS by omitting it. */
  constructor(options: {
    bucket: string;
    endpoint?: string;
    region?: string;
    accessKeyId?: string;
    secretAccessKey?: string;
  });
  readonly bucket: string;
  readonly region: string;
  /** Idempotent: already-ours is success. */
  createBucket(): Promise<void>;
  /** Single-part, signed over the real payload hash. `metadata` rides
   * as x-amz-meta-* headers -- facts ABOUT the object, never in its
   * bytes -- and comes back from headObject. */
  putObject(key: string, body: Uint8Array, options?: {
    contentType?: string;
    metadata?: Record<string, string>;
  }): Promise<{ etag: string | null }>;
  getObject(key: string): Promise<Buffer>;
  /** null when there is no such object -- absence is an answer here,
   * not an exception, because callers probe. */
  headObject(key: string): Promise<{
    size: number;
    etag: string | null;
    metadata: Record<string, string>;
  } | null>;
  /** Deleting the already-gone succeeds. */
  deleteObject(key: string): Promise<void>;
  /** Every key under `prefix`, paged internally until the listing is
   * complete. `delimiter: '/'` answers "what directories are here". */
  list(prefix: string, options?: { delimiter?: string; maxKeysPerPage?: number }):
    Promise<{ keys: Array<{ key: string; size: number }>; prefixes: string[] }>;
}
