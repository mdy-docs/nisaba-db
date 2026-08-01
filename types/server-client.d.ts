/**
 * Types for `nisaba/server-client` (src/db-server-client.js) -- the client
 * for the database server (server/main.c), over a TCP socket.
 *
 * Deliberately NOT the full Collection type from ./nisaba.js: the wire
 * carries ten operations and this carries the same ten. A method the
 * server has no op for throws a sentence saying so, and giving it a type
 * here would promise otherwise.
 */
import type { Document, Filter, Update, ObjectId } from './nisaba.js';

export { encode, decode, ObjectId, Pointer } from './nisaba.js';

/** The op names the wire carries; wasm/src/db_request.c owns the list. */
export const WIRE_OPS: readonly string[];

/** A refusal from the server: `code` is the DC_ERR_* it answered with. */
export class ServerError extends Error {
  readonly code: number;
}

export function parseAddress(address: string | { host?: string; port: number }):
  { host: string; port: number };

export interface RemoteFindOptions {
  sort?: Document;
  projection?: Document;
  skip?: number;
  limit?: number;
}

export interface RemoteWriteResult {
  acknowledged: boolean;
  matchedCount: number;
  modifiedCount: number;
  deletedCount: number;
  insertedCount: number;
  upsertedId: ObjectId | null;
}

export interface RemoteCursor<T = Document> {
  toArray(): Promise<T[]>;
  [Symbol.asyncIterator](): AsyncIterableIterator<T>;
}

export interface RemoteCollection<T extends Document = Document> {
  readonly collectionName: string;
  find(filter?: Filter, options?: RemoteFindOptions): RemoteCursor<T>;
  findOne(filter?: Filter): Promise<T | null>;
  countDocuments(filter?: Filter): Promise<number>;
  distinct(field: string, filter?: Filter): Promise<unknown[]>;
  /** The `_id` is minted client-side (C will not invent one: it needs a clock). */
  insertOne(doc: T): Promise<RemoteWriteResult & { insertedId: ObjectId }>;
  deleteOne(filter?: Filter): Promise<RemoteWriteResult>;
  deleteMany(filter?: Filter): Promise<RemoteWriteResult>;
  replaceOne(filter: Filter, doc: T, options?: { upsert?: boolean }): Promise<RemoteWriteResult>;
  updateOne(filter: Filter, update: Update, options?: { upsert?: boolean }): Promise<RemoteWriteResult>;
  updateMany(filter: Filter, update: Update, options?: { upsert?: boolean }): Promise<RemoteWriteResult>;
}

export interface RemoteDb {
  readonly isOpen: boolean;
  readonly address: string;
  collection<T extends Document = Document>(name: string): RemoteCollection<T>;
  /** The op that touches no collection: keeps a connection warm past the
   * server's --idle-timeout. Sent automatically on a timer unless
   * connectServer was given `keepAliveMs: 0`. */
  ping(): Promise<true>;
  /** Send any op the wire has and read the response object as it came. */
  request(req: Document): Promise<Document>;
  close(): Promise<void>;
}

/**
 * Connect to a server. There is no database name: the server was pointed
 * at one directory when it started and serves that one for its lifetime.
 *
 * A held connection occupies one of the server's --max-clients slots;
 * past that, connecting still succeeds and the first call rejects with a
 * ServerError (code -44). A connection that asks nothing is closed after
 * the server's --idle-timeout (code -45), which `keepAliveMs` exists to
 * prevent: it pings on that interval (20s; 0 disables), on an unref'd
 * timer that never holds a process open.
 */
export function connectServer(
  address: string | { host?: string; port: number },
  options?: { keepAliveMs?: number }
): Promise<RemoteDb>;
