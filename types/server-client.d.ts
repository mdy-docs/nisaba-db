/**
 * Types for `nisaba/server-client` (src/db-server-client.js) -- the client
 * for the database server (server/main.c), over a TCP socket.
 *
 * Deliberately NOT the full Collection type from ./nisaba.js: the wire
 * carries a fixed set of operations and this carries the same set. A
 * method the server has no op for throws a sentence saying so, and giving
 * it a type here would promise otherwise.
 */
import type { Document, Filter, Update, ObjectId } from './nisaba.js';

export { encode, decode, ObjectId, Pointer } from './nisaba.js';

/** The op names the wire carries; wasm/src/db_request.c owns the list. */
export const WIRE_OPS: readonly string[];

/** A refusal from the server: `code` is the DC_ERR_* it answered with.
 * `index` is present only when the refusal names a position in a list of
 * operations (a malformed bulkWrite). */
export class ServerError extends Error {
  readonly code: number;
  readonly index?: number;
}

export function parseAddress(address: string | { host?: string; port: number }):
  { host: string; port: number };

export interface RemoteFindOptions {
  sort?: Document;
  projection?: Document;
  skip?: number;
  limit?: number;
  /** Page the scan instead of returning one frame. Refused together with
   * `sort` (ServerError -48): a sort needs every match before the first
   * ordered result exists. */
  batchSize?: number;
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
  /** One document at a time, the shape the in-process cursor uses. */
  next(): Promise<{ value: T; done: false } | { value: undefined; done: true }>;
  /** One batch per call; empty when the scan is done. Only meaningful
   * for a cursor opened with `batchSize`. */
  nextBatch(): Promise<T[]>;
  /** Give the server's cursor slot back early. Draining, and closing the
   * connection, both do it for you. */
  close(): Promise<void>;
  [Symbol.asyncIterator](): AsyncIterableIterator<T>;
}

export interface RemoteInsertManyResult {
  acknowledged: boolean;
  insertedCount: number;
  insertedIds: Record<number, ObjectId>;
}

export interface RemoteBulkWriteResult {
  acknowledged: boolean;
  insertedCount: number;
  matchedCount: number;
  modifiedCount: number;
  deletedCount: number;
  upsertedCount: number;
  insertedIds: Record<number, ObjectId>;
  upsertedIds: Record<number, ObjectId>;
}

export interface RemoteCollection<T extends Document = Document> {
  readonly collectionName: string;
  find(filter?: Filter, options?: RemoteFindOptions): RemoteCursor<T>;
  findOne(filter?: Filter): Promise<T | null>;
  countDocuments(filter?: Filter): Promise<number>;
  distinct(field: string, filter?: Filter): Promise<unknown[]>;
  /** The `_id` is minted client-side (C will not invent one: it needs a clock). */
  insertOne(doc: T): Promise<RemoteWriteResult & { insertedId: ObjectId }>;
  /** Every document in one round trip; ids minted client-side as above.
   * `ordered` (default true) stops at the first failing document. A
   * failure rejects with the ServerError for that document, carrying what
   * did land as `err.result`. */
  insertMany(docs: T[], options?: { ordered?: boolean }): Promise<RemoteInsertManyResult>;
  /** A list of writes in one round trip. The grammar is the server's
   * (db_bulk.h): a malformed operation rejects the whole list with a
   * ServerError whose `index` names it, before any of it runs. A failure
   * while running rejects with `err.result` and `err.writeErrors`. */
  bulkWrite(operations: Document[], options?: { ordered?: boolean }): Promise<RemoteBulkWriteResult>;
  deleteOne(filter?: Filter): Promise<RemoteWriteResult>;
  deleteMany(filter?: Filter): Promise<RemoteWriteResult>;
  replaceOne(filter: Filter, doc: T, options?: { upsert?: boolean }): Promise<RemoteWriteResult>;
  updateOne(filter: Filter, update: Update, options?: { upsert?: boolean }): Promise<RemoteWriteResult>;
  updateMany(filter: Filter, update: Update, options?: { upsert?: boolean }): Promise<RemoteWriteResult>;
  /** Rewrite this collection's files without their append-only history.
   * Refused (ServerError -49) while any cursor is open over it. */
  compact(): Promise<{ generation: number; bytesBefore: number; bytesAfter: number; bytesFreed: number }>;
  /** Plans, creates, backfills and records the index; resolves with the
   * name the server chose (ServerError -56 if that name is taken). */
  createIndex(keys: Record<string, 1 | 'text' | '2dsphere'>, options?: {
    name?: string;
    unique?: boolean;
    sparse?: boolean;
    partialFilterExpression?: Filter;
    expireAfterSeconds?: number;
  }): Promise<string>;
  /** ServerError -57 if the collection has no index of that name. */
  dropIndex(name: string): Promise<void>;
  listIndexes(): Promise<Document[]>;
}

export interface RemoteDb {
  readonly isOpen: boolean;
  readonly address: string;
  collection<T extends Document = Document>(name: string): RemoteCollection<T>;
  /** Make an empty collection. Resolves false if it already existed --
   * both are success. An insert into a missing collection makes one too. */
  createCollection(name: string): Promise<boolean>;
  /** Remove a collection and every file it owns. False if there was no
   * such collection. */
  dropCollection(name: string): Promise<boolean>;
  /** Every collection in the database this server holds. */
  listCollections(): Promise<string[]>;
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
