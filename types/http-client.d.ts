/**
 * Types for `nisaba/http-client` (src/db-http-client.js) -- the
 * JavaScript client for the HTTP front end (db-http-front.js): the
 * TCP client's surface, spelled against fetch, runnable in a browser.
 *
 * The shared shapes are imported from ./server-client.js rather than
 * restated: the two clients are a mirror, and a second copy of a
 * mirrored type is how the mirror drifts. What is declared here is
 * only what HTTP changes -- the connect, the errors' classes, and the
 * change stream's resume machinery.
 */
import type { Document, Filter, ObjectId } from './nisaba.js';
import type {
  RemoteFindOptions, RemoteCursor, RemotePlan, RemoteWriteResult,
  RemoteInsertManyResult, RemoteBulkWriteResult, RemoteChangeEvent,
  RemoteCollection
} from './server-client.js';

export { ObjectId } from './nisaba.js';
export type {
  RemoteFindOptions, RemoteCursor, RemotePlan, RemoteWriteResult,
  RemoteInsertManyResult, RemoteBulkWriteResult, RemoteChangeEvent
};

/** The op names the wire carries; engine/src/db_request.c owns the list. */
export const WIRE_OPS: readonly string[];

/** A refusal from the server, carried through the front end verbatim:
 * `code` is the DC_ERR_* it answered with, `status` the HTTP status it
 * rode in under. Catch by `code` -- that is the part the server meant.
 * This class is this module's own (the TCP client's would drag
 * node:net into a page); the shape is identical. */
export class ServerError extends Error {
  readonly code: number;
  readonly status?: number;
  readonly index?: number;
  readonly leaderId?: number;
  readonly leader?: Document;
}

/** The server stopped holding events for the stream. Reaching this
 * side means there was no log to resume from (the front end bridges
 * the resumable kind itself): watch again and re-read current state. */
export class ChangeStreamOverflowError extends Error {
  readonly resumeFrom: number | null;
}

/** `http://host:port`, `host:port` or a bare port, normalized to an
 * origin. Scheme-less means http, the front end's own default. */
export function parseBaseUrl(base: string): string;

/**
 * A change stream over the front end's Server-Sent Events: the TCP
 * client's stream object, plus the resume machinery HTTP carries.
 */
export interface HttpChangeStream extends AsyncIterable<RemoteChangeEvent> {
  on(event: 'change', cb: (change: RemoteChangeEvent) => void): this;
  off(event: 'change', cb: (change: RemoteChangeEvent) => void): this;
  next(): Promise<{ value: RemoteChangeEvent; done: false } | { value: undefined; done: true }>;
  close(): Promise<void>;
  /** Resolves once the front end has committed the stream; rejects
   * with the refusal otherwise. Iterating alone surfaces it too. */
  readonly ready: Promise<HttpChangeStream>;
  /** The log index of the last event handed over (or the subscribe's
   * replay ceiling before any event) -- `watch({ from })` resumes right
   * after it. null on a server without a log. */
  readonly resumeFrom: number | null;
  /** The member the stream is subscribed on, once `ready`. */
  readonly member?: string;
}

/** The TCP client's collection surface, with HTTP's watch: `from`
 * resumes on a replicated server. */
export interface HttpCollection<T extends Document = Document>
  extends Omit<RemoteCollection<T>, 'watch'> {
  watch(options?: { from?: number }): HttpChangeStream;
}

export interface HttpDb {
  readonly name: string;
  readonly isOpen: boolean;
  collection<T extends Document = Document>(name: string): HttpCollection<T>;
  createCollection(name: string): Promise<boolean>;
  dropCollection(name: string): Promise<boolean>;
  compact(options?: { minBytes?: number; factor?: number; skipBusy?: boolean }):
    Promise<Record<string, { generation: number; bytesBefore: number; bytesAfter: number; bytesFreed: number } | null>>;
  listCollections(): Promise<string[]>;
  /** Send any op the wire has: `op` and `coll` become the URL, the
   * rest the body -- the front end's own split. */
  request(req: Document): Promise<Document>;
  close(): Promise<void>;
}

export interface HttpClient {
  readonly isOpen: boolean;
  /** The front end's origin, as normalized. */
  readonly address: string;
  db(name: string): HttpDb;
  listDatabases(): Promise<string[]>;
  dropDatabase(name: string): Promise<boolean>;
  /** On a replicated server, also where a member says what it is:
   * { pong, role, leaderId, applied, commit, base, last }. */
  ping(): Promise<Document>;
  /** Hand leadership to member `to`; resolves once leadership has
   * actually moved. -63 off the leader, -74 without a log, -75 when
   * the deadline passed (a retry is then safe). */
  transferLeadership(to: number): Promise<void>;
  /** The escape hatch: {op, db?, coll?, ...rest}. */
  request(req: Document): Promise<Document>;
  /** Closes this side's held state -- open change streams and cursor
   * sessions. HTTP itself holds nothing. */
  close(): Promise<void>;
}

/**
 * Connect to an HTTP front end (db-http, src/db-http-front.js).
 * Verified with one ping: a wrong URL is an error here, at the name
 * that says "connect", not on the first query.
 */
export function connectHttp(base: string): Promise<HttpClient>;
