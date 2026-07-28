/**
 * Type declarations for the `nisaba` main entry (src/db.js) — the full
 * in-process database (docs/db-api.md is the prose reference; docs/
 * roadmap.md P0 #1). The other entries re-use these types:
 * `nisaba/remote` (types/remote.d.ts), `nisaba/node` (types/node.d.ts),
 * `nisaba/coordinator` (types/coordinator.d.ts), `nisaba/wasm`
 * (types/wasm.d.ts).
 *
 * Filters/updates are deliberately loose (`Record<string, any>`): the
 * operator grammar lives in the engine and a full MongoDB-style
 * conditional type tower would promise more precision than the runtime
 * checks. Documents are generic where it pays (reads), plain where it
 * doesn't.
 */

// ---- values -------------------------------------------------------------

export class ObjectId {
  /** No argument generates a new id; accepts a 24-hex string, a 12-byte array, or another ObjectId (copy). */
  constructor(id?: string | Uint8Array | number[] | ObjectId);
  toString(): string;
  toHexString(): string;
  getTimestamp(): Date;
  equals(other: ObjectId | string): boolean;
  compare(other: ObjectId): -1 | 0 | 1;
}

export class Pointer {
  constructor(offset: number);
  valueOf(): number;
  toString(): string;
}

export type Document = Record<string, any>;
/** Query filter -- field values and `$`-operators (docs/db-api.md "Query operators"). */
export type Filter = Record<string, any>;
/** Update document -- `$set`/`$inc`/... (docs/db-api.md "Update operators"). */
export type Update = Record<string, any>;
export type SortSpec = Record<string, 1 | -1>;
/** Inclusion XOR exclusion; `_id` included by default. */
export type Projection = Record<string, 0 | 1 | boolean>;

/** The pure-JS (no WASM) binjson codec -- safe before ready(). */
export function encode(value: any): Uint8Array;
export function decode(bytes: Uint8Array): any;

// ---- errors -------------------------------------------------------------

/** Base of every coded error; `code` is the C-side error code. Errors
 * proxied across the coordinator RPC wire keep `name`/`code` as data but
 * not their prototype -- branch on `err.name`, not `instanceof`, when an
 * error may have crossed tabs. */
export class NisabaError extends Error {
  code: number;
}
/** Duplicate `_id` (-10) or unique-index violation (-12). */
export class DuplicateKeyError extends NisabaError {}
/** Document lacks a field a non-sparse index requires (-13). */
export class MissingIndexedFieldError extends NisabaError {}
/** Indexed field value can't be key-encoded (-14). */
export class UnindexableValueError extends NisabaError {}
/** A ChangeStream's iterator buffer overflowed (watch()'s `maxBuffered`). */
export class ChangeStreamOverflowError extends NisabaError {}
/** `_id` was not an ObjectId. Unlike MongoDB, scalar _ids are not
 * supported by the on-disk format -- keep natural keys in their own
 * field with a unique index. */
export class InvalidIdError extends NisabaError {}

// ---- storage providers ----------------------------------------------------

/** The FileSystemSyncAccessHandle surface the engine needs. */
export interface SyncAccessHandle {
  getSize(): number;
  read(buffer: Uint8Array, options?: { at?: number }): number;
  write(buffer: Uint8Array, options?: { at?: number }): number;
  truncate(len: number): void;
  flush(): void;
  close(): void | Promise<void>;
}

/** What connect()/connectClient() need from storage. `listFiles` is
 * optional but enables the orphan sweep (docs/compaction.md). */
export interface StorageProvider {
  openFile(name: string, options?: { create?: boolean }): Promise<SyncAccessHandle>;
  deleteFile(name: string): Promise<void>;
  listFiles?(): Promise<string[]>;
  subProvider?(name: string): Promise<StorageProvider>;
}

export class MemoryStorageProvider implements StorageProvider {
  openFile(name: string, options?: { create?: boolean }): Promise<SyncAccessHandle>;
  deleteFile(name: string): Promise<void>;
  listFiles(): Promise<string[]>;
  subProvider(name: string): Promise<MemoryStorageProvider>;
}

export class OPFSStorageProvider implements StorageProvider {
  /** Defaults to navigator.storage.getDirectory(). Real browsers only
   * allow sync access handles inside a Worker. */
  constructor(dirHandle?: any /* FileSystemDirectoryHandle */);
  openFile(name: string, options?: { create?: boolean }): Promise<SyncAccessHandle>;
  deleteFile(name: string): Promise<void>;
  listFiles(): Promise<string[]>;
  subProvider(name: string): Promise<OPFSStorageProvider>;
}

// ---- results -------------------------------------------------------------

export interface InsertOneResult {
  acknowledged: true;
  insertedId: ObjectId;
}
export interface InsertManyResult {
  acknowledged: true;
  insertedCount: number;
  insertedIds: Record<number, ObjectId>;
}
export interface UpdateResult {
  acknowledged: true;
  matchedCount: number;
  modifiedCount: number;
  /** null unless an upsert inserted a new document. */
  upsertedId: ObjectId | null;
}
export interface DeleteResult {
  acknowledged: true;
  deletedCount: number;
}
export interface BulkWriteResult {
  acknowledged: true;
  insertedCount: number;
  matchedCount: number;
  modifiedCount: number;
  deletedCount: number;
  upsertedCount: number;
  insertedIds: Record<number, ObjectId>;
  upsertedIds: Record<number, ObjectId>;
}
export interface CompactStats {
  generation: number;
  bytesBefore: number;
  bytesAfter: number;
  bytesFreed: number;
}
export interface IndexDescription {
  name: string;
  key: Record<string, 1 | 'text' | '2dsphere'>;
  unique?: boolean;
  sparse?: boolean;
  partialFilterExpression?: Filter;
  expireAfterSeconds?: number;
}

// ---- cursors & change streams ---------------------------------------------

/** Which candidate source a filter's query dispatch uses -- consults the
 * same C planners the queries run. 'scan' is the signal to add an index. */
export interface ExplainResult {
  source: 'scan' | 'ids' | 'equality' | 'text' | 'geo';
  index: string | null;
}

export interface FindCursor<T extends Document = Document> extends AsyncIterable<T> {
  sort(spec: SortSpec): FindCursor<T>;
  skip(n: number): FindCursor<T>;
  limit(n: number): FindCursor<T>;
  project(spec: Projection): FindCursor<T>;
  toArray(): Promise<T[]>;
  /** Not supported after .sort() -- use toArray()/for-await instead. */
  next(): Promise<{ value: T | undefined; done: boolean }>;
  /** The plan this cursor's filter gets -- Collection.explain sugar. */
  explain(): Promise<ExplainResult>;
  /** Required if the cursor is abandoned unexhausted (an open cursor
   * blocks compact(); a GC safety net exists but has no timing
   * guarantee). */
  close(): Promise<void>;
}

/** aggregate()'s handle: one execution on first pull. */
export interface AggregationCursor<T extends Document = Document> extends AsyncIterable<T> {
  toArray(): Promise<T[]>;
  next(): Promise<{ value: T | undefined; done: boolean }>;
  close(): Promise<void>;
}

export interface ChangeEvent<T extends Document = Document> {
  operationType: 'insert' | 'update' | 'replace' | 'delete';
  ns: { coll: string };
  documentKey: { _id: ObjectId };
  /** Absent for 'delete'. */
  fullDocument?: T;
}

export class ChangeStream<T extends Document = Document> implements AsyncIterable<ChangeEvent<T>> {
  on(event: 'change', cb: (change: ChangeEvent<T>) => void): this;
  off(cb: (change: ChangeEvent<T>) => void): this;
  next(): Promise<{ value: ChangeEvent<T> | undefined; done: boolean }>;
  close(): void;
  return(): Promise<{ value: undefined; done: true }>;
  [Symbol.asyncIterator](): AsyncIterator<ChangeEvent<T>>;
}

export interface WatchOptions {
  /** Bound on unconsumed events buffered for the iterator side (default
   * 4096); overflow closes the stream with ChangeStreamOverflowError. */
  maxBuffered?: number;
}

// ---- collection / db / client ----------------------------------------------

export class Collection<T extends Document = Document> {
  readonly name: string;

  insertOne(doc: T): Promise<InsertOneResult>;
  insertMany(docs: T[], options?: { ordered?: boolean }): Promise<InsertManyResult>;

  findOne(filter?: Filter, options?: { projection?: Projection }): Promise<T | null>;
  find(filter?: Filter, options?: { sort?: SortSpec; skip?: number; limit?: number; projection?: Projection }): FindCursor<T>;
  countDocuments(filter?: Filter): Promise<number>;
  estimatedDocumentCount(): Promise<number>;
  distinct(field: string, filter?: Filter): Promise<any[]>;
  /** Replicated-log integration: the last log index applied to this
   * collection (0 = not log-driven). Resume replay from appliedIndex()+1. */
  appliedIndex(): Promise<number>;
  /** Stage a log entry's index onto every structure this collection owns;
   * the entry's own mutation then persists it atomically. Monotonic. */
  setAppliedIndex(index: number): Promise<void>;
  /** Which source serves `filter` (index or scan) -- see ExplainResult. */
  explain(filter?: Filter): Promise<ExplainResult>;
  /** Small aggregation subset: $match (leading stage runs in the engine
   * with indexes + full grammar), $sort, $skip, $limit, $project, $group
   * ($sum/$avg/$min/$max/$first/$last/$push/$addToSet/$count), $count. */
  aggregate<R extends Document = Document>(pipeline?: Document[]): AggregationCursor<R>;

  updateOne(filter: Filter, update: Update, options?: { upsert?: boolean }): Promise<UpdateResult>;
  updateMany(filter: Filter, update: Update, options?: { upsert?: boolean }): Promise<UpdateResult>;
  replaceOne(filter: Filter, replacement: T, options?: { upsert?: boolean }): Promise<UpdateResult>;
  deleteOne(filter?: Filter): Promise<DeleteResult>;
  deleteMany(filter?: Filter): Promise<DeleteResult>;
  findOneAndUpdate(filter: Filter, update: Update, options?: { upsert?: boolean; returnDocument?: 'before' | 'after' }): Promise<T | null>;
  findOneAndReplace(filter: Filter, replacement: T, options?: { upsert?: boolean; returnDocument?: 'before' | 'after' }): Promise<T | null>;
  findOneAndDelete(filter?: Filter): Promise<T | null>;
  bulkWrite(operations: Document[], options?: { ordered?: boolean }): Promise<BulkWriteResult>;

  createIndex(keys: Record<string, 1 | 'text' | '2dsphere'>, options?: {
    name?: string;
    unique?: boolean;
    sparse?: boolean;
    partialFilterExpression?: Filter;
    expireAfterSeconds?: number;
  }): Promise<string>;
  dropIndex(name: string): Promise<void>;
  listIndexes(): Promise<IndexDescription[]>;
  findByIndex(name: string, values: any[]): Promise<T[]>;
  /** Host-driven TTL enforcement -- deletes documents past a TTL index's cutoff. */
  pruneExpired(): Promise<number>;

  watch(pipeline?: never[], options?: WatchOptions): ChangeStream<T>;
  /** Rewrite this collection's whole file set without its append-only
   * history (docs/compaction.md). Throws if find() cursors are open. */
  compact(): Promise<CompactStats>;
}

export interface AutoCompactOptions {
  minBytes?: number;
  factor?: number;
}

export interface ConnectOptions {
  /** B+ tree fan-out (default 32) -- rarely needs changing. */
  order?: number;
  /** Schedule one deferred compact({ ...opts, skipBusy: true }) sweep
   * after every open (and, under connectShared, on every leadership
   * acquisition). Observable via db.autoCompacted. */
  autoCompact?: AutoCompactOptions;
}

export class Db {
  readonly isOpen: boolean;
  /** Resolves with the deferred autoCompact sweep's results (null after
   * a failure); stays null when the option is off. */
  autoCompacted: Promise<Record<string, CompactStats | null> | null> | null;

  collection<T extends Document = Document>(name: string): Promise<Collection<T>>;
  listCollections(): Promise<string[]>;
  dropCollection(name: string): Promise<boolean>;
  compact(options?: { minBytes?: number; factor?: number; skipBusy?: boolean }): Promise<Record<string, CompactStats | null>>;
  /** navigator.storage.estimate() where available, else null. */
  storageEstimate(): Promise<{ usage?: number; quota?: number } | null>;
  close(): Promise<void>;
}

export class Client {
  db(name: string): Promise<Db>;
  close(): Promise<void>;
}

/** Open (creating if needed) the single database rooted at `provider`. */
export function connect(provider: StorageProvider, options?: ConnectOptions): Promise<Db>;
/** MongoClient-shaped: many named databases under one root provider. */
export function connectClient(provider: StorageProvider, options?: ConnectOptions): Promise<Client>;

// ---- main-thread worker bridge (re-exported from nisaba/remote) -------------

export interface RemoteBridge {
  /** Wrap a worker-side handle id as a local-feeling proxy; every
   * property access is one postMessage round trip -- always await. */
  makeProxy(handleId: string): any;
  rpcCall(handleId: string, method: string, args: any[]): Promise<any>;
}
export function createRemoteBridge(worker: Worker): RemoteBridge;
