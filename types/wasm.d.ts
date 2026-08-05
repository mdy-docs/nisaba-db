/**
 * Types for `nisaba/wasm` (src/nisaba-wasm.js) -- the full module. The
 * database-level surface is typed precisely (re-used from the main
 * entry); the lower-level structure classes (B+ tree, R-tree, text
 * index/log, diff/patch) are exported but deliberately left loose --
 * they're the engine's internals, documented by their source, not part
 * of the API reference (docs/db-api.md).
 */
export * from './nisaba.js';

/** Instantiate the WASM module; idempotent. connect() awaits it
 * transitively -- explicit ready() is only needed before using the
 * WASM-backed encode/decode or the low-level classes directly. */
export function ready(): Promise<any>;
export function isReady(): boolean;

// Low-level surface: present, intentionally untyped.
export const TYPE: Record<string, number>;
export function valueSize(bytes: Uint8Array): number;
export class BinJsonFile { [key: string]: any }
export class MemoryHandle { constructor(bytes?: Uint8Array); [key: string]: any }
export class BPlusTree { constructor(handle: any, order?: number); [key: string]: any }
export class RTree { constructor(handle: any); [key: string]: any }
export class TextLog { [key: string]: any }
export class TiledTextLog { [key: string]: any }
export class TextIndex { [key: string]: any }
export const ENTRY_TYPE: Record<string, number>;
export class EntryLog { constructor(handle: any, options?: { baseIndex?: number; baseTerm?: number }); [key: string]: any }
export const ENTRYLOG_TYPE: Record<string, number>;
export class SnapshotStore { [key: string]: any }
export function crc32(bytes: Uint8Array, prev?: number): number;
export function exists(dir: any, name: string): Promise<boolean>;
export function deleteFile(dir: any, name: string): Promise<void>;
export function getFileHandle(dir: any, name: string, options?: { create?: boolean }): Promise<any>;
/**
 * Composite-key encoding (keyenc.h). These are variadic, not array-taking:
 * compositeKey('core', 36), never compositeKey(['core', 36]) -- an array
 * argument throws "unsupported part type: object". The declarations said
 * `any[]` until the keyenc de-duplication, which is a shape no caller
 * could ever have used successfully.
 */
export function orderedKey(value: number | string): Uint8Array;
export function compositeKey(...parts: Array<number | string>): Uint8Array;
export function compositeUpperBound(...parts: Array<number | string>): Uint8Array;
export function haversineDistance(a: [number, number], b: [number, number]): number;
export function stemmer(word: string): string;
export function createPatch(before: string, after: string): any;
export function unifiedDiff(before: string, after: string): string;
export function applyPatch(before: string, patch: any): string;
export function createDelta(before: Uint8Array, after: Uint8Array): Uint8Array;
export function applyDelta(before: Uint8Array, delta: Uint8Array): Uint8Array;
