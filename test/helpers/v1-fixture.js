/**
 * test/helpers/v1-fixture.js — turn a database this build wrote back
 * into the version-1 one it would have been.
 *
 * There is no version-1 build left to write a fixture with, and hand-
 * built bytes would test this file's idea of the old format rather than
 * the old format. So the fixture is made by DOWNGRADING a real database:
 * a v1 primary tree is the same rows under the raw twelve ObjectId bytes
 * instead of the tagged key form, which is the whole of what format 2
 * changed about it (docs/format-compatibility.md).
 *
 * Three details that are easy to get wrong, and matter:
 *
 * - The rows are rebuilt from the DOCUMENTS, not from the keys read back
 *   out. A raw ObjectId is not UTF-8, and a key that crossed the bridge
 *   as text would come back mauled.
 * - Index files are left exactly as written. That is not laziness: their
 *   composite keys already ended in the tagged id and their rows already
 *   held its value form, so a v1 index file and a v2 one ARE the same
 *   bytes. Rewriting them would make the fixture less faithful.
 * - The cross-file journal is dropped rather than rewritten. Its recorded
 *   lengths describe the primary file this replaces, and a cleanly closed
 *   database has nothing in flight for it to describe anyway.
 */
import { BPlusTree, journalFileName } from '../../src/nisaba-wasm.js';

const CATALOG = '__catalog__.bj';
const ORDER = 32;

/**
 * Downgrade `collections` of the database at `provider` to version 1.
 * Every named collection must hold only ObjectId ids — version 1 had no
 * other kind, so a fixture with one would be a database that never
 * existed.
 *
 * `stamp` defaults to `{ v: 1 }`; pass one explicitly to build the state
 * an interrupted migration leaves (`{ v: 2, migrating: true }`).
 */
export async function downgradeToV1(provider, collections, stamp = { v: 1 }) {
  const catalog = new BPlusTree(await provider.openFile(CATALOG, { create: false }), ORDER);
  await catalog.open();
  try {
    for (const name of collections) {
      const entry = catalog.search(name);
      if (!entry) throw new Error(`downgradeToV1: no catalog entry for "${name}"`);

      const tree = new BPlusTree(await provider.openFile(entry.file, { create: false }), ORDER);
      await tree.open();
      const rows = tree.toArray();
      await tree.close();

      await provider.deleteFile(entry.file);
      const v1 = new BPlusTree(await provider.openFile(entry.file, { create: true }), ORDER);
      await v1.open();
      for (const { value } of rows) {
        if (typeof value?._id?.toBytes !== 'function') {
          throw new Error(`downgradeToV1: "${name}" holds a non-ObjectId _id`);
        }
        v1.add(value._id.toBytes(), value);
      }
      v1.flush();
      await v1.close();
      await provider.deleteFile(entry.journal ?? journalFileName(name));
    }
    catalog.add('__format__', stamp);
    catalog.flush();
  } finally {
    await catalog.close();
  }
}
