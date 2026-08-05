/**
 * db-names.test.js — the file-naming scheme and format stamp, through the
 * JS surface.
 *
 * The scheme moved into C (engine/src/db_names.c) so the catalog, the
 * compaction generation flip and the orphan sweep can follow it there --
 * a host that cannot name a file cannot own a catalog. These assertions
 * are the names the JS produced before the move; test/native/main.c
 * asserts the same strings one layer down against the C directly.
 *
 * The orphan-sweep cases carry the most weight: isDbFile decides what
 * Db.open() is allowed to DELETE, so a false positive on the catalog file
 * or a host's own file is data loss, not a cosmetic bug.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import {
  ready, dbCatalogFile, dbFormatKey, dbFormatVersion,
  collectionFileName, indexFileName, textIndexFileNames, journalFileName, isDbFile
} from '../src/nisaba-wasm.js';

beforeAll(async () => { await ready(); });

describe('db-names: constants', () => {
  it('keeps the catalog file name and format key the JS constants had', () => {
    expect(dbCatalogFile()).toBe('__catalog__.bj');
    expect(dbFormatKey()).toBe('__format__');
  });

  it('reports the on-disk format version from C, not a JS constant', () => {
    expect(dbFormatVersion()).toBe(1);
  });
});

describe('db-names: file names', () => {
  it('names gen-0 files exactly as before', () => {
    expect(collectionFileName('users')).toBe('coll-users.bj');
    expect(indexFileName('users', 'team_1')).toBe('idx-users-team_1.bj');
    expect(journalFileName('users')).toBe('coll-users-journal.bj');
    expect(textIndexFileNames('posts', 'body_text')).toEqual({
      index: 'idx-posts-body_text-terms.bj',
      docTerms: 'idx-posts-body_text-documents.bj',
      docLengths: 'idx-posts-body_text-lengths.bj'
    });
  });

  it('prefixes later generations', () => {
    expect(collectionFileName('users', 1)).toBe('g1-coll-users.bj');
    expect(collectionFileName('users', 42)).toBe('g42-coll-users.bj');
    expect(indexFileName('users', 'team_1', 3)).toBe('g3-idx-users-team_1.bj');
    expect(journalFileName('users', 2)).toBe('g2-coll-users-journal.bj');
    expect(textIndexFileNames('posts', 'body_text', 5).index)
      .toBe('g5-idx-posts-body_text-terms.bj');
  });

  it('is a prefix, not a suffix, so a dotted name cannot claim a generation', () => {
    // A collection literally named "users.g2" must not collide with
    // generation 2 of "users" -- the reason for the prefix convention.
    expect(collectionFileName('users.g2')).toBe('coll-users.g2.bj');
    expect(collectionFileName('users', 2)).toBe('g2-coll-users.bj');
  });

  it('round-trips non-ASCII collection names', () => {
    expect(collectionFileName('café')).toBe('coll-café.bj');
    expect(collectionFileName('日本語', 1)).toBe('g1-coll-日本語.bj');
  });
});

describe('db-names: isDbFile (what the orphan sweep may delete)', () => {
  it('matches every file this layer creates, at any generation', () => {
    for (const name of [
      'coll-users.bj', 'idx-users-team_1.bj', 'coll-users-journal.bj',
      'g1-coll-users.bj', 'g42-idx-users-team_1.bj', 'g7-coll-users-journal.bj',
      'idx-posts-body_text-terms.bj', 'coll-users.g2.bj'
    ]) expect(isDbFile(name), name).toBe(true);
  });

  it('never matches the catalog, the WAL, or a host file', () => {
    for (const name of ['__catalog__.bj', '__wal__.bj', 'notes.txt', 'README.md']) {
      expect(isDbFile(name), name).toBe(false);
    }
    expect(isDbFile(dbCatalogFile())).toBe(false);
  });

  it('rejects near misses the old regex also rejected', () => {
    for (const name of [
      'coll-users.txt',   // wrong extension
      'collusers.bj',     // no separator
      'xcoll-users.bj',   // prefix not at the start
      'g-coll-users.bj',  // g with no digits
      'g12coll-users.bj', // digits with no dash
      'g1-notes.bj',      // generation on a non-db name
      '', 'coll-', '.bj'
    ]) expect(isDbFile(name), name).toBe(false);
  });

  it('agrees with the regex it replaced across every generated name', () => {
    const OLD = /^(?:g\d+-)?(?:coll|idx)-.*\.bj$/;
    const names = [];
    for (const gen of [0, 1, 9, 10, 12345]) {
      names.push(collectionFileName('users', gen));
      names.push(indexFileName('users', 'team_1', gen));
      names.push(journalFileName('users', gen));
      const t = textIndexFileNames('posts', 'body_text', gen);
      names.push(t.index, t.docTerms, t.docLengths);
    }
    names.push('__catalog__.bj', '__wal__.bj', 'notes.txt', 'g-coll-x.bj', 'coll-.bj');
    for (const n of names) expect(isDbFile(n), n).toBe(OLD.test(n));
  });
});
