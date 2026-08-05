// End-to-end sanity check of the actual compiled src/nisaba-wasm.js
// (not a mock) against a real in-memory database -- run with:
//   node test/node_smoke.mjs
// after `git submodule update --init && ./build-wasm.sh`.
import { ready, connect, MemoryStorageProvider, ObjectId } from '../src/nisaba-wasm.js';

let failures = 0;
function check(cond, what) {
  if (cond) {
    console.log(`[PASS] ${what}`);
  } else {
    console.log(`[FAIL] ${what}`);
    failures++;
  }
}

await ready();
const db = await connect(new MemoryStorageProvider());
const users = await db.collection('users');

const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core', bio: 'loves compilers' });
check(insertedId instanceof ObjectId, 'insertOne: assigns a real ObjectId');

const found = await users.findOne({ _id: insertedId });
check(found?.name === 'Ada', 'findOne: retrieves the inserted document by _id');

await users.insertOne({ name: 'Grace', team: 'infra', bio: 'loves compiler diagnostics' });
await users.createIndex({ team: 1 });
const byIndex = await users.find({ team: 'core' }).toArray();
check(byIndex.length === 1 && byIndex[0].name === 'Ada', 'find: query hits a secondary equality index');

// $regex -- proves the third_party/regex-engine link is real, not just compiled in unused.
const regexHits = await users.find({ bio: { $regex: 'compiler(?! diagnostics)' } }).toArray();
check(regexHits.length === 1 && regexHits[0].name === 'Ada', '$regex: negative lookahead matches via the linked regex engine');

const updateResult = await users.updateOne({ _id: insertedId }, { $set: { team: 'kernel' } });
check(updateResult.modifiedCount === 1, 'updateOne: modifies the matched document');

const deleteResult = await users.deleteOne({ name: 'Grace' });
check(deleteResult.deletedCount === 1, 'deleteOne: removes the matched document');
check((await users.countDocuments({})) === 1, 'countDocuments: reflects the delete');

await db.close();

console.log(failures === 0 ? `\nAll checks passed.` : `\n${failures} check(s) failed.`);
process.exit(failures === 0 ? 0 : 1);
