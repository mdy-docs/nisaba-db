/*
 * who-leads.mjs — ask every member of a cluster what it is.
 *
 *   node examples/who-leads.mjs 127.0.0.1:8097 127.0.0.1:8098 ...
 *
 * Prints a table to STDERR and the LEADER's address to STDOUT, so a
 * shell script can do:
 *
 *   LEADER=$(node examples/who-leads.mjs "$@")
 *
 * `ping` is the one op every member answers. Reads and writes belong to
 * the leader — a follower is behind by at least a round trip and cannot
 * tell by how much, so it refuses both rather than presenting staleness
 * as authority (docs/db-server.md). That makes ping the way to watch a
 * cluster: it carries what the member IS and how far it has got.
 *
 * `applied` is that member's own floor. It reports and promises nothing:
 * it is precisely the number that may NOT be used to serve a read, which
 * is why serving one takes a quorum round instead.
 */
import { connectServer } from '../src/db-server-client.js';

const addresses = process.argv.slice(2);
if (addresses.length === 0) {
  console.error('usage: node examples/who-leads.mjs HOST:PORT [HOST:PORT ...]');
  process.exit(2);
}

let leader = null;
const rows = [];

for (const address of addresses) {
  let client = null;
  try {
    client = await connectServer(address);
    const s = await client.ping();
    // Unreplicated servers answer `{ pong: true }` and nothing else.
    const role = s.role ?? 'not replicated';
    rows.push(`  ${address.padEnd(20)} ${role.padEnd(10)} ` +
              `applied=${s.applied ?? '-'} commit=${s.commit ?? '-'} ` +
              `leaderId=${s.leaderId ?? '-'}`);
    if (role === 'leader') leader = address;
  } catch (err) {
    rows.push(`  ${address.padEnd(20)} unreachable  (${err.message})`);
  } finally {
    await client?.close().catch(() => {});
  }
}

console.error(rows.join('\n'));
if (!leader) {
  console.error('  -- no leader right now (an election is in progress; try again)');
  process.exit(1);
}
console.log(leader);
