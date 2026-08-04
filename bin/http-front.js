#!/usr/bin/env node
/**
 * The HTTP front end (src/db-http-front.js) as a process: HTTP on one
 * side, binjson frames over TCP to one nisaba-server — or to every
 * member of a cluster of them — on the other. No engine in this process;
 * it holds sockets, follows the leader, and translates JSON.
 *
 *     db-http --listen 8080 --server 127.0.0.1:8097
 *     db-http --listen 0.0.0.0:8080 \
 *             --server 127.0.0.1:9001 --server 127.0.0.1:9002 --server 127.0.0.1:9003
 *
 * With several --server flags, requests reach whichever member currently
 * leads; the flags are the client addresses of the members, and every
 * member should be listed — an unlisted member that becomes leader is
 * one this process cannot find.
 */
import { DbHttpFront } from '../src/db-http-front.js';

function usage() {
  console.error(`Usage: db-http --listen [host:]port --server host:port [--server host:port ...]

Options:
  --listen [host:]port   Address to serve HTTP on. Host defaults to
                         127.0.0.1 -- loopback, deliberately; widen it
                         consciously.
  --server host:port     A nisaba-server's client address. Repeat it for
                         a cluster: one flag per member.
  --retry-ms n           How long a request chases a moved leader before
                         the refusal is the answer (default 8000).

The URL grammar and everything else: docs/http-front.md.`);
  process.exit(1);
}

const servers = [];
let listen = null;
let retryMs = 8000;
const argv = process.argv.slice(2);
for (let i = 0; i < argv.length; i++) {
  const arg = argv[i];
  if (arg === '-h' || arg === '--help') usage();
  else if (arg === '--server' && argv[i + 1]) servers.push(argv[++i]);
  else if (arg === '--listen' && argv[i + 1]) listen = argv[++i];
  else if (arg === '--retry-ms' && argv[i + 1]) retryMs = Number(argv[++i]);
  else {
    console.error(`Error: unknown or incomplete argument '${arg}'`);
    usage();
  }
}
if (!listen || servers.length === 0) usage();

let listenHost = '127.0.0.1';
let listenPort = listen;
if (String(listen).includes(':')) {
  const i = String(listen).lastIndexOf(':');
  listenHost = listen.slice(0, i) || '127.0.0.1';
  listenPort = listen.slice(i + 1);
}
listenPort = Number(listenPort);
if (!Number.isInteger(listenPort) || listenPort < 0 || listenPort > 65535) {
  console.error(`Error: not a listen address: '${listen}'`);
  usage();
}

const front = new DbHttpFront(servers, { listenPort, listenHost, retryMs });
await front.start();
const bound = front.address();
// "serving" on stderr, the word the server prints when its listener is
// up -- tests and scripts wait for it the same way for both processes.
console.error(`db-http: serving http on ${bound.address}:${bound.port} for ${servers.join(', ')}`);

const bye = async () => { await front.stop(); process.exit(0); };
process.on('SIGINT', bye);
process.on('SIGTERM', bye);
