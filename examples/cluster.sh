#!/usr/bin/env bash
#
# A three-process nisaba cluster, from nothing to documents.
#
#   ./examples/cluster.sh
#
# Every step below is a command you could type yourself, in order, and
# the script is written to be read that way as much as run that way.
# Run it whole, or copy the lines out one at a time.
#
# What it shows: three processes elect a leader, a write goes through the
# log before it is applied, every member ends up with the same documents,
# only the leader will answer, and the cluster survives losing one.
#
# It uses loopback ports 8097-8099 (clients) and 9001-9003 (peers), and a
# temporary directory it removes on the way out.

set -euo pipefail
cd "$(dirname "$0")/.."      # the repo root: every path below is from there

SERVER=wasm/lib/nisaba-server
DIR=$(mktemp -d "${TMPDIR:-/tmp}/nisaba-cluster.XXXXXX")

# The cluster, written down once: node id, the port CLIENTS use, and the
# port the OTHER MEMBERS use. Everything below derives from this table.
NODES="1:8097:9001 2:8098:9002 3:8099:9003"
MEMBERS=$(for n in $NODES; do echo -n "127.0.0.1:$(echo "$n" | cut -d: -f2) "; done)

field()  { echo "$1" | cut -d: -f"$2"; }              # field <a:b:c> <n>
port_of()   { field "$1" 2; }
node_for() {   # node_for HOST:PORT -> the "id:port:raftport" row
  local want=${1##*:} n
  for n in $NODES; do [ "$(port_of "$n")" = "$want" ] && { echo "$n"; return 0; }; done
  return 1
}
dir_for()  { echo "$DIR/node$(field "$(node_for "$1")" 1)"; }

step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
# Echo the command the way you would type it, then run it.
run() {
  printf '\033[2m$'
  for a in "$@"; do
    case "$a" in *[\ \{\}\"\$]*) printf " '%s'" "$a" ;; *) printf ' %s' "$a" ;; esac
  done
  printf '\033[0m\n'
  "$@"
}

# RUNNING is "port:pid ..." -- one process per member, and stopping a
# member is stopping its process. Nothing else has to be told.
RUNNING=""
stop_member() {
  local pid=$1
  # The braces keep bash's own "Terminated" job notice off the transcript.
  { kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; } 2>/dev/null
}
pid_of() {   # pid_of HOST:PORT
  local want=${1##*:} pair
  for pair in $RUNNING; do
    [ "${pair%%:*}" = "$want" ] && { echo "${pair##*:}"; return 0; }
  done
  return 1
}

cleanup() {
  step "Stopping the cluster"
  local pair
  for pair in $RUNNING; do stop_member "${pair##*:}"; done
  rm -rf "$DIR"
  echo "removed $DIR"
}
# INT and TERM too, and PIPE: `./examples/cluster.sh | head` would
# otherwise kill the script before it stopped anything, and leave three
# servers holding the ports it says are free.
trap cleanup EXIT INT TERM PIPE

# ---------------------------------------------------------------------
step "Checking the ports are free"
#
# One process owns one directory, and one listener owns one port. If
# something is already on these, this script would quietly talk to it
# instead of to the cluster it thinks it started -- which looks like a
# cluster with somebody else's documents in it. So it refuses.
busy=""
for m in $MEMBERS; do
  for p in "${m##*:}" "$(field "$(node_for "$m")" 3)"; do
    if nc -z 127.0.0.1 "$p" 2>/dev/null; then busy="$busy $p"; fi
  done
done
command -v nc >/dev/null || echo "  (no nc; skipping the check -- ports may be in use)"
if [ -n "$busy" ]; then
  echo "Ports already in use:$busy"
  echo "Something is listening there -- an earlier run, perhaps. Stop it first:"
  echo "  pkill -f 'nisaba-server --port 809'"
  exit 1
fi
echo "  8097-8099 (clients) and 9001-9003 (peers) are free"

# ---------------------------------------------------------------------
step "Building the server (once)"
# The native build needs nothing but a cc. The deployment target is
# wasm32-wasip2 -- ./wasm/build-server.sh with no flag builds that, and
# it is run with: wasmtime run -S inherit-network --dir DIR::. ...
[ -x "$SERVER" ] || run ./wasm/build-server.sh --native
echo "$SERVER is ready"

# ---------------------------------------------------------------------
step "Starting three members"
#
# Each member gets:
#   --raft ID        who it is in the cluster
#   --raft-port N    where the OTHER MEMBERS reach it (not clients)
#   --peer ID@ADDR   another member and where to reach IT
#   --port N         where CLIENTS reach it
#
# The client port and the peer port are separate listeners speaking
# separate grammars. Every member is given the same --peer set, because a
# member missing from one node's list is a vote that node will never
# count. (That is only how a cluster is BOOTSTRAPPED: once the log holds
# a membership entry it is the truth, and a restart needs no --peer at
# all. `--join ADDRESS` adds a member to a cluster already running.)
#
# Each directory starts EMPTY. Nothing is copied between them: the three
# become one database because the log makes them so.
start_member() {
  local id=$1 port=$2 raft_port=$3; shift 3
  # The DIRECTORY is what makes this member's data its own, and the
  # server takes it from where it is standing -- there is no --dir flag,
  # because under WASI the host grants the directory and calls it ".".
  # One process owns one directory for its lifetime, which is the whole
  # answer to concurrent writers.
  mkdir -p "$DIR/node$id"
  ( cd "$DIR/node$id" && exec "$OLDPWD/$SERVER" \
      --port "$port" --raft "$id" --raft-port "$raft_port" "$@" \
  ) > "$DIR/node$id.log" 2>&1 &
  RUNNING="$RUNNING $port:$!"
  echo "  node $id: clients on $port, peers on $raft_port, data in $DIR/node$id"
}

# Nothing is served until the listener is bound, and the server says so
# on stderr when it is. Waiting for the LINE rather than for a sleep is
# what makes this reliable on a slow machine.
wait_for_member() {
  local id=$1
  for _ in $(seq 1 150); do
    grep -q '^nisaba: serving' "$DIR/node$id.log" 2>/dev/null && return 0
    sleep 0.2
  done
  echo "node $id never started; its log says:"; cat "$DIR/node$id.log"; exit 1
}

for me in $NODES; do
  peers=""
  for other in $NODES; do
    [ "$other" = "$me" ] && continue
    peers="$peers --peer $(field "$other" 1)@127.0.0.1:$(field "$other" 3)"
  done
  # shellcheck disable=SC2086
  start_member $(field "$me" 1) $(field "$me" 2) $(field "$me" 3) $peers
done
for me in $NODES; do wait_for_member "$(field "$me" 1)"; done

# ---------------------------------------------------------------------
step "Waiting for an election"
#
# `ping` is the one op every member answers, and on a replica it says
# what that member is. Reads and writes belong to the LEADER, so this is
# also how you watch a cluster: see examples/who-leads.mjs.
LEADER=""
for _ in $(seq 1 100); do
  if LEADER=$(node examples/who-leads.mjs $MEMBERS 2>/dev/null); then break; fi
  sleep 0.2
done
node examples/who-leads.mjs $MEMBERS >/dev/null    # print the table
[ -n "$LEADER" ] || { echo "no leader; see $DIR/node*.log"; exit 1; }
echo "  -> the leader is $LEADER"

# ---------------------------------------------------------------------
step "Inserting documents"
#
# `db --server ADDR <database> <command> ...` is the same CLI that works
# on a local directory; --server just points it at a process. The
# database is named per request, so one connection reaches all of them --
# `main` here is made on first use, like a collection is.
#
# A write on a replicated server is answered only after it has been
# through the log and a quorum holds it. That is the latency you are
# paying for surviving a machine.
run node bin/db.js --server "$LEADER" main insert users '{"name":"Ada","team":"core"}'
run node bin/db.js --server "$LEADER" main insert users '{"name":"Grace","team":"core"}'
run node bin/db.js --server "$LEADER" main insert users '{"name":"Edsger","team":"research"}'

# ---------------------------------------------------------------------
step "Reading them back"
#
# Reads are linearizable: this sees everything committed before it was
# asked, so a client always reads its own writes. The leader proves it
# still leads (one round to a quorum) before answering.
run node bin/db.js --server "$LEADER" main find users
run node bin/db.js --server "$LEADER" main count users
run node bin/db.js --server "$LEADER" main find users '{"team":"core"}'

# ---------------------------------------------------------------------
step "Asking a follower instead"
#
# It refuses, and says who leads and at what address -- it does not
# forward. A server holding a request it cannot promise anything about
# would be worse than a refusal a client can act on. `ServerError`
# carries `.code` (-63), `.leaderId` and `.leader`, so following the
# redirect is one line in a real client.
FOLLOWER=$(for m in $MEMBERS; do [ "$m" = "$LEADER" ] || { echo "$m"; break; }; done)
echo "  asking $FOLLOWER, which is not the leader:"
run node bin/db.js --server "$FOLLOWER" main count users || true

# ---------------------------------------------------------------------
step "Every member holds the same log"
#
# Followers do not serve data, so what they will tell you is how far
# their apply pump has got. All three converge on the same index.
sleep 1
node examples/who-leads.mjs $MEMBERS >/dev/null

# ---------------------------------------------------------------------
step "Killing the leader"
#
# Two of three is still a quorum, so the survivors elect a new leader and
# its log already holds everything the old one committed -- that is what
# the election rules buy.
echo "  killing the member serving $LEADER"
stop_member "$(pid_of "$LEADER")"
RUNNING=$(for pair in $RUNNING; do
            [ "${pair%%:*}" = "${LEADER##*:}" ] || echo -n "$pair "; done)

SURVIVORS=$(for m in $MEMBERS; do [ "$m" = "$LEADER" ] || echo -n "$m "; done)
NEW=""
for _ in $(seq 1 100); do
  if NEW=$(node examples/who-leads.mjs $SURVIVORS 2>/dev/null); then break; fi
  sleep 0.2
done
node examples/who-leads.mjs $SURVIVORS >/dev/null
[ -n "$NEW" ] || { echo "no new leader; see $DIR/node*.log"; exit 1; }
echo "  -> the new leader is $NEW"

step "The documents are still there, and it still takes writes"
run node bin/db.js --server "$NEW" main count users
run node bin/db.js --server "$NEW" main insert users '{"name":"Alan","team":"research"}'
run node bin/db.js --server "$NEW" main find users '{"team":"research"}'

# ---------------------------------------------------------------------
step "Adding a fourth member, which knows one ADDRESS"
#
# No ids, no member list: it asks a seed, is redirected to the leader if
# that seed is not it, and is admitted. It enters as a LEARNER -- it
# replicates and applies but does not vote -- and the leader promotes it
# once its own bookkeeping proves the new member has caught up. Adding
# capacity never thins the failure margin in between.
#
# The seed address is a member's PEER port, not its client port.
mkdir -p "$DIR/node4"
( cd "$DIR/node4" && exec "$OLDPWD/$SERVER" \
    --port 8100 --raft 4 --raft-port 9004 --join 127.0.0.1:9002 \
) > "$DIR/node4.log" 2>&1 &
RUNNING="$RUNNING 8100:$!"
NODES="$NODES 4:8100:9004"
wait_for_member 4
echo "  node 4 joined; it holds the writes made before it existed:"
sleep 1
node examples/who-leads.mjs $SURVIVORS 127.0.0.1:8100 >/dev/null

# A restart needs neither --join nor --peer: its own log says who its
# cluster is. To remove it, ask any member -- this is a one-shot command
# that exits rather than a server:
step "...and removing it again"
run "$SERVER" --leave 4 --join 127.0.0.1:9002
stop_member "$(pid_of 127.0.0.1:8100)"
RUNNING=$(for pair in $RUNNING; do
            [ "${pair%%:*}" = "8100" ] || echo -n "$pair "; done)

# ---------------------------------------------------------------------
step "What is on disk"
#
# One directory per member, a subdirectory per database, and the log
# beside them. Any of these directories opens with the JavaScript
# implementation, or with `db` without --server -- the same files.
run ls -1 "$(dir_for "$NEW")"
run ls -1 "$(dir_for "$NEW")/main"

echo
echo "Done. The trap below stops the members and removes $DIR."
