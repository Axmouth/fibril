#!/usr/bin/env bash
# Start N REAL fibril-server processes (own ports, own data dirs) and check
# each one's topology through the real fibrilctl CLI over HTTP.
#
# Usage:
#   scripts/cluster-tryout.sh              # 3 standalone nodes, check topology, shut down
#   scripts/cluster-tryout.sh --ganglion   # 3 nodes forming ONE raft coordination cluster
#   scripts/cluster-tryout.sh --staggered   # ganglion mode, nodes join one by one (watch the
#                                           # cluster form: no quorum -> election -> all join)
#   scripts/cluster-tryout.sh --nodes 5
#   scripts/cluster-tryout.sh --keep       # leave the cluster running to play with
#
# In --ganglion mode the servers share an embedded raft coordinator over real
# TCP: the script asserts all nodes agree on one leader and voter set.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NODES=3
KEEP=false
GANGLION=false
STAGGERED=false
BASE_BROKER_PORT=7180
BASE_ADMIN_PORT=7190
BASE_RAFT_PORT=7300

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes) NODES="$2"; shift 2 ;;
    --keep) KEEP=true; shift ;;
    --ganglion) GANGLION=true; shift ;;
    --staggered) GANGLION=true; STAGGERED=true; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

RUN_DIR="$(mktemp -d /tmp/fibril-cluster-tryout.XXXXXX)"
declare -a PIDS=()

cleanup() {
  if [[ "$KEEP" == true ]]; then
    return
  fi
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

echo "building fibril-server and fibrilctl..."
cargo build --quiet -p fibril -p fibril-cli
SERVER=target/debug/fibril-server
CTL=target/debug/fibrilctl

PEERS=""
if [[ "$GANGLION" == true ]]; then
  for i in $(seq 1 "$NODES"); do
    PEERS+="${PEERS:+,}$i=127.0.0.1:$((BASE_RAFT_PORT + i))"
  done
fi

start_node() {
  local i="$1"
  local broker_port=$((BASE_BROKER_PORT + i))
  local admin_port=$((BASE_ADMIN_PORT + i))
  local env_vars=(
    "FIBRIL_DATA_DIR=$RUN_DIR/node-$i"
    "FIBRIL_BROKER_BIND=127.0.0.1:$broker_port"
    "FIBRIL_ADMIN_BIND=127.0.0.1:$admin_port"
  )
  if [[ "$GANGLION" == true ]]; then
    env_vars+=(
      "FIBRIL_COORDINATION_MODE=ganglion"
      "FIBRIL_COORDINATION_NODE_ID=broker-$i"
      "FIBRIL_COORDINATION_RAFT_ID=$i"
      "FIBRIL_COORDINATION_LISTEN=127.0.0.1:$((BASE_RAFT_PORT + i))"
      "FIBRIL_COORDINATION_PEERS=$PEERS"
    )
    if [[ "$i" -eq 1 ]]; then
      env_vars+=("FIBRIL_COORDINATION_BOOTSTRAP=true")
    fi
  fi
  env "${env_vars[@]}" "$SERVER" >"$RUN_DIR/node-$i.log" 2>&1 &
  PIDS+=("$!")
  echo "  node-$i: broker=127.0.0.1:$broker_port admin=127.0.0.1:$admin_port pid=${PIDS[-1]}"
}

wait_admin() {
  local i="$1"
  local admin_port=$((BASE_ADMIN_PORT + i))
  for attempt in $(seq 1 50); do
    curl -sf "http://127.0.0.1:$admin_port/healthz" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "FAIL: node-$i admin endpoint never came up; log tail:" >&2
  tail -20 "$RUN_DIR/node-$i.log" >&2
  exit 1
}

show_node() {
  local i="$1"
  "$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + $1))" admin topology --json 2>/dev/null \
    | jq -c '{leader: .raft.leader, voters: .raft.voters, healthy: .raft.healthy, brokers: (.coordination.nodes | length)}'
}

if [[ "$STAGGERED" == true ]]; then
  echo "staggered join: starting node 1 alone (bootstrap) under $RUN_DIR"
  start_node 1
  wait_admin 1
  sleep 1.5
  echo "  node-1 alone (no quorum expected): $(show_node 1)"

  echo "starting node 2 — quorum forms, election runs"
  start_node 2
  wait_admin 2
  for attempt in $(seq 1 50); do
    leader="$(show_node 1 | jq -r '.leader')"
    [[ "$leader" != "null" && -n "$leader" ]] && break
    sleep 0.3
  done
  echo "  after node-2 joined: $(show_node 1)"

  for i in $(seq 3 "$NODES"); do
    echo "starting node $i — joins the running cluster"
    start_node "$i"
    wait_admin "$i"
  done
else
  echo "starting $NODES fibril servers under $RUN_DIR (ganglion=$GANGLION)"
  for i in $(seq 1 "$NODES"); do
    start_node "$i"
  done
fi

echo "waiting for admin endpoints..."
for i in $(seq 1 "$NODES"); do
  wait_admin "$i"
done

if [[ "$GANGLION" == true ]]; then
  echo "waiting for raft election and broker registration..."
  for attempt in $(seq 1 80); do
    json="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + 1))" admin topology --json 2>/dev/null || echo '{}')"
    leader="$(echo "$json" | jq -r '.raft.leader')"
    registered="$(echo "$json" | jq -r '.coordination.nodes | length')"
    if [[ "$leader" != "null" && -n "$leader" && "$registered" -ge "$NODES" ]]; then
      break
    fi
    sleep 0.3
  done
fi

echo
FAILED=0
declare -a LEADERS=()
declare -a VOTERS=()
for i in $(seq 1 "$NODES"); do
  broker_port=$((BASE_BROKER_PORT + i))
  admin_port=$((BASE_ADMIN_PORT + i))
  echo "=== node-$i topology (fibrilctl --admin 127.0.0.1:$admin_port admin topology) ==="
  "$CTL" --admin "127.0.0.1:$admin_port" admin topology || FAILED=1

  json="$("$CTL" --admin "127.0.0.1:$admin_port" admin topology --json)"
  if [[ "$GANGLION" == true ]]; then
    # Shared-cluster checks: a real raft block with one leader and all voters.
    leader="$(echo "$json" | jq -r '.raft.leader')"
    voters="$(echo "$json" | jq -c '.raft.voters')"
    if [[ "$leader" == "null" ]]; then
      echo "FAIL: node-$i reports no raft leader" >&2
      FAILED=1
    fi
    LEADERS+=("$leader")
    VOTERS+=("$voters")
    # Every broker must have self-registered into the shared node table.
    registered="$(echo "$json" | jq -r '.coordination.nodes | length')"
    if [[ "$registered" -lt "$NODES" ]]; then
      echo "FAIL: node-$i sees only $registered/$NODES registered brokers" >&2
      FAILED=1
    fi
  else
    # Standalone check: the node reports its own broker address.
    reported="$(echo "$json" | jq -r '.coordination.nodes[0].broker_addr')"
    if [[ "$reported" != "127.0.0.1:$broker_port" ]]; then
      echo "FAIL: node-$i reported broker_addr=$reported, expected 127.0.0.1:$broker_port" >&2
      FAILED=1
    fi
  fi
  echo
done

if [[ "$GANGLION" == true ]]; then
  for value in "${LEADERS[@]}"; do
    if [[ "$value" != "${LEADERS[0]}" ]]; then
      echo "FAIL: nodes disagree on the raft leader: ${LEADERS[*]}" >&2
      FAILED=1
    fi
  done
  for value in "${VOTERS[@]}"; do
    if [[ "$value" != "${VOTERS[0]}" ]]; then
      echo "FAIL: nodes disagree on the voter set: ${VOTERS[*]}" >&2
      FAILED=1
    fi
  done
  if [[ "$FAILED" -eq 0 ]]; then
    echo "shared cluster confirmed: leader=${LEADERS[0]} voters=${VOTERS[0]} on all $NODES nodes"
  fi
fi

if [[ "$FAILED" -ne 0 ]]; then
  echo "cluster-tryout: FAILED"
  exit 1
fi

echo "cluster-tryout: all $NODES servers serve topology correctly"
if [[ "$KEEP" == true ]]; then
  echo
  echo "cluster left running (--keep). Try:"
  for i in $(seq 1 "$NODES"); do
    echo "  $CTL --admin 127.0.0.1:$((BASE_ADMIN_PORT + i)) admin topology"
  done
  echo "logs/data: $RUN_DIR ; stop with: kill ${PIDS[*]}"
fi
