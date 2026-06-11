#!/usr/bin/env bash
# Start N REAL fibril-server processes (own ports, own data dirs) and check
# each one's topology through the real fibrilctl CLI over HTTP.
#
# Usage:
#   scripts/cluster-tryout.sh              # 3 nodes, check topology, shut down
#   scripts/cluster-tryout.sh --nodes 5
#   scripts/cluster-tryout.sh --keep       # leave the cluster running to play with
#
# NOTE: until cluster coordination is config-wired (REPLICATION_PLANNING.md
# phase F3 + a raft wire transport), each server reports its own single-node
# topology. This script is the real-process harness; the same checks become a
# shared-cluster assertion once those land.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NODES=3
KEEP=false
BASE_BROKER_PORT=7180
BASE_ADMIN_PORT=7190

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes) NODES="$2"; shift 2 ;;
    --keep) KEEP=true; shift ;;
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

echo "starting $NODES fibril servers under $RUN_DIR"
for i in $(seq 1 "$NODES"); do
  broker_port=$((BASE_BROKER_PORT + i))
  admin_port=$((BASE_ADMIN_PORT + i))
  FIBRIL_DATA_DIR="$RUN_DIR/node-$i" \
  FIBRIL_BROKER_BIND="127.0.0.1:$broker_port" \
  FIBRIL_ADMIN_BIND="127.0.0.1:$admin_port" \
    "$SERVER" >"$RUN_DIR/node-$i.log" 2>&1 &
  PIDS+=("$!")
  echo "  node-$i: broker=127.0.0.1:$broker_port admin=127.0.0.1:$admin_port pid=${PIDS[-1]}"
done

echo "waiting for admin endpoints..."
for i in $(seq 1 "$NODES"); do
  admin_port=$((BASE_ADMIN_PORT + i))
  for attempt in $(seq 1 50); do
    if curl -sf "http://127.0.0.1:$admin_port/healthz" >/dev/null 2>&1; then
      break
    fi
    if [[ "$attempt" -eq 50 ]]; then
      echo "FAIL: node-$i admin endpoint never came up; log tail:" >&2
      tail -20 "$RUN_DIR/node-$i.log" >&2
      exit 1
    fi
    sleep 0.2
  done
done

echo
FAILED=0
for i in $(seq 1 "$NODES"); do
  broker_port=$((BASE_BROKER_PORT + i))
  admin_port=$((BASE_ADMIN_PORT + i))
  echo "=== node-$i topology (fibrilctl --admin 127.0.0.1:$admin_port admin topology) ==="
  "$CTL" --admin "127.0.0.1:$admin_port" admin topology || FAILED=1

  # Machine check: the JSON must report this node's broker address.
  json="$("$CTL" --admin "127.0.0.1:$admin_port" admin topology --json)"
  reported="$(echo "$json" | jq -r '.coordination.nodes[0].broker_addr')"
  if [[ "$reported" != "127.0.0.1:$broker_port" ]]; then
    echo "FAIL: node-$i reported broker_addr=$reported, expected 127.0.0.1:$broker_port" >&2
    FAILED=1
  fi
  echo
done

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
