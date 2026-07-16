#!/usr/bin/env bash
# Broker memory regression probe: repeated load -> drain -> evict cycles
# against one broker, reporting settled RSS per cycle. Findings and the
# reasoning behind the thresholds live in MEMORY_AUDIT.md.
#
#   scripts/memory-audit.sh                 # 2 cycles, report only
#   scripts/memory-audit.sh --check         # non-zero exit on regression
#   QUEUES=8 MSGS=50000 CYCLES=4 scripts/memory-audit.sh
#
# --check fails when eviction never completes or a later cycle's settled
# RSS exceeds the first cycle's by more than 25% (the one-time warm cost is
# by design, cycle-over-cycle stability is the invariant). Requires release
# builds of fibril-server and e2e_c. Uses a temp data dir, removed on exit.
set -uo pipefail
cd "$(dirname "$0")/.."

CHECK=false
[ "${1:-}" = "--check" ] && CHECK=true
QUEUES="${QUEUES:-32}"
MSGS="${MSGS:-200000}"
CYCLES="${CYCLES:-2}"
SETTLE_SECS="${SETTLE_SECS:-300}"
BROKER=127.0.0.1:19893
ADMIN=http://127.0.0.1:18093

D=$(mktemp -d)
SRV=""
cleanup() {
  [ -n "$SRV" ] && kill "$SRV" 2>/dev/null
  sleep 1
  [ -n "$SRV" ] && kill -9 "$SRV" 2>/dev/null
  rm -rf "$D"
}
trap cleanup EXIT

for bin in target/release/fibril-server target/release/e2e_c; do
  [ -x "$bin" ] || { echo "missing $bin - build with: cargo build --release" >&2; exit 2; }
done

env FIBRIL_DATA_DIR="$D/data" \
  FIBRIL_BROKER_BIND=$BROKER \
  FIBRIL_ADMIN_BIND=127.0.0.1:18093 \
  FIBRIL_QUEUE_IDLE_EVICT_AFTER_MS=60000 \
  FIBRIL_QUEUE_IDLE_SWEEP_INTERVAL_MS=10000 \
  target/release/fibril-server >"$D/server.log" 2>&1 &
SRV=$!
for _ in $(seq 1 30); do
  curl -sf "$ADMIN/healthz" >/dev/null 2>&1 && break
  kill -0 "$SRV" 2>/dev/null || { echo "server died at boot (see $D/server.log)" >&2; exit 2; }
  sleep 1
done

rss() { ps -o rss= -p "$SRV" | tr -d ' '; }
sleep 30
echo "baseline: $(rss) KB"

fail=0
first_settled=""
for cycle in $(seq 1 "$CYCLES"); do
  for i in $(seq 1 "$QUEUES"); do
    target/release/e2e_c --addr $BROKER --writer -c 1 -m "$MSGS" -s 256 --topic "audit-$i" >/dev/null 2>&1
  done
  for i in $(seq 1 "$QUEUES"); do
    target/release/e2e_c --addr $BROKER --reader --firehose -c 1 -m "$MSGS" --topic "audit-$i" --idle-timeout-ms 8000 >/dev/null 2>&1
  done
  evicted=false
  for _ in $(seq 1 60); do
    mat=$(curl -s "$ADMIN/admin/api/queues_debug" | python3 -c "
import json, sys
d = json.load(sys.stdin)
qs = d if isinstance(d, list) else d.get('queues', [])
print(sum(1 for q in qs if str(q.get('topic', '')).startswith('audit-') and q.get('materialized')))
" 2>/dev/null || echo "?")
    if [ "$mat" = "0" ]; then evicted=true; break; fi
    sleep 10
  done
  if [ "$evicted" != true ]; then
    echo "cycle $cycle: EVICTION DID NOT COMPLETE"
    fail=1
    break
  fi
  sleep "$SETTLE_SECS"
  settled=$(rss)
  echo "cycle $cycle settled: $settled KB"
  if [ -z "$first_settled" ]; then
    first_settled=$settled
  elif [ "$settled" -gt $((first_settled * 125 / 100)) ]; then
    echo "cycle $cycle settled RSS is more than 25% over cycle 1 ($settled vs $first_settled KB)"
    fail=1
  fi
done

if [ "$CHECK" = true ] && [ "$fail" -ne 0 ]; then
  echo "MEMORY REGRESSION CHECK FAILED"
  exit 1
fi
[ "$fail" -eq 0 ] && echo "memory profile stable across $CYCLES cycle(s)"
exit 0
