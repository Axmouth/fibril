#!/usr/bin/env bash
# Plexus fan-out benchmark with one OS process per reader and per writer, so each
# role runs on its own core (a single process shares one event loop or one
# publisher goroutine and understates fan-out, especially for the event-loop
# clients). Launches WRITERS publisher processes and READERS stream-subscriber
# processes against one stream, then aggregates publish and deliver throughput.
#
# Usage:
#   CLIENT=go READERS=3 WRITERS=1 clients/bench-plexus-fanout.sh
#   CLIENT=python READERS=4 DURATION=10 clients/bench-plexus-fanout.sh
#
# Starts a fresh release broker on tmpfs unless you point it at your own:
#   FIBRIL_ADDR=127.0.0.1:9876 FIBRIL_EXTERNAL_BROKER=1 CLIENT=ts clients/bench-plexus-fanout.sh
#
# Env: CLIENT (go|python|ts), READERS, WRITERS, DURATION (s), WARMUP (s), SIZE,
# PARTITIONS, PREFETCH, DURABILITY (ephemeral|speculative|durable), TOPIC.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
CLIENT="${CLIENT:-go}"
READERS="${READERS:-3}"
WRITERS="${WRITERS:-1}"
DURATION="${DURATION:-8}"
WARMUP="${WARMUP:-2}"
SIZE="${SIZE:-1024}"
PARTITIONS="${PARTITIONS:-1}"
PREFETCH="${PREFETCH:-4096}"
DURABILITY="${DURABILITY:-durable}"
TOPIC="${TOPIC:-benchplexusfanout.$$}"
PORT="${FIBRIL_PORT:-9876}"
ADDR="${FIBRIL_ADDR:-127.0.0.1:$PORT}"

case "$CLIENT" in
  go)     DIR="$REPO_ROOT/clients/go";         RUN=(go run ./examples/bench-plexus);        DUR_ENV="DURATION_S=$DURATION WARMUP_S=$WARMUP" ;;
  python) DIR="$REPO_ROOT/clients/python";     RUN=(uv run --quiet python examples/bench_plexus.py); DUR_ENV="DURATION_S=$DURATION WARMUP_S=$WARMUP" ;;
  ts)     DIR="$REPO_ROOT/clients/typescript"; RUN=(npx --yes tsx examples/bench-plexus.ts); DUR_ENV="DURATION_MS=$((DURATION * 1000)) WARMUP_MS=$((WARMUP * 1000))" ;;
  *) echo "unknown CLIENT=$CLIENT (want go|python|ts)" >&2; exit 2 ;;
esac

if [[ "${FIBRIL_EXTERNAL_BROKER:-0}" != "1" ]]; then
  SERVER="$REPO_ROOT/target/release/fibril-server"
  [[ -x "$SERVER" ]] || { echo "building release broker..."; (cd "$REPO_ROOT" && cargo build -q --release -p fibril-server); }
  DATA_DIR="$(mktemp -d "${TMPDIR:-/dev/shm}/fibrilbench.XXXXXX" 2>/dev/null || mktemp -d)"
  FIBRIL_DATA_DIR="$DATA_DIR" FIBRIL_BROKER_BIND="127.0.0.1:$PORT" FIBRIL_ADMIN_BIND="127.0.0.1:$((PORT + 1))" \
    "$SERVER" >"$DATA_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  trap 'kill "$SERVER_PID" 2>/dev/null || true; rm -rf "$DATA_DIR"' EXIT
  for _ in $(seq 1 60); do (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && { exec 3>&- 3<&-; break; }; sleep 0.3; done
fi

OUT="$(mktemp -d)"; trap 'rm -rf "$OUT"' RETURN 2>/dev/null || true
common="FIBRIL_ADDR=$ADDR $DUR_ENV SIZE=$SIZE TOPIC=$TOPIC PARTITIONS=$PARTITIONS PREFETCH=$PREFETCH DURABILITY=$DURABILITY"
pids=()

echo "client=$CLIENT readers=$READERS writers=$WRITERS topic=$TOPIC durability=$DURABILITY size=$SIZE duration=${DURATION}s"

# Readers first, so they are attached (StreamLatest) before any writer publishes.
for i in $(seq 1 "$READERS"); do
  ( cd "$DIR" && env $common MODE=sub CONSUMERS=1 "${RUN[@]}" >"$OUT/r$i.out" 2>&1 ) & pids+=($!)
done
sleep 2
for i in $(seq 1 "$WRITERS"); do
  ( cd "$DIR" && env $common MODE=pub "${RUN[@]}" >"$OUT/w$i.out" 2>&1 ) & pids+=($!)
done
wait "${pids[@]}"

sum() { grep -ho "$1=[0-9]*" "$OUT"/$2*.out 2>/dev/null | cut -d= -f2 | paste -sd+ | bc; }
pub="$(sum publish w)"; del="$(sum deliver r)"
echo "-------------------------------------"
echo "total publish  = ${pub:-0}  msgs/s  (across $WRITERS writer process(es))"
echo "total deliver  = ${del:-0}  msgs/s  (fan-out across $READERS reader process(es))"
