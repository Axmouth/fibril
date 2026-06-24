#!/usr/bin/env bash
set -euo pipefail

# Plexus stream steady-state benchmark over full TCP. The stream counterpart of
# bench-steady-c.sh: it starts a broker, runs the `bench-stream` binary against
# it, samples server RSS, and parses the results. Pick the durability tier with
# DURABILITY and the data filesystem with DATA_DIR (e.g. repo SSD vs /dev/shm
# tmpfs) to compare the express lane against the durable default.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
writers="${WRITERS:-4}"
readers="${READERS:-4}"
mode="${BENCH_MODE:-mixed}"
durability="${DURABILITY:-durable}"
rate_per_sec="${RATE_PER_SEC:-100000}"
warmup_secs="${WARMUP_SECS:-5}"
duration_secs="${DURATION_SECS:-30}"
drain_timeout_secs="${DRAIN_TIMEOUT_SECS:-10}"
size="${SIZE:-1024}"
prefetch="${PREFETCH:-16384}"
partitions="${PARTITIONS:-1}"
confirmed="${CONFIRMED:-0}"
confirm_window="${CONFIRM_WINDOW:-1024}"
build="${BUILD:-1}"
start_server="${START_SERVER:-1}"
broker_addr="${BROKER_ADDR:-127.0.0.1:9876}"
admin_addr="${ADMIN_ADDR:-127.0.0.1:8081}"
topic="${TOPIC:-stream1}"
# Where the broker keeps its data. Defaults to a tmpdir (tmpfs on most boxes);
# point at the repo filesystem for an honest fsync (SSD) measurement.
data_dir="${DATA_DIR:-$(mktemp -d)}"
log_file="${LOG_FILE:-$data_dir/stream.log}"
results_file="${RESULTS_FILE:-$data_dir/stream-results.txt}"
memory_file="$data_dir/server-rss-kib.txt"
bench_args=()
memory_sampler_pid=""
server_pid=""

if [ "$confirmed" != "0" ]; then
  bench_args+=(--confirmed --confirm-window "$confirm_window")
fi

# Give each reader a unique durable cursor name so auto-ack commits its cursor
# per record (exercises the cursor-commit microbatcher).
if [ "${DURABLE_READERS:-0}" != "0" ]; then
  bench_args+=(--durable-readers)
fi

mkdir -p "$data_dir" "$(dirname "$log_file")" "$(dirname "$results_file")"
: >"$log_file"
: >"$results_file"

echo "Stream benchmark log: $log_file"
echo "Stream benchmark results: $results_file"
echo "Stream benchmark data dir: $data_dir"

if [ "$build" != "0" ]; then
  cargo build --manifest-path "$repo_root/Cargo.toml" --release --bin fibril-server --bin bench-stream >>"$log_file" 2>&1
fi

if [ "$start_server" != "0" ]; then
  (
    cd "$data_dir"
    "$repo_root/target/release/fibril-server"
  ) >>"$log_file" 2>&1 &
  server_pid="$!"
fi

cleanup() {
  if [ -n "$memory_sampler_pid" ]; then
    kill "$memory_sampler_pid" >/dev/null 2>&1 || true
    wait "$memory_sampler_pid" >/dev/null 2>&1 || true
  fi
  if [ -n "$server_pid" ]; then
    kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
  fi
  if [ -z "${DATA_DIR:-}" ]; then
    rm -rf "$data_dir"
  fi
}
trap cleanup EXIT

ready=0
for _ in $(seq 1 60); do
  if [ -n "$server_pid" ] && ! kill -0 "$server_pid" >/dev/null 2>&1; then
    echo "Benchmark server exited before becoming healthy. Log tail:" >&2
    tail -n 80 "$log_file" >&2 || true
    exit 1
  fi
  if curl --silent --show-error --fail "http://$admin_addr/healthz" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [ "$ready" != "1" ]; then
  echo "Benchmark server did not become healthy within 60 seconds. Log tail:" >&2
  tail -n 80 "$log_file" >&2 || true
  exit 1
fi

sample_memory() {
  while kill -0 "$server_pid" >/dev/null 2>&1; do
    rss="$(ps -o rss= -p "$server_pid" 2>/dev/null | tr -d ' ')"
    if [ -n "$rss" ]; then
      printf '%s %s\n' "$(date +%s)" "$rss" >>"$memory_file"
    fi
    sleep 1
  done
}

if [ -n "$server_pid" ]; then
  sample_memory &
  memory_sampler_pid="$!"
fi

"$repo_root/target/release/bench-stream" \
  --mode "$mode" \
  --durability "$durability" \
  --broker-addr "$broker_addr" \
  --topic "$topic" \
  --writers "$writers" \
  --readers "$readers" \
  --rate-per-sec "$rate_per_sec" \
  --warmup-secs "$warmup_secs" \
  --duration-secs "$duration_secs" \
  --drain-timeout-secs "$drain_timeout_secs" \
  --size "$size" \
  --prefetch "$prefetch" \
  --partitions "$partitions" \
  "${bench_args[@]}" >>"$results_file" 2>>"$log_file"

if [ -n "$memory_sampler_pid" ]; then
  kill "$memory_sampler_pid" >/dev/null 2>&1 || true
  wait "$memory_sampler_pid" >/dev/null 2>&1 || true
fi

if [ -s "$memory_file" ]; then
  awk '
    BEGIN { sum = 0; count = 0; peak = 0 }
    { rss = $2; sum += rss; count += 1; if (rss > peak) peak = rss }
    END {
      if (count > 0) {
        printf "Server RSS avg MiB: %.1f\n", sum / count / 1024
        printf "Server RSS peak MiB: %.1f\n", peak / 1024
      }
    }
  ' "$memory_file" >>"$results_file"
else
  echo "Server RSS avg MiB: unavailable" >>"$results_file"
  echo "Server RSS peak MiB: unavailable" >>"$results_file"
fi

echo "Stream benchmark summary:"
grep -E '^(Stream steady benchmark:|Sent total:|Confirmed total:|Publish errors:|Confirm errors:|Measured sent:|Measured delivered .*:|Measured publish rate:|Measured deliver rate .*:|Latency .* ms|Server RSS .* MiB:)' "$results_file" || true
echo "Full stream benchmark results: $results_file"
echo "Full stream benchmark log: $log_file"
echo "Runtime warnings/errors in log:"
grep -Ein ' (WARN|ERROR) |panic|failed' "$log_file" || true
