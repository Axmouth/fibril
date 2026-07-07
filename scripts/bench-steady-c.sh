#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
writers="${WRITERS:-10}"
readers="${READERS:-10}"
mode="${BENCH_MODE:-mixed}"
rate_per_sec="${RATE_PER_SEC:-100000}"
warmup_secs="${WARMUP_SECS:-5}"
duration_secs="${DURATION_SECS:-30}"
drain_timeout_secs="${DRAIN_TIMEOUT_SECS:-10}"
size="${SIZE:-1024}"
prefetch="${PREFETCH:-16384}"
confirmed="${CONFIRMED:-0}"
confirm_window="${CONFIRM_WINDOW:-1024}"
build="${BUILD:-1}"
start_server="${START_SERVER:-1}"
broker_addr="${BROKER_ADDR:-127.0.0.1:9876}"
admin_addr="${ADMIN_ADDR:-127.0.0.1:8081}"
durability_label="${DURABILITY_LABEL:-local}"
topic="${TOPIC:-topic1}"
preload_messages="${PRELOAD_MESSAGES:-100000}"
preload_confirmed="${PRELOAD_CONFIRMED:-1}"
data_dir="$(mktemp -d)"
log_file="${LOG_FILE:-$data_dir/steady.log}"
results_file="${RESULTS_FILE:-$data_dir/steady-results.txt}"
memory_file="$data_dir/server-rss-kib.txt"
# Where the started server keeps its segments (defaults to the work dir). Point it
# at a specific device (e.g. an nvme scratch mount) to benchmark that storage.
server_data_dir="${SERVER_DATA_DIR:-$data_dir}"
# Optional server config file (e.g. to set storage.keratin.segment_preallocate_bytes).
server_config="${SERVER_CONFIG:-}"
bench_args=()
memory_sampler_pid=""
server_pid=""

if [ "$confirmed" != "0" ]; then
  bench_args+=(--confirmed)
fi

if [ "$confirmed" != "0" ] || [ "$preload_confirmed" != "0" ]; then
  bench_args+=(--confirm-window "$confirm_window")
fi

if [ "$preload_confirmed" = "0" ]; then
  bench_args+=(--preload-unconfirmed)
fi

mkdir -p "$(dirname "$log_file")" "$(dirname "$results_file")"
: >"$log_file"
: >"$results_file"

echo "Steady benchmark log: $log_file"
echo "Steady benchmark results: $results_file"

if [ "$build" != "0" ]; then
  cargo build --manifest-path "$repo_root/Cargo.toml" --release --bin fibril-server --bin steady_c >>"$log_file" 2>&1
fi

if [ "$start_server" != "0" ]; then
  mkdir -p "$server_data_dir"
  server_cmd=("$repo_root/target/release/fibril-server" --data-dir "$server_data_dir")
  if [ -n "$server_config" ]; then
    server_cmd+=(--config "$server_config")
  fi
  (
    cd "$data_dir"
    "${server_cmd[@]}"
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
  rm -rf "$data_dir"
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

curl --silent --show-error --fail "http://$admin_addr/healthz" >>"$log_file" 2>&1

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

"$repo_root/target/release/steady_c" \
  --mode "$mode" \
  --broker-addr "$broker_addr" \
  --durability-label "$durability_label" \
  --topic "$topic" \
  --writers "$writers" \
  --readers "$readers" \
  --rate-per-sec "$rate_per_sec" \
  --warmup-secs "$warmup_secs" \
  --duration-secs "$duration_secs" \
  --drain-timeout-secs "$drain_timeout_secs" \
  --size "$size" \
  --prefetch "$prefetch" \
  --preload-messages "$preload_messages" \
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

{
  echo "--- queue snapshot: after steady run ---"
  curl --silent --show-error --fail "http://$admin_addr/admin/api/queues" \
    | jq --compact-output .
  echo "--- queue debug: after steady run ---"
  curl --silent --show-error --fail "http://$admin_addr/admin/api/queues_debug" \
    | jq --compact-output .
} >>"$results_file"

echo "Steady benchmark summary:"
grep -E '^(Steady benchmark:|Sent total:|Confirmed total:|Publish errors:|Confirm errors:|Received total:|Measured sent:|Measured received:|Measured missing:|Actual measured publish rate:|Measured receive rate:|Retries seen:|Latency .* ms|Server RSS .* MiB:|--- queue snapshot: after steady run ---|\{"queues")' "$results_file" || true
echo "Full steady benchmark results: $results_file"
echo "Full steady benchmark log: $log_file"
echo "Runtime warnings/errors in log:"
grep -Ein ' (WARN|ERROR) |panic|failed' "$log_file" || true
