#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
writers="${WRITERS:-10}"
readers="${READERS:-10}"
rate_per_sec="${RATE_PER_SEC:-100000}"
warmup_secs="${WARMUP_SECS:-5}"
duration_secs="${DURATION_SECS:-30}"
drain_timeout_secs="${DRAIN_TIMEOUT_SECS:-10}"
size="${SIZE:-1024}"
prefetch="${PREFETCH:-16384}"
confirmed="${CONFIRMED:-0}"
confirm_window="${CONFIRM_WINDOW:-1024}"
build="${BUILD:-1}"
data_dir="$(mktemp -d)"
log_file="${LOG_FILE:-$data_dir/steady.log}"
results_file="${RESULTS_FILE:-$data_dir/steady-results.txt}"
memory_file="$data_dir/server-rss-kib.txt"
bench_args=()
memory_sampler_pid=""

if [ "$confirmed" != "0" ]; then
  bench_args+=(--confirmed --confirm-window "$confirm_window")
fi

mkdir -p "$(dirname "$log_file")" "$(dirname "$results_file")"
: >"$log_file"
: >"$results_file"

echo "Steady benchmark log: $log_file"
echo "Steady benchmark results: $results_file"

if [ "$build" != "0" ]; then
  cargo build --manifest-path "$repo_root/Cargo.toml" --release --bin fibril-server --bin steady_c >>"$log_file" 2>&1
fi

(
  cd "$data_dir"
  "$repo_root/target/release/fibril-server"
) >>"$log_file" 2>&1 &
server_pid="$!"

cleanup() {
  if [ -n "$memory_sampler_pid" ]; then
    kill "$memory_sampler_pid" >/dev/null 2>&1 || true
    wait "$memory_sampler_pid" >/dev/null 2>&1 || true
  fi
  kill "$server_pid" >/dev/null 2>&1 || true
  wait "$server_pid" >/dev/null 2>&1 || true
  rm -rf "$data_dir"
}
trap cleanup EXIT

ready=0
for _ in $(seq 1 60); do
  if ! kill -0 "$server_pid" >/dev/null 2>&1; then
    echo "Benchmark server exited before becoming healthy. Log tail:" >&2
    tail -n 80 "$log_file" >&2 || true
    exit 1
  fi
  if curl --silent --show-error --fail http://127.0.0.1:8081/healthz >/dev/null 2>&1; then
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

curl --silent --show-error --fail http://127.0.0.1:8081/healthz >>"$log_file" 2>&1

sample_memory() {
  while kill -0 "$server_pid" >/dev/null 2>&1; do
    rss="$(ps -o rss= -p "$server_pid" 2>/dev/null | tr -d ' ')"
    if [ -n "$rss" ]; then
      printf '%s %s\n' "$(date +%s)" "$rss" >>"$memory_file"
    fi
    sleep 1
  done
}

sample_memory &
memory_sampler_pid="$!"

"$repo_root/target/release/steady_c" \
  --writers "$writers" \
  --readers "$readers" \
  --rate-per-sec "$rate_per_sec" \
  --warmup-secs "$warmup_secs" \
  --duration-secs "$duration_secs" \
  --drain-timeout-secs "$drain_timeout_secs" \
  --size "$size" \
  --prefetch "$prefetch" \
  "${bench_args[@]}" >>"$results_file" 2>>"$log_file"

kill "$memory_sampler_pid" >/dev/null 2>&1 || true
wait "$memory_sampler_pid" >/dev/null 2>&1 || true

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
  curl --silent --show-error --fail http://127.0.0.1:8081/admin/api/queues \
    | jq --compact-output .
  echo "--- queue debug: after steady run ---"
  curl --silent --show-error --fail http://127.0.0.1:8081/admin/api/queues_debug \
    | jq --compact-output .
} >>"$results_file"

echo "Steady benchmark summary:"
grep -E '^(Steady benchmark:|Sent total:|Confirmed total:|Publish errors:|Confirm errors:|Received total:|Measured sent:|Measured received:|Measured missing:|Actual measured publish rate:|Measured receive rate:|Retries seen:|Latency .* ms|Server RSS .* MiB:|--- queue snapshot: after steady run ---|\{"queues")' "$results_file" || true
echo "Full steady benchmark results: $results_file"
echo "Full steady benchmark log: $log_file"
echo "Runtime warnings/errors in log:"
grep -Ein ' (WARN|ERROR) |panic|failed' "$log_file" || true
