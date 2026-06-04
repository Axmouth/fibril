#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
messages="${MESSAGES:-500000}"
clients="${CLIENTS:-10}"
size="${SIZE:-1024}"
prefetch="${PREFETCH:-16384}"
warmup_messages="${WARMUP_MESSAGES:-1000}"
ready_settle_seconds="${READY_SETTLE_SECONDS:-0.5}"
idle_timeout_ms="${IDLE_TIMEOUT_MS:-10000}"
confirmed="${CONFIRMED:-0}"
probe_redelivery="${PROBE_REDELIVERY:-0}"
probe_redelivery_wait_seconds="${PROBE_REDELIVERY_WAIT_SECONDS:-45}"
data_dir="$(mktemp -d)"
writer_args=()
if [ "$confirmed" != "0" ]; then
  writer_args+=(--confirmed)
fi

cargo build --manifest-path "$repo_root/Cargo.toml" --release --bin fibril-server --bin e2e_c

(
  cd "$data_dir"
  "$repo_root/target/release/fibril-server"
) &
server_pid="$!"
cleanup() {
  kill "$server_pid" >/dev/null 2>&1 || true
  wait "$server_pid" >/dev/null 2>&1 || true
  rm -rf "$data_dir"
}
trap cleanup EXIT

wait_for_ready() {
  local expected="$1"
  local dir="$2"

  for _ in $(seq 1 100); do
    local ready_count
    ready_count="$(find "$dir" -name '*.ready' -type f | wc -l)"
    if [ "$ready_count" -ge "$expected" ]; then
      return 0
    fi
    sleep 0.1
  done

  echo "Timed out waiting for ${expected} reader clients to become ready" >&2
  return 1
}

print_queue_snapshot() {
  local label="$1"

  echo "--- queue snapshot: ${label} ---"
  curl --silent --show-error --fail http://127.0.0.1:8081/admin/api/queues \
    | jq --compact-output .
  echo "--- queue debug: ${label} ---"
  curl --silent --show-error --fail http://127.0.0.1:8081/admin/api/queues_debug \
    | jq --compact-output .
}

for _ in $(seq 1 60); do
  if curl --silent --show-error --fail http://127.0.0.1:8081/healthz >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

curl --silent --show-error --fail http://127.0.0.1:8081/healthz >/dev/null

echo "Warmup: ${warmup_messages} messages, 1 client"
warmup_ready_dir="$data_dir/warmup-ready"
mkdir -p "$warmup_ready_dir"
"$repo_root/target/release/e2e_c" -m "$warmup_messages" -c 1 --reader --prefetch "$prefetch" --ready-dir "$warmup_ready_dir" --idle-timeout-ms "$idle_timeout_ms" &
warmup_reader_pid="$!"
wait_for_ready 1 "$warmup_ready_dir"
sleep "$ready_settle_seconds"
"$repo_root/target/release/e2e_c" -m "$warmup_messages" -c 1 --writer --size "$size" "${writer_args[@]}"
wait "$warmup_reader_pid"
if [ "$probe_redelivery" != "0" ]; then
  print_queue_snapshot "after warmup"
fi

echo "Benchmark: ${messages} messages/client, ${clients} clients, ${size} byte payload, prefetch ${prefetch}"
benchmark_ready_dir="$data_dir/benchmark-ready"
mkdir -p "$benchmark_ready_dir"
"$repo_root/target/release/e2e_c" -m "$messages" -c "$clients" --reader --prefetch "$prefetch" --ready-dir "$benchmark_ready_dir" --idle-timeout-ms "$idle_timeout_ms" &
reader_pid="$!"
wait_for_ready "$clients" "$benchmark_ready_dir"
sleep "$ready_settle_seconds"
"$repo_root/target/release/e2e_c" -m "$messages" -c "$clients" --writer --size "$size" "${writer_args[@]}"
wait "$reader_pid"
if [ "$probe_redelivery" != "0" ]; then
  print_queue_snapshot "after measured reader exit"
  echo "Waiting ${probe_redelivery_wait_seconds}s for inflight expiry/redelivery"
  sleep "$probe_redelivery_wait_seconds"
  print_queue_snapshot "after redelivery wait"

  echo "Second drain: ${messages} messages/client, ${clients} clients"
  second_drain_ready_dir="$data_dir/second-drain-ready"
  mkdir -p "$second_drain_ready_dir"
  "$repo_root/target/release/e2e_c" -m "$messages" -c "$clients" --reader --prefetch "$prefetch" --ready-dir "$second_drain_ready_dir" --idle-timeout-ms "$idle_timeout_ms" &
  second_reader_pid="$!"
  wait_for_ready "$clients" "$second_drain_ready_dir"
  wait "$second_reader_pid"
  print_queue_snapshot "after second drain"
fi
