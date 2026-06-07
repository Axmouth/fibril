#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out_dir="${OUT_DIR:-$repo_root/bench-results/steady-$(date +%Y%m%d-%H%M%S)}"
default_warmup_secs="${WARMUP_SECS:-2}"
default_duration_secs="${DURATION_SECS:-5}"
default_drain_timeout_secs="${DRAIN_TIMEOUT_SECS:-10}"
default_writers="${WRITERS:-10}"
default_readers="${READERS:-10}"
default_prefetch="${PREFETCH:-16384}"
default_confirm_window="${CONFIRM_WINDOW:-1024}"
build="${BUILD:-1}"

usage() {
  cat <<'EOF'
Usage: scripts/bench-matrix.sh [scenario...]

Runs named steady-state benchmark scenarios and writes one result/log pair per
case under OUT_DIR.

Scenarios:
  smoke          quick low-rate sanity check
  baseline       1KB 50k/s and 150k/s unconfirmed
  confirmed      1KB 50k/s and 150k/s with pipelined confirmations
  throughput-1k  1KB high-rate exploratory sweep
  payload        8KB, 64KB, 512KB, and 1MB spot checks
  large-backlog  large-payload backlog checks
  all            baseline, confirmed, throughput-1k, payload

Useful environment overrides:
  OUT_DIR, WRITERS, READERS, WARMUP_SECS, DURATION_SECS,
  DRAIN_TIMEOUT_SECS, PREFETCH, CONFIRM_WINDOW, BUILD=0
EOF
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi

mkdir -p "$out_dir"
index_file="$out_dir/index.txt"
{
  echo "Fibril steady benchmark matrix"
  echo "Started: $(date --iso-8601=seconds)"
  echo "Output directory: $out_dir"
  echo
} >"$index_file"

if [ "$build" != "0" ]; then
  build_log="$out_dir/build.log"
  echo "Building release benchmark binaries. Log: $build_log"
  cargo build --manifest-path "$repo_root/Cargo.toml" --release --bin fibril-server --bin steady_c >"$build_log" 2>&1
fi

run_case() {
  local name="$1"
  shift
  local results_file="$out_dir/$name.results.txt"
  local log_file="$out_dir/$name.log"

  echo
  echo "==> $name"
  echo "    results: $results_file"
  echo "    log:     $log_file"

  {
    echo "$name"
    echo "  results: $results_file"
    echo "  log:     $log_file"
  } >>"$index_file"

  env \
    WRITERS="$default_writers" \
    READERS="$default_readers" \
    WARMUP_SECS="$default_warmup_secs" \
    DURATION_SECS="$default_duration_secs" \
    DRAIN_TIMEOUT_SECS="$default_drain_timeout_secs" \
    PREFETCH="$default_prefetch" \
    CONFIRM_WINDOW="$default_confirm_window" \
    LOG_FILE="$log_file" \
    RESULTS_FILE="$results_file" \
    BUILD=0 \
    "$@" \
    "$repo_root/scripts/bench-steady-c.sh"
}

run_scenario() {
  local scenario="$1"
  case "$scenario" in
    smoke)
      run_case smoke-1k-10k \
        WRITERS="${SMOKE_WRITERS:-2}" \
        READERS="${SMOKE_READERS:-2}" \
        RATE_PER_SEC="${SMOKE_RATE_PER_SEC:-10000}" \
        WARMUP_SECS="${SMOKE_WARMUP_SECS:-1}" \
        DURATION_SECS="${SMOKE_DURATION_SECS:-2}" \
        SIZE=1024
      ;;
    baseline)
      run_case baseline-1k-50k RATE_PER_SEC=50000 SIZE=1024
      run_case baseline-1k-150k RATE_PER_SEC=150000 SIZE=1024
      ;;
    confirmed)
      run_case confirmed-1k-50k RATE_PER_SEC=50000 SIZE=1024 CONFIRMED=1
      run_case confirmed-1k-150k RATE_PER_SEC=150000 SIZE=1024 CONFIRMED=1
      ;;
    throughput-1k)
      run_case throughput-1k-250k RATE_PER_SEC=250000 SIZE=1024
      run_case throughput-1k-350k RATE_PER_SEC=350000 SIZE=1024
      run_case throughput-1k-400k RATE_PER_SEC=400000 SIZE=1024
      run_case throughput-1k-500k RATE_PER_SEC=500000 SIZE=1024
      ;;
    payload)
      run_case payload-8k-50k RATE_PER_SEC=50000 SIZE=8192
      run_case payload-8k-150k RATE_PER_SEC=150000 SIZE=8192
      run_case payload-64k-10k RATE_PER_SEC=10000 SIZE=65536
      run_case payload-64k-20k RATE_PER_SEC=20000 SIZE=65536
      run_case payload-512k-1k RATE_PER_SEC=1000 SIZE=524288 PREFETCH=512
      run_case payload-1m-500 RATE_PER_SEC=500 SIZE=1048576 PREFETCH=256
      ;;
    large-backlog)
      run_case backlog-512k-2k RATE_PER_SEC=2000 SIZE=524288 PREFETCH=512
      run_case backlog-1m-1k RATE_PER_SEC=1000 SIZE=1048576 PREFETCH=256
      ;;
    all)
      run_scenario baseline
      run_scenario confirmed
      run_scenario throughput-1k
      run_scenario payload
      ;;
    *)
      echo "Unknown benchmark scenario: $scenario" >&2
      usage >&2
      exit 1
      ;;
  esac
}

if [ "$#" -eq 0 ]; then
  set -- smoke
fi

for scenario in "$@"; do
  run_scenario "$scenario"
done

{
  echo
  echo "Finished: $(date --iso-8601=seconds)"
} >>"$index_file"

echo
echo "Benchmark matrix complete."
echo "Index: $index_file"
