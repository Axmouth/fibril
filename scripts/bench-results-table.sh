#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/bench-results-table.sh <result-file>...

Print a Markdown table from steady benchmark result files.
EOF
}

if [ "$#" -eq 0 ] || [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage >&2
  exit 2
fi

extract_config() {
  local key="$1"
  local file="$2"
  local line
  line="$(grep -m1 '^Steady benchmark:' "$file" || true)"
  if [[ "$line" != *"${key}="* ]]; then
    return
  fi
  sed -E "s/^.*${key}=//; s/, [[:alpha:]_][[:alnum:]_]*=.*$//" <<<"$line"
}

extract_value() {
  local label="$1"
  local file="$2"
  sed -n "s/^${label}: //p" "$file" | head -n1
}

format_rate() {
  awk -v value="$1" 'BEGIN { printf "%.0f/s", value }'
}

format_latency() {
  local label="$1"
  local file="$2"
  local line
  line="$(grep -m1 "^${label}:" "$file" || true)"
  if [ -z "$line" ]; then
    printf 'n/a'
    return
  fi
  sed -E "s/^${label}: p50=([^,]+), p95=([^,]+), p99=([^,]+), max=([^,]+)$/\1 \/ \2 \/ \3 \/ \4 ms/" <<<"$line"
}

format_publish_latency() {
  local file="$1"
  format_latency "Latency publish->deliver ms" "$file"
}

format_publish_receive_latency() {
  local file="$1"
  format_latency "Latency publish->server-receive ms" "$file"
}

format_server_latency() {
  local file="$1"
  format_latency "Latency server-receive->deliver ms" "$file"
}

format_mode() {
  local file="$1"
  local confirmed
  local window
  confirmed="$(extract_config "confirmed" "$file")"
  if [ "$confirmed" = "true" ]; then
    window="$(extract_config "confirm_window" "$file")"
    printf 'confirmed, window=%s' "$window"
  else
    printf 'unconfirmed'
  fi
}

format_durability() {
  local file="$1"
  local durability
  durability="$(extract_config "durability" "$file")"
  printf '%s' "${durability:-unknown}"
}

format_topic() {
  local file="$1"
  local topic
  topic="$(extract_config "topic" "$file")"
  printf '%s' "${topic:-unknown}"
}

format_errors() {
  local file="$1"
  local publish_errors
  local confirm_errors
  publish_errors="$(extract_value "Publish errors" "$file")"
  confirm_errors="$(extract_value "Confirm errors" "$file")"
  publish_errors="${publish_errors:-0}"
  confirm_errors="${confirm_errors:-0}"
  if [ "$confirm_errors" = "0" ]; then
    printf '%s' "$publish_errors"
  else
    printf 'publish=%s, confirm=%s' "$publish_errors" "$confirm_errors"
  fi
}

format_rss() {
  local file="$1"
  local avg
  local peak
  avg="$(extract_value "Server RSS avg MiB" "$file")"
  peak="$(extract_value "Server RSS peak MiB" "$file")"
  if [ -z "$avg" ] || [ -z "$peak" ]; then
    printf 'n/a'
  else
    printf '%s / %s MiB' "$avg" "$peak"
  fi
}

format_queue() {
  local file="$1"
  local json
  json="$(awk '/^--- queue snapshot:/{getline; print; exit}' "$file")"
  if [ -z "$json" ]; then
    printf 'n/a'
    return
  fi
  if command -v jq >/dev/null 2>&1; then
    jq -r '
      .queues
      | to_entries
      | map("ready=\(.value.ready_count), inflight=\(.value.inflight_count)")
      | join("; ")
    ' <<<"$json"
  else
    printf '%s' "$json"
  fi
}

printf '| Case | Mode | Durability | Topic | Target | Actual | Missing | publish→server p50/p95/p99/max | publish→deliver p50/p95/p99/max | server-receive→deliver p50/p95/p99/max | Errors | Server RSS avg/peak | End queue |\n'
printf '| --- | --- | --- | --- | ---: | ---: | ---: | --- | --- | --- | ---: | --- | --- |\n'

for file in "$@"; do
  case_name="$(basename "$file" .results.txt)"
  target="$(extract_config "rate_per_sec" "$file")"
  actual="$(extract_value "Actual measured publish rate" "$file")"
  missing="$(extract_value "Measured missing" "$file")"

  printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n' \
    "$case_name" \
    "$(format_mode "$file")" \
    "$(format_durability "$file")" \
    "$(format_topic "$file")" \
    "$(format_rate "$target")" \
    "$(format_rate "$actual")" \
    "${missing:-n/a}" \
    "$(format_publish_receive_latency "$file")" \
    "$(format_publish_latency "$file")" \
    "$(format_server_latency "$file")" \
    "$(format_errors "$file")" \
    "$(format_rss "$file")" \
    "$(format_queue "$file")"
done
