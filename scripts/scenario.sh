#!/usr/bin/env bash
# Scenario builder: drive a broker (or a small cluster) through a scripted
# sequence of operator actions and watch the dashboard react - the Activity
# feed, the attention panel, the Cluster page, desktop notifications.
#
# Usage:
#   scripts/scenario.sh scripts/scenarios/activity-tour.scenario
#   scripts/scenario.sh scripts/scenarios/membership-churn.scenario --nodes 3 --ganglion
#   scripts/scenario.sh <file> --admin http://127.0.0.1:8081   # drive an existing broker
#     (with --admin, publish-burst/consume assume the broker port is admin's
#      host with port 9876 - script-managed nodes handle this automatically)
#
# A scenario file is one step per line (# comments allowed):
#   say <text>                          narrate progress
#   wait <seconds>                      let the dashboard breathe
#   declare-queue <topic> [partitions]
#   declare-stream <topic> [partitions] [durability]
#   test-publish <topic> [text...]      one marked message via the admin API
#   publish-burst <topic> <count> [size]   real client load via e2e_c
#   consume <topic> <seconds>           background reader draining the topic
#   delete-queue <topic>
#   drain [grace_ms]
#   node-kill <i>                       cluster mode only (script-managed nodes)
#   node-start <i>
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

SCENARIO="${1:?usage: scripts/scenario.sh <scenario-file> [--nodes N] [--ganglion] [--admin URL]}"
shift
NODES=1
GANGLION=false
ADMIN_URL=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes) NODES="$2"; shift 2 ;;
    --ganglion) GANGLION=true; shift ;;
    --admin) ADMIN_URL="$2"; shift 2 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# Deliberately OFF the default ports so a dev broker on 9876/8081 is never
# silently adopted or disturbed.
BASE_BROKER_PORT=19875
BASE_ADMIN_PORT=18080
BASE_RAFT_PORT=17070
RUN_DIR="$(mktemp -d)"
declare -a PIDS=()
declare -a CONSUMERS=()

cleanup() {
  for pid in "${CONSUMERS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$RUN_DIR"
}
trap cleanup EXIT

admin_url() { echo "${ADMIN_URL:-http://127.0.0.1:$((BASE_ADMIN_PORT + 1))}"; }
broker_addr() {
  if [[ -n "$ADMIN_URL" ]]; then echo "127.0.0.1:9876"; else echo "127.0.0.1:$((BASE_BROKER_PORT + 1))"; fi
}

start_node() {
  local i="$1"
  local env_vars=(
    "FIBRIL_DATA_DIR=$RUN_DIR/node-$i"
    "FIBRIL_BROKER_BIND=127.0.0.1:$((BASE_BROKER_PORT + i))"
    "FIBRIL_ADMIN_BIND=127.0.0.1:$((BASE_ADMIN_PORT + i))"
  )
  if [[ "$GANGLION" == true ]]; then
    local peers=""
    for ((p = 1; p <= NODES; p++)); do
      peers+="${peers:+,}$p=127.0.0.1:$((BASE_RAFT_PORT + p))"
    done
    env_vars+=(
      "FIBRIL_CLUSTER_SECRET=scenario-secret"
      "FIBRIL_COORDINATION_MODE=ganglion"
      "FIBRIL_COORDINATION_NODE_ID=broker-$i"
      "FIBRIL_COORDINATION_RAFT_ID=$i"
      "FIBRIL_COORDINATION_LISTEN=127.0.0.1:$((BASE_RAFT_PORT + i))"
      "FIBRIL_COORDINATION_PEERS=$peers"
    )
    [[ "$i" -eq 1 ]] && env_vars+=("FIBRIL_COORDINATION_BOOTSTRAP=true")
  fi
  env "${env_vars[@]}" target/release/fibril-server >"$RUN_DIR/node-$i.log" 2>&1 &
  PIDS[$((i - 1))]="$!"
  echo "  node-$i up: admin http://127.0.0.1:$((BASE_ADMIN_PORT + i)) (pid ${PIDS[$((i - 1))]})"
}

wait_admin() {
  local url="$1" pid="${2:-}"
  for _ in $(seq 1 60); do
    if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
      echo "scenario node (pid $pid) exited during startup - port clash or bad config; see its log under $RUN_DIR" >&2
      exit 1
    fi
    curl -sf "$url/healthz" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "broker at $url never became healthy" >&2
  exit 1
}

post() {
  local path="$1" body="$2"
  curl -sf -X POST "$(admin_url)$path" -H 'Content-Type: application/json' -d "$body" \
    | head -c 200 || echo "  (request failed: $path)"
  echo
}

if [[ -z "$ADMIN_URL" ]]; then
  cargo build --release --bin fibril-server --bin e2e_c --bin bench-stream >/dev/null 2>&1
  echo "Starting $NODES node(s) (dirs under $RUN_DIR)..."
  for ((i = 1; i <= NODES; i++)); do start_node "$i"; done
  wait_admin "$(admin_url)" "${PIDS[0]}"
fi
echo
echo "Dashboard: $(admin_url)/admin  (Activity: $(admin_url)/admin/activity)"
echo "Running scenario: $SCENARIO"
echo

while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="$(echo "$line" | sed 's/^ *//;s/ *$//')"
  [[ -z "$line" ]] && continue
  read -r verb rest <<<"$line"
  case "$verb" in
    say)
      echo ">> $rest" ;;
    wait)
      sleep "$rest" ;;
    declare-queue)
      read -r topic partitions <<<"$rest"
      post "/admin/api/queues" "{\"topic\":\"$topic\",\"partition_count\":${partitions:-1}}" ;;
    declare-stream)
      read -r topic partitions durability <<<"$rest"
      post "/admin/api/streams" "{\"topic\":\"$topic\",\"partition_count\":${partitions:-1},\"durability\":\"${durability:-durable}\"}" ;;
    test-publish)
      read -r topic text <<<"$rest"
      post "/admin/api/publish" "{\"topic\":\"$topic\",\"text\":\"${text:-scenario test message}\"}" ;;
    publish-burst)
      read -r topic count size <<<"$rest"
      target/release/e2e_c --addr "$(broker_addr)" -m "${count:-1000}" -c 1 --writer \
        --size "${size:-256}" --topic "$topic" >/dev/null 2>&1 || echo "  (burst failed)"
      echo "  published ${count:-1000} to $topic" ;;
    stream-load)
      # Rate-limited stream writers plus durable-cursor readers via
      # bench_stream. Cursor names are stable (bench-reader-N), so a later
      # stream-load on the same topic RESUMES the same cursors - the
      # disconnect-and-resume story on the Streams page cursor table.
      read -r topic rate seconds readers partitions <<<"$rest"
      timeout $(( ${seconds:-30} + 30 )) target/release/bench-stream \
        --broker-addr "$(broker_addr)" --topic "$topic" \
        --partitions "${partitions:-1}" --durability durable \
        --writers 1 --readers "${readers:-2}" --durable-readers \
        --rate-per-sec "${rate:-200}" --warmup-secs 0 \
        --duration-secs "${seconds:-30}" --size 256 >/dev/null 2>&1 &
      CONSUMERS+=("$!")
      echo "  stream load on $topic: ${rate:-200}/s for ${seconds:-30}s with ${readers:-2} durable readers" ;;
    consume)
      read -r topic seconds <<<"$rest"
      timeout "${seconds:-10}" target/release/e2e_c --addr "$(broker_addr)" -m 1000000 -c 1 --reader \
        --topic "$topic" --idle-timeout-ms $(( (${seconds:-10}) * 1000 )) >/dev/null 2>&1 &
      CONSUMERS+=("$!")
      echo "  consumer on $topic for ${seconds:-10}s" ;;
    delete-queue)
      read -r topic <<<"$rest"
      post "/admin/api/queues/delete" "{\"topic\":\"$topic\"}" ;;
    drain)
      post "/admin/api/drain" "{\"grace_ms\":${rest:-5000},\"message\":\"scenario drain\"}" ;;
    node-kill)
      i="$rest"
      kill "${PIDS[$((i - 1))]}" 2>/dev/null && echo "  node-$i killed" ;;
    node-start)
      i="$rest"
      start_node "$i"
      wait_admin "http://127.0.0.1:$((BASE_ADMIN_PORT + i))" "${PIDS[$((i - 1))]}" ;;
    *)
      echo "unknown verb: $verb" >&2 ;;
  esac
done <"$SCENARIO"

echo
echo "Scenario complete. Leaving everything up for browsing - Ctrl-C tears it down."
wait
