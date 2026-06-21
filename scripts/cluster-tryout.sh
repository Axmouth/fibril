#!/usr/bin/env bash
# Start N REAL fibril-server processes (own ports, own data dirs) and check
# each one's topology through the real fibrilctl CLI over HTTP.
#
# Usage:
#   scripts/cluster-tryout.sh              # 3 standalone nodes, check topology, shut down
#   scripts/cluster-tryout.sh --ganglion   # 3 nodes forming ONE raft coordination cluster
#   scripts/cluster-tryout.sh --staggered   # ganglion mode, nodes join one by one (watch the
#                                           # cluster form: no quorum -> election -> all join)
#   scripts/cluster-tryout.sh --nodes 5
#   scripts/cluster-tryout.sh --nodes 21 --ganglion --summary
#   scripts/cluster-tryout.sh --nodes 21 --ganglion --summary --port-offset 4000
#   scripts/cluster-tryout.sh --nodes 3 --ganglion --dynamic-membership
#   scripts/cluster-tryout.sh --nodes 3 --ganglion --replica-durable
#   scripts/cluster-tryout.sh --nodes 3 --ganglion --replica-durable --failover-smoke
#   scripts/cluster-tryout.sh --nodes 3 --failover-verify   # identity zero-loss check under owner kill
#   scripts/cluster-tryout.sh --nodes 3 --chaos             # chaos soak: repeated mixed faults (kill+rejoin, pause) under load, zero-loss + reconverge
#   scripts/cluster-tryout.sh --nodes 5 --chaos --chaos-rounds 12   # longer, wider chaos soak
#   scripts/cluster-tryout.sh --nodes 3 --ganglion --steady-bench
#   scripts/cluster-tryout.sh --nodes 100 --ganglion --summary --resource-summary --admin-wait-secs 5 --cluster-wait-secs 90
#   scripts/cluster-tryout.sh --keep       # leave the cluster running to play with (detaches, prints a kill command)
#   scripts/cluster-tryout.sh --ganglion --hold   # like --keep but stay attached; Ctrl-C tears the whole cluster down
#
# In --ganglion mode the servers share an embedded raft coordinator over real
# TCP: the script asserts all nodes agree on one leader and voter set.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NODES=3
KEEP=false
HOLD=false
GANGLION=false
STAGGERED=false
SUMMARY=false
PORT_OFFSET=""
RESOURCE_SUMMARY=false
DYNAMIC_MEMBERSHIP=false
STEADY_BENCH=false
BENCH_TOPIC="orders"
# Repartition the steady-bench topic to this many partitions before benching, so a
# multi-partition cluster's aggregate throughput can be measured (load spreads
# round-robin across partition owners). 1 = leave single-partition.
BENCH_PARTITION_COUNT="${BENCH_PARTITION_COUNT:-1}"
ASSIGNMENT_DURABILITY=""
FAILOVER_SMOKE=false
FAILOVER_VERIFY=false
FAILOVER_VERIFY_COUNT="${FAILOVER_VERIFY_COUNT:-30000}"
FAILOVER_VERIFY_RATE="${FAILOVER_VERIFY_RATE:-5000}"
REPARTITION_SMOKE=false
CHAOS=false
CHAOS_ROUNDS="${CHAOS_ROUNDS:-6}"
CHAOS_LOAD_SECS="${CHAOS_LOAD_SECS:-70}"
COORDINATION_HEARTBEAT_INTERVAL_MS="${COORDINATION_HEARTBEAT_INTERVAL_MS:-}"
COORDINATION_LIVENESS_TTL_MS="${COORDINATION_LIVENESS_TTL_MS:-}"
REPLICATION_CAUGHT_UP_POLL_MS="${REPLICATION_CAUGHT_UP_POLL_MS:-}"
REPLICATION_RETRY_POLL_MS="${REPLICATION_RETRY_POLL_MS:-}"
REPLICATION_CHECKPOINT_RETRY_POLL_MS="${REPLICATION_CHECKPOINT_RETRY_POLL_MS:-}"
REPLICATION_MAX_MESSAGES_PER_READ="${REPLICATION_MAX_MESSAGES_PER_READ:-}"
REPLICATION_MAX_EVENTS_PER_READ="${REPLICATION_MAX_EVENTS_PER_READ:-}"
REPLICATION_MAX_BYTES_PER_READ="${REPLICATION_MAX_BYTES_PER_READ:-}"
REPLICATION_MAX_ITERATIONS_PER_TICK="${REPLICATION_MAX_ITERATIONS_PER_TICK:-}"
REPLICATION_STREAM_APPLY_LINGER_US="${REPLICATION_STREAM_APPLY_LINGER_US:-}"
CLUSTER_TRYOUT_RUN_ROOT="${CLUSTER_TRYOUT_RUN_ROOT:-/tmp}"
ADMIN_WAIT_SECS=5
CLUSTER_WAIT_SECS=90
# Keep wide spacing between port bands so large local clusters do not make
# broker/admin/raft ports collide as node indexes grow.
BASE_BROKER_PORT=17180
BASE_ADMIN_PORT=27180
BASE_RAFT_PORT=37180
STABLE_ROUNDS_REQUIRED=10

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes) NODES="$2"; shift 2 ;;
    --keep) KEEP=true; shift ;;
    --hold) HOLD=true; shift ;;
    --ganglion) GANGLION=true; shift ;;
    --staggered) GANGLION=true; STAGGERED=true; shift ;;
    --summary) SUMMARY=true; shift ;;
    --port-offset) PORT_OFFSET="$2"; shift 2 ;;
    --resource-summary) RESOURCE_SUMMARY=true; shift ;;
    --dynamic-membership) GANGLION=true; DYNAMIC_MEMBERSHIP=true; shift ;;
    --steady-bench) STEADY_BENCH=true; shift ;;
    --bench-topic) BENCH_TOPIC="$2"; shift 2 ;;
    --replica-durable) GANGLION=true; ASSIGNMENT_DURABILITY="replica_durable:2"; shift ;;
    --assignment-durability) GANGLION=true; ASSIGNMENT_DURABILITY="$2"; shift 2 ;;
    --failover-smoke) GANGLION=true; FAILOVER_SMOKE=true; shift ;;
    --failover-verify)
      GANGLION=true; FAILOVER_VERIFY=true
      [[ -z "$ASSIGNMENT_DURABILITY" ]] && ASSIGNMENT_DURABILITY="replica_durable:2"
      shift ;;
    --repartition-smoke) GANGLION=true; REPARTITION_SMOKE=true; shift ;;
    --chaos)
      GANGLION=true; CHAOS=true
      [[ -z "$ASSIGNMENT_DURABILITY" ]] && ASSIGNMENT_DURABILITY="replica_durable:2"
      shift ;;
    --chaos-rounds) CHAOS_ROUNDS="$2"; shift 2 ;;
    --admin-wait-secs) ADMIN_WAIT_SECS="$2"; shift 2 ;;
    --cluster-wait-secs) CLUSTER_WAIT_SECS="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if ! [[ "$ADMIN_WAIT_SECS" =~ ^[0-9]+$ ]] || [[ "$ADMIN_WAIT_SECS" -lt 1 ]]; then
  echo "FAIL: --admin-wait-secs must be a positive integer" >&2
  exit 2
fi
if ! [[ "$CLUSTER_WAIT_SECS" =~ ^[0-9]+$ ]] || [[ "$CLUSTER_WAIT_SECS" -lt 1 ]]; then
  echo "FAIL: --cluster-wait-secs must be a positive integer" >&2
  exit 2
fi
if [[ -z "$BENCH_TOPIC" ]]; then
  echo "FAIL: --bench-topic must not be empty" >&2
  exit 2
fi
if [[ -n "$ASSIGNMENT_DURABILITY" && "$ASSIGNMENT_DURABILITY" =~ ^replica_ && "$NODES" -lt 2 ]]; then
  echo "FAIL: replica assignment durability needs at least two nodes" >&2
  exit 2
fi
if [[ "$FAILOVER_SMOKE" == true && "$NODES" -lt 3 ]]; then
  echo "FAIL: --failover-smoke needs at least three nodes so raft keeps quorum after one death" >&2
  exit 2
fi
if [[ "$FAILOVER_VERIFY" == true && "$NODES" -lt 3 ]]; then
  echo "FAIL: --failover-verify needs at least three nodes so raft keeps quorum after one death" >&2
  exit 2
fi
if [[ "$CHAOS" == true && "$NODES" -lt 3 ]]; then
  echo "FAIL: --chaos needs at least three nodes so the cluster keeps quorum while one is faulted" >&2
  exit 2
fi
if ! [[ "$CHAOS_ROUNDS" =~ ^[0-9]+$ ]] || [[ "$CHAOS_ROUNDS" -lt 1 ]]; then
  echo "FAIL: --chaos-rounds must be a positive integer" >&2
  exit 2
fi
if [[ "$REPARTITION_SMOKE" == true && "$NODES" -lt 2 ]]; then
  echo "FAIL: --repartition-smoke needs at least two nodes to exercise routed client traffic" >&2
  exit 2
fi
if [[ "$HOLD" == true && "$KEEP" == true ]]; then
  echo "FAIL: --hold and --keep are mutually exclusive (--keep detaches and leaves the cluster after exit; --hold stays attached and tears it down on Ctrl-C)" >&2
  exit 2
fi

require_positive_int_env() {
  local name="$1"
  local value="${!name:-}"
  if [[ -n "$value" ]] && { ! [[ "$value" =~ ^[0-9]+$ ]] || [[ "$value" -lt 1 ]]; }; then
    echo "FAIL: $name must be a positive integer" >&2
    exit 2
  fi
}

for numeric_env in \
  COORDINATION_HEARTBEAT_INTERVAL_MS \
  COORDINATION_LIVENESS_TTL_MS \
  REPLICATION_CAUGHT_UP_POLL_MS \
  REPLICATION_RETRY_POLL_MS \
  REPLICATION_CHECKPOINT_RETRY_POLL_MS \
  REPLICATION_MAX_MESSAGES_PER_READ \
  REPLICATION_MAX_EVENTS_PER_READ \
  REPLICATION_MAX_BYTES_PER_READ \
  REPLICATION_MAX_ITERATIONS_PER_TICK
do
  require_positive_int_env "$numeric_env"
done

# Apply linger may be 0 (drain-only), so it is non-negative rather than positive.
if [[ -n "$REPLICATION_STREAM_APPLY_LINGER_US" ]] \
  && ! [[ "$REPLICATION_STREAM_APPLY_LINGER_US" =~ ^[0-9]+$ ]]; then
  echo "FAIL: REPLICATION_STREAM_APPLY_LINGER_US must be a non-negative integer" >&2
  exit 2
fi

cluster_attempts() {
  # Cluster checks sleep 0.3s between attempts. Round up so the configured
  # seconds are a real lower bound, not an accidental shorter window.
  echo $(((CLUSTER_WAIT_SECS * 10 + 2) / 3))
}

if [[ -z "$PORT_OFFSET" ]]; then
  PORT_OFFSET=$(( (RANDOM % 20) * 1000 ))
fi

mkdir -p "$CLUSTER_TRYOUT_RUN_ROOT"
RUN_DIR="$(mktemp -d "$CLUSTER_TRYOUT_RUN_ROOT/fibril-cluster-tryout.XXXXXX")"
declare -a PIDS=()
EXPECTED_PROCESSES="$NODES"
if [[ "$DYNAMIC_MEMBERSHIP" == true ]]; then
  EXPECTED_PROCESSES=$((NODES + 1))
fi

# Pick a single port offset whose whole broker/admin/raft port set is free.
# Large local clusters routinely collide with stale --keep clusters, overlapping
# tryouts, or unrelated services, so list the busy ports up front and skip any
# offset that overlaps them instead of failing outright.
declare -A USED_PORTS=()
HAVE_PORT_SNAPSHOT=false
snapshot_used_ports() {
  local listing=""
  if command -v ss >/dev/null 2>&1; then
    listing="$(ss -Htln 2>/dev/null | awk '{print $4}')"
  elif command -v netstat >/dev/null 2>&1; then
    listing="$(netstat -ltn 2>/dev/null | awk 'NR>2 {print $4}')"
  else
    return 1
  fi
  local addr port
  while read -r addr; do
    port="${addr##*:}"
    [[ "$port" =~ ^[0-9]+$ ]] && USED_PORTS["$port"]=1
  done <<< "$listing"
  return 0
}
if snapshot_used_ports; then
  HAVE_PORT_SNAPSHOT=true
fi

port_busy() {
  local port="$1"
  if [[ "$HAVE_PORT_SNAPSHOT" == true ]]; then
    [[ -n "${USED_PORTS[$port]:-}" ]]
  else
    # No listing tool available, probe the port directly.
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      exec 3>&- 2>/dev/null || true
      return 0
    fi
    return 1
  fi
}

offset_is_free() {
  local off="$1" i port
  for ((i = 1; i <= EXPECTED_PROCESSES; i++)); do
    for port in $((BASE_BROKER_PORT + off + i)) $((BASE_ADMIN_PORT + off + i)) $((BASE_RAFT_PORT + off + i)); do
      port_busy "$port" && return 1
    done
  done
  return 0
}

# Raft sits on the highest base, so it bounds how far the offset can reach
# before any port would exceed 65535.
MAX_OFFSET=$((65535 - BASE_RAFT_PORT - EXPECTED_PROCESSES - 1))
if [[ "$MAX_OFFSET" -lt 0 ]]; then
  echo "FAIL: $EXPECTED_PROCESSES nodes need more ports than the local range allows" >&2
  exit 1
fi

CHOSEN_OFFSET=""
# Honor the requested offset first when it fits and is free, then sweep aligned
# offsets from the bottom of the range.
for candidate in "$PORT_OFFSET" $(seq 0 1000 "$MAX_OFFSET"); do
  [[ "$candidate" -gt "$MAX_OFFSET" ]] && continue
  if offset_is_free "$candidate"; then
    CHOSEN_OFFSET="$candidate"
    break
  fi
done
if [[ -z "$CHOSEN_OFFSET" ]]; then
  echo "FAIL: no free port offset for $EXPECTED_PROCESSES nodes near $PORT_OFFSET (local ports too crowded?)" >&2
  exit 1
fi
if [[ "$CHOSEN_OFFSET" != "$PORT_OFFSET" ]]; then
  echo "note: port_offset $PORT_OFFSET overlapped in-use ports, using $CHOSEN_OFFSET instead"
fi
PORT_OFFSET="$CHOSEN_OFFSET"
BASE_BROKER_PORT=$((BASE_BROKER_PORT + PORT_OFFSET))
BASE_ADMIN_PORT=$((BASE_ADMIN_PORT + PORT_OFFSET))
BASE_RAFT_PORT=$((BASE_RAFT_PORT + PORT_OFFSET))

print_resource_summary() {
  if [[ "$RESOURCE_SUMMARY" != true ]]; then
    return
  fi

  echo
  echo "resource summary:"
  echo "  run_dir: $RUN_DIR"
  echo "  host ulimit -n: $(ulimit -n 2>/dev/null || echo unknown)"

  local live=0
  local total_rss_kb=0
  local max_rss_kb=0
  local total_fds=0
  local max_fds=0
  local samples=0
  local rss_kb=""
  local fd_count=""
  local fd_mix=""
  local fd_categories=""
  local fd_regular_sample=""

  for pid in "${PIDS[@]:-}"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      continue
    fi
    live=$((live + 1))

    rss_kb="$(ps -o rss= -p "$pid" 2>/dev/null | awk '{print $1}' || true)"
    if [[ "$rss_kb" =~ ^[0-9]+$ ]]; then
      total_rss_kb=$((total_rss_kb + rss_kb))
      if [[ "$rss_kb" -gt "$max_rss_kb" ]]; then
        max_rss_kb="$rss_kb"
      fi
    fi

    if [[ -d "/proc/$pid/fd" ]]; then
      fd_count="$(find "/proc/$pid/fd" -maxdepth 1 -mindepth 1 2>/dev/null | wc -l | awk '{print $1}' || true)"
      if [[ "$fd_count" =~ ^[0-9]+$ ]]; then
        total_fds=$((total_fds + fd_count))
        if [[ "$fd_count" -gt "$max_fds" ]]; then
          max_fds="$fd_count"
        fi
      fi
      if [[ "$samples" -lt 3 ]]; then
        fd_categories="$(
          find "/proc/$pid/fd" -maxdepth 1 -mindepth 1 -printf '%l\n' 2>/dev/null \
            | awk -v run_dir="$RUN_DIR" '
                /^socket:\[/ { print "socket"; next }
                /^anon_inode:\[/ {
                  gsub(/^anon_inode:\[/, "anon_inode:");
                  gsub(/\]$/, "");
                  print;
                  next
                }
                $0 == "" { print "unreadable-or-empty"; next }
                index($0, run_dir) == 1 { print "run-dir-file"; next }
                /^\// { print "regular-file"; next }
                { print "other" }
              ' \
            | sort \
            | uniq -c \
            | sort -nr \
            || true
        )"
        fd_mix="$(
          find "/proc/$pid/fd" -maxdepth 1 -mindepth 1 -printf '%l\n' 2>/dev/null \
            | sed -E \
                -e 's/^socket:\[[0-9]+\]$/socket/' \
                -e 's/^anon_inode:\[([^]]+)\]$/anon_inode:\1/' \
                -e 's|/tmp/fibril-cluster-tryout\.[^/]+|/tmp/fibril-cluster-tryout/...|' \
            | sort \
            | uniq -c \
            | sort -nr \
            | sed -n '1,12p' \
            || true
        )"
        fd_regular_sample="$(
          find "/proc/$pid/fd" -maxdepth 1 -mindepth 1 -printf '%l\n' 2>/dev/null \
            | awk -v run_dir="$RUN_DIR" '
                $0 == "" { next }
                /^socket:\[/ { next }
                /^anon_inode:\[/ { next }
                index($0, run_dir) == 1 { next }
                /^\// { print; next }
              ' \
            | sed -n '1,12p' \
            || true
        )"
        echo "  pid $pid fd categories:"
        if [[ -n "$fd_categories" ]]; then
          echo "$fd_categories" | sed 's/^/    /'
        else
          echo "    unavailable"
        fi
        echo "  pid $pid fd mix:"
        if [[ -n "$fd_mix" ]]; then
          echo "$fd_mix" | sed 's/^/    /'
        else
          echo "    unavailable"
        fi
        if [[ -n "$fd_regular_sample" ]]; then
          echo "  pid $pid regular file sample:"
          echo "$fd_regular_sample" | sed 's/^/    /'
        fi
        samples=$((samples + 1))
      fi
    fi
  done

  if [[ "$live" -eq 0 ]]; then
    echo "  live server processes: 0"
    return
  fi

  echo "  live server processes: $live/$EXPECTED_PROCESSES"
  echo "  rss total/max: $((total_rss_kb / 1024)) MiB / $((max_rss_kb / 1024)) MiB"
  echo "  fd total/max: $total_fds / $max_fds"
}

cleanup() {
  local status=$?
  if [[ "$status" -ne 0 ]]; then
    print_resource_summary
  fi
  if [[ "$KEEP" == true ]]; then
    return
  fi
  for pid in "${PIDS[@]:-}"; do
    # Resume any chaos-paused (SIGSTOP) node so it can act on the term signal.
    kill -CONT "$pid" 2>/dev/null || true
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

# The offset search above already cleared this band, so a hit here means a port
# was claimed in the race between snapshot and start. Fail fast rather than test
# against a stale server.
for i in $(seq 1 "$EXPECTED_PROCESSES"); do
  for port in $((BASE_BROKER_PORT + i)) $((BASE_ADMIN_PORT + i)) $((BASE_RAFT_PORT + i)); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      exec 3>&- 2>/dev/null || true
      echo "FAIL: port $port got claimed after the offset search (port_offset=$PORT_OFFSET; concurrent bind?)" >&2
      exit 1
    fi
  done
done

if [[ "$STEADY_BENCH" == true || "$FAILOVER_VERIFY" == true || "$CHAOS" == true ]]; then
  echo "building release fibril-server, fibrilctl, and bench bins..."
  cargo build --quiet --release -p fibril -p fibril-cli
  [[ "$STEADY_BENCH" == true ]] && cargo build --quiet --release -p fibril-benches --bin steady_c
  if [[ "$FAILOVER_VERIFY" == true || "$CHAOS" == true ]]; then
    cargo build --quiet --release -p fibril-benches --bin failover_verify
  fi
  SERVER=target/release/fibril-server
  CTL=target/release/fibrilctl
  VERIFY_BIN=target/release/failover_verify
else
  echo "building fibril-server and fibrilctl..."
  cargo build --quiet -p fibril -p fibril-cli
  SERVER=target/debug/fibril-server
  CTL=target/debug/fibrilctl
fi

PEERS=""
if [[ "$GANGLION" == true ]]; then
  for i in $(seq 1 "$NODES"); do
    PEERS+="${PEERS:+,}$i=127.0.0.1:$((BASE_RAFT_PORT + i))"
  done
fi

start_node() {
  local i="$1"
  # On a restart (chaos kill + rejoin) the node recovers from its data dir and
  # rejoins the existing cluster, so it must NOT re-bootstrap. Pass skip_bootstrap.
  local skip_bootstrap="${2:-false}"
  local broker_port=$((BASE_BROKER_PORT + i))
  local admin_port=$((BASE_ADMIN_PORT + i))
  # Per-node data-dir override (poor-man's separate-disk test): if
  # CLUSTER_TRYOUT_NODE<i>_ROOT is set, this node's data lives there instead of
  # under RUN_DIR, so different nodes can sit on different filesystems.
  local node_root_var="CLUSTER_TRYOUT_NODE${i}_ROOT"
  local node_data="$RUN_DIR/node-$i"
  if [[ -n "${!node_root_var:-}" ]]; then
    mkdir -p "${!node_root_var}"
    node_data="${!node_root_var}/node-$i"
  fi
  local env_vars=(
    "FIBRIL_DATA_DIR=$node_data"
    "FIBRIL_BROKER_BIND=127.0.0.1:$broker_port"
    "FIBRIL_ADMIN_BIND=127.0.0.1:$admin_port"
  )
  if [[ "$GANGLION" == true ]]; then
    env_vars+=(
      "FIBRIL_COORDINATION_MODE=ganglion"
      "FIBRIL_COORDINATION_NODE_ID=broker-$i"
      "FIBRIL_COORDINATION_RAFT_ID=$i"
      "FIBRIL_COORDINATION_LISTEN=127.0.0.1:$((BASE_RAFT_PORT + i))"
      "FIBRIL_COORDINATION_PEERS=$PEERS"
    )
    if [[ -n "$ASSIGNMENT_DURABILITY" ]]; then
      env_vars+=("FIBRIL_COORDINATION_ASSIGNMENT_DURABILITY=$ASSIGNMENT_DURABILITY")
    fi
    if [[ -n "$COORDINATION_HEARTBEAT_INTERVAL_MS" ]]; then
      env_vars+=("FIBRIL_COORDINATION_HEARTBEAT_INTERVAL_MS=$COORDINATION_HEARTBEAT_INTERVAL_MS")
    fi
    if [[ -n "$COORDINATION_LIVENESS_TTL_MS" ]]; then
      env_vars+=("FIBRIL_COORDINATION_LIVENESS_TTL_MS=$COORDINATION_LIVENESS_TTL_MS")
    fi
    if [[ "$i" -eq 1 && "$skip_bootstrap" != true ]]; then
      env_vars+=("FIBRIL_COORDINATION_BOOTSTRAP=true")
    fi
  fi
  (
    # The caller may have many unrelated descriptors open. Close them in the
    # child before exec so high-node stress runs measure Fibril, not inherited
    # harness state.
    if [[ -d "/proc/$BASHPID/fd" ]]; then
      for fd_path in /proc/"$BASHPID"/fd/*; do
        fd="${fd_path##*/}"
        [[ "$fd" =~ ^[0-9]+$ ]] || continue
        if [[ "$fd" -gt 2 ]]; then
          eval "exec ${fd}>&-" 2>/dev/null || true
        fi
      done
    fi
    exec env "${env_vars[@]}" "$SERVER"
  ) >"$RUN_DIR/node-$i.log" 2>&1 &
  local pid="$!"
  # Assign by slot (not append) so a chaos restart of node i replaces its dead
  # pid in place. For the initial sequential 1..N start this matches appending.
  PIDS[$((i - 1))]="$pid"
  if [[ "$KEEP" == true ]]; then
    disown -h "$pid" 2>/dev/null || true
  fi
  echo "  node-$i: broker=127.0.0.1:$broker_port admin=127.0.0.1:$admin_port pid=$pid"
}

wait_admin() {
  local i="$1"
  local admin_port=$((BASE_ADMIN_PORT + i))
  local attempts=$((ADMIN_WAIT_SECS * 5))
  for attempt in $(seq 1 "$attempts"); do
    curl -sf "http://127.0.0.1:$admin_port/healthz" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "FAIL: node-$i admin endpoint never came up after ${ADMIN_WAIT_SECS}s; log tail:" >&2
  tail -20 "$RUN_DIR/node-$i.log" >&2
  exit 1
}

show_node() {
  local i="$1"
  "$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + $1))" admin topology --json 2>/dev/null \
    | jq -c '{leader: .consensus.leader, voters: .consensus.voters, healthy: .consensus.healthy, brokers: (.coordination.nodes | length)}'
}

wait_voters() {
  local expected="$1"
  local node_count="${2:-$NODES}"
  local last_seen=""
  for attempt in $(seq 1 "$(cluster_attempts)"); do
    local ready=true
    last_seen=""
    for i in $(seq 1 "$node_count"); do
      local json=""
      json="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + i))" admin topology --json 2>/dev/null || echo '{}')"
      local voters=""
      voters="$(echo "$json" | jq -c '.consensus.voters // []')"
      last_seen="${last_seen} node-$i=$voters"
      if [[ "$voters" != "$expected" ]]; then
        ready=false
        break
      fi
    done
    if [[ "$ready" == true ]]; then
      return 0
    fi
    sleep 0.3
  done
  echo "FAIL: coordination voting member set did not converge to $expected; last seen:$last_seen" >&2
  exit 1
}

current_leader_id() {
  local leader=""
  for attempt in $(seq 1 "$(cluster_attempts)"); do
    for i in $(seq 1 "$NODES"); do
      leader="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + i))" admin topology --json 2>/dev/null | jq -r '.consensus.leader // empty' || true)"
      if [[ "$leader" =~ ^[0-9]+$ ]]; then
        echo "$leader"
        return 0
      fi
    done
    sleep 0.3
  done
  echo "FAIL: could not determine current coordination leader" >&2
  exit 1
}

topology_from_any_live_node() {
  local i json
  for i in $(seq 1 "$NODES"); do
    if [[ -n "${PIDS[$((i - 1))]:-}" ]] && ! kill -0 "${PIDS[$((i - 1))]}" 2>/dev/null; then
      continue
    fi
    json="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + i))" admin topology --json 2>/dev/null || true)"
    if [[ -n "$json" && "$json" != "{}" ]]; then
      echo "$json"
      return 0
    fi
  done
  return 1
}

assignment_for_topic_from_json() {
  local json="$1"
  local topic="$2"
  echo "$json" | jq -c --arg topic "$topic" \
    '.coordination.assignments[]? | select(.topic == $topic and .partition == 0 and (.group == null))' \
    | sed -n '1p'
}

assignment_for_topic_partition_from_json() {
  local json="$1"
  local topic="$2"
  local partition="$3"
  echo "$json" | jq -c --arg topic "$topic" --argjson partition "$partition" \
    '.coordination.assignments[]? | select(.topic == $topic and .partition == $partition and (.group == null))' \
    | sed -n '1p'
}

wait_assignment_for_topic() {
  local topic="$1"
  local last=""
  local json=""
  for attempt in $(seq 1 "$(cluster_attempts)"); do
    json="$(topology_from_any_live_node || true)"
    if [[ -n "$json" ]]; then
      last="$(assignment_for_topic_from_json "$json" "$topic")"
      if [[ -n "$last" ]]; then
        echo "$last"
        return 0
      fi
    fi
    sleep 0.3
  done
  echo "FAIL: controller never assigned topic '$topic'; last seen: $last" >&2
  return 1
}

wait_all_nodes_see_topic_partition() {
  local topic="$1"
  local partition="$2"
  local ready=false
  local last_seen=""
  local json=""
  local assignment=""

  for attempt in $(seq 1 "$(cluster_attempts)"); do
    ready=true
    last_seen=""
    for i in $(seq 1 "$NODES"); do
      json="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + i))" admin topology --json 2>/dev/null || echo '{}')"
      assignment="$(assignment_for_topic_partition_from_json "$json" "$topic" "$partition")"
      last_seen="${last_seen} node-$i=${assignment:-missing}"
      if [[ -z "$assignment" ]]; then
        ready=false
        break
      fi
    done
    if [[ "$ready" == true ]]; then
      return 0
    fi
    sleep 0.3
  done

  echo "FAIL: partition $partition for topic '$topic' was not visible on every node; last seen:$last_seen" >&2
  return 1
}

wait_assignment_owner_change() {
  local topic="$1"
  local old_owner="$2"
  local assignment=""
  local owner=""
  for attempt in $(seq 1 "$(cluster_attempts)"); do
    assignment="$(wait_assignment_for_topic "$topic" 2>/dev/null || true)"
    if [[ -n "$assignment" ]]; then
      owner="$(echo "$assignment" | jq -r '.owner // empty')"
      if [[ -n "$owner" && "$owner" != "$old_owner" ]]; then
        echo "$assignment"
        return 0
      fi
    fi
    sleep 0.3
  done
  echo "FAIL: assignment for '$topic' did not move away from $old_owner" >&2
  return 1
}

assignment_durability_label() {
  local assignment="$1"
  local mode nodes
  mode="$(echo "$assignment" | jq -r '.durability.mode // "cluster-routed"')"
  nodes="$(echo "$assignment" | jq -r '.durability.nodes // empty')"
  if [[ -n "$nodes" ]]; then
    echo "$mode:$nodes"
  else
    echo "$mode"
  fi
}

wait_follower_progress() {
  local follower_node="$1"
  local topic="$2"
  local min_message_next="$3"
  local min_event_next="$4"
  local admin_port=$((BASE_ADMIN_PORT + follower_node))
  local last=""
  local json=""
  for attempt in $(seq 1 "$(cluster_attempts)"); do
    json="$(curl -sf "http://127.0.0.1:$admin_port/admin/api/queues_debug" 2>/dev/null || true)"
    if [[ -n "$json" ]]; then
      last="$(echo "$json" | jq -c --arg topic "$topic" \
        '.replication_followers[]? | select(.topic == $topic and .partition == 0 and (.group == null))' \
        | sed -n '1p')"
      if [[ -n "$last" ]] && echo "$last" | jq -e \
        --argjson min_message_next "$min_message_next" \
        --argjson min_event_next "$min_event_next" \
        '.state != null and (.state.message_next_offset >= $min_message_next) and (.state.event_next_offset >= $min_event_next)' >/dev/null; then
        echo "$last"
        return 0
      fi
    fi
    sleep 0.3
  done
  echo "FAIL: follower broker-$follower_node did not reach message_next>=$min_message_next event_next>=$min_event_next for '$topic'; last seen: $last" >&2
  return 1
}

check_bench_replication_tails() {
  local owner_node="$1"
  local follower_node="$2"
  local topic="$3"
  local min_tail="$4"
  local owner_port=$((BASE_ADMIN_PORT + owner_node))
  local follower_port=$((BASE_ADMIN_PORT + follower_node))
  local follower_id="broker-$follower_node"
  local owner_json=""
  local follower_json=""
  local owner_view=""
  local follower_state=""
  local last_owner=""
  local last_follower=""
  local owner_message_next=""
  local owner_event_next=""
  local owner_in_sync=""
  local follower_message_next=""
  local follower_event_next=""
  local invalid_offset="18446744073709551615"

  for attempt in $(seq 1 "$(cluster_attempts)"); do
    owner_json="$(curl -sf "http://127.0.0.1:$owner_port/admin/api/queues_debug" 2>/dev/null || true)"
    follower_json="$(curl -sf "http://127.0.0.1:$follower_port/admin/api/queues_debug" 2>/dev/null || true)"

    if [[ -n "$owner_json" && -n "$follower_json" ]]; then
      owner_view="$(echo "$owner_json" | jq -c --arg topic "$topic" --arg follower "$follower_id" \
        '.owned_replicas[]?
         | select(.topic == $topic and .partition == 0 and (.group == null))
         | .followers[]?
         | select(.node_id == $follower)' \
        | sed -n '1p')"
      follower_state="$(echo "$follower_json" | jq -c --arg topic "$topic" \
        '.replication_followers[]?
         | select(.topic == $topic and .partition == 0 and (.group == null))
         | .state' \
        | sed -n '1p')"
      last_owner="$owner_view"
      last_follower="$follower_state"

      if [[ -n "$owner_view" && -n "$follower_state" ]]; then
        owner_message_next="$(echo "$owner_view" | jq -r '.durable_message_next // empty')"
        owner_event_next="$(echo "$owner_view" | jq -r '.durable_event_next // empty')"
        owner_in_sync="$(echo "$owner_view" | jq -r '.in_sync // false')"
        follower_message_next="$(echo "$follower_state" | jq -r '.message_next_offset // empty')"
        follower_event_next="$(echo "$follower_state" | jq -r '.event_next_offset // empty')"

        if [[ "$owner_message_next" == "$follower_message_next" \
          && "$owner_event_next" == "$follower_event_next" \
          && "$owner_in_sync" == "true" \
          && "$owner_message_next" =~ ^[0-9]+$ \
          && "$owner_event_next" =~ ^[0-9]+$ \
          && "$follower_message_next" =~ ^[0-9]+$ \
          && "$follower_event_next" =~ ^[0-9]+$ \
          && "$owner_message_next" != "$invalid_offset" \
          && "$owner_event_next" != "$invalid_offset" \
          && "$follower_message_next" != "$invalid_offset" \
          && "$follower_event_next" != "$invalid_offset" \
          && "$owner_message_next" -ge "$min_tail" \
          && "$owner_event_next" -ge 1 ]]; then
          echo "  owner/follower replication cursors match: message_next=$owner_message_next event_next=$owner_event_next in_sync=true"
          return 0
        fi
      fi
    fi
    sleep 0.3
  done

  echo "FAIL: owner/follower replication cursors did not converge for '$topic'" >&2
  echo "  owner view: ${last_owner:-missing}" >&2
  echo "  follower view: ${last_follower:-missing}" >&2
  return 1
}

print_replication_cache_metrics() {
  local owner_node="$1"
  local follower_node="$2"
  local results_file="$3"
  local owner_port=$((BASE_ADMIN_PORT + owner_node))
  local follower_port=$((BASE_ADMIN_PORT + follower_node))
  local owner_metrics=""
  local follower_metrics=""
  local owner_timing=""
  local follower_timing=""

  owner_metrics="$(curl -sf "http://127.0.0.1:$owner_port/admin/api/queues_debug" 2>/dev/null \
    | jq -c '.replication_cache_metrics' 2>/dev/null || true)"
  follower_metrics="$(curl -sf "http://127.0.0.1:$follower_port/admin/api/queues_debug" 2>/dev/null \
    | jq -c '.replication_cache_metrics' 2>/dev/null || true)"
  owner_timing="$(curl -sf "http://127.0.0.1:$owner_port/admin/api/queues_debug" 2>/dev/null \
    | jq -c '.replication_timing' 2>/dev/null || true)"
  follower_timing="$(curl -sf "http://127.0.0.1:$follower_port/admin/api/queues_debug" 2>/dev/null \
    | jq -c '.replication_timing' 2>/dev/null || true)"

  echo "--- replication cache metrics: after steady run ---" >>"$results_file"
  echo "owner broker-$owner_node: ${owner_metrics:-missing}" >>"$results_file"
  echo "follower broker-$follower_node: ${follower_metrics:-missing}" >>"$results_file"
  echo "--- replication timing: after steady run ---" >>"$results_file"
  echo "owner broker-$owner_node: ${owner_timing:-missing}" >>"$results_file"
  echo "follower broker-$follower_node: ${follower_timing:-missing}" >>"$results_file"
  echo "  owner cache metrics: ${owner_metrics:-missing}"
  echo "  follower cache metrics: ${follower_metrics:-missing}"
  echo "  owner replication timing: ${owner_timing:-missing}"
  echo "  follower replication timing: ${follower_timing:-missing}"
}

run_steady_benchmark() {
  local broker_node="$1"
  local admin_node="$2"
  local durability="$3"
  local broker_addr="127.0.0.1:$((BASE_BROKER_PORT + broker_node))"
  local admin_addr="127.0.0.1:$((BASE_ADMIN_PORT + admin_node))"
  local bench_dir="$RUN_DIR/steady-bench"
  local results_file="$bench_dir/results.txt"
  local log_file="$bench_dir/bench.log"
  local summary_file="$bench_dir/summary.md"
  local min_tail=""

  mkdir -p "$bench_dir"
  echo
  echo "running steady benchmark against live tryout cluster..."
  echo "  topic=$BENCH_TOPIC broker=broker-$broker_node($broker_addr) admin=broker-$admin_node($admin_addr) durability=$durability"
  if ! env \
    START_SERVER=0 \
    BUILD=0 \
    BROKER_ADDR="$broker_addr" \
    ADMIN_ADDR="$admin_addr" \
    TOPIC="$BENCH_TOPIC" \
    DURABILITY_LABEL="$durability" \
    LOG_FILE="$log_file" \
    RESULTS_FILE="$results_file" \
    scripts/bench-steady-c.sh; then
    echo "FAIL: steady benchmark failed" >&2
    return 1
  fi
  scripts/bench-results-table.sh "$results_file" >"$summary_file"
  echo "  steady benchmark summary: $summary_file"
  sed -n '1,4p' "$summary_file"

  if [[ "$GANGLION" == true && -n "$BENCH_FOLLOWER_NODE" && "$BENCH_PARTITION_COUNT" -le 1 ]]; then
    min_tail="$(sed -n 's/^Confirmed total: //p' "$results_file" | head -n1)"
    if [[ -z "$min_tail" ]]; then
      min_tail="$(sed -n 's/^Sent total: //p' "$results_file" | head -n1)"
    fi
    if ! [[ "$min_tail" =~ ^[0-9]+$ ]] || [[ "$min_tail" -lt 1 ]]; then
      min_tail=1
    fi

    echo "  checking follower broker-$BENCH_FOLLOWER_NODE replicated at least $min_tail messages..."
    wait_follower_progress "$BENCH_FOLLOWER_NODE" "$BENCH_TOPIC" "$min_tail" 1 >/dev/null
    echo "  follower replication tail reached benchmark writes"
    check_bench_replication_tails "$BENCH_OWNER_NODE" "$BENCH_FOLLOWER_NODE" "$BENCH_TOPIC" "$min_tail"
    print_replication_cache_metrics "$BENCH_OWNER_NODE" "$BENCH_FOLLOWER_NODE" "$results_file"
  fi
}

run_failover_smoke() {
  local topic="${BENCH_TOPIC}-failover"
  local assignment=""
  local durability_mode=""
  local owner=""
  local owner_node=""
  local follower=""
  local follower_node=""
  local publish_node=1
  local payload="failover-pre-$RANDOM-$RANDOM"
  local post_payload="failover-post-$RANDOM-$RANDOM"
  local owner_pid=""
  local reassigned=""
  local new_owner=""
  local new_owner_node=""

  echo
  echo "running replica failover smoke with topic '$topic'..."
  if ! "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + 1))" queue declare "$topic" >/dev/null; then
    echo "FAIL: failover smoke failed to declare topic '$topic'" >&2
    return 1
  fi
  assignment="$(wait_assignment_for_topic "$topic")"
  echo "  initial assignment: $assignment"

  durability_mode="$(echo "$assignment" | jq -r '.durability.mode // "local_durable"')"
  if [[ "$durability_mode" != "replica_durable" && "$durability_mode" != "majority_durable" ]]; then
    echo "FAIL: failover smoke needs durable replica assignment, got $durability_mode" >&2
    return 1
  fi

  owner="$(echo "$assignment" | jq -r '.owner')"
  follower="$(echo "$assignment" | jq -r '.followers[0] // empty')"
  if [[ -z "$follower" ]]; then
    echo "FAIL: failover smoke assignment has no follower: $assignment" >&2
    return 1
  fi
  owner_node="${owner#broker-}"
  follower_node="${follower#broker-}"
  if ! [[ "$owner_node" =~ ^[0-9]+$ && "$follower_node" =~ ^[0-9]+$ ]]; then
    echo "FAIL: failover smoke owner/follower ids are not broker-N: owner=$owner follower=$follower" >&2
    return 1
  fi
  if [[ "$publish_node" -eq "$owner_node" ]]; then
    publish_node=2
  fi

  echo "  publishing 10 pre-failover messages through broker-$publish_node"
  for n in $(seq 1 10); do
    if ! "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + publish_node))" \
      queue publish "$topic" --message "$payload" >/dev/null; then
      echo "FAIL: failover smoke failed to publish pre-failover message $n" >&2
      return 1
    fi
  done

  echo "  waiting for follower $follower to apply replicated messages..."
  wait_follower_progress "$follower_node" "$topic" 10 10 >/dev/null || return 1

  echo "  waiting one broker heartbeat so follower tails reach coordination state..."
  sleep 4

  owner_pid="${PIDS[$((owner_node - 1))]}"
  echo "  killing owner $owner pid=$owner_pid"
  kill "$owner_pid" 2>/dev/null || true
  wait "$owner_pid" 2>/dev/null || true

  echo "  waiting for assignment to move away from $owner..."
  reassigned="$(wait_assignment_owner_change "$topic" "$owner")"
  echo "  failover assignment: $reassigned"
  new_owner="$(echo "$reassigned" | jq -r '.owner')"
  new_owner_node="${new_owner#broker-}"
  if ! [[ "$new_owner_node" =~ ^[0-9]+$ ]]; then
    echo "FAIL: failover owner is not broker-N: $new_owner" >&2
    return 1
  fi

  echo "  consuming pre-failover replicated messages through new owner $new_owner"
  if ! "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + new_owner_node))" \
    queue consume "$topic" --count 10 --timeout-ms 20000 --expect "$payload" >/dev/null; then
    echo "FAIL: failover smoke could not consume pre-failover replicated messages from $new_owner" >&2
    return 1
  fi

  echo "  publishing and consuming one post-failover message through $new_owner"
  if ! "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + new_owner_node))" \
    queue publish "$topic" --message "$post_payload" >/dev/null; then
    echo "FAIL: failover smoke failed to publish post-failover message" >&2
    return 1
  fi
  if ! "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + new_owner_node))" \
    queue consume "$topic" --timeout-ms 20000 --expect "$post_payload" >/dev/null; then
    echo "FAIL: failover smoke could not consume post-failover message from $new_owner" >&2
    return 1
  fi
  echo "  failover smoke delivered pre- and post-failover messages"
}

# Identity-based failover correctness: run the Jepsen-lite verifier (a confirmed
# producer + a concurrent consumer) against a SURVIVOR broker, kill the partition
# owner mid-run, and require zero loss (every confirmed id delivered) + no phantoms.
# Stronger than the smoke: it proves the durability contract under load + failover,
# not just that traffic resumes.
run_failover_verify() {
  local topic="${BENCH_TOPIC}-failover-verify"
  local assignment durability_mode owner follower owner_node follower_node connect_node
  local owner_pid verify_pid vexit
  local vdir="$RUN_DIR/failover-verify"
  local vlog="$vdir/verify.log"
  mkdir -p "$vdir"

  echo
  echo "running identity-based failover VERIFY with topic '$topic'..."
  if ! "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + 1))" queue declare "$topic" >/dev/null; then
    echo "FAIL: failover-verify could not declare '$topic'" >&2
    return 1
  fi
  assignment="$(wait_assignment_for_topic "$topic")"
  echo "  initial assignment: $assignment"
  durability_mode="$(echo "$assignment" | jq -r '.durability.mode // "local_durable"')"
  if [[ "$durability_mode" != "replica_durable" && "$durability_mode" != "majority_durable" ]]; then
    echo "FAIL: failover-verify needs a durable replica assignment, got $durability_mode" >&2
    return 1
  fi
  owner="$(echo "$assignment" | jq -r '.owner')"
  follower="$(echo "$assignment" | jq -r '.followers[0] // empty')"
  if [[ -z "$follower" ]]; then
    echo "FAIL: failover-verify assignment has no follower: $assignment" >&2
    return 1
  fi
  owner_node="${owner#broker-}"
  follower_node="${follower#broker-}"
  if ! [[ "$owner_node" =~ ^[0-9]+$ && "$follower_node" =~ ^[0-9]+$ ]]; then
    echo "FAIL: failover-verify owner/follower not broker-N: owner=$owner follower=$follower" >&2
    return 1
  fi
  # Connect to a SURVIVOR (the follower) so when the owner dies the client refreshes
  # topology and routes to the promoted owner (ride-through, not a dead bootstrap).
  connect_node="$follower_node"

  echo "  launching verifier (count=$FAILOVER_VERIFY_COUNT rate=$FAILOVER_VERIFY_RATE/s) against survivor broker-$connect_node; owner=broker-$owner_node will be killed mid-run"
  "$VERIFY_BIN" \
    --broker-addr "127.0.0.1:$((BASE_BROKER_PORT + connect_node))" \
    --topic "$topic" \
    --count "$FAILOVER_VERIFY_COUNT" \
    --rate-per-sec "$FAILOVER_VERIFY_RATE" \
    >"$vlog" 2>&1 &
  verify_pid=$!

  # Let load ramp and replicate so there are confirmed+replicated ids to lose.
  sleep 3
  owner_pid="${PIDS[$((owner_node - 1))]}"
  echo "  killing owner $owner pid=$owner_pid mid-run"
  kill "$owner_pid" 2>/dev/null || true
  wait "$owner_pid" 2>/dev/null || true

  echo "  waiting for the verifier to finish (producer + consumer ride-through)..."
  if wait "$verify_pid"; then vexit=0; else vexit=$?; fi

  echo "  --- verifier verdict ---"
  grep -E "CONFIRMED|RECEIVED|LOSS|PHANTOM|DUPLICATE|unconfirmed|PASS|FAIL" "$vlog" | sed 's/^/  /' || true
  echo "  ------------------------"
  if [[ "$vexit" -ne 0 ]]; then
    echo "FAIL: failover-verify reported loss/phantom (exit $vexit); full log: $vlog" >&2
    return 1
  fi
  echo "  failover-verify: every confirmed id survived the owner kill (zero loss, no phantoms)"
}

# Chaos soak: run a confirmed producer + consumer against a SURVIVOR while
# injecting a SEQUENCE of varied faults (kill+rejoin and SIGSTOP/SIGCONT pause),
# one at a time so the cluster always keeps quorum, then require zero loss + no
# phantoms AND that the cluster reconverged. Stronger than --failover-verify
# (one kill): this is repeated, mixed faults under sustained load.
run_chaos() {
  local topic="${BENCH_TOPIC}-chaos"
  local assignment durability_mode owner follower owner_node follower_node connect_node
  local verify_pid vexit round victim victim_pid count
  local vdir="$RUN_DIR/chaos"
  local vlog="$vdir/verify.log"
  local expected_all
  expected_all="$(seq -s, 1 "$NODES" | sed 's/^/[/' | sed 's/$/]/')"
  mkdir -p "$vdir"

  echo
  echo "running CHAOS soak with topic '$topic' (nodes=$NODES, rounds=$CHAOS_ROUNDS, load=${CHAOS_LOAD_SECS}s)..."
  if ! "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + 1))" queue declare "$topic" >/dev/null; then
    echo "FAIL: chaos could not declare '$topic'" >&2
    return 1
  fi
  assignment="$(wait_assignment_for_topic "$topic")" || return 1
  durability_mode="$(echo "$assignment" | jq -r '.durability.mode // "local_durable"')"
  if [[ "$durability_mode" != "replica_durable" && "$durability_mode" != "majority_durable" ]]; then
    echo "FAIL: chaos needs a durable replica assignment, got $durability_mode" >&2
    return 1
  fi
  owner="$(echo "$assignment" | jq -r '.owner')"
  follower="$(echo "$assignment" | jq -r '.followers[0] // empty')"
  owner_node="${owner#broker-}"
  follower_node="${follower#broker-}"
  # Connect the load to a node OUTSIDE this topic's replica set, so its bootstrap
  # is never faulted AND every owner fault forces the consumer to migrate to a new
  # owner - the path that exercises consumer ride-through and resume.
  connect_node=""
  for n in $(seq 1 "$NODES"); do
    if [[ "$n" -ne "$owner_node" && "$n" -ne "$follower_node" ]]; then
      connect_node="$n"
      break
    fi
  done
  if ! [[ "$connect_node" =~ ^[0-9]+$ ]]; then connect_node="$follower_node"; fi
  if ! [[ "$connect_node" =~ ^[0-9]+$ ]]; then connect_node=1; fi

  count=$((CHAOS_LOAD_SECS * FAILOVER_VERIFY_RATE))
  echo "  launching load (count=$count rate=$FAILOVER_VERIFY_RATE/s) against survivor broker-$connect_node; faulting others mid-run"
  "$VERIFY_BIN" \
    --broker-addr "127.0.0.1:$((BASE_BROKER_PORT + connect_node))" \
    --topic "$topic" \
    --count "$count" \
    --rate-per-sec "$FAILOVER_VERIFY_RATE" \
    >"$vlog" 2>&1 &
  verify_pid=$!

  sleep 3   # let load ramp and replicate before the first fault

  round=0
  while kill -0 "$verify_pid" 2>/dev/null && [[ "$round" -lt "$CHAOS_ROUNDS" ]]; do
    round=$((round + 1))
    # Fault a node IN this topic's replica set (prefer the current owner) so the
    # consumer is forced to migrate/recover every round. Query fresh since
    # ownership moves. Never the connect node. Fall back to any other node.
    cur="$(wait_assignment_for_topic "$topic" 2>/dev/null || echo "$assignment")"
    victim=""
    for cand in \
      "$(echo "$cur" | jq -r '.owner // empty' | sed 's/^broker-//')" \
      "$(echo "$cur" | jq -r '.followers[0] // empty' | sed 's/^broker-//')"; do
      if [[ "$cand" =~ ^[0-9]+$ && "$cand" -ne "$connect_node" ]]; then
        victim="$cand"
        break
      fi
    done
    if [[ -z "$victim" ]]; then
      victim="$connect_node"
      while [[ "$victim" -eq "$connect_node" ]]; do
        victim=$(((RANDOM % NODES) + 1))
      done
    fi
    victim_pid="${PIDS[$((victim - 1))]}"
    if (( round % 2 == 0 )); then
      echo "  [round $round/$CHAOS_ROUNDS] kill + rejoin broker-$victim (pid $victim_pid)"
      kill "$victim_pid" 2>/dev/null || true
      wait "$victim_pid" 2>/dev/null || true
      sleep 2                     # let the cluster notice and fail over if it owned anything
      start_node "$victim" true   # restart in place: recover + rejoin, no re-bootstrap
      wait_admin "$victim"
    else
      echo "  [round $round/$CHAOS_ROUNDS] pause + resume broker-$victim (SIGSTOP/SIGCONT, pid $victim_pid)"
      kill -STOP "$victim_pid" 2>/dev/null || true
      sleep 3                     # appear unreachable past a liveness tick
      kill -CONT "$victim_pid" 2>/dev/null || true
    fi
    # Restore a healthy single-fault baseline before the next round: wait for all
    # voters to be visible again so we never fault two nodes at once.
    wait_voters "$expected_all" "$NODES" >/dev/null 2>&1 || true
    sleep 1
  done

  echo "  injected $round fault round(s); waiting for the load to finish..."
  if wait "$verify_pid"; then vexit=0; else vexit=$?; fi

  echo "  --- verifier verdict ---"
  grep -E "CONFIRMED|RECEIVED|LOSS|PHANTOM|DUPLICATE|unconfirmed|PASS|FAIL" "$vlog" | sed 's/^/  /' || true
  echo "  ------------------------"
  if [[ "$vexit" -ne 0 ]]; then
    echo "FAIL: chaos soak reported loss/phantom (exit $vexit); full log: $vlog" >&2
    return 1
  fi

  echo "  verifying the cluster reconverged (all $NODES voters, one leader)..."
  wait_voters "$expected_all" "$NODES" || return 1
  current_leader_id >/dev/null || return 1
  echo "  chaos soak: zero confirmed-id loss across $round fault round(s), cluster reconverged"
}

run_repartition_smoke() {
  local topic="${BENCH_TOPIC}-repartition"
  local admin_node=1
  local publish_node=2
  local consume_node="$NODES"
  local grown=""
  local shrunk=""
  local count=""
  local version=""
  local payload=""

  if [[ "$publish_node" -gt "$NODES" ]]; then
    publish_node=1
  fi
  if [[ "$consume_node" -eq "$publish_node" && "$NODES" -ge 2 ]]; then
    consume_node=1
  fi

  echo
  echo "running live repartition smoke with topic '$topic'..."
  "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + 1))" queue declare "$topic" >/dev/null \
    || { echo "FAIL: repartition smoke failed to declare topic '$topic'" >&2; return 1; }
  wait_assignment_for_topic "$topic" >/dev/null || return 1

  echo "  growing $topic from 1 to 2 partitions..."
  grown="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + admin_node))" admin repartition "$topic" 2)"
  count="$(echo "$grown" | jq -r '.partitioning.partition_count // empty')"
  version="$(echo "$grown" | jq -r '.partitioning.partitioning_version // empty')"
  if [[ "$count" != "2" || -z "$version" ]]; then
    echo "FAIL: repartition grow returned unexpected response: $grown" >&2
    return 1
  fi
  wait_all_nodes_see_topic_partition "$topic" 1 || return 1
  echo "  grow visible cluster-wide at partitioning_version=$version"

  payload="repartition-grown-$RANDOM-$RANDOM"
  for n in $(seq 1 4); do
    "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + publish_node))" \
      queue publish "$topic" --partition-key "grown-$n" --message "$payload" >/dev/null \
      || { echo "FAIL: repartition smoke failed to publish grown message $n" >&2; return 1; }
  done
  "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + consume_node))" \
    queue consume "$topic" --count 4 --prefetch 4 --timeout-ms 20000 --expect "$payload" >/dev/null \
    || { echo "FAIL: repartition smoke failed to consume after grow" >&2; return 1; }

  echo "  shrinking $topic from 2 to 1 partition..."
  shrunk="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + admin_node))" admin repartition "$topic" 1)"
  count="$(echo "$shrunk" | jq -r '.partitioning.partition_count // empty')"
  version="$(echo "$shrunk" | jq -r '.partitioning.partitioning_version // empty')"
  if [[ "$count" != "1" || -z "$version" ]]; then
    echo "FAIL: repartition shrink returned unexpected response: $shrunk" >&2
    return 1
  fi
  echo "  shrink committed at partitioning_version=$version"

  payload="repartition-shrunk-$RANDOM-$RANDOM"
  for n in $(seq 1 4); do
    "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + publish_node))" \
      queue publish "$topic" --partition-key "shrunk-$n" --message "$payload" >/dev/null \
      || { echo "FAIL: repartition smoke failed to publish shrunk message $n" >&2; return 1; }
  done
  "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + consume_node))" \
    queue consume "$topic" --count 4 --prefetch 4 --timeout-ms 20000 --expect "$payload" >/dev/null \
    || { echo "FAIL: repartition smoke failed to consume after shrink" >&2; return 1; }
  echo "  repartition smoke delivered after grow and after shrink"
}

if [[ "$STAGGERED" == true ]]; then
  echo "staggered join: starting node 1 alone (bootstrap) under $RUN_DIR"
  start_node 1
  wait_admin 1
  sleep 1.5
  echo "  node-1 alone (no quorum expected): $(show_node 1)"

  echo "starting node 2 — quorum forms, election runs"
  start_node 2
  wait_admin 2
  for attempt in $(seq 1 "$(cluster_attempts)"); do
    leader="$(show_node 1 | jq -r '.leader')"
    [[ "$leader" != "null" && -n "$leader" ]] && break
    sleep 0.3
  done
  echo "  after node-2 joined: $(show_node 1)"

  for i in $(seq 3 "$NODES"); do
    echo "starting node $i — joins the running cluster"
    start_node "$i"
    wait_admin "$i"
  done
else
  echo "starting $NODES fibril servers under $RUN_DIR (ganglion=$GANGLION, port_offset=$PORT_OFFSET, admin_wait_secs=$ADMIN_WAIT_SECS, cluster_wait_secs=$CLUSTER_WAIT_SECS)"
  for i in $(seq 1 "$NODES"); do
    start_node "$i"
  done
fi

echo "waiting for admin endpoints..."
for i in $(seq 1 "$NODES"); do
  wait_admin "$i"
done

if [[ "$GANGLION" == true ]]; then
  echo "waiting for stable raft election and broker registration (up to ${CLUSTER_WAIT_SECS}s)..."
  stable_rounds=0
  cluster_stable=false
  for attempt in $(seq 1 "$(cluster_attempts)"); do
    ready=true
    expected_leader=""
    expected_voters=""
    for i in $(seq 1 "$NODES"); do
      json="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + i))" admin topology --json 2>/dev/null || echo '{}')"
      leader="$(echo "$json" | jq -r '.consensus.leader')"
      voters="$(echo "$json" | jq -c '.consensus.voters')"
      registered="$(echo "$json" | jq -r '.coordination.nodes | length')"
      if [[ "$leader" == "null" || -z "$leader" || "$registered" -lt "$NODES" ]]; then
        ready=false
        break
      fi
      if [[ -z "$expected_leader" ]]; then
        expected_leader="$leader"
        expected_voters="$voters"
      elif [[ "$leader" != "$expected_leader" || "$voters" != "$expected_voters" ]]; then
        ready=false
        break
      fi
    done
    if [[ "$ready" == true ]]; then
      stable_rounds=$((stable_rounds + 1))
      if [[ "$stable_rounds" -ge "$STABLE_ROUNDS_REQUIRED" ]]; then
        cluster_stable=true
        break
      fi
    else
      stable_rounds=0
    fi
    sleep 0.3
  done
  if [[ "$cluster_stable" != true ]]; then
    echo "FAIL: raft leader or broker registry did not stay stable for $STABLE_ROUNDS_REQUIRED consecutive checks" >&2
    exit 1
  fi
fi

echo
FAILED=0
BENCH_BROKER_NODE=1
BENCH_ADMIN_NODE=1
BENCH_DURABILITY_LABEL="standalone"
BENCH_OWNER_NODE=""
BENCH_FOLLOWER_NODE=""
declare -a LEADERS=()
declare -a VOTERS=()
for i in $(seq 1 "$NODES"); do
  broker_port=$((BASE_BROKER_PORT + i))
  admin_port=$((BASE_ADMIN_PORT + i))
  if [[ "$SUMMARY" == true ]]; then
    echo "=== node-$i topology summary ==="
    show_node "$i" || FAILED=1
  else
    echo "=== node-$i topology (fibrilctl --admin 127.0.0.1:$admin_port admin topology) ==="
    "$CTL" --admin "127.0.0.1:$admin_port" admin topology || FAILED=1
  fi

  json="$("$CTL" --admin "127.0.0.1:$admin_port" admin topology --json)"
  if [[ "$GANGLION" == true ]]; then
    # Shared-cluster checks: a real raft block with one leader and all voters.
    leader="$(echo "$json" | jq -r '.consensus.leader')"
    voters="$(echo "$json" | jq -c '.consensus.voters')"
    if [[ "$leader" == "null" ]]; then
      echo "FAIL: node-$i reports no raft leader" >&2
      FAILED=1
    fi
    LEADERS+=("$leader")
    VOTERS+=("$voters")
    # Every broker must have self-registered into the shared node table.
    registered="$(echo "$json" | jq -r '.coordination.nodes | length')"
    if [[ "$registered" -lt "$NODES" ]]; then
      echo "FAIL: node-$i sees only $registered/$NODES registered brokers" >&2
      FAILED=1
    fi
  else
    # Standalone check: the node reports its own broker address.
    reported="$(echo "$json" | jq -r '.coordination.nodes[0].broker_addr')"
    if [[ "$reported" != "127.0.0.1:$broker_port" ]]; then
      echo "FAIL: node-$i reported broker_addr=$reported, expected 127.0.0.1:$broker_port" >&2
      FAILED=1
    fi
  fi
  echo
done

if [[ "$GANGLION" == true ]]; then
  for value in "${LEADERS[@]}"; do
    if [[ "$value" != "${LEADERS[0]}" ]]; then
      echo "FAIL: nodes disagree on the raft leader: ${LEADERS[*]}" >&2
      FAILED=1
      break
    fi
  done
  for value in "${VOTERS[@]}"; do
    if [[ "$value" != "${VOTERS[0]}" ]]; then
      echo "FAIL: nodes disagree on the voter set: ${VOTERS[*]}" >&2
      FAILED=1
      break
    fi
  done
  if [[ "$FAILED" -ne 0 ]]; then
    echo "cluster-tryout: FAILED"
    exit 1
  fi

  if [[ "$DYNAMIC_MEMBERSHIP" == true ]]; then
    joiner=$((NODES + 1))
    joiner_raft_addr="127.0.0.1:$((BASE_RAFT_PORT + joiner))"
    leader_id="$(current_leader_id)"
    leader_admin="127.0.0.1:$((BASE_ADMIN_PORT + leader_id))"
    expected_with_joiner="$(seq -s, 1 "$joiner" | sed 's/^/[/' | sed 's/$/]/')"
    expected_initial="$(seq -s, 1 "$NODES" | sed 's/^/[/' | sed 's/$/]/')"

    echo "starting node-$joiner as a live membership joiner..."
    start_node "$joiner"
    wait_admin "$joiner"

    echo "adding node-$joiner as a coordination voting member through leader node-$leader_id..."
    "$CTL" --admin "$leader_admin" admin coordination add-voting-member "$joiner" "$joiner_raft_addr" >/dev/null
    wait_voters "$expected_with_joiner" "$joiner"
    echo "  voting members after add: $expected_with_joiner"

    leader_id="$(current_leader_id)"
    leader_admin="127.0.0.1:$((BASE_ADMIN_PORT + leader_id))"
    echo "removing node-$joiner from coordination voting members through leader node-$leader_id..."
    "$CTL" --admin "$leader_admin" admin coordination remove-voting-member "$joiner" >/dev/null
    wait_voters "$expected_initial" "$NODES"
    echo "  voting members after remove: $expected_initial"
  fi

  # Declare a queue through the data-plane CLI on node 1; the embedded
  # controller must assign it (owner + follower + epoch) cluster-wide.
  echo "declaring queue '$BENCH_TOPIC' and waiting for controller assignment (up to ${CLUSTER_WAIT_SECS}s)..."
  "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + 1))" queue declare "$BENCH_TOPIC" >/dev/null
  assigned="$(wait_assignment_for_topic "$BENCH_TOPIC" || true)"
  if [[ -z "$assigned" ]]; then
    echo "FAIL: controller never assigned the declared queue" >&2
    FAILED=1
  else
    echo "  assignment: $assigned"
    owner="$(echo "$assigned" | jq -r '.owner')"
    epoch="$(echo "$assigned" | jq -r '.epoch')"
    if [[ -z "$owner" || "$owner" == "null" || "$epoch" -lt 1 ]]; then
      echo "FAIL: assignment missing owner or epoch: $assigned" >&2
      FAILED=1
    fi
    # Every node should converge to the same committed assignment. On larger
    # all-voter stress runs, some followers can expose the commit a little
    # later than the first node that observed it.
    for i in $(seq 1 "$NODES"); do
      assignment_synced=false
      node_view=""
      for attempt in $(seq 1 "$(cluster_attempts)"); do
        node_json="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + i))" admin topology --json 2>/dev/null || echo '{}')"
        node_view="$(assignment_for_topic_from_json "$node_json" "$BENCH_TOPIC")"
        if [[ "$node_view" == "$assigned" ]]; then
          assignment_synced=true
          break
        fi
        sleep 0.3
      done
      if [[ "$assignment_synced" != true ]]; then
        echo "FAIL: node-$i never converged to the declared assignment; last seen: $node_view" >&2
        FAILED=1
      fi
    done

    owner_node="${owner#broker-}"
    if ! [[ "$owner_node" =~ ^[0-9]+$ ]]; then
      echo "FAIL: assignment owner is not a broker-N id: $owner" >&2
      FAILED=1
    else
      publish_node=1
      if [[ "$publish_node" -eq "$owner_node" && "$NODES" -ge 2 ]]; then
        publish_node=2
      fi
      consume_node="$NODES"
      if [[ "$consume_node" -eq "$publish_node" && "$NODES" -ge 2 ]]; then
        consume_node=1
      fi
      BENCH_BROKER_NODE="$publish_node"
      BENCH_ADMIN_NODE="$publish_node"
      BENCH_DURABILITY_LABEL="$(assignment_durability_label "$assigned")"
      BENCH_OWNER_NODE="$owner_node"
      follower="$(echo "$assigned" | jq -r '.followers[0] // empty')"
      follower_node="${follower#broker-}"
      if [[ -n "$follower" && "$follower_node" =~ ^[0-9]+$ ]]; then
        BENCH_FOLLOWER_NODE="$follower_node"
      fi
      payload="cluster-smoke-$RANDOM-$RANDOM"

      echo "publishing and consuming one message through public client routing..."
      echo "  owner=$owner publish_node=broker-$publish_node consume_node=broker-$consume_node"
      "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + publish_node))" \
        queue publish "$BENCH_TOPIC" --message "$payload" >/dev/null \
        || { echo "FAIL: publish smoke failed" >&2; FAILED=1; }
      "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + consume_node))" \
        queue consume "$BENCH_TOPIC" --timeout-ms 10000 --expect "$payload" >/dev/null \
        || { echo "FAIL: consume smoke failed" >&2; FAILED=1; }
      if [[ "$FAILED" -eq 0 ]]; then
        echo "  data-plane smoke delivered expected payload"
      fi
    fi
  fi

  # Replicated runtime settings: PUT on node 1 must become effective on all.
  echo "updating runtime settings on node-1 and waiting for cluster sync (up to ${CLUSTER_WAIT_SECS}s)..."
  current="$(curl -sf "http://127.0.0.1:$((BASE_ADMIN_PORT + 1))/admin/api/runtime-settings")"
  expected_version="$(echo "$current" | jq -r '.version')"
  new_ttl="$(( $(echo "$current" | jq -r '.settings.delivery.inflight_ttl_ms') + 111 ))"
  settings="$(echo "$current" | jq -c --argjson ttl "$new_ttl" \
    '.settings | .delivery.inflight_ttl_ms = $ttl')"
  if [[ -n "${REPLICATION_CAUGHT_UP_POLL_MS:-}" ]]; then
    settings="$(echo "$settings" | jq -c --argjson value "$REPLICATION_CAUGHT_UP_POLL_MS" \
      '.replication.caught_up_poll_ms = $value')"
  fi
  if [[ -n "${REPLICATION_RETRY_POLL_MS:-}" ]]; then
    settings="$(echo "$settings" | jq -c --argjson value "$REPLICATION_RETRY_POLL_MS" \
      '.replication.retry_poll_ms = $value')"
  fi
  if [[ -n "${REPLICATION_CHECKPOINT_RETRY_POLL_MS:-}" ]]; then
    settings="$(echo "$settings" | jq -c --argjson value "$REPLICATION_CHECKPOINT_RETRY_POLL_MS" \
      '.replication.checkpoint_retry_poll_ms = $value')"
  fi
  if [[ -n "${REPLICATION_MAX_MESSAGES_PER_READ:-}" ]]; then
    settings="$(echo "$settings" | jq -c --argjson value "$REPLICATION_MAX_MESSAGES_PER_READ" \
      '.replication.max_messages_per_read = $value')"
  fi
  if [[ -n "${REPLICATION_MAX_EVENTS_PER_READ:-}" ]]; then
    settings="$(echo "$settings" | jq -c --argjson value "$REPLICATION_MAX_EVENTS_PER_READ" \
      '.replication.max_events_per_read = $value')"
  fi
  if [[ -n "${REPLICATION_MAX_BYTES_PER_READ:-}" ]]; then
    settings="$(echo "$settings" | jq -c --argjson value "$REPLICATION_MAX_BYTES_PER_READ" \
      '.replication.max_bytes_per_read = $value')"
  fi
  if [[ -n "${REPLICATION_MAX_ITERATIONS_PER_TICK:-}" ]]; then
    settings="$(echo "$settings" | jq -c --argjson value "$REPLICATION_MAX_ITERATIONS_PER_TICK" \
      '.replication.max_iterations_per_tick = $value')"
  fi
  if [[ -n "${REPLICATION_STREAM_APPLY_LINGER_US:-}" ]]; then
    settings="$(echo "$settings" | jq -c --argjson value "$REPLICATION_STREAM_APPLY_LINGER_US" \
      '.replication.stream_apply_linger_us = $value')"
  fi
  body="$(jq -cn --argjson v "$expected_version" --argjson settings "$settings" \
    '{expected_version: $v, settings: $settings}')"
  curl -sf -X PUT -H 'content-type: application/json' -d "$body" \
    "http://127.0.0.1:$((BASE_ADMIN_PORT + 1))/admin/api/runtime-settings" >/dev/null \
    || { echo "FAIL: runtime settings PUT failed" >&2; FAILED=1; }
  for i in $(seq 2 "$NODES"); do
    synced=false
    for attempt in $(seq 1 "$(cluster_attempts)"); do
      got="$(curl -sf "http://127.0.0.1:$((BASE_ADMIN_PORT + i))/admin/api/runtime-settings" \
        | jq -r '.settings.delivery.inflight_ttl_ms')"
      if [[ "$got" == "$new_ttl" ]]; then synced=true; break; fi
      sleep 0.3
    done
    if [[ "$synced" == true ]]; then
      echo "  node-$i runtime settings synced (inflight_ttl_ms=$new_ttl)"
    else
      echo "FAIL: node-$i never received the replicated settings" >&2
      FAILED=1
    fi
  done

  if [[ "$FAILED" -eq 0 ]]; then
    echo "shared cluster confirmed: leader=${LEADERS[0]} voters=${VOTERS[0]} on all $NODES nodes"
  fi
fi

if [[ "$REPARTITION_SMOKE" == true && "$FAILED" -eq 0 ]]; then
  run_repartition_smoke || FAILED=1
fi

if [[ "$STEADY_BENCH" == true ]]; then
  if [[ "$GANGLION" != true ]]; then
    echo "declaring queue '$BENCH_TOPIC' on standalone node-1 for steady benchmark..."
    "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + 1))" queue declare "$BENCH_TOPIC" >/dev/null \
      || { echo "FAIL: standalone benchmark queue declare failed" >&2; FAILED=1; }
  fi
  if [[ "$FAILED" -eq 0 && "$BENCH_PARTITION_COUNT" -gt 1 ]]; then
    echo "repartitioning '$BENCH_TOPIC' to $BENCH_PARTITION_COUNT partitions for the bench..."
    repart="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + 1))" admin repartition "$BENCH_TOPIC" "$BENCH_PARTITION_COUNT")"
    got_count="$(echo "$repart" | jq -r '.partitioning.partition_count // empty')"
    if [[ "$got_count" != "$BENCH_PARTITION_COUNT" ]]; then
      echo "FAIL: repartition to $BENCH_PARTITION_COUNT returned: $repart" >&2
      FAILED=1
    elif [[ "$GANGLION" == true ]]; then
      wait_all_nodes_see_topic_partition "$BENCH_TOPIC" "$((BENCH_PARTITION_COUNT - 1))" || FAILED=1
      echo "  $BENCH_TOPIC now has $BENCH_PARTITION_COUNT partitions visible cluster-wide"
    fi
  fi
  if [[ "$FAILED" -eq 0 ]]; then
    run_steady_benchmark "$BENCH_BROKER_NODE" "$BENCH_ADMIN_NODE" "$BENCH_DURABILITY_LABEL" \
      || FAILED=1
  fi
fi

if [[ "$FAILOVER_SMOKE" == true && "$FAILED" -eq 0 ]]; then
  run_failover_smoke || FAILED=1
fi

if [[ "$FAILOVER_VERIFY" == true && "$FAILED" -eq 0 ]]; then
  run_failover_verify || FAILED=1
fi

if [[ "$CHAOS" == true && "$FAILED" -eq 0 ]]; then
  run_chaos || FAILED=1
fi

if [[ "$FAILED" -ne 0 ]]; then
  echo "cluster-tryout: FAILED"
  exit 1
fi

if [[ "$CHAOS" == true ]]; then
  echo "cluster-tryout: all checks passed, including a chaos soak (repeated mixed faults under load, zero loss, reconverged)"
elif [[ "$FAILOVER_VERIFY" == true ]]; then
  echo "cluster-tryout: all checks passed, including identity-verified zero-loss failover under load"
elif [[ "$FAILOVER_SMOKE" == true ]]; then
  echo "cluster-tryout: all checks passed, including intentional owner kill/failover"
elif [[ "$REPARTITION_SMOKE" == true ]]; then
  echo "cluster-tryout: all checks passed, including live repartition grow/shrink"
else
  echo "cluster-tryout: all $NODES servers serve topology correctly"
fi
print_resource_summary
if [[ "$KEEP" == true ]]; then
  echo
  echo "cluster left running (--keep). Try:"
  for i in $(seq 1 "$NODES"); do
    echo "  $CTL --admin 127.0.0.1:$((BASE_ADMIN_PORT + i)) admin topology"
  done
  echo "logs/data: $RUN_DIR ; stop with: kill ${PIDS[*]}"
fi

if [[ "$HOLD" == true ]]; then
  echo
  echo "cluster held open (--hold). Admin dashboards:"
  for i in $(seq 1 "$NODES"); do
    admin_port=$((BASE_ADMIN_PORT + i))
    echo "  node-$i: http://127.0.0.1:$admin_port/   (topology: http://127.0.0.1:$admin_port/admin/topology)"
  done
  echo "logs/data: $RUN_DIR"
  echo
  echo "press Ctrl-C to stop all $NODES servers and clean up."
  # Stay attached. Ctrl-C (or a TERM) runs this trap, and the EXIT trap then
  # tears every node down because KEEP is false in hold mode.
  trap 'echo; echo "stopping cluster..."; exit 0' INT TERM
  wait
fi
