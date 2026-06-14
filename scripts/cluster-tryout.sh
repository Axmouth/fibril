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
#   scripts/cluster-tryout.sh --nodes 100 --ganglion --summary --resource-summary --admin-wait-secs 5 --cluster-wait-secs 90
#   scripts/cluster-tryout.sh --keep       # leave the cluster running to play with
#
# In --ganglion mode the servers share an embedded raft coordinator over real
# TCP: the script asserts all nodes agree on one leader and voter set.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NODES=3
KEEP=false
GANGLION=false
STAGGERED=false
SUMMARY=false
PORT_OFFSET=""
RESOURCE_SUMMARY=false
DYNAMIC_MEMBERSHIP=false
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
    --ganglion) GANGLION=true; shift ;;
    --staggered) GANGLION=true; STAGGERED=true; shift ;;
    --summary) SUMMARY=true; shift ;;
    --port-offset) PORT_OFFSET="$2"; shift 2 ;;
    --resource-summary) RESOURCE_SUMMARY=true; shift ;;
    --dynamic-membership) GANGLION=true; DYNAMIC_MEMBERSHIP=true; shift ;;
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

cluster_attempts() {
  # Cluster checks sleep 0.3s between attempts. Round up so the configured
  # seconds are a real lower bound, not an accidental shorter window.
  echo $(((CLUSTER_WAIT_SECS * 10 + 2) / 3))
}

if [[ -z "$PORT_OFFSET" ]]; then
  PORT_OFFSET=$(( (RANDOM % 20) * 1000 ))
fi
BASE_BROKER_PORT=$((BASE_BROKER_PORT + PORT_OFFSET))
BASE_ADMIN_PORT=$((BASE_ADMIN_PORT + PORT_OFFSET))
BASE_RAFT_PORT=$((BASE_RAFT_PORT + PORT_OFFSET))

RUN_DIR="$(mktemp -d /tmp/fibril-cluster-tryout.XXXXXX)"
declare -a PIDS=()
EXPECTED_PROCESSES="$NODES"
if [[ "$DYNAMIC_MEMBERSHIP" == true ]]; then
  EXPECTED_PROCESSES=$((NODES + 1))
fi

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
    kill "$pid" 2>/dev/null || true
  done
  for pid in "${PIDS[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT

# Fail fast if a previous --keep cluster still holds our ports; testing
# stale servers produces confusing results.
for i in $(seq 1 "$EXPECTED_PROCESSES"); do
  for port in $((BASE_BROKER_PORT + i)) $((BASE_ADMIN_PORT + i)) $((BASE_RAFT_PORT + i)); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      exec 3>&- 2>/dev/null || true
      echo "FAIL: port $port already in use (port_offset=$PORT_OFFSET; old --keep cluster or overlapping tryout?)" >&2
      exit 1
    fi
  done
done

echo "building fibril-server and fibrilctl..."
cargo build --quiet -p fibril -p fibril-cli
SERVER=target/debug/fibril-server
CTL=target/debug/fibrilctl

PEERS=""
if [[ "$GANGLION" == true ]]; then
  for i in $(seq 1 "$NODES"); do
    PEERS+="${PEERS:+,}$i=127.0.0.1:$((BASE_RAFT_PORT + i))"
  done
fi

start_node() {
  local i="$1"
  local broker_port=$((BASE_BROKER_PORT + i))
  local admin_port=$((BASE_ADMIN_PORT + i))
  local env_vars=(
    "FIBRIL_DATA_DIR=$RUN_DIR/node-$i"
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
    if [[ "$i" -eq 1 ]]; then
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
  PIDS+=("$pid")
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
    | jq -c '{leader: .raft.leader, voters: .raft.voters, healthy: .raft.healthy, brokers: (.coordination.nodes | length)}'
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
      voters="$(echo "$json" | jq -c '.raft.voters // []')"
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
      leader="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + i))" admin topology --json 2>/dev/null | jq -r '.raft.leader // empty' || true)"
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
      leader="$(echo "$json" | jq -r '.raft.leader')"
      voters="$(echo "$json" | jq -c '.raft.voters')"
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
    leader="$(echo "$json" | jq -r '.raft.leader')"
    voters="$(echo "$json" | jq -c '.raft.voters')"
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
  echo "declaring queue 'orders' and waiting for controller assignment (up to ${CLUSTER_WAIT_SECS}s)..."
  "$CTL" --broker "127.0.0.1:$((BASE_BROKER_PORT + 1))" queue declare orders >/dev/null
  assigned=""
  for attempt in $(seq 1 "$(cluster_attempts)"); do
    assigned="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + 1))" admin topology --json 2>/dev/null \
      | jq -c '.coordination.assignments[0] // empty')"
    [[ -n "$assigned" ]] && break
    sleep 0.3
  done
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
        node_view="$("$CTL" --admin "127.0.0.1:$((BASE_ADMIN_PORT + i))" admin topology --json 2>/dev/null \
          | jq -c '.coordination.assignments[0] // empty')"
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
  fi

  # Replicated runtime settings: PUT on node 1 must become effective on all.
  echo "updating runtime settings on node-1 and waiting for cluster sync (up to ${CLUSTER_WAIT_SECS}s)..."
  current="$(curl -sf "http://127.0.0.1:$((BASE_ADMIN_PORT + 1))/admin/api/runtime-settings")"
  expected_version="$(echo "$current" | jq -r '.version')"
  new_ttl="$(( $(echo "$current" | jq -r '.settings.delivery.inflight_ttl_ms') + 111 ))"
  body="$(echo "$current" | jq -c --argjson v "$expected_version" --argjson ttl "$new_ttl" \
    '{expected_version: $v, settings: (.settings | .delivery.inflight_ttl_ms = $ttl)}')"
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

if [[ "$FAILED" -ne 0 ]]; then
  echo "cluster-tryout: FAILED"
  exit 1
fi

echo "cluster-tryout: all $NODES servers serve topology correctly"
print_resource_summary
if [[ "$KEEP" == true ]]; then
  echo
  echo "cluster left running (--keep). Try:"
  for i in $(seq 1 "$NODES"); do
    echo "  $CTL --admin 127.0.0.1:$((BASE_ADMIN_PORT + i)) admin topology"
  done
  echo "logs/data: $RUN_DIR ; stop with: kill ${PIDS[*]}"
fi
