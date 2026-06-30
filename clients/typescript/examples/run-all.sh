#!/usr/bin/env bash
# Run every *.example.ts against a real single-node broker as a light end-to-end
# smoke test. Starts a fresh broker, runs each example (each self-validates and
# exits non-zero on failure), tears the broker down, and exits non-zero if any
# example failed.
#
# Usage: clients/typescript/examples/run-all.sh
#   Reuse an already-running broker instead of starting one:
#     FIBRIL_ADDR=127.0.0.1:9876 FIBRIL_EXTERNAL_BROKER=1 examples/run-all.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
CLIENT_DIR="$(cd "$HERE/.." && pwd)"
REPO_ROOT="$(cd "$HERE/../../.." && pwd)"
PORT="${FIBRIL_PORT:-9876}"
ADDR="${FIBRIL_ADDR:-127.0.0.1:$PORT}"

# The examples import the published package surface (@fibril/client, which
# resolves to dist/index.js), so compile the client before running them.
echo "building @fibril/client..."
(cd "$CLIENT_DIR" && npm run --silent build)

if [[ "${FIBRIL_EXTERNAL_BROKER:-0}" != "1" ]]; then
  SERVER="$REPO_ROOT/target/debug/fibril-server"
  if [[ ! -x "$SERVER" ]]; then
    echo "building fibril-server..."
    (cd "$REPO_ROOT" && cargo build -q -p fibril)
  fi
  DATA_DIR="$(mktemp -d)"
  FIBRIL_DATA_DIR="$DATA_DIR" \
    FIBRIL_BROKER_BIND="127.0.0.1:$PORT" \
    FIBRIL_ADMIN_BIND="127.0.0.1:$((PORT + 1))" \
    "$SERVER" >"$DATA_DIR/server.log" 2>&1 &
  SERVER_PID=$!
  trap 'kill "$SERVER_PID" 2>/dev/null || true; rm -rf "$DATA_DIR"' EXIT
  # Wait for the broker to come up.
  for _ in $(seq 1 60); do
    grep -qiE "uptime|tcp" "$DATA_DIR/server.log" 2>/dev/null && break
    sleep 0.3
  done
  # Fail loudly if the broker died (for example the port was already taken by
  # another broker) rather than silently running the examples against whatever
  # else is listening on the port.
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "broker did not start (is port $PORT already in use?); server log:" >&2
    cat "$DATA_DIR/server.log" >&2
    exit 1
  fi
fi

failures=0
for example in "$HERE"/*.example.ts; do
  echo "=== $(basename "$example") ==="
  # --check makes continuous examples run a bounded, self-validating burst and
  # exit. Bounded examples ignore it. Run npx from the client dir so tsx resolves
  # regardless of where this script was invoked from.
  if ! (cd "$CLIENT_DIR" && FIBRIL_ADDR="$ADDR" npx tsx "$example" --check); then
    failures=$((failures + 1))
  fi
done

echo "-------------------------------------"
if [[ "$failures" -eq 0 ]]; then
  echo "all examples passed"
else
  echo "$failures example(s) failed"
fi
exit "$failures"
