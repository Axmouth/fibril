#!/bin/sh
# Fibril visualizer tryout. Brings up one broker and drops you into a live terminal
# visualizer of real wire traffic and partition routing, with Docker, no clone and
# no build required.
#
#   curl -fsSL fibril.sh/viz.sh | sh
#
# It animates publishes, confirms, deliveries, acks, pings and errors as moving
# dots across partition lanes, with a metrics HUD. Press q to quit, which also
# tears the broker down.
set -eu

REPO="${FIBRIL_REPO:-Axmouth/fibril}"
REF="${FIBRIL_REF:-main}"
COMPOSE_URL="${FIBRIL_COMPOSE_URL:-https://raw.githubusercontent.com/${REPO}/${REF}/compose.viz.example.yaml}"
WORKDIR="${FIBRIL_VIZ_DIR:-fibril-viz}"

err() { printf 'fibril viz: %s\n' "$1" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || err "docker is required (see https://docs.docker.com/get-docker/)"
docker compose version >/dev/null 2>&1 || err "the docker compose plugin is required (see https://docs.docker.com/compose/)"

# The visualizer is interactive, so it needs a real terminal. When this script is
# piped into sh, stdin is the script text rather than your keyboard, so the run
# below reattaches the controlling terminal explicitly. Without one (e.g. CI)
# there is nothing to drive.
[ -r /dev/tty ] || err "the visualizer needs an interactive terminal (no /dev/tty available)"

mkdir -p "$WORKDIR"
printf 'fibril viz: fetching the visualizer compose file...\n'
if command -v curl >/dev/null 2>&1; then
  curl -fsSL "$COMPOSE_URL" -o "$WORKDIR/compose.yaml"
elif command -v wget >/dev/null 2>&1; then
  wget -qO "$WORKDIR/compose.yaml" "$COMPOSE_URL"
else
  err "curl or wget is required"
fi

cleanup() {
  printf '\nfibril viz: stopping the broker and removing its data...\n'
  ( cd "$WORKDIR" && docker compose down -v >/dev/null 2>&1 || true )
}
trap cleanup EXIT INT TERM

printf 'fibril viz: starting a broker and the visualizer (the first run pulls the image)...\n'
# Run the visualizer attached to the real terminal. `compose run` brings the broker
# up as its dependency; the trap tears it back down when the visualizer exits.
( cd "$WORKDIR" && docker compose run --rm viz ) < /dev/tty
