#!/bin/sh
# Fibril cluster tryout. Brings up a local three-broker coordinated cluster with
# Docker, no clone and no build required.
#
#   curl -fsSL fibril.sh/tryout.sh | sh
#
# Clustering is experimental and not yet production-ready high availability.
set -eu

REPO="${FIBRIL_REPO:-Axmouth/fibril}"
REF="${FIBRIL_REF:-main}"
COMPOSE_URL="${FIBRIL_COMPOSE_URL:-https://raw.githubusercontent.com/${REPO}/${REF}/compose.cluster.example.yaml}"
WORKDIR="${FIBRIL_TRYOUT_DIR:-fibril-tryout}"

err() { printf 'fibril tryout: %s\n' "$1" >&2; exit 1; }

command -v docker >/dev/null 2>&1 || err "docker is required (see https://docs.docker.com/get-docker/)"
docker compose version >/dev/null 2>&1 || err "the docker compose plugin is required (see https://docs.docker.com/compose/)"

mkdir -p "$WORKDIR"
printf 'fibril tryout: fetching the cluster compose file...\n'
if command -v curl >/dev/null 2>&1; then
  curl -fsSL "$COMPOSE_URL" -o "$WORKDIR/compose.yaml"
elif command -v wget >/dev/null 2>&1; then
  wget -qO "$WORKDIR/compose.yaml" "$COMPOSE_URL"
else
  err "curl or wget is required"
fi

printf 'fibril tryout: starting a 3-broker cluster (the first run pulls the image)...\n'
( cd "$WORKDIR" && docker compose up -d )

cat <<EOF

Fibril cluster is starting. A demo world of realistic fake traffic (orders,
freight, hotel bookings, live streams) runs alongside the brokers, so every
dashboard page has something to show within a couple of minutes. Open a
dashboard and watch:

  http://127.0.0.1:8081/      http://127.0.0.1:8082/      http://127.0.0.1:8083/

Connect a client to any broker at 127.0.0.1:9876, :9877, or :9878.

Stop and remove everything, including the data volumes:

  ( cd "$WORKDIR" && docker compose down -v )
EOF
