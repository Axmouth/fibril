---
title: Running Fibril
description: Current options for running the pre-alpha Fibril broker.
---

Fibril does not have a stable packaged release yet. For now, treat deployments
as experimental and run the broker from the `main` Docker image, a locally built
Docker image, a source checkout, or the latest `main` branch binary produced by
CI.

Do not expose the development server directly to the public internet without
reviewing the current defaults and putting it behind appropriate network
controls.

## Current server defaults

The current server binary:

- listens for broker TCP traffic on `0.0.0.0:9876`
- serves the early admin interface on `0.0.0.0:8081`
- stores durable state under `server_data/`
- uses development authentication defaults in the server binary

These defaults are expected to change as configuration and packaging mature.

## Run with Docker

The easiest current runtime path is the `main` image from GitHub Container
Registry:

```sh
docker pull ghcr.io/axmouth/fibril-server:main
docker run --rm \
  -p 9876:9876 \
  -p 8081:8081 \
  -v fibril-server-data:/app/server_data \
  ghcr.io/axmouth/fibril-server:main
```

The `main` tag is a moving pre-release image. CI also publishes immutable
`sha-<commit>` tags for exact builds.

The image exposes:

- `9876` for broker TCP traffic
- `8081` for the admin surface

Persistent data lives under `/app/server_data` in the container.

## Runtime knobs

The server now loads a typed startup config. Without a config file it uses the
same defaults as before.

Use a TOML config file with either:

```sh
cargo run --release --bin fibril-server -- --config fibril.toml
```

or:

```sh
FIBRIL_CONFIG=fibril.toml cargo run --release --bin fibril-server
```

The repository includes `fibril.example.toml` as a starting point.

Startup config precedence is:

```txt
compiled defaults < TOML config file < environment variables < CLI arguments
```

For the complete field reference, environment variables, CLI flags, runtime
seed behavior, and runtime locks, see [configuration](/latest/configuration/).

For sparse workloads, enable publisher idle expiry alongside queue cleanup;
otherwise a long-lived connection that published to a queue can keep that queue
active until the connection closes.

### Docker Compose

The repository includes a small Compose example using the published image:

```sh
docker compose -f compose.server.example.yaml up -d
docker compose -f compose.server.example.yaml ps
```

The Compose example maps the same ports, keeps data in a named volume, and uses
the admin `/healthz` endpoint for container health checks.

## Build a local Docker image

Build an image from the repository:

```sh
docker build -f Dockerfile -t fibril-server:local .
```

Run it with persistent server data:

```sh
docker run --rm \
  -p 9876:9876 \
  -p 8081:8081 \
  -v fibril-server-data:/app/server_data \
  fibril-server:local
```

## Run from a source checkout

Use this when you want the simplest path from the current repository state:

```sh
git clone https://github.com/Axmouth/fibril.git
cd fibril
cargo run --release --bin fibril-server
```

## Latest binary artifact

On pushes to `main`, CI also uploads the latest Linux server binary and checksum
as the `fibril-server-linux-x86_64-main` GitHub Actions artifact.

Use this only as a moving pre-release build. It tracks the current `main`
branch, so it can change without compatibility guarantees.

After downloading the artifact, verify the checksum before running it:

```sh
sha256sum -c fibril-server.sha256
chmod +x fibril-server
./fibril-server
```

## Health check

The admin surface exposes a lightweight health endpoint:

```sh
curl http://127.0.0.1:8081/healthz
```

Healthy servers return `ok`.

## Website deployment

The documentation website is separate from the broker process. It builds to static files and is served by nginx in a small container behind Traefik.

```sh
cd website
npm ci
npm run build
```
