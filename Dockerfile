FROM rust:1.96-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY scripts ./scripts
COPY benches ./benches
COPY crates ./crates

# Build against the git-sourced Keratin and Ganglion dependencies. The committed
# Cargo.toml carries local [patch] overrides pointing at sibling ../keratin and
# ../ganglion checkouts for everyday development, which are not in the build
# context. Stripping them here makes the image build self-contained, so a plain
# `docker build .` works with no caller-side prep. The lockfile is in local-patch
# form, so cargo re-resolves the git deps (no --locked).
RUN scripts/stroma-source.sh git

RUN cargo build --release -p fibril --bin fibril-server -p fibril-cli --bin fibrilctl \
    -p fibril-tui-example --bin fibril-tui-example -p fibril-demo --bin fibril-demo

FROM debian:bookworm-slim

ARG VCS_REF=unknown

LABEL org.opencontainers.image.title="Fibril server" \
      org.opencontainers.image.description="Pre-alpha Fibril message broker server" \
      org.opencontainers.image.source="https://github.com/Axmouth/fibril" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="MIT"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/fibril-server /usr/local/bin/fibril-server
COPY --from=builder /app/target/release/fibrilctl /usr/local/bin/fibrilctl
COPY --from=builder /app/target/release/fibril-tui-example /usr/local/bin/fibril-tui-example
COPY --from=builder /app/target/release/fibril-demo /usr/local/bin/fibril-demo

VOLUME ["/app/server_data"]
EXPOSE 9876 8081

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl --fail --silent --show-error http://127.0.0.1:8081/healthz || exit 1

CMD ["fibril-server"]
