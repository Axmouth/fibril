FROM rust:1.96-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY benches ./benches
COPY crates ./crates
RUN cargo build --release -p fibril --bin fibril-server -p fibril-cli --bin fibrilctl \
    -p fibril-tui-example --bin fibril-tui-example

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

VOLUME ["/app/server_data"]
EXPOSE 9876 8081

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl --fail --silent --show-error http://127.0.0.1:8081/healthz || exit 1

CMD ["fibril-server"]
