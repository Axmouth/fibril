FROM rust:1.96-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY benches ./benches
COPY crates ./crates
RUN cargo build --release --locked -p fibril --bin fibril-server

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/fibril-server /usr/local/bin/fibril-server

VOLUME ["/app/server_data"]
EXPOSE 9876 8081

CMD ["fibril-server"]
