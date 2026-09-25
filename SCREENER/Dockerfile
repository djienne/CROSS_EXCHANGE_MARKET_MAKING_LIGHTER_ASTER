# syntax=docker/dockerfile:1
#
# The pair screener (README.md): public market data only, no credentials, no orders.
FROM rust:1.92-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked \
 && cp /app/target/release/screener /usr/local/bin/screener

FROM debian:bookworm-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /usr/local/bin/screener /usr/local/bin/screener
COPY screener.toml /app/screener.toml
# SIGINT and SIGTERM both flush the data file and stop.
STOPSIGNAL SIGINT
ENTRYPOINT ["screener"]
CMD ["collect"]
