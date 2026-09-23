# syntax=docker/dockerfile:1
#
# Aster/Lighter bot: the XEMM livebot (Aster post-only maker -> Lighter IOC hedge) and the
# taker-taker engine (`lighter_aster_bot taker ...`) in one binary.
# Multi-stage: a Rust builder compiles the release binary; a slim Debian runtime
# carries ONLY the binary + the (non-secret) live configs. Secrets
# (aster.env / lighter.env), signer binaries, and the runs/ output dir are MOUNTED at runtime
# by docker-compose and are NEVER baked into an image layer.
#
# Build locally (fast box, plenty of RAM) and ship the image to the 2 GB VPS:
#   docker compose build                       # tags lighter_aster_bot:live
#   docker save lighter_aster_bot:live | gzip | ssh ... 'gunzip | docker load'
# The VPS never compiles (a Rust LTO release build would risk OOM on 1.9 GiB).

############################
# 1) Builder
############################
# Match Cargo.toml and the verified release toolchain; retain bookworm glibc.
FROM rust:1.92-bookworm AS builder

WORKDIR /app

# Only what the crate needs to compile: manifests + lockfile + sources.
# (No build.rs / workspace; TLS=rustls, SQLite=bundled, all *-sys crates build via
# the stock image's `cc` toolchain — no extra apt packages required.)
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# LIVE requires the DEFAULT features (hotpath -> live signing: k256/keccak/hex).
# Do NOT pass --no-default-features (livebot hard-bails at runtime without hotpath).
# BuildKit cache mounts make iterative rebuilds (synced bug fixes) fast; the final
# binary is copied to a stable, non-cache path so the runtime stage can pull it.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked --bin lighter_aster_bot \
 && cp /app/target/release/lighter_aster_bot /usr/local/bin/lighter_aster_bot

############################
# 2) Runtime
############################
FROM debian:bookworm-slim AS runtime

# rustls/webpki-roots bundles Mozilla roots, but install ca-certificates so every
# reqwest/TLS path has system roots (outbound HTTPS to fapi.asterdex.com /
# mainnet.zklighter.elliot.ai at startup for exchangeInfo + Lighter specs). No OpenSSL needed.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/local/bin/lighter_aster_bot /usr/local/bin/lighter_aster_bot
# Non-secret live configs baked in as a fallback; compose also bind-mounts them so the
# markets/edge can be tuned without rebuilding the image.
COPY config-live-lighter.toml /app/config-live-lighter.toml
COPY configs /app/configs

# The bot wires ONLY tokio ctrl_c (SIGINT) for graceful drain (cancel resting orders,
# finalize tape/DB). Docker's default SIGTERM is UNhandled -> SIGKILL with no cancel.
# Make SIGINT the stop signal so `docker stop` drains cleanly (fills are hedged and net
# residuals corrected; paired positions stay open).
STOPSIGNAL SIGINT

ENTRYPOINT ["lighter_aster_bot"]
# Safe default: print help and exit. Real runs pass an explicit command via
# `docker compose run --rm bot <args>` (see docker-compose.yml / DOCKER_DEPLOY.md).
CMD ["--help"]
