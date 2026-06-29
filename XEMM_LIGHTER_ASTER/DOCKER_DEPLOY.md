# XEMM Lighter Deploy Runbook

Containerized live XEMM bot: Aster post-only maker orders with Lighter as the hedge venue.

## Safety Boundary
- Read-only probes are safe: `aster-balance`, `aster-positions`, `aster-open-orders`, `lighter-balance`, `lighter-open-orders`.
- `lighter-order-dry-run` signs IOC and native market buy/sell plans but does not submit them.
- `lighter-market` submits real Lighter market buy/sell orders. It requires `--i-understand-live --max-usd <N>` and should only be run with explicit approval.
- Livebot `--mode paper` never reaches venue order submission. Livebot `--mode live` uses real funds and must be run for one selected market.

## One-Time Deploy
Set the deploy target explicitly; no host or private-key path is stored in git:

```bash
export VPS_HOST='ubuntu@<host>'
export KEY="$HOME/.ssh/<deploy-key>.pem"
```

```bash
scripts/deploy_vps.sh source     # source + config + compose + signers -> /home/ubuntu/XEMM_LIGHTER_ASTER
scripts/deploy_vps.sh secrets    # aster.env + lighter.env (chmod 600)
scripts/deploy_vps.sh image      # build image locally, ship with docker save | docker load
```

Env values: `VPS_HOST` and `KEY` are required. `DEST` and `IMAGE` have defaults
and can be overridden.

## Local Native Run
```bash
cargo build --release

./target/release/xemm_lighter_aster --config config-live-lighter.toml fetch-specs --markets HYPE
./target/release/xemm_lighter_aster --config config-live-lighter.toml probe lighter-balance --market HYPE
./target/release/xemm_lighter_aster --config config-live-lighter.toml probe lighter-open-orders --market HYPE
./target/release/xemm_lighter_aster --config config-live-lighter.toml probe lighter-order-dry-run --market HYPE
./target/release/xemm_lighter_aster --config config-live-lighter.toml verify-books --markets HYPE --secs 8
```

## Docker Run
```bash
docker compose run --rm xemm --config config-live-lighter.toml livebot --mode paper --markets HYPE --secs 30

docker compose run --rm --name xemm-hype xemm \
  --config config-live-lighter.toml livebot --mode live --markets HYPE \
  --secs 900 \
  --db runs/live-hype.sqlite --out runs/live-hype.jsonl.zst
```

Stop gracefully with `docker kill --signal=SIGINT xemm-hype`. Shutdown cancels resting orders and leaves positions open; it does not flatten.

## Lighter Market Probe
```bash
./target/release/xemm_lighter_aster --config config-live-lighter.toml \
  probe lighter-market --market HYPE --i-understand-live --max-usd 6
```

That command sends live native market orders and can lose money through spread, fees, and slippage. Do not run it as part of routine validation.
