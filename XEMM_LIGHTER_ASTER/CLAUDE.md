# XEMM Lighter/Aster Agent Notes

## Live Trading Safety

This repository runs a real-money cross-exchange bot: maker quotes on Aster, hedged on Lighter.
Treat every `livebot --mode live` run as capable of placing real orders.

Graceful stop: send `SIGINT`/Ctrl-C. The bot cancels resting orders on shutdown, but it can leave a
delta-neutral Aster/Lighter position open.

Required local secrets are `aster.env` and `lighter.env`; both are ignored by git.

## Normal Run

Build release before live use:

```bash
cargo build --release
```

Run the HYPE live config:

```bash
./target/release/xemm_lighter_aster --config config-live-lighter.toml livebot --mode live --markets HYPE \
    --db runs/live-hype.sqlite --out runs/live-hype.jsonl.zst
```

## Useful Read-Only Checks

```bash
./target/release/xemm_lighter_aster --config config-live-lighter.toml probe leverage --market HYPE
./target/release/xemm_lighter_aster --config config-live-lighter.toml probe aster-positions --market HYPE
./target/release/xemm_lighter_aster --config config-live-lighter.toml probe aster-open-orders --market HYPE
./target/release/xemm_lighter_aster --config config-live-lighter.toml probe lighter-balance --market HYPE
./target/release/xemm_lighter_aster --config config-live-lighter.toml probe lighter-open-orders --market HYPE
```

Summarize paired live hedge rounds from the bot journal:

```bash
python3 scripts/check_hedged_trade.py runs/<db-stem>-journal.jsonl --config config-live-lighter.toml
```
