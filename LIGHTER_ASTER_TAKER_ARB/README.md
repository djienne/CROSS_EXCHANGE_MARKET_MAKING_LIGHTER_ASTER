# Lighter/Aster Taker Arbitrage

Standalone live-only taker-taker arbitrage bot for Aster and Lighter.

The bot checks both directions:

- sell Aster / buy Lighter
- sell Lighter / buy Aster

It only trades when the top-of-book spread clears the configured Aster taker fee,
Lighter taker fee, and margin.

Live scans use Aster REST for the Aster book and Lighter websockets for the
Lighter book, positions, available balance, and open-order guard. Configure
`lighter_market_index`, `lighter_size_decimals`, `lighter_price_decimals`, and
`lighter_min_notional` under each market to avoid fetching Lighter public REST
metadata at startup.

Build release before any live run:

```bash
cargo build --release
```

Read-only checks:

```bash
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml fetch-specs --markets HYPE
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml probe --market HYPE
```

Aster live MARKET roundtrip test:

```bash
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml aster-market-roundtrip --market HYPE --i-understand-live --max-usd 6
```

This uses the same `AsterRest::submit_market_order` path as the bot, requires a
flat Aster starting position, buys up to `--max-usd`, checks position/balance,
then sells the resulting position reduce-only and verifies final position is flat.

Lighter live MARKET roundtrip test:

```bash
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml lighter-market-roundtrip --market HYPE --i-understand-live --max-usd 12
```

This uses the same native Rust `LighterVenue::submit_market_order` path as the
bot, including the bundled signer shared library. It requires a flat Lighter
starting position and no open Lighter orders, buys the minimum order size allowed
by `--max-usd`, then sells the resulting position reduce-only and verifies final
position is flat.

Live run:

```bash
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml run --markets HYPE
```

Observe-only history collection:

```bash
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml run --markets HYPE --observe-only
```

Observe-only mode connects to the same live market/account feeds and records
opportunity history, but skips order submission unconditionally.

The live config starts with `startup_warmup_ms = 15000`, so after connecting and
checking account state the bot waits 15 seconds before the first scan can place
an order. `cooldown_ms` then controls the grace period between completed trades.

Adaptive entry gate:

```toml
[arb.entry_gate]
enabled = true
mode = "shadow"
history_window_hours = 72
sample_interval_ms = 1000
min_history_samples = 500
entry_percentile = "90"
min_extra_bps = "0.5"
```

The gate records profitable, size-valid opportunities to
`runs/opportunities_<MARKET>.jsonl`. While fewer than `min_history_samples`
recent rows are available, live entries are blocked in both `shadow` and
`enforce` mode. After warmup, `shadow` logs whether the current opportunity
would pass the recent percentile threshold but does not block orders. Switch
`mode` to `"enforce"` to require the current gross edge to be at least the
greater of the recent percentile and `required_gross_edge_bps + min_extra_bps`.

One-trade diagnostic run:

```bash
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml run --markets HYPE --min-size --max-trades 1 --secs 300
```

The diagnostic run uses the larger minimum size across Aster and Lighter. Before
submitting it logs selected direction, top-of-book prices, expected gross/fee/net
edge, and margin room. After both legs are accepted it logs actual fill VWAPs,
filled notionals, fees, gross/net USD, net bps, post-trade positions, and
available margin before/after. Per-scan detail is DEBUG-gated, and fill
accounting runs only after accepted trades, so normal INFO logging does not add
formatting or fill-query work to the scan hot path.

Persistent PnL and circuit breaker:

```toml
[pnl]
enabled = true
persist_dir = "runs"
since = "2026-06-23T23:00:00Z"
max_loss_usdc = "5"
```

Successful fill-accounted trades are appended to
`runs/trades_<MARKET>.jsonl`. On startup the bot sums `actual_net_usd` from rows
at or after `pnl.since`; if cumulative PnL is at or below `-max_loss_usdc`, it
writes `runs/circuit_breaker_<MARKET>.json` and stops trading until manually
reset.

Manual breaker reset:

```bash
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml reset-circuit-breaker --market HYPE
```

The reset command archives the active breaker file. It does not rewrite the
ledger; if the configured `pnl.since` window is still beyond the loss limit, the
next startup will recreate the breaker and refuse to trade.
