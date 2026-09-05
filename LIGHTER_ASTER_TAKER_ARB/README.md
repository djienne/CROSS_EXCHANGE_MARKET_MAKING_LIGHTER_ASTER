# Lighter/Aster Taker Arbitrage

Single-market taker arbitrage: sell Aster/buy Lighter or sell Lighter/buy Aster.
The scanner prices configured depth, including the liquidity multiple, and requires
the spread to clear fees, margin, and the adaptive entry threshold. Regular Aster
orders are bounded IOC limits; Lighter orders use its native market/IOC path.

## Commands

Build and validate from this directory. Repository policy requires release Cargo
commands and forbids `cargo fmt`.

```bash
cargo build --release --locked
cargo test --release --locked
```

Read-only market/account checks:

```bash
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml fetch-specs --markets HYPE
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml status --market HYPE --json
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml probe --market HYPE
```

History collection and live execution:

```bash
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml run --markets HYPE --observe-only
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml run --markets HYPE
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml run --markets HYPE --min-size --max-trades 1 --secs 300
```

`--observe-only` records eligible opportunities without submitting orders. A normal
stop verifies positions are balanced within the configured mismatch tolerance and
that no orders remain; balanced inventory can remain open after a live run.

The explicit roundtrip diagnostics place live orders. They require a flat starting
position and no open orders on the tested venue:

```bash
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml aster-market-roundtrip --market HYPE --i-understand-live --max-usd 6
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml lighter-market-roundtrip --market HYPE --i-understand-live --max-usd 12
```

The Aster diagnostic opens with MARKET; it therefore does not reproduce the
regular IOC entry. Both diagnostics run reduce-only cleanup after diagnostic
errors, including failed balance/fill reads. Cleanup uses current quantities and
bounded prices, allows at most three closes within 30 seconds, and requires
terminal-order and flat-position evidence. Insufficient evidence leaves the
session blocked rather than reporting a successful roundtrip.

## Execution and hot/cold separation

Both books arrive over websockets. Published book updates wake the scanner;
`poll_interval_ms` remains its fallback. Local receipt age and source emission age
must pass `max_book_staleness_ms`; source clocks may lead by at most one second.
Matching-engine change time is diagnostic and does not make an otherwise fresh,
quiet book stale.

Book reads, cheap top-price rejection, depth sizing, and final risk checks use
memory. Account/order queries, lease file reads, nonce refresh, exact percentile
maintenance, serialization, and journal writes run on cold paths. Account
snapshots carry the execution epoch and query-start time: a pre-trade request
cannot replace the completed execution's positions. Final admission checks current
book identity/freshness, account epoch/age, clear orders, execution rights,
transport readiness, and applicable risk limits before either leg is submitted.

A `--control-file` lease enforces reducing execution even if the exposure filter
was omitted. The cold reader polls every 250 ms; cached control evidence expires
after 500 ms. Activation requires a nonempty lease ID, matching market,
`reduce_only` mode, future expiry, nonce refresh, and verified account/order state.
Reduce quantities are capped at both existing positions.

Unknown submission outcomes retain order/client IDs, native transaction identity,
and fill tracking. Missing order rows or a flat position do not establish no fill.
A known missing hedge gets one retry within `hedge_retry_timeout_ms`; configuration
requires `max_hedge_retry_attempts = 1`. Remaining exposure uses bounded reduce-only
recovery. An unresolved retry/close prevents further submissions.

## History, accounting, and recovery

`configs/live-hype.toml` is the parameter source. The entry gate records profitable,
size-valid opportunities to `runs/opportunities_<MARKET>.jsonl`. Both `shadow` and
`enforce` block during history warmup. After warmup, `shadow` reports the decision;
`enforce` requires the greater of the nearest-rank percentile and
`required_gross_edge_bps + min_extra_bps`. The candidate is evaluated before its own
sample is added. Exact rank updates and timestamp-based pruning stay cold; the
scanner requires a current published history version and expiry boundary.

Bounded cold workers serialize journal rows under per-file locks, including across
processes. Signal updates coalesce into a single atomic writer. Queue/write failure
blocks new execution; financially required rows are acknowledged before loss
breakers return. Shutdown allows five seconds to drain each writer.

Version 2 trade rows contain `economic_status`, `execution_id`, `source_event_id`,
and fee provenance. Lighter fees are computed per fill as
`notional_usd * own_role_fee_ticks / 1_000_000`, preserving rebates. Individual
`lighter_fee_evidence` records retain rate, notional, role, trade/order identity,
and available event time. Missing/null selected fees remain unknown.

Ordinary `actual_net_usd` records matched spread capture minus fees, not realized
account PnL for open inventory. Recovery rows are labeled conservative equity-delta
estimates. Legacy, incomplete, or unknown-fee gains cannot offset losses in the
execution guard. A separate session backstop compares already-fetched combined
marked equity with its once-armed baseline using the same `pnl.max_loss_usdc`;
that measurement also reflects funding and account cash movements.

An active session marker is armed durably before execution rights are granted.
Only verified, drained shutdown retires it. After an unclean exit, use the
read-only resolver when the marker contains complete scoped order identities:

```bash
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml resolve-session --market HYPE
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml reset-circuit-breaker --market HYPE
```

`resolve-session` requires an inactive owner, matching terminal orders, positions
consistent with those fills, and no open orders; it saves a resolution artifact.
A crash-before-receipt marker without sufficient identities remains blocked pending
primary venue evidence. `reset-circuit-breaker` archives the loss breaker only;
it neither resolves an uncertain session nor rewrites the ledger. An unchanged
loss window can recreate the breaker on startup. Historical journals stay intact;
repairs belong in separately identified derived artifacts.

The explicit cached-gate/rank CPU benchmark is local and makes no venue requests:

```bash
cargo test --release --locked benchmark_cached_hot_gate_and_exact_cold_rank_updates -- --ignored --nocapture
```
