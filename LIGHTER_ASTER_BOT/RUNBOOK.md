# Operating the bot

`lighter_aster_bot run` trades one market with both engines in one process. The taker–taker
engine and the reduce-only XEMM engine take turns holding execution rights; the controller
switches them in memory. `--mode live` trades real money; `--mode dry-run` runs the same bot
against simulated venues fed by live market data ([Dry run](#dry-run)). Commands run from
this directory (`LIGHTER_ASTER_BOT/`) and read `bot.toml`. `run --mode live`, `taker run`
without `--observe-only` and the `*-market`/`*-roundtrip` probes submit real orders.

## How `run` switches

- **Taker (normal)** holds the rights unless it is blocked: margin-limited with no
  executable reducing trade. Margin-limited means under 2 clips of position headroom, under
  2 clips of free margin (after the buffer) on the tighter venue, or a profitable
  opportunity held back by headroom or margin.
- **XEMM** takes over once the taker has been blocked for 90 s. It only posts the Aster maker
  side whose Lighter hedge reduces inventory (both sides when flat), so `run` refuses a
  config with `[maker.live.quote] reduce_position_only = false`.
- **Back to the taker** once it has been ready for 45 s: near flat (one clip), a reducing
  trade executable, or no longer margin-limited at 3 clips (the 2-vs-3 band is the
  hysteresis).
- **Reduce taker.** While XEMM is active, a reduce-only taker observes. A burst of 3 reducing
  opportunities within 2 s moves the rights to it under a 180 s lease. Each burst is used
  once and must be under 60 s old. Newer bursts and reducing trades extend the lease, never
  past 300 s from the grant. Near flat returns to the normal taker; an expired lease returns
  to XEMM.

Rights move only after the previous holder has fully stopped; when XEMM hands over, its
status must also show no open order on either venue. An engine that fails its drain or does
not stop within 185 s halts the bot instead of switching. The `[controller]` table of
`bot.toml` holds these thresholds and the cross-engine loss stop.

## Build, secrets, configuration

```bash
cargo build --release --locked        # Rust 1.92; never `cargo fmt`
```

- `aster.env` and `lighter.env` sit in the working directory (or at `ASTER_ENV_PATH` /
  `LIGHTER_ENV_PATH`), mode `600`; `run --mode live` refuses them if group/other can read
  them. `aster.env` must list the signer address in `wallet_address`/`subaccount_address`,
  and that address must match the private key.
- Every Aster signer process shares `ASTER_NONCE_DIR`, a memory-mapped counter per signer that
  survives restarts. The default is the OS temp dir's `lighter-aster-nonces`. Keep it
  persistent, writable by the bot user, and never delete it while any signer runs.
- `bot.toml` has `[controller]`, `[taker]`, `[maker]` and `[dry_run]`. Unknown or misplaced
  keys fail the load, and so do the retired XEMM switches (`live.partials.max_pending_count`,
  `live.quote.price_change_ticks_to_requote`, `expires_after_ms`, …) and non-mainnet
  `[maker.live]` URLs.
  The mode is a command-line choice only. `[taker.live] enabled = true, mode = "live"` arms
  the taker engine.

## Run and stop

Native, in tmux:

```bash
tmux new -s lighter_aster_bot
./target/release/lighter_aster_bot run --market HYPE --mode live
```

Docker (see [Deploy](#deploy)):

```bash
docker compose run --rm --name bot-hype bot run --market HYPE --mode live
```

Stop with Ctrl-C, SIGINT, SIGTERM or SIGHUP (`tmux send-keys -t lighter_aster_bot C-c`,
`docker kill --signal=SIGINT bot-hype`). The active engine drains first, then the observer.
XEMM quiesces admission, cancels makers, drains fills and execution outcomes, corrects net
residuals, reconciles and flushes persistence; this can take up to 175 s. Never stop
it with a shorter kill: `docker stop` needs `-t 200`, and compose already sets
`stop_grace_period: 200s`. Paired positions stay open and delta-neutral. Exit 0 means a
clean stop; nonzero means a halt, an unresolved engine stop or an unwritable event log.

Only one live writer per market runs at a time: `run --mode live` and `taker run` (unless
`--observe-only`) take the exclusive lock `runs/bot-<MARKET>.lock` and name the holder's pid
on contention. A dry run locks `runs/dry-run/bot-<MARKET>.lock`, so it can run beside live.

## Dry run

`run --mode dry-run` is the whole bot, both engines, the controller and every client and
signer unchanged, against an in-process simulated Aster and Lighter on loopback. The
simulator follows the live public market data and answers in each venue's own protocol. It
needs no credentials: the bot signs with a fixed dry-run identity whose keys exist on no
venue, so a request that escaped to mainnet could not trade. Its files live in
`runs/dry-run/`, which live never touches.

The venues respond as seen from AWS Tokyo, and pessimistically where the data cannot decide
(`[dry_run]` in `bot.toml` cites each value's source):

- **Time shift.** The simulated world is the live one `shift_ms` (1000) late, timestamps
  included, so the bot sees books as fresh as a Tokyo host would, by its own clocks. A frame
  that reaches this host later than that is applied on arrival and counted as late.
- **Latency.** Each request draws a lognormal round trip from the benchmarked `[p50, p99]`
  and takes effect at `effect_fraction` (0.9) of it. Lighter taker orders wait a further
  `lighter_taker_delay_ms` (300, the Standard account's delay).
- **Takers** fill against the worse of the two book states around their effect time, and
  liquidity the bot took stays gone until the feed shows that level smaller.
- **Makers** wait behind the visible size at their price times `1 + hidden_queue_multiplier`.
  Prints at their price work through that queue before filling them; a print through their
  price, or a book that crosses them, fills them outright.
- **Venue rules**: the live filters, reduce-only, the Aster deadman and listen-key expiry,
  Lighter's sequential nonces, and both venues' rate limits (Lighter Standard: 60 REST
  requests and 60 transactions a minute).
- **Accounts**: one cross account per venue from the `[dry_run]` balances. Fees come from the
  bot's own fee keys, so the dry run cannot catch a wrong one. Funding follows the public
  rates at each venue's funding times. A maintenance-margin breach is reported, not
  liquidated.

Docker (from this directory; the fleet's `start_all.bat` also starts it):

```bash
docker compose up -d --build dryrun
docker compose logs -f dryrun
```

Natively: `./target/release/lighter_aster_bot run --market HYPE --mode dry-run`.

It stops like live (SIGINT, a drain, positions stay open) and saves the simulated venues;
the next start takes up their accounts, positions and resting orders. The market moved
meanwhile, so the first book fills any resting order it crosses, an expired Aster deadman
cancels, and funding that fell due is charged. To start afresh, stop it and move
`runs/dry-run/` away. Moving only `sim-<M>.state.json` would leave the drawdown baseline
measuring the reset accounts. A fresh directory also restarts the taker's entry-gate
history: as after a fresh live start, the taker trades only once it has seen 500
opportunities above its required edge (`[taker.arb.entry_gate]`).

An unclean stop (a host reboot, `docker kill`) loses at most the venues' last second. The
next start archives the engines' unclean-session markers, whether a kill or an unresolved
engine stop left them, as `<name>.unclean.<stamp>`. Live keeps them until an operator has
resolved the session against the venues' records; here the simulated venues' own state is
the only record, and the engines reconcile to it at start. Docker treats `docker kill` as a
deliberate stop and does not restart the container; `docker compose up -d dryrun` does.

**Halts.** A halted dry run parks: the process and the simulated venues keep running, the
diagnostics too, until it is stopped, so the restart policy never resumes it unreviewed. The
service passes `--ack-breaker`, so `docker compose restart dryrun` is the review. An
equity-drawdown halt parks again, asking for `--reset-breaker-baseline`:

```bash
docker compose stop dryrun
docker compose run --rm dryrun run --market HYPE --mode dry-run --ack-breaker --reset-breaker-baseline
# Ctrl-C once it has started, then resume in the background:
docker compose up -d dryrun
```

The engines' own loss latches ([Halts and recovery](#halts-and-recovery)) have dry-run
resets: `python scripts/reset_breaker.py --runs-dir runs/dry-run --coin HYPE` for XEMM, and
`docker compose run --rm dryrun taker reset-circuit-breaker --market HYPE --dry-run` for the
taker. Without `--dry-run` that command resets live's breaker.

**Diagnostics.** Every minute the simulator logs a one-line gist and appends a row to
`runs/dry-run/sim-<M>.diag.jsonl`, per venue (quantiles are `{n, p50, p90, p99, max}`):

| Field | Read it as |
|---|---|
| `late_frames` of `frames` | Frames later than the shift. Keep them under 1 %, or raise `shift_ms`. |
| `lag_ms` (`book`, `top`, `trade`) | Arrival minus exchange time, clock skew included. The p99 must stay under `shift_ms` − 250 (the lookahead that finds the later book state). |
| `stale_frames`, `gaps` | Out-of-order book frames, and upstream breaks. A gap closes the bot's streams, as the venue would, and orders are rejected `Unavailable` until the next snapshot. |
| `lateness_ms` (whole row) | How late the simulator ran its events. Tens of ms mean the container is short of CPU. |
| `rtt_ms`, `private_ms` | The latencies drawn. |
| `requests`, `orders`, `rejects` | Rejects by reason. Each needs an explanation in the bot's log; `RateLimited` means the bot outran a venue limit, which is a finding about the bot. |
| `maker_fills`, `taker_fills`, `queue_ahead`, `maker_wait_ms` | Fills, the queue ahead of each order that came to rest, and each maker fill's wait since placement. |
| `prints`, `prints_inside_spread`, `prints_over_visible` | Trades the visible book cannot explain: hidden orders, or orders placed and taken between two book updates. Their share bounds from above the hidden liquidity `hidden_queue_multiplier` assumes. |
| `account` | Balance, unrealized, equity, realized, fees, funding, positions, maintenance breach. |

Simulator warnings start with `dry-run`. `no route`, `no websocket` or `not simulated` means
the bot used something the simulator does not serve, so the dry run no longer matches live:
treat it as a bug. The reports take `--dry-run` (`python3 combined_pnl.py --dry-run`;
`python3 trade_history.py --dry-run` keeps its own database in `runs/dry-run/`).

What the dry run cannot tell: whether the fee keys are right; the bot's market impact beyond
the liquidity it takes; how Aster's ~100 ms splits around matching, and how Lighter treats an
IOC it cannot fill (both assumed pessimistically until live acks calibrate them); anything
about liquidation. Lighter signatures are not verified.

**Going live.**

1. The dry run has run for days with no unexplained reject, halt or `no route` warning, and
   its reports agree with the simulated equity net of funding and open-position marks.
2. `aster.env` and `lighter.env` are in place ([Build, secrets, configuration](#build-secrets-configuration)).
3. Build the live image: `docker compose build bot` (the dry run's image is separate).
4. The read-only probes pass: `docker compose run --rm bot probe aster-balance`, then
   `probe lighter-balance`, `probe lighter-open-orders` and `taker probe --market HYPE`.
5. The fee keys in `bot.toml` match both accounts' actual tiers.
6. Neither venue has open orders, and positions are flat or paired.
7. `docker compose run --rm --name bot-hype bot run --market HYPE --mode live`. Its files are
   in `runs/`, and its drawdown baseline starts at the first sample.

## Runtime files (`runs/`)

| File | What |
|---|---|
| `bot-<M>.events.jsonl` | Controller events: switches, leases, loss samples, halts |
| `bot-<M>.state.json` | Current regime, lease, accounts; read by `combined_pnl.py` / `trade_history.py` |
| `bot-<M>.breaker.json` | Controller halt latch |
| `bot-<M>.baseline.json`, `bot-<M>.equity.jsonl` | Equity-drawdown baseline and samples |
| `bot-<M>-journal.jsonl` | XEMM execution journal |
| `bot-<M>.trip.json`, `bot-<M>.active.json` | XEMM loss latch and unclean-session marker |
| `trades_<M>.jsonl`, `opportunities_<M>.jsonl` | Taker ledger and entry-gate history |
| `active_session_<M>.json`, `circuit_breaker_<M>.json` | Taker unclean-session marker and loss breaker |

The taker's observe-only history still feeds `opportunities_<M>.jsonl`, as live history
collection does. A dry run writes the same files in `runs/dry-run/`, plus the simulated
venues' `sim-<M>.state.json` and `sim-<M>.diag.jsonl`.

## Halts and recovery

**Controller halt.** The bot stops both engines (writer first), writes
`bot-<M>.breaker.json` with the reason, and exits nonzero. The reasons are:

- the cross-engine loss stop (`pnl_breaker`): marked equity at or below the persisted
  baseline minus `max_loss_usdc` (15), or realized trade PnL of both engines at or below −15.
  Unverified gains never count; unverified losses do.
- a failed or hung engine stop (`*_shutdown_unresolved`);
- resting orders when rights should move (`*_orders_not_clear*`, `startup_orders_not_clear`);
- an engine error (`active_bot_exited_nonzero`), or three clean exits each under 10 minutes
  of uptime (`active_bot_crash_loop`);
- three unreadable required statuses in a row (`status_unavailable`);
- XEMM not reduce-only (`xemm_reduce_position_only_disabled`).

Review `bot-<M>.events.jsonl` and the breaker, then restart with `--ack-breaker`, which
archives it as `.acked.<stamp>`. An equity-drawdown breaker also needs
`--reset-breaker-baseline`, which re-arms the drawdown stop on the next sample. The
engines' own latches below are separate and each blocks its engine on its own.

**XEMM.** `bot-<M>.active.json` remains after an unclean or unresolved session. Keep it
until cold venue records resolve every attempted order in the journal and orders and
positions are reconciled; an empty orders snapshot alone is not enough. The loss latch
`bot-<M>.trip.json` trips when marked equity falls below the median of the first 5 fresh
samples by `max_cumulative_loss_usdc` (10) on 3 consecutive samples. Clear it after review
with `python scripts/reset_breaker.py --coin <M>`; that does not clear an unresolved session.

**Taker.** The session marker is armed before execution rights are granted, and only a
verified, drained shutdown retires it. After an unclean exit, when the marker holds complete
scoped order identities:

```bash
./target/release/lighter_aster_bot taker resolve-session --market HYPE
./target/release/lighter_aster_bot taker reset-circuit-breaker --market HYPE
```

`resolve-session` needs an inactive owner, matching terminal orders, positions consistent
with those fills, and no open orders; it saves a resolution artifact. A marker from a crash
before any receipt, without enough identities, stays blocked pending primary venue evidence.
`reset-circuit-breaker` archives the loss breaker only. It neither resolves a session nor
rewrites the ledger, and an unchanged loss window can recreate the breaker at startup.

## Engines

**Taker.** It prices configured depth in both directions (Aster sell/Lighter buy and the
reverse). A clip trades only when the depth-weighted edge clears both taker fees plus the
margin, both books hold `liquidity_multiple` times the clip within `max_levels`, and the
edge passes the entry gate. The gate uses the greater of the 90th percentile of recent
samples and the required edge plus `min_extra_bps`, and it blocks during history warmup.
Aster orders are bounded IOC limits; Lighter uses its native market/IOC path.

Book updates wake the scanner. Receipt and source age must pass `max_book_staleness_ms`.
Account and order queries, lease validation, nonce refresh, percentile maintenance and
journal writes run on cold paths. Final admission rechecks book identity and freshness,
account epoch and age, clear orders, execution rights, transport readiness and risk limits
before either leg is submitted.

Under a lease, execution is reduce-only whatever the exposure filter, and quantities are
capped at both existing positions. A new lease id refreshes the Lighter nonce and requires
a fresh verified account/order snapshot first.

Unknown submission outcomes keep order and client ids, native transaction identity and fill
tracking; a missing order row or a flat position does not prove no fill. A known missing
hedge gets one retry within `hedge_retry_timeout_ms`. Recovery then closes only the
same-sign net residual, and an unresolved retry or close stops further submissions.

**XEMM.** Maker transmission rechecks the admission ticket, quote deadline and book versions
after rate limiting; missing or stale source time blocks new exposure. Logical obligations
keep separate transmission attempts. Cancellation can revoke an unclaimed attempt; claimed
or ambiguous attempts stay reserved until matching terminal evidence arrives. A timeout, a
balanced position snapshot or an empty open-order list cannot prove nonexecution. After 60 s
without resolution, new exposure stays frozen while late evidence is still accepted.
Corrections use the smallest rounded-down quantity that removes the residual, at most two
attempts per incident. The margin guard reserves directional margin for resting makers and
hedge obligations above the per-venue buffers ($26 shipped); reductions stay possible.

**Accounting.** Version 2 taker rows carry `economic_status`, `execution_id`,
`source_event_id` and fee provenance. Lighter fees are per fill,
`notional_usd * own_role_fee_ticks / 1_000_000`, so rebates keep their sign. Lighter omits
zero fees, so an omitted fee is zero; an explicit null or malformed fee, or an IOC fill
flagged as maker, stays unknown. `actual_net_usd` is matched spread capture minus fees, not
account PnL of open inventory, and recovery rows are conservative equity-delta estimates.
Legacy, incomplete or unknown-fee gains never offset losses in the taker's or the
controller's realized-loss stop.

## Deploy

The VPS does not compile. Build the image locally and ship it:

```bash
export VPS_HOST='ubuntu@<host>' KEY="$HOME/.ssh/<deploy-key>.pem"
scripts/deploy_vps.sh source     # bot.toml, compose, sources, signers -> ~/LIGHTER_ASTER_BOT
scripts/deploy_vps.sh secrets    # aster.env + lighter.env, chmod 600 (once)
scripts/deploy_vps.sh image      # docker build here, docker save | ssh docker load
```

On the VPS, match the container user to the host user, and create the output and nonce dirs:

```bash
export XEMM_UID="$(id -u)" XEMM_GID="$(id -g)" ASTER_NONCE_DIR=/tmp/lighter-aster-nonces
mkdir -p runs "$ASTER_NONCE_DIR" && chmod 700 "$ASTER_NONCE_DIR"
docker compose run --rm bot probe aster-balance        # signed reads, no orders
docker compose run --rm bot taker probe --market HYPE
```

Compose mounts this directory read-only (config, signers, env files), `runs/` read-write and
the nonce dir at `/nonce`. It never restarts the live bot: a halt stays halted until reviewed.

## Probes

- Read-only: `probe aster-balance | aster-positions | aster-open-orders | leverage |
  lighter-balance | lighter-open-orders`, `taker probe`, `taker status --json`,
  `fetch-specs`.
- `probe lighter-order-dry-run` signs IOC and native market plans without submitting them.
- These submit real orders; run them only with explicit approval: `probe lighter-market
  --i-understand-live --max-usd 12` and `taker aster-market-roundtrip` /
  `taker lighter-market-roundtrip --i-understand-live --max-usd <N>`. The roundtrips need a
  flat start and no open orders. They clean up reduce-only (at most three closes in 30 s)
  and stay blocked without terminal-order and flat-position evidence.

## Moving from the orchestrator

The stack used to run `orchestrator.py` with the engines as child processes. Before the
first `run --mode live` on such a host:

1. Stop the orchestrator (Ctrl-C in its tmux), then confirm that no writer is left:
   `pgrep -af 'orchestrator.py|lighter_aster_bot|lighter_aster_taker_arb|xemm_lighter_aster'`.
   `run` refuses to start while the orchestrator still holds `runs/orchestrator_<M>.lock`.
   Children orphaned by a killed orchestrator hold no lock, so only `pgrep` finds them.
2. Review, resolve and then archive its latches in the stack root's `runs/`:
   `orchestrator_breaker_<M>.json`, and XEMM's `orchestrator-xemm-<M>.trip.json` and
   `orchestrator-xemm-<M>.active.json`. `run` refuses to start while any of them exists; an
   `.active.json` is an unresolved session (see [Halts and recovery](#halts-and-recovery)).
   Check `runs/` for other `*.trip.json` / `*.active.json` files left by direct runs of the
   retired `livebot` command.
3. The taker's files keep their names. The drawdown baseline starts fresh at the first
   sample. To carry the old one over, copy `runs/orchestrator_baseline_<M>.json` to
   `LIGHTER_ASTER_BOT/runs/bot-<M>.baseline.json`; it is discarded if unrefreshed for 48 h.
4. Start `run --mode live` and watch the first switches in `bot-<M>.events.jsonl`. The
   reports read both the old and the new journal and state files.
