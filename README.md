# Aster/Lighter Cross-Exchange Market Making and Arbitrage

A live Aster/Lighter trading stack with two strategies: a taker–taker arbitrage
engine and an XEMM maker/taker hedging engine. Both are built into one Rust
binary, `lighter_aster_bot` in `LIGHTER_ASTER_BOT/`. The top-level orchestrator
switches between them for one market and tracks risk state, logs and combined
PnL.

This is a live trading codebase. `taker run` (without `--observe-only`),
`livebot --mode live` and the live market probes can place real orders and can
lose money through spread, fees, slippage and execution failures. Use read-only
probes or paper/observe modes before any live run.

## Components

- `orchestrator.py` is the control plane. It supervises the two engines for
  one market, switches between them based on status and margin conditions,
  writes risk/log state into `runs/`, and can keep the taker engine in
  reduce-only standby while XEMM is active.
- `combined_pnl.py` and `trade_history.py` report combined execution economics
  across taker trade logs and XEMM hedge journals.
- `lighter_aster_bot taker ...` is the taker–taker arbitrage engine
  ([guide](LIGHTER_ASTER_BOT/TAKER.md)). It checks both Aster-sell/Lighter-buy
  and Lighter-sell/Aster-buy directions and trades a clip only when the
  depth-weighted edge clears both taker fees plus the configured margin, both
  books hold `liquidity_multiple` times the clip within `max_levels`, and the
  edge passes the entry gate (the greater of the 90th percentile of recent
  opportunity samples and the required edge plus `min_extra_bps`).
- `lighter_aster_bot livebot ...` is the XEMM engine
  ([runbook](LIGHTER_ASTER_BOT/LIVE_RUNBOOK.md)). It quotes on Aster and hedges
  on Lighter; the same binary also has the probe, record/replay and report
  commands.

## Repository Layout

```text
.
├── orchestrator.py
├── combined_pnl.py
├── trade_history.py
├── economics.py            shared execution-economics parser for both reports
├── tests/                  Python tests + shared fixtures (tests/fixtures/)
└── LIGHTER_ASTER_BOT/
    ├── config-live-lighter.toml     XEMM config
    ├── configs/taker-live-hype.toml taker config
    ├── scripts/            check_hedged_trade.py, reset_breaker.py, deploy_vps.sh
    ├── signers/            Lighter signer shared libraries
    ├── src/                XEMM + research modules; src/taker/ is the taker engine
    ├── TAKER.md
    ├── DOCKER_DEPLOY.md
    └── LIVE_RUNBOOK.md
```

Runtime directories such as `runs/` and Rust build directories such as
`target/` are intentionally ignored by git.

## Secrets

Credentials are local files and must not be committed:

- `LIGHTER_ASTER_BOT/aster.env`
- `LIGHTER_ASTER_BOT/lighter.env`

Both engines read this one pair from their working directory
(`LIGHTER_ASTER_BOT/`). Keep the files mode `600` on the machine running the
bot — the orchestrator refuses `--live` if an env file is readable by
group/other. The `.gitignore` files ignore env files, run outputs, sqlite
databases, logs, jsonl/zst tapes, build outputs, PEM/key files, and local tool
state.

`aster.env` must explicitly list the API-wallet (signer) address in
`wallet_address`/`subaccount_address` and it must match `private_key`'s derived
address; startup fails otherwise (catches a rotated key against a stale env
file before anything is signed).

The tracked `signers/` shared libraries are binary dependencies used by the
Lighter signing path. They are not credential files.

## Prerequisites

- Rust 1.92 (the crate manifest and the Docker builder use this version).
- Python 3 for the orchestrator and reporting scripts.
- `tmux` for long-running sessions.
- `jq` is optional but useful for inspecting JSON status output.

Do not run `cargo fmt` in this stack unless that instruction is explicitly
overridden. Keep formatting changes narrow.

## Build

Build the release binary before live use:

```bash
(cd LIGHTER_ASTER_BOT && cargo build --release --locked)
```

Expected binary: `LIGHTER_ASTER_BOT/target/release/lighter_aster_bot`.

## Read-Only Checks

Run from `LIGHTER_ASTER_BOT/`. Taker engine:

```bash
./target/release/lighter_aster_bot taker --config configs/taker-live-hype.toml fetch-specs --markets HYPE
./target/release/lighter_aster_bot taker --config configs/taker-live-hype.toml probe --market HYPE
```

XEMM engine:

```bash
./target/release/lighter_aster_bot --config config-live-lighter.toml fetch-specs --markets HYPE
./target/release/lighter_aster_bot --config config-live-lighter.toml probe leverage --market HYPE
./target/release/lighter_aster_bot --config config-live-lighter.toml probe aster-positions --market HYPE
./target/release/lighter_aster_bot --config config-live-lighter.toml probe lighter-balance --market HYPE
./target/release/lighter_aster_bot --config config-live-lighter.toml probe lighter-open-orders --market HYPE
```

Top-level orchestrator decision cycle without starting/stopping child bots:

```bash
python3 orchestrator.py --market HYPE --once
```

## Live Orchestrator

The orchestrator is the normal top-level entry point for running the stack:

```bash
tmux new -s lighter_aster_orchestrator
python3 -u orchestrator.py --live --market HYPE
```

`--live` also terminates stray taker/XEMM writers before the first poll.

Useful options:

- `--once` runs one status/decision cycle.
- `--poll-sec N` controls the normal supervision interval.
- `--max-loss-usdc N` sets the orchestrator-level loss stop: it halts when
  account equity falls N below its persisted baseline or realized trade PnL in
  the `--pnl-since` window reaches −N
  (default 15 — deliberately above the bot-level `max_loss_usdc` /
  `max_cumulative_loss_usdc` of 10, so the bot breaker trips first and the
  supervisor stays a genuine backstop).
- `--pnl-since startup|now|<RFC3339>` controls the PnL window.
- `--no-taker-observer` disables reduce-only taker standby while XEMM is active.
- `--taker-arg`, `--taker-observer-arg`, and `--xemm-arg` append extra args to
  child bot commands.

Stop the tmux-run orchestrator cleanly with `Ctrl-C` inside tmux, or from
another shell:

```bash
tmux send-keys -t lighter_aster_orchestrator C-c
```

After stopping, verify no writers are still running:

```bash
pgrep -af 'orchestrator.py|lighter_aster_bot|lighter_aster_taker_arb|xemm_lighter_aster' || true
```

### Moving a deployment from the two-crate layout

Before the first start of `lighter_aster_bot` on a host that ran the old
`LIGHTER_ASTER_TAKER_ARB/` and `XEMM_LIGHTER_ASTER/` crates:

1. Stop the orchestrator and confirm with the `pgrep` above that no writer runs.
2. Move both old `runs/` directories into `LIGHTER_ASTER_BOT/runs/`. The taker's
   `active_session_*`, `circuit_breaker_*`, `trades_*` and `opportunities_*`
   files carry unresolved-session markers, loss breakers, the ledger and the
   entry-gate history; a taker started without them would skip those checks.
3. Confirm both old env pairs derive the same Aster signer/user and Lighter
   account/API key, then keep one pair in `LIGHTER_ASTER_BOT/` (mode `600`).
4. Build the binary and start the orchestrator as above.

## Direct Bot Runs

Prefer the top-level orchestrator for normal operation. Direct bot commands are
useful for diagnostics and controlled tests.

Run from `LIGHTER_ASTER_BOT/`. Taker observe-only:

```bash
./target/release/lighter_aster_bot taker --config configs/taker-live-hype.toml run --markets HYPE --observe-only
```

Taker live:

```bash
./target/release/lighter_aster_bot taker --config configs/taker-live-hype.toml run --markets HYPE
```

XEMM paper:

```bash
./target/release/lighter_aster_bot --config config-live-lighter.toml livebot --mode paper --markets HYPE --secs 30
```

XEMM live:

```bash
./target/release/lighter_aster_bot --config config-live-lighter.toml livebot --mode live --markets HYPE \
  --db runs/live-hype.sqlite --out runs/live-hype.jsonl.zst
```

Live roundtrip and market probes require explicit `--i-understand-live` flags in
the bot CLIs. Treat those as real order-submitting commands, not routine health
checks.

## Monitoring and PnL

Runtime logs, journals, state files, and bot ledgers are written under `runs/`
directories and are ignored by git.

Combined execution economics (funding and account marks are separate):

```bash
python3 combined_pnl.py --market HYPE --since 2026-06-23T16:00:00Z
python3 combined_pnl.py --market HYPE --json
```

Canonical local trade-history DB in LAN mode:

```bash
python3 trade_history.py --mode lan --market HYPE
python3 trade_history.py --mode lan --market HYPE --json
```

LAN mode reads local artifacts and preserves actual fee evidence. Version 2 records
carry logical/attempt identities, executed quantities, matched quantity, residual
exposure and fee provenance. Corrections revise the original logical trade without
adding trades or volume. Lighter fees use each own-account fill's selected fee ticks
and notional; rebates remain signed. Legacy aggregates without sufficient evidence
remain incomplete. Unknown economics are `null` in JSON and SQL; reports show a
known subtotal and incomplete count, and suppress complete totals and projections.

Execution economics combine venue-realized closes and spread on matched opposite
remaining positions, less actual fees. They exclude funding and the marked value
of unpaired exposure; use account equity and residual positions to assess those.
Trade evidence is paired before filtering by economic time. The Rust `live-report`
and Python `LIGHTER_ASTER_BOT/scripts/check_hedged_trade.py` entry points use the same
fixture contract (`tests/fixtures/execution_economics.json`).

Historical repair writes a separate candidate and a before/after JSON comparison:

```bash
python3 trade_history.py --market HYPE --db runs/trade_history.sqlite --rebuild \
  --xemm-journal runs/<journal>.jsonl --raw-fills runs/<own-account-fills>.jsonl
# After inspecting the comparison, apply that unchanged candidate with a backup:
python3 trade_history.py --market HYPE --db runs/trade_history.sqlite --replace-rebuilt
```

`--raw-fills` is optional and repeatable; it accepts individual own-account fills
with order/trade identities, quantities, notional and fee evidence. Repair never
multiplies aggregate legacy fees by aggregate notional. Rebuilding is idempotent,
keeps journals unchanged, and refuses replacement if either database changed since
review. No production history is included in this checkout.

All signer processes must share `ASTER_NONCE_DIR` (or supervisor
`--aster-nonce-dir`). The default is the OS temporary directory's
`lighter-aster-nonces` child. Each signer has a memory-mapped atomic counter;
initialization uses an OS lock. Keep that directory persistent and writable by the
same user for host and container processes; see the [Docker runbook](LIGHTER_ASTER_BOT/DOCKER_DEPLOY.md).

The supervisor blocks replacement after an unresolved or unclean child exit.
A lease permits only reducing taker trades; cold observations expire after 500 ms
and transmission checks the lease's actual expiry. See the [taker guide](LIGHTER_ASTER_BOT/TAKER.md)
and [XEMM runbook](LIGHTER_ASTER_BOT/LIVE_RUNBOOK.md) for execution recovery.

Replay scenarios are independent per market, queue model and hedge latency. Each
owns positions, quotes, reserved exposure, consumed liquidity and fees. Reports
use venue-realized P&L plus fresh marks less fees, and show censored future hedges
and residuals. The smallest latency is the primary display; old rows are labelled
unassigned and need tape replay for corrected results.

## Evidence Limits

No production history or representative long market tape exists here; historical
repair is validated with explicit fixtures. The short public tape establishes
transport/paper/replay operation, not trading edge. Execution economics exclude
funding and unpaired account marks; account-equity changes also include transfers.
Unresolved sessions lacking terminal venue identity remain blocked until primary
evidence resolves them. The tests do not certify deployed latency, actual
profitability, live venue acceptance or signer-binary supply-chain integrity.

Protocol evidence: Aster signing follows the
[documented EIP-712 contract](https://asterdex.github.io/aster-api-website/asterCode/authentication/).
Lighter fee units, and "an omitted fee means zero", come from the
[trade circuit](https://github.com/elliottech/lighter-prover/blob/main/circuit/src/apply_trade.rs),
[fee constants](https://github.com/elliottech/lighter-prover/blob/main/circuit/src/types/constants.rs)
and the [WebSocket reference](https://apidocs.lighter.xyz/docs/websocket-reference).
Offline protocol vectors do not establish private venue acceptance.

## Git Hygiene

Before committing, check that only intended source/config/docs are staged:

```bash
git status --short
git diff --cached --name-only
git diff --cached --check
```

Check for private/runtime paths in the index:

```bash
git ls-files | rg '(^|/)(aster|lighter)\.env$|(^|/)runs/|(^|/)target/|\.sqlite($|-)|\.db$|\.log$|\.pid$|\.logpath$|\.jsonl($|\.)|\.zst$|\.pem$|\.env$|\.key$' || true
```

Check that local credential/runtime files are ignored:

```bash
git check-ignore -v \
  LIGHTER_ASTER_BOT/aster.env \
  LIGHTER_ASTER_BOT/lighter.env \
  runs/orchestrator_state_HYPE.json
```

Scan staged text for common secret patterns:

```bash
git grep --cached -n -I -E 'BEGIN (RSA |EC |OPENSSH |DSA |)PRIVATE KEY|mnemonic|seed phrase|password\s*[:=]|secret\s*[:=]|private[_-]?key\s*[:=]|api[_-]?key\s*[:=]|access[_-]?token\s*[:=]|refresh[_-]?token\s*[:=]' || true
```

Some source files intentionally contain credential field names and deterministic
test vectors. Env files and real private values must remain untracked.
