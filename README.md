# Aster/Lighter Cross-Exchange Market Making and Arbitrage

A live Aster/Lighter trading stack that coordinates two strategies: a
takerâ€“taker arbitrage bot and an XEMM maker/taker hedging bot. The top-level
orchestrator supervises switching, risk state, logs, and combined PnL across
both bots.

The repository is intentionally kept as one top-level git project:
`LIGHTER_ASTER_TAKER_ARB` and `XEMM_LIGHTER_ASTER` are normal subdirectories,
not nested git repositories.

This is a live trading codebase. Commands that use `run`, `livebot --mode live`,
or live market probes can place real orders and can lose money through spread,
fees, slippage, and execution failures. Use read-only probes or paper/observe
modes before any live run.

## Components

- `orchestrator.py` is the control plane. It supervises the two strategies for
  one market, switches between them based on status and margin conditions,
  writes risk/log state into `runs/`, and can keep the taker-arb bot in
  reduce-only standby while XEMM is active.
- `combined_pnl.py` reports combined execution economics across taker-arb trade logs
  and XEMM hedge journals.
- `LIGHTER_ASTER_TAKER_ARB/` is the standalone takerâ€“taker arbitrage bot. It
  checks both Aster-sell/Lighter-buy and Lighter-sell/Aster-buy directions and
  only trades when top-of-book edge clears fees and configured margin.
- `XEMM_LIGHTER_ASTER/` is the maker/taker XEMM bot. It quotes on Aster and
  hedges on Lighter, with paper, probe, record/replay, and livebot workflows.

## Repository Layout

```text
.
â”œâ”€â”€ orchestrator.py
â”œâ”€â”€ combined_pnl.py
â”œâ”€â”€ LIGHTER_ASTER_TAKER_ARB/
â”‚   â”œâ”€â”€ configs/live-hype.toml
â”‚   â”œâ”€â”€ src/
â”‚   â””â”€â”€ README.md
â””â”€â”€ XEMM_LIGHTER_ASTER/
    â”œâ”€â”€ config-live-lighter.toml
    â”œâ”€â”€ config-paper-lighter.toml
    â”œâ”€â”€ src/
    â”œâ”€â”€ DOCKER_DEPLOY.md
    â””â”€â”€ LIVE_RUNBOOK.md
```

Runtime directories such as `runs/` and Rust build directories such as
`target/` are intentionally ignored by git.

## Secrets

Credentials are local files and must not be committed:

- `LIGHTER_ASTER_TAKER_ARB/aster.env`
- `LIGHTER_ASTER_TAKER_ARB/lighter.env`
- `XEMM_LIGHTER_ASTER/aster.env`
- `XEMM_LIGHTER_ASTER/lighter.env`

Keep these files mode `600` on the machine running the bots â€” the orchestrator
refuses `--live` if any env file is readable by group/other. The top-level and
bot-level `.gitignore` files ignore env files, run outputs, sqlite databases,
logs, jsonl/zst tapes, build outputs, PEM/key files, and local tool state.

The taker-arb `aster.env` must explicitly list the API-wallet (signer) address
in `wallet_address`/`subaccount_address` and it must match `private_key`'s
derived address; startup fails otherwise (catches a rotated key against a
stale env file before anything is signed).

The tracked `signers/` shared libraries are binary dependencies used by the
Lighter signing path. They are not credential files.

## Prerequisites

- Rust 1.92 (both crate manifests and the Docker builder use this version).
- Python 3 for the orchestrator and reporting scripts.
- `tmux` for long-running sessions.
- `jq` is optional but useful for inspecting JSON status output.

Do not run `cargo fmt` in this stack unless that instruction is explicitly
overridden. Keep formatting changes narrow.

## Build

Build both release binaries before live use:

```bash
(cd LIGHTER_ASTER_TAKER_ARB && cargo build --release --locked)
(cd XEMM_LIGHTER_ASTER && cargo build --release --locked)
```

Expected binaries:

- `LIGHTER_ASTER_TAKER_ARB/target/release/lighter_aster_taker_arb`
- `XEMM_LIGHTER_ASTER/target/release/xemm_lighter_aster`

## Read-Only Checks

Taker-arb:

```bash
cd LIGHTER_ASTER_TAKER_ARB
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml fetch-specs --markets HYPE
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml probe --market HYPE
```

XEMM:

```bash
cd XEMM_LIGHTER_ASTER
./target/release/xemm_lighter_aster --config config-live-lighter.toml fetch-specs --markets HYPE
./target/release/xemm_lighter_aster --config config-live-lighter.toml probe leverage --market HYPE
./target/release/xemm_lighter_aster --config config-live-lighter.toml probe aster-positions --market HYPE
./target/release/xemm_lighter_aster --config config-live-lighter.toml probe lighter-balance --market HYPE
./target/release/xemm_lighter_aster --config config-live-lighter.toml probe lighter-open-orders --market HYPE
```

Top-level orchestrator decision cycle without starting/stopping child bots:

```bash
python3 orchestrator.py --market HYPE --once
```

## Live Orchestrator

The orchestrator is the normal top-level entry point for running the stack:

```bash
tmux new -s lighter_aster_orchestrator
python3 -u orchestrator.py --live --market HYPE --preflight-kill-existing
```

Useful options:

- `--once` runs one status/decision cycle.
- `--poll-sec N` controls the normal supervision interval.
- `--max-loss-usdc N` sets the orchestrator-level realized-loss stop
  (default 15 â€” deliberately above the bot-level `max_loss_usdc` /
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
pgrep -af 'orchestrator.py|lighter_aster_taker_arb|xemm_lighter_aster' || true
```

## Direct Bot Runs

Prefer the top-level orchestrator for normal operation. Direct bot commands are
useful for diagnostics and controlled tests.

Taker-arb observe-only:

```bash
cd LIGHTER_ASTER_TAKER_ARB
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml run --markets HYPE --observe-only
```

Taker-arb live:

```bash
cd LIGHTER_ASTER_TAKER_ARB
./target/release/lighter_aster_taker_arb --config configs/live-hype.toml run --markets HYPE
```

XEMM paper:

```bash
cd XEMM_LIGHTER_ASTER
./target/release/xemm_lighter_aster --config config-live-lighter.toml livebot --mode paper --markets HYPE --secs 30
```

XEMM live:

```bash
cd XEMM_LIGHTER_ASTER
./target/release/xemm_lighter_aster --config config-live-lighter.toml livebot --mode live --markets HYPE \
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
and Python `scripts/check_hedged_trade.py` entry points use the same fixture contract.

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
same user for host and container processes; see the [Docker runbook](XEMM_LIGHTER_ASTER/DOCKER_DEPLOY.md).

The supervisor blocks replacement after an unresolved or unclean child exit.
A lease permits only reducing taker trades; cold observations expire after 500 ms
and transmission checks the lease's actual expiry. See the [taker guide](LIGHTER_ASTER_TAKER_ARB/README.md)
and [XEMM runbook](XEMM_LIGHTER_ASTER/LIVE_RUNBOOK.md) for execution recovery.

Replay scenarios are independent per market, queue model and hedge latency. Each
owns positions, quotes, reserved exposure, consumed liquidity and fees. Reports
use venue-realized P&L plus fresh marks less fees, and show censored future hedges
and residuals. The smallest latency is the primary display; old rows are labelled
unassigned and need tape replay for corrected results.

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
  LIGHTER_ASTER_TAKER_ARB/aster.env \
  LIGHTER_ASTER_TAKER_ARB/lighter.env \
  XEMM_LIGHTER_ASTER/aster.env \
  XEMM_LIGHTER_ASTER/lighter.env \
  runs/orchestrator_state_HYPE.json
```

Scan staged text for common secret patterns:

```bash
git grep --cached -n -I -E 'BEGIN (RSA |EC |OPENSSH |DSA |)PRIVATE KEY|mnemonic|seed phrase|password\s*[:=]|secret\s*[:=]|private[_-]?key\s*[:=]|api[_-]?key\s*[:=]|access[_-]?token\s*[:=]|refresh[_-]?token\s*[:=]' || true
```

Some source files intentionally contain credential field names and deterministic
test vectors. Env files and real private values must remain untracked.
