# Aster/Lighter Cross-Exchange Market Making and Arbitrage

A live Aster/Lighter trading stack that coordinates two strategies: a
taker–taker arbitrage bot and an XEMM maker/taker hedging bot. The top-level
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
- `combined_pnl.py` reports combined realized PnL across taker-arb trade logs
  and XEMM hedge journals.
- `LIGHTER_ASTER_TAKER_ARB/` is the standalone taker–taker arbitrage bot. It
  checks both Aster-sell/Lighter-buy and Lighter-sell/Aster-buy directions and
  only trades when top-of-book edge clears fees and configured margin.
- `XEMM_LIGHTER_ASTER/` is the maker/taker XEMM bot. It quotes on Aster and
  hedges on Lighter, with paper, probe, record/replay, and livebot workflows.

## Repository Layout

```text
.
├── orchestrator.py
├── combined_pnl.py
├── LIGHTER_ASTER_TAKER_ARB/
│   ├── configs/live-hype.toml
│   ├── src/
│   └── README.md
└── XEMM_LIGHTER_ASTER/
    ├── config-live-lighter.toml
    ├── config-paper-lighter.toml
    ├── src/
    ├── DOCKER_DEPLOY.md
    └── LIVE_RUNBOOK.md
```

Runtime directories such as `runs/` and Rust build directories such as
`target/` are intentionally ignored by git.

## Secrets

Credentials are local files and must not be committed:

- `LIGHTER_ASTER_TAKER_ARB/aster.env`
- `LIGHTER_ASTER_TAKER_ARB/lighter.env`
- `XEMM_LIGHTER_ASTER/aster.env`
- `XEMM_LIGHTER_ASTER/lighter.env`

Keep these files mode `600` on the machine running the bots. The top-level and
bot-level `.gitignore` files ignore env files, run outputs, sqlite databases,
logs, jsonl/zst tapes, build outputs, PEM/key files, and local tool state.

The tracked `signers/` shared libraries are binary dependencies used by the
Lighter signing path. They are not credential files.

## Prerequisites

- Rust toolchain compatible with `rust-version = "1.87"` in both Rust crates.
- Python 3 for the orchestrator and reporting scripts.
- `tmux` for long-running sessions.
- `jq` is optional but useful for inspecting JSON status output.

Do not run `cargo fmt` in this stack unless that instruction is explicitly
overridden. Keep formatting changes narrow.

## Build

Build both release binaries before live use:

```bash
(cd LIGHTER_ASTER_TAKER_ARB && cargo build --release)
(cd XEMM_LIGHTER_ASTER && cargo build --release)
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
- `--max-loss-usdc N` sets the orchestrator-level realized-loss stop.
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

Combined realized PnL:

```bash
python3 combined_pnl.py --market HYPE --since 2026-06-23T16:00:00Z
python3 combined_pnl.py --market HYPE --json
```

XEMM hedge journal summary:

```bash
cd XEMM_LIGHTER_ASTER
python3 scripts/check_hedged_trade.py runs/<journal>.jsonl --config config-live-lighter.toml
```

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
