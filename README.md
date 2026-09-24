# Aster/Lighter Cross-Exchange Market Making and Arbitrage

A live Aster/Lighter trading bot with two engines: a taker–taker arbitrage engine and
an XEMM maker/taker hedging engine. Both are built into one Rust binary,
`lighter_aster_bot` in `LIGHTER_ASTER_BOT/`. Its `run` command trades one market with
both engines in one process: it switches execution rights between them in memory and
enforces a cross-engine loss stop. `run --mode dry-run` runs the same bot against
simulated Aster and Lighter venues fed by live market data, with no credentials and no
real orders.

This is a live trading codebase. `run --mode live`, `taker run` (without
`--observe-only`) and the live market probes can place real orders and can lose money
through spread, fees, slippage and execution failures. Use the dry run, read-only probes
or the taker's observe-only mode before any live run.

## Components

- `lighter_aster_bot run` is the bot. The taker holds execution rights while it has
  margin. When it is margin-blocked, a reduce-only XEMM unwinds inventory, and a
  reduce-only taker takes over briefly under a lease whenever it sees a burst of
  reducing trades. The [runbook](LIGHTER_ASTER_BOT/RUNBOOK.md) covers the switching
  rules, halts and recovery.
- The taker engine (`lighter_aster_bot taker ...` on its own) checks both
  Aster-sell/Lighter-buy and Lighter-sell/Aster-buy directions. It trades a clip only
  when the depth-weighted edge clears both taker fees plus the configured margin, both
  books hold `liquidity_multiple` times the clip within `max_levels`, and the edge passes
  the entry gate: the greater of the 90th percentile of recent opportunity samples and
  the required edge plus `min_extra_bps`.
- The XEMM engine quotes on Aster and hedges on Lighter; it runs inside `run`. The same
  binary also has the probe, status and `live-report` commands.
- `combined_pnl.py` and `trade_history.py` report combined execution economics across
  taker trade logs and XEMM hedge journals.

## Repository Layout

```text
.
├── combined_pnl.py
├── trade_history.py
├── economics.py            shared execution-economics parser for both reports
├── tests/                  Python tests + shared fixtures (tests/fixtures/)
└── LIGHTER_ASTER_BOT/
    ├── bot.toml            config: [controller], [taker], [maker], [dry_run]
    ├── scripts/            check_hedged_trade.py, reset_breaker.py, deploy_vps.sh
    ├── signers/            Lighter signer shared libraries
    ├── src/                controller/ (run), taker/ (taker engine), livebot/ (XEMM),
    │                       dryrun/ (the simulated venues)
    └── RUNBOOK.md          operation, deploy, halts and recovery
```

Runtime directories such as `runs/` and Rust build directories such as
`target/` are intentionally ignored by git.

## Secrets

Credentials are local files and must not be committed:

- `LIGHTER_ASTER_BOT/aster.env`
- `LIGHTER_ASTER_BOT/lighter.env`

Both engines read this one pair from their working directory
(`LIGHTER_ASTER_BOT/`). Keep the files mode `600` on the machine running the
bot — `run --mode live` refuses an env file readable by group/other. The
`.gitignore` files ignore env files, run outputs, sqlite databases, logs,
jsonl/zst files, build outputs, PEM/key files, and local tool state.

`aster.env` must explicitly list the API-wallet (signer) address in
`wallet_address`/`subaccount_address` and it must match `private_key`'s derived
address; startup fails otherwise (catches a rotated key against a stale env
file before anything is signed).

The tracked `signers/` shared libraries are binary dependencies used by the
Lighter signing path. They are not credential files.

## Prerequisites

- Rust 1.92 (the crate manifest and the Docker builder use this version).
- Python 3 for the reporting scripts.
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

Run from `LIGHTER_ASTER_BOT/`; every command reads `bot.toml` by default. Taker engine:

```bash
./target/release/lighter_aster_bot taker fetch-specs --markets HYPE
./target/release/lighter_aster_bot taker probe --market HYPE
./target/release/lighter_aster_bot taker status --market HYPE --json
```

XEMM engine:

```bash
./target/release/lighter_aster_bot fetch-specs --markets HYPE
./target/release/lighter_aster_bot probe leverage --market HYPE
./target/release/lighter_aster_bot probe aster-positions --market HYPE
./target/release/lighter_aster_bot probe lighter-balance --market HYPE
./target/release/lighter_aster_bot probe lighter-open-orders --market HYPE
```

## Dry Run

The whole bot against simulated venues that follow the live public market data as seen
from AWS Tokyo, pessimistic where the data cannot decide (queue position, taker fills). It
needs no credentials and places no real order. It runs in Docker in the background, and
the fleet's `start_all.bat` starts it too:

```bash
cd LIGHTER_ASTER_BOT
docker compose up -d --build dryrun
docker compose logs -f dryrun
```

Natively: `./target/release/lighter_aster_bot run --market HYPE --mode dry-run`. Its files,
including the simulated venues' state and a diagnostics row per minute, are in
`LIGHTER_ASTER_BOT/runs/dry-run/`. The model, halts, diagnostics and the checklist for going
live are in the [runbook](LIGHTER_ASTER_BOT/RUNBOOK.md#dry-run).

## Live Run

```bash
cd LIGHTER_ASTER_BOT
tmux new -s lighter_aster_bot
./target/release/lighter_aster_bot run --market HYPE --mode live
```

- `[controller] max_loss_usdc` (default 15) is the cross-engine loss stop: the bot
  halts when marked equity falls 15 below its persisted baseline or realized trade PnL
  of both engines reaches −15. It sits above the engines' own `max_loss_usdc` /
  `max_cumulative_loss_usdc` of 10, so an engine breaker trips first and the controller
  stays a genuine backstop.
- A halt writes `runs/bot-<MARKET>.breaker.json`; restart after review with
  `--ack-breaker` (an equity-drawdown halt also needs `--reset-breaker-baseline`).

Stop with Ctrl-C in tmux (or `tmux send-keys -t lighter_aster_bot C-c`, SIGINT,
SIGTERM or SIGHUP). The active engine drains fully, which can take up to 175 s, and
paired positions stay open. Docker, deploy, the cutover from the retired
`orchestrator.py` and every latch are in the [runbook](LIGHTER_ASTER_BOT/RUNBOOK.md).

## The Taker on Its Own

Useful for diagnostics and controlled tests. A live taker takes the same per-market
lock as `run`, so it cannot run beside it. Run from `LIGHTER_ASTER_BOT/`. Observe-only,
then live:

```bash
./target/release/lighter_aster_bot taker run --markets HYPE --observe-only
./target/release/lighter_aster_bot taker run --markets HYPE
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

Both reports read `run`'s files (`LIGHTER_ASTER_BOT/runs/bot-<MARKET>-journal.jsonl`,
`bot-<MARKET>.state.json`, `trades_<MARKET>.jsonl`) and still read the retired
orchestrator's journal, ledger and state in the stack root's `runs/`. With `--dry-run`
they read only `LIGHTER_ASTER_BOT/runs/dry-run/`, and `trade_history.py` keeps its
database there:

```bash
python3 combined_pnl.py --market HYPE --dry-run
python3 trade_history.py --market HYPE --dry-run
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

`--raw-fills` and `--xemm-journal` are optional and repeatable; `--raw-fills`
accepts individual own-account fills with order/trade identities, quantities,
notional and fee evidence. Repair never multiplies aggregate legacy fees by
aggregate notional. Rebuilding is idempotent, keeps journals unchanged, and refuses
replacement if either database changed since review. No production history is
included in this checkout.

All signer processes must share `ASTER_NONCE_DIR`. The default is the OS temporary
directory's `lighter-aster-nonces` child. Each signer has a memory-mapped atomic
counter; initialization uses an OS lock. Keep that directory persistent and writable
by the same user for host and container processes; see the
[runbook](LIGHTER_ASTER_BOT/RUNBOOK.md#deploy).

The controller never switches engines after an unresolved or unclean engine stop; it
halts. A lease permits only reducing taker trades; cold observations expire after
500 ms and transmission checks the lease's actual expiry. See the
[runbook](LIGHTER_ASTER_BOT/RUNBOOK.md#halts-and-recovery) for execution recovery.

## Evidence Limits

No production history exists here; historical repair is validated with explicit
fixtures. Execution economics exclude
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
git ls-files | rg '(^|/)(aster|lighter)\.env$|(^|/)runs/|(^|/)target/|\.sqlite($|-)|\.db$|\.log$|\.pid$|\.logpath$|\.jsonl($|\.)|\.pem$|\.env$|\.key$' || true
git ls-files '*.zst'   # only tests/fixtures/*.zst belongs here
```

Check that local credential/runtime files are ignored:

```bash
git check-ignore -v \
  LIGHTER_ASTER_BOT/aster.env \
  LIGHTER_ASTER_BOT/lighter.env \
  LIGHTER_ASTER_BOT/runs/bot-HYPE.state.json
```

Scan staged text for common secret patterns:

```bash
git grep --cached -n -I -E 'BEGIN (RSA |EC |OPENSSH |DSA |)PRIVATE KEY|mnemonic|seed phrase|password\s*[:=]|secret\s*[:=]|private[_-]?key\s*[:=]|api[_-]?key\s*[:=]|access[_-]?token\s*[:=]|refresh[_-]?token\s*[:=]' || true
```

Some source files intentionally contain credential field names and deterministic
test vectors. Env files and real private values must remain untracked.
