# Audit fixes and validation - 2026-09-05

Baseline: `5dae45f`. Implemented on `codex/fix-execution-accounting-latency`.
No live restart, deployment, private probe or real order submission was performed.
Profitability thresholds, cooldowns and slippage settings remain unchanged.

## Finding to fix

| Baseline finding | Implemented behavior and evidence |
|---|---|
| 1: timed-out original can execute after replacement | Atomic queued/claimed/cancelled admission, separate logical obligations and attempts, cumulative terminal evidence, held ambiguous exposure and a 60-second resolution budget. Claim/cancel races, late fills and duplicate evidence are exercised. |
| 2: stalled Lighter handshake | Cold connection owner with 3-second handshake bound; unready submissions return NotSent. Write/response uncertainty stays Unknown. Local stalled sockets and ambiguous transport tests pass. |
| 3: wrong-direction or whole-position correction | Reduce the actual same-sign residual position, prefer Aster when eligible, floor to step, retain dust and limit each incident to two tracked attempts. Closing partials on both venues and rejection/budget cases are covered. |
| 4: absent real hedge-capacity guard | Separate wallet/equity/free margin; reserve directional maker/hedge obligations once. Lighter buffer is $25 default/$26 shipped under 1x. Margin rejection and permitted reduction cases pass. |
| 5 and 11: stale prices/rights and account refresh race | Cold 250 ms lease/order observations, 500 ms validity, execution epochs and a final in-memory book/source/epoch/lease/transport gate. Maker admission is rechecked after limiter waits. |
| 6 and 10: safety delayed by persistence; omitted breaker-triggering recovery | Quiesce and enqueue cancellation before cold persistence. Acknowledge economic records before breaker return/clean exit. Writer stalls, full queues, durable recovery and shutdown are exercised. |
| 7: roundtrip diagnostic can leave a bought position | Always reconcile possible acceptance and attempt cleanup within 30 seconds/three closes, retaining the original diagnostic error and cleanup result. |
| 8 and 9: fee units, partial economics and lost corrections | Versioned shared fixtures; individual own-role fee ticks times notional / 1,000,000; signed rebates; raw/cumulative deduplication; revisions preserve logical counts. Unknown evidence stays NULL in SQL/JSON. |
| Historical repair | Separate candidate, comparison and reviewed replacement with backup/concurrency checks. Fixtures exercise idempotence, actual raw-fee repair, stronger-confirmation preservation, no duplicated confirmed fills, and retained schema extensions/unrelated tables. |
| 12: shared latency trajectories, residual loss and double counting | Independent market/queue/latency state, chronological scheduled execution, private consumed liquidity, actual VWAP/quantity positions, retained residuals/censored orders and fresh marked ledgers. Analytical full-input cashflows and scenario isolation pass. |
| 13: Docker/toolchain mismatch | Both manifests and Docker builder use Rust 1.92 and locked dependencies. Actual image builds, offline replay and public-feed paper smoke pass. |
| Signing, nonce and source/environment contracts | Exact transmitted EIP-712 query with independent eth-account vectors; signer-scoped cross-process mapped nonce counter; one supervisor/container directory; source timestamps and 1,000 ms future tolerance; unsupported operational origins rejected. |
| Hot-path work, concurrent writers and ineffective controls | Coalesced book wakes with 10 ms fallback; top-book bound before depth; cold exact counted partitions for the unchanged 72-hour/one-second history. Persistent bounded writers, cold serialization, OS append locks and coalesced snapshots. Retired keys fail startup. Dead signing, float-book and execution code/tests removed. |
| Weak tests and supervisor restart | Quiet connected-socket watchdogs, actual child termination, actual loss-stop/backoff timing, native nonce subprocess tests and shared arithmetic fixtures. Unclean/unresolved exits block automatic replacement. |

Aster protocol evidence follows the [documented EIP-712 contract](https://asterdex.github.io/aster-api-website/asterCode/authentication/).
Lighter fee units/omission evidence comes from the [trade circuit](https://github.com/elliottech/lighter-prover/blob/main/circuit/src/apply_trade.rs),
[fee constants](https://github.com/elliottech/lighter-prover/blob/main/circuit/src/types/constants.rs) and
[WebSocket reference](https://apidocs.lighter.xyz/docs/websocket-reference).
Offline protocol vectors do not establish private venue acceptance.

## Validation

- First transport commit's isolated staged Linux tree: taker 51/XEMM 36 selected release tests passed.
- Atomic execution commit's isolated staged Linux tree: taker 140/XEMM 403 release tests passed.
- Accounting commit's isolated staged Linux tree: Python 100 and Rust report 4 tests passed.
- Final full Linux release/default and no-default results: taker 140 passed; XEMM 411 default / 164 no-default passed. No-default production builds and the default Docker production build passed. Explicit ignored tests are subprocess helpers exercised by parents and the opt-in benchmark.
- All five in-memory mutations were killed by the final 100-test Python suite, including the previously surviving loss-breaker, child-stop and observer-backoff mutations.
- Docker public-feed paper: 20 seconds, both feeds connected/fresh, 401 recorded events, clean shutdown, no fills. Replayed that captured tape offline. Synthetic offline replay additionally exercised fills, different hedge arrivals and EOF censoring.
- Fixture repair reproduces maker 0.20 at 100 / hedge 0.12 at 101 / fee 0.01 as matched execution net 0.11 with residual 0.08. A separate raw-fee repair yields net 0.93228; the shared signed-rebate case yields net 0.126424. Old unproven aggregates remain incomplete.
- No `cargo fmt`; no credentials, runtime output or benchmark harnesses are committed.

## Local performance

Windows AMD Ryzen 9 7900, Rust 1.92 release. These are CPU/queue measurements with OS scheduling and concurrent development activity, not deployed or network latency.

| Workload | Before | After | Measurement scope |
|---|---:|---:|---|
| Exact decimal book publication | median 8.435 us; p95 10.490 us | median 1.720 us; p95 2.739 us | 101 alternating 1,000-update batches; decode four deltas, 100 levels/side, publish 20 levels and build scaled HotBook. Percentiles are of batch averages. |
| Full 259,200-sample entry history | p50 19.344 ms; sampled p99 23.083 ms | cached hot p50/p99 about 0.1 us; cold update p50/p99 2.2/3.7 us | Same LCG distribution; 20 baseline sorts, versioned hot threshold and cold counted partitions. Timer resolution limits the hot read; 20 repetitions give a coarse baseline tail estimate. |
| Journal fill bursts | p50/p95/p99 1.8/4.3/6.3 us | 0.9/2.2/3.7 us | Five alternating rounds x 32,768 events, draining 64-event bursts. Old four-field JSON versus richer typed maker evidence; old extra context logging is excluded. |
| Full journal queue | p50/p95/p99 0.4/0.7/0.8 us | 0.2/0.3/0.3 us | 65,536-record blocked queue; every further journal enqueue rejected, every cancellation queue dispatch succeeded. New health latch blocks further entries. |

The 20-level taker scan benchmark used 10,000 samples per case on the same host.
For unprofitable books, p50/p95/p99 changed from 3.4/5.1/5.7 us to
0.2/0.2/0.2 us; for qualified books, from 5.1/7.9/8.7 us to
4.7/5.8/6.7 us. The top-book rejection saves depth work; surviving candidates
still run precise sizing. These measurements exclude gate I/O, signing and network.
Full-history cold timing includes insertion, timestamp expiry and threshold publication;
the cached hot timing excludes a due sample and cannot be used as full scan latency.

## Remaining evidence limits

No production history or representative long market tape exists here; historical repair is validated with explicit fixtures. The short public tape establishes transport/paper/replay operation, not trading edge. Execution economics exclude funding and unpaired account marks; account-equity changes also include transfers. Unresolved sessions lacking terminal venue identity remain blocked until primary evidence resolves them. These checks do not certify deployed latency, actual profitability, live venue acceptance or signer-binary supply-chain integrity.

## Commit sequence

- `a4df429`: bounded cold transport.
- `fadb13b`: atomic signing/execution/recovery and cold-plane contracts, validated together because their shared interfaces are coupled.
- `bc7d80a`: accounting, historical repair and supervisor corrections.
- `735bd44`: independent simulation and scenario-aware verification.
- `2f1b8d7`: Docker/nonce deployment configuration and obsolete probe removal.
- Final documentation commit: this compact finding/validation map and consolidated runbooks.

Operational commands and repair usage are consolidated in [README.md](README.md), the
[taker guide](LIGHTER_ASTER_TAKER_ARB/README.md), and the [XEMM runbook](XEMM_LIGHTER_ASTER/LIVE_RUNBOOK.md).
Validation logs and bounded harnesses remain under ignored `runs/audit-20260905/`.
