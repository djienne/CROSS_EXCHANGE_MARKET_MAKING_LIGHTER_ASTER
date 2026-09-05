# XEMM operation and recovery

Use Rust 1.92 and `cargo build --release --locked`. Configuration and container
setup are in [DOCKER_DEPLOY.md](DOCKER_DEPLOY.md); shared accounting and historical
repair are documented in the [stack README](../README.md).

Paper mode runs public feeds and the simulated executor:

```bash
./target/release/xemm_lighter_aster --config config-live-lighter.toml livebot \
  --mode paper --markets HYPE --secs 30 --db runs/paper.sqlite --out runs/paper.jsonl.zst
./target/release/xemm_lighter_aster --config config-live-lighter.toml live-report \
  --journal runs/paper-journal.jsonl --market HYPE --details
```

Live mode requires explicit enabled configuration and one selected market. Aster
uses the documented EIP-712 request contract; Lighter connections are established
by the transport worker. Only supported mainnet REST origins authorize live operation.
Maker transmission rechecks the in-memory admission ticket, quote deadline and
book versions after rate limiting. Missing/stale source time blocks new exposure.

Logical obligations retain separate transmission attempts. Cancellation can revoke
an unclaimed attempt; claimed or ambiguous attempts stay reserved until matching
terminal evidence arrives. A timeout, balanced position snapshot or empty open-order
list cannot prove nonexecution. After 60 seconds without resolution, new exposure
stays frozen while late evidence is still accepted. Cumulative evidence applies
only newly confirmed quantities. Corrective reductions use the smallest rounded-down
quantity that removes the residual, at most two attempts per incident; unresolved
attempts and minimum/margin rejections cannot start a replacement loop.

The Lighter free-margin buffer is $25 by default and $26 in the shipped live config,
under the enforced 1x assumption. Existing maker orders and hedge obligations reserve
directional margin. Confirmed reductions remain possible when opening margin is low.
Retired partial-policy switches, quote-tick thresholds, maximum pending count and
`expires_after_ms` fail startup instead of silently doing nothing.

Stop with Ctrl-C or SIGINT. Shutdown first quiesces admission and cancels makers,
then drains fills and execution outcomes, performs final reconciliation and drains
persistence. Paired positions may remain open. A clean exit requires verified orders,
residuals and persistence; a nonzero exit must halt supervisor replacement.

`<db-stem>.active.json` remains after an unclean/unresolved session. Keep it until
cold venue records definitively resolve every attempted order in the journal and
orders/positions are reconciled; an empty orders snapshot alone is insufficient.
The separate `<db-stem>.trip.json` loss latch can be reset with
`scripts/reset_breaker.py` after reviewing the loss. Resetting a trip does not clear
an unresolved session. Use the same DB path when resuming an established run.
