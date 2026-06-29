//! Account/position reconciler (plan §2, §6 clean-start, §10 cold backstop). Reads both venues
//! via signed Aster REST + unsigned HL `/info` and assembles an [`AccountSnapshot`] of the REAL
//! positions. This module only READS + PUBLISHES the truth; the strategy's `recover_orphans`
//! (on the cold tick) is what ACTS on it — actively hedging or flattening any persistent net
//! delta a missed/dropped/rejected hedge left behind, and folding the reported positions back
//! into the predicted state. Runs once at startup (to gate clean-start) and then on a cold loop.

use std::collections::HashMap;

use anyhow::Result;
use rust_decimal::Decimal;
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::hotpath::clock::mono_now_ns;
use crate::markets::MarketSpec;
use crate::types::{MarketId, Side};

use super::account::{AccountSnapshot, AccountState, OpenOrderSnapshot, ScaledPosition, Venue};
use super::exec::aster::AsterRest;
use super::exec::hyperliquid::HlExchange;

/// Reads both venues and publishes [`AccountSnapshot`]s.
pub struct Reconciler {
    aster: AsterRest,
    hl: HlExchange,
    /// Aster UPPER symbol → market id.
    aster_sym_to_market: HashMap<String, MarketId>,
    /// HL coin → market id.
    hl_coin_to_market: HashMap<String, MarketId>,
}

impl Reconciler {
    pub fn new(aster: AsterRest, hl: HlExchange, specs: &[MarketSpec]) -> Self {
        let mut aster_sym_to_market = HashMap::new();
        let mut hl_coin_to_market = HashMap::new();
        for s in specs {
            aster_sym_to_market.insert(s.aster_symbol.to_uppercase(), s.market_id.clone());
            hl_coin_to_market.insert(s.hl_coin.clone(), s.market_id.clone());
        }
        Reconciler { aster, hl, aster_sym_to_market, hl_coin_to_market }
    }

    /// Assemble a fresh snapshot from live reads on both venues.
    pub async fn snapshot(&self) -> Result<AccountSnapshot> {
        // Stamp the read-START before ANY venue read (the orphan backstop's straddle guard requires
        // a timestamp from BEFORE the reads, not the post-read `source_ts_ns`).
        let read_start_ns = mono_now_ns();
        // Aster: balance + positions + open orders (signed).
        let bal = self.aster.balance().await?;
        let pos = self.aster.position_risk().await?;
        let oo = self.aster.open_orders(None).await?;
        // HL: clearinghouse state + open orders (unsigned /info).
        let ch = self.hl.clearinghouse_state().await?;
        let hloo = self.hl.open_orders_info().await?;

        // Aster available USD = the sum of actually-deposited collateral (`balance`/wallet
        // balance), NOT `availableBalance`. The per-asset `availableBalance` is an inflated
        // cross-margin projection (e.g. a token row reporting thousands while its real balance
        // is 0); summing real `balance` across stablecoins gives the true ~$124 USDC collateral.
        // "Any stablecoin counts" (the account is multi-collateral cross-margin).
        let aster_available_usd = bal
            .iter()
            .filter_map(|r| r.balance.parse::<Decimal>().ok())
            .filter(|b| *b > Decimal::ZERO)
            .sum();
        let hl_withdrawable_usd = ch.withdrawable.parse().unwrap_or(Decimal::ZERO);

        // TOTAL (mark-to-market) equity per venue for the circuit breaker — NOT the free-margin
        // figures above, which drop by the locked margin when a hedge is open and would false-trip.
        // Aster: wallet balance + Σ position unrealized PnL. HL: marginSummary.accountValue (already
        // includes unrealized). For a delta-neutral book the unrealized legs cancel ⇒ stable equity.
        let aster_unrealized_usd: Decimal = pos
            .iter()
            .filter_map(|p| p.unrealized_profit.parse::<Decimal>().ok())
            .sum();
        let aster_equity_usd = aster_available_usd + aster_unrealized_usd;
        let hl_equity_usd = ch.margin_summary.account_value.parse().unwrap_or(Decimal::ZERO);

        // NET positionRisk rows per market: in hedge (dual-side) mode Aster returns separate
        // LONG and SHORT rows for one symbol, each with its own signed positionAmt. Summing the
        // signed amounts gives the correct net regardless of mode, so a `.find()` downstream can
        // never silently drop a side. (One-way mode is also asserted at startup — see is_one_way.)
        let mut aster_net: HashMap<MarketId, (Decimal, Decimal)> = HashMap::new(); // market -> (net_qty, entry_px)
        for p in &pos {
            let qty: Decimal = p.position_amt.parse().unwrap_or(Decimal::ZERO);
            if qty == Decimal::ZERO {
                continue;
            }
            if let Some(market) = self.aster_sym_to_market.get(&p.symbol.to_uppercase()) {
                let e = aster_net.entry(market.clone()).or_insert((Decimal::ZERO, Decimal::ZERO));
                e.0 += qty;
                // Keep the entry px of the larger-magnitude leg (informational only).
                e.1 = p.entry_price.parse().unwrap_or(e.1);
            }
        }
        let aster_positions: Vec<ScaledPosition> = aster_net
            .into_iter()
            .filter(|(_, (q, _))| *q != Decimal::ZERO)
            .map(|(market, (signed_qty, entry_px))| ScaledPosition { venue: Venue::Aster, market, signed_qty, entry_px })
            .collect();

        let mut hl_positions = Vec::new();
        for ap in &ch.asset_positions {
            let qty: Decimal = ap.position.szi.parse().unwrap_or(Decimal::ZERO);
            if qty == Decimal::ZERO {
                continue;
            }
            if let Some(market) = self.hl_coin_to_market.get(&ap.position.coin) {
                hl_positions.push(ScaledPosition {
                    venue: Venue::Hyperliquid,
                    market: market.clone(),
                    signed_qty: qty,
                    entry_px: ap.position.entry_px.as_deref().and_then(|s| s.parse().ok()).unwrap_or(Decimal::ZERO),
                });
            }
        }

        let mut open_orders = Vec::new();
        for o in &oo {
            if let Some(market) = self.aster_sym_to_market.get(&o.symbol.to_uppercase()) {
                open_orders.push(OpenOrderSnapshot {
                    venue: Venue::Aster,
                    market: market.clone(),
                    side: if o.side.eq_ignore_ascii_case("SELL") { Side::Sell } else { Side::Buy },
                    price: o.price.parse().unwrap_or(Decimal::ZERO),
                    qty: o.orig_qty.parse().unwrap_or(Decimal::ZERO),
                    client_id: (!o.client_order_id.is_empty()).then(|| o.client_order_id.clone()),
                    venue_order_id: Some(o.order_id.to_string()),
                });
            }
        }
        for o in &hloo {
            if let Some(market) = self.hl_coin_to_market.get(&o.coin) {
                open_orders.push(OpenOrderSnapshot {
                    venue: Venue::Hyperliquid,
                    market: market.clone(),
                    side: if o.side.eq_ignore_ascii_case("A") { Side::Sell } else { Side::Buy },
                    price: o.limit_px.parse().unwrap_or(Decimal::ZERO),
                    qty: o.sz.parse().unwrap_or(Decimal::ZERO),
                    client_id: None,
                    venue_order_id: Some(o.oid.to_string()),
                });
            }
        }

        Ok(AccountSnapshot {
            aster_available_usd,
            hl_withdrawable_usd,
            aster_equity_usd,
            hl_equity_usd,
            aster_positions,
            hl_positions,
            open_orders,
            generation: 0, // set by AccountState::publish
            source_ts_ns: mono_now_ns(),
            read_start_ns,
        })
    }

    /// Refuse to trade live unless the Aster account is in ONE-WAY position mode (the bot sends
    /// `positionSide=BOTH` and nets positions assuming one-way; hedge mode would mis-route + mis-
    /// report — see the reconciler's per-market netting and aster.rs::place_params).
    pub async fn assert_one_way(&self) -> Result<()> {
        if !self.aster.is_one_way().await? {
            anyhow::bail!(
                "Aster account is in HEDGE (dual-side) position mode; this bot requires ONE-WAY \
                 mode. Switch it (asterdex.com or POST /fapi/v3/positionSide/dual dualSidePosition=false) \
                 before live trading."
            );
        }
        info!("aster position mode: ONE-WAY (verified)");
        Ok(())
    }

    /// Enforce the CLEAN-START invariant (§8.1 inv 7) before quoting: cancel all resting orders on
    /// our symbols, then poll `openOrders` until no bot-prefixed (`X…`) order remains — so a fast
    /// startup can never begin quoting while stray orders from a PRIOR run still rest. Bounded poll
    /// (≤6 tries) so startup can't hang. With `require_clean_start`, a still-dirty book after the
    /// retries is a HARD error (refuse to quote into a dirty book). At startup the bot has placed
    /// nothing, so every `X…` order is by definition a prior-run stray (each run uses a fresh random
    /// session id) — the empty-known-set analogue of [`AccountSnapshot::unknown_bot_orders`].
    pub async fn ensure_clean_start(&self, startup_cancel_all: bool, require_clean_start: bool) -> Result<()> {
        if startup_cancel_all {
            for market in self.aster_sym_to_market.values() {
                if let Err(e) = self.aster.cancel_all_symbol(market).await {
                    warn!("startup cancel-all on {market} failed: {e:#}");
                }
            }
        }
        for attempt in 1..=6u32 {
            // A FAILED read must NOT be mistaken for an empty book — treating Err as "no orders"
            // would certify a possibly-dirty book clean on the first transient error (TLS reset on a
            // fresh pooled conn, a 429 after the cancel-all burst, a timeout) and skip both the
            // remaining retries and the `require_clean_start` bail. So on Err we warn, consume the
            // attempt, and retry — the early `return Ok(())` below is reachable ONLY after a
            // SUCCESSFUL read proves the stray set empty.
            let open = match self.aster.open_orders(None).await {
                Ok(o) => o,
                Err(e) => {
                    warn!("clean-start: openOrders read failed (attempt {attempt}/6): {e:#}");
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    continue;
                }
            };
            let stray: Vec<String> = open
                .iter()
                .filter(|o| {
                    o.client_order_id.starts_with('X')
                        && self.aster_sym_to_market.contains_key(&o.symbol.to_uppercase())
                })
                .map(|o| o.client_order_id.clone())
                .collect();
            if stray.is_empty() {
                info!("clean start verified: no stray bot orders on our symbols");
                return Ok(());
            }
            warn!("clean-start: {} stray bot order(s) remain (attempt {attempt}/6): {stray:?}", stray.len());
            if startup_cancel_all {
                for market in self.aster_sym_to_market.values() {
                    let _ = self.aster.cancel_all_symbol(market).await; // re-cancel anything still resting
                }
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        // Reached only if no SUCCESSFUL read ever proved the book empty — either stray bot orders
        // kept resting after cancel-all, or every openOrders read errored. Either way the book is
        // not VERIFIED clean.
        if require_clean_start {
            anyhow::bail!(
                "clean-start failed: could not verify an empty order book after cancel-all (stray bot \
                 orders still resting, or every openOrders read errored); refusing to quote into a \
                 possibly-dirty book (set [live] require_clean_start=false to override)"
            );
        }
        warn!("clean-start NOT verified but require_clean_start=false — proceeding (deadman backstop active)");
        Ok(())
    }

    /// Reconcile once and publish. Returns the published snapshot.
    pub async fn reconcile_and_publish(&self, account: &AccountState) -> Result<AccountSnapshot> {
        let snap = self.snapshot().await?;
        account.publish(snap.clone());
        Ok(snap)
    }

    /// Cold reconcile loop: publish a fresh snapshot every `interval`, until cancelled. A failed
    /// read keeps the prior snapshot (the strategy's `account_fresh` gate then closes quoting if
    /// it ages out — fail-safe). The snapshot must refresh well within
    /// `max_account_snapshot_age_ms`, so `interval` should be a fraction of it.
    pub async fn run(self, account: AccountState, shutdown: CancellationToken, interval: Duration) {
        info!("account reconciler started (interval {:?})", interval);
        // A single reconcile must NEVER wedge the loop. It awaits sequential signed REST reads; a
        // black-holed connection (no response AND no error) would otherwise hang the await forever —
        // the snapshot then ages out, which SILENTLY closes the maker gate (`account_fresh`) AND
        // disables the orphan-recovery backstop (which early-returns on a stale snapshot). Bounding
        // each cycle drops a hung read so the loop keeps retrying and a transient black-hole self-heals.
        let budget = (interval * 3).max(Duration::from_secs(5));
        let mut consecutive_stalls: u32 = 0;
        let mut tick = tokio::time::interval_at(Instant::now() + interval, interval);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = tick.tick() => {
                    match tokio::time::timeout(budget, self.reconcile_and_publish(&account)).await {
                        Ok(Ok(_)) => {
                            if consecutive_stalls > 0 {
                                info!("reconcile recovered after {consecutive_stalls} stalled cycle(s)");
                            }
                            consecutive_stalls = 0;
                        }
                        Ok(Err(e)) => {
                            consecutive_stalls += 1;
                            warn!("reconcile failed (keeping prior snapshot, {consecutive_stalls} in a row): {e:#}");
                        }
                        Err(_) => {
                            consecutive_stalls += 1;
                            warn!("reconcile TIMED OUT after {budget:?} (venue read wedged?); keeping prior snapshot, {consecutive_stalls} in a row");
                        }
                    }
                    // Once snapshots stop advancing for several cycles the snapshot is going stale: the
                    // maker gate will close on ACCOUNT_SNAPSHOT_STALE and orphan recovery is paused.
                    // Make that LOUD so the operator sees it instead of discovering a dead bot hours later.
                    if consecutive_stalls == 3 {
                        error!(
                            "account reconciler STALLED {consecutive_stalls} cycles (~{:?}): snapshot going stale — \
                             maker quoting will freeze (ACCOUNT_SNAPSHOT_STALE) and orphan recovery is paused until reads recover",
                            interval * consecutive_stalls
                        );
                    }
                }
            }
        }
        info!("account reconciler stopped");
    }
}
