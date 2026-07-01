//! The deterministic simulation engine. Consumes `(local_recv_ts, seq)`-ordered
//! events and, per (market, queue-model) state, recomputes quotes on book moves,
//! fills resting quotes on Aster sweeps, accumulates sub-min inventory, and
//! schedules HL hedges at each latency bucket — persisting every artifact.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::book::OrderBook;
use crate::config::Config;
use crate::edge::EdgeConfig;
use crate::events::{Event, EventKind};
use crate::fill_sweep::{apply_print, AsterAggTrade, SimulatedAsterFill};
use crate::hedge::{resolve_hedge, PendingHedge};
use crate::inventory::{check_pending_limits, handle_fill, HedgeabilityRules};
use crate::markets::{MarketSpec, MarketState};
use crate::position::SignedPosition;
use crate::quote_engine::{
    compute_desired_quote, resting_quote_net_edge_bps, PositionContext, QuoteEngineConfig,
};
use crate::requoter::{LiveQuote, LiveQuoteState, ReplaceReason, RequoteConfig};
use crate::store::db::{FillRow, HedgeRow, OpportunityRow, PendingEventRow, QuoteRevisionRow};
use crate::store::Db;
use crate::types::{MarketId, QueueModel, RejectReason, Side};
use tracing::{debug, trace};

/// A hedge scheduled to resolve at `resolve_at`; ordered as a min-heap so the
/// earliest-due hedge is popped first.
struct Scheduled {
    resolve_at: DateTime<Utc>,
    seq: u64,
    hedge: PendingHedge,
}

impl PartialEq for Scheduled {
    fn eq(&self, o: &Self) -> bool {
        self.resolve_at == o.resolve_at && self.seq == o.seq
    }
}
impl Eq for Scheduled {}
impl Ord for Scheduled {
    fn cmp(&self, o: &Self) -> Ordering {
        // Reverse so the smallest resolve_at is the heap maximum.
        o.resolve_at
            .cmp(&self.resolve_at)
            .then_with(|| o.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

pub struct SimEngine {
    edge: EdgeConfig,
    quote: QuoteEngineConfig,
    requote: RequoteConfig,
    partials_min_notional: Decimal,
    strict_partials: bool,
    max_pending_notional: Decimal,
    max_pending_age_ms: i64,
    hidden_mult: Decimal,
    buckets: Vec<i64>,
    staleness_ms: i64,
    halt_on_stale: bool,
    ring_window_ms: i64,
    aster_cap_notional: Decimal,
    hl_cap_notional: Decimal,
    enforce_cap: bool,
    models: Vec<QueueModel>,
    states: HashMap<(MarketId, QueueModel), MarketState>,
    pending: BinaryHeap<Scheduled>,
    hedge_seq: u64,
}

impl SimEngine {
    pub fn new(cfg: Config, specs: Vec<MarketSpec>) -> Result<Self> {
        let models = cfg.queue_model.parsed_models()?;
        let mut states = HashMap::new();
        for spec in &specs {
            for m in &models {
                states.insert(
                    (spec.market_id.clone(), *m),
                    MarketState::new(spec.clone(), *m),
                );
            }
        }
        let max_bucket = cfg.simulation.hedge_latency_buckets_ms.iter().copied().max().unwrap_or(1000);
        let ring_window_ms = max_bucket + 2_000;
        Ok(SimEngine {
            edge: cfg.edge.clone(),
            quote: cfg.quote.clone(),
            requote: cfg.simulation.requote_config(),
            partials_min_notional: cfg.partials.hyperliquid_min_notional,
            strict_partials: cfg.partials.strict_all_partials_must_be_hedgeable,
            max_pending_notional: cfg.partials.max_pending_inventory_notional,
            max_pending_age_ms: cfg.partials.max_pending_inventory_age_ms,
            hidden_mult: cfg.queue_model.hidden_queue_multiplier,
            buckets: cfg.simulation.hedge_latency_buckets_ms.clone(),
            staleness_ms: cfg.simulation.max_book_staleness_ms,
            halt_on_stale: cfg.simulation.halt_trading_on_stale_feed,
            ring_window_ms,
            aster_cap_notional: cfg.capital.aster_cap_notional(),
            hl_cap_notional: cfg.capital.hyperliquid_cap_notional(),
            enforce_cap: cfg.capital.enforce_position_cap,
            models,
            states,
            pending: BinaryHeap::new(),
            hedge_seq: 0,
        })
    }

    pub fn on_event(&mut self, ev: &Event, db: &mut Db) -> Result<()> {
        let now = ev.local_recv_ts;
        let models = self.models.clone();

        // 1. Resolve hedges whose resolve time has arrived.
        resolve_due_hedges(
            &self.states,
            &mut self.pending,
            &self.edge,
            self.staleness_ms,
            now,
            db,
        )?;

        // 2. Advance time-driven state machines and GC dying quotes.
        for st in self.states.values_mut() {
            advance_and_gc(st, now, db)?;
        }

        // 3. Apply the event.
        match &ev.kind {
            EventKind::HlL2Book { bids, asks, exch_ts } => {
                let book = OrderBook::from_levels(bids.clone(), asks.clone(), *exch_ts, now);
                let window = self.ring_window_ms;
                for m in &models {
                    if let Some(st) = self.states.get_mut(&(ev.market.clone(), *m)) {
                        st.hl_book_ring.push_back(book.clone());
                        crate::sim::clock::prune_ring(&mut st.hl_book_ring, now, window);
                    }
                }
                for m in &models {
                    if let Some(st) = self.states.get_mut(&(ev.market.clone(), *m)) {
                        recompute_quotes(
                            st, &self.edge, &self.quote, &self.requote, self.hidden_mult,
                            self.staleness_ms, self.aster_cap_notional, self.hl_cap_notional,
                            self.enforce_cap, now, db,
                        )?;
                    }
                }
            }
            EventKind::AsterDepth { bids, asks, exch_ts } => {
                let book = OrderBook::from_levels(bids.clone(), asks.clone(), *exch_ts, now);
                for m in &models {
                    if let Some(st) = self.states.get_mut(&(ev.market.clone(), *m)) {
                        st.aster_book = Some(book.clone());
                        recompute_quotes(
                            st, &self.edge, &self.quote, &self.requote, self.hidden_mult,
                            self.staleness_ms, self.aster_cap_notional, self.hl_cap_notional,
                            self.enforce_cap, now, db,
                        )?;
                    }
                }
            }
            EventKind::AsterAggTrade { price, qty, buyer_is_maker, exch_ts } => {
                let agg = AsterAggTrade {
                    market: ev.market.clone(),
                    price: *price,
                    qty: *qty,
                    buyer_is_maker: *buyer_is_maker,
                    exch_ts: *exch_ts,
                    local_recv_ts: now,
                };
                for m in &models {
                    if let Some(st) = self.states.get_mut(&(ev.market.clone(), *m)) {
                        apply_trade(
                            st, &agg, &self.edge, &self.requote, self.partials_min_notional,
                            self.strict_partials, &self.buckets, self.staleness_ms, self.halt_on_stale,
                            &mut self.pending, &mut self.hedge_seq, db, now,
                        )?;
                    }
                }
            }
            EventKind::HlTrade { .. } => { /* diagnostics only */ }
        }

        // 4. Pending-inventory risk checks.
        let (max_notional, max_age) = (self.max_pending_notional, self.max_pending_age_ms);
        for st in self.states.values_mut() {
            check_state_pending(st, max_notional, max_age, now, db)?;
        }
        Ok(())
    }

    /// Resolve all remaining hedges (flagged stale), mark out residual inventory,
    /// and stamp the run finished.
    pub fn finalize(&mut self, end_ts: DateTime<Utc>, db: &mut Db) -> Result<()> {
        // Resolve leftovers against the newest available HL book.
        while let Some(s) = self.pending.pop() {
            let key = (s.hedge.market.clone(), s.hedge.queue_model);
            let book = self.states.get(&key).and_then(|st| st.hl_book_ring.back());
            match book {
                Some(book) => {
                    let stale =
                        (s.hedge.resolve_at - book.local_recv_ts).num_milliseconds() > self.staleness_ms;
                    let res = resolve_hedge(&s.hedge, book, &self.edge, stale);
                    db.insert_hedge(&HedgeRow::from_result(&res))?;
                }
                // No HL book ever seen for this market: record the leftover hedge as
                // unbooked rather than dropping it (every scheduled hedge -> one row).
                None => db.insert_hedge(&HedgeRow::unbooked(&s.hedge, RejectReason::MissingHlBook.as_str()))?,
            }
        }
        // Mark residual pending inventory to market.
        for st in self.states.values_mut() {
            if let Some(inv) = st.pending_inv.take() {
                let mark = mark_price(st).unwrap_or(inv.avg_aster_px);
                let abs = inv.signed_qty.abs();
                let mtm = if inv.signed_qty > Decimal::ZERO {
                    abs * (mark - inv.avg_aster_px)
                } else {
                    abs * (inv.avg_aster_px - mark)
                };
                let mut row = PendingEventRow::new(
                    st.spec.market_id.clone(), st.queue_model, "MARKED",
                    inv.signed_qty, inv.avg_aster_px, abs * mark, end_ts,
                );
                row.mark_px = Some(mark);
                row.realized_pnl = Some(mtm);
                row.first_fill_ts = Some(inv.first_fill_ts);
                row.last_fill_ts = Some(inv.last_fill_ts);
                row.reason = Some("END_OF_RUN".to_string());
                db.insert_pending_event(&row)?;
            }
        }
        db.finish_run(end_ts)?;
        db.flush()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Free helpers (one MarketState at a time; engine passes disjoint self fields).
// ---------------------------------------------------------------------------

fn mark_price(st: &MarketState) -> Option<Decimal> {
    let am = st.aster_book.as_ref().and_then(|b| b.mid());
    let hm = st.hl_book().and_then(|b| b.mid());
    match (am, hm) {
        (Some(a), Some(h)) => Some((a + h) / Decimal::from(2)),
        (Some(a), None) => Some(a),
        (None, Some(h)) => Some(h),
        (None, None) => None,
    }
}

fn advance_and_gc(st: &mut MarketState, now: DateTime<Utc>, db: &mut Db) -> Result<()> {
    // Touch prices for the post-only (GTX) activation check: a quote that would cross
    // the book by the time placement latency elapsed would be rejected by Aster.
    let best_ask = st.aster_book.as_ref().and_then(|b| b.best_ask()).map(|l| l.px);
    let best_bid = st.aster_book.as_ref().and_then(|b| b.best_bid()).map(|l| l.px);

    // Sides whose just-activated quote crossed and would be GTX-rejected. Recorded
    // after the per-quote borrows are released. The PendingPlacement->Live transition
    // fires exactly once per quote, so each crossing is counted exactly once (no dedup).
    let mut gtx_rejected: Vec<Side> = Vec::new();

    if let Some(q) = st.live_bid.as_mut() {
        let was_placing = q.state == LiveQuoteState::PendingPlacement;
        q.advance_state(now);
        if was_placing && q.state == LiveQuoteState::Live {
            if let Some(ask) = best_ask {
                if q.price() >= ask {
                    q.state = LiveQuoteState::Cancelled; // GTX would reject a crossing bid
                    gtx_rejected.push(Side::Buy);
                }
            }
        }
        if q.is_terminal() {
            st.live_bid = None;
        }
    }
    if let Some(q) = st.live_ask.as_mut() {
        let was_placing = q.state == LiveQuoteState::PendingPlacement;
        q.advance_state(now);
        if was_placing && q.state == LiveQuoteState::Live {
            if let Some(bid) = best_bid {
                if q.price() <= bid {
                    q.state = LiveQuoteState::Cancelled; // GTX would reject a crossing ask
                    gtx_rejected.push(Side::Sell);
                }
            }
        }
        if q.is_terminal() {
            st.live_ask = None;
        }
    }
    for q in st.dying.iter_mut() {
        q.advance_state(now);
    }
    st.dying.retain(|q| !q.is_terminal());

    // Make GTX (post-only) placement rejects visible — previously a crossing quote was
    // silently cancelled, so the report could not show how often this happened.
    for side in gtx_rejected {
        db.record_opportunity(&OpportunityRow::rejected(
            st.spec.market_id.clone(),
            side,
            st.queue_model,
            RejectReason::PostOnlyRejectedOnPlacement,
            now,
        ))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn recompute_quotes(
    st: &mut MarketState,
    edge: &EdgeConfig,
    quote: &QuoteEngineConfig,
    requote: &RequoteConfig,
    hidden_mult: Decimal,
    staleness_ms: i64,
    aster_cap_notional: Decimal,
    hl_cap_notional: Decimal,
    enforce_cap: bool,
    now: DateTime<Utc>,
    db: &mut Db,
) -> Result<()> {
    let (aster, hl) = match (st.aster_book.clone(), st.hl_book().cloned()) {
        (Some(a), Some(h)) => (a, h),
        _ => return Ok(()),
    };
    let spec = st.spec.clone();
    let qm = st.queue_model;
    let market = spec.market_id.clone();
    // Capital/position context — constant across both sides within this tick.
    let pos = PositionContext {
        aster_pos_qty: st.aster_pos.qty,
        hl_pos_qty: st.hl_pos.qty,
        aster_cap_notional,
        hl_cap_notional,
        enforce: enforce_cap,
        reduce_position_only: false,
    };

    for side in [Side::Buy, Side::Sell] {
        let res = compute_desired_quote(
            edge, quote, &aster, &hl, side, spec.tick, spec.step, spec.aster_min_qty,
            spec.aster_min_notional, spec.hl_min_notional, staleness_ms, now, &pos,
        );
        match res {
            Ok(dq) => {
                let cur = slot_take(st, side);
                match cur {
                    None => {
                        db.record_opportunity(&OpportunityRow::accepted(market.clone(), qm, &dq, edge, now))?;
                        trace!(
                            "place {} {:?} {:?} {} x{} edge={}bps clamped={}",
                            market.0, qm, side, dq.price, dq.qty, dq.instant_edge_bps, dq.size_clamped_up,
                        );
                        let lq = LiveQuote::from_desired(market.clone(), dq, now, requote, qm, hidden_mult);
                        slot_set(st, side, Some(lq));
                        st.set_last_requote(side, now);
                    }
                    Some(mut q) => {
                        let throttle_ok = st
                            .last_requote(side)
                            .is_none_or(|t| (now - t).num_milliseconds() >= quote.min_requote_interval_ms as i64);
                        // Re-validate the resting quote against the CURRENT HL book on every
                        // book move (independent of the throttle — cancelling is never rate-
                        // limited). If it can no longer be hedged at the hurdle, replace it now
                        // with the fresh profitable quote so we never rest a losing quote.
                        let resting_unprofitable = q.is_active()
                            && resting_quote_net_edge_bps(
                                edge,
                                &hl,
                                side,
                                q.price(),
                                q.remaining_qty,
                                dq.ref_px,
                                quote.depth_liquidity_multiple,
                            )
                            .is_none_or(|e| e < edge.min_net_profit_bps);
                        let replace = if resting_unprofitable {
                            Some(ReplaceReason::NoLongerProfitable)
                        } else if q.is_active() && throttle_ok {
                            q.should_replace(&dq, quote.price_change_ticks_to_requote, spec.tick)
                        } else {
                            None
                        };
                        match replace {
                            Some(reason) => {
                                db.record_opportunity(&OpportunityRow::accepted(market.clone(), qm, &dq, edge, now))?;
                                db.record_quote_revision(&QuoteRevisionRow {
                                    id: uuid::Uuid::new_v4().to_string(),
                                    market: market.clone(),
                                    side,
                                    queue_model: qm,
                                    previous_quote_id: Some(q.id.to_string()),
                                    new_quote_id: None,
                                    reason: reason.as_str().to_string(),
                                    previous_price: Some(q.price()),
                                    new_price: Some(dq.price),
                                    previous_instant_edge_bps: Some(q.desired.instant_edge_bps),
                                    new_instant_edge_bps: Some(dq.instant_edge_bps),
                                    event_ts: now,
                                })?;
                                trace!(
                                    "requote {} {:?} {:?} {} -> {} ({})",
                                    market.0, qm, side, q.price(), dq.price, reason.as_str(),
                                );
                                q.request_cancel(now, requote, reason);
                                st.dying.push(q);
                                let lq = LiveQuote::from_desired(market.clone(), dq, now, requote, qm, hidden_mult);
                                slot_set(st, side, Some(lq));
                                st.set_last_requote(side, now);
                            }
                            None => slot_set(st, side, Some(q)),
                        }
                    }
                }
                reject_clear(st, side);
            }
            Err(reason) => {
                if reject_changed(st, side, reason) {
                    debug!("reject {} {:?} {:?} {}", market.0, qm, side, reason.as_str());
                    db.record_opportunity(&OpportunityRow::rejected(market.clone(), side, qm, reason, now))?;
                }
                // The quote can no longer be re-derived under current conditions: cancel it
                // (still fillable until effective). Tag the cancel with the honest cause —
                // a stale/absent feed surfaces as FeedStale, matching the fill-time halt
                // below; everything else collapses to NoLongerProfitable.
                if let Some(mut q) = slot_take(st, side) {
                    if q.is_active() {
                        q.request_cancel(now, requote, ReplaceReason::from_reject(reason));
                        st.dying.push(q);
                    } else {
                        slot_set(st, side, Some(q));
                    }
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_trade(
    st: &mut MarketState,
    agg: &AsterAggTrade,
    edge: &EdgeConfig,
    requote: &RequoteConfig,
    min_notional: Decimal,
    strict: bool,
    buckets: &[i64],
    staleness_ms: i64,
    halt_on_stale: bool,
    pending: &mut BinaryHeap<Scheduled>,
    hedge_seq: &mut u64,
    db: &mut Db,
    now: DateTime<Utc>,
) -> Result<()> {
    // Only quotes on the side the taker hits can fill: a market sell (buyer_is_maker)
    // hits our bids; a market buy lifts our asks.
    let matched_side = if agg.buyer_is_maker { Side::Buy } else { Side::Sell };

    // Feed-staleness halt: if either book is stale we wouldn't trust the instant hedge
    // price, so a real maker requests a cancel on the matched side — the simulator
    // analogue of the live watchdog's TradingGate closing. CRITICAL: requesting a cancel
    // does NOT make the resting order vanish. On a real exchange it stays fillable
    // through the cancel round-trip, and a fill on a stale book is exactly the adverse
    // selection we must count — suppressing it makes the report optimistic. So we cancel
    // but FALL THROUGH to the fill loop (the quote, now PendingCancel in `dying`, remains
    // fillable until cancel_effective_at) and tag any resulting fill `feed_stale_at_fill`.
    // (Recompute formally re-prices/cancels on the next book event.)
    let mut feed_stale = false;
    if halt_on_stale {
        let aster_stale = st.aster_book.as_ref().is_none_or(|b| b.age_ms(now) > staleness_ms);
        let hl_stale = st.hl_book().is_none_or(|b| b.age_ms(now) > staleness_ms);
        if aster_stale || hl_stale {
            feed_stale = true;
            debug!(
                "feed stale (aster={} hl={}), cancelling {:?} {:?} on {} (still fillable until cancel effective)",
                aster_stale, hl_stale, st.queue_model, matched_side, st.spec.market_id.0,
            );
            if let Some(mut q) = slot_take(st, matched_side) {
                if q.is_active() {
                    q.request_cancel(now, requote, ReplaceReason::FeedStale);
                    st.dying.push(q);
                } else {
                    slot_set(st, matched_side, Some(q));
                }
            }
        }
    }

    // Gather our resting quotes on that side (the live slot + any dying quotes) and
    // share ONE taker residual across them, walked in price priority, so a single
    // sweep can never fill more than its size across our combined same-side orders.
    let mut fills: Vec<SimulatedAsterFill> = Vec::new();
    {
        let mut quotes: Vec<&mut LiveQuote> = Vec::new();
        let live = match matched_side {
            Side::Buy => st.live_bid.as_mut(),
            Side::Sell => st.live_ask.as_mut(),
        };
        if let Some(q) = live {
            quotes.push(q);
        }
        for q in st.dying.iter_mut() {
            if q.side() == matched_side {
                quotes.push(q);
            }
        }
        // The taker consumes the best price first: bids high->low, asks low->high.
        quotes.sort_by(|a, b| match matched_side {
            Side::Buy => b.price().cmp(&a.price()),
            Side::Sell => a.price().cmp(&b.price()),
        });
        let mut taker_remaining = agg.qty;
        for q in quotes {
            if taker_remaining <= Decimal::ZERO {
                break;
            }
            if let Some(mut f) = apply_print(q, agg, &mut taker_remaining) {
                f.feed_stale_at_fill = feed_stale;
                if q.remaining_qty <= Decimal::ZERO {
                    q.mark_filled();
                }
                fills.push(f);
            }
        }
    }
    for fill in &fills {
        on_fill(st, fill, edge, min_notional, strict, buckets, pending, hedge_seq, db, now)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn on_fill(
    st: &mut MarketState,
    fill: &SimulatedAsterFill,
    edge: &EdgeConfig,
    min_notional: Decimal,
    strict: bool,
    buckets: &[i64],
    pending: &mut BinaryHeap<Scheduled>,
    hedge_seq: &mut u64,
    db: &mut Db,
    now: DateTime<Utc>,
) -> Result<()> {
    let ref_px = mark_price(st).unwrap_or(fill.fill_px);

    // Capital accounting: every Aster maker fill moves the Aster maker-leg position.
    let fill_signed = SignedPosition::signed(fill.aster_side, fill.fill_qty);
    st.aster_pos.apply_fill(fill_signed, fill.fill_px);
    debug!(
        "fill {} {:?} {:?} {} @ {} (aster_pos -> {})",
        st.spec.market_id.0, st.queue_model, fill.aster_side, fill.fill_qty, fill.fill_px,
        st.aster_pos.qty,
    );

    let rules = HedgeabilityRules {
        hyperliquid_min_notional: min_notional,
        hyperliquid_qty_step: st.spec.hl_qty_step,
    };
    let outcome = handle_fill(fill, st.pending_inv.take(), &rules, ref_px, edge.aster_maker_fee_rate());
    st.pending_inv = outcome.pending;

    let market = st.spec.market_id.clone();
    let qm = st.queue_model;

    if let Some(n) = outcome.netted {
        let mut row = PendingEventRow::new(
            market.clone(), qm, "NETTED", n.closed_qty, n.open_px, n.closed_qty * ref_px, now,
        );
        row.realized_pnl = Some(n.realized_pnl);
        row.mark_px = Some(n.close_px);
        db.insert_pending_event(&row)?;
    }
    if let Some(notional) = outcome.accumulated_notional {
        if strict {
            // Strict mode: a sub-min partial that cannot be hedged immediately is a
            // violation — mark it to market instead of pretending it accumulated.
            if let Some(inv) = st.pending_inv.take() {
                let abs = inv.signed_qty.abs();
                let mtm = if inv.signed_qty > Decimal::ZERO {
                    abs * (ref_px - inv.avg_aster_px)
                } else {
                    abs * (inv.avg_aster_px - ref_px)
                };
                let mut row = PendingEventRow::new(
                    market.clone(), qm, "STRICT_FAILED", inv.signed_qty, inv.avg_aster_px, notional, now,
                );
                row.mark_px = Some(ref_px);
                row.realized_pnl = Some(mtm);
                row.reason = Some(RejectReason::StrictPartialHedgeabilityFailed.as_str().to_string());
                db.insert_pending_event(&row)?;
            }
        } else if let Some(inv) = &st.pending_inv {
            let mut row = PendingEventRow::new(
                market.clone(), qm, "ACCUMULATE", inv.signed_qty, inv.avg_aster_px, notional, now,
            );
            row.first_fill_ts = Some(inv.first_fill_ts);
            row.last_fill_ts = Some(inv.last_fill_ts);
            db.insert_pending_event(&row)?;
        }
    }
    if let Some(h) = outcome.hedge {
        // The hedge is dispatched now, moving the HL hedge-leg position (the latency
        // buckets only measure its fill price, not whether or when it happens).
        let hedge_signed = SignedPosition::signed(h.hedge_side, h.qty);
        st.hl_pos.apply_fill(hedge_signed, ref_px);
        debug!(
            "hedge {} {:?} {:?} {} @ ref {} ({} buckets, hl_pos -> {})",
            market.0, qm, h.hedge_side, h.qty, ref_px, buckets.len(), st.hl_pos.qty,
        );

        let mut row = PendingEventRow::new(
            market.clone(), qm, "HEDGED", h.qty, h.avg_aster_px, h.qty * ref_px, now,
        );
        row.reason = Some(h.hedge_side.as_str().to_string());
        db.insert_pending_event(&row)?;

        for &bucket in buckets {
            let resolve_at = fill.local_recv_ts + chrono::Duration::milliseconds(bucket);
            let ph = PendingHedge {
                id: uuid::Uuid::new_v4(),
                fill_id: fill.id,
                market: market.clone(),
                queue_model: qm,
                hedge_side: h.hedge_side,
                qty: h.qty,
                aster_ref_px: h.avg_aster_px,
                fill_local_ts: fill.local_recv_ts,
                resolve_at,
                latency_bucket_ms: bucket,
            };
            pending.push(Scheduled { resolve_at, seq: *hedge_seq, hedge: ph });
            *hedge_seq += 1;
        }
    }

    // Persist the fill with the post-fill leg positions; track peak notionals.
    let aster_pos_notional = st.aster_pos.qty * ref_px;
    let hl_pos_notional = st.hl_pos.qty * ref_px;
    st.max_abs_aster_notional = st.max_abs_aster_notional.max(aster_pos_notional.abs());
    st.max_abs_hl_notional = st.max_abs_hl_notional.max(hl_pos_notional.abs());
    let mut frow = FillRow::from_fill(fill, qm);
    frow.aster_pos_notional = Some(aster_pos_notional);
    frow.hl_pos_notional = Some(hl_pos_notional);
    db.insert_fill(&frow)?;

    Ok(())
}

fn resolve_due_hedges(
    states: &HashMap<(MarketId, QueueModel), MarketState>,
    pending: &mut BinaryHeap<Scheduled>,
    edge: &EdgeConfig,
    staleness_ms: i64,
    now: DateTime<Utc>,
    db: &mut Db,
) -> Result<()> {
    while pending.peek().is_some_and(|s| s.resolve_at <= now) {
        let s = pending.pop().unwrap();
        let key = (s.hedge.market.clone(), s.hedge.queue_model);
        let book = states
            .get(&key)
            .and_then(|st| crate::sim::clock::last_hl_book_at_or_before(&st.hl_book_ring, s.hedge.resolve_at));
        match book {
            Some(book) => {
                let stale = (s.hedge.resolve_at - book.local_recv_ts).num_milliseconds() > staleness_ms;
                let res = resolve_hedge(&s.hedge, book, edge, stale);
                db.insert_hedge(&HedgeRow::from_result(&res))?;
            }
            // No HL book at/before resolve time: never drop the hedge — record it as
            // unbooked (filled_qty = 0) so the unhedged exposure remains visible.
            None => db.insert_hedge(&HedgeRow::unbooked(&s.hedge, RejectReason::MissingHlBook.as_str()))?,
        }
    }
    Ok(())
}

fn check_state_pending(
    st: &mut MarketState,
    max_notional: Decimal,
    max_age_ms: i64,
    now: DateTime<Utc>,
    db: &mut Db,
) -> Result<()> {
    let mark = match mark_price(st) {
        Some(m) => m,
        None => return Ok(()),
    };
    let event = st
        .pending_inv
        .as_ref()
        .and_then(|inv| check_pending_limits(inv, max_notional, max_age_ms, mark, now));
    if let Some(e) = event {
        let mut row = PendingEventRow::new(
            st.spec.market_id.clone(), st.queue_model, e.kind.as_str(),
            e.signed_qty, e.avg_aster_px, e.notional, now,
        );
        row.mark_px = Some(e.mark_px);
        row.realized_pnl = Some(e.mark_to_market_pnl);
        db.insert_pending_event(&row)?;
        // Marked to market: clear so it does not re-fire every event.
        st.pending_inv = None;
    }
    Ok(())
}

// --- side-indexed slot helpers (short, scoped borrows) ---

fn slot_take(st: &mut MarketState, side: Side) -> Option<LiveQuote> {
    match side {
        Side::Buy => st.live_bid.take(),
        Side::Sell => st.live_ask.take(),
    }
}

fn slot_set(st: &mut MarketState, side: Side, q: Option<LiveQuote>) {
    match side {
        Side::Buy => st.live_bid = q,
        Side::Sell => st.live_ask = q,
    }
}

fn reject_changed(st: &mut MarketState, side: Side, reason: crate::types::RejectReason) -> bool {
    let last = match side {
        Side::Buy => &mut st.last_reject_bid,
        Side::Sell => &mut st.last_reject_ask,
    };
    if *last != Some(reason) {
        *last = Some(reason);
        true
    } else {
        false
    }
}

fn reject_clear(st: &mut MarketState, side: Side) {
    match side {
        Side::Buy => st.last_reject_bid = None,
        Side::Sell => st.last_reject_ask = None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quote_engine::{AsterEffectiveTouchSource, DesiredQuote};
    use rust_decimal_macros::dec;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn spec() -> MarketSpec {
        MarketSpec {
            market_id: "BTC".into(),
            aster_symbol: "BTCUSDT".into(),
            hl_coin: "BTC".into(),
            lighter_market_id: 1,
            lighter_price_decimals: 1,
            lighter_size_decimals: 5,
            lighter_price_tick: dec!(0.1),
            tick: dec!(0.1),
            step: dec!(0.001),
            aster_min_qty: dec!(0.001),
            aster_min_notional: dec!(5),
            hl_sz_decimals: 5,
            hl_qty_step: dec!(0.00001),
            hl_min_notional: dec!(10),
        }
    }

    fn rq() -> RequoteConfig {
        RequoteConfig {
            simulated_aster_place_latency_ms: 25,
            simulated_aster_cancel_latency_ms: 25,
            quote_ttl_ms: 5_000,
        }
    }

    fn desired(side: Side, price: Decimal) -> DesiredQuote {
        DesiredQuote {
            aster_side: side,
            price,
            qty: dec!(1),
            hedge_side: side.opposite(),
            expected_hl_vwap: price,
            expected_hl_depth_filled_qty: dec!(1),
            expected_hl_slippage_bps: dec!(0),
            expected_hl_worst_px: price,
            expected_hl_depth_levels_used: 1,
            instant_edge_bps: dec!(3),
            profitable_bound_px: price,
            post_only_constraint_px: price,
            required_bps: dec!(7.5),
            ref_px: price,
            aster_mid: price,
            hl_mid: price,
            better_levels_qty: dec!(0),
            queue_ahead_qty: dec!(0),
            distance_from_touch_bps: dec!(0),
            effective_aster_touch_px: price,
            effective_aster_touch_source: AsterEffectiveTouchSource::Depth,
            depth_liquidity_multiple: dec!(1),
            depth_target_qty: dec!(1),
            aster_depth_filled_qty: dec!(1),
            aster_depth_levels_used: 1,
            size_clamped_up: false,
            queue_truncated: false,
        }
    }

    /// Finding 3: a just-activated post-only quote that would cross the book is
    /// cancelled (as before) AND now recorded as a POST_ONLY_REJECTED_ON_PLACEMENT
    /// reject, so the report can show how often this happens (previously silent).
    #[test]
    fn gtx_reject_on_placement_is_recorded() {
        for side in [Side::Buy, Side::Sell] {
            let dir =
                std::env::temp_dir().join(format!("xemm_gtx_{}.sqlite", uuid::Uuid::new_v4()));
            let mut db = Db::open(&dir).unwrap();
            db.insert_run("r", ts(), "replay", None, "t", "{}").unwrap();

            let mut st = MarketState::new(spec(), QueueModel::Optimistic);
            // best_bid 100.0 / best_ask 100.1
            st.aster_book = Some(OrderBook::from_levels(
                [(dec!(100.0), dec!(10))],
                [(dec!(100.1), dec!(10))],
                ts(),
                ts(),
            ));

            // Buy at 100.2 (>= best_ask) or Sell at 99.9 (<= best_bid) crosses on activation.
            let price = match side {
                Side::Buy => dec!(100.2),
                Side::Sell => dec!(99.9),
            };
            let q = LiveQuote::from_desired(
                "BTC".into(),
                desired(side, price),
                ts(),
                &rq(),
                QueueModel::Optimistic,
                dec!(1),
            );
            match side {
                Side::Buy => st.live_bid = Some(q),
                Side::Sell => st.live_ask = Some(q),
            }

            // Advance past the 25ms placement latency: PendingPlacement -> Live, then cross.
            let now = ts() + chrono::Duration::milliseconds(30);
            advance_and_gc(&mut st, now, &mut db).unwrap();

            assert!(
                st.live_bid.is_none() && st.live_ask.is_none(),
                "side={side:?}: crossing quote should be cancelled"
            );
            db.flush().unwrap();
            assert_eq!(
                db.count("opportunity_rejects").unwrap(),
                1,
                "side={side:?}: exactly one GTX reject recorded"
            );
            std::fs::remove_file(&dir).ok();
        }
    }
}
