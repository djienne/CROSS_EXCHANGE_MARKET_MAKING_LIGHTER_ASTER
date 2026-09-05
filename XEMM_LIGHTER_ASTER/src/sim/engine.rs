//! Chronological execution of independent market/queue/latency scenarios.
//! Market observations are shared; fills, capital, reservations and ledgers are not.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use anyhow::{bail, Result};
use chrono::{DateTime, Duration, Utc};
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
use crate::quote_engine::{compute_desired_quote, resting_quote_net_edge_bps, PositionContext, QuoteEngineConfig};
use crate::requoter::{LiveQuote, LiveQuoteState, ReplaceReason, RequoteConfig};
use crate::store::db::{FillRow, HedgeRow, OpportunityRow, PendingEventRow, QuoteRevisionRow, ScenarioResultRow};
use crate::store::Db;
use crate::types::{MarketId, QueueModel, RejectReason, Side};
use tracing::{debug, trace};

type ScenarioKey = (MarketId, QueueModel, i64);
enum Action { Hedge(PendingHedge), Wake(ScenarioKey, u64) }
struct Scheduled { at: DateTime<Utc>, seq: u64, action: Action }
impl PartialEq for Scheduled { fn eq(&self, o: &Self) -> bool { self.at == o.at && self.seq == o.seq } }
impl Eq for Scheduled {}
impl Ord for Scheduled {
    fn cmp(&self, o: &Self) -> Ordering { o.at.cmp(&self.at).then_with(|| o.seq.cmp(&self.seq)) }
}
impl PartialOrd for Scheduled { fn partial_cmp(&self, o: &Self) -> Option<Ordering> { Some(self.cmp(o)) } }
fn push_action(heap: &mut BinaryHeap<Scheduled>, seq: &mut u64, at: DateTime<Utc>, action: Action) {
    heap.push(Scheduled { at, seq: *seq, action });
    *seq += 1;
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
    staleness_ms: i64,
    halt_on_stale: bool,
    aster_cap_notional: Decimal,
    hl_cap_notional: Decimal,
    enforce_cap: bool,
    keys: Vec<ScenarioKey>,
    states: HashMap<ScenarioKey, MarketState>,
    pending: BinaryHeap<Scheduled>,
    action_seq: u64,
    last_event: Option<(DateTime<Utc>, u64)>,
    finished: bool,
}

impl SimEngine {
    pub fn new(cfg: Config, specs: Vec<MarketSpec>) -> Result<Self> {
        cfg.validate()?;
        let models = cfg.queue_model.parsed_models()?;
        let mut buckets = cfg.simulation.hedge_latency_buckets_ms.clone();
        buckets.sort_unstable();
        buckets.dedup();
        let mut keys = Vec::new();
        let mut states = HashMap::new();
        for spec in &specs {
            for &model in &models {
                for &latency in &buckets {
                    let key = (spec.market_id.clone(), model, latency);
                    states.insert(key.clone(), MarketState::new(spec.clone(), model, latency));
                    keys.push(key);
                }
            }
        }
        Ok(Self {
            edge: cfg.edge.clone(), quote: cfg.quote.clone(), requote: cfg.simulation.requote_config(),
            partials_min_notional: cfg.partials.hyperliquid_min_notional,
            strict_partials: cfg.partials.strict_all_partials_must_be_hedgeable,
            max_pending_notional: cfg.partials.max_pending_inventory_notional,
            max_pending_age_ms: cfg.partials.max_pending_inventory_age_ms,
            hidden_mult: cfg.queue_model.hidden_queue_multiplier,
            staleness_ms: cfg.simulation.max_book_staleness_ms,
            halt_on_stale: cfg.simulation.halt_trading_on_stale_feed,
            aster_cap_notional: cfg.capital.aster_cap_notional(), hl_cap_notional: cfg.capital.hyperliquid_cap_notional(),
            enforce_cap: cfg.capital.enforce_position_cap, keys, states,
            pending: BinaryHeap::new(), action_seq: 0, last_event: None, finished: false,
        })
    }

    pub fn on_event(&mut self, ev: &Event, db: &mut Db) -> Result<()> {
        if self.finished { bail!("simulation already finalized"); }
        let now = ev.local_recv_ts;
        if self.last_event.is_some_and(|previous| (now, ev.seq) < previous) {
            bail!("simulation events must be ordered by (local_recv_ts, seq)");
        }
        self.advance_to(now, db)?;
        let keys: Vec<_> = self.keys.iter().filter(|k| k.0 == ev.market).cloned().collect();
        let observed = match &ev.kind {
            EventKind::HlL2Book { bids, asks, exch_ts } | EventKind::AsterDepth { bids, asks, exch_ts } =>
                Some(Arc::new(OrderBook::from_levels(bids.clone(), asks.clone(), *exch_ts, now))),
            _ => None,
        };
        for key in keys {
            let st = self.states.get_mut(&key).unwrap();
            match &ev.kind {
                EventKind::HlL2Book { .. } => {
                    st.hl_observation = observed.clone();
                    st.hl_execution_book = observed.as_deref().cloned();
                }
                EventKind::AsterDepth { .. } => st.aster_book = observed.clone(),
                EventKind::AsterAggTrade { price, qty, buyer_is_maker, exch_ts } => {
                    if *price <= Decimal::ZERO || *qty < Decimal::ZERO { bail!("invalid Aster trade print"); }
                    let agg = AsterAggTrade { market: ev.market.clone(), price: *price, qty: *qty,
                        buyer_is_maker: *buyer_is_maker, exch_ts: *exch_ts, local_recv_ts: now };
                    apply_trade(st, &agg, &self.edge, &self.requote, self.partials_min_notional,
                        self.strict_partials, self.staleness_ms, self.halt_on_stale,
                        &mut self.pending, &mut self.action_seq, db, now)?;
                }
                EventKind::HlTrade { .. } => {}
            }
            self.refresh_scenario(&key, now, db)?;
        }
        // Zero-delay arrivals and activations follow their cause, before the next input.
        self.advance_to(now, db)?;
        self.last_event = Some((now, ev.seq));
        Ok(())
    }

    fn refresh_scenario(&mut self, key: &ScenarioKey, now: DateTime<Utc>, db: &mut Db) -> Result<()> {
        let st = self.states.get_mut(key).unwrap();
        advance_and_gc(st, now, db)?;
        check_state_pending(st, self.max_pending_notional, self.max_pending_age_ms, now, db)?;
        if st.frozen.is_some() {
            cancel_scenario(st, now, &self.requote);
        } else {
            recompute_quotes(st, &self.edge, &self.quote, &self.requote, self.hidden_mult, self.staleness_ms,
                self.aster_cap_notional, self.hl_cap_notional, self.enforce_cap, now, db)?;
        }
        update_peaks(st);
        self.schedule_wake(key, now);
        Ok(())
    }

    fn schedule_wake(&mut self, key: &ScenarioKey, now: DateTime<Utc>) {
        let st = self.states.get_mut(key).unwrap();
        let mut deadlines = Vec::new();
        for q in st.live_bid.iter().chain(st.live_ask.iter()).chain(st.dying.iter()) {
            match q.state {
                LiveQuoteState::PendingPlacement => deadlines.push(q.active_at),
                LiveQuoteState::Live => deadlines.push(q.expires_at + Duration::nanoseconds(1)),
                LiveQuoteState::PendingCancel => {
                    if let Some(at) = q.cancel_effective_at {
                        deadlines.push(at);
                        if !q.was_live && q.active_at < at { deadlines.push(q.active_at); }
                    }
                    deadlines.push(q.expires_at + Duration::nanoseconds(1));
                }
                _ => {}
            }
        }
        if st.frozen.is_none() {
            if let Some(inv) = &st.pending_inv {
                deadlines.push(inv.first_fill_ts + Duration::milliseconds(self.max_pending_age_ms + 1));
            }
            if st.live_bid.is_some() || st.live_ask.is_some() {
                for b in st.aster_book.iter().chain(st.hl_observation.iter()) {
                    deadlines.push(b.local_recv_ts + Duration::milliseconds(self.staleness_ms + 1));
                    deadlines.push(b.exch_ts + Duration::milliseconds(self.staleness_ms + 1));
                }
            }
        }
        let next = deadlines.into_iter().filter(|at| *at >= now).min();
        if next != st.timer_at {
            st.timer_generation += 1;
            st.timer_at = next;
            if let Some(at) = next {
                push_action(&mut self.pending, &mut self.action_seq, at, Action::Wake(key.clone(), st.timer_generation));
            }
        }
    }

    fn advance_to(&mut self, now: DateTime<Utc>, db: &mut Db) -> Result<()> {
        while self.pending.peek().is_some_and(|s| s.at <= now) {
            let scheduled = self.pending.pop().unwrap();
            let key = match scheduled.action {
                Action::Wake(key, generation) => {
                    let st = self.states.get_mut(&key).unwrap();
                    if generation != st.timer_generation { continue; }
                    st.timer_at = None;
                    key
                }
                Action::Hedge(ph) => {
                    let key = (ph.market.clone(), ph.queue_model, ph.latency_bucket_ms);
                    resolve_arrival(self.states.get_mut(&key).unwrap(), ph, &self.edge, self.staleness_ms, db)?;
                    key
                }
            };
            self.refresh_scenario(&key, scheduled.at, db)?;
        }
        Ok(())
    }

    pub fn finalize(&mut self, end_ts: DateTime<Utc>, db: &mut Db) -> Result<()> {
        if self.finished { bail!("simulation already finalized"); }
        if self.last_event.is_some_and(|(at, _)| end_ts < at) { bail!("observation end precedes its last event"); }
        self.advance_to(end_ts, db)?;
        while let Some(s) = self.pending.pop() {
            if let Action::Hedge(ph) = s.action {
                db.insert_hedge(&HedgeRow::unbooked(&ph, "AFTER_OBSERVATION_END"))?;
            }
        }
        for key in &self.keys {
            let st = &self.states[key];
            let marked = |position: SignedPosition, book: Option<&OrderBook>| -> Option<Decimal> {
                if position.qty == Decimal::ZERO { return Some(Decimal::ZERO); }
                let b = book.filter(|b| book_usable(b, end_ts, self.staleness_ms))?;
                Some(position.qty * (b.mid()? - position.avg_px))
            };
            let unrealized = marked(st.aster_pos, st.aster_book.as_deref())
                .zip(marked(st.hl_pos, st.hl_book())).map(|(a,h)| a+h);
            let gross = st.aster_realized_gross + st.hl_realized_gross;
            let fees = st.aster_fees + st.hl_fees;
            let net = unrealized.filter(|_| st.unpriced_hedge_qty == Decimal::ZERO).map(|u| gross-fees+u);
            db.insert_scenario_result(&ScenarioResultRow {
                market: key.0.clone(), queue_model: key.1, latency_bucket_ms: key.2,
                aster_qty: st.aster_pos.qty, lighter_qty: st.hl_pos.qty,
                realized_gross: gross, fees, unrealized_pnl: unrealized, net_pnl: net,
                reserved_hedge_qty: st.reserved_hl_buys + st.reserved_hl_sells,
                unpriced_hedge_qty: st.unpriced_hedge_qty,
                peak_aster_notional: st.max_abs_aster_notional, peak_lighter_notional: st.max_abs_hl_notional,
                frozen_reason: st.frozen,
            })?;
        }
        self.finished = true;
        db.finish_run(end_ts)?;
        db.flush()?;
        Ok(())
    }
}

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
    let ask = st.aster_book.as_ref().and_then(|b|b.best_ask()).map(|l|l.px);
    let bid = st.aster_book.as_ref().and_then(|b|b.best_bid()).map(|l|l.px);
    let mut rejected = Vec::new();
    for q in st.live_bid.iter_mut().chain(st.live_ask.iter_mut()).chain(st.dying.iter_mut()) {
        let was_live = q.was_live;
        q.advance_state(now);
        if !was_live && q.was_live && !q.is_terminal() {
            let crosses = match q.side() {
                Side::Buy => ask.is_some_and(|px|q.price()>=px),
                Side::Sell => bid.is_some_and(|px|q.price()<=px),
            };
            if crosses {
                q.state = LiveQuoteState::Cancelled;
                rejected.push(q.side());
            }
        }
    }
    if st.live_bid.as_ref().is_some_and(|q|q.is_terminal()) { st.live_bid = None; }
    if st.live_ask.as_ref().is_some_and(|q|q.is_terminal()) { st.live_ask = None; }
    st.dying.retain(|q|!q.is_terminal());
    for side in rejected {
        db.record_opportunity(&OpportunityRow::rejected(st.spec.market_id.clone(),side,st.queue_model,
            st.latency_bucket_ms,RejectReason::PostOnlyRejectedOnPlacement,now))?;
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
    let mut pos = PositionContext {
        aster_pos_qty: st.aster_pos.qty,
        hl_pos_qty: st.hl_pos.qty,
        aster_cap_notional,
        hl_cap_notional,
        enforce: enforce_cap,
        reduce_position_only: false,
    };

    for side in [Side::Buy, Side::Sell] {
        let dying_qty: Decimal = st.dying.iter().filter(|q| q.side() == side && !q.is_terminal()).map(|q| q.remaining_qty).sum();
        let pending = st.pending_inv.as_ref().map(|p| p.signed_qty).unwrap_or_default();
        pos.hl_pos_qty = st.hl_pos.qty + match side {
            Side::Buy => -st.reserved_hl_sells - pending.max(Decimal::ZERO) - dying_qty,
            Side::Sell => st.reserved_hl_buys + (-pending).max(Decimal::ZERO) + dying_qty,
        };
        pos.aster_pos_qty = st.aster_pos.qty + SignedPosition::signed(side, dying_qty);
        let res = compute_desired_quote(
            edge, quote, &aster, &hl, side, spec.tick, spec.step, spec.aster_min_qty,
            spec.aster_min_notional, spec.hl_min_notional, staleness_ms, now, &pos,
        );
        match res {
            Ok(dq) => {
                let cur = slot_take(st, side);
                match cur {
                    None => {
                        db.record_opportunity(&OpportunityRow::accepted(market.clone(), qm, st.latency_bucket_ms, &dq, edge, now))?;
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
                                // The previous quote may fill before cancellation. Reserve its
                                // maker quantity and future hedge before admitting a replacement.
                                let mut replacement_pos = pos.clone();
                                replacement_pos.aster_pos_qty += SignedPosition::signed(side,q.remaining_qty);
                                replacement_pos.hl_pos_qty += SignedPosition::signed(side.opposite(),q.remaining_qty);
                                let replacement = compute_desired_quote(edge,quote,&aster,&hl,side,spec.tick,spec.step,
                                    spec.aster_min_qty,spec.aster_min_notional,spec.hl_min_notional,staleness_ms,now,&replacement_pos);
                                db.record_quote_revision(&QuoteRevisionRow {
                                    id: uuid::Uuid::new_v4().to_string(),
                                    market: market.clone(),
                                    side,
                                    queue_model: qm,
                                    latency_bucket_ms: st.latency_bucket_ms,
                                    previous_quote_id: Some(q.id.to_string()),
                                    new_quote_id: None,
                                    reason: reason.as_str().to_string(),
                                    previous_price: Some(q.price()),
                                    new_price: replacement.as_ref().ok().map(|d|d.price),
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
                                if let Ok(dq) = replacement {
                                    db.record_opportunity(&OpportunityRow::accepted(market.clone(),qm,st.latency_bucket_ms,&dq,edge,now))?;
                                    let lq = LiveQuote::from_desired(market.clone(), dq, now, requote, qm, hidden_mult);
                                    slot_set(st, side, Some(lq));
                                    st.set_last_requote(side, now);
                                }
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
                    db.record_opportunity(&OpportunityRow::rejected(market.clone(), side, qm, st.latency_bucket_ms, reason, now))?;
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
        on_fill(st, fill, edge, min_notional, strict, pending, hedge_seq, db, now)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn on_fill(
    st: &mut MarketState, fill: &SimulatedAsterFill, edge: &EdgeConfig,
    min_notional: Decimal, strict: bool, pending: &mut BinaryHeap<Scheduled>,
    action_seq: &mut u64, db: &mut Db, now: DateTime<Utc>,
) -> Result<()> {
    let ref_px = mark_price(st).unwrap_or(fill.fill_px);
    let delta = SignedPosition::signed(fill.aster_side, fill.fill_qty);
    st.aster_realized_gross += st.aster_pos.apply_fill(delta, fill.fill_px);
    st.aster_fees += fill.fill_qty * fill.fill_px * edge.aster_maker_fee_rate();
    let rules = HedgeabilityRules {
        hyperliquid_min_notional: min_notional,
        hyperliquid_qty_step: st.spec.hl_qty_step,
    };
    let outcome = handle_fill(fill, st.pending_inv.take(), &rules, ref_px, edge.aster_maker_fee_rate());
    st.pending_inv = outcome.pending;
    if let Some(n) = outcome.netted {
        let mut row = PendingEventRow::new(st.spec.market_id.clone(), st.queue_model,
            st.latency_bucket_ms, "NETTED", n.closed_qty, n.open_px, n.closed_qty * ref_px, now);
        row.realized_pnl = Some(n.realized_pnl);
        row.mark_px = Some(n.close_px);
        db.insert_pending_event(&row)?;
    }
    if let Some(notional) = outcome.accumulated_notional {
        let inv = st.pending_inv.as_ref().expect("accumulation has a residual");
        let mut row = PendingEventRow::new(st.spec.market_id.clone(), st.queue_model,
            st.latency_bucket_ms, if strict { "STRICT_FAILED" } else { "ACCUMULATE" },
            inv.signed_qty, inv.avg_aster_px, notional, now);
        row.first_fill_ts = Some(inv.first_fill_ts);
        row.last_fill_ts = Some(inv.last_fill_ts);
        db.insert_pending_event(&row)?;
        if strict { st.frozen = Some("STRICT_PARTIAL"); }
    }
    if let Some(h) = outcome.hedge {
        let qty = crate::decimal::floor_to_step(h.qty, st.spec.hl_qty_step);
        if qty > Decimal::ZERO {
            match h.hedge_side {
                Side::Buy => st.reserved_hl_buys += qty,
                Side::Sell => st.reserved_hl_sells += qty,
            }
            let ph = PendingHedge {
                id: uuid::Uuid::new_v4(), fill_id: fill.id,
                market: st.spec.market_id.clone(), queue_model: st.queue_model,
                hedge_side: h.hedge_side, qty, aster_ref_px: h.avg_aster_px,
                fill_local_ts: now,
                resolve_at: now + Duration::milliseconds(st.latency_bucket_ms),
                latency_bucket_ms: st.latency_bucket_ms,
            };
            push_action(pending, action_seq, ph.resolve_at, Action::Hedge(ph));
        }
        if qty < h.qty { st.frozen = Some("HEDGE_QUANTIZATION_RESIDUAL"); }
    }
    update_peaks(st);
    let mut row = FillRow::from_fill(fill, st.queue_model, st.latency_bucket_ms);
    row.aster_pos_notional = Some(st.aster_pos.qty * ref_px);
    row.hl_pos_notional = Some(st.hl_pos.qty * ref_px);
    db.insert_fill(&row)?;
    Ok(())
}

fn book_usable(book: &OrderBook, now: DateTime<Utc>, max_age_ms: i64) -> bool {
    let receive_age = (now - book.local_recv_ts).num_milliseconds();
    let source_age = (now - book.exch_ts).num_milliseconds();
    !book.is_crossed() && book.mid().is_some() && receive_age >= 0
        && receive_age <= max_age_ms && source_age >= -1000 && source_age <= max_age_ms
}

fn resolve_arrival(st: &mut MarketState, ph: PendingHedge, edge: &EdgeConfig,
    staleness_ms: i64, db: &mut Db) -> Result<()> {
    let usable = st.hl_book().is_some_and(|b| book_usable(b, ph.resolve_at, staleness_ms));
    if !usable {
        st.unpriced_hedge_qty += ph.qty;
        st.frozen = Some("UNPRICED_HEDGE");
        db.insert_hedge(&HedgeRow::unbooked(&ph, "UNPRICED_HEDGE"))?;
        return Ok(()); // execution is unobserved; keep the reservation, never invent a zero-fill proof
    }
    let book = st.hl_execution_book.as_mut().unwrap();
    let mut result = resolve_hedge(&ph, book, edge, false);
    // An IOC cannot execute a fractional venue lot, including at exhausted depth.
    let filled = crate::decimal::floor_to_step(result.filled_qty, st.spec.hl_qty_step);
    if filled != result.filled_qty && filled > Decimal::ZERO {
        let mut quantized = ph.clone();
        quantized.qty = filled;
        result = resolve_hedge(&quantized, book, edge, false);
        result.qty = ph.qty;
        result.depth_exhausted = true;
    } else if filled == Decimal::ZERO {
        result.filled_qty = Decimal::ZERO;
        result.gross_pnl = Decimal::ZERO;
        result.aster_fee = Decimal::ZERO;
        result.hl_fee = Decimal::ZERO;
        result.net_pnl = Decimal::ZERO;
        result.realized_edge_bps = Decimal::ZERO;
        result.depth_exhausted = ph.qty > Decimal::ZERO;
    }
    let mut remaining = result.filled_qty;
    let levels = match ph.hedge_side { Side::Buy => &mut book.asks, Side::Sell => &mut book.bids };
    for level in levels.iter_mut() {
        let taken = remaining.min(level.qty);
        level.qty -= taken;
        remaining -= taken;
        if remaining <= Decimal::ZERO { break; }
    }
    levels.retain(|l| l.qty > Decimal::ZERO);
    match ph.hedge_side {
        Side::Buy => st.reserved_hl_buys -= ph.qty,
        Side::Sell => st.reserved_hl_sells -= ph.qty,
    }
    st.hl_realized_gross += st.hl_pos.apply_fill(
        SignedPosition::signed(ph.hedge_side, result.filled_qty), result.hl_vwap);
    st.hl_fees += result.hl_fee;
    if result.filled_qty < ph.qty { st.frozen = Some("PARTIAL_HEDGE"); }
    update_peaks(st);
    db.insert_hedge(&HedgeRow::from_result(&result))?;
    Ok(())
}

fn update_peaks(st: &mut MarketState) {
    if let Some(px) = st.aster_book.as_ref().and_then(|b| b.mid()) {
        st.max_abs_aster_notional = st.max_abs_aster_notional.max(st.aster_pos.notional(px));
    }
    if let Some(px) = st.hl_book().and_then(|b| b.mid()) {
        st.max_abs_hl_notional = st.max_abs_hl_notional.max(st.hl_pos.notional(px));
    }
}

fn check_state_pending(st: &mut MarketState, max_notional: Decimal, max_age_ms: i64,
    now: DateTime<Utc>, db: &mut Db) -> Result<()> {
    if st.frozen.is_some() { return Ok(()); }
    let Some(mark) = mark_price(st) else { return Ok(()); };
    if let Some(e) = st.pending_inv.as_ref().and_then(|inv| check_pending_limits(inv, max_notional, max_age_ms, mark, now)) {
        let mut row = PendingEventRow::new(st.spec.market_id.clone(), st.queue_model,
            st.latency_bucket_ms, e.kind.as_str(), e.signed_qty, e.avg_aster_px, e.notional, now);
        row.mark_px = Some(e.mark_px);
        row.reason = Some("UNREALIZED_RESIDUAL_RETAINED".into());
        db.insert_pending_event(&row)?;
        st.frozen = Some(e.kind.as_str());
    }
    Ok(())
}

fn cancel_scenario(st: &mut MarketState, now: DateTime<Utc>, cfg: &RequoteConfig) {
    for side in [Side::Buy, Side::Sell] {
        if let Some(mut q) = slot_take(st, side) {
            q.request_cancel(now, cfg, ReplaceReason::NoLongerProfitable);
            if !q.is_terminal() { st.dying.push(q); }
        }
    }
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

    fn config(latencies: &[i64]) -> Config {
        let mut c: Config = toml::from_str(include_str!("../../config-paper-lighter.toml")).unwrap();
        c.live.enabled = false;
        c.edge.min_net_profit_bps = Decimal::ZERO;
        c.edge.slippage_buffer_bps = Decimal::ZERO;
        c.edge.latency_buffer_bps = Decimal::ZERO;
        c.edge.basis_buffer_bps = Decimal::ZERO;
        c.edge.funding_buffer_bps = Decimal::ZERO;
        c.edge.aster_maker_fee_bps = Decimal::ZERO;
        c.edge.taker_fee_bps = Decimal::ZERO;
        c.quote.desired_notional = dec!(100.5);
        c.quote.depth_liquidity_multiple = Decimal::ONE;
        c.quote.max_quote_distance_bps = dec!(500);
        c.quote.min_aster_touch_distance_bps = Decimal::ZERO;
        c.quote.min_aster_touch_hysteresis_bps = Decimal::ZERO;
        c.quote.clamp_to_min_lot = false;
        c.simulation.simulated_aster_place_latency_ms = 25;
        c.simulation.simulated_aster_cancel_latency_ms = 25;
        c.simulation.quote_ttl_ms = 500;
        c.simulation.max_book_staleness_ms = 1000;
        c.simulation.hedge_latency_buckets_ms = latencies.to_vec();
        c.partials.strict_all_partials_must_be_hedgeable = false;
        c.partials.hyperliquid_min_notional = dec!(10);
        c.partials.max_pending_inventory_notional = dec!(25);
        c.partials.max_pending_inventory_age_ms = 50;
        c.queue_model.models = vec!["optimistic".into()];
        c.capital.aster_capital_usd = dec!(10000);
        c.capital.hyperliquid_capital_usd = dec!(10000);
        c
    }

    fn book_event(seq: u64, ms: i64, aster: bool, bid: Decimal, ask: Decimal, bid_qty: Decimal) -> Event {
        let at = ts() + Duration::milliseconds(ms);
        let bids = vec![(bid,bid_qty)];
        let asks = vec![(ask,dec!(10))];
        Event { seq, local_recv_ts: at, market: "BTC".into(), kind: if aster {
            EventKind::AsterDepth { bids, asks, exch_ts: at }
        } else { EventKind::HlL2Book { bids, asks, exch_ts: at } } }
    }

    fn tape(fill_qty: Decimal) -> Vec<Event> {
        vec![book_event(1,0,true,dec!(99),dec!(101),dec!(10)),
            book_event(2,0,false,dec!(100),dec!(100.2),dec!(10)),
            Event { seq: 3, local_recv_ts: ts()+Duration::milliseconds(30), market: "BTC".into(),
                kind: EventKind::AsterAggTrade { price: dec!(99), qty: fill_qty,
                    buyer_is_maker: true, exch_ts: ts()+Duration::milliseconds(30) } }]
    }

    fn simulate(c: Config, events: &[Event], end_ms: i64) -> crate::report::ReportSummary {
        let dir = std::env::temp_dir().join(format!("xemm_scenarios_{}",uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("results.sqlite");
        let mut db = Db::open(&path).unwrap();
        db.insert_run("scenario-test",ts(),"replay",None,"test",&serde_json::to_string(&c).unwrap()).unwrap();
        let mut instrument = spec();
        instrument.step = dec!(0.01);
        db.insert_market(&instrument).unwrap();
        let mut engine = SimEngine::new(c,vec![instrument]).unwrap();
        for event in events { engine.on_event(event,&mut db).unwrap(); }
        engine.finalize(ts()+Duration::milliseconds(end_ms),&mut db).unwrap();
        let report = crate::report::generate(&path,Some("scenario-test".into()),&dir).unwrap();
        assert!(dir.join("report.csv").is_file());
        drop(db);
        for name in ["results.sqlite","results.sqlite-wal","results.sqlite-shm","report.json","report.csv"] {
            let _ = std::fs::remove_file(dir.join(name));
        }
        std::fs::remove_dir(&dir).unwrap();
        report
    }

    fn bucket(report: &crate::report::ReportSummary, latency: i64) -> &crate::report::BucketReport {
        report.markets[0].models[0].buckets.iter().find(|b| b.latency_bucket_ms == latency).unwrap()
    }

    #[test]
    fn independent_latencies_have_actual_execution_ledgers_and_are_isolated() {
        let mut events = tape(dec!(1));
        events.push(book_event(4,40,true,dec!(97),dec!(99),dec!(10)));
        events.push(book_event(5,40,false,dec!(98),dec!(98.2),dec!(10)));
        let report = simulate(config(&[0,100]),&events,200);
        let augmented = simulate(config(&[100,50,0,100]),&events,200);
        for (latency, expected) in [(0,dec!(-0.1)),(100,dec!(-2.1))] {
            let b = bucket(&report,latency);
            assert_eq!(b.fills,1);
            assert_eq!(b.aster_qty,Some(dec!(1)));
            assert_eq!(b.lighter_qty,Some(dec!(-1)));
            assert_eq!(b.total_net_pnl,Some(expected));
            assert_eq!(b.total_net_pnl,bucket(&augmented,latency).total_net_pnl);
            assert_eq!(b.fills,bucket(&augmented,latency).fills);
        }
        assert_eq!(augmented.markets[0].models[0].buckets.len(),3);
    }

    #[test]
    fn future_arrival_is_censored_and_stale_marks_are_not_zero_loss() {
        let report = simulate(config(&[100]),&tape(dec!(1)),50);
        let b = bucket(&report,100);
        assert_eq!(b.aster_qty,Some(dec!(1)));
        assert_eq!(b.lighter_qty,Some(dec!(0)));
        assert_eq!(b.reserved_hedge_qty,Some(dec!(1)));
        assert_eq!(b.total_net_pnl,Some(dec!(0)));
        assert_eq!(b.n_hedges,0);
        assert_eq!(b.n_censored_hedges,1);
        assert_eq!(b.n_depth_exhausted,0);
        let stale = simulate(config(&[100]),&tape(dec!(1)),5000);
        assert_eq!(bucket(&stale,100).total_net_pnl,None);
        assert!(!bucket(&stale,100).valuation_complete);
    }

    #[test]
    fn expired_pending_inventory_keeps_later_price_losses() {
        let mut events = tape(dec!(0.05));
        events.push(book_event(4,120,true,dec!(89),dec!(91),dec!(10)));
        events.push(book_event(5,120,false,dec!(90),dec!(90.2),dec!(10)));
        let report = simulate(config(&[100]),&events,200);
        let b = bucket(&report,100);
        assert_eq!(b.aster_qty,Some(dec!(0.05)));
        assert_eq!(b.lighter_qty,Some(dec!(0)));
        assert_eq!(b.total_net_pnl,Some(dec!(-0.5)));
        assert!(b.frozen_reason.is_some());
    }

    #[test]
    fn partial_execution_changes_only_filled_quantity_and_preserves_residual() {
        let mut events = tape(dec!(1));
        events.push(book_event(4,100,false,dec!(100),dec!(100.2),dec!(0.4)));
        let report = simulate(config(&[100]),&events,200);
        let b = bucket(&report,100);
        assert_eq!(b.lighter_qty,Some(dec!(-0.4)));
        assert_eq!(b.residual_qty,Some(dec!(0.6)));
        assert_eq!(b.reserved_hedge_qty,Some(dec!(0)));
        assert_eq!(b.total_net_pnl,Some(dec!(-0.04)));
        assert_eq!(b.frozen_reason.as_deref(),Some("PARTIAL_HEDGE"));
    }

    #[test]
    fn same_timestamp_market_update_cannot_reprice_an_already_due_hedge() {
        let mut events = tape(dec!(1));
        events.push(book_event(4,130,false,dec!(98),dec!(98.2),dec!(10)));
        let report = simulate(config(&[100]),&events,200);
        assert_eq!(bucket(&report,100).total_net_pnl,Some(dec!(1.9)));
    }

    #[test]
    fn fees_are_charged_once_and_spread_is_not_added_to_marked_positions() {
        let mut c = config(&[0]);
        c.edge.aster_maker_fee_bps = dec!(1);
        c.edge.taker_fee_bps = dec!(2);
        let report = simulate(c,&tape(dec!(1)),200);
        let b = bucket(&report,0);
        // Fee-adjusted bid rounds down to 99.9; hedge sells at 100.
        // Venue marks are 100 and 100.1, so +0.1 and -0.1 cancel.
        assert_eq!(b.realized_gross,Some(dec!(0)));
        assert_eq!(b.unrealized_pnl,Some(dec!(0)));
        assert_eq!(b.fees,Some(dec!(0.02999)));
        assert_eq!(b.total_net_pnl,Some(dec!(-0.02999)));
        assert!((b.captured_spread_pnl - 0.07001).abs()<1e-10);
    }

    #[test]
    fn opposite_pending_hedges_reserve_capital_and_close_with_zero_double_counting() {
        let mut c = config(&[100]);
        c.capital.aster_capital_usd = dec!(100.5);
        c.capital.hyperliquid_capital_usd = dec!(100.5);
        let mut events = tape(dec!(1));
        events.push(Event { seq: 4, local_recv_ts: ts()+Duration::milliseconds(35), market: "BTC".into(),
            kind: EventKind::AsterAggTrade { price: dec!(101),qty:dec!(1),buyer_is_maker:false,
                exch_ts:ts()+Duration::milliseconds(35) } });
        let report = simulate(c,&events,200);
        let b = bucket(&report,100);
        assert_eq!(b.fills,2);
        assert_eq!(b.aster_qty,Some(dec!(0)));
        assert_eq!(b.lighter_qty,Some(dec!(0)));
        assert_eq!(b.reserved_hedge_qty,Some(dec!(0)));
        assert_eq!(b.total_net_pnl,Some(dec!(0)));
        assert!(b.peak_aster_notional.unwrap()<=dec!(100.5));
        assert!(b.peak_lighter_notional.unwrap()<=dec!(100.5));
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

            let mut st = MarketState::new(spec(), QueueModel::Optimistic, 50);
            // best_bid 100.0 / best_ask 100.1
            st.aster_book = Some(Arc::new(OrderBook::from_levels(
                [(dec!(100.0), dec!(10))],
                [(dec!(100.1), dec!(10))],
                ts(),
                ts(),
            )));

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
