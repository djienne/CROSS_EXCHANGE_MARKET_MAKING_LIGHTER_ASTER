//! Simulate our resting quote being filled by an Aster aggTrade sweep.
//!
//! Corrected vs the original design: a Binance-style aggTrade aggregates per (taker
//! order, price), so a multi-level sweep is a *sequence* of prints. We carry a
//! running `remaining_ahead_qty` on the quote across prints; prints strictly
//! better than our price only burn the queue ahead, while prints at-or-through
//! our price burn any residual queue then fill us. The fill price is always our
//! resting price; the print price is kept only for diagnostics.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::requoter::LiveQuote;
use crate::types::{MarketId, Side};

#[derive(Debug, Clone)]
pub struct AsterAggTrade {
    pub market: MarketId,
    pub price: Decimal,
    pub qty: Decimal,
    /// Aster `m`: is the buyer the market maker? (true => taker sold).
    pub buyer_is_maker: bool,
    pub exch_ts: DateTime<Utc>,
    pub local_recv_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SimulatedAsterFill {
    pub id: Uuid,
    pub quote_id: Uuid,
    pub market: MarketId,
    pub aster_side: Side,
    pub fill_px: Decimal,
    pub fill_qty: Decimal,
    pub sweep_print_px: Decimal,
    /// The resting quote's quoted spread at fill time (the "spread used" for this
    /// trade): instant edge over the hedge, and distance from the touch, in bps.
    pub quoted_edge_bps: Decimal,
    pub quoted_distance_bps: Decimal,
    pub remaining_quote_qty_after_fill: Decimal,
    pub was_trade_through: bool,
    pub was_partial: bool,
    /// The matched feed was stale at fill time (set by the engine's stale-feed path:
    /// a quote that could not be cancelled in time was still hit during cancel latency).
    pub feed_stale_at_fill: bool,
    /// The resting quote was beyond Aster's captured depth20, so its seeded queue
    /// ahead was only a lower bound (this fill may be optimistic).
    pub queue_truncated: bool,
    pub exch_ts: DateTime<Utc>,
    pub local_recv_ts: DateTime<Utc>,
}

/// Does this taker flow hit a resting quote on `side`?
/// Our bid is hit by a market sell (`buyer_is_maker == true`); our ask is lifted
/// by a market buy (`buyer_is_maker == false`).
#[inline]
pub fn print_matches(side: Side, buyer_is_maker: bool) -> bool {
    match side {
        Side::Buy => buyer_is_maker,
        Side::Sell => !buyer_is_maker,
    }
}

/// Apply one aggTrade print to a quote, mutating its queue/fill state. Returns a
/// fill if (and only if) this print reached and consumed some of our quantity.
///
/// `taker_remaining` is the unconsumed taker quantity of this print, shared across
/// all of our same-side resting quotes (live + dying) and decremented as the print
/// burns queue or fills us — so a single sweep can never fill more than its size
/// across our combined orders. The caller seeds it to `t.qty` and walks our
/// same-side quotes in price priority.
pub fn apply_print(
    quote: &mut LiveQuote,
    t: &AsterAggTrade,
    taker_remaining: &mut Decimal,
) -> Option<SimulatedAsterFill> {
    if *taker_remaining <= Decimal::ZERO {
        return None;
    }
    if !quote.is_fillable_at(t.local_recv_ts) {
        return None;
    }
    let side = quote.side();
    if !print_matches(side, t.buyer_is_maker) {
        return None;
    }
    let qpx = quote.price();

    let strictly_better = match side {
        Side::Buy => t.price > qpx,  // better bids ahead of us
        Side::Sell => t.price < qpx, // better asks ahead of us
    };
    if strictly_better {
        let consumed = quote.remaining_ahead_qty.min(*taker_remaining);
        quote.remaining_ahead_qty -= consumed;
        *taker_remaining -= consumed;
        return None;
    }

    // At our price or through us: burn residual queue-ahead, then fill us.
    let consumed = quote.remaining_ahead_qty.min(*taker_remaining);
    quote.remaining_ahead_qty -= consumed;
    *taker_remaining -= consumed;
    if *taker_remaining <= Decimal::ZERO {
        return None;
    }
    let fill = quote.remaining_qty.min(*taker_remaining);
    if fill <= Decimal::ZERO {
        return None;
    }
    quote.remaining_qty -= fill;
    *taker_remaining -= fill;

    let was_trade_through = match side {
        Side::Buy => t.price < qpx,
        Side::Sell => t.price > qpx,
    };
    Some(SimulatedAsterFill {
        id: Uuid::new_v4(),
        quote_id: quote.id,
        market: quote.market.clone(),
        aster_side: side,
        fill_px: qpx,
        fill_qty: fill,
        sweep_print_px: t.price,
        quoted_edge_bps: quote.desired.instant_edge_bps,
        quoted_distance_bps: quote.desired.distance_from_touch_bps,
        remaining_quote_qty_after_fill: quote.remaining_qty,
        was_trade_through,
        was_partial: quote.remaining_qty > Decimal::ZERO,
        // Default false; the engine's halt-on-stale path overrides this to true for a
        // fill that lands while the matched feed is stale (a stale-window adverse fill).
        feed_stale_at_fill: false,
        queue_truncated: quote.desired.queue_truncated,
        exch_ts: t.exch_ts,
        local_recv_ts: t.local_recv_ts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quote_engine::{AsterEffectiveTouchSource, DesiredQuote};
    use crate::requoter::{LiveQuote, LiveQuoteState, RequoteConfig};
    use crate::types::QueueModel;
    use rust_decimal_macros::dec;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn cfg() -> RequoteConfig {
        RequoteConfig {
            simulated_aster_place_latency_ms: 25,
            simulated_aster_cancel_latency_ms: 25,
            quote_ttl_ms: 5_000,
        }
    }

    fn desired(side: Side, price: Decimal, qty: Decimal) -> DesiredQuote {
        DesiredQuote {
            aster_side: side,
            price,
            qty,
            hedge_side: side.opposite(),
            expected_hl_vwap: price,
            expected_hl_depth_filled_qty: qty,
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
            depth_target_qty: qty,
            aster_depth_filled_qty: qty,
            aster_depth_levels_used: 1,
            size_clamped_up: false,
            queue_truncated: false,
        }
    }

    // Build a live, fillable quote with explicit queue/qty state.
    fn quote(side: Side, price: Decimal, qty: Decimal, ahead: Decimal) -> LiveQuote {
        let mut lq = LiveQuote::from_desired(
            "BTC".into(),
            desired(side, price, qty),
            ts(),
            &cfg(),
            QueueModel::Optimistic,
            dec!(1),
        );
        lq.state = LiveQuoteState::Live;
        lq.remaining_qty = qty;
        lq.remaining_ahead_qty = ahead;
        lq
    }

    fn agg(price: Decimal, qty: Decimal, buyer_is_maker: bool) -> AsterAggTrade {
        AsterAggTrade {
            market: "BTC".into(),
            price,
            qty,
            buyer_is_maker,
            exch_ts: ts(),
            local_recv_ts: ts() + chrono::Duration::milliseconds(100),
        }
    }

    // Single-quote convenience: a fresh taker residual per print.
    fn apply(q: &mut LiveQuote, t: &AsterAggTrade) -> Option<SimulatedAsterFill> {
        let mut taker = t.qty;
        apply_print(q, t, &mut taker)
    }

    #[test]
    fn optimistic_single_partial() {
        let mut q = quote(Side::Buy, dec!(100), dec!(1), dec!(0));
        // market sell at our price, qty 0.3 < our 1 => partial fill.
        let f = apply(&mut q, &agg(dec!(100), dec!(0.3), true)).unwrap();
        assert_eq!(f.fill_qty, dec!(0.3));
        assert_eq!(f.fill_px, dec!(100));
        assert!(f.was_partial);
        assert_eq!(q.remaining_qty, dec!(0.7));
    }

    #[test]
    fn visible_queue_eats_first_then_fills() {
        let mut q = quote(Side::Buy, dec!(100), dec!(1), dec!(5));
        // First print burns part of the queue ahead; no fill.
        assert!(apply(&mut q, &agg(dec!(100), dec!(3), true)).is_none());
        assert_eq!(q.remaining_ahead_qty, dec!(2));
        // Second print: 2 burns the rest of the queue, 2 fills us (capped at 1).
        let f = apply(&mut q, &agg(dec!(100), dec!(4), true)).unwrap();
        assert_eq!(f.fill_qty, dec!(1));
        assert_eq!(q.remaining_ahead_qty, dec!(0));
    }

    #[test]
    fn better_level_print_decrements_ahead() {
        let mut q = quote(Side::Buy, dec!(100), dec!(1), dec!(3));
        // Print strictly better (101 > 100) only burns ahead.
        assert!(apply(&mut q, &agg(dec!(101), dec!(2), true)).is_none());
        assert_eq!(q.remaining_ahead_qty, dec!(1));
        // At-price print: 1 burns rest, 0.5 fills.
        let f = apply(&mut q, &agg(dec!(100), dec!(1.5), true)).unwrap();
        assert_eq!(f.fill_qty, dec!(0.5));
    }

    #[test]
    fn trade_through_honors_ahead_then_fills() {
        let mut q = quote(Side::Buy, dec!(100), dec!(1), dec!(2));
        // Sell printed below our bid (99.5) with large qty => through us.
        let f = apply(&mut q, &agg(dec!(99.5), dec!(5), true)).unwrap();
        assert!(f.was_trade_through);
        assert_eq!(f.fill_qty, dec!(1)); // 2 ahead burned, then full fill
        assert_eq!(q.remaining_qty, dec!(0));
    }

    #[test]
    fn wrong_direction_no_fill() {
        let mut q = quote(Side::Buy, dec!(100), dec!(1), dec!(0));
        // buyer_is_maker == false => market BUY, cannot fill our bid.
        assert!(apply(&mut q, &agg(dec!(100), dec!(1), false)).is_none());
        assert_eq!(q.remaining_qty, dec!(1));
    }

    #[test]
    fn not_fillable_before_active() {
        let mut q = quote(Side::Buy, dec!(100), dec!(1), dec!(0));
        q.state = LiveQuoteState::PendingPlacement; // active_at = ts()+25ms
        let early = AsterAggTrade {
            local_recv_ts: ts(), // before active_at
            ..agg(dec!(100), dec!(1), true)
        };
        assert!(apply(&mut q, &early).is_none());
    }

    #[test]
    fn ask_filled_by_market_buy() {
        let mut q = quote(Side::Sell, dec!(100), dec!(1), dec!(0));
        // market buy (buyer_is_maker == false) at our ask.
        let f = apply(&mut q, &agg(dec!(100), dec!(0.4), false)).unwrap();
        assert_eq!(f.fill_qty, dec!(0.4));
    }

    #[test]
    fn shared_residual_caps_combined_fill() {
        // Two of our bids at the same price (e.g. a live quote + its dying predecessor),
        // each wanting 1.0, no queue ahead. A single market sell of only 0.6, shared,
        // must fill at most 0.6 across BOTH — never 1.2.
        let mut a = quote(Side::Buy, dec!(100), dec!(1), dec!(0));
        let mut b = quote(Side::Buy, dec!(100), dec!(1), dec!(0));
        let t = agg(dec!(100), dec!(0.6), true);
        let mut taker = t.qty;
        let fa = apply_print(&mut a, &t, &mut taker);
        let fb = apply_print(&mut b, &t, &mut taker);
        let total = fa.map(|f| f.fill_qty).unwrap_or_default()
            + fb.map(|f| f.fill_qty).unwrap_or_default();
        assert_eq!(total, dec!(0.6));
        assert!(taker <= Decimal::ZERO);
    }
}
