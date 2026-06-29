//! Simulated quote lifecycle. A desired quote becomes a
//! `LiveQuote` that is fillable only after a simulated placement latency and,
//! critically, REMAINS fillable while a cancel is in flight — the window where
//! stale-quote losses happen. The three queue models differ solely in the
//! `remaining_ahead_qty` seeded here.

use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::quote_engine::DesiredQuote;
use crate::types::{MarketId, QueueModel, RejectReason, Side};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveQuoteState {
    PendingPlacement,
    Live,
    PendingCancel,
    Cancelled,
    Filled,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaceReason {
    PriceChanged,
    QuantityChanged,
    EdgeBelowMinimum,
    LighterBookMoved,
    AsterBookMoved,
    QuoteTooFarFromTouch,
    QuoteTooCloseToTouch,
    QuoteExpired,
    NoLongerProfitable,
    /// The market-data feed went stale; pull the quote (we can't trust the hedge
    /// price). The simulator analogue of the live `TradingGate` closing.
    FeedStale,
}

impl ReplaceReason {
    pub fn as_str(self) -> &'static str {
        match self {
            ReplaceReason::PriceChanged => "PRICE_CHANGED",
            ReplaceReason::QuantityChanged => "QUANTITY_CHANGED",
            ReplaceReason::EdgeBelowMinimum => "EDGE_BELOW_MINIMUM",
            ReplaceReason::LighterBookMoved => "LIGHTER_BOOK_MOVED",
            ReplaceReason::AsterBookMoved => "ASTER_BOOK_MOVED",
            ReplaceReason::QuoteTooFarFromTouch => "QUOTE_TOO_FAR_FROM_TOUCH",
            ReplaceReason::QuoteTooCloseToTouch => "QUOTE_TOO_CLOSE_TO_TOUCH",
            ReplaceReason::QuoteExpired => "QUOTE_EXPIRED",
            ReplaceReason::NoLongerProfitable => "NO_LONGER_PROFITABLE",
            ReplaceReason::FeedStale => "FEED_STALE",
        }
    }

    /// Map a quote-rejection cause into the reason we tag the cancel of a now-invalid
    /// standing quote. Feed-state failures — a stale or absent book on either venue, the
    /// same conditions the fill-time stale-halt in `sim::engine` reports as `FeedStale` —
    /// surface as `FeedStale`; every other cause (no profitable edge, crossed book,
    /// position cap, insufficient depth, …) collapses to `NoLongerProfitable`. Keeps the
    /// recompute cancel reason honest and consistent with the apply-trade path; the full
    /// `RejectReason` is still preserved verbatim in the rejected-opportunity row.
    pub fn from_reject(reason: RejectReason) -> ReplaceReason {
        use RejectReason::*;
        match reason {
            AsterBookStale | HlBookStale | MissingAsterBook | MissingHlBook | HlBboThinAndL2Stale | AsterEffectiveTouchUnavailable => {
                ReplaceReason::FeedStale
            }
            QuoteTooCloseToTouch => ReplaceReason::QuoteTooCloseToTouch,
            _ => ReplaceReason::NoLongerProfitable,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequoteConfig {
    pub simulated_aster_place_latency_ms: i64,
    pub simulated_aster_cancel_latency_ms: i64,
    pub quote_ttl_ms: i64,
}

#[derive(Debug, Clone)]
pub struct LiveQuote {
    pub id: Uuid,
    pub market: MarketId,
    pub queue_model: QueueModel,
    pub desired: DesiredQuote,

    pub state: LiveQuoteState,

    pub created_at: DateTime<Utc>,
    pub active_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,

    pub cancel_sent_at: Option<DateTime<Utc>>,
    pub cancel_effective_at: Option<DateTime<Utc>>,
    pub replace_reason: Option<ReplaceReason>,

    pub remaining_qty: Decimal,
    pub remaining_ahead_qty: Decimal,
    /// True once the quote has actually rested on the book (reached `Live`). A
    /// quote cancelled while still being placed never rested, so it is not fillable.
    pub was_live: bool,
}

impl LiveQuote {
    pub fn from_desired(
        market: MarketId,
        desired: DesiredQuote,
        now: DateTime<Utc>,
        cfg: &RequoteConfig,
        queue_model: QueueModel,
        hidden_queue_multiplier: Decimal,
    ) -> Self {
        let same = desired.queue_ahead_qty;
        let better = desired.better_levels_qty;
        let remaining_ahead_qty = match queue_model {
            QueueModel::Optimistic => better,
            QueueModel::VisibleQueue => better + same,
            QueueModel::Conservative => better + same + same * hidden_queue_multiplier,
        };
        LiveQuote {
            id: Uuid::new_v4(),
            market,
            queue_model,
            remaining_qty: desired.qty,
            remaining_ahead_qty,
            desired,
            state: LiveQuoteState::PendingPlacement,
            created_at: now,
            active_at: now + Duration::milliseconds(cfg.simulated_aster_place_latency_ms),
            expires_at: now + Duration::milliseconds(cfg.quote_ttl_ms),
            cancel_sent_at: None,
            cancel_effective_at: None,
            replace_reason: None,
            was_live: false,
        }
    }

    #[inline]
    pub fn side(&self) -> Side {
        self.desired.aster_side
    }

    #[inline]
    pub fn price(&self) -> Decimal {
        self.desired.price
    }

    /// Active = the slot's current quote (resting or being placed), not dying or terminal.
    #[inline]
    pub fn is_active(&self) -> bool {
        matches!(
            self.state,
            LiveQuoteState::PendingPlacement | LiveQuoteState::Live
        )
    }

    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            LiveQuoteState::Cancelled | LiveQuoteState::Filled | LiveQuoteState::Expired
        )
    }

    /// Whether an incoming trade at `now` could fill this quote.
    pub fn is_fillable_at(&self, now: DateTime<Utc>) -> bool {
        match self.state {
            LiveQuoteState::PendingPlacement => now >= self.active_at && now <= self.expires_at,
            LiveQuoteState::Live => now <= self.expires_at,
            LiveQuoteState::PendingCancel => {
                // Only fillable if the order actually rested: a quote cancelled while
                // still being placed (never reached `Live`) never hit the book.
                self.was_live
                    && match self.cancel_effective_at {
                        Some(t) => now < t,
                        None => true,
                    }
            }
            LiveQuoteState::Cancelled | LiveQuoteState::Filled | LiveQuoteState::Expired => false,
        }
    }

    /// Advance time-driven state transitions (placement, expiry, cancel-effective).
    pub fn advance_state(&mut self, now: DateTime<Utc>) {
        match self.state {
            LiveQuoteState::PendingPlacement if now >= self.active_at => {
                self.state = LiveQuoteState::Live;
                self.was_live = true;
            }
            _ => {}
        }
        if self.state == LiveQuoteState::Live && now > self.expires_at {
            self.state = LiveQuoteState::Expired;
        }
        if self.state == LiveQuoteState::PendingCancel {
            if let Some(t) = self.cancel_effective_at {
                if now >= t {
                    self.state = LiveQuoteState::Cancelled;
                }
            }
        }
    }

    /// Send a simulated cancel; the quote stays fillable until `cancel_effective_at`.
    pub fn request_cancel(&mut self, now: DateTime<Utc>, cfg: &RequoteConfig, reason: ReplaceReason) {
        if self.is_terminal() || self.state == LiveQuoteState::PendingCancel {
            return;
        }
        self.state = LiveQuoteState::PendingCancel;
        self.cancel_sent_at = Some(now);
        self.cancel_effective_at =
            Some(now + Duration::milliseconds(cfg.simulated_aster_cancel_latency_ms));
        self.replace_reason = Some(reason);
    }

    pub fn mark_filled(&mut self) {
        self.state = LiveQuoteState::Filled;
    }

    /// Decide whether a freshly computed desired quote warrants replacing this one.
    pub fn should_replace(
        &self,
        new: &DesiredQuote,
        price_change_ticks: u32,
        tick: Decimal,
    ) -> Option<ReplaceReason> {
        let threshold = Decimal::from(price_change_ticks) * tick;
        if (self.desired.price - new.price).abs() >= threshold {
            return Some(ReplaceReason::PriceChanged);
        }
        if self.desired.qty != new.qty {
            return Some(ReplaceReason::QuantityChanged);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::OrderBook;
    use crate::edge::EdgeConfig;
    use crate::quote_engine::{compute_desired_quote, PositionContext, QuoteEngineConfig};
    use rust_decimal_macros::dec;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn cfg() -> RequoteConfig {
        RequoteConfig {
            simulated_aster_place_latency_ms: 25,
            simulated_aster_cancel_latency_ms: 25,
            quote_ttl_ms: 500,
        }
    }

    fn desired() -> DesiredQuote {
        let edge = EdgeConfig {
            min_net_profit_bps: dec!(3.0),
            slippage_buffer_bps: dec!(1.5),
            latency_buffer_bps: dec!(2.0),
            basis_buffer_bps: dec!(1.0),
            funding_buffer_bps: dec!(0.0),
            aster_maker_fee_bps: dec!(0.0),
            taker_fee_bps: dec!(4.5),
        };
        let q = QuoteEngineConfig {
            desired_notional: dec!(100),
            max_quote_distance_bps: dec!(50.0),
            min_aster_touch_distance_bps: dec!(0.0),
            min_aster_touch_hysteresis_bps: dec!(2.0),
            max_aster_touch_hysteresis_ms: 300_000,
            depth_liquidity_multiple: dec!(10.0),
            max_hedge_slippage_bps: dec!(50.0),
            min_requote_interval_ms: 20,
            price_change_ticks_to_requote: 1,
            clamp_to_min_lot: true,
            min_requote_bps: dec!(1.0),
        };
        let aster = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            ts(),
            ts(),
        );
        let hl = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(100))],
            vec![(dec!(100.05), dec!(100))],
            ts(),
            ts(),
        );
        compute_desired_quote(
            &edge, &q, &aster, &hl, Side::Buy, dec!(0.01), dec!(0.001), dec!(0.001), dec!(5), dec!(5), 750,
            ts(), &PositionContext::unconstrained(),
        )
        .unwrap()
    }

    #[test]
    fn quote_too_close_maps_to_specific_cancel_reason() {
        assert_eq!(
            ReplaceReason::from_reject(RejectReason::QuoteTooCloseToTouch),
            ReplaceReason::QuoteTooCloseToTouch
        );
        assert_eq!(ReplaceReason::QuoteTooCloseToTouch.as_str(), "QUOTE_TOO_CLOSE_TO_TOUCH");
    }

    #[test]
    fn fillable_window() {
        let lq = LiveQuote::from_desired(
            "BTC".into(),
            desired(),
            ts(),
            &cfg(),
            QueueModel::Optimistic,
            dec!(1),
        );
        assert!(!lq.is_fillable_at(ts())); // before placement latency
        assert!(lq.is_fillable_at(ts() + Duration::milliseconds(25)));
        assert!(lq.is_fillable_at(ts() + Duration::milliseconds(400)));
        assert!(!lq.is_fillable_at(ts() + Duration::milliseconds(600))); // past TTL (Live->checks expires)
    }

    #[test]
    fn fillable_until_cancel_effective() {
        let mut lq = LiveQuote::from_desired(
            "BTC".into(),
            desired(),
            ts(),
            &cfg(),
            QueueModel::Optimistic,
            dec!(1),
        );
        let t = ts() + Duration::milliseconds(100);
        lq.advance_state(t);
        lq.request_cancel(t, &cfg(), ReplaceReason::PriceChanged);
        assert!(lq.is_fillable_at(t + Duration::milliseconds(10))); // still fillable
        assert!(!lq.is_fillable_at(t + Duration::milliseconds(30))); // cancel effective
    }

    #[test]
    fn conservative_seeds_more_ahead_than_optimistic() {
        let d = desired();
        let opt = LiveQuote::from_desired("BTC".into(), d.clone(), ts(), &cfg(), QueueModel::Optimistic, dec!(1));
        let cons = LiveQuote::from_desired("BTC".into(), d, ts(), &cfg(), QueueModel::Conservative, dec!(1));
        assert!(cons.remaining_ahead_qty >= opt.remaining_ahead_qty);
    }

    #[test]
    fn never_live_cancel_is_not_fillable() {
        // Cancel while still PendingPlacement (active_at = ts()+25ms): the order never
        // rested, so it must not be fillable even within the cancel window.
        let mut lq = LiveQuote::from_desired(
            "BTC".into(),
            desired(),
            ts(),
            &cfg(),
            QueueModel::Optimistic,
            dec!(1),
        );
        lq.request_cancel(ts(), &cfg(), ReplaceReason::PriceChanged);
        assert_eq!(lq.state, LiveQuoteState::PendingCancel);
        assert!(!lq.is_fillable_at(ts() + Duration::milliseconds(10)));
    }
}
