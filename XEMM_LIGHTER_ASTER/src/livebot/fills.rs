//! Aster fill detection → Hyperliquid hedge state machine (plan §4.1, §8.3).
//!
//! The single most important live-safety property: **every Aster fill produces exactly one
//! hedge, even if the fill event is delivered more than once** (invariants 2 & 4). Aster's
//! user stream can repeat `ORDER_TRADE_UPDATE`s, so we dedup on `(order_id, trade_id)` —
//! with a `(order_id, cumulative_filled_qty)` fallback when the trade id is missing — and
//! key the hedge on a deterministic cloid so a restart can ask Hyperliquid "did this
//! already hedge?" instead of double-hedging.

use std::collections::HashSet;

use rust_decimal::Decimal;

use crate::types::{MarketId, Side};

use super::ids::Cloid;

/// A parsed Aster maker fill (from `ORDER_TRADE_UPDATE` with `x = TRADE`). Field names
/// mirror the venue: `z` cumulative filled, `l` last filled qty, `L` last filled price.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsterFill {
    pub market: MarketId,
    /// Aster maker side that filled (the hedge is the opposite side).
    pub aster_side: Side,
    /// Venue order id (`i`).
    pub order_id: String,
    /// Trade id (`t`) — may be empty if the venue omitted it.
    pub trade_id: String,
    /// Bot client order id (`c`), used to attribute the fill to a known quote.
    pub client_id: String,
    /// Last filled quantity (`l`) — the increment this event represents.
    pub last_fill_qty: Decimal,
    /// Last filled price (`L`).
    pub last_fill_px: Decimal,
    /// Cumulative filled quantity (`z`) on the order so far.
    pub cum_filled_qty: Decimal,
    /// Event time (`E`) in venue ms — used to order updates.
    pub event_time_ms: i64,
    /// Whether this fill was on a REDUCE-ONLY order (`o.R`). A reduce-only fill is one of our own
    /// flatten/recovery closes — it REDUCES delta, so it must update the predicted position but
    /// must NOT trigger a new hedge (which would loop: hedge → flatten → its fill → hedge → …).
    pub reduce_only: bool,
}

/// The dedup key for a fill. Prefers `(order_id, trade_id)`; when the trade id is absent or
/// unreliable, falls back to `(order_id, cumulative_filled_qty)` (plan §4.1) — two
/// different cumulative levels are two distinct fills.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FillKey {
    Trade { order_id: String, trade_id: String },
    CumQty { order_id: String, cum_scaled: i64 },
}

impl FillKey {
    pub fn of(fill: &AsterFill) -> Self {
        // A SENTINEL trade id ("0"/"-1"/empty) is NOT a real, unique trade id — Aster can emit it
        // for every partial on an order. Treating it as reliable would collapse distinct partials
        // onto one key and silently DROP the later (unhedged) fills. Fall back to cumulative qty.
        let tid = fill.trade_id.trim();
        let reliable = !tid.is_empty() && tid != "0" && tid != "-1";
        if reliable {
            FillKey::Trade {
                order_id: fill.order_id.clone(),
                trade_id: fill.trade_id.clone(),
            }
        } else {
            // Scale cumulative qty to integer micro-units so the key is hashable/exact.
            FillKey::CumQty {
                order_id: fill.order_id.clone(),
                cum_scaled: cum_scaled(fill.cum_filled_qty),
            }
        }
    }
}

/// Tracks which fills have already triggered a hedge, so a repeated event never hedges
/// twice. A processed-but-not-yet-keyed fill can be re-observed safely.
#[derive(Debug, Default)]
pub struct FillDedup {
    hedged: HashSet<FillKey>,
}

impl FillDedup {
    pub fn new() -> Self {
        FillDedup::default()
    }

    /// Returns `true` the FIRST time this fill is seen (caller should create a hedge),
    /// `false` on every repeat (caller must NOT hedge again). Idempotent.
    pub fn observe(&mut self, fill: &AsterFill) -> bool {
        self.hedged.insert(FillKey::of(fill))
    }

    /// Whether this fill has already been hedged (without recording it).
    pub fn already_hedged(&self, fill: &AsterFill) -> bool {
        self.hedged.contains(&FillKey::of(fill))
    }

    pub fn len(&self) -> usize {
        self.hedged.len()
    }
    pub fn is_empty(&self) -> bool {
        self.hedged.is_empty()
    }
}

/// Fill-to-hedge lifecycle (plan §8.3). Forward path:
/// `Created → Submitted → Acked → Filled → Reconciled`. Any failure transition routes to a
/// terminal-ish state that freezes maker quoting until resolved (invariant 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HedgeState {
    Created,
    Submitted,
    Acked,
    Filled,
    Reconciled,
    // --- failure states ---
    Rejected,
    PartiallyFilled,
    Unknown,
    TimedOut,
}

impl HedgeState {
    pub fn as_str(self) -> &'static str {
        match self {
            HedgeState::Created => "CREATED",
            HedgeState::Submitted => "SUBMITTED",
            HedgeState::Acked => "ACKED",
            HedgeState::Filled => "FILLED",
            HedgeState::Reconciled => "RECONCILED",
            HedgeState::Rejected => "REJECTED",
            HedgeState::PartiallyFilled => "PARTIALLY_FILLED",
            HedgeState::Unknown => "UNKNOWN",
            HedgeState::TimedOut => "TIMED_OUT",
        }
    }

    /// A fully resolved hedge: no further action and does not block quoting.
    pub fn is_resolved(self) -> bool {
        matches!(self, HedgeState::Reconciled)
    }

    /// A state that must FREEZE maker quoting until an operator/reconciler resolves it
    /// (unknown / timed-out / rejected / partial — the orphan-leg danger zone).
    pub fn is_dangerous(self) -> bool {
        matches!(
            self,
            HedgeState::Rejected | HedgeState::PartiallyFilled | HedgeState::Unknown | HedgeState::TimedOut
        )
    }

    /// Still in flight (created/submitted/acked) — hedging is in progress, not yet orphaned.
    pub fn is_in_flight(self) -> bool {
        matches!(self, HedgeState::Created | HedgeState::Submitted | HedgeState::Acked)
    }
}

/// One hedge obligation created from an Aster fill. Carries the deterministic cloid so it
/// can be recovered by querying Hyperliquid `orderStatus` after a restart (§8.2).
#[derive(Debug, Clone)]
pub struct HedgeIntent {
    pub cloid: Cloid,
    pub market: MarketId,
    /// HL hedge side (opposite the Aster fill).
    pub hedge_side: Side,
    pub qty: Decimal,
    /// Average Aster fill price the hedge is offsetting (for PnL attribution).
    pub aster_fill_px: Decimal,
    pub state: HedgeState,
    pub created_ns: i64,
    pub submitted_ns: Option<i64>,
    /// HL order id once acked.
    pub hl_oid: Option<String>,
    /// Quantity actually hedged so far (for partial handling).
    pub filled_qty: Decimal,
    /// How many submit attempts have been made (normal → emergency → freeze, §4.3).
    pub attempts: u32,
    /// True for reconciler-backstop (orphan recovery) hedges. Lets `recover_orphans`
    /// recognize its own outstanding intents so it never overwrites one or races a second
    /// order onto the wire for the same net.
    pub recovery: bool,
}

/// Scale a cumulative-fill quantity to integer micro-units for the cloid / FillKey (matches
/// [`FillKey::of`]). Session-independent — derived purely from exchange data.
pub fn cum_scaled(cum_filled_qty: Decimal) -> i64 {
    use rust_decimal::prelude::ToPrimitive;
    (cum_filled_qty * Decimal::from(1_000_000)).round().to_i64().unwrap_or(i64::MAX)
}

impl HedgeIntent {
    /// Create a hedge obligation from a fill (invariant 2: exactly one per fill). The hedge
    /// side is the opposite of the Aster maker side; the cloid is deterministic and
    /// **session-independent** (derived only from the exchange fill identity), so a restart
    /// re-processing the same fill computes the SAME cloid and recovery-by-cloid works (§8.2).
    pub fn from_fill(fill: &AsterFill, now_ns: i64) -> Self {
        HedgeIntent {
            cloid: Cloid::hedge(&fill.order_id, &fill.trade_id, cum_scaled(fill.cum_filled_qty)),
            market: fill.market.clone(),
            hedge_side: fill.aster_side.opposite(),
            qty: fill.last_fill_qty,
            aster_fill_px: fill.last_fill_px,
            state: HedgeState::Created,
            created_ns: now_ns,
            submitted_ns: None,
            hl_oid: None,
            filled_qty: Decimal::ZERO,
            attempts: 0,
            recovery: false,
        }
    }

    /// A hedge intent for a given `qty` not tied 1:1 to a single fill — used both for an
    /// ACCUMULATED hedge (the net of several sub-min partials reached hedgeable size) and for a
    /// RECOVERY hedge (the reconciler backstop offsetting an orphaned net delta; set
    /// `recovery = true` on the returned intent). `cloid` is the caller's deterministic id;
    /// `qty` is the amount to hedge.
    pub fn with_qty(cloid: Cloid, market: MarketId, hedge_side: Side, qty: Decimal, ref_px: Decimal, now_ns: i64) -> Self {
        HedgeIntent {
            cloid,
            market,
            hedge_side,
            qty,
            aster_fill_px: ref_px,
            state: HedgeState::Created,
            created_ns: now_ns,
            submitted_ns: None,
            hl_oid: None,
            filled_qty: Decimal::ZERO,
            attempts: 0,
            recovery: false,
        }
    }

    pub fn mark_submitted(&mut self, now_ns: i64) {
        self.state = HedgeState::Submitted;
        self.submitted_ns = Some(now_ns);
        self.attempts += 1;
    }

    pub fn mark_acked(&mut self, hl_oid: String) {
        self.hl_oid = Some(hl_oid);
        if self.state == HedgeState::Submitted {
            self.state = HedgeState::Acked;
        }
    }

    /// Apply a hedge fill increment. Transitions to `Filled` once fully hedged, else
    /// `PartiallyFilled` (which freezes quoting until resolved).
    pub fn apply_fill(&mut self, filled_qty: Decimal) {
        self.filled_qty += filled_qty;
        if self.filled_qty >= self.qty {
            self.state = HedgeState::Filled;
        } else {
            self.state = HedgeState::PartiallyFilled;
        }
    }

    pub fn mark_reconciled(&mut self) {
        self.state = HedgeState::Reconciled;
    }
    pub fn mark_rejected(&mut self) {
        self.state = HedgeState::Rejected;
    }
    pub fn mark_unknown(&mut self) {
        self.state = HedgeState::Unknown;
    }

    /// Mark timed-out if it has been in flight longer than `timeout_ns` without resolving.
    pub fn check_timeout(&mut self, now_ns: i64, timeout_ns: i64) {
        if self.state.is_in_flight()
            && now_ns.saturating_sub(self.created_ns) > timeout_ns
        {
            self.state = HedgeState::TimedOut;
        }
    }

    pub fn remaining_qty(&self) -> Decimal {
        (self.qty - self.filled_qty).max(Decimal::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn fill(order: &str, trade: &str, last: Decimal, cum: Decimal) -> AsterFill {
        AsterFill {
            market: "BTC".into(),
            aster_side: Side::Buy,
            order_id: order.into(),
            trade_id: trade.into(),
            client_id: "Xabc-BTC-B-0".into(),
            last_fill_qty: last,
            last_fill_px: dec!(100),
            cum_filled_qty: cum,
            event_time_ms: 1,
            reduce_only: false,
        }
    }

    #[test]
    fn dedup_hedges_once_per_trade() {
        let mut d = FillDedup::new();
        let f = fill("100", "T7", dec!(0.5), dec!(0.5));
        assert!(d.observe(&f)); // first sighting => hedge
        assert!(!d.observe(&f)); // repeat => do NOT hedge again
        assert!(d.already_hedged(&f));
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn dedup_falls_back_to_cum_qty_without_trade_id() {
        let mut d = FillDedup::new();
        let a = fill("100", "", dec!(0.3), dec!(0.3));
        let b = fill("100", "", dec!(0.2), dec!(0.5)); // same order, new cumulative
        assert!(d.observe(&a));
        assert!(d.observe(&b)); // distinct cumulative => distinct fill
        assert!(!d.observe(&a)); // a repeated again => deduped
        assert_eq!(d.len(), 2);
    }

    #[test]
    fn sentinel_trade_id_zero_is_not_a_reliable_key() {
        // A sentinel "0"/"-1" trade id must be treated as ABSENT, so two distinct partials with
        // trade_id "0" are keyed by cumulative qty (distinct) — not collapsed onto one Trade key
        // (which would DROP the second, unhedged).
        let mut d = FillDedup::new();
        let a = fill("100", "0", dec!(0.3), dec!(0.3));
        let b = fill("100", "0", dec!(0.2), dec!(0.5)); // same order + sentinel tid, NEW cumulative
        assert!(matches!(FillKey::of(&a), FillKey::CumQty { .. }), "sentinel tid must fall back to CumQty");
        assert!(d.observe(&a));
        assert!(d.observe(&b), "distinct cumulative with sentinel tid must NOT be deduped away");
        assert_eq!(d.len(), 2);
    }

    #[test]
    fn hedge_intent_from_fill_is_opposite_side_and_deterministic() {
        let f = fill("100", "T7", dec!(0.5), dec!(0.5));
        let h1 = HedgeIntent::from_fill(&f, 1_000);
        let h2 = HedgeIntent::from_fill(&f, 9_999);
        assert_eq!(h1.hedge_side, Side::Sell); // Buy fill => Sell hedge
        assert_eq!(h1.qty, dec!(0.5));
        assert_eq!(h1.state, HedgeState::Created);
        // same fill identity => same cloid regardless of wall-clock (idempotent recovery)
        assert_eq!(h1.cloid, h2.cloid);
        // cloid is session-independent: a freshly-constructed intent from the SAME exchange
        // fill (no bot-side counter involved) is identical — restart recovery works.
        let h3 = HedgeIntent::from_fill(&fill("100", "T7", dec!(0.5), dec!(0.5)), 42);
        assert_eq!(h1.cloid, h3.cloid);
    }

    #[test]
    fn hedge_lifecycle_forward_path() {
        let f = fill("100", "T7", dec!(0.5), dec!(0.5));
        let mut h = HedgeIntent::from_fill(&f, 0);
        h.mark_submitted(10);
        assert_eq!(h.state, HedgeState::Submitted);
        assert_eq!(h.attempts, 1);
        h.mark_acked("oid-1".into());
        assert_eq!(h.state, HedgeState::Acked);
        h.apply_fill(dec!(0.5));
        assert_eq!(h.state, HedgeState::Filled);
        assert_eq!(h.remaining_qty(), dec!(0));
        h.mark_reconciled();
        assert!(h.state.is_resolved());
    }

    #[test]
    fn partial_hedge_is_dangerous() {
        let f = fill("100", "T7", dec!(0.5), dec!(0.5));
        let mut h = HedgeIntent::from_fill(&f, 0);
        h.mark_submitted(0);
        h.apply_fill(dec!(0.2)); // only 0.2 of 0.5
        assert_eq!(h.state, HedgeState::PartiallyFilled);
        assert!(h.state.is_dangerous());
        assert_eq!(h.remaining_qty(), dec!(0.3));
    }

    #[test]
    fn accumulated_fills_reach_filled() {
        // A hedge reported across MULTIPLE fill events must reach Filled once the cumulative
        // filled reaches qty — at which point the strategy reconciles it. (A genuine shortfall
        // larger than the venue qty step stays PartiallyFilled and is handled by recovery.)
        let f = fill("100", "T7", dec!(0.5), dec!(0.5));
        let mut h = HedgeIntent::from_fill(&f, 0);
        h.mark_submitted(0);
        h.apply_fill(dec!(0.3));
        assert_eq!(h.state, HedgeState::PartiallyFilled);
        assert_eq!(h.remaining_qty(), dec!(0.2));
        h.apply_fill(dec!(0.2)); // cumulative 0.5 == qty
        assert_eq!(h.state, HedgeState::Filled);
        assert_eq!(h.remaining_qty(), dec!(0));
    }

    #[test]
    fn hedge_times_out_when_in_flight_too_long() {
        let f = fill("100", "T7", dec!(0.5), dec!(0.5));
        let mut h = HedgeIntent::from_fill(&f, 0);
        h.mark_submitted(0);
        h.check_timeout(500, 1_000); // not yet
        assert!(h.state.is_in_flight());
        h.check_timeout(2_000, 1_000); // overdue
        assert_eq!(h.state, HedgeState::TimedOut);
        assert!(h.state.is_dangerous());
    }
}
