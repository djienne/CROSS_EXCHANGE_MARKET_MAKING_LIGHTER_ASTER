//! Pair eligibility classification (plan §7.2) and the strict-mode filter — one of the
//! most important live risks (§7). The danger: an Aster maker order can partial-fill an
//! amount BELOW Hyperliquid's minimum hedge notional, leaving a temporary unhedged leg.
//!
//! At startup we classify every market against a reference price and the configured quote
//! size, then the partial policy decides which pairs may trade live.

use rust_decimal::Decimal;

use crate::config::PartialPolicy;
use crate::decimal::{ceil_to_step, floor_to_step};
use crate::inventory::{hl_min_hedge_qty, HedgeabilityRules};
use crate::markets::MarketSpec;

/// Pair classes (plan §7.2), best → worst.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PairClass {
    /// Every possible Aster fill (down to a single lot/step) is HL-hedgeable.
    A,
    /// The full quote is hedgeable, but a small partial fill may be sub-minimum on HL.
    B,
    /// The desired quote size itself is below the HL minimum hedge — un-hedgeable as quoted.
    C,
    /// Degenerate / un-sizeable (non-positive reference, zero step, or no valid min order).
    D,
}

impl PairClass {
    pub fn as_str(self) -> &'static str {
        match self {
            PairClass::A => "A",
            PairClass::B => "B",
            PairClass::C => "C",
            PairClass::D => "D",
        }
    }
}

/// The sizing facts behind a classification, surfaced for logging / the eligibility report.
#[derive(Debug, Clone)]
pub struct PairClassification {
    pub class: PairClass,
    /// Smallest Aster execution increment (one lot/step) — the smallest a partial can be.
    pub aster_min_fill_qty: Decimal,
    /// Minimum HL-hedgeable quantity at the reference price.
    pub hl_min_hedge_qty: Decimal,
    /// The desired quote quantity (floored to a whole step).
    pub desired_qty: Decimal,
}

/// Classify a market at `ref_px` for a `desired_notional` quote.
pub fn classify(spec: &MarketSpec, ref_px: Decimal, desired_notional: Decimal) -> PairClassification {
    if ref_px <= Decimal::ZERO || spec.step <= Decimal::ZERO {
        return PairClassification {
            class: PairClass::D,
            aster_min_fill_qty: Decimal::ZERO,
            hl_min_hedge_qty: Decimal::ZERO,
            desired_qty: Decimal::ZERO,
        };
    }
    let rules = HedgeabilityRules {
        hyperliquid_min_notional: spec.hl_min_notional,
        hyperliquid_qty_step: spec.hl_qty_step,
    };
    let hl_min = hl_min_hedge_qty(&rules, ref_px);
    // The smallest possible Aster fill is one lot (a large order can partial-fill a single
    // step); `aster_min_qty` floors the order itself but not a partial, so the step governs.
    let aster_min_fill = spec.step.max(Decimal::ZERO);
    let desired_qty = floor_to_step(desired_notional / ref_px, spec.step);

    let class = if desired_qty < hl_min {
        // Even the whole intended quote can't be hedged on HL.
        PairClass::C
    } else if aster_min_fill >= hl_min {
        // Every conceivable partial (≥ one step) already clears the HL minimum.
        PairClass::A
    } else {
        // Full quote hedgeable, but a sub-min partial is possible.
        PairClass::B
    };

    PairClassification {
        class,
        aster_min_fill_qty: aster_min_fill,
        hl_min_hedge_qty: hl_min,
        desired_qty,
    }
}

/// Whether a class is eligible for live trading under `policy` (plan §7.3 / §7.4):
/// - **strict**: only Class A (every fill hedgeable).
/// - **accumulate**: Class A and B (B's sub-min partials flow into pending inventory).
/// - C and D are never eligible.
pub fn is_eligible(class: PairClass, policy: PartialPolicy) -> bool {
    match policy {
        PartialPolicy::StrictEveryFillMustBeHedgeable => class == PairClass::A,
        PartialPolicy::AccumulateSubMin => matches!(class, PairClass::A | PairClass::B),
    }
}

/// Minimum live quote quantity (plan §7.5): large enough that the order clears Aster's
/// min-qty AND min-notional, the HL min hedge, and a configured safety notional — floored
/// to whole steps but never below one step. Returns the quantity the strategy should size
/// up to.
pub fn min_quote_qty(spec: &MarketSpec, ref_px: Decimal, safety_notional: Decimal) -> Decimal {
    if ref_px <= Decimal::ZERO || spec.step <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let rules = HedgeabilityRules {
        hyperliquid_min_notional: spec.hl_min_notional,
        hyperliquid_qty_step: spec.hl_qty_step,
    };
    let by_aster_notional = ceil_to_step(spec.aster_min_notional / ref_px, spec.step);
    let by_safety = ceil_to_step(safety_notional / ref_px, spec.step);
    spec.aster_min_qty
        .max(by_aster_notional)
        .max(hl_min_hedge_qty(&rules, ref_px))
        .max(by_safety)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn spec(step: Decimal, hl_min_notional: Decimal, hl_qty_step: Decimal) -> MarketSpec {
        MarketSpec {
            market_id: "X".into(),
            aster_symbol: "XUSDT".into(),
            hl_coin: "X".into(),
            lighter_market_id: 1,
            lighter_price_decimals: 2,
            lighter_size_decimals: 3,
            lighter_price_tick: dec!(0.01),
            tick: dec!(0.01),
            step,
            aster_min_qty: step,
            aster_min_notional: dec!(5),
            hl_sz_decimals: 3,
            hl_qty_step,
            hl_min_notional,
        }
    }

    #[test]
    fn class_a_when_every_step_hedges() {
        // ref 100, HL min $10 => hl_min 0.1. step 0.1 => a single-step partial (0.1) clears.
        // desired $100 => 1.0 >= 0.1. => Class A.
        let s = spec(dec!(0.1), dec!(10), dec!(0.001));
        let c = classify(&s, dec!(100), dec!(100));
        assert_eq!(c.class, PairClass::A);
        assert!(is_eligible(c.class, PartialPolicy::StrictEveryFillMustBeHedgeable));
    }

    #[test]
    fn class_b_when_partial_can_be_sub_min() {
        // ref 100, HL min $10 => hl_min 0.1. step 0.001 => a 0.001 partial ($0.10) is sub-min.
        // desired $100 => 1.0 >= 0.1 hedgeable in full. => Class B.
        let s = spec(dec!(0.001), dec!(10), dec!(0.001));
        let c = classify(&s, dec!(100), dec!(100));
        assert_eq!(c.class, PairClass::B);
        assert!(!is_eligible(c.class, PartialPolicy::StrictEveryFillMustBeHedgeable));
        assert!(is_eligible(c.class, PartialPolicy::AccumulateSubMin));
    }

    #[test]
    fn class_c_when_quote_below_hl_min() {
        // ref 100, HL min $50 => hl_min 0.5. desired $20 => 0.2 < 0.5 => Class C (un-hedgeable).
        let s = spec(dec!(0.001), dec!(50), dec!(0.001));
        let c = classify(&s, dec!(100), dec!(20));
        assert_eq!(c.class, PairClass::C);
        assert!(!is_eligible(c.class, PartialPolicy::StrictEveryFillMustBeHedgeable));
        assert!(!is_eligible(c.class, PartialPolicy::AccumulateSubMin));
    }

    #[test]
    fn class_d_on_degenerate_spec() {
        let s = spec(dec!(0), dec!(10), dec!(0.001));
        assert_eq!(classify(&s, dec!(100), dec!(100)).class, PairClass::D);
        assert_eq!(classify(&spec(dec!(0.1), dec!(10), dec!(0.001)), dec!(0), dec!(100)).class, PairClass::D);
    }

    #[test]
    fn min_quote_qty_respects_all_floors() {
        // ref 100: aster_min_notional $5 => 0.05; HL min $10 => 0.1; safety $25 => 0.25.
        // step 0.001 => the binding floor is the $25 safety => 0.25.
        let s = spec(dec!(0.001), dec!(10), dec!(0.001));
        assert_eq!(min_quote_qty(&s, dec!(100), dec!(25)), dec!(0.25));
        // With no safety notional, HL min (0.1) binds.
        assert_eq!(min_quote_qty(&s, dec!(100), dec!(0)), dec!(0.1));
    }
}
