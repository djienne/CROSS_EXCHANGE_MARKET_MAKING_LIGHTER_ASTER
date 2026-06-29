//! Volume-weighted average price for a simulated taker order walking the book.
//! `vwap_take` requires the full quantity to be fillable (used when pricing a
//! quote); `vwap_take_partial` resolves against whatever depth exists and flags
//! exhaustion (used when resolving a hedge against a possibly-thin book).

use rust_decimal::Decimal;

use crate::book::OrderBook;
use crate::decimal::rate_to_bps;
use crate::types::Side;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VwapResult {
    pub target_qty: Decimal,
    pub vwap: Decimal,
    pub filled_qty: Decimal,
    pub worst_px: Decimal,
    pub levels_used: usize,
    /// Slippage of the VWAP from the touch on the taken side, in bps (>= 0).
    pub slippage_bps: Decimal,
    /// True if the book ran out before the requested quantity was filled.
    pub exhausted: bool,
}

/// Take `qty` from the side a taker would consume (`Buy` lifts asks, `Sell`
/// hits bids). Returns `None` unless the full quantity is fillable.
pub fn vwap_take(book: &OrderBook, take_side: Side, qty: Decimal) -> Option<VwapResult> {
    let r = vwap_take_partial(book, take_side, qty)?;
    if r.exhausted {
        None
    } else {
        Some(r)
    }
}

/// Like [`vwap_take`] but returns a partial fill (with `exhausted = true`) when
/// the book is too thin. Returns `None` if the relevant side is empty, `qty <= 0`,
/// or the touch price is non-positive (a garbage book — would divide-by-zero below).
pub fn vwap_take_partial(book: &OrderBook, take_side: Side, qty: Decimal) -> Option<VwapResult> {
    if qty <= Decimal::ZERO {
        return None;
    }
    let levels = match take_side {
        Side::Buy => &book.asks,  // buying lifts asks
        Side::Sell => &book.bids, // selling hits bids
    };
    let touch = levels.first()?.px;
    if touch <= Decimal::ZERO {
        return None; // garbage book; touch is the slippage divisor below
    }

    let mut remaining = qty;
    let mut notional = Decimal::ZERO;
    let mut filled = Decimal::ZERO;
    let mut worst_px = touch;
    let mut levels_used = 0;
    for lvl in levels {
        if remaining <= Decimal::ZERO {
            break;
        }
        let take = remaining.min(lvl.qty);
        notional += take * lvl.px;
        filled += take;
        remaining -= take;
        worst_px = lvl.px;
        levels_used += 1;
    }
    if filled <= Decimal::ZERO {
        return None;
    }
    let vwap = notional / filled;
    // Slippage from touch, always non-negative on the taken side.
    let slip_rate = match take_side {
        Side::Buy => (vwap - touch) / touch,
        Side::Sell => (touch - vwap) / touch,
    };
    Some(VwapResult {
        target_qty: qty,
        vwap,
        filled_qty: filled,
        worst_px,
        levels_used,
        slippage_bps: rate_to_bps(slip_rate),
        exhausted: remaining > Decimal::ZERO,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use rust_decimal_macros::dec;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn book() -> OrderBook {
        OrderBook::from_levels(
            vec![(dec!(100.0), dec!(2)), (dec!(99.0), dec!(10))],
            vec![(dec!(101.0), dec!(2)), (dec!(102.0), dec!(10))],
            ts(),
            ts(),
        )
    }

    #[test]
    fn exact_single_level() {
        let r = vwap_take(&book(), Side::Buy, dec!(2)).unwrap();
        assert_eq!(r.target_qty, dec!(2));
        assert_eq!(r.vwap, dec!(101.0));
        assert_eq!(r.worst_px, dec!(101.0));
        assert_eq!(r.levels_used, 1);
        assert_eq!(r.slippage_bps, dec!(0));
        assert!(!r.exhausted);
    }

    #[test]
    fn multi_level_vwap_and_slippage() {
        // Buy 4: 2@101 + 2@102 => vwap 101.5; slippage vs touch 101 = 0.5/101.
        let r = vwap_take(&book(), Side::Buy, dec!(4)).unwrap();
        assert_eq!(r.vwap, dec!(101.5));
        assert_eq!(r.worst_px, dec!(102.0));
        assert_eq!(r.levels_used, 2);
        let expected = (dec!(101.5) - dec!(101.0)) / dec!(101.0) * dec!(10000);
        assert_eq!(r.slippage_bps, expected);

        // Sell 4: 2@100 + 2@99 => vwap 99.5; slippage vs touch 100 = 0.5/100 = 50 bps.
        let s = vwap_take(&book(), Side::Sell, dec!(4)).unwrap();
        assert_eq!(s.vwap, dec!(99.5));
        assert_eq!(s.worst_px, dec!(99.0));
        assert_eq!(s.levels_used, 2);
        assert_eq!(s.slippage_bps, dec!(50));
    }

    #[test]
    fn exhausted_returns_none_for_strict_but_some_for_partial() {
        assert!(vwap_take(&book(), Side::Buy, dec!(100)).is_none());
        let p = vwap_take_partial(&book(), Side::Buy, dec!(100)).unwrap();
        assert!(p.exhausted);
        assert_eq!(p.filled_qty, dec!(12)); // 2 + 10 available
        assert_eq!(p.worst_px, dec!(102.0));
        assert_eq!(p.levels_used, 2);
    }
}
