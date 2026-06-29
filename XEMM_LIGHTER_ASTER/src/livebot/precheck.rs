//! Fast integer precheck: emit cheap, risk-reducing cancels before falling through to the
//! exact Decimal quote engine. It deliberately does NOT fast-hold: a top-of-book distance
//! check cannot prove edge after fees, buffers, rounding, and hedge depth/VWAP.

use crate::hot_types::HotBook;
use crate::types::Side;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HotPrecheck {
    CancelFast(&'static str),
    NeedExactQuote,
}

#[derive(Debug, Clone, Copy)]
pub struct HotCurrentOrder {
    pub px_ticks: i64,
    pub qty_lots: i64,
}

#[derive(Debug, Clone)]
pub struct HotPrecheckConfig {
    pub max_book_stale_ns: i64,
    pub requote_threshold_ticks: i64,
}

pub fn hot_precheck_side(
    aster: &HotBook,
    hl: &HotBook,
    side: Side,
    current: Option<HotCurrentOrder>,
    now_ns: i64,
    cfg: &HotPrecheckConfig,
) -> HotPrecheck {
    let aster_touch = match side {
        Side::Buy => aster.best_bid_ticks(),
        Side::Sell => aster.best_ask_ticks(),
    };
    // Maker Buy hedges by selling into HL bids; Maker Sell hedges by buying HL asks.
    let hl_hedge_touch = match side {
        Side::Buy => hl.best_bid_ticks(),
        Side::Sell => hl.best_ask_ticks(),
    };
    if aster_touch.is_none() || hl_hedge_touch.is_none() {
        return HotPrecheck::CancelFast("missing_bbo");
    }

    if aster.is_crossed() || hl.is_crossed() {
        return HotPrecheck::CancelFast("crossed");
    }

    if now_ns.saturating_sub(aster.recv_ns) > cfg.max_book_stale_ns {
        return HotPrecheck::CancelFast("stale_aster");
    }
    if now_ns.saturating_sub(hl.recv_ns) > cfg.max_book_stale_ns {
        return HotPrecheck::CancelFast("stale_hl");
    }

    let current = match current {
        Some(c) => c,
        None => return HotPrecheck::NeedExactQuote,
    };

    match side {
        Side::Buy => {
            if let Some(ask) = aster.best_ask_ticks() {
                if current.px_ticks >= ask {
                    return HotPrecheck::CancelFast("would_cross_postonly");
                }
            }
        }
        Side::Sell => {
            if let Some(bid) = aster.best_bid_ticks() {
                if current.px_ticks <= bid {
                    return HotPrecheck::CancelFast("would_cross_postonly");
                }
            }
        }
    }

    // Keep these reads for cheap diagnostics / future conservative fast-hold work, but do not
    // fast-hold today. Holding must still pass `evaluate_side` so profitability and depth are
    // checked with the exact Decimal engine.
    let _aster_move = aster_touch.unwrap().abs_diff(current.px_ticks) as i64;
    let _hl_move = hl_hedge_touch.unwrap().abs_diff(current.px_ticks) as i64;
    let _threshold = cfg.requote_threshold_ticks;

    HotPrecheck::NeedExactQuote
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot_types::*;
    use crate::livebot::scale::{MarketScale, build_hot_book};
    use crate::book::OrderBook;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    fn scale() -> MarketScale {
        MarketScale { tick: dec!(0.1), step: dec!(0.001), hl_qty_step: dec!(0.001) }
    }

    fn hot(bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)], recv_ns: i64) -> HotBook {
        let now = Utc::now();
        let book = OrderBook::from_levels(
            bids.iter().map(|&(p, q)| (p, q)),
            asks.iter().map(|&(p, q)| (p, q)),
            now, now,
        );
        build_hot_book(&book, &scale(), 1, recv_ns)
    }

    fn pcfg() -> HotPrecheckConfig {
        HotPrecheckConfig { max_book_stale_ns: 5_000_000_000, requote_threshold_ticks: 2 }
    }

    #[test]
    fn missing_bbo_cancels() {
        let aster = hot(&[], &[(dec!(101), dec!(1))], 0);
        let hl = hot(&[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))], 0);
        assert_eq!(
            hot_precheck_side(&aster, &hl, Side::Buy, None, 0, &pcfg()),
            HotPrecheck::CancelFast("missing_bbo"),
        );
    }

    #[test]
    fn crossed_cancels() {
        let aster = hot(&[(dec!(101), dec!(1))], &[(dec!(100), dec!(1))], 0);
        let hl = hot(&[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))], 0);
        assert_eq!(
            hot_precheck_side(&aster, &hl, Side::Buy, Some(HotCurrentOrder { px_ticks: 1000, qty_lots: 10 }), 0, &pcfg()),
            HotPrecheck::CancelFast("crossed"),
        );
    }

    #[test]
    fn stale_aster_cancels() {
        let now_ns = 10_000_000_000i64;
        let aster = hot(&[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))], 0);
        let hl = hot(&[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))], now_ns);
        assert_eq!(
            hot_precheck_side(&aster, &hl, Side::Buy, Some(HotCurrentOrder { px_ticks: 1000, qty_lots: 10 }), now_ns, &pcfg()),
            HotPrecheck::CancelFast("stale_aster"),
        );
    }

    #[test]
    fn stale_hl_cancels() {
        let now_ns = 10_000_000_000i64;
        let aster = hot(&[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))], now_ns);
        let hl = hot(&[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))], 0);
        assert_eq!(
            hot_precheck_side(&aster, &hl, Side::Buy, Some(HotCurrentOrder { px_ticks: 1000, qty_lots: 10 }), now_ns, &pcfg()),
            HotPrecheck::CancelFast("stale_hl"),
        );
    }

    #[test]
    fn no_current_order_needs_exact() {
        let now_ns = 100;
        let aster = hot(&[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))], now_ns);
        let hl = hot(&[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))], now_ns);
        assert_eq!(
            hot_precheck_side(&aster, &hl, Side::Buy, None, now_ns, &pcfg()),
            HotPrecheck::NeedExactQuote,
        );
    }

    #[test]
    fn would_cross_postonly_cancels_buy() {
        let now_ns = 100;
        let aster = hot(&[(dec!(100), dec!(1))], &[(dec!(100.1), dec!(1))], now_ns);
        let hl = hot(&[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))], now_ns);
        assert_eq!(
            hot_precheck_side(&aster, &hl, Side::Buy, Some(HotCurrentOrder { px_ticks: 1001, qty_lots: 10 }), now_ns, &pcfg()),
            HotPrecheck::CancelFast("would_cross_postonly"),
        );
    }

    #[test]
    fn would_cross_postonly_cancels_sell() {
        let now_ns = 100;
        let aster = hot(&[(dec!(100), dec!(1))], &[(dec!(100.1), dec!(1))], now_ns);
        let hl = hot(&[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))], now_ns);
        assert_eq!(
            hot_precheck_side(&aster, &hl, Side::Sell, Some(HotCurrentOrder { px_ticks: 1000, qty_lots: 10 }), now_ns, &pcfg()),
            HotPrecheck::CancelFast("would_cross_postonly"),
        );
    }

    #[test]
    fn small_move_still_needs_exact_quote() {
        let now_ns = 100;
        let aster = hot(&[(dec!(100), dec!(1))], &[(dec!(100.1), dec!(1))], now_ns);
        let hl = hot(&[(dec!(99.9), dec!(1))], &[(dec!(100.1), dec!(1))], now_ns);
        assert_eq!(
            hot_precheck_side(&aster, &hl, Side::Buy, Some(HotCurrentOrder { px_ticks: 1000, qty_lots: 10 }), now_ns, &pcfg()),
            HotPrecheck::NeedExactQuote,
        );
    }

    #[test]
    fn large_move_needs_exact() {
        let now_ns = 100;
        let aster = hot(&[(dec!(100.5), dec!(1))], &[(dec!(101), dec!(1))], now_ns);
        let hl = hot(&[(dec!(100), dec!(1))], &[(dec!(101), dec!(1))], now_ns);
        assert_eq!(
            hot_precheck_side(&aster, &hl, Side::Buy, Some(HotCurrentOrder { px_ticks: 1000, qty_lots: 10 }), now_ns, &pcfg()),
            HotPrecheck::NeedExactQuote,
        );
    }
}
