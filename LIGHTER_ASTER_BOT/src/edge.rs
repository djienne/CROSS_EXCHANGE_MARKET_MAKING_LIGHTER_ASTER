//! Edge math, in exact `Decimal`. The quote is priced
//! *backward* from the Lighter hedge: given the hedge VWAP and a reference
//! price, find the most aggressive Aster price that still clears the required
//! edge after fees and buffers.
//!
//! Notation: `ref` normalizes edge to bps; `f_a` = Aster maker fee rate; `f_l`
//! = Lighter taker fee rate; `req` = (min_net_profit + slippage + latency + basis +
//! funding) / 10000.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::decimal::{bps_to_rate, rate_to_bps};
use crate::types::Side;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeConfig {
    pub min_net_profit_bps: Decimal,
    pub slippage_buffer_bps: Decimal,
    pub latency_buffer_bps: Decimal,
    pub basis_buffer_bps: Decimal,
    pub funding_buffer_bps: Decimal,
    pub aster_maker_fee_bps: Decimal,
    pub taker_fee_bps: Decimal,
}

impl EdgeConfig {
    /// min_net_profit + all buffers (the edge a quote must clear at placement).
    pub fn required_bps(&self) -> Decimal {
        self.min_net_profit_bps + self.total_buffer_bps()
    }

    /// Just the safety buffers (slippage + latency + basis + funding), excluding
    /// `min_net_profit_bps`. `instant_edge_bps` is net of fees *and* these buffers;
    /// `realized_edge_bps` is net of fees only — so `realized ≈ instant + this` by
    /// construction. The report adds this back to put instant on the same basis as
    /// realized (see `report.rs`).
    pub fn total_buffer_bps(&self) -> Decimal {
        self.slippage_buffer_bps
            + self.latency_buffer_bps
            + self.basis_buffer_bps
            + self.funding_buffer_bps
    }

    pub fn required_rate(&self) -> Decimal {
        bps_to_rate(self.required_bps())
    }

    pub fn aster_maker_fee_rate(&self) -> Decimal {
        bps_to_rate(self.aster_maker_fee_bps)
    }

    pub fn taker_fee_rate(&self) -> Decimal {
        bps_to_rate(self.taker_fee_bps)
    }
}

/// Aster maker buy hedged by a Lighter taker sell: the maximum Aster bid that still
/// satisfies the required edge. `None` if no positive price clears it.
pub fn max_profitable_aster_bid(
    lighter_sell_vwap: Decimal,
    ref_px: Decimal,
    cfg: &EdgeConfig,
) -> Option<Decimal> {
    let one = Decimal::ONE;
    let numerator = lighter_sell_vwap * (one - cfg.taker_fee_rate()) - cfg.required_rate() * ref_px;
    let denominator = one + cfg.aster_maker_fee_rate();
    if denominator <= Decimal::ZERO || numerator <= Decimal::ZERO {
        return None;
    }
    Some(numerator / denominator)
}

/// Aster maker sell hedged by a Lighter taker buy: the minimum Aster ask that still
/// satisfies the required edge.
pub fn min_profitable_aster_ask(
    lighter_buy_vwap: Decimal,
    ref_px: Decimal,
    cfg: &EdgeConfig,
) -> Option<Decimal> {
    let one = Decimal::ONE;
    let numerator = lighter_buy_vwap * (one + cfg.taker_fee_rate()) + cfg.required_rate() * ref_px;
    let denominator = one - cfg.aster_maker_fee_rate();
    if denominator <= Decimal::ZERO || numerator <= Decimal::ZERO {
        return None;
    }
    Some(numerator / denominator)
}

/// Net edge in bps after fees and buffers, but BEFORE subtracting
/// `min_net_profit_bps`. A quote is acceptable iff this is `>= min_net_profit_bps`.
pub fn net_edge_bps_after_fees_and_buffers(
    aster_side: Side,
    aster_px: Decimal,
    lighter_hedge_vwap: Decimal,
    ref_px: Decimal,
    cfg: &EdgeConfig,
) -> Decimal {
    let one = Decimal::ONE;
    let f_a = cfg.aster_maker_fee_rate();
    let f_l = cfg.taker_fee_rate();
    let net_unit = match aster_side {
        Side::Buy => lighter_hedge_vwap * (one - f_l) - aster_px * (one + f_a),
        Side::Sell => aster_px * (one - f_a) - lighter_hedge_vwap * (one + f_l),
    };
    if ref_px <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    rate_to_bps(net_unit / ref_px)
        - cfg.slippage_buffer_bps
        - cfg.latency_buffer_bps
        - cfg.basis_buffer_bps
        - cfg.funding_buffer_bps
}

/// Realized two-leg PnL of a fill + hedge. Quantities are in
/// base units; fees are charged on each leg's notional.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PnlBreakdown {
    pub gross: Decimal,
    pub aster_fee: Decimal,
    pub lighter_fee: Decimal,
    pub net: Decimal,
}

pub fn pnl_breakdown(
    aster_side: Side,
    qty: Decimal,
    aster_fill_px: Decimal,
    lighter_vwap: Decimal,
    cfg: &EdgeConfig,
) -> PnlBreakdown {
    let f_a = cfg.aster_maker_fee_rate();
    let f_l = cfg.taker_fee_rate();
    let gross = match aster_side {
        // Buy Aster, sell Lighter.
        Side::Buy => qty * (lighter_vwap - aster_fill_px),
        // Sell Aster, buy Lighter.
        Side::Sell => qty * (aster_fill_px - lighter_vwap),
    };
    let aster_fee = qty * aster_fill_px * f_a;
    let lighter_fee = qty * lighter_vwap * f_l;
    PnlBreakdown {
        gross,
        aster_fee,
        lighter_fee,
        net: gross - aster_fee - lighter_fee,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn cfg() -> EdgeConfig {
        EdgeConfig {
            min_net_profit_bps: dec!(3.0),
            slippage_buffer_bps: dec!(1.5),
            latency_buffer_bps: dec!(2.0),
            basis_buffer_bps: dec!(1.0),
            funding_buffer_bps: dec!(0.0),
            aster_maker_fee_bps: dec!(1.0),
            taker_fee_bps: dec!(4.5),
        }
    }

    /// The headline invariant: a quote priced exactly at the profitable bound,
    /// fed back through the net-edge formula, yields net == min_net_profit_bps.
    /// Catches any fee or sign error in the whole edge stack.
    #[test]
    fn bid_round_trip() {
        let cfg = cfg();
        let hl_sell_vwap = dec!(100.0);
        let ref_px = dec!(100.0);
        let bound = max_profitable_aster_bid(hl_sell_vwap, ref_px, &cfg).unwrap();
        let net = net_edge_bps_after_fees_and_buffers(Side::Buy, bound, hl_sell_vwap, ref_px, &cfg);
        assert!((net - cfg.min_net_profit_bps).abs() < dec!(0.0001), "net={net}");
    }

    #[test]
    fn ask_round_trip() {
        let cfg = cfg();
        let hl_buy_vwap = dec!(100.0);
        let ref_px = dec!(100.0);
        let bound = min_profitable_aster_ask(hl_buy_vwap, ref_px, &cfg).unwrap();
        let net = net_edge_bps_after_fees_and_buffers(Side::Sell, bound, hl_buy_vwap, ref_px, &cfg);
        assert!((net - cfg.min_net_profit_bps).abs() < dec!(0.0001), "net={net}");
    }

    #[test]
    fn bid_below_bound_increases_edge() {
        // Buying lower than the bound only improves edge.
        let cfg = cfg();
        let (hl, refp) = (dec!(100.0), dec!(100.0));
        let bound = max_profitable_aster_bid(hl, refp, &cfg).unwrap();
        let cheaper = bound - dec!(0.01);
        let net = net_edge_bps_after_fees_and_buffers(Side::Buy, cheaper, hl, refp, &cfg);
        assert!(net > cfg.min_net_profit_bps);
    }

    #[test]
    fn pnl_signs() {
        let cfg = cfg();
        // Bought Aster at 100, hedge sold Lighter at 100.2 => positive gross.
        let p = pnl_breakdown(Side::Buy, dec!(1), dec!(100.0), dec!(100.2), &cfg);
        assert_eq!(p.gross, dec!(0.2));
        assert!(p.net < p.gross); // fees reduce it
        assert!(p.net > dec!(0));
        // Sold Aster at 100.2, hedge bought Lighter at 100.0 => positive gross.
        let s = pnl_breakdown(Side::Sell, dec!(1), dec!(100.2), dec!(100.0), &cfg);
        assert_eq!(s.gross, dec!(0.2));
    }

    /// Finding 1 guardrail: `realized_edge` (net of fees only) sits *exactly*
    /// `total_buffer_bps` above `instant_edge` (net of fees + buffers) on an
    /// unchanged book — so the report's `instant_gross = instant + buffers` is the
    /// basis directly comparable to realized, and realized > instant is expected.
    #[test]
    fn realized_equals_instant_plus_buffers_on_unchanged_book() {
        let cfg = cfg();
        let (px, hv, refp) = (dec!(99.95), dec!(100.0), dec!(100.0));
        for side in [Side::Buy, Side::Sell] {
            let instant = net_edge_bps_after_fees_and_buffers(side, px, hv, refp, &cfg);
            // Mirrors hedge.rs: realized_edge_bps = rate_to_bps(pnl.net / (qty*ref)).
            let pnl = pnl_breakdown(side, dec!(1), px, hv, &cfg);
            let realized = rate_to_bps(pnl.net / (dec!(1) * refp));
            let offset = realized - instant;
            assert!(
                (offset - cfg.total_buffer_bps()).abs() < dec!(0.0001),
                "side={side:?}: realized {realized} - instant {instant} = {offset}, \
                 expected buffers {}",
                cfg.total_buffer_bps()
            );
        }
    }

    #[test]
    fn net_edge_zero_ref_is_safe() {
        let cfg = cfg();
        let edge = net_edge_bps_after_fees_and_buffers(Side::Buy, dec!(100), dec!(100), dec!(0), &cfg);
        assert_eq!(edge, dec!(0));
    }

    #[test]
    fn unprofitable_bound_is_none() {
        let mut cfg = cfg();
        cfg.min_net_profit_bps = dec!(100000); // absurd requirement
        assert!(max_profitable_aster_bid(dec!(100), dec!(100), &cfg).is_none());
    }
}
