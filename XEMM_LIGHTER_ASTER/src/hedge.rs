//! Resolve a hedge against the HL book at `fill_ts + latency_bucket` and compute
//! realized two-leg PnL. One `PendingHedge` is created per
//! configured latency bucket per fill, so the report can show edge decay.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

use crate::book::OrderBook;
use crate::decimal::rate_to_bps;
use crate::edge::{pnl_breakdown, EdgeConfig};
use crate::types::{MarketId, QueueModel, Side};
use crate::vwap::vwap_take_partial;

#[derive(Debug, Clone)]
pub struct PendingHedge {
    pub id: Uuid,
    pub fill_id: Uuid,
    pub market: MarketId,
    pub queue_model: QueueModel,
    /// Side of the HL hedge (opposite of the Aster maker side).
    pub hedge_side: Side,
    pub qty: Decimal,
    /// Size-weighted Aster fill price being hedged.
    pub aster_ref_px: Decimal,
    pub fill_local_ts: DateTime<Utc>,
    pub resolve_at: DateTime<Utc>,
    pub latency_bucket_ms: i64,
}

#[derive(Debug, Clone)]
pub struct HedgeResult {
    pub id: Uuid,
    pub fill_id: Uuid,
    pub market: MarketId,
    pub queue_model: QueueModel,
    pub hedge_side: Side,
    /// Requested/dispatched hedge size.
    pub qty: Decimal,
    /// Size that actually filled against the HL book this resolution (<= `qty`
    /// when the book was depth-exhausted; 0 when no book existed on that side).
    pub filled_qty: Decimal,
    pub aster_fill_px: Decimal,
    pub hl_vwap: Decimal,
    pub latency_bucket_ms: i64,
    pub gross_pnl: Decimal,
    pub aster_fee: Decimal,
    pub hl_fee: Decimal,
    pub net_pnl: Decimal,
    pub realized_edge_bps: Decimal,
    pub hl_slippage_bps: Decimal,
    pub depth_exhausted: bool,
    pub hedged_on_stale_book: bool,
    pub fill_local_ts: DateTime<Utc>,
    pub resolve_ts: DateTime<Utc>,
    pub hl_book_ts: DateTime<Utc>,
}

/// Resolve `ph` against `hl_book` (the HL state at/after `resolve_at`).
/// `hedged_on_stale_book` is decided by the caller (book older than tolerance).
pub fn resolve_hedge(
    ph: &PendingHedge,
    hl_book: &OrderBook,
    edge: &EdgeConfig,
    hedged_on_stale_book: bool,
) -> HedgeResult {
    let aster_side = ph.hedge_side.opposite(); // the maker side that was filled
    let two = Decimal::from(2);

    // Resolve against whatever HL depth exists. PnL and realized edge are computed
    // on the qty that ACTUALLY fills (`filled_qty`), never the full request: a thin
    // book that fills only part of the hedge must not book full-size edge. The
    // unfilled remainder (`qty - filled_qty`) is left unhedged and surfaced in the
    // report (under-hedged volume).
    let (hl_vwap, slippage_bps, filled_qty, depth_exhausted) =
        match vwap_take_partial(hl_book, ph.hedge_side, ph.qty) {
            Some(r) => (r.vwap, r.slippage_bps, r.filled_qty, r.exhausted),
            // Empty book on that side: nothing fills. Flag exhausted; PnL is zero on a
            // zero fill (ref px is only a display placeholder for `hl_vwap`).
            None => (ph.aster_ref_px, Decimal::ZERO, Decimal::ZERO, true),
        };

    let pnl = pnl_breakdown(aster_side, filled_qty, ph.aster_ref_px, hl_vwap, edge);
    let hl_mid = hl_book.mid().unwrap_or(hl_vwap);
    let ref_px = (ph.aster_ref_px + hl_mid) / two;
    let realized_edge_bps = if ref_px > Decimal::ZERO && filled_qty > Decimal::ZERO {
        rate_to_bps(pnl.net / (filled_qty * ref_px))
    } else {
        Decimal::ZERO
    };

    HedgeResult {
        id: Uuid::new_v4(),
        fill_id: ph.fill_id,
        market: ph.market.clone(),
        queue_model: ph.queue_model,
        hedge_side: ph.hedge_side,
        qty: ph.qty,
        filled_qty,
        aster_fill_px: ph.aster_ref_px,
        hl_vwap,
        latency_bucket_ms: ph.latency_bucket_ms,
        gross_pnl: pnl.gross,
        aster_fee: pnl.aster_fee,
        hl_fee: pnl.lighter_fee,
        net_pnl: pnl.net,
        realized_edge_bps,
        hl_slippage_bps: slippage_bps,
        depth_exhausted,
        hedged_on_stale_book,
        fill_local_ts: ph.fill_local_ts,
        resolve_ts: ph.resolve_at,
        hl_book_ts: hl_book.local_recv_ts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn edge() -> EdgeConfig {
        EdgeConfig {
            min_net_profit_bps: dec!(3.0),
            slippage_buffer_bps: dec!(1.5),
            latency_buffer_bps: dec!(2.0),
            basis_buffer_bps: dec!(1.0),
            funding_buffer_bps: dec!(0.0),
            aster_maker_fee_bps: dec!(0.0),
            taker_fee_bps: dec!(0.0),
        }
    }

    #[test]
    fn hedge_buy_aster_sell_hl_positive_pnl() {
        // Bought Aster at 100; hedge sells into HL bids at 100.2.
        let hl = OrderBook::from_levels(
            vec![(dec!(100.2), dec!(10))],
            vec![(dec!(100.3), dec!(10))],
            ts(),
            ts(),
        );
        let ph = PendingHedge {
            id: Uuid::new_v4(),
            fill_id: Uuid::new_v4(),
            market: "BTC".into(),
            queue_model: QueueModel::Optimistic,
            hedge_side: Side::Sell,
            qty: dec!(1),
            aster_ref_px: dec!(100),
            fill_local_ts: ts(),
            resolve_at: ts(),
            latency_bucket_ms: 50,
        };
        let r = resolve_hedge(&ph, &hl, &edge(), false);
        assert_eq!(r.gross_pnl, dec!(0.2));
        assert_eq!(r.net_pnl, dec!(0.2)); // zero fees in this cfg
        assert!(!r.depth_exhausted);
        assert!(r.realized_edge_bps > dec!(0));
    }

    #[test]
    fn thin_book_flags_exhausted() {
        let hl = OrderBook::from_levels(
            vec![(dec!(100.2), dec!(0.1))],
            vec![(dec!(100.3), dec!(0.1))],
            ts(),
            ts(),
        );
        let ph = PendingHedge {
            id: Uuid::new_v4(),
            fill_id: Uuid::new_v4(),
            market: "BTC".into(),
            queue_model: QueueModel::Optimistic,
            hedge_side: Side::Sell,
            qty: dec!(1),
            aster_ref_px: dec!(100),
            fill_local_ts: ts(),
            resolve_at: ts(),
            latency_bucket_ms: 50,
        };
        let r = resolve_hedge(&ph, &hl, &edge(), false);
        assert!(r.depth_exhausted);
        // PnL is computed on what actually filled (0.1), NOT the full request (1.0).
        assert_eq!(r.qty, dec!(1));
        assert_eq!(r.filled_qty, dec!(0.1));
        assert_eq!(r.gross_pnl, dec!(0.02)); // 0.1 * (100.2 - 100), not 1.0 * 0.2
    }

    #[test]
    fn empty_book_side_books_no_pnl() {
        // No bids at all: a Sell hedge cannot fill. filled_qty = 0, PnL = 0, flagged
        // exhausted — never a phantom full-size hedge at the Aster reference price.
        let hl = OrderBook::from_levels(vec![], vec![(dec!(100.3), dec!(5))], ts(), ts());
        let ph = PendingHedge {
            id: Uuid::new_v4(),
            fill_id: Uuid::new_v4(),
            market: "BTC".into(),
            queue_model: QueueModel::Optimistic,
            hedge_side: Side::Sell,
            qty: dec!(1),
            aster_ref_px: dec!(100),
            fill_local_ts: ts(),
            resolve_at: ts(),
            latency_bucket_ms: 50,
        };
        let r = resolve_hedge(&ph, &hl, &edge(), false);
        assert!(r.depth_exhausted);
        assert_eq!(r.filled_qty, dec!(0));
        assert_eq!(r.net_pnl, dec!(0));
        assert_eq!(r.realized_edge_bps, dec!(0));
        assert_eq!(r.qty, dec!(1)); // requested still recorded
    }
}
