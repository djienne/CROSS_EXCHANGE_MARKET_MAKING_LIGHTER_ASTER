//! Pending-inventory accumulation for sub-min-notional fills.
//!
//! Corrected vs the original design: an opposite-direction fill nets the position down and books
//! the realized PnL on the closed quantity (it never silently zeroes the average
//! price). If the residual flips sign it opens fresh at the new fill price. A
//! same-direction fill accumulates with a size-weighted average.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::decimal::ceil_to_step;
use crate::fill_sweep::SimulatedAsterFill;
use crate::types::Side;

#[derive(Debug, Clone)]
pub struct HedgeabilityRules {
    pub hyperliquid_min_notional: Decimal,
    pub hyperliquid_qty_step: Decimal,
}

#[derive(Debug, Clone)]
pub struct PendingInventory {
    /// Positive => net long Aster (hedge by selling HL); negative => net short.
    pub signed_qty: Decimal,
    pub avg_aster_px: Decimal,
    pub first_fill_ts: DateTime<Utc>,
    pub last_fill_ts: DateTime<Utc>,
}

/// Realized PnL booked when an opposite fill closes part of the pending position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NettedRecord {
    pub closed_qty: Decimal,
    pub open_px: Decimal,
    pub close_px: Decimal,
    pub realized_pnl: Decimal,
}

/// A hedge that should now be sent (the net inventory reached hedgeable size).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HedgeOrder {
    pub hedge_side: Side,
    pub qty: Decimal,
    pub avg_aster_px: Decimal,
}

#[derive(Debug, Clone)]
pub struct FillOutcome {
    /// Inventory still pending after this fill (None if flushed to a hedge or zeroed).
    pub pending: Option<PendingInventory>,
    /// Realized PnL booked from netting, if any.
    pub netted: Option<NettedRecord>,
    /// A hedge to schedule, if the net inventory became hedgeable.
    pub hedge: Option<HedgeOrder>,
    /// Notional now sitting in pending inventory (set when accumulating).
    pub accumulated_notional: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingRiskKind {
    TooOld,
    TooLarge,
}

impl PendingRiskKind {
    pub fn as_str(self) -> &'static str {
        match self {
            PendingRiskKind::TooOld => "PENDING_INVENTORY_TOO_OLD",
            PendingRiskKind::TooLarge => "PENDING_INVENTORY_TOO_LARGE",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PendingRiskEvent {
    pub kind: PendingRiskKind,
    pub signed_qty: Decimal,
    pub avg_aster_px: Decimal,
    pub mark_px: Decimal,
    pub notional: Decimal,
    pub mark_to_market_pnl: Decimal,
}

/// Minimum hedgeable quantity on HL for a given reference price ($10 min notional
/// rounded up to the size step, but at least one step).
pub fn hl_min_hedge_qty(rules: &HedgeabilityRules, ref_px: Decimal) -> Decimal {
    if ref_px <= Decimal::ZERO {
        return rules.hyperliquid_qty_step;
    }
    let by_notional = ceil_to_step(rules.hyperliquid_min_notional / ref_px, rules.hyperliquid_qty_step);
    by_notional.max(rules.hyperliquid_qty_step)
}

#[inline]
pub fn signed_aster_qty(side: Side, qty: Decimal) -> Decimal {
    match side {
        Side::Buy => qty,
        Side::Sell => -qty,
    }
}

/// Hedge side for a signed inventory: long Aster -> sell HL; short Aster -> buy HL.
pub fn hedge_side_for_signed(signed_qty: Decimal) -> Option<Side> {
    if signed_qty > Decimal::ZERO {
        Some(Side::Sell)
    } else if signed_qty < Decimal::ZERO {
        Some(Side::Buy)
    } else {
        None
    }
}

/// Fold a fill into pending inventory, returning what to book / hedge / keep.
pub fn handle_fill(
    fill: &SimulatedAsterFill,
    pending: Option<PendingInventory>,
    rules: &HedgeabilityRules,
    ref_px: Decimal,
    aster_maker_fee_rate: Decimal,
) -> FillOutcome {
    handle_fill_parts(
        fill.aster_side,
        fill.fill_qty,
        fill.fill_px,
        fill.local_recv_ts,
        pending,
        rules,
        ref_px,
        aster_maker_fee_rate,
    )
}

/// The core accumulation logic, in primitive params so BOTH the dry-run sim (via
/// [`handle_fill`]) and the LIVE strategy (which has a venue [`AsterFill`](crate::livebot::fills::AsterFill),
/// not a `SimulatedAsterFill`) share one exact implementation. A same-direction fill accumulates
/// with a size-weighted average; an opposite fill nets down and books realized PnL; the result
/// carries a [`HedgeOrder`] the MOMENT the net clears the HL minimum (primary fast-hedge path),
/// else keeps the sub-min residual pending — never per-partial flattening.
#[allow(clippy::too_many_arguments)]
pub fn handle_fill_parts(
    aster_side: Side,
    fill_qty: Decimal,
    fill_px: Decimal,
    recv_ts: DateTime<Utc>,
    pending: Option<PendingInventory>,
    rules: &HedgeabilityRules,
    ref_px: Decimal,
    aster_maker_fee_rate: Decimal,
) -> FillOutcome {
    let fill_signed = signed_aster_qty(aster_side, fill_qty);

    let mut inv = pending.unwrap_or(PendingInventory {
        signed_qty: Decimal::ZERO,
        avg_aster_px: fill_px,
        first_fill_ts: recv_ts,
        last_fill_ts: recv_ts,
    });

    let mut netted = None;

    let same_dir = inv.signed_qty == Decimal::ZERO
        || (inv.signed_qty > Decimal::ZERO) == (fill_signed > Decimal::ZERO);
    if same_dir {
        // Size-weighted average accumulation.
        let old_abs = inv.signed_qty.abs();
        let new_abs = fill_signed.abs();
        let total = old_abs + new_abs;
        if total > Decimal::ZERO {
            inv.avg_aster_px = (inv.avg_aster_px * old_abs + fill_px * new_abs) / total;
        }
        inv.signed_qty += fill_signed;
    } else {
        // Opposite fill: close as much as possible, book realized PnL.
        let closed = inv.signed_qty.abs().min(fill_signed.abs());
        let gross = if inv.signed_qty > Decimal::ZERO {
            // Was long, this fill sells: profit if closing above entry.
            closed * (fill_px - inv.avg_aster_px)
        } else {
            // Was short, this fill buys: profit if closing below entry.
            closed * (inv.avg_aster_px - fill_px)
        };
        let fees = closed * inv.avg_aster_px * aster_maker_fee_rate
            + closed * fill_px * aster_maker_fee_rate;
        netted = Some(NettedRecord {
            closed_qty: closed,
            open_px: inv.avg_aster_px,
            close_px: fill_px,
            realized_pnl: gross - fees,
        });

        let new_signed = inv.signed_qty + fill_signed;
        if new_signed == Decimal::ZERO {
            inv.signed_qty = Decimal::ZERO;
        } else if (new_signed > Decimal::ZERO) == (inv.signed_qty > Decimal::ZERO) {
            // Partial close, residual on the original side: average unchanged.
            inv.signed_qty = new_signed;
        } else {
            // Flipped: residual opens fresh at this fill price.
            inv.signed_qty = new_signed;
            inv.avg_aster_px = fill_px;
        }
    }
    inv.last_fill_ts = recv_ts;

    let abs = inv.signed_qty.abs();
    if abs == Decimal::ZERO {
        return FillOutcome {
            pending: None,
            netted,
            hedge: None,
            accumulated_notional: None,
        };
    }

    let min_hedge = hl_min_hedge_qty(rules, ref_px);
    if abs >= min_hedge {
        let hedge = HedgeOrder {
            hedge_side: hedge_side_for_signed(inv.signed_qty).expect("non-zero inventory"),
            qty: abs,
            avg_aster_px: inv.avg_aster_px,
        };
        FillOutcome {
            pending: None,
            netted,
            hedge: Some(hedge),
            accumulated_notional: None,
        }
    } else {
        FillOutcome {
            pending: Some(inv),
            netted,
            hedge: None,
            accumulated_notional: Some(abs * ref_px),
        }
    }
}

/// Flag pending inventory that has aged out or grown too large, with a
/// mark-to-market PnL. The caller should record the event and clear inventory.
pub fn check_pending_limits(
    inv: &PendingInventory,
    max_pending_notional: Decimal,
    max_pending_age_ms: i64,
    mark_px: Decimal,
    now: DateTime<Utc>,
) -> Option<PendingRiskEvent> {
    let abs = inv.signed_qty.abs();
    if abs == Decimal::ZERO {
        return None;
    }
    let notional = abs * mark_px;
    let age_ms = (now - inv.first_fill_ts).num_milliseconds();
    let mtm = if inv.signed_qty > Decimal::ZERO {
        abs * (mark_px - inv.avg_aster_px)
    } else {
        abs * (inv.avg_aster_px - mark_px)
    };
    let kind = if notional > max_pending_notional {
        PendingRiskKind::TooLarge
    } else if age_ms > max_pending_age_ms {
        PendingRiskKind::TooOld
    } else {
        return None;
    };
    Some(PendingRiskEvent {
        kind,
        signed_qty: inv.signed_qty,
        avg_aster_px: inv.avg_aster_px,
        mark_px,
        notional,
        mark_to_market_pnl: mtm,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MarketId;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn rules() -> HedgeabilityRules {
        HedgeabilityRules {
            hyperliquid_min_notional: dec!(10),
            hyperliquid_qty_step: dec!(0.001),
        }
    }

    fn fill(side: Side, px: Decimal, qty: Decimal) -> SimulatedAsterFill {
        SimulatedAsterFill {
            id: Uuid::new_v4(),
            quote_id: Uuid::new_v4(),
            market: MarketId("BTC".into()),
            aster_side: side,
            fill_px: px,
            fill_qty: qty,
            sweep_print_px: px,
            quoted_edge_bps: dec!(0),
            quoted_distance_bps: dec!(0),
            remaining_quote_qty_after_fill: dec!(0),
            was_trade_through: false,
            was_partial: false,
            feed_stale_at_fill: false,
            queue_truncated: false,
            exch_ts: ts(),
            local_recv_ts: ts(),
        }
    }

    #[test]
    fn min_hedge_qty_from_notional() {
        // ref 100, min $10 => 0.1 base, step 0.001.
        assert_eq!(hl_min_hedge_qty(&rules(), dec!(100)), dec!(0.1));
    }

    #[test]
    fn sub_min_accumulates_then_hedges() {
        // ref 100 => min hedge 0.1.
        let o1 = handle_fill(&fill(Side::Buy, dec!(100), dec!(0.05)), None, &rules(), dec!(100), dec!(0));
        assert!(o1.hedge.is_none());
        let inv = o1.pending.unwrap();
        assert_eq!(inv.signed_qty, dec!(0.05));

        let o2 = handle_fill(&fill(Side::Buy, dec!(100), dec!(0.06)), Some(inv), &rules(), dec!(100), dec!(0));
        let h = o2.hedge.unwrap();
        assert_eq!(h.hedge_side, Side::Sell); // long Aster => sell HL
        assert_eq!(h.qty, dec!(0.11));
        assert!(o2.pending.is_none());
    }

    #[test]
    fn opposite_fill_books_pnl_and_keeps_residual() {
        // Pending long 0.08 @ 100 (sub-min).
        let inv = handle_fill(&fill(Side::Buy, dec!(100), dec!(0.08)), None, &rules(), dec!(100), dec!(0))
            .pending
            .unwrap();
        // Opposite sell 0.05 @ 101 closes 0.05 for +0.05 gross.
        let o = handle_fill(&fill(Side::Sell, dec!(101), dec!(0.05)), Some(inv), &rules(), dec!(100), dec!(0));
        let n = o.netted.unwrap();
        assert_eq!(n.closed_qty, dec!(0.05));
        assert_eq!(n.realized_pnl, dec!(0.05));
        let resid = o.pending.unwrap();
        assert_eq!(resid.signed_qty, dec!(0.03)); // still long
        assert_eq!(resid.avg_aster_px, dec!(100)); // average unchanged
    }

    #[test]
    fn opposite_fill_flips_and_hedges() {
        let inv = handle_fill(&fill(Side::Buy, dec!(100), dec!(0.08)), None, &rules(), dec!(100), dec!(0))
            .pending
            .unwrap();
        // Big opposite sell 0.2 @ 101: closes 0.08 (+0.08), flips to short 0.12 @ 101.
        let o = handle_fill(&fill(Side::Sell, dec!(101), dec!(0.2)), Some(inv), &rules(), dec!(100), dec!(0));
        assert_eq!(o.netted.unwrap().realized_pnl, dec!(0.08));
        let h = o.hedge.unwrap();
        assert_eq!(h.hedge_side, Side::Buy); // short Aster => buy HL
        assert_eq!(h.qty, dec!(0.12));
        assert_eq!(h.avg_aster_px, dec!(101)); // fresh at flip price
    }

    #[test]
    fn pending_marks_to_market_when_too_old() {
        let inv = PendingInventory {
            signed_qty: dec!(0.05),
            avg_aster_px: dec!(100),
            first_fill_ts: ts(),
            last_fill_ts: ts(),
        };
        let now = ts() + chrono::Duration::milliseconds(2_000);
        let e = check_pending_limits(&inv, dec!(25), 1_000, dec!(99), now).unwrap();
        assert_eq!(e.kind, PendingRiskKind::TooOld);
        assert_eq!(e.mark_to_market_pnl, dec!(-0.05)); // long marked down
    }
}
