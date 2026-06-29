use arrayvec::ArrayVec;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::types::Side;

pub const MAX_BOOK_LEVELS: usize = 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub px: Decimal,
    pub qty: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthQuote {
    pub side: Side,
    pub target_qty: Decimal,
    pub available_qty: Decimal,
    pub vwap_px: Decimal,
    pub worst_px: Decimal,
    pub best_px: Decimal,
    pub best_qty: Decimal,
    pub levels_used: usize,
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: ArrayVec<Level, MAX_BOOK_LEVELS>,
    pub asks: ArrayVec<Level, MAX_BOOK_LEVELS>,
    pub exch_ts: DateTime<Utc>,
    pub local_recv_ts: DateTime<Utc>,
}

impl OrderBook {
    pub fn from_levels(
        bids: impl IntoIterator<Item = (Decimal, Decimal)>,
        asks: impl IntoIterator<Item = (Decimal, Decimal)>,
        exch_ts: DateTime<Utc>,
        local_recv_ts: DateTime<Utc>,
    ) -> Self {
        OrderBook {
            bids: build_side(bids, true),
            asks: build_side(asks, false),
            exch_ts,
            local_recv_ts,
        }
    }

    pub fn best_bid(&self) -> Option<Level> {
        self.bids.first().copied()
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.asks.first().copied()
    }

    pub fn mid(&self) -> Option<Decimal> {
        Some((self.best_bid()?.px + self.best_ask()?.px) / Decimal::from(2))
    }

    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => b.px >= a.px,
            _ => false,
        }
    }

    pub fn age_ms(&self, now: DateTime<Utc>) -> i64 {
        (now - self.local_recv_ts).num_milliseconds()
    }

    pub fn side_levels(&self, side: Side) -> &[Level] {
        match side {
            Side::Buy => &self.asks,
            Side::Sell => &self.bids,
        }
    }

    pub fn cumulative_qty(&self, side: Side, max_levels: usize) -> Decimal {
        self.side_levels(side)
            .iter()
            .take(max_levels.min(MAX_BOOK_LEVELS))
            .fold(Decimal::ZERO, |acc, level| acc + level.qty)
    }

    pub fn depth_vwap(
        &self,
        side: Side,
        target_qty: Decimal,
        max_levels: usize,
    ) -> Option<DepthQuote> {
        if target_qty <= Decimal::ZERO || max_levels == 0 {
            return None;
        }
        let levels = self.side_levels(side);
        let best = levels.first().copied()?;
        let mut remaining = target_qty;
        let mut filled = Decimal::ZERO;
        let mut notional = Decimal::ZERO;
        let mut available = Decimal::ZERO;
        let mut worst_px = best.px;
        let mut levels_used = 0usize;
        for level in levels.iter().take(max_levels.min(MAX_BOOK_LEVELS)) {
            available += level.qty;
            if remaining > Decimal::ZERO {
                let take = level.qty.min(remaining);
                if take > Decimal::ZERO {
                    filled += take;
                    notional += take * level.px;
                    remaining -= take;
                    worst_px = level.px;
                    levels_used += 1;
                }
            }
        }
        if filled < target_qty {
            return None;
        }
        Some(DepthQuote {
            side,
            target_qty,
            available_qty: available,
            vwap_px: notional / target_qty,
            worst_px,
            best_px: best.px,
            best_qty: best.qty,
            levels_used,
        })
    }
}

fn build_side(
    levels: impl IntoIterator<Item = (Decimal, Decimal)>,
    descending: bool,
) -> ArrayVec<Level, MAX_BOOK_LEVELS> {
    let mut out = ArrayVec::<Level, MAX_BOOK_LEVELS>::new();
    for (px, qty) in levels {
        if px <= Decimal::ZERO || qty <= Decimal::ZERO {
            continue;
        }
        let pos = match out.binary_search_by(|cur| {
            if descending {
                cur.px.cmp(&px).reverse()
            } else {
                cur.px.cmp(&px)
            }
        }) {
            Ok(idx) => {
                out[idx].qty += qty;
                continue;
            }
            Err(idx) => idx,
        };
        if out.len() < MAX_BOOK_LEVELS {
            out.insert(pos, Level { px, qty });
        } else if pos < MAX_BOOK_LEVELS {
            let _ = out.pop();
            out.insert(pos, Level { px, qty });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn depth_vwap_consumes_partial_final_level() {
        let now = Utc::now();
        let book = OrderBook::from_levels(
            [(dec!(10.00), dec!(1)), (dec!(9.90), dec!(5))],
            [(dec!(10.10), dec!(1)), (dec!(10.20), dec!(3))],
            now,
            now,
        );
        let quote = book.depth_vwap(Side::Buy, dec!(2), 20).unwrap();
        assert_eq!(quote.best_px, dec!(10.10));
        assert_eq!(quote.best_qty, dec!(1));
        assert_eq!(quote.worst_px, dec!(10.20));
        assert_eq!(quote.vwap_px, dec!(10.15));
        assert_eq!(quote.levels_used, 2);
    }

    #[test]
    fn depth_vwap_requires_full_target_depth() {
        let now = Utc::now();
        let book = OrderBook::from_levels(
            [(dec!(10.00), dec!(1))],
            [(dec!(10.10), dec!(1))],
            now,
            now,
        );
        assert!(book.depth_vwap(Side::Sell, dec!(2), 20).is_none());
    }
}
