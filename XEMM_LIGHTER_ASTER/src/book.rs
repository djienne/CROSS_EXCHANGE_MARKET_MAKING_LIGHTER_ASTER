//! In-memory order book built from a partial-depth snapshot (both venues push
//! whole-book snapshots, so there is no diff/sequence maintenance). Bids are
//! sorted descending, asks ascending; zero-qty levels are dropped on build.

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

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub bids: ArrayVec<Level, MAX_BOOK_LEVELS>, // descending by px
    pub asks: ArrayVec<Level, MAX_BOOK_LEVELS>, // ascending by px
    pub exch_ts: DateTime<Utc>,
    pub local_recv_ts: DateTime<Utc>,
}

impl OrderBook {
    /// Build a normalized book from raw (px, qty) snapshot rows. Aggregates duplicate prices, drops
    /// non-positive prices AND quantities (a corrupted/garbage exchange tick — a
    /// zero/negative price would otherwise poison `mid`/`touch` and panic the
    /// divide-by-zero math downstream), sorts each side canonically, and truncates
    /// to [`MAX_BOOK_LEVELS`] (exchanges send <= 20 levels; the truncation only
    /// fires on pathological input).
    pub fn from_levels(
        bids: impl IntoIterator<Item = (Decimal, Decimal)>,
        asks: impl IntoIterator<Item = (Decimal, Decimal)>,
        exch_ts: DateTime<Utc>,
        local_recv_ts: DateTime<Utc>,
    ) -> Self {
        // Hot path: exchanges send at most ~20 levels, and BBO assists send one. Avoid
        // heap-allocating + sorting a Vec on every websocket frame; keep the best
        // MAX_BOOK_LEVELS directly in a fixed ArrayVec via insertion sort/truncation.
        let bids = build_side(bids, true);
        let asks = build_side(asks, false);

        OrderBook { bids, asks, exch_ts, local_recv_ts }
    }

    #[inline]
    pub fn best_bid(&self) -> Option<Level> {
        self.bids.first().copied()
    }

    #[inline]
    pub fn best_ask(&self) -> Option<Level> {
        self.asks.first().copied()
    }

    #[inline]
    pub fn mid(&self) -> Option<Decimal> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some((b.px + a.px) / Decimal::from(2)),
            _ => None,
        }
    }

    /// True if the top of book is crossed or locked (bid >= ask).
    #[inline]
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => b.px >= a.px,
            _ => false,
        }
    }

    /// Visible quantity resting at exactly `px` on the quote's own side.
    pub fn qty_at_price(&self, side: Side, px: Decimal) -> Decimal {
        let levels = match side {
            Side::Buy => self.bids.as_slice(),
            Side::Sell => self.asks.as_slice(),
        };
        levels
            .iter()
            .filter(|l| l.px == px)
            .map(|l| l.qty)
            .sum()
    }

    /// Visible quantity at price levels strictly better than `px` on the quote's
    /// own side (higher bids for a buy quote, lower asks for a sell quote) — the
    /// volume a sweep must consume before reaching `px`.
    pub fn qty_better_than(&self, side: Side, px: Decimal) -> Decimal {
        match side {
            Side::Buy => self
                .bids
                .iter()
                .filter(|l| l.px > px)
                .map(|l| l.qty)
                .sum(),
            Side::Sell => self
                .asks
                .iter()
                .filter(|l| l.px < px)
                .map(|l| l.qty)
                .sum(),
        }
    }

    /// Age in milliseconds of this book relative to `now` (by local receive time).
    #[inline]
    pub fn age_ms(&self, now: DateTime<Utc>) -> i64 {
        (now - self.local_recv_ts).num_milliseconds()
    }

    /// True when a resting quote at `px` on `side` sits beyond the deepest captured
    /// level (below the lowest bid for a buy, above the highest ask for a sell). The
    /// book is a partial-depth snapshot (Aster `@depth20`), so when our quote rests
    /// past the captured bottom, the levels between it and our price are unseen and
    /// [`qty_better_than`] is only a LOWER bound on the true queue ahead — a fill here
    /// may be simulated too easily. A measurement flag, not a reject (the report
    /// separates "queue observed" from "queue truncated"). Note: a genuinely shallow
    /// book that pushed fewer than its cap of levels can also trip this; the flag
    /// reads as "queue not fully observed from this snapshot."
    pub fn queue_truncated_at(&self, side: Side, px: Decimal) -> bool {
        match side {
            Side::Buy => self.bids.last().is_some_and(|l| px < l.px),
            Side::Sell => self.asks.last().is_some_and(|l| px > l.px),
        }
    }
}

#[inline]
fn build_side(
    levels: impl IntoIterator<Item = (Decimal, Decimal)>,
    descending: bool,
) -> ArrayVec<Level, MAX_BOOK_LEVELS> {
    let mut out = ArrayVec::<Level, MAX_BOOK_LEVELS>::new();
    for (px, qty) in levels {
        if px <= Decimal::ZERO || qty <= Decimal::ZERO {
            continue;
        }
        upsert_sorted(&mut out, Level { px, qty }, descending);
    }
    out
}

#[inline]
fn upsert_sorted(out: &mut ArrayVec<Level, MAX_BOOK_LEVELS>, level: Level, descending: bool) {
    // `out` is maintained in canonical book order, so one binary search both finds an
    // existing duplicate price (aggregate quantity) and the insertion point for a new level.
    let pos = match out.binary_search_by(|cur| {
        if descending {
            cur.px.cmp(&level.px).reverse()
        } else {
            cur.px.cmp(&level.px)
        }
    }) {
        Ok(idx) => {
            out[idx].qty += level.qty;
            return;
        }
        Err(idx) => idx,
    };

    if out.len() < MAX_BOOK_LEVELS {
        out.insert(pos, level);
    } else if pos < MAX_BOOK_LEVELS {
        // Keep only the best MAX_BOOK_LEVELS. Drop the current worst before inserting.
        let _ = out.pop();
        out.insert(pos, level);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn sample() -> OrderBook {
        OrderBook::from_levels(
            vec![(dec!(100.0), dec!(2)), (dec!(99.9), dec!(5)), (dec!(99.8), dec!(7))],
            vec![(dec!(100.1), dec!(3)), (dec!(100.2), dec!(4)), (dec!(100.3), dec!(0))],
            ts(),
            ts(),
        )
    }

    #[test]
    fn sorting_and_zero_drop() {
        let b = sample();
        assert_eq!(b.best_bid().unwrap().px, dec!(100.0));
        assert_eq!(b.best_ask().unwrap().px, dec!(100.1));
        assert_eq!(b.asks.len(), 2); // zero-qty ask dropped
        assert_eq!(b.mid().unwrap(), dec!(100.05));
        assert!(!b.is_crossed());
    }

    #[test]
    fn nonpositive_price_dropped() {
        let b = OrderBook::from_levels(
            vec![(dec!(100.0), dec!(2)), (dec!(0), dec!(5)), (dec!(-1), dec!(9))],
            vec![(dec!(100.1), dec!(3)), (dec!(0), dec!(4))],
            ts(),
            ts(),
        );
        assert_eq!(b.bids.len(), 1);
        assert_eq!(b.asks.len(), 1);
        assert_eq!(b.best_bid().unwrap().px, dec!(100.0));
        assert_eq!(b.best_ask().unwrap().px, dec!(100.1));
        assert_eq!(b.mid().unwrap(), dec!(100.05));

        let all_bad = OrderBook::from_levels(
            vec![(dec!(0), dec!(5))],
            vec![(dec!(100.1), dec!(3))],
            ts(),
            ts(),
        );
        assert!(all_bad.best_bid().is_none());
        assert!(all_bad.mid().is_none());
    }


    #[test]
    fn duplicate_price_levels_are_aggregated() {
        let b = OrderBook::from_levels(
            vec![(dec!(100.0), dec!(2)), (dec!(100.0), dec!(3)), (dec!(99.9), dec!(1))],
            vec![(dec!(100.1), dec!(4)), (dec!(100.1), dec!(6))],
            ts(),
            ts(),
        );
        assert_eq!(b.bids.len(), 2);
        assert_eq!(b.asks.len(), 1);
        assert_eq!(b.best_bid().unwrap().qty, dec!(5));
        assert_eq!(b.best_ask().unwrap().qty, dec!(10));
    }

    #[test]
    fn queue_volume_queries() {
        let b = sample();
        assert_eq!(b.qty_better_than(Side::Buy, dec!(99.9)), dec!(2));
        assert_eq!(b.qty_at_price(Side::Buy, dec!(99.9)), dec!(5));
        assert_eq!(b.qty_better_than(Side::Sell, dec!(100.2)), dec!(3));
        assert_eq!(b.qty_at_price(Side::Sell, dec!(100.2)), dec!(4));
    }

    #[test]
    fn queue_truncation_detection() {
        let b = sample();
        assert!(!b.queue_truncated_at(Side::Buy, dec!(99.8)));
        assert!(!b.queue_truncated_at(Side::Buy, dec!(99.85)));
        assert!(b.queue_truncated_at(Side::Buy, dec!(99.7)));
        assert!(!b.queue_truncated_at(Side::Sell, dec!(100.2)));
        assert!(b.queue_truncated_at(Side::Sell, dec!(100.3)));
    }

    #[test]
    fn crossed_detection() {
        let b = OrderBook::from_levels(
            vec![(dec!(101), dec!(1))],
            vec![(dec!(100), dec!(1))],
            ts(),
            ts(),
        );
        assert!(b.is_crossed());
    }

    #[test]
    fn truncates_to_max_levels() {
        let many_bids: Vec<(Decimal, Decimal)> = (0..30)
            .map(|i| (dec!(100) - Decimal::from(i) * dec!(0.1), dec!(1)))
            .collect();
        let b = OrderBook::from_levels(many_bids, vec![(dec!(101), dec!(1))], ts(), ts());
        assert_eq!(b.bids.len(), MAX_BOOK_LEVELS);
        assert_eq!(b.best_bid().unwrap().px, dec!(100.0));
    }

    #[test]
    fn truncation_keeps_best_levels_from_unsorted_input() {
        let mut bids: Vec<(Decimal, Decimal)> = (0..30)
            .map(|i| (dec!(90) + Decimal::from(i) * dec!(0.1), dec!(1)))
            .collect();
        bids.reverse();
        bids.push((dec!(105), dec!(1))); // best level arrives after the first 20 rows
        let b = OrderBook::from_levels(bids, vec![(dec!(106), dec!(1))], ts(), ts());
        assert_eq!(b.bids.len(), MAX_BOOK_LEVELS);
        assert_eq!(b.best_bid().unwrap().px, dec!(105));
        assert!(b.bids.iter().all(|l| l.px >= dec!(91.0)));
    }
}
