//! Exact decimal order book. Each side keeps parallel sorted vectors so updates
//! use binary search and publishing the best 20 levels requires no whole-book copy.

use rust_decimal::Decimal;

#[derive(Debug, Clone, Default)]
pub struct BookSide {
    prices: Vec<Decimal>,
    sizes: Vec<Decimal>,
}

impl BookSide {
    pub fn new() -> Self {
        Self { prices: Vec::with_capacity(256), sizes: Vec::with_capacity(256) }
    }

    pub fn len(&self) -> usize { self.prices.len() }
    pub fn is_empty(&self) -> bool { self.prices.is_empty() }
    pub fn clear(&mut self) { self.prices.clear(); self.sizes.clear(); }

    pub fn upsert(&mut self, price: Decimal, size: Decimal) {
        let index = self.prices.partition_point(|p| *p < price);
        if index < self.prices.len() && self.prices[index] == price {
            if size.is_zero() {
                self.prices.remove(index);
                self.sizes.remove(index);
            } else {
                self.sizes[index] = size;
            }
        } else if !size.is_zero() {
            self.prices.insert(index, price);
            self.sizes.insert(index, size);
        }
    }

    pub fn lowest(&self) -> Option<(Decimal, Decimal)> {
        self.prices.first().zip(self.sizes.first()).map(|(p, q)| (*p, *q))
    }
    pub fn highest(&self) -> Option<(Decimal, Decimal)> {
        self.prices.last().zip(self.sizes.last()).map(|(p, q)| (*p, *q))
    }

    pub fn apply_snapshot(&mut self, mut levels: Vec<(Decimal, Decimal)>) {
        self.clear();
        levels.retain(|(_, size)| !size.is_zero());
        levels.sort_by_key(|(price, _)| *price);
        self.prices.reserve(levels.len());
        self.sizes.reserve(levels.len());
        for (price, size) in levels {
            // A duplicate snapshot price is an absolute replacement, like a delta.
            if self.prices.last() == Some(&price) {
                *self.sizes.last_mut().unwrap() = size;
            } else {
                self.prices.push(price);
                self.sizes.push(size);
            }
        }
    }

    pub fn top_ascending(&self, count: usize) -> impl Iterator<Item = (Decimal, Decimal)> + '_ {
        self.prices.iter().copied().zip(self.sizes.iter().copied()).take(count)
    }
    pub fn top_descending(&self, count: usize) -> impl Iterator<Item = (Decimal, Decimal)> + '_ {
        self.prices.iter().copied().zip(self.sizes.iter().copied()).rev().take(count)
    }

    /// Size resting at `price` (0 when there is no level).
    pub fn size_at(&self, price: Decimal) -> Decimal {
        self.prices.binary_search(&price).map_or(Decimal::ZERO, |index| self.sizes[index])
    }
    /// Drops every level priced above `price`.
    pub fn remove_above(&mut self, price: Decimal) {
        let keep = self.prices.partition_point(|p| *p <= price);
        self.prices.truncate(keep);
        self.sizes.truncate(keep);
    }
    /// Drops every level priced below `price`.
    pub fn remove_below(&mut self, price: Decimal) {
        let cut = self.prices.partition_point(|p| *p < price);
        self.prices.drain(..cut);
        self.sizes.drain(..cut);
    }
}

#[derive(Debug, Clone, Default)]
pub struct LocalBook {
    pub bids: BookSide,
    pub asks: BookSide,
    pub initialized: bool,
    pub last_offset: Option<u64>,
}

impl LocalBook {
    pub fn new() -> Self {
        Self { bids: BookSide::new(), asks: BookSide::new(), initialized: false, last_offset: None }
    }
    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.initialized = false;
        self.last_offset = None;
    }
    pub fn best_bid(&self) -> Option<Decimal> { self.bids.highest().map(|(price, _)| price) }
    pub fn best_ask(&self) -> Option<Decimal> { self.asks.lowest().map(|(price, _)| price) }
    pub fn mid(&self) -> Option<Decimal> { Some((self.best_bid()? + self.best_ask()?) / Decimal::from(2)) }

    pub fn apply_snapshot(&mut self, bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) {
        self.bids.apply_snapshot(bids);
        self.asks.apply_snapshot(asks);
        self.initialized = true;
    }
    pub fn apply_delta(&mut self, bids: &[(Decimal, Decimal)], asks: &[(Decimal, Decimal)]) {
        for &(price, size) in bids { self.bids.upsert(price, size); }
        for &(price, size) in asks { self.asks.upsert(price, size); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn decimal_snapshot_and_absolute_updates_preserve_wire_values() {
        let mut book = LocalBook::new();
        book.apply_snapshot(vec![(dec!(64820.2), dec!(0.00051)), (dec!(64820.1), dec!(1))],
            vec![(dec!(64820.3), dec!(0.19283))]);
        assert_eq!(book.best_bid(), Some(dec!(64820.2)));
        assert_eq!(book.mid(), Some(dec!(64820.25)));
        book.apply_delta(&[(dec!(64820.2), dec!(0.00052))], &[(dec!(64820.3), Decimal::ZERO)]);
        assert_eq!(book.bids.highest(), Some((dec!(64820.2), dec!(0.00052))));
        assert!(book.best_ask().is_none());
        assert!(book.mid().is_none());
    }

    #[test]
    fn best_level_iterators_have_correct_order_and_depth() {
        let mut book = LocalBook::new();
        book.apply_snapshot(
            (0..25).map(|i| (Decimal::from(100 + i), Decimal::from(i + 1))).collect(),
            (0..25).map(|i| (Decimal::from(200 + i), Decimal::from(i + 1))).collect(),
        );
        let bids: Vec<_> = book.bids.top_descending(20).collect();
        let asks: Vec<_> = book.asks.top_ascending(20).collect();
        assert_eq!(bids.len(), 20);
        assert_eq!(asks.len(), 20);
        assert_eq!(bids[0], (dec!(124), dec!(25)));
        assert_eq!(bids[19], (dec!(105), dec!(6)));
        assert_eq!(asks[0], (dec!(200), dec!(1)));
        assert_eq!(asks[19], (dec!(219), dec!(20)));
        book.reset();
        assert!(!book.initialized);
        assert_eq!(book.bids.top_descending(20).count(), 0);
    }
}
