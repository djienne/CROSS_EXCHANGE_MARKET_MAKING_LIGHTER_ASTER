//! Local order book — port of `_vol_obi_fast.pyx::CBookSide`.
//!
//! Each side is two parallel `Vec`s (prices, sizes) sorted ASCENDING by price
//! (matches CBookSide for BOTH sides — codex review). Best bid = highest price (last
//! element of the bid side); best ask = lowest price (first element of the ask side).
//! O(log n) upsert via binary search, O(log n + k) range sums.

/// One side of the book, prices ascending.
#[derive(Debug, Clone, Default)]
pub struct BookSide {
    prices: Vec<f64>,
    sizes: Vec<f64>,
}

impl BookSide {
    pub fn new() -> Self {
        Self {
            prices: Vec::with_capacity(256),
            sizes: Vec::with_capacity(256),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.prices.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.prices.is_empty()
    }

    pub fn clear(&mut self) {
        self.prices.clear();
        self.sizes.clear();
    }

    /// bisect_left: first index with price >= `price`.
    #[inline]
    fn bisect_left(&self, price: f64) -> usize {
        self.prices.partition_point(|&p| p < price)
    }

    /// bisect_right: first index with price > `price`.
    #[inline]
    fn bisect_right(&self, price: f64) -> usize {
        self.prices.partition_point(|&p| p <= price)
    }

    /// Insert/update (`size > 0`) or remove (`size == 0`) a price level.
    pub fn upsert(&mut self, price: f64, size: f64) {
        let idx = self.bisect_left(price);
        let present = idx < self.prices.len() && self.prices[idx] == price;
        if present {
            if size == 0.0 {
                self.prices.remove(idx);
                self.sizes.remove(idx);
            } else {
                self.sizes[idx] = size;
            }
        } else if size != 0.0 {
            self.prices.insert(idx, price);
            self.sizes.insert(idx, size);
        }
    }

    /// Lowest price level (front).
    #[inline]
    pub fn lowest(&self) -> Option<(f64, f64)> {
        if self.prices.is_empty() {
            None
        } else {
            Some((self.prices[0], self.sizes[0]))
        }
    }

    /// Highest price level (back).
    #[inline]
    pub fn highest(&self) -> Option<(f64, f64)> {
        let n = self.prices.len();
        if n == 0 {
            None
        } else {
            Some((self.prices[n - 1], self.sizes[n - 1]))
        }
    }

    /// Sum sizes for all levels with price >= `min_price`.
    pub fn sum_sizes_from(&self, min_price: f64) -> f64 {
        let start = self.bisect_left(min_price);
        let mut total = 0.0;
        for i in start..self.sizes.len() {
            total += self.sizes[i];
        }
        total
    }

    /// Sum sizes for all levels with price <= `max_price`.
    pub fn sum_sizes_to(&self, max_price: f64) -> f64 {
        let end = self.bisect_right(max_price);
        let mut total = 0.0;
        for &s in &self.sizes[..end] {
            total += s;
        }
        total
    }

    /// Replace the whole side from a snapshot (already parsed levels).
    pub fn apply_snapshot(&mut self, mut levels: Vec<(f64, f64)>) {
        self.clear();
        levels.retain(|&(_, s)| s != 0.0);
        levels.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        self.prices.reserve(levels.len());
        self.sizes.reserve(levels.len());
        for (p, s) in levels {
            self.prices.push(p);
            self.sizes.push(s);
        }
    }

    pub fn levels(&self) -> Vec<(f64, f64)> {
        self.prices
            .iter()
            .copied()
            .zip(self.sizes.iter().copied())
            .collect()
    }
}

/// Two-sided local order book with offset/sequence tracking.
#[derive(Debug, Clone, Default)]
pub struct LocalBook {
    pub bids: BookSide,
    pub asks: BookSide,
    pub initialized: bool,
    pub last_offset: Option<u64>,
}

impl LocalBook {
    pub fn new() -> Self {
        Self {
            bids: BookSide::new(),
            asks: BookSide::new(),
            initialized: false,
            last_offset: None,
        }
    }

    pub fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.initialized = false;
        self.last_offset = None;
    }

    #[inline]
    pub fn best_bid(&self) -> Option<f64> {
        self.bids.highest().map(|(p, _)| p)
    }

    #[inline]
    pub fn best_ask(&self) -> Option<f64> {
        self.asks.lowest().map(|(p, _)| p)
    }

    /// Mid price; `None` if the book is one-sided.
    #[inline]
    pub fn mid(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) => Some((b + a) * 0.5),
            _ => None,
        }
    }

    pub fn apply_snapshot(&mut self, bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) {
        self.bids.apply_snapshot(bids);
        self.asks.apply_snapshot(asks);
        self.initialized = true;
    }

    pub fn apply_delta(&mut self, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
        for &(p, s) in bids {
            self.bids.upsert(p, s);
        }
        for &(p, s) in asks {
            self.asks.upsert(p, s);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn best_and_mid() {
        let mut b = LocalBook::new();
        b.apply_snapshot(
            vec![(100.0, 1.0), (99.0, 2.0), (101.0, 0.5)], // bids ascending after sort
            vec![(102.0, 1.0), (103.0, 2.0)],
        );
        assert_eq!(b.best_bid(), Some(101.0)); // highest bid
        assert_eq!(b.best_ask(), Some(102.0)); // lowest ask
        assert_eq!(b.mid(), Some(101.5));
    }

    #[test]
    fn delta_upsert_remove() {
        let mut b = LocalBook::new();
        b.apply_snapshot(vec![(100.0, 1.0)], vec![(102.0, 1.0)]);
        b.apply_delta(&[(100.5, 3.0)], &[(102.0, 0.0)]); // add bid, remove ask
        assert_eq!(b.best_bid(), Some(100.5));
        assert_eq!(b.best_ask(), None);
    }

    #[test]
    fn obi_range_sums() {
        let mut book = LocalBook::new();
        // bids: 99,100,101 ; asks: 102,103,104
        book.apply_snapshot(
            vec![(99.0, 1.0), (100.0, 2.0), (101.0, 3.0)],
            vec![(102.0, 4.0), (103.0, 5.0), (104.0, 6.0)],
        );
        let mid = 101.5;
        let depth = 0.025;
        let lower = mid * (1.0 - depth); // ~98.96 -> all bids
        let upper = mid * (1.0 + depth); // ~104.0 -> all asks (<=)
        let bid_sum = book.bids.sum_sizes_from(lower); // 1+2+3
        let ask_sum = book.asks.sum_sizes_to(upper); // 4+5+6 (104 included)
        assert!((bid_sum - 6.0).abs() < 1e-9);
        assert!((ask_sum - 15.0).abs() < 1e-9);
        assert!((bid_sum - ask_sum - (-9.0)).abs() < 1e-9);
    }
}
