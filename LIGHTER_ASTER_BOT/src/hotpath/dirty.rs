//! Atomic bitset tracking which markets have new book data since the last reprice.
//!
//! The venue ingest thread calls `mark(idx)` on every publish; the strategy loop calls
//! `take_into()` on wake to get exactly the dirty set and reprice only those markets.
//! For < 64 markets a single `AtomicU64` on one cache line is sufficient.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::MarketIdx;

pub struct DirtyMarkets {
    segments: Vec<AtomicU64>,
    num_markets: usize,
}

impl DirtyMarkets {
    pub fn new(num_markets: usize) -> Self {
        let num_segs = (num_markets + 63) / 64;
        DirtyMarkets {
            segments: (0..num_segs).map(|_| AtomicU64::new(0)).collect(),
            num_markets,
        }
    }

    #[inline]
    pub fn mark(&self, idx: MarketIdx) {
        let seg = idx.0 as usize / 64;
        let bit = idx.0 as usize % 64;
        if seg < self.segments.len() {
            self.segments[seg].fetch_or(1u64 << bit, Ordering::Release);
        }
    }

    /// Drain all currently dirty market indexes into a caller-owned scratch buffer.
    ///
    /// This is the hot wake path, so the strategy reuses the same `Vec` every loop and avoids an
    /// allocation on every BBO/depth wake.
    pub fn take_into(&self, out: &mut Vec<MarketIdx>) {
        out.clear();
        for (seg_idx, seg) in self.segments.iter().enumerate() {
            let mut bits = seg.swap(0, Ordering::AcqRel);
            let base = (seg_idx as u16) * 64;
            while bits != 0 {
                let tz = bits.trailing_zeros() as u16;
                bits &= bits - 1;
                let idx = base + tz;
                if (idx as usize) < self.num_markets {
                    out.push(MarketIdx(idx));
                }
            }
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    fn take_all(d: &DirtyMarkets) -> Vec<MarketIdx> {
        let mut out = Vec::new();
        d.take_into(&mut out);
        out
    }

    #[test]
    fn mark_and_take_returns_exact_indices() {
        let d = DirtyMarkets::new(8);
        d.mark(MarketIdx(1));
        d.mark(MarketIdx(3));
        d.mark(MarketIdx(5));
        let got = take_all(&d);
        assert_eq!(got, vec![MarketIdx(1), MarketIdx(3), MarketIdx(5)]);
    }

    #[test]
    fn take_clears_bits() {
        let d = DirtyMarkets::new(8);
        d.mark(MarketIdx(2));
        take_all(&d);
        assert!(take_all(&d).is_empty());
    }

    #[test]
    fn take_into_reuses_caller_buffer() {
        let d = DirtyMarkets::new(8);
        let mut buf = Vec::with_capacity(8);
        d.mark(MarketIdx(1));
        d.mark(MarketIdx(4));
        d.take_into(&mut buf);
        assert_eq!(buf, vec![MarketIdx(1), MarketIdx(4)]);
        let cap = buf.capacity();
        d.mark(MarketIdx(2));
        d.take_into(&mut buf);
        assert_eq!(buf, vec![MarketIdx(2)]);
        assert_eq!(buf.capacity(), cap);
    }

    #[test]
    fn concurrent_mark_take() {
        let d = std::sync::Arc::new(DirtyMarkets::new(64));
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let d = d.clone();
                std::thread::spawn(move || {
                    for i in 0..16 {
                        d.mark(MarketIdx(t * 16 + i));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let got: std::collections::HashSet<u16> = take_all(&d).into_iter().map(|m| m.0).collect();
        assert_eq!(got.len(), 64);
    }

    #[test]
    fn empty_take_returns_nothing() {
        let d = DirtyMarkets::new(16);
        assert!(take_all(&d).is_empty());
    }
}
