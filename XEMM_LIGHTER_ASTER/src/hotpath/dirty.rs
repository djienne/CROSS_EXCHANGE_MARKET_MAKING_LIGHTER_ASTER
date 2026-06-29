//! Atomic bitset tracking which markets have new book data since the last reprice.
//!
//! The venue ingest thread calls `mark(idx)` on every publish; the strategy loop calls
//! `take_into()` on wake to get exactly the dirty set and reprice only those markets.
//! For < 64 markets a single `AtomicU64` on one cache line is sufficient.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::types::MarketIdx;

pub struct DirtyMarkets {
    segments: Vec<AtomicU64>,
    num_markets: usize,
    reprice_all: AtomicBool,
}

impl DirtyMarkets {
    pub fn new(num_markets: usize) -> Self {
        let num_segs = (num_markets + 63) / 64;
        DirtyMarkets {
            segments: (0..num_segs).map(|_| AtomicU64::new(0)).collect(),
            num_markets,
            reprice_all: AtomicBool::new(false),
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

    pub fn mark_all(&self) {
        self.reprice_all.store(true, Ordering::Release);
    }

    pub fn take_reprice_all(&self) -> bool {
        self.reprice_all.swap(false, Ordering::AcqRel)
    }

    /// Drain all currently dirty market indexes into a caller-owned scratch buffer.
    ///
    /// This is the hot wake path, so the strategy reuses the same `Vec` every loop and avoids the
    /// allocation that `take_all().collect()` would otherwise do on every BBO/depth wake.
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

    pub fn take_all(&self) -> DirtyIter {
        let mut indices = Vec::with_capacity(self.num_markets.min(64));
        self.take_into(&mut indices);
        DirtyIter { indices, pos: 0 }
    }

    #[allow(dead_code)]
    pub fn num_markets(&self) -> usize {
        self.num_markets
    }
}

pub struct DirtyIter {
    indices: Vec<MarketIdx>,
    pos: usize,
}

impl Iterator for DirtyIter {
    type Item = MarketIdx;
    fn next(&mut self) -> Option<MarketIdx> {
        let idx = self.indices.get(self.pos).copied();
        if idx.is_some() {
            self.pos += 1;
        }
        idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mark_and_take_returns_exact_indices() {
        let d = DirtyMarkets::new(8);
        d.mark(MarketIdx(1));
        d.mark(MarketIdx(3));
        d.mark(MarketIdx(5));
        let got: Vec<MarketIdx> = d.take_all().collect();
        assert_eq!(got, vec![MarketIdx(1), MarketIdx(3), MarketIdx(5)]);
    }

    #[test]
    fn take_clears_bits() {
        let d = DirtyMarkets::new(8);
        d.mark(MarketIdx(2));
        let _ = d.take_all().collect::<Vec<_>>();
        assert_eq!(d.take_all().count(), 0);
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
    fn reprice_all_overrides() {
        let d = DirtyMarkets::new(4);
        d.mark_all();
        assert!(d.take_reprice_all());
        assert!(!d.take_reprice_all());
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
        let got: std::collections::HashSet<u16> = d.take_all().map(|m| m.0).collect();
        assert_eq!(got.len(), 64);
    }

    #[test]
    fn empty_take_returns_nothing() {
        let d = DirtyMarkets::new(16);
        assert_eq!(d.take_all().count(), 0);
    }
}
