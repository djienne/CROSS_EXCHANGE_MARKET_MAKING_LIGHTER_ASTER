//! Lightweight lock-free latency histograms for hot-path instrumentation.
//!
//! All operations are `Relaxed` atomics — no cross-thread ordering guarantees beyond
//! eventual visibility, which is fine for diagnostic counters. Static globals avoid
//! any allocation on the recording path.

use std::sync::atomic::{AtomicU64, Ordering};

/// Bucket upper boundaries in nanoseconds.
const BOUNDARIES: [u64; 15] = [
    1_000,       // 1 µs
    2_000,       // 2 µs
    5_000,       // 5 µs
    10_000,      // 10 µs
    20_000,      // 20 µs
    50_000,      // 50 µs
    100_000,     // 100 µs
    200_000,     // 200 µs
    500_000,     // 500 µs
    1_000_000,   // 1 ms
    2_000_000,   // 2 ms
    5_000_000,   // 5 ms
    10_000_000,  // 10 ms
    50_000_000,  // 50 ms
    100_000_000, // 100 ms
];

const NUM_BUCKETS: usize = BOUNDARIES.len() + 1; // 15 bounded + 1 overflow

pub struct LatencyHistogram {
    buckets: [AtomicU64; NUM_BUCKETS],
    count: AtomicU64,
}

pub struct HistSnapshot {
    pub p50_us: u64,
    pub p99_us: u64,
    pub count: u64,
}

impl LatencyHistogram {
    pub const fn new() -> Self {
        // const-compatible initialization for use in `static`
        const ZERO: AtomicU64 = AtomicU64::new(0);
        LatencyHistogram {
            buckets: [ZERO; NUM_BUCKETS],
            count: ZERO,
        }
    }

    #[inline]
    pub fn record(&self, duration_ns: u64) {
        let idx = BOUNDARIES.iter().position(|&b| duration_ns < b).unwrap_or(BOUNDARIES.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> HistSnapshot {
        let mut bucket_counts = [0u64; NUM_BUCKETS];
        for (i, b) in self.buckets.iter().enumerate() {
            bucket_counts[i] = b.load(Ordering::Relaxed);
        }
        let total = self.count.load(Ordering::Relaxed);

        if total == 0 {
            return HistSnapshot { p50_us: 0, p99_us: 0, count: 0 };
        }

        let p50_us = percentile_us(&bucket_counts, total, 50);
        let p99_us = percentile_us(&bucket_counts, total, 99);

        HistSnapshot { p50_us, p99_us, count: total }
    }

    pub fn reset(&self) {
        for b in &self.buckets {
            b.store(0, Ordering::Relaxed);
        }
        self.count.store(0, Ordering::Relaxed);
    }
}

fn percentile_us(buckets: &[u64; NUM_BUCKETS], total: u64, pct: u64) -> u64 {
    let target = (total * pct + 99) / 100; // ceiling
    let mut cumulative = 0u64;
    for (i, &count) in buckets.iter().enumerate() {
        cumulative += count;
        if cumulative >= target {
            return if i < BOUNDARIES.len() {
                BOUNDARIES[i] / 1_000 // upper bound of this bucket, in µs
            } else {
                100_000 // overflow bucket: >= 100 ms
            };
        }
    }
    100_000
}

// Global histogram instances — zero-cost static allocation.
pub static BOOK_BUILD: LatencyHistogram = LatencyHistogram::new();
pub static VENUE_PUBLISH: LatencyHistogram = LatencyHistogram::new();
pub static WAKE_REPRICE: LatencyHistogram = LatencyHistogram::new();
pub static TICK_REPRICE: LatencyHistogram = LatencyHistogram::new();
pub static SINGLE_REPRICE: LatencyHistogram = LatencyHistogram::new();

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_snapshot() {
        let h = LatencyHistogram::new();
        let s = h.snapshot();
        assert_eq!(s.count, 0);
        assert_eq!(s.p50_us, 0);
    }

    #[test]
    fn single_sample() {
        let h = LatencyHistogram::new();
        h.record(500); // 500 ns → bucket [0] (< 1µs)
        let s = h.snapshot();
        assert_eq!(s.count, 1);
        assert_eq!(s.p50_us, 1); // upper bound of bucket 0 is 1µs
    }

    #[test]
    fn mixed_samples() {
        let h = LatencyHistogram::new();
        // 80 samples at 500ns (bucket 0: <1µs), 20 at 5ms (bucket 12: <10ms, since 5M is NOT <5M)
        for _ in 0..80 {
            h.record(500);
        }
        for _ in 0..20 {
            h.record(5_000_000);
        }
        let s = h.snapshot();
        assert_eq!(s.count, 100);
        assert_eq!(s.p50_us, 1);       // 50th pct in the 500ns bucket
        assert_eq!(s.p99_us, 10_000);  // 99th pct: 5M ns >= 5M boundary → lands in <10ms bucket
    }

    #[test]
    fn reset_clears() {
        let h = LatencyHistogram::new();
        h.record(1_000);
        h.reset();
        let s = h.snapshot();
        assert_eq!(s.count, 0);
    }

    #[test]
    fn overflow_bucket() {
        let h = LatencyHistogram::new();
        h.record(200_000_000); // 200ms → overflow
        let s = h.snapshot();
        assert_eq!(s.p50_us, 100_000);
    }
}
