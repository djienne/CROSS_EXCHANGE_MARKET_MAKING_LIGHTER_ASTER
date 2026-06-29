//! Shared hot-path data types — plain `Copy` structs with no feature-gate dependency.
//!
//! `HotBook` and `HotLevel` are the scaled-integer order book representation used by
//! the live strategy loop and the `VenueBook` cell. They live here (outside `hotpath`
//! and `livebot`) so both modules can import them without circular feature-gate issues.

use crate::types::Side;

/// Number of book levels carried on the hot path (matches Aster `@depth20` / HL l2Book).
pub const HOT_LEVELS: usize = 20;

/// One book level in scaled-integer form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HotLevel {
    pub px_ticks: i64,
    pub qty_lots: i64,
}

/// The live hot-path order book: fixed-capacity integer levels, a generation stamp, and
/// the monotonic receive time. Built from a [`crate::book::OrderBook`] snapshot via a
/// [`crate::livebot::scale::MarketScale`]. No heap, no `Decimal` — cheap to copy and
/// compare on the quote loop.
#[derive(Debug, Clone, Copy)]
pub struct HotBook {
    bids: [HotLevel; HOT_LEVELS], // descending by px_ticks
    asks: [HotLevel; HOT_LEVELS], // ascending by px_ticks
    bid_len: u8,
    ask_len: u8,
    pub generation: u64,
    pub recv_ns: i64,
    /// Exchange timestamp (milliseconds since Unix epoch) carried by this snapshot. Used to
    /// merge independent BBO/depth feeds without letting a locally-late older BBO override
    /// a newer L2 book. Zero means "unknown" and is treated conservatively by callers.
    pub exch_ms: i64,
}

impl HotBook {
    /// Construct directly from pre-filled level arrays. Used by `build_hot_book` in
    /// `livebot::scale`.
    pub fn new(
        bids: [HotLevel; HOT_LEVELS],
        asks: [HotLevel; HOT_LEVELS],
        bid_len: u8,
        ask_len: u8,
        generation: u64,
        recv_ns: i64,
        exch_ms: i64,
    ) -> Self {
        HotBook { bids, asks, bid_len, ask_len, generation, recv_ns, exch_ms }
    }

    #[inline]
    pub fn bids(&self) -> &[HotLevel] {
        &self.bids[..self.bid_len as usize]
    }
    #[inline]
    pub fn asks(&self) -> &[HotLevel] {
        &self.asks[..self.ask_len as usize]
    }

    #[inline]
    pub fn best_bid_ticks(&self) -> Option<i64> {
        self.bids().first().map(|l| l.px_ticks)
    }
    #[inline]
    pub fn best_ask_ticks(&self) -> Option<i64> {
        self.asks().first().map(|l| l.px_ticks)
    }

    /// True when the top of book is crossed or locked (bid >= ask), or when either
    /// side is missing (an incomplete book is untradeable). Callers that already
    /// check `best_bid_ticks()`/`best_ask_ticks()` for `None` separately will see
    /// no behavior change; for any future direct caller this is the safe default.
    #[inline]
    pub fn is_crossed(&self) -> bool {
        match (self.best_bid_ticks(), self.best_ask_ticks()) {
            (Some(b), Some(a)) => b >= a,
            _ => true,
        }
    }

    /// Mid in HALF-ticks (so a half-tick mid stays integer): `best_bid + best_ask`. `None`
    /// if either side is empty. Compare two half-tick mids directly; divide by 2 only when
    /// you need the actual mid.
    #[inline]
    pub fn mid_half_ticks(&self) -> Option<i64> {
        match (self.best_bid_ticks(), self.best_ask_ticks()) {
            (Some(b), Some(a)) => Some(b + a),
            _ => None,
        }
    }

    /// Touch price (best price on `side`) in ticks.
    #[inline]
    pub fn touch_ticks(&self, side: Side) -> Option<i64> {
        match side {
            Side::Buy => self.best_bid_ticks(),
            Side::Sell => self.best_ask_ticks(),
        }
    }

    /// Milliseconds since this book was received, at monotonic `now_ns`.
    #[inline]
    pub fn age_ms(&self, now_ns: i64) -> i64 {
        now_ns.saturating_sub(self.recv_ns) / 1_000_000
    }
}
