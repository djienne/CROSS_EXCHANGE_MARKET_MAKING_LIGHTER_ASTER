//! The lock-free latest-book cell — one per (market, venue). A single writer (the
//! venue ingest thread) publishes the freshest [`OrderBook`] via an atomic pointer
//! swap; many readers (the stream watchdog, a future strategy hot loop) read it
//! wait-free. A separate atomic stamps the last-message time for staleness checks.
//!
//! This is a *side output* of the ingest path: it never feeds the deterministic
//! recorder channel, the JSONL log, or `SimEngine`. See `hotpath::mod` for the
//! determinism contract.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use tokio::sync::Notify;

use super::clock::mono_now_ns;
use crate::book::OrderBook;
use crate::hot_types::HotBook;

/// Which venue a [`VenueBook`] belongs to. Part of the registry key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VenueTag {
    Aster,
    Hyperliquid,
}

impl VenueTag {
    pub fn as_str(self) -> &'static str {
        match self {
            VenueTag::Aster => "aster",
            VenueTag::Hyperliquid => "hyperliquid",
        }
    }
}

/// One venue's latest book for one market, plus a liveness stamp.
///
/// Writer: the venue ingest thread (single writer per cell). Readers: the watchdog
/// and a future strategy loop (many readers, wait-free). `book` is `None` until the
/// first snapshot arrives.
pub struct VenueBook {
    book: ArcSwapOption<OrderBook>,
    /// Fast top-of-book assist, currently published by Hyperliquid `bbo`. This is
    /// separate from `book` so a one-level BBO update never overwrites the 20-level
    /// `l2Book` depth used as a VWAP fallback.
    bbo: ArcSwapOption<OrderBook>,
    /// The integer-scaled hot book, published alongside the raw `OrderBook` when a
    /// `MarketScale` is available (live mode). `None` until the first hot publish, or
    /// always `None` in record mode (no scale available).
    hot: ArcSwapOption<HotBook>,
    /// Integer projection of `bbo`, used by the fast cancel precheck.
    bbo_hot: ArcSwapOption<HotBook>,
    /// True after an integer L2 projection was published before the slower raw Decimal book.
    /// The strategy may use that update for risk-reducing fast cancels, but must not place a
    /// new exact quote until the matching raw book arrives and clears this flag.
    hot_only_pending: AtomicBool,
    /// Same guard for BBO assists: hot BBO can wake a fast cancel before raw BBO is built.
    bbo_hot_only_pending: AtomicBool,
    /// Monotonic-clock nanos of the last message of ANY kind on this venue stream (book,
    /// trade, or pong) — CONNECTION liveness, so a venue briefly only streaming
    /// trades, or only answering pings, still reads as alive (drives reconnect).
    last_msg_ns: AtomicI64,
    /// Monotonic-clock nanos of the last BOOK SNAPSHOT specifically (set by `publish`, never by
    /// `touch`) — TRADING-DATA freshness. A feed streaming only trades/pongs keeps
    /// `last_msg_ns` fresh while its order book goes stale, so the trading gate must
    /// gate on THIS, not on `last_msg_ns`.
    last_book_ns: AtomicI64,
    /// Monotonic-clock nanos of the last fast BBO update. For Hyperliquid this can
    /// refresh quote-touch freshness while the slower `l2Book` snapshot cadence is ~5s.
    last_bbo_ns: AtomicI64,
    /// Last exchange timestamp accepted for a full-depth snapshot. Used to reject
    /// out-of-order websocket frames that arrive late on combined feeds and would
    /// otherwise roll the book backwards while still looking locally fresh.
    last_book_exch_ms: AtomicI64,
    /// Last exchange timestamp accepted for the BBO assist (separate from L2 because
    /// the feeds are independent and intentionally stored separately).
    last_bbo_exch_ms: AtomicI64,
    /// Set by the periodic REST cross-check ([`super::book_check`]) when this book's
    /// content disagrees with a REST snapshot (a stuck or corrupt feed that keeps
    /// delivering frames). The watchdog reads it: a divergent cell closes the trading
    /// gate even while frames keep arriving. Orthogonal to staleness (no frames).
    rest_divergent: AtomicBool,
    /// Monotonically increasing book-version counter, bumped on every [`publish`]. A
    /// strategy loop snapshots `generation()` per market and only recomputes when it
    /// changed — exact change detection without diffing the book or polling on a timer.
    generation: AtomicU64,
    /// Separate version counter for BBO updates. `quote_generation()` combines this
    /// with the L2 generation so BBO-only changes wake and reprice the live strategy.
    bbo_generation: AtomicU64,
    /// Optional coalescing strategy wakeup. When a live strategy loop is attached
    /// ([`VenueBook::with_wake`]) every `publish` calls `notify_one`, so the loop wakes
    /// on the next book change instead of sleep-polling (plan §5.2). `None` on the
    /// dry-run `record`/`live` path, so that path is behaviorally unchanged.
    wake: Option<Arc<Notify>>,
    /// When set, each `publish`/`publish_hot` marks this market dirty in the shared
    /// bitset so the strategy loop can reprice only changed markets on wake.
    dirty: Option<(Arc<super::dirty::DirtyMarkets>, crate::types::MarketIdx)>,
}

#[inline]
fn exch_ms(book: &OrderBook) -> i64 {
    book.exch_ts.timestamp_millis()
}

/// Accept equal-or-newer exchange timestamps and reject strictly older frames.
/// Local receive time alone is not sufficient on combined websocket streams: a
/// delayed depth/BBO message can arrive after a newer one and would otherwise
/// overwrite the latest book while still appearing fresh to the strategy.
#[inline]
fn accept_exch_ts(atom: &AtomicI64, next_ms: i64) -> bool {
    let mut prev = atom.load(Ordering::Acquire);
    loop {
        if next_ms < prev {
            return false;
        }
        if next_ms == prev {
            return true;
        }
        match atom.compare_exchange(prev, next_ms, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(actual) => prev = actual,
        }
    }
}

impl Default for VenueBook {
    fn default() -> Self {
        Self::new()
    }
}

impl VenueBook {
    pub fn new() -> Self {
        Self::build(None)
    }

    /// Like [`new`] but wired to a shared coalescing strategy-wakeup handle: every
    /// `publish` calls `wake.notify_one()`. Used only by the live bot's registry.
    pub fn with_wake(wake: Arc<Notify>) -> Self {
        Self::build(Some(wake))
    }

    fn build(wake: Option<Arc<Notify>>) -> Self {
        VenueBook {
            book: ArcSwapOption::empty(),
            bbo: ArcSwapOption::empty(),
            hot: ArcSwapOption::empty(),
            bbo_hot: ArcSwapOption::empty(),
            hot_only_pending: AtomicBool::new(false),
            bbo_hot_only_pending: AtomicBool::new(false),
            last_msg_ns: AtomicI64::new(0),
            last_book_ns: AtomicI64::new(0),
            last_bbo_ns: AtomicI64::new(0),
            last_book_exch_ms: AtomicI64::new(0),
            last_bbo_exch_ms: AtomicI64::new(0),
            rest_divergent: AtomicBool::new(false),
            generation: AtomicU64::new(0),
            bbo_generation: AtomicU64::new(0),
            wake,
            dirty: None,
        }
    }

    /// Like [`with_wake`] but also wired to a shared dirty-market bitset: every `publish`
    /// marks this market's index dirty so the strategy loop can reprice only changed markets.
    pub fn with_wake_and_dirty(wake: Arc<Notify>, dirty: Arc<super::dirty::DirtyMarkets>, idx: crate::types::MarketIdx) -> Self {
        let mut vb = Self::build(Some(wake));
        vb.dirty = Some((dirty, idx));
        vb
    }

    /// Hot publish: store the freshest book, refresh BOTH liveness stamps (a book is
    /// also a frame), bump the generation, and wake any attached strategy loop. Called
    /// from the ingest thread on every book snapshot. Wait-free for readers; no lock.
    #[inline]
    pub fn publish(&self, book: OrderBook) {
        if !accept_exch_ts(&self.last_book_exch_ms, exch_ms(&book)) {
            self.touch(); // frame arrived, but do not roll book content/generation backwards
            return;
        }
        let t0 = mono_now_ns();
        self.book.store(Some(Arc::new(book)));
        // Clear the hot-only guard only AFTER the raw book is installed: clearing first
        // opens a window where the strategy passes the has_hot_only_update gate and pairs
        // NEW hot data with the OLD raw book — exactly what the guard exists to prevent.
        self.hot_only_pending.store(false, Ordering::Release);
        let ns = mono_now_ns();
        self.last_book_ns.store(ns, Ordering::Release);
        self.last_msg_ns.store(ns, Ordering::Release);
        self.generation.fetch_add(1, Ordering::Release);
        crate::metrics::VENUE_PUBLISH.record((ns - t0) as u64);
        if let Some((dirty, idx)) = &self.dirty {
            dirty.mark(*idx);
        }
        if let Some(w) = &self.wake {
            w.notify_one();
        }
    }

    /// Hot publish: store both the raw `OrderBook` and its integer `HotBook` projection.
    /// Used when a `MarketScale` is available (live mode with `Tap.scale` set).
    #[inline]
    pub fn publish_hot(&self, book: OrderBook, hot: HotBook) {
        if !accept_exch_ts(&self.last_book_exch_ms, exch_ms(&book)) {
            self.touch();
            return;
        }
        let t0 = mono_now_ns();
        self.hot.store(Some(Arc::new(hot)));
        self.book.store(Some(Arc::new(book)));
        // Guard cleared only after BOTH stores (see publish()).
        self.hot_only_pending.store(false, Ordering::Release);
        let ns = mono_now_ns();
        self.last_book_ns.store(ns, Ordering::Release);
        self.last_msg_ns.store(ns, Ordering::Release);
        self.generation.fetch_add(1, Ordering::Release);
        crate::metrics::VENUE_PUBLISH.record((ns - t0) as u64);
        if let Some((dirty, idx)) = &self.dirty {
            dirty.mark(*idx);
        }
        if let Some(w) = &self.wake {
            w.notify_one();
        }
    }

    /// Publish only the integer L2 projection before the raw Decimal book is available.
    /// This wakes the strategy for risk-reducing fast cancels, but deliberately does NOT
    /// refresh `last_book_ns`; exact Decimal readers must still see the raw book's age.
    #[inline]
    pub fn publish_hot_only(&self, hot: HotBook, exch_ts: chrono::DateTime<chrono::Utc>) {
        if !accept_exch_ts(&self.last_book_exch_ms, exch_ts.timestamp_millis()) {
            self.touch();
            return;
        }
        let t0 = mono_now_ns();
        self.hot.store(Some(Arc::new(hot)));
        let ns = mono_now_ns();
        self.hot_only_pending.store(true, Ordering::Release);
        self.last_msg_ns.store(ns, Ordering::Release);
        self.generation.fetch_add(1, Ordering::Release);
        crate::metrics::VENUE_PUBLISH.record((ns - t0).max(0) as u64);
        if let Some((dirty, idx)) = &self.dirty {
            dirty.mark(*idx);
        }
        if let Some(w) = &self.wake {
            w.notify_one();
        }
    }

    /// Publish a fast one-level BBO assist. This updates connection/quote freshness
    /// and wakes the strategy, but deliberately does NOT update `book`/`last_book_ns`.
    #[inline]
    pub fn publish_bbo(&self, book: OrderBook) {
        if !accept_exch_ts(&self.last_bbo_exch_ms, exch_ms(&book)) {
            self.touch();
            return;
        }
        let t0 = mono_now_ns();
        self.bbo.store(Some(Arc::new(book)));
        // Guard cleared only after the raw BBO is installed (see publish()).
        self.bbo_hot_only_pending.store(false, Ordering::Release);
        let ns = mono_now_ns();
        self.last_bbo_ns.store(ns, Ordering::Release);
        self.last_msg_ns.store(ns, Ordering::Release);
        self.bbo_generation.fetch_add(1, Ordering::Release);
        crate::metrics::VENUE_PUBLISH.record((ns - t0) as u64);
        if let Some((dirty, idx)) = &self.dirty {
            dirty.mark(*idx);
        }
        if let Some(w) = &self.wake {
            w.notify_one();
        }
    }

    /// Publish a fast BBO assist plus its integer projection.
    #[inline]
    pub fn publish_bbo_hot(&self, book: OrderBook, hot: HotBook) {
        if !accept_exch_ts(&self.last_bbo_exch_ms, exch_ms(&book)) {
            self.touch();
            return;
        }
        let t0 = mono_now_ns();
        self.bbo_hot.store(Some(Arc::new(hot)));
        self.bbo.store(Some(Arc::new(book)));
        // Guard cleared only after BOTH stores (see publish()).
        self.bbo_hot_only_pending.store(false, Ordering::Release);
        let ns = mono_now_ns();
        self.last_bbo_ns.store(ns, Ordering::Release);
        self.last_msg_ns.store(ns, Ordering::Release);
        self.bbo_generation.fetch_add(1, Ordering::Release);
        crate::metrics::VENUE_PUBLISH.record((ns - t0) as u64);
        if let Some((dirty, idx)) = &self.dirty {
            dirty.mark(*idx);
        }
        if let Some(w) = &self.wake {
            w.notify_one();
        }
    }

    /// Publish only the integer BBO projection before the raw Decimal BBO is available.
    /// Safe because the strategy treats a pending hot-only update as cancel-only; raw
    /// BBO freshness is unchanged until the Decimal BBO is installed.
    #[inline]
    pub fn publish_bbo_hot_only(&self, hot: HotBook, exch_ts: chrono::DateTime<chrono::Utc>) {
        if !accept_exch_ts(&self.last_bbo_exch_ms, exch_ts.timestamp_millis()) {
            self.touch();
            return;
        }
        let t0 = mono_now_ns();
        self.bbo_hot.store(Some(Arc::new(hot)));
        let ns = mono_now_ns();
        self.bbo_hot_only_pending.store(true, Ordering::Release);
        self.last_msg_ns.store(ns, Ordering::Release);
        self.bbo_generation.fetch_add(1, Ordering::Release);
        crate::metrics::VENUE_PUBLISH.record((ns - t0).max(0) as u64);
        if let Some((dirty, idx)) = &self.dirty {
            dirty.mark(*idx);
        }
        if let Some(w) = &self.wake {
            w.notify_one();
        }
    }

    fn bbo_price_changed(&self, book: &OrderBook) -> bool {
        let old = self.bbo.load_full();
        let old = old.as_deref();
        let old_bid = old.and_then(|b| b.best_bid()).map(|l| l.px);
        let old_ask = old.and_then(|b| b.best_ask()).map(|l| l.px);
        let new_bid = book.best_bid().map(|l| l.px);
        let new_ask = book.best_ask().map(|l| l.px);
        old_bid != new_bid || old_ask != new_ask
    }

    /// Publish a fast BBO assist, but wake the strategy only when the top prices
    /// changed. Size-only updates still refresh BBO freshness and the stored BBO.
    #[inline]
    pub fn publish_bbo_price_wake(&self, book: OrderBook) {
        if !accept_exch_ts(&self.last_bbo_exch_ms, exch_ms(&book)) {
            self.touch();
            return;
        }
        let t0 = mono_now_ns();
        let price_changed = self.bbo_price_changed(&book);
        let had_hot_only = self.bbo_hot_only_pending.swap(false, Ordering::AcqRel);
        self.bbo.store(Some(Arc::new(book)));
        let ns = mono_now_ns();
        self.last_bbo_ns.store(ns, Ordering::Release);
        self.last_msg_ns.store(ns, Ordering::Release);
        crate::metrics::VENUE_PUBLISH.record((ns - t0).max(0) as u64);
        if price_changed || had_hot_only {
            self.bbo_generation.fetch_add(1, Ordering::Release);
            if let Some((dirty, idx)) = &self.dirty {
                dirty.mark(*idx);
            }
            if let Some(w) = &self.wake {
                w.notify_one();
            }
        }
    }

    /// Publish a coalesced BBO assist plus its integer projection.
    #[inline]
    pub fn publish_bbo_price_wake_hot(&self, book: OrderBook, hot: HotBook) {
        if !accept_exch_ts(&self.last_bbo_exch_ms, exch_ms(&book)) {
            self.touch();
            return;
        }
        let t0 = mono_now_ns();
        let price_changed = self.bbo_price_changed(&book);
        let had_hot_only = self.bbo_hot_only_pending.swap(false, Ordering::AcqRel);
        self.bbo_hot.store(Some(Arc::new(hot)));
        self.bbo.store(Some(Arc::new(book)));
        let ns = mono_now_ns();
        self.last_bbo_ns.store(ns, Ordering::Release);
        self.last_msg_ns.store(ns, Ordering::Release);
        crate::metrics::VENUE_PUBLISH.record((ns - t0).max(0) as u64);
        if price_changed || had_hot_only {
            self.bbo_generation.fetch_add(1, Ordering::Release);
            if let Some((dirty, idx)) = &self.dirty {
                dirty.mark(*idx);
            }
            if let Some(w) = &self.wake {
                w.notify_one();
            }
        }
    }

    /// Wait-free read of the latest hot book. `None` until the first hot publish, or
    /// always `None` if the publisher doesn't have a `MarketScale` (record mode).
    #[inline]
    pub fn load_hot(&self) -> Option<Arc<HotBook>> {
        self.hot.load_full()
    }

    /// Wait-free read of the latest fast BBO assist book.
    #[inline]
    pub fn load_bbo(&self) -> Option<Arc<OrderBook>> {
        self.bbo.load_full()
    }

    /// Wait-free read of the latest integer BBO assist book.
    #[inline]
    pub fn load_bbo_hot(&self) -> Option<Arc<HotBook>> {
        self.bbo_hot.load_full()
    }

    /// Current book version — bumped once per [`publish`], 0 before the first book.
    /// A strategy loop compares this to its last-seen value to detect a change.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Current BBO version. Zero before the first BBO update.
    #[inline]
    pub fn bbo_generation(&self) -> u64 {
        self.bbo_generation.load(Ordering::Acquire)
    }

    /// Generation used by quote loops: changes on either full-depth book updates or BBO updates.
    #[inline]
    pub fn quote_generation(&self) -> u64 {
        self.generation().wrapping_add(self.bbo_generation())
    }

    /// Liveness touch without a new book — call on every inbound frame (including
    /// trades and pongs) so a quiet-but-alive stream isn't flagged stale.
    #[inline]
    pub fn touch(&self) {
        let ns = mono_now_ns();
        self.last_msg_ns.store(ns, Ordering::Release);
    }

    /// Wait-free read of the latest book for the strategy loop. Cheap `Arc` clone.
    #[inline]
    pub fn load(&self) -> Option<Arc<OrderBook>> {
        self.book.load_full()
    }

    /// Monotonic-clock nanos of the last frame, or 0 if nothing has arrived yet.
    #[inline]
    pub fn last_msg_ns(&self) -> i64 {
        self.last_msg_ns.load(Ordering::Acquire)
    }

    /// Milliseconds since the last frame of ANY kind at `now_ns` (connection
    /// liveness). `i64::MAX` if nothing has arrived yet ("infinitely stale").
    #[inline]
    pub fn age_ms(&self, now_ns: i64) -> i64 {
        let last = self.last_msg_ns();
        if last == 0 {
            i64::MAX
        } else {
            now_ns.saturating_sub(last) / 1_000_000
        }
    }

    /// Monotonic-clock nanos of the last book snapshot, or 0 if none yet.
    #[inline]
    pub fn last_book_ns(&self) -> i64 {
        self.last_book_ns.load(Ordering::Acquire)
    }

    /// Monotonic-clock nanos of the last fast BBO update, or 0 if none yet.
    #[inline]
    pub fn last_bbo_ns(&self) -> i64 {
        self.last_bbo_ns.load(Ordering::Acquire)
    }

    /// Milliseconds since the last BOOK SNAPSHOT at `now_ns` — trading-data freshness,
    /// as opposed to [`age_ms`] (any-frame connection liveness). `i64::MAX` until the
    /// first book arrives. The trading gate must use this so a trades-only feed with a
    /// stale book is not mistaken for fresh.
    #[inline]
    pub fn book_age_ms(&self, now_ns: i64) -> i64 {
        let last = self.last_book_ns();
        if last == 0 {
            i64::MAX
        } else {
            now_ns.saturating_sub(last) / 1_000_000
        }
    }

    /// Milliseconds since the last fast BBO update. `i64::MAX` until the first BBO.
    #[inline]
    pub fn bbo_age_ms(&self, now_ns: i64) -> i64 {
        let last = self.last_bbo_ns();
        if last == 0 {
            i64::MAX
        } else {
            now_ns.saturating_sub(last) / 1_000_000
        }
    }

    /// Freshness of quote-touch data: full L2 book or fast BBO, whichever is newer.
    #[inline]
    pub fn quote_age_ms(&self, now_ns: i64) -> i64 {
        self.book_age_ms(now_ns).min(self.bbo_age_ms(now_ns))
    }

    /// Flag (or clear) a REST-confirmed content divergence on this book. Called only
    /// from the slow REST cross-check thread.
    #[inline]
    pub fn mark_divergent(&self, divergent: bool) {
        self.rest_divergent.store(divergent, Ordering::Release);
    }

    /// Whether the REST cross-check currently considers this book divergent.
    #[inline]
    pub fn is_divergent(&self) -> bool {
        self.rest_divergent.load(Ordering::Acquire)
    }

    /// True when the latest integer projection has arrived ahead of its matching raw Decimal book.
    /// Strategy code may use such a snapshot for risk-reducing fast cancels, but should skip exact
    /// quote placement until the raw publish clears this guard.
    #[inline]
    pub fn has_hot_only_update(&self) -> bool {
        self.hot_only_pending.load(Ordering::Acquire) || self.bbo_hot_only_pending.load(Ordering::Acquire)
    }
}

/// Lets a `VenueBook` be attached to a connector as its hot-path tap. The connector
/// only ever sees the core `BookTap` trait, never this hotpath type.
impl crate::connectors::BookTap for VenueBook {
    #[inline]
    fn publish(&self, book: OrderBook) {
        VenueBook::publish(self, book);
    }
    #[inline]
    fn touch(&self) {
        VenueBook::touch(self);
    }
    #[inline]
    fn publish_hot(&self, book: OrderBook, hot: HotBook) {
        VenueBook::publish_hot(self, book, hot);
    }
    #[inline]
    fn publish_hot_only(&self, hot: HotBook, exch_ts: chrono::DateTime<chrono::Utc>) {
        VenueBook::publish_hot_only(self, hot, exch_ts);
    }
    #[inline]
    fn publish_bbo(&self, book: OrderBook) {
        VenueBook::publish_bbo(self, book);
    }
    #[inline]
    fn publish_bbo_hot(&self, book: OrderBook, hot: HotBook) {
        VenueBook::publish_bbo_hot(self, book, hot);
    }
    #[inline]
    fn publish_bbo_hot_only(&self, hot: HotBook, exch_ts: chrono::DateTime<chrono::Utc>) {
        VenueBook::publish_bbo_hot_only(self, hot, exch_ts);
    }
    #[inline]
    fn publish_bbo_price_wake(&self, book: OrderBook) {
        VenueBook::publish_bbo_price_wake(self, book);
    }
    #[inline]
    fn publish_bbo_price_wake_hot(&self, book: OrderBook, hot: HotBook) {
        VenueBook::publish_bbo_price_wake_hot(self, book, hot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn book(bid: rust_decimal::Decimal) -> OrderBook {
        let now = Utc::now();
        OrderBook::from_levels(vec![(bid, dec!(1))], vec![(bid + dec!(1), dec!(1))], now, now)
    }

    #[test]
    fn starts_empty_and_infinitely_stale() {
        let vb = VenueBook::new();
        assert!(vb.load().is_none());
        assert_eq!(vb.last_msg_ns(), 0);
        assert_eq!(vb.age_ms(Utc::now().timestamp_nanos_opt().unwrap()), i64::MAX);
    }

    #[test]
    fn publish_then_load_returns_latest() {
        let vb = VenueBook::new();
        vb.publish(book(dec!(100)));
        let first = vb.load();
        assert_eq!(first.as_deref().unwrap().best_bid().unwrap().px, dec!(100));
        vb.publish(book(dec!(101)));
        let second = vb.load();
        assert_eq!(second.as_deref().unwrap().best_bid().unwrap().px, dec!(101));
        // The earlier Arc snapshot is unaffected by the later store (wait-free read).
        assert_eq!(first.as_deref().unwrap().best_bid().unwrap().px, dec!(100));
    }

    #[test]
    fn touch_refreshes_liveness() {
        let vb = VenueBook::new();
        vb.touch();
        let ns = vb.last_msg_ns();
        assert!(ns > 0);
        // age at a time just after the stamp is small and non-negative.
        let age = vb.age_ms(ns + 5_000_000); // +5ms in ns
        assert!((0..=10).contains(&age), "age {age}");
    }

    #[test]
    fn touch_keeps_book_stale_while_connection_is_live() {
        // A trades-only stream: frames arrive (touch) but no book is published.
        let vb = VenueBook::new();
        vb.touch();
        let now = vb.last_msg_ns();
        // Connection reads alive...
        assert!(vb.age_ms(now) < 10);
        // ...but no book has ever published, so trading-data freshness is infinite.
        assert_eq!(vb.book_age_ms(now), i64::MAX);
        // After a real snapshot, both stamps are fresh.
        vb.publish(book(dec!(100)));
        let t = vb.last_book_ns();
        assert!(t > 0);
        assert!(vb.age_ms(t) < 10);
        assert!(vb.book_age_ms(t) < 10);
    }

    #[test]
    fn divergence_flag_defaults_false_and_roundtrips() {
        let vb = VenueBook::new();
        assert!(!vb.is_divergent());
        vb.mark_divergent(true);
        assert!(vb.is_divergent());
        vb.mark_divergent(false);
        assert!(!vb.is_divergent());
    }

    #[test]
    fn generation_bumps_once_per_publish() {
        let vb = VenueBook::new();
        assert_eq!(vb.generation(), 0); // no book yet
        vb.publish(book(dec!(100)));
        assert_eq!(vb.generation(), 1);
        vb.publish(book(dec!(101)));
        assert_eq!(vb.generation(), 2);
        // touch() is liveness only — it must NOT bump the book generation.
        vb.touch();
        assert_eq!(vb.generation(), 2);
    }

    #[test]
    fn bbo_updates_quote_generation_without_overwriting_l2() {
        let vb = VenueBook::new();
        vb.publish(book(dec!(100)));
        let l2_gen = vb.generation();
        let quote_gen = vb.quote_generation();
        let now = Utc::now();
        let bbo = OrderBook::from_levels(vec![(dec!(101), dec!(2))], vec![(dec!(102), dec!(2))], now, now);
        vb.publish_bbo(bbo);
        assert_eq!(vb.generation(), l2_gen, "BBO must not mutate full-depth generation");
        assert!(vb.bbo_generation() > 0);
        assert!(vb.quote_generation() > quote_gen, "BBO must trigger quote-generation changes");
        assert_eq!(vb.load().as_deref().unwrap().best_bid().unwrap().px, dec!(100));
        assert_eq!(vb.load_bbo().as_deref().unwrap().best_bid().unwrap().px, dec!(101));
    }

    #[test]
    fn bbo_price_wake_coalesces_size_only_updates() {
        let vb = VenueBook::new();
        vb.publish(book(dec!(100)));
        let now = Utc::now();
        vb.publish_bbo_price_wake(OrderBook::from_levels(vec![(dec!(101), dec!(2))], vec![(dec!(102), dec!(2))], now, now));
        let quote_gen = vb.quote_generation();
        vb.publish_bbo_price_wake(OrderBook::from_levels(vec![(dec!(101), dec!(5))], vec![(dec!(102), dec!(6))], now, now));
        assert_eq!(vb.quote_generation(), quote_gen, "size-only BBO updates must not wake/reprice");
        assert_eq!(vb.load_bbo().as_deref().unwrap().best_bid().unwrap().qty, dec!(5));
        vb.publish_bbo_price_wake(OrderBook::from_levels(vec![(dec!(101.1), dec!(5))], vec![(dec!(102), dec!(6))], now, now));
        assert!(vb.quote_generation() > quote_gen, "top-price BBO updates must wake/reprice");
    }

    fn book_at(ms: i64, bid: rust_decimal::Decimal) -> OrderBook {
        let exch = chrono::DateTime::from_timestamp_millis(ms).unwrap();
        OrderBook::from_levels(vec![(bid, dec!(1))], vec![(bid + dec!(1), dec!(1))], exch, Utc::now())
    }

    #[test]
    fn stale_exchange_timestamp_does_not_roll_back_l2() {
        let vb = VenueBook::new();
        vb.publish(book_at(2_000, dec!(101)));
        let generation = vb.generation();
        vb.publish(book_at(1_000, dec!(99)));
        assert_eq!(vb.generation(), generation, "older exchange timestamp must not bump generation");
        assert_eq!(vb.load().as_deref().unwrap().best_bid().unwrap().px, dec!(101));
    }

    #[test]
    fn stale_exchange_timestamp_does_not_roll_back_bbo() {
        let vb = VenueBook::new();
        vb.publish_bbo_price_wake(book_at(2_000, dec!(101)));
        let quote_gen = vb.quote_generation();
        vb.publish_bbo_price_wake(book_at(1_000, dec!(99)));
        assert_eq!(vb.quote_generation(), quote_gen, "older BBO must not wake/reprice");
        assert_eq!(vb.load_bbo().as_deref().unwrap().best_bid().unwrap().px, dec!(101));
    }

    #[tokio::test]
    async fn publish_wakes_attached_strategy() {
        let wake = Arc::new(Notify::new());
        let vb = VenueBook::with_wake(wake.clone());
        // Pre-arm a permit by publishing before anyone parks: notify_one buffers it.
        vb.publish(book(dec!(100)));
        // The wakeup is delivered (would hang without the notify on publish).
        tokio::time::timeout(std::time::Duration::from_secs(1), wake.notified())
            .await
            .expect("publish must wake an attached strategy loop");
        assert_eq!(vb.generation(), 1);
    }

    #[test]
    fn publish_hot_stores_both_raw_and_hot() {
        use crate::livebot::scale::{build_hot_book, MarketScale};
        let vb = VenueBook::new();
        let b = book(dec!(100));
        let scale = MarketScale { tick: dec!(0.1), step: dec!(0.001), hl_qty_step: dec!(0.001) };
        let hot = build_hot_book(&b, &scale, 0, 12345);
        vb.publish_hot(b, hot);
        assert!(vb.load().is_some());
        let h = vb.load_hot();
        let h = h.as_deref().unwrap();
        assert_eq!(h.best_bid_ticks(), Some(1000)); // 100.0 / 0.1
        assert_eq!(vb.generation(), 1);
    }

    #[test]
    fn hot_only_publish_sets_guard_until_raw_publish() {
        use crate::livebot::scale::{build_hot_book, MarketScale};
        let vb = VenueBook::new();
        let b = book(dec!(100));
        let scale = MarketScale { tick: dec!(0.1), step: dec!(0.001), hl_qty_step: dec!(0.001) };
        let hot = build_hot_book(&b, &scale, 0, 12345);
        let exch_ts = b.exch_ts.clone();
        vb.publish_hot_only(hot, exch_ts);
        assert!(vb.load_hot().is_some());
        assert!(vb.has_hot_only_update());
        assert_eq!(vb.last_book_ns(), 0, "hot-only must not refresh raw L2 freshness");
        vb.publish_hot(b, hot);
        assert!(!vb.has_hot_only_update());
        assert!(vb.last_book_ns() > 0);
    }

    #[test]
    fn bbo_hot_only_does_not_refresh_raw_bbo_freshness() {
        use crate::livebot::scale::{build_hot_book, MarketScale};
        let vb = VenueBook::new();
        let b = book(dec!(100));
        let scale = MarketScale { tick: dec!(0.1), step: dec!(0.001), hl_qty_step: dec!(0.001) };
        let hot = build_hot_book(&b, &scale, 0, 12345);
        vb.publish_bbo_hot_only(hot, b.exch_ts.clone());
        assert!(vb.load_bbo_hot().is_some());
        assert!(vb.has_hot_only_update());
        assert_eq!(vb.last_bbo_ns(), 0, "BBO hot-only must not refresh raw BBO freshness");
        vb.publish_bbo_hot(b, hot);
        assert!(!vb.has_hot_only_update());
        assert!(vb.last_bbo_ns() > 0);
    }

    #[test]
    fn plain_publish_leaves_hot_none() {
        let vb = VenueBook::new();
        vb.publish(book(dec!(100)));
        assert!(vb.load().is_some());
        assert!(vb.load_hot().is_none());
    }

    #[tokio::test]
    async fn publish_hot_bumps_generation_and_wakes() {
        use crate::livebot::scale::{build_hot_book, MarketScale};
        let wake = Arc::new(Notify::new());
        let vb = VenueBook::with_wake(wake.clone());
        let b = book(dec!(100));
        let scale = MarketScale { tick: dec!(0.1), step: dec!(0.001), hl_qty_step: dec!(0.001) };
        let hot = build_hot_book(&b, &scale, 0, 12345);
        vb.publish_hot(b, hot);
        assert_eq!(vb.generation(), 1);
        assert!(vb.last_book_ns() > 0);
        tokio::time::timeout(std::time::Duration::from_secs(1), wake.notified())
            .await
            .expect("publish_hot must wake an attached strategy loop");
    }
}
