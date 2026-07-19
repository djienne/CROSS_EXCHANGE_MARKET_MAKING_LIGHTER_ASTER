//! Live market-data connectors (Aster + Lighter) and one-shot REST spec fetch.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{Sink, SinkExt};
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::book::OrderBook;
use crate::events::{EventKind, PriceLevel};
use crate::types::MarketId;
use chrono::{DateTime, Utc};

pub mod aster;
pub mod lighter;
pub mod rest_book;
pub mod rest_specs;


/// Cold/canonical market-data event sink used by websocket readers.
///
/// `record` and `verify-books` use the lossless unbounded variant so their tapes remain complete.
/// `livebot` uses the bounded lossy variant: hot-path `VenueBook` publication happens independently,
/// and a stalled cold recorder increments an explicit drop counter instead of growing memory without
/// bound or backpressuring websocket keepalives.
#[derive(Clone)]
pub enum EventSink {
    Lossless(mpsc::UnboundedSender<(MarketId, EventKind)>),
    Lossy {
        tx: mpsc::Sender<(MarketId, EventKind)>,
        dropped: Arc<AtomicU64>,
    },
}

impl EventSink {
    pub fn lossless(tx: mpsc::UnboundedSender<(MarketId, EventKind)>) -> Self {
        EventSink::Lossless(tx)
    }

    pub fn lossy(tx: mpsc::Sender<(MarketId, EventKind)>, dropped: Arc<AtomicU64>) -> Self {
        EventSink::Lossy { tx, dropped }
    }

    /// Non-blocking send from a websocket reader. Lossless mode preserves the old unbounded
    /// recorder behavior; lossy mode drops when the cold channel is full/closed and records it.
    #[inline]
    pub fn send(&self, market: MarketId, kind: EventKind) {
        match self {
            EventSink::Lossless(tx) => {
                let _ = tx.send((market, kind));
            }
            EventSink::Lossy { tx, dropped } => {
                if tx.try_send((market, kind)).is_err() {
                    dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    #[inline]
    pub fn dropped(&self) -> u64 {
        match self {
            EventSink::Lossless(_) => 0,
            EventSink::Lossy { dropped, .. } => dropped.load(Ordering::Relaxed),
        }
    }
}

/// A sink for the freshest book on the live hot path. Implemented by
/// `hotpath::VenueBook`, but defined here (core, hotpath-agnostic) so the connectors
/// can fan out a copy of each book without ever depending on the `hotpath` module —
/// keeping `record` buildable with `--no-default-features`.
///
/// `publish` is called on each book snapshot; `touch` on every other inbound frame
/// (trades, pongs) so a quiet-but-alive stream still reads as fresh.
pub trait BookTap: Send + Sync {
    fn publish(&self, book: OrderBook);
    fn touch(&self);
    /// Publish both the raw `OrderBook` and the integer `HotBook`. Default
    /// implementation ignores the hot book (backward compat for record mode).
    fn publish_hot(&self, book: OrderBook, _hot: crate::hot_types::HotBook) {
        self.publish(book);
    }
    /// Publish only the integer L2 projection before the raw Decimal book is ready.
    /// Default just stamps liveness; real hot cells use it for cancel-only prechecks.
    fn publish_hot_only(&self, _hot: crate::hot_types::HotBook, _exch_ts: DateTime<Utc>) {
        self.touch();
    }
    /// Publish a fast one-level best bid/ask assist. Default no-op keeps record mode
    /// and venues without a BBO assist unchanged.
    fn publish_bbo(&self, _book: OrderBook) {}
    /// Publish a fast BBO assist plus its integer projection.
    fn publish_bbo_hot(&self, book: OrderBook, _hot: crate::hot_types::HotBook) {
        self.publish_bbo(book);
    }
    /// Publish only the integer BBO projection before the raw Decimal BBO is ready.
    fn publish_bbo_hot_only(&self, _hot: crate::hot_types::HotBook, _exch_ts: DateTime<Utc>) {
        self.touch();
    }
    /// Publish a BBO assist but wake/reprice only when top prices changed. Venues
    /// whose BBO size affects quote safety should keep using `publish_bbo`.
    fn publish_bbo_price_wake(&self, book: OrderBook) {
        self.publish_bbo(book);
    }
    fn publish_bbo_price_wake_hot(&self, book: OrderBook, _hot: crate::hot_types::HotBook) {
        self.publish_bbo_price_wake(book);
    }
    /// Notify the cell that the venue stream is KNOWN down (disconnect/error/gap-resync),
    /// so the stored book must not be trusted until a full snapshot lands. Default no-op
    /// keeps record mode unchanged.
    fn mark_stream_down(&self) {}
}

/// The optional hot-path side outputs threaded into a connector reader. Both are
/// `None` in `record` mode, so the reader is behaviorally identical to before.
#[derive(Clone)]
pub struct Tap {
    /// Lock-free latest-book cell to publish into (the live strategy reads it).
    pub book: Option<Arc<dyn BookTap>>,
    /// Edge-triggered "drop and reconnect" signal from the stream watchdog.
    pub reconnect: Option<Arc<Notify>>,
    /// When set, `publish` builds a `HotBook` alongside the raw `OrderBook` and calls
    /// `BookTap::publish_hot` for wait-free integer reads on the strategy loop.
    #[cfg(feature = "hotpath")]
    pub scale: Option<crate::livebot::scale::MarketScale>,
    #[cfg(feature = "hotpath")]
    pub qty_scale: crate::livebot::scale::HotQtyScale,
}

impl Default for Tap {
    fn default() -> Self {
        Tap {
            book: None,
            reconnect: None,
            #[cfg(feature = "hotpath")]
            scale: None,
            #[cfg(feature = "hotpath")]
            qty_scale: crate::livebot::scale::HotQtyScale::Aster,
        }
    }
}

impl Tap {
    /// No hot-path outputs — the `record` path.
    pub fn none() -> Self {
        Tap::default()
    }

    /// Build the integer hot book directly from exchange decimal strings. This avoids
    /// converting websocket levels to `rust_decimal::Decimal` just to convert them back
    /// into ticks/lots for the strategy precheck.
    #[cfg(feature = "hotpath")]
    #[inline]
    pub(crate) fn hot_book_from_raw<'a, I, J>(
        &self,
        bids: I,
        asks: J,
        exch_ts: DateTime<Utc>,
    ) -> Option<(crate::hot_types::HotBook, i64)>
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
        J: IntoIterator<Item = (&'a str, &'a str)>,
    {
        let scale = self.scale.as_ref()?;
        let t0 = crate::hotpath::clock::mono_now_ns();
        let recv_ns = crate::hotpath::clock::mono_now_ns();
        let hot = crate::livebot::scale::build_hot_book_from_strs_with_qty_scale(
            bids,
            asks,
            scale,
            self.qty_scale,
            0,
            recv_ns,
            exch_ts.timestamp_millis(),
        );
        let done_ns = crate::hotpath::clock::mono_now_ns();
        crate::metrics::BOOK_BUILD.record((done_ns - t0).max(0) as u64);
        Some((hot, recv_ns))
    }

    /// Build the integer hot book from already-parsed Decimal levels (best-first) — the
    /// numeric sibling of [`Tap::hot_book_from_raw`] for connectors that no longer
    /// format levels as strings. Same stamps and metric.
    #[cfg(feature = "hotpath")]
    #[inline]
    pub(crate) fn hot_book_from_levels(
        &self,
        bids: &[PriceLevel],
        asks: &[PriceLevel],
        exch_ts: DateTime<Utc>,
    ) -> Option<(crate::hot_types::HotBook, i64)> {
        let scale = self.scale.as_ref()?;
        let t0 = crate::hotpath::clock::mono_now_ns();
        let recv_ns = crate::hotpath::clock::mono_now_ns();
        let hot = crate::livebot::scale::build_hot_book_from_dec_levels_with_qty_scale(
            bids,
            asks,
            scale,
            self.qty_scale,
            0,
            recv_ns,
            exch_ts.timestamp_millis(),
        );
        let done_ns = crate::hotpath::clock::mono_now_ns();
        crate::metrics::BOOK_BUILD.record((done_ns - t0).max(0) as u64);
        Some((hot, recv_ns))
    }

    /// Publish only a prebuilt integer L2 snapshot before the raw Decimal book is built.
    /// The hot cell marks this as cancel-only until the subsequent full publish clears it.
    #[cfg(feature = "hotpath")]
    #[inline]
    pub(crate) fn publish_hot_only(&self, hot: crate::hot_types::HotBook, exch_ts: DateTime<Utc>) {
        if let Some(cell) = &self.book {
            cell.publish_hot_only(hot, exch_ts);
        }
    }

    /// Publish a fresh book, optionally reusing a `HotBook` that was built directly from
    /// the websocket strings before the cold Decimal parse.
    #[inline]
    pub(crate) fn publish_prebuilt(
        &self,
        bids: &[PriceLevel],
        asks: &[PriceLevel],
        exch_ts: DateTime<Utc>,
        #[cfg(feature = "hotpath")] prebuilt_hot: Option<(crate::hot_types::HotBook, i64)>,
        #[cfg(not(feature = "hotpath"))] _prebuilt_hot: Option<(crate::hot_types::HotBook, i64)>,
    ) {
        if let Some(cell) = &self.book {
            #[cfg(feature = "hotpath")]
            let t0 = crate::hotpath::clock::mono_now_ns();
            let book = OrderBook::from_levels(bids.iter().copied(), asks.iter().copied(), exch_ts, Utc::now());
            #[cfg(feature = "hotpath")]
            let recv_ns = prebuilt_hot
                .as_ref()
                .map(|(_, ns)| *ns)
                .unwrap_or_else(crate::hotpath::clock::mono_now_ns);
            #[cfg(feature = "hotpath")]
            if prebuilt_hot.is_none() {
                crate::metrics::BOOK_BUILD.record((recv_ns - t0).max(0) as u64);
            }
            #[cfg(feature = "hotpath")]
            if let Some(scale) = &self.scale {
                let hot = prebuilt_hot
                    .map(|(hot, _)| hot)
                    .unwrap_or_else(|| {
                        crate::livebot::scale::build_hot_book_with_qty_scale(
                            &book,
                            scale,
                            self.qty_scale,
                            0,
                            recv_ns,
                        )
                    });
                cell.publish_hot(book, hot);
                return;
            }
            cell.publish(book);
        }
    }

    /// Publish only a prebuilt integer BBO before the raw Decimal BBO is built.
    #[cfg(feature = "hotpath")]
    #[inline]
    pub(crate) fn publish_bbo_hot_only(&self, hot: crate::hot_types::HotBook, exch_ts: DateTime<Utc>) {
        if let Some(cell) = &self.book {
            cell.publish_bbo_hot_only(hot, exch_ts);
        }
    }

    /// Publish a BBO assist, optionally reusing an already-built integer projection.
    #[inline]
    pub(crate) fn publish_bbo_prebuilt(
        &self,
        bid: PriceLevel,
        ask: PriceLevel,
        exch_ts: DateTime<Utc>,
        #[cfg(feature = "hotpath")] prebuilt_hot: Option<(crate::hot_types::HotBook, i64)>,
        #[cfg(not(feature = "hotpath"))] _prebuilt_hot: Option<(crate::hot_types::HotBook, i64)>,
    ) {
        if let Some(cell) = &self.book {
            #[cfg(feature = "hotpath")]
            let t0 = crate::hotpath::clock::mono_now_ns();
            let book = OrderBook::from_levels([bid], [ask], exch_ts, Utc::now());
            #[cfg(feature = "hotpath")]
            let recv_ns = prebuilt_hot
                .as_ref()
                .map(|(_, ns)| *ns)
                .unwrap_or_else(crate::hotpath::clock::mono_now_ns);
            #[cfg(feature = "hotpath")]
            if prebuilt_hot.is_none() {
                crate::metrics::BOOK_BUILD.record((recv_ns - t0).max(0) as u64);
            }
            #[cfg(feature = "hotpath")]
            if let Some(scale) = &self.scale {
                let hot = prebuilt_hot
                    .map(|(hot, _)| hot)
                    .unwrap_or_else(|| {
                        crate::livebot::scale::build_hot_book_with_qty_scale(
                            &book,
                            scale,
                            self.qty_scale,
                            0,
                            recv_ns,
                        )
                    });
                cell.publish_bbo_hot(book, hot);
                return;
            }
            cell.publish_bbo(book);
        }
    }

    /// Publish a BBO assist with a COALESCED wake: data + freshness always stored, the
    /// strategy woken only when the top prices changed (or a hot-only publish was
    /// pending). For a BBO that mirrors an L2 frame whose own publish already woke the
    /// strategy, the unconditional wake of `publish_bbo_prebuilt` is pure overhead.
    #[inline]
    pub(crate) fn publish_bbo_price_wake_prebuilt(
        &self,
        bid: PriceLevel,
        ask: PriceLevel,
        exch_ts: DateTime<Utc>,
        #[cfg(feature = "hotpath")] prebuilt_hot: Option<(crate::hot_types::HotBook, i64)>,
        #[cfg(not(feature = "hotpath"))] _prebuilt_hot: Option<(crate::hot_types::HotBook, i64)>,
    ) {
        if let Some(cell) = &self.book {
            let book = OrderBook::from_levels([bid], [ask], exch_ts, Utc::now());
            #[cfg(feature = "hotpath")]
            if let Some(scale) = &self.scale {
                let recv_ns = prebuilt_hot
                    .as_ref()
                    .map(|(_, ns)| *ns)
                    .unwrap_or_else(crate::hotpath::clock::mono_now_ns);
                let hot = prebuilt_hot.map(|(hot, _)| hot).unwrap_or_else(|| {
                    crate::livebot::scale::build_hot_book_with_qty_scale(
                        &book,
                        scale,
                        self.qty_scale,
                        0,
                        recv_ns,
                    )
                });
                cell.publish_bbo_price_wake_hot(book, hot);
                return;
            }
            cell.publish_bbo_price_wake(book);
        }
    }

    /// Stamp liveness on any inbound frame.
    #[inline]
    fn touch(&self) {
        if let Some(cell) = &self.book {
            cell.touch();
        }
    }

    /// Flag the attached book cell as stream-down (no-op without a cell, e.g. record mode).
    #[inline]
    pub(crate) fn mark_stream_down(&self) {
        if let Some(cell) = &self.book {
            cell.mark_stream_down();
        }
    }

    /// Await a watchdog reconnect request. When no signal is attached (`record`
    /// mode) the returned future never resolves, so its `select!` arm never fires.
    async fn wait_reconnect(&self) {
        match &self.reconnect {
            Some(n) => n.notified().await,
            None => std::future::pending::<()>().await,
        }
    }
}

/// How long a keepalive/control write may block before we treat the socket's write
/// side as wedged. A healthy send completes in well under a millisecond; this only
/// fires on a genuinely stalled sink.
pub(crate) const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Send a control/keepalive frame (Pong, subscribe, ping) with a stall guard.
/// Awaiting `write.send(...)` directly inside the reader's `select!` means a wedged
/// write side would starve the idle-timeout and reconnect arms; bounding the send
/// lets the reader bail and self-heal instead of hanging until the OS socket timeout.
pub(crate) async fn send_guarded<S>(write: &mut S, msg: Message) -> anyhow::Result<()>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    match tokio::time::timeout(WRITE_TIMEOUT, write.send(msg)).await {
        Ok(res) => res.map_err(anyhow::Error::from),
        Err(_) => anyhow::bail!("ws write stalled >{}s, forcing reconnect", WRITE_TIMEOUT.as_secs()),
    }
}
