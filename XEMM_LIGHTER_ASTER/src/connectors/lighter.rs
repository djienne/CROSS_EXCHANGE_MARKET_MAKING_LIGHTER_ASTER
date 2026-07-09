//! Lighter market-data WebSocket connector. Lighter sends an initial snapshot and
//! incremental deltas on `order_book/{market_id}`, so this connector maintains a
//! local book before publishing full book snapshots to the rest of XEMM.

use chrono::Utc;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tracing::warn;

use serde::Deserialize;

use super::{EventSink, Tap};
use crate::decimal::dec_from_f64_book;
use crate::events::{EventKind, PriceLevel};
use crate::lighter::local_book::LocalBook;
use crate::lighter::messages::{BookUpdateContiguity, OrderBookMsgRef, PriceLevelRef};
use crate::lighter::ws::{subscribe_loop, SubscribeOptions};
use crate::types::MarketId;

const PUBLISH_LEVELS: usize = 20;

#[derive(Default)]
struct StreamState {
    book: LocalBook,
    last_nonce: Option<i64>,
    /// Lifetime count of gap-forced resyncs on this stream — surfaced in the gap warn so
    /// reconnect churn (e.g. from wrong sequence assumptions) is visible in logs.
    gap_resyncs: u64,
}

impl StreamState {
    fn reset(&mut self) {
        self.book.reset();
        self.last_nonce = None;
    }
}

pub async fn run(
    market_id: u32,
    label: String,
    market: MarketId,
    tx: tokio::sync::mpsc::UnboundedSender<(MarketId, EventKind)>,
) {
    run_with_tap(
        market_id,
        label,
        market,
        EventSink::lossless(tx),
        Tap::none(),
    )
    .await
}

pub async fn run_with_tap(
    market_id: u32,
    label: String,
    market: MarketId,
    tx: EventSink,
    tap: Tap,
) {
    let channel = format!("order_book/{market_id}");
    let mut opts = SubscribeOptions::new(
        &format!("lighter-order-book-{label}-{market_id}"),
        vec![channel],
    );
    // This is the hedge-source L2 feed: every sequence-gap resync blanks the book and
    // then waits out the reconnect delay. The 5s default means a ≥5-6s dark window per
    // gap (Aster's equivalent base is 1s); 0.5s keeps resyncs prompt while consecutive
    // failures still escalate toward reconnect_max.
    opts.reconnect_base = 0.5;
    let reconnect = tap
        .reconnect
        .clone()
        .unwrap_or_else(|| Arc::new(Notify::new()));
    let state = Arc::new(Mutex::new(StreamState::default()));
    let state_for_disconnect = state.clone();
    let reconnect_on_gap = reconnect.clone();
    subscribe_loop(
        opts,
        Some(reconnect),
        move |data| {
            let mut state = state.lock().expect("Lighter stream book state poisoned");
            if !handle_value(data, &market, &tx, &tap, &mut state) {
                state.gap_resyncs += 1;
                warn!(
                    "Lighter order_book sequence gap for market {} (resync #{}); reconnecting for fresh snapshot",
                    market_id, state.gap_resyncs
                );
                state.reset();
                reconnect_on_gap.notify_one();
            }
        },
        move || {
            state_for_disconnect
                .lock()
                .expect("Lighter stream book state poisoned")
                .reset();
        },
    )
    .await;
}

fn handle_value(
    data: &serde_json::Value,
    market: &MarketId,
    tx: &EventSink,
    tap: &Tap,
    state: &mut StreamState,
) -> bool {
    // Borrowed deserialize straight from the routed `Value`: no deep clone, no
    // re-parse, no per-level String allocations on the hedge-source ingest thread.
    let msg = match OrderBookMsgRef::deserialize(data) {
        Ok(m) => m,
        Err(_) => return true,
    };
    if !msg.is_snapshot() {
        if !state.book.initialized {
            // A delta before the subscribe snapshot has nothing to apply to; seeding the
            // book from it would publish a nearly-empty top-of-book. Resync instead.
            return false;
        }
        match msg.contiguity(state.last_nonce, state.book.last_offset) {
            BookUpdateContiguity::Apply => {}
            BookUpdateContiguity::SkipStale => return true, // duplicate/replay: keep the book
            BookUpdateContiguity::Gap => return false,
        }
    }
    let bids_f = parse_lighter_levels(&msg.order_book.bids);
    let asks_f = parse_lighter_levels(&msg.order_book.asks);
    if msg.is_snapshot() || !state.book.initialized {
        state.book.apply_snapshot(bids_f, asks_f);
    } else {
        state.book.apply_delta(&bids_f, &asks_f);
    }
    state.book.last_offset = msg.effective_offset().or(state.book.last_offset);
    state.last_nonce = msg.order_book.nonce.or(state.last_nonce);
    if !state.book.initialized {
        return true;
    }
    let exch_ts = Utc::now();
    // Same publishability gate as the old string path: both sides non-empty.
    if state.book.bids.is_empty() || state.book.asks.is_empty() {
        return true;
    }
    // Numeric top-20 straight off the local book: dec_from_f64_book reproduces the
    // legacy format!("{v:.12}")+trim string path bit-for-bit (pinned by tests), so
    // the tape stays byte-identical while skipping ~80 String allocations per frame.
    let bid_levels: Vec<PriceLevel> = state
        .book
        .bids
        .top_descending(PUBLISH_LEVELS)
        .filter_map(|(p, q)| Some((dec_from_f64_book(p)?, dec_from_f64_book(q)?)))
        .collect();
    let ask_levels: Vec<PriceLevel> = state
        .book
        .asks
        .top_ascending(PUBLISH_LEVELS)
        .filter_map(|(p, q)| Some((dec_from_f64_book(p)?, dec_from_f64_book(q)?)))
        .collect();
    #[cfg(feature = "hotpath")]
    let prebuilt_hot = tap.hot_book_from_levels(&bid_levels, &ask_levels, exch_ts);
    #[cfg(feature = "hotpath")]
    if let Some((hot, _)) = prebuilt_hot.as_ref() {
        // Integer projection first (mirrors the Aster connector): fast-cancel
        // prechecks see the move before the raw Decimal book is installed.
        tap.publish_hot_only(*hot, exch_ts);
    }
    #[cfg(not(feature = "hotpath"))]
    let prebuilt_hot = None;
    tap.publish_prebuilt(&bid_levels, &ask_levels, exch_ts, prebuilt_hot);
    // Lighter has no separate bookTicker stream (Aster does), so mirror the L2
    // top-of-book into the optional BBO fast-path slot. Without this the slot is
    // never populated: the hedge always takes the slower L2 walk and qdiag shows a
    // misleading hl_bbo=none / age=i64::MAX. Same data and freshness as this L2
    // frame, so hedge pricing is unchanged — it only lets the BBO fast path engage.
    if let (Some(&bid_top), Some(&ask_top)) = (bid_levels.first(), ask_levels.first()) {
        #[cfg(feature = "hotpath")]
        let bbo_hot = tap.hot_book_from_levels(
            std::slice::from_ref(&bid_top),
            std::slice::from_ref(&ask_top),
            exch_ts,
        );
        #[cfg(feature = "hotpath")]
        if let Some((hot, _)) = bbo_hot.as_ref() {
            tap.publish_bbo_hot_only(*hot, exch_ts);
        }
        #[cfg(not(feature = "hotpath"))]
        let bbo_hot = None;
        tap.publish_bbo_prebuilt(bid_top, ask_top, exch_ts, bbo_hot);
    }
    tx.send(
        market.clone(),
        EventKind::HlL2Book {
            bids: bid_levels,
            asks: ask_levels,
            exch_ts,
        },
    );
    tap.touch();
    true
}

fn parse_lighter_levels(levels: &[PriceLevelRef<'_>]) -> Vec<(f64, f64)> {
    levels
        .iter()
        .filter_map(|l| {
            let (p, q) = l.parsed();
            (p > 0.0 && q >= 0.0).then_some((p, q))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn published_levels_match_legacy_string_formatting() {
        // The numeric Decimal path must serialize exactly like the old
        // format!("{v:.12}")+trim string path, byte for byte on the tape.
        let legacy = |v: f64| {
            let s = format!("{v:.12}");
            s.trim_end_matches('0').trim_end_matches('.').to_string()
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = EventSink::lossless(tx);
        let tap = Tap::none();
        let market = MarketId("BTC".to_string());
        let mut state = StreamState::default();
        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "offset": 1,
            "order_book": {
                "bids": [
                    {"price": "64820.2", "size": "0.00051"},
                    {"price": "0.30000000000000004", "size": "1"}
                ],
                "asks": [{"price": "64820.3", "size": "0.19283"}],
                "offset": 1
            }
        });
        assert!(handle_value(&snapshot, &market, &sink, &tap, &mut state));
        let (_, kind) = rx.try_recv().expect("book published");
        let EventKind::HlL2Book { bids, asks, .. } = kind else {
            panic!("expected HlL2Book event");
        };
        // Bids best-first (highest price), asks best-first (lowest price).
        assert_eq!(bids.len(), 2);
        assert_eq!(bids[0].0.to_string(), legacy(64820.2)); // "64820.199999999997"
        assert_eq!(bids[0].1.to_string(), legacy(0.00051));
        assert_eq!(bids[1].0.to_string(), legacy(0.30000000000000004)); // "0.3"
        assert_eq!(bids[1].1.to_string(), "1");
        assert_eq!(asks[0].0.to_string(), legacy(64820.3));
        assert_eq!(asks[0].1.to_string(), legacy(0.19283));
    }

    #[test]
    fn handle_value_mirrors_l2_top_into_bbo_slot() {
        // Lighter has no bookTicker stream; the connector mirrors the L2 top-of-book
        // into the BBO slot so the hedge fast path can engage and qdiag stops showing
        // hl_bbo=none. Regression guard: the slot must be populated with exactly the
        // top level of each side.
        use crate::connectors::BookTap;
        use crate::hotpath::book_cell::VenueBook;
        use std::sync::Arc;

        let (tx, _rx) = mpsc::unbounded_channel();
        let sink = EventSink::lossless(tx);
        let cell = Arc::new(VenueBook::new());
        let tap = Tap { book: Some(cell.clone() as Arc<dyn BookTap>), ..Tap::none() };
        let market = MarketId("BTC".to_string());
        let mut state = StreamState::default();
        // Prices chosen exactly representable in f64 so the dec_from_f64_book path
        // yields clean strings.
        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "offset": 1,
            "order_book": {
                "bids": [{"price": "100.5", "size": "3"}, {"price": "100.25", "size": "5"}],
                "asks": [{"price": "100.75", "size": "4"}, {"price": "101", "size": "6"}],
                "offset": 1
            }
        });
        assert!(handle_value(&snapshot, &market, &sink, &tap, &mut state));

        let bbo = cell.load_bbo().expect("BBO slot populated from L2 top");
        let bid = bbo.best_bid().expect("bbo bid");
        let ask = bbo.best_ask().expect("bbo ask");
        assert_eq!(bid.px.to_string(), "100.5");
        assert_eq!(bid.qty.to_string(), "3");
        assert_eq!(ask.px.to_string(), "100.75");
        assert_eq!(ask.qty.to_string(), "4");
        // A 1-level mirror: the second level must not leak into the BBO book.
        assert!(bbo.bids.len() == 1 && bbo.asks.len() == 1);
    }

    #[test]
    fn handle_value_detects_orderbook_nonce_gap() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = EventSink::lossless(tx);
        let tap = Tap::none();
        let market = MarketId("BTC".to_string());
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(handle_value(&snapshot, &market, &sink, &tap, &mut state));
        assert_eq!(state.last_nonce, Some(10));
        assert!(rx.try_recv().is_ok());

        // begin_nonce ahead of our position => updates were missed => resync.
        let gap = serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "begin_nonce": 11,
                "nonce": 12,
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        });
        assert!(!handle_value(&gap, &market, &sink, &tap, &mut state));
    }

    #[test]
    fn handle_value_applies_forward_extending_nonce_overlap() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = EventSink::lossless(tx);
        let tap = Tap::none();
        let market = MarketId("BTC".to_string());
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(handle_value(&snapshot, &market, &sink, &tap, &mut state));
        let _ = rx.try_recv();

        // Levels carry absolute sizes, so an overlap that extends forward is safe to apply.
        let overlap = serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "begin_nonce": 9,
                "nonce": 11,
                "bids": [{"price": "100", "size": "3"}],
                "asks": []
            }
        });
        assert!(handle_value(&overlap, &market, &sink, &tap, &mut state));
        assert_eq!(state.last_nonce, Some(11));
        assert!(rx.try_recv().is_ok(), "applied overlap must publish");
    }

    #[test]
    fn handle_value_skips_stale_nonce_replay_without_resync() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = EventSink::lossless(tx);
        let tap = Tap::none();
        let market = MarketId("BTC".to_string());
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(handle_value(&snapshot, &market, &sink, &tap, &mut state));
        let _ = rx.try_recv();

        // Ends at-or-before our position: a replay. Dropped, book kept, no resync.
        let stale = serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "begin_nonce": 8,
                "nonce": 9,
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        });
        assert!(handle_value(&stale, &market, &sink, &tap, &mut state));
        assert_eq!(state.last_nonce, Some(10), "stale replay must not move the position");
        assert!(rx.try_recv().is_err(), "stale replay must not publish");
    }

    #[test]
    fn handle_value_detects_orderbook_offset_gap_without_nonce() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let sink = EventSink::lossless(tx);
        let tap = Tap::none();
        let market = MarketId("BTC".to_string());
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "offset": 10,
            "order_book": {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(handle_value(&snapshot, &market, &sink, &tap, &mut state));
        assert_eq!(state.book.last_offset, Some(10));

        let gap = serde_json::json!({
            "type": "update/order_book",
            "offset": 12,
            "order_book": {
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        });
        assert!(!handle_value(&gap, &market, &sink, &tap, &mut state));
    }

    #[test]
    fn handle_value_skips_duplicate_offset_without_resync() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = EventSink::lossless(tx);
        let tap = Tap::none();
        let market = MarketId("BTC".to_string());
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "offset": 10,
            "order_book": {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(handle_value(&snapshot, &market, &sink, &tap, &mut state));
        let _ = rx.try_recv();

        // Same offset re-delivered: a duplicate, not a gap — no reconnect churn.
        let dup = serde_json::json!({
            "type": "update/order_book",
            "offset": 10,
            "order_book": {
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        });
        assert!(handle_value(&dup, &market, &sink, &tap, &mut state));
        assert_eq!(state.book.last_offset, Some(10));
        assert!(rx.try_recv().is_err(), "duplicate must not publish");

        // The next contiguous delta still applies and preserves a known offset even if
        // the message itself omits one elsewhere in the pipeline.
        let next = serde_json::json!({
            "type": "update/order_book",
            "offset": 11,
            "order_book": {
                "bids": [{"price": "100", "size": "2"}],
                "asks": []
            }
        });
        assert!(handle_value(&next, &market, &sink, &tap, &mut state));
        assert_eq!(state.book.last_offset, Some(11));
    }

    #[test]
    fn handle_value_resyncs_on_delta_before_snapshot() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let sink = EventSink::lossless(tx);
        let tap = Tap::none();
        let market = MarketId("BTC".to_string());
        let mut state = StreamState::default();

        // A delta with no snapshot to apply it to must never seed the book.
        let delta = serde_json::json!({
            "type": "update/order_book",
            "offset": 11,
            "order_book": {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(!handle_value(&delta, &market, &sink, &tap, &mut state));
        assert!(!state.book.initialized);
    }

    #[test]
    fn handle_value_rejects_delta_without_sequence_metadata() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let sink = EventSink::lossless(tx);
        let tap = Tap::none();
        let market = MarketId("BTC".to_string());
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(handle_value(&snapshot, &market, &sink, &tap, &mut state));

        let unsequenced = serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        });
        assert!(!handle_value(&unsequenced, &market, &sink, &tap, &mut state));
    }
}
