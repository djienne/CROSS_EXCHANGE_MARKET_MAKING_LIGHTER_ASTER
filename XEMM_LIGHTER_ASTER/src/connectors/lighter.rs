//! Lighter market-data WebSocket connector. Lighter sends an initial snapshot and
//! incremental deltas on `order_book/{market_id}`, so this connector maintains a
//! local book before publishing full book snapshots to the rest of XEMM.

use chrono::Utc;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tracing::warn;

use super::{EventSink, Tap};
use crate::decimal::parse_dec;
use crate::events::{EventKind, PriceLevel};
use crate::lighter::local_book::LocalBook;
use crate::lighter::messages::{BookUpdateContiguity, OrderBookMsg};
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
    let opts = SubscribeOptions::new(
        &format!("lighter-order-book-{label}-{market_id}"),
        vec![channel],
    );
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
    let msg: OrderBookMsg = match serde_json::from_value(data.clone()) {
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
    let bids = side_to_decimals(&state.book.bids, false);
    let asks = side_to_decimals(&state.book.asks, true);
    if bids.is_empty() || asks.is_empty() {
        return true;
    }
    #[cfg(feature = "hotpath")]
    let prebuilt_hot = tap.hot_book_from_raw(
        bids.iter().map(|(p, q)| (p.as_str(), q.as_str())),
        asks.iter().map(|(p, q)| (p.as_str(), q.as_str())),
        exch_ts,
    );
    #[cfg(not(feature = "hotpath"))]
    let prebuilt_hot = None;

    let bid_levels: Vec<PriceLevel> = bids
        .iter()
        .filter_map(|(p, q)| Some((parse_dec(p).ok()?, parse_dec(q).ok()?)))
        .collect();
    let ask_levels: Vec<PriceLevel> = asks
        .iter()
        .filter_map(|(p, q)| Some((parse_dec(p).ok()?, parse_dec(q).ok()?)))
        .collect();
    tap.publish_prebuilt(&bid_levels, &ask_levels, exch_ts, prebuilt_hot);
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

fn parse_lighter_levels(levels: &[crate::lighter::messages::PriceLevel]) -> Vec<(f64, f64)> {
    levels
        .iter()
        .filter_map(|l| {
            let (p, q) = l.parsed();
            (p > 0.0 && q >= 0.0).then_some((p, q))
        })
        .collect()
}

fn side_to_decimals(
    side: &crate::lighter::local_book::BookSide,
    ask: bool,
) -> Vec<(String, String)> {
    let mut rows = side.levels();
    if !ask {
        rows.reverse();
    }
    rows.into_iter()
        .take(PUBLISH_LEVELS)
        .map(|(p, q)| (format_float(p), format_float(q)))
        .collect()
}

fn format_float(v: f64) -> String {
    let s = format!("{v:.12}");
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn formats_float_without_noise() {
        assert_eq!(format_float(100.0), "100");
        assert_eq!(format_float(0.001), "0.001");
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
