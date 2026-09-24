//! Lighter market-data WebSocket connector. Lighter sends an initial snapshot and
//! incremental deltas on `order_book/{market_id}`, so this connector maintains a
//! local book before publishing full book snapshots to the rest of XEMM.

use chrono::Utc;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tracing::warn;

use super::Tap;
use crate::decimal::parse_dec;
use rust_decimal::Decimal;
use crate::book::PriceLevel;
use crate::lighter::local_book::LocalBook;
use crate::lighter::messages::{BookUpdateContiguity, OrderBookMsgRef, PriceLevelRef};
use crate::lighter::ws::{subscribe_loop, SubscribeOptions};

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

pub async fn run_with_tap(
    ws_url: String,
    market_id: u32,
    label: String,
    tap: Tap,
) {
    let channel = format!("order_book/{market_id}");
    let mut opts = SubscribeOptions::new(
        &ws_url,
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
    let tap_for_disconnect = tap.clone();
    subscribe_loop(
        opts,
        Some(reconnect),
        move |frame| {
            let mut state = state.lock().expect("Lighter stream book state poisoned");
            if !handle_raw(frame.raw, &tap, &mut state) {
                state.gap_resyncs += 1;
                warn!(
                    "Lighter order_book gap or unusable frame for market {} (resync #{}); reconnecting for fresh snapshot",
                    market_id, state.gap_resyncs
                );
                state.reset();
                // The book missed updates: untrustworthy until the post-resync snapshot.
                tap.mark_stream_down();
                reconnect_on_gap.notify_one();
            }
        },
        move || {
            state_for_disconnect
                .lock()
                .expect("Lighter stream book state poisoned")
                .reset();
            // KNOWN disconnect: close the maker gate until a fresh snapshot lands.
            tap_for_disconnect.mark_stream_down();
        },
    )
    .await;
}

/// Test shim: the suite drives frames as `serde_json::json!` values; production ingest
/// goes through [`handle_raw`] on the raw WS text.
#[cfg(test)]
fn handle_value(
    data: &serde_json::Value,
    tap: &Tap,
    state: &mut StreamState,
) -> bool {
    // Sequence tests supply a valid emission timestamp; missing-source behavior is
    // exercised separately through handle_raw without this fixture convenience.
    let mut data = data.clone();
    if data.get("timestamp").is_none() {
        data["timestamp"] = serde_json::json!(Utc::now().timestamp_millis());
    }
    handle_raw(&data.to_string(), tap, state)
}

fn handle_raw(
    raw: &str,
    tap: &Tap,
    state: &mut StreamState,
) -> bool {
    // Borrowed deserialize straight from the raw frame text: no `Value` tree, no deep
    // clone, no per-level String allocations on the hedge-source ingest thread.
    let msg = match serde_json::from_str::<OrderBookMsgRef<'_>>(raw) {
        Ok(m) => m,
        Err(_) => return false,
    };
    let source_ms = msg.source_time_ms().unwrap_or(0);
    let wall = Utc::now();
    if crate::hot_types::source_age_at_receive_ms(source_ms, wall.timestamp_millis()) == i64::MAX {
        return false;
    }
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
    // Unparseable level → false → the caller's gap path (reset + reconnect for a
    // fresh snapshot). Applying a coerced level would silently desync the book.
    let Some(bids_f) = parse_lighter_levels(&msg.order_book.bids) else {
        return false;
    };
    let Some(asks_f) = parse_lighter_levels(&msg.order_book.asks) else {
        return false;
    };
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
    let Some(exch_ts) = chrono::DateTime::<Utc>::from_timestamp_millis(source_ms) else {
        return false;
    };
    // Same publishability gate as the old string path: both sides non-empty.
    if state.book.bids.is_empty() || state.book.asks.is_empty() {
        return true;
    }
    // Publish the exact decimal wire values; do not reconstruct them from f64.
    let bid_levels: Vec<PriceLevel> = state
        .book
        .bids
        .top_descending(PUBLISH_LEVELS)
        .collect();
    let ask_levels: Vec<PriceLevel> = state
        .book
        .asks
        .top_ascending(PUBLISH_LEVELS)
        .collect();
    let prebuilt_hot = tap.hot_book_from_levels(&bid_levels, &ask_levels, exch_ts);
    if let Some((hot, _)) = prebuilt_hot.as_ref() {
        // Integer projection first (mirrors the Aster connector): fast-cancel
        // prechecks see the move before the raw Decimal book is installed.
        tap.publish_hot_only(*hot, exch_ts);
    }
    tap.publish_prebuilt(&bid_levels, &ask_levels, exch_ts, prebuilt_hot);
    // Lighter has no separate bookTicker stream (Aster does), so mirror the L2
    // top-of-book into the optional BBO fast-path slot. Without this the slot is
    // never populated: the hedge always takes the slower L2 walk and qdiag shows a
    // misleading hl_bbo=none / age=i64::MAX. Same data and freshness as this L2
    // frame, so hedge pricing is unchanged — it only lets the BBO fast path engage.
    // Coalesced-wake publish: the L2 publish above already woke the strategy for this
    // frame, so the mirror only stamps data + freshness (no redundant generation bump).
    // No hot-only pre-publish either — that half of the pair exists for Aster's
    // independent bookTicker stream, not for a mirror of the frame just published.
    if let (Some(&bid_top), Some(&ask_top)) = (bid_levels.first(), ask_levels.first()) {
        let bbo_hot = tap.hot_book_from_levels(
            std::slice::from_ref(&bid_top),
            std::slice::from_ref(&ask_top),
            exch_ts,
        );
        tap.publish_bbo_price_wake_prebuilt(bid_top, ask_top, exch_ts, bbo_hot);
    }
    tap.touch();
    true
}

/// `None` when any level is unparseable — the caller must resync rather than apply
/// (a size coerced to 0.0 would DELETE the level; a dropped price desyncs the book).
/// Explicit zero sizes are deletions; impossible numeric values invalidate the delta.
fn parse_lighter_levels(levels: &[PriceLevelRef<'_>]) -> Option<Vec<(Decimal, Decimal)>> {
    let mut out = Vec::with_capacity(levels.len());
    for l in levels {
        let p = parse_dec(&l.price).ok()?;
        let q = parse_dec(&l.size).ok()?;
        if p <= Decimal::ZERO || q < Decimal::ZERO {
            return None;
        }
        out.push((p, q));
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::OrderBook;
    use crate::connectors::BookTap;

    /// Records each full-book publish, standing in for the hot-path cell.
    #[derive(Default)]
    struct Published(Mutex<Vec<OrderBook>>);

    impl BookTap for Published {
        fn publish(&self, book: OrderBook) {
            self.0.lock().unwrap().push(book);
        }
        fn touch(&self) {}
    }

    impl Published {
        /// The books published since the last call.
        fn take(&self) -> Vec<OrderBook> {
            std::mem::take(&mut *self.0.lock().unwrap())
        }
    }

    fn recording_tap() -> (Arc<Published>, Tap) {
        let published = Arc::new(Published::default());
        let tap = Tap { book: Some(published.clone() as Arc<dyn BookTap>), ..Tap::none() };
        (published, tap)
    }

    #[test]
    fn published_levels_preserve_exchange_tick_values() {
        let (published, tap) = recording_tap();
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
        assert!(handle_value(&snapshot, &tap, &mut state));
        let book = published.take().pop().expect("book published");
        // Bids best-first (highest price), asks best-first (lowest price).
        assert_eq!(book.bids.len(), 2);
        assert_eq!(book.bids[0].px.to_string(), "64820.2");
        assert_eq!(book.bids[0].qty.to_string(), "0.00051");
        assert_eq!(book.bids[1].px.to_string(), "0.30000000000000004");
        assert_eq!(book.bids[1].qty.to_string(), "1");
        assert_eq!(book.asks[0].px.to_string(), "64820.3");
        assert_eq!(book.asks[0].qty.to_string(), "0.19283");
    }

    #[test]
    fn handle_value_mirrors_l2_top_into_bbo_slot() {
        // Lighter has no bookTicker stream; the connector mirrors the L2 top-of-book
        // into the BBO slot so the hedge fast path can engage and qdiag stops showing
        // hl_bbo=none. Regression guard: the slot must be populated with exactly the
        // top level of each side.
        use crate::hotpath::book_cell::VenueBook;

        let cell = Arc::new(VenueBook::new());
        let tap = Tap { book: Some(cell.clone() as Arc<dyn BookTap>), ..Tap::none() };
        let mut state = StreamState::default();
        // Decimal prices and quantities retain the exact wire values.
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
        assert!(handle_value(&snapshot, &tap, &mut state));

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
        let (published, tap) = recording_tap();
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(handle_value(&snapshot, &tap, &mut state));
        assert_eq!(state.last_nonce, Some(10));
        assert_eq!(published.take().len(), 1);

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
        assert!(!handle_value(&gap, &tap, &mut state));
    }

    #[test]
    fn handle_value_applies_forward_extending_nonce_overlap() {
        let (published, tap) = recording_tap();
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(handle_value(&snapshot, &tap, &mut state));
        published.take();

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
        assert!(handle_value(&overlap, &tap, &mut state));
        assert_eq!(state.last_nonce, Some(11));
        assert_eq!(published.take().len(), 1, "applied overlap must publish");
    }

    #[test]
    fn handle_value_skips_stale_nonce_replay_without_resync() {
        let (published, tap) = recording_tap();
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(handle_value(&snapshot, &tap, &mut state));
        published.take();

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
        assert!(handle_value(&stale, &tap, &mut state));
        assert_eq!(state.last_nonce, Some(10), "stale replay must not move the position");
        assert!(published.take().is_empty(), "stale replay must not publish");
    }

    #[test]
    fn handle_value_detects_orderbook_offset_gap_without_nonce() {
        let tap = Tap::none();
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "offset": 10,
            "order_book": {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(handle_value(&snapshot, &tap, &mut state));
        assert_eq!(state.book.last_offset, Some(10));

        let gap = serde_json::json!({
            "type": "update/order_book",
            "offset": 12,
            "order_book": {
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        });
        assert!(!handle_value(&gap, &tap, &mut state));
    }

    #[test]
    fn handle_value_skips_duplicate_offset_without_resync() {
        let (published, tap) = recording_tap();
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "offset": 10,
            "order_book": {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(handle_value(&snapshot, &tap, &mut state));
        published.take();

        // Same offset re-delivered: a duplicate, not a gap — no reconnect churn.
        let dup = serde_json::json!({
            "type": "update/order_book",
            "offset": 10,
            "order_book": {
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        });
        assert!(handle_value(&dup, &tap, &mut state));
        assert_eq!(state.book.last_offset, Some(10));
        assert!(published.take().is_empty(), "duplicate must not publish");

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
        assert!(handle_value(&next, &tap, &mut state));
        assert_eq!(state.book.last_offset, Some(11));
    }

    #[test]
    fn handle_value_resyncs_on_delta_before_snapshot() {
        let tap = Tap::none();
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
        assert!(!handle_value(&delta, &tap, &mut state));
        assert!(!state.book.initialized);
    }

    #[test]
    fn handle_value_rejects_delta_without_sequence_metadata() {
        let tap = Tap::none();
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "101", "size": "2"}]
            }
        });
        assert!(handle_value(&snapshot, &tap, &mut state));

        let unsequenced = serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        });
        assert!(!handle_value(&unsequenced, &tap, &mut state));
    }

    #[test]
    fn handle_value_resyncs_on_malformed_level_and_keeps_zero_size_deletes() {
        let (published, tap) = recording_tap();
        let mut state = StreamState::default();

        let snapshot = serde_json::json!({
            "type": "subscribed/order_book",
            "offset": 10,
            "order_book": {
                "bids": [{"price": "100", "size": "1"}, {"price": "99", "size": "2"}],
                "asks": [{"price": "101", "size": "2"}],
                "offset": 10
            }
        });
        assert!(handle_value(&snapshot, &tap, &mut state));
        published.take();

        // Regression pin: an explicit "0" size is a deletion, not a resync.
        let delete = serde_json::json!({
            "type": "update/order_book",
            "offset": 11,
            "order_book": {
                "bids": [{"price": "99", "size": "0"}],
                "asks": []
            }
        });
        assert!(handle_value(&delete, &tap, &mut state));
        let book = published.take().pop().expect("delete delta published");
        assert_eq!(book.bids.len(), 1, "size=0 must delete the 99 level");

        // A malformed size must resync (return false), never coerce to 0.0 — that
        // would silently DELETE the level. And nothing may be published.
        let malformed = serde_json::json!({
            "type": "update/order_book",
            "offset": 12,
            "order_book": {
                "bids": [{"price": "100", "size": "not-a-number"}],
                "asks": []
            }
        });
        assert!(!handle_value(&malformed, &tap, &mut state));
        assert!(published.take().is_empty(), "malformed delta must not publish");
    }

    #[test]
    fn preserves_source_time_and_rejects_missing_or_future_source() {
        let (published, tap) = recording_tap();
        let source = Utc::now().timestamp_millis() - 10_000;
        let mut frame = serde_json::json!({
            "type":"subscribed/order_book","timestamp":source,"order_book":{
                "nonce":1,"bids":[{"price":"100","size":"1"}],"asks":[{"price":"101","size":"1"}]
            }
        });
        let mut state = StreamState::default();
        assert!(handle_raw(&frame.to_string(), &tap, &mut state));
        let book = published.take().pop().expect("book expected");
        assert_eq!(book.exch_ts.timestamp_millis(), source);
        frame.as_object_mut().unwrap().remove("timestamp");
        assert!(!handle_raw(&frame.to_string(), &tap, &mut state));
        frame["timestamp"] = serde_json::json!(Utc::now().timestamp_millis() + 2_000);
        assert!(!handle_raw(&frame.to_string(), &tap, &mut state));
    }
}
