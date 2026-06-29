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
use crate::lighter::messages::OrderBookMsg;
use crate::lighter::ws::{subscribe_loop, SubscribeOptions};
use crate::types::MarketId;

const PUBLISH_LEVELS: usize = 20;

#[derive(Default)]
struct StreamState {
    book: LocalBook,
    last_nonce: Option<i64>,
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
                warn!(
                    "Lighter order_book nonce gap for market {}; reconnecting for fresh snapshot",
                    market_id
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
    if state.book.initialized && !msg.is_snapshot() {
        if let (Some(begin_nonce), Some(last_nonce)) =
            (msg.order_book.begin_nonce, state.last_nonce)
        {
            if begin_nonce != last_nonce {
                return false;
            }
        }
    }
    let bids_f = parse_lighter_levels(&msg.order_book.bids);
    let asks_f = parse_lighter_levels(&msg.order_book.asks);
    if msg.is_snapshot() || !state.book.initialized {
        state.book.apply_snapshot(bids_f, asks_f);
    } else {
        state.book.apply_delta(&bids_f, &asks_f);
    }
    state.book.last_offset = msg.effective_offset();
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

        let gap = serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "begin_nonce": 9,
                "nonce": 11,
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        });
        assert!(!handle_value(&gap, &market, &sink, &tap, &mut state));
    }
}
