//! Aster (asterdex) futures market-data WebSocket connector: subscribes to the
//! `<sym>@depth20@100ms` partial-depth snapshot, `<sym>@bookTicker` top-of-book
//! assist, and `<sym>@aggTrade` streams via a combined-stream connection, and
//! emits venue-agnostic `EventKind`s. Using partial-depth snapshots avoids
//! diff/sequence (U/u/pu) maintenance entirely.
//! Responds to server pings; reconnects with capped backoff (24h server cap).

use anyhow::{Context, Result};
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tracing::{debug, info, warn};

use super::{EventSink, Tap};
use crate::decimal::parse_dec;
use crate::events::{EventKind, PriceLevel};
use crate::types::MarketId;

const WS_BASE: &str = "wss://fstream.asterdex.com";

#[derive(Deserialize)]
struct Combined<'a> {
    stream: &'a str,
    #[serde(borrow)]
    data: &'a serde_json::value::RawValue,
}

#[derive(Deserialize)]
struct DepthMsg<'a> {
    #[serde(rename = "E", default)]
    event_time: i64,
    #[serde(rename = "b", alias = "bids", borrow, default)]
    bids: Vec<[&'a str; 2]>,
    #[serde(rename = "a", alias = "asks", borrow, default)]
    asks: Vec<[&'a str; 2]>,
}

#[derive(Deserialize)]
struct AggTradeMsg<'a> {
    #[serde(rename = "T")]
    trade_time: i64,
    #[serde(rename = "p")]
    price: &'a str,
    #[serde(rename = "q")]
    qty: &'a str,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

#[derive(Deserialize)]
struct BookTickerMsg<'a> {
    #[serde(rename = "E", default)]
    event_time: i64,
    #[serde(rename = "T", default)]
    trade_time: i64,
    #[serde(rename = "b")]
    bid_px: &'a str,
    #[serde(rename = "B")]
    bid_qty: &'a str,
    #[serde(rename = "a")]
    ask_px: &'a str,
    #[serde(rename = "A")]
    ask_qty: &'a str,
}

impl<'a> BookTickerMsg<'a> {
    fn ts_ms(&self) -> i64 {
        if self.event_time != 0 { self.event_time } else { self.trade_time }
    }
}

/// Idle-reconnect threshold. `depth20@100ms` is *event-driven* — it only pushes when
/// the top-20 levels change, so a quiet/thin book can be legitimately silent for many
/// seconds while the socket is perfectly healthy (Aster keeps it alive with a server
/// ping every ~3–5 min). So we size this well above that ping cadence: long enough not
/// to tear down a healthy quiet feed, short enough to still catch a genuinely half-open
/// socket (no Close frame, no error) inside Aster's ~10–15 min pong-death window. Fast
/// liveness/freshness detection is owned elsewhere — the 60s stream watchdog and the
/// 30s REST book-check.
const IDLE_TIMEOUT: Duration = Duration::from_secs(360);
/// A connection that stayed up at least this long is "healthy"; reset the backoff
/// after it so a single long session isn't punished by a grown backoff.
const HEALTHY_AFTER: Duration = Duration::from_secs(60);

/// Run forever (until aborted), reconnecting on error. The `record` path (no hot
/// tap); delegates to [`run_with_tap`].
pub async fn run(symbol_lower: String, market: MarketId, tx: UnboundedSender<(MarketId, EventKind)>) {
    run_with_tap(symbol_lower, market, EventSink::lossless(tx), Tap::none()).await
}

/// Like [`run`] but also fans each book out to a lock-free [`Tap`] and honors the
/// watchdog's reconnect signal — the `live`/strategy hot path. With `Tap::none()`
/// it is behaviorally identical to `run`.
pub async fn run_with_tap(
    symbol_lower: String,
    market: MarketId,
    tx: EventSink,
    tap: Tap,
) {
    let mut backoff = 1u64;
    loop {
        let started = std::time::Instant::now();
        match stream_once(&symbol_lower, &market, &tx, &tap).await {
            Ok(()) => info!("[ASTER {}] stream closed", symbol_lower),
            Err(e) => warn!("[ASTER {}] error: {e:#}", symbol_lower),
        }
        // Whether closed or errored, the stream is KNOWN down until the next connect's
        // full snapshot: flag the cell so the maker gate closes now, not at age expiry.
        tap.mark_stream_down();
        if started.elapsed() >= HEALTHY_AFTER {
            backoff = 1; // the connection was healthy; start the next retry fast
        }
        sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(30);
    }
}

async fn stream_once(
    symbol: &str,
    market: &MarketId,
    tx: &EventSink,
    tap: &Tap,
) -> Result<()> {
    let url = format!("{WS_BASE}/stream?streams={symbol}@depth20@100ms/{symbol}@bookTicker/{symbol}@aggTrade");
    let (ws, _) = connect_async(&url).await.context("connect Aster ws")?;
    let (mut write, mut read) = ws.split();
    info!("[ASTER {}] subscribed depth20@100ms + bookTicker + aggTrade", symbol);

    // Race each read against (a) an idle deadline so a silent socket forces a
    // reconnect instead of hanging here forever, and (b) the watchdog's reconnect
    // signal. The idle deadline resets on every frame.
    loop {
        tokio::select! {
            msg = read.next() => {
                let Some(msg) = msg else { break };
                match msg? {
                    Message::Text(text) => handle(&text, market, tx, tap).await,
                    Message::Ping(p) => super::send_guarded(&mut write, Message::Pong(p)).await?,
                    Message::Close(_) => break,
                    _ => {}
                }
                tap.touch(); // liveness on every inbound frame
            }
            _ = sleep(IDLE_TIMEOUT) => {
                anyhow::bail!("idle >{}s, forcing reconnect", IDLE_TIMEOUT.as_secs());
            }
            _ = tap.wait_reconnect() => {
                anyhow::bail!("watchdog forced reconnect");
            }
        }
    }
    Ok(())
}

async fn handle(text: &str, market: &MarketId, tx: &EventSink, tap: &Tap) {
    let Combined { stream, data } = match serde_json::from_str::<Combined<'_>>(text) {
        Ok(c) => c,
        Err(_) => return,
    };
    if stream.contains("@aggTrade") {
        if let Ok(t) = serde_json::from_str::<AggTradeMsg<'_>>(data.get()) {
            let (price, qty) = match (parse_dec(t.price), parse_dec(t.qty)) {
                (Ok(p), Ok(q)) if p > Decimal::ZERO && q > Decimal::ZERO => (p, q),
                _ => return,
            };
            tx.send(
                market.clone(),
                EventKind::AsterAggTrade {
                    price,
                    qty,
                    buyer_is_maker: t.buyer_is_maker,
                    exch_ts: ms_to_dt(t.trade_time),
                },
            );
        }
    } else if stream.contains("@bookTicker") {
        if let Ok(t) = serde_json::from_str::<BookTickerMsg<'_>>(data.get()) {
            // Validate BEFORE any publish. The hot-only publish below sets the
            // bbo_hot_only_pending guard that only the raw publish clears; a
            // crossed/zero-qty frame that bailed between the two used to leave the
            // guard latched — reprice_market then early-returns, freezing placements
            // AND slow-path cancels until the next VALID bookTicker arrives.
            let (Ok(bp), Ok(bq), Ok(ap), Ok(aq)) = (
                parse_dec(t.bid_px),
                parse_dec(t.bid_qty),
                parse_dec(t.ask_px),
                parse_dec(t.ask_qty),
            ) else {
                return;
            };
            if bp <= Decimal::ZERO || bq <= Decimal::ZERO || ap <= Decimal::ZERO || aq <= Decimal::ZERO || bp >= ap {
                return;
            }
            let exch_ts = ms_to_dt(t.ts_ms());
            #[cfg(feature = "hotpath")]
            let prebuilt_hot = tap.hot_book_from_raw(
                std::iter::once((t.bid_px, t.bid_qty)),
                std::iter::once((t.ask_px, t.ask_qty)),
                exch_ts,
            );
            #[cfg(feature = "hotpath")]
            if let Some((hot, _)) = prebuilt_hot.as_ref() {
                tap.publish_bbo_hot_only(*hot, exch_ts);
            }
            #[cfg(not(feature = "hotpath"))]
            let prebuilt_hot = None;
            // Aster BBO *size* influences quote safety: the quote engine only trusts a
            // bookTicker touch as the effective touch when its visible quantity covers
            // the candidate order. Therefore size-only updates must wake the strategy
            // too; using the price-only coalescer here can leave a quote resting up to
            // the next cold tick after top size vanishes.
            tap.publish_bbo_prebuilt((bp, bq), (ap, aq), exch_ts, prebuilt_hot);
        }
    } else if stream.contains("@depth") {
        if let Ok(d) = serde_json::from_str::<DepthMsg<'_>>(data.get()) {
            let exch_ts = ms_to_dt(d.event_time);
            #[cfg(feature = "hotpath")]
            let prebuilt_hot = tap.hot_book_from_raw(
                d.bids.iter().map(|r| (r[0], r[1])),
                d.asks.iter().map(|r| (r[0], r[1])),
                exch_ts,
            );
            #[cfg(feature = "hotpath")]
            if let Some((hot, _)) = prebuilt_hot.as_ref() {
                tap.publish_hot_only(*hot, exch_ts);
            }
            #[cfg(not(feature = "hotpath"))]
            let prebuilt_hot = None;
            let bids = to_levels(&d.bids);
            let asks = to_levels(&d.asks);
            // Hot tap FIRST (strategy latency), then canonical consumer (recording).
            tap.publish_prebuilt(&bids, &asks, exch_ts, prebuilt_hot);
            tx.send(market.clone(), EventKind::AsterDepth { bids, asks, exch_ts });
        }
    } else {
        debug!("[ASTER] unhandled stream {stream}");
    }
}

fn to_levels(rows: &[[&str; 2]]) -> Vec<PriceLevel> {
    rows.iter()
        .filter_map(|r| match (parse_dec(r[0]), parse_dec(r[1])) {
            (Ok(p), Ok(q)) if p > Decimal::ZERO && q > Decimal::ZERO => Some((p, q)),
            _ => None,
        })
        .collect()
}

fn ms_to_dt(ms: i64) -> chrono::DateTime<chrono::Utc> {
    if ms > 0 { chrono::DateTime::from_timestamp_millis(ms).unwrap_or_else(chrono::Utc::now) } else { chrono::Utc::now() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalid_book_ticker_never_latches_the_hot_only_guard() {
        // A crossed/zero-qty bookTicker frame must be a complete no-op. Before the
        // validate-first reorder, the hot-only publish fired before validation and the
        // bail skipped the raw publish that clears bbo_hot_only_pending — leaving the
        // guard latched so reprice_market froze placements AND slow-path cancels until
        // the next valid frame.
        use crate::connectors::BookTap;
        use crate::hotpath::book_cell::VenueBook;
        use std::sync::Arc;
        use tokio::sync::mpsc;

        let (tx, _rx) = mpsc::unbounded_channel();
        let sink = EventSink::lossless(tx);
        let cell = Arc::new(VenueBook::new());
        let tap = Tap { book: Some(cell.clone() as Arc<dyn BookTap>), ..Tap::none() };
        let market = MarketId("HYPE".to_string());
        let frame = |b: &str, bq: &str, a: &str, aq: &str| {
            format!(
                r#"{{"stream":"hypeusdt@bookTicker","data":{{"e":"bookTicker","u":1,"s":"HYPEUSDT","b":"{b}","B":"{bq}","a":"{a}","A":"{aq}","T":1,"E":2}}}}"#
            )
        };

        // Valid frame: BBO populated and the pending guard is cleared by the raw publish.
        handle(&frame("70.5", "10", "70.6", "12"), &market, &sink, &tap).await;
        let before = cell.load_bbo().expect("valid frame populates the BBO slot");
        assert!(!cell.has_hot_only_update());

        // Crossed frame (bid >= ask): no publish at all, guard must stay clear.
        handle(&frame("70.7", "10", "70.6", "12"), &market, &sink, &tap).await;
        assert!(
            !cell.has_hot_only_update(),
            "crossed bookTicker latched the hot-only guard"
        );
        // Zero-qty frame likewise.
        handle(&frame("70.5", "0", "70.6", "12"), &market, &sink, &tap).await;
        assert!(
            !cell.has_hot_only_update(),
            "zero-qty bookTicker latched the hot-only guard"
        );
        // And the previously published BBO is untouched.
        let after = cell.load_bbo().expect("BBO still present");
        assert_eq!(
            before.best_bid().unwrap().px,
            after.best_bid().unwrap().px
        );
        assert_eq!(
            before.best_ask().unwrap().px,
            after.best_ask().unwrap().px
        );
    }

    #[test]
    fn parses_aster_book_ticker_wire_shape() {
        let raw = r#"{
            "stream":"hypeusdt@bookTicker",
            "data":{
                "e":"bookTicker",
                "u":481270967632,
                "s":"HYPEUSDT",
                "b":"70.61400",
                "B":"365.37",
                "a":"70.62800",
                "A":"220.16",
                "T":1781952197150,
                "E":1781952197185
            }
        }"#;
        let wrap: Combined<'_> = serde_json::from_str(raw).unwrap();
        assert_eq!(wrap.stream, "hypeusdt@bookTicker");
        let ticker: BookTickerMsg<'_> = serde_json::from_str(wrap.data.get()).unwrap();
        assert_eq!(ticker.event_time, 1781952197185);
        assert_eq!(ticker.trade_time, 1781952197150);
        assert_eq!(ticker.ts_ms(), 1781952197185);
        assert_eq!(ticker.bid_px, "70.61400");
        assert_eq!(ticker.bid_qty, "365.37");
        assert_eq!(ticker.ask_px, "70.62800");
        assert_eq!(ticker.ask_qty, "220.16");
    }
}
