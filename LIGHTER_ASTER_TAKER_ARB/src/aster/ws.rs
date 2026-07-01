//! Lightweight Aster futures public depth feed for the arb scanner hot path.
//!
//! The existing scanner path polled `/fapi/v3/depth` every iteration.  That makes the
//! price decision depend on REST latency and rate limits.  This module keeps the
//! Aster 20-level book hot in memory from the futures `@depth20@100ms` stream; the
//! trading loop only clones the latest [`OrderBook`].

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use arc_swap::ArcSwapOption;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::Notify;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::book::OrderBook;
use crate::decimal::parse_dec;

const RECONNECT_BASE: Duration = Duration::from_millis(250);
const RECONNECT_MAX: Duration = Duration::from_secs(10);
const READY_POLL: Duration = Duration::from_millis(20);
/// The @depth20@100ms stream delivers a frame every ~100ms; silence this long means a
/// dead/half-open connection. Without this watchdog a NAT/LB silently dropping the
/// connection blocks `read.next()` forever, the book freezes, and the staleness gate
/// halts trading permanently with no reconnect.
const FRAME_TIMEOUT: Duration = Duration::from_secs(10);
/// Client keepalive ping cadence (keeps NAT/LB state alive between server pings).
const PING_INTERVAL: Duration = Duration::from_secs(30);
/// Bound on sink writes so a wedged socket can never block the session task.
const WRITE_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone)]
pub struct AsterBookFeed {
    symbol: String,
    state: Arc<AsterBookState>,
    reconnect: Arc<Notify>,
}

#[derive(Default)]
struct AsterBookState {
    book: ArcSwapOption<OrderBook>,
}

impl AsterBookFeed {
    pub fn spawn_from_rest_base(rest_base_url: &str, symbol_upper: &str) -> Self {
        let symbol = symbol_upper.to_ascii_uppercase();
        let url = futures_depth_url(rest_base_url, symbol_upper);
        let state = Arc::new(AsterBookState::default());
        let reconnect = Arc::new(Notify::new());
        tokio::spawn(depth_loop(
            url,
            symbol.clone(),
            state.clone(),
            reconnect.clone(),
        ));
        Self {
            symbol,
            state,
            reconnect,
        }
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.order_book().is_ok() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("Aster websocket depth not ready for {}", self.symbol);
            }
            tokio::time::sleep(READY_POLL).await;
        }
    }

    pub fn order_book(&self) -> Result<OrderBook> {
        self.state
            .book
            .load_full()
            .map(|arc| (*arc).clone())
            .ok_or_else(|| anyhow!("Aster websocket depth not ready for {}", self.symbol))
    }

    pub fn request_reconnect(&self) {
        self.state.book.store(None);
        self.reconnect.notify_one();
    }
}

async fn depth_loop(
    url: String,
    symbol: String,
    state: Arc<AsterBookState>,
    reconnect: Arc<Notify>,
) {
    let mut backoff = RECONNECT_BASE;
    loop {
        match depth_session(&url, &symbol, state.clone(), reconnect.clone()).await {
            Ok(()) => {}
            Err(e) => tracing::warn!("Aster depth websocket disconnected: {e:#}"),
        }
        state.book.store(None);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(RECONNECT_MAX);
    }
}

async fn depth_session(
    url: &str,
    symbol: &str,
    state: Arc<AsterBookState>,
    reconnect: Arc<Notify>,
) -> Result<()> {
    let (ws, _) = connect_async(url).await?;
    let (mut write, mut read) = ws.split();
    tracing::info!("Aster depth connected: symbol={} url={}", symbol, url);

    let mut ping_tick = tokio::time::interval(PING_INTERVAL);
    ping_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ping_tick.tick().await; // first tick fires immediately; skip it (just connected)

    loop {
        tokio::select! {
            _ = reconnect.notified() => {
                tracing::warn!("Aster depth reconnect requested: symbol={symbol}");
                return Ok(());
            }
            _ = ping_tick.tick() => {
                match tokio::time::timeout(WRITE_TIMEOUT, write.send(Message::Ping(Vec::new()))).await {
                    Ok(Ok(())) => {}
                    _ => anyhow::bail!("Aster depth keepalive ping failed/wedged: symbol={symbol}"),
                }
            }
            msg = tokio::time::timeout(FRAME_TIMEOUT, read.next()) => {
                let msg = match msg {
                    Ok(Some(msg)) => msg?,
                    Ok(None) => return Ok(()),
                    Err(_) => anyhow::bail!(
                        "Aster depth frame timeout ({}s of silence, half-open connection?): symbol={symbol}",
                        FRAME_TIMEOUT.as_secs()
                    ),
                };
                match msg {
                    Message::Text(text) => {
                        if let Some(book) = parse_depth(&text)? {
                            state.book.store(Some(Arc::new(book)));
                        }
                    }
                    Message::Ping(payload) => {
                        // Do not depend on a future writer to flush tungstenite's auto-pong.
                        match tokio::time::timeout(WRITE_TIMEOUT, write.send(Message::Pong(payload))).await {
                            Ok(Ok(())) => {}
                            _ => anyhow::bail!("Aster depth pong write failed/wedged: symbol={symbol}"),
                        }
                    }
                    Message::Close(_) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct DepthMsg {
    #[serde(rename = "E", default)]
    event_time_ms: i64,
    #[serde(rename = "T", default)]
    transaction_time_ms: i64,
    #[serde(default, rename = "bids", alias = "b")]
    bids: Vec<[String; 2]>,
    #[serde(default, rename = "asks", alias = "a")]
    asks: Vec<[String; 2]>,
}

fn parse_depth(text: &str) -> Result<Option<OrderBook>> {
    let value: serde_json::Value = serde_json::from_str(text)?;
    if value.get("result").is_some() || value.get("id").is_some() && value.get("data").is_none() {
        return Ok(None);
    }
    let payload = value.get("data").unwrap_or(&value).clone();
    let msg: DepthMsg = serde_json::from_value(payload)?;
    let bids = parse_levels(msg.bids)?;
    let asks = parse_levels(msg.asks)?;
    if bids.is_empty() || asks.is_empty() {
        return Ok(None);
    }
    let exch_ts = ms_to_dt(msg.transaction_time_ms.max(msg.event_time_ms));
    Ok(Some(OrderBook::from_levels(
        bids,
        asks,
        exch_ts,
        Utc::now(),
    )))
}

fn parse_levels(raw: Vec<[String; 2]>) -> Result<Vec<(Decimal, Decimal)>> {
    let mut out = Vec::with_capacity(raw.len());
    for [px, qty] in raw {
        let px = parse_dec(&px)?;
        let qty = parse_dec(&qty)?;
        if px > Decimal::ZERO && qty > Decimal::ZERO {
            out.push((px, qty));
        }
    }
    Ok(out)
}

fn futures_depth_url(rest_base_url: &str, symbol_upper: &str) -> String {
    let symbol = symbol_upper.to_ascii_lowercase();
    let trimmed = rest_base_url.trim_end_matches('/');
    if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        if trimmed.contains("/ws/") || trimmed.contains("/stream") {
            return trimmed.to_string();
        }
        return format!("{trimmed}/ws/{symbol}@depth20@100ms");
    }
    if trimmed.contains("testnet") {
        format!("wss://fstream5.asterdex-testnet.com/ws/{symbol}@depth20@100ms")
    } else {
        format!("wss://fstream.asterdex.com/ws/{symbol}@depth20@100ms")
    }
}

fn ms_to_dt(ms: i64) -> chrono::DateTime<chrono::Utc> {
    if ms > 0 {
        chrono::DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
    } else {
        Utc::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn parses_raw_depth() {
        let raw = r#"{"e":"depthUpdate","E":1568014460893,"T":1568014460891,"s":"HYPEUSDT","bids":[["25.35","31.21"],["25.34","12.00"]],"asks":[["25.36","40.66"],["25.37","9.00"]]}"#;
        let book = parse_depth(raw).unwrap().unwrap();
        assert_eq!(book.best_bid().unwrap().px, dec!(25.35));
        assert_eq!(book.best_bid().unwrap().qty, dec!(31.21));
        assert_eq!(book.best_ask().unwrap().px, dec!(25.36));
        assert_eq!(book.best_ask().unwrap().qty, dec!(40.66));
        assert_eq!(book.bids[1].px, dec!(25.34));
        assert_eq!(book.asks[1].px, dec!(25.37));
    }

    #[test]
    fn parses_combined_depth() {
        let raw = r#"{"stream":"hypeusdt@depth20@100ms","data":{"e":"depthUpdate","E":1,"T":2,"b":[["10","1"]],"a":[["11","2"]]}}"#;
        let book = parse_depth(raw).unwrap().unwrap();
        assert_eq!(book.best_bid().unwrap().px, dec!(10));
        assert_eq!(book.best_ask().unwrap().px, dec!(11));
    }

    #[test]
    fn derives_mainnet_futures_url_from_rest_base() {
        assert_eq!(
            futures_depth_url("https://fapi.asterdex.com", "HYPEUSDT"),
            "wss://fstream.asterdex.com/ws/hypeusdt@depth20@100ms"
        );
    }
}
