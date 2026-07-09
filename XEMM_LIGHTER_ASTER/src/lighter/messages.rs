//! Serde models for Lighter REST responses and WebSocket payloads.
//!
//! Field names verified against the live API (`/api/v1/orderBooks`) and live WS captures.
//! Prices/sizes arrive as STRINGS — parse with `fast-float`. Account-channel models are
//! modeled from the Python handlers and refined against live (authenticated) captures.

use serde::Deserialize;
use std::collections::HashMap;

#[inline]
pub fn parse_f64(s: &str) -> f64 {
    fast_float::parse(s).unwrap_or(0.0)
}

/// Like [`parse_f64`] but surfaces malformed input instead of coercing it to `0.0`.
/// Book ingest must use this: a malformed SIZE coerced to zero DELETES the level from
/// the local book, and a malformed price silently drops it — both desync the book.
#[inline]
pub fn parse_f64_opt(s: &str) -> Option<f64> {
    fast_float::parse(s).ok()
}

// ----------------------------- REST -----------------------------

#[derive(Debug, Deserialize)]
pub struct OrderBooksResponse {
    #[serde(default)]
    pub order_books: Vec<OrderBookDetail>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct OrderBookDetail {
    pub symbol: String,
    pub market_id: u32,
    #[serde(default)]
    pub min_base_amount: String,
    #[serde(default)]
    pub min_quote_amount: String,
    #[serde(default)]
    pub supported_size_decimals: u32,
    #[serde(default)]
    pub supported_price_decimals: u32,
    #[serde(default)]
    pub maker_fee: String,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct NextNonceResponse {
    pub nonce: i64,
}

/// Response shape shared by REST sendTx[Batch] and the tx WebSocket.
#[derive(Debug, Deserialize, Default)]
pub struct TxResponse {
    #[serde(default)]
    pub code: i64,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub volume_quota_remaining: Option<i64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct AccountActiveOrdersResponse {
    #[serde(default)]
    pub orders: Vec<RemoteOrder>,
}

/// A live order as reported by the exchange (REST or account_orders WS).
#[derive(Debug, Deserialize, Clone, Default)]
pub struct RemoteOrder {
    #[serde(default)]
    pub client_order_index: Option<i64>,
    #[serde(default)]
    pub order_index: Option<i64>,
    #[serde(default)]
    pub is_ask: Option<bool>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub remaining_base_amount: Option<String>,
    #[serde(default)]
    pub filled_base_amount: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
}

impl RemoteOrder {
    /// Live = order still resting. `accountActiveOrders` rows may omit `status` entirely
    /// (every row there is by definition active) — treat a missing status as LIVE so the
    /// reconcile poller never mass-clears tracked slots (codex review). Only an explicit
    /// terminal status (filled/cancelled/expired) marks an order dead.
    pub fn is_live(&self) -> bool {
        match self.status.as_deref() {
            None => true,
            Some(s) => matches!(s, "open" | "partial_filled" | "pending" | "in-progress"),
        }
    }
}

// ----------------------------- WebSocket -----------------------------

/// A single order book level (strings on the wire).
#[derive(Debug, Deserialize, Clone)]
pub struct PriceLevel {
    pub price: String,
    pub size: String,
}

impl PriceLevel {
    #[inline]
    pub fn parsed(&self) -> (f64, f64) {
        (parse_f64(&self.price), parse_f64(&self.size))
    }
}

#[derive(Debug, Deserialize)]
pub struct OrderBookPayload {
    #[serde(default)]
    pub bids: Vec<PriceLevel>,
    #[serde(default)]
    pub asks: Vec<PriceLevel>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub nonce: Option<i64>,
    #[serde(default)]
    pub begin_nonce: Option<i64>,
}

/// `order_book/{m}` envelope. `type` is `subscribed/order_book` (snapshot) or
/// `update/order_book` (delta).
#[derive(Debug, Deserialize)]
pub struct OrderBookMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub offset: Option<u64>,
    pub order_book: OrderBookPayload,
}

/// Verdict for folding a non-snapshot `order_book` update into a locally-maintained book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookUpdateContiguity {
    /// Continues the local book: chained nonce, next offset, or a forward-extending overlap.
    Apply,
    /// Fully stale/duplicate delta — drop it and keep the book. NOT a reason to resync.
    SkipStale,
    /// Updates were missed (or the delta carries no usable sequence metadata), so the local
    /// book can no longer be trusted — resync via a fresh snapshot.
    Gap,
}

/// Classify a delta against the last applied sequence position. Single-sourced for the
/// owned [`OrderBookMsg`] and borrowed [`OrderBookMsgRef`] views — this is fail-closed
/// sequencing logic and must never fork.
///
/// Nonce chain: `begin_nonce` is where the delta starts, `end_nonce` where it ends.
/// Levels carry ABSOLUTE sizes, so an overlapping delta that extends forward
/// (`begin < last < end`) re-states known levels idempotently and is safe to apply;
/// only a delta that ends at-or-before our position is a stale replay.
pub fn classify_book_update(
    begin_nonce: Option<i64>,
    end_nonce: Option<i64>,
    effective_offset: Option<u64>,
    last_nonce: Option<i64>,
    last_offset: Option<u64>,
) -> BookUpdateContiguity {
    use BookUpdateContiguity::*;
    if let (Some(begin), Some(last)) = (begin_nonce, last_nonce) {
        return if begin > last {
            Gap
        } else if begin == last || end_nonce.is_some_and(|end| end > last) {
            Apply
        } else {
            SkipStale
        };
    }
    if let (Some(offset), Some(last)) = (effective_offset, last_offset) {
        return if offset == last.saturating_add(1) {
            Apply
        } else if offset <= last {
            SkipStale
        } else {
            Gap
        };
    }
    Gap
}

impl OrderBookMsg {
    pub fn is_snapshot(&self) -> bool {
        self.msg_type.contains("subscribed")
    }
    /// Prefer envelope offset, fall back to payload offset.
    pub fn effective_offset(&self) -> Option<u64> {
        self.offset.or(self.order_book.offset)
    }

    /// See [`classify_book_update`].
    pub fn contiguity(
        &self,
        last_nonce: Option<i64>,
        last_offset: Option<u64>,
    ) -> BookUpdateContiguity {
        classify_book_update(
            self.order_book.begin_nonce,
            self.order_book.nonce,
            self.effective_offset(),
            last_nonce,
            last_offset,
        )
    }
}

/// Borrowed view of a book level — zero-copy when deserializing from raw WS text or an
/// already-parsed `serde_json::Value`. `Cow` (not `&str`): borrowing from RAW TEXT fails
/// on JSON-escaped strings, and while numeric strings can't legally need escaping, `Cow`
/// makes that failure mode an allocation instead of a dropped frame.
#[derive(Debug, Deserialize)]
pub struct PriceLevelRef<'a> {
    #[serde(borrow)]
    pub price: std::borrow::Cow<'a, str>,
    #[serde(borrow)]
    pub size: std::borrow::Cow<'a, str>,
}

impl PriceLevelRef<'_> {
    #[inline]
    pub fn parsed(&self) -> (f64, f64) {
        (parse_f64(&self.price), parse_f64(&self.size))
    }

    /// `None` when either field is unparseable — callers must treat that as a desynced
    /// frame (resync for a fresh snapshot), never as a zero.
    #[inline]
    pub fn parsed_opt(&self) -> Option<(f64, f64)> {
        Some((parse_f64_opt(&self.price)?, parse_f64_opt(&self.size)?))
    }
}

#[derive(Debug, Deserialize)]
pub struct OrderBookPayloadRef<'a> {
    #[serde(borrow, default)]
    pub bids: Vec<PriceLevelRef<'a>>,
    #[serde(borrow, default)]
    pub asks: Vec<PriceLevelRef<'a>>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(default)]
    pub nonce: Option<i64>,
    #[serde(default)]
    pub begin_nonce: Option<i64>,
}

/// Borrowed twin of [`OrderBookMsg`] for the hot ingest path: deserializing straight from
/// raw WS text (or a `&Value`) borrows every string instead of deep-cloning the tree.
/// `Cow` on the type tag: an escaped tag (legal JSON, e.g. `"subscribed\/order_book"`)
/// costs an allocation instead of a dropped frame.
#[derive(Debug, Deserialize)]
pub struct OrderBookMsgRef<'a> {
    #[serde(rename = "type", borrow)]
    pub msg_type: std::borrow::Cow<'a, str>,
    #[serde(default)]
    pub offset: Option<u64>,
    #[serde(borrow)]
    pub order_book: OrderBookPayloadRef<'a>,
}

impl OrderBookMsgRef<'_> {
    pub fn is_snapshot(&self) -> bool {
        self.msg_type.contains("subscribed")
    }
    /// Prefer envelope offset, fall back to payload offset.
    pub fn effective_offset(&self) -> Option<u64> {
        self.offset.or(self.order_book.offset)
    }
    /// See [`classify_book_update`].
    pub fn contiguity(
        &self,
        last_nonce: Option<i64>,
        last_offset: Option<u64>,
    ) -> BookUpdateContiguity {
        classify_book_update(
            self.order_book.begin_nonce,
            self.order_book.nonce,
            self.effective_offset(),
            last_nonce,
            last_offset,
        )
    }
}

/// `ticker/{m}` — best bid/ask nested under `ticker`.
#[derive(Debug, Deserialize)]
pub struct TickerMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub ticker: HashMap<String, serde_json::Value>,
}

impl TickerMsg {
    fn field(&self, k: &str) -> Option<f64> {
        self.ticker.get(k).and_then(|v| match v {
            serde_json::Value::String(s) => fast_float::parse(s).ok(),
            serde_json::Value::Number(n) => n.as_f64(),
            _ => None,
        })
    }
    pub fn best_bid(&self) -> Option<f64> {
        self.field("best_bid").or_else(|| self.field("bid"))
    }
    pub fn best_ask(&self) -> Option<f64> {
        self.field("best_ask").or_else(|| self.field("ask"))
    }
}

/// `account_orders/{m}/{a}` / `account_all_orders/{a}` — `orders` keyed by market id.
/// `type` is defaulted: the pre-typed consumer extracted `orders` regardless of the tag,
/// and an untyped frame must keep updating the open-orders cache rather than stall it.
#[derive(Debug, Deserialize)]
pub struct AccountOrdersMsg {
    #[serde(rename = "type", default)]
    pub msg_type: String,
    #[serde(default)]
    pub orders: HashMap<String, Vec<RemoteOrder>>,
}

/// `account_all/{a}` — trades plus assorted account fields keyed by market id. Positions can
/// also appear here, but in live traffic those payloads may be sparse; position state is kept
/// from the dedicated `account_all_positions/{a}` stream below.
#[derive(Debug, Deserialize)]
pub struct AccountAllMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub positions: HashMap<String, PositionPayload>,
    #[serde(default)]
    pub trades: HashMap<String, Vec<TradePayload>>,
}

/// `account_all_positions/{a}` — initial full snapshot is explicitly tagged
/// `subscribed/account_all_positions`; later `update/account_all_positions` messages are sparse
/// deltas and an absent market means unchanged, not flat.
#[derive(Debug, Deserialize)]
pub struct AccountAllPositionsMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub positions: HashMap<String, PositionPayload>,
}

impl AccountAllPositionsMsg {
    pub fn is_snapshot(&self) -> bool {
        self.msg_type.starts_with("subscribed/")
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct PositionPayload {
    #[serde(default)]
    pub position: Option<String>,
    #[serde(default)]
    pub sign: Option<i32>,
    #[serde(default)]
    pub avg_entry_price: Option<String>,
}

impl PositionPayload {
    /// Signed position size in base units.
    pub fn signed(&self) -> f64 {
        let mag = self.position.as_deref().map(parse_f64).unwrap_or(0.0);
        match self.sign {
            Some(s) if s < 0 => -mag.abs(),
            _ => mag.abs(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct TradePayload {
    #[serde(rename = "type", default)]
    pub trade_type: Option<String>,
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub size: Option<String>,
    #[serde(default)]
    pub usd_amount: Option<String>,
    #[serde(default)]
    pub is_maker_ask: Option<bool>,
    #[serde(default)]
    pub ask_id: Option<i64>,
    #[serde(default)]
    pub bid_id: Option<i64>,
    #[serde(default)]
    pub ask_client_id: Option<i64>,
    #[serde(default)]
    pub bid_client_id: Option<i64>,
    #[serde(default)]
    pub ask_account_id: Option<i64>,
    #[serde(default)]
    pub bid_account_id: Option<i64>,
    #[serde(default)]
    pub ask_account_pnl: Option<String>,
    #[serde(default)]
    pub bid_account_pnl: Option<String>,
    #[serde(default)]
    pub maker_fee: Option<serde_json::Value>,
    #[serde(default)]
    pub taker_fee: Option<serde_json::Value>,
    #[serde(default)]
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub transaction_time: Option<i64>,
    #[serde(default)]
    pub trade_id: Option<i64>,
}

impl TradePayload {
    #[inline]
    pub fn price_f64(&self) -> Option<f64> {
        parse_opt_f64(&self.price)
    }

    #[inline]
    pub fn size_f64(&self) -> Option<f64> {
        parse_opt_f64(&self.size)
    }

    #[inline]
    pub fn usd_amount_f64(&self) -> Option<f64> {
        parse_opt_f64(&self.usd_amount)
    }

    #[inline]
    pub fn ask_account_pnl_f64(&self) -> Option<f64> {
        parse_opt_f64(&self.ask_account_pnl)
    }

    #[inline]
    pub fn bid_account_pnl_f64(&self) -> Option<f64> {
        parse_opt_f64(&self.bid_account_pnl)
    }

    #[inline]
    pub fn event_time_ms(&self) -> Option<i64> {
        self.transaction_time
            .or(self.timestamp)
            .map(normalize_timestamp_ms)
    }
}

#[inline]
fn parse_opt_f64(v: &Option<String>) -> Option<f64> {
    v.as_deref().and_then(|s| fast_float::parse(s).ok())
}

fn normalize_timestamp_ms(ts: i64) -> i64 {
    let abs = ts.abs();
    if abs > 10_000_000_000_000_000 {
        ts / 1_000_000
    } else if abs > 10_000_000_000_000 {
        ts / 1_000
    } else if abs < 10_000_000_000 {
        ts * 1_000
    } else {
        ts
    }
}

/// `user_stats/{a}` — capital/portfolio.
#[derive(Debug, Deserialize)]
pub struct UserStatsMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    #[serde(default)]
    pub stats: StatsPayload,
}

#[derive(Debug, Deserialize, Default)]
pub struct StatsPayload {
    #[serde(default)]
    pub available_balance: Option<serde_json::Value>,
    #[serde(default)]
    pub portfolio_value: Option<serde_json::Value>,
}

fn val_f64(v: &Option<serde_json::Value>) -> Option<f64> {
    match v {
        Some(serde_json::Value::String(s)) => fast_float::parse(s).ok(),
        Some(serde_json::Value::Number(n)) => n.as_f64(),
        _ => None,
    }
}

impl StatsPayload {
    pub fn available_capital(&self) -> Option<f64> {
        val_f64(&self.available_balance)
    }
    pub fn portfolio_value(&self) -> Option<f64> {
        val_f64(&self.portfolio_value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_orderbook_snapshot() {
        let raw = r#"{"type":"subscribed/order_book","offset":405053,
            "order_book":{"bids":[{"price":"64820.2","size":"0.00051"}],
            "asks":[{"price":"64820.3","size":"0.19283"}],"offset":405053}}"#;
        let m: OrderBookMsg = serde_json::from_str(raw).unwrap();
        assert!(m.is_snapshot());
        assert_eq!(m.effective_offset(), Some(405053));
        assert_eq!(m.order_book.bids[0].parsed(), (64820.2, 0.00051));
    }

    #[test]
    fn borrowed_orderbook_msg_matches_owned() {
        // The borrowed hot-path view and the owned type must agree on snapshot
        // detection, offsets, level values, and every contiguity verdict.
        let cases = [
            r#"{"type":"subscribed/order_book","offset":405053,
                "order_book":{"bids":[{"price":"64820.2","size":"0.00051"}],
                "asks":[{"price":"64820.3","size":"0.19283"}],"offset":405053}}"#,
            r#"{"type":"update/order_book",
                "order_book":{"bids":[{"price":"1.5","size":"0"}],"asks":[],
                "offset":7,"nonce":12,"begin_nonce":10}}"#,
            r#"{"type":"update/order_book","order_book":{"bids":[],"asks":[]}}"#,
        ];
        let seq_positions = [
            (None, None),
            (Some(10i64), Some(6u64)),
            (Some(11), Some(7)),
            (Some(12), Some(8)),
            (Some(9), Some(3)),
        ];
        for raw in cases {
            let value: serde_json::Value = serde_json::from_str(raw).unwrap();
            let owned: OrderBookMsg = serde_json::from_value(value.clone()).unwrap();
            // The borrowed view must parse identically from a routed Value AND from
            // raw text (the production ingest path since the LighterFrame refactor).
            for borrowed in [
                OrderBookMsgRef::deserialize(&value).unwrap(),
                serde_json::from_str::<OrderBookMsgRef<'_>>(raw).unwrap(),
            ] {
                assert_eq!(owned.is_snapshot(), borrowed.is_snapshot());
                assert_eq!(owned.effective_offset(), borrowed.effective_offset());
                assert_eq!(owned.order_book.bids.len(), borrowed.order_book.bids.len());
                for (o, b) in owned.order_book.bids.iter().zip(&borrowed.order_book.bids) {
                    assert_eq!(o.parsed(), b.parsed());
                }
                for (last_nonce, last_offset) in seq_positions {
                    assert_eq!(
                        owned.contiguity(last_nonce, last_offset),
                        borrowed.contiguity(last_nonce, last_offset),
                        "contiguity diverged for {raw} at {last_nonce:?}/{last_offset:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn borrowed_orderbook_msg_survives_json_escapes_in_raw_text() {
        // An escaped solidus in the type tag is legal JSON. Borrowing &str from raw
        // text would fail here — the Cow fields must fall back to owned instead of
        // dropping the frame.
        let raw = r#"{"type":"subscribed\/order_book","offset":1,
            "order_book":{"bids":[{"price":"100.5","size":"2"}],
            "asks":[{"price":"101","size":"3"}],"offset":1}}"#;
        let m: OrderBookMsgRef<'_> = serde_json::from_str(raw).expect("escaped tag must parse");
        assert!(m.is_snapshot());
        assert_eq!(m.order_book.bids[0].parsed(), (100.5, 2.0));
    }

    #[test]
    fn parse_orderbooks_rest() {
        let raw = r#"{"code":200,"order_books":[{"symbol":"BTC","market_id":1,
            "min_base_amount":"0.00020","min_quote_amount":"10.000000",
            "supported_size_decimals":5,"supported_price_decimals":1,"maker_fee":"0.0000","status":"active"}]}"#;
        let r: OrderBooksResponse = serde_json::from_str(raw).unwrap();
        let btc = &r.order_books[0];
        assert_eq!(btc.market_id, 1);
        assert_eq!(btc.supported_price_decimals, 1);
        assert_eq!(parse_f64(&btc.min_base_amount), 0.0002);
    }

    #[test]
    fn position_sign() {
        let p = PositionPayload {
            position: Some("0.0050".into()),
            sign: Some(-1),
            avg_entry_price: None,
        };
        assert!((p.signed() + 0.005).abs() < 1e-12);
    }

    #[test]
    fn parse_account_all_trade_fields_for_pnl() {
        let raw = r#"{
            "type":"update/account_all",
            "trades":{"1":[{
                "type":"trade",
                "trade_id":123,
                "timestamp":1781764389313,
                "transaction_time":1781764389314,
                "price":"64152.1",
                "size":"0.00043",
                "usd_amount":"27.585403",
                "ask_id":11,
                "bid_id":22,
                "ask_client_id":111,
                "bid_client_id":222,
                "ask_account_id":9,
                "bid_account_id":7,
                "ask_account_pnl":"-0.01",
                "bid_account_pnl":"0.02",
                "is_maker_ask":false,
                "maker_fee":40
            }]}
        }"#;
        let msg: AccountAllMsg = serde_json::from_str(raw).unwrap();
        let t = &msg.trades["1"][0];
        assert_eq!(t.trade_type.as_deref(), Some("trade"));
        assert_eq!(t.trade_id, Some(123));
        assert_eq!(t.ask_client_id, Some(111));
        assert_eq!(t.bid_client_id, Some(222));
        assert_eq!(t.ask_id, Some(11));
        assert_eq!(t.bid_id, Some(22));
        assert_eq!(t.event_time_ms(), Some(1_781_764_389_314));
        assert!((t.price_f64().unwrap() - 64152.1).abs() < 1e-12);
        assert!((t.size_f64().unwrap() - 0.00043).abs() < 1e-12);
        assert!((t.usd_amount_f64().unwrap() - 27.585403).abs() < 1e-12);
        assert!((t.ask_account_pnl_f64().unwrap() + 0.01).abs() < 1e-12);
        assert!((t.bid_account_pnl_f64().unwrap() - 0.02).abs() < 1e-12);
    }
}
