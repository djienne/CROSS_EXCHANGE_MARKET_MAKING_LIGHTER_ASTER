//! Aster V3 live execution worker (plan §3). Builds correct V3 signed-request payloads
//! (post-only GTX maker orders, per-order + bulk cancels, dead-man countdown), manages the
//! microsecond nonce / millisecond timestamp, signs via [`AsterSigner`] (ABI-encode + EIP-191,
//! confirmed live), POSTs them, and parses the response into lifecycle [`ExecEvent`]s.
//!
//! The signing scheme is the working Passivbot recipe (NOT the EIP-712 typed-data the docs
//! described) — verified against the live API: a real post-only place returned HTTP 200/`NEW`
//! and a cancel returned `CANCELED`. See `scripts/aster_probe.py` for the byte-for-byte oracle.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use reqwest::Method;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{info, warn};

use super::command::{ExecCommand, ExecEvent};
use super::sign::{AsterNonce, AsterSigner, MonotonicMs};
use crate::livebot::scale::MarketScale;
use crate::types::{MarketId, Side};

const ASTER_ORDER_PATH: &str = "/fapi/v3/order";
const ASTER_ALL_ORDERS_PATH: &str = "/fapi/v3/allOpenOrders";
const ASTER_DEADMAN_PATH: &str = "/fapi/v3/countdownCancelAll";
const ASTER_RECV_WINDOW: &str = "50000";
const USER_AGENT: &str = "xemm-livebot";

/// Per-market wire context: the scale (ticks/lots → Decimal) and the Aster symbol.
#[derive(Clone)]
struct MarketWire {
    scale: MarketScale,
    symbol: String,
}

/// A row of the signed `/fapi/v3/balance` response.
#[derive(Debug, Clone, Deserialize)]
pub struct AsterBalanceRow {
    pub asset: String,
    pub balance: String,
    #[serde(rename = "crossWalletBalance", default)]
    pub cross_wallet_balance: String,
    #[serde(rename = "availableBalance", default)]
    pub available_balance: String,
}

/// A row of the signed `/fapi/v3/positionRisk` response.
#[derive(Debug, Clone, Deserialize)]
pub struct AsterPositionRow {
    pub symbol: String,
    #[serde(rename = "positionAmt")]
    pub position_amt: String,
    #[serde(rename = "entryPrice", default)]
    pub entry_price: String,
    #[serde(rename = "unRealizedProfit", default)]
    pub unrealized_profit: String,
    #[serde(rename = "positionSide", default)]
    pub position_side: String,
    /// Account leverage for this symbol (read by the startup leverage gate).
    #[serde(default)]
    pub leverage: String,
}

/// A row of the signed `/fapi/v3/openOrders` response.
#[derive(Debug, Clone, Deserialize)]
pub struct AsterOpenOrder {
    pub symbol: String,
    #[serde(rename = "orderId")]
    pub order_id: i64,
    #[serde(rename = "clientOrderId", default)]
    pub client_order_id: String,
    #[serde(default)]
    pub side: String,
    #[serde(default)]
    pub price: String,
    #[serde(rename = "origQty", default)]
    pub orig_qty: String,
    #[serde(default)]
    pub status: String,
}

/// Parsed Aster order response (POST/DELETE `/fapi/v3/order`). Only the fields we act on.
#[derive(Debug, Deserialize)]
struct AsterOrderResp {
    #[serde(rename = "orderId")]
    order_id: Option<i64>,
    status: Option<String>,
    #[serde(rename = "clientOrderId")]
    client_order_id: Option<String>,
    code: Option<i64>,
    msg: Option<String>,
}

/// The signed Aster REST client + nonce/timestamp + per-market wire context. Constructed only
/// in `mode = "live"`.
pub struct AsterRest {
    client: reqwest::Client,
    base_url: String,
    signer: Arc<dyn AsterSigner>,
    nonce: AsterNonce,
    timestamp: MonotonicMs,
    markets: HashMap<MarketId, MarketWire>,
    deadman_countdown_ms: i64,
    rate_limit_backoff_ms: i64,
    max_rest_requests_per_minute: u32,
    /// Self-trade-prevention mode for maker orders (e.g. `EXPIRE_MAKER`); `None` omits it.
    stp_mode: Option<String>,
}

impl AsterRest {
    pub fn new(
        base_url: String,
        signer: Arc<dyn AsterSigner>,
        market_scales: HashMap<MarketId, (MarketScale, String)>,
        deadman_countdown_ms: i64,
        rate_limit_backoff_ms: i64,
        max_rest_requests_per_minute: u32,
        stp_mode: Option<String>,
    ) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5)) // short: a stalled order call must not wedge the worker
            // Transport tuning (deployment-independent — helps on ANY network, not a fast-VPS knob):
            // disable Nagle so a small signed order POST goes out immediately; keep the pre-warmed
            // TLS connection alive between sparse orders so a hedge/cancel never pays a fresh handshake.
            .tcp_nodelay(true)
            .pool_idle_timeout(Some(Duration::from_secs(120)))
            .pool_max_idle_per_host(4)
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .build()?;
        let markets = market_scales
            .into_iter()
            .map(|(m, (scale, symbol))| (m, MarketWire { scale, symbol }))
            .collect();
        Ok(AsterRest {
            client,
            base_url,
            signer,
            nonce: AsterNonce::new(),
            timestamp: MonotonicMs::new(),
            markets,
            deadman_countdown_ms: deadman_countdown_ms.max(1000),
            rate_limit_backoff_ms: rate_limit_backoff_ms.max(1000),
            max_rest_requests_per_minute: max_rest_requests_per_minute.max(1),
            stp_mode,
        })
    }

    fn wire(&self, market: &MarketId) -> Result<&MarketWire> {
        self.markets
            .get(market)
            .ok_or_else(|| anyhow!("no wire context for market {market}"))
    }

    /// Build business params for a post-only GTX maker order (one-way mode: `positionSide=BOTH`).
    fn place_params(
        &self,
        market: &MarketId,
        side: Side,
        price_ticks: i64,
        qty_lots: i64,
        client_id: &str,
        reduce_only: bool,
    ) -> Result<Vec<(String, String)>> {
        let w = self.wire(market)?;
        let price = w.scale.ticks_to_price(price_ticks);
        let qty = w.scale.lots_to_qty(qty_lots);
        let mut p = vec![
            ("symbol".into(), w.symbol.clone()),
            ("side".into(), side.as_str().to_string()), // BUY / SELL
            ("type".into(), "LIMIT".into()),
            ("timeInForce".into(), "GTX".into()), // Good Till Crossing = post-only
            ("quantity".into(), trim_dec(qty)),
            ("price".into(), trim_dec(price)),
            ("newClientOrderId".into(), client_id.to_string()),
            ("positionSide".into(), "BOTH".into()),
        ];
        if reduce_only {
            p.push(("reduceOnly".into(), "true".into()));
        }
        if let Some(stp) = &self.stp_mode {
            p.push(("stpMode".into(), stp.clone()));
        }
        Ok(p)
    }

    /// Inject auth (recvWindow/timestamp/nonce/user/signer/signature), sign the canonical
    /// `json_str`, send, and return the response body. Reads use GET (query string); writes use
    /// the body form. FAILS only on transport/HTTP error — a `200` with a venue error code is
    /// returned for the caller to classify.
    async fn signed_request(&self, method: Method, path: &str, business: Vec<(String, String)>) -> Result<String> {
        // The signed params = business params + recvWindow + timestamp (NOT nonce/user/signer).
        let mut params = business;
        params.push(("recvWindow".into(), ASTER_RECV_WINDOW.into()));
        params.push(("timestamp".into(), self.timestamp.next().to_string()));
        // Canonical json_str: sorted keys, compact separators — must match the signer's ABI input.
        let json_map: std::collections::BTreeMap<&str, &str> =
            params.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let json_str = serde_json::to_string(&json_map)?;
        let nonce = self.nonce.next();
        let sig = self.signer.sign_v3(&json_str, nonce)?;
        params.push(("nonce".into(), nonce.to_string()));
        params.push(("user".into(), self.signer.user_address().to_string()));
        params.push(("signer".into(), self.signer.signer_address().to_string()));
        params.push(("signature".into(), sig.0));

        let url = format!("{}{}", self.base_url, path);
        let builder = match method {
            Method::GET => self.client.get(&url).query(&params),
            Method::POST => self.client.post(&url).form(&params),
            Method::DELETE => self.client.delete(&url).form(&params),
            Method::PUT => self.client.put(&url).form(&params),
            other => return Err(anyhow!("unsupported method {other}")),
        };
        let resp = builder.header("User-Agent", USER_AGENT).send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(anyhow!("aster {path} HTTP {}: {}", status.as_u16(), text));
        }
        Ok(text)
    }

    /// Place a maker order from `Decimal` price/qty (rounds passively: buy floors, sell ceils
    /// to tick; qty floors to lot). Used by the probe harness and flatten paths.
    pub(crate) async fn place_decimal(
        &self,
        market: &MarketId,
        side: Side,
        px: Decimal,
        qty: Decimal,
        client_id: &str,
        reduce_only: bool,
    ) -> ExecEvent {
        let w = match self.wire(market) {
            Ok(w) => w,
            Err(e) => return ExecEvent::PlaceReject { client_id: client_id.to_string(), reason: e.to_string() },
        };
        let price_ticks = match side {
            Side::Buy => w.scale.price_floor_ticks(px),
            Side::Sell => w.scale.price_ceil_ticks(px),
        };
        let qty_lots = w.scale.qty_to_lots(qty);
        self.place(market, side, price_ticks, qty_lots, client_id, reduce_only).await
    }

    // --- signed reads (shared by the probe harness + the account reconciler) ---

    /// Signed USDⓈ-M balance read (`GET /fapi/v3/balance`).
    pub async fn balance(&self) -> Result<Vec<AsterBalanceRow>> {
        let body = self.signed_request(Method::GET, "/fapi/v3/balance", vec![]).await?;
        serde_json::from_str(&body).map_err(|e| anyhow!("parse balance: {e}: {body}"))
    }

    /// Signed position read (`GET /fapi/v3/positionRisk`).
    pub async fn position_risk(&self) -> Result<Vec<AsterPositionRow>> {
        let body = self.signed_request(Method::GET, "/fapi/v3/positionRisk", vec![]).await?;
        serde_json::from_str(&body).map_err(|e| anyhow!("parse positionRisk: {e}: {body}"))
    }

    /// Whether the account is in ONE-WAY position mode (`dualSidePosition == false`). The bot
    /// assumes one-way (`positionSide=BOTH`); hedge mode would mis-route orders and mis-report
    /// positions, so live trading refuses to start unless this is true.
    pub async fn is_one_way(&self) -> Result<bool> {
        let body = self.signed_request(Method::GET, "/fapi/v3/positionSide/dual", vec![]).await?;
        let v: serde_json::Value = serde_json::from_str(&body)?;
        Ok(!v.get("dualSidePosition").and_then(|d| d.as_bool()).unwrap_or(false))
    }

    /// Signed open-orders read (`GET /fapi/v3/openOrders`), optionally for one symbol.
    pub async fn open_orders(&self, market: Option<&MarketId>) -> Result<Vec<AsterOpenOrder>> {
        let params = match market {
            Some(m) => vec![("symbol".into(), self.wire(m)?.symbol.clone())],
            None => vec![],
        };
        let body = self.signed_request(Method::GET, "/fapi/v3/openOrders", params).await?;
        serde_json::from_str(&body).map_err(|e| anyhow!("parse openOrders: {e}: {body}"))
    }

    /// The Aster symbol for a market (for probe display / position matching).
    pub fn symbol_of(&self, market: &MarketId) -> Option<String> {
        self.markets.get(market).map(|w| w.symbol.clone())
    }

    // --- user-data stream listenKey lifecycle (signed; NO listenKey param per V3 docs) ---

    /// Create a user-data stream listenKey (`POST /fapi/v3/listenKey`).
    pub async fn create_listen_key(&self) -> Result<String> {
        let body = self.signed_request(Method::POST, "/fapi/v3/listenKey", vec![]).await?;
        let v: serde_json::Value = serde_json::from_str(&body)?;
        v.get("listenKey")
            .and_then(|s| s.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("no listenKey in response: {body}"))
    }

    /// Keep the listenKey alive (`PUT /fapi/v3/listenKey`, no params). ~30-min cadence.
    pub async fn keepalive_listen_key(&self) -> Result<()> {
        self.signed_request(Method::PUT, "/fapi/v3/listenKey", vec![]).await.map(|_| ())
    }

    /// Close the listenKey (`DELETE /fapi/v3/listenKey`) — only on graceful shutdown.
    pub async fn close_listen_key(&self) -> Result<()> {
        self.signed_request(Method::DELETE, "/fapi/v3/listenKey", vec![]).await.map(|_| ())
    }

    /// Place a maker order and classify the response into a place lifecycle event.
    pub(crate) async fn place(
        &self,
        market: &MarketId,
        side: Side,
        price_ticks: i64,
        qty_lots: i64,
        client_id: &str,
        reduce_only: bool,
    ) -> ExecEvent {
        let params = match self.place_params(market, side, price_ticks, qty_lots, client_id, reduce_only) {
            Ok(p) => p,
            Err(e) => return ExecEvent::PlaceReject { client_id: client_id.to_string(), reason: e.to_string() },
        };
        match self.signed_request(Method::POST, ASTER_ORDER_PATH, params).await {
            Ok(body) => classify_place(client_id, &body),
            Err(e) => ExecEvent::PlaceUnknown {
                client_id: client_id.to_string(),
                reason: e.to_string(),
            },
        }
    }

    /// Cancel a specific order by client id (DELETE `/fapi/v3/order`). A `-2011` "unknown order"
    /// is treated as already-gone (success), per plan §3.
    pub(crate) async fn cancel_order(&self, market: &MarketId, client_id: &str) -> Result<CancelOutcome> {
        let w = self.wire(market)?;
        let params = vec![
            ("symbol".into(), w.symbol.clone()),
            ("origClientOrderId".into(), client_id.to_string()),
        ];
        match self.signed_request(Method::DELETE, ASTER_ORDER_PATH, params).await {
            // Aster returns HTTP 200 even for some venue errors ({code,msg} in the body), so
            // transport success != cancel success — classify the body (plan: no false CancelAck).
            Ok(body) => classify_cancel(&body),
            Err(e) if e.to_string().contains("-2011") => Ok(CancelOutcome::AlreadyGone),
            Err(e) => Err(e),
        }
    }

    /// Reduce-only MARKET (taker) order to flatten an orphaned position (recovery path).
    /// Reduce-only orders are exempt from the min-notional filter, so a sub-min residual can
    /// still be closed. `side` = SELL to close a long, BUY to close a short.
    pub(crate) async fn flatten(&self, market: &MarketId, side: Side, qty: Decimal) -> Result<()> {
        let w = self.wire(market)?;
        let qty_lots = w.scale.qty_to_lots(qty);
        if qty_lots <= 0 {
            return Ok(()); // sub-lot dust: nothing to flatten
        }
        let q = w.scale.lots_to_qty(qty_lots);
        let params = vec![
            ("symbol".into(), w.symbol.clone()),
            ("side".into(), side.as_str().to_string()),
            ("type".into(), "MARKET".into()),
            ("quantity".into(), trim_dec(q)),
            ("positionSide".into(), "BOTH".into()),
            ("reduceOnly".into(), "true".into()),
        ];
        self.signed_request(Method::POST, ASTER_ORDER_PATH, params).await.map(|_| ())
    }

    /// Refresh the Aster dead-man countdown for a symbol (heartbeat; §3.4).
    async fn refresh_deadman(&self, market: &MarketId) -> Result<()> {
        let w = self.wire(market)?;
        let params = vec![
            ("symbol".into(), w.symbol.clone()),
            ("countdownTime".into(), self.deadman_countdown_ms.to_string()),
        ];
        self.signed_request(Method::POST, ASTER_DEADMAN_PATH, params).await.map(|_| ())
    }

    pub(crate) async fn cancel_all_symbol(&self, market: &MarketId) -> Result<()> {
        let w = self.wire(market)?;
        let params = vec![("symbol".into(), w.symbol.clone())];
        self.signed_request(Method::DELETE, ASTER_ALL_ORDERS_PATH, params).await.map(|_| ())
    }

    /// Read the account's CURRENT leverage for a symbol (`GET /fapi/v3/positionRisk?symbol=…`).
    /// Aster has no EVM-signed SET-leverage endpoint (`/fapi/v1/leverage` is legacy HMAC-only and
    /// rejects EVM auth with `-2014`), so the startup gate VERIFIES leverage rather than setting it —
    /// the operator sets it once on the Aster UI. The `symbol` param makes the row present even when
    /// the position is flat.
    pub(crate) async fn get_leverage(&self, market: &MarketId) -> Result<u32> {
        let w = self.wire(market)?;
        let body = self
            .signed_request(Method::GET, "/fapi/v3/positionRisk", vec![("symbol".into(), w.symbol.clone())])
            .await?;
        let rows: Vec<AsterPositionRow> =
            serde_json::from_str(&body).map_err(|e| anyhow::anyhow!("parse positionRisk leverage: {e}: {body}"))?;
        let row = rows
            .iter()
            .find(|r| r.symbol.eq_ignore_ascii_case(&w.symbol))
            .ok_or_else(|| anyhow::anyhow!("aster positionRisk has no row for {}", w.symbol))?;
        row.leverage
            .trim()
            .parse::<f64>()
            .map(|l| l.round() as u32)
            .map_err(|e| anyhow::anyhow!("aster leverage parse '{}': {e}", row.leverage))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CancelOutcome {
    /// Venue explicitly confirmed cancel.
    Canceled,
    /// Venue says it does not know the order. Safe for cancel, not safe for replace-place.
    AlreadyGone,
    /// The order is no longer resting because it filled/expired. Safe for cancel, not for replace-place.
    FilledOrExpired,
}

/// Classify a DELETE `/fapi/v3/order` response into cancel success/failure. Aster returns HTTP 200
/// even for some venue errors, so the BODY decides. `-2011` ("unknown order") is already-gone =
/// success for ordinary cancels, but not enough proof to place a replacement. `FILLED`/`EXPIRED`
/// likewise means not resting, but the strategy must wait for the user stream/reconciler before
/// placing a replacement. Any other code/status is a REAL failure the worker must report as a
/// `CancelReject` so the strategy keeps the (possibly still-resting) order and freezes.
fn classify_cancel(body: &str) -> Result<CancelOutcome> {
    match serde_json::from_str::<AsterOrderResp>(body) {
        Ok(r) => {
            if let Some(code) = r.code {
                if code == -2011 {
                    return Ok(CancelOutcome::AlreadyGone);
                }
                anyhow::bail!("cancel venue error code {code}: {}", r.msg.unwrap_or_default());
            }
            match r.status.as_deref() {
                Some("CANCELED") => Ok(CancelOutcome::Canceled),
                Some("FILLED") | Some("EXPIRED") => Ok(CancelOutcome::FilledOrExpired),
                // Conservative: a missing/unexpected status is NOT a confirmed cancel — treat it as a
                // failure (→ CancelReject → freeze + sweep). A false negative only costs a transient
                // freeze; a false positive (acking an un-cancelled order) is the dangerous case.
                Some(other) => anyhow::bail!("cancel unexpected status {other}"),
                None => anyhow::bail!("cancel response missing status (and no code): {body}"),
            }
        }
        Err(e) => anyhow::bail!("unparseable cancel response: {e}: {body}"),
    }
}

/// Classify a `/fapi/v3/order` POST response body into a place lifecycle event. A GTX order that
/// would cross rests as `EXPIRED` (treat as a reject → re-quote, not an error). Only a clean
/// `NEW` response is a safe resting-order ack. `FILLED`/`PARTIALLY_FILLED`, malformed success
/// bodies, and missing `orderId` are ambiguous: an order may have existed and moved inventory
/// before the user stream reported it, so freeze+sweep via `PlaceUnknown` instead of silently
/// closing the local slot.
fn classify_place(client_id: &str, body: &str) -> ExecEvent {
    match serde_json::from_str::<AsterOrderResp>(body) {
        Ok(r) => {
            if let Some(code) = r.code {
                return ExecEvent::PlaceReject {
                    client_id: client_id.to_string(),
                    reason: format!("code {code}: {}", r.msg.unwrap_or_default()),
                };
            }
            match r.status.as_deref() {
                Some("NEW") => match r.order_id {
                    Some(oid) => ExecEvent::PlaceAck {
                        client_id: r.client_order_id.unwrap_or_else(|| client_id.to_string()),
                        venue_order_id: oid.to_string(),
                    },
                    None => ExecEvent::PlaceUnknown {
                        client_id: client_id.to_string(),
                        reason: format!("NEW response missing orderId: {body}"),
                    },
                },
                Some("EXPIRED") => ExecEvent::PlaceReject {
                    client_id: client_id.to_string(),
                    reason: "status EXPIRED".into(),
                },
                Some("REJECTED") => ExecEvent::PlaceReject {
                    client_id: client_id.to_string(),
                    reason: "status REJECTED".into(),
                },
                Some("PARTIALLY_FILLED") | Some("FILLED") => ExecEvent::PlaceUnknown {
                    client_id: r.client_order_id.unwrap_or_else(|| client_id.to_string()),
                    reason: format!("place returned fill status; wait for user stream/reconcile: {body}"),
                },
                Some(other) => ExecEvent::PlaceUnknown {
                    client_id: client_id.to_string(),
                    reason: format!("unexpected place status {other}: {body}"),
                },
                None => ExecEvent::PlaceUnknown {
                    client_id: client_id.to_string(),
                    reason: format!("place response missing status: {body}"),
                },
            }
        }
        Err(e) => ExecEvent::PlaceUnknown {
            client_id: client_id.to_string(),
            reason: format!("unparseable place response: {e}: {body}"),
        },
    }
}

fn is_aster_rate_limit_reason(reason: &str) -> bool {
    let r = reason.to_ascii_lowercase();
    r.contains("http 429") || r.contains("code -1003") || r.contains("too many requests")
}

fn exec_event_rate_limit_reason(ev: &ExecEvent) -> Option<&str> {
    match ev {
        ExecEvent::PlaceReject { reason, .. }
        | ExecEvent::PlaceUnknown { reason, .. }
        | ExecEvent::CancelReject { reason, .. }
        | ExecEvent::AsterFlattenReject { reason, .. } if is_aster_rate_limit_reason(reason) => Some(reason.as_str()),
        _ => None,
    }
}

async fn notify_rate_limited(tx: &Sender<ExecEvent>, reason: String, backoff_ms: i64) {
    warn!("aster REST rate limited/backing off for {backoff_ms}ms: {reason}");
    let _ = tx.send(ExecEvent::AsterRateLimited { reason, backoff_ms }).await;
}

async fn send_backoff_reject(tx: &Sender<ExecEvent>, cmd: ExecCommand, reason: String, backoff_ms: i64) {
    match cmd {
        ExecCommand::Place { client_id, .. } => {
            let _ = tx.send(ExecEvent::PlaceReject { client_id, reason: reason.clone() }).await;
        }
        ExecCommand::Cancel { client_id, .. } => {
            let _ = tx.send(ExecEvent::CancelReject { client_id, reason: reason.clone() }).await;
        }
        ExecCommand::Replace { old_client_id, new_client_id, .. } => {
            let _ = tx.send(ExecEvent::CancelReject { client_id: old_client_id, reason: reason.clone() }).await;
            let _ = tx
                .send(ExecEvent::PlaceReject {
                    client_id: new_client_id,
                    reason: "replace skipped because Aster REST backoff is active".into(),
                })
                .await;
        }
        ExecCommand::FlattenAster { market, side, qty } => {
            let _ = tx.send(ExecEvent::AsterFlattenReject { market, side, qty, reason: reason.clone() }).await;
        }
        ExecCommand::CancelMarket { .. }
        | ExecCommand::CancelAllBot
        | ExecCommand::RefreshDeadman { .. } => {}
        ExecCommand::Shutdown => {}
    }
    notify_rate_limited(tx, reason, backoff_ms).await;
}

struct RestCommandLimiter {
    max_per_minute: u32,
    sent: VecDeque<tokio::time::Instant>,
}

impl RestCommandLimiter {
    fn new(max_per_minute: u32) -> Self {
        RestCommandLimiter { max_per_minute: max_per_minute.max(1), sent: VecDeque::new() }
    }

    async fn acquire(&mut self) {
        let window = Duration::from_secs(60);
        loop {
            let now = tokio::time::Instant::now();
            while self.sent.front().is_some_and(|&t| now.saturating_duration_since(t) >= window) {
                self.sent.pop_front();
            }
            if (self.sent.len() as u32) < self.max_per_minute {
                self.sent.push_back(now);
                return;
            }
            if let Some(&oldest) = self.sent.front() {
                tokio::time::sleep_until(oldest + window).await;
            } else {
                return;
            }
        }
    }
}

/// The Aster execution worker loop: drain commands, perform venue I/O, publish events.
/// Constructed only under `mode = "live"`.
pub async fn run_aster_worker(mut rx: Receiver<ExecCommand>, tx: Sender<ExecEvent>, rest: AsterRest) {
    info!("aster live exec worker started (real signing wired; ABI+EIP-191, live-verified)");
    let mut backoff_until: Option<tokio::time::Instant> = None;
    let mut limiter = RestCommandLimiter::new(rest.max_rest_requests_per_minute);
    while let Some(cmd) = rx.recv().await {
        if matches!(cmd, ExecCommand::Shutdown) {
            break;
        }
        if let Some(until) = backoff_until {
            let now = tokio::time::Instant::now();
            if now < until {
                let remaining_ms = until.saturating_duration_since(now).as_millis() as i64;
                send_backoff_reject(
                    &tx,
                    cmd,
                    format!("Aster REST backoff active ({}ms remaining)", remaining_ms),
                    remaining_ms.max(1),
                )
                .await;
                continue;
            }
            backoff_until = None;
        }

        let mut rate_limit_reason: Option<String> = None;
        match cmd {
            ExecCommand::Place { market, side, price_ticks, qty_lots, client_id } => {
                limiter.acquire().await;
                let ev = rest.place(&market, side, price_ticks, qty_lots, &client_id, false).await;
                if let Some(reason) = exec_event_rate_limit_reason(&ev) {
                    rate_limit_reason = Some(reason.to_string());
                }
                let _ = tx.send(ev).await;
            }
            ExecCommand::Cancel { client_id, market, .. } => {
                // Only ack a cancel that actually succeeded — a failed cancel must NOT close the
                // strategy's slot (the order may still be resting). Report the real outcome.
                limiter.acquire().await;
                let ev = match rest.cancel_order(&market, &client_id).await {
                    Ok(CancelOutcome::Canceled | CancelOutcome::AlreadyGone) => ExecEvent::CancelAck { client_id },
                    Ok(CancelOutcome::FilledOrExpired) => {
                        // A cancel returning FILLED/EXPIRED proves the order is not resting, but it
                        // may also mean a fill happened before the cancel. Do not silently resume
                        // quoting; freeze/sweep until the user stream or reconciler accounts for it.
                        ExecEvent::CancelReject {
                            client_id,
                            reason: "cancel returned FILLED/EXPIRED; waiting for fill/reconcile".into(),
                        }
                    }
                    Err(e) => {
                        warn!("aster cancel {client_id} failed: {e:#}");
                        ExecEvent::CancelReject { client_id, reason: e.to_string() }
                    }
                };
                if let Some(reason) = exec_event_rate_limit_reason(&ev) {
                    rate_limit_reason = Some(reason.to_string());
                }
                let _ = tx.send(ev).await;
            }
            ExecCommand::Replace { old_client_id, new_client_id, market, side, price_ticks, qty_lots, .. } => {
                // Safe path: cancel-then-place (atomic PUT modify is a [VERIFY] item). NEVER place
                // the new order unless the old cancel is VERIFIED — else both could rest at once.
                limiter.acquire().await;
                match rest.cancel_order(&market, &old_client_id).await {
                    Ok(CancelOutcome::Canceled) => {
                        let _ = tx.send(ExecEvent::CancelAck { client_id: old_client_id }).await;
                        limiter.acquire().await;
                        let ev = rest.place(&market, side, price_ticks, qty_lots, &new_client_id, false).await;
                        if let Some(reason) = exec_event_rate_limit_reason(&ev) {
                            rate_limit_reason = Some(reason.to_string());
                        }
                        let _ = tx.send(ev).await;
                    }
                    Ok(outcome) => {
                        warn!(
                            "aster replace: old {old_client_id} outcome {outcome:?}; NOT placing new {new_client_id}"
                        );
                        match outcome {
                            CancelOutcome::AlreadyGone => {
                                let _ = tx.send(ExecEvent::CancelAck { client_id: old_client_id }).await;
                            }
                            CancelOutcome::FilledOrExpired => {
                                let _ = tx
                                    .send(ExecEvent::CancelReject {
                                        client_id: old_client_id,
                                        reason: "replace cancel returned FILLED/EXPIRED; waiting for fill/reconcile".into(),
                                    })
                                    .await;
                            }
                            CancelOutcome::Canceled => unreachable!("handled above"),
                        }
                        let _ = tx
                            .send(ExecEvent::PlaceReject {
                                client_id: new_client_id,
                                reason: format!("replace skipped after old cancel outcome {outcome:?}"),
                            })
                            .await;
                    }
                    Err(e) => {
                        let reason = e.to_string();
                        if is_aster_rate_limit_reason(&reason) {
                            rate_limit_reason = Some(reason.clone());
                        }
                        warn!("aster replace: cancel {old_client_id} failed ({e:#}); NOT placing new order");
                        // Old order may still rest; keep the slot and let the strategy freeze/recover.
                        let _ = tx.send(ExecEvent::CancelReject { client_id: old_client_id, reason }).await;
                        let _ = tx
                            .send(ExecEvent::PlaceReject {
                                client_id: new_client_id,
                                reason: "replace skipped because old cancel failed".into(),
                            })
                            .await;
                    }
                }
            }
            ExecCommand::CancelMarket { market } => {
                if let Err(e) = rest.cancel_all_symbol(&market).await {
                    let reason = e.to_string();
                    if is_aster_rate_limit_reason(&reason) {
                        rate_limit_reason = Some(reason.clone());
                    }
                    warn!("aster cancelMarket failed: {e:#}");
                }
            }
            ExecCommand::CancelAllBot => {
                for market in rest.markets.keys().cloned().collect::<Vec<_>>() {
                    if let Err(e) = rest.cancel_all_symbol(&market).await {
                        let reason = e.to_string();
                        if is_aster_rate_limit_reason(&reason) {
                            rate_limit_reason = Some(reason.clone());
                            warn!("aster cancelAllBot hit rate limit on {market}: {e:#}");
                            break;
                        }
                        warn!("aster cancelAllBot failed on {market}: {e:#}");
                    }
                }
            }
            ExecCommand::FlattenAster { market, side, qty } => {
                let ev = match rest.flatten(&market, side, qty).await {
                    Ok(()) => {
                        info!("aster flatten sent: {side:?} {qty} {market}");
                        ExecEvent::AsterFlattenAck { market, side, qty }
                    }
                    Err(e) => {
                        warn!("aster flatten ({side:?} {qty} {market}) failed: {e:#}");
                        ExecEvent::AsterFlattenReject { market, side, qty, reason: e.to_string() }
                    }
                };
                if let Some(reason) = exec_event_rate_limit_reason(&ev) {
                    rate_limit_reason = Some(reason.to_string());
                }
                let _ = tx.send(ev).await;
            }
            ExecCommand::RefreshDeadman { market } => {
                if let Err(e) = rest.refresh_deadman(&market).await {
                    let reason = e.to_string();
                    if is_aster_rate_limit_reason(&reason) {
                        rate_limit_reason = Some(reason.clone());
                    }
                    warn!("aster deadman refresh failed: {e:#}");
                }
            }
            ExecCommand::Shutdown => unreachable!("handled before backoff gate"),
        }

        if let Some(reason) = rate_limit_reason {
            backoff_until = Some(tokio::time::Instant::now() + Duration::from_millis(rest.rate_limit_backoff_ms as u64));
            notify_rate_limited(&tx, reason, rest.rate_limit_backoff_ms).await;
        }
    }
    info!("aster live exec worker stopped");
}

/// Format a Decimal for the wire without scientific notation or trailing-zero noise.
fn trim_dec(d: Decimal) -> String {
    d.normalize().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::livebot::exec::sign::test_support::TestSigner;
    use crate::markets::MarketSpec;
    use rust_decimal_macros::dec;

    fn spec() -> MarketSpec {
        MarketSpec {
            market_id: "BTC".into(),
            aster_symbol: "BTCUSDT".into(),
            hl_coin: "BTC".into(),
            lighter_market_id: 1,
            lighter_price_decimals: 1,
            lighter_size_decimals: 3,
            lighter_price_tick: dec!(0.1),
            tick: dec!(0.1),
            step: dec!(0.001),
            aster_min_qty: dec!(0.001),
            aster_min_notional: dec!(5),
            hl_sz_decimals: 3,
            hl_qty_step: dec!(0.001),
            hl_min_notional: dec!(10),
        }
    }

    fn rest() -> AsterRest {
        let signer = Arc::new(TestSigner::new());
        let mut scales = HashMap::new();
        scales.insert("BTC".into(), (MarketScale::from_spec(&spec()), "BTCUSDT".to_string()));
        AsterRest::new("https://fapi.asterdex.com".into(), signer, scales, 5000, 10_000, 1_200, None).unwrap()
    }

    #[test]
    fn classify_cancel_distinguishes_success_from_venue_error() {
        // Ordinary cancels may ack every not-resting outcome, but replace-place only proceeds
        // after the stronger Canceled outcome.
        assert_eq!(
            classify_cancel(r#"{"orderId":1,"status":"CANCELED","clientOrderId":"X1"}"#).unwrap(),
            CancelOutcome::Canceled
        );
        assert_eq!(
            classify_cancel(r#"{"code":-2011,"msg":"Unknown order sent."}"#).unwrap(),
            CancelOutcome::AlreadyGone
        );
        assert_eq!(
            classify_cancel(r#"{"orderId":1,"status":"FILLED"}"#).unwrap(),
            CancelOutcome::FilledOrExpired
        );
        assert_eq!(
            classify_cancel(r#"{"orderId":1,"status":"EXPIRED"}"#).unwrap(),
            CancelOutcome::FilledOrExpired
        );
        // A real venue error (HTTP 200 body) must be a FAILURE so the worker emits CancelReject.
        assert!(classify_cancel(r#"{"code":-4000,"msg":"rate limited"}"#).is_err());
        assert!(classify_cancel(r#"{"status":"NEW"}"#).is_err()); // unexpected: cancel didn't take
        assert!(classify_cancel("not json").is_err());
    }

    #[test]
    fn place_params_are_post_only_gtx_one_way() {
        let r = rest();
        // 1000 ticks * 0.1 = 100.0 price; 5 lots * 0.001 = 0.005 qty.
        let p = r.place_params(&"BTC".into(), Side::Buy, 1000, 5, "Xs-BTC-B-0", false).unwrap();
        let map: HashMap<_, _> = p.iter().cloned().collect();
        assert_eq!(map["symbol"], "BTCUSDT");
        assert_eq!(map["side"], "BUY");
        assert_eq!(map["type"], "LIMIT");
        assert_eq!(map["timeInForce"], "GTX"); // post-only
        assert_eq!(map["price"], "100");
        assert_eq!(map["quantity"], "0.005");
        assert_eq!(map["newClientOrderId"], "Xs-BTC-B-0");
        assert_eq!(map["positionSide"], "BOTH"); // one-way mode
        assert!(!map.contains_key("reduceOnly"));
    }

    #[test]
    fn place_params_reduce_only_flag() {
        let r = rest();
        let p = r.place_params(&"BTC".into(), Side::Sell, 1000, 5, "Xs-BTC-S-0", true).unwrap();
        let map: HashMap<_, _> = p.iter().cloned().collect();
        assert_eq!(map["reduceOnly"], "true");
    }

    #[test]
    fn classify_place_ack_on_new() {
        let body = r#"{"orderId":2037568488,"symbol":"HYPEUSDT","status":"NEW","clientOrderId":"Xs-BTC-B-0","price":"100.0"}"#;
        match classify_place("Xs-BTC-B-0", body) {
            ExecEvent::PlaceAck { client_id, venue_order_id } => {
                assert_eq!(client_id, "Xs-BTC-B-0");
                assert_eq!(venue_order_id, "2037568488");
            }
            other => panic!("expected PlaceAck, got {other:?}"),
        }
    }

    #[test]
    fn classify_place_reject_on_expired_post_only() {
        let body = r#"{"orderId":1,"symbol":"HYPEUSDT","status":"EXPIRED","clientOrderId":"x"}"#;
        assert!(matches!(classify_place("x", body), ExecEvent::PlaceReject { .. }));
    }

    #[test]
    fn classify_place_reject_on_error_code() {
        let body = r#"{"code":-4164,"msg":"Order's notional must be no smaller than 5"}"#;
        match classify_place("x", body) {
            ExecEvent::PlaceReject { reason, .. } => assert!(reason.contains("-4164")),
            other => panic!("expected PlaceReject, got {other:?}"),
        }
    }

    #[test]
    fn classify_place_unknown_on_fill_status() {
        let body = r#"{"orderId":1,"symbol":"HYPEUSDT","status":"FILLED","clientOrderId":"x"}"#;
        match classify_place("x", body) {
            ExecEvent::PlaceUnknown { reason, .. } => assert!(reason.contains("fill status")),
            other => panic!("expected PlaceUnknown, got {other:?}"),
        }
    }

    #[test]
    fn classify_place_unknown_on_unparseable_success_body() {
        match classify_place("x", "not json") {
            ExecEvent::PlaceUnknown { reason, .. } => assert!(reason.contains("unparseable")),
            other => panic!("expected PlaceUnknown, got {other:?}"),
        }
    }

    #[test]
    fn classify_place_unknown_on_new_without_order_id() {
        let body = r#"{"status":"NEW","clientOrderId":"x"}"#;
        match classify_place("x", body) {
            ExecEvent::PlaceUnknown { reason, .. } => assert!(reason.contains("missing orderId")),
            other => panic!("expected PlaceUnknown, got {other:?}"),
        }
    }

    #[test]
    fn signed_request_builds_real_signature() {
        // Smoke: building the signed params for a place must not error at the signer.
        let r = rest();
        let p = r.place_params(&"BTC".into(), Side::Buy, 1000, 5, "Xs-BTC-B-0", false).unwrap();
        // We can't hit the network in a unit test, but we can confirm signing succeeds by
        // re-creating the json_str + nonce path the way signed_request does.
        let mut params = p;
        params.push(("recvWindow".into(), ASTER_RECV_WINDOW.into()));
        params.push(("timestamp".into(), "1700000000000".into()));
        let map: std::collections::BTreeMap<&str, &str> = params.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let json_str = serde_json::to_string(&map).unwrap();
        let sig = r.signer.sign_v3(&json_str, 1_700_000_000_000_000).unwrap();
        assert!(sig.0.starts_with("0x") && sig.0.len() == 132); // 0x + 130 hex (65 bytes)
    }
}
