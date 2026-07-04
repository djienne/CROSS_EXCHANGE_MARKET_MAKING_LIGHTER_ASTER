//! Lighter hedge worker. The module name is kept as `hyperliquid` to minimize churn in
//! the existing strategy/reconciler code, but all live hedge I/O here goes to Lighter.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::command::{ExecEvent, HedgeCommand};
use super::creds::LighterCreds;
use crate::book::OrderBook;
use crate::connectors::rest_book;
use crate::lighter::auth::generate_ws_auth_token;
use crate::lighter::messages::{
    AccountAllMsg, AccountAllPositionsMsg, BookUpdateContiguity, OrderBookMsg, PriceLevel,
    RemoteOrder, TradePayload,
    UserStatsMsg,
};
use crate::lighter::nonce::NonceManager;
use crate::lighter::rest::RestClient;
use crate::lighter::signer::{
    SignedTx, Signer, DEFAULT_IOC_EXPIRY, MARGIN_MODE_CROSS, NIL_TRIGGER_PRICE, ORDER_TYPE_LIMIT,
    ORDER_TYPE_MARKET, TIF_IMMEDIATE_OR_CANCEL,
};
use crate::lighter::tx_ws::TxWebSocket;
use crate::lighter::ws::{subscribe_loop, subscribe_loop_authed, SubscribeOptions};
use crate::livebot::ids::Cloid;
use crate::markets::MarketSpec;
use crate::types::{MarketId, Side, TxSendStatus};

#[derive(Clone, Debug)]
struct LighterMarketWire {
    market_index: i32,
    symbol: String,
    size_decimals: u32,
    price_decimals: u32,
}

#[derive(Debug, Clone)]
pub struct LighterOrderPlan {
    pub market_index: i32,
    pub client_order_index: i64,
    pub base_amount: i64,
    pub price: i32,
    pub order_expiry: i64,
    pub is_ask: bool,
    pub order_type: i32,
    pub time_in_force: i32,
    pub reduce_only: bool,
}

#[derive(Debug, Clone)]
struct LighterFill {
    qty: Decimal,
    px: Decimal,
    fee_usd: Decimal,
}

#[derive(Default)]
struct FillTracker {
    pending: Mutex<HashMap<i64, (u64, mpsc::UnboundedSender<TradePayload>)>>,
    next_token: std::sync::atomic::AtomicU64,
}

impl FillTracker {
    /// Register a fill listener for `client_order_index`. Returns a registration token:
    /// `unregister` removes the route only while the token still matches, so a stale or
    /// aborted waiter can never delete a NEWER order's fill route. A colliding register
    /// (same index while one is pending) should be unreachable now that recovery/flatten
    /// cloids are salted — if it ever happens it is logged at error and the newer order
    /// wins; the older waiter degrades to a fill-timeout (HedgeUnknown, fail-closed),
    /// never to fill mis-attribution.
    fn register(&self, client_order_index: i64) -> (u64, mpsc::UnboundedReceiver<TradePayload>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let token = self
            .next_token
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        let prev = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(client_order_index, (token, tx));
        if prev.is_some() {
            tracing::error!(
                "FillTracker register collision on client_order_index {client_order_index}; \
                 replacing the older listener (it will report Unknown)"
            );
        }
        (token, rx)
    }

    fn unregister(&self, client_order_index: i64, token: u64) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending
            .get(&client_order_index)
            .is_some_and(|(t, _)| *t == token)
        {
            pending.remove(&client_order_index);
        }
    }

    fn on_trade(&self, trade: TradePayload) {
        let ids = [trade.ask_client_id, trade.bid_client_id];
        let pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        for id in ids.into_iter().flatten() {
            if let Some((_, tx)) = pending.get(&id) {
                let _ = tx.send(trade.clone());
            }
        }
    }
}

fn lighter_trade_key(trade: &TradePayload) -> String {
    if let Some(id) = trade.trade_id {
        return format!("id:{id}");
    }
    format!(
        "fallback:{:?}",
        (
            trade.ask_id,
            trade.bid_id,
            trade.ask_client_id,
            trade.bid_client_id,
            trade.price.as_deref(),
            trade.size.as_deref(),
            trade.usd_amount.as_deref(),
            trade.timestamp,
            trade.transaction_time,
            &trade.maker_fee,
            &trade.taker_fee,
        )
    )
}

fn record_lighter_fill_trade(
    trade: &TradePayload,
    seen: &mut HashSet<String>,
    qty: &mut Decimal,
    notional: &mut Decimal,
    fee_usd: &mut Decimal,
) -> bool {
    if !seen.insert(lighter_trade_key(trade)) {
        return false;
    }
    let q = trade
        .size
        .as_deref()
        .and_then(|s| s.parse::<Decimal>().ok())
        .unwrap_or(Decimal::ZERO);
    let p = trade
        .price
        .as_deref()
        .and_then(|s| s.parse::<Decimal>().ok())
        .unwrap_or(Decimal::ZERO);
    if q > Decimal::ZERO && p > Decimal::ZERO {
        *qty += q;
        *notional += q * p;
        *fee_usd += trade_fee_usd(trade);
        return true;
    }
    false
}

/// Outcome of the SEND PHASE of a hedge/flatten IOC (see `send_hedge_tx`).
enum HedgeSendOutcome {
    /// The dispatch resolved at send time (build/sign failure, venue reject, not-sent,
    /// unknown). Emit this event; there is nothing to wait for.
    Terminal(ExecEvent),
    /// The venue accepted the tx: run the wait phase (fill aggregation) — safe to do OFF
    /// the worker loop, it touches neither the nonce manager nor the tx socket.
    AwaitFills {
        client_order_index: i64,
        token: u64,
        rx: mpsc::UnboundedReceiver<TradePayload>,
        requested_qty: Decimal,
    },
}

/// Cancellation-safe unregister for a spawned wait task: if the task is aborted (bounded
/// shutdown drain), the fill route is still removed. Token-scoped, so a double unregister
/// (guard + `wait_fill`'s own) is a harmless no-op and can never evict a newer route.
struct FillRouteGuard {
    fills: Arc<FillTracker>,
    client_order_index: i64,
    token: u64,
}

impl Drop for FillRouteGuard {
    fn drop(&mut self) {
        self.fills.unregister(self.client_order_index, self.token);
    }
}

/// Collect `account_all` fill events for one IOC dispatch until the venue-visible requested
/// quantity is reached (an IOC cannot overfill, so this is the fully-filled fast path — no
/// artificial grace delay) or `fill_timeout` expires. Partials that trickle in late are
/// still counted right up to the timeout, so a slow multi-trade IOC is never under-reported
/// as PartiallyFilled while the venue actually filled it.
///
/// Free function (not `&self`) so a spawned waiter task can run it without borrowing the
/// exchange; `token` scopes the unregister to THIS registration (see [`FillTracker`]).
async fn wait_fill(
    fills: &FillTracker,
    client_order_index: i64,
    token: u64,
    mut rx: mpsc::UnboundedReceiver<TradePayload>,
    requested_qty: Decimal,
    fill_timeout: Duration,
) -> Option<LighterFill> {
    let deadline = tokio::time::Instant::now() + fill_timeout;
    let mut seen = HashSet::new();
    let mut qty = Decimal::ZERO;
    let mut notional = Decimal::ZERO;
    let mut fee_usd = Decimal::ZERO;
    while !(requested_qty > Decimal::ZERO && qty >= requested_qty) {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(tr)) => {
                record_lighter_fill_trade(&tr, &mut seen, &mut qty, &mut notional, &mut fee_usd);
            }
            _ => break, // timeout, or the fill route was closed
        }
    }
    fills.unregister(client_order_index, token);
    if qty > Decimal::ZERO {
        Some(LighterFill {
            qty,
            px: notional / qty,
            fee_usd,
        })
    } else {
        None
    }
}

#[derive(Default)]
struct AccountFeedState {
    positions: Mutex<HashMap<u32, (Decimal, Decimal)>>,
    available_balance: Mutex<Option<Decimal>>,
    portfolio_value: Mutex<Option<Decimal>>,
    open_orders: Mutex<HashMap<u32, Vec<RemoteOrder>>>,
    positions_ready: AtomicBool,
    user_stats_ready: AtomicBool,
    open_orders_ready: AtomicBool,
    /// mono_now_ns of the last APPLIED message per feed. The reconciler uses these to
    /// decide whether the WS cache is fresh enough to stand in for a venue read — a
    /// "ready" flag alone says the feed produced data once, not that it is still alive.
    positions_updated_ns: std::sync::atomic::AtomicI64,
    stats_updated_ns: std::sync::atomic::AtomicI64,
    open_orders_updated_ns: std::sync::atomic::AtomicI64,
}

impl AccountFeedState {
    fn set_position(&self, market_id: u32, qty: Decimal, entry_px: Decimal) {
        self.positions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(market_id, (qty, entry_px));
    }

    fn mark_positions_ready(&self) {
        self.positions_ready.store(true, Ordering::Release);
    }

    fn stamp_positions_updated(&self) {
        self.positions_updated_ns
            .store(crate::hotpath::clock::mono_now_ns(), Ordering::Release);
    }

    fn positions_updated_ns(&self) -> i64 {
        self.positions_updated_ns.load(Ordering::Acquire)
    }

    fn stats_updated_ns(&self) -> i64 {
        self.stats_updated_ns.load(Ordering::Acquire)
    }

    fn open_orders_updated_ns(&self) -> i64 {
        self.open_orders_updated_ns.load(Ordering::Acquire)
    }

    fn position(&self, market_id: u32) -> Option<(Decimal, Decimal)> {
        if !self.positions_ready.load(Ordering::Acquire) {
            return None;
        }
        self.positions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&market_id)
            .copied()
    }

    fn all_positions(&self) -> Option<HashMap<u32, (Decimal, Decimal)>> {
        if !self.positions_ready.load(Ordering::Acquire) {
            return None;
        }
        Some(
            self.positions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        )
    }

    fn set_stats(&self, available: Option<Decimal>, portfolio: Option<Decimal>) {
        *self
            .available_balance
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = available;
        *self
            .portfolio_value
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = portfolio;
        self.stats_updated_ns
            .store(crate::hotpath::clock::mono_now_ns(), Ordering::Release);
        self.user_stats_ready.store(true, Ordering::Release);
    }

    fn stats(&self) -> (Option<Decimal>, Option<Decimal>) {
        (
            *self
                .available_balance
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
            *self
                .portfolio_value
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        )
    }

    fn stats_ready(&self) -> bool {
        self.user_stats_ready.load(Ordering::Acquire)
    }

    fn set_open_orders_for_markets(&self, known_markets: &[u32], orders: &serde_json::Value) {
        let order_obj = orders.as_object();
        let mut out = HashMap::new();
        for market_id in known_markets {
            let rows = order_obj
                .and_then(|obj| obj.get(&market_id.to_string()))
                .and_then(|v| v.as_array())
                .map(|rows| {
                    rows.iter()
                        .filter_map(|row| serde_json::from_value::<RemoteOrder>(row.clone()).ok())
                        .filter(RemoteOrder::is_live)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            out.insert(*market_id, rows);
        }
        *self.open_orders.lock().unwrap_or_else(|e| e.into_inner()) = out;
        self.open_orders_updated_ns
            .store(crate::hotpath::clock::mono_now_ns(), Ordering::Release);
        self.open_orders_ready.store(true, Ordering::Release);
    }

    fn open_orders(&self) -> Option<HashMap<u32, Vec<RemoteOrder>>> {
        if !self.open_orders_ready.load(Ordering::Acquire) {
            return None;
        }
        Some(
            self.open_orders
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
        )
    }

    fn open_orders_ready(&self) -> bool {
        self.open_orders_ready.load(Ordering::Acquire)
    }
}

#[derive(Default)]
struct BookFeedState {
    books: Mutex<HashMap<u32, LighterBook>>,
}

impl BookFeedState {
    fn apply(&self, market_id: u32, msg: &OrderBookMsg) -> bool {
        let mut books = self.books.lock().unwrap_or_else(|e| e.into_inner());
        let book = books.entry(market_id).or_default();
        book.apply(msg)
    }

    fn reset(&self, market_id: u32) {
        self.books
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&market_id);
    }

    fn order_book(&self, market_id: u32) -> Option<OrderBook> {
        self.books
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&market_id)
            .and_then(LighterBook::to_order_book)
    }
}

#[derive(Default)]
struct LighterBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    initialized: bool,
    updated_at: Option<DateTime<Utc>>,
    last_nonce: Option<i64>,
    last_offset: Option<u64>,
}

impl LighterBook {
    fn apply(&mut self, msg: &OrderBookMsg) -> bool {
        if !msg.is_snapshot() {
            if !self.initialized {
                // A delta before the subscribe snapshot must never seed the book.
                return false;
            }
            match msg.contiguity(self.last_nonce, self.last_offset) {
                BookUpdateContiguity::Apply => {}
                BookUpdateContiguity::SkipStale => return true, // duplicate/replay: keep book
                BookUpdateContiguity::Gap => return false,
            }
        }
        if msg.is_snapshot() || !self.initialized {
            self.bids.clear();
            self.asks.clear();
            self.initialized = true;
        }
        apply_levels(&mut self.bids, &msg.order_book.bids);
        apply_levels(&mut self.asks, &msg.order_book.asks);
        self.updated_at = Some(Utc::now());
        self.last_nonce = msg.order_book.nonce.or(self.last_nonce);
        self.last_offset = msg.effective_offset().or(self.last_offset);
        true
    }

    fn to_order_book(&self) -> Option<OrderBook> {
        if !self.initialized {
            return None;
        }
        let ts = self.updated_at?;
        Some(OrderBook::from_levels(
            self.bids.iter().rev().map(|(p, q)| (*p, *q)),
            self.asks.iter().map(|(p, q)| (*p, *q)),
            ts,
            ts,
        ))
    }
}

/// Lighter-backed hedge exchange. The public type name is intentionally kept as `HlExchange`
/// because many strategy/reconciler interfaces still use "HL" as shorthand for hedge leg.
#[derive(Clone)]
pub struct HlExchange {
    rest: RestClient,
    tx_ws: Arc<TxWebSocket>,
    signer: Arc<Signer>,
    nonce: Arc<NonceManager>,
    account_index: i64,
    api_key_index: i32,
    base_url: String,
    markets: HashMap<MarketId, LighterMarketWire>,
    symbol_to_market: HashMap<String, MarketId>,
    fills: Arc<FillTracker>,
    account_feed: Arc<AccountFeedState>,
    book_feed: Arc<BookFeedState>,
    ws_url: String,
    fill_timeout: Duration,
    /// Max age of a WS account-feed cache entry before the reconciler's reads fall back to
    /// REST (see clearinghouse_state). At/below the reconcile cadence so two consecutive
    /// snapshots can never both be built from the same stale cache read.
    ws_account_max_age: Duration,
}

impl HlExchange {
    pub async fn new_lighter(
        base_url: String,
        signers_dir: &Path,
        creds: LighterCreds,
        specs: &[MarketSpec],
        fill_timeout_ms: i64,
        ws_account_max_age_ms: i64,
    ) -> Result<Self> {
        let rest = RestClient::new(&base_url)?;
        let signer = Arc::new(Signer::load(
            signers_dir,
            &base_url,
            &creds.api_private_key,
            creds.api_key_index,
            creds.account_index,
        )?);
        signer
            .check_client(creds.api_key_index)
            .context("Lighter CheckClient")?;
        let nonce =
            Arc::new(NonceManager::init(&rest, creds.account_index, creds.api_key_index).await?);
        let ws_url = lighter_ws_url(&base_url);
        let tx_ws = Arc::new(TxWebSocket::new(&ws_url));
        tx_ws
            .connect()
            .await
            .with_context(|| format!("preconnect Lighter tx websocket {ws_url}"))?;

        let mut markets = HashMap::new();
        let mut symbol_to_market = HashMap::new();
        for s in specs {
            let wire = LighterMarketWire {
                market_index: s.lighter_market_id as i32,
                symbol: s.hl_coin.clone(),
                size_decimals: s.lighter_size_decimals,
                price_decimals: s.lighter_price_decimals,
            };
            symbol_to_market.insert(s.hl_coin.to_ascii_uppercase(), s.market_id.clone());
            markets.insert(s.market_id.clone(), wire);
        }
        Ok(HlExchange {
            rest,
            tx_ws,
            signer,
            nonce,
            account_index: creds.account_index,
            api_key_index: creds.api_key_index,
            base_url,
            markets,
            symbol_to_market,
            fills: Arc::new(FillTracker::default()),
            account_feed: Arc::new(AccountFeedState::default()),
            book_feed: Arc::new(BookFeedState::default()),
            ws_url,
            fill_timeout: Duration::from_millis(fill_timeout_ms.max(250) as u64),
            ws_account_max_age: Duration::from_millis(ws_account_max_age_ms.max(250) as u64),
        })
    }

    fn wire(&self, market: &MarketId) -> Result<&LighterMarketWire> {
        self.markets
            .get(market)
            .ok_or_else(|| anyhow!("no Lighter wire context for market {market}"))
    }

    fn known_lighter_markets(&self) -> Vec<u32> {
        self.markets
            .values()
            .map(|w| w.market_index as u32)
            .collect()
    }

    pub async fn wait_ready(&self, market: &MarketId, timeout: Duration) -> Result<()> {
        let wire = self.wire(market)?;
        let market_id = wire.market_index as u32;
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let book_ready = self.book_feed.order_book(market_id).is_some();
            let position_ready = self.account_feed.position(market_id).is_some();
            let stats_ready = self.account_feed.stats_ready();
            let orders_ready = self.account_feed.open_orders_ready();
            if book_ready && position_ready && stats_ready && orders_ready {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "Lighter websocket state not ready for {market}: book={} position={} user_stats={} all_orders={}",
                    book_ready,
                    position_ready,
                    stats_ready,
                    orders_ready
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub fn start_private_streams(
        &self,
        shutdown: CancellationToken,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        vec![
            self.spawn_account_all(shutdown.clone()),
            self.spawn_account_all_positions(shutdown.clone()),
            self.spawn_account_all_orders(shutdown.clone()),
            self.spawn_user_stats(shutdown.clone()),
            self.spawn_order_books(shutdown),
        ]
    }

    fn spawn_account_all(&self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        let signer = self.signer.clone();
        let api_key_index = self.api_key_index;
        let channel = format!("account_all/{}", self.account_index);
        let mut opts = SubscribeOptions::new("lighter-account-all", vec![channel.clone()]);
        opts.url = self.ws_url.clone();
        opts.data_timeout = None;
        opts.frame_timeout = 90.0;
        let fills = self.fills.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = subscribe_loop_authed(
                    opts,
                    move || auth_map(&signer, api_key_index, &channel),
                    move |data| {
                        if let Ok(msg) = serde_json::from_value::<AccountAllMsg>(data.clone()) {
                            for trades in msg.trades.values() {
                                for tr in trades {
                                    fills.on_trade(tr.clone());
                                }
                            }
                        }
                    },
                ) => {}
            }
        })
    }

    fn spawn_account_all_positions(
        &self,
        shutdown: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let channel = format!("account_all_positions/{}", self.account_index);
        let mut opts = SubscribeOptions::new("lighter-account-all-positions", vec![channel]);
        opts.url = self.ws_url.clone();
        opts.data_timeout = None;
        opts.frame_timeout = 90.0;
        let known_markets = self.known_lighter_markets();
        let state = self.account_feed.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = subscribe_loop(
                    opts,
                    None,
                    move |data| {
                        if let Ok(msg) = serde_json::from_value::<AccountAllPositionsMsg>(data.clone()) {
                            apply_account_all_positions(&state, &known_markets, &msg);
                        }
                    },
                    || {},
                ) => {}
            }
        })
    }

    fn spawn_account_all_orders(&self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        let signer = self.signer.clone();
        let api_key_index = self.api_key_index;
        let channel = format!("account_all_orders/{}", self.account_index);
        let auth_channel = channel.clone();
        let mut opts = SubscribeOptions::new("lighter-account-all-orders", vec![channel]);
        opts.url = self.ws_url.clone();
        opts.data_timeout = None;
        opts.frame_timeout = 90.0;
        let known_markets = self.known_lighter_markets();
        let state = self.account_feed.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = subscribe_loop_authed(
                    opts,
                    move || auth_map(&signer, api_key_index, &auth_channel),
                    move |data| {
                        let orders = data.get("orders").unwrap_or(&serde_json::Value::Null);
                        state.set_open_orders_for_markets(&known_markets, orders);
                    },
                ) => {}
            }
        })
    }

    fn spawn_user_stats(&self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        let signer = self.signer.clone();
        let api_key_index = self.api_key_index;
        let channel = format!("user_stats/{}", self.account_index);
        let mut opts = SubscribeOptions::new("lighter-user-stats", vec![channel.clone()]);
        opts.url = self.ws_url.clone();
        opts.data_timeout = None;
        opts.frame_timeout = 90.0;
        let state = self.account_feed.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = subscribe_loop_authed(
                    opts,
                    move || auth_map(&signer, api_key_index, &channel),
                    move |data| {
                        if let Ok(msg) = serde_json::from_value::<UserStatsMsg>(data.clone()) {
                            state.set_stats(
                                value_dec(msg.stats.available_balance.as_ref()),
                                value_dec(msg.stats.portfolio_value.as_ref()),
                            );
                        }
                    },
                ) => {}
            }
        })
    }

    fn spawn_order_books(&self, shutdown: CancellationToken) -> tokio::task::JoinHandle<()> {
        let specs = self
            .markets
            .values()
            .map(|w| (w.market_index as u32, w.symbol.clone()))
            .collect::<Vec<_>>();
        let ws_url = self.ws_url.clone();
        let books = self.book_feed.clone();
        tokio::spawn(async move {
            let mut handles = Vec::new();
            for (market_id, symbol) in specs {
                handles.push(spawn_order_book_stream(
                    ws_url.clone(),
                    market_id,
                    symbol,
                    books.clone(),
                    shutdown.clone(),
                ));
            }
            tokio::select! {
                _ = shutdown.cancelled() => {}
                _ = futures_util::future::join_all(handles) => {}
            }
        })
    }

    pub fn build_ioc_limit_plan(
        &self,
        market: &MarketId,
        side: Side,
        px: Decimal,
        sz: Decimal,
        client_order_index: i64,
        reduce_only: bool,
    ) -> Result<LighterOrderPlan> {
        let w = self.wire(market)?;
        Ok(LighterOrderPlan {
            market_index: w.market_index,
            client_order_index,
            base_amount: raw_amount(sz, w.size_decimals)?,
            price: raw_price(px, w.price_decimals, side)?,
            order_expiry: DEFAULT_IOC_EXPIRY,
            is_ask: matches!(side, Side::Sell),
            order_type: ORDER_TYPE_LIMIT,
            time_in_force: TIF_IMMEDIATE_OR_CANCEL,
            reduce_only,
        })
    }

    pub fn build_market_plan(
        &self,
        market: &MarketId,
        side: Side,
        px_bound: Decimal,
        sz: Decimal,
        client_order_index: i64,
        reduce_only: bool,
    ) -> Result<LighterOrderPlan> {
        let w = self.wire(market)?;
        Ok(LighterOrderPlan {
            market_index: w.market_index,
            client_order_index,
            base_amount: raw_amount(sz, w.size_decimals)?,
            // Native MARKET orders still require a positive marketable price bound.
            // A non-marketable bound can be accepted by sendtx without opening a position.
            price: raw_price(px_bound, w.price_decimals, side)?,
            order_expiry: DEFAULT_IOC_EXPIRY,
            is_ask: matches!(side, Side::Sell),
            order_type: ORDER_TYPE_MARKET,
            time_in_force: TIF_IMMEDIATE_OR_CANCEL,
            reduce_only,
        })
    }

    pub fn sign_order_plan(&self, plan: &LighterOrderPlan, nonce: i64) -> Result<SignedTx> {
        self.signer.sign_create_order(
            plan.market_index,
            plan.client_order_index,
            plan.base_amount,
            plan.price,
            plan.is_ask,
            plan.order_type,
            plan.time_in_force,
            plan.reduce_only,
            NIL_TRIGGER_PRICE,
            plan.order_expiry,
            nonce,
            self.api_key_index,
        )
    }

    async fn send_signed(&self, tx: SignedTx) -> crate::types::TxSendResult {
        self.tx_ws.send_batch(&[tx.tx_type], &[tx.tx_info]).await
    }

    /// SEND PHASE of one hedge/flatten IOC: build → register fill route → reserve nonce →
    /// sign → wire send → classify. Runs entirely inside the worker loop so everything
    /// that touches the NonceManager or the tx websocket stays strictly serialized —
    /// including the nonce repair on failure (`acknowledge_failure` / `rollback` /
    /// `hard_refresh`), which must observe the outcome before the next command's nonce is
    /// reserved. Only a wire-accepted order returns `AwaitFills`; the caller runs the wait
    /// phase (which touches neither nonce nor socket) off the worker's critical path.
    async fn send_hedge_tx(
        &self,
        market: &MarketId,
        side: Side,
        px: Decimal,
        sz: Decimal,
        cloid: Cloid,
        reduce_only: bool,
    ) -> HedgeSendOutcome {
        let client_order_index = cloid.to_lighter_client_order_index();
        let plan = match self.build_ioc_limit_plan(
            market,
            side,
            px,
            sz,
            client_order_index,
            reduce_only,
        ) {
            Ok(p) => p,
            Err(e) => {
                return HedgeSendOutcome::Terminal(ExecEvent::HedgeReject {
                    cloid,
                    reason: e.to_string(),
                })
            }
        };
        // The venue-visible quantity: raw_amount FLOORS sz to the size step, so comparing
        // fills against the raw `sz` would make a fully-filled order look partial and burn
        // the whole fill timeout on the happy path.
        let requested_qty = match self.wire(market) {
            Ok(w) => Decimal::new(plan.base_amount, w.size_decimals),
            Err(e) => {
                return HedgeSendOutcome::Terminal(ExecEvent::HedgeReject {
                    cloid,
                    reason: e.to_string(),
                })
            }
        };
        let (fill_token, fill_rx) = self.fills.register(client_order_index);
        let nonce = self.nonce.next();
        let sign_start_ns = crate::hotpath::clock::mono_now_ns();
        let signed = match self.sign_order_plan(&plan, nonce) {
            Ok(tx) => tx,
            Err(e) => {
                self.nonce.acknowledge_failure();
                self.fills.unregister(client_order_index, fill_token);
                return HedgeSendOutcome::Terminal(ExecEvent::HedgeReject {
                    cloid,
                    reason: e.to_string(),
                });
            }
        };
        let sign_ns = crate::hotpath::clock::mono_now_ns() - sign_start_ns;
        let send_start_ns = crate::hotpath::clock::mono_now_ns();
        let outcome = match self.send_signed(signed).await {
            r if r.status == TxSendStatus::Ok => HedgeSendOutcome::AwaitFills {
                client_order_index,
                token: fill_token,
                rx: fill_rx,
                requested_qty,
            },
            r if r.status == TxSendStatus::Rejected => {
                self.fills.unregister(client_order_index, fill_token);
                // A nonce-shaped reject means the local optimistic counter desynced from
                // the venue (the reject consumed nothing); without a refresh EVERY
                // subsequent hedge would reject the same way and the recovery path (same
                // nonce manager) could not execute either. Cold path — REST is fine.
                if r.message.to_ascii_lowercase().contains("nonce") {
                    warn!("Lighter nonce-shaped reject ({}); hard-refreshing nonce", r.message);
                    let _ = self.nonce.hard_refresh(&self.rest).await;
                }
                HedgeSendOutcome::Terminal(ExecEvent::HedgeReject {
                    cloid,
                    reason: format!("Lighter reject code={} {}", r.code, r.message),
                })
            }
            r if r.status == TxSendStatus::NotSent => {
                self.nonce.rollback(1);
                self.fills.unregister(client_order_index, fill_token);
                HedgeSendOutcome::Terminal(ExecEvent::HedgeReject {
                    cloid,
                    reason: format!("Lighter tx not sent: {}", r.message),
                })
            }
            r => {
                let _ = self.nonce.hard_refresh(&self.rest).await;
                self.fills.unregister(client_order_index, fill_token);
                HedgeSendOutcome::Terminal(ExecEvent::HedgeUnknown {
                    cloid,
                    reason: format!("Lighter tx outcome unknown: {}", r.message),
                })
            }
        };
        // Cold (post-wire): per-hedge sign + wire-accept cost. The FFI sign was the one
        // unmeasured term in the fill->hedge latency budget; hedges are sparse enough
        // that one line per dispatch is free.
        info!(
            "hedge timing: coi={} sign_us={} send_us={}",
            client_order_index,
            sign_ns / 1_000,
            (crate::hotpath::clock::mono_now_ns() - send_start_ns) / 1_000
        );
        outcome
    }

    pub(crate) async fn place_raw(
        &self,
        market: &MarketId,
        side: Side,
        px: Decimal,
        sz: Decimal,
        tif: &str,
        reduce_only: bool,
        cloid_hex: Option<String>,
    ) -> Result<String> {
        let client_order_index = cloid_hex
            .as_deref()
            .and_then(client_index_from_hex)
            .unwrap_or_else(|| random_client_order_index(market, side));
        let plan = if tif.eq_ignore_ascii_case("market") {
            self.build_market_plan(market, side, px, sz, client_order_index, reduce_only)?
        } else {
            self.build_ioc_limit_plan(market, side, px, sz, client_order_index, reduce_only)?
        };
        let nonce = self.nonce.next();
        let signed = self.sign_order_plan(&plan, nonce)?;
        let result = self.send_signed(signed).await;
        serde_json::to_string(&serde_json::json!({
            "status": format!("{:?}", result.status),
            "code": result.code,
            "message": result.message,
            "client_order_index": client_order_index,
            "quota_remaining": result.quota_remaining,
        }))
        .context("serialize Lighter tx result")
    }

    #[allow(dead_code)]
    pub(crate) async fn cancel_by_oid(&self, market: &MarketId, oid: u64) -> Result<String> {
        let w = self.wire(market)?;
        let nonce = self.nonce.next();
        let signed =
            self.signer
                .sign_cancel_order(w.market_index, oid as i64, nonce, self.api_key_index)?;
        let result = self.send_signed(signed).await;
        serde_json::to_string(&serde_json::json!({
            "status": format!("{:?}", result.status),
            "code": result.code,
            "message": result.message,
            "quota_remaining": result.quota_remaining,
        }))
        .context("serialize Lighter cancel result")
    }

    #[allow(dead_code)]
    pub(crate) async fn update_leverage(
        &self,
        market: &MarketId,
        leverage: u32,
        _is_cross: bool,
    ) -> Result<()> {
        let w = self.wire(market)?;
        let nonce = self.nonce.next();
        let signed = self.signer.sign_update_leverage(
            w.market_index,
            leverage as i32,
            MARGIN_MODE_CROSS,
            nonce,
            self.api_key_index,
        )?;
        // Lighter rejects update-leverage when it is sent through sendtxbatch
        // ("unsupported tx type: for batch operation"). Control-plane single txs
        // use REST sendTx, matching lighter_MM_RUST's startup/cancel-all path.
        let resp = self.rest.send_tx(signed.tx_type, &signed.tx_info).await?;
        let code = if resp.code == 200 { 0 } else { resp.code };
        if code == 0 {
            Ok(())
        } else {
            let _ = self.nonce.hard_refresh(&self.rest).await;
            bail!("Lighter update_leverage rejected: {}", resp.message)
        }
    }

    /// Read the current Lighter market leverage from the account payload.
    ///
    /// Lighter exposes `initial_margin_fraction` as a percentage. A 1x market shows
    /// `100.00`, 2x shows `50.00`, etc., so leverage is `100 / fraction`.
    pub(crate) async fn get_leverage(&self, market: &MarketId) -> Result<Decimal> {
        let w = self.wire(market)?;
        let raw = self.rest.account_raw(self.account_index).await?;
        lighter_leverage_from_account(&raw, w.market_index as u32, &w.symbol)
    }

    pub async fn clearinghouse_state(&self) -> Result<HlClearinghouse> {
        // Read-start stamp BEFORE any venue read: if REST is used, the data's origin can
        // be no later than this instant — exactly the semantics the reconciler's straddle
        // guard needs (see AccountSnapshot::read_start_ns).
        let rest_read_start_ns = crate::hotpath::clock::mono_now_ns();
        let max_age_ns = self.ws_account_max_age.as_nanos() as i64;
        let fresh = |updated_ns: i64| {
            updated_ns > 0 && rest_read_start_ns.saturating_sub(updated_ns) <= max_age_ns
        };
        let (available, portfolio) = self.account_feed.stats();
        let positions_snapshot = self.account_feed.all_positions();
        // The WS cache may serve a piece only while that feed is FRESH. A cache that was
        // populated once and then went quiet (stalled stream, degraded endpoint) must not
        // masquerade as a venue read — that is the double-hedge window: a missed hedge
        // fill plus a stale rep_h=0 convinces recover_orphans to hedge again.
        let use_cached_positions =
            positions_snapshot.is_some() && fresh(self.account_feed.positions_updated_ns());
        let use_cached_stats = available.is_some()
            && portfolio.is_some()
            && fresh(self.account_feed.stats_updated_ns());
        let needs_rest = !use_cached_positions || !use_cached_stats;
        let raw = if needs_rest {
            Some(self.rest.account_raw(self.account_index).await?)
        } else {
            None
        };
        let account = raw.as_ref().map(|raw| {
            raw.get("accounts")
                .and_then(|a| a.as_array())
                .and_then(|a| a.first())
                .unwrap_or(raw)
        });
        let fallback_portfolio = account.and_then(|account| {
            value_dec(account.get("portfolio_value"))
                .or_else(|| value_dec(account.get("account_value")))
                .or_else(|| value_dec(account.get("collateral")))
        });
        let fallback_available = account.and_then(|account| {
            value_dec(account.get("available_balance"))
                .or_else(|| value_dec(account.get("available")))
        });
        // Prefer the source we decided to trust: fresh cache, else the REST fallback (a
        // stale cache value is last resort only if REST omitted the field entirely).
        let (account_value, withdrawable) = if use_cached_stats {
            (
                portfolio.unwrap_or(Decimal::ZERO),
                available.unwrap_or(Decimal::ZERO),
            )
        } else {
            (
                fallback_portfolio.or(portfolio).unwrap_or(Decimal::ZERO),
                fallback_available.or(available).unwrap_or(Decimal::ZERO),
            )
        };

        let mut positions = Vec::new();
        if let Some(ws_positions) = positions_snapshot.filter(|_| use_cached_positions) {
            for (market_id, (qty, entry)) in ws_positions {
                if qty == Decimal::ZERO {
                    continue;
                }
                let symbol = self
                    .markets
                    .values()
                    .find(|w| w.market_index == market_id as i32)
                    .map(|w| w.symbol.clone())
                    .unwrap_or_else(|| market_id.to_string());
                positions.push(HlAssetPosition {
                    position: HlPosition {
                        coin: symbol,
                        szi: qty.normalize().to_string(),
                        entry_px: Some(entry.normalize().to_string()),
                    },
                });
            }
        } else if let Some(rows) =
            account.and_then(|account| account.get("positions").and_then(|p| p.as_array()))
        {
            for p in rows {
                let Some(market_id) = p
                    .get("market_id")
                    .and_then(|m| m.as_u64())
                    .map(|v| v as u32)
                else {
                    continue;
                };
                let qty = signed_position_from_json(p);
                if qty == Decimal::ZERO {
                    continue;
                }
                let entry = value_dec(p.get("avg_entry_price"))
                    .or_else(|| value_dec(p.get("entry_price")))
                    .unwrap_or(Decimal::ZERO);
                let symbol = self
                    .markets
                    .values()
                    .find(|w| w.market_index == market_id as i32)
                    .map(|w| w.symbol.clone())
                    .unwrap_or_else(|| market_id.to_string());
                positions.push(HlAssetPosition {
                    position: HlPosition {
                        coin: symbol,
                        szi: qty.normalize().to_string(),
                        entry_px: Some(entry.normalize().to_string()),
                    },
                });
            }
        }
        // The earliest origin of any data actually used: a fresh-cache piece originated at
        // its feed stamp; a REST piece originated no later than the read-start stamp.
        let positions_origin_ns = if use_cached_positions {
            self.account_feed.positions_updated_ns()
        } else {
            rest_read_start_ns
        };
        let stats_origin_ns = if use_cached_stats {
            self.account_feed.stats_updated_ns()
        } else {
            rest_read_start_ns
        };
        Ok(HlClearinghouse {
            margin_summary: HlMarginSummary {
                account_value: account_value.normalize().to_string(),
            },
            asset_positions: positions,
            withdrawable: withdrawable.normalize().to_string(),
            data_origin_ns: positions_origin_ns.min(stats_origin_ns),
        })
    }

    pub async fn open_orders_info(&self) -> Result<Vec<HlOpenOrder>> {
        let mut out = Vec::new();
        // Same freshness rule as clearinghouse_state: a quiet/stalled orders feed must
        // not stand in for a venue read (the sweep gate trusts this via read_start_ns).
        let now_ns = crate::hotpath::clock::mono_now_ns();
        let max_age_ns = self.ws_account_max_age.as_nanos() as i64;
        let orders_fresh = {
            let updated = self.account_feed.open_orders_updated_ns();
            updated > 0 && now_ns.saturating_sub(updated) <= max_age_ns
        };
        if let Some(cached) = self.account_feed.open_orders().filter(|_| orders_fresh) {
            for w in self.markets.values() {
                let rows = cached
                    .get(&(w.market_index as u32))
                    .cloned()
                    .unwrap_or_default();
                for o in rows.into_iter().filter(|o| o.is_live()) {
                    out.push(HlOpenOrder {
                        coin: w.symbol.clone(),
                        oid: o
                            .order_index
                            .or(o.client_order_index)
                            .unwrap_or_default()
                            .max(0) as u64,
                        side: if o.is_ask.unwrap_or(false) {
                            "A".into()
                        } else {
                            "B".into()
                        },
                        limit_px: o.price.unwrap_or_default(),
                        sz: o
                            .remaining_base_amount
                            .or(o.filled_base_amount)
                            .unwrap_or_default(),
                    });
                }
            }
            return Ok(out);
        }

        let auth = generate_ws_auth_token(&self.signer, self.api_key_index)?;
        for w in self.markets.values() {
            let rows = self
                .rest
                .account_active_orders(self.account_index, w.market_index as u32, &auth)
                .await
                .with_context(|| format!("accountActiveOrders {}", w.symbol))?;
            for o in rows.into_iter().filter(|o| o.is_live()) {
                out.push(HlOpenOrder {
                    coin: w.symbol.clone(),
                    oid: o
                        .order_index
                        .or(o.client_order_index)
                        .unwrap_or_default()
                        .max(0) as u64,
                    side: if o.is_ask.unwrap_or(false) {
                        "A".into()
                    } else {
                        "B".into()
                    },
                    limit_px: o.price.unwrap_or_default(),
                    sz: o
                        .remaining_base_amount
                        .or(o.filled_base_amount)
                        .unwrap_or_default(),
                });
            }
        }
        Ok(out)
    }

    /// Lighter mid for `market` from the live WS book cache, plus the age (ms) of the book
    /// data (wall-clock since the last applied order_book message). `None` when the cache has
    /// no initialized book (pre-warmup, or after a sequence-gap reset — the reset clears the
    /// book, so a dead stream reads as `None`, never as an old mid).
    pub fn cached_lighter_mid(&self, market: &MarketId) -> Option<(Decimal, i64)> {
        let w = self.wire(market).ok()?;
        let book = self.book_feed.order_book(w.market_index as u32)?;
        let mid = book.mid()?;
        Some((mid, book.age_ms(chrono::Utc::now())))
    }

    /// Lighter mid fetched directly over REST, bypassing the WS book cache. Used by the
    /// reconciler's uPnL marking when the cached book is stale — `mid()` would serve the
    /// stale cache first. Also the only path in the `status` command (no WS streams).
    pub async fn rest_mid(&self, market: &MarketId) -> Result<Decimal> {
        let w = self.wire(market)?;
        let client = rest_book::client()?;
        let book = rest_book::fetch_lighter_book_from_base(
            &client,
            &self.base_url,
            w.market_index as u32,
            20,
        )
        .await?;
        book.mid()
            .ok_or_else(|| anyhow!("no Lighter mid for {}", market.0))
    }

    pub async fn mid(&self, coin: &str) -> Result<Decimal> {
        let market = self
            .symbol_to_market
            .get(&coin.to_ascii_uppercase())
            .ok_or_else(|| anyhow!("no Lighter market configured for {coin}"))?;
        let w = self.wire(market)?;
        if let Some(mid) = self
            .book_feed
            .order_book(w.market_index as u32)
            .and_then(|book| book.mid())
        {
            return Ok(mid);
        }
        let client = rest_book::client()?;
        let book = rest_book::fetch_lighter_book_from_base(
            &client,
            &self.base_url,
            w.market_index as u32,
            20,
        )
        .await?;
        book.mid()
            .ok_or_else(|| anyhow!("no Lighter mid for {coin}"))
    }
}

#[derive(Debug, Clone)]
pub struct HlClearinghouse {
    pub margin_summary: HlMarginSummary,
    pub asset_positions: Vec<HlAssetPosition>,
    pub withdrawable: String,
    /// mono_now_ns no later than when the OLDEST piece of this state was read from the
    /// venue (WS feed stamp for fresh-cache pieces, REST read-start otherwise). The
    /// reconciler mins this into `AccountSnapshot::read_start_ns` so the orphan backstop's
    /// straddle guard sees the true data origin, not the snapshot assembly time.
    pub data_origin_ns: i64,
}
#[derive(Debug, Clone)]
pub struct HlMarginSummary {
    pub account_value: String,
}
#[derive(Debug, Clone)]
pub struct HlAssetPosition {
    pub position: HlPosition,
}
#[derive(Debug, Clone)]
pub struct HlPosition {
    pub coin: String,
    pub szi: String,
    pub entry_px: Option<String>,
}
#[derive(Debug, Clone)]
pub struct HlOpenOrder {
    pub coin: String,
    pub oid: u64,
    pub side: String,
    pub limit_px: String,
    pub sz: String,
}

pub async fn run_hl_worker(mut rx: Receiver<HedgeCommand>, tx: Sender<ExecEvent>, ex: HlExchange) {
    info!("lighter live hedge worker started (native signer + tx websocket)");
    // Fill-wait continuations run here, OFF the command loop: a hedge that was accepted on
    // the wire no longer blocks the next command for its fill_timeout, so a second maker
    // fill's hedge hits the wire immediately instead of queueing ~2s behind the first.
    // Everything nonce/socket-touching stays inside the loop (see send_hedge_tx).
    let mut waits = tokio::task::JoinSet::new();
    let fill_timeout = ex.fill_timeout;
    while let Some(cmd) = rx.recv().await {
        // Reap finished waiters without blocking so the set stays bounded.
        while waits.try_join_next().is_some() {}
        match cmd {
            HedgeCommand::Hedge {
                intent,
                aggressive_px,
                ..
            } => {
                let cloid = intent.cloid;
                match ex
                    .send_hedge_tx(
                        &intent.market,
                        intent.hedge_side,
                        aggressive_px,
                        intent.qty,
                        cloid,
                        false,
                    )
                    .await
                {
                    HedgeSendOutcome::Terminal(ev) => {
                        let _ = tx.send(ev).await;
                    }
                    HedgeSendOutcome::AwaitFills {
                        client_order_index,
                        token,
                        rx: fill_rx,
                        requested_qty,
                    } => {
                        let fills = ex.fills.clone();
                        let events = tx.clone();
                        waits.spawn(async move {
                            let _guard = FillRouteGuard {
                                fills: fills.clone(),
                                client_order_index,
                                token,
                            };
                            let ev = match wait_fill(
                                &fills,
                                client_order_index,
                                token,
                                fill_rx,
                                requested_qty,
                                fill_timeout,
                            )
                            .await
                            {
                                Some(fill) => ExecEvent::HedgeFill {
                                    cloid,
                                    filled_qty: fill.qty,
                                    px: fill.px,
                                    fee_usd: fill.fee_usd,
                                },
                                None => ExecEvent::HedgeUnknown {
                                    cloid,
                                    reason: format!(
                                        "Lighter accepted tx but no matching account_all fill within {fill_timeout:?}"
                                    ),
                                },
                            };
                            let _ = events.send(ev).await;
                        });
                    }
                }
            }
            HedgeCommand::Flatten {
                market,
                side,
                qty,
                aggressive_px,
                ..
            } => {
                // Flatten cloids are salted per dispatch: Lighter does NOT dedupe client
                // order indices, so a deterministic id would collide with an equal-sized
                // recovery hedge or an overlapping earlier flatten in the FillTracker.
                let cloid = Cloid::flatten(
                    &market,
                    crate::livebot::fills::cum_scaled(qty),
                    crate::hotpath::clock::mono_now_ns(),
                );
                // Reduce-only: a flatten must clamp at zero, never open the opposite
                // direction off a stale snapshot (see exec/command.rs contract).
                match ex
                    .send_hedge_tx(&market, side, aggressive_px, qty, cloid, true)
                    .await
                {
                    HedgeSendOutcome::Terminal(ev) => {
                        let reason = match ev {
                            ExecEvent::HedgeReject { reason, .. }
                            | ExecEvent::HedgeUnknown { reason, .. } => reason,
                            other => format!("unexpected flatten event {other:?}"),
                        };
                        let _ = tx
                            .send(ExecEvent::HlFlattenReject {
                                market,
                                side,
                                qty,
                                reason,
                            })
                            .await;
                    }
                    HedgeSendOutcome::AwaitFills {
                        client_order_index,
                        token,
                        rx: fill_rx,
                        requested_qty,
                    } => {
                        let fills = ex.fills.clone();
                        let events = tx.clone();
                        waits.spawn(async move {
                            let _guard = FillRouteGuard {
                                fills: fills.clone(),
                                client_order_index,
                                token,
                            };
                            let ev = match wait_fill(
                                &fills,
                                client_order_index,
                                token,
                                fill_rx,
                                requested_qty,
                                fill_timeout,
                            )
                            .await
                            {
                                Some(fill) => ExecEvent::HlFlattenFill {
                                    market,
                                    side,
                                    filled_qty: fill.qty,
                                    px: fill.px,
                                },
                                None => ExecEvent::HlFlattenReject {
                                    market,
                                    side,
                                    qty,
                                    reason: format!(
                                        "Lighter accepted flatten tx but no matching fill within {fill_timeout:?}"
                                    ),
                                },
                            };
                            let _ = events.send(ev).await;
                        });
                    }
                }
            }
            HedgeCommand::Shutdown => break,
        }
    }
    // Drain pending waits BOUNDED, then abort. Never abort first: the venue-side effect of
    // an accepted IOC already happened — the drain is what preserves intent resolution
    // (HedgeFill/HedgeUnknown) on a clean stop. run.rs awaits this worker, so the drain is
    // genuinely part of the shutdown sequence; the cap keeps a wedged wait from hanging it.
    let drain = async {
        while waits.join_next().await.is_some() {}
    };
    if tokio::time::timeout(fill_timeout + Duration::from_millis(500), drain)
        .await
        .is_err()
    {
        warn!(
            "hedge worker shutdown: aborting {} pending fill-wait(s) after drain timeout",
            waits.len()
        );
        waits.abort_all();
    }
    info!("lighter live hedge worker stopped");
}

fn apply_account_all_positions(
    state: &AccountFeedState,
    known_markets: &[u32],
    msg: &AccountAllPositionsMsg,
) {
    let mut seen = HashSet::new();
    for (market, position) in &msg.positions {
        if let Ok(market_id) = market.parse::<u32>() {
            seen.insert(market_id);
            state.set_position(
                market_id,
                signed_position_payload_dec(position),
                position_entry_px_dec(position),
            );
        }
    }

    if msg.is_snapshot() {
        for market_id in known_markets {
            if !seen.contains(market_id) {
                state.set_position(*market_id, Decimal::ZERO, Decimal::ZERO);
            }
        }
        state.mark_positions_ready();
    }
    // Every applied message (snapshot or delta) proves the feed is alive; the reconciler
    // uses this stamp to decide whether the cache can stand in for a venue read.
    state.stamp_positions_updated();
}

fn spawn_order_book_stream(
    ws_url: String,
    market_id: u32,
    symbol: String,
    books: Arc<BookFeedState>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    let channel = format!("order_book/{market_id}");
    let mut opts = SubscribeOptions::new(
        &format!("lighter-order-book-{symbol}-{market_id}"),
        vec![channel],
    );
    opts.url = ws_url;
    let books_for_msg = books.clone();
    let books_for_disconnect = books.clone();
    let reconnect = Arc::new(Notify::new());
    let reconnect_on_gap = reconnect.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = shutdown.cancelled() => {}
            _ = subscribe_loop(
                opts,
                Some(reconnect),
                move |data| {
                    if let Ok(msg) = serde_json::from_value::<OrderBookMsg>(data.clone()) {
                        if !books_for_msg.apply(market_id, &msg) {
                            tracing::warn!(
                                "Lighter order_book sequence gap for market {}; reconnecting for fresh snapshot",
                                market_id
                            );
                            books_for_msg.reset(market_id);
                            reconnect_on_gap.notify_one();
                        }
                    }
                },
                move || {
                    books_for_disconnect.reset(market_id);
                },
            ) => {}
        }
    })
}

fn auth_map(signer: &Signer, api_key_index: i32, channel: &str) -> HashMap<String, String> {
    generate_ws_auth_token(signer, api_key_index)
        .map(|token| HashMap::from([(channel.to_string(), token)]))
        .unwrap_or_default()
}

fn raw_amount(qty: Decimal, decimals: u32) -> Result<i64> {
    if qty <= Decimal::ZERO {
        bail!("quantity must be positive");
    }
    let scale = Decimal::from(10u64.pow(decimals));
    let raw = (qty * scale)
        .round_dp_with_strategy(0, RoundingStrategy::ToZero)
        .to_i64()
        .ok_or_else(|| anyhow!("quantity raw amount overflow"))?;
    if raw <= 0 {
        bail!("quantity rounds to zero at {decimals} decimals");
    }
    Ok(raw)
}

fn raw_price(px: Decimal, decimals: u32, side: Side) -> Result<i32> {
    if px <= Decimal::ZERO {
        bail!("price must be positive");
    }
    let scale = Decimal::from(10u64.pow(decimals));
    let strat = match side {
        Side::Buy => RoundingStrategy::ToPositiveInfinity,
        Side::Sell => RoundingStrategy::ToNegativeInfinity,
    };
    let raw = (px * scale)
        .round_dp_with_strategy(0, strat)
        .to_i32()
        .ok_or_else(|| anyhow!("price raw amount overflow"))?;
    if raw <= 0 {
        bail!("price rounds to zero at {decimals} decimals");
    }
    Ok(raw)
}

fn value_dec(v: Option<&serde_json::Value>) -> Option<Decimal> {
    match v? {
        serde_json::Value::String(s) => s.parse().ok(),
        serde_json::Value::Number(n) => n.to_string().parse().ok(),
        _ => None,
    }
}

fn apply_levels(side: &mut BTreeMap<Decimal, Decimal>, levels: &[PriceLevel]) {
    for level in levels {
        let Some(px) = level.price.parse::<Decimal>().ok() else {
            continue;
        };
        let Some(qty) = level.size.parse::<Decimal>().ok() else {
            continue;
        };
        if px <= Decimal::ZERO {
            continue;
        }
        if qty <= Decimal::ZERO {
            side.remove(&px);
        } else {
            side.insert(px, qty);
        }
    }
}

fn signed_position_payload_dec(p: &crate::lighter::messages::PositionPayload) -> Decimal {
    let mag = p
        .position
        .as_deref()
        .and_then(|s| s.parse::<Decimal>().ok())
        .unwrap_or(Decimal::ZERO)
        .abs();
    if p.sign.is_some_and(|sign| sign < 0) {
        -mag
    } else {
        mag
    }
}

fn position_entry_px_dec(p: &crate::lighter::messages::PositionPayload) -> Decimal {
    p.avg_entry_price
        .as_deref()
        .and_then(|s| s.parse::<Decimal>().ok())
        .unwrap_or(Decimal::ZERO)
}

fn lighter_ws_url(base_url: &str) -> String {
    let trimmed = base_url.trim_end_matches('/');
    if let Some(rest) = trimmed.strip_prefix("https://") {
        format!("wss://{rest}/stream")
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        format!("ws://{rest}/stream")
    } else if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        format!("{trimmed}/stream")
    } else {
        format!("wss://{trimmed}/stream")
    }
}

fn trade_fee_usd(tr: &TradePayload) -> Decimal {
    let raw = value_dec(tr.taker_fee.as_ref()).or_else(|| value_dec(tr.maker_fee.as_ref()));
    raw.map(|v| v / Decimal::from(1_000_000u64))
        .unwrap_or(Decimal::ZERO)
}

fn lighter_leverage_from_account(
    raw: &serde_json::Value,
    market_id: u32,
    symbol: &str,
) -> Result<Decimal> {
    let account = raw
        .get("accounts")
        .and_then(|a| a.as_array())
        .and_then(|a| a.first())
        .unwrap_or(raw);
    let rows = account
        .get("positions")
        .and_then(|p| p.as_array())
        .ok_or_else(|| anyhow!("Lighter account payload has no positions/leverage rows"))?;
    let row = rows
        .iter()
        .find(|p| p.get("market_id").and_then(|m| m.as_u64()) == Some(market_id as u64))
        .ok_or_else(|| anyhow!("Lighter account payload has no leverage row for {symbol}"))?;
    let margin_mode = row
        .get("margin_mode")
        .and_then(|m| m.as_i64())
        .unwrap_or(-1);
    if margin_mode != MARGIN_MODE_CROSS as i64 {
        bail!(
            "Lighter margin mode for {} is {} (expected cross margin mode {})",
            symbol,
            margin_mode,
            MARGIN_MODE_CROSS
        );
    }
    let fraction = value_dec(row.get("initial_margin_fraction")).ok_or_else(|| {
        anyhow!("Lighter leverage row for {symbol} has no initial_margin_fraction")
    })?;
    if fraction <= Decimal::ZERO {
        bail!("Lighter initial_margin_fraction for {symbol} is {fraction}");
    }
    Ok((Decimal::from(100u32) / fraction).normalize())
}

fn signed_position_from_json(v: &serde_json::Value) -> Decimal {
    let mag = value_dec(v.get("position"))
        .or_else(|| value_dec(v.get("size")))
        .unwrap_or(Decimal::ZERO)
        .abs();
    let sign = v.get("sign").and_then(|s| s.as_i64()).unwrap_or(1);
    if sign < 0 {
        -mag
    } else {
        mag
    }
}

fn client_index_from_hex(s: &str) -> Option<i64> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    if hex.len() != 32 {
        return None;
    }
    let bytes = hex::decode(hex).ok()?;
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&bytes);
    Some(Cloid::from_bytes_for_lighter(arr).to_lighter_client_order_index())
}

fn random_client_order_index(market: &MarketId, side: Side) -> i64 {
    let cloid = Cloid::recovery(
        market,
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default() ^ side as i64,
    );
    cloid.to_lighter_client_order_index()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn raw_amount_floors_size() {
        assert_eq!(raw_amount(dec!(1.239), 2).unwrap(), 123);
        assert!(raw_amount(dec!(0.0001), 2).is_err());
    }

    #[test]
    fn raw_price_rounds_toward_marketability() {
        assert_eq!(raw_price(dec!(123.451), 2, Side::Buy).unwrap(), 12346);
        assert_eq!(raw_price(dec!(123.459), 2, Side::Sell).unwrap(), 12345);
    }

    #[test]
    fn ioc_order_plan_conventions_match_lighter_signer() {
        let limit_ioc = LighterOrderPlan {
            market_index: 24,
            client_order_index: 1,
            base_amount: 1,
            price: 12345,
            order_expiry: 0,
            is_ask: false,
            order_type: ORDER_TYPE_LIMIT,
            time_in_force: TIF_IMMEDIATE_OR_CANCEL,
            reduce_only: false,
        };
        let market = LighterOrderPlan {
            order_type: ORDER_TYPE_MARKET,
            order_expiry: 0,
            ..limit_ioc.clone()
        };
        assert_eq!(limit_ioc.order_expiry, 0);
        assert!(market.price > 0);
        assert_eq!(market.order_expiry, 0);
    }

    #[test]
    fn signed_position_respects_lighter_sign() {
        let v = serde_json::json!({"position":"2.5","sign":-1});
        assert_eq!(signed_position_from_json(&v), dec!(-2.5));
    }

    #[test]
    fn trade_fee_usd_uses_lighter_callback_fee_when_present() {
        assert_eq!(trade_fee_usd(&TradePayload::default()), Decimal::ZERO);
        let tr = TradePayload {
            taker_fee: Some(serde_json::json!(12345)),
            ..TradePayload::default()
        };
        assert_eq!(trade_fee_usd(&tr), dec!(0.012345));
    }

    #[test]
    fn lighter_fill_trade_dedupes_replayed_messages() {
        let mut seen = HashSet::new();
        let mut qty = Decimal::ZERO;
        let mut notional = Decimal::ZERO;
        let mut fee_usd = Decimal::ZERO;
        let trade = TradePayload {
            trade_id: Some(42),
            bid_client_id: Some(77),
            price: Some("10".into()),
            size: Some("0.5".into()),
            taker_fee: Some(serde_json::json!(1000000)),
            ..TradePayload::default()
        };

        assert!(record_lighter_fill_trade(&trade, &mut seen, &mut qty, &mut notional, &mut fee_usd));
        assert!(!record_lighter_fill_trade(&trade, &mut seen, &mut qty, &mut notional, &mut fee_usd));
        assert_eq!(qty, dec!(0.5));
        assert_eq!(notional, dec!(5.0));
        assert_eq!(fee_usd, dec!(1));

        let fallback = TradePayload {
            bid_client_id: Some(77),
            price: Some("10".into()),
            size: Some("0.25".into()),
            timestamp: Some(123),
            ..TradePayload::default()
        };
        assert!(record_lighter_fill_trade(&fallback, &mut seen, &mut qty, &mut notional, &mut fee_usd));
        assert!(!record_lighter_fill_trade(&fallback, &mut seen, &mut qty, &mut notional, &mut fee_usd));
        assert_eq!(qty, dec!(0.75));
        assert_eq!(notional, dec!(7.50));
    }

    #[test]
    fn lighter_leverage_parses_initial_margin_fraction_percent() {
        let raw = serde_json::json!({
            "accounts": [{
                "positions": [
                    {"market_id": 1, "symbol": "BTC", "initial_margin_fraction": "50.00", "margin_mode": 0},
                    {"market_id": 24, "symbol": "HYPE", "initial_margin_fraction": "100.00", "margin_mode": 0}
                ]
            }]
        });
        assert_eq!(
            lighter_leverage_from_account(&raw, 24, "HYPE").unwrap(),
            dec!(1)
        );
        assert_eq!(
            lighter_leverage_from_account(&raw, 1, "BTC").unwrap(),
            dec!(2)
        );
    }

    #[test]
    fn lighter_leverage_fails_closed_on_missing_or_non_cross_market() {
        let missing = serde_json::json!({"accounts": [{"positions": []}]});
        assert!(lighter_leverage_from_account(&missing, 24, "HYPE").is_err());

        let isolated = serde_json::json!({
            "accounts": [{
                "positions": [
                    {"market_id": 24, "symbol": "HYPE", "initial_margin_fraction": "100.00", "margin_mode": 1}
                ]
            }]
        });
        assert!(lighter_leverage_from_account(&isolated, 24, "HYPE").is_err());
    }

    #[test]
    fn fill_tracker_routes_by_client_order_index() {
        let tracker = FillTracker::default();
        let (_token, mut rx) = tracker.register(77);
        tracker.on_trade(TradePayload {
            bid_client_id: Some(77),
            price: Some("100".into()),
            size: Some("0.1".into()),
            ..TradePayload::default()
        });
        assert_eq!(rx.try_recv().unwrap().bid_client_id, Some(77));
    }

    fn fill_trade(client_id: i64, trade_id: i64, px: &str, sz: &str) -> TradePayload {
        TradePayload {
            bid_client_id: Some(client_id),
            trade_id: Some(trade_id),
            price: Some(px.into()),
            size: Some(sz.into()),
            ..TradePayload::default()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn wait_fill_exits_immediately_on_full_fill() {
        let fills = FillTracker::default();
        let (token, rx) = fills.register(1);
        fills.on_trade(fill_trade(1, 1, "10", "0.5"));
        let started = tokio::time::Instant::now();
        let fill = wait_fill(&fills, 1, token, rx, dec!(0.5), Duration::from_secs(2))
            .await
            .expect("fully filled");
        assert_eq!(fill.qty, dec!(0.5));
        assert_eq!(
            tokio::time::Instant::now(),
            started,
            "fully-filled fast path must not wait out any grace/timeout"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn wait_fill_holds_full_timeout_on_partial() {
        let fills = FillTracker::default();
        let (token, rx) = fills.register(2);
        fills.on_trade(fill_trade(2, 1, "10", "0.2"));
        let started = tokio::time::Instant::now();
        let fill = wait_fill(&fills, 2, token, rx, dec!(0.5), Duration::from_millis(500))
            .await
            .expect("partial reported");
        assert_eq!(fill.qty, dec!(0.2));
        assert!(
            tokio::time::Instant::now() - started >= Duration::from_millis(500),
            "a partial must keep listening until the full fill timeout"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn wait_fill_counts_slow_partials_up_to_timeout() {
        // The second partial lands 400ms after the first — the old fixed 200ms grace window
        // dropped it, latching a spurious PartiallyFilled freeze.
        let fills = std::sync::Arc::new(FillTracker::default());
        let (token, rx) = fills.register(3);
        fills.on_trade(fill_trade(3, 1, "10", "0.2"));
        let late = fills.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            late.on_trade(fill_trade(3, 2, "10", "0.3"));
        });
        let fill = wait_fill(&fills, 3, token, rx, dec!(0.5), Duration::from_secs(2))
            .await
            .expect("filled across slow partials");
        assert_eq!(fill.qty, dec!(0.5), "late partial must be counted");
    }

    #[test]
    fn fill_tracker_token_scopes_unregister() {
        let tracker = FillTracker::default();
        let (stale_token, _stale_rx) = tracker.register(77);
        // A collision replaces the older listener (error-logged); the newer route must
        // survive a stale unregister from the older waiter.
        let (_fresh_token, mut fresh_rx) = tracker.register(77);
        tracker.unregister(77, stale_token);
        tracker.on_trade(TradePayload {
            bid_client_id: Some(77),
            price: Some("100".into()),
            size: Some("0.1".into()),
            ..TradePayload::default()
        });
        assert_eq!(
            fresh_rx.try_recv().unwrap().bid_client_id,
            Some(77),
            "stale unregister must not remove the newer registration"
        );
    }

    #[test]
    fn account_all_positions_sparse_update_does_not_zero_missing_markets() {
        let state = AccountFeedState::default();
        let snapshot: AccountAllPositionsMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/account_all_positions",
            "positions": {
                "24": {"position": "1.5", "sign": -1, "avg_entry_price": "2.25"}
            }
        }))
        .unwrap();
        apply_account_all_positions(&state, &[24, 25], &snapshot);
        assert_eq!(state.position(24), Some((dec!(-1.5), dec!(2.25))));
        assert_eq!(state.position(25), Some((Decimal::ZERO, Decimal::ZERO)));

        let sparse_update: AccountAllPositionsMsg = serde_json::from_value(serde_json::json!({
            "type": "update/account_all_positions",
            "positions": {}
        }))
        .unwrap();
        apply_account_all_positions(&state, &[24, 25], &sparse_update);
        assert_eq!(state.position(24), Some((dec!(-1.5), dec!(2.25))));
        assert_eq!(state.position(25), Some((Decimal::ZERO, Decimal::ZERO)));
    }

    #[test]
    fn account_feed_caches_live_open_orders_by_market() {
        let state = AccountFeedState::default();
        let orders = serde_json::json!({
            "24": [
                {"client_order_index": 1, "is_ask": true, "price": "101", "remaining_base_amount": "0.5", "status": "open"},
                {"client_order_index": 2, "status": "filled"}
            ],
            "25": []
        });
        state.set_open_orders_for_markets(&[24, 25, 26], &orders);
        let cached = state.open_orders().unwrap();
        assert_eq!(cached.get(&24).unwrap().len(), 1);
        assert_eq!(cached.get(&25).unwrap().len(), 0);
        assert_eq!(cached.get(&26).unwrap().len(), 0);
    }

    #[test]
    fn book_feed_detects_nonce_gap_and_keeps_top_of_book() {
        let feed = BookFeedState::default();
        let first: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "102", "size": "2"}]
            }
        }))
        .unwrap();
        assert!(feed.apply(24, &first));
        assert_eq!(feed.order_book(24).and_then(|b| b.mid()), Some(dec!(101)));

        // begin_nonce ahead of our position => missed updates => resync.
        let gap: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "begin_nonce": 11,
                "nonce": 12,
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        }))
        .unwrap();
        assert!(!feed.apply(24, &gap));
    }

    #[test]
    fn book_feed_skips_stale_replay_and_rejects_pre_snapshot_delta() {
        let feed = BookFeedState::default();
        // A delta with no snapshot to apply to must never seed the book.
        let premature: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "102", "size": "2"}]
            }
        }))
        .unwrap();
        assert!(!feed.apply(24, &premature));
        assert!(feed.order_book(24).is_none());

        let snapshot: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "nonce": 10,
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "102", "size": "2"}]
            }
        }))
        .unwrap();
        assert!(feed.apply(24, &snapshot));

        // A replay ending at-or-before our position is dropped without a resync.
        let stale: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "begin_nonce": 8,
                "nonce": 9,
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        }))
        .unwrap();
        assert!(feed.apply(24, &stale));
        assert_eq!(feed.order_book(24).and_then(|b| b.mid()), Some(dec!(101)));
    }

    #[test]
    fn book_feed_detects_offset_gap_without_nonce() {
        let feed = BookFeedState::default();
        let first: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/order_book",
            "offset": 10,
            "order_book": {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "102", "size": "2"}]
            }
        }))
        .unwrap();
        assert!(feed.apply(24, &first));

        let gap: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "update/order_book",
            "offset": 12,
            "order_book": {
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        }))
        .unwrap();
        assert!(!feed.apply(24, &gap));
    }

    #[test]
    fn book_feed_rejects_delta_without_sequence_metadata() {
        let feed = BookFeedState::default();
        let first: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "subscribed/order_book",
            "order_book": {
                "bids": [{"price": "100", "size": "1"}],
                "asks": [{"price": "102", "size": "2"}]
            }
        }))
        .unwrap();
        assert!(feed.apply(24, &first));

        let unsequenced: OrderBookMsg = serde_json::from_value(serde_json::json!({
            "type": "update/order_book",
            "order_book": {
                "bids": [{"price": "100", "size": "0"}],
                "asks": []
            }
        }))
        .unwrap();
        assert!(!feed.apply(24, &unsequenced));
    }

    #[test]
    fn lighter_ws_url_tracks_configured_base_url() {
        assert_eq!(
            lighter_ws_url("https://mainnet.zklighter.elliot.ai"),
            "wss://mainnet.zklighter.elliot.ai/stream"
        );
        assert_eq!(
            lighter_ws_url("http://localhost:8080/"),
            "ws://localhost:8080/stream"
        );
        assert_eq!(
            lighter_ws_url("wss://example.test/ws"),
            "wss://example.test/ws/stream"
        );
    }
}
