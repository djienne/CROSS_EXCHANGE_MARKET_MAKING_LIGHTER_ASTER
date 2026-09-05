//! Resolved Aster/Lighter specifications and independent market/queue/latency
//! simulation state. Recorded specifications make replay independent of the network.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::book::OrderBook;
use crate::inventory::PendingInventory;
use crate::position::SignedPosition;
use crate::requoter::LiveQuote;
use crate::types::{MarketId, QueueModel, Side};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSpec {
    pub market_id: MarketId,
    pub aster_symbol: String,
    pub hl_coin: String,
    #[serde(default)]
    pub lighter_market_id: u32,
    #[serde(default)]
    pub lighter_price_decimals: u32,
    #[serde(default)]
    pub lighter_size_decimals: u32,
    #[serde(default)]
    pub lighter_price_tick: Decimal,
    pub tick: Decimal,
    pub step: Decimal,
    pub aster_min_qty: Decimal,
    pub aster_min_notional: Decimal,
    pub hl_sz_decimals: i32,
    pub hl_qty_step: Decimal,
    pub hl_min_notional: Decimal,
}

/// One independent market/queue/latency simulation. Observations are shared;
/// hypothetical executions, reservations and ledgers belong only to this scenario.
#[derive(Debug)]
pub struct MarketState {
    pub spec: MarketSpec,
    pub queue_model: QueueModel,
    pub latency_bucket_ms: i64,
    pub aster_book: Option<Arc<OrderBook>>,
    pub hl_observation: Option<Arc<OrderBook>>,
    /// Remaining hypothetical executable depth, reset by a new observation.
    pub hl_execution_book: Option<OrderBook>,
    pub live_bid: Option<LiveQuote>,
    pub live_ask: Option<LiveQuote>,
    /// Replaced/cancelled quotes still fillable until their cancel takes effect.
    pub dying: Vec<LiveQuote>,
    pub pending_inv: Option<PendingInventory>,
    /// Per-side requote throttle stamps (bid and ask are throttled independently).
    pub last_requote_bid: Option<DateTime<Utc>>,
    pub last_requote_ask: Option<DateTime<Utc>>,
    /// Running signed futures position on each leg, used to enforce the capital cap.
    /// Only confirmed simulated executions change these positions.
    pub aster_pos: SignedPosition,
    pub hl_pos: SignedPosition,
    pub aster_realized_gross: Decimal,
    pub hl_realized_gross: Decimal,
    pub aster_fees: Decimal,
    pub hl_fees: Decimal,
    pub reserved_hl_buys: Decimal,
    pub reserved_hl_sells: Decimal,
    pub unpriced_hedge_qty: Decimal,
    pub frozen: Option<&'static str>,
    pub timer_at: Option<DateTime<Utc>>,
    pub timer_generation: u64,
    /// Peak |position| notional reached on each leg over the run (for reporting).
    pub max_abs_aster_notional: Decimal,
    pub max_abs_hl_notional: Decimal,
    /// Last reject reason persisted per side, to log opportunities on-change only.
    pub last_reject_bid: Option<crate::types::RejectReason>,
    pub last_reject_ask: Option<crate::types::RejectReason>,
}

impl MarketState {
    pub fn new(spec: MarketSpec, queue_model: QueueModel, latency_bucket_ms: i64) -> Self {
        MarketState {
            spec,
            queue_model,
            latency_bucket_ms,
            aster_book: None,
            hl_observation: None,
            hl_execution_book: None,
            live_bid: None,
            live_ask: None,
            dying: Vec::new(),
            pending_inv: None,
            last_requote_bid: None,
            last_requote_ask: None,
            aster_pos: SignedPosition::default(),
            hl_pos: SignedPosition::default(),
            aster_realized_gross: Decimal::ZERO,
            hl_realized_gross: Decimal::ZERO,
            aster_fees: Decimal::ZERO,
            hl_fees: Decimal::ZERO,
            reserved_hl_buys: Decimal::ZERO,
            reserved_hl_sells: Decimal::ZERO,
            unpriced_hedge_qty: Decimal::ZERO,
            frozen: None,
            timer_at: None,
            timer_generation: 0,
            max_abs_aster_notional: Decimal::ZERO,
            max_abs_hl_notional: Decimal::ZERO,
            last_reject_bid: None,
            last_reject_ask: None,
        }
    }

    pub fn hl_book(&self) -> Option<&OrderBook> {
        self.hl_observation.as_deref()
    }

    /// Per-side requote throttle stamp.
    pub fn last_requote(&self, side: Side) -> Option<DateTime<Utc>> {
        match side {
            Side::Buy => self.last_requote_bid,
            Side::Sell => self.last_requote_ask,
        }
    }

    pub fn set_last_requote(&mut self, side: Side, ts: DateTime<Utc>) {
        match side {
            Side::Buy => self.last_requote_bid = Some(ts),
            Side::Sell => self.last_requote_ask = Some(ts),
        }
    }
}
