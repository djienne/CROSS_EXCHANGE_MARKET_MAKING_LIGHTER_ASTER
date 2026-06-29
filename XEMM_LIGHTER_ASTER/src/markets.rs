//! Per-market resolved specification (tick/step/min from Aster `exchangeInfo`
//! and szDecimals from HL `meta`) and the live per-(market, queue-model) sim
//! state. `MarketSpec` is serialized into the run-log header so replay needs no
//! network. `MarketState` is populated by the sim engine (Phase 3).

use std::collections::VecDeque;

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

/// Live simulation state for one market under one queue model. Books are stored
/// per (market, model) state; the engine routes each book event to every model.
#[derive(Debug)]
pub struct MarketState {
    pub spec: MarketSpec,
    pub queue_model: QueueModel,
    pub aster_book: Option<OrderBook>,
    /// Recent HL books (bounded ring) used to resolve hedges at t+latency.
    pub hl_book_ring: VecDeque<OrderBook>,
    pub live_bid: Option<LiveQuote>,
    pub live_ask: Option<LiveQuote>,
    /// Replaced/cancelled quotes still fillable until their cancel takes effect.
    pub dying: Vec<LiveQuote>,
    pub pending_inv: Option<PendingInventory>,
    /// Per-side requote throttle stamps (bid and ask are throttled independently).
    pub last_requote_bid: Option<DateTime<Utc>>,
    pub last_requote_ask: Option<DateTime<Utc>>,
    /// Running signed futures position on each leg, used to enforce the capital cap.
    /// `aster_pos` = net of all Aster maker fills; `hl_pos` = net of all dispatched
    /// hedges (≈ −aster_pos, differing by the unhedged sub-min `pending_inv`).
    pub aster_pos: SignedPosition,
    pub hl_pos: SignedPosition,
    /// Peak |position| notional reached on each leg over the run (for reporting).
    pub max_abs_aster_notional: Decimal,
    pub max_abs_hl_notional: Decimal,
    /// Last reject reason persisted per side, to log opportunities on-change only.
    pub last_reject_bid: Option<crate::types::RejectReason>,
    pub last_reject_ask: Option<crate::types::RejectReason>,
}

impl MarketState {
    pub fn new(spec: MarketSpec, queue_model: QueueModel) -> Self {
        MarketState {
            spec,
            queue_model,
            aster_book: None,
            hl_book_ring: VecDeque::new(),
            live_bid: None,
            live_ask: None,
            dying: Vec::new(),
            pending_inv: None,
            last_requote_bid: None,
            last_requote_ask: None,
            aster_pos: SignedPosition::default(),
            hl_pos: SignedPosition::default(),
            max_abs_aster_notional: Decimal::ZERO,
            max_abs_hl_notional: Decimal::ZERO,
            last_reject_bid: None,
            last_reject_ask: None,
        }
    }

    pub fn hl_book(&self) -> Option<&OrderBook> {
        self.hl_book_ring.back()
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
