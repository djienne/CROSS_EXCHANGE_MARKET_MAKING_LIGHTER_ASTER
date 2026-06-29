//! The execution command/event contract (plan §1.1 / §5.4). The strategy and fill reactor
//! talk to the execution workers through **bounded command queues** and a shared **event
//! channel** — never by calling an `async` trait per book event. This keeps the hot path
//! single-owner and allocation-light: the strategy `try_send`s a small `Copy`-ish command
//! and moves on; the worker owns the venue client.
//!
//! Prices/quantities on the maker side are carried as scaled integers (`px_ticks`/
//! `qty_lots`); the worker converts to wire `Decimal` using its per-market
//! [`MarketScale`](crate::livebot::scale::MarketScale).

use rust_decimal::Decimal;

use crate::livebot::fills::{AsterFill, HedgeIntent};
use crate::livebot::ids::Cloid;
use crate::types::{MarketId, Side};

/// Strategy → Aster execution worker. Maker placement / cancel / replace / safety cancels +
/// the dead-man heartbeat.
#[derive(Debug, Clone)]
pub enum ExecCommand {
    /// Rest a post-only (GTX) maker order.
    Place {
        market: MarketId,
        side: Side,
        price_ticks: i64,
        qty_lots: i64,
        client_id: String,
    },
    /// Cancel a specific resting order.
    Cancel {
        market: MarketId,
        side: Side,
        client_id: String,
        venue_order_id: Option<String>,
    },
    /// Atomic replace (modify) when supported, else the worker does cancel+place.
    Replace {
        market: MarketId,
        side: Side,
        old_client_id: String,
        old_venue_order_id: Option<String>,
        new_client_id: String,
        price_ticks: i64,
        qty_lots: i64,
    },
    /// Cancel every resting order in one market (safety).
    CancelMarket { market: MarketId },
    /// Cancel every bot order across all markets (gate close / shutdown).
    CancelAllBot,
    /// Reduce-only taker (MARKET) order to FLATTEN an orphaned Aster position (recovery path):
    /// `side` closes the leg (SELL to close a long, BUY to close a short), `qty` base units.
    FlattenAster { market: MarketId, side: Side, qty: Decimal },
    /// Refresh the per-symbol dead-man countdown (plan §3.4).
    RefreshDeadman { market: MarketId },
    /// Drain and stop the worker.
    Shutdown,
}

/// Fill reactor / strategy → Hyperliquid hedge worker.
#[derive(Debug, Clone)]
pub enum HedgeCommand {
    /// Send an aggressive IOC hedge for this intent at `aggressive_px` with `slippage_bps`
    /// as the acceptable cap. `emergency` selects the wider second-attempt slippage ladder.
    Hedge {
        intent: HedgeIntent,
        aggressive_px: Decimal,
        slippage_bps: Decimal,
        emergency: bool,
    },
    /// Reduce-only IOC to flatten an HL position (orphan resolution): `side` closes the leg,
    /// `qty` base units. `aggressive_px` crosses the book; `slippage_bps` caps it.
    Flatten { market: MarketId, side: Side, qty: Decimal, aggressive_px: Decimal, slippage_bps: Decimal },
    Shutdown,
}

/// Worker / venue → strategy + risk reactor. Order/hedge lifecycle notifications. Aster
/// maker fills primarily arrive on the user-data stream (not the exec worker), but the
/// paper executor synthesizes them here, and the variant is shared so both paths converge.
#[derive(Debug, Clone)]
pub enum ExecEvent {
    PlaceAck { client_id: String, venue_order_id: String },
    PlaceReject { client_id: String, reason: String },
    /// Placement outcome is ambiguous: the request may have reached the venue, but
    /// the worker did not receive a definitive response. The strategy must freeze
    /// and sweep/reconcile; it must NOT close the local slot as if this were a reject.
    PlaceUnknown { client_id: String, reason: String },
    CancelAck { client_id: String },
    /// Aster REST quota / overload signal (HTTP 429 or venue code -1003). The strategy freezes
    /// maker quoting and backs off Aster command dispatch briefly.
    AsterRateLimited { reason: String, backoff_ms: i64 },
    /// A cancel/replace-cancel that FAILED at the venue (or returned a venue error body). The
    /// order may still be resting — the strategy must NOT close the slot; it freezes + reconciles.
    CancelReject { client_id: String, reason: String },
    /// A maker fill detected (from the Aster user stream, or synthesized in paper mode).
    MakerFill(AsterFill),
    HedgeAck { cloid: Cloid, hl_oid: String },
    HedgeFill { cloid: Cloid, filled_qty: Decimal, px: Decimal, fee_usd: Decimal },
    HedgeReject { cloid: Cloid, reason: String },
    /// Hedge outcome is ambiguous: the request may have reached Hyperliquid, but
    /// the worker did not receive a definitive response. The strategy must freeze
    /// and reconcile by deterministic cloid/position before any retry.
    HedgeUnknown { cloid: Cloid, reason: String },
    AsterFlattenAck { market: MarketId, side: Side, qty: Decimal },
    AsterFlattenReject { market: MarketId, side: Side, qty: Decimal, reason: String },
    HlFlattenFill { market: MarketId, side: Side, filled_qty: Decimal, px: Decimal },
    HlFlattenReject { market: MarketId, side: Side, qty: Decimal, reason: String },
}

/// Default bounded depth of each command queue. Deep enough to absorb a quoting burst, small
/// enough that a wedged worker is noticed (a `try_send` failure) rather than growing
/// unbounded — the live analogue of the recorder's backlog watch.
pub const CMD_QUEUE_DEPTH: usize = 1024;
