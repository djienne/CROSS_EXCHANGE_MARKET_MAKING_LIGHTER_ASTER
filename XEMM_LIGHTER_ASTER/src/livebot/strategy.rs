//! Strategy/order hot path (plan §1.1.B, §5). A single-owner loop reprices each market
//! side, places/cancels/replaces the Aster maker order, and reacts to fills. It REUSES the
//! deterministic, well-tested quote math (`quote_engine::compute_desired_quote` and
//! `resting_quote_net_edge_bps`) rather than re-deriving the edge stack in integer math —
//! the integer hot types accelerate the touch/crossed/staleness pre-checks and carry the
//! order representation, but money math stays exact (plan §5.3).
//!
//! This file holds the **pure decision table** ([`evaluate_side`]) — exhaustively testable —
//! and the async driver ([`run_strategy`]) that turns decisions into [`ExecCommand`]s and
//! folds fills into the hedge/risk state machine.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures_util::future::FutureExt;
use rust_decimal::Decimal;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use crate::book::{Level, OrderBook};
use crate::config::Config;
use crate::edge::EdgeConfig;
use crate::hot_types::HotBook;
use crate::hotpath::{VenueRegistry, VenueTag};
use crate::inventory::{self, HedgeabilityRules, PendingInventory};
use crate::markets::MarketSpec;
use crate::quote_engine::{
    compute_desired_quote_with_aster_touch_source, resting_quote_net_edge_bps, DesiredQuote,
    PositionContext, QuoteEngineConfig,
};
use crate::position::SignedPosition;
use crate::requoter::ReplaceReason;
use crate::types::{MarketId, RejectReason, Side};

use super::account::{AccountState, Venue};
use super::exec::command::{ExecCommand, ExecEvent, HedgeCommand};
use super::exec::paper::cap_aggressive_px;
use super::exec::ExecMode;
use super::fills::{AsterFill, FillDedup, HedgeIntent};
use super::ids::SessionId;
use super::journal::Journal;
use super::orders::{CancelAfterAckReason, CancelTarget, OrderLifecycle, OrderManager};
use super::risk::{evaluate_maker_gate, position_mismatch, CooldownScope, CooldownState, MakerGateInputs};
use super::precheck::{hot_precheck_side, HotPrecheck};
use super::scale::MarketScale;

const MAKER_GATE_FROZEN: &str = "FROZEN";
// Keep a small cushion of Aster command-queue slots for risk-reducing commands
// (targeted cancels, CancelAllBot, dead-man refresh). Optional quote churn must
// not be allowed to consume the entire bounded queue and then block a cancel.
const EXEC_CANCEL_RESERVE: usize = 64;
/// Circuit-breaker baseline = median of this many fresh marked equity samples (~10s at the
/// 2s reconcile cadence). A single-read baseline let one bad startup sample manufacture
/// phantom loss for a whole run (2026-07-04 incident).
const BREAKER_BASELINE_SAMPLES: usize = 5;
/// Consecutive fresh marked samples that must breach the loss limit before the breaker
/// trips (~4-6s at the 2s reconcile cadence). One anomalous snapshot must not halt the bot.
const BREAKER_TRIP_STREAK: u32 = 3;
const ASTER_CMD_WINDOW_NS: i64 = 60_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsterCommandPriority {
    /// Optional quote creation/churn. Must leave the configured safety reserve unused.
    Optional,
    /// Risk-reducing but targeted work, e.g. cancel a known slot or flatten an orphan.
    RiskReducing,
    /// Bulk safety operation such as CancelAllBot. Uses the reserved portion but still obeys the hard cap.
    Safety,
    /// Dead-man refresh. Important, but when the budget is exhausted the fail-safe is to let the
    /// venue countdown cancel orders rather than keep flooding refreshes into a 429 storm.
    Deadman,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecDispatch {
    Sent,
    QueueFull,
    QueueClosed,
    BudgetBlocked,
}

/// The minimal view of the current resting order the pure decision function needs.
#[derive(Debug, Clone, Copy)]
pub struct CurrentOrder {
    pub price: Decimal,
    pub qty: Decimal,
}

/// An Aster trade print forwarded to the strategy (paper maker-fill detection).
#[derive(Debug, Clone)]
pub struct TradePrint {
    pub market: MarketId,
    pub price: Decimal,
    pub qty: Decimal,
    /// Aster `aggTrade` `m`: true ⇒ the buyer was the maker (a SELL-aggressor print, which
    /// can fill our resting BID); false ⇒ buyer was the taker (can fill our resting ASK).
    pub buyer_is_maker: bool,
}

/// What to do with one market side this evaluation (plan §5.3 decision table).
#[derive(Debug, Clone)]
pub enum SideDecision {
    /// Leave the slot as is.
    Hold,
    /// Pull the resting order and place nothing (gate closed / cooldown / stale / no edge).
    Cancel { reason: ReplaceReason },
    /// Place a fresh quote (slot was empty).
    Place(Box<DesiredQuote>),
    /// Replace the resting quote (price moved / qty changed / no longer profitable).
    Replace { desired: Box<DesiredQuote>, reason: ReplaceReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HlQuoteSource {
    Bbo,
    L2,
}

impl HlQuoteSource {
    fn as_str(self) -> &'static str {
        match self {
            HlQuoteSource::Bbo => "bbo",
            HlQuoteSource::L2 => "l2",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsterQuoteSource {
    Bbo,
    L2,
}

impl AsterQuoteSource {
    fn as_str(self) -> &'static str {
        match self {
            AsterQuoteSource::Bbo => "bbo",
            AsterQuoteSource::L2 => "l2",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HlHedgePath {
    Decimal,
    Hot,
}

impl HlHedgePath {
    fn as_str(self, source: HlQuoteSource) -> &'static str {
        match (self, source) {
            (HlHedgePath::Decimal, HlQuoteSource::Bbo) => "decimal_bbo",
            (HlHedgePath::Decimal, HlQuoteSource::L2) => "decimal_l2",
            (HlHedgePath::Hot, HlQuoteSource::Bbo) => "hot_bbo",
            (HlHedgePath::Hot, HlQuoteSource::L2) => "hot_l2",
        }
    }
}

#[derive(Debug, Clone)]
struct SelectedHlBook {
    source: HlQuoteSource,
    path: HlHedgePath,
    book: Arc<OrderBook>,
    age_ms: i64,
    bbo_depth: Option<HlBboDepthSnapshot>,
}

#[derive(Debug, Clone)]
struct SelectedHlHotBook {
    source: HlQuoteSource,
    book: Arc<HotBook>,
    age_ms: i64,
    bbo_depth: Option<HlBboHotDepthSnapshot>,
}

#[derive(Debug, Clone)]
struct HlBboDepthSnapshot {
    top_qty: Option<Decimal>,
    required_qty: Decimal,
    multiple: Decimal,
    sufficient: bool,
}

#[derive(Debug, Clone)]
struct HlBboHotDepthSnapshot {
    top_lots: Option<i64>,
    required_lots: i64,
    multiple: Decimal,
    sufficient: bool,
}

#[derive(Debug, Clone)]
struct SelectedAsterTouch {
    source: AsterQuoteSource,
    book: Arc<OrderBook>,
    age_ms: i64,
}

#[derive(Debug, Clone)]
struct AsterFillTouchContext {
    source: AsterQuoteSource,
    book: Arc<OrderBook>,
    age_ms: i64,
    touch: Level,
    signed_distance_bps: Decimal,
    distance_bps: Decimal,
    quote_invalid_at_fill: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AsterTouchGuardStatus {
    Off,
    Active,
    Expired,
}

impl AsterTouchGuardStatus {
    fn as_str(self) -> &'static str {
        match self {
            AsterTouchGuardStatus::Off => "off",
            AsterTouchGuardStatus::Active => "ON",
            AsterTouchGuardStatus::Expired => "expired",
        }
    }
}

fn quote_cfg_for_touch_guard(
    quote: &QuoteEngineConfig,
    touch_guard_blocked: bool,
    current: Option<CurrentOrder>,
) -> QuoteEngineConfig {
    let mut q = quote.clone();
    if touch_guard_blocked
        && current.is_none()
        && quote.min_aster_touch_distance_bps > Decimal::ZERO
        && quote.min_aster_touch_hysteresis_bps > Decimal::ZERO
    {
        q.min_aster_touch_distance_bps = quote.aster_touch_rearm_distance_bps();
    }
    q
}

fn fresh_hot_book(book: &HotBook, now_ns: i64, max_stale_ns: i64) -> bool {
    now_ns.saturating_sub(book.recv_ns) <= max_stale_ns && !book.is_crossed()
}

#[inline]
fn executable_hot_book(book: &HotBook) -> bool {
    !book.is_crossed() && book.best_bid_ticks().is_some() && book.best_ask_ticks().is_some()
}

fn fresh_quote_book(book: &OrderBook, now: DateTime<Utc>, max_staleness_ms: i64) -> bool {
    book.age_ms(now) <= max_staleness_ms
        && !book.is_crossed()
        && book.best_bid().is_some()
        && book.best_ask().is_some()
}

#[inline]
fn executable_quote_book(book: &OrderBook) -> bool {
    !book.is_crossed() && book.best_bid().is_some() && book.best_ask().is_some()
}

#[inline]
fn hl_bbo_top_qty(book: &OrderBook, hedge_side: Side) -> Option<Decimal> {
    match hedge_side {
        Side::Buy => book.best_ask().map(|ask| ask.qty),
        Side::Sell => book.best_bid().map(|bid| bid.qty),
    }
}

#[inline]
fn hl_bbo_top_lots(book: &HotBook, hedge_side: Side) -> Option<i64> {
    match hedge_side {
        Side::Buy => book.asks().first().map(|ask| ask.qty_lots),
        Side::Sell => book.bids().first().map(|bid| bid.qty_lots),
    }
}

fn hl_bbo_depth_snapshot(
    book: &OrderBook,
    hedge_side: Side,
    hedge_qty: Decimal,
    multiple: Decimal,
) -> HlBboDepthSnapshot {
    let multiple = multiple.max(Decimal::ONE);
    let required_qty = hedge_qty * multiple;
    let top_qty = hl_bbo_top_qty(book, hedge_side);
    let sufficient = top_qty.is_some_and(|qty| qty >= required_qty);
    HlBboDepthSnapshot { top_qty, required_qty, multiple, sufficient }
}

fn hl_bbo_hot_depth_snapshot(
    scale: &MarketScale,
    book: &HotBook,
    hedge_side: Side,
    hedge_qty: Decimal,
    multiple: Decimal,
) -> HlBboHotDepthSnapshot {
    let multiple = multiple.max(Decimal::ONE);
    let required_lots = scale.hl_qty_to_lots_ceil(hedge_qty * multiple);
    let top_lots = hl_bbo_top_lots(book, hedge_side);
    let sufficient = required_lots > 0 && top_lots.is_some_and(|qty| qty >= required_lots);
    HlBboHotDepthSnapshot { top_lots, required_lots, multiple, sufficient }
}

#[inline]
fn hl_bbo_depth_sufficient(
    book: &OrderBook,
    hedge_side: Side,
    hedge_qty: Decimal,
    multiple: Decimal,
) -> bool {
    hl_bbo_depth_snapshot(book, hedge_side, hedge_qty, multiple).sufficient
}

#[inline]
fn bbo_not_older_than_l2(bbo: &OrderBook, l2: Option<&OrderBook>) -> bool {
    l2.is_none_or(|l2| bbo.exch_ts >= l2.exch_ts)
}

#[inline]
fn hot_bbo_not_older_than_l2(bbo: &HotBook, l2: Option<&HotBook>) -> bool {
    l2.is_none_or(|l2| bbo.exch_ms >= l2.exch_ms)
}

fn select_aster_hot_for_precheck<'a>(
    l2: Option<&'a HotBook>,
    bbo: Option<&'a HotBook>,
    now_ns: i64,
    max_stale_ns: i64,
) -> Option<&'a HotBook> {
    if let Some(bbo) = bbo
        .filter(|b| fresh_hot_book(b, now_ns, max_stale_ns))
        .filter(|b| hot_bbo_not_older_than_l2(b, l2))
    {
        return Some(bbo);
    }
    l2
}

fn select_hl_hot_for_precheck<'a>(
    l2: Option<&'a HotBook>,
    bbo: Option<&'a HotBook>,
    now_ns: i64,
    max_stale_ns: i64,
) -> Option<&'a HotBook> {
    if let Some(bbo) = bbo
        .filter(|b| fresh_hot_book(b, now_ns, max_stale_ns))
        .filter(|b| hot_bbo_not_older_than_l2(b, l2))
    {
        return Some(bbo);
    }
    l2
}

fn select_aster_touch_book<'a>(
    l2: &'a OrderBook,
    bbo: Option<&'a OrderBook>,
    now: DateTime<Utc>,
    max_staleness_ms: i64,
) -> Result<(&'a OrderBook, AsterQuoteSource), RejectReason> {
    if let Some(bbo) = bbo
        .filter(|b| fresh_quote_book(b, now, max_staleness_ms))
        .filter(|b| bbo_not_older_than_l2(b, Some(l2)))
    {
        return Ok((bbo, AsterQuoteSource::Bbo));
    }
    if fresh_quote_book(l2, now, max_staleness_ms) {
        return Ok((l2, AsterQuoteSource::L2));
    }
    if l2.best_bid().is_none() || l2.best_ask().is_none() {
        return Err(RejectReason::MissingAsterBook);
    }
    if l2.is_crossed() {
        return Err(RejectReason::BookCrossed);
    }
    Err(RejectReason::AsterBookStale)
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn compute_desired_quote_select_hl<'a>(
    edge: &EdgeConfig,
    quote: &QuoteEngineConfig,
    aster_book: &'a OrderBook,
    hl_l2_book: Option<&'a OrderBook>,
    hl_bbo_book: Option<&'a OrderBook>,
    side: Side,
    spec: &MarketSpec,
    max_staleness_ms: i64,
    now: DateTime<Utc>,
    pos: &PositionContext,
) -> Result<(DesiredQuote, &'a OrderBook, HlQuoteSource), RejectReason> {
    let (desired, hl_book, hl_source, _aster_source) = compute_desired_quote_select_books(
        edge,
        quote,
        aster_book,
        None,
        hl_l2_book,
        hl_bbo_book,
        side,
        spec,
        max_staleness_ms,
        now,
        pos,
    )?;
    Ok((desired, hl_book, hl_source))
}

#[allow(clippy::too_many_arguments)]
fn compute_desired_quote_select_books<'a>(
    edge: &EdgeConfig,
    quote: &QuoteEngineConfig,
    aster_depth_book: &'a OrderBook,
    aster_bbo_book: Option<&'a OrderBook>,
    hl_l2_book: Option<&'a OrderBook>,
    hl_bbo_book: Option<&'a OrderBook>,
    side: Side,
    spec: &MarketSpec,
    max_staleness_ms: i64,
    now: DateTime<Utc>,
    pos: &PositionContext,
) -> Result<(DesiredQuote, &'a OrderBook, HlQuoteSource, AsterQuoteSource), RejectReason> {
    let (aster_touch_book, aster_source) = select_aster_touch_book(aster_depth_book, aster_bbo_book, now, max_staleness_ms)?;
    let fresh_bbo = hl_bbo_book.filter(|b| {
        b.age_ms(now) <= max_staleness_ms
            && !b.is_crossed()
            && b.best_bid().is_some()
            && b.best_ask().is_some()
            && bbo_not_older_than_l2(b, hl_l2_book)
    });
    if let Some(bbo) = fresh_bbo {
        match compute_desired_quote_with_aster_touch_source(
            edge,
            quote,
            aster_depth_book,
            aster_touch_book,
            matches!(aster_source, AsterQuoteSource::Bbo),
            bbo,
            side,
            spec.tick,
            spec.step,
            spec.aster_min_qty,
            spec.aster_min_notional,
            spec.hl_min_notional,
            max_staleness_ms,
            now,
            pos,
        ) {
            Ok(desired) => {
                if hl_bbo_depth_sufficient(
                    bbo,
                    desired.hedge_side,
                    desired.qty,
                    quote.depth_liquidity_multiple,
                ) {
                    return Ok((desired, bbo, HlQuoteSource::Bbo, aster_source));
                }
                let Some(l2) = hl_l2_book else {
                    return Err(RejectReason::HlBboThinAndL2Stale);
                };
                if l2.age_ms(now) > max_staleness_ms || l2.is_crossed() {
                    return Err(RejectReason::HlBboThinAndL2Stale);
                }
            }
            Err(RejectReason::HlHedgeVwapUnavailable) => {
                let Some(l2) = hl_l2_book else {
                    return Err(RejectReason::HlBboThinAndL2Stale);
                };
                if l2.age_ms(now) > max_staleness_ms || l2.is_crossed() {
                    return Err(RejectReason::HlBboThinAndL2Stale);
                }
            }
            Err(reason) => return Err(reason),
        }
    }

    let l2 = hl_l2_book.ok_or(RejectReason::MissingHlBook)?;
    let desired = compute_desired_quote_with_aster_touch_source(
        edge,
        quote,
        aster_depth_book,
        aster_touch_book,
        matches!(aster_source, AsterQuoteSource::Bbo),
        l2,
        side,
        spec.tick,
        spec.step,
        spec.aster_min_qty,
        spec.aster_min_notional,
        spec.hl_min_notional,
        max_staleness_ms,
        now,
        pos,
    )?;
    Ok((desired, l2, HlQuoteSource::L2, aster_source))
}

/// Pure decision for one market side. `may_quote` folds the feed gate + risk freeze +
/// cooldown (false ⇒ we may only cancel). `current` is the resting order, if any.
/// Aggressive IOC hedge price that CROSSES the executable HL touch: a buy hedge crosses the best
/// ask (+slippage), a sell hedge crosses the best bid (−slippage). Pricing off the touch (NOT mid,
/// NOT the Aster fill price) guarantees the IOC takes liquidity unless the touch moved more than
/// `slip_bps` since the snapshot — far more robust than `mid ± slip` on a sparse book (the live ETH
/// failure: HL `l2Book` ≈0.46 updates/s, so mid was 1–3 s stale and mid±10 bps did not cross).
/// `None` when the relevant book side is empty — the caller must NOT hedge off a fallback price.
fn crossing_hedge_px(book: &OrderBook, hedge_side: Side, slip_bps: Decimal) -> Option<Decimal> {
    let f = slip_bps / Decimal::from(10_000);
    match hedge_side {
        Side::Buy => book.best_ask().map(|ask| ask.px * (Decimal::ONE + f)),
        Side::Sell => book.best_bid().map(|bid| bid.px * (Decimal::ONE - f)),
    }
}

#[inline]
fn level_px(level: Option<Level>) -> Option<String> {
    level.map(|l| l.px.to_string())
}

#[inline]
fn level_qty(level: Option<Level>) -> Option<String> {
    level.map(|l| l.qty.to_string())
}

#[inline]
fn decimal_qty(qty: Option<Decimal>) -> Option<String> {
    qty.map(|q| q.to_string())
}

fn aster_fill_touch_context(
    selected: SelectedAsterTouch,
    side: Side,
    fill_px: Decimal,
    min_touch_distance_bps: Decimal,
) -> Option<AsterFillTouchContext> {
    if fill_px <= Decimal::ZERO {
        return None;
    }
    let touch = match side {
        Side::Buy => selected.book.best_bid(),
        Side::Sell => selected.book.best_ask(),
    }?;
    let signed_gap = match side {
        Side::Buy => touch.px - fill_px,
        Side::Sell => fill_px - touch.px,
    };
    let signed_distance_bps = signed_gap / fill_px * Decimal::from(10_000);
    let distance_bps = signed_distance_bps.max(Decimal::ZERO);
    let quote_invalid_at_fill =
        signed_gap <= Decimal::ZERO || distance_bps < min_touch_distance_bps;
    Some(AsterFillTouchContext {
        source: selected.source,
        book: selected.book,
        age_ms: selected.age_ms,
        touch,
        signed_distance_bps,
        distance_bps,
        quote_invalid_at_fill,
    })
}

fn hl_hedge_context_json(
    selected: &SelectedHlBook,
    aggressive_px: Decimal,
    slippage_bps: Decimal,
) -> serde_json::Value {
    let bid = selected.book.best_bid();
    let ask = selected.book.best_ask();
    let bbo_depth = selected.bbo_depth.as_ref();
    serde_json::json!({
        "source": selected.source.as_str(),
        "source_path": selected.path.as_str(selected.source),
        "age_ms": selected.age_ms,
        "exch_ts_ms": selected.book.exch_ts.timestamp_millis(),
        "bid_px": level_px(bid),
        "bid_qty": level_qty(bid),
        "ask_px": level_px(ask),
        "ask_qty": level_qty(ask),
        "bbo_top_qty": decimal_qty(bbo_depth.and_then(|d| d.top_qty)),
        "bbo_required_qty": bbo_depth.map(|d| d.required_qty.to_string()),
        "bbo_depth_multiple": bbo_depth.map(|d| d.multiple.to_string()),
        "bbo_depth_sufficient": bbo_depth.map(|d| d.sufficient),
        "aggressive_px": aggressive_px.to_string(),
        "slippage_bps": slippage_bps.to_string(),
    })
}

fn aster_fill_touch_context_json(ctx: Option<&AsterFillTouchContext>) -> serde_json::Value {
    match ctx {
        Some(ctx) => {
            let bid = ctx.book.best_bid();
            let ask = ctx.book.best_ask();
            serde_json::json!({
                "source": ctx.source.as_str(),
                "age_ms": ctx.age_ms,
                "exch_ts_ms": ctx.book.exch_ts.timestamp_millis(),
                "bid_px": level_px(bid),
                "bid_qty": level_qty(bid),
                "ask_px": level_px(ask),
                "ask_qty": level_qty(ask),
                "touch_px": ctx.touch.px.to_string(),
                "touch_qty": ctx.touch.qty.to_string(),
                "signed_touch_distance_bps": ctx.signed_distance_bps.to_string(),
                "touch_distance_bps": ctx.distance_bps.to_string(),
                "quote_invalid_at_fill": ctx.quote_invalid_at_fill,
            })
        }
        None => serde_json::json!({
            "source": serde_json::Value::Null,
            "available": false,
            "quote_invalid_at_fill": true,
        }),
    }
}

/// Effective Aster position-notional cap for ONE market under the live margin guard.
///
/// `cap_base_usd` is the conservative real-collateral measure (the min of wallet balance and
/// mark-to-market equity, so unrealized losses on the Aster leg tighten it). The live cap is
/// `(cap_base - buffer).max(0) * leverage`, minus the notional already consumed by OTHER markets'
/// Aster positions (collateral is account-wide, but the cap is enforced per market). The static
/// config cap still wins when it is the smaller of the two. Never returns a negative cap.
fn effective_aster_cap_notional(
    static_cap: Decimal,
    cap_base_usd: Decimal,
    buffer_usd: Decimal,
    leverage: Decimal,
    other_markets_notional: Decimal,
) -> Decimal {
    let usable = (cap_base_usd - buffer_usd).max(Decimal::ZERO) * leverage;
    let for_this_market = (usable - other_markets_notional).max(Decimal::ZERO);
    static_cap.min(for_this_market)
}

#[allow(clippy::too_many_arguments)]
pub fn evaluate_side(
    edge: &EdgeConfig,
    quote: &QuoteEngineConfig,
    aster_book: &OrderBook,
    hl_book: &OrderBook,
    side: Side,
    spec: &MarketSpec,
    max_staleness_ms: i64,
    now: DateTime<Utc>,
    pos: &PositionContext,
    may_quote: bool,
    current: Option<CurrentOrder>,
    replace_immediately_if_unprofitable: bool,
) -> SideDecision {
    evaluate_side_with_hl_sources(
        edge,
        quote,
        aster_book,
        None,
        Some(hl_book),
        None,
        side,
        spec,
        max_staleness_ms,
        now,
        pos,
        may_quote,
        current,
        replace_immediately_if_unprofitable,
    )
    .0
}

/// Returns the side decision plus, when the quote engine rejected the candidate, the
/// reject reason — so callers (the empty-side touch-hysteresis latch) never need to
/// re-run the engine just to learn WHY a Hold happened.
#[allow(clippy::too_many_arguments)]
fn evaluate_side_with_hl_sources(
    edge: &EdgeConfig,
    quote: &QuoteEngineConfig,
    aster_book: &OrderBook,
    aster_bbo_book: Option<&OrderBook>,
    hl_l2_book: Option<&OrderBook>,
    hl_bbo_book: Option<&OrderBook>,
    side: Side,
    spec: &MarketSpec,
    max_staleness_ms: i64,
    now: DateTime<Utc>,
    pos: &PositionContext,
    may_quote: bool,
    current: Option<CurrentOrder>,
    replace_immediately_if_unprofitable: bool,
) -> (SideDecision, Option<RejectReason>) {
    // Gate closed / cooldown / risk freeze: cancel anything resting, place nothing.
    if !may_quote {
        return (
            match current {
                Some(_) => SideDecision::Cancel { reason: ReplaceReason::FeedStale },
                None => SideDecision::Hold,
            },
            None,
        );
    }

    let desired = compute_desired_quote_select_books(
        edge,
        quote,
        aster_book,
        aster_bbo_book,
        hl_l2_book,
        hl_bbo_book,
        side,
        spec,
        max_staleness_ms,
        now,
        pos,
    );

    let desired = match desired {
        Ok(d) => d,
        Err(reason) => {
            // No acceptable quote right now. Pull a resting order with an honest reason
            // (stale feed vs no-longer-profitable), consistent with the simulator.
            return (
                match current {
                    Some(_) => SideDecision::Cancel { reason: ReplaceReason::from_reject(reason) },
                    None => SideDecision::Hold,
                },
                Some(reason),
            );
        }
    };
    let (desired, selected_hl_book, _hl_source, _aster_source) = desired;

    let decision = match current {
        None => SideDecision::Place(Box::new(desired)),
        Some(cur) => {
            // Urgent safety check FIRST: a resting order that no longer clears the minimum edge
            // must be pulled/replaced even when the desired price moved by less than the churn
            // deadband. The deadband is only allowed to hold quotes that are still profitable.
            if replace_immediately_if_unprofitable {
                let still_ok = resting_quote_net_edge_bps(
                    edge,
                    selected_hl_book,
                    side,
                    cur.price,
                    cur.qty,
                    desired.ref_px,
                    quote.depth_liquidity_multiple,
                )
                    .is_some_and(|e| e >= edge.min_net_profit_bps);
                if !still_ok {
                    return (
                        SideDecision::Replace {
                            desired: Box::new(desired),
                            reason: ReplaceReason::NoLongerProfitable,
                        },
                        None,
                    );
                }
            }
            // Per-side requote DEADBAND (plan: don't churn on sub-bps moves): if the new desired
            // price is within `min_requote_bps` of the current resting price, leave the quote in
            // place only after the profitability recheck above passed. A genuine qty change still
            // requotes.
            let move_bps = if cur.price > Decimal::ZERO {
                (cur.price - desired.price).abs() / cur.price * Decimal::from(10_000)
            } else {
                Decimal::from(10_000)
            };
            if move_bps < quote.min_requote_bps && cur.qty == desired.qty {
                return (SideDecision::Hold, None);
            }
            // Non-urgent: replace only if price moved past the tick threshold or qty changed.
            let threshold = Decimal::from(quote.price_change_ticks_to_requote) * spec.tick;
            if (cur.price - desired.price).abs() >= threshold {
                SideDecision::Replace { desired: Box::new(desired), reason: ReplaceReason::PriceChanged }
            } else if cur.qty != desired.qty {
                SideDecision::Replace { desired: Box::new(desired), reason: ReplaceReason::QuantityChanged }
            } else {
                SideDecision::Hold
            }
        }
    };
    (decision, None)
}

/// Per-market immutable context resolved at startup.
struct MarketCtx {
    spec: Arc<MarketSpec>,
    scale: MarketScale,
    /// false ⇒ not eligible for live trading under the partial policy (never quoted).
    eligible: bool,
}

/// Per-market generation tracking for the generation-gated reprice (Phase 3).
struct GenSlot {
    last_aster_gen: u64,
    last_hl_gen: u64,
}

#[derive(Debug, Clone, Copy)]
struct SweepState {
    requested_ns: i64,
    last_attempt_ns: i64,
    reason: &'static str,
}

/// All the wiring the strategy loop owns (single-thread; no locks).
pub struct Strategy {
    cfg: Config,
    markets: Vec<MarketId>,
    ctx: HashMap<MarketId, MarketCtx>,
    registry: Arc<VenueRegistry>,
    account: AccountState,
    journal: Journal,
    orders: OrderManager,
    cooldown: CooldownState,
    dedup: FillDedup,
    /// In-flight hedge obligations keyed by cloid hex.
    hedges: HashMap<String, HedgeIntent>,
    /// Predicted signed positions per market on each leg (for the cap + mismatch checks).
    aster_pos: HashMap<MarketId, SignedPosition>,
    hl_pos: HashMap<MarketId, SignedPosition>,
    /// Sub-min UNHEDGED Aster inventory per market: partial fills accumulate here and hedge on
    /// HL the moment the net clears the HL minimum (the primary fast-hedge path — never a
    /// per-partial taker flatten). A residual that genuinely lingers is flattened in `on_tick`.
    pending: HashMap<MarketId, PendingInventory>,
    exec_tx: Sender<ExecCommand>,
    hedge_tx: Sender<HedgeCommand>,
    cooldown_ns: i64,
    exec_mode: ExecMode,
    /// Synthetic trade-id counter for paper maker fills (no venue trade id).
    synthetic_trade_seq: u64,
    /// Startup reconciliation done (invariant 7) — no quoting before this.
    clean_start: bool,
    /// Latched freeze after an orphan-leg danger (hedge reject / timeout); cleared only by
    /// an operator-driven reconcile (out of scope for this build — stays frozen, safe).
    frozen: bool,
    /// A safety cancel-all is pending; local maker slots are not trusted until a fresh
    /// account snapshot proves no bot-owned Aster orders remain.
    sweep_pending: Option<SweepState>,
    /// Rolling one-minute budget of Aster REST commands successfully enqueued by the strategy.
    /// A cancel+place replace counts as two because the worker performs two REST writes.
    aster_cmd_times_ns: VecDeque<i64>,
    /// Local freeze/backoff deadline after an Aster HTTP 429 / code -1003 notification.
    aster_rate_limited_until_ns: i64,
    /// Count of Aster REST rate-limit notifications observed in this strategy process.
    aster_429_count: u64,
    /// Per-market throttle for the orphan/pending flatten + recovery: `(dispatch_ns, snapshot
    /// source_ts_ns at dispatch, hedge_side)`. A recovery re-fires only when BOTH the wall-clock
    /// cooldown has elapsed AND a STRICTLY NEWER snapshot has arrived (so the prior action has had
    /// a chance to land in ground truth) — edge-triggered on fresh state, not just wall-clock.
    /// The side is used by the anti-flip guard: if recovery would reverse direction within 3×
    /// cooldown, it is suppressed (prevents round-trip thrashing on transient snapshot glitches).
    last_recovery: HashMap<MarketId, (i64, i64, Option<Side>)>,
    /// Per-market monotonic salt for recovery cloids. Lighter does NOT dedupe client order
    /// indices, so every recovery dispatch must get a fresh cloid (attempt 0 = the base
    /// `Cloid::recovery` id, then `-a{n}` salts) — reusing one against a possibly-live
    /// earlier order would cross-attribute its fills in the FillTracker.
    recovery_attempt_seq: HashMap<MarketId, u32>,
    /// Persistence gate for the orphan backstop: `(signed_orphan_net, snapshot source_ts_ns)` of
    /// the FIRST snapshot a net delta was seen. The backstop only ACTS when the SAME orphan (same
    /// sign, comparable size) is still present in a STRICTLY NEWER snapshot — so a transient
    /// snapshot lag (a primary hedge that resolved but hasn't appeared in the reported snapshot
    /// yet) is filtered out instead of triggering a redundant recovery hedge. This keeps recovery
    /// EXCEPTIONAL (real persistent orphans only), so fast fills are hedged by the primary path.
    orphan_seen: HashMap<MarketId, (Decimal, i64)>,
    /// Monotonic ns of the most recent hot action (maker fill processed / primary hedge dispatched)
    /// per market. The orphan backstop ignores any snapshot whose READS BEGAN before this — such a
    /// snapshot cannot yet reflect the action, so acting on it could double-hedge (the fast-network
    /// straddle race). Self-clocking: no fixed timing constant.
    last_hot_action_ns: HashMap<MarketId, i64>,
    /// Self-heal persistence: snapshot `source_ts_ns` at which the freeze-clear condition (no
    /// outstanding hedges + positions reconciled + stream fresh) was FIRST observed. The freeze
    /// clears only once that condition holds again in a STRICTLY NEWER snapshot, so a transient
    /// snapshot lag can't unfreeze on a phantom-clean reading. Mirrors the `orphan_seen` gate.
    heal_confirm: Option<i64>,
    /// Aster user-stream liveness (live only): gates quoting on stream freshness (§6). `None`
    /// in paper (no real stream).
    aster_stream: Option<Arc<super::userstream::StreamLiveness>>,
    /// Cumulative-loss circuit breaker (live only). Cloned shutdown token used to halt the whole
    /// process when the breaker trips; set via [`Strategy::arm_circuit_breaker`].
    shutdown: tokio_util::sync::CancellationToken,
    /// Where to write the persistent trip latch on a trip (set with the shutdown token).
    trip_file_path: Option<std::path::PathBuf>,
    /// Total cross-venue equity baseline: the MEDIAN of the first
    /// [`BREAKER_BASELINE_SAMPLES`] fresh marked snapshots (a single-read baseline let one
    /// bad startup sample manufacture phantom loss for the whole run — 2026-07-04 incident).
    breaker_baseline_equity: Option<Decimal>,
    /// Fresh marked equity samples collected while arming the baseline.
    breaker_baseline_samples: Vec<Decimal>,
    /// Consecutive fresh marked samples breaching the limit; trips at
    /// [`BREAKER_TRIP_STREAK`]. Reset by any non-breaching, stale, or unmarked sample.
    breaker_breach_streak: u32,
    /// Snapshot generation of the last sample the breaker processed — each published
    /// snapshot is counted at most once (the tick and publish cadences would otherwise let
    /// one bad snapshot be seen several times in a row).
    breaker_last_generation: u64,
    /// Latched once the breaker fires (prevents re-tripping / duplicate latch writes).
    breaker_tripped: bool,
    /// Per-market maker-gate suppression tracking, for OBSERVABILITY: `(since_ns, reason, logged)`.
    /// A closed maker gate (orphan hedge / unhedged-over-limit / stale snapshot / stale feed / …)
    /// otherwise suppresses quoting with NO log and no `frozen` latch — the exact failure mode that
    /// left a live bot dead for hours with zero signal. We record when the gate first closed; once
    /// the closure PERSISTS past a short grace (so normal post-fill cooldowns / transient feed blips
    /// don't spam), `logged` flips true and we emit a WARN + journal entry naming the reason. When the
    /// gate reopens we log a RESUMED line with the duration. Pure observability — does not gate.
    quote_suppressed: HashMap<MarketId, (i64, &'static str, bool)>,
    /// Per-(market, side) margin-reject suppression timestamp (monotonic ns).
    /// Set when a PlaceReject contains "insufficient" (Aster -2019).
    /// Cleared on: fill on that market, or 10s cooldown expiry.
    margin_suppressed: HashMap<(MarketId, Side), i64>,
    /// Per-side Aster touch hysteresis latch. Set after a side is rejected/cancelled
    /// for `QUOTE_TOO_CLOSE_TO_TOUCH`; while the slot is empty, placement requires
    /// `min_aster_touch_distance_bps + min_aster_touch_hysteresis_bps` clearance.
    aster_touch_guard_blocked: HashMap<(MarketId, Side), i64>,
    /// Shared dirty-market bitset — `None` in tests without dirty wiring.
    dirty: Option<Arc<crate::hotpath::dirty::DirtyMarkets>>,
    /// Per-market last-seen generation for each venue — skip reprice when unchanged.
    gen_slots: Vec<GenSlot>,
    /// Phase 4 hot-integer precheck config (built from Config at startup).
    precheck_cfg: super::precheck::HotPrecheckConfig,
    /// Per-market HL mid mark cache, refreshed once per wake/tick batch to avoid O(N²)
    /// book loads in `positions_reconciled` (called per-market inside `reprice_market`).
    mark_cache: HashMap<MarketId, Decimal>,
}

impl Strategy {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: Config,
        specs: &[MarketSpec],
        eligibility: &HashMap<MarketId, bool>,
        registry: Arc<VenueRegistry>,
        account: AccountState,
        journal: Journal,
        session: SessionId,
        exec_tx: Sender<ExecCommand>,
        hedge_tx: Sender<HedgeCommand>,
        exec_mode: ExecMode,
    ) -> Self {
        let markets: Vec<MarketId> = specs.iter().map(|s| s.market_id.clone()).collect();
        let ctx: HashMap<MarketId, MarketCtx> = specs
            .iter()
            .map(|s| {
                (
                    s.market_id.clone(),
                    MarketCtx {
                        spec: Arc::new(s.clone()),
                        scale: MarketScale::from_spec(s),
                        eligible: *eligibility.get(&s.market_id).unwrap_or(&false),
                    },
                )
            })
            .collect();
        let scope = if cfg.live.cooldown_is_global() {
            CooldownScope::Global
        } else {
            CooldownScope::PerMarket
        };
        let cooldown_ns = cfg.live.post_trade_cooldown_ms.max(0) * 1_000_000;
        let orders = OrderManager::new(session, &markets);
        let num_markets = registry.num_markets();
        let precheck_cfg = super::precheck::HotPrecheckConfig {
            max_book_stale_ns: cfg.simulation.max_book_staleness_ms * 1_000_000,
            requote_threshold_ticks: cfg.live.quote.price_change_ticks_to_requote as i64,
        };
        Strategy {
            cfg,
            markets,
            ctx,
            registry,
            account,
            journal,
            orders,
            cooldown: CooldownState::new(scope),
            dedup: FillDedup::new(),
            hedges: HashMap::new(),
            aster_pos: HashMap::new(),
            hl_pos: HashMap::new(),
            pending: HashMap::new(),
            exec_tx,
            hedge_tx,
            cooldown_ns,
            exec_mode,
            synthetic_trade_seq: 0,
            clean_start: false,
            frozen: false,
            sweep_pending: None,
            aster_cmd_times_ns: VecDeque::new(),
            aster_rate_limited_until_ns: 0,
            aster_429_count: 0,
            last_recovery: HashMap::new(),
            recovery_attempt_seq: HashMap::new(),
            orphan_seen: HashMap::new(),
            last_hot_action_ns: HashMap::new(),
            heal_confirm: None,
            aster_stream: None,
            shutdown: tokio_util::sync::CancellationToken::new(),
            trip_file_path: None,
            breaker_baseline_equity: None,
            breaker_baseline_samples: Vec::new(),
            breaker_breach_streak: 0,
            breaker_last_generation: 0,
            breaker_tripped: false,
            quote_suppressed: HashMap::new(),
            margin_suppressed: HashMap::new(),
            aster_touch_guard_blocked: HashMap::new(),
            dirty: None,
            gen_slots: (0..num_markets).map(|_| GenSlot { last_aster_gen: 0, last_hl_gen: 0 }).collect(),
            precheck_cfg,
            mark_cache: HashMap::new(),
        }
    }

    /// Wire the shared dirty-market bitset (set by the registry builder in `run.rs`).
    pub fn set_dirty(&mut self, dirty: Arc<crate::hotpath::dirty::DirtyMarkets>) {
        self.dirty = Some(dirty);
    }

    /// Arm the cumulative-loss circuit breaker: give the strategy the process shutdown token (to
    /// halt on a trip) and the trip-latch path (to persist the trip). Called once at wire-up, before
    /// `run_strategy`. The breaker only acts live and only when `live.circuit_breaker.enabled`.
    pub fn arm_circuit_breaker(
        &mut self,
        trip_file_path: std::path::PathBuf,
        shutdown: tokio_util::sync::CancellationToken,
    ) {
        self.trip_file_path = Some(trip_file_path);
        self.shutdown = shutdown;
    }

    /// Mark startup reconciliation complete — quoting may begin (still gated by feeds/cooldown).
    pub fn mark_clean_start(&mut self) {
        self.clean_start = true;
        self.account.hot.set_trading_allowed(true);
    }

    /// Wire the Aster user-stream liveness so [`may_quote`](Self::may_quote) can freeze on a
    /// silently-dead fill stream (live only).
    pub fn set_user_stream(&mut self, s: Arc<super::userstream::StreamLiveness>) {
        self.aster_stream = Some(s);
    }

    #[inline]
    fn touch_hysteresis_enabled(&self) -> bool {
        self.cfg.quote.min_aster_touch_distance_bps > Decimal::ZERO
            && self.cfg.quote.min_aster_touch_hysteresis_bps > Decimal::ZERO
    }

    fn latch_aster_touch_guard(&mut self, market: &MarketId, side: Side, now_ns: i64) {
        if !self.touch_hysteresis_enabled() {
            return;
        }
        let key = (market.clone(), side);
        if !self.aster_touch_guard_blocked.contains_key(&key) {
            self.aster_touch_guard_blocked.insert(key, now_ns);
            debug!(
                "Aster touch guard blocked for {market} {side:?}; rearm at {}bps",
                self.cfg.quote.aster_touch_rearm_distance_bps()
            );
        }
    }

    fn clear_aster_touch_guard(&mut self, market: &MarketId, side: Side, now_ns: i64) {
        if let Some(since_ns) = self.aster_touch_guard_blocked.remove(&(market.clone(), side)) {
            debug!(
                "Aster touch guard rearmed for {market} {side:?} after {}ms",
                now_ns.saturating_sub(since_ns) / 1_000_000
            );
        }
    }

    fn aster_touch_guard_status_for_empty(
        &self,
        market: &MarketId,
        side: Side,
        current: Option<CurrentOrder>,
        now_ns: i64,
    ) -> AsterTouchGuardStatus {
        if current.is_some() {
            return AsterTouchGuardStatus::Off;
        }
        let Some(since_ns) = self.aster_touch_guard_blocked.get(&(market.clone(), side)) else {
            return AsterTouchGuardStatus::Off;
        };
        let max_ms = self.cfg.quote.max_aster_touch_hysteresis_ms;
        if max_ms > 0 && now_ns.saturating_sub(*since_ns) >= max_ms.saturating_mul(1_000_000) {
            AsterTouchGuardStatus::Expired
        } else {
            AsterTouchGuardStatus::Active
        }
    }

    fn expire_aster_touch_guard_if_needed(
        &mut self,
        market: &MarketId,
        side: Side,
        current: Option<CurrentOrder>,
        now_ns: i64,
    ) -> AsterTouchGuardStatus {
        let status = self.aster_touch_guard_status_for_empty(market, side, current, now_ns);
        if status == AsterTouchGuardStatus::Expired {
            self.clear_aster_touch_guard(market, side, now_ns);
        }
        status
    }

    fn current_order_for_decision(
        &self,
        market: &MarketId,
        side: Side,
        scale: &MarketScale,
    ) -> Option<CurrentOrder> {
        self.orders.slot(market, side).and_then(|s| {
            (s.is_live() && s.remaining_lots() > 0).then(|| CurrentOrder {
                price: scale.ticks_to_price(s.price_ticks),
                qty: scale.lots_to_qty(s.remaining_lots()),
            })
        })
    }

    /// Latch the hysteresis state for an empty side that just rejected on the base
    /// touch threshold. Existing orders are handled by `apply_decision` when the
    /// cancel reason is `QUOTE_TOO_CLOSE_TO_TOUCH`. The reject reason is the one the
    /// side evaluation already computed — whenever this latch is reachable (guard not
    /// active) that evaluation ran on the base quote config, so re-running the quote
    /// engine here (the pre-2026-07 behavior, ~2x reprice cost in the standby state)
    /// would produce the identical result.
    fn latch_empty_touch_reject_if_needed(
        &mut self,
        market: &MarketId,
        side: Side,
        decision: &SideDecision,
        reject: Option<RejectReason>,
        current: Option<CurrentOrder>,
        now_ns: i64,
    ) {
        if !self.touch_hysteresis_enabled() {
            return;
        }
        if current.is_some() || !matches!(decision, SideDecision::Hold) {
            return;
        }
        if self.aster_touch_guard_blocked.contains_key(&(market.clone(), side)) {
            return;
        }
        if reject == Some(RejectReason::QuoteTooCloseToTouch) {
            self.latch_aster_touch_guard(market, side, now_ns);
        }
    }

    /// Latch a freeze. Maker quoting stops until the cold reconciler proves the account/order state
    /// is clean across snapshots and self-heals it.
    fn freeze(&mut self, now_ns: i64, cause: &'static str) {
        if !self.frozen {
            self.frozen = true;
            self.account.hot.set_trading_allowed(false);
            warn!("maker quoting FROZEN: {cause}");
            self.journal.record(now_ns, "freeze", None, serde_json::json!({ "cause": cause }));
        }
    }

    fn freeze_and_sweep(&mut self, now_ns: i64, cause: &'static str) {
        self.freeze(now_ns, cause);
        if self.exec_mode.sends_real_orders() {
            self.request_safety_sweep(now_ns, cause);
        }
    }

    #[inline]
    fn exec_queue_low_for_optional_work(&self) -> bool {
        self.exec_mode.sends_real_orders() && self.exec_tx.capacity() <= EXEC_CANCEL_RESERVE
    }

    #[inline]
    fn aster_backoff_remaining_ms(&self, now_ns: i64) -> i64 {
        self.aster_rate_limited_until_ns.saturating_sub(now_ns) / 1_000_000
    }

    fn prune_aster_cmd_budget(&mut self, now_ns: i64) {
        let cutoff = now_ns.saturating_sub(ASTER_CMD_WINDOW_NS);
        while self.aster_cmd_times_ns.front().is_some_and(|&t| t < cutoff) {
            self.aster_cmd_times_ns.pop_front();
        }
    }

    fn aster_cmds_in_window(&self, now_ns: i64) -> u32 {
        let cutoff = now_ns.saturating_sub(ASTER_CMD_WINDOW_NS);
        self.aster_cmd_times_ns.iter().filter(|&&t| t >= cutoff).count() as u32
    }

    fn aster_command_cost(&self, cmd: &ExecCommand) -> u32 {
        match cmd {
            ExecCommand::Replace { .. } => 2, // worker performs cancel + place
            ExecCommand::CancelAllBot => self.markets.len().max(1) as u32,
            ExecCommand::Shutdown => 0,
            _ => 1,
        }
    }

    fn aster_budget_allows(&mut self, priority: AsterCommandPriority, cost: u32, now_ns: i64) -> bool {
        if !self.exec_mode.sends_real_orders() || cost == 0 {
            return true;
        }
        if now_ns < self.aster_rate_limited_until_ns {
            return false;
        }
        self.prune_aster_cmd_budget(now_ns);
        let cap = self.cfg.live.aster.effective_max_rest_requests_per_minute();
        if cap == 0 {
            return false;
        }
        let used = self.aster_cmd_times_ns.len() as u32;
        let reserve = self.cfg.live.aster.effective_optional_rest_reserve_per_minute();
        let limit = match priority {
            AsterCommandPriority::Optional => cap.saturating_sub(reserve),
            AsterCommandPriority::RiskReducing | AsterCommandPriority::Safety | AsterCommandPriority::Deadman => cap,
        };
        used.saturating_add(cost) <= limit
    }

    fn record_aster_command_dispatch(&mut self, cost: u32, now_ns: i64) {
        if !self.exec_mode.sends_real_orders() || cost == 0 {
            return;
        }
        self.prune_aster_cmd_budget(now_ns);
        for _ in 0..cost {
            self.aster_cmd_times_ns.push_back(now_ns);
        }
    }

    fn try_send_aster_cmd(&mut self, cmd: ExecCommand, priority: AsterCommandPriority, now_ns: i64) -> ExecDispatch {
        let cost = self.aster_command_cost(&cmd);
        if !self.aster_budget_allows(priority, cost, now_ns) {
            return ExecDispatch::BudgetBlocked;
        }
        match self.exec_tx.try_send(cmd) {
            Ok(()) => {
                self.record_aster_command_dispatch(cost, now_ns);
                ExecDispatch::Sent
            }
            Err(TrySendError::Full(_)) => ExecDispatch::QueueFull,
            Err(TrySendError::Closed(_)) => ExecDispatch::QueueClosed,
        }
    }

    fn note_aster_budget_block(&mut self, now_ns: i64, cause: &'static str, priority: AsterCommandPriority) {
        let remaining = self.aster_backoff_remaining_ms(now_ns);
        if remaining > 0 {
            warn!("Aster REST command deferred during rate-limit backoff ({remaining}ms remaining): {cause}");
        } else {
            warn!(
                "Aster REST command budget exhausted: {cause} priority={priority:?} used={}/{} reserve={} exec_capacity={}",
                self.aster_cmds_in_window(now_ns),
                self.cfg.live.aster.effective_max_rest_requests_per_minute(),
                self.cfg.live.aster.effective_optional_rest_reserve_per_minute(),
                self.exec_tx.capacity()
            );
        }
        self.journal.record(
            now_ns,
            "aster_cmd_blocked",
            None,
            serde_json::json!({
                "cause": cause,
                "priority": format!("{priority:?}"),
                "cmd_rate": self.aster_cmds_in_window(now_ns),
                "cmd_cap": self.cfg.live.aster.effective_max_rest_requests_per_minute(),
                "backoff_ms": remaining.max(0),
                "exec_capacity": self.exec_tx.capacity(),
            }),
        );
    }

    fn on_aster_rate_limited(&mut self, now_ns: i64, reason: String, backoff_ms: i64) {
        let backoff_ns = backoff_ms.max(1).saturating_mul(1_000_000);
        self.aster_rate_limited_until_ns = self.aster_rate_limited_until_ns.max(now_ns.saturating_add(backoff_ns));
        self.aster_429_count = self.aster_429_count.saturating_add(1);
        warn!(
            "maker quoting FROZEN: Aster REST rate limit ({}ms backoff, count={}): {}",
            backoff_ms,
            self.aster_429_count,
            reason
        );
        self.freeze(now_ns, "aster_rate_limited");
        self.sweep_pending.get_or_insert(SweepState {
            requested_ns: now_ns,
            last_attempt_ns: now_ns,
            reason: "aster_rate_limited",
        });
        self.journal.record(
            now_ns,
            "aster_rate_limited",
            None,
            serde_json::json!({"reason": reason, "backoff_ms": backoff_ms, "count": self.aster_429_count}),
        );
    }

    fn cancel_target(&mut self, market: &MarketId, side: Side, now_ns: i64) -> CancelTarget {
        if self.exec_mode.sends_real_orders() && self.sweep_pending.is_some() {
            return CancelTarget::Suppressed;
        }
        self.orders
            .cancel_target(market, side, now_ns, self.cfg.live.aster.cancel_retry_backoff_ms)
    }

    fn book(&self, market: &MarketId, venue: VenueTag) -> Option<Arc<OrderBook>> {
        self.registry.cell(market, venue).and_then(|c| c.load())
    }

    /// Fresh executable HL quote source for immediate hedging: prefer BBO, then L2.
    ///
    /// Uses the VenueBook monotonic stamps, not OrderBook wall-clock age, so NTP jumps
    /// cannot make stale data look fresh. The returned Arc keeps the chosen book alive
    /// across subsequent &mut self work in the fill handler.
    fn fresh_hl_quote_book(&self, market: &MarketId, now_ns: i64) -> Option<SelectedHlBook> {
        let cell = self.registry.cell(market, VenueTag::Hyperliquid)?;
        let max_stale_ms = self.cfg.simulation.max_book_staleness_ms;

        // Read age before the ArcSwap pointer so a concurrent publish cannot pair an
        // old book Arc with a newer freshness stamp. A false negative is safe; a false
        // fresh hedge source is not.
        let bbo_age_ms = cell.bbo_age_ms(now_ns);
        let bbo = cell.load_bbo();
        if bbo_age_ms <= max_stale_ms && bbo.as_deref().is_some_and(executable_quote_book) {
            return bbo.map(|book| SelectedHlBook { source: HlQuoteSource::Bbo, path: HlHedgePath::Decimal, book, age_ms: bbo_age_ms, bbo_depth: None });
        }

        let l2_age_ms = cell.book_age_ms(now_ns);
        let l2 = cell.load();
        if l2_age_ms <= max_stale_ms && l2.as_deref().is_some_and(executable_quote_book) {
            return l2.map(|book| SelectedHlBook { source: HlQuoteSource::L2, path: HlHedgePath::Decimal, book, age_ms: l2_age_ms, bbo_depth: None });
        }

        None
    }

    /// Fresh executable HL book for a known hedge quantity. BBO is trusted only when the
    /// relevant top size is materially deeper than the intended hedge; otherwise use fresh L2.
    fn fresh_hl_hedge_book(
        &self,
        market: &MarketId,
        now_ns: i64,
        hedge_side: Side,
        hedge_qty: Decimal,
    ) -> Option<SelectedHlBook> {
        let cell = self.registry.cell(market, VenueTag::Hyperliquid)?;
        let max_stale_ms = self.cfg.simulation.max_book_staleness_ms;
        let depth_multiple = self.cfg.quote.depth_liquidity_multiple;

        let l2_age_ms = cell.book_age_ms(now_ns);
        let l2 = cell.load();
        let l2_ok = l2_age_ms <= max_stale_ms && l2.as_deref().is_some_and(executable_quote_book);

        let bbo_age_ms = cell.bbo_age_ms(now_ns);
        let bbo = cell.load_bbo();
        let mut bbo_depth = None;
        if bbo_age_ms <= max_stale_ms
            && bbo.as_deref().is_some_and(executable_quote_book)
            && bbo.as_deref().is_some_and(|b| bbo_not_older_than_l2(b, l2.as_deref()))
        {
            let book = bbo.as_ref().expect("checked above");
            let snapshot = hl_bbo_depth_snapshot(book.as_ref(), hedge_side, hedge_qty, depth_multiple);
            if snapshot.sufficient {
                return Some(SelectedHlBook {
                    source: HlQuoteSource::Bbo,
                    path: HlHedgePath::Decimal,
                    book: Arc::clone(book),
                    age_ms: bbo_age_ms,
                    bbo_depth: Some(snapshot),
                });
            }
            bbo_depth = Some(snapshot);
        }

        if l2_ok {
            return l2.map(|book| SelectedHlBook { source: HlQuoteSource::L2, path: HlHedgePath::Decimal, book, age_ms: l2_age_ms, bbo_depth });
        }

        None
    }

    /// Fresh executable HL hot book for a known hedge quantity. This mirrors
    /// `fresh_hl_hedge_book` but uses the prebuilt integer books and HL quantity lots.
    fn fresh_hl_hedge_hot(
        &self,
        market: &MarketId,
        now_ns: i64,
        hedge_side: Side,
        hedge_qty: Decimal,
    ) -> Option<SelectedHlHotBook> {
        let cell = self.registry.cell(market, VenueTag::Hyperliquid)?;
        let ctx = self.ctx.get(market)?;
        let max_stale_ns = self.cfg.simulation.max_book_staleness_ms * 1_000_000;
        let depth_multiple = self.cfg.quote.depth_liquidity_multiple;

        let l2_age_ms = cell.book_age_ms(now_ns);
        let l2 = cell.load_hot();
        let l2_ok = l2
            .as_deref()
            .is_some_and(|b| now_ns.saturating_sub(b.recv_ns) <= max_stale_ns && executable_hot_book(b));

        let bbo_age_ms = cell.bbo_age_ms(now_ns);
        let bbo = cell.load_bbo_hot();
        let mut bbo_depth = None;
        if bbo
            .as_deref()
            .is_some_and(|b| now_ns.saturating_sub(b.recv_ns) <= max_stale_ns && executable_hot_book(b))
            && bbo.as_deref().is_some_and(|b| hot_bbo_not_older_than_l2(b, l2.as_deref()))
        {
            let book = bbo.as_ref().expect("checked above");
            let snapshot = hl_bbo_hot_depth_snapshot(&ctx.scale, book.as_ref(), hedge_side, hedge_qty, depth_multiple);
            if snapshot.sufficient {
                return Some(SelectedHlHotBook {
                    source: HlQuoteSource::Bbo,
                    book: Arc::clone(book),
                    age_ms: bbo_age_ms,
                    bbo_depth: Some(snapshot),
                });
            }
            bbo_depth = Some(snapshot);
        }

        if l2_ok {
            return l2.map(|book| SelectedHlHotBook { source: HlQuoteSource::L2, book, age_ms: l2_age_ms, bbo_depth });
        }

        None
    }

    /// Try to hydrate a hot hedge-source choice to a matching raw book for the existing
    /// Decimal IOC crossing-price calculation. If the raw book has not caught up to the hot
    /// snapshot yet, return None and let the caller use the existing Decimal fallback path.
    fn hydrate_hl_hot_hedge_book(
        &self,
        market: &MarketId,
        now_ns: i64,
        hedge_side: Side,
        hedge_qty: Decimal,
        selected: &SelectedHlHotBook,
    ) -> Option<SelectedHlBook> {
        let cell = self.registry.cell(market, VenueTag::Hyperliquid)?;
        let ctx = self.ctx.get(market)?;
        let max_stale_ms = self.cfg.simulation.max_book_staleness_ms;
        let depth_multiple = self.cfg.quote.depth_liquidity_multiple;

        let l2_age_ms = cell.book_age_ms(now_ns);
        let l2 = cell.load();
        let l2_ok = l2_age_ms <= max_stale_ms && l2.as_deref().is_some_and(executable_quote_book);

        match selected.source {
            HlQuoteSource::Bbo => {
                let bbo_age_ms = cell.bbo_age_ms(now_ns);
                let bbo = cell.load_bbo()?;
                if bbo_age_ms > max_stale_ms
                    || !executable_quote_book(bbo.as_ref())
                    || bbo.exch_ts.timestamp_millis() < selected.book.exch_ms
                    || !bbo_not_older_than_l2(bbo.as_ref(), l2.as_deref())
                {
                    return None;
                }
                let snapshot = hl_bbo_depth_snapshot(bbo.as_ref(), hedge_side, hedge_qty, depth_multiple);
                if !snapshot.sufficient {
                    return None;
                }
                Some(SelectedHlBook {
                    source: HlQuoteSource::Bbo,
                    path: HlHedgePath::Hot,
                    book: bbo,
                    age_ms: selected.age_ms,
                    bbo_depth: Some(snapshot),
                })
            }
            HlQuoteSource::L2 => {
                let l2 = l2?;
                if !l2_ok || l2.exch_ts.timestamp_millis() < selected.book.exch_ms {
                    return None;
                }
                Some(SelectedHlBook {
                    source: HlQuoteSource::L2,
                    path: HlHedgePath::Hot,
                    book: l2,
                    age_ms: selected.age_ms,
                    bbo_depth: selected.bbo_depth.as_ref().map(|d| HlBboDepthSnapshot {
                        top_qty: d.top_lots.map(|lots| ctx.scale.hl_lots_to_qty(lots)),
                        required_qty: ctx.scale.hl_lots_to_qty(d.required_lots),
                        multiple: d.multiple,
                        sufficient: d.sufficient,
                    }),
                })
            }
        }
    }

    fn fresh_hl_hedge_book_hot_first(
        &self,
        market: &MarketId,
        now_ns: i64,
        hedge_side: Side,
        hedge_qty: Decimal,
    ) -> Option<SelectedHlBook> {
        if let Some(hot) = self.fresh_hl_hedge_hot(market, now_ns, hedge_side, hedge_qty) {
            if let Some(raw) = self.hydrate_hl_hot_hedge_book(market, now_ns, hedge_side, hedge_qty, &hot) {
                return Some(raw);
            }
        }
        self.fresh_hl_hedge_book(market, now_ns, hedge_side, hedge_qty)
    }

    /// Fresh executable Aster touch source for fill-time diagnostics: prefer BBO when it is
    /// fresh and not exchange-older than the installed L2 book, otherwise use fresh L2.
    fn fresh_aster_touch_book(&self, market: &MarketId, now_ns: i64) -> Option<SelectedAsterTouch> {
        let cell = self.registry.cell(market, VenueTag::Aster)?;
        let max_stale_ms = self.cfg.simulation.max_book_staleness_ms;

        let l2_age_ms = cell.book_age_ms(now_ns);
        let l2 = cell.load();
        let l2_ok = l2_age_ms <= max_stale_ms && l2.as_deref().is_some_and(executable_quote_book);

        let bbo_age_ms = cell.bbo_age_ms(now_ns);
        let bbo = cell.load_bbo();
        if bbo_age_ms <= max_stale_ms
            && bbo
                .as_deref()
                .is_some_and(|b| executable_quote_book(b) && bbo_not_older_than_l2(b, l2.as_deref()))
        {
            return bbo.map(|book| SelectedAsterTouch { source: AsterQuoteSource::Bbo, book, age_ms: bbo_age_ms });
        }

        if l2_ok {
            return l2.map(|book| SelectedAsterTouch { source: AsterQuoteSource::L2, book, age_ms: l2_age_ms });
        }

        None
    }

    fn has_open_aster_bot_orders_in(&self, snap: &super::account::AccountSnapshot) -> bool {
        snap.open_orders.iter().any(|o| {
            o.venue == Venue::Aster && self.ctx.contains_key(&o.market) && o.is_bot_order()
        })
    }

    fn request_safety_sweep(&mut self, now_ns: i64, reason: &'static str) {
        let should_send = self
            .sweep_pending
            .is_none_or(|s| now_ns.saturating_sub(s.last_attempt_ns) >= (self.cfg.live.aster.safety_sweep_retry_ms as i64).saturating_mul(1_000_000));
        if !should_send {
            return;
        }

        let requested_ns = self.sweep_pending.map(|s| s.requested_ns).unwrap_or(now_ns);
        match self.try_send_aster_cmd(ExecCommand::CancelAllBot, AsterCommandPriority::Safety, now_ns) {
            ExecDispatch::Sent => {
                warn!("safety sweep requested: {reason}");
                self.journal.record(now_ns, "safety_sweep", None, serde_json::json!({"reason": reason}));
                self.sweep_pending = Some(SweepState { requested_ns, last_attempt_ns: now_ns, reason });
            }
            ExecDispatch::BudgetBlocked => {
                self.note_aster_budget_block(now_ns, reason, AsterCommandPriority::Safety);
                self.sweep_pending = Some(SweepState { requested_ns, last_attempt_ns: now_ns, reason });
                self.freeze(now_ns, "safety_sweep_budget_blocked");
            }
            ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                error!("CRITICAL: could not enqueue safety sweep ({reason}); will retry");
                self.sweep_pending = Some(SweepState { requested_ns, last_attempt_ns: now_ns, reason });
                self.freeze(now_ns, "safety_sweep_dispatch_failed");
            }
        }
    }

    fn drive_safety_sweep(&mut self, now_ns: i64) {
        let Some(sweep) = self.sweep_pending else {
            return;
        };

        let snap = self.account.load();
        // Trust only a snapshot whose reads began after the original sweep request. While a sweep
        // is pending, maker quoting is gated, so a fresh snapshot with no bot-owned Aster orders is
        // enough to clear local slots; retry timestamps are delivery backoff, not a new proof bar.
        if snap.read_start_ns > sweep.requested_ns && !self.has_open_aster_bot_orders_in(&snap) {
            for (m, side) in self.orders.live_slots() {
                self.orders.on_closed(&m, side);
            }
            self.sweep_pending = None;
            warn!("safety sweep confirmed clean by account snapshot");
            self.journal.record(now_ns, "safety_sweep_clean", None, serde_json::json!({"reason": sweep.reason}));
            return;
        }

        if now_ns.saturating_sub(sweep.last_attempt_ns) >= (self.cfg.live.aster.safety_sweep_retry_ms as i64).saturating_mul(1_000_000) {
            self.request_safety_sweep(now_ns, sweep.reason);
        }
    }

    /// Per-market feed freshness — the per-`(market)` analogue of the global watchdog
    /// `TradingGate`, which over-broadly halts ALL pairs when ANY single feed is stale. This
    /// market may quote only when its Aster book is fresh and Hyperliquid has fresh quote-touch
    /// data (fast BBO or L2 snapshot) AND neither side is REST-divergent,
    /// so a stale or divergent feed on one pair no longer suppresses quoting on every other
    /// pair. Connection-staleness (the watchdog's 60 s reconnect threshold) is subsumed: a
    /// dead socket implies a stale book, which the tighter book-staleness test already catches.
    /// Uses the monotonic `now_ns` (same clock `publish` stamps), matching the watchdog scan.
    fn market_feeds_fresh(&self, market: &MarketId, now_ns: i64) -> bool {
        let max_stale = self.cfg.simulation.max_book_staleness_ms;
        let aster_fresh = self
            .registry
            .cell(market, VenueTag::Aster)
            // Aster depth is still the queue/depth source, but a fresh bookTicker/BBO is
            // sufficient for live quote-touch safety on quiet event-driven depth feeds.
            .is_some_and(|c| c.quote_age_ms(now_ns) <= max_stale && !c.is_divergent());
        let hl_fresh = self
            .registry
            .cell(market, VenueTag::Hyperliquid)
            .is_some_and(|c| c.quote_age_ms(now_ns) <= max_stale && !c.is_divergent());
        aster_fresh && hl_fresh
    }

    fn position_context(&self, market: &MarketId, now_ns: i64) -> PositionContext {
        let a = self.aster_pos.get(market).copied().unwrap_or_default();
        let h = self.hl_pos.get(market).copied().unwrap_or_default();
        let mut aster_cap_notional = self.cfg.capital.aster_cap_notional();
        // Live margin guard: shrink the Aster cap to the REAL available collateral (minus a buffer)
        // so the position-increasing side stops quoting before the exchange rejects with -2019. Only
        // when live, enabled, and the snapshot is fresh — otherwise keep the static cap (a stale
        // snapshot already closes the maker gate via `account_fresh`, so no order is placed off it).
        // Load the snapshot ONCE and judge freshness from that same Arc to avoid a load/age race.
        let mg = &self.cfg.live.margin_guard;
        if self.exec_mode.sends_real_orders() && mg.enabled {
            let snap = self.account.load();
            let fresh = snap.source_ts_ns != 0
                && now_ns.saturating_sub(snap.source_ts_ns) / 1_000_000 <= self.cfg.live.max_account_snapshot_age_ms;
            if fresh {
                // Conservative collateral: min(wallet balance, mark-to-market equity).
                let cap_base = snap.aster_available_usd.min(snap.aster_equity_usd);
                // Notional already consumed by OTHER markets' Aster legs (account-wide collateral).
                let other: Decimal = snap
                    .aster_positions
                    .iter()
                    .filter(|p| &p.market != market)
                    .map(|p| p.signed_qty.abs() * p.entry_px)
                    .sum();
                aster_cap_notional = effective_aster_cap_notional(
                    aster_cap_notional,
                    cap_base,
                    mg.aster_safety_buffer_usd,
                    self.cfg.capital.leverage,
                    other,
                );
            }
        }
        PositionContext {
            aster_pos_qty: a.qty,
            hl_pos_qty: h.qty,
            aster_cap_notional,
            hl_cap_notional: self.cfg.capital.hyperliquid_cap_notional(),
            enforce: self.cfg.capital.enforce_position_cap,
            reduce_position_only: self.exec_mode.sends_real_orders()
                && self.cfg.live.quote.reduce_position_only,
        }
    }

    /// The reason new maker quoting is currently closed for `market`, or `None` if it may quote.
    /// Builds the full [`MakerGateInputs`] and runs the canonical [`evaluate_maker_gate`] (plan §6
    /// reopen conditions / §8.1 invariants 5–7), then the cooldown. Live-only inputs (account
    /// freshness, position reconciliation) are vacuously satisfied in paper (there is no exchange
    /// to be stale against), so paper behaviour is unchanged. Risk-reducing actions ignore this.
    /// `Some(reason)` is the human-readable cause (a [`FreezeReason`] string or `"COOLDOWN"`) so a
    /// closure can be surfaced instead of silently stopping quotes — see
    /// [`note_quote_gate`](Self::note_quote_gate).
    fn maker_gate_reason(&self, market: &MarketId, now_ns: i64) -> Option<&'static str> {
        let live = self.exec_mode.sends_real_orders();
        if live && self.sweep_pending.is_some() {
            return Some("SAFETY_SWEEP_PENDING");
        }
        if self.frozen && self.clean_start {
            return Some(MAKER_GATE_FROZEN);
        }
        let inputs = MakerGateInputs {
            clean_start_done: self.clean_start,
            // Per-market (NOT the global watchdog gate): only THIS market's own Aster+HL feed
            // freshness gates it, so one stale low-liquidity pair no longer pulls resting
            // quotes on every other pair. The global gate stays a logged gauge in the watchdog.
            feed_gate_open: self.market_feeds_fresh(market, now_ns),
            // No exchange account/user stream in paper ⇒ freshness is vacuous there.
            account_fresh: !live || self.account.age_ms(now_ns) <= self.cfg.live.max_account_snapshot_age_ms,
            // Aster fill stream liveness (live): a silently-dead stream stops us SEEING fills, so
            // freeze new quoting (the reconciler backstop still recovers any orphan meanwhile). If
            // the stream isn't wired, default fresh to avoid a startup deadlock.
            aster_stream_fresh: !live
                || self
                    .aster_stream
                    .as_ref()
                    .is_none_or(|s| s.age_ms(now_ns) <= self.cfg.live.max_user_stream_staleness_ms),
            // HL maker fills are not sourced from a separate user stream today (hedge acks come on
            // the exec event channel), so this is vacuously fresh.
            hl_stream_fresh: true,
            positions_reconciled: !live || self.positions_reconciled(),
            no_orphan_hedge: !self.has_orphan_hedge(),
            unhedged_within_limits: self.unhedged_within_limits(now_ns),
        };
        if let Err(reason) = evaluate_maker_gate(&inputs) {
            return Some(reason.as_str());
        }
        if self.cooldown.active(now_ns, market) {
            return Some("COOLDOWN");
        }
        None
    }

    /// Whether new maker quoting is currently allowed (the gate is open). Side-effect-free wrapper
    /// over [`maker_gate_reason`](Self::maker_gate_reason); production drives the gate through
    /// [`note_quote_gate`](Self::note_quote_gate) (which also logs transitions), so this is used
    /// only by tests.
    #[cfg(test)]
    fn may_quote(&self, market: &MarketId, now_ns: i64) -> bool {
        self.maker_gate_reason(market, now_ns).is_none()
    }

    /// Evaluate the maker gate for `market`, LOGGING + journaling the transition so a lasting
    /// suppression is never invisible — the failure mode where the gate closes (orphan hedge /
    /// unhedged-over-limit / stale account snapshot / stale feed / position mismatch) and quoting
    /// silently stops with no log and no `frozen` latch. Returns whether quoting is allowed. Logs
    /// only on a *persistent* closure (past a short grace ≈ the normal post-trade cooldown) and on
    /// resume, so routine cooldowns / one-tick feed blips never spam the log.
    fn note_quote_gate(&mut self, market: &MarketId, now_ns: i64) -> bool {
        let reason = self.maker_gate_reason(market, now_ns);
        // Never log a closure shorter than this: a normal post-fill COOLDOWN (and brief feed blips)
        // clear well within it; a real latch (orphan / stale snapshot) lasts far longer.
        let grace_ns = self.cooldown_ns.saturating_mul(2).max(5_000_000_000);
        match reason {
            Some(r) => {
                match self.quote_suppressed.get(market).copied() {
                    // New closure, or the reason changed: (re)start the timer; not yet logged.
                    Some((_, prev_r, _)) if prev_r != r => {
                        self.quote_suppressed.insert(market.clone(), (now_ns, r, false));
                    }
                    None => {
                        self.quote_suppressed.insert(market.clone(), (now_ns, r, false));
                    }
                    // Same reason, already logged: nothing to do (no spam).
                    Some((_, _, true)) => {}
                    // Same reason, not yet logged: log ONCE it has persisted past the grace.
                    Some((since_ns, _, false)) => {
                        if now_ns.saturating_sub(since_ns) >= grace_ns {
                            warn!("maker quoting SUPPRESSED on {market}: {r} (gate closed, placing no new quotes)");
                            self.journal.record(now_ns, "quote_suppressed", Some(market.0.clone()), serde_json::json!({"reason": r}));
                            self.quote_suppressed.insert(market.clone(), (since_ns, r, true));
                        }
                    }
                }
                let user_stream_stale = r == "ASTER_USER_STREAM_STALE";
                let should_sweep = self.exec_mode.sends_real_orders()
                    && r != "COOLDOWN"
                    && r != "SAFETY_SWEEP_PENDING"
                    && r != MAKER_GATE_FROZEN
                    && (self.cfg.live.cancel_all_on_gate_close
                        || (user_stream_stale && self.cfg.live.cancel_all_on_user_stream_stale));
                if should_sweep {
                    self.request_safety_sweep(now_ns, r);
                }
                false
            }
            None => {
                // Gate open. If we had logged a suppression, announce the resume with its duration.
                if let Some((since_ns, prev_r, logged)) = self.quote_suppressed.remove(market) {
                    if logged {
                        let secs = now_ns.saturating_sub(since_ns) as f64 / 1e9;
                        info!("maker quoting RESUMED on {market} after {secs:.1}s suppressed ({prev_r})");
                        self.journal.record(now_ns, "quote_resumed", Some(market.0.clone()), serde_json::json!({"prev_reason": prev_r, "suppressed_secs": secs}));
                    }
                }
                true
            }
        }
    }

    /// Refresh the per-market HL mid mark cache. Called once per wake/tick batch to avoid
    /// O(N²) book loads in `positions_reconciled` (which is called per-market inside
    /// `reprice_market`, and iterates all markets internally).
    fn refresh_mark_cache(&mut self) {
        self.mark_cache.clear();
        for m in &self.markets {
            let mark = self
                .book(m, VenueTag::Hyperliquid)
                .and_then(|b| b.mid())
                .unwrap_or(Decimal::ZERO);
            if mark > Decimal::ZERO {
                self.mark_cache.insert(m.clone(), mark);
            }
        }
    }

    /// True when every market's predicted position agrees with the exchange-reported snapshot
    /// within `max_position_mismatch_usd` (invariant 6). A single mismatch ⇒ freeze (returns
    /// false). Only meaningful in live mode (paper has no exchange snapshot).
    fn positions_reconciled(&self) -> bool {
        let snap = self.account.load();
        let tol = self.cfg.live.max_position_mismatch_usd;
        for m in &self.markets {
            let mark = self.mark_cache.get(m).copied().unwrap_or(Decimal::ZERO);
            if mark <= Decimal::ZERO {
                continue; // no mark ⇒ can't judge; don't spuriously freeze on a missing book
            }
            let pred_a = self.aster_pos.get(m).map(|p| p.qty).unwrap_or(Decimal::ZERO);
            let rep_a = snap.reported_position(super::account::Venue::Aster, m);
            let pred_h = self.hl_pos.get(m).map(|p| p.qty).unwrap_or(Decimal::ZERO);
            let rep_h = snap.reported_position(super::account::Venue::Hyperliquid, m);
            if position_mismatch(pred_a, rep_a, mark, tol) || position_mismatch(pred_h, rep_h, mark, tol) {
                return false;
            }
        }
        true
    }

    /// True while the total in-flight (not yet hedged) Aster notional and the oldest unhedged
    /// fill's age are both within the configured limits (plan §6 / P0 max-unhedged). An
    /// in-flight hedge's outstanding leg is the risk; resolved hedges don't count.
    fn unhedged_within_limits(&self, now_ns: i64) -> bool {
        let max_notional = self.cfg.live.max_unhedged_notional_usd;
        let max_age_ns = self.cfg.live.max_unhedged_age_ms.max(0) * 1_000_000;
        let mut total_notional = Decimal::ZERO;
        for h in self.hedges.values() {
            if !h.state.is_in_flight() {
                continue;
            }
            let mark = self
                .book(&h.market, VenueTag::Hyperliquid)
                .and_then(|b| b.mid())
                .unwrap_or(h.aster_fill_px);
            total_notional += h.remaining_qty() * mark.abs();
            if now_ns.saturating_sub(h.created_ns) > max_age_ns {
                return false; // an unhedged leg has aged out
            }
        }
        total_notional <= max_notional
    }

    /// DIAGNOSTIC (live): log loop-liveness + the exact per-side quote decision, so a silent
    /// no-quote state is explainable — is the maker gate closed (and why), is `compute_desired_quote`
    /// REJECTING (and why), or is a side already resting? Called on a throttle from `run_strategy`;
    /// read-only, never changes behaviour. If these lines stop appearing the strategy loop itself
    /// has stalled; if they keep appearing the loop is alive and the reason field explains the
    /// no-quote. (Added to root-cause the stuck-after-fill no-quote without a blind redeploy.)
    pub fn log_quote_diag(&self, now_ns: i64) {
        for market in &self.markets {
            let gate = self.maker_gate_reason(market, now_ns).unwrap_or("OPEN");
            let (Some(a_cell), Some(h_cell)) = (
                self.registry.cell(market, VenueTag::Aster),
                self.registry.cell(market, VenueTag::Hyperliquid),
            ) else {
                info!("qdiag {market}: gate={gate} (book cell missing)");
                continue;
            };
            let ab = a_cell.load();
            let a_bbo = a_cell.load_bbo();
            let hl_l2 = h_cell.load();
            let hl_bbo = h_cell.load_bbo();
            let Some(aster_book) = ab.as_deref() else {
                info!("qdiag {market}: gate={gate} (no published Aster book yet)");
                continue;
            };
            let aster_bbo_book = a_bbo.as_deref();
            let hl_l2_book = hl_l2.as_deref();
            let hl_bbo_book = hl_bbo.as_deref();
            if hl_l2_book.is_none() && hl_bbo_book.is_none() {
                info!("qdiag {market}: gate={gate} (no published HL L2/BBO yet)");
                continue;
            }
            let Some(ctx) = self.ctx.get(market) else { continue };
            let spec = &ctx.spec;
            let pos = self.position_context(market, now_ns);
            let max_stale = self.cfg.simulation.max_book_staleness_ms;
            let now = Utc::now();
            let mut decided = String::new();
            for side in [Side::Buy, Side::Sell] {
                let current = self.current_order_for_decision(market, side, &ctx.scale);
                let resting = current.is_some();
                let touch_status = self.aster_touch_guard_status_for_empty(market, side, current, now_ns);
                let touch_blocked = touch_status == AsterTouchGuardStatus::Active;
                let quote_cfg = quote_cfg_for_touch_guard(&self.cfg.quote, touch_blocked, current);
                let r = compute_desired_quote_select_books(
                    &self.cfg.edge,
                    &quote_cfg,
                    aster_book,
                    aster_bbo_book,
                    hl_l2_book,
                    hl_bbo_book,
                    side,
                    spec,
                    max_stale,
                    now,
                    &pos,
                );
                let d = match r {
                    Ok((q, _, hsrc, asrc)) => format!("{side:?}=OK(px={} qty={} depth={}x target={} hlvwap={} hlworst={} hllvls={} asrc={} aeff={}@{} alvls={} hlsrc={})", q.price, q.qty, q.depth_liquidity_multiple, q.depth_target_qty, q.expected_hl_vwap, q.expected_hl_worst_px, q.expected_hl_depth_levels_used, asrc.as_str(), q.effective_aster_touch_source.as_str(), q.effective_aster_touch_px, q.aster_depth_levels_used, hsrc.as_str()),
                    Err(e) => format!("{side:?}=REJECT({})", e.as_str()),
                };
                decided.push_str(&d);
                decided.push_str(&format!(
                    " rest={resting} touch_block={} ",
                    touch_status.as_str()
                ));
            }
            let pa = self.aster_pos.get(market).map(|p| p.qty).unwrap_or_default();
            let ph = self.hl_pos.get(market).map(|p| p.qty).unwrap_or_default();
            // rate=N/cap: the replace-rate limiter usage. If this is at cap while sides show OK but
            // rest=false, the rate limiter is the reason quoting stopped (see OrderManager::replace_rate_ok).
            let rate = self.orders.replaces_in_window(market, now_ns);
            let cap = self.cfg.live.quote.effective_max_replaces_per_minute_per_symbol();
            let cmd_rate = self.aster_cmds_in_window(now_ns);
            let cmd_cap = self.cfg.live.aster.effective_max_rest_requests_per_minute();
            let cmd_reserve = self.cfg.live.aster.effective_optional_rest_reserve_per_minute();
            let exec_cap = self.exec_tx.capacity();
            let backoff_ms = self.aster_backoff_remaining_ms(now_ns).max(0);
            let rate_limited = self.aster_429_count;
            let aster_l2_age = a_cell.book_age_ms(now_ns);
            let aster_bbo_age = a_cell.bbo_age_ms(now_ns);
            let hl_l2_age = h_cell.book_age_ms(now_ns);
            let hl_bbo_age = h_cell.bbo_age_ms(now_ns);
            let a_bbo = aster_bbo_book
                .and_then(|b| Some((b.best_bid()?, b.best_ask()?)))
                .map(|(bid, ask)| format!("{}x{} / {}x{}", bid.qty, bid.px, ask.qty, ask.px))
                .unwrap_or_else(|| "none".to_string());
            let bbo = hl_bbo_book
                .and_then(|b| Some((b.best_bid()?, b.best_ask()?)))
                .map(|(bid, ask)| format!("{}x{} / {}x{}", bid.qty, bid.px, ask.qty, ask.px))
                .unwrap_or_else(|| "none".to_string());
            info!("qdiag {market}: gate={gate} replace_rate={rate}/{cap} aster_cmd_rate={cmd_rate}/{cmd_cap} reserve={cmd_reserve} exec_cap={exec_cap} aster_429={rate_limited} backoff_ms={backoff_ms} pos_a={pa} pos_h={ph} acap={} hedges={} aster_l2_age_ms={aster_l2_age} aster_bbo_age_ms={aster_bbo_age} aster_bbo={a_bbo} hl_l2_age_ms={hl_l2_age} hl_bbo_age_ms={hl_bbo_age} hl_bbo={bbo} | {decided}", pos.aster_cap_notional, self.hedges.len());
        }
    }

    /// Re-evaluate one market (both sides) and emit the resulting commands.
    /// When `force` is false (wake path), skip if neither venue's book generation changed.
    pub async fn reprice_market(&mut self, market: &MarketId, now: DateTime<Utc>, now_ns: i64, force: bool) {
        let t0 = crate::hotpath::clock::mono_now_ns();
        let Some(ctx) = self.ctx.get(market) else { return };
        if !ctx.eligible {
            return; // pair not eligible under the partial policy
        }
        let spec = ctx.spec.clone();
        let scale = ctx.scale.clone();
        if !force {
            if let Some(idx) = self.registry.market_idx(market) {
                let slot = &mut self.gen_slots[idx.0 as usize];
                let a_gen = self.registry.cell(market, VenueTag::Aster).map_or(0, |c| c.quote_generation());
                let h_gen = self.registry.cell(market, VenueTag::Hyperliquid).map_or(0, |c| c.quote_generation());
                if a_gen == slot.last_aster_gen && h_gen == slot.last_hl_gen {
                    return;
                }
                slot.last_aster_gen = a_gen;
                slot.last_hl_gen = h_gen;
            }
        }
        let mut fast_cancelled = [false; 2]; // [Buy, Sell]
        if self.cfg.live.quote.use_hot_integer_math && !force {
            if let (Some(a_cell), Some(h_cell)) = (
                self.registry.cell(market, VenueTag::Aster),
                self.registry.cell(market, VenueTag::Hyperliquid),
            ) {
                let a_arc = a_cell.load_hot();
                let a_bbo_arc = a_cell.load_bbo_hot();
                let h_arc = h_cell.load_hot();
                let h_bbo_arc = h_cell.load_bbo_hot();
                if let Some(a_hot) = select_aster_hot_for_precheck(
                    a_arc.as_deref(),
                    a_bbo_arc.as_deref(),
                    now_ns,
                    self.precheck_cfg.max_book_stale_ns,
                ) {
                    for side in [Side::Buy, Side::Sell] {
                        let current = self.orders.current_hot_order(market, side);
                        let Some(h_hot) = select_hl_hot_for_precheck(
                            h_arc.as_deref(),
                            h_bbo_arc.as_deref(),
                            now_ns,
                            self.precheck_cfg.max_book_stale_ns,
                        ) else {
                            continue;
                        };
                        match hot_precheck_side(a_hot, h_hot, side, current, now_ns, &self.precheck_cfg) {
                            HotPrecheck::CancelFast(_reason) => {
                                let target = self.cancel_target(market, side, now_ns);
                                let CancelTarget::Send { client_id, venue_order_id } = target else {
                                    continue;
                                };
                                let cmd = ExecCommand::Cancel {
                                    market: market.clone(), side, client_id, venue_order_id,
                                };
                                match self.try_send_aster_cmd(cmd, AsterCommandPriority::RiskReducing, now_ns) {
                                    ExecDispatch::Sent => {
                                        self.orders.on_cancel_sent(market, side, now_ns);
                                        let idx = if side == Side::Buy { 0 } else { 1 };
                                        fast_cancelled[idx] = true;
                                    }
                                    ExecDispatch::BudgetBlocked => {
                                        self.note_aster_budget_block(now_ns, "fast_cancel_budget_blocked", AsterCommandPriority::RiskReducing);
                                        self.freeze_and_sweep(now_ns, "fast_cancel_budget_blocked");
                                    }
                                    ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                                        self.freeze_and_sweep(now_ns, "fast_cancel_dispatch_failed");
                                    }
                                }
                            }
                            HotPrecheck::NeedExactQuote => {}
                        }
                    }
                }
            }
        }
        let (Some(a_cell), Some(h_cell)) = (
            self.registry.cell(market, VenueTag::Aster),
            self.registry.cell(market, VenueTag::Hyperliquid),
        ) else {
            return;
        };
        // A direct string-to-integer hot publish may arrive a few microseconds before the raw
        // Decimal book/BBO used by exact quote placement. Use it for fast cancels above, but do
        // not place or replace from raw data until the matching full publish clears the guard.
        if self.cfg.live.quote.use_hot_integer_math && (a_cell.has_hot_only_update() || h_cell.has_hot_only_update()) {
            return;
        }

        let max_stale = self.cfg.simulation.max_book_staleness_ms;

        // Read freshness before the ArcSwap pointers so a concurrent publish cannot
        // pair an older book Arc with a newer stamp. At worst we skip one fresh update
        // until the next wake/tick; we never quote from a falsely-fresh stale Arc.
        let a_bbo_age_ms = a_cell.bbo_age_ms(now_ns);
        let hl_l2_age_ms = h_cell.book_age_ms(now_ns);
        let hl_bbo_age_ms = h_cell.bbo_age_ms(now_ns);

        let ab = a_cell.load();
        let a_bbo = a_cell.load_bbo();
        let hl_l2 = h_cell.load();
        let hl_bbo = h_cell.load_bbo();

        // Keep the full Aster book even if stale: evaluate_side() must be able to
        // return a cancel decision when may_quote is false. Fast/optional feeds are
        // filtered by monotonic VenueBook age before they can influence quote price.
        let Some(aster_book) = ab.as_deref() else {
            return;
        };

        let aster_bbo_book = a_bbo
            .as_deref()
            .filter(|b| a_bbo_age_ms <= max_stale && executable_quote_book(b));

        let hl_l2_book = hl_l2
            .as_deref()
            .filter(|b| hl_l2_age_ms <= max_stale && executable_quote_book(b));

        let hl_bbo_book = hl_bbo
            .as_deref()
            .filter(|b| hl_bbo_age_ms <= max_stale && executable_quote_book(b));
        let replace_unprof = self.cfg.live.quote.replace_immediately_if_unprofitable;
        // Evaluate the maker gate BEFORE building the position context. The gate's account-freshness
        // load must happen first so that, if the gate is open (snapshot fresh), the subsequent
        // position_context load sees a same-or-newer (hence still-fresh) snapshot and applies the
        // dynamic margin cap. Were pos built first, a publish landing between the two loads could open
        // the gate while pos still held the static cap (now_ns is shared, so a newer ts is only fresher).
        let may = self.note_quote_gate(market, now_ns);
        let pos = self.position_context(market, now_ns);

        for side in [Side::Buy, Side::Sell] {
            let fc_idx = if side == Side::Buy { 0 } else { 1 };
            if fast_cancelled[fc_idx] {
                continue;
            }
            let current = self.current_order_for_decision(market, side, &scale);
            let touch_status = self.expire_aster_touch_guard_if_needed(market, side, current, now_ns);
            let touch_blocked = touch_status == AsterTouchGuardStatus::Active;
            let quote_cfg = quote_cfg_for_touch_guard(&self.cfg.quote, touch_blocked, current);
            let (decision, reject) = evaluate_side_with_hl_sources(
                &self.cfg.edge,
                &quote_cfg,
                aster_book,
                aster_bbo_book,
                hl_l2_book,
                hl_bbo_book,
                side,
                &spec,
                max_stale,
                now,
                &pos,
                may,
                current,
                replace_unprof,
            );
            self.latch_empty_touch_reject_if_needed(market, side, &decision, reject, current, now_ns);
            self.apply_decision(market, side, decision, &scale, now_ns).await;
        }
        crate::metrics::SINGLE_REPRICE.record((crate::hotpath::clock::mono_now_ns() - t0) as u64);
    }

    async fn apply_decision(&mut self, market: &MarketId, side: Side, decision: SideDecision, scale: &MarketScale, now_ns: i64) {
        match decision {
            SideDecision::Hold => {}
            SideDecision::Cancel { reason } => {
                let target = self.cancel_target(market, side, now_ns);
                let CancelTarget::Send { client_id, venue_order_id } = target else {
                    return;
                };
                // Dispatch FIRST; mutate local state only if the command is actually queued.
                // A dropped cancel that silently desyncs local state is a safety hazard.
                let cmd = ExecCommand::Cancel { market: market.clone(), side, client_id, venue_order_id };
                match self.try_send_aster_cmd(cmd, AsterCommandPriority::RiskReducing, now_ns) {
                    ExecDispatch::Sent => {
                        self.orders.on_cancel_sent(market, side, now_ns);
                        if reason == ReplaceReason::QuoteTooCloseToTouch {
                            self.latch_aster_touch_guard(market, side, now_ns);
                        }
                        self.journal.record(now_ns, "cancel", Some(market.0.clone()), serde_json::json!({"side": side.as_str(), "reason": reason.as_str()}));
                    }
                    ExecDispatch::BudgetBlocked => {
                        self.note_aster_budget_block(now_ns, "targeted_cancel_budget_blocked", AsterCommandPriority::RiskReducing);
                        self.freeze_and_sweep(now_ns, "aster_command_budget_exhausted");
                    }
                    ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                        warn!("exec queue full/closed: cancel NOT sent for {market} {side:?}; freezing + safety sweep");
                        self.freeze_and_sweep(now_ns, "exec_queue_send_failed");
                    }
                }
            }
            SideDecision::Place(desired) => {
                if let Some(&suppress_ns) = self.margin_suppressed.get(&(market.clone(), side)) {
                    if now_ns.saturating_sub(suppress_ns) < 10_000_000_000 {
                        return;
                    }
                    self.margin_suppressed.remove(&(market.clone(), side));
                    info!("margin suppression expired for {market} {side:?}");
                }
                if !self.orders.replace_rate_ok(market, self.cfg.live.quote.effective_max_replaces_per_minute_per_symbol(), now_ns) {
                    return;
                }
                let price_ticks = scale.price_to_ticks(desired.price);
                let qty_lots = scale.qty_to_lots(desired.qty);
                if qty_lots <= 0 {
                    return;
                }
                if self.exec_queue_low_for_optional_work() {
                    debug!(
                        "exec queue backpressure: skipping optional place for {market} {side:?} (capacity={})",
                        self.exec_tx.capacity()
                    );
                    return;
                }
                if let Some(cid) = self.orders.next_client_id(market, side) {
                    let cmd = ExecCommand::Place { market: market.clone(), side, price_ticks, qty_lots, client_id: cid.clone() };
                    match self.try_send_aster_cmd(cmd, AsterCommandPriority::Optional, now_ns) {
                        ExecDispatch::Sent => {
                            self.orders.on_place_sent(market, side, cid, price_ticks, qty_lots, now_ns);
                            self.clear_aster_touch_guard(market, side, now_ns);
                            self.journal.record(now_ns, "place", Some(market.0.clone()), serde_json::json!({"side": side.as_str(), "price": desired.price.to_string(), "qty": desired.qty.to_string()}));
                        }
                        ExecDispatch::BudgetBlocked => {
                            debug!("Aster command budget/backoff: optional place deferred for {market} {side:?}");
                        }
                        ExecDispatch::QueueFull => {
                            warn!("exec queue full: optional place deferred for {market} {side:?}");
                        }
                        ExecDispatch::QueueClosed => {
                            error!("exec queue closed: place NOT sent for {market} {side:?}; freezing");
                            self.freeze(now_ns, "exec_queue_closed");
                        }
                    }
                }
            }
            SideDecision::Replace { desired, reason } => {
                if let Some(&suppress_ns) = self.margin_suppressed.get(&(market.clone(), side)) {
                    if now_ns.saturating_sub(suppress_ns) < 10_000_000_000 {
                        return;
                    }
                    self.margin_suppressed.remove(&(market.clone(), side));
                    info!("margin suppression expired for {market} {side:?}");
                }
                if self.exec_mode.sends_real_orders()
                    && self.cfg.live.quote.reduce_position_only
                    && reason == ReplaceReason::NoLongerProfitable
                {
                    let target = self.cancel_target(market, side, now_ns);
                    let CancelTarget::Send { client_id, venue_order_id } = target else {
                        return;
                    };
                    let cmd = ExecCommand::Cancel {
                        market: market.clone(),
                        side,
                        client_id,
                        venue_order_id,
                    };
                    match self.try_send_aster_cmd(cmd, AsterCommandPriority::RiskReducing, now_ns) {
                        ExecDispatch::Sent => {
                            self.orders.on_cancel_sent(market, side, now_ns);
                            self.journal.record(
                                now_ns,
                                "cancel",
                                Some(market.0.clone()),
                                serde_json::json!({
                                    "side": side.as_str(),
                                    "reason": "NO_LONGER_PROFITABLE_CANCEL_ONLY",
                                }),
                            );
                        }
                        ExecDispatch::BudgetBlocked => {
                            self.note_aster_budget_block(now_ns, "no_longer_profitable_cancel_only", AsterCommandPriority::RiskReducing);
                            self.freeze_and_sweep(now_ns, "aster_command_budget_exhausted");
                        }
                        ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                            warn!("exec queue full/closed: no-longer-profitable cancel-only NOT sent for {market} {side:?}; freezing + safety sweep");
                            self.freeze_and_sweep(now_ns, "exec_queue_send_failed");
                        }
                    }
                    return;
                }
                // Requote pacing (T1.4): honor `min_requote_interval_ms` for NON-URGENT requotes
                // (price/qty drift). Outside live reduce-only cancel-only mode above, an urgent
                // `NoLongerProfitable` replace BYPASSES this small per-side throttle, but it still
                // obeys the global Aster command budget/backoff below.
                // The fix is deliberate: urgent no-longer-profitable work must be risk-reducing, not
                // an unbounded cancel+place flood that consumes the safety queue and trips venue 429s.
                let non_urgent = matches!(&reason, ReplaceReason::PriceChanged | ReplaceReason::QuantityChanged);
                if non_urgent
                    && !self
                        .orders
                        .slot(market, side)
                        .is_some_and(|s| s.throttle_ok(now_ns, self.cfg.live.quote.min_requote_interval_ms))
                {
                    return; // too soon since the last requote on this side — skip this non-urgent replace
                }
                if !self.orders.replace_rate_ok(market, self.cfg.live.quote.effective_max_replaces_per_minute_per_symbol(), now_ns) {
                    return;
                }
                let price_ticks = scale.price_to_ticks(desired.price);
                let qty_lots = scale.qty_to_lots(desired.qty);
                if qty_lots <= 0 {
                    return;
                }
                let (old_cid, old_voi) = match self.orders.slot(market, side) {
                    Some(s) if s.state == OrderLifecycle::Open => (s.client_id.clone(), s.venue_order_id.clone()),
                    None => (None, None),
                    Some(_) => return,
                };
                let Some(old_cid) = old_cid else { return };
                if self.exec_mode.sends_real_orders() && self.sweep_pending.is_some() {
                    return;
                }
                let full_replace_budget_ok = self.aster_budget_allows(AsterCommandPriority::Optional, 2, now_ns);
                if non_urgent && (self.exec_queue_low_for_optional_work() || !full_replace_budget_ok) {
                    debug!(
                        "exec queue/budget backpressure: skipping optional replace for {market} {side:?} ({}) (capacity={} budget_ok={})",
                        reason.as_str(),
                        self.exec_tx.capacity(),
                        full_replace_budget_ok
                    );
                    return;
                }
                if !non_urgent && (self.exec_queue_low_for_optional_work() || !full_replace_budget_ok) {
                    // Under backpressure, prefer a cancel-only risk reduction over adding a cancel+place
                    // replace. This drains stale/unprofitable exposure while preserving queue reserve.
                    let target = self.cancel_target(market, side, now_ns);
                    let CancelTarget::Send { client_id, venue_order_id } = target else {
                        return;
                    };
                    let cmd = ExecCommand::Cancel {
                        market: market.clone(),
                        side,
                        client_id,
                        venue_order_id,
                    };
                    match self.try_send_aster_cmd(cmd, AsterCommandPriority::RiskReducing, now_ns) {
                        ExecDispatch::Sent => {
                            self.orders.on_cancel_sent(market, side, now_ns);
                            self.journal.record(now_ns, "cancel", Some(market.0.clone()), serde_json::json!({"side": side.as_str(), "reason": "BACKPRESSURE_CANCEL_ONLY"}));
                        }
                        ExecDispatch::BudgetBlocked => {
                            self.note_aster_budget_block(now_ns, "urgent_cancel_only_budget_blocked", AsterCommandPriority::RiskReducing);
                            self.freeze_and_sweep(now_ns, "aster_command_budget_exhausted");
                        }
                        ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                            warn!("exec queue full/closed: backpressure cancel-only NOT sent for {market} {side:?}; freezing + safety sweep");
                            self.freeze_and_sweep(now_ns, "exec_queue_send_failed");
                        }
                    }
                    return;
                }
                if let Some(new_cid) = self.orders.next_client_id(market, side) {
                    let cmd = ExecCommand::Replace {
                        market: market.clone(),
                        side,
                        old_client_id: old_cid,
                        old_venue_order_id: old_voi,
                        new_client_id: new_cid.clone(),
                        price_ticks,
                        qty_lots,
                    };
                    let priority = if non_urgent { AsterCommandPriority::Optional } else { AsterCommandPriority::RiskReducing };
                    match self.try_send_aster_cmd(cmd, priority, now_ns) {
                        ExecDispatch::Sent => {
                            // Keep the old client id active until its cancel is verified. The worker
                            // emits CancelAck(old) before PlaceAck(new); only then do we promote the
                            // replacement to PendingPlace. This preserves fill/cancel attribution during
                            // the cancel-then-place race window.
                            self.orders.on_replace_sent(market, side, new_cid, price_ticks, qty_lots, now_ns);
                            self.clear_aster_touch_guard(market, side, now_ns);
                            self.journal.record(now_ns, "replace", Some(market.0.clone()), serde_json::json!({"side": side.as_str(), "reason": reason.as_str(), "price": desired.price.to_string()}));
                        }
                        ExecDispatch::BudgetBlocked if non_urgent => {
                            debug!("Aster command budget/backoff: optional replace deferred for {market} {side:?} ({})", reason.as_str());
                        }
                        ExecDispatch::BudgetBlocked => {
                            self.note_aster_budget_block(now_ns, "urgent_replace_budget_blocked", AsterCommandPriority::RiskReducing);
                            self.freeze_and_sweep(now_ns, "aster_command_budget_exhausted");
                        }
                        ExecDispatch::QueueFull if non_urgent => {
                            warn!("exec queue full: optional replace deferred for {market} {side:?} ({})", reason.as_str());
                        }
                        ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                            warn!("exec queue full/closed: replace NOT sent for {market} {side:?}; freezing + safety sweep");
                            self.freeze_and_sweep(now_ns, "exec_queue_send_failed");
                        }
                    }
                }
            }
        }
    }

    /// Handle an Aster maker fill (live: from the user stream; paper: synthesized).
    /// Exactly-once hedging (invariant 4): a deduped repeat is ignored. Triggers the
    /// post-trade cooldown and cancels the residual on that side (§4.1, §8.4).
    pub async fn handle_maker_fill(&mut self, fill: AsterFill, now_ns: i64) {
        // ATTRIBUTION (live only): only a fill from THIS session's bot order may hedge. A foreign /
        // manual / prior-run order on the same symbol must NEVER trigger an HL hedge. (Paper fills are
        // synthesized from our own slots, so the check is skipped there.)
        if self.exec_mode.sends_real_orders() && !self.orders.is_own_client_id(&fill.client_id) {
            warn!("ignoring non-bot Aster fill on {} (client_id {:?}) — not this session's order", fill.market, fill.client_id);
            return;
        }
        if !self.dedup.observe(&fill) {
            debug!("duplicate fill ignored (already hedged): order={} trade={}", fill.order_id, fill.trade_id);
            return;
        }
        // 1. Update predicted Aster position (signed by side), and close the local maker
        // slot immediately when the venue reports the order is fully filled. Otherwise
        // cancel_both_sides() would send a cancel for an already-FILLED order; Aster can
        // answer FILLED/EXPIRED, which this strategy correctly treats as ambiguous and
        // freezes. A normal full fill should hedge + cancel the opposite side, not self-freeze.
        let signed = SignedPosition::signed(fill.aster_side, fill.last_fill_qty);
        self.aster_pos.entry(fill.market.clone()).or_default().apply_fill(signed, fill.last_fill_px);
        if let Some(ctx) = self.ctx.get(&fill.market) {
            let cum_filled_lots = ctx.scale.qty_to_lots(fill.cum_filled_qty);
            self.orders.on_maker_fill_progress(&fill.market, fill.aster_side, &fill.client_id, cum_filled_lots);
        }
        self.margin_suppressed.remove(&(fill.market.clone(), Side::Buy));
        self.margin_suppressed.remove(&(fill.market.clone(), Side::Sell));
        // Stamp the last hot action for this market: a reconcile snapshot whose reads BEGAN before
        // now cannot yet reflect this fill (or the hedge we are about to fire), so the orphan
        // backstop must ignore it (T2.2 straddle guard). Stamped for reduce-only fills too — they
        // also move the Aster position the backstop reads.
        self.last_hot_action_ns.insert(fill.market.clone(), now_ns);
        // 2. Start cooldown (any execution event, §6).
        self.cooldown.trigger(now_ns, self.cooldown_ns, &fill.market);
        self.account.hot.set_cooldown_until_ns(self.cooldown.hot_mirror_until_ns());
        // A REDUCE-ONLY fill is one of OUR OWN flatten/recovery closes: it already reduced the
        // position in step 1, and it must NOT trigger a new hedge — doing so would loop
        // (hedge → its fill → flatten → its reduce-only fill → hedge → …). Stop here.
        if fill.reduce_only {
            debug!("reduce-only fill (flatten/recovery close) on {}: position updated, no hedge", fill.market);
            return;
        }
        // PRIMARY HEDGE PATH (the money-making path; orphan correction is NOT this). Fold the fill
        // into pending inventory and hedge on HL the MOMENT the accumulated net clears the HL minimum.
        // A full ~$12 clip hedges immediately; small partials accumulate into ONE hedgeable chunk —
        // never a per-partial taker flatten. Opposite fills net down (booking realized PnL). A residual
        // that genuinely lingers is flattened later in `on_tick` (exceptional). All cheap Decimal math.
        let (hl_min_notional, hl_qty_step) = match self.ctx.get(&fill.market) {
            Some(c) => (c.spec.hl_min_notional, c.spec.hl_qty_step),
            None => {
                // Unknown market (should not happen — we have ctx for every spec): still pull both
                // resting sides for post-fill safety, then stop.
                self.cancel_both_sides(&fill.market, now_ns);
                return;
            }
        };
        let hl_mark_book = self.fresh_hl_quote_book(&fill.market, now_ns);
        let mark = hl_mark_book
            .as_ref()
            .and_then(|b| b.book.mid())
            .unwrap_or(fill.last_fill_px);
        if mark <= Decimal::ZERO {
            warn!("invalid non-positive HL mark {} for {} fill; freezing (backstop recovers the leg)", mark, fill.market);
            self.cancel_both_sides(&fill.market, now_ns);
            self.freeze(now_ns, "invalid_hl_mark_at_fill");
            return;
        }
        let fee_rate = self.cfg.edge.aster_maker_fee_bps / Decimal::from(10_000);
        let normal_slip = self.cfg.live.hyperliquid.normal_slippage_bps;
        let rules = HedgeabilityRules { hyperliquid_min_notional: hl_min_notional, hyperliquid_qty_step: hl_qty_step };

        let prev = self.pending.remove(&fill.market);
        let outcome = inventory::handle_fill_parts(fill.aster_side, fill.last_fill_qty, fill.last_fill_px, Utc::now(), prev, &rules, mark, fee_rate);

        if let Some(hedge) = outcome.hedge {
            // The accumulated net is hedgeable. Deterministic cloid from the triggering fill identity;
            // the third component is the fill's CUMULATIVE filled qty (strictly increasing per
            // order_id) — NOT the net hedge qty, which can repeat across flushes and collide
            // (overwriting an in-flight hedge + a reused-cloid HL reject → an unhedged leg).
            let cloid = super::ids::Cloid::hedge(&fill.order_id, &fill.trade_id, super::fills::cum_scaled(fill.cum_filled_qty));
            let cloid_hex = cloid.to_hex();
            // Price the IOC to CROSS the current executable HL touch (NOT mid, NOT the Aster fill
            // price). The temporary book guard drops at the end of this statement, before any &mut self.
            let hl_hedge_book = self.fresh_hl_hedge_book_hot_first(&fill.market, now_ns, hedge.hedge_side, hedge.qty);
            let aggressive_px = hl_hedge_book
                .as_ref()
                .and_then(|ob| crossing_hedge_px(ob.book.as_ref(), hedge.hedge_side, normal_slip));
            let mut intent = HedgeIntent::with_qty(cloid, fill.market.clone(), hedge.hedge_side, hedge.qty, hedge.avg_aster_px, now_ns);
            let Some(aggressive_px) = aggressive_px else {
                // No fresh HL touch to cross — do NOT hedge off a stale fallback price (that is the
                // reject we observed live). Keep an explicit UNKNOWN hedge obligation so the maker
                // gate closes for the right reason and the orphan-recovery backstop has a durable
                // record of the exact unhedged quantity instead of relying only on later snapshots.
                warn!("no HL book touch for {} {:?} hedge; freezing (backstop recovers the leg)", fill.market, hedge.hedge_side);
                intent.mark_unknown();
                self.hedges.insert(cloid_hex.clone(), intent);
                self.cancel_both_sides(&fill.market, now_ns);
                self.journal.record(now_ns, "hedge_unknown", Some(fill.market.0.clone()), serde_json::json!({"reason": "no_hl_touch_at_fill", "qty": hedge.qty.to_string(), "side": hedge.hedge_side.as_str(), "cloid": cloid_hex}));
                self.freeze(now_ns, "no_hl_touch_at_fill");
                return;
            };
            intent.mark_submitted(now_ns);
            // Insert BEFORE the send (invariant): a HedgeAck/Fill can't be processed until this fn
            // returns (single task), so the map always knows the cloid first.
            self.hedges.insert(cloid_hex.clone(), intent.clone());
            // >>> FIRE THE HEDGE NOW — before cancels/journaling. This is the latency-critical wire
            //     send; the bookkeeping below must never sit ahead of it on the hot path. <<<
            let dispatch_err = self
                .hedge_tx
                .try_send(HedgeCommand::Hedge { intent, aggressive_px, slippage_bps: normal_slip, emergency: false })
                .err();
            let hedge_dispatch_ok = dispatch_err.is_none();
            // Cancel BOTH resting sides immediately after the hedge (post-fill cooldown ⇒ neither
            // should rest); kept right after the send so the resting-order window stays tiny.
            self.cancel_both_sides(&fill.market, now_ns);
            // Journal (cold audit trail) AFTER the hedge is on the wire.
            if let Some(hl_context) = &hl_hedge_book {
                let aster_touch = self
                    .fresh_aster_touch_book(&fill.market, now_ns)
                    .and_then(|selected| {
                        aster_fill_touch_context(
                            selected,
                            fill.aster_side,
                            fill.last_fill_px,
                            self.cfg.quote.min_aster_touch_distance_bps,
                        )
                    });
                self.journal.record(
                    now_ns,
                    "fill_hedge_context",
                    Some(fill.market.0.clone()),
                    serde_json::json!({
                        "order_id": fill.order_id.clone(),
                        "trade_id": fill.trade_id.clone(),
                        "client_id": fill.client_id.clone(),
                        "cloid": cloid_hex.clone(),
                        "aster_side": fill.aster_side.as_str(),
                        "hedge_side": hedge.hedge_side.as_str(),
                        "qty": hedge.qty.to_string(),
                        "aster_fill_px": hedge.avg_aster_px.to_string(),
                        "last_fill_px": fill.last_fill_px.to_string(),
                        "hedge_dispatch_ok": hedge_dispatch_ok,
                        "hl": hl_hedge_context_json(hl_context, aggressive_px, normal_slip),
                        "aster": aster_fill_touch_context_json(aster_touch.as_ref()),
                    }),
                );
            }
            if let Some(rec) = &outcome.netted {
                self.journal.record(now_ns, "net", Some(fill.market.0.clone()), serde_json::json!({"closed_qty": rec.closed_qty.to_string(), "realized_pnl": rec.realized_pnl.to_string()}));
            }
            self.journal.record(now_ns, "fill", Some(fill.market.0.clone()), serde_json::json!({"side": hedge.hedge_side.as_str(), "qty": hedge.qty.to_string(), "avg_aster_px": hedge.avg_aster_px.to_string(), "cloid": cloid_hex}));
            // A dropped dispatch (queue full / worker wedged) must NOT be swallowed — mark the intent
            // dangerous + freeze so the backstop recovers it (rather than the slow timeout). This is
            // the ONLY place a primary dispatch can fail; it is exceptional.
            if let Some(e) = dispatch_err {
                warn!("hedge dispatch failed for {cloid_hex} ({e}); marking orphan + freezing (backstop will recover)");
                if let Some(h) = self.hedges.get_mut(&cloid_hex) {
                    h.mark_unknown();
                }
                self.freeze(now_ns, "hedge_dispatch_failed");
            }
            // pending was flushed (outcome.pending is None).
        } else {
            // No hedge this fill: still cancel both resting sides + journal any netting; keep the
            // sub-min residual accumulating if present (the common, cheap case — never a taker flatten).
            self.cancel_both_sides(&fill.market, now_ns);
            if let Some(rec) = &outcome.netted {
                self.journal.record(now_ns, "net", Some(fill.market.0.clone()), serde_json::json!({"closed_qty": rec.closed_qty.to_string(), "realized_pnl": rec.realized_pnl.to_string()}));
            }
            if let Some(inv) = outcome.pending {
                self.pending.insert(fill.market.clone(), inv);
            }
            // (no hedge & no pending) ⇒ the fill netted exactly flat: nothing else to do.
        }
    }

    /// Cancel BOTH resting maker sides for `market` via TARGETED per-order cancels (§8.4
    /// cancel-opposite). NOT `CancelMarket` (allOpenOrders) — that emits no per-order ack, so the
    /// slot tracking would desync in live; each targeted Cancel emits a CancelAck that closes the
    /// slot via `close_by_client_id` in BOTH paper and live. Called after a fill (the post-fill
    /// cooldown means neither side should rest while we hedge).
    fn cancel_both_sides(&mut self, market: &MarketId, now_ns: i64) {
        for side in [Side::Buy, Side::Sell] {
            let target = self.cancel_target(market, side, now_ns);
            let CancelTarget::Send { client_id, venue_order_id } = target else {
                continue;
            };
            // Dispatch FIRST; a dropped post-fill cancel leaves a maker order resting (could
            // re-fill) while local state says cancelled — escalate to a freeze, never silent.
            let cmd = ExecCommand::Cancel { market: market.clone(), side, client_id, venue_order_id };
            match self.try_send_aster_cmd(cmd, AsterCommandPriority::RiskReducing, now_ns) {
                ExecDispatch::Sent => self.orders.on_cancel_sent(market, side, now_ns),
                ExecDispatch::BudgetBlocked => {
                    self.note_aster_budget_block(now_ns, "post_fill_cancel_budget_blocked", AsterCommandPriority::RiskReducing);
                    self.freeze_and_sweep(now_ns, "aster_command_budget_exhausted");
                }
                ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                    error!("CRITICAL: post-fill cancel for {market} {side:?} dropped (queue full/closed); freezing");
                    self.freeze_and_sweep(now_ns, "exec_queue_send_failed");
                }
            }
        }
    }

    /// Paper/shadow maker-fill detection from an Aster trade print. An OPTIMISTIC model
    /// (ignores queue position — the research-grade fill sim is the dry-run `SimEngine`): our
    /// resting BID fills when a sell-aggressor (`buyer_is_maker`) print lands at/below it; our
    /// resting ASK fills when a buy-aggressor print lands at/above it. In live mode this is a
    /// no-op — real fills arrive on the Aster user stream.
    pub async fn handle_trade_print(&mut self, t: TradePrint, now_ns: i64) {
        if self.exec_mode.sends_real_orders() {
            return; // live: user stream owns fills
        }
        let Some(scale) = self.ctx.get(&t.market).map(|c| c.scale.clone()) else { return };
        for side in [Side::Buy, Side::Sell] {
            let Some(slot) = self.orders.slot(&t.market, side) else { continue };
            if !slot.is_live() || slot.client_id.is_none() {
                continue;
            }
            let our_px = scale.ticks_to_price(slot.price_ticks);
            let remaining_lots = slot.remaining_lots();
            if remaining_lots <= 0 {
                continue;
            }
            let prior_cum_qty = scale.lots_to_qty(slot.filled_lots);
            let our_qty = scale.lots_to_qty(remaining_lots);
            let crosses = match side {
                Side::Buy => t.buyer_is_maker && t.price <= our_px, // sell aggressor hits our bid
                Side::Sell => !t.buyer_is_maker && t.price >= our_px, // buy aggressor lifts our ask
            };
            if !crosses {
                continue;
            }
            let fill_qty = t.qty.min(our_qty);
            if fill_qty <= Decimal::ZERO {
                continue;
            }
            self.synthetic_trade_seq += 1;
            let fill = AsterFill {
                market: t.market.clone(),
                aster_side: side,
                order_id: slot.client_id.clone().unwrap_or_default(),
                trade_id: format!("paper-{}", self.synthetic_trade_seq),
                client_id: slot.client_id.clone().unwrap_or_default(),
                last_fill_qty: fill_qty,
                last_fill_px: our_px,
                cum_filled_qty: prior_cum_qty + fill_qty,
                event_time_ms: 0,
                reduce_only: false,
            };
            self.handle_maker_fill(fill, now_ns).await;
            return; // one fill per print is enough for the paper model
        }
    }

    /// Fold a worker/venue event back into the order + hedge state.
    pub fn handle_exec_event(&mut self, ev: ExecEvent, now_ns: i64) {
        match ev {
            ExecEvent::PlaceAck { client_id, venue_order_id } => {
                if let Some((market, side, cancel_venue_order_id, cancel_reason)) =
                    self.ack_by_client_id(&client_id, venue_order_id)
                {
                    warn!(
                        "replacement {client_id} acked after {}; cancelling it immediately",
                        cancel_reason.as_str()
                    );
                    let cmd = ExecCommand::Cancel {
                        market: market.clone(),
                        side,
                        client_id: client_id.clone(),
                        venue_order_id: cancel_venue_order_id,
                    };
                    if self.sweep_pending.is_some() {
                        warn!(
                            "cancel-after-ack for {market} {side:?} suppressed while safety sweep is pending; sweep owns cleanup"
                        );
                    } else {
                        match self.try_send_aster_cmd(cmd, AsterCommandPriority::RiskReducing, now_ns) {
                            ExecDispatch::Sent => {
                                self.orders.on_cancel_sent(&market, side, now_ns);
                                self.journal.record(
                                    now_ns,
                                    "cancel_after_ack",
                                    Some(market.0),
                                    serde_json::json!({
                                        "side": side.as_str(),
                                        "client_id": client_id,
                                        "reason": cancel_reason.as_str(),
                                    }),
                                );
                            }
                            ExecDispatch::BudgetBlocked => {
                                self.note_aster_budget_block(now_ns, "cancel_after_ack", AsterCommandPriority::RiskReducing);
                                self.freeze(now_ns, "cancel_after_ack_budget_blocked");
                                self.request_safety_sweep(now_ns, "cancel_after_ack_budget_blocked");
                            }
                            ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                                error!(
                                    "CRITICAL: cancel-after-ack for {market} {side:?} dropped (queue/backpressure); freezing + safety sweep"
                                );
                                self.freeze(now_ns, "cancel_after_ack_dispatch_failed");
                                self.request_safety_sweep(now_ns, "cancel_after_ack_dispatch_failed");
                            }
                        }
                    }
                }
            }
            ExecEvent::PlaceReject { client_id, reason } => {
                warn!("place rejected (client {client_id}): {reason}");
                let slot_info = self.find_slot_by_client(&client_id);
                self.close_by_client_id(&client_id);
                if reason.to_ascii_lowercase().contains("insufficient") {
                    if let Some((m, side)) = slot_info {
                        if !self.margin_suppressed.contains_key(&(m.clone(), side)) {
                            warn!("margin suppressed {m} {side:?}: {reason}");
                        }
                        self.margin_suppressed.insert((m, side), now_ns);
                    }
                }
            }
            ExecEvent::PlaceUnknown { client_id, reason } => {
                // The order may be resting. Do NOT close the local slot. Freeze and
                // sweep/reconcile so account/openOrders becomes the source of truth.
                warn!("place outcome UNKNOWN (client {client_id}): {reason}; sweeping all bot orders + freezing");
                self.freeze(now_ns, "place_unknown");
                self.request_safety_sweep(now_ns, "place_unknown");
            }
            ExecEvent::CancelAck { client_id } => {
                self.cancel_ack_by_client_id(&client_id);
            }
            ExecEvent::CancelReject { client_id, reason } => {
                // The cancel FAILED, so the order may still be resting. Freeze and request a
                // cancel-all, but do NOT forget local slots until a newer account snapshot proves
                // no bot-owned Aster orders remain.
                warn!("cancel REJECTED (client {client_id}): {reason}; sweeping all bot orders + freezing");
                self.freeze(now_ns, "cancel_rejected");
                self.request_safety_sweep(now_ns, "cancel_rejected");
            }
            ExecEvent::MakerFill(_fill) => {
                // Routed through handle_maker_fill by the driver; nothing here.
            }
            ExecEvent::AsterRateLimited { reason, backoff_ms } => {
                self.on_aster_rate_limited(now_ns, reason, backoff_ms);
            }
            ExecEvent::HedgeAck { cloid, hl_oid } => {
                if let Some(h) = self.hedges.get_mut(&cloid.to_hex()) {
                    h.mark_acked(hl_oid);
                }
            }
            ExecEvent::HedgeFill { cloid, filled_qty, px, fee_usd } => {
                let cloid_hex = cloid.to_hex();
                // The venue qty step for this hedge's market. A complete hedge can be reported across
                // MULTIPLE HedgeFill events, or leave a sub-step rounding remainder, so "filled to
                // within one qty step" MUST count as fully Filled. Otherwise the intent latches
                // `PartiallyFilled` (a dangerous/orphan state) and silently closes the maker gate
                // forever even though the leg is fully hedged on-venue — the stuck-quoting bug.
                let step = self
                    .hedges
                    .get(&cloid_hex)
                    .and_then(|h| self.ctx.get(&h.market))
                    .map(|c| c.spec.hl_qty_step)
                    .unwrap_or(Decimal::ZERO);
                let mut journal_event = None;
                if let Some(h) = self.hedges.get_mut(&cloid_hex) {
                    h.apply_fill(filled_qty);
                    let signed = SignedPosition::signed(h.hedge_side, filled_qty);
                    self.hl_pos.entry(h.market.clone()).or_default().apply_fill(signed, px);
                    journal_event = Some((
                        h.market.0.clone(),
                        h.hedge_side.as_str().to_string(),
                    ));
                    if h.state == super::fills::HedgeState::Filled || h.remaining_qty() <= step {
                        h.mark_reconciled();
                    }
                }
                if let Some((market, side)) = journal_event {
                    self.journal.record(
                        now_ns,
                        "hedge_fill",
                        Some(market),
                        serde_json::json!({
                            "cloid": cloid_hex,
                            "side": side,
                            "qty": filled_qty.to_string(),
                            "px": px.to_string(),
                            "fee_usd": fee_usd.to_string(),
                        }),
                    );
                }
            }
            ExecEvent::HedgeUnknown { cloid, reason } => {
                let cloid_hex = cloid.to_hex();
                warn!("hedge outcome UNKNOWN (cloid {cloid_hex}): {reason}; freezing until reconcile");
                let market = self.hedges.get_mut(&cloid_hex).map(|h| {
                    h.mark_unknown();
                    h.market.0.clone()
                });
                if let Some(market) = market {
                    self.journal.record(
                        now_ns,
                        "hedge_unknown",
                        Some(market),
                        serde_json::json!({"cloid": cloid_hex, "reason": reason}),
                    );
                }
                self.freeze(now_ns, "hedge_unknown");
            }
            ExecEvent::HedgeReject { cloid, reason } => {
                let cloid_hex = cloid.to_hex();
                warn!("hedge rejected (cloid {cloid_hex}): {reason}");
                // Safe to auto-retry ONLY when NOTHING can have landed on HL (a fresh-touch
                // emergency resend, ~1 RTT vs the slow ~4–6 s recovery, no double-hedge risk):
                // the venue-confirmed IOC no-fill ("could not immediately match") and the
                // NotSent fast-fail (frame never left the socket, nonce rolled back). NOT
                // "unexpectedly resting" (an order IS on the book → retrying would double up —
                // freeze instead) and NOT an ambiguous transport error (might have landed →
                // freeze + let the reconciler resolve).
                let definitive_no_fill =
                    super::exec::hyperliquid::hedge_reject_is_definitive_no_fill(&reason);
                let info = self
                    .hedges
                    .get(&cloid_hex)
                    .map(|h| (h.market.clone(), h.hedge_side, h.remaining_qty(), h.attempts));
                if let Some((market, hedge_side, qty, attempts)) = info {
                    if definitive_no_fill && attempts < 2 && qty > Decimal::ZERO {
                        let emerg_slip = self.cfg.live.hyperliquid.emergency_slippage_bps;
                        let fresh_book = self.fresh_hl_hedge_book_hot_first(&market, now_ns, hedge_side, qty);
                        let fresh_px = fresh_book
                            .as_ref()
                            .and_then(|ob| crossing_hedge_px(ob.book.as_ref(), hedge_side, emerg_slip));
                        if let Some(px) = fresh_px {
                            // Reuse the SAME deterministic cloid + intent (idempotent: the original
                            // never landed). mark_submitted bumps attempts; clone before the send so
                            // the &mut borrow is released.
                            let intent = self.hedges.get_mut(&cloid_hex).map(|h| {
                                h.mark_submitted(now_ns);
                                h.clone()
                            });
                            if let Some(intent) = intent {
                                let cmd = HedgeCommand::Hedge { intent, aggressive_px: px, slippage_bps: emerg_slip, emergency: true };
                                if self.hedge_tx.try_send(cmd).is_ok() {
                                    if let Some(ctx) = &fresh_book {
                                        self.journal.record(
                                            now_ns,
                                            "hedge_retry_context",
                                            Some(market.0.clone()),
                                            serde_json::json!({
                                                "cloid": cloid_hex.clone(),
                                                "reason": reason.clone(),
                                                "attempt": attempts + 1,
                                                "side": hedge_side.as_str(),
                                                "qty": qty.to_string(),
                                                "hl": hl_hedge_context_json(ctx, px, emerg_slip),
                                            }),
                                        );
                                    }
                                    warn!("hedge {cloid_hex} retry #{} off fresh touch @ {px} (emergency {emerg_slip} bps)", attempts + 1);
                                    return;
                                }
                                if let Some(h) = self.hedges.get_mut(&cloid_hex) {
                                    h.mark_unknown();
                                }
                            }
                        }
                    }
                }
                // No retry (ambiguous / exhausted / no touch): mark rejected + freeze. Orphan-leg danger.
                if let Some(h) = self.hedges.get_mut(&cloid_hex) {
                    h.mark_rejected();
                }
                self.freeze(now_ns, "hedge_rejected");
            }
            ExecEvent::AsterFlattenAck { market, side, qty } => {
                self.journal.record(
                    now_ns,
                    "aster_flatten_ack",
                    Some(market.0),
                    serde_json::json!({"side": side.as_str(), "qty": qty.to_string()}),
                );
            }
            ExecEvent::AsterFlattenReject { market, side, qty, reason } => {
                error!("aster flatten rejected on {market}: {side:?} {qty}: {reason}");
                self.freeze(now_ns, "aster_flatten_rejected");
                self.journal.record(
                    now_ns,
                    "aster_flatten_reject",
                    Some(market.0),
                    serde_json::json!({"side": side.as_str(), "qty": qty.to_string(), "reason": reason}),
                );
            }
            ExecEvent::HlFlattenFill { market, side, filled_qty, px } => {
                let signed = SignedPosition::signed(side, filled_qty);
                self.hl_pos.entry(market.clone()).or_default().apply_fill(signed, px);
                self.journal.record(
                    now_ns,
                    "hl_flatten_fill",
                    Some(market.0),
                    serde_json::json!({"side": side.as_str(), "qty": filled_qty.to_string(), "px": px.to_string()}),
                );
            }
            ExecEvent::HlFlattenReject { market, side, qty, reason } => {
                error!("hl flatten rejected on {market}: {side:?} {qty}: {reason}");
                self.freeze(now_ns, "hl_flatten_rejected");
                self.journal.record(
                    now_ns,
                    "hl_flatten_reject",
                    Some(market.0),
                    serde_json::json!({"side": side.as_str(), "qty": qty.to_string(), "reason": reason}),
                );
            }
        }
    }

    /// Ack a placed order by client id. Returns `(market, side, venue_order_id)` when this ack
    /// belongs to a replacement that was already marked for post-fill/gate-close cancellation.
    fn ack_by_client_id(
        &mut self,
        client_id: &str,
        venue_order_id: String,
    ) -> Option<(MarketId, Side, Option<String>, CancelAfterAckReason)> {
        if let Some((m, side)) = self.find_slot_by_client(client_id) {
            if let Some(cancel_after_ack_reason) = self.orders.on_acked(&m, side, venue_order_id) {
                let venue_order_id = self
                    .orders
                    .slot(&m, side)
                    .and_then(|slot| slot.venue_order_id.clone());
                return Some((m, side, venue_order_id, cancel_after_ack_reason));
            }
        }
        None
    }
    fn cancel_ack_by_client_id(&mut self, client_id: &str) {
        if let Some((m, side)) = self.find_slot_by_client(client_id) {
            self.orders.on_cancel_acked(&m, side);
        }
    }
    fn close_by_client_id(&mut self, client_id: &str) {
        if let Some((m, side)) = self.find_slot_by_client(client_id) {
            self.orders.on_closed(&m, side);
        }
    }
    fn find_slot_by_client(&self, client_id: &str) -> Option<(MarketId, Side)> {
        for m in &self.markets {
            for side in [Side::Buy, Side::Sell] {
                if self.orders.slot(m, side).and_then(|s| s.client_id.as_deref()) == Some(client_id) {
                    return Some((m.clone(), side));
                }
            }
        }
        None
    }

    /// Whether any in-flight hedge is in a dangerous (orphan) state — feeds the risk gate.
    pub fn has_orphan_hedge(&self) -> bool {
        self.hedges.values().any(|h| h.state.is_dangerous())
    }

    /// Cumulative-loss circuit breaker (live only). Measures TOTAL cross-venue MARKED equity
    /// (Aster wallet+unrealized + Lighter portfolio_value + marked Lighter uPnL) against a
    /// baseline armed from the median of the first [`BREAKER_BASELINE_SAMPLES`] fresh marked
    /// snapshots; a drawdown beyond `live.circuit_breaker.max_cumulative_loss_usdc` must
    /// persist for [`BREAKER_TRIP_STREAK`] consecutive fresh marked samples before tripping
    /// (accepted trade-off: a true catastrophic drawdown trips ~4-6s later — the breaker only
    /// cancels quotes and halts, positions stay open regardless — in exchange for immunity to
    /// single-sample venue glitches). On trip: cancels orders via the graceful-shutdown path,
    /// LEAVES the delta-neutral position open, writes a persistent trip latch, and halts the
    /// process (which then refuses to restart until reset, and exits nonzero so the
    /// supervisor safe-halts in one step). Off the money path (cold tick), counting each
    /// published snapshot generation at most once. NEVER trips on untrusted data (no
    /// snapshot yet, stale snapshot, unmarked Lighter uPnL, or non-positive equity).
    fn check_circuit_breaker(&mut self, now_ns: i64) {
        if !self.cfg.live.circuit_breaker.enabled
            || !self.exec_mode.sends_real_orders()
            || self.breaker_tripped
        {
            return;
        }
        let limit = self.cfg.live.circuit_breaker.max_cumulative_loss_usdc;
        let snap = self.account.load();
        if snap.source_ts_ns == 0 || self.account.age_ms(now_ns) > self.cfg.live.max_account_snapshot_age_ms {
            // No snapshot yet or stale — don't trip on data we can't trust, and don't let
            // pre-staleness breaches persist across the gap.
            self.breaker_breach_streak = 0;
            return;
        }
        if snap.generation == self.breaker_last_generation {
            return; // same sample as last time: neither counts toward nor resets the streak
        }
        self.breaker_last_generation = snap.generation;
        if !snap.hl_upnl_marked {
            // The Lighter leg's uPnL could not be marked (missing mark or entry px): the
            // combined equity is the exact distorted metric that false-tripped 2026-07-04.
            // Skip the sample entirely (no trip, no baseline arming, streak reset).
            self.breaker_breach_streak = 0;
            return;
        }
        let equity = snap.total_equity_usd();
        if equity <= Decimal::ZERO {
            // A zero/garbage read (e.g. a failed parse) must never look like a total loss.
            self.breaker_breach_streak = 0;
            return;
        }
        let baseline = match self.breaker_baseline_equity {
            Some(b) => b,
            None => {
                // Arm from the MEDIAN of the first K fresh marked samples: one outlier
                // startup read must not set a phantom reference for the whole run.
                self.breaker_baseline_samples.push(equity);
                if self.breaker_baseline_samples.len() >= BREAKER_BASELINE_SAMPLES {
                    let mut v = self.breaker_baseline_samples.clone();
                    v.sort();
                    let b = v[v.len() / 2];
                    self.breaker_baseline_equity = Some(b);
                    info!(
                        "circuit breaker armed: baseline equity = {b} USD (median of {} samples), limit = {limit} USD",
                        v.len()
                    );
                }
                return;
            }
        };
        let loss = baseline - equity;
        if loss <= limit {
            self.breaker_breach_streak = 0;
            return;
        }
        self.breaker_breach_streak += 1;
        if self.breaker_breach_streak < BREAKER_TRIP_STREAK {
            warn!(
                "circuit breaker: breach {} of {BREAKER_TRIP_STREAK} (loss {loss} USD > limit {limit} USD); not tripping yet",
                self.breaker_breach_streak
            );
            return;
        }
        // TRIP — latch, journal, persist, and halt (graceful drain leaves the position open).
        self.breaker_tripped = true;
        let market = self.markets.first().map(|m| m.0.clone()).unwrap_or_default();
        error!(
            "CIRCUIT BREAKER TRIPPED on {market}: equity {equity} USD is {loss} USD below baseline \
             {baseline} USD (limit {limit} USD). Cancelling orders, LEAVING positions open, halting."
        );
        self.journal.record(
            now_ns,
            "circuit_trip",
            Some(market.clone()),
            serde_json::json!({
                "baseline_usd": baseline.to_string(),
                "equity_usd": equity.to_string(),
                "loss_usd": loss.to_string(),
                "limit_usd": limit.to_string(),
                // Components + freshness, so the next forensic run reads straight off the row.
                "aster_equity_usd": snap.aster_equity_usd.to_string(),
                "hl_equity_usd": snap.hl_equity_usd.to_string(),
                "hl_unrealized_usd": snap.hl_unrealized_usd.to_string(),
                "snapshot_age_ms": self.account.age_ms(now_ns),
                "read_start_age_ms": now_ns.saturating_sub(snap.read_start_ns) / 1_000_000,
                "breach_streak": self.breaker_breach_streak,
                "generation": snap.generation,
            }),
        );
        if let Some(path) = self.trip_file_path.clone() {
            let rec = super::breaker::TripRecord {
                ts_utc: Utc::now().to_rfc3339(),
                market,
                baseline_usd: baseline,
                equity_usd: equity,
                loss_usd: loss,
                limit_usd: limit,
                reason: "cumulative loss exceeded max_cumulative_loss_usdc".to_string(),
            };
            if let Err(e) = super::breaker::write_trip(&path, &rec) {
                error!("failed to write circuit-breaker trip latch to {}: {e:#}", path.display());
            }
        }
        if self.cfg.live.shutdown_cancel_all {
            self.request_safety_sweep(now_ns, "circuit_breaker");
        }
        self.freeze(now_ns, "circuit_breaker");
        // Reuse the existing graceful shutdown: run.rs observes this token, retries CancelAllBot
        // with a bounded awaited send, and exits. Positions are LEFT OPEN.
        self.shutdown.cancel();
    }

    /// Periodic maintenance: time out overdue hedges, run the orphan-recovery backstop, and
    /// refresh the dead-man countdown.
    pub async fn on_tick(&mut self, now_ns: i64) {
        // Safety FIRST: the cumulative-loss circuit breaker. If it trips it halts the process; do it
        // before any maintenance so a tripped run does nothing else this tick.
        self.check_circuit_breaker(now_ns);
        self.drive_safety_sweep(now_ns);
        let timeout_ns = self.cfg.live.max_unhedged_age_ms.max(0) * 1_000_000;
        let mut newly_dangerous = false;
        for h in self.hedges.values_mut() {
            let was = h.state.is_dangerous();
            h.check_timeout(now_ns, timeout_ns);
            if !was && h.state.is_dangerous() {
                newly_dangerous = true;
            }
        }
        if newly_dangerous {
            self.freeze(now_ns, "hedge_timeout");
        }
        // Flatten any sub-min PENDING residual that has genuinely lingered (aged out) or grown too
        // large — the exceptional case where accumulation never reached a hedgeable chunk. The
        // normal case never reaches here (a pending residual is flushed to a hedge by the next
        // fill). Reduce-only on Aster keeps it delta-neutral.
        if self.exec_mode.sends_real_orders() {
            let max_notional = self.cfg.live.partials.max_pending_notional_usd;
            let max_age_ms = self.cfg.live.partials.max_pending_age_ms;
            let now_utc = Utc::now();
            let stuck: Vec<(MarketId, Side, Decimal)> = self
                .pending
                .iter()
                .filter_map(|(m, inv)| {
                    let mark = self
                        .book(m, VenueTag::Hyperliquid)
                        .and_then(|b| b.mid())
                        .unwrap_or(inv.avg_aster_px);
                    inventory::check_pending_limits(inv, max_notional, max_age_ms, mark, now_utc).map(|_| {
                        let side = if inv.signed_qty > Decimal::ZERO { Side::Sell } else { Side::Buy };
                        (m.clone(), side, inv.signed_qty.abs())
                    })
                })
                .collect();
            let snap_src = self.account.load().source_ts_ns;
            for (m, side, qty) in stuck {
                self.pending.remove(&m);
                // Share the recovery throttle so recover_orphans (same tick / next ticks) does NOT
                // re-flatten this residual before the FlattenAster lands in a newer snapshot.
                self.last_recovery.insert(m.clone(), (now_ns, snap_src, None));
                warn!("pending residual on {m} lingered/too-large; flattening {side:?} {qty} reduce-only on Aster (exceptional)");
                self.journal.record(now_ns, "flatten_pending", Some(m.0.clone()), serde_json::json!({"side": side.as_str(), "qty": qty.to_string()}));
                let client_id = self.orders.next_flatten_client_id(&m);
                // Stamp the hot action: a snapshot straddling this flatten's execution must
                // not be trusted by the orphan backstop (same reason as maker fills).
                self.last_hot_action_ns.insert(m.clone(), now_ns);
                match self.try_send_aster_cmd(
                    ExecCommand::FlattenAster { market: m.clone(), side, qty, client_id },
                    AsterCommandPriority::Safety,
                    now_ns,
                ) {
                    ExecDispatch::Sent => {}
                    ExecDispatch::BudgetBlocked => {
                        self.note_aster_budget_block(now_ns, "pending_flatten", AsterCommandPriority::Safety);
                        self.freeze(now_ns, "pending_flatten_budget_blocked");
                    }
                    ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                        error!("CRITICAL: pending-residual FlattenAster for {m} dropped (queue/backpressure); freezing");
                        self.freeze(now_ns, "pending_flatten_dispatch_failed");
                    }
                }
            }
        }
        // Bound the hedge map + clear stale resolved intents (a Reconciled hedge is done).
        self.hedges.retain(|_, h| !h.state.is_resolved());
        // Active orphan recovery: AFTER the timeout pass (so a timed-out hedge is no longer
        // in-flight and its net delta is recoverable here).
        self.recover_orphans(now_ns);
        // Dead-man heartbeat (§3.4): refresh each eligible market's countdown.
        if self.cfg.live.aster.deadman_enabled {
            for m in self.markets.clone() {
                if self.ctx.get(&m).is_some_and(|c| c.eligible) {
                    match self.try_send_aster_cmd(ExecCommand::RefreshDeadman { market: m.clone() }, AsterCommandPriority::Deadman, now_ns) {
                        ExecDispatch::Sent | ExecDispatch::BudgetBlocked | ExecDispatch::QueueFull => {}
                        ExecDispatch::QueueClosed => self.freeze(now_ns, "deadman_queue_closed"),
                    }
                }
            }
        }
    }

    /// Active orphan-recovery backstop (§6/§10) — the safety net the architecture promised.
    /// Using the reconciled snapshot (ground truth) plus in-flight hedge deltas, detect any
    /// persistent net delta per market and ACTIVELY neutralize it: hedge the net on HL if it
    /// clears the HL minimum, else flatten each leg reduce-only on its own venue. A
    /// missed/dropped/rejected/timed-out hedge all surface here as a net delta and get RESOLVED,
    /// not merely frozen. Live only; throttled per market so it can't re-fire before the action
    /// lands in the next snapshot; folding in-flight hedges prevents a double-hedge in the normal
    /// post-fill window.
    fn recover_orphans(&mut self, now_ns: i64) {
        if !self.exec_mode.sends_real_orders() {
            return; // paper has no real positions
        }
        let snap = self.account.load();
        if snap.source_ts_ns == 0 {
            return; // no snapshot yet
        }
        // Only act on a reasonably fresh snapshot; a stale one is handled by the maker-gate freeze.
        if now_ns.saturating_sub(snap.source_ts_ns) / 1_000_000 > self.cfg.live.max_account_snapshot_age_ms * 2 {
            return;
        }
        let recovery_cooldown_ns = self.cfg.live.max_unhedged_age_ms.max(1000) * 2 * 1_000_000;
        let hl_min = self.cfg.partials.hyperliquid_min_notional;
        let dust = Decimal::new(5, 1); // $0.50: below the acceptance tolerance, above rounding noise
        let emerg_slip = self.cfg.live.hyperliquid.emergency_slippage_bps;

        for m in self.markets.clone() {
            // STRADDLE GUARD (T2.2): trust a snapshot for this market ONLY if its REST reads BEGAN
            // STRICTLY AFTER the market's last hot action (a maker fill / primary hedge dispatch);
            // otherwise skip it. A snapshot whose reads began at-or-before the action cannot yet
            // reflect it, so acting on it — or even seeding the persistence gate from it — could
            // double-hedge during the fast-network window where the fill is visible but the hedge is
            // not. `read_start_ns` is stamped at the START of the reconcile reads (not after, which
            // `source_ts_ns` is). The compare is `<=`, not `<`: `mono_now_ns()` (QueryPerformanceCounter
            // on Windows, ~100ns granularity) can return the SAME value for two calls in one tick, so on
            // a fast VPS `read_start_ns == action_ns` is reachable and means "the read may have queried
            // the venue the same instant the hedge was dispatched, before it propagated" — untrustworthy,
            // so defer. Self-clocking, no constant; mirrors the strictly-newer `heal_confirm` gate. A
            // genuine orphan still recovers: a failed hedge FREEZES quoting → fills stop → last_hot_action
            // stops advancing → a later snapshot's read_start clears the guard. So this only ever DEFERS
            // recovery, never skips it.
            if self.last_hot_action_ns.get(&m).is_some_and(|&action_ns| snap.read_start_ns <= action_ns) {
                continue;
            }
            let mark = self
                .book(&m, VenueTag::Hyperliquid)
                .and_then(|b| b.mid())
                .unwrap_or(Decimal::ZERO);
            if mark <= Decimal::ZERO {
                continue; // no mark ⇒ can't size/judge
            }
            let rep_a = snap.reported_position(super::account::Venue::Aster, &m);
            let rep_h = snap.reported_position(super::account::Venue::Hyperliquid, &m);
            // In-flight hedges will change reality soon; fold their signed remaining qty in so we
            // don't double-hedge during the normal post-fill window.
            let in_flight: Decimal = self
                .hedges
                .values()
                .filter(|h| h.market == m && h.state.is_in_flight())
                .map(|h| SignedPosition::signed(h.hedge_side, h.remaining_qty()))
                .sum();
            // Exclude the legitimately-accumulating sub-min pending inventory: it is EXPECTED to be
            // unhedged (it's batching toward a hedgeable chunk) and is resolved by the accumulation
            // path or the pending-age flatten — NOT an orphan. Subtracting it prevents the backstop
            // from taker-flattening a healthy accumulation (which would lose money).
            let pending_signed = self.pending.get(&m).map(|p| p.signed_qty).unwrap_or(Decimal::ZERO);
            let effective_net = rep_a + rep_h + in_flight - pending_signed;
            let net_notional = effective_net.abs() * mark;
            if net_notional <= dust {
                // Delta-neutral enough. CRITICAL: sync predicted to reported HERE TOO (not only in
                // the orphan branch) before retiring dangerous intents — otherwise a missed
                // reduce-only fill can leave predicted stale within the mismatch tolerance, and the
                // self-heal below would un-freeze on a phantom position. Syncing makes
                // positions_reconciled see ground truth, so the unfreeze is only ever legitimate.
                self.aster_pos.entry(m.clone()).or_default().qty = rep_a;
                self.hl_pos.entry(m.clone()).or_default().qty = rep_h;
                self.hedges.retain(|_, h| !(h.market == m && h.state.is_dangerous()));
                self.orphan_seen.remove(&m); // orphan resolved — reset the persistence record
                continue;
            }
            // ── SNAPSHOT-PREDICTED CROSS-CHECK ──
            // The predicted positions were synced to reported from the PREVIOUS good snapshot (in
            // the dust branch above). If they show balanced (predicted_net ≤ dust) but the CURRENT
            // snapshot disagrees, a venue REST read likely returned stale/empty data (e.g. HL
            // position momentarily reads as 0). Acting on a phantom would round-trip (sell then buy
            // back), burning fees. Skip with a warning; a real orphan shows up in BOTH predicted
            // and snapshot because the missed fill updated aster_pos via the user stream.
            let pred_a = self.aster_pos.get(&m).map(|p| p.qty).unwrap_or_default();
            let pred_h = self.hl_pos.get(&m).map(|p| p.qty).unwrap_or_default();
            let predicted_net = pred_a + pred_h + in_flight - pending_signed;
            if predicted_net.abs() * mark <= dust && net_notional > dust {
                warn!("orphan skip {m}: predicted balanced (pred_a={pred_a} pred_h={pred_h} net={predicted_net}) \
                       but snapshot disagrees (rep_a={rep_a} rep_h={rep_h} eff={effective_net} ${net_notional}) — \
                       likely a transient venue read; deferring");
                self.orphan_seen.remove(&m);
                continue;
            }
            // ── PERSISTENCE GATE ──
            // Only ACT once this orphan has persisted into a STRICTLY NEWER snapshot than first
            // seen (same sign, at least half the size). Filters transient snapshot lag.
            let confirmed = matches!(
                self.orphan_seen.get(&m),
                Some(&(prev_net, prev_src))
                    if snap.source_ts_ns > prev_src
                        && (prev_net > Decimal::ZERO) == (effective_net > Decimal::ZERO)
                        && effective_net.abs() * Decimal::from(2) >= prev_net.abs()
            );
            if !confirmed {
                self.orphan_seen.insert(m.clone(), (effective_net, snap.source_ts_ns));
                continue; // first sighting — wait for a newer snapshot to confirm it's real
            }
            // Confirmed real orphan ⇒ reality (reported) is the truth (events were missed/failed):
            // sync predicted to it so the maker gate / capital / reconcile see the real position.
            self.aster_pos.entry(m.clone()).or_default().qty = rep_a;
            self.hl_pos.entry(m.clone()).or_default().qty = rep_h;
            // ── OUTSTANDING-RECOVERY GUARD ──
            // Never race a second recovery order onto the wire for this market while one is
            // still in flight. Its signed remaining qty is already folded into
            // `effective_net` above, so this only fires in the arithmetic edge where a net
            // remains anyway — skip, do NOT overwrite the outstanding intent's record.
            if self
                .hedges
                .values()
                .any(|h| h.market == m && h.recovery && h.state.is_in_flight())
            {
                warn!("orphan recovery skip {m}: a recovery hedge is still in flight; deferring");
                continue;
            }
            // ── THROTTLE + ANTI-FLIP GUARD ──
            // Re-fire only when the wall-clock cooldown elapsed AND a STRICTLY NEWER snapshot
            // arrived. Additionally: if the recovery would REVERSE direction from the last action
            // (sell→buy or vice-versa) within 3× cooldown, suppress it — a flip that fast is
            // almost certainly a transient snapshot oscillation, not a real orphan reversal.
            let hedge_side = if effective_net > Decimal::ZERO { Side::Sell } else { Side::Buy };
            if let Some(&(last_ns, last_src, last_side)) = self.last_recovery.get(&m) {
                if now_ns.saturating_sub(last_ns) < recovery_cooldown_ns || snap.source_ts_ns <= last_src {
                    continue;
                }
                if let Some(prev_side) = last_side {
                    if prev_side != hedge_side && now_ns.saturating_sub(last_ns) < recovery_cooldown_ns * 3 {
                        warn!("orphan anti-flip {m}: would reverse {prev_side:?}→{hedge_side:?} within 3× cooldown; \
                               suppressing (rep_a={rep_a} rep_h={rep_h} eff={effective_net})");
                        continue;
                    }
                }
            }
            // Diagnostic logging — capture all inputs for post-incident analysis.
            warn!("orphan recovery ACT {m}: rep_a={rep_a} rep_h={rep_h} in_flight={in_flight} \
                   pending={pending_signed} → eff_net={effective_net} (${net_notional}) \
                   pred_a={pred_a} pred_h={pred_h} side={hedge_side:?}");
            self.last_recovery.insert(m.clone(), (now_ns, snap.source_ts_ns, Some(hedge_side)));

            if net_notional >= hl_min {
                let qty = effective_net.abs();
                // Supersede any DANGEROUS (Unknown/timed-out/partial) recovery record for
                // this market before re-dispatching: reality was just re-confirmed by two
                // snapshots and predicted is synced to reported above, so the old record's
                // uncertainty is subsumed — and keeping it would wedge the self-heal's
                // `hedges.is_empty()` condition forever. Journaled for the audit trail.
                let superseded: Vec<String> = self
                    .hedges
                    .iter()
                    .filter(|(_, h)| h.market == m && h.recovery && h.state.is_dangerous())
                    .map(|(hex, _)| hex.clone())
                    .collect();
                for hex in superseded {
                    if let Some(old) = self.hedges.remove(&hex) {
                        self.journal.record(now_ns, "recovery_superseded", Some(m.0.clone()), serde_json::json!({
                            "cloid": hex,
                            "state": old.state.as_str(),
                            "qty": old.qty.to_string(),
                            "filled_qty": old.filled_qty.to_string(),
                        }));
                    }
                }
                // Fresh salted cloid per dispatch — the venue does not dedupe indices, so a
                // reused id against a possibly-live earlier order would cross-attribute fills.
                let attempt = {
                    let seq = self.recovery_attempt_seq.entry(m.clone()).or_insert(0);
                    let cur = *seq;
                    *seq = seq.saturating_add(1);
                    cur
                };
                let cloid = super::ids::Cloid::recovery_attempt(&m, super::fills::cum_scaled(effective_net), attempt);
                let cloid_hex = cloid.to_hex();
                let aggressive_px = cap_aggressive_px(mark, hedge_side, emerg_slip);
                warn!("orphan recovery: net {effective_net} on {m} (${net_notional}); HL hedge {hedge_side:?} {qty} (cloid {cloid_hex} attempt {attempt})");
                self.journal.record(now_ns, "recover_hedge", Some(m.0.clone()), serde_json::json!({"net": effective_net.to_string(), "side": hedge_side.as_str(), "qty": qty.to_string(), "attempt": attempt}));
                let mut intent = HedgeIntent::with_qty(cloid, m.clone(), hedge_side, qty, mark, now_ns);
                intent.recovery = true;
                intent.mark_submitted(now_ns);
                self.hedges.insert(cloid_hex.clone(), intent.clone());
                if self.hedge_tx.try_send(HedgeCommand::Hedge { intent, aggressive_px, slippage_bps: emerg_slip, emergency: true }).is_err() {
                    if let Some(h) = self.hedges.get_mut(&cloid_hex) {
                        h.mark_unknown();
                    }
                    self.freeze(now_ns, "recovery_dispatch_failed");
                }
            } else {
                let orphan_a = rep_a - pending_signed;
                warn!("orphan recovery: sub-min net {effective_net} on {m} (${net_notional}); flatten orphan reduce-only (orphan_a={orphan_a} hl={rep_h})");
                self.journal.record(now_ns, "recover_flatten", Some(m.0.clone()), serde_json::json!({"net": effective_net.to_string(), "orphan_a": orphan_a.to_string(), "hl": rep_h.to_string()}));
                if orphan_a.abs() * mark > dust {
                    let side = if orphan_a > Decimal::ZERO { Side::Sell } else { Side::Buy };
                    let client_id = self.orders.next_flatten_client_id(&m);
                    // Stamp the hot action: a snapshot straddling this flatten must not be
                    // trusted by the backstop (mirrors the maker-fill stamp).
                    self.last_hot_action_ns.insert(m.clone(), now_ns);
                    match self.try_send_aster_cmd(
                        ExecCommand::FlattenAster { market: m.clone(), side, qty: orphan_a.abs(), client_id },
                        AsterCommandPriority::Safety,
                        now_ns,
                    ) {
                        ExecDispatch::Sent => {}
                        ExecDispatch::BudgetBlocked => {
                            self.note_aster_budget_block(now_ns, "recovery_flatten", AsterCommandPriority::Safety);
                            self.freeze(now_ns, "recovery_flatten_budget_blocked");
                        }
                        ExecDispatch::QueueFull | ExecDispatch::QueueClosed => {
                            error!("CRITICAL: recovery FlattenAster for {m} dropped (queue/backpressure); freezing — orphan unresolved");
                            self.freeze(now_ns, "recovery_flatten_dispatch_failed");
                        }
                    }
                }
                if rep_h.abs() * mark > dust {
                    let side = if rep_h > Decimal::ZERO { Side::Sell } else { Side::Buy };
                    let aggressive_px = cap_aggressive_px(mark, side, emerg_slip);
                    if self.hedge_tx.try_send(HedgeCommand::Flatten { market: m.clone(), side, qty: rep_h.abs(), aggressive_px, slippage_bps: emerg_slip }).is_err() {
                        error!("CRITICAL: recovery HL Flatten for {m} dropped (queue full); freezing — orphan unresolved");
                        self.freeze(now_ns, "recovery_flatten_dispatch_failed");
                    }
                }
            }
        }
        // SELF-HEAL: once there is no outstanding hedge work (the map holds only non-resolved
        // intents; empty ⇒ none in-flight or dangerous) and positions reconcile, clear a latched
        // freeze. This lets a TRANSIENT issue the backstop already resolved (a single timed-out or
        // rejected hedge) stop halting the bot — important for a long multi-round live run — while
        // staying conservative: it un-freezes ONLY when verifiably clean + delta-neutral AND the
        // Aster fill stream is fresh (so we trust that corrective fills are being delivered and the
        // synced predicted positions reflect reality, not a silent-stream phantom).
        let stream_fresh = self
            .aster_stream
            .as_ref()
            .is_none_or(|s| s.age_ms(now_ns) <= self.cfg.live.max_user_stream_staleness_ms);
        let no_open_bot_orders = !self.has_open_aster_bot_orders_in(&snap);
        let clean = self.frozen
            && stream_fresh
            && self.hedges.is_empty()
            && self.positions_reconciled()
            && self.sweep_pending.is_none()
            && no_open_bot_orders;
        if clean {
            // T2.1 PERSISTENCE: require the clean condition to hold again in a STRICTLY NEWER snapshot
            // before clearing the freeze, so a transient snapshot lag (positions momentarily reading
            // neutral) can't unfreeze on a phantom-clean reading. Mirrors the `orphan_seen` gate.
            match self.heal_confirm {
                Some(first_src) if snap.source_ts_ns > first_src => {
                    self.frozen = false;
                    self.account.hot.set_trading_allowed(true);
                    self.heal_confirm = None;
                    warn!("freeze cleared (self-healed): clean across two snapshots (no outstanding hedges + reconciled)");
                    self.journal.record(now_ns, "unfreeze", None, serde_json::json!({"reason": "self_healed"}));
                }
                Some(_) => {} // first-seen snapshot not yet superseded — keep waiting
                None => self.heal_confirm = Some(snap.source_ts_ns), // first clean sighting — record it
            }
        } else {
            self.heal_confirm = None; // condition broke (or not frozen) — reset the persistence record
        }
    }
}

const PRIORITY_DRAIN_LIMIT: usize = 64;

/// Drain latency-critical events between markets during a tick/wake reprice sweep.
/// This prevents a maker fill from waiting behind a full all-market reprice batch.
async fn drain_priority_events(
    strat: &mut Strategy,
    exec_events: &mut Receiver<ExecEvent>,
    maker_fills: &mut Receiver<AsterFill>,
    trade_prints: &mut Receiver<TradePrint>,
) {
    for _ in 0..PRIORITY_DRAIN_LIMIT {
        let now_ns = crate::hotpath::clock::mono_now_ns();

        if let Ok(fill) = maker_fills.try_recv() {
            strat.handle_maker_fill(fill, now_ns).await;
            continue;
        }

        if let Ok(ev) = exec_events.try_recv() {
            strat.handle_exec_event(ev, now_ns);
            continue;
        }

        if let Ok(print) = trade_prints.try_recv() {
            strat.handle_trade_print(print, now_ns).await;
            continue;
        }

        break;
    }
}

/// Drive the strategy: wake on a book change (the coalescing registry `Notify`) or a
/// periodic tick, and consume worker events. Runs until `shutdown` resolves.
#[allow(clippy::too_many_arguments)]
pub async fn run_strategy(
    mut strat: Strategy,
    wake: Arc<Notify>,
    mut exec_events: Receiver<ExecEvent>,
    mut maker_fills: Receiver<AsterFill>,
    mut trade_prints: Receiver<TradePrint>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    use crate::hotpath::clock::mono_now_ns;
    info!("strategy loop started ({} markets)", strat.markets.len());
    let mut tick = tokio::time::interval(tokio::time::Duration::from_millis(
        strat.cfg.live.aster.deadman_refresh_ms.max(100) as u64,
    ));
    let mut last_diag_ns: i64 = 0; // throttle for the quote diagnostic (see log_quote_diag)
    let mut dirty_idx_buf = Vec::with_capacity(strat.markets.len());
    let mut dirty_market_buf = Vec::with_capacity(strat.markets.len());
    loop {
        // BIASED: the latency-critical fill->hedge and hedge-event arms are polled FIRST, so a
        // pending maker fill deterministically preempts the reprice-all-markets (`wake`) and cold
        // `on_tick` work instead of waiting behind it (random select could schedule them first).
        // Order: shutdown > maker fills > exec events > paper trade prints > tick > wake. `tick`
        // precedes `wake` so the recovery/deadman tick is never starved by a continuously-ready
        // book `wake`. This is a tie-break change only (same handlers); on a fast VPS it removes
        // head-of-line jitter from the hot path.
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => break,
            Some(fill) = maker_fills.recv() => {
                strat.handle_maker_fill(fill, mono_now_ns()).await;
            }
            Some(ev) = exec_events.recv() => {
                strat.handle_exec_event(ev, mono_now_ns());
            }
            Some(print) = trade_prints.recv() => {
                strat.handle_trade_print(print, mono_now_ns()).await;
            }
            _ = tick.tick() => {
                let now_ns = mono_now_ns();
                strat.refresh_mark_cache();
                strat.on_tick(now_ns).await;
                // Throttled quote diagnostic (~every 20s): proves the loop is alive and explains any
                // no-quote (gate closed vs compute_desired_quote reject vs already resting).
                if now_ns.saturating_sub(last_diag_ns) >= 20_000_000_000 {
                    strat.log_quote_diag(now_ns);
                    let bb = crate::metrics::BOOK_BUILD.snapshot();
                    let vp = crate::metrics::VENUE_PUBLISH.snapshot();
                    let wr = crate::metrics::WAKE_REPRICE.snapshot();
                    let tr = crate::metrics::TICK_REPRICE.snapshot();
                    let sr = crate::metrics::SINGLE_REPRICE.snapshot();
                    info!(
                        "[metrics] book_build=p50:{}us/p99:{}us  publish=p50:{}us/p99:{}us  \
                         wake=p50:{}us/p99:{}us  tick=p50:{}us/p99:{}us  reprice=p50:{}us/p99:{}us  n={}",
                        bb.p50_us, bb.p99_us, vp.p50_us, vp.p99_us,
                        wr.p50_us, wr.p99_us, tr.p50_us, tr.p99_us,
                        sr.p50_us, sr.p99_us, sr.count,
                    );
                    crate::metrics::BOOK_BUILD.reset();
                    crate::metrics::VENUE_PUBLISH.reset();
                    crate::metrics::WAKE_REPRICE.reset();
                    crate::metrics::TICK_REPRICE.reset();
                    crate::metrics::SINGLE_REPRICE.reset();
                    last_diag_ns = now_ns;
                }
                // Reprice on every tick too, so a gate close / cooldown expiry is acted on
                // promptly even without a book change (§9.1 cancel-all on gate close).
                // If a book-change wake is pending, consume it and use force=false (the
                // generation gate skips unchanged markets — cheaper than force=true, and
                // the pending wake's dirty markets get processed now instead of waiting
                // for the next select iteration). If no wake is pending, use force=true
                // to catch gate-close / cooldown-expiry that wouldn't otherwise trigger.
                let wake_pending = wake.notified().now_or_never().is_some();
                let force = !wake_pending;
                let t0_tick = mono_now_ns();
                let now = Utc::now();
                let n_markets = strat.markets.len();
                for i in 0..n_markets {
                    let m = strat.markets[i].clone();
                    strat.reprice_market(&m, now, now_ns, force).await;
                    drain_priority_events(&mut strat, &mut exec_events, &mut maker_fills, &mut trade_prints).await;
                }
                crate::metrics::TICK_REPRICE.record((mono_now_ns() - t0_tick) as u64);
            }
            _ = wake.notified() => {
                let (now, now_ns) = (Utc::now(), mono_now_ns());
                let t0_wake = now_ns;
                strat.refresh_mark_cache();
                match &strat.dirty {
                    Some(dirty) => {
                        if dirty.take_reprice_all() {
                            let n_markets = strat.markets.len();
                            for i in 0..n_markets {
                                let m = strat.markets[i].clone();
                                strat.reprice_market(&m, now, now_ns, false).await;
                                drain_priority_events(&mut strat, &mut exec_events, &mut maker_fills, &mut trade_prints).await;
                            }
                        } else {
                            dirty.take_into(&mut dirty_idx_buf);
                            dirty_market_buf.clear();
                            dirty_market_buf.extend(
                                dirty_idx_buf
                                    .iter()
                                    .filter_map(|&idx| strat.registry.market_id(idx).cloned()),
                            );
                            for m in &dirty_market_buf {
                                strat.reprice_market(m, now, now_ns, false).await;
                                drain_priority_events(&mut strat, &mut exec_events, &mut maker_fills, &mut trade_prints).await;
                            }
                        }
                    }
                    None => {
                        let n_markets = strat.markets.len();
                        for i in 0..n_markets {
                            let m = strat.markets[i].clone();
                            strat.reprice_market(&m, now, now_ns, false).await;
                            drain_priority_events(&mut strat, &mut exec_events, &mut maker_fills, &mut trade_prints).await;
                        }
                    }
                }
                crate::metrics::WAKE_REPRICE.record((mono_now_ns() - t0_wake) as u64);
            }
        }
    }
    info!("strategy loop stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }
    fn edge() -> EdgeConfig {
        EdgeConfig {
            min_net_profit_bps: dec!(3.0),
            slippage_buffer_bps: dec!(1.5),
            latency_buffer_bps: dec!(2.0),
            basis_buffer_bps: dec!(1.0),
            funding_buffer_bps: dec!(0.0),
            aster_maker_fee_bps: dec!(0.0),
            taker_fee_bps: dec!(4.5),
        }
    }
    fn qcfg() -> QuoteEngineConfig {
        QuoteEngineConfig {
            desired_notional: dec!(100),
            max_quote_distance_bps: dec!(50.0),
            min_aster_touch_distance_bps: dec!(0.0),
            min_aster_touch_hysteresis_bps: dec!(2.0),
            max_aster_touch_hysteresis_ms: 300_000,
            depth_liquidity_multiple: dec!(10.0),
            max_hedge_slippage_bps: dec!(50.0),
            min_requote_interval_ms: 20,
            price_change_ticks_to_requote: 1,
            clamp_to_min_lot: true,
            min_requote_bps: dec!(1.0),
        }
    }
    fn spec() -> MarketSpec {
        MarketSpec {
            market_id: "BTC".into(),
            aster_symbol: "BTCUSDT".into(),
            hl_coin: "BTC".into(),
            lighter_market_id: 1,
            lighter_price_decimals: 2,
            lighter_size_decimals: 3,
            lighter_price_tick: dec!(0.01),
            tick: dec!(0.01),
            step: dec!(0.001),
            aster_min_qty: dec!(0.001),
            aster_min_notional: dec!(5),
            hl_sz_decimals: 3,
            hl_qty_step: dec!(0.001),
            hl_min_notional: dec!(5),
        }
    }
    fn books() -> (OrderBook, OrderBook) {
        let a = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            ts(),
            ts(),
        );
        let h = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(100))],
            vec![(dec!(100.05), dec!(100))],
            ts(),
            ts(),
        );
        (a, h)
    }

    fn hl_bbo_at(bid_qty: Decimal, ask_qty: Decimal, local_recv_ts: DateTime<Utc>) -> OrderBook {
        hl_bbo_at_ts(bid_qty, ask_qty, local_recv_ts, local_recv_ts)
    }

    fn hl_bbo_at_ts(
        bid_qty: Decimal,
        ask_qty: Decimal,
        exch_ts: DateTime<Utc>,
        local_recv_ts: DateTime<Utc>,
    ) -> OrderBook {
        OrderBook::from_levels(
            vec![(dec!(99.96), bid_qty)],
            vec![(dec!(100.04), ask_qty)],
            exch_ts,
            local_recv_ts,
        )
    }

    fn aster_bbo_at(bid_px: Decimal, ask_px: Decimal, local_recv_ts: DateTime<Utc>) -> OrderBook {
        aster_bbo_at_ts(bid_px, ask_px, local_recv_ts, local_recv_ts)
    }

    fn aster_bbo_at_ts(
        bid_px: Decimal,
        ask_px: Decimal,
        exch_ts: DateTime<Utc>,
        local_recv_ts: DateTime<Utc>,
    ) -> OrderBook {
        OrderBook::from_levels(
            vec![(bid_px, dec!(1000))],
            vec![(ask_px, dec!(1000))],
            exch_ts,
            local_recv_ts,
        )
    }

    fn publish_hl_l2_hot(strat: &Strategy, book: OrderBook, recv_ns: i64) {
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let hot = crate::livebot::scale::build_hot_book_with_qty_scale(
            &book,
            &scale,
            crate::livebot::scale::HotQtyScale::Hyperliquid,
            0,
            recv_ns,
        );
        strat
            .registry
            .cell(&m, VenueTag::Hyperliquid)
            .unwrap()
            .publish_hot(book, hot);
    }

    fn publish_hl_bbo_hot(strat: &Strategy, book: OrderBook, recv_ns: i64) {
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let hot = crate::livebot::scale::build_hot_book_with_qty_scale(
            &book,
            &scale,
            crate::livebot::scale::HotQtyScale::Hyperliquid,
            0,
            recv_ns,
        );
        strat
            .registry
            .cell(&m, VenueTag::Hyperliquid)
            .unwrap()
            .publish_bbo_hot(book, hot);
    }

    #[test]
    fn fill_telemetry_hl_source_prefers_fresh_bbo() {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account, ExecMode::Paper);
        let m: MarketId = "BTC".into();
        strat
            .registry
            .cell(&m, VenueTag::Hyperliquid)
            .unwrap()
            .publish_bbo(hl_bbo_at(dec!(2), dec!(3), ts()));

        let selected = strat.fresh_hl_quote_book(&m, crate::hotpath::clock::mono_now_ns()).unwrap();
        assert_eq!(selected.source, HlQuoteSource::Bbo);
        assert_eq!(selected.book.best_bid().unwrap().qty, dec!(2));
        assert_eq!(selected.book.best_ask().unwrap().qty, dec!(3));
    }

    #[test]
    fn fill_telemetry_hl_source_falls_back_when_bbo_crossed() {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account, ExecMode::Paper);
        let m: MarketId = "BTC".into();
        let crossed = OrderBook::from_levels(
            vec![(dec!(100.10), dec!(2))],
            vec![(dec!(100.00), dec!(2))],
            ts(),
            ts(),
        );
        strat
            .registry
            .cell(&m, VenueTag::Hyperliquid)
            .unwrap()
            .publish_bbo(crossed);

        let selected = strat.fresh_hl_quote_book(&m, crate::hotpath::clock::mono_now_ns()).unwrap();
        assert_eq!(selected.source, HlQuoteSource::L2);
        assert_eq!(selected.book.best_bid().unwrap().px, dec!(99.95));
    }

    #[test]
    fn fill_hedge_source_falls_back_when_bbo_depth_insufficient() {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account, ExecMode::Paper);
        let m: MarketId = "BTC".into();
        strat
            .registry
            .cell(&m, VenueTag::Hyperliquid)
            .unwrap()
            .publish_bbo(hl_bbo_at(dec!(0.2), dec!(0.2), ts()));

        let selected = strat
            .fresh_hl_hedge_book(&m, crate::hotpath::clock::mono_now_ns(), Side::Sell, dec!(0.2))
            .unwrap();
        assert_eq!(selected.source, HlQuoteSource::L2);
        let depth = selected.bbo_depth.as_ref().expect("must explain BBO fallback");
        assert_eq!(depth.top_qty, Some(dec!(0.2)));
        assert_eq!(depth.required_qty, dec!(2.0));
        assert!(!depth.sufficient);
    }

    #[test]
    fn fill_hedge_source_uses_bbo_with_depth_factor() {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account, ExecMode::Paper);
        let m: MarketId = "BTC".into();
        strat
            .registry
            .cell(&m, VenueTag::Hyperliquid)
            .unwrap()
            .publish_bbo(hl_bbo_at(dec!(2.1), dec!(2.1), ts()));

        let selected = strat
            .fresh_hl_hedge_book(&m, crate::hotpath::clock::mono_now_ns(), Side::Sell, dec!(0.2))
            .unwrap();
        assert_eq!(selected.source, HlQuoteSource::Bbo);
        let depth = selected.bbo_depth.as_ref().expect("BBO selection should carry depth details");
        assert_eq!(depth.top_qty, Some(dec!(2.1)));
        assert_eq!(depth.required_qty, dec!(2.0));
        assert!(depth.sufficient);
    }

    #[test]
    fn hot_hl_bbo_selected_when_fresh_and_10x_deep() {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account, ExecMode::Paper);
        let m: MarketId = "BTC".into();
        publish_hl_l2_hot(&strat, books().1, 1_000_000);
        publish_hl_bbo_hot(&strat, hl_bbo_at(dec!(2.1), dec!(2.1), ts()), 1_000_000);

        let selected = strat
            .fresh_hl_hedge_book_hot_first(&m, 2_000_000, Side::Sell, dec!(0.2))
            .unwrap();

        assert_eq!(selected.source, HlQuoteSource::Bbo);
        assert_eq!(selected.path, HlHedgePath::Hot);
        let depth = selected.bbo_depth.as_ref().expect("hot BBO should carry depth");
        assert_eq!(depth.top_qty, Some(dec!(2.1)));
        assert_eq!(depth.required_qty, dec!(2.0));
        assert!(depth.sufficient);
    }

    #[test]
    fn hot_hl_bbo_depth_check_is_side_specific() {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account, ExecMode::Paper);
        let m: MarketId = "BTC".into();
        publish_hl_l2_hot(&strat, books().1, 1_000_000);
        // Bid is too thin for a SELL hedge, ask is deep enough for a BUY hedge.
        publish_hl_bbo_hot(&strat, hl_bbo_at(dec!(0.2), dec!(2.1), ts()), 1_000_000);

        let sell = strat
            .fresh_hl_hedge_book_hot_first(&m, 2_000_000, Side::Sell, dec!(0.2))
            .unwrap();
        assert_eq!(sell.source, HlQuoteSource::L2);
        assert_eq!(sell.path, HlHedgePath::Hot);
        assert_eq!(sell.bbo_depth.as_ref().unwrap().top_qty, Some(dec!(0.2)));

        let buy = strat
            .fresh_hl_hedge_book_hot_first(&m, 2_000_000, Side::Buy, dec!(0.2))
            .unwrap();
        assert_eq!(buy.source, HlQuoteSource::Bbo);
        assert_eq!(buy.path, HlHedgePath::Hot);
        assert_eq!(buy.bbo_depth.as_ref().unwrap().top_qty, Some(dec!(2.1)));
    }

    #[test]
    fn hot_hl_bbo_older_than_l2_is_ignored() {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let strat = live_strat(etx, htx, account, ExecMode::Paper);
        let m: MarketId = "BTC".into();
        let newer = ts() + chrono::Duration::milliseconds(10);
        let l2 = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(100))],
            vec![(dec!(100.05), dec!(100))],
            newer,
            newer,
        );
        publish_hl_l2_hot(&strat, l2, 1_000_000);
        publish_hl_bbo_hot(&strat, hl_bbo_at_ts(dec!(2.1), dec!(2.1), ts(), newer), 1_500_000);

        let selected = strat
            .fresh_hl_hedge_book_hot_first(&m, 2_000_000, Side::Sell, dec!(0.2))
            .unwrap();

        assert_eq!(selected.source, HlQuoteSource::L2);
        assert_eq!(selected.path, HlHedgePath::Hot);
    }

    #[tokio::test]
    async fn primary_fill_hedge_prefers_hot_bbo_when_deep_enough() {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(64);
        let (htx, mut hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account, ExecMode::Paper);
        let m: MarketId = "BTC".into();
        publish_hl_l2_hot(&strat, books().1, 1_000_000);
        publish_hl_bbo_hot(&strat, hl_bbo_at(dec!(2.1), dec!(2.1), ts()), 1_000_000);

        let fill = AsterFill {
            market: m.clone(),
            aster_side: Side::Buy,
            order_id: "oid-fill-hot".into(),
            trade_id: "trade-fill-hot".into(),
            client_id: "cid-fill-hot".into(),
            last_fill_qty: dec!(0.2),
            last_fill_px: dec!(100),
            cum_filled_qty: dec!(0.2),
            event_time_ms: 1_700_000_000_000,
            reduce_only: false,
        };

        strat.handle_maker_fill(fill, 2_000_000).await;

        let cmd = hrx.try_recv().expect("primary hedge command must be sent");
        match cmd {
            HedgeCommand::Hedge { aggressive_px, emergency, intent, .. } => {
                assert!(!emergency);
                assert_eq!(intent.hedge_side, Side::Sell);
                assert_eq!(intent.qty, dec!(0.2));
                assert_eq!(aggressive_px, dec!(99.96) * (Decimal::ONE - strat.cfg.live.hyperliquid.normal_slippage_bps / Decimal::from(10_000)));
            }
            other => panic!("expected primary hedge, got {other:?}"),
        }
    }

    #[test]
    fn fill_telemetry_buy_touch_distance_marks_invalid_when_too_close_or_wrong_side() {
        let book = Arc::new(OrderBook::from_levels(
            vec![(dec!(100.00), dec!(5))],
            vec![(dec!(100.50), dec!(5))],
            ts(),
            ts(),
        ));
        let valid = aster_fill_touch_context(
            SelectedAsterTouch { source: AsterQuoteSource::L2, book: book.clone(), age_ms: 0 },
            Side::Buy,
            dec!(99.75),
            dec!(20.0),
        )
        .unwrap();
        assert!(!valid.quote_invalid_at_fill);
        assert!(valid.distance_bps > dec!(20.0));

        let too_close = aster_fill_touch_context(
            SelectedAsterTouch { source: AsterQuoteSource::L2, book: book.clone(), age_ms: 0 },
            Side::Buy,
            dec!(99.90),
            dec!(20.0),
        )
        .unwrap();
        assert!(too_close.quote_invalid_at_fill);

        let wrong_side = aster_fill_touch_context(
            SelectedAsterTouch { source: AsterQuoteSource::L2, book, age_ms: 0 },
            Side::Buy,
            dec!(100.01),
            dec!(20.0),
        )
        .unwrap();
        assert!(wrong_side.quote_invalid_at_fill);
        assert!(wrong_side.signed_distance_bps < Decimal::ZERO);
    }

    #[test]
    fn fill_telemetry_sell_touch_distance_marks_invalid_when_too_close_or_wrong_side() {
        let book = Arc::new(OrderBook::from_levels(
            vec![(dec!(99.50), dec!(5))],
            vec![(dec!(100.00), dec!(5))],
            ts(),
            ts(),
        ));
        let valid = aster_fill_touch_context(
            SelectedAsterTouch { source: AsterQuoteSource::L2, book: book.clone(), age_ms: 0 },
            Side::Sell,
            dec!(100.30),
            dec!(20.0),
        )
        .unwrap();
        assert!(!valid.quote_invalid_at_fill);
        assert!(valid.distance_bps > dec!(20.0));

        let too_close = aster_fill_touch_context(
            SelectedAsterTouch { source: AsterQuoteSource::L2, book: book.clone(), age_ms: 0 },
            Side::Sell,
            dec!(100.10),
            dec!(20.0),
        )
        .unwrap();
        assert!(too_close.quote_invalid_at_fill);

        let wrong_side = aster_fill_touch_context(
            SelectedAsterTouch { source: AsterQuoteSource::L2, book, age_ms: 0 },
            Side::Sell,
            dec!(99.99),
            dec!(20.0),
        )
        .unwrap();
        assert!(wrong_side.quote_invalid_at_fill);
        assert!(wrong_side.signed_distance_bps < Decimal::ZERO);
    }

    #[test]
    fn hl_bbo_selected_when_fresh_and_sufficient() {
        let (a, h) = books();
        let bbo = hl_bbo_at(dec!(20), dec!(20), ts());
        let (desired, _book, source) = compute_desired_quote_select_hl(
            &edge(),
            &qcfg(),
            &a,
            Some(&h),
            Some(&bbo),
            Side::Buy,
            &spec(),
            5000,
            ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(source, HlQuoteSource::Bbo);
        assert_eq!(desired.expected_hl_vwap, dec!(99.96));
    }

    #[test]
    fn hl_bbo_depth_factor_rejects_under_10x_and_falls_back_to_l2() {
        let (a, h) = books();
        let bbo = hl_bbo_at(dec!(9.99), dec!(9.99), ts());
        let (desired, _book, source) = compute_desired_quote_select_hl(
            &edge(),
            &qcfg(),
            &a,
            Some(&h),
            Some(&bbo),
            Side::Buy,
            &spec(),
            5000,
            ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(source, HlQuoteSource::L2);
        assert_eq!(desired.expected_hl_vwap, dec!(99.95));
    }

    #[test]
    fn locally_fresh_but_exchange_older_hl_bbo_is_ignored() {
        let now = ts() + chrono::Duration::milliseconds(10);
        let a = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            now,
            now,
        );
        let h_newer = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(100))],
            vec![(dec!(100.05), dec!(100))],
            now,
            now,
        );
        // Local receive is fresh, but the BBO exchange timestamp predates the installed L2.
        let old_bbo = hl_bbo_at_ts(dec!(2), dec!(2), ts(), now);
        let (desired, _book, source) = compute_desired_quote_select_hl(
            &edge(),
            &qcfg(),
            &a,
            Some(&h_newer),
            Some(&old_bbo),
            Side::Buy,
            &spec(),
            5000,
            now,
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(source, HlQuoteSource::L2);
        assert_eq!(desired.expected_hl_vwap, dec!(99.95));
    }

    #[test]
    fn thin_hl_bbo_falls_back_to_fresh_l2() {
        let (a, h) = books();
        let bbo = hl_bbo_at(dec!(0.1), dec!(0.1), ts());
        let (desired, _book, source) = compute_desired_quote_select_hl(
            &edge(),
            &qcfg(),
            &a,
            Some(&h),
            Some(&bbo),
            Side::Buy,
            &spec(),
            5000,
            ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(source, HlQuoteSource::L2);
        assert_eq!(desired.expected_hl_vwap, dec!(99.95));
    }

    #[test]
    fn thin_hl_bbo_with_stale_l2_rejects_explicitly() {
        let (_, h) = books();
        let now = ts() + chrono::Duration::milliseconds(10_000);
        let a = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            now,
            now,
        );
        let bbo = hl_bbo_at(dec!(0.1), dec!(0.1), now);
        let err = compute_desired_quote_select_hl(
            &edge(),
            &qcfg(),
            &a,
            Some(&h),
            Some(&bbo),
            Side::Buy,
            &spec(),
            5000,
            now,
            &PositionContext::unconstrained(),
        )
        .unwrap_err();
        assert_eq!(err, RejectReason::HlBboThinAndL2Stale);
    }

    #[test]
    fn hl_bbo_depth_factor_with_stale_l2_rejects_explicitly() {
        let (_, h) = books();
        let now = ts() + chrono::Duration::milliseconds(10_000);
        let a = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            now,
            now,
        );
        let bbo = hl_bbo_at(dec!(9.99), dec!(9.99), now);
        let err = compute_desired_quote_select_hl(
            &edge(),
            &qcfg(),
            &a,
            Some(&h),
            Some(&bbo),
            Side::Buy,
            &spec(),
            5000,
            now,
            &PositionContext::unconstrained(),
        )
        .unwrap_err();
        assert_eq!(err, RejectReason::HlBboThinAndL2Stale);
    }

    #[test]
    fn aster_bbo_selected_when_fresh() {
        let (a, h) = books();
        let a_bbo = aster_bbo_at(dec!(99.99), dec!(100.03), ts());
        let (desired, _hl_book, hl_source, aster_source) = compute_desired_quote_select_books(
            &edge(),
            &qcfg(),
            &a,
            Some(&a_bbo),
            Some(&h),
            None,
            Side::Buy,
            &spec(),
            5000,
            ts(),
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(aster_source, AsterQuoteSource::Bbo);
        assert_eq!(hl_source, HlQuoteSource::L2);
        assert_eq!(desired.aster_mid, dec!(100.01));
    }

    #[test]
    fn locally_fresh_but_exchange_older_aster_bbo_is_ignored() {
        let now = ts() + chrono::Duration::milliseconds(10);
        let a_newer = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            now,
            now,
        );
        let h = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(100))],
            vec![(dec!(100.05), dec!(100))],
            now,
            now,
        );
        let old_bbo = aster_bbo_at_ts(dec!(99.99), dec!(100.03), ts(), now);
        let (desired, _hl_book, _hl_source, aster_source) = compute_desired_quote_select_books(
            &edge(),
            &qcfg(),
            &a_newer,
            Some(&old_bbo),
            Some(&h),
            None,
            Side::Buy,
            &spec(),
            5000,
            now,
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(aster_source, AsterQuoteSource::L2);
        assert_eq!(desired.aster_mid, dec!(100.00));
    }

    #[test]
    fn stale_aster_bbo_falls_back_to_l2() {
        let now = ts() + chrono::Duration::milliseconds(10_000);
        let a = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            now,
            now,
        );
        let h = OrderBook::from_levels(
            vec![(dec!(99.95), dec!(100))],
            vec![(dec!(100.05), dec!(100))],
            now,
            now,
        );
        let stale_bbo = aster_bbo_at(dec!(99.99), dec!(100.03), ts());
        let (desired, _hl_book, _hl_source, aster_source) = compute_desired_quote_select_books(
            &edge(),
            &qcfg(),
            &a,
            Some(&stale_bbo),
            Some(&h),
            None,
            Side::Buy,
            &spec(),
            5000,
            now,
            &PositionContext::unconstrained(),
        )
        .unwrap();
        assert_eq!(aster_source, AsterQuoteSource::L2);
        assert_eq!(desired.aster_mid, dec!(100.00));
    }

    #[test]
    fn hot_precheck_ignores_exchange_older_bbo() {
        let scale = MarketScale::from_spec(&spec());
        let newer_exch = ts() + chrono::Duration::milliseconds(10);
        let l2 = OrderBook::from_levels(
            vec![(dec!(99.50), dec!(100))],
            vec![(dec!(100.50), dec!(100))],
            newer_exch,
            newer_exch,
        );
        let old_bbo = aster_bbo_at_ts(dec!(99.99), dec!(100.03), ts(), newer_exch);
        let l2_hot = crate::livebot::scale::build_hot_book(&l2, &scale, 0, 1_000);
        let bbo_hot = crate::livebot::scale::build_hot_book(&old_bbo, &scale, 0, 2_000);
        let selected = select_aster_hot_for_precheck(Some(&l2_hot), Some(&bbo_hot), 2_000, 5_000_000_000)
            .expect("must pick one book");
        assert_eq!(selected.exch_ms, l2_hot.exch_ms);
        assert_eq!(selected.best_bid_ticks(), l2_hot.best_bid_ticks());
    }

    #[test]
    fn crossing_hedge_px_crosses_the_opposite_touch() {
        let (_a, h) = books(); // HL book: best bid 99.95 / best ask 100.05
        let slip = dec!(10); // 10 bps
        // A BUY hedge must price AT/ABOVE the ask to take liquidity; a SELL hedge AT/BELOW the bid.
        let buy = crossing_hedge_px(&h, Side::Buy, slip).unwrap();
        assert!(buy >= h.best_ask().unwrap().px, "buy hedge {buy} must cross the ask {}", h.best_ask().unwrap().px);
        let sell = crossing_hedge_px(&h, Side::Sell, slip).unwrap();
        assert!(sell <= h.best_bid().unwrap().px, "sell hedge {sell} must cross the bid {}", h.best_bid().unwrap().px);
        // An empty book side yields None — the caller must NOT hedge off a fallback price.
        let empty = OrderBook::from_levels(vec![], vec![], ts(), ts());
        assert!(crossing_hedge_px(&empty, Side::Buy, slip).is_none());
        assert!(crossing_hedge_px(&empty, Side::Sell, slip).is_none());
    }

    #[test]
    fn effective_aster_cap_shrinks_to_real_collateral() {
        // Real ~$124 collateral, $26 buffer, 1x lev, single market: cap = (124-26)*1 = 98, below the
        // $200 static config cap, so the dynamic margin cap binds (this is the incident fix).
        assert_eq!(effective_aster_cap_notional(dec!(200), dec!(124), dec!(26), dec!(1), dec!(0)), dec!(98));
        // Buffer >= collateral => zero cap (the increasing side stops quoting entirely).
        assert_eq!(effective_aster_cap_notional(dec!(200), dec!(20), dec!(26), dec!(1), dec!(0)), dec!(0));
        // The static config cap is the smaller of the two => it still wins.
        assert_eq!(effective_aster_cap_notional(dec!(50), dec!(124), dec!(26), dec!(1), dec!(0)), dec!(50));
        // Leverage scales the usable notional; still clamped by the static cap.
        assert_eq!(effective_aster_cap_notional(dec!(200), dec!(124), dec!(26), dec!(2), dec!(0)), dec!(196));
        // Account-wide collateral: OTHER markets' Aster notional is deducted first.
        assert_eq!(effective_aster_cap_notional(dec!(200), dec!(124), dec!(26), dec!(1), dec!(40)), dec!(58));
        // Other markets consume more than the usable collateral => clamp to zero (never negative).
        assert_eq!(effective_aster_cap_notional(dec!(200), dec!(124), dec!(26), dec!(1), dec!(200)), dec!(0));
    }

    #[test]
    fn gate_closed_cancels_resting_holds_empty() {
        let (a, h) = books();
        let pos = PositionContext::unconstrained();
        // resting order + may_quote=false => cancel
        let d = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, false, Some(CurrentOrder { price: dec!(99), qty: dec!(1) }), true);
        assert!(matches!(d, SideDecision::Cancel { reason: ReplaceReason::FeedStale }));
        // empty slot + may_quote=false => hold
        let d = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, false, None, true);
        assert!(matches!(d, SideDecision::Hold));
    }

    #[test]
    fn empty_slot_places_when_profitable() {
        let (a, h) = books();
        let pos = PositionContext::unconstrained();
        let d = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, None, true);
        assert!(matches!(d, SideDecision::Place(_)));
    }

    #[test]
    fn stale_book_cancels_with_feed_stale() {
        let (a, h) = books();
        let pos = PositionContext::unconstrained();
        let later = ts() + chrono::Duration::milliseconds(10_000); // both books now stale
        let d = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, later, &pos, true, Some(CurrentOrder { price: dec!(99), qty: dec!(1) }), true);
        assert!(matches!(d, SideDecision::Cancel { reason: ReplaceReason::FeedStale }));
    }

    #[test]
    fn requote_deadband_holds_within_min_bps_but_replaces_beyond() {
        let (a, h) = books();
        let pos = PositionContext::unconstrained();
        // Find where the engine wants to quote (empty slot -> Place).
        let place = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, None, true);
        let SideDecision::Place(d) = place else { panic!("expected a Place for an empty profitable slot") };
        let (px, qty) = (d.price, d.qty);
        // A resting order AT that price (0 bps away) must HOLD — the deadband suppresses churn.
        let near = CurrentOrder { price: px, qty };
        let held = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, Some(near), true);
        assert!(matches!(held, SideDecision::Hold), "within {}bps deadband must Hold", qcfg().min_requote_bps);
        // A resting order far away (~100 bps) must REPLACE.
        let far = CurrentOrder { price: px * dec!(0.99), qty };
        let replaced = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, Some(far), true);
        assert!(matches!(replaced, SideDecision::Replace { .. }), "beyond the deadband must Replace");
    }

    #[test]
    fn deadband_does_not_hold_unprofitable_resting_quote() {
        let (a, h) = books();
        let pos = PositionContext::unconstrained();
        let mut cfg = qcfg();
        cfg.min_requote_bps = dec!(10.0); // make a one-tick bad move sit inside the deadband

        let place = evaluate_side(&edge(), &cfg, &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, None, true);
        let SideDecision::Place(d) = place else { panic!("expected a Place for an empty profitable slot") };

        // A buy quote one tick ABOVE the computed profitable bound is a worse maker price. Even
        // though this is inside the configured 10 bps deadband, it must take the urgent safety path.
        let current = CurrentOrder { price: d.price + spec().tick, qty: d.qty };
        let decision = evaluate_side(&edge(), &cfg, &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, Some(current), true);
        assert!(
            matches!(decision, SideDecision::Replace { reason: ReplaceReason::NoLongerProfitable, .. }),
            "unprofitable quote inside the deadband must still be replaced"
        );
    }

    fn full_cfg() -> Config {
        let toml = r#"
[edge]
min_net_profit_bps = "3.0"
slippage_buffer_bps = "1.5"
latency_buffer_bps = "2.0"
basis_buffer_bps = "1.0"
funding_buffer_bps = "0.0"
aster_maker_fee_bps = "0.0"
taker_fee_bps = "4.5"
[quote]
desired_notional = "100"
max_quote_distance_bps = "50.0"
max_hedge_slippage_bps = "50.0"
min_requote_interval_ms = 20
price_change_ticks_to_requote = 1
clamp_to_min_lot = true
[simulation]
simulated_aster_place_latency_ms = 25
simulated_aster_cancel_latency_ms = 25
quote_ttl_ms = 500
hedge_latency_buckets_ms = [50]
max_book_staleness_ms = 5000
[partials]
strict_all_partials_must_be_hedgeable = false
accumulate_sub_min_fills = true
lighter_min_notional = "10"
max_pending_inventory_notional = "25"
max_pending_inventory_age_ms = 1000
mark_pending_inventory_to_market = true
[queue_model]
models = ["optimistic"]
hidden_queue_multiplier = "1.0"
[[markets]]
aster_symbol = "BTCUSDT"
lighter_symbol = "BTC"
"#;
        toml::from_str(toml).unwrap()
    }

    #[tokio::test]
    async fn may_quote_reflects_clean_start_and_per_market_feeds() {
        use crate::hotpath::clock::mono_now_ns;
        let specs = vec![spec()];
        let elig: HashMap<MarketId, bool> = [("BTC".into(), true)].into_iter().collect();
        let reg = Arc::new(VenueRegistry::new(&["BTC".into()]));
        // Publish fresh books on BOTH of BTC's cells so its per-market feeds read fresh
        // (freshness is the cell's mono publish stamp, not the book's embedded ts).
        let (ab, hb) = books();
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(ab);
        reg.cell(&"BTC".into(), VenueTag::Hyperliquid).unwrap().publish(hb);
        let account = AccountState::new(rust_decimal_macros::dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = Strategy::new(
            full_cfg(), &specs, &elig, reg.clone(), account, Journal::null(),
            SessionId::from_tag("t"), etx, htx, ExecMode::Paper,
        );
        let now = mono_now_ns();
        // Before clean-start: no quoting (invariant 7).
        assert!(!strat.may_quote(&"BTC".into(), now));
        // After clean-start + fresh per-market feeds: quoting allowed (paper: account/position
        // checks are vacuous, no hedges in flight).
        strat.mark_clean_start();
        assert!(strat.may_quote(&"BTC".into(), now));
        // A REST-divergent feed on THIS market closes quoting for it (per-market, not global).
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().mark_divergent(true);
        assert!(!strat.may_quote(&"BTC".into(), now));
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().mark_divergent(false);
        assert!(strat.may_quote(&"BTC".into(), now));
        // A stale book also closes quoting (evaluated >max_book_staleness_ms past the publish).
        let stale_now = now + 10_000_000_000; // +10s
        assert!(!strat.may_quote(&"BTC".into(), stale_now));
        // A latched freeze (e.g. hedge reject) stops quoting even with everything else green.
        strat.freeze(now, "test");
        assert!(!strat.may_quote(&"BTC".into(), now));
    }

    #[tokio::test]
    async fn orphan_hedge_names_gate_reason_and_substep_fill_reconciles() {
        use crate::hotpath::clock::mono_now_ns;
        let specs = vec![spec()];
        let elig: HashMap<MarketId, bool> = [("BTC".into(), true)].into_iter().collect();
        let reg = Arc::new(VenueRegistry::new(&["BTC".into()]));
        let (ab, hb) = books();
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(ab);
        reg.cell(&"BTC".into(), VenueTag::Hyperliquid).unwrap().publish(hb);
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = Strategy::new(
            full_cfg(), &specs, &elig, reg.clone(), account, Journal::null(),
            SessionId::from_tag("t"), etx, htx, ExecMode::Paper,
        );
        strat.mark_clean_start();
        let now = mono_now_ns();
        assert_eq!(strat.maker_gate_reason(&"BTC".into(), now), None, "gate open before any orphan");

        // Inject a partially-filled (dangerous/orphan) hedge. The gate must NAME the cause as
        // ORPHAN_HEDGE — not silently suppress quoting (the bug) nor mislabel it FEED_STALE.
        let cloid = crate::livebot::ids::Cloid::recovery(&"BTC".into(), 1);
        let mut h = HedgeIntent::with_qty(cloid, "BTC".into(), Side::Sell, dec!(0.5), dec!(100), now);
        h.mark_submitted(now);
        strat.hedges.insert(cloid.to_hex(), h);
        // A HedgeFill leaving only a SUB-STEP remainder (qty step is 0.001) must RECONCILE — the leg
        // is hedged to within an untradeable increment, so it must not latch PartiallyFilled forever.
        strat.handle_exec_event(ExecEvent::HedgeFill { cloid, filled_qty: dec!(0.4995), px: dec!(100), fee_usd: Decimal::ZERO }, now);
        assert!(
            strat.hedges.values().all(|h| !h.state.is_dangerous()),
            "sub-step remainder must reconcile, not orphan"
        );
        assert_eq!(strat.maker_gate_reason(&"BTC".into(), now), None, "gate reopens once the orphan reconciles");

        // And a GENUINE shortfall (bigger than the step) stays an orphan and the gate names it.
        let cloid2 = crate::livebot::ids::Cloid::recovery(&"BTC".into(), 2);
        let mut h2 = HedgeIntent::with_qty(cloid2, "BTC".into(), Side::Sell, dec!(0.5), dec!(100), now);
        h2.mark_submitted(now);
        strat.hedges.insert(cloid2.to_hex(), h2);
        strat.handle_exec_event(ExecEvent::HedgeFill { cloid: cloid2, filled_qty: dec!(0.2), px: dec!(100), fee_usd: Decimal::ZERO }, now);
        assert_eq!(
            strat.maker_gate_reason(&"BTC".into(), now),
            Some("ORPHAN_HEDGE"),
            "a real partial (remainder > step) keeps the gate closed with a NAMED reason"
        );
    }

    #[tokio::test]
    async fn hedge_unknown_marks_dangerous_and_freezes() {
        use crate::hotpath::clock::mono_now_ns;
        let specs = vec![spec()];
        let elig: HashMap<MarketId, bool> = [("BTC".into(), true)].into_iter().collect();
        let reg = Arc::new(VenueRegistry::new(&["BTC".into()]));
        let (ab, hb) = books();
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(ab);
        reg.cell(&"BTC".into(), VenueTag::Hyperliquid).unwrap().publish(hb);
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = Strategy::new(
            full_cfg(), &specs, &elig, reg, account, Journal::null(),
            SessionId::from_tag("t"), etx, htx, ExecMode::Paper,
        );
        strat.mark_clean_start();
        let now = mono_now_ns();
        let cloid = crate::livebot::ids::Cloid::recovery(&"BTC".into(), 9);
        let mut h = HedgeIntent::with_qty(cloid, "BTC".into(), Side::Sell, dec!(0.5), dec!(100), now);
        h.mark_submitted(now);
        strat.hedges.insert(cloid.to_hex(), h);

        strat.handle_exec_event(ExecEvent::HedgeUnknown { cloid, reason: "timeout".into() }, now);

        let h = strat.hedges.get(&cloid.to_hex()).unwrap();
        assert_eq!(h.state, crate::livebot::fills::HedgeState::Unknown);
        assert!(h.state.is_dangerous());
        assert_eq!(strat.maker_gate_reason(&"BTC".into(), now), Some("FROZEN"));
    }

    #[test]
    fn matching_resting_order_holds() {
        let (a, h) = books();
        let pos = PositionContext::unconstrained();
        // First compute what we'd place, then feed it back as the current order => Hold.
        let placed = match evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, None, true) {
            SideDecision::Place(d) => *d,
            other => panic!("expected place, got {other:?}"),
        };
        let cur = CurrentOrder { price: placed.price, qty: placed.qty };
        let d = evaluate_side(&edge(), &qcfg(), &a, &h, Side::Buy, &spec(), 5000, ts(), &pos, true, Some(cur), true);
        assert!(matches!(d, SideDecision::Hold), "identical resting quote should hold, got {d:?}");
    }

    #[test]
    fn touch_guard_hysteresis_only_raises_empty_blocked_slots() {
        let mut cfg = qcfg();
        cfg.min_aster_touch_distance_bps = dec!(24.0);
        cfg.min_aster_touch_hysteresis_bps = dec!(2.0);

        let empty_blocked = quote_cfg_for_touch_guard(&cfg, true, None);
        assert_eq!(empty_blocked.min_aster_touch_distance_bps, dec!(26.0));

        let current = Some(CurrentOrder { price: dec!(100), qty: dec!(1) });
        let resting_blocked = quote_cfg_for_touch_guard(&cfg, true, current);
        assert_eq!(resting_blocked.min_aster_touch_distance_bps, dec!(24.0));

        let empty_unblocked = quote_cfg_for_touch_guard(&cfg, false, None);
        assert_eq!(empty_unblocked.min_aster_touch_distance_bps, dec!(24.0));
    }

    // --- fast-VPS hardening tests (Tier 1/2) ---

    fn live_strat(
        etx: tokio::sync::mpsc::Sender<ExecCommand>,
        htx: tokio::sync::mpsc::Sender<HedgeCommand>,
        account: AccountState,
        mode: ExecMode,
    ) -> Strategy {
        let specs = vec![spec()];
        let elig: HashMap<MarketId, bool> = [("BTC".into(), true)].into_iter().collect();
        let reg = Arc::new(VenueRegistry::new(&["BTC".into()]));
        let (ab, hb) = books();
        reg.cell(&"BTC".into(), VenueTag::Aster).unwrap().publish(ab);
        reg.cell(&"BTC".into(), VenueTag::Hyperliquid).unwrap().publish(hb);
        Strategy::new(full_cfg(), &specs, &elig, reg, account, Journal::null(), SessionId::from_tag("t"), etx, htx, mode)
    }

    #[test]
    fn touch_guard_status_expires_to_base_guard_after_timeout() {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account, ExecMode::Live);
        strat.cfg.quote.min_aster_touch_distance_bps = dec!(24.0);
        strat.cfg.quote.min_aster_touch_hysteresis_bps = dec!(1.0);
        strat.cfg.quote.max_aster_touch_hysteresis_ms = 300_000;
        let m: MarketId = "BTC".into();
        let t0 = 1_000_000_000_i64;

        strat.latch_aster_touch_guard(&m, Side::Sell, t0);
        let active = strat.aster_touch_guard_status_for_empty(&m, Side::Sell, None, t0 + 299_999_000_000);
        assert_eq!(active, AsterTouchGuardStatus::Active);
        let active_cfg = quote_cfg_for_touch_guard(&strat.cfg.quote, active == AsterTouchGuardStatus::Active, None);
        assert_eq!(active_cfg.min_aster_touch_distance_bps, dec!(25.0));

        let expired = strat.expire_aster_touch_guard_if_needed(&m, Side::Sell, None, t0 + 300_000_000_000);
        assert_eq!(expired, AsterTouchGuardStatus::Expired);
        assert_eq!(
            strat.aster_touch_guard_status_for_empty(&m, Side::Sell, None, t0 + 300_000_000_001),
            AsterTouchGuardStatus::Off
        );
        let base_cfg = quote_cfg_for_touch_guard(&strat.cfg.quote, false, None);
        assert_eq!(base_cfg.min_aster_touch_distance_bps, dec!(24.0));
    }

    #[test]
    fn zero_touch_hysteresis_timeout_keeps_latch_until_rearm() {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account, ExecMode::Live);
        strat.cfg.quote.min_aster_touch_distance_bps = dec!(24.0);
        strat.cfg.quote.min_aster_touch_hysteresis_bps = dec!(1.0);
        strat.cfg.quote.max_aster_touch_hysteresis_ms = 0;
        let m: MarketId = "BTC".into();
        let t0 = 1_000_000_000_i64;

        strat.latch_aster_touch_guard(&m, Side::Sell, t0);
        assert_eq!(
            strat.expire_aster_touch_guard_if_needed(&m, Side::Sell, None, t0 + 3_600_000_000_000),
            AsterTouchGuardStatus::Active
        );
    }

    #[tokio::test]
    async fn min_requote_interval_throttles_nonurgent_replace_only() {
        // T1.4: the live path now honors `min_requote_interval_ms`. NON-URGENT replaces (price/qty
        // drift) are throttled; an urgent `NoLongerProfitable` replace BYPASSES. (`Place`/`Cancel`
        // are never gated.) Mirrors the SimEngine.
        let account = AccountState::new(dec!(5));
        let (etx, mut erx) = tokio::sync::mpsc::channel(64);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account, ExecMode::Paper);
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let desired = match evaluate_side(&edge(), &qcfg(), &books().0, &books().1, Side::Buy, &spec(), 5000, ts(), &PositionContext::unconstrained(), true, None, true) {
            SideDecision::Place(d) => *d,
            other => panic!("expected place, got {other:?}"),
        };
        let min_ms = full_cfg().live.quote.min_requote_interval_ms as i64; // 20 (default)
        let t0 = 1_000_000_000_i64;
        // Seed a resting order (records last_requote_ns = t0 + a client id).
        strat.apply_decision(&m, Side::Buy, SideDecision::Place(Box::new(desired.clone())), &scale, t0).await;
        let place_cid = match erx.try_recv() {
            Ok(ExecCommand::Place { client_id, .. }) => client_id,
            other => panic!("place must emit, got {other:?}"),
        };
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: place_cid, venue_order_id: "oid0".into() }, t0 + 1);
        // Non-urgent replace within the interval => THROTTLED (no command emitted).
        let t_soon = t0 + (min_ms - 5) * 1_000_000;
        strat.apply_decision(&m, Side::Buy, SideDecision::Replace { desired: Box::new(desired.clone()), reason: ReplaceReason::PriceChanged }, &scale, t_soon).await;
        assert!(erx.try_recv().is_err(), "non-urgent replace within min_requote_interval must be throttled");
        // Urgent replace within the interval => BYPASSES (records last_requote_ns = t_soon).
        strat.apply_decision(&m, Side::Buy, SideDecision::Replace { desired: Box::new(desired.clone()), reason: ReplaceReason::NoLongerProfitable }, &scale, t_soon).await;
        let (old_cid, new_cid) = match erx.try_recv() {
            Ok(ExecCommand::Replace { old_client_id, new_client_id, .. }) => (old_client_id, new_client_id),
            other => panic!("urgent NoLongerProfitable replace must bypass the throttle, got {other:?}"),
        };
        // After the interval (measured from t_soon), a non-urgent replace goes through.
        let t_later = t_soon + (min_ms + 5) * 1_000_000;
        strat.apply_decision(&m, Side::Buy, SideDecision::Replace { desired: Box::new(desired.clone()), reason: ReplaceReason::QuantityChanged }, &scale, t_later).await;
        assert!(erx.try_recv().is_err(), "must not send a second replace while the first replace is pending");
        strat.handle_exec_event(ExecEvent::CancelAck { client_id: old_cid }, t_soon + 1);
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: new_cid, venue_order_id: "oid1".into() }, t_soon + 2);
        strat.apply_decision(&m, Side::Buy, SideDecision::Replace { desired: Box::new(desired), reason: ReplaceReason::QuantityChanged }, &scale, t_later).await;
        assert!(matches!(erx.try_recv(), Ok(ExecCommand::Replace { .. })), "non-urgent replace after the interval must go through");
    }

    #[tokio::test]
    async fn live_reduce_only_no_longer_profitable_uses_cancel_only() {
        let account = AccountState::new(dec!(5));
        let (etx, mut erx) = tokio::sync::mpsc::channel(64);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account, ExecMode::Live);
        strat.cfg.live.quote.reduce_position_only = true;
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let desired = match evaluate_side(&edge(), &qcfg(), &books().0, &books().1, Side::Buy, &spec(), 5000, ts(), &PositionContext::unconstrained(), true, None, true) {
            SideDecision::Place(d) => *d,
            other => panic!("expected place, got {other:?}"),
        };
        let t0 = 1_000_000_000_i64;

        let place_cid = strat.orders.next_client_id(&m, Side::Buy).unwrap();
        strat.orders.on_place_sent(&m, Side::Buy, place_cid.clone(), 1000, 10, t0);
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: place_cid, venue_order_id: "oid0".into() }, t0 + 1);

        strat
            .apply_decision(
                &m,
                Side::Buy,
                SideDecision::Replace { desired: Box::new(desired), reason: ReplaceReason::NoLongerProfitable },
                &scale,
                t0 + 2,
            )
            .await;

        assert!(matches!(erx.try_recv(), Ok(ExecCommand::Cancel { .. })), "live reduce-only stale quote must be cancel-only");
        assert!(erx.try_recv().is_err(), "cancel-only path must not enqueue a replacement place");
    }

    #[test]
    fn self_heal_unfreezes_only_after_two_clean_snapshots() {
        // T2.1: a latched freeze clears only when the clean condition (no outstanding hedges +
        // positions reconciled + stream fresh) holds again in a STRICTLY NEWER snapshot.
        use crate::livebot::account::AccountSnapshot;
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone(), ExecMode::Live);
        strat.freeze(0, "test");
        assert!(strat.frozen);
        let now = 1_000_000_000_i64;
        let flat = |src: i64| AccountSnapshot {
            aster_available_usd: dec!(1000),
            hl_withdrawable_usd: dec!(1000),
            aster_equity_usd: dec!(1000),
            hl_equity_usd: dec!(1000),
            hl_unrealized_usd: dec!(0),
            hl_upnl_marked: true,
            aster_positions: vec![],
            hl_positions: vec![],
            open_orders: vec![],
            generation: 0,
            source_ts_ns: src,
            read_start_ns: src,
        };
        // First clean snapshot: records heal_confirm, does NOT unfreeze yet.
        account.publish(flat(now));
        strat.recover_orphans(now + 1_000_000);
        assert!(strat.frozen, "must not unfreeze on the first clean snapshot");
        assert_eq!(strat.heal_confirm, Some(now));
        // A strictly-newer clean snapshot: NOW unfreeze.
        account.publish(flat(now + 500_000_000));
        strat.recover_orphans(now + 501_000_000);
        assert!(!strat.frozen, "must unfreeze once clean persists into a strictly-newer snapshot");
        assert_eq!(strat.heal_confirm, None);
    }

    #[test]
    fn frozen_after_clean_start_is_not_reported_as_not_reconciled() {
        let account = AccountState::new(dec!(5));
        let (etx, mut erx) = tokio::sync::mpsc::channel(128);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account, ExecMode::Live);
        let m: MarketId = "BTC".into();
        strat.mark_clean_start();
        strat.freeze(0, "exec_queue_send_failed");

        assert_eq!(strat.maker_gate_reason(&m, 1_000_000_000), Some(MAKER_GATE_FROZEN));
        assert!(!strat.note_quote_gate(&m, 1_000_000_000));
        assert!(strat.sweep_pending.is_none(), "FROZEN itself must not re-arm safety sweeps");
        assert!(erx.try_recv().is_err(), "FROZEN gate evaluation must not enqueue CancelAllBot");
    }

    #[test]
    fn clean_safety_sweep_is_not_immediately_requeued_by_frozen_gate() {
        use crate::livebot::account::AccountSnapshot;
        let account = AccountState::new(dec!(5));
        let (etx, mut erx) = tokio::sync::mpsc::channel(128);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone(), ExecMode::Live);
        let m: MarketId = "BTC".into();
        strat.mark_clean_start();
        strat.freeze(0, "exec_queue_send_failed");
        strat.sweep_pending = Some(SweepState {
            requested_ns: 100,
            last_attempt_ns: 100,
            reason: "exec_queue_send_failed",
        });

        account.publish(AccountSnapshot {
            aster_available_usd: dec!(1000),
            hl_withdrawable_usd: dec!(1000),
            aster_equity_usd: dec!(1000),
            hl_equity_usd: dec!(1000),
            hl_unrealized_usd: dec!(0),
            hl_upnl_marked: true,
            aster_positions: vec![],
            hl_positions: vec![],
            open_orders: vec![],
            generation: 0,
            source_ts_ns: 200,
            read_start_ns: 200,
        });

        strat.drive_safety_sweep(300);
        assert!(strat.sweep_pending.is_none(), "clean snapshot should clear the pending sweep");
        assert!(!strat.note_quote_gate(&m, 301));
        assert!(strat.sweep_pending.is_none(), "frozen gate must not re-add the just-cleared sweep");
        assert!(erx.try_recv().is_err(), "no new CancelAllBot should be queued");
    }

    #[tokio::test]
    async fn failed_cancel_dispatch_freezes_and_arms_safety_sweep() {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(1);
        let etx_fill = etx.clone();
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account, ExecMode::Live);
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let t0 = 1_000_000_000_i64;

        let cid = strat.orders.next_client_id(&m, Side::Buy).unwrap();
        strat.orders.on_place_sent(&m, Side::Buy, cid.clone(), 1000, 10, t0);
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: cid, venue_order_id: "oid0".into() }, t0 + 1);

        etx_fill.try_send(ExecCommand::RefreshDeadman { market: m.clone() }).unwrap();
        strat
            .apply_decision(&m, Side::Buy, SideDecision::Cancel { reason: ReplaceReason::FeedStale }, &scale, t0 + 2)
            .await;

        assert!(strat.frozen, "a dropped risk-reducing cancel must freeze");
        assert!(strat.sweep_pending.is_some(), "a dropped risk-reducing cancel must arm sweep recovery");
    }

    #[tokio::test]
    async fn sweep_pending_suppresses_targeted_cancel_spam() {
        let account = AccountState::new(dec!(5));
        let (etx, mut erx) = tokio::sync::mpsc::channel(128);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account, ExecMode::Live);
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let t0 = 1_000_000_000_i64;

        let cid = strat.orders.next_client_id(&m, Side::Buy).unwrap();
        strat.orders.on_place_sent(&m, Side::Buy, cid.clone(), 1000, 10, t0);
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: cid, venue_order_id: "oid0".into() }, t0 + 1);
        strat.sweep_pending = Some(SweepState {
            requested_ns: t0 + 2,
            last_attempt_ns: t0 + 2,
            reason: "test_sweep",
        });

        strat
            .apply_decision(&m, Side::Buy, SideDecision::Cancel { reason: ReplaceReason::FeedStale }, &scale, t0 + 3)
            .await;
        assert!(erx.try_recv().is_err(), "targeted cancel must be suppressed while CancelAllBot sweep is pending");
    }

    #[tokio::test]
    async fn pending_cancel_retry_backoff_suppresses_repeat_cancel() {
        let account = AccountState::new(dec!(5));
        let (etx, mut erx) = tokio::sync::mpsc::channel(128);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account, ExecMode::Live);
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let t0 = 1_000_000_000_i64;

        let cid = strat.orders.next_client_id(&m, Side::Buy).unwrap();
        strat.orders.on_place_sent(&m, Side::Buy, cid.clone(), 1000, 10, t0);
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: cid, venue_order_id: "oid0".into() }, t0 + 1);

        strat
            .apply_decision(&m, Side::Buy, SideDecision::Cancel { reason: ReplaceReason::FeedStale }, &scale, t0 + 2)
            .await;
        assert!(matches!(erx.try_recv(), Ok(ExecCommand::Cancel { .. })), "first cancel must send");

        let retry_too_soon = t0 + 2 + (strat.cfg.live.aster.cancel_retry_backoff_ms as i64 - 1) * 1_000_000;
        strat
            .apply_decision(&m, Side::Buy, SideDecision::Cancel { reason: ReplaceReason::FeedStale }, &scale, retry_too_soon)
            .await;
        assert!(erx.try_recv().is_err(), "duplicate cancel must be suppressed until retry backoff expires");
    }

    #[tokio::test]
    async fn urgent_no_longer_profitable_respects_aster_command_budget() {
        let account = AccountState::new(dec!(5));
        let (etx, mut erx) = tokio::sync::mpsc::channel(128);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account, ExecMode::Live);
        strat.cfg.live.aster.max_rest_requests_per_minute = 1;
        strat.cfg.live.aster.optional_rest_reserve_per_minute = 0;
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let desired = match evaluate_side(&edge(), &qcfg(), &books().0, &books().1, Side::Buy, &spec(), 5000, ts(), &PositionContext::unconstrained(), true, None, true) {
            SideDecision::Place(d) => *d,
            other => panic!("expected place, got {other:?}"),
        };
        let t0 = 1_000_000_000_i64;

        strat.apply_decision(&m, Side::Buy, SideDecision::Place(Box::new(desired.clone())), &scale, t0).await;
        let cid = match erx.try_recv() {
            Ok(ExecCommand::Place { client_id, .. }) => client_id,
            other => panic!("initial place should consume the one-command budget, got {other:?}"),
        };
        strat.handle_exec_event(ExecEvent::PlaceAck { client_id: cid, venue_order_id: "oid0".into() }, t0 + 1);

        strat
            .apply_decision(
                &m,
                Side::Buy,
                SideDecision::Replace { desired: Box::new(desired), reason: ReplaceReason::NoLongerProfitable },
                &scale,
                t0 + 2,
            )
            .await;
        assert!(erx.try_recv().is_err(), "urgent cancel-only must not bypass the global Aster REST command budget");
        assert!(strat.frozen, "blocked urgent risk-reducing work must fail closed");
        assert!(strat.sweep_pending.is_some(), "blocked urgent cancel-only must arm sweep/reconcile recovery");
    }

    #[tokio::test]
    async fn aster_rate_limit_event_freezes_and_backs_off_commands() {
        let account = AccountState::new(dec!(5));
        let (etx, mut erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account, ExecMode::Live);
        let m: MarketId = "BTC".into();
        let scale = MarketScale::from_spec(&spec());
        let desired = match evaluate_side(&edge(), &qcfg(), &books().0, &books().1, Side::Buy, &spec(), 5000, ts(), &PositionContext::unconstrained(), true, None, true) {
            SideDecision::Place(d) => *d,
            other => panic!("expected place, got {other:?}"),
        };
        let t0 = 1_000_000_000_i64;

        strat.handle_exec_event(
            ExecEvent::AsterRateLimited { reason: "HTTP 429 code -1003".into(), backoff_ms: 10_000 },
            t0,
        );
        assert!(strat.frozen);
        assert_eq!(strat.aster_429_count, 1);
        assert!(strat.aster_backoff_remaining_ms(t0 + 1_000_000) > 0);

        strat.apply_decision(&m, Side::Buy, SideDecision::Place(Box::new(desired)), &scale, t0 + 1_000_000).await;
        assert!(erx.try_recv().is_err(), "no Aster REST command should be enqueued during 429 backoff");
    }

    #[test]
    fn straddle_guard_skips_recovery_when_snapshot_predates_action() {
        // T2.2: a snapshot whose REST reads BEGAN at-or-before this market's last hot action cannot
        // yet reflect it, so the orphan backstop ignores it — no dispatch AND the persistence gate is
        // not seeded. The boundary is STRICT: read_start == action is also skipped (a same-tick
        // `mono_now_ns()` collision must not be trusted). Only reads that began STRICTLY AFTER the
        // action are trusted (seed the gate).
        use crate::livebot::account::{AccountSnapshot, ScaledPosition, Venue};
        let account = AccountState::new(dec!(50));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, mut hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone(), ExecMode::Live);
        let m: MarketId = "BTC".into();
        let t_action = 1_000_000_000_i64;
        strat.last_hot_action_ns.insert(m.clone(), t_action);
        // Simulate a fill that updated predicted Aster position (hedge failed, so HL stays 0).
        // This ensures the predicted-net cross-check sees the orphan as genuine (both predicted
        // and snapshot agree on the imbalance), not a phantom snapshot glitch.
        strat.aster_pos.insert(m.clone(), SignedPosition { qty: dec!(0.5), avg_px: dec!(100) });
        let orphan = |src: i64, read_start: i64| AccountSnapshot {
            aster_available_usd: dec!(1000),
            hl_withdrawable_usd: dec!(1000),
            aster_equity_usd: dec!(1000),
            hl_equity_usd: dec!(1000),
            hl_unrealized_usd: dec!(0),
            hl_upnl_marked: true,
            aster_positions: vec![ScaledPosition { venue: Venue::Aster, market: m.clone(), signed_qty: dec!(0.5), entry_px: dec!(100) }],
            hl_positions: vec![],
            open_orders: vec![],
            generation: 0,
            source_ts_ns: src,
            read_start_ns: read_start,
        };
        // Reads BEGAN before the action (straddled) => guard SKIPS: no dispatch, gate not seeded.
        account.publish(orphan(t_action + 2_000_000, t_action - 1_000_000));
        strat.recover_orphans(t_action + 3_000_000);
        assert!(hrx.try_recv().is_err(), "straddled snapshot must not dispatch recovery");
        assert!(!strat.orphan_seen.contains_key(&m), "straddled snapshot must not seed the persistence gate");
        // EXACT-EQUALITY boundary: reads that began the SAME tick as the action (read_start == action)
        // are still untrustworthy (a same-instant `mono_now_ns()` collision) => guard SKIPS, gate not seeded.
        account.publish(orphan(t_action + 2_000_000, t_action));
        strat.recover_orphans(t_action + 3_000_000);
        assert!(hrx.try_recv().is_err(), "same-tick snapshot must not dispatch recovery");
        assert!(!strat.orphan_seen.contains_key(&m), "same-tick snapshot (read_start == action) must not seed the gate");
        // Reads BEGAN after the action => trusted: the persistence gate records the first sighting
        // (still no dispatch on the first valid snapshot — recovery needs a confirming snapshot).
        account.publish(orphan(t_action + 6_000_000, t_action + 4_000_000));
        strat.recover_orphans(t_action + 7_000_000);
        assert!(strat.orphan_seen.contains_key(&m), "post-action snapshot must seed the persistence gate");
        assert!(hrx.try_recv().is_err(), "first valid sighting must not dispatch yet");
    }

    fn orphan_snapshot_for(m: &MarketId, qty: Decimal, src: i64, read_start: i64) -> crate::livebot::account::AccountSnapshot {
        use crate::livebot::account::{ScaledPosition, Venue};
        let mut s = crate::livebot::account::AccountSnapshot::empty();
        s.aster_available_usd = dec!(1000);
        s.hl_withdrawable_usd = dec!(1000);
        s.aster_equity_usd = dec!(1000);
        s.hl_equity_usd = dec!(1000);
        s.aster_positions = vec![ScaledPosition { venue: Venue::Aster, market: m.clone(), signed_qty: qty, entry_px: dec!(100) }];
        s.source_ts_ns = src;
        s.read_start_ns = read_start;
        s
    }

    #[test]
    fn recovery_skips_redispatch_while_recovery_in_flight() {
        // An in-flight recovery intent that only partially covers the net must NOT trigger a
        // second overlapping recovery order — skip and defer, keep the outstanding record.
        let account = AccountState::new(dec!(50));
        let (etx, mut erx) = tokio::sync::mpsc::channel(16);
        let (htx, mut hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone(), ExecMode::Live);
        let m: MarketId = "BTC".into();
        strat.aster_pos.insert(m.clone(), SignedPosition { qty: dec!(0.5), avg_px: dec!(100) });
        let cloid = crate::livebot::ids::Cloid::recovery(&m, crate::livebot::fills::cum_scaled(dec!(0.5)));
        let mut intent = HedgeIntent::with_qty(cloid, m.clone(), Side::Sell, dec!(0.2), dec!(100), 1);
        intent.recovery = true;
        intent.mark_submitted(1);
        strat.hedges.insert(cloid.to_hex(), intent);

        // Two confirming snapshots, both read after any hot action (none recorded).
        account.publish(orphan_snapshot_for(&m, dec!(0.5), 10_000_000, 9_000_000));
        strat.recover_orphans(11_000_000);
        account.publish(orphan_snapshot_for(&m, dec!(0.5), 20_000_000_000, 19_000_000_000));
        strat.recover_orphans(21_000_000_000);

        assert!(hrx.try_recv().is_err(), "no second recovery order while one is in flight");
        assert!(erx.try_recv().is_err(), "no flatten either");
        assert!(
            strat.hedges.contains_key(&cloid.to_hex()),
            "outstanding in-flight record must not be overwritten"
        );
    }

    #[test]
    fn recovery_redispatches_salted_cloid_after_dangerous() {
        // After a dangerous (Unknown) recovery attempt, a confirmed persistent orphan may be
        // re-dispatched — but ONLY under a fresh salted cloid (the venue does not dedupe
        // client order indices), and the superseded dangerous record must be removed.
        let account = AccountState::new(dec!(50));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, mut hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone(), ExecMode::Live);
        let m: MarketId = "BTC".into();
        strat.aster_pos.insert(m.clone(), SignedPosition { qty: dec!(0.5), avg_px: dec!(100) });
        let base_cloid = crate::livebot::ids::Cloid::recovery(&m, crate::livebot::fills::cum_scaled(dec!(0.5)));
        let mut dangerous = HedgeIntent::with_qty(base_cloid, m.clone(), Side::Sell, dec!(0.5), dec!(100), 1);
        dangerous.recovery = true;
        dangerous.mark_submitted(1);
        dangerous.mark_unknown();
        strat.hedges.insert(base_cloid.to_hex(), dangerous);
        // Simulate that attempt 0 was already consumed by the dangerous dispatch.
        strat.recovery_attempt_seq.insert(m.clone(), 1);

        account.publish(orphan_snapshot_for(&m, dec!(0.5), 10_000_000, 9_000_000));
        strat.recover_orphans(11_000_000);
        assert!(hrx.try_recv().is_err(), "first sighting must not dispatch");
        account.publish(orphan_snapshot_for(&m, dec!(0.5), 20_000_000_000, 19_000_000_000));
        strat.recover_orphans(21_000_000_000);

        let cmd = hrx.try_recv().expect("confirmed persistent orphan must redispatch");
        let HedgeCommand::Hedge { intent, .. } = cmd else {
            panic!("expected a hedge command");
        };
        assert_ne!(
            intent.cloid.to_hex(),
            base_cloid.to_hex(),
            "redispatch must use a fresh salted cloid, never reuse the dangerous one"
        );
        assert!(
            !strat.hedges.contains_key(&base_cloid.to_hex()),
            "superseded dangerous record must be removed"
        );
        assert!(
            strat.hedges.contains_key(&intent.cloid.to_hex()),
            "new intent must be tracked under the salted cloid"
        );
    }

    // --- circuit breaker ---
    use crate::livebot::account::AccountSnapshot;
    use tokio_util::sync::CancellationToken;

    /// A fresh, flat snapshot whose total cross-venue equity is `total` at monotonic `src`.
    fn equity_snap(total: Decimal, src: i64) -> AccountSnapshot {
        let mut s = AccountSnapshot::empty();
        s.aster_equity_usd = total; // total_equity_usd() = aster + hl; put it all on one venue
        s.hl_equity_usd = Decimal::ZERO;
        s.source_ts_ns = src;
        s.read_start_ns = src;
        s
    }

    fn tmp_trip_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("xemm_cb_{}_{}.trip.json", std::process::id(), tag))
    }

    #[test]
    fn circuit_breaker_trips_on_equity_drawdown_and_latches() {
        let account = AccountState::new(dec!(5));
        let (etx, mut erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        let mut strat = live_strat(etx, htx, account.clone(), ExecMode::Live);
        strat.cfg.live.circuit_breaker.enabled = true;
        strat.cfg.live.circuit_breaker.max_cumulative_loss_usdc = dec!(5);
        let tok = CancellationToken::new();
        let trip = tmp_trip_path("trips");
        let _ = std::fs::remove_file(&trip);
        strat.arm_circuit_breaker(trip.clone(), tok.clone());

        let mut t = 1_000_000_000_i64;
        // Baseline arms from the MEDIAN of the first K fresh marked samples; not before.
        for _ in 0..BREAKER_BASELINE_SAMPLES {
            assert!(strat.breaker_baseline_equity.is_none());
            account.publish(equity_snap(dec!(100), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert_eq!(strat.breaker_baseline_equity, Some(dec!(100)));
        assert!(!strat.breaker_tripped);
        assert!(!tok.is_cancelled());

        // Drawdown within the limit (loss 4 <= 5) does NOT trip.
        account.publish(equity_snap(dec!(96), t));
        strat.check_circuit_breaker(t);
        t += 1_000_000;
        assert!(!strat.breaker_tripped);
        assert!(!tok.is_cancelled());

        // Drawdown beyond the limit (loss 6 > 5) must PERSIST: breaches 1 and 2 warn only...
        for expect_streak in 1..BREAKER_TRIP_STREAK {
            account.publish(equity_snap(dec!(94), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
            assert_eq!(strat.breaker_breach_streak, expect_streak);
            assert!(!strat.breaker_tripped);
            assert!(!tok.is_cancelled());
        }
        // ...the Nth consecutive breach TRIPS: cancels shutdown + writes the latch.
        account.publish(equity_snap(dec!(94), t));
        strat.check_circuit_breaker(t);
        t += 1_000_000;
        assert!(strat.breaker_tripped);
        assert!(tok.is_cancelled(), "breaker must cancel the shutdown token to halt the process");
        assert!(trip.exists(), "breaker must write the trip latch");
        assert!(matches!(erx.try_recv().unwrap(), ExecCommand::CancelAllBot));

        // Latched: a further reading neither un-trips nor rewrites the latch.
        let latch = std::fs::read_to_string(&trip).unwrap();
        account.publish(equity_snap(dec!(80), t));
        strat.check_circuit_breaker(t);
        assert!(strat.breaker_tripped);
        assert_eq!(std::fs::read_to_string(&trip).unwrap(), latch, "latch must not be rewritten");
        let _ = std::fs::remove_file(&trip);
    }

    #[test]
    fn circuit_breaker_inert_in_paper_and_never_trips_on_stale_or_zero() {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(16);
        let (htx, _hrx) = tokio::sync::mpsc::channel(16);
        // Paper mode: breaker is gated off entirely even with a huge drawdown.
        let mut strat = live_strat(etx, htx, account.clone(), ExecMode::Paper);
        strat.cfg.live.circuit_breaker.enabled = true;
        strat.cfg.live.circuit_breaker.max_cumulative_loss_usdc = dec!(5);
        let tok = CancellationToken::new();
        strat.arm_circuit_breaker(tmp_trip_path("paper"), tok.clone());
        let now = 2_000_000_000_i64;
        account.publish(equity_snap(dec!(100), now));
        strat.check_circuit_breaker(now);
        account.publish(equity_snap(dec!(10), now + 1_000_000));
        strat.check_circuit_breaker(now + 1_000_000);
        assert!(!strat.breaker_tripped, "paper mode must never trip");
        assert!(!tok.is_cancelled());
        assert!(strat.breaker_baseline_equity.is_none(), "paper mode must not even arm a baseline");

        // Live, but a STALE snapshot (age > max_account_snapshot_age_ms) must never trip.
        let mut strat2 = live_strat(
            tokio::sync::mpsc::channel(16).0,
            tokio::sync::mpsc::channel(16).0,
            account.clone(),
            ExecMode::Live,
        );
        strat2.cfg.live.circuit_breaker.enabled = true;
        strat2.cfg.live.circuit_breaker.max_cumulative_loss_usdc = dec!(5);
        let tok2 = CancellationToken::new();
        strat2.arm_circuit_breaker(tmp_trip_path("stale"), tok2.clone());
        account.publish(equity_snap(dec!(100), now));
        // now_ns is far ahead of the snapshot's source_ts_ns => age >> max age => skip (no baseline).
        let way_later = now + 10_000 * 1_000_000;
        strat2.check_circuit_breaker(way_later);
        assert!(strat2.breaker_baseline_equity.is_none(), "stale snapshot must not arm/trip");
        assert_eq!(strat2.breaker_breach_streak, 0, "stale sample must reset the breach streak");
        assert!(!tok2.is_cancelled());
    }

    /// A live strategy with the breaker enabled (limit 5) and an armed baseline of 100,
    /// built from `BREAKER_BASELINE_SAMPLES` fresh publishes. Returns (strat, account,
    /// token, next monotonic ns).
    fn armed_breaker_strat(
        tag: &str,
    ) -> (Strategy, AccountState, CancellationToken, i64) {
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(64);
        let (htx, _hrx) = tokio::sync::mpsc::channel(64);
        let mut strat = live_strat(etx, htx, account.clone(), ExecMode::Live);
        strat.cfg.live.circuit_breaker.enabled = true;
        strat.cfg.live.circuit_breaker.max_cumulative_loss_usdc = dec!(5);
        let tok = CancellationToken::new();
        let trip = tmp_trip_path(tag);
        let _ = std::fs::remove_file(&trip);
        strat.arm_circuit_breaker(trip, tok.clone());
        let mut t = 3_000_000_000_i64;
        for _ in 0..BREAKER_BASELINE_SAMPLES {
            account.publish(equity_snap(dec!(100), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert_eq!(strat.breaker_baseline_equity, Some(dec!(100)));
        (strat, account, tok, t)
    }

    #[test]
    fn breaker_outlier_sample_does_not_trip() {
        let (mut strat, account, tok, mut t) = armed_breaker_strat("outlier");
        // Two breaching samples, then a recovered one: the streak resets.
        for _ in 0..2 {
            account.publish(equity_snap(dec!(94), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert_eq!(strat.breaker_breach_streak, 2);
        account.publish(equity_snap(dec!(100), t));
        strat.check_circuit_breaker(t);
        t += 1_000_000;
        assert_eq!(strat.breaker_breach_streak, 0);
        // Two more breaches still don't trip (the earlier pair must not carry over)...
        for _ in 0..2 {
            account.publish(equity_snap(dec!(94), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert!(!strat.breaker_tripped);
        assert!(!tok.is_cancelled());
        // ...but a third consecutive one does.
        account.publish(equity_snap(dec!(94), t));
        strat.check_circuit_breaker(t);
        assert!(strat.breaker_tripped);
        let _ = std::fs::remove_file(tmp_trip_path("outlier"));
    }

    #[test]
    fn breaker_stale_sample_resets_streak() {
        let (mut strat, account, tok, mut t) = armed_breaker_strat("stale_reset");
        for _ in 0..2 {
            account.publish(equity_snap(dec!(94), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert_eq!(strat.breaker_breach_streak, 2);
        // The same (last) snapshot seen way later is stale: the streak must reset.
        strat.check_circuit_breaker(t + 10_000 * 1_000_000);
        assert_eq!(strat.breaker_breach_streak, 0);
        // Two fresh breaches after the gap must not trip (need a full new streak).
        t += 11_000 * 1_000_000;
        for _ in 0..2 {
            account.publish(equity_snap(dec!(94), t));
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert!(!strat.breaker_tripped);
        assert!(!tok.is_cancelled());
        let _ = std::fs::remove_file(tmp_trip_path("stale_reset"));
    }

    #[test]
    fn breaker_unmarked_sample_resets_streak_and_never_arms() {
        // Unmarked samples must never ARM a baseline...
        let account = AccountState::new(dec!(5));
        let (etx, _erx) = tokio::sync::mpsc::channel(64);
        let (htx, _hrx) = tokio::sync::mpsc::channel(64);
        let mut strat = live_strat(etx, htx, account.clone(), ExecMode::Live);
        strat.cfg.live.circuit_breaker.enabled = true;
        strat.cfg.live.circuit_breaker.max_cumulative_loss_usdc = dec!(5);
        let tok = CancellationToken::new();
        strat.arm_circuit_breaker(tmp_trip_path("unmarked_arm"), tok.clone());
        let mut t = 4_000_000_000_i64;
        for _ in 0..(BREAKER_BASELINE_SAMPLES + 2) {
            let mut s = equity_snap(dec!(100), t);
            s.hl_upnl_marked = false;
            account.publish(s);
            strat.check_circuit_breaker(t);
            t += 1_000_000;
        }
        assert!(strat.breaker_baseline_equity.is_none(), "unmarked samples must not arm");

        // ...and must RESET an in-progress breach streak, never count toward a trip.
        let (mut strat2, account2, tok2, mut t2) = armed_breaker_strat("unmarked_reset");
        for _ in 0..2 {
            account2.publish(equity_snap(dec!(94), t2));
            strat2.check_circuit_breaker(t2);
            t2 += 1_000_000;
        }
        assert_eq!(strat2.breaker_breach_streak, 2);
        let mut s = equity_snap(dec!(94), t2);
        s.hl_upnl_marked = false;
        account2.publish(s);
        strat2.check_circuit_breaker(t2);
        t2 += 1_000_000;
        assert_eq!(strat2.breaker_breach_streak, 0, "unmarked sample must reset the streak");
        for _ in 0..2 {
            account2.publish(equity_snap(dec!(94), t2));
            strat2.check_circuit_breaker(t2);
            t2 += 1_000_000;
        }
        assert!(!strat2.breaker_tripped);
        assert!(!tok2.is_cancelled());
        let _ = std::fs::remove_file(tmp_trip_path("unmarked_reset"));
    }

    #[test]
    fn breaker_same_generation_not_double_counted() {
        let (mut strat, account, tok, t) = armed_breaker_strat("same_gen");
        // ONE breaching snapshot observed on three consecutive ticks counts once.
        account.publish(equity_snap(dec!(94), t));
        for i in 0..3 {
            strat.check_circuit_breaker(t + i * 1_000_000);
        }
        assert_eq!(strat.breaker_breach_streak, 1, "one published sample must count once");
        assert!(!strat.breaker_tripped);
        assert!(!tok.is_cancelled());
        let _ = std::fs::remove_file(tmp_trip_path("same_gen"));
    }
}
