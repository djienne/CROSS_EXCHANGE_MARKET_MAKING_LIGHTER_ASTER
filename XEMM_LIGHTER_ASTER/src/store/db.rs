//! SQLite persistence: a thin `Db` wrapper with typed row structs and
//! transparent transaction batching (inserts are buffered and committed every
//! `BATCH` rows; call [`Db::flush`] at the end). Row constructors map the domain
//! types onto the schema so the engine stays terse.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rusqlite::{params, Connection};
use uuid::Uuid;

use crate::edge::EdgeConfig;
use crate::fill_sweep::SimulatedAsterFill;
use crate::hedge::{HedgeResult, PendingHedge};
use crate::markets::MarketSpec;
use crate::quote_engine::DesiredQuote;
use crate::types::{MarketId, QueueModel, RejectReason, Side};

use super::schema::{PRAGMAS, SCHEMA};

const BATCH: usize = 5_000;

/// In-memory aggregate of the accepted (place/requote) opportunity stream for one
/// (market, side, queue_model). The report only reads these back as SUM/COUNT/AVG, so
/// we fold the firehose into counters here and write one summary row at run end
/// instead of persisting millions of rows. Rejects are NOT aggregated — they are kept
/// per-row (see [`Db::record_opportunity`]). Sums are accumulated in event order so
/// `sum/accepted` reproduces the old `AVG(CAST(... AS REAL))` to display precision.
#[derive(Default)]
struct OppAgg {
    accepted: i64,
    sum_instant_edge_bps: f64,
    sum_distance_bps: f64,
    size_clamped: i64,
    queue_truncated: i64,
}

pub struct Db {
    conn: Connection,
    run_id: String,
    tx_open: bool,
    writes: usize,
    /// Keyed by (market, side, queue_model); flushed to `opportunity_stats` at run end.
    opp_aggs: HashMap<(String, String, String), OppAgg>,
}

// --- small conversion helpers ---
fn s(d: Decimal) -> String {
    d.to_string()
}
fn os(d: Option<Decimal>) -> Option<String> {
    d.map(|x| x.to_string())
}
fn t(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339()
}
fn ot(dt: Option<DateTime<Utc>>) -> Option<String> {
    dt.map(|x| x.to_rfc3339())
}
fn bit(x: bool) -> i64 {
    i64::from(x)
}

impl Db {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).ok();
            }
        }
        let conn = Connection::open(path).with_context(|| format!("opening db {}", path.display()))?;
        conn.execute_batch(PRAGMAS)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Db {
            conn,
            run_id: String::new(),
            tx_open: false,
            writes: 0,
            opp_aggs: HashMap::new(),
        })
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    fn ensure_tx(&mut self) -> Result<()> {
        if !self.tx_open {
            self.conn.execute_batch("BEGIN")?;
            self.tx_open = true;
        }
        Ok(())
    }

    fn after_write(&mut self) -> Result<()> {
        self.writes += 1;
        if self.writes >= BATCH {
            self.conn.execute_batch("COMMIT")?;
            self.tx_open = false;
            self.writes = 0;
        }
        Ok(())
    }

    /// Commit any buffered writes. Call at the end of a run.
    pub fn flush(&mut self) -> Result<()> {
        if self.tx_open {
            self.conn.execute_batch("COMMIT")?;
            self.tx_open = false;
            self.writes = 0;
        }
        Ok(())
    }

    pub fn insert_run(
        &mut self,
        run_id: &str,
        started_at: DateTime<Utc>,
        mode: &str,
        events_path: Option<&str>,
        code_version: &str,
        config_json: &str,
    ) -> Result<()> {
        self.run_id = run_id.to_string();
        self.ensure_tx()?;
        self.conn.execute(
            "INSERT OR REPLACE INTO runs (run_id, started_at, finished_at, mode, events_path, code_version, config_json)
             VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6)",
            params![run_id, t(started_at), mode, events_path, code_version, config_json],
        )?;
        self.after_write()
    }

    pub fn finish_run(&mut self, finished_at: DateTime<Utc>) -> Result<()> {
        self.flush_opportunity_stats()?;
        self.ensure_tx()?;
        self.conn.execute(
            "UPDATE runs SET finished_at = ?1 WHERE run_id = ?2",
            params![t(finished_at), self.run_id],
        )?;
        self.after_write()
    }

    /// Write the folded accepted-opportunity aggregates as one summary row per
    /// (market, side, queue_model). Called once at run end by [`finish_run`]; the rows
    /// are committed by the subsequent [`flush`]. Counts are small (markets x 2 x
    /// models), so a single transaction without batching is fine.
    fn flush_opportunity_stats(&mut self) -> Result<()> {
        let aggs = std::mem::take(&mut self.opp_aggs);
        let run_id = self.run_id.clone();
        self.ensure_tx()?;
        for ((market, side, queue_model), agg) in &aggs {
            self.conn.execute(
                "INSERT OR REPLACE INTO opportunity_stats
                 (run_id, market, side, queue_model, accepted, sum_instant_edge_bps, sum_distance_bps, size_clamped, queue_truncated)
                 VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![
                    run_id, market, side, queue_model,
                    agg.accepted, agg.sum_instant_edge_bps, agg.sum_distance_bps, agg.size_clamped, agg.queue_truncated
                ],
            )?;
        }
        Ok(())
    }

    pub fn insert_market(&mut self, spec: &MarketSpec) -> Result<()> {
        self.ensure_tx()?;
        self.conn.execute(
            "INSERT OR REPLACE INTO markets
             (run_id, market, aster_symbol, hl_coin, tick_size, step_size, aster_min_qty, aster_min_notional, hl_sz_decimals, hl_qty_step, hl_min_notional)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                self.run_id, spec.market_id.0, spec.aster_symbol, spec.hl_coin,
                s(spec.tick), s(spec.step), s(spec.aster_min_qty), s(spec.aster_min_notional),
                spec.hl_sz_decimals, s(spec.hl_qty_step), s(spec.hl_min_notional),
            ],
        )?;
        self.after_write()
    }

    /// Record one opportunity. Accepted (place/requote) events are folded into the
    /// in-memory [`OppAgg`] counters (written by [`finish_run`]); rejects are kept
    /// per-row in `opportunity_rejects` with their timestamp. The engine logs rejects
    /// only when the reason changes, so the per-row stream stays sparse.
    pub fn record_opportunity(&mut self, r: &OpportunityRow) -> Result<()> {
        if r.accepted {
            let agg = self
                .opp_aggs
                .entry((
                    r.market.0.clone(),
                    r.side.as_str().to_string(),
                    r.queue_model.as_str().to_string(),
                ))
                .or_default();
            agg.accepted += 1;
            if let Some(e) = r.instant_edge_bps {
                agg.sum_instant_edge_bps += e.to_f64().unwrap_or(0.0);
            }
            if let Some(d) = r.distance_from_touch_bps {
                agg.sum_distance_bps += d.to_f64().unwrap_or(0.0);
            }
            if r.size_clamped_up {
                agg.size_clamped += 1;
            }
            if r.queue_truncated {
                agg.queue_truncated += 1;
            }
            return Ok(());
        }
        self.ensure_tx()?;
        self.conn.execute(
            "INSERT INTO opportunity_rejects
             (run_id, market, side, queue_model, reject_reason, event_ts)
             VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                self.run_id, r.market.0, r.side.as_str(), r.queue_model.as_str(),
                r.reject_reason.map(|x| x.as_str()), t(r.event_ts),
            ],
        )?;
        self.after_write()
    }

    pub fn insert_quote_revision(&mut self, r: &QuoteRevisionRow) -> Result<()> {
        self.ensure_tx()?;
        self.conn.execute(
            "INSERT INTO quote_revisions
             (id, run_id, market, side, queue_model, previous_quote_id, new_quote_id, reason,
              previous_price, new_price, previous_instant_edge_bps, new_instant_edge_bps, event_ts)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![
                r.id, self.run_id, r.market.0, r.side.as_str(), r.queue_model.as_str(),
                r.previous_quote_id, r.new_quote_id, r.reason,
                os(r.previous_price), os(r.new_price), os(r.previous_instant_edge_bps), os(r.new_instant_edge_bps),
                t(r.event_ts),
            ],
        )?;
        self.after_write()
    }

    pub fn insert_fill(&mut self, r: &FillRow) -> Result<()> {
        self.ensure_tx()?;
        self.conn.execute(
            "INSERT INTO simulated_fills
             (id, run_id, quote_id, market, queue_model, aster_side, fill_px, fill_qty, sweep_print_px,
              quoted_edge_bps, quoted_distance_bps,
              remaining_quote_qty_after_fill, was_trade_through, was_partial, feed_stale_at_fill, queue_truncated,
              aster_pos_notional, hl_pos_notional, exch_ts, local_recv_ts)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
            params![
                r.id, self.run_id, r.quote_id, r.market.0, r.queue_model.as_str(), r.aster_side.as_str(),
                s(r.fill_px), s(r.fill_qty), s(r.sweep_print_px), s(r.quoted_edge_bps), s(r.quoted_distance_bps),
                s(r.remaining_quote_qty_after_fill),
                bit(r.was_trade_through), bit(r.was_partial), bit(r.feed_stale_at_fill), bit(r.queue_truncated),
                os(r.aster_pos_notional), os(r.hl_pos_notional),
                t(r.exch_ts), t(r.local_recv_ts),
            ],
        )?;
        self.after_write()
    }

    pub fn insert_hedge(&mut self, r: &HedgeRow) -> Result<()> {
        self.ensure_tx()?;
        self.conn.execute(
            "INSERT INTO hedges
             (id, run_id, fill_id, market, queue_model, hedge_side, qty, filled_qty, aster_fill_px, hl_vwap, latency_bucket_ms,
              gross_pnl, aster_fee, hl_fee, net_pnl, realized_edge_bps, hl_slippage_bps, depth_exhausted,
              hedged_on_stale_book, fill_local_ts, resolve_ts, hl_book_ts, reason)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23)",
            params![
                r.id, self.run_id, r.fill_id, r.market.0, r.queue_model.as_str(), r.hedge_side.as_str(),
                s(r.qty), s(r.filled_qty), s(r.aster_fill_px), s(r.hl_vwap), r.latency_bucket_ms,
                s(r.gross_pnl), s(r.aster_fee), s(r.hl_fee), s(r.net_pnl), s(r.realized_edge_bps),
                os(r.hl_slippage_bps), bit(r.depth_exhausted), bit(r.hedged_on_stale_book),
                t(r.fill_local_ts), t(r.resolve_ts), t(r.hl_book_ts), r.reason.clone(),
            ],
        )?;
        self.after_write()
    }

    pub fn insert_pending_event(&mut self, r: &PendingEventRow) -> Result<()> {
        self.ensure_tx()?;
        self.conn.execute(
            "INSERT INTO pending_inventory_events
             (id, run_id, market, queue_model, event_type, signed_qty, avg_aster_px, mark_px, pending_notional,
              realized_pnl, first_fill_ts, last_fill_ts, event_ts, reason)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
            params![
                r.id, self.run_id, r.market.0, r.queue_model.as_str(), r.event_type,
                s(r.signed_qty), s(r.avg_aster_px), os(r.mark_px), s(r.pending_notional),
                os(r.realized_pnl), ot(r.first_fill_ts), ot(r.last_fill_ts), t(r.event_ts), r.reason.clone(),
            ],
        )?;
        self.after_write()
    }

    /// Row count of a table (test/diagnostic helper). The table name is validated
    /// against a fixed allowlist — it is never derived from untrusted input, but the
    /// allowlist keeps this string-interpolated SQL from ever becoming an injection
    /// vector (clippy/readers flag the pattern otherwise).
    pub fn count(&self, table: &str) -> Result<i64> {
        const TABLES: &[&str] = &[
            "runs",
            "markets",
            "opportunity_stats",
            "opportunity_rejects",
            "quote_revisions",
            "simulated_fills",
            "hedges",
            "pending_inventory_events",
        ];
        if !TABLES.contains(&table) {
            anyhow::bail!("count: unknown table {table:?}");
        }
        let n: i64 = self
            .conn
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0))?;
        Ok(n)
    }

    /// Borrow the underlying connection (read queries in the report phase).
    pub fn conn(&self) -> &Connection {
        &self.conn
    }
}

// --------------------------------------------------------------------------
// Row structs + constructors mapping domain types -> schema columns.
// --------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct OpportunityRow {
    pub id: String,
    pub market: MarketId,
    pub side: Side,
    pub queue_model: QueueModel,
    pub accepted: bool,
    pub reject_reason: Option<RejectReason>,
    pub ref_px: Option<Decimal>,
    pub aster_mid: Option<Decimal>,
    pub hl_mid: Option<Decimal>,
    pub quote_px: Option<Decimal>,
    pub qty: Option<Decimal>,
    pub hedge_side: Option<Side>,
    pub expected_hl_vwap: Option<Decimal>,
    pub expected_hl_depth_filled_qty: Option<Decimal>,
    pub expected_hl_slippage_bps: Option<Decimal>,
    pub expected_hl_worst_px: Option<Decimal>,
    pub expected_hl_depth_levels_used: Option<usize>,
    pub instant_edge_bps: Option<Decimal>,
    pub profitable_bound_px: Option<Decimal>,
    pub post_only_constraint_px: Option<Decimal>,
    pub required_bps: Option<Decimal>,
    pub min_net_profit_bps: Option<Decimal>,
    pub slippage_buffer_bps: Option<Decimal>,
    pub latency_buffer_bps: Option<Decimal>,
    pub basis_buffer_bps: Option<Decimal>,
    pub funding_buffer_bps: Option<Decimal>,
    pub better_levels_qty: Option<Decimal>,
    pub same_level_queue_ahead_qty: Option<Decimal>,
    pub total_ahead_qty: Option<Decimal>,
    pub distance_from_touch_bps: Option<Decimal>,
    pub effective_aster_touch_px: Option<Decimal>,
    pub depth_liquidity_multiple: Option<Decimal>,
    pub depth_target_qty: Option<Decimal>,
    pub aster_depth_filled_qty: Option<Decimal>,
    pub aster_depth_levels_used: Option<usize>,
    /// The order was clamped up to the venue minimum lot (desired_notional too small).
    pub size_clamped_up: bool,
    /// The accepted quote rests beyond Aster's captured depth20 (queue under-observed).
    pub queue_truncated: bool,
    pub event_ts: DateTime<Utc>,
}

impl OpportunityRow {
    pub fn accepted(
        market: MarketId,
        queue_model: QueueModel,
        dq: &DesiredQuote,
        edge: &EdgeConfig,
        event_ts: DateTime<Utc>,
    ) -> Self {
        OpportunityRow {
            id: Uuid::new_v4().to_string(),
            market,
            side: dq.aster_side,
            queue_model,
            accepted: true,
            reject_reason: None,
            ref_px: Some(dq.ref_px),
            aster_mid: Some(dq.aster_mid),
            hl_mid: Some(dq.hl_mid),
            quote_px: Some(dq.price),
            qty: Some(dq.qty),
            hedge_side: Some(dq.hedge_side),
            expected_hl_vwap: Some(dq.expected_hl_vwap),
            expected_hl_depth_filled_qty: Some(dq.expected_hl_depth_filled_qty),
            expected_hl_slippage_bps: Some(dq.expected_hl_slippage_bps),
            expected_hl_worst_px: Some(dq.expected_hl_worst_px),
            expected_hl_depth_levels_used: Some(dq.expected_hl_depth_levels_used),
            instant_edge_bps: Some(dq.instant_edge_bps),
            profitable_bound_px: Some(dq.profitable_bound_px),
            post_only_constraint_px: Some(dq.post_only_constraint_px),
            required_bps: Some(dq.required_bps),
            min_net_profit_bps: Some(edge.min_net_profit_bps),
            slippage_buffer_bps: Some(edge.slippage_buffer_bps),
            latency_buffer_bps: Some(edge.latency_buffer_bps),
            basis_buffer_bps: Some(edge.basis_buffer_bps),
            funding_buffer_bps: Some(edge.funding_buffer_bps),
            better_levels_qty: Some(dq.better_levels_qty),
            same_level_queue_ahead_qty: Some(dq.queue_ahead_qty),
            total_ahead_qty: Some(dq.total_ahead_qty()),
            distance_from_touch_bps: Some(dq.distance_from_touch_bps),
            effective_aster_touch_px: Some(dq.effective_aster_touch_px),
            depth_liquidity_multiple: Some(dq.depth_liquidity_multiple),
            depth_target_qty: Some(dq.depth_target_qty),
            aster_depth_filled_qty: Some(dq.aster_depth_filled_qty),
            aster_depth_levels_used: Some(dq.aster_depth_levels_used),
            size_clamped_up: dq.size_clamped_up,
            queue_truncated: dq.queue_truncated,
            event_ts,
        }
    }

    pub fn rejected(
        market: MarketId,
        side: Side,
        queue_model: QueueModel,
        reason: RejectReason,
        event_ts: DateTime<Utc>,
    ) -> Self {
        OpportunityRow {
            id: Uuid::new_v4().to_string(),
            market,
            side,
            queue_model,
            accepted: false,
            reject_reason: Some(reason),
            ref_px: None,
            aster_mid: None,
            hl_mid: None,
            quote_px: None,
            qty: None,
            hedge_side: None,
            expected_hl_vwap: None,
            expected_hl_depth_filled_qty: None,
            expected_hl_slippage_bps: None,
            expected_hl_worst_px: None,
            expected_hl_depth_levels_used: None,
            instant_edge_bps: None,
            profitable_bound_px: None,
            post_only_constraint_px: None,
            required_bps: None,
            min_net_profit_bps: None,
            slippage_buffer_bps: None,
            latency_buffer_bps: None,
            basis_buffer_bps: None,
            funding_buffer_bps: None,
            better_levels_qty: None,
            same_level_queue_ahead_qty: None,
            total_ahead_qty: None,
            distance_from_touch_bps: None,
            effective_aster_touch_px: None,
            depth_liquidity_multiple: None,
            depth_target_qty: None,
            aster_depth_filled_qty: None,
            aster_depth_levels_used: None,
            size_clamped_up: false,
            queue_truncated: false,
            event_ts,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuoteRevisionRow {
    pub id: String,
    pub market: MarketId,
    pub side: Side,
    pub queue_model: QueueModel,
    pub previous_quote_id: Option<String>,
    pub new_quote_id: Option<String>,
    pub reason: String,
    pub previous_price: Option<Decimal>,
    pub new_price: Option<Decimal>,
    pub previous_instant_edge_bps: Option<Decimal>,
    pub new_instant_edge_bps: Option<Decimal>,
    pub event_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct FillRow {
    pub id: String,
    pub quote_id: String,
    pub market: MarketId,
    pub queue_model: QueueModel,
    pub aster_side: Side,
    pub fill_px: Decimal,
    pub fill_qty: Decimal,
    pub sweep_print_px: Decimal,
    /// The resting quote's quoted spread at fill time ("spread used" for this trade).
    pub quoted_edge_bps: Decimal,
    pub quoted_distance_bps: Decimal,
    pub remaining_quote_qty_after_fill: Decimal,
    pub was_trade_through: bool,
    pub was_partial: bool,
    /// The matched feed was stale when this fill landed (stale-window adverse fill).
    pub feed_stale_at_fill: bool,
    /// The quote rested beyond Aster's captured depth20 (queue ahead under-observed).
    pub queue_truncated: bool,
    /// Signed Aster / HL leg position notional after this fill (set by the engine).
    pub aster_pos_notional: Option<Decimal>,
    pub hl_pos_notional: Option<Decimal>,
    pub exch_ts: DateTime<Utc>,
    pub local_recv_ts: DateTime<Utc>,
}

impl FillRow {
    pub fn from_fill(f: &SimulatedAsterFill, queue_model: QueueModel) -> Self {
        FillRow {
            id: f.id.to_string(),
            quote_id: f.quote_id.to_string(),
            market: f.market.clone(),
            queue_model,
            aster_side: f.aster_side,
            fill_px: f.fill_px,
            fill_qty: f.fill_qty,
            sweep_print_px: f.sweep_print_px,
            quoted_edge_bps: f.quoted_edge_bps,
            quoted_distance_bps: f.quoted_distance_bps,
            remaining_quote_qty_after_fill: f.remaining_quote_qty_after_fill,
            was_trade_through: f.was_trade_through,
            was_partial: f.was_partial,
            feed_stale_at_fill: f.feed_stale_at_fill,
            queue_truncated: f.queue_truncated,
            aster_pos_notional: None,
            hl_pos_notional: None,
            exch_ts: f.exch_ts,
            local_recv_ts: f.local_recv_ts,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HedgeRow {
    pub id: String,
    pub fill_id: Option<String>,
    pub market: MarketId,
    pub queue_model: QueueModel,
    pub hedge_side: Side,
    pub qty: Decimal,
    pub filled_qty: Decimal,
    pub aster_fill_px: Decimal,
    pub hl_vwap: Decimal,
    pub latency_bucket_ms: i64,
    pub gross_pnl: Decimal,
    pub aster_fee: Decimal,
    pub hl_fee: Decimal,
    pub net_pnl: Decimal,
    pub realized_edge_bps: Decimal,
    pub hl_slippage_bps: Option<Decimal>,
    pub depth_exhausted: bool,
    pub hedged_on_stale_book: bool,
    pub fill_local_ts: DateTime<Utc>,
    pub resolve_ts: DateTime<Utc>,
    pub hl_book_ts: DateTime<Utc>,
    /// Non-NULL only for an exceptional resolution (e.g. MISSING_HL_BOOK).
    pub reason: Option<String>,
}

impl HedgeRow {
    pub fn from_result(h: &HedgeResult) -> Self {
        HedgeRow {
            id: h.id.to_string(),
            fill_id: Some(h.fill_id.to_string()),
            market: h.market.clone(),
            queue_model: h.queue_model,
            hedge_side: h.hedge_side,
            qty: h.qty,
            filled_qty: h.filled_qty,
            aster_fill_px: h.aster_fill_px,
            hl_vwap: h.hl_vwap,
            latency_bucket_ms: h.latency_bucket_ms,
            gross_pnl: h.gross_pnl,
            aster_fee: h.aster_fee,
            hl_fee: h.hl_fee,
            net_pnl: h.net_pnl,
            realized_edge_bps: h.realized_edge_bps,
            hl_slippage_bps: Some(h.hl_slippage_bps),
            depth_exhausted: h.depth_exhausted,
            hedged_on_stale_book: h.hedged_on_stale_book,
            fill_local_ts: h.fill_local_ts,
            resolve_ts: h.resolve_ts,
            hl_book_ts: h.hl_book_ts,
            reason: None,
        }
    }

    /// A hedge that could not be priced because no HL book was available at resolve
    /// time. Recorded (never dropped) with `filled_qty = 0` and a reason, so every
    /// scheduled hedge maps to exactly one row and the unhedged exposure stays visible.
    pub fn unbooked(ph: &PendingHedge, reason: &str) -> Self {
        HedgeRow {
            id: Uuid::new_v4().to_string(),
            fill_id: Some(ph.fill_id.to_string()),
            market: ph.market.clone(),
            queue_model: ph.queue_model,
            hedge_side: ph.hedge_side,
            qty: ph.qty,
            filled_qty: Decimal::ZERO,
            aster_fill_px: ph.aster_ref_px,
            hl_vwap: ph.aster_ref_px, // display placeholder; no book existed
            latency_bucket_ms: ph.latency_bucket_ms,
            gross_pnl: Decimal::ZERO,
            aster_fee: Decimal::ZERO,
            hl_fee: Decimal::ZERO,
            net_pnl: Decimal::ZERO,
            realized_edge_bps: Decimal::ZERO,
            hl_slippage_bps: None,
            depth_exhausted: true,
            hedged_on_stale_book: false,
            fill_local_ts: ph.fill_local_ts,
            resolve_ts: ph.resolve_at,
            hl_book_ts: ph.resolve_at, // no book; placeholder (column is NOT NULL)
            reason: Some(reason.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PendingEventRow {
    pub id: String,
    pub market: MarketId,
    pub queue_model: QueueModel,
    pub event_type: String,
    pub signed_qty: Decimal,
    pub avg_aster_px: Decimal,
    pub mark_px: Option<Decimal>,
    pub pending_notional: Decimal,
    pub realized_pnl: Option<Decimal>,
    pub first_fill_ts: Option<DateTime<Utc>>,
    pub last_fill_ts: Option<DateTime<Utc>>,
    pub event_ts: DateTime<Utc>,
    pub reason: Option<String>,
}

impl PendingEventRow {
    pub fn new(
        market: MarketId,
        queue_model: QueueModel,
        event_type: &str,
        signed_qty: Decimal,
        avg_aster_px: Decimal,
        pending_notional: Decimal,
        event_ts: DateTime<Utc>,
    ) -> Self {
        PendingEventRow {
            id: Uuid::new_v4().to_string(),
            market,
            queue_model,
            event_type: event_type.to_string(),
            signed_qty,
            avg_aster_px,
            mark_px: None,
            pending_notional,
            realized_pnl: None,
            first_fill_ts: None,
            last_fill_ts: None,
            event_ts,
            reason: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn ts() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    #[test]
    fn schema_inits_and_inserts_roundtrip() {
        let dir = std::env::temp_dir().join(format!("xemm_test_{}.sqlite", Uuid::new_v4()));
        let mut db = Db::open(&dir).unwrap();
        db.insert_run("run1", ts(), "replay", Some("x.jsonl"), "test", "{}").unwrap();
        db.insert_market(&MarketSpec {
            market_id: "BTC".into(),
            aster_symbol: "BTCUSDT".into(),
            hl_coin: "BTC".into(),
            lighter_market_id: 1,
            lighter_price_decimals: 1,
            lighter_size_decimals: 5,
            lighter_price_tick: dec!(0.1),
            tick: dec!(0.1),
            step: dec!(0.001),
            aster_min_qty: dec!(0.001),
            aster_min_notional: dec!(5),
            hl_sz_decimals: 5,
            hl_qty_step: dec!(0.00001),
            hl_min_notional: dec!(10),
        })
        .unwrap();
        // A reject is kept per-row; an accepted place folds into the in-memory
        // aggregate and is written to opportunity_stats by finish_run.
        db.record_opportunity(&OpportunityRow::rejected(
            "BTC".into(),
            Side::Buy,
            QueueModel::Optimistic,
            RejectReason::NoProfitableAsterBid,
            ts(),
        ))
        .unwrap();
        db.finish_run(ts()).unwrap();
        db.flush().unwrap();
        assert_eq!(db.count("runs").unwrap(), 1);
        assert_eq!(db.count("markets").unwrap(), 1);
        assert_eq!(db.count("opportunity_rejects").unwrap(), 1);
        std::fs::remove_file(&dir).ok();
    }

    #[test]
    fn unbooked_hedge_row_is_zero_filled_with_reason() {
        // The Fix-4 record: a hedge that could not be priced (no HL book) is still a row,
        // with filled_qty = 0 and a reason, never a silent drop.
        let ph = PendingHedge {
            id: Uuid::new_v4(),
            fill_id: Uuid::new_v4(),
            market: "BTC".into(),
            queue_model: QueueModel::Optimistic,
            hedge_side: Side::Sell,
            qty: dec!(0.5),
            aster_ref_px: dec!(100),
            fill_local_ts: ts(),
            resolve_at: ts(),
            latency_bucket_ms: 50,
        };
        let row = HedgeRow::unbooked(&ph, "MISSING_HL_BOOK");
        assert_eq!(row.qty, dec!(0.5)); // requested preserved
        assert_eq!(row.filled_qty, dec!(0)); // nothing hedged
        assert_eq!(row.net_pnl, dec!(0));
        assert!(row.depth_exhausted);
        assert_eq!(row.reason.as_deref(), Some("MISSING_HL_BOOK"));

        // It persists without error.
        let dir = std::env::temp_dir().join(format!("xemm_unbooked_{}.sqlite", Uuid::new_v4()));
        let mut db = Db::open(&dir).unwrap();
        db.insert_run("r", ts(), "replay", None, "t", "{}").unwrap();
        db.insert_hedge(&row).unwrap();
        db.flush().unwrap();
        assert_eq!(db.count("hedges").unwrap(), 1);
        std::fs::remove_file(&dir).ok();
    }
}
