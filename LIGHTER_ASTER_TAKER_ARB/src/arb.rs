//! Taker-arb engine: scan both books for cross-venue edge, then fire simultaneous IOCs.
//!
//! # Hot/cold separation rules (hold these when changing the scan loop)
//!
//! The scan iteration (fetch books → f64 edge scan → gate checks) is the hot path:
//! * **Book reads are lock-free** — Aster via `ArcSwapOption` load, Lighter via the
//!   per-market cells resolved at venue construction (never the feed writer's mutex).
//! * **All math on the scan path is f64** (`MarketMathF64`); Decimal appears only at the
//!   submission/logging boundary for a qualifying opportunity.
//! * **No inline file I/O** — entry-gate samples, reduce-signal files, and execution logs
//!   go through `spawn_blocking` (see `write_file_atomic_off_path`); account state arrives
//!   via a `watch` channel from the background refresher.
//! * **No inline REST on the iteration** — the lease nonce refresh runs as a spawned task
//!   with execution gated until it lands; account snapshots refresh on their own task.
//! Execution itself (sign + submit both legs concurrently, confirm, reconcile, rescue) is
//! deliberately synchronous within the loop: nothing may scan for new entries while legs
//! are unconfirmed.

use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::signal;
use tokio::sync::watch;
use tracing::{debug, error, info, warn, Level};

use crate::aster::creds::{AsterCreds, LighterCreds};
use crate::aster::rest::{
    immediate_fill_from_order_response, AsterImmediateFill, AsterRest,
    SubmitOutcome as AsterOutcome,
};
use crate::aster::sign::{AsterSigner, EvmAsterSigner};
use crate::aster::ws::AsterBookFeed;
use crate::book::OrderBook;
use crate::config::{Config, MarketCfg};
use crate::connectors::{rest_book, rest_specs};
use crate::decimal::bps_to_rate;
use crate::entry_gate::{OpportunityGate, OpportunityGateInput};
use crate::markets::MarketSpec;
use crate::pnl::{format_ts, PnlTracker, TradeLedgerRow};
use crate::types::{FillSummary, MarketId, Side};
use crate::venues::lighter::{
    LighterFillConfirmation, LighterVenue, SubmitOutcome as LighterOutcome,
};

static EXECUTION_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    SellAsterBuyLighter,
    SellLighterBuyAster,
}

impl Direction {
    fn as_str(self) -> &'static str {
        match self {
            Direction::SellAsterBuyLighter => "SELL_ASTER_BUY_LIGHTER",
            Direction::SellLighterBuyAster => "SELL_LIGHTER_BUY_ASTER",
        }
    }

    fn aster_side(self) -> Side {
        match self {
            Direction::SellAsterBuyLighter => Side::Sell,
            Direction::SellLighterBuyAster => Side::Buy,
        }
    }

    fn lighter_side(self) -> Side {
        self.aster_side().opposite()
    }
}

#[derive(Debug, Clone)]
struct Opportunity {
    direction: Direction,
    qty: Decimal,
    qty_f64: f64,
    gross_edge_bps: Decimal,
    expected_net_margin_bps: Decimal,
    sell_px: Decimal,
    buy_px: Decimal,
    ref_px: Decimal,
    top_depth_qty: Decimal,
    depth_guard_enabled: bool,
    liquidity_multiple: Decimal,
    depth_supported_qty: Decimal,
    sell_depth_target_qty: Decimal,
    buy_depth_target_qty: Decimal,
    sell_depth_available_qty: Decimal,
    buy_depth_available_qty: Decimal,
    sell_depth_worst_px: Decimal,
    buy_depth_worst_px: Decimal,
    sell_depth_levels_used: usize,
    buy_depth_levels_used: usize,
    sell_best_px: Decimal,
    buy_best_px: Decimal,
    sell_best_qty: Decimal,
    buy_best_qty: Decimal,
    desired_qty: Decimal,
    min_qty: Decimal,
    headroom_qty: Decimal,
    margin_room_qty: Decimal,
    expected_gross_usd: Decimal,
    expected_fee_usd: Decimal,
    expected_net_usd: Decimal,
    required_margin_usd: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExposureEffect {
    Reduce,
    Increase,
    Flat,
    Unknown,
}

#[derive(Debug, Clone, Copy)]
struct PositionSnapshot {
    aster_qty: Decimal,
    lighter_qty: Decimal,
}

impl PositionSnapshot {
    fn net_qty(self) -> Decimal {
        self.aster_qty + self.lighter_qty
    }
}

#[derive(Debug, Clone, Copy)]
struct MarginSnapshot {
    aster_available_usd: Decimal,
    lighter_available_usd: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct PositionF64 {
    aster_qty: f64,
    lighter_qty: f64,
}

impl PositionF64 {
    fn from_snapshot(pos: PositionSnapshot) -> Option<Self> {
        Some(Self {
            aster_qty: decimal_to_f64(pos.aster_qty)?,
            lighter_qty: decimal_to_f64(pos.lighter_qty)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct MarginF64 {
    aster_available_usd: f64,
    lighter_available_usd: f64,
}

impl MarginF64 {
    fn from_snapshot(margins: MarginSnapshot) -> Option<Self> {
        Some(Self {
            aster_available_usd: decimal_to_f64(margins.aster_available_usd)?,
            lighter_available_usd: decimal_to_f64(margins.lighter_available_usd)?,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct MarketMathF64 {
    common_qty_step: f64,
    qty_decimal_places: u32,
    qty_tol_cap: f64,
    aster_min_qty: f64,
    aster_min_notional: f64,
    lighter_min_notional: f64,
    desired_notional: f64,
    aster_taker_fee_rate: f64,
    lighter_taker_fee_rate: f64,
    margin_rate: f64,
    max_abs_position_notional_usd: f64,
    margin_buffer_usd: f64,
    depth_liquidity_multiple: f64,
}

impl MarketMathF64 {
    fn from_config_spec(cfg: &Config, spec: &MarketSpec) -> Result<Self> {
        let common_step_dec = common_qty_step_dec(spec.step, spec.lighter_qty_step)?;
        let common_qty_step = positive_decimal_to_f64(common_step_dec)
            .context("common quantity step is not representable as finite f64")?;
        let qty_decimal_places = common_step_dec.normalize().scale();
        Ok(Self {
            common_qty_step,
            qty_decimal_places,
            qty_tol_cap: (common_qty_step * 1e-6).max(f64::EPSILON * 16.0),
            aster_min_qty: positive_decimal_to_f64(spec.aster_min_qty)
                .context("Aster min qty is not representable as finite positive f64")?,
            aster_min_notional: positive_decimal_to_f64(spec.aster_min_notional)
                .context("Aster min notional is not representable as finite positive f64")?,
            lighter_min_notional: positive_decimal_to_f64(spec.lighter_min_notional)
                .context("Lighter min notional is not representable as finite positive f64")?,
            desired_notional: positive_decimal_to_f64(cfg.arb.desired_notional)
                .context("desired notional is not representable as finite positive f64")?,
            aster_taker_fee_rate: non_negative_decimal_to_f64(cfg.arb.aster_taker_fee_bps)
                .context("Aster taker fee is not representable as finite f64")?
                / 10_000.0,
            lighter_taker_fee_rate: non_negative_decimal_to_f64(cfg.arb.lighter_taker_fee_bps)
                .context("Lighter taker fee is not representable as finite f64")?
                / 10_000.0,
            margin_rate: non_negative_decimal_to_f64(cfg.arb.margin_bps)
                .context("margin bps is not representable as finite f64")?
                / 10_000.0,
            max_abs_position_notional_usd: positive_decimal_to_f64(
                cfg.risk.max_abs_position_notional_usd,
            )
            .context("max abs position notional is not representable as finite positive f64")?,
            margin_buffer_usd: non_negative_decimal_to_f64(cfg.risk.margin_buffer_usd)
                .context("margin buffer is not representable as finite f64")?,
            depth_liquidity_multiple: positive_decimal_to_f64(cfg.arb.depth_guard.liquidity_multiple)
                .context("depth liquidity multiple is not representable as finite positive f64")?,
        })
    }

    fn liquidity_multiple(self, depth_guard_enabled: bool) -> f64 {
        if depth_guard_enabled {
            self.depth_liquidity_multiple
        } else {
            1.0
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct AccountSnapshot {
    position: PositionSnapshot,
    lighter_ws_qty: Option<Decimal>,
    lighter_ws_rest_divergence_qty: Option<Decimal>,
    margins: MarginSnapshot,
    refreshed_at: tokio::time::Instant,
}

impl AccountSnapshot {
    fn is_stale(self, max_age: Duration) -> bool {
        self.refreshed_at.elapsed() >= max_age
    }

    fn age_ms(self) -> u128 {
        self.refreshed_at.elapsed().as_millis()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum ExposureFilter {
    #[default]
    Any,
    Reduce,
}

#[derive(Debug, Clone)]
pub struct RunOptions {
    pub secs: Option<u64>,
    pub max_trades: Option<u64>,
    pub min_size: bool,
    pub observe_only: bool,
    pub exposure_filter: ExposureFilter,
    pub control_file: Option<PathBuf>,
    pub signal_file: Option<PathBuf>,
    pub reduce_cooldown_ms: u64,
    pub reduce_signal_min_samples: usize,
    pub reduce_signal_window_ms: i64,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            secs: None,
            max_trades: None,
            min_size: false,
            observe_only: false,
            exposure_filter: ExposureFilter::Any,
            control_file: None,
            signal_file: None,
            reduce_cooldown_ms: 5_000,
            reduce_signal_min_samples: 3,
            reduce_signal_window_ms: 2_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ExecutionLease {
    market: String,
    mode: String,
    lease_id: Option<String>,
    expires_at: DateTime<Utc>,
}

/// The lease file is re-read at most this often. The scan loop polls every ~10ms but
/// the orchestrator rewrites the lease at seconds cadence; validity (market/mode/
/// expiry) is still re-checked against `now` on EVERY scan, so expiry stays exact and
/// a revocation-by-rewrite is honored within one interval — negligible next to the
/// post-trade cooldown, and fail-closed (a missing/invalid new lease disables
/// execution at the next read).
const LEASE_REREAD_INTERVAL: Duration = Duration::from_millis(250);

/// Caches the parsed execution-lease file between throttled reads so lease mode does
/// not pay a blocking `read_to_string` + JSON parse on every scan iteration.
struct LeaseFileCache {
    last_read_at: Option<tokio::time::Instant>,
    lease: Option<ExecutionLease>,
}

impl LeaseFileCache {
    fn new() -> Self {
        Self {
            last_read_at: None,
            lease: None,
        }
    }

    fn read_if_due(&mut self, path: &Path) {
        let due = match self.last_read_at {
            Some(at) => at.elapsed() >= LEASE_REREAD_INTERVAL,
            None => true,
        };
        if !due {
            return;
        }
        self.last_read_at = Some(tokio::time::Instant::now());
        self.lease = match std::fs::read_to_string(path) {
            Ok(text) => match serde_json::from_str::<ExecutionLease>(&text) {
                Ok(lease) => Some(lease),
                Err(e) => {
                    warn!("invalid execution lease {}: {e:#}", path.display());
                    None
                }
            },
            Err(_) => None,
        };
    }
}

#[derive(Debug, Clone)]
struct ReduceSignalSample {
    timestamp: DateTime<Utc>,
    opportunity: Opportunity,
    gate_decision: &'static str,
    gate_threshold_bps: Option<Decimal>,
    gate_sample_count: usize,
}

#[derive(Debug)]
struct ReduceSignalTracker {
    path: Option<PathBuf>,
    min_samples: usize,
    window_ms: i64,
    samples: VecDeque<ReduceSignalSample>,
}

#[derive(Debug, Serialize)]
struct ReduceSignalFile<'a> {
    timestamp: DateTime<Utc>,
    market: &'a str,
    status: &'static str,
    samples: usize,
    window_ms: i64,
    first_seen: DateTime<Utc>,
    last_seen: DateTime<Utc>,
    best: ReduceSignalOpportunity<'a>,
}

#[derive(Debug, Serialize)]
struct ReduceSignalOpportunity<'a> {
    direction: &'a str,
    qty: Decimal,
    gross_edge_bps: Decimal,
    expected_net_margin_bps: Decimal,
    expected_net_usd: Decimal,
    sell_px: Decimal,
    buy_px: Decimal,
    ref_px: Decimal,
    top_depth_qty: Decimal,
    depth_guard_enabled: bool,
    liquidity_multiple: Decimal,
    depth_supported_qty: Decimal,
    sell_depth_target_qty: Decimal,
    buy_depth_target_qty: Decimal,
    sell_depth_available_qty: Decimal,
    buy_depth_available_qty: Decimal,
    sell_depth_worst_px: Decimal,
    buy_depth_worst_px: Decimal,
    sell_depth_levels_used: usize,
    buy_depth_levels_used: usize,
    sell_best_px: Decimal,
    buy_best_px: Decimal,
    sell_best_qty: Decimal,
    buy_best_qty: Decimal,
    gate_decision: &'a str,
    gate_threshold_bps: Option<Decimal>,
    gate_sample_count: usize,
}

impl ReduceSignalTracker {
    fn new(options: &RunOptions) -> Self {
        Self {
            path: options.signal_file.clone(),
            min_samples: options.reduce_signal_min_samples.max(1),
            window_ms: options.reduce_signal_window_ms.max(1),
            samples: VecDeque::new(),
        }
    }

    fn observe(
        &mut self,
        spec: &MarketSpec,
        opp: &Opportunity,
        gate: &crate::entry_gate::GateEvaluation,
        now: DateTime<Utc>,
    ) {
        if self.path.is_none() || !gate.allow_execution {
            return;
        }
        self.prune(now);
        self.samples.push_back(ReduceSignalSample {
            timestamp: now,
            opportunity: opp.clone(),
            gate_decision: gate.decision,
            gate_threshold_bps: gate.threshold_bps,
            gate_sample_count: gate.sample_count,
        });
        self.prune(now);
        if self.samples.len() >= self.min_samples {
            self.write_confirmed(spec, now);
        }
    }

    fn prune(&mut self, now: DateTime<Utc>) {
        let cutoff = now - chrono::Duration::milliseconds(self.window_ms);
        while self
            .samples
            .front()
            .is_some_and(|sample| sample.timestamp < cutoff)
        {
            self.samples.pop_front();
        }
    }

    fn write_confirmed(&self, spec: &MarketSpec, now: DateTime<Utc>) {
        let Some(path) = &self.path else {
            return;
        };
        let Some(first) = self.samples.front() else {
            return;
        };
        let Some(last) = self.samples.back() else {
            return;
        };
        let Some(best) = self.samples.iter().max_by(|a, b| {
            a.opportunity
                .expected_net_usd
                .cmp(&b.opportunity.expected_net_usd)
        }) else {
            return;
        };
        let body = ReduceSignalFile {
            timestamp: now,
            market: &spec.market_id.0,
            status: "confirmed",
            samples: self.samples.len(),
            window_ms: self.window_ms,
            first_seen: first.timestamp,
            last_seen: last.timestamp,
            best: ReduceSignalOpportunity {
                direction: best.opportunity.direction.as_str(),
                qty: best.opportunity.qty,
                gross_edge_bps: best.opportunity.gross_edge_bps,
                expected_net_margin_bps: best.opportunity.expected_net_margin_bps,
                expected_net_usd: best.opportunity.expected_net_usd,
                sell_px: best.opportunity.sell_px,
                buy_px: best.opportunity.buy_px,
                ref_px: best.opportunity.ref_px,
                top_depth_qty: best.opportunity.top_depth_qty,
                depth_guard_enabled: best.opportunity.depth_guard_enabled,
                liquidity_multiple: best.opportunity.liquidity_multiple,
                depth_supported_qty: best.opportunity.depth_supported_qty,
                sell_depth_target_qty: best.opportunity.sell_depth_target_qty,
                buy_depth_target_qty: best.opportunity.buy_depth_target_qty,
                sell_depth_available_qty: best.opportunity.sell_depth_available_qty,
                buy_depth_available_qty: best.opportunity.buy_depth_available_qty,
                sell_depth_worst_px: best.opportunity.sell_depth_worst_px,
                buy_depth_worst_px: best.opportunity.buy_depth_worst_px,
                sell_depth_levels_used: best.opportunity.sell_depth_levels_used,
                buy_depth_levels_used: best.opportunity.buy_depth_levels_used,
                sell_best_px: best.opportunity.sell_best_px,
                buy_best_px: best.opportunity.buy_best_px,
                sell_best_qty: best.opportunity.sell_best_qty,
                buy_best_qty: best.opportunity.buy_best_qty,
                gate_decision: best.gate_decision,
                gate_threshold_bps: best.gate_threshold_bps,
                gate_sample_count: best.gate_sample_count,
            },
        };
        // Serialize in-memory (fast), then hand the actual filesystem work to a blocking
        // task: this runs inside the scan loop, and an fsync/rename on a slow disk would
        // otherwise stall the hot path for 0.5-10ms.
        match serde_json::to_vec_pretty(&body) {
            Ok(bytes) => write_file_atomic_off_path(path.clone(), bytes),
            Err(e) => warn!("failed to serialize reduce signal {}: {e:#}", path.display()),
        }
    }
}

/// Atomically write `bytes` to `path` (tmp + rename) OFF the calling task when a tokio
/// runtime is available. The scan loop must never block on filesystem latency; outside a
/// runtime (tests/tools) the write happens inline.
fn write_file_atomic_off_path(path: PathBuf, bytes: Vec<u8>) {
    let write = move || {
        if let Err(e) = write_bytes_atomic(&path, &bytes) {
            warn!("failed to write {}: {e:#}", path.display());
        }
    };
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn_blocking(write);
    } else {
        write();
    }
}

fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create parent dir {}", parent.display()))?;
    }
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("reduce_signal.json");
    let tmp = path.with_file_name(format!(".{file_name}.tmp"));
    std::fs::write(&tmp, bytes).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename {} to {}", tmp.display(), path.display()))?;
    Ok(())
}

fn valid_execution_lease(
    cache: &mut LeaseFileCache,
    options: &RunOptions,
    spec: &MarketSpec,
    now: DateTime<Utc>,
) -> Option<ExecutionLease> {
    if options.observe_only {
        return None;
    }
    let Some(path) = &options.control_file else {
        return None;
    };
    cache.read_if_due(path);
    let lease = cache.lease.as_ref()?;
    if lease.market != spec.market_id.0 || lease.mode != "reduce_only" || lease.expires_at <= now {
        return None;
    }
    if let Some(lease_id) = lease.lease_id.as_deref() {
        debug!(
            "valid execution lease market={} lease_id={lease_id}",
            spec.market_id
        );
    }
    Some(lease.clone())
}

fn execution_lease_enabled(
    cache: &mut LeaseFileCache,
    options: &RunOptions,
    spec: &MarketSpec,
    now: DateTime<Utc>,
) -> (bool, Option<ExecutionLease>) {
    if options.observe_only {
        return (false, None);
    }
    if options.control_file.is_none() {
        return (true, None);
    }
    let lease = valid_execution_lease(cache, options, spec, now);
    (lease.is_some(), lease)
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
struct SizingDecision {
    qty: Decimal,
    desired_qty: Decimal,
    min_qty: Decimal,
    top_depth_qty: Decimal,
    depth_guard_enabled: bool,
    liquidity_multiple: Decimal,
    depth_supported_qty: Decimal,
    sell_depth_target_qty: Decimal,
    buy_depth_target_qty: Decimal,
    sell_depth_available_qty: Decimal,
    buy_depth_available_qty: Decimal,
    sell_depth_worst_px: Decimal,
    buy_depth_worst_px: Decimal,
    sell_depth_levels_used: usize,
    buy_depth_levels_used: usize,
    sell_best_px: Decimal,
    buy_best_px: Decimal,
    sell_best_qty: Decimal,
    buy_best_qty: Decimal,
    headroom_qty: Decimal,
    margin_room_qty: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct SizingDecisionF64 {
    qty: f64,
    desired_qty: f64,
    min_qty: f64,
    top_depth_qty: f64,
    depth_guard_enabled: bool,
    liquidity_multiple: f64,
    depth_supported_qty: f64,
    sell_depth_target_qty: f64,
    buy_depth_target_qty: f64,
    sell_depth_available_qty: f64,
    buy_depth_available_qty: f64,
    sell_depth_worst_px: f64,
    buy_depth_worst_px: f64,
    sell_depth_levels_used: usize,
    buy_depth_levels_used: usize,
    sell_best_px: f64,
    buy_best_px: f64,
    sell_best_qty: f64,
    buy_best_qty: f64,
    headroom_qty: f64,
    margin_room_qty: f64,
}

#[inline]
fn f64_to_dec(v: f64) -> Decimal {
    Decimal::from_f64_retain(v)
        .unwrap_or(Decimal::ZERO)
        .round_dp(12)
}

/// Skip the Decimal opportunity build only when the f64 edge is CLEARLY below the
/// exact threshold. 1e-9 bps dwarfs every error term between the f64 edge and its
/// round_dp(12) Decimal (<= 5e-13 bps) plus the Decimal->f64 threshold conversion
/// (<= ~2e-12 bps for realistic thresholds), while being economically zero; anything
/// closer than this band still takes the exact Decimal comparison, so borderline
/// accept/reject behavior is unchanged.
const EDGE_PREFILTER_EPS_BPS: f64 = 1e-9;

fn qty_f64_to_dec(math: &MarketMathF64, v: f64) -> Decimal {
    Decimal::from_f64_retain(round_qty_to_scale_f64(v, math))
        .unwrap_or(Decimal::ZERO)
        .round_dp(math.qty_decimal_places)
}

fn decimal_to_f64(value: Decimal) -> Option<f64> {
    let out = value.to_f64()?;
    out.is_finite().then_some(out)
}

fn positive_decimal_to_f64(value: Decimal) -> Option<f64> {
    let out = decimal_to_f64(value)?;
    (out > 0.0).then_some(out)
}

fn non_negative_decimal_to_f64(value: Decimal) -> Option<f64> {
    let out = decimal_to_f64(value)?;
    (out >= 0.0).then_some(out)
}

fn common_qty_step_dec(aster_step: Decimal, lighter_step: Decimal) -> Result<Decimal> {
    let (a_units, a_scale) = decimal_step_units(aster_step)?;
    let (l_units, l_scale) = decimal_step_units(lighter_step)?;
    let scale = a_scale.max(l_scale);
    let a = a_units
        .checked_mul(pow10_u128(scale - a_scale)?)
        .context("Aster quantity step scale overflow")?;
    let l = l_units
        .checked_mul(pow10_u128(scale - l_scale)?)
        .context("Lighter quantity step scale overflow")?;
    let common = lcm_u128(a, l).context("common quantity step overflow")?;
    let common_i128 = i128::try_from(common).context("common quantity step too large")?;
    Ok(Decimal::from_i128_with_scale(common_i128, scale).normalize())
}

fn decimal_step_units(step: Decimal) -> Result<(u128, u32)> {
    if step <= Decimal::ZERO {
        bail!("quantity step must be positive");
    }
    let normalized = step.normalize();
    let mantissa = normalized.mantissa().abs();
    if mantissa == 0 {
        bail!("quantity step must be positive");
    }
    let units = u128::try_from(mantissa).context("quantity step mantissa overflow")?;
    Ok((units, normalized.scale()))
}

fn pow10_u128(exp: u32) -> Result<u128> {
    let mut out = 1u128;
    for _ in 0..exp {
        out = out.checked_mul(10).context("decimal scale overflow")?;
    }
    Ok(out)
}

fn gcd_u128(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a
}

fn lcm_u128(a: u128, b: u128) -> Option<u128> {
    let gcd = gcd_u128(a, b);
    a.checked_div(gcd)?.checked_mul(b)
}

fn unit_snap_tol(units: f64) -> f64 {
    (units.abs() * f64::EPSILON * 64.0).max(1e-12)
}

fn snap_step_units(units: f64) -> f64 {
    let nearest = units.round();
    if (units - nearest).abs() <= unit_snap_tol(units) {
        nearest
    } else {
        units
    }
}

fn round_qty_to_scale_f64(qty: f64, math: &MarketMathF64) -> f64 {
    if !qty.is_finite() || qty <= 0.0 {
        return 0.0;
    }
    let scale = 10f64.powi(math.qty_decimal_places as i32);
    if !scale.is_finite() || scale <= 0.0 {
        return qty;
    }
    (qty * scale).round() / scale
}

fn qty_cmp_tol(math: &MarketMathF64, a: f64, b: f64) -> f64 {
    let raw = a.abs().max(b.abs()) * f64::EPSILON * 64.0;
    raw.max(f64::EPSILON).min(math.qty_tol_cap)
}

fn qty_le(a: f64, b: f64, math: &MarketMathF64) -> bool {
    a <= b + qty_cmp_tol(math, a, b)
}

fn qty_ge(a: f64, b: f64, math: &MarketMathF64) -> bool {
    a + qty_cmp_tol(math, a, b) >= b
}

fn qty_gt(a: f64, b: f64, math: &MarketMathF64) -> bool {
    a > b + qty_cmp_tol(math, a, b)
}

#[derive(Debug, Clone, Copy)]
struct TradeReport {
    position: PositionSnapshot,
    lighter_ws_qty: Option<Decimal>,
    lighter_ws_rest_divergence_qty: Option<Decimal>,
    margin_before: MarginSnapshot,
    margin_after: MarginSnapshot,
    economics: ActualEconomics,
    aster_order_id: i64,
    lighter_client_order_index: i64,
    hedge_retry_action_taken: bool,
}

impl TradeReport {
    fn available_margin_delta_usd(self) -> Decimal {
        (self.margin_after.aster_available_usd + self.margin_after.lighter_available_usd)
            - (self.margin_before.aster_available_usd + self.margin_before.lighter_available_usd)
    }
}

#[derive(Debug, Clone, Copy)]
struct ActualEconomics {
    aster_fill: FillSummary,
    lighter_fill: FillSummary,
    gross_usd: Decimal,
    fees_usd: Decimal,
    net_usd: Decimal,
    net_bps: Decimal,
    fill_qty_mismatch: Decimal,
    /// Signed unmatched leg qty (sell − buy): NOT PnL, open exposure. Booked separately so
    /// the loss breaker sees real matched PnL instead of up to ~$3/trade of phantom profit.
    residual_qty: Decimal,
    /// Absolute residual valued at the opportunity reference price.
    residual_notional_usd: Decimal,
}

#[derive(Debug, Clone, Copy)]
struct RecoveryReport {
    action_taken: bool,
    position: PositionSnapshot,
    lighter_ws_qty: Option<Decimal>,
    margin_after: MarginSnapshot,
    estimated_loss_usdc: Decimal,
    aster_open_orders: usize,
    lighter_open_orders: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HedgeRetryVenue {
    Aster,
    Lighter,
}

impl HedgeRetryVenue {
    fn as_str(self) -> &'static str {
        match self {
            HedgeRetryVenue::Aster => "Aster",
            HedgeRetryVenue::Lighter => "Lighter",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct HedgeRetryPlan {
    venue: HedgeRetryVenue,
    side: Side,
    qty: Decimal,
    price_bound: Decimal,
}

#[derive(Debug, Clone)]
struct HedgeRetryAttempt {
    attempt: u64,
    venue: HedgeRetryVenue,
    side: Side,
    qty: Decimal,
    price_bound: Decimal,
    reduce_only: bool,
    submit_result: Option<String>,
    fill: Option<FillSummary>,
    fill_status: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct HedgeRetryReport {
    attempted: bool,
    succeeded: bool,
    slippage_bps: Decimal,
    attempts: Vec<HedgeRetryAttempt>,
    final_position: Option<PositionSnapshot>,
    net_notional: Option<Decimal>,
    aster_open_orders: Option<usize>,
    lighter_open_orders: Option<usize>,
    error: Option<String>,
}

impl HedgeRetryReport {
    fn empty(slippage_bps: Decimal, error: Option<String>) -> Self {
        Self {
            attempted: false,
            succeeded: false,
            slippage_bps,
            attempts: Vec::new(),
            final_position: None,
            net_notional: None,
            aster_open_orders: None,
            lighter_open_orders: None,
            error,
        }
    }
}

#[derive(Debug, Default)]
struct RecoveredFailureTracker {
    events: VecDeque<(tokio::time::Instant, Decimal)>,
}

impl RecoveredFailureTracker {
    fn record(&mut self, loss_usdc: Decimal, cfg: &Config) -> Option<String> {
        let now = tokio::time::Instant::now();
        self.events
            .retain(|(ts, _)| now.duration_since(*ts) <= Duration::from_secs(3600));
        self.events.push_back((now, loss_usdc.max(Decimal::ZERO)));
        let count = self.events.len() as u64;
        let loss_sum = self
            .events
            .iter()
            .fold(Decimal::ZERO, |acc, (_, loss)| acc + *loss);
        if count > cfg.arb.max_recovered_failures_per_hour {
            return Some(format!(
                "recovered failure count {count} exceeds hourly limit {}",
                cfg.arb.max_recovered_failures_per_hour
            ));
        }
        if loss_sum > cfg.arb.max_recovered_loss_usdc_per_hour {
            return Some(format!(
                "recovered failure loss ${loss_sum} exceeds hourly limit ${}",
                cfg.arb.max_recovered_loss_usdc_per_hour
            ));
        }
        None
    }

    #[cfg(test)]
    fn event_count(&self) -> usize {
        self.events.len()
    }
}

#[derive(Debug, thiserror::Error)]
enum ExecutionError {
    #[error("{details}")]
    Skipped { details: String },
    #[error("{details}")]
    LegRejected { one_sided: bool, details: String },
    #[error("{details}")]
    Unreconciled { details: String },
    #[error("{details}")]
    AccountingUnavailable { details: String },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl ExecutionError {
    fn needs_recovery(&self) -> bool {
        matches!(
            self,
            ExecutionError::LegRejected {
                one_sided: true,
                ..
            } | ExecutionError::Unreconciled { .. }
                | ExecutionError::AccountingUnavailable { .. }
        )
    }

    fn is_skip(&self) -> bool {
        matches!(self, ExecutionError::Skipped { .. })
    }
}

pub async fn run(cfg: Config, markets: Vec<MarketCfg>, options: RunOptions) -> Result<()> {
    if !cfg.live.enabled || !cfg.live.mode.eq_ignore_ascii_case("live") {
        bail!("refusing to run: set [live] enabled = true and mode = \"live\"");
    }
    if markets.len() != 1 {
        bail!(
            "live taker arb is single-market only; selected {} markets",
            markets.len()
        );
    }

    let specs = rest_specs::build_market_specs(
        &markets,
        &cfg.venues.aster_base_url,
        &cfg.venues.lighter_base_url,
    )
    .await?;
    let spec = specs.first().context("no resolved market spec")?.clone();
    let math = MarketMathF64::from_config_spec(&cfg, &spec)?;
    info!(
        "resolved market {}: Aster {} step={} min_notional={} | Lighter {} market_id={} qty_step={} min_notional={} common_qty_step={}",
        spec.market_id,
        spec.aster_symbol,
        spec.step,
        spec.aster_min_notional,
        spec.lighter_symbol,
        spec.lighter_market_id,
        spec.lighter_qty_step,
        spec.lighter_min_notional,
        math.common_qty_step
    );

    let bot_start = Utc::now();
    let mut pnl = if cfg.pnl.enabled {
        let tracker = PnlTracker::new(&cfg.pnl, &spec.market_id, bot_start)?;
        let snapshot = tracker.snapshot();
        info!(
            "pnl tracker enabled: market={} since={} loaded_trades={} cumulative_pnl=${} max_loss=${} ledger={} breaker={}",
            spec.market_id,
            format_ts(snapshot.since),
            snapshot.loaded_trades,
            snapshot.cumulative_pnl_usdc,
            snapshot.max_loss_usdc,
            snapshot.ledger_path.display(),
            snapshot.breaker_path.display()
        );
        if let Some(breaker) = tracker.active_breaker()? {
            bail!(
                "circuit breaker active: market={} since={} cumulative_pnl=${} max_loss=${}; reset with reset-circuit-breaker",
                breaker.market,
                format_ts(breaker.pnl_since),
                breaker.cumulative_pnl_usdc,
                breaker.max_loss_usdc
            );
        }
        if let Some(breaker) = tracker.trip_from_loaded_pnl_if_needed()? {
            bail!(
                "circuit breaker triggered from persisted PnL: market={} since={} cumulative_pnl=${} max_loss=${}; reset with reset-circuit-breaker after changing pnl.since or limit",
                breaker.market,
                format_ts(breaker.pnl_since),
                breaker.cumulative_pnl_usdc,
                breaker.max_loss_usdc
            );
        }
        Some(tracker)
    } else {
        info!("pnl tracker disabled");
        None
    };
    let mut entry_gate = OpportunityGate::new(
        &cfg.arb.entry_gate,
        &spec.market_id,
        &cfg.pnl.persist_dir,
        bot_start,
    )?;
    if cfg.arb.entry_gate.enabled {
        info!(
            "entry gate configured: market={} mode={} loaded_samples={} history_window_hours={} min_history_samples={} entry_percentile={} min_extra_bps={} sample_interval_ms={} history={}",
            spec.market_id,
            cfg.arb.entry_gate.mode.as_str(),
            entry_gate.loaded_samples(),
            cfg.arb.entry_gate.history_window_hours,
            cfg.arb.entry_gate.min_history_samples,
            cfg.arb.entry_gate.entry_percentile,
            cfg.arb.entry_gate.min_extra_bps,
            cfg.arb.entry_gate.sample_interval_ms,
            entry_gate.path().display()
        );
    } else {
        info!("entry gate disabled");
    }

    let aster_env = std::env::var("ASTER_ENV_PATH").unwrap_or_else(|_| "aster.env".to_string());
    let lighter_env =
        std::env::var("LIGHTER_ENV_PATH").unwrap_or_else(|_| "lighter.env".to_string());
    let acreds = AsterCreds::load(Path::new(&aster_env))?;
    let lcreds = LighterCreds::load(Path::new(&lighter_env))?;
    let aster_signer: Arc<dyn AsterSigner> =
        Arc::new(EvmAsterSigner::new(acreds.user, acreds.signer, acreds.key)?);
    let aster = Arc::new(AsterRest::new(
        cfg.venues.aster_base_url.clone(),
        aster_signer,
        &specs,
    )?);
    let lighter = Arc::new(
        LighterVenue::new(
            &cfg.venues.lighter_base_url,
            Path::new(&cfg.venues.signers_dir),
            lcreds,
            &specs,
        )
        .await?,
    );
    let aster_books =
        AsterBookFeed::spawn_from_rest_base(&cfg.venues.aster_base_url, &spec.aster_symbol);

    aster_books.wait_ready(Duration::from_secs(20)).await?;
    info!("Aster websocket book ready: market={}", spec.market_id);
    lighter
        .wait_ready(&spec.market_id, Duration::from_secs(20))
        .await?;
    info!("Lighter websocket state ready: market={}", spec.market_id);
    let standby_until_lease = options.control_file.is_some();
    ensure_clean_start(
        &cfg,
        &spec,
        &aster_books,
        &aster,
        &lighter,
        options.observe_only || standby_until_lease,
    )
    .await?;
    let account_snapshot_max_age =
        Duration::from_millis(cfg.live.max_account_snapshot_age_ms as u64);
    let mut account = refresh_account_snapshot(&spec.market_id, &aster, &lighter).await?;
    let (account_tx, mut account_rx) = watch::channel(account);
    let account_refresh_paused = Arc::new(AtomicBool::new(false));
    let _account_refresh_task = spawn_account_snapshot_refresher(
        &cfg,
        spec.market_id.clone(),
        aster.clone(),
        lighter.clone(),
        account_tx.clone(),
        account_refresh_paused.clone(),
    );

    let http = rest_book::client()?;
    let book_sanity = crate::book_sanity::start(
        cfg.clone(),
        spec.clone(),
        aster_books.clone(),
        lighter.clone(),
        http.clone(),
    );
    let deadline = options
        .secs
        .map(|s| tokio::time::Instant::now() + Duration::from_secs(s));
    let mut cooldown_until =
        tokio::time::Instant::now() + Duration::from_millis(cfg.arb.startup_warmup_ms);
    let mut trades_executed = 0u64;
    let mut reduce_signal_tracker = ReduceSignalTracker::new(&options);
    let mut nonce_refreshed_for_lease: Option<String> = None;
    // In-flight lease nonce refresh (E: keep the REST round-trip off the scan iteration).
    let mut nonce_refresh_task: Option<(String, tokio::task::JoinHandle<Result<()>>)> = None;
    let mut last_stale_account_log_at: Option<tokio::time::Instant> = None;
    let mut last_book_sanity_block_log_at: Option<tokio::time::Instant> = None;
    // Gated/standby decisions can repeat every 10ms scan while an edge stays visible;
    // log only on decision change or every 5s (recorded samples stay unthrottled).
    let mut last_gate_log: Option<(&'static str, tokio::time::Instant)> = None;
    let mut last_standby_log_at: Option<tokio::time::Instant> = None;
    let mut recovered_failures = RecoveredFailureTracker::default();
    // Consecutive main-loop position-mismatch detections (see the guard below): after
    // `risk.mismatch_flatten_after_checks` in a row the residual is actively flattened
    // instead of pausing forever on a naked position.
    let mut mismatch_consecutive: u32 = 0;
    let mut last_flatten_denied_log_at: Option<tokio::time::Instant> = None;
    let mut last_fill_stats_log = tokio::time::Instant::now();
    let mut lease_cache = LeaseFileCache::new();

    info!(
        "taker arb running: market={} required_gross_edge={}bps desired_notional=${} min_size={} max_trades={:?} observe_only={} exposure_filter={:?} control_file={:?} signal_file={:?} startup_warmup_ms={} cooldown_ms={} reduce_cooldown_ms={} fees_bps=aster:{} lighter:{} margin_bps={} slippage_bps=aster:{} lighter:{} depth_guard_enabled={} liquidity_multiple={} depth_max_levels={} rescue_breaker=count_per_hour:{} loss_per_hour:${} risk_max_abs_notional=${} risk_mismatch=${} margin_buffer=${}",
        spec.market_id,
        cfg.arb.required_gross_edge_bps(),
        cfg.arb.desired_notional,
        options.min_size,
        options.max_trades,
        options.observe_only,
        options.exposure_filter,
        options.control_file,
        options.signal_file,
        cfg.arb.startup_warmup_ms,
        cfg.arb.cooldown_ms,
        options.reduce_cooldown_ms,
        cfg.arb.aster_taker_fee_bps,
        cfg.arb.lighter_taker_fee_bps,
        cfg.arb.margin_bps,
        cfg.arb.max_aster_slippage_bps,
        cfg.arb.max_lighter_slippage_bps,
        cfg.arb.depth_guard.enabled,
        cfg.arb.depth_guard.liquidity_multiple,
        cfg.arb.depth_guard.max_levels,
        cfg.arb.max_recovered_failures_per_hour,
        cfg.arb.max_recovered_loss_usdc_per_hour,
        cfg.risk.max_abs_position_notional_usd,
        cfg.risk.max_position_mismatch_usd,
        cfg.risk.margin_buffer_usd
    );
    if cfg.arb.startup_warmup_ms > 0 {
        info!(
            "startup warmup active: waiting {}ms before first scan",
            cfg.arb.startup_warmup_ms
        );
    }

    // Register the SIGINT listener ONCE, before the loop. A fresh signal::ctrl_c()
    // per iteration only listens while the select! below is polling — a SIGINT landing
    // during any of the loop's plain poll-interval sleeps was silently swallowed
    // (observed live 2026-07-02: the reduce-only observer survived SIGINT for minutes
    // and needed SIGTERM). The pinned future buffers a signal from the moment it is
    // first polled and resolves at the next select.
    let ctrl_c = signal::ctrl_c();
    tokio::pin!(ctrl_c);
    loop {
        if let Some(deadline) = deadline {
            if tokio::time::Instant::now() >= deadline {
                info!("duration elapsed; stopping");
                break;
            }
        }
        tokio::select! {
            // biased: cooldown_until is usually already elapsed, and an unbiased
            // select could keep picking the ready timer over a pending SIGINT.
            biased;
            _ = &mut ctrl_c => {
                info!("ctrl-c; stopping");
                break;
            }
            _ = tokio::time::sleep_until(cooldown_until) => {}
        }
        if let Some(deadline) = deadline {
            if tokio::time::Instant::now() >= deadline {
                info!("duration elapsed; stopping");
                break;
            }
        }

        let (aster_book, lighter_book) = match fetch_books(&spec, &aster_books, &lighter) {
            Ok(v) => v,
            Err(e) => {
                warn!("book fetch failed: {e:#}");
                tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
                continue;
            }
        };
        let now = Utc::now();
        if !book_ok(&aster_book, now, cfg.arb.max_book_staleness_ms)
            || !book_ok(&lighter_book, now, cfg.arb.max_book_staleness_ms)
        {
            tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
            continue;
        }

        account = *account_rx.borrow_and_update();
        if account.is_stale(account_snapshot_max_age) {
            let log_now = match last_stale_account_log_at {
                Some(ts) => ts.elapsed() >= Duration::from_secs(5),
                None => true,
            };
            if log_now {
                warn!(
                    "cold account snapshot stale; skipping scan iteration: age_ms={} max_age_ms={}",
                    account.age_ms(),
                    account_snapshot_max_age.as_millis()
                );
                last_stale_account_log_at = Some(tokio::time::Instant::now());
            }
            tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
            continue;
        }
        last_stale_account_log_at = None;

        // Throttled fill-matching health log (cold: one Instant compare per iteration).
        if last_fill_stats_log.elapsed() >= Duration::from_secs(60) {
            last_fill_stats_log = tokio::time::Instant::now();
            let s = lighter.fill_tracker_stats();
            info!(
                "lighter fill-tracker stats: registered={} trades_seen={} matched={} unmatched={} duplicates={} timeouts={}",
                s.registered, s.trades_seen, s.matched_trades, s.unmatched_trades, s.duplicate_trades, s.timeouts
            );
        }

        let pos = account.position;
        let Some(mismatch_notional) = net_mismatch_notional(pos, &aster_book, &lighter_book) else {
            warn!(
                "position mismatch check unavailable due to f64 conversion failure: aster={} lighter={}",
                pos.aster_qty, pos.lighter_qty
            );
            tokio::time::sleep(Duration::from_millis(cfg.risk.min_reconcile_interval_ms)).await;
            continue;
        };
        if mismatch_notional > cfg.risk.max_position_mismatch_usd {
            mismatch_consecutive = mismatch_consecutive.saturating_add(1);
            warn!(
                "position mismatch too large: aster={} lighter={} net={}; pausing ({} consecutive, auto-flatten at {})",
                pos.aster_qty,
                pos.lighter_qty,
                pos.net_qty(),
                mismatch_consecutive,
                cfg.risk.mismatch_flatten_after_checks
            );
            // A residual that persists across several reconciles is a real naked position
            // (e.g. an external fill, or an Unknown-outcome leg that landed): act on it
            // with the same reduce-only emergency-bound machinery as the rescue path,
            // instead of riding market moves until a human notices.
            // Flattening places LIVE reduce-only orders. Only an instance holding
            // execution rights may act: the 24/7 observer/standby process shares the
            // accounts with the active bot, and a transiently-unhedged leg of the
            // active bot's own recovery must never be raced by a second flattener.
            let (execution_allowed, _) =
                execution_lease_enabled(&mut lease_cache, &options, &spec, now);
            if cfg.risk.auto_flatten_on_mismatch
                && mismatch_consecutive >= cfg.risk.mismatch_flatten_after_checks
                && !execution_allowed
            {
                let log_now = match last_flatten_denied_log_at {
                    Some(ts) => ts.elapsed() >= Duration::from_secs(5),
                    None => true,
                };
                if log_now {
                    error!(
                        "position mismatch persists ({mismatch_consecutive} checks, ${mismatch_notional}) but this instance holds no execution rights (observer/standby); NOT auto-flattening"
                    );
                    last_flatten_denied_log_at = Some(tokio::time::Instant::now());
                }
            }
            if cfg.risk.auto_flatten_on_mismatch
                && mismatch_consecutive >= cfg.risk.mismatch_flatten_after_checks
                && execution_allowed
            {
                error!(
                    "position mismatch persisted {mismatch_consecutive} checks (${mismatch_notional}); auto-flattening residual reduce-only"
                );
                account_refresh_paused.store(true, Ordering::Release);
                let recovery_result =
                    recover_if_needed(&cfg, &spec, &aster, &lighter, &http, account.margins).await;
                account_refresh_paused.store(false, Ordering::Release);
                match recovery_result {
                    Ok(recovery) => {
                        mismatch_consecutive = 0;
                        warn!(
                            "mismatch auto-flatten complete action_taken={} estimated_loss=${} final_aster={} final_lighter={}",
                            recovery.action_taken,
                            recovery.estimated_loss_usdc,
                            recovery.position.aster_qty,
                            recovery.position.lighter_qty
                        );
                        if recovery.action_taken {
                            if let Some(reason) =
                                recovered_failures.record(recovery.estimated_loss_usdc, &cfg)
                            {
                                bail!("recovered-failure breaker triggered: {reason}");
                            }
                            record_recovery_loss(&mut pnl, &spec, &recovery)?;
                        }
                        account = AccountSnapshot {
                            position: recovery.position,
                            lighter_ws_qty: recovery.lighter_ws_qty,
                            lighter_ws_rest_divergence_qty: recovery
                                .lighter_ws_qty
                                .map(|ws| (ws - recovery.position.lighter_qty).abs()),
                            margins: recovery.margin_after,
                            refreshed_at: tokio::time::Instant::now(),
                        };
                        let _ = account_tx.send(account);
                    }
                    Err(e) => {
                        error!("mismatch auto-flatten failed: {e:#}");
                        // Repeated failure to even inspect/flatten means the bot cannot
                        // guarantee its own safety: exit nonzero so the supervisor
                        // safe-halts loudly instead of pausing on a naked position forever.
                        if mismatch_consecutive
                            >= cfg.risk.mismatch_flatten_after_checks.saturating_mul(3)
                        {
                            bail!(
                                "position mismatch persisted {mismatch_consecutive} checks and auto-flatten kept failing: {e:#}"
                            );
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(cfg.risk.min_reconcile_interval_ms)).await;
            continue;
        }
        mismatch_consecutive = 0;
        let mark = aster_book
            .mid()
            .or_else(|| lighter_book.mid())
            .unwrap_or(Decimal::ZERO);
        if let (Some(ws_qty), Some(divergence_qty)) = (
            account.lighter_ws_qty,
            account.lighter_ws_rest_divergence_qty,
        ) {
            let divergence_notional = divergence_qty * mark;
            if divergence_notional > cfg.risk.max_position_mismatch_usd {
                warn!(
                    "Lighter REST/WS position divergence too large; pausing new entries: rest_qty={} ws_qty={} divergence_qty={} divergence_notional=${}",
                    pos.lighter_qty,
                    ws_qty,
                    divergence_qty,
                    divergence_notional
                );
                tokio::time::sleep(Duration::from_millis(cfg.risk.min_reconcile_interval_ms)).await;
                continue;
            }
        }
        let margins = account.margins;
        let Some(pos_f) = PositionF64::from_snapshot(pos) else {
            warn!(
                "position f64 conversion failed; skipping scan iteration: aster={} lighter={}",
                pos.aster_qty, pos.lighter_qty
            );
            tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
            continue;
        };
        let Some(margins_f) = MarginF64::from_snapshot(margins) else {
            warn!(
                "margin f64 conversion failed; skipping scan iteration: aster_available={} lighter_available={}",
                margins.aster_available_usd, margins.lighter_available_usd
            );
            tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
            continue;
        };
        if tracing::enabled!(Level::DEBUG) {
            log_scan_state(&cfg, &spec, &aster_book, &lighter_book, pos, margins);
        }
        let (execution_enabled, valid_lease) =
            execution_lease_enabled(&mut lease_cache, &options, &spec, now);
        if options.control_file.is_some() && valid_lease.is_none() {
            nonce_refreshed_for_lease = None;
        }
        if execution_enabled
            && options.exposure_filter == ExposureFilter::Reduce
            && options.control_file.is_some()
        {
            let lease_key = valid_lease
                .as_ref()
                .and_then(|lease| lease.lease_id.clone())
                .unwrap_or_else(|| "unidentified".to_string());
            if nonce_refreshed_for_lease.as_deref() != Some(lease_key.as_str()) {
                // Run the refresh OFF the scan iteration (a REST round-trip would stall
                // the loop). Execution under the new lease stays gated until the refresh
                // completes — the `continue`s below skip this iteration — so there is no
                // nonce-reuse risk, only a brief entry delay.
                match nonce_refresh_task.take() {
                    Some((pending_key, handle))
                        if pending_key == lease_key && handle.is_finished() =>
                    {
                        match handle.await {
                            Ok(Ok(())) => {
                                nonce_refreshed_for_lease = Some(lease_key.clone());
                                info!(
                                    "refreshed Lighter nonce for reduce execution lease market={} lease_id={lease_key}",
                                    spec.market_id
                                );
                            }
                            Ok(Err(e)) => {
                                return Err(e.context("lease nonce refresh failed"));
                            }
                            Err(e) => bail!("lease nonce refresh task panicked: {e}"),
                        }
                    }
                    Some((pending_key, handle)) if pending_key == lease_key => {
                        // Still in flight: keep waiting, do not execute under this lease.
                        nonce_refresh_task = Some((pending_key, handle));
                        tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
                        continue;
                    }
                    stale => {
                        if let Some((_, handle)) = stale {
                            handle.abort(); // lease changed under a pending refresh
                        }
                        let lighter_for_refresh = lighter.clone();
                        nonce_refresh_task = Some((
                            lease_key.clone(),
                            tokio::spawn(async move { lighter_for_refresh.refresh_nonce().await }),
                        ));
                        tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
                        continue;
                    }
                }
            }
        }
        let Some(opp) = best_opportunity(
            &cfg,
            &spec,
            &math,
            &aster_book,
            &lighter_book,
            pos_f,
            margins_f,
            options.min_size,
            options.exposure_filter,
        ) else {
            tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
            continue;
        };
        if let Some(sanity) = book_sanity.entry_block() {
            if exposure_effect_f64(
                pos_f.aster_qty,
                pos_f.lighter_qty,
                opp.direction,
                opp.qty_f64,
                &math,
            ) != ExposureEffect::Reduce
            {
                let log_now = match last_book_sanity_block_log_at {
                    Some(ts) => ts.elapsed() >= Duration::from_secs(5),
                    None => true,
                };
                if log_now {
                    warn!(
                        "book sanity blocked new ARB entry market={} direction={} qty={} reason={:?} blocked_until={:?} failure_streak={} success_streak={}",
                        spec.market_id,
                        opp.direction.as_str(),
                        opp.qty,
                        sanity.last_reason,
                        sanity.blocked_until,
                        sanity.failure_streak,
                        sanity.success_streak
                    );
                    last_book_sanity_block_log_at = Some(tokio::time::Instant::now());
                }
                tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
                continue;
            }
        } else {
            last_book_sanity_block_log_at = None;
        }
        if tracing::enabled!(Level::DEBUG) {
            debug!(
                "arb opportunity {} qty={} gross={}bps net_margin={}bps sell_vwap={} buy_vwap={} expected_gross=${} expected_fee=${} expected_net=${} threshold_margin=${} min_qty={} desired_qty={} top_depth={} depth_supported={} liquidity_multiple={} sell_depth_target={} buy_depth_target={} sell_depth_available={} buy_depth_available={} sell_levels_used={} buy_levels_used={} headroom={} margin_room={}",
                opp.direction.as_str(),
                opp.qty,
                opp.gross_edge_bps,
                opp.expected_net_margin_bps,
                opp.sell_px,
                opp.buy_px,
                opp.expected_gross_usd,
                opp.expected_fee_usd,
                opp.expected_net_usd,
                opp.required_margin_usd,
                opp.min_qty,
                opp.desired_qty,
                opp.top_depth_qty,
                opp.depth_supported_qty,
                opp.liquidity_multiple,
                opp.sell_depth_target_qty,
                opp.buy_depth_target_qty,
                opp.sell_depth_available_qty,
                opp.buy_depth_available_qty,
                opp.sell_depth_levels_used,
                opp.buy_depth_levels_used,
                opp.headroom_qty,
                opp.margin_room_qty
            );
        }
        let gate = entry_gate.evaluate(
            OpportunityGateInput {
                timestamp: now,
                direction: opp.direction.as_str(),
                gross_edge_bps: opp.gross_edge_bps,
                expected_net_margin_bps: opp.expected_net_margin_bps,
                expected_net_usd: opp.expected_net_usd,
                qty: opp.qty,
                sell_px: opp.sell_px,
                buy_px: opp.buy_px,
                ref_px: opp.ref_px,
                top_depth_qty: opp.top_depth_qty,
                depth_guard_enabled: opp.depth_guard_enabled,
                liquidity_multiple: opp.liquidity_multiple,
                depth_supported_qty: opp.depth_supported_qty,
                sell_depth_target_qty: opp.sell_depth_target_qty,
                buy_depth_target_qty: opp.buy_depth_target_qty,
                sell_depth_available_qty: opp.sell_depth_available_qty,
                buy_depth_available_qty: opp.buy_depth_available_qty,
                sell_depth_worst_px: opp.sell_depth_worst_px,
                buy_depth_worst_px: opp.buy_depth_worst_px,
                sell_depth_levels_used: opp.sell_depth_levels_used,
                buy_depth_levels_used: opp.buy_depth_levels_used,
                sell_best_px: opp.sell_best_px,
                buy_best_px: opp.buy_best_px,
                sell_best_qty: opp.sell_best_qty,
                buy_best_qty: opp.buy_best_qty,
                aster_book_age_ms: aster_book.age_ms(now),
                lighter_book_age_ms: lighter_book.age_ms(now),
                force_record: execution_enabled,
            },
            cfg.arb.required_gross_edge_bps(),
        );
        if gate.recorded || !gate.allow_execution {
            // A visible-but-gated edge re-evaluates every poll; only log transitions
            // and a 5s heartbeat. Recorded samples (<=1/s by design) always log.
            let log_now = gate.recorded
                || match last_gate_log {
                    Some((decision, at)) => {
                        decision != gate.decision || at.elapsed() >= Duration::from_secs(5)
                    }
                    None => true,
                };
            if log_now {
                info!(
                    "entry gate decision market={} mode={} decision={} allow_execution={} would_allow={} gross={}bps threshold={:?} samples={} recorded={}",
                    spec.market_id,
                    cfg.arb.entry_gate.mode.as_str(),
                    gate.decision,
                    gate.allow_execution,
                    gate.would_allow,
                    opp.gross_edge_bps,
                    gate.threshold_bps,
                    gate.sample_count,
                    gate.recorded
                );
                last_gate_log = Some((gate.decision, tokio::time::Instant::now()));
            }
        }
        if !gate.allow_execution {
            tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
            continue;
        }
        if exposure_effect_f64(
            pos_f.aster_qty,
            pos_f.lighter_qty,
            opp.direction,
            opp.qty_f64,
            &math,
        ) == ExposureEffect::Reduce
        {
            reduce_signal_tracker.observe(&spec, &opp, &gate, now);
        }
        if !execution_enabled {
            // Standby/observe hits this every poll while an edge exists; 5s heartbeat.
            let log_now = match last_standby_log_at {
                Some(at) => at.elapsed() >= Duration::from_secs(5),
                None => true,
            };
            if log_now {
                info!(
                    "standby skip order submission market={} direction={} qty={} gross={}bps expected_net=${} gate_decision={} threshold={:?} samples={} recorded={} observe_only={} lease_required={}",
                    spec.market_id,
                    opp.direction.as_str(),
                    opp.qty,
                    opp.gross_edge_bps,
                    opp.expected_net_usd,
                    gate.decision,
                    gate.threshold_bps,
                    gate.sample_count,
                    gate.recorded,
                    options.observe_only,
                    options.control_file.is_some()
                );
                last_standby_log_at = Some(tokio::time::Instant::now());
            }
            tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
            continue;
        }
        let reduce_only = options.exposure_filter == ExposureFilter::Reduce;
        if reduce_only
            && exposure_effect_f64(
                pos_f.aster_qty,
                pos_f.lighter_qty,
                opp.direction,
                opp.qty_f64,
                &math,
            ) != ExposureEffect::Reduce
        {
            warn!(
                "reduce-only execution guard skipped non-reducing opportunity market={} direction={} qty={} pos_aster={} pos_lighter={}",
                spec.market_id,
                opp.direction.as_str(),
                opp.qty,
                pos.aster_qty,
                pos.lighter_qty
            );
            tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
            continue;
        }
        if reduce_only && options.control_file.is_some() {
            match tokio::join!(
                aster.open_orders(&spec.market_id),
                lighter.open_orders_count(&spec.market_id),
            ) {
                (Ok(open_a), Ok(open_l)) if open_a.is_empty() && open_l == 0 => {}
                (Ok(open_a), Ok(open_l)) => {
                    warn!(
                        "reduce-only lease execution skipped because open orders remain: market={} aster_open_orders={} lighter_open_orders={}",
                        spec.market_id,
                        open_a.len(),
                        open_l
                    );
                    tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
                    continue;
                }
                (a_res, l_res) => {
                    warn!(
                        "reduce-only lease execution skipped because open-order check failed: market={} aster_result={:?} lighter_result={:?}",
                        spec.market_id,
                        a_res.as_ref().map(|orders| orders.len()),
                        l_res
                    );
                    tokio::time::sleep(Duration::from_millis(cfg.arb.poll_interval_ms)).await;
                    continue;
                }
            }
        }
        account_refresh_paused.store(true, Ordering::Release);
        let execution_result = execute_opportunity(
            &cfg,
            &spec,
            &aster,
            &lighter,
            &opp,
            pos,
            margins,
            reduce_only,
        )
        .await;
        match execution_result {
            Ok(report) => {
                account = AccountSnapshot {
                    position: report.position,
                    lighter_ws_qty: report.lighter_ws_qty,
                    lighter_ws_rest_divergence_qty: report.lighter_ws_rest_divergence_qty,
                    margins: report.margin_after,
                    refreshed_at: tokio::time::Instant::now(),
                };
                let _ = account_tx.send(account);
                account_refresh_paused.store(false, Ordering::Release);
                trades_executed += 1;
                info!(
                    "trade report count={} expected_net=${} actual_gross=${} actual_fees=${} actual_net=${} actual_net_bps={} fill_qty_mismatch={} aster_fill_qty={} aster_vwap={} aster_notional=${} aster_fee=${} lighter_fill_qty={} lighter_vwap={} lighter_notional=${} lighter_fee=${} available_margin_delta=${} available_before=${} available_after=${} aster_available_before=${} aster_available_after=${} lighter_available_before=${} lighter_available_after=${} final_aster_pos={} final_lighter_pos={} final_net_pos={}",
                    trades_executed,
                    opp.expected_net_usd,
                    report.economics.gross_usd,
                    report.economics.fees_usd,
                    report.economics.net_usd,
                    report.economics.net_bps,
                    report.economics.fill_qty_mismatch,
                    report.economics.aster_fill.qty,
                    report.economics.aster_fill.vwap,
                    report.economics.aster_fill.notional,
                    report.economics.aster_fill.fee_usd,
                    report.economics.lighter_fill.qty,
                    report.economics.lighter_fill.vwap,
                    report.economics.lighter_fill.notional,
                    report.economics.lighter_fill.fee_usd,
                    report.available_margin_delta_usd(),
                    report.margin_before.aster_available_usd + report.margin_before.lighter_available_usd,
                    report.margin_after.aster_available_usd + report.margin_after.lighter_available_usd,
                    report.margin_before.aster_available_usd,
                    report.margin_after.aster_available_usd,
                    report.margin_before.lighter_available_usd,
                    report.margin_after.lighter_available_usd,
                    report.position.aster_qty,
                    report.position.lighter_qty,
                    report.position.net_qty()
                );
                if report.hedge_retry_action_taken {
                    warn!(
                        "hedge retry counted as recovered risk event count_limit={} loss_limit=${}",
                        cfg.arb.max_recovered_failures_per_hour,
                        cfg.arb.max_recovered_loss_usdc_per_hour
                    );
                    if let Some(reason) = recovered_failures.record(Decimal::ZERO, &cfg) {
                        bail!("recovered-failure breaker triggered after hedge retry: {reason}");
                    }
                }
                if let Some(pnl) = pnl.as_mut() {
                    let row = pnl_trade_row(&spec, &opp, &report);
                    let update = pnl.record_trade(row)?;
                    let snapshot = pnl.snapshot();
                    info!(
                        "pnl update market={} since={} trades={} cumulative_pnl=${} max_loss=${} breaker_tripped={}",
                        spec.market_id,
                        format_ts(snapshot.since),
                        update.trade_count,
                        update.cumulative_pnl_usdc,
                        snapshot.max_loss_usdc,
                        update.breaker.is_some()
                    );
                    if let Some(breaker) = update.breaker {
                        error!(
                            "circuit breaker triggered: market={} since={} cumulative_pnl=${} max_loss=${} last_trade_net=${}",
                            breaker.market,
                            format_ts(breaker.pnl_since),
                            breaker.cumulative_pnl_usdc,
                            breaker.max_loss_usdc,
                            breaker.last_trade_actual_net_usd
                        );
                        bail!(
                            "circuit breaker triggered: cumulative PnL ${} <= -${}; manual reset required",
                            breaker.cumulative_pnl_usdc,
                            breaker.max_loss_usdc
                        );
                    }
                }
                if options.max_trades.is_some_and(|max| trades_executed >= max) {
                    info!("max_trades reached; stopping");
                    break;
                }
                let cooldown_ms = if reduce_only {
                    options.reduce_cooldown_ms
                } else {
                    cfg.arb.cooldown_ms
                };
                cooldown_until = tokio::time::Instant::now() + Duration::from_millis(cooldown_ms);
            }
            Err(e) => {
                if e.is_skip() {
                    warn!("arb execution skipped: {e:#}");
                    let cooldown_ms = if options.exposure_filter == ExposureFilter::Reduce {
                        options.reduce_cooldown_ms.max(1000)
                    } else {
                        cfg.arb.cooldown_ms.max(1000)
                    };
                    cooldown_until =
                        tokio::time::Instant::now() + Duration::from_millis(cooldown_ms);
                    account_refresh_paused.store(false, Ordering::Release);
                    continue;
                }
                let needs_recovery = e.needs_recovery();
                if needs_recovery {
                    error!("arb execution failed; checking for rescue: {e:#}");
                } else {
                    warn!("arb execution failed without one-sided acceptance: {e:#}");
                }
                if !needs_recovery {
                    let cooldown_ms = if options.exposure_filter == ExposureFilter::Reduce {
                        options.reduce_cooldown_ms.max(1000)
                    } else {
                        cfg.arb.cooldown_ms.max(1000)
                    };
                    cooldown_until =
                        tokio::time::Instant::now() + Duration::from_millis(cooldown_ms);
                    account_refresh_paused.store(false, Ordering::Release);
                    continue;
                }
                account_refresh_paused.store(true, Ordering::Release);
                let recovery_result =
                    recover_if_needed(&cfg, &spec, &aster, &lighter, &http, margins).await;
                let recovery = recovery_result?;
                info!(
                    "rescue check complete action_taken={} estimated_loss=${} final_aster_pos={} final_lighter_pos={} lighter_ws={:?} aster_open_orders={} lighter_open_orders={} available_after=${}",
                    recovery.action_taken,
                    recovery.estimated_loss_usdc,
                    recovery.position.aster_qty,
                    recovery.position.lighter_qty,
                    recovery.lighter_ws_qty,
                    recovery.aster_open_orders,
                    recovery.lighter_open_orders,
                    recovery.margin_after.aster_available_usd + recovery.margin_after.lighter_available_usd
                );
                account = AccountSnapshot {
                    position: recovery.position,
                    lighter_ws_qty: recovery.lighter_ws_qty,
                    lighter_ws_rest_divergence_qty: recovery
                        .lighter_ws_qty
                        .map(|ws| (ws - recovery.position.lighter_qty).abs()),
                    margins: recovery.margin_after,
                    refreshed_at: tokio::time::Instant::now(),
                };
                let _ = account_tx.send(account);
                account_refresh_paused.store(false, Ordering::Release);
                if recovery.action_taken {
                    if let Some(reason) =
                        recovered_failures.record(recovery.estimated_loss_usdc, &cfg)
                    {
                        bail!("recovered-failure breaker triggered: {reason}");
                    }
                    record_recovery_loss(&mut pnl, &spec, &recovery)?;
                }
                match refresh_account_snapshot(&spec.market_id, &aster, &lighter).await {
                    Ok(snapshot) => {
                        account = snapshot;
                        let _ = account_tx.send(account);
                    }
                    Err(refresh_err) => {
                        warn!("account snapshot refresh failed after recovery: {refresh_err:#}")
                    }
                }
                let cooldown_ms = if options.exposure_filter == ExposureFilter::Reduce {
                    options.reduce_cooldown_ms.max(1000)
                } else {
                    cfg.arb.cooldown_ms.max(1000)
                };
                cooldown_until = tokio::time::Instant::now() + Duration::from_millis(cooldown_ms);
            }
        }
    }
    Ok(())
}

fn fetch_books(
    spec: &MarketSpec,
    aster_books: &AsterBookFeed,
    lighter: &LighterVenue,
) -> Result<(OrderBook, OrderBook)> {
    let aster = aster_books.order_book()?;
    let lighter_book = lighter.order_book(&spec.market_id)?;
    Ok((aster, lighter_book))
}

async fn fetch_books_rest_lighter(
    cfg: &Config,
    spec: &MarketSpec,
    http: &reqwest::Client,
    lighter: &LighterVenue,
) -> Result<(OrderBook, OrderBook)> {
    let aster =
        rest_book::fetch_aster_book(http, &cfg.venues.aster_base_url, &spec.aster_symbol, 20)
            .await?;
    let lighter_book = lighter.order_book(&spec.market_id)?;
    Ok((aster, lighter_book))
}

fn book_ok(book: &OrderBook, now: chrono::DateTime<Utc>, max_age_ms: i64) -> bool {
    let age_ms = book.age_ms(now);
    book.best_bid().is_some()
        && book.best_ask().is_some()
        && !book.is_crossed()
        && age_ms >= 0
        && age_ms <= max_age_ms
}

fn best_opportunity(
    cfg: &Config,
    spec: &MarketSpec,
    math: &MarketMathF64,
    aster: &OrderBook,
    lighter: &OrderBook,
    pos: PositionF64,
    margins: MarginF64,
    min_size: bool,
    exposure_filter: ExposureFilter,
) -> Option<Opportunity> {
    let required = cfg.arb.required_gross_edge_bps();
    // f64 mirror of `required` for the cheap pre-filter; a non-representable Decimal
    // falls back to NEG_INFINITY, which disables early skipping (never fails open).
    let required_f = decimal_to_f64(required).unwrap_or(f64::NEG_INFINITY);

    let mut best: Option<Opportunity> = None;
    for direction in [
        Direction::SellAsterBuyLighter,
        Direction::SellLighterBuyAster,
    ] {
        let Some((sizing, sell_px, buy_px, ref_px)) =
            depth_priced_sizing(cfg, spec, math, direction, aster, lighter, pos, margins, min_size)
        else {
            continue;
        };
        // Same expression as build_opportunity_f64 -> identical f64 by IEEE determinism.
        let gross_edge_bps_f = (sell_px - buy_px) / ref_px * 10_000.0;
        if gross_edge_bps_f < required_f - EDGE_PREFILTER_EPS_BPS {
            // Clearly below the edge floor: skip the ~30-conversion Decimal build.
            // Candidates within the epsilon band fall through to the exact filter.
            continue;
        }
        let opp = build_opportunity_f64(cfg, math, direction, sizing, sell_px, buy_px, ref_px);
        if opp.gross_edge_bps < required {
            continue;
        }
        if opportunity_allowed_by_exposure_filter(pos, &opp, exposure_filter, math) {
            best = Some(choose_better_opportunity(best, opp));
        }
    }

    best
}

fn opportunity_allowed_by_exposure_filter(
    pos: PositionF64,
    opp: &Opportunity,
    exposure_filter: ExposureFilter,
    math: &MarketMathF64,
) -> bool {
    match exposure_filter {
        ExposureFilter::Any => true,
        ExposureFilter::Reduce => {
            exposure_effect_f64(
                pos.aster_qty,
                pos.lighter_qty,
                opp.direction,
                opp.qty_f64,
                math,
            ) == ExposureEffect::Reduce
        }
    }
}

fn exposure_effect(pos: PositionSnapshot, direction: Direction, qty: Decimal) -> ExposureEffect {
    if qty <= Decimal::ZERO {
        return ExposureEffect::Unknown;
    }
    let a_sign = if matches!(direction.aster_side(), Side::Buy) {
        Decimal::ONE
    } else {
        -Decimal::ONE
    };
    let l_sign = -a_sign;
    let before = pos.aster_qty.abs().max(pos.lighter_qty.abs());
    let after_a = pos.aster_qty + a_sign * qty;
    let after_l = pos.lighter_qty + l_sign * qty;
    let after = after_a.abs().max(after_l.abs());
    if after < before {
        ExposureEffect::Reduce
    } else if after > before {
        ExposureEffect::Increase
    } else {
        ExposureEffect::Flat
    }
}

fn exposure_effect_f64(
    aster_qty: f64,
    lighter_qty: f64,
    direction: Direction,
    qty: f64,
    math: &MarketMathF64,
) -> ExposureEffect {
    if qty <= 0.0 || !qty.is_finite() || !aster_qty.is_finite() || !lighter_qty.is_finite() {
        return ExposureEffect::Unknown;
    }
    let a_sign = if matches!(direction.aster_side(), Side::Buy) {
        1.0
    } else {
        -1.0
    };
    let l_sign = -a_sign;
    let before = aster_qty.abs().max(lighter_qty.abs());
    let after_a = aster_qty + a_sign * qty;
    let after_l = lighter_qty + l_sign * qty;
    let after = after_a.abs().max(after_l.abs());
    let tol = qty_cmp_tol(math, before.max(qty), after);
    if after + tol < before {
        ExposureEffect::Reduce
    } else if after > before + tol {
        ExposureEffect::Increase
    } else {
        ExposureEffect::Flat
    }
}

fn choose_better_opportunity(current: Option<Opportunity>, candidate: Opportunity) -> Opportunity {
    match current {
        None => candidate,
        Some(existing) => {
            if candidate.expected_net_usd > existing.expected_net_usd
                || (candidate.expected_net_usd == existing.expected_net_usd
                    && candidate.expected_net_margin_bps > existing.expected_net_margin_bps)
            {
                candidate
            } else {
                existing
            }
        }
    }
}

fn build_opportunity_f64(
    cfg: &Config,
    math: &MarketMathF64,
    direction: Direction,
    sizing: SizingDecisionF64,
    sell_px_f: f64,
    buy_px_f: f64,
    ref_px_f: f64,
) -> Opportunity {
    let gross_edge_bps_f = (sell_px_f - buy_px_f) / ref_px_f * 10_000.0;
    let aster_px_f = if matches!(direction.aster_side(), Side::Sell) {
        sell_px_f
    } else {
        buy_px_f
    };
    let lighter_px_f = if matches!(direction.lighter_side(), Side::Sell) {
        sell_px_f
    } else {
        buy_px_f
    };
    let gross_usd_f = sizing.qty * (sell_px_f - buy_px_f);
    let fee_usd_f = sizing.qty
        * (aster_px_f * math.aster_taker_fee_rate
            + lighter_px_f * math.lighter_taker_fee_rate);
    let required_margin_usd_f = sizing.qty * ref_px_f * math.margin_rate;
    let sell_px = f64_to_dec(sell_px_f);
    let buy_px = f64_to_dec(buy_px_f);
    let ref_px = f64_to_dec(ref_px_f);
    let gross_edge_bps = f64_to_dec(gross_edge_bps_f);
    let gross_usd = f64_to_dec(gross_usd_f);
    let fee_usd = f64_to_dec(fee_usd_f);
    Opportunity {
        direction,
        qty: qty_f64_to_dec(math, sizing.qty),
        qty_f64: sizing.qty,
        gross_edge_bps,
        expected_net_margin_bps: gross_edge_bps - cfg.arb.required_gross_edge_bps(),
        sell_px,
        buy_px,
        ref_px,
        top_depth_qty: f64_to_dec(sizing.top_depth_qty),
        depth_guard_enabled: sizing.depth_guard_enabled,
        liquidity_multiple: f64_to_dec(sizing.liquidity_multiple),
        depth_supported_qty: f64_to_dec(sizing.depth_supported_qty),
        sell_depth_target_qty: f64_to_dec(sizing.sell_depth_target_qty),
        buy_depth_target_qty: f64_to_dec(sizing.buy_depth_target_qty),
        sell_depth_available_qty: f64_to_dec(sizing.sell_depth_available_qty),
        buy_depth_available_qty: f64_to_dec(sizing.buy_depth_available_qty),
        sell_depth_worst_px: f64_to_dec(sizing.sell_depth_worst_px),
        buy_depth_worst_px: f64_to_dec(sizing.buy_depth_worst_px),
        sell_depth_levels_used: sizing.sell_depth_levels_used,
        buy_depth_levels_used: sizing.buy_depth_levels_used,
        sell_best_px: f64_to_dec(sizing.sell_best_px),
        buy_best_px: f64_to_dec(sizing.buy_best_px),
        sell_best_qty: f64_to_dec(sizing.sell_best_qty),
        buy_best_qty: f64_to_dec(sizing.buy_best_qty),
        desired_qty: f64_to_dec(sizing.desired_qty),
        min_qty: f64_to_dec(sizing.min_qty),
        headroom_qty: f64_to_dec(sizing.headroom_qty),
        margin_room_qty: f64_to_dec(sizing.margin_room_qty),
        expected_gross_usd: gross_usd,
        expected_fee_usd: fee_usd,
        expected_net_usd: gross_usd - fee_usd,
        required_margin_usd: f64_to_dec(required_margin_usd_f),
    }
}

fn depth_priced_opportunity(
    cfg: &Config,
    spec: &MarketSpec,
    math: &MarketMathF64,
    direction: Direction,
    aster: &OrderBook,
    lighter: &OrderBook,
    pos: PositionF64,
    margins: MarginF64,
    min_size: bool,
) -> Option<Opportunity> {
    let (sizing, sell_px, buy_px, ref_px_f) = depth_priced_sizing(
        cfg, spec, math, direction, aster, lighter, pos, margins, min_size,
    )?;
    Some(build_opportunity_f64(
        cfg, math, direction, sizing, sell_px, buy_px, ref_px_f,
    ))
}

/// The all-f64 sizing/pricing stage of [`depth_priced_opportunity`], split out so the
/// scan loop can apply the edge pre-filter before paying for the Decimal build.
fn depth_priced_sizing(
    cfg: &Config,
    spec: &MarketSpec,
    math: &MarketMathF64,
    direction: Direction,
    aster: &OrderBook,
    lighter: &OrderBook,
    pos: PositionF64,
    margins: MarginF64,
    min_size: bool,
) -> Option<(SizingDecisionF64, f64, f64, f64)> {
    let ref_px_f = aster.mid_f64().or_else(|| lighter.mid_f64())?;
    if ref_px_f <= 0.0 {
        return None;
    }
    let (sell_book, buy_book) = match direction {
        Direction::SellAsterBuyLighter => (aster, lighter),
        Direction::SellLighterBuyAster => (lighter, aster),
    };
    let sell_top = sell_book.best_bid_f64()?;
    let buy_top = buy_book.best_ask_f64()?;
    let top_depth_qty_f = sell_top.1.min(buy_top.1);
    let desired_f = math.desired_notional / ref_px_f;
    let est_aster_px = if matches!(direction.aster_side(), Side::Sell) {
        sell_top.0
    } else {
        buy_top.0
    };
    let est_lighter_px = if matches!(direction.lighter_side(), Side::Sell) {
        sell_top.0
    } else {
        buy_top.0
    };
    let est_min_qty = min_trade_qty_f64(math, est_aster_px, est_lighter_px)?;
    let a_delta_sign = if matches!(direction.aster_side(), Side::Buy) {
        1.0
    } else {
        -1.0
    };
    let l_delta_sign = -a_delta_sign;
    let max_abs_qty_f = math.max_abs_position_notional_usd / ref_px_f;
    let headroom = max_qty_by_headroom_f64(
        max_abs_qty_f,
        pos.aster_qty,
        pos.lighter_qty,
        a_delta_sign,
        l_delta_sign,
    );
    let margin_room = max_qty_by_available_margin_f64(
        ref_px_f,
        pos.aster_qty,
        pos.lighter_qty,
        a_delta_sign,
        l_delta_sign,
        margins.aster_available_usd,
        margins.lighter_available_usd,
        math.margin_buffer_usd,
    );

    let depth_guard_enabled = cfg.arb.depth_guard.enabled;
    let liquidity_multiple = math.liquidity_multiple(depth_guard_enabled);
    let max_levels = if depth_guard_enabled {
        cfg.arb.depth_guard.max_levels
    } else {
        1
    };
    let sell_available = sell_book.cumulative_qty_f64(Side::Sell, max_levels)?;
    let buy_available = buy_book.cumulative_qty_f64(Side::Buy, max_levels)?;
    let depth_supported_qty = sell_available.min(buy_available) / liquidity_multiple;
    let max_qty = depth_supported_qty.min(headroom).min(margin_room);
    if max_qty <= 0.0 {
        return None;
    }

    let initial_qty = if min_size {
        let q = ceil_to_common_step_f64(est_min_qty, math);
        if qty_le(q, max_qty, math) {
            q
        } else {
            return None;
        }
    } else {
        let q = floor_to_common_step_f64(desired_f.min(max_qty), math);
        if qty_ge(q, est_min_qty, math) {
            q
        } else {
            return None;
        }
    };

    let (sizing, sell_px, buy_px) = depth_price_sized_qty_f64(
        spec,
        math,
        direction,
        sell_book,
        buy_book,
        initial_qty,
        desired_f,
        top_depth_qty_f,
        headroom,
        margin_room,
        depth_guard_enabled,
        liquidity_multiple,
        max_levels,
        depth_supported_qty,
        min_size,
    )?;
    Some((sizing, sell_px, buy_px, ref_px_f))
}

fn depth_price_sized_qty_f64(
    _spec: &MarketSpec,
    math: &MarketMathF64,
    direction: Direction,
    sell_book: &OrderBook,
    buy_book: &OrderBook,
    initial_qty: f64,
    desired_qty: f64,
    top_depth_qty: f64,
    headroom_qty: f64,
    margin_room_qty: f64,
    depth_guard_enabled: bool,
    liquidity_multiple: f64,
    max_levels: usize,
    depth_supported_qty: f64,
    min_size: bool,
) -> Option<(SizingDecisionF64, f64, f64)> {
    let mut qty = initial_qty;
    for _ in 0..3 {
        if qty <= 0.0 || !qty.is_finite() || qty_gt(qty, depth_supported_qty, math) {
            return None;
        }
        let depth_target = qty * liquidity_multiple;
        let sell_quote = sell_book.depth_vwap_f64(Side::Sell, depth_target, max_levels)?;
        let buy_quote = buy_book.depth_vwap_f64(Side::Buy, depth_target, max_levels)?;
        let sell_px = sell_quote.vwap_px;
        let buy_px = buy_quote.vwap_px;
        let aster_px = if matches!(direction.aster_side(), Side::Sell) {
            sell_px
        } else {
            buy_px
        };
        let lighter_px = if matches!(direction.lighter_side(), Side::Sell) {
            sell_px
        } else {
            buy_px
        };
        let min_qty = min_trade_qty_f64(math, aster_px, lighter_px)?;
        if min_size {
            let min_step_qty = ceil_to_common_step_f64(min_qty, math);
            if qty_gt(min_step_qty, qty, math) {
                if qty_gt(min_step_qty, depth_supported_qty, math)
                    || qty_gt(min_step_qty, headroom_qty, math)
                    || qty_gt(min_step_qty, margin_room_qty, math)
                {
                    return None;
                }
                qty = min_step_qty;
                continue;
            }
        } else if !qty_ge(qty, min_qty, math) {
            return None;
        }
        return Some((
            SizingDecisionF64 {
                qty,
                desired_qty,
                min_qty,
                top_depth_qty,
                depth_guard_enabled,
                liquidity_multiple,
                depth_supported_qty,
                sell_depth_target_qty: sell_quote.target_qty,
                buy_depth_target_qty: buy_quote.target_qty,
                sell_depth_available_qty: sell_quote.available_qty,
                buy_depth_available_qty: buy_quote.available_qty,
                sell_depth_worst_px: sell_quote.worst_px,
                buy_depth_worst_px: buy_quote.worst_px,
                sell_depth_levels_used: sell_quote.levels_used,
                buy_depth_levels_used: buy_quote.levels_used,
                sell_best_px: sell_quote.best_px,
                buy_best_px: buy_quote.best_px,
                sell_best_qty: sell_quote.best_qty,
                buy_best_qty: buy_quote.best_qty,
                headroom_qty,
                margin_room_qty,
            },
            sell_px,
            buy_px,
        ));
    }
    None
}

fn min_trade_qty_f64(math: &MarketMathF64, aster_px: f64, lighter_px: f64) -> Option<f64> {
    if aster_px <= 0.0 || lighter_px <= 0.0 || !aster_px.is_finite() || !lighter_px.is_finite() {
        return None;
    }
    Some(
        math.aster_min_qty
            .max(math.aster_min_notional / aster_px)
            .max(math.lighter_min_notional / lighter_px),
    )
}

fn max_qty_by_headroom_f64(
    max_abs_qty: f64,
    aster_qty: f64,
    lighter_qty: f64,
    a_sign: f64,
    l_sign: f64,
) -> f64 {
    fn leg(max_abs_qty: f64, current: f64, sign: f64) -> f64 {
        let same_direction = current == 0.0
            || (current > 0.0 && sign > 0.0)
            || (current < 0.0 && sign < 0.0);
        if same_direction {
            (max_abs_qty - current.abs()).max(0.0)
        } else {
            current.abs() + max_abs_qty
        }
    }
    leg(max_abs_qty, aster_qty, a_sign).min(leg(max_abs_qty, lighter_qty, l_sign))
}

fn max_qty_by_available_margin_f64(
    ref_px: f64,
    aster_qty: f64,
    lighter_qty: f64,
    a_sign: f64,
    l_sign: f64,
    aster_available: f64,
    lighter_available: f64,
    buffer: f64,
) -> f64 {
    fn leg(ref_px: f64, current: f64, sign: f64, available: f64, buffer: f64) -> f64 {
        let increases_abs = current == 0.0
            || (current > 0.0 && sign > 0.0)
            || (current < 0.0 && sign < 0.0);
        if !increases_abs {
            return f64::MAX;
        }
        let usable = available - buffer;
        if usable <= 0.0 || ref_px <= 0.0 {
            0.0
        } else {
            usable / ref_px
        }
    }
    leg(ref_px, aster_qty, a_sign, aster_available, buffer)
        .min(leg(ref_px, lighter_qty, l_sign, lighter_available, buffer))
}

fn floor_to_common_step_f64(qty: f64, math: &MarketMathF64) -> f64 {
    if qty <= 0.0 || !qty.is_finite() {
        return 0.0;
    }
    let units = snap_step_units(qty / math.common_qty_step).floor();
    round_qty_to_scale_f64(units * math.common_qty_step, math)
}

fn ceil_to_common_step_f64(qty: f64, math: &MarketMathF64) -> f64 {
    if qty <= 0.0 || !qty.is_finite() {
        return 0.0;
    }
    let units = snap_step_units(qty / math.common_qty_step).ceil();
    round_qty_to_scale_f64(units * math.common_qty_step, math)
}

#[cfg(test)]
fn min_trade_qty(
    spec: &MarketSpec,
    aster_px: Decimal,
    lighter_px: Decimal,
) -> Option<Decimal> {
    if aster_px <= Decimal::ZERO || lighter_px <= Decimal::ZERO {
        return None;
    }
    Some(
        spec.aster_min_qty
            .max(spec.aster_min_notional / aster_px)
            .max(spec.lighter_min_notional / lighter_px),
    )
}

#[cfg(test)]
fn max_qty_by_headroom(
    max_abs_qty: Decimal,
    pos: PositionSnapshot,
    a_sign: Decimal,
    l_sign: Decimal,
) -> Decimal {
    fn leg(max_abs_qty: Decimal, current: Decimal, sign: Decimal) -> Decimal {
        let same_direction = current == Decimal::ZERO
            || (current > Decimal::ZERO && sign > Decimal::ZERO)
            || (current < Decimal::ZERO && sign < Decimal::ZERO);
        if same_direction {
            (max_abs_qty - current.abs()).max(Decimal::ZERO)
        } else {
            current.abs() + max_abs_qty
        }
    }
    leg(max_abs_qty, pos.aster_qty, a_sign).min(leg(max_abs_qty, pos.lighter_qty, l_sign))
}

#[cfg(test)]
fn max_qty_by_available_margin(
    cfg: &Config,
    ref_px: Decimal,
    pos: PositionSnapshot,
    margins: MarginSnapshot,
    a_sign: Decimal,
    l_sign: Decimal,
) -> Decimal {
    fn leg(
        ref_px: Decimal,
        current: Decimal,
        sign: Decimal,
        available: Decimal,
        buffer: Decimal,
    ) -> Decimal {
        let increases_abs = current == Decimal::ZERO
            || (current > Decimal::ZERO && sign > Decimal::ZERO)
            || (current < Decimal::ZERO && sign < Decimal::ZERO);
        if !increases_abs {
            return Decimal::MAX;
        }
        let usable = available - buffer;
        if usable <= Decimal::ZERO || ref_px <= Decimal::ZERO {
            Decimal::ZERO
        } else {
            usable / ref_px
        }
    }
    leg(
        ref_px,
        pos.aster_qty,
        a_sign,
        margins.aster_available_usd,
        cfg.risk.margin_buffer_usd,
    )
    .min(leg(
        ref_px,
        pos.lighter_qty,
        l_sign,
        margins.lighter_available_usd,
        cfg.risk.margin_buffer_usd,
    ))
}

fn next_execution_id() -> String {
    let seq = EXECUTION_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{seq}", Utc::now().timestamp_millis())
}

fn execution_log_path(cfg: &Config, market: &MarketId) -> PathBuf {
    let component: String = market
        .0
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    PathBuf::from(&cfg.pnl.persist_dir).join(format!("executions_{component}.jsonl"))
}

fn append_execution_log(cfg: &Config, spec: &MarketSpec, row: serde_json::Value) {
    let path = execution_log_path(cfg, &spec.market_id);
    tokio::task::spawn_blocking(move || {
        if let Err(e) = append_execution_log_inner(&path, &row) {
            warn!(
                "failed to append execution diagnostics {}: {e:#}",
                path.display()
            );
        }
    });
}

fn append_execution_log_inner(path: &Path, row: &serde_json::Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create execution diagnostics dir {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open execution diagnostics {}", path.display()))?;
    serde_json::to_writer(&mut file, row)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

fn insert_json<T: Serialize>(
    row: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: T,
) {
    let value = serde_json::to_value(value).unwrap_or(serde_json::Value::Null);
    row.insert(key.to_string(), value);
}

async fn execute_opportunity(
    cfg: &Config,
    spec: &MarketSpec,
    aster: &AsterRest,
    lighter: &LighterVenue,
    opp: &Opportunity,
    pre_position: PositionSnapshot,
    margin_before: MarginSnapshot,
    reduce_only: bool,
) -> std::result::Result<TradeReport, ExecutionError> {
    let execution_id = next_execution_id();
    let started_at = Utc::now();
    let aster_bound = aster_price_bound(opp, cfg.arb.max_aster_slippage_bps);
    let lighter_bound = lighter_price_bound(opp, cfg.arb.max_lighter_slippage_bps);
    let aster_side = opp.direction.aster_side();
    let lighter_side = opp.direction.lighter_side();
    info!(
        "arb execution start execution_id={} market={} direction={} qty={} reduce_only={} gross={}bps expected_net=${} aster_bound={} lighter_bound={} aster_slippage_bps={} lighter_slippage_bps={} pre_aster_pos={} pre_lighter_pos={}",
        execution_id,
        spec.market_id,
        opp.direction.as_str(),
        opp.qty,
        reduce_only,
        opp.gross_edge_bps,
        opp.expected_net_usd,
        aster_bound,
        lighter_bound,
        cfg.arb.max_aster_slippage_bps,
        cfg.arb.max_lighter_slippage_bps,
        pre_position.aster_qty,
        pre_position.lighter_qty
    );
    if tracing::enabled!(Level::DEBUG) {
        debug!(
            "submitting arb legs execution_id={} direction={} qty={} aster_side={} lighter_side={} reduce_only={} aster_price_bound={} lighter_price_bound={} available_before=${} aster_available_before=${} lighter_available_before=${}",
            execution_id,
            opp.direction.as_str(),
            opp.qty,
            aster_side,
            lighter_side,
            reduce_only,
            aster_bound,
            lighter_bound,
            margin_before.aster_available_usd + margin_before.lighter_available_usd,
            margin_before.aster_available_usd,
            margin_before.lighter_available_usd
        );
    }
    let a = aster.submit_ioc_order(
        &spec.market_id,
        aster_side,
        opp.qty,
        aster_bound,
        reduce_only,
    );
    let l = lighter.submit_market_order_deferred_fill(
        &spec.market_id,
        lighter_side,
        opp.qty,
        lighter_bound,
        reduce_only,
    );
    let (a_res, (l_res, lighter_pending_fill)) = tokio::join!(a, l);
    info!(
        "arb leg submission results execution_id={execution_id} Aster={a_res:?} Lighter={l_res:?}"
    );

    let aster_accepted = matches!(&a_res, AsterOutcome::Accepted { .. });
    let lighter_accepted = matches!(&l_res, LighterOutcome::Accepted { .. });
    match (&a_res, &l_res) {
        (
            AsterOutcome::Accepted { venue_order_id, .. },
            LighterOutcome::Accepted {
                client_order_index, ..
            },
        ) => {
            info!(
                "arb legs accepted execution_id={} {} qty={} aster_oid={:?} lighter_client_order_index={}",
                execution_id,
                opp.direction.as_str(),
                opp.qty,
                venue_order_id,
                client_order_index
            );
        }
        _ => {
            let (pos_after, aster_open, lighter_open) = tokio::join!(
                reconcile_positions(&spec.market_id, aster, lighter),
                aster.open_orders(&spec.market_id),
                lighter.rest_open_orders_count(&spec.market_id),
            );
            let lighter_ws_qty = lighter.ws_position_qty(&spec.market_id).ok();
            let pos_after_ok = pos_after.ok();
            append_execution_log(
                cfg,
                spec,
                serde_json::json!({
                    "timestamp": Utc::now(),
                    "started_at": started_at,
                    "execution_id": execution_id,
                    "market": spec.market_id.to_string(),
                    "execution_mode": "concurrent_confirm_rescue",
                    "outcome": "submit_rejected",
                    "direction": opp.direction.as_str(),
                    "qty": opp.qty,
                    "reduce_only": reduce_only,
                    "gross_edge_bps": opp.gross_edge_bps,
                    "expected_net_usd": opp.expected_net_usd,
                    "depth_guard": {
                        "enabled": opp.depth_guard_enabled,
                        "liquidity_multiple": opp.liquidity_multiple,
                        "depth_supported_qty": opp.depth_supported_qty,
                        "sell_depth_target_qty": opp.sell_depth_target_qty,
                        "buy_depth_target_qty": opp.buy_depth_target_qty,
                        "sell_depth_available_qty": opp.sell_depth_available_qty,
                        "buy_depth_available_qty": opp.buy_depth_available_qty,
                        "sell_depth_worst_px": opp.sell_depth_worst_px,
                        "buy_depth_worst_px": opp.buy_depth_worst_px,
                        "sell_depth_levels_used": opp.sell_depth_levels_used,
                        "buy_depth_levels_used": opp.buy_depth_levels_used,
                        "sell_best_px": opp.sell_best_px,
                        "buy_best_px": opp.buy_best_px,
                        "sell_best_qty": opp.sell_best_qty,
                        "buy_best_qty": opp.buy_best_qty,
                    },
                    "aster_bound": aster_bound,
                    "lighter_bound": lighter_bound,
                    "aster_submit": format!("{a_res:?}"),
                    "lighter_submit": format!("{l_res:?}"),
                    "one_sided_accept": aster_accepted ^ lighter_accepted,
                    "aster_position_after": pos_after_ok.map(|p| p.aster_qty),
                    "lighter_position_rest_after": pos_after_ok.map(|p| p.lighter_qty),
                    "lighter_position_ws_after": lighter_ws_qty,
                    "aster_open_orders_after": aster_open.as_ref().ok().map(|orders| orders.len()),
                    "lighter_open_orders_rest_after": lighter_open.as_ref().ok().copied(),
                    "error": format!("one or both legs not accepted: Aster={a_res:?} Lighter={l_res:?}"),
                }),
            );
            let aster_unknown = matches!(&a_res, AsterOutcome::Unknown { .. });
            let lighter_unknown = matches!(&l_res, LighterOutcome::Unknown { .. });
            if !aster_accepted && !lighter_accepted {
                if aster_unknown || lighter_unknown {
                    // An Unknown outcome (HTTP timeout, ws response_timeout /
                    // disconnected_after_send) means the order may actually be LIVE and
                    // FILLED at the venue. That is not a clean skip: classify as
                    // Unreconciled so needs_recovery() runs the post-trade reconcile +
                    // rescue path, which verifies real positions and flattens any
                    // residual leg instead of cooling down on top of a naked position.
                    return Err(ExecutionError::Unreconciled {
                        details: format!(
                            "no leg accepted but outcome(s) unknown (order may exist at venue): Aster={a_res:?} Lighter={l_res:?}"
                        ),
                    });
                }
                return Err(ExecutionError::Skipped {
                    details: format!(
                        "neither leg accepted: Aster={a_res:?} Lighter={l_res:?}"
                    ),
                });
            }
            return Err(ExecutionError::LegRejected {
                one_sided: aster_accepted ^ lighter_accepted,
                details: format!(
                    "one or both legs not accepted: Aster={a_res:?} Lighter={l_res:?}"
                ),
            });
        }
    }

    let (aster_order_id, aster_immediate_fill, aster_immediate_error) = match &a_res {
        AsterOutcome::Accepted {
            venue_order_id: Some(order_id),
            raw,
        } => match immediate_fill_from_order_response(raw) {
            Ok(fill) => (*order_id, fill, None),
            Err(e) => (
                *order_id,
                AsterImmediateFill {
                    qty: Decimal::ZERO,
                    vwap: Decimal::ZERO,
                    notional: Decimal::ZERO,
                },
                Some(format!("{e:#}")),
            ),
        },
        other => {
            return Err(ExecutionError::AccountingUnavailable {
                details: format!("Aster accepted response missing orderId: {other:?}"),
            });
        }
    };
    let aster_initial_zero_fill = aster_immediate_fill.qty <= Decimal::ZERO;
    let lighter_client_order_index = match &l_res {
        LighterOutcome::Accepted {
            client_order_index, ..
        } => *client_order_index,
        other => {
            return Err(ExecutionError::AccountingUnavailable {
                details: format!("Lighter accepted response missing client id: {other:?}"),
            });
        }
    };
    let Some(lighter_pending_fill) = lighter_pending_fill else {
        append_execution_log(
            cfg,
            spec,
            serde_json::json!({
                "timestamp": Utc::now(),
                "started_at": started_at,
                "execution_id": execution_id,
                "market": spec.market_id.to_string(),
                "execution_mode": "concurrent_confirm_rescue",
                "outcome": "accounting_unavailable",
                "direction": opp.direction.as_str(),
                "qty": opp.qty,
                "reduce_only": reduce_only,
                "aster_submit": format!("{a_res:?}"),
                "lighter_submit": format!("{l_res:?}"),
                "aster_order_id": aster_order_id,
                "lighter_client_order_index": lighter_client_order_index,
                "error": format!("Lighter accepted response missing deferred fill waiter: {l_res:?}"),
            }),
        );
        return Err(ExecutionError::AccountingUnavailable {
            details: format!("Lighter accepted response missing deferred fill waiter: {l_res:?}"),
        });
    };

    let reconcile_fut = wait_post_trade_reconciled(cfg, spec, aster, lighter, opp);
    let aster_fill_fut = aster.wait_order_fill_summary(
        &spec.market_id,
        aster_order_id,
        opp.qty,
        Duration::from_secs(10),
    );
    let lighter_fill_fut = lighter_pending_fill.wait_confirmed(Duration::from_secs(10));
    let (mut reconciled, aster_fill, lighter_confirmation) =
        tokio::join!(reconcile_fut, aster_fill_fut, lighter_fill_fut);

    let mut aster_fill_ok = None;
    let mut aster_fill_error = None;
    let mut aster_fill_note = None;
    match aster_fill {
        Ok(fill) => {
            aster_fill_ok = Some(fill);
        }
        Err(e) if aster_immediate_fill.qty > Decimal::ZERO
            && aster_immediate_fill.notional > Decimal::ZERO =>
        {
            let note = format!(
                "Aster userTrades unavailable for orderId={aster_order_id}; using immediate IOC response fill qty={} vwap={} notional=${}: {e:#}",
                aster_immediate_fill.qty,
                aster_immediate_fill.vwap,
                aster_immediate_fill.notional
            );
            warn!("{note}");
            aster_fill_note = Some(note);
            aster_fill_ok = Some(immediate_fill_summary(
                aster_immediate_fill,
                cfg.arb.aster_taker_fee_bps,
            ));
        }
        Err(e) if aster_initial_zero_fill => {
            let note = format!(
                "Aster IOC orderId={} reported zero initial fill and no userTrades before timeout; treating Aster leg as zero fill: {e:#}",
                aster_order_id
            );
            warn!(
                "Aster IOC orderId={} reported zero initial fill and no userTrades before timeout; treating Aster leg as zero fill: {e:#}",
                aster_order_id
            );
            aster_fill_note = Some(note);
            aster_fill_ok = Some(zero_fill_summary());
        }
        Err(e) => {
            aster_fill_error = Some(format!(
                "Aster fill accounting unavailable for orderId={aster_order_id}: {e:#}"
            ));
        }
    }
    let mut lighter_fill_ok = lighter_confirmation.fill;
    let mut hedge_retry_report = None;
    if reconciled.is_err() {
        let retry_start_position = reconcile_positions(&spec.market_id, aster, lighter).await.ok();
        if let Some(retry_start_position) = retry_start_position {
            let retry = try_missing_hedge_retry(
                cfg,
                spec,
                aster,
                lighter,
                opp,
                pre_position,
                retry_start_position,
                reduce_only,
            )
            .await;
            if retry.succeeded {
                if let (Some(position), Some(net_notional)) =
                    (retry.final_position, retry.net_notional)
                {
                    reconciled = Ok((position, net_notional));
                }
                for attempt in &retry.attempts {
                    match attempt.venue {
                        HedgeRetryVenue::Aster => {
                            aster_fill_ok = add_fill_summary(aster_fill_ok, attempt.fill);
                        }
                        HedgeRetryVenue::Lighter => {
                            lighter_fill_ok = add_fill_summary(lighter_fill_ok, attempt.fill);
                        }
                    }
                }
            }
            hedge_retry_report = Some(retry);
        } else {
            hedge_retry_report = Some(HedgeRetryReport::empty(
                cfg.arb.hedge_retry_slippage_bps,
                Some("could not reconcile positions before hedge retry".to_string()),
            ));
        }
    }
    let lighter_fill_error = lighter_fill_ok.is_none().then(|| {
        format!(
            "Lighter fill accounting unavailable for client_order_index={lighter_client_order_index} status={}",
            lighter_confirmation.status.as_str()
        )
    });
    let (reconciled_position, net_notional, reconcile_error) = match &reconciled {
        Ok((pos, net_notional)) => (Some(*pos), Some(*net_notional), None),
        Err(e) => (None, None, Some(format!("{e:#}"))),
    };
    let lighter_ws_qty = lighter.ws_position_qty(&spec.market_id).ok();
    let lighter_ws_rest_divergence_qty =
        reconciled_position.and_then(|pos| lighter_ws_qty.map(|ws| (ws - pos.lighter_qty).abs()));
    let (aster_open, lighter_open, margin_after_result) = tokio::join!(
        aster.open_orders(&spec.market_id),
        lighter.rest_open_orders_count(&spec.market_id),
        reconcile_margins(aster, lighter),
    );
    let (margin_after, margin_after_error) = match margin_after_result {
        Ok(margin) => (Some(margin), None),
        Err(e) => (
            None,
            Some(format!(
                "margin reconciliation unavailable after trade: {e:#}"
            )),
        ),
    };
    let mut economics_ok = None;
    let mut economics_error = None;
    if reconcile_error.is_none() {
        if let (Some(aster_fill), Some(lighter_fill)) = (aster_fill_ok, lighter_fill_ok) {
            match actual_economics(cfg, opp, aster_fill, lighter_fill) {
                Ok(economics) => economics_ok = Some(economics),
                Err(e) => economics_error = Some(format!("{e:#}")),
            }
        }
    }
    let margin_before_json = serde_json::json!({
        "aster_available_usd": margin_before.aster_available_usd,
        "lighter_available_usd": margin_before.lighter_available_usd,
        "total_available_usd": margin_before.aster_available_usd + margin_before.lighter_available_usd,
    });
    let margin_after_json = margin_after.map(|m| {
        serde_json::json!({
            "aster_available_usd": m.aster_available_usd,
            "lighter_available_usd": m.lighter_available_usd,
            "total_available_usd": m.aster_available_usd + m.lighter_available_usd,
        })
    });
    let actual_economics_json = economics_ok.map(|e| {
        serde_json::json!({
            "gross_usd": e.gross_usd,
            "fees_usd": e.fees_usd,
            "net_usd": e.net_usd,
            "net_bps": e.net_bps,
            "fill_qty_mismatch": e.fill_qty_mismatch,
            "residual_qty": e.residual_qty,
            "residual_notional_usd": e.residual_notional_usd,
        })
    });
    let final_positions_json = reconciled_position.map(|p| {
        serde_json::json!({
            "aster_qty": p.aster_qty,
            "lighter_rest_qty": p.lighter_qty,
            "lighter_ws_qty": lighter_ws_qty,
            "lighter_ws_rest_divergence_qty": lighter_ws_rest_divergence_qty,
            "net_qty": p.net_qty(),
            "net_notional_usd": net_notional,
        })
    });
    let open_orders_after_json = serde_json::json!({
        "aster": aster_open.as_ref().ok().map(|orders| orders.len()),
        "lighter_rest": lighter_open.as_ref().ok().copied(),
        "aster_error": aster_open.as_ref().err().map(|e| format!("{e:#}")),
        "lighter_error": lighter_open.as_ref().err().map(|e| format!("{e:#}")),
    });
    let open_orders_error = match (&aster_open, &lighter_open) {
        (Ok(a_orders), Ok(l_orders)) if a_orders.is_empty() && *l_orders == 0 => None,
        (Ok(a_orders), Ok(l_orders)) => Some(format!(
            "open orders remain after execution: aster={} lighter={}",
            a_orders.len(),
            l_orders
        )),
        (a_res, l_res) => Some(format!(
            "open-order check unavailable after execution: aster={:?} lighter={:?}",
            a_res.as_ref().map(|orders| orders.len()),
            l_res
        )),
    };
    let aster_immediate_fill_json = serde_json::json!({
        "qty": aster_immediate_fill.qty,
        "vwap": aster_immediate_fill.vwap,
        "notional": aster_immediate_fill.notional,
        "error": aster_immediate_error,
    });
    let final_error = reconcile_error
        .clone()
        .or_else(|| aster_fill_error.clone())
        .or_else(|| lighter_fill_error.clone())
        .or_else(|| economics_error.clone())
        .or_else(|| open_orders_error.clone())
        .or_else(|| margin_after_error.clone());
    let outcome = if final_error.is_some() {
        "failed"
    } else {
        "success"
    };
    let mut execution_row = serde_json::Map::new();
    insert_json(&mut execution_row, "timestamp", Utc::now());
    insert_json(&mut execution_row, "started_at", started_at);
    insert_json(&mut execution_row, "execution_id", execution_id.clone());
    insert_json(&mut execution_row, "market", spec.market_id.to_string());
    insert_json(
        &mut execution_row,
        "execution_mode",
        "concurrent_confirm_rescue",
    );
    insert_json(&mut execution_row, "outcome", outcome);
    insert_json(
        &mut execution_row,
        "confirmation_policy",
        "both_legs_confirmed_or_reduce_only_rescue",
    );
    insert_json(&mut execution_row, "direction", opp.direction.as_str());
    insert_json(&mut execution_row, "qty", opp.qty);
    insert_json(&mut execution_row, "reduce_only", reduce_only);
    insert_json(&mut execution_row, "gross_edge_bps", opp.gross_edge_bps);
    insert_json(
        &mut execution_row,
        "expected_net_margin_bps",
        opp.expected_net_margin_bps,
    );
    insert_json(&mut execution_row, "expected_net_usd", opp.expected_net_usd);
    insert_json(&mut execution_row, "required_margin_usd", opp.required_margin_usd);
    insert_json(&mut execution_row, "sell_px", opp.sell_px);
    insert_json(&mut execution_row, "buy_px", opp.buy_px);
    insert_json(&mut execution_row, "ref_px", opp.ref_px);
    insert_json(&mut execution_row, "top_depth_qty", opp.top_depth_qty);
    insert_json(
        &mut execution_row,
        "depth_guard",
        serde_json::json!({
            "enabled": opp.depth_guard_enabled,
            "liquidity_multiple": opp.liquidity_multiple,
            "depth_supported_qty": opp.depth_supported_qty,
            "sell_depth_target_qty": opp.sell_depth_target_qty,
            "buy_depth_target_qty": opp.buy_depth_target_qty,
            "sell_depth_available_qty": opp.sell_depth_available_qty,
            "buy_depth_available_qty": opp.buy_depth_available_qty,
            "sell_depth_worst_px": opp.sell_depth_worst_px,
            "buy_depth_worst_px": opp.buy_depth_worst_px,
            "sell_depth_levels_used": opp.sell_depth_levels_used,
            "buy_depth_levels_used": opp.buy_depth_levels_used,
            "sell_best_px": opp.sell_best_px,
            "buy_best_px": opp.buy_best_px,
            "sell_best_qty": opp.sell_best_qty,
            "buy_best_qty": opp.buy_best_qty,
        }),
    );
    insert_json(&mut execution_row, "aster_side", aster_side.as_str());
    insert_json(&mut execution_row, "lighter_side", lighter_side.as_str());
    insert_json(&mut execution_row, "aster_bound", aster_bound);
    insert_json(&mut execution_row, "lighter_bound", lighter_bound);
    insert_json(
        &mut execution_row,
        "slippage_bps",
        serde_json::json!({
            "aster_entry": cfg.arb.max_aster_slippage_bps,
            "lighter_entry": cfg.arb.max_lighter_slippage_bps,
            "hedge_retry": cfg.arb.hedge_retry_slippage_bps,
            "emergency": cfg.arb.emergency_slippage_bps,
        }),
    );
    insert_json(
        &mut execution_row,
        "pre_positions",
        serde_json::json!({
            "aster_qty": pre_position.aster_qty,
            "lighter_qty": pre_position.lighter_qty,
            "net_qty": pre_position.net_qty(),
        }),
    );
    insert_json(&mut execution_row, "margin_before", margin_before_json);
    insert_json(&mut execution_row, "margin_after", margin_after_json);
    insert_json(&mut execution_row, "aster_submit", format!("{a_res:?}"));
    insert_json(&mut execution_row, "lighter_submit", format!("{l_res:?}"));
    insert_json(&mut execution_row, "aster_order_id", aster_order_id);
    insert_json(
        &mut execution_row,
        "aster_immediate_fill",
        aster_immediate_fill_json,
    );
    insert_json(
        &mut execution_row,
        "lighter_client_order_index",
        lighter_client_order_index,
    );
    insert_json(&mut execution_row, "aster_fill", aster_fill_ok);
    insert_json(&mut execution_row, "lighter_fill", lighter_fill_ok);
    insert_json(
        &mut execution_row,
        "lighter_fill_confirmation",
        serde_json::json!({
            "status": lighter_confirmation.status.as_str(),
            "filled_qty": lighter_confirmation.filled_qty,
            "matched_trades_seen": lighter_confirmation.matched_trades_seen,
            "terminal_order": lighter_confirmation.terminal_order.as_ref().map(|order| format!("{order:?}")),
        }),
    );
    insert_json(
        &mut execution_row,
        "hedge_retry",
        hedge_retry_report_json(hedge_retry_report.as_ref()),
    );
    insert_json(
        &mut execution_row,
        "aster_fill_error",
        aster_fill_error.clone(),
    );
    insert_json(&mut execution_row, "aster_fill_note", aster_fill_note);
    insert_json(
        &mut execution_row,
        "lighter_fill_error",
        lighter_fill_error.clone(),
    );
    insert_json(&mut execution_row, "reconcile_error", reconcile_error);
    insert_json(
        &mut execution_row,
        "economics_error",
        economics_error.clone(),
    );
    insert_json(
        &mut execution_row,
        "open_orders_error",
        open_orders_error.clone(),
    );
    insert_json(&mut execution_row, "margin_after_error", margin_after_error);
    insert_json(&mut execution_row, "actual_economics", actual_economics_json);
    insert_json(&mut execution_row, "final_positions", final_positions_json);
    insert_json(&mut execution_row, "open_orders_after", open_orders_after_json);
    insert_json(&mut execution_row, "error", final_error.clone());
    append_execution_log(
        cfg,
        spec,
        serde_json::Value::Object(execution_row),
    );

    let (pos, net_notional) = reconciled?;
    let _aster_fill = aster_fill_ok.ok_or_else(|| ExecutionError::AccountingUnavailable {
        details: aster_fill_error.unwrap_or_else(|| {
            format!("Aster fill accounting unavailable for orderId={aster_order_id}")
        }),
    })?;
    let _lighter_fill = lighter_fill_ok.ok_or_else(|| ExecutionError::AccountingUnavailable {
        details: lighter_fill_error.unwrap_or_else(|| {
            format!(
                "Lighter fill accounting unavailable for client_order_index={lighter_client_order_index}"
            )
        }),
    })?;
    let economics = economics_ok.ok_or_else(|| ExecutionError::AccountingUnavailable {
        details: economics_error
            .unwrap_or_else(|| "actual economics unavailable after fill accounting".to_string()),
    })?;
    if let Some(open_orders_error) = open_orders_error {
        return Err(ExecutionError::AccountingUnavailable {
            details: open_orders_error,
        });
    }
    let margin_after = margin_after.ok_or_else(|| ExecutionError::AccountingUnavailable {
        details: "margin reconciliation unavailable after trade".to_string(),
    })?;
    info!(
        "post-trade reconciled execution_id={execution_id}: aster={} lighter_rest={} lighter_ws={:?} net={} (${net_notional}) available_after=${} available_margin_delta=${} actual_gross=${} actual_fees=${} actual_net=${} actual_net_bps={} aster_vwap={} lighter_vwap={}",
        pos.aster_qty,
        pos.lighter_qty,
        lighter_ws_qty,
        pos.net_qty(),
        margin_after.aster_available_usd + margin_after.lighter_available_usd,
        (margin_after.aster_available_usd + margin_after.lighter_available_usd)
            - (margin_before.aster_available_usd + margin_before.lighter_available_usd),
        economics.gross_usd,
        economics.fees_usd,
        economics.net_usd,
        economics.net_bps,
        economics.aster_fill.vwap,
        economics.lighter_fill.vwap
    );
    Ok(TradeReport {
        position: pos,
        lighter_ws_qty,
        lighter_ws_rest_divergence_qty,
        margin_before,
        margin_after,
        economics,
        aster_order_id,
        lighter_client_order_index,
        hedge_retry_action_taken: hedge_retry_report
            .as_ref()
            .is_some_and(|report| report.attempted),
    })
}

fn zero_fill_summary() -> FillSummary {
    FillSummary {
        qty: Decimal::ZERO,
        vwap: Decimal::ZERO,
        notional: Decimal::ZERO,
        fee_usd: Decimal::ZERO,
    }
}

fn immediate_fill_summary(fill: AsterImmediateFill, fee_bps: Decimal) -> FillSummary {
    FillSummary {
        qty: fill.qty,
        vwap: fill.vwap,
        notional: fill.notional,
        fee_usd: fill.notional * bps_to_rate(fee_bps),
    }
}

/// Ledger row for a recovery/auto-flatten that took action: books the estimated realized
/// loss into cumulative PnL so the loss breaker cannot develop blind spots (previously
/// recovery losses only fed the coarse hourly recovered-loss limiter, never the ledger).
fn recovery_loss_row(spec: &MarketSpec, recovery: &RecoveryReport) -> TradeLedgerRow {
    let loss = recovery.estimated_loss_usdc;
    TradeLedgerRow {
        timestamp: Utc::now(),
        market: spec.market_id.0.clone(),
        direction: "RECOVERY".to_string(),
        qty: Decimal::ZERO,
        expected_net_usd: Decimal::ZERO,
        actual_gross_usd: -loss,
        actual_fees_usd: Decimal::ZERO,
        actual_net_usd: -loss,
        actual_net_bps: Decimal::ZERO,
        fill_qty_mismatch: Decimal::ZERO,
        aster_fill: zero_fill_summary(),
        lighter_fill: zero_fill_summary(),
        aster_order_id: 0,
        lighter_client_order_index: 0,
        final_aster_position: recovery.position.aster_qty,
        final_lighter_position: recovery.position.lighter_qty,
        final_net_position: recovery.position.net_qty(),
        available_before_usd: recovery.margin_after.aster_available_usd
            + recovery.margin_after.lighter_available_usd
            + loss,
        available_after_usd: recovery.margin_after.aster_available_usd
            + recovery.margin_after.lighter_available_usd,
        aster_available_before_usd: recovery.margin_after.aster_available_usd,
        aster_available_after_usd: recovery.margin_after.aster_available_usd,
        lighter_available_before_usd: recovery.margin_after.lighter_available_usd,
        lighter_available_after_usd: recovery.margin_after.lighter_available_usd,
    }
}

/// Record a recovery-loss ledger row (if the PnL tracker is enabled) and enforce the
/// cumulative-loss breaker, mirroring the normal trade path.
fn record_recovery_loss(
    pnl: &mut Option<PnlTracker>,
    spec: &MarketSpec,
    recovery: &RecoveryReport,
) -> Result<()> {
    if recovery.estimated_loss_usdc <= Decimal::ZERO {
        return Ok(());
    }
    let Some(pnl) = pnl.as_mut() else {
        return Ok(());
    };
    let update = pnl.record_trade(recovery_loss_row(spec, recovery))?;
    warn!(
        "recovery loss booked to pnl ledger: market={} loss=${} cumulative_pnl=${}",
        spec.market_id, recovery.estimated_loss_usdc, update.cumulative_pnl_usdc
    );
    if let Some(breaker) = update.breaker {
        bail!(
            "circuit breaker triggered by recovery loss: cumulative PnL ${} <= -${}; manual reset required",
            breaker.cumulative_pnl_usdc,
            breaker.max_loss_usdc
        );
    }
    Ok(())
}

fn pnl_trade_row(spec: &MarketSpec, opp: &Opportunity, report: &TradeReport) -> TradeLedgerRow {
    TradeLedgerRow {
        timestamp: Utc::now(),
        market: spec.market_id.0.clone(),
        direction: opp.direction.as_str().to_string(),
        qty: report
            .economics
            .aster_fill
            .qty
            .min(report.economics.lighter_fill.qty),
        expected_net_usd: opp.expected_net_usd,
        actual_gross_usd: report.economics.gross_usd,
        actual_fees_usd: report.economics.fees_usd,
        actual_net_usd: report.economics.net_usd,
        actual_net_bps: report.economics.net_bps,
        fill_qty_mismatch: report.economics.fill_qty_mismatch,
        aster_fill: report.economics.aster_fill,
        lighter_fill: report.economics.lighter_fill,
        aster_order_id: report.aster_order_id,
        lighter_client_order_index: report.lighter_client_order_index,
        final_aster_position: report.position.aster_qty,
        final_lighter_position: report.position.lighter_qty,
        final_net_position: report.position.net_qty(),
        available_before_usd: report.margin_before.aster_available_usd
            + report.margin_before.lighter_available_usd,
        available_after_usd: report.margin_after.aster_available_usd
            + report.margin_after.lighter_available_usd,
        aster_available_before_usd: report.margin_before.aster_available_usd,
        aster_available_after_usd: report.margin_after.aster_available_usd,
        lighter_available_before_usd: report.margin_before.lighter_available_usd,
        lighter_available_after_usd: report.margin_after.lighter_available_usd,
    }
}

fn actual_economics(
    cfg: &Config,
    opp: &Opportunity,
    aster_fill: FillSummary,
    lighter_fill: FillSummary,
) -> std::result::Result<ActualEconomics, ExecutionError> {
    let fill_qty_mismatch = (aster_fill.qty - lighter_fill.qty).abs();
    if fill_qty_mismatch * opp.ref_px > cfg.risk.max_position_mismatch_usd {
        return Err(ExecutionError::AccountingUnavailable {
            details: format!(
                "fill accounting qty mismatch too large: aster_qty={} lighter_qty={} mismatch={} notional=${}",
                aster_fill.qty,
                lighter_fill.qty,
                fill_qty_mismatch,
                fill_qty_mismatch * opp.ref_px
            ),
        });
    }
    let (sell_fill, buy_fill) = match opp.direction {
        Direction::SellAsterBuyLighter => (aster_fill, lighter_fill),
        Direction::SellLighterBuyAster => (lighter_fill, aster_fill),
    };
    // PnL is realized only over the MATCHED quantity at the two VWAPs. With unequal legs
    // (tolerated up to max_position_mismatch_usd) the naive `sell.notional − buy.notional`
    // books the unhedged residual as pure profit/loss — masking the loss breaker with
    // fiction and later mirrored by the residual close. The residual is reported as open
    // exposure instead; recovery losses book via `estimated_loss_usdc`.
    let matched_qty = sell_fill.qty.min(buy_fill.qty);
    let gross_usd = matched_qty * (sell_fill.vwap - buy_fill.vwap);
    let residual_qty = sell_fill.qty - buy_fill.qty;
    let residual_notional_usd = residual_qty.abs() * opp.ref_px;
    let fees_usd = aster_fill.fee_usd + lighter_fill.fee_usd;
    let net_usd = gross_usd - fees_usd;
    let denom = (aster_fill.notional + lighter_fill.notional) / Decimal::from(2u32);
    let net_bps = if denom > Decimal::ZERO {
        net_usd / denom * Decimal::from(10_000u32)
    } else {
        Decimal::ZERO
    };
    Ok(ActualEconomics {
        aster_fill,
        lighter_fill,
        gross_usd,
        fees_usd,
        net_usd,
        net_bps,
        fill_qty_mismatch,
        residual_qty,
        residual_notional_usd,
    })
}

fn add_fill_summary(base: Option<FillSummary>, extra: Option<FillSummary>) -> Option<FillSummary> {
    match (base, extra) {
        (Some(a), Some(b)) => {
            let qty = a.qty + b.qty;
            let notional = a.notional + b.notional;
            if qty <= Decimal::ZERO || notional <= Decimal::ZERO {
                None
            } else {
                Some(FillSummary {
                    qty,
                    vwap: notional / qty,
                    notional,
                    fee_usd: a.fee_usd + b.fee_usd,
                })
            }
        }
        (Some(fill), None) | (None, Some(fill)) => Some(fill),
        (None, None) => None,
    }
}

fn expected_post_position(
    pre_position: PositionSnapshot,
    direction: Direction,
    qty: Decimal,
) -> PositionSnapshot {
    let aster_sign = if matches!(direction.aster_side(), Side::Buy) {
        Decimal::ONE
    } else {
        -Decimal::ONE
    };
    PositionSnapshot {
        aster_qty: pre_position.aster_qty + aster_sign * qty,
        lighter_qty: pre_position.lighter_qty - aster_sign * qty,
    }
}

fn retry_price_bound(opp: &Opportunity, side: Side, slippage_bps: Decimal) -> Decimal {
    let rate = bps_to_rate(slippage_bps);
    match side {
        Side::Buy => opp.buy_px * (Decimal::ONE + rate),
        Side::Sell => opp.sell_px * (Decimal::ONE - rate),
    }
}

fn hedge_retry_plan(
    cfg: &Config,
    spec: &MarketSpec,
    opp: &Opportunity,
    pre_position: PositionSnapshot,
    current_position: PositionSnapshot,
) -> Option<HedgeRetryPlan> {
    let expected = expected_post_position(pre_position, opp.direction, opp.qty);
    let aster_missing = expected.aster_qty - current_position.aster_qty;
    let lighter_missing = expected.lighter_qty - current_position.lighter_qty;
    let aster_missing_notional = aster_missing.abs() * opp.ref_px;
    let lighter_missing_notional = lighter_missing.abs() * opp.ref_px;
    let aster_needs_retry = aster_missing_notional > cfg.risk.max_position_mismatch_usd;
    let lighter_needs_retry = lighter_missing_notional > cfg.risk.max_position_mismatch_usd;
    if aster_needs_retry == lighter_needs_retry {
        return None;
    }
    let (venue, missing_qty, step, min_qty, min_notional) = if aster_needs_retry {
        (
            HedgeRetryVenue::Aster,
            aster_missing,
            spec.step,
            spec.aster_min_qty,
            spec.aster_min_notional,
        )
    } else {
        (
            HedgeRetryVenue::Lighter,
            lighter_missing,
            spec.lighter_qty_step,
            spec.lighter_qty_step,
            spec.lighter_min_notional,
        )
    };
    let qty = floor_to_step(missing_qty.abs(), step);
    if qty <= Decimal::ZERO
        || qty < min_qty
        || qty * opp.ref_px < min_notional
        || qty * opp.ref_px <= cfg.risk.max_position_mismatch_usd
    {
        return None;
    }
    let side = if missing_qty > Decimal::ZERO {
        Side::Buy
    } else {
        Side::Sell
    };
    Some(HedgeRetryPlan {
        venue,
        side,
        qty,
        price_bound: retry_price_bound(opp, side, cfg.arb.hedge_retry_slippage_bps),
    })
}

async fn submit_aster_hedge_retry(
    cfg: &Config,
    spec: &MarketSpec,
    aster: &AsterRest,
    plan: HedgeRetryPlan,
    reduce_only: bool,
    timeout: Duration,
) -> (String, Option<FillSummary>, Option<String>, Option<String>) {
    let submit = aster
        .submit_ioc_order(
            &spec.market_id,
            plan.side,
            plan.qty,
            plan.price_bound,
            reduce_only,
        )
        .await;
    let submit_result = format!("{submit:?}");
    match submit {
        AsterOutcome::Accepted {
            venue_order_id: Some(order_id),
            raw,
        } => {
            let immediate = immediate_fill_from_order_response(&raw).ok();
            match aster
                .wait_order_fill_summary(&spec.market_id, order_id, plan.qty, timeout)
                .await
            {
                Ok(fill) => (
                    submit_result,
                    Some(fill),
                    Some("filled".to_string()),
                    None,
                ),
                Err(e) => {
                    if let Some(immediate) = immediate {
                        if immediate.qty > Decimal::ZERO && immediate.notional > Decimal::ZERO {
                            return (
                                submit_result,
                                Some(immediate_fill_summary(
                                    immediate,
                                    cfg.arb.aster_taker_fee_bps,
                                )),
                                Some("immediate_fill_fallback".to_string()),
                                None,
                            );
                        }
                    }
                    (
                        submit_result,
                        None,
                        Some("accounting_unavailable".to_string()),
                        Some(format!("{e:#}")),
                    )
                }
            }
        }
        other => (
            submit_result,
            None,
            Some("not_accepted".to_string()),
            Some(format!("Aster hedge retry not accepted: {other:?}")),
        ),
    }
}

async fn submit_lighter_hedge_retry(
    spec: &MarketSpec,
    lighter: &LighterVenue,
    plan: HedgeRetryPlan,
    reduce_only: bool,
    timeout: Duration,
) -> (String, Option<FillSummary>, Option<String>, Option<String>) {
    let (submit, pending_fill) = lighter
        .submit_market_order_deferred_fill(
            &spec.market_id,
            plan.side,
            plan.qty,
            plan.price_bound,
            reduce_only,
        )
        .await;
    let submit_result = format!("{submit:?}");
    match (submit, pending_fill) {
        (LighterOutcome::Accepted { .. }, Some(pending_fill)) => {
            let confirmation: LighterFillConfirmation = pending_fill.wait_confirmed(timeout).await;
            let status = confirmation.status.as_str().to_string();
            let error = confirmation.fill.is_none().then(|| {
                format!(
                    "Lighter hedge retry fill unavailable status={} filled_qty={} matched_trades_seen={}",
                    status, confirmation.filled_qty, confirmation.matched_trades_seen
                )
            });
            (submit_result, confirmation.fill, Some(status), error)
        }
        (other, _) => (
            submit_result,
            None,
            Some("not_accepted".to_string()),
            Some(format!("Lighter hedge retry not accepted: {other:?}")),
        ),
    }
}

async fn try_missing_hedge_retry(
    cfg: &Config,
    spec: &MarketSpec,
    aster: &AsterRest,
    lighter: &LighterVenue,
    opp: &Opportunity,
    pre_position: PositionSnapshot,
    mut current_position: PositionSnapshot,
    reduce_only: bool,
) -> HedgeRetryReport {
    let timeout = Duration::from_millis(cfg.arb.hedge_retry_timeout_ms);
    let mut report = HedgeRetryReport::empty(cfg.arb.hedge_retry_slippage_bps, None);
    for attempt_no in 1..=cfg.arb.max_hedge_retry_attempts {
        let Some(plan) = hedge_retry_plan(cfg, spec, opp, pre_position, current_position) else {
            report.error = Some(format!(
                "no single missing hedge retry plan for current positions aster={} lighter={} expected_aster={} expected_lighter={}",
                current_position.aster_qty,
                current_position.lighter_qty,
                expected_post_position(pre_position, opp.direction, opp.qty).aster_qty,
                expected_post_position(pre_position, opp.direction, opp.qty).lighter_qty
            ));
            return report;
        };
        report.attempted = true;
        warn!(
            "missing hedge retry attempt={} venue={} side={} qty={} bound={} reduce_only={} slippage_bps={}",
            attempt_no,
            plan.venue.as_str(),
            plan.side,
            plan.qty,
            plan.price_bound,
            reduce_only,
            cfg.arb.hedge_retry_slippage_bps
        );
        let (submit_result, fill, fill_status, submit_error) = match plan.venue {
            HedgeRetryVenue::Aster => {
                submit_aster_hedge_retry(cfg, spec, aster, plan, reduce_only, timeout).await
            }
            HedgeRetryVenue::Lighter => {
                submit_lighter_hedge_retry(spec, lighter, plan, reduce_only, timeout).await
            }
        };
        report.attempts.push(HedgeRetryAttempt {
            attempt: attempt_no,
            venue: plan.venue,
            side: plan.side,
            qty: plan.qty,
            price_bound: plan.price_bound,
            reduce_only,
            submit_result: Some(submit_result),
            fill,
            fill_status,
            error: submit_error.clone(),
        });
        let (reconciled, open_a, open_l) = tokio::join!(
            wait_post_trade_reconciled_for(cfg, spec, aster, lighter, opp, timeout),
            aster.open_orders(&spec.market_id),
            lighter.rest_open_orders_count(&spec.market_id),
        );
        match (reconciled, open_a, open_l) {
            (Ok((position, net_notional)), Ok(aster_orders), Ok(lighter_orders))
                if aster_orders.is_empty() && lighter_orders == 0 =>
            {
                report.succeeded = true;
                report.final_position = Some(position);
                report.net_notional = Some(net_notional);
                report.aster_open_orders = Some(aster_orders.len());
                report.lighter_open_orders = Some(lighter_orders);
                warn!(
                    "missing hedge retry succeeded attempt={} venue={} final_aster={} final_lighter={} net_notional=${}",
                    attempt_no,
                    plan.venue.as_str(),
                    position.aster_qty,
                    position.lighter_qty,
                    net_notional
                );
                return report;
            }
            (Ok((position, net_notional)), Ok(aster_orders), Ok(lighter_orders)) => {
                current_position = position;
                report.final_position = Some(position);
                report.net_notional = Some(net_notional);
                report.aster_open_orders = Some(aster_orders.len());
                report.lighter_open_orders = Some(lighter_orders);
                report.error = Some(format!(
                    "hedge retry left open orders: aster={} lighter={}",
                    aster_orders.len(),
                    lighter_orders
                ));
            }
            (reconciled, open_a, open_l) => {
                if let Ok(position) = reconcile_positions(&spec.market_id, aster, lighter).await {
                    current_position = position;
                    report.final_position = Some(position);
                }
                report.error = Some(format!(
                    "hedge retry verification failed: reconciled={:?} aster_open={:?} lighter_open={:?}",
                    reconciled.as_ref().map(|(_, notional)| *notional),
                    open_a.as_ref().map(|orders| orders.len()),
                    open_l
                ));
            }
        }
    }
    report
}

fn hedge_retry_report_json(report: Option<&HedgeRetryReport>) -> serde_json::Value {
    let Some(report) = report else {
        return serde_json::Value::Null;
    };
    let attempts: Vec<_> = report
        .attempts
        .iter()
        .map(|attempt| {
            serde_json::json!({
                "attempt": attempt.attempt,
                "venue": attempt.venue.as_str(),
                "side": attempt.side.as_str(),
                "qty": attempt.qty,
                "price_bound": attempt.price_bound,
                "reduce_only": attempt.reduce_only,
                "submit_result": attempt.submit_result,
                "fill": attempt.fill,
                "fill_status": attempt.fill_status,
                "error": attempt.error,
            })
        })
        .collect();
    serde_json::json!({
        "attempted": report.attempted,
        "succeeded": report.succeeded,
        "slippage_bps": report.slippage_bps,
        "attempts": attempts,
        "final_position": report.final_position.map(|position| serde_json::json!({
            "aster_qty": position.aster_qty,
            "lighter_qty": position.lighter_qty,
            "net_qty": position.net_qty(),
        })),
        "net_notional": report.net_notional,
        "aster_open_orders": report.aster_open_orders,
        "lighter_open_orders": report.lighter_open_orders,
        "error": report.error,
    })
}

async fn wait_post_trade_reconciled(
    cfg: &Config,
    spec: &MarketSpec,
    aster: &AsterRest,
    lighter: &LighterVenue,
    opp: &Opportunity,
) -> std::result::Result<(PositionSnapshot, Decimal), ExecutionError> {
    wait_post_trade_reconciled_for(cfg, spec, aster, lighter, opp, Duration::from_secs(10)).await
}

async fn wait_post_trade_reconciled_for(
    cfg: &Config,
    spec: &MarketSpec,
    aster: &AsterRest,
    lighter: &LighterVenue,
    opp: &Opportunity,
    timeout: Duration,
) -> std::result::Result<(PositionSnapshot, Decimal), ExecutionError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let poll = Duration::from_millis(cfg.risk.min_reconcile_interval_ms.max(250));
    loop {
        tokio::time::sleep(poll).await;
        // A transient position-query failure must NOT abort as a generic error (which is
        // excluded from recovery): retry within the deadline, and if the venue still can't
        // be read, classify as Unreconciled so the rescue path runs — the trade may have
        // filled and the positions are simply unverified.
        let pos = match reconcile_positions(&spec.market_id, aster, lighter).await {
            Ok(pos) => pos,
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(ExecutionError::Unreconciled {
                        details: format!(
                            "post-trade position query kept failing (positions unverified): {e:#}"
                        ),
                    });
                }
                warn!("post-trade position query failed; retrying: {e:#}");
                continue;
            }
        };
        let net_notional = pos.net_qty().abs() * opp.ref_px;
        if net_notional <= cfg.risk.max_position_mismatch_usd {
            return Ok((pos, net_notional));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ExecutionError::Unreconciled {
                details: format!(
                    "post-trade residual too large after wait: aster={} lighter={} net_notional=${}",
                    pos.aster_qty, pos.lighter_qty, net_notional
                ),
            });
        }
    }
}

async fn recover_if_needed(
    cfg: &Config,
    spec: &MarketSpec,
    aster: &AsterRest,
    lighter: &LighterVenue,
    http: &reqwest::Client,
    margin_before: MarginSnapshot,
) -> Result<RecoveryReport> {
    tokio::time::sleep(Duration::from_millis(cfg.risk.min_reconcile_interval_ms)).await;
    let pos = reconcile_positions(&spec.market_id, aster, lighter).await?;
    let (aster_book, lighter_book) = fetch_books_rest_lighter(cfg, spec, http, lighter).await?;
    let mark = aster_book
        .mid()
        .or_else(|| lighter_book.mid())
        .unwrap_or(Decimal::ZERO);
    if mark <= Decimal::ZERO {
        bail!("residual recovery mark unavailable");
    }
    let net = pos.net_qty();
    let net_notional = net.abs() * mark;
    let (open_a, open_l, margin_after) = tokio::join!(
        aster.open_orders(&spec.market_id),
        lighter.rest_open_orders_count(&spec.market_id),
        reconcile_margins(aster, lighter),
    );
    if net_notional <= cfg.risk.max_position_mismatch_usd {
        let open_a = open_a?;
        let open_l = open_l?;
        let margin_after = margin_after?;
        if !open_a.is_empty() || open_l > 0 {
            bail!(
                "rescue check found open orders despite balanced positions: aster={} lighter={}",
                open_a.len(),
                open_l
            );
        }
        let estimated_loss_usdc = (margin_before.aster_available_usd
            + margin_before.lighter_available_usd
            - margin_after.aster_available_usd
            - margin_after.lighter_available_usd)
            .max(Decimal::ZERO);
        return Ok(RecoveryReport {
            action_taken: false,
            position: pos,
            lighter_ws_qty: lighter.ws_position_qty(&spec.market_id).ok(),
            margin_after,
            estimated_loss_usdc,
            aster_open_orders: open_a.len(),
            lighter_open_orders: open_l,
        });
    }
    warn!(
        "residual recovery: aster={} lighter={} net={} ${}",
        pos.aster_qty, pos.lighter_qty, net, net_notional
    );
    let mut action_taken = false;
    if pos.aster_qty.abs() * mark > cfg.risk.max_position_mismatch_usd {
        let side = if pos.aster_qty > Decimal::ZERO {
            Side::Sell
        } else {
            Side::Buy
        };
        let bound = if matches!(side, Side::Buy) {
            mark * (Decimal::ONE + bps_to_rate(cfg.arb.emergency_slippage_bps))
        } else {
            mark * (Decimal::ONE - bps_to_rate(cfg.arb.emergency_slippage_bps))
        };
        let res = aster
            .submit_ioc_order(&spec.market_id, side, pos.aster_qty.abs(), bound, true)
            .await;
        warn!("Aster reduce-only recovery result: {res:?}");
        action_taken = true;
    }
    if pos.lighter_qty.abs() * mark > cfg.risk.max_position_mismatch_usd {
        let side = if pos.lighter_qty > Decimal::ZERO {
            Side::Sell
        } else {
            Side::Buy
        };
        let bound = if matches!(side, Side::Buy) {
            mark * (Decimal::ONE + bps_to_rate(cfg.arb.emergency_slippage_bps))
        } else {
            mark * (Decimal::ONE - bps_to_rate(cfg.arb.emergency_slippage_bps))
        };
        let res = lighter
            .submit_market_order(&spec.market_id, side, pos.lighter_qty.abs(), bound, true)
            .await;
        warn!("Lighter reduce-only recovery result: {res:?}");
        action_taken = true;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        tokio::time::sleep(Duration::from_millis(
            cfg.risk.min_reconcile_interval_ms.max(250),
        ))
        .await;
        let (final_pos, open_a, open_l, margin_after) = tokio::join!(
            reconcile_positions(&spec.market_id, aster, lighter),
            aster.open_orders(&spec.market_id),
            lighter.rest_open_orders_count(&spec.market_id),
            reconcile_margins(aster, lighter),
        );
        let final_pos = final_pos?;
        let open_a = open_a?;
        let open_l = open_l?;
        let margin_after = margin_after?;
        let final_net_notional = final_pos.net_qty().abs() * mark;
        let final_aster_notional = final_pos.aster_qty.abs() * mark;
        let final_lighter_notional = final_pos.lighter_qty.abs() * mark;
        let lighter_ws_qty = lighter.ws_position_qty(&spec.market_id).ok();
        warn!(
            "residual recovery verification: aster={} lighter_rest={} lighter_ws={:?} net={} ${} aster_abs=${} lighter_abs=${} aster_open_orders={} lighter_open_orders={}",
            final_pos.aster_qty,
            final_pos.lighter_qty,
            lighter_ws_qty,
            final_pos.net_qty(),
            final_net_notional,
            final_aster_notional,
            final_lighter_notional,
            open_a.len(),
            open_l
        );
        if final_aster_notional <= cfg.risk.max_position_mismatch_usd
            && final_lighter_notional <= cfg.risk.max_position_mismatch_usd
            && open_a.is_empty()
            && open_l == 0
        {
            let estimated_loss_usdc = (margin_before.aster_available_usd
                + margin_before.lighter_available_usd
                - margin_after.aster_available_usd
                - margin_after.lighter_available_usd)
                .max(Decimal::ZERO);
            return Ok(RecoveryReport {
                action_taken,
                position: final_pos,
                lighter_ws_qty,
                margin_after,
                estimated_loss_usdc,
                aster_open_orders: open_a.len(),
                lighter_open_orders: open_l,
            });
        }
        if tokio::time::Instant::now() >= deadline {
            bail!(
                "residual recovery failed to verify flat: aster={} lighter={} net={} ${} aster_abs=${} lighter_abs=${} aster_open_orders={} lighter_open_orders={}",
                final_pos.aster_qty,
                final_pos.lighter_qty,
                final_pos.net_qty(),
                final_net_notional,
                final_aster_notional,
                final_lighter_notional,
                open_a.len(),
                open_l
            );
        }
    }
}

async fn reconcile_positions(
    market: &MarketId,
    aster: &AsterRest,
    lighter: &LighterVenue,
) -> Result<PositionSnapshot> {
    let (a, l) = tokio::join!(
        aster.position_qty(market),
        lighter.rest_position_qty(market)
    );
    Ok(PositionSnapshot {
        aster_qty: a?,
        lighter_qty: l?,
    })
}

async fn refresh_account_snapshot(
    market: &MarketId,
    aster: &AsterRest,
    lighter: &LighterVenue,
) -> Result<AccountSnapshot> {
    let (aster_pos, aster_available, lighter_account) = tokio::join!(
        aster.position_qty(market),
        aster.available_usdc(),
        lighter.account_snapshot(market)
    );
    let lighter_account = lighter_account?;
    let position = PositionSnapshot {
        aster_qty: aster_pos?,
        lighter_qty: lighter_account.position_qty,
    };
    let lighter_ws_qty = lighter.ws_position_qty(market).ok();
    let lighter_ws_rest_divergence_qty = lighter_ws_qty.map(|ws| (ws - position.lighter_qty).abs());
    let margins = MarginSnapshot {
        aster_available_usd: aster_available?,
        lighter_available_usd: lighter_account.available_usdc,
    };
    Ok(AccountSnapshot {
        position,
        lighter_ws_qty,
        lighter_ws_rest_divergence_qty,
        margins,
        refreshed_at: tokio::time::Instant::now(),
    })
}

fn spawn_account_snapshot_refresher(
    cfg: &Config,
    market: MarketId,
    aster: Arc<AsterRest>,
    lighter: Arc<LighterVenue>,
    tx: watch::Sender<AccountSnapshot>,
    paused: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    let refresh_ms = (cfg.live.max_account_snapshot_age_ms as u64 / 2).max(10_000);
    let retry_ms = cfg.risk.min_reconcile_interval_ms.max(5_000);
    let rate_limit_retry_ms = 30_000u64;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(refresh_ms));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            if paused.load(Ordering::Acquire) {
                continue;
            }
            match refresh_account_snapshot(&market, &aster, &lighter).await {
                Ok(snapshot) => {
                    // Re-check the pause flag before publishing: a refresh already in
                    // flight when the executor set paused=true would otherwise
                    // overwrite the fresh post-trade snapshot with pre-trade positions
                    // stamped refreshed_at=now, defeating the staleness gate for up to
                    // a full refresh interval.
                    if paused.load(Ordering::Acquire) {
                        continue;
                    }
                    debug!(
                        "cold account snapshot refreshed: age_ms=0 aster_pos={} lighter_pos={} lighter_ws_pos={:?} lighter_rest_ws_divergence_qty={:?} aster_available_usd={} lighter_available_usd={}",
                        snapshot.position.aster_qty,
                        snapshot.position.lighter_qty,
                        snapshot.lighter_ws_qty,
                        snapshot.lighter_ws_rest_divergence_qty,
                        snapshot.margins.aster_available_usd,
                        snapshot.margins.lighter_available_usd
                    );
                    if tx.send(snapshot).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let sleep_ms = if is_rate_limit_error(&e) {
                        rate_limit_retry_ms
                    } else {
                        retry_ms
                    };
                    warn!("cold account snapshot refresh failed: {e:#}; retrying in {sleep_ms}ms");
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
            }
        }
    })
}

fn is_rate_limit_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<reqwest::Error>()
            .and_then(reqwest::Error::status)
            == Some(reqwest::StatusCode::TOO_MANY_REQUESTS)
            || cause.to_string().contains("429 Too Many Requests")
    })
}

async fn reconcile_margins(aster: &AsterRest, lighter: &LighterVenue) -> Result<MarginSnapshot> {
    let (a, l) = tokio::join!(aster.available_usdc(), lighter.available_usdc());
    Ok(MarginSnapshot {
        aster_available_usd: a?,
        lighter_available_usd: l?,
    })
}

async fn ensure_clean_start(
    cfg: &Config,
    spec: &MarketSpec,
    aster_books: &AsterBookFeed,
    aster: &AsterRest,
    lighter: &LighterVenue,
    observe_only: bool,
) -> Result<()> {
    let pos = reconcile_positions(&spec.market_id, aster, lighter).await?;
    let lighter_ws_qty = lighter.ws_position_qty(&spec.market_id).ok();
    let (aster_book, lighter_book) = fetch_books(spec, aster_books, lighter)?;
    let open_a = aster.open_orders(&spec.market_id).await?;
    let open_l = lighter.open_orders_count(&spec.market_id).await?;
    if !open_a.is_empty() || open_l > 0 {
        if observe_only {
            warn!(
                "observe-only start: existing open orders present; continuing without order submission: Aster open_orders={} Lighter open_orders={}",
                open_a.len(),
                open_l
            );
        } else {
            bail!(
                "clean-start failed: Aster open_orders={} Lighter open_orders={}",
                open_a.len(),
                open_l
            );
        }
    }
    let mark = aster_book
        .mid()
        .or_else(|| lighter_book.mid())
        .unwrap_or(Decimal::ONE);
    let mismatch = pos.net_qty().abs() * mark;
    if let Some(ws_qty) = lighter_ws_qty {
        let divergence_notional = (ws_qty - pos.lighter_qty).abs() * mark;
        if divergence_notional > cfg.risk.max_position_mismatch_usd {
            bail!(
                "clean-start failed: Lighter REST/WS position divergence rest={} ws={} divergence_notional=${}",
                pos.lighter_qty,
                ws_qty,
                divergence_notional
            );
        }
    }
    if mismatch > cfg.risk.max_position_mismatch_usd {
        if observe_only {
            // A read-only start (observer / standby-until-lease) must tolerate the
            // active bot's transient residuals — bailing here crash-loops the observer
            // exactly while the active bot is mid-recovery.
            warn!(
                "observe-only start: positions not balanced (likely the active bot's transient); continuing without order submission: aster={} lighter={} net={}",
                pos.aster_qty,
                pos.lighter_qty,
                pos.net_qty()
            );
        } else {
            bail!(
                "clean-start failed: positions not balanced aster={} lighter={} net={}",
                pos.aster_qty,
                pos.lighter_qty,
                pos.net_qty()
            );
        }
    }
    info!(
        "clean start confirmed: aster={} lighter_rest={} lighter_ws={:?} aster_open_orders={} lighter_open_orders={} observe_only={}",
        pos.aster_qty,
        pos.lighter_qty,
        lighter_ws_qty,
        open_a.len(),
        open_l,
        observe_only
    );
    Ok(())
}

fn net_mismatch_notional(
    pos: PositionSnapshot,
    aster: &OrderBook,
    lighter: &OrderBook,
) -> Option<Decimal> {
    let mark_f = aster
        .mid_f64()
        .or_else(|| lighter.mid_f64())
        ?;
    let net_f = decimal_to_f64(pos.aster_qty)? + decimal_to_f64(pos.lighter_qty)?;
    Some(f64_to_dec(net_f.abs() * mark_f))
}

fn log_scan_state(
    cfg: &Config,
    spec: &MarketSpec,
    aster: &OrderBook,
    lighter: &OrderBook,
    pos: PositionSnapshot,
    margins: MarginSnapshot,
) {
    let Some(a_bid) = aster.best_bid() else {
        return;
    };
    let Some(a_ask) = aster.best_ask() else {
        return;
    };
    let Some(l_bid) = lighter.best_bid() else {
        return;
    };
    let Some(l_ask) = lighter.best_ask() else {
        return;
    };
    let Some(ref_px) = aster.mid().or_else(|| lighter.mid()) else {
        return;
    };
    let edge_sell_aster = (a_bid.px - l_ask.px) / ref_px * Decimal::from(10_000);
    let edge_sell_lighter = (l_bid.px - a_ask.px) / ref_px * Decimal::from(10_000);
    debug!(
        "arb scan market={} a_bid={}x{} a_ask={}x{} l_bid={}x{} l_ask={}x{} edge_sell_aster={}bps edge_sell_lighter={}bps required={}bps pos_aster={} pos_lighter={} net_pos={} margin_aster=${} margin_lighter=${}",
        spec.market_id,
        a_bid.px,
        a_bid.qty,
        a_ask.px,
        a_ask.qty,
        l_bid.px,
        l_bid.qty,
        l_ask.px,
        l_ask.qty,
        edge_sell_aster,
        edge_sell_lighter,
        cfg.arb.required_gross_edge_bps(),
        pos.aster_qty,
        pos.lighter_qty,
        pos.net_qty(),
        margins.aster_available_usd,
        margins.lighter_available_usd
    );
}

fn aster_price_bound(opp: &Opportunity, slippage_bps: Decimal) -> Decimal {
    let rate = bps_to_rate(slippage_bps);
    match opp.direction.aster_side() {
        Side::Buy => opp.buy_px * (Decimal::ONE + rate),
        Side::Sell => opp.sell_px * (Decimal::ONE - rate),
    }
}

fn lighter_price_bound(opp: &Opportunity, slippage_bps: Decimal) -> Decimal {
    let rate = bps_to_rate(slippage_bps);
    match opp.direction.lighter_side() {
        Side::Buy => opp.buy_px * (Decimal::ONE + rate),
        Side::Sell => opp.sell_px * (Decimal::ONE - rate),
    }
}

#[cfg(test)]
fn floor_to_common_step(qty: Decimal, aster_step: Decimal, lighter_step: Decimal) -> Decimal {
    floor_to_step(floor_to_step(qty, aster_step), lighter_step)
}

#[cfg(test)]
fn ceil_to_common_step(qty: Decimal, aster_step: Decimal, lighter_step: Decimal) -> Decimal {
    if qty <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let mut out = qty;
    for _ in 0..8 {
        let next = ceil_to_step(ceil_to_step(out, aster_step), lighter_step);
        if next == out && is_step_multiple(out, aster_step) && is_step_multiple(out, lighter_step) {
            return out;
        }
        out = next;
    }
    out
}

fn floor_to_step(qty: Decimal, step: Decimal) -> Decimal {
    if qty <= Decimal::ZERO || step <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (qty / step).floor() * step
}

#[cfg(test)]
fn ceil_to_step(qty: Decimal, step: Decimal) -> Decimal {
    if qty <= Decimal::ZERO || step <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (qty / step).ceil() * step
}

#[cfg(test)]
fn is_step_multiple(qty: Decimal, step: Decimal) -> bool {
    if qty < Decimal::ZERO || step <= Decimal::ZERO {
        return false;
    }
    (qty / step).fract() == Decimal::ZERO
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ArbCfg, LiveCfg, PnlCfg, RiskCfg, VenueCfg};
    use rust_decimal_macros::dec;

    fn test_cfg() -> Config {
        Config {
            arb: ArbCfg {
                entry_gate: crate::config::EntryGateCfg {
                    enabled: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            pnl: PnlCfg::default(),
            live: LiveCfg::default(),
            venues: VenueCfg::default(),
            risk: RiskCfg::default(),
            markets: vec![],
        }
    }

    fn test_spec() -> MarketSpec {
        MarketSpec {
            market_id: MarketId("HYPE".to_string()),
            aster_symbol: "HYPEUSDT".to_string(),
            lighter_symbol: "HYPE".to_string(),
            lighter_market_id: 24,
            lighter_price_decimals: 4,
            lighter_size_decimals: 2,
            lighter_price_tick: dec!(0.0001),
            tick: dec!(0.001),
            step: dec!(0.01),
            aster_min_qty: dec!(0.01),
            aster_min_notional: dec!(10),
            lighter_qty_step: dec!(0.01),
            lighter_min_notional: dec!(10),
        }
    }

    fn book(bid: Decimal, ask: Decimal) -> OrderBook {
        let now = Utc::now();
        OrderBook::from_levels([(bid, dec!(10))], [(ask, dec!(10))], now, now)
    }

    fn depth_book(
        bids: impl IntoIterator<Item = (Decimal, Decimal)>,
        asks: impl IntoIterator<Item = (Decimal, Decimal)>,
    ) -> OrderBook {
        let now = Utc::now();
        OrderBook::from_levels(bids, asks, now, now)
    }

    fn margins() -> MarginSnapshot {
        MarginSnapshot {
            aster_available_usd: dec!(1000),
            lighter_available_usd: dec!(1000),
        }
    }

    fn test_math(cfg: &Config, spec: &MarketSpec) -> MarketMathF64 {
        MarketMathF64::from_config_spec(cfg, spec).unwrap()
    }

    fn pos_f64(pos: PositionSnapshot) -> PositionF64 {
        PositionF64::from_snapshot(pos).unwrap()
    }

    fn margins_f64(margins: MarginSnapshot) -> MarginF64 {
        MarginF64::from_snapshot(margins).unwrap()
    }

    fn retry_opp(direction: Direction) -> Opportunity {
        Opportunity {
            direction,
            qty: dec!(0.20),
            qty_f64: 0.20,
            gross_edge_bps: dec!(9),
            expected_net_margin_bps: dec!(3),
            sell_px: dec!(63.8000),
            buy_px: dec!(63.7200),
            ref_px: dec!(63.7600),
            top_depth_qty: dec!(0.20),
            depth_guard_enabled: true,
            liquidity_multiple: dec!(10),
            depth_supported_qty: dec!(0.20),
            sell_depth_target_qty: dec!(2.00),
            buy_depth_target_qty: dec!(2.00),
            sell_depth_available_qty: dec!(2.00),
            buy_depth_available_qty: dec!(2.00),
            sell_depth_worst_px: dec!(63.8000),
            buy_depth_worst_px: dec!(63.7200),
            sell_depth_levels_used: 1,
            buy_depth_levels_used: 1,
            sell_best_px: dec!(63.8000),
            buy_best_px: dec!(63.7200),
            sell_best_qty: dec!(2.00),
            buy_best_qty: dec!(2.00),
            desired_qty: dec!(0.20),
            min_qty: dec!(0.20),
            headroom_qty: dec!(10),
            margin_room_qty: dec!(10),
            expected_gross_usd: dec!(0.016),
            expected_fee_usd: dec!(0.005),
            expected_net_usd: dec!(0.011),
            required_margin_usd: dec!(0.0025),
        }
    }

    #[test]
    fn actual_economics_books_only_matched_qty() {
        // Unequal legs: sell 0.20 @ 63.80, buy 0.16 @ 63.72. The naive notional difference
        // (12.76 − 10.1952 = +2.56) would book the unhedged 0.04 residual as phantom
        // profit; real matched PnL is 0.16 × (63.80 − 63.72) = 0.0128.
        let cfg = test_cfg();
        let opp = retry_opp(Direction::SellAsterBuyLighter);
        let aster_fill = FillSummary {
            qty: dec!(0.20),
            vwap: dec!(63.80),
            notional: dec!(12.76),
            fee_usd: dec!(0.005),
        };
        let lighter_fill = FillSummary {
            qty: dec!(0.16),
            vwap: dec!(63.72),
            notional: dec!(10.1952),
            fee_usd: dec!(0),
        };
        let e = actual_economics(&cfg, &opp, aster_fill, lighter_fill).expect("within tolerance");
        assert_eq!(e.gross_usd, dec!(0.0128));
        assert_eq!(e.net_usd, dec!(0.0078));
        assert_eq!(e.residual_qty, dec!(0.04));
        assert_eq!(e.residual_notional_usd, dec!(0.04) * opp.ref_px);
    }

    #[test]
    fn headroom_allows_reducing_then_flipping() {
        let pos = PositionSnapshot {
            aster_qty: dec!(1),
            lighter_qty: dec!(-1),
        };
        let q = max_qty_by_headroom(dec!(2), pos, dec!(-1), dec!(1));
        assert_eq!(q, dec!(3));
    }

    #[test]
    fn common_step_floors_qty() {
        assert_eq!(
            floor_to_common_step(dec!(1.2345), dec!(0.01), dec!(0.001)),
            dec!(1.23)
        );
    }

    #[test]
    fn f64_common_step_snaps_near_integer_units() {
        let cfg = test_cfg();
        let mut spec = test_spec();
        spec.step = dec!(0.1);
        spec.lighter_qty_step = dec!(0.1);
        let math = test_math(&cfg, &spec);
        let floored = floor_to_common_step_f64(0.3, &math);
        assert!(
            (floored - 0.3).abs() < 1e-12,
            "floor should snap 0.3 to the 0.1 grid, got {floored:?}"
        );
        let ceiled = ceil_to_common_step_f64(0.30000000000000004, &math);
        assert!(
            (ceiled - 0.3).abs() < 1e-12,
            "ceil should not jump f64-noisy 0.3 to 0.4, got {ceiled:?}"
        );
    }

    #[test]
    fn reduce_filter_selects_reducing_opportunity() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: dec!(-1),
            lighter_qty: dec!(1),
        };
        let opp = best_opportunity(
            &cfg,
            &spec,
            &math,
            &book(dec!(99), dec!(100)),
            &book(dec!(101), dec!(101.1)),
            pos_f64(pos),
            margins_f64(margins()),
            false,
            ExposureFilter::Reduce,
        )
        .expect("reduce opportunity should be selected");
        assert_eq!(opp.direction, Direction::SellLighterBuyAster);
        assert_eq!(
            exposure_effect(pos, opp.direction, opp.qty),
            ExposureEffect::Reduce
        );
    }

    #[test]
    fn reduce_filter_ignores_increasing_opportunity() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: dec!(-1),
            lighter_qty: dec!(1),
        };
        let aster = book(dec!(101), dec!(101.1));
        let lighter = book(dec!(100), dec!(100.1));
        let any = best_opportunity(
            &cfg,
            &spec,
            &math,
            &aster,
            &lighter,
            pos_f64(pos),
            margins_f64(margins()),
            false,
            ExposureFilter::Any,
        )
        .expect("increasing opportunity exists");
        assert_eq!(any.direction, Direction::SellAsterBuyLighter);
        assert_eq!(
            exposure_effect(pos, any.direction, any.qty),
            ExposureEffect::Increase
        );
        assert!(best_opportunity(
            &cfg,
            &spec,
            &math,
            &aster,
            &lighter,
            pos_f64(pos),
            margins_f64(margins()),
            false,
            ExposureFilter::Reduce,
        )
        .is_none());
    }

    #[test]
    fn depth_guard_rejects_profitable_but_thin_top_of_book() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: Decimal::ZERO,
            lighter_qty: Decimal::ZERO,
        };
        let aster = depth_book(
            [(dec!(101), dec!(0.20)), (dec!(99), dec!(10))],
            [(dec!(103), dec!(10))],
        );
        let lighter = depth_book(
            [(dec!(98), dec!(10))],
            [(dec!(100), dec!(0.20)), (dec!(102), dec!(10))],
        );
        assert!(best_opportunity(
            &cfg,
            &spec,
            &math,
            &aster,
            &lighter,
            pos_f64(pos),
            margins_f64(margins()),
            false,
            ExposureFilter::Any,
        )
        .is_none());

        let mut top_only_cfg = cfg.clone();
        top_only_cfg.arb.depth_guard.enabled = false;
        let top_only_math = test_math(&top_only_cfg, &spec);
        assert!(best_opportunity(
            &top_only_cfg,
            &spec,
            &top_only_math,
            &aster,
            &lighter,
            pos_f64(pos),
            margins_f64(margins()),
            false,
            ExposureFilter::Any,
        )
        .is_some());
    }

    #[test]
    fn opportunity_profitability_uses_depth_vwap() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: Decimal::ZERO,
            lighter_qty: Decimal::ZERO,
        };
        let aster = depth_book(
            [(dec!(101), dec!(0.20)), (dec!(100.90), dec!(10))],
            [(dec!(103), dec!(10))],
        );
        let lighter = depth_book(
            [(dec!(98), dec!(10))],
            [(dec!(100), dec!(0.20)), (dec!(100.10), dec!(10))],
        );
        let opp = best_opportunity(
            &cfg,
            &spec,
            &math,
            &aster,
            &lighter,
            pos_f64(pos),
            margins_f64(margins()),
            false,
            ExposureFilter::Any,
        )
        .expect("depth-priced opportunity should remain profitable");
        assert_eq!(opp.direction, Direction::SellAsterBuyLighter);
        assert!(opp.sell_px < dec!(101));
        assert!(opp.buy_px > dec!(100));
        assert_eq!(opp.sell_depth_target_qty, opp.qty * dec!(10));
        assert_eq!(opp.buy_depth_target_qty, opp.qty * dec!(10));
        assert_eq!(opp.sell_depth_levels_used, 2);
        assert_eq!(opp.buy_depth_levels_used, 2);
        assert!(opp.gross_edge_bps < dec!(100));
        assert!(opp.gross_edge_bps >= cfg.arb.required_gross_edge_bps());
    }

    #[test]
    fn edge_prefilter_never_skips_what_exact_filter_accepts() {
        // The f64 pre-filter may only skip candidates the exact Decimal filter also
        // rejects; anything within the epsilon band must fall through to the exact
        // comparison. Sweep a dense neighborhood of several thresholds.
        for required in [dec!(6), dec!(0.5), dec!(4.5), dec!(10.000000000001)] {
            let required_f = decimal_to_f64(required).unwrap();
            for k in -10_000i64..=10_000 {
                let edge_f = required_f + k as f64 * 1e-13;
                if edge_f < required_f - EDGE_PREFILTER_EPS_BPS {
                    assert!(
                        f64_to_dec(edge_f) < required,
                        "pre-filter skipped a candidate the exact filter accepts: edge_f={edge_f} required={required}"
                    );
                }
            }
            // The threshold itself and its immediate neighborhood sit inside the band:
            // they must NOT be pre-skipped (the exact filter decides them, as today).
            for k in [-5_000i64, -1, 0, 1, 5_000] {
                let edge_f = required_f + k as f64 * 1e-13;
                assert!(edge_f >= required_f - EDGE_PREFILTER_EPS_BPS);
            }
            // NaN never triggers the skip; it falls through and f64_to_dec(NaN) == 0
            // is rejected by the exact filter — identical to the pre-change behavior.
            assert!(!(f64::NAN < required_f - EDGE_PREFILTER_EPS_BPS));
        }
    }

    #[test]
    fn flatten_execution_rights_gate_denies_observer_and_standby() {
        // The mismatch auto-flatten reuses execution_lease_enabled as its rights gate:
        // pin the four deployment shapes. (The orchestrator's 24/7 observer runs with
        // --control-file and NO lease — it must never be allowed to flatten.)
        let spec = test_spec();
        let now = DateTime::parse_from_rfc3339("2026-07-09T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        // Normal active taker: no control file, not observe-only -> allowed.
        let mut cache = LeaseFileCache::new();
        let options = RunOptions::default();
        let (allowed, _) = execution_lease_enabled(&mut cache, &options, &spec, now);
        assert!(allowed, "normal taker must keep its auto-flatten safety net");

        // --observe-only: never allowed, even without a control file.
        let mut cache = LeaseFileCache::new();
        let options = RunOptions {
            observe_only: true,
            ..RunOptions::default()
        };
        let (allowed, _) = execution_lease_enabled(&mut cache, &options, &spec, now);
        assert!(!allowed, "observe-only must never hold execution rights");

        // Standby observer: control file set, lease file absent -> not allowed.
        let dir = std::env::temp_dir().join(format!(
            "lighter_aster_taker_arb_gate_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("lease.json");
        let mut cache = LeaseFileCache::new();
        let options = RunOptions {
            control_file: Some(path.clone()),
            ..RunOptions::default()
        };
        let (allowed, _) = execution_lease_enabled(&mut cache, &options, &spec, now);
        assert!(!allowed, "standby without a lease must not hold execution rights");

        // Reduce taker with a valid lease -> allowed.
        let expires = now + chrono::Duration::seconds(60);
        std::fs::write(
            &path,
            format!(
                r#"{{"market":"HYPE","mode":"reduce_only","lease_id":"g","expires_at":"{}"}}"#,
                expires.to_rfc3339()
            ),
        )
        .unwrap();
        let mut cache = LeaseFileCache::new();
        let (allowed, lease) = execution_lease_enabled(&mut cache, &options, &spec, now);
        assert!(allowed, "a valid reduce lease grants execution rights");
        assert_eq!(lease.unwrap().lease_id.as_deref(), Some("g"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn lease_cache_throttles_reads_and_enforces_expiry() {
        let dir = std::env::temp_dir().join(format!(
            "lighter_aster_taker_arb_lease_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("lease.json");
        let now = DateTime::parse_from_rfc3339("2026-06-24T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let expires = now + chrono::Duration::seconds(60);
        let write_lease = |id: &str| {
            std::fs::write(
                &path,
                format!(
                    r#"{{"market":"HYPE","mode":"reduce_only","lease_id":"{id}","expires_at":"{}"}}"#,
                    expires.to_rfc3339()
                ),
            )
            .unwrap()
        };
        write_lease("a");
        let options = RunOptions {
            control_file: Some(path.clone()),
            ..RunOptions::default()
        };
        let spec = test_spec();
        let mut cache = LeaseFileCache::new();

        let (enabled, lease) = execution_lease_enabled(&mut cache, &options, &spec, now);
        assert!(enabled);
        assert_eq!(lease.unwrap().lease_id.as_deref(), Some("a"));

        // A rewrite within the reread interval is not seen yet (the read is throttled)...
        write_lease("b");
        let (_, lease) = execution_lease_enabled(&mut cache, &options, &spec, now);
        assert_eq!(lease.unwrap().lease_id.as_deref(), Some("a"));

        // ...but expiry is enforced against the CACHED lease on every call, no re-read.
        let (enabled, lease) = execution_lease_enabled(&mut cache, &options, &spec, expires);
        assert!(!enabled);
        assert!(lease.is_none());

        // Once the interval passes (backdate the last read) the rewrite is picked up.
        cache.last_read_at = Some(tokio::time::Instant::now() - LEASE_REREAD_INTERVAL);
        let (_, lease) = execution_lease_enabled(&mut cache, &options, &spec, now);
        assert_eq!(lease.unwrap().lease_id.as_deref(), Some("b"));

        // A deleted file fails closed at the next re-read.
        std::fs::remove_file(&path).unwrap();
        cache.last_read_at = Some(tokio::time::Instant::now() - LEASE_REREAD_INTERVAL);
        let (enabled, lease) = execution_lease_enabled(&mut cache, &options, &spec, now);
        assert!(!enabled);
        assert!(lease.is_none());

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn best_opportunity_boundary_matches_exact_filter() {
        // End-to-end: books one tick apart straddling the 6 bps threshold (test_cfg:
        // 4 + 0 + 2). ref = aster mid = s + 0.1; edge_bps = (s - 100)/(s + 0.1) * 1e4:
        // s = 100.061 -> ~6.09 bps (accept), s = 100.060 -> ~5.99 bps (reject).
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: Decimal::ZERO,
            lighter_qty: Decimal::ZERO,
        };
        let lighter = depth_book([(dec!(98), dec!(10))], [(dec!(100), dec!(10))]);
        let run = |s: Decimal| {
            let aster = depth_book([(s, dec!(10))], [(s + dec!(0.2), dec!(10))]);
            best_opportunity(
                &cfg,
                &spec,
                &math,
                &aster,
                &lighter,
                pos_f64(pos),
                margins_f64(margins()),
                false,
                ExposureFilter::Any,
            )
        };
        let above = run(dec!(100.061)).expect("edge above threshold must survive");
        assert_eq!(above.direction, Direction::SellAsterBuyLighter);
        assert!(above.gross_edge_bps >= cfg.arb.required_gross_edge_bps());
        assert!(run(dec!(100.060)).is_none(), "edge below threshold must be filtered");
    }

    #[test]
    fn hedge_retry_plan_completes_missing_lighter_buy() {
        let cfg = test_cfg();
        let spec = test_spec();
        let opp = retry_opp(Direction::SellAsterBuyLighter);
        let pre = PositionSnapshot {
            aster_qty: dec!(-0.60),
            lighter_qty: dec!(0.60),
        };
        let current = PositionSnapshot {
            aster_qty: dec!(-0.80),
            lighter_qty: dec!(0.60),
        };
        let plan = hedge_retry_plan(&cfg, &spec, &opp, pre, current).unwrap();
        assert_eq!(plan.venue, HedgeRetryVenue::Lighter);
        assert_eq!(plan.side, Side::Buy);
        assert_eq!(plan.qty, dec!(0.20));
        assert_eq!(plan.price_bound, dec!(63.9111600));
    }

    #[test]
    fn hedge_retry_plan_completes_missing_aster_buy() {
        let cfg = test_cfg();
        let spec = test_spec();
        let opp = retry_opp(Direction::SellLighterBuyAster);
        let pre = PositionSnapshot {
            aster_qty: dec!(-0.60),
            lighter_qty: dec!(0.60),
        };
        let current = PositionSnapshot {
            aster_qty: dec!(-0.60),
            lighter_qty: dec!(0.40),
        };
        let plan = hedge_retry_plan(&cfg, &spec, &opp, pre, current).unwrap();
        assert_eq!(plan.venue, HedgeRetryVenue::Aster);
        assert_eq!(plan.side, Side::Buy);
        assert_eq!(plan.qty, dec!(0.20));
        assert_eq!(plan.price_bound, dec!(63.9111600));
    }

    #[test]
    fn hedge_retry_plan_skips_ambiguous_two_sided_miss() {
        let cfg = test_cfg();
        let spec = test_spec();
        let opp = retry_opp(Direction::SellAsterBuyLighter);
        let pre = PositionSnapshot {
            aster_qty: dec!(-0.60),
            lighter_qty: dec!(0.60),
        };
        assert!(hedge_retry_plan(&cfg, &spec, &opp, pre, pre).is_none());
    }

    #[test]
    fn recovered_failure_tracker_trips_count_limit() {
        let mut cfg = test_cfg();
        cfg.arb.max_recovered_failures_per_hour = 2;
        let mut tracker = RecoveredFailureTracker::default();
        assert!(tracker.record(dec!(0.01), &cfg).is_none());
        assert!(tracker.record(dec!(0.01), &cfg).is_none());
        let reason = tracker
            .record(dec!(0.01), &cfg)
            .expect("third recovered failure should trip count breaker");
        assert!(reason.contains("count"));
        assert_eq!(tracker.event_count(), 3);
    }

    #[test]
    fn recovered_failure_tracker_trips_loss_limit() {
        let mut cfg = test_cfg();
        cfg.arb.max_recovered_loss_usdc_per_hour = dec!(0.02);
        let mut tracker = RecoveredFailureTracker::default();
        assert!(tracker.record(dec!(0.01), &cfg).is_none());
        let reason = tracker
            .record(dec!(0.011), &cfg)
            .expect("loss sum above limit should trip loss breaker");
        assert!(reason.contains("loss"));
        assert_eq!(tracker.event_count(), 2);
    }

    fn assert_opp_eq_f64_vs_decimal(
        f64_opp: &Opportunity,
        dec_opp: &Opportunity,
        ctx: &str,
    ) {
        let qty_diff = (f64_opp.qty - dec_opp.qty).abs();
        assert!(
            qty_diff <= dec!(0.0000001),
            "{ctx}: qty drift exceeded: f64={} dec={} diff={}",
            f64_opp.qty,
            dec_opp.qty,
            qty_diff
        );
        let edge_diff = (f64_opp.gross_edge_bps - dec_opp.gross_edge_bps).abs();
        assert!(
            edge_diff < dec!(0.001),
            "{ctx}: gross_edge_bps drift exceeded: f64={} dec={} diff={}",
            f64_opp.gross_edge_bps,
            dec_opp.gross_edge_bps,
            edge_diff
        );
        let net_diff = (f64_opp.expected_net_usd - dec_opp.expected_net_usd).abs();
        assert!(
            net_diff < dec!(0.0000001),
            "{ctx}: expected_net_usd drift exceeded: f64={} dec={} diff={}",
            f64_opp.expected_net_usd,
            dec_opp.expected_net_usd,
            net_diff
        );
    }

    fn build_opportunity_decimal(
        cfg: &Config,
        direction: Direction,
        sizing: SizingDecision,
        sell_px: Decimal,
        buy_px: Decimal,
        ref_px: Decimal,
    ) -> Opportunity {
        let gross_edge_bps = (sell_px - buy_px) / ref_px * Decimal::from(10_000);
        let aster_px = if matches!(direction.aster_side(), Side::Sell) {
            sell_px
        } else {
            buy_px
        };
        let lighter_px = if matches!(direction.lighter_side(), Side::Sell) {
            sell_px
        } else {
            buy_px
        };
        let gross_usd = sizing.qty * (sell_px - buy_px);
        let fee_usd = sizing.qty
            * (aster_px * bps_to_rate(cfg.arb.aster_taker_fee_bps)
                + lighter_px * bps_to_rate(cfg.arb.lighter_taker_fee_bps));
        let required_margin_usd = sizing.qty * ref_px * bps_to_rate(cfg.arb.margin_bps);
        Opportunity {
            direction,
            qty: sizing.qty,
            qty_f64: sizing.qty.to_f64().unwrap(),
            gross_edge_bps,
            expected_net_margin_bps: gross_edge_bps - cfg.arb.required_gross_edge_bps(),
            sell_px,
            buy_px,
            ref_px,
            top_depth_qty: sizing.top_depth_qty,
            depth_guard_enabled: sizing.depth_guard_enabled,
            liquidity_multiple: sizing.liquidity_multiple,
            depth_supported_qty: sizing.depth_supported_qty,
            sell_depth_target_qty: sizing.sell_depth_target_qty,
            buy_depth_target_qty: sizing.buy_depth_target_qty,
            sell_depth_available_qty: sizing.sell_depth_available_qty,
            buy_depth_available_qty: sizing.buy_depth_available_qty,
            sell_depth_worst_px: sizing.sell_depth_worst_px,
            buy_depth_worst_px: sizing.buy_depth_worst_px,
            sell_depth_levels_used: sizing.sell_depth_levels_used,
            buy_depth_levels_used: sizing.buy_depth_levels_used,
            sell_best_px: sizing.sell_best_px,
            buy_best_px: sizing.buy_best_px,
            sell_best_qty: sizing.sell_best_qty,
            buy_best_qty: sizing.buy_best_qty,
            desired_qty: sizing.desired_qty,
            min_qty: sizing.min_qty,
            headroom_qty: sizing.headroom_qty,
            margin_room_qty: sizing.margin_room_qty,
            expected_gross_usd: gross_usd,
            expected_fee_usd: fee_usd,
            expected_net_usd: gross_usd - fee_usd,
            required_margin_usd,
        }
    }

    fn decimal_depth_priced_opportunity(
        cfg: &Config,
        spec: &MarketSpec,
        direction: Direction,
        aster: &OrderBook,
        lighter: &OrderBook,
        pos: PositionSnapshot,
        margins: MarginSnapshot,
        min_size: bool,
    ) -> Option<Opportunity> {
        let ref_px = aster.mid().or_else(|| lighter.mid())?;
        if ref_px <= Decimal::ZERO {
            return None;
        }
        let (sell_book, buy_book) = match direction {
            Direction::SellAsterBuyLighter => (aster, lighter),
            Direction::SellLighterBuyAster => (lighter, aster),
        };
        let sell_top = sell_book.side_levels(Side::Sell).first().copied()?;
        let buy_top = buy_book.side_levels(Side::Buy).first().copied()?;
        let top_depth_qty = sell_top.qty.min(buy_top.qty);
        let desired = cfg.arb.desired_notional / ref_px;
        let est_aster_px = if matches!(direction.aster_side(), Side::Sell) {
            sell_top.px
        } else {
            buy_top.px
        };
        let est_lighter_px = if matches!(direction.lighter_side(), Side::Sell) {
            sell_top.px
        } else {
            buy_top.px
        };
        let est_min_qty = min_trade_qty(spec, est_aster_px, est_lighter_px)?;
        let a_delta_sign = if matches!(direction.aster_side(), Side::Buy) {
            Decimal::ONE
        } else {
            -Decimal::ONE
        };
        let l_delta_sign = -a_delta_sign;
        let headroom = max_qty_by_headroom(
            cfg.risk.max_abs_position_notional_usd / ref_px,
            pos,
            a_delta_sign,
            l_delta_sign,
        );
        let margin_room =
            max_qty_by_available_margin(cfg, ref_px, pos, margins, a_delta_sign, l_delta_sign);
        let depth_guard_enabled = cfg.arb.depth_guard.enabled;
        let liquidity_multiple = if depth_guard_enabled {
            cfg.arb.depth_guard.liquidity_multiple
        } else {
            Decimal::ONE
        };
        let max_levels = if depth_guard_enabled {
            cfg.arb.depth_guard.max_levels
        } else {
            1
        };
        let sell_available = sell_book.cumulative_qty(Side::Sell, max_levels);
        let buy_available = buy_book.cumulative_qty(Side::Buy, max_levels);
        let depth_supported_qty = sell_available.min(buy_available) / liquidity_multiple;
        let max_qty = depth_supported_qty.min(headroom).min(margin_room);
        if max_qty <= Decimal::ZERO {
            return None;
        }
        let initial_qty = if min_size {
            let q = ceil_to_common_step(est_min_qty, spec.step, spec.lighter_qty_step);
            if q <= max_qty { q } else { return None; }
        } else {
            let q = floor_to_common_step(desired.min(max_qty), spec.step, spec.lighter_qty_step);
            if q >= est_min_qty { q } else { return None; }
        };
        let (sizing, sell_px, buy_px) = decimal_depth_price_sized_qty(
            spec, direction, sell_book, buy_book, initial_qty, desired,
            top_depth_qty, headroom, margin_room, depth_guard_enabled,
            liquidity_multiple, max_levels, depth_supported_qty, min_size,
        )?;
        Some(build_opportunity_decimal(cfg, direction, sizing, sell_px, buy_px, ref_px))
    }

    fn decimal_depth_price_sized_qty(
        spec: &MarketSpec,
        direction: Direction,
        sell_book: &OrderBook,
        buy_book: &OrderBook,
        initial_qty: Decimal,
        desired_qty: Decimal,
        top_depth_qty: Decimal,
        headroom_qty: Decimal,
        margin_room_qty: Decimal,
        depth_guard_enabled: bool,
        liquidity_multiple: Decimal,
        max_levels: usize,
        depth_supported_qty: Decimal,
        min_size: bool,
    ) -> Option<(SizingDecision, Decimal, Decimal)> {
        let mut qty = initial_qty;
        for _ in 0..3 {
            if qty <= Decimal::ZERO || qty > depth_supported_qty {
                return None;
            }
            let depth_target = qty * liquidity_multiple;
            let sell_quote = sell_book.depth_vwap(Side::Sell, depth_target, max_levels)?;
            let buy_quote = buy_book.depth_vwap(Side::Buy, depth_target, max_levels)?;
            let sell_px = sell_quote.vwap_px;
            let buy_px = buy_quote.vwap_px;
            let aster_px = if matches!(direction.aster_side(), Side::Sell) {
                sell_px
            } else {
                buy_px
            };
            let lighter_px = if matches!(direction.lighter_side(), Side::Sell) {
                sell_px
            } else {
                buy_px
            };
            let min_qty = min_trade_qty(spec, aster_px, lighter_px)?;
            if min_size {
                let min_step_qty = ceil_to_common_step(min_qty, spec.step, spec.lighter_qty_step);
                if min_step_qty > qty {
                    if min_step_qty > depth_supported_qty
                        || min_step_qty > headroom_qty
                        || min_step_qty > margin_room_qty
                    {
                        return None;
                    }
                    qty = min_step_qty;
                    continue;
                }
            } else if qty < min_qty {
                return None;
            }
            return Some((
                SizingDecision {
                    qty,
                    desired_qty,
                    min_qty,
                    top_depth_qty,
                    depth_guard_enabled,
                    liquidity_multiple,
                    depth_supported_qty,
                    sell_depth_target_qty: sell_quote.target_qty,
                    buy_depth_target_qty: buy_quote.target_qty,
                    sell_depth_available_qty: sell_quote.available_qty,
                    buy_depth_available_qty: buy_quote.available_qty,
                    sell_depth_worst_px: sell_quote.worst_px,
                    buy_depth_worst_px: buy_quote.worst_px,
                    sell_depth_levels_used: sell_quote.levels_used,
                    buy_depth_levels_used: buy_quote.levels_used,
                    sell_best_px: sell_quote.best_px,
                    buy_best_px: buy_quote.best_px,
                    sell_best_qty: sell_quote.best_qty,
                    buy_best_qty: buy_quote.best_qty,
                    headroom_qty,
                    margin_room_qty,
                },
                sell_px,
                buy_px,
            ));
        }
        None
    }

    #[test]
    fn f64_matches_decimal_top_of_book_opportunity() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: Decimal::ZERO,
            lighter_qty: Decimal::ZERO,
        };
        let margins = margins();
        for direction in [Direction::SellAsterBuyLighter, Direction::SellLighterBuyAster] {
            let (a_bid, a_ask, l_bid, l_ask) = match direction {
                Direction::SellAsterBuyLighter => (dec!(101), dec!(103), dec!(98), dec!(100)),
                Direction::SellLighterBuyAster => (dec!(100), dec!(101.5), dec!(101), dec!(103)),
            };
            let aster = book(a_bid, a_ask);
            let lighter = book(l_bid, l_ask);
            let f64_opp = depth_priced_opportunity(
                &cfg,
                &spec,
                &math,
                direction,
                &aster,
                &lighter,
                pos_f64(pos),
                margins_f64(margins),
                false,
            )
            .expect("f64 opportunity should exist");
            let dec_opp = decimal_depth_priced_opportunity(
                &cfg, &spec, direction, &aster, &lighter, pos, margins, false,
            )
            .expect("decimal opportunity should exist");
            assert_opp_eq_f64_vs_decimal(&f64_opp, &dec_opp, direction.as_str());
        }
    }

    #[test]
    fn f64_matches_decimal_multi_level_depth_opportunity() {
        let mut cfg = test_cfg();
        cfg.arb.depth_guard.enabled = true;
        cfg.arb.depth_guard.liquidity_multiple = dec!(2);
        cfg.arb.depth_guard.max_levels = 5;
        cfg.arb.desired_notional = dec!(1000);
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: Decimal::ZERO,
            lighter_qty: Decimal::ZERO,
        };
        let margins = margins();
        let aster = depth_book(
            [
                (dec!(100.00), dec!(5)),
                (dec!(99.50), dec!(10)),
                (dec!(99.00), dec!(20)),
                (dec!(98.50), dec!(30)),
            ],
            [
                (dec!(100.50), dec!(5)),
                (dec!(101.00), dec!(10)),
                (dec!(101.50), dec!(20)),
                (dec!(102.00), dec!(30)),
            ],
        );
        let lighter = depth_book(
            [
                (dec!(99.00), dec!(4)),
                (dec!(98.50), dec!(8)),
                (dec!(98.00), dec!(16)),
                (dec!(97.50), dec!(32)),
            ],
            [
                (dec!(99.50), dec!(4)),
                (dec!(100.00), dec!(8)),
                (dec!(100.50), dec!(16)),
                (dec!(101.00), dec!(32)),
            ],
        );
        for direction in [Direction::SellAsterBuyLighter, Direction::SellLighterBuyAster] {
            let f64_opp = depth_priced_opportunity(
                &cfg,
                &spec,
                &math,
                direction,
                &aster,
                &lighter,
                pos_f64(pos),
                margins_f64(margins),
                false,
            )
            .expect("f64 opportunity should exist");
            let dec_opp = decimal_depth_priced_opportunity(
                &cfg, &spec, direction, &aster, &lighter, pos, margins, false,
            )
            .expect("decimal opportunity should exist");
            assert_opp_eq_f64_vs_decimal(&f64_opp, &dec_opp, direction.as_str());
        }
    }

    #[test]
    fn f64_matches_decimal_with_non_zero_position() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: dec!(2.5),
            lighter_qty: dec!(-2.5),
        };
        let margins = margins();
        for direction in [Direction::SellAsterBuyLighter, Direction::SellLighterBuyAster] {
            let aster = book(dec!(100), dec!(101));
            let lighter = book(dec!(99), dec!(100));
            let f64_opp = depth_priced_opportunity(
                &cfg,
                &spec,
                &math,
                direction,
                &aster,
                &lighter,
                pos_f64(pos),
                margins_f64(margins),
                false,
            );
            let dec_opp = decimal_depth_priced_opportunity(
                &cfg, &spec, direction, &aster, &lighter, pos, margins, false,
            );
            match (f64_opp, dec_opp) {
                (Some(f), Some(d)) => assert_opp_eq_f64_vs_decimal(&f, &d, direction.as_str()),
                (None, None) => {}
                (f64_opp, dec_opp) => panic!(
                    "{}: f64 and decimal disagree on existence: f64={:?} dec={:?}",
                    direction.as_str(),
                    f64_opp.map(|o| o.qty),
                    dec_opp.map(|o| o.qty)
                ),
            }
        }
    }

    #[test]
    fn f64_matches_decimal_min_size_near_min_notional() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: Decimal::ZERO,
            lighter_qty: Decimal::ZERO,
        };
        let margins = margins();
        let px = dec!(20.0);
        let aster = book(px - dec!(0.05), px + dec!(0.05));
        let lighter = book(px - dec!(0.10), px - dec!(0.05));
        let f64_opp = depth_priced_opportunity(
            &cfg,
            &spec,
            &math,
            Direction::SellLighterBuyAster,
            &aster,
            &lighter,
            pos_f64(pos),
            margins_f64(margins),
            true,
        );
        let dec_opp = decimal_depth_priced_opportunity(
            &cfg, &spec, Direction::SellLighterBuyAster, &aster, &lighter, pos, margins, true,
        );
        match (f64_opp, dec_opp) {
            (Some(f), Some(d)) => assert_opp_eq_f64_vs_decimal(&f, &d, "min_size"),
            (None, None) => {}
            (f64_opp, dec_opp) => panic!(
                "min_size: f64 and decimal disagree on existence: f64={:?} dec={:?}",
                f64_opp.map(|o| o.qty),
                dec_opp.map(|o| o.qty)
            ),
        }
    }

    #[test]
    fn f64_matches_decimal_thin_top_of_book_with_depth() {
        let mut cfg = test_cfg();
        cfg.arb.depth_guard.enabled = true;
        cfg.arb.depth_guard.liquidity_multiple = dec!(10);
        cfg.arb.depth_guard.max_levels = 3;
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let pos = PositionSnapshot {
            aster_qty: Decimal::ZERO,
            lighter_qty: Decimal::ZERO,
        };
        let margins = margins();
        let aster = depth_book(
            [(dec!(101), dec!(0.20)), (dec!(100.90), dec!(10))],
            [(dec!(103), dec!(10))],
        );
        let lighter = depth_book(
            [(dec!(98), dec!(10))],
            [(dec!(100), dec!(0.20)), (dec!(100.10), dec!(10))],
        );
        let f64_opp = depth_priced_opportunity(
            &cfg,
            &spec,
            &math,
            Direction::SellAsterBuyLighter,
            &aster,
            &lighter,
            pos_f64(pos),
            margins_f64(margins),
            false,
        );
        let dec_opp = decimal_depth_priced_opportunity(
            &cfg, &spec, Direction::SellAsterBuyLighter, &aster, &lighter, pos, margins, false,
        );
        match (f64_opp, dec_opp) {
            (Some(f), Some(d)) => assert_opp_eq_f64_vs_decimal(&f, &d, "thin_top"),
            (None, None) => {}
            (f64_opp, dec_opp) => panic!(
                "thin_top: f64 and decimal disagree on existence: f64={:?} dec={:?}",
                f64_opp.map(|o| o.qty),
                dec_opp.map(|o| o.qty)
            ),
        }
    }

    #[test]
    fn exposure_effect_f64_matches_decimal() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        let cases = [
            (dec!(1), dec!(-1), dec!(0.2), Direction::SellAsterBuyLighter),
            (dec!(-1), dec!(1), dec!(0.2), Direction::SellLighterBuyAster),
            (dec!(0), dec!(0), dec!(0.5), Direction::SellAsterBuyLighter),
            (dec!(2), dec!(-2), dec!(3), Direction::SellAsterBuyLighter),
            (dec!(-2), dec!(2), dec!(3), Direction::SellLighterBuyAster),
        ];
        for (a, l, q, dir) in cases {
            let pos = PositionSnapshot { aster_qty: a, lighter_qty: l };
            let dec_result = exposure_effect(pos, dir, q);
            let f64_result = exposure_effect_f64(
                a.to_f64().unwrap(),
                l.to_f64().unwrap(),
                dir,
                q.to_f64().unwrap(),
                &math,
            );
            assert_eq!(dec_result, f64_result,
                "exposure_effect mismatch for pos=({},{}) qty={} dir={}: dec={:?} f64={:?}",
                a, l, q, dir.as_str(), dec_result, f64_result);
        }
    }

    #[test]
    fn exposure_effect_f64_treats_noisy_boundary_as_flat() {
        let cfg = test_cfg();
        let spec = test_spec();
        let math = test_math(&cfg, &spec);
        assert_eq!(
            exposure_effect_f64(
                0.2,
                -0.1,
                Direction::SellAsterBuyLighter,
                0.3,
                &math,
            ),
            ExposureEffect::Flat
        );
    }

    #[test]
    fn net_mismatch_notional_f64_matches_decimal() {
        let cases = [
            (dec!(1), dec!(-1), dec!(100), dec!(101)),
            (dec!(0.5), dec!(-0.3), dec!(50), dec!(51)),
            (dec!(0), dec!(0), dec!(100), dec!(101)),
        ];
        for (a, l, bid, ask) in cases {
            let pos = PositionSnapshot { aster_qty: a, lighter_qty: l };
            let aster = book(bid, ask);
            let lighter = book(bid, ask);
            let mark = aster.mid().or_else(|| lighter.mid()).unwrap();
            let dec_result = pos.net_qty().abs() * mark;
            let f64_result = net_mismatch_notional(pos, &aster, &lighter).unwrap();
            let diff = (dec_result - f64_result).abs();
            assert!(diff < dec!(0.0000001),
                "net_mismatch drift for pos=({},{}) bid={} ask={}: dec={} f64={} diff={}",
                a, l, bid, ask, dec_result, f64_result, diff);
        }
    }

}
