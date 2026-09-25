//! The supervisor loop: runs the engine tasks, hands execution rights between them and halts
//! fail-closed. A port of orchestrator.py's `run`/`tick`/`fast_tick`/`ensure_bot` (7d47337)
//! without the process plumbing:
//! * every 250 ms (`fast_tick`): reap finished engines and consume a fresh confirmed reduce
//!   burst — extend the lease in reduce mode, or stop XEMM and promote the standby observer;
//! * every `poll_sec` (`tick`): poll the statuses, update the loss stops, decide, switch.
//!
//! Stopping an engine is its own bounded graceful drain (XEMM cancels, hedges, corrects and
//! verifies), awaited in full: there is no kill ladder. An engine that fails or outlives
//! `ENGINE_STOP_TIMEOUT` halts the bot instead of switching, and execution rights move only
//! after the previous holder has stopped and both venues show no open orders.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use rust_decimal_macros::dec;
use serde_json::{json, Map, Value};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::regime::{py_decimal, Bot, Decision, ReduceLease, Regime, Target, TakerMode, TAKER_BOT, XEMM_BOT};
use super::risk::{EquityTracker, RealizedTrades};
use super::{iso, ControllerCfg, EventLog};
use crate::config::LiveMode;
use crate::taker::arb::{ExecutionLease, ReduceSignal};
use crate::taker::pnl::write_json_atomic;

const FAST_POLL: Duration = Duration::from_millis(250);
const STATUS_TIMEOUT: Duration = Duration::from_secs(25);
/// Consecutive ticks without the required status before the network pause.
const MAX_STATUS_FAILURES: u32 = 3;
/// Consecutive good status ticks (4 x poll_sec, ~60 s) before a network pause lifts: a
/// flapping network must not resume two-leg trades that a drop can leave half-filled.
const STABLE_TICKS: u32 = 4;
/// A failed poll of the idle engine is not retried for this long.
const INACTIVE_STATUS_BACKOFF: Duration = Duration::from_secs(60);
const ORDERS_CLEAR_TIMEOUT: Duration = Duration::from_secs(5);
/// At startup, long enough for the Aster deadman countdown (`deadman_countdown_ms`, 10 s in
/// bot.toml) to cancel orders left by a crashed process.
const STARTUP_ORDERS_CLEAR_TIMEOUT: Duration = Duration::from_secs(15);
/// Above XEMM's worst-case bounded drain (~190 s: 5 quiesce + 4x2 sends + 70 + 65 + 30 verify
/// + 5 journal + 5 trip retry). Compose's stop_grace_period (460 s) covers a status tick in
/// progress (2 x STATUS_TIMEOUT) plus this for the active engine and again for the observer.
const ENGINE_STOP_TIMEOUT: Duration = Duration::from_secs(200);
/// The third spontaneous exit of the active engine, counting only exits under 10 minutes of
/// uptime, halts (no time window, as in orchestrator.py).
const CRASH_LOOP_EXITS: u32 = 3;
const SHORT_UPTIME_SEC: i64 = 600;
/// A standby observer that exits is restarted after 60, 120, 240, then 300 s.
const OBSERVER_RESTART_BASE_SEC: i64 = 60;
const OBSERVER_RESTART_MAX_SEC: i64 = 300;
const DIVERGENCE_EVENT_INTERVAL_SEC: i64 = 1800;

/// What an engine task runs as. The observer is a reduce-only taker in standby: it trades
/// only under the lease and publishes confirmed reduce bursts; a promotion turns it into the
/// active `Taker(Reduce)` in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Taker(TakerMode),
    Xemm,
    Observer,
}

impl Role {
    pub fn label(self) -> &'static str {
        match self {
            Role::Taker(TakerMode::Normal) => "taker",
            Role::Taker(TakerMode::Reduce) => "reduce_taker",
            Role::Xemm => "xemm",
            Role::Observer => "observer",
        }
    }
}

/// The channels every reduce-capable taker (observer, reduce taker) is started with.
#[derive(Clone)]
pub struct EngineIo {
    pub lease: watch::Receiver<Option<ExecutionLease>>,
    pub signals: watch::Sender<Option<ReduceSignal>>,
    /// Network pause: while set, no engine opens new exposure (taker entries, XEMM quotes);
    /// in-flight executions, hedges, recovery and shutdown carry on.
    pub paused: Arc<AtomicBool>,
}

/// What the supervisor drives: the real engines in production, fakes in tests.
pub trait Engines {
    fn spawn(&mut self, role: Role, io: &EngineIo, stop: CancellationToken) -> JoinHandle<Result<()>>;
    /// The engine's status report as JSON (`taker::status` / `livebot::status`).
    fn status(&self, bot: Bot) -> impl Future<Output = Result<Value>>;
}

/// Controller files: `bot-<M>.*`. The XEMM stem `bot-<M>` names XEMM's own journal, trip latch
/// and unclean-session marker (`bot-<M>-journal.jsonl`, `bot-<M>.trip.json`, `bot-<M>.active.json`).
pub struct Files {
    pub events: PathBuf,
    pub state: PathBuf,
    pub breaker: PathBuf,
    pub baseline: PathBuf,
    pub equity: PathBuf,
    pub xemm_stem: PathBuf,
}

impl Files {
    pub fn new(dir: &Path, market: &str) -> Self {
        let stem = format!("bot-{market}");
        let path = |suffix: &str| dir.join(format!("{stem}{suffix}"));
        Self {
            events: path(".events.jsonl"),
            state: path(".state.json"),
            breaker: path(".breaker.json"),
            baseline: path(".baseline.json"),
            equity: path(".equity.jsonl"),
            xemm_stem: path(""),
        }
    }
}

struct Task {
    role: Role,
    stop: CancellationToken,
    handle: JoinHandle<Result<()>>,
    started_at: DateTime<Utc>,
}

impl Task {
    fn running(&self) -> bool {
        !self.handle.is_finished()
    }

    /// Cancels the engine and awaits its own graceful drain.
    async fn stop(mut self) -> Result<()> {
        self.stop.cancel();
        match tokio::time::timeout(ENGINE_STOP_TIMEOUT, &mut self.handle).await {
            Ok(joined) => flatten(joined),
            Err(_) => {
                // Dropping the handle would leave it running (a parked dry run would trade on).
                self.handle.abort();
                bail!("{} did not stop within {}s", self.role.label(), ENGINE_STOP_TIMEOUT.as_secs())
            }
        }
    }

    async fn join(self) -> Result<()> {
        flatten(self.handle.await)
    }
}

fn flatten(joined: std::result::Result<Result<()>, tokio::task::JoinError>) -> Result<()> {
    joined.unwrap_or_else(|error| Err(anyhow!("engine task failed: {error}")))
}

fn error_text(result: &Result<()>) -> Value {
    result.as_ref().err().map_or(Value::Null, |error| json!(format!("{error:#}")))
}

fn slot(bot: Bot) -> usize {
    match bot {
        Bot::Taker => 0,
        Bot::Xemm => 1,
    }
}

pub struct Supervisor<E: Engines> {
    cfg: ControllerCfg,
    market: String,
    run_mode: LiveMode,
    files: Files,
    engines: E,
    events: EventLog,
    regime: Regime,
    /// The task of the engine holding execution rights (`regime.active`).
    active: Option<Task>,
    observer: Option<Task>,
    io: EngineIo,
    lease_tx: watch::Sender<Option<ExecutionLease>>,
    signal_rx: watch::Receiver<Option<ReduceSignal>>,
    /// Consume-once: a burst is used only if newer than the last one used.
    last_signal_at: Option<DateTime<Utc>>,
    leases_granted: u64,
    status_failures: u32,
    /// Set while no engine may open new exposure, because the status became unreadable.
    paused_since: Option<DateTime<Utc>>,
    stable_ticks: u32,
    backoff_until: [Option<Instant>; 2],
    active_exit_count: u32,
    observer_exit_count: u32,
    observer_retry_after: Option<DateTime<Utc>>,
    mode_started_at: DateTime<Utc>,
    switch_counts: [u64; 2],
    equity: EquityTracker,
    trades: RealizedTrades,
    equity_failures: u32,
    equity_source: Option<&'static str>,
    last_divergence_at: Option<DateTime<Utc>>,
    halted: Option<&'static str>,
    shutdown: bool,
    stop: CancellationToken,
}

impl<E: Engines> Supervisor<E> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cfg: ControllerCfg,
        market: String,
        run_mode: LiveMode,
        files: Files,
        taker_ledger: PathBuf,
        engines: E,
        mut events: EventLog,
        stop: CancellationToken,
    ) -> Self {
        let now = Utc::now();
        let (lease_tx, lease_rx) = watch::channel(None);
        let (signal_tx, signal_rx) = watch::channel(None);
        let equity = EquityTracker::load(
            &market,
            files.baseline.clone(),
            files.equity.clone(),
            cfg.max_loss_usdc,
            cfg.baseline_max_gap_hours,
            now,
            &mut events,
        );
        let trades = RealizedTrades::new(&market, now, taker_ledger, crate::live_report::inferred_journal_path(&files.xemm_stem));
        Self {
            regime: Regime::new(cfg.thresholds()),
            cfg,
            market,
            run_mode,
            files,
            engines,
            events,
            active: None,
            observer: None,
            io: EngineIo { lease: lease_rx, signals: signal_tx, paused: Arc::new(AtomicBool::new(false)) },
            lease_tx,
            signal_rx,
            last_signal_at: None,
            leases_granted: 0,
            status_failures: 0,
            paused_since: None,
            stable_ticks: 0,
            backoff_until: [None, None],
            active_exit_count: 0,
            observer_exit_count: 0,
            observer_retry_after: None,
            mode_started_at: now,
            switch_counts: [0, 0],
            equity,
            trades,
            equity_failures: 0,
            equity_source: None,
            last_divergence_at: None,
            halted: None,
            shutdown: false,
            stop,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        self.events.emit("bot_started", json!({"market": self.market, "run_mode": self.run_mode.as_str()}));
        self.trades.prime();
        // In-process engines die with the process: a crash can leave orders resting, so no
        // engine starts until both venues are verified clear.
        if let Err(status) = self.verify_orders_clear(STARTUP_ORDERS_CLEAR_TIMEOUT).await {
            self.safe_halt("startup_orders_not_clear", json!({"xemm_status": status})).await;
        }
        let mut next_tick = Instant::now();
        while !self.stopping() {
            self.fast_tick().await;
            if !self.stopping() && Instant::now() >= next_tick {
                self.tick().await;
                next_tick = Instant::now() + Duration::from_secs(self.cfg.poll_sec);
            }
            if self.events.unwritable() {
                self.shutdown = true;
            }
            tokio::select! {
                _ = tokio::time::sleep(FAST_POLL) => {}
                _ = self.stop.cancelled() => {}
            }
        }
        // Active writer first, then the observer.
        self.events.emit("bot_stopping", json!({"active_bot": self.regime.active.map(Bot::label)}));
        let active = self.stop_active("shutdown").await;
        let observer = self.stop_observer("shutdown").await;
        if let Some(reason) = self.halted {
            bail!("safe halt: {reason} (see {})", self.files.breaker.display());
        }
        if self.events.unwritable() {
            bail!("event log {} is unwritable", self.files.events.display());
        }
        active.and(observer).map_err(|error| error.context("engine stop unresolved"))
    }

    fn stopping(&self) -> bool {
        self.halted.is_some() || self.shutdown || self.stop.is_cancelled()
    }

    async fn fast_tick(&mut self) {
        self.check_exits().await;
        if self.stopping() || self.paused_since.is_some() {
            return;
        }
        let Some(signal) = self.fresh_signal() else { return };
        match (self.regime.active, self.regime.taker_mode) {
            (Some(Bot::Taker), TakerMode::Reduce) => {
                self.last_signal_at = Some(signal.timestamp);
                self.extend_lease("fresh_reduce_signal", Some(&signal));
            }
            (Some(Bot::Xemm), _) => {
                self.last_signal_at = Some(signal.timestamp);
                self.activate_reduce(signal).await;
            }
            // A burst seen by a normal taker or at bootstrap is left unconsumed.
            _ => {}
        }
    }

    fn fresh_signal(&self) -> Option<ReduceSignal> {
        let signal = self.signal_rx.borrow().clone()?;
        if signal.status != "confirmed" || signal.market != self.market || signal.samples < self.cfg.reduce_burst_min_samples {
            return None;
        }
        if self.last_signal_at.is_some_and(|last| signal.timestamp <= last) {
            return None;
        }
        let age_ms = (Utc::now() - signal.timestamp).num_milliseconds();
        (0..=self.cfg.reduce_signal_fresh_ms).contains(&age_ms).then_some(signal)
    }

    /// XEMM → reduce taker: stop XEMM (full drain), verify both venues clear, grant the lease,
    /// then promote the warm observer (or cold-start a reduce taker).
    async fn activate_reduce(&mut self, signal: ReduceSignal) {
        self.events.emit("reduce_burst_switch_start", json!({"signal": signal}));
        self.regime.clear_confirm_windows();
        if let Err(error) = self.stop_active("reduce_burst_signal").await {
            return self.safe_halt("xemm_shutdown_unresolved", json!({"error": format!("{error:#}")})).await;
        }
        if let Err(status) = self.verify_orders_clear(ORDERS_CLEAR_TIMEOUT).await {
            return self.safe_halt("xemm_orders_not_clear_for_reduce_arb", json!({"xemm_status": status, "signal": signal})).await;
        }
        if self.stopping() {
            return;
        }
        self.grant_lease("reduce_burst_signal", Some(&signal));
        if let Some(dead) = self.observer.take_if(|task| !task.running()) {
            self.on_observer_exit(dead).await;
        }
        let task = match self.observer.take() {
            Some(observer) => {
                self.events.emit("observer_promoted_to_reduce_arb", json!({"lease_id": self.regime.lease.as_ref().map(|l| &l.id)}));
                Task { role: Role::Taker(TakerMode::Reduce), ..observer }
            }
            None => {
                self.events.emit("reduce_standby_missing_starting_cold", json!({"signal": signal}));
                self.spawn(Role::Taker(TakerMode::Reduce))
            }
        };
        self.active = Some(task);
        self.set_active(Bot::Taker, TakerMode::Reduce);
    }

    async fn tick(&mut self) {
        self.check_exits().await;
        if self.stopping() {
            return;
        }
        let (taker, mut xemm) = self.poll_statuses().await;
        let required = match self.regime.active {
            Some(Bot::Taker) => taker.is_some(),
            Some(Bot::Xemm) => xemm.is_some(),
            None => taker.is_some() || xemm.is_some(),
        };
        if !required {
            // Unreadable status is almost always the network: halting cannot drain or cancel
            // without it either, so pause new exposure and wait for it to come back.
            self.status_failures += 1;
            self.stable_ticks = 0;
            self.events.emit("status_poll_failed", json!({"consecutive_failures": self.status_failures}));
            if self.status_failures >= MAX_STATUS_FAILURES && self.paused_since.is_none() {
                self.paused_since = Some(Utc::now());
                self.io.paused.store(true, Ordering::Release);
                self.events.emit("network_pause", json!({"consecutive_failures": self.status_failures}));
            }
            return;
        }
        self.status_failures = 0;
        let new_taker_trades = self.trades.poll(&mut self.events);
        if self.regime.active == Some(Bot::Taker) && self.regime.taker_mode == TakerMode::Reduce && new_taker_trades > 0 {
            self.extend_lease("reduce_trade", None);
        }
        let sample = self.record_equity(taker.as_ref(), xemm.as_ref());
        if let Some(reason) = self.equity.breach().or_else(|| self.trades.breach(self.cfg.max_loss_usdc)) {
            return self.safe_halt("pnl_breaker", json!({"breaker_reason": reason, "pnl_sample": sample})).await;
        }
        // Loss stops run on every readable status; switching waits for a stable network.
        if let Some(since) = self.paused_since {
            self.stable_ticks += 1;
            if self.stable_ticks < STABLE_TICKS {
                return;
            }
            self.paused_since = None;
            self.io.paused.store(false, Ordering::Release);
            self.events.emit("network_resume", json!({"paused_secs": (Utc::now() - since).num_seconds()}));
        }
        let decision = self.regime.decide(taker.as_ref(), xemm.as_ref(), Instant::now(), Utc::now());
        let target = match decision.target {
            Target::SafeHalt => return self.safe_halt(decision.reason, Value::Object(decision.details)).await,
            Target::Bot(bot) => bot,
        };
        if target == Bot::Xemm && xemm.is_none() {
            xemm = self.read_status(Bot::Xemm, true).await;
            match &xemm {
                None => {
                    self.events.emit("xemm_switch_deferred_no_status", json!({"reason": decision.reason}));
                    return self.write_state(taker.as_ref(), None, &decision);
                }
                Some(status) if status.get("reduce_position_only") != Some(&Value::Bool(true)) => {
                    let flag = status.get("reduce_position_only").cloned();
                    return self.safe_halt("xemm_reduce_position_only_disabled", json!({"reduce_position_only": flag})).await;
                }
                Some(_) => {}
            }
        }
        self.ensure_bot(target, decision.reason, &decision.details, decision.taker_mode.unwrap_or(TakerMode::Normal)).await;
        if self.stopping() {
            return;
        }
        self.ensure_observer_for(target).await;
        if self.stopping() {
            return;
        }
        self.write_state(taker.as_ref(), xemm.as_ref(), &decision);
    }

    /// The taker only while it holds the rights; with XEMM active XEMM is required and the idle
    /// taker is read too (for resume); at bootstrap XEMM is read only if the taker failed.
    async fn poll_statuses(&mut self) -> (Option<Value>, Option<Value>) {
        match self.regime.active {
            Some(Bot::Taker) => (self.read_status(Bot::Taker, false).await, None),
            Some(Bot::Xemm) => {
                let xemm = self.read_status(Bot::Xemm, false).await;
                (self.read_status(Bot::Taker, true).await, xemm)
            }
            None => {
                let taker = self.read_status(Bot::Taker, false).await;
                let xemm = if taker.is_none() { self.read_status(Bot::Xemm, true).await } else { None };
                (taker, xemm)
            }
        }
    }

    async fn read_status(&mut self, bot: Bot, inactive: bool) -> Option<Value> {
        let index = slot(bot);
        if inactive && self.backoff_until[index].is_some_and(|until| Instant::now() < until) {
            return None;
        }
        let failure = match tokio::time::timeout(STATUS_TIMEOUT, self.engines.status(bot)).await {
            Ok(Ok(mut status)) if status.get("market").and_then(Value::as_str) == Some(self.market.as_str()) => {
                if let Some(map) = status.as_object_mut() {
                    map.entry("bot").or_insert_with(|| json!(bot.label()));
                }
                self.backoff_until[index] = None;
                return Some(status);
            }
            Ok(Ok(status)) => json!({"error": "status for another market", "market": status.get("market")}),
            Ok(Err(error)) => json!({"error": format!("{error:#}")}),
            Err(_) => json!({"error": "timeout"}),
        };
        self.events.emit("status_read_failed", json!({"bot": bot.label(), "inactive": inactive, "failure": failure}));
        if inactive {
            self.backoff_until[index] = Some(Instant::now() + INACTIVE_STATUS_BACKOFF);
        }
        None
    }

    /// Polls XEMM's status until both venues show no open order for the market, at least
    /// once. Fail-closed: anything unreadable counts as not clear.
    async fn verify_orders_clear(&mut self, deadline: Duration) -> std::result::Result<(), Value> {
        let until = Instant::now() + deadline;
        let mut last = Value::Null;
        loop {
            if let Some(status) = self.read_status(Bot::Xemm, false).await {
                let count = |venue: &str| status.pointer(&format!("/accounts/{venue}_open_orders")).and_then(Value::as_u64);
                if count("aster") == Some(0) && count("lighter") == Some(0) {
                    return Ok(());
                }
                last = status;
            }
            if Instant::now() >= until {
                return Err(last);
            }
            tokio::time::sleep(FAST_POLL).await;
        }
    }

    /// The drawdown stop samples the taker's marked equity when its status has one, else
    /// XEMM's. The orchestrator sampled the active engine, so each switch swapped equity
    /// calculators under one baseline; this keeps one definition unless the taker status is
    /// unavailable.
    fn record_equity(&mut self, taker: Option<&Value>, xemm: Option<&Value>) -> Value {
        let now = Utc::now();
        let equity_of = |status: Option<&Value>| status.and_then(|s| py_decimal(s.pointer("/accounts/total_equity_usd")));
        let (taker_equity, xemm_equity) = (equity_of(taker), equity_of(xemm));
        if let (Some(t), Some(x)) = (taker_equity, xemm_equity) {
            let due = self.last_divergence_at.is_none_or(|at| (now - at).num_seconds() >= DIVERGENCE_EVENT_INTERVAL_SEC);
            if (t - x).abs() >= self.cfg.max_loss_usdc * dec!(0.2) && due {
                self.events.emit("equity_calc_divergence", json!({"taker_equity_usd": t, "xemm_equity_usd": x}));
                self.last_divergence_at = Some(now);
            }
        }
        let source = match (taker_equity, xemm_equity) {
            (Some(equity), _) => Some((TAKER_BOT, equity, taker)),
            (None, Some(equity)) => Some((XEMM_BOT, equity, xemm)),
            (None, None) => None,
        };
        let Some((label, equity, status)) = source else {
            if taker.is_some() || xemm.is_some() {
                self.equity_failures += 1;
                if self.equity_failures == 3 {
                    self.events.emit("equity_feed_starving", json!({"consecutive_failures": self.equity_failures}));
                }
            }
            return Value::Null;
        };
        self.equity_failures = 0;
        if let Some(previous) = self.equity_source.filter(|previous| *previous != label) {
            self.events.emit("pnl_source_switched", json!({"from_bot": previous, "to_bot": label}));
        }
        self.equity_source = Some(label);
        let accounts = status.and_then(|s| s.get("accounts")).cloned().unwrap_or(Value::Null);
        self.equity.record(equity, label, &accounts, self.regime.active.map(Bot::label), now, &mut self.events)
    }

    async fn ensure_bot(&mut self, target: Bot, reason: &'static str, details: &Map<String, Value>, mode: TakerMode) {
        let running = self.active.as_ref().is_some_and(Task::running);
        if self.regime.active == Some(target) && running && (target != Bot::Taker || self.regime.taker_mode == mode) {
            return;
        }
        self.check_exits().await;
        if self.stopping() {
            return;
        }
        let stopping_xemm_for_taker = self.regime.active == Some(Bot::Xemm) && target == Bot::Taker;
        let why = format!("switch_to_{}", target.label());
        if target == Bot::Taker {
            if let Err(error) = self.stop_observer(&why).await {
                return self.safe_halt("observer_shutdown_unresolved", json!({"error": format!("{error:#}")})).await;
            }
        }
        if let Err(error) = self.stop_active(&why).await {
            return self.safe_halt("bot_shutdown_unresolved", json!({"error": format!("{error:#}")})).await;
        }
        if stopping_xemm_for_taker {
            if let Err(status) = self.verify_orders_clear(ORDERS_CLEAR_TIMEOUT).await {
                return self.safe_halt("xemm_orders_not_clear_on_resume", json!({"xemm_status": status})).await;
            }
        }
        // A stop asked for during the drain starts nothing new.
        if self.stopping() {
            return;
        }
        let role = if target == Bot::Xemm { Role::Xemm } else { Role::Taker(mode) };
        let task = self.spawn(role);
        self.events.emit("bot_switched", json!({"bot": target.label(), "role": role.label(), "reason": reason, "details": details}));
        self.active = Some(task);
        self.regime.clear_confirm_windows();
        self.set_active(target, mode);
    }

    async fn ensure_observer_for(&mut self, target: Bot) {
        let unwanted = if !self.cfg.observer {
            Some("observer_disabled")
        } else if target != Bot::Xemm {
            Some("active_taker")
        } else {
            None
        };
        if let Some(reason) = unwanted {
            if let Err(error) = self.stop_observer(reason).await {
                self.safe_halt("observer_shutdown_unresolved", json!({"error": format!("{error:#}")})).await;
            }
            return;
        }
        if let Some(dead) = self.observer.take_if(|task| !task.running()) {
            self.on_observer_exit(dead).await;
        }
        if self.observer.is_some() || self.observer_retry_after.is_some_and(|at| Utc::now() < at) {
            return;
        }
        self.observer = Some(self.spawn(Role::Observer));
    }

    fn spawn(&mut self, role: Role) -> Task {
        let stop = CancellationToken::new();
        let handle = self.engines.spawn(role, &self.io, stop.clone());
        self.events.emit("bot_started_engine", json!({"role": role.label()}));
        Task { role, stop, handle, started_at: Utc::now() }
    }

    fn set_active(&mut self, bot: Bot, mode: TakerMode) {
        self.regime.active = Some(bot);
        self.regime.taker_mode = if bot == Bot::Taker { mode } else { TakerMode::Normal };
        self.mode_started_at = Utc::now();
        self.switch_counts[slot(bot)] += 1;
    }

    /// Stops the engine holding the rights; the reduce lease is revoked before its stop.
    async fn stop_active(&mut self, reason: &str) -> Result<()> {
        if self.regime.active == Some(Bot::Taker) && self.regime.taker_mode == TakerMode::Reduce {
            self.revoke_lease(reason);
        }
        let result = match self.active.take() {
            Some(task) => self.stop_task(task, reason).await,
            None => Ok(()),
        };
        self.regime.active = None;
        self.regime.taker_mode = TakerMode::Normal;
        result
    }

    /// An observer that already exited takes the restart backoff instead of failing the stop.
    async fn stop_observer(&mut self, reason: &str) -> Result<()> {
        match self.observer.take() {
            Some(task) if !task.running() => {
                self.on_observer_exit(task).await;
                Ok(())
            }
            Some(task) => self.stop_task(task, reason).await,
            None => Ok(()),
        }
    }

    async fn stop_task(&mut self, task: Task, reason: &str) -> Result<()> {
        let role = task.role;
        let result = task.stop().await;
        self.events.emit("bot_stopped", json!({"role": role.label(), "reason": reason, "error": error_text(&result)}));
        result
    }

    async fn check_exits(&mut self) {
        if let Some(task) = self.active.take_if(|task| !task.running()) {
            let (role, started_at) = (task.role, task.started_at);
            let reduce = role == Role::Taker(TakerMode::Reduce);
            let result = task.join().await;
            self.events.emit("bot_exited", json!({"role": role.label(), "error": error_text(&result)}));
            if reduce {
                self.revoke_lease("active_reduce_taker_exited");
            }
            let bot = self.regime.active.map_or("none", Bot::label);
            self.regime.active = None;
            self.regime.taker_mode = TakerMode::Normal;
            self.regime.clear_confirm_windows();
            if let Err(error) = result {
                let details = json!({"bot": bot, "role": role.label(), "error": format!("{error:#}"), "reduce_mode": reduce});
                return self.safe_halt("active_bot_exited_nonzero", details).await;
            }
            let uptime = (Utc::now() - started_at).num_seconds();
            self.active_exit_count = if uptime >= SHORT_UPTIME_SEC { 0 } else { self.active_exit_count + 1 };
            if self.active_exit_count >= CRASH_LOOP_EXITS {
                let details = json!({"bot": bot, "consecutive_short_exits": self.active_exit_count, "last_uptime_sec": uptime});
                return self.safe_halt("active_bot_crash_loop", details).await;
            }
        }
        if let Some(task) = self.observer.take_if(|task| !task.running()) {
            self.on_observer_exit(task).await;
        }
    }

    /// Observer exits never halt; they back off 60/120/240/300 s.
    async fn on_observer_exit(&mut self, task: Task) {
        let started_at = task.started_at;
        let result = task.join().await;
        let now = Utc::now();
        if (now - started_at).num_seconds() >= SHORT_UPTIME_SEC {
            self.observer_exit_count = 0;
        }
        self.observer_exit_count += 1;
        let retry_sec = (OBSERVER_RESTART_BASE_SEC << (self.observer_exit_count - 1).min(3)).min(OBSERVER_RESTART_MAX_SEC);
        self.observer_retry_after = Some(now + chrono::Duration::seconds(retry_sec));
        self.events.emit("observer_restart_delayed", json!({
            "error": error_text(&result), "retry_sec": retry_sec, "consecutive_exits": self.observer_exit_count,
        }));
    }

    /// A fresh lease (new id, cap from now) unless one is held; either way the expiry becomes
    /// `now + reduce_lease_sec`, capped.
    fn grant_lease(&mut self, reason: &str, signal: Option<&ReduceSignal>) {
        let now = Utc::now();
        if self.regime.lease.is_none() {
            self.leases_granted += 1;
            self.regime.lease = Some(ReduceLease {
                id: format!("reduce-{}-{}-{}", self.market, now.format("%Y%m%dT%H%M%S%.6fZ"), self.leases_granted),
                started_at: now,
                expires_at: now,
                max_expires_at: now + chrono::Duration::seconds(self.cfg.reduce_lease_max_sec),
            });
        }
        let lease = self.regime.lease.as_mut().expect("lease set above");
        lease.expires_at = (now + chrono::Duration::seconds(self.cfg.reduce_lease_sec)).min(lease.max_expires_at);
        let lease = lease.clone();
        self.publish_lease(&lease);
        self.events.emit("reduce_lease_granted", json!({
            "reason": reason, "lease_id": lease.id, "expires_at": iso(lease.expires_at),
            "max_expires_at": iso(lease.max_expires_at), "signal": signal,
        }));
    }

    /// Extends only when that moves the expiry by more than a second (never once capped).
    fn extend_lease(&mut self, reason: &str, signal: Option<&ReduceSignal>) {
        let Some(lease) = self.regime.lease.as_mut() else {
            return self.grant_lease(reason, signal);
        };
        let now = Utc::now();
        // An expired lease stays expired: the next decision hands the rights back to XEMM.
        if lease.expires_at <= now {
            return;
        }
        let desired = (now + chrono::Duration::seconds(self.cfg.reduce_lease_sec)).min(lease.max_expires_at);
        if desired <= lease.expires_at + chrono::Duration::seconds(1) {
            return;
        }
        lease.expires_at = desired;
        let lease = lease.clone();
        self.publish_lease(&lease);
        self.events.emit("reduce_lease_extended", json!({"reason": reason, "lease_id": lease.id, "expires_at": iso(lease.expires_at)}));
    }

    fn revoke_lease(&mut self, reason: &str) {
        self.lease_tx.send_replace(None);
        if let Some(lease) = self.regime.lease.take() {
            self.events.emit("reduce_lease_revoked", json!({"reason": reason, "lease_id": lease.id}));
        }
    }

    fn publish_lease(&self, lease: &ReduceLease) {
        self.lease_tx.send_replace(Some(ExecutionLease {
            market: self.market.clone(),
            lease_id: lease.id.clone(),
            expires_at: lease.expires_at,
        }));
    }

    /// Stops the active writer, then the observer, and latches the breaker file that
    /// refuses the next start until `--ack-breaker`.
    async fn safe_halt(&mut self, reason: &'static str, details: Value) {
        if self.halted.is_some() {
            return;
        }
        self.halted = Some(reason);
        self.events.emit("safe_halt", json!({"reason": reason, "details": details}));
        if let Err(error) = self.stop_active("safe_halt").await {
            self.events.emit("stop_failed", json!({"stage": "active", "error": format!("{error:#}")}));
        }
        if let Err(error) = self.stop_observer("safe_halt").await {
            self.events.emit("stop_failed", json!({"stage": "observer", "error": format!("{error:#}")}));
        }
        let breaker = json!({
            "active": true, "triggered_at": iso(Utc::now()), "market": self.market, "reason": reason,
            "details": details, "pnl": self.equity.summary(), "trades": self.trades.summary(),
        });
        if let Err(error) = write_json_atomic(&self.files.breaker, &breaker, true) {
            self.events.emit("breaker_file_write_failed", json!({"path": self.files.breaker.display().to_string(), "error": format!("{error:#}")}));
        }
        let details = match details {
            Value::Object(map) => map,
            other => Map::from_iter([("details".to_owned(), other)]),
        };
        let decision = Decision { target: Target::SafeHalt, reason, taker_mode: None, details };
        self.write_state(None, None, &decision);
    }

    /// `bot-<M>.state.json`: read by combined_pnl.py (and trade_history.py through it) and
    /// bot_stats.py (`active_bot`, `accounts.{taker,xemm}.total_equity_usd`, `pnl`) and by
    /// operators.
    fn write_state(&mut self, taker: Option<&Value>, xemm: Option<&Value>, decision: &Decision) {
        let now = Utc::now();
        let field = |status: Option<&Value>, key: &str| status.and_then(|s| s.get(key)).cloned().unwrap_or(Value::Null);
        let lease = self.regime.lease.as_ref();
        let state = json!({
            "timestamp": iso(now), "market": self.market, "run_mode": self.run_mode.as_str(),
            "active_bot": self.regime.active.map(Bot::label),
            "active_taker_mode": (self.regime.active == Some(Bot::Taker)).then(|| self.regime.taker_mode.as_str()),
            "observer": {
                "enabled": self.cfg.observer, "running": self.observer.as_ref().is_some_and(Task::running),
                "retry_after": self.observer_retry_after.map(iso),
            },
            "reduce_lease": {
                "id": lease.map(|l| &l.id), "active": self.regime.lease_active(now), "started_at": lease.map(|l| iso(l.started_at)),
                "expires_at": lease.map(|l| iso(l.expires_at)), "max_expires_at": lease.map(|l| iso(l.max_expires_at)),
            },
            "mode_age_sec": (now - self.mode_started_at).num_seconds(),
            "switch_counts": {TAKER_BOT: self.switch_counts[0], XEMM_BOT: self.switch_counts[1]},
            "decision": decision.to_json(),
            "pnl": self.equity.summary(),
            "trades": self.trades.summary(),
            "positions": {"taker": field(taker, "positions"), "xemm": field(xemm, "positions")},
            "accounts": {"taker": field(taker, "accounts"), "xemm": field(xemm, "accounts")},
        });
        if let Err(error) = write_json_atomic(&self.files.state, &state, false) {
            self.events.emit("state_write_failed", json!({"error": format!("{error:#}")}));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;

    /// How a fake engine behaves once asked to stop.
    #[derive(Clone, Copy)]
    enum OnStop {
        /// Drains for this long, then returns Ok.
        Drain(Duration),
        Fail,
        Hang,
    }

    #[derive(Clone, Default)]
    struct Shared {
        statuses: Arc<Mutex<HashMap<&'static str, Value>>>,
        spawned: Arc<Mutex<Vec<Role>>>,
        /// Set when the fake XEMM finishes its drain.
        xemm_drained: Arc<AtomicBool>,
        /// Whether XEMM had drained when a reduce taker first saw a lease.
        lease_seen_after_drain: Arc<Mutex<Option<bool>>>,
        io: Arc<Mutex<Option<EngineIo>>>,
    }

    struct Fake {
        shared: Shared,
        on_stop: HashMap<&'static str, OnStop>,
        /// Engines that return Ok on their own right after starting.
        exit_at_once: bool,
    }

    impl Engines for Fake {
        fn spawn(&mut self, role: Role, io: &EngineIo, stop: CancellationToken) -> JoinHandle<Result<()>> {
            self.shared.spawned.lock().unwrap().push(role);
            *self.shared.io.lock().unwrap() = Some(io.clone());
            let on_stop = *self.on_stop.get(role.label()).unwrap_or(&OnStop::Drain(Duration::ZERO));
            let (shared, exit_at_once) = (self.shared.clone(), self.exit_at_once);
            let mut lease = io.lease.clone();
            tokio::spawn(async move {
                if exit_at_once {
                    return Ok(());
                }
                if matches!(role, Role::Observer | Role::Taker(TakerMode::Reduce)) {
                    let shared = shared.clone();
                    tokio::spawn(async move {
                        while lease.changed().await.is_ok() {
                            if lease.borrow().is_some() {
                                let drained = shared.xemm_drained.load(Ordering::SeqCst);
                                shared.lease_seen_after_drain.lock().unwrap().get_or_insert(drained);
                            }
                        }
                    });
                }
                stop.cancelled().await;
                match on_stop {
                    OnStop::Drain(time) => {
                        tokio::time::sleep(time).await;
                        if role == Role::Xemm {
                            shared.xemm_drained.store(true, Ordering::SeqCst);
                        }
                        Ok(())
                    }
                    OnStop::Fail => Err(anyhow!("drain verification failed")),
                    OnStop::Hang => std::future::pending().await,
                }
            })
        }

        async fn status(&self, bot: Bot) -> Result<Value> {
            let key = if bot == Bot::Taker { "taker" } else { "xemm" };
            self.shared.statuses.lock().unwrap().get(key).cloned().ok_or_else(|| anyhow!("{key} status unavailable"))
        }
    }

    /// A taker margin-limited at the default clips (min available 40 - buffer 25 < 26).
    fn blocked_taker() -> Value {
        json!({"market": "HYPE", "desired_notional_usd": "13", "required_gross_edge_bps": "6", "margin_buffer_usd": "25",
               "opportunities": [], "positions": {"abs_position_notional_usd": "150", "headroom_notional_usd": "50"},
               "accounts": {"aster_available_usd": "40", "lighter_available_usd": "40", "total_equity_usd": "200"}})
    }

    fn xemm_status(open_orders: u64) -> Value {
        json!({"market": "HYPE", "reduce_position_only": true, "desired_notional_usd": "13",
               "positions": {"abs_position_notional_usd": "150"},
               "accounts": {"aster_open_orders": open_orders, "lighter_open_orders": 0, "total_equity_usd": "200"}})
    }

    fn supervisor(on_stop: &[(&'static str, OnStop)], exit_at_once: bool) -> (Supervisor<Fake>, Shared, PathBuf) {
        let dir = crate::dryrun::tests::temp_dir("bot-supervisor");
        let shared = Shared::default();
        let fake = Fake { shared: shared.clone(), on_stop: on_stop.iter().copied().collect(), exit_at_once };
        let files = Files::new(&dir, "HYPE");
        let events = EventLog::new(files.events.clone());
        let sup = Supervisor::new(ControllerCfg::default(), "HYPE".into(), LiveMode::Live, files, dir.join("trades_HYPE.jsonl"), fake, events, CancellationToken::new());
        (sup, shared, dir)
    }

    fn set_status(shared: &Shared, key: &'static str, status: Value) {
        shared.statuses.lock().unwrap().insert(key, status);
    }

    fn event_kinds(dir: &Path) -> Vec<String> {
        let text = std::fs::read_to_string(dir.join("bot-HYPE.events.jsonl")).unwrap_or_default();
        text.lines().filter_map(|line| serde_json::from_str::<Value>(line).ok()).map(|row| row["kind"].as_str().unwrap().to_owned()).collect()
    }

    fn signal(age_ms: i64) -> ReduceSignal {
        let at = Utc::now() - chrono::Duration::milliseconds(age_ms);
        ReduceSignal { timestamp: at, market: "HYPE".into(), status: "confirmed", samples: 3, window_ms: 2000, first_seen: at, last_seen: at, best: Default::default() }
    }

    fn publish(shared: &Shared, signal: ReduceSignal) {
        shared.io.lock().unwrap().as_ref().unwrap().signals.send_replace(Some(signal));
    }

    /// Bootstrap on a blocked taker → XEMM plus the standby observer.
    async fn start_on_xemm(sup: &mut Supervisor<Fake>, shared: &Shared) {
        set_status(shared, "taker", blocked_taker());
        set_status(shared, "xemm", xemm_status(0));
        sup.tick().await;
        assert_eq!(sup.regime.active, Some(Bot::Xemm));
        assert_eq!(*shared.spawned.lock().unwrap(), [Role::Xemm, Role::Observer]);
    }

    #[tokio::test(start_paused = true)]
    async fn reduce_rights_move_only_after_xemm_drained_and_orders_clear() {
        let (mut sup, shared, dir) = supervisor(&[("xemm", OnStop::Drain(Duration::from_secs(3)))], false);
        start_on_xemm(&mut sup, &shared).await;
        let burst = signal(10);
        publish(&shared, burst.clone());
        sup.fast_tick().await;
        // Let the fake engines' tasks observe the published lease.
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(sup.regime.active, Some(Bot::Taker));
        assert_eq!(sup.regime.taker_mode, TakerMode::Reduce);
        assert_eq!(*shared.lease_seen_after_drain.lock().unwrap(), Some(true), "the lease must follow XEMM's drain");
        assert_eq!(*shared.spawned.lock().unwrap(), [Role::Xemm, Role::Observer], "the warm observer is promoted, not restarted");
        let lease = sup.io.lease.borrow().clone().expect("lease granted");
        // The same burst is consumed once; a newer one extends the lease in reduce mode.
        sup.fast_tick().await;
        assert_eq!(sup.regime.active, Some(Bot::Taker));
        tokio::time::advance(Duration::from_secs(2)).await;
        let later = ReduceSignal { timestamp: Utc::now(), ..burst };
        publish(&shared, later.clone());
        sup.fast_tick().await;
        assert_eq!(sup.last_signal_at, Some(later.timestamp));
        assert_eq!(sup.io.lease.borrow().as_ref().unwrap().lease_id, lease.lease_id);
        assert!(event_kinds(&dir).contains(&"reduce_lease_granted".to_owned()));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn failed_xemm_drain_halts_without_granting_the_lease() {
        let (mut sup, shared, dir) = supervisor(&[("xemm", OnStop::Fail)], false);
        start_on_xemm(&mut sup, &shared).await;
        publish(&shared, signal(10));
        sup.fast_tick().await;
        assert_eq!(sup.halted, Some("xemm_shutdown_unresolved"));
        assert!(sup.io.lease.borrow().is_none());
        assert!(dir.join("bot-HYPE.breaker.json").exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn resting_orders_after_the_drain_block_the_lease() {
        let (mut sup, shared, dir) = supervisor(&[], false);
        start_on_xemm(&mut sup, &shared).await;
        set_status(&shared, "xemm", xemm_status(1));
        publish(&shared, signal(10));
        sup.fast_tick().await;
        assert_eq!(sup.halted, Some("xemm_orders_not_clear_for_reduce_arb"));
        assert!(sup.io.lease.borrow().is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn stale_or_foreign_bursts_are_ignored() {
        let (mut sup, shared, dir) = supervisor(&[], false);
        start_on_xemm(&mut sup, &shared).await;
        publish(&shared, signal(60_001));
        sup.fast_tick().await;
        publish(&shared, ReduceSignal { market: "ETH".into(), ..signal(10) });
        sup.fast_tick().await;
        publish(&shared, ReduceSignal { samples: 2, ..signal(10) });
        sup.fast_tick().await;
        assert_eq!(sup.regime.active, Some(Bot::Xemm));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn near_flat_in_reduce_mode_returns_to_a_normal_taker_after_revoking_the_lease() {
        let (mut sup, shared, dir) = supervisor(&[], false);
        start_on_xemm(&mut sup, &shared).await;
        publish(&shared, signal(10));
        sup.fast_tick().await;
        let mut flat = blocked_taker();
        flat["positions"]["abs_position_notional_usd"] = json!("12");
        set_status(&shared, "taker", flat);
        sup.tick().await;
        assert_eq!((sup.regime.active, sup.regime.taker_mode), (Some(Bot::Taker), TakerMode::Normal));
        assert!(sup.io.lease.borrow().is_none());
        assert_eq!(*shared.spawned.lock().unwrap(), [Role::Xemm, Role::Observer, Role::Taker(TakerMode::Normal)]);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_lease_returns_to_xemm() {
        let (mut sup, shared, dir) = supervisor(&[], false);
        start_on_xemm(&mut sup, &shared).await;
        publish(&shared, signal(10));
        sup.fast_tick().await;
        sup.regime.lease.as_mut().unwrap().expires_at = Utc::now() - chrono::Duration::seconds(1);
        sup.tick().await;
        assert_eq!(sup.regime.active, Some(Bot::Xemm));
        assert!(sup.io.lease.borrow().is_none());
        assert_eq!(shared.spawned.lock().unwrap().last(), Some(&Role::Observer));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn an_engine_that_never_stops_halts_instead_of_switching() {
        let (mut sup, shared, dir) = supervisor(&[("xemm", OnStop::Hang)], false);
        start_on_xemm(&mut sup, &shared).await;
        publish(&shared, signal(10));
        sup.fast_tick().await;
        assert_eq!(sup.halted, Some("xemm_shutdown_unresolved"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn three_short_clean_exits_halt_as_a_crash_loop() {
        let (mut sup, shared, dir) = supervisor(&[], true);
        set_status(&shared, "taker", blocked_taker());
        set_status(&shared, "xemm", xemm_status(0));
        for _ in 0..3 {
            sup.tick().await;
            tokio::task::yield_now().await;
            sup.check_exits().await;
        }
        assert_eq!(sup.halted, Some("active_bot_crash_loop"));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn unreadable_status_pauses_and_a_stable_network_resumes() {
        let (mut sup, shared, dir) = supervisor(&[], false);
        let paused = sup.io.paused.clone();
        for _ in 0..3 {
            sup.tick().await;
        }
        assert!(sup.halted.is_none() && paused.load(Ordering::Acquire), "an outage pauses, never halts");
        set_status(&shared, "taker", blocked_taker());
        for _ in 0..STABLE_TICKS - 1 {
            sup.tick().await;
        }
        shared.statuses.lock().unwrap().remove("taker");
        sup.tick().await; // a drop inside the window restarts it
        set_status(&shared, "taker", blocked_taker());
        for _ in 0..STABLE_TICKS - 1 {
            sup.tick().await;
            assert!(paused.load(Ordering::Acquire), "still inside the stability window");
        }
        sup.tick().await;
        assert!(!paused.load(Ordering::Acquire) && sup.halted.is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }
}
