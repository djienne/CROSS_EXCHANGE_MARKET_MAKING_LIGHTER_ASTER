//! Live bot orchestration (plan §1 / §12). Wires the four planes: ingest threads + watchdog
//! (market-data hot path), the strategy loop (strategy/order hot path), the execution
//! workers behind command queues (execution hot path), and account/journal/book-check (cold
//! plane). Selects the executor once from the mode.
//!
//! ## Hard safety gate
//!
//! `run` refuses to start unless `[live] enabled = true`. `mode = "live"` additionally
//! requires a single selected market and live credentials/signers. `paper` never constructs
//! a live worker at all.

use std::collections::{HashMap, VecDeque};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use tokio::sync::{mpsc, oneshot};
use tokio::sync::Notify;
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::{Config, LiveMode, MarketCfg};
use crate::connectors::{rest_book, rest_specs, EventSink};
use crate::events::{open_log_writer, write_event, write_header, Event, EventKind, RunHeader};
use crate::hotpath::clock::mono_now_ns;
use crate::hotpath::{
    run_book_check, run_watchdog, spawn_venue_thread, BookCheckParams, BookCheckTarget, ReconnectHandle,
    TradingGate, VenueRegistry, VenueTag,
};
use crate::markets::MarketSpec;
use crate::sim::SimEngine;
use crate::store::Db;
use crate::types::MarketId;

use super::account::{AccountSnapshot, AccountState};
use super::scale;
use super::exec::command::{ExecCommand, ExecEvent, HedgeCommand, CMD_QUEUE_DEPTH};
use super::exec::paper::PaperExec;
use super::exec::ExecMode;
use super::fills::AsterFill;
use super::ids::SessionId;
use super::journal::{run_journal_writer, Journal};
use super::pairs::{classify, is_eligible};
use super::strategy::{run_strategy, Strategy, TradePrint};

/// Connection-stale threshold for the watchdog (same as dry-run `live`): 60s.
const WATCHDOG_STALE_MS: i64 = 60_000;
const WATCHDOG_SCAN: std::time::Duration = std::time::Duration::from_millis(250);
/// Bounded livebot cold-recorder queue. The websocket hot path always publishes to VenueBook first;
/// this queue is only the research tape/sim side channel, so dropping cold events under recorder
/// stalls is safer than unbounded memory growth.
const COLD_INGEST_QUEUE_DEPTH: usize = 16_384;

async fn send_exec_safety(tx: &mpsc::Sender<ExecCommand>, cmd: ExecCommand, label: &'static str) {
    match tokio::time::timeout(Duration::from_secs(2), tx.send(cmd)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!("safety send failed ({label}): receiver closed: {e}"),
        Err(_) => warn!("safety send timed out ({label})"),
    }
}

async fn send_hedge_safety(tx: &mpsc::Sender<HedgeCommand>, cmd: HedgeCommand, label: &'static str) {
    match tokio::time::timeout(Duration::from_secs(2), tx.send(cmd)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!("hedge safety send failed ({label}): receiver closed: {e}"),
        Err(_) => warn!("hedge safety send timed out ({label})"),
    }
}

fn panic_payload_message(panic: &(dyn std::any::Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string())
}

/// Entry point for the `livebot` command.
///
/// In ALL modes a cold **research plane** runs alongside the bot: every ingested event is
/// recorded to a per-run tape AND fed through the deterministic `SimEngine` into a SQLite
/// results DB (opened in APPEND mode, so it is reused/recovered across stop→restart). So a
/// paper run gathers ≥ the old research run's information (replay the tape for the identical
/// report) PLUS the bot's own journal. The bot's hot planes (strategy/exec) run concurrently
/// off the lock-free `VenueBook` cells; the research plane never touches them.
pub async fn run(
    cfg: &Config,
    markets: Vec<MarketCfg>,
    secs: Option<u64>,
    mode_override: Option<LiveMode>,
    out: Option<PathBuf>,
    db_path: PathBuf,
) -> Result<()> {
    if markets.is_empty() {
        bail!("no markets selected for livebot");
    }
    // --- the hard safety gate ---
    if !cfg.live.enabled {
        bail!("livebot is disabled: set [live] enabled = true in the config to run it");
    }
    let mode = mode_override.unwrap_or(cfg.live.mode);
    let exec_mode = ExecMode::from_cfg(mode);
    if exec_mode.sends_real_orders() && markets.len() != 1 {
        bail!(
            "refusing to run mode=\"live\" with {} markets selected; real-money live mode is single-market only",
            markets.len()
        );
    }
    if exec_mode.sends_real_orders() {
        warn!(
            "livebot mode=LIVE: placing REAL orders on Aster + Lighter with REAL funds. \
             Signing is wired through Aster EVM signing and Lighter native signer FFI. Gated behind \
             enabled + mode=live + single-market selection."
        );
    }
    info!("livebot starting: mode={}, {} market(s)", mode.as_str(), markets.len());

    // --- resolve specs + classify pair eligibility ---
    let specs = rest_specs::build_market_specs_with_bases(
        &markets,
        cfg.partials.hyperliquid_min_notional,
        &cfg.live.aster.base_url,
        &cfg.live.hyperliquid.base_url,
    )
    .await?;
    let eligibility = classify_markets(&specs, cfg, exec_mode).await;
    let market_ids: Vec<MarketId> = specs.iter().map(|s| s.market_id.clone()).collect();
    let eligible_count = eligibility.values().filter(|&&e| e).count();
    let elig_basis = if exec_mode.sends_real_orders() {
        cfg.live.partials.policy.as_str()
    } else {
        "paper (all non-degenerate pairs)"
    };
    info!("pair eligibility: {eligible_count}/{} markets tradeable — {elig_basis}", specs.len());
    if eligible_count == 0 {
        warn!("no eligible pairs under the partial policy — the bot will quote nothing");
    }

    // --- market-data hot path: registry (with strategy wakeup + dirty bitset) + ingest threads + watchdog ---
    let wake = Arc::new(Notify::new());
    let dirty = Arc::new(crate::hotpath::dirty::DirtyMarkets::new(market_ids.len()));
    let registry = Arc::new(VenueRegistry::with_wake_and_dirty(&market_ids, wake.clone(), dirty.clone()));
    let gate = Arc::new(TradingGate::new());
    let shutdown = CancellationToken::new();

    let (ingest_tx, ingest_rx) = mpsc::channel::<(MarketId, EventKind)>(COLD_INGEST_QUEUE_DEPTH);
    let cold_drop_count = Arc::new(AtomicU64::new(0));
    let ingest_sink = EventSink::lossy(ingest_tx, cold_drop_count.clone());
    let mut reconnect_map: HashMap<(MarketId, VenueTag), ReconnectHandle> = HashMap::new();
    let mut venue_handles = Vec::new();
    let spec_by_id: HashMap<MarketId, &MarketSpec> = specs.iter().map(|s| (s.market_id.clone(), s)).collect();
    let mut core_hint = 0usize;
    for m in &markets {
        let id = m.id();
        let scale = if cfg.live.quote.use_hot_integer_math {
            spec_by_id.get(&id).map(|s| scale::MarketScale::from_spec(s))
        } else {
            None
        };
        for (venue, symbol) in [
            (VenueTag::Aster, m.aster_symbol.to_lowercase()),
            (
                VenueTag::Hyperliquid,
                spec_by_id
                    .get(&id)
                    .map(|s| format!("{}:{}", s.lighter_market_id, s.hl_coin))
                    .unwrap_or_else(|| format!("0:{}", m.hl_coin)),
            ),
        ] {
            let cell = registry.cell(&id, venue).expect("registry has every cell");
            let handle = ReconnectHandle::new();
            let notify = handle.notify();
            reconnect_map.insert((id.clone(), venue), handle);
            venue_handles.push(spawn_venue_thread(
                venue, symbol, id.clone(), ingest_sink.clone(), cell, notify, shutdown.clone(), Some(core_hint),
                scale.clone(),
            ));
            core_hint += 1;
        }
    }
    drop(ingest_sink);

    let book_check_reconnect = reconnect_map.clone();
    let watchdog_stop = Arc::new(AtomicBool::new(false));
    let watchdog_handle = {
        let (reg, g, stop) = (registry.clone(), gate.clone(), watchdog_stop.clone());
        let book_stale_ms = cfg.simulation.max_book_staleness_ms;
        thread::Builder::new()
            .name("livebot-watchdog".into())
            .spawn(move || run_watchdog(reg, g, reconnect_map, WATCHDOG_STALE_MS, book_stale_ms, WATCHDOG_SCAN, stop))
            .expect("spawn watchdog")
    };

    let book_check_handle = if cfg.book_check.enabled {
        let reg = registry.clone();
        let targets: Vec<BookCheckTarget> = markets
            .iter()
            .flat_map(|m| {
                let id = m.id();
                [
                    BookCheckTarget { market: id.clone(), venue: VenueTag::Aster, symbol: m.aster_symbol.to_uppercase() },
                    BookCheckTarget {
                        market: id.clone(),
                        venue: VenueTag::Hyperliquid,
                        symbol: spec_by_id
                            .get(&id)
                            .map(|s| s.lighter_market_id.to_string())
                            .unwrap_or_else(|| "0".into()),
                    },
                ]
            })
            .collect();
        let params = BookCheckParams {
            tolerance_bps: cfg.book_check.tolerance_bps,
            consecutive_breaches: cfg.book_check.consecutive_breaches,
            depth_limit: cfg.book_check.depth_limit,
            interval: std::time::Duration::from_secs(cfg.book_check.interval_secs.max(1)),
            max_quote_staleness_ms: cfg.simulation.max_book_staleness_ms,
            max_concurrent_requests: cfg.book_check.max_concurrent_requests,
            max_rest_snapshot_age_ms: cfg.book_check.max_rest_snapshot_age_ms,
            aster_base_url: cfg.live.aster.base_url.clone(),
            hl_base_url: cfg.live.hyperliquid.base_url.clone(),
        };
        let sd = shutdown.clone();
        Some(
            thread::Builder::new()
                .name("livebot-book-check".into())
                .spawn(move || run_book_check(reg, targets, book_check_reconnect, params, sd))
                .expect("spawn book-check"),
        )
    } else {
        None
    };

    // --- cold plane: account state + journal ---
    let account = AccountState::new(cfg.live.max_unhedged_notional_usd);
    let (journal, jrx) = Journal::channel();
    // Journal path is derived from --db so a SEPARATE run (e.g. a live HYPE session alongside the
    // always-on multi-pair paper dry run) never clobbers the other's journal. db `runs/x.sqlite`
    // → journal `runs/x-journal.jsonl`.
    let journal_path = {
        let stem = db_path.file_stem().and_then(|s| s.to_str()).unwrap_or("livebot");
        let dir = db_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("runs"));
        std::fs::create_dir_all(&dir).ok();
        dir.join(format!("{stem}-journal.jsonl"))
    };
    let journal_task = {
        match std::fs::OpenOptions::new().create(true).append(true).open(&journal_path) {
            Ok(f) => Some(tokio::spawn(run_journal_writer(jrx, std::io::BufWriter::new(f)))),
            Err(e) => {
                warn!("could not open journal {}: {e}; journaling to a sink", journal_path.display());
                None
            }
        }
    };

    // --- execution plane: bounded command queues ---
    let (exec_tx, exec_rx) = mpsc::channel::<ExecCommand>(CMD_QUEUE_DEPTH);
    // Priority lane for acked cancels + flattens (see exec::command::is_priority_cmd):
    // depth mirrors the strategy's EXEC_CANCEL_RESERVE. run() keeps `exec_prio_tx` alive
    // for its whole lifetime so the worker's priority arm never closes early.
    let (exec_prio_tx, exec_prio_rx) = mpsc::channel::<ExecCommand>(64);
    let (hedge_tx, hedge_rx) = mpsc::channel::<HedgeCommand>(CMD_QUEUE_DEPTH);
    let (events_tx, events_rx) = mpsc::channel::<ExecEvent>(CMD_QUEUE_DEPTH);
    let (maker_fill_tx, maker_fill_rx) = mpsc::channel::<AsterFill>(256);
    let (trade_tx, trade_rx) = mpsc::channel::<TradePrint>(1024);

    // --- circuit-breaker trip latch (persistent across restarts) ---
    // If a prior run tripped the cumulative-loss breaker it left a latch file next to this run's DB.
    // Refuse to start (bail) until an operator clears it (scripts/reset_breaker.py) — checked BEFORE
    // any live execution setup so a tripped bot can never resume trading. No-op for a fresh db stem.
    super::breaker::check_startup(&db_path)?;

    // --- bootstrap + execution/cold planes (mode-specific) ---
    // paper: synthesize a flat snapshot; the simulated worker fabricates acks/fills.
    // live: real workers + the account reconciler (clean-start + cold backstop) + the Aster
    // user (fill) stream feeding the maker-fill channel. The initial reconcile gates clean-start.
    let mut aux_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let (worker_task, clean_start, stream_liveness) = if exec_mode.sends_real_orders() {
        setup_live_planes(
            cfg, &specs, &account, exec_rx, exec_prio_rx, hedge_rx, events_tx, maker_fill_tx, shutdown.clone(), &mut aux_tasks,
        )
        .await?
    } else {
        let mut snap = AccountSnapshot::empty();
        snap.source_ts_ns = mono_now_ns();
        account.publish(snap);
        (tokio::spawn(run_paper_workers(exec_rx, exec_prio_rx, hedge_rx, events_tx)), true, None)
    };
    // (Startup cancel-all + clean-start verification now happen inside `setup_live_planes`
    // BEFORE the initial reconcile via `Reconciler::ensure_clean_start`, so the bot can never
    // begin quoting while stray prior-run orders still rest. The old fire-and-forget here was
    // racy on a fast startup.)

    // --- strategy ---
    let session = SessionId::random();
    // The global TradingGate stays wired to the watchdog (reconnect nudging + the OPEN/CLOSED
    // gauge log); the strategy gates per-market off each pair's own feed freshness, so one
    // stale feed no longer halts quoting on every pair.
    let mut strat = Strategy::new(
        cfg.clone(), &specs, &eligibility, registry.clone(), account.clone(),
        journal.clone(), session, exec_tx.clone(), hedge_tx.clone(), exec_mode,
    );
    strat.set_exec_prio_lane(exec_prio_tx.clone());
    if clean_start {
        strat.mark_clean_start();
    }
    // Seed predicted positions from the startup snapshot (published by the initial reconcile in
    // setup_live_planes). Without this, a non-neutral restart froze quoting forever with the
    // imbalance unhedged: predicted started empty, so the orphan cross-check treated every
    // snapshot as a transient venue read. No-op in paper; refuses stale/absent snapshots.
    strat.adopt_reported_positions(mono_now_ns());
    // Arm the cumulative-loss circuit breaker: it halts via this same shutdown token and persists a
    // trip latch at this run's per-db path (the startup guard above reads the same path). Inert
    // unless live.circuit_breaker.enabled and running live.
    strat.arm_circuit_breaker(super::breaker::trip_path(&db_path), shutdown.clone());
    // In-memory trip backstop: guarantees a nonzero exit at shutdown even if the persistent
    // latch write fails (unwritable runs/ dir) — see the shutdown check at the end of run().
    let breaker_tripped_flag = Arc::new(AtomicBool::new(false));
    strat.set_trip_flag(breaker_tripped_flag.clone());
    strat.set_dirty(dirty);
    if let Some(ls) = stream_liveness {
        strat.set_user_stream(ls); // freeze quoting if the Aster fill stream silently dies
    }
    // --- strategy ---
    // Spawned on a DEDICATED OS thread with its own single-threaded tokio runtime, so the
    // strategy loop's latency-critical wake/reprice/fill→hedge path is isolated from the
    // reconciler, user stream, journal writer, and exec workers on the main runtime. Core-
    // pinned to the next available core after the ingest threads.
    let strat_core_hint = core_hint;
    let strat_shutdown = shutdown.clone();
    let (strat_done_tx, mut strat_done_rx) = oneshot::channel::<std::result::Result<(), String>>();
    let strat_handle = thread::Builder::new()
        .name("livebot-strategy".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                crate::hotpath::maybe_pin_core(Some(strat_core_hint));
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("strategy runtime");
                rt.block_on(run_strategy(strat, wake.clone(), events_rx, maker_fill_rx, trade_rx, strat_shutdown));
            }))
            .map_err(|panic| panic_payload_message(panic.as_ref()));
            let _ = strat_done_tx.send(result);
        })
        .expect("spawn strategy thread");

    // --- cold research plane: record the tape + drive the SimEngine -> SQLite (append) ---
    // Runs on a DEDICATED OS thread so JSONL/SimEngine I/O cannot steal tokio timeslices
    // from the strategy loop. Stops when all ingest_tx senders are dropped (venue thread exit).
    let run_id = Uuid::new_v4().to_string();
    let started_at = Utc::now();
    let tape_path = out.unwrap_or_else(|| {
        PathBuf::from(format!("runs/livebot-{}.jsonl.zst", started_at.format("%Y%m%dT%H%M%SZ")))
    });
    if let Some(p) = tape_path.parent() {
        if !p.as_os_str().is_empty() {
            std::fs::create_dir_all(p).ok();
        }
    }
    let mut writer = open_log_writer(&tape_path)?;
    let mode_tag = format!("livebot-{}", mode.as_str());
    let header = RunHeader {
        run_id: run_id.clone(),
        started_at,
        mode: mode_tag.clone(),
        code_version: env!("CARGO_PKG_VERSION").to_string(),
        config: cfg.clone(),
        market_specs: specs.clone(),
    };
    write_header(&mut writer, &header)?;
    let mut db = Db::open(&db_path)?;
    db.insert_run(&run_id, started_at, &mode_tag, tape_path.to_str(), env!("CARGO_PKG_VERSION"), &serde_json::to_string(cfg)?)?;
    for s in &specs {
        db.insert_market(s)?;
    }
    let engine = SimEngine::new(cfg.clone(), specs.clone())?;

    info!(
        "livebot running: bot + research recording -> {} (results db {}). Ctrl-C to stop.",
        tape_path.display(), db_path.display()
    );

    let cold_cfg = cfg.clone();
    let cold_tape = tape_path.clone();
    let cold_db_path = db_path.clone();
    let cold_run_id = run_id;
    let cold_handle = thread::Builder::new()
        .name("cold-recorder".into())
        .spawn(move || {
            run_cold_recorder(
                ingest_rx, trade_tx, writer, engine, db,
                cold_cfg, cold_tape, cold_db_path, cold_run_id, started_at, cold_drop_count,
            )
        })
        .expect("spawn cold-recorder");

    // --- main orchestrator: wait for ctrl-c, deadline, or an internal safety halt ---
    let deadline = secs.map(|s| Instant::now() + Duration::from_secs(s));
    let mut strategy_done_seen = false;
    let mut strategy_error: Option<anyhow::Error> = None;
    tokio::select! {
        _ = shutdown.cancelled() => {
            warn!("internal shutdown requested: coordinating safety shutdown");
        }
        strat_result = &mut strat_done_rx => {
            strategy_done_seen = true;
            match strat_result {
                Ok(Ok(())) if shutdown.is_cancelled() => {}
                Ok(Ok(())) => {
                    warn!("strategy thread exited unexpectedly; coordinating safety shutdown");
                    strategy_error = Some(anyhow::anyhow!("strategy thread exited unexpectedly"));
                    shutdown.cancel();
                }
                Ok(Err(msg)) => {
                    warn!("strategy thread panicked: {msg}; coordinating safety shutdown");
                    strategy_error = Some(anyhow::anyhow!("strategy thread panicked: {msg}"));
                    shutdown.cancel();
                }
                Err(_) => {
                    warn!("strategy thread supervision channel closed; coordinating safety shutdown");
                    strategy_error = Some(anyhow::anyhow!("strategy thread supervision channel closed"));
                    shutdown.cancel();
                }
            }
        }
        _ = async {
            match deadline {
                Some(d) => tokio::time::sleep_until(d).await,
                None => std::future::pending::<()>().await,
            }
        } => {
            info!("duration elapsed: shutting down");
            shutdown.cancel();
        }
        _ = tokio::signal::ctrl_c() => {
            info!("ctrl-c: shutting down");
            shutdown.cancel();
        }
    }

    // --- shutdown: stop the bot's planes, then let cold recorder finalize ---
    // Stop strategy/reconcile/userstream from creating new work before cancelling orders.
    shutdown.cancel();
    if cfg.live.shutdown_cancel_all {
        send_exec_safety(&exec_tx, ExecCommand::CancelAllBot, "shutdown CancelAllBot").await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Wait (bounded) for the strategy's shutdown fill-drain before stopping the workers:
    // a fill queued at ctrl-c raced the old immediate worker Shutdown and could be dropped
    // unhedged. The strategy's own drain is capped at 3s; a panic fires the supervision
    // channel immediately (the send is outside the unwind), so crash paths don't wait.
    if !strategy_done_seen {
        match tokio::time::timeout(Duration::from_secs(10), &mut strat_done_rx).await {
            Ok(res) => {
                strategy_done_seen = true;
                match res {
                    Ok(Ok(())) => {}
                    Ok(Err(msg)) => {
                        warn!("strategy thread panicked: {msg}");
                        if strategy_error.is_none() {
                            strategy_error = Some(anyhow::anyhow!("strategy thread panicked: {msg}"));
                        }
                    }
                    Err(_) => {
                        if strategy_error.is_none() {
                            strategy_error =
                                Some(anyhow::anyhow!("strategy thread supervision channel closed"));
                        }
                    }
                }
            }
            Err(_) => {
                warn!("strategy did not stop within 10s of shutdown; stopping workers anyway");
            }
        }
    }
    send_exec_safety(&exec_tx, ExecCommand::Shutdown, "exec Shutdown").await;
    send_hedge_safety(&hedge_tx, HedgeCommand::Shutdown, "hedge Shutdown").await;
    watchdog_stop.store(true, Ordering::Release);
    // Joining venue threads drops their ingest_tx senders → cold recorder's recv() returns
    // None → cold thread drains, finalizes SimEngine, and writes the report.
    for h in venue_handles {
        let _ = h.join();
    }
    let _ = watchdog_handle.join();
    if let Some(h) = book_check_handle {
        let _ = h.join();
    }
    match cold_handle.join() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            warn!("cold recorder error: {e:#}");
            return Err(e);
        }
        Err(panic) => {
            let msg = panic
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "unknown panic".to_string());
            warn!("cold recorder thread panicked: {msg}");
            return Err(anyhow::anyhow!("cold recorder panicked: {msg}"));
        }
    }
    if let Err(panic) = strat_handle.join() {
        let msg = panic_payload_message(panic.as_ref());
        warn!("strategy thread panicked outside supervisor: {msg}");
        if strategy_error.is_none() {
            strategy_error = Some(anyhow::anyhow!("strategy thread panicked: {msg}"));
        }
    }
    if !strategy_done_seen {
        match strat_done_rx.try_recv() {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => {
                warn!("strategy thread panicked: {msg}");
                if strategy_error.is_none() {
                    strategy_error = Some(anyhow::anyhow!("strategy thread panicked: {msg}"));
                }
            }
            Err(oneshot::error::TryRecvError::Empty) => {}
            Err(oneshot::error::TryRecvError::Closed) => {
                if strategy_error.is_none() {
                    strategy_error = Some(anyhow::anyhow!("strategy thread supervision channel closed"));
                }
            }
        }
    }
    let _ = worker_task.await;
    for h in aux_tasks {
        let _ = h.await;
    }
    drop(journal);
    if let Some(j) = journal_task {
        let _ = j.await;
    }

    if let Some(e) = strategy_error {
        return Err(e);
    }
    // A circuit-breaker trip rides the graceful-shutdown path above; without this guard it
    // exits 0 — indistinguishable from a clean stop — and the supervisor restarts the bot
    // straight into the startup latch (observed 2026-07-04). The startup guard barred any
    // pre-existing latch, so "latch exists at shutdown" ⇔ "the breaker fired THIS run".
    if exec_mode.sends_real_orders() {
        super::breaker::check_shutdown(&db_path)?;
    }
    // In-memory backstop for the same trip: if the persistent latch write FAILED at trip time
    // (unwritable runs/ dir), check_shutdown above sees no file and passes — and the supervisor
    // would restart straight into trading. The strategy sets this flag before attempting the
    // write, so re-attempt the latch best-effort and exit nonzero regardless.
    if breaker_tripped_flag.load(Ordering::Acquire) {
        let trip = super::breaker::trip_path(&db_path);
        if !trip.exists() {
            let rec = super::breaker::TripRecord {
                ts_utc: Utc::now().to_rfc3339(),
                market: specs.first().map(|s| s.market_id.0.clone()).unwrap_or_default(),
                baseline_usd: Default::default(),
                equity_usd: Default::default(),
                loss_usd: Default::default(),
                limit_usd: Default::default(),
                reason: "circuit breaker tripped this run; latch rewritten at shutdown \
                         (original write failed)"
                    .to_string(),
            };
            if let Err(e) = super::breaker::write_trip(&trip, &rec) {
                warn!("failed to re-write circuit-breaker trip latch at shutdown ({}): {e:#}", trip.display());
            }
        }
        bail!("circuit breaker tripped during this run (in-memory flag); exiting nonzero so the supervisor halts");
    }

    info!("livebot stopped. research tape -> {} ; results db -> {}", tape_path.display(), db_path.display());
    Ok(())
}

/// Cold research recorder: ingest events → JSONL tape + SimEngine → SQLite. Runs on a
/// dedicated OS thread with its own single-threaded tokio runtime, so JSONL writes and
/// SimEngine processing cannot steal timeslices from the main strategy runtime.
/// Exits when all `ingest_tx` senders are dropped (venue threads stopped), then drains
/// the buffer, finalizes the SimEngine, and generates the research report.
fn run_cold_recorder(
    ingest_rx: mpsc::Receiver<(MarketId, EventKind)>,
    trade_tx: mpsc::Sender<TradePrint>,
    mut writer: crate::events::LogWriter,
    mut engine: SimEngine,
    mut db: Db,
    cfg: Config,
    tape_path: PathBuf,
    db_path: PathBuf,
    run_id: String,
    started_at: DateTime<Utc>,
    cold_drop_count: Arc<AtomicU64>,
) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        let delay_ms = cfg.simulation.hedge_latency_buckets_ms.iter().copied().max().unwrap_or(1000) + 500;
        let delay = chrono::Duration::milliseconds(delay_ms);
        let mut buffer: VecDeque<Event> = VecDeque::new();
        let mut seq: u64 = 0;
        let mut last_ts = started_at;
        let mut released_ts = started_at;
        let mut tick = tokio::time::interval_at(
            Instant::now() + Duration::from_millis(100),
            Duration::from_millis(100),
        );
        let mut ingest_rx = ingest_rx;
        let mut last_drop_log = 0u64;

        loop {
            tokio::select! {
                msg = ingest_rx.recv() => {
                    match msg {
                        Some((market, kind)) => {
                            if let EventKind::AsterAggTrade { price, qty, buyer_is_maker, .. } = &kind {
                                let _ = trade_tx.try_send(TradePrint {
                                    market: market.clone(), price: *price, qty: *qty, buyer_is_maker: *buyer_is_maker,
                                });
                            }
                            let now = Utc::now().max(last_ts);
                            last_ts = now;
                            let ev = Event { seq, local_recv_ts: now, market, kind };
                            seq += 1;
                            write_event(&mut writer, &ev)?;
                            buffer.push_back(ev);
                        }
                        None => break,
                    }
                }
                _ = tick.tick() => {
                    let dropped = cold_drop_count.load(Ordering::Relaxed);
                    if dropped != last_drop_log {
                        warn!(
                            "cold recorder dropped {} live ingest events (bounded queue depth {})",
                            dropped - last_drop_log,
                            COLD_INGEST_QUEUE_DEPTH
                        );
                        last_drop_log = dropped;
                    }
                    let cutoff = Utc::now() - delay;
                    released_ts = release_until(&mut buffer, cutoff, &mut engine, &mut db, released_ts)?;
                    writer.flush().ok();
                }
            }
        }

        let far_future = last_ts + chrono::Duration::seconds(3600);
        released_ts = release_until(&mut buffer, far_future, &mut engine, &mut db, released_ts)?;
        writer.finish()?;
        engine.finalize(released_ts.max(last_ts), &mut db)?;
        let dropped = cold_drop_count.load(Ordering::Relaxed);
        if dropped > 0 {
            warn!("cold recorder finalized after dropping {dropped} live ingest events");
        }
        info!("cold recorder finalized: tape -> {} ; results db -> {}", tape_path.display(), db_path.display());
        let out_dir = db_path.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from("."));
        crate::report::generate(&db_path, Some(run_id), &out_dir)?;
        Ok(())
    })
}

/// Release buffered events with `local_recv_ts <= cutoff` into the SimEngine, in order
/// (ported from the former `live` command). Returns the timestamp of the last released event.
fn release_until(
    buffer: &mut VecDeque<Event>,
    cutoff: DateTime<Utc>,
    engine: &mut SimEngine,
    db: &mut Db,
    mut released_ts: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
    while let Some(front) = buffer.front() {
        if front.local_recv_ts > cutoff {
            break;
        }
        let ev = buffer.pop_front().unwrap();
        released_ts = ev.local_recv_ts;
        engine.on_event(&ev, db)?;
    }
    Ok(released_ts)
}

/// Classify every market's pair eligibility against a REST-fetched HL reference mid.
///
/// The strict Class-A-only filter is a REAL-MONEY orphan-leg safety: it bars pairs where a
/// sub-minimum partial Aster fill could be un-hedgeable on HL (plan §7). In **paper** mode
/// there is no real orphan risk — `PaperExec` always fills the hedge — so paper quotes EVERY
/// non-degenerate pair (the everyday "dry-run on all pairs"). In **live** mode the strict
/// policy applies (with the `accumulate_sub_min` fallback noted below).
async fn classify_markets(specs: &[MarketSpec], cfg: &Config, exec_mode: ExecMode) -> HashMap<MarketId, bool> {
    use super::pairs::PairClass;
    let live = exec_mode.sends_real_orders();
    // Sub-min handling IS implemented now (a sub-min Aster partial ACCUMULATES into pending
    // inventory and hedges on HL once the net clears the minimum; a genuinely stuck residual is
    // flattened reduce-only on Aster, and the reconciler backstop neutralizes anything else), so a
    // Class-B pair's sub-min partial can no longer orphan. `accumulate_sub_min` therefore safely
    // admits Class A and B; `strict` still restricts to Class A.
    let effective_policy = cfg.live.partials.policy;
    let client = rest_book::client().ok();
    let mut out = HashMap::new();
    for s in specs {
        let ref_px = match &client {
            Some(c) => rest_book::fetch_lighter_book_from_base(
                c,
                &cfg.live.hyperliquid.base_url,
                s.lighter_market_id,
                cfg.book_check.depth_limit,
            )
                .await
                .ok()
                .and_then(|b| b.mid()),
            None => None,
        };
        let eligible = match ref_px {
            Some(px) => {
                let c = classify(s, px, cfg.quote.desired_notional);
                // Live: strict orphan-leg safety. Paper: quote anything non-degenerate.
                let ok = if live { is_eligible(c.class, effective_policy) } else { c.class != PairClass::D };
                info!("  {} class {} (aster_min_fill {}, hl_min_hedge {}) -> {}", s.market_id, c.class.as_str(), c.aster_min_fill_qty, c.hl_min_hedge_qty, if ok { "eligible" } else { "EXCLUDED" });
                ok
            }
            None => {
                warn!("  {} reference price unavailable -> EXCLUDED (conservative)", s.market_id);
                false
            }
        };
        out.insert(s.market_id.clone(), eligible);
    }
    out
}

/// Paper executor task: one loop draining both command queues into the event channel.
async fn run_paper_workers(
    mut exec_rx: mpsc::Receiver<ExecCommand>,
    mut exec_prio_rx: mpsc::Receiver<ExecCommand>,
    mut hedge_rx: mpsc::Receiver<HedgeCommand>,
    events_tx: mpsc::Sender<ExecEvent>,
) {
    let paper = PaperExec::new();
    loop {
        tokio::select! {
            biased;
            cmd = exec_prio_rx.recv() => match cmd {
                Some(c) => {
                    let stop = matches!(c, ExecCommand::Shutdown);
                    for ev in paper.on_exec_command(c) {
                        let _ = events_tx.send(ev).await;
                    }
                    if stop { break; }
                }
                None => break,
            },
            cmd = exec_rx.recv() => match cmd {
                Some(c) => {
                    let stop = matches!(c, ExecCommand::Shutdown);
                    for ev in paper.on_exec_command(c) {
                        let _ = events_tx.send(ev).await;
                    }
                    if stop { break; }
                }
                None => break,
            },
            cmd = hedge_rx.recv() => match cmd {
                Some(c) => {
                    let stop = matches!(c, HedgeCommand::Shutdown);
                    for ev in paper.on_hedge_command(c) {
                        let _ = events_tx.send(ev).await;
                    }
                    if stop { break; }
                }
                None => break,
            },
        }
    }
}

/// Build + spawn ALL live planes (plan §2/§4/§6): the venue workers, the account reconciler
/// (initial reconcile for clean-start + a cold backstop loop), and the Aster user (fill) stream.
/// Real signing is wired from `aster.env`/`lighter.env` — reached ONLY under `mode = "live"`.
/// Roles are derived from the keys, not the env field names (see [`super::exec::creds`]).
/// Returns the worker task and `clean_start = true`
/// (quoting is then still gated per-market on feed freshness and position reconciliation).
#[allow(clippy::too_many_arguments)]
async fn setup_live_planes(
    cfg: &Config,
    specs: &[MarketSpec],
    account: &AccountState,
    exec_rx: mpsc::Receiver<ExecCommand>,
    exec_prio_rx: mpsc::Receiver<ExecCommand>,
    hedge_rx: mpsc::Receiver<HedgeCommand>,
    events_tx: mpsc::Sender<ExecEvent>,
    maker_fill_tx: mpsc::Sender<AsterFill>,
    shutdown: CancellationToken,
    aux: &mut Vec<tokio::task::JoinHandle<()>>,
) -> Result<(tokio::task::JoinHandle<()>, bool, Option<Arc<super::userstream::StreamLiveness>>)> {
    use std::path::Path;

    use super::exec::aster::{run_aster_worker, AsterRest};
    use super::exec::creds::{AsterCreds, LighterCreds};
    use super::exec::hyperliquid::{run_hl_worker, HlExchange};
    use super::exec::sign::{AsterSigner, EvmAsterSigner};
    use super::reconcile::Reconciler;
    use super::scale::MarketScale;
    use super::userstream::{run_aster_user_stream, StreamLiveness};

    // Load + role-resolve credentials; build the signers once (shared across clients).
    let aster_env = std::env::var("ASTER_ENV_PATH").unwrap_or_else(|_| "aster.env".into());
    let hl_env = std::env::var("LIGHTER_ENV_PATH").unwrap_or_else(|_| "lighter.env".into());
    let acreds = AsterCreds::load(Path::new(&aster_env))?;
    let hcreds = LighterCreds::load(Path::new(&hl_env))?;
    let aster_signer: Arc<dyn AsterSigner> = Arc::new(EvmAsterSigner::new(acreds.user, acreds.signer, acreds.key)?);

    // Per-market wire data shared by all Aster/HL client instances.
    let mut scales: HashMap<MarketId, (MarketScale, String)> = HashMap::new();
    for s in specs {
        scales.insert(s.market_id.clone(), (MarketScale::from_spec(s), s.aster_symbol.clone()));
    }
    for s in specs {
        if s.lighter_market_id == 0 {
            anyhow::bail!(
                "Lighter market id not resolved for market {} (symbol {}); refusing to start live",
                s.market_id.0,
                s.hl_coin
            );
        }
    }
    let new_aster = || {
        // STP omitted by default (our bid<ask never self-cross under GTX).
        AsterRest::new(
            cfg.live.aster.base_url.clone(),
            aster_signer.clone(),
            scales.clone(),
            cfg.live.aster.deadman_countdown_ms,
            cfg.live.aster.rate_limit_backoff_ms,
            cfg.live.aster.effective_max_rest_requests_per_minute(),
            None,
        )
    };
    let signers_dir = Path::new(&cfg.live.hyperliquid.signers_dir);
    let hedge = HlExchange::new_lighter(
        cfg.live.hyperliquid.base_url.clone(),
        signers_dir,
        hcreds,
        specs,
        cfg.live.hyperliquid.fill_timeout_ms,
        cfg.live.hyperliquid.ws_account_max_age_ms,
    )
    .await?;

    // Separate client instances per plane (each is a cheap reqwest client + shared signer Arc):
    // writes (worker), reads (reconciler), listenKey+WS (user stream).
    let worker_aster = new_aster()?;
    let worker_hl = hedge.clone();
    let recon = Reconciler::new(new_aster()?, hedge.clone(), specs, cfg.simulation.max_book_staleness_ms);
    let stream_aster = new_aster()?;
    let mut sym_to_market: HashMap<String, MarketId> = HashMap::new();
    for s in specs {
        sym_to_market.insert(s.aster_symbol.to_uppercase(), s.market_id.clone());
    }

    // Pre-warm the worker connections (establish TLS now, off the hot path) so the FIRST real
    // order / hedge doesn't pay a handshake — latency matters most on the very first fill.
    aux.extend(worker_hl.start_private_streams(shutdown.clone()));
    let _ = worker_aster.balance().await;
    for s in specs {
        worker_hl
            .wait_ready(&s.market_id, Duration::from_secs(15))
            .await
            .map_err(|e| anyhow::anyhow!("Lighter websocket warmup failed for {}: {e:#}", s.market_id.0))?;
    }
    let _ = worker_hl.clearinghouse_state().await;

    // LEVERAGE GATE: ensure REAL venue leverage == 1 on BOTH venues for every traded market, else
    // BAIL. This is the actual exchange leverage (NOT the config [capital] soft cap, which only sizes
    // orders) — a leftover 5x/20x amplifies exposure beyond the deposited capital. Aster has no
    // EVM-signed set-leverage endpoint, so we VERIFY it (operator sets it once on the Aster UI).
    // Lighter is also verified read-only from the account payload's per-market
    // initial_margin_fraction. Done before any trading.
    for s in specs {
        let aster_lev = worker_aster
            .get_leverage(&s.market_id)
            .await
            .map_err(|e| anyhow::anyhow!("Aster leverage read failed for {}: {e:#}", s.market_id.0))?;
        if aster_lev != 1 {
            anyhow::bail!(
                "Aster leverage for {} is {aster_lev}x (expected 1x) — set {} to 1x cross on the Aster UI and restart",
                s.market_id.0, s.aster_symbol
            );
        }
        let lighter_lev = worker_hl
            .get_leverage(&s.market_id)
            .await
            .map_err(|e| anyhow::anyhow!("Lighter leverage read failed for {}: {e:#}", s.market_id.0))?;
        if lighter_lev != rust_decimal::Decimal::ONE {
            anyhow::bail!(
                "Lighter leverage for {} is {lighter_lev}x (expected 1x) — set {} to 1x cross on the Lighter UI and restart",
                s.market_id.0, s.hl_coin
            );
        }
        info!("leverage gate: {} = 1x (Aster verified, Lighter verified)", s.market_id.0);
    }

    // Spawn the venue workers (writes) as SEPARATE tasks: a join! in one task would
    // serialize them, letting an Aster ECDSA sign (or response parse) head-of-line
    // block a hedge dequeue exactly when a fill just landed. The returned handle is a
    // supervisor that only awaits the two real tasks at shutdown.
    let etx = events_tx.clone();
    let aster_worker_task = tokio::spawn(run_aster_worker(exec_rx, exec_prio_rx, etx, worker_aster));
    let hl_worker_task = tokio::spawn(run_hl_worker(hedge_rx, events_tx, worker_hl));
    let worker_task = tokio::spawn(async move {
        let _ = aster_worker_task.await;
        let _ = hl_worker_task.await;
    });

    // Refuse to trade live in hedge mode (the bot assumes one-way).
    recon.assert_one_way().await?;

    // Enforce the CLEAN-START invariant BEFORE the initial reconcile and before any quoting: cancel
    // stray orders on our symbols and poll until the book is clean (or bail if require_clean_start).
    // Replaces the old fire-and-forget startup CancelAllBot that could let a fast startup begin
    // quoting while prior-run orders still rest.
    recon
        .ensure_clean_start(cfg.live.startup_cancel_all, cfg.live.require_clean_start)
        .await?;

    // Initial reconcile — publishes the first snapshot; failure aborts clean-start.
    let snap = recon
        .reconcile_and_publish(account)
        .await
        .map_err(|e| anyhow::anyhow!("initial account reconcile failed (cannot clean-start live): {e:#}"))?;
    info!(
        "initial reconcile: aster_avail=${} hl_withdrawable=${} aster_pos={} hl_pos={} open_orders={}",
        snap.aster_available_usd, snap.hl_withdrawable_usd, snap.aster_positions.len(), snap.hl_positions.len(), snap.open_orders.len()
    );

    // Cold reconcile loop: refresh well inside `max_account_snapshot_age_ms`.
    let interval = Duration::from_millis((cfg.live.max_account_snapshot_age_ms / 2).clamp(500, 2000) as u64);
    aux.push(tokio::spawn(recon.run(account.clone(), shutdown.clone(), interval)));

    // Aster user (fill) stream → maker-fill channel. Keep a clone of the liveness stamp to hand
    // to the strategy so it can freeze quoting if the stream silently dies.
    let liveness = Arc::new(StreamLiveness::default());
    aux.push(tokio::spawn(run_aster_user_stream(stream_aster, sym_to_market, maker_fill_tx, liveness.clone(), shutdown.clone())));

    Ok((worker_task, true, Some(liveness)))
}
