//! The `run` command: one process holding both engines for one market and handing execution
//! rights between them in memory. It replaces the retired orchestrator.py, its child
//! processes, `status --json` scraping and lease/signal files.
//!
//! * [`regime`]: which engine should hold the rights (the ported `decide()`).
//! * [`risk`]: the cross-engine loss stops (equity drawdown, realized trade PnL).
//! * `supervisor`: the loop — engine tasks, the reduce-only lease, switching, halts.
//! * `engines`: the real engine tasks and status pollers behind the supervisor.

pub(crate) mod engines;
pub mod regime;
pub mod risk;
pub(crate) mod supervisor;

use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::LiveMode;

/// Runtime state (journals, latches, controller files) lives here, relative to the working
/// directory: the crate dir, bind-mounted as /app/runs in Docker.
pub const RUNS_DIR: &str = "runs";

/// `[controller]` of bot.toml. The defaults are the orchestrator's live values.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ControllerCfg {
    /// Status poll and regime decision cadence.
    pub poll_sec: u64,
    /// A blocked normal taker must stay blocked this long before XEMM takes over.
    pub blocked_confirm_sec: u64,
    /// XEMM hands back once the taker has been ready this long.
    pub resume_confirm_sec: u64,
    /// The taker is blocked below this many clips of headroom / of free margin ...
    pub switch_headroom_clips: Decimal,
    pub switch_margin_clips: Decimal,
    /// ... and ready again only above these (default: switch + 1, the hysteresis band).
    pub resume_headroom_clips: Option<Decimal>,
    pub resume_margin_clips: Option<Decimal>,
    /// Near flat at or below this notional; `0` means one clip.
    pub near_flat_notional_usd: Decimal,
    /// Keep a reduce-only taker in standby while XEMM is active: the reduce fast path.
    pub observer: bool,
    /// A confirmed reduce burst older than this is ignored.
    pub reduce_signal_fresh_ms: i64,
    /// This many reducing opportunities within the window confirm a burst.
    pub reduce_burst_min_samples: usize,
    pub reduce_burst_window_ms: i64,
    /// The reduce-only lease is granted or extended for this long, and never past this long
    /// after its first grant.
    pub reduce_lease_sec: i64,
    pub reduce_lease_max_sec: i64,
    /// Cooldown after each reduce-filtered trade.
    pub reduce_cooldown_ms: u64,
    /// Cross-engine loss stop, inclusive (equity drawdown or realized trade PnL).
    pub max_loss_usdc: Decimal,
    /// A persisted equity baseline not refreshed for this long is discarded; `0` keeps it.
    pub baseline_max_gap_hours: u64,
}

impl Default for ControllerCfg {
    fn default() -> Self {
        Self {
            poll_sec: 15,
            blocked_confirm_sec: 90,
            resume_confirm_sec: 45,
            switch_headroom_clips: Decimal::TWO,
            switch_margin_clips: Decimal::TWO,
            resume_headroom_clips: None,
            resume_margin_clips: None,
            near_flat_notional_usd: Decimal::ZERO,
            observer: true,
            reduce_signal_fresh_ms: 60_000,
            reduce_burst_min_samples: 3,
            reduce_burst_window_ms: 2_000,
            reduce_lease_sec: 180,
            reduce_lease_max_sec: 300,
            reduce_cooldown_ms: 5_000,
            max_loss_usdc: Decimal::from(15),
            baseline_max_gap_hours: 48,
        }
    }
}

impl ControllerCfg {
    pub fn validate(&self) -> Result<()> {
        // The loss stops run once per poll.
        ensure!((1..=60).contains(&self.poll_sec), "controller.poll_sec must be within 1..=60");
        let clips = [self.switch_headroom_clips, self.switch_margin_clips, self.near_flat_notional_usd];
        ensure!(clips.iter().chain(self.resume_headroom_clips.iter()).chain(self.resume_margin_clips.iter()).all(|c| !c.is_sign_negative()),
            "controller clip and near-flat thresholds must be >= 0");
        // Without the hysteresis band the taker reads as blocked and ready at once, and the
        // engines flip every confirm window, each flip a full XEMM drain.
        let t = self.thresholds();
        ensure!(t.resume_headroom_clips > t.switch_headroom_clips && t.resume_margin_clips > t.switch_margin_clips,
            "controller resume clips must exceed the switch clips");
        ensure!(self.max_loss_usdc > Decimal::ZERO, "controller.max_loss_usdc must be > 0");
        ensure!(self.reduce_signal_fresh_ms > 0 && self.reduce_burst_window_ms > 0 && self.reduce_burst_min_samples > 0,
            "controller reduce-burst settings must be > 0");
        ensure!(self.reduce_lease_sec > 0 && self.reduce_lease_max_sec >= self.reduce_lease_sec,
            "controller.reduce_lease_sec must be > 0 and <= reduce_lease_max_sec");
        Ok(())
    }

    pub fn thresholds(&self) -> regime::Thresholds {
        regime::Thresholds {
            blocked_confirm: std::time::Duration::from_secs(self.blocked_confirm_sec),
            resume_confirm: std::time::Duration::from_secs(self.resume_confirm_sec),
            switch_headroom_clips: self.switch_headroom_clips,
            switch_margin_clips: self.switch_margin_clips,
            resume_headroom_clips: self.resume_headroom_clips.unwrap_or(self.switch_headroom_clips + Decimal::ONE),
            resume_margin_clips: self.resume_margin_clips.unwrap_or(self.switch_margin_clips + Decimal::ONE),
            near_flat_notional_usd: self.near_flat_notional_usd,
        }
    }
}

/// bot.toml: `[controller]`, plus each engine's full config under `[taker]` and `[maker]`
/// (the standalone `taker ...` and XEMM commands read their own table of the same file), and
/// the simulated venues of `--mode dry-run` under `[dry_run]`.
pub struct BotConfig {
    pub controller: ControllerCfg,
    pub taker: crate::taker::config::Config,
    pub maker: crate::config::Config,
    pub dry_run: Option<crate::dryrun::DryRunCfg>,
}

impl BotConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
        let mut value: toml::Value = toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        let table = value.as_table_mut().context("config is not a TOML table")?;
        let dry_run = table.remove("dry_run");
        let mut take = |name: &str| table.remove(name).with_context(|| format!("{} has no [{name}] table", path.display()));
        let (controller, taker, maker) = (take("controller")?, take("taker")?, take("maker")?);
        if let Some(extra) = table.keys().next() {
            bail!("unknown top-level table [{extra}] in {}", path.display());
        }
        let controller: ControllerCfg = crate::config::strict_from_toml(controller).context("[controller]")?;
        controller.validate()?;
        let dry_run: Option<crate::dryrun::DryRunCfg> =
            dry_run.map(crate::config::strict_from_toml).transpose().context("[dry_run]")?;
        if let Some(dry_run) = &dry_run {
            dry_run.validate()?;
        }
        Ok(Self {
            controller,
            taker: crate::taker::config::Config::from_table(taker).context("[taker]")?,
            maker: crate::config::Config::from_table(maker).context("[maker]")?,
            dry_run,
        })
    }

    /// Both engines' entry for `market`, which must name the same Aster and Lighter
    /// instruments. XEMM only ever unwinds inventory here, so it must be reduce-only.
    pub fn select(&self, market: &str) -> Result<(Vec<crate::taker::config::MarketCfg>, Vec<crate::config::MarketCfg>)> {
        let taker = self.taker.select_markets(Some(market));
        let maker = self.maker.select_markets(Some(market));
        let (Some(t), Some(m), 1, 1) = (taker.first(), maker.first(), taker.len(), maker.len()) else {
            bail!("market {market} must appear exactly once in both [[taker.markets]] and [[maker.markets]]");
        };
        ensure!(t.id().0 == market && m.id().0 == market, "market ids must be spelled {market} in both engine configs");
        ensure!(t.aster_symbol.eq_ignore_ascii_case(&m.aster_symbol) && t.lighter_symbol.eq_ignore_ascii_case(&m.hl_coin),
            "taker and maker configs name different instruments for {market}");
        ensure!(self.maker.live.quote.reduce_position_only, "[maker.live.quote] reduce_position_only must be true under `run`");
        ensure!(self.maker.live.enabled, "[maker.live] enabled must be true under `run`");
        ensure!(self.taker.pnl.enabled && self.maker.live.circuit_breaker.enabled,
            "`run` keeps both engines' own loss stops: [taker.pnl] and [maker.live.circuit_breaker] need enabled = true");
        Ok((taker, maker))
    }
}

/// `run`: both engines for `market`, execution rights switched in memory. Returns `Err` on a
/// safe halt or an unresolved engine stop, `Ok` after a clean signal-driven stop; a halted
/// dry run instead stays parked until stopped.
pub async fn run(config: &Path, market: &str, mode: LiveMode, ack_breaker: bool, reset_baseline: bool, stop: CancellationToken) -> Result<()> {
    let cfg = BotConfig::load(config)?;
    run_with(cfg, Path::new(RUNS_DIR), market, mode, ack_breaker, reset_baseline, stop).await
}

/// [`run`] with the config loaded and the runs directory given.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_with(
    mut cfg: BotConfig,
    runs_root: &Path,
    market: &str,
    mode: LiveMode,
    ack_breaker: bool,
    reset_baseline: bool,
    stop: CancellationToken,
) -> Result<()> {
    let market = market.to_ascii_uppercase();
    let (taker_markets, maker_markets) = cfg.select(&market)?;
    let live = mode.is_real();
    // Live and dry-run never share a file.
    let runs_dir = if live { runs_root.to_path_buf() } else { runs_root.join("dry-run") };
    let _lock = lock_market(&runs_dir, &market)?;
    let sim = if live {
        refuse_insecure_env_files()?;
        refuse_legacy_stack(&market, &[runs_root, Path::new("../runs")])?;
        None
    } else {
        let dry_run = cfg.dry_run.clone().context("--mode dry-run needs a [dry_run] table in the config")?;
        Some(crate::dryrun::start(&dry_run, &mut cfg, &taker_markets[0], &runs_dir).await?)
    };
    let parked = stop.clone();
    let result = async move {
        let files = supervisor::Files::new(&runs_dir, &market);
        let mut events = EventLog::new(files.events.clone());
        let taker_session = crate::taker::pnl::session_path(&cfg.taker.pnl, &taker_markets[0].id());
        let markers = [taker_session, crate::livebot::breaker::active_path(&files.xemm_stem)];
        if !live {
            archive_unclean_sessions(markers, &mut events)?;
        } else if let Some(marker) = markers.iter().find(|marker| marker.exists()) {
            // Found now, not when its engine first arms, hours in for a standby taker (whose
            // arming would fail on it every tick).
            bail!("{} is an unresolved engine session: resolve it first (RUNBOOK.md, Halts and recovery)", marker.display());
        }
        risk::check_breaker(&files.breaker, ack_breaker, reset_baseline, &mut events)?;
        if reset_baseline && files.baseline.exists() {
            std::fs::remove_file(&files.baseline)?;
            events.emit("baseline_reset", serde_json::json!({"path": files.baseline.display().to_string()}));
        }
        let taker_ledger = crate::taker::pnl::ledger_path(&cfg.taker.pnl, &taker_markets[0].id());
        let engines = engines::LiveEngines::new(&cfg, &market, taker_markets, maker_markets, files.xemm_stem.clone()).await?;
        supervisor::Supervisor::new(cfg.controller, market, mode, files, taker_ledger, engines, events, stop).run().await
    }
    .await;
    if let Some(sim) = sim {
        if let Err(error) = &result {
            // Once the venues are up, a dry run parks on any halt, a refused start included:
            // exiting would let a restart policy resume it unreviewed (or loop), and a
            // deliberate restart is the review.
            warn!("dry run halted, parked until stopped: {error:#}");
            parked.cancelled().await;
        }
        if let Err(error) = sim.save().await {
            warn!("dry run: saving the simulated venues: {error:#}");
        }
    }
    result
}

/// The engines' unclean-session markers (`markers`) a killed dry run, or an unresolved engine
/// stop, left behind. Live keeps them until an operator resolves the session against the venues'
/// records; the simulated venues' own saved state is the only record here, and the engines
/// reconcile to it at start (a halt was already recorded, and restarting is its review).
/// Archives each as `<name>.unclean.<stamp>`.
fn archive_unclean_sessions(markers: [PathBuf; 2], events: &mut EventLog) -> Result<()> {
    for marker in markers.into_iter().filter(|path| path.exists()) {
        let archived = PathBuf::from(format!("{}.unclean.{}", marker.display(), Utc::now().format("%Y%m%dT%H%M%SZ")));
        std::fs::rename(&marker, &archived).with_context(|| format!("archiving {}", marker.display()))?;
        warn!("dry run: the last run stopped uncleanly; archived {} as {}", marker.display(), archived.display());
        events.emit("dry_run_unclean_session_archived", serde_json::json!({"path": marker.display().to_string(), "archived": archived.display().to_string()}));
    }
    Ok(())
}

/// Live refuses credential files readable by group or other (mode must be 600).
fn refuse_insecure_env_files() -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut insecure = Vec::new();
        for path in crate::livebot::exec::creds::env_files() {
            if let Ok(meta) = std::fs::metadata(&path) {
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    insecure.push(format!("{} (mode {mode:03o})", path.display()));
                }
            }
        }
        ensure!(insecure.is_empty(), "credential env file(s) readable by group/other; chmod 600 them: {}", insecure.join(", "));
    }
    Ok(())
}

/// The two-process stack must be stopped, and its latches reviewed, before `run`: a running
/// orchestrator.py (it holds this flock for life), its breaker, and XEMM's trip latch and
/// unclean-session marker under the old db stem, which the new file names would silently
/// skip. Looked for in `runs/` and in the stack root's `runs/`, where the orchestrator kept
/// them. Children orphaned by a killed orchestrator hold no lock; the cutover checklist's
/// `pgrep` covers those. Transitional: delete once no host runs the old layout.
fn refuse_legacy_stack(market: &str, dirs: &[&Path]) -> Result<()> {
    for dir in dirs {
        let lock = dir.join(format!("orchestrator_{market}.lock"));
        if let Ok(file) = File::open(&lock) {
            if let Err(std::fs::TryLockError::WouldBlock) = file.try_lock() {
                bail!("orchestrator.py is still running ({} is locked); stop it and every bot it started", lock.display());
            }
        }
        for name in [
            format!("orchestrator_breaker_{market}.json"),
            format!("orchestrator-xemm-{market}.trip.json"),
            format!("orchestrator-xemm-{market}.active.json"),
        ] {
            let path = dir.join(name);
            if path.exists() {
                bail!("legacy latch {} (orchestrator era): review it, then archive or delete it before `run`", path.display());
            }
        }
    }
    Ok(())
}

/// Exclusive per-market lock held by every writer (`run`, `taker run`) in its runs directory:
/// two writers on one account and market break client-order-index uniqueness, nonce sequencing
/// and position accounting. The OS drops the lock with the process, so it never goes stale.
/// Ponytail: keyed by the runs directory under the working directory, so a writer started from
/// another directory is not excluded.
pub fn lock_market(runs_dir: &Path, market: &str) -> Result<File> {
    std::fs::create_dir_all(runs_dir)?;
    let path = runs_dir.join(format!("bot-{}.lock", market.to_ascii_uppercase()));
    let mut file = std::fs::OpenOptions::new().create(true).truncate(false).read(true).write(true).open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => {
            let mut holder = String::new();
            let _ = file.read_to_string(&mut holder);
            bail!("another live writer holds {} (pid {})", path.display(), holder.trim());
        }
        Err(std::fs::TryLockError::Error(error)) => return Err(error).with_context(|| format!("locking {}", path.display())),
    }
    file.set_len(0)?;
    file.rewind()?;
    writeln!(file, "{}", std::process::id())?;
    Ok(file)
}

/// A token cancelled by SIGINT, SIGTERM or SIGHUP (Ctrl-C only off Unix): every live entry
/// point takes the same graceful drain whether stopped from a terminal, by Docker or by a
/// closed session.
pub fn stop_on_signals() -> CancellationToken {
    let stop = CancellationToken::new();
    let token = stop.clone();
    tokio::spawn(async move {
        let name = stop_signal().await;
        info!("{name}: shutting down");
        token.cancel();
    });
    stop
}

async fn ctrl_c() {
    if tokio::signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await; // no handler: never "received"
    }
}

#[cfg(unix)]
async fn stop_signal() -> &'static str {
    use tokio::signal::unix::{signal, SignalKind};
    let (Ok(mut term), Ok(mut hup)) = (signal(SignalKind::terminate()), signal(SignalKind::hangup())) else {
        warn!("SIGTERM/SIGHUP handlers unavailable; only Ctrl-C drains gracefully");
        ctrl_c().await;
        return "SIGINT";
    };
    tokio::select! {
        _ = ctrl_c() => "SIGINT",
        _ = term.recv() => "SIGTERM",
        _ = hup.recv() => "SIGHUP",
    }
}

#[cfg(not(unix))]
async fn stop_signal() -> &'static str {
    ctrl_c().await;
    "ctrl-c"
}

/// Append-only JSONL record of controller events (`bot-<M>.events.jsonl`), mirrored to the
/// log. It never takes the bot down by itself: a failed write is reported, and only a file
/// unwritable 10 times in a row (e.g. a full disk) asks the supervisor to stop.
pub struct EventLog {
    path: PathBuf,
    failures: u32,
}

impl EventLog {
    pub fn new(path: PathBuf) -> Self {
        Self { path, failures: 0 }
    }

    pub fn emit(&mut self, kind: &str, details: Value) {
        info!("event {kind} {details}");
        let mut row = match details {
            Value::Object(map) => map,
            Value::Null => Map::new(),
            other => Map::from_iter([("details".to_owned(), other)]),
        };
        row.insert("timestamp".into(), Value::String(iso(Utc::now())));
        row.insert("kind".into(), Value::String(kind.to_owned()));
        match crate::taker::pnl::append_json_line(&self.path, &row, false) {
            Ok(()) => self.failures = 0,
            Err(error) => {
                self.failures += 1;
                warn!("event log write failed ({} in a row): {error:#}", self.failures);
            }
        }
    }

    pub fn unwritable(&self) -> bool {
        self.failures >= 10
    }
}

pub(crate) fn iso(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_bot_toml_loads_strictly_and_rejects_stray_keys() {
        let shipped = include_str!("../../bot.toml");
        let dir = crate::dryrun::tests::temp_dir("bot-config");
        let path = dir.join("bot.toml");
        std::fs::write(&path, shipped).unwrap();
        let cfg = BotConfig::load(&path).unwrap();
        let (taker, maker) = cfg.select("HYPE").unwrap();
        assert_eq!((taker[0].lighter_market_index, maker[0].aster_symbol.as_str()), (Some(24), "HYPEUSDT"));
        assert!(cfg.select("BNB").is_err(), "BNB has no taker entry");
        for (edited, expected) in [
            // XEMM's old `[live] mode` key: the mode is a command-line choice only.
            (shipped.replace("[maker.live]", "[maker.live]\nmode = \"live\""), "live.mode"),
            (shipped.replacen("poll_sec", "poll_secs", 1), "poll_secs"),
            (format!("{shipped}\n[venues]\nsigners_dir = \"signers\"\n"), "top-level table [venues]"),
        ] {
            std::fs::write(&path, edited).unwrap();
            let error = format!("{:#}", BotConfig::load(&path).err().expect("stray key accepted"));
            assert!(error.contains(expected), "{error}");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    /// `run --mode dry-run` end to end: the real controller and engines against the simulated
    /// venues, fed by a scripted market standing in for mainnet. Docker runs it with
    /// `--network none`, so nothing can reach a real venue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dry_run_hedges_a_scripted_arbitrage_and_drains_on_stop() {
        use rust_decimal_macros::dec;
        use std::time::Duration;
        let _ = tracing_subscriber::fmt().with_test_writer().try_init();
        let market = std::sync::Arc::new(crate::dryrun::tests::World::start().await);
        let dir = crate::dryrun::tests::temp_dir("dry-run-e2e");
        let mut cfg = crate::dryrun::tests::shipped_config(&market, &dir);
        cfg.controller.poll_sec = 1;
        // The taker's warm-up and history gates would need minutes of market data.
        let arb = &mut cfg.taker.arb;
        (arb.startup_warmup_ms, arb.entry_gate.enabled, arb.book_sanity.enabled) = (0, false, false);
        // Aster asks 98 while Lighter bids 99: 100 bps across the venues.
        market.set_aster(dec!(97), dec!(98));
        let fresh = market.keep_fresh();
        let stop = CancellationToken::new();
        let bot = tokio::spawn({
            let (stop, runs) = (stop.clone(), dir.clone());
            async move { run_with(cfg, &runs, "HYPE", LiveMode::DryRun, false, false, stop).await }
        });
        let ledger = dir.join("dry-run").join("trades_HYPE.jsonl");
        let row = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if let Some(line) = std::fs::read_to_string(&ledger).ok().and_then(|text| text.lines().next().map(str::to_string)) {
                    break serde_json::from_str::<crate::taker::pnl::TradeLedgerRow>(&line).unwrap();
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("no hedged trade within 60 s");
        let confirmed = crate::taker::pnl::EconomicStatus::Confirmed;
        assert_eq!((row.economic_status, row.direction.as_str()), (confirmed, "SELL_LIGHTER_BUY_ASTER"), "{row:?}");
        assert_eq!((row.aster_fill.vwap, row.lighter_fill.vwap), (dec!(98), dec!(99)), "each leg took the top: {row:?}");
        assert_eq!(row.aster_fill.qty, row.lighter_fill.qty, "{row:?}");
        assert!(row.aster_fill.fee_usd > Decimal::ZERO && row.lighter_fill.fee_usd.is_zero(), "Aster charges 4 bps: {row:?}");
        assert!(row.final_net_position.is_zero(), "{row:?}");
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(60), bot).await.expect("the drain hung").unwrap().expect("a clean stop");
        // The final save keeps the hedged pair for the next start.
        let state = std::fs::read_to_string(dir.join("dry-run").join("sim-HYPE.state.json")).unwrap();
        let state: serde_json::Value = serde_json::from_str(&state).unwrap();
        let qty = |venue: usize, market: &str| state[venue]["account"]["positions"][market]["qty"].as_str().and_then(|q| q.parse::<Decimal>().ok());
        assert_eq!((qty(0, "HYPEUSDT"), qty(1, "24")), (Some(dec!(0.13)), Some(dec!(-0.13))), "{state}");
        let mut written: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name().into_string().unwrap()).collect();
        written.sort();
        assert_eq!(written, ["bot.toml", "dry-run"], "a dry run writes under runs/dry-run only");

        // A restart that must not resume (a drawdown halt acknowledged without a baseline
        // reset) parks, so a restart policy cannot loop on it. A kill had also left the
        // taker's unclean-session marker, which the dry run archives.
        let latch = r#"{"reason":"pnl_breaker","details":{"breaker_reason":"equity_drawdown"}}"#;
        std::fs::write(dir.join("dry-run").join("bot-HYPE.breaker.json"), latch).unwrap();
        let marker = dir.join("dry-run").join("active_session_HYPE.json");
        std::fs::write(&marker, "{}").unwrap();
        let (cfg, stop) = (crate::dryrun::tests::shipped_config(&market, &dir), CancellationToken::new());
        let again = tokio::spawn({
            let (stop, runs) = (stop.clone(), dir.clone());
            async move { run_with(cfg, &runs, "HYPE", LiveMode::DryRun, true, false, stop).await }
        });
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(!again.is_finished(), "a refused dry-run start parks");
        stop.cancel();
        let error = again.await.unwrap().expect_err("still a halt");
        assert!(format!("{error:#}").contains("--reset-breaker-baseline"), "{error:#}");
        let archived = std::fs::read_dir(dir.join("dry-run")).unwrap().flatten()
            .any(|entry| entry.file_name().to_string_lossy().starts_with("active_session_HYPE.json.unclean."));
        assert!(!marker.exists() && archived, "the unclean-session marker is archived");
        fresh.abort();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_running_orchestrator_or_its_latches_block_run() {
        let dir = crate::dryrun::tests::temp_dir("bot-legacy");
        let check = || refuse_legacy_stack("HYPE", &[dir.as_path()]);
        // A stopped orchestrator leaves its lock file behind, unlocked.
        let lock = File::create(dir.join("orchestrator_HYPE.lock")).unwrap();
        check().unwrap();
        lock.try_lock().unwrap();
        assert!(format!("{:#}", check().unwrap_err()).contains("orchestrator.py is still running"));
        drop(lock);
        std::fs::write(dir.join("orchestrator-xemm-HYPE.trip.json"), "{}").unwrap();
        assert!(format!("{:#}", check().unwrap_err()).contains("legacy latch"));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
