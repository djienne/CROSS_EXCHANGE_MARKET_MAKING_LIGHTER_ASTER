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
        ensure!(self.poll_sec > 0, "controller.poll_sec must be > 0");
        let clips = [self.switch_headroom_clips, self.switch_margin_clips, self.near_flat_notional_usd];
        ensure!(clips.iter().chain(self.resume_headroom_clips.iter()).chain(self.resume_margin_clips.iter()).all(|c| !c.is_sign_negative()),
            "controller clip and near-flat thresholds must be >= 0");
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
/// (the standalone `taker ...` and XEMM commands read their own table of the same file).
pub struct BotConfig {
    pub controller: ControllerCfg,
    pub taker: crate::taker::config::Config,
    pub maker: crate::config::Config,
}

impl BotConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
        let mut value: toml::Value = toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;
        let table = value.as_table_mut().context("config is not a TOML table")?;
        let mut take = |name: &str| table.remove(name).with_context(|| format!("{} has no [{name}] table", path.display()));
        let (controller, taker, maker) = (take("controller")?, take("taker")?, take("maker")?);
        if let Some(extra) = table.keys().next() {
            bail!("unknown top-level table [{extra}] in {}", path.display());
        }
        let controller: ControllerCfg = crate::config::strict_from_toml(controller).context("[controller]")?;
        controller.validate()?;
        Ok(Self {
            controller,
            taker: crate::taker::config::Config::from_table(taker).context("[taker]")?,
            maker: crate::config::Config::from_table(maker).context("[maker]")?,
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
        Ok((taker, maker))
    }
}

/// `run`: both engines for `market`, execution rights switched in memory. Returns `Err` on a
/// safe halt or an unresolved engine stop, `Ok` after a clean signal-driven stop.
pub async fn run(config: &Path, market: &str, mode: LiveMode, ack_breaker: bool, reset_baseline: bool, stop: CancellationToken) -> Result<()> {
    let market = market.to_ascii_uppercase();
    let cfg = BotConfig::load(config)?;
    let (taker_markets, maker_markets) = cfg.select(&market)?;
    let live = mode.is_real();
    let _lock = if live { Some(lock_market(&market)?) } else { None };
    if live {
        refuse_insecure_env_files()?;
        refuse_legacy_stack(&market, &[Path::new(RUNS_DIR), Path::new("../runs")])?;
    }
    let files = supervisor::Files::new(Path::new(RUNS_DIR), &market, live);
    std::fs::create_dir_all(RUNS_DIR)?;
    let mut events = EventLog::new(files.events.clone());
    risk::check_breaker(&files.breaker, ack_breaker, reset_baseline, &mut events)?;
    if reset_baseline && files.baseline.exists() {
        std::fs::remove_file(&files.baseline)?;
        events.emit("baseline_reset", serde_json::json!({"path": files.baseline.display().to_string()}));
    }
    let taker_ledger = crate::taker::pnl::ledger_path(&cfg.taker.pnl, &taker_markets[0].id());
    let engines = engines::LiveEngines::new(&cfg, &market, taker_markets, maker_markets, mode, files.xemm_db.clone()).await?;
    supervisor::Supervisor::new(cfg.controller, market, live, files, taker_ledger, engines, events, stop).run().await
}

/// Live refuses credential files readable by group or other (mode must be 600).
fn refuse_insecure_env_files() -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut insecure = Vec::new();
        for (var, default) in [("ASTER_ENV_PATH", "aster.env"), ("LIGHTER_ENV_PATH", "lighter.env")] {
            let path = std::env::var(var).unwrap_or_else(|_| default.into());
            if let Ok(meta) = std::fs::metadata(&path) {
                let mode = meta.permissions().mode() & 0o777;
                if mode & 0o077 != 0 {
                    insecure.push(format!("{path} (mode {mode:03o})"));
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

/// Exclusive per-market lock held by every live writer (`run`, `taker run`, `livebot --mode
/// live`): two writers on one account and market break client-order-index uniqueness, nonce
/// sequencing and position accounting. The OS drops the lock with the process, so it never
/// goes stale. Ponytail: keyed by `runs/` under the working directory, so a writer started
/// from another directory is not excluded.
pub fn lock_market(market: &str) -> Result<File> {
    std::fs::create_dir_all(RUNS_DIR)?;
    let path = Path::new(RUNS_DIR).join(format!("bot-{}.lock", market.to_ascii_uppercase()));
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
        let dir = std::env::temp_dir().join(format!("bot-config-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
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

    #[test]
    fn a_running_orchestrator_or_its_latches_block_run() {
        let dir = std::env::temp_dir().join(format!("bot-legacy-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
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
