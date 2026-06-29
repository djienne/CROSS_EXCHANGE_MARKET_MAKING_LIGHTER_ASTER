//! Circuit-breaker trip latch (persistent across restarts).
//!
//! When the strategy's cumulative-loss circuit breaker fires it writes a small JSON marker next to
//! the run's DB (`<runs_dir>/<db_stem>.trip.json`) and halts. On every subsequent startup, [`run`]
//! checks for that marker BEFORE any live execution setup and refuses to start while it exists. The
//! operator clears it (after reviewing what happened) with `scripts/reset_breaker.py`, which simply
//! deletes the file — it works through the Docker bind mount (`./runs:/app/runs`), no `docker exec`.
//!
//! This module is the single source of truth for the trip-file PATH (so the writer in `strategy.rs`
//! and the startup guard in `run.rs` can never disagree) and the record schema.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// The on-disk trip record. Human-readable; read back by the startup guard and `reset_breaker.py`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TripRecord {
    /// UTC timestamp (RFC3339) when the breaker tripped.
    pub ts_utc: String,
    /// The market the bot was running (informational; the latch blocks restart regardless).
    pub market: String,
    /// Total cross-venue equity baseline captured at startup (USD).
    pub baseline_usd: Decimal,
    /// Total cross-venue equity at the moment of the trip (USD).
    pub equity_usd: Decimal,
    /// Drawdown that crossed the limit = baseline - equity (USD).
    pub loss_usd: Decimal,
    /// The configured limit that was exceeded (USD).
    pub limit_usd: Decimal,
    /// Short human reason.
    pub reason: String,
}

/// The trip-latch path for a given run DB path: `<runs_dir>/<db_stem>.trip.json`.
///
/// Mirrors the journal-path derivation in `run.rs` so the latch lands in the same `runs/` directory
/// the rest of the run's artifacts do. Per-DB-stem ⇒ a HYPE trip blocks HYPE restarts, not ETH.
pub fn trip_path(db_path: &Path) -> PathBuf {
    let stem = db_path.file_stem().and_then(|s| s.to_str()).unwrap_or("livebot");
    let dir = db_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("runs"));
    dir.join(format!("{stem}.trip.json"))
}

/// Startup guard: bail loudly if the trip latch for this run DB exists. Called BEFORE any live
/// execution setup so a tripped bot can never resume trading until the operator clears the latch.
pub fn check_startup(db_path: &Path) -> Result<()> {
    let path = trip_path(db_path);
    if !path.exists() {
        return Ok(());
    }
    // The mere existence of the file is the latch; the contents are best-effort context.
    let (reason, loss, ts) = match std::fs::read_to_string(&path)
        .ok()
        .and_then(|s| serde_json::from_str::<TripRecord>(&s).ok())
    {
        Some(r) => (r.reason, r.loss_usd.to_string(), r.ts_utc),
        None => ("(unreadable trip file)".to_string(), "?".to_string(), "?".to_string()),
    };
    bail!(
        "circuit breaker TRIPPED — refusing to start. reason={reason}, loss={loss} USD, at={ts}. \
         File: {}. Review, then reset with:  python scripts/reset_breaker.py",
        path.display()
    );
}

/// Atomically write the trip latch (temp file + rename) so a reader never sees a partial file.
pub fn write_trip(path: &Path, rec: &TripRecord) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    let json = serde_json::to_string_pretty(rec).context("serialize trip record")?;
    // `runs/live-eth.trip.json` -> `runs/live-eth.trip.json.tmp` (extension is the final `.json`).
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, json.as_bytes()).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("rename trip latch into place: {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn trip_path_is_stem_based_in_runs_dir() {
        let p = trip_path(Path::new("runs/live-eth.sqlite"));
        assert_eq!(p, PathBuf::from("runs/live-eth.trip.json"));
        // No parent dir → defaults to runs/.
        assert_eq!(trip_path(Path::new("foo.sqlite")), PathBuf::from("runs/foo.trip.json"));
    }

    #[test]
    fn check_startup_ok_when_absent_bails_when_present() {
        let dir = std::env::temp_dir().join(format!("xemm_breaker_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("live-eth.sqlite");
        // Absent → Ok.
        let _ = std::fs::remove_file(trip_path(&db));
        assert!(check_startup(&db).is_ok());
        // Present → Err.
        let rec = TripRecord {
            ts_utc: "2026-06-15T00:00:00Z".into(),
            market: "ETH".into(),
            baseline_usd: dec!(100),
            equity_usd: dec!(94),
            loss_usd: dec!(6),
            limit_usd: dec!(5),
            reason: "test".into(),
        };
        write_trip(&trip_path(&db), &rec).unwrap();
        assert!(check_startup(&db).is_err());
        // Re-read round-trips.
        let back: TripRecord =
            serde_json::from_str(&std::fs::read_to_string(trip_path(&db)).unwrap()).unwrap();
        assert_eq!(back.loss_usd, dec!(6));
        let _ = std::fs::remove_file(trip_path(&db));
    }
}
