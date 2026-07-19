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

/// Shutdown guard for `run()`: if the trip latch exists when the run ends, the breaker fired
/// DURING this run (`check_startup` barred any pre-existing latch at startup) — return `Err`
/// so the process exits NONZERO and the supervisor safe-halts in one step. Without this, a
/// trip rode the graceful-shutdown path to exit 0, indistinguishable from a clean stop: the
/// orchestrator restarted the bot, the restart refused via the latch (exit 1), and only then
/// did it halt (observed 2026-07-04).
pub fn check_shutdown(db_path: &Path) -> Result<()> {
    let path = trip_path(db_path);
    if path.exists() {
        bail!(
            "circuit breaker tripped during this run (latch: {}); exiting nonzero so the supervisor halts",
            path.display()
        );
    }
    Ok(())
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

// --- shutdown residual report (same directory + atomic-write conventions as the trip latch) ---

/// One traded market's leftover position legs found by the shutdown verification sweep.
/// `net_qty == 0` (with nonzero legs) is the documented-normal delta-neutral leftover the
/// graceful shutdown leaves open; a nonzero `net_qty` is a real imbalance (recovered by
/// position adoption on the next start).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidualLine {
    pub market: String,
    pub aster_qty: Decimal,
    pub hl_qty: Decimal,
    pub net_qty: Decimal,
}

/// The on-disk shutdown residual report (`<runs_dir>/<db_stem>.residual.json`), overwritten on
/// each shutdown. Informational only — never a startup latch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResidualRecord {
    /// UTC timestamp (RFC3339) of the shutdown verification.
    pub ts_utc: String,
    /// True iff the final snapshot showed no bot-prefixed open orders on either venue.
    pub orders_verified_empty: bool,
    /// Per-market leftover legs (empty ⇒ verified flat everywhere).
    pub residuals: Vec<ResidualLine>,
}

/// The residual-report path for a given run DB path (mirrors [`trip_path`]).
pub fn residual_path(db_path: &Path) -> PathBuf {
    let stem = db_path.file_stem().and_then(|s| s.to_str()).unwrap_or("livebot");
    let dir = db_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("runs"));
    dir.join(format!("{stem}.residual.json"))
}

/// PURE: extract per-market leftover position legs from a snapshot. Only markets in `markets`
/// are considered (untraded symbols are the operator's business, not the bot's), and only
/// markets with at least one nonzero leg produce a line.
pub fn residual_positions(
    snap: &super::account::AccountSnapshot,
    markets: &[crate::types::MarketId],
) -> Vec<ResidualLine> {
    use super::account::Venue;
    let mut out = Vec::new();
    for m in markets {
        let aster_qty = snap.reported_position(Venue::Aster, m);
        let hl_qty = snap.reported_position(Venue::Hyperliquid, m);
        if aster_qty == Decimal::ZERO && hl_qty == Decimal::ZERO {
            continue;
        }
        out.push(ResidualLine { market: m.0.clone(), aster_qty, hl_qty, net_qty: aster_qty + hl_qty });
    }
    out
}

/// Atomically write the shutdown residual report (temp file + rename, mirrors [`write_trip`]).
pub fn write_residual(path: &Path, rec: &ResidualRecord) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    let json = serde_json::to_string_pretty(rec).context("serialize residual record")?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, json.as_bytes()).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("rename residual report into place: {}", path.display()))?;
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

    #[test]
    fn check_shutdown_errs_when_latch_present() {
        let dir = std::env::temp_dir().join(format!("xemm_breaker_shutdown_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("live-eth.sqlite");
        let _ = std::fs::remove_file(trip_path(&db));
        assert!(check_shutdown(&db).is_ok());
        let rec = TripRecord {
            ts_utc: "2026-07-04T04:23:57Z".into(),
            market: "HYPE".into(),
            baseline_usd: dec!(244),
            equity_usd: dec!(234),
            loss_usd: dec!(10),
            limit_usd: dec!(10),
            reason: "test".into(),
        };
        write_trip(&trip_path(&db), &rec).unwrap();
        assert!(check_shutdown(&db).is_err());
        let _ = std::fs::remove_file(trip_path(&db));
    }

    // --- shutdown residual report ---
    use crate::livebot::account::{AccountSnapshot, ScaledPosition, Venue};
    use crate::types::MarketId;

    fn residual_snap(rows: &[(&str, Venue, Decimal)]) -> AccountSnapshot {
        let mut s = AccountSnapshot::empty();
        for (market, venue, qty) in rows {
            let pos = ScaledPosition {
                venue: *venue,
                market: MarketId(market.to_string()),
                signed_qty: *qty,
                entry_px: dec!(100),
            };
            match venue {
                Venue::Aster => s.aster_positions.push(pos),
                Venue::Hyperliquid => s.hl_positions.push(pos),
            }
        }
        s
    }

    #[test]
    fn residual_positions_classifies_flat_neutral_and_net() {
        let markets: Vec<MarketId> = vec!["HYPE".into(), "ETH".into()];
        // Flat everywhere → empty.
        assert!(residual_positions(&residual_snap(&[]), &markets).is_empty());
        // Delta-neutral pair → one line with net 0.
        let s = residual_snap(&[("HYPE", Venue::Aster, dec!(0.5)), ("HYPE", Venue::Hyperliquid, dec!(-0.5))]);
        let lines = residual_positions(&s, &markets);
        assert_eq!(
            lines,
            vec![ResidualLine { market: "HYPE".into(), aster_qty: dec!(0.5), hl_qty: dec!(-0.5), net_qty: dec!(0.0) }]
        );
        // NET imbalance is flagged via a nonzero net; untraded markets ignored.
        let s = residual_snap(&[
            ("HYPE", Venue::Aster, dec!(0.5)),
            ("HYPE", Venue::Hyperliquid, dec!(-0.2)),
            ("DOGE", Venue::Aster, dec!(9)), // not in `markets` — operator's business
        ]);
        let lines = residual_positions(&s, &markets);
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].market, "HYPE");
        assert_eq!(lines[0].net_qty, dec!(0.3));
        // One-legged residual on the second traded market also reports.
        let s = residual_snap(&[("ETH", Venue::Hyperliquid, dec!(-1))]);
        let lines = residual_positions(&s, &markets);
        assert_eq!(lines, vec![ResidualLine { market: "ETH".into(), aster_qty: dec!(0), hl_qty: dec!(-1), net_qty: dec!(-1) }]);
    }

    #[test]
    fn residual_record_write_and_read_round_trip() {
        let dir = std::env::temp_dir().join(format!("xemm_residual_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("live-eth.sqlite");
        let path = residual_path(&db);
        assert_eq!(path, dir.join("live-eth.residual.json"));
        let rec = ResidualRecord {
            ts_utc: "2026-07-19T00:00:00Z".into(),
            orders_verified_empty: true,
            residuals: vec![ResidualLine {
                market: "HYPE".into(),
                aster_qty: dec!(0.5),
                hl_qty: dec!(-0.5),
                net_qty: dec!(0),
            }],
        };
        write_residual(&path, &rec).unwrap();
        let back: ResidualRecord = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert!(back.orders_verified_empty);
        assert_eq!(back.residuals, rec.residuals);
        // Overwrites on the next shutdown.
        let rec2 = ResidualRecord { ts_utc: "2026-07-20T00:00:00Z".into(), orders_verified_empty: false, residuals: vec![] };
        write_residual(&path, &rec2).unwrap();
        let back2: ResidualRecord = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert!(!back2.orders_verified_empty);
        assert!(back2.residuals.is_empty());
        let _ = std::fs::remove_file(&path);
    }
}
