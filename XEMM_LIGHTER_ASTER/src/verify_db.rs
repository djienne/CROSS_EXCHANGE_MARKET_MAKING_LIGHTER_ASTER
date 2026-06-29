//! `verify-db`: audit a results SQLite database for internal consistency.
//!
//! The DB is a *regenerable cache* of the JSONL tape, so the schema keeps
//! `foreign_keys=OFF` for append speed (see `store::schema`). This command is the
//! integrity check that catches the orphaned or miscounted rows that FK enforcement
//! would otherwise have to police on the hot write path. It opens the database
//! READ-ONLY, runs a handful of invariant queries, prints the risk signals this build
//! records, and exits non-zero if any invariant is violated.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

/// Audit `db_path`. Returns `Err` (non-zero exit) if any integrity invariant fails.
pub fn run(db_path: impl AsRef<Path>) -> Result<()> {
    let path = db_path.as_ref();
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening {} read-only", path.display()))?;

    let count = |sql: &str| -> Result<i64> { Ok(conn.query_row(sql, [], |r| r.get(0))?) };

    println!("verify-db: auditing {}", path.display());
    let runs = count("SELECT COUNT(*) FROM runs")?;
    let fills = count("SELECT COUNT(*) FROM simulated_fills")?;
    let hedges = count("SELECT COUNT(*) FROM hedges")?;
    println!("  rows: runs={runs} fills={fills} hedges={hedges}");

    let mut violations: Vec<String> = Vec::new();

    // 1. Referential integrity (checked by hand, since FK enforcement is off).
    let orphan_fill_run =
        count("SELECT COUNT(*) FROM simulated_fills WHERE run_id NOT IN (SELECT run_id FROM runs)")?;
    if orphan_fill_run > 0 {
        violations.push(format!("{orphan_fill_run} fills reference a missing run_id"));
    }
    let orphan_hedge_run =
        count("SELECT COUNT(*) FROM hedges WHERE run_id NOT IN (SELECT run_id FROM runs)")?;
    if orphan_hedge_run > 0 {
        violations.push(format!("{orphan_hedge_run} hedges reference a missing run_id"));
    }
    let orphan_hedge_fill = count(
        "SELECT COUNT(*) FROM hedges WHERE fill_id IS NOT NULL \
         AND fill_id NOT IN (SELECT id FROM simulated_fills)",
    )?;
    if orphan_hedge_fill > 0 {
        violations.push(format!("{orphan_hedge_fill} hedges reference a missing fill_id"));
    }

    // 2. Quantity sanity.
    let bad_fill_qty =
        count("SELECT COUNT(*) FROM simulated_fills WHERE CAST(fill_qty AS REAL) <= 0")?;
    if bad_fill_qty > 0 {
        violations.push(format!("{bad_fill_qty} fills have non-positive fill_qty"));
    }
    let over_filled =
        count("SELECT COUNT(*) FROM hedges WHERE CAST(filled_qty AS REAL) > CAST(qty AS REAL) + 1e-9")?;
    if over_filled > 0 {
        violations.push(format!("{over_filled} hedges filled more than requested"));
    }

    // 3. Per-run hedge-bucket consistency: a fill that hedged must have exactly one row
    // per configured latency bucket (the engine schedules all buckets together, so a
    // hedged fill_id appears once per bucket; a sub-min fill that only accumulated has
    // zero — both are fine, anything else is a bug).
    {
        let mut stmt = conn.prepare("SELECT run_id FROM runs")?;
        let run_ids: Vec<String> =
            stmt.query_map([], |r| r.get::<_, String>(0))?.collect::<std::result::Result<_, _>>()?;
        for run_id in run_ids {
            let n_buckets: i64 = conn.query_row(
                "SELECT COUNT(DISTINCT latency_bucket_ms) FROM hedges WHERE run_id=?1",
                [&run_id],
                |r| r.get(0),
            )?;
            if n_buckets == 0 {
                continue;
            }
            let inconsistent: i64 = conn.query_row(
                "SELECT COUNT(*) FROM (
                     SELECT fill_id FROM hedges WHERE run_id=?1 AND fill_id IS NOT NULL
                     GROUP BY fill_id HAVING COUNT(*) != ?2
                 )",
                rusqlite::params![run_id, n_buckets],
                |r| r.get(0),
            )?;
            if inconsistent > 0 {
                violations.push(format!(
                    "run {run_id}: {inconsistent} fill(s) have a hedge-row count != {n_buckets} (one per bucket)"
                ));
            }
        }
    }

    // Informational — the honesty signals this build records (never failures by
    // themselves; they quantify exposure the report surfaces).
    let unbooked = count("SELECT COUNT(*) FROM hedges WHERE reason='MISSING_HL_BOOK'")?;
    let exhausted = count("SELECT COUNT(*) FROM hedges WHERE depth_exhausted=1")?;
    let stale_fills = count("SELECT COUNT(*) FROM simulated_fills WHERE feed_stale_at_fill=1")?;
    let trunc_fills = count("SELECT COUNT(*) FROM simulated_fills WHERE queue_truncated=1")?;
    let underhedged: f64 = conn.query_row(
        "SELECT COALESCE(SUM(CAST(qty AS REAL) - CAST(filled_qty AS REAL)),0.0) FROM hedges",
        [],
        |r| r.get(0),
    )?;
    println!(
        "  risk signals: unbooked_hedges={unbooked} depth_exhausted={exhausted} \
         stale_window_fills={stale_fills} queue_truncated_fills={trunc_fills} underhedged_qty={underhedged:.6}"
    );

    if violations.is_empty() {
        println!("verify-db: OK — no integrity violations.");
        Ok(())
    } else {
        for v in &violations {
            println!("  VIOLATION: {v}");
        }
        anyhow::bail!("verify-db: {} integrity violation(s) found", violations.len())
    }
}
