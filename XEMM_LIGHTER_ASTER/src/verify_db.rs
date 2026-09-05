//! `verify-db`: audit a results SQLite database for internal consistency.
//!
//! The DB is a *regenerable cache* of the JSONL tape, so the schema keeps
//! `foreign_keys=OFF` for append speed (see `store::schema`). This command is the
//! integrity check that catches the orphaned or miscounted rows that FK enforcement
//! would otherwise have to police on the hot write path. It opens the database
//! READ-ONLY, runs a handful of invariant queries, prints the risk signals this build
//! records, and exits non-zero if any invariant is violated.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

/// Audit `db_path`. Returns `Err` (non-zero exit) if any integrity invariant fails.
pub fn run(db_path: impl AsRef<Path>) -> Result<()> {
    let path = db_path.as_ref();
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening {} read-only", path.display()))?;

    println!("verify-db: auditing {}", path.display());
    let violations = audit(&conn)?;
    if violations.is_empty() {
        println!("verify-db: OK — no integrity violations.");
        Ok(())
    } else {
        for violation in &violations { println!("  VIOLATION: {violation}"); }
        anyhow::bail!("verify-db: {} integrity violation(s) found", violations.len())
    }
}

fn audit(conn: &Connection) -> Result<Vec<String>> {
    let count = |sql: &str| -> Result<i64> { Ok(conn.query_row(sql, [], |r| r.get(0))?) };
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

    // A hedge must refer to its own run/market/model, not merely an existing UUID.
    let wrong_scope = count(
        "SELECT COUNT(*) FROM hedges h JOIN simulated_fills f ON h.fill_id=f.id
         WHERE h.run_id!=f.run_id OR h.market!=f.market OR h.queue_model!=f.queue_model",
    )?;
    if wrong_scope > 0 {
        violations.push(format!("{wrong_scope} hedges reference a fill in another scenario"));
    }

    // V1 shares a fill trajectory across all latency buckets. V2 gives each
    // scenario its own fills, so each hedged fill has ONE row in its own bucket.
    // Zero rows remain valid for accumulated/netted sub-minimum maker fills.
    let has_version = conn.prepare("PRAGMA table_info(runs)")?.query_map([], |r| r.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?.iter().any(|name| name == "semantics_version");
    let sql = if has_version { "SELECT run_id, config_json, semantics_version FROM runs" }
        else { "SELECT run_id, config_json, 1 FROM runs" };
    let mut stmt = conn.prepare(sql)?;
    let runs = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, i64>(2)?)))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (run_id, config, version) in runs {
        let config: Option<serde_json::Value> = serde_json::from_str(&config).ok();
        let configured: BTreeSet<i64> = config.as_ref().and_then(|c| c.pointer("/simulation/hedge_latency_buckets_ms"))
            .and_then(|v| v.as_array()).into_iter().flatten().filter_map(|v| v.as_i64()).collect();
        let mut stmt = conn.prepare("SELECT DISTINCT latency_bucket_ms FROM hedges WHERE run_id=?1")?;
        let observed = stmt.query_map([&run_id], |r| r.get::<_, i64>(0))?.collect::<rusqlite::Result<BTreeSet<_>>>()?;
        if !configured.is_empty() && !observed.is_subset(&configured) {
            violations.push(format!("run {run_id}: hedges use an unconfigured latency bucket"));
        }
        if version >= 2 {
            let wrong_bucket: i64 = conn.query_row(
                "SELECT COUNT(*) FROM hedges h LEFT JOIN simulated_fills f ON h.fill_id=f.id
                 WHERE h.run_id=?1 AND (h.fill_id IS NULL OR f.latency_bucket_ms!=h.latency_bucket_ms
                     OR h.latency_bucket_ms<0)",
                [&run_id], |r| r.get(0),
            )?;
            let duplicate: i64 = conn.query_row(
                "SELECT COUNT(*) FROM (SELECT fill_id FROM hedges WHERE run_id=?1 AND fill_id IS NOT NULL
                 GROUP BY fill_id HAVING COUNT(*)!=1)", [&run_id], |r| r.get(0),
            )?;
            if wrong_bucket > 0 {
                violations.push(format!("run {run_id}: {wrong_bucket} hedges lack a fill in their own latency scenario"));
            }
            if duplicate > 0 {
                violations.push(format!("run {run_id}: {duplicate} scenario fill(s) have more than one hedge row"));
            }
        } else {
            let n_buckets = if configured.is_empty() { observed.len() } else { configured.len() } as i64;
            if n_buckets == 0 { continue; }
            let inconsistent: i64 = conn.query_row(
                "SELECT COUNT(*) FROM (SELECT fill_id FROM hedges WHERE run_id=?1 AND fill_id IS NOT NULL
                 GROUP BY fill_id HAVING COUNT(*)!=?2 OR COUNT(DISTINCT latency_bucket_ms)!=?2)",
                rusqlite::params![run_id, n_buckets], |r| r.get(0),
            )?;
            if inconsistent > 0 {
                violations.push(format!(
                    "run {run_id}: {inconsistent} legacy fill(s) lack exactly one hedge per latency bucket ({n_buckets})"
                ));
            }
        }
    }
    let executed_censored = count(
        "SELECT COUNT(*) FROM hedges WHERE reason='AFTER_OBSERVATION_END' AND
         (CAST(filled_qty AS REAL)!=0 OR CAST(gross_pnl AS REAL)!=0 OR CAST(net_pnl AS REAL)!=0
          OR CAST(aster_fee AS REAL)!=0 OR CAST(hl_fee AS REAL)!=0)",
    )?;
    if executed_censored > 0 {
        violations.push(format!("{executed_censored} censored hedges incorrectly book execution or P&L"));
    }

    // Informational — the honesty signals this build records (never failures by
    // themselves; they quantify exposure the report surfaces).
    let unbooked = count("SELECT COUNT(*) FROM hedges WHERE reason='MISSING_HL_BOOK'")?;
    // EOF censoring and an unavailable book do not demonstrate exhausted depth.
    let exhausted = count("SELECT COUNT(*) FROM hedges WHERE depth_exhausted=1 AND reason IS NULL")?;
    let censored = count("SELECT COUNT(*) FROM hedges WHERE reason='AFTER_OBSERVATION_END'")?;
    let unpriced = count("SELECT COUNT(*) FROM hedges WHERE reason='UNPRICED_HEDGE'")?;
    let stale_fills = count("SELECT COUNT(*) FROM simulated_fills WHERE feed_stale_at_fill=1")?;
    let trunc_fills = count("SELECT COUNT(*) FROM simulated_fills WHERE queue_truncated=1")?;
    let underhedged: f64 = conn.query_row(
        "SELECT COALESCE(SUM(CAST(qty AS REAL) - CAST(filled_qty AS REAL)),0.0) FROM hedges",
        [],
        |r| r.get(0),
    )?;
    println!(
        "  risk signals: unbooked_hedges={unbooked} unpriced_hedges={unpriced} censored_hedges={censored} \
         depth_exhausted={exhausted} stale_window_fills={stale_fills} queue_truncated_fills={trunc_fills} \
         unfilled_or_unresolved_qty={underhedged:.6}"
    );

    Ok(violations)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database(version: i64) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(crate::store::schema::SCHEMA).unwrap();
        conn.execute(
            "INSERT INTO runs(run_id,started_at,finished_at,mode,config_json,semantics_version)
             VALUES('r','2026-01-01T00:00:00Z','2026-01-01T00:00:01Z','replay',?1,?2)",
            rusqlite::params![r#"{"simulation":{"hedge_latency_buckets_ms":[0,50,100]}}"#,version],
        ).unwrap();
        conn
    }

    fn fill(conn: &Connection, id: &str, latency: i64) {
        conn.execute(
            "INSERT INTO simulated_fills(id,run_id,quote_id,market,queue_model,latency_bucket_ms,
             aster_side,fill_px,fill_qty,sweep_print_px,quoted_edge_bps,quoted_distance_bps,
             remaining_quote_qty_after_fill,was_trade_through,was_partial,feed_stale_at_fill,
             queue_truncated,exch_ts,local_recv_ts)
             VALUES(?1,'r',?1,'BTC','optimistic',?2,'buy','100','1','99','0','0','0',0,0,0,0,
                    '2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",
            rusqlite::params![id,latency],
        ).unwrap();
    }

    fn hedge(conn: &Connection, id: &str, fill: &str, latency: i64, reason: Option<&str>) {
        conn.execute(
            "INSERT INTO hedges(id,run_id,fill_id,market,queue_model,hedge_side,qty,filled_qty,
             aster_fill_px,hl_vwap,latency_bucket_ms,gross_pnl,aster_fee,hl_fee,net_pnl,
             realized_edge_bps,depth_exhausted,hedged_on_stale_book,fill_local_ts,resolve_ts,hl_book_ts,reason)
             VALUES(?1,'r',?2,'BTC','optimistic','sell','1',?3,'100','100',?4,'0','0','0','0','0',?5,0,
                    '2026-01-01T00:00:00Z','2026-01-01T00:00:02Z','2026-01-01T00:00:00Z',?6)",
            rusqlite::params![id,fill,if reason.is_some() { "0" } else { "1" },latency,reason.is_some(),reason],
        ).unwrap();
    }

    #[test]
    fn independent_scenario_fills_each_have_one_hedge_including_censoring() {
        let conn = database(2);
        for (id,latency,reason) in [("a",0,None),("b",50,Some("UNPRICED_HEDGE")),
            ("c",100,Some("AFTER_OBSERVATION_END"))] {
            fill(&conn,id,latency);
            hedge(&conn,id,id,latency,reason);
        }
        fill(&conn,"accumulated",50); // No hedge is legitimate below the venue minimum.
        assert!(audit(&conn).unwrap().is_empty());
        conn.execute("UPDATE hedges SET filled_qty='0.1' WHERE id='c'",[]).unwrap();
        assert!(audit(&conn).unwrap().iter().any(|v|v.contains("censored hedges")));
    }

    #[test]
    fn scenario_mismatch_and_duplicate_hedges_are_rejected() {
        let conn = database(2);
        fill(&conn,"a",0);
        hedge(&conn,"a","a",50,None);
        assert!(audit(&conn).unwrap().iter().any(|v|v.contains("own latency scenario")));
        conn.execute("UPDATE hedges SET latency_bucket_ms=0,market='ETH'",[]).unwrap();
        assert!(audit(&conn).unwrap().iter().any(|v|v.contains("another scenario")));
        conn.execute("UPDATE hedges SET market='BTC'",[]).unwrap();
        hedge(&conn,"duplicate","a",0,None);
        assert!(audit(&conn).unwrap().iter().any(|v|v.contains("more than one")));
    }

    #[test]
    fn legacy_shared_fill_requires_all_configured_distinct_buckets() {
        let conn = database(1);
        // Also exercise databases predating both new columns.
        conn.execute_batch("ALTER TABLE runs DROP COLUMN semantics_version;
            ALTER TABLE simulated_fills DROP COLUMN latency_bucket_ms;").unwrap();
        conn.execute(
            "INSERT INTO simulated_fills(id,run_id,quote_id,market,queue_model,aster_side,fill_px,fill_qty,
             sweep_print_px,quoted_edge_bps,quoted_distance_bps,remaining_quote_qty_after_fill,
             was_trade_through,was_partial,feed_stale_at_fill,queue_truncated,exch_ts,local_recv_ts)
             VALUES('a','r','a','BTC','optimistic','buy','100','1','99','0','0','0',0,0,0,0,
                    '2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')",[],
        ).unwrap();
        for (id,latency) in [("a",0),("b",50),("c",100)] { hedge(&conn,id,"a",latency,None); }
        assert!(audit(&conn).unwrap().is_empty());
        conn.execute("UPDATE hedges SET latency_bucket_ms=50 WHERE id='c'",[]).unwrap();
        assert!(audit(&conn).unwrap().iter().any(|v|v.contains("legacy fill")));
        conn.execute("DELETE FROM hedges WHERE id='c'",[]).unwrap();
        assert!(audit(&conn).unwrap().iter().any(|v|v.contains("legacy fill")));
    }
}
