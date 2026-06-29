//! Aggregate a replayed run into the headline evaluator metrics: opportunity
//! accept/reject distribution, fills, and — the product — realized PnL and
//! realized edge per latency bucket, with the instant-vs-realized decay that
//! quantifies adverse selection, plus per-leg capital usage.
//!
//! CRITICAL: latency buckets and queue models are *alternative* scenarios, never
//! additive. We never sum net PnL across buckets (that would multiply a single
//! scenario by the bucket count) or across models (three hypotheticals over the
//! same tape). The headline PnL is reported per model at the primary (smallest)
//! latency bucket; the full per-bucket table shows the decay.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use rusqlite::Connection;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ReportSummary {
    pub run_id: String,
    pub markets: Vec<MarketReport>,
    /// Fills summed across the queue-model worlds (each model is a separate
    /// hypothetical over the same tape — NOT additive strategies).
    pub total_fills: i64,
    pub total_hedges: i64,
    /// The smallest configured latency bucket; the headline PnL is reported here.
    pub primary_bucket_ms: i64,
    /// Net PnL per queue model, summed across markets, at the primary bucket. The
    /// three models are alternatives — compare them, never add them.
    pub net_pnl_by_model: Vec<(String, f64)>,
    /// Sum of the run's safety buffers (slippage+latency+basis+funding), in bps,
    /// read from the run's own config snapshot. `instant_edge` is net of fees and
    /// these buffers; `realized_edge` is net of fees only, so realized sits ≈ this
    /// much above instant by construction (not a sign bug).
    pub buffer_bps: f64,
}

#[derive(Debug, Serialize)]
pub struct MarketReport {
    pub market: String,
    pub opportunities_accepted: i64,
    pub opportunities_rejected: i64,
    pub reject_reasons: Vec<(String, i64)>,
    /// Fills across all models for this market (sum of the per-model worlds).
    pub fills: i64,
    pub fill_notional: f64,
    pub models: Vec<ModelReport>,
    pub pending_events: Vec<(String, i64)>,
}

#[derive(Debug, Serialize)]
pub struct ModelReport {
    pub queue_model: String,
    pub fills: i64,
    /// Mean instant edge net of fees AND buffers (the quoting threshold).
    pub mean_instant_edge_bps: Option<f64>,
    /// Same, but net of fees ONLY (= mean_instant + buffers). This is the basis
    /// directly comparable to `mean_realized_edge_bps`, which is also fees-only;
    /// `realized ≈ instant_gross` at the smallest latency bucket on an unchanged book.
    pub mean_instant_edge_gross_bps: Option<f64>,
    /// How deep below the touch our accepted quotes rested (bps).
    pub mean_quote_distance_bps: Option<f64>,
    pub primary_bucket_ms: i64,
    /// Net PnL at the primary (smallest) latency bucket. Buckets are alternative
    /// latency scenarios, never summed.
    pub net_pnl_primary: f64,
    pub peak_aster_notional: f64,
    pub peak_hl_notional: f64,
    pub aster_cap: f64,
    pub hl_cap: f64,
    pub cap_blocked: i64,
    /// Accepted quotes whose size was clamped UP to the venue minimum lot
    /// (desired_notional below the minimum, e.g. $50 on BTC).
    pub size_clamped: i64,
    /// Fills that landed while the matched feed was stale (stale-window adverse fills
    /// — a quote hit during its cancel round-trip on a feed we no longer trusted).
    pub stale_window_fills: i64,
    /// Fills on quotes resting beyond captured depth20 (queue ahead under-observed, so
    /// the fill may be optimistic).
    pub queue_truncated_fills: i64,
    pub buckets: Vec<BucketReport>,
}

#[derive(Debug, Serialize)]
pub struct BucketReport {
    pub latency_bucket_ms: i64,
    pub n_hedges: i64,
    /// Total net PnL across all fills in this single latency scenario (correct to sum).
    pub total_net_pnl: f64,
    pub mean_realized_edge_bps: f64,
    pub n_stale: i64,
    pub n_depth_exhausted: i64,
    /// Requested-minus-filled hedge base qty summed over this bucket: volume that could
    /// NOT be hedged (thin or absent HL book). Non-zero => realized edge is on less than
    /// full size — pair with `n_depth_exhausted`.
    pub underhedged_qty: f64,
}

/// Generate the report for `run_id` (or the latest run) from `db_path`, printing
/// to the console and writing report.json / report.csv into `out_dir`.
pub fn generate(
    db_path: impl AsRef<Path>,
    run_id: Option<String>,
    out_dir: impl AsRef<Path>,
) -> Result<ReportSummary> {
    let conn = Connection::open(db_path.as_ref())
        .with_context(|| format!("opening {}", db_path.as_ref().display()))?;

    let run_id = match run_id {
        Some(r) => r,
        None => conn
            .query_row("SELECT run_id FROM runs ORDER BY rowid DESC LIMIT 1", [], |r| r.get(0))
            .map_err(|_| anyhow!("no runs found in database"))?,
    };

    // Per-leg capital caps + safety-buffer total from the run's own config snapshot
    // (same for all markets). Reading from the snapshot — not the current config.toml —
    // keeps the report self-contained and correct even if config later drifts.
    let (aster_cap, hl_cap) = caps_from_run(&conn, &run_id);
    let buffer_bps = buffers_from_run(&conn, &run_id);
    // Headline latency = the smallest bucket actually present (fallback 0).
    let primary_bucket_ms: i64 = conn
        .query_row(
            "SELECT COALESCE(MIN(latency_bucket_ms), 0) FROM hedges WHERE run_id = ?1",
            [&run_id],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let markets = market_list(&conn, &run_id)?;
    let mut market_reports = Vec::new();
    for market in markets {
        market_reports.push(build_market_report(
            &conn,
            &run_id,
            &market,
            primary_bucket_ms,
            aster_cap,
            hl_cap,
            buffer_bps,
        )?);
    }

    let total_fills: i64 = market_reports.iter().map(|m| m.fills).sum();
    let total_hedges: i64 =
        conn.query_row("SELECT COUNT(*) FROM hedges WHERE run_id = ?1", [&run_id], |r| r.get(0))?;

    // Net PnL per model = sum ACROSS MARKETS (additive) of each model's primary-bucket
    // PnL. Never across models or buckets.
    let mut by_model: BTreeMap<String, f64> = BTreeMap::new();
    for m in &market_reports {
        for mr in &m.models {
            *by_model.entry(mr.queue_model.clone()).or_default() += mr.net_pnl_primary;
        }
    }
    let net_pnl_by_model: Vec<(String, f64)> = by_model.into_iter().collect();

    let summary = ReportSummary {
        run_id,
        markets: market_reports,
        total_fills,
        total_hedges,
        primary_bucket_ms,
        net_pnl_by_model,
        buffer_bps,
    };

    print_console(&summary);
    write_artifacts(&summary, out_dir.as_ref())?;
    Ok(summary)
}

fn caps_from_run(conn: &Connection, run_id: &str) -> (f64, f64) {
    let config_json: String = conn
        .query_row("SELECT config_json FROM runs WHERE run_id = ?1", [run_id], |r| r.get(0))
        .unwrap_or_default();
    match serde_json::from_str::<crate::config::Config>(&config_json) {
        Ok(c) => (
            dec_f64(c.capital.aster_cap_notional()),
            dec_f64(c.capital.hyperliquid_cap_notional()),
        ),
        Err(_) => (0.0, 0.0),
    }
}

fn dec_f64(d: rust_decimal::Decimal) -> f64 {
    d.to_string().parse().unwrap_or(0.0)
}

/// Sum of the run's safety buffers (slippage+latency+basis+funding), in bps, from
/// the run's config snapshot. 0.0 if the snapshot is missing/unparseable.
fn buffers_from_run(conn: &Connection, run_id: &str) -> f64 {
    let config_json: String = conn
        .query_row("SELECT config_json FROM runs WHERE run_id = ?1", [run_id], |r| r.get(0))
        .unwrap_or_default();
    match serde_json::from_str::<crate::config::Config>(&config_json) {
        Ok(c) => dec_f64(c.edge.total_buffer_bps()),
        Err(_) => 0.0,
    }
}

fn market_list(conn: &Connection, run_id: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare("SELECT market FROM markets WHERE run_id = ?1 ORDER BY market")?;
    let rows = stmt.query_map([run_id], |r| r.get::<_, String>(0))?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}

fn build_market_report(
    conn: &Connection,
    run_id: &str,
    market: &str,
    primary_bucket_ms: i64,
    aster_cap: f64,
    hl_cap: f64,
    buffer_bps: f64,
) -> Result<MarketReport> {
    let accepted: i64 = conn.query_row(
        "SELECT COALESCE(SUM(accepted),0) FROM opportunity_stats WHERE run_id=?1 AND market=?2",
        rusqlite::params![run_id, market],
        |r| r.get(0),
    )?;
    let rejected: i64 = conn.query_row(
        "SELECT COUNT(*) FROM opportunity_rejects WHERE run_id=?1 AND market=?2",
        rusqlite::params![run_id, market],
        |r| r.get(0),
    )?;

    let mut reject_reasons = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT reject_reason, COUNT(*) n FROM opportunity_rejects WHERE run_id=?1 AND market=?2
             GROUP BY reject_reason ORDER BY n DESC",
        )?;
        let rows = stmt.query_map(rusqlite::params![run_id, market], |r| {
            Ok((r.get::<_, Option<String>>(0)?.unwrap_or_default(), r.get::<_, i64>(1)?))
        })?;
        for row in rows {
            reject_reasons.push(row?);
        }
    }

    let (fills, fill_notional) = conn.query_row(
        "SELECT COUNT(*), COALESCE(SUM(CAST(fill_qty AS REAL)*CAST(fill_px AS REAL)),0.0)
         FROM simulated_fills WHERE run_id=?1 AND market=?2",
        rusqlite::params![run_id, market],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, f64>(1)?)),
    )?;

    // queue models present for this market
    let models: Vec<String> = {
        let mut stmt = conn.prepare(
            "SELECT queue_model FROM opportunity_stats WHERE run_id=?1 AND market=?2
             UNION
             SELECT queue_model FROM opportunity_rejects WHERE run_id=?1 AND market=?2
             ORDER BY queue_model",
        )?;
        let rows = stmt.query_map(rusqlite::params![run_id, market], |r| r.get::<_, String>(0))?;
        rows.collect::<std::result::Result<_, _>>()?
    };

    let mut model_reports = Vec::new();
    for qm in models {
        let p = rusqlite::params![run_id, market, qm];

        let mean_instant: Option<f64> = conn.query_row(
            "SELECT CASE WHEN SUM(accepted)>0 THEN SUM(sum_instant_edge_bps)/SUM(accepted) END
             FROM opportunity_stats WHERE run_id=?1 AND market=?2 AND queue_model=?3",
            p,
            |r| r.get::<_, Option<f64>>(0),
        )?;
        let mean_distance: Option<f64> = conn.query_row(
            "SELECT CASE WHEN SUM(accepted)>0 THEN SUM(sum_distance_bps)/SUM(accepted) END
             FROM opportunity_stats WHERE run_id=?1 AND market=?2 AND queue_model=?3",
            p,
            |r| r.get::<_, Option<f64>>(0),
        )?;
        let fills_m: i64 = conn.query_row(
            "SELECT COUNT(*) FROM simulated_fills WHERE run_id=?1 AND market=?2 AND queue_model=?3",
            p,
            |r| r.get(0),
        )?;
        let peak_aster: f64 = conn.query_row(
            "SELECT COALESCE(MAX(ABS(CAST(aster_pos_notional AS REAL))),0.0)
             FROM simulated_fills WHERE run_id=?1 AND market=?2 AND queue_model=?3",
            p,
            |r| r.get(0),
        )?;
        let peak_hl: f64 = conn.query_row(
            "SELECT COALESCE(MAX(ABS(CAST(hl_pos_notional AS REAL))),0.0)
             FROM simulated_fills WHERE run_id=?1 AND market=?2 AND queue_model=?3",
            p,
            |r| r.get(0),
        )?;
        let cap_blocked: i64 = conn.query_row(
            "SELECT COUNT(*) FROM opportunity_rejects WHERE run_id=?1 AND market=?2 AND queue_model=?3
             AND reject_reason IN (
                'ASTER_POSITION_CAP_REACHED',
                'LIGHTER_POSITION_CAP_REACHED',
                'HYPERLIQUID_POSITION_CAP_REACHED'
             )",
            p,
            |r| r.get(0),
        )?;
        let size_clamped: i64 = conn.query_row(
            "SELECT COALESCE(SUM(size_clamped),0) FROM opportunity_stats
             WHERE run_id=?1 AND market=?2 AND queue_model=?3",
            p,
            |r| r.get(0),
        )?;
        let stale_window_fills: i64 = conn.query_row(
            "SELECT COALESCE(SUM(feed_stale_at_fill),0) FROM simulated_fills
             WHERE run_id=?1 AND market=?2 AND queue_model=?3",
            p,
            |r| r.get(0),
        )?;
        let queue_truncated_fills: i64 = conn.query_row(
            "SELECT COALESCE(SUM(queue_truncated),0) FROM simulated_fills
             WHERE run_id=?1 AND market=?2 AND queue_model=?3",
            p,
            |r| r.get(0),
        )?;

        let mut buckets = Vec::new();
        let mut stmt = conn.prepare(
            "SELECT latency_bucket_ms, COUNT(*), COALESCE(SUM(CAST(net_pnl AS REAL)),0.0),
                    COALESCE(AVG(CAST(realized_edge_bps AS REAL)),0.0),
                    COALESCE(SUM(hedged_on_stale_book),0), COALESCE(SUM(depth_exhausted),0),
                    COALESCE(SUM(CAST(qty AS REAL) - CAST(filled_qty AS REAL)),0.0)
             FROM hedges WHERE run_id=?1 AND market=?2 AND queue_model=?3
             GROUP BY latency_bucket_ms ORDER BY latency_bucket_ms",
        )?;
        let rows = stmt.query_map(p, |r| {
            Ok(BucketReport {
                latency_bucket_ms: r.get(0)?,
                n_hedges: r.get(1)?,
                total_net_pnl: r.get(2)?,
                mean_realized_edge_bps: r.get(3)?,
                n_stale: r.get(4)?,
                n_depth_exhausted: r.get(5)?,
                underhedged_qty: r.get(6)?,
            })
        })?;
        for b in rows {
            buckets.push(b?);
        }
        // Headline = the primary bucket's total (NOT a sum across buckets).
        let net_pnl_primary = buckets
            .iter()
            .find(|b| b.latency_bucket_ms == primary_bucket_ms)
            .map(|b| b.total_net_pnl)
            .unwrap_or(0.0);

        model_reports.push(ModelReport {
            queue_model: qm,
            fills: fills_m,
            mean_instant_edge_bps: mean_instant,
            // Fees-only basis, comparable to realized: instant (net of fees+buffers) + buffers.
            mean_instant_edge_gross_bps: mean_instant.map(|v| v + buffer_bps),
            mean_quote_distance_bps: mean_distance,
            primary_bucket_ms,
            net_pnl_primary,
            peak_aster_notional: peak_aster,
            peak_hl_notional: peak_hl,
            aster_cap,
            hl_cap,
            cap_blocked,
            size_clamped,
            stale_window_fills,
            queue_truncated_fills,
            buckets,
        });
    }

    let mut pending_events = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT event_type, COUNT(*) FROM pending_inventory_events WHERE run_id=?1 AND market=?2
             GROUP BY event_type ORDER BY event_type",
        )?;
        let rows = stmt.query_map(rusqlite::params![run_id, market], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?;
        for row in rows {
            pending_events.push(row?);
        }
    }

    Ok(MarketReport {
        market: market.to_string(),
        opportunities_accepted: accepted,
        opportunities_rejected: rejected,
        reject_reasons,
        fills,
        fill_notional,
        models: model_reports,
        pending_events,
    })
}

fn print_console(s: &ReportSummary) {
    println!("\n=== XEMM dry-run report  (run {}) ===", s.run_id);
    println!(
        "  edge bases: instant_edge is net of fees + {:.1}bps buffers (the quoting threshold); \
         instant_gross = instant + buffers is net of fees only and is the basis comparable to \
         realized_edge.",
        s.buffer_bps
    );
    println!(
        "  realized sits ~+{:.1}bps above instant by construction (expected, NOT a sign bug); \
         adverse selection shows as realized_edge DECAY across latency buckets (50ms >= 1000ms).",
        s.buffer_bps
    );
    for m in &s.markets {
        println!("\n## {}", m.market);
        println!(
            "  opportunities: {} accepted / {} rejected   fills: {} (notional ~{:.2})",
            m.opportunities_accepted, m.opportunities_rejected, m.fills, m.fill_notional
        );
        if !m.reject_reasons.is_empty() && m.opportunities_accepted == 0 {
            let top: Vec<String> = m
                .reject_reasons
                .iter()
                .take(4)
                .map(|(r, n)| format!("{r}={n}"))
                .collect();
            println!("  rejects: {}", top.join("  "));
        }
        for mr in &m.models {
            let instant = mr
                .mean_instant_edge_bps
                .map(|v| format!("{v:.2}"))
                .unwrap_or_else(|| "-".into());
            let instant_gross = mr
                .mean_instant_edge_gross_bps
                .map(|v| format!("{v:.2}"))
                .unwrap_or_else(|| "-".into());
            let dist = mr
                .mean_quote_distance_bps
                .map(|v| format!("{v:.1}"))
                .unwrap_or_else(|| "-".into());
            println!(
                "  [{}]  fills={}  net_pnl@{}ms={:+.5}  instant_edge={}/{} bps (net/gross)  quote_depth={} bps",
                mr.queue_model,
                mr.fills,
                mr.primary_bucket_ms,
                mr.net_pnl_primary,
                instant,
                instant_gross,
                dist
            );
            println!(
                "       capital: peak_aster=${:.2}  peak_hl=${:.2}  / ${:.0} cap   cap_blocked={}   min_lot_clamped={}",
                mr.peak_aster_notional, mr.peak_hl_notional, mr.aster_cap, mr.cap_blocked, mr.size_clamped
            );
            println!(
                "       honesty: stale_window_fills={}  queue_truncated_fills={}",
                mr.stale_window_fills, mr.queue_truncated_fills
            );
            if !mr.buckets.is_empty() {
                println!(
                    "      {:>8} {:>8} {:>14} {:>16} {:>7} {:>7} {:>10}",
                    "lat_ms", "hedges", "net_pnl", "real_edge_bps", "stale", "thin", "unhedged"
                );
                for b in &mr.buckets {
                    println!(
                        "      {:>8} {:>8} {:>+14.5} {:>16.3} {:>7} {:>7} {:>10.4}",
                        b.latency_bucket_ms,
                        b.n_hedges,
                        b.total_net_pnl,
                        b.mean_realized_edge_bps,
                        b.n_stale,
                        b.n_depth_exhausted,
                        b.underhedged_qty
                    );
                }
            }
        }
        if !m.pending_events.is_empty() {
            let pe: Vec<String> = m.pending_events.iter().map(|(t, n)| format!("{t}={n}")).collect();
            println!("  pending inventory: {}", pe.join("  "));
        }
    }
    println!(
        "\n== TOTAL ==  fills={} (across models)  hedges={}",
        s.total_fills, s.total_hedges
    );
    for (model, pnl) in &s.net_pnl_by_model {
        println!("   [{}]  net_pnl@{}ms = {:+.5}", model, s.primary_bucket_ms, pnl);
    }
    println!();
}

fn write_artifacts(s: &ReportSummary, out_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(out_dir).ok();
    let json_path = out_dir.join("report.json");
    std::fs::write(&json_path, serde_json::to_string_pretty(s)?)?;

    let csv_path = out_dir.join("report.csv");
    let mut w = std::fs::File::create(&csv_path)?;
    writeln!(
        w,
        "market,queue_model,latency_bucket_ms,n_hedges,total_net_pnl,mean_realized_edge_bps,mean_instant_edge_bps,mean_instant_edge_gross_bps,mean_quote_distance_bps,peak_aster_notional,peak_hl_notional,cap_blocked,size_clamped,n_stale,n_depth_exhausted,underhedged_qty,stale_window_fills,queue_truncated_fills"
    )?;
    for m in &s.markets {
        for mr in &m.models {
            let instant = mr.mean_instant_edge_bps.map(|v| v.to_string()).unwrap_or_default();
            let instant_gross =
                mr.mean_instant_edge_gross_bps.map(|v| v.to_string()).unwrap_or_default();
            let dist = mr.mean_quote_distance_bps.map(|v| v.to_string()).unwrap_or_default();
            for b in &mr.buckets {
                writeln!(
                    w,
                    "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                    m.market,
                    mr.queue_model,
                    b.latency_bucket_ms,
                    b.n_hedges,
                    b.total_net_pnl,
                    b.mean_realized_edge_bps,
                    instant,
                    instant_gross,
                    dist,
                    mr.peak_aster_notional,
                    mr.peak_hl_notional,
                    mr.cap_blocked,
                    mr.size_clamped,
                    b.n_stale,
                    b.n_depth_exhausted,
                    b.underhedged_qty,
                    mr.stale_window_fills,
                    mr.queue_truncated_fills
                )?;
            }
        }
    }
    println!("wrote {} and {}", json_path.display(), csv_path.display());
    Ok(())
}
