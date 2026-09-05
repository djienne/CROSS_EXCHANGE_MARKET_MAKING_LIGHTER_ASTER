//! Independent scenario ledgers. Execution spread is a diagnostic, not additional P&L.
use std::collections::BTreeMap;
use std::path::Path;
use anyhow::{anyhow, Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use rust_decimal::Decimal;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct ReportSummary {
    pub run_id: String,
    pub semantics_version: i64,
    pub markets: Vec<MarketReport>,
    pub total_fills: i64,
    pub total_hedges: i64,
    pub primary_bucket_ms: i64,
    pub net_pnl_by_model: Vec<(String, Option<Decimal>)>,
    pub buffer_bps: Decimal,
}
#[derive(Debug, Serialize)]
pub struct MarketReport {
    pub market: String,
    pub models: Vec<ModelReport>,
}
#[derive(Debug, Serialize)]
pub struct ModelReport {
    pub queue_model: String,
    pub primary_bucket_ms: i64,
    pub net_pnl_primary: Option<Decimal>,
    pub buckets: Vec<BucketReport>,
}
#[derive(Debug, Serialize)]
pub struct BucketReport {
    pub latency_bucket_ms: i64,
    pub legacy_shared_trajectory: bool,
    pub fills: i64,
    pub opportunities_accepted: i64,
    pub opportunities_rejected: i64,
    pub mean_instant_edge_bps: Option<f64>,
    pub mean_quote_distance_bps: Option<f64>,
    pub n_hedges: i64,
    pub n_censored_hedges: i64,
    pub n_unpriced_hedges: i64,
    pub captured_spread_pnl: f64,
    pub mean_realized_edge_bps: Option<f64>,
    pub n_stale: i64,
    pub n_depth_exhausted: i64,
    pub underhedged_qty: f64,
    pub realized_gross: Option<Decimal>,
    pub fees: Option<Decimal>,
    pub unrealized_pnl: Option<Decimal>,
    pub total_net_pnl: Option<Decimal>,
    pub aster_qty: Option<Decimal>,
    pub lighter_qty: Option<Decimal>,
    pub residual_qty: Option<Decimal>,
    pub reserved_hedge_qty: Option<Decimal>,
    pub unpriced_hedge_qty: Option<Decimal>,
    pub peak_aster_notional: Option<Decimal>,
    pub peak_lighter_notional: Option<Decimal>,
    pub valuation_complete: bool,
    pub frozen_reason: Option<String>,
}

pub fn generate(db_path: impl AsRef<Path>, run_id: Option<String>, out_dir: impl AsRef<Path>) -> Result<ReportSummary> {
    let conn = Connection::open_with_flags(db_path.as_ref(), OpenFlags::SQLITE_OPEN_READ_ONLY)
        .with_context(|| format!("opening {}", db_path.as_ref().display()))?;
    let run_id = run_id.or_else(|| conn.query_row(
        "SELECT run_id FROM runs ORDER BY rowid DESC LIMIT 1", [], |r| r.get(0)).ok())
        .ok_or_else(|| anyhow!("no runs found in database"))?;
    let summary = summarize(&conn, &run_id)?;
    print_console(&summary);
    write_artifacts(&summary, out_dir.as_ref())?;
    Ok(summary)
}

pub fn summarize(conn: &Connection, run_id: &str) -> Result<ReportSummary> {
    let version = conn.query_row("SELECT semantics_version FROM runs WHERE run_id=?1", [run_id], |r| r.get::<_, i64>(0)).unwrap_or(1);
    let modern = version >= 2;
    let raw_config: String = conn.query_row("SELECT config_json FROM runs WHERE run_id=?1", [run_id], |r| r.get(0))?;
    let cfg = serde_json::from_str::<crate::config::Config>(&raw_config).ok();
    let primary = cfg.as_ref().and_then(|c| c.simulation.hedge_latency_buckets_ms.iter().min().copied())
        .or_else(|| conn.query_row("SELECT MIN(latency_bucket_ms) FROM hedges WHERE run_id=?1", [run_id], |r| r.get(0)).ok()).unwrap_or(0);
    let mut q = conn.prepare("SELECT market FROM markets WHERE run_id=?1 ORDER BY market")?;
    let ids = q.query_map([run_id], |r| r.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut markets = Vec::new();
    let mut by_model: BTreeMap<String, Option<Decimal>> = BTreeMap::new();
    let (mut total_fills, mut total_hedges) = (0, 0);
    for market in ids {
        let mut sql = "SELECT queue_model FROM opportunity_stats WHERE run_id=?1 AND market=?2
            UNION SELECT queue_model FROM opportunity_rejects WHERE run_id=?1 AND market=?2".to_string();
        if modern { sql.push_str(" UNION SELECT queue_model FROM scenario_results WHERE run_id=?1 AND market=?2"); }
        sql.push_str(" ORDER BY queue_model");
        let mut q = conn.prepare(&sql)?;
        let model_ids = q.query_map(params![run_id, market], |r| r.get::<_, String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        let mut models = Vec::new();
        for model in model_ids {
            let sql = if modern {
                "SELECT latency_bucket_ms FROM scenario_results WHERE run_id=?1 AND market=?2 AND queue_model=?3
                 UNION SELECT latency_bucket_ms FROM hedges WHERE run_id=?1 AND market=?2 AND queue_model=?3 ORDER BY latency_bucket_ms"
            } else {
                "SELECT DISTINCT latency_bucket_ms FROM hedges WHERE run_id=?1 AND market=?2 AND queue_model=?3 ORDER BY latency_bucket_ms"
            };
            let mut q = conn.prepare(sql)?;
            let mut latencies = q.query_map(params![run_id, market, model], |r| r.get::<_, i64>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            if latencies.is_empty() { latencies.push(primary); }
            let mut buckets = Vec::new();
            for latency in latencies {
                let b = scenario(conn, run_id, &market, &model, latency, modern)?;
                if modern || latency == primary { total_fills += b.fills; }
                total_hedges += b.n_hedges;
                buckets.push(b);
            }
            let net = buckets.iter().find(|b| b.latency_bucket_ms == primary).and_then(|b| b.total_net_pnl);
            let aggregate = by_model.entry(model.clone()).or_insert(Some(Decimal::ZERO));
            *aggregate = aggregate.zip(net).map(|(sum, n)| sum+n);
            models.push(ModelReport { queue_model: model, primary_bucket_ms: primary, net_pnl_primary: net, buckets });
        }
        markets.push(MarketReport { market, models });
    }
    Ok(ReportSummary { run_id: run_id.into(), semantics_version: version, markets, total_fills, total_hedges,
        primary_bucket_ms: primary, net_pnl_by_model: by_model.into_iter().collect(),
        buffer_bps: cfg.map(|c| c.edge.total_buffer_bps()).unwrap_or_default() })
}

fn scenario(conn: &Connection, run: &str, market: &str, model: &str, latency: i64, modern: bool) -> Result<BucketReport> {
    let filter = if modern { format!(" AND latency_bucket_ms={latency}") } else { String::new() };
    let p = params![run, market, model];
    let fills = conn.query_row(&format!("SELECT COUNT(*) FROM simulated_fills WHERE run_id=?1 AND market=?2 AND queue_model=?3{filter}"), p, |r| r.get(0))?;
    let (accepted, instant, distance) = conn.query_row(&format!(
        "SELECT COALESCE(SUM(accepted),0),CASE WHEN SUM(accepted)>0 THEN SUM(sum_instant_edge_bps)/SUM(accepted) END,
         CASE WHEN SUM(accepted)>0 THEN SUM(sum_distance_bps)/SUM(accepted) END
         FROM opportunity_stats WHERE run_id=?1 AND market=?2 AND queue_model=?3{filter}"), p,
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let rejected = conn.query_row(&format!("SELECT COUNT(*) FROM opportunity_rejects WHERE run_id=?1 AND market=?2 AND queue_model=?3{filter}"), p, |r| r.get(0))?;
    let (n_hedges, spread, edge, stale, thin, remainder, censored, unpriced) = conn.query_row(
        "SELECT COALESCE(SUM(CAST(filled_qty AS REAL)>0),0),COALESCE(SUM(CAST(net_pnl AS REAL)),0.0),
         AVG(CASE WHEN CAST(filled_qty AS REAL)>0 THEN CAST(realized_edge_bps AS REAL) END),
         COALESCE(SUM(hedged_on_stale_book),0),COALESCE(SUM(CASE WHEN reason IS NULL THEN depth_exhausted ELSE 0 END),0),
         COALESCE(SUM(CAST(qty AS REAL)-CAST(filled_qty AS REAL)),0.0),
         COALESCE(SUM(reason='AFTER_OBSERVATION_END'),0),COALESCE(SUM(reason='UNPRICED_HEDGE'),0)
         FROM hedges WHERE run_id=?1 AND market=?2 AND queue_model=?3 AND latency_bucket_ms=?4",
        params![run,market,model,latency], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?)))?;
    let mut b = BucketReport { latency_bucket_ms: latency, legacy_shared_trajectory: !modern,
        fills, opportunities_accepted: accepted, opportunities_rejected: rejected,
        mean_instant_edge_bps: instant, mean_quote_distance_bps: distance,
        n_hedges, n_censored_hedges: censored, n_unpriced_hedges: unpriced,
        captured_spread_pnl: spread, mean_realized_edge_bps: edge,
        n_stale: stale, n_depth_exhausted: thin, underhedged_qty: remainder,
        realized_gross: None, fees: None, unrealized_pnl: None, total_net_pnl: None,
        aster_qty: None, lighter_qty: None, residual_qty: None, reserved_hedge_qty: None, unpriced_hedge_qty: None,
        peak_aster_notional: None, peak_lighter_notional: None, valuation_complete: false, frozen_reason: None };
    if modern {
        let values = conn.query_row(
            "SELECT aster_qty,lighter_qty,realized_gross,fees,unrealized_pnl,net_pnl,residual_qty,
             reserved_hedge_qty,unpriced_hedge_qty,peak_aster_notional,peak_lighter_notional,valuation_complete,frozen_reason
             FROM scenario_results WHERE run_id=?1 AND market=?2 AND queue_model=?3 AND latency_bucket_ms=?4",
            params![run,market,model,latency], |r| {
                let values = (0..11).map(|i| r.get::<_, Option<String>>(i)).collect::<rusqlite::Result<Vec<_>>>()?;
                Ok((values,r.get::<_,bool>(11)?,r.get::<_,Option<String>>(12)?))
            }).optional()?;
        if let Some((raw, complete, reason)) = values {
            let d = raw.into_iter().map(|s| s.map(|v| v.parse::<Decimal>()).transpose()).collect::<std::result::Result<Vec<_>,_>>()?;
            b.aster_qty=d[0]; b.lighter_qty=d[1]; b.realized_gross=d[2]; b.fees=d[3]; b.unrealized_pnl=d[4];
            b.total_net_pnl=d[5].filter(|_| complete); b.residual_qty=d[6]; b.reserved_hedge_qty=d[7];
            b.unpriced_hedge_qty=d[8]; b.peak_aster_notional=d[9]; b.peak_lighter_notional=d[10];
            b.valuation_complete=complete && b.total_net_pnl.is_some(); b.frozen_reason=reason;
        }
    }
    Ok(b)
}

fn number(value: Option<Decimal>) -> String {
    value.map(|d|d.normalize().to_string()).unwrap_or_default()
}
fn print_console(s: &ReportSummary) {
    println!("\nXEMM simulation {} (semantics v{})", s.run_id, s.semantics_version);
    println!("Queue/latency scenarios are alternatives. Funding is excluded from this model.");
    if s.semantics_version < 2 { println!("Legacy shared trajectories: spread diagnostics only; complete position P&L is unavailable."); }
    for market in &s.markets {
        for model in &market.models {
            for b in &model.buckets {
                let net = b.total_net_pnl.map(|d|d.to_string()).unwrap_or_else(||"unavailable".into());
                println!("{} {} {}ms: fills={} hedges={} censored={} unpriced={} net={} realized={} fees={} unrealized={} residual={} frozen={}",
                    market.market,model.queue_model,b.latency_bucket_ms,b.fills,b.n_hedges,b.n_censored_hedges,b.n_unpriced_hedges,net,
                    number(b.realized_gross),number(b.fees),number(b.unrealized_pnl),number(b.residual_qty),
                    b.frozen_reason.as_deref().unwrap_or("-"));
            }
        }
    }
}

fn write_artifacts(s: &ReportSummary, out: &Path) -> Result<()> {
    std::fs::create_dir_all(out)?;
    std::fs::write(out.join("report.json"),serde_json::to_string_pretty(s)?)?;
    let mut csv = csv::Writer::from_path(out.join("report.csv"))?;
    csv.write_record(["market","queue_model","latency_bucket_ms","fills","n_hedges","total_net_pnl",
        "realized_gross","fees","unrealized_pnl","residual_qty","reserved_hedge_qty","valuation_complete",
        "captured_spread_pnl","mean_realized_edge_bps","legacy_shared_trajectory","frozen_reason","n_censored_hedges","n_unpriced_hedges"])?;
    for market in &s.markets {
        for model in &market.models {
            for b in &model.buckets {
                csv.write_record([market.market.clone(),model.queue_model.clone(),b.latency_bucket_ms.to_string(),
                    b.fills.to_string(),b.n_hedges.to_string(),number(b.total_net_pnl),number(b.realized_gross),number(b.fees),
                    number(b.unrealized_pnl),number(b.residual_qty),number(b.reserved_hedge_qty),b.valuation_complete.to_string(),
                    b.captured_spread_pnl.to_string(),b.mean_realized_edge_bps.map(|v|v.to_string()).unwrap_or_default(),
                    b.legacy_shared_trajectory.to_string(),b.frozen_reason.clone().unwrap_or_default(),
                    b.n_censored_hedges.to_string(),b.n_unpriced_hedges.to_string()])?;
            }
        }
    }
    csv.flush()?;
    Ok(())
}
