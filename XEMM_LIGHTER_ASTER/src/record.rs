//! Recorder: connect to Aster + Lighter for every selected market, stamp
//! each inbound message with a monotonic `(local_recv_ts, seq)` at dequeue (so
//! the log is replay-ordered by construction), and append JSONL after a
//! `RunHeader`. No simulation here — just faithful capture.

use std::path::PathBuf;

use anyhow::Result;
use chrono::Utc;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::{Config, MarketCfg};
use crate::connectors::{aster, lighter, rest_specs};
use crate::events::{open_log_writer, write_event, write_header, Event, RunHeader};

pub struct RecordSummary {
    pub events: u64,
    pub out: PathBuf,
    pub run_id: String,
}

/// Record `duration_secs` of market data for `markets` into `out_path`.
pub async fn run(
    cfg: &Config,
    markets: Vec<MarketCfg>,
    out_path: PathBuf,
    duration_secs: u64,
) -> Result<RecordSummary> {
    if markets.is_empty() {
        anyhow::bail!("no markets selected to record");
    }

    info!("fetching market specs (Aster exchangeInfo + Lighter orderBooks)...");
    let specs = rest_specs::build_market_specs(&markets, cfg.partials.hyperliquid_min_notional).await?;
    for s in &specs {
        info!(
            "  {} aster={} lighter={} market_id={} tick={} step={} sizeDec={}",
            s.market_id, s.aster_symbol, s.hl_coin, s.lighter_market_id, s.tick, s.step, s.hl_sz_decimals
        );
    }

    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).ok();
        }
    }
    let mut writer = open_log_writer(&out_path)?;

    let run_id = Uuid::new_v4().to_string();
    let header = RunHeader {
        run_id: run_id.clone(),
        started_at: Utc::now(),
        mode: "record".to_string(),
        code_version: env!("CARGO_PKG_VERSION").to_string(),
        config: cfg.clone(),
        market_specs: specs.clone(),
    };
    write_header(&mut writer, &header)?;

    // Unbounded so a slow recorder consumer can never backpressure (and thereby stall
    // the keepalive of) the WS readers. FIFO + no drops preserves the recorded tape.
    let (tx, mut rx) = mpsc::unbounded_channel::<(crate::types::MarketId, crate::events::EventKind)>();
    let mut handles = Vec::new();
    for s in &specs {
        handles.push(tokio::spawn(lighter::run(s.lighter_market_id, s.hl_coin.clone(), s.market_id.clone(), tx.clone())));
        handles.push(tokio::spawn(aster::run(s.aster_symbol.to_lowercase(), s.market_id.clone(), tx.clone())));
    }
    drop(tx); // only connector clones remain

    info!("recording {} market(s) for {}s -> {}", markets.len(), duration_secs, out_path.display());
    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let mut seq: u64 = 0;
    let mut last_ts = header.started_at;
    let mut count: u64 = 0;
    // Backlog watch: the recorder channel is unbounded so a slow consumer never
    // backpressures the WS readers (and never stalls their keepalives) — but that also
    // means a stalled writer/disk grows memory silently. Sample the queue depth and surface
    // a sustained backlog instead of failing invisibly.
    const BACKLOG_WARN: usize = 50_000;
    let mut backlog_hwm: usize = 0;
    let mut backlog_warned = false;

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            _ = tokio::signal::ctrl_c() => { info!("ctrl-c: stopping recording"); break; }
            msg = rx.recv() => {
                match msg {
                    Some((market, kind)) => {
                        // Stamp a monotonic local receive time at dequeue.
                        let now = Utc::now().max(last_ts);
                        last_ts = now;
                        let ev = Event { seq, local_recv_ts: now, market, kind };
                        seq += 1;
                        write_event(&mut writer, &ev)?;
                        count += 1;
                        let backlog = rx.len();
                        if backlog > backlog_hwm {
                            backlog_hwm = backlog;
                        }
                        if backlog >= BACKLOG_WARN && !backlog_warned {
                            backlog_warned = true;
                            warn!("recorder backlog high: {backlog} events queued (hwm {backlog_hwm}) — writer/disk stalling?");
                        } else if backlog_warned && backlog < BACKLOG_WARN / 2 {
                            backlog_warned = false; // drained; re-arm
                        }
                        if count.is_multiple_of(2_000) {
                            use std::io::Write;
                            writer.flush().ok();
                            info!("  ... {count} events (backlog {backlog}, hwm {backlog_hwm})");
                        }
                    }
                    None => break,
                }
            }
        }
    }

    for h in handles {
        h.abort();
    }
    writer.finish()?; // closes the zstd frame (no-op flush for a plain log)
    info!("recorded {count} events -> {}", out_path.display());

    Ok(RecordSummary { events: count, out: out_path, run_id })
}
