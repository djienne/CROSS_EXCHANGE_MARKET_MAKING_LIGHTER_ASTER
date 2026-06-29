//! Replay driver: stream a recorded JSONL event log through the `SimEngine`
//! deterministically and persist results into SQLite. Config can be overridden
//! (e.g. different buffers or queue models) while reusing the recorded data and
//! market specs.

use std::path::Path;

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::config::Config;
use crate::events::EventLogReader;
use crate::sim::SimEngine;
use crate::store::Db;

pub struct ReplayOutcome {
    pub run_id: String,
    pub events: u64,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

/// Replay `events_path` into `db_path`. If `config_override` is `Some`, it
/// replaces the config recorded in the log header (market specs are always taken
/// from the header). Returns the new run id.
pub fn run(
    events_path: impl AsRef<Path>,
    db_path: impl AsRef<Path>,
    config_override: Option<Config>,
) -> Result<ReplayOutcome> {
    let events_path = events_path.as_ref();
    let (header, reader) = EventLogReader::open(events_path)?;
    let cfg = match config_override {
        Some(c) => {
            c.validate()?;
            c
        }
        None => {
            header.config.validate()?;
            header.config.clone()
        }
    };

    let mut db = Db::open(db_path)?;
    let run_id = Uuid::new_v4().to_string();
    let config_json = serde_json::to_string(&cfg)?;
    db.insert_run(
        &run_id,
        header.started_at,
        "replay",
        events_path.to_str(),
        env!("CARGO_PKG_VERSION"),
        &config_json,
    )?;
    for spec in &header.market_specs {
        db.insert_market(spec)?;
    }

    let mut engine = SimEngine::new(cfg, header.market_specs.clone())?;

    let mut count: u64 = 0;
    let mut last_ts = header.started_at;
    let mut last_key: Option<(DateTime<Utc>, u64)> = None;
    for ev in reader {
        let ev = ev?;
        let key = (ev.local_recv_ts, ev.seq);
        if let Some(prev) = last_key {
            if key < prev {
                bail!(
                    "event log not ordered: {:?} < {:?} at event {}",
                    key,
                    prev,
                    count
                );
            }
        }
        last_key = Some(key);
        last_ts = ev.local_recv_ts;
        engine.on_event(&ev, &mut db)?;
        count += 1;
    }

    engine.finalize(last_ts, &mut db)?;

    Ok(ReplayOutcome {
        run_id,
        events: count,
        started_at: header.started_at,
        finished_at: last_ts,
    })
}
