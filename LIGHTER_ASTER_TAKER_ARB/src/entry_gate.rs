use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::config::{EntryGateCfg, EntryGateMode};
use crate::types::MarketId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunitySample {
    pub timestamp: DateTime<Utc>,
    pub market: String,
    pub direction: String,
    pub gross_edge_bps: Decimal,
    pub expected_net_margin_bps: Decimal,
    pub expected_net_usd: Decimal,
    pub qty: Decimal,
    pub sell_px: Decimal,
    pub buy_px: Decimal,
    pub ref_px: Decimal,
    pub top_depth_qty: Decimal,
    #[serde(default)]
    pub depth_guard_enabled: bool,
    #[serde(default)]
    pub liquidity_multiple: Decimal,
    #[serde(default)]
    pub depth_supported_qty: Decimal,
    #[serde(default)]
    pub sell_depth_target_qty: Decimal,
    #[serde(default)]
    pub buy_depth_target_qty: Decimal,
    #[serde(default)]
    pub sell_depth_available_qty: Decimal,
    #[serde(default)]
    pub buy_depth_available_qty: Decimal,
    #[serde(default)]
    pub sell_depth_worst_px: Decimal,
    #[serde(default)]
    pub buy_depth_worst_px: Decimal,
    #[serde(default)]
    pub sell_depth_levels_used: usize,
    #[serde(default)]
    pub buy_depth_levels_used: usize,
    #[serde(default)]
    pub sell_best_px: Decimal,
    #[serde(default)]
    pub buy_best_px: Decimal,
    #[serde(default)]
    pub sell_best_qty: Decimal,
    #[serde(default)]
    pub buy_best_qty: Decimal,
    pub aster_book_age_ms: i64,
    pub lighter_book_age_ms: i64,
    pub decision: String,
    pub gate_threshold_bps: Option<Decimal>,
    pub history_sample_count: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct OpportunityGateInput<'a> {
    pub timestamp: DateTime<Utc>,
    pub direction: &'a str,
    pub gross_edge_bps: Decimal,
    pub expected_net_margin_bps: Decimal,
    pub expected_net_usd: Decimal,
    pub qty: Decimal,
    pub sell_px: Decimal,
    pub buy_px: Decimal,
    pub ref_px: Decimal,
    pub top_depth_qty: Decimal,
    pub depth_guard_enabled: bool,
    pub liquidity_multiple: Decimal,
    pub depth_supported_qty: Decimal,
    pub sell_depth_target_qty: Decimal,
    pub buy_depth_target_qty: Decimal,
    pub sell_depth_available_qty: Decimal,
    pub buy_depth_available_qty: Decimal,
    pub sell_depth_worst_px: Decimal,
    pub buy_depth_worst_px: Decimal,
    pub sell_depth_levels_used: usize,
    pub buy_depth_levels_used: usize,
    pub sell_best_px: Decimal,
    pub buy_best_px: Decimal,
    pub sell_best_qty: Decimal,
    pub buy_best_qty: Decimal,
    pub aster_book_age_ms: i64,
    pub lighter_book_age_ms: i64,
    pub force_record: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct GateEvaluation {
    pub allow_execution: bool,
    pub would_allow: bool,
    pub threshold_bps: Option<Decimal>,
    pub sample_count: usize,
    pub decision: &'static str,
    pub recorded: bool,
}

pub struct OpportunityGate {
    cfg: EntryGateCfg,
    market: MarketId,
    path: PathBuf,
    samples: VecDeque<OpportunitySample>,
    last_sample_at: Option<DateTime<Utc>>,
}

impl OpportunityGate {
    pub fn new(
        cfg: &EntryGateCfg,
        market: &MarketId,
        persist_dir: &str,
        now: DateTime<Utc>,
    ) -> Result<Self> {
        let dir = PathBuf::from(persist_dir);
        let path = dir.join(format!("opportunities_{}.jsonl", market_component(market)));
        let samples = if cfg.enabled {
            fs::create_dir_all(&dir)
                .with_context(|| format!("create opportunity history dir {}", dir.display()))?;
            load_recent_samples(&path, market, cutoff(now, cfg.history_window_hours))?
        } else {
            VecDeque::new()
        };
        Ok(Self {
            cfg: cfg.clone(),
            market: market.clone(),
            path,
            samples,
            last_sample_at: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn loaded_samples(&self) -> usize {
        self.samples.len()
    }

    pub fn evaluate(
        &mut self,
        input: OpportunityGateInput<'_>,
        required_gross_edge_bps: Decimal,
    ) -> GateEvaluation {
        self.prune(input.timestamp);

        if !self.cfg.active() {
            return GateEvaluation {
                allow_execution: true,
                would_allow: true,
                threshold_bps: None,
                sample_count: self.samples.len(),
                decision: "off",
                recorded: false,
            };
        }

        let sample_count = self.samples.len();
        let (threshold_bps, would_allow, decision, allow_execution) =
            if sample_count < self.cfg.min_history_samples {
                (None, false, "warmup_block", false)
            } else {
                let threshold = self.dynamic_threshold(required_gross_edge_bps);
                let would_allow = input.gross_edge_bps >= threshold;
                match self.cfg.mode {
                    EntryGateMode::Off => (None, true, "off", true),
                    EntryGateMode::Shadow => {
                        let decision = if would_allow {
                            "would_execute"
                        } else {
                            "shadow_block"
                        };
                        (Some(threshold), would_allow, decision, true)
                    }
                    EntryGateMode::Enforce => {
                        let decision = if would_allow {
                            "would_execute"
                        } else {
                            "gated_out"
                        };
                        (Some(threshold), would_allow, decision, would_allow)
                    }
                }
            };

        let force_record = allow_execution && input.force_record;
        let recorded =
            self.record_if_due(input, decision, threshold_bps, sample_count, force_record);
        GateEvaluation {
            allow_execution,
            would_allow,
            threshold_bps,
            sample_count,
            decision,
            recorded,
        }
    }

    fn dynamic_threshold(&self, required_gross_edge_bps: Decimal) -> Decimal {
        let percentile = percentile(
            self.samples.iter().map(|s| s.gross_edge_bps),
            self.cfg.entry_percentile,
        )
        .unwrap_or(required_gross_edge_bps);
        (required_gross_edge_bps + self.cfg.min_extra_bps).max(percentile)
    }

    fn prune(&mut self, now: DateTime<Utc>) {
        let cutoff = cutoff(now, self.cfg.history_window_hours);
        while self
            .samples
            .front()
            .is_some_and(|sample| sample.timestamp < cutoff)
        {
            self.samples.pop_front();
        }
    }

    fn record_if_due(
        &mut self,
        input: OpportunityGateInput<'_>,
        decision: &str,
        threshold_bps: Option<Decimal>,
        sample_count: usize,
        force: bool,
    ) -> bool {
        if !force && !self.sample_due(input.timestamp) {
            return false;
        }
        let sample = OpportunitySample {
            timestamp: input.timestamp,
            market: self.market.0.clone(),
            direction: input.direction.to_string(),
            gross_edge_bps: input.gross_edge_bps,
            expected_net_margin_bps: input.expected_net_margin_bps,
            expected_net_usd: input.expected_net_usd,
            qty: input.qty,
            sell_px: input.sell_px,
            buy_px: input.buy_px,
            ref_px: input.ref_px,
            top_depth_qty: input.top_depth_qty,
            depth_guard_enabled: input.depth_guard_enabled,
            liquidity_multiple: input.liquidity_multiple,
            depth_supported_qty: input.depth_supported_qty,
            sell_depth_target_qty: input.sell_depth_target_qty,
            buy_depth_target_qty: input.buy_depth_target_qty,
            sell_depth_available_qty: input.sell_depth_available_qty,
            buy_depth_available_qty: input.buy_depth_available_qty,
            sell_depth_worst_px: input.sell_depth_worst_px,
            buy_depth_worst_px: input.buy_depth_worst_px,
            sell_depth_levels_used: input.sell_depth_levels_used,
            buy_depth_levels_used: input.buy_depth_levels_used,
            sell_best_px: input.sell_best_px,
            buy_best_px: input.buy_best_px,
            sell_best_qty: input.sell_best_qty,
            buy_best_qty: input.buy_best_qty,
            aster_book_age_ms: input.aster_book_age_ms,
            lighter_book_age_ms: input.lighter_book_age_ms,
            decision: decision.to_string(),
            gate_threshold_bps: threshold_bps,
            history_sample_count: sample_count,
        };
        self.samples.push_back(sample.clone());
        self.last_sample_at = Some(input.timestamp);
        append_sample(&self.path, &sample);
        true
    }

    fn sample_due(&self, now: DateTime<Utc>) -> bool {
        let Some(last) = self.last_sample_at else {
            return true;
        };
        now.signed_duration_since(last)
            >= Duration::milliseconds(self.cfg.sample_interval_ms as i64)
    }
}

fn load_recent_samples(
    path: &Path,
    market: &MarketId,
    cutoff: DateTime<Utc>,
) -> Result<VecDeque<OpportunitySample>> {
    if !path.exists() {
        return Ok(VecDeque::new());
    }
    let file =
        File::open(path).with_context(|| format!("open opportunity history {}", path.display()))?;
    let mut samples = VecDeque::new();
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("read opportunity history line {}", idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: OpportunitySample = match serde_json::from_str(&line) {
            Ok(row) => row,
            Err(e) => {
                warn!(
                    "skipping malformed opportunity history row {} line {}: {e:#}",
                    path.display(),
                    idx + 1
                );
                continue;
            }
        };
        if row.market == market.0 && row.timestamp >= cutoff {
            samples.push_back(row);
        }
    }
    Ok(samples)
}

fn append_sample(path: &Path, sample: &OpportunitySample) {
    if let Err(e) = append_sample_inner(path, sample) {
        warn!(
            "failed to append opportunity history {}: {e:#}",
            path.display()
        );
    }
}

fn append_sample_inner(path: &Path, sample: &OpportunitySample) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create opportunity history dir {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open opportunity history for append {}", path.display()))?;
    serde_json::to_writer(&mut file, sample).context("serialize opportunity sample")?;
    file.write_all(b"\n")
        .context("write opportunity sample newline")?;
    file.flush().context("flush opportunity sample")?;
    Ok(())
}

fn percentile<I>(values: I, percentile: Decimal) -> Option<Decimal>
where
    I: IntoIterator<Item = Decimal>,
{
    let mut values: Vec<Decimal> = values.into_iter().collect();
    if values.is_empty() {
        return None;
    }
    values.sort();
    let p = percentile.to_f64().unwrap_or(90.0).clamp(0.0, 100.0);
    let rank = ((p / 100.0) * values.len() as f64).ceil() as usize;
    let idx = rank.saturating_sub(1).min(values.len() - 1);
    Some(values[idx])
}

fn cutoff(now: DateTime<Utc>, window_hours: u64) -> DateTime<Utc> {
    now - Duration::hours(window_hours as i64)
}

fn market_component(market: &MarketId) -> String {
    market
        .0
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn tmp_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "lighter_aster_taker_arb_gate_{name}_{}_{}",
            std::process::id(),
            nanos
        ))
    }

    fn cfg(mode: EntryGateMode) -> EntryGateCfg {
        EntryGateCfg {
            enabled: true,
            mode,
            history_window_hours: 72,
            sample_interval_ms: 1000,
            min_history_samples: 3,
            entry_percentile: dec!(90),
            min_extra_bps: dec!(0.5),
        }
    }

    fn input(ts: DateTime<Utc>, gross_edge_bps: Decimal) -> OpportunityGateInput<'static> {
        OpportunityGateInput {
            timestamp: ts,
            direction: "SELL_ASTER_BUY_LIGHTER",
            gross_edge_bps,
            expected_net_margin_bps: gross_edge_bps - dec!(6),
            expected_net_usd: dec!(0.01),
            qty: dec!(0.20),
            sell_px: dec!(62.20),
            buy_px: dec!(62.14),
            ref_px: dec!(62.17),
            top_depth_qty: dec!(1.0),
            depth_guard_enabled: true,
            liquidity_multiple: dec!(10),
            depth_supported_qty: dec!(0.20),
            sell_depth_target_qty: dec!(2.00),
            buy_depth_target_qty: dec!(2.00),
            sell_depth_available_qty: dec!(2.00),
            buy_depth_available_qty: dec!(2.00),
            sell_depth_worst_px: dec!(62.20),
            buy_depth_worst_px: dec!(62.14),
            sell_depth_levels_used: 1,
            buy_depth_levels_used: 1,
            sell_best_px: dec!(62.20),
            buy_best_px: dec!(62.14),
            sell_best_qty: dec!(2.00),
            buy_best_qty: dec!(2.00),
            aster_book_age_ms: 10,
            lighter_book_age_ms: 20,
            force_record: true,
        }
    }

    fn sampled_input(ts: DateTime<Utc>, gross_edge_bps: Decimal) -> OpportunityGateInput<'static> {
        OpportunityGateInput {
            force_record: false,
            ..input(ts, gross_edge_bps)
        }
    }

    fn row(ts: DateTime<Utc>, market: &str, edge: Decimal) -> OpportunitySample {
        OpportunitySample {
            timestamp: ts,
            market: market.to_string(),
            direction: "SELL_ASTER_BUY_LIGHTER".to_string(),
            gross_edge_bps: edge,
            expected_net_margin_bps: edge - dec!(6),
            expected_net_usd: dec!(0.01),
            qty: dec!(0.20),
            sell_px: dec!(62.20),
            buy_px: dec!(62.14),
            ref_px: dec!(62.17),
            top_depth_qty: dec!(1.0),
            depth_guard_enabled: true,
            liquidity_multiple: dec!(10),
            depth_supported_qty: dec!(0.20),
            sell_depth_target_qty: dec!(2.00),
            buy_depth_target_qty: dec!(2.00),
            sell_depth_available_qty: dec!(2.00),
            buy_depth_available_qty: dec!(2.00),
            sell_depth_worst_px: dec!(62.20),
            buy_depth_worst_px: dec!(62.14),
            sell_depth_levels_used: 1,
            buy_depth_levels_used: 1,
            sell_best_px: dec!(62.20),
            buy_best_px: dec!(62.14),
            sell_best_qty: dec!(2.00),
            buy_best_qty: dec!(2.00),
            aster_book_age_ms: 10,
            lighter_book_age_ms: 20,
            decision: "would_execute".to_string(),
            gate_threshold_bps: None,
            history_sample_count: 0,
        }
    }

    fn append_test_row(path: &Path, sample: &OpportunitySample) {
        append_sample_inner(path, sample).unwrap();
    }

    #[test]
    fn percentile_uses_nearest_rank() {
        let values = vec![dec!(6), dec!(8), dec!(10), dec!(12)];
        assert_eq!(percentile(values, dec!(90)).unwrap(), dec!(12));
    }

    #[test]
    fn insufficient_history_blocks_execution_and_records() {
        let dir = tmp_dir("warmup");
        let now = DateTime::parse_from_rfc3339("2026-06-24T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut gate = OpportunityGate::new(
            &cfg(EntryGateMode::Enforce),
            &MarketId::from("HYPE"),
            dir.to_str().unwrap(),
            now,
        )
        .unwrap();
        let decision = gate.evaluate(input(now, dec!(6)), dec!(6));
        assert!(!decision.allow_execution);
        assert!(!decision.would_allow);
        assert_eq!(decision.decision, "warmup_block");
        assert_eq!(gate.loaded_samples(), 1);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn insufficient_history_blocks_shadow_mode_too() {
        let dir = tmp_dir("warmup_shadow");
        let now = DateTime::parse_from_rfc3339("2026-06-24T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut gate = OpportunityGate::new(
            &cfg(EntryGateMode::Shadow),
            &MarketId::from("HYPE"),
            dir.to_str().unwrap(),
            now,
        )
        .unwrap();
        let decision = gate.evaluate(input(now, dec!(6)), dec!(6));
        assert!(!decision.allow_execution);
        assert!(!decision.would_allow);
        assert_eq!(decision.decision, "warmup_block");
        assert_eq!(gate.loaded_samples(), 1);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn enforce_blocks_below_recent_percentile() {
        let dir = tmp_dir("enforce_block");
        let now = DateTime::parse_from_rfc3339("2026-06-24T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let market = MarketId::from("HYPE");
        let path = dir.join("opportunities_HYPE.jsonl");
        append_test_row(&path, &row(now - Duration::minutes(10), "HYPE", dec!(6)));
        append_test_row(&path, &row(now - Duration::minutes(9), "HYPE", dec!(11)));
        append_test_row(&path, &row(now - Duration::minutes(8), "HYPE", dec!(12)));
        let mut gate = OpportunityGate::new(
            &cfg(EntryGateMode::Enforce),
            &market,
            dir.to_str().unwrap(),
            now,
        )
        .unwrap();
        let decision = gate.evaluate(input(now, dec!(6)), dec!(6));
        assert!(!decision.allow_execution);
        assert_eq!(decision.decision, "gated_out");
        assert_eq!(decision.threshold_bps, Some(dec!(12)));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn shadow_never_blocks_execution() {
        let dir = tmp_dir("shadow");
        let now = DateTime::parse_from_rfc3339("2026-06-24T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let market = MarketId::from("HYPE");
        let path = dir.join("opportunities_HYPE.jsonl");
        append_test_row(&path, &row(now - Duration::minutes(10), "HYPE", dec!(6)));
        append_test_row(&path, &row(now - Duration::minutes(9), "HYPE", dec!(11)));
        append_test_row(&path, &row(now - Duration::minutes(8), "HYPE", dec!(12)));
        let mut gate = OpportunityGate::new(
            &cfg(EntryGateMode::Shadow),
            &market,
            dir.to_str().unwrap(),
            now,
        )
        .unwrap();
        let decision = gate.evaluate(input(now, dec!(6)), dec!(6));
        assert!(decision.allow_execution);
        assert!(!decision.would_allow);
        assert_eq!(decision.decision, "shadow_block");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn off_mode_preserves_current_behavior_without_recording() {
        let dir = tmp_dir("off");
        let now = DateTime::parse_from_rfc3339("2026-06-24T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut config = cfg(EntryGateMode::Off);
        config.enabled = true;
        let mut gate =
            OpportunityGate::new(&config, &MarketId::from("HYPE"), dir.to_str().unwrap(), now)
                .unwrap();
        let decision = gate.evaluate(input(now, dec!(6)), dec!(6));
        assert!(decision.allow_execution);
        assert_eq!(decision.decision, "off");
        assert_eq!(gate.loaded_samples(), 0);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn loads_recent_rows_and_skips_old_or_malformed_rows() {
        let dir = tmp_dir("load");
        let now = DateTime::parse_from_rfc3339("2026-06-24T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let path = dir.join("opportunities_HYPE.jsonl");
        append_test_row(&path, &row(now - Duration::hours(80), "HYPE", dec!(8)));
        append_test_row(&path, &row(now - Duration::hours(1), "OTHER", dec!(9)));
        append_test_row(&path, &row(now - Duration::hours(1), "HYPE", dec!(10)));
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"not-json\n").unwrap();
        }
        let gate = OpportunityGate::new(
            &cfg(EntryGateMode::Shadow),
            &MarketId::from("HYPE"),
            dir.to_str().unwrap(),
            now,
        )
        .unwrap();
        assert_eq!(gate.loaded_samples(), 1);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn throttles_blocked_samples() {
        let dir = tmp_dir("throttle");
        let now = DateTime::parse_from_rfc3339("2026-06-24T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let market = MarketId::from("HYPE");
        let path = dir.join("opportunities_HYPE.jsonl");
        append_test_row(&path, &row(now - Duration::minutes(10), "HYPE", dec!(6)));
        append_test_row(&path, &row(now - Duration::minutes(9), "HYPE", dec!(11)));
        append_test_row(&path, &row(now - Duration::minutes(8), "HYPE", dec!(12)));
        let mut gate = OpportunityGate::new(
            &cfg(EntryGateMode::Enforce),
            &market,
            dir.to_str().unwrap(),
            now,
        )
        .unwrap();
        let first = gate.evaluate(input(now, dec!(6)), dec!(6));
        let second = gate.evaluate(input(now + Duration::milliseconds(250), dec!(6)), dec!(6));
        assert!(first.recorded);
        assert!(!second.recorded);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn throttles_allowed_samples_when_not_forced() {
        let dir = tmp_dir("allowed_throttle");
        let now = DateTime::parse_from_rfc3339("2026-06-24T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let market = MarketId::from("HYPE");
        let path = dir.join("opportunities_HYPE.jsonl");
        append_test_row(&path, &row(now - Duration::minutes(10), "HYPE", dec!(6)));
        append_test_row(&path, &row(now - Duration::minutes(9), "HYPE", dec!(11)));
        append_test_row(&path, &row(now - Duration::minutes(8), "HYPE", dec!(12)));
        let mut gate = OpportunityGate::new(
            &cfg(EntryGateMode::Enforce),
            &market,
            dir.to_str().unwrap(),
            now,
        )
        .unwrap();
        let first = gate.evaluate(sampled_input(now, dec!(13)), dec!(6));
        let second = gate.evaluate(
            sampled_input(now + Duration::milliseconds(250), dec!(13)),
            dec!(6),
        );
        assert!(first.allow_execution);
        assert!(first.recorded);
        assert!(second.allow_execution);
        assert!(!second.recorded);
        let _ = fs::remove_dir_all(dir);
    }
}
