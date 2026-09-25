//! `screener.toml` (the values and their sources are documented there).

use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::universe::Pair;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub collect: Collect,
    pub report: Report,
}

/// Which pairs are followed; written at the head of every data file. What is recorded of them
/// follows from `[report]` (`collect::Recording`).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Collect {
    pub min_volume_usd: f64,
    pub max_pairs: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub clip_usd: f64,
    pub depth_multiple: f64,
    pub lighter_tier: String,
    pub aster_taker_ms: f64,
    pub lighter_rtt_ms: f64,
    pub aster_fill_notice_ms: f64,
    pub lighter_fill_notice_ms: f64,
    pub aster: AsterFees,
    pub lighter: LighterTiers,
    pub taker: Taker,
    pub xemm: Xemm,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AsterFees {
    pub maker_bps: f64,
    pub taker_bps: f64,
    pub group_b_taker_bps: f64,
    pub rwa_taker_bps: f64,
    pub rwa_subtypes: Vec<String>,
    pub group_b: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LighterTiers {
    pub standard: LighterTier,
    pub premium: LighterTier,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LighterTier {
    pub maker_bps: f64,
    pub taker_bps: f64,
    pub taker_delay_ms: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Taker {
    pub margin_bps: f64,
    pub gate_percentile: f64,
    pub gate_window_hours: f64,
    pub gate_min_samples: usize,
    pub gate_extra_bps: f64,
    pub sample_interval_ms: i64,
    pub cooldown_ms: i64,
    pub max_position_usd: f64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Xemm {
    pub required_bps: f64,
    pub min_touch_distance_bps: f64,
    pub max_quote_distance_bps: f64,
    pub quote_age_ms: i64,
    pub cooldown_ms: i64,
    pub sweep_bps: Vec<f64>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Config> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
    }
}

impl Report {
    pub fn lighter(&self) -> Result<&LighterTier> {
        match self.lighter_tier.as_str() {
            "standard" => Ok(&self.lighter.standard),
            "premium" => Ok(&self.lighter.premium),
            other => anyhow::bail!("lighter_tier must be standard or premium, not {other}"),
        }
    }

    /// What a side needs at the top for the bot to trade it.
    pub fn depth_usd(&self) -> f64 {
        self.depth_multiple * self.clip_usd
    }

    /// The lowest XEMM required edge scored: the bot's or its sweep's.
    pub fn xemm_min_bps(&self) -> f64 {
        self.xemm.sweep_bps.iter().copied().fold(self.xemm.required_bps, f64::min)
    }

    /// The bot's taker threshold on `pair` at `lighter`'s fees: both taker fees plus the margin.
    pub fn taker_required_bps(&self, pair: &Pair, lighter: &LighterTier) -> f64 {
        self.aster_taker_bps(&pair.aster, &pair.aster_subtypes) + lighter.taker_bps + self.taker.margin_bps
    }

    /// Aster's taker fee for `symbol`, whose exchangeInfo `underlyingSubType` is `subtypes`.
    pub fn aster_taker_bps(&self, symbol: &str, subtypes: &[String]) -> f64 {
        let fees = &self.aster;
        if fees.group_b.iter().any(|s| s == symbol) {
            fees.group_b_taker_bps
        } else if subtypes.iter().any(|s| fees.rwa_subtypes.contains(s)) {
            fees.rwa_taker_bps
        } else {
            fees.taker_bps
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shipped_config_loads_and_group_b_beats_rwa() {
        let cfg = Config::load(Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/screener.toml"))).unwrap();
        let r = &cfg.report;
        let stock = vec!["STOCK".to_string(), "Semiconductor".to_string()];
        assert_eq!(r.aster_taker_bps("SKHYNIXUSDT", &stock), 10.0);
        assert_eq!(r.aster_taker_bps("NVDAUSDT", &stock), 1.25);
        assert_eq!(r.aster_taker_bps("HYPEUSDT", &[]), 4.0);
        assert_eq!(r.lighter().unwrap().taker_delay_ms, 300.0);
    }
}
