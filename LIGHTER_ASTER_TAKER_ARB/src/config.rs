use std::collections::HashSet;
use std::path::Path;

use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::book::MAX_BOOK_LEVELS;
use crate::types::MarketId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub arb: ArbCfg,
    #[serde(default)]
    pub pnl: PnlCfg,
    #[serde(default)]
    pub live: LiveCfg,
    #[serde(default)]
    pub venues: VenueCfg,
    #[serde(default)]
    pub risk: RiskCfg,
    #[serde(default)]
    pub markets: Vec<MarketCfg>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("read config {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&text).with_context(|| format!("parse config {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.markets.is_empty() {
            bail!("config has no [[markets]]");
        }
        if self.arb.desired_notional <= Decimal::ZERO {
            bail!("arb.desired_notional must be positive");
        }
        if self.arb.margin_bps < Decimal::ZERO {
            bail!("arb.margin_bps must be non-negative");
        }
        if self.arb.aster_taker_fee_bps < Decimal::ZERO
            || self.arb.lighter_taker_fee_bps < Decimal::ZERO
        {
            bail!("taker fees must be non-negative");
        }
        if self.arb.max_aster_slippage_bps < Decimal::ZERO
            || self.arb.max_lighter_slippage_bps < Decimal::ZERO
            || self.arb.emergency_slippage_bps < Decimal::ZERO
            || self.arb.hedge_retry_slippage_bps < Decimal::ZERO
        {
            bail!("slippage bps must be non-negative");
        }
        if self.arb.hedge_retry_timeout_ms == 0 {
            bail!("arb.hedge_retry_timeout_ms must be > 0");
        }
        if self.arb.max_hedge_retry_attempts == 0 {
            bail!("arb.max_hedge_retry_attempts must be > 0");
        }
        self.arb.depth_guard.validate()?;
        self.arb.book_sanity.validate()?;
        if self.arb.poll_interval_ms == 0 {
            bail!("arb.poll_interval_ms must be > 0");
        }
        if self.arb.max_book_staleness_ms <= 0 {
            bail!("arb.max_book_staleness_ms must be > 0");
        }
        if self.arb.max_recovered_failures_per_hour == 0 {
            bail!("arb.max_recovered_failures_per_hour must be > 0");
        }
        if self.arb.max_recovered_loss_usdc_per_hour <= Decimal::ZERO {
            bail!("arb.max_recovered_loss_usdc_per_hour must be positive");
        }
        self.arb.entry_gate.validate()?;
        if self.pnl.enabled && self.pnl.max_loss_usdc <= Decimal::ZERO {
            bail!("pnl.max_loss_usdc must be positive when pnl is enabled");
        }
        if self.live.max_account_snapshot_age_ms <= 0 {
            bail!("live.max_account_snapshot_age_ms must be > 0");
        }
        if self.risk.max_abs_position_notional_usd <= Decimal::ZERO {
            bail!("risk.max_abs_position_notional_usd must be positive");
        }
        if self.risk.max_position_mismatch_usd < Decimal::ZERO
            || self.risk.margin_buffer_usd < Decimal::ZERO
        {
            bail!("risk mismatch and margin buffer values must be non-negative");
        }
        if self.risk.min_reconcile_interval_ms == 0 {
            bail!("risk.min_reconcile_interval_ms must be > 0");
        }
        let mut ids = HashSet::new();
        for m in &self.markets {
            if !ids.insert(m.id().0) {
                bail!("duplicate market_id in [[markets]]");
            }
        }
        Ok(())
    }

    pub fn select_markets(&self, filter: Option<&str>) -> Vec<MarketCfg> {
        let Some(filter) = filter else {
            return self.markets.clone();
        };
        let wanted: HashSet<String> = filter
            .split(',')
            .map(|s| s.trim().to_ascii_uppercase())
            .filter(|s| !s.is_empty())
            .collect();
        self.markets
            .iter()
            .filter(|m| wanted.contains(&m.id().0.to_ascii_uppercase()))
            .cloned()
            .collect()
    }
}

fn default_max_aster_slippage_bps() -> Decimal {
    Decimal::from(3)
}

fn default_max_recovered_failures_per_hour() -> u64 {
    3
}

fn default_max_recovered_loss_usdc_per_hour() -> Decimal {
    Decimal::new(25, 2)
}

fn default_hedge_retry_slippage_bps() -> Decimal {
    Decimal::from(30)
}

fn default_hedge_retry_timeout_ms() -> u64 {
    5_000
}

fn default_max_hedge_retry_attempts() -> u64 {
    1
}

fn default_depth_guard_enabled() -> bool {
    true
}

fn default_depth_guard_liquidity_multiple() -> Decimal {
    Decimal::from(10)
}

fn default_depth_guard_max_levels() -> usize {
    MAX_BOOK_LEVELS
}

fn default_book_sanity_enabled() -> bool {
    false
}

fn default_book_sanity_interval_ms() -> u64 {
    10_000
}

fn default_book_sanity_max_top_bps() -> Decimal {
    Decimal::from(8)
}

fn default_book_sanity_max_vwap_bps() -> Decimal {
    Decimal::from(8)
}

fn default_book_sanity_required_failures() -> u64 {
    2
}

fn default_book_sanity_required_successes() -> u64 {
    2
}

fn default_book_sanity_block_cooldown_ms() -> u64 {
    15_000
}

fn default_book_sanity_rest_depth_levels() -> usize {
    MAX_BOOK_LEVELS
}

fn default_book_sanity_liquidity_multiple() -> Decimal {
    Decimal::from(10)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbCfg {
    pub desired_notional: Decimal,
    pub margin_bps: Decimal,
    pub aster_taker_fee_bps: Decimal,
    pub lighter_taker_fee_bps: Decimal,
    #[serde(default = "default_max_aster_slippage_bps")]
    pub max_aster_slippage_bps: Decimal,
    pub max_lighter_slippage_bps: Decimal,
    pub emergency_slippage_bps: Decimal,
    #[serde(default = "default_hedge_retry_slippage_bps")]
    pub hedge_retry_slippage_bps: Decimal,
    #[serde(default = "default_hedge_retry_timeout_ms")]
    pub hedge_retry_timeout_ms: u64,
    #[serde(default = "default_max_hedge_retry_attempts")]
    pub max_hedge_retry_attempts: u64,
    #[serde(default)]
    pub depth_guard: DepthGuardCfg,
    #[serde(default)]
    pub book_sanity: BookSanityCfg,
    pub cooldown_ms: u64,
    pub startup_warmup_ms: u64,
    pub poll_interval_ms: u64,
    pub max_book_staleness_ms: i64,
    #[serde(default = "default_max_recovered_failures_per_hour")]
    pub max_recovered_failures_per_hour: u64,
    #[serde(default = "default_max_recovered_loss_usdc_per_hour")]
    pub max_recovered_loss_usdc_per_hour: Decimal,
    #[serde(default)]
    pub entry_gate: EntryGateCfg,
}

impl Default for ArbCfg {
    fn default() -> Self {
        ArbCfg {
            desired_notional: Decimal::from(13),
            margin_bps: Decimal::from(2),
            aster_taker_fee_bps: Decimal::from(4),
            lighter_taker_fee_bps: Decimal::ZERO,
            max_aster_slippage_bps: Decimal::from(3),
            max_lighter_slippage_bps: Decimal::from(3),
            emergency_slippage_bps: Decimal::from(25),
            hedge_retry_slippage_bps: default_hedge_retry_slippage_bps(),
            hedge_retry_timeout_ms: default_hedge_retry_timeout_ms(),
            max_hedge_retry_attempts: default_max_hedge_retry_attempts(),
            depth_guard: DepthGuardCfg::default(),
            book_sanity: BookSanityCfg::default(),
            cooldown_ms: 60_000,
            startup_warmup_ms: 15_000,
            poll_interval_ms: 250,
            max_book_staleness_ms: 2000,
            max_recovered_failures_per_hour: default_max_recovered_failures_per_hour(),
            max_recovered_loss_usdc_per_hour: default_max_recovered_loss_usdc_per_hour(),
            entry_gate: EntryGateCfg::default(),
        }
    }
}

impl ArbCfg {
    pub fn required_gross_edge_bps(&self) -> Decimal {
        self.aster_taker_fee_bps + self.lighter_taker_fee_bps + self.margin_bps
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BookSanityCfg {
    pub enabled: bool,
    pub interval_ms: u64,
    pub max_top_bps: Decimal,
    pub max_vwap_bps: Decimal,
    pub required_failures: u64,
    pub required_successes: u64,
    pub block_cooldown_ms: u64,
    pub rest_depth_levels: usize,
    pub liquidity_multiple: Decimal,
}

impl Default for BookSanityCfg {
    fn default() -> Self {
        Self {
            enabled: default_book_sanity_enabled(),
            interval_ms: default_book_sanity_interval_ms(),
            max_top_bps: default_book_sanity_max_top_bps(),
            max_vwap_bps: default_book_sanity_max_vwap_bps(),
            required_failures: default_book_sanity_required_failures(),
            required_successes: default_book_sanity_required_successes(),
            block_cooldown_ms: default_book_sanity_block_cooldown_ms(),
            rest_depth_levels: default_book_sanity_rest_depth_levels(),
            liquidity_multiple: default_book_sanity_liquidity_multiple(),
        }
    }
}

impl BookSanityCfg {
    fn validate(&self) -> Result<()> {
        if self.interval_ms == 0 {
            bail!("arb.book_sanity.interval_ms must be > 0");
        }
        if self.max_top_bps < Decimal::ZERO || self.max_vwap_bps < Decimal::ZERO {
            bail!("arb.book_sanity divergence thresholds must be non-negative");
        }
        if self.required_failures == 0 {
            bail!("arb.book_sanity.required_failures must be > 0");
        }
        if self.required_successes == 0 {
            bail!("arb.book_sanity.required_successes must be > 0");
        }
        if self.block_cooldown_ms == 0 {
            bail!("arb.book_sanity.block_cooldown_ms must be > 0");
        }
        if self.rest_depth_levels == 0 || self.rest_depth_levels > MAX_BOOK_LEVELS {
            bail!("arb.book_sanity.rest_depth_levels must be in [1, {MAX_BOOK_LEVELS}]");
        }
        if self.liquidity_multiple <= Decimal::ZERO {
            bail!("arb.book_sanity.liquidity_multiple must be positive");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DepthGuardCfg {
    pub enabled: bool,
    pub liquidity_multiple: Decimal,
    pub max_levels: usize,
}

impl Default for DepthGuardCfg {
    fn default() -> Self {
        Self {
            enabled: default_depth_guard_enabled(),
            liquidity_multiple: default_depth_guard_liquidity_multiple(),
            max_levels: default_depth_guard_max_levels(),
        }
    }
}

impl DepthGuardCfg {
    fn validate(&self) -> Result<()> {
        if self.liquidity_multiple <= Decimal::ZERO {
            bail!("arb.depth_guard.liquidity_multiple must be positive");
        }
        if self.max_levels == 0 || self.max_levels > MAX_BOOK_LEVELS {
            bail!("arb.depth_guard.max_levels must be in [1, {MAX_BOOK_LEVELS}]");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryGateMode {
    Off,
    Shadow,
    Enforce,
}

impl Default for EntryGateMode {
    fn default() -> Self {
        EntryGateMode::Shadow
    }
}

impl EntryGateMode {
    pub fn as_str(self) -> &'static str {
        match self {
            EntryGateMode::Off => "off",
            EntryGateMode::Shadow => "shadow",
            EntryGateMode::Enforce => "enforce",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EntryGateCfg {
    pub enabled: bool,
    pub mode: EntryGateMode,
    pub history_window_hours: u64,
    pub sample_interval_ms: u64,
    pub min_history_samples: usize,
    pub entry_percentile: Decimal,
    pub min_extra_bps: Decimal,
}

impl Default for EntryGateCfg {
    fn default() -> Self {
        EntryGateCfg {
            enabled: true,
            mode: EntryGateMode::Shadow,
            history_window_hours: 72,
            sample_interval_ms: 1000,
            min_history_samples: 500,
            entry_percentile: Decimal::from(90),
            min_extra_bps: Decimal::new(5, 1),
        }
    }
}

impl EntryGateCfg {
    pub fn active(&self) -> bool {
        self.enabled && self.mode != EntryGateMode::Off
    }

    fn validate(&self) -> Result<()> {
        if self.history_window_hours == 0 {
            bail!("arb.entry_gate.history_window_hours must be > 0");
        }
        if self.sample_interval_ms == 0 {
            bail!("arb.entry_gate.sample_interval_ms must be > 0");
        }
        if self.min_history_samples == 0 {
            bail!("arb.entry_gate.min_history_samples must be > 0");
        }
        if self.entry_percentile <= Decimal::ZERO || self.entry_percentile > Decimal::from(100) {
            bail!("arb.entry_gate.entry_percentile must be in (0, 100]");
        }
        if self.min_extra_bps < Decimal::ZERO {
            bail!("arb.entry_gate.min_extra_bps must be non-negative");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnlCfg {
    pub enabled: bool,
    pub persist_dir: String,
    pub since: String,
    pub max_loss_usdc: Decimal,
}

impl Default for PnlCfg {
    fn default() -> Self {
        PnlCfg {
            enabled: true,
            persist_dir: "runs".to_string(),
            since: "2026-06-23T23:00:00Z".to_string(),
            max_loss_usdc: Decimal::from(5),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveCfg {
    pub enabled: bool,
    pub mode: String,
    pub max_account_snapshot_age_ms: i64,
}

impl Default for LiveCfg {
    fn default() -> Self {
        LiveCfg {
            enabled: false,
            mode: "live".to_string(),
            max_account_snapshot_age_ms: 30000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueCfg {
    pub aster_base_url: String,
    pub lighter_base_url: String,
    pub signers_dir: String,
}

impl Default for VenueCfg {
    fn default() -> Self {
        VenueCfg {
            aster_base_url: "https://fapi.asterdex.com".to_string(),
            lighter_base_url: "https://mainnet.zklighter.elliot.ai".to_string(),
            signers_dir: "signers".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskCfg {
    pub max_abs_position_notional_usd: Decimal,
    pub max_position_mismatch_usd: Decimal,
    pub margin_buffer_usd: Decimal,
    pub min_reconcile_interval_ms: u64,
}

impl Default for RiskCfg {
    fn default() -> Self {
        RiskCfg {
            max_abs_position_notional_usd: Decimal::from(200),
            max_position_mismatch_usd: Decimal::from(3),
            margin_buffer_usd: Decimal::from(25),
            min_reconcile_interval_ms: 500,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketCfg {
    pub aster_symbol: String,
    pub lighter_symbol: String,
    pub market_id: Option<String>,
    pub lighter_market_index: Option<u32>,
    pub lighter_price_decimals: Option<u32>,
    pub lighter_size_decimals: Option<u32>,
    pub lighter_min_notional: Option<Decimal>,
}

impl MarketCfg {
    pub fn id(&self) -> MarketId {
        MarketId(
            self.market_id
                .clone()
                .unwrap_or_else(|| self.lighter_symbol.clone()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn valid_config() -> Config {
        Config {
            arb: ArbCfg::default(),
            pnl: PnlCfg::default(),
            live: LiveCfg::default(),
            venues: VenueCfg::default(),
            risk: RiskCfg::default(),
            markets: vec![MarketCfg {
                aster_symbol: "HYPEUSDT".to_string(),
                lighter_symbol: "HYPE".to_string(),
                market_id: Some("HYPE".to_string()),
                lighter_market_index: Some(24),
                lighter_price_decimals: Some(4),
                lighter_size_decimals: Some(2),
                lighter_min_notional: Some(dec!(10)),
            }],
        }
    }

    #[test]
    fn entry_gate_defaults_to_shadow_mode() {
        let cfg = EntryGateCfg::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.mode, EntryGateMode::Shadow);
        assert_eq!(cfg.history_window_hours, 72);
        assert_eq!(cfg.sample_interval_ms, 1000);
        assert_eq!(cfg.min_history_samples, 500);
        assert_eq!(cfg.entry_percentile, dec!(90));
        assert_eq!(cfg.min_extra_bps, dec!(0.5));
    }

    #[test]
    fn entry_gate_rejects_invalid_values() {
        let mut cfg = EntryGateCfg::default();
        cfg.history_window_hours = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = EntryGateCfg::default();
        cfg.sample_interval_ms = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = EntryGateCfg::default();
        cfg.min_history_samples = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = EntryGateCfg::default();
        cfg.entry_percentile = Decimal::ZERO;
        assert!(cfg.validate().is_err());

        let mut cfg = EntryGateCfg::default();
        cfg.entry_percentile = dec!(100.1);
        assert!(cfg.validate().is_err());

        let mut cfg = EntryGateCfg::default();
        cfg.min_extra_bps = dec!(-0.1);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn entry_gate_toml_allows_partial_override() {
        let cfg: EntryGateCfg = toml::from_str("mode = \"enforce\"").unwrap();
        assert_eq!(cfg.mode, EntryGateMode::Enforce);
        assert_eq!(cfg.history_window_hours, 72);
        assert_eq!(cfg.entry_percentile, dec!(90));
        assert_eq!(cfg.min_extra_bps, dec!(0.5));
    }

    #[test]
    fn arb_rejects_invalid_hedge_retry_values() {
        let mut cfg = valid_config();
        cfg.arb.hedge_retry_slippage_bps = dec!(-0.1);
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.hedge_retry_timeout_ms = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.max_hedge_retry_attempts = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn arb_rejects_invalid_depth_guard_values() {
        let mut cfg = valid_config();
        cfg.arb.depth_guard.liquidity_multiple = Decimal::ZERO;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.depth_guard.max_levels = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.depth_guard.max_levels = MAX_BOOK_LEVELS + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn book_sanity_defaults_are_conservative() {
        let cfg = BookSanityCfg::default();
        assert!(!cfg.enabled);
        assert_eq!(cfg.interval_ms, 10_000);
        assert_eq!(cfg.max_top_bps, dec!(8));
        assert_eq!(cfg.max_vwap_bps, dec!(8));
        assert_eq!(cfg.required_failures, 2);
        assert_eq!(cfg.required_successes, 2);
        assert_eq!(cfg.block_cooldown_ms, 15_000);
        assert_eq!(cfg.rest_depth_levels, MAX_BOOK_LEVELS);
        assert_eq!(cfg.liquidity_multiple, dec!(10));
    }

    #[test]
    fn arb_rejects_invalid_book_sanity_values() {
        let mut cfg = valid_config();
        cfg.arb.book_sanity.interval_ms = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.book_sanity.max_top_bps = dec!(-0.1);
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.book_sanity.required_failures = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.book_sanity.required_successes = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.book_sanity.block_cooldown_ms = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.book_sanity.rest_depth_levels = MAX_BOOK_LEVELS + 1;
        assert!(cfg.validate().is_err());

        let mut cfg = valid_config();
        cfg.arb.book_sanity.liquidity_multiple = Decimal::ZERO;
        assert!(cfg.validate().is_err());
    }
}
