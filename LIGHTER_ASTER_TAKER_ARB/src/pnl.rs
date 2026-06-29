use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::config::PnlCfg;
use crate::types::{FillSummary, MarketId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeLedgerRow {
    pub timestamp: DateTime<Utc>,
    pub market: String,
    pub direction: String,
    pub qty: Decimal,
    pub expected_net_usd: Decimal,
    pub actual_gross_usd: Decimal,
    pub actual_fees_usd: Decimal,
    pub actual_net_usd: Decimal,
    pub actual_net_bps: Decimal,
    pub fill_qty_mismatch: Decimal,
    pub aster_fill: FillSummary,
    pub lighter_fill: FillSummary,
    pub aster_order_id: i64,
    pub lighter_client_order_index: i64,
    pub final_aster_position: Decimal,
    pub final_lighter_position: Decimal,
    pub final_net_position: Decimal,
    pub available_before_usd: Decimal,
    pub available_after_usd: Decimal,
    pub aster_available_before_usd: Decimal,
    pub aster_available_after_usd: Decimal,
    pub lighter_available_before_usd: Decimal,
    pub lighter_available_after_usd: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerState {
    pub active: bool,
    pub triggered_at: DateTime<Utc>,
    pub market: String,
    pub pnl_since: DateTime<Utc>,
    pub cumulative_pnl_usdc: Decimal,
    pub max_loss_usdc: Decimal,
    pub last_trade_timestamp: DateTime<Utc>,
    pub last_trade_actual_net_usd: Decimal,
    pub last_aster_order_id: i64,
    pub last_lighter_client_order_index: i64,
    pub final_aster_position: Decimal,
    pub final_lighter_position: Decimal,
    pub final_net_position: Decimal,
}

#[derive(Debug, Clone)]
pub struct PnlSnapshot {
    pub since: DateTime<Utc>,
    pub loaded_trades: usize,
    pub cumulative_pnl_usdc: Decimal,
    pub max_loss_usdc: Decimal,
    pub ledger_path: PathBuf,
    pub breaker_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct PnlUpdate {
    pub cumulative_pnl_usdc: Decimal,
    pub trade_count: usize,
    pub breaker: Option<CircuitBreakerState>,
}

pub struct PnlTracker {
    market: MarketId,
    since: DateTime<Utc>,
    max_loss_usdc: Decimal,
    ledger_path: PathBuf,
    breaker_path: PathBuf,
    cumulative_pnl_usdc: Decimal,
    loaded_trades: usize,
    last_trade: Option<TradeLedgerRow>,
}

impl PnlTracker {
    pub fn new(cfg: &PnlCfg, market: &MarketId, bot_start: DateTime<Utc>) -> Result<Self> {
        let since = parse_since(&cfg.since, bot_start)?;
        let dir = PathBuf::from(&cfg.persist_dir);
        fs::create_dir_all(&dir)
            .with_context(|| format!("create pnl persist dir {}", dir.display()))?;
        let component = market_component(market);
        let ledger_path = dir.join(format!("trades_{component}.jsonl"));
        let breaker_path = dir.join(format!("circuit_breaker_{component}.json"));
        let (loaded_trades, cumulative_pnl_usdc, last_trade) =
            load_cumulative_pnl(&ledger_path, since)?;
        Ok(Self {
            market: market.clone(),
            since,
            max_loss_usdc: cfg.max_loss_usdc,
            ledger_path,
            breaker_path,
            cumulative_pnl_usdc,
            loaded_trades,
            last_trade,
        })
    }

    pub fn snapshot(&self) -> PnlSnapshot {
        PnlSnapshot {
            since: self.since,
            loaded_trades: self.loaded_trades,
            cumulative_pnl_usdc: self.cumulative_pnl_usdc,
            max_loss_usdc: self.max_loss_usdc,
            ledger_path: self.ledger_path.clone(),
            breaker_path: self.breaker_path.clone(),
        }
    }

    pub fn active_breaker(&self) -> Result<Option<CircuitBreakerState>> {
        read_active_breaker(&self.breaker_path)
    }

    pub fn trip_from_loaded_pnl_if_needed(&self) -> Result<Option<CircuitBreakerState>> {
        if !self.should_trip() {
            return Ok(None);
        }
        let Some(last_trade) = self.last_trade.as_ref() else {
            return Ok(None);
        };
        let state = self.breaker_from_row(last_trade);
        write_breaker(&self.breaker_path, &state)?;
        Ok(Some(state))
    }

    pub fn record_trade(&mut self, row: TradeLedgerRow) -> Result<PnlUpdate> {
        append_trade(&self.ledger_path, &row)?;
        if row.timestamp >= self.since {
            self.loaded_trades += 1;
            self.cumulative_pnl_usdc += row.actual_net_usd;
            self.last_trade = Some(row.clone());
        }
        let breaker = if self.should_trip() {
            let state = self.breaker_from_row(&row);
            write_breaker(&self.breaker_path, &state)?;
            Some(state)
        } else {
            None
        };
        Ok(PnlUpdate {
            cumulative_pnl_usdc: self.cumulative_pnl_usdc,
            trade_count: self.loaded_trades,
            breaker,
        })
    }

    fn should_trip(&self) -> bool {
        self.cumulative_pnl_usdc <= -self.max_loss_usdc
    }

    fn breaker_from_row(&self, row: &TradeLedgerRow) -> CircuitBreakerState {
        CircuitBreakerState {
            active: true,
            triggered_at: Utc::now(),
            market: self.market.0.clone(),
            pnl_since: self.since,
            cumulative_pnl_usdc: self.cumulative_pnl_usdc,
            max_loss_usdc: self.max_loss_usdc,
            last_trade_timestamp: row.timestamp,
            last_trade_actual_net_usd: row.actual_net_usd,
            last_aster_order_id: row.aster_order_id,
            last_lighter_client_order_index: row.lighter_client_order_index,
            final_aster_position: row.final_aster_position,
            final_lighter_position: row.final_lighter_position,
            final_net_position: row.final_net_position,
        }
    }
}

pub fn reset_circuit_breaker(cfg: &PnlCfg, market: &MarketId) -> Result<Option<PathBuf>> {
    let dir = PathBuf::from(&cfg.persist_dir);
    let component = market_component(market);
    let breaker_path = dir.join(format!("circuit_breaker_{component}.json"));
    if !breaker_path.exists() {
        return Ok(None);
    }
    fs::create_dir_all(&dir)
        .with_context(|| format!("create pnl persist dir {}", dir.display()))?;
    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let archive_path = dir.join(format!("circuit_breaker_{component}.{stamp}.json"));
    fs::rename(&breaker_path, &archive_path).with_context(|| {
        format!(
            "archive breaker {} to {}",
            breaker_path.display(),
            archive_path.display()
        )
    })?;
    Ok(Some(archive_path))
}

pub fn parse_since(raw: &str, bot_start: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let raw = raw.trim();
    if raw.eq_ignore_ascii_case("startup") || raw.eq_ignore_ascii_case("now") {
        return Ok(bot_start);
    }
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .with_context(|| format!("parse pnl.since={raw:?} as RFC3339 timestamp"))
}

fn load_cumulative_pnl(
    path: &Path,
    since: DateTime<Utc>,
) -> Result<(usize, Decimal, Option<TradeLedgerRow>)> {
    if !path.exists() {
        return Ok((0, Decimal::ZERO, None));
    }
    let file = File::open(path).with_context(|| format!("open pnl ledger {}", path.display()))?;
    let mut count = 0usize;
    let mut total = Decimal::ZERO;
    let mut last_trade = None;
    for (idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.with_context(|| format!("read pnl ledger line {}", idx + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: TradeLedgerRow = serde_json::from_str(&line)
            .with_context(|| format!("parse pnl ledger {} line {}", path.display(), idx + 1))?;
        if row.timestamp >= since {
            count += 1;
            total += row.actual_net_usd;
            last_trade = Some(row);
        }
    }
    Ok((count, total, last_trade))
}

fn append_trade(path: &Path, row: &TradeLedgerRow) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create pnl ledger dir {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open pnl ledger for append {}", path.display()))?;
    serde_json::to_writer(&mut file, row).context("serialize pnl trade row")?;
    file.write_all(b"\n")
        .context("write pnl trade row newline")?;
    file.flush().context("flush pnl trade ledger")?;
    Ok(())
}

fn read_active_breaker(path: &Path) -> Result<Option<CircuitBreakerState>> {
    if !path.exists() {
        return Ok(None);
    }
    let text =
        fs::read_to_string(path).with_context(|| format!("read breaker {}", path.display()))?;
    let state: CircuitBreakerState =
        serde_json::from_str(&text).with_context(|| format!("parse breaker {}", path.display()))?;
    if state.active {
        Ok(Some(state))
    } else {
        Ok(None)
    }
}

fn write_breaker(path: &Path, state: &CircuitBreakerState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create breaker dir {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    let text = serde_json::to_string_pretty(state).context("serialize circuit breaker")?;
    fs::write(&tmp, text).with_context(|| format!("write breaker temp {}", tmp.display()))?;
    fs::rename(&tmp, path)
        .with_context(|| format!("move breaker temp {} to {}", tmp.display(), path.display()))?;
    Ok(())
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

pub fn format_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Secs, true)
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
            "lighter_aster_taker_arb_{name}_{}_{}",
            std::process::id(),
            nanos
        ))
    }

    fn cfg(dir: &Path) -> PnlCfg {
        PnlCfg {
            enabled: true,
            persist_dir: dir.display().to_string(),
            since: "2026-06-23T23:00:00Z".to_string(),
            max_loss_usdc: dec!(5),
        }
    }

    fn row(ts: &str, net: Decimal) -> TradeLedgerRow {
        TradeLedgerRow {
            timestamp: DateTime::parse_from_rfc3339(ts)
                .unwrap()
                .with_timezone(&Utc),
            market: "HYPE".to_string(),
            direction: "SELL_ASTER_BUY_LIGHTER".to_string(),
            qty: dec!(0.17),
            expected_net_usd: net,
            actual_gross_usd: net,
            actual_fees_usd: Decimal::ZERO,
            actual_net_usd: net,
            actual_net_bps: Decimal::ZERO,
            fill_qty_mismatch: Decimal::ZERO,
            aster_fill: FillSummary::from_qty_notional(dec!(0.17), dec!(10), Decimal::ZERO)
                .unwrap(),
            lighter_fill: FillSummary::from_qty_notional(dec!(0.17), dec!(10), Decimal::ZERO)
                .unwrap(),
            aster_order_id: 1,
            lighter_client_order_index: 2,
            final_aster_position: dec!(-0.17),
            final_lighter_position: dec!(0.17),
            final_net_position: Decimal::ZERO,
            available_before_usd: dec!(100),
            available_after_usd: dec!(100),
            aster_available_before_usd: dec!(50),
            aster_available_after_usd: dec!(50),
            lighter_available_before_usd: dec!(50),
            lighter_available_after_usd: dec!(50),
        }
    }

    #[test]
    fn default_since_is_long_term_window_start() {
        assert_eq!(PnlCfg::default().since, "2026-06-23T23:00:00Z");
    }

    #[test]
    fn startup_since_resolves_to_bot_start() {
        let start = DateTime::parse_from_rfc3339("2026-06-24T01:02:03Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(parse_since("startup", start).unwrap(), start);
        assert_eq!(parse_since("now", start).unwrap(), start);
    }

    #[test]
    fn rfc3339_since_parses() {
        let start = Utc::now();
        let parsed = parse_since("2026-06-23T23:00:00Z", start).unwrap();
        assert_eq!(format_ts(parsed), "2026-06-23T23:00:00Z");
    }

    #[test]
    fn ledger_ignores_rows_before_since() {
        let dir = tmp_dir("ledger");
        let market = MarketId::from("HYPE");
        let tracker = PnlTracker::new(&cfg(&dir), &market, Utc::now()).unwrap();
        append_trade(
            &tracker.ledger_path,
            &row("2026-06-23T22:59:59Z", dec!(-100)),
        )
        .unwrap();
        append_trade(
            &tracker.ledger_path,
            &row("2026-06-23T23:00:00Z", dec!(1.25)),
        )
        .unwrap();
        let tracker = PnlTracker::new(&cfg(&dir), &market, Utc::now()).unwrap();
        assert_eq!(tracker.snapshot().loaded_trades, 1);
        assert_eq!(tracker.snapshot().cumulative_pnl_usdc, dec!(1.25));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn breaker_trips_at_exact_max_loss() {
        let dir = tmp_dir("trip");
        let market = MarketId::from("HYPE");
        let mut tracker = PnlTracker::new(&cfg(&dir), &market, Utc::now()).unwrap();
        let update = tracker
            .record_trade(row("2026-06-23T23:00:01Z", dec!(-5.00)))
            .unwrap();
        assert!(update.breaker.is_some());
        assert!(tracker.active_breaker().unwrap().is_some());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn breaker_does_not_trip_before_max_loss() {
        let dir = tmp_dir("notrip");
        let market = MarketId::from("HYPE");
        let mut tracker = PnlTracker::new(&cfg(&dir), &market, Utc::now()).unwrap();
        let update = tracker
            .record_trade(row("2026-06-23T23:00:01Z", dec!(-4.99)))
            .unwrap();
        assert!(update.breaker.is_none());
        assert!(tracker.active_breaker().unwrap().is_none());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn reset_archives_and_clears_breaker() {
        let dir = tmp_dir("reset");
        let market = MarketId::from("HYPE");
        let config = cfg(&dir);
        let mut tracker = PnlTracker::new(&config, &market, Utc::now()).unwrap();
        tracker
            .record_trade(row("2026-06-23T23:00:01Z", dec!(-5.00)))
            .unwrap();
        let archive = reset_circuit_breaker(&config, &market).unwrap().unwrap();
        assert!(archive.exists());
        assert!(tracker.active_breaker().unwrap().is_none());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn startup_recreates_breaker_from_loaded_loss_window() {
        let dir = tmp_dir("startup_trip");
        let market = MarketId::from("HYPE");
        let config = cfg(&dir);
        let tracker = PnlTracker::new(&config, &market, Utc::now()).unwrap();
        append_trade(
            &tracker.ledger_path,
            &row("2026-06-23T23:00:01Z", dec!(-5.01)),
        )
        .unwrap();
        let tracker = PnlTracker::new(&config, &market, Utc::now()).unwrap();
        let breaker = tracker.trip_from_loaded_pnl_if_needed().unwrap().unwrap();
        assert_eq!(breaker.cumulative_pnl_usdc, dec!(-5.01));
        assert!(tracker.active_breaker().unwrap().is_some());
        let _ = fs::remove_dir_all(dir);
    }
}
