//! The real engines behind the supervisor: the taker (`taker::arb::run`) and XEMM
//! (`livebot::run`) as tasks of this process, and their status reports from long-lived
//! pollers (one REST client per venue instead of a `status --json` process per poll).

use std::path::PathBuf;

use anyhow::Result;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::regime::{Bot, TakerMode};
use super::supervisor::{EngineIo, Engines, Role};
use super::BotConfig;
use crate::config::LiveMode;
use crate::taker::arb::RunOptions;

pub struct LiveEngines {
    taker_cfg: crate::taker::config::Config,
    taker_markets: Vec<crate::taker::config::MarketCfg>,
    maker_cfg: crate::config::Config,
    maker_markets: Vec<crate::config::MarketCfg>,
    mode: LiveMode,
    xemm_db: PathBuf,
    reduce_cooldown_ms: u64,
    reduce_burst_min_samples: usize,
    reduce_burst_window_ms: i64,
    taker_status: crate::taker::status::StatusPoller,
    xemm_status: crate::livebot::status::StatusPoller,
}

impl LiveEngines {
    pub async fn new(
        cfg: &BotConfig,
        market: &str,
        taker_markets: Vec<crate::taker::config::MarketCfg>,
        maker_markets: Vec<crate::config::MarketCfg>,
        mode: LiveMode,
        xemm_db: PathBuf,
    ) -> Result<Self> {
        Ok(Self {
            taker_status: crate::taker::status::StatusPoller::new(&cfg.taker, taker_markets.clone()).await?,
            xemm_status: crate::livebot::status::StatusPoller::new(&cfg.maker, market).await?,
            taker_cfg: cfg.taker.clone(),
            taker_markets,
            maker_cfg: cfg.maker.clone(),
            maker_markets,
            mode,
            xemm_db,
            reduce_cooldown_ms: cfg.controller.reduce_cooldown_ms,
            reduce_burst_min_samples: cfg.controller.reduce_burst_min_samples,
            reduce_burst_window_ms: cfg.controller.reduce_burst_window_ms,
        })
    }
}

impl Engines for LiveEngines {
    /// Paper mode runs XEMM on its simulated executor and every taker observe-only.
    fn spawn(&mut self, role: Role, io: &EngineIo, stop: CancellationToken) -> JoinHandle<Result<()>> {
        let observe_only = !self.mode.is_real();
        let (cfg, markets) = (self.taker_cfg.clone(), self.taker_markets.clone());
        match role {
            Role::Xemm => {
                let (cfg, markets, mode, db) = (self.maker_cfg.clone(), self.maker_markets.clone(), self.mode, self.xemm_db.clone());
                tokio::spawn(async move { crate::livebot::run(&cfg, markets, None, mode, None, db, false, stop).await })
            }
            Role::Taker(TakerMode::Normal) => {
                tokio::spawn(crate::taker::arb::run(cfg, markets, RunOptions { observe_only, ..RunOptions::default() }, stop))
            }
            Role::Taker(TakerMode::Reduce) | Role::Observer => {
                let options = RunOptions {
                    observe_only,
                    lease: Some(io.lease.clone()),
                    reduce_signals: Some(io.signals.clone()),
                    reduce_cooldown_ms: self.reduce_cooldown_ms,
                    reduce_signal_min_samples: self.reduce_burst_min_samples,
                    reduce_signal_window_ms: self.reduce_burst_window_ms,
                    ..RunOptions::default()
                };
                tokio::spawn(crate::taker::arb::run(cfg, markets, options, stop))
            }
        }
    }

    async fn status(&self, bot: Bot) -> Result<Value> {
        Ok(match bot {
            Bot::Taker => serde_json::to_value(self.taker_status.report().await?)?,
            Bot::Xemm => serde_json::to_value(self.xemm_status.report().await?)?,
        })
    }
}
