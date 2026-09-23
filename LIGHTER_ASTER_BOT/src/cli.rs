//! Command-line interface: `run` (the bot: both engines, one market) and the XEMM/research
//! subcommands `record`, `replay`, `report`, `livebot`, `live-report`, `probe`, `status`,
//! `fetch-specs`, `verify-books`, `verify-db`. The taker engine has its own CLI under `taker`.

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "lighter_aster_bot",
    version,
    about = "Aster/Lighter bot (`run`): taker-taker arbitrage and an XEMM inventory unwinder with execution rights switched in memory; plus probes and research tools",
    after_help = "Taker engine on its own: `lighter_aster_bot taker --help`."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Path to the TOML config file (bot.toml; XEMM commands read its [maker] table).
    #[arg(long, global = true, default_value = "bot.toml")]
    pub config: PathBuf,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run the bot for one market: the taker holds execution rights while it has margin; a
    /// reduce-only XEMM unwinds inventory when it does not. REAL orders only with `--mode live`.
    Run {
        /// Market id, listed once in both [[taker.markets]] and [[maker.markets]] (e.g. HYPE).
        #[arg(long)]
        market: String,
        /// live | paper (required). Paper runs XEMM on its simulated executor and the taker
        /// observe-only; account state is still read with the real credentials.
        #[arg(long)]
        mode: String,
        /// Archive a latched bot breaker (`runs/bot-<M>.breaker.json`) and start.
        #[arg(long, default_value_t = false)]
        ack_breaker: bool,
        /// Discard the persisted equity baseline: the drawdown stop re-arms on the first sample.
        #[arg(long, default_value_t = false)]
        reset_breaker_baseline: bool,
    },

    /// Record live Aster + Lighter market data to a JSONL event log (no simulation).
    Record {
        /// Comma-separated market ids (e.g. BTC,ETH). Defaults to all config markets.
        #[arg(long)]
        markets: Option<String>,
        /// Recording duration in seconds.
        #[arg(long, default_value_t = 120)]
        secs: u64,
        /// Output event-log path. A `.zst` suffix (the default) writes a zstd-compressed
        /// log; any other suffix writes plain JSONL. Replay auto-detects either.
        #[arg(long, default_value = "runs/session.jsonl.zst")]
        out: PathBuf,
    },

    /// Replay a recorded JSONL event log deterministically through the simulation into SQLite.
    Replay {
        /// Input JSONL event log.
        #[arg(long)]
        events: PathBuf,
        /// Output SQLite database path.
        #[arg(long, default_value = "runs/eval.sqlite")]
        db: PathBuf,
    },

    /// Aggregate a replayed SQLite database into a console + JSON + CSV report.
    Report {
        #[arg(long, default_value = "runs/eval.sqlite")]
        db: PathBuf,
        /// Restrict to a run id (defaults to the latest run).
        #[arg(long)]
        run_id: Option<String>,
        /// Output directory for report.json / report.csv.
        #[arg(long, default_value = "runs")]
        out: PathBuf,
    },

    /// Summarize a livebot journal into logical trades and versioned execution economics.
    LiveReport {
        /// Livebot results DB; used to infer `<db-stem>-journal.jsonl` when --journal is omitted.
        /// The default is the live `run` bot's XEMM stem for HYPE.
        #[arg(long, default_value = "runs/bot-HYPE.sqlite")]
        db: PathBuf,
        /// Explicit journal path. Overrides --db inference.
        #[arg(long)]
        journal: Option<PathBuf>,
        /// Restrict to one market id, e.g. HYPE.
        #[arg(long)]
        market: Option<String>,
        /// Include logical trades at/after this epoch-milliseconds timestamp.
        /// Pair cumulative and individual evidence before filtering by economic time.
        #[arg(long)]
        since_ms: Option<i64>,
        /// Print one row per logical trade, including incomplete economics.
        #[arg(long, default_value_t = false)]
        details: bool,
        /// Print a machine-readable JSON summary.
        #[arg(long, default_value_t = false)]
        json: bool,
    },


    /// Run the XEMM engine alone. `--mode paper` (the selected markets, NO real orders) records
    /// the market tape + persists research results. `--mode live` (one market, real funds) is
    /// gated behind `[live] enabled = true` and wired live signers.
    Livebot {
        #[arg(long, default_value = "HYPE")]
        markets: Option<String>,
        /// paper | live.
        #[arg(long, default_value = "paper")]
        mode: String,
        /// Optional duration in seconds; runs until stopped if omitted.
        #[arg(long)]
        secs: Option<u64>,
        /// Market-data tape path.
        #[arg(long, default_value = "runs/livebot.jsonl.zst")]
        out: Option<PathBuf>,
        /// Results database (appended, never recreated — reused/recovered across restarts).
        #[arg(long, default_value = "runs/livebot.sqlite")]
        db: PathBuf,
    },

    /// Probe a single live venue primitive: balance / open-orders / post-only
    /// place+cancel far from mid / IOC market round-trip. No-risk checks run freely; the
    /// money-risking `lighter-market` needs `--i-understand-live --max-usd <N>`. Uses the real
    /// signers + `aster.env`/`lighter.env`.
    Probe {
        /// Which check: aster-balance | aster-positions | aster-open-orders | aster-place-cancel |
        /// leverage | lighter-balance | lighter-open-orders | lighter-order-dry-run | lighter-market
        check: String,
        /// Target market id from config (e.g. HYPE). Defaults to HYPE.
        #[arg(long)]
        market: Option<String>,
        /// Required confirmation for money-risking probes (lighter-market).
        #[arg(long, default_value_t = false)]
        i_understand_live: bool,
        /// USD cap for money-risking probes.
        #[arg(long, default_value = "0")]
        max_usd: rust_decimal::Decimal,
    },

    /// Read-only account/book/quote status: the XEMM report `run` polls every tick.
    Status {
        /// Target market id from config (e.g. HYPE). Defaults to HYPE.
        #[arg(long)]
        market: Option<String>,
        /// Print a machine-readable JSON report.
        #[arg(long, default_value_t = false)]
        json: bool,
    },

    /// Fetch and print resolved market specs (Aster exchangeInfo + Lighter orderBooks).
    FetchSpecs {
        #[arg(long)]
        markets: Option<String>,
    },

    /// One-shot check that the websocket-built books match the venues' REST snapshots.
    VerifyBooks {
        #[arg(long)]
        markets: Option<String>,
        /// Seconds to collect websocket books before comparing against REST.
        #[arg(long, default_value_t = 8)]
        secs: u64,
    },

    /// Audit a results SQLite database for internal consistency (orphaned/miscounted
    /// rows). Read-only; exits non-zero if any integrity invariant is violated.
    VerifyDb {
        #[arg(long, default_value = "runs/eval.sqlite")]
        db: PathBuf,
    },
}

/// Parse a `--mode` string into a [`crate::config::LiveMode`].
#[cfg(feature = "hotpath")]
fn parse_live_mode(s: &str) -> Result<crate::config::LiveMode> {
    use crate::config::LiveMode;
    match s.trim().to_ascii_lowercase().as_str() {
        "paper" => Ok(LiveMode::Paper),
        "live" => Ok(LiveMode::Live),
        other => anyhow::bail!("unknown --mode {other:?} (expected paper | live)"),
    }
}

/// Dispatch a parsed CLI to the appropriate module entry point.
pub async fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        #[cfg(feature = "hotpath")]
        Commands::Run { market, mode, ack_breaker, reset_breaker_baseline } => {
            let mode = parse_live_mode(&mode)?;
            crate::taker::abort_on_panic();
            // Listen from the start: a signal during setup still takes the graceful drain.
            let stop = crate::controller::stop_on_signals();
            crate::controller::run(&cli.config, &market, mode, ack_breaker, reset_breaker_baseline, stop).await?;
        }
        #[cfg(not(feature = "hotpath"))]
        Commands::Run { .. } => {
            anyhow::bail!("`run` requires the 'hotpath' feature (default); rebuild without --no-default-features");
        }
        Commands::Record { markets, secs, out } => {
            let cfg = crate::config::Config::load(&cli.config)?;
            let selected = cfg.select_markets(markets.as_deref());
            if selected.is_empty() {
                anyhow::bail!("no markets selected (check --markets against config [[markets]])");
            }
            let summary = crate::record::run(&cfg, selected, out, secs).await?;
            println!(
                "recorded {} events (run_id={}) -> {}",
                summary.events,
                summary.run_id,
                summary.out.display()
            );
        }
        Commands::Replay { events, db } => {
            // Default: use the config recorded in the log header. Override only when
            // the user explicitly passes a non-default --config path.
            let override_cfg = if cli.config.as_os_str() != "bot.toml" {
                Some(crate::config::Config::load(&cli.config)?)
            } else {
                None
            };
            let outcome = crate::replay::run(&events, &db, override_cfg)?;
            println!(
                "replay complete: run_id={} events={} window={}..{} db={}",
                outcome.run_id,
                outcome.events,
                outcome.started_at.to_rfc3339(),
                outcome.finished_at.to_rfc3339(),
                db.display()
            );
        }
        Commands::Report { db, run_id, out } => {
            crate::report::generate(&db, run_id, &out)?;
        }
        Commands::LiveReport { db, journal, market, since_ms, details, json } => {
            let journal_path = journal.unwrap_or_else(|| crate::live_report::inferred_journal_path(&db));
            let summary = crate::live_report::summarize_path(&journal_path, market.as_deref(), since_ms)?;
            if json {
                crate::live_report::print_summary_json(&journal_path, &summary)?;
            } else {
                crate::live_report::print_summary(&journal_path, &summary, details);
            }
        }
        #[cfg(feature = "hotpath")]
        Commands::Livebot { markets, mode, secs, out, db } => {
            let cfg = crate::config::Config::load(&cli.config)?;
            let selected = cfg.select_markets(markets.as_deref());
            if selected.is_empty() {
                anyhow::bail!("no markets selected (check --markets against config [[markets]])");
            }
            let mode = parse_live_mode(&mode)?;
            let _lock = match (mode.is_real(), selected.as_slice()) {
                (true, [market]) => Some(crate::controller::lock_market(&market.id().0)?),
                _ => None,
            };
            // Listen from the start: a signal during setup still takes the graceful drain.
            let stop = crate::controller::stop_on_signals();
            crate::livebot::run(&cfg, selected, secs, mode, out, db, true, stop).await?;
        }
        #[cfg(not(feature = "hotpath"))]
        Commands::Livebot { .. } => {
            anyhow::bail!("`livebot` requires the 'hotpath' feature (default); rebuild without --no-default-features");
        }
        #[cfg(feature = "hotpath")]
        Commands::Probe { check, market, i_understand_live, max_usd } => {
            let cfg = crate::config::Config::load(&cli.config)?;
            crate::livebot::probe::run(&cfg, &check, market, i_understand_live, max_usd).await?;
        }
        #[cfg(not(feature = "hotpath"))]
        Commands::Probe { .. } => {
            anyhow::bail!("`probe` requires the 'hotpath' feature (default); rebuild without --no-default-features");
        }
        #[cfg(feature = "hotpath")]
        Commands::Status { market, json } => {
            let cfg = crate::config::Config::load(&cli.config)?;
            crate::livebot::status::run(&cfg, market, json).await?;
        }
        #[cfg(not(feature = "hotpath"))]
        Commands::Status { .. } => {
            anyhow::bail!("`status` requires the 'hotpath' feature (default); rebuild without --no-default-features");
        }
        Commands::FetchSpecs { markets } => {
            let cfg = crate::config::Config::load(&cli.config)?;
            let selected = cfg.select_markets(markets.as_deref());
            let specs = crate::connectors::rest_specs::build_market_specs_with_bases(
                &selected,
                cfg.partials.hyperliquid_min_notional,
                &cfg.live.aster.base_url,
                &cfg.live.hyperliquid.base_url,
            )
            .await?;
            println!(
                "{:<6} {:<10} {:<10} {:>8} {:>12} {:>12} {:>12} {:>12} {:>7} {:>14}",
                "id", "aster", "lighter", "mkt_id", "tick", "step", "minQty", "minNotl", "szDec", "qtyStep"
            );
            for s in &specs {
                println!(
                    "{:<6} {:<10} {:<10} {:>8} {:>12} {:>12} {:>12} {:>12} {:>7} {:>14}",
                    s.market_id.0,
                    s.aster_symbol,
                    s.hl_coin,
                    s.lighter_market_id,
                    s.tick,
                    s.step,
                    s.aster_min_qty,
                    s.aster_min_notional,
                    s.hl_sz_decimals,
                    s.hl_qty_step
                );
            }
        }
        Commands::VerifyBooks { markets, secs } => {
            let cfg = crate::config::Config::load(&cli.config)?;
            let selected = cfg.select_markets(markets.as_deref());
            if selected.is_empty() {
                anyhow::bail!("no markets selected (check --markets against config [[markets]])");
            }
            crate::verify::run(&cfg, selected, secs).await?;
        }
        Commands::VerifyDb { db } => {
            crate::verify_db::run(&db)?;
        }
    }
    Ok(())
}
