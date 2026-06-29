use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use rust_decimal::Decimal;

use crate::aster::creds::{AsterCreds, LighterCreds};
use crate::aster::rest::AsterRest;
use crate::aster::sign::{AsterSigner, EvmAsterSigner};
use crate::config::Config;
use crate::connectors::rest_book;
use crate::connectors::rest_specs;
use crate::decimal::bps_to_rate;
use crate::types::Side;
use crate::venues::lighter::LighterVenue;

#[derive(Parser, Debug)]
#[command(
    name = "lighter_aster_taker_arb",
    version,
    about = "Standalone Lighter/Aster taker-taker arbitrage bot"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[arg(long, global = true, default_value = "configs/live-hype.toml")]
    pub config: PathBuf,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Run {
        #[arg(long, default_value = "HYPE")]
        markets: Option<String>,
        #[arg(long)]
        secs: Option<u64>,
        #[arg(long)]
        max_trades: Option<u64>,
        #[arg(long)]
        min_size: bool,
        /// Scan and persist opportunity history without submitting orders.
        #[arg(long)]
        observe_only: bool,
        /// Restrict executable opportunities by exposure effect.
        #[arg(long, value_enum, default_value_t = crate::arb::ExposureFilter::Any)]
        exposure_filter: crate::arb::ExposureFilter,
        /// Optional JSON lease file. When set, missing/expired/invalid lease prevents execution.
        #[arg(long)]
        control_file: Option<PathBuf>,
        /// Optional JSON output file for confirmed reduce-burst signals.
        #[arg(long)]
        signal_file: Option<PathBuf>,
        /// Cooldown after reduce-filtered trades.
        #[arg(long, default_value_t = 5_000)]
        reduce_cooldown_ms: u64,
        /// Number of reduce opportunities required inside the signal window.
        #[arg(long, default_value_t = 3)]
        reduce_signal_min_samples: usize,
        /// Reduce-burst signal confirmation window.
        #[arg(long, default_value_t = 2_000)]
        reduce_signal_window_ms: i64,
    },
    FetchSpecs {
        #[arg(long)]
        markets: Option<String>,
    },
    Probe {
        #[arg(long, default_value = "HYPE")]
        market: Option<String>,
    },
    /// Live Aster MARKET-order roundtrip using the real Aster taker code path.
    ///
    /// Requires a flat Aster starting position. Buys up to `--max-usd`, checks
    /// balance/position, then sells the actual resulting position reduce-only and
    /// verifies the final Aster position is flat.
    AsterMarketRoundtrip {
        #[arg(long, default_value = "HYPE")]
        market: Option<String>,
        #[arg(long)]
        i_understand_live: bool,
        #[arg(long)]
        max_usd: Decimal,
    },
    /// Live Lighter MARKET-order roundtrip using the real native signer path.
    ///
    /// Requires a flat Lighter starting position. Buys the minimum executable
    /// size capped by `--max-usd`, then sells the resulting position reduce-only
    /// and verifies the final Lighter position is flat.
    LighterMarketRoundtrip {
        #[arg(long, default_value = "HYPE")]
        market: Option<String>,
        #[arg(long)]
        i_understand_live: bool,
        #[arg(long)]
        max_usd: Decimal,
    },
    /// Archive an active per-market loss circuit breaker so trading can be restarted manually.
    ResetCircuitBreaker {
        #[arg(long, default_value = "HYPE")]
        market: Option<String>,
    },
    /// Read-only machine-readable account/book/opportunity status for orchestration.
    Status {
        #[arg(long, default_value = "HYPE")]
        market: Option<String>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
}

pub async fn dispatch(cli: Cli) -> Result<()> {
    let cfg = Config::load(&cli.config)?;
    match cli.command {
        Commands::Run {
            markets,
            secs,
            max_trades,
            min_size,
            observe_only,
            exposure_filter,
            control_file,
            signal_file,
            reduce_cooldown_ms,
            reduce_signal_min_samples,
            reduce_signal_window_ms,
        } => {
            let selected = cfg.select_markets(markets.as_deref());
            if selected.is_empty() {
                anyhow::bail!("no markets selected");
            }
            crate::arb::run(
                cfg,
                selected,
                crate::arb::RunOptions {
                    secs,
                    max_trades,
                    min_size,
                    observe_only,
                    exposure_filter,
                    control_file,
                    signal_file,
                    reduce_cooldown_ms,
                    reduce_signal_min_samples,
                    reduce_signal_window_ms,
                },
            )
            .await
        }
        Commands::FetchSpecs { markets } => {
            let selected = cfg.select_markets(markets.as_deref());
            let specs = rest_specs::build_market_specs(
                &selected,
                &cfg.venues.aster_base_url,
                &cfg.venues.lighter_base_url,
            )
            .await?;
            println!(
                "{:<8} {:<12} {:<12} {:>8} {:>12} {:>12} {:>12} {:>12}",
                "id", "aster", "lighter", "mkt_id", "a_step", "l_step", "a_min_notl", "l_min_notl"
            );
            for s in specs {
                println!(
                    "{:<8} {:<12} {:<12} {:>8} {:>12} {:>12} {:>12} {:>12}",
                    s.market_id.0,
                    s.aster_symbol,
                    s.lighter_symbol,
                    s.lighter_market_id,
                    s.step,
                    s.lighter_qty_step,
                    s.aster_min_notional,
                    s.lighter_min_notional
                );
            }
            Ok(())
        }
        Commands::Probe { market } => {
            let selected = cfg.select_markets(market.as_deref());
            let spec = rest_specs::build_market_specs(
                &selected,
                &cfg.venues.aster_base_url,
                &cfg.venues.lighter_base_url,
            )
            .await?
            .into_iter()
            .next()
            .context("no selected market spec")?;
            let aster_env =
                std::env::var("ASTER_ENV_PATH").unwrap_or_else(|_| "aster.env".to_string());
            let lighter_env =
                std::env::var("LIGHTER_ENV_PATH").unwrap_or_else(|_| "lighter.env".to_string());
            let acreds = AsterCreds::load(std::path::Path::new(&aster_env))?;
            let lcreds = LighterCreds::load(std::path::Path::new(&lighter_env))?;
            let signer: Arc<dyn AsterSigner> =
                Arc::new(EvmAsterSigner::new(acreds.user, acreds.signer, acreds.key)?);
            let aster = AsterRest::new(
                cfg.venues.aster_base_url.clone(),
                signer,
                std::slice::from_ref(&spec),
            )?;
            let lighter = LighterVenue::new(
                &cfg.venues.lighter_base_url,
                std::path::Path::new(&cfg.venues.signers_dir),
                lcreds,
                std::slice::from_ref(&spec),
            )
            .await?;
            lighter
                .wait_ready(&spec.market_id, std::time::Duration::from_secs(20))
                .await?;
            let (ap, lp, aa, la, ao, lo) = tokio::join!(
                aster.position_qty(&spec.market_id),
                lighter.position_qty(&spec.market_id),
                aster.available_usdc(),
                lighter.available_usdc(),
                aster.open_orders(&spec.market_id),
                lighter.open_orders_count(&spec.market_id),
            );
            println!("market={}", spec.market_id);
            println!("aster_position={}", ap?);
            println!("lighter_position={}", lp?);
            println!("aster_available_usd={}", aa?);
            println!("lighter_available_usd={}", la?);
            println!("aster_open_orders={}", ao?.len());
            println!("lighter_open_orders={}", lo?);
            Ok(())
        }
        Commands::AsterMarketRoundtrip {
            market,
            i_understand_live,
            max_usd,
        } => {
            if !i_understand_live {
                bail!("refusing live Aster market roundtrip without --i-understand-live");
            }
            if max_usd <= Decimal::ZERO {
                bail!("--max-usd must be positive");
            }
            let selected = cfg.select_markets(market.as_deref());
            let spec = rest_specs::build_market_specs(
                &selected,
                &cfg.venues.aster_base_url,
                &cfg.venues.lighter_base_url,
            )
            .await?
            .into_iter()
            .next()
            .context("no selected market spec")?;
            let aster_env =
                std::env::var("ASTER_ENV_PATH").unwrap_or_else(|_| "aster.env".to_string());
            let acreds = AsterCreds::load(std::path::Path::new(&aster_env))?;
            let signer: Arc<dyn AsterSigner> =
                Arc::new(EvmAsterSigner::new(acreds.user, acreds.signer, acreds.key)?);
            let aster = AsterRest::new(
                cfg.venues.aster_base_url.clone(),
                signer,
                std::slice::from_ref(&spec),
            )?;

            let http = rest_book::client()?;
            let book = rest_book::fetch_aster_book(
                &http,
                &cfg.venues.aster_base_url,
                &spec.aster_symbol,
                20,
            )
            .await?;
            let ask = book.best_ask().context("Aster book has no ask")?;
            let bid = book.best_bid().context("Aster book has no bid")?;
            if book.is_crossed() {
                bail!("Aster book is crossed/locked; refusing roundtrip");
            }
            let initial_pos = aster.position_qty(&spec.market_id).await?;
            let open_orders = aster.open_orders(&spec.market_id).await?;
            if !open_orders.is_empty() {
                bail!(
                    "Aster has {} open order(s); refusing roundtrip",
                    open_orders.len()
                );
            }
            if initial_pos != Decimal::ZERO {
                bail!("Aster starting position is {initial_pos}, not flat; refusing roundtrip");
            }
            let qty = floor_to_step(max_usd / ask.px, spec.step);
            if qty <= Decimal::ZERO {
                bail!(
                    "--max-usd rounds to zero quantity at Aster step {}",
                    spec.step
                );
            }
            if qty < spec.aster_min_qty || qty * ask.px < spec.aster_min_notional {
                bail!(
                    "roundtrip size too small: qty={} notional={} min_qty={} min_notional={}",
                    qty,
                    qty * ask.px,
                    spec.aster_min_qty,
                    spec.aster_min_notional
                );
            }
            println!(
                "aster_market_roundtrip_start market={} qty={} ask={} bid={} max_usd={}",
                spec.market_id, qty, ask.px, bid.px, max_usd
            );
            let bal_before = aster.available_usdc().await?;
            println!("balance_before={bal_before}");

            let buy = aster
                .submit_market_order(&spec.market_id, Side::Buy, qty, false)
                .await;
            println!("buy_result={buy:?}");
            ensure_accepted("buy", &buy)?;
            let buy_fill = wait_aster_fill("buy", &aster, &spec.market_id, &buy, qty).await?;
            println!("buy_fill={buy_fill:?}");
            let after_buy =
                wait_position_after_buy(&aster, &spec.market_id, qty, spec.step).await?;
            let bal_after_buy = aster.available_usdc().await?;
            println!("position_after_buy={after_buy}");
            println!("balance_after_buy={bal_after_buy}");

            let sell_qty = floor_to_step(after_buy.abs(), spec.step);
            if sell_qty <= Decimal::ZERO {
                bail!("buy accepted but no positive position visible to sell");
            }
            let sell = aster
                .submit_market_order(&spec.market_id, Side::Sell, sell_qty, true)
                .await;
            println!("sell_result={sell:?}");
            ensure_accepted("sell", &sell)?;
            let sell_fill =
                wait_aster_fill("sell", &aster, &spec.market_id, &sell, sell_qty).await?;
            println!("sell_fill={sell_fill:?}");
            let final_pos = wait_position_flat(&aster, &spec.market_id, Decimal::ZERO).await?;
            let bal_after_sell = aster.available_usdc().await?;
            println!("position_final={final_pos}");
            println!("balance_after_sell={bal_after_sell}");
            Ok(())
        }
        Commands::LighterMarketRoundtrip {
            market,
            i_understand_live,
            max_usd,
        } => {
            if !i_understand_live {
                bail!("refusing live Lighter market roundtrip without --i-understand-live");
            }
            if max_usd <= Decimal::ZERO {
                bail!("--max-usd must be positive");
            }
            let selected = cfg.select_markets(market.as_deref());
            let spec = rest_specs::build_market_specs(
                &selected,
                &cfg.venues.aster_base_url,
                &cfg.venues.lighter_base_url,
            )
            .await?
            .into_iter()
            .next()
            .context("no selected market spec")?;
            let lighter_env =
                std::env::var("LIGHTER_ENV_PATH").unwrap_or_else(|_| "lighter.env".to_string());
            let lcreds = LighterCreds::load(std::path::Path::new(&lighter_env))?;
            let lighter = LighterVenue::new(
                &cfg.venues.lighter_base_url,
                std::path::Path::new(&cfg.venues.signers_dir),
                lcreds,
                std::slice::from_ref(&spec),
            )
            .await?;

            lighter
                .wait_ready(&spec.market_id, std::time::Duration::from_secs(20))
                .await?;
            let book = lighter.order_book(&spec.market_id)?;
            let ask = book.best_ask().context("Lighter book has no ask")?;
            let bid = book.best_bid().context("Lighter book has no bid")?;
            if book.is_crossed() {
                bail!("Lighter book is crossed/locked; refusing roundtrip");
            }
            let initial_pos = lighter.position_qty(&spec.market_id).await?;
            let open_orders = lighter.open_orders_count(&spec.market_id).await?;
            if open_orders > 0 {
                bail!("Lighter has {open_orders} open order(s); refusing roundtrip");
            }
            if initial_pos != Decimal::ZERO {
                bail!("Lighter starting position is {initial_pos}, not flat; refusing roundtrip");
            }

            let min_qty = ceil_to_step(spec.lighter_min_notional / ask.px, spec.lighter_qty_step);
            let qty = min_qty.max(spec.lighter_qty_step);
            let notional = qty * ask.px;
            if qty <= Decimal::ZERO {
                bail!(
                    "Lighter minimum rounds to zero at qty step {}",
                    spec.lighter_qty_step
                );
            }
            if notional > max_usd {
                bail!(
                    "minimum Lighter roundtrip notional {} exceeds --max-usd {}; raise --max-usd",
                    notional,
                    max_usd
                );
            }

            let slippage = bps_to_rate(cfg.arb.emergency_slippage_bps);
            let buy_bound = ask.px * (Decimal::ONE + slippage);
            let sell_bound = bid.px * (Decimal::ONE - slippage);
            println!(
                "lighter_market_roundtrip_start market={} market_id={} qty={} bid={} ask={} buy_bound={} sell_bound={} max_usd={} min_notional={}",
                spec.market_id,
                spec.lighter_market_id,
                qty,
                bid.px,
                ask.px,
                buy_bound,
                sell_bound,
                max_usd,
                spec.lighter_min_notional
            );
            let bal_before = lighter.available_usdc().await?;
            println!("balance_before={bal_before}");

            let buy = lighter
                .submit_market_order(&spec.market_id, Side::Buy, qty, buy_bound, false)
                .await;
            println!("buy_result={buy:?}");
            ensure_lighter_accepted("buy", &buy)?;
            ensure_lighter_fill("buy", &buy)?;
            let after_buy = wait_lighter_position_after_buy(
                &lighter,
                &spec.market_id,
                qty,
                spec.lighter_qty_step,
            )
            .await?;
            let bal_after_buy = lighter.available_usdc().await?;
            println!("position_after_buy={after_buy}");
            println!("balance_after_buy={bal_after_buy}");

            let sell_qty = floor_to_step(after_buy.abs(), spec.lighter_qty_step);
            if sell_qty <= Decimal::ZERO {
                bail!("buy accepted but no positive Lighter position visible to sell");
            }
            let sell = lighter
                .submit_market_order(&spec.market_id, Side::Sell, sell_qty, sell_bound, true)
                .await;
            println!("sell_result={sell:?}");
            ensure_lighter_accepted("sell", &sell)?;
            ensure_lighter_fill("sell", &sell)?;
            let final_pos =
                wait_lighter_position_flat(&lighter, &spec.market_id, Decimal::ZERO).await?;
            let bal_after_sell = lighter.available_usdc().await?;
            let final_open_orders = lighter.open_orders_count(&spec.market_id).await?;
            println!("position_final={final_pos}");
            println!("balance_after_sell={bal_after_sell}");
            println!("open_orders_final={final_open_orders}");
            if final_open_orders > 0 {
                bail!("Lighter roundtrip left {final_open_orders} open order(s)");
            }
            Ok(())
        }
        Commands::ResetCircuitBreaker { market } => {
            let selected = cfg.select_markets(market.as_deref());
            let market_id = selected
                .into_iter()
                .next()
                .context("no selected market for reset-circuit-breaker")?
                .id();
            match crate::pnl::reset_circuit_breaker(&cfg.pnl, &market_id)? {
                Some(path) => {
                    println!(
                        "circuit_breaker_reset market={} archive={}",
                        market_id,
                        path.display()
                    );
                }
                None => {
                    println!("circuit_breaker_reset market={} active=false", market_id);
                }
            }
            println!("pnl_since={}", cfg.pnl.since);
            println!("pnl_persist_dir={}", cfg.pnl.persist_dir);
            Ok(())
        }
        Commands::Status { market, json } => {
            let selected = cfg.select_markets(market.as_deref());
            if selected.is_empty() {
                anyhow::bail!("no markets selected");
            }
            crate::status::run(&cfg, selected, json).await
        }
    }
}

fn ensure_accepted(label: &str, outcome: &crate::aster::rest::SubmitOutcome) -> Result<()> {
    match outcome {
        crate::aster::rest::SubmitOutcome::Accepted { .. } => Ok(()),
        other => bail!("{label} market order was not accepted: {other:?}"),
    }
}

async fn wait_aster_fill(
    label: &str,
    aster: &AsterRest,
    market: &crate::types::MarketId,
    outcome: &crate::aster::rest::SubmitOutcome,
    expected_qty: Decimal,
) -> Result<crate::types::FillSummary> {
    let crate::aster::rest::SubmitOutcome::Accepted {
        venue_order_id: Some(order_id),
        ..
    } = outcome
    else {
        bail!("{label} Aster market order was accepted without an orderId: {outcome:?}");
    };
    aster
        .wait_order_fill_summary(
            market,
            *order_id,
            expected_qty,
            std::time::Duration::from_secs(10),
        )
        .await
}

fn ensure_lighter_accepted(
    label: &str,
    outcome: &crate::venues::lighter::SubmitOutcome,
) -> Result<()> {
    match outcome {
        crate::venues::lighter::SubmitOutcome::Accepted { .. } => Ok(()),
        other => bail!("{label} Lighter market order was not accepted: {other:?}"),
    }
}

fn ensure_lighter_fill(
    label: &str,
    outcome: &crate::venues::lighter::SubmitOutcome,
) -> Result<crate::types::FillSummary> {
    match outcome {
        crate::venues::lighter::SubmitOutcome::Accepted {
            fill: Some(fill), ..
        } => Ok(*fill),
        other => bail!("{label} Lighter market order accepted without fill detail: {other:?}"),
    }
}

async fn wait_position_after_buy(
    aster: &AsterRest,
    market: &crate::types::MarketId,
    target: Decimal,
    tolerance: Decimal,
) -> Result<Decimal> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut last = Decimal::ZERO;
    loop {
        let pos = aster.position_qty(market).await?;
        if pos > Decimal::ZERO {
            last = pos;
        }
        if pos >= target - tolerance {
            return Ok(pos);
        }
        if tokio::time::Instant::now() >= deadline {
            if last > Decimal::ZERO {
                return Ok(last);
            }
            bail!(
                "timed out waiting for Aster position >= {}; last={}",
                target,
                pos
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

async fn wait_position_flat(
    aster: &AsterRest,
    market: &crate::types::MarketId,
    tolerance: Decimal,
) -> Result<Decimal> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let pos = aster.position_qty(market).await?;
        if pos.abs() <= tolerance {
            return Ok(pos);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("timed out waiting for Aster flat position; last={}", pos);
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

async fn wait_lighter_position_after_buy(
    lighter: &LighterVenue,
    market: &crate::types::MarketId,
    target: Decimal,
    tolerance: Decimal,
) -> Result<Decimal> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut last = Decimal::ZERO;
    loop {
        let pos = lighter.position_qty(market).await?;
        if pos > Decimal::ZERO {
            last = pos;
        }
        if pos >= target - tolerance {
            return Ok(pos);
        }
        if tokio::time::Instant::now() >= deadline {
            if last > Decimal::ZERO {
                return Ok(last);
            }
            bail!(
                "timed out waiting for Lighter position >= {}; last={}",
                target,
                pos
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

async fn wait_lighter_position_flat(
    lighter: &LighterVenue,
    market: &crate::types::MarketId,
    tolerance: Decimal,
) -> Result<Decimal> {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let pos = lighter.position_qty(market).await?;
        if pos.abs() <= tolerance {
            return Ok(pos);
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("timed out waiting for Lighter flat position; last={}", pos);
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

fn floor_to_step(qty: Decimal, step: Decimal) -> Decimal {
    if qty <= Decimal::ZERO || step <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (qty / step).floor() * step
}

fn ceil_to_step(qty: Decimal, step: Decimal) -> Decimal {
    if qty <= Decimal::ZERO || step <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    (qty / step).ceil() * step
}
