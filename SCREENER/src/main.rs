//! `screener`: which pairs listed on both Aster and Lighter would suit the bot's taker-taker and
//! XEMM strategies? `collect` records the moments that matter from public feeds; `report` scores
//! them with the bot's rules. Independent of the bot: public data only, no credentials, no orders.

mod collect;
mod config;
mod feeds;
mod report;
mod store;
mod universe;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(about = "Ranks Aster/Lighter pairs for the bot's taker-taker and XEMM strategies")]
struct Cli {
    #[arg(long, default_value = "screener.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List today's pairs and the name matches whose prices disagree.
    Universe,
    /// Record the pairs' public feeds into `out` until SIGINT/SIGTERM.
    Collect {
        #[arg(long, default_value = "data")]
        out: PathBuf,
    },
    /// Rank the pairs from the data in `data`.
    Report(report::Args),
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    let terminal = std::io::IsTerminal::is_terminal(&std::io::stdout());
    tracing_subscriber::fmt().with_env_filter(filter).with_target(false).with_ansi(terminal).init();
    let cli = Cli::parse();
    let cfg = config::Config::load(&cli.config)?;
    match cli.command {
        Command::Universe => {
            let (pairs, mismatched) = universe::discover(&cfg.collect).await?;
            println!("{:<14} {:<16} {:>6} {:>6} {:>7} {:>12} {:>12}", "pair", "aster", "id", "scale", "fee", "aster $24h", "lighter $24h");
            for p in &pairs {
                let fee = cfg.report.aster_taker_bps(&p.aster, &p.aster_subtypes);
                println!(
                    "{:<14} {:<16} {:>6} {:>6} {:>7} {:>12.0} {:>12.0}",
                    p.name, p.aster, p.lighter_id, p.scale, fee, p.aster_volume_usd, p.lighter_volume_usd
                );
            }
            println!("{} pairs; same name, different price (skipped): {mismatched:?}", pairs.len());
        }
        Command::Collect { out } => collect::run(cfg, out, stop_on_signals()).await?,
        Command::Report(args) => report::run(&cfg.report, &args)?,
    }
    Ok(())
}

fn stop_on_signals() -> CancellationToken {
    let stop = CancellationToken::new();
    let token = stop.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("stopping");
        token.cancel();
    });
    stop
}
