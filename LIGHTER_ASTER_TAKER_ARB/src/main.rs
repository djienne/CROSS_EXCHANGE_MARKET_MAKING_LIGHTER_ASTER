mod arb;
mod aster;
mod book;
mod book_sanity;
mod cli;
mod config;
mod connectors;
mod decimal;
mod entry_gate;
mod lighter;
mod markets;
mod pnl;
mod status;
mod types;
mod venues;

use anyhow::Result;
use clap::Parser;
use cli::{dispatch, Cli};

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    dispatch(Cli::parse()).await
}
