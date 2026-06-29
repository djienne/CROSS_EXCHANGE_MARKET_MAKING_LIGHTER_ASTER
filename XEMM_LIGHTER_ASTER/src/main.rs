//! Binary entry point: initialize tracing, parse the CLI, and dispatch.

use anyhow::Result;
use clap::Parser;
use xemm_lighter_aster::cli::{dispatch, Cli};

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    let cli = Cli::parse();
    dispatch(cli).await
}
