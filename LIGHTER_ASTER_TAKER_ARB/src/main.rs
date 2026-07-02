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
    // Non-blocking writer: a log line from the scan loop must never be a synchronous
    // write(2) that can stall on a slow disk. The guard flushes the buffer on drop, so
    // shutdown logs survive. ANSI only on a real terminal (the orchestrator redirects
    // stdout to a file).
    let (writer, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout()))
        .with_writer(writer)
        .init();

    dispatch(Cli::parse()).await
}
