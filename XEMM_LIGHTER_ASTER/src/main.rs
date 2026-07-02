//! Binary entry point: initialize tracing, parse the CLI, and dispatch.

use anyhow::Result;
use clap::Parser;
use xemm_lighter_aster::cli::{dispatch, Cli};

#[tokio::main]
async fn main() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    // Non-blocking writer: a log line from a hot thread (strategy/ingest/worker) must
    // never be a synchronous write(2) that can stall on a slow disk. The guard flushes
    // the buffer on drop, so shutdown logs survive. ANSI only on a real terminal (the
    // orchestrator redirects stdout to a file).
    let (writer, _guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout()))
        .with_writer(writer)
        .init();

    let cli = Cli::parse();
    dispatch(cli).await
}
