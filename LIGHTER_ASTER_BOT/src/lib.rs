//! Aster/Lighter bot. `run` (`controller`) trades one market with the
//! taker–taker engine (`taker`) and the XEMM engine (`livebot`: quotes on Aster, hedges
//! fills on Lighter), switching execution rights between them in memory.

pub mod book;
pub mod cli;
pub mod config;
/// The `run` controller: both engines in one process, execution rights handed over in memory.
pub mod controller;
pub mod connectors;
pub mod decimal;
pub mod edge;
pub mod hot_types;
/// Lock-free real-time substrate (latest-book cell, stream watchdog, execution
/// seam) used by the `livebot`.
pub mod hotpath;
pub mod inventory;
pub mod lighter;
/// The XEMM engine (`run`'s maker): one market, real money; hard-gated behind `[live]
/// enabled`, `--mode live` and the real signers.
pub mod livebot;
pub mod markets;
pub mod metrics;
pub mod position;
pub mod quote_engine;
pub mod live_report;
/// Taker–taker arbitrage engine (`lighter_aster_bot taker ...`).
pub mod taker;
pub mod types;
pub mod vwap;

/// Crate-wide result alias.
pub type Result<T> = anyhow::Result<T>;
