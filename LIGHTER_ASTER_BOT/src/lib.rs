//! Aster/Lighter bot and its research core. `run` (`controller`) trades one market with the
//! taker–taker engine (`taker`) and the XEMM engine (`livebot`: quotes on Aster, hedges
//! fills on Lighter), switching execution rights between them in memory.
//! `record`/`replay`/`report` simulate the XEMM quotes offline to measure realized edge under
//! latency, queue position and partial fills.

pub mod book;
pub mod cli;
pub mod config;
/// The `run` controller: both engines in one process, execution rights handed over in memory.
#[cfg(feature = "hotpath")]
pub mod controller;
pub mod connectors;
pub mod decimal;
pub mod edge;
pub mod events;
pub mod fill_sweep;
pub mod hedge;
pub mod hot_types;
/// Lock-free real-time substrate (latest-book cell, stream watchdog, execution
/// seam) used by the `livebot`. Never on the deterministic record/replay path.
/// Compiled out under `--no-default-features`.
#[cfg(feature = "hotpath")]
pub mod hotpath;
pub mod inventory;
pub mod lighter;
/// The XEMM engine (the `livebot` command, and `run`'s maker), with two modes: `paper` (the
/// selected markets, simulated executor, NO real orders; `livebot` also records the market
/// tape + persists results) and `live` (one market, real money; hard-gated behind `[live]
/// enabled`, `--mode live` and the real signers). Requires `hotpath` (lock-free ingest
/// substrate). The deterministic research core (`record`/`replay`/`report`) does not.
#[cfg(feature = "hotpath")]
pub mod livebot;
pub mod markets;
pub mod metrics;
pub mod position;
pub mod quote_engine;
pub mod record;
pub mod replay;
pub mod live_report;
pub mod report;
pub mod requoter;
pub mod sim;
pub mod store;
/// Taker–taker arbitrage engine (`lighter_aster_bot taker ...`). Needs the live signing
/// stack, so it is compiled out under `--no-default-features`.
#[cfg(feature = "hotpath")]
pub mod taker;
pub mod types;
/// One-shot `verify-books` diagnostic: confirm the websocket-built books match REST
/// snapshots. Uses only the deterministic-core connectors, so it is not feature-gated.
pub mod verify;
/// `verify-db` diagnostic: audit a results SQLite database for internal consistency
/// (orphaned/miscounted rows) — the integrity check that replaces FK enforcement.
pub mod verify_db;
pub mod vwap;

/// Crate-wide result alias.
pub type Result<T> = anyhow::Result<T>;
