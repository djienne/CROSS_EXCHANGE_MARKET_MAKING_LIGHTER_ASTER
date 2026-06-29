//! XEMM dry-run evaluator: simulate an Aster maker quote priced backward from a
//! Lighter taker hedge, then measure realized edge under latency, queue
//! position, and partial-fill accumulation. See `docs/DESIGN.md`.

pub mod book;
pub mod cli;
pub mod config;
pub mod connectors;
pub mod decimal;
pub mod edge;
pub mod events;
pub mod fill_sweep;
pub mod hedge;
pub mod hot_types;
/// Lock-free real-time substrate (latest-book cell, stream watchdog, execution
/// seam) — the foundation for a future live trading bot. Never on the deterministic
/// record/replay path. Compiled out under `--no-default-features`.
#[cfg(feature = "hotpath")]
pub mod hotpath;
pub mod inventory;
pub mod lighter;
/// The trading bot (the `livebot` command) — the four-plane execution architecture from
/// `docs/UPDATE_PLAN.md`, with two modes: `paper` (all pairs, dry-run — records the market
/// tape + persists results) and `live` (single pair, real money; hard-gated behind explicit
/// opt-in + a wired/testnet-verified signer). Requires `hotpath` (lock-free ingest
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
