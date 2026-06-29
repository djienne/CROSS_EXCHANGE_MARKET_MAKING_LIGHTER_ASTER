//! # Live trading bot (`livebot`) — the real-execution four-plane architecture
//!
//! This module is the build-out described in `docs/UPDATE_PLAN.md`: a controlled migration
//! from the dry-run evaluator to a real Aster↔Hyperliquid cross-exchange maker/taker bot.
//! It is **entirely separate** from the deterministic dry-run core (`record`/`replay`/
//! `sim`/`live`) — those are untouched, so the canonical research pipeline and its soaks
//! keep running bit-identically.
//!
//! ## Safety model (read this first)
//!
//! Nothing here can move real funds unless ALL of the following hold:
//! 1. `[live] enabled = true` in the config, AND
//! 2. `mode = "live"` is selected explicitly, AND
//! 3. exactly one market is selected.
//!
//! `paper` (all pairs) runs the full order/fill/hedge/risk state machine against a simulated
//! executor ([`exec::PaperExec`]) — no network order I/O — while recording the market tape +
//! persisting results. Only `live` (single pair) wires the real signed venue workers. The
//! cryptographic signing is now implemented and live-verified (Aster ABI+EIP-191, HL EIP-712
//! with vault threading — both confirmed by a live place+cancel and golden-tested byte-for-byte;
//! see [`exec::crypto`] / [`exec::sign`]). So the safety boundary is the explicit opt-in chain
//! above (enabled + live + single market); the `probe` checks add per-call confirmation and a
//! `--max-usd` cap. An accidental `mode = "paper"` can never reach a signer (it uses
//! [`exec::PaperExec`]).
//!
//! ## The four planes (plan §1.1)
//!
//! - **Market-data hot path** — the existing `hotpath` ingest threads + [`VenueBook`] cells,
//!   now carrying a generation counter and an optional coalescing strategy wakeup.
//! - **Strategy/order hot path** — [`strategy`]: a single-owner loop that reprices, places/
//!   cancels/replaces Aster maker orders, and reacts to fills.
//! - **Execution hot path** — [`exec`]: concrete Aster + HL workers behind bounded command
//!   queues (no per-event dynamic dispatch).
//! - **Cold/control plane** — [`account`] reconciliation, [`risk`] gating, [`journal`]
//!   telemetry; the existing REST book-check and stream watchdog.
//!
//! [`VenueBook`]: crate::hotpath::VenueBook

pub mod account;
pub mod breaker;
pub mod exec;
pub mod fills;
pub mod ids;
pub mod journal;
pub mod orders;
pub mod pairs;
pub mod precheck;
pub mod probe;
pub mod reconcile;
pub mod risk;
pub mod run;
pub mod scale;
pub mod status;
pub mod strategy;
pub mod userstream;

pub use crate::config::{LiveCfg, LiveMode, PartialPolicy};
pub use run::run;
