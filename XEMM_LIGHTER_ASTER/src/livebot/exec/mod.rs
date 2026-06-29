//! Execution plane (plan §1.1.C / §3 / §4). Concrete venue workers behind bounded command
//! queues — NOT an `async` trait invoked per book event. The strategy `try_send`s small
//! commands; a worker owns the venue client and publishes lifecycle events back.
//!
//! - [`command`] — the `ExecCommand` / `HedgeCommand` / `ExecEvent` contract + queue depth.
//! - [`paper`] — [`PaperExec`], the fully-functional simulated executor (`mode = "paper"`).
//! - [`sign`] — signer traits + monotonic nonces + the real Aster signer.
//! - [`creds`] — `aster.env`/`lighter.env` loading + key-derived role resolution.
//! - [`crypto`] — golden-tested signing primitives and legacy helper coverage.
//! - [`aster`] / [`hyperliquid`] — the GATED live workers (real funds; signer-gated; the
//!   hedge module name is legacy, but its live I/O is Lighter).
//!
//! `ExecMode` is chosen ONCE at startup, so there is no per-event dynamic dispatch.

pub mod aster;
pub mod command;
pub mod creds;
pub mod crypto;
pub mod hyperliquid;
pub mod paper;
pub mod sign;

pub use command::{ExecCommand, ExecEvent, HedgeCommand, CMD_QUEUE_DEPTH};
pub use creds::AsterCreds;
pub use paper::PaperExec;
pub use sign::{
    AsterNonce, AsterSigner, EvmAsterSigner, EvmHlSigner, HlNonce, HlSigner, MonotonicMs, SignError,
};

/// Which executor backs this run. Selected once at startup from [`crate::config::LiveMode`];
/// the strategy and reactors are identical across modes — only the worker differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecMode {
    /// Simulated executor; no network order I/O ([`crate::config::LiveMode::Paper`]).
    Paper,
    /// Real signed venue workers ([`crate::config::LiveMode::Live`]) — gated at the signer.
    Live,
}

impl ExecMode {
    pub fn from_cfg(mode: crate::config::LiveMode) -> Self {
        match mode {
            crate::config::LiveMode::Paper => ExecMode::Paper,
            crate::config::LiveMode::Live => ExecMode::Live,
        }
    }
    /// Whether this mode ever sends a real order to a venue.
    pub fn sends_real_orders(self) -> bool {
        matches!(self, ExecMode::Live)
    }
}
