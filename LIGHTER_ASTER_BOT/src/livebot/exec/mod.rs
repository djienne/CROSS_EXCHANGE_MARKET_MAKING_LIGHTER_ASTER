//! Execution plane. Concrete venue workers behind bounded command
//! queues — NOT an `async` trait invoked per book event. The strategy `try_send`s small
//! commands; a worker owns the venue client and publishes lifecycle events back.
//!
//! - [`command`] — the `ExecCommand` / `HedgeCommand` / `ExecEvent` contract + queue depth.
//! - [`sign`] — signer traits + monotonic nonces + the real Aster signer.
//! - [`creds`] — `aster.env`/`lighter.env` loading + key-derived role resolution.
//! - [`crypto`] — golden-tested Aster EIP-712 signing primitives.
//! - [`aster`] / [`hyperliquid`] — the GATED live workers (real funds; signer-gated; the
//!   hedge module name is legacy, but its live I/O is Lighter).

pub mod aster;
pub mod command;
pub mod creds;
pub mod crypto;
pub mod hyperliquid;
pub mod sign;
