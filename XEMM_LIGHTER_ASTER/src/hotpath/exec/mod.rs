//! Execution seam for a future live trading bot. See `traits` for the contract and
//! `live_stub` for the (deliberately unimplemented) live placeholder. The
//! deterministic simulator is never routed through this.

pub mod live_stub;
pub mod traits;

pub use live_stub::LiveExecution;
pub use traits::{ExecError, Execution, MakerOrder, OrderHandle};
