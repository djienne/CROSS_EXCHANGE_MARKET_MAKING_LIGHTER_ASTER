//! Live Aster maker / Lighter hedge execution.
//!
//! Live submission requires enabled configuration, explicit live mode and one market.
//!
//! Book publication and strategy ownership stay in memory; bounded queues connect
//! venue workers, cold account reconciliation and persistent journal writing.
//! Aster uses EIP-712 requests; Lighter uses the native signer and transaction socket.
//! See `RUNBOOK.md` for uncertainty, shutdown and restart handling.

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
pub use run::{run, STRATEGY_THREAD};
