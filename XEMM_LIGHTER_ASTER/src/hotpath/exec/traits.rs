//! The live execution boundary: place / cancel / replace the Aster maker quote and
//! fire the Hyperliquid market hedge. This is a **seam, not an implementation** — it
//! exists so a future real low-latency bot has a typed interface to build against.
//!
//! IMPORTANT: the deterministic `SimEngine` does NOT implement or call this trait.
//! Dry-run evaluation never touches it; the simulator stays the canonical model of
//! what this exec path *would* do.

use rust_decimal::Decimal;

use crate::types::{MarketId, Side};

/// A maker order to rest on Aster.
#[derive(Debug, Clone)]
pub struct MakerOrder {
    pub market: MarketId,
    pub side: Side,
    pub price: Decimal,
    pub qty: Decimal,
    /// Post-only (GTX): the venue rejects it rather than crossing.
    pub post_only: bool,
    pub client_id: uuid::Uuid,
}

/// A handle to a live order (client id + venue-assigned id once known).
#[derive(Debug, Clone)]
pub struct OrderHandle {
    pub client_id: uuid::Uuid,
    pub venue_order_id: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ExecError {
    #[error("execution not implemented (dry-run seam)")]
    NotImplemented,
    #[error("order rejected: {0}")]
    Rejected(String),
    #[error("transport error: {0}")]
    Transport(String),
}

/// The maker-quoting + taker-hedging interface a real bot implements. Async because
/// real venue I/O is async; the dry-run path never invokes it.
#[async_trait::async_trait]
pub trait Execution: Send + Sync {
    /// Rest a post-only maker quote on Aster.
    async fn place_maker(&self, order: MakerOrder) -> Result<OrderHandle, ExecError>;
    /// Cancel a resting maker quote.
    async fn cancel_maker(&self, handle: &OrderHandle) -> Result<(), ExecError>;
    /// Cancel+replace as one logical op (venues often expose an atomic amend).
    async fn replace_maker(
        &self,
        handle: &OrderHandle,
        new: MakerOrder,
    ) -> Result<OrderHandle, ExecError>;
    /// Immediate market (IOC) hedge on Hyperliquid — the taker leg.
    async fn market_hedge(
        &self,
        market: &MarketId,
        side: Side,
        qty: Decimal,
    ) -> Result<OrderHandle, ExecError>;
}
