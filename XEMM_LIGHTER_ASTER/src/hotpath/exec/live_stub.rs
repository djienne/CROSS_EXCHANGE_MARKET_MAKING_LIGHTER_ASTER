//! LIVE EXECUTION — NOT IMPLEMENTED. A placeholder so the [`Execution`] seam
//! type-checks and the future real bot has one obvious place to wire Aster/HL order
//! calls. Every method is a hard `unimplemented!()` so this can NEVER silently
//! "trade" if accidentally invoked from the dry-run path.

use rust_decimal::Decimal;

use crate::types::{MarketId, Side};

use super::traits::{ExecError, Execution, MakerOrder, OrderHandle};

/// Stub live executor. Fields a real implementation would hold are listed as TODOs.
#[derive(Default)]
pub struct LiveExecution {
    // TODO(live-bot): aster signed-REST client, hyperliquid client + signer, API
    // keys / wallet, in-flight order map, rate limiters.
}

impl LiveExecution {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl Execution for LiveExecution {
    async fn place_maker(&self, _order: MakerOrder) -> Result<OrderHandle, ExecError> {
        unimplemented!("LIVE place_maker: wire the current Aster V3 signed order endpoint (timeInForce=GTX); verify against live docs — V1 key issuance is deprecated from 2026-03-25")
    }
    async fn cancel_maker(&self, _handle: &OrderHandle) -> Result<(), ExecError> {
        unimplemented!("LIVE cancel_maker: wire the current Aster V3 signed cancel endpoint; verify against live docs (V1 deprecated 2026-03-25)")
    }
    async fn replace_maker(
        &self,
        _handle: &OrderHandle,
        _new: MakerOrder,
    ) -> Result<OrderHandle, ExecError> {
        unimplemented!("LIVE replace_maker: wire Aster amend, or cancel+place, here")
    }
    async fn market_hedge(
        &self,
        _market: &MarketId,
        _side: Side,
        _qty: Decimal,
    ) -> Result<OrderHandle, ExecError> {
        unimplemented!("LIVE market_hedge: wire Hyperliquid market (IOC) order here")
    }
}
