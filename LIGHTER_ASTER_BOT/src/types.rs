//! Small shared value types: `Side`, `MarketId`, `QueueModel`, and the
//! `RejectReason` enumeration (core reasons plus extra gates).

use serde::{Deserialize, Serialize};
use std::fmt;

/// Order side. For a maker quote this is the Aster side; the hedge is `opposite`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    #[inline]
    pub fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Logical market identifier (e.g. "BTC"). Maps to an Aster symbol + HL coin.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketId(pub String);

impl fmt::Display for MarketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Lighter transaction transport outcome classification. A `NotSent` result means no frame was
/// written and the caller may retry. `Unknown` means a signed frame may have reached Lighter, so
/// callers must freeze and reconcile instead of blindly resending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxSendStatus {
    Ok,
    Rejected,
    NotSent,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct TxSendResult {
    pub status: TxSendStatus,
    pub code: i64,
    pub message: String,
    pub quota_remaining: Option<i64>,
}

impl TxSendResult {
    pub fn not_sent(reason: impl Into<String>) -> Self {
        TxSendResult {
            status: TxSendStatus::NotSent,
            code: -1,
            message: reason.into(),
            quota_remaining: None,
        }
    }

    pub fn unknown(reason: impl Into<String>) -> Self {
        TxSendResult {
            status: TxSendStatus::Unknown,
            code: -1,
            message: reason.into(),
            quota_remaining: None,
        }
    }
}

impl From<&str> for MarketId {
    fn from(s: &str) -> Self {
        MarketId(s.to_string())
    }
}

impl From<String> for MarketId {
    fn from(s: String) -> Self {
        MarketId(s)
    }
}

/// Dense index into per-market arrays (dirty bitset, generation slots). Assigned
/// sequentially at registry construction; stable for the process lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MarketIdx(pub u16);

/// Queue-position assumption used when seeding a resting quote's "ahead" volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QueueModel {
    Optimistic,
    VisibleQueue,
    Conservative,
}

impl QueueModel {
    pub const ALL: [QueueModel; 3] = [
        QueueModel::Optimistic,
        QueueModel::VisibleQueue,
        QueueModel::Conservative,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            QueueModel::Optimistic => "optimistic",
            QueueModel::VisibleQueue => "visible_queue",
            QueueModel::Conservative => "conservative",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "optimistic" => Some(QueueModel::Optimistic),
            "visible_queue" | "visible" => Some(QueueModel::VisibleQueue),
            "conservative" => Some(QueueModel::Conservative),
            _ => None,
        }
    }
}

impl fmt::Display for QueueModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why a candidate quote (or a fill's hedge) was not acted upon. Persisted on
/// rejected opportunities so the reject distribution is inspectable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    // --- core reject reasons ---
    NoProfitableAsterBid,
    NoProfitableAsterAsk,
    EdgeBelowMinAfterRounding,
    QuoteTooFarFromTouch,
    QuoteTooCloseToTouch,
    HlHedgeVwapUnavailable,
    HlHedgeSlippageTooHigh,
    AsterPostOnlyPriceInvalid,
    AsterQuoteStalePendingCancel,
    FillBelowHlMinHedge,
    PartialFillAccumulated,
    PendingInventoryTooOld,
    PendingInventoryTooLarge,
    StrictPartialHedgeabilityFailed,
    // --- added gates ---
    AsterBookStale,
    HlBookStale,
    BookCrossed,
    MissingAsterBook,
    MissingHlBook,
    MissingMid,
    InsufficientLighterDepth,
    /// Lighter BBO was fresh but too small for the hedge, and the slower L2
    /// depth snapshot was stale/missing, so neither quote source is safe.
    HlBboThinAndL2Stale,
    /// Aster BBO top was too thin for the candidate maker order, and fresh depth20
    /// could not provide an effective same-side touch for the order size.
    AsterEffectiveTouchUnavailable,
    QuantityBelowMinimum,
    // --- capital / position cap ---
    /// Increasing the Aster futures position further would exceed its capital cap.
    AsterPositionCapReached,
    /// Increasing the Lighter hedge position would exceed its capital cap.
    LighterPositionCapReached,
    /// Live inventory-unwind mode is enabled and this candidate would not reduce the paired
    /// Aster/Lighter absolute position.
    PositionReduceOnly,
    /// A post-only (GTX) quote would have crossed the book by the time placement
    /// latency elapsed; Aster would have rejected it.
    PostOnlyRejectedOnPlacement,
    /// `clamp_to_min_lot` is on but the venue minimum lot exceeds the remaining
    /// capital headroom (or won't clear min-notional at the quoted price), so even
    /// the smallest valid order cannot be placed.
    MinLotExceedsHeadroom,
}

impl RejectReason {
    pub fn as_str(self) -> &'static str {
        use RejectReason::*;
        match self {
            NoProfitableAsterBid => "NO_PROFITABLE_ASTER_BID",
            NoProfitableAsterAsk => "NO_PROFITABLE_ASTER_ASK",
            EdgeBelowMinAfterRounding => "EDGE_BELOW_MIN_AFTER_ROUNDING",
            QuoteTooFarFromTouch => "QUOTE_TOO_FAR_FROM_TOUCH",
            QuoteTooCloseToTouch => "QUOTE_TOO_CLOSE_TO_TOUCH",
            HlHedgeVwapUnavailable => "HL_HEDGE_VWAP_UNAVAILABLE",
            HlHedgeSlippageTooHigh => "HL_HEDGE_SLIPPAGE_TOO_HIGH",
            AsterPostOnlyPriceInvalid => "ASTER_POST_ONLY_PRICE_INVALID",
            AsterQuoteStalePendingCancel => "ASTER_QUOTE_STALE_PENDING_CANCEL",
            FillBelowHlMinHedge => "FILL_BELOW_HL_MIN_HEDGE",
            PartialFillAccumulated => "PARTIAL_FILL_ACCUMULATED",
            PendingInventoryTooOld => "PENDING_INVENTORY_TOO_OLD",
            PendingInventoryTooLarge => "PENDING_INVENTORY_TOO_LARGE",
            StrictPartialHedgeabilityFailed => "STRICT_PARTIAL_HEDGEABILITY_FAILED",
            AsterBookStale => "ASTER_BOOK_STALE",
            HlBookStale => "HL_BOOK_STALE",
            BookCrossed => "BOOK_CROSSED",
            MissingAsterBook => "MISSING_ASTER_BOOK",
            MissingHlBook => "MISSING_HL_BOOK",
            MissingMid => "MISSING_MID",
            InsufficientLighterDepth => "INSUFFICIENT_LIGHTER_DEPTH",
            HlBboThinAndL2Stale => "HL_BBO_THIN_AND_L2_STALE",
            AsterEffectiveTouchUnavailable => "ASTER_EFFECTIVE_TOUCH_UNAVAILABLE",
            QuantityBelowMinimum => "QUANTITY_BELOW_MINIMUM",
            AsterPositionCapReached => "ASTER_POSITION_CAP_REACHED",
            LighterPositionCapReached => "LIGHTER_POSITION_CAP_REACHED",
            PositionReduceOnly => "POSITION_REDUCE_ONLY",
            PostOnlyRejectedOnPlacement => "POST_ONLY_REJECTED_ON_PLACEMENT",
            MinLotExceedsHeadroom => "MIN_LOT_EXCEEDS_HEADROOM",
        }
    }
}

impl fmt::Display for RejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn side_opposite() {
        assert_eq!(Side::Buy.opposite(), Side::Sell);
        assert_eq!(Side::Sell.opposite(), Side::Buy);
    }

    #[test]
    fn queue_model_roundtrip() {
        for m in QueueModel::ALL {
            assert_eq!(QueueModel::parse(m.as_str()), Some(m));
        }
        assert_eq!(QueueModel::parse("nope"), None);
    }
}
