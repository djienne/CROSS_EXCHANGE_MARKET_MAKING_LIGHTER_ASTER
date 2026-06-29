use serde::{Deserialize, Serialize};
use std::fmt;

use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }

    pub fn opposite(self) -> Self {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketId(pub String);

impl fmt::Display for MarketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
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

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct FillSummary {
    pub qty: Decimal,
    pub vwap: Decimal,
    pub notional: Decimal,
    pub fee_usd: Decimal,
}

impl FillSummary {
    pub fn from_qty_notional(qty: Decimal, notional: Decimal, fee_usd: Decimal) -> Option<Self> {
        if qty <= Decimal::ZERO || notional <= Decimal::ZERO {
            return None;
        }
        Some(Self {
            qty,
            vwap: notional / qty,
            notional,
            fee_usd,
        })
    }
}
