use anyhow::{anyhow, Result};
use rust_decimal::Decimal;

pub fn parse_dec(s: &str) -> Result<Decimal> {
    s.parse::<Decimal>()
        .map_err(|e| anyhow!("invalid decimal {s:?}: {e}"))
}

pub fn bps_to_rate(bps: Decimal) -> Decimal {
    bps / Decimal::from(10_000)
}

pub fn rate_to_bps(rate: Decimal) -> Decimal {
    rate * Decimal::from(10_000)
}

pub fn trim_dec(d: Decimal) -> String {
    d.normalize().to_string()
}
