//! Pure `Decimal` helpers: bps<->rate conversion, tick/step rounding, parsing,
//! and Hyperliquid price quantization. Kept dependency-free and exhaustively
//! tested because every edge/PnL number flows through here.

use anyhow::{Context, Result};
use rust_decimal::Decimal;
use std::str::FromStr;

#[inline]
pub fn bps_to_rate(bps: Decimal) -> Decimal {
    bps / Decimal::from(10_000)
}

#[inline]
pub fn rate_to_bps(rate: Decimal) -> Decimal {
    rate * Decimal::from(10_000)
}

/// Largest multiple of `step` that is `<= value` (round toward negative infinity).
/// Returns `value` unchanged if `step <= 0`.
#[inline]
pub fn floor_to_step(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).floor() * step
}

/// Smallest multiple of `step` that is `>= value` (round toward positive infinity).
/// Returns `value` unchanged if `step <= 0`.
#[inline]
pub fn ceil_to_step(value: Decimal, step: Decimal) -> Decimal {
    if step <= Decimal::ZERO {
        return value;
    }
    (value / step).ceil() * step
}

/// Parse a decimal from an exchange string, trimming whitespace.
pub fn parse_dec(s: &str) -> Result<Decimal> {
    Decimal::from_str(s.trim()).with_context(|| format!("invalid decimal: {s:?}"))
}

/// Quantize a price to a valid Hyperliquid perp price: at most 5 significant
/// figures AND at most `6 - sz_decimals` decimal places (integers are always
/// valid). Not on the hedge path (hedging takes VWAP from the book), but used
/// where a representable HL price is needed and validated by tests.
pub fn round_hl_price(px: Decimal, sz_decimals: i32) -> Decimal {
    if px.is_zero() {
        return px;
    }
    let max_dp: i32 = (6 - sz_decimals).max(0);
    let abs = px.abs();
    let sig_dp: i32 = if abs >= Decimal::ONE {
        // Integer part consumes significant figures; >=5 integer digits => 0 dp
        // (and integer prices are always valid).
        (5 - integer_digits(abs)).max(0)
    } else {
        // For px < 1, significant figures start after the leading zeros.
        5 + leading_zeros_after_point(abs)
    };
    let dp = max_dp.min(sig_dp).max(0) as u32;
    px.round_dp(dp)
}

fn integer_digits(v: Decimal) -> i32 {
    let mut t = v.trunc();
    if t < Decimal::ONE {
        return 0;
    }
    let ten = Decimal::from(10);
    let mut d = 0;
    while t >= Decimal::ONE {
        t = (t / ten).floor();
        d += 1;
    }
    d
}

fn leading_zeros_after_point(v: Decimal) -> i32 {
    // Precondition: 0 < v < 1.
    let ten = Decimal::from(10);
    let tenth = Decimal::new(1, 1); // 0.1
    let mut x = v;
    let mut z = 0;
    while x < tenth {
        x *= ten;
        z += 1;
    }
    z
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn bps_roundtrip() {
        assert_eq!(bps_to_rate(dec!(4.5)), dec!(0.00045));
        assert_eq!(rate_to_bps(dec!(0.00045)), dec!(4.5));
        assert_eq!(rate_to_bps(bps_to_rate(dec!(3.0))), dec!(3.0));
    }

    #[test]
    fn floor_step() {
        assert_eq!(floor_to_step(dec!(100.45), dec!(0.1)), dec!(100.4));
        assert_eq!(floor_to_step(dec!(100.40), dec!(0.1)), dec!(100.4));
        assert_eq!(floor_to_step(dec!(0.00057), dec!(0.0001)), dec!(0.0005));
        // step <= 0 is a no-op
        assert_eq!(floor_to_step(dec!(1.23), dec!(0)), dec!(1.23));
    }

    #[test]
    fn ceil_step() {
        assert_eq!(ceil_to_step(dec!(100.41), dec!(0.1)), dec!(100.5));
        assert_eq!(ceil_to_step(dec!(100.40), dec!(0.1)), dec!(100.4));
        assert_eq!(ceil_to_step(dec!(0.00051), dec!(0.0001)), dec!(0.0006));
    }

    #[test]
    fn hl_price_rules() {
        // szDecimals=1 => max 5 dp; 5 sig figs.
        assert_eq!(round_hl_price(dec!(0.0123456), 1), dec!(0.01235));
        // 4 integer digits => 1 dp allowed by sig-fig rule.
        assert_eq!(round_hl_price(dec!(1234.567), 1), dec!(1234.6));
        // Integer price always valid even beyond 5 sig figs.
        assert_eq!(round_hl_price(dec!(123456), 2), dec!(123456));
        // Sub-1 price keeps 5 sig figs (leading zeros don't count).
        assert_eq!(round_hl_price(dec!(0.16234567), 0), dec!(0.16235));
    }

    #[test]
    fn parse_ok_and_err() {
        assert_eq!(parse_dec(" 12345.6 ").unwrap(), dec!(12345.6));
        assert!(parse_dec("not-a-number").is_err());
    }
}
