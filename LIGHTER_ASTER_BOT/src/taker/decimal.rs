use anyhow::{anyhow, bail, Context, Result};
use rust_decimal::Decimal;

pub fn parse_dec(s: &str) -> Result<Decimal> {
    s.parse::<Decimal>()
        .map_err(|e| anyhow!("invalid decimal {s:?}: {e}"))
}

pub fn trim_dec(d: Decimal) -> String {
    d.normalize().to_string()
}

pub fn common_qty_step(aster_step: Decimal, lighter_step: Decimal) -> Result<Decimal> {
    let (a_units, a_scale) = decimal_step_units(aster_step)?;
    let (l_units, l_scale) = decimal_step_units(lighter_step)?;
    let scale = a_scale.max(l_scale);
    let a = a_units
        .checked_mul(pow10_u128(scale - a_scale)?)
        .context("Aster quantity step scale overflow")?;
    let l = l_units
        .checked_mul(pow10_u128(scale - l_scale)?)
        .context("Lighter quantity step scale overflow")?;
    let common = lcm_u128(a, l).context("common quantity step overflow")?;
    let common_i128 = i128::try_from(common).context("common quantity step too large")?;
    Ok(Decimal::try_from_i128_with_scale(common_i128, scale).context("common quantity step exceeds Decimal precision")?.normalize())
}

fn decimal_step_units(step: Decimal) -> Result<(u128, u32)> {
    if step <= Decimal::ZERO {
        bail!("quantity step must be positive");
    }
    let normalized = step.normalize();
    let mantissa = normalized.mantissa().abs();
    if mantissa == 0 {
        bail!("quantity step must be positive");
    }
    let units = u128::try_from(mantissa).context("quantity step mantissa overflow")?;
    Ok((units, normalized.scale()))
}

fn pow10_u128(exp: u32) -> Result<u128> {
    let mut out = 1u128;
    for _ in 0..exp {
        out = out.checked_mul(10).context("decimal scale overflow")?;
    }
    Ok(out)
}

fn gcd_u128(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        let r = a % b;
        a = b;
        b = r;
    }
    a
}

fn lcm_u128(a: u128, b: u128) -> Option<u128> {
    let gcd = gcd_u128(a, b);
    a.checked_div(gcd)?.checked_mul(b)
}
