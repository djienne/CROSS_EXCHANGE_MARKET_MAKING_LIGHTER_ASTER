//! Scaled-integer market scale + the live hot-path order book builder.
//!
//! The deterministic cold path uses `rust_decimal::Decimal` everywhere (exact, but
//! heap-ish and slow). The live quote loop instead works in **scaled integers**:
//! prices as integer multiples of the venue tick (`px_ticks`) and quantities as
//! integer multiples of the venue lot/step (`qty_lots`). Comparisons, requote-threshold
//! checks, and crossed/touch tests are then branch-light `i64` math with no allocation
//! (plan §1.1 / §5.1).
//!
//! `Decimal` is kept for config, edge/PnL math, and cold reconciliation — we convert at
//! the boundary when building a [`MarketScale`] from a [`MarketSpec`] and when emitting an
//! order. We deliberately do NOT reimplement the edge/VWAP stack in integer math: that is
//! the exact, well-tested money math, and re-deriving it in `i64` for a few microseconds
//! would be a real-funds correctness hazard. The integers carry the *hot, hot* part
//! (touch/crossed/staleness/price-move detection + order representation); the proven
//! `Decimal` quote engine prices the actual quote (plan §5.3 "reuse pure calculation
//! code where appropriate").

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::book::OrderBook;
use crate::markets::MarketSpec;

// Re-export so existing `use crate::livebot::scale::*` imports keep working.
pub use crate::hot_types::{HotBook, HotLevel, HOT_LEVELS};

/// Per-market conversion between exact `Decimal` prices/quantities and the scaled `i64`
/// ticks/lots the hot path uses. Built once at startup from the resolved [`MarketSpec`].
#[derive(Debug, Clone)]
pub struct MarketScale {
    pub tick: Decimal,
    pub step: Decimal,
    /// Hyperliquid hedge-leg size step (from szDecimals) — hedges round to this.
    pub hl_qty_step: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HotQtyScale {
    Aster,
    Hyperliquid,
}

impl MarketScale {
    pub fn from_spec(spec: &MarketSpec) -> Self {
        MarketScale {
            tick: spec.tick,
            step: spec.step,
            hl_qty_step: spec.hl_qty_step,
        }
    }

    /// Quantize a price to an integer number of ticks (nearest tick). Saturates to 0 on a
    /// non-representable / non-positive input (a garbage price never becomes a live order).
    #[inline]
    pub fn price_to_ticks(&self, px: Decimal) -> i64 {
        if self.tick <= Decimal::ZERO || px <= Decimal::ZERO {
            return 0;
        }
        (px / self.tick).round().to_i64().unwrap_or(0)
    }

    /// Round a price DOWN to a whole tick (post-only bid pricing).
    #[inline]
    pub fn price_floor_ticks(&self, px: Decimal) -> i64 {
        if self.tick <= Decimal::ZERO || px <= Decimal::ZERO {
            return 0;
        }
        (px / self.tick).floor().to_i64().unwrap_or(0)
    }

    /// Round a price UP to a whole tick (post-only ask pricing).
    #[inline]
    pub fn price_ceil_ticks(&self, px: Decimal) -> i64 {
        if self.tick <= Decimal::ZERO || px <= Decimal::ZERO {
            return 0;
        }
        (px / self.tick).ceil().to_i64().unwrap_or(0)
    }

    /// Exact `Decimal` price for a tick count.
    #[inline]
    pub fn ticks_to_price(&self, ticks: i64) -> Decimal {
        Decimal::from(ticks) * self.tick
    }

    /// Quantize a quantity DOWN to whole lots (you never round a size up into more risk).
    #[inline]
    pub fn qty_to_lots(&self, qty: Decimal) -> i64 {
        if self.step <= Decimal::ZERO || qty <= Decimal::ZERO {
            return 0;
        }
        (qty / self.step).floor().to_i64().unwrap_or(0)
    }

    /// Quantize a Hyperliquid quantity DOWN to whole HL lots.
    #[inline]
    pub fn hl_qty_to_lots(&self, qty: Decimal) -> i64 {
        if self.hl_qty_step <= Decimal::ZERO || qty <= Decimal::ZERO {
            return 0;
        }
        (qty / self.hl_qty_step).floor().to_i64().unwrap_or(0)
    }

    /// Quantize a Hyperliquid quantity UP to whole HL lots. Used for minimum
    /// visible-depth requirements so the hot path never under-requires liquidity.
    #[inline]
    pub fn hl_qty_to_lots_ceil(&self, qty: Decimal) -> i64 {
        if self.hl_qty_step <= Decimal::ZERO || qty <= Decimal::ZERO {
            return 0;
        }
        (qty / self.hl_qty_step).ceil().to_i64().unwrap_or(0)
    }

    /// Parse an exchange decimal price string directly into integer ticks, without
    /// constructing a `Decimal` on the websocket hot path. Prices are rounded to the
    /// nearest tick to mirror [`price_to_ticks`].
    #[inline]
    pub fn price_str_to_ticks(&self, px: &str) -> Option<i64> {
        decimal_str_to_units(px, self.tick, UnitRound::Nearest)
    }

    /// Parse an exchange decimal quantity string directly into integer lots, without
    /// constructing a `Decimal` on the websocket hot path. Quantities are rounded down
    /// to mirror [`qty_to_lots`] and avoid overstating visible size.
    #[inline]
    pub fn qty_str_to_lots(&self, qty: &str) -> Option<i64> {
        decimal_str_to_units(qty, self.step, UnitRound::Floor)
    }

    /// Parse a Hyperliquid quantity string directly into HL lots.
    #[inline]
    pub fn hl_qty_str_to_lots(&self, qty: &str) -> Option<i64> {
        decimal_str_to_units(qty, self.hl_qty_step, UnitRound::Floor)
    }

    /// Exact `Decimal` quantity for a lot count.
    #[inline]
    pub fn lots_to_qty(&self, lots: i64) -> Decimal {
        Decimal::from(lots) * self.step
    }

    /// Exact Hyperliquid quantity for an HL lot count.
    #[inline]
    pub fn hl_lots_to_qty(&self, lots: i64) -> Decimal {
        Decimal::from(lots) * self.hl_qty_step
    }
}

#[derive(Debug, Clone, Copy)]
enum UnitRound {
    Floor,
    Nearest,
}

#[inline]
fn decimal_str_to_units(s: &str, unit: Decimal, round: UnitRound) -> Option<i64> {
    if unit <= Decimal::ZERO {
        return None;
    }
    let (mant, scale) = parse_positive_decimal(s)?;
    let unit_mant = unit.mantissa().abs();
    if unit_mant <= 0 {
        return None;
    }
    let numerator = mant.checked_mul(pow10(unit.scale())?)?;
    let denominator = unit_mant.checked_mul(pow10(scale)?)?;
    if denominator <= 0 {
        return None;
    }
    let units = match round {
        UnitRound::Floor => numerator / denominator,
        UnitRound::Nearest => (numerator + denominator / 2) / denominator,
    };
    i64::try_from(units).ok().filter(|v| *v > 0)
}

fn parse_positive_decimal(s: &str) -> Option<(i128, u32)> {
    let s = s.trim();
    if s.is_empty() || s.starts_with('-') {
        return None;
    }
    let s = s.strip_prefix('+').unwrap_or(s);
    let mut mant = 0i128;
    let mut scale = 0u32;
    let mut saw_digit = false;
    let mut saw_dot = false;
    for b in s.bytes() {
        match b {
            b'0'..=b'9' => {
                saw_digit = true;
                mant = mant.checked_mul(10)?.checked_add((b - b'0') as i128)?;
                if saw_dot {
                    scale = scale.checked_add(1)?;
                }
            }
            b'.' if !saw_dot => saw_dot = true,
            _ => return None,
        }
    }
    if !saw_digit || mant <= 0 {
        return None;
    }
    Some((mant, scale))
}

#[inline]
fn pow10(exp: u32) -> Option<i128> {
    let mut v = 1i128;
    for _ in 0..exp {
        v = v.checked_mul(10)?;
    }
    Some(v)
}

/// Convert a `Decimal` [`OrderBook`] into a scaled-integer [`HotBook`], truncating to
/// [`HOT_LEVELS`] and dropping any level whose scaled price or qty rounds to <= 0.
pub fn build_hot_book(book: &OrderBook, scale: &MarketScale, generation: u64, recv_ns: i64) -> HotBook {
    build_hot_book_with_qty_scale(book, scale, HotQtyScale::Aster, generation, recv_ns)
}

pub fn build_hot_book_with_qty_scale(
    book: &OrderBook,
    scale: &MarketScale,
    qty_scale: HotQtyScale,
    generation: u64,
    recv_ns: i64,
) -> HotBook {
    let mut bids = [HotLevel::default(); HOT_LEVELS];
    let mut asks = [HotLevel::default(); HOT_LEVELS];
    let bid_len = fill(&mut bids, &book.bids, scale, qty_scale);
    let ask_len = fill(&mut asks, &book.asks, scale, qty_scale);
    HotBook::new(
        bids,
        asks,
        bid_len,
        ask_len,
        generation,
        recv_ns,
        book.exch_ts.timestamp_millis(),
    )
}

/// Build a [`HotBook`] directly from exchange decimal strings. This is the websocket
/// hot-path builder: it avoids `rust_decimal::Decimal` allocation/construction for the
/// integer precheck representation while preserving canonical ordering, duplicate-price
/// aggregation, non-positive filtering, and [`HOT_LEVELS`] truncation.
pub fn build_hot_book_from_strs<'a, I, J>(
    bids_in: I,
    asks_in: J,
    scale: &MarketScale,
    generation: u64,
    recv_ns: i64,
    exch_ms: i64,
) -> HotBook
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
    J: IntoIterator<Item = (&'a str, &'a str)>,
{
    build_hot_book_from_strs_with_qty_scale(
        bids_in,
        asks_in,
        scale,
        HotQtyScale::Aster,
        generation,
        recv_ns,
        exch_ms,
    )
}

pub fn build_hot_book_from_strs_with_qty_scale<'a, I, J>(
    bids_in: I,
    asks_in: J,
    scale: &MarketScale,
    qty_scale: HotQtyScale,
    generation: u64,
    recv_ns: i64,
    exch_ms: i64,
) -> HotBook
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
    J: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut bids = [HotLevel::default(); HOT_LEVELS];
    let mut asks = [HotLevel::default(); HOT_LEVELS];
    let bid_len = fill_raw(&mut bids, bids_in, scale, qty_scale, true);
    let ask_len = fill_raw(&mut asks, asks_in, scale, qty_scale, false);
    HotBook::new(bids, asks, bid_len, ask_len, generation, recv_ns, exch_ms)
}

/// Fill a fixed level array from `Decimal` levels (already canonically sorted by
/// `OrderBook::from_levels`). Returns the count written.
fn fill(out: &mut [HotLevel; HOT_LEVELS], levels: &[crate::book::Level], scale: &MarketScale, qty_scale: HotQtyScale) -> u8 {
    let mut n = 0usize;
    for lvl in levels {
        if n >= HOT_LEVELS {
            break;
        }
        let px_ticks = scale.price_to_ticks(lvl.px);
        let qty_lots = match qty_scale {
            HotQtyScale::Aster => scale.qty_to_lots(lvl.qty),
            HotQtyScale::Hyperliquid => scale.hl_qty_to_lots(lvl.qty),
        };
        if px_ticks <= 0 || qty_lots <= 0 {
            continue;
        }
        out[n] = HotLevel { px_ticks, qty_lots };
        n += 1;
    }
    n as u8
}

fn fill_raw<'a, I>(
    out: &mut [HotLevel; HOT_LEVELS],
    levels: I,
    scale: &MarketScale,
    qty_scale: HotQtyScale,
    descending: bool,
) -> u8
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut n = 0usize;
    for (px_s, qty_s) in levels {
        let Some(px_ticks) = scale.price_str_to_ticks(px_s) else { continue };
        let qty_lots = match qty_scale {
            HotQtyScale::Aster => scale.qty_str_to_lots(qty_s),
            HotQtyScale::Hyperliquid => scale.hl_qty_str_to_lots(qty_s),
        };
        let Some(qty_lots) = qty_lots else { continue };
        if px_ticks <= 0 || qty_lots <= 0 {
            continue;
        }
        upsert_hot_sorted(out, &mut n, HotLevel { px_ticks, qty_lots }, descending);
    }
    n as u8
}

fn upsert_hot_sorted(out: &mut [HotLevel; HOT_LEVELS], len: &mut usize, level: HotLevel, descending: bool) {
    for cur in out.iter_mut().take(*len) {
        if cur.px_ticks == level.px_ticks {
            cur.qty_lots = cur.qty_lots.saturating_add(level.qty_lots);
            return;
        }
    }

    let pos = (0..*len)
        .find(|&i| if descending { level.px_ticks > out[i].px_ticks } else { level.px_ticks < out[i].px_ticks })
        .unwrap_or(*len);

    if *len < HOT_LEVELS {
        for i in (pos..*len).rev() {
            out[i + 1] = out[i];
        }
        out[pos] = level;
        *len += 1;
    } else if pos < HOT_LEVELS {
        for i in (pos..HOT_LEVELS - 1).rev() {
            out[i + 1] = out[i];
        }
        out[pos] = level;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal_macros::dec;

    fn spec() -> MarketSpec {
        MarketSpec {
            market_id: "BTC".into(),
            aster_symbol: "BTCUSDT".into(),
            hl_coin: "BTC".into(),
            lighter_market_id: 1,
            lighter_price_decimals: 1,
            lighter_size_decimals: 3,
            lighter_price_tick: dec!(0.1),
            tick: dec!(0.1),
            step: dec!(0.001),
            aster_min_qty: dec!(0.001),
            aster_min_notional: dec!(5),
            hl_sz_decimals: 3,
            hl_qty_step: dec!(0.001),
            hl_min_notional: dec!(10),
        }
    }

    #[test]
    fn price_qty_round_trip() {
        let s = MarketScale::from_spec(&spec());
        assert_eq!(s.price_to_ticks(dec!(100.0)), 1000);
        assert_eq!(s.ticks_to_price(1000), dec!(100.0));
        assert_eq!(s.price_to_ticks(dec!(100.04)), 1000);
        assert_eq!(s.price_to_ticks(dec!(100.06)), 1001);
        assert_eq!(s.price_floor_ticks(dec!(100.06)), 1000);
        assert_eq!(s.price_ceil_ticks(dec!(100.04)), 1001);
        assert_eq!(s.qty_to_lots(dec!(0.0059)), 5);
        assert_eq!(s.lots_to_qty(5), dec!(0.005));
        assert_eq!(s.price_to_ticks(dec!(0)), 0);
        assert_eq!(s.qty_to_lots(dec!(-1)), 0);
    }

    fn book() -> OrderBook {
        let now = Utc::now();
        OrderBook::from_levels(
            vec![(dec!(100.0), dec!(2)), (dec!(99.9), dec!(5))],
            vec![(dec!(100.1), dec!(3)), (dec!(100.2), dec!(4))],
            now,
            now,
        )
    }

    #[test]
    fn parses_exchange_strings_to_scaled_units_without_decimal() {
        let s = MarketScale::from_spec(&spec());
        assert_eq!(s.price_str_to_ticks("100.04"), Some(1000));
        assert_eq!(s.price_str_to_ticks("100.06"), Some(1001));
        assert_eq!(s.qty_str_to_lots("0.0059"), Some(5));
        assert_eq!(s.hl_qty_str_to_lots("0.0059"), Some(5));
        assert_eq!(s.price_str_to_ticks("0"), None);
        assert_eq!(s.qty_str_to_lots("bad"), None);
    }

    #[test]
    fn hyperliquid_hot_quantities_use_hl_step() {
        let mut spec = spec();
        spec.step = dec!(0.01);
        spec.hl_qty_step = dec!(0.001);
        let s = MarketScale::from_spec(&spec);

        let aster = build_hot_book_from_strs_with_qty_scale(
            [("100.0", "0.019")],
            [("100.1", "0.019")],
            &s,
            HotQtyScale::Aster,
            1,
            100,
            1_700_000_000_000,
        );
        let hl = build_hot_book_from_strs_with_qty_scale(
            [("100.0", "0.019")],
            [("100.1", "0.019")],
            &s,
            HotQtyScale::Hyperliquid,
            1,
            100,
            1_700_000_000_000,
        );

        assert_eq!(aster.bids()[0].qty_lots, 1);
        assert_eq!(hl.bids()[0].qty_lots, 19);
        assert_eq!(s.hl_qty_to_lots(dec!(0.0199)), 19);
        assert_eq!(s.hl_qty_to_lots_ceil(dec!(0.0191)), 20);
        assert_eq!(s.hl_lots_to_qty(19), dec!(0.019));
    }

    #[test]
    fn hot_book_from_strings_sorts_aggregates_and_truncates() {
        let s = MarketScale::from_spec(&spec());
        let hb = build_hot_book_from_strs(
            [("99.9", "1"), ("100.0", "2"), ("100.0", "3")],
            [("100.2", "1"), ("100.1", "4")],
            &s,
            7,
            123,
            1_700_000_000_000,
        );
        assert_eq!(hb.generation, 7);
        assert_eq!(hb.exch_ms, 1_700_000_000_000);
        assert_eq!(hb.best_bid_ticks(), Some(1000));
        assert_eq!(hb.bids()[0].qty_lots, 5000);
        assert_eq!(hb.best_ask_ticks(), Some(1001));
    }

    #[test]
    fn hot_book_from_decimal() {
        let s = MarketScale::from_spec(&spec());
        let hb = build_hot_book(&book(), &s, 7, 123);
        assert_eq!(hb.generation, 7);
        assert_eq!(hb.recv_ns, 123);
        assert_eq!(hb.best_bid_ticks(), Some(1000));
        assert_eq!(hb.best_ask_ticks(), Some(1001));
        assert_eq!(hb.bids().len(), 2);
        assert_eq!(hb.asks().len(), 2);
        assert!(!hb.is_crossed());
        assert_eq!(hb.mid_half_ticks(), Some(2001));
        assert_eq!(hb.touch_ticks(crate::types::Side::Buy), Some(1000));
        assert_eq!(hb.touch_ticks(crate::types::Side::Sell), Some(1001));
    }

    #[test]
    fn hot_book_detects_crossed() {
        let s = MarketScale::from_spec(&spec());
        let now = Utc::now();
        let crossed = OrderBook::from_levels(vec![(dec!(101), dec!(1))], vec![(dec!(100), dec!(1))], now, now);
        let hb = build_hot_book(&crossed, &s, 1, 0);
        assert!(hb.is_crossed());
    }

    #[test]
    fn hot_book_truncates_to_capacity() {
        let s = MarketScale::from_spec(&spec());
        let now = Utc::now();
        let many: Vec<(Decimal, Decimal)> = (0..30).map(|i| (dec!(100) - Decimal::from(i) * dec!(0.1), dec!(1))).collect();
        let deep = OrderBook::from_levels(many, vec![(dec!(100.1), dec!(1))], now, now);
        let hb = build_hot_book(&deep, &s, 1, 0);
        assert_eq!(hb.bids().len(), HOT_LEVELS);
    }
}
