//! Live account state: capital + positions + open orders, reconciled from both venues
//! (plan §2). Two layers, as the plan prescribes:
//!
//! 1. **`AccountSnapshot`** — a full, immutable snapshot published through
//!    `ArcSwap<AccountSnapshot>`. The strategy reads a consistent picture without locking.
//! 2. **`HotRisk`** — a handful of scaled-integer atomics for the few values checked on
//!    *every* quote (trading-allowed flag, cooldown deadline, account generation), read
//!    wait-free on the hot path.
//!
//! `Decimal` is kept for the snapshot (config / reporting / reconciliation precision);
//! the hot atomics carry only what the quote loop must consult per iteration.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use rust_decimal::Decimal;

use crate::types::{MarketId, Side};

/// Which venue a position / order belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Venue {
    Aster,
    Hyperliquid,
}

impl Venue {
    pub fn as_str(self) -> &'static str {
        match self {
            Venue::Aster => "aster",
            Venue::Hyperliquid => "hyperliquid",
        }
    }
}

/// A reconciled signed position on one venue for one market. `signed_qty > 0` long.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScaledPosition {
    pub venue: Venue,
    pub market: MarketId,
    /// Signed base-unit quantity (positive long, negative short). Exact `Decimal`.
    pub signed_qty: Decimal,
    pub entry_px: Decimal,
}

/// A reconciled open order known to the venue at snapshot time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenOrderSnapshot {
    pub venue: Venue,
    pub market: MarketId,
    pub side: Side,
    pub price: Decimal,
    pub qty: Decimal,
    /// Bot-assigned client id, if this order is recognized as ours.
    pub client_id: Option<String>,
    /// Venue-assigned id (Aster orderId / HL oid).
    pub venue_order_id: Option<String>,
}

impl OpenOrderSnapshot {
    /// Whether this order looks like one of ours (client id carries our `X` prefix).
    pub fn is_bot_order(&self) -> bool {
        self.client_id.as_deref().is_some_and(|c| c.starts_with('X'))
    }
}

/// A full, immutable account snapshot. Published atomically via [`AccountState`].
#[derive(Debug, Clone)]
pub struct AccountSnapshot {
    pub aster_available_usd: Decimal,
    pub hl_withdrawable_usd: Decimal,
    /// Aster total equity = wallet balance + unrealized PnL (mark-to-market), NOT the free-margin
    /// `aster_available_usd`. Used by the circuit breaker so opening a hedge (which locks margin)
    /// does not look like a loss.
    pub aster_equity_usd: Decimal,
    /// HL total equity = `marginSummary.accountValue` (includes unrealized), NOT the free-margin
    /// `hl_withdrawable_usd` (which drops when a position locks margin).
    pub hl_equity_usd: Decimal,
    pub aster_positions: Vec<ScaledPosition>,
    pub hl_positions: Vec<ScaledPosition>,
    pub open_orders: Vec<OpenOrderSnapshot>,
    /// Bumped on each publish so the strategy can detect a changed snapshot.
    pub generation: u64,
    /// Monotonic-clock nanos when this snapshot finished being assembled (set AFTER the venue reads;
    /// used for the freshness check).
    pub source_ts_ns: i64,
    /// Monotonic-clock nanos when the venue reads for this snapshot STARTED (set BEFORE the first
    /// read). The orphan backstop only trusts a snapshot whose reads all began after its last hot
    /// action (`read_start_ns > last_hot_action_ns`), so a snapshot that straddles a fill/hedge can't
    /// trigger a double-hedge. Distinct from `source_ts_ns` (which is stamped after the reads).
    pub read_start_ns: i64,
}

impl AccountSnapshot {
    /// An empty snapshot (pre-bootstrap): no capital, no positions, generation 0.
    pub fn empty() -> Self {
        AccountSnapshot {
            aster_available_usd: Decimal::ZERO,
            hl_withdrawable_usd: Decimal::ZERO,
            aster_equity_usd: Decimal::ZERO,
            hl_equity_usd: Decimal::ZERO,
            aster_positions: Vec::new(),
            hl_positions: Vec::new(),
            open_orders: Vec::new(),
            generation: 0,
            source_ts_ns: 0,
            read_start_ns: 0,
        }
    }

    /// Total cross-venue mark-to-market equity (USD). For a delta-neutral book this is stable; it
    /// moves only with realized PnL, fees, funding, and residual basis — the circuit breaker's signal.
    pub fn total_equity_usd(&self) -> Decimal {
        self.aster_equity_usd + self.hl_equity_usd
    }

    /// Reported signed position for a (venue, market), 0 if none.
    pub fn reported_position(&self, venue: Venue, market: &MarketId) -> Decimal {
        let list = match venue {
            Venue::Aster => &self.aster_positions,
            Venue::Hyperliquid => &self.hl_positions,
        };
        list.iter()
            .find(|p| &p.market == market)
            .map(|p| p.signed_qty)
            .unwrap_or(Decimal::ZERO)
    }

    /// Bot-owned open orders not matched to any expected client id — the "unknown open
    /// order" set the clean-start invariant (§8.1 inv 7) must be empty over.
    pub fn unknown_bot_orders<'a>(
        &'a self,
        known: &'a std::collections::HashSet<String>,
    ) -> impl Iterator<Item = &'a OpenOrderSnapshot> + 'a {
        self.open_orders.iter().filter(move |o| {
            o.is_bot_order() && o.client_id.as_deref().is_some_and(|c| !known.contains(c))
        })
    }
}

/// Scaled-integer fixed-point used by the hot atomics: USD micro-dollars (1e-6 USD) so a
/// `Decimal` USD value fits an `i64` with sub-cent resolution up to ~9.2e12 USD.
pub const USD_SCALE: i64 = 1_000_000;

/// Convert a `Decimal` USD amount to scaled `i64` micro-dollars (saturating).
pub fn usd_to_scaled(v: Decimal) -> i64 {
    use rust_decimal::prelude::ToPrimitive;
    (v * Decimal::from(USD_SCALE)).round().to_i64().unwrap_or(i64::MAX)
}

/// The few values consulted on EVERY quote, as lock-free atomics (plan §2.5 `HotRisk`).
/// Updated by the single account/risk reactor; read wait-free by the strategy loop.
pub struct HotRisk {
    /// Master "may place new maker quotes" flag (gate AND risk both open).
    trading_allowed: AtomicBool,
    /// Monotonic-clock nanos until which the post-trade cooldown suppresses new quotes.
    cooldown_until_ns: AtomicI64,
    /// Max tolerated unhedged notional, scaled micro-dollars (mirror of config for the loop).
    max_unhedged_notional: AtomicI64,
    /// Bumped whenever a new account snapshot is published.
    account_generation: AtomicU64,
}

impl Default for HotRisk {
    fn default() -> Self {
        HotRisk {
            trading_allowed: AtomicBool::new(false), // closed until bootstrap completes
            cooldown_until_ns: AtomicI64::new(0),
            max_unhedged_notional: AtomicI64::new(0),
            account_generation: AtomicU64::new(0),
        }
    }
}

impl HotRisk {
    pub fn new(max_unhedged_notional_usd: Decimal) -> Self {
        let hr = HotRisk::default();
        hr.max_unhedged_notional
            .store(usd_to_scaled(max_unhedged_notional_usd), Ordering::Release);
        hr
    }

    #[inline]
    pub fn set_trading_allowed(&self, v: bool) {
        self.trading_allowed.store(v, Ordering::Release);
    }
    #[inline]
    pub fn trading_allowed(&self) -> bool {
        self.trading_allowed.load(Ordering::Acquire)
    }
    #[inline]
    pub fn set_cooldown_until_ns(&self, ns: i64) {
        self.cooldown_until_ns.store(ns, Ordering::Release);
    }
    #[inline]
    pub fn cooldown_until_ns(&self) -> i64 {
        self.cooldown_until_ns.load(Ordering::Acquire)
    }
    /// Whether the cooldown is active at monotonic `now_ns`.
    #[inline]
    pub fn in_cooldown(&self, now_ns: i64) -> bool {
        now_ns < self.cooldown_until_ns()
    }
    #[inline]
    pub fn max_unhedged_notional_scaled(&self) -> i64 {
        self.max_unhedged_notional.load(Ordering::Acquire)
    }
    #[inline]
    pub fn bump_account_generation(&self) -> u64 {
        self.account_generation.fetch_add(1, Ordering::Release) + 1
    }
    #[inline]
    pub fn account_generation(&self) -> u64 {
        self.account_generation.load(Ordering::Acquire)
    }

    /// May the strategy place a NEW maker quote right now? True only when trading is
    /// allowed AND the cooldown has expired. Risk-reducing actions (cancel/hedge) ignore
    /// this and are always allowed.
    #[inline]
    pub fn may_quote(&self, now_ns: i64) -> bool {
        self.trading_allowed() && !self.in_cooldown(now_ns)
    }
}

/// The published account state: an `ArcSwap` snapshot plus the hot atomics. Cloneable
/// handle (shares the inner `Arc`s), so each plane holds one.
#[derive(Clone)]
pub struct AccountState {
    snapshot: Arc<ArcSwap<AccountSnapshot>>,
    pub hot: Arc<HotRisk>,
}

impl AccountState {
    pub fn new(max_unhedged_notional_usd: Decimal) -> Self {
        AccountState {
            snapshot: Arc::new(ArcSwap::from_pointee(AccountSnapshot::empty())),
            hot: Arc::new(HotRisk::new(max_unhedged_notional_usd)),
        }
    }

    /// Publish a new snapshot and bump the account generation atomic.
    ///
    /// Store the snapshot first and then publish the hot generation, so a strategy reader that sees
    /// generation `N` can never still load snapshot `N - 1`. The reconciler is the single writer.
    pub fn publish(&self, mut snap: AccountSnapshot) {
        let next_generation = self.hot.account_generation.load(Ordering::Acquire).saturating_add(1);
        snap.generation = next_generation;
        self.snapshot.store(Arc::new(snap));
        self.hot.account_generation.store(next_generation, Ordering::Release);
    }

    /// Wait-free read of the current snapshot.
    pub fn load(&self) -> Arc<AccountSnapshot> {
        self.snapshot.load_full()
    }

    /// Snapshot age in ms at monotonic `now_ns` (`i64::MAX` before the first publish).
    pub fn age_ms(&self, now_ns: i64) -> i64 {
        let ts = self.load().source_ts_ns;
        if ts == 0 {
            i64::MAX
        } else {
            now_ns.saturating_sub(ts) / 1_000_000
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::collections::HashSet;

    fn snap() -> AccountSnapshot {
        AccountSnapshot {
            aster_available_usd: dec!(1000),
            hl_withdrawable_usd: dec!(900),
            aster_equity_usd: dec!(1000),
            hl_equity_usd: dec!(900),
            aster_positions: vec![ScaledPosition {
                venue: Venue::Aster,
                market: "BTC".into(),
                signed_qty: dec!(0.5),
                entry_px: dec!(100),
            }],
            hl_positions: vec![ScaledPosition {
                venue: Venue::Hyperliquid,
                market: "BTC".into(),
                signed_qty: dec!(-0.5),
                entry_px: dec!(100),
            }],
            open_orders: vec![
                OpenOrderSnapshot {
                    venue: Venue::Aster,
                    market: "BTC".into(),
                    side: Side::Buy,
                    price: dec!(99),
                    qty: dec!(0.1),
                    client_id: Some("Xabc-BTC-B-0".into()),
                    venue_order_id: Some("111".into()),
                },
                OpenOrderSnapshot {
                    venue: Venue::Aster,
                    market: "BTC".into(),
                    side: Side::Sell,
                    price: dec!(101),
                    qty: dec!(0.1),
                    client_id: Some("manual-order".into()), // not ours (no X prefix)
                    venue_order_id: Some("222".into()),
                },
            ],
            generation: 3,
            source_ts_ns: 5_000_000,
            read_start_ns: 4_000_000,
        }
    }

    #[test]
    fn reported_position_lookup() {
        let s = snap();
        assert_eq!(s.reported_position(Venue::Aster, &"BTC".into()), dec!(0.5));
        assert_eq!(s.reported_position(Venue::Hyperliquid, &"BTC".into()), dec!(-0.5));
        assert_eq!(s.reported_position(Venue::Aster, &"ETH".into()), dec!(0)); // absent
    }

    #[test]
    fn unknown_bot_orders_excludes_known_and_manual() {
        let s = snap();
        // No known ids: our Xabc order is unknown; the manual order is not a bot order.
        let known: HashSet<String> = HashSet::new();
        let unknown: Vec<_> = s.unknown_bot_orders(&known).collect();
        assert_eq!(unknown.len(), 1);
        assert_eq!(unknown[0].client_id.as_deref(), Some("Xabc-BTC-B-0"));
        // Once we know that id, the unknown set is empty (clean start).
        let known: HashSet<String> = ["Xabc-BTC-B-0".to_string()].into_iter().collect();
        assert_eq!(s.unknown_bot_orders(&known).count(), 0);
    }

    #[test]
    fn account_state_publish_and_generation() {
        let st = AccountState::new(dec!(5));
        assert_eq!(st.hot.account_generation(), 0);
        assert_eq!(st.load().generation, 0); // empty
        assert!(!st.hot.trading_allowed()); // closed pre-bootstrap
        st.publish(snap());
        assert_eq!(st.hot.account_generation(), 1);
        assert_eq!(st.load().generation, 1);
        assert_eq!(st.load().aster_available_usd, dec!(1000));
        // max_unhedged mirror is scaled into micro-dollars.
        assert_eq!(st.hot.max_unhedged_notional_scaled(), 5_000_000);
    }

    #[test]
    fn hot_risk_may_quote_respects_cooldown_and_flag() {
        let hr = HotRisk::new(dec!(5));
        // closed by default
        assert!(!hr.may_quote(1000));
        hr.set_trading_allowed(true);
        assert!(hr.may_quote(1000));
        // cooldown until ns=2000 suppresses quoting before then
        hr.set_cooldown_until_ns(2000);
        assert!(hr.in_cooldown(1500));
        assert!(!hr.may_quote(1500));
        assert!(hr.may_quote(2000)); // expired (>=)
    }

    #[test]
    fn usd_scaling_round_trips_within_resolution() {
        assert_eq!(usd_to_scaled(dec!(5)), 5_000_000);
        assert_eq!(usd_to_scaled(dec!(0.000001)), 1);
        assert_eq!(usd_to_scaled(dec!(1234.56)), 1_234_560_000);
    }
}
