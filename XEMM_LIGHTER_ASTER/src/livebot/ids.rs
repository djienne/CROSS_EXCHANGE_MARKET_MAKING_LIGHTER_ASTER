//! Deterministic client IDs (plan §8.2). Idempotency and restart recovery are
//! mandatory for a cross-exchange bot: every order carries an id we can recompute and
//! query by, so a process that dies mid-hedge can ask the venue "did this fill?".
//!
//! - **Aster maker client id**: `X{session}-{market}-{B|S}-{epoch}` — unique per quote,
//!   kept inside Aster's `newClientOrderId` charset/length budget (Binance-style
//!   `^[A-Za-z0-9_:/.\-]{1,36}$`). Maker ids need not survive a restart (startup cancels
//!   all Aster orders), only be unique within a session.
//! - **Hyperliquid hedge cloid**: a 128-bit id derived **purely** from the exchange-supplied
//!   Aster fill identity `(aster_order_id, aster_trade_id, cumulative_filled_qty)` — every
//!   input comes from the venue, NONE from a bot-side session counter — so it is
//!   **session-independent**: re-processing the same fill after a restart yields the SAME
//!   cloid; we then query HL `orderStatus` by it and never double-hedge (invariants 3 & 4).
//!
//! Hashing is a tiny inline FNV-1a (no new dependency, and stable across toolchains —
//! `std`'s `DefaultHasher` is explicitly NOT stable, so it must not be used here).

use crate::types::{MarketId, Side};

/// 64-bit FNV-1a over bytes with a caller-chosen offset basis (varying the basis gives an
/// independent hash for packing >64 bits). Stable forever by construction.
fn fnv1a64(bytes: &[u8], mut hash: u64) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

const FNV_BASIS_A: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_BASIS_B: u64 = 0x1099_5163_2d4b_c7e1; // a distinct basis for the high 64 bits
pub const LIGHTER_MAX_CLIENT_ORDER_INDEX: i64 = 281_474_976_710_655; // 2^48 - 1

/// A short per-process session tag for maker order ids. Derived from a UUID so it is
/// unique per run; truncated to keep order ids short.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionId(String);

impl SessionId {
    /// A fresh 6-char base36 session tag from a v4 UUID's low bits.
    pub fn random() -> Self {
        let u = uuid::Uuid::new_v4();
        let n = u128::from_le_bytes(*u.as_bytes()) as u64;
        SessionId(base36(n, 6))
    }
    /// Construct from an explicit tag (tests / reproducible runs). Sanitized to the
    /// allowed charset and clamped to 6 chars.
    pub fn from_tag(tag: &str) -> Self {
        let s: String = tag.chars().filter(|c| c.is_ascii_alphanumeric()).take(6).collect();
        SessionId(if s.is_empty() { "0".into() } else { s.to_ascii_lowercase() })
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A compact market code for an order id: uppercase alphanumerics, ≤ 7 chars. Market ids
/// are already short symbols ("BTC", "TRUMP"), so this is just a defensive clamp.
fn market_code(market: &MarketId) -> String {
    market
        .0
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(7)
        .collect::<String>()
        .to_ascii_uppercase()
}

/// Aster maker `newClientOrderId`. `quote_epoch` is a per-(market,side) monotonic counter
/// the order-state layer increments on each new quote, guaranteeing uniqueness. Form:
/// `X{session}-{MARKET}-{B|S}-{epoch36}` — always within the 36-char / charset budget.
/// Session-prefixed client id for a FLATTEN (reduce-only close) order. Carries the same
/// `X{session}-` prefix as maker ids so `OrderManager::is_own_client_id` attributes the
/// resulting reduce-only fills to this session — without it the venue assigns a foreign-
/// looking id and the strategy drops its own flatten fills ("non-bot Aster fill").
pub fn aster_flatten_client_id(session: &SessionId, market: &MarketId, epoch: u64) -> String {
    let s = format!(
        "X{}-{}-F-{}",
        session.as_str(),
        market_code(market),
        base36(epoch, 0),
    );
    if s.len() > 36 {
        return s[..36].to_string();
    }
    s
}

pub fn aster_client_id(session: &SessionId, market: &MarketId, side: Side, quote_epoch: u64) -> String {
    let s = format!(
        "X{}-{}-{}-{}",
        session.as_str(),
        market_code(market),
        match side {
            Side::Buy => "B",
            Side::Sell => "S",
        },
        base36(quote_epoch, 0),
    );
    // Defensive: never exceed Aster's 36-char client-id cap even with pathological inputs.
    if s.len() > 36 {
        s[..36].to_string()
    } else {
        s
    }
}

/// A 128-bit Hyperliquid client order id (`cloid`). Hyperliquid requires a 16-byte hex
/// value (`0x` + 32 hex digits); a human-readable string is NOT accepted, so the
/// `XEMM-HEDGE-...` mnemonic from the plan is realized as a deterministic hash of the same
/// fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cloid([u8; 16]);

impl Cloid {
    /// Deterministic hedge cloid from the Aster fill identity. SAME inputs ⇒ SAME cloid,
    /// across restarts and processes — the basis for hedge idempotency / recovery (§8.2).
    ///
    /// CRITICAL: every input is **exchange-supplied and session-independent**: the order id,
    /// the trade id, and the cumulative filled quantity (scaled to integer micro-units). It
    /// must NOT depend on any bot-side session counter, or a restart that re-processes the
    /// same fill would compute a different cloid and a recovery query by cloid would miss it.
    /// `(order_id, trade_id)` uniquely identifies a fill; `cum_scaled` disambiguates the
    /// rare case where the venue omits the trade id (mirrors [`super::fills::FillKey`]).
    pub fn hedge(aster_order_id: &str, aster_trade_id: &str, cum_scaled: i64) -> Self {
        let key = format!("XEMM-HEDGE-{aster_order_id}-{aster_trade_id}-{cum_scaled}");
        let lo = fnv1a64(key.as_bytes(), FNV_BASIS_A);
        let hi = fnv1a64(key.as_bytes(), FNV_BASIS_B);
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&hi.to_be_bytes());
        b[8..].copy_from_slice(&lo.to_be_bytes());
        Cloid(b)
    }

    /// Deterministic RECOVERY cloid from a market + scaled net delta. Same inputs ⇒ same
    /// cloid, so the STRATEGY can recognize (and skip) a re-dispatch for the same orphan net
    /// while one is already in flight, by looking the cloid up in its own intent map.
    ///
    /// IMPORTANT: the venue provides NO dedupe — Lighter keys orders on the derived
    /// `client_order_index` and happily accepts a reused one, which would cross-attribute
    /// fills in the FillTracker. Uniqueness is therefore the STRATEGY's job: a redispatch
    /// after a dangerous/Unknown attempt must use [`Cloid::recovery_attempt`] with a fresh
    /// attempt number, never this base cloid again. Distinct from [`Cloid::hedge`] (the
    /// `RECOVER` tag changes the hash).
    pub fn recovery(market: &MarketId, net_scaled: i64) -> Self {
        let key = format!("XEMM-RECOVER-{}-{net_scaled}", market.0);
        Self::from_key(&key)
    }

    /// Attempt-salted recovery cloid: attempt 0 is the base [`Cloid::recovery`] id; each
    /// re-dispatch after a dangerous (Unknown) attempt bumps the salt so the possibly-live
    /// earlier order can never share a Lighter client_order_index with the new one.
    pub fn recovery_attempt(market: &MarketId, net_scaled: i64, attempt: u32) -> Self {
        if attempt == 0 {
            return Self::recovery(market, net_scaled);
        }
        let key = format!("XEMM-RECOVER-{}-{net_scaled}-a{attempt}", market.0);
        Self::from_key(&key)
    }

    /// FLATTEN cloid, salted with the dispatch time: flattens need no restart-recovery
    /// identity (recovery is snapshot-driven; nothing queries Lighter by flatten id), and
    /// per-dispatch uniqueness is what prevents FillTracker collisions between overlapping
    /// flattens or an equal-sized recovery hedge (the venue does not dedupe indices).
    pub fn flatten(market: &MarketId, qty_scaled: i64, dispatch_ns: i64) -> Self {
        let key = format!("XEMM-FLATTEN-{}-{qty_scaled}-{dispatch_ns}", market.0);
        Self::from_key(&key)
    }

    fn from_key(key: &str) -> Self {
        let lo = fnv1a64(key.as_bytes(), FNV_BASIS_A);
        let hi = fnv1a64(key.as_bytes(), FNV_BASIS_B);
        let mut b = [0u8; 16];
        b[..8].copy_from_slice(&hi.to_be_bytes());
        b[8..].copy_from_slice(&lo.to_be_bytes());
        Cloid(b)
    }

    /// Hyperliquid wire form: `0x` followed by 32 lowercase hex digits. Single allocation
    /// (`hex::encode`) — this is on the fill→hedge hot path, so avoid the per-byte `format!` loop.
    pub fn to_hex(self) -> String {
        format!("0x{}", hex::encode(self.0))
    }

    pub fn bytes(self) -> [u8; 16] {
        self.0
    }

    pub(crate) fn from_bytes_for_lighter(bytes: [u8; 16]) -> Self {
        Cloid(bytes)
    }

    /// Lighter wire form: a positive integer `client_order_index` in `[1, 2^48 - 1]`.
    /// The mapping is deterministic from the same 128-bit id used for Hyperliquid cloids; zero
    /// is remapped to one because several exchange/client paths treat `0` as unset.
    pub fn to_lighter_client_order_index(self) -> i64 {
        let mut lo = [0u8; 8];
        lo.copy_from_slice(&self.0[8..]);
        let raw = u64::from_be_bytes(lo) % (LIGHTER_MAX_CLIENT_ORDER_INDEX as u64);
        let idx = raw as i64;
        if idx <= 0 { 1 } else { idx }
    }
}

/// Lowercase base36 of `n`, left-padded with '0' to at least `width` chars.
fn base36(mut n: u64, width: usize) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".repeat(width.max(1));
    }
    let mut buf = Vec::new();
    while n > 0 {
        buf.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    while buf.len() < width {
        buf.push(b'0');
    }
    buf.reverse();
    String::from_utf8(buf).expect("base36 digits are ascii")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_cloid_distinct_from_recovery_same_inputs() {
        let m: MarketId = "HYPE".into();
        let recovery = Cloid::recovery(&m, 1_000_000);
        let flatten = Cloid::flatten(&m, 1_000_000, 42);
        assert_ne!(recovery.bytes(), flatten.bytes());
        assert_ne!(
            recovery.to_lighter_client_order_index(),
            flatten.to_lighter_client_order_index(),
            "an equal-sized flatten and recovery must never share a Lighter index"
        );
        // Flatten is additionally salted per dispatch time.
        let flatten_later = Cloid::flatten(&m, 1_000_000, 43);
        assert_ne!(flatten.bytes(), flatten_later.bytes());
    }

    #[test]
    fn recovery_attempt_salt_changes_index() {
        let m: MarketId = "HYPE".into();
        let base = Cloid::recovery(&m, 5);
        assert_eq!(Cloid::recovery_attempt(&m, 5, 0).bytes(), base.bytes());
        let a1 = Cloid::recovery_attempt(&m, 5, 1);
        let a2 = Cloid::recovery_attempt(&m, 5, 2);
        assert_ne!(a1.bytes(), base.bytes());
        assert_ne!(a1.bytes(), a2.bytes());
        assert_ne!(
            a1.to_lighter_client_order_index(),
            a2.to_lighter_client_order_index(),
            "each redispatch must get a fresh Lighter index"
        );
    }

    #[test]
    fn aster_id_is_unique_and_in_charset() {
        let s = SessionId::from_tag("abc123");
        let a = aster_client_id(&s, &"BTC".into(), Side::Buy, 0);
        let b = aster_client_id(&s, &"BTC".into(), Side::Buy, 1);
        let c = aster_client_id(&s, &"BTC".into(), Side::Sell, 0);
        assert_ne!(a, b); // different epoch
        assert_ne!(a, c); // different side
        for id in [&a, &b, &c] {
            assert!(id.len() <= 36, "id too long: {id} ({})", id.len());
            assert!(
                id.chars().all(|ch| ch.is_ascii_alphanumeric() || "-_:/.".contains(ch)),
                "bad charset: {id}"
            );
        }
        assert!(a.starts_with("Xabc123-BTC-B-"));
    }

    #[test]
    fn aster_id_respects_36_char_cap_under_pathological_input() {
        let s = SessionId::from_tag("zzzzzz");
        let long_market = MarketId("VERYLONGMARKETNAME".into());
        let id = aster_client_id(&s, &long_market, Side::Sell, u64::MAX);
        assert!(id.len() <= 36);
    }

    #[test]
    fn hedge_cloid_is_deterministic_and_distinct() {
        let c1 = Cloid::hedge("AST-100", "T-7", 500_000);
        let c2 = Cloid::hedge("AST-100", "T-7", 500_000);
        let c3 = Cloid::hedge("AST-100", "T-7", 600_000); // different cumulative fill
        let c4 = Cloid::hedge("AST-101", "T-7", 500_000); // different order
        assert_eq!(c1, c2, "same exchange fill identity must yield the same cloid (idempotency)");
        assert_ne!(c1, c3);
        assert_ne!(c1, c4);
    }

    #[test]
    fn hedge_cloid_hex_format() {
        let c = Cloid::hedge("AST-100", "T-7", 500_000);
        let h = c.to_hex();
        assert!(h.starts_with("0x"));
        assert_eq!(h.len(), 34); // 0x + 32 hex
        assert!(h[2..].chars().all(|ch| ch.is_ascii_hexdigit()));
        // hex round-trips the bytes
        let bytes = c.bytes();
        assert_eq!(&h[2..4], &format!("{:02x}", bytes[0]));
    }

    #[test]
    fn lighter_client_order_index_is_stable_positive_and_in_range() {
        let c1 = Cloid::hedge("AST-100", "T-7", 500_000);
        let c2 = Cloid::hedge("AST-100", "T-7", 500_000);
        let idx = c1.to_lighter_client_order_index();
        assert_eq!(idx, c2.to_lighter_client_order_index());
        assert!(idx > 0);
        assert!(idx <= LIGHTER_MAX_CLIENT_ORDER_INDEX);
    }

    #[test]
    fn base36_pads_and_encodes() {
        assert_eq!(base36(0, 6), "000000");
        assert_eq!(base36(35, 0), "z");
        assert_eq!(base36(36, 0), "10");
    }
}
