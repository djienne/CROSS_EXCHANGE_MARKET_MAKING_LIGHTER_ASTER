//! Request signing for the live venue workers + the monotonic nonces/timestamps both venues
//! require. The cryptographic primitives live in [`super::crypto`] (golden-tested byte-for-byte
//! against live-confirmed oracles); this module wires a private key into the venue trait shapes.
//!
//! ## Safety model (post-wiring)
//!
//! Real signing is now implemented ([`EvmAsterSigner`] / [`EvmHlSigner`]). The hard gate is no
//! longer "the signer refuses" but the explicit opt-in chain enforced in [`super::super::run`]:
//! a real order requires `[live] enabled = true`, `mode = "live"`, and exactly one selected
//! market. Paper mode never constructs a live worker, so it can never reach a signer. The
//! money-risking `probe` checks add per-call confirmation and a `--max-usd` cap on top.
//!
//! [`TestSigner`] (cfg(test)) is a fixed-key signer used by the worker unit tests so they
//! exercise the *real* signed-request construction without any network.

use std::sync::atomic::{AtomicI64, Ordering};

use chrono::Utc;
use k256::ecdsa::SigningKey;

use super::crypto::{self, EvmSignature};

/// Why a signing attempt failed.
#[derive(Debug, thiserror::Error)]
pub enum SignError {
    /// A real signer rejected the input (bad key, malformed payload, …).
    #[error("signing failed: {0}")]
    Failed(String),
}

/// An Aster signature in venue wire form (`0x`-prefixed 65-byte `r||s||v` hex).
#[derive(Debug, Clone)]
pub struct Signature(pub String);

/// Signs Aster V3 requests: ABI-encode `(string json, address user, address signer, uint256
/// nonce)` → keccak256 → EIP-191 `personal_sign` (see [`crypto::aster_sign_v3`]). Returns the
/// `0x`-hex signature to attach as the `signature` request field.
pub trait AsterSigner: Send + Sync {
    /// The API-wallet (`signer`) address sent as the `signer` request field (raw form).
    fn signer_address(&self) -> &str;
    /// The main-account (`user`) address sent as the `user` request field (raw form).
    fn user_address(&self) -> &str;
    /// Sign the canonical `json_str` (sorted, trimmed params) with the microsecond `nonce`.
    fn sign_v3(&self, json_str: &str, nonce: i64) -> Result<Signature, SignError>;
}

/// Signs Hyperliquid L1 actions: a precomputed action-hash (`connectionId`, with the vault +
/// nonce already folded in by the worker) → EIP-712 phantom `Agent` digest → `{r,s,v}`.
pub trait HlSigner: Send + Sync {
    /// The L1 (sub-account / vault) address used to query account state and set `vaultAddress`.
    fn account_address(&self) -> &str;
    /// Sign a precomputed 32-byte action hash (connectionId) for the given network.
    fn sign_l1(
        &self,
        connection_id: &[u8; 32],
        is_mainnet: bool,
    ) -> Result<EvmSignature, SignError>;
}

/// A real Aster V3 signer backed by the API-wallet private key. Carries the parsed `user`/
/// `signer` address bytes (for the ABI encode) and their original string forms (for the request
/// fields — Aster looks the agent pair up case-insensitively, so we preserve the source form).
pub struct EvmAsterSigner {
    user: String,
    signer: String,
    user_bytes: [u8; 20],
    signer_bytes: [u8; 20],
    signing_key: SigningKey,
}

impl EvmAsterSigner {
    /// Build from the resolved roles. Validates that `signer` is the address of `key`.
    pub fn new(user: String, signer: String, key: [u8; 32]) -> anyhow::Result<Self> {
        let signing_key = SigningKey::from_slice(&key)
            .map_err(|e| anyhow::anyhow!("bad aster private key: {e}"))?;
        let user_bytes = crypto::parse_address(&user)?;
        let signer_bytes = crypto::parse_address(&signer)?;
        let derived = crypto::address_from_signing_key(&signing_key);
        if derived != signer_bytes {
            anyhow::bail!(
                "aster signer address {} does not match the private key's address {}",
                signer,
                crypto::address_hex(&derived)
            );
        }
        Ok(EvmAsterSigner {
            user,
            signer,
            user_bytes,
            signer_bytes,
            signing_key,
        })
    }
}

impl AsterSigner for EvmAsterSigner {
    fn signer_address(&self) -> &str {
        &self.signer
    }
    fn user_address(&self) -> &str {
        &self.user
    }
    fn sign_v3(&self, json_str: &str, nonce: i64) -> Result<Signature, SignError> {
        let sig = crypto::aster_sign_v3_with_key(
            &self.signing_key,
            &self.user_bytes,
            &self.signer_bytes,
            json_str,
            nonce as u64,
        )
        .map_err(|e| SignError::Failed(e.to_string()))?;
        Ok(Signature(sig))
    }
}

/// A real Hyperliquid signer backed by the agent (API-wallet) private key. `account` is the
/// sub-account/vault address that L1 actions trade on behalf of and that `/info` reads query.
pub struct EvmHlSigner {
    account: String,
    key: [u8; 32],
}

impl EvmHlSigner {
    pub fn new(account: String, key: [u8; 32]) -> anyhow::Result<Self> {
        // Validate the key parses; the account need not equal the key's address (agent model).
        let _ = crypto::address_from_priv(&key)?;
        crypto::parse_address(&account)?;
        Ok(EvmHlSigner { account, key })
    }

    /// The agent address this key derives to (for logging / sanity only).
    pub fn agent_address(&self) -> anyhow::Result<String> {
        Ok(crypto::address_hex(&crypto::address_from_priv(&self.key)?))
    }
}

impl HlSigner for EvmHlSigner {
    fn account_address(&self) -> &str {
        &self.account
    }
    fn sign_l1(
        &self,
        connection_id: &[u8; 32],
        is_mainnet: bool,
    ) -> Result<EvmSignature, SignError> {
        crypto::hl_sign(&self.key, connection_id, is_mainnet)
            .map_err(|e| SignError::Failed(e.to_string()))
    }
}

/// Strictly-increasing Aster nonce in **microseconds** (API ref §A2). Wall-clock based but
/// ratcheted so a stalled clock — or two calls in the same microsecond — never repeats or goes
/// backward (Aster dedups the last 100 nonces per user).
#[derive(Debug)]
pub struct AsterNonce {
    last_us: AtomicI64,
}

impl Default for AsterNonce {
    fn default() -> Self {
        AsterNonce {
            last_us: AtomicI64::new(0),
        }
    }
}

impl AsterNonce {
    pub fn new() -> Self {
        Self::default()
    }

    /// Next strictly-increasing microsecond nonce.
    pub fn next(&self) -> i64 {
        let now = Utc::now().timestamp_micros();
        loop {
            let prev = self.last_us.load(Ordering::Acquire);
            let candidate = now.max(prev + 1);
            if self
                .last_us
                .compare_exchange(prev, candidate, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return candidate;
            }
        }
    }
}

/// Strictly-increasing **millisecond** clock — used for the Aster `timestamp` param and the
/// Hyperliquid nonce (HL rejects duplicate-ms nonces under hedge bursts, so it must ratchet).
#[derive(Debug)]
pub struct MonotonicMs {
    last_ms: AtomicI64,
}

impl Default for MonotonicMs {
    fn default() -> Self {
        MonotonicMs {
            last_ms: AtomicI64::new(0),
        }
    }
}

impl MonotonicMs {
    pub fn new() -> Self {
        Self::default()
    }
    /// Next strictly-increasing millisecond value.
    pub fn next(&self) -> i64 {
        let now = Utc::now().timestamp_millis();
        loop {
            let prev = self.last_ms.load(Ordering::Acquire);
            let candidate = now.max(prev + 1);
            if self
                .last_ms
                .compare_exchange(prev, candidate, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return candidate;
            }
        }
    }
}

/// Hyperliquid nonce in milliseconds (a [`MonotonicMs`] returning `u64`).
#[derive(Debug, Default)]
pub struct HlNonce {
    inner: MonotonicMs,
}

impl HlNonce {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn next(&self) -> u64 {
        self.inner.next() as u64
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// The well-known test key 0x…01 (address 0x7E5F…95Bdf). Never a real account.
    pub const TEST_KEY: [u8; 32] = {
        let mut k = [0u8; 32];
        k[31] = 1;
        k
    };

    /// A fixed-key signer for worker unit tests: real signing, no network. Implements BOTH
    /// venue traits so one value can back both workers.
    pub struct TestSigner {
        aster: EvmAsterSigner,
        hl: EvmHlSigner,
    }

    impl TestSigner {
        pub fn new() -> Self {
            // user is an arbitrary distinct address; signer must equal the test key's address.
            let signer = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf".to_string();
            let user = "0x062903894bce55d4f80ee5931c46c77cd7881351".to_string();
            TestSigner {
                aster: EvmAsterSigner::new(user, signer, TEST_KEY).unwrap(),
                hl: EvmHlSigner::new(
                    "0x062903894bce55d4f80ee5931c46c77cd7881351".into(),
                    TEST_KEY,
                )
                .unwrap(),
            }
        }
    }

    impl AsterSigner for TestSigner {
        fn signer_address(&self) -> &str {
            self.aster.signer_address()
        }
        fn user_address(&self) -> &str {
            self.aster.user_address()
        }
        fn sign_v3(&self, json_str: &str, nonce: i64) -> Result<Signature, SignError> {
            self.aster.sign_v3(json_str, nonce)
        }
    }

    impl HlSigner for TestSigner {
        fn account_address(&self) -> &str {
            self.hl.account_address()
        }
        fn sign_l1(
            &self,
            connection_id: &[u8; 32],
            is_mainnet: bool,
        ) -> Result<EvmSignature, SignError> {
            self.hl.sign_l1(connection_id, is_mainnet)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{TestSigner, TEST_KEY};
    use super::*;

    #[test]
    fn evm_aster_signer_rejects_mismatched_signer() {
        // signer address that is NOT the test key's address must be rejected.
        // (We avoid unwrap_err so EvmAsterSigner need not derive Debug — it holds a key.)
        let res = EvmAsterSigner::new(
            "0x062903894bce55d4f80ee5931c46c77cd7881351".into(),
            "0x0000000000000000000000000000000000000001".into(),
            TEST_KEY,
        );
        assert!(res.is_err());
        assert!(res.err().unwrap().to_string().contains("does not match"));
    }

    #[test]
    fn test_signer_produces_real_aster_signature() {
        let s = TestSigner::new();
        // The canonical golden json_str signs to the known oracle signature.
        let json = "{\"price\":\"40.0\",\"quantity\":\"0.3\",\"recvWindow\":\"50000\",\"side\":\"BUY\",\"symbol\":\"HYPEUSDT\",\"timeInForce\":\"GTX\",\"timestamp\":\"1700000000000\",\"type\":\"LIMIT\"}";
        let sig = s.sign_v3(json, 1_700_000_000_000_000).unwrap();
        assert_eq!(sig.0, "0x89e4e500a9a37a27e4a3ea2e726d02c10ca1661bcf708b19c32f9d696ed346ea7cebfb4861c2c1dd953ff0333ea9626f0f69944cfbd8071ca19331a86711a01e1c");
    }

    #[test]
    fn test_signer_produces_real_hl_signature() {
        let s = TestSigner::new();
        let mut cid = [0u8; 32];
        for (i, b) in cid.iter_mut().enumerate() {
            *b = i as u8;
        }
        let sig = s.sign_l1(&cid, true).unwrap();
        assert_eq!(sig.v, 28);
        assert_eq!(
            sig.r,
            "0x6fac96b099be7b8cdf3bc5ccec1b7966b2543f97941734f89f106b3f18e28d3b"
        );
    }

    #[test]
    fn aster_nonce_strictly_increases() {
        let n = AsterNonce::new();
        let a = n.next();
        let b = n.next();
        let c = n.next();
        assert!(b > a, "nonce must increase: {a} -> {b}");
        assert!(c > b);
    }

    #[test]
    fn monotonic_ms_strictly_increases() {
        let n = MonotonicMs::new();
        let a = n.next();
        let b = n.next();
        assert!(b > a);
    }

    #[test]
    fn hl_nonce_strictly_increases() {
        let n = HlNonce::new();
        let a = n.next();
        let b = n.next();
        assert!(b > a);
    }
}
