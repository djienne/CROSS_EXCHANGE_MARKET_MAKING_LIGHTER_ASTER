//! Optimistic nonce manager — port of `lighter/nonce_manager.py::OptimisticNonceManager`.
//!
//! Init: `nonce = nextNonce() - 1`. `next()` = `++nonce` (atomic). `acknowledge_failure()`
//! = `--nonce` (roll back a reservation that failed to sign). `hard_refresh()` = re-fetch
//! and set to `N-1`. Single api-key in practice; one counter.
//!
//! Concurrency: sign+send is serialized under one async Mutex upstream, so the increment /
//! rollback sequence around each tx is atomic w.r.t. other txs (matches Python `_sdk_write_lock`).

use crate::lighter::rest::RestClient;
use anyhow::Result;
use std::sync::atomic::{AtomicI64, Ordering};

pub struct NonceManager {
    account_index: i64,
    api_key_index: i32,
    nonce: AtomicI64,
}

impl NonceManager {
    /// Fetch the current nonce and store `N - 1` (so the first `next()` yields `N`).
    pub async fn init(rest: &RestClient, account_index: i64, api_key_index: i32) -> Result<Self> {
        let n = rest.next_nonce(account_index, api_key_index).await?;
        Ok(Self {
            account_index,
            api_key_index,
            nonce: AtomicI64::new(n - 1),
        })
    }

    /// Reserve and return the next nonce.
    #[inline]
    pub fn next(&self) -> i64 {
        self.nonce.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Roll back the last reserved nonce (sign failure before send).
    #[inline]
    pub fn acknowledge_failure(&self) {
        self.nonce.fetch_sub(1, Ordering::SeqCst);
    }

    /// Roll back `n` reserved nonces in one shot (a whole batch that was signed but whose
    /// frame was rejected/never-consumed). Mirrors the Python which calls
    /// `acknowledge_failure` once per signed nonce. No-op for `n == 0`.
    #[inline]
    pub fn rollback(&self, n: usize) {
        if n > 0 {
            self.nonce.fetch_sub(n as i64, Ordering::SeqCst);
        }
    }

    /// Re-fetch from the API and reset to `N - 1` (after a nonce error / unknown outcome).
    pub async fn hard_refresh(&self, rest: &RestClient) -> Result<()> {
        let n = rest.next_nonce(self.account_index, self.api_key_index).await?;
        self.nonce.store(n - 1, Ordering::SeqCst);
        Ok(())
    }

    #[inline]
    pub fn api_key_index(&self) -> i32 {
        self.api_key_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_and_rollback() {
        let nm = NonceManager {
            account_index: 1,
            api_key_index: 0,
            nonce: AtomicI64::new(99), // as if nextNonce returned 100 -> stored 99
        };
        assert_eq!(nm.next(), 100);
        assert_eq!(nm.next(), 101);
        nm.acknowledge_failure(); // roll back the 101
        assert_eq!(nm.next(), 101);
    }

    #[test]
    fn batch_rollback() {
        let nm = NonceManager {
            account_index: 1,
            api_key_index: 0,
            nonce: AtomicI64::new(199), // nextNonce 200 -> stored 199
        };
        // Reserve a batch of 4 (200..=203).
        assert_eq!(nm.next(), 200);
        assert_eq!(nm.next(), 201);
        assert_eq!(nm.next(), 202);
        assert_eq!(nm.next(), 203);
        // Whole batch rejected without consuming nonces -> roll all 4 back.
        nm.rollback(4);
        assert_eq!(nm.next(), 200);
        // rollback(0) is a no-op.
        nm.rollback(0);
        assert_eq!(nm.next(), 201);
    }
}
