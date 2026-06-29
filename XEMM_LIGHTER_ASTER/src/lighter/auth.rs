//! WS auth token generation for private channels (account_orders, accountActiveOrders).
//! Port of `_generate_ws_auth_token` + the 9-minute refresh loop. The native signer's
//! `CreateAuthToken` takes an ABSOLUTE unix-seconds deadline (Python passes `now + ttl`).

use crate::lighter::signer::Signer;
use anyhow::Result;
use std::time::{SystemTime, UNIX_EPOCH};

/// Server token TTL is ~10 min; refresh at 9 min to avoid expiry races.
pub const AUTH_TTL_SECS: i64 = 600;
pub const AUTH_REFRESH_SECS: u64 = 540;

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Generate a fresh short-lived WS auth token (deadline = now + TTL).
pub fn generate_ws_auth_token(signer: &Signer, api_key_index: i32) -> Result<String> {
    signer.create_auth_token(now_unix() + AUTH_TTL_SECS, api_key_index)
}
