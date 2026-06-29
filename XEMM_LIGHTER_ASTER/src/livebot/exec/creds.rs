//! Credential loading for live trading (plan §1, §2.2). Reads the `aster.env` / `lighter.env`
//! dotenv files at the repo root, derives the venue ROLE for each address from the **key**, not
//! from the (user-editable, sometimes mislabeled) field names, and validates the mapping before
//! a single signed call.
//!
//! These files contain real private keys in plaintext — they MUST be gitignored and never
//! logged. This module logs only public addresses, never key material.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use tracing::info;

use super::crypto::{address_from_priv, address_hex, parse_address, parse_priv_key};

/// Parse a tiny `key=value` dotenv file (the live env files are a handful of lines).
fn parse_env_file(path: &Path) -> Result<HashMap<String, String>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("could not read credentials file {}", path.display()))?;
    let mut out = HashMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || !line.contains('=') {
            continue;
        }
        let (k, v) = line.split_once('=').unwrap();
        out.insert(k.trim().to_string(), v.trim().to_string());
    }
    Ok(out)
}

fn required(m: &HashMap<String, String>, key: &str) -> Result<String> {
    m.get(key)
        .filter(|v| !v.trim().is_empty())
        .cloned()
        .ok_or_else(|| anyhow!("credentials env missing {key}"))
}

/// Resolved Aster credentials: the main-account `user`, the API-wallet `signer` (= the key's
/// address), and the signing key. The string forms preserve the source case (Aster looks up the
/// agent pair case-insensitively).
pub struct AsterCreds {
    pub user: String,
    pub signer: String,
    pub key: [u8; 32],
}

impl AsterCreds {
    /// Load + role-resolve from a dotenv file. `signer` = the address derived from `private_key`;
    /// `user` = the address field (`wallet_address`/`subaccount_address`) that ISN'T the signer.
    pub fn load(path: &Path) -> Result<Self> {
        let m = parse_env_file(path)?;
        let key = parse_priv_key(m.get("private_key").context("aster env missing private_key")?)?;
        let derived = address_from_priv(&key)?;
        let derived_lc = address_hex(&derived); // lowercase 0x form

        let mut signer: Option<String> = None;
        let mut user: Option<String> = None;
        for field in ["wallet_address", "subaccount_address"] {
            if let Some(v) = m.get(field) {
                let vb = parse_address(v)
                    .with_context(|| format!("aster env {field} is not a valid address"))?;
                if address_hex(&vb) == derived_lc {
                    signer = Some(v.clone()); // preserve source case for the request field
                } else {
                    user = Some(v.clone());
                }
            }
        }
        // The signer field is optional in the file (we can synthesize it from the key); the
        // user (main account) is mandatory and cannot be the signer.
        let signer = signer.unwrap_or_else(|| derived_lc.clone());
        let user = user.ok_or_else(|| {
            anyhow!(
                "could not determine the Aster main-account (user) address from the env file: \
                 no wallet_address/subaccount_address differs from the key's address {derived_lc}"
            )
        })?;
        if parse_address(&signer)? != derived {
            bail!("aster signer field does not match the private key's address");
        }
        info!("aster credentials: user={user} signer={signer} (roles derived from the key)");
        Ok(AsterCreds { user, signer, key })
    }
}

/// Resolved Lighter API-key credentials. These are not EVM keys, so validation is limited to
/// required-field presence and numeric account/API-key ids; the native signer performs the
/// authoritative key check through `CreateClient` / `CheckClient`.
#[derive(Debug, Clone)]
pub struct LighterCreds {
    pub api_private_key: String,
    pub api_public_key: String,
    pub api_key_index: i32,
    pub account_index: i64,
    pub wallet_address: String,
}

impl LighterCreds {
    pub fn load(path: &Path) -> Result<Self> {
        let m = parse_env_file(path)?;
        let api_private_key = required(&m, "API_KEY_PRIVATE_KEY")?;
        let api_public_key = required(&m, "API_KEY_PUBLIC_KEY")?;
        let api_key_index = required(&m, "API_KEY_INDEX")?
            .parse::<i32>()
            .context("API_KEY_INDEX must be an integer")?;
        let account_index = required(&m, "ACCOUNT_INDEX")?
            .parse::<i64>()
            .context("ACCOUNT_INDEX must be an integer")?;
        let wallet_address = required(&m, "WALLET_ADDRESS")?;
        info!(
            "lighter credentials: account_index={} api_key_index={} wallet={} public_key_len={}",
            account_index,
            api_key_index,
            wallet_address,
            api_public_key.len()
        );
        Ok(LighterCreds {
            api_private_key,
            api_public_key,
            api_key_index,
            account_index,
            wallet_address,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_tmp(name: &str, body: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("xemm-creds-{name}-{}.env", std::process::id()));
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        p
    }

    // Test key 0x…01 → address 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf.
    const KEY1: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";
    const ADDR1: &str = "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf";
    const USER1: &str = "0x1111111111111111111111111111111111111111";

    #[test]
    fn aster_roles_derived_from_key_not_field_names() {
        // Sample role layout: signer in subaccount_address, user in wallet_address.
        let body = format!(
            "wallet_name=xemm\nwallet_address={USER1}\nprivate_key={KEY1}\nsubaccount_address={ADDR1}\n"
        );
        let p = write_tmp("aster", &body);
        let c = AsterCreds::load(&p).unwrap();
        assert_eq!(c.user.to_lowercase(), USER1);
        assert_eq!(c.signer.to_lowercase(), ADDR1);
        std::fs::remove_file(p).ok();
    }

    #[test]
    fn aster_rejects_when_no_user_distinct_from_signer() {
        let body = format!("private_key={KEY1}\nwallet_address={ADDR1}\nsubaccount_address={ADDR1}\n");
        let p = write_tmp("aster-bad", &body);
        assert!(AsterCreds::load(&p).is_err());
        std::fs::remove_file(p).ok();
    }

    #[test]
    fn lighter_loads_required_fields_without_printing_secret() {
        let body = "\
API_KEY_PRIVATE_KEY=priv
API_KEY_PUBLIC_KEY=pub
API_KEY_INDEX=2
ACCOUNT_INDEX=42
WALLET_ADDRESS=0xabc
";
        let p = write_tmp("lighter", body);
        let c = LighterCreds::load(&p).unwrap();
        assert_eq!(c.api_private_key, "priv");
        assert_eq!(c.api_public_key, "pub");
        assert_eq!(c.api_key_index, 2);
        assert_eq!(c.account_index, 42);
        assert_eq!(c.wallet_address, "0xabc");
        std::fs::remove_file(p).ok();
    }
}
