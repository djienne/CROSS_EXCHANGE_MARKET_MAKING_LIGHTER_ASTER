//! Cryptographic primitives for live-order signing (plan §4.1/§4.3). Pure functions, no I/O,
//! no async — the hot path calls these microsecond-scale operations off the book-build path.
//!
//! Two venue signing schemes are implemented here and validated byte-for-byte by golden tests
//! against live-confirmed oracles (`scripts/aster_probe.py golden` + the Python EIP-712 oracle):
//!
//! - **Aster V3** ([`aster_sign_v3`]): ABI-encode `(string json, address user, address signer,
//!   uint256 nonce)` → keccak256 → EIP-191 `personal_sign` of the **decoded 32-byte digest** →
//!   65-byte `r||s||v` hex. (Confirmed live: a real place+cancel returned HTTP 200.)
//! - **Hyperliquid L1** ([`hl_sign`]): keccak256 of `msgpack(action) || nonce_be8 ||
//!   vault_marker || expires_marker` → EIP-712 phantom-`Agent` digest (domain
//!   `Exchange`/v1/chainId **1337**, `verifyingContract` 0x0) → `{r,s,v}`. The vault marker is
//!   `0x01 ++ addr20` when a sub-account is used (the reference impl we ported from omitted the
//!   address bytes because it always passed `vault=None`; we need them — see [`hl_action_hash`]).
//!
//! secp256k1 signing is k256's deterministic-RFC6979 + low-S normalization, matching
//! `eth_account`/ethers (so `v ∈ {27,28}`).

use anyhow::{bail, Result};
use k256::ecdsa::SigningKey;
use tiny_keccak::{Hasher, Keccak};

/// keccak256 (the Ethereum hash, NOT SHA3-256).
pub fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut k = Keccak::v256();
    let mut out = [0u8; 32];
    k.update(data);
    k.finalize(&mut out);
    out
}

/// Parse a `0x`-prefixed (or bare) 32-byte hex private key.
pub fn parse_priv_key(s: &str) -> Result<[u8; 32]> {
    let s = s.trim().strip_prefix("0x").unwrap_or(s.trim());
    let bytes = hex::decode(s).map_err(|e| anyhow::anyhow!("private key not hex: {e}"))?;
    if bytes.len() != 32 {
        bail!("private key must be 32 bytes, got {}", bytes.len());
    }
    let mut k = [0u8; 32];
    k.copy_from_slice(&bytes);
    Ok(k)
}

/// Parse a `0x`-prefixed (or bare) 20-byte hex address into raw bytes.
pub fn parse_address(s: &str) -> Result<[u8; 20]> {
    let s = s.trim().strip_prefix("0x").unwrap_or(s.trim());
    let bytes = hex::decode(s).map_err(|e| anyhow::anyhow!("address not hex: {e}"))?;
    if bytes.len() != 20 {
        bail!("address must be 20 bytes, got {}", bytes.len());
    }
    let mut a = [0u8; 20];
    a.copy_from_slice(&bytes);
    Ok(a)
}

/// The lowercase `0x`-hex form of a 20-byte address.
pub fn address_hex(addr: &[u8; 20]) -> String {
    format!("0x{}", hex::encode(addr))
}

/// The Ethereum address of a private key: last 20 bytes of `keccak256(uncompressed_pubkey[1..])`.
pub fn address_from_priv(key: &[u8; 32]) -> Result<[u8; 20]> {
    let sk = SigningKey::from_slice(key).map_err(|e| anyhow::anyhow!("bad private key: {e}"))?;
    let vk = sk.verifying_key();
    let point = vk.to_encoded_point(false); // 0x04 || X(32) || Y(32)
    let hash = keccak256(&point.as_bytes()[1..]);
    let mut a = [0u8; 20];
    a.copy_from_slice(&hash[12..]);
    Ok(a)
}

/// secp256k1 recoverable sign of a 32-byte prehash. Returns `(r, s, v)` with `v ∈ {27, 28}`
/// (Ethereum convention: `27 + recovery_id`). Low-S normalized, like `eth_account`.
pub fn sign_recoverable(key: &[u8; 32], prehash: &[u8; 32]) -> Result<([u8; 32], [u8; 32], u8)> {
    let sk = SigningKey::from_slice(key).map_err(|e| anyhow::anyhow!("bad private key: {e}"))?;
    let (sig, recid) = sk
        .sign_prehash_recoverable(prehash)
        .map_err(|e| anyhow::anyhow!("secp256k1 sign failed: {e}"))?;
    let mut r = [0u8; 32];
    let mut s = [0u8; 32];
    r.copy_from_slice(&sig.r().to_bytes());
    s.copy_from_slice(&sig.s().to_bytes());
    let v = 27 + recid.to_byte();
    Ok((r, s, v))
}

/// EIP-191 `personal_sign` digest over a 32-byte payload:
/// `keccak256("\x19Ethereum Signed Message:\n32" || payload)`. NOTE the payload is the raw
/// 32 digest bytes, not their hex text (a classic port bug).
pub fn eip191_digest(payload: &[u8; 32]) -> [u8; 32] {
    let mut buf = Vec::with_capacity(28 + 32);
    buf.extend_from_slice(b"\x19Ethereum Signed Message:\n32");
    buf.extend_from_slice(payload);
    keccak256(&buf)
}

// ---------------------------------------------------------------------------------------------
// Aster V3 — ABI-encode + EIP-191
// ---------------------------------------------------------------------------------------------

fn u256_be(n: u128) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[16..].copy_from_slice(&n.to_be_bytes());
    w
}

fn addr_word(a: &[u8; 20]) -> [u8; 32] {
    let mut w = [0u8; 32];
    w[12..].copy_from_slice(a);
    w
}

/// Solidity ABI-encode the fixed 4-tuple `(string json, address user, address signer,
/// uint256 nonce)` exactly as `eth_abi.encode(["string","address","address","uint256"], …)`
/// does: a 4-word head (string offset 0x80, user, signer, nonce) then the string tail
/// (length word + UTF-8 bytes zero-padded to a 32-byte multiple).
pub fn abi_encode_aster(json: &str, user: &[u8; 20], signer: &[u8; 20], nonce: u64) -> Vec<u8> {
    let bytes = json.as_bytes();
    let mut out = Vec::with_capacity(128 + 32 + bytes.len() + 32);
    out.extend_from_slice(&u256_be(128)); // offset to the dynamic string = 4*32
    out.extend_from_slice(&addr_word(user));
    out.extend_from_slice(&addr_word(signer));
    out.extend_from_slice(&u256_be(nonce as u128));
    out.extend_from_slice(&u256_be(bytes.len() as u128));
    out.extend_from_slice(bytes);
    let pad = (32 - (bytes.len() % 32)) % 32;
    out.resize(out.len() + pad, 0); // zero-pad the string tail to a 32-byte multiple
    out
}

/// The Aster V3 signing digest: `keccak256(abi_encode(json, user, signer, nonce))`. This is
/// what `Web3.keccak(encoded)` produces before the EIP-191 wrap (golden: `0x356ed6…`).
pub fn aster_digest(json: &str, user: &[u8; 20], signer: &[u8; 20], nonce: u64) -> [u8; 32] {
    keccak256(&abi_encode_aster(json, user, signer, nonce))
}

/// Full Aster V3 signature for the canonical `json_str` of trimmed params: returns the
/// `0x`-prefixed 65-byte `r||s||v` hex to attach as the `signature` request field.
pub fn aster_sign_v3(
    key: &[u8; 32],
    user: &[u8; 20],
    signer: &[u8; 20],
    json: &str,
    nonce: u64,
) -> Result<String> {
    let digest = aster_digest(json, user, signer, nonce);
    let eip191 = eip191_digest(&digest);
    let (r, s, v) = sign_recoverable(key, &eip191)?;
    let mut sig = Vec::with_capacity(65);
    sig.extend_from_slice(&r);
    sig.extend_from_slice(&s);
    sig.push(v);
    Ok(format!("0x{}", hex::encode(sig)))
}

// ---------------------------------------------------------------------------------------------
// Hyperliquid L1 — msgpack action hash + EIP-712 phantom agent
// ---------------------------------------------------------------------------------------------

/// The HL action hash (a.k.a. `connectionId`):
/// `keccak256(msgpack_named(action) || nonce.to_be_bytes(8) || vault_marker || expires_marker)`
/// where `vault_marker = 0x01 ++ addr20` (sub-account) or `0x00` (none), and `expires_marker =
/// 0x00 ++ value.to_be_bytes(8)` when an `expiresAfter` is set, else nothing.
pub fn hl_action_hash(
    msgpack: &[u8],
    nonce: u64,
    vault: Option<&[u8; 20]>,
    expires_after: Option<u64>,
) -> [u8; 32] {
    let mut data = Vec::with_capacity(msgpack.len() + 64);
    data.extend_from_slice(msgpack);
    data.extend_from_slice(&nonce.to_be_bytes());
    match vault {
        Some(addr) => {
            data.push(0x01);
            data.extend_from_slice(addr);
        }
        None => data.push(0x00),
    }
    if let Some(exp) = expires_after {
        data.push(0x00);
        data.extend_from_slice(&exp.to_be_bytes());
    }
    keccak256(&data)
}

/// The EIP-712 domain separator for the HL `Exchange` domain (name "Exchange", version "1",
/// chainId **1337** always, verifyingContract `address(0)`). Constant; computed once.
pub fn hl_domain_separator() -> [u8; 32] {
    let type_hash =
        keccak256(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");
    let name_hash = keccak256(b"Exchange");
    let version_hash = keccak256(b"1");
    let mut chain_id = [0u8; 32];
    chain_id[24..].copy_from_slice(&1337u64.to_be_bytes());
    let verifying_contract = [0u8; 32];
    let mut enc = Vec::with_capacity(160);
    enc.extend_from_slice(&type_hash);
    enc.extend_from_slice(&name_hash);
    enc.extend_from_slice(&version_hash);
    enc.extend_from_slice(&chain_id);
    enc.extend_from_slice(&verifying_contract);
    keccak256(&enc)
}

/// The EIP-712 phantom-`Agent` digest for a connection id. `source` is `"a"` (mainnet) or
/// `"b"` (testnet). `digest = keccak256(0x1901 || domainSeparator || structHash)` where
/// `structHash = keccak256(typeHash("Agent(string source,bytes32 connectionId)") ||
/// keccak256(source) || connectionId)`.
pub fn hl_agent_digest(connection_id: &[u8; 32], is_mainnet: bool) -> [u8; 32] {
    let domain_sep = hl_domain_separator();
    let agent_type_hash = keccak256(b"Agent(string source,bytes32 connectionId)");
    let source_hash = keccak256(if is_mainnet { b"a" } else { b"b" });
    let mut struct_enc = Vec::with_capacity(96);
    struct_enc.extend_from_slice(&agent_type_hash);
    struct_enc.extend_from_slice(&source_hash);
    struct_enc.extend_from_slice(connection_id);
    let struct_hash = keccak256(&struct_enc);
    let mut digest_input = Vec::with_capacity(66);
    digest_input.push(0x19);
    digest_input.push(0x01);
    digest_input.extend_from_slice(&domain_sep);
    digest_input.extend_from_slice(&struct_hash);
    keccak256(&digest_input)
}

/// An HL wire signature `{r, s, v}` (r/s as `0x`-hex, v as 27/28).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvmSignature {
    pub r: String,
    pub s: String,
    pub v: u8,
}

/// Sign an HL L1 action by its connection id (action hash). Returns the `{r,s,v}` for the
/// `/exchange` envelope's `signature` object.
pub fn hl_sign(key: &[u8; 32], connection_id: &[u8; 32], is_mainnet: bool) -> Result<EvmSignature> {
    let digest = hl_agent_digest(connection_id, is_mainnet);
    let (r, s, v) = sign_recoverable(key, &digest)?;
    Ok(EvmSignature {
        r: format!("0x{}", hex::encode(r)),
        s: format!("0x{}", hex::encode(s)),
        v,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    // The well-known test key 0x…01 → address 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf.
    const KEY1: [u8; 32] = {
        let mut k = [0u8; 32];
        k[31] = 1;
        k
    };

    fn hexstr(b: &[u8]) -> String {
        format!("0x{}", hex::encode(b))
    }

    #[test]
    fn address_from_key_matches_known() {
        let a = address_from_priv(&KEY1).unwrap();
        assert_eq!(address_hex(&a), "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf");
    }

    // ---- Aster golden vector (byte-exact vs scripts/aster_probe.py golden, confirmed live) ----
    #[test]
    fn aster_golden_vector_byte_exact() {
        let user = parse_address("0x062903894bce55d4f80ee5931c46c77cd7881351").unwrap();
        let signer = parse_address("0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf").unwrap();
        let json = "{\"price\":\"40.0\",\"quantity\":\"0.3\",\"recvWindow\":\"50000\",\"side\":\"BUY\",\"symbol\":\"HYPEUSDT\",\"timeInForce\":\"GTX\",\"timestamp\":\"1700000000000\",\"type\":\"LIMIT\"}";
        let nonce: u64 = 1_700_000_000_000_000;
        // The abi+keccak digest matches Web3.keccak(encoded).
        let digest = aster_digest(json, &user, &signer, nonce);
        assert_eq!(hexstr(&digest), "0x356ed606bef291597ca9b9d03b9e7efaef1d30dd8621ec0c952e542dd3dd0ecd");
        // The full EIP-191 signature byte-matches the Python oracle.
        let sig = aster_sign_v3(&KEY1, &user, &signer, json, nonce).unwrap();
        assert_eq!(sig, "0x89e4e500a9a37a27e4a3ea2e726d02c10ca1661bcf708b19c32f9d696ed346ea7cebfb4861c2c1dd953ff0333ea9626f0f69944cfbd8071ca19331a86711a01e1c");
    }

    // ---- Hyperliquid golden vectors (vs the Python EIP-712 + msgpack oracle) ----
    #[test]
    fn hl_domain_separator_matches_oracle() {
        assert_eq!(
            hexstr(&hl_domain_separator()),
            "0xd79297fcdf2ffcd4ae223d01edaa2ba214ff8f401d7c9300d995d17c82aa4040"
        );
    }

    #[test]
    fn hl_agent_digest_matches_oracle() {
        let mut cid = [0u8; 32];
        for (i, b) in cid.iter_mut().enumerate() {
            *b = i as u8; // 0x000102…1f
        }
        assert_eq!(
            hexstr(&hl_agent_digest(&cid, true)),
            "0xb9e7c81cff512fa0969928e37d7c2475af657f1b314b7458c8dd7a023044cac0"
        );
        assert_eq!(
            hexstr(&hl_agent_digest(&cid, false)),
            "0x4384ea9179d358ab65dd0375834d2e206330bd138f8e05c763d97f6e3f54bac1"
        );
    }

    #[test]
    fn hl_sign_matches_oracle() {
        let mut cid = [0u8; 32];
        for (i, b) in cid.iter_mut().enumerate() {
            *b = i as u8;
        }
        let sig = hl_sign(&KEY1, &cid, true).unwrap();
        assert_eq!(sig.r, "0x6fac96b099be7b8cdf3bc5ccec1b7966b2543f97941734f89f106b3f18e28d3b");
        assert_eq!(sig.s, "0x1ab95dff17bf2a60a06d9471d504e174cabc3496da75c50c04fe21ddb30f13ef");
        assert_eq!(sig.v, 28);
    }

    // The HL action wire structs (declaration order = msgpack field order: a,b,p,s,r,t,c).
    #[derive(Serialize)]
    struct TLimit {
        tif: String,
    }
    #[derive(Serialize)]
    struct TType {
        limit: TLimit,
    }
    #[derive(Serialize)]
    struct TOrder {
        a: u32,
        b: bool,
        p: String,
        s: String,
        r: bool,
        t: TType,
        #[serde(skip_serializing_if = "Option::is_none")]
        c: Option<String>,
    }
    #[derive(Serialize)]
    struct TAction {
        #[serde(rename = "type")]
        type_: String,
        orders: Vec<TOrder>,
        grouping: String,
    }

    #[test]
    fn hl_msgpack_and_action_hash_match_oracle() {
        let action = TAction {
            type_: "order".into(),
            orders: vec![TOrder {
                a: 5,
                b: true,
                p: "123.45".into(),
                s: "0.5".into(),
                r: false,
                t: TType { limit: TLimit { tif: "Ioc".into() } },
                c: Some("0x000102030405060708090a0b0c0d0e0f".into()),
            }],
            grouping: "na".into(),
        };
        let packed = rmp_serde::to_vec_named(&action).unwrap();
        // Byte-exact vs Python msgpack.packb (validates field order + compact int/bool encoding).
        assert_eq!(
            hexstr(&packed),
            "0x83a474797065a56f72646572a66f72646572739187a16105a162c3a170a63132332e3435a173a3302e35a172c2a17481a56c696d697481a3746966a3496f63a163d92230783030303130323033303430353036303730383039306130623063306430653066a867726f7570696e67a26e61"
        );
        let hash = hl_action_hash(&packed, 1_234_567, None, None);
        assert_eq!(hexstr(&hash), "0xdc76e4ee96d5ed33eae5fc25d0c3efd2ae108782b741627fd794be7b006eeba5");
        // With a sub-account vault (0x11..11) and with vault+expiresAfter (the real hedge path).
        let vault = [0x11u8; 20];
        assert_eq!(
            hexstr(&hl_action_hash(&packed, 1_234_567, Some(&vault), None)),
            "0xc9faf441009cc3eca3112b510bd4bde8b1643edddcdd869cabed0d86d5468f88"
        );
        assert_eq!(
            hexstr(&hl_action_hash(&packed, 1_234_567, Some(&vault), Some(9_999_999))),
            "0xe82fb2a10383cb80986147f11cb09de79436a1dd8c976f3ab29bf64678b8836f"
        );
    }

    #[test]
    fn eip191_prefixes_decoded_digest() {
        // Sanity: the prefix is over the 32 raw bytes, length literal "32".
        let payload = [0xABu8; 32];
        let d = eip191_digest(&payload);
        let mut manual = Vec::new();
        manual.extend_from_slice(b"\x19Ethereum Signed Message:\n32");
        manual.extend_from_slice(&payload);
        assert_eq!(d, keccak256(&manual));
    }

    #[test]
    fn vault_marker_changes_action_hash() {
        let packed = b"dummy-action";
        let no_vault = hl_action_hash(packed, 1, None, None);
        let vault = [0x11u8; 20];
        let with_vault = hl_action_hash(packed, 1, Some(&vault), None);
        assert_ne!(no_vault, with_vault, "vault address must fold into the hash");
        // expiresAfter must also change it.
        let with_exp = hl_action_hash(packed, 1, Some(&vault), Some(1000));
        assert_ne!(with_vault, with_exp);
    }
}
