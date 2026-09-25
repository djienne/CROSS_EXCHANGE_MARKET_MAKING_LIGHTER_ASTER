//! Aster V3 EIP-712 signing of the exact transmitted query string.
//! Public contract: AsterSignTransaction / version 1 / mainnet chain 1666.
//! Offline vectors use eth-account 0.13.7; they do not assert live venue acceptance.

use anyhow::{bail, Result};
use k256::ecdsa::{RecoveryId, Signature, SigningKey, VerifyingKey};
use tiny_keccak::{Hasher, Keccak};

pub fn keccak256(data: &[u8]) -> [u8; 32] {
    let mut hash = Keccak::v256();
    let mut out = [0u8; 32];
    hash.update(data);
    hash.finalize(&mut out);
    out
}

pub fn parse_priv_key(value: &str) -> Result<[u8; 32]> {
    let value = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    let bytes = hex::decode(value).map_err(|_| anyhow::anyhow!("private key is not hexadecimal"))?;
    bytes.try_into().map_err(|v: Vec<u8>| anyhow::anyhow!("private key must be 32 bytes, got {}", v.len()))
}

pub fn parse_address(value: &str) -> Result<[u8; 20]> {
    let value = value.trim().strip_prefix("0x").unwrap_or(value.trim());
    let bytes = hex::decode(value).map_err(|e| anyhow::anyhow!("address not hex: {e}"))?;
    bytes.try_into().map_err(|v: Vec<u8>| anyhow::anyhow!("address must be 20 bytes, got {}", v.len()))
}

pub fn address_hex(address: &[u8; 20]) -> String {
    format!("0x{}", hex::encode(address))
}

pub fn address_from_priv(key: &[u8; 32]) -> Result<[u8; 20]> {
    let key = SigningKey::from_slice(key).map_err(|_| anyhow::anyhow!("invalid private key"))?;
    Ok(address_from_signing_key(&key))
}

pub fn address_from_signing_key(key: &SigningKey) -> [u8; 20] {
    address_of(key.verifying_key())
}

fn address_of(key: &VerifyingKey) -> [u8; 20] {
    let point = key.to_encoded_point(false);
    let hash = keccak256(&point.as_bytes()[1..]);
    let mut address = [0u8; 20];
    address.copy_from_slice(&hash[12..]);
    address
}

pub fn aster_domain_separator() -> [u8; 32] {
    let mut encoded = [0u8; 160];
    encoded[..32].copy_from_slice(&keccak256(
        b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"
    ));
    encoded[32..64].copy_from_slice(&keccak256(b"AsterSignTransaction"));
    encoded[64..96].copy_from_slice(&keccak256(b"1"));
    encoded[120..128].copy_from_slice(&1666u64.to_be_bytes());
    // verifyingContract is the zero address.
    keccak256(&encoded)
}

pub fn aster_digest(encoded_query: &str) -> [u8; 32] {
    let mut message = [0u8; 64];
    message[..32].copy_from_slice(&keccak256(b"Message(string msg)"));
    message[32..].copy_from_slice(&keccak256(encoded_query.as_bytes()));
    let mut encoded = [0u8; 66];
    encoded[..2].copy_from_slice(&[0x19, 0x01]);
    encoded[2..34].copy_from_slice(&aster_domain_separator());
    encoded[34..].copy_from_slice(&keccak256(&message));
    keccak256(&encoded)
}

pub fn aster_sign_v3_with_key(key: &SigningKey, encoded_query: &str) -> Result<String> {
    if encoded_query.is_empty() {
        bail!("cannot sign an empty Aster request");
    }
    let (signature, recovery_id) = key.sign_prehash_recoverable(&aster_digest(encoded_query))
        .map_err(|e| anyhow::anyhow!("secp256k1 signing failed: {e}"))?;
    let mut bytes = [0u8; 65];
    bytes[..64].copy_from_slice(&signature.to_bytes());
    bytes[64] = 27 + recovery_id.to_byte();
    Ok(format!("0x{}", hex::encode(bytes)))
}

/// The address that signed `encoded_query` with `signature` (`0x` r‖s‖v): what the venue
/// compares with the request's `signer`.
pub fn aster_recover_signer(encoded_query: &str, signature: &str) -> Result<[u8; 20]> {
    let bytes = hex::decode(signature.trim().trim_start_matches("0x"))
        .map_err(|_| anyhow::anyhow!("signature is not hexadecimal"))?;
    if bytes.len() != 65 {
        bail!("signature must be 65 bytes, got {}", bytes.len());
    }
    let signature = Signature::from_slice(&bytes[..64]).map_err(|e| anyhow::anyhow!("malformed signature: {e}"))?;
    let recovery_id = RecoveryId::from_byte(bytes[64].wrapping_sub(27))
        .ok_or_else(|| anyhow::anyhow!("signature recovery byte must be 27 or 28"))?;
    let key = VerifyingKey::recover_from_prehash(&aster_digest(encoded_query), &signature, recovery_id)
        .map_err(|e| anyhow::anyhow!("signature does not recover a key: {e}"))?;
    Ok(address_of(&key))
}

#[cfg(test)]
mod tests {
    use super::*;

    const QUERY: &str = "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=IOC&quantity=0.5&price=100.25&nonce=1700000000000000&user=0x1111111111111111111111111111111111111111&signer=0x7e5f4552091a69125d5dfcb7b8c2659029395bdf";

    #[test]
    fn current_aster_eip712_matches_independent_eth_account_vector() {
        // eth_account.messages.encode_typed_data, chainId=1666, fixed private key 1.
        // Specification: asterdex.github.io/aster-api-website/asterCode/authentication/
        let mut raw_key = [0u8; 32];
        raw_key[31] = 1;
        let key = SigningKey::from_slice(&raw_key).unwrap();
        assert_eq!(address_hex(&address_from_signing_key(&key)),
            "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf");
        assert_eq!(hex::encode(aster_domain_separator()),
            "a95d0a7a6f3f17fcebb0c2336645385ce79cde7523f71ab147fdb7f15e9f37f9");
        assert_eq!(hex::encode(aster_digest(QUERY)),
            "4414058dff74d1ef9262b117c76149683a285e43f35e80e1cf33bfeb0996ee17");
        assert_eq!(aster_sign_v3_with_key(&key, QUERY).unwrap(),
            "0x16fc22851e7ea6821868690c16116d6c50266c0cf681ea9eeaabc5e2c5ca697b2deebb413c9fe24013ff1fabc677f8a7dae0b5a90ae2684b99a8022926c3a3dc1c");
        assert_ne!(aster_digest(QUERY), aster_digest(&QUERY.replace("price=100.25", "price=100.26")));
        let signature = aster_sign_v3_with_key(&key, QUERY).unwrap();
        assert_eq!(aster_recover_signer(QUERY, &signature).unwrap(), address_from_signing_key(&key));
        let tampered = QUERY.replace("price=100.25", "price=100.26");
        assert_ne!(aster_recover_signer(&tampered, &signature).unwrap(), address_from_signing_key(&key));
    }
}
