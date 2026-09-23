//! Aster signing uses a cached key and a signer-scoped nonce shared by local processes.

use k256::ecdsa::SigningKey;
use super::crypto;

#[path = "shared_nonce.rs"]
mod shared_nonce;
pub use shared_nonce::AsterNonce;

#[derive(Debug, thiserror::Error)]
pub enum SignError {
    #[error("signing failed: {0}")]
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct Signature(pub String);

pub trait AsterSigner: Send + Sync {
    fn signer_address(&self) -> &str;
    fn user_address(&self) -> &str;
    /// Sign the exact form/query string sent before appending the signature.
    fn sign_v3(&self, encoded_query: &str) -> Result<Signature, SignError>;
}

pub struct EvmAsterSigner {
    user: String,
    signer: String,
    key: SigningKey,
}

impl EvmAsterSigner {
    pub fn new(user: String, signer: String, key: [u8; 32]) -> anyhow::Result<Self> {
        crypto::parse_address(&user)?;
        let address = crypto::parse_address(&signer)?;
        let key = SigningKey::from_slice(&key).map_err(|_| anyhow::anyhow!("invalid Aster private key"))?;
        if crypto::address_from_signing_key(&key) != address {
            anyhow::bail!("aster signer address does not match the private key");
        }
        Ok(Self { user, signer, key })
    }
}

impl AsterSigner for EvmAsterSigner {
    fn signer_address(&self) -> &str { &self.signer }
    fn user_address(&self) -> &str { &self.user }
    fn sign_v3(&self, encoded_query: &str) -> Result<Signature, SignError> {
        crypto::aster_sign_v3_with_key(&self.key, encoded_query)
            .map(Signature).map_err(|e| SignError::Failed(e.to_string()))
    }
}

#[cfg(test)]
pub mod test_support {
    use super::*;

    pub const TEST_KEY: [u8; 32] = {
        let mut key = [0u8; 32];
        key[31] = 1;
        key
    };

    pub struct TestSigner(EvmAsterSigner);
    impl TestSigner {
        pub fn new() -> Self {
            Self(EvmAsterSigner::new(
                "0x1111111111111111111111111111111111111111".into(),
                "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf".into(), TEST_KEY
            ).unwrap())
        }
    }
    impl AsterSigner for TestSigner {
        fn signer_address(&self) -> &str { self.0.signer_address() }
        fn user_address(&self) -> &str { self.0.user_address() }
        fn sign_v3(&self, query: &str) -> Result<Signature, SignError> { self.0.sign_v3(query) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_a_signer_that_does_not_own_the_key() {
        let result = EvmAsterSigner::new(
            "0x1111111111111111111111111111111111111111".into(),
            "0x0000000000000000000000000000000000000001".into(), test_support::TEST_KEY
        );
        assert!(result.is_err());
    }
}
