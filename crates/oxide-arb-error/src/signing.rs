//! Cryptographic signing and key management errors.

use thiserror::Error;

/// Errors from key loading, EIP-712 signing, and credential derivation.
#[derive(Debug, Error)]
pub enum SigningError {
    #[error("Invalid private key: {0}")]
    InvalidKey(String),

    #[error("EIP-712 signing failed: {0}")]
    Eip712(String),

    #[error("L2 HMAC derivation failed: {0}")]
    HmacDerivation(String),

    #[error("Key not loaded: {key_source} source unavailable")]
    KeyNotLoaded { key_source: String },

    #[error("Credential expired or invalid: {0}")]
    CredentialInvalid(String),
}
