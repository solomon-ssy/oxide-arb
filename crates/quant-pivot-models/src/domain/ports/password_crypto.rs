//! Credential hashing dependency-inversion boundary.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

/// Bounded asynchronous password hashing and verification service.
#[async_trait]
pub trait PasswordCryptoPort: Send + Sync {
    /// Hash a new plaintext credential without blocking an async runtime worker.
    async fn hash(&self, plaintext: String) -> QuantResult<String>;

    /// Verify a plaintext credential against an optional stored PHC string.
    /// Implementations must perform equivalent password work when the stored
    /// hash is absent so username existence is not exposed through timing.
    async fn verify(&self, plaintext: String, stored_hash: Option<String>) -> QuantResult<bool>;
}
