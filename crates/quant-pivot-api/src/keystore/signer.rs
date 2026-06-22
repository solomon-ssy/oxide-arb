//! EIP-712 order signing via alloy `PrivateKeySigner`.

use alloy::{
    primitives::Address,
    signers::{Signer, local::PrivateKeySigner},
};
use quant_pivot_error::signing::SigningError;

/// Order signer backed by an alloy `PrivateKeySigner` (secp256k1).
///
/// Exposes the inner signer for SDK authentication flows.
pub struct OrderSigner {
    signer: PrivateKeySigner,
}

impl OrderSigner {
    /// Create from raw key bytes (32 bytes for secp256k1).
    pub fn from_bytes(key_bytes: &[u8]) -> Result<Self, SigningError> {
        if key_bytes.len() != 32 {
            return Err(SigningError::InvalidKey(format!(
                "Expected 32 bytes, got {}",
                key_bytes.len()
            )));
        }

        let signer = PrivateKeySigner::from_slice(key_bytes)
            .map_err(|e| SigningError::InvalidKey(e.to_string()))?;

        Ok(Self { signer })
    }

    /// Attach a chain ID to the inner signer (required for CLOB EIP-712 signing).
    #[must_use]
    pub fn with_chain_id(mut self, chain_id: Option<u64>) -> Self {
        self.signer.set_chain_id(chain_id);
        self
    }

    /// Get the Ethereum address derived from the signing key.
    pub const fn address(&self) -> Address {
        self.signer.address()
    }

    /// Get the address as a checksummed hex string.
    pub fn address_string(&self) -> String {
        self.signer.address().to_checksum(None)
    }

    /// Expose the inner `PrivateKeySigner` for SDK auth builder integration.
    ///
    /// The SDK's `authentication_builder` requires a `&impl alloy::signers::Signer`.
    pub const fn inner(&self) -> &PrivateKeySigner {
        &self.signer
    }
}
