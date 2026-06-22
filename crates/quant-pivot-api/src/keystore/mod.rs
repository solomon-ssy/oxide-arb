//! Key management and order signing.

mod signer;

pub use signer::OrderSigner;

use alloy::primitives::Address;
use quant_pivot_error::signing::SigningError;
use quant_pivot_models::config::KeysConfig;
use std::sync::Arc;
use zeroize::Zeroizing;

/// Unified keystore for signing and CLOB authentication.
///
/// Holds the `OrderSigner` (alloy `PrivateKeySigner`). Polymarket L2 HMAC
/// credentials are derived by the SDK during [`crate::clob::ClobClient::connect`].
pub struct Keystore {
    signer: Arc<OrderSigner>,
}

impl Keystore {
    /// Initialize keystore from configuration.
    ///
    /// Loads the hex-encoded `private_key` from [`KeysConfig`].
    pub fn from_config(config: &KeysConfig) -> Result<Self, SigningError> {
        let private_key_hex =
            config
                .private_key
                .as_deref()
                .ok_or_else(|| SigningError::KeyNotLoaded {
                    key_source: "env".into(),
                })?;

        let key_bytes = Zeroizing::new(
            hex::decode(
                private_key_hex
                    .strip_prefix("0x")
                    .unwrap_or(private_key_hex),
            )
            .map_err(|e| SigningError::InvalidKey(e.to_string()))?,
        );

        let signer = OrderSigner::from_bytes(&key_bytes)?;

        Ok(Self {
            signer: Arc::new(signer),
        })
    }

    /// Get the order signer (for SDK auth and signing).
    pub fn signer(&self) -> &OrderSigner {
        &self.signer
    }

    /// Shared signer handle for [`crate::clob::ClobClient::connect`].
    pub fn signer_arc(&self) -> Arc<OrderSigner> {
        Arc::clone(&self.signer)
    }

    /// Get the wallet address.
    pub fn address(&self) -> Address {
        self.signer.address()
    }

    /// Get the wallet address as a checksummed hex string.
    pub fn address_string(&self) -> String {
        self.signer.address_string()
    }
}
