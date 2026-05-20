//! Key management and order signing.

mod credentials;
mod signer;

pub use credentials::L2Credentials;
pub use signer::OrderSigner;

use alloy::primitives::Address;
use oxide_arb_error::signing::SigningError;
use oxide_arb_models::config::KeysConfig;
use zeroize::Zeroizing;

/// Unified keystore for signing and authentication.
///
/// Holds the `OrderSigner` (alloy `LocalSigner`) and optional L2 HMAC
/// credentials for authenticated CLOB access.
pub struct Keystore {
    signer: OrderSigner,
    credentials: Option<L2Credentials>,
}

impl Keystore {
    /// Initialize keystore from configuration.
    ///
    /// Loads the private key from environment (hex-encoded) and
    /// stores L2 HMAC credentials if all three fields are present.
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

        let credentials = match (
            &config.polymarket_api_key,
            &config.polymarket_api_secret,
            &config.polymarket_passphrase,
        ) {
            (Some(key), Some(secret), Some(pass)) => Some(L2Credentials {
                api_key: key.clone(),
                api_secret: secret.clone(),
                passphrase: pass.clone(),
            }),
            _ => None,
        };

        Ok(Self {
            signer,
            credentials,
        })
    }

    /// Get the order signer (for SDK auth and signing).
    pub const fn signer(&self) -> &OrderSigner {
        &self.signer
    }

    /// Get L2 credentials if available.
    pub const fn credentials(&self) -> Option<&L2Credentials> {
        self.credentials.as_ref()
    }

    /// Get the wallet address.
    pub const fn address(&self) -> Address {
        self.signer.address()
    }

    /// Get the wallet address as a checksummed hex string.
    pub fn address_string(&self) -> String {
        self.signer.address_string()
    }
}
