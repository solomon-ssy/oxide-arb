//! API keys and credential source configuration.

use serde::Deserialize;

/// Credential source and material. Sensitive values must be supplied via
/// `OXIDE_ARB__KEYS__*` environment variables, never the TOML.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KeysConfig {
    /// Where credentials are read from. Default: `env`.
    pub source: KeySource,
    /// Path to the encrypted keystore file (if `source` is `Keystore`).
    pub keystore_path: Option<String>,
    /// Polymarket API key (if `source` is `Env`).
    pub polymarket_api_key: Option<String>,
    /// Polymarket API secret (if `source` is `Env`).
    pub polymarket_api_secret: Option<String>,
    /// Polymarket API passphrase (if `source` is `Env`).
    pub polymarket_passphrase: Option<String>,
    /// Private key for on-chain signing (if `source` is `Env`).
    pub private_key: Option<String>,
}

/// Source of cryptographic credentials.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeySource {
    /// Keys read from environment variables.
    #[default]
    Env,
    /// Keys read from an encrypted keystore file.
    Keystore,
}
