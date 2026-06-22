//! API keys and credential source configuration.
//!
//! # Loading precedence (per field, high → low)
//!
//! Same as the rest of [`DeployConfig`](super::DeployConfig): the `config`
//! crate merges sources in registration order; **later sources win**.
//!
//! 1. `OXIDE_ARB__KEYS__PRIVATE_KEY` environment variable
//! 2. `config/oxide-arb.local.toml` under `[keys]` (optional, gitignored)
//! 3. `config/oxide-arb.toml` under `[keys]`
//! 4. Unset (`None`)
//!
//! Polymarket CLOB L2 credentials (`api_key` / `secret` / `passphrase`) are
//! **not** configured here — `ClobClient::connect` derives them from
//! `private_key` via the SDK at runtime.

use serde::Deserialize;
use std::fmt;

/// Environment variable for the bot wallet private key.
pub const ENV_PRIVATE_KEY: &str = "OXIDE_ARB__KEYS__PRIVATE_KEY";

/// Credential source and wallet private key.
///
/// `private_key` may be supplied in `oxide-arb.toml`, `oxide-arb.local.toml`,
/// and/or `OXIDE_ARB__KEYS__PRIVATE_KEY`. Environment variables override file
/// values when present.
#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KeysConfig {
    /// Credential source hint. `env` = inline `private_key` (TOML and/or env);
    /// `keystore` reserved for encrypted keystore file (future).
    pub source: KeySource,
    /// Path to the encrypted keystore file (if `source` is `Keystore`).
    pub keystore_path: Option<String>,
    /// Wallet private key for signing, CLOB L1 auth, and runtime L2 derivation.
    pub private_key: Option<String>,
}

impl fmt::Debug for KeysConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeysConfig")
            .field("source", &self.source)
            .field(
                "keystore_path",
                &self.keystore_path.as_ref().map(|p| format!("{p:?}")),
            )
            .field("private_key", &secret_present(self.private_key.as_ref()))
            .finish()
    }
}

const fn secret_present(value: Option<&String>) -> &'static str {
    if value.is_some() {
        "<redacted>"
    } else {
        "<unset>"
    }
}

impl KeysConfig {
    /// Normalize credential fields after config-crate deserialization.
    ///
    /// Empty strings (e.g. `private_key = ""` in TOML) are treated as unset.
    pub fn normalize(&mut self) {
        empty_to_none(&mut self.private_key);
        empty_to_none(&mut self.keystore_path);
    }

    /// Whether the wallet private key is populated.
    #[must_use]
    pub const fn private_key_present(&self) -> bool {
        self.private_key.is_some()
    }
}

/// Source of cryptographic credentials.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeySource {
    /// Inline `[keys].private_key` (TOML and/or `OXIDE_ARB__KEYS__PRIVATE_KEY`).
    #[default]
    Env,
    /// Encrypted keystore file at `keystore_path` (reserved).
    Keystore,
}

fn empty_to_none(slot: &mut Option<String>) {
    if slot.as_deref().is_some_and(str::is_empty) {
        *slot = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_clears_empty_strings() {
        let mut keys = KeysConfig {
            private_key: Some(String::new()),
            ..KeysConfig::default()
        };
        keys.normalize();
        assert!(!keys.private_key_present());
    }

    #[test]
    fn debug_redacts_secrets() {
        let keys = KeysConfig {
            private_key: Some("0xsecret".into()),
            ..KeysConfig::default()
        };
        let debug = format!("{keys:?}");
        assert!(!debug.contains("0xsecret"));
        assert!(debug.contains("redacted"));
    }

    #[test]
    fn private_key_present_helper() {
        assert!(!KeysConfig::default().private_key_present());
        assert!(
            KeysConfig {
                private_key: Some("0xabc".into()),
                ..KeysConfig::default()
            }
            .private_key_present()
        );
    }
}
