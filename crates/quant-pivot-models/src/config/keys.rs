//! Wallet credential configuration.
//!
//! The deploy document contains only a typed systemd credential reference.
//! Plaintext in TOML, environment variables, or process arguments is rejected.
//!
//! Polymarket CLOB L2 credentials (`api_key` / `secret` / `passphrase`) are
//! **not** configured here — `ClobClient::connect` derives them from
//! `private_key` via the SDK at runtime.

use serde::Deserialize;

use super::secret::SystemdCredentialRef;

/// Wallet private key.
///
/// `private_key` is a systemd credential name. It is the single credential the
/// process signs with and derives Polymarket CLOB L2 read/write credentials
/// from at connect time.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct KeysConfig {
    /// Wallet private key for signing, CLOB L1 auth, and runtime L2 derivation.
    pub private_key: Option<SystemdCredentialRef>,
}

impl KeysConfig {
    /// Normalize credential fields after config-crate deserialization.
    ///
    /// Empty reference names are treated as unset.
    pub fn normalize(&mut self) {
        if self
            .private_key
            .as_ref()
            .is_some_and(|credential| !credential.is_configured())
        {
            self.private_key = None;
        }
    }

    /// Expose the private key only at the signing boundary.
    #[must_use]
    pub fn private_key(&self) -> Option<&str> {
        self.private_key
            .as_ref()
            .map(|credential| credential.secret().expose_secret())
    }

    /// Whether the wallet private key is populated.
    #[must_use]
    pub const fn private_key_present(&self) -> bool {
        self.private_key.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_clears_empty_strings() {
        let mut keys = KeysConfig {
            private_key: Some(String::new().into()),
        };
        keys.normalize();
        assert!(!keys.private_key_present());
    }

    #[test]
    fn debug_redacts_secrets() {
        let keys = KeysConfig {
            private_key: Some("0xsecret".into()),
        };
        let debug = format!("{keys:?}");
        assert!(!debug.contains("0xsecret"));
        assert!(debug.contains("secret:redacted"));
    }

    #[test]
    fn private_key_present_helper() {
        assert!(!KeysConfig::default().private_key_present());
        assert!(
            KeysConfig {
                private_key: Some("0xabc".into()),
            }
            .private_key_present()
        );
    }
}
