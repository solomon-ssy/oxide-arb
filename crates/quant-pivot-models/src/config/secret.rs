//! In-memory deploy secret value with non-leaking formatting semantics.

use std::fmt::{Debug, Formatter, Result as FmtResult};

use serde::{Deserialize, Deserializer};
use zeroize::Zeroizing;

/// Plaintext secret loaded directly from deploy configuration.
///
/// The value is zeroized on drop and deliberately implements neither
/// `Display` nor `Serialize`. `Debug` reports only whether it is configured.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SecretText(Zeroizing<String>);

impl SecretText {
    /// Access plaintext only at the adapter that owns the credential boundary.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Debug for SecretText {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter.write_str(if self.is_empty() {
            "<secret:unset>"
        } else {
            "<secret:redacted>"
        })
    }
}

impl<'de> Deserialize<'de> for SecretText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::from)
    }
}

impl From<String> for SecretText {
    fn from(value: String) -> Self {
        Self(Zeroizing::new(value))
    }
}

impl From<&str> for SecretText {
    fn from(value: &str) -> Self {
        Self::from(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::SecretText;

    #[test]
    fn plaintext_deserializes_never_exposes() {
        let secret: SecretText =
            serde_json::from_str("\"correct horse battery staple\"").expect("deserialize secret");
        assert_eq!(secret.expose_secret(), "correct horse battery staple");
        let debug = format!("{secret:?}");
        assert!(!debug.contains("correct horse"));
        assert_eq!(debug, "<secret:redacted>");
    }
}
