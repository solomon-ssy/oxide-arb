//! In-memory deploy secret value with non-leaking formatting semantics.

use std::fmt;

use serde::{Deserialize, Deserializer};
use zeroize::Zeroizing;

/// Secret configuration text.
///
/// The plaintext is zeroized on drop and is deliberately neither `Display`
/// nor `Serialize`. `Debug` exposes only whether a value is configured.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SecretText(Zeroizing<String>);

impl SecretText {
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    fn debug_never_exposes_plaintext() {
        let secret = SecretText::from("correct horse battery staple");
        let debug = format!("{secret:?}");
        assert!(!debug.contains("correct horse"));
        assert_eq!(debug, "<secret:redacted>");
    }
}
