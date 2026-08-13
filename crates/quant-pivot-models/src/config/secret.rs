//! In-memory deploy secret value with non-leaking formatting semantics.

use std::{
    borrow::Cow,
    fmt::{Debug, Formatter, Result as FmtResult},
};

use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
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

impl JsonSchema for SecretText {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("SecretText")
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        schemars::json_schema!({
            "type": "string",
            "writeOnly": true,
            "description": "A zeroizing plaintext deploy secret. Read projections expose only configured or missing state."
        })
    }
}

/// Serialize a secret-bearing field as an empty template value.
pub(super) fn serialize_empty<S>(_: &SecretText, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str("")
}

/// Serialize an optional secret without exposing its plaintext.
pub(super) fn serialize_optional_empty<S, T>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: OptionalSecretPresence,
{
    if value.is_configured() {
        serializer.serialize_some("")
    } else {
        serializer.serialize_none()
    }
}

pub(super) trait OptionalSecretPresence {
    fn is_configured(&self) -> bool;
}

impl OptionalSecretPresence for Option<SecretText> {
    fn is_configured(&self) -> bool {
        self.is_some()
    }
}

/// Serialize a secret history as equally sized empty template entries.
pub(super) fn serialize_vec_empty<S>(value: &[SecretText], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    value
        .iter()
        .map(|_| "")
        .collect::<Vec<_>>()
        .serialize(serializer)
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
