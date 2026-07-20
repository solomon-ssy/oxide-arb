//! Strongly typed JSONB documents at persistence boundaries.
//!
//! Internal documents are stored as a versioned, discriminated envelope. A
//! mismatched discriminator or schema version is rejected while `SeaORM` decodes
//! the row; callers never receive an unclassified `serde_json::Value` from an
//! internal JSONB column. Only explicitly external payload boundaries use
//! [`ExternalJsonDocument`].

use std::{collections::BTreeMap, ops::Deref};

use sea_orm::FromJsonQueryResult;
use serde::{Deserialize, Serialize};

const MAX_OPERATION_DETAIL_BYTES: usize = 64 * 1024;
const MAX_OPERATION_DETAIL_DEPTH: usize = 8;
const MAX_OPERATION_DETAIL_NODES: usize = 512;
const SENSITIVE_DETAIL_KEYS: [&str; 10] = [
    "access_token",
    "authorization",
    "cookie",
    "credential",
    "jwt",
    "password",
    "private_key",
    "refresh_token",
    "secret",
    "set-cookie",
];

/// Rejected general-operation audit detail.
#[derive(Debug, thiserror::Error)]
pub enum OperationDetailError {
    #[error("operation detail must be a JSON object")]
    NotObject,
    #[error("operation detail exceeds {MAX_OPERATION_DETAIL_BYTES} bytes")]
    TooLarge,
    #[error("operation detail exceeds maximum depth {MAX_OPERATION_DETAIL_DEPTH}")]
    TooDeep,
    #[error("operation detail exceeds maximum node count {MAX_OPERATION_DETAIL_NODES}")]
    TooManyNodes,
    #[error("operation detail contains forbidden sensitive key `{0}`")]
    SensitiveKey(String),
    #[error("operation detail serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Open-world, redacted forensic summary for the cross-domain operation log.
///
/// This is intentionally not an action-specific tagged enum: the document is
/// non-authoritative and only displayed as a whole. The type instead enforces
/// the invariants common to every writer: object shape, bounded complexity and
/// rejection of credential-bearing keys.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct OperationDetailDocument(BTreeMap<String, serde_json::Value>);

impl OperationDetailDocument {
    #[must_use]
    pub const fn empty() -> Self {
        Self(BTreeMap::new())
    }

    pub fn from_serializable<T: Serialize>(value: &T) -> Result<Self, OperationDetailError> {
        Self::try_from(serde_json::to_value(value)?)
    }
}

impl TryFrom<serde_json::Value> for OperationDetailDocument {
    type Error = OperationDetailError;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        if serde_json::to_vec(&value)?.len() > MAX_OPERATION_DETAIL_BYTES {
            return Err(OperationDetailError::TooLarge);
        }
        let mut node_count = 0;
        validate_operation_detail_value(&value, 0, &mut node_count)?;
        let serde_json::Value::Object(values) = value else {
            return Err(OperationDetailError::NotObject);
        };
        Ok(Self(values.into_iter().collect()))
    }
}

impl<'de> Deserialize<'de> for OperationDetailDocument {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::try_from(value).map_err(serde::de::Error::custom)
    }
}

fn validate_operation_detail_value(
    value: &serde_json::Value,
    depth: usize,
    node_count: &mut usize,
) -> Result<(), OperationDetailError> {
    if depth > MAX_OPERATION_DETAIL_DEPTH {
        return Err(OperationDetailError::TooDeep);
    }
    *node_count += 1;
    if *node_count > MAX_OPERATION_DETAIL_NODES {
        return Err(OperationDetailError::TooManyNodes);
    }
    match value {
        serde_json::Value::Object(values) => {
            for (key, child) in values {
                if SENSITIVE_DETAIL_KEYS
                    .iter()
                    .any(|sensitive| key.eq_ignore_ascii_case(sensitive))
                {
                    return Err(OperationDetailError::SensitiveKey(key.clone()));
                }
                validate_operation_detail_value(child, depth + 1, node_count)?;
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                validate_operation_detail_value(child, depth + 1, node_count)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Opaque JSON received from an external system and retained byte-semantically.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(transparent)]
pub struct ExternalJsonDocument(serde_json::Value);

impl ExternalJsonDocument {
    #[must_use]
    pub fn into_inner(self) -> serde_json::Value {
        self.0
    }
}

impl From<serde_json::Value> for ExternalJsonDocument {
    fn from(value: serde_json::Value) -> Self {
        Self(value)
    }
}

impl Deref for ExternalJsonDocument {
    type Target = serde_json::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::OperationDetailDocument;

    #[test]
    fn operation_detail_requires_redacted_bounded_object() {
        assert!(OperationDetailDocument::try_from(serde_json::json!([1, 2])).is_err());
        assert!(
            OperationDetailDocument::try_from(serde_json::json!({
                "nested": { "private_key": "never persist" }
            }))
            .is_err()
        );
        assert!(
            OperationDetailDocument::try_from(serde_json::json!({
                "token_id": "domain identifier",
                "reason": "operator action"
            }))
            .is_ok()
        );
    }
}
