//! Exhaustive, redacted deployment projection for the Config API.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use toml::{Value as TomlValue, ser::Error as TomlSerializeError};

use super::{
    DeployConfig, DeployConfigDescriptor, DeployConfigFieldDescriptor, DeployConfigTemplate,
    DeploySensitivity,
};

/// Whether a protected deploy value is configured without exposing its literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeployProtectedStatus {
    Configured,
    Missing,
}

/// Safe value projection selected by descriptor sensitivity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "visibility", rename_all = "snake_case")]
pub enum DeployProjectedValue {
    Public { value: Value },
    Protected { status: DeployProtectedStatus },
}

/// One descriptor and its exhaustive safe runtime value projection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeployConfigFieldProjection {
    pub descriptor: DeployConfigFieldDescriptor,
    pub projection: DeployProjectedValue,
}

/// Projection construction failures remain inside the owning models facade.
#[derive(Debug, Error)]
pub enum DeployProjectionError {
    #[error("failed to build the safe Deploy Config projection")]
    SafeSerialization(#[source] TomlSerializeError),
    #[error("Deploy Config descriptor audit failed: {0}")]
    Descriptor(String),
}

impl DeployConfig {
    /// Project every descriptor exactly once while redacting all protected literals.
    pub fn safe_projection(
        &self,
    ) -> Result<Vec<DeployConfigFieldProjection>, DeployProjectionError> {
        let descriptor = DeployConfigDescriptor::generate();
        let failures = descriptor.audit();
        if !failures.is_empty() {
            return Err(DeployProjectionError::Descriptor(failures.join("; ")));
        }
        let safe_value = TomlValue::try_from(DeployConfigTemplate::from(self))
            .map_err(DeployProjectionError::SafeSerialization)?;
        descriptor
            .fields
            .into_iter()
            .map(|mut descriptor| {
                let values = Self::projected_values(&safe_value, &descriptor.toml_path);
                let projection = match descriptor.sensitivity {
                    DeploySensitivity::Public => DeployProjectedValue::Public {
                        value: Self::public_projection(&descriptor, &values),
                    },
                    DeploySensitivity::Secret => DeployProjectedValue::Protected {
                        status: if self
                            .secret_configured(&descriptor.toml_path)
                            .unwrap_or(false)
                        {
                            DeployProtectedStatus::Configured
                        } else {
                            DeployProtectedStatus::Missing
                        },
                    },
                    DeploySensitivity::SensitiveEndpoint
                    | DeploySensitivity::SensitiveIdentifier => DeployProjectedValue::Protected {
                        status: if values.iter().any(|value| Self::is_configured(value)) {
                            DeployProtectedStatus::Configured
                        } else {
                            DeployProtectedStatus::Missing
                        },
                    },
                };
                if descriptor.sensitivity != DeploySensitivity::Public {
                    descriptor.default = None;
                    descriptor.example = None;
                }
                Ok(DeployConfigFieldProjection {
                    descriptor,
                    projection,
                })
            })
            .collect()
    }

    fn public_projection(descriptor: &DeployConfigFieldDescriptor, values: &[&TomlValue]) -> Value {
        if descriptor.dynamic {
            return Value::Array(
                values
                    .iter()
                    .filter_map(|value| serde_json::to_value(value).ok())
                    .collect(),
            );
        }
        values
            .first()
            .and_then(|value| serde_json::to_value(value).ok())
            .unwrap_or(Value::Null)
    }

    fn projected_values<'a>(value: &'a TomlValue, path: &str) -> Vec<&'a TomlValue> {
        let mut values = Vec::new();
        Self::collect_values(value, &path.split('.').collect::<Vec<_>>(), &mut values);
        values
    }

    fn collect_values<'a>(value: &'a TomlValue, path: &[&str], values: &mut Vec<&'a TomlValue>) {
        let Some((head, tail)) = path.split_first() else {
            values.push(value);
            return;
        };
        if *head == "*" {
            match value {
                TomlValue::Array(items) => {
                    for item in items {
                        Self::collect_values(item, tail, values);
                    }
                }
                TomlValue::Table(items) => {
                    for item in items.values() {
                        Self::collect_values(item, tail, values);
                    }
                }
                _ => {}
            }
            return;
        }
        if let TomlValue::Table(table) = value
            && let Some(child) = table.get(*head)
        {
            Self::collect_values(child, tail, values);
        }
    }

    fn is_configured(value: &TomlValue) -> bool {
        match value {
            TomlValue::String(value) => !value.trim().is_empty(),
            TomlValue::Array(values) => {
                !values.is_empty() && values.iter().any(Self::is_configured)
            }
            TomlValue::Table(values) => {
                !values.is_empty() && values.values().any(Self::is_configured)
            }
            _ => true,
        }
    }

    fn secret_configured(&self, path: &str) -> Option<bool> {
        match path {
            "cache.redis.password" => Some(!self.cache.redis.password.is_empty()),
            "db.clickhouse.password" => Some(!self.db.clickhouse.password.is_empty()),
            "db.postgres.password" => Some(!self.db.postgres.password.is_empty()),
            "domain_sources.chainlink_data_streams.api_key" => Some(
                self.domain_sources
                    .chainlink_data_streams
                    .api_key
                    .as_ref()
                    .is_some_and(|secret| !secret.is_empty()),
            ),
            "domain_sources.chainlink_data_streams.api_secret" => Some(
                self.domain_sources
                    .chainlink_data_streams
                    .api_secret
                    .as_ref()
                    .is_some_and(|secret| !secret.is_empty()),
            ),
            "keys.private_key" => Some(self.keys.private_key_present()),
            "notifications.telegram.bot_token" => {
                Some(!self.notifications.telegram.bot_token.is_empty())
            }
            "notifications.webhook.authorization" => {
                Some(!self.notifications.webhook.authorization.is_empty())
            }
            "notifications.webhook.url" => Some(!self.notifications.webhook.url.is_empty()),
            "polymarket.relayer.api_key" => Some(
                self.polymarket
                    .relayer
                    .api_key
                    .as_ref()
                    .is_some_and(|secret| !secret.is_empty()),
            ),
            "research.evidence_attestation.previous_signing_keys" => Some(
                self.research
                    .evidence_attestation
                    .previous_signing_keys
                    .iter()
                    .any(|secret| !secret.is_empty()),
            ),
            "research.evidence_attestation.signing_key" => {
                Some(!self.research.evidence_attestation.signing_key.is_empty())
            }
            "web.jwt.signing_key" => Some(self.web.has_jwt_signing_key()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DeployConfig, DeployProjectedValue};
    use crate::config::{DEPLOY_CONFIG_EXPECTED_LEAF_COUNT, DEPLOY_SECRET_PATHS};

    #[test]
    fn projection_is_exhaustive_redacted() {
        let config = DeployConfig::default();
        let projection = config.safe_projection().expect("safe projection");
        assert_eq!(projection.len(), DEPLOY_CONFIG_EXPECTED_LEAF_COUNT);
        for path in DEPLOY_SECRET_PATHS {
            let field = projection
                .iter()
                .find(|field| field.descriptor.toml_path == path)
                .expect("secret descriptor projection");
            assert!(matches!(
                field.projection,
                DeployProjectedValue::Protected { .. }
            ));
            assert!(field.descriptor.default.is_none());
            assert!(field.descriptor.example.is_none());
        }
    }
}
