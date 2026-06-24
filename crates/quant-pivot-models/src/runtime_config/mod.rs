//! Versioned, hot-reloadable runtime configuration (`schema_version = 3`).

pub mod factor_families;
pub mod json_schema;
pub mod preferences_schema;
pub mod sections;
pub mod validation;
pub mod wire;

pub use factor_families::{GENERIC_FACTOR_FAMILY_WIRES, is_generic_factor_family_wire};
pub use json_schema::{
    RuntimeConfigPatchError, apply_runtime_config_patch, build_schema_fields, schema_leaf_paths,
    sensitive_leaf_paths,
};
pub use preferences_schema::{build_preferences_schema, preferences_schema_ui_gaps};
pub use sections::*;
pub use validation::validate_runtime_config;
pub use wire::*;

use schemars::{JsonSchema, SchemaGenerator, generate::SchemaSettings};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::SchemaVersion;

/// The only supported runtime-config schema version.
pub const RUNTIME_CONFIG_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(3);

/// Root of the quant-pivot hot-reloadable runtime configuration document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Document schema version; must equal [`RUNTIME_CONFIG_SCHEMA_VERSION`].
    #[schemars(extend("x-format" = "integer", "x-ui-visible" = false))]
    pub schema_version: SchemaVersion,
    /// Market selection selection policy for reports and model runs.
    pub selection: SelectionConfig,
    /// Data-quality gates used before feature/model/report generation.
    pub data_quality: DataQualityConfig,
    /// Feature schema and enabled feature families.
    pub features: FeaturesConfig,
    /// Factor selection and weighted-scorer configuration.
    pub factors: FactorsConfig,
    /// Active and shadow model references.
    pub model: ModelConfig,
    /// Report schedules and payload sizing.
    pub reports: ReportsConfig,
    /// Portfolio budget and exposure constraints.
    pub portfolio: PortfolioConfig,
    /// Optional execution policy rooted in recommendations.
    pub execution: ExecutionConfig,
    /// Operator notification channels.
    pub notification: NotificationConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
            selection: SelectionConfig::default(),
            data_quality: DataQualityConfig::default(),
            features: FeaturesConfig::default(),
            factors: FactorsConfig::default(),
            model: ModelConfig::default(),
            reports: ReportsConfig::default(),
            portfolio: PortfolioConfig::default(),
            execution: ExecutionConfig::default(),
            notification: NotificationConfig::default(),
        }
    }
}

/// Runtime-config parse / encode failures.
#[derive(Debug, Error)]
pub enum RuntimeConfigError {
    /// The document failed to deserialize into [`RuntimeConfig`].
    #[error("runtime config parse failed: {0}")]
    Parse(#[from] serde_json::Error),
    /// The document declares an unsupported schema version.
    #[error(
        "unsupported runtime config schema_version {found} (expected {RUNTIME_CONFIG_SCHEMA_VERSION})"
    )]
    UnsupportedSchemaVersion { found: SchemaVersion },
}

impl RuntimeConfig {
    /// Typed parse of a stored v3 `config_json` document.
    pub fn from_json(config_json: &serde_json::Value) -> Result<Self, RuntimeConfigError> {
        let config: Self = serde_json::from_value(config_json.clone())?;
        if config.schema_version != RUNTIME_CONFIG_SCHEMA_VERSION {
            return Err(RuntimeConfigError::UnsupportedSchemaVersion {
                found: config.schema_version,
            });
        }
        Ok(config)
    }

    /// Encode to the canonical JSON document stored in `runtime_config_version`.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    /// Generate the inlined JSON Schema for the runtime-config document.
    #[must_use]
    pub fn json_schema() -> serde_json::Value {
        let settings = SchemaSettings::default().with(|s| s.inline_subschemas = true);
        SchemaGenerator::new(settings)
            .into_root_schema_for::<Self>()
            .to_value()
    }

    /// Encode to JSON with sensitive notification credentials masked.
    #[must_use]
    pub fn to_masked_json(&self) -> serde_json::Value {
        let mut masked = self.clone();
        if !masked.notification.telegram.bot_token.trim().is_empty() {
            MASKED_SECRET.clone_into(&mut masked.notification.telegram.bot_token);
        }
        if !masked.notification.webhook.url.trim().is_empty() {
            MASKED_SECRET.clone_into(&mut masked.notification.webhook.url);
        }
        masked.to_json()
    }
}

/// Dotted document paths whose values are masked on read surfaces.
#[must_use]
pub fn sensitive_paths() -> Vec<String> {
    let schema = RuntimeConfig::json_schema();
    let mut paths = Vec::new();
    schema_paths_with_flag(&schema, "x-sensitive", "", &mut paths);
    paths.sort();
    paths
}

/// Replace masked sensitive placeholders with values from the current config.
pub fn unmask_with(incoming: &mut serde_json::Value, current: &RuntimeConfig) {
    let current_json = current.to_json();
    for path in sensitive_paths() {
        let segments = path.split('.').collect::<Vec<_>>();
        let Some(incoming_leaf) = leaf_mut(incoming, &segments) else {
            continue;
        };
        if *incoming_leaf != serde_json::json!(MASKED_SECRET) {
            continue;
        }
        let Some(current_leaf) = leaf(&current_json, &segments) else {
            continue;
        };
        if current_leaf
            .as_str()
            .is_some_and(|value| value.trim().is_empty())
        {
            continue;
        }
        *incoming_leaf = current_leaf.clone();
    }
}

/// Mask a stored `config_json` document for a read surface.
#[must_use]
pub fn mask_config_json(config_json: &serde_json::Value) -> serde_json::Value {
    RuntimeConfig::from_json(config_json).map_or_else(
        |_| {
            let mut document = config_json.clone();
            if let Some(object) = document.as_object_mut() {
                object.remove("notification");
            }
            document
        },
        |config| config.to_masked_json(),
    )
}

fn schema_paths_with_flag(
    schema: &serde_json::Value,
    flag: &str,
    prefix: &str,
    out: &mut Vec<String>,
) {
    let Some(properties) = schema.get("properties").and_then(|p| p.as_object()) else {
        return;
    };
    for (key, child) in properties {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        if child.get(flag).and_then(serde_json::Value::as_bool) == Some(true) {
            out.push(path.clone());
        }
        schema_paths_with_flag(child, flag, &path, out);
    }
}

fn leaf<'a>(document: &'a serde_json::Value, segments: &[&str]) -> Option<&'a serde_json::Value> {
    let mut cursor = document;
    for segment in segments {
        cursor = cursor.get(*segment)?;
    }
    Some(cursor)
}

fn leaf_mut<'a>(
    document: &'a mut serde_json::Value,
    segments: &[&str],
) -> Option<&'a mut serde_json::Value> {
    let mut cursor = document;
    for segment in segments {
        cursor = cursor.get_mut(*segment)?;
    }
    Some(cursor)
}

#[cfg(test)]
mod tests {
    use super::{
        RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig, RuntimeConfigError, sensitive_paths,
    };
    use serde_json::json;

    #[test]
    fn schema_version_is_three() {
        assert_eq!(RuntimeConfig::default().schema_version.get(), 3);
        assert_eq!(RUNTIME_CONFIG_SCHEMA_VERSION.get(), 3);
    }

    #[test]
    fn rejects_v2_documents() {
        let mut document = RuntimeConfig::default().to_json();
        document["schema_version"] = json!(2);
        let error = RuntimeConfig::from_json(&document).expect_err("v2 must be rejected");
        assert!(matches!(
            error,
            RuntimeConfigError::UnsupportedSchemaVersion { found } if found.get() == 2
        ));
    }

    #[test]
    fn sensitive_paths_are_schema_derived() {
        assert_eq!(
            sensitive_paths(),
            vec![
                "notification.telegram.bot_token".to_owned(),
                "notification.webhook.url".to_owned(),
            ]
        );
    }
}
