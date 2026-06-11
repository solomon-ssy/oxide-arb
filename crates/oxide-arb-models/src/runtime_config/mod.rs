//! Versioned, hot-reloadable runtime configuration (`schema_version = 1`).
//!
//! [`RuntimeConfig`] is the single typed schema for the JSON stored in the
//! `runtime_config_version.config_json` column. It owns every operator tunable
//! that must take effect **without a restart**: staleness thresholds, detection
//! parameters, execution operational tunables, the full risk model, settlement
//! operations, and notification channels.
//!
//! Restart-bound infrastructure (connections, pools, shard counts, channel
//! capacities, credentials sources, web server) lives in
//! [`DeployConfig`](crate::config::DeployConfig) instead.
//!
//! # Lifecycle
//!
//! 1. First boot seeds a `Bootstrap` version from [`RuntimeConfig::default`].
//! 2. Operators create immutable versions (`POST /api/runtime-config/versions`,
//!    typed-parsed and validated) and activate them through the audited
//!    governance path.
//! 3. Activation swaps the in-process store and propagates to every subscriber
//!    (risk engine, detection chain, execution chain, settlement, alerts).
//!
//! There is exactly one schema version; non-`1` documents are rejected at
//! create/activate time (the project has no legacy data to migrate).

mod detection;
mod execution;
mod market_data;
mod notification;
mod risk;
mod settlement;
pub mod validation;

pub use detection::*;
pub use execution::*;
pub use market_data::*;
pub use notification::*;
pub use risk::*;
pub use settlement::*;

use schemars::{JsonSchema, SchemaGenerator, generate::SchemaSettings};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The only supported runtime-config schema version.
pub const RUNTIME_CONFIG_SCHEMA_VERSION: i32 = 1;

/// Root of the hot-reloadable runtime configuration document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Document schema version; must equal [`RUNTIME_CONFIG_SCHEMA_VERSION`].
    pub schema_version: i32,
    /// Book staleness ladder (gates detection + validation).
    pub market_data: MarketDataRuntimeConfig,
    /// Opportunity detection (endgame + calibration).
    pub detection: DetectionConfig,
    /// Execution operational tunables (timeouts, funnel, coalescer, latency).
    pub execution: ExecutionRuntimeConfig,
    /// Risk limits, circuit breaker, and position sizing.
    pub risk: RiskConfig,
    /// Settlement operations (oracle, lifecycle, redeem route).
    pub settlement: SettlementRuntimeConfig,
    /// Operator alert channels (full, including credentials; masked on read).
    pub notification: NotificationConfig,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
            market_data: MarketDataRuntimeConfig::default(),
            detection: DetectionConfig::default(),
            execution: ExecutionRuntimeConfig::default(),
            risk: RiskConfig::default(),
            settlement: SettlementRuntimeConfig::default(),
            notification: NotificationConfig::default(),
        }
    }
}

/// Typed parse / encode failures for runtime-config documents.
#[derive(Debug, Error)]
pub enum RuntimeConfigError {
    /// The document failed to deserialize into [`RuntimeConfig`] (unknown
    /// fields, wrong types, malformed values).
    #[error("runtime config parse failed: {0}")]
    Parse(#[from] serde_json::Error),
    /// The document declares an unsupported schema version.
    #[error(
        "unsupported runtime config schema_version {found} (expected {RUNTIME_CONFIG_SCHEMA_VERSION})"
    )]
    UnsupportedSchemaVersion { found: i32 },
}

impl RuntimeConfig {
    /// Typed parse of a stored `config_json` document.
    ///
    /// Rejects unknown fields (typo safety for a money-critical document) and
    /// any `schema_version` other than [`RUNTIME_CONFIG_SCHEMA_VERSION`].
    pub fn from_json(config_json: &serde_json::Value) -> Result<Self, RuntimeConfigError> {
        let config: Self = serde_json::from_value(config_json.clone())?;
        if config.schema_version != RUNTIME_CONFIG_SCHEMA_VERSION {
            return Err(RuntimeConfigError::UnsupportedSchemaVersion {
                found: config.schema_version,
            });
        }
        Ok(config)
    }

    /// Encode to the canonical JSON document stored in
    /// `runtime_config_version.config_json`.
    ///
    /// `RuntimeConfig` contains only JSON-representable values, so encoding is
    /// infallible by construction.
    #[must_use]
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
    }

    /// Generate the inlined JSON Schema (draft 2020-12) for the document.
    ///
    /// Subschemas are inlined (no `$defs` / `$ref`) so consumers can walk the
    /// tree directly. Field Rustdoc becomes each property's `description`, and
    /// the custom `x-money-critical` / `x-sensitive` keywords carry governance
    /// metadata declared next to the field definitions — the single source of
    /// truth for the UI form renderer.
    #[must_use]
    pub fn json_schema() -> serde_json::Value {
        let settings = SchemaSettings::default().with(|s| s.inline_subschemas = true);
        SchemaGenerator::new(settings)
            .into_root_schema_for::<Self>()
            .to_value()
    }

    /// Encode to JSON with sensitive notification credentials masked.
    ///
    /// Every read surface (REST, UI) must use this — never `to_json` — when
    /// returning a document to a client.
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

/// Placeholder substituted for sensitive values on read surfaces.
pub const MASKED_SECRET: &str = "***";

/// Mask a stored `config_json` document for a read surface.
///
/// Documents that fail the typed parse (legacy rows) have their entire
/// `notification` section removed instead — never returned verbatim.
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

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    #[test]
    fn default_round_trips_through_json() {
        let config = RuntimeConfig::default();
        let parsed = RuntimeConfig::from_json(&config.to_json()).expect("round trip");
        assert_eq!(parsed, config);
    }

    #[test]
    fn schema_version_is_one() {
        assert_eq!(
            RuntimeConfig::default().schema_version,
            RUNTIME_CONFIG_SCHEMA_VERSION
        );
        assert_eq!(RUNTIME_CONFIG_SCHEMA_VERSION, 1);
    }

    #[test]
    fn rejects_unsupported_schema_version() {
        let mut document = RuntimeConfig::default().to_json();
        document["schema_version"] = json!(2);
        let error = RuntimeConfig::from_json(&document).expect_err("schema_version 2 rejected");
        assert!(matches!(
            error,
            RuntimeConfigError::UnsupportedSchemaVersion { found: 2 }
        ));
    }

    #[test]
    fn rejects_unknown_fields() {
        let mut document = RuntimeConfig::default().to_json();
        document["treasury"] = json!({ "target_balance_usd": "1000" });
        assert!(RuntimeConfig::from_json(&document).is_err());
    }

    #[test]
    fn rejects_unknown_nested_fields() {
        let mut document = RuntimeConfig::default().to_json();
        document["detection"]["endgame"]["enabled"] = json!(true);
        assert!(
            RuntimeConfig::from_json(&document).is_err(),
            "deleted detection.endgame.enabled must be rejected"
        );
    }

    #[test]
    fn partial_document_fills_defaults() {
        let document = json!({
            "risk": { "max_daily_loss_usd": "150" }
        });
        let parsed = RuntimeConfig::from_json(&document).expect("partial document");
        assert_eq!(parsed.risk.max_daily_loss_usd, dec!(150));
        assert_eq!(parsed.schema_version, RUNTIME_CONFIG_SCHEMA_VERSION);
        assert_eq!(parsed.detection, DetectionConfig::default());
    }

    /// Collect dotted paths of properties carrying a given `x-` keyword.
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

    /// The `x-sensitive` schema markers and the masking implementation must
    /// agree exactly — a field masked but not marked (or vice versa) is a
    /// credential-leak hazard for the UI.
    #[test]
    fn sensitive_schema_markers_match_masking() {
        let schema = RuntimeConfig::json_schema();
        let mut marked = Vec::new();
        schema_paths_with_flag(&schema, "x-sensitive", "", &mut marked);
        marked.sort();

        let mut config = RuntimeConfig::default();
        config.notification.telegram.bot_token = "tg-secret".into();
        config.notification.webhook.url = "https://hooks.example/secret".into();
        let plain = config.to_json();
        let masked_doc = config.to_masked_json();
        let mut masked_paths = Vec::new();
        collect_masked_paths(&plain, &masked_doc, "", &mut masked_paths);
        masked_paths.sort();

        assert_eq!(
            marked, masked_paths,
            "x-sensitive markers and to_masked_json must mask the same fields"
        );
        assert_eq!(
            marked,
            vec![
                "notification.telegram.bot_token".to_owned(),
                "notification.webhook.url".to_owned(),
            ]
        );
    }

    /// Diff two documents and record leaf paths whose value became masked.
    fn collect_masked_paths(
        plain: &serde_json::Value,
        masked: &serde_json::Value,
        prefix: &str,
        out: &mut Vec<String>,
    ) {
        match (plain, masked) {
            (serde_json::Value::Object(a), serde_json::Value::Object(b)) => {
                for (key, plain_child) in a {
                    let path = if prefix.is_empty() {
                        key.clone()
                    } else {
                        format!("{prefix}.{key}")
                    };
                    if let Some(masked_child) = b.get(key) {
                        collect_masked_paths(plain_child, masked_child, &path, out);
                    }
                }
            }
            (plain_leaf, masked_leaf) => {
                if plain_leaf != masked_leaf && *masked_leaf == json!(MASKED_SECRET) {
                    out.push(prefix.to_owned());
                }
            }
        }
    }

    /// Every leaf property in the generated schema must carry a description
    /// (sourced from the field Rustdoc) — empty descriptions break the UI form.
    #[test]
    fn schema_has_descriptions_on_every_property() {
        fn walk(schema: &serde_json::Value, prefix: &str, missing: &mut Vec<String>) {
            let Some(properties) = schema.get("properties").and_then(|p| p.as_object()) else {
                return;
            };
            for (key, child) in properties {
                let path = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                let described = child
                    .get("description")
                    .and_then(|d| d.as_str())
                    .is_some_and(|d| !d.trim().is_empty());
                if !described {
                    missing.push(path.clone());
                }
                walk(child, &path, missing);
            }
        }
        let schema = RuntimeConfig::json_schema();
        let mut missing = Vec::new();
        walk(&schema, "", &mut missing);
        assert!(missing.is_empty(), "fields without Rustdoc: {missing:?}");
    }
}
