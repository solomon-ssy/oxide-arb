//! Governance versioned runtime-config API contract.
//!
//! Runtime configuration is changed only through immutable, audited versions:
//! create a version, then activate (Promote) or roll back to one. There is no
//! bare in-place mutation. Each request carries a `reason` recorded on the
//! chained audit event; the acting role is supplied via the `X-Acting-Role`
//! header and authorized by the authz middleware.
//!
//! `config_json` is **typed**: handlers parse it into
//! [`RuntimeConfig`](crate::runtime_config::RuntimeConfig) (`schema_version =
//! 1`, unknown fields rejected) and run semantic validation before anything is
//! persisted. Sensitive notification credentials are masked on every read.

use crate::{
    domain::governance::RuntimeConfigVersionInfo,
    enums::runtime_config::RuntimeConfigVersionSource, types::RuntimeConfigVersionId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Create a new immutable runtime-config version.
///
/// The handler typed-parses `config_json` as `schema_version = 1`, validates
/// it semantically (fail-closed), canonicalizes the JSON, derives the content
/// hash, mints the version id, sets the source to `Operator`, and records
/// `created_by` from the authenticated actor.
#[derive(Debug, Deserialize, Validate)]
pub struct CreateRuntimeConfigVersionRequest {
    /// The full runtime-config document as JSON (partial documents are filled
    /// with schema defaults; unknown fields are rejected).
    pub config_json: serde_json::Value,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Activate an existing runtime-config version (Promote).
#[derive(Debug, Deserialize, Validate)]
pub struct ActivateRuntimeConfigRequest {
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Roll back to an existing runtime-config version (Rollback).
#[derive(Debug, Deserialize, Validate)]
pub struct RollbackRuntimeConfigRequest {
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Version-catalog page size (capped in the handler).
#[derive(Debug, Deserialize)]
pub struct RuntimeConfigVersionListQuery {
    pub limit: Option<u64>,
}

/// Catalog/read projection of a runtime-config version with sensitive
/// notification credentials masked.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeConfigVersionView {
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub config_hash: String,
    pub schema_version: i32,
    /// Document JSON with `notification.telegram.bot_token` and
    /// `notification.webhook.url` masked.
    pub config_json: serde_json::Value,
    pub source: RuntimeConfigVersionSource,
    pub created_by: String,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

impl RuntimeConfigVersionView {
    /// Project a persistence row into the masked read view.
    #[must_use]
    pub fn from_info(
        info: RuntimeConfigVersionInfo,
        masked_config_json: serde_json::Value,
    ) -> Self {
        Self {
            runtime_config_version_id: info.runtime_config_version_id,
            config_hash: info.config_hash,
            schema_version: info.schema_version,
            config_json: masked_config_json,
            source: info.source,
            created_by: info.created_by,
            reason: info.reason,
            created_at: info.created_at,
        }
    }
}

/// `GET /runtime-config` response: the live in-process snapshot (masked) plus
/// the active version metadata.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeConfigCurrentView {
    /// Active version row (masked); `None` only before the bootstrap seed.
    pub version: Option<RuntimeConfigVersionView>,
    /// The live, currently-applied runtime config (masked).
    pub config: serde_json::Value,
}

/// JSON value type of a runtime-config schema field, as rendered by the UI
/// form (`string` | `number` | `boolean` | `array` | `object`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JsonValueType {
    String,
    Number,
    Boolean,
    Array,
    Object,
}

/// One field of the runtime-config schema (`GET /runtime-config/schema`),
/// rendered by the UI as a typed form.
///
/// Derived from the generated JSON Schema
/// ([`RuntimeConfig::json_schema`](crate::runtime_config::RuntimeConfig::json_schema)):
/// `description` comes from the field Rustdoc, `money_critical` / `sensitive`
/// from the `x-money-critical` / `x-sensitive` schema keywords declared next
/// to the field definitions.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeConfigSchemaFieldView {
    /// Dotted path within the document (e.g. `risk.max_daily_loss_usd`).
    pub path: String,
    /// JSON value type.
    pub value_type: JsonValueType,
    /// Compiled-in default value.
    pub default: serde_json::Value,
    /// Human-readable purpose of the field (from the field Rustdoc).
    pub description: String,
    /// Whether the field directly bounds money at risk (UI renders a
    /// confirmation affordance for these).
    pub money_critical: bool,
    /// Whether reads mask the value (notification credentials).
    pub sensitive: bool,
}
