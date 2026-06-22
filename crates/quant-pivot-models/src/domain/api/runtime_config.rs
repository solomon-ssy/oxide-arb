//! Governance versioned runtime-config API contract.
//!
//! Runtime configuration is changed only through immutable, audited versions:
//! create a version, then activate (Promote) or roll back to one. There is no
//! bare in-place mutation. Each request carries a `reason` recorded on the
//! chained audit event; the acting role is supplied via the `X-Acting-Role`
//! header and authorized by the authz middleware.
//!
//! UI preference edits submit a sparse [`CreateRuntimeConfigVersionRequest::config_patch`];
//! advanced JSON editing submits a full [`CreateRuntimeConfigVersionRequest::config_json`].

use std::collections::BTreeMap;

use crate::{
    domain::governance::{RuntimeConfigActivationInfo, RuntimeConfigVersionInfo},
    enums::runtime_config::RuntimeConfigVersionSource,
    types::RuntimeConfigVersionId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use validator::Validate;

/// Localized UI text payload.
#[derive(Debug, Clone, Serialize)]
pub struct UiText {
    pub zh_cn: String,
    pub en: String,
}

impl UiText {
    #[must_use]
    pub fn plain(value: impl Into<String>) -> Self {
        let text = value.into();
        Self {
            zh_cn: text.clone(),
            en: text,
        }
    }

    #[must_use]
    pub fn localized(en: impl Into<String>, zh_cn: impl Into<String>) -> Self {
        Self {
            zh_cn: zh_cn.into(),
            en: en.into(),
        }
    }

    #[must_use]
    pub fn has_en_and_zh(&self) -> bool {
        !self.en.trim().is_empty() && !self.zh_cn.trim().is_empty()
    }
}

/// Optional runtime-config form widget hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldWidget {
    PlainString,
    SecretString,
    Integer,
    DurationMs,
    DecimalString,
    Boolean,
    EnumSelect,
    EnumSet,
    StringList,
    EnumDecimalMap,
    JsonTree,
}

/// Optional field semantics for client-side rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldSemantics {
    Money,
    RuntimeMode,
    Credential,
    EmptyMeansAll,
}

/// Enum item metadata for schema fields.
#[derive(Debug, Clone, Serialize)]
pub struct EnumItemView {
    pub key: Value,
    pub label: UiText,
}

/// Conditional display or validation rule.
#[derive(Debug, Clone, Serialize)]
pub struct FieldWhen {
    pub target_path: String,
    pub value: Value,
}

/// Create a new immutable runtime-config version.
///
/// Exactly one of [`config_patch`](Self::config_patch) or
/// [`config_json`](Self::config_json) must be supplied. Patch mode merges
/// against the live config without transmitting unchanged sensitive fields.
#[derive(Debug, Deserialize, Validate)]
pub struct CreateRuntimeConfigVersionRequest {
    /// Sparse UI patch: dotted path → new leaf value (governed preference drawer).
    pub config_patch: Option<BTreeMap<String, Value>>,
    /// Full-document advanced editor payload (governed JSON drawer).
    pub config_json: Option<Value>,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

impl CreateRuntimeConfigVersionRequest {
    /// Fail-closed body validation beyond field-level `#[validate]`.
    pub fn ensure_payload(&self) -> Result<(), String> {
        match (&self.config_patch, &self.config_json) {
            (Some(_), None) | (None, Some(_)) => Ok(()),
            (None, None) => Err("either config_patch or config_json is required".into()),
            (Some(_), Some(_)) => Err("config_patch and config_json are mutually exclusive".into()),
        }
    }
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
    pub config_json: Value,
    pub source: RuntimeConfigVersionSource,
    pub created_by: String,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

impl RuntimeConfigVersionView {
    /// Project a persistence row into the masked read view.
    #[must_use]
    pub fn from_info(info: RuntimeConfigVersionInfo, masked_config_json: Value) -> Self {
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
    pub config: Value,
    /// Latest activation row for the active version (promote / rollback lineage).
    pub activation: Option<RuntimeConfigActivationInfo>,
}

/// JSON value type of a runtime-config schema field, as rendered by the UI form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonValueType {
    String,
    Number,
    Boolean,
    /// Free-form JSON array — fallback tree editor only.
    Array,
    /// Free-form JSON object — fallback tree editor only.
    Object,
    /// Scalar enum (single select).
    Enum,
    /// `Vec<Enum>` — multi-select (e.g. enabled trade categories).
    EnumArray,
    /// `Vec<String>` — tag list (e.g. permanent blacklist IDs).
    StringArray,
    /// Enum-keyed map with decimal string values (e.g. category scoring weights).
    EnumDecimalMap,
}

/// Wire format hint for string leaves (decimal money fields, durations, etc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaFieldFormat {
    Decimal,
    Integer,
    DurationMs,
}

/// Machine-readable constraints extracted from JSON Schema for client validation.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SchemaFieldConstraints {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minimum: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maximum: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive_minimum: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive_maximum: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_length: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_length: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enum_values: Option<Vec<Value>>,
}

/// One preferences group in `GET /runtime-config/schema`.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeConfigSchemaGroupView {
    pub id: String,
    pub label: UiText,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<UiText>,
    pub order: u16,
}

/// Envelope returned by `GET /runtime-config/schema` for the preferences UI.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeConfigSchemaView {
    pub groups: Vec<RuntimeConfigSchemaGroupView>,
    pub fields: Vec<RuntimeConfigSchemaFieldView>,
}

/// One field of the runtime-config schema (`GET /runtime-config/schema`),
/// rendered by the UI as a typed form.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeConfigSchemaFieldView {
    /// Dotted path within the document (e.g. `risk.max_daily_loss_usd`).
    pub path: String,
    /// Root document section (e.g. `risk`, `detection`).
    pub group: String,
    /// Display order within the group.
    pub order: u16,
    /// Localized field title.
    pub label: UiText,
    /// Localized helper / tooltip body.
    pub help: UiText,
    /// JSON value type.
    pub value_type: JsonValueType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<SchemaFieldFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub widget: Option<FieldWidget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantics: Option<FieldSemantics>,
    /// Compiled-in default value.
    pub default: Value,
    /// Human-readable purpose of the field (from Rustdoc; audit fallback only).
    pub description: String,
    /// Whether the field directly bounds money at risk (UI renders a
    /// confirmation affordance for these).
    pub money_critical: bool,
    /// Whether reads mask the value (notification credentials).
    pub sensitive: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraints: Option<SchemaFieldConstraints>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enum_items: Option<Vec<EnumItemView>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub when: Option<Vec<FieldWhen>>,
}
