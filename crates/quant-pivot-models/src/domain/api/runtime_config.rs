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
    domain::governance::{
        RuntimeConfigActivationInfo, RuntimeConfigApprovalInfo, RuntimeConfigVersionInfo,
    },
    enums::{
        common::MarketCategory,
        runtime_config::{RuntimeConfigApprovalDecision, RuntimeConfigVersionSource},
    },
    runtime_config::ScheduleCadence,
    types::{ContentHash, RuntimeConfigApprovalId, RuntimeConfigVersionId, SchemaVersion},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use validator::Validate;

/// Locale identifier used as a key in [`UiText::locales`] (matches the SPA i18n).
pub const LOCALE_EN: &str = "en-US";
/// Locale identifier used as a key in [`UiText::locales`] (matches the SPA i18n).
pub const LOCALE_ZH_CN: &str = "zh-CN";

/// Localized UI text payload keyed by SPA locale id (`en-US`, `zh-CN`).
///
/// Extensible: adding a locale is a data change, not a schema change.
#[derive(Debug, Clone, Serialize)]
pub struct UiText {
    pub locales: BTreeMap<String, String>,
}

impl UiText {
    #[must_use]
    pub fn plain(value: impl Into<String>) -> Self {
        let text = value.into();
        Self::localized(text.clone(), text)
    }

    #[must_use]
    pub fn localized(en: impl Into<String>, zh_cn: impl Into<String>) -> Self {
        let mut locales = BTreeMap::new();
        locales.insert(LOCALE_EN.to_owned(), en.into());
        locales.insert(LOCALE_ZH_CN.to_owned(), zh_cn.into());
        Self { locales }
    }

    #[must_use]
    pub fn has_en_and_zh(&self) -> bool {
        let non_empty = |key: &str| {
            self.locales
                .get(key)
                .is_some_and(|text| !text.trim().is_empty())
        };
        non_empty(LOCALE_EN) && non_empty(LOCALE_ZH_CN)
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
    /// Open string→decimal map (e.g. factor weights keyed by factor name).
    DecimalMap,
    /// `[0, 1]` ratio decimal with slider control.
    RatioSlider,
    /// Normalized weight map (sliders + sum-to-one UX).
    WeightMap,
    /// Structured report-schedule list editor (row add/remove + cadence union).
    ScheduleList,
    JsonTree,
    /// Governed model-version picker (11.2.2 remediation R8), backed by
    /// `GET /research/models/published-catalog`. See [`ModelPickerProps`]
    /// for the category/side filtering this widget requires.
    ModelVersionSelect,
}

/// Which model-runtime slot a [`FieldWidget::ModelVersionSelect`] field fills,
/// filtering the picker's candidate list to versions that could actually be
/// loaded into that slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelPickerSide {
    /// Buy-side entry ranker (`WeightedFactor` / classical families).
    Buy,
    /// Sell-side hold-vs-exit scorer (`HoldVsExitWeighted` family).
    Sell,
}

/// Filtering metadata for a [`FieldWidget::ModelVersionSelect`] field.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ModelPickerProps {
    /// Restrict candidates to this category's scope, or `None` (or an
    /// artifact declaring `category_scope = None`) for a generic-purpose
    /// picker (e.g. `model.active_model_version_id`). A category-pointer
    /// field (e.g. `model.category_model_pointers.crypto`) sets this to
    /// `Some(MarketCategory::Crypto)`; the picker still offers `None`-scoped
    /// (generic) versions too, since a category slot may deliberately point
    /// at the generic scorer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<MarketCategory>,
    /// Which runtime side this slot loads (Buy ranker vs. Sell scorer);
    /// families outside this side are never valid candidates.
    pub side: ModelPickerSide,
}

/// Presentation hints for a field, independent of its data type / widget.
///
/// Purely cosmetic guidance for the form renderer (unit suffixes, grid width,
/// read-only display). Never affects validation or the submitted value.
#[derive(Debug, Clone, Default, Serialize)]
pub struct UiProps {
    /// Localized placeholder text for empty inputs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<UiText>,
    /// Static leading adornment (e.g. `$`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Static trailing adornment / unit (e.g. `USD`, `bps`, `secs`, `%`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suffix: Option<String>,
    /// Grid width in 24-column units (`1..=24`); `None` ⇒ full row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub col_span: Option<u8>,
    /// Render the value read-only (display only; never submitted as an edit).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_only: Option<bool>,
    /// Slider minimum (inclusive) for [`FieldWidget::RatioSlider`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slider_min: Option<f64>,
    /// Slider maximum (inclusive) for [`FieldWidget::RatioSlider`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slider_max: Option<f64>,
    /// Slider step for [`FieldWidget::RatioSlider`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slider_step: Option<f64>,
}

impl UiProps {
    /// A unit-suffix-only props (the common case for money / bps / secs fields).
    #[must_use]
    pub fn suffix(unit: impl Into<String>) -> Self {
        Self {
            suffix: Some(unit.into()),
            ..Self::default()
        }
    }
}

/// Optional field behavior/governance hint (distinct from the render `widget`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldSemantics {
    /// Reads mask the value; empty patch keeps the stored secret.
    Credential,
    /// Empty selection means "all" (do not treat as an empty allow-list).
    EmptyMeansAll,
    /// Mutation bounds money/behavior at risk; the UI forces a danger confirmation.
    GovernanceCritical,
}

/// Enum item metadata for schema fields.
#[derive(Debug, Clone, Serialize)]
pub struct EnumItemView {
    pub key: Value,
    pub label: UiText,
}

/// Effect applied to a schema field when a [`FieldWhen`] rule matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WhenEffect {
    /// Field is visible only while every `if` rule matches.
    If,
    /// Field is required only while the rule matches.
    Require,
}

/// Comparison operator evaluated by a [`FieldWhen`] rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WhenOperator {
    Eq,
    Ne,
}

/// Conditional display or validation rule referencing another schema leaf.
#[derive(Debug, Clone, Serialize)]
pub struct FieldWhen {
    pub effect: WhenEffect,
    pub operator: WhenOperator,
    pub target_path: String,
    pub value: Value,
}

impl FieldWhen {
    /// Visible only while `target_path == value`.
    #[must_use]
    pub fn visible_when_eq(target_path: impl Into<String>, value: Value) -> Self {
        Self {
            effect: WhenEffect::If,
            operator: WhenOperator::Eq,
            target_path: target_path.into(),
            value,
        }
    }
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
    pub runtime_config_approval_id: RuntimeConfigApprovalId,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Roll back to an existing runtime-config version (Rollback).
#[derive(Debug, Deserialize, Validate)]
pub struct RollbackRuntimeConfigRequest {
    pub runtime_config_approval_id: RuntimeConfigApprovalId,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Append one immutable approval or rejection for an exact config version.
#[derive(Debug, Deserialize, Validate)]
pub struct RecordRuntimeConfigApprovalRequest {
    pub decision: RuntimeConfigApprovalDecision,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
    pub expires_at: Option<DateTime<Utc>>,
}

/// Approved revisions eligible for an operator activation selector.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeConfigApprovalView {
    #[serde(flatten)]
    pub approval: RuntimeConfigApprovalInfo,
}

/// Version-catalog page size (capped in the handler).
#[derive(Debug, Deserialize)]
pub struct RuntimeConfigVersionListQuery {
    pub limit: Option<u64>,
}

/// Preview the next fire times for a report-schedule cadence.
///
/// Stateless dry-run against the same cron parser the scheduler uses; drives the
/// schedule-list editor's "next runs" hint without mutating anything.
#[derive(Debug, Deserialize, Validate)]
pub struct SchedulePreviewRequest {
    /// Cadence to evaluate (fixed interval or 6-field cron with optional IANA tz).
    pub cadence: ScheduleCadence,
    /// Number of upcoming fire times to return (`1..=20`).
    #[validate(range(min = 1, max = 20))]
    #[serde(default = "default_preview_count")]
    pub count: u8,
}

const fn default_preview_count() -> u8 {
    5
}

/// The next fire times (UTC) for a previewed cadence.
#[derive(Debug, Clone, Serialize)]
pub struct SchedulePreviewView {
    /// Upcoming fire instants in ascending order, in UTC.
    pub next_fire_times: Vec<DateTime<Utc>>,
}

/// Catalog/read projection of a runtime-config version with sensitive
/// notification credentials masked.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeConfigVersionView {
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub config_hash: ContentHash,
    pub schema_version: SchemaVersion,
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
    /// Open string-keyed map with decimal string values (e.g. factor weights).
    DecimalMap,
}

/// Homogeneous JSON-array element type hint for compact table editors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrayItemType {
    Integer,
    String,
    Unknown,
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

/// One node of the runtime-config layout tree.
///
/// The schema is delivered as a **normalized field dictionary**
/// ([`RuntimeConfigSchemaView::fields`], keyed by dotted path) plus this
/// **layout tree** describing how those fields are grouped and gated. A node is
/// one of: a nested [`SchemaSection`], a [`SchemaFieldRef`] pointing at a field
/// in the dictionary, or a discriminated [`SchemaUnion`].
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchemaNode {
    Section(SchemaSection),
    Field(SchemaFieldRef),
    Union(SchemaUnion),
}

/// A (possibly nested) group of nodes rendered as a collapsible card.
#[derive(Debug, Clone, Serialize)]
pub struct SchemaSection {
    /// Stable section id (dotted, e.g. `execution.exit_monitor`).
    pub id: String,
    /// Localized section title.
    pub label: UiText,
    /// Localized section description / purpose.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<UiText>,
    /// Iconify icon id (same convention as RBAC menu `icon`, e.g. `lucide:wallet`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// Whether the section renders collapsed-capable.
    pub collapsible: bool,
    /// Display order among sibling nodes.
    pub order: u16,
    /// Child nodes (fields, sub-sections, unions).
    pub children: Vec<SchemaNode>,
}

/// A reference to one field in [`RuntimeConfigSchemaView::fields`].
#[derive(Debug, Clone, Serialize)]
pub struct SchemaFieldRef {
    /// Dotted path key into the field dictionary.
    pub path: String,
    /// Display order among sibling nodes.
    pub order: u16,
}

/// A discriminated group.
///
/// Only the case matching the live discriminator value renders. Models genuinely
/// variant config shapes (e.g. an emergency-exit action whose parameters only
/// apply to one action kind).
#[derive(Debug, Clone, Serialize)]
pub struct SchemaUnion {
    /// Display order among sibling nodes.
    pub order: u16,
    /// Dotted path of the discriminator field (whose value selects the case).
    pub discriminator: String,
    /// Localized union title (rendered above the active case).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<UiText>,
    /// The candidate cases; the one whose `case_value` equals the live
    /// discriminator value is rendered.
    pub cases: Vec<SchemaUnionCase>,
}

/// One case of a [`SchemaUnion`].
#[derive(Debug, Clone, Serialize)]
pub struct SchemaUnionCase {
    /// Discriminator value that activates this case.
    pub case_value: Value,
    /// Nodes rendered when this case is active.
    pub children: Vec<SchemaNode>,
}

/// Envelope returned by `GET /runtime-config/schema` for the preferences UI:
/// a normalized field dictionary plus the layout tree over it.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeConfigSchemaView {
    /// Layout tree (nested sections / field refs / unions).
    pub tree: Vec<SchemaNode>,
    /// Field metadata dictionary, keyed by `path`.
    pub fields: Vec<RuntimeConfigSchemaFieldView>,
}

/// One field of the runtime-config schema (`GET /runtime-config/schema`),
/// rendered by the UI as a typed form.
#[derive(Debug, Clone, Serialize)]
pub struct RuntimeConfigSchemaFieldView {
    /// Dotted path within the document (e.g. `portfolio.budget.total_budget_usd`).
    /// This is the dictionary key; layout / grouping is owned by the tree.
    pub path: String,
    /// Localized field title.
    pub label: UiText,
    /// Localized helper / tooltip body (authored, distinct from `label`).
    pub help: UiText,
    /// JSON value type.
    pub value_type: JsonValueType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<SchemaFieldFormat>,
    /// Presentation hints (unit suffix, grid width, read-only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ui_props: Option<UiProps>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub widget: Option<FieldWidget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantics: Option<FieldSemantics>,
    /// Category/side filtering for [`FieldWidget::ModelVersionSelect`] fields.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_picker: Option<ModelPickerProps>,
    /// Compiled-in default value.
    pub default: Value,
    /// Human-readable purpose of the field (from Rustdoc; audit fallback only).
    pub description: String,
    /// Whether reads mask the value (notification credentials).
    pub sensitive: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraints: Option<SchemaFieldConstraints>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enum_items: Option<Vec<EnumItemView>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub when: Option<Vec<FieldWhen>>,
    /// When `value_type` is [`JsonValueType::Array`], the JSON Schema `items.type`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub array_item_type: Option<ArrayItemType>,
}
