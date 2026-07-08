//! Quant model registry HTTP contract types.

use crate::{
    domain::{ModelPickerSide, ModelSpecInfo, pagination::PageRequest},
    enums::{common::MarketCategory, model::ModelFamily, quant::PublicationStatus},
    types::{ModelSpecId, ModelVersionId, SchemaVersion},
};
use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Inbound body for `POST /research/model-specs`.
///
/// A model spec is the **authoring root** of the offline research lifecycle:
/// the operator declares the model family, prediction horizon, and feature /
/// label schema versions the downstream dataset build and training runs bind
/// to. Specs are minted in `draft`; a trained version is what later gets
/// published (see `TrainModelRequest` / model-governance).
///
/// `model_family` deserializes from its canonical wire label (`"weighted_factor"`,
/// `"classical_random_forest"`, `"hold_vs_exit_weighted"`, …); an unknown label
/// is rejected at the boundary with `400`.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct CreateModelSpecRequest {
    /// Human-facing spec name (unique-ish label shown in the catalog picker).
    #[validate(length(min = 1, max = 128))]
    pub name: String,
    /// Model family this spec authors (Buy ranker, Sell/exit scorer, classical).
    pub model_family: ModelFamily,
    /// Model-intrinsic prediction horizon in seconds (`>= 1`).
    #[validate(range(min = 1))]
    pub prediction_horizon_secs: i64,
    /// Feature schema version the spec targets (defaults to the first version).
    #[serde(default = "default_schema_version")]
    pub feature_schema_version: SchemaVersion,
    /// Label schema version the spec targets (defaults to the first version).
    #[serde(default = "default_schema_version")]
    pub label_schema_version: SchemaVersion,
    /// Free-form authoring metadata (notes, tuning intent). Defaults to `{}`.
    #[serde(default)]
    pub spec_json: serde_json::Value,
    /// Governed feature-requirements contract (11.2.2 remediation R7): the
    /// generic + per-category domain feature names the eventual model
    /// requires, mirroring `quant_pivot_research::selection::ModelFeatureRequirements`'s
    /// wire shape (`{"generic": [...], "by_category": {...}}`). Defaults to
    /// an empty requirement set (no gating) when omitted.
    #[serde(default)]
    pub feature_requirements: serde_json::Value,
    /// Operator reason recorded on the operation log (UI should require non-empty).
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

const fn default_schema_version() -> SchemaVersion {
    SchemaVersion::FIRST
}

/// Outbound projection for a model specification row (the training entry point:
/// the operator picks a spec before planning a dataset or training a version).
#[derive(Debug, Clone, Serialize)]
pub struct QuantModelSpecView {
    pub model_spec_id: String,
    pub name: String,
    pub model_family: String,
    pub prediction_horizon_secs: i64,
    pub feature_schema_version: SchemaVersion,
    pub label_schema_version: SchemaVersion,
    pub spec_json: serde_json::Value,
    pub feature_requirements: serde_json::Value,
    pub status: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl From<ModelSpecInfo> for QuantModelSpecView {
    fn from(info: ModelSpecInfo) -> Self {
        Self {
            model_spec_id: info.model_spec_id.to_string(),
            name: info.name,
            model_family: info.model_family.as_str().to_owned(),
            prediction_horizon_secs: info.prediction_horizon_secs,
            feature_schema_version: info.feature_schema_version,
            label_schema_version: info.label_schema_version,
            spec_json: info.spec_json,
            feature_requirements: info.feature_requirements,
            status: info.status.as_str().to_owned(),
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// Paginated filter for the model-spec catalog (training/dataset selector source).
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct ModelSpecListQuery {
    /// Narrow by model family (Buy ranker, exit scorer, classical, …).
    pub model_family: Option<ModelFamily>,
    /// Narrow by publication lifecycle (`draft`/`published`/`retired`/…).
    pub status: Option<PublicationStatus>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Filter for `GET /research/models/published-catalog`.
///
/// The category/side-aware picker source for every `FieldWidget::ModelVersionSelect`
/// field (11.2.2 remediation R8).
#[derive(Debug, Clone, Deserialize)]
pub struct ModelPublishedCatalogQuery {
    /// Restrict to versions whose artifact declares this category or `None`.
    /// Absent = no category filter (used by the generic pointer fields).
    pub category: Option<MarketCategory>,
    /// Restrict to versions of this runtime side (Buy ranker vs. Sell scorer).
    pub side: ModelPickerSide,
}

/// One `Published` model version offered by the governed picker widget.
#[derive(Debug, Clone, Serialize)]
pub struct PublishedModelOptionView {
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub spec_name: String,
    pub version: i32,
    pub model_family: String,
    /// The artifact's own declared scope (`None` = generic cross-category).
    pub category_scope: Option<MarketCategory>,
    pub published_at: Option<DateTime<Utc>>,
}
