//! Quant model registry HTTP contract types.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{pagination::PageRequest, quant::ModelSpecInfo},
    enums::{common::MarketCategory, model::ModelFamily},
    types::{
        ContentHash, ModelInputContract, ModelSpecId, ModelTrainingContract, ModelVersionId,
        RoleCode, SchemaVersion, UserId, model_spec::ModelSpecThesis,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelPickerSide {
    Buy,
    Sell,
}

/// Inbound body for `POST /research/model-specs`.
///
/// A model spec is the **authoring root** of the offline research lifecycle:
/// the operator declares the model family, prediction horizon, and feature /
/// label schema versions the downstream dataset build and training runs bind
/// to. A spec is immutable once created; publication lifecycle belongs only to
/// trained model versions (see `TrainModelRequest` / model-governance).
///
/// `model_family` deserializes from its canonical wire label (`"weighted_factor"`,
/// `"classical_random_forest"`, `"hold_vs_exit_weighted"`, …); an unknown label
/// is rejected at the boundary with `400`.
#[derive(Debug, Clone, Deserialize, Validate, JsonSchema)]
#[serde(deny_unknown_fields)]
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
    #[serde(default)]
    pub feature_schema_version: SchemaVersion,
    /// Label schema version the spec targets (defaults to the first version).
    #[serde(default)]
    pub label_schema_version: SchemaVersion,
    /// Closed, human-authored research thesis. This cannot carry executable
    /// parameters or arbitrary metadata keys.
    pub thesis: ModelSpecThesis,
    /// Ordered raw-input contract. This field is mandatory: an empty contract,
    /// unknown feature, duplicate, or encoded/synthetic name is rejected.
    pub input_contract: ModelInputContract,
    /// Frozen target label/horizon and CV folds. Training cannot override it.
    pub training_contract: ModelTrainingContract,
    /// Operator reason recorded on the operation log (UI should require non-empty).
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Outbound projection for a model specification row (the training entry point:
/// the operator picks a spec before planning a dataset or training a version).
#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct QuantModelSpecView {
    pub model_spec_id: ModelSpecId,
    pub name: String,
    pub model_family: ModelFamily,
    pub prediction_horizon_secs: i64,
    pub feature_schema_version: SchemaVersion,
    pub label_schema_version: SchemaVersion,
    pub thesis: ModelSpecThesis,
    pub input_contract: ModelInputContract,
    pub training_contract: ModelTrainingContract,
    pub definition_hash: ContentHash,
    pub created_by_user_id: Option<UserId>,
    pub created_by_label: String,
    pub created_by_role: Option<RoleCode>,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

impl From<ModelSpecInfo> for QuantModelSpecView {
    fn from(info: ModelSpecInfo) -> Self {
        Self {
            model_spec_id: info.model_spec_id,
            name: info.name,
            model_family: info.model_family,
            prediction_horizon_secs: info.prediction_horizon_secs,
            feature_schema_version: info.feature_schema_version,
            label_schema_version: info.label_schema_version,
            thesis: info.thesis,
            input_contract: info.input_contract,
            training_contract: info.training_contract,
            definition_hash: info.definition_hash,
            created_by_user_id: info.created_by_user_id,
            created_by_label: info.created_by_label,
            created_by_role: info.created_by_role,
            reason: info.reason,
            created_at: info.created_at,
        }
    }
}

/// Paginated filter for the model-spec catalog (training/dataset selector source).
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct ModelSpecListQuery {
    /// Narrow by model family (Buy ranker, exit scorer, classical, …).
    pub model_family: Option<ModelFamily>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Filter for `GET /research/models/published-catalog`.
///
/// The category/side-aware picker source for every `FieldWidget::ModelVersionSelect`
/// field.
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
    pub artifact_hash: ContentHash,
    pub model_family: ModelFamily,
    /// The artifact's own declared scope (`None` = generic cross-category).
    pub category_scope: Option<MarketCategory>,
    pub published_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::CreateModelSpecRequest;
    use crate::types::SchemaVersion;

    fn base_request() -> Value {
        serde_json::json!({
            "name": "typed-input-model",
            "model_family": "weighted_factor",
            "prediction_horizon_secs": 86_400,
            "training_contract": {
                "target_label_name": "settlement_outcome",
                "target_label_horizon_secs": 0,
                "validation_folds": 3
            },
            "thesis": {
                "summary": "Typed input model",
                "hypothesis": "Governed raw inputs predict the declared target",
                "limitations": ["Only valid for the frozen Polymarket research contract"]
            },
            "reason": "author governed spec"
        })
    }

    #[test]
    fn input_contract_mandatory_wire() {
        assert!(serde_json::from_value::<CreateModelSpecRequest>(base_request()).is_err());
    }

    #[test]
    fn request_decodes_typed_default() {
        let mut request = base_request();
        request["input_contract"] = serde_json::json!({
            "inputs": [{
                "feature_name": "book.mid",
                "requiredness": "required"
            }]
        });
        let decoded = serde_json::from_value::<CreateModelSpecRequest>(request)
            .expect("typed model spec request");
        assert_eq!(decoded.feature_schema_version, SchemaVersion::FIRST);
        assert_eq!(decoded.label_schema_version.get(), 1);
        assert_eq!(decoded.input_contract.inputs[0].feature_name, "book.mid");
    }

    #[test]
    fn request_rejects_retired_requirements() {
        let mut request = base_request();
        request["input_contract"] = serde_json::json!({
            "inputs": [{
                "feature_name": "book.mid",
                "requiredness": "required"
            }]
        });
        request["feature_requirements"] = serde_json::json!({"required": ["book.mid"]});
        assert!(serde_json::from_value::<CreateModelSpecRequest>(request).is_err());
    }
}
