//! Trainer admin HTTP contract.
//!
//! UI surface for offline model training from a frozen training dataset:
//!
//! 1. Operator picks an integrity-gated `ready` [`TrainingDatasetId`].
//! 2. `POST /research/models/train` — train, register a **Candidate** version,
//!    and return its [`TrainedModelView`].
//! 3. `GET /research/models/{id}` — poll the registered version.
//!
//! The trainer produces a content-addressed artifact (`models/<hash>.json`) and
//! a `quant_model_version` row in `Candidate` status; promotion to `Published`
//! is governed separately.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{pagination::PageRequest, quant::ModelVersionInfo},
    enums::quant::PublicationStatus,
    types::{
        BacktestPathSetId, ContentHash, ModelRunId, ModelSpecId, ModelVersionId,
        TradePolicyArtifactId, TrainingDatasetId, model_metrics::ModelVersionMetrics,
        model_spec::ModelSpecThesis, model_training::ModelTrainingObjective,
    },
};

/// Inbound body for `POST /research/models/train`.
///
/// `Serialize` is derived so the request can be frozen into a durable research
/// job's `params_json` and replayed on execute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct TrainModelRequest {
    /// Frozen training dataset to train on (must be `ready`).
    pub training_dataset_id: TrainingDatasetId,
    /// Operator reason recorded on the operation log.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

#[cfg(test)]
mod request_tests {
    use super::TrainModelRequest;

    #[test]
    fn request_only_accepts_reason() {
        let base = serde_json::json!({
            "training_dataset_id": uuid::Uuid::now_v7(),
            "reason": "train frozen artifact"
        });
        serde_json::from_value::<TrainModelRequest>(base.clone()).expect("minimal request");

        for retired in [
            "model_family",
            "target_label_name",
            "prediction_horizon_secs",
            "decision_policy_snapshot_id",
            "input_contract",
        ] {
            let mut request = base.clone();
            request[retired] = serde_json::json!("retired-client-override");
            assert!(
                serde_json::from_value::<TrainModelRequest>(request).is_err(),
                "retired field `{retired}` must fail closed"
            );
        }
    }
}

/// Registered model version returned after training and on poll.
#[derive(Debug, Clone, Serialize)]
pub struct TrainedModelView {
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub model_spec_name: String,
    pub model_spec_thesis: ModelSpecThesis,
    pub model_spec_definition_hash: ContentHash,
    pub version: i32,
    pub artifact_hash: ContentHash,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub trade_policy_hash: Option<ContentHash>,
    /// CPCV path set bound for publish quality gates (`None` until bound).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub publish_path_set_id: Option<BacktestPathSetId>,
    /// Lifecycle status — a freshly trained version is `candidate`.
    pub publication_status: String,
    /// Trainer metrics (in-sample + validation objective report).
    pub metrics: ModelVersionMetrics,
    /// Frozen training objective provenance used for this model version.
    pub training_objective: ModelTrainingObjective,
    pub created_at: DateTime<Utc>,
    /// Materialization run id — populated on `POST.../train` only (absent on poll).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_run_id: Option<ModelRunId>,
    /// Model family wire label from the owning spec (`weighted_factor`,
    /// `hold_vs_exit_weighted`, …). Required — repository JOIN fills
    /// [`ModelVersionInfo::model_family`] before projection.
    pub model_family: String,
}

/// Paginated filter for the trained-model registry catalog.
///
/// `from` / `to` bound `created_at`; `model_spec_id` scopes to one spec and
/// `publication_status` narrows the governance lifecycle. The pagination window
/// is the shared [`PageRequest`], flattened so the query string stays flat.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct ModelVersionListQuery {
    pub model_spec_id: Option<ModelSpecId>,
    pub publication_status: Option<PublicationStatus>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

impl From<ModelVersionInfo> for TrainedModelView {
    fn from(info: ModelVersionInfo) -> Self {
        Self {
            model_version_id: info.model_version_id,
            model_spec_id: info.model_spec_id,
            model_spec_name: info.model_spec_name,
            model_spec_thesis: info.model_spec_thesis,
            model_spec_definition_hash: info.model_spec_definition_hash,
            version: info.version,
            artifact_hash: info.artifact_hash,
            training_dataset_id: info.training_dataset_id,
            trade_policy_artifact_id: info.trade_policy_artifact_id,
            trade_policy_hash: info.trade_policy_hash,
            publish_path_set_id: info.publish_path_set_id,
            publication_status: info.publication_status.to_string(),
            metrics: info.metrics,
            training_objective: info.training_objective,
            created_at: info.created_at,
            model_run_id: None,
            model_family: info.model_family.to_string(),
        }
    }
}
