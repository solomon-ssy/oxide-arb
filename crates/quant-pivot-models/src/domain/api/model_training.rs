//! Trainer admin HTTP contract (Phase 3.6).
//!
//! UI surface for offline model training from a frozen training dataset:
//!
//! 1. Operator picks a frozen [`RuntimeConfigVersionId`], [`ModelSpecId`], and a
//!    `ready`/`built` [`TrainingDatasetId`].
//! 2. `POST /research/models/train` — train, register a **Candidate** version,
//!    and return its [`TrainedModelView`].
//! 3. `GET /research/models/{id}` — poll the registered version.
//!
//! The trainer produces a content-addressed artifact (`models/<hash>.json`) and
//! a `quant_model_version` row in `Candidate` status; promotion to `Published`
//! is governed separately (Phase 3.7).

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{ModelVersionInfo, pagination::PageRequest},
    enums::quant::PublicationStatus,
    types::{
        ContentHash, ModelRunId, ModelSpecId, ModelVersionId, RuntimeConfigVersionId,
        TrainingDatasetId,
    },
};

/// Inbound body for `POST /research/models/train`.
///
/// `Serialize` is derived so the request can be frozen into a durable research
/// job's `params_json` and replayed on execute.
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct TrainModelRequest {
    /// Target model specification the trained version is bound to.
    pub model_spec_id: ModelSpecId,
    /// Frozen training dataset to train on (must be `built` or `ready`).
    pub training_dataset_id: TrainingDatasetId,
    /// Frozen runtime-config version governing feature/factor schemas and the
    /// `factor_weights` training seed.
    pub runtime_config_version_id: RuntimeConfigVersionId,
    /// Model family to train: `"weighted_factor"` or `"classical:<kind>"`
    /// (e.g. `"classical:random_forest"`). Classical families require the
    /// `ml-classical` build, else the request is rejected.
    #[validate(length(min = 1))]
    pub model_family: String,
    /// Supervised target label name (e.g. `"settlement_outcome"`).
    #[validate(length(min = 1))]
    pub label_name: String,
    /// Horizon of the target label in seconds (`0` for horizon-independent
    /// labels such as settlement outcome).
    pub label_horizon_secs: u64,
    /// Model-intrinsic prediction horizon in seconds, frozen into the trained
    /// artifact (`WeightedFactorModelArtifact.prediction_horizon_secs`) and used
    /// online for the horizon score multiplier and each candidate's
    /// `suggested_horizon_secs`. This is a training-authoring parameter — online
    /// inference reads the frozen artifact value, never runtime config.
    #[validate(range(min = 1))]
    #[serde(default = "default_prediction_horizon_secs")]
    pub prediction_horizon_secs: u64,
    /// Number of rolling validation folds (`>= 2`).
    #[validate(range(min = 2, max = 20))]
    #[serde(default = "default_validation_folds")]
    pub validation_folds: u32,
    /// Operator reason recorded on the operation log.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
    /// Pre-assigned id frozen at async enqueue for effectively-once recovery;
    /// omit on direct calls — the job engine mints one before persisting params.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_version_id: Option<ModelVersionId>,
}

const fn default_validation_folds() -> u32 {
    3
}

const fn default_prediction_horizon_secs() -> u64 {
    86_400
}

/// Registered model version returned after training and on poll.
#[derive(Debug, Clone, Serialize)]
pub struct TrainedModelView {
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub version: i32,
    pub artifact_hash: ContentHash,
    pub training_dataset_id: Option<TrainingDatasetId>,
    /// Lifecycle status — a freshly trained version is `candidate`.
    pub publication_status: String,
    /// Trainer metrics (in-sample + validation objective report).
    pub metrics: serde_json::Value,
    pub created_at: DateTime<Utc>,
    /// Materialization run id — populated on `POST .../train` only (absent on poll).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_run_id: Option<ModelRunId>,
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
            version: info.version,
            artifact_hash: info.artifact_hash,
            training_dataset_id: info.training_dataset_id,
            publication_status: info.publication_status.as_str().to_owned(),
            metrics: info.metrics_json,
            created_at: info.created_at,
            model_run_id: None,
        }
    }
}
