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
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::ModelVersionInfo,
    types::{ContentHash, ModelSpecId, ModelVersionId, RuntimeConfigVersionId, TrainingDatasetId},
};

/// Inbound body for `POST /research/models/train`.
#[derive(Debug, Clone, Deserialize, Validate)]
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
    /// Number of rolling validation folds (`>= 2`).
    #[validate(range(min = 2, max = 20))]
    #[serde(default = "default_validation_folds")]
    pub validation_folds: u32,
    /// Operator reason recorded on the operation log.
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

const fn default_validation_folds() -> u32 {
    3
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
        }
    }
}
