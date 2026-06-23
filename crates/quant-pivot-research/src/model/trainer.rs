//! Model training contract: [`ModelTrainer`] and its request/output shells.
//!
//! Offline closure (3.6). 3.0 fixes the trait + minimal I/O so the trainer can
//! be implemented without changing the contract; the request/output bodies
//! (hyperparameters, objective report, validation metrics) are filled in 3.6.

use chrono::{DateTime, Utc};

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::types::{ContentHash, TrainingDatasetId};

use crate::model::{artifact::ModelArtifact, runtime::ModelFamily};

/// Request to train a model of a given family from a frozen dataset.
///
/// Extended in 3.6 with hyperparameters, label spec, and split policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrainModelRequest {
    /// Family to train.
    pub model_family: ModelFamily,
    /// Frozen training dataset to train on.
    pub training_dataset_id: TrainingDatasetId,
    /// Decision time the training was requested as of.
    pub as_of: DateTime<Utc>,
}

/// A freshly trained, content-addressed model artifact.
///
/// Extended in 3.6 with the objective report and validation metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrainedModelArtifact {
    /// The trained artifact.
    pub artifact: ModelArtifact,
    /// Canonical hash of the artifact.
    pub artifact_hash: ContentHash,
}

/// Trains a model family into a content-addressed artifact.
#[async_trait]
pub trait ModelTrainer: Send + Sync {
    /// Family this trainer produces.
    fn model_family(&self) -> ModelFamily;

    /// Train and emit a hashed artifact.
    async fn train(&self, request: TrainModelRequest) -> QuantResult<TrainedModelArtifact>;
}
