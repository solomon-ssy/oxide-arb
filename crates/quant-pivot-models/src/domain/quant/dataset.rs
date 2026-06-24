//! Training-dataset ledger persistence DTOs.

use crate::{
    enums::quant::TrainingDatasetStatus,
    types::{ArtifactUri, ContentHash, ModelSpecId, RuntimeConfigVersionId, TrainingDatasetId},
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Frozen training-dataset ledger row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_training_dataset::Entity")]
pub struct TrainingDatasetInfo {
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: TrainingDatasetStatus,
    pub feature_schema_hash: ContentHash,
    pub factor_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
    pub dataset_hash: ContentHash,
    pub parquet_uri: ArtifactUri,
    pub sample_count: i64,
    pub coverage_json: serde_json::Value,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    TrainingDatasetInfo,
    crate::entities::quant_training_dataset::Model,
    {
        training_dataset_id,
        model_spec_id,
        window_start,
        window_end,
        status,
        feature_schema_hash,
        factor_schema_hash,
        label_schema_hash,
        dataset_hash,
        parquet_uri,
        sample_count,
        coverage_json,
        runtime_config_version_id,
        created_at,
    }
);

/// Insert payload for `quant_training_dataset`.
///
/// Covers every `ActiveModel` column except the DB-managed `created_at`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_training_dataset::ActiveModel")]
pub struct NewTrainingDataset {
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: TrainingDatasetStatus,
    pub feature_schema_hash: ContentHash,
    pub factor_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
    pub dataset_hash: ContentHash,
    pub parquet_uri: ArtifactUri,
    pub sample_count: i64,
    pub coverage_json: serde_json::Value,
    pub runtime_config_version_id: RuntimeConfigVersionId,
}
