//! Training-dataset ledger persistence DTOs.

use crate::{
    enums::quant::{DatasetPurpose, TrainingDatasetStatus},
    types::{
        ArtifactUri, ContentHash, DatasetCoverage, ModelSpecId, RuntimeConfigVersionId,
        TrainingDatasetId, TrainingHorizonsSecs,
    },
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
    /// What the materialized examples are used for (Phase 11.3 §4): `Training`
    /// (the default, model-training spine) or `Calibration` (an independent
    /// held-out split a `ProbabilityCalibrator` fits on — must be disjoint +
    /// embargoed from the model's own `Training` dataset).
    pub purpose: DatasetPurpose,
    pub feature_schema_hash: ContentHash,
    pub factor_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
    pub dataset_hash: ContentHash,
    pub parquet_uri: ArtifactUri,
    pub sample_count: i64,
    /// Feature source visibility delay (PIT cutoff) the dataset was built with.
    /// Persisted so a backtest can recompute features byte-identically.
    pub source_delay_secs: i64,
    /// Deterministic sampling cadence (seconds) the build grid used.
    pub sample_interval_secs: i64,
    /// Forward label horizons (seconds) the build materialized.
    pub horizons_secs: TrainingHorizonsSecs,
    pub coverage_json: DatasetCoverage,
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
        purpose,
        feature_schema_hash,
        factor_schema_hash,
        label_schema_hash,
        dataset_hash,
        parquet_uri,
        sample_count,
        source_delay_secs,
        sample_interval_secs,
        horizons_secs,
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
    pub purpose: DatasetPurpose,
    pub feature_schema_hash: ContentHash,
    pub factor_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
    pub dataset_hash: ContentHash,
    pub parquet_uri: ArtifactUri,
    pub sample_count: i64,
    pub source_delay_secs: i64,
    pub sample_interval_secs: i64,
    pub horizons_secs: TrainingHorizonsSecs,
    pub coverage_json: DatasetCoverage,
    pub runtime_config_version_id: RuntimeConfigVersionId,
}
