//! Training-dataset ledger persistence DTOs.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_training_dataset,
    enums::quant::{DatasetPurpose, TrainingDatasetStatus},
    types::{
        ArtifactUri, ContentHash, DatasetCoverage, DatasetManifest, DecisionPolicySnapshotId,
        ModelSpecId, SchemaVersion, TrainingDatasetId, TrainingHorizonsSecs, TrainingSampleSources,
    },
};

/// Frozen training-dataset ledger row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_training_dataset::Entity")]
pub struct TrainingDatasetInfo {
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub model_spec_definition_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: TrainingDatasetStatus,
    /// What the materialized examples are used for: `Training`
    /// (the default, model-training spine) or `Calibration` (an independent
    /// held-out split a `ProbabilityCalibrator` fits on — must be disjoint +
    /// embargoed from the model's own `Training` dataset).
    pub purpose: DatasetPurpose,
    pub feature_schema_hash: Option<ContentHash>,
    pub factor_schema_hash: Option<ContentHash>,
    pub label_schema_hash: Option<ContentHash>,
    pub dataset_hash: Option<ContentHash>,
    /// Canonical hash of the manifest embedded in the Parquet envelope.
    pub manifest_hash: Option<ContentHash>,
    /// Exact structured manifest embedded in the Parquet envelope. Legacy
    /// retired audit rows may be `None`; no API layer reconstructs it.
    pub manifest_json: Option<DatasetManifest>,
    /// Exact hash of the persisted Parquet bytes.
    pub artifact_bytes_hash: Option<ContentHash>,
    pub parquet_uri: Option<ArtifactUri>,
    pub sample_count: Option<i64>,
    /// Feature source visibility delay (PIT cutoff) the dataset was built with.
    /// Persisted so a backtest can recompute features byte-identically.
    pub knowledge_lag_secs: i64,
    /// Deterministic sampling cadence (seconds) the build grid used.
    pub sample_interval_secs: i64,
    /// Forward label horizons (seconds) the build materialized.
    pub horizons_secs: TrainingHorizonsSecs,
    pub feature_schema_version: Option<SchemaVersion>,
    pub sample_sources: Option<TrainingSampleSources>,
    pub coverage_json: Option<DatasetCoverage>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub failure_detail: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    TrainingDatasetInfo,
    quant_training_dataset::Model,
    {
        training_dataset_id,
        model_spec_id,
        model_spec_definition_hash,
        window_start,
        window_end,
        status,
        purpose,
        feature_schema_hash,
        factor_schema_hash,
        label_schema_hash,
        dataset_hash,
        manifest_hash,
        manifest_json,
        artifact_bytes_hash,
        parquet_uri,
        sample_count,
        knowledge_lag_secs,
        sample_interval_secs,
        horizons_secs,
        feature_schema_version,
        sample_sources,
        coverage_json,
        decision_policy_snapshot_id,
        failure_detail,
        completed_at,
        created_at,
    }
);

/// Immutable plan inserted before materialization starts.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_training_dataset::ActiveModel")]
pub struct NewTrainingDatasetPlan {
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub model_spec_definition_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub purpose: DatasetPurpose,
    pub knowledge_lag_secs: i64,
    pub sample_interval_secs: i64,
    pub horizons_secs: TrainingHorizonsSecs,
    pub feature_schema_version: Option<SchemaVersion>,
    pub sample_sources: Option<TrainingSampleSources>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
}

/// Artifact bindings committed atomically with the build's terminal status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteTrainingDatasetBuild {
    pub status: TrainingDatasetStatus,
    pub feature_schema_hash: ContentHash,
    pub factor_schema_hash: ContentHash,
    pub label_schema_hash: ContentHash,
    pub dataset_hash: ContentHash,
    pub manifest_hash: ContentHash,
    pub manifest_json: DatasetManifest,
    pub artifact_bytes_hash: ContentHash,
    pub parquet_uri: ArtifactUri,
    pub sample_count: i64,
    pub coverage_json: DatasetCoverage,
    pub failure_detail: Option<String>,
}

/// Fully materialized artifact fields borrowed from a lifecycle row.
pub struct TrainingDatasetMaterialization<'a> {
    pub feature_schema_hash: &'a ContentHash,
    pub factor_schema_hash: &'a ContentHash,
    pub label_schema_hash: &'a ContentHash,
    pub dataset_hash: &'a ContentHash,
    pub manifest_hash: &'a ContentHash,
    pub manifest: &'a DatasetManifest,
    pub artifact_bytes_hash: &'a ContentHash,
    pub parquet_uri: &'a ArtifactUri,
    pub sample_count: i64,
    pub coverage: &'a DatasetCoverage,
}

impl TrainingDatasetInfo {
    /// Return the complete artifact binding only when every materialized field
    /// is present. Callers must still enforce the lifecycle status they accept.
    #[must_use]
    pub fn materialization(&self) -> Option<TrainingDatasetMaterialization<'_>> {
        Some(TrainingDatasetMaterialization {
            feature_schema_hash: self.feature_schema_hash.as_ref()?,
            factor_schema_hash: self.factor_schema_hash.as_ref()?,
            label_schema_hash: self.label_schema_hash.as_ref()?,
            dataset_hash: self.dataset_hash.as_ref()?,
            manifest_hash: self.manifest_hash.as_ref()?,
            manifest: self.manifest_json.as_ref()?,
            artifact_bytes_hash: self.artifact_bytes_hash.as_ref()?,
            parquet_uri: self.parquet_uri.as_ref()?,
            sample_count: self.sample_count?,
            coverage: self.coverage_json.as_ref()?,
        })
    }
}
