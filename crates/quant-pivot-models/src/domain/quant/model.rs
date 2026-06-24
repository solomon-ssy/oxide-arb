//! Model registry persistence DTOs.

use crate::{
    enums::quant::{ModelPublicationStatus, ModelRunErrorCode, ModelRunKind, ModelRunStatus},
    types::{
        ContentHash, MarketSelectionId, ModelRunId, ModelSpecId, ModelVersionId,
        RuntimeConfigVersionId, SchemaVersion, TrainingDatasetId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Governed model specification row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_model_spec::Entity")]
pub struct ModelSpecInfo {
    pub model_spec_id: ModelSpecId,
    pub name: String,
    pub model_family: String,
    pub prediction_horizon_secs: i64,
    pub feature_schema_version: SchemaVersion,
    pub label_schema_version: SchemaVersion,
    pub spec_json: serde_json::Value,
    pub status: ModelPublicationStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(ModelSpecInfo, crate::entities::quant_model_spec::Model, {
    model_spec_id, name, model_family, prediction_horizon_secs, feature_schema_version,
    label_schema_version, spec_json, status, created_at, updated_at,
});

/// Insert payload for `quant_model_spec`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_model_spec::ActiveModel")]
pub struct NewModelSpec {
    pub model_spec_id: ModelSpecId,
    pub name: String,
    pub model_family: String,
    pub prediction_horizon_secs: i64,
    pub feature_schema_version: SchemaVersion,
    pub label_schema_version: SchemaVersion,
    pub spec_json: serde_json::Value,
    pub status: ModelPublicationStatus,
}

/// Published or candidate model version row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_model_version::Entity")]
pub struct ModelVersionInfo {
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub version: i32,
    pub artifact_hash: ContentHash,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub metrics_json: serde_json::Value,
    pub quality_gate_report: serde_json::Value,
    pub publication_status: ModelPublicationStatus,
    pub published_at: Option<DateTime<Utc>>,
    pub retired_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ModelVersionInfo,
    crate::entities::quant_model_version::Model,
    {
        model_version_id,
        model_spec_id,
        version,
        artifact_hash,
        training_dataset_id,
        metrics_json,
        quality_gate_report,
        publication_status,
        published_at,
        retired_at,
        created_at,
    }
);

/// Insert payload for `quant_model_version`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_model_version::ActiveModel")]
pub struct NewModelVersion {
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub version: i32,
    pub artifact_hash: ContentHash,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub metrics_json: serde_json::Value,
    pub quality_gate_report: serde_json::Value,
    pub publication_status: ModelPublicationStatus,
    pub published_at: Option<DateTime<Utc>>,
    pub retired_at: Option<DateTime<Utc>>,
}

/// Training, backtest, shadow, or inference run row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_model_run::Entity")]
pub struct ModelRunInfo {
    pub model_run_id: ModelRunId,
    pub run_kind: ModelRunKind,
    pub model_version_id: Option<ModelVersionId>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub market_selection_id: Option<MarketSelectionId>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: ModelRunStatus,
    pub input_hash: ContentHash,
    pub output_hash: Option<ContentHash>,
    pub metrics_json: serde_json::Value,
    pub error_code: Option<ModelRunErrorCode>,
    pub error_message: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

info_from_model!(ModelRunInfo, crate::entities::quant_model_run::Model, {
    model_run_id, run_kind, model_version_id, runtime_config_version_id,
    market_selection_id, window_start, window_end, status, input_hash, output_hash,
    metrics_json, error_code, error_message, started_at, finished_at,
});

/// Insert payload for `quant_model_run`.
///
/// Covers every `ActiveModel` column (no DB-managed timestamps); `SeaORM`'s derive
/// emits a redundant `..Default::default()` that triggers `needless_update`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_model_run::ActiveModel")]
pub struct NewModelRun {
    pub model_run_id: ModelRunId,
    pub run_kind: ModelRunKind,
    pub model_version_id: Option<ModelVersionId>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub market_selection_id: Option<MarketSelectionId>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: ModelRunStatus,
    pub input_hash: ContentHash,
    pub output_hash: Option<ContentHash>,
    pub metrics_json: serde_json::Value,
    pub error_code: Option<ModelRunErrorCode>,
    pub error_message: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

/// Runtime model-run aggregate before persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantModelRunModel {
    pub run: NewModelRun,
}
