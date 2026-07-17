//! `quant_training_dataset` table entity.

use crate::{
    enums::quant::{DatasetPurpose, TrainingDatasetStatus},
    types::{
        ArtifactUri, ContentHash, DatasetCoverage, DatasetManifest, ModelSpecId,
        RuntimeConfigVersionId, SchemaVersion, TrainingDatasetId, TrainingHorizonsSecs,
        TrainingSampleSources,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_training_dataset")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub training_dataset_id: TrainingDatasetId,
    pub model_spec_id: ModelSpecId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: TrainingDatasetStatus,
    pub purpose: DatasetPurpose,
    pub feature_schema_hash: Option<ContentHash>,
    pub factor_schema_hash: Option<ContentHash>,
    pub label_schema_hash: Option<ContentHash>,
    pub dataset_hash: Option<ContentHash>,
    pub manifest_hash: Option<ContentHash>,
    #[sea_orm(column_type = "JsonBinary")]
    pub manifest_json: Option<DatasetManifest>,
    pub artifact_bytes_hash: Option<ContentHash>,
    pub parquet_uri: Option<ArtifactUri>,
    pub sample_count: Option<i64>,
    pub knowledge_lag_secs: i64,
    pub sample_interval_secs: i64,
    #[sea_orm(column_type = "JsonBinary")]
    pub horizons_secs: TrainingHorizonsSecs,
    pub feature_schema_version: Option<SchemaVersion>,
    #[sea_orm(column_type = "JsonBinary")]
    pub sample_sources: Option<TrainingSampleSources>,
    #[sea_orm(column_type = "JsonBinary")]
    pub coverage_json: Option<DatasetCoverage>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub failure_detail: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelSpec",
        from = "model_spec_id",
        to = "model_spec_id"
    )]
    pub model_spec: BelongsTo<super::quant_model_spec::Entity>,
    #[sea_orm(has_many, relation_enum = "ModelVersion")]
    pub model_version: HasMany<super::quant_model_version::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
