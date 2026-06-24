//! `quant_training_dataset` table entity.

use crate::{
    enums::quant::TrainingDatasetStatus,
    types::{ArtifactUri, ContentHash, ModelSpecId, RuntimeConfigVersionId, TrainingDatasetId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_training_dataset")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
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
    #[sea_orm(column_type = "JsonBinary")]
    pub coverage_json: Json,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_model_spec::Entity",
        from = "Column::ModelSpecId",
        to = "super::quant_model_spec::Column::ModelSpecId"
    )]
    ModelSpec,
    #[sea_orm(has_many = "super::quant_model_version::Entity")]
    ModelVersion,
}

impl Related<super::quant_model_spec::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelSpec.def()
    }
}

impl Related<super::quant_model_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelVersion.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
