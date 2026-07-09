//! `quant_model_version` table entity.

use crate::{
    enums::quant::PublicationStatus,
    types::{BacktestPathSetId, ContentHash, ModelSpecId, ModelVersionId, TrainingDatasetId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_version")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub version: i32,
    pub artifact_hash: ContentHash,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub publish_path_set_id: Option<BacktestPathSetId>,
    #[sea_orm(column_type = "JsonBinary")]
    pub metrics_json: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub training_objective_json: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub quality_gate_report: Json,
    pub publication_status: PublicationStatus,
    pub published_at: Option<DateTime<Utc>>,
    pub retired_at: Option<DateTime<Utc>>,
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
    #[sea_orm(
        belongs_to = "super::quant_training_dataset::Entity",
        from = "Column::TrainingDatasetId",
        to = "super::quant_training_dataset::Column::TrainingDatasetId"
    )]
    TrainingDataset,
    #[sea_orm(has_many = "super::quant_model_run::Entity")]
    ModelRun,
}

impl Related<super::quant_model_spec::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelSpec.def()
    }
}

impl Related<super::quant_training_dataset::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::TrainingDataset.def()
    }
}

impl Related<super::quant_model_run::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelRun.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
