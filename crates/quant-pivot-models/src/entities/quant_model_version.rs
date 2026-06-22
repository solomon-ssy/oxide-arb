//! `quant_model_version` table entity.

use crate::{
    enums::quant::ModelPublicationStatus,
    types::{ModelSpecId, ModelVersionId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_version")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub version: i32,
    #[sea_orm(column_type = "Text")]
    pub artifact_hash: String,
    pub training_dataset_id: Option<Uuid>,
    #[sea_orm(column_type = "JsonBinary")]
    pub metrics_json: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub quality_gate_report: Json,
    pub publication_status: ModelPublicationStatus,
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
    #[sea_orm(has_many = "super::quant_model_run::Entity")]
    ModelRun,
}

impl Related<super::quant_model_spec::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelSpec.def()
    }
}

impl Related<super::quant_model_run::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelRun.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
