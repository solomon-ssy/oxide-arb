//! `quant_model_spec` table entity.

use crate::{enums::quant::ModelPublicationStatus, types::ModelSpecId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_spec")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub model_spec_id: ModelSpecId,
    #[sea_orm(column_type = "Text", unique)]
    pub name: String,
    #[sea_orm(column_type = "Text")]
    pub model_family: String,
    pub prediction_horizon_secs: i64,
    pub feature_schema_version: i32,
    pub label_schema_version: i32,
    #[sea_orm(column_type = "JsonBinary")]
    pub spec_json: Json,
    pub status: ModelPublicationStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::quant_model_version::Entity")]
    ModelVersion,
}

impl Related<super::quant_model_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelVersion.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
