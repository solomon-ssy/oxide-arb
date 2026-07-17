//! `quant_model_spec` table entity.

use crate::{
    enums::{model::ModelFamily, quant::PublicationStatus},
    types::{ModelInputContract, ModelSpecId, ModelTrainingContract, SchemaVersion},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_spec")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub model_spec_id: ModelSpecId,
    #[sea_orm(column_type = "Text", unique)]
    pub name: String,
    pub model_family: ModelFamily,
    pub prediction_horizon_secs: i64,
    pub feature_schema_version: SchemaVersion,
    pub label_schema_version: SchemaVersion,
    #[sea_orm(column_type = "JsonBinary")]
    pub spec_json: Json,
    /// Ordered raw features consumed by this model. Encoded columns are derived
    /// exclusively by the fitted input transform and cannot be persisted here.
    #[sea_orm(column_type = "JsonBinary")]
    pub input_contract: ModelInputContract,
    /// Frozen target and validation policy; train requests cannot override it.
    #[sea_orm(column_type = "JsonBinary")]
    pub training_contract: ModelTrainingContract,
    pub status: PublicationStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "ModelVersion")]
    pub model_version: HasMany<super::quant_model_version::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
