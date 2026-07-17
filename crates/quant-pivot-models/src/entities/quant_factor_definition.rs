//! `quant_factor_definition` table entity.

use crate::{
    enums::{
        factor::{FactorDefinitionScope, FactorFamily},
        quant::PublicationStatus,
    },
    types::{ContentHash, FactorDefinitionId, SchemaVersion},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_factor_definition")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub factor_definition_id: FactorDefinitionId,
    #[sea_orm(column_type = "Text", unique)]
    pub definition_hash: ContentHash,
    #[sea_orm(column_type = "Text")]
    pub feature_contract_hash: ContentHash,
    #[sea_orm(column_type = "Text")]
    pub name: String,
    pub factor_family: FactorFamily,
    pub scope: FactorDefinitionScope,
    pub input_schema_version: SchemaVersion,
    pub output_schema_version: SchemaVersion,
    #[sea_orm(column_type = "JsonBinary")]
    pub definition_json: Json,
    pub status: PublicationStatus,
    pub created_by: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "FactorValue")]
    pub factor_value: HasMany<super::quant_factor_value::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
