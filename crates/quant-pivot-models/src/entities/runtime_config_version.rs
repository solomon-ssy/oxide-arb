//! `runtime_config_version` table entity.

use crate::{
    enums::runtime_config::RuntimeConfigVersionSource,
    types::{ContentHash, RuntimeConfigVersionId, SchemaVersion},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "runtime_config_version")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub runtime_config_version_id: RuntimeConfigVersionId,
    #[sea_orm(unique)]
    pub config_hash: ContentHash,
    pub schema_version: SchemaVersion,
    #[sea_orm(column_type = "JsonBinary")]
    pub config_json: Json,
    pub source: RuntimeConfigVersionSource,
    #[sea_orm(column_type = "Text")]
    pub created_by: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub created_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "Approval")]
    pub approval: HasMany<super::runtime_config_approval::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
