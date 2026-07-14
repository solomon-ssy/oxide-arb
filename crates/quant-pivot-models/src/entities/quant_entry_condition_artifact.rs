//! Immutable entry-condition artifact ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::types::{ContentHash, EntryConditionArtifactId, EntryConditionArtifactV1};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_entry_condition_artifact")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub artifact_id: EntryConditionArtifactId,
    pub content_hash: ContentHash,
    pub schema_version: i32,
    pub evaluator_version: i32,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload_json: EntryConditionArtifactV1,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::quant_entry_condition_instance::Entity")]
    Instance,
}

impl Related<super::quant_entry_condition_instance::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Instance.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
