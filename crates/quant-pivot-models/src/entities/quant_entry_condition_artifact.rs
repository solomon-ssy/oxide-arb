//! Immutable entry-condition artifact ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_entry_condition_instance;
use crate::types::{ContentHash, EntryConditionArtifactId, EntryConditionArtifactV1};

#[sea_orm::model]
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

    #[sea_orm(has_many, relation_enum = "Instance")]
    pub instance: HasMany<quant_entry_condition_instance::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
