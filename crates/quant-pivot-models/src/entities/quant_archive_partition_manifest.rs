//! Immutable manifest for one verified `ClickHouse` Parquet partition archive.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use uuid::Uuid;

use crate::types::{ArtifactUri, ContentHash};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_archive_partition_manifest")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub manifest_id: Uuid,
    pub table_name: String,
    pub partition_key: String,
    pub retention_days: i32,
    pub row_count: i64,
    pub parquet_uri: ArtifactUri,
    pub byte_hash: ContentHash,
    pub content_hash: ContentHash,
    pub manifest_hash: ContentHash,
    pub sealed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_one = "super::quant_archive_partition_drop_command::Entity")]
    DropCommand,
    #[sea_orm(has_one = "super::quant_archive_partition_drop_audit::Entity")]
    DropAudit,
}

impl Related<super::quant_archive_partition_drop_audit::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::DropAudit.def()
    }
}

impl Related<super::quant_archive_partition_drop_command::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::DropCommand.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
