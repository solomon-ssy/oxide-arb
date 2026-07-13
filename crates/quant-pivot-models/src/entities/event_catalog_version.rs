//! `event_catalog_version` immutable normalized event snapshots.

use crate::types::{CatalogSyncBatchId, ContentHash, EventCatalogVersionId, EventId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "event_catalog_version")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub event_catalog_version_id: EventCatalogVersionId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub event_id: EventId,
    pub source_effective_at: DateTime<Utc>,
    pub source_timestamp_quality: String,
    pub available_at: DateTime<Utc>,
    pub origin: String,
    pub content_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload: Json,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::catalog_sync_batch::Entity",
        from = "Column::CatalogSyncBatchId",
        to = "super::catalog_sync_batch::Column::CatalogSyncBatchId"
    )]
    CatalogSyncBatch,
    #[sea_orm(has_many = "super::market_catalog_version::Entity")]
    MarketCatalogVersion,
}

impl Related<super::catalog_sync_batch::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::CatalogSyncBatch.def()
    }
}

impl Related<super::market_catalog_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::MarketCatalogVersion.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
