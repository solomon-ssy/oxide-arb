//! Append-only event changes committed by Gamma reconciliation batches.

use crate::{
    enums::catalog::{CatalogChangeType, CatalogTimestampQuality},
    types::{CatalogEventChangeId, CatalogEventObjectId, CatalogSyncBatchId, EventId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "catalog_event_change")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub event_change_id: CatalogEventChangeId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub event_object_id: CatalogEventObjectId,
    pub event_id: EventId,
    pub source_effective_at: DateTime<Utc>,
    pub source_timestamp_quality: CatalogTimestampQuality,
    pub change_type: CatalogChangeType,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "CatalogSyncBatch",
        from = "catalog_sync_batch_id",
        to = "catalog_sync_batch_id"
    )]
    pub catalog_sync_batch: BelongsTo<super::catalog_sync_batch::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "EventObject",
        from = "event_object_id",
        to = "event_object_id"
    )]
    pub event_object: BelongsTo<super::catalog_event_object::Entity>,
    #[sea_orm(has_many, relation_enum = "MarketChange")]
    pub market_change: HasMany<super::catalog_market_change::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
