//! Append-only market changes committed by Gamma reconciliation batches.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{catalog_event_change, catalog_market_object, catalog_sync_batch};
use crate::{
    enums::catalog::{CatalogChangeType, CatalogTimestampQuality},
    types::{
        CatalogEventChangeId, CatalogMarketChangeId, CatalogMarketObjectId, CatalogSyncBatchId,
        EventId, MarketId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "catalog_market_change")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_change_id: CatalogMarketChangeId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub event_change_id: CatalogEventChangeId,
    pub market_object_id: CatalogMarketObjectId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub source_effective_at: DateTime<Utc>,
    pub source_timestamp_quality: CatalogTimestampQuality,
    pub source_created_at: Option<DateTime<Utc>>,
    pub change_type: CatalogChangeType,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "CatalogSyncBatch",
        from = "catalog_sync_batch_id",
        to = "catalog_sync_batch_id"
    )]
    pub catalog_sync_batch: BelongsTo<catalog_sync_batch::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "EventChange",
        from = "event_change_id",
        to = "event_change_id"
    )]
    pub event_change: BelongsTo<catalog_event_change::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "MarketObject",
        from = "market_object_id",
        to = "market_object_id"
    )]
    pub market_object: BelongsTo<catalog_market_object::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
