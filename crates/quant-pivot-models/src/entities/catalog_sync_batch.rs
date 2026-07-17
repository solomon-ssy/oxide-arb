//! `catalog_sync_batch` append-only Gamma synchronization ledger.

use crate::{
    enums::catalog::{CatalogSyncFailureStage, CatalogSyncKind, CatalogSyncStatus},
    types::{CatalogSyncBatchId, ContentHash},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "catalog_sync_batch")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub sync_kind: CatalogSyncKind,
    pub status: CatalogSyncStatus,
    pub started_at: DateTime<Utc>,
    pub fetched_at: Option<DateTime<Utc>>,
    pub committed_at: Option<DateTime<Utc>>,
    pub event_count: i64,
    pub market_count: i64,
    pub rejected_count: i64,
    pub batch_hash: Option<ContentHash>,
    pub failure_stage: Option<CatalogSyncFailureStage>,
    pub failure_detail: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "EventChange")]
    pub event_change: HasMany<super::catalog_event_change::Entity>,
    #[sea_orm(has_many, relation_enum = "MarketChange")]
    pub market_change: HasMany<super::catalog_market_change::Entity>,
    #[sea_orm(has_many, relation_enum = "SyncRejection")]
    pub sync_rejection: HasMany<super::catalog_sync_rejection::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
