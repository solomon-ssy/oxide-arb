//! `catalog_sync_batch` append-only Gamma synchronization ledger.

use crate::types::{CatalogSyncBatchId, ContentHash};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "catalog_sync_batch")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub sync_kind: String,
    pub status: String,
    pub source_cursor: Option<DateTime<Utc>>,
    pub started_at: DateTime<Utc>,
    pub fetched_at: Option<DateTime<Utc>>,
    pub committed_at: Option<DateTime<Utc>>,
    pub event_count: i64,
    pub market_count: i64,
    pub rejected_count: i64,
    pub batch_hash: Option<ContentHash>,
    pub failure_stage: Option<String>,
    pub failure_detail: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::event_catalog_version::Entity")]
    EventCatalogVersion,
    #[sea_orm(has_many = "super::market_catalog_version::Entity")]
    MarketCatalogVersion,
}

impl Related<super::event_catalog_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::EventCatalogVersion.def()
    }
}

impl Related<super::market_catalog_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::MarketCatalogVersion.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
