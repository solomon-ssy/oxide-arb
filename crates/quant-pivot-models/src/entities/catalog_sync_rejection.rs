//! Typed catalog input rejections retained with the failed sync attempt.

use crate::{
    enums::catalog::{CatalogEntityKind, CatalogRejectionReason},
    types::{CatalogSyncBatchId, CatalogSyncRejectionId, ExternalJsonDocument},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "catalog_sync_rejection")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub catalog_sync_rejection_id: CatalogSyncRejectionId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub entity_kind: CatalogEntityKind,
    pub source_id: Option<String>,
    pub reason_code: CatalogRejectionReason,
    pub detail: String,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub raw_payload: Option<ExternalJsonDocument>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "CatalogSyncBatch",
        from = "catalog_sync_batch_id",
        to = "catalog_sync_batch_id"
    )]
    pub catalog_sync_batch: BelongsTo<super::catalog_sync_batch::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
