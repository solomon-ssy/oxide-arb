//! Content-addressed Gamma catalog objects and append-only change ledger.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, FromQueryResult};
use serde::{Deserialize, Serialize};

use crate::{
    domain::market::{UpsertEvent, UpsertMarket},
    entities::{
        catalog_event_change, catalog_event_object, catalog_market_change, catalog_market_object,
        catalog_sync_rejection,
    },
    enums::catalog::{
        CatalogChangeType, CatalogEntityKind, CatalogRejectionReason, CatalogSyncFailureStage,
        CatalogSyncKind, CatalogSyncStatus, CatalogTimestampQuality,
    },
    types::{
        CatalogEventChangeId, CatalogEventObjectId, CatalogMarketChangeId, CatalogMarketObjectId,
        CatalogSyncBatchId, CatalogSyncRejectionId, ContentHash, EventId, ExternalJsonDocument,
        MarketId,
    },
};

pub const CATALOG_OBJECT_SCHEMA_VERSION: i32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult)]
pub struct CatalogSyncBatchInfo {
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
}

info_from_model!(CatalogSyncBatchInfo, crate::entities::catalog_sync_batch::Model, {
    catalog_sync_batch_id,
    sync_kind,
    status,
    started_at,
    fetched_at,
    committed_at,
    event_count,
    market_count,
    rejected_count,
    batch_hash,
    failure_stage,
    failure_detail,
    created_at,
    updated_at,
});

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::catalog_sync_batch::ActiveModel")]
pub struct NewCatalogSyncBatch {
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub sync_kind: CatalogSyncKind,
    pub started_at: DateTime<Utc>,
    pub fetched_at: DateTime<Utc>,
    pub event_count: i64,
    pub market_count: i64,
    pub rejected_count: i64,
    pub batch_hash: ContentHash,
}

/// Immutable audit payload for an attempt that failed before a catalog commit.
#[derive(Debug, Clone)]
pub struct CatalogBatchFailure {
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub sync_kind: CatalogSyncKind,
    pub started_at: DateTime<Utc>,
    pub fetched_at: Option<DateTime<Utc>>,
    pub failure_stage: CatalogSyncFailureStage,
    pub failure_detail: String,
    pub rejections: Vec<NewCatalogSyncRejection>,
}

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "catalog_event_object::ActiveModel")]
pub struct NewCatalogEventObject {
    pub event_object_id: CatalogEventObjectId,
    pub content_hash: ContentHash,
    pub schema_version: i32,
    pub payload: ExternalJsonDocument,
}

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "catalog_event_change::ActiveModel")]
pub struct NewCatalogEventChange {
    pub event_change_id: CatalogEventChangeId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub event_object_id: CatalogEventObjectId,
    pub event_id: EventId,
    pub source_effective_at: DateTime<Utc>,
    pub source_timestamp_quality: CatalogTimestampQuality,
    pub change_type: CatalogChangeType,
}

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "catalog_market_object::ActiveModel")]
pub struct NewCatalogMarketObject {
    pub market_object_id: CatalogMarketObjectId,
    pub content_hash: ContentHash,
    pub schema_version: i32,
    pub payload: ExternalJsonDocument,
}

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "catalog_market_change::ActiveModel")]
pub struct NewCatalogMarketChange {
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
}

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "catalog_sync_rejection::ActiveModel")]
pub struct NewCatalogSyncRejection {
    pub catalog_sync_rejection_id: CatalogSyncRejectionId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub entity_kind: CatalogEntityKind,
    pub source_id: Option<String>,
    pub reason_code: CatalogRejectionReason,
    pub detail: String,
    pub raw_payload: Option<ExternalJsonDocument>,
}

/// Candidate event object/change/projection evaluated against the current hash in the writer transaction.
#[derive(Debug, Clone)]
pub struct CatalogEventCandidate {
    pub projection: UpsertEvent,
    pub object: NewCatalogEventObject,
    pub change: NewCatalogEventChange,
}

/// Candidate market object/change/projection evaluated against the current hash in the writer transaction.
#[derive(Debug, Clone)]
pub struct CatalogMarketCandidate {
    pub projection: UpsertMarket,
    pub object: NewCatalogMarketObject,
    pub market_change_id: CatalogMarketChangeId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub event_object_id: CatalogEventObjectId,
    pub source_effective_at: DateTime<Utc>,
    pub source_timestamp_quality: CatalogTimestampQuality,
    pub source_created_at: Option<DateTime<Utc>>,
    pub change_type: CatalogChangeType,
}

/// Atomic write payload: objects, changes, and current projections commit together.
pub struct CatalogBatchCommit {
    pub batch: NewCatalogSyncBatch,
    pub events: Vec<CatalogEventCandidate>,
    pub markets: Vec<CatalogMarketCandidate>,
}

/// Joined event change and its immutable content object.
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult)]
pub struct CatalogEventChangeInfo {
    pub event_change_id: CatalogEventChangeId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub event_object_id: CatalogEventObjectId,
    pub event_id: EventId,
    pub source_effective_at: DateTime<Utc>,
    pub source_timestamp_quality: CatalogTimestampQuality,
    pub available_at: DateTime<Utc>,
    pub change_type: CatalogChangeType,
    pub content_hash: ContentHash,
    pub schema_version: i32,
    pub payload: ExternalJsonDocument,
    pub created_at: DateTime<Utc>,
}

/// Joined market change and its immutable content object.
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult)]
pub struct CatalogMarketChangeInfo {
    pub market_change_id: CatalogMarketChangeId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub event_change_id: CatalogEventChangeId,
    pub market_object_id: CatalogMarketObjectId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub source_effective_at: DateTime<Utc>,
    pub source_timestamp_quality: CatalogTimestampQuality,
    pub source_created_at: Option<DateTime<Utc>>,
    pub available_at: DateTime<Utc>,
    pub change_type: CatalogChangeType,
    pub content_hash: ContentHash,
    pub schema_version: i32,
    pub payload: ExternalJsonDocument,
    pub created_at: DateTime<Utc>,
}

/// One transactionally consistent point-in-time catalog snapshot.
#[derive(Debug, Clone)]
pub struct CatalogSnapshotInfo {
    pub market: CatalogMarketChangeInfo,
    pub event: Arc<CatalogEventChangeInfo>,
    pub event_markets: Arc<[CatalogMarketChangeInfo]>,
}

/// Immutable catalog changes required to replay a bounded historical window.
#[derive(Debug, Clone)]
pub struct CatalogWindowInfo {
    pub market_changes: Vec<CatalogMarketChangeInfo>,
    pub event_changes: Vec<CatalogEventChangeInfo>,
}

/// Ordered committed catalog batches proving the ledger prefix used by a source slice.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogBatchChainInfo {
    pub batches: Vec<CatalogSyncBatchInfo>,
}
