//! Append-only, bitemporal Gamma catalog versions.

use crate::{
    domain::{UpsertEvent, UpsertMarket},
    entities::{event_catalog_version, market_catalog_version},
    types::{
        CatalogSyncBatchId, ContentHash, EventCatalogVersionId, EventId, MarketCatalogVersionId,
        MarketId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Which Gamma synchronization produced a committed catalog batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogSyncKind {
    Full,
    Incremental,
}

/// Durable lifecycle of one catalog synchronization attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogSyncStatus {
    Preparing,
    Committed,
    Failed,
}

impl CatalogSyncStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Preparing => "preparing",
            Self::Committed => "committed",
            Self::Failed => "failed",
        }
    }
}

/// Stable stage taxonomy for failed catalog attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogSyncFailureStage {
    Fetch,
    Prepare,
    Persist,
    CommitVisibility,
    Recovery,
}

impl CatalogSyncFailureStage {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fetch => "fetch",
            Self::Prepare => "prepare",
            Self::Persist => "persist",
            Self::CommitVisibility => "commit_visibility",
            Self::Recovery => "recovery",
        }
    }
}

impl CatalogSyncKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Incremental => "incremental",
        }
    }
}

/// Provenance quality of a catalog row's effective timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogTimestampQuality {
    Source,
    AvailableAtFallback,
}

impl CatalogTimestampQuality {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::AvailableAtFallback => "available_at_fallback",
        }
    }
}

/// Origin recorded for versions committed by the transactional Gamma writer.
pub const CATALOG_ORIGIN_GAMMA_SYNC: &str = "gamma_sync";

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::catalog_sync_batch::Entity")]
pub struct CatalogSyncBatchInfo {
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

info_from_model!(CatalogSyncBatchInfo, crate::entities::catalog_sync_batch::Model, {
    catalog_sync_batch_id,
    sync_kind,
    status,
    source_cursor,
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
    pub sync_kind: String,
    pub source_cursor: Option<DateTime<Utc>>,
    pub started_at: DateTime<Utc>,
    pub fetched_at: DateTime<Utc>,
    pub event_count: i64,
    pub market_count: i64,
    pub rejected_count: i64,
    pub batch_hash: ContentHash,
}

/// Immutable audit payload for an attempt that failed before a catalog commit.
#[derive(Debug, Clone)]
pub struct NewFailedCatalogSyncBatch {
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub sync_kind: String,
    pub source_cursor: Option<DateTime<Utc>>,
    pub started_at: DateTime<Utc>,
    pub fetched_at: Option<DateTime<Utc>>,
    pub failure_stage: CatalogSyncFailureStage,
    pub failure_detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::event_catalog_version::Entity")]
pub struct EventCatalogVersionInfo {
    pub event_catalog_version_id: EventCatalogVersionId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub event_id: EventId,
    pub source_effective_at: DateTime<Utc>,
    pub source_timestamp_quality: String,
    pub available_at: DateTime<Utc>,
    pub origin: String,
    pub content_hash: ContentHash,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    EventCatalogVersionInfo,
    event_catalog_version::Model,
    {
        event_catalog_version_id,
        catalog_sync_batch_id,
        event_id,
        source_effective_at,
        source_timestamp_quality,
        available_at,
        origin,
        content_hash,
        payload,
        created_at,
    }
);

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::event_catalog_version::ActiveModel")]
pub struct NewEventCatalogVersion {
    pub event_catalog_version_id: EventCatalogVersionId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub event_id: EventId,
    pub source_effective_at: DateTime<Utc>,
    pub source_timestamp_quality: String,
    pub available_at: DateTime<Utc>,
    pub origin: String,
    pub content_hash: ContentHash,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::market_catalog_version::Entity")]
pub struct MarketCatalogVersionInfo {
    pub market_catalog_version_id: MarketCatalogVersionId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub event_catalog_version_id: EventCatalogVersionId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub source_effective_at: DateTime<Utc>,
    pub source_timestamp_quality: String,
    pub source_created_at: Option<DateTime<Utc>>,
    pub available_at: DateTime<Utc>,
    pub origin: String,
    pub content_hash: ContentHash,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    MarketCatalogVersionInfo,
    market_catalog_version::Model,
    {
        market_catalog_version_id,
        catalog_sync_batch_id,
        event_catalog_version_id,
        market_id,
        event_id,
        source_effective_at,
        source_timestamp_quality,
        source_created_at,
        available_at,
        origin,
        content_hash,
        payload,
        created_at,
    }
);

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::market_catalog_version::ActiveModel")]
pub struct NewMarketCatalogVersion {
    pub market_catalog_version_id: MarketCatalogVersionId,
    pub catalog_sync_batch_id: CatalogSyncBatchId,
    pub event_catalog_version_id: EventCatalogVersionId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub source_effective_at: DateTime<Utc>,
    pub source_timestamp_quality: String,
    pub source_created_at: Option<DateTime<Utc>>,
    pub available_at: DateTime<Utc>,
    pub origin: String,
    pub content_hash: ContentHash,
    pub payload: serde_json::Value,
}

/// Atomic write payload: current projections and immutable versions commit together.
pub struct CatalogCommit {
    pub batch: NewCatalogSyncBatch,
    pub current_events: Vec<UpsertEvent>,
    pub event_versions: Vec<NewEventCatalogVersion>,
    pub current_markets: Vec<UpsertMarket>,
    pub market_versions: Vec<NewMarketCatalogVersion>,
}

/// One transactionally consistent point-in-time catalog snapshot.
///
/// `market` and `event` are linked by the immutable
/// `event_catalog_version_id`. `event_markets` contains the latest visible
/// version of every materialized market named by that exact event version.
/// Missing members are intentionally omitted so callers can preserve the
/// event's expected membership count and fail closed.
#[derive(Debug, Clone)]
pub struct CatalogSnapshotInfo {
    pub market: MarketCatalogVersionInfo,
    pub event: EventCatalogVersionInfo,
    pub event_markets: Vec<MarketCatalogVersionInfo>,
}

/// Immutable catalog revisions required to replay a bounded historical window.
///
/// The repository returns every visible revision through the supplied end
/// boundary, including pre-window baselines and all event members referenced by
/// those revisions. The materialized PIT engine applies each sample's own
/// boundary in memory without touching Postgres.
#[derive(Debug, Clone)]
pub struct CatalogWindowInfo {
    pub market_versions: Vec<MarketCatalogVersionInfo>,
    pub event_versions: Vec<EventCatalogVersionInfo>,
}

/// Ordered committed catalog batches proving the ledger prefix used by a
/// source slice.
///
/// The first row is always a complete synchronization committed no later than
/// the requested source window start. Every later committed batch through the
/// PIT cutoff is retained in canonical commit order. The materializer hashes
/// this exact vector; it never substitutes mutable catalog projections.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogBatchChainInfo {
    pub batches: Vec<CatalogSyncBatchInfo>,
}
