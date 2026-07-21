use std::{collections::HashSet, sync::Arc};

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::MarketPageQuery,
        data_plane::DecisionBoundary,
        market::{
            CatalogBatchChainInfo, CatalogBatchCommit, CatalogBatchFailure, CatalogEventChangeInfo,
            CatalogMarketChangeInfo, CatalogSnapshotInfo, CatalogSyncBatchInfo, CatalogWindowInfo,
            MarketInfo, UpsertMarket,
        },
        pagination::Paginated,
    },
    enums::market::MarketStatus,
    types::{ClobMarketInfoVersion, EventId, HistoryCoverage, MarketId},
};

#[async_trait::async_trait]
pub trait MarketRepository: Send + Sync {
    async fn find_by_id(&self, id: &MarketId) -> Result<Option<Arc<MarketInfo>>, StorageError>;
    async fn find_by_ids(&self, ids: &[MarketId]) -> Result<Vec<Arc<MarketInfo>>, StorageError>;
    async fn page(&self, query: MarketPageQuery) -> Result<Paginated<MarketInfo>, StorageError>;
    async fn find_active(&self) -> Result<Arc<[MarketInfo]>, StorageError>;
    async fn find_by_event(&self, event_id: &str) -> Result<Vec<Arc<MarketInfo>>, StorageError>;
    async fn find_existing_ids(&self, ids: &[MarketId]) -> Result<HashSet<String>, StorageError>;
    async fn upsert(&self, market: UpsertMarket) -> Result<Arc<MarketInfo>, StorageError>;
    async fn upsert_batch(&self, markets: Vec<UpsertMarket>) -> Result<u64, StorageError>;
    async fn update_status(
        &self,
        id: &MarketId,
        status: MarketStatus,
        outcome: Option<&str>,
    ) -> Result<(), StorageError>;
}

/// Append-only bitemporal authority for CLOB market parameters and fees.
#[async_trait::async_trait]
pub trait ClobMarketInfoRepository: Send + Sync {
    /// Retention coverage of the append-only bitemporal source.
    async fn research_history_coverage(
        &self,
        _as_of: DateTime<Utc>,
    ) -> Result<Vec<HistoryCoverage>, StorageError> {
        Err(StorageError::invariant_violation(
            Some("clob_market_info_version"),
            "repository does not implement research history coverage",
        ))
    }

    async fn insert_observation(
        &self,
        observation: ClobMarketInfoVersion,
    ) -> Result<ClobMarketInfoVersion, StorageError>;

    async fn at(
        &self,
        market_id: &MarketId,
        effective_at: DateTime<Utc>,
        available_at_cutoff: DateTime<Utc>,
    ) -> Result<Option<ClobMarketInfoVersion>, StorageError>;

    /// Resolve at most one latest PIT-visible revision per market in one
    /// repository round trip. Implementations must return unique market ids.
    async fn at_many(
        &self,
        market_ids: &[MarketId],
        effective_at: DateTime<Utc>,
        available_at_cutoff: DateTime<Utc>,
    ) -> Result<Vec<ClobMarketInfoVersion>, StorageError>;

    async fn latest(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<ClobMarketInfoVersion>, StorageError>;

    /// Every PIT-visible version needed to replay a market page: the latest
    /// baseline before `effective_from`, followed by all versions in the
    /// half-open effective window.
    async fn window(
        &self,
        _market_ids: &[MarketId],
        _effective_from: DateTime<Utc>,
        _effective_to: DateTime<Utc>,
        _available_by: DateTime<Utc>,
    ) -> Result<Vec<ClobMarketInfoVersion>, StorageError> {
        Ok(Vec::new())
    }
}

/// Atomic writer and point-in-time reader for the immutable Gamma catalog ledger.
///
/// Historical/replay callers must use this repository instead of the mutable
/// `market` / `event` projections.
#[async_trait::async_trait]
pub trait CatalogLedgerRepository: Send + Sync {
    /// Retention coverage for both event and market change ledgers.
    async fn research_history_coverage(
        &self,
        _as_of: DateTime<Utc>,
    ) -> Result<Vec<HistoryCoverage>, StorageError> {
        Err(StorageError::invariant_violation(
            Some("catalog_sync_batch"),
            "repository does not implement research history coverage",
        ))
    }

    async fn commit(
        &self,
        commit: CatalogBatchCommit,
    ) -> Result<CatalogSyncBatchInfo, StorageError>;

    /// Persist a terminal failure for an attempt that never reached a durable commit.
    async fn record_failure(
        &self,
        failure: CatalogBatchFailure,
    ) -> Result<CatalogSyncBatchInfo, StorageError>;

    async fn coverage_start(&self) -> Result<Option<DateTime<Utc>>, StorageError>;

    /// Commit watermark of the newest complete catalog sync batch.
    async fn watermark(&self) -> Result<Option<DateTime<Utc>>, StorageError>;

    /// Freeze the complete committed batch chain visible to a source slice.
    ///
    /// Implementations return `None` when no complete baseline was committed
    /// by `window_start` or when no committed batch is visible by `pit_cutoff`.
    async fn batch_chain(
        &self,
        _window_start: DateTime<Utc>,
        _pit_cutoff: DateTime<Utc>,
    ) -> Result<Option<CatalogBatchChainInfo>, StorageError> {
        Ok(None)
    }

    async fn market_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> Result<Option<CatalogMarketChangeInfo>, StorageError>;

    async fn markets_at(
        &self,
        market_ids: &[MarketId],
        boundary: &DecisionBoundary,
    ) -> Result<Vec<CatalogMarketChangeInfo>, StorageError>;

    async fn event_at(
        &self,
        event_id: &EventId,
        boundary: &DecisionBoundary,
    ) -> Result<Option<CatalogEventChangeInfo>, StorageError>;

    async fn event_markets_at(
        &self,
        event_id: &EventId,
        boundary: &DecisionBoundary,
    ) -> Result<Vec<CatalogMarketChangeInfo>, StorageError>;

    /// Resolve market metadata, its exact event revision, and visible event
    /// membership from one repeatable-read database snapshot.
    ///
    /// Implementations must reject a decision before `coverage_start`; a
    /// mutable current projection is never an admissible fallback.
    async fn snapshot_at(
        &self,
        market_id: &MarketId,
        boundary: &DecisionBoundary,
    ) -> Result<Option<CatalogSnapshotInfo>, StorageError>;

    /// Resolve the complete market candidate set visible at one boundary in a
    /// single repeatable-read snapshot, including each exact event revision and
    /// its membership rows.
    async fn snapshots_at_boundary(
        &self,
        boundary: &DecisionBoundary,
    ) -> Result<Vec<CatalogSnapshotInfo>, StorageError>;

    /// Load every catalog revision needed to resolve `market_ids` and their
    /// event membership through `end_boundary` without database reads in the
    /// replay loop.
    async fn window_through(
        &self,
        market_ids: &[MarketId],
        end_boundary: &DecisionBoundary,
    ) -> Result<CatalogWindowInfo, StorageError>;
}
