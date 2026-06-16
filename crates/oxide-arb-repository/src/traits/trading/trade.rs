use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        EdgeBucket, MarketPerformanceRow, NewTrade, PageRequest, Paginated, ReportTradeStats,
        TradeAnalyticsFilter, TradeInfo, TradeObservation, TradePageQuery,
        evidence::EvidenceQueryResult,
    },
    enums::common::{TradeBusinessOutcome, TradeReconcileResolution, TradeState},
    types::{ExecutionId, MarketId, TradeId},
};
use std::collections::HashMap;

use crate::traits::timeseries::evidence_query_result;

#[async_trait::async_trait]
pub trait TradeRepository: Send + Sync {
    /// Record a new trade in `Intent` state. The repository assigns timestamps.
    async fn create(&self, trade: NewTrade) -> Result<TradeInfo, StorageError>;

    /// Batch-insert multiple trades, respecting `PostgreSQL` bind-variable limits.
    /// Returns the number of rows inserted.
    async fn create_batch(&self, trades: Vec<NewTrade>) -> Result<u64, StorageError>;

    /// `Intent` → `Submitted`: persist that the order was sent to the venue.
    /// Returns `true` if the row was in `Intent` and transitioned.
    async fn mark_submitted(
        &self,
        trade_id: &TradeId,
        submitted_at: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    /// `Intent`/`Submitted` → `*_observed`: write the venue result columns.
    async fn mark_observed(
        &self,
        trade_id: &TradeId,
        observation: TradeObservation,
    ) -> Result<(), StorageError>;

    /// `Orphaned` → `*_observed` after external reconciliation proves a venue
    /// outcome. The normal post-trade relay owns side effects after this write.
    async fn mark_reconciled_observed(
        &self,
        trade_id: &TradeId,
        observation: TradeObservation,
        resolution: TradeReconcileResolution,
        note: &str,
    ) -> Result<bool, StorageError> {
        let _ = (trade_id, observation, resolution, note);
        Err(StorageError::StaleData(
            "mark_reconciled_observed is not implemented for this repository".into(),
        ))
    }

    /// Lease a batch of unprocessed or expired-processing trades for relay work.
    ///
    /// Implementations must use one linearizing write (`UPDATE ... RETURNING` on
    /// `PostgreSQL`) so multiple relay instances cannot observe the same claim.
    async fn claim_unprocessed(
        &self,
        limit: u64,
        owner: &str,
        claimed_at: DateTime<Utc>,
        lease_expired_before: DateTime<Utc>,
    ) -> Result<Vec<TradeInfo>, StorageError>;

    /// Atomic state transition gate (the relay linearization point):
    /// `UPDATE ... SET state = to WHERE trade_id = ? AND state = from`.
    /// Returns `true` iff this caller performed the transition.
    async fn advance_state(
        &self,
        trade_id: &TradeId,
        from: TradeState,
        to: TradeState,
    ) -> Result<bool, StorageError>;

    /// `Submitted` → `Orphaned` and flag for reconciliation. Returns `true` if applied.
    async fn mark_orphaned(&self, trade_id: &TradeId) -> Result<bool, StorageError>;

    /// Trades stuck in `Submitted` older than `older_than` (orphan scan).
    async fn find_stale_submitted(
        &self,
        older_than: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError>;

    /// Trades whose venue outcome is unknown and must be reconciled before they
    /// can participate in business-outcome reporting or release pinned exposure.
    async fn find_needs_reconcile(&self, limit: u64) -> Result<Vec<TradeInfo>, StorageError> {
        let _ = limit;
        Err(StorageError::StaleData(
            "find_needs_reconcile is not implemented for this repository".into(),
        ))
    }

    /// Record a terminal reconciliation conclusion that could not safely be
    /// mapped into normal Fill/Miss post-trade processing.
    async fn mark_reconciled(
        &self,
        trade_id: &TradeId,
        resolution: TradeReconcileResolution,
        note: &str,
        reconciled_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let _ = (trade_id, resolution, note, reconciled_at);
        Err(StorageError::StaleData(
            "mark_reconciled is not implemented for this repository".into(),
        ))
    }

    async fn find_by_id(&self, trade_id: &TradeId) -> Result<Option<TradeInfo>, StorageError>;

    async fn find_by_execution(
        &self,
        execution_id: &ExecutionId,
    ) -> Result<Vec<TradeInfo>, StorageError>;

    /// Paginated, filtered list for the web trades dashboard (newest first).
    async fn page(&self, query: TradePageQuery) -> Result<Paginated<TradeInfo>, StorageError>;

    /// Detected-edge histogram over the window, aggregated SQL-side (no per-trade
    /// row load). Buckets are right-open basis-point ranges; rows with a NULL
    /// `detected_edge_bps` are excluded.
    async fn edge_histogram(
        &self,
        filter: TradeAnalyticsFilter,
    ) -> Result<Vec<EdgeBucket>, StorageError>;

    /// Per-market execution performance over the window: SQL `GROUP BY` with
    /// `ORDER BY net_profit_usd DESC` and server-side `LIMIT`/`OFFSET`.
    async fn market_performance(
        &self,
        filter: TradeAnalyticsFilter,
        page: PageRequest,
    ) -> Result<Paginated<MarketPerformanceRow>, StorageError>;

    async fn find_by_market(
        &self,
        market_id: &MarketId,
        limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError>;

    async fn find_recent(
        &self,
        since: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<TradeInfo>, StorageError>;

    async fn find_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<TradeInfo>, StorageError>;

    async fn find_between_evidence(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<TradeInfo>, StorageError> {
        let rows = self.find_between(start, end).await?;
        evidence_query_result(
            "TradeRepository",
            "find_between",
            &(start, end),
            vec!["created_at ASC".to_owned(), "trade_id ASC".to_owned()],
            Some(1),
            rows,
        )
    }

    /// Count trades grouped by `business_outcome` (NULL/in-flight rows excluded).
    async fn count_by_outcome(
        &self,
        since: DateTime<Utc>,
    ) -> Result<HashMap<TradeBusinessOutcome, i64>, StorageError>;

    async fn aggregate_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<ReportTradeStats, StorageError>;
}
