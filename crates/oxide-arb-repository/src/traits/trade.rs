use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{NewTrade, ReportTradeStats, TradeInfo, TradeObservation},
    enums::common::{TradeBusinessOutcome, TradeState},
    types::{MarketId, TradeId},
};
use std::collections::HashMap;

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

    async fn find_by_id(&self, trade_id: &TradeId) -> Result<Option<TradeInfo>, StorageError>;

    async fn find_by_execution(&self, execution_id: &str) -> Result<Vec<TradeInfo>, StorageError>;

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
