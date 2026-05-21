use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::domain::{NewTrade, UpdateTradeOutcome};
use oxide_arb_models::entities::trade;
use oxide_arb_models::types::{MarketId, TradeId};
use std::collections::HashMap;

pub trait TradeRepository: Send + Sync {
    /// Record a new trade. The repository assigns `trade_id` and timestamps.
    async fn create(&self, trade: NewTrade) -> Result<trade::Model, StorageError>;

    /// Batch-insert multiple trades, respecting `PostgreSQL` bind-variable limits.
    /// Returns the number of rows inserted.
    async fn create_batch(&self, trades: Vec<NewTrade>) -> Result<u64, StorageError>;

    /// Update a trade's execution outcome fields.
    async fn update_outcome(
        &self,
        trade_id: &TradeId,
        update: UpdateTradeOutcome,
    ) -> Result<trade::Model, StorageError>;

    async fn find_by_id(&self, trade_id: &TradeId) -> Result<Option<trade::Model>, StorageError>;

    async fn find_by_execution(
        &self,
        execution_id: &str,
    ) -> Result<Vec<trade::Model>, StorageError>;

    async fn find_by_market(
        &self,
        market_id: &MarketId,
        limit: u64,
    ) -> Result<Vec<trade::Model>, StorageError>;

    async fn find_recent(
        &self,
        since: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<trade::Model>, StorageError>;

    async fn count_by_outcome(
        &self,
        since: DateTime<Utc>,
    ) -> Result<HashMap<String, i64>, StorageError>;
}
