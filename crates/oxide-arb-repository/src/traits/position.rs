use chrono::{DateTime, Utc};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{
        MarkRedeemedParams, NewPosition, PositionInfo, PositionPatch, SettlePositionParams,
        SettledPositionStats,
    },
    enums::common::SettlementTrigger,
    types::{MarketId, PositionId, TokenId, TradeId, Usd},
};
use rust_decimal::Decimal;

#[async_trait::async_trait]
pub trait PositionRepository: Send + Sync {
    async fn find_open(&self) -> Result<Vec<PositionInfo>, StorageError>;

    async fn find_by_id(
        &self,
        position_id: &PositionId,
    ) -> Result<Option<PositionInfo>, StorageError>;

    async fn find_by_market(&self, market_id: &MarketId)
    -> Result<Vec<PositionInfo>, StorageError>;

    async fn find_open_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PositionInfo>, StorageError>;

    async fn find_by_trade_id(
        &self,
        trade_id: &TradeId,
    ) -> Result<Option<PositionInfo>, StorageError>;

    async fn find_redeem_retry_candidates(
        &self,
        max_attempts: u32,
    ) -> Result<Vec<PositionInfo>, StorageError>;

    async fn find_open_for_resolved_markets(
        &self,
        limit: u64,
    ) -> Result<Vec<PositionInfo>, StorageError>;

    async fn find_accounting_retry_candidates(
        &self,
        max_attempts: u32,
    ) -> Result<Vec<PositionInfo>, StorageError>;

    /// Open a new position. The repository assigns `position_id` and `opened_at`.
    async fn create(&self, position: NewPosition) -> Result<PositionInfo, StorageError>;

    /// Apply partial updates to a position (shares, pnl, status, close/settle time).
    async fn update(
        &self,
        position_id: &PositionId,
        patch: PositionPatch,
    ) -> Result<PositionInfo, StorageError>;

    async fn close_position(
        &self,
        position_id: &PositionId,
        realized_pnl: Decimal,
    ) -> Result<(), StorageError>;

    async fn settle_position(
        &self,
        position_id: &PositionId,
        params: SettlePositionParams,
    ) -> Result<PositionInfo, StorageError>;

    async fn mark_redeemed(
        &self,
        position_id: &PositionId,
        params: MarkRedeemedParams,
    ) -> Result<PositionInfo, StorageError>;

    async fn mark_accounted(
        &self,
        position_id: &PositionId,
        accounted_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<PositionInfo, StorageError>;

    async fn mark_accounting_failed(
        &self,
        position_id: &PositionId,
        error: String,
    ) -> Result<PositionInfo, StorageError>;

    async fn record_redeem_failure(
        &self,
        position_id: &PositionId,
        attempts: u32,
        winning_token_id: &TokenId,
        settlement_trigger: SettlementTrigger,
    ) -> Result<PositionInfo, StorageError>;

    async fn patch_oracle_verdict(
        &self,
        position_id: &PositionId,
        verdict: serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn total_exposure(&self) -> Result<Usd, StorageError>;

    async fn count_open(&self) -> Result<usize, StorageError>;

    async fn aggregate_settled_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<SettledPositionStats, StorageError>;
}
