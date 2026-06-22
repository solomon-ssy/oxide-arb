use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        MarkRedeemedParams, NewPosition, Paginated, PositionInfo, PositionPageQuery, PositionPatch,
        SettlePositionParams, SettledPositionStats, evidence::EvidenceQueryResult,
        position::PositionRedeemSnapshot,
    },
    enums::{LegacyExecutionMode, common::SettlementTrigger},
    types::{MarketId, PositionId, TokenId, TradeId, Usd},
};
use rust_decimal::Decimal;

use crate::traits::timeseries::evidence_query_result;

#[async_trait::async_trait]
pub trait PositionRepository: Send + Sync {
    /// Paginated, filtered list for the web positions dashboard (newest first).
    async fn page(&self, query: PositionPageQuery)
    -> Result<Paginated<PositionInfo>, StorageError>;

    /// Open positions for one execution mode.
    ///
    /// Active exposure is always mode-contextual: Live risk must never count
    /// simulated (dry-run/paper) positions, and vice versa.
    async fn find_open(&self, mode: LegacyExecutionMode)
    -> Result<Vec<PositionInfo>, StorageError>;

    async fn open_as_of(&self, at: DateTime<Utc>) -> Result<Vec<PositionInfo>, StorageError>;

    async fn open_as_of_evidence(
        &self,
        at: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<PositionInfo>, StorageError> {
        let rows = self.open_as_of(at).await?;
        evidence_query_result(
            "PositionRepository",
            "open_as_of",
            &at,
            vec!["opened_at ASC".to_owned(), "position_id ASC".to_owned()],
            Some(1),
            rows,
        )
    }

    async fn changed_between(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<PositionInfo>, StorageError>;

    async fn changed_between_evidence(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<EvidenceQueryResult<PositionInfo>, StorageError> {
        let rows = self.changed_between(start, end).await?;
        evidence_query_result(
            "PositionRepository",
            "changed_between",
            &(start, end),
            vec!["opened_at ASC".to_owned(), "position_id ASC".to_owned()],
            Some(1),
            rows,
        )
    }

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

    /// Open positions awaiting on-chain redeem (any execution mode).
    async fn find_open_pending_redeem(&self) -> Result<Vec<PositionInfo>, StorageError>;

    /// Persist a backfilled or corrected redeem snapshot on an open position.
    async fn update_redeem_snapshot(
        &self,
        position_id: &PositionId,
        snapshot: &PositionRedeemSnapshot,
    ) -> Result<PositionInfo, StorageError>;

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

    async fn mark_redeem_terminal(
        &self,
        position_id: &PositionId,
        attempts: u32,
        winning_token_id: &TokenId,
        settlement_trigger: SettlementTrigger,
        reason: String,
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
