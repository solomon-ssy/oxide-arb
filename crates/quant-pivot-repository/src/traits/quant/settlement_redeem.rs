use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        ConfirmSettlementRedeem, NewSettlementRedeem, Paginated, SettlementRedeemInfo,
        SettlementRedeemListQuery, SettlementRedeemLotInfo, SettlementRedeemSummary,
    },
    types::{EvmAddress, EvmTransactionHash, MarketId, SettlementRedeemId},
};

#[async_trait::async_trait]
pub trait SettlementRedeemRepository: Send + Sync {
    async fn find_by_id(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Option<SettlementRedeemInfo>, StorageError>;

    async fn find_by_market_funder(
        &self,
        market_id: &MarketId,
        funder_address: &EvmAddress,
    ) -> Result<Option<SettlementRedeemInfo>, StorageError>;

    /// Page redeem batches, each carrying its joined `lot_count`.
    async fn page(
        &self,
        query: SettlementRedeemListQuery,
    ) -> Result<Paginated<SettlementRedeemSummary>, StorageError>;

    async fn list_lots_by_redeem(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
    ) -> Result<Vec<SettlementRedeemLotInfo>, StorageError>;

    async fn upsert_pending(
        &self,
        redeem: NewSettlementRedeem,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    async fn mark_submitted(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        tx_hash: EvmTransactionHash,
        submitted_at: DateTime<Utc>,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    async fn mark_failed(
        &self,
        settlement_redeem_id: &SettlementRedeemId,
        error: String,
        next_attempt_at: Option<DateTime<Utc>>,
        failed_at: DateTime<Utc>,
        manual_required: bool,
    ) -> Result<SettlementRedeemInfo, StorageError>;

    async fn confirm(
        &self,
        write: ConfirmSettlementRedeem,
    ) -> Result<SettlementRedeemInfo, StorageError>;
}
