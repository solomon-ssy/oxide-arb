use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{PositionExit, PositionFill, PositionInfo},
    types::{MarketId, TokenId},
};

/// Current-position ledger persistence port.
#[async_trait::async_trait]
pub trait PositionRepository: Send + Sync {
    async fn apply_fill(&self, fill: PositionFill) -> Result<PositionInfo, StorageError>;

    async fn apply_exit(
        &self,
        token_id: &TokenId,
        exit: PositionExit,
    ) -> Result<PositionInfo, StorageError>;

    async fn find_by_token(&self, token_id: &TokenId)
    -> Result<Option<PositionInfo>, StorageError>;

    async fn find_open_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PositionInfo>, StorageError>;
}
