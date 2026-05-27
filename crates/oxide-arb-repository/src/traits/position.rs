use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{NewPosition, PositionInfo, UpdatePosition},
    types::{MarketId, PositionId, Usd},
};
use rust_decimal::Decimal;

pub trait PositionRepository: Send + Sync {
    async fn find_open(&self) -> Result<Vec<PositionInfo>, StorageError>;

    async fn find_by_id(
        &self,
        position_id: &PositionId,
    ) -> Result<Option<PositionInfo>, StorageError>;

    async fn find_by_market(&self, market_id: &MarketId)
    -> Result<Vec<PositionInfo>, StorageError>;

    /// Open a new position. The repository assigns `position_id` and `opened_at`.
    async fn create(&self, position: NewPosition) -> Result<PositionInfo, StorageError>;

    /// Apply partial updates to a position (shares, pnl, status, close/settle time).
    async fn update(
        &self,
        position_id: &PositionId,
        update: UpdatePosition,
    ) -> Result<PositionInfo, StorageError>;

    async fn close_position(
        &self,
        position_id: &PositionId,
        realized_pnl: Decimal,
    ) -> Result<(), StorageError>;

    async fn settle_position(
        &self,
        position_id: &PositionId,
        realized_pnl: Decimal,
    ) -> Result<(), StorageError>;

    async fn total_exposure(&self) -> Result<Usd, StorageError>;

    async fn count_open(&self) -> Result<usize, StorageError>;
}
