use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::{PositionListQuery, PositionSummary},
        pagination::Paginated,
        quant::{
            ExitTrainingLotRow, NewStrategyPositionLot, PositionExit, PositionFill,
            StrategyPositionLot,
        },
    },
    types::{ExecutionAccountId, MarketId, OrderIntentId, StrategyPositionLotId, TokenId, Usd},
};

/// Current-position ledger persistence port (one lot per filled entry intent).
#[async_trait::async_trait]
pub trait StrategyPositionLotRepository: Send + Sync {
    /// Insert one account-recovery or opening-inventory lot.
    async fn create_recovery_lot(
        &self,
        lot: NewStrategyPositionLot,
    ) -> Result<StrategyPositionLot, StorageError>;

    /// Upsert the per-intent lot from a fill (weighted-average over the same
    /// intent's fills — exact lot cost, never blended across intents).
    async fn apply_fill(&self, fill: PositionFill) -> Result<StrategyPositionLot, StorageError>;

    /// Reduce or close the per-intent lot from an exit fill.
    async fn apply_exit(
        &self,
        order_intent_id: &OrderIntentId,
        exit: PositionExit,
    ) -> Result<StrategyPositionLot, StorageError>;

    /// The single lot for an entry intent, if it has filled.
    async fn find_by_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<Option<StrategyPositionLot>, StorageError>;

    /// Read-view lookup: the lot joined with its originating recommendation.
    async fn find_by_id(
        &self,
        strategy_position_lot_id: &StrategyPositionLotId,
    ) -> Result<Option<PositionSummary>, StorageError>;

    /// Read-view page: each lot joined with its originating recommendation.
    async fn page(
        &self,
        query: PositionListQuery,
    ) -> Result<Paginated<PositionSummary>, StorageError>;

    /// All open (`Open`/`Closing`) lots — the exit monitor's scan source.
    async fn find_open_lots(&self) -> Result<Vec<StrategyPositionLot>, StorageError>;

    /// All open lots owned by one immutable execution account.
    async fn find_account_open_lots(
        &self,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<Vec<StrategyPositionLot>, StorageError>;

    /// All lots for a token (per-token aggregate view; sum at the call site).
    async fn find_lots_by_token(
        &self,
        token_id: &TokenId,
    ) -> Result<Vec<StrategyPositionLot>, StorageError>;

    /// Open lots for one immutable execution account and market.
    async fn find_open_position(
        &self,
        market_id: &MarketId,
        execution_account_id: &ExecutionAccountId,
    ) -> Result<Vec<StrategyPositionLot>, StorageError>;

    /// Cumulative realized `PnL` over all strategy position lots.
    async fn realized_pnl_cumulative_usd(&self) -> Result<Usd, StorageError>;

    /// Closed/settled lots whose `closed_at` falls in `[closed_from, closed_to)`.
    async fn find_exit_training_lots(
        &self,
        closed_from: DateTime<Utc>,
        closed_to: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<ExitTrainingLotRow>, StorageError>;
}
