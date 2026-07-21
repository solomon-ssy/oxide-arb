use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        api::{PositionListQuery, PositionSummary},
        pagination::Paginated,
        quant::{ExitTrainingLotRow, PositionExit, PositionFill, PositionInfo},
    },
    types::{MarketId, OrderIntentId, PositionId, TokenId, Usd},
};

/// Current-position ledger persistence port (one lot per filled entry intent).
#[async_trait::async_trait]
pub trait PositionRepository: Send + Sync {
    /// Upsert the per-intent lot from a fill (weighted-average over the same
    /// intent's fills — exact lot cost, never blended across intents).
    async fn apply_fill(&self, fill: PositionFill) -> Result<PositionInfo, StorageError>;

    /// Reduce or close the per-intent lot from an exit fill.
    async fn apply_exit(
        &self,
        order_intent_id: &OrderIntentId,
        exit: PositionExit,
    ) -> Result<PositionInfo, StorageError>;

    /// The single lot for an entry intent, if it has filled.
    async fn find_by_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<Option<PositionInfo>, StorageError>;

    /// Read-view lookup: the lot joined with its originating recommendation.
    async fn find_by_id(
        &self,
        position_id: &PositionId,
    ) -> Result<Option<PositionSummary>, StorageError>;

    /// Read-view page: each lot joined with its originating recommendation.
    async fn page(
        &self,
        query: PositionListQuery,
    ) -> Result<Paginated<PositionSummary>, StorageError>;

    /// All open (`Open`/`Closing`) lots — the exit monitor's scan source.
    async fn find_open_lots(&self) -> Result<Vec<PositionInfo>, StorageError>;

    /// All lots for a token (per-token aggregate view; sum at the call site).
    async fn find_lots_by_token(
        &self,
        token_id: &TokenId,
    ) -> Result<Vec<PositionInfo>, StorageError>;

    async fn find_open_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PositionInfo>, StorageError>;

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
