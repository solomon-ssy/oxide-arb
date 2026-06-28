use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{PositionExit, PositionFill, PositionInfo},
    types::{MarketId, OrderIntentId, TokenId},
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
}
