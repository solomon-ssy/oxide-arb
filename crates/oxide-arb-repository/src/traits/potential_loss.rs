use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::{NewPotentialLoss, PotentialLossInfo, ResolvePotentialLoss},
    types::{LedgerId, MarketId, Usd},
};

#[async_trait::async_trait]
pub trait PotentialLossRepository: Send + Sync {
    async fn create(&self, entry: NewPotentialLoss) -> Result<PotentialLossInfo, StorageError>;

    async fn find_active(&self) -> Result<Vec<PotentialLossInfo>, StorageError>;

    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PotentialLossInfo>, StorageError>;

    async fn resolve(
        &self,
        ledger_id: &LedgerId,
        command: ResolvePotentialLoss,
    ) -> Result<PotentialLossInfo, StorageError>;

    async fn total_active_loss(&self) -> Result<Usd, StorageError>;
}
