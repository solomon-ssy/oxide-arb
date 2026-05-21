use oxide_arb_error::storage::StorageError;
use oxide_arb_models::entities::potential_loss_ledger;
use oxide_arb_models::types::{MarketId, Usd};

pub trait PotentialLossRepository: Send + Sync {
    async fn record(
        &self,
        entry: potential_loss_ledger::ActiveModel,
    ) -> Result<potential_loss_ledger::Model, StorageError>;
    async fn find_active(&self) -> Result<Vec<potential_loss_ledger::Model>, StorageError>;
    async fn find_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<potential_loss_ledger::Model>, StorageError>;
    async fn resolve(&self, ledger_id: &str) -> Result<(), StorageError>;
    async fn total_active_loss(&self) -> Result<Usd, StorageError>;
}
