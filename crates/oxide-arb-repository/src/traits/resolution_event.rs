use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    domain::settlement::{NewResolutionEvent, ResolutionEventInfo},
    types::MarketId,
};

#[async_trait::async_trait]
pub trait ResolutionEventRepository: Send + Sync {
    async fn append(&self, event: NewResolutionEvent) -> Result<(), StorageError>;

    async fn latest_for_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Option<ResolutionEventInfo>, StorageError>;

    async fn latest_by_source(
        &self,
        market_id: &MarketId,
        source: &str,
    ) -> Result<Option<ResolutionEventInfo>, StorageError>;
}
