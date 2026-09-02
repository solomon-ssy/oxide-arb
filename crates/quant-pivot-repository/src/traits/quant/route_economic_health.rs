use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{
        pagination::{PageWindow, Paginated},
        quant::{NewRouteEconomicHealth, RouteEconomicHealthInfo, RouteEconomicHealthSource},
    },
    runtime_config::BuyModelRoute,
    types::{ContentHash, ResearchProfileArtifactId},
};

#[async_trait::async_trait]
pub trait RouteEconomicHealthRepository: Send + Sync {
    async fn insert(
        &self,
        health: NewRouteEconomicHealth,
    ) -> Result<RouteEconomicHealthInfo, StorageError>;

    async fn latest(
        &self,
        route_identity_hash: &ContentHash,
        profile_id: &ResearchProfileArtifactId,
        available_through: DateTime<Utc>,
    ) -> Result<Option<RouteEconomicHealthInfo>, StorageError>;

    async fn latest_for_route(
        &self,
        route: &BuyModelRoute,
        available_through: DateTime<Utc>,
    ) -> Result<Option<RouteEconomicHealthInfo>, StorageError>;

    async fn page_for_route(
        &self,
        route: &BuyModelRoute,
        available_through: DateTime<Utc>,
        window: PageWindow,
    ) -> Result<Paginated<RouteEconomicHealthInfo>, StorageError>;

    async fn source_window(
        &self,
        route: &BuyModelRoute,
        profile_id: &ResearchProfileArtifactId,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        available_through: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<RouteEconomicHealthSource>, StorageError>;
}
