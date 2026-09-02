//! Public read port for recommendation economic feedback.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        api::{
            EconomicHealthQuery, RecommendationEconomicOutcomeView,
            RecommendationExecutionComparisonView, RouteEconomicHealthView,
        },
        pagination::Paginated,
    },
    types::RecommendationId,
};

#[async_trait]
pub trait EconomicFeedbackPort: Send + Sync {
    async fn recommendation_outcome(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<Option<RecommendationEconomicOutcomeView>>;

    async fn execution_comparison(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<Option<RecommendationExecutionComparisonView>>;

    async fn route_health(
        &self,
        query: EconomicHealthQuery,
        available_through: DateTime<Utc>,
    ) -> QuantResult<Paginated<RouteEconomicHealthView>>;
}
