//! Web-facing read port for execution orders, position lots, and attribution.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        ExecutionOrderInfo, ExecutionOrderListQuery, Paginated, PositionInfo, PositionListQuery,
        RecommendationAttributionInfo,
    },
    types::{ExecutionOrderId, PositionId, RecommendationId},
};

#[async_trait]
pub trait ExecutionReadPort: Send + Sync {
    async fn list_execution_orders(
        &self,
        query: ExecutionOrderListQuery,
    ) -> QuantResult<Paginated<ExecutionOrderInfo>>;

    async fn get_execution_order(
        &self,
        id: &ExecutionOrderId,
    ) -> QuantResult<Option<ExecutionOrderInfo>>;

    async fn list_positions(
        &self,
        query: PositionListQuery,
    ) -> QuantResult<Paginated<PositionInfo>>;

    async fn get_position(&self, id: &PositionId) -> QuantResult<Option<PositionInfo>>;

    async fn get_recommendation_attribution(
        &self,
        id: &RecommendationId,
    ) -> QuantResult<Option<RecommendationAttributionInfo>>;
}
