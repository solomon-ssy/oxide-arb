//! Core implementation of [`ExecutionReadPort`] for the Admin API.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        ExecutionOrderInfo, ExecutionOrderListQuery, ExecutionReadPort, Paginated, PositionInfo,
        PositionListQuery, RecommendationAttributionInfo,
    },
    types::{ExecutionOrderId, PositionId, RecommendationId},
};
use quant_pivot_repository::traits::{
    AttributionRepository, ExecutionOrderRepository, PositionRepository,
};

pub struct CoreExecutionReadPort {
    execution_orders: Arc<dyn ExecutionOrderRepository>,
    positions: Arc<dyn PositionRepository>,
    attribution: Arc<dyn AttributionRepository>,
}

impl CoreExecutionReadPort {
    #[must_use]
    pub const fn new(
        execution_orders: Arc<dyn ExecutionOrderRepository>,
        positions: Arc<dyn PositionRepository>,
        attribution: Arc<dyn AttributionRepository>,
    ) -> Self {
        Self {
            execution_orders,
            positions,
            attribution,
        }
    }
}

#[async_trait]
impl ExecutionReadPort for CoreExecutionReadPort {
    async fn list_execution_orders(
        &self,
        query: ExecutionOrderListQuery,
    ) -> QuantResult<Paginated<ExecutionOrderInfo>> {
        self.execution_orders.page(query).await.map_err(Into::into)
    }

    async fn get_execution_order(
        &self,
        id: &ExecutionOrderId,
    ) -> QuantResult<Option<ExecutionOrderInfo>> {
        self.execution_orders
            .find_by_id(id)
            .await
            .map_err(Into::into)
    }

    async fn list_positions(
        &self,
        query: PositionListQuery,
    ) -> QuantResult<Paginated<PositionInfo>> {
        self.positions.page(query).await.map_err(Into::into)
    }

    async fn get_position(&self, id: &PositionId) -> QuantResult<Option<PositionInfo>> {
        self.positions.find_by_id(id).await.map_err(Into::into)
    }

    async fn get_recommendation_attribution(
        &self,
        id: &RecommendationId,
    ) -> QuantResult<Option<RecommendationAttributionInfo>> {
        self.attribution
            .find_by_recommendation(id)
            .await
            .map_err(Into::into)
    }
}
