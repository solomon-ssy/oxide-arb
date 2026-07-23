//! Core implementation of [`ExecutionReadPort`] for the Admin API.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        api::{
            ExecutionOrderListQuery, PositionListQuery, PositionSummary, ReconciliationListQuery,
            settlement_redeem::{
                SettlementRedeemDetail, SettlementRedeemListQuery, SettlementRedeemSummary,
            },
        },
        pagination::Paginated,
        ports::ExecutionReadPort,
        quant::{
            ExecutionOrderInfo, PositionInfo, RecommendationAttributionInfo, ReconciliationInfo,
        },
    },
    types::{
        ExecutionOrderId, OrderIntentId, PositionId, RecommendationId, ReconciliationId,
        SettlementRedeemId,
    },
};
use quant_pivot_repository::traits::{
    AttributionRepository, ExecutionOrderRepository, PositionRepository, ReconciliationRepository,
    quant::settlement_redeem::SettlementRedeemRepository,
};

pub struct CoreExecutionReadPort {
    execution_orders: Arc<dyn ExecutionOrderRepository>,
    positions: Arc<dyn PositionRepository>,
    attribution: Arc<dyn AttributionRepository>,
    reconciliation: Arc<dyn ReconciliationRepository>,
    settlement_redeem: Arc<dyn SettlementRedeemRepository>,
}

impl CoreExecutionReadPort {
    #[must_use]
    pub fn new(
        execution_orders: Arc<dyn ExecutionOrderRepository>,
        positions: Arc<dyn PositionRepository>,
        attribution: Arc<dyn AttributionRepository>,
        reconciliation: Arc<dyn ReconciliationRepository>,
        settlement_redeem: Arc<dyn SettlementRedeemRepository>,
    ) -> Self {
        Self {
            execution_orders,
            positions,
            attribution,
            reconciliation,
            settlement_redeem,
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
    ) -> QuantResult<Paginated<PositionSummary>> {
        self.positions.page(query).await.map_err(Into::into)
    }

    async fn get_position(&self, id: &PositionId) -> QuantResult<Option<PositionSummary>> {
        self.positions.find_by_id(id).await.map_err(Into::into)
    }

    async fn get_position_by_intent(
        &self,
        intent_id: &OrderIntentId,
    ) -> QuantResult<Option<PositionInfo>> {
        self.positions
            .find_by_intent(intent_id)
            .await
            .map_err(Into::into)
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

    async fn list_reconciliations(
        &self,
        query: ReconciliationListQuery,
    ) -> QuantResult<Paginated<ReconciliationInfo>> {
        self.reconciliation.page(query).await.map_err(Into::into)
    }

    async fn get_reconciliation(
        &self,
        id: &ReconciliationId,
    ) -> QuantResult<Option<ReconciliationInfo>> {
        self.reconciliation.find_by_id(id).await.map_err(Into::into)
    }

    async fn list_settlement_redeems(
        &self,
        query: SettlementRedeemListQuery,
    ) -> QuantResult<Paginated<SettlementRedeemSummary>> {
        self.settlement_redeem.page(query).await.map_err(Into::into)
    }

    async fn get_settlement_redeem(
        &self,
        id: &SettlementRedeemId,
    ) -> QuantResult<Option<SettlementRedeemDetail>> {
        let Some(redeem) = self.settlement_redeem.find_by_id(id).await? else {
            return Ok(None);
        };
        let inventory_lots = self.settlement_redeem.list_current_inventory(id).await?;
        let redeemed_lots = self.settlement_redeem.list_lots_by_redeem(id).await?;
        let submissions = self
            .settlement_redeem
            .list_submissions_by_redeem(id)
            .await?;
        Ok(Some(SettlementRedeemDetail {
            redeem,
            inventory_lots,
            redeemed_lots,
            submissions,
        }))
    }
}
