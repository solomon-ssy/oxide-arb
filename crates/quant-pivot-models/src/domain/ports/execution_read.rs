//! Web-facing read port for execution orders, position lots, and attribution.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        api::{
            ExecutionOrderListQuery, PositionListQuery, PositionSummary, ReconciliationListQuery,
            settlement_redeem::{
                SettlementRedeemDetail, SettlementRedeemListQuery, SettlementRedeemSummary,
            },
        },
        pagination::Paginated,
        quant::{
            ExecutionOrderInfo, PositionInfo, RecommendationAttributionInfo, ReconciliationInfo,
        },
    },
    types::{
        ExecutionOrderId, OrderIntentId, PositionId, RecommendationId, ReconciliationId,
        SettlementRedeemId,
    },
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
    ) -> QuantResult<Paginated<PositionSummary>>;

    async fn get_position(&self, id: &PositionId) -> QuantResult<Option<PositionSummary>>;

    async fn get_position_by_intent(
        &self,
        intent_id: &OrderIntentId,
    ) -> QuantResult<Option<PositionInfo>>;

    async fn get_recommendation_attribution(
        &self,
        id: &RecommendationId,
    ) -> QuantResult<Option<RecommendationAttributionInfo>>;

    async fn list_reconciliations(
        &self,
        query: ReconciliationListQuery,
    ) -> QuantResult<Paginated<ReconciliationInfo>>;

    async fn get_reconciliation(
        &self,
        id: &ReconciliationId,
    ) -> QuantResult<Option<ReconciliationInfo>>;

    async fn list_settlement_redeems(
        &self,
        query: SettlementRedeemListQuery,
    ) -> QuantResult<Paginated<SettlementRedeemSummary>>;

    async fn get_settlement_redeem(
        &self,
        id: &SettlementRedeemId,
    ) -> QuantResult<Option<SettlementRedeemDetail>>;
}
