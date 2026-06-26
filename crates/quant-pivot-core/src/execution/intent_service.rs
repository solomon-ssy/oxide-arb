//! Order-intent service contract.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{ApproveOrderIntent, OrderIntentInfo},
    enums::execution::ApprovalInvalidation,
    types::{OrderIntentId, RecommendationId},
};
use uuid::Uuid;

/// Request to create an order intent from a recommendation.
#[derive(Debug, Clone)]
pub struct CreateOrderIntentRequest {
    pub recommendation_id: RecommendationId,
    pub requested_by: Option<Uuid>,
    pub requested_at: DateTime<Utc>,
    pub reason: String,
}

/// Request to cancel an order intent before venue submission.
#[derive(Debug, Clone)]
pub struct CancelOrderIntentRequest {
    pub order_intent_id: OrderIntentId,
    pub cancelled_by: Uuid,
    pub cancelled_at: DateTime<Utc>,
    pub reason: String,
}

/// Request to invalidate an order intent because a governed fact changed.
#[derive(Debug, Clone)]
pub struct InvalidateOrderIntentRequest {
    pub order_intent_id: OrderIntentId,
    pub reason: ApprovalInvalidation,
    pub invalidated_at: DateTime<Utc>,
}

/// Governed order-intent service boundary.
#[async_trait]
pub trait OrderIntentService: Send + Sync {
    async fn create_intent(
        &self,
        request: CreateOrderIntentRequest,
    ) -> QuantResult<OrderIntentInfo>;

    async fn approve_intent(
        &self,
        order_intent_id: OrderIntentId,
        approval: ApproveOrderIntent,
    ) -> QuantResult<OrderIntentInfo>;

    async fn cancel_intent(
        &self,
        request: CancelOrderIntentRequest,
    ) -> QuantResult<OrderIntentInfo>;

    async fn invalidate_intent(
        &self,
        request: InvalidateOrderIntentRequest,
    ) -> QuantResult<OrderIntentInfo>;
}
