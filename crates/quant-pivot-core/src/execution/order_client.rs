//! Polymarket order-client contract.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::ExecutionOrderInfo,
    enums::execution::VenueOrderStatus,
    types::{OrderId, OrderIntentId},
};

/// Venue acknowledgement after submitting an execution order.
#[derive(Debug, Clone)]
pub struct VenueOrderAck {
    pub order_intent_id: OrderIntentId,
    pub venue_order_id: OrderId,
    pub status: VenueOrderStatus,
    pub received_at: DateTime<Utc>,
}

/// Venue acknowledgement after cancelling an order.
#[derive(Debug, Clone)]
pub struct VenueCancelAck {
    pub venue_order_id: OrderId,
    pub status: VenueOrderStatus,
    pub received_at: DateTime<Utc>,
}

/// Adapter boundary for Polymarket CLOB order writes.
#[async_trait]
pub trait PolymarketOrderClient: Send + Sync {
    async fn submit_order(&self, order: ExecutionOrderInfo) -> QuantResult<VenueOrderAck>;

    async fn cancel_order(&self, venue_order_id: OrderId) -> QuantResult<VenueCancelAck>;
}
