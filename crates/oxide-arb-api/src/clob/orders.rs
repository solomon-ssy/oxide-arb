//! Order submission, cancellation, and query types.

use oxide_arb_models::enums::common::Side;
use oxide_arb_models::types::{OrderId, Price, Shares, TokenId};
use serde::{Deserialize, Serialize};

/// Result of cancelling a single order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelResult {
    pub order_id: OrderId,
    pub success: bool,
    pub reason: Option<String>,
}

/// Result of cancelling all orders.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelAllResult {
    pub canceled: Vec<OrderId>,
    pub not_canceled: Vec<(OrderId, String)>,
}

/// An open order resting on the book.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub order_id: OrderId,
    pub token_id: TokenId,
    pub side: Side,
    pub price: Price,
    pub size: Shares,
    pub filled: Shares,
}
