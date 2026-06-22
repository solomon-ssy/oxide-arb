//! Quant execution intent HTTP contract types.

use crate::domain::OrderIntentInfo;
use serde::Serialize;

/// Outbound projection for an order intent row.
#[derive(Debug, Clone, Serialize)]
pub struct QuantOrderIntentView {
    pub order_intent_id: String,
    pub recommendation_id: String,
    pub status: String,
    pub approval_status: String,
}

impl From<OrderIntentInfo> for QuantOrderIntentView {
    fn from(info: OrderIntentInfo) -> Self {
        Self {
            order_intent_id: info.order_intent_id.to_string(),
            recommendation_id: info.recommendation_id.to_string(),
            status: info.status.as_str().to_owned(),
            approval_status: info.approval_status.as_str().to_owned(),
        }
    }
}

/// Approve-order-intent request body for semi-auto execution.
#[derive(Debug, Clone, serde::Deserialize, validator::Validate)]
pub struct ApproveQuantOrderIntentRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}
