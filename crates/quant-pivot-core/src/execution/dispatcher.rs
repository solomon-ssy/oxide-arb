//! Execution dispatcher contract.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{ExecutionOrderInfo, OrderIntentInfo},
    enums::quant::ExecutionOrderState,
    types::OrderIntentId,
};

/// Dispatch request for a previously admitted order intent.
#[derive(Debug, Clone)]
pub struct DispatchOrderIntentRequest {
    pub order_intent_id: OrderIntentId,
    pub dispatched_at: DateTime<Utc>,
}

/// Dispatch result after local order rows and venue state have been reconciled.
#[derive(Debug, Clone)]
pub struct DispatchOrderIntentResult {
    pub order_intent: OrderIntentInfo,
    pub execution_orders: Vec<ExecutionOrderInfo>,
    pub terminal_state: Option<ExecutionOrderState>,
}

/// Dispatch boundary for entry/exit execution.
#[async_trait]
pub trait ExecutionDispatcher: Send + Sync {
    async fn dispatch(
        &self,
        request: DispatchOrderIntentRequest,
    ) -> QuantResult<DispatchOrderIntentResult>;
}
