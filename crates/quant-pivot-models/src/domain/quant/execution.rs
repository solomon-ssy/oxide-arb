//! Execution-intent persistence DTOs.

use crate::{
    enums::quant::{
        ApprovalStatus, ExecutionOrderState, OrderIntentStatus, QuantRuntimeMode, SignalSide,
    },
    types::{
        ContentHash, ExecutionOrderId, MarketId, OrderId, OrderIntentId, Price, RecommendationId,
        Shares, TokenId, Usd,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Governed bridge from a recommendation to execution.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_order_intent::Entity")]
pub struct OrderIntentInfo {
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
    pub runtime_mode: QuantRuntimeMode,
    pub intent_kind: String,
    pub status: OrderIntentStatus,
    pub approval_status: ApprovalStatus,
    pub approved_by: Option<Uuid>,
    pub approval_reason: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
    pub entry_order_json: serde_json::Value,
    pub exit_policy_json: serde_json::Value,
    pub risk_envelope_hash: ContentHash,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(OrderIntentInfo, crate::entities::quant_order_intent::Model, {
    order_intent_id, recommendation_id, runtime_mode, intent_kind, status,
    approval_status, approved_by, approval_reason, approved_at, entry_order_json,
    exit_policy_json, risk_envelope_hash, expires_at, created_at, updated_at,
});

/// Insert payload for `quant_order_intent`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_order_intent::ActiveModel")]
pub struct NewOrderIntent {
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
    pub runtime_mode: QuantRuntimeMode,
    pub intent_kind: String,
    pub status: OrderIntentStatus,
    pub approval_status: ApprovalStatus,
    pub approved_by: Option<Uuid>,
    pub approval_reason: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
    pub entry_order_json: serde_json::Value,
    pub exit_policy_json: serde_json::Value,
    pub risk_envelope_hash: ContentHash,
    pub expires_at: DateTime<Utc>,
}

/// Approval transition payload for an order intent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApproveOrderIntent {
    pub approved_by: Uuid,
    pub approval_reason: String,
    pub approved_at: DateTime<Utc>,
}

/// Internal execution-order lifecycle row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_execution_order::Entity")]
pub struct ExecutionOrderInfo {
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    pub order_phase: String,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: SignalSide,
    pub order_type: String,
    pub price: Price,
    pub shares: Shares,
    pub cost_usd: Usd,
    pub venue_order_id: Option<OrderId>,
    pub venue_status: Option<String>,
    pub state: ExecutionOrderState,
    pub submitted_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(ExecutionOrderInfo, crate::entities::quant_execution_order::Model, {
    execution_order_id, order_intent_id, order_phase, market_id, token_id, side,
    order_type, price, shares, cost_usd, venue_order_id, venue_status, state,
    submitted_at, filled_at, cancelled_at, error_message, created_at, updated_at,
});

/// Insert payload for `quant_execution_order`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_execution_order::ActiveModel")]
pub struct NewExecutionOrder {
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    pub order_phase: String,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: SignalSide,
    pub order_type: String,
    pub price: Price,
    pub shares: Shares,
    pub cost_usd: Usd,
    pub venue_order_id: Option<OrderId>,
    pub venue_status: Option<String>,
    pub state: ExecutionOrderState,
    pub submitted_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
}

/// Runtime execution plan emitted from an approved intent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionOrderModel {
    pub order: NewExecutionOrder,
}
