//! Execution-intent persistence DTOs.

use crate::{
    domain::{
        RecommendationInfo, RecommendationReportInfo,
        patch::{NullablePatch, Patch},
    },
    enums::{
        common::Side,
        execution::{
            ApprovalInvalidation, ExecutionOrderPhase, OrderIntentKind, OrderTypeKind,
            VenueOrderStatus,
        },
        quant::{
            ApprovalStatus, ExecutionOrderState, OrderIntentStatus, QuantRuntimeMode,
            RecommendationReportStatus,
        },
    },
    types::{
        ContentHash, EntryOrderSpec, ExecutionOrderId, ExitPolicySpec, MarketId, ModelVersionId,
        OrderId, OrderIntentId, Price, RecommendationId, RuntimeConfigVersionId, Shares, TokenId,
        Usd,
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
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub model_version_id: ModelVersionId,
    pub intent_kind: OrderIntentKind,
    pub status: OrderIntentStatus,
    pub approval_status: ApprovalStatus,
    pub approved_by: Option<Uuid>,
    pub approval_reason: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
    pub policy_id: Option<String>,
    pub policy_hash: Option<ContentHash>,
    pub status_reason: Option<String>,
    pub admission_trace_ref: Option<String>,
    pub entry_order_json: EntryOrderSpec,
    pub exit_policy_json: ExitPolicySpec,
    pub risk_envelope_hash: ContentHash,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(OrderIntentInfo, crate::entities::quant_order_intent::Model, {
    order_intent_id, recommendation_id, runtime_mode, runtime_config_version_id,
    model_version_id, intent_kind, status, approval_status, approved_by,
    approval_reason, approved_at, policy_id, policy_hash, status_reason,
    admission_trace_ref, entry_order_json, exit_policy_json, risk_envelope_hash,
    expires_at, created_at, updated_at,
});

/// Insert payload for `quant_order_intent`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_order_intent::ActiveModel")]
pub struct NewOrderIntent {
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
    pub runtime_mode: QuantRuntimeMode,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub model_version_id: ModelVersionId,
    pub intent_kind: OrderIntentKind,
    pub status: OrderIntentStatus,
    pub approval_status: ApprovalStatus,
    pub approved_by: Option<Uuid>,
    pub approval_reason: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
    pub policy_id: Option<String>,
    pub policy_hash: Option<ContentHash>,
    pub status_reason: Option<String>,
    pub admission_trace_ref: Option<String>,
    pub entry_order_json: EntryOrderSpec,
    pub exit_policy_json: ExitPolicySpec,
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
    pub order_phase: ExecutionOrderPhase,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub order_type: OrderTypeKind,
    pub price: Price,
    pub shares: Shares,
    pub cost_usd: Usd,
    pub venue_order_id: Option<OrderId>,
    pub venue_status: Option<VenueOrderStatus>,
    pub state: ExecutionOrderState,
    pub submitted_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub gtd_expiration_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(ExecutionOrderInfo, crate::entities::quant_execution_order::Model, {
    execution_order_id, order_intent_id, order_phase, market_id, token_id, side,
    order_type, price, shares, cost_usd, venue_order_id, venue_status, state,
    submitted_at, filled_at, cancelled_at, gtd_expiration_at, error_message, created_at, updated_at,
});

/// Insert payload for `quant_execution_order`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_execution_order::ActiveModel")]
pub struct NewExecutionOrder {
    pub execution_order_id: ExecutionOrderId,
    pub order_intent_id: OrderIntentId,
    pub order_phase: ExecutionOrderPhase,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub order_type: OrderTypeKind,
    pub price: Price,
    pub shares: Shares,
    pub cost_usd: Usd,
    pub venue_order_id: Option<OrderId>,
    pub venue_status: Option<VenueOrderStatus>,
    pub state: ExecutionOrderState,
    pub submitted_at: Option<DateTime<Utc>>,
    pub filled_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub gtd_expiration_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
}

/// Controlled partial update for an execution-order lifecycle row.
#[derive(Debug, Clone, Default)]
pub struct ExecutionOrderPatch {
    pub state: Patch<ExecutionOrderState>,
    pub venue_order_id: NullablePatch<OrderId>,
    pub venue_status: NullablePatch<VenueOrderStatus>,
    pub submitted_at: NullablePatch<DateTime<Utc>>,
    pub filled_at: NullablePatch<DateTime<Utc>>,
    pub cancelled_at: NullablePatch<DateTime<Utc>>,
    pub gtd_expiration_at: NullablePatch<DateTime<Utc>>,
    pub error_message: NullablePatch<String>,
}

/// Result of an approval attempt after the in-transaction invalidation re-check.
#[derive(Debug, Clone)]
pub enum ApproveOrderIntentOutcome {
    /// Intent transitioned to `Approved` (capital unchanged or downscaled).
    Approved(OrderIntentInfo),
    /// A governed fact changed inside the approval transaction; intent invalidated
    /// and capital released (HTTP origin — no operation-log row in this txn).
    Invalidated(OrderIntentInfo, ApprovalInvalidation),
}

/// Cheap, deterministic approval-time invalidation re-check (Phase 05.2 set).
///
/// Model retirement, data-quality thresholds, and envelope-hash recomputation
/// are the admission engine's responsibility (Phase 05.3).
#[must_use]
pub fn evaluate_intent_approval_invalidation(
    rec: &RecommendationInfo,
    report: &RecommendationReportInfo,
    kill_switch_allows_entry: bool,
    active_config_version_id: &RuntimeConfigVersionId,
    intent_config_version_id: &RuntimeConfigVersionId,
    intent_risk_envelope_hash: &ContentHash,
    now: DateTime<Utc>,
) -> Option<ApprovalInvalidation> {
    if rec.valid_until < now {
        return Some(ApprovalInvalidation::RecommendationExpired);
    }
    if report.status != RecommendationReportStatus::Published {
        return Some(ApprovalInvalidation::ReportRevoked);
    }
    if !kill_switch_allows_entry {
        return Some(ApprovalInvalidation::KillSwitchOpened);
    }
    if intent_config_version_id != active_config_version_id {
        return Some(ApprovalInvalidation::RuntimeConfigChanged);
    }
    if *intent_risk_envelope_hash != rec.risk_envelope.envelope_hash {
        return Some(ApprovalInvalidation::RiskEnvelopeMismatch);
    }
    None
}

/// Venue submission outcome applied to an execution-order row (Phase 05.4 write path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionOrderSubmissionResult {
    pub state: ExecutionOrderState,
    pub venue_order_id: Option<OrderId>,
    pub venue_status: Option<VenueOrderStatus>,
    pub submitted_at: DateTime<Utc>,
    pub filled_at: Option<DateTime<Utc>>,
    pub cancelled_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
}
