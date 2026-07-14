//! Execution-intent persistence DTOs.

use crate::{
    domain::{
        NewReconciliation, PositionExit, PositionFill, RecommendationInfo,
        RecommendationReportInfo,
        patch::{NullablePatch, Patch},
    },
    enums::{
        common::Side,
        execution::{
            ApprovalInvalidation, ExecutionOrderPhase, ExitReason, ExitState, OrderIntentKind,
            OrderTypeKind, VenueOrderStatus,
        },
        quant::{
            ApprovalStatus, ExecutionOrderState, OrderIntentStatus, QuantRuntimeMode,
            RecommendationReportStatus,
        },
    },
    types::{
        ContentHash, EntryConditionInstanceId, EntryOrderSpec, ExecutionOrderId, ExitPolicySpec,
        ExitReinferenceObservation, MarketId, ModelVersionId, OrderId, OrderIntentId, Price,
        RecommendationId, RecommendationReportId, RuntimeConfigVersionId, ScaleOutState, Shares,
        TokenId, Usd,
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
    pub condition_instance_id: EntryConditionInstanceId,
    pub entry_order_json: EntryOrderSpec,
    pub exit_policy_json: ExitPolicySpec,
    pub risk_envelope_hash: ContentHash,
    pub expires_at: DateTime<Utc>,
    pub exit_state: ExitState,
    pub exit_reason: Option<ExitReason>,
    pub next_check_at: Option<DateTime<Utc>>,
    pub peak_mark_price: Option<Price>,
    pub last_signal_recheck_at: Option<DateTime<Utc>>,
    pub latest_reinference_json: Option<ExitReinferenceObservation>,
    pub scale_out_state: ScaleOutState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(OrderIntentInfo, crate::entities::quant_order_intent::Model, {
    order_intent_id, recommendation_id, runtime_mode, runtime_config_version_id,
    model_version_id, intent_kind, status, approval_status, approved_by,
    approval_reason, approved_at, policy_id, policy_hash, status_reason,
    admission_trace_ref, condition_instance_id, entry_order_json, exit_policy_json, risk_envelope_hash,
    expires_at, exit_state, exit_reason, next_check_at, peak_mark_price,
    last_signal_recheck_at, latest_reinference_json, scale_out_state, created_at, updated_at,
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
    pub condition_instance_id: EntryConditionInstanceId,
    pub entry_order_json: EntryOrderSpec,
    pub exit_policy_json: ExitPolicySpec,
    pub risk_envelope_hash: ContentHash,
    pub expires_at: DateTime<Utc>,
}

/// Transactional limits attached to one policy-bound `SemiAuto` canary intent.
///
/// The repository evaluates these limits under the same lock and transaction
/// that reserve capital, so concurrent requests cannot both pass a stale
/// process-local count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentCreationLimits {
    pub recommendation_report_id: RecommendationReportId,
    pub max_open_intents: u32,
    pub max_total_usd_per_report: Usd,
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
    let Some((_, _, _, _, risk_envelope)) = rec.trade_plan.frozen() else {
        return Some(ApprovalInvalidation::RiskEnvelopeMismatch);
    };
    if *intent_risk_envelope_hash != risk_envelope.envelope_hash {
        return Some(ApprovalInvalidation::RiskEnvelopeMismatch);
    }
    None
}

/// How a submission outcome settles the intent's capital allocation (Phase 05.4).
///
/// The capital row carries explicit amounts (`locked`/`spent`/`released`); the
/// state enum is a coarse lifecycle marker. The reserved aggregate is
/// `max(allocated, locked) - spent - released`, so a partial fill can keep the
/// row `Locked` while increasing `spent` and still report the correct remaining
/// exposure.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CapitalSettlement {
    /// Full settle: `Locked -> Spent`. `spent_usd` is the realized fill cost; any
    /// unspent locked remainder (filled below the limit) is released.
    SettleFull { spent_usd: Usd },
    /// Partial fill with a resting remainder: increase `spent_usd`, keep the row
    /// `Locked` (remaining exposure stays reserved until recon / exit).
    SettlePartial { spent_usd: Usd },
    /// Nothing filled: release all locked capital (`Locked -> Released`).
    ReleaseAll,
    /// Leave locked capital untouched — `Ambiguous` (may have filled) or a
    /// resting `Open` order with no fill yet. Fail-closed: never free capital
    /// that might already be spent on the venue.
    Hold,
}

/// Complete ledger write applied by `record_submission_result` in one txn.
///
/// Built by the dispatcher from the venue [`VenueSubmitResult`] and applied
/// atomically across execution order + capital + position + intent + recon.
#[derive(Debug, Clone)]
pub struct SubmissionLedgerWrite {
    /// Target execution-order state (from the venue outcome).
    pub state: ExecutionOrderState,
    /// Target order-intent status (kept consistent with `state`).
    pub intent_status: OrderIntentStatus,
    /// Venue-assigned order id, when the venue acknowledged one.
    pub venue_order_id: Option<OrderId>,
    /// Raw venue order status, when parseable.
    pub venue_status: Option<VenueOrderStatus>,
    /// When the order was sent to the venue.
    pub submitted_at: DateTime<Utc>,
    /// When the order (partially) filled, when known.
    pub filled_at: Option<DateTime<Utc>>,
    /// When the order was cancelled/expired, when known.
    pub cancelled_at: Option<DateTime<Utc>>,
    /// Failure / ambiguity detail recorded on the order row.
    pub error_message: Option<String>,
    /// How to settle the capital allocation.
    pub capital: CapitalSettlement,
    /// Position upsert (present only on a fill).
    pub fill: Option<PositionFill>,
    /// Reconciliation row to enqueue (`None` only for a resting `Open` order).
    pub reconciliation: Option<NewReconciliation>,
}

/// Complete ledger write applied by `record_exit_result` in one txn (Phase 05.6).
///
/// Built by the exit dispatcher from the venue [`VenueSubmitResult`] of a Sell
/// exit order and applied atomically across the exit execution order, the
/// per-intent position lot, the capital allocation (`Spent -> Released` on full
/// exit), the intent's exit FSM, and reconciliation.
#[derive(Debug, Clone)]
pub struct ExitLedgerWrite {
    /// Target exit-order state (from the venue outcome).
    pub order_state: ExecutionOrderState,
    /// Venue-assigned order id, when acknowledged.
    pub venue_order_id: Option<OrderId>,
    /// Raw venue order status, when parseable.
    pub venue_status: Option<VenueOrderStatus>,
    /// When the exit (partially) filled, when known.
    pub filled_at: Option<DateTime<Utc>>,
    /// When the exit order was cancelled/expired, when known.
    pub cancelled_at: Option<DateTime<Utc>>,
    /// Failure / ambiguity detail recorded on the order row.
    pub error_message: Option<String>,
    /// Exit-FSM state to set on the intent.
    pub exit_state: ExitState,
    /// Why the exit fired (recorded on the intent).
    pub exit_reason: ExitReason,
    /// Position-lot exit fill (present only on a (partial) fill).
    pub position_exit: Option<PositionExit>,
    /// Whether the lot is now fully exited (capital `Spent -> Released`). A
    /// partial keeps the capital `Spent` and the lot `Closing`.
    pub fully_exited: bool,
    /// Revert the lot `Closing -> Open` (a failed/cancelled exit attempt that
    /// must be re-monitored).
    pub revert_to_open: bool,
    /// Reconciliation row to enqueue (`None` for a resting `Open` exit order).
    pub reconciliation: Option<NewReconciliation>,
}
