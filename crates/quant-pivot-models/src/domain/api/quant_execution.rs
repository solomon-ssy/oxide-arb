//! Order-intent execution HTTP contract types.
//!
//! Three families per the DTO paradigm: outbound `OrderIntentView` (`Serialize`
//! only, built from the persistence `OrderIntentInfo`), the inbound paginated
//! `OrderIntentListQuery`, and the governed mutation requests
//! (`CreateIntentRequest` / `ApproveIntentRequest` / `RejectIntentRequest` /
//! `CancelIntentRequest`, all `Deserialize` + `Validate`). The persistence
//! struct is never serialized directly.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use validator::Validate;

use crate::{
    domain::{
        pagination::PageRequest,
        quant::{
            EntryConditionArtifactInfo, EntryConditionAuditInfo, EntryConditionInstanceInfo,
            ExecutionOrderInfo, OrderIntentInfo, PositionInfo, RecommendationAttributionInfo,
        },
    },
    enums::{
        common::Side,
        execution::{
            ExecutionOrderPhase, ExitReason, ExitState, OrderIntentKind, OrderTypeKind,
            PositionLedgerState, VenueOrderStatus,
        },
        quant::{
            ApprovalStatus, EntryConditionAuditAction, EntryConditionState, ExecutionOrderState,
            OrderIntentStatus, QuantRuntimeMode, RecommendationAttributionOutcome,
        },
    },
    types::{
        AttributionDetail, ConditionLeafEvidence, ConditionNodeEvaluation, ConditionTruth,
        ConditionUnavailableReason, ContentHash, DecisionPolicySnapshotId,
        EntryConditionArtifactId, EntryConditionArtifactV1, EntryConditionAuditId,
        EntryConditionInstanceId, EntryConditionNode, EntryOrderSpec, EntryOutcome,
        ExecutionOrderId, ExitOutcome, ExitPolicySpec, ExitReinferenceObservation, MarketId,
        ModelVersionId, NextScaleOutProjection, OrderAmount, OrderId, OrderIntentId, PositionId,
        Price, RecommendationId, ScaleOutState, Shares, TokenId, Usd, UserId,
    },
};

/// Outbound projection of a governed order intent (full operator transparency).
#[derive(Debug, Clone, Serialize)]
pub struct OrderIntentView {
    pub order_intent_id: OrderIntentId,
    pub recommendation_id: RecommendationId,
    pub runtime_mode: QuantRuntimeMode,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_version_id: ModelVersionId,
    pub intent_kind: OrderIntentKind,
    pub status: OrderIntentStatus,
    pub approval_status: ApprovalStatus,
    pub approved_by: Option<UserId>,
    pub approval_reason: Option<String>,
    pub approved_at: Option<DateTime<Utc>>,
    pub policy_id: Option<String>,
    pub policy_hash: Option<ContentHash>,
    pub status_reason: Option<String>,
    pub admission_trace_ref: Option<String>,
    pub condition_instance_id: EntryConditionInstanceId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entry_condition: Option<EntryConditionInstanceSummaryView>,
    pub entry_order: EntryOrderSpec,
    pub exit_policy: ExitPolicySpec,
    pub risk_envelope_hash: ContentHash,
    pub expires_at: DateTime<Utc>,
    pub exit_state: ExitState,
    pub exit_reason: Option<ExitReason>,
    pub next_check_at: Option<DateTime<Utc>>,
    pub peak_mark_price: Option<Price>,
    pub last_signal_recheck_at: Option<DateTime<Utc>>,
    pub latest_reinference: Option<ExitReinferenceObservation>,
    /// Read-time projection for a filled lot; absent before the entry fills.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_monitor_observation: Option<ExitMonitorObservationView>,
    pub scale_out_state: ScaleOutState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<OrderIntentInfo> for OrderIntentView {
    fn from(info: OrderIntentInfo) -> Self {
        Self {
            order_intent_id: info.order_intent_id,
            recommendation_id: info.recommendation_id,
            runtime_mode: info.runtime_mode,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            model_version_id: info.model_version_id,
            intent_kind: info.intent_kind,
            status: info.status,
            approval_status: info.approval_status,
            approved_by: info.approved_by,
            approval_reason: info.approval_reason,
            approved_at: info.approved_at,
            policy_id: info.policy_id,
            policy_hash: info.policy_hash,
            status_reason: info.status_reason,
            admission_trace_ref: info.admission_trace_ref,
            condition_instance_id: info.condition_instance_id,
            entry_condition: None,
            entry_order: info.entry_order_json,
            exit_policy: info.exit_policy_json,
            risk_envelope_hash: info.risk_envelope_hash,
            expires_at: info.expires_at,
            exit_state: info.exit_state,
            exit_reason: info.exit_reason,
            next_check_at: info.next_check_at,
            peak_mark_price: info.peak_mark_price,
            last_signal_recheck_at: info.last_signal_recheck_at,
            latest_reinference: info.latest_reinference_json,
            exit_monitor_observation: None,
            scale_out_state: info.scale_out_state,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// Shared summary embedded by recommendation and intent detail views.
#[derive(Debug, Clone, Serialize)]
pub struct EntryConditionInstanceSummaryView {
    pub condition_instance_id: EntryConditionInstanceId,
    pub artifact_id: Option<EntryConditionArtifactId>,
    pub artifact_hash: Option<ContentHash>,
    pub state: EntryConditionState,
    pub truth: Option<ConditionTruth>,
    pub revision: i64,
    pub evaluation_hash: Option<ContentHash>,
    pub input_fingerprint: Option<ContentHash>,
    pub continuity_hash: Option<ContentHash>,
    pub confirmation_started_at: Option<DateTime<Utc>>,
    pub last_evaluated_at: Option<DateTime<Utc>>,
    pub next_evaluation_at: Option<DateTime<Utc>>,
    pub expires_at: DateTime<Utc>,
    pub lease_epoch: i64,
    pub claimed_by_intent_id: Option<OrderIntentId>,
    pub claim_admission_state_version: Option<ContentHash>,
    pub consumed_at: Option<DateTime<Utc>>,
}

impl From<EntryConditionInstanceInfo> for EntryConditionInstanceSummaryView {
    fn from(info: EntryConditionInstanceInfo) -> Self {
        Self {
            condition_instance_id: info.condition_instance_id,
            artifact_id: info.artifact_id,
            artifact_hash: info.artifact_hash,
            state: info.state,
            truth: info.truth_json,
            revision: info.revision,
            evaluation_hash: info.evaluation_hash,
            input_fingerprint: info.input_fingerprint,
            continuity_hash: info.continuity_hash,
            confirmation_started_at: info.confirmation_started_at,
            last_evaluated_at: info.last_evaluated_at,
            next_evaluation_at: info.next_evaluation_at,
            expires_at: info.expires_at,
            lease_epoch: info.lease_epoch,
            claimed_by_intent_id: info.claimed_by_intent_id,
            claim_admission_state_version: info.claim_admission_state_version,
            consumed_at: info.consumed_at,
        }
    }
}

/// Full condition artifact plus stable server-generated node identities.
#[derive(Debug, Clone, Serialize)]
pub struct EntryConditionArtifactView {
    pub artifact_id: EntryConditionArtifactId,
    pub content_hash: ContentHash,
    pub schema_version: i32,
    pub evaluator_version: i32,
    pub artifact: EntryConditionArtifactV1,
    pub nodes: Vec<EntryConditionNode>,
}

impl EntryConditionArtifactView {
    #[must_use]
    pub fn from_info(info: EntryConditionArtifactInfo, nodes: Vec<EntryConditionNode>) -> Self {
        Self {
            artifact_id: info.artifact_id,
            content_hash: info.content_hash,
            schema_version: info.schema_version,
            evaluator_version: info.evaluator_version,
            artifact: info.payload_json,
            nodes,
        }
    }
}

/// Recommendation-owned condition state with its immutable evaluator contract.
#[derive(Debug, Clone, Serialize)]
pub struct EntryConditionDetailView {
    pub instance: EntryConditionInstanceSummaryView,
    pub artifact: Option<EntryConditionArtifactView>,
    pub latest_authoritative_evaluation: Option<EntryConditionEvaluationView>,
}

/// Latest PostgreSQL-applied evaluator trace and its operator-facing evidence.
#[derive(Debug, Clone, Serialize)]
pub struct EntryConditionEvaluationView {
    pub evaluation_id: ContentHash,
    pub applied_revision: i64,
    pub evaluator_version: u32,
    pub evaluated_at: DateTime<Utc>,
    pub state: EntryConditionState,
    pub truth: ConditionTruth,
    pub evaluation_hash: ContentHash,
    pub input_fingerprint: ContentHash,
    pub continuity_hash: ContentHash,
    pub tree: ConditionNodeEvaluation,
    pub leaf_evidence: Vec<EntryConditionLeafEvidenceView>,
}

/// One evaluated leaf with explicit source/freshness/checkpoint projections.
#[derive(Debug, Clone, Serialize)]
pub struct EntryConditionLeafEvidenceView {
    pub node_id: u16,
    pub truth: ConditionTruth,
    pub evidence: ConditionLeafEvidence,
    pub observed_at: Option<DateTime<Utc>>,
    pub available_at: Option<DateTime<Utc>>,
    pub freshness_ms: Option<i64>,
    pub source_checkpoint: Option<EntryConditionSourceCheckpointView>,
    pub unavailable_reason: Option<ConditionUnavailableReason>,
}

/// Typed compact checkpoint projected from one closed leaf-evidence variant.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum EntryConditionSourceCheckpointView {
    Price {
        token_id: TokenId,
        observed_at: DateTime<Utc>,
        available_at: DateTime<Utc>,
        gap_generation: u64,
    },
    Factor {
        definition_hash: ContentHash,
        model_version_id: ModelVersionId,
        snapshot_hash: ContentHash,
    },
    Weather {
        revision: u64,
        report_hash: ContentHash,
        gap_generation: u64,
    },
    Crypto {
        source_sequence: Option<u64>,
        report_hash: Option<ContentHash>,
        gap_generation: u64,
        discontinuity_epoch: u64,
        latched: bool,
    },
}

/// One immutable condition lifecycle audit event.
#[derive(Debug, Clone, Serialize)]
pub struct EntryConditionAuditView {
    pub audit_id: EntryConditionAuditId,
    pub condition_instance_id: EntryConditionInstanceId,
    pub revision: i64,
    pub action: EntryConditionAuditAction,
    pub from_state: Option<EntryConditionState>,
    pub to_state: EntryConditionState,
    pub truth: Option<ConditionTruth>,
    pub evaluation_hash: Option<ContentHash>,
    pub input_fingerprint: Option<ContentHash>,
    pub continuity_hash: Option<ContentHash>,
    pub lease_epoch: i64,
    pub detail: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

impl From<EntryConditionAuditInfo> for EntryConditionAuditView {
    fn from(info: EntryConditionAuditInfo) -> Self {
        Self {
            audit_id: info.audit_id,
            condition_instance_id: info.condition_instance_id,
            revision: info.revision,
            action: info.action,
            from_state: info.from_state,
            to_state: info.to_state,
            truth: info.truth_json,
            evaluation_hash: info.evaluation_hash,
            input_fingerprint: info.input_fingerprint,
            continuity_hash: info.continuity_hash,
            lease_epoch: info.lease_epoch,
            detail: info.detail,
            occurred_at: info.occurred_at,
        }
    }
}

/// Authoritative read-time projection of one lot's governed exit monitor.
#[derive(Debug, Clone, Serialize)]
pub struct ExitMonitorObservationView {
    pub state: ExitState,
    pub reason: Option<ExitReason>,
    pub current_executable_bid: Option<Price>,
    pub book_observed_at: Option<DateTime<Utc>>,
    pub book_age_ms: Option<u64>,
    pub book_fresh: bool,
    pub peak_mark: Option<Price>,
    pub effective_stop: Option<Price>,
    pub next_scale_out: Option<NextScaleOutProjection>,
    pub cumulative_exited_shares: Shares,
    pub cumulative_exit_pct: Option<Decimal>,
    pub latest_reinference: Option<ExitReinferenceObservation>,
    pub last_check_at: Option<DateTime<Utc>>,
    pub next_check_at: Option<DateTime<Utc>>,
}

/// Outbound projection of an execution order (the result of a submission).
#[derive(Debug, Clone, Serialize)]
pub struct ExecutionOrderView {
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

impl From<ExecutionOrderInfo> for ExecutionOrderView {
    fn from(info: ExecutionOrderInfo) -> Self {
        Self {
            execution_order_id: info.execution_order_id,
            order_intent_id: info.order_intent_id,
            order_phase: info.order_phase,
            market_id: info.market_id,
            token_id: info.token_id,
            side: info.side,
            order_type: info.order_type,
            price: info.price,
            shares: info.shares,
            cost_usd: info.cost_usd,
            venue_order_id: info.venue_order_id,
            venue_status: info.venue_status,
            state: info.state,
            submitted_at: info.submitted_at,
            filled_at: info.filled_at,
            cancelled_at: info.cancelled_at,
            gtd_expiration_at: info.gtd_expiration_at,
            error_message: info.error_message,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// Read-port aggregate: one position lot with its originating recommendation.
///
/// The lot row (`PositionInfo`) carries only `order_intent_id`; the read path
/// joins in `recommendation_id` so the operator can deep-link a lot straight to
/// its recommendation attribution without a second hop through the intent.
#[derive(Debug, Clone)]
pub struct PositionSummary {
    pub position: PositionInfo,
    pub recommendation_id: RecommendationId,
}

/// Outbound projection of one per-intent position lot.
#[derive(Debug, Clone, Serialize)]
pub struct PositionView {
    /// Distinguishes system lot ledger from venue account positions (`/quant/account/*`).
    pub position_plane: &'static str,
    pub position_id: PositionId,
    pub order_intent_id: OrderIntentId,
    /// Originating recommendation (attribution deep-link target).
    pub recommendation_id: RecommendationId,
    pub token_id: TokenId,
    pub market_id: MarketId,
    pub state: PositionLedgerState,
    pub shares: Shares,
    pub avg_price: Price,
    pub cost_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub opened_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
}

impl From<PositionSummary> for PositionView {
    fn from(summary: PositionSummary) -> Self {
        let PositionSummary {
            position: info,
            recommendation_id,
        } = summary;
        Self {
            position_plane: "system_lot",
            position_id: info.position_id,
            order_intent_id: info.order_intent_id,
            recommendation_id,
            token_id: info.token_id,
            market_id: info.market_id,
            state: info.state,
            shares: info.shares,
            avg_price: info.avg_price,
            cost_usd: info.cost_usd,
            realized_pnl_usd: info.realized_pnl_usd,
            opened_at: info.opened_at,
            updated_at: info.updated_at,
            closed_at: info.closed_at,
        }
    }
}

/// Strong detail response: lot truth plus its associated exit-monitor projection.
#[derive(Debug, Clone, Serialize)]
pub struct PositionDetailView {
    pub position: PositionView,
    pub exit_monitor_observation: ExitMonitorObservationView,
}

/// Outbound projection of the final WORM recommendation attribution.
#[derive(Debug, Clone, Serialize)]
pub struct RecommendationAttributionView {
    pub recommendation_id: RecommendationId,
    pub outcome: RecommendationAttributionOutcome,
    pub realized_pnl_usd: Option<Usd>,
    pub max_adverse_excursion_bps: Option<Decimal>,
    pub max_favorable_excursion_bps: Option<Decimal>,
    pub label_available_at: Option<DateTime<Utc>>,
    pub entry_outcome: EntryOutcome,
    pub exit_outcome: ExitOutcome,
    pub attribution: AttributionDetail,
    pub created_at: DateTime<Utc>,
}

impl From<RecommendationAttributionInfo> for RecommendationAttributionView {
    fn from(info: RecommendationAttributionInfo) -> Self {
        Self {
            recommendation_id: info.recommendation_id,
            outcome: info.outcome,
            realized_pnl_usd: info.realized_pnl_usd,
            max_adverse_excursion_bps: info.max_adverse_excursion_bps,
            max_favorable_excursion_bps: info.max_favorable_excursion_bps,
            label_available_at: info.label_available_at,
            entry_outcome: info.entry_outcome_json,
            exit_outcome: info.exit_outcome_json,
            attribution: info.attribution_json,
            created_at: info.created_at,
        }
    }
}

/// Paginated filter for listing order intents.
///
/// `from` / `to` bound `created_at`; the pagination window is the shared
/// [`PageRequest`], flattened so the query string stays flat.
///
/// `statuses` is a comma-separated multi-status filter driving the queue
/// console's triage presets (e.g. `approved,approved_by_policy`). When present
/// it supersedes the single `status`. `approval_status` narrows the ledger by
/// human/policy approval provenance.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct OrderIntentListQuery {
    pub status: Option<OrderIntentStatus>,
    #[serde(default, deserialize_with = "deserialize_statuses_csv")]
    pub statuses: Option<Vec<OrderIntentStatus>>,
    pub approval_status: Option<ApprovalStatus>,
    pub runtime_mode: Option<QuantRuntimeMode>,
    pub recommendation_id: Option<RecommendationId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Decode a comma-separated `statuses` query value (`a,b,c`) into a
/// `Vec<OrderIntentStatus>` via each variant's canonical wire label.
///
/// `web::Query` (`serde_urlencoded`) cannot decode repeated keys, so the queue
/// console sends one CSV field. An empty or whitespace-only value decodes to
/// `None`; an unknown label fails the request (fail-closed).
fn deserialize_statuses_csv<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<OrderIntentStatus>>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(raw) = Option::<String>::deserialize(deserializer)? else {
        return Ok(None);
    };
    let mut statuses = Vec::new();
    for token in raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let status = token
            .parse::<OrderIntentStatus>()
            .map_err(D::Error::custom)?;
        statuses.push(status);
    }
    Ok((!statuses.is_empty()).then_some(statuses))
}

/// Paginated filter for listing execution orders.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct ExecutionOrderListQuery {
    pub state: Option<ExecutionOrderState>,
    pub order_phase: Option<ExecutionOrderPhase>,
    pub order_intent_id: Option<OrderIntentId>,
    pub market_id: Option<MarketId>,
    pub token_id: Option<TokenId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Paginated filter for listing position lots.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct PositionListQuery {
    pub state: Option<PositionLedgerState>,
    pub order_intent_id: Option<OrderIntentId>,
    pub market_id: Option<MarketId>,
    pub token_id: Option<TokenId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Inbound body for `POST /quant/intents` (create from a recommendation).
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct CreateIntentRequest {
    pub recommendation_id: RecommendationId,
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Inbound body for `POST /quant/intents/{id}/approve`.
///
/// Approval may only narrow the frozen tagged amount and side-aware price.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct ApproveIntentRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
    pub override_amount: Option<OrderAmount>,
    pub override_price: Option<Price>,
}

/// Inbound body for `POST /quant/intents/{id}/reject`.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct RejectIntentRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Inbound body for `POST /quant/intents/{id}/cancel`.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct CancelIntentRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::OrderIntentListQuery;
    use crate::enums::quant::OrderIntentStatus;

    fn parse_statuses(csv: &str) -> Option<Vec<OrderIntentStatus>> {
        let query: OrderIntentListQuery =
            serde_json::from_value(serde_json::json!({ "statuses": csv })).unwrap();
        query.statuses
    }

    #[test]
    fn statuses_csv_decodes_each_wire_label() {
        assert_eq!(
            parse_statuses("approved,approved_by_policy"),
            Some(vec![
                OrderIntentStatus::Approved,
                OrderIntentStatus::ApprovedByPolicy,
            ]),
        );
    }

    #[test]
    fn statuses_csv_trims_and_skips_blanks() {
        assert_eq!(
            parse_statuses(" submitted , , partially_filled "),
            Some(vec![
                OrderIntentStatus::Submitted,
                OrderIntentStatus::PartiallyFilled,
            ]),
        );
    }

    #[test]
    fn statuses_csv_empty_is_none() {
        assert_eq!(parse_statuses(""), None);
        assert_eq!(parse_statuses("   "), None);
    }

    #[test]
    fn statuses_csv_rejects_unknown_label() {
        let result: Result<OrderIntentListQuery, _> =
            serde_json::from_value(serde_json::json!({ "statuses": "approved,bogus" }));
        assert!(result.is_err());
    }

    #[test]
    fn absent_statuses_defaults_to_none() {
        let query: OrderIntentListQuery = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(query.statuses, None);
    }
}
