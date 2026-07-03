//! Order-intent execution HTTP contract types.
//!
//! Three families per the DTO paradigm: outbound `OrderIntentView` (`Serialize`
//! only, built from the persistence `OrderIntentInfo`), the inbound paginated
//! `OrderIntentListQuery`, and the governed mutation requests
//! (`CreateIntentRequest` / `ApproveIntentRequest` / `RejectIntentRequest` /
//! `CancelIntentRequest`, all `Deserialize` + `Validate`). The persistence
//! struct is never serialized directly.

use crate::{
    domain::{
        ExecutionOrderInfo, OrderIntentInfo, PositionInfo, RecommendationAttributionInfo,
        pagination::PageRequest,
    },
    enums::{
        common::Side,
        execution::{
            ExecutionOrderPhase, OrderIntentKind, OrderTypeKind, PositionLedgerState,
            VenueOrderStatus,
        },
        quant::{
            ApprovalStatus, ExecutionOrderState, OrderIntentStatus, QuantRuntimeMode,
            RecommendationAttributionOutcome,
        },
    },
    types::{
        AttributionDetail, ContentHash, EntryOrderSpec, EntryOutcome, ExecutionOrderId,
        ExitOutcome, ExitPolicySpec, MarketId, ModelVersionId, OrderId, OrderIntentId, PositionId,
        Price, RecommendationId, RuntimeConfigVersionId, Shares, TokenId, Usd,
    },
};
use chrono::{DateTime, Utc};
use serde::de::Error as _;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;

/// Outbound projection of a governed order intent (full operator transparency).
#[derive(Debug, Clone, Serialize)]
pub struct OrderIntentView {
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
    pub entry_order: EntryOrderSpec,
    pub exit_policy: ExitPolicySpec,
    pub risk_envelope_hash: ContentHash,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<OrderIntentInfo> for OrderIntentView {
    fn from(info: OrderIntentInfo) -> Self {
        Self {
            order_intent_id: info.order_intent_id,
            recommendation_id: info.recommendation_id,
            runtime_mode: info.runtime_mode,
            runtime_config_version_id: info.runtime_config_version_id,
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
            entry_order: info.entry_order_json,
            exit_policy: info.exit_policy_json,
            risk_envelope_hash: info.risk_envelope_hash,
            expires_at: info.expires_at,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
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

/// Outbound projection of the final WORM recommendation attribution.
#[derive(Debug, Clone, Serialize)]
pub struct RecommendationAttributionView {
    pub recommendation_id: RecommendationId,
    pub outcome: RecommendationAttributionOutcome,
    pub realized_pnl_usd: Option<Usd>,
    pub max_adverse_excursion_bps: Option<rust_decimal::Decimal>,
    pub max_favorable_excursion_bps: Option<rust_decimal::Decimal>,
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

/// Inbound body for `POST /quant/intents/{id}/submit` (operator-triggered).
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct SubmitIntentRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
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
#[derive(Debug, Clone, Default, Deserialize)]
pub struct OrderIntentListQuery {
    pub status: Option<OrderIntentStatus>,
    #[serde(default, deserialize_with = "deserialize_statuses_csv")]
    pub statuses: Option<Vec<OrderIntentStatus>>,
    pub approval_status: Option<ApprovalStatus>,
    pub runtime_mode: Option<QuantRuntimeMode>,
    pub recommendation_id: Option<RecommendationId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub page: PageRequest,
}

impl OrderIntentListQuery {
    /// Return a copy with the embedded pagination window normalized.
    #[must_use]
    pub fn normalized(self) -> Self {
        Self {
            page: self.page.normalized(),
            ..self
        }
    }
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
    D: serde::Deserializer<'de>,
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
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExecutionOrderListQuery {
    pub state: Option<ExecutionOrderState>,
    pub order_phase: Option<ExecutionOrderPhase>,
    pub order_intent_id: Option<OrderIntentId>,
    pub market_id: Option<MarketId>,
    pub token_id: Option<TokenId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub page: PageRequest,
}

impl ExecutionOrderListQuery {
    #[must_use]
    pub fn normalized(self) -> Self {
        Self {
            page: self.page.normalized(),
            ..self
        }
    }
}

/// Paginated filter for listing position lots.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PositionListQuery {
    pub state: Option<PositionLedgerState>,
    pub order_intent_id: Option<OrderIntentId>,
    pub market_id: Option<MarketId>,
    pub token_id: Option<TokenId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub page: PageRequest,
}

impl PositionListQuery {
    #[must_use]
    pub fn normalized(self) -> Self {
        Self {
            page: self.page.normalized(),
            ..self
        }
    }
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
/// Approval may only narrow the order: `override_shares` / `override_limit_price`
/// must be ≤ the frozen entry, and `max_allowed_usd` caps the notional.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct ApproveIntentRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
    pub override_shares: Option<Shares>,
    pub override_limit_price: Option<Price>,
    pub max_allowed_usd: Option<Usd>,
    #[validate(length(min = 1, max = 512))]
    pub override_note: Option<String>,
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
