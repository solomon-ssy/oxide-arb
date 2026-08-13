//! Quant recommendation HTTP contract types (outbound projections).
//!
//! A [`QuantRecommendationView`] is the full per-recommendation decision contract
//! (entry / sizing / exit / risk / factor breakdown / eligibility). It reuses the
//! strong-typed payload value objects (`types::report_payload`) as leaf blocks —
//! those are pure decision contracts with no internal handles — while the view
//! itself is the projection boundary that selects which columns leave the system
//! (the persistence `RecommendationInfo` is never serialized directly).
//!
//! [`QuantEvidenceView`] exposes replay handles as opaque strings so a client can
//! reconstruct the decision trail without binding to internal id newtypes.

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{
    domain::{
        api::{DecisionBoundaryEvidenceView, FeatureCellEvidenceView, ModelInputEvidenceView},
        quant::{ExecutableEconomicTier, RecommendationEconomics, RecommendationInfo},
    },
    enums::quant::{OutcomeSide, RecommendationReportStatus, RecommendationStatus},
    runtime_config::BuyModelRoute,
    types::{
        EconomicTierId, EventId, EvidenceRefs, ExecutionEligibility, MarketContext, MarketId,
        OrderIntentId, PortfolioPlanId, RecommendationFactorBreakdown, RecommendationId,
        RecommendationIdentity, RecommendationReportId, RecommendationTradePlan, ReportRouteRunId,
        TokenId,
    },
};

/// Full outbound projection of one actionable recommendation.
///
/// Beyond the frozen decision contract, the view carries the two governance
/// facts a client needs to decide whether an `OrderIntent` may still be
/// created without a follow-up round-trip: the parent report's current
/// lifecycle [`Self::report_status`] and the id of any blocking pre-submission
/// intent [`Self::active_order_intent_id`]. Both are resolved server-side (the
/// single source of truth) so the intent-creation gate never guesses.
#[derive(Debug, Clone, Serialize)]
pub struct QuantRecommendationView {
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub report_route_run_id: ReportRouteRunId,
    pub portfolio_plan_id: PortfolioPlanId,
    pub economic_tier_id: EconomicTierId,
    pub rank: i32,
    pub route: BuyModelRoute,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub economics: RecommendationEconomics,
    pub economic_tier: ExecutableEconomicTier,
    pub identity: RecommendationIdentity,
    pub market_context: MarketContext,
    pub trade_plan: RecommendationTradePlan,
    pub factor_breakdown: RecommendationFactorBreakdown,
    pub execution_eligibility: ExecutionEligibility,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub status: RecommendationStatus,
    pub created_at: DateTime<Utc>,
    /// Current lifecycle state of the parent report (authoritative).
    pub report_status: RecommendationReportStatus,
    /// Id of the blocking pre-submission order intent, when one already exists.
    pub active_order_intent_id: Option<OrderIntentId>,
}

/// Assembly input for a [`QuantRecommendationView`].
///
/// The view joins facts owned by three repositories (recommendation, report,
/// order intent). This context is the single construction path — there is no
/// `From<RecommendationInfo>` shortcut — so a view can never be emitted without
/// its governance facts resolved.
#[derive(Debug, Clone)]
pub struct RecommendationViewContext {
    pub recommendation: RecommendationInfo,
    pub report_status: RecommendationReportStatus,
    pub active_order_intent_id: Option<OrderIntentId>,
}

impl From<RecommendationViewContext> for QuantRecommendationView {
    fn from(ctx: RecommendationViewContext) -> Self {
        let RecommendationViewContext {
            recommendation: info,
            report_status,
            active_order_intent_id,
        } = ctx;
        Self {
            recommendation_id: info.recommendation_id,
            recommendation_report_id: info.recommendation_report_id,
            report_route_run_id: info.report_route_run_id,
            portfolio_plan_id: info.portfolio_plan_id,
            economic_tier_id: info.economic_tier_id,
            rank: info.rank,
            route: info.route,
            market_id: info.market_id,
            event_id: info.event_id,
            token_id: info.token_id,
            outcome_side: info.outcome_side,
            economics: info.economics_json,
            economic_tier: info.economic_tier_json,
            identity: info.identity,
            market_context: info.market_context,
            trade_plan: info.trade_plan,
            factor_breakdown: info.factor_breakdown,
            execution_eligibility: info.execution_eligibility,
            valid_from: info.valid_from,
            valid_until: info.valid_until,
            status: info.status,
            created_at: info.created_at,
            report_status,
            active_order_intent_id,
        }
    }
}

/// Replay handles for one recommendation, projected as opaque strings.
///
/// Every id is rendered as a string so a client can feed it back into a replay
/// query without depending on the internal id newtypes; no mutable handle is
/// exposed.
#[derive(Debug, Clone, Serialize)]
pub struct QuantEvidenceView {
    pub recommendation_id: RecommendationId,
    pub signal_candidate_id: String,
    pub feature_vector_id: String,
    pub model_run_id: String,
    pub market_selection_id: String,
    pub book_snapshot_ref: String,
    pub decision_policy_snapshot_id: String,
    pub model_version_id: String,
    pub factor_definition_versions: Vec<String>,
    pub data_quality_snapshot_ref: String,
    /// True only when the serving run's durable completion marker exists.
    pub evidence_complete: bool,
    pub decision_boundary: Option<DecisionBoundaryEvidenceView>,
    pub feature_schema_hash: Option<String>,
    pub feature_hash: Option<String>,
    pub feature_cells: Vec<FeatureCellEvidenceView>,
    pub model_inputs: Vec<ModelInputEvidenceView>,
}

impl QuantEvidenceView {
    /// Build an evidence view from a recommendation id and its evidence refs.
    #[must_use]
    pub fn new(recommendation_id: RecommendationId, evidence: EvidenceRefs) -> Self {
        Self {
            recommendation_id,
            signal_candidate_id: evidence.signal_candidate_id.to_string(),
            feature_vector_id: evidence.feature_vector_id.to_string(),
            model_run_id: evidence.model_run_id.to_string(),
            market_selection_id: evidence.market_selection_id.to_string(),
            book_snapshot_ref: evidence.book_snapshot_ref.canonical_string(),
            decision_policy_snapshot_id: evidence.decision_policy_snapshot_id.to_string(),
            model_version_id: evidence.model_version_id.to_string(),
            factor_definition_versions: evidence
                .factor_definition_versions
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            data_quality_snapshot_ref: evidence.data_quality_snapshot_ref.to_string(),
            evidence_complete: false,
            decision_boundary: None,
            feature_schema_hash: None,
            feature_hash: None,
            feature_cells: Vec::new(),
            model_inputs: Vec::new(),
        }
    }
}

impl From<RecommendationInfo> for QuantEvidenceView {
    fn from(info: RecommendationInfo) -> Self {
        Self::new(info.recommendation_id, info.evidence_refs)
    }
}
