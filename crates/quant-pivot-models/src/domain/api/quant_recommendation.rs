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

use crate::{
    domain::RecommendationInfo,
    enums::quant::{OutcomeSide, RecommendationStatus},
    types::{
        Bps, EntryPlan, EventId, EvidenceRefs, ExecutionEligibility, ExitPlan, MarketContext,
        MarketId, Probability, RecommendationFactorBreakdown, RecommendationId,
        RecommendationIdentity, RecommendationReportId, RiskEnvelope, SizingPlan, TokenId,
    },
};
use chrono::{DateTime, Utc};
use serde::Serialize;

/// Full outbound projection of one actionable recommendation.
#[derive(Debug, Clone, Serialize)]
pub struct QuantRecommendationView {
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub rank: i32,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub composite_score: Probability,
    pub risk_adjusted_score: Probability,
    pub confidence: Probability,
    pub expected_return_bps: Bps,
    pub downside_bps: Bps,
    pub identity: RecommendationIdentity,
    pub market_context: MarketContext,
    pub rank_before_portfolio: i32,
    pub liquidity_score: Probability,
    pub data_quality_score: Probability,
    pub model_score_percentile: Probability,
    pub entry_plan: EntryPlan,
    pub sizing_plan: SizingPlan,
    pub exit_plan: ExitPlan,
    pub risk_envelope: RiskEnvelope,
    pub factor_breakdown: RecommendationFactorBreakdown,
    pub execution_eligibility: ExecutionEligibility,
    pub valid_from: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub status: RecommendationStatus,
    pub created_at: DateTime<Utc>,
}

impl From<RecommendationInfo> for QuantRecommendationView {
    fn from(info: RecommendationInfo) -> Self {
        Self {
            recommendation_id: info.recommendation_id,
            recommendation_report_id: info.recommendation_report_id,
            rank: info.rank,
            market_id: info.market_id,
            event_id: info.event_id,
            token_id: info.token_id,
            outcome_side: info.outcome_side,
            composite_score: info.composite_score,
            risk_adjusted_score: info.risk_adjusted_score,
            confidence: info.confidence,
            expected_return_bps: info.expected_return_bps,
            downside_bps: info.downside_bps,
            identity: info.identity,
            market_context: info.market_context,
            rank_before_portfolio: info.rank_before_portfolio,
            liquidity_score: info.liquidity_score,
            data_quality_score: info.data_quality_score,
            model_score_percentile: info.model_score_percentile,
            entry_plan: info.entry_plan,
            sizing_plan: info.sizing_plan,
            exit_plan: info.exit_plan,
            risk_envelope: info.risk_envelope,
            factor_breakdown: info.factor_breakdown,
            execution_eligibility: info.execution_eligibility,
            valid_from: info.valid_from,
            valid_until: info.valid_until,
            status: info.status,
            created_at: info.created_at,
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
    pub runtime_config_version_id: String,
    pub model_version_id: String,
    pub factor_definition_versions: Vec<String>,
    pub data_quality_snapshot_ref: String,
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
            runtime_config_version_id: evidence.runtime_config_version_id.to_string(),
            model_version_id: evidence.model_version_id.to_string(),
            factor_definition_versions: evidence
                .factor_definition_versions
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            data_quality_snapshot_ref: evidence.data_quality_snapshot_ref.to_string(),
        }
    }
}

impl From<RecommendationInfo> for QuantEvidenceView {
    fn from(info: RecommendationInfo) -> Self {
        Self::new(info.recommendation_id, info.evidence_refs)
    }
}
