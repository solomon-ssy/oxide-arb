//! Recommendation economics and Route health API projections.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    domain::{
        pagination::PageRequest,
        quant::{
            RecommendationEconomicOutcomeInfo, RecommendationEconomicOutcomePayload,
            RouteEconomicHealthEvidenceDocument, RouteEconomicHealthInfo,
        },
    },
    enums::quant::{RecommendationEconomicOutcomeState, RouteEconomicHealthState},
    runtime_config::BuyModelRoute,
    types::{
        Bps, ContentHash, DecisionPolicySnapshotId, EconomicTierId, ModelVersionId, Price,
        RecommendationId, RecommendationReportId, ReportRouteRunId, ResearchProfileArtifactId,
        TradePolicyArtifactId, Usd,
    },
};

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RecommendationEconomicOutcomeView {
    pub recommendation_id: RecommendationId,
    pub recommendation_report_id: RecommendationReportId,
    pub report_route_run_id: ReportRouteRunId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub economic_tier_id: EconomicTierId,
    pub model_version_id: ModelVersionId,
    pub trade_policy_artifact_id: TradePolicyArtifactId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub state: RecommendationEconomicOutcomeState,
    pub decision_at: DateTime<Utc>,
    pub horizon_at: DateTime<Utc>,
    pub source_available_until: DateTime<Utc>,
    pub replay_kernel_version: String,
    pub payload: RecommendationEconomicOutcomePayload,
    pub evidence_hash: ContentHash,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl From<RecommendationEconomicOutcomeInfo> for RecommendationEconomicOutcomeView {
    fn from(info: RecommendationEconomicOutcomeInfo) -> Self {
        Self {
            recommendation_id: info.recommendation_id,
            recommendation_report_id: info.recommendation_report_id,
            report_route_run_id: info.report_route_run_id,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            economic_tier_id: info.economic_tier_id,
            model_version_id: info.model_version_id,
            trade_policy_artifact_id: info.trade_policy_artifact_id,
            research_profile_artifact_id: info.research_profile_artifact_id,
            state: info.state,
            decision_at: info.decision_at,
            horizon_at: info.horizon_at,
            source_available_until: info.source_available_until,
            replay_kernel_version: info.replay_kernel_version,
            payload: info.payload_json,
            evidence_hash: info.evidence_hash,
            available_at: info.available_at,
            created_at: info.created_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionComparisonNotEvaluableReasonView {
    PlannedEntryUnavailable,
    PlannedEconomicsCensored,
    ActualBaselineUnavailable,
    IdentityMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum ExecutionComparisonEvaluationView {
    Evaluated {
        #[serde(flatten)]
        #[schemars(flatten)]
        metrics: Box<EvaluatedExecutionComparisonView>,
    },
    NotEvaluable {
        reason: ExecutionComparisonNotEvaluableReasonView,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct EvaluatedExecutionComparisonView {
    pub planned_entry_latency_ms: u64,
    pub actual_entry_latency_ms: u64,
    pub latency_delta_ms: i64,
    pub planned_entry_price: Price,
    pub actual_entry_price: Price,
    pub actual_vs_planned_price_bps: Bps,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub planned_fill_ratio: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub actual_fill_ratio: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub fill_ratio_delta: Decimal,
    pub planned_fee_usd: Usd,
    pub actual_fee_usd: Usd,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub fee_delta_usd: Decimal,
    pub planned_net_return_bps: Bps,
    pub actual_net_return_bps: Bps,
    pub return_delta_bps: Bps,
    pub policy_missed_return_bps: Option<Bps>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct RecommendationExecutionComparisonView {
    pub recommendation_id: RecommendationId,
    pub economic_outcome_hash: ContentHash,
    pub trajectory_artifact_hash: ContentHash,
    pub policy_counterfactual_hash: ContentHash,
    pub evaluation: ExecutionComparisonEvaluationView,
    pub comparison_hash: ContentHash,
}

#[derive(Debug, Clone, Deserialize, NormalizePageQuery)]
#[serde(deny_unknown_fields)]
pub struct EconomicHealthQuery {
    pub route: BuyModelRoute,
    #[serde(flatten)]
    #[normalize_page]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RouteEconomicHealthView {
    pub route: BuyModelRoute,
    pub state: RouteEconomicHealthState,
    pub route_identity_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub feedback_policy_hash: ContentHash,
    pub window_start: Option<DateTime<Utc>>,
    pub assessed_through: DateTime<Utc>,
    pub due_observation_count: i64,
    pub usable_observation_count: i64,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub coverage: Decimal,
    #[serde(with = "rust_decimal::serde::str_option")]
    #[schemars(with = "Option<String>")]
    pub effective_sample_size: Option<Decimal>,
    pub weighted_mean_return_bps: Option<Bps>,
    pub lower_confidence_return_bps: Option<Bps>,
    pub evidence: RouteEconomicHealthEvidenceDocument,
    pub evidence_hash: ContentHash,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

impl From<RouteEconomicHealthInfo> for RouteEconomicHealthView {
    fn from(info: RouteEconomicHealthInfo) -> Self {
        Self {
            route: info.route,
            state: info.state,
            route_identity_hash: info.route_identity_hash,
            research_profile_artifact_id: info.research_profile_artifact_id,
            feedback_policy_hash: info.feedback_policy_hash,
            window_start: info.window_start,
            assessed_through: info.assessed_through,
            due_observation_count: info.due_observation_count,
            usable_observation_count: info.usable_observation_count,
            coverage: info.coverage,
            effective_sample_size: info.effective_sample_size,
            weighted_mean_return_bps: info.weighted_mean_return_bps,
            lower_confidence_return_bps: info.lower_confidence_return_bps,
            evidence: info.evidence_json,
            evidence_hash: info.evidence_hash,
            available_at: info.available_at,
            created_at: info.created_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        domain::pagination::{NormalizePageQuery, PageRequest},
        runtime_config::BuyModelRoute,
    };

    use super::EconomicHealthQuery;

    #[test]
    fn health_query_is_bounded() {
        let query = EconomicHealthQuery {
            route: BuyModelRoute::Crypto,
            page: PageRequest {
                page: 0,
                size: u64::MAX,
            },
        }
        .normalized();

        assert_eq!(query.page.page, 1);
        assert_eq!(query.page.size, PageRequest::MAX_SIZE);
    }
}
