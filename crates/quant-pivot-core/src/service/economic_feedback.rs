//! Public recommendation-economic feedback reads over immutable source artifacts.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{
    QuantError, QuantResult,
    research::ResearchError,
    storage::{StorageError, entity::QUANT_RECOMMENDATION},
};
use quant_pivot_models::{
    domain::{
        api::{
            EconomicHealthQuery, EvaluatedExecutionComparisonView,
            ExecutionComparisonEvaluationView, ExecutionComparisonNotEvaluableReasonView,
            RecommendationEconomicOutcomeView, RecommendationExecutionComparisonView,
            RouteEconomicHealthView,
        },
        pagination::{NormalizePageQuery, PageWindow, Paginated},
        ports::EconomicFeedbackPort,
        quant::RecommendationInfo,
    },
    enums::quant::AttributionArtifactKind,
    types::{ContentHash, RecommendationId},
};
use quant_pivot_repository::traits::{
    AttributionArtifactRepository, RecommendationEconomicOutcomeRepository,
    RecommendationReportRepository, RecommendationRepository, RouteEconomicHealthRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    attribution::{
        AttributionArtifact, AttributionArtifactCodec, ExecutionTrajectoryArtifact,
        PolicyCounterfactualOutcome,
    },
    execution_comparison::{
        ExecutionComparisonEvaluation, ExecutionComparisonNotEvaluableReason,
        PlannedActualExecutionComparisonBuilder, PlannedActualExecutionComparisonInput,
    },
};

pub struct EconomicFeedbackServiceDeps {
    pub outcomes: Arc<dyn RecommendationEconomicOutcomeRepository>,
    pub route_health: Arc<dyn RouteEconomicHealthRepository>,
    pub attribution_index: Arc<dyn AttributionArtifactRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub recommendations: Arc<dyn RecommendationRepository>,
    pub reports: Arc<dyn RecommendationReportRepository>,
}

pub struct EconomicFeedbackService {
    outcomes: Arc<dyn RecommendationEconomicOutcomeRepository>,
    route_health: Arc<dyn RouteEconomicHealthRepository>,
    attribution_index: Arc<dyn AttributionArtifactRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    recommendations: Arc<dyn RecommendationRepository>,
    reports: Arc<dyn RecommendationReportRepository>,
}

impl EconomicFeedbackService {
    #[must_use]
    pub fn new(deps: EconomicFeedbackServiceDeps) -> Self {
        Self {
            outcomes: deps.outcomes,
            route_health: deps.route_health,
            attribution_index: deps.attribution_index,
            artifacts: deps.artifacts,
            recommendations: deps.recommendations,
            reports: deps.reports,
        }
    }

    async fn artifact_pair(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<
        Option<(
            ExecutionTrajectoryArtifact,
            ContentHash,
            PolicyCounterfactualOutcome,
            ContentHash,
        )>,
    > {
        let Some(trajectory_info) = self
            .attribution_index
            .latest_for_recommendation(
                recommendation_id,
                AttributionArtifactKind::ExecutionTrajectory,
            )
            .await?
        else {
            return Ok(None);
        };
        let Some(counterfactual_info) = self
            .attribution_index
            .latest_for_recommendation(
                recommendation_id,
                AttributionArtifactKind::PolicyCounterfactualOutcome,
            )
            .await?
        else {
            return Ok(None);
        };
        let trajectory_bytes = self.artifacts.get(&trajectory_info.artifact_uri).await?;
        let counterfactual_bytes = self
            .artifacts
            .get(&counterfactual_info.artifact_uri)
            .await?;
        if AttributionArtifactCodec::hash(&trajectory_bytes) != trajectory_info.artifact_hash
            || AttributionArtifactCodec::hash(&counterfactual_bytes)
                != counterfactual_info.artifact_hash
        {
            return Err(methodology(
                "attribution index hash differs from artifact bytes",
            ));
        }
        let AttributionArtifact::ExecutionTrajectory(trajectory) =
            AttributionArtifactCodec::decode(&trajectory_bytes)?
        else {
            return Err(methodology(
                "trajectory index points to a different artifact kind",
            ));
        };
        let AttributionArtifact::PolicyCounterfactualOutcome(counterfactual) =
            AttributionArtifactCodec::decode(&counterfactual_bytes)?
        else {
            return Err(methodology(
                "counterfactual index points to a different artifact kind",
            ));
        };
        Ok(Some((
            *trajectory,
            trajectory_info.artifact_hash,
            *counterfactual,
            counterfactual_info.artifact_hash,
        )))
    }

    async fn require_recommendation(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<RecommendationInfo> {
        self.recommendations
            .find_by_id(recommendation_id)
            .await?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION, recommendation_id).into())
    }
}

#[async_trait]
impl EconomicFeedbackPort for EconomicFeedbackService {
    async fn recommendation_outcome(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<Option<RecommendationEconomicOutcomeView>> {
        let Some(outcome) = self.outcomes.find_by_id(recommendation_id).await? else {
            self.require_recommendation(recommendation_id).await?;
            return Ok(None);
        };
        outcome.verify().map_err(|error| {
            methodology(format!("economic outcome failed verification: {error}"))
        })?;
        Ok(Some(outcome.into()))
    }

    async fn execution_comparison(
        &self,
        recommendation_id: &RecommendationId,
    ) -> QuantResult<Option<RecommendationExecutionComparisonView>> {
        let Some(outcome) = self.outcomes.find_by_id(recommendation_id).await? else {
            self.require_recommendation(recommendation_id).await?;
            return Ok(None);
        };
        let Some(recommendation) = self.recommendations.find_by_id(recommendation_id).await? else {
            return Err(methodology("economic outcome lost its recommendation"));
        };
        let Some(report) = self
            .reports
            .find_by_id(&recommendation.recommendation_report_id)
            .await?
        else {
            return Err(methodology("recommendation lost its report"));
        };
        let Some((trajectory, trajectory_hash, counterfactual, counterfactual_hash)) =
            self.artifact_pair(recommendation_id).await?
        else {
            return Ok(None);
        };
        let comparison = PlannedActualExecutionComparisonBuilder::build(
            &PlannedActualExecutionComparisonInput {
                recommendation_id: *recommendation_id,
                decision_at: report.decision_at,
                requested_shares: recommendation.trade_plan.sizing.requested_shares,
                economic_outcome: &outcome,
                trajectory_artifact_hash: trajectory_hash,
                trajectory: &trajectory,
                policy_counterfactual_hash: counterfactual_hash,
                counterfactual: &counterfactual,
            },
        )?;
        Ok(Some(RecommendationExecutionComparisonView {
            recommendation_id: comparison.recommendation_id,
            economic_outcome_hash: comparison.economic_outcome_hash,
            trajectory_artifact_hash: comparison.trajectory_artifact_hash,
            policy_counterfactual_hash: comparison.policy_counterfactual_hash,
            evaluation: comparison_evaluation(comparison.evaluation),
            comparison_hash: comparison.comparison_hash,
        }))
    }

    async fn route_health(
        &self,
        query: EconomicHealthQuery,
        available_through: DateTime<Utc>,
    ) -> QuantResult<Paginated<RouteEconomicHealthView>> {
        let query = query.normalized();
        self.route_health
            .page_for_route(
                &query.route,
                available_through,
                PageWindow::from_query(&query),
            )
            .await
            .map(|page| page.map(Into::into))
            .map_err(Into::into)
    }
}

fn comparison_evaluation(
    evaluation: ExecutionComparisonEvaluation,
) -> ExecutionComparisonEvaluationView {
    match evaluation {
        ExecutionComparisonEvaluation::Evaluated { metrics } => {
            let metrics = *metrics;
            ExecutionComparisonEvaluationView::Evaluated {
                metrics: Box::new(EvaluatedExecutionComparisonView {
                    planned_entry_latency_ms: metrics.planned_entry_latency_ms,
                    actual_entry_latency_ms: metrics.actual_entry_latency_ms,
                    latency_delta_ms: metrics.latency_delta_ms,
                    planned_entry_price: metrics.planned_entry_price,
                    actual_entry_price: metrics.actual_entry_price,
                    actual_vs_planned_price_bps: metrics.actual_vs_planned_price_bps,
                    planned_fill_ratio: metrics.planned_fill_ratio,
                    actual_fill_ratio: metrics.actual_fill_ratio,
                    fill_ratio_delta: metrics.fill_ratio_delta,
                    planned_fee_usd: metrics.planned_fee_usd,
                    actual_fee_usd: metrics.actual_fee_usd,
                    fee_delta_usd: metrics.fee_delta_usd,
                    planned_net_return_bps: metrics.planned_net_return_bps,
                    actual_net_return_bps: metrics.actual_net_return_bps,
                    return_delta_bps: metrics.return_delta_bps,
                    policy_missed_return_bps: metrics.policy_missed_return_bps,
                }),
            }
        }
        ExecutionComparisonEvaluation::NotEvaluable { reason } => {
            ExecutionComparisonEvaluationView::NotEvaluable {
                reason: not_evaluable_reason(reason),
            }
        }
    }
}

const fn not_evaluable_reason(
    reason: ExecutionComparisonNotEvaluableReason,
) -> ExecutionComparisonNotEvaluableReasonView {
    match reason {
        ExecutionComparisonNotEvaluableReason::PlannedEntryUnavailable => {
            ExecutionComparisonNotEvaluableReasonView::PlannedEntryUnavailable
        }
        ExecutionComparisonNotEvaluableReason::PlannedEconomicsCensored => {
            ExecutionComparisonNotEvaluableReasonView::PlannedEconomicsCensored
        }
        ExecutionComparisonNotEvaluableReason::ActualBaselineUnavailable => {
            ExecutionComparisonNotEvaluableReasonView::ActualBaselineUnavailable
        }
        ExecutionComparisonNotEvaluableReason::IdentityMismatch => {
            ExecutionComparisonNotEvaluableReasonView::IdentityMismatch
        }
    }
}

fn methodology(detail: impl Into<String>) -> QuantError {
    ResearchError::ValidationMethodology {
        detail: detail.into(),
    }
    .into()
}
