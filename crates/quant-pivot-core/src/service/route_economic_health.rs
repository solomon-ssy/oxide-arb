//! Route-local economic-health assessment over immutable recommendation outcomes.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, report::ReportError};
use quant_pivot_models::{
    domain::quant::{
        NewRouteEconomicHealth, RouteEconomicHealthEvidenceDocument, RouteEconomicHealthInfo,
    },
    runtime_config::BuyModelRoute,
    types::{
        ContentHash, ResearchFeedbackPolicy, ResearchProfileArtifactId, RouteEconomicHealthId,
    },
};
use quant_pivot_repository::traits::RouteEconomicHealthRepository;
use quant_pivot_research::route_economic_health::{
    RouteEconomicHealthEvaluator, RouteEconomicHealthObservation, RouteEconomicHealthRequest,
};

const MAX_HEALTH_OBSERVATIONS: u64 = 1_000_000;

pub struct RouteEconomicHealthService {
    repository: Arc<dyn RouteEconomicHealthRepository>,
}

impl RouteEconomicHealthService {
    #[must_use]
    pub const fn new(repository: Arc<dyn RouteEconomicHealthRepository>) -> Self {
        Self { repository }
    }

    pub async fn assess(
        &self,
        route: &BuyModelRoute,
        route_identity_hash: ContentHash,
        profile_id: ResearchProfileArtifactId,
        policy: &ResearchFeedbackPolicy,
        assessed_through: DateTime<Utc>,
    ) -> QuantResult<RouteEconomicHealthInfo> {
        policy
            .validate()
            .map_err(|error| contract_error(&error.to_string()))?;
        let window_days = i64::from(policy.evaluation_window_days);
        let window_start = assessed_through
            .checked_sub_signed(Duration::days(window_days))
            .ok_or_else(|| contract_error("economic-health window underflowed UTC"))?;
        let sources = self
            .repository
            .source_window(
                route,
                &profile_id,
                window_start,
                assessed_through,
                assessed_through,
                MAX_HEALTH_OBSERVATIONS,
            )
            .await?;
        let observations = sources
            .into_iter()
            .map(|source| RouteEconomicHealthObservation {
                recommendation_id: source.recommendation_id,
                market_id: source.market_id,
                event_id: source.event_id,
                decision_at: source.decision_at,
                terminal_at: source.terminal_at,
                available_at: source.available_at,
                net_return_bps: source.net_return_bps,
            })
            .collect::<Vec<_>>();
        let assessment = RouteEconomicHealthEvaluator::evaluate(&RouteEconomicHealthRequest {
            route_identity_hash,
            assessed_through,
            policy,
            observations: &observations,
        })?;
        let due_observation_count = i64::try_from(assessment.due_observation_count)
            .map_err(|error| contract_error(&format!("due count overflow: {error}")))?;
        let usable_observation_count = i64::try_from(assessment.usable_observation_count)
            .map_err(|error| contract_error(&format!("usable count overflow: {error}")))?;
        let comparison_minimum_observations = i64::try_from(policy.comparison_minimum_observations)
            .map_err(|error| contract_error(&format!("comparison minimum overflow: {error}")))?;
        let feedback_policy_hash = policy
            .content_hash()
            .map_err(|error| contract_error(&error.to_string()))?;
        self.repository
            .insert(NewRouteEconomicHealth {
                route_economic_health_id: RouteEconomicHealthId::from_content_hash(
                    &assessment.evidence_hash,
                ),
                route: *route,
                route_identity_hash,
                research_profile_artifact_id: profile_id,
                feedback_policy_hash,
                state: assessment.state,
                window_start: assessment.window_start,
                assessed_through,
                due_observation_count,
                usable_observation_count,
                coverage: assessment.coverage,
                effective_sample_size: assessment.effective_sample_size,
                weighted_mean_return_bps: assessment.weighted_mean_return_bps,
                lower_confidence_return_bps: assessment.lower_confidence_return_bps,
                comparison_minimum_observations,
                minimum_coverage: policy.minimum_coverage,
                minimum_effect_bps: policy.minimum_effect_bps,
                confidence: policy.effect_confidence,
                evidence_json: RouteEconomicHealthEvidenceDocument {
                    observation_hash: assessment.observation_hash,
                    uniqueness_weight_hash: assessment.uniqueness_weight_hash,
                    methodology_version: assessment.methodology_version,
                },
                evidence_hash: assessment.evidence_hash,
                available_at: assessed_through,
            })
            .await
            .map_err(Into::into)
    }
}

fn contract_error(detail: &str) -> QuantError {
    ReportError::ContractViolation {
        detail: format!("Route economic health: {detail}"),
    }
    .into()
}
