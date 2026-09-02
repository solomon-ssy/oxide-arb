//! Route-local executable economic health with overlap and cluster dependence.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::quant::RouteEconomicHealthState,
    hashing::CanonicalDigest,
    types::{Bps, ContentHash, EventId, MarketId, RecommendationId, ResearchFeedbackPolicy},
};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};

use crate::policy_validation::{PolicyPerformanceObservation, interval_uniqueness_weights};

pub const ROUTE_ECONOMIC_HEALTH_METHODOLOGY_VERSION: &str =
    "route_economic_health_average_uniqueness_event_cluster_circular_bootstrap_v1";
const OBSERVATION_HASH_DOMAIN: &str = "quant-pivot/route-economic-health-observations";
const WEIGHT_HASH_DOMAIN: &str = "quant-pivot/route-economic-health-weights";
const EVIDENCE_HASH_DOMAIN: &str = "quant-pivot/route-economic-health-evidence";
const HASH_VERSION: u32 = 1;

/// One horizon-due recommendation, including explicit unavailable economics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteEconomicHealthObservation {
    pub recommendation_id: RecommendationId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub decision_at: DateTime<Utc>,
    pub terminal_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub net_return_bps: Option<Bps>,
}

/// Frozen inputs for one Route health assessment.
pub struct RouteEconomicHealthRequest<'a> {
    pub route_identity_hash: ContentHash,
    pub assessed_through: DateTime<Utc>,
    pub policy: &'a ResearchFeedbackPolicy,
    pub observations: &'a [RouteEconomicHealthObservation],
}

/// Complete statistical evidence, with numeric values absent when not justified.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteEconomicHealthAssessment {
    pub state: RouteEconomicHealthState,
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: DateTime<Utc>,
    pub due_observation_count: u64,
    pub usable_observation_count: u64,
    pub coverage: Decimal,
    pub effective_sample_size: Option<Decimal>,
    pub weighted_mean_return_bps: Option<Bps>,
    pub lower_confidence_return_bps: Option<Bps>,
    pub observation_hash: ContentHash,
    pub uniqueness_weight_hash: Option<ContentHash>,
    pub methodology_version: String,
    pub evidence_hash: ContentHash,
}

#[derive(Serialize)]
struct BootstrapWord {
    route_identity_hash: ContentHash,
    seed: u64,
    repetition: u32,
    draw: u64,
    attempt: u64,
}

#[derive(Serialize)]
struct AssessmentPreimage<'a> {
    route_identity_hash: ContentHash,
    assessed_through: DateTime<Utc>,
    policy_hash: ContentHash,
    observations: &'a [RouteEconomicHealthObservation],
    assessment: AssessmentWithoutHash<'a>,
}

#[derive(Clone, Copy, Serialize)]
struct AssessmentWithoutHash<'a> {
    state: RouteEconomicHealthState,
    window_start: Option<DateTime<Utc>>,
    window_end: DateTime<Utc>,
    due_observation_count: u64,
    usable_observation_count: u64,
    coverage: Decimal,
    effective_sample_size: Option<Decimal>,
    weighted_mean_return_bps: Option<Bps>,
    lower_confidence_return_bps: Option<Bps>,
    observation_hash: ContentHash,
    uniqueness_weight_hash: Option<ContentHash>,
    methodology_version: &'a str,
}

struct UsableObservation<'a> {
    source: &'a RouteEconomicHealthObservation,
    return_bps: Decimal,
    weight: Decimal,
}

pub struct RouteEconomicHealthEvaluator;

impl RouteEconomicHealthEvaluator {
    pub fn evaluate(
        request: &RouteEconomicHealthRequest<'_>,
    ) -> QuantResult<RouteEconomicHealthAssessment> {
        request
            .policy
            .validate()
            .map_err(|error| methodology(format!("invalid feedback policy: {error}")))?;
        Self::validate_request(request)?;
        let due_count = u64::try_from(request.observations.len())
            .map_err(|error| methodology(format!("observation count overflow: {error}")))?;
        let usable_count = u64::try_from(
            request
                .observations
                .iter()
                .filter(|observation| observation.net_return_bps.is_some())
                .count(),
        )
        .map_err(|error| methodology(format!("usable count overflow: {error}")))?;
        let coverage = if due_count == 0 {
            Decimal::ZERO
        } else {
            (Decimal::from(usable_count) / Decimal::from(due_count)).round_dp(8)
        };
        let observation_hash = CanonicalDigest::content_hash_typed(
            OBSERVATION_HASH_DOMAIN,
            HASH_VERSION,
            &request.observations,
        )?;
        let window_start = request
            .observations
            .first()
            .map(|observation| observation.decision_at);
        let enough_due = due_count >= request.policy.comparison_minimum_observations;
        let enough_usable = usable_count >= request.policy.comparison_minimum_observations;
        if !enough_due {
            return Self::seal(
                request,
                AssessmentWithoutHash {
                    state: RouteEconomicHealthState::InsufficientEvidence,
                    window_start,
                    window_end: request.assessed_through,
                    due_observation_count: due_count,
                    usable_observation_count: usable_count,
                    coverage,
                    effective_sample_size: None,
                    weighted_mean_return_bps: None,
                    lower_confidence_return_bps: None,
                    observation_hash,
                    uniqueness_weight_hash: None,
                    methodology_version: ROUTE_ECONOMIC_HEALTH_METHODOLOGY_VERSION,
                },
            );
        }
        if !enough_usable || coverage < request.policy.minimum_coverage {
            return Self::seal(
                request,
                AssessmentWithoutHash {
                    state: RouteEconomicHealthState::DataIncomplete,
                    window_start,
                    window_end: request.assessed_through,
                    due_observation_count: due_count,
                    usable_observation_count: usable_count,
                    coverage,
                    effective_sample_size: None,
                    weighted_mean_return_bps: None,
                    lower_confidence_return_bps: None,
                    observation_hash,
                    uniqueness_weight_hash: None,
                    methodology_version: ROUTE_ECONOMIC_HEALTH_METHODOLOGY_VERSION,
                },
            );
        }
        let usable = Self::usable_observations(request.observations)?;
        let weights = usable.iter().map(|item| item.weight).collect::<Vec<_>>();
        let weight_hash = CanonicalDigest::content_hash_typed(
            WEIGHT_HASH_DOMAIN,
            HASH_VERSION,
            &request
                .observations
                .iter()
                .filter(|observation| observation.net_return_bps.is_some())
                .map(|observation| observation.recommendation_id)
                .zip(&weights)
                .collect::<Vec<_>>(),
        )?;
        let effective_sample_size = Self::effective_sample_size(&weights)?;
        let weighted_mean = Self::weighted_mean(&usable)?;
        let lower_bound = Self::clustered_lower_bound(request, &usable)?;
        let state = if lower_bound >= request.policy.minimum_effect_bps.inner() {
            RouteEconomicHealthState::Healthy
        } else {
            RouteEconomicHealthState::Degraded
        };
        Self::seal(
            request,
            AssessmentWithoutHash {
                state,
                window_start,
                window_end: request.assessed_through,
                due_observation_count: due_count,
                usable_observation_count: usable_count,
                coverage,
                effective_sample_size: Some(effective_sample_size),
                weighted_mean_return_bps: Some(Bps::new(weighted_mean)),
                lower_confidence_return_bps: Some(Bps::new(lower_bound)),
                observation_hash,
                uniqueness_weight_hash: Some(weight_hash),
                methodology_version: ROUTE_ECONOMIC_HEALTH_METHODOLOGY_VERSION,
            },
        )
    }

    fn validate_request(request: &RouteEconomicHealthRequest<'_>) -> QuantResult<()> {
        if request.assessed_through.timestamp_millis() <= 0 {
            return Err(methodology("assessment cutoff must be positive"));
        }
        let mut ids = BTreeSet::new();
        let mut prior = None;
        for observation in request.observations {
            let duplicate = !ids.insert(observation.recommendation_id.to_string());
            let invalid_timeline = observation.decision_at >= observation.terminal_at
                || observation.terminal_at > observation.available_at;
            let unavailable_at_cutoff = observation.available_at > request.assessed_through;
            let out_of_order = prior.is_some_and(|prior| prior > observation.decision_at);
            if duplicate || invalid_timeline || unavailable_at_cutoff || out_of_order {
                return Err(methodology(
                    "economic health observations are malformed, duplicated, or unordered",
                ));
            }
            prior = Some(observation.decision_at);
        }
        Ok(())
    }

    fn usable_observations(
        observations: &[RouteEconomicHealthObservation],
    ) -> QuantResult<Vec<UsableObservation<'_>>> {
        let policy_observations = observations
            .iter()
            .filter_map(|observation| {
                observation
                    .net_return_bps
                    .map(|return_bps| PolicyPerformanceObservation {
                        observation_id: observation.recommendation_id.to_string(),
                        market_id: observation.market_id.clone(),
                        decision_at: observation.decision_at,
                        label_horizon_end: observation.terminal_at,
                        candidate_expected_return_bps: vec![Some(return_bps.inner())],
                        candidate_risk_return_bps: vec![Some(return_bps.inner())],
                    })
            })
            .collect::<Vec<_>>();
        let weights = interval_uniqueness_weights(&policy_observations)?;
        observations
            .iter()
            .filter_map(|observation| {
                observation
                    .net_return_bps
                    .map(|return_bps| (observation, return_bps.inner()))
            })
            .zip(weights)
            .map(|((source, return_bps), weight)| {
                if weight <= Decimal::ZERO {
                    return Err(methodology("economic uniqueness weight must be positive"));
                }
                Ok(UsableObservation {
                    source,
                    return_bps,
                    weight,
                })
            })
            .collect()
    }

    fn effective_sample_size(weights: &[Decimal]) -> QuantResult<Decimal> {
        let sum = weights.iter().copied().sum::<Decimal>();
        let squares = weights
            .iter()
            .map(|weight| *weight * *weight)
            .sum::<Decimal>();
        if sum <= Decimal::ZERO || squares <= Decimal::ZERO {
            return Err(methodology(
                "economic effective sample size has zero weight",
            ));
        }
        Ok((sum * sum / squares).round_dp(8))
    }

    fn weighted_mean(observations: &[UsableObservation<'_>]) -> QuantResult<Decimal> {
        let weighted = observations
            .iter()
            .map(|observation| observation.return_bps * observation.weight)
            .sum::<Decimal>();
        let weight = observations
            .iter()
            .map(|observation| observation.weight)
            .sum::<Decimal>();
        if weight <= Decimal::ZERO {
            return Err(methodology("economic weighted mean has zero weight"));
        }
        Ok((weighted / weight).round_dp(8))
    }

    fn clustered_lower_bound(
        request: &RouteEconomicHealthRequest<'_>,
        observations: &[UsableObservation<'_>],
    ) -> QuantResult<Decimal> {
        let mut clusters = BTreeMap::<&EventId, Vec<&UsableObservation<'_>>>::new();
        for observation in observations {
            clusters
                .entry(&observation.source.event_id)
                .or_default()
                .push(observation);
        }
        let clusters = clusters.into_values().collect::<Vec<_>>();
        let block_length = usize::try_from(request.policy.comparison_block_length)
            .map_err(|error| methodology(format!("block length overflow: {error}")))?;
        let mut replicates = Vec::with_capacity(
            usize::try_from(request.policy.comparison_bootstrap_repetitions)
                .map_err(|error| methodology(format!("repetition count overflow: {error}")))?,
        );
        for repetition in 0..request.policy.comparison_bootstrap_repetitions {
            let mut weighted = Decimal::ZERO;
            let mut total_weight = Decimal::ZERO;
            for draw in 0..clusters.len() {
                let cluster_index = Self::bootstrap_index(
                    request,
                    repetition,
                    u64::try_from(draw)
                        .map_err(|error| methodology(format!("cluster draw overflow: {error}")))?,
                    clusters.len(),
                )?;
                let cluster = &clusters[cluster_index];
                let length = block_length.min(cluster.len());
                let start = Self::bootstrap_index(
                    request,
                    repetition,
                    u64::try_from(clusters.len() + draw)
                        .map_err(|error| methodology(format!("block draw overflow: {error}")))?,
                    cluster.len(),
                )?;
                for offset in 0..length {
                    let observation = cluster[(start + offset) % cluster.len()];
                    weighted += observation.return_bps * observation.weight;
                    total_weight += observation.weight;
                }
            }
            if total_weight <= Decimal::ZERO {
                return Err(methodology("economic bootstrap replicate has zero weight"));
            }
            replicates.push((weighted / total_weight).round_dp(8));
        }
        replicates.sort();
        let alpha = Decimal::ONE - request.policy.effect_confidence;
        let index = (alpha * Decimal::from(replicates.len()))
            .floor()
            .to_usize()
            .ok_or_else(|| methodology("economic bootstrap percentile overflow"))?
            .min(replicates.len().saturating_sub(1));
        replicates
            .get(index)
            .copied()
            .ok_or_else(|| methodology("economic bootstrap lower bound is unavailable"))
    }

    fn bootstrap_index(
        request: &RouteEconomicHealthRequest<'_>,
        repetition: u32,
        draw: u64,
        upper_bound: usize,
    ) -> QuantResult<usize> {
        let bound = u64::try_from(upper_bound)
            .map_err(|error| methodology(format!("bootstrap bound overflow: {error}")))?;
        let space = u128::from(u64::MAX) + 1;
        let acceptance_limit = space - space % u128::from(bound);
        for attempt in 0..u64::MAX {
            let hash = CanonicalDigest::content_hash_typed(
                "quant-pivot/route-economic-health-bootstrap-index",
                HASH_VERSION,
                &BootstrapWord {
                    route_identity_hash: request.route_identity_hash,
                    seed: request.policy.comparison_bootstrap_seed,
                    repetition,
                    draw,
                    attempt,
                },
            )?;
            let mut bytes = [0_u8; 8];
            bytes.copy_from_slice(&hash.as_bytes()[..8]);
            let value = u64::from_be_bytes(bytes);
            if u128::from(value) < acceptance_limit {
                return usize::try_from(value % bound)
                    .map_err(|error| methodology(format!("bootstrap index overflow: {error}")));
            }
        }
        Err(methodology(
            "economic bootstrap rejection sampling exhausted",
        ))
    }

    fn seal(
        request: &RouteEconomicHealthRequest<'_>,
        assessment: AssessmentWithoutHash<'_>,
    ) -> QuantResult<RouteEconomicHealthAssessment> {
        let policy_hash = request
            .policy
            .content_hash()
            .map_err(|error| methodology(format!("feedback policy hash failed: {error}")))?;
        let evidence_hash = CanonicalDigest::content_hash_typed(
            EVIDENCE_HASH_DOMAIN,
            HASH_VERSION,
            &AssessmentPreimage {
                route_identity_hash: request.route_identity_hash,
                assessed_through: request.assessed_through,
                policy_hash,
                observations: request.observations,
                assessment,
            },
        )?;
        Ok(RouteEconomicHealthAssessment {
            state: assessment.state,
            window_start: assessment.window_start,
            window_end: assessment.window_end,
            due_observation_count: assessment.due_observation_count,
            usable_observation_count: assessment.usable_observation_count,
            coverage: assessment.coverage,
            effective_sample_size: assessment.effective_sample_size,
            weighted_mean_return_bps: assessment.weighted_mean_return_bps,
            lower_confidence_return_bps: assessment.lower_confidence_return_bps,
            observation_hash: assessment.observation_hash,
            uniqueness_weight_hash: assessment.uniqueness_weight_hash,
            methodology_version: ROUTE_ECONOMIC_HEALTH_METHODOLOGY_VERSION.to_owned(),
            evidence_hash,
        })
    }
}

fn methodology(detail: impl Into<String>) -> QuantError {
    ResearchError::ValidationMethodology {
        detail: detail.into(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        enums::quant::RouteEconomicHealthState,
        types::{
            Bps, ContentHash, EventId, MarketId, RecommendationId, ResearchFeedbackPolicy,
            builtin_research_profiles,
        },
    };
    use rust_decimal_macros::dec;

    use super::{
        RouteEconomicHealthAssessment, RouteEconomicHealthEvaluator,
        RouteEconomicHealthObservation, RouteEconomicHealthRequest,
    };

    struct HealthFixture;

    impl HealthFixture {
        fn policy() -> ResearchFeedbackPolicy {
            let mut policy = builtin_research_profiles()
                .expect("builtin profiles")
                .into_iter()
                .next()
                .expect("pooled profile")
                .spec
                .feedback_policy;
            policy.comparison_minimum_observations = 4;
            policy.comparison_block_length = 2;
            policy.minimum_coverage = dec!(0.75);
            policy.minimum_effect_bps = Bps::new(dec!(25));
            policy
        }

        fn observations(returns: [Option<i64>; 4]) -> Vec<RouteEconomicHealthObservation> {
            let start = Utc.timestamp_opt(1_800_000_000, 0).single().expect("time");
            returns
                .into_iter()
                .enumerate()
                .map(|(index, return_bps)| RouteEconomicHealthObservation {
                    recommendation_id: RecommendationId::from_v7(),
                    market_id: MarketId::new(if index < 2 { "market-a" } else { "market-b" }),
                    event_id: EventId::new(if index < 2 { "event-a" } else { "event-b" }),
                    decision_at: start + Duration::minutes(i64::try_from(index).expect("index")),
                    terminal_at: start
                        + Duration::minutes(i64::try_from(index).expect("index"))
                        + Duration::minutes(10),
                    available_at: start + Duration::minutes(20),
                    net_return_bps: return_bps.map(|value| Bps::new(value.into())),
                })
                .collect()
        }

        fn evaluate(
            observations: &[RouteEconomicHealthObservation],
        ) -> RouteEconomicHealthAssessment {
            RouteEconomicHealthEvaluator::evaluate(&RouteEconomicHealthRequest {
                route_identity_hash: ContentHash::from_bytes([7; 32]),
                assessed_through: observations.last().map_or_else(
                    || Utc.timestamp_opt(1_800_000_000, 0).single().expect("time"),
                    |observation| observation.available_at,
                ),
                policy: &Self::policy(),
                observations,
            })
            .expect("economic health")
        }
    }

    #[test]
    fn classifies_health_states() {
        let mut healthy_observations =
            HealthFixture::observations([Some(100), Some(100), Some(100), Some(100)]);
        healthy_observations[1].terminal_at =
            healthy_observations[1].decision_at + Duration::minutes(1);
        let healthy = HealthFixture::evaluate(&healthy_observations);
        assert_eq!(healthy.state, RouteEconomicHealthState::Healthy);
        assert!(
            healthy
                .effective_sample_size
                .is_some_and(|size| size < dec!(4))
        );
        let degraded = HealthFixture::evaluate(&HealthFixture::observations([
            Some(0),
            Some(0),
            Some(0),
            Some(0),
        ]));
        assert_eq!(degraded.state, RouteEconomicHealthState::Degraded);
        let incomplete = HealthFixture::evaluate(&HealthFixture::observations([
            Some(100),
            Some(100),
            Some(100),
            None,
        ]));
        assert_eq!(incomplete.state, RouteEconomicHealthState::DataIncomplete);
        let short = HealthFixture::observations([Some(100), Some(100), Some(100), Some(100)]);
        let insufficient = HealthFixture::evaluate(&short[..3]);
        assert_eq!(
            insufficient.state,
            RouteEconomicHealthState::InsufficientEvidence
        );
    }

    #[test]
    fn bootstrap_is_deterministic() {
        let observations = HealthFixture::observations([Some(80), Some(120), Some(60), Some(140)]);
        let first = HealthFixture::evaluate(&observations);
        let second = HealthFixture::evaluate(&observations);
        assert_eq!(first, second);
        assert!(first.uniqueness_weight_hash.is_some());
        assert!(first.lower_confidence_return_bps.is_some());
    }
}
