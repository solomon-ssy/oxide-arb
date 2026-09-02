//! Immutable Route economic-health persistence contract.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_route_economic_health,
    enums::quant::RouteEconomicHealthState,
    hashing::CanonicalDigest,
    runtime_config::BuyModelRoute,
    types::{
        Bps, ContentHash, EventId, MarketId, ModelVersionId, RecommendationId,
        ResearchProfileArtifactId, RouteEconomicHealthId, TradePolicyArtifactId,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteEconomicHealthIdentity {
    pub route: BuyModelRoute,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub model_version_id: ModelVersionId,
    pub trade_policy_artifact_id: TradePolicyArtifactId,
}

impl RouteEconomicHealthIdentity {
    pub fn content_hash(&self) -> Result<ContentHash, String> {
        CanonicalDigest::content_hash_typed("quant-pivot/route-economic-health-identity", 1, self)
            .map_err(|error| error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RouteEconomicHealthEvidenceDocument {
    pub observation_hash: ContentHash,
    pub uniqueness_weight_hash: Option<ContentHash>,
    pub methodology_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteEconomicHealthSource {
    pub recommendation_id: RecommendationId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub decision_at: DateTime<Utc>,
    pub terminal_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub net_return_bps: Option<Bps>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "quant_route_economic_health::ActiveModel")]
pub struct NewRouteEconomicHealth {
    pub route_economic_health_id: RouteEconomicHealthId,
    pub route: BuyModelRoute,
    pub route_identity_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub feedback_policy_hash: ContentHash,
    pub state: RouteEconomicHealthState,
    pub window_start: Option<DateTime<Utc>>,
    pub assessed_through: DateTime<Utc>,
    pub due_observation_count: i64,
    pub usable_observation_count: i64,
    pub coverage: Decimal,
    pub effective_sample_size: Option<Decimal>,
    pub weighted_mean_return_bps: Option<Bps>,
    pub lower_confidence_return_bps: Option<Bps>,
    pub comparison_minimum_observations: i64,
    pub minimum_coverage: Decimal,
    pub minimum_effect_bps: Bps,
    pub confidence: Decimal,
    pub evidence_json: RouteEconomicHealthEvidenceDocument,
    pub evidence_hash: ContentHash,
    pub available_at: DateTime<Utc>,
}

impl NewRouteEconomicHealth {
    pub fn validate(&self) -> Result<(), &'static str> {
        let numeric_complete = self.effective_sample_size.is_some()
            && self.weighted_mean_return_bps.is_some()
            && self.lower_confidence_return_bps.is_some()
            && self.evidence_json.uniqueness_weight_hash.is_some();
        let numeric_absent = self.effective_sample_size.is_none()
            && self.weighted_mean_return_bps.is_none()
            && self.lower_confidence_return_bps.is_none()
            && self.evidence_json.uniqueness_weight_hash.is_none();
        if self.route_economic_health_id
            != RouteEconomicHealthId::from_content_hash(&self.evidence_hash)
            || self.due_observation_count < 0
            || self.usable_observation_count < 0
            || self.usable_observation_count > self.due_observation_count
            || self.coverage < Decimal::ZERO
            || self.coverage > Decimal::ONE
            || self.minimum_coverage <= Decimal::ZERO
            || self.minimum_coverage > Decimal::ONE
            || self.minimum_effect_bps <= Bps::ZERO
            || self.comparison_minimum_observations <= 0
            || self.confidence <= Decimal::ZERO
            || self.confidence > Decimal::ONE
            || self.assessed_through > self.available_at
            || self.evidence_json.methodology_version.trim().is_empty()
        {
            return Err(
                "Route economic-health identity, counts, thresholds, or timeline is invalid",
            );
        }
        let expected_coverage = if self.due_observation_count == 0 {
            Decimal::ZERO
        } else {
            (Decimal::from(self.usable_observation_count)
                / Decimal::from(self.due_observation_count))
            .round_dp(8)
        };
        if self.coverage != expected_coverage {
            return Err("Route economic-health coverage differs from durable counts");
        }
        match self.state {
            RouteEconomicHealthState::InsufficientEvidence => {
                if !numeric_absent
                    || self.due_observation_count >= self.comparison_minimum_observations
                {
                    return Err("insufficient Route health cannot carry numeric evidence");
                }
            }
            RouteEconomicHealthState::DataIncomplete => {
                if !numeric_absent
                    || (self.coverage >= self.minimum_coverage
                        && self.usable_observation_count >= self.comparison_minimum_observations)
                {
                    return Err("data-incomplete Route health has contradictory coverage");
                }
            }
            RouteEconomicHealthState::Healthy => {
                if !numeric_complete
                    || self.usable_observation_count < self.comparison_minimum_observations
                    || self
                        .lower_confidence_return_bps
                        .is_none_or(|bound| bound < self.minimum_effect_bps)
                {
                    return Err("healthy Route lacks threshold-qualified evidence");
                }
            }
            RouteEconomicHealthState::Degraded => {
                if !numeric_complete
                    || self.usable_observation_count < self.comparison_minimum_observations
                    || self
                        .lower_confidence_return_bps
                        .is_some_and(|bound| bound >= self.minimum_effect_bps)
                {
                    return Err("degraded Route has threshold-qualified evidence");
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel, JsonSchema)]
#[sea_orm(entity = "quant_route_economic_health::Entity")]
pub struct RouteEconomicHealthInfo {
    pub route_economic_health_id: RouteEconomicHealthId,
    pub route: BuyModelRoute,
    pub route_identity_hash: ContentHash,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub feedback_policy_hash: ContentHash,
    pub state: RouteEconomicHealthState,
    pub window_start: Option<DateTime<Utc>>,
    pub assessed_through: DateTime<Utc>,
    pub due_observation_count: i64,
    pub usable_observation_count: i64,
    pub coverage: Decimal,
    pub effective_sample_size: Option<Decimal>,
    pub weighted_mean_return_bps: Option<Bps>,
    pub lower_confidence_return_bps: Option<Bps>,
    pub comparison_minimum_observations: i64,
    pub minimum_coverage: Decimal,
    pub minimum_effect_bps: Bps,
    pub confidence: Decimal,
    pub evidence_json: RouteEconomicHealthEvidenceDocument,
    pub evidence_hash: ContentHash,
    pub available_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(RouteEconomicHealthInfo, quant_route_economic_health::Model, {
    route_economic_health_id, route, route_identity_hash, research_profile_artifact_id,
    feedback_policy_hash, state, window_start, assessed_through, due_observation_count,
    usable_observation_count, coverage, effective_sample_size, weighted_mean_return_bps,
    lower_confidence_return_bps, comparison_minimum_observations, minimum_coverage,
    minimum_effect_bps, confidence, evidence_json, evidence_hash, available_at, created_at,
});
