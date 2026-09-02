//! `PostgreSQL` WORM Route economic-health assessments.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_ROUTE_ECONOMIC_HEALTH};
use quant_pivot_models::{
    domain::{
        pagination::{PageWindow, Paginated},
        quant::{NewRouteEconomicHealth, RouteEconomicHealthInfo, RouteEconomicHealthSource},
    },
    entities::{
        quant_recommendation::{Column as RecommendationColumn, Entity as RecommendationEntity},
        quant_recommendation_economic_outcome::{
            Column as OutcomeColumn, Entity as OutcomeEntity, Relation as OutcomeRelation,
        },
        quant_route_economic_health::{Column, Entity, Model},
    },
    runtime_config::BuyModelRoute,
    types::{Bps, ContentHash, RecommendationId, ResearchProfileArtifactId},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, JoinType, QueryFilter,
    QueryOrder, QuerySelect, RelationTrait,
};

use crate::{postgres::query::paginate_mapped, traits::RouteEconomicHealthRepository};

const MAX_SOURCE_ROWS: u64 = 1_000_000;

pub struct PgRouteEconomicHealthRepository {
    db: DatabaseConnection,
}

impl PgRouteEconomicHealthRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn exact_retry(stored: &Model, incoming: &NewRouteEconomicHealth) -> bool {
        stored.route_economic_health_id == incoming.route_economic_health_id
            && stored.route == incoming.route
            && stored.route_identity_hash == incoming.route_identity_hash
            && stored.research_profile_artifact_id == incoming.research_profile_artifact_id
            && stored.feedback_policy_hash == incoming.feedback_policy_hash
            && stored.state == incoming.state
            && stored.window_start == incoming.window_start
            && stored.assessed_through == incoming.assessed_through
            && stored.due_observation_count == incoming.due_observation_count
            && stored.usable_observation_count == incoming.usable_observation_count
            && stored.coverage == incoming.coverage
            && stored.effective_sample_size == incoming.effective_sample_size
            && stored.weighted_mean_return_bps == incoming.weighted_mean_return_bps
            && stored.lower_confidence_return_bps == incoming.lower_confidence_return_bps
            && stored.comparison_minimum_observations == incoming.comparison_minimum_observations
            && stored.minimum_coverage == incoming.minimum_coverage
            && stored.minimum_effect_bps == incoming.minimum_effect_bps
            && stored.confidence == incoming.confidence
            && stored.evidence_json == incoming.evidence_json
            && stored.evidence_hash == incoming.evidence_hash
            && stored.available_at == incoming.available_at
    }
}

#[async_trait::async_trait]
impl RouteEconomicHealthRepository for PgRouteEconomicHealthRepository {
    async fn insert(
        &self,
        health: NewRouteEconomicHealth,
    ) -> Result<RouteEconomicHealthInfo, StorageError> {
        health.validate().map_err(|detail| {
            StorageError::invariant_violation(Some(QUANT_ROUTE_ECONOMIC_HEALTH), detail)
        })?;
        if let Some(stored) = Entity::find_by_id(health.route_economic_health_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        {
            if Self::exact_retry(&stored, &health) {
                return Ok(stored.into());
            }
            return Err(StorageError::state_conflict(
                QUANT_ROUTE_ECONOMIC_HEALTH,
                Some(health.route_economic_health_id),
                "Route economic-health retry changed immutable content",
            ));
        }
        Entity::insert(health.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn latest(
        &self,
        route_identity_hash: &ContentHash,
        profile_id: &ResearchProfileArtifactId,
        available_through: DateTime<Utc>,
    ) -> Result<Option<RouteEconomicHealthInfo>, StorageError> {
        Entity::find()
            .filter(Column::RouteIdentityHash.eq(*route_identity_hash))
            .filter(Column::ResearchProfileArtifactId.eq(profile_id.clone()))
            .filter(Column::AvailableAt.lte(available_through))
            .order_by_desc(Column::AssessedThrough)
            .order_by_desc(Column::RouteEconomicHealthId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn latest_for_route(
        &self,
        route: &BuyModelRoute,
        available_through: DateTime<Utc>,
    ) -> Result<Option<RouteEconomicHealthInfo>, StorageError> {
        Entity::find()
            .filter(Column::Route.eq(*route))
            .filter(Column::AvailableAt.lte(available_through))
            .order_by_desc(Column::AvailableAt)
            .order_by_desc(Column::RouteEconomicHealthId)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page_for_route(
        &self,
        route: &BuyModelRoute,
        available_through: DateTime<Utc>,
        window: PageWindow,
    ) -> Result<Paginated<RouteEconomicHealthInfo>, StorageError> {
        paginate_mapped(
            Entity::find()
                .filter(Column::Route.eq(*route))
                .filter(Column::AvailableAt.lte(available_through))
                .order_by_desc(Column::AvailableAt)
                .order_by_desc(Column::RouteEconomicHealthId),
            &self.db,
            window,
            Into::into,
        )
        .await
    }

    async fn source_window(
        &self,
        route: &BuyModelRoute,
        profile_id: &ResearchProfileArtifactId,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        available_through: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<RouteEconomicHealthSource>, StorageError> {
        if window_start >= window_end
            || window_end > available_through
            || limit == 0
            || limit > MAX_SOURCE_ROWS
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_ROUTE_ECONOMIC_HEALTH),
                "Route economic-health source window or limit is invalid",
            ));
        }
        let outcomes = OutcomeEntity::find()
            .join(JoinType::InnerJoin, OutcomeRelation::Recommendation.def())
            .filter(RecommendationColumn::Route.eq(*route))
            .filter(OutcomeColumn::ResearchProfileArtifactId.eq(profile_id.clone()))
            .filter(OutcomeColumn::DecisionAt.gte(window_start))
            .filter(OutcomeColumn::DecisionAt.lt(window_end))
            .filter(OutcomeColumn::AvailableAt.lte(available_through))
            .order_by_asc(OutcomeColumn::DecisionAt)
            .order_by_asc(OutcomeColumn::RecommendationId)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        let ids = outcomes
            .iter()
            .map(|outcome| outcome.recommendation_id)
            .collect::<Vec<RecommendationId>>();
        let recommendations = RecommendationEntity::find()
            .filter(RecommendationColumn::RecommendationId.is_in(ids))
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(|recommendation| (recommendation.recommendation_id, recommendation))
            .collect::<HashMap<_, _>>();
        outcomes
            .into_iter()
            .map(|outcome| {
                let recommendation =
                    recommendations
                        .get(&outcome.recommendation_id)
                        .ok_or_else(|| {
                            StorageError::invariant_violation(
                                Some(QUANT_ROUTE_ECONOMIC_HEALTH),
                                "economic outcome lost its recommendation",
                            )
                        })?;
                Ok(RouteEconomicHealthSource {
                    recommendation_id: outcome.recommendation_id,
                    market_id: recommendation.market_id.clone(),
                    event_id: recommendation.event_id.clone(),
                    decision_at: outcome.decision_at,
                    terminal_at: outcome
                        .payload_json
                        .detail
                        .terminal_at()
                        .unwrap_or(outcome.horizon_at),
                    available_at: outcome.available_at,
                    net_return_bps: outcome.payload_json.amounts.net_return_bps.map(Bps::new),
                })
            })
            .collect()
    }
}
