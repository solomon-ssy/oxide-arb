//! Frozen all-active-route selector contract shared by serving and replay.

use quant_pivot_error::{QuantResult, report::ReportError};
use quant_pivot_models::{
    hashing::CanonicalDigest,
    runtime_config::BuyModelRoute,
    types::{ContentHash, DecisionPolicySnapshotId, HistoryServingHeadSealId, ModelVersionId},
};
use quant_pivot_research::selection::{ModelFeatureRequirements, RouteAvailabilityContract};

use crate::service::model_serving_generation::ModelServingRouteSnapshot;

/// The immutable model identity and requiredness of one active selector Route.
#[derive(Clone)]
pub struct ReportUniverseRoute {
    route: BuyModelRoute,
    model_version_id: ModelVersionId,
    serving_contract_hash: ContentHash,
    model_spec_definition_hash: ContentHash,
    profile_hash: ContentHash,
    requirements: ModelFeatureRequirements,
}

impl From<&ModelServingRouteSnapshot> for ReportUniverseRoute {
    fn from(serving: &ModelServingRouteSnapshot) -> Self {
        let version = serving.active_version();
        Self {
            route: serving.route(),
            model_version_id: version.model_version_id,
            serving_contract_hash: version.serving_contract_hash,
            model_spec_definition_hash: version.model_spec_definition_hash,
            profile_hash: version.profile_ref.content_hash,
            requirements: serving.model_requirements(),
        }
    }
}

/// Complete selector authority, including active Routes with no selected market.
pub struct ReportUniverseContract {
    /// Exact all-active-route identity consumed by the market selector.
    pub availability: RouteAvailabilityContract,
    /// Merged requiredness from every active Route, not only selected Routes.
    pub requirements: ModelFeatureRequirements,
}

impl ReportUniverseContract {
    /// Freeze one policy generation and validated execution-history head into
    /// the canonical selector contract used by serving and durable producers.
    pub fn try_new(
        policy_id: DecisionPolicySnapshotId,
        snapshot_hash: ContentHash,
        mut routes: Vec<ReportUniverseRoute>,
        serving_head_seal_id: HistoryServingHeadSealId,
        serving_head_seal_hash: ContentHash,
    ) -> QuantResult<Self> {
        routes.sort_unstable_by_key(|route| route.route);
        let primary_route = BuyModelRoute::Pooled;
        let active_routes = routes.iter().map(|route| route.route).collect::<Vec<_>>();
        if !active_routes.contains(&primary_route) {
            return Err(ReportError::RouteReadiness {
                route: primary_route.as_str().to_owned(),
                detail: "the primary pooled route is not active in the pinned serving generation"
                    .to_owned(),
            }
            .into());
        }
        if active_routes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ReportError::InvariantViolation {
                stage: "report_universe",
                detail: "the complete active Route set contains duplicate Routes".to_owned(),
            }
            .into());
        }
        let route_lineage = routes
            .iter()
            .map(|route| {
                (
                    route.route,
                    route.model_version_id,
                    route.serving_contract_hash,
                    route.model_spec_definition_hash,
                    route.profile_hash,
                )
            })
            .collect::<Vec<_>>();
        let universe_plan_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/report-universe-plan",
            1,
            &(
                policy_id,
                snapshot_hash,
                primary_route,
                &active_routes,
                &route_lineage,
                serving_head_seal_id,
                serving_head_seal_hash,
            ),
        )?;
        let mut requirements = ModelFeatureRequirements::default();
        for route in routes {
            requirements.merge(route.requirements);
        }
        Ok(Self {
            availability: RouteAvailabilityContract {
                primary_route,
                active_routes,
                universe_plan_hash,
            },
            requirements,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Duration};
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::{
        domain::quant::{DomainAvailability, MarketCandidate, MarketDataHealth},
        enums::{common::MarketCategory, market::MarketStatus},
        hashing::CanonicalDigest,
        runtime_config::{BuyModelRoute, DataQualityConfig, FeaturesConfig, SelectionConfig},
        types::{
            ContentHash, DecisionPolicySnapshotId, EventId, HistoryServingHeadSealId, MarketId,
            ModelVersionId, Price, TokenId, Usd, stable_name::FeatureName,
        },
    };
    use quant_pivot_research::{
        hashing::ResearchHasher,
        selection::{
            ConfiguredMarketSelector, MarketSelectionBuildRequest, MarketSelector,
            ModelFeatureRequirements,
        },
    };
    use rust_decimal_macros::dec;

    use super::{ReportUniverseContract, ReportUniverseRoute};

    struct UniverseFixture {
        policy_id: DecisionPolicySnapshotId,
        snapshot_hash: ContentHash,
        head_id: HistoryServingHeadSealId,
        head_hash: ContentHash,
        routes: Vec<ReportUniverseRoute>,
    }

    impl UniverseFixture {
        fn new() -> QuantResult<Self> {
            let hash = ResearchHasher::canonical(&"universe fixture")?;
            Ok(Self {
                policy_id: DecisionPolicySnapshotId::from_v7(),
                snapshot_hash: hash,
                head_id: HistoryServingHeadSealId::from_v7(),
                head_hash: hash,
                routes: [BuyModelRoute::Pooled, BuyModelRoute::Weather]
                    .into_iter()
                    .map(|route| ReportUniverseRoute {
                        route,
                        model_version_id: ModelVersionId::from_v7(),
                        serving_contract_hash: hash,
                        model_spec_definition_hash: hash,
                        profile_hash: hash,
                        requirements: ModelFeatureRequirements {
                            generic: Vec::new(),
                            by_category: match route {
                                BuyModelRoute::Weather => BTreeMap::from([(
                                    MarketCategory::Weather,
                                    vec![FeatureName::new("book.spread_bps")],
                                )]),
                                _ => BTreeMap::new(),
                            },
                        },
                    })
                    .collect(),
            })
        }

        fn contract(
            &self,
            routes: Vec<ReportUniverseRoute>,
        ) -> QuantResult<ReportUniverseContract> {
            ReportUniverseContract::try_new(
                self.policy_id,
                self.snapshot_hash,
                routes,
                self.head_id,
                self.head_hash,
            )
        }

        fn selection(&self, contract: ReportUniverseContract) -> MarketSelectionBuildRequest {
            MarketSelectionBuildRequest {
                decision_at: DateTime::from_timestamp(1_800_000_000, 0).expect("fixture decision"),
                decision_policy_snapshot_id: self.policy_id,
                selection: SelectionConfig::default(),
                data_quality: DataQualityConfig::default(),
                features: FeaturesConfig::default(),
                model_requirements: contract.requirements,
                knowledge_lag_secs: 10,
                route_availability: Some(contract.availability),
            }
        }
    }

    #[test]
    fn preserves_hash_preimage() -> QuantResult<()> {
        let fixture = UniverseFixture::new()?;
        let contract = fixture.contract(fixture.routes.clone())?;
        let lineage = fixture
            .routes
            .iter()
            .map(|route| {
                (
                    route.route,
                    route.model_version_id,
                    route.serving_contract_hash,
                    route.model_spec_definition_hash,
                    route.profile_hash,
                )
            })
            .collect::<Vec<_>>();
        let expected = CanonicalDigest::content_hash_typed(
            "quant-pivot/report-universe-plan",
            1,
            &(
                fixture.policy_id,
                fixture.snapshot_hash,
                BuyModelRoute::Pooled,
                &[BuyModelRoute::Pooled, BuyModelRoute::Weather],
                &lineage,
                fixture.head_id,
                fixture.head_hash,
            ),
        )?;
        assert_eq!(contract.availability.universe_plan_hash, expected);
        let mut reordered = fixture.routes.clone();
        reordered.reverse();
        assert_eq!(
            fixture.contract(reordered)?.availability,
            contract.availability
        );
        Ok(())
    }

    #[test]
    fn binds_unrepresented_routes() -> QuantResult<()> {
        let fixture = UniverseFixture::new()?;
        let complete = fixture.contract(fixture.routes.clone())?;
        let represented = fixture.contract(vec![fixture.routes[0].clone()])?;
        assert_ne!(
            complete.availability.universe_plan_hash,
            represented.availability.universe_plan_hash,
        );
        assert!(
            complete
                .requirements
                .by_category
                .contains_key(&MarketCategory::Weather)
        );
        assert!(
            !represented
                .requirements
                .by_category
                .contains_key(&MarketCategory::Weather)
        );
        let mut changed = fixture.routes.clone();
        changed[1].serving_contract_hash =
            ResearchHasher::canonical(&"changed unrepresented route")?;
        assert_ne!(
            fixture.contract(changed)?.availability.universe_plan_hash,
            complete.availability.universe_plan_hash,
        );
        assert!(fixture.contract(vec![fixture.routes[1].clone()]).is_err());
        assert!(
            fixture
                .contract(vec![fixture.routes[0].clone(), fixture.routes[0].clone()])
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn selector_restores_full_contract() -> QuantResult<()> {
        let fixture = UniverseFixture::new()?;
        let online_request = fixture.selection(fixture.contract(fixture.routes.clone())?);
        let replay_request = fixture.selection(fixture.contract(fixture.routes.clone())?);
        let mut omitted_availability = replay_request.clone();
        omitted_availability.route_availability = None;
        let represented_only =
            fixture.selection(fixture.contract(vec![fixture.routes[0].clone()])?);
        let selector = ConfiguredMarketSelector::new();
        // An empty candidate world still commits all active Routes, including
        // the Weather Route that cannot appear in represented market membership.
        let online = selector.build_snapshot(online_request, Vec::new()).await?;
        let replay = selector.build_snapshot(replay_request, Vec::new()).await?;
        let omitted = selector
            .build_snapshot(omitted_availability, Vec::new())
            .await?;
        let narrowed = selector
            .build_snapshot(represented_only, Vec::new())
            .await?;
        assert_eq!(online.selector_hash, replay.selector_hash);
        assert_eq!(online.selector_evidence, replay.selector_evidence);
        assert_ne!(online.selector_hash, omitted.selector_hash);
        assert_ne!(
            online.selector_evidence.contract_hash,
            omitted.selector_evidence.contract_hash
        );
        assert_ne!(online.selector_hash, narrowed.selector_hash);
        assert_ne!(
            online.selector_evidence.model_requirements_hash,
            narrowed.selector_evidence.model_requirements_hash
        );
        Ok(())
    }

    #[tokio::test]
    async fn omitted_availability_changes_selection() -> QuantResult<()> {
        let fixture = UniverseFixture::new()?;
        let request = fixture.selection(fixture.contract(vec![fixture.routes[0].clone()])?);
        let candidate = MarketCandidate {
            market_id: MarketId::new("weather-unactivated"),
            event_id: EventId::new("weather-event"),
            category: MarketCategory::Weather,
            status: MarketStatus::Active,
            primary_token_id: TokenId::new("weather-yes"),
            secondary_token_id: Some(TokenId::new("weather-no")),
            end_date: Some(request.decision_at + Duration::days(7)),
            liquidity_usd: Some(Usd::new(dec!(10000))),
            volume_24h_usd: Some(Usd::new(dec!(5000))),
            best_bid: Some(Price::new(dec!(0.49))),
            best_ask: Some(Price::new(dec!(0.51))),
            depth_usd: Some(Usd::new(dec!(2000))),
            book_age_ms: Some(500),
            crossed: Some(false),
            empty: Some(false),
            market_data_health: MarketDataHealth::Healthy,
            ingest_lag_ms: Some(1000),
            domain_availability: DomainAvailability::NotMapped,
            decision_at: request.decision_at,
        };
        let mut omitted = request.clone();
        omitted.route_availability = None;
        let selector = ConfiguredMarketSelector::new();
        let online = selector
            .build_snapshot(request, vec![candidate.clone()])
            .await?;
        let replay = selector.build_snapshot(omitted, vec![candidate]).await?;
        assert_eq!(online.exclusion_summary.route_not_activated_count, 1);
        assert!(online.included.is_empty());
        assert_eq!(replay.exclusion_summary.route_not_activated_count, 0);
        assert_eq!(replay.included.len(), 1);
        assert_ne!(online.selector_hash, replay.selector_hash);
        Ok(())
    }
}
