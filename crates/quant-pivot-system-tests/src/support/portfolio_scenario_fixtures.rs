//! Mixed-Route scenario-model fixtures for the real production-stack closure.

use std::{collections::BTreeMap, slice, sync::Arc};

use anyhow::{Context, Error as AnyhowError, Result, bail, ensure};
use chrono::{DateTime, Duration, Utc};
use quant_pivot_core::app::ports::feedback_mutation::FeedbackCycleFreezePlan;
use quant_pivot_models::{
    domain::quant::{
        BacktestPathSetInfo, DiscountCurvePoint, ModelVersionInfo, NewBacktestPathSet,
        NewBacktestPathSetInput, NewModelRun, PortfolioScenarioEvidenceRegime,
        PortfolioScenarioFitEvidence, PortfolioScenarioKind, PortfolioScenarioModelArtifact,
        PortfolioScenarioModelState, PortfolioScenarioResamplingMethod,
        PortfolioScenarioRouteFactor, PortfolioScenarioRouteFitLineage,
        PortfolioScenarioRouteModelLineage, PortfolioScenarioVisibility, RepresentedRouteSet,
        RouteCompatibilityDigests, RouteContractHash, ScenarioDistribution, ScenarioWeight,
    },
    enums::{quant::ModelRunKind, runtime_config::ConfigResourceKind},
    hashing::CanonicalDigest,
    runtime_config::{
        BuyModelRoute, BuyRouteBinding, DecisionPolicySnapshot, ModelBinding, ModelBindingSource,
        PortfolioScenarioModelArtifactBinding,
    },
    types::{
        BacktestPathSetId, CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId,
        ModelRunId, ModelVersionId, PolicyBundleGeneration, PortfolioScenarioModelArtifactId,
        ResearchFeatureContract, ResearchProfileRef, SchemaVersion, ServingAuthority,
        TradePolicyCohortProvenance, TrainingDatasetId,
        backtest::{
            BacktestPath, CpcvEstimatorIdentity, CpcvFoldArtifact, CpcvFoldArtifacts,
            CpcvFoldCalibrationPolicy, CpcvFoldValidationRegime, CpcvMethodologyBinding,
            CpcvPathSetSubject, CpcvTrialPathBinding, CscvTrialGridBinding, SharpeDistribution,
        },
        model_lineage::ModelVersionDerivation,
    },
};
use quant_pivot_repository::{
    postgres::{
        PgBacktestPathSetRepository, PgModelRegistryRepository, PgModelRunRepository,
        PgPolicyRepository, PgTradePolicyRepository, PgTrainingDatasetRepository,
    },
    traits::{
        BacktestPathSetRepository, CpcvPathSetCommit, ModelRegistryRepository, ModelRunRepository,
        PolicyRepository, TradePolicyRepository, TrainingDatasetRepository,
    },
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    factors::names::DOMAIN_CRYPTO_STRIKE_PRESSURE,
    hashing::ResearchHasher,
    portfolio::{CapitalTimeBucketContract, PortfolioScenarioGenerator},
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;

use crate::postgres::PostgresClock;

use super::{
    execution_pg_seed::{
        CalibratedModelHead, CalibrationEvidencePreset, SeedModelVersionInput, SharedDemoInfra,
        fixture_profile_ref, seed_model_version_named,
    },
    model_spec_fixtures::crypto_profile_ref,
    policy_fixtures::{activate_policy_bundle, bootstrap_policy_bundle},
    research_fixtures::cscv_selection_fixture,
    trade_policy_fixtures::FixtureBookTiming,
};

const PIT_STATE_COUNT: u32 = 320;
const CALIBRATION_STATE_COUNT: u32 = 40;
const STRESS_STATE_COUNT: u32 = 40;
const SCENARIO_STATE_COUNT: u32 = PIT_STATE_COUNT + CALIBRATION_STATE_COUNT + STRESS_STATE_COUNT;
const EXPECTED_BLOCK_LENGTH: u32 = 8;
// Disposable E2E methodology: 20 buckets require 40 complete observations
// with block=8 and prove only the algorithm/governance flow. Operational
// Activation must bind a separately governed real scenario artifact; evidence
// produced by this fixture is never activation evidence.
const SCENARIO_HORIZON_BUCKETS: u32 = 20;
const PATH_HISTORY_DAYS: i64 = 180;

#[derive(serde::Serialize)]
struct BootstrapRecommendationContract<'a> {
    profile_ref: &'a ResearchProfileRef,
    feature_contract: ResearchFeatureContract,
    serving_authority: ServingAuthority,
}

struct FixtureRouteContract {
    route: BuyModelRoute,
    model_version_id: ModelVersionId,
    model_artifact_hash: ContentHash,
    serving_contract_hash: ContentHash,
    calibration_source_model_version_id: ModelVersionId,
    calibration_source_model_artifact_hash: ContentHash,
    calibration_source_serving_contract_hash: ContentHash,
    calibration_artifact_id: CalibrationArtifactId,
    calibration_contract_hash: ContentHash,
    recommendation_contract_hash: ContentHash,
    prediction_horizon_secs: i64,
}

impl FixtureRouteContract {
    fn from_version(
        route: BuyModelRoute,
        version: &ModelVersionInfo,
        calibration_source: &ModelVersionInfo,
    ) -> Result<Self> {
        ensure!(
            BuyModelRoute::try_from(version.category_scope)? == route,
            "model {} does not own {route:?}",
            version.model_version_id
        );
        let serving = version.verified_serving_contract()?;
        let bindings = serving.bindings();
        let calibration = bindings
            .model
            .calibration
            .as_ref()
            .context("scenario Route serving contract has no calibration")?;
        let profile = bindings
            .model
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(AnyhowError::msg)
            .context("resolve scenario Route ResearchProfile")?;
        let recommendation_contract_hash = match profile.spec.serving_authority {
            ServingAuthority::ExecutionEligible => {
                bindings
                    .trade_policy
                    .as_ref()
                    .context("execution scenario Route serving contract has no Trade Policy")?
                    .content_hash
            }
            ServingAuthority::AnalysisOnlyWithLiveL2 => {
                ensure!(
                    bindings.trade_policy.is_none(),
                    "AnalysisOnly scenario Route must not bind a Trade Policy"
                );
                CanonicalDigest::content_hash_typed(
                    "quant-pivot/bootstrap-recommendation-contract",
                    1,
                    &BootstrapRecommendationContract {
                        profile_ref: &profile.profile_ref,
                        feature_contract: profile.spec.feature_contract,
                        serving_authority: profile.spec.serving_authority,
                    },
                )?
            }
        };
        let ModelVersionDerivation::ReturnCalibration {
            parent_model_version_id,
            calibration_artifact_id,
        } = version.verified_derivation()?
        else {
            bail!("scenario Route serving model is not a calibrated child")
        };
        ensure!(
            parent_model_version_id == calibration_source.model_version_id
                && calibration_artifact_id == calibration.artifact_id
                && calibration_source.verified_derivation()? == ModelVersionDerivation::Training
                && version.model_version_id != calibration_source.model_version_id
                && version.artifact_hash != calibration_source.artifact_hash
                && version.serving_contract_hash != calibration_source.serving_contract_hash,
            "scenario Route calibration edge differs from its source or serving contract"
        );
        Ok(Self {
            route,
            model_version_id: version.model_version_id,
            model_artifact_hash: version.artifact_hash,
            serving_contract_hash: serving.contract_hash(),
            calibration_source_model_version_id: calibration_source.model_version_id,
            calibration_source_model_artifact_hash: calibration_source.artifact_hash,
            calibration_source_serving_contract_hash: calibration_source.serving_contract_hash,
            calibration_artifact_id: calibration.artifact_id,
            calibration_contract_hash: calibration.content_hash,
            recommendation_contract_hash,
            prediction_horizon_secs: version.model_spec_prediction_horizon_secs,
        })
    }
}

struct SeededScenarioGraph {
    bindings: Vec<PortfolioScenarioModelArtifactBinding>,
    content_hashes: BTreeMap<ContentHash, ContentHash>,
}

struct FeedbackRouteSets(Vec<RepresentedRouteSet>);

impl FeedbackRouteSets {
    fn build() -> Result<Self> {
        Ok(Self(vec![
            RepresentedRouteSet::from_routes([BuyModelRoute::Pooled])?,
            RepresentedRouteSet::from_routes([BuyModelRoute::Crypto])?,
            RepresentedRouteSet::from_routes([BuyModelRoute::Weather])?,
            RepresentedRouteSet::from_routes([BuyModelRoute::Pooled, BuyModelRoute::Crypto])?,
            RepresentedRouteSet::from_routes([BuyModelRoute::Pooled, BuyModelRoute::Weather])?,
            RepresentedRouteSet::from_routes([BuyModelRoute::Crypto, BuyModelRoute::Weather])?,
            RepresentedRouteSet::from_routes([
                BuyModelRoute::Pooled,
                BuyModelRoute::Crypto,
                BuyModelRoute::Weather,
            ])?,
        ]))
    }
}

impl SeededScenarioGraph {
    async fn route_versions(
        db: &DatabaseConnection,
        policy: &DecisionPolicySnapshot,
        crypto_model_version_id: ModelVersionId,
        weather_model_version_id: ModelVersionId,
    ) -> Result<BTreeMap<BuyModelRoute, ModelVersionInfo>> {
        let registry = PgModelRegistryRepository::new(db.clone());
        let pooled_model_version_id = policy
            .model_routing
            .model
            .route_binding(BuyModelRoute::Pooled)?
            .champion
            .model_version_id;
        let mut versions = BTreeMap::new();
        for (route, model_version_id) in [
            (BuyModelRoute::Pooled, pooled_model_version_id),
            (BuyModelRoute::Crypto, crypto_model_version_id),
            (BuyModelRoute::Weather, weather_model_version_id),
        ] {
            versions.insert(
                route,
                registry
                    .find_model_version(&model_version_id)
                    .await?
                    .with_context(|| format!("mixed-Route {route:?} champion row is missing"))?,
            );
        }
        Ok(versions)
    }

    async fn build(
        db: &DatabaseConnection,
        artifact_store: &Arc<dyn ArtifactStore>,
        policy: &DecisionPolicySnapshot,
        versions: &BTreeMap<BuyModelRoute, ModelVersionInfo>,
        route_sets: &[RepresentedRouteSet],
        replay_data_cutoff: DateTime<Utc>,
        evidence_clock: DateTime<Utc>,
    ) -> Result<Self> {
        let mut paths = BTreeMap::new();
        let mut routes = route_sets
            .iter()
            .flat_map(|represented| represented.routes.iter().copied())
            .collect::<Vec<_>>();
        routes.sort_unstable();
        routes.dedup();
        ensure!(
            !routes.is_empty(),
            "scenario fixture requires at least one Route"
        );
        for route in routes {
            let version = versions
                .get(&route)
                .with_context(|| format!("scenario model map lost {route:?}"))?;
            paths.insert(
                route,
                seed_path_set(db, route, version, replay_data_cutoff).await?,
            );
        }
        let mut bindings = Vec::with_capacity(route_sets.len());
        let mut content_hashes = BTreeMap::new();
        let registry = PgModelRegistryRepository::new(db.clone());
        for represented in route_sets {
            let mut contracts = Vec::with_capacity(represented.routes.len());
            for route in &represented.routes {
                let version = versions
                    .get(route)
                    .expect("represented Route model was checked above");
                let ModelVersionDerivation::ReturnCalibration {
                    parent_model_version_id,
                    ..
                } = version.verified_derivation()?
                else {
                    bail!("scenario Route serving model is not a calibrated child")
                };
                let source = registry
                    .find_model_version(&parent_model_version_id)
                    .await?
                    .context("scenario Route calibration source is missing")?;
                contracts.push(FixtureRouteContract::from_version(
                    *route, version, &source,
                )?);
            }
            let (model, binding) =
                build_model(policy, represented, &contracts, &paths, evidence_clock)?;
            persist_model(artifact_store, &model).await?;
            PortfolioScenarioGenerator::verify_model(
                &binding,
                &model,
                represented,
                evidence_clock,
                PortfolioScenarioVisibility::PointInTime,
            )?;
            ensure!(
                content_hashes
                    .insert(represented.digest, model.content_hash)
                    .is_none(),
                "scenario fixture received a duplicate Route set"
            );
            bindings.push(binding);
        }
        bindings.sort_by_key(|binding| {
            (
                binding.route_set_digest,
                binding.model_content_hash,
                binding.portfolio_scenario_model_artifact_id.as_uuid(),
            )
        });
        Ok(Self {
            bindings,
            content_hashes,
        })
    }

    fn content_hash(&self, represented: &RepresentedRouteSet) -> Result<ContentHash> {
        self.content_hashes
            .get(&represented.digest)
            .copied()
            .context("scenario fixture did not produce the requested Route set")
    }
}

/// Bootstrap a complete Weather benchmark and freeze its scenario methodology
/// before any challenger Dataset is materialized.
///
/// The returned snapshot is the only policy that challengers may bind. The
/// benchmark remains an independent calibrated Champion, while the scenario
/// graph is fitted solely from evidence that predates `replay_data_cutoff`.
pub async fn bootstrap_weather_evaluation_portfolio(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    mut snapshot: DecisionPolicySnapshot,
    replay_data_cutoff: DateTime<Utc>,
) -> Result<DecisionPolicySnapshotId> {
    let policies = PgPolicyRepository::new(db.clone());
    ensure!(
        policies.load_current_bundle().await?.is_none(),
        "Weather evaluation bootstrap requires a fresh policy database"
    );

    let champion_model_version_id = ModelVersionId::from_v7();
    let initial_bound_at = db.statement_time().await;
    snapshot.model_routing.model.buy_routes.insert(
        BuyModelRoute::Weather,
        BuyRouteBinding {
            champion: ModelBinding::new(
                champion_model_version_id,
                ModelBindingSource::Bootstrap,
                initial_bound_at,
                PolicyBundleGeneration::FIRST,
                1,
            ),
            shadow: None,
        },
    );
    let base_snapshot_id = bootstrap_policy_bundle(
        &policies,
        &snapshot,
        "weather-evaluation-bootstrap",
        "freeze the independent Weather Champion before challenger materialization",
    )
    .await;

    let seeded = Box::pin(seed_model_version_named(
        db,
        SeedModelVersionInput {
            decision_policy_snapshot_id: base_snapshot_id,
            book_timing: FixtureBookTiming::standard(),
            model_version_id: champion_model_version_id,
            model_name: "weather-evaluation-benchmark",
            profile_ref: fixture_profile_ref(),
            artifact_store: Some(artifact_store),
            head: CalibratedModelHead::Policy,
            calibration_preset: CalibrationEvidencePreset::Baseline,
        },
    ))
    .await;
    ensure!(
        seeded.model_version_id == champion_model_version_id,
        "Weather evaluation seed published an unexpected Champion identity"
    );

    let base = policies
        .load_current_bundle()
        .await?
        .context("Weather evaluation bootstrap produced no active policy")?;
    ensure!(
        base.decision_policy_snapshot_id == base_snapshot_id,
        "Weather evaluation bootstrap activated an unexpected base snapshot"
    );
    let champion = PgModelRegistryRepository::new(db.clone())
        .find_model_version(&champion_model_version_id)
        .await?
        .context("Weather evaluation Champion row is missing")?;
    let represented = RepresentedRouteSet::from_routes([BuyModelRoute::Weather])?;
    let evidence_clock = db.statement_time().await;
    let graph = SeededScenarioGraph::build(
        db,
        artifact_store,
        &base.snapshot,
        &BTreeMap::from([(BuyModelRoute::Weather, champion)]),
        slice::from_ref(&represented),
        replay_data_cutoff,
        evidence_clock,
    )
    .await?;
    let activation_generation = base
        .generation
        .checked_next()
        .context("Weather evaluation policy generation overflowed")?;
    let bindings = graph.bindings;
    let snapshot_id = activate_policy_bundle(
        &policies,
        ConfigResourceKind::ModelRouting,
        "weather-evaluation-scenario",
        "bind the independent Weather benchmark and its ex-ante scenario methodology",
        move |candidate| {
            candidate
                .model_routing
                .model
                .buy_routes
                .get_mut(&BuyModelRoute::Weather)
                .expect("Weather benchmark Route binding exists")
                .champion
                .config_revision = activation_generation;
            candidate
                .model_routing
                .model
                .portfolio_scenario_model_bindings = bindings;
        },
    )
    .await;
    let active = policies
        .load_current_bundle()
        .await?
        .context("Weather scenario activation produced no current policy")?;
    ensure!(
        active.decision_policy_snapshot_id == snapshot_id
            && active.generation == activation_generation,
        "Weather scenario activation did not publish the expected atomic bundle"
    );
    Ok(snapshot_id)
}

/// Fit and atomically activate a complete scenario-model graph for a report fixture.
pub async fn activate_report_portfolio(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    route_sets: impl IntoIterator<Item = RepresentedRouteSet>,
    visible_at: DateTime<Utc>,
) -> Result<DecisionPolicySnapshotId> {
    let policies = PgPolicyRepository::new(db.clone());
    let base = policies
        .load_current_bundle()
        .await?
        .context("report scenario fixture has no active policy bundle")?;
    let route_sets = route_sets.into_iter().collect::<Vec<_>>();
    ensure!(
        !route_sets.is_empty(),
        "report scenario fixture requires represented Route sets"
    );
    let mut represented_routes = route_sets
        .iter()
        .flat_map(|represented| represented.routes.iter().copied())
        .collect::<Vec<_>>();
    represented_routes.sort_unstable();
    represented_routes.dedup();
    let registry = PgModelRegistryRepository::new(db.clone());
    let mut versions = BTreeMap::new();
    for route in &represented_routes {
        let model_version_id = base
            .snapshot
            .model_routing
            .model
            .route_binding(*route)?
            .champion
            .model_version_id;
        let version = registry
            .find_model_version(&model_version_id)
            .await?
            .with_context(|| format!("report Route {route:?} champion is missing"))?;
        versions.insert(*route, version);
    }
    let evidence_clock = db.statement_time().await;
    ensure!(
        visible_at >= evidence_clock,
        "report scenario visibility precedes its fitted evidence clock"
    );
    let replay_data_cutoff = visible_at - Duration::days(1);
    let graph = SeededScenarioGraph::build(
        db,
        artifact_store,
        &base.snapshot,
        &versions,
        &route_sets,
        replay_data_cutoff,
        evidence_clock,
    )
    .await?;
    let activation_generation = base
        .generation
        .checked_next()
        .context("report scenario policy generation overflowed")?;
    let bindings = graph.bindings;
    Ok(activate_policy_bundle(
        &policies,
        ConfigResourceKind::ModelRouting,
        "report-scenario-portfolio-fixture",
        "activate the exact Route scenario graph before report generation",
        move |snapshot| {
            for route in &represented_routes {
                snapshot
                    .model_routing
                    .model
                    .buy_routes
                    .get_mut(route)
                    .expect("report Route binding exists")
                    .champion
                    .config_revision = activation_generation;
            }
            snapshot
                .model_routing
                .model
                .portfolio_scenario_model_bindings = bindings;
        },
    )
    .await)
}

/// Persist one calibrated Crypto Route model with complete Trade Policy lineage.
pub async fn seed_crypto_model(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    model_version_id: ModelVersionId,
    model_name: &str,
) -> Result<ModelVersionInfo> {
    Box::pin(seed_model_version_named(
        db,
        SeedModelVersionInput {
            decision_policy_snapshot_id,
            book_timing: FixtureBookTiming::standard(),
            model_version_id,
            model_name,
            profile_ref: crypto_profile_ref(),
            artifact_store: Some(artifact_store),
            head: CalibratedModelHead::Policy,
            calibration_preset: CalibrationEvidencePreset::Baseline,
        },
    ))
    .await;
    PgModelRegistryRepository::new(db.clone())
        .find_model_version(&model_version_id)
        .await?
        .with_context(|| format!("calibrated Crypto model {model_version_id} is missing"))
}

struct FixtureFoldEvidence {
    route: BuyModelRoute,
    model_version_id: ModelVersionId,
    calibration_artifact_id: CalibrationArtifactId,
    calibration_hash: ContentHash,
    parent_model_version_id: ModelVersionId,
    parent_artifact_hash: ContentHash,
    parent_serving_contract_hash: ContentHash,
}

impl FixtureFoldEvidence {
    fn hash(&self, label: &str) -> Result<ContentHash> {
        Ok(ResearchHasher::canonical(&(
            "feedback-closure-route-fold-v1",
            self.route,
            self.model_version_id,
            label,
        ))?)
    }

    fn methodology(&self, trial_grid: CscvTrialGridBinding) -> Result<CpcvMethodologyBinding> {
        Ok(CpcvMethodologyBinding::new(
            self.hash("config")?,
            self.hash("portfolio-caps")?,
            self.hash("replay")?,
            CpcvFoldCalibrationPolicy::CalibratedSubjectParentHeuristic {
                calibration_artifact_id: self.calibration_artifact_id,
                calibration_hash: self.calibration_hash,
                parent_model_version_id: self.parent_model_version_id,
                parent_artifact_hash: self.parent_artifact_hash,
                parent_serving_contract_hash: self.parent_serving_contract_hash,
                parent_return_model_hash: self.hash("parent-return-model")?,
            },
            CpcvTrialPathBinding::try_new(0, vec![0])?,
            trial_grid,
        ))
    }

    fn artifacts(&self) -> Result<CpcvFoldArtifacts> {
        Ok(CpcvFoldArtifacts::try_new(vec![
            CpcvFoldArtifact {
                validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
                identity: CpcvEstimatorIdentity::Validation {
                    combination_index: 0,
                    test_partitions_hash: self.hash("test-partitions")?,
                    test_partition_count: 1,
                    test_groups_hash: self.hash("test-groups")?,
                    test_group_count: 1,
                },
                training_groups_hash: self.hash("training-groups")?,
                training_group_count: 179,
                calibration_fit_groups_hash: self.hash("calibration-fit-groups")?,
                calibration_fit_group_count: 10,
                scenario_fit_groups_hash: self.hash("scenario-fit-groups")?,
                scenario_fit_group_count: 10,
                model_artifact_hash: self.hash("fold-model")?,
                serving_contract_hash: self.hash("fold-serving")?,
                model_payload_hash: self.hash("fold-payload")?,
                calibration_function_hash: self.hash("fold-calibration-function")?,
                scenario_economic_function_hash: self.hash("fold-scenario-function")?,
                calibration_artifact_hash: self.hash("fold-calibration")?,
                scenario_model_hash: self.hash("fold-scenario")?,
            },
            CpcvFoldArtifact {
                validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
                identity: CpcvEstimatorIdentity::TrialPathValidation {
                    trial_id: 0,
                    path_index: 0,
                    combination_index: 0,
                    test_partitions_hash: self.hash("test-partitions")?,
                    test_partition_count: 1,
                    test_groups_hash: self.hash("test-groups")?,
                    test_group_count: 1,
                },
                training_groups_hash: self.hash("trial-groups")?,
                training_group_count: 180,
                calibration_fit_groups_hash: self.hash("trial-calibration-fit-groups")?,
                calibration_fit_group_count: 10,
                scenario_fit_groups_hash: self.hash("trial-scenario-fit-groups")?,
                scenario_fit_group_count: 10,
                model_artifact_hash: self.hash("trial-model")?,
                serving_contract_hash: self.hash("trial-serving")?,
                model_payload_hash: self.hash("trial-payload")?,
                calibration_function_hash: self.hash("trial-calibration-function")?,
                scenario_economic_function_hash: self.hash("trial-scenario-function")?,
                calibration_artifact_hash: self.hash("trial-calibration")?,
                scenario_model_hash: self.hash("trial-scenario")?,
            },
            CpcvFoldArtifact {
                validation_regime: CpcvFoldValidationRegime::PortfolioEconomics,
                identity: CpcvEstimatorIdentity::TrialPathValidation {
                    trial_id: 1,
                    path_index: 0,
                    combination_index: 0,
                    test_partitions_hash: self.hash("test-partitions")?,
                    test_partition_count: 1,
                    test_groups_hash: self.hash("test-groups")?,
                    test_group_count: 1,
                },
                training_groups_hash: self.hash("challenger-trial-groups")?,
                training_group_count: 180,
                calibration_fit_groups_hash: self.hash("challenger-calibration-fit-groups")?,
                calibration_fit_group_count: 10,
                scenario_fit_groups_hash: self.hash("challenger-scenario-fit-groups")?,
                scenario_fit_group_count: 10,
                model_artifact_hash: self.hash("challenger-trial-model")?,
                serving_contract_hash: self.hash("challenger-trial-serving")?,
                model_payload_hash: self.hash("challenger-trial-payload")?,
                calibration_function_hash: self.hash("challenger-calibration-function")?,
                scenario_economic_function_hash: self.hash("challenger-scenario-function")?,
                calibration_artifact_hash: self.hash("challenger-trial-calibration")?,
                scenario_model_hash: self.hash("challenger-trial-scenario")?,
            },
        ])?)
    }
}

async fn seed_scenario_validation_run(
    db: &DatabaseConnection,
    model_version_id: ModelVersionId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    scenario_model_content_hash: ContentHash,
) -> Result<ModelRunId> {
    let model_run_id = ModelRunId::from_v7();
    let window_end = db.statement_time().await;
    let window_start = window_end - Duration::milliseconds(1);
    let input_hash = ResearchHasher::canonical(&(
        "feedback-closure-post-scenario-validation-v1",
        model_version_id,
        decision_policy_snapshot_id,
        scenario_model_content_hash,
    ))?;
    let runs = PgModelRunRepository::new(db.clone());
    runs.create(NewModelRun {
        model_run_id,
        run_kind: ModelRunKind::Backtest,
        model_version_id: Some(model_version_id),
        decision_policy_snapshot_id,
        market_selection_id: None,
        window_start,
        window_end,
        input_hash,
    })
    .await?;
    runs.succeed(&model_run_id, scenario_model_content_hash, None)
        .await?;
    Ok(model_run_id)
}

impl FixtureBookTiming {
    async fn verify_published(
        self,
        db: &DatabaseConnection,
        provenance: &TradePolicyCohortProvenance,
    ) -> Result<()> {
        let policy = PgTradePolicyRepository::new(db.clone())
            .find(&provenance.artifact_id)
            .await?
            .context("mixed-Route Weather trade policy is absent")?;
        let cohort = policy
            .payload_json
            .cohorts
            .get(usize::try_from(provenance.cohort_index)?)
            .context("mixed-Route Weather policy cohort is absent")?;
        ensure!(
            policy.content_hash == provenance.artifact_hash
                && cohort.key == provenance.cohort_key
                && cohort.max_book_age_ms == self.max_book_age_ms,
            "Crypto/Weather fixture book timing must be identical before model sealing"
        );
        Ok(())
    }
}

/// Install the Crypto champion and complete scenario graph with the same
/// fixture book timing as the already-published Weather policy.
///
/// Single-Route artifacts govern route-local backtests; the three-Route
/// artifact governs production reports and is refitted on Weather promotion.
pub async fn finalize_feedback_portfolio(
    db: &DatabaseConnection,
    artifact_store: &Arc<dyn ArtifactStore>,
    infra: SharedDemoInfra,
    weather_champion_model_version_id: ModelVersionId,
    evaluation_dataset_id: TrainingDatasetId,
    book_timing: FixtureBookTiming,
    calibration_preset: CalibrationEvidencePreset,
) -> Result<SharedDemoInfra> {
    let policies = PgPolicyRepository::new(db.clone());
    let base = policies
        .load_current_bundle()
        .await?
        .context("mixed-Route fixture has no active policy bundle")?;
    let weather_binding = base
        .snapshot
        .model_routing
        .model
        .route_binding(BuyModelRoute::Weather)?;
    ensure!(
        weather_binding.champion.model_version_id == weather_champion_model_version_id,
        "mixed-Route fixture Weather champion differs from the governed research model"
    );
    book_timing
        .verify_published(db, &infra.trade_policy)
        .await?;

    let crypto = Box::pin(seed_model_version_named(
        db,
        SeedModelVersionInput {
            decision_policy_snapshot_id: base.decision_policy_snapshot_id,
            book_timing,
            model_version_id: ModelVersionId::from_v7(),
            model_name: "feedback-closure-crypto-champion",
            profile_ref: crypto_profile_ref(),
            artifact_store: Some(artifact_store),
            head: CalibratedModelHead::alpha_simplex([(
                DOMAIN_CRYPTO_STRIKE_PRESSURE,
                Decimal::ONE,
            )])
            .expect("Crypto scenario fixture head"),
            calibration_preset,
        },
    ))
    .await;
    let versions = SeededScenarioGraph::route_versions(
        db,
        &base.snapshot,
        crypto.model_version_id,
        weather_champion_model_version_id,
    )
    .await?;
    let weather = versions
        .get(&BuyModelRoute::Weather)
        .context("mixed-Route version map lost Weather")?;
    let evaluation = PgTrainingDatasetRepository::new(db.clone())
        .find_by_id(&evaluation_dataset_id)
        .await?
        .context("mixed-Route fixture evaluation Dataset is missing")?;
    let profile = fixture_profile_ref()
        .resolve_builtin_research_profile()
        .map_err(AnyhowError::msg)?;
    let feedback_plan = FeedbackCycleFreezePlan::derive(
        &profile,
        weather.model_spec_id,
        weather.model_spec_definition_hash,
        base.decision_policy_snapshot_id,
        base.snapshot_hash,
        db.statement_time().await,
    )?;
    let replay_data_cutoff = evaluation
        .window_start
        .min(feedback_plan.evaluation().window_start());
    let evidence_clock = db.statement_time().await;
    let route_sets = FeedbackRouteSets::build()?;
    let scenario_graph = SeededScenarioGraph::build(
        db,
        artifact_store,
        &base.snapshot,
        &versions,
        &route_sets.0,
        replay_data_cutoff,
        evidence_clock,
    )
    .await?;
    let mixed_routes =
        RepresentedRouteSet::from_routes([BuyModelRoute::Crypto, BuyModelRoute::Weather])?;
    let mixed_model_content_hash = scenario_graph.content_hash(&mixed_routes)?;

    let activation_generation = base
        .generation
        .checked_next()
        .context("mixed-Route policy generation overflowed")?;
    let crypto_model_version_id = crypto.model_version_id;
    let bindings_for_activation = scenario_graph.bindings;
    let snapshot_id = activate_policy_bundle(
        &policies,
        ConfigResourceKind::ModelRouting,
        "feedback-closure-portfolio-fixture",
        "activate exact Pooled/Crypto/Weather scenario graph before the production closure",
        move |snapshot| {
            let weather = snapshot
                .model_routing
                .model
                .buy_routes
                .get_mut(&BuyModelRoute::Weather)
                .expect("Weather Route binding exists");
            weather.champion.config_revision = activation_generation;
            snapshot.model_routing.model.buy_routes.insert(
                BuyModelRoute::Crypto,
                BuyRouteBinding {
                    champion: ModelBinding::new(
                        crypto_model_version_id,
                        ModelBindingSource::Bootstrap,
                        evidence_clock,
                        activation_generation,
                        1,
                    ),
                    shadow: None,
                },
            );
            snapshot
                .model_routing
                .model
                .portfolio_scenario_model_bindings = bindings_for_activation;
        },
    )
    .await;
    let active = policies
        .load_current_bundle()
        .await?
        .context("mixed-Route activation produced no current policy")?;
    ensure!(
        active.decision_policy_snapshot_id == snapshot_id
            && active.generation == activation_generation,
        "mixed-Route activation did not publish the expected atomic bundle"
    );

    let model_run_id = seed_scenario_validation_run(
        db,
        weather_champion_model_version_id,
        snapshot_id,
        mixed_model_content_hash,
    )
    .await?;

    Ok(SharedDemoInfra {
        feature_parity_state_id: infra.feature_parity_state_id,
        decision_policy_snapshot_id: snapshot_id,
        model_version_id: infra.model_version_id,
        calibration_artifact_id: infra.calibration_artifact_id,
        model_run_id,
        trade_policy: infra.trade_policy,
        factor_serving_plane: infra.factor_serving_plane,
    })
}

async fn seed_path_set(
    db: &DatabaseConnection,
    route: BuyModelRoute,
    model: &ModelVersionInfo,
    replay_data_cutoff: DateTime<Utc>,
) -> Result<BacktestPathSetInfo> {
    let bindings = model.verified_serving_contract()?.bindings();
    let training_dataset_id = model
        .training_dataset_id
        .context("scenario Route model has no training Dataset")?;
    let ModelVersionDerivation::ReturnCalibration {
        parent_model_version_id,
        calibration_artifact_id,
    } = model.verified_derivation()?
    else {
        bail!("scenario Route model is not a calibrated child")
    };
    let calibration = bindings
        .model
        .calibration
        .as_ref()
        .context("scenario Route model has no calibration binding")?;
    ensure!(
        calibration.artifact_id == calibration_artifact_id,
        "scenario Route calibration derivation differs from serving"
    );
    let parent = PgModelRegistryRepository::new(db.clone())
        .find_model_version(&parent_model_version_id)
        .await?
        .context("scenario Route calibration parent is missing")?;
    let fold_evidence = FixtureFoldEvidence {
        route,
        model_version_id: model.model_version_id,
        calibration_artifact_id,
        calibration_hash: calibration.content_hash,
        parent_model_version_id,
        parent_artifact_hash: parent.artifact_hash,
        parent_serving_contract_hash: parent.serving_contract_hash,
    };
    let day_epoch = replay_data_cutoff.timestamp().div_euclid(86_400) * 86_400;
    let day = DateTime::from_timestamp(day_epoch, 0).context("scenario day clock overflowed")?;
    let window_end = day - Duration::days(2);
    let window_start = window_end - Duration::days(PATH_HISTORY_DAYS);
    let path_set_id = BacktestPathSetId::from_v7();
    let model_run_id = ModelRunId::from_v7();
    let input_hash = ResearchHasher::canonical(&(
        "feedback-closure-route-cpcv-v1",
        route,
        model.model_version_id,
        path_set_id,
        window_start,
        window_end,
    ))?;
    PgModelRunRepository::new(db.clone())
        .start_exact(NewModelRun {
            model_run_id,
            run_kind: ModelRunKind::Cpcv,
            model_version_id: Some(model.model_version_id),
            decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
            market_selection_id: None,
            window_start,
            window_end,
            input_hash,
        })
        .await?;
    let decision_times = (0..PATH_HISTORY_DAYS)
        .map(|offset| window_start + Duration::days(offset) + Duration::hours(1))
        .collect::<Vec<_>>();
    let group_returns = (0..PATH_HISTORY_DAYS)
        .map(|ordinal| route_return(route, ordinal))
        .collect::<Vec<_>>();
    let challenger_returns = group_returns
        .iter()
        .map(|value| *value - dec!(0.001))
        .collect::<Vec<_>>();
    let (trial_grid, cscv_selection_evidence) = cscv_selection_fixture(
        &format!("scenario-{route:?}"),
        &decision_times,
        &[group_returns.clone(), challenger_returns],
        4,
    );
    let dsr_conservative_independent_trial_count = i64::from(
        cscv_selection_evidence
            .trial_dependence
            .conservative_independent_trial_count(),
    );
    let scenario_residuals = (0..PATH_HISTORY_DAYS)
        .map(|ordinal| Some(route_payout_residual(route, ordinal)))
        .collect::<Vec<_>>();
    let path_set = NewBacktestPathSet::try_seal(NewBacktestPathSetInput {
        path_set_id,
        model_version_id: model.model_version_id,
        model_run_id,
        training_dataset_id,
        decision_policy_snapshot_id: bindings.policy_snapshot.decision_policy_snapshot_id,
        window_start,
        window_end,
        subject: CpcvPathSetSubject::new(
            model.artifact_hash,
            model.serving_contract_hash,
            bindings.transform.training_dataset_hash,
            bindings.dataset.manifest_hash,
            bindings.dataset.artifact_bytes_hash,
            bindings.policy_snapshot.snapshot_hash,
        ),
        methodology: fold_evidence.methodology(trial_grid)?,
        fold_artifacts: fold_evidence.artifacts()?,
        path_count: 1,
        combination_count: 1,
        median_target_rank_ic: dec!(0.12),
        sharpe_distribution: SharpeDistribution {
            min: dec!(0.45),
            p25: dec!(0.55),
            median: dec!(0.65),
            p75: dec!(0.75),
            max: dec!(0.85),
            median_max_drawdown: Some(dec!(0.08)),
            median_tail_loss: Some(dec!(-0.025)),
            median_turnover: Some(dec!(0.2)),
            baseline_uplift: Some(dec!(0.04)),
        },
        paths: vec![BacktestPath {
            path_index: 0,
            decision_times,
            risk_group_returns: group_returns.clone(),
            group_returns,
            scenario_residuals,
            sharpe: dec!(0.65),
            target_rank_ic: dec!(0.12),
            max_drawdown: dec!(0.08),
            tail_loss: dec!(-0.025),
            turnover: Some(dec!(0.2)),
        }]
        .into(),
        deflated_sharpe: dec!(0.92),
        dsr_benchmark_sharpe: dec!(0.2),
        pbo: cscv_selection_evidence.pbo,
        cscv_selection_evidence,
        min_track_record_length_secs: Some(90 * 86_400),
        dsr_conservative_independent_trial_count,
        trial_grid_count: 2,
        coord_search_effective_n: 1,
    })?;
    Ok(PgBacktestPathSetRepository::new(db.clone())
        .commit_cpcv(CpcvPathSetCommit {
            path_set,
            input_hash,
        })
        .await?)
}

const fn route_return(route: BuyModelRoute, ordinal: i64) -> Decimal {
    let cycle = ordinal.rem_euclid(12);
    match route {
        BuyModelRoute::Crypto => match cycle {
            0 | 4 | 9 => dec!(0.018),
            2 | 7 => dec!(0.011),
            1 | 6 | 10 => dec!(-0.009),
            _ => dec!(0.004),
        },
        BuyModelRoute::Weather => match cycle {
            0 | 5 | 8 => dec!(0.014),
            2 | 10 => dec!(0.009),
            1 | 6 | 11 => dec!(-0.007),
            _ => dec!(0.003),
        },
        BuyModelRoute::Pooled => match cycle {
            0 | 5 | 9 => dec!(0.012),
            2 | 7 => dec!(0.008),
            1 | 6 | 10 => dec!(-0.006),
            _ => dec!(0.003),
        },
    }
}

const fn route_payout_residual(route: BuyModelRoute, ordinal: i64) -> Decimal {
    let cycle = ordinal.rem_euclid(12);
    match route {
        BuyModelRoute::Crypto => match cycle {
            0 | 4 | 9 => dec!(0.12),
            2 | 7 => dec!(0.06),
            1 | 6 | 10 => dec!(-0.09),
            _ => dec!(0.02),
        },
        BuyModelRoute::Weather => match cycle {
            0 | 5 | 8 => dec!(0.10),
            2 | 10 => dec!(0.05),
            1 | 6 | 11 => dec!(-0.07),
            _ => dec!(0.015),
        },
        BuyModelRoute::Pooled => match cycle {
            0 | 5 | 9 => dec!(0.08),
            2 | 7 => dec!(0.04),
            1 | 6 | 10 => dec!(-0.06),
            _ => dec!(0.01),
        },
    }
}

fn build_model(
    policy: &DecisionPolicySnapshot,
    represented: &RepresentedRouteSet,
    contracts: &[FixtureRouteContract],
    paths: &BTreeMap<BuyModelRoute, BacktestPathSetInfo>,
    bound_at: DateTime<Utc>,
) -> Result<(
    PortfolioScenarioModelArtifact,
    PortfolioScenarioModelArtifactBinding,
)> {
    let compatibility = RouteCompatibilityDigests::try_new(
        represented,
        &route_hashes(contracts, |contract| contract.serving_contract_hash),
        &route_hashes(contracts, |contract| contract.calibration_contract_hash),
        &route_hashes(contracts, |contract| contract.recommendation_contract_hash),
    )?;
    let fit_window_start = represented
        .routes
        .iter()
        .map(|route| {
            paths
                .get(route)
                .expect("represented Route path was seeded before scenario fitting")
                .window_start
        })
        .max()
        .context("mixed-Route scenario fixture has no fit window")?;
    let as_of = represented
        .routes
        .iter()
        .map(|route| {
            paths
                .get(route)
                .expect("represented Route path was seeded before scenario fitting")
                .window_end
        })
        .min()
        .context("mixed-Route scenario fixture has no as-of boundary")?;
    let time_bucket_secs = contracts
        .iter()
        .map(|contract| u64::try_from(contract.prediction_horizon_secs))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .max()
        .context("mixed-Route scenario fixture has no prediction horizon")?;
    let resampling_method = PortfolioScenarioResamplingMethod::StationaryBootstrap {
        expected_block_length: EXPECTED_BLOCK_LENGTH,
        scenario_horizon_buckets: SCENARIO_HORIZON_BUCKETS,
    };
    let capital_time_bucket_contract_digest = CapitalTimeBucketContract::try_from(
        policy
            .execution_risk
            .portfolio
            .tail_risk
            .capital_time_buckets
            .as_slice(),
    )?
    .content_hash()?;
    let pit_residual_panel_hash = ResearchHasher::canonical(&(
        "feedback-closure-initial-joint-panel-v1",
        represented
            .routes
            .iter()
            .map(|route| {
                (
                    *route,
                    paths
                        .get(route)
                        .expect("represented Route path was seeded before scenario fitting")
                        .path_set_hash,
                )
            })
            .collect::<Vec<_>>(),
    ))?;
    let calibration_uncertainty_model_hash = ResearchHasher::canonical(&(
        "feedback-closure-initial-calibration-model-v1",
        contracts
            .iter()
            .map(|contract| {
                (
                    contract.route,
                    contract.calibration_artifact_id,
                    contract.calibration_contract_hash,
                )
            })
            .collect::<Vec<_>>(),
    ))?;
    let states = scenario_states(represented, contracts)?;
    let stress_catalog_hash =
        ResearchHasher::canonical(&("feedback-closure-stress-catalog-v1", &states))?;
    let route_fit_lineage = route_fit_lineage(contracts, paths, fit_window_start, as_of);
    let mut artifact = PortfolioScenarioModelArtifact {
        portfolio_scenario_model_artifact_id: PortfolioScenarioModelArtifactId::from_content_hash(
            &pit_residual_panel_hash,
        ),
        schema_version: SchemaVersion::FIRST,
        as_of,
        fit_window_start,
        time_bucket_secs,
        ordered_routes: represented.routes.clone(),
        route_set_digest: represented.digest,
        serving_contract_digest: compatibility.serving_contract_digest,
        calibration_contract_digest: compatibility.calibration_contract_digest,
        recommendation_contract_digest: compatibility.recommendation_contract_digest,
        evidence_regime: PortfolioScenarioEvidenceRegime::FullL2ExecutionEconomics,
        capital_time_bucket_contract_digest,
        scenario_random_stream_hash: ResearchHasher::canonical(&(
            "feedback-closure-scenario-random-stream-v1",
            represented.digest,
            fit_window_start,
            as_of,
        ))?,
        pit_residual_panel_hash,
        calibration_uncertainty_model_hash,
        stress_catalog_hash,
        resampling_method,
        route_fit_lineage,
        states,
        distributions: scenario_distributions(),
        discount_curve: policy
            .execution_risk
            .portfolio
            .tail_risk
            .capital_time_buckets
            .iter()
            .map(|bucket| DiscountCurvePoint {
                end_secs: bucket.end_secs,
                annualized_cost_bps: 500,
            })
            .collect(),
        content_hash: pit_residual_panel_hash,
    };
    artifact.content_hash = artifact.recomputed_hash()?;
    artifact.portfolio_scenario_model_artifact_id =
        PortfolioScenarioModelArtifactId::from_content_hash(&artifact.content_hash);
    let binding = PortfolioScenarioModelArtifactBinding {
        portfolio_scenario_model_artifact_id: artifact.portfolio_scenario_model_artifact_id,
        ordered_routes: artifact.ordered_routes.clone(),
        route_set_digest: artifact.route_set_digest,
        serving_contract_digest: artifact.serving_contract_digest,
        calibration_contract_digest: artifact.calibration_contract_digest,
        recommendation_contract_digest: artifact.recommendation_contract_digest,
        scenario_model_schema_version: artifact.schema_version,
        capital_time_bucket_contract_digest: artifact.capital_time_bucket_contract_digest,
        model_content_hash: artifact.content_hash,
        bound_at,
    };
    Ok((artifact, binding))
}

fn route_fit_lineage(
    contracts: &[FixtureRouteContract],
    paths: &BTreeMap<BuyModelRoute, BacktestPathSetInfo>,
    fit_window_start: DateTime<Utc>,
    fit_window_end: DateTime<Utc>,
) -> Vec<PortfolioScenarioRouteFitLineage> {
    contracts
        .iter()
        .map(|contract| {
            let path = paths
                .get(&contract.route)
                .expect("Route path map covers represented contracts");
            PortfolioScenarioRouteFitLineage {
                route: contract.route,
                model_lineage: PortfolioScenarioRouteModelLineage {
                    evaluated_model_version_id: contract.model_version_id,
                    evaluated_model_artifact_hash: contract.model_artifact_hash,
                    evaluated_serving_contract_hash: contract.serving_contract_hash,
                    calibration_source_model_version_id: contract
                        .calibration_source_model_version_id,
                    calibration_source_model_artifact_hash: contract
                        .calibration_source_model_artifact_hash,
                    calibration_source_serving_contract_hash: contract
                        .calibration_source_serving_contract_hash,
                },
                fit_evidence: PortfolioScenarioFitEvidence::CpcvPath {
                    backtest_path_set_id: path.path_set_id,
                    backtest_path_set_hash: path.path_set_hash,
                    representative_path_index: 0,
                },
                calibration_artifact_id: contract.calibration_artifact_id,
                calibration_artifact_hash: contract.calibration_contract_hash,
                recommendation_contract_hash: contract.recommendation_contract_hash,
                fit_window_start,
                fit_window_end,
            }
        })
        .collect()
}

fn route_hashes(
    contracts: &[FixtureRouteContract],
    select: impl Fn(&FixtureRouteContract) -> ContentHash,
) -> Vec<RouteContractHash> {
    contracts
        .iter()
        .map(|contract| RouteContractHash {
            route: contract.route,
            content_hash: select(contract),
        })
        .collect()
}

fn scenario_states(
    represented: &RepresentedRouteSet,
    contracts: &[FixtureRouteContract],
) -> Result<Vec<PortfolioScenarioModelState>> {
    (0..SCENARIO_STATE_COUNT)
        .map(|scenario_index| {
            let kind = if scenario_index < PIT_STATE_COUNT {
                PortfolioScenarioKind::PitBootstrap
            } else if scenario_index < PIT_STATE_COUNT + CALIBRATION_STATE_COUNT {
                PortfolioScenarioKind::CalibrationUncertainty
            } else {
                PortfolioScenarioKind::StructuralStress
            };
            let quantile = match kind {
                PortfolioScenarioKind::PitBootstrap => {
                    scenario_index.saturating_mul(9_973) % 10_000
                }
                PortfolioScenarioKind::CalibrationUncertainty => {
                    (scenario_index - PIT_STATE_COUNT).saturating_mul(251) % 10_000
                }
                PortfolioScenarioKind::StructuralStress => {
                    9_000
                        + (scenario_index - PIT_STATE_COUNT - CALIBRATION_STATE_COUNT)
                            .saturating_mul(25)
                }
            };
            let route_factors = represented
                .routes
                .iter()
                .map(|route| {
                    let contract = contracts
                        .iter()
                        .find(|contract| contract.route == *route)
                        .with_context(|| format!("scenario state lost {route:?} contract"))?;
                    let (probability_shift, win_recovery, executable, release) = match kind {
                        PortfolioScenarioKind::PitBootstrap => (0, 10_000, 9_500, 10_000),
                        PortfolioScenarioKind::CalibrationUncertainty => {
                            (-200, 9_500, 8_500, 12_000)
                        }
                        PortfolioScenarioKind::StructuralStress => (-500, 8_000, 5_000, 20_000),
                    };
                    Ok(PortfolioScenarioRouteFactor {
                        route: *route,
                        systematic_quantile_bps: quantile,
                        systematic_weight_bps: 6_500,
                        calibrated_probability_shift_bps: probability_shift,
                        split_probability_quantile_bps: match kind {
                            PortfolioScenarioKind::PitBootstrap => 5_000,
                            PortfolioScenarioKind::CalibrationUncertainty => quantile,
                            PortfolioScenarioKind::StructuralStress => 10_000,
                        },
                        win_cash_recovery_bps: win_recovery,
                        split_cash_recovery_bps: 5_000,
                        loss_cash_recovery_bps: 0,
                        executable_share_bps: executable,
                        capital_release_multiplier_bps: release,
                        factor_lineage_hash: ResearchHasher::canonical(&(
                            "feedback-closure-scenario-factor-v1",
                            scenario_index,
                            kind,
                            route,
                            contract.serving_contract_hash,
                            contract.calibration_contract_hash,
                            contract.recommendation_contract_hash,
                        ))?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let mut state = PortfolioScenarioModelState {
                scenario_index,
                kind,
                label: format!("feedback-closure-{kind:?}-{scenario_index:04}"),
                scenario_state_hash: ContentHash::from_bytes([0; 32]),
                route_factors,
            };
            state.scenario_state_hash = state.recomputed_state_hash()?;
            Ok(state)
        })
        .collect()
}

fn scenario_distributions() -> Vec<ScenarioDistribution> {
    let nominal = (0..SCENARIO_STATE_COUNT)
        .map(|scenario_index| ScenarioWeight {
            scenario_index,
            probability_bps: 25,
        })
        .collect();
    let robust = (0..SCENARIO_STATE_COUNT)
        .map(|scenario_index| ScenarioWeight {
            scenario_index,
            probability_bps: if scenario_index < PIT_STATE_COUNT {
                12
            } else if scenario_index < PIT_STATE_COUNT + CALIBRATION_STATE_COUNT {
                54
            } else {
                100
            },
        })
        .collect();
    vec![
        ScenarioDistribution {
            distribution_id: "nominal".to_owned(),
            nominal: true,
            weights: nominal,
        },
        ScenarioDistribution {
            distribution_id: "robust-stress".to_owned(),
            nominal: false,
            weights: robust,
        },
    ]
}

async fn persist_model(
    artifact_store: &Arc<dyn ArtifactStore>,
    artifact: &PortfolioScenarioModelArtifact,
) -> Result<()> {
    let key = ArtifactKey::new(
        ArtifactNamespace::PortfolioScenarioModel,
        artifact.portfolio_scenario_model_artifact_id.to_string(),
        "json",
    )?;
    let bytes = serde_json::to_vec(artifact)?;
    artifact_store.put(key.clone(), &bytes).await?;
    let stored = artifact_store.get_by_key(&key).await?;
    ensure!(
        stored == bytes,
        "scenario-model artifact failed exact read-back"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, slice};

    use anyhow::{Context, Result, bail};
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            market::{book::BookLevel, fee::BuilderFeeAttribution},
            order::PolymarketOrderRules,
            quant::{
                DiscountCurvePoint, ExecutableEconomicTier, PortfolioScenarioEvidenceRegime,
                PortfolioScenarioFitEvidence, PortfolioScenarioModelArtifact,
                PortfolioScenarioResamplingMethod, PortfolioScenarioRouteFitLineage,
                PortfolioScenarioRouteModelLineage, PortfolioScenarioVisibility,
                RepresentedRouteSet, RouteCompatibilityDigests, RouteContractHash,
                ScenarioDistribution,
            },
        },
        enums::{
            common::{MarketCategory, TickSize},
            quant::{FillRequirement, OutcomeSide},
        },
        hashing::CanonicalDigest,
        runtime_config::{BuyModelRoute, PortfolioScenarioModelArtifactBinding},
        types::{
            BacktestPathSetId, Bps, CalibrationArtifactId, ContentHash, EventId, MarketId,
            ModelVersionId, PortfolioScenarioModelArtifactId, Price, ReportRouteRunId,
            SchemaVersion, Shares, SignalCandidateId, TokenId, Usd,
            calibration::CalibratedPayoutDistribution,
        },
    };
    use quant_pivot_research::{
        execution_semantics::PitFeeSchedule,
        hashing::ResearchHasher,
        portfolio::{
            CapitalTimeBucketContract, EconomicTierFactory, ExecutableCashTierSeedFactory,
            ExecutableCashTierSeedInput, PortfolioScenarioFoldFitInput,
            PortfolioScenarioGenerationInput, PortfolioScenarioGenerator,
            PortfolioScenarioLegInput, PortfolioScenarioMethodology, PortfolioScenarioModelFitter,
            PortfolioScenarioResidualObservation, TierSeedBuild, VerifiedPortfolioScenarioModel,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use super::{
        EXPECTED_BLOCK_LENGTH, FixtureRouteContract, SCENARIO_HORIZON_BUCKETS,
        route_payout_residual, scenario_distributions, scenario_states,
    };
    use crate::support::execution_pg_seed::CalibrationEvidencePreset;

    const ADMISSION_FLOOR_BPS: u32 = 5_200;
    const REQUIRED_MARGIN_BPS: u32 = 800;

    struct CadenceScenario {
        represented: RepresentedRouteSet,
        model: PortfolioScenarioModelArtifact,
        binding: PortfolioScenarioModelArtifactBinding,
    }

    #[test]
    fn strong_margin_crosses_streams() -> Result<()> {
        let payout =
            CalibrationEvidencePreset::StrongBinarySignal.calibrate_distribution(dec!(0.60))?;
        // Consecutive production-closure cadences intentionally re-key the
        // fitted stream; neither date is selected from a searched seed set.
        let cadences = [
            Utc.with_ymd_and_hms(2026, 6, 28, 12, 0, 0)
                .single()
                .context("first closure cadence is invalid")?,
            Utc.with_ymd_and_hms(2026, 6, 29, 12, 0, 0)
                .single()
                .context("adjacent closure cadence is invalid")?,
        ];
        let quotes = [
            (OutcomeSide::Yes, dec!(0.395), dec!(0.425)),
            (OutcomeSide::No, dec!(0.3975), dec!(0.4225)),
            (OutcomeSide::Yes, dec!(0.400), dec!(0.420)),
            (OutcomeSide::No, dec!(0.4025), dec!(0.4175)),
            (OutcomeSide::Yes, dec!(0.405), dec!(0.415)),
        ];
        let mut streams = BTreeSet::new();

        for cadence in cadences {
            let scenario = CadenceScenario::build(cadence)?;
            assert!(
                streams.insert(scenario.model.scenario_random_stream_hash),
                "adjacent closure cadences must select different scenario streams"
            );
            let verified = VerifiedPortfolioScenarioModel::verify(
                &scenario.binding,
                &scenario.model,
                &scenario.represented,
            )?;
            for (index, (side, bid, ask)) in quotes.into_iter().enumerate() {
                let ordinal = u32::try_from(index + 1)?;
                let (tier, distributions) =
                    tier_for_candidate(cadence, ordinal, side, bid, ask, payout, &verified)?;
                let lower = distributions
                    .iter()
                    .map(|distribution| {
                        let probability = positive_probability_bps(&tier, distribution)?;
                        assert!(
                            probability >= ADMISSION_FLOOR_BPS + REQUIRED_MARGIN_BPS,
                            "StrongBinarySignal margin regressed: cadence={cadence} stream={} ordinal={ordinal} side={side:?} distribution={} probability_bps={probability} required_bps={}",
                            scenario.model.scenario_random_stream_hash,
                            distribution.distribution_id,
                            ADMISSION_FLOOR_BPS + REQUIRED_MARGIN_BPS,
                        );
                        Ok(probability)
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .min()
                    .context("scenario artifact has no allowed distribution")?;
                assert_eq!(
                    tier.profit_probability_lower_bps, lower,
                    "production economic tier must retain the conservative distribution floor"
                );
            }
        }
        assert_eq!(streams.len(), cadences.len());
        Ok(())
    }

    impl CadenceScenario {
        fn build(cadence: DateTime<Utc>) -> Result<Self> {
            let represented = RepresentedRouteSet::from_routes([BuyModelRoute::Crypto])?;
            let contracts = [fixture_contract()?];
            let contract = &contracts[0];
            let compatibility = RouteCompatibilityDigests::try_new(
                &represented,
                &[RouteContractHash {
                    route: BuyModelRoute::Crypto,
                    content_hash: contract.serving_contract_hash,
                }],
                &[RouteContractHash {
                    route: BuyModelRoute::Crypto,
                    content_hash: contract.calibration_contract_hash,
                }],
                &[RouteContractHash {
                    route: BuyModelRoute::Crypto,
                    content_hash: contract.recommendation_contract_hash,
                }],
            )?;
            let model = scenario_template(cadence, &represented, &contracts, compatibility)?;
            let methodology = PortfolioScenarioMethodology::from_promoted(&model)?;
            let calibration =
                CalibrationEvidencePreset::StrongBinarySignal.resolved_calibration()?;
            let observations = fit_observations(cadence);
            let fitted = PortfolioScenarioModelFitter::fit_fold(&PortfolioScenarioFoldFitInput {
                methodology: &methodology,
                represented_routes: &represented,
                compatibility,
                evidence_regime: PortfolioScenarioEvidenceRegime::FullL2ExecutionEconomics,
                route: BuyModelRoute::Crypto,
                model_version_id: contract.model_version_id,
                model_artifact_hash: contract.model_artifact_hash,
                serving_contract_hash: contract.serving_contract_hash,
                calibration_artifact_hash: contract.calibration_contract_hash,
                calibration: &calibration,
                recommendation_contract_hash: contract.recommendation_contract_hash,
                prediction_horizon_secs: 3_600,
                observations: &observations,
                estimator_identity_hash: fixture_hash("strong-margin-estimator")?,
                model_fit_groups_hash: fixture_hash("strong-margin-model-groups")?,
                calibration_fit_groups_hash: fixture_hash("strong-margin-calibration-groups")?,
                scenario_fit_groups_hash: fixture_hash("strong-margin-scenario-groups")?,
                bound_at: cadence,
            })?;
            assert_ne!(
                fitted.artifact.scenario_random_stream_hash, model.scenario_random_stream_hash,
                "regression must exercise the fitted stream, not the bootstrap template"
            );
            Ok(Self {
                represented,
                model: fitted.artifact,
                binding: fitted.binding,
            })
        }
    }

    fn scenario_template(
        cadence: DateTime<Utc>,
        represented: &RepresentedRouteSet,
        contracts: &[FixtureRouteContract],
        compatibility: RouteCompatibilityDigests,
    ) -> Result<PortfolioScenarioModelArtifact> {
        let contract = contracts
            .first()
            .context("StrongBinarySignal template lost its Crypto contract")?;
        let fit_window_start = cadence - Duration::days(182);
        let as_of = cadence - Duration::days(2);
        let states = scenario_states(represented, contracts)?;
        let discount_curve = vec![DiscountCurvePoint {
            end_secs: 7_200,
            annualized_cost_bps: 500,
        }];
        let capital_time_bucket_contract_digest =
            CapitalTimeBucketContract::try_from(discount_curve.as_slice())?.content_hash()?;
        let scenario_random_stream_hash = ResearchHasher::canonical(&(
            "feedback-closure-scenario-random-stream-v1",
            represented.digest,
            fit_window_start,
            as_of,
        ))?;
        let pit_residual_panel_hash = ResearchHasher::canonical(&(
            "feedback-closure-margin-panel-v1",
            represented.digest,
            fit_window_start,
            as_of,
        ))?;
        let mut model = PortfolioScenarioModelArtifact {
            portfolio_scenario_model_artifact_id:
                PortfolioScenarioModelArtifactId::from_content_hash(&pit_residual_panel_hash),
            schema_version: SchemaVersion::FIRST,
            as_of,
            fit_window_start,
            time_bucket_secs: 3_600,
            ordered_routes: represented.routes.clone(),
            route_set_digest: represented.digest,
            serving_contract_digest: compatibility.serving_contract_digest,
            calibration_contract_digest: compatibility.calibration_contract_digest,
            recommendation_contract_digest: compatibility.recommendation_contract_digest,
            evidence_regime: PortfolioScenarioEvidenceRegime::FullL2ExecutionEconomics,
            capital_time_bucket_contract_digest,
            scenario_random_stream_hash,
            pit_residual_panel_hash,
            calibration_uncertainty_model_hash: ResearchHasher::canonical(&(
                "feedback-closure-margin-calibration-v1",
                contract.calibration_artifact_id,
                contract.calibration_contract_hash,
            ))?,
            stress_catalog_hash: ResearchHasher::canonical(&(
                "feedback-closure-margin-stress-v1",
                &states,
            ))?,
            resampling_method: PortfolioScenarioResamplingMethod::StationaryBootstrap {
                expected_block_length: EXPECTED_BLOCK_LENGTH,
                scenario_horizon_buckets: SCENARIO_HORIZON_BUCKETS,
            },
            route_fit_lineage: vec![PortfolioScenarioRouteFitLineage {
                route: BuyModelRoute::Crypto,
                model_lineage: PortfolioScenarioRouteModelLineage {
                    evaluated_model_version_id: contract.model_version_id,
                    evaluated_model_artifact_hash: contract.model_artifact_hash,
                    evaluated_serving_contract_hash: contract.serving_contract_hash,
                    calibration_source_model_version_id: contract
                        .calibration_source_model_version_id,
                    calibration_source_model_artifact_hash: contract
                        .calibration_source_model_artifact_hash,
                    calibration_source_serving_contract_hash: contract
                        .calibration_source_serving_contract_hash,
                },
                fit_evidence: PortfolioScenarioFitEvidence::CpcvPath {
                    backtest_path_set_id: BacktestPathSetId::new(fixture_uuid(
                        "strong-margin-path-set",
                    )),
                    backtest_path_set_hash: pit_residual_panel_hash,
                    representative_path_index: 0,
                },
                calibration_artifact_id: contract.calibration_artifact_id,
                calibration_artifact_hash: contract.calibration_contract_hash,
                recommendation_contract_hash: contract.recommendation_contract_hash,
                fit_window_start,
                fit_window_end: as_of,
            }],
            states,
            distributions: scenario_distributions(),
            discount_curve,
            content_hash: pit_residual_panel_hash,
        };
        model.content_hash = model.recomputed_hash()?;
        model.portfolio_scenario_model_artifact_id =
            PortfolioScenarioModelArtifactId::from_content_hash(&model.content_hash);
        Ok(model)
    }

    fn fit_observations(cadence: DateTime<Utc>) -> Vec<PortfolioScenarioResidualObservation> {
        let observation_start = cadence - Duration::days(181);
        (0_i64..180)
            .map(|offset| PortfolioScenarioResidualObservation {
                decision_at: observation_start + Duration::days(offset),
                market_id: MarketId::new(format!(
                    "strong-margin-fit-market-{}",
                    offset.rem_euclid(5) + 1
                )),
                token_id: TokenId::new(format!(
                    "strong-margin-fit-token-{}",
                    offset.rem_euclid(5) + 1
                )),
                economic_residual: route_payout_residual(BuyModelRoute::Crypto, offset),
            })
            .collect()
    }

    fn fixture_contract() -> Result<FixtureRouteContract> {
        Ok(FixtureRouteContract {
            route: BuyModelRoute::Crypto,
            model_version_id: ModelVersionId::new(fixture_uuid("strong-margin-model")),
            model_artifact_hash: fixture_hash("strong-margin-model-artifact")?,
            serving_contract_hash: fixture_hash("strong-margin-serving-contract")?,
            calibration_source_model_version_id: ModelVersionId::new(fixture_uuid(
                "strong-margin-calibration-source",
            )),
            calibration_source_model_artifact_hash: fixture_hash(
                "strong-margin-calibration-source-artifact",
            )?,
            calibration_source_serving_contract_hash: fixture_hash(
                "strong-margin-calibration-source-serving",
            )?,
            calibration_artifact_id: CalibrationArtifactId::new(fixture_uuid(
                "strong-margin-calibration",
            )),
            calibration_contract_hash: fixture_hash("strong-margin-calibration-contract")?,
            recommendation_contract_hash: fixture_hash("strong-margin-recommendation-contract")?,
            prediction_horizon_secs: 3_600,
        })
    }

    fn tier_for_candidate(
        cadence: DateTime<Utc>,
        ordinal: u32,
        side: OutcomeSide,
        bid: Decimal,
        ask: Decimal,
        payout: CalibratedPayoutDistribution,
        model: &VerifiedPortfolioScenarioModel<'_>,
    ) -> Result<(ExecutableEconomicTier, Vec<ScenarioDistribution>)> {
        let market_id = MarketId::new(format!("feedback-closure-report-crypto-market-{ordinal}"));
        let token_id = TokenId::new(format!("strong-margin-{side:?}-{ordinal}"));
        let lineage_hash = ResearchHasher::canonical(&(
            "feedback-closure-strong-margin-candidate-v1",
            cadence,
            ordinal,
            side,
        ))?;
        let leg = PortfolioScenarioLegInput {
            route: BuyModelRoute::Crypto,
            market_id: market_id.clone(),
            token_id: token_id.clone(),
            outcome_side: side,
            calibrated_payout_distribution: payout,
            observed_exit_capacity_shares: Shares::new(dec!(2_400)),
            base_capital_release_secs: 3_600,
            lineage_hash,
        };
        let scenario = PortfolioScenarioGenerator::generate(PortfolioScenarioGenerationInput {
            model_contract: model,
            decision_at: cadence + Duration::seconds(2),
            visibility: PortfolioScenarioVisibility::PointInTime,
            input_universe_hash: ResearchHasher::canonical(&(
                "feedback-closure-strong-margin-universe-v1",
                cadence,
            ))?,
            legs: slice::from_ref(&leg),
        })?;
        let bids = [
            BookLevel::from_decimal(Price::new(bid), Shares::new(dec!(2_400)))
                .context("closure margin bid is not representable")?,
        ];
        let asks = [
            BookLevel::from_decimal(Price::new(ask), Shares::new(dec!(21_600)))
                .context("closure margin ask is not representable")?,
        ];
        let fee_schedule = PitFeeSchedule {
            schedule_hash: fixture_hash("strong-margin-fee-schedule")?,
            effective_at: cadence - Duration::days(1),
            available_at: cadence - Duration::days(1),
            platform_rate: dec!(0.04),
            exponent: Decimal::ONE,
            taker_only: true,
            builder_maker_fee_bps: Bps::ZERO,
            builder_taker_fee_bps: Bps::ZERO,
            builder_attribution: BuilderFeeAttribution::NoBuilderCode,
        };
        let rules = PolymarketOrderRules::new(TickSize::QuarterCent, Shares::new(dec!(5)))?;
        let TierSeedBuild::Ready(seed) =
            ExecutableCashTierSeedFactory::build(ExecutableCashTierSeedInput {
                report_route_run_id: ReportRouteRunId::new(fixture_uuid(&format!(
                    "strong-margin-route-run-{cadence}"
                ))),
                candidate_id: SignalCandidateId::new(fixture_uuid(&format!(
                    "strong-margin-candidate-{cadence}-{ordinal}"
                ))),
                tier_ordinal: ordinal,
                route: BuyModelRoute::Crypto,
                market_id,
                event_id: EventId::new(format!("strong-margin-event-{ordinal}")),
                category: MarketCategory::Crypto,
                token_id,
                outcome_side: side,
                bids: &bids,
                asks: &asks,
                fee_schedule: &fee_schedule,
                fill_at: cadence,
                limit_price: Price::new(ask),
                cash_budget: Usd::new(dec!(25)),
                fill_requirement: FillRequirement::AllOrNothing,
                order_rules: rules,
                source_lineage_hash: lineage_hash,
            })?
        else {
            bail!("closure StrongBinarySignal tier did not remain exactly executable")
        };
        let distributions = scenario.distributions.clone();
        let tier = EconomicTierFactory::build(*seed, &scenario)?;
        Ok((tier, distributions))
    }

    fn positive_probability_bps(
        tier: &ExecutableEconomicTier,
        distribution: &ScenarioDistribution,
    ) -> Result<u32> {
        distribution
            .weights
            .iter()
            .try_fold(0_u32, |total, weight| {
                let cashflow = tier
                    .scenario_cashflows
                    .get(usize::try_from(weight.scenario_index)?)
                    .filter(|cashflow| cashflow.scenario_index == weight.scenario_index)
                    .context("distribution references an absent scenario cash flow")?;
                if cashflow.discounted_net_usd.is_positive() {
                    total
                        .checked_add(weight.probability_bps)
                        .context("positive scenario mass overflowed u32")
                } else {
                    Ok(total)
                }
            })
    }

    fn fixture_hash(label: &str) -> Result<ContentHash> {
        Ok(CanonicalDigest::content_hash_typed(
            "quant-pivot/feedback-closure-margin-fixture",
            1,
            &label,
        )?)
    }

    fn fixture_uuid(label: &str) -> Uuid {
        Uuid::new_v5(&Uuid::NAMESPACE_OID, label.as_bytes())
    }
}
