//! Atomic global report orchestration across every represented model Route.

use std::{
    collections::{BTreeMap, HashMap, HashSet, btree_map::Entry},
    sync::Arc,
};

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, report::ReportError};
use quant_pivot_models::{
    config::PortfolioSolverDeployConfig,
    domain::{
        data_plane::{DecisionBoundary, DecisionClock, DecisionSource},
        governance::DecisionPolicySnapshotInfo,
        quant::{
            ExecutableEconomicTier, ExistingPortfolioState, MarketCandidate, NewPortfolioPlan,
            NewReportDataQualitySnapshot, NewReportRouteRun, PortfolioDecisionResult,
            PortfolioScenarioVisibility, RepresentedRouteSet, RouteCandidateFunnel,
            RouteModelLineage, RouteRunOutcome, TradePolicyArtifactInfo,
        },
    },
    enums::quant::{EmptyReportReason, OutcomeSide, TradePolicyStatus},
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, DecisionPolicySnapshot},
    types::{
        ContentHash, DecisionPolicySnapshotId, EconomicTierId, EntryOrderTemplate, FeatureVectorId,
        MarketId, ModelRunId, ModelVersionId, PortfolioPlanId, Price, ReportDataQualitySnapshotId,
        ReportDataQualityTokens, ReportRouteRunId, ReportRunId, Shares, SignalCandidateId, TokenId,
        TradePolicyCohort, TradePolicyCohortProvenance, Usd,
        calibration::CalibratedPayoutDistribution,
    },
};
use quant_pivot_repository::traits::{
    MarketSelectionRepository, PolicyRepository, TradePolicyRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    execution_semantics::{PitFeeSchedule, aggressive_buy_limit},
    features::{FeatureVector, MarketDecisionCapture},
    hashing::ResearchHasher,
    model::{CalibrationArtifactLoader, ModelArtifact, SignalCandidate},
    portfolio::{
        AccountSnapshot, EconomicTierFactory, ExecutableCashTierSeedFactory,
        ExecutableCashTierSeedInput, ExecutableTierSeed, ExistingPortfolioFactory,
        GlobalPortfolioInput, GlobalPortfolioPlanner, GlobalPortfolioResult, PlannedEconomicTier,
        PortfolioScenarioGenerationInput, PortfolioScenarioGenerator, PortfolioScenarioLegInput,
        SealedPortfolioScenarioArtifact, TierAdmissionRejection, VerifiedPortfolioScenarioModel,
    },
    selection::{
        MarketSelectionBuildRequest, MarketSelectionSnapshot, MarketSelector,
        ModelFeatureRequirements, SelectedMarket,
    },
};
use rust_decimal::Decimal;
use serde::Serialize;

use super::{
    composer::{ComposeReportInput, RecommendationComposer},
    readiness::ReportReadinessGate,
    types::{
        BuildReportRequest, ComposedReport, EmptyReportContext, PlannedReportRecommendation,
        ReportTierRejection, ReportTrigger,
    },
};
use crate::{
    governance::{RuntimeControlsHandle, resolve_return_model_calibration},
    ingest::data_pipeline::MicrostructureCommitBarrier,
    observability::serving_evidence::FeatureEvidenceCommitment,
    prefetch::market_candidates::{DecisionSnapshotSource, MarketCandidateProvider},
    service::{
        account::AccountProviderFactory,
        equity::{DrawdownProvider, ReportEquitySnapshot},
        feature_pipeline::{FeaturePipelineRequest, FeaturePipelineResult, FeaturePipelineService},
        market_selection::map_snapshot_to_model,
        model_runner::{
            ActiveModelRequirements, ActiveRouteRequirementsRequest, ModelMarketDecision,
            ModelRunRequest, ModelRunner,
        },
        portfolio_context::{
            PromotedPortfolioContext, PromotedPortfolioContextLoader, PromotedRouteContract,
        },
    },
};

/// Report builder interface.
#[async_trait::async_trait]
pub trait ReportBuilder: Send + Sync {
    /// Build a complete report artifact without writing the report transaction.
    async fn build(&self, request: BuildReportRequest) -> QuantResult<ComposedReport>;
}

/// Dependencies for [`DefaultReportBuilder`].
pub struct ReportBuilderDeps {
    pub runtime_config_repo: Arc<dyn PolicyRepository>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub calibration_loader: Arc<dyn CalibrationArtifactLoader>,
    pub trade_policy_repo: Arc<dyn TradePolicyRepository>,
    pub market_selector: Arc<dyn MarketSelector>,
    pub market_selection_repo: Arc<dyn MarketSelectionRepository>,
    pub candidate_provider: Arc<MarketCandidateProvider>,
    pub feature_pipeline: Arc<FeaturePipelineService>,
    pub model_runner: Arc<ModelRunner>,
    pub account_provider_factory: Arc<AccountProviderFactory>,
    pub drawdown_provider: Arc<dyn DrawdownProvider>,
    pub composer: Arc<dyn RecommendationComposer>,
    pub portfolio_solver: PortfolioSolverDeployConfig,
    pub runtime_controls: RuntimeControlsHandle,
    pub readiness_gate: Arc<dyn ReportReadinessGate>,
    pub microstructure_commit: Arc<dyn MicrostructureCommitBarrier>,
}

/// Production report builder.
pub struct DefaultReportBuilder {
    deps: ReportBuilderDeps,
}

struct BuildContext {
    report_run_id: ReportRunId,
    version: DecisionPolicySnapshotInfo,
    config: DecisionPolicySnapshot,
    boundary: DecisionBoundary,
    top_n: u32,
}

struct ReadyRoute {
    active: ActiveModelRequirements,
    contract: PromotedRouteContract,
    trade_policy: TradePolicyArtifactInfo,
    report_route_run_id: ReportRouteRunId,
    model_run_id: Option<ModelRunId>,
    eligible_markets: u32,
    feature_complete_markets: u32,
    calibrated_candidates: u32,
    admitted_economic_tiers: u32,
    selected_recommendations: u32,
}

#[derive(Clone)]
struct RoutedCandidate {
    route: BuyModelRoute,
    model_version_id: ModelVersionId,
    report_route_run_id: ReportRouteRunId,
    candidate: SignalCandidate,
}

#[derive(Clone)]
struct TierSource {
    route: BuyModelRoute,
    report_route_run_id: ReportRouteRunId,
    model_version_id: ModelVersionId,
    model_run_id: ModelRunId,
    candidate: SignalCandidate,
    trade_policy: TradePolicyCohortProvenance,
    trade_policy_cohort: TradePolicyCohort,
    entry_limit_price: Price,
}

struct SeededTier {
    seed: ExecutableTierSeed,
    source: TierSource,
}

struct PortfolioBuild {
    row: NewPortfolioPlan,
    selected: Vec<PlannedEconomicTier>,
    rejections: Vec<TierAdmissionRejection>,
}

impl DefaultReportBuilder {
    /// Build a report builder from wired dependencies.
    #[must_use]
    pub const fn new(deps: ReportBuilderDeps) -> Self {
        Self { deps }
    }
}

#[async_trait::async_trait]
impl ReportBuilder for DefaultReportBuilder {
    async fn build(&self, request: BuildReportRequest) -> QuantResult<ComposedReport> {
        Box::pin(self.build_report(request)).await
    }
}

impl DefaultReportBuilder {
    async fn build_report(&self, request: BuildReportRequest) -> QuantResult<ComposedReport> {
        if self.deps.readiness_gate.is_system_degraded() {
            return Err(ReportError::InvariantViolation {
                stage: "report_readiness",
                detail: format!(
                    "operational phase {:?} is below the report boundary",
                    self.deps.readiness_gate.operational_phase()
                ),
            }
            .into());
        }

        let context = self.prepare_context(&request).await?;
        let batch = self
            .deps
            .candidate_provider
            .candidates(
                &context.boundary,
                &context.config.profile_artifacts.domain.definition,
            )
            .await?;
        enforce_candidate_ceiling(
            batch.candidates.len(),
            context.config.recommendation.reports.hard_candidate_ceiling,
        )?;

        let account = self.account_snapshot(&context).await?;
        let mut equity = self
            .deps
            .drawdown_provider
            .snapshot_for_report(&account)
            .await?;
        self.refresh_drawdown(&account, &mut equity).await?;

        let initial = self
            .select_snapshot(
                &context,
                ModelFeatureRequirements::default(),
                batch.candidates.clone(),
            )
            .await?;
        let represented_routes = represented_routes(&initial, &account)?;
        let mut routes = self
            .resolve_ready_routes(&context, &represented_routes, &initial)
            .await?;
        let portfolio_context = self
            .load_portfolio_context(&context, &represented_routes, &routes)
            .await?;

        let requirements = merged_requirements(&routes);
        let selection = self
            .select_snapshot(&context, requirements.clone(), batch.candidates.clone())
            .await?;
        self.persist_selection(&selection, &batch.candidates)
            .await?;

        let features = self
            .build_features(
                &context,
                &selection,
                batch.snapshot_source.as_ref(),
                &requirements,
            )
            .await?;
        let (routed_candidates, model_decisions) = self
            .run_route_models(&context, &selection, &features, &mut routes)
            .await?;
        let seeded_tiers = build_economic_tier_seeds(
            &context,
            &selection,
            &features.captures,
            &routes,
            &routed_candidates,
            portfolio_context.as_ref(),
        )?;
        let scenario_artifact = materialize_portfolio_scenario(
            &context,
            &account,
            &represented_routes,
            portfolio_context.as_ref(),
            &seeded_tiers,
        )?;
        let (tiers, tier_sources) =
            finalize_economic_tiers(seeded_tiers, scenario_artifact.as_ref())?;
        let portfolio = build_portfolio(&PortfolioBuildInput {
            context: &context,
            selection: &selection,
            account: &account,
            equity: &equity,
            represented_routes: &represented_routes,
            promoted: portfolio_context.as_ref(),
            scenario_artifact: scenario_artifact.as_ref(),
            tiers: &tiers,
        })?;
        let tier_rejections = report_tier_rejections(&tiers, &portfolio.rejections)?;
        update_route_funnels(&mut routes, &tiers, &portfolio)?;
        let planned = planned_recommendations(&portfolio.selected, &tier_sources)?;
        // Every derived report fact is anchored to the database-owned frozen
        // decision clock. Using the process wall clock here makes the same
        // report-run preimage non-replayable and can place availability after
        // the PostgreSQL commit clock when host/container clocks drift.
        let published_at = context.boundary.decision_at();
        let route_runs = route_rows(&request, &routes, published_at);
        let empty = empty_context(&EmptyContextInput {
            initial: &initial,
            selection: &selection,
            features: &features,
            candidates: &routed_candidates,
            tiers: &tiers,
            planned: &planned,
            account: &account,
            config: &context.config,
        })?;
        let feature_vector_by_market = features.vector_ids_by_market()?;
        let trigger_key = request
            .trigger
            .key(request.trigger_time)
            .map_err(|error| QuantError::config(error.to_string()))?
            .to_string();
        self.deps.composer.compose(ComposeReportInput {
            report_run_id: request.report_run_id,
            trigger: &request.trigger,
            trigger_key,
            decision_at: context.boundary.decision_at(),
            published_at,
            decision_policy_snapshot_id: context.version.decision_policy_snapshot_id,
            runtime_config: &context.config,
            runtime_mode: self.deps.runtime_controls.quant_runtime_mode(),
            selection: &selection,
            account: &account,
            account_snapshot: equity.account_snapshot,
            equity_snapshot: equity.equity_snapshot,
            portfolio_plan: portfolio.row,
            route_runs,
            tiers: &tiers,
            planned: &planned,
            tier_rejections: &tier_rejections,
            feature_rejected: &features.rejected,
            model_decisions: &model_decisions,
            captures: &features.captures,
            feature_vector_by_market: &feature_vector_by_market,
            data_quality_snapshot: features.data_quality_snapshot,
            candidate_count: count_u32(routed_candidates.len(), "report.candidate_count")?,
            empty,
            top_n: context.top_n,
        })
    }

    async fn prepare_context(&self, request: &BuildReportRequest) -> QuantResult<BuildContext> {
        let version = self
            .deps
            .runtime_config_repo
            .load_active_at(request.trigger_time)
            .await?
            .ok_or_else(|| QuantError::config("no active decision policy snapshot"))?;
        let config = version.snapshot.clone();
        let knowledge_lag_secs = resolve_knowledge_lag(request, &config)?;
        let boundary = DecisionClock::new(knowledge_lag_secs).serving_boundary(
            request.trigger_time,
            config
                .profile_artifacts
                .domain
                .definition
                .crypto
                .availability_lag_secs,
            config
                .profile_artifacts
                .domain
                .definition
                .weather
                .availability_lag_secs,
        )?;
        self.deps
            .microstructure_commit
            .commit_through(boundary.cutoff_for(DecisionSource::Microstructure))
            .await?;
        Ok(BuildContext {
            report_run_id: request.report_run_id,
            version,
            top_n: resolve_top_n(request, &config)?,
            config,
            boundary,
        })
    }

    async fn account_snapshot(&self, context: &BuildContext) -> QuantResult<AccountSnapshot> {
        let budget = Usd::new(
            context
                .config
                .execution_risk
                .portfolio
                .budget
                .total_budget_usd
                .value,
        );
        self.deps
            .account_provider_factory
            .create(budget)?
            .snapshot(context.boundary.decision_at())
            .await
    }

    async fn refresh_drawdown(
        &self,
        account: &AccountSnapshot,
        equity: &mut ReportEquitySnapshot,
    ) -> QuantResult<()> {
        let resolved = self
            .deps
            .drawdown_provider
            .resolve_drawdown_for_sizing(account, equity.drawdown)
            .await?;
        equity.drawdown = resolved.drawdown;
        equity.equity_snapshot.high_water_mark_usd = resolved.high_water_mark_usd;
        equity.equity_snapshot.drawdown_pct = resolved.drawdown.current_ratio;
        Ok(())
    }

    async fn select_snapshot(
        &self,
        context: &BuildContext,
        model_requirements: ModelFeatureRequirements,
        candidates: Vec<MarketCandidate>,
    ) -> QuantResult<MarketSelectionSnapshot> {
        self.deps
            .market_selector
            .build_snapshot(
                MarketSelectionBuildRequest {
                    decision_at: context.boundary.decision_at(),
                    decision_policy_snapshot_id: context.version.decision_policy_snapshot_id,
                    selection: context.config.recommendation.selection.clone(),
                    data_quality: context.config.recommendation.data_quality.clone(),
                    features: context.config.profile_artifacts.features.definition.clone(),
                    model_requirements,
                    knowledge_lag_secs: context.boundary.knowledge_lag_secs(),
                },
                candidates,
            )
            .await
    }

    async fn persist_selection(
        &self,
        selection: &MarketSelectionSnapshot,
        candidates: &[MarketCandidate],
    ) -> QuantResult<()> {
        let model = map_snapshot_to_model(selection, candidates)?;
        self.deps
            .market_selection_repo
            .create_snapshot(model.snapshot, model.members)
            .await?;
        Ok(())
    }

    async fn resolve_ready_routes(
        &self,
        context: &BuildContext,
        represented_routes: &RepresentedRouteSet,
        initial: &MarketSelectionSnapshot,
    ) -> QuantResult<Vec<ReadyRoute>> {
        if represented_routes.is_empty() {
            return Ok(Vec::new());
        }
        let active = self
            .deps
            .model_runner
            .active_route_requirements(ActiveRouteRequirementsRequest {
                policy: &context.version,
                represented_routes,
            })
            .await?;
        let mut ready = Vec::with_capacity(active.len());
        for active_route in active {
            let route = active_route.route;
            let resolved = self
                .verify_route(context, active_route)
                .await
                .map_err(|error| route_error(route, &error))?;
            let eligible_markets = count_u32(
                initial
                    .included
                    .iter()
                    .filter(|market| BuyModelRoute::from(market.category) == route)
                    .count(),
                "route.eligible_markets",
            )?;
            ready.push(ReadyRoute {
                active: resolved.0,
                contract: resolved.1,
                trade_policy: resolved.2,
                report_route_run_id: route_run_id(context, route)?,
                model_run_id: None,
                eligible_markets,
                feature_complete_markets: 0,
                calibrated_candidates: 0,
                admitted_economic_tiers: 0,
                selected_recommendations: 0,
            });
        }
        if ready
            .iter()
            .map(|route| route.active.route)
            .collect::<Vec<_>>()
            != represented_routes.routes
        {
            return Err(ReportError::InvariantViolation {
                stage: "route_readiness",
                detail: "resolved Route order differs from represented Route order".to_owned(),
            }
            .into());
        }
        Ok(ready)
    }

    async fn verify_route(
        &self,
        context: &BuildContext,
        active: ActiveModelRequirements,
    ) -> QuantResult<(
        ActiveModelRequirements,
        PromotedRouteContract,
        TradePolicyArtifactInfo,
    )> {
        let artifact =
            ModelArtifact::load_verified(self.deps.artifact_store.as_ref(), &active.version)
                .await?;
        let calibration =
            resolve_return_model_calibration(self.deps.calibration_loader.as_ref(), &artifact)
                .await?
                .ok_or_else(|| ReportError::RouteReadiness {
                    route: active.route.as_str().to_owned(),
                    detail: "champion has no active calibrated-probability artifact".to_owned(),
                })?;
        let contract = PromotedRouteContract::from_version(active.route, &active.version)?;
        if calibration.artifact_id != contract.calibration_artifact_id {
            return Err(ReportError::RouteReadiness {
                route: active.route.as_str().to_owned(),
                detail: "resolved calibration differs from the serving contract".to_owned(),
            }
            .into());
        }
        let policy = load_trade_policy(
            self.deps.trade_policy_repo.as_ref(),
            &artifact,
            &active,
            &contract,
        )
        .await?;
        if context.boundary.decision_at() < policy.created_at {
            return Err(ReportError::RouteReadiness {
                route: active.route.as_str().to_owned(),
                detail: "Trade Policy is newer than the frozen decision boundary".to_owned(),
            }
            .into());
        }
        Ok((active, contract, policy))
    }

    async fn load_portfolio_context(
        &self,
        context: &BuildContext,
        represented_routes: &RepresentedRouteSet,
        routes: &[ReadyRoute],
    ) -> QuantResult<Option<PromotedPortfolioContext>> {
        if represented_routes.is_empty() {
            return Ok(None);
        }
        let loader = PromotedPortfolioContextLoader::new(
            Arc::clone(&self.deps.artifact_store),
            self.deps.portfolio_solver,
            context.config.clone(),
        );
        loader
            .load_routes(
                represented_routes.clone(),
                routes.iter().map(|route| route.contract.clone()).collect(),
                context.boundary.decision_at(),
                context.top_n,
            )
            .await
            .map(Some)
    }

    async fn build_features(
        &self,
        context: &BuildContext,
        selection: &MarketSelectionSnapshot,
        decision_snapshot: &DecisionSnapshotSource,
        requirements: &ModelFeatureRequirements,
    ) -> QuantResult<FeaturePipelineResult> {
        if selection.included.is_empty() {
            return Ok(FeaturePipelineResult {
                accepted: Vec::new(),
                rejected: Vec::new(),
                persisted: Vec::new(),
                feature_evidence: None,
                captures: HashMap::new(),
                data_quality_snapshot: context.empty_data_quality_snapshot(),
            });
        }
        self.deps
            .feature_pipeline
            .run(FeaturePipelineRequest {
                included: &selection.included,
                boundary: context.boundary.clone(),
                features: &context.config.profile_artifacts.features.definition,
                domain: &context.config.profile_artifacts.domain.definition,
                data_quality: &context.config.recommendation.data_quality,
                model_requirements: requirements,
                pit: decision_snapshot,
                decision_policy_snapshot_id: context.version.decision_policy_snapshot_id,
                liquidity_cap_usd: Usd::new(
                    context
                        .config
                        .execution_risk
                        .portfolio
                        .exposure_limits
                        .max_single_recommendation_usd
                        .value,
                ),
            })
            .await
    }

    async fn run_route_models(
        &self,
        context: &BuildContext,
        selection: &MarketSelectionSnapshot,
        features: &FeaturePipelineResult,
        routes: &mut [ReadyRoute],
    ) -> QuantResult<(Vec<RoutedCandidate>, Vec<ModelMarketDecision>)> {
        let persisted = features.vector_ids_by_market()?;
        let accepted = features
            .accepted
            .iter()
            .map(|vector| (vector.market_id.clone(), vector))
            .collect::<HashMap<_, _>>();
        let mut routed_candidates = Vec::new();
        let mut decisions = Vec::new();
        for route in routes {
            let markets = selection
                .included
                .iter()
                .filter(|market| {
                    BuyModelRoute::from(market.category) == route.active.route
                        && accepted.contains_key(&market.market_id)
                })
                .cloned()
                .collect::<Vec<_>>();
            route.feature_complete_markets =
                count_u32(markets.len(), "route.feature_complete_markets")?;
            if markets.is_empty() {
                continue;
            }
            let (vectors, ids) = route_vectors(&markets, &accepted, &persisted)?;
            let evidence = features.route_evidence()?;
            let outcome = self
                .deps
                .model_runner
                .run(ModelRunRequest {
                    decision_policy_snapshot_id: context.version.decision_policy_snapshot_id,
                    market_selection_id: Some(selection.market_selection_id),
                    selection: &markets,
                    feature_vectors: &vectors,
                    feature_vector_ids: &ids,
                    feature_evidence: &evidence,
                    serving: &route.active.serving,
                    top_n: bounded_usize(context.top_n)?,
                    boundary: context.boundary.clone(),
                })
                .await?;
            if outcome.model_version_id != route.active.model_version_id {
                return Err(ReportError::RouteReadiness {
                    route: route.active.route.as_str().to_owned(),
                    detail: "model run authority differs from the frozen champion".to_owned(),
                }
                .into());
            }
            for candidate in &outcome.accepted {
                if candidate.payout_distribution.is_none() {
                    return Err(ReportError::RouteReadiness {
                        route: route.active.route.as_str().to_owned(),
                        detail: format!(
                            "candidate {} has no calibrated payout distribution",
                            candidate.signal_candidate_id
                        ),
                    }
                    .into());
                }
            }
            route.model_run_id = Some(outcome.model_run_id);
            route.calibrated_candidates = outcome.emitted;
            routed_candidates.extend(outcome.accepted.into_iter().map(|candidate| {
                RoutedCandidate {
                    route: route.active.route,
                    model_version_id: route.active.model_version_id,
                    report_route_run_id: route.report_route_run_id,
                    candidate,
                }
            }));
            decisions.extend(outcome.decisions);
        }
        Ok((routed_candidates, decisions))
    }
}

async fn load_trade_policy(
    repository: &dyn TradePolicyRepository,
    model: &ModelArtifact,
    active: &ActiveModelRequirements,
    contract: &PromotedRouteContract,
) -> QuantResult<TradePolicyArtifactInfo> {
    let header = model.header();
    let (Some(artifact_id), Some(expected_hash)) = (
        header.trade_policy_artifact_id(),
        header.trade_policy_hash(),
    ) else {
        return Err(ReportError::RouteReadiness {
            route: active.route.as_str().to_owned(),
            detail: "model must bind Trade Policy id and hash together".to_owned(),
        }
        .into());
    };
    let policy =
        repository
            .find(&artifact_id)
            .await?
            .ok_or_else(|| ReportError::RouteReadiness {
                route: active.route.as_str().to_owned(),
                detail: format!("bound Trade Policy {artifact_id} is missing"),
            })?;
    let computed = ResearchHasher::canonical(&policy.payload_json)?;
    let fit = &policy.payload_json.fit_contract;
    let horizon = u64::try_from(contract.prediction_horizon_secs).map_err(|error| {
        ReportError::RouteReadiness {
            route: active.route.as_str().to_owned(),
            detail: format!("prediction horizon is invalid: {error}"),
        }
    })?;
    if artifact_id != contract.trade_policy_artifact_id
        || policy.status != TradePolicyStatus::Published
        || !policy.payload_json.is_publishable()
        || policy.content_hash != expected_hash
        || computed != policy.content_hash
        || fit.profile_ref != contract.research_profile_ref
        || fit.target_horizon_secs != horizon
    {
        return Err(ReportError::RouteReadiness {
            route: active.route.as_str().to_owned(),
            detail: "Trade Policy is not Published, canonical, and contract-compatible".to_owned(),
        }
        .into());
    }
    Ok(policy)
}

fn build_economic_tier_seeds(
    context: &BuildContext,
    selection: &MarketSelectionSnapshot,
    captures: &HashMap<MarketId, MarketDecisionCapture>,
    routes: &[ReadyRoute],
    candidates: &[RoutedCandidate],
    portfolio: Option<&PromotedPortfolioContext>,
) -> QuantResult<Vec<SeededTier>> {
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    let portfolio = portfolio.ok_or_else(|| ReportError::ScenarioArtifact {
        detail: "calibrated candidates exist without a promoted scenario-model context".to_owned(),
    })?;
    let selected = selection
        .included
        .iter()
        .map(|market| (market.market_id.clone(), market))
        .collect::<HashMap<_, _>>();
    let route_map = routes
        .iter()
        .map(|route| (route.active.route, route))
        .collect::<HashMap<_, _>>();
    let mut tiers = Vec::new();
    for routed in candidates {
        let market = selected
            .get(&routed.candidate.market_id)
            .copied()
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "economic_tier",
                detail: format!(
                    "candidate market {} is absent from final selection",
                    routed.candidate.market_id
                ),
            })?;
        let capture = captures.get(&routed.candidate.market_id).ok_or_else(|| {
            ReportError::InvariantViolation {
                stage: "economic_tier",
                detail: format!(
                    "candidate market {} has no frozen capture",
                    routed.candidate.market_id
                ),
            }
        })?;
        let route = route_map.get(&routed.route).copied().ok_or_else(|| {
            ReportError::InvariantViolation {
                stage: "economic_tier",
                detail: format!("candidate Route {:?} has no readiness state", routed.route),
            }
        })?;
        let built = candidate_tiers(context, market, capture, routed, route, portfolio)?;
        tiers.extend(
            built
                .into_iter()
                .map(|(seed, source)| SeededTier { seed, source }),
        );
    }
    tiers.sort_by(|left, right| {
        (
            left.seed.route,
            left.seed.market_id.as_str(),
            left.seed.tier_ordinal,
        )
            .cmp(&(
                right.seed.route,
                right.seed.market_id.as_str(),
                right.seed.tier_ordinal,
            ))
    });
    Ok(tiers)
}

fn candidate_tiers(
    context: &BuildContext,
    market: &SelectedMarket,
    capture: &MarketDecisionCapture,
    routed: &RoutedCandidate,
    route: &ReadyRoute,
    portfolio: &PromotedPortfolioContext,
) -> QuantResult<Vec<(ExecutableTierSeed, TierSource)>> {
    let horizon = validate_candidate_route(market, routed, route)?;
    let book = capture
        .book_for(&routed.candidate.token_id)
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "economic_tier",
            detail: format!(
                "candidate token {} has no frozen side-specific book",
                routed.candidate.token_id
            ),
        })?;
    let fee =
        capture
            .market
            .fee_schedule
            .as_ref()
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "economic_tier",
                detail: format!("market {} has no PIT fee schedule", market.market_id),
            })?;
    let fee_schedule = PitFeeSchedule::from_market_fee_schedule(fee).map_err(|error| {
        ReportError::InvariantViolation {
            stage: "economic_tier",
            detail: format!(
                "market {} fee schedule is invalid: {error:?}",
                market.market_id
            ),
        }
    })?;
    let best_ask = book
        .asks
        .first()
        .map(|level| level.price_decimal())
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "economic_tier",
            detail: format!(
                "market {} candidate-side ask book is empty",
                market.market_id
            ),
        })?;
    let mut by_budget = BTreeMap::<Usd, Vec<(ExecutableTierSeed, TierSource)>>::new();
    for (index, cohort) in route.trade_policy.payload_json.cohorts.iter().enumerate() {
        if cohort.key.category != market.category
            || cohort.key.horizon_secs != horizon
            || cohort.key.profile_ref != route.contract.research_profile_ref
            || !matches!(
                &cohort.entry_condition,
                quant_pivot_models::types::EntryConditionTemplate::Immediate
            )
        {
            continue;
        }
        let (fill_requirement, max_slippage_bps, max_book_age_ms) = match &cohort.entry_order {
            EntryOrderTemplate::Aggressive {
                fill_requirement,
                max_slippage_bps,
                max_book_age_ms,
            } => (*fill_requirement, *max_slippage_bps, *max_book_age_ms),
            EntryOrderTemplate::PassivePostOnly { .. } => continue,
        };
        if cohort.max_slippage_bps != max_slippage_bps || cohort.max_book_age_ms != max_book_age_ms
        {
            return Err(ReportError::RouteReadiness {
                route: routed.route.as_str().to_owned(),
                detail: "Trade Policy cohort duplicates inconsistent entry limits".to_owned(),
            }
            .into());
        }
        let cohort_index = u32::try_from(index).map_err(|error| ReportError::NumericOverflow {
            field: "trade_policy.cohort_index",
            detail: error.to_string(),
        })?;
        let tier_ordinal =
            cohort_index
                .checked_add(1)
                .ok_or_else(|| ReportError::NumericOverflow {
                    field: "economic_tier.tier_ordinal",
                    detail: "cohort index overflowed u32".to_owned(),
                })?;
        let limit_price = aggressive_buy_limit(best_ask, max_slippage_bps);
        let lineage_hash = tier_lineage_hash(&TierLineageInput {
            context,
            capture,
            routed,
            route,
            cohort,
            cohort_index,
            fee_schedule: &fee_schedule,
            portfolio,
        })?;
        let Some(seed) = ExecutableCashTierSeedFactory::build(ExecutableCashTierSeedInput {
            report_route_run_id: routed.report_route_run_id,
            candidate_id: routed.candidate.signal_candidate_id,
            tier_ordinal,
            route: routed.route,
            market_id: market.market_id.clone(),
            event_id: market.event_id.clone(),
            category: market.category,
            token_id: routed.candidate.token_id.clone(),
            outcome_side: routed.candidate.outcome_side,
            bids: &book.bids,
            asks: &book.asks,
            fee_schedule: &fee_schedule,
            fill_at: context.boundary.decision_at(),
            limit_price,
            cash_budget: cohort.key.cash_budget_tier,
            fill_requirement,
            source_lineage_hash: lineage_hash,
        })?
        else {
            continue;
        };
        let price = seed.entry.entry_vwap;
        if price < cohort.key.entry_price_min
            || price > cohort.key.entry_price_max
            || (price == cohort.key.entry_price_max && price != Price::ONE)
        {
            continue;
        }
        let model_run_id = route
            .model_run_id
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "economic_tier",
                detail: "candidate Route has no model run".to_owned(),
            })?;
        let provenance = TradePolicyCohortProvenance {
            artifact_id: route.trade_policy.artifact_id,
            artifact_hash: route.trade_policy.content_hash,
            cohort_index,
            cohort_key: cohort.key.clone(),
        };
        by_budget
            .entry(cohort.key.cash_budget_tier)
            .or_default()
            .push((
                seed,
                TierSource {
                    route: routed.route,
                    report_route_run_id: routed.report_route_run_id,
                    model_version_id: routed.model_version_id,
                    model_run_id,
                    candidate: routed.candidate.clone(),
                    trade_policy: provenance,
                    trade_policy_cohort: cohort.clone(),
                    entry_limit_price: limit_price,
                },
            ));
    }
    unique_budget_tiers(by_budget, routed)
}

fn validate_candidate_route(
    market: &SelectedMarket,
    routed: &RoutedCandidate,
    route: &ReadyRoute,
) -> QuantResult<u64> {
    if BuyModelRoute::from(market.category) != routed.route
        || routed.report_route_run_id != route.report_route_run_id
        || routed.model_version_id != route.active.model_version_id
    {
        return Err(ReportError::InvariantViolation {
            stage: "economic_tier",
            detail: "candidate, selected market, and Route readiness disagree".to_owned(),
        }
        .into());
    }
    let horizon = u64::try_from(route.contract.prediction_horizon_secs).map_err(|error| {
        ReportError::RouteReadiness {
            route: routed.route.as_str().to_owned(),
            detail: format!("prediction horizon is invalid: {error}"),
        }
    })?;
    if routed.candidate.suggested_horizon_secs != horizon {
        return Err(ReportError::RouteReadiness {
            route: routed.route.as_str().to_owned(),
            detail: format!(
                "candidate {} horizon differs from the promoted contract",
                routed.candidate.signal_candidate_id
            ),
        }
        .into());
    }
    Ok(horizon)
}

fn unique_budget_tiers(
    by_budget: BTreeMap<Usd, Vec<(ExecutableTierSeed, TierSource)>>,
    routed: &RoutedCandidate,
) -> QuantResult<Vec<(ExecutableTierSeed, TierSource)>> {
    let mut result = Vec::new();
    for (budget, mut matches) in by_budget {
        if matches.len() != 1 {
            return Err(ReportError::RouteReadiness {
                route: routed.route.as_str().to_owned(),
                detail: format!(
                    "candidate {} cash tier {budget} resolves to {} Trade Policy cohorts",
                    routed.candidate.signal_candidate_id,
                    matches.len()
                ),
            }
            .into());
        }
        result.push(
            matches
                .pop()
                .ok_or_else(|| ReportError::InvariantViolation {
                    stage: "economic_tier",
                    detail: "unique Trade Policy cohort disappeared".to_owned(),
                })?,
        );
    }
    Ok(result)
}

struct TierLineageInput<'a> {
    context: &'a BuildContext,
    capture: &'a MarketDecisionCapture,
    routed: &'a RoutedCandidate,
    route: &'a ReadyRoute,
    cohort: &'a TradePolicyCohort,
    cohort_index: u32,
    fee_schedule: &'a PitFeeSchedule,
    portfolio: &'a PromotedPortfolioContext,
}

fn tier_lineage_hash(input: &TierLineageInput<'_>) -> QuantResult<ContentHash> {
    #[derive(Serialize)]
    struct Preimage<'a> {
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        decision_at: DateTime<Utc>,
        route: BuyModelRoute,
        report_route_run_id: ReportRouteRunId,
        model_version_id: ModelVersionId,
        model_run_id: ModelRunId,
        signal_candidate_id: SignalCandidateId,
        book_snapshot_hash: ContentHash,
        fee_schedule_hash: ContentHash,
        trade_policy_hash: ContentHash,
        cohort_index: u32,
        cohort: &'a TradePolicyCohort,
        scenario_model_hash: ContentHash,
    }
    let model_run_id = input
        .route
        .model_run_id
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "economic_tier",
            detail: "candidate Route has no model run lineage".to_owned(),
        })?;
    let book_snapshot_hash = input
        .capture
        .book_snapshot_ref_for(&input.routed.candidate.token_id)
        .map(|reference| reference.content_hash)
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "economic_tier",
            detail: "candidate-side book reference is missing".to_owned(),
        })?;
    Ok(CanonicalDigest::content_hash_typed(
        "quant-pivot/economic-tier-source",
        1,
        &Preimage {
            decision_policy_snapshot_id: input.context.version.decision_policy_snapshot_id,
            decision_at: input.context.boundary.decision_at(),
            route: input.routed.route,
            report_route_run_id: input.routed.report_route_run_id,
            model_version_id: input.routed.model_version_id,
            model_run_id,
            signal_candidate_id: input.routed.candidate.signal_candidate_id,
            book_snapshot_hash,
            fee_schedule_hash: input.fee_schedule.schedule_hash,
            trade_policy_hash: input.route.trade_policy.content_hash,
            cohort_index: input.cohort_index,
            cohort: input.cohort,
            scenario_model_hash: input.portfolio.scenario_model.content_hash,
        },
    )?)
}

struct ScenarioLegAccumulator {
    route: BuyModelRoute,
    market_id: MarketId,
    token_id: TokenId,
    outcome_side: OutcomeSide,
    payout_distribution: CalibratedPayoutDistribution,
    observed_exit_capacity_shares: Shares,
    base_capital_release_secs: u64,
    lineage_hashes: Vec<ContentHash>,
}

fn materialize_portfolio_scenario(
    context: &BuildContext,
    account: &AccountSnapshot,
    represented_routes: &RepresentedRouteSet,
    promoted: Option<&PromotedPortfolioContext>,
    seeded_tiers: &[SeededTier],
) -> QuantResult<Option<SealedPortfolioScenarioArtifact>> {
    if represented_routes.is_empty() {
        if !seeded_tiers.is_empty()
            || account
                .positions
                .iter()
                .any(|position| position.size.is_positive())
        {
            return Err(ReportError::ScenarioArtifact {
                detail: "economic exposure exists without a represented Route".to_owned(),
            }
            .into());
        }
        return Ok(None);
    }
    let promoted = promoted.ok_or_else(|| ReportError::ScenarioArtifact {
        detail: "represented Routes exist without a promoted scenario model".to_owned(),
    })?;
    let scenario_model = VerifiedPortfolioScenarioModel::verify(
        &promoted.scenario_model_binding,
        &promoted.scenario_model,
        represented_routes,
    )?;
    let accumulators = scenario_accumulators(account, seeded_tiers)?;
    let mut legs = Vec::with_capacity(accumulators.len());
    for mut accumulator in accumulators {
        accumulator.lineage_hashes.sort_unstable();
        accumulator.lineage_hashes.dedup();
        let lineage_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/report-scenario-leg",
            1,
            &(
                accumulator.route,
                &accumulator.market_id,
                &accumulator.token_id,
                accumulator.outcome_side,
                accumulator.payout_distribution,
                accumulator.observed_exit_capacity_shares,
                accumulator.base_capital_release_secs,
                &accumulator.lineage_hashes,
            ),
        )?;
        legs.push(PortfolioScenarioLegInput {
            route: accumulator.route,
            market_id: accumulator.market_id,
            token_id: accumulator.token_id,
            outcome_side: accumulator.outcome_side,
            calibrated_payout_distribution: accumulator.payout_distribution,
            observed_exit_capacity_shares: accumulator.observed_exit_capacity_shares,
            base_capital_release_secs: accumulator.base_capital_release_secs,
            lineage_hash,
        });
    }
    let input_universe_hash = CanonicalDigest::content_hash_typed(
        "quant-pivot/report-scenario-input-universe",
        1,
        &(
            context.version.decision_policy_snapshot_id,
            context.boundary.decision_at(),
            represented_routes.digest,
            promoted.scenario_model.content_hash,
            account.as_of,
            &legs,
        ),
    )?;
    PortfolioScenarioGenerator::generate(PortfolioScenarioGenerationInput {
        model_contract: &scenario_model,
        decision_at: context.boundary.decision_at(),
        visibility: PortfolioScenarioVisibility::PointInTime,
        input_universe_hash,
        legs: &legs,
    })
    .map(Some)
}

fn scenario_accumulators(
    account: &AccountSnapshot,
    seeded_tiers: &[SeededTier],
) -> QuantResult<Vec<ScenarioLegAccumulator>> {
    let mut accumulators: BTreeMap<_, ScenarioLegAccumulator> = BTreeMap::new();
    for seeded in seeded_tiers {
        let payout_distribution = seeded
            .source
            .candidate
            .payout_distribution
            .ok_or_else(|| {
            ReportError::ScenarioArtifact {
                detail: format!(
                    "candidate {} has no calibrated payout distribution for scenario materialization",
                    seeded.source.candidate.signal_candidate_id
                ),
            }
        })?;
        let release_secs = seeded.source.trade_policy_cohort.vertical_barrier_secs;
        let key = (
            seeded.seed.route,
            seeded.seed.market_id.clone(),
            seeded.seed.token_id.clone(),
            seeded.seed.outcome_side.as_str(),
        );
        match accumulators.entry(key) {
            Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                if existing.payout_distribution != payout_distribution
                    || existing.base_capital_release_secs != release_secs
                    || existing.observed_exit_capacity_shares
                        != seeded.seed.observed_exit_capacity_shares
                {
                    return Err(ReportError::ScenarioArtifact {
                        detail: format!(
                            "market {} has inconsistent payout, exit-capacity, or Trade Policy release inputs",
                            seeded.seed.market_id
                        ),
                    }
                    .into());
                }
                existing
                    .lineage_hashes
                    .push(seeded.seed.source_lineage_hash);
            }
            Entry::Vacant(entry) => {
                entry.insert(ScenarioLegAccumulator {
                    route: seeded.seed.route,
                    market_id: seeded.seed.market_id.clone(),
                    token_id: seeded.seed.token_id.clone(),
                    outcome_side: seeded.seed.outcome_side,
                    payout_distribution,
                    observed_exit_capacity_shares: seeded.seed.observed_exit_capacity_shares,
                    base_capital_release_secs: release_secs,
                    lineage_hashes: vec![seeded.seed.source_lineage_hash],
                });
            }
        }
    }
    for position in account
        .positions
        .iter()
        .filter(|position| position.size.is_positive())
    {
        let outcome_side = parse_position_outcome(&position.outcome)?;
        let route = BuyModelRoute::from(position.category);
        let key = (
            route,
            position.market_id.clone(),
            position.token_id.clone(),
            outcome_side.as_str(),
        );
        let leg = accumulators.get_mut(&key);
        let Some(leg) = leg else {
            return Err(ReportError::ScenarioArtifact {
                detail: format!(
                    "open position {} has no calibrated Route/Trade Policy scenario input",
                    position.token_id
                ),
            }
            .into());
        };
        leg.lineage_hashes.push(CanonicalDigest::content_hash_typed(
            "quant-pivot/scenario-open-position",
            1,
            position,
        )?);
    }
    Ok(accumulators.into_values().collect())
}

fn parse_position_outcome(outcome: &str) -> QuantResult<OutcomeSide> {
    match outcome.trim().to_ascii_lowercase().as_str() {
        "yes" => Ok(OutcomeSide::Yes),
        "no" => Ok(OutcomeSide::No),
        _ => Err(ReportError::ScenarioArtifact {
            detail: format!("open position outcome `{outcome}` is not canonical Yes/No"),
        }
        .into()),
    }
}

fn finalize_economic_tiers(
    seeded_tiers: Vec<SeededTier>,
    scenario_artifact: Option<&SealedPortfolioScenarioArtifact>,
) -> QuantResult<(
    Vec<ExecutableEconomicTier>,
    HashMap<EconomicTierId, TierSource>,
)> {
    if seeded_tiers.is_empty() {
        return Ok((Vec::new(), HashMap::new()));
    }
    let artifact = scenario_artifact.ok_or_else(|| ReportError::ScenarioArtifact {
        detail: "executable entry tiers exist without a concrete scenario artifact".to_owned(),
    })?;
    let mut tiers = Vec::with_capacity(seeded_tiers.len());
    let mut sources = HashMap::with_capacity(seeded_tiers.len());
    for seeded in seeded_tiers {
        let tier = EconomicTierFactory::build(seeded.seed, artifact)?;
        if sources
            .insert(tier.economic_tier_id, seeded.source)
            .is_some()
        {
            return Err(ReportError::InvariantViolation {
                stage: "economic_tier",
                detail: format!("duplicate economic tier {}", tier.economic_tier_id),
            }
            .into());
        }
        tiers.push(tier);
    }
    Ok((tiers, sources))
}

struct PortfolioBuildInput<'a> {
    context: &'a BuildContext,
    selection: &'a MarketSelectionSnapshot,
    account: &'a AccountSnapshot,
    equity: &'a ReportEquitySnapshot,
    represented_routes: &'a RepresentedRouteSet,
    promoted: Option<&'a PromotedPortfolioContext>,
    scenario_artifact: Option<&'a SealedPortfolioScenarioArtifact>,
    tiers: &'a [ExecutableEconomicTier],
}

fn build_portfolio(input: &PortfolioBuildInput<'_>) -> QuantResult<PortfolioBuild> {
    let context = input.context;
    let selection = input.selection;
    let account = input.account;
    let equity = input.equity;
    let represented_routes = input.represented_routes;
    let tiers = input.tiers;
    let portfolio_plan_id = PortfolioPlanId::from_v7();
    let current_drawdown_usd = equity.current_drawdown_usd()?;
    let (existing, result, scenario_id, scenario_hash, scenario_json) =
        match (input.promoted, input.scenario_artifact) {
            (Some(promoted), Some(scenario_artifact)) => {
                let existing = ExistingPortfolioFactory::build(
                    account,
                    current_drawdown_usd,
                    scenario_artifact,
                )?;
                let result = GlobalPortfolioPlanner::solve_and_verify(GlobalPortfolioInput {
                    portfolio_plan_id,
                    account,
                    existing: &existing,
                    represented_routes,
                    scenario_model_binding: &promoted.scenario_model_binding,
                    scenario_artifact,
                    policy: &promoted.policy,
                    solver: &promoted.solver,
                    tiers,
                    top_n: context.top_n,
                })?;
                (
                    existing,
                    result,
                    Some(scenario_artifact.portfolio_scenario_artifact_id),
                    Some(scenario_artifact.content_hash),
                    Some(scenario_artifact.artifact().clone()),
                )
            }
            (None, None) => {
                if !represented_routes.is_empty()
                    || !tiers.is_empty()
                    || !account.positions.is_empty()
                {
                    return Err(ReportError::ScenarioArtifact {
                        detail: "economic exposure exists without a promoted scenario artifact"
                            .to_owned(),
                    }
                    .into());
                }
                (
                    ExistingPortfolioState {
                        existing_open_capital_usd: Usd::ZERO,
                        existing_open_recommendations: 0,
                        current_drawdown_usd,
                        scenario_cashflows: Vec::new(),
                        capital_occupancy: Vec::new(),
                    },
                    GlobalPortfolioResult {
                        plan: None,
                        selected: Vec::new(),
                        rejected: Vec::new(),
                    },
                    None,
                    None,
                    None,
                )
            }
            _ => {
                return Err(ReportError::ScenarioArtifact {
                detail:
                    "promoted scenario model and concrete report artifact must be present together"
                        .to_owned(),
            }
            .into());
            }
        };
    let decision_json = match &result.plan {
        Some(plan) => PortfolioDecisionResult::Optimized {
            plan: Box::new(plan.clone()),
        },
        None => PortfolioDecisionResult::ZeroCandidates {
            rejected_tier_count: count_u32(result.rejected.len(), "portfolio.rejected_tier_count")?,
            evidence_hash: zero_candidate_evidence(
                represented_routes,
                scenario_hash,
                tiers,
                result.rejected.len(),
            )?,
        },
    };
    Ok(PortfolioBuild {
        row: NewPortfolioPlan {
            portfolio_plan_id,
            account_snapshot_id: equity.account_snapshot.account_snapshot_id,
            decision_policy_snapshot_id: context.version.decision_policy_snapshot_id,
            market_selection_id: selection.market_selection_id,
            decision_at: context.boundary.decision_at(),
            represented_routes_json: represented_routes.clone(),
            scenario_artifact_id: scenario_id,
            scenario_artifact_hash: scenario_hash,
            scenario_artifact_json: scenario_json,
            portfolio_policy_json: context.config.execution_risk.portfolio.clone(),
            existing_state_json: existing,
            decision_json,
        },
        selected: result.selected,
        rejections: result.rejected,
    })
}

fn report_tier_rejections(
    tiers: &[ExecutableEconomicTier],
    rejected: &[TierAdmissionRejection],
) -> QuantResult<Vec<ReportTierRejection>> {
    let markets = tiers
        .iter()
        .map(|tier| (tier.economic_tier_id, tier.market_id.clone()))
        .collect::<HashMap<_, _>>();
    rejected
        .iter()
        .map(|rejection| {
            Ok(ReportTierRejection {
                economic_tier_id: rejection.economic_tier_id,
                market_id: markets
                    .get(&rejection.economic_tier_id)
                    .cloned()
                    .ok_or_else(|| ReportError::InvariantViolation {
                        stage: "portfolio_rejection",
                        detail: format!(
                            "rejected tier {} is absent from solver input",
                            rejection.economic_tier_id
                        ),
                    })?,
                code: rejection.code,
            })
        })
        .collect()
}

fn update_route_funnels(
    routes: &mut [ReadyRoute],
    tiers: &[ExecutableEconomicTier],
    portfolio: &PortfolioBuild,
) -> QuantResult<()> {
    let rejected = portfolio
        .rejections
        .iter()
        .map(|rejection| rejection.economic_tier_id)
        .collect::<HashSet<_>>();
    for route in routes {
        route.admitted_economic_tiers = count_u32(
            tiers
                .iter()
                .filter(|tier| tier.route == route.active.route)
                .filter(|tier| !rejected.contains(&tier.economic_tier_id))
                .count(),
            "route.admitted_economic_tiers",
        )?;
        route.selected_recommendations = count_u32(
            portfolio
                .selected
                .iter()
                .filter(|planned| planned.tier.route == route.active.route)
                .count(),
            "route.selected_recommendations",
        )?;
    }
    Ok(())
}

fn planned_recommendations(
    selected: &[PlannedEconomicTier],
    sources: &HashMap<EconomicTierId, TierSource>,
) -> QuantResult<Vec<PlannedReportRecommendation>> {
    selected
        .iter()
        .enumerate()
        .map(|(index, planned)| {
            let source = sources.get(&planned.tier.economic_tier_id).ok_or_else(|| {
                ReportError::InvariantViolation {
                    stage: "portfolio_ranking",
                    detail: format!(
                        "selected tier {} has no candidate lineage",
                        planned.tier.economic_tier_id
                    ),
                }
            })?;
            Ok(PlannedReportRecommendation {
                rank: u32::try_from(index)
                    .ok()
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "recommendation.rank",
                        detail: "selected rank overflowed u32".to_owned(),
                    })?,
                route: source.route,
                report_route_run_id: source.report_route_run_id,
                model_version_id: source.model_version_id,
                model_run_id: source.model_run_id,
                candidate: source.candidate.clone(),
                tier: planned.tier.clone(),
                trade_policy: source.trade_policy.clone(),
                trade_policy_cohort: source.trade_policy_cohort.clone(),
                entry_limit_price: source.entry_limit_price,
            })
        })
        .collect()
}

fn route_rows(
    request: &BuildReportRequest,
    routes: &[ReadyRoute],
    finished_at: DateTime<Utc>,
) -> Vec<NewReportRouteRun> {
    routes
        .iter()
        .map(|route| {
            let lineage = RouteModelLineage {
                model_version_id: route.active.model_version_id,
                model_run_id: route.model_run_id,
                calibration_artifact_id: route.contract.calibration_artifact_id,
                trade_policy_artifact_id: route.contract.trade_policy_artifact_id,
                research_profile_artifact_id: route.contract.research_profile_artifact_id.clone(),
                research_profile_ref: route.contract.research_profile_ref.clone(),
                prediction_horizon_secs: route.contract.prediction_horizon_secs,
                feature_contract_digest: route.contract.feature_contract_digest,
                pit_lineage_digest: route.contract.pit_lineage_digest,
                serving_contract_digest: route.contract.serving_contract_hash,
            };
            NewReportRouteRun {
                report_route_run_id: route.report_route_run_id,
                report_run_id: request.report_run_id,
                route: route.active.route,
                outcome: if route.selected_recommendations == 0 {
                    RouteRunOutcome::ZeroCandidates
                } else {
                    RouteRunOutcome::Ready
                },
                model_version_id: Some(route.active.model_version_id),
                model_run_id: route.model_run_id,
                calibration_artifact_id: Some(route.contract.calibration_artifact_id),
                trade_policy_artifact_id: Some(route.contract.trade_policy_artifact_id),
                research_profile_artifact_id: Some(
                    route.contract.research_profile_artifact_id.clone(),
                ),
                lineage_json: Some(lineage),
                funnel_json: RouteCandidateFunnel {
                    eligible_markets: route.eligible_markets,
                    feature_complete_markets: route.feature_complete_markets,
                    calibrated_candidates: route.calibrated_candidates,
                    admitted_economic_tiers: route.admitted_economic_tiers,
                    selected_recommendations: route.selected_recommendations,
                },
                diagnostic_code: None,
                finished_at,
            }
        })
        .collect()
}

struct EmptyContextInput<'a> {
    initial: &'a MarketSelectionSnapshot,
    selection: &'a MarketSelectionSnapshot,
    features: &'a FeaturePipelineResult,
    candidates: &'a [RoutedCandidate],
    tiers: &'a [ExecutableEconomicTier],
    planned: &'a [PlannedReportRecommendation],
    account: &'a AccountSnapshot,
    config: &'a DecisionPolicySnapshot,
}

fn empty_context(input: &EmptyContextInput<'_>) -> QuantResult<Option<EmptyReportContext>> {
    let EmptyContextInput {
        initial,
        selection,
        features,
        candidates,
        tiers,
        planned,
        account,
        config,
    } = input;
    if !planned.is_empty() {
        return Ok(None);
    }
    let reason = if initial.included.is_empty() && account.positions.is_empty() {
        EmptyReportReason::EmptySelection
    } else if selection.included.is_empty() || features.accepted.is_empty() {
        EmptyReportReason::InsufficientDataQuality
    } else if candidates.is_empty() || tiers.is_empty() {
        EmptyReportReason::NoPositiveSignal
    } else if account.available_usd
        <= Usd::new(
            config
                .execution_risk
                .portfolio
                .budget
                .cash_reserve_usd
                .value,
        )
    {
        EmptyReportReason::AvailableCashExhausted
    } else {
        EmptyReportReason::PortfolioBudgetExhausted
    };
    let rejected = initial
        .excluded
        .len()
        .checked_add(features.rejected.len())
        .and_then(|value| value.checked_add(candidates.len().saturating_sub(tiers.len())))
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "report.empty_rejected_count",
            detail: "empty-report rejected count overflowed usize".to_owned(),
        })?;
    Ok(Some(EmptyReportContext {
        reason,
        candidate_count: count_u32(candidates.len(), "report.empty_candidate_count")?,
        rejected_count: count_u32(rejected, "report.empty_rejected_count")?,
        warnings: Vec::new(),
    }))
}

fn represented_routes(
    selection: &MarketSelectionSnapshot,
    account: &AccountSnapshot,
) -> QuantResult<RepresentedRouteSet> {
    RepresentedRouteSet::from_categories(
        selection
            .included
            .iter()
            .map(|market| market.category)
            .chain(account.positions.iter().map(|position| position.category)),
    )
    .map_err(|error| QuantError::config(format!("derive represented Route set: {error}")))
}

fn merged_requirements(routes: &[ReadyRoute]) -> ModelFeatureRequirements {
    let mut merged = ModelFeatureRequirements::default();
    for route in routes {
        merged.merge(route.active.model_requirements.clone());
    }
    merged
}

fn route_vectors(
    markets: &[SelectedMarket],
    vectors: &HashMap<MarketId, &FeatureVector>,
    persisted: &HashMap<MarketId, FeatureVectorId>,
) -> QuantResult<(Vec<FeatureVector>, Vec<FeatureVectorId>)> {
    let mut selected_vectors = Vec::with_capacity(markets.len());
    let mut selected_ids = Vec::with_capacity(markets.len());
    for market in markets {
        selected_vectors.push(
            vectors
                .get(&market.market_id)
                .copied()
                .cloned()
                .ok_or_else(|| ReportError::InvariantViolation {
                    stage: "route_features",
                    detail: format!("market {} has no accepted feature vector", market.market_id),
                })?,
        );
        selected_ids.push(*persisted.get(&market.market_id).ok_or_else(|| {
            ReportError::InvariantViolation {
                stage: "route_features",
                detail: format!("market {} has no persisted feature id", market.market_id),
            }
        })?);
    }
    Ok((selected_vectors, selected_ids))
}

impl FeaturePipelineResult {
    fn route_evidence(&self) -> QuantResult<FeatureEvidenceCommitment> {
        Ok(self
            .feature_evidence
            .clone()
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "route_features",
                detail: "accepted route vectors have no durable feature commitment".to_owned(),
            })?)
    }

    fn vector_ids_by_market(&self) -> QuantResult<HashMap<MarketId, FeatureVectorId>> {
        if self.persisted.len() != self.accepted.len() {
            return Err(ReportError::InvariantViolation {
                stage: "feature_lineage",
                detail: "accepted feature vectors and persisted ids are not aligned".to_owned(),
            }
            .into());
        }
        Ok(self
            .persisted
            .iter()
            .zip(&self.accepted)
            .map(|(info, vector)| (vector.market_id.clone(), info.feature_vector_id))
            .collect())
    }
}

impl ReportEquitySnapshot {
    fn current_drawdown_usd(&self) -> QuantResult<Usd> {
        let drawdown = self
            .equity_snapshot
            .high_water_mark_usd
            .inner()
            .checked_sub(self.equity_snapshot.capital_base_usd.inner())
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "portfolio.current_drawdown_usd",
                detail: "high-water mark minus capital base overflowed Decimal".to_owned(),
            })?
            .max(Decimal::ZERO);
        Ok(Usd::new(drawdown))
    }
}

fn zero_candidate_evidence(
    routes: &RepresentedRouteSet,
    scenario_hash: Option<ContentHash>,
    tiers: &[ExecutableEconomicTier],
    rejected_count: usize,
) -> QuantResult<ContentHash> {
    #[derive(Serialize)]
    struct Preimage<'a> {
        route_set_digest: ContentHash,
        scenario_hash: Option<ContentHash>,
        tier_ids: Vec<EconomicTierId>,
        rejected_count: u32,
        ordered_routes: &'a [BuyModelRoute],
    }
    Ok(CanonicalDigest::content_hash_typed(
        "quant-pivot/zero-candidate-portfolio",
        1,
        &Preimage {
            route_set_digest: routes.digest,
            scenario_hash,
            tier_ids: tiers.iter().map(|tier| tier.economic_tier_id).collect(),
            rejected_count: count_u32(rejected_count, "portfolio.rejected_tier_count")?,
            ordered_routes: &routes.routes,
        },
    )?)
}

fn route_run_id(context: &BuildContext, route: BuyModelRoute) -> QuantResult<ReportRouteRunId> {
    let hash = CanonicalDigest::content_hash_typed(
        "quant-pivot/report-route-run",
        1,
        &(
            context.report_run_id,
            context.version.decision_policy_snapshot_id,
            context.boundary.decision_at(),
            route,
        ),
    )?;
    Ok(ReportRouteRunId::from_content_hash(&hash))
}

fn route_error(route: BuyModelRoute, error: &QuantError) -> QuantError {
    ReportError::RouteReadiness {
        route: route.as_str().to_owned(),
        detail: error.to_string(),
    }
    .into()
}

fn resolve_top_n(
    request: &BuildReportRequest,
    config: &DecisionPolicySnapshot,
) -> QuantResult<u32> {
    let top_n = match &request.trigger {
        ReportTrigger::Scheduled { schedule_id } => {
            config
                .report_schedule
                .schedules
                .iter()
                .find(|schedule| schedule.schedule_id == *schedule_id)
                .ok_or_else(|| {
                    QuantError::config(format!("unknown report schedule {schedule_id}"))
                })?
                .top_n
        }
        ReportTrigger::AdHoc { .. } => request
            .top_n_override
            .unwrap_or(config.recommendation.reports.ad_hoc_default_top_n),
    };
    if top_n == 0 || top_n > config.recommendation.reports.max_top_n {
        return Err(QuantError::config(format!(
            "report top_n {top_n} outside 1..={}",
            config.recommendation.reports.max_top_n
        )));
    }
    Ok(top_n)
}

fn resolve_knowledge_lag(
    request: &BuildReportRequest,
    config: &DecisionPolicySnapshot,
) -> QuantResult<u64> {
    match &request.trigger {
        ReportTrigger::Scheduled { schedule_id } => {
            let schedule = config
                .report_schedule
                .schedules
                .iter()
                .find(|schedule| schedule.schedule_id == *schedule_id)
                .ok_or_else(|| {
                    QuantError::config(format!("unknown report schedule {schedule_id}"))
                })?;
            if !schedule.enabled {
                return Err(QuantError::config(format!(
                    "report schedule {schedule_id} is disabled"
                )));
            }
            Ok(schedule.knowledge_lag_secs)
        }
        ReportTrigger::AdHoc { .. } => {
            if !config.recommendation.reports.ad_hoc_report_enabled {
                return Err(QuantError::config("ad-hoc report generation is disabled"));
            }
            Ok(request.knowledge_lag_secs_override.unwrap_or(
                config
                    .recommendation
                    .reports
                    .ad_hoc_default_knowledge_lag_secs,
            ))
        }
    }
}

fn enforce_candidate_ceiling(actual: usize, configured: u32) -> QuantResult<()> {
    let ceiling = usize::try_from(configured).map_err(|error| ReportError::NumericOverflow {
        field: "reports.hard_candidate_ceiling",
        detail: error.to_string(),
    })?;
    if actual > ceiling {
        return Err(ReportError::ResourceCapacityExceeded {
            resource: "catalog_visible_markets",
            actual,
            ceiling,
        }
        .into());
    }
    Ok(())
}

fn bounded_usize(value: u32) -> QuantResult<usize> {
    usize::try_from(value)
        .map_err(|error| ReportError::NumericOverflow {
            field: "reports.top_n",
            detail: error.to_string(),
        })
        .map_err(Into::into)
}

fn count_u32(count: usize, field: &'static str) -> QuantResult<u32> {
    u32::try_from(count)
        .map_err(|error| ReportError::NumericOverflow {
            field,
            detail: error.to_string(),
        })
        .map_err(Into::into)
}

impl BuildContext {
    fn empty_data_quality_snapshot(&self) -> NewReportDataQualitySnapshot {
        NewReportDataQualitySnapshot {
            report_data_quality_snapshot_id: ReportDataQualitySnapshotId::from_v7(),
            decision_at: self.boundary.decision_at(),
            decision_policy_snapshot_id: self.version.decision_policy_snapshot_id,
            tokens_json: ReportDataQualityTokens(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_models::{
        runtime_config::DecisionPolicySnapshot,
        types::{CorrelationId, ReportRunId},
    };

    use super::{BuildReportRequest, ReportTrigger, resolve_top_n};

    #[test]
    fn ad_hoc_uses_default() {
        let mut config = DecisionPolicySnapshot::default();
        config.recommendation.reports.ad_hoc_default_top_n = 17;
        let request = BuildReportRequest {
            report_run_id: ReportRunId::from_v7(),
            trigger: ReportTrigger::AdHoc {
                request_id: CorrelationId::new("fixture-request"),
            },
            trigger_time: Utc::now(),
            top_n_override: None,
            knowledge_lag_secs_override: None,
        };
        assert_eq!(resolve_top_n(&request, &config).expect("top_n"), 17);
    }
}
