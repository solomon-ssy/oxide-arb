//! Atomic global report orchestration across every represented model Route.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, btree_map::Entry},
    iter::once,
    sync::Arc,
};

use chrono::{DateTime, Days, Utc};
use quant_pivot_error::{QuantError, QuantResult, report::ReportError};
use quant_pivot_models::{
    config::PortfolioSolverDeployConfig,
    domain::{
        data_plane::{
            DecisionBoundary, DecisionClock, DecisionSource, ExchangeHistoryFrontier,
            HistoryServingHeadSeal,
        },
        governance::DecisionPolicySnapshotInfo,
        order::PolymarketOrderRules,
        quant::{
            EntryExecutionEconomics, ExecutableEconomicTier, ExistingPortfolioState,
            FeatureVectorInfo, MarketCandidate, NewPortfolioPlan, NewReportDataQualitySnapshot,
            NewReportRouteRun, PortfolioDecisionResult, PortfolioScenarioVisibility,
            RepresentedRouteSet, RouteCandidateFunnel, RouteHistoryLineage, RouteModelLineage,
            RouteRunOutcome, TradePolicyArtifactInfo,
        },
    },
    enums::quant::{EmptyReportReason, FillRequirement, OutcomeSide, TradePolicyStatus},
    hashing::CanonicalDigest,
    runtime_config::{BuyModelRoute, DecisionPolicySnapshot, MakerRebatePolicy},
    types::{
        Bps, ContentHash, DecisionPolicySnapshotId, EconomicTierId, EntryConditionTemplate,
        EntryOrderTemplate, ExecutionAccountId, FeatureVectorId, MakerRebateDelayBasis,
        MakerRebateObjectiveStatus, MakerRebateValuationEvidence, MakerRebateValuationHealth,
        MarketId, ModelRunId, ModelVersionId, PortfolioPlanId, Price, ReportDataQualitySnapshotId,
        ReportDataQualityTokens, ReportFunnelDiagnostics, ReportRouteRunId, ReportRunId,
        ServingAuthority, Shares, SignalCandidateId, TokenId, TradePolicyCohort,
        TradePolicyCohortProvenance, Usd, calibration::CalibratedPayoutDistribution,
    },
};
use quant_pivot_repository::traits::{
    ExchangeHistoryRepository, MarketSelectionRepository, PolicyRepository, TradePolicyRepository,
    VenueIncentiveRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    execution_semantics::{
        PitFeeSchedule, PitMakerRebateEvidence, PitMakerRebateUnavailableReason,
        PitMarketExecutionEconomics, aggressive_buy_limit, passive_buy_limit,
    },
    features::{FeatureVector, MarketDecisionCapture, ResolvedBook},
    hashing::ResearchHasher,
    model::{CalibrationArtifactLoader, ModelArtifact, SignalCandidate},
    portfolio::{
        AccountSnapshot, EconomicTierFactory, ExecutableCashTierSeedFactory,
        ExecutableCashTierSeedInput, ExecutablePassiveTierSeedFactory,
        ExecutablePassiveTierSeedInput, ExecutableTierSeed, ExistingPortfolioFactory,
        GlobalPortfolioInput, GlobalPortfolioPlanner, GlobalPortfolioResult,
        MakerRebateValuationFactory, MakerRebateValuationInput, PlannedEconomicTier,
        PortfolioScenarioGenerationInput, PortfolioScenarioGenerator, PortfolioScenarioLegInput,
        SealedPortfolioScenarioArtifact, TierAdmissionRejection, TierAdmissionRejectionCode,
        TierSeedBuild, VerifiedPortfolioScenarioModel,
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
        BuildReportRequest, ComposedReport, EconomicTierBuildRejection, EmptyReportContext,
        PlannedRecommendationContract, PlannedReportRecommendation, ReportTierRejection,
        ReportTrigger,
    },
    universe::{ReportUniverseContract, ReportUniverseRoute},
};
use crate::{
    governance::{RuntimeControlsHandle, resolve_return_model_calibration},
    ingest::data_pipeline::MicrostructureCommitBarrier,
    observability::{metrics_hub::MetricsHub, serving_evidence::FeatureEvidenceCommitment},
    prefetch::market_candidates::{DecisionSnapshotSource, MarketCandidateProvider},
    service::{
        account::AccountProviderFactory,
        equity::{DrawdownProvider, ReportEquitySnapshot},
        feature_pipeline::{FeaturePipelineRequest, FeaturePipelineService, RejectedMarket},
        market_selection::map_snapshot_to_model,
        model_runner::{
            ActiveModelRequirements, ModelMarketDecision, ModelRunRequest, ModelRunner,
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
    pub exchange_history_repo: Arc<dyn ExchangeHistoryRepository>,
    pub venue_incentive_repo: Arc<dyn VenueIncentiveRepository>,
    pub execution_account_id: ExecutionAccountId,
    pub venue_incentive_stale_secs: u64,
    pub metrics: Arc<MetricsHub>,
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

struct ReportUniversePlan {
    active: Vec<ActiveModelRequirements>,
    contract: ReportUniverseContract,
    serving_head: HistoryServingHeadSeal,
}

struct EconomicTierSeedBuild {
    admitted: Vec<SeededTier>,
    rejections: Vec<EconomicTierBuildRejection>,
}

struct CandidateTierBuild {
    admitted: Vec<(ExecutableTierSeed, TierSource)>,
    rejection: Option<EconomicTierBuildRejection>,
}

struct FullL2TierBuild {
    admitted: Vec<(ExecutableTierSeed, TierSource)>,
    passive_rejection: Option<PitMakerRebateUnavailableReason>,
    minimum_rejection: Option<(Shares, Shares)>,
}

enum PolicyTierBuild {
    Ready {
        seed: Box<ExecutableTierSeed>,
        price: Price,
        limit_price: Price,
    },
    Unfilled,
    BelowMinimum {
        requested: Shares,
        minimum: Shares,
    },
}

struct ReadyRoute {
    active: ActiveModelRequirements,
    contract: PromotedRouteContract,
    trade_policy: Option<TradePolicyArtifactInfo>,
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
    contract: PlannedRecommendationContract,
    entry_limit_price: Price,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TierSourceKey {
    report_route_run_id: ReportRouteRunId,
    candidate_id: SignalCandidateId,
    tier_ordinal: u32,
}

impl TierSourceKey {
    const fn from_tier(tier: &ExecutableEconomicTier) -> Self {
        Self {
            report_route_run_id: tier.report_route_run_id,
            candidate_id: tier.candidate_id,
            tier_ordinal: tier.tier_ordinal,
        }
    }
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

struct RouteFeatureRound {
    route: BuyModelRoute,
    accepted: Vec<FeatureVector>,
    persisted: Vec<FeatureVectorInfo>,
    feature_evidence: Option<FeatureEvidenceCommitment>,
}

struct ReportFeatureResults {
    routes: Vec<RouteFeatureRound>,
    rejected: Vec<RejectedMarket>,
    captures: HashMap<MarketId, MarketDecisionCapture>,
    data_quality_snapshot: NewReportDataQualitySnapshot,
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
        let result = Box::pin(self.build_report(request)).await;
        if let Err(QuantError::Report(error)) = &result {
            match error {
                ReportError::UnmodeledOpenExposure { route, .. } => {
                    self.deps.metrics.record_unmodeled_exposure(route);
                }
                ReportError::HistoryWindowInvalidated { .. } => {
                    self.deps
                        .metrics
                        .record_history_window_invalidation("serving_head");
                }
                _ => {}
            }
        }
        result
    }
}

impl DefaultReportBuilder {
    async fn build_report(&self, request: BuildReportRequest) -> QuantResult<ComposedReport> {
        self.require_report_ready()?;
        let (context, universe) = self.prepare_report_context(&request).await?;
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
        let maker_rebate_valuation = self.maker_rebate_valuation(&context).await?;

        let requirements = universe.contract.requirements.clone();
        let selection = self
            .select_snapshot(&context, requirements, batch.candidates.clone(), &universe)
            .await?;
        self.deps
            .metrics
            .record_route_not_activated(selection.exclusion_summary.route_not_activated_count);
        self.persist_selection(&selection, &batch.candidates)
            .await?;
        let represented_routes = represented_routes(&selection, &account, &universe)?;
        let mut routes = self
            .resolve_ready_routes(&context, &represented_routes, &selection, &universe)
            .await?;
        let portfolio_context = self
            .load_portfolio_context(&context, &represented_routes, &routes)
            .await?;

        let features = self
            .build_features(
                &context,
                &selection,
                batch.snapshot_source.as_ref(),
                &routes,
                &universe,
            )
            .await?;
        let (routed_candidates, model_decisions) = self
            .run_route_models(&context, &selection, &features, &mut routes)
            .await?;
        let tier_build = build_economic_tier_seeds(
            &context,
            &selection,
            &features.captures,
            &routes,
            &routed_candidates,
            portfolio_context.as_ref(),
            &maker_rebate_valuation,
        )?;
        let scenario_artifact = materialize_portfolio_scenario(
            &context,
            &account,
            &represented_routes,
            portfolio_context.as_ref(),
            &tier_build.admitted,
        )?;
        let (tiers, tier_sources) =
            finalize_economic_tiers(tier_build.admitted, scenario_artifact.as_ref())?;
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
        self.record_rebate_diagnostics(&maker_rebate_valuation, &tier_build.rejections, &portfolio);
        let tier_rejections = report_tier_rejections(
            &tiers,
            &portfolio.rejections,
            &context.config,
            scenario_artifact.as_ref(),
        )?;
        update_route_funnels(&mut routes, &tiers, &portfolio)?;
        let planned = planned_recommendations(&portfolio.selected, &tier_sources)?;
        // Every derived report fact is anchored to the database-owned frozen
        // decision clock. Using the process wall clock here makes the same
        // report-run preimage non-replayable and can place availability after
        // the PostgreSQL commit clock when host/container clocks drift.
        let published_at = context.boundary.decision_at();
        let route_runs = route_rows(&request, &routes, &universe, published_at);
        let empty = empty_context(&EmptyContextInput {
            initial: &selection,
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
        self.deps
            .feature_pipeline
            .validate_execution_history_seal(&universe.serving_head)
            .await
            .map_err(|error| ReportError::HistoryWindowInvalidated {
                seal_id: universe.serving_head.seal.serving_head_seal_id.into(),
                detail: error.to_string(),
            })?;
        self.deps.composer.compose(ComposeReportInput {
            report_run_id: request.report_run_id,
            trigger: &request.trigger,
            trigger_key,
            decision_at: context.boundary.decision_at(),
            published_at,
            decision_policy_snapshot_id: context.version.decision_policy_snapshot_id,
            runtime_config: &context.config,
            selection: &selection,
            account: &account,
            account_snapshot: equity.account_snapshot,
            equity_snapshot: equity.equity_snapshot,
            portfolio_plan: portfolio.row,
            route_runs,
            tiers: &tiers,
            planned: &planned,
            tier_rejections: &tier_rejections,
            tier_build_rejections: &tier_build.rejections,
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

    fn require_report_ready(&self) -> QuantResult<()> {
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
        Ok(())
    }

    async fn prepare_report_context(
        &self,
        request: &BuildReportRequest,
    ) -> QuantResult<(BuildContext, ReportUniversePlan)> {
        let mut context = self.prepare_context(request).await?;
        let universe = self.report_universe_plan(&context).await?;
        context.boundary = context.boundary.with_source_watermark(
            DecisionSource::FinalizedExecution,
            universe.serving_head.seal.effective_through_at,
        )?;
        Ok((context, universe))
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

    async fn maker_rebate_valuation(
        &self,
        context: &BuildContext,
    ) -> QuantResult<MakerRebateValuationEvidence> {
        let as_of = context.boundary.decision_at();
        let policy = &context.config.execution_risk.maker_rebate;
        let Some(health_to) = as_of.date_naive().checked_sub_days(Days::new(1)) else {
            return Self::unavailable_rebate_valuation(
                as_of,
                policy,
                "health window end underflow",
            );
        };
        let Some(health_from) = health_to.checked_sub_days(Days::new(29)) else {
            return Self::unavailable_rebate_valuation(
                as_of,
                policy,
                "health window start underflow",
            );
        };
        let scans = self
            .deps
            .venue_incentive_repo
            .scans(&self.deps.execution_account_id, health_from, health_to)
            .await;
        let events = self
            .deps
            .venue_incentive_repo
            .maker_valuation_events(&self.deps.execution_account_id, as_of)
            .await;
        match (scans, events) {
            (Ok(scans), Ok(events)) => {
                MakerRebateValuationFactory::build(&MakerRebateValuationInput {
                    as_of,
                    stale_after_secs: self.deps.venue_incentive_stale_secs,
                    health_from,
                    health_to,
                    scans: &scans,
                    events: &events,
                    policy,
                })
                .or_else(|error| {
                    Self::unavailable_rebate_valuation(
                        as_of,
                        policy,
                        &format!("invalid ledger: {error}"),
                    )
                })
            }
            (scans, events) => Self::unavailable_rebate_valuation(
                as_of,
                policy,
                &format!(
                    "repository unavailable: scans={:?}, events={:?}",
                    scans.err(),
                    events.err()
                ),
            ),
        }
    }

    fn unavailable_rebate_valuation(
        as_of: DateTime<Utc>,
        policy: &MakerRebatePolicy,
        detail: &str,
    ) -> QuantResult<MakerRebateValuationEvidence> {
        tracing::warn!(detail, "maker rebate objective valuation is unavailable");
        let evidence_hash = CanonicalDigest::content_hash_typed(
            "quant-pivot/maker-rebate-valuation-unavailable",
            1,
            &(as_of, detail),
        )?;
        Ok(MakerRebateValuationEvidence {
            as_of,
            health: MakerRebateValuationHealth::Unavailable,
            program_day_baselines: Vec::new(),
            payout_threshold_usd: Usd::new(policy.payout_threshold_usd.value),
            delay_basis: MakerRebateDelayBasis::ConservativeFallback {
                lag_from_program_close_secs: policy.fallback_lag_from_program_close_secs,
            },
            evidence_hash,
        })
    }

    fn record_rebate_diagnostics(
        &self,
        valuation: &MakerRebateValuationEvidence,
        rejections: &[EconomicTierBuildRejection],
        portfolio: &PortfolioBuild,
    ) {
        self.deps
            .metrics
            .record_maker_rebate_diagnostic("valuation_health", valuation.health.metric_label());
        for rejection in rejections {
            if let EconomicTierBuildRejection::PassiveMakerRebateUnavailable { reason, .. } =
                rejection
            {
                self.deps
                    .metrics
                    .record_maker_rebate_diagnostic("passive_suppression", reason.metric_label());
            }
        }
        for selected in &portfolio.selected {
            if let EntryExecutionEconomics::Passive(entry) = &selected.tier.entry_execution
                && let MakerRebateObjectiveStatus::Zero { reason } =
                    entry.maker_rebate_objective_status
            {
                self.deps
                    .metrics
                    .record_maker_rebate_diagnostic("objective_zero", reason.metric_label());
            }
        }
    }

    async fn select_snapshot(
        &self,
        context: &BuildContext,
        model_requirements: ModelFeatureRequirements,
        candidates: Vec<MarketCandidate>,
        universe: &ReportUniversePlan,
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
                    route_availability: Some(universe.contract.availability.clone()),
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
        universe: &ReportUniversePlan,
    ) -> QuantResult<Vec<ReadyRoute>> {
        if represented_routes.is_empty() {
            return Ok(Vec::new());
        }
        let active = universe
            .active
            .iter()
            .filter(|active| represented_routes.routes.contains(&active.route))
            .cloned()
            .collect::<Vec<_>>();
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

    async fn report_universe_plan(
        &self,
        context: &BuildContext,
    ) -> QuantResult<ReportUniversePlan> {
        let serving_head = self
            .deps
            .exchange_history_repo
            .serving_head_at(
                ExchangeHistoryFrontier::Activation,
                context.boundary.decision_at(),
            )
            .await?
            .ok_or_else(|| ReportError::HistoryServingHeadUnavailable {
                detail: "no serving-head seal exists at the report decision boundary".to_owned(),
            })?;
        self.deps
            .feature_pipeline
            .validate_execution_history_seal(&serving_head)
            .await
            .map_err(|error| ReportError::HistoryWindowInvalidated {
                seal_id: serving_head.seal.serving_head_seal_id.into(),
                detail: error.to_string(),
            })?;
        let mut active = self
            .deps
            .model_runner
            .available_route_requirements(&context.version)
            .await?;
        active.sort_unstable_by_key(|route| route.route);
        let contract = ReportUniverseContract::try_new(
            context.version.decision_policy_snapshot_id,
            context.version.snapshot_hash,
            active
                .iter()
                .map(|route| ReportUniverseRoute::from(&route.serving))
                .collect(),
            serving_head.seal.serving_head_seal_id,
            serving_head.seal.seal_hash,
        )?;
        Ok(ReportUniversePlan {
            active,
            contract,
            serving_head,
        })
    }

    async fn verify_route(
        &self,
        context: &BuildContext,
        active: ActiveModelRequirements,
    ) -> QuantResult<(
        ActiveModelRequirements,
        PromotedRouteContract,
        Option<TradePolicyArtifactInfo>,
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
        let policy = match contract.serving_authority {
            ServingAuthority::ExecutionEligible => Some(
                load_trade_policy(
                    self.deps.trade_policy_repo.as_ref(),
                    &artifact,
                    &active,
                    &contract,
                )
                .await?,
            ),
            ServingAuthority::AnalysisOnlyWithLiveL2 => None,
        };
        if policy
            .as_ref()
            .is_some_and(|policy| context.boundary.decision_at() < policy.created_at)
        {
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
        routes: &[ReadyRoute],
        universe: &ReportUniversePlan,
    ) -> QuantResult<ReportFeatureResults> {
        let mut report = ReportFeatureResults {
            routes: Vec::with_capacity(routes.len()),
            rejected: Vec::new(),
            captures: HashMap::new(),
            data_quality_snapshot: context.empty_data_quality_snapshot(),
        };
        if selection.included.is_empty() {
            return Ok(report);
        }
        let mut quality_rows = Vec::with_capacity(selection.included.len());
        for route in routes {
            let markets = selection
                .included
                .iter()
                .filter(|market| BuyModelRoute::from(market.category) == route.active.route)
                .cloned()
                .collect::<Vec<_>>();
            if markets.is_empty() {
                continue;
            }
            let result = self
                .deps
                .feature_pipeline
                .run(FeaturePipelineRequest {
                    included: &markets,
                    feature_contract: route.contract.feature_contract,
                    boundary: context.boundary.clone(),
                    features: &context.config.profile_artifacts.features.definition,
                    domain: &context.config.profile_artifacts.domain.definition,
                    data_quality: &context.config.recommendation.data_quality,
                    model_requirements: &route.active.model_requirements,
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
                    execution_history_seal: Some(&universe.serving_head),
                })
                .await?;
            if result.data_quality_snapshot.decision_at != context.boundary.decision_at()
                || result.data_quality_snapshot.decision_policy_snapshot_id
                    != context.version.decision_policy_snapshot_id
            {
                return Err(ReportError::InvariantViolation {
                    stage: "route_features",
                    detail: format!(
                        "Route {:?} returned a data-quality snapshot for a different boundary",
                        route.active.route
                    ),
                }
                .into());
            }
            for (market_id, capture) in result.captures {
                if report.captures.insert(market_id.clone(), capture).is_some() {
                    return Err(ReportError::InvariantViolation {
                        stage: "route_features",
                        detail: format!(
                            "market {market_id} was materialized by more than one Route contract"
                        ),
                    }
                    .into());
                }
            }
            quality_rows.extend(result.data_quality_snapshot.tokens_json.0);
            report.rejected.extend(result.rejected);
            report.routes.push(RouteFeatureRound {
                route: route.active.route,
                accepted: result.accepted,
                persisted: result.persisted,
                feature_evidence: result.feature_evidence,
            });
        }
        quality_rows.sort_by(|left, right| {
            left.market_id
                .cmp(&right.market_id)
                .then_with(|| left.token_id.cmp(&right.token_id))
        });
        report
            .rejected
            .sort_by(|left, right| left.market_id.cmp(&right.market_id));
        report.data_quality_snapshot.tokens_json = ReportDataQualityTokens(quality_rows);
        Ok(report)
    }

    async fn run_route_models(
        &self,
        context: &BuildContext,
        selection: &MarketSelectionSnapshot,
        features: &ReportFeatureResults,
        routes: &mut [ReadyRoute],
    ) -> QuantResult<(Vec<RoutedCandidate>, Vec<ModelMarketDecision>)> {
        let mut routed_candidates = Vec::new();
        let mut decisions = Vec::new();
        for route in routes {
            if !selection
                .included
                .iter()
                .any(|market| BuyModelRoute::from(market.category) == route.active.route)
            {
                route.feature_complete_markets = 0;
                continue;
            }
            let round = features.for_route(route.active.route)?;
            let persisted = round.vector_ids_by_market()?;
            let accepted = round
                .accepted
                .iter()
                .map(|vector| (vector.market_id.clone(), vector))
                .collect::<HashMap<_, _>>();
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
            let evidence = round.evidence()?;
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
    if Some(artifact_id) != contract.trade_policy_artifact_id
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
    maker_rebate_valuation: &MakerRebateValuationEvidence,
) -> QuantResult<EconomicTierSeedBuild> {
    if candidates.is_empty() {
        return Ok(EconomicTierSeedBuild {
            admitted: Vec::new(),
            rejections: Vec::new(),
        });
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
    let mut rejections = Vec::new();
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
        let built = candidate_tiers(
            context,
            market,
            capture,
            routed,
            route,
            portfolio,
            maker_rebate_valuation,
        )?;
        tiers.extend(
            built
                .admitted
                .into_iter()
                .map(|(seed, source)| SeededTier { seed, source }),
        );
        if let Some(rejection) = built.rejection {
            rejections.push(rejection);
        }
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
    Ok(EconomicTierSeedBuild {
        admitted: tiers,
        rejections,
    })
}

fn candidate_tiers(
    context: &BuildContext,
    market: &SelectedMarket,
    capture: &MarketDecisionCapture,
    routed: &RoutedCandidate,
    route: &ReadyRoute,
    portfolio: &PromotedPortfolioContext,
    maker_rebate_valuation: &MakerRebateValuationEvidence,
) -> QuantResult<CandidateTierBuild> {
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
    let order_rules =
        capture
            .market
            .order_rules
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "economic_tier",
                detail: format!(
                    "market {} has no point-in-time CLOB order rules",
                    market.market_id
                ),
            })?;
    if order_rules.tick_size != capture.market_context.tick_size {
        return Err(ReportError::InvariantViolation {
            stage: "economic_tier",
            detail: format!(
                "market {} captured tick differs from its CLOB order rules",
                market.market_id
            ),
        }
        .into());
    }
    let fee =
        capture
            .market
            .fee_schedule
            .as_ref()
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "economic_tier",
                detail: format!("market {} has no PIT fee schedule", market.market_id),
            })?;
    let Ok(execution_economics) = PitMarketExecutionEconomics::resolve(
        fee,
        &capture.market.maker_rebate_evidence,
        capture.market.available_at,
        context.boundary.decision_at(),
    ) else {
        return Ok(CandidateTierBuild {
            admitted: Vec::new(),
            rejection: Some(EconomicTierBuildRejection::ExecutionEconomicsUnavailable {
                market_id: market.market_id.clone(),
            }),
        });
    };
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
    if route.contract.serving_authority == ServingAuthority::AnalysisOnlyWithLiveL2 {
        return bootstrap_candidate_tiers(BootstrapTierInput {
            context,
            market,
            capture,
            routed,
            route,
            portfolio,
            book,
            fee_schedule: &execution_economics.fee_schedule,
            best_ask,
            horizon,
            order_rules,
        });
    }
    let policy = route
        .trade_policy
        .as_ref()
        .ok_or_else(|| ReportError::RouteReadiness {
            route: routed.route.as_str().to_owned(),
            detail: "execution-eligible Route lost its Trade Policy".to_owned(),
        })?;
    let built = full_l2_candidate_tiers(FullL2TierInput {
        context,
        market,
        capture,
        routed,
        route,
        portfolio,
        maker_rebate_valuation,
        policy,
        book,
        execution_economics: &execution_economics,
        best_ask,
        horizon,
        order_rules,
    })?;
    let minimum_rejection = if built.admitted.is_empty() {
        built.minimum_rejection.map(|(requested, minimum)| {
            EconomicTierBuildRejection::BelowMinimumOrderSize {
                market_id: market.market_id.clone(),
                requested,
                minimum,
            }
        })
    } else {
        None
    };
    Ok(CandidateTierBuild {
        admitted: built.admitted,
        rejection: minimum_rejection.or_else(|| {
            built.passive_rejection.map(|reason| {
                EconomicTierBuildRejection::PassiveMakerRebateUnavailable {
                    market_id: market.market_id.clone(),
                    reason,
                }
            })
        }),
    })
}

#[derive(Clone, Copy)]
struct FullL2TierInput<'a> {
    context: &'a BuildContext,
    market: &'a SelectedMarket,
    capture: &'a MarketDecisionCapture,
    routed: &'a RoutedCandidate,
    route: &'a ReadyRoute,
    portfolio: &'a PromotedPortfolioContext,
    maker_rebate_valuation: &'a MakerRebateValuationEvidence,
    policy: &'a TradePolicyArtifactInfo,
    book: &'a ResolvedBook,
    execution_economics: &'a PitMarketExecutionEconomics,
    best_ask: Price,
    horizon: u64,
    order_rules: PolymarketOrderRules,
}

fn policy_tier_seed(
    input: FullL2TierInput<'_>,
    cohort: &TradePolicyCohort,
    tier_ordinal: u32,
    lineage_hash: ContentHash,
) -> QuantResult<PolicyTierBuild> {
    let FullL2TierInput {
        context,
        market,
        capture: _,
        routed,
        book,
        execution_economics,
        maker_rebate_valuation,
        best_ask,
        order_rules,
        ..
    } = input;
    match &cohort.entry_order {
        EntryOrderTemplate::Aggressive {
            fill_requirement,
            max_slippage_bps,
            max_book_age_ms,
        } => {
            if cohort.max_slippage_bps != *max_slippage_bps
                || cohort.max_book_age_ms != *max_book_age_ms
            {
                return Err(ReportError::RouteReadiness {
                    route: routed.route.as_str().to_owned(),
                    detail: "Trade Policy aggressive cohort has inconsistent entry limits"
                        .to_owned(),
                }
                .into());
            }
            let limit_price = aggressive_buy_limit(best_ask, *max_slippage_bps, order_rules)
                .map_err(|error| ReportError::InvariantViolation {
                    stage: "economic_tier",
                    detail: format!("aggressive BUY limit is invalid: {error}"),
                })?;
            let build = ExecutableCashTierSeedFactory::build(ExecutableCashTierSeedInput {
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
                fee_schedule: &execution_economics.fee_schedule,
                fill_at: context.boundary.decision_at(),
                limit_price,
                cash_budget: cohort.key.cash_budget_tier,
                fill_requirement: *fill_requirement,
                order_rules,
                source_lineage_hash: lineage_hash,
            })?;
            let seed = match build {
                TierSeedBuild::Ready(seed) => seed,
                TierSeedBuild::Unfilled => return Ok(PolicyTierBuild::Unfilled),
                TierSeedBuild::BelowMinimum { requested, minimum } => {
                    return Ok(PolicyTierBuild::BelowMinimum { requested, minimum });
                }
            };
            let execution_vwap = match &seed.entry_execution {
                EntryExecutionEconomics::Aggressive(entry) => entry.execution_vwap,
                EntryExecutionEconomics::Passive(_) => {
                    return Err(ReportError::InvariantViolation {
                        stage: "economic_tier",
                        detail: "aggressive seed factory returned a passive entry".to_owned(),
                    }
                    .into());
                }
            };
            Ok(PolicyTierBuild::Ready {
                seed,
                price: execution_vwap,
                limit_price,
            })
        }
        EntryOrderTemplate::PassivePostOnly {
            placement,
            good_til_secs,
            max_book_age_ms,
        } => {
            if cohort.max_slippage_bps != Bps::ZERO || cohort.max_book_age_ms != *max_book_age_ms {
                return Err(ReportError::RouteReadiness {
                    route: routed.route.as_str().to_owned(),
                    detail: "Trade Policy passive cohort has inconsistent entry limits".to_owned(),
                }
                .into());
            }
            let distribution = cohort.passive_fill_distribution.clone().ok_or_else(|| {
                ReportError::RouteReadiness {
                    route: routed.route.as_str().to_owned(),
                    detail: "passive cohort has no published OOS fill distribution".to_owned(),
                }
            })?;
            let best_bid = book
                .bids
                .first()
                .map(|level| level.price_decimal())
                .ok_or_else(|| ReportError::InvariantViolation {
                    stage: "economic_tier",
                    detail: "passive candidate has no best bid".to_owned(),
                })?;
            let limit_price =
                passive_buy_limit(best_bid, best_ask, *placement, order_rules.tick_size).map_err(
                    |error| ReportError::InvariantViolation {
                        stage: "economic_tier",
                        detail: format!("passive post-only price is invalid: {error:?}"),
                    },
                )?;
            let requested_shares =
                Shares::new(cohort.key.cash_budget_tier.inner() / limit_price.inner());
            let build = ExecutablePassiveTierSeedFactory::build(ExecutablePassiveTierSeedInput {
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
                execution_economics,
                decision_at: context.boundary.decision_at(),
                limit_price,
                requested_shares,
                cash_budget: cohort.key.cash_budget_tier,
                good_til_secs: *good_til_secs,
                fill_distribution: distribution,
                maker_rebate_valuation: (*maker_rebate_valuation).clone(),
                order_rules,
                source_lineage_hash: lineage_hash,
            })?;
            match build {
                TierSeedBuild::Ready(seed) => Ok(PolicyTierBuild::Ready {
                    seed,
                    price: limit_price,
                    limit_price,
                }),
                TierSeedBuild::Unfilled => Ok(PolicyTierBuild::Unfilled),
                TierSeedBuild::BelowMinimum { requested, minimum } => {
                    Ok(PolicyTierBuild::BelowMinimum { requested, minimum })
                }
            }
        }
    }
}

fn full_l2_candidate_tiers(input: FullL2TierInput<'_>) -> QuantResult<FullL2TierBuild> {
    let FullL2TierInput {
        context,
        market,
        capture,
        routed,
        route,
        portfolio,
        policy,
        book: _,
        execution_economics,
        best_ask: _,
        horizon,
        maker_rebate_valuation: _,
        order_rules: _,
    } = input;
    let mut by_budget = BTreeMap::<Usd, Vec<(ExecutableTierSeed, TierSource)>>::new();
    let mut passive_rejection = None;
    let mut minimum_rejection = None;
    for (index, cohort) in policy.payload_json.cohorts.iter().enumerate() {
        if cohort.key.category != market.category
            || cohort.key.horizon_secs != horizon
            || cohort.key.profile_ref != route.contract.research_profile_ref
            || !matches!(&cohort.entry_condition, EntryConditionTemplate::Immediate)
        {
            continue;
        }
        if matches!(
            cohort.entry_order,
            EntryOrderTemplate::PassivePostOnly { .. }
        ) && let PitMakerRebateEvidence::Unavailable { reason, .. } =
            &execution_economics.maker_rebate_evidence
        {
            passive_rejection.get_or_insert(*reason);
            continue;
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
        let lineage_hash = tier_lineage_hash(&TierLineageInput {
            context,
            capture,
            routed,
            route,
            cohort,
            cohort_index,
            execution_economics,
            portfolio,
            order_rules: input.order_rules,
        })?;
        let (seed, price, limit_price) =
            match policy_tier_seed(input, cohort, tier_ordinal, lineage_hash)? {
                PolicyTierBuild::Ready {
                    seed,
                    price,
                    limit_price,
                } => (*seed, price, limit_price),
                PolicyTierBuild::Unfilled => continue,
                PolicyTierBuild::BelowMinimum { requested, minimum } => {
                    minimum_rejection.get_or_insert((requested, minimum));
                    continue;
                }
            };
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
            artifact_id: policy.artifact_id,
            artifact_hash: policy.content_hash,
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
                    contract: PlannedRecommendationContract::FullL2 {
                        provenance,
                        cohort: Box::new(cohort.clone()),
                    },
                    entry_limit_price: limit_price,
                },
            ));
    }
    Ok(FullL2TierBuild {
        admitted: route_unique_budget_tiers(by_budget, routed)?,
        passive_rejection,
        minimum_rejection,
    })
}

#[derive(Clone, Copy)]
struct BootstrapTierInput<'a> {
    context: &'a BuildContext,
    market: &'a SelectedMarket,
    capture: &'a MarketDecisionCapture,
    routed: &'a RoutedCandidate,
    route: &'a ReadyRoute,
    portfolio: &'a PromotedPortfolioContext,
    book: &'a ResolvedBook,
    fee_schedule: &'a PitFeeSchedule,
    best_ask: Price,
    horizon: u64,
    order_rules: PolymarketOrderRules,
}

impl BootstrapTierInput<'_> {
    fn visible_ask_depth(&self, limit_price: Price) -> QuantResult<Decimal> {
        let depth = self
            .book
            .asks
            .iter()
            .take_while(|level| level.price_decimal() <= limit_price)
            .try_fold(Decimal::ZERO, |total, level| {
                level
                    .price_decimal()
                    .inner()
                    .checked_mul(level.size_decimal().inner())
                    .and_then(|notional| total.checked_add(notional))
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "bootstrap.visible_depth_usd",
                        detail: "live ask depth overflowed Decimal".to_owned(),
                    })
            })?;
        Ok(depth)
    }
}

fn bootstrap_candidate_tiers(input: BootstrapTierInput<'_>) -> QuantResult<CandidateTierBuild> {
    let BootstrapTierInput {
        context,
        market,
        capture,
        routed,
        route,
        portfolio,
        book,
        fee_schedule,
        best_ask,
        horizon,
        order_rules,
    } = input;
    let max_slippage_bps = Bps::new(Decimal::from(
        context
            .config
            .execution_risk
            .entry_order_policy
            .max_slippage_bps,
    ));
    let min_depth_usd = Usd::new(
        context
            .config
            .execution_risk
            .entry_order_policy
            .min_entry_book_depth_usd
            .value,
    );
    let limit_price =
        aggressive_buy_limit(best_ask, max_slippage_bps, order_rules).map_err(|error| {
            ReportError::InvariantViolation {
                stage: "economic_tier",
                detail: format!("bootstrap aggressive BUY limit is invalid: {error}"),
            }
        })?;
    let visible_depth = input.visible_ask_depth(limit_price)?;
    if visible_depth < min_depth_usd.inner() {
        return Ok(CandidateTierBuild {
            admitted: Vec::new(),
            rejection: Some(EconomicTierBuildRejection::InsufficientLiveDepth {
                market_id: market.market_id.clone(),
                visible_usd: Usd::new(visible_depth),
                required_usd: min_depth_usd,
                limit_price,
            }),
        });
    }
    let model_run_id = route
        .model_run_id
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "economic_tier",
            detail: "bootstrap candidate Route has no model run".to_owned(),
        })?;
    let mut tiers = Vec::new();
    let mut minimum_rejection = None;
    for (index, cash_budget) in route
        .contract
        .profile
        .spec
        .allowed_cash_budget_tiers
        .iter()
        .copied()
        .enumerate()
    {
        let tier_ordinal = u32::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| ReportError::NumericOverflow {
                field: "bootstrap.tier_ordinal",
                detail: "bootstrap cash-tier index overflowed u32".to_owned(),
            })?;
        let lineage_hash = bootstrap_tier_lineage_hash(&BootstrapLineageInput {
            context,
            capture,
            routed,
            route,
            fee_schedule,
            portfolio,
            tier_ordinal,
            cash_budget,
            order_rules,
        })?;
        let build = ExecutableCashTierSeedFactory::build(ExecutableCashTierSeedInput {
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
            fee_schedule,
            fill_at: context.boundary.decision_at(),
            limit_price,
            cash_budget,
            fill_requirement: FillRequirement::AllowPartial,
            order_rules,
            source_lineage_hash: lineage_hash,
        })?;
        let seed = match build {
            TierSeedBuild::Ready(seed) => *seed,
            TierSeedBuild::Unfilled => continue,
            TierSeedBuild::BelowMinimum { requested, minimum } => {
                minimum_rejection.get_or_insert((requested, minimum));
                continue;
            }
        };
        tiers.push((
            seed,
            TierSource {
                route: routed.route,
                report_route_run_id: routed.report_route_run_id,
                model_version_id: routed.model_version_id,
                model_run_id,
                candidate: routed.candidate.clone(),
                contract: PlannedRecommendationContract::Bootstrap {
                    profile_ref: route.contract.research_profile_ref.clone(),
                    feature_contract: route.contract.feature_contract,
                    recommendation_contract_hash: route.contract.recommendation_contract_hash,
                    cash_budget_tier: cash_budget,
                    reference_horizon_secs: horizon,
                    max_slippage_bps,
                    min_depth_usd,
                    max_book_age_ms: context.config.recommendation.data_quality.max_book_age_ms,
                },
                entry_limit_price: limit_price,
            },
        ));
    }
    let rejection = if tiers.is_empty() {
        minimum_rejection.map(|(requested, minimum)| {
            EconomicTierBuildRejection::BelowMinimumOrderSize {
                market_id: market.market_id.clone(),
                requested,
                minimum,
            }
        })
    } else {
        None
    };
    Ok(CandidateTierBuild {
        admitted: tiers,
        rejection,
    })
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

fn route_unique_budget_tiers(
    by_budget: BTreeMap<Usd, Vec<(ExecutableTierSeed, TierSource)>>,
    routed: &RoutedCandidate,
) -> QuantResult<Vec<(ExecutableTierSeed, TierSource)>> {
    let mut result = Vec::new();
    for (budget, matches) in by_budget {
        let mut seen_routes = BTreeSet::new();
        for (seed, source) in matches {
            let PlannedRecommendationContract::FullL2 { cohort, .. } = &source.contract else {
                return Err(ReportError::InvariantViolation {
                    stage: "economic_tier",
                    detail: "Trade Policy tier has no FullL2 cohort contract".to_owned(),
                }
                .into());
            };
            if cohort.key.entry_route != cohort.entry_order.route()
                || !seen_routes.insert(cohort.key.entry_route)
            {
                return Err(ReportError::RouteReadiness {
                    route: routed.route.as_str().to_owned(),
                    detail: format!(
                        "candidate {} cash tier {budget} has duplicate or inconsistent {:?} Trade Policy route",
                        routed.candidate.signal_candidate_id, cohort.key.entry_route
                    ),
                }
                .into());
            }
            result.push((seed, source));
        }
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
    execution_economics: &'a PitMarketExecutionEconomics,
    portfolio: &'a PromotedPortfolioContext,
    order_rules: PolymarketOrderRules,
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
        execution_economics_hash: ContentHash,
        order_rules: PolymarketOrderRules,
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
        3,
        &Preimage {
            decision_policy_snapshot_id: input.context.version.decision_policy_snapshot_id,
            decision_at: input.context.boundary.decision_at(),
            route: input.routed.route,
            report_route_run_id: input.routed.report_route_run_id,
            model_version_id: input.routed.model_version_id,
            model_run_id,
            signal_candidate_id: input.routed.candidate.signal_candidate_id,
            book_snapshot_hash,
            execution_economics_hash: input.execution_economics.composite_hash,
            order_rules: input.order_rules,
            trade_policy_hash: input
                .route
                .trade_policy
                .as_ref()
                .ok_or_else(|| ReportError::RouteReadiness {
                    route: input.routed.route.as_str().to_owned(),
                    detail: "full-L2 tier lineage has no Trade Policy".to_owned(),
                })?
                .content_hash,
            cohort_index: input.cohort_index,
            cohort: input.cohort,
            scenario_model_hash: input.portfolio.scenario_model.content_hash,
        },
    )?)
}

struct BootstrapLineageInput<'a> {
    context: &'a BuildContext,
    capture: &'a MarketDecisionCapture,
    routed: &'a RoutedCandidate,
    route: &'a ReadyRoute,
    fee_schedule: &'a PitFeeSchedule,
    portfolio: &'a PromotedPortfolioContext,
    tier_ordinal: u32,
    cash_budget: Usd,
    order_rules: PolymarketOrderRules,
}

fn bootstrap_tier_lineage_hash(input: &BootstrapLineageInput<'_>) -> QuantResult<ContentHash> {
    let model_run_id = input
        .route
        .model_run_id
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "economic_tier",
            detail: "bootstrap candidate Route has no model run lineage".to_owned(),
        })?;
    let book_snapshot_hash = input
        .capture
        .book_snapshot_ref_for(&input.routed.candidate.token_id)
        .map(|reference| reference.content_hash)
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "economic_tier",
            detail: "bootstrap candidate-side book reference is missing".to_owned(),
        })?;
    Ok(CanonicalDigest::content_hash_typed(
        "quant-pivot/bootstrap-economic-tier-source",
        2,
        &(
            input.context.version.decision_policy_snapshot_id,
            input.context.boundary.decision_at(),
            input.routed.route,
            input.routed.report_route_run_id,
            input.routed.model_version_id,
            model_run_id,
            input.routed.candidate.signal_candidate_id,
            book_snapshot_hash,
            input.fee_schedule.schedule_hash,
            input.order_rules,
            input.route.contract.recommendation_contract_hash,
            input.portfolio.scenario_model.content_hash,
            input.tier_ordinal,
            input.cash_budget,
        ),
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
        let release_secs = seeded.source.contract.release_secs();
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
                            "market {} has inconsistent payout, exit-capacity, or recommendation-contract release inputs",
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
            return Err(ReportError::UnmodeledOpenExposure {
                route: route.as_str().to_owned(),
                market_id: position.market_id.to_string(),
                token_id: position.token_id.to_string(),
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
    HashMap<TierSourceKey, TierSource>,
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
        let source_key = TierSourceKey::from_tier(&tier);
        if sources.insert(source_key, seeded.source).is_some() {
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
                    || account
                        .positions
                        .iter()
                        .any(|position| position.size > Shares::ZERO)
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
    config: &DecisionPolicySnapshot,
    scenario: Option<&SealedPortfolioScenarioArtifact>,
) -> QuantResult<Vec<ReportTierRejection>> {
    let markets = tiers
        .iter()
        .map(|tier| (tier.economic_tier_id, tier))
        .collect::<HashMap<_, _>>();
    rejected
        .iter()
        .map(|rejection| {
            let tier = markets
                .get(&rejection.economic_tier_id)
                .copied()
                .ok_or_else(|| ReportError::InvariantViolation {
                    stage: "portfolio_rejection",
                    detail: format!(
                        "rejected tier {} is absent from solver input",
                        rejection.economic_tier_id
                    ),
                })?;
            let diagnostics =
                if rejection.code == TierAdmissionRejectionCode::ProfitProbabilityFloor {
                    let scenario = scenario.ok_or_else(|| ReportError::InvariantViolation {
                        stage: "portfolio_rejection",
                        detail: format!(
                            "probability-rejected tier {} has no scenario artifact",
                            rejection.economic_tier_id
                        ),
                    })?;
                    let admission = &config.execution_risk.portfolio.admission;
                    Some(ReportFunnelDiagnostics::ProfitProbabilityFloor {
                        economic_tier_id: tier.economic_tier_id,
                        scenario_artifact_id: scenario.portfolio_scenario_artifact_id,
                        scenario_artifact_hash: scenario.content_hash,
                        nominal_profit_probability_bps: tier.economics.profit_probability_bps,
                        lower_profit_probability_bps: tier.profit_probability_lower_bps,
                        minimum_profit_probability_bps: admission.min_profit_probability_bps,
                        probability_interval_width_bps: tier.probability_interval_width_bps,
                        maximum_probability_interval_width_bps: admission
                            .max_probability_interval_width_bps,
                        nominal_expected_net_usd: tier.economics.nominal_expected_net_usd,
                        robust_expected_net_usd: tier.economics.robust_expected_net_usd,
                    })
                } else {
                    None
                };
            let report_rejection = ReportTierRejection {
                economic_tier_id: rejection.economic_tier_id,
                market_id: tier.market_id.clone(),
                code: rejection.code,
                diagnostics,
            };
            report_rejection
                .validate()
                .map_err(|detail| ReportError::InvariantViolation {
                    stage: "portfolio_rejection",
                    detail: format!(
                        "rejected tier {} has invalid diagnostics: {detail}",
                        rejection.economic_tier_id
                    ),
                })?;
            Ok(report_rejection)
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
    sources: &HashMap<TierSourceKey, TierSource>,
) -> QuantResult<Vec<PlannedReportRecommendation>> {
    selected
        .iter()
        .enumerate()
        .map(|(index, planned)| {
            let source = sources
                .get(&TierSourceKey::from_tier(&planned.tier))
                .ok_or_else(|| ReportError::InvariantViolation {
                    stage: "portfolio_ranking",
                    detail: format!(
                        "selected tier {} has no candidate lineage",
                        planned.tier.economic_tier_id
                    ),
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
                contract: source.contract.clone(),
                entry_limit_price: source.entry_limit_price,
            })
        })
        .collect()
}

fn route_rows(
    request: &BuildReportRequest,
    routes: &[ReadyRoute],
    universe: &ReportUniversePlan,
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
                recommendation_contract_hash: route.contract.recommendation_contract_hash,
                report_universe_plan_hash: universe.contract.availability.universe_plan_hash,
                history: RouteHistoryLineage::Runtime {
                    serving_head_seal_id: universe.serving_head.seal.serving_head_seal_id,
                    serving_head_seal_hash: universe.serving_head.seal.seal_hash,
                },
                serving_authority: route.contract.serving_authority,
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
                trade_policy_artifact_id: route.contract.trade_policy_artifact_id,
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
    features: &'a ReportFeatureResults,
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
    let reason = if initial.included.is_empty()
        && !account
            .positions
            .iter()
            .any(|position| position.size > Shares::ZERO)
    {
        EmptyReportReason::EmptySelection
    } else if selection.included.is_empty() || features.accepted_count() == 0 {
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
    universe: &ReportUniversePlan,
) -> QuantResult<RepresentedRouteSet> {
    for position in account
        .positions
        .iter()
        .filter(|position| position.size.is_positive())
    {
        let route = BuyModelRoute::from(position.category);
        if !universe
            .contract
            .availability
            .active_routes
            .contains(&route)
        {
            return Err(ReportError::UnmodeledOpenExposure {
                route: route.as_str().to_owned(),
                market_id: position.market_id.to_string(),
                token_id: position.token_id.to_string(),
            }
            .into());
        }
    }
    let represented = RepresentedRouteSet::from_routes(
        once(universe.contract.availability.primary_route)
            .chain(
                selection
                    .included
                    .iter()
                    .map(|market| BuyModelRoute::from(market.category)),
            )
            .chain(
                account
                    .positions
                    .iter()
                    .filter(|position| position.size.is_positive())
                    .map(|position| BuyModelRoute::from(position.category)),
            ),
    )
    .map_err(|error| QuantError::config(format!("derive represented Route set: {error}")))?;
    if represented
        .routes
        .iter()
        .any(|route| !universe.contract.availability.active_routes.contains(route))
    {
        return Err(ReportError::InvariantViolation {
            stage: "report_universe",
            detail: "represented routes escaped the pinned active route set".to_owned(),
        }
        .into());
    }
    Ok(represented)
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

impl RouteFeatureRound {
    fn evidence(&self) -> QuantResult<FeatureEvidenceCommitment> {
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

impl ReportFeatureResults {
    fn for_route(&self, route: BuyModelRoute) -> QuantResult<&RouteFeatureRound> {
        self.routes
            .iter()
            .find(|round| round.route == route)
            .ok_or_else(|| {
                ReportError::InvariantViolation {
                    stage: "route_features",
                    detail: format!("Route {route:?} has no feature-contract round"),
                }
                .into()
            })
    }

    fn accepted_count(&self) -> usize {
        self.routes.iter().map(|round| round.accepted.len()).sum()
    }

    fn vector_ids_by_market(&self) -> QuantResult<HashMap<MarketId, FeatureVectorId>> {
        let mut by_market = HashMap::new();
        for round in &self.routes {
            for (market_id, vector_id) in round.vector_ids_by_market()? {
                if by_market.insert(market_id.clone(), vector_id).is_some() {
                    return Err(ReportError::InvariantViolation {
                        stage: "feature_lineage",
                        detail: format!(
                            "market {market_id} has feature vectors from more than one Route contract"
                        ),
                    }
                    .into());
                }
            }
        }
        Ok(by_market)
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
