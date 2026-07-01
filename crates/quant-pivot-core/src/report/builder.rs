//! Report builder orchestration.

use std::{collections::HashMap, sync::Arc};

use chrono::{Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        PointInTimeDataSource, RuntimeConfigVersionInfo,
        quant::{NewPortfolioPlan, NewReportDataQualitySnapshot},
    },
    enums::quant::{EmptyReason, RejectionReason},
    runtime_config::RuntimeConfig,
    types::{
        Bps, FeatureVectorId, MarketId, MarketSelectionId, ModelRunId, PortfolioPlanId,
        ReportDataQualitySnapshotId, ReportDataQualityTokens, Usd,
    },
};
use quant_pivot_repository::traits::{
    MarketSelectionRepository, QuantFactReadRepository, RuntimeConfigVersionRepository,
};
use quant_pivot_research::{
    backtest::PortfolioCaps,
    features::{MarketDecisionCapture, PitView},
    model::SignalCandidate,
    portfolio::{
        AccountSnapshot, CorrelationConstraint, CorrelationEstimator, CorrelationInput,
        CorrelationMarket, DefaultPortfolioPlanner, PlanCandidate, PortfolioPlanInput,
        PortfolioPlanOutput, PortfolioPlanner, RejectedCandidate, optimizer_from_config,
        sizing_model_from_config,
    },
    selection::{
        MarketSelectionBuildRequest, MarketSelectionSnapshot, MarketSelector, SelectedMarket,
    },
};
use rust_decimal::Decimal;

use crate::{
    governance::RuntimeModeHandle,
    pipeline::market_candidate_provider::MarketCandidateProvider,
    service::{
        account::AccountProviderFactory,
        equity::{DrawdownProvider, ReportEquitySnapshot},
        feature_pipeline::{FeaturePipelineRequest, FeaturePipelineResult, FeaturePipelineService},
        market_selection::map_snapshot_to_model,
        model_runner::{
            ActiveModelRequirements, ActiveModelRequirementsRequest, ModelRunOutcome,
            ModelRunRequest, ModelRunner,
        },
    },
};

use super::{
    composer::{ComposeReportInput, RecommendationComposer, empty_plan_for_report},
    readiness::ReportReadinessGate,
    types::{BuildReportRequest, ComposedReport, EmptyReportContext, ReportTrigger},
};

/// Report builder interface.
#[async_trait::async_trait]
pub trait ReportBuilder: Send + Sync {
    /// Build a complete report artifact without writing the report transaction.
    async fn build(&self, request: BuildReportRequest) -> QuantResult<ComposedReport>;
}

/// Dependencies for [`DefaultReportBuilder`].
pub struct ReportBuilderDeps {
    pub runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository>,
    pub market_selector: Arc<dyn MarketSelector>,
    pub market_selection_repo: Arc<dyn MarketSelectionRepository>,
    pub candidate_provider: Arc<MarketCandidateProvider>,
    pub feature_pipeline: Arc<FeaturePipelineService>,
    pub model_runner: Arc<ModelRunner>,
    pub account_provider_factory: Arc<AccountProviderFactory>,
    pub drawdown_provider: Arc<dyn DrawdownProvider>,
    pub composer: Arc<dyn RecommendationComposer>,
    pub pit_source: Arc<dyn PointInTimeDataSource>,
    pub quant_fact_read_repo: Arc<dyn QuantFactReadRepository>,
    pub correlation_estimator: Arc<dyn CorrelationEstimator>,
    pub runtime_mode: RuntimeModeHandle,
    pub readiness_gate: Arc<dyn ReportReadinessGate>,
}

/// Production report builder.
pub struct DefaultReportBuilder {
    deps: ReportBuilderDeps,
}

struct BuildContext {
    version: RuntimeConfigVersionInfo,
    config: RuntimeConfig,
    source_delay_secs: u64,
    as_of: chrono::DateTime<Utc>,
    top_n: u32,
    active: ActiveModelRequirements,
}

struct EmptyComposeInput<'a> {
    request: &'a BuildReportRequest,
    context: &'a BuildContext,
    market_selection_id: MarketSelectionId,
    account: &'a AccountSnapshot,
    equity: &'a ReportEquitySnapshot,
    empty: EmptyReportContext,
    model_run_id: Option<ModelRunId>,
    market_selection_count: u32,
    captures: HashMap<MarketId, MarketDecisionCapture>,
    feature_vector_by_market: HashMap<MarketId, FeatureVectorId>,
    data_quality_snapshot: NewReportDataQualitySnapshot,
    /// When the planner ran but published nothing, carry its plan row (solver
    /// provenance + rejected summary) instead of synthesizing a default plan.
    portfolio_plan: Option<NewPortfolioPlan>,
    planner_rejected: &'a [RejectedCandidate],
}

struct PublishedComposeInput<'a> {
    request: &'a BuildReportRequest,
    context: &'a BuildContext,
    selection: &'a MarketSelectionSnapshot,
    account: &'a AccountSnapshot,
    equity: &'a ReportEquitySnapshot,
    features: &'a FeaturePipelineResult,
    model_outcome: ModelRunOutcome,
    plan: PortfolioPlanOutput,
}

struct FeatureStageRefs<'a> {
    request: &'a BuildReportRequest,
    context: &'a BuildContext,
    selection: &'a MarketSelectionSnapshot,
    account: &'a AccountSnapshot,
    equity: &'a ReportEquitySnapshot,
    features: &'a FeaturePipelineResult,
}

#[derive(Clone)]
struct FeatureStageArtifacts {
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
        self.build_report(request).await
    }
}

impl DefaultReportBuilder {
    async fn build_report(&self, request: BuildReportRequest) -> QuantResult<ComposedReport> {
        let context = self.prepare_context(&request).await?;
        self.deps
            .readiness_gate
            .ensure_degraded_empty_allowed(&context.config)?;
        let selection = self.build_selection(&context).await?;
        let account = self
            .account_snapshot(context.as_of, &context.config)
            .await?;
        let mut equity = self
            .deps
            .drawdown_provider
            .snapshot_for_report(&account)
            .await?;

        if let Some(report) =
            self.compose_if_system_degraded(&request, &context, &selection, &account, &equity)?
        {
            return Ok(report);
        }
        if let Some(report) =
            self.compose_if_empty_selection(&request, &context, &selection, &account, &equity)?
        {
            return Ok(report);
        }

        let features = self.build_features(&context, &selection).await?;
        let model_outcome = {
            let stage = FeatureStageRefs {
                request: &request,
                context: &context,
                selection: &selection,
                account: &account,
                equity: &equity,
                features: &features,
            };
            if let Some(report) = self.compose_if_no_accepted_features(&stage)? {
                return Ok(report);
            }

            let model_outcome = self.run_model(&context, &selection, &features).await?;
            let artifacts = FeatureStageArtifacts {
                captures: features.captures.clone(),
                data_quality_snapshot: features.data_quality_snapshot.clone(),
            };
            if let Some(report) =
                self.compose_if_no_model_signals(&stage, &model_outcome, artifacts)?
            {
                return Ok(report);
            }
            model_outcome
        };
        let artifacts = FeatureStageArtifacts {
            captures: features.captures.clone(),
            data_quality_snapshot: features.data_quality_snapshot.clone(),
        };

        let correlation = self.build_correlation(&context, &selection).await?;
        self.finalize_equity_for_sizing(&account, &mut equity)
            .await?;
        let plan = Self::plan_portfolio(
            &context,
            &selection,
            &model_outcome,
            &account,
            equity.drawdown_state,
            correlation.as_ref(),
        )?;
        if plan.planned.is_empty() {
            return self.compose_empty(EmptyComposeInput {
                request: &request,
                context: &context,
                market_selection_id: selection.market_selection_id.clone(),
                account: &account,
                equity: &equity,
                empty: EmptyReportContext {
                    reason: empty_reason_from_planner_rejections(&plan.rejected),
                    candidate_count: model_outcome.emitted,
                    rejected_count: u32::try_from(plan.rejected.len()).unwrap_or(u32::MAX),
                    warnings: Vec::new(),
                },
                model_run_id: Some(model_outcome.model_run_id.clone()),
                market_selection_count: market_selection_count(&selection.included),
                captures: artifacts.captures,
                feature_vector_by_market: feature_vector_by_market(&features),
                data_quality_snapshot: artifacts.data_quality_snapshot,
                portfolio_plan: Some(plan.plan_row),
                planner_rejected: &plan.rejected,
            });
        }

        self.finalize_equity_for_sizing(&account, &mut equity)
            .await?;

        self.compose_published(PublishedComposeInput {
            request: &request,
            context: &context,
            selection: &selection,
            account: &account,
            equity: &equity,
            features: &features,
            model_outcome,
            plan,
        })
    }

    fn compose_if_system_degraded(
        &self,
        request: &BuildReportRequest,
        context: &BuildContext,
        selection: &MarketSelectionSnapshot,
        account: &AccountSnapshot,
        equity: &ReportEquitySnapshot,
    ) -> QuantResult<Option<ComposedReport>> {
        if !self.deps.readiness_gate.is_system_degraded() {
            return Ok(None);
        }
        self.compose_empty(EmptyComposeInput {
            request,
            context,
            market_selection_id: selection.market_selection_id.clone(),
            account,
            equity,
            empty: EmptyReportContext {
                reason: EmptyReason::SystemDegraded,
                candidate_count: 0,
                rejected_count: 0,
                warnings: vec!["operational phase is not Operational".to_owned()],
            },
            model_run_id: None,
            market_selection_count: market_selection_count(&selection.included),
            captures: HashMap::new(),
            feature_vector_by_market: HashMap::new(),
            data_quality_snapshot: empty_data_quality_snapshot(context, context.as_of),
            portfolio_plan: None,
            planner_rejected: &[],
        })
        .map(Some)
    }

    fn compose_if_empty_selection(
        &self,
        request: &BuildReportRequest,
        context: &BuildContext,
        selection: &MarketSelectionSnapshot,
        account: &AccountSnapshot,
        equity: &ReportEquitySnapshot,
    ) -> QuantResult<Option<ComposedReport>> {
        if !selection.included.is_empty() {
            return Ok(None);
        }
        self.compose_empty(EmptyComposeInput {
            request,
            context,
            market_selection_id: selection.market_selection_id.clone(),
            account,
            equity,
            empty: EmptyReportContext {
                reason: EmptyReason::EmptySelection,
                candidate_count: 0,
                rejected_count: u32::try_from(selection.excluded.len()).unwrap_or(u32::MAX),
                warnings: Vec::new(),
            },
            model_run_id: None,
            market_selection_count: market_selection_count(&selection.included),
            captures: HashMap::new(),
            feature_vector_by_market: HashMap::new(),
            data_quality_snapshot: empty_data_quality_snapshot(context, context.as_of),
            portfolio_plan: None,
            planner_rejected: &[],
        })
        .map(Some)
    }

    fn compose_if_no_accepted_features(
        &self,
        stage: &FeatureStageRefs<'_>,
    ) -> QuantResult<Option<ComposedReport>> {
        if !stage.features.accepted.is_empty() {
            return Ok(None);
        }
        self.compose_empty(EmptyComposeInput {
            request: stage.request,
            context: stage.context,
            market_selection_id: stage.selection.market_selection_id.clone(),
            account: stage.account,
            equity: stage.equity,
            empty: EmptyReportContext {
                reason: EmptyReason::InsufficientDataQuality,
                candidate_count: 0,
                rejected_count: u32::try_from(stage.features.rejected.len()).unwrap_or(u32::MAX),
                warnings: vec!["feature pipeline accepted zero markets".to_owned()],
            },
            model_run_id: None,
            market_selection_count: market_selection_count(&stage.selection.included),
            captures: stage.features.captures.clone(),
            feature_vector_by_market: feature_vector_by_market(stage.features),
            data_quality_snapshot: stage.features.data_quality_snapshot.clone(),
            portfolio_plan: None,
            planner_rejected: &[],
        })
        .map(Some)
    }

    fn compose_if_no_model_signals(
        &self,
        stage: &FeatureStageRefs<'_>,
        model_outcome: &ModelRunOutcome,
        artifacts: FeatureStageArtifacts,
    ) -> QuantResult<Option<ComposedReport>> {
        if !model_outcome.accepted.is_empty() {
            return Ok(None);
        }
        self.compose_empty(EmptyComposeInput {
            request: stage.request,
            context: stage.context,
            market_selection_id: stage.selection.market_selection_id.clone(),
            account: stage.account,
            equity: stage.equity,
            empty: EmptyReportContext {
                reason: EmptyReason::NoPositiveSignal,
                candidate_count: model_outcome.emitted,
                rejected_count: 0,
                warnings: vec!["active model emitted no positive candidate".to_owned()],
            },
            model_run_id: Some(model_outcome.model_run_id.clone()),
            market_selection_count: market_selection_count(&stage.selection.included),
            captures: artifacts.captures,
            feature_vector_by_market: feature_vector_by_market(stage.features),
            data_quality_snapshot: artifacts.data_quality_snapshot,
            portfolio_plan: None,
            planner_rejected: &[],
        })
        .map(Some)
    }
}

impl DefaultReportBuilder {
    async fn finalize_equity_for_sizing(
        &self,
        account: &AccountSnapshot,
        equity: &mut ReportEquitySnapshot,
    ) -> QuantResult<()> {
        let resolution = self
            .deps
            .drawdown_provider
            .resolve_drawdown_for_sizing(account, equity.drawdown_state)
            .await?;
        equity.drawdown_state = resolution.drawdown_state;
        equity.equity_snapshot.high_water_mark_usd = resolution.high_water_mark_usd;
        equity.equity_snapshot.drawdown_pct = resolution.drawdown_state.current_drawdown;
        Ok(())
    }

    async fn prepare_context(&self, request: &BuildReportRequest) -> QuantResult<BuildContext> {
        let (version, config) = self.load_config(request).await?;
        let source_delay_secs = resolve_source_delay(request, &config)?;
        let as_of = request.trigger_time - checked_source_delay(source_delay_secs)?;
        let top_n = resolve_top_n(request, &config)?;
        let active = self
            .deps
            .model_runner
            .active_requirements(ActiveModelRequirementsRequest {
                features: &config.features,
                factors: &config.factors,
                model: &config.model,
                as_of,
            })
            .await?;

        Ok(BuildContext {
            version,
            config,
            source_delay_secs,
            as_of,
            top_n,
            active,
        })
    }

    async fn build_selection(
        &self,
        context: &BuildContext,
    ) -> QuantResult<MarketSelectionSnapshot> {
        let candidates = self.deps.candidate_provider.candidates(context.as_of);
        let selection = self
            .deps
            .market_selector
            .build_snapshot(
                MarketSelectionBuildRequest {
                    as_of: context.as_of,
                    runtime_config_version_id: context.version.runtime_config_version_id.clone(),
                    selection: context.config.selection.clone(),
                    data_quality: context.config.data_quality.clone(),
                    features: context.config.features.clone(),
                    model_requirements: context.active.model_requirements.clone(),
                    source_delay_secs: context.source_delay_secs,
                },
                candidates.clone(),
            )
            .await?;
        let selection_model = map_snapshot_to_model(&selection, &candidates)?;
        self.deps
            .market_selection_repo
            .create_snapshot(selection_model.snapshot, selection_model.members)
            .await?;
        Ok(selection)
    }

    async fn build_features(
        &self,
        context: &BuildContext,
        selection: &MarketSelectionSnapshot,
    ) -> QuantResult<FeaturePipelineResult> {
        self.deps
            .feature_pipeline
            .run(FeaturePipelineRequest {
                included: &selection.included,
                as_of: context.as_of,
                runtime_config_version_id: context.version.runtime_config_version_id.clone(),
                features: &context.config.features,
                data_quality: &context.config.data_quality,
                model_requirements: &context.active.model_requirements,
                source_delay_secs: context.source_delay_secs,
                pit: PitView::Live(self.deps.pit_source.as_ref()),
                liquidity_cap_usd: liquidity_score_cap(&context.config)?,
            })
            .await
    }

    async fn run_model(
        &self,
        context: &BuildContext,
        selection: &MarketSelectionSnapshot,
        features: &FeaturePipelineResult,
    ) -> QuantResult<ModelRunOutcome> {
        let feature_vector_ids = features
            .persisted
            .iter()
            .map(|info| info.feature_vector_id.clone())
            .collect::<Vec<_>>();
        self.deps
            .model_runner
            .run(ModelRunRequest {
                runtime_config_version_id: context.version.runtime_config_version_id.clone(),
                market_selection_id: Some(selection.market_selection_id.clone()),
                selection: &selection.included,
                feature_vectors: &features.accepted,
                feature_vector_ids: &feature_vector_ids,
                features: &context.config.features,
                factors: &context.config.factors,
                model: &context.config.model,
                top_n: bounded_usize(context.top_n),
                as_of: context.as_of,
            })
            .await
    }

    fn plan_portfolio(
        context: &BuildContext,
        selection: &MarketSelectionSnapshot,
        model_outcome: &ModelRunOutcome,
        account: &AccountSnapshot,
        drawdown_state: quant_pivot_research::portfolio::DrawdownState,
        correlation: Option<&CorrelationConstraint>,
    ) -> QuantResult<PortfolioPlanOutput> {
        let caps = portfolio_caps(&context.config)?;
        let sizing = sizing_model_from_config(&context.config.portfolio.sizing)?;
        // The optimizer is built per report from the active (hot-reloadable)
        // runtime config, so a config change takes effect on the next run.
        let allocator = optimizer_from_config(&context.config.portfolio.optimizer)?;
        let planner = DefaultPortfolioPlanner::new(allocator);
        let plan_candidates = plan_candidates(&model_outcome.accepted, &selection.included);
        planner.plan(PortfolioPlanInput {
            portfolio_plan_id: PortfolioPlanId::from_v7(),
            model_run_id: model_outcome.model_run_id.clone(),
            market_selection_id: selection.market_selection_id.clone(),
            as_of: context.as_of,
            candidates: plan_candidates,
            account,
            drawdown_state,
            caps: &caps,
            max_correlated_exposure_usd: Usd::new(parse_decimal(
                "portfolio.constraints.max_correlated_exposure_usd",
                &context
                    .config
                    .portfolio
                    .constraints
                    .max_correlated_exposure_usd
                    .value,
            )?),
            correlation,
            sizing: sizing.as_ref(),
            entry_max_slippage_bps: Bps::new(Decimal::from(
                context.config.execution.entry_order_policy.max_slippage_bps,
            )),
            top_n: bounded_usize(context.top_n),
        })
    }

    /// Pre-fetch historical mid-price series and estimate correlated clusters,
    /// when the correlation cap is enabled and configured. Returns `None` (the
    /// cap does not bind) when disabled or the cap is unset — the Phase 4 path.
    async fn build_correlation(
        &self,
        context: &BuildContext,
        selection: &MarketSelectionSnapshot,
    ) -> QuantResult<Option<CorrelationConstraint>> {
        let constraints = &context.config.portfolio.constraints;
        let correlation = &constraints.correlation;
        if !correlation.enabled {
            return Ok(None);
        }
        let cap = parse_decimal(
            "portfolio.constraints.max_correlated_exposure_usd",
            &constraints.max_correlated_exposure_usd.value,
        )?;
        if cap <= Decimal::ZERO {
            return Ok(None);
        }
        let cluster_threshold = parse_decimal(
            "portfolio.constraints.correlation.cluster_threshold",
            &correlation.cluster_threshold.value,
        )?;

        let lookback_secs = i64::from(correlation.lookback_days)
            .checked_mul(86_400)
            .ok_or_else(|| QuantError::config("correlation.lookback_days too large"))?;
        let from_ms = (context.as_of - Duration::seconds(lookback_secs)).timestamp_millis();
        let to_ms = context.as_of.timestamp_millis();
        let token_ids = selection
            .included
            .iter()
            .map(|market| market.primary_token_id.clone())
            .collect::<Vec<_>>();
        let rows = self
            .deps
            .quant_fact_read_repo
            .mid_price_series(token_ids, from_ms, to_ms, CORRELATION_BUCKET_SECS)
            .await?;

        let mut by_token: HashMap<String, std::collections::BTreeMap<i64, Decimal>> =
            HashMap::new();
        let mut grid: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
        for row in rows {
            if let Some(price) = row.mid_price {
                by_token
                    .entry(row.token_id.as_str().to_owned())
                    .or_default()
                    .insert(row.bucket_ms, price.to_price().inner());
                grid.insert(row.bucket_ms);
            }
        }
        let grid: Vec<i64> = grid.into_iter().collect();

        let markets = selection
            .included
            .iter()
            .map(|market| CorrelationMarket {
                market_id: market.market_id.clone(),
                event_id: Some(market.event_id.clone()),
                category: market.category,
                mid_series: aligned_series(by_token.get(market.primary_token_id.as_str()), &grid),
            })
            .collect::<Vec<_>>();

        let groups = self
            .deps
            .correlation_estimator
            .estimate(&CorrelationInput {
                markets: &markets,
                min_observations: correlation.min_observations,
                cluster_threshold,
            })?;
        Ok(Some(groups.into_constraint(Usd::new(cap))))
    }

    fn compose_published(&self, input: PublishedComposeInput<'_>) -> QuantResult<ComposedReport> {
        let PortfolioPlanOutput {
            planned,
            rejected,
            plan_row,
        } = input.plan;
        self.deps.composer.compose(ComposeReportInput {
            trigger: &input.request.trigger,
            trigger_key: input.request.trigger.key(input.request.trigger_time),
            trigger_time: input.request.trigger_time,
            source_delay_secs: input.context.source_delay_secs,
            as_of: input.context.as_of,
            runtime_config_version_id: input.context.version.runtime_config_version_id.clone(),
            runtime_config: &input.context.config,
            runtime_mode: self.deps.runtime_mode.current(),
            model_version_id: input.context.active.model_version_id.clone(),
            market_selection_id: input.selection.market_selection_id.clone(),
            account: input.account,
            account_snapshot: input.equity.account_snapshot.clone(),
            equity_snapshot: input.equity.equity_snapshot.clone(),
            portfolio_plan: plan_row,
            planned: &planned,
            planner_rejected: &rejected,
            captures: input.features.captures.clone(),
            feature_vector_by_market: feature_vector_by_market(input.features),
            data_quality_snapshot: input.features.data_quality_snapshot.clone(),
            model_run_id: Some(input.model_outcome.model_run_id),
            candidate_count: input.model_outcome.emitted,
            feature_rejected_count: u32::try_from(input.features.rejected.len())
                .unwrap_or(u32::MAX),
            market_selection_count: market_selection_count(&input.selection.included),
            empty: None,
            top_n: input.context.top_n,
        })
    }

    async fn load_config(
        &self,
        request: &BuildReportRequest,
    ) -> QuantResult<(RuntimeConfigVersionInfo, RuntimeConfig)> {
        let version = self
            .deps
            .runtime_config_repo
            .load_active_at(request.trigger_time)
            .await?
            .ok_or_else(|| QuantError::config("no active runtime config version"))?;
        let config = RuntimeConfig::from_json(&version.config_json)?;
        Ok((version, config))
    }

    async fn account_snapshot(
        &self,
        as_of: chrono::DateTime<Utc>,
        config: &RuntimeConfig,
    ) -> QuantResult<AccountSnapshot> {
        let caps = portfolio_caps(config)?;
        let provider = self
            .deps
            .account_provider_factory
            .create(Usd::new(caps.total_budget_usd))?;
        provider.snapshot(as_of).await
    }

    fn compose_empty(&self, input: EmptyComposeInput<'_>) -> QuantResult<ComposedReport> {
        let portfolio_plan = input.portfolio_plan.unwrap_or_else(|| {
            empty_plan_for_report(
                input.model_run_id.clone(),
                input.market_selection_id.clone(),
                input.context.as_of,
                input.account,
                &input.context.config,
                input.empty.reason,
                input.empty.rejected_count,
            )
        });
        self.deps.composer.compose(ComposeReportInput {
            trigger: &input.request.trigger,
            trigger_key: input.request.trigger.key(input.request.trigger_time),
            trigger_time: input.request.trigger_time,
            source_delay_secs: input.context.source_delay_secs,
            as_of: input.context.as_of,
            runtime_config_version_id: input.context.version.runtime_config_version_id.clone(),
            runtime_config: &input.context.config,
            runtime_mode: self.deps.runtime_mode.current(),
            model_version_id: input.context.active.model_version_id.clone(),
            market_selection_id: input.market_selection_id,
            account: input.account,
            account_snapshot: input.equity.account_snapshot.clone(),
            equity_snapshot: input.equity.equity_snapshot.clone(),
            portfolio_plan,
            planned: &[],
            planner_rejected: input.planner_rejected,
            captures: input.captures,
            feature_vector_by_market: input.feature_vector_by_market,
            data_quality_snapshot: input.data_quality_snapshot,
            model_run_id: input.model_run_id,
            candidate_count: input.empty.candidate_count,
            feature_rejected_count: if input.empty.reason == EmptyReason::InsufficientDataQuality {
                input.empty.rejected_count
            } else {
                0
            },
            market_selection_count: input.market_selection_count,
            empty: Some(input.empty),
            top_n: input.context.top_n,
        })
    }
}

/// Hourly aggregation bucket for the correlation mid-price lookback.
const CORRELATION_BUCKET_SECS: u32 = 3_600;

/// Align a token's sparse `bucket → mid` map onto the shared grid, carrying the
/// last observation forward (and back-filling leading gaps with the first
/// observation). An absent / empty series yields an empty vector, which the
/// estimator treats as insufficient history (proxy fallback).
fn aligned_series(
    series: Option<&std::collections::BTreeMap<i64, Decimal>>,
    grid: &[i64],
) -> Vec<Decimal> {
    let Some(series) = series.filter(|series| !series.is_empty()) else {
        return Vec::new();
    };
    let mut last = *series
        .values()
        .next()
        .expect("non-empty series has a first value");
    let mut out = Vec::with_capacity(grid.len());
    for bucket in grid {
        if let Some(value) = series.get(bucket) {
            last = *value;
        }
        out.push(last);
    }
    out
}

fn resolve_top_n(request: &BuildReportRequest, config: &RuntimeConfig) -> QuantResult<u32> {
    let top_n = match &request.trigger {
        ReportTrigger::Scheduled { schedule_id } => {
            config
                .reports
                .schedules
                .iter()
                .find(|schedule| schedule.schedule_id == *schedule_id)
                .ok_or_else(|| {
                    QuantError::config(format!("unknown report schedule {schedule_id}"))
                })?
                .top_n
        }
        ReportTrigger::AdHoc { .. } => request.top_n_override.ok_or_else(|| {
            QuantError::config("ad-hoc report requires an explicit top_n (no configured default)")
        })?,
    };
    if top_n == 0 || top_n > config.reports.max_top_n {
        return Err(QuantError::config(format!(
            "report top_n {top_n} outside 1..={}",
            config.reports.max_top_n
        )));
    }
    Ok(top_n)
}

fn checked_source_delay(source_delay_secs: u64) -> QuantResult<Duration> {
    let seconds = i64::try_from(source_delay_secs)
        .map_err(|error| QuantError::config(format!("source_delay_secs too large: {error}")))?;
    Ok(Duration::seconds(seconds))
}

fn bounded_usize(value: u32) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn resolve_source_delay(request: &BuildReportRequest, config: &RuntimeConfig) -> QuantResult<u64> {
    match &request.trigger {
        ReportTrigger::Scheduled { schedule_id } => {
            let schedule = config
                .reports
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
            Ok(schedule.source_delay_secs)
        }
        ReportTrigger::AdHoc { .. } => {
            if !config.reports.ad_hoc_report_enabled {
                return Err(QuantError::config("ad-hoc report generation is disabled"));
            }
            request.source_delay_secs_override.ok_or_else(|| {
                QuantError::config(
                    "ad-hoc report requires an explicit source_delay_secs (no configured default)",
                )
            })
        }
    }
}

pub(super) async fn report_as_of(
    runtime_config_repo: &dyn RuntimeConfigVersionRepository,
    request: &BuildReportRequest,
) -> QuantResult<chrono::DateTime<Utc>> {
    let version = runtime_config_repo
        .load_active_at(request.trigger_time)
        .await?
        .ok_or_else(|| QuantError::config("no active runtime config version"))?;
    let config = RuntimeConfig::from_json(&version.config_json)?;
    let source_delay_secs = resolve_source_delay(request, &config)?;
    Ok(request.trigger_time - checked_source_delay(source_delay_secs)?)
}

fn plan_candidates<'a>(
    candidates: &'a [SignalCandidate],
    selected: &'a [SelectedMarket],
) -> Vec<PlanCandidate<'a>> {
    candidates
        .iter()
        .filter_map(|candidate| {
            selected
                .iter()
                .find(|market| market.market_id == candidate.market_id)
                .map(|market| PlanCandidate {
                    candidate,
                    category: market.category,
                    event_id: Some(market.event_id.clone()),
                    liquidity_usd: market.liquidity_usd,
                    liquidity_score: candidate.liquidity_score,
                })
        })
        .collect()
}

fn market_selection_count(selected: &[SelectedMarket]) -> u32 {
    u32::try_from(selected.len()).unwrap_or(u32::MAX)
}

fn empty_data_quality_snapshot(
    context: &BuildContext,
    as_of: chrono::DateTime<Utc>,
) -> NewReportDataQualitySnapshot {
    NewReportDataQualitySnapshot {
        report_data_quality_snapshot_id: ReportDataQualitySnapshotId::from_v7(),
        as_of,
        runtime_config_version_id: context.version.runtime_config_version_id.clone(),
        tokens_json: ReportDataQualityTokens(Vec::new()),
    }
}

fn feature_vector_by_market(
    features: &FeaturePipelineResult,
) -> HashMap<MarketId, FeatureVectorId> {
    features
        .persisted
        .iter()
        .zip(features.accepted.iter())
        .map(|(info, vector)| (vector.market_id.clone(), info.feature_vector_id.clone()))
        .collect()
}

fn portfolio_caps(config: &RuntimeConfig) -> QuantResult<PortfolioCaps> {
    Ok(PortfolioCaps {
        total_budget_usd: parse_decimal(
            "portfolio.budget.total_budget_usd",
            &config.portfolio.budget.total_budget_usd.value,
        )?,
        max_single_recommendation_usd: parse_decimal(
            "portfolio.budget.max_single_recommendation_usd",
            &config.portfolio.budget.max_single_recommendation_usd.value,
        )?,
        min_recommendation_usd: parse_decimal(
            "portfolio.budget.min_recommendation_usd",
            &config.portfolio.budget.min_recommendation_usd.value,
        )?,
        max_market_exposure_usd: parse_decimal(
            "portfolio.constraints.max_market_exposure_usd",
            &config.portfolio.constraints.max_market_exposure_usd.value,
        )?,
        max_event_exposure_usd: parse_decimal(
            "portfolio.constraints.max_event_exposure_usd",
            &config.portfolio.constraints.max_event_exposure_usd.value,
        )?,
        max_category_exposure_usd: parse_decimal(
            "portfolio.constraints.max_category_exposure_usd",
            &config.portfolio.constraints.max_category_exposure_usd.value,
        )?,
        liquidity_usage_cap_pct: parse_decimal(
            "portfolio.constraints.liquidity_usage_cap_pct",
            &config.portfolio.constraints.liquidity_usage_cap_pct.value,
        )?,
    })
}

fn parse_decimal(field: &str, value: &str) -> QuantResult<Decimal> {
    value
        .trim()
        .parse::<Decimal>()
        .map_err(|error| QuantError::config(format!("{field} is not a valid decimal: {error}")))
}

fn liquidity_score_cap(config: &RuntimeConfig) -> QuantResult<Usd> {
    let caps = portfolio_caps(config)?;
    if caps.liquidity_usage_cap_pct > Decimal::ZERO
        && caps.max_single_recommendation_usd > Decimal::ZERO
    {
        return Ok(Usd::new(
            caps.max_single_recommendation_usd / caps.liquidity_usage_cap_pct,
        ));
    }
    Ok(Usd::new(Decimal::from(10_000)))
}

/// Map planner rejections to the report-level empty reason (04.2 §4 step 8).
fn empty_reason_from_planner_rejections(rejected: &[RejectedCandidate]) -> EmptyReason {
    if rejected
        .iter()
        .any(|rejected| matches!(rejected.reason, RejectionReason::BudgetExhausted))
    {
        EmptyReason::PortfolioBudgetExhausted
    } else {
        EmptyReason::NoPositiveSignal
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        enums::quant::{EmptyReason, RejectionReason},
        types::{MarketId, SignalCandidateId},
    };
    use quant_pivot_research::portfolio::RejectedCandidate;

    use super::empty_reason_from_planner_rejections;

    fn rejected(reason: RejectionReason) -> RejectedCandidate {
        RejectedCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            market_id: MarketId::new("0xmarket"),
            reason,
            detail: String::new(),
        }
    }

    #[test]
    fn empty_reason_uses_portfolio_budget_only_for_budget_exhausted() {
        assert_eq!(
            empty_reason_from_planner_rejections(&[rejected(RejectionReason::BudgetExhausted)]),
            EmptyReason::PortfolioBudgetExhausted
        );
        assert_eq!(
            empty_reason_from_planner_rejections(&[rejected(
                RejectionReason::AvailableCashExhausted
            )]),
            EmptyReason::NoPositiveSignal
        );
        assert_eq!(
            empty_reason_from_planner_rejections(&[rejected(RejectionReason::MarketCapExhausted)]),
            EmptyReason::NoPositiveSignal
        );
    }
}
