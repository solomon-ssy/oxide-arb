//! Report builder orchestration.

use std::sync::Arc;

use chrono::{Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{FeatureVectorInfo, PointInTimeDataSource, RuntimeConfigVersionInfo},
    enums::quant::{EmptyReason, RejectionReason},
    runtime_config::RuntimeConfig,
    types::{Bps, MarketSelectionId, ModelRunId, PortfolioPlanId, Usd},
};
use quant_pivot_repository::traits::{MarketSelectionRepository, RuntimeConfigVersionRepository};
use quant_pivot_research::{
    backtest::PortfolioCaps,
    features::PitView,
    model::SignalCandidate,
    portfolio::{
        AccountSnapshot, PlanCandidate, PortfolioPlanInput, PortfolioPlanOutput, PortfolioPlanner,
        RejectedCandidate, sizing_model_from_config,
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
    pub portfolio_planner: Arc<dyn PortfolioPlanner>,
    pub composer: Arc<dyn RecommendationComposer>,
    pub pit_source: Arc<dyn PointInTimeDataSource>,
    pub runtime_mode: RuntimeModeHandle,
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
    empty: EmptyReportContext,
    model_run_id: Option<ModelRunId>,
    selected: &'a [SelectedMarket],
    feature_infos: &'a [FeatureVectorInfo],
}

struct PublishedComposeInput<'a> {
    request: &'a BuildReportRequest,
    context: &'a BuildContext,
    selection: &'a MarketSelectionSnapshot,
    account: &'a AccountSnapshot,
    features: &'a FeaturePipelineResult,
    model_outcome: ModelRunOutcome,
    plan: PortfolioPlanOutput,
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
        let context = self.prepare_context(&request).await?;
        let selection = self.build_selection(&context).await?;
        let account = self
            .account_snapshot(context.as_of, &context.config)
            .await?;

        if selection.included.is_empty() {
            return self.compose_empty(EmptyComposeInput {
                request: &request,
                context: &context,
                market_selection_id: selection.market_selection_id.clone(),
                account: &account,
                empty: EmptyReportContext {
                    reason: EmptyReason::EmptySelection,
                    candidate_count: 0,
                    rejected_count: u32::try_from(selection.excluded.len()).unwrap_or(u32::MAX),
                    warnings: Vec::new(),
                },
                model_run_id: None,
                selected: &selection.included,
                feature_infos: &[],
            });
        }

        let features = self.build_features(&context, &selection).await?;
        if features.accepted.is_empty() {
            return self.compose_empty(EmptyComposeInput {
                request: &request,
                context: &context,
                market_selection_id: selection.market_selection_id.clone(),
                account: &account,
                empty: EmptyReportContext {
                    reason: EmptyReason::InsufficientDataQuality,
                    candidate_count: 0,
                    rejected_count: u32::try_from(features.rejected.len()).unwrap_or(u32::MAX),
                    warnings: vec!["feature pipeline accepted zero markets".to_owned()],
                },
                model_run_id: None,
                selected: &selection.included,
                feature_infos: &features.persisted,
            });
        }

        let model_outcome = self.run_model(&context, &selection, &features).await?;
        if model_outcome.accepted.is_empty() {
            return self.compose_empty(EmptyComposeInput {
                request: &request,
                context: &context,
                market_selection_id: selection.market_selection_id.clone(),
                account: &account,
                empty: EmptyReportContext {
                    reason: EmptyReason::NoPositiveSignal,
                    candidate_count: model_outcome.emitted,
                    rejected_count: 0,
                    warnings: vec!["active model emitted no positive candidate".to_owned()],
                },
                model_run_id: Some(model_outcome.model_run_id),
                selected: &selection.included,
                feature_infos: &features.persisted,
            });
        }

        let plan = self.plan_portfolio(&context, &selection, &model_outcome, &account)?;

        if plan.planned.is_empty() {
            return self.compose_empty(EmptyComposeInput {
                request: &request,
                context: &context,
                market_selection_id: selection.market_selection_id.clone(),
                account: &account,
                empty: EmptyReportContext {
                    reason: empty_reason_from_planner_rejections(&plan.rejected),
                    candidate_count: model_outcome.emitted,
                    rejected_count: u32::try_from(plan.rejected.len()).unwrap_or(u32::MAX),
                    warnings: Vec::new(),
                },
                model_run_id: Some(model_outcome.model_run_id),
                selected: &selection.included,
                feature_infos: &features.persisted,
            });
        }

        self.compose_published(PublishedComposeInput {
            request: &request,
            context: &context,
            selection: &selection,
            account: &account,
            features: &features,
            model_outcome,
            plan,
        })
    }
}

impl DefaultReportBuilder {
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
                features: &context.config.features,
                data_quality: &context.config.data_quality,
                model_requirements: &context.active.model_requirements,
                source_delay_secs: context.source_delay_secs,
                pit: PitView::Live(self.deps.pit_source.as_ref()),
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
        &self,
        context: &BuildContext,
        selection: &MarketSelectionSnapshot,
        model_outcome: &ModelRunOutcome,
        account: &AccountSnapshot,
    ) -> QuantResult<PortfolioPlanOutput> {
        let caps = portfolio_caps(&context.config)?;
        let sizing = sizing_model_from_config(&context.config.portfolio.sizing)?;
        let plan_candidates = plan_candidates(&model_outcome.accepted, &selection.included);
        self.deps.portfolio_planner.plan(PortfolioPlanInput {
            portfolio_plan_id: PortfolioPlanId::from_v7(),
            model_run_id: model_outcome.model_run_id.clone(),
            market_selection_id: selection.market_selection_id.clone(),
            as_of: context.as_of,
            candidates: plan_candidates,
            account,
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
            sizing: sizing.as_ref(),
            entry_max_slippage_bps: Bps::new(Decimal::from(
                context.config.execution.entry_order_policy.max_slippage_bps,
            )),
            top_n: bounded_usize(context.top_n),
        })
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
            portfolio_plan: plan_row,
            planned: &planned,
            planner_rejected: &rejected,
            selected: &input.selection.included,
            feature_infos: &input.features.persisted,
            model_run_id: Some(input.model_outcome.model_run_id),
            candidate_count: input.model_outcome.emitted,
            feature_rejected_count: u32::try_from(input.features.rejected.len())
                .unwrap_or(u32::MAX),
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
        let portfolio_plan = empty_plan_for_report(
            input.model_run_id.clone(),
            input.market_selection_id.clone(),
            input.context.as_of,
            input.account,
            &input.context.config,
            input.empty.reason,
            input.empty.rejected_count,
        );
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
            portfolio_plan,
            planned: &[],
            planner_rejected: &[],
            selected: input.selected,
            feature_infos: input.feature_infos,
            model_run_id: input.model_run_id,
            candidate_count: input.empty.candidate_count,
            feature_rejected_count: if input.empty.reason == EmptyReason::InsufficientDataQuality {
                input.empty.rejected_count
            } else {
                0
            },
            empty: Some(input.empty),
            top_n: input.context.top_n,
        })
    }
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
        ReportTrigger::AdHoc { .. } => request
            .top_n_override
            .unwrap_or(config.reports.default_top_n),
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
            Ok(request
                .source_delay_secs_override
                .unwrap_or(config.data_quality.source_delay_secs))
        }
    }
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
                })
        })
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
