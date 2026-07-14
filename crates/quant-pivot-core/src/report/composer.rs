//! Report payload composition.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, report::ReportError};
use quant_pivot_models::{
    clickhouse::{ChProbability, ChUsd, QuantRecommendationEventRow},
    domain::{
        MarketSubject, NewAccountSnapshot, NewEntryConditionArtifact, NewEntryConditionInstance,
        NewEquitySnapshot, NewOperationLog, NewPortfolioPlan, NewRecommendation,
        NewRecommendationReport, NewReportDataQualitySnapshot, NewReportTransaction,
        PriceComparator, TradePolicyArtifactInfo, market::book::BookLevel,
    },
    enums::{
        common::{MarketCategory, TickSize},
        domain::LinkageSourceRole,
        operation_log::{OperationCategory, OperationOutcome},
        quant::{
            EmptyReportReason, EntryConditionState, IneligibilityReason, OutcomeSide,
            PriceComparison, QuantRuntimeMode, RecommendationReportStatus, RecommendationStatus,
            ReportKind,
        },
        rbac::ResourceType,
    },
    hashing::canonical_state_hash,
    runtime_config::RuntimeConfig,
    types::{
        Bps, ClockAnchor, ClockCondition, ConditionTruth, ConfidenceSummary, ConfirmationPolicy,
        ContentHash, CryptoSubjectPredicateEntered, ENTRY_CONDITION_EVALUATOR_VERSION,
        ENTRY_CONDITION_SCHEMA_VERSION, EligibilitySummary, EntryConditionArtifactId,
        EntryConditionArtifactV1, EntryConditionBinding, EntryConditionFactorBinding,
        EntryConditionInstanceId, EntryConditionPlan, EntryConditionSourceBinding,
        EntryConditionTemplate, EntryConditionTemplateV1, EntryConditionV1, EntryOrderPolicy,
        EntryOrderTemplate, EntryPlan, EventId, EvidenceRefs, EvidenceRefsInput,
        ExecutionEligibility, ExitPlan, FactorBreakdownEntry, FactorCondition, FactorDefinitionId,
        FeatureVectorId, MarketEventCondition, MarketEventTemplate, MarketId, MarketSelectionId,
        ModelRunId, ModelVersionId, OperationLogId, PortfolioConstraintsSnapshot,
        PortfolioOptimizerMeta, PortfolioPlanId, PortfolioRejectedSummary, PortfolioRiskBudget,
        Price, PriceCondition, Probability, RecommendationFactorBreakdown, RecommendationId,
        RecommendationReportId, RecommendationTradePlan, RejectionReasonCount,
        ReportDataQualitySnapshotId, ReportSummary, RiskEnvelope, RuntimeConfigVersionId,
        ScaleOutTarget, SizingPlan, ThesisInvalidationPolicy, TradePlanBlocker, TradePolicyCohort,
        TradePolicyCohortProvenance, TrailingStopPolicy, Usd, WeatherDailyHighEnteredBand,
        WeatherDailyHighExceededBandUpper, WeatherObservationDayClosedOutsideBand,
    },
};
use quant_pivot_research::{
    features::MarketDecisionCapture,
    model::SignalCandidate,
    portfolio::{AccountSnapshot, PlannedRecommendation, RejectedCandidate},
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use super::types::{
    ComposedReport, EmptyReportContext, NotificationRecommendation, ReportNotificationPayload,
    ReportTrigger,
};

/// Inputs required to compose one report artifact.
pub struct ComposeReportInput<'a> {
    pub trigger: &'a ReportTrigger,
    pub trigger_key: String,
    pub trigger_time: DateTime<Utc>,
    pub knowledge_lag_secs: u64,
    pub decision_at: DateTime<Utc>,
    pub published_at: DateTime<Utc>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub runtime_config: &'a RuntimeConfig,
    pub runtime_mode: QuantRuntimeMode,
    pub model_version_id: ModelVersionId,
    pub market_selection_id: MarketSelectionId,
    pub market_selection_hash: ContentHash,
    pub account: &'a AccountSnapshot,
    pub account_snapshot: NewAccountSnapshot,
    pub equity_snapshot: NewEquitySnapshot,
    pub portfolio_plan: NewPortfolioPlan,
    pub planned: &'a [PlannedRecommendation],
    pub planner_rejected: &'a [RejectedCandidate],
    pub captures: HashMap<MarketId, MarketDecisionCapture>,
    pub feature_vector_by_market: HashMap<MarketId, FeatureVectorId>,
    pub data_quality_snapshot: NewReportDataQualitySnapshot,
    pub model_run_id: Option<ModelRunId>,
    pub candidate_count: u32,
    pub feature_rejected_count: u32,
    pub market_selection_count: u32,
    pub empty: Option<EmptyReportContext>,
    pub top_n: u32,
    pub return_model_calibrated: bool,
    pub trade_policy: Option<&'a TradePolicyArtifactInfo>,
    /// Exact source-native reference price visible at the decision boundary,
    /// keyed by market. Only relative Crypto event predicates consume it.
    pub crypto_reference_prices: HashMap<MarketId, Usd>,
}

/// Converts builder/planner output into persistence rows and post-commit events.
pub trait RecommendationComposer: Send + Sync {
    /// Compose the complete report transaction and post-commit side-effect rows.
    fn compose(&self, input: ComposeReportInput<'_>) -> QuantResult<ComposedReport>;
}

/// Production recommendation composer.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultRecommendationComposer;

impl DefaultRecommendationComposer {
    /// Build the composer.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl RecommendationComposer for DefaultRecommendationComposer {
    fn compose(&self, input: ComposeReportInput<'_>) -> QuantResult<ComposedReport> {
        let report_id = RecommendationReportId::from_v7();
        let fallback_horizon_secs = input.runtime_config.reports.fallback_horizon_secs;
        let entry_window_ratio = parse_decimal(
            "reports.entry_window_ratio",
            &input.runtime_config.reports.entry_window_ratio.value,
        )?;
        let data_quality_snapshot_ref = input
            .data_quality_snapshot
            .report_data_quality_snapshot_id
            .clone();

        let recommendation_rows = input
            .planned
            .iter()
            .map(|planned| {
                compose_recommendation(
                    &report_id,
                    planned,
                    &input,
                    entry_window_ratio,
                    fallback_horizon_secs,
                    &data_quality_snapshot_ref,
                )
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let recommendations = recommendation_rows
            .iter()
            .map(|rows| rows.recommendation.clone())
            .collect::<Vec<_>>();
        let entry_condition_artifacts = recommendation_rows
            .iter()
            .filter_map(|rows| rows.artifact.clone())
            .collect();
        let entry_condition_instances = recommendation_rows
            .into_iter()
            .map(|rows| rows.instance)
            .collect();

        // Report validity is the data-driven roll-up of its recommendations'
        // validity (latest entry-by); an empty report falls back to the governed
        // horizon so it still ages out.
        let report_valid_until = match recommendations.iter().map(|rec| rec.valid_until).max() {
            Some(max) => max,
            None => instant_plus_secs(input.decision_at, fallback_horizon_secs)?,
        };

        let status = if recommendations.is_empty() {
            RecommendationReportStatus::PublishedEmpty
        } else {
            RecommendationReportStatus::Published
        };
        let summary = report_summary(&input, &recommendations)?;
        // Build the notification before `summary` / `recommendations` move below.
        let notification =
            report_notification(&report_id, status, &input, &summary, &recommendations)?;

        let report = build_report_header(
            &report_id,
            &input,
            status,
            report_valid_until,
            data_quality_snapshot_ref,
            summary,
        )?;
        let operation_log = operation_log(&report_id, &input, status, &report)?;
        let ch_rows = recommendation_events(&report_id, &recommendations, input.published_at)?;

        Ok(ComposedReport {
            transaction: NewReportTransaction {
                feature_parity_state_id: None,
                account_snapshot: input.account_snapshot,
                equity_snapshot: input.equity_snapshot,
                data_quality_snapshot: input.data_quality_snapshot,
                portfolio_plan: input.portfolio_plan,
                report,
                recommendations,
                entry_condition_artifacts,
                entry_condition_instances,
                sampled_feature_parity: None,
                operation_log,
            },
            ch_rows,
            notification,
            delivery_policy: input.runtime_config.reports.delivery_policy,
            notify_operators: input.runtime_config.notification.policies.report_published,
        })
    }
}

fn build_report_header(
    report_id: &RecommendationReportId,
    input: &ComposeReportInput<'_>,
    status: RecommendationReportStatus,
    valid_until: DateTime<Utc>,
    data_quality_snapshot_ref: ReportDataQualitySnapshotId,
    summary: ReportSummary,
) -> QuantResult<NewRecommendationReport> {
    let fallback_horizon_secs = input.runtime_config.reports.fallback_horizon_secs;
    Ok(NewRecommendationReport {
        recommendation_report_id: report_id.clone(),
        report_kind: ReportKind::TopN,
        trigger_kind: input.trigger.kind(),
        trigger_key: input.trigger_key.clone(),
        trigger_time: input.trigger_time,
        knowledge_lag_secs: i64::try_from(input.knowledge_lag_secs).map_err(|error| {
            QuantError::config(format!("knowledge_lag_secs too large: {error}"))
        })?,
        decision_at: input.decision_at,
        horizon_secs: i64::try_from(fallback_horizon_secs).map_err(|error| {
            QuantError::config(format!("reports.fallback_horizon_secs too large: {error}"))
        })?,
        runtime_mode: input.runtime_mode,
        runtime_config_version_id: input.runtime_config_version_id.clone(),
        model_run_id: input.model_run_id.clone(),
        model_version_id: input.model_version_id.clone(),
        market_selection_id: input.market_selection_id.clone(),
        portfolio_plan_id: input.portfolio_plan.portfolio_plan_id.clone(),
        top_n: i32::try_from(input.top_n).map_err(|error| {
            QuantError::config(format!("reports top_n exceeds i32::MAX: {error}"))
        })?,
        status,
        account_source: input.account.source,
        capital_base_usd: input.account.capital_base_usd,
        account_snapshot_ref: input.account_snapshot.account_snapshot_id.clone(),
        equity_snapshot_ref: input.equity_snapshot.equity_snapshot_id.clone(),
        data_quality_snapshot_ref,
        summary_json: summary,
        published_at: Some(input.published_at),
        valid_until: Some(valid_until),
        revoked_at: None,
        expired_at: None,
        status_reason: input
            .empty
            .as_ref()
            .map(|empty| empty.reason.as_str().to_owned()),
    })
}

/// `instant + secs`, failing closed on overflow.
fn instant_plus_secs(instant: DateTime<Utc>, secs: u64) -> QuantResult<DateTime<Utc>> {
    let secs = i64::try_from(secs)
        .map_err(|error| QuantError::config(format!("horizon seconds too large: {error}")))?;
    instant
        .checked_add_signed(Duration::seconds(secs))
        .ok_or_else(|| QuantError::config("horizon deadline is outside chrono range"))
}

/// Per-recommendation effective horizon (seconds): the model's frozen prediction
/// horizon (`suggested_horizon_secs`), falling back to the governed default when
/// the model supplies none (classical / non-ML runs), capped by the market's
/// time-to-resolution hard wall. A zero result means there is no actionable
/// prediction window and must be rejected by the caller.
const fn effective_horizon_secs(
    candidate: &SignalCandidate,
    capture: &MarketDecisionCapture,
    fallback_horizon_secs: u64,
) -> u64 {
    effective_horizon_from(
        candidate.suggested_horizon_secs,
        capture.market_context.time_to_resolution_secs,
        fallback_horizon_secs,
    )
}

/// Pure core of [`effective_horizon_secs`]: the model horizon (or the governed
/// fallback when absent) capped by the market's time-to-resolution.
const fn effective_horizon_from(
    suggested_horizon_secs: u64,
    time_to_resolution_secs: Option<u64>,
    fallback_horizon_secs: u64,
) -> u64 {
    let base = if suggested_horizon_secs > 0 {
        suggested_horizon_secs
    } else {
        fallback_horizon_secs
    };
    match time_to_resolution_secs {
        Some(ttr) if ttr < base => ttr,
        _ => base,
    }
}

/// Entry-window length (seconds) = `effective_horizon * entry_window_ratio`,
/// rounded, floored at 1s — the recommendation accepts new entries only while at
/// least `ratio` of the signal's edge horizon remains (the half-life point at
/// `ratio = 0.5`). The exit / time-stop still uses the full effective horizon.
fn entry_window_secs(effective_horizon_secs: u64, entry_window_ratio: Decimal) -> QuantResult<u64> {
    if effective_horizon_secs == 0 {
        return Err(ReportError::InvariantViolation {
            stage: "compose",
            detail: "recommendation has no actionable prediction horizon".to_owned(),
        }
        .into());
    }
    if entry_window_ratio <= Decimal::ZERO || entry_window_ratio > Decimal::ONE {
        return Err(QuantError::config(
            "reports.entry_window_ratio must be in (0, 1]",
        ));
    }
    let secs = (Decimal::from(effective_horizon_secs) * entry_window_ratio).round();
    let secs = secs.to_u64().ok_or_else(|| ReportError::NumericOverflow {
        field: "recommendation.entry_window_secs",
        detail: format!("cannot represent rounded window {secs} as u64"),
    })?;
    Ok(secs.max(1))
}

/// Build the operator-notification payload from the composed report parts.
fn report_notification(
    report_id: &RecommendationReportId,
    status: RecommendationReportStatus,
    input: &ComposeReportInput<'_>,
    summary: &ReportSummary,
    recommendations: &[NewRecommendation],
) -> QuantResult<ReportNotificationPayload> {
    Ok(ReportNotificationPayload {
        report_id: report_id.clone(),
        kind: ReportKind::TopN,
        status: status.as_str().to_owned(),
        runtime_mode: input.runtime_mode,
        published_count: u32::try_from(recommendations.len()).map_err(|error| {
            ReportError::NumericOverflow {
                field: "report.notification.published_count",
                detail: error.to_string(),
            }
        })?,
        total_suggested_usd: summary.total_suggested_usd,
        top3: recommendations
            .iter()
            .take(3)
            .map(|rec| NotificationRecommendation {
                market_id: rec.market_id.to_string(),
                outcome_side: rec.outcome_side,
                score: rec.composite_score,
                suggested_usd: rec.trade_plan.sizing().map(|sizing| sizing.suggested_usd),
            })
            .collect(),
        warnings: summary.warnings.clone(),
        empty_reason: input.empty.as_ref().map(|empty| empty.reason),
    })
}

fn compose_recommendation(
    report_id: &RecommendationReportId,
    planned: &PlannedRecommendation,
    input: &ComposeReportInput<'_>,
    entry_window_ratio: Decimal,
    fallback_horizon_secs: u64,
    data_quality_snapshot_ref: &ReportDataQualitySnapshotId,
) -> QuantResult<ComposedRecommendationRows> {
    let candidate = &planned.candidate;
    let capture = input.captures.get(&candidate.market_id).ok_or_else(|| {
        ReportError::InvariantViolation {
            stage: "compose",
            detail: format!(
                "missing decision capture for recommendation market {}",
                candidate.market_id
            ),
        }
    })?;

    let horizon_secs = effective_horizon_secs(candidate, capture, fallback_horizon_secs);
    let valid_until = actionable_valid_until(
        input.decision_at,
        input.published_at,
        entry_window_secs(horizon_secs, entry_window_ratio)?,
        &candidate.market_id,
    )?;
    let compose_context = resolve_compose_context(candidate, input)?;
    let policy = if input.return_model_calibrated {
        resolve_recommendation_policy(planned, capture, input, horizon_secs)
    } else {
        Err(vec![TradePlanBlocker::ReturnModelUncalibrated])
    };
    let mut auto_gate = calibrated_auto_execution_gate(
        candidate,
        planned.rank,
        &planned.sizing,
        input.runtime_config,
        input.return_model_calibrated,
    )?;
    if policy.is_err() {
        auto_gate.allowed = false;
        if !auto_gate
            .reasons
            .contains(&IneligibilityReason::TradePolicyUnavailable)
        {
            auto_gate
                .reasons
                .push(IneligibilityReason::TradePolicyUnavailable);
        }
    }
    let risk_envelope = compose_risk_envelope(planned, candidate, &auto_gate);

    let assembly = NewRecommendationAssembly {
        report_id,
        planned,
        candidate,
        capture,
        input,
        compose_context,
        valid_until,
        data_quality_snapshot_ref,
        auto_gate: &auto_gate,
        risk_envelope,
        policy,
    };
    build_new_recommendation(assembly)
}

fn actionable_valid_until(
    decision_at: DateTime<Utc>,
    published_at: DateTime<Utc>,
    entry_window_secs: u64,
    market_id: &MarketId,
) -> QuantResult<DateTime<Utc>> {
    let valid_until = instant_plus_secs(decision_at, entry_window_secs)?;
    if valid_until <= published_at {
        return Err(ReportError::InvariantViolation {
            stage: "compose",
            detail: format!(
                "recommendation market {market_id} has no actionable entry window at \
                 publication: published_at={published_at} valid_until={valid_until}"
            ),
        }
        .into());
    }
    Ok(valid_until)
}

fn resolve_compose_context(
    candidate: &SignalCandidate,
    input: &ComposeReportInput<'_>,
) -> QuantResult<ComposeRecommendationContext> {
    let feature_vector_id = input
        .feature_vector_by_market
        .get(&candidate.market_id)
        .cloned()
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "compose",
            detail: format!(
                "missing persisted feature vector for recommendation market {}",
                candidate.market_id
            ),
        })?;
    let model_run_id =
        input
            .model_run_id
            .clone()
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "compose",
                detail: "non-empty recommendation missing model_run_id".into(),
            })?;
    Ok(ComposeRecommendationContext {
        feature_vector_id,
        model_run_id,
    })
}

struct ComposeRecommendationContext {
    feature_vector_id: FeatureVectorId,
    model_run_id: ModelRunId,
}

#[derive(Clone)]
struct ResolvedRecommendationPolicy {
    provenance: TradePolicyCohortProvenance,
    cohort: TradePolicyCohort,
    executable_entry_price: Price,
    worst_ask_price: Price,
}

fn resolve_recommendation_policy(
    planned: &PlannedRecommendation,
    capture: &MarketDecisionCapture,
    input: &ComposeReportInput<'_>,
    horizon_secs: u64,
) -> Result<ResolvedRecommendationPolicy, Vec<TradePlanBlocker>> {
    let Some(artifact) = input.trade_policy else {
        return Err(vec![TradePlanBlocker::ModelPolicyBindingMissing]);
    };
    let (entry_price, worst_ask_price) =
        ask_vwap_for_usd(&capture.book.asks, planned.sizing.suggested_usd)
            .ok_or_else(|| vec![TradePlanBlocker::LiquidityInsufficient])?;
    let liquidity_tier = match capture.market_context.depth_usd.inner() {
        value if value >= Decimal::from(10_000) => "deep",
        value if value >= Decimal::from(1_000) => "medium",
        _ => "shallow",
    };
    let candidates = artifact
        .payload_json
        .cohorts
        .iter()
        .enumerate()
        .filter(|(_, cohort)| {
            cohort.key.category == capture.identity.category
                && cohort.key.horizon_secs == horizon_secs
                && cohort.key.notional_tier == planned.sizing.suggested_usd
                && cohort.key.liquidity.bucket_id == liquidity_tier
                && entry_price >= cohort.key.entry_price_min
                && (entry_price < cohort.key.entry_price_max
                    || (cohort.key.entry_price_max == Price::ONE && entry_price == Price::ONE))
        })
        .collect::<Vec<_>>();
    let [(cohort_index, cohort)] = candidates.as_slice() else {
        return Err(vec![TradePlanBlocker::CohortNotFound]);
    };
    if cohort.executable_coverage
        < artifact
            .payload_json
            .fit_contract
            .quality_gate
            .min_executable_coverage
        || cohort.lower_confidence_utility_bps
            < Some(
                artifact
                    .payload_json
                    .fit_contract
                    .quality_gate
                    .min_lower_confidence_utility_bps,
            )
    {
        return Err(vec![TradePlanBlocker::CohortCoverageInsufficient]);
    }
    let cohort_index = u32::try_from(*cohort_index)
        .map_err(|_| vec![TradePlanBlocker::ArtifactFormatUnsupported])?;
    Ok(ResolvedRecommendationPolicy {
        provenance: TradePolicyCohortProvenance {
            artifact_id: artifact.artifact_id.clone(),
            artifact_hash: artifact.content_hash.clone(),
            cohort_index,
            cohort_key: cohort.key.clone(),
        },
        cohort: (*cohort).clone(),
        executable_entry_price: entry_price,
        worst_ask_price,
    })
}

fn ask_vwap_for_usd(asks: &[BookLevel], target: Usd) -> Option<(Price, Price)> {
    if !target.is_positive() {
        return None;
    }
    let mut remaining = target.inner();
    let mut shares = Decimal::ZERO;
    let mut worst = None;
    for level in asks {
        if remaining <= Decimal::ZERO {
            break;
        }
        let price = level.price_decimal();
        if !price.is_positive() || !level.size_decimal().is_positive() {
            continue;
        }
        let available_usd = price.inner() * level.size_decimal().inner();
        let spend = available_usd.min(remaining);
        shares += spend / price.inner();
        remaining -= spend;
        worst = Some(price);
    }
    if remaining > Decimal::ZERO || shares <= Decimal::ZERO {
        return None;
    }
    Some((Price::new(target.inner() / shares), worst?))
}

struct MaterializedEntryPlan {
    plan: EntryPlan,
    artifact: Option<NewEntryConditionArtifact>,
}

#[derive(Clone, Copy)]
struct EntryPlanMaterialization<'a> {
    recommendation_id: &'a RecommendationId,
    published_at: DateTime<Utc>,
    valid_until: DateTime<Utc>,
    tick_size: TickSize,
    candidate: &'a SignalCandidate,
    capture: &'a MarketDecisionCapture,
    input: &'a ComposeReportInput<'a>,
    policy: &'a ResolvedRecommendationPolicy,
}

fn policy_entry_plan(context: EntryPlanMaterialization<'_>) -> QuantResult<MaterializedEntryPlan> {
    let EntryPlanMaterialization {
        recommendation_id,
        published_at,
        valid_until,
        tick_size,
        candidate,
        capture,
        input,
        policy,
    } = context;
    let (condition, artifact) = match &policy.cohort.entry_condition {
        EntryConditionTemplate::Immediate => (EntryConditionPlan::Immediate, None),
        EntryConditionTemplate::Conditional {
            root,
            confirmation_ms,
            max_observation_gap_ms,
        } => {
            let root = materialize_condition_tree(root, tick_size, candidate, capture, input)?;
            let (factor_bindings, source_bindings) = condition_leaf_bindings(&root);
            if !source_bindings.is_empty()
                && (capture.market_linkage_id.is_none() || capture.market_linkage_hash.is_none())
            {
                return Err(ReportError::InvariantViolation {
                    stage: "entry_condition",
                    detail: format!(
                        "market-event condition for market {} has no PIT linkage revision",
                        candidate.market_id
                    ),
                }
                .into());
            }
            let (condition, artifact) = materialize_condition_artifact(ConditionArtifactInput {
                recommendation_id,
                candidate,
                capture,
                input,
                root,
                confirmation_ms: *confirmation_ms,
                max_observation_gap_ms: *max_observation_gap_ms,
                factor_bindings,
                source_bindings,
            })?;
            (condition, Some(artifact))
        }
    };
    let order_policy = match policy.cohort.entry_order {
        EntryOrderTemplate::Passive { post_only } => EntryOrderPolicy::Passive {
            limit_price: policy.executable_entry_price,
            post_only,
        },
        EntryOrderTemplate::Aggressive { fill_requirement } => EntryOrderPolicy::Aggressive {
            worst_price: policy.worst_ask_price,
            fill_requirement,
        },
    };
    let mut plan = EntryPlan {
        condition,
        order_policy,
        max_slippage_bps: policy.cohort.max_slippage_bps,
        valid_from: published_at,
        valid_until,
        min_depth_usd: policy.cohort.key.notional_tier,
        max_book_age_ms: policy.cohort.max_book_age_ms,
        cancel_if_not_triggered: true,
        entry_reason: format!(
            "published trade policy {} cohort {}",
            policy.provenance.artifact_id, policy.provenance.cohort_index
        ),
    };
    align_entry_plan_to_tick(&mut plan, tick_size);
    Ok(MaterializedEntryPlan { plan, artifact })
}

struct ConditionArtifactInput<'a> {
    recommendation_id: &'a RecommendationId,
    candidate: &'a SignalCandidate,
    capture: &'a MarketDecisionCapture,
    input: &'a ComposeReportInput<'a>,
    root: EntryConditionV1,
    confirmation_ms: u64,
    max_observation_gap_ms: u64,
    factor_bindings: Vec<EntryConditionFactorBinding>,
    source_bindings: Vec<EntryConditionSourceBinding>,
}

fn materialize_condition_artifact(
    artifact_input: ConditionArtifactInput<'_>,
) -> QuantResult<(EntryConditionPlan, NewEntryConditionArtifact)> {
    let ConditionArtifactInput {
        recommendation_id,
        candidate,
        capture,
        input,
        root,
        confirmation_ms,
        max_observation_gap_ms,
        factor_bindings,
        source_bindings,
    } = artifact_input;
    let artifact = EntryConditionArtifactV1 {
        schema_version: ENTRY_CONDITION_SCHEMA_VERSION,
        evaluator_version: ENTRY_CONDITION_EVALUATOR_VERSION,
        binding: EntryConditionBinding {
            recommendation_id: recommendation_id.clone(),
            market_id: candidate.market_id.clone(),
            token_id: candidate.token_id.clone(),
            outcome_side: candidate.outcome_side,
            market_linkage_id: capture.market_linkage_id.clone(),
            market_linkage_hash: capture.market_linkage_hash.clone(),
            catalog_snapshot_id: input.market_selection_id.clone(),
            catalog_snapshot_hash: input.market_selection_hash.clone(),
            model_version_id: input.model_version_id.clone(),
            runtime_config_version_id: input.runtime_config_version_id.clone(),
            factor_bindings,
            source_bindings,
        },
        confirmation: ConfirmationPolicy {
            required_continuous_ms: confirmation_ms,
            max_observation_gap_ms,
        },
        root,
    }
    .canonicalize()
    .map_err(|error| ReportError::InvariantViolation {
        stage: "entry_condition",
        detail: error.to_string(),
    })?;
    let content_hash =
        artifact
            .canonical_content_hash()
            .map_err(|error| ReportError::InvariantViolation {
                stage: "entry_condition",
                detail: error.to_string(),
            })?;
    let artifact_id = EntryConditionArtifactId::from_content_hash(&content_hash);
    Ok((
        EntryConditionPlan::Conditional {
            artifact_id: artifact_id.clone(),
            content_hash: content_hash.clone(),
        },
        NewEntryConditionArtifact {
            artifact_id,
            content_hash,
            schema_version: i32::try_from(ENTRY_CONDITION_SCHEMA_VERSION).map_err(|error| {
                ReportError::NumericOverflow {
                    field: "entry_condition.schema_version",
                    detail: error.to_string(),
                }
            })?,
            evaluator_version: i32::try_from(ENTRY_CONDITION_EVALUATOR_VERSION).map_err(
                |error| ReportError::NumericOverflow {
                    field: "entry_condition.evaluator_version",
                    detail: error.to_string(),
                },
            )?,
            payload_json: artifact,
        },
    ))
}

fn materialize_condition_tree(
    node: &EntryConditionTemplateV1,
    tick_size: TickSize,
    candidate: &SignalCandidate,
    capture: &MarketDecisionCapture,
    input: &ComposeReportInput<'_>,
) -> QuantResult<EntryConditionV1> {
    match node {
        EntryConditionTemplateV1::Price {
            comparison,
            threshold,
            max_input_age_ms,
        } => {
            let direction = match comparison {
                PriceComparison::AtOrAbove => TickDirection::Up,
                PriceComparison::AtOrBelow => TickDirection::Down,
            };
            Ok(EntryConditionV1::Price(PriceCondition {
                token_id: candidate.token_id.clone(),
                comparison: *comparison,
                threshold: tick_aligned_price(threshold.inner(), tick_size, direction),
                max_input_age_ms: *max_input_age_ms,
            }))
        }
        EntryConditionTemplateV1::Clock { anchor, offset_ms } => {
            materialize_clock_condition(*anchor, *offset_ms, candidate, capture, input.decision_at)
        }
        EntryConditionTemplateV1::Factor {
            definition_id,
            definition_hash,
            measure,
            comparison,
            threshold,
            minimum_confidence,
            max_input_age_ms,
        } => {
            if FactorDefinitionId::from_definition_hash(definition_hash) != *definition_id {
                return Err(ReportError::InvariantViolation {
                    stage: "entry_condition",
                    detail: format!("factor {definition_id} definition id/hash mismatch"),
                }
                .into());
            }
            Ok(EntryConditionV1::Factor(FactorCondition {
                definition_id: definition_id.clone(),
                definition_hash: definition_hash.clone(),
                model_version_id: input.model_version_id.clone(),
                measure: *measure,
                comparison: *comparison,
                threshold: *threshold,
                minimum_confidence: *minimum_confidence,
                max_input_age_ms: *max_input_age_ms,
            }))
        }
        EntryConditionTemplateV1::MarketEvent { event } => {
            materialize_market_event(*event, candidate, capture, input)
        }
        EntryConditionTemplateV1::All { children } => Ok(EntryConditionV1::All {
            children: children
                .iter()
                .map(|child| {
                    materialize_condition_tree(child, tick_size, candidate, capture, input)
                })
                .collect::<QuantResult<Vec<_>>>()?,
        }),
        EntryConditionTemplateV1::Any { children } => Ok(EntryConditionV1::Any {
            children: children
                .iter()
                .map(|child| {
                    materialize_condition_tree(child, tick_size, candidate, capture, input)
                })
                .collect::<QuantResult<Vec<_>>>()?,
        }),
    }
}

fn materialize_clock_condition(
    anchor: ClockAnchor,
    offset_ms: i64,
    candidate: &SignalCandidate,
    capture: &MarketDecisionCapture,
    decision_at: DateTime<Utc>,
) -> QuantResult<EntryConditionV1> {
    let anchor_at = match anchor {
        ClockAnchor::RecommendationDecision => decision_at,
        ClockAnchor::MarketStart => {
            capture
                .market
                .start_date
                .ok_or_else(|| ReportError::InvariantViolation {
                    stage: "entry_condition",
                    detail: format!(
                        "market {} has no frozen market_start clock anchor",
                        candidate.market_id
                    ),
                })?
        }
        ClockAnchor::MarketEnd => {
            capture
                .market
                .end_date
                .ok_or_else(|| ReportError::InvariantViolation {
                    stage: "entry_condition",
                    detail: format!(
                        "market {} has no frozen market_end clock anchor",
                        candidate.market_id
                    ),
                })?
        }
    };
    let deadline_at = anchor_at
        .checked_add_signed(Duration::milliseconds(offset_ms))
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "entry_condition",
            detail: "clock condition deadline is outside chrono range".to_owned(),
        })?;
    Ok(EntryConditionV1::Clock(ClockCondition {
        anchor,
        anchor_at,
        offset_ms,
        deadline_at,
    }))
}

fn materialize_market_event(
    template: MarketEventTemplate,
    candidate: &SignalCandidate,
    capture: &MarketDecisionCapture,
    input: &ComposeReportInput<'_>,
) -> QuantResult<EntryConditionV1> {
    let binding =
        capture
            .domain_binding
            .as_ref()
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "entry_condition",
                detail: format!(
                    "market-event condition for market {} has no PIT typed binding",
                    candidate.market_id
                ),
            })?;
    let live = binding
        .source_bindings
        .iter()
        .find(|source| source.role == LinkageSourceRole::LiveEvent)
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "entry_condition",
            detail: format!(
                "market-event condition for market {} has no live-event source binding",
                candidate.market_id
            ),
        })?;
    let source = EntryConditionSourceBinding {
        source_id: live.source_id.clone(),
        instrument_key: live.instrument_key.clone(),
        binding_hash: live.binding_hash.clone(),
    };
    let family_matches = matches!(
        (&binding.subject, template),
        (
            MarketSubject::Crypto(_),
            MarketEventTemplate::CryptoSubjectPredicateEntered { .. }
        ) | (
            MarketSubject::Weather(_),
            MarketEventTemplate::WeatherDailyHighPredicate { .. }
        )
    );
    if !family_matches {
        return Err(ReportError::InvariantViolation {
            stage: "entry_condition",
            detail: format!(
                "market-event template family does not match market {} linkage family",
                candidate.market_id
            ),
        }
        .into());
    }
    let event = materialize_event_payload(&binding.subject, template, source, candidate, input)?;
    Ok(EntryConditionV1::MarketEvent { event })
}

fn materialize_event_payload(
    subject: &MarketSubject,
    template: MarketEventTemplate,
    source: EntryConditionSourceBinding,
    candidate: &SignalCandidate,
    input: &ComposeReportInput<'_>,
) -> QuantResult<MarketEventCondition> {
    let event = match (subject, template) {
        (
            MarketSubject::Crypto(subject),
            MarketEventTemplate::CryptoSubjectPredicateEntered { max_input_age_ms },
        ) => MarketEventCondition::CryptoSubjectPredicateEntered(CryptoSubjectPredicateEntered {
            source,
            comparator: subject.comparator,
            strike: subject.strike,
            reference_price: match subject.comparator {
                PriceComparator::UpVsReference => Some(
                    input
                        .crypto_reference_prices
                        .get(&candidate.market_id)
                        .copied()
                        .ok_or_else(|| ReportError::InvariantViolation {
                            stage: "entry_condition",
                            detail: format!(
                                "relative Crypto market {} has no source-native PIT reference price",
                                candidate.market_id
                            ),
                        })?,
                ),
                _ => None,
            },
            recommended_outcome: candidate.outcome_side,
            max_input_age_ms,
        }),
        (
            MarketSubject::Weather(subject),
            MarketEventTemplate::WeatherDailyHighPredicate { max_input_age_ms },
        ) => match candidate.outcome_side {
                OutcomeSide::Yes => {
                    MarketEventCondition::WeatherDailyHighEnteredBand(WeatherDailyHighEnteredBand {
                        source,
                        station: subject.station.to_string(),
                        local_date: subject.local_date,
                        unit: subject.market_unit,
                        band: subject.outcome_band.clone(),
                        proxy_methodology_hash: subject.proxy_methodology_hash.clone(),
                        max_input_age_ms,
                    })
                }
                OutcomeSide::No => match subject.outcome_band.upper_inclusive {
                    Some(upper_inclusive) => {
                        MarketEventCondition::WeatherDailyHighExceededBandUpper(
                            WeatherDailyHighExceededBandUpper {
                                source,
                                station: subject.station.to_string(),
                                local_date: subject.local_date,
                                unit: subject.market_unit,
                                upper_inclusive,
                                proxy_methodology_hash: subject.proxy_methodology_hash.clone(),
                                max_input_age_ms,
                            },
                        )
                    }
                    None => MarketEventCondition::WeatherObservationDayClosedOutsideBand(
                        WeatherObservationDayClosedOutsideBand {
                            source,
                            station: subject.station.to_string(),
                            local_date: subject.local_date,
                            unit: subject.market_unit,
                            band: subject.outcome_band.clone(),
                            proxy_methodology_hash: subject.proxy_methodology_hash.clone(),
                        },
                    ),
                },
            },
        (_, _) => {
            return Err(ReportError::InvariantViolation {
                stage: "entry_condition",
                detail: format!(
                    "market-event template family does not match market {} linkage family",
                    candidate.market_id
                ),
            }
            .into());
        }
    };
    Ok(event)
}

fn condition_leaf_bindings(
    root: &EntryConditionV1,
) -> (
    Vec<EntryConditionFactorBinding>,
    Vec<EntryConditionSourceBinding>,
) {
    let mut factors = Vec::new();
    let mut sources = Vec::new();
    collect_condition_leaf_bindings(root, &mut factors, &mut sources);
    factors.sort_by(|left, right| {
        left.definition_id
            .to_string()
            .cmp(&right.definition_id.to_string())
    });
    factors.dedup();
    sources.sort();
    sources.dedup();
    (factors, sources)
}

fn collect_condition_leaf_bindings(
    node: &EntryConditionV1,
    factors: &mut Vec<EntryConditionFactorBinding>,
    sources: &mut Vec<EntryConditionSourceBinding>,
) {
    match node {
        EntryConditionV1::Factor(condition) => factors.push(EntryConditionFactorBinding {
            definition_id: condition.definition_id.clone(),
            definition_hash: condition.definition_hash.clone(),
        }),
        EntryConditionV1::MarketEvent { event: condition } => {
            let source = match condition {
                MarketEventCondition::CryptoSubjectPredicateEntered(event) => &event.source,
                MarketEventCondition::WeatherDailyHighEnteredBand(event) => &event.source,
                MarketEventCondition::WeatherDailyHighExceededBandUpper(event) => &event.source,
                MarketEventCondition::WeatherObservationDayClosedOutsideBand(event) => {
                    &event.source
                }
            };
            sources.push(source.clone());
        }
        EntryConditionV1::All { children } | EntryConditionV1::Any { children } => {
            for child in children {
                collect_condition_leaf_bindings(child, factors, sources);
            }
        }
        EntryConditionV1::Price(_) | EntryConditionV1::Clock(_) => {}
    }
}

fn policy_exit_plan(
    as_of: DateTime<Utc>,
    tick_size: TickSize,
    policy: &ResolvedRecommendationPolicy,
) -> QuantResult<ExitPlan> {
    let entry = policy.executable_entry_price.inner();
    let upper_factor =
        Decimal::ONE + policy.cohort.upper_barrier_bps.inner() / Decimal::from(10_000);
    let lower_factor =
        Decimal::ONE - policy.cohort.lower_barrier_bps.inner() / Decimal::from(10_000);
    let time_exit_at = instant_plus_secs(as_of, policy.cohort.vertical_barrier_secs)?;
    let scale_out_targets = policy
        .cohort
        .scale_out_targets
        .iter()
        .map(|target| ScaleOutTarget {
            target_id: target.target_id.clone(),
            trigger_price: bounded_price(
                entry * (Decimal::ONE + target.trigger_return_bps.inner() / Decimal::from(10_000)),
            ),
            target_cumulative_exit_pct: target.target_cumulative_exit_pct,
            min_price: None,
            valid_after: None,
            valid_until: Some(time_exit_at),
            reason: format!("trade policy cohort {}", policy.provenance.cohort_index),
        })
        .collect();
    let trailing_stop = policy
        .cohort
        .trailing_stop
        .as_ref()
        .map(|trailing| TrailingStopPolicy {
            trail_bps: trailing.trail_bps,
            activation_price: Some(bounded_price(
                entry
                    * (Decimal::ONE
                        + trailing.activation_return_bps.inner() / Decimal::from(10_000)),
            )),
        });
    let mut plan = ExitPlan {
        take_profit_price: Some(bounded_price(entry * upper_factor)),
        take_profit_pct: Some(upper_factor - Decimal::ONE),
        stop_loss_price: Some(bounded_price(entry * lower_factor)),
        stop_loss_pct: Some(Decimal::ONE - lower_factor),
        time_exit_at: Some(time_exit_at),
        max_hold_secs: Some(policy.cohort.vertical_barrier_secs),
        scale_out_targets,
        trailing_stop,
        thesis_invalidation: ThesisInvalidationPolicy {
            min_score_retention: policy.cohort.min_score_retention,
            min_expected_return_bps: policy.cohort.min_expected_return_bps,
            require_execution_eligibility: policy.cohort.require_execution_eligibility,
        },
        settlement_mode: policy.cohort.settlement_mode,
        redeem_policy: policy.cohort.redeem_policy,
        manual_review_at: None,
        exit_reason: format!(
            "published trade policy {} cohort {}",
            policy.provenance.artifact_id, policy.provenance.cohort_index
        ),
    };
    align_exit_plan_to_tick(&mut plan, tick_size);
    Ok(plan)
}

#[derive(Clone, Copy)]
enum TickDirection {
    Down,
    Up,
}

fn align_entry_plan_to_tick(plan: &mut EntryPlan, tick_size: TickSize) {
    match &mut plan.order_policy {
        EntryOrderPolicy::Passive { limit_price, .. } => {
            *limit_price = tick_aligned_price(limit_price.inner(), tick_size, TickDirection::Down);
        }
        EntryOrderPolicy::Aggressive { worst_price, .. } => {
            *worst_price = tick_aligned_price(worst_price.inner(), tick_size, TickDirection::Up);
        }
    }
}

fn align_exit_plan_to_tick(plan: &mut ExitPlan, tick_size: TickSize) {
    for price in [
        plan.take_profit_price.as_mut(),
        plan.stop_loss_price.as_mut(),
        plan.trailing_stop
            .as_mut()
            .and_then(|trailing| trailing.activation_price.as_mut()),
    ]
    .into_iter()
    .flatten()
    {
        *price = tick_aligned_price(price.inner(), tick_size, TickDirection::Up);
    }
    for target in &mut plan.scale_out_targets {
        target.trigger_price =
            tick_aligned_price(target.trigger_price.inner(), tick_size, TickDirection::Up);
        if let Some(min_price) = target.min_price.as_mut() {
            *min_price = tick_aligned_price(min_price.inner(), tick_size, TickDirection::Up);
        }
    }
}

fn tick_aligned_price(value: Decimal, tick_size: TickSize, direction: TickDirection) -> Price {
    let tick = tick_size.as_decimal();
    let min = tick;
    let max = Decimal::ONE - tick;
    let units = value.clamp(min, max) / tick;
    let rounded_units = match direction {
        TickDirection::Down => units.floor(),
        TickDirection::Up => units.ceil(),
    };
    Price::new((rounded_units * tick).clamp(min, max))
}

fn bounded_price(value: Decimal) -> Price {
    Price::new(value.clamp(Decimal::new(1, 4), Decimal::new(9_999, 4)))
}

fn calibrated_auto_execution_gate(
    candidate: &SignalCandidate,
    rank: u32,
    sizing: &SizingPlan,
    runtime_config: &RuntimeConfig,
    return_model_calibrated: bool,
) -> QuantResult<AutoExecutionGate> {
    let mut auto_gate = auto_execution_gate(candidate, rank, sizing, runtime_config)?;
    if !return_model_calibrated {
        auto_gate.allowed = false;
        if !auto_gate
            .reasons
            .contains(&IneligibilityReason::ReturnModelUncalibrated)
        {
            auto_gate
                .reasons
                .push(IneligibilityReason::ReturnModelUncalibrated);
        }
    }
    Ok(auto_gate)
}

struct NewRecommendationAssembly<'a> {
    report_id: &'a RecommendationReportId,
    planned: &'a PlannedRecommendation,
    candidate: &'a SignalCandidate,
    capture: &'a MarketDecisionCapture,
    input: &'a ComposeReportInput<'a>,
    compose_context: ComposeRecommendationContext,
    valid_until: DateTime<Utc>,
    data_quality_snapshot_ref: &'a ReportDataQualitySnapshotId,
    auto_gate: &'a AutoExecutionGate,
    risk_envelope: RiskEnvelope,
    policy: Result<ResolvedRecommendationPolicy, Vec<TradePlanBlocker>>,
}

struct ComposedRecommendationRows {
    recommendation: NewRecommendation,
    artifact: Option<NewEntryConditionArtifact>,
    instance: NewEntryConditionInstance,
}

fn build_new_recommendation(
    assembly: NewRecommendationAssembly<'_>,
) -> QuantResult<ComposedRecommendationRows> {
    let NewRecommendationAssembly {
        report_id,
        planned,
        candidate,
        capture,
        input,
        compose_context,
        valid_until,
        data_quality_snapshot_ref,
        auto_gate,
        risk_envelope,
        policy,
    } = assembly;
    let recommendation_id = RecommendationId::from_v7();
    let (trade_plan, artifact, trade_policy_available) =
        build_recommendation_trade_plan(RecommendationTradePlanInput {
            recommendation_id: &recommendation_id,
            input,
            candidate,
            capture,
            planned,
            valid_until,
            risk_envelope,
            policy,
        })?;
    let condition = condition_instance_projection(&trade_plan, input.published_at);
    let recommendation = NewRecommendation {
        recommendation_id: recommendation_id.clone(),
        recommendation_report_id: report_id.clone(),
        rank: i32::try_from(planned.rank).map_err(|error| ReportError::NumericOverflow {
            field: "recommendation.rank",
            detail: error.to_string(),
        })?,
        market_id: candidate.market_id.clone(),
        event_id: capture.event_id.clone(),
        token_id: candidate.token_id.clone(),
        outcome_side: candidate.outcome_side,
        composite_score: candidate.composite_score,
        risk_adjusted_score: planned.risk_adjusted_score,
        confidence: candidate.confidence,
        expected_return_bps: Bps::new(candidate.expected_return_bps),
        downside_bps: Bps::new(candidate.downside_bps),
        identity: capture.identity.clone(),
        market_context: capture.market_context.clone(),
        rank_before_portfolio: i32::try_from(candidate.rank_before_portfolio).map_err(|error| {
            ReportError::NumericOverflow {
                field: "recommendation.rank_before_portfolio",
                detail: error.to_string(),
            }
        })?,
        liquidity_score: candidate.liquidity_score,
        data_quality_score: candidate.data_quality_score,
        model_score_percentile: candidate.model_score_percentile,
        trade_plan,
        factor_breakdown: factor_breakdown(candidate),
        evidence_refs: EvidenceRefs::from_input(EvidenceRefsInput {
            signal_candidate_id: candidate.signal_candidate_id.clone(),
            feature_vector_id: compose_context.feature_vector_id,
            model_run_id: compose_context.model_run_id,
            market_selection_id: input.market_selection_id.clone(),
            book_snapshot_ref: capture.book_snapshot_ref.clone(),
            runtime_config_version_id: input.runtime_config_version_id.clone(),
            model_version_id: input.model_version_id.clone(),
            factor_definition_versions: factor_definition_versions(candidate),
            data_quality_snapshot_ref: data_quality_snapshot_ref.clone(),
        }),
        execution_eligibility: execution_eligibility(
            auto_gate,
            input.return_model_calibrated,
            trade_policy_available,
        ),
        valid_from: input.published_at,
        valid_until,
        status: RecommendationStatus::Published,
    };
    let instance = new_condition_instance(recommendation_id, condition, valid_until);
    Ok(ComposedRecommendationRows {
        recommendation,
        artifact,
        instance,
    })
}

struct RecommendationTradePlanInput<'a> {
    recommendation_id: &'a RecommendationId,
    input: &'a ComposeReportInput<'a>,
    candidate: &'a SignalCandidate,
    capture: &'a MarketDecisionCapture,
    planned: &'a PlannedRecommendation,
    valid_until: DateTime<Utc>,
    risk_envelope: RiskEnvelope,
    policy: Result<ResolvedRecommendationPolicy, Vec<TradePlanBlocker>>,
}

fn build_recommendation_trade_plan(
    plan_input: RecommendationTradePlanInput<'_>,
) -> QuantResult<(
    RecommendationTradePlan,
    Option<NewEntryConditionArtifact>,
    bool,
)> {
    let RecommendationTradePlanInput {
        recommendation_id,
        input,
        candidate,
        capture,
        planned,
        valid_until,
        risk_envelope,
        policy,
    } = plan_input;
    let trade_policy_available = policy.is_ok();
    let (trade_plan, artifact) = match policy {
        Ok(policy) => {
            let entry = policy_entry_plan(EntryPlanMaterialization {
                recommendation_id,
                published_at: input.published_at,
                valid_until,
                tick_size: capture.market_context.tick_size,
                candidate,
                capture,
                input,
                policy: &policy,
            })?;
            let exit =
                policy_exit_plan(input.decision_at, capture.market_context.tick_size, &policy)?;
            (
                RecommendationTradePlan::Frozen {
                    policy: Box::new(policy.provenance),
                    entry: entry.plan,
                    sizing: Box::new(planned.sizing.clone()),
                    exit: Box::new(exit),
                    risk_envelope: Box::new(risk_envelope),
                },
                entry.artifact,
            )
        }
        Err(blockers) => (RecommendationTradePlan::Unavailable { blockers }, None),
    };
    Ok((trade_plan, artifact, trade_policy_available))
}

struct ConditionInstanceProjection {
    state: EntryConditionState,
    truth: Option<ConditionTruth>,
    artifact_id: Option<EntryConditionArtifactId>,
    artifact_hash: Option<ContentHash>,
    next_evaluation_at: Option<DateTime<Utc>>,
}

fn condition_instance_projection(
    trade_plan: &RecommendationTradePlan,
    published_at: DateTime<Utc>,
) -> ConditionInstanceProjection {
    match trade_plan {
        RecommendationTradePlan::Frozen { entry, .. } => match &entry.condition {
            EntryConditionPlan::Immediate => ConditionInstanceProjection {
                state: EntryConditionState::NotRequired,
                truth: Some(ConditionTruth::Satisfied),
                artifact_id: None,
                artifact_hash: None,
                next_evaluation_at: None,
            },
            EntryConditionPlan::Conditional {
                artifact_id,
                content_hash,
            } => ConditionInstanceProjection {
                state: EntryConditionState::Waiting,
                truth: None,
                artifact_id: Some(artifact_id.clone()),
                artifact_hash: Some(content_hash.clone()),
                next_evaluation_at: Some(published_at),
            },
        },
        RecommendationTradePlan::Unavailable { .. } => ConditionInstanceProjection {
            state: EntryConditionState::Invalidated,
            truth: None,
            artifact_id: None,
            artifact_hash: None,
            next_evaluation_at: None,
        },
    }
}

fn new_condition_instance(
    recommendation_id: RecommendationId,
    condition: ConditionInstanceProjection,
    expires_at: DateTime<Utc>,
) -> NewEntryConditionInstance {
    NewEntryConditionInstance {
        condition_instance_id: EntryConditionInstanceId::from_v7(),
        recommendation_id,
        artifact_id: condition.artifact_id,
        artifact_hash: condition.artifact_hash,
        state: condition.state,
        truth_json: condition.truth,
        revision: 0,
        evaluation_hash: None,
        input_fingerprint: None,
        continuity_hash: None,
        confirmation_started_at: None,
        last_evaluated_at: None,
        next_evaluation_at: condition.next_evaluation_at,
        expires_at,
        lease_owner: None,
        lease_expires_at: None,
        lease_epoch: 0,
        claimed_by_intent_id: None,
        claim_admission_state_version: None,
        consumed_at: None,
    }
}

/// Build the recommendation risk envelope: clone the planned baseline, apply the
/// auto-execution gate, and fold in any candidate rejection warnings as notes.
fn compose_risk_envelope(
    planned: &PlannedRecommendation,
    candidate: &SignalCandidate,
    auto_gate: &AutoExecutionGate,
) -> RiskEnvelope {
    let mut risk_envelope = planned.risk_envelope.clone();
    risk_envelope.auto_execution_allowed = auto_gate.allowed;
    risk_envelope.requires_approval = !auto_gate.allowed;
    if !candidate.rejection_warnings.is_empty() {
        risk_envelope.risk_notes.extend(
            candidate
                .rejection_warnings
                .iter()
                .map(|warning| format!("{warning:?}")),
        );
    }
    risk_envelope
}

fn factor_breakdown(candidate: &SignalCandidate) -> RecommendationFactorBreakdown {
    RecommendationFactorBreakdown(
        candidate
            .factor_breakdown
            .iter()
            .map(|factor| FactorBreakdownEntry {
                factor_name: factor.name.to_string(),
                family: factor.family,
                value_state: factor.value_state,
                raw_value: factor.raw_value,
                normalized_score: factor.normalized_score,
                normalization_source: factor.normalization_source,
                indeterminate_reason: factor.indeterminate_reason,
                weight: factor.weight,
                contribution: factor.contribution,
                confidence: factor.confidence,
                direction: factor.direction,
                explanation: factor.explanation.clone(),
                source_refs: factor.source_refs.clone(),
            })
            .collect(),
    )
}

fn factor_definition_versions(candidate: &SignalCandidate) -> Vec<FactorDefinitionId> {
    let mut ids = Vec::new();
    for factor in &candidate.factor_breakdown {
        if !ids.contains(&factor.definition_id) {
            ids.push(factor.definition_id.clone());
        }
    }
    ids
}

/// Outcome of the auto-execution policy gate for one recommendation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct AutoExecutionGate {
    allowed: bool,
    reasons: Vec<IneligibilityReason>,
}

fn execution_eligibility(
    gate: &AutoExecutionGate,
    return_model_calibrated: bool,
    trade_policy_available: bool,
) -> ExecutionEligibility {
    let mut eligible_modes = vec![QuantRuntimeMode::ReportOnly];
    if return_model_calibrated && trade_policy_available {
        eligible_modes.push(QuantRuntimeMode::SemiAuto);
    }
    if gate.allowed && return_model_calibrated && trade_policy_available {
        eligible_modes.push(QuantRuntimeMode::AutoExecution);
    }
    let mut reasons = if gate.allowed {
        Vec::new()
    } else {
        gate.reasons.clone()
    };
    if !return_model_calibrated && !reasons.contains(&IneligibilityReason::ReturnModelUncalibrated)
    {
        reasons.push(IneligibilityReason::ReturnModelUncalibrated);
    }
    ExecutionEligibility {
        eligible_modes,
        uncalibrated_watermark: reasons.contains(&IneligibilityReason::ReturnModelUncalibrated),
        ineligibility_reasons: reasons,
        approval_required: !gate.allowed || !return_model_calibrated || !trade_policy_available,
        auto_policy_id: (gate.allowed && return_model_calibrated && trade_policy_available)
            .then(|| "runtime_config.execution.auto_execution".to_owned()),
    }
}

fn auto_execution_gate(
    candidate: &SignalCandidate,
    rank: u32,
    sizing: &SizingPlan,
    config: &RuntimeConfig,
) -> QuantResult<AutoExecutionGate> {
    let policy = &config.execution.auto_execution;
    if !policy.enabled {
        return Ok(AutoExecutionGate {
            allowed: false,
            reasons: Vec::new(),
        });
    }

    let min_score = parse_decimal(
        "execution.auto_execution.min_score",
        &policy.min_score.value,
    )?;
    let min_confidence = parse_decimal(
        "execution.auto_execution.min_confidence",
        &policy.min_confidence.value,
    )?;
    let max_total = parse_decimal(
        "execution.auto_execution.max_total_usd_per_report",
        &policy.max_total_usd_per_report.value,
    )?;

    let mut reasons = Vec::new();
    if candidate.composite_score.inner() < min_score {
        reasons.push(IneligibilityReason::LowConfidence);
    }
    if candidate.confidence.inner() < min_confidence
        && !reasons.contains(&IneligibilityReason::LowConfidence)
    {
        reasons.push(IneligibilityReason::LowConfidence);
    }

    let allowed = rank <= policy.max_orders_per_report
        && candidate.composite_score.inner() >= min_score
        && candidate.confidence.inner() >= min_confidence
        && sizing.suggested_usd.inner() <= max_total
        && reasons.is_empty();

    Ok(AutoExecutionGate { allowed, reasons })
}

/// The aggregate-exposure hard cap actually enforced by the LP for this
/// report (`capital_base_usd × portfolio.kelly_safety.max_aggregate_exposure_pct`),
/// frozen at compose time from the *exact* account snapshot + runtime-config
/// this report solved against.
///
/// `None` when the cap is disabled (`max_aggregate_exposure_pct <= 0`, the LP
/// applies no aggregate bucket) or the capital base is non-positive — the UI
/// must render "no cap", never a fabricated fallback value re-derived from a
/// separately-fetched, possibly-mismatched runtime-config version.
fn aggregate_exposure_cap_usd(input: &ComposeReportInput<'_>) -> Option<Usd> {
    let pct: Decimal = input
        .runtime_config
        .portfolio
        .kelly_safety
        .max_aggregate_exposure_pct
        .value
        .trim()
        .parse()
        .ok()?;
    compute_aggregate_exposure_cap_usd(input.account.capital_base_usd.inner(), pct)
}

/// Pure `capital_base_usd × max_aggregate_exposure_pct`, matching the LP's own
/// `lp.rs::build_buckets` gate exactly (`pct <= 0` or `capital_base <= 0`
/// disables the cap — `None`, never a fabricated `0`).
fn compute_aggregate_exposure_cap_usd(
    capital_base_usd: Decimal,
    max_aggregate_exposure_pct: Decimal,
) -> Option<Usd> {
    if max_aggregate_exposure_pct <= Decimal::ZERO || capital_base_usd <= Decimal::ZERO {
        return None;
    }
    Some(Usd::new(
        (max_aggregate_exposure_pct * capital_base_usd).round_dp(2),
    ))
}

fn report_summary(
    input: &ComposeReportInput<'_>,
    recommendations: &[NewRecommendation],
) -> QuantResult<ReportSummary> {
    let mut category_allocation: BTreeMap<MarketCategory, Usd> = BTreeMap::new();
    let mut event_allocation: BTreeMap<EventId, Usd> = BTreeMap::new();
    for rec in recommendations {
        let Some(sizing) = rec.trade_plan.sizing() else {
            continue;
        };
        *category_allocation
            .entry(rec.identity.category)
            .or_default() += sizing.suggested_usd;
        *event_allocation.entry(rec.event_id.clone()).or_default() += sizing.suggested_usd;
    }
    let total_suggested_usd = recommendations
        .iter()
        .filter_map(|rec| rec.trade_plan.sizing().map(|sizing| sizing.suggested_usd))
        .sum();
    let max_single_recommendation_usd = recommendations
        .iter()
        .filter_map(|rec| rec.trade_plan.sizing().map(|sizing| sizing.suggested_usd))
        .max()
        .unwrap_or(Usd::ZERO);
    let average_score = average_probability(
        recommendations
            .iter()
            .map(|rec| rec.composite_score.inner()),
    );
    let min_score = recommendations
        .iter()
        .map(|rec| rec.composite_score)
        .min()
        .unwrap_or_default();

    let empty_rejected_count = input.empty.as_ref().map_or(0, |empty| {
        if empty.reason == EmptyReportReason::InsufficientDataQuality {
            0
        } else {
            empty.rejected_count
        }
    });

    let planner_rejected_count = u32::try_from(input.planner_rejected.len()).map_err(|error| {
        ReportError::NumericOverflow {
            field: "report.summary.planner_rejected_count",
            detail: error.to_string(),
        }
    })?;
    let rejected_count = input
        .feature_rejected_count
        .checked_add(planner_rejected_count)
        .and_then(|count| count.checked_add(empty_rejected_count))
        .ok_or_else(|| ReportError::NumericOverflow {
            field: "report.summary.rejected_count",
            detail: "feature, planner, and empty rejection counts exceed u32".to_owned(),
        })?;
    let published_recommendation_count =
        u32::try_from(recommendations.len()).map_err(|error| ReportError::NumericOverflow {
            field: "report.summary.published_recommendation_count",
            detail: error.to_string(),
        })?;

    Ok(ReportSummary {
        market_selection_count: input.market_selection_count,
        candidate_count: input.candidate_count,
        rejected_count,
        published_recommendation_count,
        total_suggested_usd,
        max_single_recommendation_usd,
        aggregate_exposure_cap_usd: aggregate_exposure_cap_usd(input),
        category_allocation,
        event_allocation,
        average_score,
        min_score,
        model_confidence_summary: confidence_summary(recommendations),
        data_quality_summary: input.data_quality_snapshot.tokens_json.summary(),
        top_rejection_reasons: rejection_summary(input),
        execution_eligibility_summary: eligibility_summary(recommendations),
        empty_reason: input.empty.as_ref().map(|empty| empty.reason),
        warnings: input
            .empty
            .as_ref()
            .map_or_else(Vec::new, |empty| empty.warnings.clone()),
    })
}

fn confidence_summary(recommendations: &[NewRecommendation]) -> ConfidenceSummary {
    if recommendations.is_empty() {
        return ConfidenceSummary::default();
    }
    let mean_confidence =
        average_probability(recommendations.iter().map(|rec| rec.confidence.inner()));
    ConfidenceSummary {
        mean_confidence,
        min_confidence: recommendations
            .iter()
            .map(|rec| rec.confidence)
            .min()
            .unwrap_or_default(),
        max_confidence: recommendations
            .iter()
            .map(|rec| rec.confidence)
            .max()
            .unwrap_or_default(),
    }
}

fn rejection_summary(input: &ComposeReportInput<'_>) -> Vec<RejectionReasonCount> {
    let mut counts: BTreeMap<String, u32> = BTreeMap::new();
    if let Some(empty) = &input.empty
        && empty.reason != EmptyReportReason::InsufficientDataQuality
        && empty.rejected_count > 0
    {
        counts.insert(empty.reason.as_str().to_owned(), empty.rejected_count);
    }
    if input.feature_rejected_count > 0 {
        *counts
            .entry(
                EmptyReportReason::InsufficientDataQuality
                    .as_str()
                    .to_owned(),
            )
            .or_default() += input.feature_rejected_count;
    }
    for rejected in input.planner_rejected {
        *counts
            .entry(rejected.reason.as_str().to_owned())
            .or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(reason, count)| RejectionReasonCount { reason, count })
        .collect()
}

fn eligibility_summary(recommendations: &[NewRecommendation]) -> EligibilitySummary {
    let mut summary = EligibilitySummary::default();
    for rec in recommendations {
        if rec
            .execution_eligibility
            .is_eligible(QuantRuntimeMode::ReportOnly)
        {
            summary.eligible_report_only += 1;
        }
        if rec
            .execution_eligibility
            .is_eligible(QuantRuntimeMode::SemiAuto)
        {
            summary.eligible_semi_auto += 1;
        }
        if rec
            .execution_eligibility
            .is_eligible(QuantRuntimeMode::AutoExecution)
        {
            summary.eligible_auto_execution += 1;
        }
    }
    summary
}

fn recommendation_events(
    report_id: &RecommendationReportId,
    recommendations: &[NewRecommendation],
    event_time: DateTime<Utc>,
) -> QuantResult<Vec<QuantRecommendationEventRow>> {
    recommendations
        .iter()
        .map(|rec| {
            let rank = u32::try_from(rec.rank).map_err(|error| ReportError::NumericOverflow {
                field: "recommendation.rank",
                detail: error.to_string(),
            })?;
            Ok(QuantRecommendationEventRow {
                event_time: event_time.timestamp_millis(),
                recommendation_report_id: report_id.clone(),
                recommendation_id: rec.recommendation_id.clone(),
                rank,
                market_id: rec.market_id.clone(),
                token_id: rec.token_id.clone(),
                side: rec.outcome_side.into(),
                score: ChProbability::from(rec.composite_score),
                risk_adjusted_score: ChProbability::from(rec.risk_adjusted_score),
                trade_plan_available: rec.trade_plan.is_available(),
                suggested_usd: rec
                    .trade_plan
                    .sizing()
                    .map(|sizing| ChUsd::from(sizing.suggested_usd)),
                valid_until: rec.valid_until.timestamp_millis(),
                status: rec.status.into(),
            })
        })
        .collect()
}

fn operation_log(
    report_id: &RecommendationReportId,
    input: &ComposeReportInput<'_>,
    status: RecommendationReportStatus,
    report: &NewRecommendationReport,
) -> QuantResult<NewOperationLog> {
    Ok(NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: input.trigger_key.clone(),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("report_lifecycle".to_owned()),
        category: OperationCategory::QuantReport,
        action: "publish".to_owned(),
        resource_type: Some(ResourceType::QuantReport),
        resource_id: Some(report_id.to_string()),
        http_method: "SYSTEM".to_owned(),
        http_path: "/system/quant/report".to_owned(),
        http_status: 201,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: serde_json::json!({
            "trigger_key": input.trigger_key,
            "trigger_kind": input.trigger.kind().as_str(),
            "status": status.as_str(),
            "decision_at": input.decision_at.to_rfc3339(),
            "published_at": input.published_at.to_rfc3339(),
            "candidate_count": input.candidate_count,
            "published_count": input.planned.len(),
            "empty_reason": input.empty.as_ref().map(|empty| empty.reason.as_str()),
        }),
        before_hash: None,
        after_hash: Some(canonical_state_hash(report).map_err(|error| {
            QuantError::config(format!("canonical state hash failed: {error}"))
        })?),
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    })
}

fn empty_portfolio_plan(
    portfolio_plan_id: PortfolioPlanId,
    model_run_id: Option<ModelRunId>,
    market_selection_id: MarketSelectionId,
    as_of: DateTime<Utc>,
    account: &AccountSnapshot,
    config: &RuntimeConfig,
    rejected_summary: PortfolioRejectedSummary,
) -> QuantResult<NewPortfolioPlan> {
    let total_budget = Usd::new(parse_decimal(
        "portfolio.budget.total_budget_usd",
        &config.portfolio.budget.total_budget_usd.value,
    )?);
    let constraints = PortfolioConstraintsSnapshot {
        max_market_exposure_usd: Usd::new(parse_decimal(
            "portfolio.constraints.max_market_exposure_usd",
            &config.portfolio.constraints.max_market_exposure_usd.value,
        )?),
        max_event_exposure_usd: Usd::new(parse_decimal(
            "portfolio.constraints.max_event_exposure_usd",
            &config.portfolio.constraints.max_event_exposure_usd.value,
        )?),
        max_category_exposure_usd: Usd::new(parse_decimal(
            "portfolio.constraints.max_category_exposure_usd",
            &config.portfolio.constraints.max_category_exposure_usd.value,
        )?),
        max_correlated_exposure_usd: Usd::new(parse_decimal(
            "portfolio.constraints.max_correlated_exposure_usd",
            &config
                .portfolio
                .constraints
                .max_correlated_exposure_usd
                .value,
        )?),
        max_single_recommendation_usd: Usd::new(parse_decimal(
            "portfolio.budget.max_single_recommendation_usd",
            &config.portfolio.budget.max_single_recommendation_usd.value,
        )?),
        min_recommendation_usd: Usd::new(parse_decimal(
            "portfolio.budget.min_recommendation_usd",
            &config.portfolio.budget.min_recommendation_usd.value,
        )?),
        liquidity_usage_cap_pct: parse_decimal(
            "portfolio.constraints.liquidity_usage_cap_pct",
            &config.portfolio.constraints.liquidity_usage_cap_pct.value,
        )?,
    };
    Ok(NewPortfolioPlan {
        portfolio_plan_id,
        model_run_id,
        market_selection_id,
        decision_at: as_of,
        budget_usd: total_budget,
        allocated_usd: Usd::ZERO,
        risk_budget_json: PortfolioRiskBudget {
            total_budget_usd: total_budget,
            capital_base_usd: account.capital_base_usd,
            reserved_usd: account.reserved_usd,
            allocated_usd: Usd::ZERO,
            remaining_usd: total_budget,
        },
        constraints_json: constraints,
        rejected_summary,
        optimizer_meta_json: PortfolioOptimizerMeta::default(),
    })
}

pub(super) fn empty_plan_for_report(
    model_run_id: Option<ModelRunId>,
    market_selection_id: MarketSelectionId,
    as_of: DateTime<Utc>,
    account: &AccountSnapshot,
    config: &RuntimeConfig,
    reason: EmptyReportReason,
    rejected_count: u32,
) -> QuantResult<NewPortfolioPlan> {
    empty_portfolio_plan(
        PortfolioPlanId::from_v7(),
        model_run_id,
        market_selection_id,
        as_of,
        account,
        config,
        PortfolioRejectedSummary {
            rejected_count,
            reasons: vec![RejectionReasonCount {
                reason: reason.as_str().to_owned(),
                count: rejected_count,
            }],
        },
    )
}

fn average_probability(values: impl IntoIterator<Item = Decimal>) -> Probability {
    let (sum, count) = values
        .into_iter()
        .fold((Decimal::ZERO, 0_u64), |(sum, count), value| {
            (sum + value, count + 1)
        });
    if count == 0 {
        return Probability::default();
    }
    Probability::new(sum / Decimal::from(count))
}

fn parse_decimal(field: &str, value: &str) -> QuantResult<Decimal> {
    value
        .trim()
        .parse::<Decimal>()
        .map_err(|error| QuantError::config(format!("{field} is not a valid decimal: {error}")))
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_error::{QuantError, report::ReportError};
    use quant_pivot_models::{
        domain::quant::{NewAccountSnapshot, NewEquitySnapshot, NewReportDataQualitySnapshot},
        enums::{
            common::TickSize,
            quant::{
                AccountSource, BindingConstraint, EmptyReportReason, IneligibilityReason,
                OutcomeSide, QuantRuntimeMode, SizingModelKind,
            },
        },
        runtime_config::RuntimeConfig,
        types::{
            AccountPositions, AccountSnapshotId, Bps, ContentHash, EquitySnapshotId, MarketId,
            MarketSelectionId, ModelRunId, ModelVersionId, Price, Probability,
            ReportDataQualitySnapshotId, ReportDataQualityTokens, RiskEnvelope,
            RuntimeConfigVersionId, Shares, SignalCandidateId, SizingPlan, TokenId, Usd,
        },
    };
    use quant_pivot_research::{
        model::{ModelExplanation, SignalCandidate},
        portfolio::{AccountSnapshot, PlannedRecommendation},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    use super::{
        ComposeReportInput, DefaultRecommendationComposer, RecommendationComposer, TickDirection,
        actionable_valid_until, auto_execution_gate, effective_horizon_from, empty_plan_for_report,
        entry_window_secs, execution_eligibility, tick_aligned_price,
    };
    use crate::report::{composer::compute_aggregate_exposure_cap_usd, types::ReportTrigger};

    #[test]
    fn effective_horizon_uses_model_horizon_capped_by_resolution() {
        // Model horizon below resolution: use the model horizon.
        assert_eq!(effective_horizon_from(3_600, Some(86_400), 7_200), 3_600);
        // Resolution closer than the model horizon: cap at resolution.
        assert_eq!(effective_horizon_from(86_400, Some(1_800), 7_200), 1_800);
        // No resolution wall: use the (uncapped) model horizon.
        assert_eq!(effective_horizon_from(3_600, None, 7_200), 3_600);
    }

    #[test]
    fn tick_rounding_is_directional_and_bounded() {
        assert_eq!(
            tick_aligned_price(dec!(0.603), TickSize::Hundredth, TickDirection::Down),
            Price::new(dec!(0.60))
        );
        assert_eq!(
            tick_aligned_price(dec!(0.603), TickSize::Hundredth, TickDirection::Up),
            Price::new(dec!(0.61))
        );
        assert_eq!(
            tick_aligned_price(Decimal::ZERO, TickSize::Hundredth, TickDirection::Down),
            Price::new(dec!(0.01))
        );
        assert_eq!(
            tick_aligned_price(Decimal::ONE, TickSize::Hundredth, TickDirection::Up),
            Price::new(dec!(0.99))
        );
    }

    #[test]
    fn effective_horizon_falls_back_when_model_horizon_absent() {
        // suggested == 0 (classical run) -> governed fallback, still capped.
        assert_eq!(effective_horizon_from(0, Some(86_400), 7_200), 7_200);
        assert_eq!(effective_horizon_from(0, Some(900), 7_200), 900);
        // A market at/after resolution has no actionable model horizon.
        assert_eq!(effective_horizon_from(0, Some(0), 7_200), 0);
    }

    #[test]
    fn entry_window_is_the_ratio_slice_of_the_horizon() {
        // Default 0.5 ratio -> enter within the first half (the half-life point).
        assert_eq!(entry_window_secs(3_600, dec!(0.5)).expect("window"), 1_800);
        // Full ratio -> entry valid across the whole horizon.
        assert_eq!(entry_window_secs(3_600, dec!(1.0)).expect("window"), 3_600);
        // Rounds and floors at 1s.
        assert_eq!(entry_window_secs(1, dec!(0.5)).expect("window"), 1);
        assert!(entry_window_secs(0, dec!(0.5)).is_err());
    }

    #[test]
    fn empty_plan_rejects_invalid_budget_instead_of_substituting_zero() {
        let as_of = Utc
            .with_ymd_and_hms(2026, 7, 10, 12, 0, 0)
            .single()
            .expect("valid time");
        let account = AccountSnapshot::new(
            as_of,
            AccountSource::Polymarket,
            Usd::new(dec!(10_000)),
            Usd::new(dec!(10_000)),
            Usd::new(dec!(10_000)),
            Usd::ZERO,
            Vec::new(),
        );
        let mut config = RuntimeConfig::default();
        config.portfolio.budget.total_budget_usd.value = "not-a-decimal".to_owned();

        assert!(
            empty_plan_for_report(
                None,
                MarketSelectionId::from_v7(),
                as_of,
                &account,
                &config,
                EmptyReportReason::NoPositiveSignal,
                0,
            )
            .is_err()
        );
    }

    #[test]
    fn validity_is_anchored_to_decision_and_opens_at_publication() {
        let decision_at = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
        let published_at = decision_at + Duration::seconds(30);
        let market_id = MarketId::new("0xmarket");

        let valid_until = actionable_valid_until(decision_at, published_at, 1_800, &market_id)
            .expect("actionable window");

        assert_eq!(valid_until, decision_at + Duration::seconds(1_800));
        assert_ne!(valid_until, published_at + Duration::seconds(1_800));
    }

    #[test]
    fn publication_after_prediction_window_fails_closed() {
        let decision_at = Utc.with_ymd_and_hms(2026, 7, 10, 12, 0, 0).unwrap();
        let market_id = MarketId::new("0xmarket");

        assert!(
            actionable_valid_until(
                decision_at,
                decision_at + Duration::seconds(1_800),
                1_800,
                &market_id,
            )
            .is_err()
        );
    }

    #[test]
    fn aggregate_exposure_cap_usd_matches_lp_gate() {
        assert_eq!(
            compute_aggregate_exposure_cap_usd(dec!(10_000), dec!(0.25)),
            Some(Usd::new(dec!(2500))),
            "capital_base_usd × pct, rounded to cent precision"
        );
    }

    #[test]
    fn aggregate_exposure_cap_usd_none_when_pct_disabled() {
        assert_eq!(
            compute_aggregate_exposure_cap_usd(dec!(10_000), dec!(0)),
            None,
            "pct <= 0 must report no cap, never a fabricated 0 cap"
        );
        assert_eq!(
            compute_aggregate_exposure_cap_usd(dec!(10_000), dec!(-1)),
            None
        );
    }

    #[test]
    fn aggregate_exposure_cap_usd_none_when_capital_base_non_positive() {
        assert_eq!(
            compute_aggregate_exposure_cap_usd(dec!(0), dec!(0.25)),
            None,
            "a non-positive capital base must never report a fabricated cap"
        );
        assert_eq!(
            compute_aggregate_exposure_cap_usd(dec!(-500), dec!(0.25)),
            None
        );
    }

    fn candidate() -> SignalCandidate {
        SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: ModelRunId::from_v7(),
            market_id: MarketId::new("0xmarket"),
            token_id: TokenId::new("token-1"),
            outcome_side: OutcomeSide::Yes,
            composite_score: Probability::new(dec!(0.75)),
            confidence: Probability::new(dec!(0.80)),
            expected_return_bps: dec!(5000),
            downside_bps: dec!(1000),
            win_probability: None,
            entry_price_ref: Price::new(dec!(0.50)),
            suggested_horizon_secs: 3_600,
            factor_breakdown: Vec::new(),
            model_explanation: ModelExplanation {
                headline: "headline".to_owned(),
                top_positive: Vec::new(),
                top_negative: Vec::new(),
            },
            rejection_warnings: Vec::new(),
            rank_before_portfolio: 1,
            liquidity_score: Probability::ZERO,
            data_quality_score: Probability::ZERO,
            model_score_percentile: Probability::ZERO,
            decision_at: Utc
                .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
                .single()
                .expect("valid time"),
        }
    }

    fn sizing_plan(suggested_usd: Usd) -> SizingPlan {
        SizingPlan {
            suggested_usd,
            suggested_shares: Shares::new(dec!(100)),
            max_usd: Usd::new(dec!(500)),
            min_usd: Usd::new(dec!(10)),
            portfolio_weight_pct: dec!(0.05),
            market_exposure_after_usd: suggested_usd,
            event_exposure_after_usd: suggested_usd,
            category_exposure_after_usd: suggested_usd,
            binding_constraint: BindingConstraint::KellyCap,
            sizing_reason: "test".to_owned(),
            sizing_model: SizingModelKind::Kelly,
            edge_bps: Some(Bps::new(dec!(120))),
            kelly_fraction_applied: Some(dec!(0.5)),
            edge_uncertainty_shrink_applied: None,
            correlation_shrink_applied: None,
            f_star_applied: None,
            kelly_fraction_config_applied: None,
            confidence_shrink_applied: None,
            drawdown_shrink_applied: None,
            raw_fraction_applied: None,
            position_cap_fraction_applied: None,
        }
    }

    #[test]
    fn execution_eligibility_keeps_semi_auto_when_auto_denied_for_low_confidence() {
        let mut config = RuntimeConfig::default();
        config.execution.auto_execution.enabled = true;
        config.execution.auto_execution.max_orders_per_report = 5;
        config.execution.auto_execution.min_score.value = "0.90".to_owned();
        config.execution.auto_execution.min_confidence.value = "0.90".to_owned();

        let gate = auto_execution_gate(&candidate(), 1, &sizing_plan(Usd::new(dec!(100))), &config)
            .expect("gate");
        assert!(!gate.allowed);
        assert_eq!(gate.reasons, vec![IneligibilityReason::LowConfidence]);

        let eligibility = execution_eligibility(&gate, true, true);
        assert!(eligibility.is_eligible(QuantRuntimeMode::ReportOnly));
        assert!(eligibility.is_eligible(QuantRuntimeMode::SemiAuto));
        assert!(!eligibility.is_eligible(QuantRuntimeMode::AutoExecution));
        assert!(
            !eligibility
                .ineligibility_reasons
                .contains(&IneligibilityReason::BudgetExhausted)
        );
    }

    #[test]
    fn execution_eligibility_allows_auto_when_policy_passes() {
        let mut config = RuntimeConfig::default();
        config.execution.auto_execution.enabled = true;
        config.execution.auto_execution.max_orders_per_report = 5;
        config.execution.auto_execution.min_score.value = "0.50".to_owned();
        config.execution.auto_execution.min_confidence.value = "0.50".to_owned();
        config
            .execution
            .auto_execution
            .max_total_usd_per_report
            .value = "1000".to_owned();

        let gate = auto_execution_gate(&candidate(), 1, &sizing_plan(Usd::new(dec!(100))), &config)
            .expect("gate");
        assert!(gate.allowed);
        assert!(gate.reasons.is_empty());

        let eligibility = execution_eligibility(&gate, true, true);
        assert!(eligibility.is_eligible(QuantRuntimeMode::AutoExecution));
    }

    fn planned_recommendation(candidate: SignalCandidate) -> PlannedRecommendation {
        PlannedRecommendation {
            candidate,
            sizing: SizingPlan {
                suggested_usd: Usd::new(dec!(100)),
                suggested_shares: Shares::new(dec!(100)),
                max_usd: Usd::new(dec!(500)),
                min_usd: Usd::new(dec!(10)),
                portfolio_weight_pct: dec!(0.05),
                market_exposure_after_usd: Usd::new(dec!(100)),
                event_exposure_after_usd: Usd::new(dec!(100)),
                category_exposure_after_usd: Usd::new(dec!(100)),
                binding_constraint: BindingConstraint::KellyCap,
                sizing_reason: "test".to_owned(),
                sizing_model: SizingModelKind::Kelly,
                edge_bps: Some(Bps::new(dec!(120))),
                kelly_fraction_applied: Some(dec!(0.5)),
                edge_uncertainty_shrink_applied: None,
                correlation_shrink_applied: None,
                f_star_applied: None,
                kelly_fraction_config_applied: None,
                confidence_shrink_applied: None,
                drawdown_shrink_applied: None,
                raw_fraction_applied: None,
                position_cap_fraction_applied: None,
            },
            risk_envelope: RiskEnvelope {
                max_loss_usd: Usd::new(dec!(120)),
                max_slippage_bps: Bps::new(dec!(50)),
                max_position_usd: Usd::new(dec!(500)),
                max_market_exposure_usd: Usd::new(dec!(500)),
                max_event_exposure_usd: Usd::new(dec!(750)),
                max_category_exposure_usd: Usd::new(dec!(1500)),
                requires_approval: true,
                auto_execution_allowed: false,
                risk_notes: Vec::new(),
                envelope_hash: ContentHash::parse(format!("blake3:{}", "0".repeat(64)))
                    .expect("hash"),
            },
            risk_adjusted_score: Probability::new(dec!(0.7)),
            rank: 1,
        }
    }

    #[test]
    fn compose_rejects_missing_decision_capture() {
        let as_of = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let trigger_time = as_of + Duration::seconds(120);
        let config = RuntimeConfig::default();
        let account = AccountSnapshot::new(
            as_of,
            AccountSource::Polymarket,
            Usd::new(dec!(10_000)),
            Usd::new(dec!(10_000)),
            Usd::new(dec!(10_000)),
            Usd::ZERO,
            Vec::new(),
        );
        let market_selection_id = MarketSelectionId::from_v7();
        let model_run_id = ModelRunId::from_v7();
        let planned = vec![planned_recommendation(candidate())];
        let portfolio_plan = empty_plan_for_report(
            Some(model_run_id.clone()),
            market_selection_id.clone(),
            as_of,
            &account,
            &config,
            EmptyReportReason::NoPositiveSignal,
            0,
        )
        .expect("valid empty portfolio plan");
        let data_quality_snapshot = NewReportDataQualitySnapshot {
            report_data_quality_snapshot_id: ReportDataQualitySnapshotId::from_v7(),
            decision_at: as_of,
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            tokens_json: ReportDataQualityTokens(Vec::new()),
        };
        let account_snapshot_id = AccountSnapshotId::from_v7();
        let account_snapshot = NewAccountSnapshot {
            account_snapshot_id: account_snapshot_id.clone(),
            as_of: account.as_of,
            source: account.source,
            venue_net_liquidation_usd: account.venue_net_liquidation_usd,
            capital_base_usd: account.capital_base_usd,
            available_usd: account.available_usd,
            reserved_usd: account.reserved_usd,
            positions_json: AccountPositions(account.positions.clone()),
            exposures_json: account.exposures.clone(),
        };
        let equity_snapshot = NewEquitySnapshot {
            equity_snapshot_id: EquitySnapshotId::from_v7(),
            as_of: account.as_of,
            source: account.source,
            venue_net_liquidation_usd: account.venue_net_liquidation_usd,
            capital_base_usd: account.capital_base_usd,
            available_usd: account.available_usd,
            reserved_usd: account.reserved_usd,
            realized_pnl_cumulative_usd: Usd::ZERO,
            unrealized_pnl_usd: Usd::ZERO,
            high_water_mark_usd: account.capital_base_usd,
            drawdown_pct: Decimal::ZERO,
            account_snapshot_ref: Some(account_snapshot_id),
        };

        let result = DefaultRecommendationComposer.compose(ComposeReportInput {
            trigger: &ReportTrigger::AdHoc {
                request_id: "compose-test".to_owned(),
            },
            trigger_key: "ad_hoc:compose-test".to_owned(),
            trigger_time,
            knowledge_lag_secs: 0,
            decision_at: as_of,
            published_at: trigger_time,
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            runtime_config: &config,
            runtime_mode: QuantRuntimeMode::ReportOnly,
            model_version_id: ModelVersionId::from_v7(),
            market_selection_id,
            market_selection_hash: ContentHash::parse(format!("blake3:{}", "a".repeat(64)))
                .expect("selection hash"),
            account: &account,
            account_snapshot,
            equity_snapshot,
            portfolio_plan,
            planned: &planned,
            planner_rejected: &[],
            captures: HashMap::new(),
            feature_vector_by_market: HashMap::new(),
            data_quality_snapshot,
            model_run_id: Some(model_run_id),
            candidate_count: 1,
            feature_rejected_count: 0,
            market_selection_count: 1,
            empty: None,
            top_n: 5,
            return_model_calibrated: true,
            trade_policy: None,
            crypto_reference_prices: HashMap::new(),
        });

        assert!(matches!(
            result,
            Err(QuantError::Report(ReportError::InvariantViolation {
                stage: "compose",
                ..
            }))
        ));
    }
}
