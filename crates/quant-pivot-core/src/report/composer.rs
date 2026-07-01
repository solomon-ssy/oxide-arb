//! Report payload composition.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, report::ReportError};
use quant_pivot_models::{
    clickhouse::{ChProbability, ChUsd, QuantRecommendationEventRow},
    domain::{
        NewAccountSnapshot, NewEquitySnapshot, NewOperationLog, NewPortfolioPlan,
        NewRecommendation, NewRecommendationReport, NewReportDataQualitySnapshot,
        NewReportTransaction,
    },
    enums::{
        common::MarketCategory,
        operation_log::{OperationCategory, OperationOutcome},
        quant::{
            EmptyReason, ExitSettlementMode, IneligibilityReason, QuantRuntimeMode,
            RecommendationReportStatus, RecommendationStatus, RedeemPolicy, ReportKind,
        },
        rbac::ResourceType,
    },
    hashing::canonical_state_hash,
    runtime_config::RuntimeConfig,
    types::{
        Bps, ConfidenceSummary, EligibilitySummary, EventId, EvidenceRefs, EvidenceRefsInput,
        ExecutionEligibility, ExitPlan, FactorBreakdownEntry, FactorDefinitionId, FeatureVectorId,
        MarketId, MarketSelectionId, ModelRunId, ModelVersionId, OperationLogId,
        PortfolioConstraintsSnapshot, PortfolioOptimizerMeta, PortfolioPlanId,
        PortfolioRejectedSummary, PortfolioRiskBudget, Price, Probability,
        RecommendationFactorBreakdown, RecommendationId, RecommendationReportId,
        RejectionReasonCount, ReportDataQualitySnapshotId, ReportSummary, RiskEnvelope,
        RuntimeConfigVersionId, SizingPlan, Usd,
    },
};
use quant_pivot_research::{
    features::MarketDecisionCapture,
    model::SignalCandidate,
    portfolio::{AccountSnapshot, PlannedRecommendation, RejectedCandidate},
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use super::{
    entry_plan::derive_entry_plan,
    types::{
        ComposedReport, EmptyReportContext, NotificationRecommendation, ReportNotificationPayload,
        ReportTrigger,
    },
};

/// Inputs required to compose one report artifact.
pub struct ComposeReportInput<'a> {
    pub trigger: &'a ReportTrigger,
    pub trigger_key: String,
    pub trigger_time: DateTime<Utc>,
    pub source_delay_secs: u64,
    pub as_of: DateTime<Utc>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub runtime_config: &'a RuntimeConfig,
    pub runtime_mode: QuantRuntimeMode,
    pub model_version_id: ModelVersionId,
    pub market_selection_id: MarketSelectionId,
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

        let recommendations = input
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

        // Report validity is the data-driven roll-up of its recommendations'
        // validity (latest entry-by); an empty report falls back to the governed
        // horizon so it still ages out.
        let report_valid_until = match recommendations.iter().map(|rec| rec.valid_until).max() {
            Some(max) => max,
            None => as_of_plus_secs(input.as_of, fallback_horizon_secs)?,
        };

        let status = if recommendations.is_empty() {
            RecommendationReportStatus::PublishedEmpty
        } else {
            RecommendationReportStatus::Published
        };
        let summary = report_summary(&input, &recommendations);
        // Build the notification before `summary` / `recommendations` move below.
        let notification =
            report_notification(&report_id, status, &input, &summary, &recommendations);

        let account_snapshot_id = input.account_snapshot.account_snapshot_id.clone();
        let equity_snapshot_id = input.equity_snapshot.equity_snapshot_id.clone();
        let report = NewRecommendationReport {
            recommendation_report_id: report_id.clone(),
            report_kind: ReportKind::TopN,
            trigger_kind: input.trigger.kind(),
            trigger_key: input.trigger_key.clone(),
            trigger_time: input.trigger_time,
            source_delay_secs: i64::try_from(input.source_delay_secs).map_err(|error| {
                QuantError::config(format!("source_delay_secs too large: {error}"))
            })?,
            as_of: input.as_of,
            // Informational nominal horizon: the governed fallback (per-rec
            // horizons are data-driven and live on each recommendation).
            horizon_secs: i64::try_from(fallback_horizon_secs).map_err(|error| {
                QuantError::config(format!("reports.fallback_horizon_secs too large: {error}"))
            })?,
            runtime_mode: input.runtime_mode,
            runtime_config_version_id: input.runtime_config_version_id.clone(),
            model_version_id: input.model_version_id.clone(),
            market_selection_id: input.market_selection_id.clone(),
            portfolio_plan_id: input.portfolio_plan.portfolio_plan_id.clone(),
            top_n: i32::try_from(input.top_n).map_err(|error| {
                QuantError::config(format!("reports top_n exceeds i32::MAX: {error}"))
            })?,
            status,
            account_source: input.account.source,
            capital_base_usd: input.account.capital_base_usd,
            account_snapshot_ref: account_snapshot_id,
            equity_snapshot_ref: equity_snapshot_id,
            data_quality_snapshot_ref,
            summary_json: summary,
            published_at: Some(input.trigger_time),
            // Roll-up of recommendation validity (data-driven), frozen at publish.
            valid_until: Some(report_valid_until),
            revoked_at: None,
            expired_at: None,
            status_reason: input
                .empty
                .as_ref()
                .map(|empty| empty.reason.as_str().to_owned()),
        };
        let operation_log = operation_log(&report_id, &input, status, &report)?;
        let ch_rows = recommendation_events(&report_id, &recommendations, input.trigger_time);

        Ok(ComposedReport {
            transaction: NewReportTransaction {
                account_snapshot: input.account_snapshot,
                equity_snapshot: input.equity_snapshot,
                data_quality_snapshot: input.data_quality_snapshot,
                portfolio_plan: input.portfolio_plan,
                report,
                recommendations,
                operation_log,
            },
            ch_rows,
            notification,
            delivery_policy: input.runtime_config.reports.delivery_policy,
            notify_operators: input.runtime_config.notification.policies.report_published,
        })
    }
}

/// `as_of + secs`, failing closed on overflow.
fn as_of_plus_secs(as_of: DateTime<Utc>, secs: u64) -> QuantResult<DateTime<Utc>> {
    let secs = i64::try_from(secs)
        .map_err(|error| QuantError::config(format!("horizon seconds too large: {error}")))?;
    Ok(as_of + Duration::seconds(secs))
}

/// Per-recommendation effective horizon (seconds): the model's frozen prediction
/// horizon (`suggested_horizon_secs`), falling back to the governed default when
/// the model supplies none (classical / non-ML runs), capped by the market's
/// time-to-resolution hard wall. Never zero.
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
/// fallback when absent) capped by the market's time-to-resolution, floored at 1s.
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
    let capped = match time_to_resolution_secs {
        Some(ttr) if ttr < base => ttr,
        _ => base,
    };
    if capped == 0 { 1 } else { capped }
}

/// Entry-window length (seconds) = `effective_horizon * entry_window_ratio`,
/// rounded, floored at 1s — the recommendation accepts new entries only while at
/// least `ratio` of the signal's edge horizon remains (the half-life point at
/// `ratio = 0.5`). The exit / time-stop still uses the full effective horizon.
fn entry_window_secs(effective_horizon_secs: u64, entry_window_ratio: Decimal) -> u64 {
    let secs = (Decimal::from(effective_horizon_secs) * entry_window_ratio).round();
    secs.to_u64().unwrap_or(effective_horizon_secs).max(1)
}

/// Build the operator-notification payload from the composed report parts.
fn report_notification(
    report_id: &RecommendationReportId,
    status: RecommendationReportStatus,
    input: &ComposeReportInput<'_>,
    summary: &ReportSummary,
    recommendations: &[NewRecommendation],
) -> ReportNotificationPayload {
    ReportNotificationPayload {
        report_id: report_id.clone(),
        kind: ReportKind::TopN,
        status: status.as_str().to_owned(),
        runtime_mode: input.runtime_mode,
        published_count: u32::try_from(recommendations.len()).unwrap_or(u32::MAX),
        total_suggested_usd: summary.total_suggested_usd,
        top3: recommendations
            .iter()
            .take(3)
            .map(|rec| NotificationRecommendation {
                market_id: rec.market_id.to_string(),
                outcome_side: rec.outcome_side,
                score: rec.composite_score,
                suggested_usd: rec.sizing_plan.suggested_usd,
            })
            .collect(),
        warnings: summary.warnings.clone(),
        empty_reason: input.empty.as_ref().map(|empty| empty.reason),
    }
}

fn compose_recommendation(
    report_id: &RecommendationReportId,
    planned: &PlannedRecommendation,
    input: &ComposeReportInput<'_>,
    entry_window_ratio: Decimal,
    fallback_horizon_secs: u64,
    data_quality_snapshot_ref: &ReportDataQualitySnapshotId,
) -> QuantResult<NewRecommendation> {
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

    // Per-recommendation, data-driven validity: the model's effective horizon
    // (capped by time-to-resolution) drives the exit/time-stop; the entry-by
    // deadline is the early `entry_window_ratio` slice of it (enter while fresh).
    let horizon_secs = effective_horizon_secs(candidate, capture, fallback_horizon_secs);
    let valid_until = as_of_plus_secs(
        input.as_of,
        entry_window_secs(horizon_secs, entry_window_ratio),
    )?;
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

    let auto_gate = auto_execution_gate(
        candidate,
        planned.rank,
        &planned.sizing,
        input.runtime_config,
    )?;
    let risk_envelope = compose_risk_envelope(planned, candidate, &auto_gate);

    Ok(NewRecommendation {
        recommendation_id: RecommendationId::from_v7(),
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
        entry_plan: derive_entry_plan(candidate, input.as_of, valid_until, input.runtime_config),
        sizing_plan: planned.sizing.clone(),
        exit_plan: exit_plan(
            candidate,
            input.as_of,
            input.runtime_config,
            horizon_secs,
            capture.market_context.time_to_resolution_secs,
        )?,
        risk_envelope,
        factor_breakdown: factor_breakdown(candidate),
        evidence_refs: EvidenceRefs::from_input(EvidenceRefsInput {
            signal_candidate_id: candidate.signal_candidate_id.clone(),
            feature_vector_id,
            model_run_id,
            market_selection_id: input.market_selection_id.clone(),
            book_snapshot_ref: capture.book_snapshot_ref.clone(),
            runtime_config_version_id: input.runtime_config_version_id.clone(),
            model_version_id: input.model_version_id.clone(),
            factor_definition_versions: factor_definition_versions(candidate),
            data_quality_snapshot_ref: data_quality_snapshot_ref.clone(),
        }),
        execution_eligibility: execution_eligibility(&auto_gate),
        valid_from: input.as_of,
        valid_until,
        status: RecommendationStatus::Published,
    })
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

fn exit_plan(
    candidate: &SignalCandidate,
    as_of: DateTime<Utc>,
    config: &RuntimeConfig,
    horizon_secs: u64,
    time_to_resolution_secs: Option<u64>,
) -> QuantResult<ExitPlan> {
    let loss = Bps::new(candidate.downside_bps)
        .to_fraction()
        .max(Decimal::ZERO);
    let reward_multiple = parse_decimal(
        "portfolio.sizing.target_reward_multiple",
        &config.portfolio.sizing.target_reward_multiple.value,
    )?;
    let gain = reward_multiple * loss;
    let take_profit_price = Price::new(
        (candidate.entry_price_ref.inner() * (Decimal::ONE + gain))
            .clamp(Decimal::ZERO, Decimal::ONE),
    );
    let stop_loss_price = Price::new(
        (candidate.entry_price_ref.inner() * (Decimal::ONE - loss))
            .clamp(Decimal::ZERO, Decimal::ONE),
    );
    let time_exit_at = as_of_plus_secs(as_of, horizon_secs)?;

    // Origination of the hold-to-resolution + auto-redeem lifecycle: when the
    // market resolves within the governed window, forcing an on-book time-exit
    // would just pay spread/slippage moments before settlement on a position
    // about to pay 0/1. Holding to resolution and redeeming the CTF payout is
    // strictly better. The protective stop-loss is retained either way (05.6
    // still honors it on hold-to-resolution lots); only the take-profit and the
    // time-exit are dropped when holding. `RedeemPolicy::Auto` is a pure config
    // decision frozen on the intent — the settlement worker remains the
    // authoritative gate that fails closed (topology / neg-risk / balance).
    let redeem = &config.execution.settlement_redeem;
    let hold_to_resolution = redeem.hold_to_resolution_enabled
        && time_to_resolution_secs.is_some_and(|ttr| ttr <= redeem.hold_to_resolution_within_secs);

    let (take_profit_price, take_profit_pct, time_exit_at, max_hold_secs) = if hold_to_resolution {
        (None, None, None, None)
    } else {
        (
            Some(take_profit_price),
            Some(gain),
            Some(time_exit_at),
            Some(horizon_secs),
        )
    };
    let (settlement_mode, redeem_policy) = if hold_to_resolution {
        let redeem_policy = if redeem.enabled {
            RedeemPolicy::Auto
        } else {
            RedeemPolicy::Manual
        };
        (ExitSettlementMode::HoldToResolution, redeem_policy)
    } else {
        (
            ExitSettlementMode::ExitBeforeResolution,
            RedeemPolicy::Manual,
        )
    };

    Ok(ExitPlan {
        take_profit_price,
        take_profit_pct,
        // The protective stop-loss is the single full-exit scalar that survives
        // into hold-to-resolution; `partial_exit_nodes` is reserved for genuine
        // *scaled* exits (`sell_pct < 1`), so the Kelly structure carries none.
        stop_loss_price: Some(stop_loss_price),
        stop_loss_pct: Some(loss),
        time_exit_at,
        max_hold_secs,
        partial_exit_nodes: Vec::new(),
        trailing_stop: None,
        signal_invalidation_rules: Vec::new(),
        settlement_mode,
        redeem_policy,
        manual_review_at: None,
        exit_reason: format!(
            "Kelly bet structure: downside={} bps, target_reward_multiple={}, target_gain={}%, settlement={}; model headline: {}",
            candidate.downside_bps,
            reward_multiple,
            (gain * Decimal::from(100)).round_dp(4),
            settlement_mode.as_str(),
            candidate.model_explanation.headline
        ),
    })
}

fn factor_breakdown(candidate: &SignalCandidate) -> RecommendationFactorBreakdown {
    RecommendationFactorBreakdown(
        candidate
            .factor_breakdown
            .iter()
            .map(|factor| FactorBreakdownEntry {
                factor_name: factor.name.to_string(),
                family: factor.family,
                raw_value: factor.raw_value,
                normalized_score: factor.normalized_score,
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

fn execution_eligibility(gate: &AutoExecutionGate) -> ExecutionEligibility {
    let mut eligible_modes = vec![QuantRuntimeMode::ReportOnly, QuantRuntimeMode::SemiAuto];
    if gate.allowed {
        eligible_modes.push(QuantRuntimeMode::AutoExecution);
    }
    ExecutionEligibility {
        eligible_modes,
        ineligibility_reasons: if gate.allowed {
            Vec::new()
        } else {
            gate.reasons.clone()
        },
        approval_required: !gate.allowed,
        auto_policy_id: gate
            .allowed
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

fn report_summary(
    input: &ComposeReportInput<'_>,
    recommendations: &[NewRecommendation],
) -> ReportSummary {
    let mut category_allocation: BTreeMap<MarketCategory, Usd> = BTreeMap::new();
    let mut event_allocation: BTreeMap<EventId, Usd> = BTreeMap::new();
    for rec in recommendations {
        *category_allocation
            .entry(rec.identity.category)
            .or_default() += rec.sizing_plan.suggested_usd;
        *event_allocation.entry(rec.event_id.clone()).or_default() += rec.sizing_plan.suggested_usd;
    }
    let total_suggested_usd = recommendations
        .iter()
        .map(|rec| rec.sizing_plan.suggested_usd)
        .sum();
    let max_single_recommendation_usd = recommendations
        .iter()
        .map(|rec| rec.sizing_plan.suggested_usd)
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
        if empty.reason == EmptyReason::InsufficientDataQuality {
            0
        } else {
            empty.rejected_count
        }
    });

    ReportSummary {
        market_selection_count: input.market_selection_count,
        candidate_count: input.candidate_count,
        rejected_count: input
            .feature_rejected_count
            .saturating_add(u32::try_from(input.planner_rejected.len()).unwrap_or(u32::MAX))
            .saturating_add(empty_rejected_count),
        published_recommendation_count: u32::try_from(recommendations.len()).unwrap_or(u32::MAX),
        total_suggested_usd,
        max_single_recommendation_usd,
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
    }
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
    if let Some(empty) = &input.empty {
        if empty.reason != EmptyReason::InsufficientDataQuality && empty.rejected_count > 0 {
            counts.insert(empty.reason.as_str().to_owned(), empty.rejected_count);
        }
    }
    if input.feature_rejected_count > 0 {
        *counts
            .entry(EmptyReason::InsufficientDataQuality.as_str().to_owned())
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
) -> Vec<QuantRecommendationEventRow> {
    recommendations
        .iter()
        .map(|rec| QuantRecommendationEventRow {
            event_time: event_time.timestamp_millis(),
            recommendation_report_id: report_id.clone(),
            recommendation_id: rec.recommendation_id.clone(),
            rank: u32::try_from(rec.rank).unwrap_or(0),
            market_id: rec.market_id.clone(),
            token_id: rec.token_id.clone(),
            side: rec.outcome_side.into(),
            score: ChProbability::from(rec.composite_score),
            risk_adjusted_score: ChProbability::from(rec.risk_adjusted_score),
            suggested_usd: ChUsd::from(rec.sizing_plan.suggested_usd),
            valid_until: rec.valid_until.timestamp_millis(),
            status: rec.status.into(),
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
            "as_of": input.as_of.to_rfc3339(),
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
) -> NewPortfolioPlan {
    let total_budget = Usd::new(parse_decimal_lossless(
        &config.portfolio.budget.total_budget_usd.value,
    ));
    let constraints = PortfolioConstraintsSnapshot {
        max_market_exposure_usd: Usd::new(parse_decimal_lossless(
            &config.portfolio.constraints.max_market_exposure_usd.value,
        )),
        max_event_exposure_usd: Usd::new(parse_decimal_lossless(
            &config.portfolio.constraints.max_event_exposure_usd.value,
        )),
        max_category_exposure_usd: Usd::new(parse_decimal_lossless(
            &config.portfolio.constraints.max_category_exposure_usd.value,
        )),
        max_correlated_exposure_usd: Usd::new(parse_decimal_lossless(
            &config
                .portfolio
                .constraints
                .max_correlated_exposure_usd
                .value,
        )),
        max_single_recommendation_usd: Usd::new(parse_decimal_lossless(
            &config.portfolio.budget.max_single_recommendation_usd.value,
        )),
        min_recommendation_usd: Usd::new(parse_decimal_lossless(
            &config.portfolio.budget.min_recommendation_usd.value,
        )),
        liquidity_usage_cap_pct: parse_decimal_lossless(
            &config.portfolio.constraints.liquidity_usage_cap_pct.value,
        ),
    };
    NewPortfolioPlan {
        portfolio_plan_id,
        model_run_id,
        market_selection_id,
        as_of,
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
    }
}

pub(super) fn empty_plan_for_report(
    model_run_id: Option<ModelRunId>,
    market_selection_id: MarketSelectionId,
    as_of: DateTime<Utc>,
    account: &AccountSnapshot,
    config: &RuntimeConfig,
    reason: EmptyReason,
    rejected_count: u32,
) -> NewPortfolioPlan {
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

fn parse_decimal_lossless(value: &str) -> Decimal {
    value.trim().parse::<Decimal>().unwrap_or(Decimal::ZERO)
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_error::{QuantError, report::ReportError};
    use quant_pivot_models::{
        domain::quant::{NewAccountSnapshot, NewEquitySnapshot, NewReportDataQualitySnapshot},
        enums::quant::{
            AccountSource, BindingConstraint, EmptyReason, ExitSettlementMode, IneligibilityReason,
            OutcomeSide, QuantRuntimeMode, RedeemPolicy, SizingModelKind,
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
        ComposeReportInput, DefaultRecommendationComposer, RecommendationComposer,
        auto_execution_gate, effective_horizon_from, empty_plan_for_report, entry_window_secs,
        execution_eligibility, exit_plan,
    };
    use crate::report::types::ReportTrigger;

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
    fn effective_horizon_falls_back_when_model_horizon_absent() {
        // suggested == 0 (classical run) -> governed fallback, still capped.
        assert_eq!(effective_horizon_from(0, Some(86_400), 7_200), 7_200);
        assert_eq!(effective_horizon_from(0, Some(900), 7_200), 900);
        // Never zero (a market at/after resolution still yields a 1s floor).
        assert_eq!(effective_horizon_from(0, Some(0), 7_200), 1);
    }

    #[test]
    fn entry_window_is_the_ratio_slice_of_the_horizon() {
        // Default 0.5 ratio -> enter within the first half (the half-life point).
        assert_eq!(entry_window_secs(3_600, dec!(0.5)), 1_800);
        // Full ratio -> entry valid across the whole horizon.
        assert_eq!(entry_window_secs(3_600, dec!(1.0)), 3_600);
        // Rounds and floors at 1s.
        assert_eq!(entry_window_secs(1, dec!(0.5)), 1);
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
            as_of: Utc
                .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
                .single()
                .expect("valid time"),
        }
    }

    #[test]
    fn exit_plan_uses_reward_multiple_not_expected_return_for_take_profit() {
        let as_of = Utc
            .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
            .single()
            .expect("valid time");
        let mut config = RuntimeConfig::default();
        config.portfolio.sizing.target_reward_multiple.value = "2.0".to_owned();

        let plan = exit_plan(&candidate(), as_of, &config, 3_600, None).expect("exit plan");

        assert_eq!(plan.stop_loss_pct, Some(dec!(0.1)));
        assert_eq!(plan.take_profit_pct, Some(dec!(0.20)));
        assert_eq!(
            plan.stop_loss_price.expect("stop loss price").inner(),
            dec!(0.45)
        );
        assert_eq!(
            plan.take_profit_price.expect("take profit price").inner(),
            dec!(0.600)
        );
        // The scalar TP/SL/time-exit fields are the single source of truth; the
        // full Kelly exit carries no (scaled) partial nodes.
        assert!(plan.partial_exit_nodes.is_empty());
        assert_eq!(plan.time_exit_at, Some(as_of + Duration::seconds(3_600)));
        assert_eq!(
            plan.settlement_mode,
            ExitSettlementMode::ExitBeforeResolution
        );
        assert_eq!(plan.redeem_policy, RedeemPolicy::Manual);
    }

    #[test]
    fn exit_plan_holds_to_resolution_and_auto_redeems_near_resolution() {
        let as_of = Utc
            .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
            .single()
            .expect("valid time");
        let mut config = RuntimeConfig::default();
        config.execution.settlement_redeem.enabled = true;
        config
            .execution
            .settlement_redeem
            .hold_to_resolution_enabled = true;
        config
            .execution
            .settlement_redeem
            .hold_to_resolution_within_secs = 86_400;

        // Market resolves in 1h, inside the 24h hold window.
        let plan = exit_plan(&candidate(), as_of, &config, 3_600, Some(3_600)).expect("exit plan");

        assert_eq!(plan.settlement_mode, ExitSettlementMode::HoldToResolution);
        assert_eq!(plan.redeem_policy, RedeemPolicy::Auto);
        // Take-profit and time-exit are dropped; the protective stop-loss stays.
        assert_eq!(plan.take_profit_price, None);
        assert_eq!(plan.time_exit_at, None);
        assert_eq!(plan.max_hold_secs, None);
        assert!(plan.stop_loss_price.is_some());

        // Far-from-resolution markets keep the on-book exit ladder.
        let far = exit_plan(&candidate(), as_of, &config, 3_600, Some(200_000)).expect("exit plan");
        assert_eq!(
            far.settlement_mode,
            ExitSettlementMode::ExitBeforeResolution
        );
        assert_eq!(far.redeem_policy, RedeemPolicy::Manual);
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

        let eligibility = execution_eligibility(&gate);
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

        let eligibility = execution_eligibility(&gate);
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
        let as_of = Utc
            .with_ymd_and_hms(2026, 6, 1, 0, 0, 0)
            .single()
            .expect("valid time");
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
            EmptyReason::NoPositiveSignal,
            0,
        );
        let data_quality_snapshot = NewReportDataQualitySnapshot {
            report_data_quality_snapshot_id: ReportDataQualitySnapshotId::from_v7(),
            as_of,
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
            source_delay_secs: 0,
            as_of,
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            runtime_config: &config,
            runtime_mode: QuantRuntimeMode::ReportOnly,
            model_version_id: ModelVersionId::from_v7(),
            market_selection_id,
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
