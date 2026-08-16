//! Global recommendation report composition.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, infra::InfraError, report::ReportError};
use quant_pivot_models::{
    clickhouse::{ChUsd, QuantReportRecommendationFactRow},
    domain::{
        governance::NewOperationLog,
        quant::{
            ExecutableEconomicTier, NewAccountSnapshot, NewEntryConditionInstance,
            NewEquitySnapshot, NewPortfolioPlan, NewRecommendation, NewRecommendationReport,
            NewReportDataQualitySnapshot, NewReportRouteRun, NewReportTransaction,
            PortfolioDecisionResult,
        },
    },
    enums::{
        common::{MarketCategory, TickSize},
        operation_log::{OperationCategory, OperationHttpMethod, OperationOutcome},
        quant::{
            EmptyReportReason, EntryConditionState, FillRequirement, IneligibilityReason,
            QuantRuntimeMode, RecommendationReportStatus, RecommendationStatus, ReportKind,
        },
        rbac::ResourceType,
    },
    hashing::{CanonicalDigest, canonical_state_hash},
    runtime_config::{BuyModelRoute, DecisionPolicySnapshot},
    types::{
        BookSnapshotRef, BootstrapExitGuidance, ConditionTruth, DecisionPolicySnapshotId,
        EligibilitySummary, EntryConditionFoldState, EntryConditionInstanceId, EntryConditionPlan,
        EntryOrderPolicy, EntryOrderTemplate, EntryPlan, EventId, EvidenceRefs, EvidenceRefsInput,
        ExecutionEligibility, ExitPlan, FactorBreakdownEntry, FactorDefinitionId, FeatureVectorId,
        MarketId, OperationDetailDocument, OperationLogId, PortfolioRejectionReason, Price,
        RecommendationExitPlan, RecommendationFactorBreakdown, RecommendationId,
        RecommendationIdentity, RecommendationReportId, RecommendationTradePlan,
        RejectionReasonCount, ReportDataQualitySnapshotId, ReportRunId, ReportSummary,
        RiskEnvelope, RiskEnvelopeHashInput, ScaleOutTarget, SizingPlan, ThesisInvalidationPolicy,
        TrailingStopPolicy, Usd, UsdHours,
    },
};
use quant_pivot_research::{
    features::MarketDecisionCapture, model::SignalCandidate, portfolio::AccountSnapshot,
    selection::MarketSelectionSnapshot,
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use super::{
    funnel::{PublishedRecommendationRef, ReportFunnelInput, build_report_market_funnel},
    types::{
        ComposedReport, EconomicTierBuildRejection, EmptyReportContext, NotificationRecommendation,
        PlannedRecommendationContract, PlannedReportRecommendation, ReportNotificationPayload,
        ReportTierRejection, ReportTrigger,
    },
};
use crate::service::{feature_pipeline::RejectedMarket, model_runner::ModelMarketDecision};

/// Frozen inputs required to compose one globally optimized report transaction.
pub struct ComposeReportInput<'a> {
    pub report_run_id: ReportRunId,
    pub trigger: &'a ReportTrigger,
    pub trigger_key: String,
    pub decision_at: DateTime<Utc>,
    pub published_at: DateTime<Utc>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub runtime_config: &'a DecisionPolicySnapshot,
    pub runtime_mode: QuantRuntimeMode,
    pub selection: &'a MarketSelectionSnapshot,
    pub account: &'a AccountSnapshot,
    pub account_snapshot: NewAccountSnapshot,
    pub equity_snapshot: NewEquitySnapshot,
    pub portfolio_plan: NewPortfolioPlan,
    pub route_runs: Vec<NewReportRouteRun>,
    pub tiers: &'a [ExecutableEconomicTier],
    pub planned: &'a [PlannedReportRecommendation],
    pub tier_rejections: &'a [ReportTierRejection],
    pub tier_build_rejections: &'a [EconomicTierBuildRejection],
    pub feature_rejected: &'a [RejectedMarket],
    pub model_decisions: &'a [ModelMarketDecision],
    pub captures: &'a HashMap<MarketId, MarketDecisionCapture>,
    pub feature_vector_by_market: &'a HashMap<MarketId, FeatureVectorId>,
    pub data_quality_snapshot: NewReportDataQualitySnapshot,
    pub candidate_count: u32,
    pub empty: Option<EmptyReportContext>,
    pub top_n: u32,
}

/// Converts frozen Route/model/portfolio output into one atomic persistence transaction.
pub trait RecommendationComposer: Send + Sync {
    fn compose(&self, input: ComposeReportInput<'_>) -> QuantResult<ComposedReport>;
}

/// Production global report composer.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultRecommendationComposer;

impl DefaultRecommendationComposer {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl RecommendationComposer for DefaultRecommendationComposer {
    fn compose(&self, input: ComposeReportInput<'_>) -> QuantResult<ComposedReport> {
        validate_compose_input(&input)?;
        let report_id = RecommendationReportId::from_v7();
        let data_quality_snapshot_ref = input.data_quality_snapshot.report_data_quality_snapshot_id;
        let mut exposure = ExposureCursor::from_account(input.account);
        let mut auto_authorized_total = Usd::ZERO;
        let mut rows = Vec::with_capacity(input.planned.len());
        let mut prior_rank = 0_u32;
        for planned in input.planned {
            if planned.rank <= prior_rank {
                return Err(ReportError::InvariantViolation {
                    stage: "global_report_compose",
                    detail: "recommendation ranks are not strictly increasing".to_owned(),
                }
                .into());
            }
            prior_rank = planned.rank;
            let composed = compose_recommendation(
                &report_id,
                planned,
                &input,
                &data_quality_snapshot_ref,
                &mut exposure,
                auto_authorized_total,
            )?;
            if composed
                .recommendation
                .execution_eligibility
                .is_eligible(QuantRuntimeMode::AutoExecution)
            {
                auto_authorized_total += planned.tier.entry.notional_usd;
            }
            rows.push(composed);
        }

        let recommendations = rows
            .iter()
            .map(|row| row.recommendation.clone())
            .collect::<Vec<_>>();
        let entry_condition_instances = rows
            .into_iter()
            .map(|row| row.condition_instance)
            .collect::<Vec<_>>();
        let summary = report_summary(&input, &recommendations)?;
        let valid_until = recommendations
            .iter()
            .map(|recommendation| recommendation.valid_until)
            .max();
        let report = NewRecommendationReport {
            recommendation_report_id: report_id,
            report_run_id: input.report_run_id,
            report_kind: ReportKind::TopN,
            decision_at: input.decision_at,
            runtime_mode: input.runtime_mode,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            market_selection_id: input.selection.market_selection_id,
            portfolio_plan_id: input.portfolio_plan.portfolio_plan_id,
            represented_routes_json: input.portfolio_plan.represented_routes_json.clone(),
            scenario_artifact_id: input.portfolio_plan.scenario_artifact_id,
            scenario_artifact_hash: input.portfolio_plan.scenario_artifact_hash,
            top_n: i32::try_from(input.top_n).map_err(|error| ReportError::NumericOverflow {
                field: "report.top_n",
                detail: error.to_string(),
            })?,
            status: RecommendationReportStatus::Prepared,
            account_source: input.account.source,
            capital_base_usd: input.account.capital_base_usd,
            account_snapshot_ref: input.account_snapshot.account_snapshot_id,
            equity_snapshot_ref: input.equity_snapshot.equity_snapshot_id,
            data_quality_snapshot_ref,
            summary_json: summary.clone(),
            published_at: None,
            successor_report_id: None,
            superseded_at: None,
            obsoleted_at: None,
            valid_until,
            revoked_at: None,
            expired_at: None,
            status_reason: None,
            created_at: input.published_at,
        };
        let operation_log = operation_log(&report_id, &input, &report)?;
        let ch_rows = recommendation_events(&report_id, &recommendations, input.published_at)?;
        let published = recommendations
            .iter()
            .map(|recommendation| PublishedRecommendationRef {
                recommendation_id: recommendation.recommendation_id,
                market_id: recommendation.market_id.clone(),
                report_route_run_id: recommendation.report_route_run_id,
                route: recommendation.route,
            })
            .collect::<Vec<_>>();
        let funnel_rows = build_report_market_funnel(ReportFunnelInput {
            report_id: &report_id,
            decision_policy_snapshot_id: &input.decision_policy_snapshot_id,
            selection: input.selection,
            route_runs: &input.route_runs,
            feature_rejected: input.feature_rejected,
            feature_vector_by_market: input.feature_vector_by_market,
            model_decisions: input.model_decisions,
            tiers: input.tiers,
            tier_rejections: input.tier_rejections,
            tier_build_rejections: input.tier_build_rejections,
            recommendations: &published,
            event_time: input.published_at,
        })?;
        let notification = report_notification(
            &report_id,
            input.runtime_mode,
            &summary,
            &recommendations,
            input.empty.as_ref().map(|empty| empty.reason),
        )?;

        Ok(ComposedReport {
            transaction: NewReportTransaction {
                feature_parity_state_id: None,
                account_snapshot: input.account_snapshot,
                equity_snapshot: input.equity_snapshot,
                data_quality_snapshot: input.data_quality_snapshot,
                portfolio_plan: input.portfolio_plan,
                report,
                route_runs: input.route_runs,
                recommendations,
                entry_condition_artifacts: Vec::new(),
                entry_condition_instances,
                sampled_feature_parity: None,
                fact_delivery: None,
                operation_log,
            },
            ch_rows,
            funnel_rows,
            notification,
            delivery_policy: input.runtime_config.recommendation.reports.delivery_policy,
            notify_operators: input
                .runtime_config
                .operations_policy
                .notifications
                .report_published,
        })
    }
}

fn validate_compose_input(input: &ComposeReportInput<'_>) -> QuantResult<()> {
    if input.account.as_of != input.decision_at
        || input.account_snapshot.as_of != input.decision_at
        || input.equity_snapshot.as_of != input.decision_at
        || input.portfolio_plan.decision_at != input.decision_at
        || input.portfolio_plan.account_snapshot_id != input.account_snapshot.account_snapshot_id
        || input.portfolio_plan.market_selection_id != input.selection.market_selection_id
        || input.portfolio_plan.decision_policy_snapshot_id != input.decision_policy_snapshot_id
    {
        return Err(ReportError::InvariantViolation {
            stage: "global_report_compose",
            detail: "account, policy, selection, equity, and portfolio snapshots are not atomic"
                .to_owned(),
        }
        .into());
    }
    if input.planned.len() > usize::try_from(input.top_n).unwrap_or(usize::MAX) {
        return Err(ReportError::InvariantViolation {
            stage: "global_report_compose",
            detail: "optimizer output exceeds frozen TopN".to_owned(),
        }
        .into());
    }
    Ok(())
}

struct ExposureCursor {
    market: BTreeMap<MarketId, Usd>,
    event: BTreeMap<EventId, Usd>,
    category: BTreeMap<MarketCategory, Usd>,
    route: BTreeMap<BuyModelRoute, Usd>,
}

impl ExposureCursor {
    fn from_account(account: &AccountSnapshot) -> Self {
        let mut route = BTreeMap::new();
        for position in &account.positions {
            *route
                .entry(BuyModelRoute::from(position.category))
                .or_default() += position.current_value;
        }
        Self {
            market: account.exposures.per_market.clone(),
            event: account.exposures.per_event.clone(),
            category: account.exposures.per_category.clone(),
            route,
        }
    }

    fn allocate(&mut self, planned: &PlannedReportRecommendation) -> SizingExposure {
        let amount = planned.tier.entry.notional_usd;
        let market = self
            .market
            .entry(planned.tier.market_id.clone())
            .or_default();
        *market += amount;
        let market_after = *market;
        let event = self.event.entry(planned.tier.event_id.clone()).or_default();
        *event += amount;
        let event_after = *event;
        let category = self.category.entry(planned.tier.category).or_default();
        *category += amount;
        let category_after = *category;
        let route = self.route.entry(planned.route).or_default();
        *route += amount;
        SizingExposure {
            market: market_after,
            event: event_after,
            category: category_after,
            route: *route,
        }
    }
}

struct SizingExposure {
    market: Usd,
    event: Usd,
    category: Usd,
    route: Usd,
}

struct RecommendationEvidence<'a> {
    capture: &'a MarketDecisionCapture,
    identity: RecommendationIdentity,
    book_snapshot_ref: BookSnapshotRef,
    feature_vector_id: FeatureVectorId,
    horizon_secs: u64,
}

struct ComposedRecommendationRows {
    recommendation: NewRecommendation,
    condition_instance: NewEntryConditionInstance,
}

fn compose_recommendation(
    report_id: &RecommendationReportId,
    planned: &PlannedReportRecommendation,
    input: &ComposeReportInput<'_>,
    data_quality_snapshot_ref: &ReportDataQualitySnapshotId,
    exposure: &mut ExposureCursor,
    auto_authorized_before: Usd,
) -> QuantResult<ComposedRecommendationRows> {
    let candidate = &planned.candidate;
    let RecommendationEvidence {
        capture,
        identity,
        book_snapshot_ref,
        feature_vector_id,
        horizon_secs,
    } = recommendation_evidence(planned, input)?;
    let effective_horizon = capture
        .market_context
        .time_to_resolution_secs
        .map_or(horizon_secs, |remaining| remaining.min(horizon_secs));
    let valid_until = actionable_valid_until(
        input.decision_at,
        input.published_at,
        entry_window_secs(
            effective_horizon,
            input
                .runtime_config
                .recommendation
                .reports
                .entry_window_ratio
                .value,
        )?,
        &candidate.market_id,
    )?;
    let exposure_after = exposure.allocate(planned);
    let portfolio_weight_pct = if input.account.capital_base_usd.is_positive() {
        planned.tier.entry.notional_usd.inner() / input.account.capital_base_usd.inner()
    } else {
        return Err(ReportError::InvariantViolation {
            stage: "global_recommendation_compose",
            detail: "capital base must be positive".to_owned(),
        }
        .into());
    };
    let sizing = SizingPlan {
        economic_tier_id: planned.tier.economic_tier_id,
        suggested_usd: planned.tier.entry.notional_usd,
        suggested_shares: planned.tier.shares,
        entry_vwap: planned.tier.entry.entry_vwap,
        portfolio_weight_pct,
        market_exposure_after_usd: exposure_after.market,
        event_exposure_after_usd: exposure_after.event,
        category_exposure_after_usd: exposure_after.category,
        route_exposure_after_usd: exposure_after.route,
        capital_occupancy_usd_hours: planned.tier.economics.capital_occupancy_usd_hours,
        sizing_reason: format!(
            "global MILP selected tier {} with marginal portfolio value {}",
            planned.tier.economic_tier_id, planned.tier.economics.marginal_portfolio_value_usd
        ),
    };
    let bootstrap = matches!(
        &planned.contract,
        PlannedRecommendationContract::Bootstrap { .. }
    );
    let auto_allowed = !bootstrap
        && auto_execution_allowed(
            planned.rank,
            sizing.suggested_usd,
            auto_authorized_before,
            input.runtime_config,
        );
    let risk_envelope = risk_envelope(planned, input.runtime_config, auto_allowed)?;
    let entry = immediate_entry_plan(planned, input.published_at, valid_until)?;
    let exit =
        recommendation_exit_plan(input.decision_at, capture.market_context.tick_size, planned)?;
    let trade_plan = RecommendationTradePlan {
        policy: Box::new(planned.contract.provenance()),
        entry,
        sizing: Box::new(sizing),
        exit: Box::new(exit),
        risk_envelope: Box::new(risk_envelope),
    };
    let execution_eligibility =
        execution_eligibility(bootstrap, auto_allowed, &input.decision_policy_snapshot_id);
    let recommendation_id = RecommendationId::from_v7();
    let recommendation = NewRecommendation {
        recommendation_id,
        recommendation_report_id: *report_id,
        report_route_run_id: planned.report_route_run_id,
        portfolio_plan_id: input.portfolio_plan.portfolio_plan_id,
        economic_tier_id: planned.tier.economic_tier_id,
        rank: i32::try_from(planned.rank).map_err(|error| ReportError::NumericOverflow {
            field: "recommendation.rank",
            detail: error.to_string(),
        })?,
        route: planned.route,
        market_id: candidate.market_id.clone(),
        event_id: planned.tier.event_id.clone(),
        token_id: candidate.token_id.clone(),
        outcome_side: candidate.outcome_side,
        economics_json: planned.tier.economics,
        economic_tier_json: planned.tier.clone(),
        identity,
        market_context: capture.market_context.clone(),
        trade_plan,
        factor_breakdown: factor_breakdown(candidate),
        evidence_refs: EvidenceRefs::from_input(EvidenceRefsInput {
            signal_candidate_id: candidate.signal_candidate_id,
            feature_vector_id,
            model_run_id: planned.model_run_id,
            market_selection_id: input.selection.market_selection_id,
            book_snapshot_ref,
            decision_policy_snapshot_id: input.decision_policy_snapshot_id,
            model_version_id: planned.model_version_id,
            factor_definition_versions: factor_definition_versions(candidate),
            data_quality_snapshot_ref: *data_quality_snapshot_ref,
        }),
        execution_eligibility,
        valid_from: input.published_at,
        valid_until,
        status: RecommendationStatus::Prepared,
        created_at: input.published_at,
    };
    let condition_instance = NewEntryConditionInstance {
        condition_instance_id: EntryConditionInstanceId::from_v7(),
        recommendation_id,
        artifact_id: None,
        artifact_hash: None,
        state: EntryConditionState::NotRequired,
        truth_json: Some(ConditionTruth::Satisfied),
        revision: 0,
        evaluation_hash: None,
        input_fingerprint: None,
        continuity_hash: None,
        fold_state_json: EntryConditionFoldState::default(),
        confirmation_started_at: None,
        last_evaluated_at: None,
        next_evaluation_at: None,
        expires_at: valid_until,
        lease_owner: None,
        lease_expires_at: None,
        lease_epoch: 0,
        claimed_by_intent_id: None,
        claim_admission_state_version: None,
        consumed_at: None,
    };
    Ok(ComposedRecommendationRows {
        recommendation,
        condition_instance,
    })
}

fn recommendation_evidence<'a>(
    planned: &PlannedReportRecommendation,
    input: &'a ComposeReportInput<'_>,
) -> QuantResult<RecommendationEvidence<'a>> {
    let candidate = &planned.candidate;
    if candidate.signal_candidate_id != planned.tier.candidate_id
        || candidate.model_run_id != planned.model_run_id
        || candidate.market_id != planned.tier.market_id
        || candidate.token_id != planned.tier.token_id
        || candidate.outcome_side != planned.tier.outcome_side
        || planned.route != planned.tier.route
        || planned.report_route_run_id != planned.tier.report_route_run_id
    {
        return Err(ReportError::InvariantViolation {
            stage: "global_recommendation_compose",
            detail: "candidate, Route run, selected tier, and exact economics disagree".to_owned(),
        }
        .into());
    }
    let capture = input.captures.get(&candidate.market_id).ok_or_else(|| {
        ReportError::InvariantViolation {
            stage: "global_recommendation_compose",
            detail: format!(
                "market {} has no frozen decision capture",
                candidate.market_id
            ),
        }
    })?;
    let identity = capture
        .identity_for(&candidate.token_id)
        .cloned()
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "global_recommendation_compose",
            detail: format!(
                "token {} has no aligned frozen identity",
                candidate.token_id
            ),
        })?;
    let book_snapshot_ref = capture
        .book_snapshot_ref_for(&candidate.token_id)
        .cloned()
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "global_recommendation_compose",
            detail: format!("token {} has no aligned L2 evidence", candidate.token_id),
        })?;
    let feature_vector_id = input
        .feature_vector_by_market
        .get(&candidate.market_id)
        .copied()
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "global_recommendation_compose",
            detail: format!(
                "market {} has no feature-vector lineage",
                candidate.market_id
            ),
        })?;
    let route_run = input
        .route_runs
        .iter()
        .find(|run| run.report_route_run_id == planned.report_route_run_id)
        .ok_or_else(|| ReportError::InvariantViolation {
            stage: "global_recommendation_compose",
            detail: format!("missing Route run {}", planned.report_route_run_id),
        })?;
    let lineage =
        route_run
            .lineage_json
            .as_ref()
            .ok_or_else(|| ReportError::InvariantViolation {
                stage: "global_recommendation_compose",
                detail: "selected Route run has no frozen lineage".to_owned(),
            })?;
    let contract_matches = match &planned.contract {
        PlannedRecommendationContract::FullL2 { provenance, .. } => {
            lineage.trade_policy_artifact_id == Some(provenance.artifact_id)
                && lineage.recommendation_contract_hash == provenance.artifact_hash
        }
        PlannedRecommendationContract::Bootstrap {
            profile_ref,
            feature_contract: _,
            recommendation_contract_hash,
            ..
        } => {
            lineage.trade_policy_artifact_id.is_none()
                && lineage.research_profile_ref == *profile_ref
                && lineage.recommendation_contract_hash == *recommendation_contract_hash
        }
    };
    if lineage.model_run_id != Some(planned.model_run_id)
        || lineage.model_version_id != planned.model_version_id
        || !contract_matches
    {
        return Err(ReportError::InvariantViolation {
            stage: "global_recommendation_compose",
            detail: "selected recommendation differs from Route lineage".to_owned(),
        }
        .into());
    }
    let horizon_secs = u64::try_from(lineage.prediction_horizon_secs).map_err(|error| {
        ReportError::InvariantViolation {
            stage: "global_recommendation_compose",
            detail: format!("Route prediction horizon is invalid: {error}"),
        }
    })?;
    Ok(RecommendationEvidence {
        capture,
        identity,
        book_snapshot_ref,
        feature_vector_id,
        horizon_secs,
    })
}

fn immediate_entry_plan(
    planned: &PlannedReportRecommendation,
    valid_from: DateTime<Utc>,
    valid_until: DateTime<Utc>,
) -> QuantResult<EntryPlan> {
    if let PlannedRecommendationContract::Bootstrap {
        max_slippage_bps,
        min_depth_usd,
        max_book_age_ms,
        ..
    } = &planned.contract
    {
        return Ok(EntryPlan {
            condition: EntryConditionPlan::Immediate,
            order_policy: EntryOrderPolicy::Aggressive {
                worst_price: planned.entry_limit_price,
                fill_requirement: FillRequirement::AllowPartial,
            },
            max_slippage_bps: *max_slippage_bps,
            valid_from,
            valid_until,
            min_depth_usd: *min_depth_usd,
            max_book_age_ms: *max_book_age_ms,
            cancel_if_not_triggered: true,
            entry_reason: "bootstrap report-only guidance priced and sized from frozen live L2"
                .to_owned(),
        });
    }
    let PlannedRecommendationContract::FullL2 { provenance, cohort } = &planned.contract else {
        return Err(ReportError::InvariantViolation {
            stage: "global_recommendation_compose",
            detail: "recommendation contract is not supported".to_owned(),
        }
        .into());
    };
    if !matches!(
        &cohort.entry_condition,
        quant_pivot_models::types::EntryConditionTemplate::Immediate
    ) {
        return Err(ReportError::InvariantViolation {
            stage: "global_recommendation_compose",
            detail: "selected executable tier is not an immediate-entry policy cohort".to_owned(),
        }
        .into());
    }
    let (fill_requirement, max_slippage_bps) = match &cohort.entry_order {
        EntryOrderTemplate::Aggressive {
            fill_requirement,
            max_slippage_bps,
            ..
        } => (*fill_requirement, *max_slippage_bps),
        EntryOrderTemplate::PassivePostOnly { .. } => {
            return Err(ReportError::InvariantViolation {
                stage: "global_recommendation_compose",
                detail: "selected executable tier uses a non-executable passive entry".to_owned(),
            }
            .into());
        }
    };
    Ok(EntryPlan {
        condition: EntryConditionPlan::Immediate,
        order_policy: EntryOrderPolicy::Aggressive {
            worst_price: planned.entry_limit_price,
            fill_requirement,
        },
        max_slippage_bps,
        valid_from,
        valid_until,
        min_depth_usd: cohort.key.cash_budget_tier,
        max_book_age_ms: cohort.max_book_age_ms,
        cancel_if_not_triggered: true,
        entry_reason: format!(
            "published Trade Policy {} cohort {}",
            provenance.artifact_id, provenance.cohort_index
        ),
    })
}

fn recommendation_exit_plan(
    as_of: DateTime<Utc>,
    tick_size: TickSize,
    planned: &PlannedReportRecommendation,
) -> QuantResult<RecommendationExitPlan> {
    let PlannedRecommendationContract::FullL2 { provenance, cohort } = &planned.contract else {
        let PlannedRecommendationContract::Bootstrap {
            reference_horizon_secs,
            ..
        } = &planned.contract
        else {
            return Err(ReportError::InvariantViolation {
                stage: "global_recommendation_compose",
                detail: "recommendation contract is not supported".to_owned(),
            }
            .into());
        };
        return Ok(RecommendationExitPlan::BootstrapAdvisory {
            guidance: BootstrapExitGuidance {
                reference_horizon_secs: *reference_horizon_secs,
                manual_review_at: instant_plus_secs(as_of, *reference_horizon_secs)?,
                settlement_value_is_terminal: true,
                guidance: "ReportOnly bootstrap: reassess manually at the reference horizon or market settlement; no executable exit thresholds are authorized"
                    .to_owned(),
            },
        });
    };
    let entry = planned.tier.entry.entry_vwap.inner();
    let upper_factor = Decimal::ONE + cohort.upper_barrier_bps.inner() / Decimal::from(10_000);
    let lower_factor = Decimal::ONE - cohort.lower_barrier_bps.inner() / Decimal::from(10_000);
    let time_exit_at = instant_plus_secs(as_of, cohort.vertical_barrier_secs)?;
    let scale_out_targets = cohort
        .scale_out_targets
        .iter()
        .map(|target| ScaleOutTarget {
            target_id: target.target_id.clone(),
            trigger_price: tick_aligned_price(
                entry * (Decimal::ONE + target.trigger_return_bps.inner() / Decimal::from(10_000)),
                tick_size,
            ),
            target_cumulative_exit_pct: target.target_cumulative_exit_pct,
            min_price: None,
            valid_after: None,
            valid_until: Some(time_exit_at),
            reason: format!("Trade Policy cohort {}", provenance.cohort_index),
        })
        .collect();
    let trailing_stop = cohort
        .trailing_stop
        .as_ref()
        .map(|trailing| TrailingStopPolicy {
            trail_bps: trailing.trail_bps,
            activation_price: Some(tick_aligned_price(
                entry
                    * (Decimal::ONE
                        + trailing.activation_return_bps.inner() / Decimal::from(10_000)),
                tick_size,
            )),
        });
    Ok(ExitPlan {
        take_profit_price: Some(tick_aligned_price(entry * upper_factor, tick_size)),
        take_profit_pct: Some(upper_factor - Decimal::ONE),
        stop_loss_price: Some(tick_aligned_price(entry * lower_factor, tick_size)),
        stop_loss_pct: Some(Decimal::ONE - lower_factor),
        time_exit_at: Some(time_exit_at),
        max_hold_secs: Some(cohort.vertical_barrier_secs),
        scale_out_targets,
        trailing_stop,
        thesis_invalidation: ThesisInvalidationPolicy {
            min_score_retention: cohort.min_score_retention,
            min_expected_return_bps: cohort.min_expected_return_bps,
            require_route_gate_eligibility: cohort.require_route_gate_eligibility,
        },
        opportunistic_exit: cohort.opportunistic_exit.clone(),
        settlement_mode: cohort.settlement_mode,
        redeem_policy: cohort.redeem_policy,
        manual_review_at: None,
        exit_reason: format!(
            "published Trade Policy {} cohort {}",
            provenance.artifact_id, provenance.cohort_index
        ),
    }
    .into())
}

fn tick_aligned_price(value: Decimal, tick_size: TickSize) -> Price {
    let tick = tick_size.as_decimal();
    let units = value.clamp(tick, Decimal::ONE - tick) / tick;
    Price::new((units.ceil() * tick).clamp(tick, Decimal::ONE - tick))
}

fn risk_envelope(
    planned: &PlannedReportRecommendation,
    config: &DecisionPolicySnapshot,
    auto_allowed: bool,
) -> QuantResult<RiskEnvelope> {
    let limits = &config.execution_risk.portfolio.exposure_limits;
    let tail = &config.execution_risk.portfolio.tail_risk;
    let input = RiskEnvelopeHashInput {
        loss_usd: planned.tier.economics.max_loss_usd,
        slippage_bps: match &planned.contract {
            PlannedRecommendationContract::FullL2 { cohort, .. } => cohort.max_slippage_bps,
            PlannedRecommendationContract::Bootstrap {
                max_slippage_bps, ..
            } => *max_slippage_bps,
        },
        position_usd: Usd::new(limits.max_single_recommendation_usd.value),
        market_exposure_usd: Usd::new(limits.max_market_exposure_usd.value),
        event_exposure_usd: Usd::new(limits.max_event_exposure_usd.value),
        category_exposure_usd: Usd::new(limits.max_category_exposure_usd.value),
        route_exposure_usd: Usd::new(limits.max_route_exposure_usd.value),
        cvar_contribution_usd: planned.tier.economics.cvar_contribution_usd,
        portfolio_cvar_cap_usd: Usd::new(tail.max_cvar_usd.value),
        maximum_scenario_loss_cap_usd: Usd::new(tail.max_scenario_loss_usd.value),
    };
    let envelope_hash = CanonicalDigest::content_hash_json(&input)
        .map_err(|error| QuantError::config(format!("risk envelope hash failed: {error}")))?;
    Ok(RiskEnvelope {
        max_loss_usd: input.loss_usd,
        max_slippage_bps: input.slippage_bps,
        max_position_usd: input.position_usd,
        max_market_exposure_usd: input.market_exposure_usd,
        max_event_exposure_usd: input.event_exposure_usd,
        max_category_exposure_usd: input.category_exposure_usd,
        max_route_exposure_usd: input.route_exposure_usd,
        cvar_contribution_usd: input.cvar_contribution_usd,
        portfolio_cvar_cap_usd: input.portfolio_cvar_cap_usd,
        maximum_scenario_loss_cap_usd: input.maximum_scenario_loss_cap_usd,
        requires_approval: !auto_allowed,
        auto_execution_allowed: auto_allowed,
        risk_notes: Vec::new(),
        envelope_hash,
    })
}

fn auto_execution_allowed(
    rank: u32,
    suggested_usd: Usd,
    already_authorized: Usd,
    config: &DecisionPolicySnapshot,
) -> bool {
    let policy = &config.execution_automation_policy.auto_execution;
    rank <= policy.max_orders_per_report
        && already_authorized
            .inner()
            .checked_add(suggested_usd.inner())
            .is_some_and(|total| total <= policy.max_total_usd_per_report.value)
}

fn execution_eligibility(
    bootstrap: bool,
    auto_allowed: bool,
    policy_snapshot_id: &DecisionPolicySnapshotId,
) -> ExecutionEligibility {
    if bootstrap {
        return ExecutionEligibility {
            eligible_modes: vec![QuantRuntimeMode::ReportOnly],
            ineligibility_reasons: vec![IneligibilityReason::ReportOnlyMode],
            approval_required: true,
            auto_policy_id: None,
        };
    }
    let mut eligible_modes = vec![QuantRuntimeMode::ReportOnly, QuantRuntimeMode::SemiAuto];
    if auto_allowed {
        eligible_modes.push(QuantRuntimeMode::AutoExecution);
    }
    ExecutionEligibility {
        eligible_modes,
        ineligibility_reasons: if auto_allowed {
            Vec::new()
        } else {
            vec![IneligibilityReason::AutomationCapExceeded]
        },
        approval_required: !auto_allowed,
        auto_policy_id: auto_allowed.then(|| policy_snapshot_id.to_string()),
    }
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
    let mut ids = candidate
        .factor_breakdown
        .iter()
        .map(|factor| factor.definition_id)
        .collect::<Vec<_>>();
    ids.sort_unstable_by_key(|id| id.as_uuid());
    ids.dedup();
    ids
}

fn report_summary(
    input: &ComposeReportInput<'_>,
    recommendations: &[NewRecommendation],
) -> QuantResult<ReportSummary> {
    let mut category_allocation = BTreeMap::new();
    let mut event_allocation = BTreeMap::new();
    let mut route_allocation = BTreeMap::new();
    for recommendation in recommendations {
        let amount = recommendation.economic_tier_json.entry.notional_usd;
        *category_allocation
            .entry(recommendation.economic_tier_json.category)
            .or_default() += amount;
        *event_allocation
            .entry(recommendation.event_id.clone())
            .or_default() += amount;
        *route_allocation.entry(recommendation.route).or_default() += amount;
    }
    let total_suggested_usd = recommendations
        .iter()
        .map(|recommendation| recommendation.economic_tier_json.entry.notional_usd)
        .sum();
    let max_single_recommendation_usd = recommendations
        .iter()
        .map(|recommendation| recommendation.economic_tier_json.entry.notional_usd)
        .max()
        .unwrap_or(Usd::ZERO);
    let (robust, nominal, cvar, maximum_loss, capital_hours) =
        match &input.portfolio_plan.decision_json {
            PortfolioDecisionResult::Optimized { plan } => (
                plan.objectives.robust_expected_net_usd,
                plan.objectives.nominal_expected_net_usd,
                plan.objectives.cvar_usd,
                plan.constraints.maximum_scenario_loss_usd,
                plan.objectives.capital_occupancy_usd_hours,
            ),
            PortfolioDecisionResult::ZeroCandidates { .. } => {
                (Usd::ZERO, Usd::ZERO, Usd::ZERO, Usd::ZERO, UsdHours::ZERO)
            }
        };
    Ok(ReportSummary {
        market_selection_count: count_u32(input.selection.included.len(), "selection count")?,
        represented_route_count: count_u32(
            input.portfolio_plan.represented_routes_json.routes.len(),
            "represented Route count",
        )?,
        candidate_count: input.candidate_count,
        rejected_tier_count: count_u32(input.tier_rejections.len(), "rejected tier count")?,
        published_recommendation_count: count_u32(
            recommendations.len(),
            "published recommendation count",
        )?,
        total_suggested_usd,
        max_single_recommendation_usd,
        robust_expected_net_usd: robust,
        nominal_expected_net_usd: nominal,
        cvar_usd: cvar,
        maximum_scenario_loss_usd: maximum_loss,
        capital_occupancy_usd_hours: capital_hours,
        category_allocation,
        event_allocation,
        route_allocation,
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

fn rejection_summary(input: &ComposeReportInput<'_>) -> Vec<RejectionReasonCount> {
    let mut counts = BTreeMap::<PortfolioRejectionReason, u32>::new();
    for rejection in input.tier_rejections {
        *counts
            .entry(PortfolioRejectionReason::from(rejection.code))
            .or_default() += 1;
    }
    let selected = u32::try_from(input.planned.len()).unwrap_or(u32::MAX);
    let rejected = u32::try_from(input.tier_rejections.len()).unwrap_or(u32::MAX);
    let not_selected = u32::try_from(input.tiers.len())
        .unwrap_or(u32::MAX)
        .saturating_sub(selected)
        .saturating_sub(rejected);
    if not_selected > 0 {
        counts.insert(
            PortfolioRejectionReason::NotSelectedByGlobalOptimum,
            not_selected,
        );
    }
    counts
        .into_iter()
        .map(|(reason, count)| RejectionReasonCount { reason, count })
        .collect()
}

fn eligibility_summary(recommendations: &[NewRecommendation]) -> EligibilitySummary {
    let mut summary = EligibilitySummary::default();
    for recommendation in recommendations {
        if recommendation
            .execution_eligibility
            .is_eligible(QuantRuntimeMode::ReportOnly)
        {
            summary.eligible_report_only += 1;
        }
        if recommendation
            .execution_eligibility
            .is_eligible(QuantRuntimeMode::SemiAuto)
        {
            summary.eligible_semi_auto += 1;
        }
        if recommendation
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
) -> QuantResult<Vec<QuantReportRecommendationFactRow>> {
    recommendations
        .iter()
        .map(|recommendation| {
            let economics = recommendation.economics_json;
            Ok(QuantReportRecommendationFactRow {
                event_time: event_time.timestamp_millis(),
                recommendation_report_id: *report_id,
                recommendation_id: recommendation.recommendation_id,
                report_route_run_id: recommendation.report_route_run_id,
                economic_tier_id: recommendation.economic_tier_id,
                route: recommendation.route.as_str().to_owned(),
                rank: u32::try_from(recommendation.rank).map_err(|error| {
                    ReportError::NumericOverflow {
                        field: "recommendation.rank",
                        detail: error.to_string(),
                    }
                })?,
                market_id: recommendation.market_id.clone(),
                token_id: recommendation.token_id.clone(),
                side: recommendation.outcome_side.into(),
                profit_probability_bps: economics
                    .profit_probability_bps
                    .inner()
                    .to_i64()
                    .ok_or_else(|| ReportError::NumericOverflow {
                        field: "recommendation.profit_probability_bps",
                        detail: "probability basis points do not fit i64".to_owned(),
                    })?,
                nominal_expected_net_usd: ChUsd::from(economics.nominal_expected_net_usd),
                robust_expected_net_usd: ChUsd::from(economics.robust_expected_net_usd),
                max_loss_usd: ChUsd::from(economics.max_loss_usd),
                cvar_contribution_usd: ChUsd::from(economics.cvar_contribution_usd),
                capital_occupancy_usd_hours: ChUsd::from(Usd::new(
                    economics.capital_occupancy_usd_hours.inner(),
                )),
                marginal_portfolio_value_usd: ChUsd::from(economics.marginal_portfolio_value_usd),
                suggested_usd: ChUsd::from(recommendation.trade_plan.sizing.suggested_usd),
                valid_until: recommendation.valid_until.timestamp_millis(),
            })
        })
        .collect()
}

fn report_notification(
    report_id: &RecommendationReportId,
    runtime_mode: QuantRuntimeMode,
    summary: &ReportSummary,
    recommendations: &[NewRecommendation],
    empty_reason: Option<EmptyReportReason>,
) -> QuantResult<ReportNotificationPayload> {
    Ok(ReportNotificationPayload {
        report_id: *report_id,
        kind: ReportKind::TopN,
        status: RecommendationReportStatus::Published.to_string(),
        runtime_mode,
        published_count: count_u32(recommendations.len(), "notification count")?,
        total_suggested_usd: summary.total_suggested_usd,
        top3: recommendations
            .iter()
            .take(3)
            .map(|recommendation| NotificationRecommendation {
                market_id: recommendation.market_id.to_string(),
                outcome_side: recommendation.outcome_side,
                route: recommendation.route,
                profit_probability_bps: recommendation.economics_json.profit_probability_bps,
                robust_expected_net_usd: recommendation.economics_json.robust_expected_net_usd,
                marginal_portfolio_value_usd: recommendation
                    .economics_json
                    .marginal_portfolio_value_usd,
                suggested_usd: recommendation.trade_plan.sizing.suggested_usd,
            })
            .collect(),
        warnings: summary.warnings.clone(),
        empty_reason,
    })
}

fn operation_log(
    report_id: &RecommendationReportId,
    input: &ComposeReportInput<'_>,
    report: &NewRecommendationReport,
) -> QuantResult<NewOperationLog> {
    Ok(NewOperationLog {
        id: OperationLogId::from_v7(),
        request_id: input.trigger_key.clone().into(),
        actor_user_id: None,
        actor_username: Some("system".to_owned()),
        acting_role: Some("report_lifecycle".into()),
        category: OperationCategory::QuantReport,
        action: "prepare".into(),
        resource_type: Some(ResourceType::QuantReport),
        resource_id: Some(report_id.to_string()),
        http_method: OperationHttpMethod::System,
        http_path: "/system/quant/report".to_owned(),
        http_status: 201,
        outcome: OperationOutcome::Success,
        client_ip: None,
        user_agent: None,
        latency_ms: 0,
        detail: OperationDetailDocument::from_serializable(&serde_json::json!({
            "trigger_key": input.trigger_key,
            "trigger_kind": input.trigger.kind().as_str(),
            "decision_at": input.decision_at.to_rfc3339(),
            "prepared_at": input.published_at.to_rfc3339(),
            "represented_routes": input.portfolio_plan.represented_routes_json.routes,
            "scenario_artifact_id": input.portfolio_plan.scenario_artifact_id,
            "candidate_count": input.candidate_count,
            "economic_tier_count": input.tiers.len(),
            "published_count": input.planned.len(),
            "empty_reason": input.empty.as_ref().map(|empty| empty.reason.as_str()),
        }))
        .map_err(|error| InfraError::AuditDetailInvalid {
            detail: error.to_string(),
        })?,
        before_hash: None,
        after_hash: Some(canonical_state_hash(report).map_err(|error| {
            QuantError::config(format!("canonical state hash failed: {error}"))
        })?),
        governance_audit_event_id: None,
        governance_audit_sequence: None,
    })
}

fn entry_window_secs(horizon_secs: u64, ratio: Decimal) -> QuantResult<u64> {
    if horizon_secs == 0 || ratio <= Decimal::ZERO || ratio > Decimal::ONE {
        return Err(QuantError::config(
            "prediction horizon and reports.entry_window_ratio must be positive; ratio must not exceed one",
        ));
    }
    (Decimal::from(horizon_secs) * ratio)
        .round()
        .to_u64()
        .filter(|seconds| *seconds > 0)
        .ok_or_else(|| {
            ReportError::NumericOverflow {
                field: "recommendation.entry_window_secs",
                detail: "entry window does not fit u64".to_owned(),
            }
            .into()
        })
}

fn actionable_valid_until(
    decision_at: DateTime<Utc>,
    published_at: DateTime<Utc>,
    seconds: u64,
    market_id: &MarketId,
) -> QuantResult<DateTime<Utc>> {
    let valid_until = instant_plus_secs(decision_at, seconds)?;
    if valid_until <= published_at {
        return Err(ReportError::InvariantViolation {
            stage: "global_recommendation_compose",
            detail: format!("market {market_id} has no actionable entry window at publication"),
        }
        .into());
    }
    Ok(valid_until)
}

fn instant_plus_secs(instant: DateTime<Utc>, seconds: u64) -> QuantResult<DateTime<Utc>> {
    let seconds = i64::try_from(seconds).map_err(|error| ReportError::NumericOverflow {
        field: "time.seconds",
        detail: error.to_string(),
    })?;
    instant
        .checked_add_signed(Duration::seconds(seconds))
        .ok_or_else(|| QuantError::config("timestamp is outside chrono range"))
}

fn count_u32(count: usize, field: &'static str) -> QuantResult<u32> {
    u32::try_from(count)
        .map_err(|error| ReportError::NumericOverflow {
            field,
            detail: error.to_string(),
        })
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::{entry_window_secs, tick_aligned_price};
    use quant_pivot_models::enums::common::TickSize;

    #[test]
    fn entry_window_no_fallback() {
        assert_eq!(entry_window_secs(3_600, dec!(0.5)).expect("window"), 1_800);
        assert!(entry_window_secs(0, dec!(0.5)).is_err());
    }

    #[test]
    fn exit_price_rounds_conservatively() {
        let price = tick_aligned_price(dec!(0.501), TickSize::Hundredth);
        assert_eq!(price.inner(), dec!(0.51));
    }
}
