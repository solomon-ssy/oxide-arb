//! Point-in-time feedback-cohort eligibility.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::quant::{
        FeedbackCohortContractError, FeedbackCohortDecision, FeedbackCohortEvidence,
        FeedbackCohortSnapshot, FeedbackExecutionEvidence, FeedbackExecutionState,
        FeedbackRecommendationContext, FeedbackResolutionEvidence,
        RecommendationExecutionRollupInfo, RecommendationResolutionOutcomeInfo,
    },
    enums::quant::{
        CohortCensorReason, CohortExclusionReason, FeedbackCohort, QuantRuntimeMode, ReportKind,
    },
};

/// Classify one recommendation without conflating market, execution, and policy truth.
///
/// Facts newer than the frozen cutoff are treated as absent. Visible facts are
/// hash-verified and identity-checked; corruption is an error rather than an
/// exclusion reason. Each cohort validates only the evidence it consumes, so
/// an execution-plane fault cannot silently remove a valid model label.
pub fn evaluate_feedback_cohort(
    cohort: FeedbackCohort,
    snapshot: &FeedbackCohortSnapshot,
    context: &FeedbackRecommendationContext,
    resolution_outcome: Option<&RecommendationResolutionOutcomeInfo>,
    execution_rollup: Option<&RecommendationExecutionRollupInfo>,
) -> Result<FeedbackCohortDecision, FeedbackCohortContractError> {
    let window = snapshot.decision_window();
    if context.profile_ref() != window.profile_ref() {
        return Err(FeedbackCohortContractError::FrozenProfileMismatch);
    }
    let Some(published_at) = context.published_at() else {
        return Ok(FeedbackCohortDecision::Excluded(
            CohortExclusionReason::RecommendationNotPublished,
        ));
    };
    if published_at > snapshot.truth_cutoff() || context.available_at() > snapshot.truth_cutoff() {
        return Ok(FeedbackCohortDecision::Excluded(
            CohortExclusionReason::RecommendationNotPublished,
        ));
    }
    if context.decision_at() < window.window_start() || context.decision_at() > window.cutoff() {
        return Ok(FeedbackCohortDecision::Excluded(
            CohortExclusionReason::OutsideFrozenWindow,
        ));
    }
    if context.report_kind() != ReportKind::TopN {
        return Ok(FeedbackCohortDecision::Excluded(
            CohortExclusionReason::NonPrimaryReport,
        ));
    }

    match cohort {
        FeedbackCohort::ModelLearning => {
            evaluate_model_learning(snapshot, context, resolution_outcome)
        }
        FeedbackCohort::ModelScoreLearning => {
            Err(FeedbackCohortContractError::InvalidCandidateTruthPlane { cohort })
        }
        FeedbackCohort::ExecutionLearning => {
            evaluate_execution_learning(snapshot, context, published_at, execution_rollup)
        }
        FeedbackCohort::PolicyEvaluation => evaluate_policy(
            snapshot,
            context,
            published_at,
            resolution_outcome,
            execution_rollup,
        ),
    }
}

fn evaluate_model_learning(
    snapshot: &FeedbackCohortSnapshot,
    context: &FeedbackRecommendationContext,
    resolution_outcome: Option<&RecommendationResolutionOutcomeInfo>,
) -> Result<FeedbackCohortDecision, FeedbackCohortContractError> {
    let Some(evidence) = visible_resolution(snapshot, context, resolution_outcome)? else {
        return Ok(FeedbackCohortDecision::Censored(
            CohortCensorReason::ResolutionUnavailableAtCutoff,
        ));
    };
    Ok(FeedbackCohortDecision::Eligible(
        FeedbackCohortEvidence::ModelLearning(evidence),
    ))
}

fn evaluate_execution_learning(
    snapshot: &FeedbackCohortSnapshot,
    context: &FeedbackRecommendationContext,
    published_at: DateTime<Utc>,
    execution_rollup: Option<&RecommendationExecutionRollupInfo>,
) -> Result<FeedbackCohortDecision, FeedbackCohortContractError> {
    if context.runtime_mode() == QuantRuntimeMode::ReportOnly {
        return Ok(FeedbackCohortDecision::Excluded(
            CohortExclusionReason::ReportOnlyNoExecutionAuthority,
        ));
    }
    let Some(evidence) = visible_execution(snapshot, context, published_at, execution_rollup)?
    else {
        return Ok(FeedbackCohortDecision::Censored(
            CohortCensorReason::ExecutionOutcomeUnavailableAtCutoff,
        ));
    };
    if evidence.attempt_count == 0 {
        return Ok(FeedbackCohortDecision::Excluded(
            CohortExclusionReason::ExecutionNotAttempted,
        ));
    }
    Ok(FeedbackCohortDecision::Eligible(
        FeedbackCohortEvidence::ExecutionLearning(evidence),
    ))
}

fn evaluate_policy(
    snapshot: &FeedbackCohortSnapshot,
    context: &FeedbackRecommendationContext,
    published_at: DateTime<Utc>,
    resolution_outcome: Option<&RecommendationResolutionOutcomeInfo>,
    execution_rollup: Option<&RecommendationExecutionRollupInfo>,
) -> Result<FeedbackCohortDecision, FeedbackCohortContractError> {
    let resolution = visible_resolution(snapshot, context, resolution_outcome)?;
    let execution = visible_execution(snapshot, context, published_at, execution_rollup)?;
    let execution_state = execution.as_ref().map(execution_state).transpose()?;
    Ok(FeedbackCohortDecision::Eligible(
        FeedbackCohortEvidence::PolicyEvaluation {
            execution_state,
            resolution_outcome_hash: resolution.map(|evidence| evidence.outcome_hash),
            execution_rollup_hash: execution.map(|evidence| evidence.rollup_hash),
        },
    ))
}

fn visible_resolution(
    snapshot: &FeedbackCohortSnapshot,
    context: &FeedbackRecommendationContext,
    outcome: Option<&RecommendationResolutionOutcomeInfo>,
) -> Result<Option<FeedbackResolutionEvidence>, FeedbackCohortContractError> {
    let Some(outcome) = outcome.filter(|outcome| outcome.available_at <= snapshot.truth_cutoff())
    else {
        return Ok(None);
    };
    outcome
        .validate()
        .map_err(FeedbackCohortContractError::InvalidResolutionOutcome)?;
    if outcome.recommendation_id != context.recommendation_id() {
        return Err(FeedbackCohortContractError::ResolutionRecommendationMismatch);
    }
    if &outcome.market_id != context.market_id() {
        return Err(FeedbackCohortContractError::ResolutionMarketMismatch);
    }
    if &outcome.token_id != context.token_id() {
        return Err(FeedbackCohortContractError::ResolutionTokenMismatch);
    }
    if outcome.source_observed_at <= context.decision_at() {
        return Err(FeedbackCohortContractError::ResolutionNotForwardLooking);
    }
    Ok(Some(FeedbackResolutionEvidence {
        resolution_kind: outcome.resolution_kind,
        token_payout_ratio: outcome.token_payout_ratio,
        resolved_at: outcome.resolved_at,
        available_at: outcome.available_at,
        outcome_hash: outcome.outcome_hash,
    }))
}

fn visible_execution(
    snapshot: &FeedbackCohortSnapshot,
    context: &FeedbackRecommendationContext,
    published_at: DateTime<Utc>,
    rollup: Option<&RecommendationExecutionRollupInfo>,
) -> Result<Option<FeedbackExecutionEvidence>, FeedbackCohortContractError> {
    let Some(rollup) = rollup.filter(|rollup| rollup.available_at <= snapshot.truth_cutoff())
    else {
        return Ok(None);
    };
    rollup
        .validate()
        .map_err(FeedbackCohortContractError::InvalidExecutionRollup)?;
    if rollup.recommendation_id != context.recommendation_id() {
        return Err(FeedbackCohortContractError::ExecutionRecommendationMismatch);
    }
    if rollup.terminal_at < published_at {
        return Err(FeedbackCohortContractError::ExecutionTerminalBeforePublication);
    }
    if context.runtime_mode() == QuantRuntimeMode::ReportOnly && rollup.attempt_count > 0 {
        return Err(FeedbackCohortContractError::ReportOnlyExecutionAttempt);
    }
    Ok(Some(FeedbackExecutionEvidence {
        intent_count: execution_count(rollup.intent_count)?,
        attempt_count: execution_count(rollup.attempt_count)?,
        unfilled_attempt_count: execution_count(rollup.unfilled_attempt_count)?,
        partially_filled_attempt_count: execution_count(rollup.partially_filled_attempt_count)?,
        fully_filled_attempt_count: execution_count(rollup.fully_filled_attempt_count)?,
        total_requested_shares: rollup.total_requested_shares,
        total_filled_shares: rollup.total_filled_shares,
        total_realized_pnl_usd: rollup.total_realized_pnl_usd,
        first_attempt_terminal_at: rollup.first_attempt_terminal_at,
        last_attempt_terminal_at: rollup.last_attempt_terminal_at,
        available_at: rollup.available_at,
        rollup_hash: rollup.rollup_hash,
    }))
}

fn execution_count(value: i32) -> Result<u32, FeedbackCohortContractError> {
    u32::try_from(value).map_err(|_| FeedbackCohortContractError::ExecutionCountOverflow)
}

fn execution_state(
    evidence: &FeedbackExecutionEvidence,
) -> Result<FeedbackExecutionState, FeedbackCohortContractError> {
    if evidence.attempt_count == 0 {
        return Ok(FeedbackExecutionState::NotAttempted);
    }
    let first_attempt_terminal_at = evidence
        .first_attempt_terminal_at
        .ok_or(FeedbackCohortContractError::ExecutionCountOverflow)?;
    let last_attempt_terminal_at = evidence
        .last_attempt_terminal_at
        .ok_or(FeedbackCohortContractError::ExecutionCountOverflow)?;
    Ok(FeedbackExecutionState::Attempted {
        attempt_count: evidence.attempt_count,
        first_attempt_terminal_at,
        last_attempt_terminal_at,
    })
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, Utc};
    use quant_pivot_models::{
        domain::quant::{
            ExecutionAttemptOutcomeInfo, FeedbackCohortContractError, FeedbackCohortDecision,
            FeedbackCohortEvidence, FeedbackCohortSnapshot, FeedbackCohortWindow,
            FeedbackRecommendationContext, NewExecutionAttemptOutcome,
            NewRecommendationExecutionRollup, NewRecommendationResolutionOutcome,
            RecommendationExecutionRollupInfo, RecommendationInfo, RecommendationReportInfo,
            RecommendationResolutionOutcomeInfo, ReportRouteRunInfo, RouteCandidateFunnel,
            RouteModelLineage, RouteRunOutcome,
        },
        enums::quant::{
            CohortCensorReason, CohortExclusionReason, ExecutionAttemptNoFillReason,
            ExecutionAttemptTerminalState, ExecutionOrderState, FeedbackCohort, OutcomeSide,
            QuantRuntimeMode, RecommendationReportStatus, RecommendationResolutionKind,
            RecommendationStatus, ReportKind,
        },
        types::{
            CalibrationArtifactId, ContentHash, ExecutionAccountId, ExecutionOrderId,
            OrderIntentId, PayoutRatio, RecommendationId, RecommendationReportId, ReconciliationId,
            SchemaVersion, Shares, TradePolicyArtifactId, Usd,
        },
    };
    use rust_decimal_macros::dec;

    use super::evaluate_feedback_cohort as evaluate_snapshot;
    use crate::test_fixtures::{
        execution_pg_seed::fixture_profile_ref,
        report_fixtures::{recommendation, report},
    };

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64)))
            .expect("valid content hash")
    }

    fn evaluate_feedback_cohort(
        cohort: FeedbackCohort,
        window: &FeedbackCohortWindow,
        context: &FeedbackRecommendationContext,
        resolution_outcome: Option<&RecommendationResolutionOutcomeInfo>,
        execution_rollup: Option<&RecommendationExecutionRollupInfo>,
    ) -> Result<FeedbackCohortDecision, FeedbackCohortContractError> {
        let snapshot = FeedbackCohortSnapshot::try_new(window.clone(), window.cutoff())?;
        evaluate_snapshot(
            cohort,
            &snapshot,
            context,
            resolution_outcome,
            execution_rollup,
        )
    }

    fn recommendation_and_report(
        runtime_mode: QuantRuntimeMode,
    ) -> (
        RecommendationInfo,
        RecommendationReportInfo,
        ReportRouteRunInfo,
    ) {
        let report_id = RecommendationReportId::from_v7();
        let recommendation_id = RecommendationId::from_v7();
        let recommendation = recommendation(
            report_id,
            recommendation_id,
            1,
            "feedback-market",
            OutcomeSide::Yes,
            Usd::new(dec!(100)),
        );
        let mut report = report(
            report_id,
            ReportKind::TopN,
            RecommendationReportStatus::Published,
        );
        report.runtime_mode = runtime_mode;
        report.market_selection_id = recommendation.evidence_refs.market_selection_id;
        report.decision_policy_snapshot_id =
            recommendation.evidence_refs.decision_policy_snapshot_id;
        report.data_quality_snapshot_ref = recommendation.evidence_refs.data_quality_snapshot_ref;
        let profile_ref = fixture_profile_ref();
        let calibration_artifact_id = CalibrationArtifactId::from_v7();
        let trade_policy_artifact_id = TradePolicyArtifactId::from_content_hash(&hash('7'));
        let lineage = RouteModelLineage {
            model_version_id: recommendation.evidence_refs.model_version_id,
            model_run_id: Some(recommendation.evidence_refs.model_run_id),
            calibration_artifact_id,
            trade_policy_artifact_id,
            research_profile_artifact_id: profile_ref.artifact_id(),
            research_profile_ref: profile_ref,
            prediction_horizon_secs: 86_400,
            feature_contract_digest: hash('f'),
            pit_lineage_digest: hash('8'),
            serving_contract_digest: hash('9'),
        };
        let route_run = ReportRouteRunInfo {
            report_route_run_id: recommendation.report_route_run_id,
            report_run_id: report.report_run_id,
            route: recommendation.route,
            outcome: RouteRunOutcome::Ready,
            model_version_id: Some(lineage.model_version_id),
            model_run_id: lineage.model_run_id,
            calibration_artifact_id: Some(calibration_artifact_id),
            trade_policy_artifact_id: Some(trade_policy_artifact_id),
            research_profile_artifact_id: Some(lineage.research_profile_artifact_id.clone()),
            lineage_json: Some(lineage),
            funnel_json: RouteCandidateFunnel {
                eligible_markets: 1,
                feature_complete_markets: 1,
                calibrated_candidates: 1,
                admitted_economic_tiers: 1,
                selected_recommendations: 1,
            },
            diagnostic_code: None,
            finished_at: report.decision_at,
            created_at: report.created_at,
        };
        (recommendation, report, route_run)
    }

    fn context(runtime_mode: QuantRuntimeMode) -> FeedbackRecommendationContext {
        let (recommendation, report, route_run) = recommendation_and_report(runtime_mode);
        FeedbackRecommendationContext::try_from_report(&recommendation, &report, &route_run)
            .expect("coherent feedback recommendation")
    }

    fn window(context: &FeedbackRecommendationContext) -> FeedbackCohortWindow {
        FeedbackCohortWindow::try_new(
            context.profile_ref().clone(),
            context.decision_at() - Duration::minutes(1),
            context.published_at().expect("published") + Duration::days(1),
        )
        .expect("valid cohort window")
    }

    fn resolution_outcome(
        context: &FeedbackRecommendationContext,
        available_at: DateTime<Utc>,
    ) -> RecommendationResolutionOutcomeInfo {
        let resolved_at = context.decision_at() + Duration::hours(1);
        let source_observed_at = resolved_at + Duration::seconds(1);
        let candidate = NewRecommendationResolutionOutcome {
            recommendation_id: context.recommendation_id(),
            market_id: context.market_id().clone(),
            token_id: context.token_id().clone(),
            resolution_kind: RecommendationResolutionKind::SplitPayout,
            token_payout_ratio: PayoutRatio::try_new(dec!(0.5)).expect("split payout"),
            resolved_at,
            source_observed_at,
            source_checkpoint_hash: hash('a'),
            resolution_fact_hash: hash('b'),
            resolution_fact_log_index: 7,
            resolution_fact_schema_version: SchemaVersion::FIRST,
        };
        let outcome_hash = candidate
            .expected_outcome_hash(available_at)
            .expect("resolution outcome hash");
        RecommendationResolutionOutcomeInfo {
            recommendation_id: candidate.recommendation_id,
            market_id: candidate.market_id,
            token_id: candidate.token_id,
            resolution_kind: candidate.resolution_kind,
            token_payout_ratio: candidate.token_payout_ratio,
            resolved_at: candidate.resolved_at,
            source_observed_at: candidate.source_observed_at,
            available_at,
            source_checkpoint_hash: candidate.source_checkpoint_hash,
            resolution_fact_hash: candidate.resolution_fact_hash,
            resolution_fact_log_index: candidate.resolution_fact_log_index,
            resolution_fact_schema_version: candidate.resolution_fact_schema_version,
            outcome_hash,
            created_at: available_at,
        }
    }

    fn execution_rollup(
        context: &FeedbackRecommendationContext,
        available_at: DateTime<Utc>,
    ) -> RecommendationExecutionRollupInfo {
        let order_intent_id = OrderIntentId::from_v7();
        let entry_execution_order_id = ExecutionOrderId::from_v7();
        let submitted_at = context.decision_at() + Duration::minutes(5);
        let terminal_at = submitted_at + Duration::minutes(10);
        let source_observed_at = available_at;
        let candidate = NewExecutionAttemptOutcome {
            recommendation_id: context.recommendation_id(),
            order_intent_id,
            entry_execution_order_id,
            entry_reconciliation_id: ReconciliationId::from_v7(),
            position_id: None,
            execution_account_id: ExecutionAccountId::from_v7(),
            market_id: context.market_id().clone(),
            token_id: context.token_id().clone(),
            runtime_mode: context.runtime_mode(),
            terminal_state: ExecutionAttemptTerminalState::Unfilled,
            no_fill_reason: Some(ExecutionAttemptNoFillReason::VenueExpired),
            entry_order_state: ExecutionOrderState::Cancelled,
            requested_shares: Shares::new(dec!(10)),
            filled_shares: Shares::ZERO,
            entry_avg_price: None,
            entry_fee_usd: Some(Usd::ZERO),
            entry_filled_at: None,
            position_terminal_state: None,
            exit_reason: None,
            exit_filled_shares: None,
            exit_avg_price: None,
            exit_fee_usd: None,
            exit_at: None,
            settlement_payout_usd: None,
            realized_pnl_usd: None,
            terminal_at,
            source_checkpoint_hash: hash('c'),
            execution_fact_hash: hash('d'),
            execution_fact_schema_version: SchemaVersion::FIRST,
        };
        let outcome_hash = candidate
            .expected_outcome_hash(source_observed_at, available_at)
            .expect("execution outcome hash");
        let outcome = ExecutionAttemptOutcomeInfo {
            recommendation_id: candidate.recommendation_id,
            order_intent_id: candidate.order_intent_id,
            entry_execution_order_id: candidate.entry_execution_order_id,
            entry_reconciliation_id: candidate.entry_reconciliation_id,
            position_id: candidate.position_id,
            execution_account_id: candidate.execution_account_id,
            market_id: candidate.market_id,
            token_id: candidate.token_id,
            runtime_mode: candidate.runtime_mode,
            terminal_state: candidate.terminal_state,
            no_fill_reason: candidate.no_fill_reason,
            entry_order_state: candidate.entry_order_state,
            requested_shares: candidate.requested_shares,
            filled_shares: candidate.filled_shares,
            entry_avg_price: candidate.entry_avg_price,
            entry_fee_usd: candidate.entry_fee_usd,
            entry_filled_at: candidate.entry_filled_at,
            position_terminal_state: candidate.position_terminal_state,
            exit_reason: candidate.exit_reason,
            exit_filled_shares: candidate.exit_filled_shares,
            exit_avg_price: candidate.exit_avg_price,
            exit_fee_usd: candidate.exit_fee_usd,
            exit_at: candidate.exit_at,
            settlement_payout_usd: candidate.settlement_payout_usd,
            realized_pnl_usd: candidate.realized_pnl_usd,
            terminal_at: candidate.terminal_at,
            source_observed_at,
            available_at,
            source_checkpoint_hash: candidate.source_checkpoint_hash,
            execution_fact_hash: candidate.execution_fact_hash,
            execution_fact_schema_version: candidate.execution_fact_schema_version,
            outcome_hash,
            created_at: available_at,
        };
        let seal = NewRecommendationExecutionRollup::aggregate(
            context.recommendation_id(),
            1,
            terminal_at,
            source_observed_at,
            vec![outcome],
        )
        .expect("aggregate execution rollup");
        let rollup = seal.rollup;
        let rollup_hash = rollup
            .expected_rollup_hash(available_at)
            .expect("execution rollup hash");
        RecommendationExecutionRollupInfo {
            recommendation_id: rollup.recommendation_id,
            intent_count: rollup.intent_count,
            attempt_count: rollup.attempt_count,
            unfilled_attempt_count: rollup.unfilled_attempt_count,
            partially_filled_attempt_count: rollup.partially_filled_attempt_count,
            fully_filled_attempt_count: rollup.fully_filled_attempt_count,
            total_requested_shares: rollup.total_requested_shares,
            total_filled_shares: rollup.total_filled_shares,
            total_entry_fee_usd: rollup.total_entry_fee_usd,
            total_exit_fee_usd: rollup.total_exit_fee_usd,
            total_settlement_payout_usd: rollup.total_settlement_payout_usd,
            total_realized_pnl_usd: rollup.total_realized_pnl_usd,
            first_attempt_terminal_at: rollup.first_attempt_terminal_at,
            last_attempt_terminal_at: rollup.last_attempt_terminal_at,
            terminal_at: rollup.terminal_at,
            source_observed_at: rollup.source_observed_at,
            available_at,
            attempt_set_hash: rollup.attempt_set_hash,
            rollup_hash,
            created_at: available_at,
        }
    }

    fn empty_execution_rollup(
        context: &FeedbackRecommendationContext,
        available_at: DateTime<Utc>,
    ) -> RecommendationExecutionRollupInfo {
        let terminal_at = context.published_at().expect("published") + Duration::minutes(1);
        let seal = NewRecommendationExecutionRollup::aggregate(
            context.recommendation_id(),
            0,
            terminal_at,
            terminal_at,
            Vec::new(),
        )
        .expect("aggregate empty execution rollup");
        let rollup = seal.rollup;
        let rollup_hash = rollup
            .expected_rollup_hash(available_at)
            .expect("empty execution rollup hash");
        RecommendationExecutionRollupInfo {
            recommendation_id: rollup.recommendation_id,
            intent_count: rollup.intent_count,
            attempt_count: rollup.attempt_count,
            unfilled_attempt_count: rollup.unfilled_attempt_count,
            partially_filled_attempt_count: rollup.partially_filled_attempt_count,
            fully_filled_attempt_count: rollup.fully_filled_attempt_count,
            total_requested_shares: rollup.total_requested_shares,
            total_filled_shares: rollup.total_filled_shares,
            total_entry_fee_usd: rollup.total_entry_fee_usd,
            total_exit_fee_usd: rollup.total_exit_fee_usd,
            total_settlement_payout_usd: rollup.total_settlement_payout_usd,
            total_realized_pnl_usd: rollup.total_realized_pnl_usd,
            first_attempt_terminal_at: rollup.first_attempt_terminal_at,
            last_attempt_terminal_at: rollup.last_attempt_terminal_at,
            terminal_at: rollup.terminal_at,
            source_observed_at: rollup.source_observed_at,
            available_at,
            attempt_set_hash: rollup.attempt_set_hash,
            rollup_hash,
            created_at: available_at,
        }
    }

    #[test]
    fn cohort_matrix_keeps_orthogonal() {
        let context = context(QuantRuntimeMode::SemiAuto);
        let window = window(&context);
        let resolution = resolution_outcome(&context, window.cutoff() - Duration::minutes(2));
        let execution = execution_rollup(&context, window.cutoff() - Duration::minutes(1));

        let model = evaluate_feedback_cohort(
            FeedbackCohort::ModelLearning,
            &window,
            &context,
            Some(&resolution),
            Some(&execution),
        )
        .expect("model cohort");
        assert!(matches!(
            model,
            FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ModelLearning(
                ref evidence
            )) if evidence.token_payout_ratio == PayoutRatio::try_new(dec!(0.5)).expect("ratio")
        ));
        assert_eq!(
            evaluate_feedback_cohort(
                FeedbackCohort::ModelLearning,
                &window,
                &context,
                None,
                Some(&execution),
            )
            .expect("unfilled execution cannot manufacture a model label"),
            FeedbackCohortDecision::Censored(CohortCensorReason::ResolutionUnavailableAtCutoff)
        );

        let execution_learning = evaluate_feedback_cohort(
            FeedbackCohort::ExecutionLearning,
            &window,
            &context,
            Some(&resolution),
            Some(&execution),
        )
        .expect("execution cohort");
        assert!(matches!(
            execution_learning,
            FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ExecutionLearning(
                ref evidence
            )) if evidence.unfilled_attempt_count == 1
                && evidence.total_filled_shares == Shares::ZERO
        ));

        let policy = evaluate_feedback_cohort(
            FeedbackCohort::PolicyEvaluation,
            &window,
            &context,
            Some(&resolution),
            Some(&execution),
        )
        .expect("policy cohort");
        assert!(matches!(
            policy,
            FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::PolicyEvaluation {
                resolution_outcome_hash: Some(_),
                execution_rollup_hash: Some(_),
                ..
            })
        ));
    }

    #[test]
    fn report_no_never_negatives() {
        let report_only = context(QuantRuntimeMode::ReportOnly);
        let report_only_window = window(&report_only);
        let report_only_decision = evaluate_feedback_cohort(
            FeedbackCohort::ExecutionLearning,
            &report_only_window,
            &report_only,
            None,
            None,
        )
        .expect("report-only classification");
        assert_eq!(
            report_only_decision,
            FeedbackCohortDecision::Excluded(CohortExclusionReason::ReportOnlyNoExecutionAuthority)
        );

        let semi_auto = context(QuantRuntimeMode::SemiAuto);
        let semi_auto_window = window(&semi_auto);
        let final_no_attempt =
            empty_execution_rollup(&semi_auto, semi_auto_window.cutoff() - Duration::minutes(1));
        let no_attempt = evaluate_feedback_cohort(
            FeedbackCohort::ExecutionLearning,
            &semi_auto_window,
            &semi_auto,
            None,
            Some(&final_no_attempt),
        )
        .expect("unattempted classification");
        assert_eq!(
            no_attempt,
            FeedbackCohortDecision::Excluded(CohortExclusionReason::ExecutionNotAttempted)
        );
    }

    #[test]
    fn missing_late_without_evaluation() {
        let context = context(QuantRuntimeMode::SemiAuto);
        let window = window(&context);
        let late_resolution = resolution_outcome(&context, window.cutoff() + Duration::minutes(1));
        let model = evaluate_feedback_cohort(
            FeedbackCohort::ModelLearning,
            &window,
            &context,
            Some(&late_resolution),
            None,
        )
        .expect("late resolution classification");
        assert_eq!(
            model,
            FeedbackCohortDecision::Censored(CohortCensorReason::ResolutionUnavailableAtCutoff)
        );

        let execution = evaluate_feedback_cohort(
            FeedbackCohort::ExecutionLearning,
            &window,
            &context,
            Some(&late_resolution),
            None,
        )
        .expect("pending execution classification");
        assert_eq!(
            execution,
            FeedbackCohortDecision::Censored(
                CohortCensorReason::ExecutionOutcomeUnavailableAtCutoff
            )
        );

        let policy = evaluate_feedback_cohort(
            FeedbackCohort::PolicyEvaluation,
            &window,
            &context,
            Some(&late_resolution),
            None,
        )
        .expect("policy remains observable");
        assert!(matches!(
            policy,
            FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::PolicyEvaluation {
                resolution_outcome_hash: None,
                execution_rollup_hash: None,
                ..
            })
        ));
    }

    #[test]
    fn truth_cutoff_follows_decision() {
        let context = context(QuantRuntimeMode::SemiAuto);
        let decision_cutoff = context.published_at().expect("published");
        let decision_window = FeedbackCohortWindow::try_new(
            context.profile_ref().clone(),
            context.decision_at() - Duration::minutes(1),
            decision_cutoff,
        )
        .expect("decision window");
        let truth_cutoff = decision_cutoff + Duration::hours(2);
        let snapshot = FeedbackCohortSnapshot::try_new(decision_window, truth_cutoff)
            .expect("later truth frontier");
        let resolution = resolution_outcome(&context, truth_cutoff - Duration::minutes(1));

        let decision = evaluate_snapshot(
            FeedbackCohort::ModelLearning,
            &snapshot,
            &context,
            Some(&resolution),
            None,
        )
        .expect("mature resolution after decision window");
        assert!(matches!(
            decision,
            FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ModelLearning(_))
        ));
    }

    #[test]
    fn publication_kind_window_boundaries() {
        let (mut recommendation, mut report, route_run) =
            recommendation_and_report(QuantRuntimeMode::SemiAuto);
        recommendation.status = RecommendationStatus::Prepared;
        report.status = RecommendationReportStatus::Prepared;
        report.published_at = None;
        let unpublished =
            FeedbackRecommendationContext::try_from_report(&recommendation, &report, &route_run)
                .expect("coherent unpublished recommendation");
        let unpublished_window = FeedbackCohortWindow::try_new(
            unpublished.profile_ref().clone(),
            unpublished.decision_at() - Duration::minutes(1),
            unpublished.decision_at() + Duration::days(1),
        )
        .expect("unpublished window");
        for cohort in [
            FeedbackCohort::ModelLearning,
            FeedbackCohort::ExecutionLearning,
            FeedbackCohort::PolicyEvaluation,
        ] {
            assert_eq!(
                evaluate_feedback_cohort(cohort, &unpublished_window, &unpublished, None, None,)
                    .expect("unpublished classification"),
                FeedbackCohortDecision::Excluded(CohortExclusionReason::RecommendationNotPublished)
            );
        }

        let (recommendation, mut report, route_run) =
            recommendation_and_report(QuantRuntimeMode::SemiAuto);
        report.report_kind = ReportKind::ShadowTopN;
        let shadow =
            FeedbackRecommendationContext::try_from_report(&recommendation, &report, &route_run)
                .expect("coherent shadow recommendation");
        assert_eq!(
            evaluate_feedback_cohort(
                FeedbackCohort::PolicyEvaluation,
                &window(&shadow),
                &shadow,
                None,
                None,
            )
            .expect("shadow classification"),
            FeedbackCohortDecision::Excluded(CohortExclusionReason::NonPrimaryReport)
        );

        let primary = context(QuantRuntimeMode::SemiAuto);
        let published_at = primary.published_at().expect("published");
        let window_start_after_decision = primary.decision_at() + Duration::seconds(1);
        assert!(window_start_after_decision < published_at);
        let outside_window = FeedbackCohortWindow::try_new(
            primary.profile_ref().clone(),
            window_start_after_decision,
            published_at + Duration::days(1),
        )
        .expect("outside window");
        assert_eq!(
            evaluate_feedback_cohort(
                FeedbackCohort::PolicyEvaluation,
                &outside_window,
                &primary,
                None,
                None,
            )
            .expect("outside classification"),
            FeedbackCohortDecision::Excluded(CohortExclusionReason::OutsideFrozenWindow)
        );

        let mut foreign_profile = primary.profile_ref().clone();
        foreign_profile.version += 1;
        let foreign_window = FeedbackCohortWindow::try_new(
            foreign_profile,
            primary.decision_at() - Duration::minutes(1),
            published_at + Duration::days(1),
        )
        .expect("foreign window");
        assert!(matches!(
            evaluate_feedback_cohort(
                FeedbackCohort::PolicyEvaluation,
                &foreign_window,
                &primary,
                None,
                None,
            ),
            Err(FeedbackCohortContractError::FrozenProfileMismatch)
        ));
    }

    #[test]
    fn visible_tamper_rejects_orthogonal() {
        let context = context(QuantRuntimeMode::SemiAuto);
        let window = window(&context);
        let resolution = resolution_outcome(&context, window.cutoff() - Duration::minutes(2));
        let execution = execution_rollup(&context, window.cutoff() - Duration::minutes(1));
        let mut corrupt_resolution = resolution.clone();
        corrupt_resolution.outcome_hash = hash('e');
        let mut corrupt_execution = execution.clone();
        corrupt_execution.rollup_hash = hash('f');

        assert!(matches!(
            evaluate_feedback_cohort(
                FeedbackCohort::ModelLearning,
                &window,
                &context,
                Some(&corrupt_resolution),
                Some(&execution),
            ),
            Err(FeedbackCohortContractError::InvalidResolutionOutcome(_))
        ));
        assert!(matches!(
            evaluate_feedback_cohort(
                FeedbackCohort::ExecutionLearning,
                &window,
                &context,
                Some(&resolution),
                Some(&corrupt_execution),
            ),
            Err(FeedbackCohortContractError::InvalidExecutionRollup(_))
        ));

        assert!(matches!(
            evaluate_feedback_cohort(
                FeedbackCohort::ModelLearning,
                &window,
                &context,
                Some(&resolution),
                Some(&corrupt_execution),
            )
            .expect("execution corruption is orthogonal to model truth"),
            FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ModelLearning(_))
        ));
        assert!(matches!(
            evaluate_feedback_cohort(
                FeedbackCohort::ExecutionLearning,
                &window,
                &context,
                Some(&corrupt_resolution),
                Some(&execution),
            )
            .expect("resolution corruption is orthogonal to execution truth"),
            FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ExecutionLearning(_))
        ));
    }

    #[test]
    fn final_rollup_governs_execution() {
        let semi_auto = context(QuantRuntimeMode::SemiAuto);
        let semi_auto_window = window(&semi_auto);
        let rollup = execution_rollup(&semi_auto, semi_auto_window.cutoff() - Duration::minutes(1));
        assert!(matches!(
            evaluate_feedback_cohort(
                FeedbackCohort::ExecutionLearning,
                &semi_auto_window,
                &semi_auto,
                None,
                Some(&rollup),
            )
            .expect("final rollup is the only execution authority"),
            FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ExecutionLearning(_))
        ));

        let report_only = context(QuantRuntimeMode::ReportOnly);
        let report_only_window = window(&report_only);
        let mut impossible_rollup = rollup;
        impossible_rollup.recommendation_id = report_only.recommendation_id();
        impossible_rollup.rollup_hash = impossible_rollup
            .as_new()
            .expected_rollup_hash(impossible_rollup.available_at)
            .expect("rebind impossible rollup hash");
        assert!(matches!(
            evaluate_feedback_cohort(
                FeedbackCohort::PolicyEvaluation,
                &report_only_window,
                &report_only,
                None,
                Some(&impossible_rollup),
            ),
            Err(FeedbackCohortContractError::ReportOnlyExecutionAttempt)
        ));
    }
}
