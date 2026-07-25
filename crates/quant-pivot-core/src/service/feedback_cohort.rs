//! Point-in-time feedback-cohort eligibility.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::quant::{
        FeedbackCohortContractError, FeedbackCohortDecision, FeedbackCohortEvidence,
        FeedbackCohortWindow, FeedbackExecutionAttempt, FeedbackExecutionEvidence,
        FeedbackRecommendationContext, FeedbackResolutionEvidence,
        RecommendationExecutionOutcomeInfo, RecommendationResolutionOutcomeInfo,
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
    window: &FeedbackCohortWindow,
    context: &FeedbackRecommendationContext,
    execution_attempt: FeedbackExecutionAttempt,
    resolution_outcome: Option<&RecommendationResolutionOutcomeInfo>,
    execution_outcome: Option<&RecommendationExecutionOutcomeInfo>,
) -> Result<FeedbackCohortDecision, FeedbackCohortContractError> {
    if context.profile_ref() != window.profile_ref() {
        return Err(FeedbackCohortContractError::FrozenProfileMismatch);
    }
    let Some(published_at) = context.published_at() else {
        return Ok(FeedbackCohortDecision::Excluded(
            CohortExclusionReason::RecommendationNotPublished,
        ));
    };
    if published_at > window.cutoff() {
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
            evaluate_model_learning(window, context, resolution_outcome)
        }
        FeedbackCohort::ExecutionLearning => evaluate_execution_learning(
            window,
            context,
            published_at,
            execution_attempt,
            execution_outcome,
        ),
        FeedbackCohort::PolicyEvaluation => evaluate_policy(
            window,
            context,
            published_at,
            execution_attempt,
            resolution_outcome,
            execution_outcome,
        ),
    }
}

fn evaluate_model_learning(
    window: &FeedbackCohortWindow,
    context: &FeedbackRecommendationContext,
    resolution_outcome: Option<&RecommendationResolutionOutcomeInfo>,
) -> Result<FeedbackCohortDecision, FeedbackCohortContractError> {
    let Some(evidence) = visible_resolution(window, context, resolution_outcome)? else {
        return Ok(FeedbackCohortDecision::Censored(
            CohortCensorReason::ResolutionUnavailableAtCutoff,
        ));
    };
    Ok(FeedbackCohortDecision::Eligible(
        FeedbackCohortEvidence::ModelLearning(evidence),
    ))
}

fn evaluate_execution_learning(
    window: &FeedbackCohortWindow,
    context: &FeedbackRecommendationContext,
    published_at: DateTime<Utc>,
    execution_attempt: FeedbackExecutionAttempt,
    execution_outcome: Option<&RecommendationExecutionOutcomeInfo>,
) -> Result<FeedbackCohortDecision, FeedbackCohortContractError> {
    let visible_attempt =
        visible_execution_attempt(window, context, published_at, execution_attempt)?;
    let visible_outcome = visible_execution(window, context, visible_attempt, execution_outcome)?;

    if context.runtime_mode() == QuantRuntimeMode::ReportOnly {
        return Ok(FeedbackCohortDecision::Excluded(
            CohortExclusionReason::ReportOnlyNoExecutionAuthority,
        ));
    }
    if visible_attempt == FeedbackExecutionAttempt::NotAttempted {
        return Ok(FeedbackCohortDecision::Excluded(
            CohortExclusionReason::ExecutionNotAttempted,
        ));
    }
    let Some(evidence) = visible_outcome else {
        return Ok(FeedbackCohortDecision::Censored(
            CohortCensorReason::ExecutionOutcomeUnavailableAtCutoff,
        ));
    };
    Ok(FeedbackCohortDecision::Eligible(
        FeedbackCohortEvidence::ExecutionLearning(evidence),
    ))
}

fn evaluate_policy(
    window: &FeedbackCohortWindow,
    context: &FeedbackRecommendationContext,
    published_at: DateTime<Utc>,
    execution_attempt: FeedbackExecutionAttempt,
    resolution_outcome: Option<&RecommendationResolutionOutcomeInfo>,
    execution_outcome: Option<&RecommendationExecutionOutcomeInfo>,
) -> Result<FeedbackCohortDecision, FeedbackCohortContractError> {
    let execution_attempt =
        visible_execution_attempt(window, context, published_at, execution_attempt)?;
    let resolution = visible_resolution(window, context, resolution_outcome)?;
    let execution = visible_execution(window, context, execution_attempt, execution_outcome)?;
    Ok(FeedbackCohortDecision::Eligible(
        FeedbackCohortEvidence::PolicyEvaluation {
            execution_attempt,
            resolution_outcome_hash: resolution.map(|evidence| evidence.outcome_hash),
            execution_outcome_hash: execution.map(|evidence| evidence.outcome_hash),
        },
    ))
}

fn visible_resolution(
    window: &FeedbackCohortWindow,
    context: &FeedbackRecommendationContext,
    outcome: Option<&RecommendationResolutionOutcomeInfo>,
) -> Result<Option<FeedbackResolutionEvidence>, FeedbackCohortContractError> {
    let Some(outcome) = outcome.filter(|outcome| outcome.available_at <= window.cutoff()) else {
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

fn visible_execution_attempt(
    window: &FeedbackCohortWindow,
    context: &FeedbackRecommendationContext,
    published_at: DateTime<Utc>,
    attempt: FeedbackExecutionAttempt,
) -> Result<FeedbackExecutionAttempt, FeedbackCohortContractError> {
    let FeedbackExecutionAttempt::Submitted { submitted_at, .. } = attempt else {
        return Ok(FeedbackExecutionAttempt::NotAttempted);
    };
    if submitted_at > window.cutoff() {
        return Ok(FeedbackExecutionAttempt::NotAttempted);
    }
    if submitted_at < published_at {
        return Err(FeedbackCohortContractError::ExecutionAttemptBeforePublication);
    }
    if context.runtime_mode() == QuantRuntimeMode::ReportOnly {
        return Err(FeedbackCohortContractError::ReportOnlyExecutionAttempt);
    }
    Ok(attempt)
}

fn visible_execution(
    window: &FeedbackCohortWindow,
    context: &FeedbackRecommendationContext,
    attempt: FeedbackExecutionAttempt,
    outcome: Option<&RecommendationExecutionOutcomeInfo>,
) -> Result<Option<FeedbackExecutionEvidence>, FeedbackCohortContractError> {
    let Some(outcome) = outcome.filter(|outcome| outcome.available_at <= window.cutoff()) else {
        return Ok(None);
    };
    outcome
        .validate()
        .map_err(FeedbackCohortContractError::InvalidExecutionOutcome)?;
    if outcome.recommendation_id != context.recommendation_id() {
        return Err(FeedbackCohortContractError::ExecutionRecommendationMismatch);
    }
    if &outcome.market_id != context.market_id() {
        return Err(FeedbackCohortContractError::ExecutionMarketMismatch);
    }
    if &outcome.token_id != context.token_id() {
        return Err(FeedbackCohortContractError::ExecutionTokenMismatch);
    }
    if outcome.runtime_mode != context.runtime_mode() {
        return Err(FeedbackCohortContractError::ExecutionRuntimeModeMismatch);
    }
    let FeedbackExecutionAttempt::Submitted {
        order_intent_id,
        entry_execution_order_id,
        submitted_at,
    } = attempt
    else {
        return Err(FeedbackCohortContractError::ExecutionOutcomeWithoutAttempt);
    };
    if outcome.order_intent_id != order_intent_id
        || outcome.entry_execution_order_id != entry_execution_order_id
    {
        return Err(FeedbackCohortContractError::ExecutionAttemptIdentityMismatch);
    }
    if outcome.terminal_at < submitted_at {
        return Err(FeedbackCohortContractError::ExecutionTerminalBeforeSubmission);
    }
    Ok(Some(FeedbackExecutionEvidence {
        order_intent_id: outcome.order_intent_id,
        entry_execution_order_id: outcome.entry_execution_order_id,
        terminal_state: outcome.terminal_state,
        no_fill_reason: outcome.no_fill_reason,
        requested_shares: outcome.requested_shares,
        filled_shares: outcome.filled_shares,
        available_at: outcome.available_at,
        outcome_hash: outcome.outcome_hash,
    }))
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, Utc};
    use quant_pivot_models::{
        domain::quant::{
            FeedbackCohortContractError, FeedbackCohortDecision, FeedbackCohortEvidence,
            FeedbackCohortWindow, FeedbackExecutionAttempt, FeedbackRecommendationContext,
            NewRecommendationExecutionOutcome, NewRecommendationResolutionOutcome,
            RecommendationExecutionOutcomeInfo, RecommendationInfo, RecommendationReportInfo,
            RecommendationResolutionOutcomeInfo,
        },
        enums::quant::{
            CohortCensorReason, CohortExclusionReason, ExecutionOrderState, FeedbackCohort,
            OutcomeSide, QuantRuntimeMode, RecommendationExecutionNoFillReason,
            RecommendationExecutionTerminalState, RecommendationReportStatus,
            RecommendationResolutionKind, RecommendationStatus, ReportKind,
        },
        types::{
            ContentHash, ExecutionAccountId, ExecutionOrderId, OrderIntentId, PayoutRatio,
            RecommendationId, RecommendationReportId, ReconciliationId, SchemaVersion, Shares, Usd,
        },
    };
    use rust_decimal_macros::dec;

    use super::evaluate_feedback_cohort;
    use crate::test_fixtures::report_fixtures::{recommendation, report};

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", seed.to_string().repeat(64)))
            .expect("valid content hash")
    }

    fn recommendation_and_report(
        runtime_mode: QuantRuntimeMode,
    ) -> (RecommendationInfo, RecommendationReportInfo) {
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
        report.model_run_id = Some(recommendation.evidence_refs.model_run_id);
        report.model_version_id = recommendation.evidence_refs.model_version_id;
        report.market_selection_id = recommendation.evidence_refs.market_selection_id;
        report.decision_policy_snapshot_id =
            recommendation.evidence_refs.decision_policy_snapshot_id;
        report.data_quality_snapshot_ref = recommendation.evidence_refs.data_quality_snapshot_ref;
        (recommendation, report)
    }

    fn context(runtime_mode: QuantRuntimeMode) -> FeedbackRecommendationContext {
        let (recommendation, report) = recommendation_and_report(runtime_mode);
        FeedbackRecommendationContext::try_from_report(&recommendation, &report)
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

    fn execution_outcome(
        context: &FeedbackRecommendationContext,
        available_at: DateTime<Utc>,
    ) -> (FeedbackExecutionAttempt, RecommendationExecutionOutcomeInfo) {
        let order_intent_id = OrderIntentId::from_v7();
        let entry_execution_order_id = ExecutionOrderId::from_v7();
        let submitted_at = context.decision_at() + Duration::minutes(5);
        let terminal_at = submitted_at + Duration::minutes(10);
        let source_observed_at = terminal_at + Duration::seconds(1);
        let candidate = NewRecommendationExecutionOutcome {
            recommendation_id: context.recommendation_id(),
            order_intent_id,
            entry_execution_order_id,
            entry_reconciliation_id: ReconciliationId::from_v7(),
            position_id: None,
            execution_account_id: ExecutionAccountId::from_v7(),
            market_id: context.market_id().clone(),
            token_id: context.token_id().clone(),
            runtime_mode: context.runtime_mode(),
            terminal_state: RecommendationExecutionTerminalState::Unfilled,
            no_fill_reason: Some(RecommendationExecutionNoFillReason::VenueExpired),
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
            max_adverse_excursion_bps: None,
            max_favorable_excursion_bps: None,
            terminal_at,
            source_checkpoint_hash: hash('c'),
            execution_fact_hash: hash('d'),
            execution_fact_schema_version: SchemaVersion::FIRST,
        };
        let outcome_hash = candidate
            .expected_outcome_hash(source_observed_at, available_at)
            .expect("execution outcome hash");
        let outcome = RecommendationExecutionOutcomeInfo {
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
            max_adverse_excursion_bps: candidate.max_adverse_excursion_bps,
            max_favorable_excursion_bps: candidate.max_favorable_excursion_bps,
            terminal_at: candidate.terminal_at,
            source_observed_at,
            available_at,
            source_checkpoint_hash: candidate.source_checkpoint_hash,
            execution_fact_hash: candidate.execution_fact_hash,
            execution_fact_schema_version: candidate.execution_fact_schema_version,
            outcome_hash,
            created_at: available_at,
        };
        (
            FeedbackExecutionAttempt::Submitted {
                order_intent_id,
                entry_execution_order_id,
                submitted_at,
            },
            outcome,
        )
    }

    #[test]
    fn cohort_matrix_keeps_orthogonal() {
        let context = context(QuantRuntimeMode::SemiAuto);
        let window = window(&context);
        let resolution = resolution_outcome(&context, window.cutoff() - Duration::minutes(2));
        let (attempt, execution) =
            execution_outcome(&context, window.cutoff() - Duration::minutes(1));

        let model = evaluate_feedback_cohort(
            FeedbackCohort::ModelLearning,
            &window,
            &context,
            attempt,
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
                attempt,
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
            attempt,
            Some(&resolution),
            Some(&execution),
        )
        .expect("execution cohort");
        assert!(matches!(
            execution_learning,
            FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ExecutionLearning(
                ref evidence
            )) if evidence.terminal_state == RecommendationExecutionTerminalState::Unfilled
                && evidence.filled_shares == Shares::ZERO
        ));

        let policy = evaluate_feedback_cohort(
            FeedbackCohort::PolicyEvaluation,
            &window,
            &context,
            attempt,
            Some(&resolution),
            Some(&execution),
        )
        .expect("policy cohort");
        assert!(matches!(
            policy,
            FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::PolicyEvaluation {
                resolution_outcome_hash: Some(_),
                execution_outcome_hash: Some(_),
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
            FeedbackExecutionAttempt::NotAttempted,
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
        let no_attempt = evaluate_feedback_cohort(
            FeedbackCohort::ExecutionLearning,
            &semi_auto_window,
            &semi_auto,
            FeedbackExecutionAttempt::NotAttempted,
            None,
            None,
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
        let attempt = FeedbackExecutionAttempt::Submitted {
            order_intent_id: OrderIntentId::from_v7(),
            entry_execution_order_id: ExecutionOrderId::from_v7(),
            submitted_at: context.decision_at() + Duration::minutes(5),
        };

        let model = evaluate_feedback_cohort(
            FeedbackCohort::ModelLearning,
            &window,
            &context,
            attempt,
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
            attempt,
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
            attempt,
            Some(&late_resolution),
            None,
        )
        .expect("policy remains observable");
        assert!(matches!(
            policy,
            FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::PolicyEvaluation {
                resolution_outcome_hash: None,
                execution_outcome_hash: None,
                ..
            })
        ));
    }

    #[test]
    fn publication_kind_window_boundaries() {
        let (mut recommendation, mut report) =
            recommendation_and_report(QuantRuntimeMode::SemiAuto);
        recommendation.status = RecommendationStatus::Prepared;
        report.status = RecommendationReportStatus::Prepared;
        report.published_at = None;
        let unpublished = FeedbackRecommendationContext::try_from_report(&recommendation, &report)
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
                evaluate_feedback_cohort(
                    cohort,
                    &unpublished_window,
                    &unpublished,
                    FeedbackExecutionAttempt::NotAttempted,
                    None,
                    None,
                )
                .expect("unpublished classification"),
                FeedbackCohortDecision::Excluded(CohortExclusionReason::RecommendationNotPublished)
            );
        }

        let (recommendation, mut report) = recommendation_and_report(QuantRuntimeMode::SemiAuto);
        report.report_kind = ReportKind::ShadowTopN;
        let shadow = FeedbackRecommendationContext::try_from_report(&recommendation, &report)
            .expect("coherent shadow recommendation");
        assert_eq!(
            evaluate_feedback_cohort(
                FeedbackCohort::PolicyEvaluation,
                &window(&shadow),
                &shadow,
                FeedbackExecutionAttempt::NotAttempted,
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
                FeedbackExecutionAttempt::NotAttempted,
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
                FeedbackExecutionAttempt::NotAttempted,
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
        let (attempt, execution) =
            execution_outcome(&context, window.cutoff() - Duration::minutes(1));
        let mut corrupt_resolution = resolution.clone();
        corrupt_resolution.outcome_hash = hash('e');
        let mut corrupt_execution = execution.clone();
        corrupt_execution.outcome_hash = hash('f');

        assert!(matches!(
            evaluate_feedback_cohort(
                FeedbackCohort::ModelLearning,
                &window,
                &context,
                attempt,
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
                attempt,
                Some(&resolution),
                Some(&corrupt_execution),
            ),
            Err(FeedbackCohortContractError::InvalidExecutionOutcome(_))
        ));

        assert!(matches!(
            evaluate_feedback_cohort(
                FeedbackCohort::ModelLearning,
                &window,
                &context,
                attempt,
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
                attempt,
                Some(&corrupt_resolution),
                Some(&execution),
            )
            .expect("resolution corruption is orthogonal to execution truth"),
            FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ExecutionLearning(_))
        ));
    }

    #[test]
    fn submitted_execution_evidence_rejects() {
        let semi_auto = context(QuantRuntimeMode::SemiAuto);
        let semi_auto_window = window(&semi_auto);
        let (_, outcome) =
            execution_outcome(&semi_auto, semi_auto_window.cutoff() - Duration::minutes(1));
        assert!(matches!(
            evaluate_feedback_cohort(
                FeedbackCohort::ExecutionLearning,
                &semi_auto_window,
                &semi_auto,
                FeedbackExecutionAttempt::NotAttempted,
                None,
                Some(&outcome),
            ),
            Err(FeedbackCohortContractError::ExecutionOutcomeWithoutAttempt)
        ));

        let report_only = context(QuantRuntimeMode::ReportOnly);
        let report_only_window = window(&report_only);
        let impossible_attempt = FeedbackExecutionAttempt::Submitted {
            order_intent_id: OrderIntentId::from_v7(),
            entry_execution_order_id: ExecutionOrderId::from_v7(),
            submitted_at: report_only.published_at().expect("published") + Duration::seconds(1),
        };
        assert!(matches!(
            evaluate_feedback_cohort(
                FeedbackCohort::ExecutionLearning,
                &report_only_window,
                &report_only,
                impossible_attempt,
                None,
                None,
            ),
            Err(FeedbackCohortContractError::ReportOnlyExecutionAttempt)
        ));
    }
}
