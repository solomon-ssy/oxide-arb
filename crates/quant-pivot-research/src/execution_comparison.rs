//! Deterministic planned-vs-actual execution comparison over existing truths.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::quant::{RecommendationEconomicOutcomeInfo, RecommendationEconomicStateDetail},
    hashing::CanonicalDigest,
    types::{Bps, ContentHash, Price, RecommendationId, Shares, Usd},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::attribution::{
    ActualExecutionBaseline, ExecutionTrajectoryArtifact, PolicyCounterfactualEvaluation,
    PolicyCounterfactualOutcome,
};

const COMPARISON_HASH_DOMAIN: &str = "quant-pivot/planned-actual-execution-comparison";
const COMPARISON_HASH_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionComparisonNotEvaluableReason {
    PlannedEntryUnavailable,
    PlannedEconomicsCensored,
    ActualBaselineUnavailable,
    IdentityMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum ExecutionComparisonEvaluation {
    Evaluated {
        #[serde(flatten)]
        metrics: Box<EvaluatedExecutionComparison>,
    },
    NotEvaluable {
        reason: ExecutionComparisonNotEvaluableReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluatedExecutionComparison {
    pub planned_entry_latency_ms: u64,
    pub actual_entry_latency_ms: u64,
    pub latency_delta_ms: i64,
    pub planned_entry_price: Price,
    pub actual_entry_price: Price,
    pub actual_vs_planned_price_bps: Bps,
    pub planned_fill_ratio: Decimal,
    pub actual_fill_ratio: Decimal,
    pub fill_ratio_delta: Decimal,
    pub planned_fee_usd: Usd,
    pub actual_fee_usd: Usd,
    pub fee_delta_usd: Decimal,
    pub planned_net_return_bps: Bps,
    pub actual_net_return_bps: Bps,
    pub return_delta_bps: Bps,
    pub policy_missed_return_bps: Option<Bps>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedActualExecutionComparison {
    pub recommendation_id: RecommendationId,
    pub economic_outcome_hash: ContentHash,
    pub trajectory_artifact_hash: ContentHash,
    pub policy_counterfactual_hash: ContentHash,
    pub evaluation: ExecutionComparisonEvaluation,
    pub comparison_hash: ContentHash,
}

pub struct PlannedActualExecutionComparisonInput<'a> {
    pub recommendation_id: RecommendationId,
    pub decision_at: DateTime<Utc>,
    pub requested_shares: Shares,
    pub economic_outcome: &'a RecommendationEconomicOutcomeInfo,
    pub trajectory: &'a ExecutionTrajectoryArtifact,
    pub trajectory_artifact_hash: ContentHash,
    pub counterfactual: &'a PolicyCounterfactualOutcome,
    pub policy_counterfactual_hash: ContentHash,
}

pub struct PlannedActualExecutionComparisonBuilder;

impl PlannedActualExecutionComparisonBuilder {
    pub fn build(
        input: &PlannedActualExecutionComparisonInput<'_>,
    ) -> QuantResult<PlannedActualExecutionComparison> {
        input
            .economic_outcome
            .verify()
            .map_err(|error| methodology(format!("economic outcome is invalid: {error}")))?;
        input.trajectory.validate()?;
        input.counterfactual.validate()?;
        let identity_matches = input.recommendation_id == input.economic_outcome.recommendation_id
            && input.recommendation_id == input.trajectory.recommendation_id
            && input.recommendation_id == input.counterfactual.recommendation_id
            && input.counterfactual.trajectory_artifact_hash == input.trajectory_artifact_hash;
        let evaluation = if identity_matches {
            Self::evaluate(input)?
        } else {
            ExecutionComparisonEvaluation::NotEvaluable {
                reason: ExecutionComparisonNotEvaluableReason::IdentityMismatch,
            }
        };
        let comparison_hash = CanonicalDigest::content_hash_typed(
            COMPARISON_HASH_DOMAIN,
            COMPARISON_HASH_VERSION,
            &(
                input.recommendation_id,
                input.economic_outcome.evidence_hash,
                input.trajectory_artifact_hash,
                input.policy_counterfactual_hash,
                &evaluation,
            ),
        )?;
        Ok(PlannedActualExecutionComparison {
            recommendation_id: input.recommendation_id,
            economic_outcome_hash: input.economic_outcome.evidence_hash,
            trajectory_artifact_hash: input.trajectory_artifact_hash,
            policy_counterfactual_hash: input.policy_counterfactual_hash,
            evaluation,
            comparison_hash,
        })
    }

    fn evaluate(
        input: &PlannedActualExecutionComparisonInput<'_>,
    ) -> QuantResult<ExecutionComparisonEvaluation> {
        let Some(planned_entry_at) =
            Self::planned_entry_at(&input.economic_outcome.payload_json.detail)
        else {
            return Ok(ExecutionComparisonEvaluation::NotEvaluable {
                reason: ExecutionComparisonNotEvaluableReason::PlannedEntryUnavailable,
            });
        };
        let amounts = &input.economic_outcome.payload_json.amounts;
        let Some(planned_return) = amounts.net_return_bps else {
            return Ok(ExecutionComparisonEvaluation::NotEvaluable {
                reason: ExecutionComparisonNotEvaluableReason::PlannedEconomicsCensored,
            });
        };
        if !amounts.entry_filled_shares.is_positive()
            || !input.requested_shares.is_positive()
            || planned_entry_at < input.decision_at
            || input.trajectory.entry_at < input.decision_at
        {
            return Err(methodology(
                "comparison entry timeline or shares are invalid",
            ));
        }
        let ActualExecutionBaseline::Evaluated {
            entry_fee_usd,
            exit_fee_usd,
            actual_net_return_bps,
            ..
        } = input.trajectory.actual_baseline
        else {
            return Ok(ExecutionComparisonEvaluation::NotEvaluable {
                reason: ExecutionComparisonNotEvaluableReason::ActualBaselineUnavailable,
            });
        };
        let planned_entry_price =
            Price::new(amounts.entry_cost_usd.inner() / amounts.entry_filled_shares.inner());
        let planned_latency = (planned_entry_at - input.decision_at).num_milliseconds();
        let actual_latency = (input.trajectory.entry_at - input.decision_at).num_milliseconds();
        let planned_entry_latency_ms = u64::try_from(planned_latency)
            .map_err(|error| methodology(format!("planned latency overflow: {error}")))?;
        let actual_entry_latency_ms = u64::try_from(actual_latency)
            .map_err(|error| methodology(format!("actual latency overflow: {error}")))?;
        let latency_delta_ms = actual_latency
            .checked_sub(planned_latency)
            .ok_or_else(|| methodology("latency delta overflow"))?;
        let planned_fill_ratio =
            (amounts.entry_filled_shares.inner() / input.requested_shares.inner()).round_dp(8);
        let actual_fill_ratio =
            (input.trajectory.entry_shares.inner() / input.requested_shares.inner()).round_dp(8);
        if planned_fill_ratio > Decimal::ONE || actual_fill_ratio > Decimal::ONE {
            return Err(methodology("comparison fill ratio exceeds one"));
        }
        let actual_fee_usd = entry_fee_usd + exit_fee_usd;
        let policy_missed_return_bps = match input.counterfactual.evaluation {
            PolicyCounterfactualEvaluation::Evaluated {
                missed_return_bps, ..
            } => Some(missed_return_bps),
            PolicyCounterfactualEvaluation::NotEvaluable { .. } => None,
        };
        Ok(ExecutionComparisonEvaluation::Evaluated {
            metrics: Box::new(EvaluatedExecutionComparison {
                planned_entry_latency_ms,
                actual_entry_latency_ms,
                latency_delta_ms,
                planned_entry_price,
                actual_entry_price: input.trajectory.entry_price,
                actual_vs_planned_price_bps: Bps::spread(
                    input.trajectory.entry_price,
                    planned_entry_price,
                )
                .ok_or_else(|| methodology("planned entry price is zero"))?,
                planned_fill_ratio,
                actual_fill_ratio,
                fill_ratio_delta: actual_fill_ratio - planned_fill_ratio,
                planned_fee_usd: amounts.execution_fee_usd,
                actual_fee_usd,
                fee_delta_usd: actual_fee_usd.inner() - amounts.execution_fee_usd.inner(),
                planned_net_return_bps: Bps::new(planned_return),
                actual_net_return_bps,
                return_delta_bps: actual_net_return_bps - Bps::new(planned_return),
                policy_missed_return_bps,
            }),
        })
    }

    const fn planned_entry_at(detail: &RecommendationEconomicStateDetail) -> Option<DateTime<Utc>> {
        match detail {
            RecommendationEconomicStateDetail::PolicyExited { entered_at, .. }
            | RecommendationEconomicStateDetail::HorizonLiquidated { entered_at, .. } => {
                Some(*entered_at)
            }
            RecommendationEconomicStateDetail::ResolvedBeforeHorizon { entered_at, .. } => {
                *entered_at
            }
            RecommendationEconomicStateDetail::EntryNotTriggered
            | RecommendationEconomicStateDetail::EntryUnfilled { .. }
            | RecommendationEconomicStateDetail::Censored { .. } => None,
        }
    }
}

fn methodology(detail: impl Into<String>) -> QuantError {
    ResearchError::ValidationMethodology {
        detail: detail.into(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::quant::{
            EconomicExitEvidenceKind, NewRecommendationEconomicOutcome,
            RecommendationEconomicAmounts, RecommendationEconomicEvidence,
            RecommendationEconomicOutcomeInfo, RecommendationEconomicOutcomeInput,
            RecommendationEconomicOutcomePayload, RecommendationEconomicStateDetail,
        },
        enums::quant::{AttributionCohort, RecommendationEconomicOutcomeState},
        types::{
            Bps, ContentHash, DecisionPolicySnapshotId, EconomicTierId, FeedbackCycleId,
            ModelVersionId, OrderIntentId, Price, RecommendationId, RecommendationReportId,
            ReportRouteRunId, ResearchProfileId, ResearchProfileRef, Shares, TradePolicyArtifactId,
            Usd,
        },
    };
    use rust_decimal_macros::dec;

    use super::{
        ExecutionComparisonEvaluation, PlannedActualExecutionComparisonBuilder,
        PlannedActualExecutionComparisonInput,
    };
    use crate::attribution::{
        ActualExecutionBaseline, AlternativeExitPolicy, AttributionLineage,
        ExecutionTrajectoryArtifact, ExecutionTrajectoryInput, PolicyCounterfactualOutcome,
    };

    fn hash(seed: u8) -> ContentHash {
        ContentHash::from_bytes([seed; 32])
    }

    #[test]
    fn compares_existing_truths() {
        let decision_at = Utc.timestamp_opt(1_800_000_000, 0).single().expect("time");
        let recommendation_id = RecommendationId::from_v7();
        let horizon_at = decision_at + Duration::hours(1);
        let profile = ResearchProfileRef {
            id: ResearchProfileId::new("comparison-test"),
            version: 1,
            content_hash: hash(1),
        }
        .artifact_id();
        let economic =
            NewRecommendationEconomicOutcome::try_seal(RecommendationEconomicOutcomeInput {
                recommendation_id,
                recommendation_report_id: RecommendationReportId::from_v7(),
                report_route_run_id: ReportRouteRunId::from_v7(),
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                economic_tier_id: EconomicTierId::from_v7(),
                model_version_id: ModelVersionId::from_v7(),
                trade_policy_artifact_id: TradePolicyArtifactId::from_v7(),
                research_profile_artifact_id: profile,
                state: RecommendationEconomicOutcomeState::HorizonLiquidated,
                decision_at,
                horizon_at,
                source_available_until: horizon_at,
                replay_kernel_version: "comparison-test".to_owned(),
                payload: RecommendationEconomicOutcomePayload {
                    detail: RecommendationEconomicStateDetail::HorizonLiquidated {
                        entered_at: decision_at + Duration::milliseconds(10),
                        liquidated_at: horizon_at,
                    },
                    amounts: RecommendationEconomicAmounts {
                        entry_filled_shares: Shares::new(dec!(80)),
                        exited_shares: Shares::new(dec!(80)),
                        entry_cost_usd: Usd::new(dec!(40)),
                        exit_proceeds_usd: Usd::new(dec!(42)),
                        resolution_payout_usd: Usd::ZERO,
                        execution_fee_usd: Usd::ZERO,
                        expected_maker_rebate_usd: Usd::ZERO,
                        net_pnl_usd: Some(Usd::new(dec!(2))),
                        net_return_bps: Some(dec!(500)),
                    },
                    evidence: RecommendationEconomicEvidence {
                        exit_evidence_kind: EconomicExitEvidenceKind::FullBidLadder,
                        full_l2_covered: true,
                        fee_covered: true,
                        passive_trade_covered: None,
                        replay_input_hash: hash(2),
                        replay_output_hash: hash(3),
                    },
                },
                available_at: horizon_at,
            })
            .expect("economic");
        let economic = RecommendationEconomicOutcomeInfo {
            recommendation_id: economic.recommendation_id,
            recommendation_report_id: economic.recommendation_report_id,
            report_route_run_id: economic.report_route_run_id,
            decision_policy_snapshot_id: economic.decision_policy_snapshot_id,
            economic_tier_id: economic.economic_tier_id,
            model_version_id: economic.model_version_id,
            trade_policy_artifact_id: economic.trade_policy_artifact_id,
            research_profile_artifact_id: economic.research_profile_artifact_id,
            state: economic.state,
            decision_at: economic.decision_at,
            horizon_at: economic.horizon_at,
            source_available_until: economic.source_available_until,
            replay_kernel_version: economic.replay_kernel_version,
            payload_json: economic.payload_json,
            evidence_hash: economic.evidence_hash,
            available_at: economic.available_at,
            created_at: economic.available_at,
        };
        let lineage = AttributionLineage::try_new(
            FeedbackCycleId::from_v7(),
            AttributionCohort::Evaluation,
            horizon_at,
            horizon_at,
            vec![hash(4)],
        )
        .expect("lineage");
        let trajectory = ExecutionTrajectoryArtifact::try_new(ExecutionTrajectoryInput {
            lineage,
            recommendation_id,
            order_intent_id: OrderIntentId::from_v7(),
            attempt_outcome_hash: hash(5),
            pit_book_contract_hash: hash(6),
            entry_at: decision_at + Duration::milliseconds(20),
            entry_shares: Shares::new(dec!(100)),
            entry_price: Price::new(dec!(0.5)),
            actual_baseline: ActualExecutionBaseline::Evaluated {
                entry_fee_usd: Usd::ZERO,
                exit_fee_usd: Usd::ZERO,
                entry_cash_outlay_usd: Usd::new(dec!(50)),
                actual_gross_pnl_usd: Usd::new(dec!(5)),
                actual_net_pnl_usd: Usd::new(dec!(5)),
                actual_gross_return_bps: Bps::new(dec!(1000)),
                actual_net_return_bps: Bps::new(dec!(1000)),
            },
            horizon_end: horizon_at,
            points: Vec::new(),
        })
        .expect("trajectory");
        let trajectory_hash = hash(7);
        let counterfactual = PolicyCounterfactualOutcome::replay(
            &trajectory,
            trajectory_hash,
            hash(8),
            AlternativeExitPolicy::LatestExecutableAtOrBeforeHorizon,
        )
        .expect("counterfactual");
        let comparison = PlannedActualExecutionComparisonBuilder::build(
            &PlannedActualExecutionComparisonInput {
                recommendation_id,
                decision_at,
                requested_shares: Shares::new(dec!(100)),
                economic_outcome: &economic,
                trajectory: &trajectory,
                trajectory_artifact_hash: trajectory_hash,
                counterfactual: &counterfactual,
                policy_counterfactual_hash: hash(9),
            },
        )
        .expect("comparison");
        let ExecutionComparisonEvaluation::Evaluated { metrics } = comparison.evaluation else {
            panic!("comparison must evaluate");
        };
        assert_eq!(metrics.latency_delta_ms, 10);
        assert_eq!(metrics.planned_fill_ratio, dec!(0.8));
        assert_eq!(metrics.actual_fill_ratio, dec!(1));
        assert_eq!(metrics.actual_vs_planned_price_bps, Bps::ZERO);
        assert_eq!(metrics.fee_delta_usd, dec!(0));
        assert_eq!(metrics.return_delta_bps, Bps::new(dec!(500)));
    }
}
