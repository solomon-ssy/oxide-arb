//! Blocking quality gates for Phase 5.4 factor materialization.

use crate::{
    evidence::{
        detector::DetectorEvidenceArtifact, portfolio::PortfolioRiskEvidenceArtifact,
        training::TrainingExampleArtifact,
    },
    factor::stats::dominance_bps,
};
use chrono::Utc;
use oxide_arb_models::{
    domain::{
        control_factor::{
            ControlFactorValue, FactorBuildArtifact, PointInTimeInputManifest, QualityGateDecision,
            QualityGateEvaluationArtifact, QualityGateEvaluationReport, QualityGatePolicy,
            StageReportBody,
        },
        evidence::{EvidenceMetric, FactorTrainingExample},
    },
    enums::control_factor::{
        ControlFactorType, EvidenceStageStatus, FactorStatus, MaterializationOutputPolicy,
        MaterializationStageName, QualityGateName, QualityGateOutcome,
    },
};

pub struct QualityGateEvaluator;

/// Inputs required to evaluate blocking gates beyond the built factor row.
pub struct QualityGateContext<'a> {
    pub policy: &'a QualityGatePolicy,
    pub output_policy: MaterializationOutputPolicy,
    pub stage_reports: &'a [StageReportBody],
    pub pit_manifest: &'a PointInTimeInputManifest,
    pub training: &'a TrainingExampleArtifact,
    pub detector: &'a DetectorEvidenceArtifact,
    pub portfolio: &'a PortfolioRiskEvidenceArtifact,
}

impl QualityGateEvaluator {
    #[must_use]
    pub fn evaluate(
        policy: &QualityGatePolicy,
        output_policy: MaterializationOutputPolicy,
        build: FactorBuildArtifact,
        gate_context: &QualityGateContext<'_>,
    ) -> QualityGateEvaluationArtifact {
        let context = QualityGateContext {
            policy,
            output_policy,
            stage_reports: gate_context.stage_reports,
            pit_manifest: gate_context.pit_manifest,
            training: gate_context.training,
            detector: gate_context.detector,
            portfolio: gate_context.portfolio,
        };
        let mut factors = Vec::new();
        let mut decisions = Vec::new();
        for mut factor in build
            .built_factors
            .into_iter()
            .chain(build.report_only_factors)
            .chain(build.rejected_factors)
        {
            let factor_decisions = evaluate_factor(&context, &factor);
            let has_blocking_failure = factor_decisions
                .iter()
                .any(QualityGateDecision::is_blocking_failure);
            factor.status = match (context.output_policy, factor.status, has_blocking_failure) {
                (_, FactorStatus::Rejected, _) => FactorStatus::Rejected,
                (MaterializationOutputPolicy::EmitDraftCandidates, FactorStatus::Draft, false) => {
                    FactorStatus::Candidate
                }
                (MaterializationOutputPolicy::EmitDraftCandidates, FactorStatus::Draft, true) => {
                    FactorStatus::Rejected
                }
                (MaterializationOutputPolicy::EmitDraftOnly, FactorStatus::Draft, _) => {
                    FactorStatus::Draft
                }
                (_, FactorStatus::ReportOnly, _) => FactorStatus::ReportOnly,
                (_, status, _) => status,
            };
            decisions.extend(factor_decisions.into_iter().map(|mut decision| {
                decision.factor_id = Some(factor.factor_id.clone());
                decision
            }));
            factors.push(factor);
        }
        let evaluated_factor_count = u64::try_from(factors.len()).unwrap_or(u64::MAX);
        let passed_factor_count = factors
            .iter()
            .filter(|factor| matches!(factor.status, FactorStatus::Candidate | FactorStatus::Draft))
            .count()
            .try_into()
            .unwrap_or(u64::MAX);
        let rejected_factor_count = factors
            .iter()
            .filter(|factor| factor.status == FactorStatus::Rejected)
            .count()
            .try_into()
            .unwrap_or(u64::MAX);
        let report_only_factor_count = factors
            .iter()
            .filter(|factor| factor.status == FactorStatus::ReportOnly)
            .count()
            .try_into()
            .unwrap_or(u64::MAX);
        QualityGateEvaluationArtifact {
            run_id: build.run_id,
            report: QualityGateEvaluationReport {
                evaluated_factor_count,
                passed_factor_count,
                rejected_factor_count,
                report_only_factor_count,
                decisions,
            },
            factors,
        }
    }
}

fn evaluate_factor(
    context: &QualityGateContext<'_>,
    factor: &ControlFactorValue,
) -> Vec<QualityGateDecision> {
    let mut decisions = Vec::new();
    evaluate_evidence_gates(context, factor, &mut decisions);
    evaluate_risk_gates(context, factor, &mut decisions);
    decisions
}

fn evaluate_evidence_gates(
    context: &QualityGateContext<'_>,
    factor: &ControlFactorValue,
    decisions: &mut Vec<QualityGateDecision>,
) {
    let thresholds = context.policy.thresholds_for(factor.factor_type);

    if gate_enabled(context.policy, QualityGateName::PointInTime) {
        push_bool_decision(
            decisions,
            factor,
            QualityGateName::PointInTime,
            context.pit_manifest.is_production_eligible()
                && factor
                    .evidence
                    .point_in_time_inputs
                    .is_production_eligible(),
            "gate.pit_ineligible",
            "point-in-time inputs are not production eligible",
        );
    }

    if gate_enabled(context.policy, QualityGateName::UpstreamStage) {
        push_bool_decision(
            decisions,
            factor,
            QualityGateName::UpstreamStage,
            upstream_evidence_ready(context, factor.factor_type),
            "gate.upstream_stage_blocked",
            "required upstream evidence stages are not production-ready",
        );
    }

    if gate_enabled(context.policy, QualityGateName::Coverage) {
        push_bool_decision(
            decisions,
            factor,
            QualityGateName::Coverage,
            factor.evidence.data_coverage.is_sufficient(),
            "gate.coverage_insufficient",
            "factor evidence coverage is insufficient",
        );
    }

    if gate_enabled(context.policy, QualityGateName::Sample) {
        push_bool_decision(
            decisions,
            factor,
            QualityGateName::Sample,
            u64::from(factor.evidence.opportunity_count) >= thresholds.min_opportunities
                && u64::from(factor.evidence.market_count) >= thresholds.min_markets
                && u64::from(factor.evidence.settlement_count) >= thresholds.min_settlements,
            "gate.sample_insufficient",
            "factor sample thresholds are not satisfied",
        );
    }

    if gate_enabled(context.policy, QualityGateName::Leakage) {
        push_bool_decision(
            decisions,
            factor,
            QualityGateName::Leakage,
            leakage_checks_pass(context, factor),
            "gate.leakage_detected",
            "historical evidence shows calibration/bucket leakage or lookahead",
        );
    }
}

fn evaluate_risk_gates(
    context: &QualityGateContext<'_>,
    factor: &ControlFactorValue,
    decisions: &mut Vec<QualityGateDecision>,
) {
    if gate_enabled(context.policy, QualityGateName::Stability) {
        let stable = market_stability_ok(context, factor);
        push_bool_decision(
            decisions,
            factor,
            QualityGateName::Stability,
            stable,
            "gate.stability_dominance",
            "factor estimate is dominated by a single market or event",
        );
    }

    if gate_enabled(context.policy, QualityGateName::TailRisk) {
        push_bool_decision(
            decisions,
            factor,
            QualityGateName::TailRisk,
            tail_risk_within_policy(context, factor),
            "gate.tail_risk_exceeded",
            "portfolio tail drawdown exceeds policy bounds for this factor",
        );
    }

    if gate_enabled(context.policy, QualityGateName::Conservative) {
        push_bool_decision(
            decisions,
            factor,
            QualityGateName::Conservative,
            factor.payload.validate_safety().is_ok(),
            "gate.not_conservative",
            "factor payload is not conservative",
        );
    }

    if gate_enabled(context.policy, QualityGateName::Ttl) {
        push_bool_decision(
            decisions,
            factor,
            QualityGateName::Ttl,
            factor.generated_at < factor.expires_at && factor.expires_at > Utc::now(),
            "gate.ttl_invalid",
            "factor TTL is missing or already expired",
        );
    }

    if gate_enabled(context.policy, QualityGateName::Owner) {
        push_bool_decision(
            decisions,
            factor,
            QualityGateName::Owner,
            !factor.owner.trim().is_empty(),
            "gate.owner_missing",
            "factor owner is required",
        );
    }
}

fn gate_enabled(policy: &QualityGatePolicy, gate_name: QualityGateName) -> bool {
    policy.enabled_gates.contains(&gate_name)
}

fn upstream_evidence_ready(
    context: &QualityGateContext<'_>,
    factor_type: ControlFactorType,
) -> bool {
    required_upstream_stages(factor_type)
        .iter()
        .all(|stage_name| {
            context
                .stage_reports
                .iter()
                .find(|report| report.stage_name == *stage_name)
                .is_some_and(|report| upstream_stage_passes(report.status))
        })
}

const fn upstream_stage_passes(status: EvidenceStageStatus) -> bool {
    matches!(
        status,
        EvidenceStageStatus::Completed | EvidenceStageStatus::CompletedWithWarnings
    )
}

const fn required_upstream_stages(
    factor_type: ControlFactorType,
) -> &'static [MaterializationStageName] {
    use MaterializationStageName::{
        BookReconstruction, DetectorEvidence, ExecutionEvidence, PortfolioRiskEvidence,
        ResolveInputs, SettlementReconciliationEvidence, TrainingExampleBuild,
    };

    match factor_type {
        ControlFactorType::BucketRisk => &[
            ResolveInputs,
            BookReconstruction,
            DetectorEvidence,
            ExecutionEvidence,
            SettlementReconciliationEvidence,
            TrainingExampleBuild,
        ],
        ControlFactorType::ExecutionQuality => &[
            ResolveInputs,
            BookReconstruction,
            DetectorEvidence,
            ExecutionEvidence,
            TrainingExampleBuild,
        ],
        ControlFactorType::PortfolioRisk => &[
            ResolveInputs,
            BookReconstruction,
            DetectorEvidence,
            ExecutionEvidence,
            PortfolioRiskEvidence,
            TrainingExampleBuild,
        ],
        ControlFactorType::ReconciliationHealth => &[
            ResolveInputs,
            SettlementReconciliationEvidence,
            TrainingExampleBuild,
        ],
        ControlFactorType::MarketAnomaly => {
            &[ResolveInputs, DetectorEvidence, TrainingExampleBuild]
        }
    }
}

fn leakage_checks_pass(context: &QualityGateContext<'_>, factor: &ControlFactorValue) -> bool {
    if !context.pit_manifest.is_production_eligible() {
        return false;
    }
    let global_calibration_mismatch =
        evidence_metric_u64(&context.detector.report.calibration_snapshot_mismatch_count)
            .is_some_and(|count| count > 0);
    let global_bucket_mismatch =
        evidence_metric_u64(&context.detector.report.bucket_mismatch_count)
            .is_some_and(|count| count > 0);
    if global_calibration_mismatch || global_bucket_mismatch {
        return false;
    }
    let examples = training_examples_for(context.training, factor.factor_type);
    examples.iter().all(|example| {
        context
            .detector
            .detections
            .iter()
            .find(|detection| detection.opportunity_id == example.opportunity_id)
            .is_none_or(|detection| {
                !detection.mismatches.bucket && !detection.mismatches.calibration_snapshot
            })
    })
}

fn market_stability_ok(context: &QualityGateContext<'_>, factor: &ControlFactorValue) -> bool {
    let examples = training_examples_for(context.training, factor.factor_type);
    if examples.is_empty() {
        return false;
    }
    let mut market_counts = std::collections::BTreeMap::<String, u64>::new();
    for example in &examples {
        *market_counts
            .entry(example.market_id.as_str().to_owned())
            .or_insert(0) += 1;
    }
    if market_counts.len() <= 1 {
        return true;
    }
    let total = u64::try_from(examples.len()).unwrap_or(u64::MAX);
    let largest = market_counts.values().copied().max().unwrap_or(0);
    dominance_bps(largest, total)
        .ok()
        .is_some_and(|bps| bps <= context.policy.defaults.max_single_market_share_bps)
}

fn tail_risk_within_policy(context: &QualityGateContext<'_>, factor: &ControlFactorValue) -> bool {
    let drawdown_bps = evidence_metric_u64(&context.portfolio.report.max_drawdown_pct_bps);
    if factor.factor_type == ControlFactorType::PortfolioRisk && drawdown_bps.is_none() {
        return false;
    }
    let Some(drawdown_bps) = drawdown_bps else {
        return true;
    };
    drawdown_bps <= u64::from(context.policy.defaults.max_tail_drawdown_pct_bps)
}

fn evidence_metric_u64<T: Copy + Into<u64>>(metric: &EvidenceMetric<T>) -> Option<u64> {
    match metric {
        EvidenceMetric::Available { value } => Some((*value).into()),
        EvidenceMetric::Unavailable { .. } => None,
    }
}

fn training_examples_for(
    training: &TrainingExampleArtifact,
    factor_type: ControlFactorType,
) -> Vec<&FactorTrainingExample> {
    training
        .examples
        .iter()
        .filter(|example| example.factor_type == factor_type)
        .collect()
}

fn push_bool_decision(
    decisions: &mut Vec<QualityGateDecision>,
    factor: &ControlFactorValue,
    gate_name: QualityGateName,
    passed: bool,
    code: &'static str,
    message: &'static str,
) {
    if passed {
        decisions.push(QualityGateDecision {
            factor_id: Some(factor.factor_id.clone()),
            factor_type: factor.factor_type,
            gate_name,
            outcome: QualityGateOutcome::Passed,
            blocking: false,
            code: "gate.passed".to_owned(),
            message: "gate passed".to_owned(),
            observed_value: None,
            threshold: None,
        });
    } else {
        decisions.push(QualityGateDecision::failed(
            factor.factor_type,
            gate_name,
            code,
            message,
        ));
    }
}

#[cfg(test)]
mod tests {
    use oxide_arb_models::{
        domain::{
            control_factor::{
                PointInTimeInputManifest, QualityGatePolicy, StageCoverageReport, StageReportBody,
            },
            evidence::EvidenceMetric,
        },
        enums::control_factor::{
            ControlFactorType, EvidenceStageStatus, MaterializationOutputPolicy,
            MaterializationStageName,
        },
        types::{MaterializationRunId, StageReportId},
    };

    use crate::evidence::{
        detector::{DetectorEvidenceArtifact, DetectorEvidenceReport},
        portfolio::{PortfolioRiskEvidenceArtifact, PortfolioRiskEvidenceReport},
        training::{TrainingExampleArtifact, TrainingExampleReport},
    };

    use super::{QualityGateContext, upstream_evidence_ready};

    #[test]
    fn upstream_requires_completed_evidence_stages() {
        let policy = QualityGatePolicy::default();
        let pit = PointInTimeInputManifest {
            inputs: Vec::new(),
            production_eligible: true,
            missing_inputs: Vec::new(),
            fatal_errors: Vec::new(),
            warnings: Vec::new(),
            manifest_hash: "pit".to_owned(),
        };
        let training = TrainingExampleArtifact {
            report: TrainingExampleReport {
                dataset_hash: "d".into(),
                feature_schema_hash: "f".into(),
                label_schema_hash: "l".into(),
                entity_count: 0,
                example_count: 0,
                label_count: 0,
                factor_types: Vec::new(),
                query_fingerprints: Vec::new(),
            },
            examples: Vec::new(),
        };
        let detector = empty_detector_artifact();
        let portfolio = empty_portfolio_artifact();
        let stages = vec![StageReportBody {
            stage_report_id: StageReportId::new_v7(),
            run_id: MaterializationRunId::new_v7(),
            stage_name: MaterializationStageName::DetectorEvidence,
            status: EvidenceStageStatus::ProductionIneligible,
            started_at: chrono::Utc::now(),
            finished_at: Some(chrono::Utc::now()),
            input_artifact_hashes: Vec::new(),
            output_artifact_hash: None,
            coverage: StageCoverageReport::complete(0),
            metrics: serde_json::Value::Null,
            records_read: 0,
            records_written: 0,
            warnings: Vec::new(),
            errors: Vec::new(),
            query_fingerprints: Vec::new(),
        }];
        let context = QualityGateContext {
            policy: &policy,
            output_policy: MaterializationOutputPolicy::EmitDraftCandidates,
            stage_reports: &stages,
            pit_manifest: &pit,
            training: &training,
            detector: &detector,
            portfolio: &portfolio,
        };
        assert!(!upstream_evidence_ready(
            &context,
            ControlFactorType::BucketRisk
        ));
    }

    fn empty_detector_artifact() -> DetectorEvidenceArtifact {
        DetectorEvidenceArtifact {
            report: DetectorEvidenceReport {
                live_detection_count: 0,
                reconstructed_book_context_count: 0,
                materialized_detection_count: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                matched_opportunity_count: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                missed_live_signal_count: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                extra_materialized_signal_count: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                score_delta_p50: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                score_delta_p95: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                bucket_mismatch_count: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                calibration_snapshot_mismatch_count: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                query_fingerprints: Vec::new(),
            },
            detections: Vec::new(),
        }
    }

    fn empty_portfolio_artifact() -> PortfolioRiskEvidenceArtifact {
        PortfolioRiskEvidenceArtifact {
            report: PortfolioRiskEvidenceReport {
                peak_reserved_usd: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                peak_potential_loss_usd: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                peak_total_exposure_usd: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                peak_open_positions: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                max_drawdown_pct_bps: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                loss_streak_max: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                risk_denial_count: 0,
                sizing_denial_count: 0,
                settlement_backlog_max: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                stale_metrics_window_ms: EvidenceMetric::Unavailable {
                    code: "x".into(),
                    reason: "x".into(),
                },
                insufficient_reasons: Vec::new(),
                query_fingerprints: Vec::new(),
            },
            sequence_complete: false,
            sequence_hash: None,
            sequence_events: Vec::new(),
        }
    }
}
