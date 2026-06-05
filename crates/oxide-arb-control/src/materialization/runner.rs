use std::sync::Arc;

use chrono::Utc;
use oxide_arb_models::{
    domain::{
        NewControlFactorMaterializationRun,
        control_factor::{
            AcquireMaterializationRunOutcome, CancelMaterializationRunOutcome,
            EnqueueMaterializationRunOptions, EnqueueMaterializationRunOutcome,
            InputResolutionReport, MaterializationRunManifest, MaterializationRunStatusPatch,
            NewControlFactorAuditEvent, NewControlFactorStageReport, NewControlFactorValue,
            QualityGateEvaluationArtifact, RunTransitionOutcome, StageCoverageReport, StageError,
            StageReportBody,
        },
    },
    enums::control_factor::{
        AuditResourceType, ControlAuditEventType, ControlFactorType, EvidenceStageStatus,
        MaterializationErrorCode, MaterializationOutputPolicy, MaterializationRunStatus,
        MaterializationStageName, OperatorRole,
    },
    types::MaterializationRunId,
};
use oxide_arb_repository::traits::ControlFactorRepository;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    evidence::{
        book::BookReconstructionArtifact, detector::DetectorEvidenceArtifact,
        engine::EvidenceEngine, execution::ExecutionEvidenceArtifact,
        portfolio::PortfolioRiskEvidenceArtifact,
        settlement::SettlementReconciliationEvidenceArtifact, training::TrainingExampleArtifact,
    },
    factor::{FactorBuildContext, FactorBuilderRegistry},
    gates::{QualityGateContext, QualityGateEvaluator},
    materialization::{
        MaterializationError, MaterializationResult, PointInTimeResolver,
        SealedMaterializationManifest, StageReportBuilder,
    },
};

#[derive(Clone)]
pub struct MaterializationRunnerDeps {
    pub control_factors: Arc<dyn ControlFactorRepository>,
    pub pit_resolver: Arc<PointInTimeResolver>,
    pub evidence_engine: Arc<EvidenceEngine>,
}

pub struct MaterializationRunner {
    deps: MaterializationRunnerDeps,
}

#[derive(Debug, Clone)]
pub enum RunExecutionOutcome {
    Enqueued(Box<EnqueueMaterializationRunOutcome>),
    Completed(MaterializationRunStatus),
    NotQueued,
    NotFound,
    Cancelled,
}

impl MaterializationRunner {
    #[must_use]
    pub const fn new(deps: MaterializationRunnerDeps) -> Self {
        Self { deps }
    }

    pub async fn enqueue(
        &self,
        sealed: &SealedMaterializationManifest,
        options: EnqueueMaterializationRunOptions,
    ) -> MaterializationResult<RunExecutionOutcome> {
        let run: NewControlFactorMaterializationRun = sealed.try_into()?;
        let outcome = self
            .deps
            .control_factors
            .enqueue_materialization_run(run, options)
            .await?;
        Ok(RunExecutionOutcome::Enqueued(Box::new(outcome)))
    }

    pub async fn execute_run(
        &self,
        run_id: &MaterializationRunId,
        cancellation: CancellationToken,
    ) -> MaterializationResult<RunExecutionOutcome> {
        if cancellation.is_cancelled() {
            return self.cancel_run(run_id, "cancelled before acquire").await;
        }
        let acquired = self
            .deps
            .control_factors
            .try_acquire_materialization_run(run_id, Utc::now())
            .await?;
        match acquired {
            AcquireMaterializationRunOutcome::Acquired(run) => {
                self.execute_acquired_run(run.manifest, run_id, cancellation)
                    .await
            }
            AcquireMaterializationRunOutcome::NotQueued(_) => Ok(RunExecutionOutcome::NotQueued),
            AcquireMaterializationRunOutcome::NotFound => Ok(RunExecutionOutcome::NotFound),
        }
    }

    pub async fn retry_and_execute(
        &self,
        run_id: &MaterializationRunId,
        cancellation: CancellationToken,
    ) -> MaterializationResult<RunExecutionOutcome> {
        match self
            .deps
            .control_factors
            .retry_materialization_run(run_id)
            .await?
        {
            RunTransitionOutcome::Transitioned(_) => {
                Box::pin(self.execute_run(run_id, cancellation)).await
            }
            RunTransitionOutcome::InvalidTransition { current_status } => {
                warn!(run_id = %run_id, status = %current_status, "invalid retry transition");
                Ok(RunExecutionOutcome::NotQueued)
            }
            RunTransitionOutcome::NotFound => Ok(RunExecutionOutcome::NotFound),
        }
    }

    async fn execute_acquired_run(
        &self,
        manifest_json: serde_json::Value,
        run_id: &MaterializationRunId,
        cancellation: CancellationToken,
    ) -> MaterializationResult<RunExecutionOutcome> {
        let manifest: MaterializationRunManifest = serde_json::from_value(manifest_json)
            .map_err(|error| MaterializationError::Codec(error.to_string()))?;
        if cancellation.is_cancelled() {
            return self
                .cancel_run(run_id, "cancelled before resolve_inputs")
                .await;
        }
        match self.execute_stage_graph(&manifest, cancellation).await {
            Ok(report) => self.complete_run(run_id, &manifest, report).await,
            Err(error) if error.code() == Some(MaterializationErrorCode::RunCancelled.as_str()) => {
                self.cancel_run(run_id, &error.to_string()).await
            }
            Err(error) => self.fail_run(run_id, error).await,
        }
    }

    /// Runs the full evidence stage graph (resolve inputs through training examples).
    /// Used by integration smoke tests; persists stage reports via [`ControlFactorRepository`].
    pub async fn execute_evidence_pipeline(
        &self,
        manifest: &MaterializationRunManifest,
    ) -> MaterializationResult<MaterializationRunReport> {
        self.execute_stage_graph(manifest, CancellationToken::new())
            .await
    }

    async fn execute_stage_graph(
        &self,
        manifest: &MaterializationRunManifest,
        cancellation: CancellationToken,
    ) -> MaterializationResult<MaterializationRunReport> {
        let mut stage_reports = Vec::new();
        let (input_report, resolve_stage_report) = self.resolve_inputs_stage(manifest).await?;
        stage_reports.push(resolve_stage_report);
        cancel_if_requested(&cancellation, "cancelled after resolve_inputs")?;

        let decision_evidence = self
            .execute_decision_evidence_stages(
                manifest,
                input_report.clone(),
                &mut stage_reports,
                &cancellation,
            )
            .await?;
        let book = decision_evidence.book;
        let detector = decision_evidence.detector;
        let execution = decision_evidence.execution;
        let audit_funnel = self.deps.evidence_engine.audit_funnel(manifest).await?;

        let portfolio_output = self.deps.evidence_engine.portfolio_evidence(
            manifest,
            &audit_funnel.rows,
            vec![audit_funnel.fingerprint.clone()],
            &book.source_bundle,
            &execution,
        )?;
        self.persist_stage_report(&portfolio_output.stage_report)
            .await?;
        stage_reports.push(portfolio_output.stage_report.clone());
        cancel_if_requested(&cancellation, "cancelled after portfolio_risk_evidence")?;
        let portfolio = required_artifact(
            portfolio_output.artifact,
            MaterializationStageName::PortfolioRiskEvidence,
        )?;

        let settlement_output = self.deps.evidence_engine.settlement_evidence(
            manifest,
            &audit_funnel.rows,
            vec![audit_funnel.fingerprint.clone()],
            &book.source_bundle,
            &execution,
        )?;
        self.persist_stage_report(&settlement_output.stage_report)
            .await?;
        stage_reports.push(settlement_output.stage_report.clone());
        cancel_if_requested(
            &cancellation,
            "cancelled after settlement_reconciliation_evidence",
        )?;
        let settlement = required_artifact(
            settlement_output.artifact,
            MaterializationStageName::SettlementReconciliationEvidence,
        )?;

        let exit_output = self.deps.evidence_engine.exit_token_evidence(
            manifest,
            &book,
            &audit_funnel.rows,
            vec![audit_funnel.fingerprint.clone()],
            &book.source_bundle,
            &execution,
        )?;
        self.persist_stage_report(&exit_output.stage_report).await?;
        stage_reports.push(exit_output.stage_report.clone());
        cancel_if_requested(&cancellation, "cancelled after exit_token_evidence")?;

        let training_output = self.deps.evidence_engine.training_examples(
            manifest,
            &detector,
            &execution,
            &settlement,
        )?;
        self.persist_stage_report(&training_output.stage_report)
            .await?;
        stage_reports.push(training_output.stage_report.clone());
        let training = required_artifact(
            training_output.artifact,
            MaterializationStageName::TrainingExampleBuild,
        )?;

        if manifest.production_output_allowed() {
            self.execute_factor_stages(
                manifest,
                &mut stage_reports,
                FactorStageInputs {
                    input_report: &input_report,
                    detector: &detector,
                    training: &training,
                    execution: &execution,
                    portfolio: &portfolio,
                    settlement: &settlement,
                },
            )
            .await?;
        }

        Ok(MaterializationRunReport {
            input_resolution: input_report,
            stage_reports,
        })
    }

    async fn complete_run(
        &self,
        run_id: &MaterializationRunId,
        manifest: &MaterializationRunManifest,
        report: MaterializationRunReport,
    ) -> MaterializationResult<RunExecutionOutcome> {
        let target = terminal_status(manifest, &report);
        let patch = MaterializationRunStatusPatch {
            finished_at: Some(Utc::now()),
            failure_code: None,
            failure_detail: None,
            report: Some(
                serde_json::to_value(&report)
                    .map_err(|error| MaterializationError::Codec(error.to_string()))?,
            ),
            report_uri: None,
        };
        let outcome = self
            .deps
            .control_factors
            .transition_materialization_run(
                run_id,
                MaterializationRunStatus::Running,
                target,
                patch,
            )
            .await?;
        match outcome {
            RunTransitionOutcome::Transitioned(_) => {
                info!(run_id = %run_id, status = %target, "materialization run completed");
                Ok(RunExecutionOutcome::Completed(target))
            }
            RunTransitionOutcome::InvalidTransition { current_status } => {
                warn!(run_id = %run_id, status = %current_status, "invalid terminal transition");
                Ok(RunExecutionOutcome::NotQueued)
            }
            RunTransitionOutcome::NotFound => Ok(RunExecutionOutcome::NotFound),
        }
    }

    async fn resolve_inputs_stage(
        &self,
        manifest: &MaterializationRunManifest,
    ) -> MaterializationResult<(InputResolutionReport, StageReportBody)> {
        let started_at = Utc::now();
        let result = self.deps.pit_resolver.resolve(manifest).await;
        let finished_at = Utc::now();
        match result {
            Ok(report) => {
                let stage_status = if report.manifest.is_production_eligible() {
                    if report.manifest.warnings.is_empty() {
                        EvidenceStageStatus::Completed
                    } else {
                        EvidenceStageStatus::CompletedWithWarnings
                    }
                } else {
                    EvidenceStageStatus::ProductionIneligible
                };
                let mut builder = StageReportBuilder::new(
                    manifest.run_id.clone(),
                    MaterializationStageName::ResolveInputs,
                    started_at,
                )
                .status(stage_status)
                .finished_at(finished_at)
                .coverage(StageCoverageReport::complete(
                    u64::try_from(report.manifest.inputs.len()).unwrap_or(u64::MAX),
                ))
                .metrics(serde_json::json!({
                    "missing_input_count": report.manifest.missing_inputs.len(),
                    "fatal_error_count": report.manifest.fatal_errors.len(),
                    "market_context_count": report.market_contexts.len(),
                }))
                .records_read(u64::try_from(report.manifest.inputs.len()).unwrap_or(u64::MAX))
                .output_artifact(&report)?;
                for input in &report.manifest.inputs {
                    builder = builder.query_fingerprint(input.query_fingerprint.clone());
                }
                for warning in &report.manifest.warnings {
                    builder = builder.warning(warning.clone());
                }
                for missing in &report.manifest.missing_inputs {
                    builder = builder.error(StageError::new(missing.code, missing.detail.clone()));
                }
                let stage_report = builder.build();
                self.deps
                    .control_factors
                    .upsert_stage_report(NewControlFactorStageReport::try_from(&stage_report)?)
                    .await?;
                Ok((report, stage_report))
            }
            Err(error) => {
                let stage_report = StageReportBuilder::new(
                    manifest.run_id.clone(),
                    MaterializationStageName::ResolveInputs,
                    started_at,
                )
                .status(EvidenceStageStatus::Failed)
                .finished_at(finished_at)
                .coverage(StageCoverageReport {
                    expected_rows: 1,
                    observed_rows: 0,
                    missing_rows: 1,
                    coverage_ratio: rust_decimal::Decimal::ZERO,
                    insufficient_reasons: vec![error.to_string()],
                })
                .error(StageError::new(
                    error
                        .code()
                        .and_then(|code| code.parse().ok())
                        .unwrap_or(MaterializationErrorCode::InputCurrentStateFallbackForbidden),
                    error.to_string(),
                ))
                .build();
                self.deps
                    .control_factors
                    .upsert_stage_report(NewControlFactorStageReport::try_from(&stage_report)?)
                    .await?;
                Err(error)
            }
        }
    }

    async fn persist_stage_report(&self, report: &StageReportBody) -> MaterializationResult<()> {
        self.deps
            .control_factors
            .upsert_stage_report(NewControlFactorStageReport::try_from(report)?)
            .await?;
        Ok(())
    }

    async fn execute_decision_evidence_stages(
        &self,
        manifest: &MaterializationRunManifest,
        input_report: InputResolutionReport,
        stage_reports: &mut Vec<StageReportBody>,
        cancellation: &CancellationToken,
    ) -> MaterializationResult<DecisionEvidenceArtifacts> {
        let book_output = self
            .deps
            .evidence_engine
            .book_reconstruction(manifest, input_report)
            .await?;
        self.persist_stage_report(&book_output.stage_report).await?;
        stage_reports.push(book_output.stage_report.clone());
        cancel_if_requested(cancellation, "cancelled after book_reconstruction")?;
        let book = required_artifact(
            book_output.artifact,
            MaterializationStageName::BookReconstruction,
        )?;

        let detector_output = self
            .deps
            .evidence_engine
            .detector_evidence(manifest, &book)
            .await?;
        self.persist_stage_report(&detector_output.stage_report)
            .await?;
        stage_reports.push(detector_output.stage_report.clone());
        cancel_if_requested(cancellation, "cancelled after detector_evidence")?;
        let detector = required_artifact(
            detector_output.artifact,
            MaterializationStageName::DetectorEvidence,
        )?;

        let execution_output = self
            .deps
            .evidence_engine
            .execution_evidence(manifest, &book, &detector)
            .await?;
        self.persist_stage_report(&execution_output.stage_report)
            .await?;
        stage_reports.push(execution_output.stage_report.clone());
        cancel_if_requested(cancellation, "cancelled after execution_evidence")?;
        let execution = required_artifact(
            execution_output.artifact,
            MaterializationStageName::ExecutionEvidence,
        )?;
        Ok(DecisionEvidenceArtifacts {
            book,
            detector,
            execution,
        })
    }

    async fn execute_factor_stages(
        &self,
        manifest: &MaterializationRunManifest,
        stage_reports: &mut Vec<StageReportBody>,
        inputs: FactorStageInputs<'_>,
    ) -> MaterializationResult<()> {
        let factor_build = FactorBuilderRegistry::default().build(&FactorBuildContext {
            manifest,
            pit_manifest: &inputs.input_report.manifest,
            stage_reports,
            training: inputs.training,
            execution: inputs.execution,
            portfolio: inputs.portfolio,
            settlement: inputs.settlement,
        });
        let factor_stage = stage_report_for_artifact(
            manifest,
            MaterializationStageName::FactorBuild,
            EvidenceStageStatus::Completed,
            factor_build.factor_count(),
            &factor_build,
        )?;
        self.persist_stage_report(&factor_stage).await?;
        stage_reports.push(factor_stage);

        let gate_artifact = QualityGateEvaluator::evaluate(
            &manifest.quality_gate_policy,
            manifest.output_policy,
            factor_build,
            &QualityGateContext {
                policy: &manifest.quality_gate_policy,
                output_policy: manifest.output_policy,
                stage_reports,
                pit_manifest: &inputs.input_report.manifest,
                training: inputs.training,
                detector: inputs.detector,
                portfolio: inputs.portfolio,
            },
        );
        let gate_status = if gate_artifact.report.has_blocking_failures() {
            EvidenceStageStatus::CompletedWithWarnings
        } else {
            EvidenceStageStatus::Completed
        };
        let gate_stage = stage_report_for_artifact(
            manifest,
            MaterializationStageName::QualityGateEvaluation,
            gate_status,
            gate_artifact.report.evaluated_factor_count,
            &gate_artifact,
        )?;
        self.persist_stage_report(&gate_stage).await?;
        stage_reports.push(gate_stage);

        let records_written = self.write_gate_factors(manifest, &gate_artifact).await?;
        let draft_stage = stage_report_for_artifact(
            manifest,
            MaterializationStageName::DraftWrite,
            EvidenceStageStatus::Completed,
            records_written,
            &gate_artifact.factors,
        )?;
        self.persist_stage_report(&draft_stage).await?;
        stage_reports.push(draft_stage);
        Ok(())
    }

    async fn write_gate_factors(
        &self,
        manifest: &MaterializationRunManifest,
        gate_artifact: &QualityGateEvaluationArtifact,
    ) -> MaterializationResult<u64> {
        let mut records_written = 0_u64;
        for factor in &gate_artifact.factors {
            // Validate the materialization output row before persistence; builders
            // must only emit Draft / Rejected / ReportOnly with safe payloads.
            factor
                .validate_as_materialization_output()
                .map_err(|error| MaterializationError::Codec(error.to_string()))?;
            let new_factor = NewControlFactorValue::from_typed(factor, None)?;
            let audit = NewControlFactorAuditEvent {
                event_type: ControlAuditEventType::FactorCreated,
                actor: manifest.created_by.clone(),
                actor_role: OperatorRole::Operator,
                resource_type: AuditResourceType::Factor,
                resource_id: factor.factor_id.as_str().to_owned(),
                request_id: manifest.run_id.as_str().to_owned(),
                reason: "materialization draft write".to_owned(),
                before_hash: None,
                after_hash: None,
                diff: serde_json::json!({
                    "run_id": manifest.run_id,
                    "status": factor.status,
                    "factor_type": factor.factor_type,
                }),
            };
            self.deps
                .control_factors
                .create_factor(new_factor, audit)
                .await?;
            records_written = records_written.saturating_add(1);
        }
        Ok(records_written)
    }

    async fn cancel_run(
        &self,
        run_id: &MaterializationRunId,
        reason: &str,
    ) -> MaterializationResult<RunExecutionOutcome> {
        match self
            .deps
            .control_factors
            .cancel_materialization_run(run_id, reason, Utc::now())
            .await?
        {
            CancelMaterializationRunOutcome::Cancelled(_) => Ok(RunExecutionOutcome::Cancelled),
            CancelMaterializationRunOutcome::AlreadyTerminal(run) => {
                Ok(RunExecutionOutcome::Completed(run.status))
            }
            CancelMaterializationRunOutcome::NotFound => Ok(RunExecutionOutcome::NotFound),
        }
    }

    async fn fail_run(
        &self,
        run_id: &MaterializationRunId,
        error: MaterializationError,
    ) -> MaterializationResult<RunExecutionOutcome> {
        let patch = MaterializationRunStatusPatch {
            finished_at: Some(Utc::now()),
            failure_code: Some(error.failure_code()),
            failure_detail: Some(error.to_string()),
            report: None,
            report_uri: None,
        };
        match self
            .deps
            .control_factors
            .transition_materialization_run(
                run_id,
                MaterializationRunStatus::Running,
                MaterializationRunStatus::Failed,
                patch,
            )
            .await?
        {
            RunTransitionOutcome::Transitioned(_) => Err(error),
            RunTransitionOutcome::InvalidTransition { current_status } => {
                warn!(run_id = %run_id, status = %current_status, "invalid failure transition");
                Ok(RunExecutionOutcome::NotQueued)
            }
            RunTransitionOutcome::NotFound => Ok(RunExecutionOutcome::NotFound),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationRunReport {
    pub input_resolution: InputResolutionReport,
    pub stage_reports: Vec<StageReportBody>,
}

struct DecisionEvidenceArtifacts {
    book: BookReconstructionArtifact,
    detector: DetectorEvidenceArtifact,
    execution: ExecutionEvidenceArtifact,
}

struct FactorStageInputs<'a> {
    input_report: &'a InputResolutionReport,
    detector: &'a DetectorEvidenceArtifact,
    training: &'a TrainingExampleArtifact,
    execution: &'a ExecutionEvidenceArtifact,
    portfolio: &'a PortfolioRiskEvidenceArtifact,
    settlement: &'a SettlementReconciliationEvidenceArtifact,
}

fn required_artifact<T>(
    artifact: Option<T>,
    stage_name: MaterializationStageName,
) -> MaterializationResult<T> {
    artifact.ok_or_else(|| {
        MaterializationError::stable(
            MaterializationErrorCode::RunInvalidTransition.as_str(),
            format!("stage {stage_name} completed without an artifact"),
        )
    })
}

fn cancel_if_requested(
    cancellation: &CancellationToken,
    reason: &str,
) -> MaterializationResult<()> {
    if cancellation.is_cancelled() {
        Err(MaterializationError::stable(
            MaterializationErrorCode::RunCancelled.as_str(),
            reason,
        ))
    } else {
        Ok(())
    }
}

fn stage_report_for_artifact<T: serde::Serialize>(
    manifest: &MaterializationRunManifest,
    stage_name: MaterializationStageName,
    status: EvidenceStageStatus,
    records: u64,
    artifact: &T,
) -> MaterializationResult<StageReportBody> {
    let started_at = Utc::now();
    let report = StageReportBuilder::new(manifest.run_id.clone(), stage_name, started_at)
        .status(status)
        .finished_at(Utc::now())
        .coverage(StageCoverageReport::complete(records))
        .metrics(
            serde_json::to_value(artifact)
                .map_err(|error| MaterializationError::Codec(error.to_string()))?,
        )
        .records_read(records)
        .records_written(records)
        .output_artifact(artifact)?
        .build();
    Ok(report)
}

fn terminal_status(
    manifest: &MaterializationRunManifest,
    report: &MaterializationRunReport,
) -> MaterializationRunStatus {
    let has_failed_stage = report
        .stage_reports
        .iter()
        .any(|stage| matches!(stage.status, EvidenceStageStatus::Failed));
    if has_failed_stage {
        return MaterializationRunStatus::Failed;
    }
    let has_non_production_stage = report.stage_reports.iter().any(|stage| {
        stage_required_for_requested(manifest, stage.stage_name)
            && matches!(
                stage.status,
                EvidenceStageStatus::InsufficientCoverage
                    | EvidenceStageStatus::ProductionIneligible
                    | EvidenceStageStatus::EvidenceOnly
            )
    });
    if matches!(
        manifest.output_policy,
        MaterializationOutputPolicy::ReportOnly | MaterializationOutputPolicy::NoFactorOutput
    ) || !manifest.production_output_allowed()
        || !report.input_resolution.manifest.production_eligible
        || has_non_production_stage
    {
        MaterializationRunStatus::ReportOnly
    } else if gate_rejected_factor_count(report) > 0 {
        MaterializationRunStatus::CompletedWithRejectedFactors
    } else {
        MaterializationRunStatus::Completed
    }
}

fn gate_rejected_factor_count(report: &MaterializationRunReport) -> u64 {
    report
        .stage_reports
        .iter()
        .find(|stage| stage.stage_name == MaterializationStageName::QualityGateEvaluation)
        .and_then(|stage| {
            serde_json::from_value::<QualityGateEvaluationArtifact>(stage.metrics.clone()).ok()
        })
        .map_or(0, |artifact| artifact.report.rejected_factor_count)
}

fn stage_required_for_requested(
    manifest: &MaterializationRunManifest,
    stage_name: MaterializationStageName,
) -> bool {
    if manifest.requested_factor_types.is_empty() {
        return true;
    }
    manifest
        .requested_factor_types
        .iter()
        .any(|factor_type| factor_requires_stage(*factor_type, stage_name))
}

const fn factor_requires_stage(
    factor_type: ControlFactorType,
    stage_name: MaterializationStageName,
) -> bool {
    match stage_name {
        MaterializationStageName::ResolveInputs
        | MaterializationStageName::BookReconstruction
        | MaterializationStageName::TrainingExampleBuild => true,
        MaterializationStageName::DetectorEvidence => matches!(
            factor_type,
            ControlFactorType::BucketRisk
                | ControlFactorType::ExecutionQuality
                | ControlFactorType::MarketAnomaly
        ),
        MaterializationStageName::ExecutionEvidence => matches!(
            factor_type,
            ControlFactorType::BucketRisk | ControlFactorType::ExecutionQuality
        ),
        MaterializationStageName::PortfolioRiskEvidence => {
            matches!(factor_type, ControlFactorType::PortfolioRisk)
        }
        MaterializationStageName::SettlementReconciliationEvidence => matches!(
            factor_type,
            ControlFactorType::BucketRisk | ControlFactorType::ReconciliationHealth
        ),
        MaterializationStageName::ExitTokenEvidence
        | MaterializationStageName::FactorBuild
        | MaterializationStageName::QualityGateEvaluation
        | MaterializationStageName::DraftWrite => false,
    }
}
