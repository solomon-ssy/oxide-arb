use std::sync::Arc;

use chrono::Utc;
use oxide_arb_models::{
    domain::{
        NewControlFactorMaterializationRun,
        control_factor::{
            AcquireMaterializationRunOutcome, CancelMaterializationRunOutcome,
            EnqueueMaterializationRunOptions, EnqueueMaterializationRunOutcome,
            InputResolutionReport, MaterializationRunManifest, MaterializationRunStatusPatch,
            NewControlFactorStageReport, RunTransitionOutcome, StageCoverageReport, StageError,
            StageReportBody,
        },
    },
    enums::control_factor::{
        ControlFactorType, EvidenceStageStatus, MaterializationErrorCode,
        MaterializationOutputPolicy, MaterializationRunStatus, MaterializationStageName,
    },
    types::MaterializationRunId,
};
use oxide_arb_repository::traits::ControlFactorRepository;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    evidence::engine::EvidenceEngine,
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
            RunTransitionOutcome::Transitioned(_) => self.execute_run(run_id, cancellation).await,
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

        let book_output = self
            .deps
            .evidence_engine
            .book_reconstruction(manifest, input_report.clone())
            .await?;
        self.persist_stage_report(&book_output.stage_report).await?;
        stage_reports.push(book_output.stage_report.clone());
        cancel_if_requested(&cancellation, "cancelled after book_reconstruction")?;
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
        cancel_if_requested(&cancellation, "cancelled after detector_evidence")?;
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
        cancel_if_requested(&cancellation, "cancelled after execution_evidence")?;
        let execution = required_artifact(
            execution_output.artifact,
            MaterializationStageName::ExecutionEvidence,
        )?;
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
        stage_reports.push(training_output.stage_report);

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
    } else {
        MaterializationRunStatus::Completed
    }
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
