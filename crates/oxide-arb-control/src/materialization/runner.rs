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
        },
    },
    enums::control_factor::{
        EvidenceStageStatus, MaterializationErrorCode, MaterializationOutputPolicy,
        MaterializationRunStatus, MaterializationStageName,
    },
    types::MaterializationRunId,
};
use oxide_arb_repository::traits::ControlFactorRepository;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::materialization::{
    MaterializationError, MaterializationResult, PointInTimeResolver,
    SealedMaterializationManifest, StageReportBuilder,
};

#[derive(Clone)]
pub struct MaterializationRunnerDeps {
    pub control_factors: Arc<dyn ControlFactorRepository>,
    pub pit_resolver: Arc<PointInTimeResolver>,
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
        match self.resolve_inputs_stage(&manifest).await {
            Ok(report) => {
                if cancellation.is_cancelled() {
                    return self
                        .cancel_run(run_id, "cancelled after resolve_inputs")
                        .await;
                }
                let target = terminal_status(&manifest, &report);
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
            Err(error) => self.fail_run(run_id, error).await,
        }
    }

    async fn resolve_inputs_stage(
        &self,
        manifest: &MaterializationRunManifest,
    ) -> MaterializationResult<InputResolutionReport> {
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
                    EvidenceStageStatus::ReportOnly
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
                Ok(report)
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

const fn terminal_status(
    manifest: &MaterializationRunManifest,
    report: &InputResolutionReport,
) -> MaterializationRunStatus {
    if matches!(
        manifest.output_policy,
        MaterializationOutputPolicy::ReportOnly | MaterializationOutputPolicy::NoFactorOutput
    ) || !manifest.production_output_allowed()
        || !report.manifest.production_eligible
    {
        MaterializationRunStatus::ReportOnly
    } else {
        MaterializationRunStatus::Completed
    }
}
