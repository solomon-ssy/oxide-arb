//! Durable research-job worker: leases queued jobs, executes them off the HTTP
//! hot path, streams progress, and recovers orphaned runs on boot.
//!
//! # Lifecycle & recovery
//!
//! On start the worker runs a **boot recovery sweep** (`reclaim_orphaned`):
//! any `running` row whose lease is owned by a dead epoch or has expired is
//! re-queued (bounded by `recovery_attempt`) or quarantined to `failed`. During
//! steady state a per-job heartbeat renews the lease and doubles as a cooperative
//! stop signal (a job that is no longer `running` under this owner is dropped).
//! A graceful shutdown stops leasing, cooperatively drains in-flight runs
//! (bounded by `shutdown_drain_secs`), and then explicitly re-queues this
//! owner's still-`running` rows (`requeue_inflight`) so
//! the next epoch re-leases them immediately rather than after a lease-expiry
//! wait. Combined with pre-assigned result ids + idempotent result writes, this
//! makes execution effectively-once across restarts.

use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_error::{
    QuantError, QuantResult, feedback::FeedbackError, research::ResearchError,
    storage::StorageError,
};
use quant_pivot_models::{
    clickhouse::QuantFeatureParityEventRow,
    config::ResearchJobsConfig,
    domain::{
        api::{
            BuildTrainingDatasetRequest, FeedbackCoverageJobParams, FeedbackDriftJobParams,
            TradePolicyValidationJobParams,
        },
        ports::{
            BacktestPort, CalibratedModelSealCommand, CalibrationArtifactFitPort,
            CalibrationRunFinalization, CandidateRecipePlanExecutionPort,
            CandidateRecipePlanExecutionResult, CandidateRecipePlanJobParams,
            CommittedPolicyApplyPort, CpcvBacktestPort, FeatureParityExecutionOutcome,
            FeatureParityExecutionPort, FeedbackAttributionJobParams, FeedbackCalibrationJobParams,
            FeedbackComparisonExecutionPort, FeedbackComparisonExecutionResult,
            FeedbackComparisonJobParams, FeedbackCoverageExecutionPort, FeedbackCpcvJobParams,
            FeedbackDatasetSealJobParams, FeedbackDecisionExecutionPort,
            FeedbackDecisionExecutionResult, FeedbackDecisionJobParams, FeedbackDriftExecutionPort,
            FeedbackGovernanceExecutionPort, FeedbackLearningExecutionPort,
            FeedbackLearningExecutionResult, FeedbackShadowExecutionPort,
            FeedbackShadowExecutionResult, FeedbackShadowJobParams, FeedbackTrainingJobParams,
            FeedbackTruthFreezeJobParams, FeedbackValidationJobParams,
            ModelCalibrationFitJobParams, ModelCalibrationFitOutcome, ModelCalibrationFitPort,
            ModelGovernancePort, ModelTrainingPort, ShadowBindingExecutionPort,
            ShadowBindingExecutionResult, ShadowBindingJobParams, TradePolicyPort,
            TrainingDatasetPort, TrainingRunFinalization,
        },
        quant::{
            JobProgressSink, NoopProgressSink, ResearchJobArtifactRef, ResearchJobFinalization,
            ResearchJobInfo, ResearchJobResultRef,
        },
    },
    enums::quant::{
        ResearchJobErrorCode, ResearchJobKind, ResearchJobResultKind, ResearchJobStatus,
    },
    types::{
        DatasetCoverage, ModelVersionId, ResearchJobError, ResearchJobParams, ResearchJobProgress,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChFeatureParityEventRepository},
    traits::{
        AttributionArtifactRepository, CalibrationArtifactRepository, ClobMarketInfoRepository,
        ExecutionAttemptOutcomeRepository, FactWriter, FactorRepository, FeatureParityRepository,
        FeatureRepository, FeedbackCohortRepository, FeedbackCycleRepository,
        FeedbackRecipeTemplateRepository, FeedbackSchedulerRepository, MarketSelectionRepository,
        ModelCandidateManifestRepository, ModelRegistryRepository,
        ModelRouteShadowBindingRepository, PolicyRepository, PromotionPermitRepository,
        RecommendationExecutionRollupRepository, RecommendationReportRepository,
        ReportRunRepository, ResearchJobRepository, ResearchJobRetryOutcome,
        ResolutionObservationRepository, ServingEvidenceRepository, TradePolicyRepository,
    },
};
use tokio::{
    sync::watch::{self, Receiver, Sender},
    task::JoinSet,
    time::{Interval, interval},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use super::{
    AppContext,
    ports::{
        backtest::CoreBacktestPort,
        cpcv_backtest::CoreCpcvBacktestPort,
        feedback_mutation::{CoreFeedbackMutationDeps, CoreFeedbackMutationPort},
        model_training::CoreModelTrainingPort,
        training_dataset::CoreTrainingDatasetPort,
    },
    research_job::ResearchJobEngine,
    task_id::TaskId,
    task_registry::AppRunner,
};
use crate::{
    observability::metrics_hub::MetricsHub,
    service::{
        durable_feature_parity::{DurableFeatureParityDeps, DurableFeatureParitySource},
        feature_parity_executor::{FeatureParityExecutor, ReportFeatureParityIncidentResponse},
        feedback_attribution::{FeedbackAttributionDeps, FeedbackAttributionMaterializer},
        feedback_comparison::{
            FeedbackComparisonExecutionDeps, FeedbackComparisonExecutionService,
        },
        feedback_comparison_stage::{FeedbackComparisonStageAdapter, FeedbackComparisonStageDeps},
        feedback_coordinator::{
            FeedbackCoordinator, FeedbackCoordinatorConfig, FeedbackCoordinatorDeps,
            FeedbackShadowCancellationPort, FeedbackStagePort,
        },
        feedback_decision::{FeedbackDecisionExecutionDeps, FeedbackDecisionExecutionService},
        feedback_decision_stage::{FeedbackDecisionStageAdapter, FeedbackDecisionStageDeps},
        feedback_evaluation::{
            FeedbackEvaluationReservationDeps, FeedbackEvaluationReservationService,
        },
        feedback_governance::{
            FeedbackGovernanceExecutionDeps, FeedbackGovernanceExecutionService,
        },
        feedback_governance_stage::{FeedbackGovernanceStageAdapter, FeedbackGovernanceStageDeps},
        feedback_learning::{FeedbackLearningExecutionDeps, FeedbackLearningExecutionService},
        feedback_learning_stage::{FeedbackLearningStageAdapter, FeedbackLearningStageDeps},
        feedback_recipe::{CandidateRecipePlanExecutionDeps, CandidateRecipePlanExecutionService},
        feedback_recipe_stage::{FeedbackRecipeStageAdapter, FeedbackRecipeStageDeps},
        feedback_scheduler::{FeedbackScheduler, FeedbackSchedulerConfig},
        feedback_shadow::{FeedbackShadowExecutionDeps, FeedbackShadowExecutionService},
        feedback_shadow_binding::{
            ShadowBindingCancellationDeps, ShadowBindingCancellationService,
            ShadowBindingExecutionDeps, ShadowBindingExecutionService,
        },
        feedback_shadow_binding_stage::{
            FeedbackShadowBindingStageAdapter, FeedbackShadowBindingStageDeps,
        },
        feedback_shadow_stage::{FeedbackShadowStageAdapter, FeedbackShadowStageDeps},
        feedback_signal_stage::{FeedbackSignalStageAdapter, FeedbackSignalStageDeps},
        feedback_signals::{FeedbackSignalService, FeedbackSignalServiceDeps},
        feedback_stage_dispatcher::{FeedbackStageDispatcher, FeedbackStageDispatcherDeps},
        model_calibration_fit::ModelCalibrationFitService,
        trade_policy::{TradePolicyService, TradePolicyServiceDeps},
    },
};

const ALL_KINDS: [ResearchJobKind; 23] = [
    ResearchJobKind::DatasetBuild,
    ResearchJobKind::ModelTrain,
    ResearchJobKind::Backtest,
    ResearchJobKind::CpcvBacktest,
    ResearchJobKind::BiasTableFit,
    ResearchJobKind::ModelCalibrationFit,
    ResearchJobKind::FeatureParity,
    ResearchJobKind::TradePolicyFit,
    ResearchJobKind::TradePolicyValidation,
    ResearchJobKind::FeedbackTruthFreeze,
    ResearchJobKind::FeedbackCoverage,
    ResearchJobKind::FeedbackAttribution,
    ResearchJobKind::FeedbackDrift,
    ResearchJobKind::FeedbackRecipePlan,
    ResearchJobKind::FeedbackDatasetSeal,
    ResearchJobKind::FeedbackTraining,
    ResearchJobKind::FeedbackCalibration,
    ResearchJobKind::FeedbackCpcv,
    ResearchJobKind::FeedbackValidation,
    ResearchJobKind::FeedbackComparison,
    ResearchJobKind::FeedbackShadowBind,
    ResearchJobKind::FeedbackShadow,
    ResearchJobKind::FeedbackDecision,
];
const COMMITTED_MODEL_READBACK_PREFIX: &str = "committed_model_readback";
const OPERATOR_CANCEL_TERMINAL_PREFIX: &str = "operator_cancel_terminalization";
const RETRY_EXHAUSTION_TERMINAL_PHASE: &str = "retry_exhaustion_terminalization";

fn lease_deadline(lease_ttl_secs: i64) -> DateTime<Utc> {
    Utc::now() + ChronoDuration::seconds(lease_ttl_secs)
}

/// Terminal outcome of one job execution.
struct JobOutcome {
    result: Option<ResearchJobResultRef>,
    artifact: Option<ResearchJobArtifactRef>,
    coverage: Option<DatasetCoverage>,
}

/// Worker-level disposition of one bounded execution attempt.
enum JobDisposition {
    Completed(Box<JobOutcome>),
    AwaitingEvidence {
        progress: ResearchJobProgress,
        retry_after: Duration,
    },
}

enum JobRunFinalization {
    Terminalized,
    CommitWon,
}

enum RetryPreparation {
    Continue(String),
    Complete,
}

/// Owns the standalone calibration-job invariant: a successful raw fit is not
/// terminal until its exact calibrator is sealed into a new immutable model
/// version through the sole governance authority.
#[derive(Clone)]
struct ModelCalibrationJobExecutor {
    fitter: Arc<dyn ModelCalibrationFitPort>,
    governance: Arc<dyn ModelGovernancePort>,
}

impl ModelCalibrationJobExecutor {
    async fn execute(
        &self,
        params: ModelCalibrationFitJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        let source_model_id = params.request.model_version_id;
        let downside_source = params.downside_source;
        let reason = params.request.reason.clone();
        let actor = params.actor.clone();
        let outcome = self.fitter.fit(params, progress, cancel).await?;
        let result = match outcome {
            ModelCalibrationFitOutcome::Calibrated { artifact_id, .. } => {
                let calibrated = self
                    .governance
                    .seal_calibrated_model(
                        &source_model_id,
                        CalibratedModelSealCommand {
                            calibrator_ref: artifact_id,
                            downside_source,
                            reason,
                        },
                        actor,
                    )
                    .await?;
                Some(ResearchJobResultRef {
                    kind: ResearchJobResultKind::ModelVersion,
                    id: calibrated.model_version_id.as_uuid(),
                })
            }
            ModelCalibrationFitOutcome::Insufficient { .. } => None,
        };
        Ok(JobOutcome {
            result,
            artifact: None,
            coverage: None,
        })
    }
}

/// Dispatches a leased job to the matching offline service.
#[derive(Clone)]
struct ResearchJobExecutor {
    datasets: Arc<dyn TrainingDatasetPort>,
    training: Arc<dyn ModelTrainingPort>,
    backtests: Arc<dyn BacktestPort>,
    cpcv_backtests: Arc<dyn CpcvBacktestPort>,
    bias_tables: Arc<dyn CalibrationArtifactFitPort>,
    model_calibration: ModelCalibrationJobExecutor,
    feature_parity: Arc<dyn FeatureParityExecutionPort>,
    trade_policies: Arc<dyn TradePolicyPort>,
    feedback_coverage: Arc<dyn FeedbackCoverageExecutionPort>,
    feedback_drift: Arc<dyn FeedbackDriftExecutionPort>,
    feedback_recipe: Arc<dyn CandidateRecipePlanExecutionPort>,
    feedback_governance: Arc<dyn FeedbackGovernanceExecutionPort>,
    feedback_learning: Arc<dyn FeedbackLearningExecutionPort>,
    feedback_comparison: Arc<dyn FeedbackComparisonExecutionPort>,
    feedback_shadow_binding: Arc<dyn ShadowBindingExecutionPort>,
    feedback_shadow: Arc<dyn FeedbackShadowExecutionPort>,
    feedback_decision: Arc<dyn FeedbackDecisionExecutionPort>,
}

/// Synchronous latest-value progress sink handed to offline services.
///
/// The single slot cannot accumulate unbounded advisory snapshots. A producer
/// superseding an unread value increments a controlled coalescing metric.
struct ChannelProgressSink {
    tx: Sender<Option<ResearchJobProgress>>,
    metrics: Arc<MetricsHub>,
}

impl JobProgressSink for ChannelProgressSink {
    fn report(&self, progress: ResearchJobProgress) {
        if self.tx.send_replace(Some(progress)).is_some() {
            self.metrics.research_progress_coalesced_total.inc();
        }
    }
}

/// Coalesces high-frequency progress reports (e.g. per cross-section) to at most
/// one durable write + WebSocket push per `min_interval`, so a fine-grained
/// build loop cannot hammer Postgres / the event bus.
struct ProgressThrottle {
    min_interval: Duration,
    last_emit: Option<Instant>,
}

impl ProgressThrottle {
    const fn new(min_interval: Duration) -> Self {
        Self {
            min_interval,
            last_emit: None,
        }
    }

    /// Whether enough time has elapsed since the last emitted report to send
    /// another (coalescing intermediate reports within `min_interval`).
    fn should_emit(&mut self) -> bool {
        let now = Instant::now();
        match self.last_emit {
            Some(previous) if now.duration_since(previous) < self.min_interval => false,
            _ => {
                self.last_emit = Some(now);
                true
            }
        }
    }
}

impl ResearchJobExecutor {
    async fn execute_dataset(
        &self,
        request: BuildTrainingDatasetRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        let view = self.datasets.build(request, progress, cancel).await?;
        Ok(JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::TrainingDataset,
                id: view.training_dataset_id.as_uuid(),
            }),
            artifact: None,
            coverage: view.coverage,
        })
    }

    async fn execute_coverage(
        &self,
        params: FeedbackCoverageJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        let outcome = self
            .feedback_coverage
            .execute(params, progress, cancel)
            .await?;
        Ok(JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackCoverageArtifact,
                id: outcome.artifact_id.as_uuid(),
            }),
            artifact: Some(outcome.artifact),
            coverage: None,
        })
    }

    async fn execute_truth_freeze(
        &self,
        params: FeedbackTruthFreezeJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        let outcome = self
            .feedback_governance
            .freeze_truth(params, progress, cancel)
            .await?;
        Ok(JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackTruthFreezeArtifact,
                id: outcome.artifact_id.as_uuid(),
            }),
            artifact: Some(outcome.artifact),
            coverage: None,
        })
    }

    async fn execute_attribution(
        &self,
        params: FeedbackAttributionJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        let outcome = self
            .feedback_governance
            .materialize_attribution(params, progress, cancel)
            .await?;
        Ok(JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackAttributionManifest,
                id: outcome.artifact_id.as_uuid(),
            }),
            artifact: Some(outcome.artifact),
            coverage: None,
        })
    }

    async fn execute_drift(
        &self,
        params: FeedbackDriftJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        let outcome = self
            .feedback_drift
            .execute(params, progress, cancel)
            .await?;
        Ok(JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackDriftArtifact,
                id: outcome.artifact_id.as_uuid(),
            }),
            artifact: Some(outcome.artifact),
            coverage: None,
        })
    }

    fn recipe_outcome(outcome: CandidateRecipePlanExecutionResult) -> JobOutcome {
        JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::CandidateRecipePlanArtifact,
                id: outcome.artifact_id.as_uuid(),
            }),
            artifact: Some(outcome.artifact),
            coverage: None,
        }
    }

    async fn execute_recipe_plan(
        &self,
        params: CandidateRecipePlanJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        self.feedback_recipe
            .plan_recipe(params, progress, cancel)
            .await
            .map(Self::recipe_outcome)
    }

    fn learning_outcome(outcome: FeedbackLearningExecutionResult) -> JobOutcome {
        JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackLearningStageArtifact,
                id: outcome.artifact_id.as_uuid(),
            }),
            artifact: Some(outcome.artifact),
            coverage: None,
        }
    }

    async fn execute_dataset_seal(
        &self,
        params: FeedbackDatasetSealJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        self.feedback_learning
            .seal_datasets(params, progress, cancel)
            .await
            .map(Self::learning_outcome)
    }

    async fn execute_feedback_training(
        &self,
        params: FeedbackTrainingJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        self.feedback_learning
            .train(params, progress, cancel)
            .await
            .map(Self::learning_outcome)
    }

    async fn execute_feedback_calibration(
        &self,
        params: FeedbackCalibrationJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        self.feedback_learning
            .calibrate(params, progress, cancel)
            .await
            .map(Self::learning_outcome)
    }

    async fn execute_feedback_cpcv(
        &self,
        params: FeedbackCpcvJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        self.feedback_learning
            .validate_cpcv(params, progress, cancel)
            .await
            .map(Self::learning_outcome)
    }

    async fn execute_validation(
        &self,
        params: FeedbackValidationJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        let outcome = self
            .feedback_governance
            .validate_candidates(params, progress, cancel)
            .await?;
        Ok(JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackValidationArtifact,
                id: outcome.artifact_id.as_uuid(),
            }),
            artifact: Some(outcome.artifact),
            coverage: None,
        })
    }

    fn comparison_outcome(outcome: FeedbackComparisonExecutionResult) -> JobOutcome {
        JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackComparisonArtifact,
                id: outcome.artifact_id.as_uuid(),
            }),
            artifact: Some(outcome.artifact),
            coverage: None,
        }
    }

    async fn execute_feedback_comparison(
        &self,
        params: FeedbackComparisonJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        self.feedback_comparison
            .execute(params, progress, cancel)
            .await
            .map(Self::comparison_outcome)
    }

    fn shadow_binding_outcome(outcome: ShadowBindingExecutionResult) -> JobOutcome {
        JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::ShadowBindingArtifact,
                id: outcome.artifact_id.as_uuid(),
            }),
            artifact: Some(outcome.artifact),
            coverage: None,
        }
    }

    async fn execute_shadow_binding(
        &self,
        params: ShadowBindingJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        self.feedback_shadow_binding
            .bind_shadow(params, progress, cancel)
            .await
            .map(Self::shadow_binding_outcome)
    }

    fn shadow_outcome(outcome: FeedbackShadowExecutionResult) -> JobOutcome {
        JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackShadowArtifact,
                id: outcome.artifact_id.as_uuid(),
            }),
            artifact: Some(outcome.artifact),
            coverage: None,
        }
    }

    async fn execute_feedback_shadow(
        &self,
        params: FeedbackShadowJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        self.feedback_shadow
            .execute(params, progress, cancel)
            .await
            .map(Self::shadow_outcome)
    }

    fn decision_outcome(outcome: FeedbackDecisionExecutionResult) -> JobOutcome {
        JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::FeedbackDecisionArtifact,
                id: outcome.artifact_id.as_uuid(),
            }),
            artifact: Some(outcome.artifact),
            coverage: None,
        }
    }

    async fn execute_feedback_decision(
        &self,
        params: FeedbackDecisionJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        self.feedback_decision
            .execute(params, progress, cancel)
            .await
            .map(Self::decision_outcome)
    }

    async fn execute_policy_validation(
        &self,
        params: TradePolicyValidationJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        self.trade_policies
            .validate(
                &params.validation_run_id,
                &params.artifact_id,
                params.actor_id,
                params.reason,
                progress.as_ref(),
                &cancel,
            )
            .await?;
        Ok(JobOutcome {
            result: Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::TradePolicyValidationRun,
                id: params.validation_run_id.as_uuid(),
            }),
            artifact: None,
            coverage: None,
        })
    }

    fn require_job_kind(job: &ResearchJobInfo) -> QuantResult<()> {
        if job.kind == job.params_json.kind() {
            return Ok(());
        }
        Err(ResearchError::Serialization {
            detail: format!(
                "research job kind {} disagrees with typed params kind {}",
                job.kind.as_str(),
                job.params_json.kind().as_str()
            ),
        }
        .into())
    }

    async fn cancel_job_runs(&self, job: &ResearchJobInfo) -> QuantResult<JobRunFinalization> {
        let reason = "research job cancelled by operator";
        match &job.params_json {
            ResearchJobParams::ModelTrain(params) => {
                let outcome = self
                    .training
                    .cancel_run(&params.model_run_id, reason.to_owned())
                    .await?;
                Self::training_finalization(outcome, params.model_version_id)
            }
            ResearchJobParams::FeedbackTraining(params) => {
                let mut finalization = JobRunFinalization::CommitWon;
                for command in &params.commands {
                    let outcome = self
                        .training
                        .cancel_run(&command.params.model_run_id, reason.to_owned())
                        .await?;
                    if matches!(
                        Self::training_finalization(outcome, command.params.model_version_id)?,
                        JobRunFinalization::Terminalized
                    ) {
                        finalization = JobRunFinalization::Terminalized;
                    }
                }
                Ok(finalization)
            }
            ResearchJobParams::ModelCalibrationFit(params) => {
                let outcome = self
                    .model_calibration
                    .fitter
                    .cancel_run(&params.model_run_id, reason.to_owned())
                    .await?;
                Ok(Self::calibration_finalization(outcome))
            }
            ResearchJobParams::FeedbackCalibration(params) => {
                let mut finalization = JobRunFinalization::CommitWon;
                for command in &params.commands {
                    let outcome = self
                        .model_calibration
                        .fitter
                        .cancel_run(&command.params.model_run_id, reason.to_owned())
                        .await?;
                    if matches!(
                        Self::calibration_finalization(outcome),
                        JobRunFinalization::Terminalized
                    ) {
                        finalization = JobRunFinalization::Terminalized;
                    }
                }
                Ok(finalization)
            }
            _ => Ok(JobRunFinalization::Terminalized),
        }
    }

    async fn fail_job_runs(
        &self,
        job: &ResearchJobInfo,
        reason: String,
    ) -> QuantResult<JobRunFinalization> {
        match &job.params_json {
            ResearchJobParams::ModelTrain(params) => {
                let outcome = self.training.fail_run(&params.model_run_id, reason).await?;
                Self::training_finalization(outcome, params.model_version_id)
            }
            ResearchJobParams::FeedbackTraining(params) => {
                let mut finalization = JobRunFinalization::CommitWon;
                for command in &params.commands {
                    let outcome = self
                        .training
                        .fail_run(&command.params.model_run_id, reason.clone())
                        .await?;
                    if matches!(
                        Self::training_finalization(outcome, command.params.model_version_id)?,
                        JobRunFinalization::Terminalized
                    ) {
                        finalization = JobRunFinalization::Terminalized;
                    }
                }
                Ok(finalization)
            }
            ResearchJobParams::ModelCalibrationFit(params) => {
                let outcome = self
                    .model_calibration
                    .fitter
                    .fail_run(&params.model_run_id, reason)
                    .await?;
                Ok(Self::calibration_finalization(outcome))
            }
            ResearchJobParams::FeedbackCalibration(params) => {
                let mut finalization = JobRunFinalization::CommitWon;
                for command in &params.commands {
                    let outcome = self
                        .model_calibration
                        .fitter
                        .fail_run(&command.params.model_run_id, reason.clone())
                        .await?;
                    if matches!(
                        Self::calibration_finalization(outcome),
                        JobRunFinalization::Terminalized
                    ) {
                        finalization = JobRunFinalization::Terminalized;
                    }
                }
                Ok(finalization)
            }
            _ => Ok(JobRunFinalization::Terminalized),
        }
    }

    fn training_finalization(
        outcome: TrainingRunFinalization,
        expected_model_version_id: ModelVersionId,
    ) -> QuantResult<JobRunFinalization> {
        match outcome {
            TrainingRunFinalization::Terminalized => Ok(JobRunFinalization::Terminalized),
            TrainingRunFinalization::CommitWon { model_version_id }
                if model_version_id == expected_model_version_id =>
            {
                Ok(JobRunFinalization::CommitWon)
            }
            TrainingRunFinalization::CommitWon { model_version_id } => {
                Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "training commit winner returned model {model_version_id}, expected {expected_model_version_id}"
                    ),
                }
                .into())
            }
        }
    }

    const fn calibration_finalization(outcome: CalibrationRunFinalization) -> JobRunFinalization {
        match outcome {
            CalibrationRunFinalization::Terminalized => JobRunFinalization::Terminalized,
            CalibrationRunFinalization::CommitWon => JobRunFinalization::CommitWon,
        }
    }

    async fn recover_job_commit(&self, job: &ResearchJobInfo) -> QuantResult<JobOutcome> {
        match job.params_json.clone() {
            ResearchJobParams::ModelTrain(params) => {
                let expected_model_version_id = params.model_version_id;
                let expected_model_run_id = params.model_run_id;
                let view = self
                    .training
                    .train(params, Arc::new(NoopProgressSink), CancellationToken::new())
                    .await?;
                if view.model_version_id != expected_model_version_id
                    || view.model_run_id != Some(expected_model_run_id)
                {
                    return Err(ResearchError::InvalidModelArtifact {
                        detail: "training commit recovery returned another model or run".to_owned(),
                    }
                    .into());
                }
                Ok(JobOutcome {
                    result: Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::ModelVersion,
                        id: view.model_version_id.as_uuid(),
                    }),
                    artifact: None,
                    coverage: None,
                })
            }
            ResearchJobParams::FeedbackTraining(params) => {
                self.execute_feedback_training(
                    params,
                    Arc::new(NoopProgressSink),
                    CancellationToken::new(),
                )
                .await
            }
            ResearchJobParams::ModelCalibrationFit(params) => {
                self.model_calibration
                    .execute(params, Arc::new(NoopProgressSink), CancellationToken::new())
                    .await
            }
            ResearchJobParams::FeedbackCalibration(params) => {
                self.execute_feedback_calibration(
                    params,
                    Arc::new(NoopProgressSink),
                    CancellationToken::new(),
                )
                .await
            }
            _ => Err(ResearchError::Serialization {
                detail: "job without a recoverable model run reported commit-won".to_owned(),
            }
            .into()),
        }
    }

    async fn resolve_operator_terminal(
        &self,
        job: &ResearchJobInfo,
        terminal: Terminal,
    ) -> Terminal {
        let terminal = terminal.after_operator_cancel();
        let Terminal::Cancelled = terminal else {
            return terminal;
        };
        match self.cancel_job_runs(job).await {
            Ok(JobRunFinalization::Terminalized) => Terminal::Cancelled,
            Ok(JobRunFinalization::CommitWon) => match self.recover_job_commit(job).await {
                Ok(outcome) => Terminal::Succeeded(Box::new(outcome)),
                Err(error) => {
                    warn!(
                        job_id = %job.job_id,
                        %error,
                        "committed model-producing result awaits exact operator-cancel read-back"
                    );
                    Terminal::Retryable(format!("{COMMITTED_MODEL_READBACK_PREFIX}: {error}"))
                }
            },
            Err(error) if is_transient_error(&error) => {
                Terminal::Retryable(format!("{OPERATOR_CANCEL_TERMINAL_PREFIX}: {error}"))
            }
            Err(error) => Terminal::Failed(format!(
                "operator model-run cancellation did not commit: {error}"
            )),
        }
    }

    async fn execute_feedback_job(
        &self,
        params: ResearchJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobOutcome> {
        match params {
            ResearchJobParams::FeedbackTruthFreeze(params) => {
                self.execute_truth_freeze(params, progress, cancel).await
            }
            ResearchJobParams::FeedbackCoverage(params) => {
                self.execute_coverage(params, progress, cancel).await
            }
            ResearchJobParams::FeedbackAttribution(params) => {
                self.execute_attribution(params, progress, cancel).await
            }
            ResearchJobParams::FeedbackDrift(params) => {
                self.execute_drift(params, progress, cancel).await
            }
            ResearchJobParams::FeedbackRecipePlan(params) => {
                self.execute_recipe_plan(*params, progress, cancel).await
            }
            ResearchJobParams::FeedbackDatasetSeal(params) => {
                self.execute_dataset_seal(params, progress, cancel).await
            }
            ResearchJobParams::FeedbackTraining(params) => {
                self.execute_feedback_training(params, progress, cancel)
                    .await
            }
            ResearchJobParams::FeedbackCalibration(params) => {
                self.execute_feedback_calibration(params, progress, cancel)
                    .await
            }
            ResearchJobParams::FeedbackCpcv(params) => {
                self.execute_feedback_cpcv(params, progress, cancel).await
            }
            ResearchJobParams::FeedbackValidation(params) => {
                self.execute_validation(params, progress, cancel).await
            }
            ResearchJobParams::FeedbackComparison(params) => {
                self.execute_feedback_comparison(*params, progress, cancel)
                    .await
            }
            ResearchJobParams::FeedbackShadowBind(params) => {
                self.execute_shadow_binding(*params, progress, cancel).await
            }
            ResearchJobParams::FeedbackShadow(params) => {
                self.execute_feedback_shadow(*params, progress, cancel)
                    .await
            }
            ResearchJobParams::FeedbackDecision(params) => {
                self.execute_feedback_decision(*params, progress, cancel)
                    .await
            }
            unexpected @ (ResearchJobParams::DatasetBuild(_)
            | ResearchJobParams::ModelTrain(_)
            | ResearchJobParams::Backtest(_)
            | ResearchJobParams::CpcvBacktest(_)
            | ResearchJobParams::BiasTableFit(_)
            | ResearchJobParams::ModelCalibrationFit(_)
            | ResearchJobParams::FeatureParity(_)
            | ResearchJobParams::TradePolicyFit(_)
            | ResearchJobParams::TradePolicyValidation(_)) => Err(ResearchError::Serialization {
                detail: format!(
                    "non-feedback research job {} reached the feedback executor",
                    unexpected.kind()
                ),
            }
            .into()),
        }
    }

    async fn execute(
        &self,
        job: &ResearchJobInfo,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<JobDisposition> {
        Self::require_job_kind(job)?;
        let outcome = match job.params_json.clone() {
            ResearchJobParams::DatasetBuild(request) => {
                self.execute_dataset(request, progress, cancel).await
            }
            ResearchJobParams::ModelTrain(params) => {
                let view = self.training.train(params, progress, cancel).await?;
                Ok(JobOutcome {
                    result: Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::ModelVersion,
                        id: view.model_version_id.as_uuid(),
                    }),
                    artifact: None,
                    coverage: None,
                })
            }
            ResearchJobParams::Backtest(params) => {
                let view = self
                    .backtests
                    .run(params.model_version_id, params.request, progress, cancel)
                    .await?;
                Ok(JobOutcome {
                    result: Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::BacktestReport,
                        id: view.backtest_report_id.as_uuid(),
                    }),
                    artifact: None,
                    coverage: None,
                })
            }
            ResearchJobParams::CpcvBacktest(params) => {
                let view = self.cpcv_backtests.run(params, progress, cancel).await?;
                Ok(JobOutcome {
                    result: Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::BacktestPathSet,
                        id: view.path_set_id.as_uuid(),
                    }),
                    artifact: None,
                    coverage: None,
                })
            }
            ResearchJobParams::BiasTableFit(params) => {
                let outcome = self.bias_tables.fit(params, progress, cancel).await?;
                // Fail-closed fits succeed with no artifact (result_ref = None).
                Ok(JobOutcome {
                    result: outcome.artifact_id.map(|id| ResearchJobResultRef {
                        kind: ResearchJobResultKind::CalibrationArtifact,
                        id: id.as_uuid(),
                    }),
                    artifact: None,
                    coverage: None,
                })
            }
            ResearchJobParams::ModelCalibrationFit(params) => {
                self.model_calibration
                    .execute(params, progress, cancel)
                    .await
            }
            ResearchJobParams::FeatureParity(params) => {
                let outcome = self
                    .feature_parity
                    .execute(params, progress, cancel)
                    .await?;
                match outcome {
                    FeatureParityExecutionOutcome::Completed(view) => Ok(JobOutcome {
                        result: Some(ResearchJobResultRef {
                            kind: ResearchJobResultKind::FeatureParityRun,
                            id: view.parity_run_id.as_uuid(),
                        }),
                        artifact: None,
                        coverage: None,
                    }),
                    FeatureParityExecutionOutcome::AwaitingMaterialization { retry_after } => {
                        return Ok(JobDisposition::AwaitingEvidence {
                            progress: ResearchJobProgress::indeterminate(
                                "pending_materialization",
                                0,
                            ),
                            retry_after,
                        });
                    }
                }
            }
            ResearchJobParams::TradePolicyFit(params) => {
                let view = self
                    .trade_policies
                    .fit(
                        &job.job_id,
                        &params.training_dataset_id,
                        params.request,
                        Arc::clone(&progress),
                        cancel.clone(),
                    )
                    .await?;
                Ok(JobOutcome {
                    result: Some(ResearchJobResultRef {
                        kind: ResearchJobResultKind::TradePolicyArtifact,
                        id: view.artifact_id.as_uuid(),
                    }),
                    artifact: None,
                    coverage: None,
                })
            }
            ResearchJobParams::TradePolicyValidation(params) => {
                self.execute_policy_validation(params, progress, cancel)
                    .await
            }
            params => self.execute_feedback_job(params, progress, cancel).await,
        }?;
        Ok(JobDisposition::Completed(Box::new(outcome)))
    }
}

struct FeedbackExecutionPorts {
    coverage: Arc<dyn FeedbackCoverageExecutionPort>,
    drift: Arc<dyn FeedbackDriftExecutionPort>,
    recipe: Arc<dyn CandidateRecipePlanExecutionPort>,
    governance: Arc<dyn FeedbackGovernanceExecutionPort>,
    learning: Arc<dyn FeedbackLearningExecutionPort>,
    comparison: Arc<dyn FeedbackComparisonExecutionPort>,
    shadow_binding: Arc<dyn ShadowBindingExecutionPort>,
    shadow: Arc<dyn FeedbackShadowExecutionPort>,
    decision: Arc<dyn FeedbackDecisionExecutionPort>,
}

impl FeedbackExecutionPorts {
    fn from_context(
        context: &AppContext,
        recipe_datasets: Arc<CoreTrainingDatasetPort>,
        datasets: Arc<dyn TrainingDatasetPort>,
        training: Arc<dyn ModelTrainingPort>,
        calibration_fit: Arc<dyn ModelCalibrationFitPort>,
        backtests: Arc<CoreBacktestPort>,
        cpcv: Arc<dyn CpcvBacktestPort>,
    ) -> QuantResult<Self> {
        let signals = Arc::new(FeedbackSignalService::try_new(FeedbackSignalServiceDeps {
            cycles: Arc::clone(&context.infra.repos.feedback_cycle)
                as Arc<dyn FeedbackCycleRepository>,
            models: Arc::clone(&context.research.model_registry_repo)
                as Arc<dyn ModelRegistryRepository>,
            policies: Arc::clone(&context.infra.repos.runtime_config) as Arc<dyn PolicyRepository>,
            preimages: Arc::clone(&context.research.serving_preimages),
            cohort_repository: Arc::clone(&context.infra.repos.feedback_cohort)
                as Arc<dyn FeedbackCohortRepository>,
            feature_repository: Arc::clone(&context.research.feature_repo)
                as Arc<dyn FeatureRepository>,
            factor_repository: Arc::clone(&context.research.factor_repo)
                as Arc<dyn FactorRepository>,
            artifact_store: Arc::clone(&context.research.artifact_store),
            compute: Arc::clone(&context.compute),
        })?);
        Ok(Self {
            coverage: Arc::clone(&signals) as Arc<dyn FeedbackCoverageExecutionPort>,
            drift: signals as Arc<dyn FeedbackDriftExecutionPort>,
            recipe: Arc::new(CandidateRecipePlanExecutionService::new(
                CandidateRecipePlanExecutionDeps {
                    compute: Arc::clone(&context.compute),
                    cycles: Arc::clone(&context.infra.repos.feedback_cycle)
                        as Arc<dyn FeedbackCycleRepository>,
                    templates: Arc::clone(&context.infra.repos.feedback_recipe_template)
                        as Arc<dyn FeedbackRecipeTemplateRepository>,
                    models: Arc::clone(&context.research.model_registry_repo)
                        as Arc<dyn ModelRegistryRepository>,
                    jobs: Arc::clone(&context.infra.repos.research_job)
                        as Arc<dyn ResearchJobRepository>,
                    policies: Arc::clone(&context.infra.repos.runtime_config)
                        as Arc<dyn PolicyRepository>,
                    training_datasets: recipe_datasets,
                    serving_preimages: Arc::clone(&context.research.serving_preimages),
                    artifacts: Arc::clone(&context.research.artifact_store),
                },
            )),
            governance: Arc::new(FeedbackGovernanceExecutionService::new(
                FeedbackGovernanceExecutionDeps {
                    resolutions: Arc::clone(&context.infra.repos.resolution_observation)
                        as Arc<dyn ResolutionObservationRepository>,
                    attempts: Arc::clone(&context.infra.repos.execution_attempt_outcome)
                        as Arc<dyn ExecutionAttemptOutcomeRepository>,
                    rollups: Arc::clone(&context.infra.repos.recommendation_execution_rollup)
                        as Arc<dyn RecommendationExecutionRollupRepository>,
                    attribution: Arc::clone(&context.infra.repos.attribution_artifact)
                        as Arc<dyn AttributionArtifactRepository>,
                    attribution_materializer: Arc::new(FeedbackAttributionMaterializer::try_new(
                        FeedbackAttributionDeps {
                            cohorts: Arc::clone(&context.infra.repos.feedback_cohort)
                                as Arc<dyn FeedbackCohortRepository>,
                            factors: Arc::clone(&context.research.factor_repo),
                            features: Arc::clone(&context.research.feature_repo),
                            models: Arc::clone(&context.research.model_registry_repo),
                            selections: Arc::clone(&context.research.market_selection_repo)
                                as Arc<dyn MarketSelectionRepository>,
                            policies: Arc::clone(&context.infra.repos.runtime_config)
                                as Arc<dyn PolicyRepository>,
                            attempts: Arc::clone(&context.infra.repos.execution_attempt_outcome)
                                as Arc<dyn ExecutionAttemptOutcomeRepository>,
                            facts: Arc::clone(&context.research.quant_fact_read),
                            clob_market_info: Arc::clone(&context.infra.repos.clob_market_info)
                                as Arc<dyn ClobMarketInfoRepository>,
                            serving_evidence: Arc::new(ChFeatureParityEventRepository::new(
                                Arc::clone(&context.infra.ch),
                            ))
                                as Arc<dyn ServingEvidenceRepository>,
                            index: Arc::clone(&context.infra.repos.attribution_artifact)
                                as Arc<dyn AttributionArtifactRepository>,
                            artifacts: Arc::clone(&context.research.artifact_store),
                            metrics: Arc::clone(&context.infra.metrics),
                            compute: Arc::clone(&context.compute),
                            compute_budget: context
                                .config
                                .quant
                                .research_jobs
                                .feedback_attribution_compute,
                        },
                    )?),
                    model_governance: Arc::clone(&context.research.model_governance),
                    artifacts: Arc::clone(&context.research.artifact_store),
                    metrics: Arc::clone(&context.infra.metrics),
                },
            )),
            learning: Arc::new(FeedbackLearningExecutionService::try_new(
                FeedbackLearningExecutionDeps {
                    datasets,
                    training,
                    calibration_fit,
                    calibration_artifacts: Arc::clone(&context.research.calibration_artifact_fit),
                    cpcv,
                    governance: Arc::clone(&context.research.model_governance),
                    artifacts: Arc::clone(&context.research.artifact_store),
                    compute: Arc::clone(&context.compute),
                },
            )?),
            comparison: Arc::new(FeedbackComparisonExecutionService::new(
                FeedbackComparisonExecutionDeps {
                    backtests,
                    path_sets: Arc::clone(&context.research.backtest_path_set_repo),
                    artifacts: Arc::clone(&context.research.artifact_store),
                },
            )),
            shadow_binding: Arc::new(ShadowBindingExecutionService::new(
                ShadowBindingExecutionDeps {
                    bindings: Arc::clone(&context.infra.repos.model_route_shadow_binding)
                        as Arc<dyn ModelRouteShadowBindingRepository>,
                    policy_apply: Arc::clone(&context.governance.committed_policy)
                        as Arc<dyn CommittedPolicyApplyPort>,
                    route_evidence: Arc::clone(&context.research.model_route_evidence),
                    artifacts: Arc::clone(&context.research.artifact_store),
                },
            )),
            shadow: Arc::new(FeedbackShadowExecutionService::new(
                FeedbackShadowExecutionDeps {
                    observations: Arc::clone(&context.research.shadow_comparison_repo),
                    artifacts: Arc::clone(&context.research.artifact_store),
                },
            )),
            decision: Arc::new(FeedbackDecisionExecutionService::new(
                FeedbackDecisionExecutionDeps {
                    artifacts: Arc::clone(&context.research.artifact_store),
                },
            )),
        })
    }
}

impl ResearchJobExecutor {
    fn from_context(context: &AppContext, config: &ResearchJobsConfig) -> QuantResult<Self> {
        let runtime_config =
            Arc::clone(&context.infra.repos.runtime_config) as Arc<dyn PolicyRepository>;
        let bias_tables = Arc::clone(&context.infra.repos.calibration_artifact)
            as Arc<dyn CalibrationArtifactRepository>;
        let backtests = Arc::new(CoreBacktestPort::from_research(
            &context.research,
            Arc::clone(&runtime_config),
        ));
        let cpcv_backtests = Arc::new(CoreCpcvBacktestPort::from_research(&context.research));
        let calibration_fit: Arc<dyn ModelCalibrationFitPort> =
            Arc::new(ModelCalibrationFitService::new(
                Arc::clone(&backtests),
                Arc::clone(&context.research.model_registry_repo),
                Arc::clone(&context.research.training_dataset_repo),
                Arc::clone(&bias_tables),
                Arc::clone(&context.research.model_run_repo),
                Arc::clone(&runtime_config),
                Arc::clone(&context.compute),
            ));
        let recipe_datasets = Arc::new(CoreTrainingDatasetPort::from_research(
            &context.research,
            Arc::clone(&runtime_config),
            Arc::clone(&bias_tables),
            config.max_spine_samples,
            config.plan_sample_slices,
            config.plan_sample_markets,
        ));
        let datasets = Arc::clone(&recipe_datasets) as Arc<dyn TrainingDatasetPort>;
        let training = Arc::new(CoreModelTrainingPort::from_research(
            &context.research,
            Arc::clone(&runtime_config),
        )) as Arc<dyn ModelTrainingPort>;
        let cpcv = Arc::clone(&cpcv_backtests) as Arc<dyn CpcvBacktestPort>;
        let serving_evidence = Arc::new(ChFeatureParityEventRepository::new(Arc::clone(
            &context.infra.ch,
        ))) as Arc<dyn ServingEvidenceRepository>;
        let parity_replay = Arc::new(DurableFeatureParitySource::try_new(
            DurableFeatureParityDeps {
                parity: Arc::clone(&context.infra.repos.feature_parity)
                    as Arc<dyn FeatureParityRepository>,
                model_runs: Arc::clone(&context.research.model_run_repo),
                serving_generations: Arc::clone(&context.research.serving_generations),
                runtime_configs: Arc::clone(&runtime_config),
                selections: Arc::clone(&context.research.market_selection_repo),
                feature_vectors: Arc::clone(&context.research.feature_repo),
                factors: Arc::clone(&context.research.factor_repo),
                reports: Arc::clone(&context.infra.repos.recommendation_report)
                    as Arc<dyn RecommendationReportRepository>,
                report_runs: Arc::clone(&context.infra.repos.report_run)
                    as Arc<dyn ReportRunRepository>,
                serving_evidence,
                fact_read: Arc::clone(&context.research.quant_fact_read),
                catalog: Arc::clone(&context.research.catalog_ledger_repo),
                clob_market_info: Arc::clone(&context.research.clob_market_info_repo),
                linkages: Arc::clone(&context.research.market_linkage_repo),
                calibration_artifacts: Arc::clone(&bias_tables),
                exchange_history: Arc::clone(&context.research.exchange_history_repo),
                compute: Arc::clone(&context.compute),
                compute_budget: config.feature_parity_compute,
            },
        )?);
        let feedback = FeedbackExecutionPorts::from_context(
            context,
            recipe_datasets,
            Arc::clone(&datasets),
            Arc::clone(&training),
            Arc::clone(&calibration_fit),
            Arc::clone(&backtests),
            Arc::clone(&cpcv),
        )?;
        Ok(Self {
            datasets: Arc::clone(&datasets),
            training,
            backtests: backtests as Arc<dyn BacktestPort>,
            cpcv_backtests: cpcv,
            bias_tables: Arc::clone(&context.research.calibration_artifact_fit),
            model_calibration: ModelCalibrationJobExecutor {
                fitter: calibration_fit,
                governance: Arc::clone(&context.research.model_governance),
            },
            feature_parity: Arc::new(FeatureParityExecutor::new(
                Arc::clone(&context.infra.repos.feature_parity) as Arc<dyn FeatureParityRepository>,
                parity_replay,
                Arc::new(ChFactWriter::new(
                    Arc::clone(&context.infra.ch),
                    Arc::clone(&context.infra.ch_write_manager),
                    "quant_feature_parity_event",
                )) as Arc<dyn FactWriter<QuantFeatureParityEventRow>>,
                Arc::new(ReportFeatureParityIncidentResponse::new(
                    context.report_lifecycle(),
                    Arc::clone(&context.infra.repos.recommendation_report)
                        as Arc<dyn RecommendationReportRepository>,
                    Arc::clone(&context.governance.alerts),
                    Arc::clone(&context.infra.metrics),
                )),
                Arc::clone(&context.infra.metrics),
                ChronoDuration::minutes(10),
                Duration::from_secs(config.poll_secs),
            )),
            trade_policies: Arc::new(TradePolicyService::new(TradePolicyServiceDeps {
                compute: Arc::clone(&context.compute),
                datasets: Arc::clone(&context.research.training_dataset_repo),
                dataset_builder: datasets,
                artifacts: Arc::clone(&context.research.artifact_store),
                policies: Arc::clone(&context.infra.repos.trade_policy)
                    as Arc<dyn TradePolicyRepository>,
                model_registry: Arc::clone(&context.research.model_registry_repo),
                runtime_configs: runtime_config,
                source_slices: Arc::clone(&context.research.source_slice_repo),
                readiness: Arc::clone(&context.research.research_readiness),
                serving_preimages: Arc::clone(&context.research.serving_preimages),
            })),
            feedback_coverage: feedback.coverage,
            feedback_drift: feedback.drift,
            feedback_recipe: feedback.recipe,
            feedback_governance: feedback.governance,
            feedback_learning: feedback.learning,
            feedback_comparison: feedback.comparison,
            feedback_shadow_binding: feedback.shadow_binding,
            feedback_shadow: feedback.shadow,
            feedback_decision: feedback.decision,
        })
    }
}

impl FeedbackCoordinator {
    fn from_context(
        context: &AppContext,
        engine: ResearchJobEngine,
        config: &ResearchJobsConfig,
    ) -> QuantResult<Self> {
        let cycles =
            Arc::clone(&context.infra.repos.feedback_cycle) as Arc<dyn FeedbackCycleRepository>;
        let jobs = Arc::clone(engine.repo());
        let recipes = Arc::new(FeedbackRecipeStageAdapter::try_new(
            FeedbackRecipeStageDeps {
                cycles: Arc::clone(&cycles),
                jobs: Arc::clone(&jobs),
                artifacts: Arc::clone(&context.research.artifact_store),
                max_recovery_attempts: config.max_recovery_attempts,
            },
        )?);
        let learning = Arc::new(FeedbackLearningStageAdapter::try_new(
            FeedbackLearningStageDeps {
                jobs: Arc::clone(&jobs),
                artifacts: Arc::clone(&context.research.artifact_store),
                recipes: Arc::clone(&recipes),
                max_recovery_attempts: config.max_recovery_attempts,
            },
        )?);
        let governance = Arc::new(FeedbackGovernanceStageAdapter::try_new(
            FeedbackGovernanceStageDeps {
                cycles: Arc::clone(&cycles),
                jobs: Arc::clone(&jobs),
                artifacts: Arc::clone(&context.research.artifact_store),
                learning: Arc::clone(&learning),
                max_recovery_attempts: config.max_recovery_attempts,
            },
        )?);
        let reservations = Arc::new(FeedbackEvaluationReservationService::new(
            FeedbackEvaluationReservationDeps {
                cycles: Arc::clone(&cycles),
                datasets: Arc::clone(&context.research.training_dataset_repo),
                learning_stages: Arc::clone(&learning),
            },
        ));
        let shadow_binding = Arc::new(FeedbackShadowBindingStageAdapter::try_new(
            FeedbackShadowBindingStageDeps {
                cycles: Arc::clone(&cycles),
                jobs: Arc::clone(&jobs),
                models: Arc::clone(&context.research.model_registry_repo),
                path_sets: Arc::clone(&context.research.backtest_path_set_repo),
                calibrations: Arc::clone(&context.research.calibration_artifact_repo),
                policies: Arc::clone(&context.infra.repos.runtime_config)
                    as Arc<dyn PolicyRepository>,
                manifests: Arc::clone(&context.infra.repos.model_candidate_manifest)
                    as Arc<dyn ModelCandidateManifestRepository>,
                artifacts: Arc::clone(&context.research.artifact_store),
                recipes: Arc::clone(&recipes),
                total_shadow_model_budget_bytes: context
                    .config
                    .research
                    .model_serving_registry
                    .max_total_shadow_model_bytes,
                max_recovery_attempts: config.max_recovery_attempts,
            },
        )?);
        let shadow_cancellation = Arc::new(ShadowBindingCancellationService::new(
            ShadowBindingCancellationDeps {
                bindings: Arc::clone(&context.infra.repos.model_route_shadow_binding)
                    as Arc<dyn ModelRouteShadowBindingRepository>,
                policies: Arc::clone(&context.infra.repos.runtime_config)
                    as Arc<dyn PolicyRepository>,
                policy_apply: Arc::clone(&context.governance.committed_policy)
                    as Arc<dyn CommittedPolicyApplyPort>,
            },
        )) as Arc<dyn FeedbackShadowCancellationPort>;
        let stages = Arc::new(FeedbackStageDispatcher::new(FeedbackStageDispatcherDeps {
            signals: Arc::new(FeedbackSignalStageAdapter::try_new(
                FeedbackSignalStageDeps {
                    jobs: Arc::clone(&jobs),
                    artifacts: Arc::clone(&context.research.artifact_store),
                    max_recovery_attempts: config.max_recovery_attempts,
                },
            )?),
            governance: Arc::clone(&governance),
            recipes: Arc::clone(&recipes),
            learning: Arc::clone(&learning),
            comparison: Arc::new(FeedbackComparisonStageAdapter::try_new(
                FeedbackComparisonStageDeps {
                    cycles: Arc::clone(&cycles),
                    jobs: Arc::clone(&jobs),
                    models: Arc::clone(&context.research.model_registry_repo),
                    path_sets: Arc::clone(&context.research.backtest_path_set_repo),
                    artifacts: Arc::clone(&context.research.artifact_store),
                    learning_stages: Arc::clone(&learning),
                    governance_stages: governance,
                    evaluation_reservations: reservations,
                    max_recovery_attempts: config.max_recovery_attempts,
                },
            )?),
            shadow_binding: Arc::clone(&shadow_binding),
            shadow: Arc::new(FeedbackShadowStageAdapter::try_new(
                FeedbackShadowStageDeps {
                    cycles: Arc::clone(&cycles),
                    jobs: Arc::clone(&jobs),
                    artifacts: Arc::clone(&context.research.artifact_store),
                    serving_generations: Arc::clone(&context.research.serving_generations),
                    recipes: Arc::clone(&recipes),
                    shadow_bindings: shadow_binding,
                    max_recovery_attempts: config.max_recovery_attempts,
                },
            )?),
            decision: Arc::new(FeedbackDecisionStageAdapter::try_new(
                FeedbackDecisionStageDeps {
                    cycles: Arc::clone(&cycles),
                    jobs,
                    artifacts: Arc::clone(&context.research.artifact_store),
                    recipes,
                    max_recovery_attempts: config.max_recovery_attempts,
                },
            )?),
        })) as Arc<dyn FeedbackStagePort>;
        Ok(Self::new(FeedbackCoordinatorDeps {
            cycles,
            scheduler: Arc::clone(&context.infra.repos.feedback_scheduler)
                as Arc<dyn FeedbackSchedulerRepository>,
            resolutions: Arc::clone(&context.infra.repos.resolution_observation)
                as Arc<dyn ResolutionObservationRepository>,
            attempts: Arc::clone(&context.infra.repos.execution_attempt_outcome)
                as Arc<dyn ExecutionAttemptOutcomeRepository>,
            rollups: Arc::clone(&context.infra.repos.recommendation_execution_rollup)
                as Arc<dyn RecommendationExecutionRollupRepository>,
            jobs: engine,
            stages,
            shadow_cancellation,
            metrics: Arc::clone(&context.infra.metrics),
            alerts: Arc::clone(&context.governance.alerts),
            config: FeedbackCoordinatorConfig::try_from(config)?,
        }))
    }
}

impl AppContext {
    fn build_feedback_scheduler(
        &self,
        engine: &ResearchJobEngine,
        config: &ResearchJobsConfig,
    ) -> QuantResult<FeedbackScheduler> {
        let runtime_config =
            Arc::clone(&self.infra.repos.runtime_config) as Arc<dyn PolicyRepository>;
        let bias_tables = Arc::clone(&self.infra.repos.calibration_artifact)
            as Arc<dyn CalibrationArtifactRepository>;
        let training_datasets = Arc::new(CoreTrainingDatasetPort::from_research(
            &self.research,
            runtime_config,
            bias_tables,
            config.max_spine_samples,
            config.plan_sample_slices,
            config.plan_sample_markets,
        ));
        let mutation = Arc::new(CoreFeedbackMutationPort::new(CoreFeedbackMutationDeps {
            cycles: Arc::clone(&self.infra.repos.feedback_cycle)
                as Arc<dyn FeedbackCycleRepository>,
            scheduler: Arc::clone(&self.infra.repos.feedback_scheduler)
                as Arc<dyn FeedbackSchedulerRepository>,
            permits: Arc::clone(&self.infra.repos.promotion_permit)
                as Arc<dyn PromotionPermitRepository>,
            permit_service: Arc::clone(&self.governance.promotion_permits),
            promotion_preflight: Arc::clone(&self.research.promotion_preflight),
            serving_preimages: Arc::clone(&self.research.serving_preimages),
            serving_generations: Arc::clone(&self.research.serving_generations),
            route_governance: Arc::clone(&self.research.model_route_governance),
            resolutions: Arc::clone(&self.infra.repos.resolution_observation)
                as Arc<dyn ResolutionObservationRepository>,
            training_datasets,
            feedback_wake: engine.feedback_wake(),
            shutdown: self.shutdown.clone(),
            metrics: Arc::clone(&self.infra.metrics),
        }));
        let lease_secs = u64::try_from(config.lease_ttl_secs).map_err(|error| {
            FeedbackError::InvalidCoordinatorState {
                detail: format!("feedback scheduler lease does not fit u64 seconds: {error}"),
            }
        })?;
        Ok(FeedbackScheduler::new(
            Arc::clone(&self.infra.repos.feedback_scheduler)
                as Arc<dyn FeedbackSchedulerRepository>,
            mutation,
            *engine.instance_id(),
            FeedbackSchedulerConfig::try_new(config.poll_secs, lease_secs)?,
        ))
    }

    /// Register the one durable research worker/coordinator runtime.
    pub fn register_research_runtime(
        &self,
        runner: &mut AppRunner,
        engine: ResearchJobEngine,
    ) -> QuantResult<()> {
        let config = Arc::new(self.config.quant.research_jobs);
        let executor = ResearchJobExecutor::from_context(self, config.as_ref())?;
        let coordinator = FeedbackCoordinator::from_context(self, engine.clone(), config.as_ref())?;
        let scheduler = self.build_feedback_scheduler(&engine, config.as_ref())?;
        let metrics = Arc::clone(&self.infra.metrics);
        let stop_intake = self.shutdown.clone();
        let worker_engine = engine;
        let worker_config = Arc::clone(&config);
        runner.spawn(TaskId::ResearchJobWorker, move |stage_token| async move {
            // Root cancellation stops leasing immediately. The stage token
            // remains a bounded-drain backstop when the registry reaches
            // Analytics; both converge on the one token observed by leases and
            // in-flight jobs.
            let worker_shutdown = CancellationToken::new();
            let worker = run_worker(
                worker_engine,
                executor,
                worker_config,
                metrics,
                worker_shutdown.clone(),
            );
            tokio::pin!(worker);
            tokio::select! {
                () = stop_intake.cancelled() => {}
                () = stage_token.cancelled() => {}
                () = &mut worker => return,
            }
            worker_shutdown.cancel();
            worker.await;
        });
        runner.spawn(TaskId::FeedbackCoordinator, move |token| async move {
            coordinator.run(token).await;
        });
        runner.spawn(TaskId::FeedbackScheduler, move |token| async move {
            scheduler.run(token).await;
        });
        Ok(())
    }
}

async fn run_worker(
    engine: ResearchJobEngine,
    executor: ResearchJobExecutor,
    config: Arc<ResearchJobsConfig>,
    metrics: Arc<MetricsHub>,
    token: CancellationToken,
) {
    let poll = Duration::from_secs(config.poll_secs);
    // Boot recovery: reclaim orphaned `running` rows before leasing anything new.
    match engine
        .repo()
        .reclaim_orphaned(engine.instance_id(), Utc::now())
        .await
    {
        Ok(outcome) if outcome.requeued > 0 || outcome.quarantined > 0 => info!(
            requeued = outcome.requeued,
            quarantined = outcome.quarantined,
            "research-job boot recovery reclaimed orphaned runs",
        ),
        Ok(_) => {}
        Err(error) => warn!(%error, "research-job boot recovery sweep failed"),
    }

    let mut tasks: JoinSet<()> = JoinSet::new();
    let inflight = Arc::new(InflightTracker::default());

    loop {
        if token.is_cancelled() {
            break;
        }
        drain_finished(&mut tasks);

        let eligible = inflight.eligible(config.as_ref());
        if eligible.is_empty() {
            wait_for_slot(&token, &mut tasks, poll).await;
            continue;
        }

        let lease = engine
            .repo()
            .lease_next(
                &eligible,
                engine.instance_id(),
                lease_deadline(config.lease_ttl_secs),
            )
            .await;
        match lease {
            Ok(leased) => match LeaseDecision::resolve(&token, leased) {
                // A lease won concurrently with root cancellation remains owned
                // by this boot epoch and is reclaimed by `requeue_inflight`
                // below without ever spawning or publishing a false start.
                LeaseDecision::Stop => break,
                LeaseDecision::Idle => {
                    wait_for_slot(&token, &mut tasks, poll).await;
                }
                LeaseDecision::Run(job) => {
                    let permit = InflightTracker::acquire(&inflight, job.kind);
                    let engine = engine.clone();
                    let executor = executor.clone();
                    let metrics = Arc::clone(&metrics);
                    let shutdown = token.clone();
                    let config = Arc::clone(&config);
                    tasks.spawn(async move {
                        let _permit = permit;
                        run_one(engine, executor, job, config, metrics, shutdown).await;
                    });
                }
            },
            Err(_) if token.is_cancelled() => break,
            Err(error) => {
                warn!(%error, "research-job lease failed; backing off");
                sleep_or_cancel(&token, poll).await;
            }
        }
    }

    // Graceful shutdown: leasing already stopped (the loop broke on `token`).
    // Cooperatively **drain** in-flight runs rather than aborting them — each
    // `run_one` observes `shutdown.cancelled` and unwinds at its next section
    // boundary, deliberately leaving its row `running`. Bound the wait so a
    // stuck build cannot stall the deploy; then explicitly re-queue this owner's
    // still-`running` rows so the next epoch re-leases them immediately, instead
    // of waiting a full `lease_ttl_secs` for the boot sweep to reclaim them.
    let drain = Duration::from_secs(config.shutdown_drain_secs);
    if tokio::time::timeout(drain, async { while tasks.join_next().await.is_some() {} })
        .await
        .is_err()
    {
        warn!(
            drain_secs = config.shutdown_drain_secs,
            "research-job graceful drain timed out; re-queueing in-flight runs anyway"
        );
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    match engine.repo().requeue_inflight(engine.instance_id()).await {
        Ok(outcome) if outcome.requeued > 0 || outcome.quarantined > 0 => info!(
            requeued = outcome.requeued,
            quarantined = outcome.quarantined,
            "research-job graceful shutdown re-queued in-flight runs",
        ),
        Ok(_) => {}
        Err(error) => warn!(%error, "research-job shutdown requeue_inflight failed"),
    }
    info!("research-job worker stopped");
}

enum LeaseDecision<T> {
    Stop,
    Idle,
    Run(T),
}

impl<T> LeaseDecision<T> {
    fn resolve(shutdown: &CancellationToken, leased: Option<T>) -> Self {
        if shutdown.is_cancelled() {
            Self::Stop
        } else {
            leased.map_or(Self::Idle, Self::Run)
        }
    }
}

struct InflightTracker {
    total: AtomicUsize,
    by_kind: HashMap<ResearchJobKind, AtomicUsize>,
}

impl Default for InflightTracker {
    fn default() -> Self {
        Self {
            total: AtomicUsize::new(0),
            by_kind: ALL_KINDS
                .into_iter()
                .map(|kind| (kind, AtomicUsize::new(0)))
                .collect(),
        }
    }
}

impl InflightTracker {
    fn acquire(tracker: &Arc<Self>, kind: ResearchJobKind) -> InflightPermit {
        tracker.total.fetch_add(1, Ordering::AcqRel);
        if let Some(count) = tracker.by_kind.get(&kind) {
            count.fetch_add(1, Ordering::AcqRel);
        }
        InflightPermit {
            tracker: Arc::clone(tracker),
            kind,
        }
    }

    fn total(&self) -> usize {
        self.total.load(Ordering::Acquire)
    }

    fn kind(&self, kind: ResearchJobKind) -> usize {
        self.by_kind
            .get(&kind)
            .map_or(0, |count| count.load(Ordering::Acquire))
    }

    fn eligible(&self, config: &ResearchJobsConfig) -> Vec<ResearchJobKind> {
        if self.total() >= config.global_concurrency {
            return Vec::new();
        }
        ALL_KINDS
            .into_iter()
            .filter(|kind| self.kind(*kind) < config.kind_concurrency(*kind))
            .collect()
    }
}

struct InflightPermit {
    tracker: Arc<InflightTracker>,
    kind: ResearchJobKind,
}

impl Drop for InflightPermit {
    fn drop(&mut self) {
        self.tracker.total.fetch_sub(1, Ordering::AcqRel);
        if let Some(count) = self.tracker.by_kind.get(&self.kind) {
            count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

fn drain_finished(tasks: &mut JoinSet<()>) {
    while let Some(joined) = tasks.try_join_next() {
        if let Err(join_error) = joined {
            error!(%join_error, "research-job supervisor task exited unexpectedly");
        }
    }
}

async fn wait_for_slot(token: &CancellationToken, tasks: &mut JoinSet<()>, poll: Duration) {
    if tasks.is_empty() {
        sleep_or_cancel(token, poll).await;
        return;
    }
    tokio::select! {
        () = token.cancelled() => {}
        joined = tasks.join_next() => {
            if let Some(Err(join_error)) = joined {
                error!(%join_error, "research-job supervisor task exited unexpectedly");
            }
        }
        () = tokio::time::sleep(poll) => {}
    }
}

async fn sleep_or_cancel(token: &CancellationToken, duration: Duration) {
    tokio::select! {
        () = token.cancelled() => {}
        () = tokio::time::sleep(duration) => {}
    }
}

impl ResearchJobEngine {
    async fn renew_job_lease(
        &self,
        job: &ResearchJobInfo,
        config: &ResearchJobsConfig,
        progress: Option<ResearchJobProgress>,
        metrics: &MetricsHub,
    ) -> bool {
        let source = if progress.is_some() {
            "progress"
        } else {
            "periodic"
        };
        let surfaced = progress
            .as_ref()
            .map(|progress| (progress.phase.clone(), progress.pct()));
        match self
            .repo()
            .heartbeat(
                &job.job_id,
                self.instance_id(),
                lease_deadline(config.lease_ttl_secs),
                progress,
            )
            .await
        {
            Ok(true) => {
                metrics.record_research_heartbeat(source, "renewed");
                if let Some((phase, pct)) = surfaced {
                    self.publish_progress(
                        &job.job_id,
                        job.kind,
                        None,
                        ResearchJobStatus::Running,
                        Some(phase),
                        pct,
                    );
                }
                true
            }
            Ok(false) => {
                metrics.record_research_heartbeat(source, "lease_lost");
                false
            }
            Err(error) => {
                metrics.record_research_heartbeat(source, "storage_error");
                warn!(job_id = %job.job_id, %error, source, "research-job heartbeat failed closed");
                false
            }
        }
    }

    async fn resume_job_terminal(
        &self,
        executor: &ResearchJobExecutor,
        job: &ResearchJobInfo,
        config: &ResearchJobsConfig,
        metrics: &MetricsHub,
    ) -> bool {
        let resumes_operator_cancel = job
            .error_json
            .as_ref()
            .is_some_and(|error| error.message.starts_with(OPERATOR_CANCEL_TERMINAL_PREFIX))
            || job
                .progress_json
                .as_ref()
                .is_some_and(|progress| progress.phase == OPERATOR_CANCEL_TERMINAL_PREFIX);
        if resumes_operator_cancel {
            let terminal = executor
                .resolve_operator_terminal(job, Terminal::Cancelled)
                .await;
            settle(self, executor, job, terminal, config, metrics).await;
            return true;
        }
        if job
            .progress_json
            .as_ref()
            .is_some_and(|progress| progress.phase == RETRY_EXHAUSTION_TERMINAL_PHASE)
        {
            self.settle_retry(
                executor,
                job,
                "retry exhaustion model-run terminalization".to_owned(),
                config,
                metrics,
            )
            .await;
            return true;
        }
        false
    }
}

/// Supervise one leased job: spawn its execution (which offloads CPU-bound work
/// to the governed executor and polls `cancel`), then `select!` over completion,
/// throttled progress-heartbeats drained from the sink channel, periodic lease
/// renewal, and graceful shutdown.
enum SupervisorEvent<T> {
    Completed(T),
    Progress(bool),
    Heartbeat,
    OperatorCancel,
    Shutdown,
}

impl<T> SupervisorEvent<T> {
    async fn next<F>(
        mut execution: Pin<&mut F>,
        progress_rx: &mut Receiver<Option<ResearchJobProgress>>,
        progress_open: bool,
        heartbeat: &mut Interval,
        cancel: &CancellationToken,
        operator_cancel_draining: bool,
        shutdown: &CancellationToken,
    ) -> Self
    where
        F: Future<Output = T>,
    {
        tokio::select! {
            result = execution.as_mut() => Self::Completed(result),
            changed = progress_rx.changed(), if progress_open => {
                Self::Progress(changed.is_ok())
            }
            _ = heartbeat.tick() => Self::Heartbeat,
            () = cancel.cancelled(), if !operator_cancel_draining => Self::OperatorCancel,
            () = shutdown.cancelled() => Self::Shutdown,
        }
    }
}

async fn run_one(
    engine: ResearchJobEngine,
    executor: ResearchJobExecutor,
    job: ResearchJobInfo,
    config: Arc<ResearchJobsConfig>,
    metrics: Arc<MetricsHub>,
    shutdown: CancellationToken,
) {
    let job_id = job.job_id;
    if shutdown.is_cancelled() {
        return;
    }
    if engine
        .resume_job_terminal(&executor, &job, config.as_ref(), metrics.as_ref())
        .await
    {
        return;
    }
    let cancel = CancellationToken::new();
    engine.register_cancel(&job_id, cancel.clone());
    engine.publish_progress(
        &job_id,
        job.kind,
        None,
        ResearchJobStatus::Running,
        Some("start".to_owned()),
        None,
    );

    // Synchronous latest-value slot: blocking execution can publish without an
    // await, while bursty intermediate snapshots are structurally coalesced.
    let (tx, mut progress_rx) = watch::channel::<Option<ResearchJobProgress>>(None);
    let progress_slot = tx.clone();
    let sink: Arc<dyn JobProgressSink> = Arc::new(ChannelProgressSink {
        tx,
        metrics: Arc::clone(&metrics),
    });
    let execution = executor.execute(&job, sink, cancel.clone());
    tokio::pin!(execution);

    let mut throttle =
        ProgressThrottle::new(Duration::from_millis(config.progress_min_interval_ms));
    let mut heartbeat = interval(Duration::from_secs(config.heartbeat_secs));
    heartbeat.tick().await; // consume the immediate first tick
    let mut progress_open = true;
    let mut operator_cancel_draining = false;

    let terminal = loop {
        match SupervisorEvent::next(
            execution.as_mut(),
            &mut progress_rx,
            progress_open,
            &mut heartbeat,
            &cancel,
            operator_cancel_draining,
            &shutdown,
        )
        .await
        {
            SupervisorEvent::Completed(result) => break Terminal::from_result(result),
            SupervisorEvent::Progress(open) => {
                if !open {
                    progress_open = false;
                    continue;
                }
                drop(progress_rx.borrow_and_update());
                let mut latest = None;
                progress_slot.send_if_modified(|slot| {
                    latest = slot.take();
                    false
                });
                let Some(progress) = latest else {
                    continue;
                };
                // Coalesce bursty per-section reports; each surfaced report also
                // renews the lease (doubles as a liveness heartbeat).
                if throttle.should_emit()
                    && !engine
                        .renew_job_lease(&job, config.as_ref(), Some(progress), metrics.as_ref())
                        .await
                {
                    cancel.cancel();
                    let _ = (&mut execution).await;
                    engine.clear_cancel(&job_id);
                    return;
                }
            }
            SupervisorEvent::Heartbeat => {
                if !engine
                    .renew_job_lease(&job, config.as_ref(), None, metrics.as_ref())
                    .await
                {
                    cancel.cancel();
                    let _ = (&mut execution).await;
                    engine.clear_cancel(&job_id);
                    return;
                }
            }
            SupervisorEvent::OperatorCancel => {
                // Keep the normal supervisor loop alive while the operation
                // drains. Progress and periodic heartbeats continue renewing
                // this exact lease, so a committed result can still win the
                // cancellation race without becoming a stale finalization.
                operator_cancel_draining = true;
            }
            SupervisorEvent::Shutdown => {
                // Signal cooperative cancellation and leave the row `running`.
                // The worker owns the one global drain deadline; after it expires
                // the task is aborted before the repository re-queues this epoch.
                cancel.cancel();
                let _ = (&mut execution).await;
                engine.clear_cancel(&job_id);
                return;
            }
        }
    };

    let terminal = if operator_cancel_draining {
        executor.resolve_operator_terminal(&job, terminal).await
    } else {
        terminal
    };

    settle(
        &engine,
        &executor,
        &job,
        terminal,
        config.as_ref(),
        &metrics,
    )
    .await;
    engine.clear_cancel(&job_id);
}

enum Terminal {
    Succeeded(Box<JobOutcome>),
    AwaitingEvidence {
        progress: ResearchJobProgress,
        retry_after: Duration,
    },
    Failed(String),
    Retryable(String),
    Cancelled,
}

impl Terminal {
    fn after_operator_cancel(self) -> Self {
        match self {
            Self::Succeeded(outcome) => Self::Succeeded(outcome),
            Self::Failed(message) => Self::Failed(message),
            Self::AwaitingEvidence { .. } | Self::Retryable(_) | Self::Cancelled => Self::Cancelled,
        }
    }

    fn from_result(result: QuantResult<JobDisposition>) -> Self {
        match result {
            Ok(JobDisposition::Completed(outcome)) => Self::Succeeded(outcome),
            Ok(JobDisposition::AwaitingEvidence {
                progress,
                retry_after,
            }) => Self::AwaitingEvidence {
                progress,
                retry_after,
            },
            // A cooperative cancel funnels through the build token and surfaces
            // as a terminal `Cancelled` (not a failure).
            Err(QuantError::Research(ResearchError::Cancelled { .. })) => Self::Cancelled,
            Err(error) if is_transient_error(&error) => Self::Retryable(error.to_string()),
            Err(error) => Self::Failed(error.to_string()),
        }
    }
}

fn is_transient_error(error: &QuantError) -> bool {
    match error {
        QuantError::Research(ResearchError::ArtifactTransport { .. }) => true,
        QuantError::Storage(error) => error.is_transient(),
        _ => false,
    }
}

async fn settle(
    engine: &ResearchJobEngine,
    executor: &ResearchJobExecutor,
    job: &ResearchJobInfo,
    terminal: Terminal,
    config: &ResearchJobsConfig,
    metrics: &MetricsHub,
) {
    let job_id = job.job_id;
    let kind = job.kind;
    let finalization = match terminal {
        Terminal::Succeeded(outcome) => {
            ResearchJobFinalization::succeeded(outcome.result, outcome.artifact, outcome.coverage)
        }
        Terminal::Failed(message) => {
            warn!(
                %job_id,
                kind = %kind,
                error = %message,
                "research job execution failed"
            );
            ResearchJobFinalization::failed(ResearchJobError::new(
                ResearchJobErrorCode::ExecutionFailed,
                message,
            ))
        }
        Terminal::AwaitingEvidence {
            progress,
            retry_after,
        } => {
            engine
                .settle_evidence_wait(job, progress, retry_after, metrics)
                .await;
            return;
        }
        Terminal::Retryable(message) => {
            engine
                .settle_retry(executor, job, message, config, metrics)
                .await;
            return;
        }
        Terminal::Cancelled => ResearchJobFinalization::cancelled(ResearchJobError::new(
            ResearchJobErrorCode::Cancelled,
            "cancelled by operator",
        )),
    };
    engine.finalize_job(job, finalization).await;
}

impl ResearchJobEngine {
    async fn finalize_job(&self, job: &ResearchJobInfo, finalization: ResearchJobFinalization) {
        let status = finalization.status();
        match self
            .repo()
            .finalize(&job.job_id, self.instance_id(), finalization)
            .await
        {
            Ok(info) => {
                let pct = if status == ResearchJobStatus::Succeeded {
                    Some(1.0)
                } else {
                    None
                };
                self.publish(&info, Some("finalize".to_owned()), pct);
            }
            Err(StorageError::StateConflict { .. }) => {
                warn!(
                    job_id = %job.job_id,
                    kind = %job.kind,
                    "stale worker skipped finalize after lease loss or double-finalize"
                );
            }
            Err(error) => error!(%error, kind = %job.kind, "failed to finalize research job"),
        }
    }

    async fn settle_evidence_wait(
        &self,
        job: &ResearchJobInfo,
        progress: ResearchJobProgress,
        retry_after: Duration,
        metrics: &MetricsHub,
    ) {
        let phase = progress.phase.clone();
        match self
            .repo()
            .await_evidence(&job.job_id, self.instance_id(), progress, retry_after)
            .await
        {
            Ok(info) => {
                metrics.record_research_heartbeat("evidence_wait", "scheduled");
                self.publish(&info, Some(phase), None);
            }
            Err(StorageError::StateConflict { .. }) => {
                metrics.record_research_heartbeat("evidence_wait", "lease_lost");
                warn!(
                    job_id = %job.job_id,
                    kind = %job.kind,
                    "stale worker skipped evidence wait after lease loss"
                );
            }
            Err(error) => {
                metrics.record_research_heartbeat("evidence_wait", "storage_error");
                error!(
                    job_id = %job.job_id,
                    kind = %job.kind,
                    %error,
                    "failed to persist research-job evidence wait"
                );
            }
        }
    }

    async fn prepare_operator_retry(
        &self,
        executor: &ResearchJobExecutor,
        job: &ResearchJobInfo,
        message: String,
        config: &ResearchJobsConfig,
        metrics: &MetricsHub,
    ) -> RetryPreparation {
        let exhausted = job.recovery_attempt >= job.max_recovery_attempts;
        match executor.cancel_job_runs(job).await {
            Ok(JobRunFinalization::Terminalized) => {
                self.finalize_job(
                    job,
                    ResearchJobFinalization::cancelled(ResearchJobError::new(
                        ResearchJobErrorCode::Cancelled,
                        "cancelled by operator",
                    )),
                )
                .await;
                RetryPreparation::Complete
            }
            Ok(JobRunFinalization::CommitWon) => match executor.recover_job_commit(job).await {
                Ok(outcome) => {
                    self.finalize_job(
                        job,
                        ResearchJobFinalization::succeeded(
                            outcome.result,
                            outcome.artifact,
                            outcome.coverage,
                        ),
                    )
                    .await;
                    RetryPreparation::Complete
                }
                Err(error) if !exhausted => RetryPreparation::Continue(format!(
                    "{COMMITTED_MODEL_READBACK_PREFIX}: {error}"
                )),
                Err(error) => {
                    let _ = self
                        .renew_job_lease(
                            job,
                            config,
                            Some(ResearchJobProgress::indeterminate(
                                COMMITTED_MODEL_READBACK_PREFIX,
                                0,
                            )),
                            metrics,
                        )
                        .await;
                    error!(
                        job_id = %job.job_id,
                        kind = %job.kind,
                        %error,
                            "committed model-producing read-back remains unavailable at retry cap"
                    );
                    RetryPreparation::Complete
                }
            },
            Err(error) if is_transient_error(&error) && !exhausted => {
                RetryPreparation::Continue(message)
            }
            Err(error) if is_transient_error(&error) => {
                let _ = self
                    .renew_job_lease(
                        job,
                        config,
                        Some(ResearchJobProgress::indeterminate(
                            OPERATOR_CANCEL_TERMINAL_PREFIX,
                            0,
                        )),
                        metrics,
                    )
                    .await;
                error!(
                    job_id = %job.job_id,
                    kind = %job.kind,
                    %error,
                    "operator model-run terminalization remains unavailable at retry cap"
                );
                RetryPreparation::Complete
            }
            Err(error) => {
                self.finalize_job(
                    job,
                    ResearchJobFinalization::failed(ResearchJobError::new(
                        ResearchJobErrorCode::ExecutionFailed,
                        format!("operator model-run terminalization failed: {error}"),
                    )),
                )
                .await;
                RetryPreparation::Complete
            }
        }
    }

    async fn prepare_exhausted_retry(
        &self,
        executor: &ResearchJobExecutor,
        job: &ResearchJobInfo,
        message: &str,
        config: &ResearchJobsConfig,
        metrics: &MetricsHub,
    ) -> bool {
        match executor
            .fail_job_runs(
                job,
                format!("research job transient retries exhausted: {message}"),
            )
            .await
        {
            Ok(JobRunFinalization::Terminalized) => false,
            Ok(JobRunFinalization::CommitWon) => {
                match executor.recover_job_commit(job).await {
                    Ok(outcome) => {
                        metrics.record_research_heartbeat("execution_retry", "commit_recovered");
                        self.finalize_job(
                            job,
                            ResearchJobFinalization::succeeded(
                                outcome.result,
                                outcome.artifact,
                                outcome.coverage,
                            ),
                        )
                        .await;
                    }
                    Err(error) => {
                        metrics
                            .record_research_heartbeat("execution_retry", "commit_readback_wait");
                        let _ = self
                            .renew_job_lease(
                                job,
                                config,
                                Some(ResearchJobProgress::indeterminate(
                                    COMMITTED_MODEL_READBACK_PREFIX,
                                    0,
                                )),
                                metrics,
                            )
                            .await;
                        warn!(
                            job_id = %job.job_id,
                            kind = %job.kind,
                            %error,
                            "committed model-producing result awaits exact retry-exhaustion read-back"
                        );
                    }
                }
                true
            }
            Err(error) => {
                let _ = self
                    .renew_job_lease(
                        job,
                        config,
                        Some(ResearchJobProgress::indeterminate(
                            RETRY_EXHAUSTION_TERMINAL_PHASE,
                            0,
                        )),
                        metrics,
                    )
                    .await;
                metrics.record_research_heartbeat("execution_retry", "model_run_terminal_error");
                error!(
                    job_id = %job.job_id,
                    kind = %job.kind,
                    %error,
                    "research job retry exhaustion could not terminalize its model run"
                );
                true
            }
        }
    }

    async fn settle_retry(
        &self,
        executor: &ResearchJobExecutor,
        job: &ResearchJobInfo,
        mut message: String,
        config: &ResearchJobsConfig,
        metrics: &MetricsHub,
    ) {
        let job_id = job.job_id;
        let kind = job.kind;
        let retry_after = Duration::from_secs(config.execution_retry_delay(job.recovery_attempt));
        let retries_exhausted = job.recovery_attempt >= job.max_recovery_attempts;
        let operator_terminalization = message.starts_with(OPERATOR_CANCEL_TERMINAL_PREFIX);
        if operator_terminalization {
            match self
                .prepare_operator_retry(executor, job, message, config, metrics)
                .await
            {
                RetryPreparation::Continue(next) => message = next,
                RetryPreparation::Complete => return,
            }
        }
        if retries_exhausted
            && !operator_terminalization
            && self
                .prepare_exhausted_retry(executor, job, &message, config, metrics)
                .await
        {
            return;
        }
        match self
            .repo()
            .retry_transient(&job_id, self.instance_id(), message.clone(), retry_after)
            .await
        {
            Ok(ResearchJobRetryOutcome::Scheduled(info)) => {
                metrics.record_research_heartbeat("execution_retry", "scheduled");
                warn!(
                    %job_id,
                    kind = %kind,
                    attempt = info.recovery_attempt,
                    max_attempts = info.max_recovery_attempts,
                    next_attempt_at = ?info.next_attempt_at,
                    error = %message,
                    "research job transient failure scheduled for durable retry"
                );
                self.publish(&info, Some("retry_scheduled".to_owned()), None);
            }
            Ok(ResearchJobRetryOutcome::Exhausted(info)) => {
                metrics.record_research_heartbeat("execution_retry", "exhausted");
                warn!(
                    %job_id,
                    kind = %kind,
                    attempts = info.recovery_attempt,
                    error = %message,
                    "research job transient failures exhausted automatic retries"
                );
                self.publish(&info, Some("retry_exhausted".to_owned()), None);
            }
            Err(StorageError::StateConflict { .. }) => {
                metrics.record_research_heartbeat("execution_retry", "lease_lost");
                warn!(
                    %job_id,
                    kind = %kind,
                    "stale worker skipped transient retry after lease loss"
                );
            }
            Err(error) => {
                metrics.record_research_heartbeat("execution_retry", "storage_error");
                error!(%error, kind = %kind, "failed to persist research-job retry");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use quant_pivot_error::{QuantResult, research::ResearchError};
    use quant_pivot_models::{
        config::ResearchJobsConfig,
        domain::{
            api::{
                FitModelCalibratorRequest, GatePreviewIntent, ModelCalibrationFitPreflightView,
                QualityGateReportView,
            },
            ports::{
                BootstrapQualityGateEvidence, BootstrapQualityGateInput,
                CalibratedModelSealCommand, CalibrationRunFinalization,
                CandidateQualityGateEvidence, GovernanceActor, ModelCalibrationFitJobParams,
                ModelCalibrationFitOutcome, ModelCalibrationFitPort, ModelGovernancePort,
            },
            quant::{JobProgressSink, ModelVersionInfo, NoopProgressSink, ResearchJobResultRef},
        },
        enums::quant::{CalibrationMethod, DownsideSource, ResearchJobKind, ResearchJobResultKind},
        types::{
            BacktestReportId, CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId,
            ModelRunId, ModelVersionId, ResearchJobProgress, TrainingDatasetId,
            model_quality::QualityGateReport,
        },
    };
    use tokio::{
        sync::{oneshot, watch},
        task::JoinSet,
        time::interval,
    };
    use tokio_util::sync::CancellationToken;

    use super::{
        ChannelProgressSink, InflightTracker, LeaseDecision, MetricsHub,
        ModelCalibrationJobExecutor, SupervisorEvent, Terminal, is_transient_error,
    };
    use crate::service::model_serving_test_support::{model_artifact, model_version};

    struct CalibrationFitFixture {
        outcome: ModelCalibrationFitOutcome,
    }

    #[test]
    fn cancelled_lease_stops() {
        let shutdown = CancellationToken::new();
        assert!(matches!(
            LeaseDecision::resolve(&shutdown, Some(7_u8)),
            LeaseDecision::Run(7)
        ));
        assert!(matches!(
            LeaseDecision::<u8>::resolve(&shutdown, None),
            LeaseDecision::Idle
        ));
        shutdown.cancel();

        assert!(matches!(
            LeaseDecision::resolve(&shutdown, Some(7_u8)),
            LeaseDecision::Stop
        ));
    }

    #[test]
    fn operator_cancel_beats_retry() {
        let terminal = Terminal::Retryable("object transport retry".to_owned());
        assert!(matches!(
            terminal.after_operator_cancel(),
            Terminal::Cancelled
        ));
    }

    #[tokio::test]
    async fn join_failure_releases_slots() {
        let inflight = Arc::new(InflightTracker::default());
        let kind = ResearchJobKind::ModelTrain;
        let mut tasks: JoinSet<()> = JoinSet::new();
        let panic_permit = InflightTracker::acquire(&inflight, kind);
        tasks.spawn(async move {
            let _permit = panic_permit;
            panic!("research worker panic fixture");
        });
        assert!(
            tasks
                .join_next()
                .await
                .is_some_and(|joined| joined.is_err())
        );
        assert_eq!(inflight.total(), 0);
        assert_eq!(inflight.kind(kind), 0);

        let abort_permit = InflightTracker::acquire(&inflight, kind);
        tasks.spawn(async move {
            let _permit = abort_permit;
            future::pending::<()>().await;
        });
        tasks.abort_all();
        assert!(
            tasks
                .join_next()
                .await
                .is_some_and(|joined| joined.is_err())
        );
        assert_eq!(inflight.total(), 0);
        assert_eq!(inflight.kind(kind), 0);
        assert!(
            inflight
                .eligible(&ResearchJobsConfig::default())
                .contains(&kind)
        );
    }

    #[tokio::test]
    async fn cancel_drain_keeps_events() {
        let cancel = CancellationToken::new();
        let shutdown = CancellationToken::new();
        let (progress_tx, mut progress_rx) = watch::channel(None);
        let (complete_tx, complete_rx) = oneshot::channel();
        let execution = complete_rx;
        tokio::pin!(execution);
        let mut heartbeat = interval(Duration::from_mins(1));
        heartbeat.tick().await;

        cancel.cancel();
        assert!(matches!(
            SupervisorEvent::next(
                execution.as_mut(),
                &mut progress_rx,
                true,
                &mut heartbeat,
                &cancel,
                false,
                &shutdown,
            )
            .await,
            SupervisorEvent::OperatorCancel
        ));

        progress_tx.send_replace(Some(ResearchJobProgress::with_total("draining", 1, 2)));
        assert!(matches!(
            SupervisorEvent::next(
                execution.as_mut(),
                &mut progress_rx,
                true,
                &mut heartbeat,
                &cancel,
                true,
                &shutdown,
            )
            .await,
            SupervisorEvent::Progress(true)
        ));
        heartbeat.reset_after(Duration::from_millis(1));
        assert!(matches!(
            tokio::time::timeout(
                Duration::from_millis(100),
                SupervisorEvent::next(
                    execution.as_mut(),
                    &mut progress_rx,
                    true,
                    &mut heartbeat,
                    &cancel,
                    true,
                    &shutdown,
                ),
            )
            .await
            .expect("cancel drain must retain periodic heartbeat"),
            SupervisorEvent::Heartbeat
        ));

        complete_tx
            .send(7_u8)
            .expect("complete cancel-drain execution fixture");
        assert!(matches!(
            SupervisorEvent::next(
                execution.as_mut(),
                &mut progress_rx,
                true,
                &mut heartbeat,
                &cancel,
                true,
                &shutdown,
            )
            .await,
            SupervisorEvent::Completed(Ok(7))
        ));
    }

    #[async_trait]
    impl ModelCalibrationFitPort for CalibrationFitFixture {
        async fn fit(
            &self,
            _params: ModelCalibrationFitJobParams,
            _progress: Arc<dyn JobProgressSink>,
            _cancel: CancellationToken,
        ) -> QuantResult<ModelCalibrationFitOutcome> {
            Ok(self.outcome)
        }

        async fn cancel_run(
            &self,
            _model_run_id: &ModelRunId,
            _reason: String,
        ) -> QuantResult<CalibrationRunFinalization> {
            Ok(CalibrationRunFinalization::Terminalized)
        }

        async fn fail_run(
            &self,
            _model_run_id: &ModelRunId,
            _reason: String,
        ) -> QuantResult<CalibrationRunFinalization> {
            Ok(CalibrationRunFinalization::Terminalized)
        }

        async fn preflight(
            &self,
            _model_version_id: &ModelVersionId,
            _calibration_dataset_id: &TrainingDatasetId,
        ) -> QuantResult<ModelCalibrationFitPreflightView> {
            Err(ResearchError::Serialization {
                detail: "unexpected calibration preflight in job-executor fixture".to_owned(),
            }
            .into())
        }
    }

    struct ModelGovernanceFixture {
        calibrated: ModelVersionInfo,
        sealed: Mutex<Option<(ModelVersionId, CalibratedModelSealCommand, GovernanceActor)>>,
    }

    impl ModelGovernanceFixture {
        fn unexpected<T>() -> QuantResult<T> {
            Err(ResearchError::Serialization {
                detail: "unexpected quality-gate call in calibration job fixture".to_owned(),
            }
            .into())
        }
    }

    #[async_trait]
    impl ModelGovernancePort for ModelGovernanceFixture {
        async fn preview_gate(
            &self,
            _model_version_id: &ModelVersionId,
            _intent: GatePreviewIntent,
            _backtest_report_id: Option<&BacktestReportId>,
        ) -> QuantResult<QualityGateReportView> {
            Self::unexpected()
        }

        async fn evaluate_candidate(
            &self,
            _model_version_id: &ModelVersionId,
            _evidence: CandidateQualityGateEvidence,
            _evaluated_at: DateTime<Utc>,
        ) -> QuantResult<QualityGateReport> {
            Self::unexpected()
        }

        async fn evaluate_bootstrap(
            &self,
            _model_version_id: &ModelVersionId,
            _input: BootstrapQualityGateInput,
            _evaluated_at: DateTime<Utc>,
        ) -> QuantResult<BootstrapQualityGateEvidence> {
            Self::unexpected()
        }

        async fn seal_calibrated_model(
            &self,
            model_version_id: &ModelVersionId,
            command: CalibratedModelSealCommand,
            actor: GovernanceActor,
        ) -> QuantResult<ModelVersionInfo> {
            *self.sealed.lock().expect("seal observation lock") =
                Some((*model_version_id, command, actor));
            Ok(self.calibrated.clone())
        }
    }

    fn calibration_params(
        source_model_id: ModelVersionId,
        artifact_id: CalibrationArtifactId,
    ) -> (
        ModelCalibrationFitJobParams,
        CalibratedModelSealCommand,
        GovernanceActor,
    ) {
        let actor = GovernanceActor::system();
        let command = CalibratedModelSealCommand {
            calibrator_ref: artifact_id,
            downside_source: DownsideSource::MfeMae,
            reason: "derive a governed calibrated model".to_owned(),
        };
        (
            ModelCalibrationFitJobParams {
                model_run_id: ModelRunId::from_v7(),
                request: FitModelCalibratorRequest {
                    model_version_id: source_model_id,
                    calibration_dataset_id: TrainingDatasetId::from_v7(),
                    method: CalibrationMethod::Platt,
                    reason: command.reason.clone(),
                },
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                downside_source: command.downside_source,
                actor: actor.clone(),
            },
            command,
            actor,
        )
    }

    #[tokio::test]
    async fn progress_slot_coalesces() {
        let metrics = Arc::new(MetricsHub::new());
        let (tx, mut receiver) = watch::channel(None);
        let reset = tx.clone();
        let sink = ChannelProgressSink {
            tx,
            metrics: Arc::clone(&metrics),
        };
        sink.report(ResearchJobProgress::with_total("first", 1, 3));
        sink.report(ResearchJobProgress::with_total("second", 2, 3));
        assert_eq!(metrics.research_progress_coalesced_total.get(), 1);

        receiver
            .changed()
            .await
            .expect("progress sender remains open");
        drop(receiver.borrow_and_update());
        let mut latest = None;
        reset.send_if_modified(|slot| {
            latest = slot.take();
            false
        });
        let latest = latest.expect("latest progress remains in the single slot");
        assert_eq!(latest.phase, "second");
        assert!(
            tokio::time::timeout(Duration::from_millis(10), receiver.changed())
                .await
                .is_err(),
            "consuming the progress slot must not self-notify the supervisor"
        );
        sink.report(ResearchJobProgress::with_total("third", 3, 3));
        assert_eq!(
            metrics.research_progress_coalesced_total.get(),
            1,
            "a consumed slot must not count as a superseded value"
        );
    }

    #[test]
    fn artifact_retry_is_typed() {
        let transport = ResearchError::ArtifactTransport {
            uri: "s3://evidence/object".to_owned(),
            detail: "request retries exhausted".to_owned(),
        }
        .into();
        let contract = ResearchError::ArtifactIo {
            uri: "s3://evidence/object".to_owned(),
            detail: "object lock is not configured".to_owned(),
        }
        .into();
        assert!(is_transient_error(&transport));
        assert!(!is_transient_error(&contract));
    }

    #[tokio::test]
    async fn calibration_fit_seals_version() {
        let source_model_id = ModelVersionId::from_v7();
        let artifact_id = CalibrationArtifactId::from_v7();
        let (params, expected_command, expected_actor) =
            calibration_params(source_model_id, artifact_id);
        let mut calibrated = model_version(&model_artifact(None));
        calibrated.model_version_id = ModelVersionId::from_v7();
        let calibrated_model_id = calibrated.model_version_id;
        let governance = Arc::new(ModelGovernanceFixture {
            calibrated,
            sealed: Mutex::new(None),
        });
        let executor = ModelCalibrationJobExecutor {
            fitter: Arc::new(CalibrationFitFixture {
                outcome: ModelCalibrationFitOutcome::Calibrated {
                    artifact_id,
                    sample_count: 128,
                },
            }),
            governance: Arc::clone(&governance) as Arc<dyn ModelGovernancePort>,
        };

        let outcome = executor
            .execute(params, Arc::new(NoopProgressSink), CancellationToken::new())
            .await
            .expect("successful fit must seal a calibrated model");

        assert_eq!(
            outcome.result,
            Some(ResearchJobResultRef {
                kind: ResearchJobResultKind::ModelVersion,
                id: calibrated_model_id.as_uuid(),
            })
        );
        assert!(outcome.artifact.is_none());
        assert!(outcome.coverage.is_none());
        assert_eq!(
            governance
                .sealed
                .lock()
                .expect("seal observation lock")
                .as_ref(),
            Some(&(source_model_id, expected_command, expected_actor))
        );
    }

    #[tokio::test]
    async fn underpowered_fit_stays_empty() {
        let source_model_id = ModelVersionId::from_v7();
        let artifact_id = CalibrationArtifactId::from_v7();
        let (params, _, _) = calibration_params(source_model_id, artifact_id);
        let governance = Arc::new(ModelGovernanceFixture {
            calibrated: model_version(&model_artifact(None)),
            sealed: Mutex::new(None),
        });
        let executor = ModelCalibrationJobExecutor {
            fitter: Arc::new(CalibrationFitFixture {
                outcome: ModelCalibrationFitOutcome::Insufficient {
                    sample_count: 9,
                    total_sample_count: 10,
                    minimum_sample_count: 100,
                    outcome_hash: ContentHash::from_bytes([7; 32]),
                },
            }),
            governance: Arc::clone(&governance) as Arc<dyn ModelGovernancePort>,
        };

        let outcome = executor
            .execute(params, Arc::new(NoopProgressSink), CancellationToken::new())
            .await
            .expect("underpowered fit is a typed successful computation");

        assert!(outcome.result.is_none());
        assert!(
            governance
                .sealed
                .lock()
                .expect("seal observation lock")
                .is_none()
        );
    }
}
