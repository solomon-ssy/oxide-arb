//! Exact feedback-stage adapters for coverage and statistical drift.

use std::sync::Arc;

use quant_pivot_error::{QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{FeedbackCoverageJobParams, FeedbackDriftJobParams},
        quant::{
            DriftReportInput, FeedbackCycleInfo, FeedbackStageJobIdentity, NewDriftReport,
            NewResearchJob, ResearchJobArtifactRef, ResearchJobInfo, ResearchJobResultRef,
        },
    },
    enums::quant::{
        FeedbackDecision, FeedbackStage, ResearchJobKind, ResearchJobResultKind, ResearchJobStatus,
    },
    types::{FeedbackCoverageArtifactId, FeedbackDriftArtifactId, ResearchJobParams, RoleCode},
};
use quant_pivot_repository::traits::ResearchJobRepository;
use quant_pivot_research::{
    artifact::ArtifactStore,
    feedback::{
        CoverageGateOutcome, DriftGateOutcome, FeedbackCoverageArtifact, FeedbackCoverageCodec,
        FeedbackDriftArtifact, FeedbackDriftCodec,
    },
};
use uuid::Uuid;

use crate::service::feedback_coordinator::FeedbackStageSuccess;

/// Dependencies for [`FeedbackSignalStageAdapter`].
pub struct FeedbackSignalStageDeps {
    pub jobs: Arc<dyn ResearchJobRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub max_recovery_attempts: i32,
}

/// Coverage/drift-only adapter consumed by the final closed-DAG dispatcher.
pub struct FeedbackSignalStageAdapter {
    jobs: Arc<dyn ResearchJobRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    max_recovery_attempts: i32,
}

impl FeedbackSignalStageAdapter {
    pub fn try_new(deps: FeedbackSignalStageDeps) -> Result<Self, FeedbackError> {
        if deps.max_recovery_attempts < 0 {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "feedback job recovery cap cannot be negative".to_owned(),
            });
        }
        Ok(Self {
            jobs: deps.jobs,
            artifacts: deps.artifacts,
            max_recovery_attempts: deps.max_recovery_attempts,
        })
    }

    /// Freeze the deterministic root coverage job.
    pub fn prepare_coverage(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        Self::require_identity(cycle, identity, FeedbackStage::Coverage)?;
        self.bind_job(
            identity,
            ResearchJobParams::FeedbackCoverage(FeedbackCoverageJobParams {
                feedback_cycle_id: cycle.feedback_cycle_id,
                cycle_idempotency_hash: cycle.idempotency_hash,
                artifact_id: FeedbackCoverageArtifactId::from_cycle_id(cycle.feedback_cycle_id),
            }),
        )
    }

    /// Freeze drift params against the exact successful coverage artifact.
    pub async fn prepare_drift(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        Self::require_identity(cycle, identity, FeedbackStage::Drift)?;
        let coverage_identity =
            FeedbackStageJobIdentity::try_root(cycle.feedback_cycle_id, FeedbackStage::Coverage)?;
        let coverage_job = self
            .jobs
            .find_by_id(&coverage_identity.job_id())
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_research_job", coverage_identity.job_id())
            })?;
        let coverage_id = FeedbackCoverageArtifactId::from_cycle_id(cycle.feedback_cycle_id);
        let artifact = Self::require_result(
            cycle,
            &coverage_job,
            FeedbackStage::Coverage,
            ResearchJobKind::FeedbackCoverage,
            ResearchJobResultKind::FeedbackCoverageArtifact,
            coverage_id.as_uuid(),
        )?;
        let coverage = self.load_coverage(cycle, &coverage_job, &artifact).await?;
        if !matches!(coverage.gate_outcome, CoverageGateOutcome::Advance { .. }) {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "drift stage cannot follow a terminal coverage NoAction".to_owned(),
            }
            .into());
        }
        self.bind_job(
            identity,
            ResearchJobParams::FeedbackDrift(FeedbackDriftJobParams {
                feedback_cycle_id: cycle.feedback_cycle_id,
                cycle_idempotency_hash: cycle.idempotency_hash,
                artifact_id: FeedbackDriftArtifactId::from_cycle_id(cycle.feedback_cycle_id),
                coverage_job_id: coverage_job.job_id,
                coverage_artifact_id: coverage_id,
                coverage_artifact_uri: artifact.uri,
                coverage_artifact_hash: artifact.content_hash,
            }),
        )
    }

    /// Validate a terminal coverage result and derive its durable directive.
    pub async fn succeeded_coverage(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        let artifact_id = FeedbackCoverageArtifactId::from_cycle_id(cycle.feedback_cycle_id);
        let result = Self::require_result(
            cycle,
            job,
            FeedbackStage::Coverage,
            ResearchJobKind::FeedbackCoverage,
            ResearchJobResultKind::FeedbackCoverageArtifact,
            artifact_id.as_uuid(),
        )?;
        let artifact = self.load_coverage(cycle, job, &result).await?;
        match artifact.gate_outcome {
            CoverageGateOutcome::Advance { .. } => Ok(FeedbackStageSuccess::advance(
                result.uri,
                result.content_hash,
            )),
            CoverageGateOutcome::NoAction { reason, .. } => FeedbackStageSuccess::try_complete(
                result.uri,
                result.content_hash,
                FeedbackDecision::NoAction,
                reason.as_str().to_owned(),
            )
            .map_err(Into::into),
        }
    }

    /// Validate a terminal drift result, derive its directive, and reconstruct
    /// the exact aggregate `PostgreSQL` headers from the immutable detail object.
    pub async fn succeeded_drift(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        let artifact_id = FeedbackDriftArtifactId::from_cycle_id(cycle.feedback_cycle_id);
        let result = Self::require_result(
            cycle,
            job,
            FeedbackStage::Drift,
            ResearchJobKind::FeedbackDrift,
            ResearchJobResultKind::FeedbackDriftArtifact,
            artifact_id.as_uuid(),
        )?;
        let artifact = self.load_drift(cycle, job, &result).await?;
        let reports = Self::drift_reports(cycle, &artifact, &result)?;
        let success = match artifact.gate_outcome {
            DriftGateOutcome::Advance { .. } => {
                FeedbackStageSuccess::advance(result.uri, result.content_hash)
            }
            DriftGateOutcome::NoAction { reason } => FeedbackStageSuccess::try_complete(
                result.uri,
                result.content_hash,
                FeedbackDecision::NoAction,
                reason.as_str().to_owned(),
            )?,
        };
        success.attach_drift(reports).map_err(Into::into)
    }

    fn require_identity(
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
        stage: FeedbackStage,
    ) -> QuantResult<()> {
        if identity.feedback_cycle_id() != cycle.feedback_cycle_id
            || identity.feedback_stage() != stage
        {
            return Err(FeedbackError::InvalidJobIdentity {
                detail: format!("{stage} adapter received another cycle or stage identity"),
            }
            .into());
        }
        Ok(())
    }

    fn bind_job(
        &self,
        identity: FeedbackStageJobIdentity,
        params: ResearchJobParams,
    ) -> QuantResult<NewResearchJob> {
        let job = NewResearchJob {
            job_id: identity.job_id(),
            feedback_cycle_id: None,
            feedback_stage: None,
            kind: params.kind(),
            status: ResearchJobStatus::Queued,
            model_spec_id: None,
            decision_policy_snapshot_id: None,
            params_json: params,
            requested_by: None,
            acting_role: RoleCode::new("system"),
            parent_job_id: identity.parent_job_id(),
            recovery_attempt: 0,
            max_recovery_attempts: self.max_recovery_attempts,
        }
        .try_bind_feedback(identity)?;
        job.validate_enqueue()?;
        Ok(job)
    }

    fn require_result(
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
        stage: FeedbackStage,
        kind: ResearchJobKind,
        result_kind: ResearchJobResultKind,
        result_id: Uuid,
    ) -> QuantResult<ResearchJobArtifactRef> {
        job.validate_identity()?;
        let expected = ResearchJobResultRef {
            kind: result_kind,
            id: result_id,
        };
        let result = job.result();
        let artifact = job.result_artifact();
        if job.feedback_cycle_id != Some(cycle.feedback_cycle_id)
            || job.feedback_stage != Some(stage)
            || job.kind != kind
            || job.params_json.kind() != kind
            || job.status != ResearchJobStatus::Succeeded
            || result != Some(expected)
            || artifact.is_none()
        {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: format!("{stage} job has invalid lineage or terminal artifact result"),
            }
            .into());
        }
        artifact.ok_or_else(|| {
            FeedbackError::InvalidCoordinatorState {
                detail: format!("{stage} job lost its terminal artifact"),
            }
            .into()
        })
    }

    async fn load_coverage(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
        result: &ResearchJobArtifactRef,
    ) -> QuantResult<FeedbackCoverageArtifact> {
        let bytes = self.artifacts.get(&result.uri).await?;
        if FeedbackCoverageCodec::bytes_hash(&bytes) != result.content_hash {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "coverage job object bytes differ from its terminal hash".to_owned(),
            }
            .into());
        }
        let artifact = FeedbackCoverageCodec::decode(&bytes)?;
        let ResearchJobParams::FeedbackCoverage(params) = &job.params_json else {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "coverage job lost its exact typed params".to_owned(),
            }
            .into());
        };
        let cycle_hash_matches = artifact.cycle_idempotency_hash == cycle.idempotency_hash;
        if artifact.feedback_cycle_id != cycle.feedback_cycle_id
            || !cycle_hash_matches
            || artifact.artifact_id != params.artifact_id
        {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "coverage artifact differs from its stage job or cycle".to_owned(),
            }
            .into());
        }
        Ok(artifact)
    }

    async fn load_drift(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
        result: &ResearchJobArtifactRef,
    ) -> QuantResult<FeedbackDriftArtifact> {
        let bytes = self.artifacts.get(&result.uri).await?;
        if FeedbackDriftCodec::bytes_hash(&bytes) != result.content_hash {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "drift job object bytes differ from its terminal hash".to_owned(),
            }
            .into());
        }
        let artifact = FeedbackDriftCodec::decode(&bytes)?;
        let ResearchJobParams::FeedbackDrift(params) = &job.params_json else {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "drift job lost its exact typed params".to_owned(),
            }
            .into());
        };
        let cycle_hash_matches = artifact.cycle_idempotency_hash == cycle.idempotency_hash;
        if artifact.feedback_cycle_id != cycle.feedback_cycle_id
            || !cycle_hash_matches
            || artifact.artifact_id != params.artifact_id
            || artifact.coverage_artifact_id != params.coverage_artifact_id
            || artifact.coverage_artifact_uri != params.coverage_artifact_uri
            || artifact.coverage_artifact_hash != params.coverage_artifact_hash
        {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "drift artifact differs from its stage job or coverage reference"
                    .to_owned(),
            }
            .into());
        }
        Ok(artifact)
    }

    fn drift_reports(
        cycle: &FeedbackCycleInfo,
        artifact: &FeedbackDriftArtifact,
        result: &ResearchJobArtifactRef,
    ) -> QuantResult<Vec<NewDriftReport>> {
        let Some(evaluation_window_start) = artifact.comparison_window_start else {
            return Ok(Vec::new());
        };
        artifact
            .observations
            .iter()
            .map(|observation| {
                let sample_count = i64::try_from(observation.sample_count).map_err(|error| {
                    FeedbackError::InvalidCoordinatorState {
                        detail: format!("drift sample count exceeds PostgreSQL i64: {error}"),
                    }
                })?;
                NewDriftReport::try_seal(DriftReportInput {
                    feedback_cycle_id: cycle.feedback_cycle_id,
                    kind: observation.kind,
                    metric: observation.metric,
                    assessment: observation.assessment,
                    baseline_window_start: artifact.champion_baseline.window_start,
                    baseline_window_end: artifact.champion_baseline.window_end,
                    evaluation_window_start,
                    evaluation_window_end: artifact.evaluation_window.cutoff(),
                    label_cutoff: cycle.label_cutoff,
                    observed_value: observation.observed_value,
                    threshold: observation.threshold,
                    sample_count,
                    detail_uri: result.uri.clone(),
                    detail_hash: result.content_hash,
                    observed_at: artifact.observed_at,
                })
                .map_err(Into::into)
            })
            .collect()
    }
}
