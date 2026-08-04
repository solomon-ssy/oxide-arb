//! Exact coordinator binding for the governed `RecipePlan` stage.

use std::sync::Arc;

use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        ports::{
            CandidateRecipePlanArtifact, CandidateRecipePlanInput, CandidateRecipePlanJobParams,
            FeedbackAttributionManifestRef, FeedbackRecipeDriftManifest,
        },
        quant::{
            FeedbackCycleInfo, FeedbackStageJobIdentity, NewResearchJob, ResearchJobArtifactRef,
            ResearchJobInfo, ResearchJobResultRef,
        },
    },
    enums::quant::{
        FeedbackDecision, FeedbackEvaluationMode, FeedbackStage, FeedbackStageEventKind,
        ResearchJobKind, ResearchJobResultKind, ResearchJobStatus,
    },
    hashing::CanonicalDigest,
    types::{ContentHash, ResearchJobParams, RoleCode, builtin_research_profiles},
};
use quant_pivot_repository::traits::{FeedbackCycleRepository, ResearchJobRepository};
use quant_pivot_research::{
    artifact::ArtifactStore,
    feedback::{DriftGateOutcome, FeedbackDriftCodec},
    feedback_governance::FeedbackGovernanceCodec,
    feedback_recipe::CandidateRecipePlanCodec,
};
use uuid::Uuid;

use crate::service::feedback_coordinator::FeedbackStageSuccess;

pub(crate) struct VerifiedRecipePlan {
    pub result: ResearchJobArtifactRef,
    pub artifact: CandidateRecipePlanArtifact,
}

pub struct FeedbackRecipeStageDeps {
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub jobs: Arc<dyn ResearchJobRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub max_recovery_attempts: i32,
}

pub struct FeedbackRecipeStageAdapter {
    cycles: Arc<dyn FeedbackCycleRepository>,
    jobs: Arc<dyn ResearchJobRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    max_recovery_attempts: i32,
}

impl FeedbackRecipeStageAdapter {
    pub fn try_new(deps: FeedbackRecipeStageDeps) -> Result<Self, FeedbackError> {
        if deps.max_recovery_attempts < 0 {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "recipe-plan recovery cap cannot be negative".to_owned(),
            });
        }
        Ok(Self {
            cycles: deps.cycles,
            jobs: deps.jobs,
            artifacts: deps.artifacts,
            max_recovery_attempts: deps.max_recovery_attempts,
        })
    }

    pub async fn prepare(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        Self::require_identity(cycle, identity)?;
        let (attribution_job, attribution_ref) = self
            .load_stage_job(
                cycle,
                FeedbackStage::Attribution,
                ResearchJobKind::FeedbackAttribution,
                ResearchJobResultKind::FeedbackAttributionManifest,
            )
            .await?;
        let attribution_bytes = self.artifacts.get(&attribution_ref.uri).await?;
        Self::require_hash(
            attribution_ref.content_hash,
            FeedbackGovernanceCodec::bytes_hash(&attribution_bytes),
            "Attribution",
        )?;
        let attribution = FeedbackGovernanceCodec::decode_attribution(&attribution_bytes)?;
        let (drift_job, drift_ref) = self
            .load_stage_job(
                cycle,
                FeedbackStage::Drift,
                ResearchJobKind::FeedbackDrift,
                ResearchJobResultKind::FeedbackDriftArtifact,
            )
            .await?;
        let drift_bytes = self.artifacts.get(&drift_ref.uri).await?;
        Self::require_hash(
            drift_ref.content_hash,
            FeedbackDriftCodec::bytes_hash(&drift_bytes),
            "Drift",
        )?;
        let drift = FeedbackDriftCodec::decode(&drift_bytes)?;
        let exceeded_metrics = match drift.gate_outcome {
            DriftGateOutcome::Advance { exceeded_metrics } => exceeded_metrics,
            DriftGateOutcome::NoAction { .. }
                if cycle.evaluation_mode == FeedbackEvaluationMode::ForcedRetraining =>
            {
                Vec::new()
            }
            DriftGateOutcome::NoAction { .. } => {
                return Err(Self::invalid(
                    "conditional RecipePlan cannot follow a Drift NoAction",
                ));
            }
        };
        let attribution_identity_exact =
            attribution.cycle_idempotency_hash == cycle.idempotency_hash;
        let drift_identity_exact = drift.cycle_idempotency_hash == cycle.idempotency_hash;
        if attribution.feedback_cycle_id != cycle.feedback_cycle_id
            || !attribution_identity_exact
            || drift.feedback_cycle_id != cycle.feedback_cycle_id
            || !drift_identity_exact
        {
            return Err(Self::invalid(
                "RecipePlan predecessors differ from the frozen cycle",
            ));
        }
        let profile = builtin_research_profiles()
            .map_err(Self::invalid)?
            .into_iter()
            .find(|profile| profile.profile_ref == cycle.profile_ref)
            .ok_or_else(|| Self::invalid("RecipePlan profile revision is unavailable"))?;
        let params = CandidateRecipePlanJobParams::try_new(CandidateRecipePlanInput {
            feedback_cycle_id: cycle.feedback_cycle_id,
            cycle_idempotency_hash: cycle.idempotency_hash,
            label_cutoff: cycle.label_cutoff,
            planned_at: self.cycles.database_time().await?,
            evaluation_mode: cycle.evaluation_mode,
            attribution: FeedbackAttributionManifestRef {
                job_id: attribution_job.job_id,
                artifact: attribution_ref,
                use_set_hash: attribution.use_set_hash,
                produced_set_hash: attribution.produced_set_hash,
            },
            drift: FeedbackRecipeDriftManifest {
                job_id: drift_job.job_id,
                artifact: drift_ref,
                exceeded_metrics,
            },
            max_challengers: profile.spec.feedback_policy.max_challengers,
        })?;
        self.bind_job(
            identity,
            ResearchJobParams::FeedbackRecipePlan(Box::new(params)),
        )
    }

    pub async fn succeeded(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        let verified = self.verify_job(cycle, job).await?;
        if let Some(blocker) = verified.artifact.blocker() {
            FeedbackStageSuccess::try_complete(
                verified.result.uri,
                verified.result.content_hash,
                FeedbackDecision::NoAction,
                blocker.reason_code().to_owned(),
            )
            .map_err(Into::into)
        } else {
            Ok(FeedbackStageSuccess::advance(
                verified.result.uri,
                verified.result.content_hash,
            ))
        }
    }

    pub(crate) async fn load_plan(
        &self,
        cycle: &FeedbackCycleInfo,
    ) -> QuantResult<VerifiedRecipePlan> {
        let (job, _) = self
            .load_stage_job(
                cycle,
                FeedbackStage::RecipePlan,
                ResearchJobKind::FeedbackRecipePlan,
                ResearchJobResultKind::CandidateRecipePlanArtifact,
            )
            .await?;
        self.verify_job(cycle, &job).await
    }

    async fn verify_job(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<VerifiedRecipePlan> {
        let result = Self::require_result(
            cycle,
            job,
            FeedbackStage::RecipePlan,
            ResearchJobKind::FeedbackRecipePlan,
            ResearchJobResultKind::CandidateRecipePlanArtifact,
        )?;
        let ResearchJobParams::FeedbackRecipePlan(params) = &job.params_json else {
            return Err(Self::invalid("RecipePlan job lost its typed parameters"));
        };
        params.validate()?;
        let bytes = self.artifacts.get(&result.uri).await?;
        Self::require_hash(
            result.content_hash,
            CanonicalDigest::content_hash_bytes(&bytes),
            "RecipePlan",
        )?;
        let artifact = CandidateRecipePlanCodec::decode(&bytes)?;
        let cycle_identity_exact = artifact.cycle_idempotency_hash == cycle.idempotency_hash;
        if artifact.feedback_cycle_id != cycle.feedback_cycle_id
            || !cycle_identity_exact
            || artifact.artifact_id != params.artifact_id
            || artifact.input_hash != params.input_hash()?
            || artifact.label_cutoff != cycle.label_cutoff
            || artifact.label_cutoff != params.label_cutoff
            || artifact.evaluation_mode != cycle.evaluation_mode
            || artifact.profile_ref != cycle.profile_ref
            || artifact.route != cycle.route
            || artifact.model_family != cycle.champion_model_family
            || artifact.attribution != params.attribution
            || artifact.drift != params.drift
        {
            return Err(Self::invalid(
                "RecipePlan artifact differs from its cycle or job preimage",
            ));
        }
        Ok(VerifiedRecipePlan { result, artifact })
    }

    async fn load_stage_job(
        &self,
        cycle: &FeedbackCycleInfo,
        stage: FeedbackStage,
        kind: ResearchJobKind,
        result_kind: ResearchJobResultKind,
    ) -> QuantResult<(ResearchJobInfo, ResearchJobArtifactRef)> {
        let events = self
            .cycles
            .list_stage_events(&cycle.feedback_cycle_id)
            .await?;
        let event = events
            .iter()
            .rev()
            .find(|event| {
                event.stage == stage && event.event_kind == FeedbackStageEventKind::Succeeded
            })
            .ok_or_else(|| Self::invalid(format!("{stage} has no WORM success event")))?;
        event.validate()?;
        let job_id = event
            .research_job_id
            .ok_or_else(|| Self::invalid(format!("{stage} success has no job identity")))?;
        let job = self
            .jobs
            .find_by_id(&job_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_research_job", job_id))?;
        let result = Self::require_result(cycle, &job, stage, kind, result_kind)?;
        if event.evidence_uri.as_ref() != Some(&result.uri)
            || event.evidence_hash != Some(result.content_hash)
        {
            return Err(Self::invalid(format!(
                "{stage} result differs from its WORM success event"
            )));
        }
        Ok((job, result))
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
    ) -> QuantResult<ResearchJobArtifactRef> {
        job.validate_identity()?;
        let result = job
            .result_artifact()
            .ok_or_else(|| Self::invalid(format!("{stage} job lost its terminal artifact")))?;
        if job.feedback_cycle_id != Some(cycle.feedback_cycle_id)
            || job.feedback_stage != Some(stage)
            || job.kind != kind
            || job.params_json.kind() != kind
            || job.status != ResearchJobStatus::Succeeded
            || job.result()
                != Some(ResearchJobResultRef {
                    kind: result_kind,
                    id: result_id(&job.params_json, stage)?,
                })
        {
            return Err(Self::invalid(format!(
                "{stage} job has invalid cycle, kind, status, or result lineage"
            )));
        }
        Ok(result)
    }

    fn require_identity(
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<()> {
        if identity.feedback_cycle_id() != cycle.feedback_cycle_id
            || identity.feedback_stage() != FeedbackStage::RecipePlan
        {
            return Err(FeedbackError::InvalidJobIdentity {
                detail: "RecipePlan adapter received another cycle or stage".to_owned(),
            }
            .into());
        }
        Ok(())
    }

    fn require_hash(
        expected: ContentHash,
        actual: ContentHash,
        stage: &'static str,
    ) -> QuantResult<()> {
        if expected != actual {
            return Err(Self::invalid(format!(
                "{stage} bytes differ from their terminal hash"
            )));
        }
        Ok(())
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidCoordinatorState {
            detail: detail.into(),
        }
        .into()
    }
}

fn result_id(params: &ResearchJobParams, stage: FeedbackStage) -> QuantResult<Uuid> {
    let id = match (params, stage) {
        (ResearchJobParams::FeedbackAttribution(params), FeedbackStage::Attribution) => {
            params.artifact_id.as_uuid()
        }
        (ResearchJobParams::FeedbackDrift(params), FeedbackStage::Drift) => {
            params.artifact_id.as_uuid()
        }
        (ResearchJobParams::FeedbackRecipePlan(params), FeedbackStage::RecipePlan) => {
            params.artifact_id.as_uuid()
        }
        _ => {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: format!("{stage} job lost its exact result identity"),
            }
            .into());
        }
    };
    Ok(id)
}
