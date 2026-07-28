//! Lease-safe F06/F09/F10-to-F11 binding and terminal decision verification.

use std::sync::Arc;

use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        ports::{
            FeedbackComparisonArtifactRef, FeedbackDecisionJobInput, FeedbackDecisionJobParams,
            FeedbackDriftArtifactRef, FeedbackShadowContract, FeedbackShadowReplayArtifactRef,
            FeedbackShadowSubject,
        },
        quant::{
            FeedbackCycleInfo, FeedbackStageEventInfo, FeedbackStageJobIdentity, NewResearchJob,
            ResearchJobArtifactRef, ResearchJobInfo, ResearchJobResultRef,
        },
    },
    enums::quant::{
        FeedbackCycleStatus, FeedbackDecision, FeedbackStage, FeedbackStageEventKind,
        ResearchJobKind, ResearchJobResultKind, ResearchJobStatus,
    },
    types::{
        ContentHash, FeedbackCycleId, FeedbackDecisionArtifactId, FeedbackShadowReplayArtifactId,
        ResearchJobParams, RoleCode,
    },
};
use quant_pivot_repository::traits::{
    FeedbackCycleLeaseGuard, FeedbackCycleRepository, ResearchJobRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    feedback::{DriftGateOutcome, FeedbackDriftArtifact, FeedbackDriftCodec},
    feedback_comparison::{FeedbackComparisonArtifact, FeedbackComparisonCodec},
    feedback_decision::{FeedbackDecisionArtifact, FeedbackDecisionCodec, FeedbackDecisionOutcome},
    feedback_shadow::{
        FeedbackShadowOutcome, FeedbackShadowReplayArtifact, FeedbackShadowReplayCodec,
    },
};

use crate::service::feedback_coordinator::FeedbackStageSuccess;

struct VerifiedDrift {
    reference: FeedbackDriftArtifactRef,
    artifact: FeedbackDriftArtifact,
}

struct VerifiedComparison {
    reference: FeedbackComparisonArtifactRef,
    artifact: FeedbackComparisonArtifact,
}

struct VerifiedShadow {
    reference: FeedbackShadowReplayArtifactRef,
    artifact: FeedbackShadowReplayArtifact,
}

struct VerifiedDecisionInputs {
    drift: VerifiedDrift,
    comparison: VerifiedComparison,
    shadow: VerifiedShadow,
}

struct VerifiedDecision {
    artifact_ref: ResearchJobArtifactRef,
    artifact: FeedbackDecisionArtifact,
    inputs: VerifiedDecisionInputs,
    job_input_hash: ContentHash,
}

/// Fully re-read F11 `CandidateReady` evidence accepted as a promotion input.
///
/// Every field is derived from the durable cycle/job/event timeline and
/// canonical artifact bytes; callers cannot supply candidate, champion,
/// generation, or artifact identities.
#[derive(Debug, Clone)]
pub struct PromotionDecisionEvidence {
    pub cycle: FeedbackCycleInfo,
    pub decision_artifact_id: FeedbackDecisionArtifactId,
    pub decision_artifact_hash: ContentHash,
    pub decision_object_hash: ContentHash,
    pub decision_job_input_hash: ContentHash,
    pub shadow_artifact_id: FeedbackShadowReplayArtifactId,
    pub shadow_artifact_hash: ContentHash,
    pub shadow_object_hash: ContentHash,
    pub candidate_recipe_hash: ContentHash,
    pub shadow_contract: FeedbackShadowContract,
}

/// Dependencies for [`FeedbackDecisionStageAdapter`].
pub struct FeedbackDecisionStageDeps {
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub jobs: Arc<dyn ResearchJobRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub max_recovery_attempts: i32,
}

/// Owns the exact evidence-only terminal Decision boundary.
pub struct FeedbackDecisionStageAdapter {
    cycles: Arc<dyn FeedbackCycleRepository>,
    jobs: Arc<dyn ResearchJobRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    max_recovery_attempts: i32,
}

impl FeedbackDecisionStageAdapter {
    pub fn try_new(deps: FeedbackDecisionStageDeps) -> Result<Self, FeedbackError> {
        if deps.max_recovery_attempts < 0 {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "feedback job recovery cap cannot be negative".to_owned(),
            });
        }
        Ok(Self {
            cycles: deps.cycles,
            jobs: deps.jobs,
            artifacts: deps.artifacts,
            max_recovery_attempts: deps.max_recovery_attempts,
        })
    }

    /// Freeze exact advancing F06, terminal F09, and terminal F10 lineage.
    pub async fn prepare_decision(
        &self,
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        Self::require_identity(cycle, lease, identity)?;
        cycle.validate()?;
        let inputs = self.load_inputs(cycle).await?;
        let policy_id = cycle
            .candidate_family
            .shared_evaluation()
            .source_lineage
            .decision_policy_snapshot_id;
        let params = FeedbackDecisionJobParams::try_new(FeedbackDecisionJobInput {
            feedback_cycle_id: cycle.feedback_cycle_id,
            cycle_idempotency_hash: cycle.idempotency_hash,
            profile_ref: cycle.profile_ref.clone(),
            feedback_policy_hash: cycle.feedback_policy_hash,
            candidate_family_hash: cycle.candidate_family_hash,
            decision_policy_snapshot_id: policy_id,
            champion_model_version_id: cycle.champion_model_version_id,
            champion_serving_contract_hash: cycle.champion_serving_contract_hash,
            drift: inputs.drift.reference,
            comparison: inputs.comparison.reference,
            shadow_replay: inputs.shadow.reference,
        })?;
        self.bind_job(cycle, identity, params)
    }

    /// Re-read every immutable predecessor and complete the cycle from the
    /// verified F11 object. This method never mutates a serving route.
    pub async fn succeeded_decision(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        let verified = self.verify_decision(cycle, job, None).await?;
        FeedbackStageSuccess::try_complete(
            verified.artifact_ref.uri,
            verified.artifact_ref.content_hash,
            verified.artifact.outcome().decision(),
            verified.artifact.outcome().reason().to_owned(),
        )
        .map_err(Into::into)
    }

    /// Re-read a terminal `CandidateReady` cycle and every F06/F09/F10/F11
    /// object before exposing its immutable promotion evidence.
    pub async fn promotion_evidence(
        &self,
        cycle_id: &FeedbackCycleId,
    ) -> QuantResult<PromotionDecisionEvidence> {
        let cycle = self
            .cycles
            .find_cycle(cycle_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_feedback_cycle", cycle_id))?;
        cycle.validate()?;
        if cycle.status != FeedbackCycleStatus::Succeeded
            || cycle.decision != Some(FeedbackDecision::CandidateReady)
        {
            return Err(Self::promotion_invalid(
                "promotion requires one terminal CandidateReady feedback cycle",
            ));
        }
        let events = self.cycles.list_stage_events(cycle_id).await?;
        let (event, job) = self
            .load_stage_job(&cycle, &events, FeedbackStage::Decision)
            .await?;
        let verified = self.verify_decision(&cycle, &job, Some(&event)).await?;
        if cycle.terminal_reason_code.as_deref() != Some(verified.artifact.outcome().reason()) {
            return Err(Self::promotion_invalid(
                "cycle terminal reason differs from its exact F11 artifact",
            ));
        }
        let FeedbackDecisionOutcome::CandidateReady { evidence } = verified.artifact.outcome()
        else {
            return Err(Self::promotion_invalid(
                "F11 artifact is not CandidateReady",
            ));
        };
        let FeedbackShadowSubject::Candidate {
            candidate_recipe_hash,
            contract,
        } = verified.inputs.shadow.artifact.subject()
        else {
            return Err(Self::promotion_invalid(
                "F10 artifact has no exact candidate subject",
            ));
        };
        if !matches!(
            verified.inputs.shadow.artifact.outcome(),
            FeedbackShadowOutcome::Stable { .. }
        ) || evidence.candidate.comparison.candidate_recipe_hash != *candidate_recipe_hash
            || evidence.candidate.replay.candidate_recipe_hash != *candidate_recipe_hash
            || evidence.candidate.replay.model_version_id != contract.candidate_model_version_id()
            || evidence.candidate.replay.serving_contract_hash
                != contract.candidate_serving_contract_hash()
        {
            return Err(Self::promotion_invalid(
                "F09/F10/F11 candidate, contract, or stability evidence differs",
            ));
        }
        Ok(PromotionDecisionEvidence {
            cycle,
            decision_artifact_id: verified.artifact.artifact_id(),
            decision_artifact_hash: verified.artifact.artifact_hash(),
            decision_object_hash: verified.artifact_ref.content_hash,
            decision_job_input_hash: verified.job_input_hash,
            shadow_artifact_id: verified.inputs.shadow.artifact.artifact_id(),
            shadow_artifact_hash: verified.inputs.shadow.artifact.artifact_hash(),
            shadow_object_hash: verified.inputs.shadow.reference.artifact.content_hash,
            candidate_recipe_hash: *candidate_recipe_hash,
            shadow_contract: contract.as_ref().clone(),
        })
    }

    async fn verify_decision(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
        event: Option<&FeedbackStageEventInfo>,
    ) -> QuantResult<VerifiedDecision> {
        let ResearchJobParams::FeedbackDecision(params) = &job.params_json else {
            return Err(Self::invalid("Decision job lost its typed parameters"));
        };
        params.validate()?;
        let expected = ResearchJobResultRef {
            kind: ResearchJobResultKind::FeedbackDecisionArtifact,
            id: params.artifact_id.as_uuid(),
        };
        let artifact_ref = job
            .result_artifact()
            .ok_or_else(|| Self::invalid("Decision job lost its terminal artifact"))?;
        if job.feedback_cycle_id != Some(cycle.feedback_cycle_id)
            || job.feedback_stage != Some(FeedbackStage::Decision)
            || job.kind != ResearchJobKind::FeedbackDecision
            || job.status != ResearchJobStatus::Succeeded
            || job.result() != Some(expected)
        {
            return Err(Self::invalid(
                "Decision job has invalid cycle, kind, status, or result lineage",
            ));
        }
        if event.is_some_and(|event| {
            event.evidence_uri.as_ref() != Some(&artifact_ref.uri)
                || event.evidence_hash != Some(artifact_ref.content_hash)
        }) {
            return Err(Self::invalid(
                "Decision job result and WORM success event differ",
            ));
        }
        let inputs = self.load_inputs(cycle).await?;
        if params.drift != inputs.drift.reference
            || params.comparison != inputs.comparison.reference
            || params.shadow_replay != inputs.shadow.reference
        {
            return Err(Self::invalid(
                "Decision job predecessors differ from the WORM stage timeline",
            ));
        }
        let bytes = self.artifacts.get(&artifact_ref.uri).await?;
        if FeedbackDecisionCodec::bytes_hash(&bytes) != artifact_ref.content_hash {
            return Err(Self::invalid(
                "Decision object bytes differ from their terminal hash",
            ));
        }
        let artifact = FeedbackDecisionCodec::decode(&bytes)?;
        artifact.validate_against(
            params,
            &inputs.drift.artifact,
            &inputs.comparison.artifact,
            &inputs.shadow.artifact,
        )?;
        Ok(VerifiedDecision {
            artifact_ref,
            artifact,
            inputs,
            job_input_hash: params.input_hash()?,
        })
    }

    async fn load_inputs(&self, cycle: &FeedbackCycleInfo) -> QuantResult<VerifiedDecisionInputs> {
        let events = self
            .cycles
            .list_stage_events(&cycle.feedback_cycle_id)
            .await?;
        let drift = self.load_drift(cycle, &events).await?;
        let comparison = self.load_comparison(cycle, &events).await?;
        let shadow = self
            .load_shadow(cycle, &events, &comparison.reference)
            .await?;
        Ok(VerifiedDecisionInputs {
            drift,
            comparison,
            shadow,
        })
    }

    async fn load_drift(
        &self,
        cycle: &FeedbackCycleInfo,
        events: &[FeedbackStageEventInfo],
    ) -> QuantResult<VerifiedDrift> {
        let (event, job) = self
            .load_stage_job(cycle, events, FeedbackStage::Drift)
            .await?;
        let ResearchJobParams::FeedbackDrift(params) = &job.params_json else {
            return Err(Self::invalid("Drift predecessor lost its typed parameters"));
        };
        let input_hash = params.input_hash()?;
        let expected = ResearchJobResultRef {
            kind: ResearchJobResultKind::FeedbackDriftArtifact,
            id: params.artifact_id.as_uuid(),
        };
        let artifact_ref = Self::require_result(&job, &event, expected)?;
        let bytes = self.artifacts.get(&artifact_ref.uri).await?;
        if FeedbackDriftCodec::bytes_hash(&bytes) != artifact_ref.content_hash {
            return Err(Self::invalid(
                "Drift predecessor bytes differ from the terminal hash",
            ));
        }
        let artifact = FeedbackDriftCodec::decode(&bytes)?;
        let params_cycle_matches = params.feedback_cycle_id == cycle.feedback_cycle_id;
        let params_hash_matches = params.cycle_idempotency_hash == cycle.idempotency_hash;
        if !params_cycle_matches
            || !params_hash_matches
            || job.kind != ResearchJobKind::FeedbackDrift
            || artifact.artifact_id != params.artifact_id
            || artifact.feedback_cycle_id != cycle.feedback_cycle_id
            || artifact.cycle_idempotency_hash != cycle.idempotency_hash
            || artifact.profile_ref != cycle.profile_ref
            || artifact.feedback_policy_hash != cycle.feedback_policy_hash
            || artifact.champion_model_version_id != cycle.champion_model_version_id
            || artifact.champion_serving_contract_hash != cycle.champion_serving_contract_hash
            || !matches!(artifact.gate_outcome, DriftGateOutcome::Advance { .. })
        {
            return Err(Self::invalid(
                "Drift predecessor differs from the advancing cycle lineage",
            ));
        }
        Ok(VerifiedDrift {
            reference: FeedbackDriftArtifactRef {
                feedback_cycle_id: cycle.feedback_cycle_id,
                job_id: job.job_id,
                artifact_id: params.artifact_id,
                input_hash,
                artifact: artifact_ref,
            },
            artifact,
        })
    }

    async fn load_comparison(
        &self,
        cycle: &FeedbackCycleInfo,
        events: &[FeedbackStageEventInfo],
    ) -> QuantResult<VerifiedComparison> {
        let (event, job) = self
            .load_stage_job(cycle, events, FeedbackStage::Comparison)
            .await?;
        let ResearchJobParams::FeedbackComparison(params) = &job.params_json else {
            return Err(Self::invalid(
                "Comparison predecessor lost its typed parameters",
            ));
        };
        params.validate()?;
        let expected = ResearchJobResultRef {
            kind: ResearchJobResultKind::FeedbackComparisonArtifact,
            id: params.artifact_id.as_uuid(),
        };
        let artifact_ref = Self::require_result(&job, &event, expected)?;
        let bytes = self.artifacts.get(&artifact_ref.uri).await?;
        if FeedbackComparisonCodec::bytes_hash(&bytes) != artifact_ref.content_hash {
            return Err(Self::invalid(
                "Comparison predecessor bytes differ from the terminal hash",
            ));
        }
        let artifact = FeedbackComparisonCodec::decode(&bytes)?;
        artifact.validate_for(params)?;
        let policy_id = cycle
            .candidate_family
            .shared_evaluation()
            .source_lineage
            .decision_policy_snapshot_id;
        let params_cycle_matches = params.feedback_cycle_id == cycle.feedback_cycle_id;
        let params_hash_matches = params.cycle_idempotency_hash == cycle.idempotency_hash;
        if !params_cycle_matches
            || !params_hash_matches
            || job.kind != ResearchJobKind::FeedbackComparison
            || params.candidate_family_hash != cycle.candidate_family_hash
            || params.decision_policy_snapshot_id != policy_id
            || params.champion_model_version_id != cycle.champion_model_version_id
            || params.champion_serving_contract_hash != cycle.champion_serving_contract_hash
        {
            return Err(Self::invalid(
                "Comparison predecessor differs from the cycle lineage",
            ));
        }
        Ok(VerifiedComparison {
            reference: FeedbackComparisonArtifactRef {
                feedback_cycle_id: cycle.feedback_cycle_id,
                job_id: job.job_id,
                artifact_id: params.artifact_id,
                input_hash: params.input_hash()?,
                candidate_family_hash: params.candidate_family_hash,
                decision_policy_snapshot_id: params.decision_policy_snapshot_id,
                artifact: artifact_ref,
            },
            artifact,
        })
    }

    async fn load_shadow(
        &self,
        cycle: &FeedbackCycleInfo,
        events: &[FeedbackStageEventInfo],
        comparison: &FeedbackComparisonArtifactRef,
    ) -> QuantResult<VerifiedShadow> {
        let (event, job) = self
            .load_stage_job(cycle, events, FeedbackStage::ShadowReplay)
            .await?;
        let ResearchJobParams::FeedbackShadowReplay(params) = &job.params_json else {
            return Err(Self::invalid(
                "ShadowReplay predecessor lost its typed parameters",
            ));
        };
        params.validate()?;
        let expected = ResearchJobResultRef {
            kind: ResearchJobResultKind::FeedbackShadowReplayArtifact,
            id: params.artifact_id.as_uuid(),
        };
        let artifact_ref = Self::require_result(&job, &event, expected)?;
        let bytes = self.artifacts.get(&artifact_ref.uri).await?;
        if FeedbackShadowReplayCodec::bytes_hash(&bytes) != artifact_ref.content_hash {
            return Err(Self::invalid(
                "ShadowReplay predecessor bytes differ from the terminal hash",
            ));
        }
        let artifact = FeedbackShadowReplayCodec::decode(&bytes)?;
        artifact.validate_for(params)?;
        let params_cycle_matches = params.feedback_cycle_id == cycle.feedback_cycle_id;
        let params_hash_matches = params.cycle_idempotency_hash == cycle.idempotency_hash;
        if !params_cycle_matches
            || !params_hash_matches
            || job.kind != ResearchJobKind::FeedbackShadowReplay
            || params.profile_ref != cycle.profile_ref
            || params.feedback_policy_hash != cycle.feedback_policy_hash
            || params.previous != *comparison
        {
            return Err(Self::invalid(
                "ShadowReplay predecessor differs from the cycle and comparison lineage",
            ));
        }
        Ok(VerifiedShadow {
            reference: FeedbackShadowReplayArtifactRef {
                feedback_cycle_id: cycle.feedback_cycle_id,
                job_id: job.job_id,
                artifact_id: params.artifact_id,
                input_hash: params.input_hash()?,
                previous: comparison.clone(),
                artifact: artifact_ref,
            },
            artifact,
        })
    }

    async fn load_stage_job(
        &self,
        cycle: &FeedbackCycleInfo,
        events: &[FeedbackStageEventInfo],
        stage: FeedbackStage,
    ) -> QuantResult<(FeedbackStageEventInfo, ResearchJobInfo)> {
        let event = events
            .iter()
            .rev()
            .find(|event| {
                event.stage == stage && event.event_kind == FeedbackStageEventKind::Succeeded
            })
            .cloned()
            .ok_or_else(|| {
                Self::invalid(format!("Decision has no succeeded {stage} predecessor"))
            })?;
        event.validate()?;
        let job_id = event
            .research_job_id
            .ok_or_else(|| Self::invalid(format!("succeeded {stage} event has no job identity")))?;
        let job = self
            .jobs
            .find_by_id(&job_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_research_job", job_id))?;
        if job.feedback_cycle_id != Some(cycle.feedback_cycle_id)
            || job.feedback_stage != Some(stage)
            || job.status != ResearchJobStatus::Succeeded
        {
            return Err(Self::invalid(format!(
                "{stage} predecessor has invalid cycle, stage, or status"
            )));
        }
        Ok((event, job))
    }

    fn require_result(
        job: &ResearchJobInfo,
        event: &FeedbackStageEventInfo,
        expected: ResearchJobResultRef,
    ) -> QuantResult<ResearchJobArtifactRef> {
        let artifact = job
            .result_artifact()
            .ok_or_else(|| Self::invalid("succeeded predecessor has no terminal artifact"))?;
        if job.result() != Some(expected)
            || event.evidence_uri.as_ref() != Some(&artifact.uri)
            || event.evidence_hash != Some(artifact.content_hash)
        {
            return Err(Self::invalid(
                "predecessor job result and WORM success event differ",
            ));
        }
        Ok(artifact)
    }

    fn bind_job(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
        params: FeedbackDecisionJobParams,
    ) -> QuantResult<NewResearchJob> {
        let job = NewResearchJob {
            job_id: identity.job_id(),
            feedback_cycle_id: None,
            feedback_stage: None,
            kind: ResearchJobKind::FeedbackDecision,
            status: ResearchJobStatus::Queued,
            model_spec_id: Some(cycle.candidate_family.shared_evaluation().model_spec_id),
            decision_policy_snapshot_id: Some(params.decision_policy_snapshot_id),
            params_json: ResearchJobParams::FeedbackDecision(Box::new(params)),
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

    fn require_identity(
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<()> {
        let lease_cycle_matches = lease.feedback_cycle_id == cycle.feedback_cycle_id;
        let lease_generation_matches = lease.expected_generation == cycle.generation;
        let lease_matches_cycle = lease_cycle_matches && lease_generation_matches;
        let identity_cycle_matches = identity.feedback_cycle_id() == cycle.feedback_cycle_id;
        let identity_stage_matches = identity.feedback_stage() == FeedbackStage::Decision;
        let identity_matches_cycle = identity_cycle_matches && identity_stage_matches;
        if !lease_matches_cycle || !identity_matches_cycle {
            return Err(Self::invalid(
                "Decision lease, generation, cycle, or job identity is invalid",
            ));
        }
        Ok(())
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidJobContract {
            detail: detail.into(),
        }
        .into()
    }

    fn promotion_invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidPromotionPreflight {
            detail: detail.into(),
        }
        .into()
    }
}
