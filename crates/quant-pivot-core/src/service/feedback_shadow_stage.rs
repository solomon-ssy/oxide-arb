//! Lease-safe F09-to-F10 binding and terminal artifact verification.

use std::sync::Arc;

use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        ports::{
            FeedbackComparisonArtifactRef, FeedbackShadowContract, FeedbackShadowContractInput,
            FeedbackShadowJobParams, FeedbackShadowSubject, FeedbackShadowUnavailableReason,
        },
        quant::{
            FeedbackCycleInfo, FeedbackStageJobIdentity, NewResearchJob, ResearchJobInfo,
            ResearchJobResultRef,
        },
    },
    enums::quant::{
        FeedbackStage, FeedbackStageEventKind, ResearchJobKind, ResearchJobResultKind,
        ResearchJobStatus,
    },
    runtime_config::BuyModelRoute,
    types::{
        DecisionPolicySnapshotId, FeedbackShadowReplayArtifactId, ResearchJobParams, RoleCode,
    },
};
use quant_pivot_repository::traits::{
    FeedbackCycleLeaseGuard, FeedbackCycleRepository, ResearchJobRepository,
};
use quant_pivot_research::{
    artifact::ArtifactStore,
    feedback_comparison::{FeedbackComparisonArtifact, FeedbackComparisonCodec, RomanoWolfOutcome},
    feedback_shadow::FeedbackShadowReplayCodec,
};

use crate::service::{
    feedback_coordinator::FeedbackStageSuccess,
    model_serving_generation::ModelServingGenerationStore,
};

struct VerifiedComparison {
    reference: FeedbackComparisonArtifactRef,
    artifact: FeedbackComparisonArtifact,
}

/// Dependencies for [`FeedbackShadowStageAdapter`].
pub struct FeedbackShadowStageDeps {
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub jobs: Arc<dyn ResearchJobRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub serving_generations: Arc<ModelServingGenerationStore>,
    pub max_recovery_attempts: i32,
}

/// Owns the exact F09-to-F10 boundary without decision/promotion authority.
pub struct FeedbackShadowStageAdapter {
    cycles: Arc<dyn FeedbackCycleRepository>,
    jobs: Arc<dyn ResearchJobRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    serving_generations: Arc<ModelServingGenerationStore>,
    max_recovery_attempts: i32,
}

impl FeedbackShadowStageAdapter {
    pub fn try_new(deps: FeedbackShadowStageDeps) -> Result<Self, FeedbackError> {
        if deps.max_recovery_attempts < 0 {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "feedback job recovery cap cannot be negative".to_owned(),
            });
        }
        Ok(Self {
            cycles: deps.cycles,
            jobs: deps.jobs,
            artifacts: deps.artifacts,
            serving_generations: deps.serving_generations,
            max_recovery_attempts: deps.max_recovery_attempts,
        })
    }

    /// Freeze one exact eligible production-shadow subject, or an explicit
    /// no-eligible subject, under the coordinator's live lease.
    pub async fn prepare_shadow(
        &self,
        cycle: &FeedbackCycleInfo,
        lease: FeedbackCycleLeaseGuard,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        Self::require_identity(cycle, lease, identity)?;
        cycle.validate()?;
        let comparison = self.load_comparison(cycle).await?;
        let subject = self
            .build_subject(
                cycle,
                &comparison.artifact,
                comparison.reference.decision_policy_snapshot_id,
            )
            .await?;
        let params = FeedbackShadowJobParams {
            feedback_cycle_id: cycle.feedback_cycle_id,
            cycle_idempotency_hash: cycle.idempotency_hash,
            artifact_id: FeedbackShadowReplayArtifactId::from_cycle_id(cycle.feedback_cycle_id),
            previous: comparison.reference,
            profile_ref: cycle.profile_ref.clone(),
            feedback_policy_hash: cycle.feedback_policy_hash,
            subject,
        };
        params.validate()?;
        self.bind_job(cycle, identity, params)
    }

    /// Verify the exact terminal F10 object before the coordinator advances.
    pub async fn succeeded_shadow(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        let ResearchJobParams::FeedbackShadowReplay(params) = &job.params_json else {
            return Err(Self::invalid("ShadowReplay job lost its typed parameters"));
        };
        params.validate()?;
        let expected = ResearchJobResultRef {
            kind: ResearchJobResultKind::FeedbackShadowReplayArtifact,
            id: params.artifact_id.as_uuid(),
        };
        let artifact_ref = job
            .result_artifact()
            .ok_or_else(|| Self::invalid("ShadowReplay job lost its terminal artifact"))?;
        if job.feedback_cycle_id != Some(cycle.feedback_cycle_id)
            || job.feedback_stage != Some(FeedbackStage::ShadowReplay)
            || job.kind != ResearchJobKind::FeedbackShadowReplay
            || job.status != ResearchJobStatus::Succeeded
            || job.result() != Some(expected)
        {
            return Err(Self::invalid(
                "ShadowReplay job has invalid cycle, kind, status, or result lineage",
            ));
        }
        let bytes = self.artifacts.get(&artifact_ref.uri).await?;
        if FeedbackShadowReplayCodec::bytes_hash(&bytes) != artifact_ref.content_hash {
            return Err(Self::invalid(
                "ShadowReplay object bytes differ from their terminal hash",
            ));
        }
        let artifact = FeedbackShadowReplayCodec::decode(&bytes)?;
        artifact.validate_for(params)?;
        Ok(FeedbackStageSuccess::advance(
            artifact_ref.uri,
            artifact_ref.content_hash,
        ))
    }

    async fn load_comparison(&self, cycle: &FeedbackCycleInfo) -> QuantResult<VerifiedComparison> {
        let events = self
            .cycles
            .list_stage_events(&cycle.feedback_cycle_id)
            .await?;
        let event = events
            .iter()
            .rev()
            .find(|event| {
                event.stage == FeedbackStage::Comparison
                    && event.event_kind == FeedbackStageEventKind::Succeeded
            })
            .ok_or_else(|| Self::invalid("ShadowReplay has no succeeded Comparison predecessor"))?;
        event.validate()?;
        let job_id = event
            .research_job_id
            .ok_or_else(|| Self::invalid("succeeded Comparison event has no job identity"))?;
        let job = self
            .jobs
            .find_by_id(&job_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_research_job", job_id))?;
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
        let artifact_ref = job
            .result_artifact()
            .ok_or_else(|| Self::invalid("Comparison predecessor has no terminal artifact"))?;
        let expected_policy_id = cycle
            .candidate_family
            .shared_evaluation()
            .source_lineage
            .decision_policy_snapshot_id;
        let cycle_id_matches = params.feedback_cycle_id == cycle.feedback_cycle_id;
        let cycle_hash_matches = params.cycle_idempotency_hash == cycle.idempotency_hash;
        let family_matches = params.candidate_family_hash == cycle.candidate_family_hash;
        let profile_matches = params.evaluation_use.profile_ref == cycle.profile_ref;
        let cutoff_matches = params.evaluation_use.label_cutoff == cycle.label_cutoff;
        let predecessor_matches_cycle = cycle_id_matches
            && cycle_hash_matches
            && family_matches
            && profile_matches
            && cutoff_matches;
        let champion_model_matches =
            params.champion_model_version_id == cycle.champion_model_version_id;
        let champion_contract_matches =
            params.champion_serving_contract_hash == cycle.champion_serving_contract_hash;
        let decision_policy_matches = params.decision_policy_snapshot_id == expected_policy_id;
        let model_and_policy_match_cycle =
            champion_model_matches && champion_contract_matches && decision_policy_matches;
        let job_matches_predecessor = job.feedback_cycle_id == Some(cycle.feedback_cycle_id)
            && job.feedback_stage == Some(FeedbackStage::Comparison)
            && job.kind == ResearchJobKind::FeedbackComparison
            && job.status == ResearchJobStatus::Succeeded
            && job.result() == Some(expected);
        let event_matches_artifact = event.evidence_uri.as_ref() == Some(&artifact_ref.uri)
            && event.evidence_hash == Some(artifact_ref.content_hash);
        if !predecessor_matches_cycle
            || !model_and_policy_match_cycle
            || !job_matches_predecessor
            || !event_matches_artifact
        {
            return Err(Self::invalid(
                "Comparison job and success event do not carry one exact lineage",
            ));
        }
        let bytes = self.artifacts.get(&artifact_ref.uri).await?;
        if FeedbackComparisonCodec::bytes_hash(&bytes) != artifact_ref.content_hash {
            return Err(Self::invalid(
                "Comparison predecessor bytes differ from the terminal hash",
            ));
        }
        let artifact = FeedbackComparisonCodec::decode(&bytes)?;
        artifact.validate_for(params)?;
        Ok(VerifiedComparison {
            reference: FeedbackComparisonArtifactRef {
                feedback_cycle_id: cycle.feedback_cycle_id,
                job_id,
                artifact_id: params.artifact_id,
                input_hash: params.input_hash()?,
                candidate_family_hash: params.candidate_family_hash,
                decision_policy_snapshot_id: params.decision_policy_snapshot_id,
                artifact: artifact_ref,
            },
            artifact,
        })
    }

    async fn build_subject(
        &self,
        cycle: &FeedbackCycleInfo,
        artifact: &FeedbackComparisonArtifact,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
    ) -> QuantResult<FeedbackShadowSubject> {
        let RomanoWolfOutcome::Compared { .. } = artifact.outcome() else {
            return Ok(FeedbackShadowSubject::NoEligibleCandidate {
                reason: FeedbackShadowUnavailableReason::ComparisonInsufficientObservations,
            });
        };
        let Some((result, replay)) = artifact.selected_candidate() else {
            return Ok(FeedbackShadowSubject::NoEligibleCandidate {
                reason: FeedbackShadowUnavailableReason::AllCandidatesRejected,
            });
        };
        let profile = cycle
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(Self::invalid)?;
        let feedback_policy_hash = profile
            .spec
            .feedback_policy
            .content_hash()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let route = BuyModelRoute::try_from(profile.spec.category)
            .map_err(|error| Self::invalid(error.to_string()))?;
        let generation = self
            .serving_generations
            .current_route(route)
            .ok_or_else(|| {
                Self::invalid(format!(
                    "current serving generation has no active route {route:?}"
                ))
            })?
            .published_shadow_identity()
            .map_err(QuantError::from)?;
        if feedback_policy_hash != cycle.feedback_policy_hash
            || generation.research_profile_artifact_id != cycle.research_profile_artifact_id
            || generation.category_scope != profile.spec.category
            || generation.active_model_version_id != cycle.champion_model_version_id
            || generation.active_serving_contract_hash != cycle.champion_serving_contract_hash
            || generation.shadow_model_version_id != replay.model_version_id
            || generation.shadow_serving_contract_hash != replay.serving_contract_hash
            || generation.decision_policy_snapshot_id != decision_policy_snapshot_id
        {
            return Err(Self::invalid(
                "published serving generation differs from eligible F09 subjects, profile, category, or policy",
            ));
        }
        let window_end = self.cycles.database_time().await?;
        let contract = FeedbackShadowContract::try_seal(FeedbackShadowContractInput {
            profile_ref: cycle.profile_ref.clone(),
            feedback_policy_hash,
            category_scope: generation.category_scope,
            decision_policy_snapshot_id: generation.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: generation.decision_policy_snapshot_hash,
            policy_bundle_generation: generation.policy_bundle_generation,
            champion_model_version_id: generation.active_model_version_id,
            champion_serving_contract_hash: generation.active_serving_contract_hash,
            candidate_model_version_id: generation.shadow_model_version_id,
            candidate_serving_contract_hash: generation.shadow_serving_contract_hash,
            observation_window_start: cycle.created_at,
            observation_window_end: window_end,
            minimum_observations: profile.spec.feedback_policy.shadow_minimum_observations,
            required_window_secs: generation.required_shadow_window_secs,
            minimum_topn_overlap: generation.minimum_topn_overlap,
        })?;
        Ok(FeedbackShadowSubject::Candidate {
            candidate_recipe_hash: result.candidate_recipe_hash,
            contract: Box::new(contract),
        })
    }

    fn bind_job(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
        params: FeedbackShadowJobParams,
    ) -> QuantResult<NewResearchJob> {
        let job = NewResearchJob {
            job_id: identity.job_id(),
            feedback_cycle_id: None,
            feedback_stage: None,
            kind: ResearchJobKind::FeedbackShadowReplay,
            status: ResearchJobStatus::Queued,
            model_spec_id: Some(cycle.candidate_family.shared_evaluation().model_spec_id),
            decision_policy_snapshot_id: Some(params.previous.decision_policy_snapshot_id),
            params_json: ResearchJobParams::FeedbackShadowReplay(Box::new(params)),
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
        let identity_matches_cycle = identity.feedback_cycle_id() == cycle.feedback_cycle_id
            && identity.feedback_stage() == FeedbackStage::ShadowReplay;
        if !lease_matches_cycle || !identity_matches_cycle {
            return Err(Self::invalid(
                "ShadowReplay lease, generation, cycle, or job identity is invalid",
            ));
        }
        Ok(())
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidComparisonEvidence {
            detail: detail.into(),
        }
        .into()
    }
}
