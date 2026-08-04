//! Exact stage bindings for canonical truth, attribution planning, and validation.

use std::sync::Arc;

use quant_pivot_error::{QuantError, QuantResult, feedback::FeedbackError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        ports::{
            FeedbackAttributionJobParams, FeedbackAttributionManifest, FeedbackCandidateValidation,
            FeedbackLearningStageArtifactRef, FeedbackTruthFreezeArtifact,
            FeedbackTruthFreezeJobParams, FeedbackValidationArtifact,
            FeedbackValidationArtifactRef, FeedbackValidationJobParams,
            FeedbackValidationTrialOutcome,
        },
        quant::{
            FeedbackCycleInfo, FeedbackStageJobIdentity, NewResearchJob, ResearchJobArtifactRef,
            ResearchJobInfo, ResearchJobResultRef,
        },
    },
    enums::quant::{
        FeedbackDecision, FeedbackStage, FeedbackStageEventKind, ResearchJobKind,
        ResearchJobResultKind, ResearchJobStatus,
    },
    types::{ContentHash, ResearchJobParams, RoleCode},
};
use quant_pivot_repository::traits::{FeedbackCycleRepository, ResearchJobRepository};
use quant_pivot_research::{
    artifact::ArtifactStore,
    feedback::{FeedbackCoverageArtifact, FeedbackCoverageCodec},
    feedback_governance::FeedbackGovernanceCodec,
    feedback_learning::{
        FeedbackCpcvStageResult, FeedbackLearningStageArtifact, FeedbackLearningStageResults,
    },
};
use uuid::Uuid;

use crate::service::{
    feedback_coordinator::FeedbackStageSuccess,
    feedback_learning_stage::FeedbackLearningStageAdapter,
};

const QUALITY_GATE_REJECTED_REASON: &str = "quality_gate_all_candidates_rejected";

pub(crate) struct VerifiedValidation {
    pub reference: FeedbackValidationArtifactRef,
    pub artifact: FeedbackValidationArtifact,
}

/// Dependencies for [`FeedbackGovernanceStageAdapter`].
pub struct FeedbackGovernanceStageDeps {
    pub cycles: Arc<dyn FeedbackCycleRepository>,
    pub jobs: Arc<dyn ResearchJobRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub learning: Arc<FeedbackLearningStageAdapter>,
    pub max_recovery_attempts: i32,
}

/// Owns the three governance boundaries inserted into the feedback DAG.
pub struct FeedbackGovernanceStageAdapter {
    cycles: Arc<dyn FeedbackCycleRepository>,
    jobs: Arc<dyn ResearchJobRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    learning: Arc<FeedbackLearningStageAdapter>,
    max_recovery_attempts: i32,
}

impl FeedbackGovernanceStageAdapter {
    pub fn try_new(deps: FeedbackGovernanceStageDeps) -> Result<Self, FeedbackError> {
        if deps.max_recovery_attempts < 0 {
            return Err(FeedbackError::InvalidCoordinatorConfig {
                detail: "feedback job recovery cap cannot be negative".to_owned(),
            });
        }
        Ok(Self {
            cycles: deps.cycles,
            jobs: deps.jobs,
            artifacts: deps.artifacts,
            learning: deps.learning,
            max_recovery_attempts: deps.max_recovery_attempts,
        })
    }

    pub fn prepare_truth(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        Self::require_identity(cycle, identity, FeedbackStage::TruthFreeze)?;
        let params = FeedbackTruthFreezeJobParams::try_new(
            cycle.feedback_cycle_id,
            cycle.idempotency_hash,
            cycle.label_cutoff,
        )?;
        self.bind_job(identity, ResearchJobParams::FeedbackTruthFreeze(params))
    }

    pub async fn prepare_attribution(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        Self::require_identity(cycle, identity, FeedbackStage::Attribution)?;
        let (job, result) = self
            .load_stage_job(
                cycle,
                FeedbackStage::TruthFreeze,
                ResearchJobKind::FeedbackTruthFreeze,
                ResearchJobResultKind::FeedbackTruthFreezeArtifact,
            )
            .await?;
        self.verify_truth(cycle, &job, &result).await?;
        let coverage = self.load_coverage(cycle).await?;
        let generated_at = self.cycles.database_time().await?;
        let params = FeedbackAttributionJobParams::try_new(
            cycle.feedback_cycle_id,
            cycle.idempotency_hash,
            cycle.label_cutoff,
            generated_at,
            coverage.evaluation_window,
            result,
        )?;
        self.bind_job(identity, ResearchJobParams::FeedbackAttribution(params))
    }

    pub async fn prepare_validation(
        &self,
        cycle: &FeedbackCycleInfo,
        identity: FeedbackStageJobIdentity,
    ) -> QuantResult<NewResearchJob> {
        Self::require_identity(cycle, identity, FeedbackStage::Validation)?;
        let (cpcv, _) = self.load_cpcv(cycle).await?;
        let evaluated_at = self.cycles.database_time().await?;
        let params = FeedbackValidationJobParams::try_new(
            cycle.feedback_cycle_id,
            cycle.idempotency_hash,
            evaluated_at,
            cpcv,
        )?;
        self.bind_job(identity, ResearchJobParams::FeedbackValidation(params))
    }

    pub async fn succeeded_truth(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        let result = Self::require_result(
            cycle,
            job,
            FeedbackStage::TruthFreeze,
            ResearchJobKind::FeedbackTruthFreeze,
            ResearchJobResultKind::FeedbackTruthFreezeArtifact,
        )?;
        self.verify_truth(cycle, job, &result).await?;
        Ok(FeedbackStageSuccess::advance(
            result.uri,
            result.content_hash,
        ))
    }

    pub async fn succeeded_attribution(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        let result = Self::require_result(
            cycle,
            job,
            FeedbackStage::Attribution,
            ResearchJobKind::FeedbackAttribution,
            ResearchJobResultKind::FeedbackAttributionManifest,
        )?;
        self.verify_attribution(cycle, job, &result).await?;
        Ok(FeedbackStageSuccess::advance(
            result.uri,
            result.content_hash,
        ))
    }

    pub async fn succeeded_validation(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
    ) -> QuantResult<FeedbackStageSuccess> {
        let result = Self::require_result(
            cycle,
            job,
            FeedbackStage::Validation,
            ResearchJobKind::FeedbackValidation,
            ResearchJobResultKind::FeedbackValidationArtifact,
        )?;
        let verified = self.verify_validation(cycle, job, result.clone()).await?;
        if verified
            .artifact
            .candidates
            .iter()
            .any(FeedbackCandidateValidation::is_comparison_eligible)
        {
            Ok(FeedbackStageSuccess::advance(
                result.uri,
                result.content_hash,
            ))
        } else {
            FeedbackStageSuccess::try_complete(
                result.uri,
                result.content_hash,
                FeedbackDecision::ChallengerRejected,
                QUALITY_GATE_REJECTED_REASON.to_owned(),
            )
            .map_err(Into::into)
        }
    }

    pub(crate) async fn load_validation(
        &self,
        cycle: &FeedbackCycleInfo,
    ) -> QuantResult<VerifiedValidation> {
        let (job, result) = self
            .load_stage_job(
                cycle,
                FeedbackStage::Validation,
                ResearchJobKind::FeedbackValidation,
                ResearchJobResultKind::FeedbackValidationArtifact,
            )
            .await?;
        self.verify_validation(cycle, &job, result).await
    }

    async fn verify_truth(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
        result: &ResearchJobArtifactRef,
    ) -> QuantResult<FeedbackTruthFreezeArtifact> {
        let ResearchJobParams::FeedbackTruthFreeze(params) = &job.params_json else {
            return Err(Self::invalid("TruthFreeze job lost its typed parameters"));
        };
        params.validate()?;
        let bytes = self.artifacts.get(&result.uri).await?;
        Self::require_hash(
            result,
            FeedbackGovernanceCodec::bytes_hash(&bytes),
            "TruthFreeze",
        )?;
        let artifact = FeedbackGovernanceCodec::decode_truth(&bytes)?;
        let job_matches_cycle = (
            &params.feedback_cycle_id,
            &params.cycle_idempotency_hash,
            &params.cutoff,
        ) == (
            &cycle.feedback_cycle_id,
            &cycle.idempotency_hash,
            &cycle.label_cutoff,
        );
        let artifact_matches_cycle = (
            &artifact.feedback_cycle_id,
            &artifact.cycle_idempotency_hash,
            &artifact.cutoff,
        ) == (
            &cycle.feedback_cycle_id,
            &cycle.idempotency_hash,
            &cycle.label_cutoff,
        );
        let artifact_matches_job = artifact.artifact_id == params.artifact_id
            && artifact.input_hash == params.input_hash()?;
        if !job_matches_cycle
            || !artifact_matches_cycle
            || !artifact_matches_job
            || !artifact.blockers.is_empty()
        {
            return Err(Self::invalid(
                "TruthFreeze artifact differs from its cycle, job, or complete barrier",
            ));
        }
        Ok(artifact)
    }

    async fn verify_attribution(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
        result: &ResearchJobArtifactRef,
    ) -> QuantResult<FeedbackAttributionManifest> {
        let ResearchJobParams::FeedbackAttribution(params) = &job.params_json else {
            return Err(Self::invalid(
                "AttributionManifest job lost its typed parameters",
            ));
        };
        params.validate()?;
        let (truth_job, truth_ref) = self
            .load_stage_job(
                cycle,
                FeedbackStage::TruthFreeze,
                ResearchJobKind::FeedbackTruthFreeze,
                ResearchJobResultKind::FeedbackTruthFreezeArtifact,
            )
            .await?;
        self.verify_truth(cycle, &truth_job, &truth_ref).await?;
        let bytes = self.artifacts.get(&result.uri).await?;
        Self::require_hash(
            result,
            FeedbackGovernanceCodec::bytes_hash(&bytes),
            "AttributionManifest",
        )?;
        let artifact = FeedbackGovernanceCodec::decode_attribution(&bytes)?;
        let job_matches_cycle = (
            &params.feedback_cycle_id,
            &params.cycle_idempotency_hash,
            &params.cutoff,
        ) == (
            &cycle.feedback_cycle_id,
            &cycle.idempotency_hash,
            &cycle.label_cutoff,
        );
        let job_matches_truth = params.truth_artifact == truth_ref;
        let artifact_matches_cycle = (
            &artifact.feedback_cycle_id,
            &artifact.cycle_idempotency_hash,
            &artifact.cutoff,
        ) == (
            &cycle.feedback_cycle_id,
            &cycle.idempotency_hash,
            &cycle.label_cutoff,
        );
        let artifact_matches_job = artifact.artifact_id == params.artifact_id
            && artifact.input_hash == params.input_hash()?;
        if !job_matches_cycle
            || !job_matches_truth
            || !artifact_matches_cycle
            || !artifact_matches_job
            || artifact.truth_artifact != truth_ref
        {
            return Err(Self::invalid(
                "AttributionManifest artifact differs from its cycle, job, or TruthFreeze lineage",
            ));
        }
        Ok(artifact)
    }

    async fn verify_validation(
        &self,
        cycle: &FeedbackCycleInfo,
        job: &ResearchJobInfo,
        result: ResearchJobArtifactRef,
    ) -> QuantResult<VerifiedValidation> {
        let ResearchJobParams::FeedbackValidation(params) = &job.params_json else {
            return Err(Self::invalid("Validation job lost its typed parameters"));
        };
        params.validate()?;
        let (cpcv, cpcv_artifact) = self.load_cpcv(cycle).await?;
        let bytes = self.artifacts.get(&result.uri).await?;
        Self::require_hash(
            &result,
            FeedbackGovernanceCodec::bytes_hash(&bytes),
            "Validation",
        )?;
        let artifact = FeedbackGovernanceCodec::decode_validation(&bytes)?;
        let job_matches_cycle = (&params.feedback_cycle_id, &params.cycle_idempotency_hash)
            == (&cycle.feedback_cycle_id, &cycle.idempotency_hash);
        let artifact_matches_cycle = (
            &artifact.feedback_cycle_id,
            &artifact.cycle_idempotency_hash,
        ) == (&cycle.feedback_cycle_id, &cycle.idempotency_hash);
        let artifact_matches_job = artifact.artifact_id == params.artifact_id
            && artifact.input_hash == params.input_hash()?
            && artifact.evaluated_at == params.evaluated_at;
        if !job_matches_cycle
            || params.cpcv != cpcv
            || !artifact_matches_cycle
            || !artifact_matches_job
            || !Self::trials_match(&cpcv_artifact, &artifact)
        {
            return Err(Self::invalid(
                "Validation artifact differs from its complete CPCV trial universe",
            ));
        }
        Ok(VerifiedValidation {
            reference: FeedbackValidationArtifactRef {
                feedback_cycle_id: cycle.feedback_cycle_id,
                job_id: job.job_id,
                artifact_id: params.artifact_id,
                input_hash: params.input_hash()?,
                cpcv,
                artifact: result,
            },
            artifact,
        })
    }

    async fn load_cpcv(
        &self,
        cycle: &FeedbackCycleInfo,
    ) -> QuantResult<(
        FeedbackLearningStageArtifactRef,
        FeedbackLearningStageArtifact,
    )> {
        let (job, result) = self
            .load_stage_job(
                cycle,
                FeedbackStage::Cpcv,
                ResearchJobKind::FeedbackCpcv,
                ResearchJobResultKind::FeedbackLearningStageArtifact,
            )
            .await?;
        let ResearchJobParams::FeedbackCpcv(params) = &job.params_json else {
            return Err(Self::invalid("CPCV predecessor lost its typed parameters"));
        };
        params.validate()?;
        let reference = FeedbackLearningStageArtifactRef {
            feedback_cycle_id: cycle.feedback_cycle_id,
            stage: FeedbackStage::Cpcv,
            job_id: job.job_id,
            artifact_id: params.artifact_id,
            input_hash: params.input_hash()?,
            artifact: result,
        };
        let artifact = self.learning.verify_reference(cycle, &reference).await?;
        Ok((reference, artifact))
    }

    async fn load_coverage(
        &self,
        cycle: &FeedbackCycleInfo,
    ) -> QuantResult<FeedbackCoverageArtifact> {
        let (job, result) = self
            .load_stage_job(
                cycle,
                FeedbackStage::Coverage,
                ResearchJobKind::FeedbackCoverage,
                ResearchJobResultKind::FeedbackCoverageArtifact,
            )
            .await?;
        let ResearchJobParams::FeedbackCoverage(params) = &job.params_json else {
            return Err(Self::invalid(
                "Coverage predecessor lost its typed parameters",
            ));
        };
        let bytes = self.artifacts.get(&result.uri).await?;
        Self::require_hash(
            &result,
            FeedbackCoverageCodec::bytes_hash(&bytes),
            "Coverage",
        )?;
        let artifact = FeedbackCoverageCodec::decode(&bytes)?;
        let cycle_identity_exact = artifact.cycle_idempotency_hash == cycle.idempotency_hash;
        if artifact.feedback_cycle_id != cycle.feedback_cycle_id
            || !cycle_identity_exact
            || artifact.artifact_id != params.artifact_id
        {
            return Err(Self::invalid(
                "Coverage predecessor differs from the cycle lineage",
            ));
        }
        Ok(artifact)
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
            .ok_or_else(|| Self::invalid(format!("{stage} success event has no job identity")))?;
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
                "{stage} job result differs from its WORM success event"
            )));
        }
        Ok((job, result))
    }

    fn trials_match(
        cpcv: &FeedbackLearningStageArtifact,
        validation: &FeedbackValidationArtifact,
    ) -> bool {
        let FeedbackLearningStageResults::Cpcv(results) = &cpcv.results else {
            return false;
        };
        results.len() == validation.candidates.len()
            && results
                .iter()
                .zip(&validation.candidates)
                .all(|(result, candidate)| {
                    let outcome = match result {
                        FeedbackCpcvStageResult::Evaluated { .. } => {
                            FeedbackValidationTrialOutcome::CpcvEvaluated
                        }
                        FeedbackCpcvStageResult::CalibrationInsufficient { .. } => {
                            FeedbackValidationTrialOutcome::CalibrationInsufficient
                        }
                    };
                    result.candidate_recipe_hash() == candidate.candidate_recipe_hash
                        && result.model_version_id() == candidate.model_version_id
                        && outcome == candidate.trial_outcome
                })
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
                    id: Self::result_id(&job.params_json, stage)?,
                })
        {
            return Err(Self::invalid(format!(
                "{stage} job has invalid cycle, kind, status, or result lineage"
            )));
        }
        Ok(result)
    }

    fn require_hash(
        result: &ResearchJobArtifactRef,
        actual: ContentHash,
        stage: &'static str,
    ) -> QuantResult<()> {
        if result.content_hash != actual {
            return Err(Self::invalid(format!(
                "{stage} object bytes differ from its terminal hash"
            )));
        }
        Ok(())
    }

    fn result_id(params: &ResearchJobParams, stage: FeedbackStage) -> QuantResult<Uuid> {
        let id = match (params, stage) {
            (ResearchJobParams::FeedbackTruthFreeze(params), FeedbackStage::TruthFreeze) => {
                params.artifact_id.as_uuid()
            }
            (ResearchJobParams::FeedbackCoverage(params), FeedbackStage::Coverage) => {
                params.artifact_id.as_uuid()
            }
            (ResearchJobParams::FeedbackAttribution(params), FeedbackStage::Attribution) => {
                params.artifact_id.as_uuid()
            }
            (ResearchJobParams::FeedbackCpcv(params), FeedbackStage::Cpcv) => {
                params.artifact_id.as_uuid()
            }
            (ResearchJobParams::FeedbackValidation(params), FeedbackStage::Validation) => {
                params.artifact_id.as_uuid()
            }
            _ => {
                return Err(Self::invalid(format!(
                    "{stage} job lost its exact typed result identity"
                )));
            }
        };
        Ok(id)
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

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidCoordinatorState {
            detail: detail.into(),
        }
        .into()
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        domain::api::FeedbackCoverageJobParams,
        enums::quant::FeedbackStage,
        types::{ContentHash, FeedbackCoverageArtifactId, FeedbackCycleId, ResearchJobParams},
    };

    use super::FeedbackGovernanceStageAdapter;

    #[test]
    fn coverage_result_id_matches() {
        let feedback_cycle_id = FeedbackCycleId::from_v7();
        let artifact_id = FeedbackCoverageArtifactId::from_cycle_id(feedback_cycle_id);
        let params = ResearchJobParams::FeedbackCoverage(FeedbackCoverageJobParams {
            feedback_cycle_id,
            cycle_idempotency_hash: ContentHash::from_bytes([7; 32]),
            artifact_id,
        });

        let result = FeedbackGovernanceStageAdapter::result_id(&params, FeedbackStage::Coverage)
            .expect("coverage result identity must remain typed");

        assert_eq!(result, artifact_id.as_uuid());
    }
}
