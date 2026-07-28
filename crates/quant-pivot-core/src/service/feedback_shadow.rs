//! Exact production-shadow observation execution for F10.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        ports::{
            FeedbackShadowExecutionPort, FeedbackShadowExecutionResult, FeedbackShadowJobParams,
            FeedbackShadowSubject,
        },
        quant::{JobProgressSink, ResearchJobArtifactRef, ShadowObservationQuery},
    },
    types::ResearchJobProgress,
};
use quant_pivot_repository::traits::ShadowComparisonRepository;
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    feedback_shadow::{
        FeedbackShadowEvaluator, FeedbackShadowOutcome, FeedbackShadowReplayArtifact,
        FeedbackShadowReplayArtifactInput, FeedbackShadowReplayCodec,
    },
};
use tokio_util::sync::CancellationToken;

/// Dependencies for [`FeedbackShadowExecutionService`].
pub struct FeedbackShadowExecutionDeps {
    pub observations: Arc<dyn ShadowComparisonRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
}

/// Reads one frozen production-generation window and writes one immutable artifact.
pub struct FeedbackShadowExecutionService {
    observations: Arc<dyn ShadowComparisonRepository>,
    artifacts: Arc<dyn ArtifactStore>,
}

impl FeedbackShadowExecutionService {
    #[must_use]
    pub fn new(deps: FeedbackShadowExecutionDeps) -> Self {
        Self {
            observations: deps.observations,
            artifacts: deps.artifacts,
        }
    }

    async fn evaluate(
        &self,
        params: &FeedbackShadowJobParams,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackShadowOutcome> {
        Self::require_active(cancel, "preflight")?;
        match &params.subject {
            FeedbackShadowSubject::NoEligibleCandidate { reason } => {
                Ok(FeedbackShadowOutcome::NoEligibleCandidate { reason: *reason })
            }
            FeedbackShadowSubject::Candidate { contract, .. } => {
                let query = ShadowObservationQuery {
                    active_model_version_id: contract.champion_model_version_id(),
                    shadow_model_version_id: contract.candidate_model_version_id(),
                    active_serving_contract_hash: contract.champion_serving_contract_hash(),
                    shadow_serving_contract_hash: contract.candidate_serving_contract_hash(),
                    research_profile_artifact_id: contract.profile_ref().artifact_id(),
                    category_scope: contract.category_scope(),
                    decision_policy_snapshot_id: contract.decision_policy_snapshot_id(),
                    decision_policy_snapshot_hash: contract.decision_policy_snapshot_hash(),
                    policy_bundle_generation: contract.policy_bundle_generation(),
                    window_start: contract.observation_window_start(),
                    window_end: contract.observation_window_end(),
                };
                let window = self.observations.observation_window(&query).await?;
                Self::require_active(cancel, "observation_query")?;
                FeedbackShadowEvaluator::evaluate(contract, &window).map_err(Into::into)
            }
        }
    }

    async fn persist(
        &self,
        artifact: FeedbackShadowReplayArtifact,
    ) -> QuantResult<FeedbackShadowExecutionResult> {
        let artifact_id = artifact.artifact_id();
        let bytes = FeedbackShadowReplayCodec::encode(&artifact)?;
        let content_hash = FeedbackShadowReplayCodec::bytes_hash(&bytes);
        let key = ArtifactKey::new(
            ArtifactNamespace::FeedbackShadowReplay,
            content_hash.hex(),
            "json",
        )?;
        let uri = self.artifacts.put(key, &bytes).await?;
        let persisted = self.artifacts.get(&uri).await?;
        if FeedbackShadowReplayCodec::bytes_hash(&persisted) != content_hash
            || FeedbackShadowReplayCodec::decode(&persisted)? != artifact
        {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: content_hash.to_string(),
                actual: FeedbackShadowReplayCodec::bytes_hash(&persisted).to_string(),
            }
            .into());
        }
        Ok(FeedbackShadowExecutionResult {
            artifact_id,
            artifact: ResearchJobArtifactRef { uri, content_hash },
        })
    }

    fn require_active(cancel: &CancellationToken, phase: &'static str) -> QuantResult<()> {
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: format!("feedback shadow {phase} cancelled"),
            }
            .into());
        }
        Ok(())
    }
}

#[async_trait]
impl FeedbackShadowExecutionPort for FeedbackShadowExecutionService {
    async fn execute(
        &self,
        params: FeedbackShadowJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackShadowExecutionResult> {
        params.validate()?;
        progress.report(ResearchJobProgress::indeterminate("shadow_preflight", 0));
        let outcome = self.evaluate(&params, &cancel).await?;
        Self::require_active(&cancel, "artifact_seal")?;
        progress.report(ResearchJobProgress::indeterminate("shadow_artifact", 0));
        let artifact = FeedbackShadowReplayArtifact::try_seal(FeedbackShadowReplayArtifactInput {
            artifact_id: params.artifact_id,
            feedback_cycle_id: params.feedback_cycle_id,
            job_input_hash: params.input_hash()?,
            previous: params.previous.clone(),
            profile_ref: params.profile_ref.clone(),
            feedback_policy_hash: params.feedback_policy_hash,
            subject: params.subject.clone(),
            outcome,
        })?;
        self.persist(artifact).await
    }
}
