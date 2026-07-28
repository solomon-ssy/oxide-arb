//! Exact predecessor loading and evidence-only F11 decision execution.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{
    QuantError, QuantResult, feedback::FeedbackError, research::ResearchError,
};
use quant_pivot_models::{
    domain::{
        ports::{
            FeedbackDecisionExecutionPort, FeedbackDecisionExecutionResult,
            FeedbackDecisionJobParams,
        },
        quant::{JobProgressSink, ResearchJobArtifactRef},
    },
    types::{ContentHash, ResearchJobProgress},
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    feedback::{FeedbackDriftArtifact, FeedbackDriftCodec},
    feedback_comparison::{FeedbackComparisonArtifact, FeedbackComparisonCodec},
    feedback_decision::{
        FeedbackDecisionArtifact, FeedbackDecisionArtifactInput, FeedbackDecisionCodec,
    },
    feedback_shadow::{FeedbackShadowReplayArtifact, FeedbackShadowReplayCodec},
};
use tokio_util::sync::CancellationToken;

/// Dependencies for [`FeedbackDecisionExecutionService`].
pub struct FeedbackDecisionExecutionDeps {
    pub artifacts: Arc<dyn ArtifactStore>,
}

/// Loads exact F06/F09/F10 objects and seals one immutable terminal decision.
pub struct FeedbackDecisionExecutionService {
    artifacts: Arc<dyn ArtifactStore>,
}

impl FeedbackDecisionExecutionService {
    #[must_use]
    pub fn new(deps: FeedbackDecisionExecutionDeps) -> Self {
        Self {
            artifacts: deps.artifacts,
        }
    }

    async fn load_drift(
        &self,
        params: &FeedbackDecisionJobParams,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackDriftArtifact> {
        Self::require_active(cancel, "drift_load")?;
        let bytes = self.artifacts.get(&params.drift.artifact.uri).await?;
        Self::require_hash(
            "drift",
            params.drift.artifact.content_hash,
            FeedbackDriftCodec::bytes_hash(&bytes),
        )?;
        let artifact = FeedbackDriftCodec::decode(&bytes)?;
        if artifact.artifact_id != params.drift.artifact_id
            || artifact.feedback_cycle_id != params.feedback_cycle_id
        {
            return Err(Self::invalid(
                "Decision drift object differs from its frozen reference",
            ));
        }
        Ok(artifact)
    }

    async fn load_comparison(
        &self,
        params: &FeedbackDecisionJobParams,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackComparisonArtifact> {
        Self::require_active(cancel, "comparison_load")?;
        let bytes = self.artifacts.get(&params.comparison.artifact.uri).await?;
        Self::require_hash(
            "comparison",
            params.comparison.artifact.content_hash,
            FeedbackComparisonCodec::bytes_hash(&bytes),
        )?;
        let artifact = FeedbackComparisonCodec::decode(&bytes)?;
        if artifact.artifact_id() != params.comparison.artifact_id
            || artifact.feedback_cycle_id() != params.feedback_cycle_id
            || artifact.job_input_hash() != params.comparison.input_hash
        {
            return Err(Self::invalid(
                "Decision comparison object differs from its frozen reference",
            ));
        }
        Ok(artifact)
    }

    async fn load_shadow(
        &self,
        params: &FeedbackDecisionJobParams,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackShadowReplayArtifact> {
        Self::require_active(cancel, "shadow_load")?;
        let bytes = self
            .artifacts
            .get(&params.shadow_replay.artifact.uri)
            .await?;
        Self::require_hash(
            "shadow",
            params.shadow_replay.artifact.content_hash,
            FeedbackShadowReplayCodec::bytes_hash(&bytes),
        )?;
        let artifact = FeedbackShadowReplayCodec::decode(&bytes)?;
        if artifact.artifact_id() != params.shadow_replay.artifact_id
            || artifact.feedback_cycle_id() != params.feedback_cycle_id
            || artifact.job_input_hash() != params.shadow_replay.input_hash
        {
            return Err(Self::invalid(
                "Decision shadow object differs from its frozen reference",
            ));
        }
        Ok(artifact)
    }

    async fn persist(
        &self,
        artifact: FeedbackDecisionArtifact,
    ) -> QuantResult<FeedbackDecisionExecutionResult> {
        let artifact_id = artifact.artifact_id();
        let bytes = FeedbackDecisionCodec::encode(&artifact)?;
        let content_hash = FeedbackDecisionCodec::bytes_hash(&bytes);
        let key = ArtifactKey::new(
            ArtifactNamespace::FeedbackDecision,
            content_hash.hex(),
            "json",
        )?;
        let uri = self.artifacts.put(key, &bytes).await?;
        let persisted = self.artifacts.get(&uri).await?;
        if FeedbackDecisionCodec::bytes_hash(&persisted) != content_hash
            || FeedbackDecisionCodec::decode(&persisted)? != artifact
        {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: content_hash.to_string(),
                actual: FeedbackDecisionCodec::bytes_hash(&persisted).to_string(),
            }
            .into());
        }
        Ok(FeedbackDecisionExecutionResult {
            artifact_id,
            artifact: ResearchJobArtifactRef { uri, content_hash },
        })
    }

    fn require_hash(
        owner: &'static str,
        expected: ContentHash,
        actual: ContentHash,
    ) -> QuantResult<()> {
        if expected != actual {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: format!("{owner}:{expected}"),
                actual: format!("{owner}:{actual}"),
            }
            .into());
        }
        Ok(())
    }

    fn require_active(cancel: &CancellationToken, phase: &'static str) -> QuantResult<()> {
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: format!("feedback decision {phase} cancelled"),
            }
            .into());
        }
        Ok(())
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidJobContract {
            detail: detail.into(),
        }
        .into()
    }
}

#[async_trait]
impl FeedbackDecisionExecutionPort for FeedbackDecisionExecutionService {
    async fn execute(
        &self,
        params: FeedbackDecisionJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackDecisionExecutionResult> {
        params.validate()?;
        progress.report(ResearchJobProgress::indeterminate(
            "decision_predecessors",
            0,
        ));
        let drift = self.load_drift(&params, &cancel).await?;
        let comparison = self.load_comparison(&params, &cancel).await?;
        let shadow = self.load_shadow(&params, &cancel).await?;
        Self::require_active(&cancel, "artifact_seal")?;
        progress.report(ResearchJobProgress::indeterminate("decision_artifact", 0));
        let artifact = FeedbackDecisionArtifact::try_seal(FeedbackDecisionArtifactInput {
            params: &params,
            drift: &drift,
            comparison: &comparison,
            shadow: &shadow,
        })?;
        self.persist(artifact).await
    }
}
