//! Canonical truth, attribution-manifest, and sole quality-gate execution.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use quant_pivot_error::{
    QuantError, QuantResult, feedback::FeedbackError, research::ResearchError,
};
use quant_pivot_models::{
    domain::{
        ports::{
            CandidateQualityGateEvidence, FeedbackAttributionJobParams,
            FeedbackAttributionManifest, FeedbackAttributionProduced, FeedbackAttributionUse,
            FeedbackCandidateValidation, FeedbackGovernanceExecutionPort,
            FeedbackGovernanceExecutionResult, FeedbackTruthFreezeArtifact,
            FeedbackTruthFreezeJobParams, FeedbackValidationArtifact, FeedbackValidationJobParams,
            FeedbackValidationTrialOutcome, ModelGovernancePort,
        },
        quant::{JobProgressSink, ResearchJobArtifactRef},
    },
    enums::quant::FeedbackStage,
    hashing::CanonicalDigest,
    types::{
        ContentHash, FeedbackAttributionManifestId, FeedbackTruthFreezeArtifactId,
        FeedbackValidationArtifactId, ResearchJobProgress,
    },
};
use quant_pivot_repository::traits::{
    AttributionArtifactRepository, ExecutionAttemptOutcomeRepository,
    RecommendationExecutionRollupRepository, ResolutionObservationRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    feedback_governance::FeedbackGovernanceCodec,
    feedback_learning::{
        FeedbackCpcvStageResult, FeedbackLearningStageArtifact, FeedbackLearningStageCodec,
        FeedbackLearningStageResults,
    },
};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::{
    observability::metrics_hub::MetricsHub,
    service::feedback_attribution::FeedbackAttributionMaterializer,
};

const TRUTH_RETRY_INTERVAL: Duration = Duration::from_secs(5);

pub struct FeedbackGovernanceExecutionDeps {
    pub resolutions: Arc<dyn ResolutionObservationRepository>,
    pub attempts: Arc<dyn ExecutionAttemptOutcomeRepository>,
    pub rollups: Arc<dyn RecommendationExecutionRollupRepository>,
    pub attribution: Arc<dyn AttributionArtifactRepository>,
    pub attribution_materializer: Arc<FeedbackAttributionMaterializer>,
    pub model_governance: Arc<dyn ModelGovernancePort>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub metrics: Arc<MetricsHub>,
}

pub struct FeedbackGovernanceExecutionService {
    resolutions: Arc<dyn ResolutionObservationRepository>,
    attempts: Arc<dyn ExecutionAttemptOutcomeRepository>,
    rollups: Arc<dyn RecommendationExecutionRollupRepository>,
    attribution: Arc<dyn AttributionArtifactRepository>,
    attribution_materializer: Arc<FeedbackAttributionMaterializer>,
    model_governance: Arc<dyn ModelGovernancePort>,
    artifacts: Arc<dyn ArtifactStore>,
    metrics: Arc<MetricsHub>,
}

impl FeedbackGovernanceExecutionService {
    #[must_use]
    pub fn new(deps: FeedbackGovernanceExecutionDeps) -> Self {
        Self {
            resolutions: deps.resolutions,
            attempts: deps.attempts,
            rollups: deps.rollups,
            attribution: deps.attribution,
            attribution_materializer: deps.attribution_materializer,
            model_governance: deps.model_governance,
            artifacts: deps.artifacts,
            metrics: deps.metrics,
        }
    }

    async fn truth_artifact(
        &self,
        params: &FeedbackTruthFreezeJobParams,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackTruthFreezeArtifact> {
        loop {
            Self::require_active(cancel, "truth_freeze")?;
            let resolution = self.resolutions.barrier(params.cutoff).await?;
            let attempts = self.attempts.barrier(params.cutoff).await?;
            let rollups = self.rollups.barrier(params.cutoff).await?;
            let artifact =
                FeedbackTruthFreezeArtifact::try_new(params, resolution, attempts, rollups)?;
            if artifact.blockers.is_empty() {
                return Ok(artifact);
            }
            progress.report(ResearchJobProgress::indeterminate(
                format!("truth_blocked:{}", artifact.blockers.len()),
                0,
            ));
            tokio::select! {
                () = cancel.cancelled() => {
                    return Err(ResearchError::Cancelled {
                        detail: "feedback truth freeze cancelled while blocked".to_owned(),
                    }
                    .into());
                }
                () = sleep(TRUTH_RETRY_INTERVAL) => {}
            }
        }
    }

    async fn load_truth(
        &self,
        params: &FeedbackAttributionJobParams,
    ) -> QuantResult<FeedbackTruthFreezeArtifact> {
        let bytes = self.artifacts.get(&params.truth_artifact.uri).await?;
        Self::require_hash(
            params.truth_artifact.content_hash,
            FeedbackGovernanceCodec::bytes_hash(&bytes),
        )?;
        let artifact = FeedbackGovernanceCodec::decode_truth(&bytes)?;
        if artifact.feedback_cycle_id != params.feedback_cycle_id
            || artifact.cutoff != params.cutoff
            || !artifact.blockers.is_empty()
        {
            return Err(Self::invalid(
                "attribution plan requires the exact complete truth-freeze artifact",
            ));
        }
        Ok(artifact)
    }

    async fn load_learning(
        &self,
        reference: &ResearchJobArtifactRef,
    ) -> QuantResult<FeedbackLearningStageArtifact> {
        let bytes = self.artifacts.get(&reference.uri).await?;
        Self::require_hash(
            reference.content_hash,
            CanonicalDigest::content_hash_bytes(&bytes),
        )?;
        FeedbackLearningStageCodec::decode(&bytes)
    }

    async fn validation_universe(
        &self,
        params: &FeedbackValidationJobParams,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<Vec<FeedbackCandidateValidation>> {
        let cpcv = self.load_learning(&params.cpcv.artifact).await?;
        if cpcv.feedback_cycle_id != params.feedback_cycle_id
            || cpcv.results.stage() != FeedbackStage::Cpcv
            || cpcv.artifact_id != params.cpcv.artifact_id
            || cpcv.input_hash != params.cpcv.input_hash
        {
            return Err(Self::invalid(
                "validation CPCV object differs from its frozen reference",
            ));
        }
        let FeedbackLearningStageResults::Cpcv(results) = cpcv.results else {
            return Err(Self::invalid("validation input is not a CPCV artifact"));
        };
        let total = u64::try_from(results.len())
            .map_err(|error| Self::invalid(format!("validation batch is too large: {error}")))?;
        let mut candidates = Vec::with_capacity(results.len());
        for (index, result) in results.into_iter().enumerate() {
            Self::require_active(cancel, "validation")?;
            let candidate_recipe_hash = result.candidate_recipe_hash();
            let model_version_id = result.model_version_id();
            let (trial_outcome, gate_evidence) = match result {
                FeedbackCpcvStageResult::Evaluated {
                    path_set_id,
                    path_set_hash,
                    ..
                } => (
                    FeedbackValidationTrialOutcome::CpcvEvaluated,
                    CandidateQualityGateEvidence::Cpcv {
                        path_set_id,
                        path_set_hash,
                    },
                ),
                FeedbackCpcvStageResult::CalibrationInsufficient { .. } => (
                    FeedbackValidationTrialOutcome::CalibrationInsufficient,
                    CandidateQualityGateEvidence::CalibrationInsufficient,
                ),
            };
            let report = self
                .model_governance
                .evaluate_candidate(&model_version_id, gate_evidence, params.evaluated_at)
                .await?;
            for outcome in &report.gates {
                self.metrics.record_feedback_quality_gate(
                    outcome.gate.wire_name(),
                    outcome.status.wire_name(),
                );
            }
            candidates.push(FeedbackCandidateValidation {
                candidate_recipe_hash,
                model_version_id,
                trial_outcome,
                quality_gate_report: report,
            });
            progress.report(ResearchJobProgress::with_total(
                "feedback-validation",
                u64::try_from(index)
                    .map_err(|error| Self::invalid(format!("validation index overflow: {error}")))?
                    .saturating_add(1),
                total,
            ));
        }
        Ok(candidates)
    }

    async fn persist_truth(
        &self,
        artifact: FeedbackTruthFreezeArtifact,
    ) -> QuantResult<FeedbackGovernanceExecutionResult<FeedbackTruthFreezeArtifactId>> {
        let artifact_id = artifact.artifact_id;
        let bytes = FeedbackGovernanceCodec::encode_truth(&artifact)?;
        let reference = self
            .persist(
                ArtifactNamespace::FeedbackTruth,
                &bytes,
                FeedbackGovernanceCodec::decode_truth,
            )
            .await?;
        Ok(FeedbackGovernanceExecutionResult {
            artifact_id,
            artifact: reference,
        })
    }

    async fn persist_attribution(
        &self,
        artifact: FeedbackAttributionManifest,
    ) -> QuantResult<FeedbackGovernanceExecutionResult<FeedbackAttributionManifestId>> {
        let artifact_id = artifact.artifact_id;
        let bytes = FeedbackGovernanceCodec::encode_attribution(&artifact)?;
        let reference = self
            .persist(
                ArtifactNamespace::FeedbackAttribution,
                &bytes,
                FeedbackGovernanceCodec::decode_attribution,
            )
            .await?;
        Ok(FeedbackGovernanceExecutionResult {
            artifact_id,
            artifact: reference,
        })
    }

    async fn persist_validation(
        &self,
        artifact: FeedbackValidationArtifact,
    ) -> QuantResult<FeedbackGovernanceExecutionResult<FeedbackValidationArtifactId>> {
        let artifact_id = artifact.artifact_id;
        let bytes = FeedbackGovernanceCodec::encode_validation(&artifact)?;
        let reference = self
            .persist(
                ArtifactNamespace::FeedbackValidation,
                &bytes,
                FeedbackGovernanceCodec::decode_validation,
            )
            .await?;
        Ok(FeedbackGovernanceExecutionResult {
            artifact_id,
            artifact: reference,
        })
    }

    async fn persist<T>(
        &self,
        namespace: ArtifactNamespace,
        bytes: &[u8],
        decode: fn(&[u8]) -> QuantResult<T>,
    ) -> QuantResult<ResearchJobArtifactRef> {
        let content_hash = FeedbackGovernanceCodec::bytes_hash(bytes);
        let key = ArtifactKey::new(namespace, content_hash.hex(), "json")?;
        let uri = self.artifacts.put(key, bytes).await?;
        let persisted = self.artifacts.get(&uri).await?;
        Self::require_hash(
            content_hash,
            FeedbackGovernanceCodec::bytes_hash(&persisted),
        )?;
        decode(&persisted)?;
        Ok(ResearchJobArtifactRef { uri, content_hash })
    }

    fn require_hash(expected: ContentHash, actual: ContentHash) -> QuantResult<()> {
        if expected != actual {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: expected.to_string(),
                actual: actual.to_string(),
            }
            .into());
        }
        Ok(())
    }

    fn require_active(cancel: &CancellationToken, phase: &'static str) -> QuantResult<()> {
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: format!("feedback governance {phase} cancelled"),
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
impl FeedbackGovernanceExecutionPort for FeedbackGovernanceExecutionService {
    async fn freeze_truth(
        &self,
        params: FeedbackTruthFreezeJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackGovernanceExecutionResult<FeedbackTruthFreezeArtifactId>> {
        params.validate()?;
        let artifact = self
            .truth_artifact(&params, progress.as_ref(), &cancel)
            .await?;
        self.persist_truth(artifact).await
    }

    async fn materialize_attribution(
        &self,
        params: FeedbackAttributionJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackGovernanceExecutionResult<FeedbackAttributionManifestId>> {
        params.validate()?;
        Self::require_active(&cancel, "attribution_manifest")?;
        self.load_truth(&params).await?;
        self.attribution_materializer
            .materialize(&params, progress.as_ref(), &cancel)
            .await?;
        let produced = self
            .attribution
            .list_for_cycle(params.feedback_cycle_id)
            .await?
            .into_iter()
            .map(|artifact| FeedbackAttributionProduced {
                attribution_artifact_id: artifact.attribution_artifact_id,
                artifact_kind: artifact.artifact_kind,
                source_cohort: artifact.source_cohort,
                model_version_id: artifact.model_version_id,
                recommendation_id: artifact.recommendation_id,
                order_intent_id: artifact.order_intent_id,
                artifact_uri: artifact.artifact_uri,
                artifact_hash: artifact.artifact_hash,
                source_cutoff: artifact.source_cutoff,
                available_at: artifact.available_at,
            })
            .collect();
        let available = self
            .attribution
            .list_available(params.feedback_cycle_id, params.cutoff)
            .await?;
        let uses = available
            .into_iter()
            .map(|artifact| FeedbackAttributionUse {
                source_feedback_cycle_id: artifact.source_feedback_cycle_id,
                artifact_kind: artifact.artifact_kind,
                source_cohort: artifact.source_cohort,
                artifact_uri: artifact.artifact_uri,
                artifact_hash: artifact.artifact_hash,
                source_cutoff: artifact.source_cutoff,
                available_at: artifact.available_at,
            })
            .collect();
        progress.report(ResearchJobProgress::indeterminate(
            "attribution-manifest-seal",
            0,
        ));
        let artifact = FeedbackAttributionManifest::try_new(&params, uses, produced)?;
        self.persist_attribution(artifact).await
    }

    async fn validate_candidates(
        &self,
        params: FeedbackValidationJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<FeedbackGovernanceExecutionResult<FeedbackValidationArtifactId>> {
        params.validate()?;
        let candidates = self
            .validation_universe(&params, progress.as_ref(), &cancel)
            .await?;
        let artifact = FeedbackValidationArtifact::try_new(&params, candidates)?;
        self.persist_validation(artifact).await
    }
}
