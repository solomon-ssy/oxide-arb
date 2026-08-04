//! Production execution of one atomic route-owned shadow binding.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::{QuantResult, feedback::FeedbackError, research::ResearchError};
use quant_pivot_models::{
    domain::{
        ports::{
            CancelShadowBinding, CommittedPolicyApplyPort, ShadowBindingArtifact,
            ShadowBindingExecutionPort, ShadowBindingExecutionResult, ShadowBindingJobParams,
        },
        quant::{FeedbackCycleInfo, JobProgressSink, ResearchJobArtifactRef},
    },
    enums::quant::{FeedbackCycleStatus, ShadowBindingStatus},
    hashing::CanonicalDigest,
    runtime_config::{ActivePolicyBundle, ModelBindingSource, PolicyBundleIdentity},
    types::{PolicyIdempotencyKey, ResearchJobProgress, ShadowBindingArtifactId},
};
use quant_pivot_repository::traits::{ModelRouteShadowBindingRepository, PolicyRepository};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    feedback_shadow_binding::ShadowBindingCodec,
};
use tokio_util::sync::CancellationToken;

use crate::service::{
    feedback_coordinator::FeedbackShadowCancellationPort,
    model_route_evidence::ModelRouteEvidenceService,
};

const CANCELLATION_NOTE: &str =
    "feedback coordinator released the route shadow before cycle finalization";

pub struct ShadowBindingExecutionDeps {
    pub bindings: Arc<dyn ModelRouteShadowBindingRepository>,
    pub policy_apply: Arc<dyn CommittedPolicyApplyPort>,
    pub route_evidence: Arc<ModelRouteEvidenceService>,
    pub artifacts: Arc<dyn ArtifactStore>,
}

pub struct ShadowBindingExecutionService {
    bindings: Arc<dyn ModelRouteShadowBindingRepository>,
    policy_apply: Arc<dyn CommittedPolicyApplyPort>,
    route_evidence: Arc<ModelRouteEvidenceService>,
    artifacts: Arc<dyn ArtifactStore>,
}

pub struct ShadowBindingCancellationDeps {
    pub bindings: Arc<dyn ModelRouteShadowBindingRepository>,
    pub policies: Arc<dyn PolicyRepository>,
    pub policy_apply: Arc<dyn CommittedPolicyApplyPort>,
}

/// Fail-closed cancellation cleanup for route-owned shadow slots.
pub struct ShadowBindingCancellationService {
    bindings: Arc<dyn ModelRouteShadowBindingRepository>,
    policies: Arc<dyn PolicyRepository>,
    policy_apply: Arc<dyn CommittedPolicyApplyPort>,
}

impl ShadowBindingCancellationService {
    #[must_use]
    pub fn new(deps: ShadowBindingCancellationDeps) -> Self {
        Self {
            bindings: deps.bindings,
            policies: deps.policies,
            policy_apply: deps.policy_apply,
        }
    }

    fn conflict(detail: impl Into<String>) -> FeedbackError {
        FeedbackError::ShadowBindingConflict {
            detail: detail.into(),
        }
    }

    async fn converge(&self, committed: Option<&ActivePolicyBundle>) -> QuantResult<()> {
        let current = self
            .policies
            .load_current_bundle()
            .await?
            .ok_or_else(|| Self::conflict("shadow cancellation has no active policy bundle"))?;
        if let Some(committed) = committed {
            if current.generation < committed.generation {
                return Err(Self::conflict(
                    "durable policy generation regressed after shadow cancellation",
                )
                .into());
            }
            if current.generation == committed.generation
                && PolicyBundleIdentity::from(&current) != PolicyBundleIdentity::from(committed)
            {
                return Err(Self::conflict(
                    "shadow cancellation generation resolved to another snapshot identity",
                )
                .into());
            }
        }
        let expected = PolicyBundleIdentity::from(&current);
        let readiness = self.policy_apply.apply_committed(current).await?;
        if !readiness.is_ready()
            || readiness.desired() != expected
            || readiness.applied() != expected
        {
            return Err(FeedbackError::ModelRouteConvergenceConflict {
                detail: "cancelled shadow policy did not converge to current durable truth"
                    .to_owned(),
            }
            .into());
        }
        Ok(())
    }
}

#[async_trait]
impl FeedbackShadowCancellationPort for ShadowBindingCancellationService {
    async fn release_cycle(&self, cycle: &FeedbackCycleInfo, reason_code: &str) -> QuantResult<()> {
        cycle.validate()?;
        if cycle.status != FeedbackCycleStatus::Running
            || cycle.decision.is_some()
            || cycle.cancel_requested_at.is_none()
        {
            return Err(Self::conflict(
                "shadow cancellation requires a running cycle with a governed cancellation request",
            )
            .into());
        }
        let binding_id = ShadowBindingArtifactId::from_cycle_id(cycle.feedback_cycle_id);
        let Some(lifecycle) = self.bindings.find_lifecycle(&binding_id).await? else {
            return Ok(());
        };
        if lifecycle.feedback_cycle_id != cycle.feedback_cycle_id
            || lifecycle.binding_id != binding_id
            || lifecycle.route != cycle.route
        {
            return Err(Self::conflict(
                "shadow cancellation lifecycle differs from its feedback cycle",
            )
            .into());
        }
        match lifecycle.status {
            ShadowBindingStatus::Cancelled => self.converge(None).await,
            ShadowBindingStatus::Rejected | ShadowBindingStatus::Promoted => {
                Err(FeedbackError::InvalidCoordinatorState {
                    detail: format!(
                        "running cancelled cycle {} owns terminal shadow status {}",
                        cycle.feedback_cycle_id, lifecycle.status
                    ),
                }
                .into())
            }
            ShadowBindingStatus::Active => {
                let current =
                    self.policies.load_current_bundle().await?.ok_or_else(|| {
                        Self::conflict("shadow cancellation has no policy bundle")
                    })?;
                let route = current
                    .snapshot
                    .model_routing
                    .model
                    .route_binding(cycle.route)
                    .map_err(|error| Self::conflict(error.to_string()))?;
                let Some(shadow) = &route.shadow else {
                    return Err(Self::conflict(
                        "active shadow cancellation binding has an empty route slot",
                    )
                    .into());
                };
                if route.champion.model_version_id != lifecycle.champion_model_version_id
                    || shadow.model_version_id != lifecycle.candidate_model_version_id
                    || shadow.generation != lifecycle.binding_generation
                    || shadow.source
                        != (ModelBindingSource::Feedback {
                            feedback_cycle_id: cycle.feedback_cycle_id,
                        })
                {
                    return Err(Self::conflict(
                        "active shadow cancellation binding differs from model-routing truth",
                    )
                    .into());
                }
                let idempotency_key =
                    PolicyIdempotencyKey::parse(format!("shadow-cancel:{}", lifecycle.binding_id))
                        .map_err(|error| Self::conflict(error.to_string()))?;
                let command = CancelShadowBinding {
                    binding_id: lifecycle.binding_id,
                    feedback_cycle_id: cycle.feedback_cycle_id,
                    expected_lifecycle_generation: lifecycle.lifecycle_generation,
                    expected_binding_generation: lifecycle.binding_generation,
                    expected_policy_generation: current.generation,
                    idempotency_key,
                    reason_code: reason_code.to_owned(),
                    note: CANCELLATION_NOTE.to_owned(),
                };
                let commit = self.bindings.cancel(command).await?;
                self.converge(Some(&commit.bundle)).await
            }
        }
    }
}

impl ShadowBindingExecutionService {
    #[must_use]
    pub fn new(deps: ShadowBindingExecutionDeps) -> Self {
        Self {
            bindings: deps.bindings,
            policy_apply: deps.policy_apply,
            route_evidence: deps.route_evidence,
            artifacts: deps.artifacts,
        }
    }

    fn require_active(cancel: &CancellationToken) -> QuantResult<()> {
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: "route-owned shadow binding cancelled".to_owned(),
            }
            .into());
        }
        Ok(())
    }

    async fn persist(
        &self,
        artifact: &ShadowBindingArtifact,
    ) -> QuantResult<ResearchJobArtifactRef> {
        let bytes = ShadowBindingCodec::encode(artifact)?;
        let content_hash = CanonicalDigest::content_hash_bytes(&bytes);
        let key = ArtifactKey::new(
            ArtifactNamespace::FeedbackShadowBinding,
            artifact.artifact_id.to_string(),
            "json",
        )?;
        let uri = self.artifacts.put(key, &bytes).await?;
        let persisted = self.artifacts.get(&uri).await?;
        let actual_hash = CanonicalDigest::content_hash_bytes(&persisted);
        if actual_hash != content_hash || ShadowBindingCodec::decode(&persisted)? != *artifact {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: content_hash.to_string(),
                actual: actual_hash.to_string(),
            }
            .into());
        }
        Ok(ResearchJobArtifactRef { uri, content_hash })
    }
}

#[async_trait]
impl ShadowBindingExecutionPort for ShadowBindingExecutionService {
    async fn bind_shadow(
        &self,
        params: ShadowBindingJobParams,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<ShadowBindingExecutionResult> {
        params.validate()?;
        Self::require_active(&cancel)?;
        progress.report(ResearchJobProgress::with_total(
            "verify_candidate_runtime",
            0,
            3,
        ));
        let candidate = self
            .route_evidence
            .load_model(params.candidate_model_version_id)
            .await?;
        self.route_evidence.load_runtime(&candidate).await?;
        Self::require_active(&cancel)?;
        progress.report(ResearchJobProgress::with_total(
            "commit_shadow_binding",
            1,
            3,
        ));
        let commit = self.bindings.commit(params.clone()).await?;
        Self::require_active(&cancel)?;
        progress.report(ResearchJobProgress::with_total("converge_runtime", 2, 3));
        let readiness = self
            .policy_apply
            .apply_committed(commit.bundle.clone())
            .await?;
        let committed_identity = PolicyBundleIdentity::from(&commit.bundle);
        if !readiness.is_ready()
            || readiness.desired() != committed_identity
            || readiness.applied() != committed_identity
        {
            return Err(FeedbackError::ModelRouteConvergenceConflict {
                detail: "shadow-binding policy did not converge to its exact committed generation"
                    .to_owned(),
            }
            .into());
        }
        let artifact = ShadowBindingArtifact::try_seal(&params, commit.receipt)?;
        let artifact_ref = self.persist(&artifact).await?;
        progress.report(ResearchJobProgress::with_total("complete", 3, 3));
        Ok(ShadowBindingExecutionResult {
            artifact_id: artifact.artifact_id,
            artifact: artifact_ref,
        })
    }
}
