//! Governed orchestration for one atomic model-route promotion.

use std::sync::Arc;

use quant_pivot_error::{QuantResult, feedback::FeedbackError};
use quant_pivot_models::{
    domain::{
        ports::{CommittedPolicyApplyPort, RejectShadowBinding},
        quant::{
            BootstrapModelRoute, CommitModelRouteBootstrap, CommitModelRoutePromotion,
            ModelGovernanceAuditDetail, ModelRouteBootstrapActor, ModelRouteBootstrapPreflight,
            PromoteModelRoute,
        },
    },
    enums::runtime_config::PolicyActorKind,
    runtime_config::{ActivePolicyBundle, PolicyBundleIdentity},
    types::{FeedbackCycleId, PolicyActivationId},
};
use quant_pivot_repository::traits::{
    ModelRouteBootstrapCommit, ModelRouteBootstrapRepository, ModelRoutePromotionCommit,
    ModelRoutePromotionRepository, ModelRouteShadowBindingRepository, PolicyRepository,
    ShadowBindingRejectCommit,
};

use super::{
    model_route_bootstrap::ModelRouteBootstrapService,
    promotion_preflight::PromotionPreflightService,
};

/// Dependencies for the sole server-owned model-route activation boundary.
pub struct ModelRouteGovernanceServiceDeps {
    pub bootstrap_preflight: Arc<ModelRouteBootstrapService>,
    pub bootstrap_repository: Arc<dyn ModelRouteBootstrapRepository>,
    pub preflight: Arc<PromotionPreflightService>,
    pub repository: Arc<dyn ModelRoutePromotionRepository>,
    pub shadow_bindings: Arc<dyn ModelRouteShadowBindingRepository>,
    pub policies: Arc<dyn PolicyRepository>,
    pub policy_apply: Arc<dyn CommittedPolicyApplyPort>,
}

/// Resolves current server truth before delegating the only activation write.
pub struct ModelRouteGovernanceService {
    deps: ModelRouteGovernanceServiceDeps,
}

impl ModelRouteGovernanceService {
    #[must_use]
    pub const fn new(deps: ModelRouteGovernanceServiceDeps) -> Self {
        Self { deps }
    }

    /// Load one immutable persisted promotion receipt without mutating runtime
    /// or recomputing a fresh preflight.
    pub async fn find_activation(
        &self,
        policy_activation_id: &PolicyActivationId,
    ) -> QuantResult<Option<ModelRoutePromotionCommit>> {
        self.deps
            .repository
            .find_activation(policy_activation_id)
            .await
            .map_err(Into::into)
    }

    /// Load the unique persisted promotion receipt bound to one feedback
    /// cycle without relying on shadow-termination metadata.
    pub async fn find_cycle_activation(
        &self,
        feedback_cycle_id: &FeedbackCycleId,
    ) -> QuantResult<Option<ModelRoutePromotionCommit>> {
        self.deps
            .repository
            .find_cycle_activation(feedback_cycle_id)
            .await
            .map_err(Into::into)
    }

    async fn converge(&self, bundle: &ActivePolicyBundle) -> QuantResult<()> {
        let current = self
            .deps
            .policies
            .load_current_bundle()
            .await?
            .ok_or_else(|| FeedbackError::ModelRouteConvergenceConflict {
                detail: "committed policy bundle disappeared before runtime apply".to_owned(),
            })?;
        if current.generation < bundle.generation {
            return Err(FeedbackError::ModelRouteConvergenceConflict {
                detail: format!(
                    "durable generation {} is older than promotion generation {}",
                    current.generation, bundle.generation
                ),
            }
            .into());
        }
        if current.generation == bundle.generation
            && PolicyBundleIdentity::from(&current) != PolicyBundleIdentity::from(bundle)
        {
            return Err(FeedbackError::ModelRouteConvergenceConflict {
                detail: "promotion generation resolved to a different durable snapshot identity"
                    .to_owned(),
            }
            .into());
        }
        self.deps.policy_apply.apply_committed(current).await?;
        Ok(())
    }

    fn verify_bootstrap(
        request: &BootstrapModelRoute,
        commit: &ModelRouteBootstrapCommit,
    ) -> QuantResult<()> {
        let ModelGovernanceAuditDetail::BootstrapRoute { record } = &commit.audit.detail else {
            return Err(FeedbackError::BootstrapTransactionConflict {
                detail: "committed bootstrap lost its typed transaction record".to_owned(),
            }
            .into());
        };
        let activation = &commit.activation;
        let actor_exact = match &request.actor {
            ModelRouteBootstrapActor::Operator(actor) => {
                record.actor_kind() == PolicyActorKind::Operator
                    && record.actor_user_id() == Some(actor.user_id)
                    && record.actor_role() == Some(&actor.acting_role)
            }
            ModelRouteBootstrapActor::FreshBootOrchestrator => {
                record.actor_kind() == PolicyActorKind::System
                    && record.actor_user_id().is_none()
                    && record.actor_username() == "fresh_boot_orchestrator"
                    && record.actor_role().is_none()
            }
        };
        let exact = record.preflight().manifest().model_version_id() == request.model_version_id
            && record.preflight().expected_policy_generation()
                == request.expected_policy_generation
            && record.preflight().expected_runtime_revision()
                == request.expected_runtime_control_revision
            && actor_exact
            && record.idempotency_key() == &request.idempotency_key
            && record.reason_code() == request.reason_code
            && record.note() == request.note
            && activation.expected_bundle_generation == request.expected_policy_generation
            && activation.idempotency_key == request.idempotency_key
            && activation.activated_by_user_id == record.actor_user_id()
            && commit.bundle.generation == activation.bundle_generation;
        if !exact {
            return Err(FeedbackError::BootstrapTransactionConflict {
                detail:
                    "bootstrap idempotency key was replayed with actor, revision, or intent drift"
                        .to_owned(),
            }
            .into());
        }
        Ok(())
    }

    /// Fill one previously empty category route with a fully governed first
    /// champion, or replay its exact committed graph.
    pub async fn bootstrap(
        &self,
        request: BootstrapModelRoute,
    ) -> QuantResult<ModelRouteBootstrapCommit> {
        request.validate()?;
        let committed = if let Some(committed) = self
            .deps
            .bootstrap_repository
            .find_committed(&request.idempotency_key)
            .await?
        {
            Self::verify_bootstrap(&request, &committed)?;
            committed
        } else {
            let plan = match Box::pin(
                self.deps
                    .bootstrap_preflight
                    .prepare(request.model_version_id),
            )
            .await
            {
                Ok(plan) => plan,
                Err(error) => {
                    if let Some(committed) = self
                        .deps
                        .bootstrap_repository
                        .find_committed(&request.idempotency_key)
                        .await?
                    {
                        Self::verify_bootstrap(&request, &committed)?;
                        self.converge(&committed.bundle).await?;
                        return Ok(committed);
                    }
                    return Err(error);
                }
            };
            return self
                .bootstrap_prepared(request, plan.preflight().clone())
                .await;
        };
        Self::verify_bootstrap(&request, &committed)?;
        self.converge(&committed.bundle).await?;
        Ok(committed)
    }

    /// Commit the exact preflight already evaluated by a durable orchestrator.
    /// This prevents a second wall-clock evaluation from changing scenario or
    /// quality evidence between the recorded preflight and the transaction.
    pub async fn bootstrap_prepared(
        &self,
        request: BootstrapModelRoute,
        preflight: ModelRouteBootstrapPreflight,
    ) -> QuantResult<ModelRouteBootstrapCommit> {
        request.validate()?;
        let committed = if let Some(committed) = self
            .deps
            .bootstrap_repository
            .find_committed(&request.idempotency_key)
            .await?
        {
            committed
        } else {
            let command = CommitModelRouteBootstrap::try_new(request.clone(), preflight)?;
            self.deps.bootstrap_repository.commit(command).await?
        };
        Self::verify_bootstrap(&request, &committed)?;
        self.converge(&committed.bundle).await?;
        Ok(committed)
    }

    fn verify_replay(
        request: &PromoteModelRoute,
        commit: &ModelRoutePromotionCommit,
    ) -> QuantResult<()> {
        let ModelGovernanceAuditDetail::PromoteRoute { record } = &commit.audit.detail else {
            return Err(FeedbackError::PromotionTransactionConflict {
                detail: "committed activation lost its typed route-promotion record".to_owned(),
            }
            .into());
        };
        let activation = &commit.activation;
        let exact = record.promotion_permit_id() == request.promotion_permit_id
            && record.preflight().feedback_cycle_id() == request.feedback_cycle_id
            && record.preflight().scope().expected_policy_generation()
                == request.expected_policy_generation
            && record.preflight().runtime_control_revision()
                == request.expected_runtime_control_revision
            && record.actor_user_id() == request.actor.user_id
            && record.actor_role() == &request.actor.acting_role
            && record.idempotency_key() == &request.idempotency_key
            && record.reason_code() == request.reason_code
            && record.note() == request.note
            && activation.expected_bundle_generation == request.expected_policy_generation
            && activation.idempotency_key == request.idempotency_key
            && activation.activated_by_user_id == Some(request.actor.user_id)
            && commit.bundle.generation == activation.bundle_generation;
        if !exact {
            return Err(FeedbackError::PromotionTransactionConflict {
                detail:
                    "activation idempotency key was replayed with actor, revision, or intent drift"
                        .to_owned(),
            }
            .into());
        }
        Ok(())
    }

    /// Promote exactly the permit-bound category route, or replay its exact
    /// previously committed graph.
    pub async fn activate(
        &self,
        request: PromoteModelRoute,
    ) -> QuantResult<ModelRoutePromotionCommit> {
        request.validate()?;
        let committed = if let Some(committed) = self
            .deps
            .repository
            .find_committed(&request.promotion_permit_id, &request.feedback_cycle_id)
            .await?
        {
            Self::verify_replay(&request, &committed)?;
            committed
        } else {
            let verified = match self
                .deps
                .preflight
                .verify_permit(&request.promotion_permit_id, request.feedback_cycle_id)
                .await
            {
                Ok(verified) => verified,
                Err(error) => {
                    if let Some(committed) = self
                        .deps
                        .repository
                        .find_committed(&request.promotion_permit_id, &request.feedback_cycle_id)
                        .await?
                    {
                        Self::verify_replay(&request, &committed)?;
                        self.converge(&committed.bundle).await?;
                        return Ok(committed);
                    }
                    return Err(error);
                }
            };
            let command =
                CommitModelRoutePromotion::try_new(request.clone(), verified.preflight().clone())?;
            self.deps.repository.commit(command).await?
        };
        Self::verify_replay(&request, &committed)?;
        self.converge(&committed.bundle).await?;
        Ok(committed)
    }

    /// Reject one exact active shadow slot, then converge runtime to the
    /// champion-only committed route projection.
    pub async fn reject_shadow(
        &self,
        command: RejectShadowBinding,
    ) -> QuantResult<ShadowBindingRejectCommit> {
        command.validate()?;
        let commit = self.deps.shadow_bindings.reject(command).await?;
        self.converge(&commit.bundle).await?;
        Ok(commit)
    }
}
