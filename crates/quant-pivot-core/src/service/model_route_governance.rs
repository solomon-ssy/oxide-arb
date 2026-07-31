//! Governed orchestration for one atomic model-route promotion.

use std::sync::Arc;

use quant_pivot_error::{QuantResult, feedback::FeedbackError};
use quant_pivot_models::{
    domain::{
        ports::CommittedPolicyApplyPort,
        quant::{
            BootstrapModelRoute, CommitModelRouteBootstrap, CommitModelRoutePromotion,
            ModelGovernanceAuditDetail, PromoteModelRoute,
        },
    },
    runtime_config::{ActivePolicyBundle, PolicyBundleIdentity},
};
use quant_pivot_repository::traits::{
    ModelRouteBootstrapCommit, ModelRouteBootstrapRepository, ModelRoutePromotionCommit,
    ModelRoutePromotionRepository, PolicyRepository,
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
        let exact = record.preflight().manifest().model_version_id() == request.model_version_id
            && record.preflight().expected_policy_generation()
                == request.expected_policy_generation
            && record.preflight().expected_runtime_revision()
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
            let command =
                CommitModelRouteBootstrap::try_new(request.clone(), plan.preflight().clone())?;
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
}
