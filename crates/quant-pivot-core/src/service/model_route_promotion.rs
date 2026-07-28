//! Governed orchestration for one atomic model-route promotion.

use std::sync::Arc;

use quant_pivot_error::{QuantResult, feedback::FeedbackError};
use quant_pivot_models::{
    domain::{
        ports::CommittedPolicyApplyPort,
        quant::{CommitModelRoutePromotion, PromoteModelRoute},
    },
    runtime_config::PolicyBundleIdentity,
};
use quant_pivot_repository::traits::{
    ModelRoutePromotionCommit, ModelRoutePromotionRepository, PolicyRepository,
};

use super::promotion_preflight::PromotionPreflightService;

/// Dependencies for the server-owned promotion command boundary.
pub struct ModelRoutePromotionServiceDeps {
    pub preflight: Arc<PromotionPreflightService>,
    pub repository: Arc<dyn ModelRoutePromotionRepository>,
    pub policies: Arc<dyn PolicyRepository>,
    pub policy_apply: Arc<dyn CommittedPolicyApplyPort>,
}

/// Resolves current server truth before delegating the only write transaction.
pub struct ModelRoutePromotionService {
    deps: ModelRoutePromotionServiceDeps,
}

impl ModelRoutePromotionService {
    #[must_use]
    pub const fn new(deps: ModelRoutePromotionServiceDeps) -> Self {
        Self { deps }
    }

    async fn converge(&self, commit: &ModelRoutePromotionCommit) -> QuantResult<()> {
        let current = self
            .deps
            .policies
            .load_current_bundle()
            .await?
            .ok_or_else(|| FeedbackError::PromotionTransactionConflict {
                detail: "committed policy bundle disappeared before runtime apply".to_owned(),
            })?;
        if current.generation < commit.bundle.generation {
            return Err(FeedbackError::PromotionTransactionConflict {
                detail: format!(
                    "durable generation {} is older than promotion generation {}",
                    current.generation, commit.bundle.generation
                ),
            }
            .into());
        }
        if current.generation == commit.bundle.generation
            && PolicyBundleIdentity::from(&current) != PolicyBundleIdentity::from(&commit.bundle)
        {
            return Err(FeedbackError::PromotionTransactionConflict {
                detail: "promotion generation resolved to a different durable snapshot identity"
                    .to_owned(),
            }
            .into());
        }
        self.deps.policy_apply.apply_committed(current).await?;
        Ok(())
    }

    /// Promote exactly the permit-bound category route, or replay its exact
    /// previously committed graph.
    pub async fn promote(
        &self,
        request: PromoteModelRoute,
    ) -> QuantResult<ModelRoutePromotionCommit> {
        let committed = if let Some(committed) = self
            .deps
            .repository
            .find_committed(&request.promotion_permit_id, &request.feedback_cycle_id)
            .await?
        {
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
                        self.converge(&committed).await?;
                        return Ok(committed);
                    }
                    return Err(error);
                }
            };
            let command = CommitModelRoutePromotion::try_new(
                request.promotion_permit_id,
                verified.preflight().clone(),
            )?;
            self.deps.repository.commit(command).await?
        };
        self.converge(&committed).await?;
        Ok(committed)
    }
}
