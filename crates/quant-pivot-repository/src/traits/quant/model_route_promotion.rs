//! Atomic model-route promotion repository port.

use quant_pivot_error::feedback::PromotionCommitError;
use quant_pivot_models::{
    domain::{
        governance::PolicyActivationInfo,
        quant::{CommitModelRoutePromotion, ModelGovernanceAuditInfo},
    },
    runtime_config::ActivePolicyBundle,
    types::{ContentHash, FeedbackCycleId, PromotionPermitId},
};

/// Durable outcome of one promotion command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelRoutePromotionOutcome {
    Committed,
    ExactReplay,
}

/// Exact committed graph returned after transaction commit or replay.
#[derive(Debug, Clone)]
pub struct ModelRoutePromotionCommit {
    pub activation: PolicyActivationInfo,
    pub bundle: ActivePolicyBundle,
    pub audit: ModelGovernanceAuditInfo,
    pub transaction_hash: ContentHash,
    pub outcome: ModelRoutePromotionOutcome,
}

/// Sole owner of the model, route, audit, outbox, and generation transaction.
#[async_trait::async_trait]
pub trait ModelRoutePromotionRepository: Send + Sync {
    /// Resolve a historical exact commit before a fresh preflight. A permit
    /// that was revoked or expired after commit remains replayable.
    async fn find_committed(
        &self,
        promotion_permit_id: &PromotionPermitId,
        feedback_cycle_id: &FeedbackCycleId,
    ) -> Result<Option<ModelRoutePromotionCommit>, PromotionCommitError>;

    /// Revalidate every database preimage under the canonical lock order and
    /// atomically commit the single-category model-route promotion.
    async fn commit(
        &self,
        command: CommitModelRoutePromotion,
    ) -> Result<ModelRoutePromotionCommit, PromotionCommitError>;
}
