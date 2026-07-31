//! Atomic first-champion model-route repository port.

use quant_pivot_error::feedback::RouteBootstrapCommitError;
use quant_pivot_models::{
    domain::{
        governance::PolicyActivationInfo,
        quant::{CommitModelRouteBootstrap, ModelGovernanceAuditInfo},
    },
    runtime_config::ActivePolicyBundle,
    types::{ContentHash, PolicyIdempotencyKey},
};

/// Durable outcome of one bootstrap command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelRouteBootstrapOutcome {
    Committed,
    ExactReplay,
}

/// Exact graph returned after commit or replay.
#[derive(Debug, Clone)]
pub struct ModelRouteBootstrapCommit {
    pub activation: PolicyActivationInfo,
    pub bundle: ActivePolicyBundle,
    pub audit: ModelGovernanceAuditInfo,
    pub transaction_hash: ContentHash,
    pub outcome: ModelRouteBootstrapOutcome,
}

/// Sole transaction boundary for a previously empty category route.
#[async_trait::async_trait]
pub trait ModelRouteBootstrapRepository: Send + Sync {
    async fn find_committed(
        &self,
        idempotency_key: &PolicyIdempotencyKey,
    ) -> Result<Option<ModelRouteBootstrapCommit>, RouteBootstrapCommitError>;

    async fn commit(
        &self,
        command: CommitModelRouteBootstrap,
    ) -> Result<ModelRouteBootstrapCommit, RouteBootstrapCommitError>;
}
