//! Atomic route-owned shadow-binding repository port.

use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::{
        governance::PolicyActivationInfo,
        ports::{
            CancelShadowBinding, RejectShadowBinding, ShadowBindingCancellationReceipt,
            ShadowBindingJobParams, ShadowBindingLifecycle, ShadowBindingReceipt,
            ShadowBindingRejectionReceipt,
        },
    },
    runtime_config::ActivePolicyBundle,
    types::ShadowBindingArtifactId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowBindingCommitOutcome {
    Committed,
    ExactReplay,
}

/// Exact committed graph returned after transaction commit or replay.
#[derive(Debug, Clone)]
pub struct ShadowBindingCommit {
    pub receipt: ShadowBindingReceipt,
    pub activation: PolicyActivationInfo,
    pub bundle: ActivePolicyBundle,
    pub outcome: ShadowBindingCommitOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowBindingRejectOutcome {
    Rejected,
    ExactReplay,
}

/// Exact committed graph for a governed shadow-slot rejection.
#[derive(Debug, Clone)]
pub struct ShadowBindingRejectCommit {
    pub receipt: ShadowBindingRejectionReceipt,
    pub activation: PolicyActivationInfo,
    pub bundle: ActivePolicyBundle,
    pub outcome: ShadowBindingRejectOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShadowBindingCancelOutcome {
    Cancelled,
    ExactReplay,
}

/// Exact committed graph for a coordinator-owned shadow-slot cancellation.
#[derive(Debug, Clone)]
pub struct ShadowBindingCancelCommit {
    pub receipt: ShadowBindingCancellationReceipt,
    pub activation: PolicyActivationInfo,
    pub bundle: ActivePolicyBundle,
    pub outcome: ShadowBindingCancelOutcome,
}

/// Sole owner of the route slot, policy revision, activation audit/outbox, and
/// durable binding receipt transaction.
#[async_trait::async_trait]
pub trait ModelRouteShadowBindingRepository: Send + Sync {
    async fn find_lifecycle(
        &self,
        binding_id: &ShadowBindingArtifactId,
    ) -> QuantResult<Option<ShadowBindingLifecycle>>;

    async fn find_committed(
        &self,
        binding_id: &ShadowBindingArtifactId,
    ) -> QuantResult<Option<ShadowBindingCommit>>;

    async fn commit(&self, params: ShadowBindingJobParams) -> QuantResult<ShadowBindingCommit>;

    async fn cancel(&self, command: CancelShadowBinding) -> QuantResult<ShadowBindingCancelCommit>;

    async fn reject(&self, command: RejectShadowBinding) -> QuantResult<ShadowBindingRejectCommit>;
}
