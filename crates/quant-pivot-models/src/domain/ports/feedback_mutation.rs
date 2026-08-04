//! Governed feedback-cycle and promotion-permit application boundary.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        api::{
            ActivateModelRouteRequest, BootstrapModelRouteRequest, CancelFeedbackCycleRequest,
            FeedbackCycleMutationView, FeedbackCycleTriggerRequest, FeedbackCycleTriggerView,
            FeedbackSchedulerControlRequest, FeedbackSchedulerMutationView,
            IssuePromotionPermitRequest, ModelRouteActivationMutationView,
            ModelRouteActivationReceiptView, ModelRouteBootstrapReceiptView,
            PromotionPermitListQuery, PromotionPermitMutationView, PromotionPermitView,
            RejectShadowBindingRequest, RemediateResolutionProjectionRequest,
            ResolutionProjectionRemediationView, RevokePromotionPermitRequest,
            ShadowBindingRejectionReceiptView,
        },
        pagination::Paginated,
        quant::FeedbackCycleActor,
    },
    types::{
        FeedbackCycleId, PolicyActivationId, PromotionPermitId, ResearchProfileId,
        ResolutionObservationId, ShadowBindingArtifactId,
    },
};

/// Read-only boundary for immutable model-route activation receipts.
#[async_trait]
pub trait FeedbackActivationReadPort: Send + Sync {
    /// Read one immutable model-route activation and its sanitized rollback
    /// target by canonical activation identity.
    async fn get_activation(
        &self,
        policy_activation_id: PolicyActivationId,
    ) -> QuantResult<Option<ModelRouteActivationReceiptView>>;

    /// Resolve the immutable model-route activation committed for one exact
    /// feedback cycle through its permit-bound promotion graph.
    async fn get_cycle_activation(
        &self,
        feedback_cycle_id: FeedbackCycleId,
    ) -> QuantResult<Option<ModelRouteActivationReceiptView>>;
}

/// Dependency-inversion boundary for governed feedback mutations and the
/// permit catalog they update.
#[async_trait]
pub trait FeedbackMutationPort: FeedbackActivationReadPort {
    /// Freeze and atomically record one manual cycle.
    async fn trigger_cycle(
        &self,
        request: FeedbackCycleTriggerRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<FeedbackCycleTriggerView>;

    /// Append one governed cancellation request under current timeline CAS.
    async fn cancel_cycle(
        &self,
        cycle_id: FeedbackCycleId,
        request: CancelFeedbackCycleRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<FeedbackCycleMutationView>;

    /// Pause/resume automatic scheduling under pause-revision CAS.
    async fn control_scheduler(
        &self,
        profile_id: ResearchProfileId,
        pause: bool,
        request: FeedbackSchedulerControlRequest,
    ) -> QuantResult<FeedbackSchedulerMutationView>;

    /// Page permits with status derived from the authoritative database clock.
    async fn list_permits(
        &self,
        query: PromotionPermitListQuery,
    ) -> QuantResult<Paginated<PromotionPermitView>>;

    /// Derive preflight from server truth and issue one governed permit.
    async fn issue_permit(
        &self,
        request: IssuePromotionPermitRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<PromotionPermitMutationView>;

    /// Apply the sole permit lifecycle transition under base-revision CAS.
    async fn revoke_permit(
        &self,
        permit_id: PromotionPermitId,
        request: RevokePromotionPermitRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<PromotionPermitMutationView>;

    /// Consume one exact active permit and atomically activate its server-
    /// derived route projection.
    async fn activate_route(
        &self,
        request: ActivateModelRouteRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<ModelRouteActivationMutationView>;

    /// Reject one exact `CandidateReady` shadow binding and converge the route
    /// to its champion-only policy projection.
    async fn reject_shadow(
        &self,
        binding_id: ShadowBindingArtifactId,
        request: RejectShadowBindingRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<ShadowBindingRejectionReceiptView>;

    /// Apply one governed Requeue/Exclude transition to a blocked resolution
    /// projection using observation-revision CAS.
    async fn remediate_resolution(
        &self,
        observation_id: ResolutionObservationId,
        request: RemediateResolutionProjectionRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<ResolutionProjectionRemediationView>;

    /// Establish the first champion for one previously empty vertical route.
    async fn bootstrap_route(
        &self,
        request: BootstrapModelRouteRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<ModelRouteBootstrapReceiptView>;
}
