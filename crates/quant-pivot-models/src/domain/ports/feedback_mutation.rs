//! Governed feedback-cycle and promotion-permit application boundary.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        api::{
            ActivateModelRouteRequest, BootstrapModelRouteRequest, CancelFeedbackCycleRequest,
            FeedbackCycleMutationView, FeedbackCycleTriggerView, FeedbackSchedulerControlRequest,
            FeedbackSchedulerMutationView, IssuePromotionPermitRequest,
            ModelRouteActivationReceiptView, ModelRouteBootstrapReceiptView,
            PromotionPermitListQuery, PromotionPermitMutationView, PromotionPermitView,
            RevokePromotionPermitRequest, TriggerFeedbackCycleRequest,
        },
        pagination::Paginated,
        quant::FeedbackCycleActor,
    },
    types::{FeedbackCycleId, PromotionPermitId, ResearchProfileId},
};

/// Dependency-inversion boundary for the four governed feedback mutations and
/// the permit catalog they update.
#[async_trait]
pub trait FeedbackMutationPort: Send + Sync {
    /// Freeze and atomically record one manual cycle.
    async fn trigger_cycle(
        &self,
        request: TriggerFeedbackCycleRequest,
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
    ) -> QuantResult<ModelRouteActivationReceiptView>;

    /// Establish the first champion for one previously empty vertical route.
    async fn bootstrap_route(
        &self,
        request: BootstrapModelRouteRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<ModelRouteBootstrapReceiptView>;
}
