//! Governed feedback-cycle and promotion-permit application boundary.

use async_trait::async_trait;
use quant_pivot_error::QuantResult;

use crate::{
    domain::{
        api::{
            CancelFeedbackCycleRequest, FeedbackCycleMutationView, IssuePromotionPermitRequest,
            PromotionPermitListQuery, PromotionPermitMutationView, PromotionPermitView,
            RevokePromotionPermitRequest, TriggerFeedbackCycleRequest,
        },
        pagination::Paginated,
        quant::FeedbackCycleActor,
    },
    types::{FeedbackCycleId, PromotionPermitId},
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
    ) -> QuantResult<FeedbackCycleMutationView>;

    /// Append one governed cancellation request under current timeline CAS.
    async fn cancel_cycle(
        &self,
        cycle_id: FeedbackCycleId,
        request: CancelFeedbackCycleRequest,
        actor: FeedbackCycleActor,
    ) -> QuantResult<FeedbackCycleMutationView>;

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
}
