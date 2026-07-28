//! Governed promotion-permit issue and revoke service.

use std::sync::Arc;

use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::domain::quant::{IssuePromotionPermit, RevokePromotionPermit};
use quant_pivot_repository::traits::{
    PromotionPermitIssueOutcome, PromotionPermitRepository, PromotionPermitRevokeOutcome,
};

/// Public application boundary for permit mutations.
pub struct PromotionPermitService {
    repository: Arc<dyn PromotionPermitRepository>,
}

impl PromotionPermitService {
    #[must_use]
    pub const fn new(repository: Arc<dyn PromotionPermitRepository>) -> Self {
        Self { repository }
    }

    /// Validate and atomically authorize one permit issuance.
    pub async fn issue(
        &self,
        command: IssuePromotionPermit,
    ) -> QuantResult<PromotionPermitIssueOutcome> {
        command.validate()?;
        self.repository
            .issue(command)
            .await
            .map_err(QuantError::from)
    }

    /// Validate and atomically authorize one permit revocation.
    pub async fn revoke(
        &self,
        command: RevokePromotionPermit,
    ) -> QuantResult<PromotionPermitRevokeOutcome> {
        command.validate()?;
        self.repository
            .revoke(command)
            .await
            .map_err(QuantError::from)
    }
}
