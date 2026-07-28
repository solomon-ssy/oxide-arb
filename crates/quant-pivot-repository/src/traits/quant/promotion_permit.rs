//! Governed promotion-permit command port.

use quant_pivot_error::feedback::PromotionPermitCommandError;
use quant_pivot_models::{
    domain::quant::{IssuePromotionPermit, PromotionPermitInfo, RevokePromotionPermit},
    types::PromotionPermitId,
};

/// Outcome of an idempotent permit issuance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromotionPermitIssueOutcome {
    Issued(PromotionPermitInfo),
    ExactReplay(PromotionPermitInfo),
}

/// Outcome of a row-lock/CAS permit revocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PromotionPermitRevokeOutcome {
    Revoked(PromotionPermitInfo),
    ExactReplay(PromotionPermitInfo),
}

/// Atomic persistence and server-side authorization owner for permit commands.
#[async_trait::async_trait]
pub trait PromotionPermitRepository: Send + Sync {
    /// Load and revalidate one exact permit. Absence is a typed failure rather
    /// than an optional authority that a caller could silently ignore.
    async fn load(
        &self,
        permit_id: &PromotionPermitId,
    ) -> Result<PromotionPermitInfo, PromotionPermitCommandError>;

    /// Authorize and persist one immutable permit, accepting only an exact
    /// replay across every immutable field.
    async fn issue(
        &self,
        command: IssuePromotionPermit,
    ) -> Result<PromotionPermitIssueOutcome, PromotionPermitCommandError>;

    /// Authorize and apply the sole revocation transition under row lock and
    /// base-revision CAS.
    async fn revoke(
        &self,
        command: RevokePromotionPermit,
    ) -> Result<PromotionPermitRevokeOutcome, PromotionPermitCommandError>;
}
