//! Publication pointer and in-memory snapshot types.

use super::value::ControlFactorValue;
use crate::{
    enums::control_factor::{FactorStatus, PublicationMode, PublicationStatus},
    types::{ControlFactorId, FactorPublicationId},
};
use chrono::{DateTime, Utc};
use quant_pivot_error::control::GovernanceError;
use serde::{Deserialize, Serialize};

/// Publication pointer consumed by live refreshers. Live behavior follows the active row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlFactorPublication {
    pub publication_id: FactorPublicationId,
    pub mode: PublicationMode,
    pub factor_ids: Vec<ControlFactorId>,
    pub previous_publication_id: Option<FactorPublicationId>,
    pub status: PublicationStatus,
    pub effective_from: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub approved_by: Option<String>,
    pub approval_reason: String,
    pub publication_hash: String,
}

impl ControlFactorPublication {
    /// Factor status that publication membership drives for this mode.
    #[must_use]
    pub const fn target_factor_status(&self) -> FactorStatus {
        match self.mode {
            PublicationMode::Shadow => FactorStatus::Shadow,
            PublicationMode::Published => FactorStatus::Published,
        }
    }

    /// Factor status required of every member before this publication may activate.
    #[must_use]
    pub const fn required_member_status(&self) -> FactorStatus {
        match self.mode {
            PublicationMode::Shadow => FactorStatus::Candidate,
            PublicationMode::Published => FactorStatus::Shadow,
        }
    }

    /// Validates a publication against its resolved member factors before activation.
    ///
    /// Pure invariant logic shared by the governance service and the repository
    /// (where it runs inside the activation transaction to close the TOCTOU gap).
    /// Publication-hash tamper verification is a separate concern handled by the
    /// hashing layer.
    pub fn validate_for_activation(
        &self,
        factors: &[ControlFactorValue],
    ) -> Result<(), GovernanceError> {
        if self.factor_ids.is_empty() {
            return Err(GovernanceError::EmptyPublication);
        }
        if self.effective_from >= self.expires_at {
            return Err(GovernanceError::InvalidPublicationWindow);
        }
        if self.factor_ids.len() != factors.len() {
            return Err(GovernanceError::FactorSetMismatch);
        }

        let target_status = self.target_factor_status();
        let required_current = self.required_member_status();

        for factor in factors {
            if !self
                .factor_ids
                .iter()
                .any(|factor_id| factor_id == &factor.factor_id)
            {
                return Err(GovernanceError::FactorSetMismatch);
            }
            if factor.status != required_current {
                return Err(GovernanceError::FactorNotReadyForPublication {
                    factor_id: factor.factor_id.to_string(),
                    mode: self.mode.as_str().to_owned(),
                    expected: required_current.to_string(),
                    actual: factor.status.to_string(),
                });
            }
            factor.validate_for_transition(target_status, None)?;
        }
        Ok(())
    }
}
