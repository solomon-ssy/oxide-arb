//! Runtime control plane errors (mode switch, config apply, book subscriptions).

use thiserror::Error;

use crate::QuantError;

/// Failures from governed runtime control operations.
#[derive(Debug, Error)]
pub enum ControlError {
    #[error("precondition failed: {0}")]
    Precondition(String),

    #[error("control operation failed: {0}")]
    Engine(String),
}

impl From<QuantError> for ControlError {
    fn from(error: QuantError) -> Self {
        Self::Engine(error.to_string())
    }
}
