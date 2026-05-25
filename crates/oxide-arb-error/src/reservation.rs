//! Exposure reservation errors.

use thiserror::Error;

/// Error returned when a capital reservation cannot be fulfilled.
#[derive(Debug, Clone, Error)]
pub enum ReservationError {
    #[error(
        "exposure limit exceeded: current={current_cents} requested={requested_cents} max={max_cents}"
    )]
    ExceedsLimit {
        current_cents: u64,
        requested_cents: u64,
        max_cents: u64,
    },

    #[error("reservation not found: {id}")]
    NotFound { id: String },

    #[error("reservation backend error: {0}")]
    Backend(String),
}
