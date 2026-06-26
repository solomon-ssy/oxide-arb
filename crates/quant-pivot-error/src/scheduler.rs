//! Report schedule plane errors.

use thiserror::Error;

/// Failures from the report schedule runner and its job factory.
#[derive(Debug, Error)]
pub enum SchedulerError {
    /// Scheduler backend lifecycle or registration failed (add/start/shutdown/remove).
    #[error("report scheduler backend: {detail}")]
    Backend { detail: String },

    /// Cadence is semantically valid but the scheduler rejected the job spec.
    #[error("invalid schedule job spec: {detail}")]
    InvalidJobSpec { detail: String },
}
