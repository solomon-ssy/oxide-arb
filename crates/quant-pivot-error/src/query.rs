//! Inbound API query validation errors.

use thiserror::Error;

/// Why a time-window or similar query failed to resolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum QueryError {
    #[error("`to` must be >= `from`")]
    Inverted,

    #[error("window too wide (max {max_days} days)")]
    TooWide { max_days: i64 },
}
