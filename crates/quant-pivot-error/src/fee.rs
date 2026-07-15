//! Fail-closed Polymarket fee quote errors.

use thiserror::Error;

/// Fee schedule resolution failure for Live execution paths.
///
/// Uses plain `String` for `market_id` so this crate stays independent of
/// `quant-pivot-models` typed IDs (models already depends on error).
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum FeeQuoteError {
    #[error("missing fee schedule for market {market_id}: {detail}")]
    MissingSchedule { market_id: String, detail: String },
    #[error("invalid fee calculation input: {detail}")]
    InvalidCalculation { detail: String },
}
