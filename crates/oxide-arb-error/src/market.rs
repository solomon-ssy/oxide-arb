//! Market catalog and registry invariant errors.

use thiserror::Error;

/// Errors raised when market metadata violates registry invariants.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MarketError {
    /// YES/NO token pair could not be resolved from outcome labels.
    #[error("Market {market_id} is missing distinct YES and NO tokens")]
    InvalidTokenPair { market_id: String },

    /// Gamma startup sync returned no active markets — detection cannot route tokens.
    #[error("Gamma sync returned zero active markets — cannot start without market catalog")]
    EmptyCatalog,
}
