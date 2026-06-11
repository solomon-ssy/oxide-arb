//! Market catalog and registry invariant errors.

use thiserror::Error;

/// Errors raised when market metadata violates registry invariants.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MarketError {
    /// YES/NO token pair could not be resolved from outcome labels.
    #[error("Market {market_id} is missing distinct YES and NO tokens")]
    InvalidTokenPair { market_id: String },

    /// A market arrived with a token count other than the binary CLOB invariant of two.
    #[error("Market {market_id} has {token_count} tokens — Polymarket CLOB markets must be binary")]
    NotBinaryMarket {
        market_id: String,
        token_count: usize,
    },

    /// Gamma startup sync returned no active markets — detection cannot route tokens.
    #[error("Gamma sync returned zero active markets — cannot start without market catalog")]
    EmptyCatalog,
}
