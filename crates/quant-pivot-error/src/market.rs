//! Market catalog and registry invariant errors.

use thiserror::Error;

/// Errors raised when market metadata violates registry invariants.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MarketError {
    #[error("market {market_id}: YES and NO token ids must differ")]
    DuplicateTokenPair { market_id: String },

    #[error("market {market_id} has no tokens")]
    EmptyTokenSet { market_id: String },

    #[error("market {market_id} is missing a NO token")]
    MissingNoToken { market_id: String },

    /// YES/NO token pair could not be resolved from outcome labels.
    #[error("Market {market_id} is missing distinct YES and NO tokens: {reason}")]
    InvalidTokenPair { market_id: String, reason: String },

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
