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

    /// A market catalog version cannot be committed without the event version
    /// that freezes membership and event-level semantics for the same batch.
    #[error("market {market_id} references event {event_id} without a catalog version")]
    MissingEventVersion { market_id: String, event_id: String },

    #[error("duplicate {entity} `{id}` in one Gamma catalog batch")]
    DuplicateCatalogEntity { entity: &'static str, id: String },

    #[error("failed to serialize {entity} `{id}` for the catalog ledger: {reason}")]
    CatalogSerialization {
        entity: &'static str,
        id: String,
        reason: String,
    },

    #[error("Gamma catalog {entity} count exceeds the supported ledger range")]
    CatalogCountOverflow { entity: &'static str },

    #[error(
        "{entity} `{id}` has source {field} {timestamp} after catalog availability {available_at}"
    )]
    CatalogTimestampInFuture {
        entity: &'static str,
        id: String,
        field: &'static str,
        timestamp: String,
        available_at: String,
    },
}
