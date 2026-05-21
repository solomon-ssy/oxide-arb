//! Orderbook snapshot types.
//!
//! [`BookLevel`] is re-exported from `oxide_arb_models::domain::book` — the
//! canonical definition shared across the workspace. [`OrderbookSnapshot`]
//! remains API-specific because it carries wire-format fields (`hash`) that
//! only the CLOB REST layer cares about.

pub use oxide_arb_models::domain::BookLevel;

use oxide_arb_models::types::TokenId;
use serde::{Deserialize, Serialize};

/// A full orderbook snapshot from the REST API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookSnapshot {
    pub token_id: TokenId,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub hash: String,
    pub timestamp_ms: u64,
}
