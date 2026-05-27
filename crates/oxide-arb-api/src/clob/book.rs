//! Orderbook snapshot types.
//!
//! [`OrderbookSnapshot`] is API-specific because it carries wire-format fields
//! (`hash`) that only the CLOB REST layer cares about. For the canonical
//! [`BookLevel`](oxide_arb_models::domain::BookLevel) type, import directly
//! from `oxide_arb_models::domain`.

use oxide_arb_models::{domain::BookLevel, types::TokenId};
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
