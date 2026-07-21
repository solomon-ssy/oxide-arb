//! Orderbook snapshot types.
//!
//! [`OrderbookSnapshot`] is API-specific because it carries wire-format fields
//! (`hash`) that only the CLOB REST layer cares about. For the canonical
//! [`BookLevel`](quant_pivot_models::domain::market::BookLevel) type, import it
//! directly from `quant_pivot_models::domain::market`.

use quant_pivot_models::{domain::market::BookLevel, types::TokenId};
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
