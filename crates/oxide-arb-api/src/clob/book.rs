//! Orderbook snapshot types.

use oxide_arb_models::types::{Price, Shares, TokenId};
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

/// A single level in the orderbook.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: Price,
    pub size: Shares,
}
