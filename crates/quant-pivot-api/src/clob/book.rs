//! Orderbook snapshot types.
//!
//! [`OrderbookSnapshot`] is API-specific because it carries wire-format fields
//! (`hash`) that only the CLOB REST layer cares about. For the canonical
//! [`BookLevel`](quant_pivot_models::domain::market::BookLevel) type, import it
//! directly from `quant_pivot_models::domain::market`.

use quant_pivot_models::{
    domain::market::BookLevel,
    enums::common::TickSize,
    types::{MarketId, Shares, TokenId},
};
use serde::{Deserialize, Serialize};

/// Venue-owned order metadata captured atomically with a live `/book` read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VenueOrderMetadata {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub tick_size: TickSize,
    pub minimum_order_size: Shares,
    pub neg_risk: bool,
}

/// A full orderbook snapshot from the REST API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookSnapshot {
    pub metadata: VenueOrderMetadata,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
    pub hash: String,
    pub timestamp_ms: u64,
}
