//! Fee estimation trait — injected by the core layer.
//!
//! The algorithm crate calls this during detection; the core layer wraps
//! `oxide_arb_api::fees::FeeCalculator` into a concrete implementation.

use oxide_arb_models::{
    enums::common::MarketCategory,
    types::{Price, Shares, TokenId, Usd},
};
use std::sync::Arc;

/// Fee estimation dependency injected by `oxide-arb-core`.
pub trait FeeEstimator: Send + Sync {
    fn estimate_fee(
        &self,
        shares: Shares,
        price: Price,
        category: MarketCategory,
        token_id: &TokenId,
    ) -> Usd;
}

impl FeeEstimator for Arc<dyn FeeEstimator> {
    fn estimate_fee(
        &self,
        shares: Shares,
        price: Price,
        category: MarketCategory,
        token_id: &TokenId,
    ) -> Usd {
        self.as_ref()
            .estimate_fee(shares, price, category, token_id)
    }
}
