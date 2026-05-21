//! Fee estimation trait — injected by the core layer.
//!
//! The algorithm crate calls this during detection; the core layer wraps
//! `oxide_arb_api::fees::FeeCalculator` into a concrete implementation.

use oxide_arb_models::{
    enums::common::MarketCategory,
    types::{Price, Shares, TokenId, Usd},
};

/// Fee estimation dependency injected by `oxide-arb-core`.
///
/// Must be `O(1)`, synchronous, and lock-free — the detection pipeline
/// invokes this for every candidate market on every scan tick.
pub trait FeeEstimator: Send + Sync {
    /// Estimate the trading fee for a hypothetical order.
    fn estimate_fee(
        &self,
        shares: Shares,
        price: Price,
        category: MarketCategory,
        token_id: &TokenId,
    ) -> Usd;
}
