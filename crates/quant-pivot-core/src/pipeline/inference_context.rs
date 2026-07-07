//! Canonical [`MarketInferenceContext`] projection (Phase 3.6).
//!
//! Both the online [`ModelRunner`](crate::service::model_runner::ModelRunner) and
//! the offline [`BacktestService`](crate::service::backtest::BacktestService) must
//! score a market from the **same** executable price / liquidity / data-quality /
//! horizon / substitution projection — otherwise the backtest scores a different
//! model than production runs and its metrics are money-unsafe. This module is the
//! single source of truth for that projection; neither service re-derives it.
//!
//! The projection is intentionally total over the inputs the weighted and
//! classical runtimes both consume; a market with no executable reference price
//! cannot be scored and yields `None` (it is dropped from the cross-section).

use quant_pivot_models::types::{Price, Probability, Usd};
use quant_pivot_research::{
    features::{
        FeatureName, FeatureValue, FeatureVector,
        names::{book, market},
    },
    model::MarketInferenceContext,
    selection::SelectedMarket,
};
use rust_decimal::Decimal;

/// Project one market's scoring context, or `None` when it cannot be scored.
///
/// Returns `None` when no executable reference price exists (the market is
/// excluded from the cross-section). This is the **only** sanctioned
/// construction of [`MarketInferenceContext`]; the online and offline planes
/// call it identically so a backtest scores the exact same context the live
/// runtime would.
#[must_use]
pub fn build_market_inference_context(
    vector: &FeatureVector,
    selected: &SelectedMarket,
) -> Option<MarketInferenceContext> {
    let yes_price = yes_price(vector)?;
    Some(MarketInferenceContext {
        secondary_token_id: selected.secondary_token_id.clone(),
        yes_price,
        no_price: None,
        liquidity_usd: selected
            .liquidity_usd
            .or_else(|| usd_feature(vector, &book::VISIBLE_LIQUIDITY_USD)),
        data_quality: vector.data_quality,
        time_to_resolution_secs: count_feature(vector, &market::TIME_TO_RESOLUTION_SECS),
        substitutions: vector.substitutions.clone(),
    })
}

/// The YES executable reference price: the mid, else the bid/ask midpoint.
fn yes_price(vector: &FeatureVector) -> Option<Price> {
    if let Some(mid) = probability_feature(vector, &book::MID) {
        return Some(Price::new(mid.inner()));
    }
    let bid = probability_feature(vector, &book::BEST_BID)?;
    let ask = probability_feature(vector, &book::BEST_ASK)?;
    Some(Price::new(
        ((bid.inner() + ask.inner()) / Decimal::from(2)).clamp(Decimal::ZERO, Decimal::ONE),
    ))
}

/// Read a `[0, 1]` probability-valued feature.
fn probability_feature(vector: &FeatureVector, name: &FeatureName) -> Option<Probability> {
    match vector.value(name) {
        Some(FeatureValue::Probability(value)) => Some(*value),
        _ => None,
    }
}

/// Read a USD-valued feature.
fn usd_feature(vector: &FeatureVector, name: &FeatureName) -> Option<Usd> {
    match vector.value(name) {
        Some(FeatureValue::Usd(value)) => Some(*value),
        _ => None,
    }
}

/// Read a count-valued feature.
fn count_feature(vector: &FeatureVector, name: &FeatureName) -> Option<u64> {
    match vector.value(name) {
        Some(FeatureValue::Count(value)) => Some(*value),
        _ => None,
    }
}
