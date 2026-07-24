//! Canonical [`MarketInferenceContext`] projection.
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

use quant_pivot_models::types::{FeatureValue, Price, Probability, Usd, stable_name::FeatureName};
use quant_pivot_research::{
    features::{
        FeatureVector,
        names::{
            book::{BEST_ASK, SECONDARY_BEST_ASK, VISIBLE_LIQUIDITY_USD},
            market::TIME_TO_RESOLUTION_SECS,
        },
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
    let yes_price = executable_ask(vector, &BEST_ASK)?;
    let no_price = selected
        .secondary_token_id
        .as_ref()
        .and_then(|_| executable_ask(vector, &SECONDARY_BEST_ASK));
    Some(MarketInferenceContext {
        secondary_token_id: selected.secondary_token_id.clone(),
        yes_price,
        no_price,
        liquidity_usd: selected
            .liquidity_usd
            .or_else(|| usd_feature(vector, &VISIBLE_LIQUIDITY_USD)),
        data_quality: vector.data_quality,
        time_to_resolution_secs: count_feature(vector, &TIME_TO_RESOLUTION_SECS),
        substitution_reasons: vector.substitution_reasons(),
    })
}

/// Executable buy reference for one outcome: its actual best ask only.
fn executable_ask(vector: &FeatureVector, name: &FeatureName) -> Option<Price> {
    let value = probability_feature(vector, name)?.inner();
    (value > Decimal::ZERO).then(|| Price::new(value))
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::{common::MarketCategory, quant::DataQualityStatus},
        types::{
            EventId, FeatureCell, FeatureStaleness, FeatureValue, MarketId, Price, Probability,
            SchemaVersion, TokenId,
        },
    };
    use quant_pivot_research::{
        features::{
            FeatureVector,
            names::book::{BEST_ASK, MID, SECONDARY_BEST_ASK},
        },
        selection::SelectedMarket,
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::build_market_inference_context;

    fn price_cell(value: Decimal) -> FeatureCell {
        FeatureCell::observed(
            FeatureValue::Probability(Probability::new(value)),
            None,
            FeatureStaleness::Unknown,
        )
    }

    fn vector(yes_ask: Decimal, no_ask: Option<Decimal>) -> FeatureVector {
        let mut generic = BTreeMap::from([
            (BEST_ASK, price_cell(yes_ask)),
            (MID, price_cell(dec!(0.50))),
        ]);
        if let Some(no_ask) = no_ask {
            generic.insert(SECONDARY_BEST_ASK, price_cell(no_ask));
        }
        FeatureVector {
            market_id: MarketId::new("market"),
            token_id: Some(TokenId::new("yes")),
            decision_at: Utc.with_ymd_and_hms(2026, 7, 12, 0, 0, 0).unwrap(),
            generic_schema_version: SchemaVersion::FIRST,
            generic,
            domain: None,
            data_quality: DataQualityStatus::Fresh,
        }
    }

    fn market(secondary_token_id: Option<TokenId>) -> SelectedMarket {
        SelectedMarket {
            market_id: MarketId::new("market"),
            event_id: EventId::new("event"),
            category: MarketCategory::Sports,
            primary_token_id: TokenId::new("yes"),
            secondary_token_id,
            liquidity_usd: None,
            volume_24h_usd: None,
            source_refs: Vec::new(),
        }
    }

    #[test]
    fn context_uses_exact_asks() {
        let context = build_market_inference_context(
            &vector(dec!(0.61), Some(dec!(0.44))),
            &market(Some(TokenId::new("no"))),
        )
        .expect("primary ask is executable");

        assert_eq!(context.yes_price.inner(), dec!(0.61));
        assert_eq!(context.no_price.map(Price::inner), Some(dec!(0.44)));
        assert_ne!(
            context.yes_price.inner(),
            dec!(0.50),
            "mid is not executable"
        );
        assert_ne!(
            context.no_price.map(Price::inner),
            Some(dec!(0.39)),
            "NO ask is never synthesized as one minus YES ask"
        );
    }

    #[test]
    fn missing_never_no_price() {
        let missing = build_market_inference_context(
            &vector(dec!(0.61), None),
            &market(Some(TokenId::new("no"))),
        )
        .expect("YES remains scoreable");
        assert!(missing.no_price.is_none());

        let unbound =
            build_market_inference_context(&vector(dec!(0.61), Some(dec!(0.44))), &market(None))
                .expect("YES remains scoreable");
        assert!(unbound.no_price.is_none());
    }

    #[test]
    fn absent_zero_rejects_row() {
        assert!(
            build_market_inference_context(&vector(dec!(0), Some(dec!(0.44))), &market(None))
                .is_none()
        );
        let mut missing = vector(dec!(0.61), None);
        missing.generic.remove(&BEST_ASK);
        assert!(build_market_inference_context(&missing, &market(None)).is_none());
    }
}
