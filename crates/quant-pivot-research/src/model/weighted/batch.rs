//! Batch linear-algebra helpers for [`super::WeightedFactorRuntime`].
//!
//! The cross-sectional `net = Σ weightᵢ · signedᵢ` reduction uses `ndarray` for
//! throughput; results are quantized back to [`Decimal`] at
//! [`RESEARCH_DECIMAL_SCALE`](crate::precision::RESEARCH_DECIMAL_SCALE) before
//! any money-typed field is built (same boundary pattern as `features::stats`).

use std::collections::BTreeMap;

use ndarray::{Array1, Array2};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

use crate::{
    factors::FactorName, model::runtime::FactorInferenceRow, precision::RESEARCH_DECIMAL_SCALE,
};

/// Column layout for batch `net` computation, built once per runtime.
pub struct ScoringBatchLayout {
    /// Non-zero weights aligned with factor column order.
    weights: Array1<f64>,
    /// Factor name → column index.
    column_index: BTreeMap<FactorName, usize>,
}

impl ScoringBatchLayout {
    /// Build the batch layout from the runtime's frozen weight index.
    #[must_use]
    pub fn from_weights(weights: &BTreeMap<FactorName, Decimal>) -> Self {
        let factor_columns: Vec<FactorName> = weights.keys().cloned().collect();
        let weight_values: Vec<f64> = factor_columns
            .iter()
            .map(|name| weights.get(name).and_then(Decimal::to_f64).unwrap_or(0.0))
            .collect();
        let column_index = factor_columns
            .iter()
            .enumerate()
            .map(|(index, name)| (name.clone(), index))
            .collect();
        Self {
            weights: Array1::from_vec(weight_values),
            column_index,
        }
    }

    /// Compute the directional net score for each inference row.
    #[must_use]
    pub fn compute_nets(&self, rows: &[FactorInferenceRow]) -> Vec<Decimal> {
        if rows.is_empty() {
            return Vec::new();
        }
        let factor_count = self.weights.len();
        if factor_count == 0 {
            return vec![Decimal::ZERO; rows.len()];
        }

        let mut signed_matrix = Array2::<f64>::zeros((rows.len(), factor_count));
        for (row_index, row) in rows.iter().enumerate() {
            for factor in &row.factors {
                let Some(column) = self.column_index.get(&factor.name) else {
                    continue;
                };
                if self.weights[*column] == 0.0 {
                    continue;
                }
                let normalized = factor.normalized_score.inner().to_f64().unwrap_or(0.0);
                let confidence = factor.confidence.inner().to_f64().unwrap_or(0.0);
                let direction = f64::from(factor.direction.as_i8());
                signed_matrix[[row_index, *column]] = direction * normalized * confidence;
            }
        }

        signed_matrix
            .dot(&self.weights)
            .iter()
            .map(|value| decimal_from_f64(*value))
            .collect()
    }
}

/// Quantize an `f64` batch result back to the research decimal scale.
fn decimal_from_f64(value: f64) -> Decimal {
    Decimal::from_f64(value).map_or(Decimal::ZERO, |decimal| {
        decimal.round_dp(RESEARCH_DECIMAL_SCALE)
    })
}

#[cfg(test)]
mod tests {
    use super::ScoringBatchLayout;
    use quant_pivot_models::{
        enums::quant::{DataQualityStatus, FactorDirection},
        types::{FactorDefinitionId, MarketId, Price, Probability, TokenId, Usd},
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::{
        factors::{
            FactorExplanation, FactorName, FactorValue,
            names::{LIQUIDITY_DEPTH, MOMENTUM},
        },
        model::runtime::{FactorInferenceRow, MarketInferenceContext},
        precision::RESEARCH_DECIMAL_SCALE,
    };

    use quant_pivot_models::enums::factor::FactorFamily;

    fn factor(
        name: FactorName,
        normalized: Probability,
        direction: FactorDirection,
    ) -> FactorValue {
        FactorValue {
            definition_id: FactorDefinitionId::from_v7(),
            name,
            family: FactorFamily::Liquidity,
            raw_value: Some(dec!(1)),
            normalized_score: normalized,
            direction,
            confidence: Probability::new(dec!(0.9)),
            explanation: FactorExplanation {
                headline: "t".to_owned(),
                drivers: Vec::new(),
                clamp: None,
            },
            input_feature_refs: Vec::new(),
        }
    }

    fn context() -> MarketInferenceContext {
        MarketInferenceContext {
            secondary_token_id: Some(TokenId::new("no")),
            yes_price: Price::new(dec!(0.5)),
            no_price: None,
            liquidity_usd: Some(Usd::new(dec!(60000))),
            data_quality: DataQualityStatus::Fresh,
            time_to_resolution_secs: Some(86_400),
            substitutions: Vec::new(),
        }
    }

    fn scalar_net(
        weights: &std::collections::BTreeMap<FactorName, Decimal>,
        row: &FactorInferenceRow,
    ) -> Decimal {
        let mut net = Decimal::ZERO;
        for factor in &row.factors {
            let Some(weight) = weights.get(&factor.name) else {
                continue;
            };
            if weight.is_zero() {
                continue;
            }
            let signed = Decimal::from(factor.direction.as_i8())
                * factor.normalized_score.inner()
                * factor.confidence.inner();
            net += *weight * signed;
        }
        net.round_dp(RESEARCH_DECIMAL_SCALE)
    }

    #[test]
    fn batch_scoring_matches_scalar_reference() {
        let mut weights = std::collections::BTreeMap::new();
        weights.insert(LIQUIDITY_DEPTH, dec!(0.5));
        weights.insert(MOMENTUM, dec!(0.5));
        let layout = ScoringBatchLayout::from_weights(&weights);

        let bullish = FactorInferenceRow {
            market_id: MarketId::new("0xbull"),
            token_id: TokenId::new("yes"),
            factors: vec![
                factor(
                    LIQUIDITY_DEPTH,
                    Probability::new(dec!(0.8)),
                    FactorDirection::Positive,
                ),
                factor(
                    MOMENTUM,
                    Probability::new(dec!(0.6)),
                    FactorDirection::Positive,
                ),
            ],
            context: context(),
        };
        let bearish = FactorInferenceRow {
            market_id: MarketId::new("0xbear"),
            token_id: TokenId::new("yes"),
            factors: vec![
                factor(
                    LIQUIDITY_DEPTH,
                    Probability::new(dec!(0.8)),
                    FactorDirection::Negative,
                ),
                factor(
                    MOMENTUM,
                    Probability::new(dec!(0.6)),
                    FactorDirection::Negative,
                ),
            ],
            context: context(),
        };
        let rows = vec![bullish, bearish];
        let batch_nets = layout.compute_nets(&rows);
        for (row, batch_net) in rows.iter().zip(batch_nets) {
            assert_eq!(batch_net, scalar_net(&weights, row));
        }
    }
}
