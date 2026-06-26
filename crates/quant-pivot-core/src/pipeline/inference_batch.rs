//! Shared model-runtime input assembly (Phase 3.6).
//!
//! Both the online [`ModelRunner`](crate::service::model_runner::ModelRunner) and
//! the offline [`BacktestService`](crate::service::backtest::BacktestService) turn
//! a scored cross-section into the runtime's batch input. Centralizing it here
//! guarantees a backtest feeds the model the exact same `FactorInferenceTable`
//! the live plane would, so replayed metrics are money-faithful. The classical
//! `FeatureMatrix` path is added alongside when the classical runtime lands.

use chrono::{DateTime, Utc};
use quant_pivot_models::types::ModelRunId;
use quant_pivot_research::{
    factors::MarketFactorOutcome,
    features::{FeatureName, FeatureValue, FeatureVector, NullReason},
    model::{
        FactorInferenceRow, FactorInferenceTable, InferenceMatrix, InferenceMatrixRow, ModelFamily,
        ModelRuntimeInput, QuantModelRuntime,
    },
    selection::SelectedMarket,
};

use crate::pipeline::inference_context::build_market_inference_context;

/// Build one factor-inference row from an aligned `(market, vector, outcome)`.
///
/// Returns `None` when the market is factor-ineligible or has no executable
/// reference price (it is excluded from the scored batch).
#[must_use]
pub fn build_factor_row(
    market: &SelectedMarket,
    vector: &FeatureVector,
    outcome: &MarketFactorOutcome,
) -> Option<FactorInferenceRow> {
    if !outcome.eligibility.is_eligible() {
        return None;
    }
    let context = build_market_inference_context(vector, market)?;
    let factors = outcome
        .factors
        .iter()
        .map(|scored| scored.value.clone())
        .collect();
    Some(FactorInferenceRow {
        market_id: outcome.market_id.clone(),
        token_id: market.primary_token_id.clone(),
        factors,
        context,
    })
}

/// Assemble a factor-inference table from index-aligned cross-section slices.
///
/// `markets`, `vectors`, and `outcomes` must be aligned (the `i`-th entry of
/// each describes the same market); ineligible/unscoreable markets are skipped.
#[must_use]
pub fn build_factor_table(
    model_run_id: &ModelRunId,
    as_of: DateTime<Utc>,
    markets: &[SelectedMarket],
    vectors: &[FeatureVector],
    outcomes: &[MarketFactorOutcome],
) -> FactorInferenceTable {
    let mut rows = Vec::with_capacity(outcomes.len());
    for ((market, vector), outcome) in markets.iter().zip(vectors).zip(outcomes) {
        if let Some(row) = build_factor_row(market, vector, outcome) {
            rows.push(row);
        }
    }
    FactorInferenceTable {
        model_run_id: model_run_id.clone(),
        as_of,
        rows,
    }
}

/// Assemble the batch input the model under test consumes from one PIT-resolved
/// cross-section, dispatching on the loaded runtime's family.
///
/// Weighted models receive a [`FactorInferenceTable`] (eligible factor rows);
/// classical models receive an [`InferenceMatrix`] over the model's required
/// feature columns. Both are built from the **same** cross-section, so a model is
/// backtested through the identical computation graph the live plane would run.
#[must_use]
pub fn build_runtime_input(
    model: &dyn QuantModelRuntime,
    model_run_id: &ModelRunId,
    as_of: DateTime<Utc>,
    markets: &[SelectedMarket],
    vectors: &[FeatureVector],
    outcomes: &[MarketFactorOutcome],
) -> ModelRuntimeInput {
    match model.model_family() {
        ModelFamily::WeightedFactor => ModelRuntimeInput::FactorTable(build_factor_table(
            model_run_id,
            as_of,
            markets,
            vectors,
            outcomes,
        )),
        family if family.is_classical() => ModelRuntimeInput::FeatureMatrix(build_feature_matrix(
            &model.required_features(),
            as_of,
            markets,
            vectors,
        )),
        _ => unreachable!("exhaustive model family routing"),
    }
}

/// Assemble a dense feature matrix over `feature_names` (the classical model's
/// required column order) from index-aligned cross-section slices.
///
/// Every market with a scoreable context is included; a feature absent from a
/// market's vector is left `Missing` and imputed by the runtime's frozen
/// preprocessing (the same mean-fill the model was standardized with).
#[must_use]
pub fn build_feature_matrix(
    feature_names: &[FeatureName],
    as_of: DateTime<Utc>,
    markets: &[SelectedMarket],
    vectors: &[FeatureVector],
) -> InferenceMatrix {
    let mut rows = Vec::with_capacity(vectors.len());
    for (market, vector) in markets.iter().zip(vectors) {
        let Some(context) = build_market_inference_context(vector, market) else {
            continue;
        };
        let features: Vec<FeatureValue> = feature_names
            .iter()
            .map(|name| {
                vector
                    .values
                    .get(name)
                    .cloned()
                    .unwrap_or(FeatureValue::Missing(NullReason::SourceUnavailable))
            })
            .collect();
        rows.push(InferenceMatrixRow {
            market_id: vector.market_id.clone(),
            token_id: market.primary_token_id.clone(),
            features,
            context,
        });
    }
    InferenceMatrix {
        as_of,
        feature_names: feature_names.to_vec(),
        rows,
    }
}
