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
    features::{
        CellAvailability, FeatureName, FeatureValue, FeatureVector, NullReason, availability_of,
        base_name_if_availability_column,
    },
    model::{
        FactorInferenceRow, FactorInferenceTable, InferenceMatrix, InferenceMatrixRow, ModelFamily,
        ModelRuntimeInput, QuantModelRuntime,
    },
    selection::SelectedMarket,
};
use rust_decimal::Decimal;

use crate::projection::inference_context::build_market_inference_context;

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

/// Resolve one classical-model feature column for `vector`.
///
/// A `{feature}.__available` column (synthesized by
/// `quant_pivot_research::training::matrix` at training time — see
/// [`base_name_if_availability_column`]) never exists as a real key in any
/// `FeatureVector`; it must be **recomputed** here from the base feature's
/// live availability via the identical [`availability_of`] rule the training
/// matrix used, or the classical model's learned availability signal is
/// always fed the same constant at serve time regardless of the market's
/// real state (11.2.2 remediation R2 — the training/serving skew this
/// closes). Every other column is a genuine feature lookup, `Missing` when
/// truly absent (imputed by the runtime's frozen preprocessing).
fn resolve_feature_column(vector: &FeatureVector, name: &FeatureName) -> FeatureValue {
    if let Some(base) = base_name_if_availability_column(name) {
        let decimal = match availability_of(vector, &base) {
            CellAvailability::Present => Decimal::ONE,
            CellAvailability::NotApplicable => Decimal::ZERO,
            CellAvailability::MissingApplicable => -Decimal::ONE,
        };
        return FeatureValue::Decimal(decimal);
    }
    vector
        .value(name)
        .cloned()
        .unwrap_or(FeatureValue::Missing(NullReason::SourceUnavailable))
}

/// Assemble a dense feature matrix over `feature_names` (the classical model's
/// required column order) from index-aligned cross-section slices.
///
/// Every market with a scoreable context is included; a feature absent from a
/// market's vector is left `Missing` and imputed by the runtime's frozen
/// preprocessing (the same mean-fill the model was standardized with). A
/// `.__available` column is never looked up as a literal key — it is
/// recomputed live (see [`resolve_feature_column`]).
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
            .map(|name| resolve_feature_column(vector, name))
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

#[cfg(test)]
mod tests {
    use super::build_feature_matrix;
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        enums::{common::MarketCategory, domain::DomainFamily, quant::DataQualityStatus},
        types::{
            EventId, MarketId, Probability, SchemaVersion, TokenId, TrainingExampleId,
            TrainingSampleSource,
        },
    };
    use quant_pivot_research::{
        features::{FeatureName, FeatureValue, FeatureVector, NullReason, names::book},
        selection::SelectedMarket,
        training::{
            FeatureColumnSpec, FeatureMatrixSpec, LabelName, MatrixScale, TrainingExample,
            TrainingLabel, build_training_matrix,
        },
    };
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;

    const FEATURE: &str = "domain.crypto.distance_to_strike";

    fn vector(domain_value: Option<FeatureValue>) -> FeatureVector {
        let mut generic = BTreeMap::new();
        generic.insert(
            book::MID,
            FeatureValue::Probability(Probability::new(dec!(0.5))),
        );
        let domain = domain_value.map(|value| quant_pivot_research::features::DomainFeatureSlice {
            family: DomainFamily::Crypto,
            schema_version: SchemaVersion::FIRST,
            values: BTreeMap::from([(FeatureName::from_static(FEATURE), value)]),
        });
        FeatureVector {
            market_id: MarketId::new("m1"),
            token_id: Some(TokenId::new("t1")),
            as_of: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            generic_schema_version: SchemaVersion::FIRST,
            generic,
            domain,
            substitutions: Vec::new(),
            data_quality: DataQualityStatus::Fresh,
            staleness_ms: 0,
            source_refs: Vec::new(),
        }
    }

    fn market() -> SelectedMarket {
        SelectedMarket {
            market_id: MarketId::new("m1"),
            event_id: EventId::new("e1"),
            category: MarketCategory::Crypto,
            primary_token_id: TokenId::new("t1"),
            secondary_token_id: None,
            liquidity_usd: None,
            volume_24h_usd: None,
            source_refs: Vec::new(),
        }
    }

    fn training_example(vector: FeatureVector) -> TrainingExample {
        let as_of = vector.as_of;
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: vector.market_id.clone(),
            token_id: vector.token_id.clone().expect("token"),
            as_of,
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: vector,
            factor_values: Vec::new(),
            labels: vec![TrainingLabel {
                label_name: LabelName::from_static("return_to_horizon"),
                horizon_secs: 60,
                value: dec!(1),
                is_resolved: true,
                matured_at: as_of + Duration::seconds(60),
            }],
            source_refs: Vec::new(),
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        }
    }

    /// The regression this closes (11.2.2 remediation R2): the training
    /// matrix's synthesized `.__available` column names, replayed through
    /// `build_feature_matrix` against the *same* `FeatureVector`s, must
    /// reproduce the exact three-state signal the training matrix itself
    /// computed — never a single collapsed constant.
    #[test]
    fn feature_matrix_recomputes_availability_never_a_constant() {
        let present = vector(Some(FeatureValue::Decimal(dec!(0.02))));
        let not_applicable = vector(None);
        let missing_applicable = vector(Some(FeatureValue::Missing(
            NullReason::DomainSourceUnavailable,
        )));

        let spec = FeatureMatrixSpec {
            columns: vec![FeatureColumnSpec {
                feature: FeatureName::from_static(FEATURE),
                scale: MatrixScale::Identity,
                critical: false,
                fill_missing: 0.0,
            }],
            label_name: LabelName::from_static("return_to_horizon"),
            label_horizon_secs: 60,
        };
        let examples = vec![
            training_example(present.clone()),
            training_example(not_applicable.clone()),
            training_example(missing_applicable.clone()),
        ];
        let matrix = build_training_matrix(&examples, &spec).expect("matrix");
        assert_eq!(
            matrix.feature_names,
            vec![
                FeatureName::from_static(FEATURE),
                FeatureName::from_static("domain.crypto.distance_to_strike.__available"),
            ]
        );

        let market = market();
        let vectors = [present, not_applicable, missing_applicable];
        let markets = [market.clone(), market.clone(), market];
        let inference = build_feature_matrix(
            &matrix.feature_names,
            Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            &markets,
            &vectors,
        );
        assert_eq!(inference.rows.len(), 3, "every row has a scoreable context");

        let availability = |row_idx: usize| &inference.rows[row_idx].features[1];
        assert_eq!(
            *availability(0),
            FeatureValue::Decimal(dec!(1)),
            "present row"
        );
        assert_eq!(
            *availability(1),
            FeatureValue::Decimal(dec!(0)),
            "structurally not-applicable row"
        );
        assert_eq!(
            *availability(2),
            FeatureValue::Decimal(dec!(-1)),
            "applicable-but-missing row"
        );
        // The three rows must be pairwise distinct — never a collapsed constant.
        assert_ne!(availability(0), availability(1));
        assert_ne!(availability(1), availability(2));
        assert_ne!(availability(0), availability(2));
    }
}
