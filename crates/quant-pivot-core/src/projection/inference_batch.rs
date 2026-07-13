//! Shared model-runtime input assembly (Phase 3.6).
//!
//! Both the online [`ModelRunner`](crate::service::model_runner::ModelRunner) and
//! the offline [`BacktestService`](crate::service::backtest::BacktestService) turn
//! a scored cross-section into the runtime's batch input. Centralizing it here
//! guarantees a backtest feeds the model the exact same `FactorInferenceTable`
//! the live plane would, so replayed metrics are money-faithful. The classical
//! `FeatureMatrix` path is added alongside when the classical runtime lands.

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::types::ModelRunId;
use quant_pivot_research::{
    factors::MarketFactorOutcome,
    features::{FeatureCell, FeatureName, FeatureStaleness, FeatureVector, NullReason},
    model::{
        FactorInferenceRow, FactorInferenceTable, InferenceMatrix, InferenceMatrixRow, ModelFamily,
        ModelRuntimeInput, QuantModelRuntime,
    },
    selection::SelectedMarket,
    training::TrainingExample,
};

use crate::projection::inference_context::build_market_inference_context;
use std::collections::HashSet;

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
    decision_at: DateTime<Utc>,
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
        decision_at,
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
    decision_at: DateTime<Utc>,
    markets: &[SelectedMarket],
    vectors: &[FeatureVector],
    outcomes: &[MarketFactorOutcome],
) -> ModelRuntimeInput {
    match model.model_family() {
        ModelFamily::WeightedFactor => ModelRuntimeInput::FactorTable(build_factor_table(
            model_run_id,
            decision_at,
            markets,
            vectors,
            outcomes,
        )),
        family if family.is_classical() => ModelRuntimeInput::FeatureMatrix(build_feature_matrix(
            &model.input_features(),
            model_run_id,
            decision_at,
            markets,
            vectors,
        )),
        _ => unreachable!("exhaustive model family routing"),
    }
}

/// Assemble a runtime batch directly from immutable v2 Parquet rows.
///
/// This is the only training-dataset backtest input path. It consumes the
/// frozen `FeatureCell`/factor bytes and frozen selection context verbatim;
/// rematerialization is reserved for a separate parity verification and can
/// never replace the model input under evaluation.
pub fn build_frozen_runtime_input(
    model: &dyn QuantModelRuntime,
    model_run_id: &ModelRunId,
    examples: &[&TrainingExample],
) -> QuantResult<ModelRuntimeInput> {
    let first = examples
        .first()
        .ok_or_else(|| ResearchError::DatasetBuild {
            detail: "cannot build a model-input batch from an empty frozen cross-section"
                .to_owned(),
        })?;
    let decision_at = first.decision_at();
    let mut market_ids = HashSet::with_capacity(examples.len());
    for example in examples {
        if example.decision_at() != decision_at {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen cross-section mixes decision times {decision_at} and {}",
                    example.decision_at()
                ),
            }
            .into());
        }
        if !market_ids.insert(example.market_id.clone()) {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "frozen cross-section contains duplicate market {} at {decision_at}",
                    example.market_id
                ),
            }
            .into());
        }
    }

    match model.model_family() {
        ModelFamily::WeightedFactor => {
            let mut rows = Vec::with_capacity(examples.len());
            for example in examples {
                // Dataset build records an empty factor vector only when the
                // factor eligibility gate rejected the market.
                if example.factor_values.is_empty() {
                    continue;
                }
                let Some(context) = build_market_inference_context(
                    &example.feature_vector,
                    &example.selected_market,
                ) else {
                    continue;
                };
                rows.push(FactorInferenceRow {
                    market_id: example.market_id.clone(),
                    token_id: example.token_id.clone(),
                    factors: example.factor_values.clone(),
                    context,
                });
            }
            Ok(ModelRuntimeInput::FactorTable(FactorInferenceTable {
                model_run_id: model_run_id.clone(),
                decision_at,
                rows,
            }))
        }
        family if family.is_classical() => {
            let markets = examples
                .iter()
                .map(|example| example.selected_market.clone())
                .collect::<Vec<_>>();
            let vectors = examples
                .iter()
                .map(|example| example.feature_vector.clone())
                .collect::<Vec<_>>();
            Ok(ModelRuntimeInput::FeatureMatrix(build_feature_matrix(
                &model.input_features(),
                model_run_id,
                decision_at,
                &markets,
                &vectors,
            )))
        }
        family => Err(ResearchError::RuntimeUnavailable {
            family: family.to_string(),
            detail: "portfolio backtest has no frozen-input route for this family".to_owned(),
        }
        .into()),
    }
}

/// Resolve one governed raw classical input for `vector`.
///
/// Encoded indicator columns do not participate here. Structural absence is
/// retained explicitly for an absent domain slice; any other missing key is an
/// applicable source/contract gap and is never fabricated as zero.
fn resolve_feature_column(vector: &FeatureVector, name: &FeatureName) -> FeatureCell {
    if let Some(cell) = vector.cell(name) {
        return cell.clone();
    }
    if name.as_str().starts_with("domain.") && vector.domain.is_none() {
        FeatureCell::not_applicable(NullReason::NotApplicable)
    } else {
        FeatureCell::missing(
            NullReason::SourceUnavailable,
            None,
            FeatureStaleness::Unknown,
        )
    }
}

/// Assemble a dense feature matrix over `feature_names` (the classical model's
/// required column order) from index-aligned cross-section slices.
///
/// Every market with a scoreable context is included; a feature absent from a
/// market's vector is left `Missing` and imputed by the runtime's frozen
/// train-partition median. Encoded indicator columns are never looked up as
/// source features; the
/// fitted transform derives them from each raw feature-cell state.
#[must_use]
pub fn build_feature_matrix(
    feature_names: &[FeatureName],
    model_run_id: &ModelRunId,
    decision_at: DateTime<Utc>,
    markets: &[SelectedMarket],
    vectors: &[FeatureVector],
) -> InferenceMatrix {
    let mut rows = Vec::with_capacity(vectors.len());
    for (market, vector) in markets.iter().zip(vectors) {
        let Some(context) = build_market_inference_context(vector, market) else {
            continue;
        };
        let features: Vec<FeatureCell> = feature_names
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
        model_run_id: model_run_id.clone(),
        decision_at,
        feature_names: feature_names.to_vec(),
        rows,
    }
}

#[cfg(test)]
mod tests {
    use super::{build_feature_matrix, build_frozen_runtime_input};
    use chrono::{TimeZone, Utc};
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::{
        domain::DecisionClock,
        enums::{
            common::MarketCategory,
            domain::DomainFamily,
            factor::FactorFamily,
            quant::{DataQualityStatus, FactorDirection},
        },
        hashing::CanonicalDigest,
        types::{
            ContentHash, EventId, FactorDefinitionId, MarketId, ModelRunId, ModelVersionId, Price,
            Probability, SchemaVersion, TokenId, TrainingExampleId, TrainingSampleSource,
        },
    };
    use quant_pivot_research::{
        factors::{FactorExplanation, FactorValue, NormalizedFactor, names::LIQUIDITY_DEPTH},
        features::{
            DomainFeatureSlice, FeatureCell, FeatureCellState, FeatureName, FeatureStaleness,
            FeatureValue, FeatureVector, NullReason, names::book,
        },
        model::{ModelFamily, ModelRuntimeInput, ModelRuntimeOutput, QuantModelRuntime},
        selection::SelectedMarket,
        training::TrainingExample,
    };
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;

    const FEATURE: &str = "domain.crypto.distance_to_strike";

    fn vector(domain_value: Option<FeatureCell>) -> FeatureVector {
        let mut generic = BTreeMap::new();
        generic.insert(
            book::BEST_ASK,
            FeatureCell::observed(
                FeatureValue::Probability(Probability::new(dec!(0.56))),
                None,
                FeatureStaleness::Unknown,
            ),
        );
        generic.insert(
            book::SECONDARY_BEST_ASK,
            FeatureCell::observed(
                FeatureValue::Probability(Probability::new(dec!(0.47))),
                None,
                FeatureStaleness::Unknown,
            ),
        );
        generic.insert(
            book::MID,
            FeatureCell::observed(
                FeatureValue::Probability(Probability::new(dec!(0.5))),
                None,
                FeatureStaleness::Unknown,
            ),
        );
        let domain = domain_value.map(|value| DomainFeatureSlice {
            family: DomainFamily::Crypto,
            schema_version: SchemaVersion::FIRST,
            values: BTreeMap::from([(FeatureName::from_static(FEATURE), value)]),
        });
        FeatureVector {
            market_id: MarketId::new("m1"),
            token_id: Some(TokenId::new("t1")),
            decision_at: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            generic_schema_version: SchemaVersion::FIRST,
            generic,
            domain,
            data_quality: DataQualityStatus::Fresh,
        }
    }

    fn market() -> SelectedMarket {
        SelectedMarket {
            market_id: MarketId::new("m1"),
            event_id: EventId::new("e1"),
            category: MarketCategory::Crypto,
            primary_token_id: TokenId::new("t1"),
            secondary_token_id: Some(TokenId::new("t1-no")),
            liquidity_usd: None,
            volume_24h_usd: None,
            source_refs: Vec::new(),
        }
    }

    #[test]
    fn feature_matrix_preserves_raw_cell_states_and_real_run_id() {
        let present = vector(Some(FeatureCell::observed(
            FeatureValue::Decimal(dec!(0.02)),
            None,
            FeatureStaleness::Unknown,
        )));
        let not_applicable = vector(None);
        let missing_applicable = vector(Some(FeatureCell::missing(
            NullReason::DomainSourceUnavailable,
            None,
            FeatureStaleness::Unknown,
        )));

        let market = market();
        let vectors = [present, not_applicable, missing_applicable];
        let markets = [market.clone(), market.clone(), market];
        let model_run_id = ModelRunId::from_v7();
        let inference = build_feature_matrix(
            &[FeatureName::from_static(FEATURE)],
            &model_run_id,
            Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            &markets,
            &vectors,
        );
        assert_eq!(inference.rows.len(), 3, "every row has a scoreable context");
        assert_eq!(inference.model_run_id, model_run_id);
        assert!(inference.rows.iter().all(|row| {
            row.context.yes_price.inner() == dec!(0.56)
                && row.context.no_price.map(Price::inner) == Some(dec!(0.47))
        }));
        assert_eq!(
            inference.rows[0].features[0],
            FeatureCell::observed(
                FeatureValue::Decimal(dec!(0.02)),
                None,
                FeatureStaleness::Unknown,
            )
        );
        assert_eq!(
            inference.rows[1].features[0],
            FeatureCell::not_applicable(NullReason::NotApplicable)
        );
        assert_eq!(
            inference.rows[2].features[0],
            FeatureCell::missing(
                NullReason::DomainSourceUnavailable,
                None,
                FeatureStaleness::Unknown,
            )
        );
        assert_eq!(
            inference.rows[2].features[0].state,
            FeatureCellState::Missing,
        );
    }

    struct WeightedProbe {
        version_id: ModelVersionId,
        schema_hash: ContentHash,
    }

    #[async_trait::async_trait]
    impl QuantModelRuntime for WeightedProbe {
        fn model_version_id(&self) -> ModelVersionId {
            self.version_id.clone()
        }

        fn model_family(&self) -> ModelFamily {
            ModelFamily::WeightedFactor
        }

        fn feature_schema_hash(&self) -> ContentHash {
            self.schema_hash.clone()
        }

        fn required_features(&self) -> Vec<FeatureName> {
            Vec::new()
        }

        async fn infer_batch(&self, _input: ModelRuntimeInput) -> QuantResult<ModelRuntimeOutput> {
            unreachable!("frozen-input projection test does not invoke inference")
        }
    }

    #[test]
    fn frozen_weighted_input_preserves_factor_bytes_and_real_run_id() {
        let feature_vector = vector(None);
        let selected_market = market();
        let decision_at = feature_vector.decision_at;
        let factor = FactorValue {
            definition_id: FactorDefinitionId::from_v7(),
            name: LIQUIDITY_DEPTH,
            family: FactorFamily::Liquidity,
            raw_value: Some(dec!(123.45)),
            normalization: NormalizedFactor::cross_section(Probability::new(dec!(0.73))),
            direction: FactorDirection::Positive,
            confidence: Probability::new(dec!(0.91)),
            explanation: FactorExplanation {
                headline: "frozen".to_owned(),
                drivers: Vec::new(),
            },
            input_feature_refs: Vec::new(),
        };
        let example = TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: selected_market.market_id.clone(),
            token_id: selected_market.primary_token_id.clone(),
            selected_market,
            decision_boundary: DecisionClock::new(30)
                .boundary(decision_at)
                .expect("boundary"),
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector,
            factor_values: vec![factor.clone()],
            labels: Vec::new(),
            source_refs: Vec::new(),
            decision_capture: None,
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        };
        let run_id = ModelRunId::from_v7();
        let runtime = WeightedProbe {
            version_id: ModelVersionId::from_v7(),
            schema_hash: CanonicalDigest::content_hash_json("schema").expect("hash"),
        };

        let input = build_frozen_runtime_input(&runtime, &run_id, &[&example]).expect("input");
        let ModelRuntimeInput::FactorTable(table) = input else {
            panic!("weighted probe must receive a factor table");
        };
        assert_eq!(table.model_run_id, run_id);
        assert_eq!(table.decision_at, decision_at);
        assert_eq!(table.rows.len(), 1);
        assert_eq!(table.rows[0].factors, vec![factor]);
        assert_eq!(table.rows[0].context.yes_price.inner(), dec!(0.56));
        assert_eq!(
            table.rows[0].context.no_price.map(Price::inner),
            Some(dec!(0.47)),
            "frozen replay must consume the persisted secondary ask verbatim"
        );

        assert!(build_frozen_runtime_input(&runtime, &run_id, &[&example, &example]).is_err());
    }
}
