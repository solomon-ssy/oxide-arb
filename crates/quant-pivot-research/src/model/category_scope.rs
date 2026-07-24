//! Category-scoped training artifacts.
//!
//! [`infer_training_category_scope`] derives `WeightedFactorModelArtifact.category_scope`
//! from the governed model-spec contract, the frozen selection policy, and the
//! materialized example plane. [`validate_category_scope_weights`] is the
//! publish-time invariant: a Crypto-scoped artifact must carry non-zero weight on
//! at least one domain-crypto factor column.

use std::collections::BTreeSet;

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::enums::common::MarketCategory;
use rust_decimal::Decimal;

use super::FactorWeight;
use crate::{
    factors::names::{DOMAIN_CRYPTO_BETA_REGIME, DOMAIN_CRYPTO_STRIKE_PRESSURE},
    features::{FeatureValue, names::market::CATEGORY},
    selection::ModelFeatureRequirements,
    training::TrainingExample,
};

/// Infer the category scope to freeze into a trained weighted-factor artifact.
///
/// Precedence (first match wins):
/// 1. Exactly one key in `requirements.by_category`.
/// 2. Exactly one entry in `selection_enabled_categories`.
/// 3. Every materialized example carries the same `market.category` feature.
#[must_use]
pub fn infer_training_category_scope(
    examples: &[TrainingExample],
    requirements: &ModelFeatureRequirements,
    selection_enabled_categories: &[MarketCategory],
) -> Option<MarketCategory> {
    if requirements.by_category.len() == 1 {
        return requirements.by_category.keys().next().copied();
    }
    if selection_enabled_categories.len() == 1 {
        return Some(selection_enabled_categories[0]);
    }
    categories_from_examples(examples)
}

/// Categories observed on the materialized example plane (deduplicated, sorted).
fn categories_from_examples(examples: &[TrainingExample]) -> Option<MarketCategory> {
    let categories: BTreeSet<MarketCategory> = examples
        .iter()
        .filter_map(|example| {
            example
                .feature_vector
                .value(&CATEGORY)
                .and_then(|value| match value {
                    FeatureValue::Category(category) => Some(*category),
                    _ => None,
                })
        })
        .collect();
    if categories.len() == 1 {
        categories.into_iter().next()
    } else {
        None
    }
}

/// A category-scoped artifact must declare non-zero weight on at least one
/// domain-crypto factor when `category_scope = Some(Crypto)`.
///
/// # Errors
///
/// Returns [`ResearchError::InvalidModelArtifact`] when the invariant is violated.
pub fn validate_category_scope_weights(
    category_scope: Option<MarketCategory>,
    weights: &[FactorWeight],
) -> QuantResult<()> {
    if category_scope != Some(MarketCategory::Crypto) {
        return Ok(());
    }
    let has_domain_weight = weights.iter().any(|weight| {
        weight.weight > Decimal::ZERO
            && (weight.factor == DOMAIN_CRYPTO_STRIKE_PRESSURE
                || weight.factor == DOMAIN_CRYPTO_BETA_REGIME)
    });
    if has_domain_weight {
        Ok(())
    } else {
        Err(ResearchError::InvalidModelArtifact {
            detail: "category_scope=Crypto requires at least one non-zero \
                      domain_crypto factor weight (strike_pressure or beta_regime)"
                .to_owned(),
        }
        .into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::Utc;
    use quant_pivot_models::{
        domain::data_plane::DecisionClock,
        enums::{common::MarketCategory, quant::DataQualityStatus},
        types::{
            MarketId, SchemaVersion, TokenId, TrainingExampleId, training::TrainingSampleSource,
        },
    };
    use rust_decimal_macros::dec;

    use super::{
        super::FactorWeight, infer_training_category_scope, validate_category_scope_weights,
    };
    use crate::{
        factors::names::{DOMAIN_CRYPTO_STRIKE_PRESSURE, LIQUIDITY_DEPTH},
        features::{
            FeatureCell, FeatureName, FeatureStaleness, FeatureValue, FeatureVector,
            names::market::CATEGORY,
        },
        selection::ModelFeatureRequirements,
        training::{TrainingExample, fixtures},
    };

    fn example(category: MarketCategory) -> TrainingExample {
        let decision_at = Utc::now();
        let mut generic = BTreeMap::new();
        generic.insert(
            CATEGORY,
            FeatureCell::observed(
                FeatureValue::Category(category),
                None,
                FeatureStaleness::Unknown,
            ),
        );
        TrainingExample {
            example_id: TrainingExampleId::from_v7(),
            market_id: MarketId::new("m"),
            token_id: TokenId::new("t"),
            selected_market: fixtures::selected_market(
                &MarketId::new("m"),
                &TokenId::new("t"),
                category,
            ),
            decision_boundary: DecisionClock::new(0)
                .boundary(decision_at)
                .expect("boundary"),
            sample_source: TrainingSampleSource::HistoricalPit,
            feature_vector: FeatureVector {
                market_id: MarketId::new("m"),
                token_id: Some(TokenId::new("t")),
                decision_at,
                generic_schema_version: SchemaVersion::new(5),
                generic,
                domain: None,
                data_quality: DataQualityStatus::Fresh,
            },
            factor_values: Vec::new(),
            labels: Vec::new(),
            source_refs: Vec::new(),
            decision_capture: None,
            lot_context: None,
            position_state: None,
            book_fidelity: None,
        }
    }

    #[test]
    fn infers_single_category_requirement() {
        let mut requirements = ModelFeatureRequirements::default();
        requirements.by_category.insert(
            MarketCategory::Crypto,
            vec![FeatureName::from_static("domain.crypto.distance_to_strike")],
        );
        assert_eq!(
            infer_training_category_scope(&[], &requirements, &[]),
            Some(MarketCategory::Crypto)
        );
    }

    #[test]
    fn infers_unanimous_example_categories() {
        let requirements = ModelFeatureRequirements::default();
        let examples = vec![
            example(MarketCategory::Crypto),
            example(MarketCategory::Crypto),
        ];
        assert_eq!(
            infer_training_category_scope(
                &examples,
                &requirements,
                &[MarketCategory::Crypto, MarketCategory::Sports]
            ),
            Some(MarketCategory::Crypto)
        );
    }

    #[test]
    fn crypto_scope_requires_weight() {
        validate_category_scope_weights(
            Some(MarketCategory::Crypto),
            &[FactorWeight {
                factor: DOMAIN_CRYPTO_STRIKE_PRESSURE,
                weight: dec!(0.4),
            }],
        )
        .expect("domain weight ok");
        assert!(
            validate_category_scope_weights(
                Some(MarketCategory::Crypto),
                &[FactorWeight {
                    factor: LIQUIDITY_DEPTH,
                    weight: dec!(1),
                }]
            )
            .is_err()
        );
        validate_category_scope_weights(None, &[]).expect("generic scope skips domain check");
    }
}
