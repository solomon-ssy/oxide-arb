//! [`WeightOverlay`]: a governed, non-persisted factor-weight override for
//! **non-published** candidate / shadow versions (3.7 hot-update closure).
//!
//! An operator may experiment with factor weights on a `Candidate` / `Shadow`
//! version without re-publishing or mutating the frozen artifact bytes: the
//! overlay is parsed from `FactorsConfig.factor_weights` and **replaces** the
//! artifact's weight table at runtime construction. It is fail-closed —
//! non-negative weights summing to 1, and (at apply time) an exact match against
//! the artifact's factor set, so an unknown or missing factor is rejected rather
//! than silently scored. The overlay never participates in `content_hash()`;
//! publishing must bake a winning experiment back into a fresh artifact.

use std::collections::BTreeMap;

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::runtime_config::FactorWeights;
use rust_decimal::Decimal;

use crate::factors::FactorName;

/// Tolerance for the overlay weight-normalization check (`|Σ − 1| ≤ ε`).
fn weight_sum_tolerance() -> Decimal {
    Decimal::new(1, 9)
}

/// A validated, non-persisted factor-weight override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeightOverlay {
    weights: BTreeMap<FactorName, Decimal>,
}

impl WeightOverlay {
    /// Parse and validate an overlay from a runtime-config `factor_weights` map.
    ///
    /// Validates a non-empty set of non-negative weights summing to 1. The
    /// per-factor key match against a specific artifact happens later, at
    /// [`crate::model::WeightedFactorRuntime::new`].
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::InvalidModelArtifact`] on an unparseable,
    /// negative, empty, or unnormalized weight set.
    pub fn from_config(factor_weights: &FactorWeights) -> QuantResult<Self> {
        let mut weights = BTreeMap::new();
        let mut sum = Decimal::ZERO;
        for (name, value) in &factor_weights.weights {
            let weight = value.value;
            if weight < Decimal::ZERO {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!("factor `{name}` overlay weight {weight} is negative"),
                }
                .into());
            }
            sum += weight;
            weights.insert(FactorName::new(name.clone()), weight);
        }
        if weights.is_empty() {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "weight overlay has no factor weights".to_owned(),
            }
            .into());
        }
        if (sum - Decimal::ONE).abs() > weight_sum_tolerance() {
            return Err(ResearchError::InvalidModelArtifact {
                detail: format!("overlay factor weights must sum to 1, got {sum}"),
            }
            .into());
        }
        Ok(Self { weights })
    }

    /// The validated overlay weights.
    #[must_use]
    pub const fn weights(&self) -> &BTreeMap<FactorName, Decimal> {
        &self.weights
    }

    /// Resolve the overlay against an artifact's factor set, returning the
    /// effective weight table (fail-closed: the overlay must cover **exactly**
    /// the artifact's factors).
    ///
    /// # Errors
    ///
    /// Returns [`ResearchError::InvalidModelArtifact`] when the overlay names a
    /// factor absent from the artifact, or omits a factor the artifact weights.
    pub fn resolve_against(
        &self,
        artifact_weights: &BTreeMap<FactorName, Decimal>,
    ) -> QuantResult<BTreeMap<FactorName, Decimal>> {
        for name in self.weights.keys() {
            if !artifact_weights.contains_key(name) {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "weight overlay names unknown factor `{name}` (not in the artifact)"
                    ),
                }
                .into());
            }
        }
        for name in artifact_weights.keys() {
            if !self.weights.contains_key(name) {
                return Err(ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "weight overlay omits artifact factor `{name}` (overlay must be complete)"
                    ),
                }
                .into());
            }
        }
        Ok(self.weights.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::WeightOverlay;
    use quant_pivot_models::runtime_config::{DecimalValue, FactorWeights};
    use rust_decimal_macros::dec;
    use std::collections::BTreeMap;

    use crate::factors::{
        FactorName,
        names::{LIQUIDITY_DEPTH, MOMENTUM_ROC},
    };

    fn weights(pairs: &[(&str, &str)]) -> FactorWeights {
        FactorWeights {
            weights: pairs
                .iter()
                .map(|(name, value)| {
                    (
                        (*name).to_owned(),
                        DecimalValue::new(value.parse().expect("fixture weight must be decimal")),
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn parses_normalized_weights() {
        let overlay = WeightOverlay::from_config(&weights(&[
            (LIQUIDITY_DEPTH.as_str(), "0.5"),
            (MOMENTUM_ROC.as_str(), "0.5"),
        ]))
        .expect("overlay");
        assert_eq!(overlay.weights().len(), 2);
    }

    #[test]
    fn rejects_unnormalized_overlay() {
        let err = WeightOverlay::from_config(&weights(&[
            (LIQUIDITY_DEPTH.as_str(), "0.5"),
            (MOMENTUM_ROC.as_str(), "0.9"),
        ]));
        assert!(err.is_err(), "overlay weights must sum to 1");
    }

    #[test]
    fn resolve_rejects_unknown_and_missing_factors() {
        let overlay = WeightOverlay::from_config(&weights(&[(LIQUIDITY_DEPTH.as_str(), "1.0")]))
            .expect("overlay");

        let mut artifact_weights: BTreeMap<FactorName, _> = BTreeMap::new();
        artifact_weights.insert(MOMENTUM_ROC, dec!(1));
        // Overlay names a factor the artifact does not weight → unknown.
        assert!(overlay.resolve_against(&artifact_weights).is_err());

        let mut matching: BTreeMap<FactorName, _> = BTreeMap::new();
        matching.insert(LIQUIDITY_DEPTH, dec!(1));
        assert!(overlay.resolve_against(&matching).is_ok());
    }
}
