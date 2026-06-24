//! The governed factor registry: `enabled families → (spec, computer)`.
//!
//! [`FactorRegistry::build`] selects the generic factors whose family is enabled
//! by `factors.enabled_factor_families`, resolving each factor's input feature
//! names against the active feature config. Definition ids are deterministic
//! (UUID v5 of the factor name), so a factor's identity is stable across runs and
//! its persisted definition is idempotent.

use std::collections::HashSet;
use std::sync::Arc;

use quant_pivot_models::runtime_config::{FactorsConfig, FeaturesConfig};

use crate::factors::{
    computer::FactorComputer,
    generic::generic_factors,
    value::{FactorDefinitionSpec, FactorSet},
};

/// A frozen registry of enabled factors: their governed specs and the computers
/// that produce them. Built once per round from frozen config.
pub struct FactorRegistry {
    factors: Vec<(FactorDefinitionSpec, Arc<dyn FactorComputer>)>,
}

impl FactorRegistry {
    /// Build the registry, selecting the generic factors whose family is enabled.
    ///
    /// Domain (vertical) factors are routed by category, not by the
    /// `enabled_factor_families` list, so they are never selected here (3.3 ships
    /// only the generic plane; see [`crate::factors::domain`]).
    #[must_use]
    pub fn build(factors: &FactorsConfig, features: &FeaturesConfig) -> Self {
        let enabled: HashSet<&str> = factors
            .enabled_factor_families
            .iter()
            .map(String::as_str)
            .collect();
        let selected = generic_factors(features)
            .into_iter()
            .filter(|(spec, _)| {
                spec.family
                    .generic_wire()
                    .is_some_and(|wire| enabled.contains(wire))
            })
            .collect();
        Self { factors: selected }
    }

    /// The enabled `(spec, computer)` pairs, in registry order.
    #[must_use]
    pub fn factors(&self) -> &[(FactorDefinitionSpec, Arc<dyn FactorComputer>)] {
        &self.factors
    }

    /// The governed factor set (specs only), for `factor_schema_hash` binding.
    #[must_use]
    pub fn factor_set(&self) -> FactorSet {
        FactorSet {
            definitions: self.factors.iter().map(|(spec, _)| spec.clone()).collect(),
        }
    }

    /// The number of enabled factors.
    #[must_use]
    pub fn len(&self) -> usize {
        self.factors.len()
    }

    /// Whether no factor is enabled.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.factors.is_empty()
    }
}

#[cfg(test)]
mod wire_contract {
    use quant_pivot_models::runtime_config::GENERIC_FACTOR_FAMILY_WIRES;

    use crate::factors::value::FactorFamily;

    /// Generic family wire labels must stay aligned with runtime-config validation.
    #[test]
    fn generic_family_wires_match_runtime_config_contract() {
        let mut wires: Vec<&str> = [
            FactorFamily::Liquidity,
            FactorFamily::Microstructure,
            FactorFamily::Momentum,
            FactorFamily::MeanReversion,
            FactorFamily::Volatility,
            FactorFamily::Activity,
            FactorFamily::Resolution,
            FactorFamily::DataQuality,
        ]
        .into_iter()
        .map(|family| family.generic_wire().expect("generic wire"))
        .collect();
        wires.sort_unstable();
        let mut expected = GENERIC_FACTOR_FAMILY_WIRES.to_vec();
        expected.sort_unstable();
        assert_eq!(wires, expected);
    }
}
