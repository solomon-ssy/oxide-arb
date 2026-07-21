//! The governed factor registry: `enabled families → (spec, computer)`.
//!
//! [`FactorRegistry::build`] selects the generic factors whose family is enabled
//! by `factors.enabled_factor_families`, resolving each factor's input feature
//! names against the active feature config. [`crate::factors::FactorEngine`]
//! binds these specs to the feature-contract hash and derives an immutable,
//! content-addressed revision identity.

use std::{collections::HashSet, sync::Arc};

use quant_pivot_models::{
    enums::factor::FactorFamily,
    runtime_config::{DomainConfig, FactorsConfig, FeaturesConfig},
};

use crate::{
    factors::{
        computer::FactorComputer,
        domain::DomainFactorRegistry,
        generic::generic_factors,
        structural::structural_factors,
        value::{FactorDefinitionDocument, FactorSet},
    },
    model::FavoriteLongshotBiasTable,
};

/// A frozen registry of enabled factors: their governed specs and the computers
/// that produce them. Built once per round from frozen config.
pub struct FactorRegistry {
    factors: Vec<(FactorDefinitionDocument, Arc<dyn FactorComputer>)>,
}

impl FactorRegistry {
    /// Build the registry: the generic + structural factors whose family is
    /// enabled by `enabled_factor_families`, plus the category-routed domain
    /// factors of every vertical enabled in `domain.enabled_by_family`.
    ///
    /// `bias_table` binds the favorite-longshot factor; `None` keeps it inert
    /// (fail-closed). The table is runtime data — it does not affect
    /// `factor_schema_hash`.
    ///
    /// Domain factors are **never** selected through `enabled_factor_families`
    /// (config validation rejects them there): they join the batch column set
    /// here and self-route per market by category, so cross-sectional
    /// normalization sees an aligned column whose non-crypto cells are
    /// structurally `NotApplicable`.
    #[must_use]
    pub fn build(
        factors: &FactorsConfig,
        features: &FeaturesConfig,
        domain: &DomainConfig,
        bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
    ) -> Self {
        let enabled: HashSet<FactorFamily> =
            factors.enabled_factor_families.iter().copied().collect();
        let selected = generic_factors(features)
            .into_iter()
            .chain(structural_factors(factors, features, bias_table))
            .filter(|(spec, _)| enabled.contains(&spec.family))
            .chain(DomainFactorRegistry::build(domain).all())
            .collect();
        Self { factors: selected }
    }

    /// The enabled `(spec, computer)` pairs, in registry order.
    #[must_use]
    pub fn factors(&self) -> &[(FactorDefinitionDocument, Arc<dyn FactorComputer>)] {
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
mod tests {
    use quant_pivot_models::{enums::factor::FactorFamily, runtime_config::FeaturesConfig};

    use crate::factors::generic::generic_factors;

    /// Every generic factor spec must declare a generic-plane family.
    #[test]
    fn generic_factor_specs_use_generic_families() {
        let features = FeaturesConfig::default();
        for (spec, _) in generic_factors(&features) {
            assert!(
                spec.family.is_generic(),
                "generic factor `{}` must not use domain family `{spec:?}`",
                spec.name.as_str(),
            );
            assert!(
                FactorFamily::ALL_GENERIC.contains(&spec.family),
                "generic factor `{}` family `{spec:?}` is not in ALL_GENERIC",
                spec.name.as_str(),
            );
        }
    }
}
