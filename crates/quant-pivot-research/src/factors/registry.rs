//! The governed factor registry: `enabled families → (spec, computer)`.
//!
//! [`FactorRegistry::build`] selects the generic factors whose family is enabled
//! by `factors.enabled_factor_families`, resolving each factor's input feature
//! names against the active feature config. [`crate::factors::FactorEngine`]
//! seals these specs with the feature contract into one immutable,
//! content-addressed serving plane.

use std::{collections::HashSet, sync::Arc};

use quant_pivot_models::{
    enums::{common::MarketCategory, factor::FactorFamily},
    runtime_config::{DomainConfig, FactorsConfig, FeaturesConfig},
    types::ResearchFeatureContract,
};

use crate::{
    factors::{
        computer::FactorComputer,
        domain::DomainFactorRegistry,
        generic::{bootstrap_trade_factors, generic_factors},
        structural::structural_factors,
        value::FactorDefinitionDocument,
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
    /// (fail-closed). The table is runtime data and its immutable artifact
    /// commitment is owned by the model-serving contract, not the factor plane.
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
        let mut selected: Vec<_> = generic_factors(features)
            .into_iter()
            .chain(structural_factors(factors, features, bias_table))
            .filter(|(spec, _)| enabled.contains(&spec.family))
            .chain(DomainFactorRegistry::build(domain).all())
            .collect();
        selected.sort_unstable_by(|left, right| left.0.name.cmp(&right.0.name));
        Self { factors: selected }
    }

    /// Build the exact factor registry for one governed model scope.
    ///
    /// Generic and structural factors remain available to every model. Domain
    /// factors are admitted only for the profile's exact category; a pooled or
    /// non-domain profile therefore cannot accidentally commit revisions from
    /// an unrelated vertical.
    #[must_use]
    pub fn for_model_scope(
        factors: &FactorsConfig,
        features: &FeaturesConfig,
        domain: &DomainConfig,
        feature_contract: ResearchFeatureContract,
        category_scope: Option<MarketCategory>,
        bias_table: Option<Arc<FavoriteLongshotBiasTable>>,
    ) -> Self {
        let enabled: HashSet<FactorFamily> =
            factors.enabled_factor_families.iter().copied().collect();
        let mut selected: Vec<_> = match feature_contract {
            ResearchFeatureContract::FullL2
            | ResearchFeatureContract::FullL2Crypto
            | ResearchFeatureContract::FullL2Weather => generic_factors(features)
                .into_iter()
                .chain(structural_factors(factors, features, bias_table))
                .filter(|(spec, _)| enabled.contains(&spec.family))
                .collect(),
            ResearchFeatureContract::TradeBootstrap
            | ResearchFeatureContract::TradeBootstrapCrypto
            | ResearchFeatureContract::TradeBootstrapWeather => {
                bootstrap_trade_factors(features, feature_contract)
                    .into_iter()
                    .filter(|(spec, _)| enabled.contains(&spec.family))
                    .collect()
            }
        };
        if let Some(category) = category_scope {
            selected.extend(
                DomainFactorRegistry::build(domain)
                    .for_category(category)
                    .into_iter()
                    .map(|(_, spec, computer)| (spec.clone(), Arc::clone(computer))),
            );
        }
        selected.sort_unstable_by(|left, right| left.0.name.cmp(&right.0.name));
        Self { factors: selected }
    }

    /// The enabled `(spec, computer)` pairs, in registry order.
    #[must_use]
    pub fn factors(&self) -> &[(FactorDefinitionDocument, Arc<dyn FactorComputer>)] {
        &self.factors
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
    fn generic_factor_specs_families() {
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
