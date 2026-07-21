//! Category-aware model routing.
//!
//! Runtime config may pin a category-specific published model via
//! `model.category_model_pointers`; markets whose category has no pointer use
//! the generic `active_model_version_id`. A configured pointer is an explicit
//! operator decision: malformed or unavailable targets fail the round rather
//! than silently changing which model scores the market.

use quant_pivot_models::{
    enums::common::MarketCategory, runtime_config::ModelConfig, types::ModelVersionId,
};
/// Which weighted scorer path a market follows at inference time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ModelRouting {
    /// Generic + structural weighted scorer — all categories without a pointer.
    GenericWeighted,
    /// Category-specific artifact (may consume that vertical's domain slice).
    CategorySpecific {
        /// The market category this route serves.
        category: MarketCategory,
        /// Published model version to load.
        artifact: ModelVersionId,
    },
}

/// Resolve the routing decision for one market category from frozen config.
///
/// A configured pointer wins; identifiers are already validated by the typed
/// configuration boundary.
pub fn resolve_model_route(category: MarketCategory, model: &ModelConfig) -> ModelRouting {
    if let Some(reference) = model.category_model_pointers.get(&category) {
        return ModelRouting::CategorySpecific {
            category,
            artifact: reference.id.clone(),
        };
    }
    ModelRouting::GenericWeighted
}

/// The generic active model version, when configured.
///
pub fn generic_model_version_id(model: &ModelConfig) -> Option<ModelVersionId> {
    model
        .active_model_version_id
        .as_ref()
        .map(|reference| reference.id.clone())
}

/// Resolve the model version id that should score a market in this category.
///
/// Category pointers override the generic active model; only absent pointers
/// use the generic model.
///
pub fn version_id_for_category(
    category: MarketCategory,
    model: &ModelConfig,
) -> Option<ModelVersionId> {
    match resolve_model_route(category, model) {
        ModelRouting::GenericWeighted => generic_model_version_id(model),
        ModelRouting::CategorySpecific { artifact, .. } => Some(artifact),
    }
}

#[cfg(test)]
mod tests {
    use std::iter;

    use quant_pivot_models::{
        enums::common::MarketCategory,
        runtime_config::{ModelConfig, ModelVersionRef},
        types::ModelVersionId,
    };

    use super::{
        ModelRouting, generic_model_version_id, resolve_model_route, version_id_for_category,
    };

    fn version_ref(id: ModelVersionId) -> ModelVersionRef {
        ModelVersionRef::new(id)
    }

    #[test]
    fn category_pointer_selects_category_specific_route() {
        let version = ModelVersionId::from_v7();
        let mut model = ModelConfig::default();
        model
            .category_model_pointers
            .insert(MarketCategory::Crypto, version_ref(version.clone()));

        assert_eq!(
            resolve_model_route(MarketCategory::Crypto, &model),
            ModelRouting::CategorySpecific {
                category: MarketCategory::Crypto,
                artifact: version.clone(),
            }
        );
        assert_eq!(
            resolve_model_route(MarketCategory::Sports, &model),
            ModelRouting::GenericWeighted
        );
        assert_eq!(
            version_id_for_category(MarketCategory::Crypto, &model),
            Some(version)
        );
    }

    #[test]
    fn absent_category_pointer_uses_generic_route() {
        let generic = ModelVersionId::from_v7();
        let model = ModelConfig {
            active_model_version_id: Some(version_ref(generic.clone())),
            category_model_pointers: iter::empty().collect(),
            ..ModelConfig::default()
        };

        assert_eq!(
            resolve_model_route(MarketCategory::Crypto, &model),
            ModelRouting::GenericWeighted
        );
        assert_eq!(generic_model_version_id(&model), Some(generic));
    }

    #[test]
    fn empty_pointers_use_generic_only() {
        let model = ModelConfig::default();
        assert_eq!(
            resolve_model_route(MarketCategory::Politics, &model),
            ModelRouting::GenericWeighted
        );
        assert!(generic_model_version_id(&model).is_none());
    }
}
