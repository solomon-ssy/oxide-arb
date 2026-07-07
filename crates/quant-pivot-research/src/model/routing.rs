//! Category-aware model routing (Phase 11.2.2 §3.8).
//!
//! Runtime config may pin a category-specific published model via
//! `model.category_model_pointers`; markets whose category has no pointer (or
//! whose artifact is unavailable at load time) fall back to the generic
//! `active_model_version_id`.

use quant_pivot_error::QuantResult;
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
/// A configured pointer wins; parse failures are ignored so a malformed ref
/// fails closed into the generic route rather than breaking the whole batch.
#[must_use]
pub fn resolve_model_route(category: MarketCategory, model: &ModelConfig) -> ModelRouting {
    if let Some(reference) = model.category_model_pointers.get(&category)
        && let Ok(artifact) = ModelVersionId::try_from(reference)
    {
        return ModelRouting::CategorySpecific { category, artifact };
    }
    ModelRouting::GenericWeighted
}

/// The generic active model version, when configured.
///
/// # Errors
///
/// Returns a config error when `active_model_version_id` is present but invalid.
pub fn generic_model_version_id(model: &ModelConfig) -> QuantResult<Option<ModelVersionId>> {
    model
        .active_model_version_id
        .as_ref()
        .map(ModelVersionId::try_from)
        .transpose()
}

/// Resolve the model version id that should score a market in this category.
///
/// Category pointers override the generic active model; absent or invalid
/// pointers fall back to generic.
///
/// # Errors
///
/// Propagates config parse failures on configured refs.
pub fn version_id_for_category(
    category: MarketCategory,
    model: &ModelConfig,
) -> QuantResult<Option<ModelVersionId>> {
    match resolve_model_route(category, model) {
        ModelRouting::GenericWeighted => generic_model_version_id(model),
        ModelRouting::CategorySpecific { artifact, .. } => Ok(Some(artifact)),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ModelRouting, generic_model_version_id, resolve_model_route, version_id_for_category,
    };
    use quant_pivot_models::{
        enums::common::MarketCategory,
        runtime_config::{ModelConfig, ModelVersionRef},
        types::ModelVersionId,
    };

    fn version_ref(id: &str) -> ModelVersionRef {
        ModelVersionRef { id: id.to_owned() }
    }

    #[test]
    fn category_pointer_selects_category_specific_route() {
        let version = ModelVersionId::from_v7();
        let mut model = ModelConfig::default();
        model
            .category_model_pointers
            .insert(MarketCategory::Crypto, version_ref(&version.to_string()));

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
            version_id_for_category(MarketCategory::Crypto, &model).expect("ok"),
            Some(version)
        );
    }

    #[test]
    fn invalid_category_pointer_falls_back_to_generic() {
        let generic = ModelVersionId::from_v7();
        let model = ModelConfig {
            active_model_version_id: Some(version_ref(&generic.to_string())),
            category_model_pointers: std::iter::once((
                MarketCategory::Crypto,
                ModelVersionRef {
                    id: "not-a-uuid".to_owned(),
                },
            ))
            .collect(),
            ..ModelConfig::default()
        };

        assert_eq!(
            resolve_model_route(MarketCategory::Crypto, &model),
            ModelRouting::GenericWeighted
        );
        assert_eq!(generic_model_version_id(&model).expect("ok"), Some(generic));
    }

    #[test]
    fn empty_pointers_use_generic_only() {
        let model = ModelConfig::default();
        assert_eq!(
            resolve_model_route(MarketCategory::Politics, &model),
            ModelRouting::GenericWeighted
        );
        assert!(generic_model_version_id(&model).expect("ok").is_none());
    }
}
