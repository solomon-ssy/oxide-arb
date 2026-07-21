//! [`WeightOverlayApplicator`]: the hot-reloadable factor-weight overlay snapshot
//! for non-published candidate / shadow versions.
//!
//! On each runtime-config activation the applicator parses
//! `FactorsConfig.factor_weights` into a research [`WeightOverlay`] and maps it
//! to the configured `active` / `shadow` model version ids. The online
//! [`ModelRunner`](crate::service::model_runner::ModelRunner) consults this
//! snapshot **after** resolving a version's `publication_status`: a `Published`
//! version always scores on its frozen artifact weights (overlay forbidden);
//! a `Candidate` / `Shadow` version may be overridden without re-publishing or
//! mutating the artifact bytes.
//!
//! A malformed overlay (un-normalized / negative weights) is rejected here and
//! the snapshot falls back to "no overlay" — fail-closed to the published
//! behavior rather than scoring on a broken weight table.

use std::{collections::HashMap, sync::Arc};

use arc_swap::ArcSwap;
use quant_pivot_models::{
    runtime_config::{FactorsConfig, ModelConfig},
    types::ModelVersionId,
};
use quant_pivot_research::model::WeightOverlay;

/// Immutable snapshot of the per-version weight overlays for one config version.
#[derive(Default)]
pub struct WeightOverlaySnapshot {
    by_version: HashMap<ModelVersionId, WeightOverlay>,
}

impl WeightOverlaySnapshot {
    /// The overlay for a model version, if any was configured.
    #[must_use]
    pub fn overlay_for(&self, version_id: &ModelVersionId) -> Option<WeightOverlay> {
        self.by_version.get(version_id).cloned()
    }

    /// Whether the snapshot carries any overlay.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_version.is_empty()
    }
}

/// Lock-free, hot-reloadable holder for the active weight-overlay snapshot.
pub struct WeightOverlayApplicator {
    snapshot: ArcSwap<WeightOverlaySnapshot>,
}

impl Default for WeightOverlayApplicator {
    fn default() -> Self {
        Self::new()
    }
}

impl WeightOverlayApplicator {
    /// An applicator with no overlay (the default before any activation).
    #[must_use]
    pub fn new() -> Self {
        Self {
            snapshot: ArcSwap::from_pointee(WeightOverlaySnapshot::default()),
        }
    }

    /// Rebuild the overlay snapshot from a freshly activated runtime config.
    ///
    /// An empty `factor_weights` set leaves no overlay; a malformed set is
    /// logged and dropped (fail-closed to artifact weights).
    pub fn reload(&self, factors: &FactorsConfig, model: &ModelConfig) {
        let snapshot = build_snapshot(factors, model);
        self.snapshot.store(Arc::new(snapshot));
    }

    /// The overlay for a model version under the current snapshot, if any.
    #[must_use]
    pub fn overlay_for(&self, version_id: &ModelVersionId) -> Option<WeightOverlay> {
        self.snapshot.load().overlay_for(version_id)
    }
}

/// Parse the config overlay and map it to the configured active / shadow ids.
fn build_snapshot(factors: &FactorsConfig, model: &ModelConfig) -> WeightOverlaySnapshot {
    let mut by_version = HashMap::new();
    if factors.factor_weights.weights.is_empty() {
        return WeightOverlaySnapshot { by_version };
    }
    let overlay = match WeightOverlay::from_config(&factors.factor_weights) {
        Ok(overlay) => overlay,
        Err(error) => {
            tracing::warn!(
                %error,
                "factor_weights overlay is invalid; falling back to artifact weights"
            );
            return WeightOverlaySnapshot { by_version };
        }
    };
    for reference in [
        model.active_model_version_id.as_ref(),
        model.shadow_model_version_id.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        by_version.insert(reference.id.clone(), overlay.clone());
    }
    WeightOverlaySnapshot { by_version }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        runtime_config::{DecimalValue, FactorsConfig, ModelConfig, ModelVersionRef},
        types::ModelVersionId,
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::WeightOverlayApplicator;

    fn factors_with(weights: &[(&str, Decimal)]) -> FactorsConfig {
        let mut config = FactorsConfig::default();
        config.factor_weights.weights = weights
            .iter()
            .map(|(name, value)| ((*name).to_owned(), DecimalValue::new(*value)))
            .collect();
        config
    }

    #[test]
    fn maps_overlay_to_active_and_shadow_versions() {
        let active = ModelVersionId::from_v7();
        let shadow = ModelVersionId::from_v7();
        let model = ModelConfig {
            active_model_version_id: Some(ModelVersionRef::new(active.clone())),
            shadow_model_version_id: Some(ModelVersionRef::new(shadow.clone())),
            ..ModelConfig::default()
        };
        let applicator = WeightOverlayApplicator::new();
        applicator.reload(
            &factors_with(&[("liquidity_depth", dec!(0.5)), ("momentum", dec!(0.5))]),
            &model,
        );
        assert!(applicator.overlay_for(&active).is_some());
        assert!(applicator.overlay_for(&shadow).is_some());
        assert!(applicator.overlay_for(&ModelVersionId::from_v7()).is_none());
    }

    #[test]
    fn invalid_overlay_falls_back_to_none() {
        let active = ModelVersionId::from_v7();
        let model = ModelConfig {
            active_model_version_id: Some(ModelVersionRef::new(active.clone())),
            ..ModelConfig::default()
        };
        let applicator = WeightOverlayApplicator::new();
        // Weights do not sum to 1 → invalid → dropped.
        applicator.reload(
            &factors_with(&[("liquidity_depth", dec!(0.5)), ("momentum", dec!(0.9))]),
            &model,
        );
        assert!(applicator.overlay_for(&active).is_none());
    }

    #[test]
    fn empty_weights_have_no_overlay() {
        let applicator = WeightOverlayApplicator::new();
        applicator.reload(&FactorsConfig::default(), &ModelConfig::default());
        assert!(applicator.overlay_for(&ModelVersionId::from_v7()).is_none());
    }
}
