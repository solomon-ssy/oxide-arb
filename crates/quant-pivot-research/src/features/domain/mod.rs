//! Vertical (domain) feature skeleton.
//!
//! The [`DomainFeatureBuilder`] trait is the contract real vertical builders will
//! implement once external data is wired (deferred — 03.2 §10). Until then,
//! [`DomainFeatureSkeleton`] emits every domain feature as
//! [`NullReason::DomainDataMissing`] so the generic model proceeds without ever
//! fabricating a default. The `(family, name)` catalog [`DOMAIN_FEATURES`] is the
//! single source the schema registry and the skeleton both read from.

use quant_pivot_models::{enums::domain::DomainFamily, runtime_config::FeatureFamily};

use crate::features::{
    builder::{FeatureComputeCtx, FeatureGroupBuilder, RawFeature},
    names::domain as domain_names,
    value::{FeatureName, NullReason},
};

/// The canonical domain feature catalog: one representative feature per vertical.
///
/// Both [`crate::features::schema::FeatureSchema::build`] and
/// [`DomainFeatureSkeleton`] read this so names never drift.
pub const DOMAIN_FEATURES: [(DomainFamily, FeatureName); 5] = [
    (DomainFamily::Sports, domain_names::SPORTS_PRE_MATCH_MOVE),
    (DomainFamily::Politics, domain_names::POLITICS_POLL_MOMENTUM),
    (DomainFamily::Crypto, domain_names::CRYPTO_UNDERLYING_BETA),
    (
        DomainFamily::Weather,
        domain_names::WEATHER_FORECAST_REVISION,
    ),
    (
        DomainFamily::Geopolitics,
        domain_names::GEOPOLITICS_NEWS_SHOCK_DECAY,
    ),
];

/// A vertical-specific feature builder.
///
/// Real implementations consume an external domain source; their absence is the
/// reason the skeleton exists. A returned [`RawFeature`] must carry
/// [`NullReason::DomainDataMissing`] when its source is unavailable.
pub trait DomainFeatureBuilder: Send + Sync {
    /// The vertical this builder serves.
    fn family(&self) -> DomainFamily;

    /// Compute the vertical's raw features for one market.
    fn compute(&self, ctx: &FeatureComputeCtx<'_>) -> Vec<RawFeature>;
}

/// The placeholder domain group builder: emits domain-missing for every spec.
pub struct DomainFeatureSkeleton;

impl FeatureGroupBuilder for DomainFeatureSkeleton {
    fn family(&self) -> FeatureFamily {
        FeatureFamily::Domain
    }

    fn compute(&self, _ctx: &FeatureComputeCtx<'_>) -> Vec<RawFeature> {
        DOMAIN_FEATURES
            .into_iter()
            .map(|(_, name)| RawFeature::missing(name, NullReason::DomainDataMissing))
            .collect()
    }
}
