//! Vertical (domain) feature skeleton.
//!
//! The [`DomainFeatureBuilder`] trait is the contract real vertical builders will
//! implement once external data is wired (deferred — 03.2 §10). Until then,
//! [`DomainFeatureSkeleton`] emits every domain feature as
//! [`NullReason::DomainDataMissing`] so the generic model proceeds without ever
//! fabricating a default. The `(family, name)` catalog [`DOMAIN_FEATURES`] is the
//! single source the schema registry and the skeleton both read from.

use crate::{
    features::{
        builder::{FeatureComputeCtx, FeatureGroupBuilder, RawFeature},
        value::{FeatureName, NullReason},
    },
    vertical::DomainFamily,
};
use quant_pivot_models::runtime_config::FeatureFamily;

/// The canonical domain feature catalog: one representative feature per vertical.
///
/// Both [`crate::features::schema::FeatureSchema::build`] and
/// [`DomainFeatureSkeleton`] read this so names never drift.
pub const DOMAIN_FEATURES: [(DomainFamily, &str); 5] = [
    (DomainFamily::Sports, "domain.sports.pre_match_move"),
    (DomainFamily::Politics, "domain.politics.poll_momentum"),
    (DomainFamily::Crypto, "domain.crypto.underlying_beta"),
    (DomainFamily::Weather, "domain.weather.forecast_revision"),
    (
        DomainFamily::Geopolitics,
        "domain.geopolitics.news_shock_decay",
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
            .map(|(_, name)| {
                RawFeature::missing(
                    FeatureName::from_static(name),
                    NullReason::DomainDataMissing,
                )
            })
            .collect()
    }
}
