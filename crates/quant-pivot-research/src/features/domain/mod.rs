//! Domain-slice feature builders: category-routed external verticals.
//!
//! A [`DomainFeatureBuilder`] is the domain-plane sibling of
//! [`FeatureGroupBuilder`](crate::features::FeatureGroupBuilder): a pure
//! function from a frozen linkage binding plus pre-fetched PIT observation
//! windows to raw features. The pipeline invokes the builder for a market
//! **only** when the market's category maps to an enabled vertical with a
//! `Resolved` linkage — otherwise the vector carries `domain: None`
//! (structurally absent, never a row of missing values).

pub mod crypto;

pub use crypto::CryptoDomainFeatureBuilder;

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::ResolvedBinding, enums::domain::DomainFamily, runtime_config::CryptoDomainConfig,
};

use crate::{domain::DomainObservationWindow, features::builder::RawFeature};

/// Pure inputs to one market's domain-slice computation.
pub struct DomainComputeCtx<'a> {
    /// Decision time.
    pub as_of: DateTime<Utc>,
    /// The frozen, validated linkage binding (subject + instrument + grounding).
    pub binding: &'a ResolvedBinding,
    /// PIT window for the feature-source instrument (e.g. Binance klines).
    pub primary: &'a DomainObservationWindow,
    /// PIT window for the settlement-oracle instrument (e.g. Chainlink feed),
    /// when one is ingested for this subject.
    pub oracle: Option<&'a DomainObservationWindow>,
    /// Frozen crypto vertical parameters.
    pub crypto: &'a CryptoDomainConfig,
}

/// A pure domain feature-group computation (no I/O, no clock, no mutable state).
pub trait DomainFeatureBuilder: Send + Sync {
    /// The vertical this builder owns.
    fn family(&self) -> DomainFamily;

    /// Compute the vertical's raw features for one linked market.
    fn compute(&self, ctx: &DomainComputeCtx<'_>) -> Vec<RawFeature>;
}

/// Owned, pre-fetched domain inputs for one market's feature build.
///
/// Constructed by the pipeline **only** for markets whose category maps to an
/// enabled vertical with a `Resolved` linkage — its existence is the proof the
/// domain slice applies. Everything inside is PIT-bounded upstream.
#[derive(Debug, Clone)]
pub struct DomainSliceInputs {
    /// The vertical this market maps to.
    pub family: DomainFamily,
    /// The frozen, validated linkage binding.
    pub binding: ResolvedBinding,
    /// PIT window for the feature-source instrument.
    pub primary: DomainObservationWindow,
    /// PIT window for the settlement-oracle instrument, when ingested.
    pub oracle: Option<DomainObservationWindow>,
}
