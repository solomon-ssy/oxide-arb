//! Domain-slice feature builders: category-routed external verticals.
//!
//! A [`DomainFeatureBuilder`] is the domain-plane sibling of
//! [`FeatureGroupBuilder`](crate::features::FeatureGroupBuilder): a pure
//! function from a frozen linkage binding plus pre-fetched PIT observation
//! windows to raw features. A `Resolved` linkage supplies compute inputs; an
//! unresolved linkage is materialized as explicit missing cells whenever the
//! category maps to an enabled vertical. `domain: None` is reserved for
//! structural non-applicability.

pub mod crypto;
pub mod weather;

use chrono::{DateTime, Utc};
pub use crypto::CryptoDomainFeatureBuilder;
use quant_pivot_models::{
    domain::quant::ResolvedBinding,
    enums::domain::DomainFamily,
    runtime_config::DomainConfig,
    types::{ContentHash, MarketLinkageId},
};
pub use weather::WeatherDomainFeatureBuilder;

use crate::{
    domain::{CryptoPriceReportWindow, DomainObservationWindow, WeatherFactWindow},
    features::{EvidenceSourceRef, builder::RawFeature},
};

/// Pure inputs to one market's domain-slice computation.
pub struct DomainComputeCtx<'a> {
    /// Decision time.
    pub decision_at: DateTime<Utc>,
    /// The frozen, validated linkage binding (subject + instrument + grounding).
    pub binding: &'a ResolvedBinding,
    /// Exact linkage revision and its effective/availability clocks.
    pub linkage_evidence: &'a EvidenceSourceRef,
    /// Family-specific, typed PIT facts. Cross-family reads are unrepresentable.
    pub data: DomainSliceDataRef<'a>,
    /// Frozen vertical parameters.
    pub domain: &'a DomainConfig,
}

/// Borrowed family-specific input projection consumed by a domain builder.
#[derive(Clone, Copy)]
pub enum DomainSliceDataRef<'a> {
    Crypto {
        primary: &'a DomainObservationWindow,
        oracle: Option<&'a CryptoPriceReportWindow>,
    },
    Weather(&'a WeatherFactWindow),
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
    /// Exact linkage ledger identity used to resolve the source roles.
    pub linkage_id: MarketLinkageId,
    /// Content address of the exact linkage revision.
    pub linkage_hash: ContentHash,
    /// The frozen, validated linkage binding.
    pub binding: ResolvedBinding,
    /// Exact linkage revision and its effective/availability clocks.
    pub linkage_evidence: EvidenceSourceRef,
    /// Family-specific typed facts. A Weather linkage cannot accidentally read
    /// Crypto windows, and vice versa.
    pub data: DomainSliceData,
}

/// Owned family-specific PIT facts.
#[derive(Debug, Clone)]
pub enum DomainSliceData {
    Crypto {
        primary: DomainObservationWindow,
        oracle: Option<CryptoPriceReportWindow>,
    },
    Weather(WeatherFactWindow),
}

impl DomainSliceData {
    #[must_use]
    pub const fn as_ref(&self) -> DomainSliceDataRef<'_> {
        match self {
            Self::Crypto { primary, oracle } => DomainSliceDataRef::Crypto {
                primary,
                oracle: oracle.as_ref(),
            },
            Self::Weather(window) => DomainSliceDataRef::Weather(window),
        }
    }

    #[must_use]
    pub fn freshest_time(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Crypto { primary, oracle } => primary
                .freshest_time()
                .into_iter()
                .chain(
                    oracle
                        .as_ref()
                        .and_then(|window| window.latest().map(|report| report.event_time)),
                )
                .max(),
            Self::Weather(window) => window.freshest_time(),
        }
    }
}
