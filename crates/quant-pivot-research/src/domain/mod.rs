//! External-vertical (domain) point-in-time contracts (Phase 11.2.2).
//!
//! Mirrors the platform PIT plane: a [`DomainPitQueryEngine`] answers
//! window queries over stored `quant_domain_observation` facts, and the
//! feature pipeline pre-fetches per-instrument [`DomainObservationWindow`]
//! snapshots so domain feature computation is a pure function. The online
//! (`ClickHouse`) and offline ([`MaterializedDomainPitEngine`]) backends
//! return byte-identical windows for the same visibility bounds.

pub mod materialized;
pub mod slice;

pub use materialized::MaterializedDomainPitEngine;
pub use slice::{
    build_domain_slice_inputs, crypto_lookback_secs, linkage_valid_at, oracle_instrument,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::DomainObservation, enums::domain::DomainMetric, types::DomainInstrumentKey,
};
use rust_decimal::Decimal;

/// PIT window query over stored domain observations.
///
/// The caller owns the visibility arithmetic (`to = as_of - source_delay`);
/// implementations return **ascending** `observed_at` order with the stable
/// ingestion-time tie-break, all strictly inside `[from, to)`.
#[async_trait]
pub trait DomainPitQueryEngine: Send + Sync {
    /// Observations for one instrument with `observed_at ∈ [from, to)`.
    ///
    /// # Errors
    ///
    /// Propagates backend query failures.
    async fn observations_between(
        &self,
        instrument_key: &DomainInstrumentKey,
        from: DateTime<Utc>,
        to_exclusive: DateTime<Utc>,
    ) -> QuantResult<Vec<DomainObservation>>;
}

/// A pre-fetched, PIT-bounded window of observations for one instrument.
///
/// All observations satisfy `observed_at <= cutoff` (the `as_of - source_delay`
/// visibility bound) and are ascending by `observed_at`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DomainObservationWindow {
    /// Upper visibility bound the window was fetched under.
    pub cutoff: DateTime<Utc>,
    /// Ascending observations, all `observed_at <= cutoff`.
    pub observations: Vec<DomainObservation>,
}

impl DomainObservationWindow {
    /// The freshest observation of `metric`, if any.
    #[must_use]
    pub fn latest(&self, metric: DomainMetric) -> Option<&DomainObservation> {
        self.observations
            .iter()
            .rev()
            .find(|observation| observation.metric == metric)
    }

    /// The freshest observation of `metric` at or before `at`.
    #[must_use]
    pub fn latest_at(&self, metric: DomainMetric, at: DateTime<Utc>) -> Option<&DomainObservation> {
        self.observations
            .iter()
            .rev()
            .find(|observation| observation.metric == metric && observation.observed_at <= at)
    }

    /// Ascending value series of `metric` within `[from, cutoff]`.
    #[must_use]
    pub fn series_since(&self, metric: DomainMetric, from: DateTime<Utc>) -> Vec<Decimal> {
        self.observations
            .iter()
            .filter(|observation| observation.metric == metric && observation.observed_at >= from)
            .map(|observation| observation.value)
            .collect()
    }

    /// The freshest observation time across all metrics, if any.
    #[must_use]
    pub fn freshest_time(&self) -> Option<DateTime<Utc>> {
        self.observations
            .last()
            .map(|observation| observation.observed_at)
    }

    /// Whether the window holds no observations.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.observations.is_empty()
    }
}
