//! External-vertical (domain) point-in-time contracts (Phase 11.2.2).
//!
//! Train-serve parity for the domain slice is a **single shared code path**,
//! not a dual-engine mirror: both the online feature pipeline
//! (`quant-pivot-core::service::feature_pipeline`) and the offline replay
//! (`quant-pivot-core::service::historical_replay`) prefetch the full
//! `quant_domain_observation` range for the round into an in-memory
//! `HashMap<DomainInstrumentKey, Vec<DomainObservation>>` via the identical
//! `QuantFactReadRepository::domain_observations_between` query, then call
//! the **same** [`build_domain_slice_inputs`] function to assemble the PIT
//! window. There is exactly one assembly implementation to keep in sync —
//! zero skew is structural, not tested-for.

pub mod slice;

pub use slice::{
    DomainAvailabilityFacts, DomainFactWindows, build_domain_slice_inputs, crypto_lookback_secs,
    domain_availability_at, linkage_valid_at, oracle_instrument, source_binding,
};

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::{
        CryptoPriceReport, DomainObservation, PublishedWeatherStationLeadBias,
        WeatherForecastPoint, WeatherObservationFact,
    },
    enums::domain::DomainMetric,
};
use rust_decimal::Decimal;

/// A pre-fetched, PIT-bounded window of observations for one instrument.
///
/// All observations satisfy the source cutoff frozen in the decision boundary
/// and are ascending by `observed_at`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DomainObservationWindow {
    /// Upper visibility bound the window was fetched under.
    pub cutoff: DateTime<Utc>,
    /// Ascending observations, all `observed_at <= cutoff`.
    pub observations: Vec<DomainObservation>,
}

/// PIT-bounded signed/venue crypto reports for one source instrument.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CryptoPriceReportWindow {
    pub cutoff: DateTime<Utc>,
    pub reports: Vec<CryptoPriceReport>,
}

/// PIT-bounded typed Weather facts for one station.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WeatherFactWindow {
    /// System decision instant bounding every fact's availability.
    pub decision_at: DateTime<Utc>,
    /// `AviationWeather` live reports and `GHCNh` calibration observations.
    pub observations: Vec<WeatherObservationFact>,
    /// Raw GEFS ensemble points; bias is applied only by the governed builder.
    pub forecasts: Vec<WeatherForecastPoint>,
    /// Latest immutable calibration publication visible at `decision_at`.
    pub calibration: Option<PublishedWeatherStationLeadBias>,
}

impl WeatherFactWindow {
    #[must_use]
    pub fn freshest_time(&self) -> Option<DateTime<Utc>> {
        self.observations
            .iter()
            .map(|fact| fact.observed_at)
            .chain(self.forecasts.iter().map(|fact| fact.valid_time))
            .max()
    }
}

impl CryptoPriceReportWindow {
    #[must_use]
    pub fn latest(&self) -> Option<&CryptoPriceReport> {
        self.reports.last()
    }

    #[must_use]
    pub fn latest_at(&self, at: DateTime<Utc>) -> Option<&CryptoPriceReport> {
        self.reports
            .iter()
            .rev()
            .find(|report| report.event_time <= at)
    }
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
