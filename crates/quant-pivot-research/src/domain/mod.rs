//! External-vertical (domain) point-in-time contracts.
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
pub mod weather_contract;

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    domain::data_plane::{
        CryptoPriceReport, DomainObservation, WeatherForecastPoint, WeatherObservationFact,
    },
    enums::domain::DomainMetric,
    types::calibration::PublishedWeatherStationLeadBias,
};
use rust_decimal::Decimal;
pub use slice::{
    DomainAvailabilityFacts, DomainFactWindows, build_domain_slice_inputs, crypto_lookback_secs,
    domain_availability_at, linkage_valid_at, oracle_instrument, source_binding,
    valid_weather_sources, weather_contract_bounds, weather_forecast_in_window,
    weather_history_start, weather_observation_in_window,
};
pub use weather_contract::{
    WeatherComparisonUnit, WeatherContractProjection, WeatherProjectionFailure,
    WeatherProjectionPurpose, WeatherTruthMaturity, project_weather_contract,
};

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

/// PIT-bounded typed Weather facts for one exact contract subject.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WeatherFactWindow {
    /// System decision instant bounding every fact's availability.
    pub decision_at: DateTime<Utc>,
    /// Source-native observations after visibility filtering.
    pub observations: Vec<WeatherObservationFact>,
    /// Source-native forecast points; calibration is applied only by the
    /// governed family-specific builder.
    pub forecasts: Vec<WeatherForecastPoint>,
    /// Latest immutable calibration publication visible at `decision_at`.
    pub calibration: Option<PublishedWeatherStationLeadBias>,
}

impl WeatherFactWindow {
    /// Freshest source-effective observation/run time.
    ///
    /// A GEFS `valid_time` is the future interval being forecast, not an
    /// observation clock. Freshness and PIT checks therefore use the model-run
    /// `reference_time`; publication and ingestion remain independently
    /// bounded by `published_at` / `available_at` when the slice is assembled.
    #[must_use]
    pub fn freshest_time(&self) -> Option<DateTime<Utc>> {
        self.observations
            .iter()
            .map(|fact| fact.observed_at)
            .chain(self.forecasts.iter().map(|fact| fact.reference_time))
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

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::data_plane::WeatherForecastPoint,
        types::{
            ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, IcaoStation,
            WeatherVariable,
        },
    };
    use rust_decimal_macros::dec;

    use super::WeatherFactWindow;

    #[test]
    fn forecast_freshness_uses_reference() {
        let station = IcaoStation::parse("KLGA").expect("station");
        let reference_time = Utc
            .with_ymd_and_hms(2026, 7, 26, 0, 0, 0)
            .single()
            .expect("reference time");
        let hash = ContentHash::parse(&format!("blake3:{}", "a".repeat(64))).expect("hash");
        let forecast = WeatherForecastPoint {
            source_id: DomainSourceId::gefs(),
            instrument_key: DomainInstrumentKey::gefs(&station),
            subject_key: station.to_string(),
            variable: WeatherVariable::TemperatureMaximum,
            value: dec!(24),
            unit: DomainMeasurementUnit::Celsius,
            precision: dec!(0.1),
            reference_time,
            valid_time: reference_time + Duration::days(2),
            published_at: reference_time + Duration::minutes(30),
            available_at: reference_time + Duration::minutes(31),
            lead_hours: 48,
            member: Some(0),
            revision: 1,
            grid_binding_hash: hash,
            run_manifest_hash: hash,
            report_hash: hash,
        };
        let window = WeatherFactWindow {
            decision_at: reference_time + Duration::hours(1),
            observations: Vec::new(),
            forecasts: vec![forecast],
            calibration: None,
        };

        assert_eq!(window.freshest_time(), Some(reference_time));
    }
}
