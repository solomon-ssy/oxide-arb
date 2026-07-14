//! Airport local-day maximum-temperature features.
//!
//! Forecast features are emitted only from a complete 31-member GEFS run after
//! every target lead has a station-specific bias estimate backed by distinct
//! `GHCNh` observations. Missing calibration is an explicit missing feature,
//! never an assumed zero correction.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Timelike, Utc};
use chrono_tz::Tz;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    domain::{
        MarketSubject, WeatherForecastPoint, WeatherLeadBiasV1, WeatherObservationFact,
        WeatherObservationReportKind, WeatherStationBiasV1, WeatherStationLeadBiasArtifactV1,
        WeatherSubject,
    },
    enums::{domain::DomainFamily, feature::EvidenceSourceKind},
    hashing::CanonicalDigest,
    runtime_config::WeatherDomainConfig,
    types::{ContentHash, IcaoStation, Probability, TemperatureCelsius},
};
use rust_decimal::Decimal;

use crate::{
    domain::WeatherFactWindow,
    features::{
        builder::RawFeature,
        domain::{DomainComputeCtx, DomainFeatureBuilder, DomainSliceDataRef},
        names::domain_weather as names,
        value::{EvidenceSourceRef, FeatureValue, NullReason},
    },
};

const SQRT_ITERATIONS: usize = 32;
const FORECAST_INTERVAL_HOURS: i64 = 3;
const WEATHER_BIAS_METHODOLOGY: &str = "gefs_00z_ensemble_mean_minus_ghcnh_three_hour_max_v1";

type ObservationKey = (IcaoStation, DateTime<Utc>);
type ForecastGroupKey = (IcaoStation, DateTime<Utc>, DateTime<Utc>, u16);
type ForecastVariantKey = (ContentHash, ContentHash);
type ForecastGroups<'a> =
    BTreeMap<ForecastGroupKey, BTreeMap<ForecastVariantKey, Vec<&'a WeatherForecastPoint>>>;
type BiasErrors = BTreeMap<IcaoStation, BTreeMap<u16, Vec<Decimal>>>;
type CalibrationSampleKey = (
    IcaoStation,
    DateTime<Utc>,
    DateTime<Utc>,
    u16,
    ContentHash,
    ContentHash,
);

#[derive(Default)]
struct WeatherCalibrationSamples {
    errors: BiasErrors,
    sample_keys: Vec<CalibrationSampleKey>,
    observation_hashes: BTreeSet<ContentHash>,
    manifest_hashes: BTreeSet<ContentHash>,
    grid_hashes: BTreeSet<ContentHash>,
}

/// Deterministic result of one offline Weather station-by-lead fit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeatherStationLeadBiasFit {
    pub payload: WeatherStationLeadBiasArtifactV1,
    pub calibration_split_hash: ContentHash,
    pub sample_count: i64,
}

/// Fit frozen station-by-lead GEFS bias from historical facts.
///
/// One sample is one complete `(station, run, valid_time, lead)` ensemble mean
/// paired with the latest-revision `GHCNh` maximum in its three-hour interval.
pub fn fit_weather_station_lead_bias(
    observations: &[WeatherObservationFact],
    forecasts: &[WeatherForecastPoint],
    fit_start: DateTime<Utc>,
    fit_end: DateTime<Utc>,
    required_members: u8,
) -> QuantResult<WeatherStationLeadBiasFit> {
    if fit_start >= fit_end || required_members == 0 {
        return Err(QuantError::config(
            "Weather calibration requires a non-empty fit window and members",
        ));
    }
    let latest = latest_historical_observations(observations, fit_start, fit_end);
    let groups = group_forecasts(forecasts, fit_start, fit_end);
    let samples = pair_calibration_samples(groups, &latest, required_members)?;
    if samples.sample_keys.is_empty() {
        return Err(QuantError::config(
            "Weather calibration has no complete forecast/observation pairs",
        ));
    }
    assemble_weather_bias_fit(samples)
}

fn latest_historical_observations(
    observations: &[WeatherObservationFact],
    fit_start: DateTime<Utc>,
    fit_end: DateTime<Utc>,
) -> BTreeMap<ObservationKey, &WeatherObservationFact> {
    let mut latest = BTreeMap::<ObservationKey, &WeatherObservationFact>::new();
    let history_start = fit_start - Duration::hours(FORECAST_INTERVAL_HOURS);
    for fact in observations.iter().filter(|fact| {
        fact.report_kind == WeatherObservationReportKind::HistoricalGhcnh
            && fact.observation_time >= history_start
            && fact.observation_time < fit_end
    }) {
        latest
            .entry((fact.station.clone(), fact.observation_time))
            .and_modify(|current| {
                if (fact.revision, fact.available_at, fact.report_hash.as_str())
                    > (
                        current.revision,
                        current.available_at,
                        current.report_hash.as_str(),
                    )
                {
                    *current = fact;
                }
            })
            .or_insert(fact);
    }
    latest
}

fn group_forecasts(
    forecasts: &[WeatherForecastPoint],
    fit_start: DateTime<Utc>,
    fit_end: DateTime<Utc>,
) -> ForecastGroups<'_> {
    let mut groups = ForecastGroups::new();
    for point in forecasts.iter().filter(|point| {
        point.valid_time >= fit_start
            && point.valid_time < fit_end
            && point.reference_time.hour() == 0
    }) {
        groups
            .entry((
                point.station.clone(),
                point.reference_time,
                point.valid_time,
                point.lead_hours,
            ))
            .or_default()
            .entry((
                point.run_manifest_hash.clone(),
                point.grid_binding_hash.clone(),
            ))
            .or_default()
            .push(point);
    }
    groups
}

fn pair_calibration_samples(
    groups: ForecastGroups<'_>,
    latest: &BTreeMap<ObservationKey, &WeatherObservationFact>,
    required_members: u8,
) -> QuantResult<WeatherCalibrationSamples> {
    let mut samples = WeatherCalibrationSamples::default();
    for ((station, reference_time, valid_time, lead), variants) in groups {
        let candidate = variants
            .into_iter()
            .filter(|(_, points)| has_complete_members(points, required_members))
            .max_by(
                |((left_manifest, _), left_points), ((right_manifest, _), right_points)| {
                    let left_available = left_points.iter().map(|point| point.available_at).max();
                    let right_available = right_points.iter().map(|point| point.available_at).max();
                    (left_available, left_manifest).cmp(&(right_available, right_manifest))
                },
            );
        let Some(((manifest_hash, grid_hash), points)) = candidate else {
            continue;
        };
        let interval_start = valid_time - Duration::hours(FORECAST_INTERVAL_HOURS);
        let observed = latest
            .range((station.clone(), interval_start)..=(station.clone(), valid_time))
            .filter(|((candidate, time), _)| {
                candidate == &station && *time > interval_start && *time <= valid_time
            })
            .map(|(_, fact)| *fact)
            .max_by_key(|fact| fact.temperature.value());
        let Some(observed) = observed else {
            continue;
        };
        let member_count = u32::try_from(points.len())
            .map_err(|error| QuantError::config(format!("GEFS member count overflow: {error}")))?;
        let forecast_mean = points
            .iter()
            .map(|point| point.tmax_celsius.value())
            .sum::<Decimal>()
            / Decimal::from(member_count);
        samples
            .errors
            .entry(station.clone())
            .or_default()
            .entry(lead)
            .or_default()
            .push(forecast_mean - observed.temperature.value());
        samples.sample_keys.push((
            station,
            reference_time,
            valid_time,
            lead,
            manifest_hash.clone(),
            observed.report_hash.clone(),
        ));
        samples
            .observation_hashes
            .insert(observed.report_hash.clone());
        samples.manifest_hashes.insert(manifest_hash);
        samples.grid_hashes.insert(grid_hash);
    }
    Ok(samples)
}

fn station_biases(errors: BiasErrors) -> QuantResult<(Vec<WeatherStationBiasV1>, i64)> {
    let mut sample_count = 0_i64;
    let mut stations = Vec::with_capacity(errors.len());
    for (station, lead_errors) in errors {
        let mut leads = Vec::with_capacity(lead_errors.len());
        for (lead_hours, values) in lead_errors {
            let count = u32::try_from(values.len()).map_err(|error| {
                QuantError::config(format!("Weather calibration sample overflow: {error}"))
            })?;
            sample_count = sample_count
                .checked_add(i64::from(count))
                .ok_or_else(|| QuantError::config("Weather calibration total sample overflow"))?;
            leads.push(WeatherLeadBiasV1 {
                lead_hours,
                sample_count: count,
                bias_celsius: values.iter().copied().sum::<Decimal>() / Decimal::from(count),
            });
        }
        stations.push(WeatherStationBiasV1 { station, leads });
    }
    Ok((stations, sample_count))
}

fn assemble_weather_bias_fit(
    mut samples: WeatherCalibrationSamples,
) -> QuantResult<WeatherStationLeadBiasFit> {
    let (stations, sample_count) = station_biases(samples.errors)?;
    samples.sample_keys.sort();
    let calibration_split_hash = CanonicalDigest::content_hash_json(&samples.sample_keys)?;
    let observation_set_hash = CanonicalDigest::content_hash_json(&samples.observation_hashes)?;
    let manifest_set_hash = CanonicalDigest::content_hash_json(&samples.manifest_hashes)?;
    let mut source_hashes = vec![observation_set_hash, manifest_set_hash];
    source_hashes.sort();
    source_hashes.dedup();
    let methodology = WEATHER_BIAS_METHODOLOGY.to_owned();
    let methodology_hash = CanonicalDigest::content_hash_json(&methodology)?;
    Ok(WeatherStationLeadBiasFit {
        payload: WeatherStationLeadBiasArtifactV1 {
            schema_version: 1,
            methodology,
            methodology_hash,
            grid_hashes: samples.grid_hashes.into_iter().collect(),
            source_hashes,
            stations,
        },
        calibration_split_hash,
        sample_count,
    })
}

/// The Weather vertical's pure feature builder.
pub struct WeatherDomainFeatureBuilder;

impl DomainFeatureBuilder for WeatherDomainFeatureBuilder {
    fn family(&self) -> DomainFamily {
        DomainFamily::Weather
    }

    fn compute(&self, ctx: &DomainComputeCtx<'_>) -> Vec<RawFeature> {
        let (MarketSubject::Weather(subject), DomainSliceDataRef::Weather(window)) =
            (&ctx.binding.subject, ctx.data)
        else {
            return Vec::new();
        };
        compute_weather_features(subject, window, &ctx.domain.weather)
    }
}

fn compute_weather_features(
    subject: &WeatherSubject,
    window: &WeatherFactWindow,
    config: &WeatherDomainConfig,
) -> Vec<RawFeature> {
    let ensemble = calibrated_ensemble(subject, window, config);
    let (ensemble_probability, ensemble_spread) = match ensemble {
        Ok(ensemble) => {
            let count = ensemble
                .member_highs
                .iter()
                .filter(|value| {
                    subject
                        .outcome_band
                        .contains(value.whole_degrees(subject.market_unit))
                })
                .count();
            let probability = Decimal::from(count) / Decimal::from(ensemble.member_highs.len());
            (
                RawFeature::present(
                    names::ENSEMBLE_BIN_PROBABILITY,
                    FeatureValue::Probability(Probability::new(probability)),
                    ensemble.evidence.clone(),
                ),
                RawFeature::present(
                    names::ENSEMBLE_SPREAD,
                    FeatureValue::Decimal(ensemble_standard_deviation(&ensemble.member_highs)),
                    ensemble.evidence,
                ),
            )
        }
        Err(reason) => (
            RawFeature::missing(names::ENSEMBLE_BIN_PROBABILITY, reason),
            RawFeature::missing(names::ENSEMBLE_SPREAD, reason),
        ),
    };

    let live_daily = daily_highs(
        &window.observations,
        WeatherObservationReportKind::Metar,
        Some(WeatherObservationReportKind::Speci),
        Some(WeatherObservationReportKind::Correction),
    );
    let observed_high = live_daily.get(&subject.local_date).copied();
    let observed_evidence = latest_observation_evidence(
        &window.observations,
        subject.local_date,
        WeatherObservationReportKind::HistoricalGhcnh,
        false,
    );
    let observed_headroom = match (observed_high, observed_evidence) {
        (Some(high), Some(evidence)) => RawFeature::present(
            names::OBSERVED_HIGH_HEADROOM,
            FeatureValue::Decimal(headroom(subject, high)),
            evidence,
        ),
        _ => RawFeature::missing(
            names::OBSERVED_HIGH_HEADROOM,
            NullReason::DomainSourceUnavailable,
        ),
    };

    let basis_risk = noaa_basis_risk(subject, window, config);
    vec![
        ensemble_probability,
        ensemble_spread,
        observed_headroom,
        basis_risk,
    ]
}

struct CalibratedEnsemble {
    member_highs: Vec<TemperatureCelsius>,
    evidence: EvidenceSourceRef,
}

fn calibrated_ensemble(
    subject: &WeatherSubject,
    window: &WeatherFactWindow,
    config: &WeatherDomainConfig,
) -> Result<CalibratedEnsemble, NullReason> {
    let timezone = subject
        .timezone
        .parse::<Tz>()
        .map_err(|_| NullReason::LinkageUnresolved)?;
    let target = latest_complete_target_run(subject, window, config, timezone)?;
    let calibration = window
        .calibration
        .as_ref()
        .ok_or(NullReason::InsufficientHistory)?;
    let station_bias = calibration
        .payload
        .stations
        .iter()
        .find(|station| station.station == subject.station)
        .ok_or(NullReason::InsufficientHistory)?;
    let mut highs = BTreeMap::<u8, Decimal>::new();
    for point in &target.points {
        let lead = station_bias
            .leads
            .iter()
            .find(|lead| lead.lead_hours == point.lead_hours)
            .ok_or(NullReason::InsufficientHistory)?;
        if lead.sample_count < config.minimum_bias_samples_per_lead {
            return Err(NullReason::InsufficientHistory);
        }
        let corrected = point.tmax_celsius.value() - lead.bias_celsius;
        highs
            .entry(point.member)
            .and_modify(|current| *current = (*current).max(corrected))
            .or_insert(corrected);
    }
    if highs.len() != usize::from(config.minimum_complete_members) {
        return Err(NullReason::DomainSourceUnavailable);
    }
    let effective_at = target
        .points
        .iter()
        .map(|point| point.valid_time)
        .max()
        .ok_or(NullReason::DomainSourceUnavailable)?;
    Ok(CalibratedEnsemble {
        member_highs: highs.into_values().map(TemperatureCelsius::new).collect(),
        evidence: EvidenceSourceRef {
            source_kind: EvidenceSourceKind::DomainExternal,
            reference: format!(
                "gefs:{}#{}:bias={}@{}",
                target.reference_time.timestamp_millis(),
                target.run_manifest_hash,
                calibration.artifact_id,
                calibration.content_hash,
            ),
            effective_at,
            available_at: Some(target.available_at.max(calibration.published_at)),
        },
    })
}

struct TargetRun<'a> {
    reference_time: DateTime<Utc>,
    available_at: DateTime<Utc>,
    run_manifest_hash: &'a ContentHash,
    points: Vec<&'a WeatherForecastPoint>,
}

fn latest_complete_target_run<'a>(
    subject: &WeatherSubject,
    window: &'a WeatherFactWindow,
    config: &WeatherDomainConfig,
    timezone: Tz,
) -> Result<TargetRun<'a>, NullReason> {
    let max_age =
        i64::try_from(config.max_forecast_age_secs).map_err(|_| NullReason::OutOfValidRange)?;
    let mut runs =
        BTreeMap::<(DateTime<Utc>, &ContentHash, &ContentHash), Vec<&WeatherForecastPoint>>::new();
    for point in &window.forecasts {
        if point.valid_time.with_timezone(&timezone).date_naive() != subject.local_date
            || point.reference_time > window.decision_at
            || window
                .decision_at
                .signed_duration_since(point.reference_time)
                .num_seconds()
                > max_age
        {
            continue;
        }
        runs.entry((
            point.reference_time,
            &point.run_manifest_hash,
            &point.grid_binding_hash,
        ))
        .or_default()
        .push(point);
    }
    runs.into_iter()
        .rev()
        .find_map(|((reference_time, run_manifest_hash, _), points)| {
            has_complete_members(&points, config.minimum_complete_members).then(|| TargetRun {
                reference_time,
                available_at: points
                    .iter()
                    .map(|point| point.available_at)
                    .max()
                    .unwrap_or(reference_time),
                run_manifest_hash,
                points,
            })
        })
        .ok_or(NullReason::DomainSourceUnavailable)
}

fn has_complete_members(points: &[&WeatherForecastPoint], required: u8) -> bool {
    if required == 0 {
        return false;
    }
    let members = points
        .iter()
        .map(|point| point.member)
        .collect::<BTreeSet<_>>();
    members == (0..required).collect::<BTreeSet<_>>()
}

fn daily_highs(
    facts: &[WeatherObservationFact],
    primary: WeatherObservationReportKind,
    secondary: Option<WeatherObservationReportKind>,
    tertiary: Option<WeatherObservationReportKind>,
) -> BTreeMap<chrono::NaiveDate, TemperatureCelsius> {
    let accepted = |kind| kind == primary || secondary == Some(kind) || tertiary == Some(kind);
    let mut latest = BTreeMap::<DateTime<Utc>, &WeatherObservationFact>::new();
    for fact in facts.iter().filter(|fact| accepted(fact.report_kind)) {
        latest
            .entry(fact.observation_time)
            .and_modify(|current| {
                if (fact.revision, fact.available_at, fact.report_hash.as_str())
                    > (
                        current.revision,
                        current.available_at,
                        current.report_hash.as_str(),
                    )
                {
                    *current = fact;
                }
            })
            .or_insert(fact);
    }
    let mut highs = BTreeMap::new();
    for fact in latest.into_values() {
        highs
            .entry(fact.local_date)
            .and_modify(|current: &mut TemperatureCelsius| {
                *current = (*current).max(fact.temperature);
            })
            .or_insert(fact.temperature);
    }
    highs
}

fn latest_observation_evidence(
    facts: &[WeatherObservationFact],
    local_date: chrono::NaiveDate,
    excluded: WeatherObservationReportKind,
    include_excluded: bool,
) -> Option<EvidenceSourceRef> {
    facts
        .iter()
        .filter(|fact| {
            fact.local_date == local_date && (include_excluded || fact.report_kind != excluded)
        })
        .max_by(|left, right| {
            (
                left.observation_time,
                left.revision,
                left.report_hash.as_str(),
            )
                .cmp(&(
                    right.observation_time,
                    right.revision,
                    right.report_hash.as_str(),
                ))
        })
        .map(|fact| EvidenceSourceRef {
            source_kind: EvidenceSourceKind::DomainExternal,
            reference: format!(
                "{}:{}@{}#{}",
                fact.source_id,
                fact.station,
                fact.observation_time.timestamp_millis(),
                fact.report_hash,
            ),
            effective_at: fact.published_at,
            available_at: Some(fact.available_at),
        })
}

fn headroom(subject: &WeatherSubject, high: TemperatureCelsius) -> Decimal {
    let high = high.whole_degrees(subject.market_unit);
    match (
        subject.outcome_band.lower_inclusive,
        subject.outcome_band.upper_inclusive,
    ) {
        (_, Some(upper)) => upper - high,
        (Some(lower), None) => high - lower,
        (None, None) => Decimal::ZERO,
    }
}

fn noaa_basis_risk(
    subject: &WeatherSubject,
    window: &WeatherFactWindow,
    config: &WeatherDomainConfig,
) -> RawFeature {
    let live = daily_highs(
        &window.observations,
        WeatherObservationReportKind::Metar,
        Some(WeatherObservationReportKind::Speci),
        Some(WeatherObservationReportKind::Correction),
    );
    let historical = daily_highs(
        &window.observations,
        WeatherObservationReportKind::HistoricalGhcnh,
        None,
        None,
    );
    let differences = live
        .iter()
        .filter_map(|(date, live_high)| {
            historical.get(date).map(|historical_high| {
                (live_high.whole_degrees(subject.market_unit)
                    - historical_high.whole_degrees(subject.market_unit))
                .abs()
            })
        })
        .collect::<Vec<_>>();
    if differences.len()
        < usize::try_from(config.minimum_bias_samples_per_lead).unwrap_or(usize::MAX)
    {
        return RawFeature::missing(
            names::NOAA_RESOLUTION_BASIS_RISK,
            NullReason::InsufficientHistory,
        );
    }
    let overlap_dates = live
        .keys()
        .filter(|date| historical.contains_key(date))
        .copied()
        .collect::<BTreeSet<_>>();
    let mut evidence_facts = window
        .observations
        .iter()
        .filter(|fact| overlap_dates.contains(&fact.local_date))
        .filter(|fact| {
            matches!(
                fact.report_kind,
                WeatherObservationReportKind::Metar
                    | WeatherObservationReportKind::Speci
                    | WeatherObservationReportKind::Correction
                    | WeatherObservationReportKind::HistoricalGhcnh
            )
        })
        .collect::<Vec<_>>();
    evidence_facts.sort_by(|left, right| {
        (
            left.local_date,
            left.observation_time,
            left.report_hash.as_str(),
        )
            .cmp(&(
                right.local_date,
                right.observation_time,
                right.report_hash.as_str(),
            ))
    });
    let effective_at = evidence_facts
        .iter()
        .map(|fact| fact.published_at)
        .max()
        .unwrap_or(window.decision_at);
    let available_at = evidence_facts
        .iter()
        .map(|fact| fact.available_at)
        .max()
        .unwrap_or(window.decision_at);
    let evidence_reports = evidence_facts
        .iter()
        .map(|fact| (&fact.report_hash, fact.revision))
        .collect::<Vec<_>>();
    let risk = differences.iter().copied().sum::<Decimal>() / Decimal::from(differences.len());
    let Ok(evidence_hash) = CanonicalDigest::content_hash_json(&(
        "weather_noaa_cross_product_basis_v1",
        subject.station.as_str(),
        &differences,
        evidence_reports,
    )) else {
        return RawFeature::missing(
            names::NOAA_RESOLUTION_BASIS_RISK,
            NullReason::OutOfValidRange,
        );
    };
    RawFeature::present(
        names::NOAA_RESOLUTION_BASIS_RISK,
        FeatureValue::Decimal(risk),
        EvidenceSourceRef {
            source_kind: EvidenceSourceKind::DomainExternal,
            reference: format!("weather-basis:{}#{evidence_hash}", subject.station),
            effective_at,
            available_at: Some(available_at),
        },
    )
}

fn ensemble_standard_deviation(values: &[TemperatureCelsius]) -> Decimal {
    if values.is_empty() {
        return Decimal::ZERO;
    }
    let mean =
        values.iter().map(|value| value.value()).sum::<Decimal>() / Decimal::from(values.len());
    let variance = values
        .iter()
        .map(|value| {
            let delta = value.value() - mean;
            delta * delta
        })
        .sum::<Decimal>()
        / Decimal::from(values.len());
    decimal_sqrt(variance)
}

fn decimal_sqrt(value: Decimal) -> Decimal {
    if value <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let two = Decimal::from(2);
    let mut estimate = value.max(Decimal::ONE);
    for _ in 0..SQRT_ITERATIONS {
        estimate = (estimate + value / estimate) / two;
    }
    estimate
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        domain::{WeatherForecastPoint, WeatherObservationFact, WeatherObservationReportKind},
        types::{ContentHash, DomainSourceId, IcaoStation, TemperatureCelsius},
    };
    use rust_decimal_macros::dec;

    use super::{decimal_sqrt, ensemble_standard_deviation, fit_weather_station_lead_bias};

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    #[test]
    fn decimal_spread_is_deterministic() {
        let spread = ensemble_standard_deviation(&[
            TemperatureCelsius::new(dec!(10)),
            TemperatureCelsius::new(dec!(12)),
        ]);
        assert_eq!(spread.round_dp(8), dec!(1));
        assert_eq!(decimal_sqrt(dec!(4)).round_dp(8), dec!(2));
    }

    #[test]
    fn station_lead_fit_uses_latest_observation_revision_and_complete_ensemble() {
        let station = IcaoStation::parse("KJFK").expect("station");
        let reference = Utc
            .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
            .single()
            .expect("reference");
        let valid = reference + Duration::hours(3);
        let observation = |revision, temperature, report_hash| WeatherObservationFact {
            source_id: DomainSourceId::ghcnh(),
            station: station.clone(),
            local_date: valid.date_naive(),
            report_kind: WeatherObservationReportKind::HistoricalGhcnh,
            temperature: TemperatureCelsius::new(temperature),
            precision_celsius: dec!(0.1),
            observation_time: valid,
            published_at: valid,
            available_at: valid + Duration::days(1),
            revision,
            report_hash,
            supersedes_report_hash: None,
        };
        let observations = vec![
            observation(0, dec!(10), hash('1')),
            observation(1, dec!(11), hash('2')),
        ];
        let forecast = |member, temperature| WeatherForecastPoint {
            source_id: DomainSourceId::gefs(),
            station: station.clone(),
            reference_time: reference,
            valid_time: valid,
            available_at: reference + Duration::hours(5),
            lead_hours: 3,
            member,
            tmax_celsius: TemperatureCelsius::new(temperature),
            grid_binding_hash: hash('3'),
            run_manifest_hash: hash('4'),
        };
        let fit = fit_weather_station_lead_bias(
            &observations,
            &[forecast(0, dec!(12)), forecast(1, dec!(14))],
            reference,
            reference + Duration::days(1),
            2,
        )
        .expect("fit");

        assert_eq!(fit.sample_count, 1);
        assert_eq!(fit.payload.stations.len(), 1);
        assert_eq!(fit.payload.stations[0].station, station);
        assert_eq!(fit.payload.stations[0].leads[0].lead_hours, 3);
        assert_eq!(fit.payload.stations[0].leads[0].sample_count, 1);
        assert_eq!(fit.payload.stations[0].leads[0].bias_celsius, dec!(2));
        assert_eq!(fit.payload.grid_hashes, vec![hash('3')]);
    }
}
