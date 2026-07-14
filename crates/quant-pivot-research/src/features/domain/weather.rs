//! Airport local-day maximum-temperature features.
//!
//! Forecast features are emitted only from a complete 31-member GEFS run after
//! every target lead has a station-specific bias estimate backed by distinct
//! `GHCNh` observations. Missing calibration is an explicit missing feature,
//! never an assumed zero correction.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use quant_pivot_models::{
    domain::{
        MarketSubject, WeatherForecastPoint, WeatherObservationFact, WeatherObservationReportKind,
        WeatherSubject,
    },
    enums::{domain::DomainFamily, feature::EvidenceSourceKind},
    hashing::CanonicalDigest,
    runtime_config::WeatherDomainConfig,
    types::{ContentHash, Probability, TemperatureCelsius},
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

const FORECAST_INTERVAL_HOURS: i64 = 3;
const SQRT_ITERATIONS: usize = 32;

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
    let biases = fit_lead_biases(window, config, target.reference_time)?;
    let mut highs = BTreeMap::<u8, Decimal>::new();
    for point in &target.points {
        let Some((sample_count, bias)) = biases.get(&point.lead_hours) else {
            return Err(NullReason::InsufficientHistory);
        };
        if *sample_count < config.minimum_bias_samples_per_lead {
            return Err(NullReason::InsufficientHistory);
        }
        let corrected = point.tmax_celsius.value() - *bias;
        highs
            .entry(point.member)
            .and_modify(|current| *current = (*current).max(corrected))
            .or_insert(corrected);
    }
    if highs.len() != usize::from(config.minimum_complete_members) {
        return Err(NullReason::DomainSourceUnavailable);
    }
    let calibration_hash =
        CanonicalDigest::content_hash_json(&biases).map_err(|_| NullReason::OutOfValidRange)?;
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
                "gefs:{}#{}:bias={calibration_hash}",
                target.reference_time.timestamp_millis(),
                target.run_manifest_hash,
            ),
            effective_at,
            available_at: Some(target.available_at),
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

/// Fit one mean `forecast TMAX - observed interval max` bias per exact lead.
/// A sample is one `(run, valid_time, lead)` ensemble, not 31 pseudo-samples.
fn fit_lead_biases(
    window: &WeatherFactWindow,
    config: &WeatherDomainConfig,
    before: DateTime<Utc>,
) -> Result<BTreeMap<u16, (u32, Decimal)>, NullReason> {
    let lookback = i64::from(config.calibration_lookback_days);
    let from = window.decision_at - Duration::days(lookback);
    let ghcnh = latest_observations(
        &window.observations,
        WeatherObservationReportKind::HistoricalGhcnh,
    );
    let mut groups = BTreeMap::<
        (DateTime<Utc>, DateTime<Utc>, u16, &ContentHash),
        Vec<&WeatherForecastPoint>,
    >::new();
    for point in &window.forecasts {
        if point.valid_time >= before || point.valid_time < from {
            continue;
        }
        groups
            .entry((
                point.reference_time,
                point.valid_time,
                point.lead_hours,
                &point.run_manifest_hash,
            ))
            .or_default()
            .push(point);
    }
    let mut errors = BTreeMap::<u16, Vec<Decimal>>::new();
    for ((_, valid_time, lead_hours, _), points) in groups {
        if !has_complete_members(&points, config.minimum_complete_members) {
            continue;
        }
        let interval_start = valid_time - Duration::hours(FORECAST_INTERVAL_HOURS);
        let observed = ghcnh
            .iter()
            .filter(|fact| {
                fact.observation_time > interval_start && fact.observation_time <= valid_time
            })
            .map(|fact| fact.temperature.value())
            .max();
        let Some(observed) = observed else {
            continue;
        };
        let forecast = points
            .iter()
            .map(|point| point.tmax_celsius.value())
            .sum::<Decimal>()
            / Decimal::from(points.len());
        errors
            .entry(lead_hours)
            .or_default()
            .push(forecast - observed);
    }
    let biases = errors
        .into_iter()
        .map(|(lead, values)| {
            let count = u32::try_from(values.len()).map_err(|_| NullReason::OutOfValidRange)?;
            let mean = values.iter().copied().sum::<Decimal>() / Decimal::from(values.len());
            Ok((lead, (count, mean)))
        })
        .collect::<Result<BTreeMap<_, _>, NullReason>>()?;
    Ok(biases)
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

fn latest_observations(
    facts: &[WeatherObservationFact],
    kind: WeatherObservationReportKind,
) -> Vec<&WeatherObservationFact> {
    let mut latest = BTreeMap::<DateTime<Utc>, &WeatherObservationFact>::new();
    for fact in facts.iter().filter(|fact| fact.report_kind == kind) {
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
    latest.into_values().collect()
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
    use quant_pivot_models::types::TemperatureCelsius;
    use rust_decimal_macros::dec;

    use super::{decimal_sqrt, ensemble_standard_deviation};

    #[test]
    fn decimal_spread_is_deterministic() {
        let spread = ensemble_standard_deviation(&[
            TemperatureCelsius::new(dec!(10)),
            TemperatureCelsius::new(dec!(12)),
        ]);
        assert_eq!(spread.round_dp(8), dec!(1));
        assert_eq!(decimal_sqrt(dec!(4)).round_dp(8), dec!(2));
    }
}
