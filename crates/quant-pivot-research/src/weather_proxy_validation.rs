//! Sealed AWC-versus-GHCNh settlement-proxy validation for Weather policies.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::{
        LinkageOutcome, MarketLinkage, MarketSubject, WeatherObservationFact,
        WeatherObservationReportKind,
    },
    hashing::CanonicalDigest,
    types::{
        IcaoStation, TemperatureCelsius, TemperatureUnit, VerticalActivationTarget,
        VerticalGateEvidence, VerticalGateKind, WeatherTemperatureStatistic,
    },
};
use rust_decimal::{Decimal, MathematicalOps};

/// Semantic identity of the sealed Weather proxy methodology.
pub const WEATHER_PROXY_VALIDATION_VERSION: &str = "awc_ghcnh_daily_temperature_proxy_v1";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProxySubject {
    station: IcaoStation,
    statistic: WeatherTemperatureStatistic,
    unit: TemperatureUnit,
}

/// Produce the fail-closed Weather activation gate from one verified Source
/// Slice. Inputs must already be bounded by the artifact PIT cutoff.
pub fn evaluate_weather_proxy_gate(
    linkages: &[MarketLinkage],
    observations: &[WeatherObservationFact],
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    target: VerticalActivationTarget,
) -> QuantResult<VerticalGateEvidence> {
    if window_start >= window_end {
        return Err(methodology(
            "Weather proxy gate requires a non-empty activation evidence window".to_owned(),
        ));
    }
    let subjects = proxy_subjects(linkages);
    if subjects.is_empty() {
        return Err(methodology(
            "Weather proxy gate has no resolved Weather station subjects".to_owned(),
        ));
    }
    let relevant = observations
        .iter()
        .filter(|fact| {
            fact.station()
                .is_some_and(|station| subjects.iter().any(|subject| subject.station == station))
                && fact.observed_at >= window_start
                && fact.observed_at < window_end
                && fact.available_at <= window_end
                && fact.temperature_celsius().is_some()
        })
        .collect::<Vec<_>>();
    let report_hashes = relevant
        .iter()
        .map(|fact| &fact.report_hash)
        .collect::<BTreeSet<_>>();
    let gaps_recovered = relevant.iter().all(|fact| {
        fact.supersedes_report_hash
            .as_ref()
            .is_none_or(|hash| report_hashes.contains(hash))
    });
    let live = daily_extremes(&relevant, false);
    let historical = daily_extremes(&relevant, true);
    let mut total_reference_days = 0_u64;
    let mut total_paired_days = 0_u64;
    let mut total_agreements = 0_u64;
    let mut paired_dates = BTreeSet::new();
    let mut per_subject = BTreeMap::<ProxySubject, (u64, u64)>::new();
    for subject in &subjects {
        for ((station, local_date, statistic), historical_extreme) in &historical {
            if station != &subject.station || statistic != &subject.statistic {
                continue;
            }
            total_reference_days = total_reference_days.checked_add(1).ok_or_else(|| {
                methodology("Weather proxy reference-day count overflow".to_owned())
            })?;
            let Some(live_extreme) = live.get(&(station.clone(), *local_date, *statistic)) else {
                continue;
            };
            total_paired_days = total_paired_days
                .checked_add(1)
                .ok_or_else(|| methodology("Weather proxy paired-day count overflow".to_owned()))?;
            paired_dates.insert(*local_date);
            let entry = per_subject.entry(subject.clone()).or_default();
            entry.1 = entry.1.checked_add(1).ok_or_else(|| {
                methodology("Weather proxy subject sample count overflow".to_owned())
            })?;
            if live_extreme.whole_degrees(subject.unit)
                == historical_extreme.whole_degrees(subject.unit)
            {
                total_agreements = total_agreements.checked_add(1).ok_or_else(|| {
                    methodology("Weather proxy agreement count overflow".to_owned())
                })?;
                entry.0 = entry.0.checked_add(1).ok_or_else(|| {
                    methodology("Weather proxy subject agreement count overflow".to_owned())
                })?;
            }
        }
    }
    let subject_results = subjects
        .iter()
        .map(|subject| {
            let (agreements, samples) = per_subject.get(subject).copied().unwrap_or_default();
            Ok((samples, wilson_lower_bound(agreements, samples)?))
        })
        .collect::<QuantResult<Vec<_>>>()?;
    // These are independent worst-case guarantees. Selecting one
    // station/statistic/unit subject by sample count and reusing its bound
    // would let a different low-agreement contract family escape the gate.
    let target_subject_sample_count = subject_results.iter().map(|row| row.0).min();
    let target_subject_wilson_lower_bound = subject_results.iter().map(|row| row.1).min();
    let methodology_hash = CanonicalDigest::content_hash_json(&(
        WEATHER_PROXY_VALIDATION_VERSION,
        "AWC_METAR_SPECI_COR_latest_revision_daily_max_and_min",
        "GHCNh_latest_revision_daily_max_and_min",
        "whole_degree_midpoint_away_from_zero",
        "two_sided_wilson_95_lower_bound",
    ))?;
    Ok(VerticalGateEvidence {
        gate: VerticalGateKind::WeatherNoaaProxy,
        target,
        methodology_hash,
        evidence_window_start: window_start,
        evidence_window_end: window_end,
        sample_count: total_paired_days,
        distinct_subject_count: u32::try_from(subjects.len()).map_err(|error| {
            methodology(format!(
                "Weather proxy subject count does not fit u32: {error}"
            ))
        })?,
        distinct_local_dates: u32::try_from(paired_dates.len()).map_err(|error| {
            methodology(format!(
                "Weather proxy local-date count does not fit u32: {error}"
            ))
        })?,
        availability: ratio(total_paired_days, total_reference_days),
        agreement_wilson_lower_bound: wilson_lower_bound(total_agreements, total_paired_days)?,
        target_subject_sample_count,
        target_subject_wilson_lower_bound,
        unresolved_mismatch_count: 0,
        gaps_recovered,
    })
}

fn proxy_subjects(linkages: &[MarketLinkage]) -> BTreeSet<ProxySubject> {
    let mut subjects = BTreeSet::new();
    for linkage in linkages {
        let LinkageOutcome::Resolved(binding) = &linkage.outcome else {
            continue;
        };
        let MarketSubject::Weather(subject) = &binding.subject else {
            continue;
        };
        subjects.insert(ProxySubject {
            station: subject.decision_group.station.clone(),
            statistic: subject.decision_group.temperature_statistic,
            unit: subject.decision_group.market_unit,
        });
    }
    subjects
}

fn daily_extremes(
    facts: &[&WeatherObservationFact],
    historical: bool,
) -> BTreeMap<(IcaoStation, NaiveDate, WeatherTemperatureStatistic), TemperatureCelsius> {
    let accepted = |kind| {
        if historical {
            kind == WeatherObservationReportKind::HistoricalGhcnh
        } else {
            matches!(
                kind,
                WeatherObservationReportKind::Metar
                    | WeatherObservationReportKind::Speci
                    | WeatherObservationReportKind::Correction
            )
        }
    };
    let mut latest = BTreeMap::<(IcaoStation, DateTime<Utc>), &WeatherObservationFact>::new();
    for fact in facts
        .iter()
        .copied()
        .filter(|fact| accepted(fact.report_kind))
    {
        let Some(station) = fact.station() else {
            continue;
        };
        latest
            .entry((station, fact.observed_at))
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
    let mut extremes = BTreeMap::new();
    for fact in latest.into_values() {
        let (Some(station), Some(temperature)) = (fact.station(), fact.temperature_celsius())
        else {
            continue;
        };
        for statistic in [
            WeatherTemperatureStatistic::Maximum,
            WeatherTemperatureStatistic::Minimum,
        ] {
            extremes
                .entry((station.clone(), fact.local_date, statistic))
                .and_modify(|current: &mut TemperatureCelsius| {
                    *current = match statistic {
                        WeatherTemperatureStatistic::Maximum => (*current).max(temperature),
                        WeatherTemperatureStatistic::Minimum => (*current).min(temperature),
                    };
                })
                .or_insert(temperature);
        }
    }
    extremes
}

fn ratio(numerator: u64, denominator: u64) -> Decimal {
    if denominator == 0 {
        Decimal::ZERO
    } else {
        Decimal::from(numerator) / Decimal::from(denominator)
    }
}

fn wilson_lower_bound(successes: u64, samples: u64) -> QuantResult<Decimal> {
    if samples == 0 {
        return Ok(Decimal::ZERO);
    }
    if successes > samples {
        return Err(methodology(
            "Weather proxy agreement count exceeds sample count".to_owned(),
        ));
    }
    let n = Decimal::from(samples);
    let p = Decimal::from(successes) / n;
    let z = Decimal::new(1_959_963_984_540_054, 15);
    let z_squared = z * z;
    let denominator = Decimal::ONE + z_squared / n;
    let center = p + z_squared / (Decimal::TWO * n);
    let variance = (p * (Decimal::ONE - p) + z_squared / (Decimal::from(4) * n)) / n;
    let margin = z * variance
        .sqrt()
        .ok_or_else(|| methodology("Weather proxy Wilson variance is invalid".to_owned()))?;
    Ok(((center - margin) / denominator).clamp(Decimal::ZERO, Decimal::ONE))
}

fn methodology(detail: String) -> QuantError {
    ResearchError::ValidationMethodology { detail }.into()
}

#[cfg(test)]
mod tests {
    use super::wilson_lower_bound;
    use rust_decimal::Decimal;

    #[test]
    fn wilson_bound_is_fail_closed_for_empty_samples() {
        assert_eq!(wilson_lower_bound(0, 0).expect("bound"), Decimal::ZERO);
        assert!(wilson_lower_bound(2, 1).is_err());
    }
}
