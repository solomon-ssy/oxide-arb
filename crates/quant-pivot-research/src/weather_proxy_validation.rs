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
        VerticalGateEvidence, VerticalGateKind,
    },
};
use rust_decimal::{Decimal, MathematicalOps};

/// Semantic identity of the sealed Weather proxy methodology.
pub const WEATHER_PROXY_VALIDATION_VERSION: &str = "awc_ghcnh_daily_high_proxy_v1";

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
    let station_units = station_units(linkages)?;
    if station_units.is_empty() {
        return Err(methodology(
            "Weather proxy gate has no resolved Weather station subjects".to_owned(),
        ));
    }
    let relevant = observations
        .iter()
        .filter(|fact| {
            station_units.contains_key(&fact.station)
                && fact.observation_time >= window_start
                && fact.observation_time < window_end
                && fact.available_at <= window_end
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
    let live = daily_highs(&relevant, false);
    let historical = daily_highs(&relevant, true);
    let mut total_reference_days = 0_u64;
    let mut total_paired_days = 0_u64;
    let mut total_agreements = 0_u64;
    let mut paired_dates = BTreeSet::new();
    let mut per_station = BTreeMap::<IcaoStation, (u64, u64)>::new();
    for ((station, local_date), historical_high) in &historical {
        total_reference_days = total_reference_days
            .checked_add(1)
            .ok_or_else(|| methodology("Weather proxy reference-day count overflow".to_owned()))?;
        let Some(live_high) = live.get(&(station.clone(), *local_date)) else {
            continue;
        };
        let unit = station_units.get(station).ok_or_else(|| {
            methodology(format!(
                "Weather proxy station {station} has no frozen market unit"
            ))
        })?;
        total_paired_days = total_paired_days
            .checked_add(1)
            .ok_or_else(|| methodology("Weather proxy paired-day count overflow".to_owned()))?;
        paired_dates.insert(*local_date);
        let entry = per_station.entry(station.clone()).or_default();
        entry.1 = entry
            .1
            .checked_add(1)
            .ok_or_else(|| methodology("Weather proxy station sample count overflow".to_owned()))?;
        if live_high.whole_degrees(*unit) == historical_high.whole_degrees(*unit) {
            total_agreements = total_agreements
                .checked_add(1)
                .ok_or_else(|| methodology("Weather proxy agreement count overflow".to_owned()))?;
            entry.0 = entry.0.checked_add(1).ok_or_else(|| {
                methodology("Weather proxy station agreement count overflow".to_owned())
            })?;
        }
    }
    let station_results = station_units
        .keys()
        .map(|station| {
            let (agreements, samples) = per_station.get(station).copied().unwrap_or_default();
            Ok((samples, wilson_lower_bound(agreements, samples)?))
        })
        .collect::<QuantResult<Vec<_>>>()?;
    // These are independent worst-case guarantees. Selecting one station by
    // sample count and reusing its bound would let a different low-agreement
    // station escape the per-subject publication gate.
    let target_subject_sample_count = station_results.iter().map(|row| row.0).min();
    let target_subject_wilson_lower_bound = station_results.iter().map(|row| row.1).min();
    let methodology_hash = CanonicalDigest::content_hash_json(&(
        WEATHER_PROXY_VALIDATION_VERSION,
        "AWC_METAR_SPECI_COR_latest_revision_daily_max",
        "GHCNh_latest_revision_daily_max",
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
        distinct_subject_count: u32::try_from(per_station.len()).map_err(|error| {
            methodology(format!(
                "Weather proxy station count does not fit u32: {error}"
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

fn station_units(
    linkages: &[MarketLinkage],
) -> QuantResult<BTreeMap<IcaoStation, TemperatureUnit>> {
    let mut units = BTreeMap::new();
    for linkage in linkages {
        let LinkageOutcome::Resolved(binding) = &linkage.outcome else {
            continue;
        };
        let MarketSubject::Weather(subject) = &binding.subject else {
            continue;
        };
        if units
            .insert(subject.station.clone(), subject.market_unit)
            .is_some_and(|prior| prior != subject.market_unit)
        {
            return Err(methodology(format!(
                "Weather proxy station {} is linked with conflicting market units",
                subject.station
            )));
        }
    }
    Ok(units)
}

fn daily_highs(
    facts: &[&WeatherObservationFact],
    historical: bool,
) -> BTreeMap<(IcaoStation, NaiveDate), TemperatureCelsius> {
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
    let mut highs = BTreeMap::new();
    for fact in latest.into_values() {
        highs
            .entry((fact.station.clone(), fact.local_date))
            .and_modify(|current: &mut TemperatureCelsius| {
                *current = (*current).max(fact.temperature);
            })
            .or_insert(fact.temperature);
    }
    highs
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
