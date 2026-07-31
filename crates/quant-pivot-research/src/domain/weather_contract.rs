//! Point-in-time Weather contract projection and final-label semantics.
//!
//! One projector owns both feature-time observations and final-label truth.
//! The caller chooses the purpose; the source precedence, unit, aggregation,
//! revision, rounding, and comparator rules remain identical. A preliminary
//! source can therefore never leak into a final label through a second code
//! path.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
};

use chrono::{DateTime, Datelike, Duration, Utc};
use quant_pivot_models::{
    domain::{
        data_plane::{WeatherObservationFact, WeatherObservationReportKind},
        quant::{
            GlobalTemperatureOutcome, GlobalTemperatureRank, MarketSubject, ResolvedBinding,
            SeaIceAggregation, SeaIceProduct, TropicalCycloneOutcome, WeatherAqiSubject,
            WeatherGlobalTemperatureSubject, WeatherPrecipitationSubject, WeatherRoundingRule,
            WeatherSeaIceSubject, WeatherSubject, WeatherTornadoFinalization,
            WeatherTornadoSubject, WeatherTropicalCycloneSubject, WeatherTruthPolicy,
            WeatherValueComparator, WeatherWindExtremeSubject, WeatherWindStatistic,
        },
    },
    enums::{
        domain::{DomainFamily, LinkageSourceRole},
        feature::EvidenceSourceKind,
    },
    types::{
        ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, EvidenceSourceRef,
        TemperatureBand, TemperatureCelsius, TemperatureUnit, WeatherTemperatureStatistic,
        WeatherVariable,
    },
};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};

use crate::domain::{
    WeatherFactWindow, valid_weather_sources, weather_contract_bounds,
    weather_observation_in_window,
};

const WIND_MAX_OBSERVATION_GAP_HOURS: i64 = 2;

/// Whether a projection may use realtime evidence or requires final truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WeatherProjectionPurpose {
    Feature,
    FinalLabel,
}

/// Maturity of the source selected by the precedence policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WeatherTruthMaturity {
    Preliminary,
    Final,
}

/// Unit in which the market comparator is evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WeatherComparisonUnit {
    Celsius,
    Fahrenheit,
    Millimeter,
    Aqi,
    Count,
    Knot,
    CelsiusAnomaly,
    Rank,
    MillionSquareKilometer,
}

/// Immutable projection of official Weather facts onto one market contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WeatherContractProjection {
    pub subject_key: String,
    pub canonical_value: Decimal,
    pub comparison_value: Decimal,
    pub comparison_unit: WeatherComparisonUnit,
    pub outcome: bool,
    /// Signed distance from the nearest decision boundary. Positive means the
    /// projected value is inside the YES region; negative means outside.
    pub boundary_distance: Decimal,
    pub maturity: WeatherTruthMaturity,
    pub source_role: LinkageSourceRole,
    pub source_id: DomainSourceId,
    pub instrument_key: DomainInstrumentKey,
    pub effective_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub report_hashes: Vec<ContentHash>,
}

impl WeatherContractProjection {
    /// Feature evidence that binds every contributing immutable fact hash.
    #[must_use]
    pub fn evidence(&self) -> EvidenceSourceRef {
        let hashes = self
            .report_hashes
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        EvidenceSourceRef {
            source_kind: EvidenceSourceKind::DomainWeather,
            reference: format!(
                "weather-contract:{}:{}:[{}]",
                self.subject_key, self.source_id, hashes
            ),
            effective_at: self.effective_at,
            available_at: Some(self.available_at),
        }
    }
}

/// Closed, machine-actionable reason a Weather contract could not project.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "code", rename_all = "snake_case")]
pub enum WeatherProjectionFailure {
    NonWeatherSubject,
    InvalidSourceBinding,
    SubjectKeyMismatch,
    WindowOpen {
        end_at: DateTime<Utc>,
        decision_at: DateTime<Utc>,
    },
    EvidenceAfterDecision {
        available_at: DateTime<Utc>,
        decision_at: DateTime<Utc>,
    },
    SourceUnavailable {
        role: LinkageSourceRole,
        source_id: DomainSourceId,
    },
    AmbiguousRevision {
        instrument_key: DomainInstrumentKey,
        observed_at: DateTime<Utc>,
        revision: u32,
    },
    UnitMismatch {
        expected: DomainMeasurementUnit,
        actual: DomainMeasurementUnit,
    },
    UnsupportedOfficialProduct,
    InsufficientCoverage,
    AggregationOverflow,
}

impl Display for WeatherProjectionFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::NonWeatherSubject => formatter.write_str("subject is not a Weather contract"),
            Self::InvalidSourceBinding => {
                formatter.write_str("Weather source binding is incomplete or inconsistent")
            }
            Self::SubjectKeyMismatch => {
                formatter.write_str("Weather fact subject key does not match the contract")
            }
            Self::WindowOpen {
                end_at,
                decision_at,
            } => write!(
                formatter,
                "Weather contract window ends at {end_at}, after decision {decision_at}"
            ),
            Self::EvidenceAfterDecision {
                available_at,
                decision_at,
            } => write!(
                formatter,
                "Weather evidence became available at {available_at}, after decision {decision_at}"
            ),
            Self::SourceUnavailable { role, source_id } => write!(
                formatter,
                "Weather source {source_id} for role {role:?} has no applicable fact"
            ),
            Self::AmbiguousRevision {
                instrument_key,
                observed_at,
                revision,
            } => write!(
                formatter,
                "Weather fact {instrument_key} at {observed_at} has divergent revision {revision}"
            ),
            Self::UnitMismatch { expected, actual } => write!(
                formatter,
                "Weather fact unit {actual:?} does not match expected {expected:?}"
            ),
            Self::UnsupportedOfficialProduct => {
                formatter.write_str("the official Weather product is not implemented")
            }
            Self::InsufficientCoverage => {
                formatter.write_str("Weather facts do not cover the complete contract window")
            }
            Self::AggregationOverflow => {
                formatter.write_str("Weather fact aggregation exceeds decimal capacity")
            }
        }
    }
}

impl Error for WeatherProjectionFailure {}

#[derive(Clone)]
struct SourceSpec {
    role: LinkageSourceRole,
    source_id: DomainSourceId,
    variable: WeatherVariable,
    unit: DomainMeasurementUnit,
    report_kind: WeatherObservationReportKind,
}

impl SourceSpec {
    fn unavailable(&self) -> WeatherProjectionFailure {
        WeatherProjectionFailure::SourceUnavailable {
            role: self.role,
            source_id: self.source_id.clone(),
        }
    }
}

struct AggregateSelection<'a> {
    facts: Vec<&'a WeatherObservationFact>,
    source_role: LinkageSourceRole,
    source_id: DomainSourceId,
    instrument_key: DomainInstrumentKey,
    value: Decimal,
    maturity: WeatherTruthMaturity,
}

/// Project PIT-visible facts onto one Weather market contract.
///
/// `FinalLabel` fails closed until the contract window is complete and the
/// family-specific final source is visible. `Feature` uses the final source
/// when already available and otherwise follows the explicit preliminary
/// source policy.
pub fn project_weather_contract(
    binding: &ResolvedBinding,
    window: &WeatherFactWindow,
    purpose: WeatherProjectionPurpose,
) -> Result<WeatherContractProjection, WeatherProjectionFailure> {
    if binding.subject.family() != DomainFamily::Weather {
        return Err(WeatherProjectionFailure::NonWeatherSubject);
    }
    if !valid_weather_sources(binding, &binding.subject) {
        return Err(WeatherProjectionFailure::InvalidSourceBinding);
    }
    let subject_key = binding
        .subject
        .weather_subject_key()
        .ok_or(WeatherProjectionFailure::NonWeatherSubject)?;
    let (from, to) = weather_contract_bounds(&binding.subject)
        .map_err(|_| WeatherProjectionFailure::InvalidSourceBinding)?;
    if purpose == WeatherProjectionPurpose::FinalLabel && window.decision_at < to {
        return Err(WeatherProjectionFailure::WindowOpen {
            end_at: to,
            decision_at: window.decision_at,
        });
    }
    if window
        .observations
        .iter()
        .any(|fact| fact.subject_key != subject_key)
        || window
            .forecasts
            .iter()
            .any(|fact| fact.subject_key != subject_key)
    {
        return Err(WeatherProjectionFailure::SubjectKeyMismatch);
    }
    if let Some(available_at) = window
        .observations
        .iter()
        .map(|fact| fact.available_at)
        .chain(window.forecasts.iter().map(|fact| fact.available_at))
        .filter(|available_at| *available_at > window.decision_at)
        .min()
    {
        return Err(WeatherProjectionFailure::EvidenceAfterDecision {
            available_at,
            decision_at: window.decision_at,
        });
    }
    let facts = canonical_weather_facts(&window.observations)?;
    let revision_history = window.observations.iter().collect::<Vec<_>>();
    let selection = match &binding.subject {
        MarketSubject::Weather(subject) => {
            select_temperature(binding, subject, &facts, from, to, purpose)?
        }
        MarketSubject::WeatherPrecipitation(subject) => {
            select_precipitation(binding, subject, &facts, from, to)?
        }
        MarketSubject::WeatherAqi(subject) => {
            select_aqi(binding, subject, &facts, from, to, purpose)?
        }
        MarketSubject::WeatherTornado(subject) => {
            let tornado_facts = match &subject.finalization {
                WeatherTornadoFinalization::StormEventsArchive => &facts,
                WeatherTornadoFinalization::FirstPublishedAfter { .. } => &revision_history,
            };
            select_tornado(binding, subject, tornado_facts, from, to, purpose)?
        }
        MarketSubject::WeatherTropicalCyclone(subject) => {
            select_cyclone(binding, subject, &facts, from, to, purpose)?
        }
        MarketSubject::WeatherGlobalTemperature(subject) => {
            return project_global(
                binding,
                subject,
                &revision_history,
                &subject_key,
                from,
                to,
                purpose,
            );
        }
        MarketSubject::WeatherSeaIce(subject) => {
            select_sea_ice(binding, subject, &facts, from, to, purpose)?
        }
        MarketSubject::WeatherWindExtreme(subject) => {
            select_wind(binding, subject, &facts, from, to, purpose)?
        }
        MarketSubject::Crypto(_) => return Err(WeatherProjectionFailure::NonWeatherSubject),
    };
    projection_from_selection(binding, selection, subject_key)
}

fn canonical_weather_facts(
    facts: &[WeatherObservationFact],
) -> Result<Vec<&WeatherObservationFact>, WeatherProjectionFailure> {
    let mut revisions = BTreeMap::<
        (
            DomainSourceId,
            DomainInstrumentKey,
            WeatherVariable,
            WeatherObservationReportKind,
            DateTime<Utc>,
            u32,
        ),
        ContentHash,
    >::new();
    for fact in facts {
        let key = (
            fact.source_id.clone(),
            fact.instrument_key.clone(),
            fact.variable,
            fact.report_kind,
            fact.observed_at,
            fact.revision,
        );
        if let Some(report_hash) = revisions.insert(key, fact.report_hash)
            && report_hash != fact.report_hash
        {
            return Err(WeatherProjectionFailure::AmbiguousRevision {
                instrument_key: fact.instrument_key.clone(),
                observed_at: fact.observed_at,
                revision: fact.revision,
            });
        }
    }
    let superseded = facts
        .iter()
        .filter_map(|fact| fact.supersedes_report_hash)
        .collect::<BTreeSet<_>>();
    let mut canonical = BTreeMap::<
        (
            DomainSourceId,
            DomainInstrumentKey,
            WeatherVariable,
            WeatherObservationReportKind,
            DateTime<Utc>,
        ),
        &WeatherObservationFact,
    >::new();
    for fact in facts
        .iter()
        .filter(|fact| !superseded.contains(&fact.report_hash))
    {
        let key = (
            fact.source_id.clone(),
            fact.instrument_key.clone(),
            fact.variable,
            fact.report_kind,
            fact.observed_at,
        );
        if let Some(current) = canonical.get(&key) {
            if current.revision == fact.revision && current.report_hash != fact.report_hash {
                return Err(WeatherProjectionFailure::AmbiguousRevision {
                    instrument_key: fact.instrument_key.clone(),
                    observed_at: fact.observed_at,
                    revision: fact.revision,
                });
            }
            if (fact.revision, fact.available_at, fact.report_hash)
                > (current.revision, current.available_at, current.report_hash)
            {
                canonical.insert(key, fact);
            }
        } else {
            canonical.insert(key, fact);
        }
    }
    Ok(canonical.into_values().collect())
}

fn select_temperature<'a>(
    binding: &ResolvedBinding,
    subject: &WeatherSubject,
    facts: &[&'a WeatherObservationFact],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    purpose: WeatherProjectionPurpose,
) -> Result<AggregateSelection<'a>, WeatherProjectionFailure> {
    let final_spec = SourceSpec {
        role: LinkageSourceRole::Resolution,
        source_id: DomainSourceId::ghcnd(),
        variable: match subject.decision_group.temperature_statistic {
            WeatherTemperatureStatistic::Maximum => WeatherVariable::TemperatureMaximum,
            WeatherTemperatureStatistic::Minimum => WeatherVariable::TemperatureMinimum,
        },
        unit: DomainMeasurementUnit::Celsius,
        report_kind: WeatherObservationReportKind::GhcndDailyTemperature,
    };
    let preliminary_spec = SourceSpec {
        role: LinkageSourceRole::LiveEvent,
        source_id: DomainSourceId::aviation_weather(),
        variable: WeatherVariable::Temperature,
        unit: DomainMeasurementUnit::Celsius,
        report_kind: WeatherObservationReportKind::Metar,
    };
    let final_facts = source_facts(binding, facts, &final_spec, from, to)?;
    let (selected, role, source_id, instrument, maturity) =
        if has_complete_coverage(&final_facts, from, to)
            && purpose == WeatherProjectionPurpose::FinalLabel
        {
            let instrument = source_instrument(binding, &final_spec)?;
            (
                final_facts,
                final_spec.role,
                final_spec.source_id.clone(),
                instrument,
                WeatherTruthMaturity::Final,
            )
        } else if purpose == WeatherProjectionPurpose::FinalLabel {
            return if final_facts.is_empty() {
                Err(final_spec.unavailable())
            } else {
                Err(WeatherProjectionFailure::InsufficientCoverage)
            };
        } else {
            let preliminary = live_temperature_facts(binding, facts, from, to)?;
            if preliminary.is_empty() {
                if final_facts.is_empty() {
                    return Err(preliminary_spec.unavailable());
                }
                let instrument = source_instrument(binding, &final_spec)?;
                (
                    final_facts,
                    final_spec.role,
                    final_spec.source_id.clone(),
                    instrument,
                    WeatherTruthMaturity::Final,
                )
            } else {
                let instrument = source_instrument(binding, &preliminary_spec)?;
                (
                    preliminary,
                    preliminary_spec.role,
                    preliminary_spec.source_id,
                    instrument,
                    WeatherTruthMaturity::Preliminary,
                )
            }
        };
    let value = match subject.decision_group.temperature_statistic {
        WeatherTemperatureStatistic::Maximum => selected.iter().map(|fact| fact.value).max(),
        WeatherTemperatureStatistic::Minimum => selected.iter().map(|fact| fact.value).min(),
    }
    .ok_or(WeatherProjectionFailure::InsufficientCoverage)?;
    Ok(AggregateSelection {
        facts: selected,
        source_role: role,
        source_id,
        instrument_key: instrument,
        value,
        maturity,
    })
}

fn live_temperature_facts<'a>(
    binding: &ResolvedBinding,
    facts: &[&'a WeatherObservationFact],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<&'a WeatherObservationFact>, WeatherProjectionFailure> {
    let source_id = DomainSourceId::aviation_weather();
    let instrument = binding
        .source_bindings
        .iter()
        .find(|source| source.role == LinkageSourceRole::LiveEvent && source.source_id == source_id)
        .map(|source| &source.instrument_key)
        .ok_or(WeatherProjectionFailure::InvalidSourceBinding)?;
    Ok(facts
        .iter()
        .copied()
        .filter(|fact| {
            fact.source_id == source_id
                && &fact.instrument_key == instrument
                && fact.variable == WeatherVariable::Temperature
                && fact.unit == DomainMeasurementUnit::Celsius
                && matches!(
                    fact.report_kind,
                    WeatherObservationReportKind::Metar
                        | WeatherObservationReportKind::Speci
                        | WeatherObservationReportKind::Correction
                )
                && weather_observation_in_window(fact, from, to)
        })
        .collect())
}

fn select_precipitation<'a>(
    binding: &ResolvedBinding,
    subject: &WeatherPrecipitationSubject,
    facts: &[&'a WeatherObservationFact],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<AggregateSelection<'a>, WeatherProjectionFailure> {
    let WeatherTruthPolicy::ObservationWithForecast {
        observation_source: source_id,
        ..
    } = &subject.truth_policy
    else {
        return Err(WeatherProjectionFailure::InvalidSourceBinding);
    };
    let final_spec = SourceSpec {
        role: LinkageSourceRole::Resolution,
        source_id: source_id.clone(),
        variable: WeatherVariable::Precipitation,
        unit: DomainMeasurementUnit::Millimeter,
        report_kind: WeatherObservationReportKind::HkoDailyRainfall,
    };
    let final_facts = source_facts(binding, facts, &final_spec, from, to)?;
    if has_complete_coverage(&final_facts, from, to)
        && final_facts.iter().all(|fact| fact.published_at >= to)
    {
        return selection_from_sum(
            binding,
            &final_spec,
            final_facts,
            WeatherTruthMaturity::Final,
        );
    }
    Err(if final_facts.is_empty() {
        final_spec.unavailable()
    } else {
        WeatherProjectionFailure::InsufficientCoverage
    })
}

fn select_aqi<'a>(
    binding: &ResolvedBinding,
    subject: &WeatherAqiSubject,
    facts: &[&'a WeatherObservationFact],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    purpose: WeatherProjectionPurpose,
) -> Result<AggregateSelection<'a>, WeatherProjectionFailure> {
    let source_id = final_source(&subject.truth_policy)?;
    let spec = SourceSpec {
        role: LinkageSourceRole::Resolution,
        source_id: source_id.clone(),
        variable: WeatherVariable::Aqi,
        unit: DomainMeasurementUnit::Aqi,
        report_kind: WeatherObservationReportKind::AirNowPm25OfficialDaily,
    };
    let selected = source_facts(binding, facts, &spec, from, to)?;
    if has_complete_coverage(&selected, from, to)
        && let Some(latest) = latest_report(&selected)
        && latest.published_at >= to
    {
        return selection_from_latest(binding, &spec, latest, WeatherTruthMaturity::Final);
    }
    if purpose == WeatherProjectionPurpose::FinalLabel {
        return Err(if selected.is_empty() {
            spec.unavailable()
        } else {
            WeatherProjectionFailure::InsufficientCoverage
        });
    }
    let preliminary_spec = SourceSpec {
        role: LinkageSourceRole::LiveEvent,
        source_id: source_id.clone(),
        variable: WeatherVariable::Aqi,
        unit: DomainMeasurementUnit::Aqi,
        report_kind: WeatherObservationReportKind::AirNowPm25AreaObservation,
    };
    let preliminary = source_facts(binding, facts, &preliminary_spec, from, to)?;
    let latest = latest_report(&preliminary).ok_or_else(|| preliminary_spec.unavailable())?;
    selection_from_latest(
        binding,
        &preliminary_spec,
        latest,
        WeatherTruthMaturity::Preliminary,
    )
}

fn select_tornado<'a>(
    binding: &ResolvedBinding,
    subject: &WeatherTornadoSubject,
    facts: &[&'a WeatherObservationFact],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    purpose: WeatherProjectionPurpose,
) -> Result<AggregateSelection<'a>, WeatherProjectionFailure> {
    let (preliminary_source, final_source_id) = precedence_sources(&subject.truth_policy)?;
    let final_kind = match &subject.finalization {
        WeatherTornadoFinalization::StormEventsArchive => {
            WeatherObservationReportKind::NceiFinalTornado
        }
        WeatherTornadoFinalization::FirstPublishedAfter { .. } => {
            WeatherObservationReportKind::NceiTornadoTimeSeries
        }
    };
    let final_spec = SourceSpec {
        role: LinkageSourceRole::Resolution,
        source_id: final_source_id.clone(),
        variable: WeatherVariable::TornadoCount,
        unit: DomainMeasurementUnit::Count,
        report_kind: final_kind,
    };
    let final_facts = source_facts(binding, facts, &final_spec, from, to)?;
    match &subject.finalization {
        WeatherTornadoFinalization::StormEventsArchive
            if has_complete_coverage(&final_facts, from, to) =>
        {
            return selection_from_sum(
                binding,
                &final_spec,
                final_facts,
                WeatherTruthMaturity::Final,
            );
        }
        WeatherTornadoFinalization::FirstPublishedAfter { not_before } => {
            let first = final_facts
                .iter()
                .copied()
                .filter(|fact| {
                    fact.published_at >= *not_before
                        && fact.valid_from.is_some_and(|value| value <= from)
                        && fact.valid_to.is_some_and(|value| value >= to)
                })
                .min_by_key(|fact| (fact.published_at, fact.available_at, fact.report_hash));
            if let Some(first) = first {
                return selection_from_latest(
                    binding,
                    &final_spec,
                    first,
                    WeatherTruthMaturity::Final,
                );
            }
        }
        WeatherTornadoFinalization::StormEventsArchive => {}
    }
    if purpose == WeatherProjectionPurpose::FinalLabel {
        return Err(if final_facts.is_empty() {
            final_spec.unavailable()
        } else {
            WeatherProjectionFailure::InsufficientCoverage
        });
    }
    let preliminary_spec = SourceSpec {
        role: LinkageSourceRole::LiveEvent,
        source_id: preliminary_source.clone(),
        variable: WeatherVariable::TornadoCount,
        unit: DomainMeasurementUnit::Count,
        report_kind: WeatherObservationReportKind::SpcPreliminaryTornado,
    };
    let preliminary = source_facts(binding, facts, &preliminary_spec, from, to)?
        .into_iter()
        .filter(|fact| {
            fact.valid_from.is_some_and(|start| start >= from)
                && fact.valid_to.is_some_and(|end| end <= to)
        })
        .collect::<Vec<_>>();
    selection_from_sum(
        binding,
        &preliminary_spec,
        preliminary,
        WeatherTruthMaturity::Preliminary,
    )
}

fn select_cyclone<'a>(
    binding: &ResolvedBinding,
    subject: &WeatherTropicalCycloneSubject,
    facts: &[&'a WeatherObservationFact],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    purpose: WeatherProjectionPurpose,
) -> Result<AggregateSelection<'a>, WeatherProjectionFailure> {
    let (preliminary_source, final_source_id) = precedence_sources(&subject.truth_policy)?;
    let final_variable = match subject.outcome {
        TropicalCycloneOutcome::MaximumSustainedWind { .. } => WeatherVariable::CycloneIntensity,
        TropicalCycloneOutcome::LandfallAtOrAbove { .. } => {
            WeatherVariable::CycloneLandfallIntensity
        }
    };
    let final_spec = SourceSpec {
        role: LinkageSourceRole::Resolution,
        source_id: final_source_id.clone(),
        variable: final_variable,
        unit: DomainMeasurementUnit::Knot,
        report_kind: WeatherObservationReportKind::NhcBestTrack,
    };
    let final_facts = source_facts(binding, facts, &final_spec, from, to)?;
    if !final_facts.is_empty() {
        return selection_from_max(
            binding,
            &final_spec,
            final_facts,
            WeatherTruthMaturity::Final,
        );
    }
    if purpose == WeatherProjectionPurpose::FinalLabel {
        return Err(final_spec.unavailable());
    }
    let preliminary_spec = SourceSpec {
        role: LinkageSourceRole::LiveEvent,
        source_id: preliminary_source.clone(),
        variable: WeatherVariable::CycloneIntensity,
        unit: DomainMeasurementUnit::Knot,
        report_kind: WeatherObservationReportKind::NhcAdvisory,
    };
    let preliminary = source_facts(binding, facts, &preliminary_spec, from, to)?;
    selection_from_max(
        binding,
        &preliminary_spec,
        preliminary,
        WeatherTruthMaturity::Preliminary,
    )
}

fn project_global(
    binding: &ResolvedBinding,
    subject: &WeatherGlobalTemperatureSubject,
    facts: &[&WeatherObservationFact],
    subject_key: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    purpose: WeatherProjectionPurpose,
) -> Result<WeatherContractProjection, WeatherProjectionFailure> {
    if subject.dataset_version != 4
        || subject.base_period_start_year != 1951
        || subject.base_period_end_year != 1980
    {
        return Err(WeatherProjectionFailure::UnsupportedOfficialProduct);
    }
    let source_id = final_source(&subject.truth_policy)?;
    let report_kind = match &subject.outcome {
        GlobalTemperatureOutcome::MonthlyAnomaly { .. }
        | GlobalTemperatureOutcome::MonthlyRecordRank { .. } => {
            WeatherObservationReportKind::NasaGistemp
        }
        GlobalTemperatureOutcome::AnnualRecordRank { .. } => {
            WeatherObservationReportKind::NasaGistempAnnual
        }
    };
    let spec = SourceSpec {
        role: LinkageSourceRole::Resolution,
        source_id: source_id.clone(),
        variable: WeatherVariable::GlobalTemperatureAnomaly,
        unit: DomainMeasurementUnit::CelsiusAnomaly,
        report_kind,
    };
    let target_facts = source_facts(binding, facts, &spec, from, to)?;
    let target = target_facts
        .iter()
        .copied()
        .filter(|fact| {
            fact.published_at >= to
                && fact.valid_from.is_some_and(|value| value <= from)
                && fact.valid_to.is_some_and(|value| value >= to)
        })
        .min_by_key(|fact| (fact.published_at, fact.available_at, fact.report_hash))
        .ok_or_else(|| {
            if target_facts.is_empty() {
                spec.unavailable()
            } else {
                WeatherProjectionFailure::InsufficientCoverage
            }
        })?;
    let selection = selection_from_latest(binding, &spec, target, WeatherTruthMaturity::Final)?;
    let (comparison_value, comparison_unit, outcome, distance) = match &subject.outcome {
        GlobalTemperatureOutcome::MonthlyAnomaly { comparator } => {
            let value = target.value;
            (
                value,
                WeatherComparisonUnit::CelsiusAnomaly,
                comparator.includes(value),
                comparator_distance(comparator, value),
            )
        }
        GlobalTemperatureOutcome::MonthlyRecordRank {
            rank: rank_contract,
        }
        | GlobalTemperatureOutcome::AnnualRecordRank {
            rank: rank_contract,
        } => {
            let instrument = source_instrument(binding, &spec)?;
            let target_month = matches!(
                &subject.outcome,
                GlobalTemperatureOutcome::MonthlyRecordRank { .. }
            )
            .then_some(from.month());
            let mut history = BTreeMap::<DateTime<Utc>, &WeatherObservationFact>::new();
            for fact in facts.iter().copied().filter(|fact| {
                fact.source_id == *source_id
                    && fact.instrument_key == instrument
                    && fact.variable == WeatherVariable::GlobalTemperatureAnomaly
                    && fact.unit == DomainMeasurementUnit::CelsiusAnomaly
                    && fact.report_kind == report_kind
                    && fact.published_at <= target.published_at
                    && fact.available_at <= target.available_at
            }) {
                let Some(period_start) = fact.valid_from else {
                    return Err(WeatherProjectionFailure::InsufficientCoverage);
                };
                if target_month.is_some_and(|month| period_start.month() != month) {
                    continue;
                }
                let replace = history.get(&period_start).is_none_or(|existing| {
                    (fact.available_at, fact.revision, fact.report_hash)
                        > (
                            existing.available_at,
                            existing.revision,
                            existing.report_hash,
                        )
                });
                if replace {
                    history.insert(period_start, fact);
                }
            }
            if history.is_empty() {
                return Err(WeatherProjectionFailure::InsufficientCoverage);
            }
            let hotter = history
                .values()
                .filter(|fact| fact.value > target.value)
                .count();
            let rank = u64::try_from(hotter)
                .ok()
                .and_then(|value| value.checked_add(1))
                .map(Decimal::from)
                .ok_or(WeatherProjectionFailure::InsufficientCoverage)?;
            let (outcome, distance) = match rank_contract {
                GlobalTemperatureRank::Exact {
                    rank: expected_rank,
                } => {
                    let expected = Decimal::from(*expected_rank);
                    (rank == expected, Decimal::ZERO - (rank - expected).abs())
                }
                GlobalTemperatureRank::AtLeast { rank: minimum_rank } => {
                    let minimum = Decimal::from(*minimum_rank);
                    (rank >= minimum, rank - minimum)
                }
            };
            (rank, WeatherComparisonUnit::Rank, outcome, distance)
        }
    };
    if purpose == WeatherProjectionPurpose::FinalLabel && window_is_open(to, target) {
        return Err(WeatherProjectionFailure::InsufficientCoverage);
    }
    build_projection(
        selection,
        subject_key.to_owned(),
        comparison_value,
        comparison_unit,
        outcome,
        distance,
    )
}

fn select_sea_ice<'a>(
    binding: &ResolvedBinding,
    subject: &WeatherSeaIceSubject,
    facts: &[&'a WeatherObservationFact],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    _purpose: WeatherProjectionPurpose,
) -> Result<AggregateSelection<'a>, WeatherProjectionFailure> {
    if subject.dataset_version != 4 || subject.concentration_threshold_percent != Decimal::from(15)
    {
        return Err(WeatherProjectionFailure::UnsupportedOfficialProduct);
    }
    let source_id = final_source(&subject.truth_policy)?;
    let report_kind = match subject.product {
        SeaIceProduct::DailyExtent => WeatherObservationReportKind::NsidcDailySeaIce,
        SeaIceProduct::MonthlyExtent => WeatherObservationReportKind::NsidcMonthlySeaIce,
    };
    let spec = SourceSpec {
        role: LinkageSourceRole::Resolution,
        source_id: source_id.clone(),
        variable: WeatherVariable::SeaIceExtent,
        unit: DomainMeasurementUnit::MillionSquareKilometer,
        report_kind,
    };
    let selected = source_facts(binding, facts, &spec, from, to)?;
    if !has_complete_coverage(&selected, from, to) {
        return Err(if selected.is_empty() {
            spec.unavailable()
        } else {
            WeatherProjectionFailure::InsufficientCoverage
        });
    }
    match subject.aggregation {
        SeaIceAggregation::MinimumDailyExtent if subject.product == SeaIceProduct::DailyExtent => {
            selection_from_min(binding, &spec, selected, WeatherTruthMaturity::Final)
        }
        SeaIceAggregation::MaximumDailyExtent if subject.product == SeaIceProduct::DailyExtent => {
            selection_from_max(binding, &spec, selected, WeatherTruthMaturity::Final)
        }
        SeaIceAggregation::MonthlyMeanExtent if subject.product == SeaIceProduct::MonthlyExtent => {
            let latest = latest_report(&selected).ok_or_else(|| spec.unavailable())?;
            selection_from_latest(binding, &spec, latest, WeatherTruthMaturity::Final)
        }
        _ => Err(WeatherProjectionFailure::UnsupportedOfficialProduct),
    }
}

fn select_wind<'a>(
    binding: &ResolvedBinding,
    subject: &WeatherWindExtremeSubject,
    facts: &[&'a WeatherObservationFact],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    purpose: WeatherProjectionPurpose,
) -> Result<AggregateSelection<'a>, WeatherProjectionFailure> {
    let WeatherTruthPolicy::ObservationWithForecast {
        observation_source: source_id,
        ..
    } = &subject.truth_policy
    else {
        return Err(WeatherProjectionFailure::InvalidSourceBinding);
    };
    let variable = match subject.statistic {
        WeatherWindStatistic::MaximumGust => WeatherVariable::WindGust,
        WeatherWindStatistic::MaximumSustainedWind => WeatherVariable::WindSpeed,
    };
    let spec = SourceSpec {
        role: LinkageSourceRole::Resolution,
        source_id: source_id.clone(),
        variable,
        unit: DomainMeasurementUnit::Knot,
        report_kind: WeatherObservationReportKind::NwsStation,
    };
    let selected = source_facts(binding, facts, &spec, from, to)?;
    if purpose == WeatherProjectionPurpose::FinalLabel
        && !has_point_coverage(
            &selected,
            from,
            to,
            Duration::hours(WIND_MAX_OBSERVATION_GAP_HOURS),
        )
    {
        return Err(if selected.is_empty() {
            spec.unavailable()
        } else {
            WeatherProjectionFailure::InsufficientCoverage
        });
    }
    let maturity = if purpose == WeatherProjectionPurpose::FinalLabel {
        WeatherTruthMaturity::Final
    } else {
        WeatherTruthMaturity::Preliminary
    };
    selection_from_max(binding, &spec, selected, maturity)
}

fn projection_from_selection(
    binding: &ResolvedBinding,
    selection: AggregateSelection<'_>,
    subject_key: String,
) -> Result<WeatherContractProjection, WeatherProjectionFailure> {
    let (comparison_value, comparison_unit, outcome, distance) = match &binding.subject {
        MarketSubject::Weather(subject) => {
            let celsius = TemperatureCelsius::new(selection.value);
            let value = celsius.whole_degrees(subject.decision_group.market_unit);
            let unit = match subject.decision_group.market_unit {
                TemperatureUnit::Celsius => WeatherComparisonUnit::Celsius,
                TemperatureUnit::Fahrenheit => WeatherComparisonUnit::Fahrenheit,
            };
            (
                value,
                unit,
                subject.outcome_band.contains(value),
                band_distance(&subject.outcome_band, value),
            )
        }
        MarketSubject::WeatherPrecipitation(subject) => comparison_projection(
            selection.value,
            subject.rounding,
            &subject.comparator,
            WeatherComparisonUnit::Millimeter,
        ),
        MarketSubject::WeatherAqi(subject) => (
            selection.value,
            WeatherComparisonUnit::Aqi,
            subject.comparator.includes(selection.value),
            comparator_distance(&subject.comparator, selection.value),
        ),
        MarketSubject::WeatherTornado(subject) => (
            selection.value,
            WeatherComparisonUnit::Count,
            subject.comparator.includes(selection.value),
            comparator_distance(&subject.comparator, selection.value),
        ),
        MarketSubject::WeatherTropicalCyclone(subject) => match &subject.outcome {
            TropicalCycloneOutcome::MaximumSustainedWind { comparator } => (
                selection.value,
                WeatherComparisonUnit::Knot,
                comparator.includes(selection.value),
                comparator_distance(comparator, selection.value),
            ),
            TropicalCycloneOutcome::LandfallAtOrAbove {
                minimum_sustained_wind_knots,
                ..
            } => (
                selection.value,
                WeatherComparisonUnit::Knot,
                selection.value >= *minimum_sustained_wind_knots,
                selection.value - *minimum_sustained_wind_knots,
            ),
        },
        MarketSubject::WeatherSeaIce(subject) => (
            selection.value,
            WeatherComparisonUnit::MillionSquareKilometer,
            subject.comparator.includes(selection.value),
            comparator_distance(&subject.comparator, selection.value),
        ),
        MarketSubject::WeatherWindExtreme(subject) => comparison_projection(
            selection.value,
            subject.rounding,
            &subject.comparator,
            WeatherComparisonUnit::Knot,
        ),
        MarketSubject::WeatherGlobalTemperature(_) | MarketSubject::Crypto(_) => {
            return Err(WeatherProjectionFailure::NonWeatherSubject);
        }
    };
    build_projection(
        selection,
        subject_key,
        comparison_value,
        comparison_unit,
        outcome,
        distance,
    )
}

fn comparison_projection(
    value: Decimal,
    rounding: WeatherRoundingRule,
    comparator: &WeatherValueComparator,
    unit: WeatherComparisonUnit,
) -> (Decimal, WeatherComparisonUnit, bool, Decimal) {
    let rounded = round_weather_value(value, rounding);
    (
        rounded,
        unit,
        comparator.includes(rounded),
        comparator_distance(comparator, rounded),
    )
}

fn build_projection(
    selection: AggregateSelection<'_>,
    subject_key: String,
    comparison_value: Decimal,
    comparison_unit: WeatherComparisonUnit,
    outcome: bool,
    boundary_distance: Decimal,
) -> Result<WeatherContractProjection, WeatherProjectionFailure> {
    let mut report_hashes = selection
        .facts
        .iter()
        .map(|fact| fact.report_hash)
        .collect::<Vec<_>>();
    report_hashes.sort();
    report_hashes.dedup();
    let first = selection
        .facts
        .first()
        .ok_or(WeatherProjectionFailure::InsufficientCoverage)?;
    let effective_at = selection
        .facts
        .iter()
        .map(|fact| fact.observed_at)
        .max()
        .unwrap_or(first.observed_at);
    let available_at = selection
        .facts
        .iter()
        .map(|fact| fact.available_at)
        .max()
        .unwrap_or(first.available_at);
    Ok(WeatherContractProjection {
        subject_key,
        canonical_value: selection.value,
        comparison_value,
        comparison_unit,
        outcome,
        boundary_distance,
        maturity: selection.maturity,
        source_role: selection.source_role,
        source_id: selection.source_id,
        instrument_key: selection.instrument_key,
        effective_at,
        available_at,
        report_hashes,
    })
}

fn source_facts<'a>(
    binding: &ResolvedBinding,
    facts: &[&'a WeatherObservationFact],
    spec: &SourceSpec,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Vec<&'a WeatherObservationFact>, WeatherProjectionFailure> {
    let instrument = source_instrument(binding, spec)?;
    let matching = facts
        .iter()
        .copied()
        .filter(|fact| {
            fact.source_id == spec.source_id
                && fact.instrument_key == instrument
                && fact.variable == spec.variable
                && fact.report_kind == spec.report_kind
                && weather_observation_in_window(fact, from, to)
        })
        .collect::<Vec<_>>();
    if let Some(fact) = matching.iter().find(|fact| fact.unit != spec.unit) {
        return Err(WeatherProjectionFailure::UnitMismatch {
            expected: spec.unit,
            actual: fact.unit,
        });
    }
    Ok(matching)
}

fn source_instrument(
    binding: &ResolvedBinding,
    spec: &SourceSpec,
) -> Result<DomainInstrumentKey, WeatherProjectionFailure> {
    binding
        .source_bindings
        .iter()
        .find(|source| source.role == spec.role && source.source_id == spec.source_id)
        .map(|source| source.instrument_key.clone())
        .ok_or(WeatherProjectionFailure::InvalidSourceBinding)
}

fn selection_from_latest<'a>(
    binding: &ResolvedBinding,
    spec: &SourceSpec,
    fact: &'a WeatherObservationFact,
    maturity: WeatherTruthMaturity,
) -> Result<AggregateSelection<'a>, WeatherProjectionFailure> {
    Ok(AggregateSelection {
        facts: vec![fact],
        source_role: spec.role,
        source_id: spec.source_id.clone(),
        instrument_key: source_instrument(binding, spec)?,
        value: fact.value,
        maturity,
    })
}

fn selection_from_max<'a>(
    binding: &ResolvedBinding,
    spec: &SourceSpec,
    facts: Vec<&'a WeatherObservationFact>,
    maturity: WeatherTruthMaturity,
) -> Result<AggregateSelection<'a>, WeatherProjectionFailure> {
    let value = facts
        .iter()
        .map(|fact| fact.value)
        .max()
        .ok_or_else(|| spec.unavailable())?;
    Ok(AggregateSelection {
        facts,
        source_role: spec.role,
        source_id: spec.source_id.clone(),
        instrument_key: source_instrument(binding, spec)?,
        value,
        maturity,
    })
}

fn selection_from_min<'a>(
    binding: &ResolvedBinding,
    spec: &SourceSpec,
    facts: Vec<&'a WeatherObservationFact>,
    maturity: WeatherTruthMaturity,
) -> Result<AggregateSelection<'a>, WeatherProjectionFailure> {
    let value = facts
        .iter()
        .map(|fact| fact.value)
        .min()
        .ok_or_else(|| spec.unavailable())?;
    Ok(AggregateSelection {
        facts,
        source_role: spec.role,
        source_id: spec.source_id.clone(),
        instrument_key: source_instrument(binding, spec)?,
        value,
        maturity,
    })
}

fn selection_from_sum<'a>(
    binding: &ResolvedBinding,
    spec: &SourceSpec,
    facts: Vec<&'a WeatherObservationFact>,
    maturity: WeatherTruthMaturity,
) -> Result<AggregateSelection<'a>, WeatherProjectionFailure> {
    if facts.is_empty() {
        return Err(spec.unavailable());
    }
    let value = facts.iter().try_fold(Decimal::ZERO, |sum, fact| {
        sum.checked_add(fact.value)
            .ok_or(WeatherProjectionFailure::AggregationOverflow)
    })?;
    Ok(AggregateSelection {
        facts,
        source_role: spec.role,
        source_id: spec.source_id.clone(),
        instrument_key: source_instrument(binding, spec)?,
        value,
        maturity,
    })
}

fn has_complete_coverage(
    facts: &[&WeatherObservationFact],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> bool {
    let mut intervals = facts
        .iter()
        .filter_map(|fact| Some((fact.valid_from?, fact.valid_to?)))
        .collect::<Vec<_>>();
    intervals.sort();
    let Some((start, mut covered_to)) = intervals.first().copied() else {
        return false;
    };
    if start > from {
        return false;
    }
    for (next_start, next_end) in intervals.into_iter().skip(1) {
        if next_start > covered_to {
            return false;
        }
        covered_to = covered_to.max(next_end);
    }
    covered_to >= to
}

fn has_point_coverage(
    facts: &[&WeatherObservationFact],
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    maximum_gap: Duration,
) -> bool {
    let mut points = facts
        .iter()
        .map(|fact| fact.observed_at)
        .filter(|observed_at| *observed_at >= from && *observed_at <= to)
        .collect::<Vec<_>>();
    points.sort();
    points.dedup();
    let Some(first) = points.first().copied() else {
        return false;
    };
    let Some(last) = points.last().copied() else {
        return false;
    };
    first - from <= maximum_gap
        && to - last <= maximum_gap
        && points
            .windows(2)
            .all(|window| window[1] - window[0] <= maximum_gap)
}

fn latest_report<'a>(facts: &[&'a WeatherObservationFact]) -> Option<&'a WeatherObservationFact> {
    facts.iter().copied().max_by_key(|fact| {
        (
            fact.published_at,
            fact.revision,
            fact.observed_at,
            fact.report_hash,
        )
    })
}

const fn final_source(
    policy: &WeatherTruthPolicy,
) -> Result<&DomainSourceId, WeatherProjectionFailure> {
    match policy {
        WeatherTruthPolicy::FinalOnly { final_source } => Ok(final_source),
        _ => Err(WeatherProjectionFailure::InvalidSourceBinding),
    }
}

const fn precedence_sources(
    policy: &WeatherTruthPolicy,
) -> Result<(&DomainSourceId, &DomainSourceId), WeatherProjectionFailure> {
    match policy {
        WeatherTruthPolicy::PreliminaryThenFinal {
            preliminary_source,
            final_source,
        } => Ok((preliminary_source, final_source)),
        _ => Err(WeatherProjectionFailure::InvalidSourceBinding),
    }
}

fn round_weather_value(value: Decimal, rule: WeatherRoundingRule) -> Decimal {
    match rule {
        WeatherRoundingRule::Exact => value,
        WeatherRoundingRule::DecimalPlaces { places } => {
            value.round_dp_with_strategy(places, RoundingStrategy::MidpointAwayFromZero)
        }
        WeatherRoundingRule::WholeUnit => {
            value.round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
        }
    }
}

fn comparator_distance(comparator: &WeatherValueComparator, value: Decimal) -> Decimal {
    match comparator {
        WeatherValueComparator::Above { threshold, .. } => value - *threshold,
        WeatherValueComparator::Below { threshold, .. } => *threshold - value,
        WeatherValueComparator::Between { lower, upper, .. } => {
            (value - *lower).min(*upper - value)
        }
    }
}

fn band_distance(band: &TemperatureBand, value: Decimal) -> Decimal {
    match (band.lower_inclusive, band.upper_inclusive) {
        (Some(lower), Some(upper)) => (value - lower).min(upper - value),
        (Some(lower), None) => value - lower,
        (None, Some(upper)) => upper - value,
        (None, None) => Decimal::ZERO,
    }
}

fn window_is_open(end_at: DateTime<Utc>, fact: &WeatherObservationFact) -> bool {
    fact.observed_at < end_at && fact.valid_to.is_none_or(|valid_to| valid_to < end_at)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Days, NaiveDate, TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            data_plane::{WeatherObservationFact, WeatherObservationReportKind},
            quant::{
                GlobalTemperatureOutcome, GlobalTemperatureRank, GroundingProof, MarketSubject,
                ResolvedBinding, WeatherContractWindow, WeatherDecisionGroupKey,
                WeatherGlobalTemperatureSubject, WeatherPrecipitationSubject, WeatherRoundingRule,
                WeatherSubject, WeatherTornadoFinalization, WeatherTornadoSubject,
                WeatherTruthPolicy, WeatherValueComparator,
            },
        },
        enums::domain::LinkageSourceRole,
        hashing::CanonicalDigest,
        types::{
            ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, IcaoStation,
            TemperatureBand, TemperatureUnit, WeatherContractFinalizationPolicy,
            WeatherTemperatureStatistic, WeatherVariable,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::{
        domain::{
            WeatherFactWindow, WeatherProjectionFailure, WeatherProjectionPurpose,
            WeatherTruthMaturity, project_weather_contract,
        },
        linkage::source_bindings_for_subject,
    };

    fn hash(seed: &str) -> ContentHash {
        CanonicalDigest::content_hash_json(&seed).expect("test hash")
    }

    fn binding(subject: MarketSubject) -> ResolvedBinding {
        let available_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        ResolvedBinding {
            source_bindings: source_bindings_for_subject(&subject, available_at)
                .expect("source bindings"),
            subject,
            grounding: GroundingProof { spans: Vec::new() },
            override_context: None,
        }
    }

    fn tornado_subject() -> MarketSubject {
        MarketSubject::WeatherTornado(WeatherTornadoSubject {
            region_key: "oklahoma".to_owned(),
            window: WeatherContractWindow {
                start_at: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
                end_at: Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap(),
                timezone: "UTC".to_owned(),
            },
            comparator: WeatherValueComparator::Above {
                threshold: dec!(2),
                inclusive: true,
            },
            finalization: WeatherTornadoFinalization::StormEventsArchive,
            truth_policy: WeatherTruthPolicy::PreliminaryThenFinal {
                preliminary_source: DomainSourceId::spc_storm_reports(),
                final_source: DomainSourceId::ncei_storm_events(),
            },
        })
    }

    fn tornado_fact(day: u64, value: Decimal) -> WeatherObservationFact {
        let local_date = NaiveDate::from_ymd_opt(2026, 5, 1)
            .expect("month start")
            .checked_add_days(Days::new(day))
            .expect("day");
        let start = local_date
            .and_hms_opt(0, 0, 0)
            .expect("day start")
            .and_utc();
        let end = local_date
            .succ_opt()
            .expect("next day")
            .and_hms_opt(0, 0, 0)
            .expect("day end")
            .and_utc();
        WeatherObservationFact {
            source_id: DomainSourceId::ncei_storm_events(),
            instrument_key: DomainInstrumentKey::ncei_tornado("oklahoma"),
            subject_key: "oklahoma".to_owned(),
            local_date,
            report_kind: WeatherObservationReportKind::NceiFinalTornado,
            variable: WeatherVariable::TornadoCount,
            value,
            unit: DomainMeasurementUnit::Count,
            precision: Decimal::ONE,
            observed_at: end,
            valid_from: Some(start),
            valid_to: Some(end),
            published_at: Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap(),
            available_at: Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap(),
            revision: 0,
            report_hash: hash(&format!("tornado-{day}-{value}")),
            supersedes_report_hash: None,
        }
    }

    fn precipitation_subject() -> MarketSubject {
        MarketSubject::WeatherPrecipitation(WeatherPrecipitationSubject {
            site_key: "Hong Kong".to_owned(),
            station_key: "HKO".to_owned(),
            latitude: dec!(22.301944),
            longitude: dec!(114.174167),
            window: WeatherContractWindow {
                start_at: Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap(),
                end_at: Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap(),
                timezone: "UTC".to_owned(),
            },
            comparator: WeatherValueComparator::Above {
                threshold: dec!(100),
                inclusive: true,
            },
            rounding: WeatherRoundingRule::DecimalPlaces { places: 1 },
            truth_policy: WeatherTruthPolicy::ObservationWithForecast {
                observation_source: DomainSourceId::hko_open_data(),
                forecast_source: DomainSourceId::gefs(),
            },
        })
    }

    fn precipitation_fact(day: u64, value: Decimal) -> WeatherObservationFact {
        let local_date = NaiveDate::from_ymd_opt(2026, 7, 1)
            .expect("month start")
            .checked_add_days(Days::new(day))
            .expect("day");
        let start = local_date
            .and_hms_opt(0, 0, 0)
            .expect("day start")
            .and_utc();
        let end = local_date
            .succ_opt()
            .expect("next day")
            .and_hms_opt(0, 0, 0)
            .expect("day end")
            .and_utc();
        WeatherObservationFact {
            source_id: DomainSourceId::hko_open_data(),
            instrument_key: DomainInstrumentKey::hko_daily_rainfall("HKO"),
            subject_key: "Hong Kong".to_owned(),
            local_date,
            report_kind: WeatherObservationReportKind::HkoDailyRainfall,
            variable: WeatherVariable::Precipitation,
            value,
            unit: DomainMeasurementUnit::Millimeter,
            precision: dec!(0.1),
            observed_at: end,
            valid_from: Some(start),
            valid_to: Some(end),
            published_at: Utc.with_ymd_and_hms(2026, 8, 2, 0, 0, 0).unwrap(),
            available_at: Utc.with_ymd_and_hms(2026, 8, 2, 0, 0, 0).unwrap(),
            revision: 0,
            report_hash: hash(&format!("rain-{day}-{value}")),
            supersedes_report_hash: None,
        }
    }

    #[test]
    fn precipitation_sums_complete_month() {
        let binding = binding(precipitation_subject());
        let observations = (0..31)
            .map(|day| precipitation_fact(day, dec!(4)))
            .collect();
        let projection = project_weather_contract(
            &binding,
            &WeatherFactWindow {
                decision_at: Utc.with_ymd_and_hms(2026, 8, 2, 0, 0, 0).unwrap(),
                observations,
                forecasts: Vec::new(),
                calibration: None,
            },
            WeatherProjectionPurpose::FinalLabel,
        )
        .expect("complete precipitation projection");

        assert_eq!(projection.canonical_value, dec!(124));
        assert_eq!(projection.report_hashes.len(), 31);
        assert!(projection.outcome);
    }

    #[test]
    fn tornado_sums_complete_days() {
        let binding = binding(tornado_subject());
        let observations = (0..31)
            .map(|day| {
                tornado_fact(
                    day,
                    match day {
                        0 => dec!(2),
                        1 => dec!(1),
                        _ => Decimal::ZERO,
                    },
                )
            })
            .collect();
        let projection = project_weather_contract(
            &binding,
            &WeatherFactWindow {
                decision_at: Utc.with_ymd_and_hms(2026, 10, 2, 0, 0, 0).unwrap(),
                observations,
                forecasts: Vec::new(),
                calibration: None,
            },
            WeatherProjectionPurpose::FinalLabel,
        )
        .expect("complete final projection");

        assert_eq!(projection.canonical_value, dec!(3));
        assert_eq!(projection.maturity, WeatherTruthMaturity::Final);
        assert!(projection.outcome);
        assert_eq!(projection.report_hashes.len(), 31);
    }

    #[test]
    fn tornado_rejects_partial_days() {
        let binding = binding(tornado_subject());
        let observations = (0..30)
            .map(|day| tornado_fact(day, Decimal::ZERO))
            .collect();
        let error = project_weather_contract(
            &binding,
            &WeatherFactWindow {
                decision_at: Utc.with_ymd_and_hms(2026, 10, 2, 0, 0, 0).unwrap(),
                observations,
                forecasts: Vec::new(),
                calibration: None,
            },
            WeatherProjectionPurpose::FinalLabel,
        )
        .expect_err("partial month must fail closed");

        assert_eq!(error, WeatherProjectionFailure::InsufficientCoverage);
    }

    fn tornado_series_fact(
        published_at: DateTime<Utc>,
        value: Decimal,
        seed: &str,
        revision: u32,
        supersedes_report_hash: Option<ContentHash>,
    ) -> WeatherObservationFact {
        let start = Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        WeatherObservationFact {
            source_id: DomainSourceId::ncei_tornado_time_series(),
            instrument_key: DomainInstrumentKey::ncei_tornado_time_series(),
            subject_key: "united_states".to_owned(),
            local_date: NaiveDate::from_ymd_opt(2026, 5, 1).expect("date"),
            report_kind: WeatherObservationReportKind::NceiTornadoTimeSeries,
            variable: WeatherVariable::TornadoCount,
            value,
            unit: DomainMeasurementUnit::Count,
            precision: Decimal::ONE,
            observed_at: end,
            valid_from: Some(start),
            valid_to: Some(end),
            published_at,
            available_at: published_at,
            revision,
            report_hash: hash(seed),
            supersedes_report_hash,
        }
    }

    #[test]
    fn tornado_freezes_first_publication() {
        let subject = MarketSubject::WeatherTornado(WeatherTornadoSubject {
            region_key: "united_states".to_owned(),
            window: WeatherContractWindow {
                start_at: Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap(),
                end_at: Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap(),
                timezone: "UTC".to_owned(),
            },
            comparator: WeatherValueComparator::Above {
                threshold: dec!(100),
                inclusive: true,
            },
            finalization: WeatherTornadoFinalization::FirstPublishedAfter {
                not_before: Utc.with_ymd_and_hms(2026, 6, 10, 15, 0, 0).unwrap(),
            },
            truth_policy: WeatherTruthPolicy::PreliminaryThenFinal {
                preliminary_source: DomainSourceId::spc_storm_reports(),
                final_source: DomainSourceId::ncei_tornado_time_series(),
            },
        });
        let binding = binding(subject);
        let first_at = Utc.with_ymd_and_hms(2026, 6, 10, 15, 0, 1).unwrap();
        let corrected_at = Utc.with_ymd_and_hms(2026, 6, 11, 15, 0, 0).unwrap();
        let projection = project_weather_contract(
            &binding,
            &WeatherFactWindow {
                decision_at: corrected_at,
                observations: vec![
                    tornado_series_fact(first_at, dec!(99), "first", 0, None),
                    tornado_series_fact(
                        corrected_at,
                        dec!(101),
                        "correction",
                        1,
                        Some(hash("first")),
                    ),
                ],
                forecasts: Vec::new(),
                calibration: None,
            },
            WeatherProjectionPurpose::FinalLabel,
        )
        .expect("first publication is final");

        assert_eq!(projection.canonical_value, dec!(99));
        assert!(!projection.outcome);
        assert_eq!(projection.report_hashes, vec![hash("first")]);
    }

    fn gistemp_month_fact(
        year: i32,
        value: Decimal,
        published_at: DateTime<Utc>,
        revision: u32,
        seed: &str,
        supersedes_report_hash: Option<ContentHash>,
    ) -> WeatherObservationFact {
        let start = Utc.with_ymd_and_hms(year, 8, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(year, 9, 1, 0, 0, 0).unwrap();
        WeatherObservationFact {
            source_id: DomainSourceId::nasa_gistemp(),
            instrument_key: DomainInstrumentKey::nasa_gistemp_loti(),
            subject_key: "global_land_ocean".to_owned(),
            local_date: NaiveDate::from_ymd_opt(year, 8, 1).expect("date"),
            report_kind: WeatherObservationReportKind::NasaGistemp,
            variable: WeatherVariable::GlobalTemperatureAnomaly,
            value,
            unit: DomainMeasurementUnit::CelsiusAnomaly,
            precision: dec!(0.01),
            observed_at: end,
            valid_from: Some(start),
            valid_to: Some(end),
            published_at,
            available_at: published_at,
            revision,
            report_hash: hash(seed),
            supersedes_report_hash,
        }
    }

    #[test]
    fn global_rank_freezes_release() {
        let subject = MarketSubject::WeatherGlobalTemperature(WeatherGlobalTemperatureSubject {
            window: WeatherContractWindow {
                start_at: Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap(),
                end_at: Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap(),
                timezone: "UTC".to_owned(),
            },
            dataset_version: 4,
            base_period_start_year: 1951,
            base_period_end_year: 1980,
            outcome: GlobalTemperatureOutcome::MonthlyRecordRank {
                rank: GlobalTemperatureRank::Exact { rank: 2 },
            },
            truth_policy: WeatherTruthPolicy::FinalOnly {
                final_source: DomainSourceId::nasa_gistemp(),
            },
        });
        let first_at = Utc.with_ymd_and_hms(2026, 9, 15, 12, 0, 0).unwrap();
        let corrected_at = Utc.with_ymd_and_hms(2026, 9, 20, 12, 0, 0).unwrap();
        let projection = project_weather_contract(
            &binding(subject),
            &WeatherFactWindow {
                decision_at: corrected_at,
                observations: vec![
                    gistemp_month_fact(
                        2024,
                        dec!(1.1),
                        Utc.with_ymd_and_hms(2024, 9, 15, 12, 0, 0).unwrap(),
                        0,
                        "2024",
                        None,
                    ),
                    gistemp_month_fact(
                        2025,
                        dec!(1.3),
                        Utc.with_ymd_and_hms(2025, 9, 15, 12, 0, 0).unwrap(),
                        0,
                        "2025",
                        None,
                    ),
                    gistemp_month_fact(2026, dec!(1.2), first_at, 0, "first", None),
                    gistemp_month_fact(
                        2026,
                        dec!(1.4),
                        corrected_at,
                        1,
                        "correction",
                        Some(hash("first")),
                    ),
                ],
                forecasts: Vec::new(),
                calibration: None,
            },
            WeatherProjectionPurpose::FinalLabel,
        )
        .expect("first GISTEMP release is final");

        assert_eq!(projection.canonical_value, dec!(1.2));
        assert_eq!(projection.comparison_value, dec!(2));
        assert!(projection.outcome);
        assert_eq!(projection.report_hashes, vec![hash("first")]);
    }

    fn temperature_subject() -> MarketSubject {
        let decision_group = WeatherDecisionGroupKey {
            temperature_statistic: WeatherTemperatureStatistic::Maximum,
            station: IcaoStation::parse("KLGA").expect("station"),
            timezone: "UTC".to_owned(),
            local_date: NaiveDate::from_ymd_opt(2026, 7, 1).expect("date"),
            market_unit: TemperatureUnit::Celsius,
            settlement_rule_url: "https://www.wunderground.com/history/daily/KLGA".to_owned(),
            finalization_policy: WeatherContractFinalizationPolicy::SourceFinalized,
            station_registry_hash: hash("registry"),
            station_profile_hash: hash("profile"),
            proxy_methodology_hash: hash("methodology"),
        };
        let decision_group_id = decision_group.decision_group_id().expect("decision group");
        MarketSubject::Weather(WeatherSubject {
            decision_group_id,
            decision_group,
            outcome_band: TemperatureBand {
                lower_inclusive: Some(dec!(31)),
                upper_inclusive: Some(dec!(31)),
            },
        })
    }

    fn temperature_fact(
        source_id: DomainSourceId,
        instrument_key: DomainInstrumentKey,
        report_kind: WeatherObservationReportKind,
        variable: WeatherVariable,
        value: Decimal,
        valid_interval: bool,
    ) -> WeatherObservationFact {
        let start = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 7, 2, 0, 0, 0).unwrap();
        WeatherObservationFact {
            source_id,
            instrument_key,
            subject_key: "KLGA".to_owned(),
            local_date: NaiveDate::from_ymd_opt(2026, 7, 1).expect("date"),
            report_kind,
            variable,
            value,
            unit: DomainMeasurementUnit::Celsius,
            precision: dec!(0.1),
            observed_at: if valid_interval {
                end
            } else {
                Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap()
            },
            valid_from: valid_interval.then_some(start),
            valid_to: valid_interval.then_some(end),
            published_at: Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap(),
            available_at: Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap(),
            revision: 0,
            report_hash: hash(&format!("{report_kind:?}-{value}")),
            supersedes_report_hash: None,
        }
    }

    #[test]
    fn temperature_requires_ghcnd() {
        let subject = temperature_subject();
        let binding = binding(subject);
        let station = IcaoStation::parse("KLGA").expect("station");
        let historical = temperature_fact(
            DomainSourceId::ghcnh(),
            DomainInstrumentKey::ghcnh(&station),
            WeatherObservationReportKind::HistoricalGhcnh,
            WeatherVariable::Temperature,
            dec!(40),
            false,
        );
        let mut window = WeatherFactWindow {
            decision_at: Utc.with_ymd_and_hms(2026, 9, 2, 0, 0, 0).unwrap(),
            observations: vec![historical],
            forecasts: Vec::new(),
            calibration: None,
        };
        let error =
            project_weather_contract(&binding, &window, WeatherProjectionPurpose::FinalLabel)
                .expect_err("hourly calibration cannot become final truth");
        assert_eq!(
            error,
            WeatherProjectionFailure::SourceUnavailable {
                role: LinkageSourceRole::Resolution,
                source_id: DomainSourceId::ghcnd(),
            }
        );

        window.observations.push(temperature_fact(
            DomainSourceId::ghcnd(),
            DomainInstrumentKey::ghcnd_temperature(&station, WeatherTemperatureStatistic::Maximum),
            WeatherObservationReportKind::GhcndDailyTemperature,
            WeatherVariable::TemperatureMaximum,
            dec!(31.2),
            true,
        ));
        let projection =
            project_weather_contract(&binding, &window, WeatherProjectionPurpose::FinalLabel)
                .expect("GHCNd daily summary is final truth");
        assert_eq!(projection.source_id, DomainSourceId::ghcnd());
        assert_eq!(projection.canonical_value, dec!(31.2));
        assert_eq!(projection.comparison_value, dec!(31));
        assert!(projection.outcome);
    }
}
