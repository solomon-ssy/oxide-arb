//! Deterministic typed parsers for non-temperature Weather contracts.
//!
//! Every parser requires its family's official-source anchor before reading
//! variable fields. A recognized family with incomplete literals returns no
//! candidate and is classified as typed insufficient evidence by the catalog
//! classifier; no parser guesses a location, station, unit, or source.

use std::{ops::Range, str::FromStr, sync::LazyLock};

use chrono::{DateTime, Datelike, Months, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::{TornadoRegionBindingConfig, TornadoRegionScopeConfig, WeatherVerticalBindingsConfig},
    domain::quant::{
        GlobalTemperatureOutcome, GlobalTemperatureRank, GroundingField, GroundingKind,
        GroundingProof, GroundingSpan, LinkageSourceMetadata, MarketSubject, SeaIceAggregation,
        SeaIceHemisphere, SeaIceProduct, TropicalCycloneOutcome, WeatherAqiAggregation,
        WeatherAqiPollutant, WeatherAqiSubject, WeatherContractWindow,
        WeatherGlobalTemperatureSubject, WeatherPrecipitationSubject, WeatherRoundingRule,
        WeatherSeaIceSubject, WeatherTornadoFinalization, WeatherTornadoSubject,
        WeatherTropicalCycloneSubject, WeatherTruthPolicy, WeatherValueComparator,
        WeatherWindExtremeSubject, WeatherWindStatistic,
    },
    enums::domain::ResolverTier,
    types::{DomainInstrumentKey, DomainSourceId, IcaoStation, Probability},
};
use regex::{Captures, Error as RegexError, Regex};
use rust_decimal::Decimal;

use super::extractor::{ExtractedCandidate, SubjectExtractor};

const GISTEMP_ANCHOR: &str = "data.giss.nasa.gov/gistemp";
const HKO_CLIMATE_ANCHOR: &str = "weather.gov.hk/en/cis/climat.htm";
const NCEI_TORNADO_ANCHOR: &str = "ncei.noaa.gov/access/monitoring/tornadoes/time-series";
const NHC_SCALE_ANCHOR: &str = "nhc.noaa.gov/aboutsshws.php";
const NHC_LANDFALL_ANCHOR: &str = "nhc.noaa.gov/aboutgloss.shtml#landfall";
static PRECIPITATION_THRESHOLD: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(above|over|exceeds?|more than|at least|below|under|less than|at most)\s+(\d+(?:\.\d+)?)\s*(mm|millimeters?|inches?)\b",
    )
});
static PRECIPITATION_SUFFIX: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(\d+(?:\.\d+)?)\s*(mm|millimeters?|inches?)\s+(or more|or less)\b")
});
static PRECIPITATION_BETWEEN: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\bbetween\s+(\d+(?:\.\d+)?)\s*(?:-|and)\s*(\d+(?:\.\d+)?)\s*(mm|millimeters?|inches?)\b",
    )
});
static AQI_COMPARATOR: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(above|over|exceeds?|at least|below|under|less than|at most)\s+(\d{1,3})\b")
});
static TORNADO_COMPARATOR: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(above|over|exceeds?|more than|at least|below|under|less than|fewer than|at most)\s+(\d+)\s+tornadoes?\b",
    )
});
static TORNADO_SUFFIX: LazyLock<Result<Regex, RegexError>> =
    LazyLock::new(|| Regex::new(r"(?i)\b(\d+)\s+(or more|or fewer)\s+tornadoes?\b"));
static TORNADO_RANGE: LazyLock<Result<Regex, RegexError>> =
    LazyLock::new(|| Regex::new(r"(?i)\b(\d+)\s+to\s+(\d+)\s+tornadoes?\b"));
static TORNADO_YEAR: LazyLock<Result<Regex, RegexError>> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:in|during)\s+(\d{4})\b"));
static TORNADO_RELEASE: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(
        r"(?is)scheduled\s+to\s+be\s+released\s+on\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4}),.*?\bor\s+(\d{1,2}):(\d{2})\s+(AM|PM)\s+ET\b",
    )
});
static MONTH_YEAR: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b",
    )
});
static CYCLONE_CATEGORY: LazyLock<Result<Regex, RegexError>> =
    LazyLock::new(|| Regex::new(r"(?i)\bcategory\s+([1-5])\s+hurricane\b"));
static CYCLONE_STORM: LazyLock<Result<Regex, RegexError>> =
    LazyLock::new(|| Regex::new(r"(?i)\b([A-Z]{2}\d{6})\b"));
static GLOBAL_COMPARATOR: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(above|over|exceeds?|more than|at least|below|under|less than|at most)\s+(-?\d+(?:\.\d+)?)\s*[°º]?\s*c\b",
    )
});
static GLOBAL_BETWEEN: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\bbetween\s+(-?\d+(?:\.\d+)?)\s*[°º]?\s*c\s+and\s+(-?\d+(?:\.\d+)?)\s*[°º]?\s*c\b",
    )
});
static GLOBAL_YEAR: LazyLock<Result<Regex, RegexError>> =
    LazyLock::new(|| Regex::new(r"(?i)\b(?:will\s+)?(\d{4})\b"));
static GLOBAL_MONTH_RANK: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(r"(?i)\b(\d+)(?:st|nd|rd|th)(\s+or\s+lower)?\s+hottest\s+on\s+record\b")
});
static SEA_ICE_COMPARATOR: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(above|over|exceeds?|more than|at least|below|under|less than|at most)\s+(\d+(?:\.\d+)?)\s*(?:m|million)\s+(?:square\s+kilometers?|sq\.?\s*km|km(?:²|2))\b",
    )
});
static SEA_ICE_DATE_RANGE: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})\s+(?:through|and)\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})\b",
    )
});
static SEA_ICE_MONTH: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})\b",
    )
});
static WIND_COMPARATOR: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(
        r"(?i)\b(above|over|exceeds?|at least|below|under|less than|at most)\s+(\d+(?:\.\d+)?)\s*(knots?|kt|mph|miles per hour|km/h)\b",
    )
});

#[derive(Debug, Clone, Copy)]
struct WeatherContractRegexes {
    precipitation_threshold: &'static Regex,
    precipitation_suffix: &'static Regex,
    precipitation_between: &'static Regex,
    aqi_comparator: &'static Regex,
    tornado_comparator: &'static Regex,
    tornado_suffix: &'static Regex,
    tornado_range: &'static Regex,
    tornado_year: &'static Regex,
    tornado_release: &'static Regex,
    month_year: &'static Regex,
    cyclone_category: &'static Regex,
    cyclone_storm: &'static Regex,
    global_comparator: &'static Regex,
    global_between: &'static Regex,
    global_year: &'static Regex,
    global_month_rank: &'static Regex,
    sea_ice_comparator: &'static Regex,
    sea_ice_date_range: &'static Regex,
    sea_ice_month: &'static Regex,
    wind_comparator: &'static Regex,
}

impl WeatherContractRegexes {
    fn load() -> QuantResult<Self> {
        let regexes = Self {
            precipitation_threshold: required_regex(
                &PRECIPITATION_THRESHOLD,
                "precipitation_threshold",
            )?,
            precipitation_suffix: required_regex(&PRECIPITATION_SUFFIX, "precipitation_suffix")?,
            precipitation_between: required_regex(&PRECIPITATION_BETWEEN, "precipitation_between")?,
            aqi_comparator: required_regex(&AQI_COMPARATOR, "aqi_comparator")?,
            tornado_comparator: required_regex(&TORNADO_COMPARATOR, "tornado_comparator")?,
            tornado_suffix: required_regex(&TORNADO_SUFFIX, "tornado_suffix")?,
            tornado_range: required_regex(&TORNADO_RANGE, "tornado_range")?,
            tornado_year: required_regex(&TORNADO_YEAR, "tornado_year")?,
            tornado_release: required_regex(&TORNADO_RELEASE, "tornado_release")?,
            month_year: required_regex(&MONTH_YEAR, "month_year")?,
            cyclone_category: required_regex(&CYCLONE_CATEGORY, "cyclone_category")?,
            cyclone_storm: required_regex(&CYCLONE_STORM, "cyclone_storm")?,
            global_comparator: required_regex(&GLOBAL_COMPARATOR, "global_comparator")?,
            global_between: required_regex(&GLOBAL_BETWEEN, "global_between")?,
            global_year: required_regex(&GLOBAL_YEAR, "global_year")?,
            global_month_rank: required_regex(&GLOBAL_MONTH_RANK, "global_month_rank")?,
            sea_ice_comparator: required_regex(&SEA_ICE_COMPARATOR, "sea_ice_comparator")?,
            sea_ice_date_range: required_regex(&SEA_ICE_DATE_RANGE, "sea_ice_date_range")?,
            sea_ice_month: required_regex(&SEA_ICE_MONTH, "sea_ice_month")?,
            wind_comparator: required_regex(&WIND_COMPARATOR, "wind_comparator")?,
        };
        regexes.validate_golden()?;
        Ok(regexes)
    }

    fn validate_golden(&self) -> QuantResult<()> {
        for (name, regex, sample) in [
            (
                "precipitation_threshold",
                self.precipitation_threshold,
                "above 100 mm",
            ),
            (
                "precipitation_suffix",
                self.precipitation_suffix,
                "100 mm or more",
            ),
            (
                "precipitation_between",
                self.precipitation_between,
                "between 100 and 120 mm",
            ),
            ("aqi_comparator", self.aqi_comparator, "below 100"),
            (
                "tornado_comparator",
                self.tornado_comparator,
                "at least 100 tornadoes",
            ),
            (
                "tornado_suffix",
                self.tornado_suffix,
                "100 or fewer tornadoes",
            ),
            ("tornado_range", self.tornado_range, "100 to 120 tornadoes"),
            ("tornado_year", self.tornado_year, "during 2026"),
            (
                "tornado_release",
                self.tornado_release,
                "scheduled to be released on January 12, 2027, or 10:00 AM ET",
            ),
            ("month_year", self.month_year, "January 2026"),
            (
                "cyclone_category",
                self.cyclone_category,
                "category 3 hurricane",
            ),
            ("cyclone_storm", self.cyclone_storm, "AL012026"),
            ("global_comparator", self.global_comparator, "above 1.25 C"),
            (
                "global_between",
                self.global_between,
                "between 1.2 C and 1.3 C",
            ),
            ("global_year", self.global_year, "will 2026"),
            (
                "global_month_rank",
                self.global_month_rank,
                "2nd hottest on record",
            ),
            (
                "sea_ice_comparator",
                self.sea_ice_comparator,
                "below 4.5 million square kilometers",
            ),
            (
                "sea_ice_date_range",
                self.sea_ice_date_range,
                "January 1, 2026 through January 31, 2026",
            ),
            ("sea_ice_month", self.sea_ice_month, "January 2026"),
            ("wind_comparator", self.wind_comparator, "at least 50 knots"),
        ] {
            if !regex.is_match(sample) {
                return Err(QuantError::config(format!(
                    "built-in Weather parser golden sample `{name}` no longer matches"
                )));
            }
        }
        Ok(())
    }
}

fn required_regex(
    compiled: &'static Result<Regex, RegexError>,
    name: &'static str,
) -> QuantResult<&'static Regex> {
    compiled.as_ref().map_err(|error| {
        QuantError::config(format!(
            "built-in Weather parser regex `{name}` failed to compile: {error}"
        ))
    })
}

/// Deploy-frozen parser inputs for official location/source identities.
#[derive(Debug, Clone)]
pub struct WeatherContractExtractor {
    bindings: WeatherVerticalBindingsConfig,
    regexes: WeatherContractRegexes,
}

impl WeatherContractExtractor {
    /// Validate every built-in parser expression and freeze deploy bindings.
    ///
    /// # Errors
    ///
    /// Returns a typed boot configuration failure when any built-in parser
    /// expression cannot compile.
    pub fn try_new(bindings: &WeatherVerticalBindingsConfig) -> QuantResult<Self> {
        Ok(Self {
            bindings: bindings.clone(),
            regexes: WeatherContractRegexes::load()?,
        })
    }

    fn parse_precipitation(&self, metadata: &LinkageSourceMetadata) -> Option<ExtractedCandidate> {
        let binding = self.bindings.hko_rainfall.iter().find(|binding| {
            metadata_contains(metadata, &binding.site_key)
                && metadata_contains(metadata, HKO_CLIMATE_ANCHOR)
                && binding.timezone.parse::<Tz>().is_ok()
        })?;
        let source_span = literal_span(metadata, HKO_CLIMATE_ANCHOR, "truth_policy")?;
        let site_span = literal_span(metadata, &binding.site_key, "site_key")?;
        let station_span = entailed_from(site_span.clone(), "station_key");
        let (comparator, comparator_span) = precipitation_comparator(metadata, &self.regexes)?;
        let rounding_text = find_any(
            metadata,
            &["one decimal place", "1 decimal place", "whole millimeter"],
            "rounding",
            GroundingKind::LiteralSpan,
        )?;
        let (window, window_span) = month_window(metadata, &binding.timezone, &self.regexes)?;
        Some(ExtractedCandidate {
            subject: MarketSubject::WeatherPrecipitation(WeatherPrecipitationSubject {
                site_key: binding.site_key.clone(),
                station_key: binding.station_key.clone(),
                latitude: binding.latitude,
                longitude: binding.longitude,
                window,
                comparator,
                rounding: if rounding_text.text.to_ascii_lowercase().contains("decimal") {
                    WeatherRoundingRule::DecimalPlaces { places: 1 }
                } else {
                    WeatherRoundingRule::WholeUnit
                },
                truth_policy: WeatherTruthPolicy::ObservationWithForecast {
                    observation_source: DomainSourceId::hko_open_data(),
                    forecast_source: DomainSourceId::gefs(),
                },
            }),
            instrument_key: DomainInstrumentKey::hko_daily_rainfall(&binding.station_key),
            confidence: Probability::ONE,
            grounding: GroundingProof {
                spans: vec![
                    site_span,
                    station_span,
                    comparator_span,
                    source_span,
                    window_span,
                    rounding_text,
                ],
            },
        })
    }

    fn parse_aqi(&self, metadata: &LinkageSourceMetadata) -> Option<ExtractedCandidate> {
        let source_span = find_any(
            metadata,
            &["airnow.gov/state/?name="],
            "truth_policy",
            GroundingKind::LiteralSpan,
        )?;
        let aggregation_span = find_any(
            metadata,
            &["daily aqi for pm2.5"],
            "aggregation",
            GroundingKind::LiteralSpan,
        )?;
        let binding = self
            .bindings
            .airnow_pm25_reporting_areas
            .iter()
            .find(|binding| {
                metadata_contains(metadata, &binding.area) && binding.timezone.parse::<Tz>().is_ok()
            })?;
        let area_span = literal_span(metadata, &binding.area, "reporting_area_key")?;
        let captures = self.regexes.aqi_comparator.captures(&metadata.question)?;
        let (comparator, comparator_span) = metric_comparator(&captures, Decimal::ONE)?;
        let window = local_window(metadata.end_date?, &binding.timezone, CalendarSpan::Day)?;
        Some(ExtractedCandidate {
            subject: MarketSubject::WeatherAqi(WeatherAqiSubject {
                reporting_area_key: format!("{}:{}", binding.state, binding.area),
                window,
                comparator,
                pollutant: WeatherAqiPollutant::Pm25,
                aggregation: WeatherAqiAggregation::OfficialDailyAqi,
                truth_policy: WeatherTruthPolicy::FinalOnly {
                    final_source: DomainSourceId::airnow(),
                },
            }),
            instrument_key: DomainInstrumentKey::airnow_pm25_observation(&format!(
                "{}:{}",
                binding.state, binding.area
            )),
            confidence: Probability::ONE,
            grounding: GroundingProof {
                spans: vec![
                    area_span,
                    comparator_span,
                    aggregation_span,
                    source_span,
                    entailed_question(metadata, "window")?,
                ],
            },
        })
    }

    fn parse_tornado(&self, metadata: &LinkageSourceMetadata) -> Option<ExtractedCandidate> {
        let source_span = literal_span(metadata, NCEI_TORNADO_ANCHOR, "truth_policy")?;
        let (binding, region_span) = tornado_binding(metadata, &self.bindings.tornado_regions)?;
        let (comparator, comparator_span) = tornado_comparator(metadata, &self.regexes)?;
        let (window, window_span) = tornado_window(metadata, &binding.timezone, &self.regexes)?;
        let mut release_span = None;
        let (finalization, final_source, instrument_key) = match &binding.scope {
            TornadoRegionScopeConfig::UnitedStates => {
                let (not_before, span) = tornado_release(metadata, &self.regexes)?;
                release_span = Some(span);
                (
                    WeatherTornadoFinalization::FirstPublishedAfter { not_before },
                    DomainSourceId::ncei_tornado_time_series(),
                    DomainInstrumentKey::ncei_tornado_time_series(),
                )
            }
            TornadoRegionScopeConfig::State { .. } => (
                WeatherTornadoFinalization::StormEventsArchive,
                DomainSourceId::ncei_storm_events(),
                DomainInstrumentKey::ncei_tornado(&binding.region_id),
            ),
        };
        Some(ExtractedCandidate {
            subject: MarketSubject::WeatherTornado(WeatherTornadoSubject {
                region_key: binding.region_id.clone(),
                window,
                comparator,
                finalization,
                truth_policy: WeatherTruthPolicy::PreliminaryThenFinal {
                    preliminary_source: DomainSourceId::spc_storm_reports(),
                    final_source,
                },
            }),
            instrument_key,
            confidence: Probability::ONE,
            grounding: GroundingProof {
                spans: [
                    Some(region_span),
                    Some(comparator_span),
                    Some(source_span),
                    Some(window_span),
                    release_span,
                ]
                .into_iter()
                .flatten()
                .collect(),
            },
        })
    }

    fn parse_cyclone(&self, metadata: &LinkageSourceMetadata) -> Option<ExtractedCandidate> {
        let scale_span = literal_span(metadata, NHC_SCALE_ANCHOR, "truth_policy")?;
        let landfall_span = literal_span(metadata, NHC_LANDFALL_ANCHOR, "outcome")?;
        let storm_match = self.regexes.cyclone_storm.find(&metadata.question)?;
        let storm_key = storm_match.as_str().to_ascii_uppercase();
        let category_captures = self.regexes.cyclone_category.captures(&metadata.question)?;
        let category = category_captures.get(1)?.as_str().parse::<u8>().ok()?;
        let category_match = category_captures.get(0)?;
        let basin = cyclone_basin(&storm_key)?;
        self.bindings
            .nhc_historical_storms
            .iter()
            .find(|binding| binding.basin == basin && binding.storm_id == storm_key)?;
        let basin_span = grounding_span(
            metadata,
            GroundingField::Question,
            storm_match.range(),
            "basin",
            GroundingKind::LiteralSpan,
        )?;
        let storm_span = grounding_span(
            metadata,
            GroundingField::Question,
            storm_match.range(),
            "storm_key",
            GroundingKind::LiteralSpan,
        )?;
        let category_span = grounding_span(
            metadata,
            GroundingField::Question,
            category_match.range(),
            "outcome",
            GroundingKind::LiteralSpan,
        )?;
        let minimum_sustained_wind_knots = match category {
            1 => Decimal::from(64),
            2 => Decimal::from(83),
            3 => Decimal::from(96),
            4 => Decimal::from(113),
            5 => Decimal::from(137),
            _ => return None,
        };
        let window = local_window(metadata.end_date?, "UTC", CalendarSpan::Year)?;
        Some(ExtractedCandidate {
            subject: MarketSubject::WeatherTropicalCyclone(WeatherTropicalCycloneSubject {
                basin: basin.to_owned(),
                storm_key: storm_key.clone(),
                window,
                outcome: TropicalCycloneOutcome::LandfallAtOrAbove {
                    minimum_category: category,
                    minimum_sustained_wind_knots,
                },
                truth_policy: WeatherTruthPolicy::PreliminaryThenFinal {
                    preliminary_source: DomainSourceId::nhc_advisory(),
                    final_source: DomainSourceId::nhc_hurdat2(),
                },
            }),
            instrument_key: DomainInstrumentKey::nhc_hurdat2(basin, &storm_key),
            confidence: Probability::ONE,
            grounding: GroundingProof {
                spans: vec![
                    basin_span,
                    storm_span,
                    category_span,
                    landfall_span,
                    scale_span,
                    entailed_question(metadata, "window")?,
                ],
            },
        })
    }

    fn parse_global_temperature(
        &self,
        metadata: &LinkageSourceMetadata,
    ) -> Option<ExtractedCandidate> {
        let source_span = literal_span(metadata, GISTEMP_ANCHOR, "truth_policy")?;
        let monthly_rank = global_month_rank(metadata, &self.regexes);
        let annual_rank = global_rank(metadata);
        let (outcome, outcome_span, window, window_span, instrument_key, version_span) =
            if let Some((rank, rank_span)) = monthly_rank {
                let (window, window_span) = month_window(metadata, "UTC", &self.regexes)?;
                (
                    GlobalTemperatureOutcome::MonthlyRecordRank { rank },
                    rank_span,
                    window,
                    window_span,
                    DomainInstrumentKey::nasa_gistemp_loti(),
                    find_any(
                        metadata,
                        &["tabledata_v4", "gistemp v4"],
                        "dataset_version",
                        GroundingKind::LiteralSpan,
                    )?,
                )
            } else if let Some((rank, rank_span)) = annual_rank {
                let (window, window_span) = global_year_window(metadata, &self.regexes)?;
                (
                    GlobalTemperatureOutcome::AnnualRecordRank { rank },
                    rank_span,
                    window,
                    window_span,
                    DomainInstrumentKey::nasa_gistemp_loti_annual(),
                    entailed_from(source_span.clone(), "dataset_version"),
                )
            } else {
                if metadata_contains(metadata, "any month") {
                    return None;
                }
                let (comparator, span) = global_comparator(metadata, &self.regexes)?;
                let (window, window_span) = month_window(metadata, "UTC", &self.regexes)?;
                (
                    GlobalTemperatureOutcome::MonthlyAnomaly { comparator },
                    rename_span(span, "outcome"),
                    window,
                    window_span,
                    DomainInstrumentKey::nasa_gistemp_loti(),
                    find_any(
                        metadata,
                        &["tabledata_v4", "gistemp v4"],
                        "dataset_version",
                        GroundingKind::LiteralSpan,
                    )?,
                )
            };
        Some(ExtractedCandidate {
            subject: MarketSubject::WeatherGlobalTemperature(WeatherGlobalTemperatureSubject {
                window,
                dataset_version: 4,
                base_period_start_year: 1951,
                base_period_end_year: 1980,
                outcome,
                truth_policy: WeatherTruthPolicy::FinalOnly {
                    final_source: DomainSourceId::nasa_gistemp(),
                },
            }),
            instrument_key,
            confidence: Probability::ONE,
            grounding: GroundingProof {
                spans: vec![
                    outcome_span,
                    version_span,
                    source_span.clone(),
                    entailed_from(source_span, "base_period"),
                    window_span,
                ],
            },
        })
    }

    fn parse_sea_ice(&self, metadata: &LinkageSourceMetadata) -> Option<ExtractedCandidate> {
        let source_span = find_any(
            metadata,
            &["national snow and ice data center", "nsidc.org/data/g02135"],
            "truth_policy",
            GroundingKind::LiteralSpan,
        )?;
        let (hemisphere, hemisphere_text) = if metadata_contains(metadata, "arctic") {
            (SeaIceHemisphere::Northern, "arctic")
        } else if metadata_contains(metadata, "antarctic") {
            (SeaIceHemisphere::Southern, "antarctic")
        } else {
            return None;
        };
        let hemisphere_span = literal_span(metadata, hemisphere_text, "hemisphere")?;
        let (product, product_span, aggregation, aggregation_span, window, window_span) =
            match hemisphere {
                SeaIceHemisphere::Northern if metadata_contains(metadata, "nh-daily-extent") => {
                    let product_span = literal_span(metadata, "nh-daily-extent", "product")?;
                    let (aggregation, aggregation_span) = sea_ice_daily_aggregation(metadata)?;
                    let (window, window_span) = sea_ice_date_range(metadata, &self.regexes)?;
                    (
                        SeaIceProduct::DailyExtent,
                        product_span,
                        aggregation,
                        aggregation_span,
                        window,
                        window_span,
                    )
                }
                SeaIceHemisphere::Southern if metadata_contains(metadata, "sh-daily-extent") => {
                    let product_span = literal_span(metadata, "sh-daily-extent", "product")?;
                    let (aggregation, aggregation_span) = sea_ice_daily_aggregation(metadata)?;
                    let (window, window_span) = sea_ice_date_range(metadata, &self.regexes)?;
                    (
                        SeaIceProduct::DailyExtent,
                        product_span,
                        aggregation,
                        aggregation_span,
                        window,
                        window_span,
                    )
                }
                _ if metadata_contains(metadata, "/monthly/data/")
                    && metadata_contains(metadata, "monthly mean") =>
                {
                    let product_span = literal_span(metadata, "/monthly/data/", "product")?;
                    let aggregation_span = literal_span(metadata, "monthly mean", "aggregation")?;
                    let (window, window_span) = sea_ice_month_window(metadata, &self.regexes)?;
                    (
                        SeaIceProduct::MonthlyExtent,
                        product_span,
                        SeaIceAggregation::MonthlyMeanExtent,
                        aggregation_span,
                        window,
                        window_span,
                    )
                }
                _ => return None,
            };
        let captures = self
            .regexes
            .sea_ice_comparator
            .captures(&metadata.question)?;
        let (comparator, comparator_span) = metric_comparator(&captures, Decimal::ONE)?;
        let concentration_span =
            entailed_from(product_span.clone(), "concentration_threshold_percent");
        let version_span = entailed_from(source_span.clone(), "dataset_version");
        let instrument_hemisphere = match hemisphere {
            SeaIceHemisphere::Northern => "north",
            SeaIceHemisphere::Southern => "south",
        };
        Some(ExtractedCandidate {
            subject: MarketSubject::WeatherSeaIce(WeatherSeaIceSubject {
                hemisphere,
                product,
                aggregation,
                window,
                comparator,
                dataset_version: 4,
                concentration_threshold_percent: Decimal::from(15),
                truth_policy: WeatherTruthPolicy::FinalOnly {
                    final_source: DomainSourceId::nsidc_sea_ice_index(),
                },
            }),
            instrument_key: match product {
                SeaIceProduct::DailyExtent => {
                    DomainInstrumentKey::nsidc_daily_extent(instrument_hemisphere)
                }
                SeaIceProduct::MonthlyExtent => {
                    DomainInstrumentKey::nsidc_monthly_extent(instrument_hemisphere)
                }
            },
            confidence: Probability::ONE,
            grounding: GroundingProof {
                spans: vec![
                    hemisphere_span,
                    product_span,
                    aggregation_span,
                    comparator_span,
                    version_span,
                    source_span,
                    concentration_span,
                    window_span,
                ],
            },
        })
    }

    fn parse_wind(&self, metadata: &LinkageSourceMetadata) -> Option<ExtractedCandidate> {
        let source_span = find_any(
            metadata,
            &["api.weather.gov/stations/"],
            "truth_policy",
            GroundingKind::LiteralSpan,
        )?;
        let binding = self.bindings.nws_wind_stations.iter().find(|binding| {
            metadata_contains(metadata, &binding.station) && binding.timezone.parse::<Tz>().is_ok()
        })?;
        let station = IcaoStation::parse(&binding.station).ok()?;
        let station_span = literal_span(metadata, &binding.station, "station_key")?;
        let captures = self.regexes.wind_comparator.captures(&metadata.question)?;
        let (comparator, comparator_span) = metric_comparator(&captures, wind_scale(&captures)?)?;
        let statistic_text = if metadata_contains(metadata, "wind gust") {
            "wind gust"
        } else {
            "wind speed"
        };
        let statistic_span = literal_span(metadata, statistic_text, "statistic")?;
        let rounding_span = find_any(
            metadata,
            &["whole mile per hour", "whole knot", "one decimal place"],
            "rounding",
            GroundingKind::LiteralSpan,
        )?;
        let rounding = if rounding_span
            .text
            .to_ascii_lowercase()
            .contains("one decimal")
        {
            WeatherRoundingRule::DecimalPlaces { places: 1 }
        } else {
            WeatherRoundingRule::WholeUnit
        };
        let window = local_window(metadata.end_date?, &binding.timezone, CalendarSpan::Month)?;
        let statistic = if statistic_text == "wind gust" {
            WeatherWindStatistic::MaximumGust
        } else {
            WeatherWindStatistic::MaximumSustainedWind
        };
        Some(ExtractedCandidate {
            subject: MarketSubject::WeatherWindExtreme(WeatherWindExtremeSubject {
                station_key: binding.station.clone(),
                window,
                statistic,
                comparator,
                rounding,
                truth_policy: WeatherTruthPolicy::ObservationWithForecast {
                    observation_source: DomainSourceId::nws_observation(),
                    forecast_source: DomainSourceId::gefs(),
                },
            }),
            instrument_key: match statistic {
                WeatherWindStatistic::MaximumGust => DomainInstrumentKey::nws_wind_gust(&station),
                WeatherWindStatistic::MaximumSustainedWind => {
                    DomainInstrumentKey::nws_wind_speed(&station)
                }
            },
            confidence: Probability::ONE,
            grounding: GroundingProof {
                spans: vec![
                    station_span,
                    statistic_span,
                    comparator_span,
                    source_span,
                    rounding_span,
                    entailed_question(metadata, "window")?,
                ],
            },
        })
    }
}

impl SubjectExtractor for WeatherContractExtractor {
    fn tier(&self) -> ResolverTier {
        ResolverTier::Tier1Template
    }

    fn extract(&self, metadata: &LinkageSourceMetadata) -> QuantResult<Option<ExtractedCandidate>> {
        Ok(self
            .parse_precipitation(metadata)
            .or_else(|| self.parse_aqi(metadata))
            .or_else(|| self.parse_tornado(metadata))
            .or_else(|| self.parse_cyclone(metadata))
            .or_else(|| self.parse_global_temperature(metadata))
            .or_else(|| self.parse_sea_ice(metadata))
            .or_else(|| self.parse_wind(metadata)))
    }
}

fn precipitation_comparator(
    metadata: &LinkageSourceMetadata,
    regexes: &WeatherContractRegexes,
) -> Option<(WeatherValueComparator, GroundingSpan)> {
    if let Some(captures) = regexes.precipitation_between.captures(&metadata.question) {
        if !metadata_contains(metadata, "falls exactly between two brackets")
            || !metadata_contains(metadata, "higher bracket")
        {
            return None;
        }
        let lower = Decimal::from_str(captures.get(1)?.as_str()).ok()?;
        let upper = Decimal::from_str(captures.get(2)?.as_str()).ok()?;
        let scale = measurement_scale(captures.get(3)?.as_str())?;
        let matched = captures.get(0)?;
        return Some((
            WeatherValueComparator::Between {
                lower: lower * scale,
                upper: upper * scale,
                lower_inclusive: true,
                upper_inclusive: false,
            },
            grounding_span(
                metadata,
                GroundingField::Question,
                matched.range(),
                "comparator",
                GroundingKind::LiteralSpan,
            )?,
        ));
    }
    if let Some(captures) = regexes.precipitation_suffix.captures(&metadata.question) {
        let threshold = Decimal::from_str(captures.get(1)?.as_str()).ok()?
            * measurement_scale(captures.get(2)?.as_str())?;
        let comparator = match captures.get(3)?.as_str().to_ascii_lowercase().as_str() {
            "or more" => WeatherValueComparator::Above {
                threshold,
                inclusive: true,
            },
            "or less" => WeatherValueComparator::Below {
                threshold,
                inclusive: true,
            },
            _ => return None,
        };
        let matched = captures.get(0)?;
        return Some((
            comparator,
            grounding_span(
                metadata,
                GroundingField::Question,
                matched.range(),
                "comparator",
                GroundingKind::LiteralSpan,
            )?,
        ));
    }
    let captures = regexes
        .precipitation_threshold
        .captures(&metadata.question)?;
    metric_comparator(&captures, measurement_scale(captures.get(3)?.as_str())?)
}

fn global_comparator(
    metadata: &LinkageSourceMetadata,
    regexes: &WeatherContractRegexes,
) -> Option<(WeatherValueComparator, GroundingSpan)> {
    if let Some(captures) = regexes.global_between.captures(&metadata.question) {
        let lower = Decimal::from_str(captures.get(1)?.as_str()).ok()?;
        let upper = Decimal::from_str(captures.get(2)?.as_str()).ok()?;
        let matched = captures.get(0)?;
        return Some((
            WeatherValueComparator::Between {
                lower,
                upper,
                lower_inclusive: true,
                upper_inclusive: true,
            },
            grounding_span(
                metadata,
                GroundingField::Question,
                matched.range(),
                "comparator",
                GroundingKind::LiteralSpan,
            )?,
        ));
    }
    let captures = regexes.global_comparator.captures(&metadata.question)?;
    metric_comparator(&captures, Decimal::ONE)
}

fn global_rank(metadata: &LinkageSourceMetadata) -> Option<(GlobalTemperatureRank, GroundingSpan)> {
    let options = [
        (
            "sixth-hottest year on record or lower",
            GlobalTemperatureRank::AtLeast { rank: 6 },
        ),
        (
            "fifth-hottest year on record",
            GlobalTemperatureRank::Exact { rank: 5 },
        ),
        (
            "fourth-hottest year on record",
            GlobalTemperatureRank::Exact { rank: 4 },
        ),
        (
            "third-hottest year on record",
            GlobalTemperatureRank::Exact { rank: 3 },
        ),
        (
            "second-hottest year on record",
            GlobalTemperatureRank::Exact { rank: 2 },
        ),
        (
            "hottest year on record",
            GlobalTemperatureRank::Exact { rank: 1 },
        ),
    ];
    options.into_iter().find_map(|(text, rank)| {
        literal_span(metadata, text, "outcome")
            .and_then(|span| (span.source == GroundingField::Question).then_some((rank, span)))
    })
}

fn global_month_rank(
    metadata: &LinkageSourceMetadata,
    regexes: &WeatherContractRegexes,
) -> Option<(GlobalTemperatureRank, GroundingSpan)> {
    let captures = regexes.global_month_rank.captures(&metadata.question)?;
    let rank = captures.get(1)?.as_str().parse::<u16>().ok()?;
    if rank == 0 {
        return None;
    }
    let matched = captures.get(0)?;
    let rank = if captures.get(2).is_some() {
        GlobalTemperatureRank::AtLeast { rank }
    } else {
        GlobalTemperatureRank::Exact { rank }
    };
    Some((
        rank,
        grounding_span(
            metadata,
            GroundingField::Question,
            matched.range(),
            "outcome",
            GroundingKind::LiteralSpan,
        )?,
    ))
}

fn global_year_window(
    metadata: &LinkageSourceMetadata,
    regexes: &WeatherContractRegexes,
) -> Option<(WeatherContractWindow, GroundingSpan)> {
    let captures = regexes.global_year.captures(&metadata.question)?;
    let year = i32::from_str(captures.get(1)?.as_str()).ok()?;
    let start = NaiveDate::from_ymd_opt(year, 1, 1)?;
    let end = NaiveDate::from_ymd_opt(year.checked_add(1)?, 1, 1)?;
    let matched = captures.get(0)?;
    Some((
        WeatherContractWindow {
            start_at: start.and_hms_opt(0, 0, 0)?.and_utc(),
            end_at: end.and_hms_opt(0, 0, 0)?.and_utc(),
            timezone: "UTC".to_owned(),
        },
        grounding_span(
            metadata,
            GroundingField::Question,
            matched.range(),
            "window",
            GroundingKind::LiteralSpan,
        )?,
    ))
}

fn tornado_binding<'a>(
    metadata: &LinkageSourceMetadata,
    bindings: &'a [TornadoRegionBindingConfig],
) -> Option<(&'a TornadoRegionBindingConfig, GroundingSpan)> {
    for binding in bindings {
        let span = match &binding.scope {
            TornadoRegionScopeConfig::UnitedStates => find_any(
                metadata,
                &["United States", "U.S."],
                "region_key",
                GroundingKind::LiteralSpan,
            ),
            TornadoRegionScopeConfig::State {
                ncei_state_name, ..
            } => find_any(
                metadata,
                &[&binding.region_id, ncei_state_name],
                "region_key",
                GroundingKind::LiteralSpan,
            ),
        };
        if let Some(span) = span {
            return Some((binding, span));
        }
    }
    None
}

fn tornado_comparator(
    metadata: &LinkageSourceMetadata,
    regexes: &WeatherContractRegexes,
) -> Option<(WeatherValueComparator, GroundingSpan)> {
    if let Some(captures) = regexes.tornado_range.captures(&metadata.question) {
        let lower = Decimal::from_str(captures.get(1)?.as_str()).ok()?;
        let upper = Decimal::from_str(captures.get(2)?.as_str()).ok()?;
        let matched = captures.get(0)?;
        return Some((
            WeatherValueComparator::Between {
                lower,
                upper,
                lower_inclusive: true,
                upper_inclusive: true,
            },
            grounding_span(
                metadata,
                GroundingField::Question,
                matched.range(),
                "comparator",
                GroundingKind::LiteralSpan,
            )?,
        ));
    }
    if let Some(captures) = regexes.tornado_suffix.captures(&metadata.question) {
        let threshold = Decimal::from_str(captures.get(1)?.as_str()).ok()?;
        let comparator = match captures.get(2)?.as_str().to_ascii_lowercase().as_str() {
            "or more" => WeatherValueComparator::Above {
                threshold,
                inclusive: true,
            },
            "or fewer" => WeatherValueComparator::Below {
                threshold,
                inclusive: true,
            },
            _ => return None,
        };
        let matched = captures.get(0)?;
        return Some((
            comparator,
            grounding_span(
                metadata,
                GroundingField::Question,
                matched.range(),
                "comparator",
                GroundingKind::LiteralSpan,
            )?,
        ));
    }
    let captures = regexes.tornado_comparator.captures(&metadata.question)?;
    metric_comparator(&captures, Decimal::ONE)
}

fn tornado_window(
    metadata: &LinkageSourceMetadata,
    timezone: &str,
    regexes: &WeatherContractRegexes,
) -> Option<(WeatherContractWindow, GroundingSpan)> {
    if let Some(window) = month_window(metadata, timezone, regexes) {
        return Some(window);
    }
    let timezone = timezone.parse::<Tz>().ok()?;
    for (source, text) in source_fields(metadata) {
        let Some(captures) = regexes.tornado_year.captures(text) else {
            continue;
        };
        let year = i32::from_str(captures.get(1)?.as_str()).ok()?;
        let start = NaiveDate::from_ymd_opt(year, 1, 1)?;
        let end = NaiveDate::from_ymd_opt(year.checked_add(1)?, 1, 1)?;
        let matched = captures.get(0)?;
        return Some((
            WeatherContractWindow {
                start_at: local_midnight(timezone, start)?,
                end_at: local_midnight(timezone, end)?,
                timezone: timezone.name().to_owned(),
            },
            grounding_span(
                metadata,
                source,
                matched.range(),
                "window",
                GroundingKind::LiteralSpan,
            )?,
        ));
    }
    None
}

fn tornado_release(
    metadata: &LinkageSourceMetadata,
    regexes: &WeatherContractRegexes,
) -> Option<(DateTime<Utc>, GroundingSpan)> {
    let description = metadata.description.as_deref()?;
    let captures = regexes.tornado_release.captures(description)?;
    let month = sea_ice_month(captures.get(1)?.as_str())?;
    let day = u32::from_str(captures.get(2)?.as_str()).ok()?;
    let year = i32::from_str(captures.get(3)?.as_str()).ok()?;
    let mut hour = u32::from_str(captures.get(4)?.as_str()).ok()?;
    let minute = u32::from_str(captures.get(5)?.as_str()).ok()?;
    let meridiem = captures.get(6)?.as_str().to_ascii_uppercase();
    if !(1..=12).contains(&hour) || minute > 59 {
        return None;
    }
    hour %= 12;
    if meridiem == "PM" {
        hour = hour.checked_add(12)?;
    } else if meridiem != "AM" {
        return None;
    }
    let local = NaiveDate::from_ymd_opt(year, month, day)?.and_hms_opt(hour, minute, 0)?;
    let timezone = "America/New_York".parse::<Tz>().ok()?;
    let not_before = timezone
        .from_local_datetime(&local)
        .single()?
        .with_timezone(&Utc);
    let matched = captures.get(0)?;
    Some((
        not_before,
        grounding_span(
            metadata,
            GroundingField::Description,
            matched.range(),
            "finalization",
            GroundingKind::LiteralSpan,
        )?,
    ))
}

fn month_window(
    metadata: &LinkageSourceMetadata,
    timezone: &str,
    regexes: &WeatherContractRegexes,
) -> Option<(WeatherContractWindow, GroundingSpan)> {
    let timezone = timezone.parse::<Tz>().ok()?;
    for (source, text) in source_fields(metadata) {
        let Some(captures) = regexes.month_year.captures(text) else {
            continue;
        };
        let month = sea_ice_month(captures.get(1)?.as_str())?;
        let year = i32::from_str(captures.get(2)?.as_str()).ok()?;
        let start = NaiveDate::from_ymd_opt(year, month, 1)?;
        let end = start.checked_add_months(Months::new(1))?;
        let matched = captures.get(0)?;
        return Some((
            WeatherContractWindow {
                start_at: local_midnight(timezone, start)?,
                end_at: local_midnight(timezone, end)?,
                timezone: timezone.name().to_owned(),
            },
            grounding_span(
                metadata,
                source,
                matched.range(),
                "window",
                GroundingKind::LiteralSpan,
            )?,
        ));
    }
    None
}

fn sea_ice_daily_aggregation(
    metadata: &LinkageSourceMetadata,
) -> Option<(SeaIceAggregation, GroundingSpan)> {
    literal_span(metadata, "minimum", "aggregation").map_or_else(
        || {
            literal_span(metadata, "maximum", "aggregation")
                .map(|span| (SeaIceAggregation::MaximumDailyExtent, span))
        },
        |span| Some((SeaIceAggregation::MinimumDailyExtent, span)),
    )
}

fn sea_ice_date_range(
    metadata: &LinkageSourceMetadata,
    regexes: &WeatherContractRegexes,
) -> Option<(WeatherContractWindow, GroundingSpan)> {
    for (source, text) in source_fields(metadata) {
        let Some(captures) = regexes.sea_ice_date_range.captures(text) else {
            continue;
        };
        let start = sea_ice_date(&captures, 1, 2, 3)?;
        let inclusive_end = sea_ice_date(&captures, 4, 5, 6)?;
        if inclusive_end < start {
            return None;
        }
        let exclusive_end = inclusive_end.succ_opt()?;
        let start_at = start.and_hms_opt(0, 0, 0)?.and_utc();
        let end_at = exclusive_end.and_hms_opt(0, 0, 0)?.and_utc();
        let matched = captures.get(0)?;
        let span = grounding_span(
            metadata,
            source,
            matched.start()..matched.end(),
            "window",
            GroundingKind::LiteralSpan,
        )?;
        return Some((
            WeatherContractWindow {
                start_at,
                end_at,
                timezone: "UTC".to_owned(),
            },
            span,
        ));
    }
    None
}

fn sea_ice_month_window(
    metadata: &LinkageSourceMetadata,
    regexes: &WeatherContractRegexes,
) -> Option<(WeatherContractWindow, GroundingSpan)> {
    let captures = regexes.sea_ice_month.captures(&metadata.question)?;
    let month = sea_ice_month(captures.get(1)?.as_str())?;
    let year = i32::from_str(captures.get(2)?.as_str()).ok()?;
    let start = NaiveDate::from_ymd_opt(year, month, 1)?;
    let end = start.checked_add_months(Months::new(1))?;
    let matched = captures.get(0)?;
    let span = grounding_span(
        metadata,
        GroundingField::Question,
        matched.start()..matched.end(),
        "window",
        GroundingKind::LiteralSpan,
    )?;
    Some((
        WeatherContractWindow {
            start_at: start.and_hms_opt(0, 0, 0)?.and_utc(),
            end_at: end.and_hms_opt(0, 0, 0)?.and_utc(),
            timezone: "UTC".to_owned(),
        },
        span,
    ))
}

fn sea_ice_date(
    captures: &Captures<'_>,
    month_index: usize,
    day_index: usize,
    year_index: usize,
) -> Option<NaiveDate> {
    let month = sea_ice_month(captures.get(month_index)?.as_str())?;
    let day = u32::from_str(captures.get(day_index)?.as_str()).ok()?;
    let year = i32::from_str(captures.get(year_index)?.as_str()).ok()?;
    NaiveDate::from_ymd_opt(year, month, day)
}

fn sea_ice_month(value: &str) -> Option<u32> {
    match value.to_ascii_lowercase().as_str() {
        "january" => Some(1),
        "february" => Some(2),
        "march" => Some(3),
        "april" => Some(4),
        "may" => Some(5),
        "june" => Some(6),
        "july" => Some(7),
        "august" => Some(8),
        "september" => Some(9),
        "october" => Some(10),
        "november" => Some(11),
        "december" => Some(12),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
enum CalendarSpan {
    Day,
    Month,
    Year,
}

fn local_window(
    end_at: DateTime<Utc>,
    timezone: &str,
    span: CalendarSpan,
) -> Option<WeatherContractWindow> {
    let timezone = timezone.parse::<Tz>().ok()?;
    let local_end = end_at.with_timezone(&timezone);
    let end_date = local_end.date_naive();
    let start_date = match span {
        CalendarSpan::Day => end_date,
        CalendarSpan::Month => NaiveDate::from_ymd_opt(end_date.year(), end_date.month(), 1)?,
        CalendarSpan::Year => NaiveDate::from_ymd_opt(end_date.year(), 1, 1)?,
    };
    let next_date = match span {
        CalendarSpan::Day => start_date.succ_opt()?,
        CalendarSpan::Month => start_date.checked_add_months(Months::new(1))?,
        CalendarSpan::Year => NaiveDate::from_ymd_opt(end_date.year().checked_add(1)?, 1, 1)?,
    };
    let start_at = local_midnight(timezone, start_date)?;
    let end_at = local_midnight(timezone, next_date)?;
    Some(WeatherContractWindow {
        start_at,
        end_at,
        timezone: timezone.name().to_owned(),
    })
}

fn local_midnight(timezone: Tz, date: NaiveDate) -> Option<DateTime<Utc>> {
    timezone
        .from_local_datetime(&date.and_hms_opt(0, 0, 0)?)
        .earliest()
        .map(|value| value.with_timezone(&Utc))
}

fn metric_comparator(
    captures: &Captures<'_>,
    scale: Decimal,
) -> Option<(WeatherValueComparator, GroundingSpan)> {
    let operation = captures.get(1)?;
    let raw_threshold = captures.get(2)?;
    let threshold = Decimal::from_str(raw_threshold.as_str()).ok()? * scale;
    let comparator = match operation.as_str().to_ascii_lowercase().as_str() {
        "above" | "exceed" | "exceeds" | "more than" | "over" => WeatherValueComparator::Above {
            threshold,
            inclusive: false,
        },
        "at least" => WeatherValueComparator::Above {
            threshold,
            inclusive: true,
        },
        "below" | "fewer than" | "less than" | "under" => WeatherValueComparator::Below {
            threshold,
            inclusive: false,
        },
        "at most" => WeatherValueComparator::Below {
            threshold,
            inclusive: true,
        },
        _ => return None,
    };
    let whole_match = captures.get(0)?;
    Some((
        comparator,
        GroundingSpan {
            subject_field: "comparator".to_owned(),
            source: GroundingField::Question,
            start: whole_match.start(),
            end: whole_match.end(),
            text: whole_match.as_str().to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
    ))
}

fn measurement_scale(unit: &str) -> Option<Decimal> {
    let unit = unit.to_ascii_lowercase();
    if unit.starts_with("inch") {
        Some(Decimal::new(254, 1))
    } else if unit == "mm" || unit.starts_with("millimeter") {
        Some(Decimal::ONE)
    } else {
        None
    }
}

fn wind_scale(captures: &Captures<'_>) -> Option<Decimal> {
    match captures.get(3)?.as_str().to_ascii_lowercase().as_str() {
        "kt" | "knot" | "knots" => Some(Decimal::ONE),
        "mph" | "miles per hour" => Decimal::from_str("0.868976").ok(),
        "km/h" => Decimal::from_str("0.539957").ok(),
        _ => None,
    }
}

fn cyclone_basin(storm_key: &str) -> Option<&'static str> {
    match storm_key.get(..2)? {
        "AL" => Some("atlantic"),
        "CP" => Some("central_pacific"),
        "EP" => Some("eastern_pacific"),
        _ => None,
    }
}

fn metadata_contains(metadata: &LinkageSourceMetadata, needle: &str) -> bool {
    let needle = needle.to_ascii_lowercase();
    source_fields(metadata)
        .into_iter()
        .any(|(_, text)| text.to_ascii_lowercase().contains(&needle))
}

fn literal_span(
    metadata: &LinkageSourceMetadata,
    needle: &str,
    subject_field: &str,
) -> Option<GroundingSpan> {
    find_any(
        metadata,
        &[needle],
        subject_field,
        GroundingKind::LiteralSpan,
    )
}

fn find_any(
    metadata: &LinkageSourceMetadata,
    needles: &[&str],
    subject_field: &str,
    kind: GroundingKind,
) -> Option<GroundingSpan> {
    for (source, text) in source_fields(metadata) {
        let lowercase = text.to_ascii_lowercase();
        for needle in needles {
            if let Some(start) = lowercase.find(&needle.to_ascii_lowercase())
                && let Some(end) = start.checked_add(needle.len())
                && let Some(span) =
                    grounding_span(metadata, source, start..end, subject_field, kind)
            {
                return Some(span);
            }
        }
    }
    None
}

fn grounding_span(
    metadata: &LinkageSourceMetadata,
    source: GroundingField,
    range: Range<usize>,
    subject_field: &str,
    kind: GroundingKind,
) -> Option<GroundingSpan> {
    let source_text = source_fields(metadata)
        .into_iter()
        .find_map(|(field, text)| (field == source).then_some(text))?;
    Some(GroundingSpan {
        subject_field: subject_field.to_owned(),
        source,
        start: range.start,
        end: range.end,
        text: source_text.get(range)?.to_owned(),
        kind,
    })
}

fn entailed_question(
    metadata: &LinkageSourceMetadata,
    subject_field: &str,
) -> Option<GroundingSpan> {
    grounding_span(
        metadata,
        GroundingField::Question,
        0..metadata.question.len(),
        subject_field,
        GroundingKind::TemplateEntailed,
    )
}

fn entailed_from(mut span: GroundingSpan, subject_field: &str) -> GroundingSpan {
    subject_field.clone_into(&mut span.subject_field);
    span.kind = GroundingKind::TemplateEntailed;
    span
}

fn rename_span(mut span: GroundingSpan, subject_field: &str) -> GroundingSpan {
    subject_field.clone_into(&mut span.subject_field);
    span
}

fn source_fields(metadata: &LinkageSourceMetadata) -> Vec<(GroundingField, &str)> {
    let mut fields = vec![
        (GroundingField::Question, metadata.question.as_str()),
        (GroundingField::Slug, metadata.slug.as_str()),
    ];
    if let Some(description) = metadata.description.as_deref() {
        fields.push((GroundingField::Description, description));
    }
    if let Some(series_slug) = metadata.series_slug.as_deref() {
        fields.push((GroundingField::SeriesSlug, series_slug));
    }
    fields
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::{
        config::WeatherVerticalBindingsConfig,
        domain::quant::{
            GlobalTemperatureOutcome, GlobalTemperatureRank, LinkageSourceMetadata, MarketSubject,
            SeaIceAggregation, SeaIceProduct, WeatherTornadoFinalization,
        },
        types::{DomainInstrumentKey, MarketId},
    };
    use regex::{Error as RegexError, Regex};
    use rust_decimal_macros::dec;

    use super::{SubjectExtractor, WeatherContractExtractor, required_regex};

    static INVALID_REGEX: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
        let mut pattern = String::with_capacity(1);
        pattern.push('(');
        Regex::new(&pattern)
    });

    fn metadata(question: &str, description: &str) -> LinkageSourceMetadata {
        metadata_at(
            question,
            description,
            Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap(),
        )
    }

    fn metadata_at(
        question: &str,
        description: &str,
        end_date: DateTime<Utc>,
    ) -> LinkageSourceMetadata {
        LinkageSourceMetadata {
            market_id: MarketId::new("weather-market"),
            slug: question.to_ascii_lowercase().replace(' ', "-"),
            question: question.to_owned(),
            description: Some(description.to_owned()),
            series_slug: None,
            decision_group_market_ids: Vec::new(),
            end_date: Some(end_date),
        }
    }

    impl WeatherContractExtractor {
        fn fixture() -> Self {
            Self::try_new(&WeatherVerticalBindingsConfig::default())
                .expect("built-in Weather parser")
        }
    }

    #[test]
    fn regex_boot_fails() {
        assert!(required_regex(&INVALID_REGEX, "invalid_test").is_err());
    }

    #[test]
    fn official_daily_aqi_parses() {
        let extractor = WeatherContractExtractor::fixture();
        let candidate = extractor
            .extract(&metadata(
                "Will Philadelphia PM2.5 AQI be below 100?",
                "Use Historical Air Quality and the finalized Daily AQI for PM2.5 at \
                 https://www.airnow.gov/state/?name=Pennsylvania.",
            ))
            .expect("extract")
            .expect("candidate");
        let MarketSubject::WeatherAqi(subject) = candidate.subject else {
            panic!("AQI subject expected");
        };
        assert!(subject.comparator.includes(dec!(99)));
        assert!(!subject.comparator.includes(dec!(100)));
    }

    #[test]
    fn hourly_aqi_rejects() {
        let extractor = WeatherContractExtractor::fixture();
        let candidate = extractor
            .extract(&metadata(
                "Will East Rutherford PM2.5 AQI be above 100?",
                "Use hourly figures for the Union City High School monitor at \
                 https://gispub.epa.gov/airnow.",
            ))
            .expect("extract");
        assert!(candidate.is_none());
    }

    #[test]
    fn national_tornado_range_parses() {
        let extractor = WeatherContractExtractor::fixture();
        let candidate = extractor
            .extract(&metadata_at(
                "Will 100 to 129 tornadoes occur in the United States in July 2026?",
                "This market uses the monthly count on \
                 https://www.ncei.noaa.gov/access/monitoring/tornadoes/time-series and only the \
                 first relevant count published after the scheduled release. The relevant report \
                 is scheduled to be released on August 10, 2026, at 5:01 PM GMT+1 or 11:00 AM ET.",
                Utc.with_ymd_and_hms(2026, 8, 1, 3, 59, 0).unwrap(),
            ))
            .expect("extract")
            .expect("candidate");
        let MarketSubject::WeatherTornado(subject) = candidate.subject else {
            panic!("tornado subject expected");
        };
        assert_eq!(subject.region_key, "united_states");
        assert!(subject.comparator.includes(dec!(100)));
        assert!(subject.comparator.includes(dec!(129)));
        assert!(!subject.comparator.includes(dec!(130)));
        assert_eq!(
            subject.window.start_at,
            Utc.with_ymd_and_hms(2026, 7, 1, 4, 0, 0).unwrap()
        );
        assert_eq!(
            subject.window.end_at,
            Utc.with_ymd_and_hms(2026, 8, 1, 4, 0, 0).unwrap()
        );
        assert_eq!(
            subject.finalization,
            WeatherTornadoFinalization::FirstPublishedAfter {
                not_before: Utc.with_ymd_and_hms(2026, 8, 10, 15, 0, 0).unwrap(),
            }
        );
    }

    #[test]
    fn national_tornado_year_parses() {
        let extractor = WeatherContractExtractor::fixture();
        let candidate = extractor
            .extract(&metadata_at(
                "Will 1250 or more tornadoes occur in the United States in 2026?",
                "This market uses the final monthly counts published on \
                 https://www.ncei.noaa.gov/access/monitoring/tornadoes/time-series.",
                Utc.with_ymd_and_hms(2026, 12, 31, 0, 0, 0).unwrap(),
            ))
            .expect("extract");
        assert!(candidate.is_none());
    }

    #[test]
    fn precipitation_converts_inches() {
        let extractor = WeatherContractExtractor::fixture();
        let candidate = extractor
            .extract(&metadata_at(
                "Will Hong Kong have more than 2 inches of precipitation in July?",
                "This market resolves to the total rainfall for all days in July 2026 under the \
                 Hong Kong Observatory Daily Extract at \
                 https://www.weather.gov.hk/en/cis/climat.htm and uses one decimal place.",
                Utc.with_ymd_and_hms(2026, 7, 31, 0, 0, 0).unwrap(),
            ))
            .expect("extract")
            .expect("candidate");
        let MarketSubject::WeatherPrecipitation(subject) = candidate.subject else {
            panic!("precipitation subject expected");
        };
        assert!(subject.comparator.includes(dec!(50.9)));
        assert!(!subject.comparator.includes(dec!(50.8)));
    }

    #[test]
    fn precipitation_owns_upper_boundary() {
        let extractor = WeatherContractExtractor::fixture();
        let candidate = extractor
            .extract(&metadata_at(
                "Will Hong Kong have between 400-425mm of precipitation in July?",
                "This market resolves to the Total Rainfall (mm) for all days in July 2026 under \
                 the Hong Kong Observatory Daily Extract at \
                 https://www.weather.gov.hk/en/cis/climat.htm. If the reported data falls \
                 exactly between two brackets, this market will resolve to the higher bracket. \
                 The source measures precipitation to 1 decimal place.",
                Utc.with_ymd_and_hms(2026, 7, 31, 0, 0, 0).unwrap(),
            ))
            .expect("extract")
            .expect("candidate");
        let MarketSubject::WeatherPrecipitation(subject) = candidate.subject else {
            panic!("precipitation subject expected");
        };
        assert!(subject.comparator.includes(dec!(400)));
        assert!(subject.comparator.includes(dec!(424.9)));
        assert!(!subject.comparator.includes(dec!(425)));
        assert_eq!(
            subject.window.start_at,
            Utc.with_ymd_and_hms(2026, 6, 30, 16, 0, 0).unwrap()
        );
        assert_eq!(
            subject.window.end_at,
            Utc.with_ymd_and_hms(2026, 7, 31, 16, 0, 0).unwrap()
        );
    }

    #[test]
    fn sea_ice_minimum_range() {
        let extractor = WeatherContractExtractor::fixture();
        let candidate = extractor
            .extract(&metadata_at(
                "Will the minimum Arctic sea ice extent this summer be less than 4m square kilometers?",
                "Use the National Snow and Ice Data Center NSIDC G02135 NH-Daily-Extent product \
                 for all days between August 1, 2026 and October 1, 2026.",
                Utc.with_ymd_and_hms(2026, 10, 1, 0, 0, 0).unwrap(),
            ))
            .expect("extract")
            .expect("candidate");
        let MarketSubject::WeatherSeaIce(subject) = candidate.subject else {
            panic!("sea-ice subject expected");
        };
        assert_eq!(subject.product, SeaIceProduct::DailyExtent);
        assert_eq!(subject.aggregation, SeaIceAggregation::MinimumDailyExtent);
        assert_eq!(
            subject.window.start_at,
            Utc.with_ymd_and_hms(2026, 8, 1, 0, 0, 0).unwrap()
        );
        assert_eq!(
            subject.window.end_at,
            Utc.with_ymd_and_hms(2026, 10, 2, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn sea_ice_monthly_explicit() {
        let extractor = WeatherContractExtractor::fixture();
        let candidate = extractor
            .extract(&metadata_at(
                "Will the September 2025 monthly mean Arctic sea ice extent be below 5 million square kilometers?",
                "Use the National Snow and Ice Data Center G02135 file at \
                 https://noaadata.apps.nsidc.org/NOAA/G02135/north/monthly/data/N_09_extent_v4.0.csv.",
                Utc.with_ymd_and_hms(2025, 10, 2, 0, 0, 0).unwrap(),
            ))
            .expect("extract")
            .expect("candidate");
        let MarketSubject::WeatherSeaIce(subject) = candidate.subject else {
            panic!("sea-ice subject expected");
        };
        assert_eq!(subject.product, SeaIceProduct::MonthlyExtent);
        assert_eq!(subject.aggregation, SeaIceAggregation::MonthlyMeanExtent);
    }

    #[test]
    fn global_monthly_range_parses() {
        let extractor = WeatherContractExtractor::fixture();
        let candidate = extractor
            .extract(&metadata_at(
                "Will global temperature increase by between 1.10ºC and 1.14ºC in July 2026?",
                "Use the GLOBAL Land-Ocean Temperature Index in 0.01 degrees Celsius under the \
                 Jul column in the 2026 row at \
                 https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.txt.",
                Utc.with_ymd_and_hms(2026, 8, 1, 3, 59, 0).unwrap(),
            ))
            .expect("extract")
            .expect("candidate");
        let MarketSubject::WeatherGlobalTemperature(subject) = candidate.subject else {
            panic!("global-temperature subject expected");
        };
        let GlobalTemperatureOutcome::MonthlyAnomaly { comparator } = subject.outcome else {
            panic!("monthly outcome expected");
        };
        assert!(comparator.includes(dec!(1.10)));
        assert!(comparator.includes(dec!(1.14)));
        assert!(!comparator.includes(dec!(1.15)));
        assert_eq!(
            subject.window.start_at,
            Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap()
        );
    }

    #[test]
    fn global_monthly_rank_parses() {
        let extractor = WeatherContractExtractor::fixture();
        let candidate = extractor
            .extract(&metadata_at(
                "Will August 2026 be the 4th or lower hottest on record?",
                "Compare the Global Land-Ocean Temperature Index for August 2026 against all \
                 other Augusts under the Aug column at \
                 https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.txt.",
                Utc.with_ymd_and_hms(2026, 9, 30, 0, 0, 0).unwrap(),
            ))
            .expect("extract")
            .expect("candidate");
        let MarketSubject::WeatherGlobalTemperature(subject) = candidate.subject else {
            panic!("global-temperature subject expected");
        };
        assert_eq!(
            subject.outcome,
            GlobalTemperatureOutcome::MonthlyRecordRank {
                rank: GlobalTemperatureRank::AtLeast { rank: 4 },
            }
        );
        assert_eq!(
            candidate.instrument_key,
            DomainInstrumentKey::nasa_gistemp_loti()
        );
    }

    #[test]
    fn global_exact_rank_parses() {
        let extractor = WeatherContractExtractor::fixture();
        let candidate = extractor
            .extract(&metadata_at(
                "Will 2026 be the second-hottest year on record?",
                "Use the No_Smoothing row from the Global Land-Ocean Temperature Index at \
                 https://data.giss.nasa.gov/gistemp/graphs/graph_data/Global_Mean_Estimates_based_on_Land_and_Ocean_Data/graph.txt.",
                Utc.with_ymd_and_hms(2026, 12, 31, 0, 0, 0).unwrap(),
            ))
            .expect("extract")
            .expect("candidate");
        let MarketSubject::WeatherGlobalTemperature(subject) = candidate.subject else {
            panic!("global-temperature subject expected");
        };
        assert_eq!(
            subject.outcome,
            GlobalTemperatureOutcome::AnnualRecordRank {
                rank: GlobalTemperatureRank::Exact { rank: 2 },
            }
        );
        assert_eq!(
            candidate.instrument_key,
            DomainInstrumentKey::nasa_gistemp_loti_annual()
        );
    }

    #[test]
    fn cyclone_requires_frozen_binding() {
        let extractor = WeatherContractExtractor::fixture();
        let description = "Use https://www.nhc.noaa.gov/aboutsshws.php and the landfall \
                           definition at https://www.nhc.noaa.gov/aboutgloss.shtml#landfall.";
        let configured = extractor
            .extract(&metadata(
                "Will AL092021 make landfall as a Category 3 hurricane?",
                description,
            ))
            .expect("extract configured storm");
        let unconfigured = extractor
            .extract(&metadata(
                "Will AL012026 make landfall as a Category 3 hurricane?",
                description,
            ))
            .expect("extract unconfigured storm");

        assert!(configured.is_some());
        assert!(unconfigured.is_none());
    }

    #[test]
    fn wind_requires_truth_source() {
        let extractor = WeatherContractExtractor::fixture();
        let aviation_only = extractor
            .extract(&metadata(
                "Will the KMWN maximum wind gust be above 100 mph?",
                "Use https://aviationweather.gov/api/data and round to a whole mile per hour.",
            ))
            .expect("extract AviationWeather-only contract");
        let nws = extractor
            .extract(&metadata(
                "Will the KMWN maximum wind gust be above 100 mph?",
                "Use https://api.weather.gov/stations/KMWN/observations and round to a whole mile \
                 per hour.",
            ))
            .expect("extract NWS contract");

        assert!(aviation_only.is_none());
        assert!(nws.is_some());
    }
}
