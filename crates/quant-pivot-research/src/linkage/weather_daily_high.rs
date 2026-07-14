//! Deterministic airport daily-high weather-market resolver.

use std::{collections::BTreeMap, sync::LazyLock};

use chrono::{Datelike, NaiveDate};
use chrono_tz::Tz;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    config::WeatherStationProfileConfig,
    domain::{
        GroundingField, GroundingKind, GroundingProof, GroundingSpan, LinkageSourceMetadata,
        MarketSubject, WeatherSubject,
    },
    enums::domain::ResolverTier,
    hashing::CanonicalDigest,
    types::{DomainInstrumentKey, IcaoStation, Probability, TemperatureBand, TemperatureUnit},
};
use regex::{Match, Regex};
use rust_decimal::Decimal;
use url::Url;

use super::extractor::{ExtractedCandidate, SubjectExtractor};

static BETWEEN_BAND: LazyLock<Option<Regex>> =
    LazyLock::new(|| Regex::new(r"(?i)between\s+(-?\d+)\s*[-–]\s*(-?\d+)\s*°?\s*([FC])\b").ok());
static OPEN_BAND: LazyLock<Option<Regex>> = LazyLock::new(|| {
    Regex::new(r"(?i)(-?\d+)\s*°?\s*([FC])\s+or\s+(higher|above|lower|below)\b").ok()
});
static QUESTION_DATE: LazyLock<Option<Regex>> =
    LazyLock::new(|| Regex::new(r"(?i)\bon\s+([a-z]+)\s+(\d{1,2})(?:,?\s+(\d{4}))?\b").ok());
static SETTLEMENT_URL: LazyLock<Option<Regex>> = LazyLock::new(|| {
    Regex::new(r"https://(?:www\.)?wunderground\.com/history/daily/[^\s)]+/[A-Za-z]{4}").ok()
});

/// Exact station profiles available in this deployment. Profiles are data,
/// not a hard-coded city→airport guess table.
#[derive(Debug, Clone, Default)]
pub struct WeatherStationCatalog {
    profiles: BTreeMap<String, WeatherStationProfileConfig>,
}

impl WeatherStationCatalog {
    #[must_use]
    pub const fn new(profiles: BTreeMap<String, WeatherStationProfileConfig>) -> Self {
        Self { profiles }
    }

    fn get(&self, station: &IcaoStation) -> Option<&WeatherStationProfileConfig> {
        self.profiles.get(station.as_str())
    }
}

/// Tier-1 parser for the one Weather vertical currently implemented:
/// Wunderground-finalized airport local-day maximum-temperature bands.
pub struct WeatherDailyHighExtractor {
    stations: WeatherStationCatalog,
}

impl WeatherDailyHighExtractor {
    #[must_use]
    pub const fn new(stations: WeatherStationCatalog) -> Self {
        Self { stations }
    }
}

impl SubjectExtractor for WeatherDailyHighExtractor {
    fn tier(&self) -> ResolverTier {
        ResolverTier::Tier1Template
    }

    fn extract(&self, metadata: &LinkageSourceMetadata) -> QuantResult<Option<ExtractedCandidate>> {
        if !metadata
            .question
            .to_ascii_lowercase()
            .contains("highest temperature")
        {
            return Ok(None);
        }
        let Some(description) = metadata.description.as_deref() else {
            return Ok(None);
        };
        if !description.contains("once information is finalized")
            || !description.to_ascii_lowercase().contains("whole degrees")
        {
            return Ok(None);
        }
        let Some(url_match) = SETTLEMENT_URL
            .as_ref()
            .and_then(|regex| regex.find(description))
        else {
            return Ok(None);
        };
        let settlement_rule_url = url_match.as_str().trim_end_matches('.').to_owned();
        let Some(station) = station_from_url(&settlement_rule_url) else {
            return Ok(None);
        };
        let Some(profile) = self.stations.get(&station) else {
            return Ok(None);
        };
        if profile.timezone.parse::<Tz>().is_err() {
            return Ok(None);
        }
        let Some((band, unit, band_span)) = parse_band(&metadata.question) else {
            return Ok(None);
        };
        let Some((local_date, date_span)) = parse_local_date(metadata) else {
            return Ok(None);
        };
        let station_profile_hash =
            CanonicalDigest::content_hash_json(&("weather_station_profile_v1", &station, profile))?;
        let proxy_methodology_hash = CanonicalDigest::content_hash_json(&(
            "weather_whole_degree_proxy_v1",
            "celsius_internal",
            "midpoint_away_from_zero",
            unit,
        ))?;
        let spans = grounding_spans(metadata, url_match, band_span, date_span);
        Ok(Some(ExtractedCandidate {
            subject: MarketSubject::Weather(WeatherSubject {
                station: station.clone(),
                timezone: profile.timezone.clone(),
                local_date,
                outcome_band: band,
                market_unit: unit,
                settlement_rule_url,
                station_profile_hash,
                proxy_methodology_hash,
            }),
            instrument_key: DomainInstrumentKey::aviation_weather(&station),
            confidence: Probability::ONE,
            grounding: GroundingProof { spans },
        }))
    }
}

fn grounding_spans(
    metadata: &LinkageSourceMetadata,
    url_match: Match<'_>,
    band_span: (usize, usize),
    date_span: (usize, usize),
) -> Vec<GroundingSpan> {
    vec![
        GroundingSpan {
            subject_field: "station".to_owned(),
            source: GroundingField::Description,
            start: url_match.start(),
            end: url_match.end(),
            text: url_match.as_str().to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
        GroundingSpan {
            subject_field: "settlement_rule_url".to_owned(),
            source: GroundingField::Description,
            start: url_match.start(),
            end: url_match.end(),
            text: url_match.as_str().to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
        GroundingSpan {
            subject_field: "outcome_band".to_owned(),
            source: GroundingField::Question,
            start: band_span.0,
            end: band_span.1,
            text: metadata.question[band_span.0..band_span.1].to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
        GroundingSpan {
            subject_field: "market_unit".to_owned(),
            source: GroundingField::Question,
            start: band_span.0,
            end: band_span.1,
            text: metadata.question[band_span.0..band_span.1].to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
        GroundingSpan {
            subject_field: "local_date".to_owned(),
            source: GroundingField::Question,
            start: date_span.0,
            end: date_span.1,
            text: metadata.question[date_span.0..date_span.1].to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
    ]
}

fn station_from_url(value: &str) -> Option<IcaoStation> {
    let url = Url::parse(value).ok()?;
    if !url
        .host_str()
        .is_some_and(|host| host == "wunderground.com" || host == "www.wunderground.com")
    {
        return None;
    }
    let station = url.path_segments()?.next_back()?.to_ascii_uppercase();
    IcaoStation::parse(station).ok()
}

fn parse_band(question: &str) -> Option<(TemperatureBand, TemperatureUnit, (usize, usize))> {
    if let Some(captures) = BETWEEN_BAND
        .as_ref()
        .and_then(|regex| regex.captures(question))
    {
        let full = captures.get(0)?;
        let lower = captures.get(1)?.as_str().parse::<Decimal>().ok()?;
        let upper = captures.get(2)?.as_str().parse::<Decimal>().ok()?;
        let unit = parse_unit(captures.get(3)?.as_str())?;
        let band = TemperatureBand {
            lower_inclusive: Some(lower),
            upper_inclusive: Some(upper),
        };
        return band
            .is_valid()
            .then_some((band, unit, (full.start(), full.end())));
    }
    let captures = OPEN_BAND.as_ref()?.captures(question)?;
    let full = captures.get(0)?;
    let bound = captures.get(1)?.as_str().parse::<Decimal>().ok()?;
    let unit = parse_unit(captures.get(2)?.as_str())?;
    let direction = captures.get(3)?.as_str().to_ascii_lowercase();
    let band = if matches!(direction.as_str(), "higher" | "above") {
        TemperatureBand {
            lower_inclusive: Some(bound),
            upper_inclusive: None,
        }
    } else {
        TemperatureBand {
            lower_inclusive: None,
            upper_inclusive: Some(bound),
        }
    };
    Some((band, unit, (full.start(), full.end())))
}

fn parse_unit(value: &str) -> Option<TemperatureUnit> {
    match value.to_ascii_uppercase().as_str() {
        "F" => Some(TemperatureUnit::Fahrenheit),
        "C" => Some(TemperatureUnit::Celsius),
        _ => None,
    }
}

fn parse_local_date(metadata: &LinkageSourceMetadata) -> Option<(NaiveDate, (usize, usize))> {
    let captures = QUESTION_DATE.as_ref()?.captures(&metadata.question)?;
    let full = captures.get(0)?;
    let month = month_number(captures.get(1)?.as_str())?;
    let day = captures.get(2)?.as_str().parse::<u32>().ok()?;
    let year = captures
        .get(3)
        .and_then(|value| value.as_str().parse::<i32>().ok())
        .or_else(|| metadata.end_date.map(|end| end.year()))?;
    NaiveDate::from_ymd_opt(year, month, day).map(|date| (date, (full.start(), full.end())))
}

fn month_number(value: &str) -> Option<u32> {
    match value.to_ascii_lowercase().as_str() {
        "january" | "jan" => Some(1),
        "february" | "feb" => Some(2),
        "march" | "mar" => Some(3),
        "april" | "apr" => Some(4),
        "may" => Some(5),
        "june" | "jun" => Some(6),
        "july" | "jul" => Some(7),
        "august" | "aug" => Some(8),
        "september" | "sep" | "sept" => Some(9),
        "october" | "oct" => Some(10),
        "november" | "nov" => Some(11),
        "december" | "dec" => Some(12),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        config::WeatherStationProfileConfig,
        domain::{LinkageSourceMetadata, MarketSubject},
        types::MarketId,
    };
    use rust_decimal_macros::dec;

    use super::{SubjectExtractor, WeatherDailyHighExtractor, WeatherStationCatalog};

    fn extractor() -> WeatherDailyHighExtractor {
        WeatherDailyHighExtractor::new(WeatherStationCatalog::new(BTreeMap::from([(
            "KLGA".to_owned(),
            WeatherStationProfileConfig {
                timezone: "America/New_York".to_owned(),
                latitude: dec!(40.7769),
                longitude: dec!(-73.8740),
                elevation_meters: dec!(6.4),
                ghcnh_station_id: "USW00014732".to_owned(),
            },
        )])))
    }

    fn metadata(question: &str) -> LinkageSourceMetadata {
        LinkageSourceMetadata {
            market_id: MarketId::new("weather"),
            slug: "highest-temperature-in-nyc-on-july-11".to_owned(),
            question: question.to_owned(),
            description: Some(
                "This market will resolve to the temperature range that contains the highest \
                 temperature recorded at the LaGuardia Airport Station. The resolution source \
                 will be information from Wunderground once information is finalized, available \
                 here: https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA. \
                 The resolution source measures temperatures to whole degrees Fahrenheit."
                    .to_owned(),
            ),
            series_slug: None,
            end_date: Some(Utc.with_ymd_and_hms(2026, 7, 11, 23, 0, 0).unwrap()),
        }
    }

    #[test]
    fn resolves_closed_and_open_temperature_bands() {
        for (question, lower, upper) in [
            (
                "Will the highest temperature in New York City be between 82-83°F on July 11?",
                Some(dec!(82)),
                Some(dec!(83)),
            ),
            (
                "Will the highest temperature in New York City be 84°F or higher on July 11?",
                Some(dec!(84)),
                None,
            ),
            (
                "Will the highest temperature in New York City be 65°F or below on July 11?",
                None,
                Some(dec!(65)),
            ),
        ] {
            let candidate = extractor()
                .extract(&metadata(question))
                .expect("extract")
                .expect("weather candidate");
            let MarketSubject::Weather(subject) = candidate.subject else {
                panic!("weather subject expected");
            };
            assert_eq!(subject.outcome_band.lower_inclusive, lower);
            assert_eq!(subject.outcome_band.upper_inclusive, upper);
            assert_eq!(subject.station.as_str(), "KLGA");
        }
    }

    #[test]
    fn unknown_station_fails_closed() {
        let extractor = WeatherDailyHighExtractor::new(WeatherStationCatalog::default());
        assert!(
            extractor
                .extract(&metadata(
                    "Will the highest temperature in New York City be between 82-83°F on July 11?"
                ))
                .expect("extract")
                .is_none()
        );
    }
}
