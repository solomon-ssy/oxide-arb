//! Deterministic airport daily-temperature sibling-market resolver.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    sync::LazyLock,
};

use chrono::{Datelike, NaiveDate};
use chrono_tz::Tz;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::{WeatherHistoricalBindingKind, WeatherStationProfileConfig},
    domain::quant::{
        GroundingField, GroundingKind, GroundingProof, GroundingSpan, LinkageSourceMetadata,
        MarketSubject, WeatherDecisionGroupKey, WeatherSubject,
    },
    enums::domain::ResolverTier,
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, IcaoStation, MarketId, Probability, TemperatureBand,
        TemperatureUnit, WeatherContractFinalizationPolicy, WeatherTemperatureStatistic,
    },
};
use regex::{Error as RegexError, Match, Regex};
use rust_decimal::Decimal;
use url::Url;

use super::extractor::{ExtractedCandidate, SubjectExtractor};

static BETWEEN_BAND: LazyLock<Result<Regex, RegexError>> =
    LazyLock::new(|| Regex::new(r"(?i)between\s+(-?\d+)\s*[-–]\s*(-?\d+)\s*°?\s*([FC])\b"));
static OPEN_BAND: LazyLock<Result<Regex, RegexError>> =
    LazyLock::new(|| Regex::new(r"(?i)(-?\d+)\s*°?\s*([FC])\s+or\s+(higher|above|lower|below)\b"));
static EXACT_BAND: LazyLock<Result<Regex, RegexError>> =
    LazyLock::new(|| Regex::new(r"(?i)\bbe\s+(-?\d+)\s*°?\s*([FC])\s+on\b"));
static TEMPERATURE_STATISTIC: LazyLock<Result<Regex, RegexError>> =
    LazyLock::new(|| Regex::new(r"(?i)\b(highest|lowest)\s+temperature\b"));
static QUESTION_DATE: LazyLock<Result<Regex, RegexError>> =
    LazyLock::new(|| Regex::new(r"(?i)\bon\s+([a-z]+)\s+(\d{1,2})(?:,?\s+(\d{4}))?\b"));
static SETTLEMENT_URL: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
    Regex::new(
        r"https://(?:(?:www\.)?wunderground\.com/history/daily/[^\s)]+/[A-Za-z]{4}|www\.weather\.gov/wrh/timeseries\?site=[A-Za-z]{4})",
    )
});

#[derive(Debug, Clone, Copy)]
struct WeatherDailyRegexes {
    between_band: &'static Regex,
    open_band: &'static Regex,
    exact_band: &'static Regex,
    temperature_statistic: &'static Regex,
    question_date: &'static Regex,
    settlement_url: &'static Regex,
}

impl WeatherDailyRegexes {
    fn load() -> QuantResult<Self> {
        let regexes = Self {
            between_band: required_regex(&BETWEEN_BAND, "between_band")?,
            open_band: required_regex(&OPEN_BAND, "open_band")?,
            exact_band: required_regex(&EXACT_BAND, "exact_band")?,
            temperature_statistic: required_regex(&TEMPERATURE_STATISTIC, "temperature_statistic")?,
            question_date: required_regex(&QUESTION_DATE, "question_date")?,
            settlement_url: required_regex(&SETTLEMENT_URL, "settlement_url")?,
        };
        regexes.validate_golden()?;
        Ok(regexes)
    }

    fn validate_golden(&self) -> QuantResult<()> {
        for (name, regex, sample) in [
            ("between_band", self.between_band, "between 82-83°F"),
            ("open_band", self.open_band, "65°F or below"),
            ("exact_band", self.exact_band, "be 84°F on"),
            (
                "temperature_statistic",
                self.temperature_statistic,
                "highest temperature",
            ),
            ("question_date", self.question_date, "on July 11, 2026"),
            (
                "settlement_url",
                self.settlement_url,
                "https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA",
            ),
        ] {
            if !regex.is_match(sample) {
                return Err(QuantError::config(format!(
                    "built-in daily Weather parser golden sample `{name}` no longer matches"
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
            "built-in daily Weather parser regex `{name}` failed to compile: {error}"
        ))
    })
}

/// Immutable exact-station registry available to this deployment. Profiles
/// are source-binding data, not a city→airport guess table.
#[derive(Debug, Clone, Default)]
pub struct WeatherStationRegistry {
    profiles: BTreeMap<String, WeatherStationProfileConfig>,
}

impl WeatherStationRegistry {
    /// Validate and freeze an exact ICAO-keyed station registry.
    ///
    /// # Errors
    ///
    /// Rejects malformed station ids, timezones, coordinates, duplicate
    /// historical ids, or canonical-hash failures.
    pub fn try_new(profiles: BTreeMap<String, WeatherStationProfileConfig>) -> QuantResult<Self> {
        let registry = Self { profiles };
        registry.validate()?;
        registry.registry_hash()?;
        Ok(registry)
    }

    fn get(&self, station: &IcaoStation) -> Option<&WeatherStationProfileConfig> {
        self.profiles.get(station.as_str())
    }

    #[must_use]
    pub fn has_daily_truth(&self, station: &IcaoStation) -> bool {
        self.get(station).is_some_and(|profile| {
            profile.ghcnh_station_id.is_some() && profile.ghcnd_station_id.is_some()
        })
    }

    fn validate(&self) -> QuantResult<()> {
        let mut hourly_ids = BTreeSet::new();
        let mut daily_ids = BTreeSet::new();
        for (station_key, profile) in &self.profiles {
            let station = IcaoStation::parse(station_key)
                .map_err(|error| QuantError::config(error.to_string()))?;
            if station.as_str() != station_key {
                return Err(QuantError::config(format!(
                    "weather station registry key `{station_key}` is not canonical"
                )));
            }
            if profile.timezone.parse::<Tz>().is_err() {
                return Err(QuantError::config(format!(
                    "weather station `{station_key}` has invalid IANA timezone `{}`",
                    profile.timezone
                )));
            }
            if !(-Decimal::from(90)..=Decimal::from(90)).contains(&profile.latitude)
                || !(-Decimal::from(180)..=Decimal::from(180)).contains(&profile.longitude)
            {
                return Err(QuantError::config(format!(
                    "weather station `{station_key}` has invalid coordinates"
                )));
            }
            match (
                profile.historical_binding_kind,
                profile.ghcnh_station_id.as_deref(),
                profile.ghcnd_station_id.as_deref(),
            ) {
                (
                    WeatherHistoricalBindingKind::ExactStation,
                    Some(hourly_station_id),
                    Some(daily_station_id),
                ) => {
                    if !hourly_ids.insert(hourly_station_id) {
                        return Err(QuantError::config(format!(
                            "weather station `{station_key}` reuses GHCNh id `{hourly_station_id}`"
                        )));
                    }
                    if !daily_ids.insert(daily_station_id) {
                        return Err(QuantError::config(format!(
                            "weather station `{station_key}` reuses GHCNd id `{daily_station_id}`"
                        )));
                    }
                }
                (
                    WeatherHistoricalBindingKind::OfficialNearbyProxy,
                    Some(ghcnh_station_id),
                    None,
                ) => {
                    if !hourly_ids.insert(ghcnh_station_id) {
                        return Err(QuantError::config(format!(
                            "weather station `{station_key}` reuses GHCNh id `{ghcnh_station_id}`"
                        )));
                    }
                }
                (WeatherHistoricalBindingKind::Unavailable, None, None) => {}
                _ => {
                    return Err(QuantError::config(format!(
                        "weather station `{station_key}` has an inconsistent historical binding"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Canonical identity of every deploy-frozen station profile.
    pub fn registry_hash(&self) -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_json(&("weather_station_registry_v2", &self.profiles))
            .map_err(Into::into)
    }
}

/// Canonical hash of one deploy-frozen station profile.
///
/// The resolver and every ingest path must call this function instead of
/// duplicating the hash domain tag. A mismatch would make a correctly frozen
/// linkage impossible to activate even when the profile bytes are identical.
///
/// # Errors
///
/// Propagates canonical serialization failures.
pub fn weather_station_profile_hash(
    station: &IcaoStation,
    profile: &WeatherStationProfileConfig,
) -> QuantResult<ContentHash> {
    CanonicalDigest::content_hash_json(&("weather_station_profile_v3", station, profile))
        .map_err(Into::into)
}

/// Tier-1 parser for Wunderground-provenance airport local-day maximum and
/// minimum temperature sibling groups.
pub struct WeatherDailyTemperatureExtractor {
    stations: WeatherStationRegistry,
    regexes: WeatherDailyRegexes,
}

impl WeatherDailyTemperatureExtractor {
    /// Validate all built-in expressions and bind the station registry.
    ///
    /// # Errors
    ///
    /// Returns a typed boot configuration failure when a built-in parser
    /// expression cannot compile.
    pub fn try_new(stations: WeatherStationRegistry) -> QuantResult<Self> {
        Ok(Self {
            stations,
            regexes: WeatherDailyRegexes::load()?,
        })
    }
}

impl SubjectExtractor for WeatherDailyTemperatureExtractor {
    fn tier(&self) -> ResolverTier {
        ResolverTier::Tier1Template
    }

    fn extract(&self, metadata: &LinkageSourceMetadata) -> QuantResult<Option<ExtractedCandidate>> {
        let Some((temperature_statistic, statistic_span)) =
            parse_temperature_statistic(&metadata.question, &self.regexes)
        else {
            return Ok(None);
        };
        let Some(description) = metadata.description.as_deref() else {
            return Ok(None);
        };
        let Some((description_statistic, description_statistic_span)) =
            parse_temperature_statistic(description, &self.regexes)
        else {
            return Ok(None);
        };
        if description_statistic != temperature_statistic {
            return Ok(None);
        }
        if !description.to_ascii_lowercase().contains("whole degrees") {
            return Ok(None);
        }
        let Some((finalization_policy, finalization_span)) = parse_finalization_policy(description)
        else {
            return Ok(None);
        };
        let Some(url_match) = self.regexes.settlement_url.find(description) else {
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
        let Some((band, unit, band_span)) = parse_band(&metadata.question, &self.regexes) else {
            return Ok(None);
        };
        let Some((local_date, date_span)) = parse_local_date(metadata, &self.regexes) else {
            return Ok(None);
        };
        let station_registry_hash = self.stations.registry_hash()?;
        let station_profile_hash = weather_station_profile_hash(&station, profile)?;
        let proxy_methodology_hash = CanonicalDigest::content_hash_json(&(
            "weather_daily_temperature_proxy_v2",
            temperature_statistic,
            "celsius_internal",
            "midpoint_away_from_zero",
            unit,
        ))?;
        let decision_group = WeatherDecisionGroupKey {
            temperature_statistic,
            station: station.clone(),
            timezone: profile.timezone.clone(),
            local_date,
            market_unit: unit,
            settlement_rule_url,
            finalization_policy,
            station_registry_hash,
            station_profile_hash,
            proxy_methodology_hash,
        };
        let decision_group_id = decision_group.decision_group_id()?;
        let spans = grounding_spans(
            metadata,
            url_match,
            band_span,
            date_span,
            statistic_span,
            description_statistic_span,
            finalization_span,
        );
        Ok(Some(ExtractedCandidate {
            subject: MarketSubject::Weather(WeatherSubject {
                decision_group_id,
                decision_group,
                outcome_band: band,
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
    statistic_span: (usize, usize),
    description_statistic_span: (usize, usize),
    finalization_span: (usize, usize),
) -> Vec<GroundingSpan> {
    vec![
        GroundingSpan {
            subject_field: "decision_group.station".to_owned(),
            source: GroundingField::Description,
            start: url_match.start(),
            end: url_match.end(),
            text: url_match.as_str().to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
        GroundingSpan {
            subject_field: "decision_group.settlement_rule_url".to_owned(),
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
            subject_field: "decision_group.market_unit".to_owned(),
            source: GroundingField::Question,
            start: band_span.0,
            end: band_span.1,
            text: metadata.question[band_span.0..band_span.1].to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
        GroundingSpan {
            subject_field: "decision_group.local_date".to_owned(),
            source: GroundingField::Question,
            start: date_span.0,
            end: date_span.1,
            text: metadata.question[date_span.0..date_span.1].to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
        GroundingSpan {
            subject_field: "decision_group.temperature_statistic".to_owned(),
            source: GroundingField::Question,
            start: statistic_span.0,
            end: statistic_span.1,
            text: metadata.question[statistic_span.0..statistic_span.1].to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
        GroundingSpan {
            subject_field: "decision_group.temperature_statistic".to_owned(),
            source: GroundingField::Description,
            start: description_statistic_span.0,
            end: description_statistic_span.1,
            text: metadata.description.as_deref().unwrap_or_default()
                [description_statistic_span.0..description_statistic_span.1]
                .to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
        GroundingSpan {
            subject_field: "decision_group.finalization_policy".to_owned(),
            source: GroundingField::Description,
            start: finalization_span.0,
            end: finalization_span.1,
            text: metadata.description.as_deref().unwrap_or_default()
                [finalization_span.0..finalization_span.1]
                .to_owned(),
            kind: GroundingKind::LiteralSpan,
        },
    ]
}

fn parse_temperature_statistic(
    question: &str,
    regexes: &WeatherDailyRegexes,
) -> Option<(WeatherTemperatureStatistic, (usize, usize))> {
    let captures = regexes.temperature_statistic.captures(question)?;
    let full = captures.get(0)?;
    let statistic = match captures.get(1)?.as_str().to_ascii_lowercase().as_str() {
        "highest" => WeatherTemperatureStatistic::Maximum,
        "lowest" => WeatherTemperatureStatistic::Minimum,
        _ => return None,
    };
    Some((statistic, (full.start(), full.end())))
}

fn parse_finalization_policy(
    description: &str,
) -> Option<(WeatherContractFinalizationPolicy, (usize, usize))> {
    let lowered = description.to_ascii_lowercase();
    for (phrase, policy) in [
        (
            "once information is finalized",
            WeatherContractFinalizationPolicy::SourceFinalized,
        ),
        (
            "first data point for the following date has been published",
            WeatherContractFinalizationPolicy::NextLocalDayFirstObservation,
        ),
        (
            "first datapoint for the following date has been published",
            WeatherContractFinalizationPolicy::NextLocalDayFirstObservation,
        ),
    ] {
        if let Some(start) = lowered.find(phrase) {
            return Some((policy, (start, start + phrase.len())));
        }
    }
    None
}

fn station_from_url(value: &str) -> Option<IcaoStation> {
    let url = Url::parse(value).ok()?;
    let station = match url.host_str()? {
        "wunderground.com" | "www.wunderground.com" => url.path_segments()?.next_back()?.to_owned(),
        "www.weather.gov" if url.path() == "/wrh/timeseries" => url
            .query_pairs()
            .find_map(|(key, value)| (key == "site").then(|| value.into_owned()))?,
        _ => return None,
    }
    .to_ascii_uppercase();
    IcaoStation::parse(station).ok()
}

fn parse_band(
    question: &str,
    regexes: &WeatherDailyRegexes,
) -> Option<(TemperatureBand, TemperatureUnit, (usize, usize))> {
    if let Some(captures) = regexes.between_band.captures(question) {
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
    if let Some(captures) = regexes.open_band.captures(question) {
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
        return Some((band, unit, (full.start(), full.end())));
    }
    let captures = regexes.exact_band.captures(question)?;
    let value_match = captures.get(1)?;
    let unit_match = captures.get(2)?;
    let value = value_match.as_str().parse::<Decimal>().ok()?;
    Some((
        TemperatureBand {
            lower_inclusive: Some(value),
            upper_inclusive: Some(value),
        },
        parse_unit(unit_match.as_str())?,
        (value_match.start(), unit_match.end()),
    ))
}

fn parse_unit(value: &str) -> Option<TemperatureUnit> {
    match value.to_ascii_uppercase().as_str() {
        "F" => Some(TemperatureUnit::Fahrenheit),
        "C" => Some(TemperatureUnit::Celsius),
        _ => None,
    }
}

fn parse_local_date(
    metadata: &LinkageSourceMetadata,
    regexes: &WeatherDailyRegexes,
) -> Option<(NaiveDate, (usize, usize))> {
    let captures = regexes.question_date.captures(&metadata.question)?;
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

/// One sibling supplied to whole-group validation. `yes_won` is absent while
/// the event is unresolved and must be present for every member once any
/// sibling has a final label.
#[derive(Debug, Clone)]
pub struct WeatherDecisionGroupMember {
    pub market_id: MarketId,
    pub subject: WeatherSubject,
    pub yes_won: Option<bool>,
}

/// Auditable result of validating one mutually exclusive sibling set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeatherDecisionGroupValidation {
    pub decision_group_id: ContentHash,
    pub member_count: usize,
    pub exhaustive: bool,
    pub resolution_complete: bool,
}

/// Validate sibling identity, integer-band topology and final winner
/// cardinality. Exhaustive groups must cover every whole-degree value exactly
/// once from an open lower tail through an open upper tail.
pub fn validate_weather_decision_group(
    members: &[WeatherDecisionGroupMember],
    require_exhaustive: bool,
) -> Result<WeatherDecisionGroupValidation, String> {
    let Some(first) = members.first() else {
        return Err("weather decision group is empty".to_owned());
    };
    if !first.subject.has_valid_decision_group() {
        return Err("weather decision group id does not match its canonical key".to_owned());
    }
    let mut market_ids = BTreeSet::new();
    let mut bands = Vec::with_capacity(members.len());
    let any_resolved = members.iter().any(|member| member.yes_won.is_some());
    let all_resolved = members.iter().all(|member| member.yes_won.is_some());
    if any_resolved && !all_resolved {
        return Err("weather decision group has a partial final-label snapshot".to_owned());
    }
    let mut winner_count = 0_usize;
    for member in members {
        if !market_ids.insert(member.market_id.as_str()) {
            return Err(format!(
                "weather decision group repeats market `{}`",
                member.market_id
            ));
        }
        if member.subject.decision_group_id != first.subject.decision_group_id
            || member.subject.decision_group != first.subject.decision_group
            || !member.subject.has_valid_decision_group()
        {
            return Err(format!(
                "weather sibling `{}` has a different decision-group identity",
                member.market_id
            ));
        }
        let band = &member.subject.outcome_band;
        if !band.is_valid()
            || band
                .lower_inclusive
                .is_some_and(|value| !value.fract().is_zero())
            || band
                .upper_inclusive
                .is_some_and(|value| !value.fract().is_zero())
        {
            return Err(format!(
                "weather sibling `{}` has a non-integer or invalid band",
                member.market_id
            ));
        }
        bands.push(band);
        winner_count += usize::from(member.yes_won == Some(true));
    }
    if all_resolved && winner_count != 1 {
        return Err(format!(
            "resolved weather decision group must have exactly one YES winner, got {winner_count}"
        ));
    }
    bands.sort_by(
        |left, right| match (left.lower_inclusive, right.lower_inclusive) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(left), Some(right)) => left.cmp(&right),
        },
    );
    if require_exhaustive
        && (bands
            .first()
            .and_then(|band| band.lower_inclusive)
            .is_some()
            || bands.last().and_then(|band| band.upper_inclusive).is_some())
    {
        return Err(
            "exhaustive weather decision group must have open lower and upper tails".to_owned(),
        );
    }
    for pair in bands.windows(2) {
        let left = pair[0];
        let right = pair[1];
        let Some(left_upper) = left.upper_inclusive else {
            return Err("weather decision group has a non-terminal open upper band".to_owned());
        };
        let Some(right_lower) = right.lower_inclusive else {
            return Err("weather decision group has a non-initial open lower band".to_owned());
        };
        if right_lower <= left_upper {
            return Err("weather decision group bands overlap".to_owned());
        }
        if require_exhaustive && right_lower != left_upper + Decimal::ONE {
            return Err(
                "exhaustive weather decision group has an uncovered integer gap".to_owned(),
            );
        }
    }
    Ok(WeatherDecisionGroupValidation {
        decision_group_id: first.subject.decision_group_id,
        member_count: members.len(),
        exhaustive: require_exhaustive,
        resolution_complete: all_resolved,
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::LazyLock};

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        config::{WeatherHistoricalBindingKind, WeatherStationProfileConfig},
        domain::quant::{LinkageSourceMetadata, MarketSubject, WeatherSubject},
        types::{
            IcaoStation, MarketId, WeatherContractFinalizationPolicy, WeatherTemperatureStatistic,
        },
    };
    use regex::{Error as RegexError, Regex};
    use rust_decimal_macros::dec;

    use super::{
        SubjectExtractor, WeatherDailyTemperatureExtractor, WeatherDecisionGroupMember,
        WeatherStationRegistry, required_regex, validate_weather_decision_group,
    };

    static INVALID_REGEX: LazyLock<Result<Regex, RegexError>> = LazyLock::new(|| {
        let mut pattern = String::with_capacity(1);
        pattern.push('(');
        Regex::new(&pattern)
    });

    fn station_profile() -> WeatherStationProfileConfig {
        WeatherStationProfileConfig {
            timezone: "America/New_York".to_owned(),
            latitude: dec!(40.7769),
            longitude: dec!(-73.8740),
            elevation_meters: dec!(6.4),
            ghcnh_station_id: Some("USW00014732".to_owned()),
            ghcnd_station_id: Some("USW00014732".to_owned()),
            historical_binding_kind: WeatherHistoricalBindingKind::ExactStation,
        }
    }

    #[test]
    fn regex_boot_fails() {
        assert!(required_regex(&INVALID_REGEX, "invalid_test").is_err());
    }

    impl WeatherDailyTemperatureExtractor {
        fn test_fixture() -> Self {
            Self::try_new(
                WeatherStationRegistry::try_new(BTreeMap::from([(
                    "KLGA".to_owned(),
                    station_profile(),
                )]))
                .expect("station registry"),
            )
            .expect("built-in daily Weather parser")
        }
    }

    fn metadata(question: &str, next_day_finalization: bool) -> LinkageSourceMetadata {
        let statistic = if question.to_ascii_lowercase().contains("lowest") {
            "lowest"
        } else {
            "highest"
        };
        let finalization = if next_day_finalization {
            "This market can not resolve until the first data point for the following date has \
             been published on the resolution source."
        } else {
            "The resolution source will be information from Wunderground once information is \
             finalized."
        };
        LinkageSourceMetadata {
            market_id: MarketId::new("weather"),
            slug: format!("{statistic}-temperature-in-nyc-on-july-11"),
            question: question.to_owned(),
            description: Some(format!(
                "This market will resolve to the temperature range that contains the {statistic} \
                 temperature recorded at the LaGuardia Airport Station. {finalization} The \
                 resolution source is available here: \
                 https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA. The \
                 resolution source measures temperatures to whole degrees Fahrenheit."
            )),
            series_slug: None,
            decision_group_market_ids: Vec::new(),
            end_date: Some(Utc.with_ymd_and_hms(2026, 7, 11, 23, 0, 0).unwrap()),
        }
    }

    fn subject(question: &str, next_day_finalization: bool) -> WeatherSubject {
        let candidate = WeatherDailyTemperatureExtractor::test_fixture()
            .extract(&metadata(question, next_day_finalization))
            .expect("extract")
            .expect("weather candidate");
        let MarketSubject::Weather(subject) = candidate.subject else {
            panic!("weather subject expected");
        };
        subject
    }

    #[test]
    fn resolves_maximum_minimum_bands() {
        for (question, lower, upper, statistic, finalization) in [
            (
                "Will the highest temperature in New York City be between 82-83°F on July 11?",
                Some(dec!(82)),
                Some(dec!(83)),
                WeatherTemperatureStatistic::Maximum,
                WeatherContractFinalizationPolicy::SourceFinalized,
            ),
            (
                "Will the highest temperature in New York City be 84°F on July 11?",
                Some(dec!(84)),
                Some(dec!(84)),
                WeatherTemperatureStatistic::Maximum,
                WeatherContractFinalizationPolicy::NextLocalDayFirstObservation,
            ),
            (
                "Will the lowest temperature in New York City be 65°F or below on July 11?",
                None,
                Some(dec!(65)),
                WeatherTemperatureStatistic::Minimum,
                WeatherContractFinalizationPolicy::SourceFinalized,
            ),
            (
                "Will the lowest temperature in New York City be 86°F or higher on July 11?",
                Some(dec!(86)),
                None,
                WeatherTemperatureStatistic::Minimum,
                WeatherContractFinalizationPolicy::NextLocalDayFirstObservation,
            ),
        ] {
            let subject = subject(
                question,
                finalization == WeatherContractFinalizationPolicy::NextLocalDayFirstObservation,
            );
            assert_eq!(subject.outcome_band.lower_inclusive, lower);
            assert_eq!(subject.outcome_band.upper_inclusive, upper);
            assert_eq!(subject.decision_group.station.as_str(), "KLGA");
            assert_eq!(subject.decision_group.temperature_statistic, statistic);
            assert_eq!(subject.decision_group.finalization_policy, finalization);
            assert!(subject.has_valid_decision_group());
        }
    }

    #[test]
    fn unknown_station_fails_closed() {
        let extractor =
            WeatherDailyTemperatureExtractor::try_new(WeatherStationRegistry::default())
                .expect("built-in daily Weather parser");
        assert!(
            extractor
                .extract(&metadata(
                    "Will the highest temperature in New York City be between 82-83°F on July 11?",
                    false,
                ))
                .expect("extract")
                .is_none()
        );
    }

    #[test]
    fn resolves_noaa_timeseries_parameter() {
        let extractor = WeatherDailyTemperatureExtractor::try_new(
            WeatherStationRegistry::try_new(BTreeMap::from([(
                "LTFM".to_owned(),
                WeatherStationProfileConfig {
                    timezone: "Europe/Istanbul".to_owned(),
                    latitude: dec!(41.262),
                    longitude: dec!(28.740),
                    elevation_meters: dec!(99),
                    ghcnh_station_id: Some("TUI0000LTFM".to_owned()),
                    ghcnd_station_id: Some("TUI0000LTFM".to_owned()),
                    historical_binding_kind: WeatherHistoricalBindingKind::ExactStation,
                },
            )]))
            .expect("station registry"),
        )
        .expect("built-in daily Weather parser");
        let metadata = LinkageSourceMetadata {
            market_id: MarketId::new("istanbul-temperature"),
            slug: "highest-temperature-in-istanbul-on-july-18".to_owned(),
            question: "Will the highest temperature in Istanbul be 24°C or below on July 18?"
                .to_owned(),
            description: Some(
                "This market will resolve to the temperature range that contains the highest \
                 temperature recorded at Istanbul Airport in whole degrees Celsius. This market \
                 can not resolve until the first data point for the following date has been \
                 published on the resolution source: \
                 https://www.weather.gov/wrh/timeseries?site=ltfm."
                    .to_owned(),
            ),
            series_slug: None,
            decision_group_market_ids: Vec::new(),
            end_date: Some(Utc.with_ymd_and_hms(2026, 7, 18, 23, 0, 0).unwrap()),
        };

        let candidate = extractor
            .extract(&metadata)
            .expect("extract")
            .expect("weather candidate");
        let MarketSubject::Weather(subject) = candidate.subject else {
            panic!("weather subject expected");
        };
        assert_eq!(subject.decision_group.station.as_str(), "LTFM");
        assert_eq!(subject.outcome_band.lower_inclusive, None);
        assert_eq!(subject.outcome_band.upper_inclusive, Some(dec!(24)));
        assert_eq!(
            subject.decision_group.settlement_rule_url,
            "https://www.weather.gov/wrh/timeseries?site=ltfm"
        );
        assert!(subject.has_valid_decision_group());
    }

    #[test]
    fn station_rejects_invalid_profiles() {
        let mut invalid_timezone = station_profile();
        invalid_timezone.timezone = "New_York-ish".to_owned();
        assert!(
            WeatherStationRegistry::try_new(BTreeMap::from([(
                "KLGA".to_owned(),
                invalid_timezone,
            )]))
            .is_err()
        );

        let duplicate = station_profile();
        assert!(
            WeatherStationRegistry::try_new(BTreeMap::from([
                ("KJFK".to_owned(), duplicate.clone()),
                ("KLGA".to_owned(), duplicate),
            ]))
            .is_err(),
            "one historical station id cannot silently bind two ICAO subjects"
        );

        let mut unavailable = station_profile();
        unavailable.ghcnh_station_id = None;
        unavailable.ghcnd_station_id = None;
        unavailable.historical_binding_kind = WeatherHistoricalBindingKind::Unavailable;
        let unavailable_registry = WeatherStationRegistry::try_new(BTreeMap::from([(
            "ZBAA".to_owned(),
            unavailable.clone(),
        )]))
        .expect("an explicit unavailable historical binding is valid");
        assert!(
            !unavailable_registry.has_daily_truth(&IcaoStation::parse("ZBAA").expect("station"))
        );

        unavailable.historical_binding_kind = WeatherHistoricalBindingKind::ExactStation;
        assert!(
            WeatherStationRegistry::try_new(BTreeMap::from([("ZBAA".to_owned(), unavailable,)]))
                .is_err(),
            "an exact binding cannot omit its official historical identity"
        );
    }

    #[test]
    fn validates_exhaustive_sibling_winner() {
        let questions = [
            "Will the highest temperature in New York City be 12°F or below on July 11?",
            "Will the highest temperature in New York City be 13°F on July 11?",
            "Will the highest temperature in New York City be 14°F or higher on July 11?",
        ];
        let members = questions
            .into_iter()
            .enumerate()
            .map(|(index, question)| WeatherDecisionGroupMember {
                market_id: MarketId::new(format!("weather-{index}")),
                subject: subject(question, true),
                yes_won: Some(index == 1),
            })
            .collect::<Vec<_>>();

        let validation =
            validate_weather_decision_group(&members, true).expect("valid sibling group");
        assert_eq!(validation.member_count, 3);
        assert!(validation.exhaustive);
        assert!(validation.resolution_complete);
        assert!(members.iter().all(|member| {
            member.subject.decision_group_id == members[0].subject.decision_group_id
        }));

        let mut multiple_winners = members.clone();
        multiple_winners[0].yes_won = Some(true);
        assert!(validate_weather_decision_group(&multiple_winners, true).is_err());

        let mut gap = members;
        gap[1].subject.outcome_band.lower_inclusive = Some(dec!(14));
        gap[1].subject.outcome_band.upper_inclusive = Some(dec!(14));
        assert!(validate_weather_decision_group(&gap, true).is_err());
    }
}
