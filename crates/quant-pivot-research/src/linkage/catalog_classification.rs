//! Deterministic full-catalog capability classification.
//!
//! Classification runs before continuous linkage reconciliation. Serving still
//! requires a fully grounded [`MarketSubject`], while every active
//! Crypto/Weather catalog member receives an explicit capability disposition
//! even when its deterministic parser is not implemented yet.

use std::collections::{BTreeMap, BTreeSet};

use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::WeatherVerticalBindingsConfig,
    domain::{
        market::{EventInfo, MarketInfo},
        quant::{
            LinkageOutcome, LinkageSourceMetadata, MarketSubject, PriceComparator, ResolutionOracle,
        },
    },
    enums::{common::MarketCategory, domain::DomainFamily},
    hashing::CanonicalDigest,
    types::{
        EventId, WEATHER_FORECAST_24H_PROFILE_ID, builtin_research_profiles,
        domain_capability::{
            DomainCapabilityReasonCode, DomainCapabilityRegistryArtifact, DomainContractFamily,
        },
        domain_classification::{
            DomainCatalogClassificationArtifact, DomainMarketClassification,
            DomainMarketClassificationOutcome,
        },
    },
};

use super::{
    LayeredResolver,
    capability_registry::domain_capability_registry,
    ruleset::{find_alias, rule_for_alias},
    weather_daily_temperature::WeatherStationRegistry,
};

/// Pure catalog classifier bound to one resolver and capability registry.
pub struct DomainCatalogClassifier {
    resolver: LayeredResolver,
    weather_stations: WeatherStationRegistry,
    capability_registry: DomainCapabilityRegistryArtifact,
    minimum_mature_weather_groups: u64,
}

impl DomainCatalogClassifier {
    /// Bind one classifier to the deploy's immutable Weather station registry.
    pub fn new(
        weather_stations: WeatherStationRegistry,
        weather_vertical_bindings: &WeatherVerticalBindingsConfig,
    ) -> QuantResult<Self> {
        let minimum_mature_weather_groups = builtin_research_profiles()
            .map_err(QuantError::config)?
            .into_iter()
            .find(|profile| profile.profile_ref.id.as_str() == WEATHER_FORECAST_24H_PROFILE_ID)
            .map(|profile| profile.spec.feedback_policy.minimum_mature_labels)
            .ok_or_else(|| QuantError::config("Weather research profile is missing"))?;
        let capability_registry = domain_capability_registry(
            &weather_stations.registry_hash()?,
            weather_vertical_bindings,
        )?;
        let resolver = LayeredResolver::deterministic(weather_stations.clone());
        Ok(Self {
            resolver,
            weather_stations,
            capability_registry,
            minimum_mature_weather_groups,
        })
    }

    /// Classify every supplied active Crypto/Weather market and seal the result.
    ///
    /// Markets outside those two primary categories are ignored. A missing
    /// event is a hard catalog-integrity error, never an exclusion guess.
    pub fn classify_catalog(
        &self,
        markets: &[MarketInfo],
        events: &BTreeMap<EventId, EventInfo>,
    ) -> QuantResult<DomainCatalogClassificationArtifact> {
        let mature_weather_groups = mature_weather_decision_group_counts(markets, events)?;
        let mut classifications = Vec::new();
        for market in markets {
            let Some(family) = domain_family(market)? else {
                continue;
            };
            let event = events.get(&market.event_id).ok_or_else(|| {
                QuantError::config(format!(
                    "active domain market {} references missing event {}",
                    market.market_id, market.event_id
                ))
            })?;
            classifications.push(self.classify_market(
                market,
                event,
                family,
                &mature_weather_groups,
            )?);
        }
        let artifact = DomainCatalogClassificationArtifact::new(
            self.resolver.resolver_version(),
            self.capability_registry.registry_hash,
            classifications,
        )?;
        artifact.validate().map_err(QuantError::config)?;
        Ok(artifact)
    }

    fn classify_market(
        &self,
        market: &MarketInfo,
        event: &EventInfo,
        family: DomainFamily,
        mature_weather_groups: &BTreeMap<DomainContractFamily, u64>,
    ) -> QuantResult<DomainMarketClassification> {
        let metadata_hash = CanonicalDigest::content_hash_json(&(
            "domain_catalog_classification_input_v1",
            &market.market_id,
            &market.content_hash,
            &event.event_id,
            &event.content_hash,
        ))?;
        let metadata = linkage_metadata(market, event);
        let (contract_family, outcome) = match family {
            DomainFamily::Crypto => self.classify_crypto(&metadata, market, event)?,
            DomainFamily::Weather => {
                self.classify_weather(&metadata, market, event, mature_weather_groups)?
            }
        };
        Ok(DomainMarketClassification {
            market_id: market.market_id.clone(),
            family,
            contract_family,
            outcome,
            metadata_hash,
        })
    }

    fn classify_crypto(
        &self,
        metadata: &LinkageSourceMetadata,
        market: &MarketInfo,
        event: &EventInfo,
    ) -> QuantResult<(
        Option<DomainContractFamily>,
        DomainMarketClassificationOutcome,
    )> {
        let text = catalog_text(market, event);
        if let Some(reason_code) = crypto_exclusion_reason(&text) {
            return Ok((
                None,
                DomainMarketClassificationOutcome::Excluded { reason_code },
            ));
        }
        if looks_like_crypto_price_contract(&text) && find_alias(&text).is_none() {
            return Ok((
                None,
                DomainMarketClassificationOutcome::Excluded {
                    reason_code: DomainCapabilityReasonCode::CryptoUnsupportedAsset,
                },
            ));
        }
        let result = self.resolver.resolve(metadata, market.updated_at)?;
        if let LinkageOutcome::Resolved(binding) = result.outcome {
            let MarketSubject::Crypto(subject) = &binding.subject else {
                return Ok((
                    None,
                    DomainMarketClassificationOutcome::UnsupportedTemplate {
                        reason_code: DomainCapabilityReasonCode::CategorySubjectMismatch,
                    },
                ));
            };
            let contract_family = crypto_contract_family(subject.comparator);
            let credential_blocked =
                matches!(
                    subject.resolution_oracle,
                    ResolutionOracle::ChainlinkDataStreams { .. }
                ) && rule_for_alias(&subject.asset.as_str().to_ascii_lowercase())
                    .is_some_and(|rule| !rule.public_rtds_supported());
            let outcome = if credential_blocked {
                DomainMarketClassificationOutcome::CredentialBlocked {
                    reason_code:
                        DomainCapabilityReasonCode::ChainlinkDataStreamsCredentialsRequired,
                }
            } else {
                DomainMarketClassificationOutcome::Supported
            };
            return Ok((Some(contract_family), outcome));
        }
        if looks_like_crypto_price_contract(&text) {
            Ok((
                None,
                DomainMarketClassificationOutcome::UnsupportedTemplate {
                    reason_code: DomainCapabilityReasonCode::CryptoPriceTemplateNotGrounded,
                },
            ))
        } else {
            Ok((
                None,
                DomainMarketClassificationOutcome::Excluded {
                    reason_code: DomainCapabilityReasonCode::CryptoNonPriceContract,
                },
            ))
        }
    }

    fn classify_weather(
        &self,
        metadata: &LinkageSourceMetadata,
        market: &MarketInfo,
        event: &EventInfo,
        mature_weather_groups: &BTreeMap<DomainContractFamily, u64>,
    ) -> QuantResult<(
        Option<DomainContractFamily>,
        DomainMarketClassificationOutcome,
    )> {
        let text = catalog_text(market, event);
        let tags = normalized_tags(event);
        if let Some(reason_code) = pre_family_weather_exclusion_reason(&text, &tags) {
            return Ok((
                None,
                DomainMarketClassificationOutcome::Excluded { reason_code },
            ));
        }
        let Some(contract_family) = detect_weather_contract_family(&text, &tags) else {
            return Ok((
                None,
                DomainMarketClassificationOutcome::Excluded {
                    reason_code: weather_exclusion_reason(&text, &tags),
                },
            ));
        };
        if contract_family == DomainContractFamily::WeatherDailyTemperature
            && is_hko_fractional_daily_temperature(&text)
        {
            return Ok((
                Some(contract_family),
                DomainMarketClassificationOutcome::Excluded {
                    reason_code:
                        DomainCapabilityReasonCode::WeatherAmbiguousFractionalBucketOwnership,
                },
            ));
        }
        if contract_family != DomainContractFamily::WeatherDailyTemperature {
            if grounded_weather_research_contract(contract_family, &text) {
                let mature_groups = mature_weather_groups
                    .get(&contract_family)
                    .copied()
                    .unwrap_or(0);
                if mature_groups < self.minimum_mature_weather_groups {
                    return Ok((
                        Some(contract_family),
                        DomainMarketClassificationOutcome::InsufficientEvidence {
                            reason_code: DomainCapabilityReasonCode::MatureLabelsUnavailable,
                        },
                    ));
                }
            }
            return Ok((
                Some(contract_family),
                DomainMarketClassificationOutcome::UnsupportedTemplate {
                    reason_code:
                        DomainCapabilityReasonCode::RecognizedWeatherFamilyParserUnavailable,
                },
            ));
        }

        let result = self.resolver.resolve(metadata, market.updated_at)?;
        let outcome = match result.outcome {
            LinkageOutcome::Resolved(binding) => match binding.subject {
                MarketSubject::Weather(subject)
                    if self
                        .weather_stations
                        .has_historical_calibration(&subject.decision_group.station) =>
                {
                    DomainMarketClassificationOutcome::Supported
                }
                MarketSubject::Weather(_) => {
                    DomainMarketClassificationOutcome::InsufficientEvidence {
                        reason_code:
                            DomainCapabilityReasonCode::WeatherHistoricalCalibrationUnavailable,
                    }
                }
                MarketSubject::Crypto(_) => {
                    DomainMarketClassificationOutcome::UnsupportedTemplate {
                        reason_code: DomainCapabilityReasonCode::CategorySubjectMismatch,
                    }
                }
            },
            LinkageOutcome::Unresolved { .. } => {
                DomainMarketClassificationOutcome::UnsupportedTemplate {
                    reason_code:
                        DomainCapabilityReasonCode::WeatherDailyTemperatureTemplateNotGrounded,
                }
            }
        };
        Ok((Some(contract_family), outcome))
    }
}

fn is_hko_fractional_daily_temperature(text: &str) -> bool {
    text.contains("hong kong observatory")
        && text.contains("weather.gov.hk/en/cis/climat.htm")
        && text.contains("one decimal place")
        && contains_any(text, &["absolute daily max", "absolute daily min"])
}

fn grounded_weather_research_contract(contract_family: DomainContractFamily, text: &str) -> bool {
    match contract_family {
        DomainContractFamily::WeatherPrecipitation => {
            text.contains("precipitation")
                && contains_any(
                    text,
                    &[
                        "weather.gov.hk/en/cis/climat.htm",
                        "metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/heathrowdata.txt",
                        "weather.gov/wrh/climate?wfo=okx",
                        "weather.gov/wrh/climate?wfo=sew",
                        "data.kma.go.kr/climate/rankstate/selectrankstatisticsdivisionlist.do",
                    ],
                )
                && text.contains("resolution source")
                && text.contains("decimal place")
        }
        DomainContractFamily::WeatherAqi => {
            (text.contains("new york city region")
                && text.contains("pm2.5")
                && text.contains("below 100")
                && text.contains("airnow.gov/state/?name=new-york"))
                || grounded_airnow_pm25_daily_city_contract(text)
                || (text.contains("highest pm2.5 air quality index")
                    && text.contains("east rutherford, new jersey")
                    && text.contains("between the opening kickoff and the end of gameplay")
                    && text.contains("airnow.gov/?city=east%20rutherford&state=nj&country=usa")
                    && text.contains("union city high school monitor")
                    && text.contains("gispub.epa.gov/airnow"))
        }
        DomainContractFamily::WeatherTornado => {
            text.contains("ncei.noaa.gov/access/monitoring/tornadoes/time-series")
                && text.contains("number of tornadoes recorded in the united states")
                && text.contains("first relevant tornado count published")
        }
        DomainContractFamily::WeatherTropicalCyclone => {
            text.contains("nhc.noaa.gov/aboutsshws.php")
                && text.contains("nhc.noaa.gov/aboutgloss.shtml#landfall")
                && text.contains("official national hurricane center advisories")
                && contains_any(text, &["category 4 hurricane", "category 5 hurricane"])
        }
        DomainContractFamily::WeatherGlobalTemperature => {
            text.contains("global land-ocean temperature index")
                && text.contains("data.giss.nasa.gov/gistemp")
                && contains_any(
                    text,
                    &[
                        "global temperature increase",
                        "hottest on record",
                        "hottest year",
                        "hottest years",
                    ],
                )
        }
        DomainContractFamily::WeatherSeaIce => {
            text.contains("minimum arctic sea ice extent")
                && text.contains("national snow and ice data center")
                && text.contains("nh-daily-extent")
                && text.contains("august 1, 2026")
                && text.contains("october 1, 2026")
        }
        DomainContractFamily::WeatherWindExtreme => {
            text.contains("highest wind speed")
                && text.contains(
                    "mountwashington.org/weather/mount-washington-weather-archives/monthly-f6",
                )
                && text.contains("f6 reports")
                && text.contains("whole mile per hour")
        }
        DomainContractFamily::CryptoDirection
        | DomainContractFamily::CryptoThreshold
        | DomainContractFamily::CryptoBand
        | DomainContractFamily::WeatherDailyTemperature => false,
    }
}

fn grounded_airnow_pm25_daily_city_contract(text: &str) -> bool {
    let exact_source = [
        ("philadelphia", "airnow.gov/state/?name=pennsylvania"),
        ("columbus", "airnow.gov/state/?name=ohio"),
        ("chicago", "airnow.gov/state/?name=illinois"),
    ]
    .iter()
    .any(|(city, source)| text.contains(city) && text.contains(source));
    exact_source
        && text.contains("pm2.5")
        && text.contains("below 100")
        && text.contains("historical air quality")
        && text.contains("daily aqi for pm2.5")
}

fn mature_weather_decision_group_counts(
    markets: &[MarketInfo],
    events: &BTreeMap<EventId, EventInfo>,
) -> QuantResult<BTreeMap<DomainContractFamily, u64>> {
    let mut event_members = BTreeMap::<EventId, Vec<&MarketInfo>>::new();
    for market in markets {
        if domain_family(market)? == Some(DomainFamily::Weather) {
            event_members
                .entry(market.event_id.clone())
                .or_default()
                .push(market);
        }
    }
    let mut mature_events = BTreeMap::<DomainContractFamily, BTreeSet<EventId>>::new();
    for (event_id, members) in event_members {
        if members
            .iter()
            .any(|market| market.resolved_at.is_none() || market.outcome.is_none())
        {
            continue;
        }
        let event = events.get(&event_id).ok_or_else(|| {
            QuantError::config(format!(
                "resolved Weather group references missing event {event_id}"
            ))
        })?;
        let tags = normalized_tags(event);
        let families = members
            .iter()
            .filter_map(|market| {
                detect_weather_contract_family(&catalog_text(market, event), &tags)
            })
            .collect::<BTreeSet<_>>();
        if families.len() > 1 {
            return Err(QuantError::config(format!(
                "Weather event {event_id} mixes multiple contract families"
            )));
        }
        if let Some(contract_family) = families.into_iter().next() {
            mature_events
                .entry(contract_family)
                .or_default()
                .insert(event_id);
        }
    }
    mature_events
        .into_iter()
        .map(|(family, events)| {
            u64::try_from(events.len())
                .map(|count| (family, count))
                .map_err(|error| {
                    QuantError::config(format!("Weather mature-group count overflow: {error}"))
                })
        })
        .collect()
}

fn linkage_metadata(market: &MarketInfo, event: &EventInfo) -> LinkageSourceMetadata {
    LinkageSourceMetadata {
        market_id: market.market_id.clone(),
        slug: market.slug.clone(),
        question: market.question.clone(),
        description: market.description.clone(),
        series_slug: event.series_slug.clone(),
        decision_group_market_ids: if market.category_set().contains(MarketCategory::Weather) {
            event.catalog_market_ids.iter().cloned().collect()
        } else {
            Vec::new()
        },
        end_date: market.end_date,
    }
}

fn domain_family(market: &MarketInfo) -> QuantResult<Option<DomainFamily>> {
    let categories = market.category_set();
    match (
        categories.contains(MarketCategory::Crypto),
        categories.contains(MarketCategory::Weather),
    ) {
        (true, false) => Ok(Some(DomainFamily::Crypto)),
        (false, true) => Ok(Some(DomainFamily::Weather)),
        (false, false) => Ok(None),
        (true, true) => Err(QuantError::config(format!(
            "market {} belongs to both Crypto and Weather verticals",
            market.market_id
        ))),
    }
}

const fn crypto_contract_family(comparator: PriceComparator) -> DomainContractFamily {
    match comparator {
        PriceComparator::UpVsReference => DomainContractFamily::CryptoDirection,
        PriceComparator::GreaterThan
        | PriceComparator::GreaterThanOrEqual
        | PriceComparator::LessThan
        | PriceComparator::LessThanOrEqual => DomainContractFamily::CryptoThreshold,
        PriceComparator::Between { .. } => DomainContractFamily::CryptoBand,
    }
}

fn catalog_text(market: &MarketInfo, event: &EventInfo) -> String {
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        market.question,
        market.slug,
        market.description.as_deref().unwrap_or_default(),
        event.title,
        event.slug,
        event.series_slug.as_deref().unwrap_or_default(),
    )
    .to_ascii_lowercase()
}

fn normalized_tags(event: &EventInfo) -> BTreeSet<String> {
    event
        .tags
        .iter()
        .map(|tag| tag.trim().to_ascii_lowercase())
        .collect()
}

fn detect_weather_contract_family(
    text: &str,
    tags: &BTreeSet<String>,
) -> Option<DomainContractFamily> {
    if tag(tags, "daily-temperature")
        || tag(tags, "highest-temperature")
        || tag(tags, "lowest-temperature")
    {
        return Some(DomainContractFamily::WeatherDailyTemperature);
    }
    if tag(tags, "precipitation")
        || contains_any(text, &["precipitation", "rainfall", "total rain"])
    {
        return Some(DomainContractFamily::WeatherPrecipitation);
    }
    if tag(tags, "aqi") || contains_any(text, &["air quality index", " aqi "]) {
        return Some(DomainContractFamily::WeatherAqi);
    }
    if tag(tags, "tornadoes") || text.contains("tornado") {
        return Some(DomainContractFamily::WeatherTornado);
    }
    if contains_any(
        text,
        &[
            "hurricane",
            "tropical cyclone",
            "tropical storm",
            "named storm",
        ],
    ) {
        return Some(DomainContractFamily::WeatherTropicalCyclone);
    }
    if tag(tags, "global-temp")
        || contains_any(
            text,
            &[
                "global temperature",
                "temperature anomaly",
                "hottest on record",
                "hottest year",
                "hottest month",
            ],
        )
    {
        return Some(DomainContractFamily::WeatherGlobalTemperature);
    }
    if text.contains("sea ice") {
        return Some(DomainContractFamily::WeatherSeaIce);
    }
    if contains_any(
        text,
        &["wind speed", "recorded wind", "wind gust", "highest wind"],
    ) {
        return Some(DomainContractFamily::WeatherWindExtreme);
    }
    None
}

fn weather_exclusion_reason(text: &str, tags: &BTreeSet<String>) -> DomainCapabilityReasonCode {
    explicit_weather_exclusion_reason(text, tags)
        .unwrap_or(DomainCapabilityReasonCode::WeatherNonAtmosphericTagNoise)
}

fn pre_family_weather_exclusion_reason(
    text: &str,
    tags: &BTreeSet<String>,
) -> Option<DomainCapabilityReasonCode> {
    match explicit_weather_exclusion_reason(text, tags) {
        Some(reason_code @ DomainCapabilityReasonCode::WeatherMixedHazardContract) => {
            Some(reason_code)
        }
        _ => None,
    }
}

fn explicit_weather_exclusion_reason(
    text: &str,
    tags: &BTreeSet<String>,
) -> Option<DomainCapabilityReasonCode> {
    if contains_any(text, &["natural disaster", "multiple disasters"]) || tag(tags, "parlays") {
        return Some(DomainCapabilityReasonCode::WeatherMixedHazardContract);
    }
    if contains_any(text, &["earthquake", "megaquake", "seismic"])
        || tag(tags, "earthquake")
        || tag(tags, "earthquakes")
    {
        return Some(DomainCapabilityReasonCode::WeatherEarthquakeContract);
    }
    if contains_any(text, &["volcano", "volcanic", "eruption", " vei "]) || tag(tags, "volcano") {
        return Some(DomainCapabilityReasonCode::WeatherVolcanoContract);
    }
    if contains_any(
        text,
        &[
            "pandemic",
            "vaccine",
            "measles",
            "ebola",
            "hantavirus",
            "influenza",
            "flu hospitalization",
            "cyclosporiasis",
        ],
    ) || tag(tags, "pandemics")
    {
        return Some(DomainCapabilityReasonCode::WeatherHealthContract);
    }
    if contains_any(
        text,
        &["meteor", "asteroid", "astroid", "in space", "spacex"],
    ) || tag(tags, "space")
    {
        return Some(DomainCapabilityReasonCode::WeatherSpaceContract);
    }
    if contains_any(
        text,
        &[
            "data center",
            "blue origin",
            "rocket",
            "software or control system",
            "engine failure",
        ],
    ) || tag(tags, "tech")
    {
        return Some(DomainCapabilityReasonCode::WeatherTechnologyContract);
    }
    None
}

fn looks_like_crypto_price_contract(text: &str) -> bool {
    contains_any(
        text,
        &[
            " price ",
            "price of ",
            "above $",
            "below $",
            "reach $",
            "hit $",
            "between $",
            "up or down",
            "up-or-down",
            "higher than",
            "lower than",
        ],
    )
}

fn crypto_exclusion_reason(text: &str) -> Option<DomainCapabilityReasonCode> {
    if contains_any(
        text,
        &[
            "fully diluted valuation",
            "fully-diluted valuation",
            " fdv ",
        ],
    ) {
        return Some(DomainCapabilityReasonCode::CryptoFdvContract);
    }
    if contains_any(
        text,
        &[
            "at any point",
            "at any time",
            "all-time high",
            "all time high",
            "new ath",
            " depeg ",
        ],
    ) || (contains_any(text, &[" hit $", " reach $", " dip to $"])
        && contains_any(text, &[" by ", " before "]))
        || (text.contains("immediately resolve")
            && text.contains(" any ")
            && contains_any(text, &["candle", "price level"]))
        || (text.contains(" first?") && contains_any(text, &[" hit ", " reach ", " dip "]))
    {
        return Some(DomainCapabilityReasonCode::CryptoPathDependentPriceContract);
    }
    if contains_any(
        text,
        &[
            "market capitalization",
            "market cap",
            "funding round",
            "fundraise",
            "token sale",
            "private valuation",
            "volatility index",
        ],
    ) {
        return Some(DomainCapabilityReasonCode::CryptoNonSpotValuationContract);
    }
    if contains_any(
        text,
        &[
            "outperform",
            "higher market cap",
            "higher price than",
            "more valuable than",
            "bitcoin vs",
            "ethereum vs",
        ],
    ) {
        return Some(DomainCapabilityReasonCode::CryptoRelativePerformanceContract);
    }
    if contains_any(
        text,
        &[
            "open interest",
            "protocol revenue",
            "network revenue",
            "gas fee",
            "gas price",
            "transaction fee",
            "transactions per second",
            "total value locked",
            " tvl ",
            "staking yield",
            "circulating supply",
        ],
    ) {
        return Some(DomainCapabilityReasonCode::CryptoProtocolMetricContract);
    }
    None
}

fn tag(tags: &BTreeSet<String>, expected: &str) -> bool {
    tags.contains(expected)
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        config::{
            WeatherHistoricalBindingKind, WeatherStationProfileConfig,
            WeatherVerticalBindingsConfig,
        },
        domain::market::{EventInfo, MarketInfo},
        enums::{
            common::{MarketCategory, TickSize},
            market::{EventStatus, MarketStatus},
        },
        types::{
            CatalogMarketIds, ContentHash, EventId, MarketId, TokenId,
            domain_capability::{DomainCapabilityReasonCode, DomainContractFamily},
            domain_classification::DomainMarketClassificationOutcome,
        },
    };
    use rust_decimal_macros::dec;

    use super::{
        DomainCatalogClassifier, WeatherStationRegistry, crypto_exclusion_reason,
        detect_weather_contract_family, grounded_airnow_pm25_daily_city_contract,
        grounded_weather_research_contract, is_hko_fractional_daily_temperature,
        looks_like_crypto_price_contract, pre_family_weather_exclusion_reason,
        weather_exclusion_reason,
    };

    fn tags(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn weather_family_detection_covers_every_supported_contract() {
        for (text, tags, expected) in [
            (
                "highest temperature in nyc",
                tags(&["daily-temperature"]),
                DomainContractFamily::WeatherDailyTemperature,
            ),
            (
                "precipitation in hong kong",
                tags(&[]),
                DomainContractFamily::WeatherPrecipitation,
            ),
            (
                "nyc air quality index below 100",
                tags(&[]),
                DomainContractFamily::WeatherAqi,
            ),
            (
                "how many tornadoes in july",
                tags(&[]),
                DomainContractFamily::WeatherTornado,
            ),
            (
                "category 5 hurricane landfall",
                tags(&[]),
                DomainContractFamily::WeatherTropicalCyclone,
            ),
            (
                "global temperature increase in july",
                tags(&[]),
                DomainContractFamily::WeatherGlobalTemperature,
            ),
            (
                "minimum arctic sea ice extent",
                tags(&[]),
                DomainContractFamily::WeatherSeaIce,
            ),
            (
                "highest recorded wind on mt washington",
                tags(&[]),
                DomainContractFamily::WeatherWindExtreme,
            ),
        ] {
            assert_eq!(detect_weather_contract_family(text, &tags), Some(expected));
        }
    }

    #[test]
    fn tagged_noise_has_stable_specific_exclusion_reasons() {
        for (text, expected) in [
            (
                "magnitude 6.5 earthquake",
                DomainCapabilityReasonCode::WeatherEarthquakeContract,
            ),
            (
                "major volcano eruption",
                DomainCapabilityReasonCode::WeatherVolcanoContract,
            ),
            (
                "measles cases in 2026",
                DomainCapabilityReasonCode::WeatherHealthContract,
            ),
            (
                "major meteor strike in 2026",
                DomainCapabilityReasonCode::WeatherSpaceContract,
            ),
            (
                "blue origin rocket launch",
                DomainCapabilityReasonCode::WeatherTechnologyContract,
            ),
            (
                "natural disaster in 2026",
                DomainCapabilityReasonCode::WeatherMixedHazardContract,
            ),
        ] {
            assert_eq!(weather_exclusion_reason(text, &tags(&[])), expected);
        }
    }

    #[test]
    fn only_mixed_hazard_contracts_preempt_weather_family_detection() {
        assert_eq!(
            pre_family_weather_exclusion_reason(
                "will a natural disaster or category 5 hurricane occur",
                &tags(&[]),
            ),
            Some(DomainCapabilityReasonCode::WeatherMixedHazardContract)
        );
        assert_eq!(
            pre_family_weather_exclusion_reason(
                "minimum arctic sea ice extent from the national snow and ice data center",
                &tags(&[]),
            ),
            None
        );
        assert_eq!(
            pre_family_weather_exclusion_reason(
                "category 5 hurricane with event tags shared by an earthquake collection",
                &tags(&["earthquake"]),
            ),
            None
        );
    }

    #[test]
    fn crypto_price_detection_does_not_promote_non_price_events() {
        assert!(looks_like_crypto_price_contract(
            "will bitcoin price reach $150000"
        ));
        assert!(looks_like_crypto_price_contract(
            "bitcoin up or down july 18"
        ));
        assert!(!looks_like_crypto_price_contract(
            "will congress pass a bitcoin reserve bill"
        ));
        assert!(matches!(
            DomainMarketClassificationOutcome::UnsupportedTemplate {
                reason_code: DomainCapabilityReasonCode::CryptoPriceTemplateNotGrounded,
            },
            DomainMarketClassificationOutcome::UnsupportedTemplate { .. }
        ));
    }

    #[test]
    fn crypto_out_of_scope_families_have_stable_exclusion_reasons() {
        for (text, expected) in [
            (
                "will bitcoin fully diluted valuation exceed $100b",
                DomainCapabilityReasonCode::CryptoFdvContract,
            ),
            (
                "will bitcoin hit $150000 at any point by december",
                DomainCapabilityReasonCode::CryptoPathDependentPriceContract,
            ),
            (
                "will the token market capitalization exceed $1b",
                DomainCapabilityReasonCode::CryptoNonSpotValuationContract,
            ),
            (
                "will bitcoin outperform ethereum",
                DomainCapabilityReasonCode::CryptoRelativePerformanceContract,
            ),
            (
                "will protocol revenue exceed $1b",
                DomainCapabilityReasonCode::CryptoProtocolMetricContract,
            ),
        ] {
            assert_eq!(crypto_exclusion_reason(text), Some(expected));
        }
    }

    #[test]
    fn hko_fractional_temperature_requires_explicit_bucket_ownership() {
        let rules = "The Hong Kong Observatory Absolute Daily Max resolves from \
                     https://www.weather.gov.hk/en/cis/climat.htm and measures temperatures to \
                     one decimal place.";
        assert!(is_hko_fractional_daily_temperature(
            &rules.to_ascii_lowercase()
        ));
        assert!(!is_hko_fractional_daily_temperature(
            "Wunderground measures temperatures to whole degrees."
        ));
    }

    #[test]
    fn research_only_weather_templates_ground_literal_official_sources() {
        for (family, text) in [
            (
                DomainContractFamily::WeatherPrecipitation,
                "precipitation resolution source measures to 1 decimal place \
                 https://www.weather.gov.hk/en/cis/climat.htm",
            ),
            (
                DomainContractFamily::WeatherAqi,
                "New York City region PM2.5 below 100 \
                 https://www.airnow.gov/state/?name=new-york",
            ),
            (
                DomainContractFamily::WeatherAqi,
                "highest PM2.5 Air Quality Index recorded in East Rutherford, New Jersey, \
                 between the opening kickoff and the end of gameplay; resolution source \
                 https://www.airnow.gov/?city=East%20Rutherford&state=NJ&country=USA; hourly \
                 figures for the Union City High School monitor at \
                 https://gispub.epa.gov/airnow/?contours=none",
            ),
            (
                DomainContractFamily::WeatherTornado,
                "number of tornadoes recorded in the United States; first relevant tornado count \
                 published at https://www.ncei.noaa.gov/access/monitoring/tornadoes/time-series",
            ),
            (
                DomainContractFamily::WeatherTropicalCyclone,
                "Category 5 hurricane official National Hurricane Center advisories \
                 https://www.nhc.noaa.gov/aboutsshws.php \
                 https://www.nhc.noaa.gov/aboutgloss.shtml#LANDFALL",
            ),
            (
                DomainContractFamily::WeatherGlobalTemperature,
                "Global Land-Ocean Temperature Index global temperature increase hottest on \
                 record https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.txt",
            ),
            (
                DomainContractFamily::WeatherSeaIce,
                "minimum Arctic sea ice extent National Snow and Ice Data Center \
                 NH-Daily-Extent August 1, 2026 through October 1, 2026",
            ),
            (
                DomainContractFamily::WeatherWindExtreme,
                "highest wind speed F6 reports whole mile per hour \
                 https://mountwashington.org/weather/mount-washington-weather-archives/monthly-f6/",
            ),
        ] {
            assert!(
                grounded_weather_research_contract(family, &text.to_ascii_lowercase()),
                "{family:?}"
            );
        }
    }

    #[test]
    fn airnow_daily_pm25_city_templates_require_exact_city_state_source() {
        for text in [
            "Philadelphia PM2.5 Air Quality Index below 100. Use the Historical Air Quality tab, \
             finalized city row under Daily AQI for PM2.5 at \
             https://www.airnow.gov/state/?name=Pennsylvania",
            "Columbus PM2.5 AQI below 100. Historical Air Quality and Daily AQI for PM2.5 at \
             https://www.airnow.gov/state/?name=Ohio",
            "Chicago PM2.5 AQI below 100. Historical Air Quality and Daily AQI for PM2.5 at \
             https://www.airnow.gov/state/?name=Illinois",
        ] {
            assert!(grounded_airnow_pm25_daily_city_contract(
                &text.to_ascii_lowercase()
            ));
        }
        assert!(!grounded_airnow_pm25_daily_city_contract(
            "Columbus PM2.5 AQI below 100. Historical Air Quality and Daily AQI for PM2.5 at \
             https://www.airnow.gov/state/?name=Indiana"
                .to_ascii_lowercase()
                .as_str()
        ));
    }

    #[test]
    fn daily_temperature_without_official_history_is_insufficient_evidence() {
        let station_registry = WeatherStationRegistry::try_new(BTreeMap::from([(
            "ZBAA".to_owned(),
            WeatherStationProfileConfig {
                timezone: "Asia/Shanghai".to_owned(),
                latitude: dec!(40.0801),
                longitude: dec!(116.5846),
                elevation_meters: dec!(35),
                ghcnh_station_id: None,
                historical_binding_kind: WeatherHistoricalBindingKind::Unavailable,
            },
        )]))
        .expect("station registry");
        let classifier = DomainCatalogClassifier::new(
            station_registry,
            &WeatherVerticalBindingsConfig::default(),
        )
        .expect("classifier");
        let now = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        let event_id = EventId::new("beijing-temperature-july-18");
        let market_id = MarketId::new("beijing-temperature-82-83");
        let market = MarketInfo {
            market_id: market_id.clone(),
            event_id: event_id.clone(),
            question: "Will the highest temperature in Beijing be between 82-83°F on July 18?"
                .to_owned(),
            slug: "highest-temperature-in-beijing-on-july-18-82-83f".to_owned(),
            description: Some(
                "This market will resolve to the temperature range that contains the highest \
                 temperature recorded at Beijing Capital Airport Station. The resolution source \
                 is available at \
                 https://www.wunderground.com/history/daily/cn/beijing/ZBAA. The resolution source \
                 measures temperatures to whole degrees Fahrenheit. The market resolves once \
                 information is finalized."
                    .to_owned(),
            ),
            categories: vec![MarketCategory::Weather],
            status: MarketStatus::Active,
            filter_reasons: Vec::new(),
            outcome: None,
            yes_token_id: TokenId::new("yes"),
            no_token_id: TokenId::new("no"),
            tick_size: TickSize::Hundredth,
            neg_risk: true,
            start_date: None,
            end_date: Some(now),
            resolved_at: None,
            content_hash: hash('a'),
            created_at: now,
            updated_at: now,
        };
        let event = EventInfo {
            event_id: event_id.clone(),
            title: "Highest temperature in Beijing on July 18".to_owned(),
            slug: "highest-temperature-in-beijing-on-july-18".to_owned(),
            series_slug: None,
            status: EventStatus::Active,
            tags: vec!["weather".to_owned(), "daily-temperature".to_owned()],
            neg_risk: true,
            catalog_market_ids: CatalogMarketIds::from(vec![market_id]),
            end_date: Some(now),
            content_hash: hash('b'),
            created_at: now,
            updated_at: now,
        };

        let artifact = classifier
            .classify_catalog(&[market], &BTreeMap::from([(event_id, event)]))
            .expect("classification");

        let [classification] = artifact.classifications.as_slice() else {
            panic!("expected one classification");
        };
        assert_eq!(
            classification.contract_family,
            Some(DomainContractFamily::WeatherDailyTemperature)
        );
        assert_eq!(
            classification.outcome,
            DomainMarketClassificationOutcome::InsufficientEvidence {
                reason_code: DomainCapabilityReasonCode::WeatherHistoricalCalibrationUnavailable,
            }
        );
    }

    fn hash(byte: char) -> ContentHash {
        ContentHash::parse(&format!("blake3:{}", byte.to_string().repeat(64)))
            .expect("content hash")
    }
}
