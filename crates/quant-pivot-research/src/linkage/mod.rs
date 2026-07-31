//! Layered market-linkage resolution.
//!
//! Deterministic-first: Tier 0 (series-slug direct read) covers the
//! traded-volume bulk with zero parsing ambiguity; Tier 1 (template parser)
//! covers the human-readable ET slugs and threshold questions; the Tier 2 LLM
//! fallback remains unavailable until it has a deterministic, governed
//! implementation behind the same [`SubjectExtractor`] trait. Every tier's candidate
//! passes the **single** [`SubjectValidator`] grounding gate before it can
//! become a frozen ledger record — precision ≫ recall, one bad link poisons
//! every downstream join.
//!
//! This module is the pure half: extraction + validation over frozen
//! [`LinkageSourceMetadata`]. The impure orchestration (metadata loading,
//! ledger writes, re-resolution triggers) lives in `quant-pivot-core`'s
//! `LinkageResolverService`; persistence is `quant-pivot-repository`'s
//! `MarketLinkageRepository`.

pub mod capability_registry;
pub mod catalog_classification;
pub mod extractor;
pub mod manual_evidence;
pub mod oracle;
pub mod ruleset;
pub mod tier0_slug;
pub mod tier1_template;
pub mod weather_contracts;
pub mod weather_daily_temperature;

use chrono::{DateTime, Utc};
pub use extractor::{
    DefaultSubjectValidator, ExtractedCandidate, SubjectExtractor, SubjectValidator,
    ValidationOutcome, validate_structural_consistency,
};
pub use manual_evidence::validate_manual_override;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::WeatherVerticalBindingsConfig,
    domain::quant::{
        CryptoSubject, GlobalTemperatureOutcome, LinkageOutcome, LinkageSourceMetadata,
        LinkageUnresolvedReason, MarketSubject, ResolutionOracle, ResolvedBinding,
        ResolvedSourceBinding, SeaIceHemisphere, SeaIceProduct, WeatherGlobalTemperatureSubject,
        WeatherSeaIceSubject, WeatherTornadoFinalization, WeatherWindExtremeSubject,
        WeatherWindStatistic,
    },
    enums::domain::{LinkageSourceRole, ResolverTier},
    hashing::CanonicalDigest,
    types::{DomainInstrumentKey, DomainSourceId, IcaoStation, Probability, ResolverVersion},
};
pub use ruleset::{AssetRule, DOMAIN_RESOLVER_VERSION, find_alias, rule_for_alias, rules};
pub use tier0_slug::Tier0SlugExtractor;
pub use tier1_template::CryptoSubjectParser;
pub use weather_contracts::WeatherContractExtractor;
pub use weather_daily_temperature::{
    WeatherDailyTemperatureExtractor, WeatherDecisionGroupMember, WeatherDecisionGroupValidation,
    WeatherStationRegistry, validate_weather_decision_group, weather_station_profile_hash,
};

/// One resolver pass's verdict for a market, ready to freeze into the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionResult {
    /// The outcome to append (binding iff resolved).
    pub outcome: LinkageOutcome,
    /// The tier that decided (the accepting tier, or the last tier tried).
    pub resolver_tier: ResolverTier,
    /// The frozen ruleset version that governed this pass.
    pub resolver_version: ResolverVersion,
    /// Extractor confidence (zero for unresolved).
    pub confidence: Probability,
}

/// The deterministic layered resolver: ordered tiers behind one grounding gate.
pub struct LayeredResolver {
    tiers: Vec<Box<dyn SubjectExtractor>>,
    validator: Box<dyn SubjectValidator>,
    resolver_version: ResolverVersion,
}

impl LayeredResolver {
    /// The production resolver: Tier 0 → Tier 1, default grounding gate.
    #[must_use]
    pub fn deterministic(
        weather_stations: WeatherStationRegistry,
        weather_bindings: &WeatherVerticalBindingsConfig,
    ) -> Self {
        Self {
            tiers: vec![
                Box::new(Tier0SlugExtractor),
                Box::new(CryptoSubjectParser),
                Box::new(WeatherContractExtractor::new(weather_bindings)),
                Box::new(WeatherDailyTemperatureExtractor::new(weather_stations)),
            ],
            validator: Box::new(DefaultSubjectValidator),
            resolver_version: DOMAIN_RESOLVER_VERSION,
        }
    }

    /// The frozen ruleset version this resolver stamps on every record.
    #[must_use]
    pub const fn resolver_version(&self) -> ResolverVersion {
        self.resolver_version
    }

    /// Resolve one market's frozen metadata into a ledger-ready verdict.
    ///
    /// Tiers run in order; the first tier that produces a candidate decides
    /// (its candidate goes through the gate — a rejected candidate is a
    /// grounding failure, not a fall-through, so it fails closed rather than
    /// letting a lower-precision tier overrule the gate).
    ///
    /// # Errors
    ///
    /// Propagates irrecoverable extractor failures.
    pub fn resolve(
        &self,
        metadata: &LinkageSourceMetadata,
        available_at: DateTime<Utc>,
    ) -> QuantResult<ResolutionResult> {
        let mut last_tier = ResolverTier::Tier0Slug;
        for tier in &self.tiers {
            last_tier = tier.tier();
            let Some(candidate) = tier.extract(metadata)? else {
                continue;
            };
            return Ok(match self.validator.validate(&candidate, metadata) {
                ValidationOutcome::Accepted => ResolutionResult {
                    outcome: LinkageOutcome::Resolved(Box::new(ResolvedBinding {
                        source_bindings: source_bindings_for_subject(
                            &candidate.subject,
                            available_at,
                        )?,
                        subject: candidate.subject,
                        grounding: candidate.grounding,
                        override_context: None,
                    })),
                    resolver_tier: tier.tier(),
                    resolver_version: self.resolver_version,
                    confidence: candidate.confidence,
                },
                ValidationOutcome::Rejected { reason } => ResolutionResult {
                    outcome: LinkageOutcome::Unresolved {
                        reason: LinkageUnresolvedReason::CandidateRejected {
                            tier: tier.tier(),
                            failure: reason,
                        },
                    },
                    resolver_tier: tier.tier(),
                    resolver_version: self.resolver_version,
                    confidence: Probability::ZERO,
                },
            });
        }
        Ok(ResolutionResult {
            outcome: LinkageOutcome::Unresolved {
                reason: LinkageUnresolvedReason::NoDeterministicTemplate,
            },
            resolver_tier: last_tier,
            resolver_version: self.resolver_version,
            confidence: Probability::ZERO,
        })
    }
}

/// Derive the complete, canonical role/source/instrument set for a subject.
///
/// # Errors
///
/// Returns a configuration error when the subject has no frozen source rule,
/// or propagates canonical hashing failures.
pub fn source_bindings_for_subject(
    subject: &MarketSubject,
    available_at: DateTime<Utc>,
) -> QuantResult<Vec<ResolvedSourceBinding>> {
    let (specs, binding_context) = match subject {
        MarketSubject::Crypto(subject) => crypto_source_plan(subject)?,
        MarketSubject::Weather(subject) => (
            vec![
                (
                    LinkageSourceRole::LiveEvent,
                    DomainSourceId::aviation_weather(),
                    DomainInstrumentKey::aviation_weather(&subject.decision_group.station),
                ),
                (
                    LinkageSourceRole::HistoricalCalibration,
                    DomainSourceId::ghcnh(),
                    DomainInstrumentKey::ghcnh(&subject.decision_group.station),
                ),
                (
                    LinkageSourceRole::Resolution,
                    DomainSourceId::ghcnd(),
                    DomainInstrumentKey::ghcnd_temperature(
                        &subject.decision_group.station,
                        subject.decision_group.temperature_statistic,
                    ),
                ),
                (
                    LinkageSourceRole::Forecast,
                    DomainSourceId::gefs(),
                    DomainInstrumentKey::gefs(&subject.decision_group.station),
                ),
            ],
            subject.decision_group.station.to_string(),
        ),
        MarketSubject::WeatherPrecipitation(subject) => (
            vec![
                (
                    LinkageSourceRole::Resolution,
                    DomainSourceId::hko_open_data(),
                    DomainInstrumentKey::hko_daily_rainfall(&subject.station_key),
                ),
                (
                    LinkageSourceRole::Forecast,
                    DomainSourceId::gefs(),
                    DomainInstrumentKey::new(format!(
                        "GEFS:GEO:{:.6}:{:.6}:APCP",
                        subject.latitude, subject.longitude
                    )),
                ),
            ],
            subject.site_key.clone(),
        ),
        MarketSubject::WeatherAqi(subject) => (
            vec![
                (
                    LinkageSourceRole::LiveEvent,
                    DomainSourceId::airnow(),
                    DomainInstrumentKey::airnow_pm25_observation(&subject.reporting_area_key),
                ),
                (
                    LinkageSourceRole::Resolution,
                    DomainSourceId::airnow(),
                    DomainInstrumentKey::airnow_pm25_observation(&subject.reporting_area_key),
                ),
                (
                    LinkageSourceRole::Forecast,
                    DomainSourceId::airnow(),
                    DomainInstrumentKey::airnow_pm25_forecast(&subject.reporting_area_key),
                ),
            ],
            subject.reporting_area_key.clone(),
        ),
        MarketSubject::WeatherTornado(subject) => {
            let (final_source, final_instrument) = match &subject.finalization {
                WeatherTornadoFinalization::StormEventsArchive => (
                    DomainSourceId::ncei_storm_events(),
                    DomainInstrumentKey::ncei_tornado(&subject.region_key),
                ),
                WeatherTornadoFinalization::FirstPublishedAfter { .. } => (
                    DomainSourceId::ncei_tornado_time_series(),
                    DomainInstrumentKey::ncei_tornado_time_series(),
                ),
            };
            (
                vec![
                    (
                        LinkageSourceRole::LiveEvent,
                        DomainSourceId::spc_storm_reports(),
                        DomainInstrumentKey::spc_tornado(&subject.region_key),
                    ),
                    (
                        LinkageSourceRole::Resolution,
                        final_source,
                        final_instrument,
                    ),
                ],
                subject.region_key.clone(),
            )
        }
        MarketSubject::WeatherTropicalCyclone(subject) => (
            vec![
                (
                    LinkageSourceRole::LiveEvent,
                    DomainSourceId::nhc_advisory(),
                    DomainInstrumentKey::nhc_advisory(&subject.basin, &subject.storm_key),
                ),
                (
                    LinkageSourceRole::Resolution,
                    DomainSourceId::nhc_hurdat2(),
                    DomainInstrumentKey::nhc_hurdat2(&subject.basin, &subject.storm_key),
                ),
            ],
            format!("{}:{}", subject.basin, subject.storm_key),
        ),
        MarketSubject::WeatherGlobalTemperature(subject) => global_temperature_plan(subject),
        MarketSubject::WeatherSeaIce(subject) => sea_ice_source_plan(subject),
        MarketSubject::WeatherWindExtreme(subject) => wind_source_plan(subject)?,
    };
    specs
        .into_iter()
        .map(|(role, source_id, instrument_key)| {
            let binding_hash = CanonicalDigest::content_hash_json(&(
                "domain_source_binding_v2",
                &binding_context,
                role,
                &source_id,
                &instrument_key,
                subject,
            ))?;
            Ok(ResolvedSourceBinding {
                role,
                source_id,
                instrument_key,
                available_at,
                binding_hash,
            })
        })
        .collect()
}

type SourceBindingSpec = (LinkageSourceRole, DomainSourceId, DomainInstrumentKey);
type SourceBindingPlan = (Vec<SourceBindingSpec>, String);

fn crypto_source_plan(subject: &CryptoSubject) -> QuantResult<SourceBindingPlan> {
    let ticker = subject.asset.as_str().to_lowercase();
    let rule = rule_for_alias(&ticker).ok_or_else(|| {
        QuantError::config(format!(
            "asset `{}` has no frozen source rule",
            subject.asset
        ))
    })?;
    let mut specs = vec![(
        LinkageSourceRole::Feature,
        rule.kline_source_id(),
        rule.instrument_key(),
    )];
    match &subject.resolution_oracle {
        ResolutionOracle::ChainlinkDataStreams { .. } => {
            let (source_id, instrument) = if rule.public_rtds_supported() {
                (
                    DomainSourceId::polymarket_rtds_chainlink(),
                    rule.rtds_chainlink_instrument(),
                )
            } else {
                (
                    DomainSourceId::chainlink_data_streams(),
                    rule.chainlink_instrument(),
                )
            };
            specs.push((
                LinkageSourceRole::LiveEvent,
                source_id.clone(),
                instrument.clone(),
            ));
            specs.push((LinkageSourceRole::Resolution, source_id, instrument));
        }
        ResolutionOracle::BinanceKline { interval, .. } => {
            let (source_id, instrument) = if rule.public_rtds_supported() {
                (
                    DomainSourceId::polymarket_rtds_binance(),
                    rule.rtds_binance_instrument(),
                )
            } else {
                (
                    rule.binance_event_source_id(),
                    rule.binance_event_instrument(),
                )
            };
            specs.push((LinkageSourceRole::LiveEvent, source_id, instrument));
            specs.push((
                LinkageSourceRole::Resolution,
                rule.kline_source_id(),
                rule.kline_instrument(*interval),
            ));
        }
    }
    Ok((specs, subject.asset.to_string()))
}

fn global_temperature_plan(subject: &WeatherGlobalTemperatureSubject) -> SourceBindingPlan {
    let instrument = match &subject.outcome {
        GlobalTemperatureOutcome::MonthlyAnomaly { .. }
        | GlobalTemperatureOutcome::MonthlyRecordRank { .. } => {
            DomainInstrumentKey::nasa_gistemp_loti()
        }
        GlobalTemperatureOutcome::AnnualRecordRank { .. } => {
            DomainInstrumentKey::nasa_gistemp_loti_annual()
        }
    };
    (
        vec![(
            LinkageSourceRole::Resolution,
            DomainSourceId::nasa_gistemp(),
            instrument,
        )],
        "GISTEMP:LOTI:v4:1951-1980".to_owned(),
    )
}

fn sea_ice_source_plan(subject: &WeatherSeaIceSubject) -> SourceBindingPlan {
    let hemisphere = match subject.hemisphere {
        SeaIceHemisphere::Northern => "north",
        SeaIceHemisphere::Southern => "south",
    };
    let instrument = match subject.product {
        SeaIceProduct::DailyExtent => DomainInstrumentKey::nsidc_daily_extent(hemisphere),
        SeaIceProduct::MonthlyExtent => DomainInstrumentKey::nsidc_monthly_extent(hemisphere),
    };
    (
        vec![(
            LinkageSourceRole::Resolution,
            DomainSourceId::nsidc_sea_ice_index(),
            instrument,
        )],
        format!("NSIDC:{hemisphere}:v4"),
    )
}

fn wind_source_plan(subject: &WeatherWindExtremeSubject) -> QuantResult<SourceBindingPlan> {
    let station = IcaoStation::parse(&subject.station_key).map_err(|error| {
        QuantError::config(format!(
            "wind subject station `{}` is invalid: {error}",
            subject.station_key
        ))
    })?;
    let observation_instrument = match subject.statistic {
        WeatherWindStatistic::MaximumGust => DomainInstrumentKey::nws_wind_gust(&station),
        WeatherWindStatistic::MaximumSustainedWind => DomainInstrumentKey::nws_wind_speed(&station),
    };
    Ok((
        vec![
            (
                LinkageSourceRole::LiveEvent,
                DomainSourceId::nws_observation(),
                observation_instrument.clone(),
            ),
            (
                LinkageSourceRole::Resolution,
                DomainSourceId::nws_observation(),
                observation_instrument,
            ),
            (
                LinkageSourceRole::HistoricalCalibration,
                DomainSourceId::ghcnh(),
                DomainInstrumentKey::ghcnh(&station),
            ),
            (
                LinkageSourceRole::Forecast,
                DomainSourceId::gefs(),
                DomainInstrumentKey::new(format!("GEFS:{station}:GUST")),
            ),
        ],
        station.to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        config::WeatherVerticalBindingsConfig,
        domain::quant::{
            GroundingField, LinkageOutcome, LinkageSourceMetadata, ResolvedSourceBinding,
        },
        enums::domain::{LinkageSourceRole, ResolverTier},
        types::{DomainSourceId, MarketId},
    };

    use super::{
        DefaultSubjectValidator, LayeredResolver, SubjectExtractor, SubjectValidator,
        Tier0SlugExtractor, ValidationOutcome, WeatherStationRegistry,
    };

    /// The literal Chainlink Data Streams rules-text anchor every observed
    /// short-cycle up/down market carries.
    const CHAINLINK_STREAM_RULES: &str = "The resolution source for this market is the \
        Chainlink BTC/USD data stream, available at https://data.chain.link/streams/btc-usd.";

    fn metadata(slug: &str, question: &str, description: Option<&str>) -> LinkageSourceMetadata {
        LinkageSourceMetadata {
            market_id: MarketId::new("0xmarket"),
            slug: slug.to_owned(),
            question: question.to_owned(),
            description: description.map(str::to_owned),
            series_slug: None,
            decision_group_market_ids: Vec::new(),
            end_date: Some(Utc.with_ymd_and_hms(2026, 7, 8, 12, 0, 0).unwrap()),
        }
    }

    #[test]
    fn tier0_wins_before_rejects() {
        let resolver = LayeredResolver::deterministic(
            WeatherStationRegistry::default(),
            &WeatherVerticalBindingsConfig::default(),
        );

        let tier0 = resolver
            .resolve(
                &metadata(
                    "btc-updown-5m-1780319100",
                    "Bitcoin Up or Down",
                    Some(CHAINLINK_STREAM_RULES),
                ),
                Utc::now(),
            )
            .expect("resolve");
        assert_eq!(tier0.resolver_tier, ResolverTier::Tier0Slug);
        assert!(matches!(tier0.outcome, LinkageOutcome::Resolved(_)));

        let unresolved = resolver
            .resolve(
                &metadata("who-wins-the-super-bowl", "Who wins the Super Bowl?", None),
                Utc::now(),
            )
            .expect("resolve");
        assert!(matches!(
            unresolved.outcome,
            LinkageOutcome::Unresolved { .. }
        ));
        assert!(unresolved.confidence.inner().is_zero());
    }

    #[test]
    fn grounding_rejects_field_source() {
        // Anti-hallucination: a candidate whose spans do not literally appear
        // in the source metadata must be rejected by the single gate.
        // Build a valid candidate, then corrupt one span's text.
        let source = metadata(
            "btc-updown-5m-1780319100",
            "Bitcoin Up or Down",
            Some(CHAINLINK_STREAM_RULES),
        );
        let mut candidate = Tier0SlugExtractor
            .extract(&source)
            .expect("extract")
            .expect("candidate");
        candidate.grounding.spans[0].text = "hallucinated".to_owned();
        assert!(matches!(
            DefaultSubjectValidator.validate(&candidate, &source),
            ValidationOutcome::Rejected { .. }
        ));

        // And a span pointing at an absent source field is rejected too.
        let mut candidate = Tier0SlugExtractor
            .extract(&source)
            .expect("extract")
            .expect("candidate");
        candidate.grounding.spans[0].source = GroundingField::Description;
        assert!(matches!(
            DefaultSubjectValidator.validate(&candidate, &source),
            ValidationOutcome::Rejected { .. }
        ));
    }

    fn resolved_bindings(outcome: LinkageOutcome) -> Vec<ResolvedSourceBinding> {
        let LinkageOutcome::Resolved(resolved) = outcome else {
            panic!("fixture must resolve")
        };
        resolved.source_bindings
    }

    #[test]
    fn public_chainlink_assets_rejects() {
        let resolver = LayeredResolver::deterministic(
            WeatherStationRegistry::default(),
            &WeatherVerticalBindingsConfig::default(),
        );
        let btc = resolver
            .resolve(
                &metadata(
                    "btc-updown-5m-1780319100",
                    "Bitcoin Up or Down",
                    Some(CHAINLINK_STREAM_RULES),
                ),
                Utc::now(),
            )
            .expect("resolve BTC");
        let btc = resolved_bindings(btc.outcome);
        assert!(btc.iter().any(|binding| {
            binding.role == LinkageSourceRole::LiveEvent
                && binding.source_id == DomainSourceId::polymarket_rtds_chainlink()
                && binding.instrument_key.as_str() == "RTDS:CHAINLINK:BTC-USD"
        }));
        assert!(btc.iter().any(|binding| {
            binding.role == LinkageSourceRole::Resolution
                && binding.source_id == DomainSourceId::polymarket_rtds_chainlink()
        }));

        let doge_rules = "The resolution source is the Chainlink DOGE/USD data stream at \
            https://data.chain.link/streams/doge-usd.";
        let doge = resolver
            .resolve(
                &metadata(
                    "doge-updown-5m-1800000000",
                    "Dogecoin Up or Down",
                    Some(doge_rules),
                ),
                Utc::now(),
            )
            .expect("resolve DOGE");
        let doge = resolved_bindings(doge.outcome);
        assert!(doge.iter().any(|binding| {
            binding.role == LinkageSourceRole::LiveEvent
                && binding.source_id == DomainSourceId::chainlink_data_streams()
                && binding.instrument_key.as_str() == "CHAINLINK_DATA_STREAMS:DOGE-USD"
        }));
        assert!(
            doge.iter().all(|binding| {
                binding.source_id != DomainSourceId::polymarket_rtds_chainlink()
            })
        );
    }

    #[test]
    fn public_binance_routes_rtds() {
        let resolver = LayeredResolver::deterministic(
            WeatherStationRegistry::default(),
            &WeatherVerticalBindingsConfig::default(),
        );
        let outcome = resolver
            .resolve(
                &metadata(
                    "will-bitcoin-reach-150000-in-july",
                    "Will Bitcoin reach $150,000 in July?",
                    Some(
                        "This market resolves according to the Binance BTCUSDT 1 hour candle \
                         closing price on the resolution date.",
                    ),
                ),
                Utc::now(),
            )
            .expect("resolve Binance market");
        let bindings = resolved_bindings(outcome.outcome);
        assert!(bindings.iter().any(|binding| {
            binding.role == LinkageSourceRole::LiveEvent
                && binding.source_id == DomainSourceId::polymarket_rtds_binance()
                && binding.instrument_key.as_str() == "RTDS:BINANCE:BTCUSDT"
        }));
        assert!(bindings.iter().any(|binding| {
            binding.role == LinkageSourceRole::Resolution
                && binding.source_id == DomainSourceId::binance()
                && binding.instrument_key.as_str() == "BINANCE:BTCUSDT:1h"
        }));
    }
}
