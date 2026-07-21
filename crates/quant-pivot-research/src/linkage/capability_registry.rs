//! Canonical Crypto/Weather capability registry.

use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::WeatherVerticalBindingsConfig,
    enums::domain::{DomainFamily, LinkageSourceRole},
    hashing::CanonicalDigest,
    types::{
        CapabilityEligibility, CapabilitySourceBinding, ContentHash, DomainCapabilityReasonCode,
        DomainCapabilityRegistryArtifact, DomainContractCapability, DomainContractFamily,
        DomainMeasurementUnit, DomainSourceId, DomainTimezonePolicy, ResearchProfileArtifact,
        ResearchProfileRef, SourceCredentialPolicy, builtin_research_profiles,
    },
};
use rust_decimal::Decimal;

use super::{
    DOMAIN_RESOLVER_VERSION,
    ruleset::{
        BINANCE_SPOT_ASSETS, BINANCE_USDM_FUTURES_ASSETS, CREDENTIAL_BINANCE_ASSETS,
        PUBLIC_RTDS_ASSETS,
    },
};

/// Build the one immutable registry consumed by resolver, ingest and serving.
///
/// The deploy-frozen Weather station registry is a first-class dependency:
/// changing an exact/proxy/unavailable binding must change the capability hash
/// and therefore invalidate affected linkage ledger rows.
pub fn domain_capability_registry(
    weather_station_registry_hash: &ContentHash,
    weather_vertical_bindings: &WeatherVerticalBindingsConfig,
) -> QuantResult<DomainCapabilityRegistryArtifact> {
    let profiles = builtin_research_profiles().map_err(QuantError::config)?;
    let crypto_profile = profile_for(&profiles, "crypto_price_15m")?;
    let weather_profile = profile_for(&profiles, "weather_forecast_24h")?;
    let mut contracts = Vec::new();
    for (contract_family, parser_template) in [
        (DomainContractFamily::CryptoDirection, "crypto_direction_v4"),
        (DomainContractFamily::CryptoThreshold, "crypto_threshold_v4"),
        (DomainContractFamily::CryptoBand, "crypto_band_v4"),
    ] {
        contracts.push(crypto_contract(
            contract_family,
            parser_template,
            PUBLIC_RTDS_ASSETS,
            &crypto_profile,
            CryptoFeatureBinding {
                source_id: DomainSourceId::binance(),
                instrument_template: "BINANCE:{symbol}:1m",
            },
            CryptoLiveBinding {
                source_id: DomainSourceId::polymarket_rtds_binance(),
                instrument_template: "RTDS:BINANCE:{symbol}",
            },
            CryptoResolutionBinding {
                eligibility: CapabilityEligibility::Supported,
                credential_policy: SourceCredentialPolicy::Public,
                source_id: DomainSourceId::polymarket_rtds_chainlink(),
                instrument_template: "RTDS:CHAINLINK:{feed}",
            },
        ));
        contracts.push(crypto_binance_contract(
            contract_family,
            parser_template,
            &crypto_profile,
        ));
        contracts.push(crypto_binance_futures_contract(
            contract_family,
            parser_template,
            &crypto_profile,
        ));
        contracts.push(crypto_contract(
            contract_family,
            parser_template,
            CREDENTIAL_BINANCE_ASSETS,
            &crypto_profile,
            CryptoFeatureBinding {
                source_id: DomainSourceId::binance(),
                instrument_template: "BINANCE:{symbol}:1m",
            },
            CryptoLiveBinding {
                source_id: DomainSourceId::binance_agg_trade(),
                instrument_template: "BINANCE_AGG_TRADE:{symbol}",
            },
            CryptoResolutionBinding {
                eligibility: CapabilityEligibility::CredentialBlocked {
                    reason_code:
                        DomainCapabilityReasonCode::ChainlinkDataStreamsCredentialsRequired,
                },
                credential_policy: SourceCredentialPolicy::Required {
                    credential_key: "chainlink_data_streams".to_owned(),
                },
                source_id: DomainSourceId::chainlink_data_streams(),
                instrument_template: "CHAINLINK_DATA_STREAMS:{feed}",
            },
        ));
        contracts.push(crypto_contract(
            contract_family,
            parser_template,
            BINANCE_USDM_FUTURES_ASSETS,
            &crypto_profile,
            CryptoFeatureBinding {
                source_id: DomainSourceId::binance_usdm_futures(),
                instrument_template: "BINANCE_USDM_FUTURES:{symbol}:1m",
            },
            CryptoLiveBinding {
                source_id: DomainSourceId::binance_usdm_futures_agg_trade(),
                instrument_template: "BINANCE_USDM_FUTURES_AGG_TRADE:{symbol}",
            },
            CryptoResolutionBinding {
                eligibility: CapabilityEligibility::CredentialBlocked {
                    reason_code:
                        DomainCapabilityReasonCode::ChainlinkDataStreamsCredentialsRequired,
                },
                credential_policy: SourceCredentialPolicy::Required {
                    credential_key: "chainlink_data_streams".to_owned(),
                },
                source_id: DomainSourceId::chainlink_data_streams(),
                instrument_template: "CHAINLINK_DATA_STREAMS:{feed}",
            },
        ));
    }
    let mut weather_contracts = weather_contracts(&weather_profile);
    let vertical_bindings_hash = CanonicalDigest::content_hash_json(&(
        "weather_vertical_bindings_v1",
        weather_vertical_bindings,
    ))?;
    for contract in &mut weather_contracts {
        contract
            .dependency_hashes
            .push(weather_station_registry_hash.clone());
        contract
            .dependency_hashes
            .push(vertical_bindings_hash.clone());
    }
    contracts.extend(weather_contracts);
    let registry = DomainCapabilityRegistryArtifact::new(DOMAIN_RESOLVER_VERSION, contracts)?;
    registry.validate().map_err(QuantError::config)?;
    Ok(registry)
}

fn crypto_binance_contract(
    contract_family: DomainContractFamily,
    parser_template: &str,
    profile: &ResearchProfileRef,
) -> DomainContractCapability {
    DomainContractCapability {
        family: DomainFamily::Crypto,
        contract_family,
        subject_scope: BINANCE_SPOT_ASSETS
            .iter()
            .map(|asset| (*asset).to_owned())
            .collect(),
        parser_template: format!("{parser_template}_binance"),
        source_bindings: vec![
            source(
                LinkageSourceRole::Feature,
                DomainSourceId::binance(),
                "BINANCE:{symbol}:1m",
                SourceCredentialPolicy::Public,
                120,
            ),
            source(
                LinkageSourceRole::LiveEvent,
                DomainSourceId::binance_agg_trade(),
                "BINANCE_AGG_TRADE:{symbol}",
                SourceCredentialPolicy::Public,
                30,
            ),
            source(
                LinkageSourceRole::Resolution,
                DomainSourceId::binance(),
                "BINANCE:{symbol}:1m",
                SourceCredentialPolicy::Public,
                120,
            ),
            source(
                LinkageSourceRole::Resolution,
                DomainSourceId::binance(),
                "BINANCE:{symbol}:1h",
                SourceCredentialPolicy::Public,
                7_200,
            ),
        ],
        unit: DomainMeasurementUnit::Usd,
        precision: Decimal::new(1, 8),
        timezone_policy: DomainTimezonePolicy::Utc,
        pit_available: true,
        profile: Some(profile.clone()),
        dependency_hashes: Vec::new(),
        eligibility: CapabilityEligibility::Supported,
    }
}

fn crypto_binance_futures_contract(
    contract_family: DomainContractFamily,
    parser_template: &str,
    profile: &ResearchProfileRef,
) -> DomainContractCapability {
    DomainContractCapability {
        family: DomainFamily::Crypto,
        contract_family,
        subject_scope: BINANCE_USDM_FUTURES_ASSETS
            .iter()
            .map(|asset| (*asset).to_owned())
            .collect(),
        parser_template: format!("{parser_template}_binance_usdm_futures"),
        source_bindings: vec![
            source(
                LinkageSourceRole::Feature,
                DomainSourceId::binance_usdm_futures(),
                "BINANCE_USDM_FUTURES:{symbol}:1m",
                SourceCredentialPolicy::Public,
                120,
            ),
            source(
                LinkageSourceRole::LiveEvent,
                DomainSourceId::binance_usdm_futures_agg_trade(),
                "BINANCE_USDM_FUTURES_AGG_TRADE:{symbol}",
                SourceCredentialPolicy::Public,
                30,
            ),
            source(
                LinkageSourceRole::Resolution,
                DomainSourceId::binance_usdm_futures(),
                "BINANCE_USDM_FUTURES:{symbol}:1m",
                SourceCredentialPolicy::Public,
                120,
            ),
            source(
                LinkageSourceRole::Resolution,
                DomainSourceId::binance_usdm_futures(),
                "BINANCE_USDM_FUTURES:{symbol}:1h",
                SourceCredentialPolicy::Public,
                7_200,
            ),
        ],
        unit: DomainMeasurementUnit::Usd,
        precision: Decimal::new(1, 8),
        timezone_policy: DomainTimezonePolicy::Utc,
        pit_available: true,
        profile: Some(profile.clone()),
        dependency_hashes: Vec::new(),
        eligibility: CapabilityEligibility::Supported,
    }
}

fn profile_for(profiles: &[ResearchProfileArtifact], id: &str) -> QuantResult<ResearchProfileRef> {
    profiles
        .iter()
        .find(|profile| profile.profile_ref.id.as_str() == id)
        .map(|profile| profile.profile_ref.clone())
        .ok_or_else(|| QuantError::config(format!("missing built-in research profile `{id}`")))
}

struct CryptoResolutionBinding {
    eligibility: CapabilityEligibility,
    credential_policy: SourceCredentialPolicy,
    source_id: DomainSourceId,
    instrument_template: &'static str,
}

struct CryptoLiveBinding {
    source_id: DomainSourceId,
    instrument_template: &'static str,
}

struct CryptoFeatureBinding {
    source_id: DomainSourceId,
    instrument_template: &'static str,
}

fn crypto_contract(
    contract_family: DomainContractFamily,
    parser_template: &str,
    assets: &[&str],
    profile: &ResearchProfileRef,
    feature: CryptoFeatureBinding,
    live: CryptoLiveBinding,
    resolution: CryptoResolutionBinding,
) -> DomainContractCapability {
    DomainContractCapability {
        family: DomainFamily::Crypto,
        contract_family,
        subject_scope: assets.iter().map(|asset| (*asset).to_owned()).collect(),
        parser_template: parser_template.to_owned(),
        source_bindings: vec![
            source(
                LinkageSourceRole::Feature,
                feature.source_id,
                feature.instrument_template,
                SourceCredentialPolicy::Public,
                120,
            ),
            source(
                LinkageSourceRole::LiveEvent,
                live.source_id,
                live.instrument_template,
                SourceCredentialPolicy::Public,
                30,
            ),
            source(
                LinkageSourceRole::Resolution,
                resolution.source_id,
                resolution.instrument_template,
                resolution.credential_policy,
                30,
            ),
        ],
        unit: DomainMeasurementUnit::Usd,
        precision: Decimal::new(1, 8),
        timezone_policy: DomainTimezonePolicy::Utc,
        pit_available: true,
        profile: Some(profile.clone()),
        dependency_hashes: Vec::new(),
        eligibility: resolution.eligibility,
    }
}

fn weather_contracts(profile: &ResearchProfileRef) -> Vec<DomainContractCapability> {
    let mut contracts = weather_observation_contracts(profile);
    contracts.extend(weather_extreme_and_climate_contracts(profile));
    for contract in &mut contracts {
        if contract.contract_family != DomainContractFamily::WeatherDailyTemperature
            && matches!(contract.eligibility, CapabilityEligibility::Supported)
        {
            contract.eligibility = CapabilityEligibility::InsufficientEvidence {
                reason_code: DomainCapabilityReasonCode::MatureLabelsUnavailable,
            };
        }
    }
    contracts
}

fn weather_observation_contracts(profile: &ResearchProfileRef) -> Vec<DomainContractCapability> {
    vec![
        weather_contract(
            DomainContractFamily::WeatherDailyTemperature,
            "weather_daily_temperature_v2",
            &["airport", "city"],
            DomainMeasurementUnit::Celsius,
            Decimal::new(1, 1),
            vec![
                public_source(
                    LinkageSourceRole::LiveEvent,
                    "aviation_weather",
                    "AVIATION_WEATHER:{station}",
                    900,
                ),
                public_source(
                    LinkageSourceRole::HistoricalCalibration,
                    "ghcnh",
                    "GHCNH:{station}",
                    86_400,
                ),
                public_source(
                    LinkageSourceRole::Forecast,
                    "gefs",
                    "GEFS:{station}",
                    43_200,
                ),
            ],
            profile,
        ),
        DomainContractCapability {
            family: DomainFamily::Weather,
            contract_family: DomainContractFamily::WeatherDailyTemperature,
            subject_scope: vec!["hko_station".to_owned()],
            parser_template: "weather_hko_daily_temperature_v1".to_owned(),
            source_bindings: vec![
                public_source(
                    LinkageSourceRole::Feature,
                    "hko_open_data",
                    "HKO:{station}:TMAX",
                    129_600,
                ),
                public_source(
                    LinkageSourceRole::Feature,
                    "hko_open_data",
                    "HKO:{station}:TMIN",
                    129_600,
                ),
            ],
            unit: DomainMeasurementUnit::Celsius,
            precision: Decimal::new(1, 1),
            timezone_policy: DomainTimezonePolicy::Named {
                timezone: "Asia/Hong_Kong".to_owned(),
            },
            pit_available: true,
            profile: Some(profile.clone()),
            dependency_hashes: Vec::new(),
            eligibility: CapabilityEligibility::Excluded {
                reason_code: DomainCapabilityReasonCode::WeatherAmbiguousFractionalBucketOwnership,
            },
        },
        weather_contract(
            DomainContractFamily::WeatherPrecipitation,
            "weather_precipitation_v1",
            &["hko_station", "nws_station", "met_office_site"],
            DomainMeasurementUnit::Millimeter,
            Decimal::new(1, 1),
            vec![
                public_source(
                    LinkageSourceRole::LiveEvent,
                    "hko_open_data",
                    "HKO:{station}:RAIN",
                    1_800,
                ),
                public_source(
                    LinkageSourceRole::Forecast,
                    "gefs",
                    "GEFS:{station}:APCP",
                    43_200,
                ),
            ],
            profile,
        ),
        weather_contract(
            DomainContractFamily::WeatherAqi,
            "weather_aqi_v3",
            &["airnow_pm25_reporting_area", "airnow_pm25_monitoring_site"],
            DomainMeasurementUnit::Aqi,
            Decimal::ONE,
            vec![
                public_source(
                    LinkageSourceRole::LiveEvent,
                    "airnow",
                    "AIRNOW:{state}:{area}:PM25:OBS",
                    7_200,
                ),
                optional_public_source(
                    LinkageSourceRole::Forecast,
                    "airnow",
                    "AIRNOW:{state}:{area}:PM25:FORECAST",
                    86_400,
                ),
                public_source(
                    LinkageSourceRole::LiveEvent,
                    "airnow",
                    "AIRNOW_SITE:{aqsid}:PM25_AQI",
                    7_200,
                ),
            ],
            profile,
        ),
        weather_contract(
            DomainContractFamily::WeatherTornado,
            "weather_tornado_v1",
            &["us_state", "us_region"],
            DomainMeasurementUnit::Count,
            Decimal::ONE,
            vec![
                public_source(
                    LinkageSourceRole::LiveEvent,
                    "spc_storm_reports",
                    "SPC:{region}:TORNADO",
                    7_200,
                ),
                public_source(
                    LinkageSourceRole::HistoricalCalibration,
                    "ncei_storm_events",
                    "NCEI:{region}:TORNADO",
                    2_678_400,
                ),
            ],
            profile,
        ),
    ]
}

fn weather_extreme_and_climate_contracts(
    profile: &ResearchProfileRef,
) -> Vec<DomainContractCapability> {
    vec![
        weather_contract(
            DomainContractFamily::WeatherTropicalCyclone,
            "weather_tropical_cyclone_v1",
            &[
                "atlantic",
                "central_pacific",
                "eastern_pacific",
                "western_north_pacific",
            ],
            DomainMeasurementUnit::Knot,
            Decimal::ONE,
            vec![
                public_source(
                    LinkageSourceRole::LiveEvent,
                    "nhc_advisory",
                    "NHC:{basin}:{storm}",
                    7_200,
                ),
                public_source(
                    LinkageSourceRole::HistoricalCalibration,
                    "nhc_hurdat2",
                    "HURDAT2:{basin}:{storm}",
                    31_536_000,
                ),
            ],
            profile,
        ),
        weather_contract(
            DomainContractFamily::WeatherGlobalTemperature,
            "weather_global_temperature_v1",
            &["global_monthly_anomaly"],
            DomainMeasurementUnit::CelsiusAnomaly,
            Decimal::new(1, 2),
            vec![public_source(
                LinkageSourceRole::Resolution,
                "nasa_gistemp",
                "GISTEMP:LOTI",
                3_888_000,
            )],
            profile,
        ),
        weather_contract(
            DomainContractFamily::WeatherSeaIce,
            "weather_sea_ice_v1",
            &["antarctic", "arctic"],
            DomainMeasurementUnit::MillionSquareKilometer,
            Decimal::new(1, 3),
            vec![public_source(
                LinkageSourceRole::Resolution,
                "nsidc_sea_ice_index",
                "NSIDC:{hemisphere}:EXTENT",
                172_800,
            )],
            profile,
        ),
        weather_contract(
            DomainContractFamily::WeatherWindExtreme,
            "weather_wind_extreme_v1",
            &["airport", "mount_washington", "nws_station"],
            DomainMeasurementUnit::Knot,
            Decimal::new(1, 1),
            vec![
                public_source(
                    LinkageSourceRole::LiveEvent,
                    "nws_observation",
                    "NWS:{station}:GUST",
                    1_800,
                ),
                public_source(
                    LinkageSourceRole::HistoricalCalibration,
                    "ghcnh",
                    "GHCNH:{station}:GUST",
                    86_400,
                ),
                public_source(
                    LinkageSourceRole::Forecast,
                    "gefs",
                    "GEFS:{station}:GUST",
                    43_200,
                ),
            ],
            profile,
        ),
    ]
}

fn weather_contract(
    contract_family: DomainContractFamily,
    parser_template: &str,
    scopes: &[&str],
    unit: DomainMeasurementUnit,
    precision: Decimal,
    source_bindings: Vec<CapabilitySourceBinding>,
    profile: &ResearchProfileRef,
) -> DomainContractCapability {
    DomainContractCapability {
        family: DomainFamily::Weather,
        contract_family,
        subject_scope: scopes.iter().map(|scope| (*scope).to_owned()).collect(),
        parser_template: parser_template.to_owned(),
        source_bindings,
        unit,
        precision,
        timezone_policy: DomainTimezonePolicy::StationLocal,
        pit_available: true,
        profile: Some(profile.clone()),
        dependency_hashes: Vec::new(),
        eligibility: CapabilityEligibility::Supported,
    }
}

fn public_source(
    role: LinkageSourceRole,
    source_id: &str,
    instrument_template: &str,
    freshness_secs: u64,
) -> CapabilitySourceBinding {
    source(
        role,
        DomainSourceId::new(source_id),
        instrument_template,
        SourceCredentialPolicy::Public,
        freshness_secs,
    )
}

fn optional_public_source(
    role: LinkageSourceRole,
    source_id: &str,
    instrument_template: &str,
    freshness_secs: u64,
) -> CapabilitySourceBinding {
    let mut binding = public_source(role, source_id, instrument_template, freshness_secs);
    binding.required = false;
    binding
}

fn source(
    role: LinkageSourceRole,
    source_id: DomainSourceId,
    instrument_template: &str,
    credential_policy: SourceCredentialPolicy,
    freshness_secs: u64,
) -> CapabilitySourceBinding {
    CapabilitySourceBinding {
        role,
        source_id,
        instrument_template: instrument_template.to_owned(),
        required: true,
        credential_policy,
        freshness_secs,
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        config::{WeatherVerticalBindingsConfig, builtin_weather_station_profiles},
        enums::domain::{DomainFamily, LinkageSourceRole},
        types::{
            CapabilityEligibility, ContentHash, DomainCapabilityRegistryArtifact,
            DomainContractFamily, DomainSourceId,
        },
    };

    use super::{
        BINANCE_USDM_FUTURES_ASSETS, CREDENTIAL_BINANCE_ASSETS, domain_capability_registry,
    };
    use crate::linkage::{
        rules, ruleset::CREDENTIAL_CHAINLINK_ASSETS,
        weather_daily_temperature::WeatherStationRegistry,
    };

    fn registry() -> DomainCapabilityRegistryArtifact {
        let stations =
            WeatherStationRegistry::try_new(builtin_weather_station_profiles()).expect("stations");
        domain_capability_registry(
            &stations.registry_hash().expect("station registry hash"),
            &WeatherVerticalBindingsConfig::default(),
        )
        .expect("registry")
    }

    #[test]
    fn registry_is_valid_and_covers_every_supported_family() {
        let registry = registry();
        assert!(registry.validate().is_ok());
        for family in [
            DomainContractFamily::CryptoDirection,
            DomainContractFamily::CryptoThreshold,
            DomainContractFamily::CryptoBand,
            DomainContractFamily::WeatherDailyTemperature,
            DomainContractFamily::WeatherPrecipitation,
            DomainContractFamily::WeatherAqi,
            DomainContractFamily::WeatherTornado,
            DomainContractFamily::WeatherTropicalCyclone,
            DomainContractFamily::WeatherGlobalTemperature,
            DomainContractFamily::WeatherSeaIce,
            DomainContractFamily::WeatherWindExtreme,
        ] {
            assert!(
                registry
                    .contracts
                    .iter()
                    .any(|contract| contract.contract_family == family),
                "missing {family:?}"
            );
        }
    }

    #[test]
    fn credential_only_crypto_assets_never_serve_as_public_chainlink() {
        let registry = registry();
        for asset in CREDENTIAL_CHAINLINK_ASSETS {
            let chainlink_contracts = registry
                .contracts
                .iter()
                .filter(|contract| {
                    contract.subject_scope.iter().any(|scope| scope == asset)
                        && contract.source_bindings.iter().any(|binding| {
                            binding.role == LinkageSourceRole::Resolution
                                && binding.source_id == DomainSourceId::chainlink_data_streams()
                        })
                })
                .collect::<Vec<_>>();
            assert!(!chainlink_contracts.is_empty());
            for contract in chainlink_contracts {
                assert!(matches!(
                    contract.eligibility,
                    CapabilityEligibility::CredentialBlocked { .. }
                ));
                if CREDENTIAL_BINANCE_ASSETS.contains(asset) {
                    assert!(contract.source_bindings.iter().any(|binding| {
                        binding.role == LinkageSourceRole::LiveEvent
                            && binding.source_id == DomainSourceId::binance_agg_trade()
                    }));
                } else {
                    assert!(contract.source_bindings.iter().any(|binding| {
                        binding.role == LinkageSourceRole::LiveEvent
                            && binding.source_id == DomainSourceId::binance_usdm_futures_agg_trade()
                    }));
                }
                assert!(contract.source_bindings.iter().all(|binding| {
                    binding.role != LinkageSourceRole::LiveEvent
                        || binding.source_id != DomainSourceId::polymarket_rtds_binance()
                }));
            }
            if CREDENTIAL_BINANCE_ASSETS.contains(asset) {
                assert!(registry.contracts.iter().any(|contract| {
                    contract.subject_scope.iter().any(|scope| scope == asset)
                        && matches!(contract.eligibility, CapabilityEligibility::Supported)
                        && contract.source_bindings.iter().any(|binding| {
                            binding.role == LinkageSourceRole::Resolution
                                && binding.source_id == DomainSourceId::binance()
                        })
                }));
            } else {
                assert!(BINANCE_USDM_FUTURES_ASSETS.contains(asset));
                assert!(registry.contracts.iter().any(|contract| {
                    contract.subject_scope.iter().any(|scope| scope == asset)
                        && matches!(contract.eligibility, CapabilityEligibility::Supported)
                        && contract.source_bindings.iter().any(|binding| {
                            binding.role == LinkageSourceRole::Resolution
                                && binding.source_id == DomainSourceId::binance_usdm_futures()
                        })
                }));
            }
        }
    }

    #[test]
    fn every_ruleset_asset_is_classified_by_the_registry() {
        let registry = registry();
        for rule in rules() {
            assert!(registry.contracts.iter().any(|contract| {
                contract
                    .subject_scope
                    .iter()
                    .any(|scope| scope == rule.ticker)
            }));
        }
    }

    #[test]
    fn weather_station_registry_hash_is_a_capability_dependency() {
        let first_station_hash =
            ContentHash::parse(format!("blake3:{}", "a".repeat(64))).expect("first station hash");
        let second_station_hash =
            ContentHash::parse(format!("blake3:{}", "b".repeat(64))).expect("second station hash");
        let bindings = WeatherVerticalBindingsConfig::default();
        let first =
            domain_capability_registry(&first_station_hash, &bindings).expect("first registry");
        let second =
            domain_capability_registry(&second_station_hash, &bindings).expect("second registry");

        assert_ne!(first.registry_hash, second.registry_hash);
        assert!(
            first
                .contracts
                .iter()
                .filter(|contract| contract.family == DomainFamily::Weather)
                .all(|contract| contract.dependency_hashes.contains(&first_station_hash))
        );
    }

    #[test]
    fn weather_vertical_bindings_hash_is_a_capability_dependency() {
        let stations = WeatherStationRegistry::try_new(builtin_weather_station_profiles())
            .expect("station registry");
        let station_hash = stations.registry_hash().expect("station registry hash");
        let first_bindings = WeatherVerticalBindingsConfig::default();
        let mut second_bindings = first_bindings.clone();
        second_bindings.airnow_pm25_sites[0].site_name = "Corrected Site Name".to_owned();
        let first =
            domain_capability_registry(&station_hash, &first_bindings).expect("first registry");
        let second =
            domain_capability_registry(&station_hash, &second_bindings).expect("second registry");

        assert_ne!(first.registry_hash, second.registry_hash);
    }
}
