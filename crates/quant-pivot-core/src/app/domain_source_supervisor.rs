//! Capability-driven source expectation reconciliation.
//!
//! Expectations are materialized before any source cursor exists. Active
//! linkages enrich market impact but are never the prerequisite for starting
//! statically known Crypto assets or configured Weather stations.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use chrono::Utc;
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::{
        TornadoRegionScopeConfig, WeatherStationProfileConfig, WeatherVerticalBindingsConfig,
    },
    domain::{
        data_plane::{
            DomainSourceExpectationDefinition, DomainSourceExpectationTransition,
            UpsertDomainSourceExpectation,
        },
        quant::{LinkageOutcome, MarketLinkage, MarketSubject},
    },
    enums::domain::{DomainFamily, DomainSourceExpectationStatus},
    types::{
        DomainCapabilityRegistryArtifact, DomainContractCapability, DomainContractFamily,
        DomainInstrumentKey, DomainSourceId, MarketId, ResearchProfileId, SourceCredentialPolicy,
    },
};
use quant_pivot_repository::traits::{DomainSourceExpectationRepository, MarketLinkageRepository};
use quant_pivot_research::linkage::{
    capability_registry::domain_capability_registry,
    weather_daily_temperature::WeatherStationRegistry,
};
use tokio::{sync::Mutex, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;

const RECONCILE_INTERVAL: Duration = Duration::from_secs(30);
const CREDENTIAL_BLOCKER_REASON: &str = "source_credentials_unavailable";
const REMOVED_BINDING_REASON: &str = "capability_binding_removed";

/// Reconciles immutable capability declarations into the expected-source
/// ledger. Runtime source workers consume the same source identities.
pub struct DomainSourceSupervisor {
    expectations: Arc<dyn DomainSourceExpectationRepository>,
    linkages: Arc<dyn MarketLinkageRepository>,
    registry: DomainCapabilityRegistryArtifact,
    weather_stations: BTreeMap<String, WeatherStationProfileConfig>,
    weather_vertical_bindings: WeatherVerticalBindingsConfig,
    credential_ready_sources: BTreeSet<DomainSourceId>,
    boot_ready: AtomicBool,
    boot_lock: Mutex<()>,
}

impl DomainSourceSupervisor {
    /// Build the supervisor from the deploy-frozen capability registry.
    pub fn new(
        expectations: Arc<dyn DomainSourceExpectationRepository>,
        linkages: Arc<dyn MarketLinkageRepository>,
        weather_stations: BTreeMap<String, WeatherStationProfileConfig>,
        weather_vertical_bindings: WeatherVerticalBindingsConfig,
        credential_ready_sources: BTreeSet<DomainSourceId>,
    ) -> QuantResult<Self> {
        let station_registry = WeatherStationRegistry::try_new(weather_stations.clone())?;
        let registry = domain_capability_registry(
            &station_registry.registry_hash()?,
            &weather_vertical_bindings,
        )?;
        Ok(Self {
            expectations,
            linkages,
            registry,
            weather_stations,
            weather_vertical_bindings,
            credential_ready_sources,
            boot_ready: AtomicBool::new(false),
            boot_lock: Mutex::new(()),
        })
    }

    /// Single-flight boot barrier shared by every source worker. A transient
    /// failed attempt is not cached; the next worker may retry reconciliation.
    pub async fn ensure_boot_reconciled(&self) -> QuantResult<()> {
        if self.boot_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        let _guard = self.boot_lock.lock().await;
        if self.boot_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        self.reconcile().await?;
        self.boot_ready.store(true, Ordering::Release);
        Ok(())
    }

    /// Materialize the complete desired ledger and retire bindings removed by
    /// the current registry. This call must complete before ingest starts.
    pub async fn reconcile(&self) -> QuantResult<()> {
        let linkages = self
            .linkages
            .latest_for_active_markets()
            .await?
            .into_iter()
            .map(MarketLinkage::from)
            .collect::<Vec<_>>();
        let desired = compile_expectations(
            &self.registry,
            &self.weather_stations,
            &self.weather_vertical_bindings,
            &linkages,
        )?;
        let existing = self
            .expectations
            .list_all()
            .await?
            .into_iter()
            .map(|row| (row.expectation_id, row))
            .collect::<HashMap<_, _>>();
        let mut desired_ids = HashSet::new();
        for definition in desired {
            let credential_blocked = definition.credential_required
                && !self
                    .credential_ready_sources
                    .contains(&definition.source_id);
            let initial_status = if credential_blocked {
                DomainSourceExpectationStatus::CredentialBlocked
            } else {
                DomainSourceExpectationStatus::NotStarted
            };
            let initial_reason = credential_blocked.then(|| CREDENTIAL_BLOCKER_REASON.to_owned());
            let candidate = UpsertDomainSourceExpectation::new(
                definition,
                initial_status,
                initial_reason,
                Utc::now(),
            )
            .map_err(QuantError::config)?;
            desired_ids.insert(candidate.expectation_id);
            let candidate = match existing.get(&candidate.expectation_id) {
                Some(current)
                    if current.binding_hash == candidate.binding_hash
                        && !credential_blocked
                        && !(current.status == DomainSourceExpectationStatus::Unsupported
                            && current.status_reason.as_deref()
                                == Some(REMOVED_BINDING_REASON)) =>
                {
                    UpsertDomainSourceExpectation {
                        status: if current.status
                            == DomainSourceExpectationStatus::CredentialBlocked
                            && current.status_reason.as_deref() == Some(CREDENTIAL_BLOCKER_REASON)
                        {
                            DomainSourceExpectationStatus::NotStarted
                        } else {
                            current.status
                        },
                        status_reason: if current.status
                            == DomainSourceExpectationStatus::CredentialBlocked
                            && current.status_reason.as_deref() == Some(CREDENTIAL_BLOCKER_REASON)
                        {
                            None
                        } else {
                            current.status_reason.clone()
                        },
                        ..candidate
                    }
                }
                _ => candidate,
            };
            self.expectations.upsert(candidate).await?;
        }
        for current in existing.into_values() {
            if desired_ids.contains(&current.expectation_id)
                || current.status == DomainSourceExpectationStatus::Unsupported
            {
                continue;
            }
            self.expectations
                .transition(DomainSourceExpectationTransition {
                    expectation_id: current.expectation_id,
                    from: current.status,
                    to: DomainSourceExpectationStatus::Unsupported,
                    reason: Some(REMOVED_BINDING_REASON.to_owned()),
                })
                .await?;
        }
        Ok(())
    }

    /// Keep market impact and capability changes reconciled after the
    /// mandatory boot-time pass.
    pub async fn run_periodic(&self, shutdown: CancellationToken) {
        let mut interval = tokio::time::interval(RECONCILE_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                _ = interval.tick() => {
                    if let Err(error) = self.reconcile().await {
                        tracing::error!(%error, "domain source expectation reconciliation failed");
                    }
                }
            }
        }
    }

    /// Record a pre-cursor or source-cycle failure in the expected ledger.
    /// Existing cursor failures are still persisted by the worker itself; this
    /// path prevents a source that never produced its first checkpoint from
    /// looking merely idle.
    pub async fn mark_source_failed(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
        reason: String,
    ) -> QuantResult<()> {
        let expectation_id = UpsertDomainSourceExpectation::identity_id(source_id, instrument_key)
            .map_err(QuantError::config)?;
        let Some(current) = self.expectations.find(&expectation_id).await? else {
            return Ok(());
        };
        if matches!(
            current.status,
            DomainSourceExpectationStatus::Failed
                | DomainSourceExpectationStatus::CredentialBlocked
                | DomainSourceExpectationStatus::Unsupported
        ) {
            return Ok(());
        }
        self.expectations
            .transition(DomainSourceExpectationTransition {
                expectation_id,
                from: current.status,
                to: DomainSourceExpectationStatus::Failed,
                reason: Some(reason),
            })
            .await?;
        Ok(())
    }

    /// Clear a durable failure after facts and the source cursor have both
    /// committed successfully.
    pub async fn mark_source_recovered(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
    ) -> QuantResult<()> {
        let expectation_id = UpsertDomainSourceExpectation::identity_id(source_id, instrument_key)
            .map_err(QuantError::config)?;
        let Some(current) = self.expectations.find(&expectation_id).await? else {
            return Ok(());
        };
        if !matches!(
            current.status,
            DomainSourceExpectationStatus::NotStarted
                | DomainSourceExpectationStatus::Stale
                | DomainSourceExpectationStatus::Failed
        ) {
            return Ok(());
        }
        self.expectations
            .transition(DomainSourceExpectationTransition {
                expectation_id,
                from: current.status,
                to: DomainSourceExpectationStatus::Live,
                reason: None,
            })
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
struct CompiledExpectation {
    family: DomainFamily,
    source_id: DomainSourceId,
    instrument_key: DomainInstrumentKey,
    required: bool,
    credential_required: bool,
    freshness_secs: i64,
    market_ids: BTreeSet<MarketId>,
    profile_ids: BTreeSet<ResearchProfileId>,
}

impl CompiledExpectation {
    fn into_definition(
        self,
        registry: &DomainCapabilityRegistryArtifact,
    ) -> DomainSourceExpectationDefinition {
        DomainSourceExpectationDefinition {
            family: self.family,
            source_id: self.source_id,
            instrument_key: self.instrument_key,
            capability_registry_hash: registry.registry_hash,
            required: self.required,
            credential_required: self.credential_required,
            freshness_secs: self.freshness_secs,
            affected_market_ids: self.market_ids.into_iter().collect(),
            affected_profile_ids: self.profile_ids.into_iter().collect(),
        }
    }
}

fn compile_expectations(
    registry: &DomainCapabilityRegistryArtifact,
    weather_stations: &BTreeMap<String, WeatherStationProfileConfig>,
    weather_vertical_bindings: &WeatherVerticalBindingsConfig,
    linkages: &[MarketLinkage],
) -> QuantResult<Vec<DomainSourceExpectationDefinition>> {
    let mut compiled =
        BTreeMap::<(DomainSourceId, DomainInstrumentKey), CompiledExpectation>::new();
    for contract in &registry.contracts {
        let profile_id = contract
            .profile
            .as_ref()
            .map(|profile| profile.id.clone())
            .ok_or_else(|| {
                QuantError::config(format!(
                    "capability {:?} has no research profile",
                    contract.contract_family
                ))
            })?;
        for rendered in
            render_static_bindings(contract, weather_stations, weather_vertical_bindings)
        {
            merge_binding(
                &mut compiled,
                registry,
                BindingCandidate {
                    family: contract.family,
                    source_id: rendered.source_id,
                    instrument_key: rendered.instrument_key,
                    required: rendered.required,
                    credential_required: rendered.credential_required,
                    freshness_secs: rendered.freshness_secs,
                    market_id: None,
                    profile_id: profile_id.clone(),
                },
            )?;
        }
    }
    for linkage in linkages {
        let LinkageOutcome::Resolved(resolved) = &linkage.outcome else {
            continue;
        };
        let profile_id = profile_for_family(registry, linkage.domain_family)?;
        for binding in &resolved.source_bindings {
            if binding.source_id == DomainSourceId::ghcnh()
                && matches!(
                    &resolved.subject,
                    MarketSubject::Weather(subject)
                        if weather_stations
                            .get(subject.decision_group.station.as_str())
                            .is_some_and(|profile| profile.ghcnh_station_id.is_none())
                )
            {
                continue;
            }
            if binding.source_id == DomainSourceId::ghcnd()
                && matches!(
                    &resolved.subject,
                    MarketSubject::Weather(subject)
                        if weather_stations
                            .get(subject.decision_group.station.as_str())
                            .is_some_and(|profile| profile.ghcnd_station_id.is_none())
                )
            {
                continue;
            }
            let (required, credential_required, freshness_secs) =
                source_policy(registry, linkage.domain_family, &binding.source_id)?;
            merge_binding(
                &mut compiled,
                registry,
                BindingCandidate {
                    family: linkage.domain_family,
                    source_id: binding.source_id.clone(),
                    instrument_key: binding.instrument_key.clone(),
                    required,
                    credential_required,
                    freshness_secs,
                    market_id: Some(linkage.market_id.clone()),
                    profile_id: profile_id.clone(),
                },
            )?;
        }
    }
    Ok(compiled
        .into_values()
        .map(|row| row.into_definition(registry))
        .collect())
}

struct RenderedBinding {
    source_id: DomainSourceId,
    instrument_key: DomainInstrumentKey,
    required: bool,
    credential_required: bool,
    freshness_secs: i64,
}

type StaticBindingSubstitutions = (
    Vec<BTreeMap<&'static str, String>>,
    Option<BTreeSet<DomainSourceId>>,
);

fn render_static_bindings(
    contract: &DomainContractCapability,
    weather_stations: &BTreeMap<String, WeatherStationProfileConfig>,
    weather_vertical_bindings: &WeatherVerticalBindingsConfig,
) -> Vec<RenderedBinding> {
    let (substitutions, allowed_sources) =
        static_binding_substitutions(contract, weather_stations, weather_vertical_bindings);
    let allowed_sources = &allowed_sources;
    substitutions
        .into_iter()
        .flat_map(|values| {
            contract.source_bindings.iter().filter_map(move |binding| {
                if allowed_sources
                    .as_ref()
                    .is_some_and(|sources| !sources.contains(&binding.source_id))
                {
                    return None;
                }
                if binding.source_id == DomainSourceId::ghcnh()
                    && values.get("station").is_some_and(|station| {
                        weather_stations
                            .get(station)
                            .is_some_and(|profile| profile.ghcnh_station_id.is_none())
                    })
                {
                    return None;
                }
                if binding.source_id == DomainSourceId::ghcnd()
                    && values.get("station").is_some_and(|station| {
                        weather_stations
                            .get(station)
                            .is_some_and(|profile| profile.ghcnd_station_id.is_none())
                    })
                {
                    return None;
                }
                let mut instrument = binding.instrument_template.clone();
                for (name, value) in &values {
                    instrument = instrument.replace(&format!("{{{name}}}"), value);
                }
                if instrument.contains('{') || instrument.contains('}') {
                    return None;
                }
                let freshness_secs = i64::try_from(binding.freshness_secs).ok()?;
                Some(RenderedBinding {
                    source_id: binding.source_id.clone(),
                    instrument_key: DomainInstrumentKey::new(instrument),
                    required: binding.required,
                    credential_required: matches!(
                        binding.credential_policy,
                        SourceCredentialPolicy::Required { .. }
                    ),
                    freshness_secs,
                })
            })
        })
        .collect()
}

fn static_binding_substitutions(
    contract: &DomainContractCapability,
    weather_stations: &BTreeMap<String, WeatherStationProfileConfig>,
    weather_vertical_bindings: &WeatherVerticalBindingsConfig,
) -> StaticBindingSubstitutions {
    match contract.contract_family {
        DomainContractFamily::CryptoDirection
        | DomainContractFamily::CryptoThreshold
        | DomainContractFamily::CryptoBand => (
            contract
                .subject_scope
                .iter()
                .map(|asset| {
                    BTreeMap::from([
                        ("asset", asset.clone()),
                        ("symbol", format!("{asset}USDT")),
                        ("feed", format!("{asset}-USD")),
                    ])
                })
                .collect(),
            None,
        ),
        DomainContractFamily::WeatherDailyTemperature
            if contract
                .source_bindings
                .iter()
                .all(|binding| binding.source_id == DomainSourceId::hko_open_data()) =>
        {
            (
                weather_vertical_bindings
                    .hko_daily_temperature
                    .iter()
                    .map(|binding| BTreeMap::from([("station", binding.station.clone())]))
                    .collect(),
                Some(BTreeSet::from([DomainSourceId::hko_open_data()])),
            )
        }
        DomainContractFamily::WeatherDailyTemperature => (
            weather_stations
                .keys()
                .flat_map(|station| {
                    ["TMAX", "TMIN"].into_iter().map(move |statistic| {
                        BTreeMap::from([
                            ("station", station.clone()),
                            ("temperature_statistic", statistic.to_owned()),
                        ])
                    })
                })
                .collect(),
            Some(BTreeSet::from([
                DomainSourceId::aviation_weather(),
                DomainSourceId::ghcnh(),
                DomainSourceId::ghcnd(),
                DomainSourceId::gefs(),
            ])),
        ),
        DomainContractFamily::WeatherPrecipitation => (
            weather_vertical_bindings
                .hko_rainfall
                .iter()
                .map(|binding| {
                    BTreeMap::from([
                        ("station", binding.station_key.clone()),
                        ("site", binding.site_key.clone()),
                    ])
                })
                .collect(),
            Some(BTreeSet::from([DomainSourceId::hko_open_data()])),
        ),
        DomainContractFamily::WeatherAqi => (
            weather_vertical_bindings
                .airnow_pm25_reporting_areas
                .iter()
                .map(|binding| {
                    BTreeMap::from([
                        ("state", binding.state.clone()),
                        ("area", binding.area.clone()),
                    ])
                })
                .chain(
                    weather_vertical_bindings
                        .airnow_pm25_sites
                        .iter()
                        .map(|binding| BTreeMap::from([("aqsid", binding.aqsid.clone())])),
                )
                .collect(),
            Some(BTreeSet::from([DomainSourceId::airnow()])),
        ),
        DomainContractFamily::WeatherTornado => {
            let national = contract
                .source_bindings
                .iter()
                .any(|binding| binding.source_id == DomainSourceId::ncei_tornado_time_series());
            let substitutions = weather_vertical_bindings
                .tornado_regions
                .iter()
                .filter(|binding| {
                    national == matches!(&binding.scope, TornadoRegionScopeConfig::UnitedStates)
                })
                .map(|binding| BTreeMap::from([("region", binding.region_id.clone())]))
                .collect();
            let sources = if national {
                BTreeSet::from([
                    DomainSourceId::spc_storm_reports(),
                    DomainSourceId::ncei_tornado_time_series(),
                ])
            } else {
                BTreeSet::from([
                    DomainSourceId::spc_storm_reports(),
                    DomainSourceId::ncei_storm_events(),
                ])
            };
            (substitutions, Some(sources))
        }
        DomainContractFamily::WeatherTropicalCyclone => (
            weather_vertical_bindings
                .nhc_historical_storms
                .iter()
                .map(|binding| {
                    BTreeMap::from([
                        ("basin", binding.basin.clone()),
                        ("storm", binding.storm_id.clone()),
                    ])
                })
                .collect(),
            Some(BTreeSet::from([DomainSourceId::nhc_hurdat2()])),
        ),
        DomainContractFamily::WeatherGlobalTemperature => (vec![BTreeMap::new()], None),
        DomainContractFamily::WeatherSeaIce => (
            contract
                .subject_scope
                .iter()
                .map(|hemisphere| BTreeMap::from([("hemisphere", hemisphere.clone())]))
                .collect(),
            None,
        ),
        DomainContractFamily::WeatherWindExtreme => (
            weather_vertical_bindings
                .nws_wind_stations
                .iter()
                .map(|binding| BTreeMap::from([("station", binding.station.clone())]))
                .collect(),
            Some(BTreeSet::from([DomainSourceId::nws_observation()])),
        ),
    }
}

struct BindingCandidate {
    family: DomainFamily,
    source_id: DomainSourceId,
    instrument_key: DomainInstrumentKey,
    required: bool,
    credential_required: bool,
    freshness_secs: i64,
    market_id: Option<MarketId>,
    profile_id: ResearchProfileId,
}

fn merge_binding(
    compiled: &mut BTreeMap<(DomainSourceId, DomainInstrumentKey), CompiledExpectation>,
    registry: &DomainCapabilityRegistryArtifact,
    candidate: BindingCandidate,
) -> QuantResult<()> {
    let key = (
        candidate.source_id.clone(),
        candidate.instrument_key.clone(),
    );
    let row = compiled.entry(key).or_insert_with(|| CompiledExpectation {
        family: candidate.family,
        source_id: candidate.source_id,
        instrument_key: candidate.instrument_key,
        required: candidate.required,
        credential_required: candidate.credential_required,
        freshness_secs: candidate.freshness_secs,
        market_ids: BTreeSet::new(),
        profile_ids: BTreeSet::new(),
    });
    if row.family != candidate.family {
        return Err(QuantError::config(format!(
            "source binding {}:{} crosses {:?}/{:?} capability families in registry {}",
            row.source_id, row.instrument_key, row.family, candidate.family, registry.registry_hash
        )));
    }
    row.required |= candidate.required;
    row.credential_required |= candidate.credential_required;
    row.freshness_secs = row.freshness_secs.min(candidate.freshness_secs);
    row.profile_ids.insert(candidate.profile_id);
    if let Some(market_id) = candidate.market_id {
        row.market_ids.insert(market_id);
    }
    Ok(())
}

fn profile_for_family(
    registry: &DomainCapabilityRegistryArtifact,
    family: DomainFamily,
) -> QuantResult<ResearchProfileId> {
    let profiles = registry
        .contracts
        .iter()
        .filter(|contract| contract.family == family)
        .filter_map(|contract| contract.profile.as_ref().map(|profile| profile.id.clone()))
        .collect::<BTreeSet<_>>();
    if profiles.len() != 1 {
        return Err(QuantError::config(format!(
            "domain family {family:?} must map to exactly one active profile, found {}",
            profiles.len()
        )));
    }
    profiles
        .into_iter()
        .next()
        .ok_or_else(|| QuantError::config("domain capability profile set is empty"))
}

fn source_policy(
    registry: &DomainCapabilityRegistryArtifact,
    family: DomainFamily,
    source_id: &DomainSourceId,
) -> QuantResult<(bool, bool, i64)> {
    let bindings = registry
        .contracts
        .iter()
        .filter(|contract| contract.family == family)
        .flat_map(|contract| &contract.source_bindings)
        .filter(|binding| &binding.source_id == source_id)
        .collect::<Vec<_>>();
    if bindings.is_empty() {
        let freshness = match family {
            DomainFamily::Crypto => 120,
            DomainFamily::Weather => 900,
        };
        return Ok((
            true,
            source_id == &DomainSourceId::chainlink_data_streams(),
            freshness,
        ));
    }
    let credential_required = bindings.iter().any(|binding| {
        matches!(
            binding.credential_policy,
            SourceCredentialPolicy::Required { .. }
        )
    });
    let freshness = bindings
        .iter()
        .map(|binding| binding.freshness_secs)
        .min()
        .ok_or_else(|| QuantError::config("domain source policy has no freshness"))?;
    let freshness = i64::try_from(freshness)
        .map_err(|error| QuantError::config(format!("source freshness overflow: {error}")))?;
    let required = bindings.iter().any(|binding| binding.required);
    Ok((required, credential_required, freshness))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use quant_pivot_models::{
        config::{
            WeatherHistoricalBindingKind, WeatherStationProfileConfig,
            WeatherVerticalBindingsConfig,
        },
        enums::domain::DomainFamily,
        types::{DomainSourceId, builtin_research_profiles},
    };
    use rust_decimal_macros::dec;

    use super::{WeatherStationRegistry, compile_expectations, domain_capability_registry};

    #[test]
    fn static_creates_without_linkage() {
        let stations = BTreeMap::from([
            (
                "KLGA".to_owned(),
                WeatherStationProfileConfig {
                    timezone: "America/New_York".to_owned(),
                    latitude: dec!(40.7769),
                    longitude: dec!(-73.8740),
                    elevation_meters: dec!(6.4),
                    ghcnh_station_id: Some("USW00014732".to_owned()),
                    ghcnd_station_id: Some("USW00014732".to_owned()),
                    historical_binding_kind: WeatherHistoricalBindingKind::ExactStation,
                },
            ),
            (
                "ZBAA".to_owned(),
                WeatherStationProfileConfig {
                    timezone: "Asia/Shanghai".to_owned(),
                    latitude: dec!(40.0801),
                    longitude: dec!(116.5846),
                    elevation_meters: dec!(35),
                    ghcnh_station_id: None,
                    ghcnd_station_id: None,
                    historical_binding_kind: WeatherHistoricalBindingKind::Unavailable,
                },
            ),
        ]);
        let station_registry =
            WeatherStationRegistry::try_new(stations.clone()).expect("station registry");
        let registry = domain_capability_registry(
            &station_registry
                .registry_hash()
                .expect("station registry hash"),
            &WeatherVerticalBindingsConfig::default(),
        )
        .expect("registry");
        let rows = compile_expectations(
            &registry,
            &stations,
            &WeatherVerticalBindingsConfig::default(),
            &[],
        )
        .expect("expectations");
        assert!(rows.iter().any(|row| {
            row.family == DomainFamily::Crypto
                && row.source_id == DomainSourceId::binance()
                && row.instrument_key.as_str() == "BINANCE:BTCUSDT:1m"
        }));
        assert_eq!(
            rows.iter()
                .filter(|row| {
                    row.family == DomainFamily::Crypto
                        && row.source_id == DomainSourceId::polymarket_rtds_binance()
                })
                .count(),
            4,
            "only assets documented by public RTDS may create RTDS expectations"
        );
        assert!(rows.iter().all(|row| {
            row.source_id != DomainSourceId::polymarket_rtds_binance()
                || row.instrument_key.as_str() != "RTDS:BINANCE:DOGEUSDT"
        }));
        assert!(rows.iter().any(|row| {
            row.family == DomainFamily::Weather
                && row.source_id == DomainSourceId::aviation_weather()
                && row.instrument_key.as_str() == "AVIATION_WEATHER:KLGA"
        }));
        assert!(rows.iter().any(|row| {
            row.family == DomainFamily::Weather
                && row.source_id == DomainSourceId::aviation_weather()
                && row.instrument_key.as_str() == "AVIATION_WEATHER:ZBAA"
        }));
        assert!(rows.iter().any(|row| {
            row.family == DomainFamily::Weather
                && row.source_id == DomainSourceId::gefs()
                && row.instrument_key.as_str() == "GEFS:ZBAA"
        }));
        assert!(rows.iter().all(|row| {
            row.source_id != DomainSourceId::ghcnh() || row.instrument_key.as_str() != "GHCNH:ZBAA"
        }));
        for (source, instrument) in [
            (DomainSourceId::hko_open_data(), "HKO:HKO:RAIN"),
            (DomainSourceId::hko_open_data(), "HKO:HKO:TMAX"),
            (DomainSourceId::hko_open_data(), "HKO:HKO:TMIN"),
            (
                DomainSourceId::airnow(),
                "AIRNOW:NY:New York City Region:PM25:OBS",
            ),
            (DomainSourceId::airnow(), "AIRNOW:PA:Philadelphia:PM25:OBS"),
            (DomainSourceId::airnow(), "AIRNOW:OH:Columbus:PM25:OBS"),
            (DomainSourceId::airnow(), "AIRNOW:IL:Chicago:PM25:OBS"),
            (
                DomainSourceId::airnow(),
                "AIRNOW_SITE:840340170008:PM25_AQI",
            ),
            (DomainSourceId::spc_storm_reports(), "SPC:oklahoma:TORNADO"),
            (DomainSourceId::ncei_storm_events(), "NCEI:oklahoma:TORNADO"),
            (DomainSourceId::nhc_hurdat2(), "HURDAT2:atlantic:AL092021"),
            (DomainSourceId::nasa_gistemp(), "GISTEMP:LOTI"),
            (DomainSourceId::nsidc_sea_ice_index(), "NSIDC:arctic:EXTENT"),
            (DomainSourceId::nws_observation(), "NWS:KMWN:GUST"),
        ] {
            assert!(
                rows.iter().any(|row| {
                    row.family == DomainFamily::Weather
                        && row.source_id == source
                        && row.instrument_key.as_str() == instrument
                }),
                "missing static Weather binding {source}/{instrument}"
            );
        }
        assert!(rows.iter().all(|row| !row.affected_profile_ids.is_empty()));
        assert_eq!(builtin_research_profiles().expect("profiles").len(), 3);
    }
}
