//! Finite, evidence-producing bootstrap for the Phase 11.9 domain data plane.
//!
//! This module composes the same catalog, linkage, source-supervisor, adapter,
//! fact-writer, and cursor paths as the long-running application without
//! constructing account or execution authority. It therefore cannot sign,
//! submit, change runtime mode, or mutate capital/trade-policy pointers.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use chrono::NaiveDate;
use quant_pivot_api::{
    binance::{BinanceAggTradeSource, BinanceKlineSource, BinanceRequestBudget},
    chainlink::ChainlinkDataStreamsSource,
    domain::DomainDataSource,
    rtds::PolymarketRtdsSource,
    weather::{
        AviationWeatherSource, GefsSource, GhcnhSource, airnow::AirNowSource,
        gistemp::NasaGistempSource, hko::HkoOpenDataSource, nhc::NhcSource,
        nsidc::NsidcSeaIceSource, nws::NwsObservationSource, tornado::TornadoSource,
    },
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::{
        CryptoPriceReportRow, DomainObservationRow, WeatherForecastFactRow,
        WeatherObservationFactRow,
    },
    config::{DeployConfig, DomainSourcesConfig},
    domain::{
        CoreEventPublisher, DomainCursorStatus, DomainSourceCursorInfo, DomainSourceExpectationInfo,
    },
    enums::{
        common::MarketCategory,
        domain::{DomainFamily, DomainSourceExpectationStatus},
    },
    types::{
        ContentHash, DomainSourceId, EventId,
        domain_classification::DomainMarketClassificationOutcome,
    },
};
use quant_pivot_repository::{
    clickhouse::{ChFactWriter, ChNativeReadRepository, FactEvidenceTable},
    traits::{
        CalibrationArtifactRepository, DomainProjectionRepository, DomainSourceCursorRepository,
        DomainSourceExpectationRepository, EventRepository, FactWriter, MarketLinkageRepository,
        MarketRepository,
    },
};
use quant_pivot_research::linkage::{
    capability_registry::domain_capability_registry,
    catalog_classification::DomainCatalogClassifier,
    weather_daily_temperature::WeatherStationRegistry,
};
use serde::Serialize;
use tokio::{task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    app::{
        DataBundle, DataBundleDeps, InfraBundle, RuntimeSnapshot,
        crypto_kline_ingest_worker::CryptoKlineIngestWorker,
        crypto_live_ingest_worker::{CryptoLiveIngestDeps, CryptoLiveIngestWorker},
        crypto_rtds_ingest_worker::{CryptoRtdsIngestDeps, CryptoRtdsIngestWorker},
        domain_source_supervisor::DomainSourceSupervisor,
        weather_ingest_worker::{WeatherIngestDeps, WeatherIngestWorker},
        weather_public_ingest_worker::{WeatherPublicIngestDeps, WeatherPublicIngestWorker},
    },
    observability::metrics_hub::MetricsHub,
    runtime_config::DecisionPolicyStore,
    service::{
        crypto_kline_ingest::CryptoKlineIngestor, weather_fact_ingest::WeatherFactIngestService,
    },
};

const EVIDENCE_FORMAT_VERSION: u32 = 1;
const CRYPTO_WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct Phase119EvidenceBootstrapOptions {
    pub categories: BTreeSet<DomainFamily>,
    pub weather_stations: BTreeSet<String>,
    pub sync_catalog: bool,
    pub max_crypto_cycles: u16,
    pub crypto_live_timeout_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GistempHistoricalTimeEvidence {
    pub row_count: u64,
    pub earliest_local_date_epoch_days: Option<i32>,
    pub earliest_observed_at_ms: Option<i64>,
    pub earliest_valid_from_ms: Option<i64>,
    pub earliest_valid_to_ms: Option<i64>,
    pub null_valid_from_rows: u64,
    pub null_valid_to_rows: u64,
    pub verified: bool,
}

struct ScopedSourceLedgerEvidence {
    expectation_count: u64,
    expectation_statuses: BTreeMap<String, u64>,
    cursor_count: u64,
    cursor_statuses: BTreeMap<DomainCursorStatus, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FactIdempotencyEvidence {
    pub physical_rows: u64,
    pub logical_keys: u64,
    pub duplicate_rows: u64,
    pub revision_conflicts: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Phase119EvidenceManifest {
    pub format_version: u32,
    pub categories: Vec<String>,
    pub weather_stations: Vec<String>,
    pub capability_registry_hash: ContentHash,
    pub catalog_hash: ContentHash,
    pub catalog_market_count: u64,
    pub classification_outcomes: BTreeMap<String, u64>,
    pub linkage_count: u64,
    pub source_expectation_count: u64,
    pub source_expectation_statuses: BTreeMap<String, u64>,
    pub source_cursor_count: u64,
    pub source_cursor_statuses: BTreeMap<DomainCursorStatus, u64>,
    pub crypto_observation_rows: u64,
    pub crypto_price_report_rows: u64,
    pub weather_observation_rows: u64,
    pub weather_forecast_rows: u64,
    pub fact_idempotency: BTreeMap<String, FactIdempotencyEvidence>,
    pub gistemp_historical_time: Option<GistempHistoricalTimeEvidence>,
    pub blockers: Vec<String>,
}

struct CryptoEvidenceSources {
    kline_spot: Arc<BinanceKlineSource>,
    kline_usdm_futures: Arc<BinanceKlineSource>,
    agg_trade_spot: Arc<BinanceAggTradeSource>,
    agg_trade_usdm_futures: Arc<BinanceAggTradeSource>,
    rtds: Option<Arc<PolymarketRtdsSource>>,
    chainlink: Option<Arc<ChainlinkDataStreamsSource>>,
}

impl Phase119EvidenceManifest {
    #[must_use]
    pub const fn passed(&self) -> bool {
        self.blockers.is_empty()
    }
}

/// Execute a finite Phase 11.9 evidence pass without account/execution wiring.
pub async fn run_phase_11_9_evidence_bootstrap(
    deploy: Arc<DeployConfig>,
    options: Phase119EvidenceBootstrapOptions,
) -> QuantResult<Phase119EvidenceManifest> {
    validate_options(&options)?;
    let shutdown = CancellationToken::new();
    let metrics = Arc::new(MetricsHub::new());
    let infra = InfraBundle::assemble(&deploy, Arc::clone(&metrics)).await?;
    let runtime = RuntimeSnapshot::bootstrap(&infra.repos, &deploy).await?;
    let (events, _event_rx) = CoreEventPublisher::bounded(128);
    let data = DataBundle::assemble_metadata_only(&DataBundleDeps {
        deploy: &deploy,
        shutdown: &shutdown,
        metrics: &metrics,
        infra: &infra,
        runtime: &runtime.config,
        events: &events,
    })?;

    let mut blockers = Vec::new();
    if options.sync_catalog
        && let Err(error) = data.gamma_service.sync().await
    {
        blockers.push(format!("catalog_sync: {error}"));
    }
    data.linkage_resolver.resolve_changed_markets(&[]).await?;

    let crypto_sources = options
        .categories
        .contains(&DomainFamily::Crypto)
        .then(|| connect_crypto_evidence_sources(&deploy))
        .transpose()?;
    let credential_ready_sources = crypto_sources
        .as_ref()
        .and_then(|sources| sources.chainlink.as_ref())
        .map(|_| DomainSourceId::chainlink_data_streams())
        .into_iter()
        .collect();

    let source_supervisor = Arc::new(DomainSourceSupervisor::new(
        Arc::clone(&infra.repos.domain_source_expectation)
            as Arc<dyn DomainSourceExpectationRepository>,
        Arc::clone(&infra.repos.market_linkage) as Arc<dyn MarketLinkageRepository>,
        deploy.domain_sources.weather_stations.clone(),
        deploy.domain_sources.weather_vertical_bindings.clone(),
        credential_ready_sources,
    )?);
    source_supervisor.ensure_boot_reconciled().await?;

    if let Some(crypto_sources) = crypto_sources
        && let Err(error) = run_crypto_evidence(
            &deploy,
            &infra,
            Arc::clone(&runtime.store),
            Arc::clone(&source_supervisor),
            crypto_sources,
            options.max_crypto_cycles,
            options.crypto_live_timeout_secs,
        )
        .await
    {
        blockers.push(format!("crypto_ingest: {error}"));
    }
    if options.categories.contains(&DomainFamily::Weather) {
        run_weather_evidence(
            &deploy,
            &infra,
            runtime.store,
            source_supervisor,
            &options.weather_stations,
            &mut blockers,
        )
        .await?;
    }

    build_manifest(&deploy, &infra, &options, blockers).await
}

fn validate_options(options: &Phase119EvidenceBootstrapOptions) -> QuantResult<()> {
    if options.categories.is_empty() {
        return Err(QuantError::config(
            "Phase 11.9 evidence bootstrap requires at least one category",
        ));
    }
    if options.categories.contains(&DomainFamily::Crypto) && options.max_crypto_cycles == 0 {
        return Err(QuantError::config(
            "Phase 11.9 Crypto evidence max cycles must be positive",
        ));
    }
    if options.categories.contains(&DomainFamily::Crypto) && options.crypto_live_timeout_secs == 0 {
        return Err(QuantError::config(
            "Phase 11.9 Crypto live evidence timeout must be positive",
        ));
    }
    Ok(())
}

fn connect_crypto_evidence_sources(deploy: &DeployConfig) -> QuantResult<CryptoEvidenceSources> {
    let sources = &deploy.domain_sources;
    if !sources.binance.enabled {
        return Err(QuantError::config(
            "Binance is disabled for Crypto evidence bootstrap",
        ));
    }
    if !sources.binance_usdm_futures.enabled {
        return Err(QuantError::config(
            "Binance USD-M Futures is disabled for Crypto evidence bootstrap",
        ));
    }
    let spot_budget = BinanceRequestBudget::new(&sources.binance)?;
    let kline_spot = Arc::new(BinanceKlineSource::connect_with_budget(
        sources.binance.clone(),
        spot_budget.clone(),
    )?);
    let agg_trade_spot = Arc::new(BinanceAggTradeSource::connect_with_budget(
        sources.binance.clone(),
        spot_budget,
    )?);
    let futures_budget = BinanceRequestBudget::new(&sources.binance_usdm_futures)?;
    let kline_usdm_futures = Arc::new(BinanceKlineSource::connect_usdm_futures_with_budget(
        sources.binance_usdm_futures.clone(),
        futures_budget.clone(),
    )?);
    let agg_trade_usdm_futures = Arc::new(BinanceAggTradeSource::connect_usdm_futures_with_budget(
        sources.binance_usdm_futures.clone(),
        futures_budget,
    )?);
    let rtds = sources.polymarket_rtds.enabled.then(|| {
        Arc::new(PolymarketRtdsSource::connect(
            sources.polymarket_rtds.clone(),
        ))
    });
    let chainlink = if sources.chainlink_data_streams.enabled {
        Some(Arc::new(ChainlinkDataStreamsSource::connect(
            sources.chainlink_data_streams.clone(),
        )?))
    } else {
        None
    };
    Ok(CryptoEvidenceSources {
        kline_spot,
        kline_usdm_futures,
        agg_trade_spot,
        agg_trade_usdm_futures,
        rtds,
        chainlink,
    })
}

async fn run_crypto_evidence(
    deploy: &DeployConfig,
    infra: &InfraBundle,
    runtime: Arc<DecisionPolicyStore>,
    source_supervisor: Arc<DomainSourceSupervisor>,
    sources: CryptoEvidenceSources,
    max_cycles: u16,
    live_timeout_secs: u64,
) -> QuantResult<()> {
    let station_registry =
        WeatherStationRegistry::try_new(deploy.domain_sources.weather_stations.clone())?;
    let capability_registry_hash = domain_capability_registry(
        &station_registry.registry_hash()?,
        &deploy.domain_sources.weather_vertical_bindings,
    )?
    .registry_hash;
    let kline_sources = vec![
        Arc::clone(&sources.kline_spot) as Arc<dyn DomainDataSource>,
        Arc::clone(&sources.kline_usdm_futures) as Arc<dyn DomainDataSource>,
    ];
    let archive_sources = vec![
        Arc::clone(&sources.kline_spot),
        Arc::clone(&sources.kline_usdm_futures),
    ];
    let writer = Arc::new(ChFactWriter::<DomainObservationRow>::new(
        Arc::clone(&infra.ch),
        Arc::clone(&infra.ch_write_manager),
        "quant_domain_observation",
    )) as Arc<dyn FactWriter<DomainObservationRow>>;
    let kline_worker = CryptoKlineIngestWorker::new(
        Arc::new(CryptoKlineIngestor::new(
            kline_sources,
            archive_sources,
            Arc::clone(&infra.repos.domain_source_cursor) as Arc<dyn DomainSourceCursorRepository>,
            writer,
            runtime,
            deploy.domain_sources.clone(),
        )),
        Arc::clone(&source_supervisor),
        deploy
            .domain_sources
            .binance
            .kline_poll_secs
            .min(deploy.domain_sources.binance_usdm_futures.kline_poll_secs),
    );
    let crypto_writer = Arc::new(ChFactWriter::<CryptoPriceReportRow>::new(
        Arc::clone(&infra.ch),
        Arc::clone(&infra.ch_write_manager),
        "quant_crypto_price_report",
    )) as Arc<dyn FactWriter<CryptoPriceReportRow>>;
    let live_worker = Arc::new(CryptoLiveIngestWorker::new(CryptoLiveIngestDeps {
        source_supervisor: Arc::clone(&source_supervisor),
        linkages: Arc::clone(&infra.repos.market_linkage) as Arc<dyn MarketLinkageRepository>,
        cursors: Arc::clone(&infra.repos.domain_source_cursor)
            as Arc<dyn DomainSourceCursorRepository>,
        projections: Arc::clone(&infra.repos.domain_projection)
            as Arc<dyn DomainProjectionRepository>,
        crypto_writer: Arc::clone(&crypto_writer),
        binance: Some(sources.agg_trade_spot),
        binance_usdm_futures: Some(sources.agg_trade_usdm_futures),
        chainlink: sources.chainlink,
    }));
    let rtds_worker = Arc::new(CryptoRtdsIngestWorker::new(CryptoRtdsIngestDeps {
        source_supervisor,
        linkages: Arc::clone(&infra.repos.market_linkage) as Arc<dyn MarketLinkageRepository>,
        cursors: Arc::clone(&infra.repos.domain_source_cursor)
            as Arc<dyn DomainSourceCursorRepository>,
        projections: Arc::clone(&infra.repos.domain_projection)
            as Arc<dyn DomainProjectionRepository>,
        writer: crypto_writer,
        source: sources.rtds,
    }));
    let shutdown = CancellationToken::new();
    let live_handle = tokio::spawn(Arc::clone(&live_worker).run(shutdown.child_token()));
    let rtds_handle = tokio::spawn(Arc::clone(&rtds_worker).run(shutdown.child_token()));
    let evidence = async {
        for _ in 0..max_cycles {
            kline_worker.run_once().await?;
            let spot_ready =
                source_is_ready(infra, &capability_registry_hash, &DomainSourceId::binance())
                    .await?;
            let futures_ready = source_is_ready(
                infra,
                &capability_registry_hash,
                &DomainSourceId::binance_usdm_futures(),
            )
            .await?;
            if spot_ready && futures_ready {
                break;
            }
        }
        wait_for_crypto_source_readiness(infra, &capability_registry_hash, live_timeout_secs).await
    }
    .await;
    shutdown.cancel();
    let shutdown_result = stop_crypto_workers(live_handle, rtds_handle).await;
    evidence?;
    shutdown_result
}

async fn source_is_ready(
    infra: &InfraBundle,
    capability_registry_hash: &ContentHash,
    source_id: &DomainSourceId,
) -> QuantResult<bool> {
    let expectations = current_crypto_expectations(infra, capability_registry_hash)
        .await?
        .into_iter()
        .filter(|expectation| expectation.source_id == *source_id)
        .collect::<Vec<_>>();
    if expectations.is_empty() {
        return Ok(false);
    }
    let cursors = infra.repos.domain_source_cursor.list_all().await?;
    Ok(crypto_source_blockers(&expectations, &cursors).is_empty())
}

async fn wait_for_crypto_source_readiness(
    infra: &InfraBundle,
    capability_registry_hash: &ContentHash,
    timeout_secs: u64,
) -> QuantResult<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let expectations = current_crypto_expectations(infra, capability_registry_hash).await?;
        let cursors = infra.repos.domain_source_cursor.list_all().await?;
        let blockers = crypto_source_blockers(&expectations, &cursors);
        if !expectations.is_empty() && blockers.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(QuantError::config(format!(
                "Crypto source readiness timed out after {timeout_secs}s: {}",
                blockers.into_iter().take(16).collect::<Vec<_>>().join("; ")
            )));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn current_crypto_expectations(
    infra: &InfraBundle,
    capability_registry_hash: &ContentHash,
) -> QuantResult<Vec<DomainSourceExpectationInfo>> {
    Ok(infra
        .repos
        .domain_source_expectation
        .list_all()
        .await?
        .into_iter()
        .filter(|expectation| {
            expectation.family == DomainFamily::Crypto
                && expectation.capability_registry_hash == *capability_registry_hash
        })
        .collect())
}

fn crypto_source_blockers(
    expectations: &[DomainSourceExpectationInfo],
    cursors: &[DomainSourceCursorInfo],
) -> Vec<String> {
    let cursor_statuses = cursors
        .iter()
        .map(|cursor| {
            (
                (cursor.source_id.clone(), cursor.instrument_key.clone()),
                cursor.status,
            )
        })
        .collect::<BTreeMap<_, _>>();
    expectations
        .iter()
        .filter_map(|expectation| {
            if expectation.status == DomainSourceExpectationStatus::CredentialBlocked
                && expectation.credential_required
            {
                return None;
            }
            if expectation.status != DomainSourceExpectationStatus::Live {
                return Some(format!(
                    "{}/{} expectation={}",
                    expectation.source_id, expectation.instrument_key, expectation.status
                ));
            }
            let cursor_status = cursor_statuses.get(&(
                expectation.source_id.clone(),
                expectation.instrument_key.clone(),
            ));
            (cursor_status.copied() != Some(DomainCursorStatus::Live)).then(|| {
                format!(
                    "{}/{} cursor={}",
                    expectation.source_id,
                    expectation.instrument_key,
                    cursor_status.map_or_else(|| "missing".to_owned(), ToString::to_string)
                )
            })
        })
        .collect()
}

async fn stop_crypto_workers(
    live_handle: JoinHandle<()>,
    rtds_handle: JoinHandle<()>,
) -> QuantResult<()> {
    stop_crypto_worker("CryptoLiveIngestWorker", live_handle).await?;
    stop_crypto_worker("CryptoRtdsIngestWorker", rtds_handle).await
}

async fn stop_crypto_worker(name: &str, mut handle: JoinHandle<()>) -> QuantResult<()> {
    if let Ok(result) = tokio::time::timeout(CRYPTO_WORKER_SHUTDOWN_TIMEOUT, &mut handle).await {
        return result.map_err(|error| {
            QuantError::config(format!("{name} evidence task join failed: {error}"))
        });
    }
    handle.abort();
    let _ = handle.await;
    Err(QuantError::config(format!(
        "{name} did not stop within {}s",
        CRYPTO_WORKER_SHUTDOWN_TIMEOUT.as_secs()
    )))
}

async fn run_weather_evidence(
    deploy: &DeployConfig,
    infra: &InfraBundle,
    runtime: Arc<DecisionPolicyStore>,
    source_supervisor: Arc<DomainSourceSupervisor>,
    weather_stations: &BTreeSet<String>,
    blockers: &mut Vec<String>,
) -> QuantResult<()> {
    let weather_writer = Arc::new(ChFactWriter::<WeatherObservationFactRow>::new(
        Arc::clone(&infra.ch),
        Arc::clone(&infra.ch_write_manager),
        "quant_weather_observation_fact",
    )) as Arc<dyn FactWriter<WeatherObservationFactRow>>;
    let forecast_writer = Arc::new(ChFactWriter::<WeatherForecastFactRow>::new(
        Arc::clone(&infra.ch),
        Arc::clone(&infra.ch_write_manager),
        "quant_weather_forecast_fact",
    )) as Arc<dyn FactWriter<WeatherForecastFactRow>>;
    let facts = Arc::new(WeatherFactIngestService::new(
        Arc::clone(&weather_writer),
        Arc::clone(&forecast_writer),
        Arc::clone(&infra.quant_fact_read),
    ));
    let sources = &deploy.domain_sources;
    let public = WeatherPublicIngestWorker::new(WeatherPublicIngestDeps {
        source_supervisor: Arc::clone(&source_supervisor),
        cursors: Arc::clone(&infra.repos.domain_source_cursor)
            as Arc<dyn DomainSourceCursorRepository>,
        facts,
        hko: connect_hko(sources)?,
        airnow: connect_airnow(sources)?,
        tornado: connect_tornado(sources)?,
        nhc: connect_nhc(sources)?,
        gistemp: connect_gistemp(sources)?,
        nsidc: connect_nsidc(sources)?,
        nws: connect_nws(sources)?,
        hko_config: sources.hko_open_data.clone(),
        airnow_config: sources.airnow.clone(),
        tornado_config: sources.tornado.clone(),
        nhc_config: sources.nhc.clone(),
        gistemp_config: sources.nasa_gistemp.clone(),
        nsidc_config: sources.nsidc_sea_ice.clone(),
        nws_config: sources.nws_observation.clone(),
        bindings: sources.weather_vertical_bindings.clone(),
    });
    if let Err(error) = public.run_once().await {
        blockers.push(format!("weather_public_ingest: {error}"));
    }

    let daily = WeatherIngestWorker::new(WeatherIngestDeps {
        source_supervisor,
        linkages: Arc::clone(&infra.repos.market_linkage) as Arc<dyn MarketLinkageRepository>,
        cursors: Arc::clone(&infra.repos.domain_source_cursor)
            as Arc<dyn DomainSourceCursorRepository>,
        projections: Arc::clone(&infra.repos.domain_projection)
            as Arc<dyn DomainProjectionRepository>,
        weather_writer,
        forecast_writer,
        fact_read: Arc::clone(&infra.quant_fact_read),
        calibrations: Arc::clone(&infra.repos.calibration_artifact)
            as Arc<dyn CalibrationArtifactRepository>,
        runtime_config: runtime,
        aviation: connect_aviation(sources)?,
        ghcnh: connect_ghcnh(sources)?,
        gefs: connect_gefs(sources)?,
        aviation_config: sources.aviation_weather.clone(),
        ghcnh_config: sources.ghcnh.clone(),
        gefs_config: sources.gefs.clone(),
        station_profiles: sources.weather_stations.clone(),
    });
    if let Err(error) = daily.run_evidence_once(weather_stations).await {
        blockers.push(format!("weather_daily_temperature_ingest: {error}"));
    }
    Ok(())
}

async fn build_manifest(
    deploy: &DeployConfig,
    infra: &InfraBundle,
    options: &Phase119EvidenceBootstrapOptions,
    mut blockers: Vec<String>,
) -> QuantResult<Phase119EvidenceManifest> {
    let markets = infra.repos.market.find_active().await?;
    let event_ids = markets
        .iter()
        .filter(|market| {
            let categories = market.category_set();
            categories.contains(MarketCategory::Crypto)
                || categories.contains(MarketCategory::Weather)
        })
        .map(|market| market.event_id.clone())
        .collect::<BTreeSet<EventId>>()
        .into_iter()
        .collect::<Vec<_>>();
    let events = infra
        .repos
        .event
        .find_by_ids(&event_ids)
        .await?
        .into_iter()
        .map(|event| (event.event_id.clone(), event))
        .collect::<BTreeMap<_, _>>();
    let station_registry =
        WeatherStationRegistry::try_new(deploy.domain_sources.weather_stations.clone())?;
    let registry = domain_capability_registry(
        &station_registry.registry_hash()?,
        &deploy.domain_sources.weather_vertical_bindings,
    )?;
    let classifier = DomainCatalogClassifier::new(
        station_registry,
        &deploy.domain_sources.weather_vertical_bindings,
    )?;
    let artifact = classifier.classify_catalog(&markets, &events)?;
    let mut classification_outcomes = BTreeMap::new();
    for row in &artifact.classifications {
        *classification_outcomes
            .entry(classification_outcome_label(row.outcome).to_owned())
            .or_insert(0_u64) += 1;
    }
    if classification_outcomes
        .get("unsupported_template")
        .copied()
        .unwrap_or(0)
        != 0
    {
        blockers.push("catalog contains UnsupportedTemplate remediation blockers".to_owned());
    }

    let source_ledger =
        audit_scoped_source_ledger(infra, options, &registry.registry_hash, &mut blockers).await?;
    let linkage_count = u64::try_from(
        infra
            .repos
            .market_linkage
            .latest_for_active_markets()
            .await?
            .len(),
    )
    .map_err(|error| QuantError::config(format!("linkage count overflow: {error}")))?;
    let fact_idempotency = fact_idempotency_evidence(infra).await?;
    for (table_name, evidence) in &fact_idempotency {
        if evidence.duplicate_rows != 0 || evidence.revision_conflicts != 0 {
            blockers.push(format!(
                "fact idempotency failed for {table_name}: duplicate_rows={}, revision_conflicts={}",
                evidence.duplicate_rows, evidence.revision_conflicts
            ));
        }
    }
    let fact_rows = |table_name: &str| {
        fact_idempotency
            .get(table_name)
            .map(|evidence| evidence.physical_rows)
            .ok_or_else(|| {
                QuantError::config(format!(
                    "ClickHouse fact idempotency evidence is absent for `{table_name}`"
                ))
            })
    };
    let crypto_rows = fact_rows("quant_domain_observation")?;
    let crypto_price_report_rows = fact_rows("quant_crypto_price_report")?;
    let weather_observation_rows = fact_rows("quant_weather_observation_fact")?;
    let weather_forecast_rows = fact_rows("quant_weather_forecast_fact")?;
    let gistemp_historical_time = if options.categories.contains(&DomainFamily::Weather) {
        let evidence = gistemp_historical_time_evidence(infra).await?;
        if !evidence.verified {
            blockers.push(format!(
                "NASA GISTEMP historical time contract failed: {evidence:?}"
            ));
        }
        Some(evidence)
    } else {
        None
    };
    if options.categories.contains(&DomainFamily::Crypto) && crypto_rows == 0 {
        blockers.push("Crypto ClickHouse observation facts are empty".to_owned());
    }
    if options.categories.contains(&DomainFamily::Crypto) && crypto_price_report_rows == 0 {
        blockers.push("Crypto ClickHouse price-report facts are empty".to_owned());
    }
    if options.categories.contains(&DomainFamily::Weather) {
        if weather_observation_rows == 0 {
            blockers.push("Weather ClickHouse observation facts are empty".to_owned());
        }
        if weather_forecast_rows == 0 {
            blockers.push("Weather ClickHouse forecast facts are empty".to_owned());
        }
    }
    blockers.sort();
    blockers.dedup();

    Ok(Phase119EvidenceManifest {
        format_version: EVIDENCE_FORMAT_VERSION,
        categories: options
            .categories
            .iter()
            .map(|family| family.as_str().to_owned())
            .collect(),
        weather_stations: options.weather_stations.iter().cloned().collect(),
        capability_registry_hash: registry.registry_hash,
        catalog_hash: artifact.catalog_hash,
        catalog_market_count: u64::try_from(artifact.classifications.len()).map_err(|error| {
            QuantError::config(format!("catalog classification count overflow: {error}"))
        })?,
        classification_outcomes,
        linkage_count,
        source_expectation_count: source_ledger.expectation_count,
        source_expectation_statuses: source_ledger.expectation_statuses,
        source_cursor_count: source_ledger.cursor_count,
        source_cursor_statuses: source_ledger.cursor_statuses,
        crypto_observation_rows: crypto_rows,
        crypto_price_report_rows,
        weather_observation_rows,
        weather_forecast_rows,
        fact_idempotency,
        gistemp_historical_time,
        blockers,
    })
}

async fn audit_scoped_source_ledger(
    infra: &InfraBundle,
    options: &Phase119EvidenceBootstrapOptions,
    capability_registry_hash: &ContentHash,
    blockers: &mut Vec<String>,
) -> QuantResult<ScopedSourceLedgerEvidence> {
    let expectations = infra
        .repos
        .domain_source_expectation
        .list_all()
        .await?
        .into_iter()
        .filter(|expectation| {
            options.categories.contains(&expectation.family)
                && expectation.capability_registry_hash == *capability_registry_hash
        })
        .collect::<Vec<_>>();
    let mut expectation_statuses = BTreeMap::new();
    for expectation in &expectations {
        *expectation_statuses
            .entry(expectation.status.as_str().to_owned())
            .or_insert(0_u64) += 1;
    }
    let expected_bindings = expectations
        .iter()
        .map(|expectation| {
            (
                expectation.source_id.clone(),
                expectation.instrument_key.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    let cursors = infra
        .repos
        .domain_source_cursor
        .list_all()
        .await?
        .into_iter()
        .filter(|cursor| {
            expected_bindings.contains(&(cursor.source_id.clone(), cursor.instrument_key.clone()))
        })
        .collect::<Vec<_>>();
    let mut cursor_statuses = BTreeMap::new();
    for cursor in &cursors {
        *cursor_statuses.entry(cursor.status).or_insert(0_u64) += 1;
    }
    audit_source_states(&expectations, &cursors, blockers);
    if expectations.is_empty() {
        blockers.push("scoped domain source expectation ledger is empty".to_owned());
    }
    if cursors.is_empty() {
        blockers.push("scoped domain source cursor ledger is empty".to_owned());
    }
    Ok(ScopedSourceLedgerEvidence {
        expectation_count: u64::try_from(expectations.len())
            .map_err(|error| QuantError::config(format!("expectation count overflow: {error}")))?,
        expectation_statuses,
        cursor_count: u64::try_from(cursors.len())
            .map_err(|error| QuantError::config(format!("cursor count overflow: {error}")))?,
        cursor_statuses,
    })
}

fn audit_source_states(
    expectations: &[DomainSourceExpectationInfo],
    cursors: &[DomainSourceCursorInfo],
    blockers: &mut Vec<String>,
) {
    let cursor_bindings = cursors
        .iter()
        .map(|cursor| (cursor.source_id.clone(), cursor.instrument_key.clone()))
        .collect::<BTreeSet<_>>();
    for expectation in expectations {
        if !expectation.required {
            continue;
        }
        match expectation.status {
            DomainSourceExpectationStatus::Live => {
                if !cursor_bindings.contains(&(
                    expectation.source_id.clone(),
                    expectation.instrument_key.clone(),
                )) {
                    blockers.push(format!(
                        "live source expectation has no cursor: {}/{}",
                        expectation.source_id, expectation.instrument_key
                    ));
                }
            }
            DomainSourceExpectationStatus::CredentialBlocked if expectation.credential_required => {
            }
            DomainSourceExpectationStatus::NotStarted
            | DomainSourceExpectationStatus::Stale
            | DomainSourceExpectationStatus::Failed
            | DomainSourceExpectationStatus::Unsupported
            | DomainSourceExpectationStatus::CredentialBlocked => blockers.push(format!(
                "source expectation is not ready: {}/{} ({})",
                expectation.source_id, expectation.instrument_key, expectation.status
            )),
        }
    }
    let required_bindings = expectations
        .iter()
        .filter(|expectation| expectation.required)
        .map(|expectation| {
            (
                expectation.source_id.clone(),
                expectation.instrument_key.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    for cursor in cursors {
        if required_bindings.contains(&(cursor.source_id.clone(), cursor.instrument_key.clone()))
            && cursor.status != DomainCursorStatus::Live
        {
            blockers.push(format!(
                "source cursor is not live: {}/{} ({})",
                cursor.source_id, cursor.instrument_key, cursor.status
            ));
        }
    }
}

async fn gistemp_historical_time_evidence(
    infra: &InfraBundle,
) -> QuantResult<GistempHistoricalTimeEvidence> {
    let raw = ChNativeReadRepository::new(Arc::clone(&infra.ch))
        .gistemp_historical_time()
        .await?;
    if raw.row_count == 0 {
        return Ok(GistempHistoricalTimeEvidence {
            row_count: raw.row_count,
            earliest_local_date_epoch_days: None,
            earliest_observed_at_ms: None,
            earliest_valid_from_ms: None,
            earliest_valid_to_ms: None,
            null_valid_from_rows: 0,
            null_valid_to_rows: 0,
            verified: false,
        });
    }

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| QuantError::config("invalid Unix epoch date constant"))?;
    let first_month = NaiveDate::from_ymd_opt(1880, 1, 1)
        .ok_or_else(|| QuantError::config("invalid GISTEMP first-month date constant"))?;
    let first_observation = NaiveDate::from_ymd_opt(1880, 2, 1)
        .ok_or_else(|| QuantError::config("invalid GISTEMP first-observation date constant"))?;
    let expected_local_date =
        i32::try_from(first_observation.signed_duration_since(epoch).num_days())
            .map_err(|error| QuantError::config(format!("GISTEMP epoch day overflow: {error}")))?;
    let expected_valid_from = first_month
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| QuantError::config("invalid GISTEMP first-month time constant"))?
        .and_utc()
        .timestamp_millis();
    let expected_observed_at = first_observation
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| QuantError::config("invalid GISTEMP first-observation time constant"))?
        .and_utc()
        .timestamp_millis();
    let verified = raw.earliest_local_date_epoch_days == Some(expected_local_date)
        && raw.earliest_observed_at_ms == Some(expected_observed_at)
        && raw.earliest_valid_from_ms == Some(expected_valid_from)
        && raw.earliest_valid_to_ms == Some(expected_observed_at)
        && raw.null_valid_from_rows == 0
        && raw.null_valid_to_rows == 0;

    Ok(GistempHistoricalTimeEvidence {
        row_count: raw.row_count,
        earliest_local_date_epoch_days: raw.earliest_local_date_epoch_days,
        earliest_observed_at_ms: raw.earliest_observed_at_ms,
        earliest_valid_from_ms: raw.earliest_valid_from_ms,
        earliest_valid_to_ms: raw.earliest_valid_to_ms,
        null_valid_from_rows: raw.null_valid_from_rows,
        null_valid_to_rows: raw.null_valid_to_rows,
        verified,
    })
}

async fn fact_idempotency_evidence(
    infra: &InfraBundle,
) -> QuantResult<BTreeMap<String, FactIdempotencyEvidence>> {
    let repository = ChNativeReadRepository::new(Arc::clone(&infra.ch));
    let (domain, crypto_reports, weather_observations, weather_forecasts) = tokio::try_join!(
        repository.fact_idempotency(FactEvidenceTable::DomainCrypto),
        repository.fact_idempotency(FactEvidenceTable::CryptoPriceReport),
        repository.fact_idempotency(FactEvidenceTable::WeatherObservation),
        repository.fact_idempotency(FactEvidenceTable::WeatherForecast),
    )?;
    Ok([
        (FactEvidenceTable::DomainCrypto, domain),
        (FactEvidenceTable::CryptoPriceReport, crypto_reports),
        (FactEvidenceTable::WeatherObservation, weather_observations),
        (FactEvidenceTable::WeatherForecast, weather_forecasts),
    ]
    .into_iter()
    .map(|(table, counts)| {
        (
            table.table_name().to_owned(),
            FactIdempotencyEvidence {
                physical_rows: counts.physical_rows,
                logical_keys: counts.logical_keys,
                duplicate_rows: counts.duplicate_rows,
                revision_conflicts: counts.revision_conflicts,
            },
        )
    })
    .collect())
}

const fn classification_outcome_label(outcome: DomainMarketClassificationOutcome) -> &'static str {
    match outcome {
        DomainMarketClassificationOutcome::Supported => "supported",
        DomainMarketClassificationOutcome::CredentialBlocked { .. } => "credential_blocked",
        DomainMarketClassificationOutcome::InsufficientEvidence { .. } => "insufficient_evidence",
        DomainMarketClassificationOutcome::Excluded { .. } => "excluded",
        DomainMarketClassificationOutcome::UnsupportedTemplate { .. } => "unsupported_template",
    }
}

fn connect_hko(sources: &DomainSourcesConfig) -> QuantResult<Option<Arc<HkoOpenDataSource>>> {
    sources
        .hko_open_data
        .enabled
        .then(|| HkoOpenDataSource::connect(sources.hko_open_data.clone()).map(Arc::new))
        .transpose()
}

fn connect_airnow(sources: &DomainSourcesConfig) -> QuantResult<Option<Arc<AirNowSource>>> {
    sources
        .airnow
        .enabled
        .then(|| AirNowSource::connect(sources.airnow.clone()).map(Arc::new))
        .transpose()
}

fn connect_tornado(sources: &DomainSourcesConfig) -> QuantResult<Option<Arc<TornadoSource>>> {
    sources
        .tornado
        .enabled
        .then(|| TornadoSource::connect(sources.tornado.clone()).map(Arc::new))
        .transpose()
}

fn connect_nhc(sources: &DomainSourcesConfig) -> QuantResult<Option<Arc<NhcSource>>> {
    sources
        .nhc
        .enabled
        .then(|| NhcSource::connect(sources.nhc.clone()).map(Arc::new))
        .transpose()
}

fn connect_gistemp(sources: &DomainSourcesConfig) -> QuantResult<Option<Arc<NasaGistempSource>>> {
    sources
        .nasa_gistemp
        .enabled
        .then(|| NasaGistempSource::connect(sources.nasa_gistemp.clone()).map(Arc::new))
        .transpose()
}

fn connect_nsidc(sources: &DomainSourcesConfig) -> QuantResult<Option<Arc<NsidcSeaIceSource>>> {
    sources
        .nsidc_sea_ice
        .enabled
        .then(|| NsidcSeaIceSource::connect(sources.nsidc_sea_ice.clone()).map(Arc::new))
        .transpose()
}

fn connect_nws(sources: &DomainSourcesConfig) -> QuantResult<Option<Arc<NwsObservationSource>>> {
    sources
        .nws_observation
        .enabled
        .then(|| NwsObservationSource::connect(sources.nws_observation.clone()).map(Arc::new))
        .transpose()
}

fn connect_aviation(
    sources: &DomainSourcesConfig,
) -> QuantResult<Option<Arc<AviationWeatherSource>>> {
    sources
        .aviation_weather
        .enabled
        .then(|| AviationWeatherSource::connect(sources.aviation_weather.clone()).map(Arc::new))
        .transpose()
}

fn connect_ghcnh(sources: &DomainSourcesConfig) -> QuantResult<Option<Arc<GhcnhSource>>> {
    sources
        .ghcnh
        .enabled
        .then(|| GhcnhSource::connect(sources.ghcnh.clone()).map(Arc::new))
        .transpose()
}

fn connect_gefs(sources: &DomainSourcesConfig) -> QuantResult<Option<Arc<GefsSource>>> {
    sources
        .gefs
        .enabled
        .then(|| GefsSource::connect(sources.gefs.clone()).map(Arc::new))
        .transpose()
}
