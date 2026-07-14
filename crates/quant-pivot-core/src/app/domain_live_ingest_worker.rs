//! Dynamic live-event ingestion for active Crypto and Weather linkages.

use std::{
    collections::{BTreeMap, BTreeSet},
    slice,
    sync::Arc,
    time::Duration as StdDuration,
};

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use futures_util::{StreamExt, TryStreamExt, stream};
use quant_pivot_api::{
    binance::BinanceAggTradeSource,
    chainlink::ChainlinkDataStreamsSource,
    weather::{
        AviationWeatherSource, GefsDecodedMember, GefsSource, GefsStationBinding, GhcnhSource,
    },
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    clickhouse::{
        ChDecimal64, ChSchemaVersion, CryptoPriceReportRow, WeatherForecastPointRow,
        WeatherObservationReportRow,
    },
    config::{
        AviationWeatherSourceConfig, GefsSourceConfig, GhcnhSourceConfig,
        WeatherStationProfileConfig,
    },
    domain::{
        CryptoPriceReport, DomainCursorStatus, DomainSourceCheckpoint, DomainSourceCursorInfo,
        LinkageOutcome, MarketLinkage, MarketSubject, UpsertDomainSourceCursor,
        WeatherObservationReport,
    },
    enums::domain::LinkageSourceRole,
    hashing::CanonicalDigest,
    types::{
        BinanceSymbol, ChainlinkFeedKey, ContentHash, DomainInstrumentKey, DomainSourceId,
        IcaoStation,
    },
};
use quant_pivot_repository::traits::{
    DomainProjectionRepository, DomainSourceCursorRepository, FactWriter, MarketLinkageRepository,
};
use tokio::{task::JoinHandle, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;

const DISCOVERY_INTERVAL: StdDuration = StdDuration::from_secs(30);
const RECONNECT_BACKOFF: StdDuration = StdDuration::from_secs(2);
const BINANCE_PAGE_SIZE: u16 = 1_000;
const AVIATION_REQUEST_SPACING: StdDuration = StdDuration::from_millis(650);

#[derive(Clone)]
struct WeatherTarget {
    station: IcaoStation,
    timezone: Tz,
    local_date: NaiveDate,
    latitude: rust_decimal::Decimal,
    longitude: rust_decimal::Decimal,
    ghcnh_station_id: String,
    station_profile_hash: ContentHash,
}

#[derive(Default)]
struct LiveBindings {
    binance: BTreeMap<DomainInstrumentKey, BinanceSymbol>,
    chainlink: BTreeMap<DomainInstrumentKey, ChainlinkFeedKey>,
    weather: BTreeMap<(String, NaiveDate), WeatherTarget>,
}

struct SourceTask {
    cancel: CancellationToken,
    handle: JoinHandle<()>,
}

/// Supervises source-native workers from the latest resolved active-market
/// bindings. Static Crypto rules and configured Weather profiles never create
/// subscriptions by themselves.
pub struct DomainLiveIngestWorker {
    linkages: Arc<dyn MarketLinkageRepository>,
    cursors: Arc<dyn DomainSourceCursorRepository>,
    projections: Arc<dyn DomainProjectionRepository>,
    crypto_writer: Arc<dyn FactWriter<CryptoPriceReportRow>>,
    weather_writer: Arc<dyn FactWriter<WeatherObservationReportRow>>,
    forecast_writer: Arc<dyn FactWriter<WeatherForecastPointRow>>,
    binance: Option<Arc<BinanceAggTradeSource>>,
    chainlink: Option<Arc<ChainlinkDataStreamsSource>>,
    aviation: Option<Arc<AviationWeatherSource>>,
    ghcnh: Option<Arc<GhcnhSource>>,
    gefs: Option<Arc<GefsSource>>,
    aviation_config: AviationWeatherSourceConfig,
    ghcnh_config: GhcnhSourceConfig,
    gefs_config: GefsSourceConfig,
    station_profiles: BTreeMap<String, WeatherStationProfileConfig>,
}

pub struct DomainLiveIngestDeps {
    pub linkages: Arc<dyn MarketLinkageRepository>,
    pub cursors: Arc<dyn DomainSourceCursorRepository>,
    pub projections: Arc<dyn DomainProjectionRepository>,
    pub crypto_writer: Arc<dyn FactWriter<CryptoPriceReportRow>>,
    pub weather_writer: Arc<dyn FactWriter<WeatherObservationReportRow>>,
    pub forecast_writer: Arc<dyn FactWriter<WeatherForecastPointRow>>,
    pub binance: Option<Arc<BinanceAggTradeSource>>,
    pub chainlink: Option<Arc<ChainlinkDataStreamsSource>>,
    pub aviation: Option<Arc<AviationWeatherSource>>,
    pub ghcnh: Option<Arc<GhcnhSource>>,
    pub gefs: Option<Arc<GefsSource>>,
    pub aviation_config: AviationWeatherSourceConfig,
    pub ghcnh_config: GhcnhSourceConfig,
    pub gefs_config: GefsSourceConfig,
    pub station_profiles: BTreeMap<String, WeatherStationProfileConfig>,
}

impl DomainLiveIngestWorker {
    #[must_use]
    pub fn new(deps: DomainLiveIngestDeps) -> Self {
        Self {
            linkages: deps.linkages,
            cursors: deps.cursors,
            projections: deps.projections,
            crypto_writer: deps.crypto_writer,
            weather_writer: deps.weather_writer,
            forecast_writer: deps.forecast_writer,
            binance: deps.binance,
            chainlink: deps.chainlink,
            aviation: deps.aviation,
            ghcnh: deps.ghcnh,
            gefs: deps.gefs,
            aviation_config: deps.aviation_config,
            ghcnh_config: deps.ghcnh_config,
            gefs_config: deps.gefs_config,
            station_profiles: deps.station_profiles,
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let binance = Arc::clone(&self).run_binance_supervisor(shutdown.child_token());
        let chainlink = Arc::clone(&self).run_chainlink_supervisor(shutdown.child_token());
        let weather = Arc::clone(&self).run_weather_loop(shutdown.child_token());
        let ghcnh = Arc::clone(&self).run_ghcnh_loop(shutdown.child_token());
        let gefs = Arc::clone(&self).run_gefs_loop(shutdown.child_token());
        tokio::join!(binance, chainlink, weather, ghcnh, gefs);
    }

    async fn discover(&self) -> QuantResult<LiveBindings> {
        let rows = self.linkages.latest_for_active_markets().await?;
        let mut bindings = LiveBindings::default();
        for row in rows {
            let linkage = row
                .into_domain()
                .map_err(|error| QuantError::config(format!("invalid linkage payload: {error}")))?;
            self.add_discovered_linkage(&mut bindings, linkage)?;
        }
        Ok(bindings)
    }

    fn add_discovered_linkage(
        &self,
        bindings: &mut LiveBindings,
        linkage: MarketLinkage,
    ) -> QuantResult<()> {
        let LinkageOutcome::Resolved(resolved) = linkage.outcome else {
            return Ok(());
        };
        let live = resolved
            .source_bindings
            .iter()
            .find(|binding| binding.role == LinkageSourceRole::LiveEvent);
        match (&resolved.subject, live) {
            (MarketSubject::Crypto(_), Some(source))
                if source.source_id == DomainSourceId::binance_agg_trade() =>
            {
                let symbol = source
                    .instrument_key
                    .as_binance_agg_trade_symbol()
                    .ok_or_else(|| {
                        QuantError::config("invalid Binance live-event linkage instrument")
                    })?;
                bindings
                    .binance
                    .insert(source.instrument_key.clone(), symbol);
            }
            (MarketSubject::Crypto(_), Some(source))
                if source.source_id == DomainSourceId::chainlink_data_streams() =>
            {
                let feed = source.instrument_key.as_chainlink_feed().ok_or_else(|| {
                    QuantError::config("invalid Chainlink live-event linkage instrument")
                })?;
                bindings
                    .chainlink
                    .insert(source.instrument_key.clone(), feed);
            }
            (MarketSubject::Weather(subject), Some(source))
                if source.source_id == DomainSourceId::aviation_weather() =>
            {
                let profile = self
                    .station_profiles
                    .get(subject.station.as_str())
                    .ok_or_else(|| {
                        QuantError::config(format!(
                            "Weather station {} is absent from deploy profiles",
                            subject.station
                        ))
                    })?;
                let profile_hash = CanonicalDigest::content_hash_json(&(
                    "weather_station_profile_v1",
                    &subject.station,
                    profile,
                ))?;
                if profile_hash != subject.station_profile_hash
                    || profile.timezone != subject.timezone
                {
                    return Err(QuantError::config(format!(
                        "Weather station profile drift for {}",
                        subject.station
                    )));
                }
                let station = source
                    .instrument_key
                    .as_aviation_weather_station()
                    .filter(|station| station == &subject.station)
                    .ok_or_else(|| {
                        QuantError::config("Weather subject/source station binding mismatch")
                    })?;
                let timezone = subject.timezone.parse::<Tz>().map_err(|error| {
                    QuantError::config(format!("invalid Weather linkage timezone: {error}"))
                })?;
                bindings.weather.insert(
                    (station.to_string(), subject.local_date),
                    WeatherTarget {
                        station,
                        timezone,
                        local_date: subject.local_date,
                        latitude: profile.latitude,
                        longitude: profile.longitude,
                        ghcnh_station_id: profile.ghcnh_station_id.clone(),
                        station_profile_hash: profile_hash,
                    },
                );
            }
            (MarketSubject::Crypto(_) | MarketSubject::Weather(_), None) => {
                tracing::warn!(
                    market_id = %linkage.market_id,
                    "resolved active linkage has no live-event source binding"
                );
            }
            _ => {
                return Err(QuantError::config(format!(
                    "active linkage {} has a cross-family live-event source",
                    linkage.market_id
                )));
            }
        }
        Ok(())
    }

    async fn run_binance_supervisor(self: Arc<Self>, shutdown: CancellationToken) {
        let mut tasks = BTreeMap::<DomainInstrumentKey, SourceTask>::new();
        let mut interval = tokio::time::interval(DISCOVERY_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            let desired = match self.discover().await {
                Ok(bindings) => bindings.binance,
                Err(error) => {
                    tracing::warn!(%error, "Binance live binding discovery failed");
                    continue;
                }
            };
            stop_removed(&mut tasks, desired.keys().collect()).await;
            let Some(source) = self.binance.as_ref() else {
                for instrument in desired.keys() {
                    self.mark_crypto_unavailable(instrument).await;
                }
                continue;
            };
            for (instrument, symbol) in desired {
                if tasks.contains_key(&instrument) {
                    continue;
                }
                let cancel = shutdown.child_token();
                let child_cancel = cancel.clone();
                let worker = Arc::clone(&self);
                let source = Arc::clone(source);
                let task_instrument = instrument.clone();
                let handle = tokio::spawn(async move {
                    worker
                        .run_binance_symbol(source, task_instrument, symbol, child_cancel)
                        .await;
                });
                tasks.insert(instrument, SourceTask { cancel, handle });
            }
        }
        stop_all(tasks).await;
    }

    async fn run_binance_symbol(
        &self,
        source: Arc<BinanceAggTradeSource>,
        instrument: DomainInstrumentKey,
        symbol: BinanceSymbol,
        shutdown: CancellationToken,
    ) {
        let mut gap_generation = self
            .projections
            .mark_crypto_source_gap(
                &DomainSourceId::binance_agg_trade(),
                &instrument,
                Utc::now(),
            )
            .await
            .unwrap_or(0);
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            match self
                .run_binance_session(
                    source.as_ref(),
                    &instrument,
                    &symbol,
                    gap_generation,
                    &shutdown,
                )
                .await
            {
                Ok(()) => return,
                Err(error) => {
                    tracing::warn!(%instrument, %error, "Binance aggTrade session failed");
                    gap_generation = self
                        .projections
                        .mark_crypto_source_gap(
                            &DomainSourceId::binance_agg_trade(),
                            &instrument,
                            Utc::now(),
                        )
                        .await
                        .unwrap_or_else(|mark_error| {
                            tracing::error!(%instrument, %mark_error, "failed to persist Binance source gap");
                            gap_generation.saturating_add(1)
                        });
                    tokio::select! {
                        () = shutdown.cancelled() => return,
                        () = tokio::time::sleep(RECONNECT_BACKOFF) => {}
                    }
                }
            }
        }
    }

    async fn run_binance_session(
        &self,
        source: &BinanceAggTradeSource,
        instrument: &DomainInstrumentKey,
        symbol: &BinanceSymbol,
        mut gap_generation: u64,
        shutdown: &CancellationToken,
    ) -> QuantResult<()> {
        let cursor = self
            .cursors
            .find(&DomainSourceId::binance_agg_trade(), instrument)
            .await?;
        let mut expected = match cursor.map(|cursor| cursor.checkpoint_json) {
            Some(DomainSourceCheckpoint::BinanceAggTrade {
                aggregate_trade_id, ..
            }) => Some(next_sequence(aggregate_trade_id)?),
            Some(_) => {
                return Err(QuantError::config(
                    "Binance aggTrade cursor contains a different source checkpoint type",
                ));
            }
            None => None,
        };
        if let Some(from_id) = expected {
            expected = Some(
                self.recover_binance(source, symbol, from_id, None, gap_generation)
                    .await?,
            );
        }
        let mut stream = source.stream(symbol).await?;
        loop {
            if stream.rotation_due() {
                return Err(QuantError::config("scheduled Binance WebSocket rotation"));
            }
            let report = tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                result = stream.next_report() => result?,
                () = tokio::time::sleep(StdDuration::from_secs(1)) => continue,
            };
            if let Some(next_id) = expected {
                if report.source_sequence < next_id {
                    continue;
                }
                if report.source_sequence > next_id {
                    gap_generation = self
                        .projections
                        .mark_crypto_source_gap(
                            &DomainSourceId::binance_agg_trade(),
                            instrument,
                            Utc::now(),
                        )
                        .await?;
                    expected = Some(
                        self.recover_binance(
                            source,
                            symbol,
                            next_id,
                            Some(report.source_sequence),
                            gap_generation,
                        )
                        .await?,
                    );
                }
            }
            if expected.is_none_or(|next_id| report.source_sequence >= next_id) {
                self.persist_crypto(report.clone(), gap_generation).await?;
                expected = Some(next_sequence(report.source_sequence)?);
            }
        }
    }

    async fn recover_binance(
        &self,
        source: &BinanceAggTradeSource,
        symbol: &BinanceSymbol,
        mut from_id: u64,
        stop_before: Option<u64>,
        gap_generation: u64,
    ) -> QuantResult<u64> {
        loop {
            let reports = source
                .recover_from(symbol, from_id, BINANCE_PAGE_SIZE, Utc::now())
                .await?;
            if reports.is_empty() {
                return Ok(from_id);
            }
            let page_len = reports.len();
            for report in reports {
                if stop_before.is_some_and(|limit| report.source_sequence >= limit) {
                    return Ok(from_id);
                }
                if report.source_sequence != from_id {
                    return Err(QuantError::config(format!(
                        "Binance aggTrade recovery gap: expected {from_id}, got {}",
                        report.source_sequence
                    )));
                }
                self.persist_crypto(report, gap_generation).await?;
                from_id = next_sequence(from_id)?;
            }
            if page_len < usize::from(BINANCE_PAGE_SIZE) {
                return Ok(from_id);
            }
        }
    }

    async fn run_chainlink_supervisor(self: Arc<Self>, shutdown: CancellationToken) {
        let mut tasks = BTreeMap::<DomainInstrumentKey, SourceTask>::new();
        let mut interval = tokio::time::interval(DISCOVERY_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                _ = interval.tick() => {}
            }
            let desired = match self.discover().await {
                Ok(bindings) => bindings.chainlink,
                Err(error) => {
                    tracing::warn!(%error, "Chainlink live binding discovery failed");
                    continue;
                }
            };
            stop_removed(&mut tasks, desired.keys().collect()).await;
            let Some(source) = self.chainlink.as_ref() else {
                for instrument in desired.keys() {
                    self.mark_crypto_unavailable(instrument).await;
                }
                continue;
            };
            for (instrument, feed) in desired {
                if tasks.contains_key(&instrument) {
                    continue;
                }
                if !source.instruments().contains(&instrument) {
                    self.mark_crypto_unavailable(&instrument).await;
                    tracing::error!(%instrument, "active Chainlink feed is not configured");
                    continue;
                }
                let cancel = shutdown.child_token();
                let child_cancel = cancel.clone();
                let worker = Arc::clone(&self);
                let source = Arc::clone(source);
                let task_instrument = instrument.clone();
                let handle = tokio::spawn(async move {
                    worker
                        .run_chainlink_feed(source, task_instrument, feed, child_cancel)
                        .await;
                });
                tasks.insert(instrument, SourceTask { cancel, handle });
            }
        }
        stop_all(tasks).await;
    }

    async fn run_chainlink_feed(
        &self,
        source: Arc<ChainlinkDataStreamsSource>,
        instrument: DomainInstrumentKey,
        feed: ChainlinkFeedKey,
        shutdown: CancellationToken,
    ) {
        let mut gap_generation = self
            .projections
            .mark_crypto_source_gap(
                &DomainSourceId::chainlink_data_streams(),
                &instrument,
                Utc::now(),
            )
            .await
            .unwrap_or(0);
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            match self
                .run_chainlink_session(
                    source.as_ref(),
                    &instrument,
                    &feed,
                    gap_generation,
                    &shutdown,
                )
                .await
            {
                Ok(()) => return,
                Err(error) => {
                    tracing::warn!(%instrument, %error, "Chainlink Data Streams session failed");
                    gap_generation = self
                        .projections
                        .mark_crypto_source_gap(
                            &DomainSourceId::chainlink_data_streams(),
                            &instrument,
                            Utc::now(),
                        )
                        .await
                        .unwrap_or_else(|_| gap_generation.saturating_add(1));
                    tokio::select! {
                        () = shutdown.cancelled() => return,
                        () = tokio::time::sleep(RECONNECT_BACKOFF) => {}
                    }
                }
            }
        }
    }

    async fn run_chainlink_session(
        &self,
        source: &ChainlinkDataStreamsSource,
        instrument: &DomainInstrumentKey,
        feed: &ChainlinkFeedKey,
        gap_generation: u64,
        shutdown: &CancellationToken,
    ) -> QuantResult<()> {
        source.validate_system_clock().await?;
        let cursor = self
            .cursors
            .find(&DomainSourceId::chainlink_data_streams(), instrument)
            .await?;
        let mut last = match cursor.map(|cursor| cursor.checkpoint_json) {
            Some(DomainSourceCheckpoint::ChainlinkDataStreams {
                observations_timestamp,
                report_hash,
            }) => Some((observations_timestamp, report_hash)),
            Some(_) => {
                return Err(QuantError::config(
                    "Chainlink cursor contains a different source checkpoint type",
                ));
            }
            None => None,
        };
        if let Some((timestamp, _)) = &last {
            let start = u128::try_from(timestamp.timestamp()).map_err(|error| {
                QuantError::config(format!("negative Chainlink checkpoint timestamp: {error}"))
            })?;
            let reports = source.reports_page(feed, start, Utc::now()).await?;
            for report in reports {
                if should_process_crypto(&report, last.as_ref()) {
                    self.persist_crypto(report.clone(), gap_generation).await?;
                    last = Some((report.event_time, report.report_hash.clone()));
                }
            }
        }
        let mut stream = source.stream(slice::from_ref(feed)).await?;
        stream.listen().await.map_err(|error| {
            QuantError::config(format!("Chainlink stream listen failed: {error}"))
        })?;
        let mut clock_check = tokio::time::interval(StdDuration::from_secs(30));
        clock_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
        clock_check.tick().await;
        loop {
            let report = tokio::select! {
                () = shutdown.cancelled() => {
                    stream.close().await.map_err(|error| {
                        QuantError::config(format!("Chainlink stream close failed: {error}"))
                    })?;
                    return Ok(());
                }
                _ = clock_check.tick() => {
                    source.validate_system_clock().await?;
                    continue;
                }
                report = source.next_report(&mut stream, Utc::now()) => report?,
            };
            if should_process_crypto(&report, last.as_ref()) {
                self.persist_crypto(report.clone(), gap_generation).await?;
                last = Some((report.event_time, report.report_hash.clone()));
            }
        }
    }

    async fn run_weather_loop(self: Arc<Self>, shutdown: CancellationToken) {
        let mut gap_generations = BTreeMap::<(String, NaiveDate), u64>::new();
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            let bindings = match self.discover().await {
                Ok(bindings) => bindings.weather,
                Err(error) => {
                    tracing::warn!(%error, "Weather live binding discovery failed");
                    tokio::time::sleep(StdDuration::from_secs(1)).await;
                    continue;
                }
            };
            gap_generations.retain(|key, _| bindings.contains_key(key));
            let Some(source) = self.aviation.as_ref() else {
                for target in bindings.values() {
                    self.mark_weather_unavailable(target).await;
                }
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(StdDuration::from_secs(self.aviation_config.poll_secs.max(1))) => {}
                }
                continue;
            };
            let stations = weather_stations(&bindings);
            for (station, targets) in stations {
                if shutdown.is_cancelled() {
                    return;
                }
                let key_generations = targets
                    .iter()
                    .map(|target| {
                        let key = (target.station.to_string(), target.local_date);
                        let generation = gap_generations.entry(key).or_insert(0);
                        (target.local_date, *generation)
                    })
                    .collect::<BTreeMap<_, _>>();
                if let Err(error) = self
                    .poll_weather_station(source.as_ref(), &station, &targets, &key_generations)
                    .await
                {
                    tracing::warn!(%station, %error, "AviationWeather station poll failed");
                    for target in &targets {
                        let generation = self
                            .projections
                            .mark_weather_source_gap(&target.station, target.local_date, Utc::now())
                            .await
                            .unwrap_or_else(|_| {
                                gap_generations
                                    .get(&(target.station.to_string(), target.local_date))
                                    .copied()
                                    .unwrap_or(0)
                                    .saturating_add(1)
                            });
                        gap_generations
                            .insert((target.station.to_string(), target.local_date), generation);
                    }
                }
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    () = tokio::time::sleep(AVIATION_REQUEST_SPACING) => {}
                }
            }
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(StdDuration::from_secs(self.aviation_config.poll_secs.max(1))) => {}
            }
        }
    }

    async fn poll_weather_station(
        &self,
        source: &AviationWeatherSource,
        station: &IcaoStation,
        targets: &[WeatherTarget],
        gap_generations: &BTreeMap<NaiveDate, u64>,
    ) -> QuantResult<()> {
        let instrument = DomainInstrumentKey::aviation_weather(station);
        let cursor = self
            .cursors
            .find(&DomainSourceId::aviation_weather(), &instrument)
            .await?;
        let hours = cursor.as_ref().map_or(360, |cursor| {
            (Utc::now() - cursor.checkpoint_json.event_time())
                .num_hours()
                .saturating_add(2)
                .clamp(1, 360)
        });
        let hours = u16::try_from(hours)
            .map_err(|error| QuantError::config(format!("Weather history window: {error}")))?;
        let reports = source.observations(station, hours, Utc::now()).await?;
        let indices = weather_resume_indices(&reports, cursor.as_ref())?;
        for index in indices {
            let report = &reports[index];
            let target = targets.iter().find(|target| {
                report
                    .observation_time
                    .with_timezone(&target.timezone)
                    .date_naive()
                    == target.local_date
            });
            let Some(target) = target else {
                continue;
            };
            let (revision, supersedes) = weather_revision(&reports, index)?;
            self.weather_writer
                .write_batch(vec![report.to_clickhouse_row(
                    target.local_date,
                    revision,
                    supersedes,
                )])
                .await?;
            let checkpoint = DomainSourceCheckpoint::AviationWeather {
                observation_time: report.observation_time,
                report_hash: report.report_hash.clone(),
            };
            self.projections
                .apply_weather_report(
                    report.clone(),
                    target.timezone.to_string(),
                    target.local_date,
                    checkpoint,
                    gap_generations
                        .get(&target.local_date)
                        .copied()
                        .unwrap_or(0),
                    true,
                )
                .await?;
        }
        for target in targets {
            if weather_day_close_due(
                target,
                Utc::now(),
                self.aviation_config.day_close_grace_secs,
            )? {
                self.projections
                    .close_weather_day(&target.station, target.local_date, Utc::now())
                    .await?;
            }
        }
        Ok(())
    }

    async fn run_ghcnh_loop(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            let bindings = match self.discover().await {
                Ok(bindings) => bindings.weather,
                Err(error) => {
                    tracing::warn!(%error, "GHCNh binding discovery failed");
                    tokio::time::sleep(StdDuration::from_secs(1)).await;
                    continue;
                }
            };
            if let Some(source) = self.ghcnh.as_ref() {
                for (station, targets) in weather_stations(&bindings) {
                    let Some(target) = targets.first() else {
                        continue;
                    };
                    if let Err(error) = self.ingest_ghcnh_station(source, &station, target).await {
                        tracing::warn!(%station, %error, "GHCNh station calibration ingest failed");
                    }
                }
            }
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(StdDuration::from_secs(self.ghcnh_config.refresh_secs.max(1))) => {}
            }
        }
    }

    async fn ingest_ghcnh_station(
        &self,
        source: &GhcnhSource,
        station: &IcaoStation,
        target: &WeatherTarget,
    ) -> QuantResult<()> {
        let available_at = Utc::now();
        let years = i32::from(self.ghcnh_config.calibration_years);
        let first_year = available_at
            .year()
            .checked_sub(years.saturating_sub(1))
            .ok_or_else(|| QuantError::config("GHCNh calibration year underflow"))?;
        let mut year_files = Vec::new();
        for year in first_year..=available_at.year() {
            year_files.push(
                source
                    .yearly_station(station, &target.ghcnh_station_id, year, available_at)
                    .await?,
            );
        }
        let file_hash = CanonicalDigest::content_hash_json(
            &year_files
                .iter()
                .map(|year| year.file_hash.clone())
                .collect::<Vec<_>>(),
        )?;
        let instrument = DomainInstrumentKey::ghcnh(station);
        let existing = self
            .cursors
            .find(&DomainSourceId::ghcnh(), &instrument)
            .await?;
        let unchanged = existing.as_ref().is_some_and(|cursor| {
            matches!(
                &cursor.checkpoint_json,
                DomainSourceCheckpoint::Ghcnh {
                    file_hash: existing_hash,
                    ..
                } if existing_hash == &file_hash
            )
        });
        let last_hour = if unchanged {
            existing
                .as_ref()
                .map(|cursor| cursor.checkpoint_json.event_time())
        } else {
            let mut last_hour: Option<DateTime<Utc>> = None;
            let mut revisions = BTreeMap::<DateTime<Utc>, u32>::new();
            let mut rows = Vec::new();
            for year in year_files {
                for report in year.reports {
                    last_hour = Some(last_hour.map_or(report.observation_time, |current| {
                        current.max(report.observation_time)
                    }));
                    let revision = revisions.entry(report.observation_time).or_insert(0);
                    let local_date = report
                        .observation_time
                        .with_timezone(&target.timezone)
                        .date_naive();
                    rows.push(report.to_clickhouse_row(local_date, *revision, None));
                    *revision = revision.checked_add(1).ok_or_else(|| {
                        QuantError::config("GHCNh same-hour revision count overflow")
                    })?;
                }
            }
            rows.sort_by(|left, right| {
                (
                    left.observation_time,
                    left.revision,
                    left.report_hash.as_str(),
                )
                    .cmp(&(
                        right.observation_time,
                        right.revision,
                        right.report_hash.as_str(),
                    ))
            });
            for batch in rows.chunks(5_000) {
                self.weather_writer.write_batch(batch.to_vec()).await?;
            }
            last_hour
        };
        let last_hour = last_hour.ok_or_else(|| {
            QuantError::config(format!(
                "GHCNh returned no accepted temperatures for {station}"
            ))
        })?;
        let checkpoint = DomainSourceCheckpoint::Ghcnh {
            last_hour,
            file_hash,
        };
        self.upsert_source_cursor(DomainSourceId::ghcnh(), instrument, checkpoint)
            .await
    }

    async fn run_gefs_loop(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            let bindings = match self.discover().await {
                Ok(bindings) => bindings.weather,
                Err(error) => {
                    tracing::warn!(%error, "GEFS binding discovery failed");
                    tokio::time::sleep(StdDuration::from_secs(1)).await;
                    continue;
                }
            };
            if let Some(source) = self.gefs.as_ref()
                && !bindings.is_empty()
                && let Err(error) = self.ingest_gefs_cycle(source, &bindings, Utc::now()).await
            {
                tracing::warn!(%error, "GEFS ensemble ingest failed");
            }
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(StdDuration::from_secs(self.gefs_config.poll_secs.max(1))) => {}
            }
        }
    }

    async fn ingest_gefs_cycle(
        &self,
        source: &Arc<GefsSource>,
        bindings: &BTreeMap<(String, NaiveDate), WeatherTarget>,
        now: DateTime<Utc>,
    ) -> QuantResult<()> {
        let reference_time = latest_gefs_cycle(now, self.gefs_config.publication_lag_secs)?;
        let request_projection = bindings
            .values()
            .map(|target| {
                (
                    target.station.clone(),
                    target.local_date,
                    target.station_profile_hash.clone(),
                )
            })
            .collect::<Vec<_>>();
        let request_hash = CanonicalDigest::content_hash_json(&(
            "gefs_tmax_request_v1",
            reference_time,
            self.gefs_config.max_lead_hours,
            &request_projection,
        ))?;
        if self
            .gefs_request_already_complete(bindings, reference_time, &request_hash)
            .await?
        {
            return Ok(());
        }

        let mut decoded = self
            .decode_gefs_members(source, bindings, reference_time)
            .await?;
        decoded.sort_by_key(|member| (member.lead_hours, member.member));
        let run_manifest_hash = CanonicalDigest::content_hash_json(&(
            &request_hash,
            decoded
                .iter()
                .map(|member| {
                    (
                        member.lead_hours,
                        member.member,
                        member.segment_hash.clone(),
                    )
                })
                .collect::<Vec<_>>(),
        ))?;
        self.write_gefs_rows(decoded, &run_manifest_hash).await?;
        for station in weather_stations(bindings).keys() {
            let checkpoint = DomainSourceCheckpoint::Gefs {
                reference_time,
                request_hash: request_hash.clone(),
                manifest_hash: run_manifest_hash.clone(),
            };
            self.upsert_source_cursor(
                DomainSourceId::gefs(),
                DomainInstrumentKey::gefs(station),
                checkpoint,
            )
            .await?;
        }
        Ok(())
    }

    async fn decode_gefs_members(
        &self,
        source: &Arc<GefsSource>,
        bindings: &BTreeMap<(String, NaiveDate), WeatherTarget>,
        reference_time: DateTime<Utc>,
    ) -> QuantResult<Vec<GefsDecodedMember>> {
        let mut decoded = Vec::new();
        for lead_hours in (3..=self.gefs_config.max_lead_hours).step_by(3) {
            let valid_time = reference_time + Duration::hours(i64::from(lead_hours));
            let stations = gefs_stations_for_valid_time(bindings, valid_time);
            if stations.is_empty() {
                continue;
            }
            let members = stream::iter(0_u8..=30)
                .map(|member| {
                    let source = Arc::clone(source);
                    let stations = stations.clone();
                    async move {
                        source
                            .tmax_member(reference_time, lead_hours, member, &stations)
                            .await
                    }
                })
                .buffer_unordered(self.gefs_config.max_concurrency)
                .try_collect::<Vec<_>>()
                .await?;
            if members.len() != 31 {
                return Err(QuantError::config(format!(
                    "GEFS lead {lead_hours} decoded {} of 31 members",
                    members.len()
                )));
            }
            decoded.extend(members);
        }
        if decoded.is_empty() {
            return Err(QuantError::config(
                "GEFS cycle has no valid times for active Weather subjects",
            ));
        }
        Ok(decoded)
    }

    async fn write_gefs_rows(
        &self,
        decoded: Vec<GefsDecodedMember>,
        run_manifest_hash: &ContentHash,
    ) -> QuantResult<()> {
        let mut rows = decoded
            .into_iter()
            .flat_map(|member| {
                let manifest = run_manifest_hash.clone();
                member
                    .points
                    .into_iter()
                    .map(move |point| WeatherForecastPointRow {
                        source_id: DomainSourceId::gefs(),
                        station: point.station.to_string(),
                        reference_time: member.reference_time.timestamp_millis(),
                        valid_time: member.valid_time.timestamp_millis(),
                        available_at: member.available_at.timestamp_millis(),
                        lead_hours: member.lead_hours,
                        member: member.member,
                        tmax_celsius: ChDecimal64::from(point.tmax_celsius.value()),
                        grid_binding_hash: point.grid_binding_hash,
                        run_manifest_hash: manifest.clone(),
                        schema_version: ChSchemaVersion::FIRST,
                    })
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            (&left.station, left.valid_time, left.member).cmp(&(
                &right.station,
                right.valid_time,
                right.member,
            ))
        });
        for batch in rows.chunks(5_000) {
            self.forecast_writer.write_batch(batch.to_vec()).await?;
        }
        Ok(())
    }

    async fn gefs_request_already_complete(
        &self,
        bindings: &BTreeMap<(String, NaiveDate), WeatherTarget>,
        reference_time: DateTime<Utc>,
        request_hash: &ContentHash,
    ) -> QuantResult<bool> {
        for station in weather_stations(bindings).keys() {
            let cursor = self
                .cursors
                .find(&DomainSourceId::gefs(), &DomainInstrumentKey::gefs(station))
                .await?;
            let complete = cursor.is_some_and(|cursor| {
                matches!(
                    cursor.checkpoint_json,
                    DomainSourceCheckpoint::Gefs {
                        reference_time: stored_reference,
                        request_hash: stored_request,
                        ..
                    } if stored_reference == reference_time && stored_request == *request_hash
                )
            });
            if !complete {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn upsert_source_cursor(
        &self,
        source_id: DomainSourceId,
        instrument_key: DomainInstrumentKey,
        checkpoint: DomainSourceCheckpoint,
    ) -> QuantResult<()> {
        let checkpoint_hash = CanonicalDigest::content_hash_json(&checkpoint)?;
        self.cursors
            .upsert(UpsertDomainSourceCursor {
                source_id,
                instrument_key,
                checkpoint_json: checkpoint,
                checkpoint_hash,
                status: DomainCursorStatus::Live.as_str().to_owned(),
                last_error: None,
                updated_at: Utc::now(),
            })
            .await?;
        Ok(())
    }

    async fn persist_crypto(
        &self,
        report: CryptoPriceReport,
        gap_generation: u64,
    ) -> QuantResult<()> {
        self.crypto_writer
            .write_batch(vec![report.to_clickhouse_row()])
            .await?;
        let checkpoint = if report.source_id == DomainSourceId::binance_agg_trade() {
            DomainSourceCheckpoint::BinanceAggTrade {
                aggregate_trade_id: report.source_sequence,
                event_time: report.event_time,
            }
        } else if report.source_id == DomainSourceId::chainlink_data_streams() {
            DomainSourceCheckpoint::ChainlinkDataStreams {
                observations_timestamp: report.observations_timestamp.ok_or_else(|| {
                    QuantError::config("Chainlink report lacks observations timestamp")
                })?,
                report_hash: report.report_hash.clone(),
            }
        } else {
            return Err(QuantError::config(format!(
                "unsupported crypto report source {}",
                report.source_id
            )));
        };
        self.projections
            .apply_crypto_report(report, checkpoint, gap_generation, true)
            .await?;
        Ok(())
    }

    async fn mark_crypto_unavailable(&self, instrument: &DomainInstrumentKey) {
        let Some(source_id) = instrument.source_id() else {
            return;
        };
        if let Err(error) = self
            .projections
            .mark_crypto_source_gap(&source_id, instrument, Utc::now())
            .await
        {
            tracing::error!(%instrument, %error, "failed to mark crypto source unavailable");
        }
    }

    async fn mark_weather_unavailable(&self, target: &WeatherTarget) {
        if let Err(error) = self
            .projections
            .mark_weather_source_gap(&target.station, target.local_date, Utc::now())
            .await
        {
            tracing::error!(station = %target.station, %error, "failed to mark Weather source unavailable");
        }
    }
}

fn should_process_crypto(
    report: &CryptoPriceReport,
    last: Option<&(DateTime<Utc>, ContentHash)>,
) -> bool {
    last.is_none_or(|(time, hash)| {
        report.event_time > *time || (report.event_time == *time && report.report_hash != *hash)
    })
}

fn next_sequence(value: u64) -> QuantResult<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| QuantError::config("source sequence overflow"))
}

fn weather_stations(
    bindings: &BTreeMap<(String, NaiveDate), WeatherTarget>,
) -> BTreeMap<IcaoStation, Vec<WeatherTarget>> {
    let mut stations = BTreeMap::<IcaoStation, Vec<WeatherTarget>>::new();
    for target in bindings.values() {
        stations
            .entry(target.station.clone())
            .or_default()
            .push(target.clone());
    }
    stations
}

fn latest_gefs_cycle(now: DateTime<Utc>, publication_lag_secs: u64) -> QuantResult<DateTime<Utc>> {
    let lag = i64::try_from(publication_lag_secs)
        .map_err(|error| QuantError::config(format!("GEFS publication lag overflow: {error}")))?;
    let eligible = now
        .checked_sub_signed(Duration::seconds(lag))
        .ok_or_else(|| QuantError::config("GEFS eligible-cycle time underflow"))?;
    let cycle_timestamp = eligible.timestamp().div_euclid(21_600) * 21_600;
    Utc.timestamp_opt(cycle_timestamp, 0)
        .single()
        .ok_or_else(|| QuantError::config("GEFS cycle timestamp is outside chrono range"))
}

fn gefs_stations_for_valid_time(
    bindings: &BTreeMap<(String, NaiveDate), WeatherTarget>,
    valid_time: DateTime<Utc>,
) -> Vec<GefsStationBinding> {
    let mut stations = BTreeMap::<IcaoStation, GefsStationBinding>::new();
    for target in bindings.values() {
        if valid_time.with_timezone(&target.timezone).date_naive() == target.local_date {
            stations
                .entry(target.station.clone())
                .or_insert_with(|| GefsStationBinding {
                    station: target.station.clone(),
                    latitude: target.latitude,
                    longitude: target.longitude,
                });
        }
    }
    stations.into_values().collect()
}

fn weather_resume_indices(
    reports: &[WeatherObservationReport],
    cursor: Option<&DomainSourceCursorInfo>,
) -> QuantResult<Vec<usize>> {
    let Some(cursor) = cursor else {
        return Ok((0..reports.len()).collect());
    };
    let DomainSourceCheckpoint::AviationWeather {
        observation_time,
        report_hash,
    } = &cursor.checkpoint_json
    else {
        return Err(QuantError::config(
            "AviationWeather cursor contains a different checkpoint type",
        ));
    };
    let same_time_position = reports.iter().position(|report| {
        report.observation_time == *observation_time && report.report_hash == *report_hash
    });
    let last_same_time_position = reports
        .iter()
        .rposition(|report| report.observation_time == *observation_time);
    Ok(reports
        .iter()
        .enumerate()
        .filter(|(index, report)| {
            report.observation_time > *observation_time
                || (report.observation_time == *observation_time
                    && same_time_position.map_or_else(
                        || {
                            last_same_time_position == Some(*index)
                                && report.report_hash != *report_hash
                        },
                        |position| *index > position,
                    ))
        })
        .map(|(index, _)| index)
        .collect())
}

fn weather_revision(
    reports: &[WeatherObservationReport],
    index: usize,
) -> QuantResult<(u32, Option<ContentHash>)> {
    let report = reports
        .get(index)
        .ok_or_else(|| QuantError::config("Weather report revision index is out of bounds"))?;
    let prior = reports[..index]
        .iter()
        .filter(|candidate| candidate.observation_time == report.observation_time)
        .collect::<Vec<_>>();
    let revision = u32::try_from(prior.len())
        .map_err(|error| QuantError::config(format!("Weather revision overflow: {error}")))?;
    Ok((
        revision,
        prior.last().map(|previous| previous.report_hash.clone()),
    ))
}

fn weather_day_close_due(
    target: &WeatherTarget,
    now: DateTime<Utc>,
    grace_secs: u64,
) -> QuantResult<bool> {
    let next_date = target
        .local_date
        .succ_opt()
        .ok_or_else(|| QuantError::config("Weather local date overflow"))?;
    let midnight = target
        .timezone
        .from_local_datetime(
            &next_date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| QuantError::config("invalid Weather local midnight"))?,
        )
        .single()
        .ok_or_else(|| QuantError::config("Weather local midnight is ambiguous or missing"))?;
    let grace = i64::try_from(grace_secs)
        .map_err(|error| QuantError::config(format!("Weather close grace overflow: {error}")))?;
    Ok(now >= (midnight + Duration::seconds(grace)).with_timezone(&Utc))
}

async fn stop_removed(
    tasks: &mut BTreeMap<DomainInstrumentKey, SourceTask>,
    desired: BTreeSet<&DomainInstrumentKey>,
) {
    let removed = tasks
        .keys()
        .filter(|key| !desired.contains(key))
        .cloned()
        .collect::<Vec<_>>();
    for key in removed {
        if let Some(task) = tasks.remove(&key) {
            task.cancel.cancel();
            if let Err(error) = task.handle.await {
                tracing::warn!(%key, %error, "domain source task join failed");
            }
        }
    }
}

async fn stop_all(tasks: BTreeMap<DomainInstrumentKey, SourceTask>) {
    for (_, task) in tasks {
        task.cancel.cancel();
        if let Err(error) = task.handle.await {
            tracing::warn!(%error, "domain source task join failed during shutdown");
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        domain::{
            DomainSourceCheckpoint, DomainSourceCursorInfo, WeatherObservationReport,
            WeatherObservationReportKind,
        },
        types::{
            ContentHash, DomainInstrumentKey, DomainSourceId, IcaoStation, TemperatureCelsius,
        },
    };
    use rust_decimal_macros::dec;

    use super::{weather_resume_indices, weather_revision};

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("test hash")
    }

    fn report(
        hash_seed: char,
        published_minute: u32,
        temperature: rust_decimal::Decimal,
    ) -> WeatherObservationReport {
        WeatherObservationReport {
            source_id: DomainSourceId::aviation_weather(),
            station: IcaoStation::parse("KLGA").expect("station"),
            report_kind: if published_minute == 2 {
                WeatherObservationReportKind::Correction
            } else {
                WeatherObservationReportKind::Metar
            },
            temperature: TemperatureCelsius::new(temperature),
            precision_celsius: dec!(0.1),
            observation_time: Utc.with_ymd_and_hms(2026, 7, 13, 16, 0, 0).unwrap(),
            published_at: Utc
                .with_ymd_and_hms(2026, 7, 13, 16, published_minute, 0)
                .unwrap(),
            available_at: Utc.with_ymd_and_hms(2026, 7, 13, 16, 3, 0).unwrap(),
            report_hash: hash(hash_seed),
            raw_report: hash_seed.to_string(),
        }
    }

    #[test]
    fn same_time_correction_resumes_after_exact_report_hash() {
        let reports = vec![report('a', 1, dec!(27)), report('b', 2, dec!(26))];
        let cursor = DomainSourceCursorInfo {
            source_id: DomainSourceId::aviation_weather(),
            instrument_key: DomainInstrumentKey::aviation_weather(&reports[0].station),
            checkpoint_json: DomainSourceCheckpoint::AviationWeather {
                observation_time: reports[0].observation_time,
                report_hash: reports[0].report_hash.clone(),
            },
            checkpoint_hash: hash('c'),
            status: "live".to_owned(),
            last_error: None,
            created_at: reports[0].available_at,
            updated_at: reports[0].available_at,
        };
        assert_eq!(
            weather_resume_indices(&reports, Some(&cursor)).expect("resume"),
            vec![1]
        );
        assert_eq!(
            weather_revision(&reports, 1).expect("revision"),
            (1, Some(reports[0].report_hash.clone()))
        );
    }
}
