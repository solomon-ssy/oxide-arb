//! Weather observation, forecast, backfill and calibration ingestion.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration as StdDuration,
};

use chrono::{DateTime, Datelike, Days, Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use futures_util::{StreamExt, TryStreamExt, stream};
use quant_pivot_api::weather::{
    AviationWeatherSource, GefsDecodedMember, GefsSource, GefsStationBinding, GhcnhSource,
};
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{WeatherForecastFactRow, WeatherObservationFactRow},
    config::{
        AviationWeatherSourceConfig, GefsSourceConfig, GhcnhSourceConfig,
        WeatherStationProfileConfig,
    },
    domain::{
        DomainCursorStatus, DomainSourceCheckpoint, DomainSourceCursorInfo, LinkageOutcome,
        MarketLinkage, MarketSubject, NewCalibrationArtifact, UpsertDomainSourceCursor,
        WeatherForecastPoint, WeatherObservationFact, WeatherObservationReport,
    },
    enums::{
        domain::{DomainFamily, LinkageSourceRole},
        quant::CalibrationKind,
    },
    hashing::CanonicalDigest,
    types::{
        CalibrationArtifactId, ContentHash, DomainInstrumentKey, DomainMeasurementUnit,
        DomainSourceId, IcaoStation, WeatherVariable,
    },
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, DomainProjectionRepository, DomainSourceCursorRepository,
    FactWriter, MarketLinkageRepository, QuantFactReadRepository,
};
use quant_pivot_research::{
    features::domain::weather::{WeatherStationLeadBiasFit, fit_weather_station_lead_bias},
    linkage::weather_station_profile_hash,
};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::{
    app::domain_source_supervisor::DomainSourceSupervisor,
    runtime_config::RuntimeConfigStore,
    service::weather_fact_ingest::{WeatherFactIngestService, WeatherObservationCandidate},
};

const AVIATION_REQUEST_SPACING: StdDuration = StdDuration::from_millis(650);
const WEATHER_BOOTSTRAP_DAYS: u64 = 14;

#[derive(Clone)]
struct WeatherTarget {
    station: IcaoStation,
    timezone: Tz,
    local_date: NaiveDate,
    latitude: rust_decimal::Decimal,
    longitude: rust_decimal::Decimal,
    ghcnh_station_id: Option<String>,
    station_profile_hash: ContentHash,
}

#[derive(Default)]
struct WeatherBindings {
    weather: BTreeMap<(String, NaiveDate), WeatherTarget>,
}

pub struct WeatherIngestWorker {
    source_supervisor: Arc<DomainSourceSupervisor>,
    linkages: Arc<dyn MarketLinkageRepository>,
    cursors: Arc<dyn DomainSourceCursorRepository>,
    projections: Arc<dyn DomainProjectionRepository>,
    facts: WeatherFactIngestService,
    fact_read: Arc<dyn QuantFactReadRepository>,
    calibrations: Arc<dyn CalibrationArtifactRepository>,
    runtime_config: Arc<RuntimeConfigStore>,
    aviation: Option<Arc<AviationWeatherSource>>,
    ghcnh: Option<Arc<GhcnhSource>>,
    gefs: Option<Arc<GefsSource>>,
    aviation_config: AviationWeatherSourceConfig,
    ghcnh_config: GhcnhSourceConfig,
    gefs_config: GefsSourceConfig,
    station_profiles: BTreeMap<String, WeatherStationProfileConfig>,
}

pub struct WeatherIngestDeps {
    pub source_supervisor: Arc<DomainSourceSupervisor>,
    pub linkages: Arc<dyn MarketLinkageRepository>,
    pub cursors: Arc<dyn DomainSourceCursorRepository>,
    pub projections: Arc<dyn DomainProjectionRepository>,
    pub weather_writer: Arc<dyn FactWriter<WeatherObservationFactRow>>,
    pub forecast_writer: Arc<dyn FactWriter<WeatherForecastFactRow>>,
    pub fact_read: Arc<dyn QuantFactReadRepository>,
    pub calibrations: Arc<dyn CalibrationArtifactRepository>,
    pub runtime_config: Arc<RuntimeConfigStore>,
    pub aviation: Option<Arc<AviationWeatherSource>>,
    pub ghcnh: Option<Arc<GhcnhSource>>,
    pub gefs: Option<Arc<GefsSource>>,
    pub aviation_config: AviationWeatherSourceConfig,
    pub ghcnh_config: GhcnhSourceConfig,
    pub gefs_config: GefsSourceConfig,
    pub station_profiles: BTreeMap<String, WeatherStationProfileConfig>,
}

impl WeatherIngestWorker {
    #[must_use]
    pub fn new(deps: WeatherIngestDeps) -> Self {
        let facts = WeatherFactIngestService::new(
            Arc::clone(&deps.weather_writer),
            Arc::clone(&deps.forecast_writer),
            Arc::clone(&deps.fact_read),
        );
        Self {
            source_supervisor: deps.source_supervisor,
            linkages: deps.linkages,
            cursors: deps.cursors,
            projections: deps.projections,
            facts,
            fact_read: deps.fact_read,
            calibrations: deps.calibrations,
            runtime_config: deps.runtime_config,
            aviation: deps.aviation,
            ghcnh: deps.ghcnh,
            gefs: deps.gefs,
            aviation_config: deps.aviation_config,
            ghcnh_config: deps.ghcnh_config,
            gefs_config: deps.gefs_config,
            station_profiles: deps.station_profiles,
        }
    }

    /// Run one finite observation/archive/forecast pass for evidence bootstrap.
    ///
    /// An empty filter means every frozen station profile. A non-empty filter
    /// is validated against the discovered bindings and is intended for a
    /// governed, reviewable bootstrap shard. Every selected station is
    /// attempted before a combined failure is returned.
    pub async fn run_evidence_once(&self, station_filter: &BTreeSet<String>) -> QuantResult<()> {
        self.source_supervisor.ensure_boot_reconciled().await?;
        let mut bindings = self.discover().await?.weather;
        if !station_filter.is_empty() {
            bindings.retain(|(station, _), _| station_filter.contains(station));
        }
        if bindings.is_empty() {
            return Err(QuantError::config(
                "Weather evidence station filter matched no frozen binding",
            ));
        }
        let selected_stations = weather_stations(&bindings);
        let discovered = selected_stations
            .keys()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        let missing = station_filter
            .difference(&discovered)
            .cloned()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(QuantError::config(format!(
                "Weather evidence station filter contains unknown bindings: {}",
                missing.join(",")
            )));
        }

        let mut failures = Vec::new();
        match self.aviation.as_ref() {
            Some(source) => {
                for (index, (station, targets)) in selected_stations.iter().enumerate() {
                    let gap_generations = targets
                        .iter()
                        .map(|target| (target.local_date, 0_u64))
                        .collect::<BTreeMap<_, _>>();
                    if let Err(error) = self
                        .poll_weather_station(source, station, targets, &gap_generations)
                        .await
                    {
                        for target in targets {
                            self.mark_weather_unavailable(target).await;
                        }
                        self.record_evidence_failure(
                            &DomainSourceId::aviation_weather(),
                            &DomainInstrumentKey::aviation_weather(station),
                            &error,
                            &mut failures,
                        )
                        .await;
                    }
                    if index + 1 < selected_stations.len() {
                        tokio::time::sleep(AVIATION_REQUEST_SPACING).await;
                    }
                }
            }
            None => failures.push("aviation_weather: adapter is unavailable".to_owned()),
        }

        match self.ghcnh.as_ref() {
            Some(source) => {
                let tasks = selected_stations
                    .iter()
                    .filter_map(|(station, targets)| {
                        targets
                            .first()
                            .filter(|target| target.ghcnh_station_id.is_some())
                            .cloned()
                            .map(|target| (station.clone(), target))
                    })
                    .collect::<Vec<_>>();
                let outcomes = stream::iter(tasks)
                    .map(|(station, target)| async move {
                        let result = self.ingest_ghcnh_station(source, &station, &target).await;
                        (station.clone(), result)
                    })
                    .buffer_unordered(self.ghcnh_config.max_concurrency)
                    .collect::<Vec<_>>()
                    .await;
                for (station, result) in outcomes {
                    if let Err(error) = result {
                        self.record_evidence_failure(
                            &DomainSourceId::ghcnh(),
                            &DomainInstrumentKey::ghcnh(&station),
                            &error,
                            &mut failures,
                        )
                        .await;
                    }
                }
            }
            None => failures.push("ghcnh: adapter is unavailable".to_owned()),
        }

        match self.gefs.as_ref() {
            Some(source) => {
                if let Err(error) = self.ingest_gefs_cycle(source, &bindings, Utc::now()).await {
                    for station in selected_stations.keys() {
                        self.record_evidence_failure(
                            &DomainSourceId::gefs(),
                            &DomainInstrumentKey::gefs(station),
                            &error,
                            &mut failures,
                        )
                        .await;
                    }
                }
            }
            None => failures.push("gefs: adapter is unavailable".to_owned()),
        }

        if failures.is_empty() {
            Ok(())
        } else {
            Err(QuantError::config(format!(
                "Weather daily-temperature evidence pass failed for {} bindings: {}",
                failures.len(),
                failures.join(" | ")
            )))
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        if let Err(error) = self.source_supervisor.ensure_boot_reconciled().await {
            tracing::error!(%error, "Weather ingest blocked: expected-source reconciliation failed");
            return;
        }
        let weather = Arc::clone(&self).run_weather_loop(shutdown.child_token());
        let ghcnh = Arc::clone(&self).run_ghcnh_loop(shutdown.child_token());
        let gefs = Arc::clone(&self).run_gefs_loop(shutdown.child_token());
        let calibration = Arc::clone(&self).run_weather_calibration_loop(shutdown.child_token());
        tokio::join!(weather, ghcnh, gefs, calibration);
    }

    pub(super) async fn run_backfill(self: Arc<Self>, shutdown: CancellationToken) {
        if let Err(error) = self.source_supervisor.ensure_boot_reconciled().await {
            tracing::error!(%error, "Weather backfill blocked: expected-source reconciliation failed");
            return;
        }
        loop {
            if shutdown.is_cancelled() {
                return;
            }
            let bindings = match self.discover().await {
                Ok(bindings) => bindings.weather,
                Err(error) => {
                    tracing::warn!(%error, "Weather backfill binding discovery failed");
                    tokio::time::sleep(StdDuration::from_secs(1)).await;
                    continue;
                }
            };
            if let Some(source) = self.gefs.as_ref()
                && !bindings.is_empty()
                && let Err(error) = self
                    .ingest_gefs_backfill_cycle(source, &bindings, Utc::now())
                    .await
            {
                tracing::warn!(%error, "GEFS historical calibration backfill failed");
            }
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep(StdDuration::from_secs(self.gefs_config.poll_secs.max(1))) => {}
            }
        }
    }

    async fn discover(&self) -> QuantResult<WeatherBindings> {
        let rows = self.linkages.latest_for_active_markets().await?;
        let mut bindings = WeatherBindings {
            weather: configured_weather_targets(&self.station_profiles, Utc::now())?,
        };
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
        bindings: &mut WeatherBindings,
        linkage: MarketLinkage,
    ) -> QuantResult<()> {
        let LinkageOutcome::Resolved(resolved) = linkage.outcome else {
            return Ok(());
        };
        let MarketSubject::Weather(subject) = &resolved.subject else {
            return Ok(());
        };
        let live = resolved
            .source_bindings
            .iter()
            .find(|binding| binding.role == LinkageSourceRole::LiveEvent);
        match live {
            Some(source) if source.source_id == DomainSourceId::aviation_weather() => {
                let profile = self
                    .station_profiles
                    .get(subject.decision_group.station.as_str())
                    .ok_or_else(|| {
                        QuantError::config(format!(
                            "Weather station {} is absent from deploy profiles",
                            subject.decision_group.station
                        ))
                    })?;
                let profile_hash =
                    weather_station_profile_hash(&subject.decision_group.station, profile)?;
                if profile_hash != subject.decision_group.station_profile_hash
                    || profile.timezone != subject.decision_group.timezone
                {
                    return Err(QuantError::config(format!(
                        "Weather station profile drift for {}",
                        subject.decision_group.station
                    )));
                }
                let station = source
                    .instrument_key
                    .as_aviation_weather_station()
                    .filter(|station| station == &subject.decision_group.station)
                    .ok_or_else(|| {
                        QuantError::config("Weather subject/source station binding mismatch")
                    })?;
                let timezone = subject
                    .decision_group
                    .timezone
                    .parse::<Tz>()
                    .map_err(|error| {
                        QuantError::config(format!("invalid Weather linkage timezone: {error}"))
                    })?;
                bindings.weather.insert(
                    (station.to_string(), subject.decision_group.local_date),
                    WeatherTarget {
                        station,
                        timezone,
                        local_date: subject.decision_group.local_date,
                        latitude: profile.latitude,
                        longitude: profile.longitude,
                        ghcnh_station_id: profile.ghcnh_station_id.clone(),
                        station_profile_hash: profile_hash,
                    },
                );
            }
            None => {
                tracing::warn!(
                    market_id = %linkage.market_id,
                    "resolved active Weather linkage has no live-event source binding"
                );
            }
            Some(_) => {
                return Err(QuantError::config(format!(
                    "active Weather linkage {} has an unsupported live-event source",
                    linkage.market_id
                )));
            }
        }
        Ok(())
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
        let mut candidates = Vec::with_capacity(indices.len());
        for index in indices {
            let report = &reports[index];
            let target = targets.iter().find(|target| {
                report
                    .observed_at
                    .with_timezone(&target.timezone)
                    .date_naive()
                    == target.local_date
            });
            let Some(target) = target else {
                continue;
            };
            candidates.push(WeatherObservationCandidate {
                report: report.clone(),
                local_date: target.local_date,
            });
        }
        let persisted = self.facts.persist_observations(candidates).await?;
        for item in persisted {
            let target = targets
                .iter()
                .find(|target| target.local_date == item.local_date)
                .ok_or_else(|| {
                    QuantError::config(format!(
                        "persisted AviationWeather report has no target for {}",
                        item.local_date
                    ))
                })?;
            let checkpoint = DomainSourceCheckpoint::AviationWeather {
                available_at: item.report.available_at,
                published_at: item.report.published_at,
                observation_time: item.report.observed_at,
                revision: item.revision,
                report_hash: item.report.report_hash.clone(),
            };
            self.projections
                .apply_weather_report(
                    item.report,
                    target.timezone.to_string(),
                    item.local_date,
                    checkpoint,
                    gap_generations.get(&item.local_date).copied().unwrap_or(0),
                    true,
                )
                .await?;
        }
        for target in targets {
            if weather_day_close_due(
                target,
                &reports,
                Utc::now(),
                self.aviation_config.day_close_grace_secs,
            )? {
                self.projections
                    .close_weather_day(&target.station, target.local_date, Utc::now())
                    .await?;
            }
        }
        if !reports.is_empty() {
            self.source_supervisor
                .mark_source_recovered(&DomainSourceId::aviation_weather(), &instrument)
                .await?;
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
                let stations = weather_stations(&bindings);
                let worker = Arc::clone(&self);
                let source = Arc::clone(source);
                let outcomes = stream::iter(stations)
                    .map(move |(station, targets)| {
                        let worker = Arc::clone(&worker);
                        let source = Arc::clone(&source);
                        let target = targets
                            .into_iter()
                            .find(|target| target.ghcnh_station_id.is_some());
                        async move {
                            let result = match target {
                                Some(target) => {
                                    worker
                                        .ingest_ghcnh_station(&source, &station, &target)
                                        .await
                                }
                                None => Ok(()),
                            };
                            (station.clone(), result)
                        }
                    })
                    .buffer_unordered(self.ghcnh_config.max_concurrency)
                    .collect::<Vec<_>>()
                    .await;
                for (station, result) in outcomes {
                    if let Err(error) = result {
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
        let current_year = available_at.year();
        let ghcnh_station_id = target.ghcnh_station_id.as_deref().ok_or_else(|| {
            QuantError::config(format!(
                "GHCNh historical calibration is unavailable for {station}"
            ))
        })?;
        let mut year_files = Vec::new();
        let mut partition_manifest = Vec::new();
        let mut unpublished_years = Vec::new();
        for year in first_year..=available_at.year() {
            match source
                .yearly_station(station, ghcnh_station_id, year, available_at)
                .await?
            {
                Some(year_file) => {
                    partition_manifest.push((year, Some(year_file.file_hash.clone())));
                    year_files.push((year, year_file));
                }
                None if year == current_year => {
                    partition_manifest.push((year, None));
                    unpublished_years.push(year);
                }
                None => {
                    return Err(QuantError::config(format!(
                        "GHCNh historical partition {year} is missing for {station}"
                    )));
                }
            }
        }
        if year_files.is_empty() {
            return Err(QuantError::config(format!(
                "GHCNh returned no published calibration partitions for {station}"
            )));
        }
        let file_hash = CanonicalDigest::content_hash_json(&(
            "ghcnh_partition_manifest_v1",
            ghcnh_station_id,
            &partition_manifest,
        ))?;
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
            let mut candidates = Vec::new();
            for (_, year) in year_files {
                for report in year.reports {
                    last_hour = Some(last_hour.map_or(report.observed_at, |current| {
                        current.max(report.observed_at)
                    }));
                    let local_date = report
                        .observed_at
                        .with_timezone(&target.timezone)
                        .date_naive();
                    candidates.push(WeatherObservationCandidate { report, local_date });
                }
            }
            self.facts.persist_observations(candidates).await?;
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
            unpublished_years,
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

    async fn run_weather_calibration_loop(self: Arc<Self>, shutdown: CancellationToken) {
        let mut interval = tokio::time::interval(StdDuration::from_hours(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return,
                _ = interval.tick() => {}
            }
            let bindings = match self.discover().await {
                Ok(bindings) => bindings.weather,
                Err(error) => {
                    tracing::warn!(%error, "Weather calibration binding discovery failed");
                    continue;
                }
            };
            if bindings.is_empty() {
                continue;
            }
            if let Err(error) = self
                .publish_weather_calibration(&bindings, Utc::now())
                .await
            {
                tracing::warn!(%error, "Weather calibration publish remains blocked");
            }
        }
    }

    async fn publish_weather_calibration(
        &self,
        bindings: &BTreeMap<(String, NaiveDate), WeatherTarget>,
        now: DateTime<Utc>,
    ) -> QuantResult<()> {
        let runtime = self.runtime_config.load();
        if !runtime.domain.family_enabled(DomainFamily::Weather) {
            return Ok(());
        }
        let config = &runtime.domain.weather;
        let stations = weather_stations(bindings);
        if !self.gefs_backfill_complete(stations.keys(), now).await? {
            return Err(QuantError::config(
                "GEFS historical backfill has not reached the calibration boundary",
            ));
        }
        let lag = i64::try_from(config.availability_lag_secs).map_err(|error| {
            QuantError::config(format!(
                "Weather calibration availability lag overflow: {error}"
            ))
        })?;
        let availability_cutoff = now
            .checked_sub_signed(Duration::seconds(lag))
            .ok_or_else(|| QuantError::config("Weather calibration cutoff underflow"))?;
        let fit_end = Utc.from_utc_datetime(
            &availability_cutoff
                .date_naive()
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| QuantError::config("invalid Weather calibration day boundary"))?,
        );
        let fit_start = fit_end
            .checked_sub_signed(Duration::days(i64::from(config.calibration_lookback_days)))
            .ok_or_else(|| QuantError::config("Weather calibration fit window underflow"))?;
        let station_names = stations.keys().map(ToString::to_string).collect::<Vec<_>>();
        let (observations, forecasts) = self
            .load_weather_calibration_facts(station_names, fit_start, fit_end, availability_cutoff)
            .await?;
        let fit = fit_weather_station_lead_bias(
            &observations,
            &forecasts,
            fit_start,
            fit_end,
            config.minimum_complete_members,
        )?;
        ensure_weather_fit_coverage(
            &fit,
            stations.len(),
            usize::from(self.gefs_config.max_lead_hours / 3),
            config.minimum_bias_samples_per_lead,
        )?;
        self.activate_weather_calibration(fit, fit_start, fit_end, now)
            .await
    }

    async fn load_weather_calibration_facts(
        &self,
        station_names: Vec<String>,
        fit_start: DateTime<Utc>,
        fit_end: DateTime<Utc>,
        availability_cutoff: DateTime<Utc>,
    ) -> QuantResult<(Vec<WeatherObservationFact>, Vec<WeatherForecastPoint>)> {
        let observation_rows = self
            .fact_read
            .weather_observation_facts_between(
                station_names.clone(),
                (fit_start - Duration::hours(3)).timestamp_millis(),
                fit_end.timestamp_millis(),
                availability_cutoff.timestamp_millis(),
                availability_cutoff.timestamp_millis(),
            )
            .await?;
        let observations = observation_rows
            .into_iter()
            .map(|row| {
                WeatherObservationFact::from_clickhouse_row(row).ok_or_else(|| {
                    QuantError::config("invalid Weather observation row during calibration")
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let forecast_rows = self
            .fact_read
            .weather_forecast_facts_between(
                station_names,
                fit_start.timestamp_millis(),
                fit_end.timestamp_millis(),
                fit_end.timestamp_millis(),
                availability_cutoff.timestamp_millis(),
            )
            .await?;
        let forecasts = forecast_rows
            .into_iter()
            .map(|row| {
                WeatherForecastPoint::from_clickhouse_row(row).ok_or_else(|| {
                    QuantError::config("invalid Weather forecast row during calibration")
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        Ok((observations, forecasts))
    }

    async fn activate_weather_calibration(
        &self,
        fit: WeatherStationLeadBiasFit,
        fit_start: DateTime<Utc>,
        fit_end: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> QuantResult<()> {
        let content_hash = CanonicalDigest::content_hash_json(&(
            CalibrationKind::WeatherStationLeadBias,
            fit_start,
            fit_end,
            &fit.calibration_split_hash,
            fit.sample_count,
            &fit.payload,
        ))?;
        let publications = self.calibrations.published_weather_through(now).await?;
        if publications
            .last()
            .is_some_and(|publication| publication.content_hash == content_hash)
        {
            return Ok(());
        }
        let artifact = if let Some(artifact) = self
            .calibrations
            .find_by_content_hash(&content_hash)
            .await?
        {
            artifact
        } else {
            let new_artifact = NewCalibrationArtifact {
                artifact_id: CalibrationArtifactId::from_v7(),
                kind: CalibrationKind::WeatherStationLeadBias,
                content_hash: content_hash.clone(),
                fit_window_start: fit_start,
                fit_window_end: fit_end,
                calibration_split_hash: fit.calibration_split_hash,
                sample_count: fit.sample_count,
                payload_json: serde_json::to_value(&fit.payload).map_err(|error| {
                    QuantError::config(format!(
                        "Weather calibration payload serialization failed: {error}"
                    ))
                })?,
                active: false,
            };
            match self.calibrations.create(new_artifact).await {
                Ok(artifact) => artifact,
                Err(StorageError::Duplicate { .. }) => self
                    .calibrations
                    .find_by_content_hash(&content_hash)
                    .await?
                    .ok_or_else(|| {
                        QuantError::config(
                            "duplicate Weather calibration disappeared after create race",
                        )
                    })?,
                Err(error) => return Err(error.into()),
            }
        };
        self.calibrations.mark_active(&artifact.artifact_id).await?;
        Ok(())
    }

    async fn gefs_backfill_complete<'a>(
        &self,
        stations: impl Iterator<Item = &'a IcaoStation>,
        now: DateTime<Utc>,
    ) -> QuantResult<bool> {
        let end_date = now
            .date_naive()
            .pred_opt()
            .ok_or_else(|| QuantError::config("GEFS calibration boundary underflow"))?;
        let end = Utc.from_utc_datetime(
            &end_date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| QuantError::config("invalid GEFS calibration boundary"))?,
        );
        let required = end
            .checked_sub_signed(Duration::days(1))
            .ok_or_else(|| QuantError::config("GEFS required backfill boundary underflow"))?;
        for station in stations {
            let cursor = self
                .cursors
                .find(
                    &DomainSourceId::gefs(),
                    &DomainInstrumentKey::gefs_backfill(station),
                )
                .await?;
            let complete = cursor.is_some_and(|cursor| {
                matches!(
                    cursor.checkpoint_json,
                    DomainSourceCheckpoint::GefsBackfill {
                        completed_reference_time,
                        ..
                    } if completed_reference_time >= required
                )
            });
            if !complete {
                return Ok(false);
            }
        }
        Ok(true)
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
            for station in weather_stations(bindings).keys() {
                self.source_supervisor
                    .mark_source_recovered(
                        &DomainSourceId::gefs(),
                        &DomainInstrumentKey::gefs(station),
                    )
                    .await?;
            }
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

    async fn ingest_gefs_backfill_cycle(
        &self,
        source: &Arc<GefsSource>,
        bindings: &BTreeMap<(String, NaiveDate), WeatherTarget>,
        now: DateTime<Utc>,
    ) -> QuantResult<()> {
        let (backfill_start, backfill_end) = self.gefs_backfill_window(now)?;
        let stations = weather_stations(bindings);
        let next_by_station = self
            .next_gefs_backfill_by_station(&stations, backfill_start)
            .await?;
        let Some(reference_time) = next_by_station.values().copied().min() else {
            return Ok(());
        };
        if reference_time >= backfill_end {
            return Ok(());
        }
        let selected = next_by_station
            .iter()
            .filter(|(_, next)| **next == reference_time)
            .filter_map(|(station, _)| {
                stations
                    .get(station)
                    .and_then(|targets| targets.first())
                    .map(|target| GefsStationBinding {
                        station: station.clone(),
                        latitude: target.latitude,
                        longitude: target.longitude,
                    })
            })
            .collect::<Vec<_>>();
        if selected.is_empty() {
            return Err(QuantError::config(
                "GEFS backfill selected no station profiles",
            ));
        }
        let request_hash = CanonicalDigest::content_hash_json(&(
            "gefs_00z_calibration_backfill_v1",
            reference_time,
            self.gefs_config.max_lead_hours,
            selected
                .iter()
                .map(|binding| (&binding.station, binding.latitude, binding.longitude))
                .collect::<Vec<_>>(),
        ))?;
        let mut decoded = self
            .decode_gefs_members_for_stations(source, &selected, reference_time)
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
        for station in selected.iter().map(|binding| &binding.station) {
            self.upsert_source_cursor(
                DomainSourceId::gefs(),
                DomainInstrumentKey::gefs_backfill(station),
                DomainSourceCheckpoint::GefsBackfill {
                    completed_reference_time: reference_time,
                    request_hash: request_hash.clone(),
                    manifest_hash: run_manifest_hash.clone(),
                },
            )
            .await?;
        }
        Ok(())
    }

    fn gefs_backfill_window(
        &self,
        now: DateTime<Utc>,
    ) -> QuantResult<(DateTime<Utc>, DateTime<Utc>)> {
        let end_date = now
            .date_naive()
            .pred_opt()
            .ok_or_else(|| QuantError::config("GEFS backfill end date underflow"))?;
        let backfill_end = Utc.from_utc_datetime(
            &end_date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| QuantError::config("invalid GEFS backfill end time"))?,
        );
        let source_days = u32::from(self.ghcnh_config.calibration_years)
            .checked_mul(365)
            .ok_or_else(|| QuantError::config("GEFS backfill lookback overflow"))?;
        let runtime_days = self
            .runtime_config
            .load()
            .domain
            .weather
            .calibration_lookback_days;
        let backfill_start = backfill_end
            .checked_sub_signed(Duration::days(i64::from(source_days.max(runtime_days))))
            .ok_or_else(|| QuantError::config("GEFS backfill start underflow"))?;
        Ok((backfill_start, backfill_end))
    }

    async fn next_gefs_backfill_by_station(
        &self,
        stations: &BTreeMap<IcaoStation, Vec<WeatherTarget>>,
        backfill_start: DateTime<Utc>,
    ) -> QuantResult<BTreeMap<IcaoStation, DateTime<Utc>>> {
        let mut next_by_station = BTreeMap::new();
        for station in stations.keys() {
            let cursor = self
                .cursors
                .find(
                    &DomainSourceId::gefs(),
                    &DomainInstrumentKey::gefs_backfill(station),
                )
                .await?;
            let next = match cursor.map(|cursor| cursor.checkpoint_json) {
                Some(DomainSourceCheckpoint::GefsBackfill {
                    completed_reference_time,
                    ..
                }) => completed_reference_time
                    .checked_add_signed(Duration::days(1))
                    .ok_or_else(|| QuantError::config("GEFS backfill cursor overflow"))?,
                Some(_) => {
                    return Err(QuantError::config(
                        "GEFS backfill cursor contains a different checkpoint type",
                    ));
                }
                None => backfill_start,
            };
            next_by_station.insert(station.clone(), next.max(backfill_start));
        }
        Ok(next_by_station)
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
                            .daily_temperature_member(reference_time, lead_hours, member, &stations)
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

    async fn decode_gefs_members_for_stations(
        &self,
        source: &Arc<GefsSource>,
        stations: &[GefsStationBinding],
        reference_time: DateTime<Utc>,
    ) -> QuantResult<Vec<GefsDecodedMember>> {
        let mut decoded = Vec::new();
        for lead_hours in (3..=self.gefs_config.max_lead_hours).step_by(3) {
            let members = stream::iter(0_u8..=30)
                .map(|member| {
                    let source = Arc::clone(source);
                    let stations = stations.to_vec();
                    async move {
                        source
                            .daily_temperature_member(reference_time, lead_hours, member, &stations)
                            .await
                    }
                })
                .buffer_unordered(self.gefs_config.max_concurrency)
                .try_collect::<Vec<_>>()
                .await?;
            if members.len() != 31 {
                return Err(QuantError::config(format!(
                    "GEFS backfill lead {lead_hours} decoded {} of 31 members",
                    members.len()
                )));
            }
            decoded.extend(members);
        }
        Ok(decoded)
    }

    async fn write_gefs_rows(
        &self,
        decoded: Vec<GefsDecodedMember>,
        run_manifest_hash: &ContentHash,
    ) -> QuantResult<()> {
        let mut points = Vec::new();
        for member in decoded {
            for point in member.points {
                let instrument_key = DomainInstrumentKey::gefs(&point.station);
                for (variable, temperature) in [
                    (WeatherVariable::TemperatureMaximum, point.tmax_celsius),
                    (WeatherVariable::TemperatureMinimum, point.tmin_celsius),
                ] {
                    let report_hash = CanonicalDigest::content_hash_json(&(
                        "weather_forecast_fact_v2",
                        &instrument_key,
                        variable,
                        member.reference_time,
                        member.valid_time,
                        member.member,
                        temperature,
                        &point.grid_binding_hash,
                        run_manifest_hash,
                    ))?;
                    points.push(WeatherForecastPoint {
                        source_id: DomainSourceId::gefs(),
                        instrument_key: instrument_key.clone(),
                        subject_key: point.station.to_string(),
                        variable,
                        value: temperature.value(),
                        unit: DomainMeasurementUnit::Celsius,
                        precision: rust_decimal::Decimal::new(1, 1),
                        reference_time: member.reference_time,
                        valid_time: member.valid_time,
                        published_at: member.available_at,
                        available_at: member.available_at,
                        lead_hours: member.lead_hours,
                        member: Some(u16::from(member.member)),
                        revision: 0,
                        grid_binding_hash: point.grid_binding_hash.clone(),
                        run_manifest_hash: run_manifest_hash.clone(),
                        report_hash,
                    });
                }
            }
        }
        self.facts.persist_forecasts(points).await?;
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
                source_id: source_id.clone(),
                instrument_key: instrument_key.clone(),
                checkpoint_json: checkpoint,
                checkpoint_hash,
                status: DomainCursorStatus::Live.as_str().to_owned(),
                last_error: None,
                updated_at: Utc::now(),
            })
            .await?;
        self.source_supervisor
            .mark_source_recovered(&source_id, &instrument_key)
            .await?;
        Ok(())
    }

    async fn record_evidence_failure(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
        error: &QuantError,
        failures: &mut Vec<String>,
    ) {
        let reason = error.to_string();
        if let Err(status_error) = self
            .source_supervisor
            .mark_source_failed(source_id, instrument_key, reason.clone())
            .await
        {
            failures.push(format!(
                "{source_id}/{instrument_key}: {reason}; source-health update failed: {status_error}"
            ));
        } else {
            failures.push(format!("{source_id}/{instrument_key}: {reason}"));
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

fn configured_weather_targets(
    station_profiles: &BTreeMap<String, WeatherStationProfileConfig>,
    now: DateTime<Utc>,
) -> QuantResult<BTreeMap<(String, NaiveDate), WeatherTarget>> {
    let mut targets = BTreeMap::new();
    for (station_code, profile) in station_profiles {
        let station = IcaoStation::parse(station_code).map_err(|error| {
            QuantError::config(format!(
                "invalid configured Weather station {station_code}: {error}"
            ))
        })?;
        let timezone = profile.timezone.parse::<Tz>().map_err(|error| {
            QuantError::config(format!(
                "invalid timezone for Weather station {station}: {error}"
            ))
        })?;
        let station_profile_hash = weather_station_profile_hash(&station, profile)?;
        let local_today = now.with_timezone(&timezone).date_naive();
        for days_ago in 0..=WEATHER_BOOTSTRAP_DAYS {
            let local_date = local_today
                .checked_sub_days(Days::new(days_ago))
                .ok_or_else(|| QuantError::config("Weather bootstrap date underflow"))?;
            targets.insert(
                (station.to_string(), local_date),
                WeatherTarget {
                    station: station.clone(),
                    timezone,
                    local_date,
                    latitude: profile.latitude,
                    longitude: profile.longitude,
                    ghcnh_station_id: profile.ghcnh_station_id.clone(),
                    station_profile_hash: station_profile_hash.clone(),
                },
            );
        }
    }
    Ok(targets)
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

fn ensure_weather_fit_coverage(
    fit: &WeatherStationLeadBiasFit,
    station_count: usize,
    expected_leads: usize,
    minimum_samples: u32,
) -> QuantResult<()> {
    let incomplete = fit.payload.stations.len() != station_count
        || fit.payload.stations.iter().any(|station| {
            station.leads.len() != expected_leads
                || station
                    .leads
                    .iter()
                    .any(|lead| lead.sample_count < minimum_samples)
        });
    if incomplete {
        return Err(QuantError::config(
            "Weather calibration does not cover every station/lead at the governed sample floor",
        ));
    }
    Ok(())
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
        available_at,
        published_at,
        observation_time,
        revision,
        report_hash,
    } = &cursor.checkpoint_json
    else {
        return Err(QuantError::config(
            "AviationWeather cursor contains a different checkpoint type",
        ));
    };
    let mut indices = Vec::new();
    for (index, report) in reports.iter().enumerate() {
        let (report_revision, _) = weather_revision(reports, index)?;
        if (
            report.available_at,
            report.published_at,
            report.observed_at,
            report_revision,
            &report.report_hash,
        ) > (
            *available_at,
            *published_at,
            *observation_time,
            *revision,
            report_hash,
        ) {
            indices.push(index);
        }
    }
    Ok(indices)
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
        .filter(|candidate| candidate.observed_at == report.observed_at)
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
    reports: &[WeatherObservationReport],
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
    let grace_elapsed = now >= (midnight + Duration::seconds(grace)).with_timezone(&Utc);
    let next_day_observed = reports.iter().any(|report| {
        report.subject_key == target.station.as_str()
            && report
                .observed_at
                .with_timezone(&target.timezone)
                .date_naive()
                >= next_date
            && report.available_at <= now
    });
    Ok(grace_elapsed && next_day_observed)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
    use quant_pivot_models::{
        config::{WeatherHistoricalBindingKind, WeatherStationProfileConfig},
        domain::{
            DomainCursorStatus, DomainSourceCheckpoint, DomainSourceCursorInfo,
            WeatherObservationReport, WeatherObservationReportKind,
        },
        types::{
            ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, IcaoStation,
            WeatherVariable,
        },
    };
    use rust_decimal_macros::dec;

    use super::{
        WEATHER_BOOTSTRAP_DAYS, WeatherTarget, configured_weather_targets, weather_day_close_due,
        weather_resume_indices,
    };

    fn hash(seed: char) -> ContentHash {
        ContentHash::parse(format!("blake3:{}", seed.to_string().repeat(64))).expect("hash")
    }

    fn report(
        observed_at: DateTime<Utc>,
        available_at: DateTime<Utc>,
        seed: char,
    ) -> WeatherObservationReport {
        let station = IcaoStation::parse("KJFK").expect("station");
        WeatherObservationReport {
            source_id: DomainSourceId::aviation_weather(),
            instrument_key: DomainInstrumentKey::aviation_weather(&station),
            subject_key: station.to_string(),
            report_kind: WeatherObservationReportKind::Metar,
            variable: WeatherVariable::Temperature,
            value: dec!(20),
            unit: DomainMeasurementUnit::Celsius,
            precision: dec!(0.1),
            observed_at,
            valid_from: None,
            valid_to: None,
            published_at: available_at,
            available_at,
            report_hash: hash(seed),
            raw_report: seed.to_string(),
        }
    }

    #[test]
    fn configured_station_bootstraps_without_market_linkage() {
        let profiles = BTreeMap::from([(
            "KJFK".to_owned(),
            WeatherStationProfileConfig {
                timezone: "America/New_York".to_owned(),
                latitude: dec!(40.6398),
                longitude: dec!(-73.7789),
                elevation_meters: dec!(4),
                ghcnh_station_id: Some("USW00094789".to_owned()),
                historical_binding_kind: WeatherHistoricalBindingKind::ExactStation,
            },
        )]);
        let targets = configured_weather_targets(
            &profiles,
            Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap(),
        )
        .expect("configured targets");
        assert_eq!(
            targets.len(),
            usize::try_from(WEATHER_BOOTSTRAP_DAYS + 1).expect("bootstrap window fits usize")
        );
        assert!(
            targets
                .values()
                .all(|target| target.station.as_str() == "KJFK")
        );
    }

    #[test]
    fn day_close_requires_first_observation_from_the_following_local_day() {
        let timezone = "America/New_York".parse().expect("timezone");
        let local_date = NaiveDate::from_ymd_opt(2026, 7, 1).expect("date");
        let target = WeatherTarget {
            station: IcaoStation::parse("KJFK").expect("station"),
            timezone,
            local_date,
            latitude: dec!(40.6398),
            longitude: dec!(-73.7789),
            ghcnh_station_id: Some("USW00094789".to_owned()),
            station_profile_hash: hash('d'),
        };
        let now = Utc.with_ymd_and_hms(2026, 7, 2, 6, 0, 0).unwrap();
        assert!(!weather_day_close_due(&target, &[], now, 3_600).expect("close gate"));
        let next_day = Utc.with_ymd_and_hms(2026, 7, 2, 4, 5, 0).unwrap();
        assert!(
            weather_day_close_due(
                &target,
                &[report(next_day, next_day + Duration::minutes(1), 'e')],
                now,
                3_600,
            )
            .expect("close gate")
        );
    }

    #[test]
    fn same_time_correction_resumes_after_exact_report_hash() {
        let observed = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
        let first = report(observed, observed + Duration::minutes(1), 'a');
        let correction = report(observed, observed + Duration::minutes(2), 'b');
        let cursor = DomainSourceCursorInfo {
            source_id: DomainSourceId::aviation_weather(),
            instrument_key: first.instrument_key.clone(),
            checkpoint_json: DomainSourceCheckpoint::AviationWeather {
                available_at: first.available_at,
                published_at: first.published_at,
                observation_time: first.observed_at,
                revision: 0,
                report_hash: first.report_hash.clone(),
            },
            checkpoint_hash: hash('c'),
            status: DomainCursorStatus::Live.as_str().to_owned(),
            last_error: None,
            created_at: observed,
            updated_at: observed,
        };
        assert_eq!(
            weather_resume_indices(&[first, correction], Some(&cursor)).expect("resume"),
            vec![1]
        );
    }
}
