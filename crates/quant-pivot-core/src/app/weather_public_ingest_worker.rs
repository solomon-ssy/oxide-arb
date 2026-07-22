//! Public Weather vertical ingestion outside daily airport temperature.

use std::{sync::Arc, time::Duration as StdDuration};

use chrono::{DateTime, Datelike, Days, Duration, Months, NaiveDate, Timelike, Utc};
use chrono_tz::{Asia::Hong_Kong, Tz};
use quant_pivot_api::weather::{
    airnow::AirNowSource,
    gistemp::NasaGistempSource,
    hko::HkoOpenDataSource,
    nhc::{NhcBasin, NhcSource},
    nsidc::{NsidcSeaIceSource, SeaIceHemisphere},
    nws::NwsObservationSource,
    tornado::TornadoSource,
};
use quant_pivot_error::{QuantError, QuantResult};
use quant_pivot_models::{
    config::{
        AirNowPm25SiteBindingConfig, AirNowSourceConfig, HkoOpenDataSourceConfig,
        NasaGistempSourceConfig, NhcSourceConfig, NsidcSeaIceSourceConfig,
        NwsObservationSourceConfig, TornadoSourceConfig, WeatherVerticalBindingsConfig,
    },
    domain::data_plane::{
        DomainCursorStatus, DomainSourceCheckpoint, UpsertDomainSourceCursor,
        WeatherObservationReport,
    },
    hashing::CanonicalDigest,
    types::{
        DomainInstrumentKey, DomainSourceId, HkoStation, IcaoStation, WeatherTemperatureStatistic,
    },
};
use quant_pivot_repository::traits::DomainSourceCursorRepository;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::{
    app::domain_source_supervisor::DomainSourceSupervisor,
    service::weather_fact_ingest::{WeatherFactIngestService, WeatherObservationCandidate},
};

const AIRNOW_CORRECTION_BATCH_HOURS: i64 = 6;
const NCEI_FINALIZATION_LAG_DAYS: u64 = 120;
const ERROR_DETAIL_LIMIT: usize = 2_000;

pub struct WeatherPublicIngestWorker {
    source_supervisor: Arc<DomainSourceSupervisor>,
    cursors: Arc<dyn DomainSourceCursorRepository>,
    facts: Arc<WeatherFactIngestService>,
    hko: Option<Arc<HkoOpenDataSource>>,
    airnow: Option<Arc<AirNowSource>>,
    tornado: Option<Arc<TornadoSource>>,
    nhc: Option<Arc<NhcSource>>,
    gistemp: Option<Arc<NasaGistempSource>>,
    nsidc: Option<Arc<NsidcSeaIceSource>>,
    nws: Option<Arc<NwsObservationSource>>,
    hko_config: HkoOpenDataSourceConfig,
    airnow_config: AirNowSourceConfig,
    tornado_config: TornadoSourceConfig,
    nhc_config: NhcSourceConfig,
    gistemp_config: NasaGistempSourceConfig,
    nsidc_config: NsidcSeaIceSourceConfig,
    nws_config: NwsObservationSourceConfig,
    bindings: WeatherVerticalBindingsConfig,
}

pub struct WeatherPublicIngestDeps {
    pub source_supervisor: Arc<DomainSourceSupervisor>,
    pub cursors: Arc<dyn DomainSourceCursorRepository>,
    pub facts: Arc<WeatherFactIngestService>,
    pub hko: Option<Arc<HkoOpenDataSource>>,
    pub airnow: Option<Arc<AirNowSource>>,
    pub tornado: Option<Arc<TornadoSource>>,
    pub nhc: Option<Arc<NhcSource>>,
    pub gistemp: Option<Arc<NasaGistempSource>>,
    pub nsidc: Option<Arc<NsidcSeaIceSource>>,
    pub nws: Option<Arc<NwsObservationSource>>,
    pub hko_config: HkoOpenDataSourceConfig,
    pub airnow_config: AirNowSourceConfig,
    pub tornado_config: TornadoSourceConfig,
    pub nhc_config: NhcSourceConfig,
    pub gistemp_config: NasaGistempSourceConfig,
    pub nsidc_config: NsidcSeaIceSourceConfig,
    pub nws_config: NwsObservationSourceConfig,
    pub bindings: WeatherVerticalBindingsConfig,
}

impl WeatherPublicIngestWorker {
    #[must_use]
    pub fn new(deps: WeatherPublicIngestDeps) -> Self {
        Self {
            source_supervisor: deps.source_supervisor,
            cursors: deps.cursors,
            facts: deps.facts,
            hko: deps.hko,
            airnow: deps.airnow,
            tornado: deps.tornado,
            nhc: deps.nhc,
            gistemp: deps.gistemp,
            nsidc: deps.nsidc,
            nws: deps.nws,
            hko_config: deps.hko_config,
            airnow_config: deps.airnow_config,
            tornado_config: deps.tornado_config,
            nhc_config: deps.nhc_config,
            gistemp_config: deps.gistemp_config,
            nsidc_config: deps.nsidc_config,
            nws_config: deps.nws_config,
            bindings: deps.bindings,
        }
    }

    /// Execute one bounded pass across every configured public Weather source.
    ///
    /// This is the finite orchestration boundary used by evidence bootstrap.
    /// It shares the exact adapter, fact, cursor, and source-health paths used
    /// by the periodic runtime loops. Every configured binding is attempted;
    /// failures are persisted first and returned together after the pass so one
    /// unavailable source cannot hide the state of the remaining sources.
    pub async fn run_once(&self) -> QuantResult<()> {
        self.source_supervisor.ensure_boot_reconciled().await?;
        let mut failures = Vec::new();

        self.run_local_observation_evidence(&mut failures).await;
        self.run_hazard_evidence(&mut failures).await;
        self.run_climate_evidence(&mut failures).await;

        if failures.is_empty() {
            Ok(())
        } else {
            Err(QuantError::config(format!(
                "public Weather evidence pass failed for {} source bindings: {}",
                failures.len(),
                failures.join(" | ")
            )))
        }
    }

    async fn run_local_observation_evidence(&self, failures: &mut Vec<String>) {
        for binding in &self.bindings.hko_rainfall {
            let instrument = DomainInstrumentKey::hko_rainfall(&binding.place);
            let result = match self.hko.as_ref() {
                Some(source) => {
                    self.ingest_hko(source, &binding.place, &binding.timezone)
                        .await
                }
                None => Err(QuantError::config("HKO adapter is unavailable")),
            };
            self.complete_evidence_cycle(
                DomainSourceId::hko_open_data(),
                instrument,
                result,
                failures,
            )
            .await;
        }
        for binding in &self.bindings.hko_daily_temperature {
            let station = match HkoStation::parse(&binding.station) {
                Ok(station) => station,
                Err(error) => {
                    failures.push(format!(
                        "hko_open_data/{}: invalid station: {error}",
                        binding.station
                    ));
                    continue;
                }
            };
            for statistic in [
                WeatherTemperatureStatistic::Maximum,
                WeatherTemperatureStatistic::Minimum,
            ] {
                let instrument = DomainInstrumentKey::hko_daily_temperature(&station, statistic);
                let result = match self.hko.as_ref() {
                    Some(source) => {
                        self.ingest_hko_daily_temperature(
                            source,
                            &station,
                            statistic,
                            &binding.timezone,
                        )
                        .await
                    }
                    None => Err(QuantError::config("HKO adapter is unavailable")),
                };
                self.complete_evidence_cycle(
                    DomainSourceId::hko_open_data(),
                    instrument,
                    result,
                    failures,
                )
                .await;
            }
        }
        for binding in &self.bindings.airnow_pm25_reporting_areas {
            let area_key = format!("{}:{}", binding.state, binding.area);
            let instrument = DomainInstrumentKey::airnow_pm25_observation(&area_key);
            let result = match self.airnow.as_ref() {
                Some(source) => {
                    self.ingest_airnow(source, &binding.area, &binding.state, &binding.timezone)
                        .await
                }
                None => Err(QuantError::config("AirNow adapter is unavailable")),
            };
            self.complete_evidence_cycle(DomainSourceId::airnow(), instrument, result, failures)
                .await;
        }
        for binding in &self.bindings.airnow_pm25_sites {
            let instrument = DomainInstrumentKey::airnow_pm25_site(&binding.aqsid);
            let result = match self.airnow.as_ref() {
                Some(source) => self.ingest_airnow_pm25_site(source, binding).await,
                None => Err(QuantError::config("AirNow adapter is unavailable")),
            };
            self.complete_evidence_cycle(DomainSourceId::airnow(), instrument, result, failures)
                .await;
        }
    }

    async fn run_hazard_evidence(&self, failures: &mut Vec<String>) {
        for binding in &self.bindings.tornado_regions {
            let spc_instrument = DomainInstrumentKey::spc_tornado(&binding.region_id);
            let spc_result = match self.tornado.as_ref() {
                Some(source) => {
                    self.ingest_spc(source, &binding.region_id, &binding.spc_state_code)
                        .await
                }
                None => Err(QuantError::config("SPC adapter is unavailable")),
            };
            self.complete_evidence_cycle(
                DomainSourceId::spc_storm_reports(),
                spc_instrument,
                spc_result,
                failures,
            )
            .await;

            let ncei_instrument = DomainInstrumentKey::ncei_tornado(&binding.region_id);
            let ncei_result = match self.tornado.as_ref() {
                Some(source) => {
                    self.ingest_ncei(
                        source,
                        &binding.region_id,
                        &binding.ncei_state_name,
                        &binding.timezone,
                    )
                    .await
                }
                None => Err(QuantError::config("NCEI adapter is unavailable")),
            };
            self.complete_evidence_cycle(
                DomainSourceId::ncei_storm_events(),
                ncei_instrument,
                ncei_result,
                failures,
            )
            .await;
        }
        if let Some(source) = self.nhc.as_ref()
            && let Err(error) = self.ingest_nhc_advisories(source).await
        {
            failures.push(format!("nhc_advisory/dynamic: {error}"));
        }
        for binding in &self.bindings.nhc_historical_storms {
            let instrument = DomainInstrumentKey::nhc_hurdat2(&binding.basin, &binding.storm_id);
            let result = match self.nhc.as_ref() {
                Some(source) => {
                    self.ingest_nhc_best_track(source, &binding.basin, &binding.storm_id)
                        .await
                }
                None => Err(QuantError::config("HURDAT2 adapter is unavailable")),
            };
            self.complete_evidence_cycle(
                DomainSourceId::nhc_hurdat2(),
                instrument,
                result,
                failures,
            )
            .await;
        }
    }

    async fn run_climate_evidence(&self, failures: &mut Vec<String>) {
        let gistemp_result = match self.gistemp.as_ref() {
            Some(source) => self.ingest_gistemp(source).await,
            None => Err(QuantError::config("NASA GISTEMP adapter is unavailable")),
        };
        self.complete_evidence_cycle(
            DomainSourceId::nasa_gistemp(),
            DomainInstrumentKey::nasa_gistemp_loti(),
            gistemp_result,
            failures,
        )
        .await;
        for hemisphere in [SeaIceHemisphere::North, SeaIceHemisphere::South] {
            let instrument = DomainInstrumentKey::nsidc_sea_ice_extent(hemisphere.as_str());
            let result = match self.nsidc.as_ref() {
                Some(source) => self.ingest_nsidc(source, hemisphere).await,
                None => Err(QuantError::config("NSIDC adapter is unavailable")),
            };
            self.complete_evidence_cycle(
                DomainSourceId::nsidc_sea_ice_index(),
                instrument,
                result,
                failures,
            )
            .await;
        }
        for binding in &self.bindings.nws_wind_stations {
            let station = match IcaoStation::parse(&binding.station) {
                Ok(station) => station,
                Err(error) => {
                    failures.push(format!(
                        "nws_observation/{}: invalid station: {error}",
                        binding.station
                    ));
                    continue;
                }
            };
            let result = match self.nws.as_ref() {
                Some(source) => self.ingest_nws(source, &station, &binding.timezone).await,
                None => Err(QuantError::config("NWS adapter is unavailable")),
            };
            self.complete_evidence_cycle(
                DomainSourceId::nws_observation(),
                DomainInstrumentKey::nws_wind_gust(&station),
                result,
                failures,
            )
            .await;
        }
    }

    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        if let Err(error) = self.source_supervisor.ensure_boot_reconciled().await {
            tracing::error!(%error, "public Weather ingest blocked by expectation reconciliation");
            return;
        }
        tokio::join!(
            Arc::clone(&self).run_hko_rainfall_loop(shutdown.child_token()),
            Arc::clone(&self).run_hko_daily_temperature_loop(shutdown.child_token()),
            Arc::clone(&self).run_airnow_loop(shutdown.child_token()),
            Arc::clone(&self).run_spc_loop(shutdown.child_token()),
            Arc::clone(&self).run_ncei_loop(shutdown.child_token()),
            Arc::clone(&self).run_nhc_advisory_loop(shutdown.child_token()),
            Arc::clone(&self).run_nhc_best_track_loop(shutdown.child_token()),
            Arc::clone(&self).run_gistemp_loop(shutdown.child_token()),
            Arc::clone(&self).run_nsidc_loop(shutdown.child_token()),
            Arc::clone(&self).run_nws_loop(shutdown.child_token()),
        );
    }

    async fn run_hko_rainfall_loop(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            for binding in &self.bindings.hko_rainfall {
                let instrument = DomainInstrumentKey::hko_rainfall(&binding.place);
                let result = match self.hko.as_ref() {
                    Some(source) => {
                        self.ingest_hko(source, &binding.place, &binding.timezone)
                            .await
                    }
                    None => Err(QuantError::config("HKO adapter is unavailable")),
                };
                self.finish_cycle(DomainSourceId::hko_open_data(), instrument, result)
                    .await;
            }
            if wait_or_cancel(
                &shutdown,
                StdDuration::from_secs(self.hko_config.rainfall_poll_secs.max(1)),
            )
            .await
            {
                return;
            }
        }
    }

    async fn run_hko_daily_temperature_loop(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            for binding in &self.bindings.hko_daily_temperature {
                let station = match HkoStation::parse(&binding.station) {
                    Ok(station) => station,
                    Err(error) => {
                        tracing::error!(station = %binding.station, %error, "invalid HKO daily-temperature binding escaped deploy validation");
                        continue;
                    }
                };
                for statistic in [
                    WeatherTemperatureStatistic::Maximum,
                    WeatherTemperatureStatistic::Minimum,
                ] {
                    let instrument =
                        DomainInstrumentKey::hko_daily_temperature(&station, statistic);
                    let result = match self.hko.as_ref() {
                        Some(source) => {
                            self.ingest_hko_daily_temperature(
                                source,
                                &station,
                                statistic,
                                &binding.timezone,
                            )
                            .await
                        }
                        None => Err(QuantError::config("HKO adapter is unavailable")),
                    };
                    self.finish_cycle(DomainSourceId::hko_open_data(), instrument, result)
                        .await;
                }
            }
            if wait_or_cancel(
                &shutdown,
                StdDuration::from_secs(self.hko_config.daily_temperature_poll_secs.max(1)),
            )
            .await
            {
                return;
            }
        }
    }

    async fn ingest_hko_daily_temperature(
        &self,
        source: &HkoOpenDataSource,
        station: &HkoStation,
        statistic: WeatherTemperatureStatistic,
        timezone: &str,
    ) -> QuantResult<DomainSourceCheckpoint> {
        if timezone != "Asia/Hong_Kong" {
            return Err(QuantError::config(format!(
                "HKO station `{station}` must use Asia/Hong_Kong timezone"
            )));
        }
        let available_at = Utc::now();
        let target_date = hko_latest_publishable_date(available_at)?;
        let mut selected = None;
        for offset in 0..u32::from(self.hko_config.daily_temperature_lookback_months) {
            let partition = hko_partition_date(target_date, offset)?;
            let month = source
                .daily_temperatures(
                    station,
                    statistic,
                    partition.year(),
                    partition.month(),
                    available_at,
                )
                .await?;
            let target_report = month
                .reports
                .iter()
                .filter(|report| {
                    report.valid_from.is_some_and(|start| {
                        start.with_timezone(&Hong_Kong).date_naive() <= target_date
                    })
                })
                .max_by_key(|report| (report.observed_at, &report.report_hash))
                .cloned();
            if let Some(target_report) = target_report {
                selected = Some((month, target_report));
                break;
            }
        }
        let (month, target_report) = selected.ok_or_else(|| {
            QuantError::config(format!(
                "HKO {station} {} has no complete row at or before {target_date} within {} monthly partitions",
                statistic.as_str(),
                self.hko_config.daily_temperature_lookback_months
            ))
        })?;
        let response_hash = month.response_hash;
        let candidates = month
            .reports
            .into_iter()
            .map(|report| {
                let local_date = report
                    .valid_from
                    .ok_or_else(|| QuantError::config("HKO daily temperature has no day start"))?
                    .with_timezone(&Hong_Kong)
                    .date_naive();
                Ok(WeatherObservationCandidate { report, local_date })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        self.facts.persist_observations(candidates).await?;
        Ok(DomainSourceCheckpoint::HkoDailyTemperature {
            day_end: target_report.observed_at,
            available_at,
            response_hash,
            report_hash: target_report.report_hash,
        })
    }

    async fn ingest_hko(
        &self,
        source: &HkoOpenDataSource,
        place: &str,
        timezone: &str,
    ) -> QuantResult<DomainSourceCheckpoint> {
        let available_at = Utc::now();
        let report = source
            .rainfall(place, available_at)
            .await?
            .ok_or_else(|| QuantError::config(format!("HKO place `{place}` is absent")))?;
        let local_date = local_date(&report, timezone)?;
        self.facts
            .persist_observations(vec![WeatherObservationCandidate {
                report: report.clone(),
                local_date,
            }])
            .await?;
        Ok(DomainSourceCheckpoint::HkoRainfall {
            window_end: report.observed_at,
            published_at: report.published_at,
            report_hash: report.report_hash,
        })
    }

    async fn run_airnow_loop(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            for binding in &self.bindings.airnow_pm25_reporting_areas {
                let area_key = format!("{}:{}", binding.state, binding.area);
                let instrument = DomainInstrumentKey::airnow_pm25_observation(&area_key);
                let result = match self.airnow.as_ref() {
                    Some(source) => {
                        self.ingest_airnow(source, &binding.area, &binding.state, &binding.timezone)
                            .await
                    }
                    None => Err(QuantError::config("AirNow adapter is unavailable")),
                };
                self.finish_cycle(DomainSourceId::airnow(), instrument, result)
                    .await;
            }
            for binding in &self.bindings.airnow_pm25_sites {
                let instrument = DomainInstrumentKey::airnow_pm25_site(&binding.aqsid);
                let result = match self.airnow.as_ref() {
                    Some(source) => self.ingest_airnow_pm25_site(source, binding).await,
                    None => Err(QuantError::config("AirNow adapter is unavailable")),
                };
                self.finish_cycle(DomainSourceId::airnow(), instrument, result)
                    .await;
            }
            if wait_or_cancel(
                &shutdown,
                StdDuration::from_secs(self.airnow_config.poll_secs.max(1)),
            )
            .await
            {
                return;
            }
        }
    }

    async fn ingest_airnow(
        &self,
        source: &AirNowSource,
        area: &str,
        state: &str,
        timezone: &str,
    ) -> QuantResult<DomainSourceCheckpoint> {
        let timezone = parse_timezone(timezone)?;
        let available_at = Utc::now();
        let snapshot = source
            .pm25_reporting_area(area, state, timezone, available_at)
            .await?;
        let area_key = format!("{state}:{area}");
        let instrument = DomainInstrumentKey::airnow_pm25_observation(&area_key);
        let cursor = self
            .cursors
            .find(&DomainSourceId::airnow(), &instrument)
            .await?;
        let now_hour = truncate_hour(available_at)?;
        let previous_scan = cursor
            .as_ref()
            .and_then(|cursor| match cursor.checkpoint_json {
                DomainSourceCheckpoint::AirNowPm25Area {
                    correction_scan_hour,
                    ..
                } => Some(correction_scan_hour),
                _ => None,
            });
        let (scan_start, scan_end) = airnow_scan_window(
            previous_scan,
            now_hour,
            self.airnow_config.correction_lookback_hours,
        )?;
        let mut reports = snapshot.observations;
        let mut hour = scan_start;
        while hour <= scan_end {
            if let Some(report) = source
                .hourly_pm25_area_observation(area, state, hour, Utc::now())
                .await?
            {
                reports.push(report);
            }
            hour = hour
                .checked_add_signed(Duration::hours(1))
                .ok_or_else(|| QuantError::config("AirNow scan hour overflow"))?;
        }
        let candidates = reports
            .iter()
            .cloned()
            .map(|report| WeatherObservationCandidate {
                local_date: report.observed_at.with_timezone(&timezone).date_naive(),
                report,
            })
            .collect();
        self.facts.persist_observations(candidates).await?;
        let latest = reports
            .iter()
            .max_by_key(|report| (report.observed_at, report.available_at, &report.report_hash))
            .ok_or_else(|| QuantError::config(format!("AirNow returned no AQI for {area_key}")))?;
        if !snapshot.forecasts.is_empty() {
            let forecasts = snapshot.forecasts;
            let reference_time = forecasts
                .iter()
                .map(|point| point.reference_time)
                .max()
                .ok_or_else(|| QuantError::config("AirNow forecast reference time is absent"))?;
            let max_valid_time = forecasts
                .iter()
                .map(|point| point.valid_time)
                .max()
                .ok_or_else(|| QuantError::config("AirNow forecast valid time is absent"))?;
            self.facts.persist_forecasts(forecasts).await?;
            let forecast_source = DomainSourceId::airnow();
            let forecast_instrument = DomainInstrumentKey::airnow_pm25_forecast(&area_key);
            self.upsert_cursor(
                forecast_source.clone(),
                forecast_instrument.clone(),
                DomainSourceCheckpoint::AirNowPm25Forecast {
                    reference_time,
                    max_valid_time,
                    available_at,
                    file_hash: snapshot.file_hash,
                },
            )
            .await?;
            self.source_supervisor
                .mark_source_recovered(&forecast_source, &forecast_instrument)
                .await?;
        }
        Ok(DomainSourceCheckpoint::AirNowPm25Area {
            valid_time: latest.observed_at,
            available_at: latest.available_at,
            report_hash: latest.report_hash,
            correction_scan_hour: scan_end,
        })
    }

    async fn ingest_airnow_pm25_site(
        &self,
        source: &AirNowSource,
        binding: &AirNowPm25SiteBindingConfig,
    ) -> QuantResult<DomainSourceCheckpoint> {
        let timezone = parse_timezone(&binding.timezone)?;
        let instrument = DomainInstrumentKey::airnow_pm25_site(&binding.aqsid);
        let cursor = self
            .cursors
            .find(&DomainSourceId::airnow(), &instrument)
            .await?;
        let (previous_scan, previous_observation) =
            cursor
                .as_ref()
                .map_or((None, None), |cursor| match &cursor.checkpoint_json {
                    DomainSourceCheckpoint::AirNowPm25Site {
                        last_valid_time,
                        available_at,
                        last_report_hash,
                        correction_scan_hour,
                    } => (
                        Some(*correction_scan_hour),
                        match (last_valid_time, last_report_hash) {
                            (Some(time), Some(hash)) => Some((*time, *available_at, *hash)),
                            _ => None,
                        },
                    ),
                    _ => (None, None),
                });
        let now_hour = truncate_hour(Utc::now())?;
        let (scan_start, scan_end) = airnow_scan_window(
            previous_scan,
            now_hour,
            self.airnow_config.correction_lookback_hours,
        )?;
        let mut reports = Vec::new();
        let mut hour = scan_start;
        while hour <= scan_end {
            if let Some(report) = source
                .hourly_pm25_site_observation(binding, hour, Utc::now())
                .await?
            {
                reports.push(report);
            }
            hour = hour
                .checked_add_signed(Duration::hours(1))
                .ok_or_else(|| QuantError::config("AirNow site scan hour overflow"))?;
        }
        self.facts
            .persist_observations(
                reports
                    .iter()
                    .cloned()
                    .map(|report| WeatherObservationCandidate {
                        local_date: report.observed_at.with_timezone(&timezone).date_naive(),
                        report,
                    })
                    .collect(),
            )
            .await?;
        let current_observation = reports
            .iter()
            .max_by_key(|report| (report.observed_at, report.available_at, &report.report_hash))
            .map(|report| (report.observed_at, report.available_at, report.report_hash));
        let latest = [previous_observation, current_observation]
            .into_iter()
            .flatten()
            .max_by_key(|(valid_time, available_at, hash)| (*valid_time, *available_at, *hash));
        let checkpoint = DomainSourceCheckpoint::AirNowPm25Site {
            last_valid_time: latest.as_ref().map(|(valid_time, _, _)| *valid_time),
            available_at: latest
                .as_ref()
                .map_or_else(Utc::now, |(_, available_at, _)| *available_at),
            last_report_hash: latest.as_ref().map(|(_, _, hash)| *hash),
            correction_scan_hour: scan_end,
        };
        if latest.is_none() {
            self.upsert_cursor_with_status(
                DomainSourceId::airnow(),
                instrument,
                checkpoint,
                DomainCursorStatus::Backfilling,
            )
            .await?;
            return Err(QuantError::config(format!(
                "AirNow site {} returned no PM2.5 AQI in {scan_start}..={scan_end}",
                binding.aqsid
            )));
        }
        Ok(checkpoint)
    }

    async fn run_spc_loop(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            for binding in &self.bindings.tornado_regions {
                let instrument = DomainInstrumentKey::spc_tornado(&binding.region_id);
                let result = match self.tornado.as_ref() {
                    Some(source) => {
                        self.ingest_spc(source, &binding.region_id, &binding.spc_state_code)
                            .await
                    }
                    None => Err(QuantError::config("SPC adapter is unavailable")),
                };
                self.finish_cycle(DomainSourceId::spc_storm_reports(), instrument, result)
                    .await;
            }
            if wait_or_cancel(
                &shutdown,
                StdDuration::from_secs(self.tornado_config.spc_poll_secs.max(1)),
            )
            .await
            {
                return;
            }
        }
    }

    async fn ingest_spc(
        &self,
        source: &TornadoSource,
        region_id: &str,
        state_code: &str,
    ) -> QuantResult<DomainSourceCheckpoint> {
        let available_at = Utc::now();
        let today = available_at.date_naive();
        let yesterday = today
            .pred_opt()
            .ok_or_else(|| QuantError::config("SPC report date underflow"))?;
        let report = match source
            .spc_preliminary_day(region_id, state_code, today, available_at)
            .await?
        {
            Some(report) => report,
            None => source
                .spc_preliminary_day(region_id, state_code, yesterday, available_at)
                .await?
                .ok_or_else(|| QuantError::config("SPC current and prior partitions are absent"))?,
        };
        self.facts
            .persist_observations(vec![WeatherObservationCandidate {
                local_date: report.observed_at.date_naive(),
                report: report.clone(),
            }])
            .await?;
        Ok(DomainSourceCheckpoint::SpcTornado {
            report_window_end: report
                .valid_to
                .ok_or_else(|| QuantError::config("SPC report has no window end"))?,
            available_at: report.available_at,
            report_hash: report.report_hash,
        })
    }

    async fn run_ncei_loop(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            for binding in &self.bindings.tornado_regions {
                let instrument = DomainInstrumentKey::ncei_tornado(&binding.region_id);
                let result = match self.tornado.as_ref() {
                    Some(source) => {
                        self.ingest_ncei(
                            source,
                            &binding.region_id,
                            &binding.ncei_state_name,
                            &binding.timezone,
                        )
                        .await
                    }
                    None => Err(QuantError::config("NCEI adapter is unavailable")),
                };
                self.finish_cycle(DomainSourceId::ncei_storm_events(), instrument, result)
                    .await;
            }
            if wait_or_cancel(
                &shutdown,
                StdDuration::from_secs(self.tornado_config.ncei_refresh_secs.max(1)),
            )
            .await
            {
                return;
            }
        }
    }

    async fn ingest_ncei(
        &self,
        source: &TornadoSource,
        region_id: &str,
        state_name: &str,
        timezone: &str,
    ) -> QuantResult<DomainSourceCheckpoint> {
        let available_at = Utc::now();
        let timezone = parse_timezone(timezone)?;
        let report_date = available_at
            .with_timezone(&timezone)
            .date_naive()
            .checked_sub_days(Days::new(NCEI_FINALIZATION_LAG_DAYS))
            .ok_or_else(|| QuantError::config("NCEI finalized report date underflow"))?;
        let day = source
            .ncei_final_day(region_id, state_name, timezone, report_date, available_at)
            .await?;
        self.facts
            .persist_observations(vec![WeatherObservationCandidate {
                local_date: report_date,
                report: day.report.clone(),
            }])
            .await?;
        Ok(DomainSourceCheckpoint::NceiStormEvents {
            report_window_end: day
                .report
                .valid_to
                .ok_or_else(|| QuantError::config("NCEI finalized report has no window end"))?,
            collection_date: day.collection_date,
            file_hash: day.file_hash,
        })
    }

    async fn run_nhc_advisory_loop(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            if let Some(source) = self.nhc.as_ref()
                && let Err(error) = self.ingest_nhc_advisories(source).await
            {
                tracing::warn!(%error, "NHC advisory ingest failed");
            }
            if wait_or_cancel(
                &shutdown,
                StdDuration::from_secs(self.nhc_config.advisory_poll_secs.max(1)),
            )
            .await
            {
                return;
            }
        }
    }

    async fn ingest_nhc_advisories(&self, source: &NhcSource) -> QuantResult<()> {
        let reports = source.active_advisories(Utc::now()).await?;
        for report in reports {
            let instrument = report.instrument_key.clone();
            let source_id = report.source_id.clone();
            let result = async {
                self.facts
                    .persist_observations(vec![WeatherObservationCandidate {
                        local_date: report.observed_at.date_naive(),
                        report: report.clone(),
                    }])
                    .await?;
                let raw: Value = serde_json::from_str(&report.raw_report)
                    .map_err(|error| QuantError::config(format!("NHC provenance: {error}")))?;
                let advisory_number = raw
                    .get("advisory_number")
                    .and_then(Value::as_str)
                    .ok_or_else(|| QuantError::config("NHC provenance has no advisory number"))?
                    .to_owned();
                Ok(DomainSourceCheckpoint::NhcAdvisory {
                    issuance: report.valid_from.ok_or_else(|| {
                        QuantError::config("NHC advisory has no nominal issuance time")
                    })?,
                    storm_id: report.subject_key.clone(),
                    advisory_number,
                    report_hash: report.report_hash,
                })
            }
            .await;
            self.finish_cycle(source_id, instrument, result).await;
        }
        Ok(())
    }

    async fn run_nhc_best_track_loop(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            for binding in &self.bindings.nhc_historical_storms {
                let instrument =
                    DomainInstrumentKey::nhc_hurdat2(&binding.basin, &binding.storm_id);
                let result = match self.nhc.as_ref() {
                    Some(source) => {
                        self.ingest_nhc_best_track(source, &binding.basin, &binding.storm_id)
                            .await
                    }
                    None => Err(QuantError::config("HURDAT2 adapter is unavailable")),
                };
                self.finish_cycle(DomainSourceId::nhc_hurdat2(), instrument, result)
                    .await;
            }
            if wait_or_cancel(
                &shutdown,
                StdDuration::from_secs(self.nhc_config.best_track_refresh_secs.max(1)),
            )
            .await
            {
                return;
            }
        }
    }

    async fn ingest_nhc_best_track(
        &self,
        source: &NhcSource,
        basin: &str,
        storm_id: &str,
    ) -> QuantResult<DomainSourceCheckpoint> {
        let basin = parse_nhc_basin(basin)?;
        let track = source
            .hurdat2_storm(basin, storm_id, Utc::now())
            .await?
            .ok_or_else(|| QuantError::config(format!("HURDAT2 storm `{storm_id}` is absent")))?;
        let last_observation = track
            .reports
            .iter()
            .map(|report| report.observed_at)
            .max()
            .ok_or_else(|| QuantError::config("HURDAT2 storm has no observations"))?;
        self.facts
            .persist_observations(
                track
                    .reports
                    .into_iter()
                    .map(|report| WeatherObservationCandidate {
                        local_date: report.observed_at.date_naive(),
                        report,
                    })
                    .collect(),
            )
            .await?;
        Ok(DomainSourceCheckpoint::NhcHurdat2 {
            last_observation,
            collection_date: track.collection_date,
            file_hash: track.file_hash,
        })
    }

    async fn run_gistemp_loop(self: Arc<Self>, shutdown: CancellationToken) {
        let source_id = DomainSourceId::nasa_gistemp();
        let instrument = DomainInstrumentKey::nasa_gistemp_loti();
        loop {
            let result = match self.gistemp.as_ref() {
                Some(source) => self.ingest_gistemp(source).await,
                None => Err(QuantError::config("NASA GISTEMP adapter is unavailable")),
            };
            self.finish_cycle(source_id.clone(), instrument.clone(), result)
                .await;
            if wait_or_cancel(
                &shutdown,
                StdDuration::from_secs(self.gistemp_config.refresh_secs.max(1)),
            )
            .await
            {
                return;
            }
        }
    }

    async fn ingest_gistemp(
        &self,
        source: &NasaGistempSource,
    ) -> QuantResult<DomainSourceCheckpoint> {
        let available_at = Utc::now();
        let dataset = source.monthly_anomalies(available_at).await?;
        let last_month_end = dataset
            .reports
            .iter()
            .map(|report| report.observed_at)
            .max()
            .ok_or_else(|| QuantError::config("GISTEMP returned no monthly anomalies"))?;
        self.facts
            .persist_observations(
                dataset
                    .reports
                    .into_iter()
                    .map(|report| WeatherObservationCandidate {
                        local_date: report.observed_at.date_naive(),
                        report,
                    })
                    .collect(),
            )
            .await?;
        Ok(DomainSourceCheckpoint::NasaGistemp {
            last_month_end,
            available_at,
            file_hash: dataset.file_hash,
        })
    }

    async fn run_nsidc_loop(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            for hemisphere in [SeaIceHemisphere::North, SeaIceHemisphere::South] {
                let instrument = DomainInstrumentKey::nsidc_sea_ice_extent(hemisphere.as_str());
                let result = match self.nsidc.as_ref() {
                    Some(source) => self.ingest_nsidc(source, hemisphere).await,
                    None => Err(QuantError::config("NSIDC adapter is unavailable")),
                };
                self.finish_cycle(DomainSourceId::nsidc_sea_ice_index(), instrument, result)
                    .await;
            }
            if wait_or_cancel(
                &shutdown,
                StdDuration::from_secs(self.nsidc_config.refresh_secs.max(1)),
            )
            .await
            {
                return;
            }
        }
    }

    async fn ingest_nsidc(
        &self,
        source: &NsidcSeaIceSource,
        hemisphere: SeaIceHemisphere,
    ) -> QuantResult<DomainSourceCheckpoint> {
        let available_at = Utc::now();
        let dataset = source.daily_extent(hemisphere, available_at).await?;
        let last_day_end = dataset
            .reports
            .iter()
            .map(|report| report.observed_at)
            .max()
            .ok_or_else(|| QuantError::config("NSIDC returned no daily extents"))?;
        self.facts
            .persist_observations(
                dataset
                    .reports
                    .into_iter()
                    .map(|report| WeatherObservationCandidate {
                        local_date: report.observed_at.date_naive(),
                        report,
                    })
                    .collect(),
            )
            .await?;
        Ok(DomainSourceCheckpoint::NsidcSeaIce {
            last_day_end,
            available_at,
            file_hash: dataset.file_hash,
        })
    }

    async fn run_nws_loop(self: Arc<Self>, shutdown: CancellationToken) {
        loop {
            for binding in &self.bindings.nws_wind_stations {
                let station = match IcaoStation::parse(&binding.station) {
                    Ok(station) => station,
                    Err(error) => {
                        tracing::error!(%error, station = %binding.station, "invalid NWS binding");
                        continue;
                    }
                };
                let result = match self.nws.as_ref() {
                    Some(source) => self.ingest_nws(source, &station, &binding.timezone).await,
                    None => Err(QuantError::config("NWS adapter is unavailable")),
                };
                self.finish_cycle(
                    DomainSourceId::nws_observation(),
                    DomainInstrumentKey::nws_wind_gust(&station),
                    result,
                )
                .await;
            }
            if wait_or_cancel(
                &shutdown,
                StdDuration::from_secs(self.nws_config.poll_secs.max(1)),
            )
            .await
            {
                return;
            }
        }
    }

    async fn ingest_nws(
        &self,
        source: &NwsObservationSource,
        station: &IcaoStation,
        timezone: &str,
    ) -> QuantResult<DomainSourceCheckpoint> {
        let reports = source.recent_wind(station, Utc::now()).await?;
        let timezone = parse_timezone(timezone)?;
        self.facts
            .persist_observations(
                reports
                    .iter()
                    .cloned()
                    .map(|report| WeatherObservationCandidate {
                        local_date: report.observed_at.with_timezone(&timezone).date_naive(),
                        report,
                    })
                    .collect(),
            )
            .await?;
        let latest = reports
            .iter()
            .max_by_key(|report| (report.observed_at, &report.report_hash))
            .ok_or_else(|| {
                QuantError::config(format!(
                    "NWS returned no accepted wind observation for {station}"
                ))
            })?;
        Ok(DomainSourceCheckpoint::NwsObservation {
            observed_at: latest.observed_at,
            available_at: latest.available_at,
            report_hash: latest.report_hash,
        })
    }

    async fn finish_cycle(
        &self,
        source_id: DomainSourceId,
        instrument_key: DomainInstrumentKey,
        result: QuantResult<DomainSourceCheckpoint>,
    ) {
        if let Err(error) = self
            .complete_cycle(source_id.clone(), instrument_key.clone(), result)
            .await
        {
            tracing::warn!(%source_id, %instrument_key, %error, "Weather source cycle failed");
        }
    }

    async fn complete_evidence_cycle(
        &self,
        source_id: DomainSourceId,
        instrument_key: DomainInstrumentKey,
        result: QuantResult<DomainSourceCheckpoint>,
        failures: &mut Vec<String>,
    ) {
        if let Err(error) = self
            .complete_cycle(source_id.clone(), instrument_key.clone(), result)
            .await
        {
            failures.push(format!("{source_id}/{instrument_key}: {error}"));
        }
    }

    async fn complete_cycle(
        &self,
        source_id: DomainSourceId,
        instrument_key: DomainInstrumentKey,
        result: QuantResult<DomainSourceCheckpoint>,
    ) -> QuantResult<()> {
        match result {
            Ok(checkpoint) => {
                if let Err(error) = self
                    .upsert_cursor(source_id.clone(), instrument_key.clone(), checkpoint)
                    .await
                {
                    self.mark_failed(&source_id, &instrument_key, &error).await;
                    return Err(error);
                }
                self.source_supervisor
                    .mark_source_recovered(&source_id, &instrument_key)
                    .await?;
                Ok(())
            }
            Err(error) => {
                self.mark_failed(&source_id, &instrument_key, &error).await;
                Err(error)
            }
        }
    }

    async fn upsert_cursor(
        &self,
        source_id: DomainSourceId,
        instrument_key: DomainInstrumentKey,
        checkpoint: DomainSourceCheckpoint,
    ) -> QuantResult<()> {
        self.upsert_cursor_with_status(
            source_id,
            instrument_key,
            checkpoint,
            DomainCursorStatus::Live,
        )
        .await
    }

    async fn upsert_cursor_with_status(
        &self,
        source_id: DomainSourceId,
        instrument_key: DomainInstrumentKey,
        checkpoint: DomainSourceCheckpoint,
        status: DomainCursorStatus,
    ) -> QuantResult<()> {
        let checkpoint_hash = CanonicalDigest::content_hash_json(&checkpoint)?;
        self.cursors
            .upsert(UpsertDomainSourceCursor {
                source_id,
                instrument_key,
                checkpoint_json: checkpoint,
                checkpoint_hash,
                status,
                last_error: None,
                updated_at: Utc::now(),
            })
            .await?;
        Ok(())
    }

    async fn mark_failed(
        &self,
        source_id: &DomainSourceId,
        instrument_key: &DomainInstrumentKey,
        error: &QuantError,
    ) {
        let reason = bounded_error(error);
        if let Ok(Some(current)) = self.cursors.find(source_id, instrument_key).await {
            let update = UpsertDomainSourceCursor {
                source_id: source_id.clone(),
                instrument_key: instrument_key.clone(),
                checkpoint_json: current.checkpoint_json,
                checkpoint_hash: current.checkpoint_hash,
                status: DomainCursorStatus::Failed,
                last_error: Some(reason.clone()),
                updated_at: Utc::now(),
            };
            if let Err(cursor_error) = self.cursors.upsert(update).await {
                tracing::error!(%source_id, %instrument_key, %cursor_error, "Weather cursor failure status could not be persisted");
            }
        }
        if let Err(status_error) = self
            .source_supervisor
            .mark_source_failed(source_id, instrument_key, reason)
            .await
        {
            tracing::error!(%source_id, %instrument_key, %status_error, "Weather expectation failure status could not be persisted");
        }
    }
}

fn local_date(report: &WeatherObservationReport, timezone: &str) -> QuantResult<NaiveDate> {
    Ok(report
        .observed_at
        .with_timezone(&parse_timezone(timezone)?)
        .date_naive())
}

fn hko_latest_publishable_date(available_at: DateTime<Utc>) -> QuantResult<NaiveDate> {
    let local = available_at.with_timezone(&Hong_Kong);
    let published_today = local.hour() > 1 || (local.hour() == 1 && local.minute() >= 30);
    local
        .date_naive()
        .checked_sub_days(Days::new(if published_today { 1 } else { 2 }))
        .ok_or_else(|| QuantError::config("HKO latest publishable date underflow"))
}

fn hko_partition_date(target_date: NaiveDate, months_ago: u32) -> QuantResult<NaiveDate> {
    target_date
        .with_day(1)
        .and_then(|month| month.checked_sub_months(Months::new(months_ago)))
        .ok_or_else(|| QuantError::config("HKO daily-temperature partition underflow"))
}

fn parse_timezone(value: &str) -> QuantResult<Tz> {
    value
        .parse::<Tz>()
        .map_err(|error| QuantError::config(format!("invalid Weather timezone `{value}`: {error}")))
}

fn parse_nhc_basin(value: &str) -> QuantResult<NhcBasin> {
    match value {
        "atlantic" => Ok(NhcBasin::Atlantic),
        "eastern_pacific" => Ok(NhcBasin::EasternPacific),
        "central_pacific" => Ok(NhcBasin::CentralPacific),
        _ => Err(QuantError::config(format!(
            "unsupported NHC basin `{value}`"
        ))),
    }
}

fn truncate_hour(value: DateTime<Utc>) -> QuantResult<DateTime<Utc>> {
    value
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(|| QuantError::config("invalid UTC hour"))
}

fn airnow_scan_window(
    previous_scan: Option<DateTime<Utc>>,
    now_hour: DateTime<Utc>,
    correction_lookback_hours: u16,
) -> QuantResult<(DateTime<Utc>, DateTime<Utc>)> {
    let window_start = now_hour
        .checked_sub_signed(Duration::hours(i64::from(correction_lookback_hours)))
        .ok_or_else(|| QuantError::config("AirNow correction window underflow"))?;
    let scan_start = previous_scan
        .and_then(|hour| hour.checked_add_signed(Duration::hours(1)))
        .filter(|hour| *hour >= window_start && *hour <= now_hour)
        .unwrap_or(window_start);
    let scan_end = scan_start
        .checked_add_signed(Duration::hours(AIRNOW_CORRECTION_BATCH_HOURS - 1))
        .map_or(now_hour, |hour| hour.min(now_hour));
    Ok((scan_start, scan_end))
}

fn bounded_error(error: &QuantError) -> String {
    error.to_string().chars().take(ERROR_DETAIL_LIMIT).collect()
}

async fn wait_or_cancel(shutdown: &CancellationToken, duration: StdDuration) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => true,
        () = tokio::time::sleep(duration) => false,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, NaiveDate, TimeZone, Utc};

    use super::{airnow_scan_window, hko_latest_publishable_date, hko_partition_date};

    #[test]
    fn airnow_correction_scan_is_bounded_resumable_and_wraps() {
        let now = Utc.with_ymd_and_hms(2026, 7, 18, 12, 0, 0).unwrap();
        let (first_start, first_end) =
            airnow_scan_window(None, now, 72).expect("first correction batch");
        assert_eq!(first_start, now - Duration::hours(72));
        assert_eq!(first_end, first_start + Duration::hours(5));

        let (next_start, next_end) =
            airnow_scan_window(Some(first_end), now, 72).expect("next correction batch");
        assert_eq!(next_start, first_end + Duration::hours(1));
        assert_eq!(next_end, next_start + Duration::hours(5));

        let (wrapped_start, _) =
            airnow_scan_window(Some(now), now, 72).expect("wrapped correction batch");
        assert_eq!(wrapped_start, now - Duration::hours(72));
    }

    #[test]
    fn hko_daily_temperature_waits_for_documented_next_day_publication() {
        let before_publication = Utc.with_ymd_and_hms(2025, 7, 4, 17, 29, 0).unwrap();
        let at_publication = Utc.with_ymd_and_hms(2025, 7, 4, 17, 30, 0).unwrap();

        assert_eq!(
            hko_latest_publishable_date(before_publication).expect("date"),
            chrono::NaiveDate::from_ymd_opt(2025, 7, 3).expect("date")
        );
        assert_eq!(
            hko_latest_publishable_date(at_publication).expect("date"),
            chrono::NaiveDate::from_ymd_opt(2025, 7, 4).expect("date")
        );
    }

    #[test]
    fn hko_partition_scan_crosses_year_boundary_newest_first() {
        let target = NaiveDate::from_ymd_opt(2026, 1, 18).expect("date");
        assert_eq!(
            hko_partition_date(target, 0).expect("current partition"),
            chrono::NaiveDate::from_ymd_opt(2026, 1, 1).expect("date")
        );
        assert_eq!(
            hko_partition_date(target, 1).expect("previous partition"),
            chrono::NaiveDate::from_ymd_opt(2025, 12, 1).expect("date")
        );
    }
}
