//! NOAA SPC preliminary and NCEI final tornado adapters.

use std::{
    collections::BTreeMap,
    io::{Cursor, Read},
    sync::Arc,
    time::Duration,
};

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, Months, NaiveDate, NaiveDateTime, TimeZone, Utc,
};
use chrono_tz::Tz;
use csv::{ReaderBuilder, StringRecord};
use flate2::read::GzDecoder;
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::{TornadoRegionScopeConfig, TornadoSourceConfig},
    domain::data_plane::{WeatherObservationReport, WeatherObservationReportKind},
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, WeatherVariable,
    },
};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::infra::{
    http::{get_optional_bytes, get_text_with_retry},
    retry::RetryPolicy,
};

const SPC_HEADERS: [&str; 8] = [
    "Time", "F_Scale", "Location", "County", "State", "Lat", "Lon", "Comments",
];

/// One NCEI yearly-file projection for a closed region-local date range.
pub struct NceiTornadoPeriod {
    pub file_hash: ContentHash,
    pub collection_date: NaiveDate,
    pub reports: Vec<WeatherObservationReport>,
}

/// NCEI U.S. Tornadoes time-series partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NceiTornadoSeries {
    Month(u32),
    Annual,
}

/// One immutable download of an official national time-series partition.
pub struct NceiTornadoSeriesDataset {
    pub file_hash: ContentHash,
    pub reports: Vec<WeatherObservationReport>,
}

#[derive(Clone, Copy)]
struct NceiParseContext<'a> {
    region_id: &'a str,
    scope: &'a TornadoRegionScopeConfig,
    timezone: Tz,
    start_date: NaiveDate,
    end_date: NaiveDate,
    collection_date: NaiveDate,
    available_at: DateTime<Utc>,
}

/// Paired preliminary/final NOAA tornado source.
pub struct TornadoSource {
    config: TornadoSourceConfig,
    http: Client,
    retry_policy: RetryPolicy,
    compute: Arc<ComputeExecutor>,
}

impl TornadoSource {
    pub fn connect(
        config: TornadoSourceConfig,
        compute: Arc<ComputeExecutor>,
    ) -> QuantResult<Self> {
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .user_agent("quant-pivot/0.1 noaa-tornado-ingest")
            .build()
            .map_err(|error| ApiError::Sdk(format!("NOAA tornado HTTP client: {error}")))?;
        Ok(Self {
            config,
            http,
            retry_policy: RetryPolicy::gamma_default(),
            compute,
        })
    }

    #[must_use]
    pub fn with_http_client(mut self, http: Client) -> Self {
        self.http = http;
        self
    }

    /// Fetch one SPC 12Z-to-12Z preliminary report partition. A header-only
    /// published file is an explicit zero count; an unpublished 404 is absent.
    pub async fn spc_preliminary_day(
        &self,
        region_id: &str,
        scope: &TornadoRegionScopeConfig,
        report_date: NaiveDate,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Option<WeatherObservationReport>> {
        validate_region(region_id, scope)?;
        let url = format!(
            "{}/{}_rpts_torn.csv",
            self.config.spc_base_url.trim_end_matches('/'),
            report_date.format("%y%m%d")
        );
        let Some(bytes) = get_optional_bytes(&self.http, &self.retry_policy, &url)
            .await
            .map_err(QuantError::from)?
        else {
            return Ok(None);
        };
        let body = String::from_utf8(bytes)
            .map_err(|error| parse_error("SPC tornado CSV", format!("invalid UTF-8: {error}")))?;
        parse_spc_day(&body, region_id, scope, report_date, available_at).map(Some)
    }

    /// Discover the latest corrected NCEI yearly archive and project every
    /// official post-storm local day in the requested closed date range.
    ///
    /// Days without tornado records are emitted as explicit zero-count facts.
    /// The range must stay within one archive year so absence can be proven
    /// against exactly one official file revision.
    pub async fn ncei_final_period(
        &self,
        region_id: &str,
        scope: &TornadoRegionScopeConfig,
        timezone: Tz,
        start_date: NaiveDate,
        end_date: NaiveDate,
        available_at: DateTime<Utc>,
    ) -> QuantResult<NceiTornadoPeriod> {
        validate_region(region_id, scope)?;
        if start_date > end_date || start_date.year() != end_date.year() {
            return Err(parse_error(
                "NCEI Storm Events range",
                "date range must be non-empty and contained in one calendar year",
            )
            .into());
        }
        let index_url = format!("{}/", self.config.ncei_csv_base_url.trim_end_matches('/'));
        let index = get_text_with_retry(&self.http, &self.retry_policy, &index_url)
            .await
            .map_err(QuantError::from)?;
        let filename = latest_ncei_filename(&index, start_date.year())?;
        let collection_date = collection_date(&filename)?;
        let url = format!(
            "{}/{}",
            self.config.ncei_csv_base_url.trim_end_matches('/'),
            filename
        );
        let bytes = get_optional_bytes(&self.http, &self.retry_policy, &url)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| parse_error("NCEI Storm Events index", "indexed file returned 404"))?;
        let file_hash = CanonicalDigest::content_hash_bytes(&bytes);
        let region_id = region_id.to_owned();
        let scope = scope.clone();
        let memory = OfflineMemory::try_bytes(bytes.len().max(1024 * 1024 * 1024))?;
        let decoded = self
            .compute
            .run_offline(memory, move || {
                let context = NceiParseContext {
                    region_id: &region_id,
                    scope: &scope,
                    timezone,
                    start_date,
                    end_date,
                    collection_date,
                    available_at,
                };
                parse_ncei_gzip(bytes, &context)
            })
            .await?;
        Ok(NceiTornadoPeriod {
            file_hash,
            collection_date,
            reports: decoded,
        })
    }

    /// Fetch one official NCEI national monthly or annual tornado-count series.
    pub async fn ncei_time_series(
        &self,
        series: NceiTornadoSeries,
        timezone: Tz,
        available_at: DateTime<Utc>,
    ) -> QuantResult<NceiTornadoSeriesDataset> {
        let path = match series {
            NceiTornadoSeries::Month(month) if (1..=12).contains(&month) => {
                format!("1/{month}/data.json")
            }
            NceiTornadoSeries::Annual => "ytd/12/data.json".to_owned(),
            NceiTornadoSeries::Month(_) => {
                return Err(
                    parse_error("NCEI tornado time series", "month must be in 1..=12").into(),
                );
            }
        };
        let url = format!(
            "{}/{}",
            self.config.ncei_time_series_base_url.trim_end_matches('/'),
            path
        );
        let body = get_text_with_retry(&self.http, &self.retry_policy, &url)
            .await
            .map_err(QuantError::from)?;
        parse_ncei_series(&body, series, timezone, available_at)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NceiSeriesWire {
    description: NceiSeriesDescription,
    tornadoes: BTreeMap<String, u64>,
    fatalities: BTreeMap<String, u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NceiSeriesDescription {
    title: String,
}

#[derive(Serialize)]
struct CanonicalNceiSeries {
    source_product: &'static str,
    series: String,
    period_start: NaiveDate,
    period_end: NaiveDate,
    tornado_count: u64,
    source_title: String,
}

fn parse_ncei_series(
    body: &str,
    series: NceiTornadoSeries,
    timezone: Tz,
    available_at: DateTime<Utc>,
) -> QuantResult<NceiTornadoSeriesDataset> {
    let wire: NceiSeriesWire = serde_json::from_str(body)
        .map_err(|error| parse_error("NCEI tornado time series", error.to_string()))?;
    validate_series_title(&wire.description.title, series)?;
    if wire.tornadoes.keys().ne(wire.fatalities.keys()) {
        return Err(parse_error(
            "NCEI tornado time series",
            "tornado and fatality periods diverge",
        )
        .into());
    }
    let file_hash = CanonicalDigest::content_hash_bytes(body.as_bytes());
    let expected_month = match series {
        NceiTornadoSeries::Month(month) => month,
        NceiTornadoSeries::Annual => 12,
    };
    let mut reports = Vec::with_capacity(wire.tornadoes.len());
    for (period, count) in wire.tornadoes {
        let (year, month) = parse_series_period(&period)?;
        if month != expected_month {
            return Err(parse_error(
                "NCEI tornado time series",
                format!("period {period} is outside requested partition"),
            )
            .into());
        }
        let period_start = match series {
            NceiTornadoSeries::Month(_) => NaiveDate::from_ymd_opt(year, month, 1),
            NceiTornadoSeries::Annual => NaiveDate::from_ymd_opt(year, 1, 1),
        }
        .ok_or_else(|| parse_error("NCEI tornado time series", "invalid period start"))?;
        let period_end = match series {
            NceiTornadoSeries::Month(_) => period_start.checked_add_months(Months::new(1)),
            NceiTornadoSeries::Annual => NaiveDate::from_ymd_opt(
                year.checked_add(1)
                    .ok_or_else(|| parse_error("NCEI tornado time series", "year overflow"))?,
                1,
                1,
            ),
        }
        .ok_or_else(|| parse_error("NCEI tornado time series", "invalid period end"))?;
        let valid_from = local_midnight(period_start, timezone)?;
        let valid_to = local_midnight(period_end, timezone)?;
        let canonical = CanonicalNceiSeries {
            source_product: "NCEI_US_Tornadoes_Time_Series",
            series: match series {
                NceiTornadoSeries::Month(_) => format!("month_{month:02}"),
                NceiTornadoSeries::Annual => "annual".to_owned(),
            },
            period_start,
            period_end,
            tornado_count: count,
            source_title: wire.description.title.clone(),
        };
        let report_hash = CanonicalDigest::content_hash_json(&canonical)?;
        let raw_report = serde_json::to_string(&canonical)
            .map_err(|error| parse_error("NCEI tornado time series", error.to_string()))?;
        reports.push(WeatherObservationReport {
            source_id: DomainSourceId::ncei_tornado_time_series(),
            instrument_key: DomainInstrumentKey::ncei_tornado_time_series(),
            subject_key: "united_states".to_owned(),
            report_kind: WeatherObservationReportKind::NceiTornadoTimeSeries,
            variable: WeatherVariable::TornadoCount,
            value: Decimal::from(count),
            unit: DomainMeasurementUnit::Count,
            precision: Decimal::ONE,
            observed_at: valid_to,
            valid_from: Some(valid_from),
            valid_to: Some(valid_to),
            published_at: available_at,
            available_at,
            report_hash,
            raw_report,
        });
    }
    reports.sort_by_key(|report| report.observed_at);
    Ok(NceiTornadoSeriesDataset { file_hash, reports })
}

fn validate_series_title(title: &str, series: NceiTornadoSeries) -> QuantResult<()> {
    let expected = match series {
        NceiTornadoSeries::Month(month) => format!("{} U.S. Tornadoes", month_name(month)?),
        NceiTornadoSeries::Annual => "January-December U.S. Tornadoes".to_owned(),
    };
    if title != expected {
        return Err(parse_error(
            "NCEI tornado time series",
            format!("unexpected title `{title}`, expected `{expected}`"),
        )
        .into());
    }
    Ok(())
}

fn month_name(month: u32) -> QuantResult<&'static str> {
    let index = usize::try_from(month)
        .ok()
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| parse_error("NCEI tornado time series", "invalid month"))?;
    [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    .get(index)
    .copied()
    .ok_or_else(|| parse_error("NCEI tornado time series", "invalid month").into())
}

fn parse_series_period(value: &str) -> QuantResult<(i32, u32)> {
    if value.len() != 6 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(parse_error("NCEI tornado time series", "period must be YYYYMM").into());
    }
    let year = value[..4]
        .parse::<i32>()
        .map_err(|error| parse_error("NCEI tornado time series", error.to_string()))?;
    let month = value[4..]
        .parse::<u32>()
        .map_err(|error| parse_error("NCEI tornado time series", error.to_string()))?;
    Ok((year, month))
}

fn parse_spc_day(
    body: &str,
    region_id: &str,
    scope: &TornadoRegionScopeConfig,
    report_date: NaiveDate,
    available_at: DateTime<Utc>,
) -> QuantResult<WeatherObservationReport> {
    let window_start = report_date
        .and_hms_opt(12, 0, 0)
        .ok_or_else(|| parse_error("SPC tornado CSV", "invalid report date"))?
        .and_utc();
    let window_end = window_start + ChronoDuration::days(1);
    let mut reader = ReaderBuilder::new().from_reader(body.as_bytes());
    let headers = reader
        .headers()
        .map_err(|error| parse_error("SPC tornado CSV", error.to_string()))?
        .clone();
    if headers.iter().ne(SPC_HEADERS) {
        return Err(parse_error("SPC tornado CSV", "unexpected header contract").into());
    }
    let mut rows = Vec::new();
    let mut last_report_at = None::<DateTime<Utc>>;
    for row in reader.records() {
        let row = row.map_err(|error| parse_error("SPC tornado CSV", error.to_string()))?;
        if !spc_row_matches(scope, row.get(4)) {
            continue;
        }
        let reported_at = parse_spc_time(
            report_date,
            row.get(0)
                .ok_or_else(|| parse_error("SPC tornado CSV", "missing report time"))?,
        )?;
        if reported_at < window_start || reported_at >= window_end {
            return Err(parse_error("SPC tornado CSV", "report is outside 12Z partition").into());
        }
        last_report_at = Some(last_report_at.map_or(reported_at, |value| value.max(reported_at)));
        rows.push(row.iter().collect::<Vec<_>>().join(","));
    }
    rows.sort();
    let value = Decimal::from(rows.len());
    let observed_at = last_report_at.unwrap_or(window_start);
    if observed_at > available_at {
        return Err(
            parse_error("SPC tornado CSV", "report time is later than availability").into(),
        );
    }
    let report_hash = CanonicalDigest::content_hash_json(&(
        "spc_preliminary_tornado_v1",
        region_id,
        report_date,
        &rows,
    ))?;
    let raw_report = serde_json::to_string(&rows)
        .map_err(|error| parse_error("SPC tornado provenance", error.to_string()))?;
    Ok(WeatherObservationReport {
        source_id: DomainSourceId::spc_storm_reports(),
        instrument_key: DomainInstrumentKey::spc_tornado(region_id),
        subject_key: region_id.to_owned(),
        report_kind: WeatherObservationReportKind::SpcPreliminaryTornado,
        variable: WeatherVariable::TornadoCount,
        value,
        unit: DomainMeasurementUnit::Count,
        precision: Decimal::ONE,
        observed_at,
        valid_from: Some(window_start),
        valid_to: Some(window_end),
        published_at: available_at,
        available_at,
        report_hash,
        raw_report,
    })
}

fn parse_ncei_gzip(
    bytes: Vec<u8>,
    context: &NceiParseContext<'_>,
) -> QuantResult<Vec<WeatherObservationReport>> {
    let decoder = GzDecoder::new(Cursor::new(bytes));
    parse_ncei_reader(decoder, context)
}

fn parse_ncei_reader<R: Read>(
    reader: R,
    context: &NceiParseContext<'_>,
) -> QuantResult<Vec<WeatherObservationReport>> {
    let NceiParseContext {
        region_id,
        scope,
        timezone,
        start_date,
        end_date,
        collection_date,
        available_at,
    } = *context;
    let published_at = collection_date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| parse_error("NCEI Storm Events CSV", "invalid collection date"))?
        .and_utc();
    if published_at > available_at {
        return Err(parse_error(
            "NCEI Storm Events CSV",
            "collection is later than availability",
        )
        .into());
    }
    let final_window_end = local_midnight(
        end_date
            .succ_opt()
            .ok_or_else(|| parse_error("NCEI Storm Events CSV", "end date overflow"))?,
        timezone,
    )?;
    if published_at < final_window_end {
        return Err(parse_error(
            "NCEI Storm Events CSV",
            "collection predates the requested completed date range",
        )
        .into());
    }
    let mut reader = ReaderBuilder::new().from_reader(reader);
    let headers = reader
        .headers()
        .map_err(|error| parse_error("NCEI Storm Events CSV", error.to_string()))?
        .clone();
    let begin = column(&headers, "BEGIN_DATE_TIME")?;
    let event_id = column(&headers, "EVENT_ID")?;
    let state = column(&headers, "STATE")?;
    let event_type = column(&headers, "EVENT_TYPE")?;
    let mut events = BTreeMap::<NaiveDate, BTreeMap<String, String>>::new();
    for row in reader.records() {
        let row = row.map_err(|error| parse_error("NCEI Storm Events CSV", error.to_string()))?;
        if !ncei_row_matches(scope, row.get(state)) || row.get(event_type) != Some("Tornado") {
            continue;
        }
        let begin_at = parse_ncei_local(
            row.get(begin)
                .ok_or_else(|| parse_error("NCEI Storm Events CSV", "missing begin time"))?,
            timezone,
        )?;
        let report_date = begin_at.with_timezone(&timezone).date_naive();
        if report_date < start_date || report_date > end_date {
            continue;
        }
        let id = row
            .get(event_id)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| parse_error("NCEI Storm Events CSV", "missing event id"))?;
        let raw_row = row.iter().collect::<Vec<_>>().join(",");
        if let Some(existing) = events
            .entry(report_date)
            .or_default()
            .insert(id.to_owned(), raw_row.clone())
            && existing != raw_row
        {
            return Err(parse_error(
                "NCEI Storm Events CSV",
                format!("event {id} has divergent duplicate rows"),
            )
            .into());
        }
    }
    let mut reports = Vec::new();
    let mut report_date = start_date;
    loop {
        let day_events = events.remove(&report_date).unwrap_or_default();
        let rows = day_events.into_values().collect::<Vec<_>>();
        let window_start = local_midnight(report_date, timezone)?;
        let next_date = report_date
            .succ_opt()
            .ok_or_else(|| parse_error("NCEI Storm Events CSV", "report date overflow"))?;
        let window_end = local_midnight(next_date, timezone)?;
        let report_hash = CanonicalDigest::content_hash_json(&(
            "ncei_final_tornado_v2",
            region_id,
            report_date,
            collection_date,
            &rows,
        ))?;
        let raw_report = serde_json::to_string(&rows)
            .map_err(|error| parse_error("NCEI tornado provenance", error.to_string()))?;
        reports.push(WeatherObservationReport {
            source_id: DomainSourceId::ncei_storm_events(),
            instrument_key: DomainInstrumentKey::ncei_tornado(region_id),
            subject_key: region_id.to_owned(),
            report_kind: WeatherObservationReportKind::NceiFinalTornado,
            variable: WeatherVariable::TornadoCount,
            value: Decimal::from(rows.len()),
            unit: DomainMeasurementUnit::Count,
            precision: Decimal::ONE,
            observed_at: window_end,
            valid_from: Some(window_start),
            valid_to: Some(window_end),
            published_at,
            available_at,
            report_hash,
            raw_report,
        });
        if report_date == end_date {
            break;
        }
        report_date = next_date;
    }
    Ok(reports)
}

fn latest_ncei_filename(index: &str, year: i32) -> QuantResult<String> {
    let prefix = format!("StormEvents_details-ftp_v1.0_d{year}_c");
    index
        .split(['\"', '\'', '<', '>'])
        .filter(|value| value.starts_with(&prefix) && value.ends_with(".csv.gz"))
        .max()
        .map(str::to_owned)
        .ok_or_else(|| {
            parse_error("NCEI Storm Events index", format!("missing year {year}")).into()
        })
}

fn collection_date(filename: &str) -> QuantResult<NaiveDate> {
    let value = filename
        .rsplit_once("_c")
        .and_then(|(_, suffix)| suffix.strip_suffix(".csv.gz"))
        .ok_or_else(|| parse_error("NCEI Storm Events filename", "missing collection date"))?;
    NaiveDate::parse_from_str(value, "%Y%m%d")
        .map_err(|error| parse_error("NCEI Storm Events filename", error.to_string()).into())
}

fn parse_spc_time(report_date: NaiveDate, value: &str) -> QuantResult<DateTime<Utc>> {
    if value.len() != 4 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(parse_error("SPC tornado CSV", "time must be HHMM").into());
    }
    let hour = value[..2]
        .parse::<u32>()
        .map_err(|error| parse_error("SPC tornado CSV", error.to_string()))?;
    let minute = value[2..]
        .parse::<u32>()
        .map_err(|error| parse_error("SPC tornado CSV", error.to_string()))?;
    let date = if hour < 12 {
        report_date
            .succ_opt()
            .ok_or_else(|| parse_error("SPC tornado CSV", "report date overflow"))?
    } else {
        report_date
    };
    date.and_hms_opt(hour, minute, 0)
        .map(|value| value.and_utc())
        .ok_or_else(|| parse_error("SPC tornado CSV", "invalid HHMM").into())
}

fn parse_ncei_local(value: &str, timezone: Tz) -> QuantResult<DateTime<Utc>> {
    let local = NaiveDateTime::parse_from_str(value, "%d-%b-%y %H:%M:%S")
        .map_err(|error| parse_error("NCEI Storm Events CSV", error.to_string()))?;
    timezone
        .from_local_datetime(&local)
        .single()
        .map(|value| value.to_utc())
        .ok_or_else(|| parse_error("NCEI Storm Events CSV", "ambiguous local event time").into())
}

fn local_midnight(date: NaiveDate, timezone: Tz) -> QuantResult<DateTime<Utc>> {
    let local = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| parse_error("NCEI Storm Events CSV", "invalid local midnight"))?;
    timezone
        .from_local_datetime(&local)
        .single()
        .map(|value| value.to_utc())
        .ok_or_else(|| parse_error("NCEI Storm Events CSV", "ambiguous local midnight").into())
}

fn column(headers: &StringRecord, name: &str) -> QuantResult<usize> {
    headers
        .iter()
        .position(|value| value == name)
        .ok_or_else(|| parse_error("NCEI Storm Events CSV", format!("missing {name}")).into())
}

fn validate_region(region_id: &str, scope: &TornadoRegionScopeConfig) -> QuantResult<()> {
    let valid = |value: &str| {
        !value.trim().is_empty()
            && value.len() <= 128
            && !value.contains(':')
            && !value.chars().any(char::is_control)
    };
    let scope_valid = match scope {
        TornadoRegionScopeConfig::UnitedStates => region_id == "united_states",
        TornadoRegionScopeConfig::State {
            spc_state_code,
            ncei_state_name,
        } => {
            spc_state_code.len() == 2
                && spc_state_code.bytes().all(|byte| byte.is_ascii_uppercase())
                && valid(ncei_state_name)
        }
    };
    if !valid(region_id) || !scope_valid {
        return Err(parse_error("tornado binding", "invalid region binding").into());
    }
    Ok(())
}

fn spc_row_matches(scope: &TornadoRegionScopeConfig, state: Option<&str>) -> bool {
    match scope {
        TornadoRegionScopeConfig::UnitedStates => state.is_some_and(|value| {
            value.len() == 2 && value.bytes().all(|byte| byte.is_ascii_uppercase())
        }),
        TornadoRegionScopeConfig::State { spc_state_code, .. } => {
            state == Some(spc_state_code.as_str())
        }
    }
}

fn ncei_row_matches(scope: &TornadoRegionScopeConfig, state: Option<&str>) -> bool {
    match scope {
        TornadoRegionScopeConfig::UnitedStates => state.is_some_and(|value| !value.is_empty()),
        TornadoRegionScopeConfig::State {
            ncei_state_name, ..
        } => state == Some(ncei_state_name.as_str()),
    }
}

fn parse_error(context: &str, detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: context.to_owned(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Write, sync::Arc};

    use chrono::{NaiveDate, TimeZone, Utc};
    use chrono_tz::America::{Chicago, New_York};
    use flate2::{Compression, write::GzEncoder};
    use quant_pivot_compute::ComputeExecutor;
    use quant_pivot_models::{
        config::{TornadoRegionScopeConfig, TornadoSourceConfig},
        domain::data_plane::WeatherObservationReportKind,
    };
    use rust_decimal_macros::dec;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers, matchers::method};

    use super::{NceiTornadoSeries, TornadoSource};

    fn test_compute() -> Arc<ComputeExecutor> {
        Arc::new(ComputeExecutor::new().expect("test compute executor"))
    }

    #[tokio::test]
    async fn spc_partition_counts_reports() {
        let server = MockServer::start().await;
        let body = concat!(
            "Time,F_Scale,Location,County,State,Lat,Lon,Comments\n",
            "2359,UNK,Town A,County A,OK,35.1,-97.1,report\n",
            "0030,EF1,Town B,County B,OK,35.2,-97.2,next UTC day\n",
            "0100,UNK,Town C,County C,TX,31.0,-99.0,other state\n",
        );
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = TornadoSource::connect(
            TornadoSourceConfig {
                spc_base_url: server.uri(),
                ..TornadoSourceConfig::default()
            },
            test_compute(),
        )
        .expect("source");
        let report = source
            .spc_preliminary_day(
                "oklahoma",
                &TornadoRegionScopeConfig::State {
                    spc_state_code: "OK".to_owned(),
                    ncei_state_name: "OKLAHOMA".to_owned(),
                },
                NaiveDate::from_ymd_opt(2026, 7, 17).expect("date"),
                Utc.with_ymd_and_hms(2026, 7, 18, 2, 0, 0).unwrap(),
            )
            .await
            .expect("SPC")
            .expect("published");
        assert_eq!(report.value, dec!(2));
        assert_eq!(
            report.report_kind,
            WeatherObservationReportKind::SpcPreliminaryTornado
        );
    }

    #[tokio::test]
    async fn ncei_period_includes_zero() {
        let server = MockServer::start().await;
        let index = concat!(
            "<a href=\"StormEvents_details-ftp_v1.0_d2025_c20260201.csv.gz\">old</a>",
            "<a href=\"StormEvents_details-ftp_v1.0_d2025_c20260323.csv.gz\">new</a>",
        );
        let csv = concat!(
            "BEGIN_DATE_TIME,EVENT_ID,STATE,EVENT_TYPE\n",
            "01-JUL-25 13:00:00,101,OKLAHOMA,Tornado\n",
            "01-JUL-25 14:00:00,102,OKLAHOMA,Tornado\n",
            "01-JUL-25 14:00:00,102,OKLAHOMA,Tornado\n",
            "01-JUL-25 15:00:00,103,TEXAS,Tornado\n",
        );
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(csv.as_bytes()).expect("gzip write");
        let gzip = encoder.finish().expect("gzip finish");
        Mock::given(method("GET"))
            .and(matchers::path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(index))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(matchers::path(
                "/StormEvents_details-ftp_v1.0_d2025_c20260323.csv.gz",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(gzip))
            .mount(&server)
            .await;
        let source = TornadoSource::connect(
            TornadoSourceConfig {
                ncei_csv_base_url: server.uri(),
                ..TornadoSourceConfig::default()
            },
            test_compute(),
        )
        .expect("source");
        let period = source
            .ncei_final_period(
                "oklahoma",
                &TornadoRegionScopeConfig::State {
                    spc_state_code: "OK".to_owned(),
                    ncei_state_name: "OKLAHOMA".to_owned(),
                },
                Chicago,
                NaiveDate::from_ymd_opt(2025, 7, 1).expect("date"),
                NaiveDate::from_ymd_opt(2025, 7, 2).expect("date"),
                Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap(),
            )
            .await
            .expect("NCEI");
        assert_eq!(
            period.collection_date,
            chrono::NaiveDate::from_ymd_opt(2026, 3, 23).expect("date")
        );
        assert_eq!(period.reports.len(), 2);
        assert_eq!(period.reports[0].value, dec!(2));
        assert_eq!(period.reports[1].value, dec!(0));
        assert_eq!(
            period.reports[0].report_kind,
            WeatherObservationReportKind::NceiFinalTornado
        );
        assert_eq!(period.reports[0].valid_to, period.reports[1].valid_from);
    }

    #[tokio::test]
    async fn national_scope_counts_states() {
        let server = MockServer::start().await;
        let body = concat!(
            "Time,F_Scale,Location,County,State,Lat,Lon,Comments\n",
            "2359,UNK,Town A,County A,OK,35.1,-97.1,report\n",
            "0030,EF1,Town B,County B,TX,31.0,-99.0,next UTC day\n",
        );
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = TornadoSource::connect(
            TornadoSourceConfig {
                spc_base_url: server.uri(),
                ..TornadoSourceConfig::default()
            },
            test_compute(),
        )
        .expect("source");
        let report = source
            .spc_preliminary_day(
                "united_states",
                &TornadoRegionScopeConfig::UnitedStates,
                NaiveDate::from_ymd_opt(2026, 7, 17).expect("date"),
                Utc.with_ymd_and_hms(2026, 7, 18, 2, 0, 0).unwrap(),
            )
            .await
            .expect("SPC")
            .expect("published");
        assert_eq!(report.value, dec!(2));
    }

    #[tokio::test]
    async fn parses_national_time_series() {
        let server = MockServer::start().await;
        let body = concat!(
            "{\"description\":{\"title\":\"July U.S. Tornadoes\"},",
            "\"tornadoes\":{\"202507\":90,\"202607\":123},",
            "\"fatalities\":{\"202507\":0,\"202607\":2}}",
        );
        Mock::given(method("GET"))
            .and(matchers::path("/1/7/data.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = TornadoSource::connect(
            TornadoSourceConfig {
                ncei_time_series_base_url: server.uri(),
                ..TornadoSourceConfig::default()
            },
            test_compute(),
        )
        .expect("source");
        let dataset = source
            .ncei_time_series(
                NceiTornadoSeries::Month(7),
                New_York,
                Utc.with_ymd_and_hms(2026, 8, 10, 15, 0, 1).unwrap(),
            )
            .await
            .expect("series");
        assert_eq!(dataset.reports.len(), 2);
        assert_eq!(dataset.reports[1].value, dec!(123));
        assert_eq!(
            dataset.reports[1].report_kind,
            WeatherObservationReportKind::NceiTornadoTimeSeries
        );
        assert_eq!(
            dataset.reports[1].valid_from,
            Some(Utc.with_ymd_and_hms(2026, 7, 1, 4, 0, 0).unwrap())
        );
    }
}
