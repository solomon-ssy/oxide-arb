//! NOAA SPC preliminary and NCEI final tornado adapters.

use std::{
    collections::BTreeSet,
    io::{Cursor, Read},
    sync::Arc,
    time::Duration,
};

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, NaiveDate, NaiveDateTime, TimeZone, Utc,
};
use chrono_tz::Tz;
use csv::{ReaderBuilder, StringRecord};
use flate2::read::GzDecoder;
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::TornadoSourceConfig,
    domain::data_plane::{WeatherObservationReport, WeatherObservationReportKind},
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, WeatherVariable,
    },
};
use reqwest::Client;
use rust_decimal::Decimal;

use crate::infra::{
    http::{get_optional_bytes, get_text_with_retry},
    retry::RetryPolicy,
};

const SPC_HEADERS: [&str; 8] = [
    "Time", "F_Scale", "Location", "County", "State", "Lat", "Lon", "Comments",
];

/// One NCEI yearly-file projection for a region-local calendar day.
pub struct NceiTornadoDay {
    pub file_hash: ContentHash,
    pub collection_date: NaiveDate,
    pub report: WeatherObservationReport,
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
        state_code: &str,
        report_date: NaiveDate,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Option<WeatherObservationReport>> {
        validate_region(region_id, state_code)?;
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
        parse_spc_day(&body, region_id, state_code, report_date, available_at).map(Some)
    }

    /// Discover the latest corrected NCEI yearly archive and project the
    /// official post-storm tornado events for one local calendar day.
    pub async fn ncei_final_day(
        &self,
        region_id: &str,
        state_name: &str,
        timezone: Tz,
        report_date: NaiveDate,
        available_at: DateTime<Utc>,
    ) -> QuantResult<NceiTornadoDay> {
        validate_region_name(region_id, state_name)?;
        let index_url = format!("{}/", self.config.ncei_csv_base_url.trim_end_matches('/'));
        let index = get_text_with_retry(&self.http, &self.retry_policy, &index_url)
            .await
            .map_err(QuantError::from)?;
        let filename = latest_ncei_filename(&index, report_date.year())?;
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
        let state_name = state_name.to_owned();
        let memory = OfflineMemory::try_bytes(bytes.len().max(1024 * 1024 * 1024))?;
        let decoded = self
            .compute
            .run_offline(memory, move || {
                parse_ncei_gzip(
                    bytes,
                    &region_id,
                    &state_name,
                    timezone,
                    report_date,
                    collection_date,
                    available_at,
                )
            })
            .await?;
        Ok(NceiTornadoDay {
            file_hash,
            collection_date,
            report: decoded,
        })
    }
}

fn parse_spc_day(
    body: &str,
    region_id: &str,
    state_code: &str,
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
        if row.get(4) != Some(state_code) {
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
    region_id: &str,
    state_name: &str,
    timezone: Tz,
    report_date: NaiveDate,
    collection_date: NaiveDate,
    available_at: DateTime<Utc>,
) -> QuantResult<WeatherObservationReport> {
    let decoder = GzDecoder::new(Cursor::new(bytes));
    parse_ncei_reader(
        decoder,
        region_id,
        state_name,
        timezone,
        report_date,
        collection_date,
        available_at,
    )
}

fn parse_ncei_reader<R: Read>(
    reader: R,
    region_id: &str,
    state_name: &str,
    timezone: Tz,
    report_date: NaiveDate,
    collection_date: NaiveDate,
    available_at: DateTime<Utc>,
) -> QuantResult<WeatherObservationReport> {
    let mut reader = ReaderBuilder::new().from_reader(reader);
    let headers = reader
        .headers()
        .map_err(|error| parse_error("NCEI Storm Events CSV", error.to_string()))?
        .clone();
    let begin = column(&headers, "BEGIN_DATE_TIME")?;
    let event_id = column(&headers, "EVENT_ID")?;
    let state = column(&headers, "STATE")?;
    let event_type = column(&headers, "EVENT_TYPE")?;
    let mut ids = BTreeSet::new();
    let mut rows = Vec::new();
    for row in reader.records() {
        let row = row.map_err(|error| parse_error("NCEI Storm Events CSV", error.to_string()))?;
        if row.get(state) != Some(state_name) || row.get(event_type) != Some("Tornado") {
            continue;
        }
        let begin_at = parse_ncei_local(
            row.get(begin)
                .ok_or_else(|| parse_error("NCEI Storm Events CSV", "missing begin time"))?,
            timezone,
        )?;
        if begin_at.with_timezone(&timezone).date_naive() != report_date {
            continue;
        }
        let id = row
            .get(event_id)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| parse_error("NCEI Storm Events CSV", "missing event id"))?;
        if ids.insert(id.to_owned()) {
            rows.push(row.iter().collect::<Vec<_>>().join(","));
        }
    }
    rows.sort();
    let window_start = local_midnight(report_date, timezone)?;
    let next_date = report_date
        .succ_opt()
        .ok_or_else(|| parse_error("NCEI Storm Events CSV", "report date overflow"))?;
    let window_end = local_midnight(next_date, timezone)?;
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
    let report_hash = CanonicalDigest::content_hash_json(&(
        "ncei_final_tornado_v1",
        region_id,
        report_date,
        collection_date,
        &rows,
    ))?;
    let raw_report = serde_json::to_string(&rows)
        .map_err(|error| parse_error("NCEI tornado provenance", error.to_string()))?;
    Ok(WeatherObservationReport {
        source_id: DomainSourceId::ncei_storm_events(),
        instrument_key: DomainInstrumentKey::ncei_tornado(region_id),
        subject_key: region_id.to_owned(),
        report_kind: WeatherObservationReportKind::NceiFinalTornado,
        variable: WeatherVariable::TornadoCount,
        value: Decimal::from(ids.len()),
        unit: DomainMeasurementUnit::Count,
        precision: Decimal::ONE,
        observed_at: window_end,
        valid_from: Some(window_start),
        valid_to: Some(window_end),
        published_at,
        available_at,
        report_hash,
        raw_report,
    })
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

fn validate_region(region_id: &str, state_code: &str) -> QuantResult<()> {
    validate_region_name(region_id, state_code)?;
    if state_code.len() != 2 || !state_code.bytes().all(|byte| byte.is_ascii_uppercase()) {
        return Err(
            parse_error("tornado binding", "SPC state must be two uppercase letters").into(),
        );
    }
    Ok(())
}

fn validate_region_name(region_id: &str, name: &str) -> QuantResult<()> {
    let valid = |value: &str| {
        !value.trim().is_empty()
            && value.len() <= 128
            && !value.contains(':')
            && !value.chars().any(char::is_control)
    };
    if !valid(region_id) || !valid(name) {
        return Err(parse_error("tornado binding", "invalid region binding").into());
    }
    Ok(())
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
    use chrono_tz::America::Chicago;
    use flate2::{Compression, write::GzEncoder};
    use quant_pivot_compute::ComputeExecutor;
    use quant_pivot_models::{
        config::TornadoSourceConfig, domain::data_plane::WeatherObservationReportKind,
    };
    use rust_decimal_macros::dec;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers, matchers::method};

    use super::TornadoSource;

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
                "OK",
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
    async fn ncei_latest_unique_events() {
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
        let day = source
            .ncei_final_day(
                "oklahoma",
                "OKLAHOMA",
                Chicago,
                NaiveDate::from_ymd_opt(2025, 7, 1).expect("date"),
                Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap(),
            )
            .await
            .expect("NCEI");
        assert_eq!(
            day.collection_date,
            chrono::NaiveDate::from_ymd_opt(2026, 3, 23).expect("date")
        );
        assert_eq!(day.report.value, dec!(2));
        assert_eq!(
            day.report.report_kind,
            WeatherObservationReportKind::NceiFinalTornado
        );
    }
}
