//! Hong Kong Observatory Open Data rainfall adapter.

use std::{collections::BTreeSet, fmt::Display, str::FromStr, time::Duration};

use chrono::{DateTime, Datelike, Days, NaiveDate, TimeDelta, TimeZone, Utc};
use chrono_tz::Asia::Hong_Kong;
use csv::{ReaderBuilder, StringRecord, StringRecordsIter};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::HkoOpenDataSourceConfig,
    domain::data_plane::{WeatherObservationReport, WeatherObservationReportKind},
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, HkoStation,
        WeatherTemperatureStatistic, WeatherVariable,
    },
};
use reqwest::{Client, Url};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::infra::{http::get_text_with_retry, retry::RetryPolicy};

#[derive(Serialize)]
struct CanonicalDailyRainfallReport<'a> {
    source_product: &'static str,
    station_key: &'a str,
    site_key: &'a str,
    local_date: NaiveDate,
    total_millimeters: Decimal,
    completeness: &'a str,
    publication_time_basis: &'static str,
}

#[derive(Debug, Deserialize)]
struct DailyTemperatureWire {
    #[serde(rename = "type")]
    dataset_type: Vec<String>,
    fields: Vec<String>,
    data: Vec<Vec<String>>,
    legend: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CanonicalDailyTemperatureReport<'a> {
    source_product: &'static str,
    station: &'a str,
    statistic: &'static str,
    local_date: NaiveDate,
    value_celsius: Decimal,
    completeness: &'a str,
    documented_earliest_publication_at: DateTime<Utc>,
    publication_time_basis: &'static str,
}

/// One validated HKO daily-temperature month response. Incomplete and
/// unavailable rows are counted but deliberately excluded from serving facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HkoDailyTemperatureMonth {
    pub reports: Vec<WeatherObservationReport>,
    pub response_hash: ContentHash,
    pub incomplete_rows: usize,
    pub unavailable_rows: usize,
}

/// Exact request for one official HKO completed daily-rainfall file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HkoDailyRainfallRequest {
    pub station_key: String,
    pub site_key: String,
    pub csv_url: String,
    pub minimum_date: NaiveDate,
    pub available_at: DateTime<Utc>,
}

impl HkoDailyRainfallRequest {
    fn validate(&self) -> QuantResult<()> {
        let printable = |value: &str| {
            !value.trim().is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
        };
        let url_valid = Url::parse(&self.csv_url).ok().is_some_and(|url| {
            matches!(url.scheme(), "http" | "https") && url.host_str().is_some()
        });
        if !printable(&self.station_key)
            || !printable(&self.site_key)
            || !url_valid
            || self.minimum_date.year() < 1884
        {
            return Err(daily_rainfall_error("invalid HKO daily rainfall request").into());
        }
        Ok(())
    }
}

/// One immutable view of an HKO completed daily-rainfall file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HkoDailyRainfallDataset {
    pub reports: Vec<WeatherObservationReport>,
    pub response_hash: ContentHash,
    pub incomplete_rows: usize,
    pub unavailable_rows: usize,
    pub trace_rows: usize,
}

/// Public HKO climate-data client.
pub struct HkoOpenDataSource {
    config: HkoOpenDataSourceConfig,
    http: Client,
    retry_policy: RetryPolicy,
}

impl HkoOpenDataSource {
    pub fn connect(config: HkoOpenDataSourceConfig) -> QuantResult<Self> {
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .user_agent("quant-pivot/0.1 hko-open-data-ingest")
            .build()
            .map_err(|error| ApiError::Sdk(format!("HKO HTTP client: {error}")))?;
        Ok(Self {
            config,
            http,
            retry_policy: RetryPolicy::gamma_default(),
        })
    }

    #[must_use]
    pub fn with_http_client(mut self, http: Client) -> Self {
        self.http = http;
        self
    }

    /// Fetch completed local-day rainfall totals from an exact station file.
    pub async fn daily_rainfall(
        &self,
        request: &HkoDailyRainfallRequest,
    ) -> QuantResult<HkoDailyRainfallDataset> {
        request.validate()?;
        let body = get_text_with_retry(&self.http, &self.retry_policy, &request.csv_url)
            .await
            .map_err(QuantError::from)?;
        parse_daily_rainfall(&body, request)
    }

    /// Fetch one official HKO station/month daily maximum or minimum series.
    ///
    /// The payload has no row-level publication timestamp. `published_at` is
    /// therefore conservatively set to this process's first observed
    /// `available_at`; it is never backdated to the documented 01:30 HKT
    /// earliest-availability boundary.
    pub async fn daily_temperatures(
        &self,
        station: &HkoStation,
        statistic: WeatherTemperatureStatistic,
        year: i32,
        month: u32,
        available_at: DateTime<Utc>,
    ) -> QuantResult<HkoDailyTemperatureMonth> {
        validate_daily_temperature_partition(year, month, available_at)?;
        let data_type = match statistic {
            WeatherTemperatureStatistic::Maximum => "CLMMAXT",
            WeatherTemperatureStatistic::Minimum => "CLMMINT",
        };
        let url = format!(
            "{}/opendata.php?dataType={data_type}&lang=en&rformat=json&station={station}&year={year}&month={month}",
            self.config.base_url.trim_end_matches('/')
        );
        let body = get_text_with_retry(&self.http, &self.retry_policy, &url)
            .await
            .map_err(QuantError::from)?;
        parse_daily_temperatures(&body, station, statistic, year, month, available_at)
    }
}

fn parse_daily_temperatures(
    body: &str,
    station: &HkoStation,
    statistic: WeatherTemperatureStatistic,
    year: i32,
    month: u32,
    available_at: DateTime<Utc>,
) -> QuantResult<HkoDailyTemperatureMonth> {
    let wire: DailyTemperatureWire =
        serde_json::from_str(body).map_err(|error| daily_temperature_error(error.to_string()))?;
    validate_daily_temperature_schema(&wire, station, statistic)?;
    let response_hash = CanonicalDigest::content_hash_bytes(body.as_bytes());
    let mut dates = BTreeSet::new();
    let mut reports = Vec::new();
    let mut incomplete_rows = 0_usize;
    let mut unavailable_rows = 0_usize;
    for row in wire.data {
        if row.len() != 5 {
            return Err(daily_temperature_error(format!(
                "daily temperature row has {} fields, expected 5",
                row.len()
            ))
            .into());
        }
        let row_year = parse_i32(&row[0], "year")?;
        let row_month = parse_u32(&row[1], "month")?;
        let row_day = parse_u32(&row[2], "day")?;
        if row_year != year || row_month != month {
            return Err(daily_temperature_error(format!(
                "response row {row_year}-{row_month:02}-{row_day:02} is outside requested partition {year}-{month:02}"
            ))
            .into());
        }
        let local_date = NaiveDate::from_ymd_opt(row_year, row_month, row_day)
            .ok_or_else(|| daily_temperature_error("daily temperature row has invalid date"))?;
        if !dates.insert(local_date) {
            return Err(daily_temperature_error(format!(
                "daily temperature response repeats {local_date}"
            ))
            .into());
        }
        match row[4].as_str() {
            "C" => {}
            "#" => {
                incomplete_rows += 1;
                continue;
            }
            _ if row[3] == "***" => {
                unavailable_rows += 1;
                continue;
            }
            completeness => {
                return Err(daily_temperature_error(format!(
                    "unsupported data completeness `{completeness}`"
                ))
                .into());
            }
        }
        let value = row[3]
            .parse::<Decimal>()
            .map_err(|error| daily_temperature_error(format!("temperature value: {error}")))?;
        let day_start = Hong_Kong
            .from_local_datetime(
                &local_date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| daily_temperature_error("local-day start is invalid"))?,
            )
            .single()
            .ok_or_else(|| daily_temperature_error("HKO local-day start is ambiguous"))?
            .with_timezone(&Utc);
        let next_date = local_date
            .checked_add_days(Days::new(1))
            .ok_or_else(|| daily_temperature_error("HKO local date overflow"))?;
        let day_end = Hong_Kong
            .from_local_datetime(
                &next_date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| daily_temperature_error("local-day end is invalid"))?,
            )
            .single()
            .ok_or_else(|| daily_temperature_error("HKO local-day end is ambiguous"))?
            .with_timezone(&Utc);
        let documented_earliest_publication_at = day_end
            .checked_add_signed(TimeDelta::minutes(90))
            .ok_or_else(|| daily_temperature_error("publication boundary overflow"))?;
        if available_at < documented_earliest_publication_at {
            return Err(daily_temperature_error(format!(
                "complete row for {local_date} appeared before documented 01:30 HKT availability"
            ))
            .into());
        }
        let canonical = CanonicalDailyTemperatureReport {
            source_product: "HKO Open Data CLMMAXT/CLMMINT",
            station: station.as_str(),
            statistic: statistic.as_str(),
            local_date,
            value_celsius: value,
            completeness: &row[4],
            documented_earliest_publication_at,
            publication_time_basis: "first_local_download_visibility",
        };
        let report_hash = CanonicalDigest::content_hash_json(&canonical)?;
        let raw_report =
            serde_json::to_string(&canonical).map_err(|error| ApiError::Deserialize {
                context: "HKO daily temperature provenance".to_owned(),
                detail: error.to_string(),
            })?;
        reports.push(WeatherObservationReport {
            source_id: DomainSourceId::hko_open_data(),
            instrument_key: DomainInstrumentKey::hko_daily_temperature(station, statistic),
            subject_key: station.to_string(),
            report_kind: WeatherObservationReportKind::HkoDailyTemperature,
            variable: match statistic {
                WeatherTemperatureStatistic::Maximum => WeatherVariable::TemperatureMaximum,
                WeatherTemperatureStatistic::Minimum => WeatherVariable::TemperatureMinimum,
            },
            value,
            unit: DomainMeasurementUnit::Celsius,
            precision: Decimal::new(1, 1),
            observed_at: day_end,
            valid_from: Some(day_start),
            valid_to: Some(day_end),
            published_at: available_at,
            available_at,
            report_hash,
            raw_report,
        });
    }
    reports.sort_by_key(|report| report.observed_at);
    Ok(HkoDailyTemperatureMonth {
        reports,
        response_hash,
        incomplete_rows,
        unavailable_rows,
    })
}

fn validate_daily_temperature_schema(
    wire: &DailyTemperatureWire,
    station: &HkoStation,
    statistic: WeatherTemperatureStatistic,
) -> QuantResult<()> {
    let expected_statistic = match statistic {
        WeatherTemperatureStatistic::Maximum => "Daily Maximum Temperature",
        WeatherTemperatureStatistic::Minimum => "Daily Minimum Temperature",
    };
    let station_name = match station.as_str() {
        "HKO" => "Hong Kong Observatory",
        _ => {
            return Err(daily_temperature_error(format!(
                "unsupported HKO daily-temperature station `{station}`"
            ))
            .into());
        }
    };
    if !wire
        .dataset_type
        .iter()
        .any(|value| value.contains(expected_statistic) && value.contains(station_name))
    {
        return Err(daily_temperature_error(format!(
            "response type does not identify {expected_statistic} at {station_name}"
        ))
        .into());
    }
    let expected_fields = ["Year", "Month", "Day", "Value", "data Completeness"];
    if wire.fields.len() != expected_fields.len()
        || !wire
            .fields
            .iter()
            .zip(expected_fields)
            .all(|(actual, expected)| actual.contains(expected))
    {
        return Err(daily_temperature_error("unexpected daily temperature field schema").into());
    }
    if !wire
        .legend
        .iter()
        .any(|value| value.contains("data Complete"))
        || !wire
            .legend
            .iter()
            .any(|value| value.contains("data incomplete"))
        || !wire
            .legend
            .iter()
            .any(|value| value.contains("unavailable"))
    {
        return Err(
            daily_temperature_error("daily temperature completeness legend is incomplete").into(),
        );
    }
    Ok(())
}

fn validate_daily_temperature_partition(
    year: i32,
    month: u32,
    available_at: DateTime<Utc>,
) -> QuantResult<()> {
    if !(1884..=available_at.year()).contains(&year) || !(1..=12).contains(&month) {
        return Err(
            daily_temperature_error("invalid HKO daily-temperature year/month partition").into(),
        );
    }
    Ok(())
}

fn parse_i32(value: &str, field: &str) -> QuantResult<i32> {
    value
        .parse::<i32>()
        .map_err(|error| daily_temperature_error(format!("{field}: {error}")).into())
}

fn parse_u32(value: &str, field: &str) -> QuantResult<u32> {
    value
        .parse::<u32>()
        .map_err(|error| daily_temperature_error(format!("{field}: {error}")).into())
}

fn parse_daily_rainfall(
    body: &str,
    request: &HkoDailyRainfallRequest,
) -> QuantResult<HkoDailyRainfallDataset> {
    let response_hash = CanonicalDigest::content_hash_bytes(body.as_bytes());
    let normalized = body.strip_prefix('\u{feff}').unwrap_or(body);
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(normalized.as_bytes());
    let mut rows = reader.records();
    let _localized_title = next_rainfall_row(&mut rows, "localized title")?;
    let english_title = next_rainfall_row(&mut rows, "English title")?;
    if english_title.len() != 1
        || !english_title[0].contains("Daily Total Rainfall (mm)")
        || !english_title[0].contains(&request.site_key)
    {
        return Err(daily_rainfall_error(
            "daily rainfall title does not match the exact configured site",
        )
        .into());
    }
    let header = next_rainfall_row(&mut rows, "header")?;
    let expected = [
        "年/Year",
        "月/Month",
        "日/Day",
        "數值/Value",
        "數據完整性/data Completeness",
    ];
    if header.len() != expected.len()
        || !header
            .iter()
            .zip(expected)
            .all(|(actual, expected)| actual.trim() == expected)
    {
        return Err(daily_rainfall_error("unexpected daily rainfall header contract").into());
    }

    let mut reports = Vec::new();
    let mut incomplete_rows = 0_usize;
    let mut unavailable_rows = 0_usize;
    let mut trace_rows = 0_usize;
    let mut footer_started = false;
    for row in rows {
        let row = row.map_err(|error| daily_rainfall_error(error.to_string()))?;
        let Ok(year) = parse_rainfall_component::<i32>(&row, 0, "year") else {
            footer_started = true;
            continue;
        };
        if footer_started || row.len() != 5 {
            return Err(daily_rainfall_error(
                "daily rainfall data row appears after the completeness footer",
            )
            .into());
        }
        let month = parse_rainfall_component::<u32>(&row, 1, "month")?;
        let day = parse_rainfall_component::<u32>(&row, 2, "day")?;
        let local_date = NaiveDate::from_ymd_opt(year, month, day)
            .ok_or_else(|| daily_rainfall_error("invalid daily rainfall date"))?;
        match (row[3].trim(), row[4].trim()) {
            ("***", _) => {
                unavailable_rows += 1;
                continue;
            }
            (_, "#") => {
                incomplete_rows += 1;
                continue;
            }
            ("Trace", "C") => {
                trace_rows += 1;
                continue;
            }
            (_, "C") => {}
            (_, completeness) => {
                return Err(daily_rainfall_error(format!(
                    "unsupported rainfall completeness `{completeness}`"
                ))
                .into());
            }
        }
        let total = parse_rainfall_component::<Decimal>(&row, 3, "rainfall")?;
        if total < Decimal::ZERO {
            return Err(daily_rainfall_error("daily rainfall cannot be negative").into());
        }
        if local_date < request.minimum_date {
            continue;
        }
        let day_start = Hong_Kong
            .from_local_datetime(
                &local_date
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| daily_rainfall_error("invalid rainfall day start"))?,
            )
            .single()
            .ok_or_else(|| daily_rainfall_error("rainfall day start is ambiguous"))?
            .with_timezone(&Utc);
        let day_end = Hong_Kong
            .from_local_datetime(
                &local_date
                    .succ_opt()
                    .ok_or_else(|| daily_rainfall_error("rainfall date overflow"))?
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| daily_rainfall_error("invalid rainfall day end"))?,
            )
            .single()
            .ok_or_else(|| daily_rainfall_error("rainfall day end is ambiguous"))?
            .with_timezone(&Utc);
        if request.available_at < day_end {
            return Err(daily_rainfall_error(
                "complete rainfall row was visible before its local day ended",
            )
            .into());
        }
        let canonical = CanonicalDailyRainfallReport {
            source_product: "HKO Daily Total Rainfall CSV",
            station_key: &request.station_key,
            site_key: &request.site_key,
            local_date,
            total_millimeters: total,
            completeness: "C",
            publication_time_basis: "first_local_download_visibility",
        };
        let report_hash = CanonicalDigest::content_hash_json(&canonical)?;
        let raw_report = serde_json::to_string(&canonical)
            .map_err(|error| daily_rainfall_error(error.to_string()))?;
        reports.push(WeatherObservationReport {
            source_id: DomainSourceId::hko_open_data(),
            instrument_key: DomainInstrumentKey::hko_daily_rainfall(&request.station_key),
            subject_key: request.site_key.clone(),
            report_kind: WeatherObservationReportKind::HkoDailyRainfall,
            variable: WeatherVariable::Precipitation,
            value: total,
            unit: DomainMeasurementUnit::Millimeter,
            precision: Decimal::new(1, total.scale()),
            observed_at: day_end,
            valid_from: Some(day_start),
            valid_to: Some(day_end),
            published_at: request.available_at,
            available_at: request.available_at,
            report_hash,
            raw_report,
        });
    }
    reports.sort_by_key(|report| report.observed_at);
    Ok(HkoDailyRainfallDataset {
        reports,
        response_hash,
        incomplete_rows,
        unavailable_rows,
        trace_rows,
    })
}

fn next_rainfall_row(
    rows: &mut StringRecordsIter<'_, &[u8]>,
    name: &str,
) -> QuantResult<StringRecord> {
    rows.next()
        .ok_or_else(|| daily_rainfall_error(format!("missing {name}")))?
        .map_err(|error| daily_rainfall_error(error.to_string()).into())
}

fn parse_rainfall_component<T>(row: &StringRecord, index: usize, name: &str) -> Result<T, ApiError>
where
    T: FromStr,
    T::Err: Display,
{
    row.get(index)
        .map(str::trim)
        .ok_or_else(|| daily_rainfall_error(format!("missing {name}")))?
        .parse::<T>()
        .map_err(|error| daily_rainfall_error(format!("invalid {name}: {error}")))
}

fn daily_rainfall_error(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "HKO Daily Total Rainfall CSV".to_owned(),
        detail: detail.into(),
    }
}

fn daily_temperature_error(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "HKO CLMMAXT/CLMMINT JSON".to_owned(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone, Utc};
    use quant_pivot_models::{
        config::HkoOpenDataSourceConfig,
        domain::data_plane::WeatherObservationReportKind,
        types::{DomainMeasurementUnit, HkoStation, WeatherTemperatureStatistic, WeatherVariable},
    };
    use rust_decimal_macros::dec;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::{HkoDailyRainfallRequest, HkoOpenDataSource};

    #[tokio::test]
    async fn daily_rainfall_preserves_truth() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                concat!(
                    "日總雨量(毫米) - 天文台\n",
                    "Daily Total Rainfall (mm) at the Hong Kong Observatory\n",
                    "年/Year,月/Month,日/Day,數值/Value,數據完整性/data Completeness\n",
                    "2026,7,15,***,\n",
                    "2026,7,16,10.0,#\n",
                    "2026,7,17,Trace,C\n",
                    "2026,7,18,18.2,C\n",
                    "*** Unavailable\n",
                ),
                "text/csv",
            ))
            .mount(&server)
            .await;
        let source =
            HkoOpenDataSource::connect(HkoOpenDataSourceConfig::default()).expect("source");
        let dataset = source
            .daily_rainfall(&HkoDailyRainfallRequest {
                station_key: "HKO".to_owned(),
                site_key: "Hong Kong Observatory".to_owned(),
                csv_url: server.uri(),
                minimum_date: NaiveDate::from_ymd_opt(2026, 7, 15).expect("date"),
                available_at: Utc.with_ymd_and_hms(2026, 7, 19, 0, 0, 0).unwrap(),
            })
            .await
            .expect("rainfall");
        assert_eq!(dataset.unavailable_rows, 1);
        assert_eq!(dataset.incomplete_rows, 1);
        assert_eq!(dataset.trace_rows, 1);
        let [report] = dataset.reports.as_slice() else {
            panic!("one completed numeric rainfall row expected")
        };
        assert_eq!(
            report.report_kind,
            WeatherObservationReportKind::HkoDailyRainfall
        );
        assert_eq!(report.variable, WeatherVariable::Precipitation);
        assert_eq!(report.unit, DomainMeasurementUnit::Millimeter);
        assert_eq!(report.value, dec!(18.2));
        assert_eq!(
            report.valid_from,
            Some(Utc.with_ymd_and_hms(2026, 7, 17, 16, 0, 0).unwrap())
        );
        assert_eq!(
            report.valid_to,
            Some(Utc.with_ymd_and_hms(2026, 7, 18, 16, 0, 0).unwrap())
        );
        assert!(report.raw_report.contains("\"total_millimeters\":\"18.2\""));
    }

    #[tokio::test]
    async fn daily_rainfall_rejects_site() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                concat!(
                    "日總雨量(毫米) - 天文台\n",
                    "Daily Total Rainfall (mm) at the Hong Kong Observatory\n",
                    "年/Year,月/Month,日/Day,數值/Value,數據完整性/data Completeness\n",
                    "2026,7,18,18.2,C\n",
                ),
                "text/csv",
            ))
            .mount(&server)
            .await;
        let source =
            HkoOpenDataSource::connect(HkoOpenDataSourceConfig::default()).expect("source");
        let result = source
            .daily_rainfall(&HkoDailyRainfallRequest {
                station_key: "TKL".to_owned(),
                site_key: "Ta Kwu Ling".to_owned(),
                csv_url: server.uri(),
                minimum_date: NaiveDate::from_ymd_opt(2026, 7, 18).expect("date"),
                available_at: Utc.with_ymd_and_hms(2026, 7, 19, 0, 0, 0).unwrap(),
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn preserves_skips_non_rows() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/opendata.php"))
            .and(query_param("dataType", "CLMMAXT"))
            .and(query_param("lang", "en"))
            .and(query_param("rformat", "json"))
            .and(query_param("station", "HKO"))
            .and(query_param("year", "2025"))
            .and(query_param("month", "7"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                r##"{
                  "type": [
                    "日最高氣溫(攝氏度) - 天文台",
                    "Daily Maximum Temperature (°C) at the Hong Kong Observatory"
                  ],
                  "fields": [
                    "年/Year", "月/Month", "日/Day", "數值/Value",
                    "數據完整性/data Completeness"
                  ],
                  "data": [
                    ["2025", "7", "1", "29.8", "C"],
                    ["2025", "7", "2", "31.2", "#"],
                    ["2025", "7", "3", "***", ""]
                  ],
                  "legend": [
                    "*** 沒有數據/unavailable",
                    "# 數據不完整/data incomplete",
                    "C 數據完整/data Complete"
                  ]
                }"##,
                "application/json",
            ))
            .mount(&server)
            .await;
        let source = HkoOpenDataSource::connect(HkoOpenDataSourceConfig {
            base_url: server.uri(),
            ..HkoOpenDataSourceConfig::default()
        })
        .expect("source");
        let station = HkoStation::parse("HKO").expect("station");
        let available_at = Utc.with_ymd_and_hms(2025, 7, 4, 0, 0, 0).unwrap();
        let month = source
            .daily_temperatures(
                &station,
                WeatherTemperatureStatistic::Maximum,
                2025,
                7,
                available_at,
            )
            .await
            .expect("daily temperatures");

        assert_eq!(month.reports.len(), 1);
        assert_eq!(month.incomplete_rows, 1);
        assert_eq!(month.unavailable_rows, 1);
        let report = &month.reports[0];
        assert_eq!(
            report.report_kind,
            WeatherObservationReportKind::HkoDailyTemperature
        );
        assert_eq!(report.variable, WeatherVariable::TemperatureMaximum);
        assert_eq!(report.unit, DomainMeasurementUnit::Celsius);
        assert_eq!(report.value, dec!(29.8));
        assert_eq!(report.precision, dec!(0.1));
        assert_eq!(
            report.valid_from,
            Some(Utc.with_ymd_and_hms(2025, 6, 30, 16, 0, 0).unwrap())
        );
        assert_eq!(
            report.valid_to,
            Some(Utc.with_ymd_and_hms(2025, 7, 1, 16, 0, 0).unwrap())
        );
        assert_eq!(report.published_at, available_at);
        assert!(
            report
                .raw_report
                .contains("first_local_download_visibility")
        );
    }
}
