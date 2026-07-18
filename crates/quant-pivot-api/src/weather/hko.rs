//! Hong Kong Observatory Open Data rainfall adapter.

use std::{collections::BTreeSet, time::Duration};

use chrono::{DateTime, Datelike, Days, NaiveDate, TimeDelta, TimeZone, Utc};
use chrono_tz::Asia::Hong_Kong;
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::HkoOpenDataSourceConfig,
    domain::{WeatherObservationReport, WeatherObservationReportKind},
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, HkoStation,
        WeatherTemperatureStatistic, WeatherVariable,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    infra::{http::get_text_with_retry, retry::RetryPolicy},
    wire::decimal::parse_decimal_value,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CurrentWeatherWire {
    rainfall: RainfallWire,
    update_time: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RainfallWire {
    data: Vec<RainfallPlaceWire>,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct RainfallPlaceWire {
    unit: String,
    place: String,
    #[serde(deserialize_with = "deserialize_decimal")]
    max: Decimal,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    min: Option<Decimal>,
}

#[derive(Serialize)]
struct CanonicalRainfallReport<'a> {
    place: &'a str,
    unit: &'a str,
    minimum: Option<Decimal>,
    maximum: Decimal,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    published_at: DateTime<Utc>,
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

/// Public HKO current-weather client. Rainfall values are rolling-window
/// district maxima; an optional district minimum remains in raw provenance.
pub struct HkoOpenDataSource {
    config: HkoOpenDataSourceConfig,
    http: reqwest::Client,
    retry_policy: RetryPolicy,
}

impl HkoOpenDataSource {
    pub fn connect(config: HkoOpenDataSourceConfig) -> QuantResult<Self> {
        let http = reqwest::Client::builder()
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
    pub fn with_http_client(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    /// Fetch one configured HKO reporting place from `rhrread`.
    pub async fn rainfall(
        &self,
        place: &str,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Option<WeatherObservationReport>> {
        validate_place(place)?;
        let url = format!(
            "{}/weather.php?dataType=rhrread&lang=en",
            self.config.base_url.trim_end_matches('/')
        );
        let body = get_text_with_retry(&self.http, &self.retry_policy, &url)
            .await
            .map_err(QuantError::from)?;
        parse_rainfall(&body, place, available_at)
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
    let response_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(body.as_bytes()))?;
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

fn parse_rainfall(
    body: &str,
    place: &str,
    available_at: DateTime<Utc>,
) -> QuantResult<Option<WeatherObservationReport>> {
    let wire: CurrentWeatherWire =
        serde_json::from_str(body).map_err(|error| parse_error(error.to_string()))?;
    if wire.rainfall.start_time >= wire.rainfall.end_time
        || wire.rainfall.end_time > wire.update_time
        || wire.update_time > available_at
    {
        return Err(parse_error(
            "rainfall window/update/availability timestamps are not monotonic",
        )
        .into());
    }
    let Some(row) = wire
        .rainfall
        .data
        .into_iter()
        .find(|row| row.place == place)
    else {
        return Ok(None);
    };
    if row.unit != "mm" {
        return Err(parse_error(format!("unsupported rainfall unit `{}`", row.unit)).into());
    }
    if row.max.is_sign_negative() || row.min.is_some_and(|minimum| minimum < Decimal::ZERO) {
        return Err(parse_error("rainfall cannot be negative").into());
    }
    if row.min.is_some_and(|minimum| minimum > row.max) {
        return Err(parse_error("rainfall minimum exceeds maximum").into());
    }
    let canonical = CanonicalRainfallReport {
        place: &row.place,
        unit: &row.unit,
        minimum: row.min,
        maximum: row.max,
        window_start: wire.rainfall.start_time,
        window_end: wire.rainfall.end_time,
        published_at: wire.update_time,
    };
    let report_hash = CanonicalDigest::content_hash_json(&canonical)?;
    let raw_report = serde_json::to_string(&canonical).map_err(|error| ApiError::Deserialize {
        context: "HKO rainfall provenance".to_owned(),
        detail: error.to_string(),
    })?;
    Ok(Some(WeatherObservationReport {
        source_id: DomainSourceId::hko_open_data(),
        instrument_key: DomainInstrumentKey::hko_rainfall(place),
        subject_key: place.to_owned(),
        report_kind: WeatherObservationReportKind::HkoRainfall,
        variable: WeatherVariable::Precipitation,
        value: row.max,
        unit: DomainMeasurementUnit::Millimeter,
        precision: Decimal::new(1, row.max.scale()),
        observed_at: wire.rainfall.end_time,
        valid_from: Some(wire.rainfall.start_time),
        valid_to: Some(wire.rainfall.end_time),
        published_at: wire.update_time,
        available_at,
        report_hash,
        raw_report,
    }))
}

fn validate_place(place: &str) -> QuantResult<()> {
    let valid =
        !place.trim().is_empty() && place.len() <= 128 && !place.chars().any(char::is_control);
    if !valid {
        return Err(
            parse_error("HKO place must be a non-empty printable value <= 128 bytes").into(),
        );
    }
    Ok(())
}

fn deserialize_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    parse_decimal_value(&value).map_err(serde::de::Error::custom)
}

fn deserialize_optional_decimal<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    value
        .map(|value| parse_decimal_value(&value).map_err(serde::de::Error::custom))
        .transpose()
}

fn parse_error(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "HKO rhrread JSON".to_owned(),
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
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        config::HkoOpenDataSourceConfig,
        domain::WeatherObservationReportKind,
        types::{DomainMeasurementUnit, HkoStation, WeatherTemperatureStatistic, WeatherVariable},
    };
    use rust_decimal_macros::dec;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::HkoOpenDataSource;

    #[tokio::test]
    async fn preserves_hko_window_and_district_range_provenance() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/weather.php"))
            .and(query_param("dataType", "rhrread"))
            .and(query_param("lang", "en"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                r#"{
                  "rainfall": {
                    "data": [
                      {"unit":"mm","place":"North District","max":18,"min":1,"main":"FALSE"},
                      {"unit":"mm","place":"Sai Kung","max":0,"main":"FALSE"}
                    ],
                    "startTime":"2026-07-18T05:45:00+08:00",
                    "endTime":"2026-07-18T06:45:00+08:00"
                  },
                  "updateTime":"2026-07-18T07:02:00+08:00"
                }"#,
                "application/json",
            ))
            .mount(&server)
            .await;
        let source = HkoOpenDataSource::connect(HkoOpenDataSourceConfig {
            base_url: server.uri(),
            ..HkoOpenDataSourceConfig::default()
        })
        .expect("source");
        let report = source
            .rainfall(
                "North District",
                Utc.with_ymd_and_hms(2026, 7, 17, 23, 3, 0).unwrap(),
            )
            .await
            .expect("rainfall")
            .expect("place");
        assert_eq!(
            report.report_kind,
            WeatherObservationReportKind::HkoRainfall
        );
        assert_eq!(report.variable, WeatherVariable::Precipitation);
        assert_eq!(report.unit, DomainMeasurementUnit::Millimeter);
        assert_eq!(report.value, dec!(18));
        assert_eq!(
            report.valid_from,
            Some(Utc.with_ymd_and_hms(2026, 7, 17, 21, 45, 0).unwrap())
        );
        assert!(report.raw_report.contains("\"minimum\":\"1\""));
    }

    #[tokio::test]
    async fn rejects_non_monotonic_or_invalid_rainfall() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                r#"{
                  "rainfall": {
                    "data": [{"unit":"mm","place":"North District","max":1,"min":2}],
                    "startTime":"2026-07-18T05:45:00+08:00",
                    "endTime":"2026-07-18T06:45:00+08:00"
                  },
                  "updateTime":"2026-07-18T07:02:00+08:00"
                }"#,
                "application/json",
            ))
            .mount(&server)
            .await;
        let source = HkoOpenDataSource::connect(HkoOpenDataSourceConfig {
            base_url: server.uri(),
            ..HkoOpenDataSourceConfig::default()
        })
        .expect("source");
        let result = source
            .rainfall(
                "North District",
                Utc.with_ymd_and_hms(2026, 7, 17, 23, 3, 0).unwrap(),
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn preserves_complete_hko_daily_temperature_and_skips_non_serving_rows() {
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
