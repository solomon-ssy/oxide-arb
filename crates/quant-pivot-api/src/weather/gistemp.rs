//! NASA GISS GISTEMP v4 monthly global anomaly adapter.

use std::{str::FromStr, time::Duration};

use chrono::{DateTime, NaiveDate, Utc};
use csv::ReaderBuilder;
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::NasaGistempSourceConfig,
    domain::data_plane::{WeatherObservationReport, WeatherObservationReportKind},
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, WeatherVariable,
    },
};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Serialize;

use crate::infra::{http::get_text_with_retry, retry::RetryPolicy};

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// One immutable view of the current GISTEMP v4 table.
pub struct GistempDataset {
    pub file_hash: ContentHash,
    pub reports: Vec<WeatherObservationReport>,
}

/// Public NASA GISS table client.
pub struct NasaGistempSource {
    config: NasaGistempSourceConfig,
    http: Client,
    retry_policy: RetryPolicy,
}

impl NasaGistempSource {
    pub fn connect(config: NasaGistempSourceConfig) -> QuantResult<Self> {
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .user_agent("quant-pivot/0.1 nasa-gistemp-ingest")
            .build()
            .map_err(|error| ApiError::Sdk(format!("NASA GISTEMP HTTP client: {error}")))?;
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

    pub async fn monthly_anomalies(
        &self,
        available_at: DateTime<Utc>,
    ) -> QuantResult<GistempDataset> {
        let body = get_text_with_retry(&self.http, &self.retry_policy, &self.config.csv_url)
            .await
            .map_err(QuantError::from)?;
        parse_gistemp(&body, available_at)
    }

    pub async fn annual_anomalies(
        &self,
        available_at: DateTime<Utc>,
    ) -> QuantResult<GistempDataset> {
        let body = get_text_with_retry(&self.http, &self.retry_policy, &self.config.annual_url)
            .await
            .map_err(QuantError::from)?;
        parse_gistemp_annual(&body, available_at)
    }
}

#[derive(Serialize)]
struct CanonicalGistempMonth {
    product: &'static str,
    month_start: NaiveDate,
    anomaly_celsius: Decimal,
}

#[derive(Serialize)]
struct CanonicalGistempYear {
    product: &'static str,
    year: i32,
    anomaly_celsius: Decimal,
}

fn parse_gistemp(body: &str, available_at: DateTime<Utc>) -> QuantResult<GistempDataset> {
    let file_hash = CanonicalDigest::content_hash_bytes(body.as_bytes());
    let (title, csv) = body
        .split_once('\n')
        .ok_or_else(|| parse_error("missing title/header separator"))?;
    if title.trim_end_matches('\r') != "Land-Ocean: Global Means" {
        return Err(parse_error("unexpected GISTEMP product title").into());
    }
    let mut reader = ReaderBuilder::new().from_reader(csv.as_bytes());
    let headers = reader
        .headers()
        .map_err(|error| parse_error(error.to_string()))?
        .clone();
    if headers.get(0) != Some("Year")
        || MONTHS
            .iter()
            .enumerate()
            .any(|(index, month)| headers.get(index + 1) != Some(*month))
    {
        return Err(parse_error("unexpected Year/Jan..Dec header contract").into());
    }
    let mut reports = Vec::new();
    for row in reader.records() {
        let row = row.map_err(|error| parse_error(error.to_string()))?;
        let year = row
            .get(0)
            .ok_or_else(|| parse_error("missing year"))?
            .parse::<i32>()
            .map_err(|error| parse_error(error.to_string()))?;
        for month in 1..=12_u32 {
            let Some(value) = row
                .get(month as usize)
                .filter(|value| *value != "***" && !value.is_empty())
            else {
                continue;
            };
            let anomaly = parse_anomaly(value)?;
            let month_start = NaiveDate::from_ymd_opt(year, month, 1)
                .ok_or_else(|| parse_error("invalid year/month"))?;
            let next_month = if month == 12 {
                NaiveDate::from_ymd_opt(year + 1, 1, 1)
            } else {
                NaiveDate::from_ymd_opt(year, month + 1, 1)
            }
            .ok_or_else(|| parse_error("invalid next month"))?;
            let observed_at = next_month
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| parse_error("invalid month boundary"))?
                .and_utc();
            if observed_at > available_at {
                return Err(
                    parse_error("published value belongs to an incomplete future month").into(),
                );
            }
            let canonical = CanonicalGistempMonth {
                product: "GISTEMP_v4_LOTI",
                month_start,
                anomaly_celsius: anomaly,
            };
            let report_hash = CanonicalDigest::content_hash_json(&canonical)?;
            let raw_report = serde_json::to_string(&canonical)
                .map_err(|error| parse_error(error.to_string()))?;
            reports.push(WeatherObservationReport {
                source_id: DomainSourceId::nasa_gistemp(),
                instrument_key: DomainInstrumentKey::nasa_gistemp_loti(),
                subject_key: "global_land_ocean".to_owned(),
                report_kind: WeatherObservationReportKind::NasaGistemp,
                variable: WeatherVariable::GlobalTemperatureAnomaly,
                value: anomaly,
                unit: DomainMeasurementUnit::CelsiusAnomaly,
                precision: Decimal::new(1, 2),
                observed_at,
                valid_from: Some(
                    month_start
                        .and_hms_opt(0, 0, 0)
                        .ok_or_else(|| parse_error("invalid month start"))?
                        .and_utc(),
                ),
                valid_to: Some(observed_at),
                published_at: available_at,
                available_at,
                report_hash,
                raw_report,
            });
        }
    }
    reports.sort_by_key(|report| report.observed_at);
    Ok(GistempDataset { file_hash, reports })
}

fn parse_anomaly(value: &str) -> QuantResult<Decimal> {
    let normalized = value.strip_prefix("-.").map_or_else(
        || {
            value
                .strip_prefix('.')
                .map_or_else(|| value.to_owned(), |suffix| format!("0.{suffix}"))
        },
        |suffix| format!("-0.{suffix}"),
    );
    Decimal::from_str(&normalized).map_err(|error| parse_error(error.to_string()).into())
}

fn parse_gistemp_annual(body: &str, available_at: DateTime<Utc>) -> QuantResult<GistempDataset> {
    let file_hash = CanonicalDigest::content_hash_bytes(body.as_bytes());
    let mut lines = body.lines();
    if lines.next() != Some("Land-Ocean Temperature Index (C)")
        || lines.next() != Some("--------------------------------")
        || lines.next() != Some("")
        || lines.next() != Some("Year No_Smoothing  Lowess(5)")
        || lines.next() != Some("----------------------------")
    {
        return Err(parse_error("unexpected annual graph header contract").into());
    }
    let mut reports = Vec::new();
    for line in lines.filter(|line| !line.trim().is_empty()) {
        let columns = line.split_whitespace().collect::<Vec<_>>();
        if columns.len() != 3 {
            return Err(parse_error("annual graph row must have three columns").into());
        }
        let year = columns[0]
            .parse::<i32>()
            .map_err(|error| parse_error(error.to_string()))?;
        let anomaly =
            Decimal::from_str(columns[1]).map_err(|error| parse_error(error.to_string()))?;
        let year_start = NaiveDate::from_ymd_opt(year, 1, 1)
            .ok_or_else(|| parse_error("invalid annual graph year"))?;
        let next_year = NaiveDate::from_ymd_opt(
            year.checked_add(1)
                .ok_or_else(|| parse_error("annual graph year overflow"))?,
            1,
            1,
        )
        .ok_or_else(|| parse_error("invalid annual graph next year"))?;
        let observed_at = next_year
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| parse_error("invalid annual boundary"))?
            .and_utc();
        if observed_at > available_at {
            return Err(parse_error("annual value belongs to an incomplete future year").into());
        }
        let canonical = CanonicalGistempYear {
            product: "GISTEMP_v4_LOTI_Annual_No_Smoothing",
            year,
            anomaly_celsius: anomaly,
        };
        let report_hash = CanonicalDigest::content_hash_json(&canonical)?;
        let raw_report =
            serde_json::to_string(&canonical).map_err(|error| parse_error(error.to_string()))?;
        reports.push(WeatherObservationReport {
            source_id: DomainSourceId::nasa_gistemp(),
            instrument_key: DomainInstrumentKey::nasa_gistemp_loti_annual(),
            subject_key: "global_land_ocean_annual".to_owned(),
            report_kind: WeatherObservationReportKind::NasaGistempAnnual,
            variable: WeatherVariable::GlobalTemperatureAnomaly,
            value: anomaly,
            unit: DomainMeasurementUnit::CelsiusAnomaly,
            precision: Decimal::new(1, 2),
            observed_at,
            valid_from: Some(
                year_start
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| parse_error("invalid annual start"))?
                    .and_utc(),
            ),
            valid_to: Some(observed_at),
            published_at: available_at,
            available_at,
            report_hash,
            raw_report,
        });
    }
    if reports.is_empty() {
        return Err(parse_error("annual graph has no data rows").into());
    }
    reports.sort_by_key(|report| report.observed_at);
    Ok(GistempDataset { file_hash, reports })
}

fn parse_error(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "NASA GISTEMP v4 CSV".to_owned(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        config::NasaGistempSourceConfig, domain::data_plane::WeatherObservationReportKind,
    };
    use rust_decimal_macros::dec;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::NasaGistempSource;

    #[tokio::test]
    async fn parses_months_skips_markers() {
        let server = MockServer::start().await;
        let body = concat!(
            "Land-Ocean: Global Means\n",
            "Year,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec,J-D\n",
            "2025,-.19,.25,***,***,***,***,***,***,***,***,***,***,***\n",
        );
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = NasaGistempSource::connect(NasaGistempSourceConfig {
            csv_url: server.uri(),
            ..NasaGistempSourceConfig::default()
        })
        .expect("source");
        let dataset = source
            .monthly_anomalies(Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap())
            .await
            .expect("dataset");
        assert_eq!(dataset.reports.len(), 2);
        assert_eq!(dataset.reports[0].value, dec!(-0.19));
        assert_eq!(dataset.reports[1].value, dec!(0.25));
        assert_eq!(
            dataset.reports[0].report_kind,
            WeatherObservationReportKind::NasaGistemp
        );
    }

    #[tokio::test]
    async fn rejects_unknown_product_title() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("Other Product\nYear,Jan\n"))
            .mount(&server)
            .await;
        let source = NasaGistempSource::connect(NasaGistempSourceConfig {
            csv_url: server.uri(),
            ..NasaGistempSourceConfig::default()
        })
        .expect("source");
        assert!(source.monthly_anomalies(Utc::now()).await.is_err());
    }

    #[tokio::test]
    async fn parses_annual_unsmoothed() {
        let server = MockServer::start().await;
        let body = concat!(
            "Land-Ocean Temperature Index (C)\n",
            "--------------------------------\n",
            "\n",
            "Year No_Smoothing  Lowess(5)\n",
            "----------------------------\n",
            "2024      1.28      1.15\n",
            "2025      1.19      1.20\n",
        );
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = NasaGistempSource::connect(NasaGistempSourceConfig {
            annual_url: server.uri(),
            ..NasaGistempSourceConfig::default()
        })
        .expect("source");
        let dataset = source
            .annual_anomalies(Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap())
            .await
            .expect("dataset");
        assert_eq!(dataset.reports.len(), 2);
        assert_eq!(dataset.reports[0].value, dec!(1.28));
        assert_eq!(
            dataset.reports[0].report_kind,
            WeatherObservationReportKind::NasaGistempAnnual
        );
    }
}
