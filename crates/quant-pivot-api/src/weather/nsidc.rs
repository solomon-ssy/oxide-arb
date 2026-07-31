//! NOAA/NSIDC Sea Ice Index v4 daily and monthly extent adapters.

use std::{fmt::Display, str::FromStr, time::Duration};

use chrono::{DateTime, Months, NaiveDate, Utc};
use csv::{ReaderBuilder, StringRecord, Trim};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::NsidcSeaIceSourceConfig,
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

/// NSIDC hemisphere partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeaIceHemisphere {
    North,
    South,
}

impl SeaIceHemisphere {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::North => "north",
            Self::South => "south",
        }
    }

    const fn file_prefix(self) -> &'static str {
        match self {
            Self::North => "N",
            Self::South => "S",
        }
    }
}

/// One immutable view of a hemisphere's current v4 CSV.
pub struct SeaIceDataset {
    pub file_hash: ContentHash,
    pub reports: Vec<WeatherObservationReport>,
}

/// Public NOAA@NSIDC file client.
pub struct NsidcSeaIceSource {
    config: NsidcSeaIceSourceConfig,
    http: Client,
    retry_policy: RetryPolicy,
}

impl NsidcSeaIceSource {
    pub fn connect(config: NsidcSeaIceSourceConfig) -> QuantResult<Self> {
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .user_agent("quant-pivot/0.1 nsidc-sea-ice-ingest")
            .build()
            .map_err(|error| ApiError::Sdk(format!("NSIDC HTTP client: {error}")))?;
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

    pub async fn daily_extent(
        &self,
        hemisphere: SeaIceHemisphere,
        available_at: DateTime<Utc>,
    ) -> QuantResult<SeaIceDataset> {
        let url = match hemisphere {
            SeaIceHemisphere::North => &self.config.north_daily_csv_url,
            SeaIceHemisphere::South => &self.config.south_daily_csv_url,
        };
        let body = get_text_with_retry(&self.http, &self.retry_policy, url)
            .await
            .map_err(QuantError::from)?;
        parse_daily_extent(&body, hemisphere, available_at)
    }

    pub async fn monthly_extent(
        &self,
        hemisphere: SeaIceHemisphere,
        month: u32,
        available_at: DateTime<Utc>,
    ) -> QuantResult<SeaIceDataset> {
        if !(1..=12).contains(&month) {
            return Err(parse_error("monthly extent month must be in 1..=12").into());
        }
        let base_url = match hemisphere {
            SeaIceHemisphere::North => &self.config.north_monthly_base_url,
            SeaIceHemisphere::South => &self.config.south_monthly_base_url,
        };
        let url = format!(
            "{}/{}_{month:02}_extent_v4.0.csv",
            base_url.trim_end_matches('/'),
            hemisphere.file_prefix()
        );
        let body = get_text_with_retry(&self.http, &self.retry_policy, &url)
            .await
            .map_err(QuantError::from)?;
        parse_monthly_extent(&body, hemisphere, month, available_at)
    }
}

#[derive(Serialize)]
struct CanonicalSeaIceDay<'a> {
    product: &'static str,
    hemisphere: &'a str,
    date: NaiveDate,
    extent_million_square_km: Decimal,
    missing_million_square_km: Decimal,
    source_data: &'a str,
}

#[derive(Serialize)]
struct CanonicalSeaIceMonth<'a> {
    product: &'static str,
    hemisphere: &'a str,
    year: i32,
    month: u32,
    extent_million_square_km: Decimal,
    area_million_square_km: Decimal,
    source_dataset: &'a str,
}

fn parse_daily_extent(
    body: &str,
    hemisphere: SeaIceHemisphere,
    available_at: DateTime<Utc>,
) -> QuantResult<SeaIceDataset> {
    let file_hash = CanonicalDigest::content_hash_bytes(body.as_bytes());
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(body.as_bytes());
    let mut rows = reader.records();
    let header = rows
        .next()
        .ok_or_else(|| parse_error("missing field header"))?
        .map_err(|error| parse_error(error.to_string()))?;
    let expected = ["Year", "Month", "Day", "Extent", "Missing", "Source Data"];
    if header
        .iter()
        .take(expected.len())
        .map(str::trim)
        .ne(expected)
    {
        return Err(parse_error("unexpected daily extent header contract").into());
    }
    let units = rows
        .next()
        .ok_or_else(|| parse_error("missing unit header"))?
        .map_err(|error| parse_error(error.to_string()))?;
    if units.get(3).map(str::trim) != Some("10^6 sq km")
        || units.get(4).map(str::trim) != Some("10^6 sq km")
    {
        return Err(parse_error("unexpected extent/missing units").into());
    }
    let mut reports = Vec::new();
    for row in rows {
        let row = row.map_err(|error| parse_error(error.to_string()))?;
        if row.len() < 5 {
            return Err(parse_error("daily extent row has fewer than five fields").into());
        }
        let year = parse_component(&row, 0, "year")?;
        let month = parse_component(&row, 1, "month")?;
        let day = parse_component(&row, 2, "day")?;
        let date = NaiveDate::from_ymd_opt(year, month, day)
            .ok_or_else(|| parse_error("invalid daily extent date"))?;
        let extent = parse_component::<Decimal>(&row, 3, "extent")?;
        let missing = parse_component::<Decimal>(&row, 4, "missing")?;
        if extent < Decimal::ZERO || missing < Decimal::ZERO {
            return Err(parse_error("extent/missing area cannot be negative").into());
        }
        let next_date = date
            .succ_opt()
            .ok_or_else(|| parse_error("daily extent date overflow"))?;
        let observed_at = next_date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| parse_error("invalid daily boundary"))?
            .and_utc();
        if observed_at > available_at {
            return Err(parse_error("published extent belongs to an incomplete future day").into());
        }
        let source_data = row.iter().skip(5).collect::<Vec<_>>().join(",");
        let canonical = CanonicalSeaIceDay {
            product: "NOAA_NSIDC_Sea_Ice_Index_v4",
            hemisphere: hemisphere.as_str(),
            date,
            extent_million_square_km: extent,
            missing_million_square_km: missing,
            source_data: &source_data,
        };
        let report_hash = CanonicalDigest::content_hash_json(&canonical)?;
        let raw_report =
            serde_json::to_string(&canonical).map_err(|error| parse_error(error.to_string()))?;
        reports.push(WeatherObservationReport {
            source_id: DomainSourceId::nsidc_sea_ice_index(),
            instrument_key: DomainInstrumentKey::nsidc_daily_extent(hemisphere.as_str()),
            subject_key: hemisphere.as_str().to_owned(),
            report_kind: WeatherObservationReportKind::NsidcDailySeaIce,
            variable: WeatherVariable::SeaIceExtent,
            value: extent,
            unit: DomainMeasurementUnit::MillionSquareKilometer,
            precision: Decimal::new(1, 3),
            observed_at,
            valid_from: Some(
                date.and_hms_opt(0, 0, 0)
                    .ok_or_else(|| parse_error("invalid day start"))?
                    .and_utc(),
            ),
            valid_to: Some(observed_at),
            published_at: available_at,
            available_at,
            report_hash,
            raw_report,
        });
    }
    reports.sort_by_key(|report| report.observed_at);
    Ok(SeaIceDataset { file_hash, reports })
}

fn parse_monthly_extent(
    body: &str,
    hemisphere: SeaIceHemisphere,
    requested_month: u32,
    available_at: DateTime<Utc>,
) -> QuantResult<SeaIceDataset> {
    let file_hash = CanonicalDigest::content_hash_bytes(body.as_bytes());
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .trim(Trim::All)
        .from_reader(body.as_bytes());
    let headers = reader
        .headers()
        .map_err(|error| parse_error(error.to_string()))?;
    let expected = ["year", "mo", "source_dataset", "region", "extent", "area"];
    if headers.iter().ne(expected) {
        return Err(parse_error("unexpected monthly extent header contract").into());
    }
    let mut reports = Vec::new();
    for row in reader.records() {
        let row = row.map_err(|error| parse_error(error.to_string()))?;
        if row.len() != expected.len() {
            return Err(parse_error("monthly extent row must have six fields").into());
        }
        let year = parse_component::<i32>(&row, 0, "year")?;
        let month = parse_component::<u32>(&row, 1, "month")?;
        if month != requested_month {
            return Err(parse_error(format!(
                "monthly extent partition {requested_month:02} contains month {month:02}"
            ))
            .into());
        }
        let source_dataset = row
            .get(2)
            .ok_or_else(|| parse_error("missing monthly source dataset"))?;
        if source_dataset != "NSIDC-0051" {
            return Err(parse_error(format!(
                "unsupported monthly source dataset `{source_dataset}`"
            ))
            .into());
        }
        let region = row
            .get(3)
            .ok_or_else(|| parse_error("missing monthly region"))?;
        if region != hemisphere.file_prefix() {
            return Err(parse_error(format!(
                "monthly extent region `{region}` does not match requested hemisphere"
            ))
            .into());
        }
        let extent = parse_component::<Decimal>(&row, 4, "extent")?;
        let area = parse_component::<Decimal>(&row, 5, "area")?;
        if extent < Decimal::ZERO || area < Decimal::ZERO || area > extent {
            return Err(parse_error("monthly extent/area relationship is invalid").into());
        }
        let start_date = NaiveDate::from_ymd_opt(year, month, 1)
            .ok_or_else(|| parse_error("invalid monthly extent date"))?;
        let end_date = start_date
            .checked_add_months(Months::new(1))
            .ok_or_else(|| parse_error("monthly extent date overflow"))?;
        let start_at = start_date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| parse_error("invalid monthly extent start"))?
            .and_utc();
        let end_at = end_date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| parse_error("invalid monthly extent end"))?
            .and_utc();
        if end_at > available_at {
            return Err(
                parse_error("monthly extent file contains an incomplete future month").into(),
            );
        }
        let canonical = CanonicalSeaIceMonth {
            product: "NOAA_NSIDC_Sea_Ice_Index_v4_monthly_mean",
            hemisphere: hemisphere.as_str(),
            year,
            month,
            extent_million_square_km: extent,
            area_million_square_km: area,
            source_dataset,
        };
        let report_hash = CanonicalDigest::content_hash_json(&canonical)?;
        let raw_report =
            serde_json::to_string(&canonical).map_err(|error| parse_error(error.to_string()))?;
        reports.push(WeatherObservationReport {
            source_id: DomainSourceId::nsidc_sea_ice_index(),
            instrument_key: DomainInstrumentKey::nsidc_monthly_extent(hemisphere.as_str()),
            subject_key: hemisphere.as_str().to_owned(),
            report_kind: WeatherObservationReportKind::NsidcMonthlySeaIce,
            variable: WeatherVariable::SeaIceExtent,
            value: extent,
            unit: DomainMeasurementUnit::MillionSquareKilometer,
            precision: Decimal::new(1, 2),
            observed_at: end_at,
            valid_from: Some(start_at),
            valid_to: Some(end_at),
            published_at: available_at,
            available_at,
            report_hash,
            raw_report,
        });
    }
    reports.sort_by_key(|report| report.observed_at);
    Ok(SeaIceDataset { file_hash, reports })
}

fn parse_component<T>(row: &StringRecord, index: usize, name: &str) -> QuantResult<T>
where
    T: FromStr,
    T::Err: Display,
{
    row.get(index)
        .map(str::trim)
        .ok_or_else(|| parse_error(format!("missing {name}")))?
        .parse::<T>()
        .map_err(|error| parse_error(format!("invalid {name}: {error}")).into())
}

fn parse_error(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "NOAA/NSIDC Sea Ice Index v4 CSV".to_owned(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        config::NsidcSeaIceSourceConfig, domain::data_plane::WeatherObservationReportKind,
    };
    use rust_decimal_macros::dec;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::{NsidcSeaIceSource, SeaIceHemisphere};

    #[tokio::test]
    async fn parses_preserves_missing_quality() {
        let server = MockServer::start().await;
        let body = concat!(
            "Year, Month, Day, Extent, Missing, Source Data\n",
            "YYYY, MM, DD, 10^6 sq km, 10^6 sq km, source\n",
            "2025, 09, 15, 4.123, 0.010, ['source-a']\n",
        );
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = NsidcSeaIceSource::connect(NsidcSeaIceSourceConfig {
            north_daily_csv_url: server.uri(),
            ..NsidcSeaIceSourceConfig::default()
        })
        .expect("source");
        let dataset = source
            .daily_extent(
                SeaIceHemisphere::North,
                Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap(),
            )
            .await
            .expect("dataset");
        assert_eq!(dataset.reports.len(), 1);
        assert_eq!(dataset.reports[0].value, dec!(4.123));
        assert!(dataset.reports[0].raw_report.contains("0.010"));
        assert_eq!(
            dataset.reports[0].report_kind,
            WeatherObservationReportKind::NsidcDailySeaIce
        );
    }

    #[tokio::test]
    async fn rejects_negative_missing_area() {
        let server = MockServer::start().await;
        let body = concat!(
            "Year, Month, Day, Extent, Missing, Source Data\n",
            "YYYY, MM, DD, 10^6 sq km, 10^6 sq km, source\n",
            "2025, 09, 15, 4.123, -0.010, source\n",
        );
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = NsidcSeaIceSource::connect(NsidcSeaIceSourceConfig {
            north_daily_csv_url: server.uri(),
            ..NsidcSeaIceSourceConfig::default()
        })
        .expect("source");
        assert!(
            source
                .daily_extent(SeaIceHemisphere::North, Utc::now())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn monthly_product_is_distinct() {
        let server = MockServer::start().await;
        let body = concat!(
            "year, mo,source_dataset, region, extent,   area\n",
            "2025,  7,    NSIDC-0051,      N,  10.31,   6.69\n",
        );
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = NsidcSeaIceSource::connect(NsidcSeaIceSourceConfig {
            north_monthly_base_url: server.uri(),
            ..NsidcSeaIceSourceConfig::default()
        })
        .expect("source");
        let dataset = source
            .monthly_extent(
                SeaIceHemisphere::North,
                7,
                Utc.with_ymd_and_hms(2026, 7, 18, 0, 0, 0).unwrap(),
            )
            .await
            .expect("dataset");
        let [report] = dataset.reports.as_slice() else {
            panic!("one monthly report expected")
        };
        assert_eq!(report.value, dec!(10.31));
        assert_eq!(
            report.report_kind,
            WeatherObservationReportKind::NsidcMonthlySeaIce
        );
        assert_eq!(
            report.valid_from,
            Some(Utc.with_ymd_and_hms(2025, 7, 1, 0, 0, 0).unwrap())
        );
        assert_eq!(
            report.valid_to,
            Some(Utc.with_ymd_and_hms(2025, 8, 1, 0, 0, 0).unwrap())
        );
    }
}
