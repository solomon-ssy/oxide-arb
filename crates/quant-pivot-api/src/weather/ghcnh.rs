//! NOAA `GHCNh` yearly station-file adapter for historical calibration.

use std::time::Duration;

use chrono::{DateTime, Datelike, NaiveDateTime, Utc};
use csv::{ReaderBuilder, StringRecord};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::GhcnhSourceConfig,
    domain::data_plane::{WeatherObservationReport, WeatherObservationReportKind},
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, IcaoStation,
        WeatherVariable,
    },
};
use reqwest::Client;
use rust_decimal::Decimal;

use crate::infra::{http::get_optional_bounded_bytes, retry::RetryPolicy};

const MAX_YEAR_FILE_BYTES: usize = 64 * 1024 * 1024;

const REQUIRED_COLUMNS: [&str; 6] = [
    "STATION",
    "DATE",
    "temperature",
    "temperature_Measurement_Code",
    "temperature_Quality_Code",
    "temperature_Report_Type",
];

/// One immutable yearly `GHCNh` station file and its accepted temperature rows.
pub struct GhcnhYear {
    pub file_hash: ContentHash,
    pub reports: Vec<WeatherObservationReport>,
}

/// Station-scoped `GHCNh` client. Historical observations retain their download
/// availability and never enter the live `AviationWeather` projection.
pub struct GhcnhSource {
    config: GhcnhSourceConfig,
    http: Client,
    retry_policy: RetryPolicy,
}

impl GhcnhSource {
    pub fn connect(config: GhcnhSourceConfig) -> QuantResult<Self> {
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .user_agent("quant-pivot/0.1 ghcnh-calibration")
            .build()
            .map_err(|error| ApiError::Sdk(format!("GHCNh HTTP client: {error}")))?;
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

    pub async fn yearly_station(
        &self,
        station: &IcaoStation,
        ghcnh_station_id: &str,
        year: i32,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Option<GhcnhYear>> {
        validate_station_id(ghcnh_station_id)?;
        let url = format!(
            "{base}/{year}/psv/GHCNh_{station_id}_{year}.psv",
            base = self.config.base_url.trim_end_matches('/'),
            station_id = ghcnh_station_id,
        );
        let Some(body) = get_optional_bounded_bytes(
            &self.http,
            &self.retry_policy,
            &url,
            "NOAA GHCNh yearly station file",
            MAX_YEAR_FILE_BYTES,
        )
        .await
        .map_err(QuantError::from)?
        else {
            return Ok(None);
        };
        parse_year_file(station, ghcnh_station_id, year, available_at, &body).map(Some)
    }
}

fn parse_year_file(
    station: &IcaoStation,
    ghcnh_station_id: &str,
    year: i32,
    available_at: DateTime<Utc>,
    body: &[u8],
) -> QuantResult<GhcnhYear> {
    let file_hash = CanonicalDigest::content_hash_bytes(body);
    let mut reader = ReaderBuilder::new()
        .delimiter(b'|')
        .flexible(true)
        .from_reader(body);
    let headers = reader
        .headers()
        .map_err(|error| parse_error(format!("invalid header: {error}")))?
        .clone();
    let columns = required_column_indices(&headers)?;
    let mut reports = Vec::new();
    for row in reader.records() {
        let row = row.map_err(|error| parse_error(format!("invalid row: {error}")))?;
        if row.get(columns.station) != Some(ghcnh_station_id) {
            return Err(parse_error("row station does not match frozen GHCNh binding").into());
        }
        let Some(temperature) = row
            .get(columns.temperature)
            .filter(|value| !value.is_empty())
            .map(str::parse::<Decimal>)
            .transpose()
            .map_err(|error| parse_error(format!("invalid temperature: {error}")))?
        else {
            continue;
        };
        let quality_code = row.get(columns.quality).unwrap_or_default();
        if !accepted_temperature_quality(quality_code) {
            continue;
        }
        let observed_at = parse_observation_time(
            row.get(columns.date)
                .ok_or_else(|| parse_error("missing DATE"))?,
        )?;
        if observed_at.year() != year {
            return Err(parse_error("row year does not match requested GHCNh file").into());
        }
        let raw_report = row.iter().collect::<Vec<_>>().join("|");
        let report_hash = CanonicalDigest::content_hash_bytes(raw_report.as_bytes());
        reports.push(WeatherObservationReport {
            source_id: DomainSourceId::ghcnh(),
            instrument_key: DomainInstrumentKey::ghcnh(station),
            subject_key: station.to_string(),
            report_kind: WeatherObservationReportKind::HistoricalGhcnh,
            variable: WeatherVariable::Temperature,
            value: temperature,
            unit: DomainMeasurementUnit::Celsius,
            precision: Decimal::new(1, temperature.scale()),
            observed_at,
            valid_from: None,
            valid_to: None,
            published_at: available_at,
            available_at,
            report_hash,
            raw_report,
        });
    }
    reports.sort_by(|left, right| {
        left.observed_at
            .cmp(&right.observed_at)
            .then_with(|| left.report_hash.cmp(&right.report_hash))
    });
    Ok(GhcnhYear { file_hash, reports })
}

struct GhcnhColumns {
    station: usize,
    date: usize,
    temperature: usize,
    quality: usize,
}

fn required_column_indices(headers: &StringRecord) -> QuantResult<GhcnhColumns> {
    for name in REQUIRED_COLUMNS {
        if !headers.iter().any(|header| header == name) {
            return Err(parse_error(format!("missing required column {name}")).into());
        }
    }
    let index = |name: &str| {
        headers
            .iter()
            .position(|header| header == name)
            .ok_or_else(|| parse_error(format!("missing required column {name}")))
    };
    Ok(GhcnhColumns {
        station: index("STATION")?,
        date: index("DATE")?,
        temperature: index("temperature")?,
        quality: index("temperature_Quality_Code")?,
    })
}

fn validate_station_id(value: &str) -> QuantResult<()> {
    let valid = value.len() == 11
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-');
    if !valid {
        return Err(parse_error(
            "GHCNh station id must be 11 uppercase ASCII letters/digits/hyphen",
        )
        .into());
    }
    Ok(())
}

fn parse_observation_time(value: &str) -> QuantResult<DateTime<Utc>> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S")
        .map(|value| value.and_utc())
        .map_err(|error| parse_error(format!("invalid DATE: {error}")).into())
}

fn accepted_temperature_quality(value: &str) -> bool {
    matches!(value, "" | "0" | "1" | "4" | "5" | "9")
}

fn parse_error(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "NOAA GHCNh PSV".into(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{config::GhcnhSourceConfig, types::IcaoStation};
    use rust_decimal_macros::dec;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::GhcnhSource;

    #[tokio::test]
    async fn reads_excludes_failed_qc() {
        let server = MockServer::start().await;
        let body = concat!(
            "STATION|DATE|temperature|temperature_Measurement_Code|temperature_Quality_Code|temperature_Report_Type\n",
            "USW00014732|2025-07-01T12:00:00|25.6||5|FM-15\n",
            "USW00014732|2025-07-01T13:00:00|99.9||C|FM-15\n",
        );
        Mock::given(method("GET"))
            .and(path("/2025/psv/GHCNh_USW00014732_2025.psv"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = GhcnhSource::connect(GhcnhSourceConfig {
            enabled: true,
            base_url: server.uri(),
            ..GhcnhSourceConfig::default()
        })
        .expect("source");
        let year = source
            .yearly_station(
                &IcaoStation::parse("KLGA").expect("station"),
                "USW00014732",
                2025,
                Utc.with_ymd_and_hms(2026, 7, 13, 0, 0, 0).unwrap(),
            )
            .await
            .expect("year request")
            .expect("published year");
        assert_eq!(year.reports.len(), 1);
        assert_eq!(year.reports[0].value, dec!(25.6));
    }

    #[tokio::test]
    async fn represents_without_empty_data() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/2026/psv/GHCNh_USW00014732_2026.psv"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;
        let source = GhcnhSource::connect(GhcnhSourceConfig {
            enabled: true,
            base_url: server.uri(),
            ..GhcnhSourceConfig::default()
        })
        .expect("source");
        let year = source
            .yearly_station(
                &IcaoStation::parse("KLGA").expect("station"),
                "USW00014732",
                2026,
                Utc.with_ymd_and_hms(2026, 7, 13, 0, 0, 0).unwrap(),
            )
            .await
            .expect("year request");
        assert!(year.is_none());
    }
}
