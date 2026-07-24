//! NOAA National Hurricane Center advisory and HURDAT2 adapters.

use std::{path::Path, str::FromStr, time::Duration};

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::NhcSourceConfig,
    domain::data_plane::{WeatherObservationReport, WeatherObservationReportKind},
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, WeatherVariable,
    },
};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::infra::{http::get_text_with_retry, retry::RetryPolicy};

/// NHC basin identity used by both current advisories and best tracks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NhcBasin {
    Atlantic,
    EasternPacific,
    CentralPacific,
}

impl NhcBasin {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Atlantic => "atlantic",
            Self::EasternPacific => "eastern_pacific",
            Self::CentralPacific => "central_pacific",
        }
    }

    const fn hurdat_prefix(self) -> &'static str {
        match self {
            Self::Atlantic => "hurdat2-1851-",
            Self::EasternPacific | Self::CentralPacific => "hurdat2-nepac-1949-",
        }
    }
}

/// Latest corrected HURDAT2 file projected to one storm.
pub struct NhcBestTrack {
    pub file_hash: ContentHash,
    pub collection_date: NaiveDate,
    pub reports: Vec<WeatherObservationReport>,
}

/// Public NHC data client.
pub struct NhcSource {
    config: NhcSourceConfig,
    http: Client,
    retry_policy: RetryPolicy,
}

impl NhcSource {
    pub fn connect(config: NhcSourceConfig) -> QuantResult<Self> {
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .user_agent("quant-pivot/0.1 nhc-ingest")
            .build()
            .map_err(|error| ApiError::Sdk(format!("NHC HTTP client: {error}")))?;
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

    /// Fetch all active storms that carry an official public advisory.
    pub async fn active_advisories(
        &self,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Vec<WeatherObservationReport>> {
        let body = get_text_with_retry(
            &self.http,
            &self.retry_policy,
            &self.config.current_storms_url,
        )
        .await
        .map_err(QuantError::from)?;
        parse_current_storms(&body, available_at)
    }

    /// Discover NHC's latest corrected HURDAT2 filename from the official data
    /// page and project one immutable storm track.
    pub async fn hurdat2_storm(
        &self,
        basin: NhcBasin,
        storm_id: &str,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Option<NhcBestTrack>> {
        validate_storm_id(storm_id)?;
        let archive = get_text_with_retry(
            &self.http,
            &self.retry_policy,
            &self.config.data_archive_url,
        )
        .await
        .map_err(QuantError::from)?;
        let (url, collection_date) = latest_hurdat_url(
            &archive,
            &self.config.data_archive_url,
            basin.hurdat_prefix(),
        )?;
        let body = get_text_with_retry(&self.http, &self.retry_policy, url.as_str())
            .await
            .map_err(QuantError::from)?;
        let file_hash = CanonicalDigest::content_hash_bytes(body.as_bytes());
        parse_hurdat2(&body, basin, storm_id, collection_date, available_at).map(|reports| {
            reports.map(|reports| NhcBestTrack {
                file_hash,
                collection_date,
                reports,
            })
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CurrentStormsWire {
    active_storms: Vec<CurrentStormWire>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CurrentStormWire {
    id: String,
    name: String,
    classification: String,
    intensity: String,
    last_update: DateTime<Utc>,
    public_advisory: Option<AdvisoryWire>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AdvisoryWire {
    adv_num: String,
    issuance: DateTime<Utc>,
    file_update_time: DateTime<Utc>,
    url: String,
}

#[derive(Serialize)]
struct CanonicalAdvisory<'a> {
    storm_id: &'a str,
    name: &'a str,
    classification: &'a str,
    intensity_knots: Decimal,
    last_update: DateTime<Utc>,
    advisory_number: &'a str,
    issuance: DateTime<Utc>,
    file_update_time: DateTime<Utc>,
    url: &'a str,
}

fn parse_current_storms(
    body: &str,
    available_at: DateTime<Utc>,
) -> QuantResult<Vec<WeatherObservationReport>> {
    let wire: CurrentStormsWire = serde_json::from_str(body)
        .map_err(|error| parse_error("NHC CurrentStorms JSON", error.to_string()))?;
    let mut reports = Vec::new();
    for storm in wire.active_storms {
        let Some(advisory) = storm.public_advisory else {
            continue;
        };
        let storm_id = storm.id.to_ascii_uppercase();
        validate_storm_id(&storm_id)?;
        let basin = basin_for_storm(&storm_id)?;
        let intensity = Decimal::from_str(&storm.intensity)
            .map_err(|error| parse_error("NHC CurrentStorms JSON", error.to_string()))?;
        if intensity < Decimal::ZERO || intensity > Decimal::from(250) {
            return Err(
                parse_error("NHC CurrentStorms JSON", "intensity outside 0..=250 knots").into(),
            );
        }
        if storm.last_update > advisory.issuance || advisory.file_update_time > available_at {
            return Err(parse_error(
                "NHC CurrentStorms JSON",
                "advisory timestamps are not PIT-valid",
            )
            .into());
        }
        let canonical = CanonicalAdvisory {
            storm_id: &storm_id,
            name: &storm.name,
            classification: &storm.classification,
            intensity_knots: intensity,
            last_update: storm.last_update,
            advisory_number: &advisory.adv_num,
            issuance: advisory.issuance,
            file_update_time: advisory.file_update_time,
            url: &advisory.url,
        };
        let report_hash = CanonicalDigest::content_hash_json(&canonical)?;
        let raw_report = serde_json::to_string(&canonical)
            .map_err(|error| parse_error("NHC advisory provenance", error.to_string()))?;
        reports.push(WeatherObservationReport {
            source_id: DomainSourceId::nhc_advisory(),
            instrument_key: DomainInstrumentKey::nhc_advisory(basin.as_str(), &storm_id),
            subject_key: storm_id,
            report_kind: WeatherObservationReportKind::NhcAdvisory,
            variable: WeatherVariable::CycloneIntensity,
            value: intensity,
            unit: DomainMeasurementUnit::Knot,
            precision: Decimal::ONE,
            observed_at: advisory.issuance,
            valid_from: Some(advisory.issuance),
            valid_to: None,
            published_at: advisory.file_update_time,
            available_at,
            report_hash,
            raw_report,
        });
    }
    reports.sort_by(|left, right| {
        left.observed_at
            .cmp(&right.observed_at)
            .then_with(|| left.subject_key.cmp(&right.subject_key))
    });
    Ok(reports)
}

fn latest_hurdat_url(
    archive: &str,
    archive_url: &str,
    prefix: &str,
) -> QuantResult<(Url, NaiveDate)> {
    let href = archive
        .split(['\"', '\''])
        .filter(|value| {
            value.rsplit('/').next().is_some_and(|filename| {
                filename.starts_with(prefix)
                    && Path::new(filename)
                        .extension()
                        .is_some_and(|extension| extension.eq_ignore_ascii_case("txt"))
            })
        })
        .max()
        .ok_or_else(|| parse_error("NHC data archive", format!("missing {prefix} HURDAT2 link")))?;
    let base = Url::parse(archive_url)
        .map_err(|error| parse_error("NHC data archive URL", error.to_string()))?;
    let url = base
        .join(href)
        .map_err(|error| parse_error("NHC HURDAT2 URL", error.to_string()))?;
    let filename = url
        .path_segments()
        .and_then(Iterator::last)
        .ok_or_else(|| parse_error("NHC HURDAT2 URL", "missing filename"))?;
    let date = filename
        .strip_suffix(".txt")
        .and_then(|value| value.rsplit('-').next())
        .ok_or_else(|| parse_error("NHC HURDAT2 filename", "missing collection date"))?;
    let collection_date = NaiveDate::parse_from_str(date, "%m%d%Y")
        .map_err(|error| parse_error("NHC HURDAT2 filename", error.to_string()))?;
    Ok((url, collection_date))
}

fn parse_hurdat2(
    body: &str,
    basin: NhcBasin,
    storm_id: &str,
    collection_date: NaiveDate,
    available_at: DateTime<Utc>,
) -> QuantResult<Option<Vec<WeatherObservationReport>>> {
    let published_at = collection_date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| parse_error("NHC HURDAT2", "invalid collection date"))?
        .and_utc();
    if published_at > available_at {
        return Err(parse_error("NHC HURDAT2", "collection is later than availability").into());
    }
    let target = storm_id.to_ascii_uppercase();
    let mut lines = body.lines();
    while let Some(header) = lines.next() {
        let fields = split_csv_line(header);
        if fields.len() < 3 {
            return Err(parse_error("NHC HURDAT2", "invalid storm header").into());
        }
        let count = fields[2]
            .parse::<usize>()
            .map_err(|error| parse_error("NHC HURDAT2", error.to_string()))?;
        let mut rows = Vec::with_capacity(count.min(256));
        for _ in 0..count {
            rows.push(
                lines
                    .next()
                    .ok_or_else(|| parse_error("NHC HURDAT2", "truncated storm rows"))?,
            );
        }
        if fields[0] != target {
            continue;
        }
        let mut reports = rows
            .into_iter()
            .filter_map(|row| {
                map_hurdat_row(row, basin, &target, published_at, available_at).transpose()
            })
            .collect::<QuantResult<Vec<_>>>()?;
        reports.sort_by_key(|report| report.observed_at);
        return Ok(Some(reports));
    }
    Ok(None)
}

fn map_hurdat_row(
    row: &str,
    basin: NhcBasin,
    storm_id: &str,
    published_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
) -> QuantResult<Option<WeatherObservationReport>> {
    let fields = split_csv_line(row);
    if fields.len() < 8 {
        return Err(parse_error("NHC HURDAT2", "best-track row has fewer than 8 fields").into());
    }
    validate_status(fields[3])?;
    validate_coordinate(fields[4], 'N', 'S')?;
    validate_coordinate(fields[5], 'E', 'W')?;
    let intensity = fields[6]
        .parse::<i16>()
        .map_err(|error| parse_error("NHC HURDAT2", error.to_string()))?;
    if intensity < 0 {
        return Ok(None);
    }
    if intensity > 250 {
        return Err(parse_error("NHC HURDAT2", "intensity exceeds 250 knots").into());
    }
    let observed_at =
        NaiveDateTime::parse_from_str(&format!("{} {}", fields[0], fields[1]), "%Y%m%d %H%M")
            .map(|value| value.and_utc())
            .map_err(|error| parse_error("NHC HURDAT2", error.to_string()))?;
    let report_hash = CanonicalDigest::content_hash_bytes(row.as_bytes());
    Ok(Some(WeatherObservationReport {
        source_id: DomainSourceId::nhc_hurdat2(),
        instrument_key: DomainInstrumentKey::nhc_hurdat2(basin.as_str(), storm_id),
        subject_key: storm_id.to_owned(),
        report_kind: WeatherObservationReportKind::NhcBestTrack,
        variable: WeatherVariable::CycloneIntensity,
        value: Decimal::from(intensity),
        unit: DomainMeasurementUnit::Knot,
        precision: Decimal::ONE,
        observed_at,
        valid_from: None,
        valid_to: None,
        published_at,
        available_at,
        report_hash,
        raw_report: row.to_owned(),
    }))
}

fn split_csv_line(line: &str) -> Vec<&str> {
    line.split(',').map(str::trim).collect()
}

fn basin_for_storm(storm_id: &str) -> QuantResult<NhcBasin> {
    match storm_id.get(..2) {
        Some("AL") => Ok(NhcBasin::Atlantic),
        Some("EP") => Ok(NhcBasin::EasternPacific),
        Some("CP") => Ok(NhcBasin::CentralPacific),
        _ => Err(parse_error("NHC storm id", "unsupported basin prefix").into()),
    }
}

fn validate_storm_id(storm_id: &str) -> QuantResult<()> {
    let bytes = storm_id.as_bytes();
    let valid = bytes.len() == 8
        && bytes[..2].iter().all(u8::is_ascii_uppercase)
        && bytes[2..].iter().all(u8::is_ascii_digit);
    if !valid {
        return Err(parse_error(
            "NHC storm id",
            "expected two uppercase letters and six digits",
        )
        .into());
    }
    basin_for_storm(storm_id).map(|_| ())
}

fn validate_status(value: &str) -> QuantResult<()> {
    if matches!(value, "TD" | "TS" | "HU" | "EX" | "SD" | "SS" | "LO" | "DB") {
        Ok(())
    } else {
        Err(parse_error("NHC HURDAT2", format!("unknown status `{value}`")).into())
    }
}

fn validate_coordinate(value: &str, positive: char, negative: char) -> QuantResult<()> {
    let suffix = value
        .chars()
        .last()
        .ok_or_else(|| parse_error("NHC HURDAT2", "empty coordinate"))?;
    if suffix != positive && suffix != negative {
        return Err(parse_error("NHC HURDAT2", "invalid coordinate hemisphere").into());
    }
    Decimal::from_str(&value[..value.len() - 1])
        .map(|_| ())
        .map_err(|error| parse_error("NHC HURDAT2", error.to_string()).into())
}

fn parse_error(context: &str, detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: context.to_owned(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use quant_pivot_models::{
        config::NhcSourceConfig, domain::data_plane::WeatherObservationReportKind,
    };
    use rust_decimal_macros::dec;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::{NhcBasin, NhcSource};

    #[tokio::test]
    async fn advisory_preserves_preposted_validity() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "activeStorms": [{
                    "id": "ep052026",
                    "name": "Elida",
                    "classification": "TS",
                    "intensity": "55",
                    "lastUpdate": "2026-07-18T09:00:00Z",
                    "publicAdvisory": {
                        "advNum": "013",
                        "issuance": "2026-07-18T09:00:00Z",
                        "fileUpdateTime": "2026-07-18T08:34:37.220Z",
                        "url": "https://www.nhc.noaa.gov/text/MIATCPEP5.shtml"
                    }
                }]
            })))
            .mount(&server)
            .await;
        let source = NhcSource::connect(NhcSourceConfig {
            current_storms_url: server.uri(),
            ..NhcSourceConfig::default()
        })
        .expect("source");
        let reports = source
            .active_advisories(Utc.with_ymd_and_hms(2026, 7, 18, 8, 40, 0).unwrap())
            .await
            .expect("advisories");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].value, dec!(55));
        assert_eq!(
            reports[0].observed_at,
            Utc.with_ymd_and_hms(2026, 7, 18, 9, 0, 0).unwrap()
        );
        assert_eq!(reports[0].valid_from, Some(reports[0].observed_at));
        assert_eq!(
            reports[0].published_at,
            "2026-07-18T08:34:37.220Z"
                .parse::<DateTime<Utc>>()
                .expect("published timestamp")
        );
        assert_eq!(
            reports[0].available_at,
            Utc.with_ymd_and_hms(2026, 7, 18, 8, 40, 0).unwrap()
        );
        assert_eq!(
            reports[0].report_kind,
            WeatherObservationReportKind::NhcAdvisory
        );
    }

    #[tokio::test]
    async fn discovers_latest_projects_storm() {
        let server = MockServer::start().await;
        let archive = format!(
            "<a href=\"{}/data/hurdat/hurdat2-1851-2025-02272026.txt\">latest</a>",
            server.uri()
        );
        Mock::given(method("GET"))
            .and(path("/data/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(archive))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/data/hurdat/hurdat2-1851-2025-02272026.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_string(concat!(
                "AL092021, IDA, 2,\n",
                "20210826, 1200,  , TD, 16.5N, 78.9W, 30, 1006,\n",
                "20210829, 1600, L, HU, 29.2N, 90.2W, 130, 930,\n",
            )))
            .mount(&server)
            .await;
        let source = NhcSource::connect(NhcSourceConfig {
            data_archive_url: format!("{}/data/", server.uri()),
            ..NhcSourceConfig::default()
        })
        .expect("source");
        let track = source
            .hurdat2_storm(NhcBasin::Atlantic, "AL092021", Utc::now())
            .await
            .expect("HURDAT2")
            .expect("storm");
        assert_eq!(track.reports.len(), 2);
        assert_eq!(track.reports[1].value, dec!(130));
        assert_eq!(
            track.reports[1].report_kind,
            WeatherObservationReportKind::NhcBestTrack
        );
    }
}
