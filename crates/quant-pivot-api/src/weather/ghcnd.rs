//! NOAA `GHCNd` archive-quality daily-temperature adapter.

use std::{collections::BTreeSet, ops::Range, str, time::Duration};

use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use chrono_tz::Tz;
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::GhcndSourceConfig,
    domain::data_plane::{WeatherObservationReport, WeatherObservationReportKind},
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, IcaoStation,
        WeatherTemperatureStatistic, WeatherVariable,
    },
};
use reqwest::Client;
use rust_decimal::Decimal;

use crate::infra::{http::get_optional_bounded_bytes, retry::RetryPolicy};

const MAX_STATION_FILE_BYTES: usize = 32 * 1024 * 1024;
const HEADER_BYTES: usize = 21;
const DAY_BYTES: usize = 8;
const DAYS_PER_ROW: usize = 31;
const ROW_BYTES: usize = HEADER_BYTES + DAYS_PER_ROW * DAY_BYTES;

/// One immutable station-file revision and its accepted daily extrema.
pub struct GhcndStation {
    pub file_hash: ContentHash,
    pub reports: Vec<WeatherObservationReport>,
}

/// Station-scoped `GHCNd` source.
pub struct GhcndSource {
    config: GhcndSourceConfig,
    http: Client,
    retry_policy: RetryPolicy,
}

impl GhcndSource {
    pub fn connect(config: GhcndSourceConfig) -> QuantResult<Self> {
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .user_agent("quant-pivot/0.1 ghcnd-resolution")
            .build()
            .map_err(|error| ApiError::Sdk(format!("GHCNd HTTP client: {error}")))?;
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

    /// Download and decode one official station file.
    pub async fn station_daily(
        &self,
        station: &IcaoStation,
        ghcnd_station_id: &str,
        timezone: Tz,
        first_year: i32,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Option<GhcndStation>> {
        validate_station_id(ghcnd_station_id)?;
        if first_year > available_at.year() {
            return Err(parse_error("first_year is after the download timestamp").into());
        }
        let url = format!(
            "{}/all/{ghcnd_station_id}.dly",
            self.config.base_url.trim_end_matches('/')
        );
        let Some(body) = get_optional_bounded_bytes(
            &self.http,
            &self.retry_policy,
            &url,
            "NOAA GHCNd station file",
            MAX_STATION_FILE_BYTES,
        )
        .await
        .map_err(QuantError::from)?
        else {
            return Ok(None);
        };
        parse_station_file(
            station,
            ghcnd_station_id,
            timezone,
            first_year,
            available_at,
            &body,
        )
        .map(Some)
    }
}

fn parse_station_file(
    station: &IcaoStation,
    ghcnd_station_id: &str,
    timezone: Tz,
    first_year: i32,
    available_at: DateTime<Utc>,
    body: &[u8],
) -> QuantResult<GhcndStation> {
    let file_hash = CanonicalDigest::content_hash_bytes(body);
    let text = str::from_utf8(body)
        .map_err(|error| parse_error(format!("station file is not UTF-8 ASCII: {error}")))?;
    let mut reports = Vec::new();
    let mut identities = BTreeSet::new();
    for raw_line in text.lines() {
        let line = raw_line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        if line.len() != ROW_BYTES || !line.is_ascii() {
            return Err(parse_error(format!(
                "station row must contain exactly {ROW_BYTES} ASCII bytes"
            ))
            .into());
        }
        if line.get(0..11) != Some(ghcnd_station_id) {
            return Err(parse_error("row station does not match frozen GHCNd binding").into());
        }
        let year = parse_i32(line, 11..15, "year")?;
        if year < first_year {
            continue;
        }
        let month = parse_u32(line, 15..17, "month")?;
        let statistic = match line.get(17..21) {
            Some("TMAX") => WeatherTemperatureStatistic::Maximum,
            Some("TMIN") => WeatherTemperatureStatistic::Minimum,
            Some(_) => continue,
            None => return Err(parse_error("row has no element").into()),
        };
        for day_index in 0..DAYS_PER_ROW {
            let offset = HEADER_BYTES + day_index * DAY_BYTES;
            let value = parse_i32(line, offset..offset + 5, "value")?;
            if value == -9_999 {
                continue;
            }
            let day = u32::try_from(day_index + 1)
                .map_err(|error| parse_error(format!("day index overflow: {error}")))?;
            let local_date = NaiveDate::from_ymd_opt(year, month, day)
                .ok_or_else(|| parse_error("non-missing value has an invalid calendar date"))?;
            let quality_flag = line
                .get(offset + 6..offset + 7)
                .ok_or_else(|| parse_error("row has no quality flag"))?;
            if quality_flag != " " {
                continue;
            }
            if !identities.insert((local_date, statistic)) {
                return Err(
                    parse_error(format!("duplicate {statistic:?} row for {local_date}")).into(),
                );
            }
            let start_at = timezone
                .from_local_datetime(
                    &local_date
                        .and_hms_opt(0, 0, 0)
                        .ok_or_else(|| parse_error("invalid local-day start"))?,
                )
                .earliest()
                .ok_or_else(|| parse_error("local-day start is not representable"))?
                .with_timezone(&Utc);
            let next_date = local_date
                .succ_opt()
                .ok_or_else(|| parse_error("local-day end overflow"))?;
            let end_at = timezone
                .from_local_datetime(
                    &next_date
                        .and_hms_opt(0, 0, 0)
                        .ok_or_else(|| parse_error("invalid local-day end"))?,
                )
                .earliest()
                .ok_or_else(|| parse_error("local-day end is not representable"))?
                .with_timezone(&Utc);
            let value = Decimal::new(i64::from(value), 1);
            let report_hash = CanonicalDigest::content_hash_json(&(
                "ghcnd_daily_temperature_v1",
                ghcnd_station_id,
                local_date,
                statistic,
                value,
                line,
            ))?;
            reports.push(WeatherObservationReport {
                source_id: DomainSourceId::ghcnd(),
                instrument_key: DomainInstrumentKey::ghcnd_temperature(station, statistic),
                subject_key: station.to_string(),
                report_kind: WeatherObservationReportKind::GhcndDailyTemperature,
                variable: match statistic {
                    WeatherTemperatureStatistic::Maximum => WeatherVariable::TemperatureMaximum,
                    WeatherTemperatureStatistic::Minimum => WeatherVariable::TemperatureMinimum,
                },
                value,
                unit: DomainMeasurementUnit::Celsius,
                precision: Decimal::new(1, 1),
                observed_at: end_at,
                valid_from: Some(start_at),
                valid_to: Some(end_at),
                published_at: available_at,
                available_at,
                report_hash,
                raw_report: format!("{line}|day={day}"),
            });
        }
    }
    reports.sort_by(|left, right| {
        left.observed_at
            .cmp(&right.observed_at)
            .then_with(|| left.instrument_key.cmp(&right.instrument_key))
            .then_with(|| left.report_hash.cmp(&right.report_hash))
    });
    Ok(GhcndStation { file_hash, reports })
}

fn validate_station_id(value: &str) -> QuantResult<()> {
    let valid = value.len() == 11
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'-');
    if !valid {
        return Err(parse_error(
            "GHCNd station id must be 11 uppercase ASCII letters/digits/hyphen",
        )
        .into());
    }
    Ok(())
}

fn parse_i32(line: &str, range: Range<usize>, field: &'static str) -> QuantResult<i32> {
    line.get(range)
        .ok_or_else(|| parse_error(format!("row has no {field}")))?
        .trim()
        .parse::<i32>()
        .map_err(|error| parse_error(format!("invalid {field}: {error}")).into())
}

fn parse_u32(line: &str, range: Range<usize>, field: &'static str) -> QuantResult<u32> {
    line.get(range)
        .ok_or_else(|| parse_error(format!("row has no {field}")))?
        .trim()
        .parse::<u32>()
        .map_err(|error| parse_error(format!("invalid {field}: {error}")).into())
}

fn parse_error(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "NOAA GHCNd DLY".into(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use chrono::{TimeZone, Utc};
    use chrono_tz::America::New_York;
    use quant_pivot_models::{
        config::GhcndSourceConfig,
        types::{DomainInstrumentKey, IcaoStation, WeatherTemperatureStatistic},
    };
    use rust_decimal_macros::dec;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::GhcndSource;

    fn monthly_row(element: &str, first_value: i32, first_quality: char) -> String {
        let mut row = format!("USW00014732202607{element}");
        for day in 0..31 {
            let (value, quality) = if day == 0 {
                (first_value, first_quality)
            } else {
                (-9_999, ' ')
            };
            write!(&mut row, "{value:>5} {quality} ")
                .expect("writing a GHCNd fixture row to String is infallible");
        }
        row
    }

    #[tokio::test]
    async fn parses_daily_extrema() {
        let server = MockServer::start().await;
        let body = format!(
            "{}\n{}\n{}",
            monthly_row("TMAX", 312, ' '),
            monthly_row("TMIN", 201, ' '),
            monthly_row("PRCP", 10, ' ')
        );
        Mock::given(method("GET"))
            .and(path("/all/USW00014732.dly"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = GhcndSource::connect(GhcndSourceConfig {
            base_url: server.uri(),
            ..GhcndSourceConfig::default()
        })
        .expect("source");
        let station = source
            .station_daily(
                &IcaoStation::parse("KLGA").expect("station"),
                "USW00014732",
                New_York,
                2026,
                Utc.with_ymd_and_hms(2026, 7, 3, 0, 0, 0).unwrap(),
            )
            .await
            .expect("request")
            .expect("station file");

        assert_eq!(station.reports.len(), 2);
        assert_eq!(station.reports[0].value, dec!(31.2));
        assert_eq!(station.reports[1].value, dec!(20.1));
        assert_eq!(
            station.reports[0].instrument_key,
            DomainInstrumentKey::ghcnd_temperature(
                &IcaoStation::parse("KLGA").expect("station"),
                WeatherTemperatureStatistic::Maximum,
            )
        );
        assert_eq!(
            station.reports[0].valid_to,
            Some(Utc.with_ymd_and_hms(2026, 7, 2, 4, 0, 0).unwrap())
        );
    }

    #[tokio::test]
    async fn excludes_failed_quality() {
        let server = MockServer::start().await;
        let body = monthly_row("TMAX", 999, 'X');
        Mock::given(method("GET"))
            .and(path("/all/USW00014732.dly"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = GhcndSource::connect(GhcndSourceConfig {
            base_url: server.uri(),
            ..GhcndSourceConfig::default()
        })
        .expect("source");
        let station = source
            .station_daily(
                &IcaoStation::parse("KLGA").expect("station"),
                "USW00014732",
                New_York,
                2026,
                Utc.with_ymd_and_hms(2026, 7, 3, 0, 0, 0).unwrap(),
            )
            .await
            .expect("request")
            .expect("station file");

        assert!(station.reports.is_empty());
    }
}
