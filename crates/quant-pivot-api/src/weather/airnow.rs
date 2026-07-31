//! EPA `AirNow` PM2.5 reporting-area and exact-site adapter.

use std::{collections::BTreeMap, str::FromStr, time::Duration};

use chrono::{
    DateTime, Duration as ChronoDuration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike,
    Utc,
};
use chrono_tz::Tz;
use csv::{ReaderBuilder, StringRecord};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::{AirNowPm25SiteBindingConfig, AirNowSourceConfig},
    domain::data_plane::{
        WeatherForecastPoint, WeatherObservationReport, WeatherObservationReportKind,
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, WeatherVariable,
    },
};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::Serialize;

use crate::infra::{
    http::{get_optional_bytes, get_text_with_retry},
    retry::RetryPolicy,
};

const FIELD_COUNT: usize = 17;
const HOURLY_AQ_FIELD_COUNT: usize = 34;

/// PIT snapshot for one unambiguous `(reporting area, state, timezone)` binding.
pub struct AirNowPm25ReportingAreaSnapshot {
    pub file_hash: ContentHash,
    pub observations: Vec<WeatherObservationReport>,
    pub forecasts: Vec<WeatherForecastPoint>,
}

/// Public nationwide `AirNow` file client. `AirNow` facts remain explicitly
/// preliminary; they are never treated as regulatory AQS observations.
pub struct AirNowSource {
    config: AirNowSourceConfig,
    http: Client,
    retry_policy: RetryPolicy,
}

impl AirNowSource {
    pub fn connect(config: AirNowSourceConfig) -> QuantResult<Self> {
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .user_agent("quant-pivot/0.1 airnow-ingest")
            .build()
            .map_err(|error| ApiError::Sdk(format!("AirNow HTTP client: {error}")))?;
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

    /// Fetch current observation, previous-day maximum, and numeric forecasts
    /// for one frozen reporting area. Category-only forecasts remain absent
    /// rather than being converted to invented AQI values.
    pub async fn pm25_reporting_area(
        &self,
        area: &str,
        state: &str,
        timezone: Tz,
        available_at: DateTime<Utc>,
    ) -> QuantResult<AirNowPm25ReportingAreaSnapshot> {
        validate_binding(area, state)?;
        let body = get_text_with_retry(
            &self.http,
            &self.retry_policy,
            &self.config.reporting_area_url,
        )
        .await
        .map_err(QuantError::from)?;
        parse_pm25_reporting_area(&body, area, state, timezone, available_at)
    }

    /// Fetch one hourly site-level AQI file and aggregate PM2.5 AQI across the
    /// exact reporting area. Callers re-read the configured correction window;
    /// equal event times remain distinct through the content hash.
    pub async fn hourly_pm25_area_observation(
        &self,
        area: &str,
        state: &str,
        hour: DateTime<Utc>,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Option<WeatherObservationReport>> {
        validate_binding(area, state)?;
        let hour = validate_hour(hour, available_at)?;
        let Some(body) = self.hourly_file(hour).await? else {
            return Ok(None);
        };
        parse_area_pm25(&body, area, state, hour, available_at)
    }

    /// Fetch one exact monitoring site's preliminary PM2.5 AQI observation.
    pub async fn hourly_pm25_site_observation(
        &self,
        binding: &AirNowPm25SiteBindingConfig,
        hour: DateTime<Utc>,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Option<WeatherObservationReport>> {
        validate_site_binding(binding)?;
        let hour = validate_hour(hour, available_at)?;
        let Some(body) = self.hourly_file(hour).await? else {
            return Ok(None);
        };
        parse_site_pm25(&body, binding, hour, available_at)
    }

    async fn hourly_file(&self, hour: DateTime<Utc>) -> QuantResult<Option<String>> {
        let url = format!(
            "{}/{}/{}/{}",
            self.config.hourly_aq_base_url.trim_end_matches('/'),
            hour.format("%Y"),
            hour.format("%Y%m%d"),
            hour.format("HourlyAQObs_%Y%m%d%H.dat")
        );
        let Some(bytes) = get_optional_bytes(&self.http, &self.retry_policy, &url)
            .await
            .map_err(QuantError::from)?
        else {
            return Ok(None);
        };
        String::from_utf8(bytes)
            .map(Some)
            .map_err(|error| ApiError::Deserialize {
                context: "AirNow HourlyAQObs encoding".to_owned(),
                detail: error.to_string(),
            })
            .map_err(QuantError::from)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum AirNowDataType {
    Observation,
    Yesterday,
    Forecast,
}

#[derive(Debug, Clone)]
struct AirNowRecord {
    issue_date: NaiveDate,
    valid_date: NaiveDate,
    valid_time: Option<NaiveTime>,
    data_type: AirNowDataType,
    aqi: Option<Decimal>,
    raw: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GroupKey {
    data_type: AirNowDataType,
    valid_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct CanonicalAirNowGroup<'a> {
    area: &'a str,
    state: &'a str,
    timezone: &'a str,
    data_type: &'a str,
    valid_at: DateTime<Utc>,
    aqi: Decimal,
    records: &'a [String],
}

#[derive(Serialize)]
struct CanonicalAirNowPm25Site<'a> {
    contract_location: &'a str,
    primary_resolution_url: &'a str,
    aqsid: &'a str,
    site_name: &'a str,
    state: &'a str,
    latitude: Decimal,
    longitude: Decimal,
    valid_at: DateTime<Utc>,
    pm25_aqi: Decimal,
    row: &'a str,
}

fn parse_pm25_reporting_area(
    body: &str,
    area: &str,
    state: &str,
    timezone: Tz,
    available_at: DateTime<Utc>,
) -> QuantResult<AirNowPm25ReportingAreaSnapshot> {
    let file_hash = CanonicalDigest::content_hash_bytes(body.as_bytes());
    let records = matching_records(body, area, state)?;
    let mut groups = BTreeMap::<GroupKey, Vec<AirNowRecord>>::new();
    for record in records {
        let valid_at = record_valid_at(&record, timezone)?;
        if valid_at > available_at && record.data_type != AirNowDataType::Forecast {
            return Err(parse_error("observation valid time is later than availability").into());
        }
        groups
            .entry(GroupKey {
                data_type: record.data_type,
                valid_at,
            })
            .or_default()
            .push(record);
    }
    let area_key = format!("{state}:{area}");
    let binding_hash = CanonicalDigest::content_hash_json(&(
        "airnow_pm25_reporting_area_v2",
        area,
        state,
        timezone.name(),
    ))?;
    let mut observations = Vec::new();
    let mut forecasts = Vec::new();
    for (key, records) in groups {
        let Some(aqi) = records.iter().filter_map(|record| record.aqi).max() else {
            continue;
        };
        let mut raw_records = records
            .iter()
            .map(|record| record.raw.clone())
            .collect::<Vec<_>>();
        raw_records.sort();
        let data_type = match key.data_type {
            AirNowDataType::Observation => "observation",
            AirNowDataType::Yesterday => "yesterday_maximum",
            AirNowDataType::Forecast => "forecast",
        };
        let canonical = CanonicalAirNowGroup {
            area,
            state,
            timezone: timezone.name(),
            data_type,
            valid_at: key.valid_at,
            aqi,
            records: &raw_records,
        };
        let report_hash = CanonicalDigest::content_hash_json(&canonical)?;
        let raw_report =
            serde_json::to_string(&canonical).map_err(|error| ApiError::Deserialize {
                context: "AirNow provenance".to_owned(),
                detail: error.to_string(),
            })?;
        if key.data_type == AirNowDataType::Forecast {
            let issue_date = records
                .iter()
                .map(|record| record.issue_date)
                .max()
                .ok_or_else(|| parse_error("empty forecast group"))?;
            let reference_time = local_midnight(issue_date, timezone)?;
            forecasts.push(WeatherForecastPoint {
                source_id: DomainSourceId::airnow(),
                instrument_key: DomainInstrumentKey::airnow_pm25_forecast(&area_key),
                subject_key: area_key.clone(),
                variable: WeatherVariable::Aqi,
                value: aqi,
                unit: DomainMeasurementUnit::Aqi,
                precision: Decimal::ONE,
                reference_time,
                valid_time: key.valid_at,
                published_at: available_at,
                available_at,
                lead_hours: forecast_lead_hours(reference_time, key.valid_at)?,
                member: None,
                revision: 0,
                grid_binding_hash: binding_hash,
                run_manifest_hash: file_hash,
                report_hash,
            });
        } else {
            let (valid_from, valid_to) = if key.data_type == AirNowDataType::Yesterday {
                let date = records
                    .first()
                    .map(|record| record.valid_date)
                    .ok_or_else(|| parse_error("empty yesterday group"))?;
                let start = local_midnight(date, timezone)?;
                (Some(start), Some(key.valid_at))
            } else {
                (None, None)
            };
            let report_kind = if key.data_type == AirNowDataType::Yesterday {
                WeatherObservationReportKind::AirNowPm25OfficialDaily
            } else {
                WeatherObservationReportKind::AirNowPm25AreaObservation
            };
            observations.push(WeatherObservationReport {
                source_id: DomainSourceId::airnow(),
                instrument_key: DomainInstrumentKey::airnow_pm25_observation(&area_key),
                subject_key: area_key.clone(),
                report_kind,
                variable: WeatherVariable::Aqi,
                value: aqi,
                unit: DomainMeasurementUnit::Aqi,
                precision: Decimal::ONE,
                observed_at: key.valid_at,
                valid_from,
                valid_to,
                published_at: available_at,
                available_at,
                report_hash,
                raw_report,
            });
        }
    }
    observations.sort_by_key(|report| report.observed_at);
    forecasts.sort_by_key(|point| point.valid_time);
    Ok(AirNowPm25ReportingAreaSnapshot {
        file_hash,
        observations,
        forecasts,
    })
}

fn parse_area_pm25(
    body: &str,
    area: &str,
    state: &str,
    hour: DateTime<Utc>,
    available_at: DateTime<Utc>,
) -> QuantResult<Option<WeatherObservationReport>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .from_reader(body.as_bytes());
    let mut rows = Vec::new();
    let mut peak = None::<Decimal>;
    for row in reader.records() {
        let row = row.map_err(|error| parse_error(format!("invalid hourly row: {error}")))?;
        if row.len() != HOURLY_AQ_FIELD_COUNT {
            return Err(parse_error(format!(
                "expected {HOURLY_AQ_FIELD_COUNT} hourly fields, received {}",
                row.len()
            ))
            .into());
        }
        if row.get(9) != Some(state)
            || !row
                .get(13)
                .unwrap_or_default()
                .split('|')
                .any(|candidate| candidate == area)
        {
            continue;
        }
        let observed_at = parse_hourly_utc(
            required(&row, 10, "hourly valid date")?,
            required(&row, 11, "hourly valid time")?,
        )?;
        if observed_at != hour {
            return Err(parse_error("hourly row does not match requested UTC partition").into());
        }
        if let Some(value) = parse_optional_aqi(row.get(16), "hourly PM2.5")? {
            peak = Some(peak.map_or(value, |current| current.max(value)));
        }
        rows.push(row.iter().collect::<Vec<_>>().join(","));
    }
    let Some(value) = peak else {
        return Ok(None);
    };
    rows.sort();
    let area_key = format!("{state}:{area}");
    let report_hash = CanonicalDigest::content_hash_json(&(
        "airnow_hourly_pm25_area_obs_v2",
        &area_key,
        hour,
        value,
        &rows,
    ))?;
    let raw_report = serde_json::to_string(&rows).map_err(|error| ApiError::Deserialize {
        context: "AirNow hourly provenance".to_owned(),
        detail: error.to_string(),
    })?;
    Ok(Some(WeatherObservationReport {
        source_id: DomainSourceId::airnow(),
        instrument_key: DomainInstrumentKey::airnow_pm25_observation(&area_key),
        subject_key: area_key,
        report_kind: WeatherObservationReportKind::AirNowPm25AreaObservation,
        variable: WeatherVariable::Aqi,
        value,
        unit: DomainMeasurementUnit::Aqi,
        precision: Decimal::ONE,
        observed_at: hour,
        valid_from: Some(hour),
        valid_to: hour.checked_add_signed(ChronoDuration::hours(1)),
        published_at: available_at,
        available_at,
        report_hash,
        raw_report,
    }))
}

fn parse_site_pm25(
    body: &str,
    binding: &AirNowPm25SiteBindingConfig,
    hour: DateTime<Utc>,
    available_at: DateTime<Utc>,
) -> QuantResult<Option<WeatherObservationReport>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .from_reader(body.as_bytes());
    let mut matched = None;
    for row in reader.records() {
        let row = row.map_err(|error| parse_error(format!("invalid hourly row: {error}")))?;
        if row.len() != HOURLY_AQ_FIELD_COUNT {
            return Err(parse_error(format!(
                "expected {HOURLY_AQ_FIELD_COUNT} hourly fields, received {}",
                row.len()
            ))
            .into());
        }
        if row.get(0) != Some(binding.aqsid.as_str()) {
            continue;
        }
        if matched.is_some() {
            return Err(parse_error(format!(
                "AirNow hourly partition contains duplicate AQSID {}",
                binding.aqsid
            ))
            .into());
        }
        validate_site_row(&row, binding, hour)?;
        matched = Some(row);
    }
    let Some(row) = matched else {
        return Ok(None);
    };
    let Some(value) = parse_optional_aqi(row.get(16), "site PM2.5")? else {
        return Ok(None);
    };
    let raw_row = row.iter().collect::<Vec<_>>().join(",");
    let canonical = CanonicalAirNowPm25Site {
        contract_location: &binding.contract_location,
        primary_resolution_url: &binding.primary_resolution_url,
        aqsid: &binding.aqsid,
        site_name: &binding.site_name,
        state: &binding.state,
        latitude: binding.latitude,
        longitude: binding.longitude,
        valid_at: hour,
        pm25_aqi: value,
        row: &raw_row,
    };
    let report_hash = CanonicalDigest::content_hash_json(&canonical)?;
    let raw_report = serde_json::to_string(&canonical).map_err(|error| ApiError::Deserialize {
        context: "AirNow PM2.5 site provenance".to_owned(),
        detail: error.to_string(),
    })?;
    Ok(Some(WeatherObservationReport {
        source_id: DomainSourceId::airnow(),
        instrument_key: DomainInstrumentKey::airnow_pm25_site(&binding.aqsid),
        subject_key: binding.aqsid.clone(),
        report_kind: WeatherObservationReportKind::AirNowPm25SiteObservation,
        variable: WeatherVariable::Aqi,
        value,
        unit: DomainMeasurementUnit::Aqi,
        precision: Decimal::ONE,
        observed_at: hour,
        valid_from: Some(hour),
        valid_to: hour.checked_add_signed(ChronoDuration::hours(1)),
        published_at: available_at,
        available_at,
        report_hash,
        raw_report,
    }))
}

fn validate_site_row(
    row: &StringRecord,
    binding: &AirNowPm25SiteBindingConfig,
    hour: DateTime<Utc>,
) -> QuantResult<()> {
    if row.get(1) != Some(binding.site_name.as_str())
        || row.get(2) != Some("Active")
        || row.get(9) != Some(binding.state.as_str())
    {
        return Err(parse_error(format!(
            "AirNow AQSID {} identity/status drifted from the frozen binding",
            binding.aqsid
        ))
        .into());
    }
    let latitude = Decimal::from_str(required(row, 4, "site latitude")?)
        .map_err(|error| parse_error(format!("invalid site latitude: {error}")))?;
    let longitude = Decimal::from_str(required(row, 5, "site longitude")?)
        .map_err(|error| parse_error(format!("invalid site longitude: {error}")))?;
    if latitude != binding.latitude || longitude != binding.longitude {
        return Err(parse_error(format!(
            "AirNow AQSID {} coordinates drifted from the frozen binding",
            binding.aqsid
        ))
        .into());
    }
    let observed_at = parse_hourly_utc(
        required(row, 10, "hourly valid date")?,
        required(row, 11, "hourly valid time")?,
    )?;
    if observed_at != hour {
        return Err(parse_error("site row does not match requested UTC partition").into());
    }
    Ok(())
}

fn matching_records(body: &str, area: &str, state: &str) -> QuantResult<Vec<AirNowRecord>> {
    let mut reader = ReaderBuilder::new()
        .delimiter(b'|')
        .has_headers(false)
        .flexible(false)
        .from_reader(body.as_bytes());
    let mut records = Vec::new();
    for row in reader.records() {
        let row = row.map_err(|error| parse_error(format!("invalid row: {error}")))?;
        if row.len() != FIELD_COUNT {
            return Err(parse_error(format!(
                "expected {FIELD_COUNT} fields, received {}",
                row.len()
            ))
            .into());
        }
        if row.get(7) != Some(area) || row.get(8) != Some(state) || row.get(11) != Some("PM2.5") {
            continue;
        }
        records.push(parse_record(&row)?);
    }
    Ok(records)
}

fn parse_record(row: &StringRecord) -> QuantResult<AirNowRecord> {
    let issue_date = parse_date(required(row, 0, "issue date")?)?;
    let valid_date = parse_date(required(row, 1, "valid date")?)?;
    let valid_time = row
        .get(2)
        .filter(|value| !value.is_empty())
        .map(|value| NaiveTime::parse_from_str(value, "%H:%M"))
        .transpose()
        .map_err(|error| parse_error(format!("invalid valid time: {error}")))?;
    let record_sequence = required(row, 4, "record sequence")?
        .parse::<i16>()
        .map_err(|error| parse_error(format!("invalid record sequence: {error}")))?;
    let data_type = match required(row, 5, "data type")? {
        "O" if record_sequence == 0 && valid_time.is_some() => AirNowDataType::Observation,
        "Y" if record_sequence == -1 && valid_time.is_none() => AirNowDataType::Yesterday,
        "F" if record_sequence >= 0 && valid_time.is_none() => AirNowDataType::Forecast,
        value => {
            return Err(parse_error(format!(
                "invalid type/sequence/time combination `{value}`/{record_sequence}"
            ))
            .into());
        }
    };
    let aqi = parse_optional_aqi(row.get(12), "reporting-area")?;
    Ok(AirNowRecord {
        issue_date,
        valid_date,
        valid_time,
        data_type,
        aqi,
        raw: row.iter().collect::<Vec<_>>().join("|"),
    })
}

fn record_valid_at(record: &AirNowRecord, timezone: Tz) -> QuantResult<DateTime<Utc>> {
    match record.data_type {
        AirNowDataType::Observation => local_datetime(
            NaiveDateTime::new(
                record.valid_date,
                record
                    .valid_time
                    .ok_or_else(|| parse_error("observation has no valid time"))?,
            ),
            timezone,
        ),
        AirNowDataType::Yesterday => {
            let next = record
                .valid_date
                .succ_opt()
                .ok_or_else(|| parse_error("yesterday date overflow"))?;
            local_midnight(next, timezone)
        }
        AirNowDataType::Forecast => local_midnight(record.valid_date, timezone),
    }
}

fn local_midnight(date: NaiveDate, timezone: Tz) -> QuantResult<DateTime<Utc>> {
    local_datetime(
        date.and_hms_opt(0, 0, 0)
            .ok_or_else(|| parse_error("invalid local midnight"))?,
        timezone,
    )
}

fn local_datetime(value: NaiveDateTime, timezone: Tz) -> QuantResult<DateTime<Utc>> {
    timezone
        .from_local_datetime(&value)
        .single()
        .map(|value| value.to_utc())
        .ok_or_else(|| {
            parse_error(format!(
                "ambiguous/nonexistent local time {value} in {timezone}"
            ))
        })
        .map_err(QuantError::from)
}

fn forecast_lead_hours(reference: DateTime<Utc>, valid: DateTime<Utc>) -> QuantResult<u16> {
    let hours = valid.signed_duration_since(reference).num_hours().max(0);
    u16::try_from(hours).map_err(|_| parse_error("forecast lead exceeds u16").into())
}

fn parse_date(value: &str) -> QuantResult<NaiveDate> {
    NaiveDate::parse_from_str(value, "%m/%d/%y")
        .map_err(|error| parse_error(format!("invalid date: {error}")).into())
}

fn parse_hourly_utc(date: &str, time: &str) -> QuantResult<DateTime<Utc>> {
    let value = format!("{date} {time}");
    let format = match date.rsplit('/').next().map(str::len) {
        Some(4) => "%m/%d/%Y %H:%M",
        Some(2) => "%m/%d/%y %H:%M",
        _ => return Err(parse_error(format!("invalid hourly UTC timestamp `{value}`")).into()),
    };
    NaiveDateTime::parse_from_str(&value, format)
        .ok()
        .map(|value| value.and_utc())
        .ok_or_else(|| parse_error(format!("invalid hourly UTC timestamp `{value}`")).into())
}

fn validate_hour(hour: DateTime<Utc>, available_at: DateTime<Utc>) -> QuantResult<DateTime<Utc>> {
    let hour = hour
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .ok_or_else(|| parse_error("invalid UTC hour"))?;
    if hour > available_at {
        return Err(parse_error("hour is later than availability").into());
    }
    Ok(hour)
}

fn required<'a>(row: &'a StringRecord, index: usize, name: &str) -> QuantResult<&'a str> {
    row.get(index)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| parse_error(format!("missing {name}")).into())
}

fn parse_optional_aqi(value: Option<&str>, field: &str) -> QuantResult<Option<Decimal>> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let value = Decimal::from_str(value)
        .map_err(|error| parse_error(format!("invalid {field} AQI: {error}")))?;
    if value == Decimal::from(-999) {
        return Ok(None);
    }
    if value < Decimal::ZERO {
        return Err(parse_error(format!(
            "{field} AQI must be non-negative or the -999 missing-value sentinel"
        ))
        .into());
    }
    Ok(Some(value))
}

fn validate_binding(area: &str, state: &str) -> QuantResult<()> {
    let area_valid = !area.trim().is_empty()
        && area.len() <= 128
        && !area.contains(':')
        && !area.chars().any(char::is_control);
    let state_valid = state.len() == 2 && state.bytes().all(|byte| byte.is_ascii_uppercase());
    if !area_valid || !state_valid {
        return Err(parse_error("invalid AirNow area/state binding").into());
    }
    Ok(())
}

fn validate_site_binding(binding: &AirNowPm25SiteBindingConfig) -> QuantResult<()> {
    let valid = binding.aqsid.len() == 12
        && binding.aqsid.bytes().all(|byte| byte.is_ascii_digit())
        && !binding.site_name.trim().is_empty()
        && binding.state.len() == 2
        && binding.state.bytes().all(|byte| byte.is_ascii_uppercase())
        && binding.latitude >= Decimal::from(-90)
        && binding.latitude <= Decimal::from(90)
        && binding.longitude >= Decimal::from(-180)
        && binding.longitude <= Decimal::from(180);
    if !valid {
        return Err(parse_error("invalid AirNow PM2.5 site binding").into());
    }
    Ok(())
}

fn parse_error(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "AirNow reportingarea.dat".to_owned(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use chrono_tz::America::New_York;
    use quant_pivot_models::{
        config::{AirNowSourceConfig, WeatherVerticalBindingsConfig},
        domain::data_plane::WeatherObservationReportKind,
        types::{DomainMeasurementUnit, WeatherVariable},
    };
    use rust_decimal_macros::dec;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::{AirNowSource, parse_optional_aqi};

    const HOURLY_HEADER: &str = "AQSID,SiteName,Status,EPARegion,Latitude,Longitude,Elevation,GMTOffset,CountryCode,StateName,ValidDate,ValidTime,DataSource,ReportingArea_PipeDelimited,OZONE_AQI,PM10_AQI,PM25_AQI,NO2_AQI,OZONE_Measured,PM10_Measured,PM25_Measured,NO2_Measured,PM25,PM25_Unit,OZONE,OZONE_Unit,NO2,NO2_Unit,CO,CO_Unit,SO2,SO2_Unit,PM10,PM10_Unit";

    struct HourlyRow<'a> {
        aqsid: &'a str,
        site_name: &'a str,
        status: &'a str,
        latitude: &'a str,
        longitude: &'a str,
        state: &'a str,
        reporting_area: &'a str,
        ozone_aqi: &'a str,
        pm25_aqi: &'a str,
    }

    fn hourly_row(row: &HourlyRow<'_>) -> String {
        [
            row.aqsid,
            row.site_name,
            row.status,
            "R2",
            row.latitude,
            row.longitude,
            "1",
            "-5",
            "US",
            row.state,
            "07/18/2026",
            "10:00",
            "Agency",
            row.reporting_area,
            row.ozone_aqi,
            "",
            row.pm25_aqi,
            "",
            "1",
            "0",
            "1",
            "0",
            "10",
            "UG/M3",
            "20",
            "PPB",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ]
        .join(",")
    }

    fn hourly_body(rows: &[HourlyRow<'_>]) -> String {
        format!(
            "{HOURLY_HEADER}\n{}\n",
            rows.iter().map(hourly_row).collect::<Vec<_>>().join("\n")
        )
    }

    #[tokio::test]
    async fn reporting_area_isolates_forecast() {
        let server = MockServer::start().await;
        let body = concat!(
            "07/18/26|07/18/26|10:00|EDT|0|O|Y|New York City Region|NY|40.7|-74.0|OZONE|45|Good|No||Agency\n",
            "07/18/26|07/18/26|10:00|EDT|0|O|N|New York City Region|NY|40.7|-74.0|PM2.5|601|Hazardous|No||Agency\n",
            "07/18/26|07/17/26||EDT|-1|Y|Y|New York City Region|NY|40.7|-74.0|PM2.5|80|Moderate|No||Agency\n",
            "07/18/26|07/19/26||EDT|1|F|Y|New York City Region|NY|40.7|-74.0|OZONE|70|Moderate|No||Agency\n",
            "07/18/26|07/19/26||EDT|1|F|N|New York City Region|NY|40.7|-74.0|PM2.5|69|Moderate|No||Agency\n",
            "07/18/26|07/18/26|09:00|CDT|0|O|Y|New York City Region|TX|31|-99|OZONE|500|Hazardous|No||Other\n",
        );
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = AirNowSource::connect(AirNowSourceConfig {
            reporting_area_url: server.uri(),
            ..AirNowSourceConfig::default()
        })
        .expect("source");
        let snapshot = source
            .pm25_reporting_area(
                "New York City Region",
                "NY",
                New_York,
                Utc.with_ymd_and_hms(2026, 7, 18, 15, 0, 0).unwrap(),
            )
            .await
            .expect("snapshot");
        assert_eq!(snapshot.observations.len(), 2);
        assert_eq!(snapshot.observations[1].value, dec!(601));
        assert_eq!(snapshot.observations[1].variable, WeatherVariable::Aqi);
        assert_eq!(snapshot.observations[1].unit, DomainMeasurementUnit::Aqi);
        assert_eq!(snapshot.forecasts.len(), 1);
        assert_eq!(snapshot.forecasts[0].value, dec!(69));
        assert_eq!(
            snapshot.forecasts[0].valid_time,
            Utc.with_ymd_and_hms(2026, 7, 19, 4, 0, 0).unwrap()
        );
    }

    #[tokio::test]
    async fn rejects_invalid_type_contract() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                "07/18/26|07/18/26||EDT|1|O|Y|New York City Region|NY|40.7|-74.0|PM2.5|45|Good|No||Agency\n",
            ))
            .mount(&server)
            .await;
        let source = AirNowSource::connect(AirNowSourceConfig {
            reporting_area_url: server.uri(),
            ..AirNowSourceConfig::default()
        })
        .expect("source");
        let result = source
            .pm25_reporting_area("New York City Region", "NY", New_York, Utc::now())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn hourly_file_aggregates_sites() {
        let server = MockServer::start().await;
        let body = hourly_body(&[
            HourlyRow {
                aqsid: "A",
                site_name: "Site A",
                status: "Active",
                latitude: "40.7",
                longitude: "-74.0",
                state: "NY",
                reporting_area: "New York City Region|Other Area",
                ozone_aqi: "40",
                pm25_aqi: "55",
            },
            HourlyRow {
                aqsid: "B",
                site_name: "Site B",
                status: "Active",
                latitude: "40.7",
                longitude: "-74.0",
                state: "NY",
                reporting_area: "New York City Region|Other Area",
                ozone_aqi: "61",
                pm25_aqi: "50",
            },
            HourlyRow {
                aqsid: "C",
                site_name: "Site C",
                status: "Active",
                latitude: "40.7",
                longitude: "-74.0",
                state: "NY",
                reporting_area: "New York City Region|Other Area",
                ozone_aqi: "",
                pm25_aqi: "-999",
            },
            HourlyRow {
                aqsid: "D",
                site_name: "Site D",
                status: "Active",
                latitude: "40.7",
                longitude: "-74.0",
                state: "NY",
                reporting_area: "New York City Region|Other Area",
                ozone_aqi: "",
                pm25_aqi: "630",
            },
        ]);
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = AirNowSource::connect(AirNowSourceConfig {
            hourly_aq_base_url: server.uri(),
            ..AirNowSourceConfig::default()
        })
        .expect("source");
        let hour = Utc.with_ymd_and_hms(2026, 7, 18, 10, 0, 0).unwrap();
        let report = source
            .hourly_pm25_area_observation(
                "New York City Region",
                "NY",
                hour,
                Utc.with_ymd_and_hms(2026, 7, 18, 10, 40, 0).unwrap(),
            )
            .await
            .expect("hourly")
            .expect("area");
        assert_eq!(report.value, dec!(630));
        assert_eq!(
            report.valid_to,
            Some(Utc.with_ymd_and_hms(2026, 7, 18, 11, 0, 0).unwrap())
        );
    }

    #[test]
    fn aqi_preserves_beyond_range() {
        assert!(
            parse_optional_aqi(Some("-999"), "fixture")
                .expect("sentinel")
                .is_none()
        );
        assert!(parse_optional_aqi(Some("500"), "fixture").is_ok());
        assert!(parse_optional_aqi(Some("501"), "fixture").is_ok());
        assert!(parse_optional_aqi(Some("-1"), "fixture").is_err());
    }

    #[tokio::test]
    async fn hourly_selects_rejects_drift() {
        let server = MockServer::start().await;
        let body = hourly_body(&[
            HourlyRow {
                aqsid: "840340170008",
                site_name: "Union City High School",
                status: "Active",
                latitude: "40.770908",
                longitude: "-74.036218",
                state: "NJ",
                reporting_area: "Northeast Urban",
                ozone_aqi: "",
                pm25_aqi: "544",
            },
            HourlyRow {
                aqsid: "840340170009",
                site_name: "Union City High School",
                status: "Active",
                latitude: "40.770908",
                longitude: "-74.036218",
                state: "NJ",
                reporting_area: "Northeast Urban",
                ozone_aqi: "500",
                pm25_aqi: "500",
            },
        ]);
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;
        let source = AirNowSource::connect(AirNowSourceConfig {
            hourly_aq_base_url: server.uri(),
            ..AirNowSourceConfig::default()
        })
        .expect("source");
        let hour = Utc.with_ymd_and_hms(2026, 7, 18, 10, 0, 0).unwrap();
        let mut binding = WeatherVerticalBindingsConfig::default()
            .airnow_pm25_sites
            .remove(0);
        let report = source
            .hourly_pm25_site_observation(
                &binding,
                hour,
                Utc.with_ymd_and_hms(2026, 7, 18, 10, 40, 0).unwrap(),
            )
            .await
            .expect("hourly site")
            .expect("PM2.5 AQI");
        assert_eq!(report.value, dec!(544));
        assert_eq!(report.subject_key, "840340170008");
        assert_eq!(
            report.report_kind,
            WeatherObservationReportKind::AirNowPm25SiteObservation
        );
        assert_eq!(
            report.instrument_key.as_str(),
            "AIRNOW_SITE:840340170008:PM25_AQI"
        );

        binding.latitude = dec!(40.770900);
        let drift = source
            .hourly_pm25_site_observation(&binding, hour, Utc::now())
            .await;
        assert!(drift.is_err());
    }

    #[test]
    fn aqi_accepts_missing_sentinel() {
        assert_eq!(
            parse_optional_aqi(Some("680"), "test").unwrap(),
            Some(dec!(680))
        );
        assert_eq!(parse_optional_aqi(Some("-999"), "test").unwrap(), None);
        assert!(parse_optional_aqi(Some("-1"), "test").is_err());
    }
}
