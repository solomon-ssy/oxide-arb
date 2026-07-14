//! NOAA GEFS 0.5-degree TMAX byte-range and GRIB2 adapter.

use std::{collections::BTreeMap, time::Duration as StdDuration};

use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use grib::LatLons;
use num_traits::FromPrimitive;
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::GefsSourceConfig,
    hashing::CanonicalDigest,
    types::{ContentHash, IcaoStation, TemperatureCelsius},
};
use reqwest::{StatusCode, header::RANGE};
use rust_decimal::Decimal;

use crate::infra::{
    http::{get_text_with_retry, is_retryable_status},
    retry::{self, RetryPolicy},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GefsStationBinding {
    pub station: IcaoStation,
    pub latitude: Decimal,
    pub longitude: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GefsStationPoint {
    pub station: IcaoStation,
    pub tmax_celsius: TemperatureCelsius,
    pub grid_binding_hash: ContentHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GefsDecodedMember {
    pub reference_time: DateTime<Utc>,
    pub valid_time: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub lead_hours: u16,
    pub member: u8,
    pub segment_hash: ContentHash,
    pub points: Vec<GefsStationPoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ByteRange {
    start: u64,
    end: u64,
}

/// Anonymous NODD GEFS client. `.idx` selection is mandatory: downloading a
/// full global product for one field is treated as a contract violation.
pub struct GefsSource {
    config: GefsSourceConfig,
    http: reqwest::Client,
    retry_policy: RetryPolicy,
}

impl GefsSource {
    pub fn connect(config: GefsSourceConfig) -> QuantResult<Self> {
        let http = reqwest::Client::builder()
            .timeout(StdDuration::from_millis(config.request_timeout_ms))
            .user_agent("quant-pivot/0.1 noaa-gefs-ingest")
            .build()
            .map_err(|error| ApiError::Sdk(format!("GEFS HTTP client: {error}")))?;
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

    pub async fn tmax_member(
        &self,
        reference_time: DateTime<Utc>,
        lead_hours: u16,
        member: u8,
        stations: &[GefsStationBinding],
    ) -> QuantResult<GefsDecodedMember> {
        validate_request(reference_time, lead_hours, member, stations)?;
        let url = product_url(
            self.config.bucket_url.trim_end_matches('/'),
            reference_time,
            lead_hours,
            member,
        );
        let index = get_text_with_retry(&self.http, &self.retry_policy, &format!("{url}.idx"))
            .await
            .map_err(QuantError::from)?;
        let range = tmax_range(&index)?;
        let segment = get_range_with_retry(&self.http, &self.retry_policy, &url, range).await?;
        let segment_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(segment.as_slice()))?;
        let points = decode_tmax(&segment, stations)?;
        Ok(GefsDecodedMember {
            reference_time,
            valid_time: reference_time + Duration::hours(i64::from(lead_hours)),
            available_at: Utc::now(),
            lead_hours,
            member,
            segment_hash,
            points,
        })
    }
}

fn validate_request(
    reference_time: DateTime<Utc>,
    lead_hours: u16,
    member: u8,
    stations: &[GefsStationBinding],
) -> QuantResult<()> {
    if !matches!(reference_time.hour(), 0 | 6 | 12 | 18)
        || reference_time.minute() != 0
        || reference_time.second() != 0
        || reference_time.nanosecond() != 0
    {
        return Err(
            parse_error("GEFS reference time must be an exact 00/06/12/18 UTC cycle").into(),
        );
    }
    if !(3..=240).contains(&lead_hours) || !lead_hours.is_multiple_of(3) {
        return Err(parse_error("GEFS lead must be a 3-hour step in 3..=240").into());
    }
    if member > 30 || stations.is_empty() {
        return Err(
            parse_error("GEFS member must be 0..=30 and stations must be non-empty").into(),
        );
    }
    if stations.iter().any(|station| {
        station.latitude < Decimal::new(-90, 0)
            || station.latitude > Decimal::new(90, 0)
            || station.longitude < Decimal::new(-180, 0)
            || station.longitude > Decimal::new(180, 0)
    }) {
        return Err(parse_error("GEFS station coordinates are outside WGS84 bounds").into());
    }
    Ok(())
}

fn product_url(base: &str, reference_time: DateTime<Utc>, lead_hours: u16, member: u8) -> String {
    let member_name = if member == 0 {
        "gec00".to_owned()
    } else {
        format!("gep{member:02}")
    };
    format!(
        "{base}/gefs.{year:04}{month:02}{day:02}/{cycle:02}/atmos/pgrb2ap5/{member_name}.t{cycle:02}z.pgrb2a.0p50.f{lead_hours:03}",
        year = reference_time.year(),
        month = reference_time.month(),
        day = reference_time.day(),
        cycle = reference_time.hour(),
    )
}

fn tmax_range(index: &str) -> QuantResult<ByteRange> {
    let entries = index
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut fields = line.split(':');
            let _record = fields.next();
            let offset = fields
                .next()
                .ok_or_else(|| parse_error("GEFS index row has no byte offset"))?
                .parse::<u64>()
                .map_err(|error| parse_error(format!("invalid GEFS byte offset: {error}")))?;
            Ok((offset, line))
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let matches = entries
        .iter()
        .enumerate()
        .filter(|(_, (_, line))| line.contains(":TMAX:2 m above ground:"))
        .collect::<Vec<_>>();
    let [(index, (start, _))] = matches.as_slice() else {
        return Err(parse_error(format!(
            "GEFS index must contain exactly one 2 m TMAX field, found {}",
            matches.len()
        ))
        .into());
    };
    let end = entries
        .get(index + 1)
        .and_then(|(next, _)| next.checked_sub(1))
        .ok_or_else(|| parse_error("GEFS TMAX field has no bounded successor offset"))?;
    if end < *start {
        return Err(parse_error("GEFS TMAX byte range is inverted").into());
    }
    Ok(ByteRange { start: *start, end })
}

async fn get_range_with_retry(
    http: &reqwest::Client,
    retry_policy: &RetryPolicy,
    url: &str,
    range: ByteRange,
) -> QuantResult<Vec<u8>> {
    let bytes = retry::retry_with_policy(retry_policy, || {
        let http = http.clone();
        let url = url.to_owned();
        async move {
            let response = http
                .get(&url)
                .header(RANGE, format!("bytes={}-{}", range.start, range.end))
                .send()
                .await
                .map_err(|error| ApiError::Http {
                    method: "GET",
                    url: url.clone(),
                    status: 0,
                    body: error.to_string(),
                    retryable: true,
                })?;
            let status = response.status();
            if status != StatusCode::PARTIAL_CONTENT {
                let code = status.as_u16();
                return Err(ApiError::Http {
                    method: "GET",
                    url,
                    status: code,
                    body: response.text().await.unwrap_or_default(),
                    retryable: is_retryable_status(code),
                });
            }
            response
                .bytes()
                .await
                .map(|bytes| bytes.to_vec())
                .map_err(|error| ApiError::Http {
                    method: "GET",
                    url,
                    status: 0,
                    body: error.to_string(),
                    retryable: true,
                })
        }
    })
    .await?;
    let expected = range
        .end
        .checked_sub(range.start)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| parse_error("GEFS byte-range length overflow"))?;
    if u64::try_from(bytes.len()).ok() != Some(expected) {
        return Err(parse_error("GEFS byte-range response length mismatch").into());
    }
    Ok(bytes)
}

fn decode_tmax(
    segment: &[u8],
    stations: &[GefsStationBinding],
) -> QuantResult<Vec<GefsStationPoint>> {
    let grib = grib::from_bytes(segment)
        .map_err(|error| parse_error(format!("invalid GEFS GRIB2 segment: {error}")))?;
    let mut messages = grib.iter();
    let (_, message) = messages
        .next()
        .ok_or_else(|| parse_error("GEFS GRIB2 segment has no submessage"))?;
    if messages.next().is_some() {
        return Err(parse_error("GEFS TMAX range contains multiple GRIB2 submessages").into());
    }
    let coordinates = message
        .latlons()
        .map_err(|error| parse_error(format!("GEFS grid decode failed: {error}")))?;
    let decoder = grib::Grib2SubmessageDecoder::from(message)
        .map_err(|error| parse_error(format!("GEFS value decoder failed: {error}")))?;
    let values = decoder
        .dispatch()
        .map_err(|error| parse_error(format!("GEFS value decode failed: {error}")))?;
    let mut samplers = stations.iter().map(GridSampler::new).collect::<Vec<_>>();
    for ((latitude, longitude), value) in coordinates.zip(values) {
        let latitude = Decimal::from_f32(latitude)
            .ok_or_else(|| parse_error("GEFS latitude is not finite"))?
            .round_dp(4);
        let longitude = Decimal::from_f32(longitude)
            .ok_or_else(|| parse_error("GEFS longitude is not finite"))?
            .round_dp(4);
        let value =
            Decimal::from_f32(value).ok_or_else(|| parse_error("GEFS TMAX value is not finite"))?;
        for sampler in &mut samplers {
            sampler.observe(latitude, longitude, value);
        }
    }
    samplers.into_iter().map(GridSampler::finish).collect()
}

struct GridSampler {
    station: IcaoStation,
    latitude: Decimal,
    longitude: Decimal,
    lower_latitude: Decimal,
    upper_latitude: Decimal,
    lower_longitude: Decimal,
    upper_longitude: Decimal,
    corners: BTreeMap<(Decimal, Decimal), Decimal>,
}

impl GridSampler {
    fn new(binding: &GefsStationBinding) -> Self {
        // The operational GEFS 0.5-degree grid is encoded on [-180, 180).
        // Keep station longitude in the same frozen convention; silently
        // normalizing to [0, 360) would select no corners on the real product.
        let longitude = binding.longitude;
        let lower_latitude = grid_floor(binding.latitude);
        let lower_longitude = grid_floor(longitude);
        Self {
            station: binding.station.clone(),
            latitude: binding.latitude,
            longitude,
            lower_latitude,
            upper_latitude: lower_latitude + grid_step(),
            lower_longitude,
            upper_longitude: lower_longitude + grid_step(),
            corners: BTreeMap::new(),
        }
    }

    fn observe(&mut self, latitude: Decimal, longitude: Decimal, kelvin: Decimal) {
        let is_latitude = latitude == self.lower_latitude || latitude == self.upper_latitude;
        let is_longitude = longitude == self.lower_longitude || longitude == self.upper_longitude;
        if is_latitude && is_longitude {
            self.corners.insert((latitude, longitude), kelvin);
        }
    }

    fn finish(self) -> QuantResult<GefsStationPoint> {
        let corner = |latitude, longitude| {
            self.corners
                .get(&(latitude, longitude))
                .copied()
                .ok_or_else(|| parse_error("GEFS grid is missing a bilinear corner"))
        };
        let lower_lower = corner(self.lower_latitude, self.lower_longitude)?;
        let lower_upper = corner(self.lower_latitude, self.upper_longitude)?;
        let upper_lower = corner(self.upper_latitude, self.lower_longitude)?;
        let upper_upper = corner(self.upper_latitude, self.upper_longitude)?;
        let x = (self.longitude - self.lower_longitude) / grid_step();
        let y = (self.latitude - self.lower_latitude) / grid_step();
        let one_minus_x = Decimal::ONE - x;
        let one_minus_y = Decimal::ONE - y;
        let kelvin = lower_lower * one_minus_x * one_minus_y
            + lower_upper * x * one_minus_y
            + upper_lower * one_minus_x * y
            + upper_upper * x * y;
        let grid_binding_hash = CanonicalDigest::content_hash_json(&(
            "gefs_0p50_four_point_bilinear_v1",
            &self.station,
            self.latitude,
            self.longitude,
            self.lower_latitude,
            self.upper_latitude,
            self.lower_longitude,
            self.upper_longitude,
            x,
            y,
        ))?;
        Ok(GefsStationPoint {
            station: self.station,
            tmax_celsius: TemperatureCelsius::new((kelvin - kelvin_offset()).round_dp(4)),
            grid_binding_hash,
        })
    }
}

fn grid_floor(value: Decimal) -> Decimal {
    (value / grid_step()).floor() * grid_step()
}

fn grid_step() -> Decimal {
    Decimal::new(5, 1)
}

fn kelvin_offset() -> Decimal {
    Decimal::new(27_315, 2)
}

fn parse_error(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "NOAA GEFS".into(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::{ByteRange, GefsStationBinding, GridSampler, product_url, tmax_range};
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::types::IcaoStation;
    use rust_decimal_macros::dec;

    #[test]
    fn selects_bounded_tmax_range() {
        let index = concat!(
            "1:0:d=2026071300:TMP:2 m above ground:3 hour fcst:\n",
            "2:100:d=2026071300:TMAX:2 m above ground:0-3 hour max fcst:\n",
            "3:250:d=2026071300:RH:2 m above ground:3 hour fcst:\n",
        );
        assert_eq!(
            tmax_range(index).expect("range"),
            ByteRange {
                start: 100,
                end: 249
            }
        );
    }

    #[test]
    fn builds_control_and_perturbed_urls() {
        let reference = Utc.with_ymd_and_hms(2026, 7, 13, 6, 0, 0).unwrap();
        assert!(product_url("https://bucket", reference, 3, 0).contains("gec00.t06z"));
        assert!(product_url("https://bucket", reference, 240, 30).contains("gep30.t06z"));
    }

    #[test]
    fn interpolates_real_gefs_negative_longitude_grid() {
        let binding = GefsStationBinding {
            station: IcaoStation::parse("KLGA").expect("station"),
            latitude: dec!(40.75),
            longitude: dec!(-73.75),
        };
        let mut sampler = GridSampler::new(&binding);
        for (latitude, longitude) in [
            (dec!(40.5), dec!(-74.0)),
            (dec!(40.5), dec!(-73.5)),
            (dec!(41.0), dec!(-74.0)),
            (dec!(41.0), dec!(-73.5)),
        ] {
            sampler.observe(latitude, longitude, dec!(300.0));
        }
        let point = sampler.finish().expect("point");
        assert_eq!(point.tmax_celsius.value(), dec!(26.85));
    }
}
