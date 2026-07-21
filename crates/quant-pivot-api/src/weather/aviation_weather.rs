//! AviationWeather.gov METAR/SPECI/COR adapter.

use std::time::Duration;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::AviationWeatherSourceConfig,
    domain::data_plane::{WeatherObservationReport, WeatherObservationReportKind},
    hashing::CanonicalDigest,
    types::{
        ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, IcaoStation,
        WeatherVariable,
    },
};
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, de::Error};
use serde_json::Value;

use crate::{
    infra::{http::get_text_with_retry, retry::RetryPolicy},
    wire::decimal::parse_decimal_value,
};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetarWire {
    icao_id: String,
    receipt_time: DateTime<Utc>,
    obs_time: i64,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    temp: Option<Decimal>,
    metar_type: String,
    raw_ob: String,
}

/// Rate-limit-friendly station-scoped NOAA observation client.
pub struct AviationWeatherSource {
    config: AviationWeatherSourceConfig,
    http: Client,
    retry_policy: RetryPolicy,
}

impl AviationWeatherSource {
    pub fn connect(config: AviationWeatherSourceConfig) -> QuantResult<Self> {
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .user_agent("quant-pivot/0.1 aviation-weather-ingest")
            .build()
            .map_err(|error| ApiError::Sdk(format!("AviationWeather HTTP client: {error}")))?;
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

    /// Fetch up to `AviationWeather`'s 15-day API history for one frozen station.
    pub async fn observations(
        &self,
        station: &IcaoStation,
        hours: u16,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Vec<WeatherObservationReport>> {
        let hours = hours.clamp(1, 15 * 24);
        let url = format!(
            "{}/metar?ids={station}&format=json&hours={hours}",
            self.config.base_url.trim_end_matches('/'),
        );
        let body = get_text_with_retry(&self.http, &self.retry_policy, &url)
            .await
            .map_err(QuantError::from)?;
        if body.trim().is_empty() {
            return Ok(Vec::new());
        }
        let rows: Vec<MetarWire> =
            serde_json::from_str(&body).map_err(|error| ApiError::Deserialize {
                context: "AviationWeather METAR JSON".into(),
                detail: error.to_string(),
            })?;
        let mut reports = rows
            .into_iter()
            .filter_map(|row| map_report(station, row, available_at).transpose())
            .collect::<QuantResult<Vec<_>>>()?;
        reports.sort_by(|left, right| {
            left.available_at
                .cmp(&right.available_at)
                .then_with(|| left.published_at.cmp(&right.published_at))
                .then_with(|| left.observed_at.cmp(&right.observed_at))
                .then_with(|| left.report_hash.as_str().cmp(right.report_hash.as_str()))
        });
        Ok(reports)
    }
}

fn map_report(
    station: &IcaoStation,
    row: MetarWire,
    available_at: DateTime<Utc>,
) -> QuantResult<Option<WeatherObservationReport>> {
    if row.icao_id != station.as_str() {
        return Err(ApiError::Deserialize {
            context: "AviationWeather METAR JSON".into(),
            detail: format!(
                "station {} does not match frozen binding {station}",
                row.icao_id
            ),
        }
        .into());
    }
    let Some(temperature) = row.temp else {
        return Ok(None);
    };
    let observation_time =
        DateTime::from_timestamp(row.obs_time, 0).ok_or_else(|| ApiError::Deserialize {
            context: "AviationWeather METAR JSON".into(),
            detail: format!("invalid obsTime: {}", row.obs_time),
        })?;
    // AWC exposes source receipt and nominal observation as independent fields.
    // Some valid METARs arrive before their nominal report minute; retaining
    // both timestamps lets PIT selection conservatively gate on event time.
    if available_at < row.receipt_time {
        return Err(ApiError::Deserialize {
            context: "AviationWeather METAR timing".into(),
            detail: "local availability precedes the source receipt timestamp".to_owned(),
        }
        .into());
    }
    let report_kind = if row
        .raw_ob
        .split_ascii_whitespace()
        .any(|token| token == "COR")
    {
        WeatherObservationReportKind::Correction
    } else if row.metar_type.eq_ignore_ascii_case("SPECI") {
        WeatherObservationReportKind::Speci
    } else if row.metar_type.eq_ignore_ascii_case("METAR") {
        WeatherObservationReportKind::Metar
    } else {
        return Err(ApiError::Deserialize {
            context: "AviationWeather METAR JSON".into(),
            detail: format!("unknown metarType: {}", row.metar_type),
        }
        .into());
    };
    let report_hash = ContentHash::parse(CanonicalDigest::prefixed_bytes(row.raw_ob.as_bytes()))?;
    let precision_celsius = Decimal::new(1, temperature.scale().min(1));
    Ok(Some(WeatherObservationReport {
        source_id: DomainSourceId::aviation_weather(),
        instrument_key: DomainInstrumentKey::aviation_weather(station),
        subject_key: station.to_string(),
        report_kind,
        variable: WeatherVariable::Temperature,
        value: temperature,
        unit: DomainMeasurementUnit::Celsius,
        precision: precision_celsius,
        observed_at: observation_time,
        valid_from: None,
        valid_to: None,
        published_at: row.receipt_time,
        available_at,
        report_hash,
        raw_report: row.raw_ob,
    }))
}

fn deserialize_optional_decimal<'de, D>(deserializer: D) -> Result<Option<Decimal>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    value
        .map(|value| parse_decimal_value(&value).map_err(Error::custom))
        .transpose()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{config::AviationWeatherSourceConfig, types::IcaoStation};
    use rust_decimal_macros::dec;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::AviationWeatherSource;

    #[tokio::test]
    async fn maps_metar_and_correction_without_losing_same_time_revision() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/metar"))
            .and(query_param("ids", "KJFK"))
            .and(query_param("format", "json"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "icaoId": "KJFK",
                    "receiptTime": "2026-07-13T16:01:00Z",
                    "obsTime": 1_783_958_400,
                    "temp": 26.7,
                    "metarType": "METAR",
                    "rawOb": "METAR KJFK 131600Z 19010KT 27/17"
                },
                {
                    "icaoId": "KJFK",
                    "receiptTime": "2026-07-13T16:02:00Z",
                    "obsTime": 1_783_958_400,
                    "temp": 26.1,
                    "metarType": "METAR",
                    "rawOb": "METAR COR KJFK 131600Z 19010KT 26/17"
                }
            ])))
            .mount(&server)
            .await;
        let source = AviationWeatherSource::connect(AviationWeatherSourceConfig {
            base_url: server.uri(),
            ..AviationWeatherSourceConfig::default()
        })
        .expect("source");
        let station = IcaoStation::parse("KJFK").expect("station");
        let available_at = Utc.with_ymd_and_hms(2026, 7, 13, 16, 3, 0).unwrap();
        let reports = source
            .observations(&station, 2, available_at)
            .await
            .expect("observations");
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].value, dec!(26.7));
        assert_eq!(reports[1].value, dec!(26.1));
        assert_ne!(reports[0].report_hash, reports[1].report_hash);
    }

    #[tokio::test]
    async fn preserves_a_source_receipt_that_precedes_nominal_observation_time() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/metar"))
            .and(query_param("ids", "KBKF"))
            .and(query_param("format", "json"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                    "icaoId": "KBKF",
                    "receiptTime": "2026-07-15T02:56:35.906Z",
                    "obsTime": 1_784_084_280,
                    "temp": 27.8,
                    "metarType": "METAR",
                    "rawOb": "METAR KBKF 150258Z 14016KT 10SM FEW140 28/09 A3018"
                }])),
            )
            .mount(&server)
            .await;
        let source = AviationWeatherSource::connect(AviationWeatherSourceConfig {
            base_url: server.uri(),
            ..AviationWeatherSourceConfig::default()
        })
        .expect("source");
        let reports = source
            .observations(
                &IcaoStation::parse("KBKF").expect("station"),
                2,
                Utc.with_ymd_and_hms(2026, 7, 15, 3, 0, 0).unwrap(),
            )
            .await
            .expect("observations");

        assert_eq!(reports.len(), 1);
        assert!(reports[0].published_at < reports[0].observed_at);
        assert!(reports[0].published_at <= reports[0].available_at);
    }
}
