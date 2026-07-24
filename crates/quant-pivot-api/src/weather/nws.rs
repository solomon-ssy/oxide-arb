//! NOAA/NWS API station wind observation adapter.

use std::time::Duration;

use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantError, QuantResult, api::ApiError};
use quant_pivot_models::{
    config::NwsObservationSourceConfig,
    domain::data_plane::{WeatherObservationReport, WeatherObservationReportKind},
    hashing::CanonicalDigest,
    types::{
        DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, IcaoStation, WeatherVariable,
    },
};
use reqwest::Client;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Deserializer, Serialize, de::Error};
use serde_json::Value;

use crate::{
    infra::{http::get_text_with_retry, retry::RetryPolicy},
    wire::decimal::parse_decimal_value,
};

/// Public NWS API station client.
pub struct NwsObservationSource {
    config: NwsObservationSourceConfig,
    http: Client,
    retry_policy: RetryPolicy,
}

impl NwsObservationSource {
    pub fn connect(config: NwsObservationSourceConfig) -> QuantResult<Self> {
        let http = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .user_agent("quant-pivot/0.1 nws-observation-ingest contact=operator")
            .build()
            .map_err(|error| ApiError::Sdk(format!("NWS HTTP client: {error}")))?;
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

    /// Fetch a bounded newest-first observation window and retain every
    /// quality-controlled wind value. Null fields remain absent; the window
    /// bridges normal missing reports without converting them to zero.
    pub async fn recent_wind(
        &self,
        station: &IcaoStation,
        available_at: DateTime<Utc>,
    ) -> QuantResult<Vec<WeatherObservationReport>> {
        let url = format!(
            "{}/stations/{station}/observations?limit={}",
            self.config.base_url.trim_end_matches('/'),
            self.config.lookback_observations
        );
        let body = get_text_with_retry(&self.http, &self.retry_policy, &url)
            .await
            .map_err(QuantError::from)?;
        parse_observations(&body, station, available_at)
    }
}

#[derive(Debug, Deserialize)]
struct NwsObservationCollectionWire {
    features: Vec<NwsObservationWire>,
}

#[derive(Debug, Deserialize)]
struct NwsObservationWire {
    id: String,
    properties: NwsPropertiesWire,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct NwsPropertiesWire {
    timestamp: DateTime<Utc>,
    #[serde(default)]
    raw_message: Option<String>,
    wind_speed: QuantitativeValueWire,
    wind_gust: QuantitativeValueWire,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuantitativeValueWire {
    unit_code: String,
    #[serde(default)]
    quality_control: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_decimal")]
    value: Option<Decimal>,
}

#[derive(Serialize)]
struct CanonicalWindReport<'a> {
    station: &'a str,
    variable: &'a str,
    observed_at: DateTime<Utc>,
    source_unit: &'a str,
    quality_control: &'a str,
    source_value: Decimal,
    knots: Decimal,
    raw_message: Option<&'a str>,
}

fn parse_observations(
    body: &str,
    station: &IcaoStation,
    available_at: DateTime<Utc>,
) -> QuantResult<Vec<WeatherObservationReport>> {
    let wire: NwsObservationCollectionWire =
        serde_json::from_str(body).map_err(|error| parse_error(error.to_string()))?;
    let mut reports = Vec::new();
    for observation in wire.features {
        reports.extend(reports_from_observation(
            &observation,
            station,
            available_at,
        )?);
    }
    reports.sort_by_key(|report| (report.observed_at, report.variable, report.report_hash));
    Ok(reports)
}

fn reports_from_observation(
    wire: &NwsObservationWire,
    station: &IcaoStation,
    available_at: DateTime<Utc>,
) -> QuantResult<Vec<WeatherObservationReport>> {
    let expected = format!("/stations/{station}/observations/");
    if !wire.id.contains(&expected) {
        return Err(parse_error("observation id does not match frozen station binding").into());
    }
    if wire.properties.timestamp > available_at {
        return Err(parse_error("observation timestamp is later than availability").into());
    }
    let mut reports = Vec::new();
    for (variable, value, instrument) in [
        (
            WeatherVariable::WindSpeed,
            &wire.properties.wind_speed,
            DomainInstrumentKey::nws_wind_speed(station),
        ),
        (
            WeatherVariable::WindGust,
            &wire.properties.wind_gust,
            DomainInstrumentKey::nws_wind_gust(station),
        ),
    ] {
        let Some(source_value) = value.value else {
            continue;
        };
        let quality_control = value
            .quality_control
            .as_deref()
            .filter(|code| matches!(*code, "C" | "G" | "S" | "V"))
            .ok_or_else(|| parse_error("non-null wind value did not pass accepted NWS QC"))?;
        if source_value < Decimal::ZERO {
            return Err(parse_error("wind value cannot be negative").into());
        }
        let knots = to_knots(source_value, &value.unit_code)?;
        let canonical = CanonicalWindReport {
            station: station.as_str(),
            variable: variable.as_str(),
            observed_at: wire.properties.timestamp,
            source_unit: &value.unit_code,
            quality_control,
            source_value,
            knots,
            raw_message: wire.properties.raw_message.as_deref(),
        };
        let report_hash = CanonicalDigest::content_hash_json(&canonical)?;
        let raw_report =
            serde_json::to_string(&canonical).map_err(|error| parse_error(error.to_string()))?;
        reports.push(WeatherObservationReport {
            source_id: DomainSourceId::nws_observation(),
            instrument_key: instrument,
            subject_key: station.to_string(),
            report_kind: WeatherObservationReportKind::NwsStation,
            variable,
            value: knots,
            unit: DomainMeasurementUnit::Knot,
            precision: Decimal::new(1, 1),
            observed_at: wire.properties.timestamp,
            valid_from: None,
            valid_to: None,
            published_at: available_at,
            available_at,
            report_hash,
            raw_report,
        });
    }
    Ok(reports)
}

fn to_knots(value: Decimal, unit: &str) -> QuantResult<Decimal> {
    let knots = match unit {
        "wmoUnit:km_h-1" => value / Decimal::new(1_852, 3),
        "wmoUnit:m_s-1" => value * Decimal::new(19_438_444_924_406, 13),
        "wmoUnit:kn" => value,
        _ => return Err(parse_error(format!("unsupported wind unit `{unit}`")).into()),
    };
    Ok(knots.round_dp_with_strategy(1, RoundingStrategy::MidpointAwayFromZero))
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

fn parse_error(detail: impl Into<String>) -> ApiError {
    ApiError::Deserialize {
        context: "NOAA/NWS station observation GeoJSON".to_owned(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        config::NwsObservationSourceConfig,
        types::{IcaoStation, WeatherVariable},
    };
    use rust_decimal_macros::dec;
    use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

    use super::NwsObservationSource;

    #[tokio::test]
    async fn converts_nws_kilometers_knots() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "features": [{
                    "id": "https://api.weather.gov/stations/KMWN/observations/2026-07-17T22:56:00Z",
                    "properties": {
                        "timestamp": "2026-07-17T22:56:00Z",
                        "rawMessage": "KMWN 172256Z 28040G55KT",
                        "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 74.08, "qualityControl": "V"},
                        "windGust": {"unitCode": "wmoUnit:km_h-1", "value": 101.86, "qualityControl": "C"}
                    }
                }]
            })))
            .mount(&server)
            .await;
        let source = NwsObservationSource::connect(NwsObservationSourceConfig {
            base_url: server.uri(),
            ..NwsObservationSourceConfig::default()
        })
        .expect("source");
        let reports = source
            .recent_wind(
                &IcaoStation::parse("KMWN").expect("station"),
                Utc.with_ymd_and_hms(2026, 7, 17, 23, 0, 0).unwrap(),
            )
            .await
            .expect("wind");
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].variable, WeatherVariable::WindSpeed);
        assert_eq!(reports[0].value, dec!(40.0));
        assert_eq!(reports[1].variable, WeatherVariable::WindGust);
        assert_eq!(reports[1].value, dec!(55.0));
    }

    #[tokio::test]
    async fn null_gust_not_zero() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "features": [{
                    "id": "https://api.weather.gov/stations/KJFK/observations/2026-07-17T23:51:00Z",
                    "properties": {
                        "timestamp": "2026-07-17T23:51:00Z",
                        "rawMessage": null,
                        "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": null, "qualityControl": "Z"},
                        "windGust": {"unitCode": "wmoUnit:km_h-1", "value": null, "qualityControl": "Z"}
                    }
                }, {
                    "id": "https://api.weather.gov/stations/KJFK/observations/2026-07-17T22:51:00Z",
                    "properties": {
                        "timestamp": "2026-07-17T22:51:00Z",
                        "rawMessage": null,
                        "windSpeed": {"unitCode": "wmoUnit:km_h-1", "value": 18.52, "qualityControl": "V"},
                        "windGust": {"unitCode": "wmoUnit:km_h-1", "value": null, "qualityControl": "Z"}
                    }
                }]
            })))
            .mount(&server)
            .await;
        let source = NwsObservationSource::connect(NwsObservationSourceConfig {
            base_url: server.uri(),
            ..NwsObservationSourceConfig::default()
        })
        .expect("source");
        let reports = source
            .recent_wind(&IcaoStation::parse("KJFK").expect("station"), Utc::now())
            .await
            .expect("wind");
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].value, dec!(10.0));
    }
}
