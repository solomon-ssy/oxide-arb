//! Live public Weather source contract tests.

use chrono::{Duration, Timelike, Utc};
use chrono_tz::Tz;
use quant_pivot_api::weather::{
    airnow::AirNowSource,
    ghcnh::GhcnhSource,
    gistemp::NasaGistempSource,
    hko::HkoOpenDataSource,
    nhc::{NhcBasin, NhcSource},
    nsidc::{NsidcSeaIceSource, SeaIceHemisphere},
    nws::NwsObservationSource,
    tornado::TornadoSource,
};
use quant_pivot_models::{
    config::{
        AirNowSourceConfig, GhcnhSourceConfig, HkoOpenDataSourceConfig, NasaGistempSourceConfig,
        NhcSourceConfig, NsidcSeaIceSourceConfig, NwsObservationSourceConfig, TornadoSourceConfig,
        WeatherVerticalBindingsConfig,
    },
    types::{
        DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, HkoStation, IcaoStation,
        WeatherTemperatureStatistic, WeatherVariable,
    },
};
use rust_decimal::Decimal;

#[tokio::test]
#[ignore = "requires live Hong Kong Observatory Open Data API"]
async fn live_hko_rainfall_preserves_official_window_and_provenance() {
    let source = HkoOpenDataSource::connect(HkoOpenDataSourceConfig::default()).expect("source");
    let report = source
        .rainfall("North District", Utc::now())
        .await
        .expect("HKO rainfall")
        .expect("configured HKO reporting place");
    assert_eq!(report.source_id, DomainSourceId::hko_open_data());
    assert_eq!(report.variable, WeatherVariable::Precipitation);
    assert_eq!(report.unit, DomainMeasurementUnit::Millimeter);
    assert!(report.value >= Decimal::ZERO);
    assert!(
        report
            .valid_from
            .is_some_and(|start| start < report.observed_at)
    );
    assert!(report.observed_at <= report.published_at);
    assert!(report.published_at <= report.available_at);
}

#[tokio::test]
#[ignore = "requires live Hong Kong Observatory climate Open Data API"]
async fn live_hko_daily_temperature_has_complete_source_native_history() {
    let source = HkoOpenDataSource::connect(HkoOpenDataSourceConfig::default()).expect("source");
    let station = HkoStation::parse("HKO").expect("station");
    for (statistic, variable) in [
        (
            WeatherTemperatureStatistic::Maximum,
            WeatherVariable::TemperatureMaximum,
        ),
        (
            WeatherTemperatureStatistic::Minimum,
            WeatherVariable::TemperatureMinimum,
        ),
    ] {
        let month = source
            .daily_temperatures(&station, statistic, 2025, 7, Utc::now())
            .await
            .expect("HKO daily temperature");
        assert_eq!(month.reports.len(), 31);
        assert_eq!(month.incomplete_rows, 0);
        assert_eq!(month.unavailable_rows, 0);
        assert!(month.reports.iter().all(|report| {
            report.source_id == DomainSourceId::hko_open_data()
                && report.variable == variable
                && report.unit == DomainMeasurementUnit::Celsius
                && report.instrument_key.as_str()
                    == DomainInstrumentKey::hko_daily_temperature(&station, statistic).as_str()
        }));
    }
}

#[tokio::test]
#[ignore = "requires live NOAA GHCNh yearly station archive"]
async fn live_ghcnh_station_year_streams_bounded_nonempty_history() {
    let source = GhcnhSource::connect(GhcnhSourceConfig::default()).expect("source");
    let station = IcaoStation::parse("KLGA").expect("station");
    let year = source
        .yearly_station(&station, "USW00014732", 2025, Utc::now())
        .await
        .expect("GHCNh request")
        .expect("published GHCNh partition");
    assert!(!year.reports.is_empty());
    assert!(year.reports.iter().all(|report| {
        report.source_id == DomainSourceId::ghcnh()
            && report.subject_key == "KLGA"
            && report.variable == WeatherVariable::Temperature
            && report.unit == DomainMeasurementUnit::Celsius
    }));
}

#[tokio::test]
#[ignore = "requires live EPA AirNow public file"]
async fn live_airnow_pm25_reporting_area_has_typed_aqi_facts() {
    let source = AirNowSource::connect(AirNowSourceConfig::default()).expect("source");
    let mut numeric_forecast_areas = 0_u8;
    for binding in WeatherVerticalBindingsConfig::default().airnow_pm25_reporting_areas {
        let timezone = binding.timezone.parse::<Tz>().expect("binding timezone");
        let snapshot = source
            .pm25_reporting_area(&binding.area, &binding.state, timezone, Utc::now())
            .await
            .expect("AirNow PM2.5 reporting area");
        assert!(!snapshot.observations.is_empty(), "{}", binding.area);
        if !snapshot.forecasts.is_empty() {
            numeric_forecast_areas = numeric_forecast_areas.saturating_add(1);
        }
        assert!(snapshot.observations.iter().all(|report| {
            report.source_id == DomainSourceId::airnow()
                && report.instrument_key
                    == DomainInstrumentKey::airnow_pm25_observation(&format!(
                        "{}:{}",
                        binding.state, binding.area
                    ))
                && report.variable == WeatherVariable::Aqi
                && report.unit == DomainMeasurementUnit::Aqi
        }));
        assert!(snapshot.forecasts.iter().all(|point| {
            point.source_id == DomainSourceId::airnow()
                && point.instrument_key
                    == DomainInstrumentKey::airnow_pm25_forecast(&format!(
                        "{}:{}",
                        binding.state, binding.area
                    ))
        }));
    }
    assert!(numeric_forecast_areas > 0);
}

#[tokio::test]
#[ignore = "requires live EPA AirNow hourly public files"]
async fn live_airnow_hourly_file_matches_typed_wire() {
    let source = AirNowSource::connect(AirNowSourceConfig::default()).expect("source");
    let now = Utc::now();
    let base_hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("UTC hour");
    let mut found = None;
    for hours_ago in 1..=6 {
        let report = source
            .hourly_pm25_area_observation(
                "New York City Region",
                "NY",
                base_hour - Duration::hours(hours_ago),
                now,
            )
            .await
            .expect("AirNow hourly partition");
        if report.is_some() {
            found = report;
            break;
        }
    }
    let report = found.expect("recent New York City Region hourly AQI");
    assert_eq!(report.source_id, DomainSourceId::airnow());
    assert_eq!(report.variable, WeatherVariable::Aqi);
    assert_eq!(report.unit, DomainMeasurementUnit::Aqi);
}

#[tokio::test]
#[ignore = "requires live EPA AirNow hourly public files"]
async fn live_airnow_union_city_site_has_exact_pm25_aqi_provenance() {
    let source = AirNowSource::connect(AirNowSourceConfig::default()).expect("source");
    let binding = WeatherVerticalBindingsConfig::default()
        .airnow_pm25_sites
        .remove(0);
    let now = Utc::now();
    let base_hour = now
        .with_minute(0)
        .and_then(|value| value.with_second(0))
        .and_then(|value| value.with_nanosecond(0))
        .expect("UTC hour");
    let mut found = None;
    for hours_ago in 1..=8 {
        let report = source
            .hourly_pm25_site_observation(&binding, base_hour - Duration::hours(hours_ago), now)
            .await
            .expect("AirNow exact site hourly partition");
        if report.is_some() {
            found = report;
            break;
        }
    }
    let report = found.expect("recent Union City High School PM2.5 AQI");
    assert_eq!(report.source_id, DomainSourceId::airnow());
    assert_eq!(report.subject_key, "840340170008");
    assert_eq!(
        report.instrument_key,
        DomainInstrumentKey::airnow_pm25_site("840340170008")
    );
    assert_eq!(report.variable, WeatherVariable::Aqi);
    assert_eq!(report.unit, DomainMeasurementUnit::Aqi);
    assert!(report.value >= Decimal::ZERO && report.value <= Decimal::from(500));
}

#[tokio::test]
#[ignore = "requires live NOAA SPC preliminary report file"]
async fn live_spc_tornado_partition_matches_typed_wire() {
    let source = TornadoSource::connect(TornadoSourceConfig::default()).expect("source");
    let report_date = (Utc::now() - Duration::days(1)).date_naive();
    let report = source
        .spc_preliminary_day("oklahoma", "OK", report_date, Utc::now())
        .await
        .expect("SPC tornado partition")
        .expect("published SPC partition");
    assert_eq!(report.source_id, DomainSourceId::spc_storm_reports());
    assert_eq!(report.variable, WeatherVariable::TornadoCount);
    assert_eq!(report.unit, DomainMeasurementUnit::Count);
}

#[tokio::test]
#[ignore = "requires live NOAA NCEI Storm Events archive"]
async fn live_ncei_tornado_archive_discovers_latest_corrected_year() {
    let source = TornadoSource::connect(TornadoSourceConfig::default()).expect("source");
    let day = source
        .ncei_final_day(
            "oklahoma",
            "OKLAHOMA",
            chrono_tz::America::Chicago,
            chrono::NaiveDate::from_ymd_opt(2013, 5, 20).expect("date"),
            Utc::now(),
        )
        .await
        .expect("NCEI final tornado day");
    assert_eq!(day.report.source_id, DomainSourceId::ncei_storm_events());
    assert!(day.report.value > Decimal::ZERO);
}

#[tokio::test]
#[ignore = "requires live NOAA NHC current advisory JSON"]
async fn live_nhc_current_advisory_matches_typed_wire() {
    let source = NhcSource::connect(NhcSourceConfig::default()).expect("source");
    let reports = source
        .active_advisories(Utc::now())
        .await
        .expect("NHC current advisories");
    assert!(reports.iter().all(|report| {
        report.source_id == DomainSourceId::nhc_advisory()
            && report.variable == WeatherVariable::CycloneIntensity
            && report.unit == DomainMeasurementUnit::Knot
            && report.valid_from == Some(report.observed_at)
            && report.published_at <= report.available_at
    }));
}

#[tokio::test]
#[ignore = "requires live NOAA NHC HURDAT2 archive"]
async fn live_nhc_hurdat_discovers_latest_corrected_file() {
    let source = NhcSource::connect(NhcSourceConfig::default()).expect("source");
    let track = source
        .hurdat2_storm(NhcBasin::Atlantic, "AL092021", Utc::now())
        .await
        .expect("HURDAT2")
        .expect("Hurricane Ida best track");
    assert!(!track.reports.is_empty());
    assert!(
        track
            .reports
            .iter()
            .all(|report| report.source_id == DomainSourceId::nhc_hurdat2())
    );
}

#[tokio::test]
#[ignore = "requires live NASA GISS GISTEMP v4 table"]
async fn live_nasa_gistemp_monthly_table_matches_typed_wire() {
    let source = NasaGistempSource::connect(NasaGistempSourceConfig::default()).expect("source");
    let dataset = source
        .monthly_anomalies(Utc::now())
        .await
        .expect("GISTEMP monthly anomalies");
    assert!(dataset.reports.len() > 1_000);
    assert!(dataset.reports.iter().all(|report| {
        report.source_id == DomainSourceId::nasa_gistemp()
            && report.variable == WeatherVariable::GlobalTemperatureAnomaly
            && report.unit == DomainMeasurementUnit::CelsiusAnomaly
    }));
}

#[tokio::test]
#[ignore = "requires live NOAA@NSIDC Sea Ice Index v4 files"]
async fn live_nsidc_both_hemisphere_files_match_typed_wire() {
    let source = NsidcSeaIceSource::connect(NsidcSeaIceSourceConfig::default()).expect("source");
    for hemisphere in [SeaIceHemisphere::North, SeaIceHemisphere::South] {
        let dataset = source
            .daily_extent(hemisphere, Utc::now())
            .await
            .expect("NSIDC daily extent");
        assert!(!dataset.reports.is_empty());
        assert!(dataset.reports.iter().all(|report| {
            report.source_id == DomainSourceId::nsidc_sea_ice_index()
                && report.variable == WeatherVariable::SeaIceExtent
                && report.unit == DomainMeasurementUnit::MillionSquareKilometer
        }));
    }
}

#[tokio::test]
#[ignore = "requires live NOAA/NWS station observation API"]
async fn live_nws_mount_washington_wind_matches_typed_wire() {
    let source =
        NwsObservationSource::connect(NwsObservationSourceConfig::default()).expect("source");
    let reports = source
        .recent_wind(&IcaoStation::parse("KMWN").expect("station"), Utc::now())
        .await
        .expect("NWS station wind");
    assert!(!reports.is_empty());
    assert!(reports.iter().all(|report| {
        report.source_id == DomainSourceId::nws_observation()
            && matches!(
                report.variable,
                WeatherVariable::WindSpeed | WeatherVariable::WindGust
            )
            && report.unit == DomainMeasurementUnit::Knot
    }));
}
