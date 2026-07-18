//! Daily maximum/minimum projection, correction and event-chain integration tests.

use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use quant_pivot_models::{
    domain::{
        DomainEventType, DomainSourceCheckpoint, WeatherDailyTemperatureProjectionInfo,
        WeatherObservationReport, WeatherObservationReportKind,
    },
    types::{
        ContentHash, DomainInstrumentKey, DomainMeasurementUnit, DomainSourceId, IcaoStation,
        WeatherTemperatureStatistic, WeatherVariable,
    },
};
use quant_pivot_repository::{
    postgres::{PgDomainProjectionRepository, PgEntryConditionRepository},
    traits::{DomainProjectionRepository, EntryConditionRepository},
};
use quant_pivot_test_support::pg::setup_pg;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use uuid::Uuid;

fn hash(fill: char) -> ContentHash {
    ContentHash::parse(format!("blake3:{}", fill.to_string().repeat(64))).expect("hash")
}

fn report(
    observed_at: DateTime<Utc>,
    available_at: DateTime<Utc>,
    value: Decimal,
    report_kind: WeatherObservationReportKind,
    report_hash: ContentHash,
) -> WeatherObservationReport {
    let station = IcaoStation::parse("KJFK").expect("station");
    WeatherObservationReport {
        source_id: DomainSourceId::aviation_weather(),
        instrument_key: DomainInstrumentKey::aviation_weather(&station),
        subject_key: station.to_string(),
        report_kind,
        variable: WeatherVariable::Temperature,
        value,
        unit: DomainMeasurementUnit::Celsius,
        precision: dec!(0.1),
        observed_at,
        valid_from: None,
        valid_to: None,
        published_at: available_at - Duration::seconds(1),
        available_at,
        report_hash,
        raw_report: "fixture".to_owned(),
    }
}

fn extreme_value(
    rows: &[WeatherDailyTemperatureProjectionInfo],
    statistic: WeatherTemperatureStatistic,
) -> Decimal {
    rows.iter()
        .find(|row| row.temperature_statistic == statistic)
        .expect("temperature statistic")
        .current_extreme
        .value()
}

fn checkpoint(report: &WeatherObservationReport, revision: u32) -> DomainSourceCheckpoint {
    DomainSourceCheckpoint::AviationWeather {
        available_at: report.available_at,
        published_at: report.published_at,
        observation_time: report.observed_at,
        revision,
        report_hash: report.report_hash.clone(),
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn weather_projection_tracks_maximum_and_minimum_with_independent_events() {
    let (pool, _container) = setup_pg().await;
    let projections = PgDomainProjectionRepository::new(pool.connection().clone());
    let conditions = PgEntryConditionRepository::new(pool.connection().clone());
    let local_date = NaiveDate::from_ymd_opt(2026, 7, 18).expect("date");
    let first_at = Utc
        .with_ymd_and_hms(2026, 7, 18, 10, 0, 0)
        .single()
        .expect("time");
    let second_at = first_at + Duration::hours(5);
    let first = report(
        first_at,
        first_at + Duration::seconds(2),
        dec!(10),
        WeatherObservationReportKind::Metar,
        hash('a'),
    );
    projections
        .apply_weather_report(
            first.clone(),
            "America/New_York".to_owned(),
            local_date,
            checkpoint(&first, 0),
            0,
            true,
        )
        .await
        .expect("first observation");
    let second = report(
        second_at,
        second_at + Duration::seconds(2),
        dec!(30),
        WeatherObservationReportKind::Metar,
        hash('b'),
    );
    let second_state = projections
        .apply_weather_report(
            second.clone(),
            "America/New_York".to_owned(),
            local_date,
            checkpoint(&second, 0),
            0,
            true,
        )
        .await
        .expect("second observation");
    assert_eq!(second_state.len(), 2);
    assert_eq!(
        extreme_value(&second_state, WeatherTemperatureStatistic::Maximum),
        dec!(30)
    );
    assert_eq!(
        extreme_value(&second_state, WeatherTemperatureStatistic::Minimum),
        dec!(10)
    );

    let correction = report(
        first_at,
        second_at + Duration::minutes(1),
        dec!(12),
        WeatherObservationReportKind::Correction,
        hash('c'),
    );
    let corrected = projections
        .apply_weather_report(
            correction.clone(),
            "America/New_York".to_owned(),
            local_date,
            checkpoint(&correction, 1),
            0,
            true,
        )
        .await
        .expect("correction");
    assert_eq!(
        corrected
            .iter()
            .find(|row| row.temperature_statistic == WeatherTemperatureStatistic::Minimum)
            .expect("corrected minimum")
            .current_extreme
            .value(),
        dec!(12)
    );
    projections
        .apply_weather_report(
            correction.clone(),
            "America/New_York".to_owned(),
            local_date,
            checkpoint(&correction, 1),
            0,
            true,
        )
        .await
        .expect("idempotent retry");

    let station = IcaoStation::parse("KJFK").expect("station");
    let instrument = DomainInstrumentKey::aviation_weather(&station);
    let minimum = conditions
        .find_weather_projection(
            &DomainSourceId::aviation_weather(),
            &instrument,
            "KJFK",
            local_date,
            WeatherTemperatureStatistic::Minimum,
        )
        .await
        .expect("query minimum")
        .expect("minimum projection");
    assert_eq!(minimum.current_extreme.value(), dec!(12));

    let closed = projections
        .close_weather_day(&station, local_date, second_at + Duration::hours(10))
        .await
        .expect("close day");
    assert_eq!(closed.len(), 2);
    assert!(closed.iter().all(|row| row.day_closed));
    assert_eq!(
        projections
            .mark_weather_source_gap(&station, local_date, second_at + Duration::hours(11))
            .await
            .expect("mark gap"),
        1
    );

    let events = projections
        .claim_pending_events(
            Uuid::new_v4(),
            Utc::now(),
            Utc::now() + Duration::minutes(1),
            100,
        )
        .await
        .expect("claim events");
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                event.event_type == DomainEventType::WeatherDailyTemperatureExtremeCorrected
            })
            .count(),
        1,
        "only the minimum event chain is corrected"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == DomainEventType::WeatherObservationDayClosed)
            .count(),
        2,
        "maximum and minimum close independently"
    );
}
