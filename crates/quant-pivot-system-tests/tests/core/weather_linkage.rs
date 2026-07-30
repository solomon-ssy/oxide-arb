//! Weather linkage decision-group system contract against disposable `PostgreSQL`.

use std::{collections::HashMap, sync::Arc};

use chrono::{TimeZone, Utc};
use quant_pivot_core::governance::linkage::{LinkageResolverDeps, LinkageResolverService};
use quant_pivot_models::{
    config::{
        WeatherHistoricalBindingKind, WeatherStationProfileConfig, WeatherVerticalBindingsConfig,
    },
    enums::common::MarketCategory,
    hashing::CanonicalDigest,
    types::{ContentHash, MarketId, TokenId},
};
use quant_pivot_repository::{
    postgres::{PgEventRepository, PgMarketLinkageRepository, PgMarketRepository},
    traits::{EventRepository, MarketLinkageRepository, MarketRepository},
};
use quant_pivot_system_tests::{
    postgres::setup_pg,
    support::catalog_fixtures::{make_event, make_market},
};
use rust_decimal_macros::dec;
use sea_orm::DatabaseConnection;
use serde::Serialize;

fn hash<T: Serialize>(value: &T) -> ContentHash {
    CanonicalDigest::content_hash_json(value).expect("fixture hash")
}

pub fn station_profiles() -> HashMap<String, WeatherStationProfileConfig> {
    HashMap::from([(
        "KLGA".to_owned(),
        WeatherStationProfileConfig {
            timezone: "America/New_York".to_owned(),
            latitude: dec!(40.7769),
            longitude: dec!(-73.8740),
            elevation_meters: dec!(6.4),
            ghcnh_station_id: Some("USW00014732".to_owned()),
            historical_binding_kind: WeatherHistoricalBindingKind::ExactStation,
        },
    )])
}

fn description() -> String {
    "This market will resolve to the temperature range that contains the highest temperature \
     recorded at the LaGuardia Airport Station. This market can not resolve until the first data \
     point for the following date has been published on the resolution source. The resolution \
     source is available here: \
     https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA. The resolution source \
     measures temperatures to whole degrees Fahrenheit."
        .to_owned()
}

async fn seed_group(db: &DatabaseConnection, event_id: &str, members: &[(&str, &str)]) {
    let ids: Vec<_> = members
        .iter()
        .map(|(market_id, _)| MarketId::new(*market_id))
        .collect();
    let mut event = make_event(
        event_id,
        "New York City highest temperature",
        event_id,
        MarketCategory::Weather,
    );
    event.neg_risk = true;
    event.catalog_market_ids = ids.clone().into();
    event.content_hash = hash(&(event_id, &ids));
    PgEventRepository::new(db.clone())
        .upsert(event)
        .await
        .expect("seed weather event");

    let end_date = Utc
        .with_ymd_and_hms(2026, 7, 11, 23, 0, 0)
        .single()
        .expect("end date");
    let repo = PgMarketRepository::new(db.clone());
    for (index, (market_id, question)) in members.iter().enumerate() {
        let mut market = make_market(
            market_id,
            event_id,
            question,
            market_id,
            MarketCategory::Weather,
            Some(end_date),
        );
        market.description = Some(description());
        market.neg_risk = true;
        market.yes_token_id = TokenId::new(format!("{market_id}-yes"));
        market.no_token_id = TokenId::new(format!("{market_id}-no"));
        market.content_hash = hash(&(market_id, question, index));
        repo.upsert(market).await.expect("seed weather market");
    }
}

fn service(db: &DatabaseConnection) -> LinkageResolverService {
    LinkageResolverService::new(
        LinkageResolverDeps {
            linkage_repo: Arc::new(PgMarketLinkageRepository::new(db.clone())),
            market_repo: Arc::new(PgMarketRepository::new(db.clone())),
            event_repo: Arc::new(PgEventRepository::new(db.clone())),
        },
        station_profiles(),
        &WeatherVerticalBindingsConfig::default(),
    )
    .expect("linkage resolver")
}

pub async fn single_sibling_validates_group() {
    let (pool, _container) = setup_pg().await;
    let db = pool.connection().clone();
    let valid_members = [
        (
            "weather-low",
            "Will the highest temperature in New York City be 12°F or below on July 11?",
        ),
        (
            "weather-mid",
            "Will the highest temperature in New York City be 13°F on July 11?",
        ),
        (
            "weather-high",
            "Will the highest temperature in New York City be 14°F or higher on July 11?",
        ),
    ];
    seed_group(&db, "weather-valid-event", &valid_members).await;

    let summary = service(&db)
        .resolve_changed_markets(&[MarketId::new("weather-mid")])
        .await
        .expect("resolve complete group");
    assert_eq!(summary.examined, 3);
    assert_eq!(summary.appended, 3);
    assert_eq!(summary.resolved, 3);
    let valid_ids: Vec<_> = valid_members
        .iter()
        .map(|(market_id, _)| MarketId::new(*market_id))
        .collect();
    let rows = PgMarketLinkageRepository::new(db.clone())
        .latest_for_markets(&valid_ids)
        .await
        .expect("valid linkage rows");
    assert_eq!(rows.len(), 3);

    let invalid_members = [
        (
            "weather-gap-low",
            "Will the highest temperature in New York City be 12°F or below on July 11?",
        ),
        (
            "weather-gap-high",
            "Will the highest temperature in New York City be 14°F or higher on July 11?",
        ),
    ];
    seed_group(&db, "weather-gap-event", &invalid_members).await;
    let error = service(&db)
        .resolve_changed_markets(&[MarketId::new("weather-gap-low")])
        .await
        .expect_err("integer gap must fail closed");
    assert!(error.to_string().contains("uncovered integer gap"));
    let invalid_ids: Vec<_> = invalid_members
        .iter()
        .map(|(market_id, _)| MarketId::new(*market_id))
        .collect();
    assert!(
        PgMarketLinkageRepository::new(db)
            .latest_for_markets(&invalid_ids)
            .await
            .expect("invalid group rows")
            .is_empty(),
        "no invalid sibling may be appended before whole-group validation"
    );
}
